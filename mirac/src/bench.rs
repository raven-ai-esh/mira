use std::time::Instant;

use std::fs;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::asm_arm64::{emit_arm64_library, supports_arm64_asm_backend};
use crate::asm_x86_64::{emit_x86_64_library, target_from_triple, X86_64ObjectFlavor};
use crate::codegen_c::{emit_benchmark_driver_from_lowered, emit_benchmark_harness};
use crate::lowered_bytecode::{compile_bytecode_program, run_bytecode_function};
use crate::lowered_exec::{
    benchmark_arg_values, lower_program_for_direct_exec, run_lowered_function,
};
use crate::toolchain::{
    compile_c_source, compile_clang_bundle_for_target_with_runtime_support,
    compile_clang_bundle_with_runtime_support, load_and_validate, run_binary,
};
use crate::types::DataValue;

#[derive(Debug, Clone)]
pub struct BenchmarkCase {
    pub case_name: &'static str,
    pub file_name: &'static str,
    pub function_name: &'static str,
    pub arguments: Vec<(String, DataValue)>,
    pub iterations: usize,
    pub unit_work: usize,
}

#[derive(Debug, Clone)]
pub struct BenchmarkResult {
    pub case_name: String,
    pub file_path: String,
    pub iterations: usize,
    pub unit_work: usize,
    pub samples_ns: Vec<u64>,
    pub median_ns: u64,
    pub p95_ns: u64,
    pub p99_ns: u64,
    pub per_call_ns: f64,
    pub units_per_second: f64,
}

pub fn run_benchmark_suite(
    examples_dir: &Path,
    output: Option<&Path>,
) -> Result<Vec<BenchmarkResult>, String> {
    let mut results = Vec::new();
    for case in benchmark_cases() {
        let file_path = examples_dir.join(case.file_name);
        let program = load_and_validate(&file_path)?;
        let harness = emit_benchmark_harness(
            &program,
            case.function_name,
            &case.arguments,
            case.iterations,
        )?;
        let binary = compile_c_source(&format!("bench_{}", case.case_name), &harness)?;
        let output_data = run_binary(&binary)?;
        if !output_data.status.success() {
            return Err(format!(
                "benchmark binary failed for {}:\n{}",
                file_path.display(),
                String::from_utf8_lossy(&output_data.stderr)
            ));
        }
        let stdout = String::from_utf8(output_data.stdout)
            .map_err(|error| format!("benchmark output was not utf-8: {error}"))?;
        let samples = parse_samples(&stdout)?;
        let median_ns = median(samples.clone());
        let p95_ns = percentile(samples.clone(), 95);
        let p99_ns = percentile(samples.clone(), 99);
        let per_call_ns = median_ns as f64 / case.iterations as f64;
        let units_per_second =
            (case.iterations * case.unit_work) as f64 / (median_ns as f64 / 1_000_000_000.0);
        results.push(BenchmarkResult {
            case_name: case.case_name.to_string(),
            file_path: file_path.display().to_string(),
            iterations: case.iterations,
            unit_work: case.unit_work,
            samples_ns: samples,
            median_ns,
            p95_ns,
            p99_ns,
            per_call_ns,
            units_per_second,
        });
    }
    if let Some(output_path) = output {
        fs::write(output_path, render_results_json(&results))
            .map_err(|error| format!("failed to write {}: {error}", output_path.display()))?;
    }
    Ok(results)
}

pub fn run_single_source_benchmark(
    source_path: &Path,
    function_name: &str,
    iterations: usize,
    unit_work: usize,
    output: Option<&Path>,
) -> Result<Vec<BenchmarkResult>, String> {
    let program = load_and_validate(source_path)?;
    let harness = emit_benchmark_harness(&program, function_name, &[], iterations)?;
    let binary = compile_c_source(
        &format!(
            "bench_single_{}",
            source_path
                .file_stem()
                .and_then(|item| item.to_str())
                .unwrap_or("mira")
        ),
        &harness,
    )?;
    let output_data = run_binary(&binary)?;
    if !output_data.status.success() {
        return Err(format!(
            "benchmark binary failed for {}:\n{}",
            source_path.display(),
            String::from_utf8_lossy(&output_data.stderr)
        ));
    }
    let stdout = String::from_utf8(output_data.stdout)
        .map_err(|error| format!("benchmark output was not utf-8: {error}"))?;
    let samples = parse_samples(&stdout)?;
    let median_ns = median(samples.clone());
    let p95_ns = percentile(samples.clone(), 95);
    let p99_ns = percentile(samples.clone(), 99);
    let per_call_ns = median_ns as f64 / iterations as f64;
    let units_per_second = (iterations * unit_work) as f64 / (median_ns as f64 / 1_000_000_000.0);
    let results = vec![BenchmarkResult {
        case_name: function_name.to_string(),
        file_path: source_path.display().to_string(),
        iterations,
        unit_work,
        samples_ns: samples,
        median_ns,
        p95_ns,
        p99_ns,
        per_call_ns,
        units_per_second,
    }];
    if let Some(output_path) = output {
        fs::write(output_path, render_results_json(&results))
            .map_err(|error| format!("failed to write {}: {error}", output_path.display()))?;
    }
    Ok(results)
}

pub fn run_single_source_bytecode_benchmark(
    source_path: &Path,
    function_name: &str,
    iterations: usize,
    unit_work: usize,
    output: Option<&Path>,
) -> Result<Vec<BenchmarkResult>, String> {
    let program = load_and_validate(source_path)?;
    let lowered = lower_program_for_direct_exec(&program)?;
    let bytecode = compile_bytecode_program(&lowered)?;
    let args = benchmark_arg_values(&program, function_name, &[])?;
    let mut samples = Vec::new();
    for _ in 0..2 {
        let started = Instant::now();
        let mut last = None;
        for _ in 0..iterations {
            last = Some(run_bytecode_function(&bytecode, function_name, &args)?);
        }
        let elapsed = started.elapsed().as_nanos() as u64;
        if last.is_none() {
            return Err(format!(
                "bytecode benchmark produced no result for {}",
                source_path.display()
            ));
        }
        samples.push(elapsed);
    }
    let median_ns = median(samples.clone());
    let p95_ns = percentile(samples.clone(), 95);
    let p99_ns = percentile(samples.clone(), 99);
    let per_call_ns = median_ns as f64 / iterations as f64;
    let units_per_second = (iterations * unit_work) as f64 / (median_ns as f64 / 1_000_000_000.0);
    let results = vec![BenchmarkResult {
        case_name: function_name.to_string(),
        file_path: source_path.display().to_string(),
        iterations,
        unit_work,
        samples_ns: samples,
        median_ns,
        p95_ns,
        p99_ns,
        per_call_ns,
        units_per_second,
    }];
    if let Some(output_path) = output {
        fs::write(output_path, render_results_json(&results))
            .map_err(|error| format!("failed to write {}: {error}", output_path.display()))?;
    }
    Ok(results)
}

pub fn run_single_source_arm64_benchmark(
    source_path: &Path,
    function_name: &str,
    iterations: usize,
    unit_work: usize,
    output: Option<&Path>,
) -> Result<Vec<BenchmarkResult>, String> {
    if !supports_arm64_asm_backend() {
        return Err("arm64 asm backend is only supported on aarch64 macOS hosts".to_string());
    }
    let program = load_and_validate(source_path)?;
    let lowered = lower_program_for_direct_exec(&program)?;
    let bytecode = compile_bytecode_program(&lowered)?;
    let asm_source = emit_arm64_library(&bytecode)?;
    let driver =
        emit_benchmark_driver_from_lowered(&lowered, &program, function_name, &[], iterations)?;
    let binary = compile_clang_bundle_with_runtime_support(
        &format!(
            "bench_single_arm64_{}_{}",
            source_path
                .file_stem()
                .and_then(|item| item.to_str())
                .unwrap_or("mira"),
            benchmark_stem_nonce()
        ),
        &[("s", &asm_source), ("c", &driver)],
        &["-std=c11"],
    )?;
    let output_data = run_binary(&binary)?;
    if !output_data.status.success() {
        return Err(format!(
            "arm64 benchmark binary failed for {}:\n{}",
            source_path.display(),
            String::from_utf8_lossy(&output_data.stderr)
        ));
    }
    let stdout = String::from_utf8(output_data.stdout)
        .map_err(|error| format!("benchmark output was not utf-8: {error}"))?;
    let samples = parse_samples(&stdout)?;
    let median_ns = median(samples.clone());
    let p95_ns = percentile(samples.clone(), 95);
    let p99_ns = percentile(samples.clone(), 99);
    let per_call_ns = median_ns as f64 / iterations as f64;
    let units_per_second = (iterations * unit_work) as f64 / (median_ns as f64 / 1_000_000_000.0);
    let results = vec![BenchmarkResult {
        case_name: function_name.to_string(),
        file_path: source_path.display().to_string(),
        iterations,
        unit_work,
        samples_ns: samples,
        median_ns,
        p95_ns,
        p99_ns,
        per_call_ns,
        units_per_second,
    }];
    if let Some(output_path) = output {
        fs::write(output_path, render_results_json(&results))
            .map_err(|error| format!("failed to write {}: {error}", output_path.display()))?;
    }
    Ok(results)
}

pub fn run_single_source_x86_64_benchmark(
    source_path: &Path,
    target_triple: &str,
    function_name: &str,
    iterations: usize,
    unit_work: usize,
    output: Option<&Path>,
) -> Result<Vec<BenchmarkResult>, String> {
    let target = target_from_triple(target_triple)?;
    if target.flavor != X86_64ObjectFlavor::MachO {
        return Err(
            "x86_64 single-source benchmark currently supports runnable Mach-O targets only"
                .to_string(),
        );
    }
    if !cfg!(target_os = "macos") {
        return Err("x86_64 single-source benchmark requires a macOS host".to_string());
    }
    let program = load_and_validate(source_path)?;
    let lowered = lower_program_for_direct_exec(&program)?;
    let bytecode = compile_bytecode_program(&lowered)?;
    let asm_source = emit_x86_64_library(&bytecode, target)?;
    let driver =
        emit_benchmark_driver_from_lowered(&lowered, &program, function_name, &[], iterations)?;
    let binary = compile_clang_bundle_for_target_with_runtime_support(
        &format!(
            "bench_single_x86_64_{}_{}_{}",
            source_path
                .file_stem()
                .and_then(|item| item.to_str())
                .unwrap_or("mira"),
            sanitize_triple(target_triple),
            benchmark_stem_nonce()
        ),
        &[("s", &asm_source), ("c", &driver)],
        &["-std=c11"],
        target_triple,
    )?;
    let output_data = run_binary(&binary)?;
    if !output_data.status.success() {
        return Err(format!(
            "x86_64 benchmark binary failed for {}:\n{}",
            source_path.display(),
            String::from_utf8_lossy(&output_data.stderr)
        ));
    }
    let stdout = String::from_utf8(output_data.stdout)
        .map_err(|error| format!("benchmark output was not utf-8: {error}"))?;
    let samples = parse_samples(&stdout)?;
    let median_ns = median(samples.clone());
    let p95_ns = percentile(samples.clone(), 95);
    let p99_ns = percentile(samples.clone(), 99);
    let per_call_ns = median_ns as f64 / iterations as f64;
    let units_per_second = (iterations * unit_work) as f64 / (median_ns as f64 / 1_000_000_000.0);
    let results = vec![BenchmarkResult {
        case_name: function_name.to_string(),
        file_path: source_path.display().to_string(),
        iterations,
        unit_work,
        samples_ns: samples,
        median_ns,
        p95_ns,
        p99_ns,
        per_call_ns,
        units_per_second,
    }];
    if let Some(output_path) = output {
        fs::write(output_path, render_results_json(&results))
            .map_err(|error| format!("failed to write {}: {error}", output_path.display()))?;
    }
    Ok(results)
}

pub fn run_direct_benchmark_suite(
    examples_dir: &Path,
    output: Option<&Path>,
) -> Result<Vec<BenchmarkResult>, String> {
    let mut results = Vec::new();
    for case in benchmark_cases() {
        let file_path = examples_dir.join(case.file_name);
        let program = load_and_validate(&file_path)?;
        let lowered = lower_program_for_direct_exec(&program)?;
        let args = benchmark_arg_values(&program, case.function_name, &case.arguments)?;
        let direct_iterations = direct_benchmark_iterations(&case);
        let mut samples = Vec::new();
        for _ in 0..2 {
            let started = Instant::now();
            let mut last = None;
            for _ in 0..direct_iterations {
                last = Some(run_lowered_function(&lowered, case.function_name, &args)?);
            }
            let elapsed = started.elapsed().as_nanos() as u64;
            if last.is_none() {
                return Err(format!(
                    "direct lowered benchmark produced no result for {}",
                    case.case_name
                ));
            }
            samples.push(elapsed);
        }
        let median_ns = median(samples.clone());
        let p95_ns = percentile(samples.clone(), 95);
        let p99_ns = percentile(samples.clone(), 99);
        let per_call_ns = median_ns as f64 / direct_iterations as f64;
        let units_per_second =
            (direct_iterations * case.unit_work) as f64 / (median_ns as f64 / 1_000_000_000.0);
        results.push(BenchmarkResult {
            case_name: case.case_name.to_string(),
            file_path: file_path.display().to_string(),
            iterations: direct_iterations,
            unit_work: case.unit_work,
            samples_ns: samples,
            median_ns,
            p95_ns,
            p99_ns,
            per_call_ns,
            units_per_second,
        });
    }
    if let Some(output_path) = output {
        fs::write(output_path, render_results_json(&results))
            .map_err(|error| format!("failed to write {}: {error}", output_path.display()))?;
    }
    Ok(results)
}

pub fn run_bytecode_benchmark_suite(
    examples_dir: &Path,
    output: Option<&Path>,
) -> Result<Vec<BenchmarkResult>, String> {
    let mut results = Vec::new();
    for case in benchmark_cases() {
        let file_path = examples_dir.join(case.file_name);
        let program = load_and_validate(&file_path)?;
        let lowered = lower_program_for_direct_exec(&program)?;
        let bytecode = compile_bytecode_program(&lowered)?;
        let args = benchmark_arg_values(&program, case.function_name, &case.arguments)?;
        let iterations = direct_benchmark_iterations(&case);
        let mut samples = Vec::new();
        for _ in 0..2 {
            let started = Instant::now();
            let mut last = None;
            for _ in 0..iterations {
                last = Some(run_bytecode_function(&bytecode, case.function_name, &args)?);
            }
            let elapsed = started.elapsed().as_nanos() as u64;
            if last.is_none() {
                return Err(format!(
                    "bytecode benchmark produced no result for {}",
                    case.case_name
                ));
            }
            samples.push(elapsed);
        }
        let median_ns = median(samples.clone());
        let p95_ns = percentile(samples.clone(), 95);
        let p99_ns = percentile(samples.clone(), 99);
        let per_call_ns = median_ns as f64 / iterations as f64;
        let units_per_second =
            (iterations * case.unit_work) as f64 / (median_ns as f64 / 1_000_000_000.0);
        results.push(BenchmarkResult {
            case_name: case.case_name.to_string(),
            file_path: file_path.display().to_string(),
            iterations,
            unit_work: case.unit_work,
            samples_ns: samples,
            median_ns,
            p95_ns,
            p99_ns,
            per_call_ns,
            units_per_second,
        });
    }
    if let Some(output_path) = output {
        fs::write(output_path, render_results_json(&results))
            .map_err(|error| format!("failed to write {}: {error}", output_path.display()))?;
    }
    Ok(results)
}

pub fn run_arm64_benchmark_suite(
    examples_dir: &Path,
    output: Option<&Path>,
) -> Result<Vec<BenchmarkResult>, String> {
    if !supports_arm64_asm_backend() {
        return Err("arm64 asm backend is only supported on aarch64 macOS hosts".to_string());
    }
    let mut results = Vec::new();
    for case in benchmark_cases() {
        let file_path = examples_dir.join(case.file_name);
        let program = load_and_validate(&file_path)?;
        let lowered = lower_program_for_direct_exec(&program)?;
        let bytecode = compile_bytecode_program(&lowered)?;
        let asm_source = emit_arm64_library(&bytecode)?;
        let driver = emit_benchmark_driver_from_lowered(
            &lowered,
            &program,
            case.function_name,
            &case.arguments,
            case.iterations,
        )?;
        let binary = compile_clang_bundle_with_runtime_support(
            &format!("bench_arm64_{}", case.case_name),
            &[("s", &asm_source), ("c", &driver)],
            &["-std=c11"],
        )?;
        let output_data = run_binary(&binary)?;
        if !output_data.status.success() {
            return Err(format!(
                "arm64 benchmark binary failed for {}:\n{}",
                file_path.display(),
                String::from_utf8_lossy(&output_data.stderr)
            ));
        }
        let stdout = String::from_utf8(output_data.stdout)
            .map_err(|error| format!("benchmark output was not utf-8: {error}"))?;
        let samples = parse_samples(&stdout)?;
        let median_ns = median(samples.clone());
        let p95_ns = percentile(samples.clone(), 95);
        let p99_ns = percentile(samples.clone(), 99);
        let per_call_ns = median_ns as f64 / case.iterations as f64;
        let units_per_second =
            (case.iterations * case.unit_work) as f64 / (median_ns as f64 / 1_000_000_000.0);
        results.push(BenchmarkResult {
            case_name: case.case_name.to_string(),
            file_path: file_path.display().to_string(),
            iterations: case.iterations,
            unit_work: case.unit_work,
            samples_ns: samples,
            median_ns,
            p95_ns,
            p99_ns,
            per_call_ns,
            units_per_second,
        });
    }
    if let Some(output_path) = output {
        fs::write(output_path, render_results_json(&results))
            .map_err(|error| format!("failed to write {}: {error}", output_path.display()))?;
    }
    Ok(results)
}

pub fn run_x86_64_benchmark_suite(
    examples_dir: &Path,
    target_triple: &str,
    output: Option<&Path>,
) -> Result<Vec<BenchmarkResult>, String> {
    let target = target_from_triple(target_triple)?;
    if target.flavor != X86_64ObjectFlavor::MachO {
        return Err(
            "x86_64 benchmark suite currently supports runnable Mach-O targets only".to_string(),
        );
    }
    if !cfg!(target_os = "macos") {
        return Err("x86_64 Mach-O benchmark suite requires a macOS host".to_string());
    }

    let mut results = Vec::new();
    for case in benchmark_cases() {
        let file_path = examples_dir.join(case.file_name);
        let program = load_and_validate(&file_path)?;
        let lowered = lower_program_for_direct_exec(&program)?;
        let bytecode = compile_bytecode_program(&lowered)?;
        let asm_source = emit_x86_64_library(&bytecode, target)?;
        let driver = emit_benchmark_driver_from_lowered(
            &lowered,
            &program,
            case.function_name,
            &case.arguments,
            case.iterations,
        )?;
        let binary = compile_clang_bundle_for_target_with_runtime_support(
            &format!(
                "bench_x86_64_{}_{}",
                case.case_name,
                sanitize_triple(target_triple)
            ),
            &[("s", &asm_source), ("c", &driver)],
            &["-std=c11"],
            target_triple,
        )?;
        let output_data = run_binary(&binary)?;
        if !output_data.status.success() {
            return Err(format!(
                "x86_64 benchmark binary failed for {}:\n{}",
                file_path.display(),
                String::from_utf8_lossy(&output_data.stderr)
            ));
        }
        let stdout = String::from_utf8(output_data.stdout)
            .map_err(|error| format!("benchmark output was not utf-8: {error}"))?;
        let samples = parse_samples(&stdout)?;
        let median_ns = median(samples.clone());
        let p95_ns = percentile(samples.clone(), 95);
        let p99_ns = percentile(samples.clone(), 99);
        let per_call_ns = median_ns as f64 / case.iterations as f64;
        let units_per_second =
            (case.iterations * case.unit_work) as f64 / (median_ns as f64 / 1_000_000_000.0);
        results.push(BenchmarkResult {
            case_name: case.case_name.to_string(),
            file_path: file_path.display().to_string(),
            iterations: case.iterations,
            unit_work: case.unit_work,
            samples_ns: samples,
            median_ns,
            p95_ns,
            p99_ns,
            per_call_ns,
            units_per_second,
        });
    }
    if let Some(output_path) = output {
        fs::write(output_path, render_results_json(&results))
            .map_err(|error| format!("failed to write {}: {error}", output_path.display()))?;
    }
    Ok(results)
}

fn direct_benchmark_iterations(case: &BenchmarkCase) -> usize {
    let target_total_work = 300_000usize;
    let scaled = (target_total_work / case.unit_work).max(1);
    case.iterations.min(scaled.max(10))
}

fn benchmark_stem_nonce() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time should be after unix epoch")
        .as_nanos()
}

fn sanitize_triple(triple: &str) -> String {
    triple.replace(['-', '.'], "_")
}

pub fn render_results_json(results: &[BenchmarkResult]) -> String {
    let mut out = String::from("{\"results\":[");
    for (index, result) in results.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        out.push_str(&format!(
            "{{\"case\":\"{}\",\"file\":\"{}\",\"iterations\":{},\"unit_work\":{},\"samples_ns\":[{}],\"median_ns\":{},\"p95_ns\":{},\"p99_ns\":{},\"per_call_ns\":{:.3},\"units_per_second\":{:.3}}}",
            result.case_name,
            crate::toolchain::escape_json(&result.file_path),
            result.iterations,
            result.unit_work,
            result.samples_ns.iter().map(|sample| sample.to_string()).collect::<Vec<_>>().join(","),
            result.median_ns,
            result.p95_ns,
            result.p99_ns,
            result.per_call_ns,
            result.units_per_second
        ));
    }
    out.push_str("]}");
    out
}

fn parse_samples(stdout: &str) -> Result<Vec<u64>, String> {
    let mut samples = Vec::new();
    for line in stdout.lines() {
        if let Some(value) = line.strip_prefix("SAMPLE=") {
            let sample = value
                .trim()
                .parse::<u64>()
                .map_err(|error| format!("invalid sample {value}: {error}"))?;
            samples.push(sample);
        }
    }
    if samples.is_empty() {
        return Err(format!("benchmark produced no samples:\n{stdout}"));
    }
    Ok(samples)
}

fn median(mut values: Vec<u64>) -> u64 {
    values.sort_unstable_by(|left, right| left.cmp(right));
    values[values.len() / 2]
}

fn percentile(mut values: Vec<u64>, percentile: usize) -> u64 {
    values.sort_unstable();
    let index = ((values.len().saturating_sub(1)) * percentile) / 100;
    values[index]
}

fn benchmark_cases() -> Vec<BenchmarkCase> {
    let sum_abs: Vec<DataValue> = (-10_000..10_000)
        .map(|value| DataValue::Int(value as i128))
        .collect();
    let dot_xs: Vec<DataValue> = (0..15_000)
        .map(|value| DataValue::Int(value as i128))
        .collect();
    let dot_ys: Vec<DataValue> = (30_000..45_000)
        .map(|value| DataValue::Int(value as i128))
        .collect();
    vec![
        BenchmarkCase {
            case_name: "sum_abs",
            file_name: "sum_abs.mira",
            function_name: "sum_abs",
            arguments: vec![("xs".to_string(), DataValue::Array(sum_abs))],
            iterations: 50,
            unit_work: 20_000,
        },
        BenchmarkCase {
            case_name: "dot_product",
            file_name: "dot_product.mira",
            function_name: "dot_product",
            arguments: vec![
                ("xs".to_string(), DataValue::Array(dot_xs)),
                ("ys".to_string(), DataValue::Array(dot_ys)),
            ],
            iterations: 40,
            unit_work: 15_000,
        },
        BenchmarkCase {
            case_name: "fib_iter",
            file_name: "fib_iter.mira",
            function_name: "fib_iter",
            arguments: vec![("n".to_string(), DataValue::Int(60))],
            iterations: 100_000,
            unit_work: 60,
        },
    ]
}
