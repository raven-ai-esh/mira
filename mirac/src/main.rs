mod asm_arm64;
mod asm_x86_64;
mod ast;
mod ast_json;
mod bench;
mod binary_ir;
mod codegen_c;
mod format;
mod lowered_bytecode;
mod lowered_exec;
mod lowered_validate;
mod machine_ir;
mod parser;
mod patch;
mod runtime_support_c;
mod toolchain;
mod types;
mod validate;

use std::env;
use std::fs;
use std::path::PathBuf;

use ast_json::{ast_schema_json, parse_program_json, render_program_json};
use bench::{
    render_results_json, run_arm64_benchmark_suite, run_benchmark_suite,
    run_bytecode_benchmark_suite, run_direct_benchmark_suite, run_single_source_arm64_benchmark,
    run_single_source_benchmark, run_single_source_bytecode_benchmark,
    run_single_source_x86_64_benchmark, run_x86_64_benchmark_suite,
};
use binary_ir::{decode_artifact, encode_program, BinaryArtifact};
use codegen_c::{emit_library, emit_test_harness, emit_test_harness_from_lowered};
use format::format_program;
use lowered_bytecode::compile_bytecode_program;
use lowered_bytecode::verify_lowered_tests_portably;
use lowered_exec::lower_program_for_direct_exec;
use lowered_validate::validate_lowered_program;
use patch::apply_patch_text;
use toolchain::{
    compile_c_source, compile_clang_bundle_for_target_with_runtime_support,
    compile_clang_bundle_with_runtime_support, compile_and_run_x86_64_bundle_in_docker_with_runtime_support,
    load_and_validate, render_diagnostics, run_binary,
};
use validate::validate_program;

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let mut args = env::args().skip(1);
    let Some(command) = args.next() else {
        print_help();
        return Ok(());
    };
    match command.as_str() {
        "check" => {
            let path = PathBuf::from(
                args.next()
                    .ok_or_else(|| "usage: mirac check <source>".to_string())?,
            );
            let program = load_and_validate(&path)?;
            println!("{}", render_check_ok(&path.display().to_string(), &program));
        }
        "emit-c" => {
            let source = PathBuf::from(
                args.next()
                    .ok_or_else(|| "usage: mirac emit-c <source> <output>".to_string())?,
            );
            let output = PathBuf::from(
                args.next()
                    .ok_or_else(|| "usage: mirac emit-c <source> <output>".to_string())?,
            );
            let program = load_and_validate(&source)?;
            let c_source = emit_library(&program)?;
            fs::write(&output, c_source)
                .map_err(|error| format!("failed to write {}: {error}", output.display()))?;
            println!("{{\"ok\":true,\"output\":\"{}\"}}", output.display());
        }
        "emit-asm-arm64" => {
            let source = PathBuf::from(
                args.next()
                    .ok_or_else(|| "usage: mirac emit-asm-arm64 <source> <output>".to_string())?,
            );
            let output = PathBuf::from(
                args.next()
                    .ok_or_else(|| "usage: mirac emit-asm-arm64 <source> <output>".to_string())?,
            );
            if !asm_arm64::supports_arm64_asm_backend() {
                return Err(
                    "arm64 asm backend is only supported on aarch64 macOS hosts".to_string()
                );
            }
            let program = load_and_validate(&source)?;
            let lowered = lowered_exec::lower_program_for_direct_exec(&program)?;
            let bytecode = lowered_bytecode::compile_bytecode_program(&lowered)?;
            let asm_source = asm_arm64::emit_arm64_library(&bytecode)?;
            fs::write(&output, asm_source)
                .map_err(|error| format!("failed to write {}: {error}", output.display()))?;
            println!("{{\"ok\":true,\"output\":\"{}\"}}", output.display());
        }
        "emit-asm-x86_64" => {
            let source = PathBuf::from(args.next().ok_or_else(|| {
                "usage: mirac emit-asm-x86_64 <source> <target-triple> <output>".to_string()
            })?);
            let triple = args.next().ok_or_else(|| {
                "usage: mirac emit-asm-x86_64 <source> <target-triple> <output>".to_string()
            })?;
            let output = PathBuf::from(args.next().ok_or_else(|| {
                "usage: mirac emit-asm-x86_64 <source> <target-triple> <output>".to_string()
            })?);
            let target = asm_x86_64::target_from_triple(&triple)?;
            let program = load_and_validate(&source)?;
            let lowered = lowered_exec::lower_program_for_direct_exec(&program)?;
            let bytecode = lowered_bytecode::compile_bytecode_program(&lowered)?;
            let asm_source = asm_x86_64::emit_x86_64_library(&bytecode, target)?;
            fs::write(&output, asm_source)
                .map_err(|error| format!("failed to write {}: {error}", output.display()))?;
            println!(
                "{{\"ok\":true,\"output\":\"{}\",\"target\":\"{}\"}}",
                output.display(),
                triple
            );
        }
        "test" => {
            let source = PathBuf::from(
                args.next()
                    .ok_or_else(|| "usage: mirac test <source>".to_string())?,
            );
            let program = load_and_validate(&source)?;
            let c_source = emit_test_harness(&program)?;
            let stem = source
                .file_stem()
                .and_then(|item| item.to_str())
                .ok_or_else(|| format!("invalid source file name {}", source.display()))?;
            let binary = compile_c_source(&format!("test_{stem}"), &c_source)?;
            let output = run_binary(&binary)?;
            if !output.status.success() {
                return Err(format!(
                    "native tests failed for {}:\n{}\n{}",
                    source.display(),
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                ));
            }
            print!("{}", String::from_utf8_lossy(&output.stdout));
        }
        "test-asm-arm64" => {
            let source = PathBuf::from(
                args.next()
                    .ok_or_else(|| "usage: mirac test-asm-arm64 <source>".to_string())?,
            );
            if !asm_arm64::supports_arm64_asm_backend() {
                return Err(
                    "arm64 asm backend is only supported on aarch64 macOS hosts".to_string()
                );
            }
            let program = load_and_validate(&source)?;
            let lowered = lower_program_for_direct_exec(&program)?;
            let bytecode = compile_bytecode_program(&lowered)?;
            let asm_source = asm_arm64::emit_arm64_library(&bytecode)?;
            let harness = emit_test_harness_from_lowered(&lowered);
            let stem = source
                .file_stem()
                .and_then(|item| item.to_str())
                .ok_or_else(|| format!("invalid source file name {}", source.display()))?;
            let binary = compile_clang_bundle_with_runtime_support(
                &format!("test_asm_arm64_{stem}"),
                &[("s", &asm_source), ("c", &harness)],
                &["-std=c11"],
            )?;
            let output = run_binary(&binary)?;
            if !output.status.success() {
                return Err(format!(
                    "arm64 asm tests failed for {}:\n{}\n{}",
                    source.display(),
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                ));
            }
            print!("{}", String::from_utf8_lossy(&output.stdout));
        }
        "test-asm-x86_64" => {
            let source = PathBuf::from(args.next().ok_or_else(|| {
                "usage: mirac test-asm-x86_64 <source> <target-triple>".to_string()
            })?);
            let triple = args.next().ok_or_else(|| {
                "usage: mirac test-asm-x86_64 <source> <target-triple>".to_string()
            })?;
            let target = asm_x86_64::target_from_triple(&triple)?;
            let program = load_and_validate(&source)?;
            let lowered = lower_program_for_direct_exec(&program)?;
            let bytecode = compile_bytecode_program(&lowered)?;
            let asm_source = asm_x86_64::emit_x86_64_library(&bytecode, target)?;
            let harness = emit_test_harness_from_lowered(&lowered);
            let stem = source
                .file_stem()
                .and_then(|item| item.to_str())
                .ok_or_else(|| format!("invalid source file name {}", source.display()))?;
            let output = if target.flavor == asm_x86_64::X86_64ObjectFlavor::MachO {
                let binary = compile_clang_bundle_for_target_with_runtime_support(
                    &format!("test_asm_x86_64_{stem}"),
                    &[("s", &asm_source), ("c", &harness)],
                    &["-std=c11"],
                    &triple,
                )?;
                run_binary(&binary)?
            } else {
                compile_and_run_x86_64_bundle_in_docker_with_runtime_support(
                    &format!("test_asm_x86_64_{stem}_{}", triple.replace(['-', '.'], "_")),
                    &[("s", &asm_source), ("c", &harness)],
                    &["-std=c11"],
                    &triple,
                )?
            };
            if !output.status.success() {
                return Err(format!(
                    "x86_64 asm tests failed for {} ({}):\n{}\n{}",
                    source.display(),
                    triple,
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                ));
            }
            print!("{}", String::from_utf8_lossy(&output.stdout));
        }
        "test-default" => {
            let source = PathBuf::from(
                args.next()
                    .ok_or_else(|| "usage: mirac test-default <source>".to_string())?,
            );
            let program = load_and_validate(&source)?;
            let lowered = lower_program_for_direct_exec(&program)?;
            if asm_arm64::supports_arm64_asm_backend() {
                let bytecode = compile_bytecode_program(&lowered)?;
                let asm_source = asm_arm64::emit_arm64_library(&bytecode)?;
                let harness = emit_test_harness_from_lowered(&lowered);
                let stem = source
                    .file_stem()
                    .and_then(|item| item.to_str())
                    .ok_or_else(|| format!("invalid source file name {}", source.display()))?;
                let binary = compile_clang_bundle_with_runtime_support(
                    &format!("test_default_arm64_{stem}"),
                    &[("s", &asm_source), ("c", &harness)],
                    &["-std=c11"],
                )?;
                let output = run_binary(&binary)?;
                if !output.status.success() {
                    return Err(format!(
                        "default arm64 tests failed for {}:\n{}\n{}",
                        source.display(),
                        String::from_utf8_lossy(&output.stdout),
                        String::from_utf8_lossy(&output.stderr)
                    ));
                }
                print!("{}", String::from_utf8_lossy(&output.stdout));
            } else if cfg!(all(target_arch = "x86_64", target_os = "macos")) {
                let bytecode = compile_bytecode_program(&lowered)?;
                let target = asm_x86_64::target_from_triple("x86_64-apple-macos13")?;
                let asm_source = asm_x86_64::emit_x86_64_library(&bytecode, target)?;
                let harness = emit_test_harness_from_lowered(&lowered);
                let stem = source
                    .file_stem()
                    .and_then(|item| item.to_str())
                    .ok_or_else(|| format!("invalid source file name {}", source.display()))?;
                let binary = compile_clang_bundle_for_target_with_runtime_support(
                    &format!("test_default_x86_64_{stem}"),
                    &[("s", &asm_source), ("c", &harness)],
                    &["-std=c11"],
                    "x86_64-apple-macos13",
                )?;
                let output = run_binary(&binary)?;
                if !output.status.success() {
                    return Err(format!(
                        "default x86_64 tests failed for {}:\n{}\n{}",
                        source.display(),
                        String::from_utf8_lossy(&output.stdout),
                        String::from_utf8_lossy(&output.stderr)
                    ));
                }
                print!("{}", String::from_utf8_lossy(&output.stdout));
            } else if let Some(summary) = verify_lowered_tests_portably(&lowered)? {
                println!("{summary}");
            } else {
                let c_source = emit_test_harness(&program)?;
                let stem = source
                    .file_stem()
                    .and_then(|item| item.to_str())
                    .ok_or_else(|| format!("invalid source file name {}", source.display()))?;
                let binary = compile_c_source(&format!("test_default_fallback_{stem}"), &c_source)?;
                let output = run_binary(&binary)?;
                if !output.status.success() {
                    return Err(format!(
                        "default fallback tests failed for {}:\n{}\n{}",
                        source.display(),
                        String::from_utf8_lossy(&output.stdout),
                        String::from_utf8_lossy(&output.stderr)
                    ));
                }
                print!("{}", String::from_utf8_lossy(&output.stdout));
            }
        }
        "emit-ast" => {
            let source = PathBuf::from(
                args.next()
                    .ok_or_else(|| "usage: mirac emit-ast <source> <output-json>".to_string())?,
            );
            let output = PathBuf::from(
                args.next()
                    .ok_or_else(|| "usage: mirac emit-ast <source> <output-json>".to_string())?,
            );
            let program = load_and_validate(&source)?;
            fs::write(&output, render_program_json(&program)?)
                .map_err(|error| format!("failed to write {}: {error}", output.display()))?;
            println!("{{\"ok\":true,\"output\":\"{}\"}}", output.display());
        }
        "check-ast" => {
            let path = PathBuf::from(
                args.next()
                    .ok_or_else(|| "usage: mirac check-ast <ast-json>".to_string())?,
            );
            let source = fs::read_to_string(&path)
                .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
            let program = parse_program_json(&source)?;
            let diagnostics = validate_program(&program);
            if !diagnostics.is_empty() {
                return Err(render_diagnostics(&diagnostics));
            }
            println!("{}", render_check_ok(&path.display().to_string(), &program));
        }
        "emit-binary" => {
            let source =
                PathBuf::from(args.next().ok_or_else(|| {
                    "usage: mirac emit-binary <source> <output-mirb>".to_string()
                })?);
            let output =
                PathBuf::from(args.next().ok_or_else(|| {
                    "usage: mirac emit-binary <source> <output-mirb>".to_string()
                })?);
            let program = load_and_validate(&source)?;
            fs::write(&output, encode_program(&program)?)
                .map_err(|error| format!("failed to write {}: {error}", output.display()))?;
            println!("{{\"ok\":true,\"output\":\"{}\"}}", output.display());
        }
        "check-binary" => {
            let path = PathBuf::from(
                args.next()
                    .ok_or_else(|| "usage: mirac check-binary <mirb>".to_string())?,
            );
            let bytes = fs::read(&path)
                .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
            match decode_artifact(&bytes)? {
                BinaryArtifact::LegacyProgram(program) => {
                    let diagnostics = validate_program(&program);
                    if !diagnostics.is_empty() {
                        return Err(render_diagnostics(&diagnostics));
                    }
                    println!("{}", render_check_ok(&path.display().to_string(), &program));
                }
                BinaryArtifact::LoweredProgram(program) => {
                    let diagnostics = validate_lowered_program(&program);
                    if !diagnostics.is_empty() {
                        return Err(render_lowered_diagnostics(&diagnostics));
                    }
                    println!(
                        "{}",
                        render_lowered_check_ok(&path.display().to_string(), &program)
                    );
                }
            }
        }
        "test-ast" => {
            let path = PathBuf::from(
                args.next()
                    .ok_or_else(|| "usage: mirac test-ast <ast-json>".to_string())?,
            );
            let source = fs::read_to_string(&path)
                .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
            let program = parse_program_json(&source)?;
            verify_program_tests(&program, &path)?;
        }
        "test-binary" => {
            let path = PathBuf::from(
                args.next()
                    .ok_or_else(|| "usage: mirac test-binary <mirb>".to_string())?,
            );
            let bytes = fs::read(&path)
                .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
            match decode_artifact(&bytes)? {
                BinaryArtifact::LegacyProgram(program) => verify_program_tests(&program, &path)?,
                BinaryArtifact::LoweredProgram(program) => {
                    verify_lowered_program_tests(&program, &path)?
                }
            }
        }
        "patch" => {
            let source = PathBuf::from(
                args.next()
                    .ok_or_else(|| "usage: mirac patch <source> <patch> <output>".to_string())?,
            );
            let patch_path = PathBuf::from(
                args.next()
                    .ok_or_else(|| "usage: mirac patch <source> <patch> <output>".to_string())?,
            );
            let output = PathBuf::from(
                args.next()
                    .ok_or_else(|| "usage: mirac patch <source> <patch> <output>".to_string())?,
            );
            let source_text = fs::read_to_string(&source)
                .map_err(|error| format!("failed to read {}: {error}", source.display()))?;
            let patch_text = fs::read_to_string(&patch_path)
                .map_err(|error| format!("failed to read {}: {error}", patch_path.display()))?;
            let program = parser::parse_program(&source_text)?;
            let patched = apply_patch_text(&program, &patch_text)?;
            let diagnostics = validate_program(&patched);
            if !diagnostics.is_empty() {
                return Err(render_diagnostics(&diagnostics));
            }
            fs::write(&output, format_program(&patched))
                .map_err(|error| format!("failed to write {}: {error}", output.display()))?;
            println!("{{\"ok\":true,\"output\":\"{}\"}}", output.display());
        }
        "ast-schema" => {
            print!("{}", ast_schema_json());
        }
        "bench-suite" => {
            let examples_dir = PathBuf::from(args.next().ok_or_else(|| {
                "usage: mirac bench-suite <examples-dir> <output-json>".to_string()
            })?);
            let output = PathBuf::from(args.next().ok_or_else(|| {
                "usage: mirac bench-suite <examples-dir> <output-json>".to_string()
            })?);
            let results = run_benchmark_suite(&examples_dir, Some(&output))?;
            println!("{}", render_results_json(&results));
        }
        "bench-suite-direct" => {
            let examples_dir = PathBuf::from(args.next().ok_or_else(|| {
                "usage: mirac bench-suite-direct <examples-dir> <output-json>".to_string()
            })?);
            let output = PathBuf::from(args.next().ok_or_else(|| {
                "usage: mirac bench-suite-direct <examples-dir> <output-json>".to_string()
            })?);
            let results = run_direct_benchmark_suite(&examples_dir, Some(&output))?;
            println!("{}", render_results_json(&results));
        }
        "bench-suite-bytecode" => {
            let examples_dir = PathBuf::from(args.next().ok_or_else(|| {
                "usage: mirac bench-suite-bytecode <examples-dir> <output-json>".to_string()
            })?);
            let output = PathBuf::from(args.next().ok_or_else(|| {
                "usage: mirac bench-suite-bytecode <examples-dir> <output-json>".to_string()
            })?);
            let results = run_bytecode_benchmark_suite(&examples_dir, Some(&output))?;
            println!("{}", render_results_json(&results));
        }
        "bench-suite-asm-arm64" => {
            let examples_dir = PathBuf::from(args.next().ok_or_else(|| {
                "usage: mirac bench-suite-asm-arm64 <examples-dir> <output-json>".to_string()
            })?);
            let output = PathBuf::from(args.next().ok_or_else(|| {
                "usage: mirac bench-suite-asm-arm64 <examples-dir> <output-json>".to_string()
            })?);
            let results = run_arm64_benchmark_suite(&examples_dir, Some(&output))?;
            println!("{}", render_results_json(&results));
        }
        "bench-suite-asm-x86_64" => {
            let examples_dir = PathBuf::from(args.next().ok_or_else(|| {
                "usage: mirac bench-suite-asm-x86_64 <examples-dir> <target-triple> <output-json>"
                    .to_string()
            })?);
            let triple = args.next().ok_or_else(|| {
                "usage: mirac bench-suite-asm-x86_64 <examples-dir> <target-triple> <output-json>"
                    .to_string()
            })?;
            let output = PathBuf::from(args.next().ok_or_else(|| {
                "usage: mirac bench-suite-asm-x86_64 <examples-dir> <target-triple> <output-json>"
                    .to_string()
            })?);
            let results = run_x86_64_benchmark_suite(&examples_dir, &triple, Some(&output))?;
            println!("{}", render_results_json(&results));
        }
        "bench-source" => {
            let source = PathBuf::from(args.next().ok_or_else(|| {
                "usage: mirac bench-source <source> <function> <iterations> <unit-work> <output-json>".to_string()
            })?);
            let function = args.next().ok_or_else(|| {
                "usage: mirac bench-source <source> <function> <iterations> <unit-work> <output-json>".to_string()
            })?;
            let iterations = args
                .next()
                .ok_or_else(|| {
                    "usage: mirac bench-source <source> <function> <iterations> <unit-work> <output-json>".to_string()
                })?
                .parse::<usize>()
                .map_err(|error| format!("invalid iterations: {error}"))?;
            let unit_work = args
                .next()
                .ok_or_else(|| {
                    "usage: mirac bench-source <source> <function> <iterations> <unit-work> <output-json>".to_string()
                })?
                .parse::<usize>()
                .map_err(|error| format!("invalid unit-work: {error}"))?;
            let output = PathBuf::from(args.next().ok_or_else(|| {
                "usage: mirac bench-source <source> <function> <iterations> <unit-work> <output-json>".to_string()
            })?);
            let results = run_single_source_benchmark(
                &source,
                &function,
                iterations,
                unit_work,
                Some(&output),
            )?;
            println!("{}", render_results_json(&results));
        }
        "bench-source-asm-arm64" => {
            let source = PathBuf::from(args.next().ok_or_else(|| {
                "usage: mirac bench-source-asm-arm64 <source> <function> <iterations> <unit-work> <output-json>".to_string()
            })?);
            let function = args.next().ok_or_else(|| {
                "usage: mirac bench-source-asm-arm64 <source> <function> <iterations> <unit-work> <output-json>".to_string()
            })?;
            let iterations = args
                .next()
                .ok_or_else(|| {
                    "usage: mirac bench-source-asm-arm64 <source> <function> <iterations> <unit-work> <output-json>".to_string()
                })?
                .parse::<usize>()
                .map_err(|error| format!("invalid iterations: {error}"))?;
            let unit_work = args
                .next()
                .ok_or_else(|| {
                    "usage: mirac bench-source-asm-arm64 <source> <function> <iterations> <unit-work> <output-json>".to_string()
                })?
                .parse::<usize>()
                .map_err(|error| format!("invalid unit-work: {error}"))?;
            let output = PathBuf::from(args.next().ok_or_else(|| {
                "usage: mirac bench-source-asm-arm64 <source> <function> <iterations> <unit-work> <output-json>".to_string()
            })?);
            let results = run_single_source_arm64_benchmark(
                &source,
                &function,
                iterations,
                unit_work,
                Some(&output),
            )?;
            println!("{}", render_results_json(&results));
        }
        "bench-source-asm-x86_64" => {
            let source = PathBuf::from(args.next().ok_or_else(|| {
                "usage: mirac bench-source-asm-x86_64 <source> <target-triple> <function> <iterations> <unit-work> <output-json>".to_string()
            })?);
            let triple = args.next().ok_or_else(|| {
                "usage: mirac bench-source-asm-x86_64 <source> <target-triple> <function> <iterations> <unit-work> <output-json>".to_string()
            })?;
            let function = args.next().ok_or_else(|| {
                "usage: mirac bench-source-asm-x86_64 <source> <target-triple> <function> <iterations> <unit-work> <output-json>".to_string()
            })?;
            let iterations = args
                .next()
                .ok_or_else(|| {
                    "usage: mirac bench-source-asm-x86_64 <source> <target-triple> <function> <iterations> <unit-work> <output-json>".to_string()
                })?
                .parse::<usize>()
                .map_err(|error| format!("invalid iterations: {error}"))?;
            let unit_work = args
                .next()
                .ok_or_else(|| {
                    "usage: mirac bench-source-asm-x86_64 <source> <target-triple> <function> <iterations> <unit-work> <output-json>".to_string()
                })?
                .parse::<usize>()
                .map_err(|error| format!("invalid unit-work: {error}"))?;
            let output = PathBuf::from(args.next().ok_or_else(|| {
                "usage: mirac bench-source-asm-x86_64 <source> <target-triple> <function> <iterations> <unit-work> <output-json>".to_string()
            })?);
            let results = run_single_source_x86_64_benchmark(
                &source,
                &triple,
                &function,
                iterations,
                unit_work,
                Some(&output),
            )?;
            println!("{}", render_results_json(&results));
        }
        "bench-source-default" => {
            let source = PathBuf::from(args.next().ok_or_else(|| {
                "usage: mirac bench-source-default <source> <function> <iterations> <unit-work> <output-json>".to_string()
            })?);
            let function = args.next().ok_or_else(|| {
                "usage: mirac bench-source-default <source> <function> <iterations> <unit-work> <output-json>".to_string()
            })?;
            let iterations = args
                .next()
                .ok_or_else(|| {
                    "usage: mirac bench-source-default <source> <function> <iterations> <unit-work> <output-json>".to_string()
                })?
                .parse::<usize>()
                .map_err(|error| format!("invalid iterations: {error}"))?;
            let unit_work = args
                .next()
                .ok_or_else(|| {
                    "usage: mirac bench-source-default <source> <function> <iterations> <unit-work> <output-json>".to_string()
                })?
                .parse::<usize>()
                .map_err(|error| format!("invalid unit-work: {error}"))?;
            let output = PathBuf::from(args.next().ok_or_else(|| {
                "usage: mirac bench-source-default <source> <function> <iterations> <unit-work> <output-json>".to_string()
            })?);
            let results = if asm_arm64::supports_arm64_asm_backend() {
                run_single_source_arm64_benchmark(
                    &source,
                    &function,
                    iterations,
                    unit_work,
                    Some(&output),
                )?
            } else if cfg!(all(target_arch = "x86_64", target_os = "macos")) {
                run_single_source_x86_64_benchmark(
                    &source,
                    "x86_64-apple-macos13",
                    &function,
                    iterations,
                    unit_work,
                    Some(&output),
                )?
            } else {
                run_single_source_bytecode_benchmark(
                    &source,
                    &function,
                    iterations,
                    unit_work,
                    Some(&output),
                )?
            };
            println!("{}", render_results_json(&results));
        }
        "help" | "--help" | "-h" => print_help(),
        other => return Err(format!("unknown command {other}")),
    }
    Ok(())
}

fn print_help() {
    println!("mirac commands:");
    println!("  mirac check <source>");
    println!("  mirac emit-ast <source> <output-json>");
    println!("  mirac check-ast <ast-json>");
    println!("  mirac emit-binary <source> <output-mirb>");
    println!("  mirac check-binary <mirb>");
    println!("  mirac emit-c <source> <output>");
    println!("  mirac emit-asm-arm64 <source> <output>");
    println!("  mirac emit-asm-x86_64 <source> <target-triple> <output>");
    println!("  mirac test <source>");
    println!("  mirac test-asm-arm64 <source>");
    println!("  mirac test-asm-x86_64 <source> <target-triple>");
    println!("  mirac test-default <source>");
    println!("  mirac test-ast <ast-json>");
    println!("  mirac test-binary <mirb>");
    println!("  mirac patch <source> <patch> <output>");
    println!("  mirac ast-schema");
    println!("  mirac bench-suite <examples-dir> <output-json>");
    println!("  mirac bench-suite-direct <examples-dir> <output-json>");
    println!("  mirac bench-suite-bytecode <examples-dir> <output-json>");
    println!("  mirac bench-suite-asm-arm64 <examples-dir> <output-json>");
    println!("  mirac bench-suite-asm-x86_64 <examples-dir> <target-triple> <output-json>");
    println!("  mirac bench-source <source> <function> <iterations> <unit-work> <output-json>");
    println!(
        "  mirac bench-source-asm-arm64 <source> <function> <iterations> <unit-work> <output-json>"
    );
    println!("  mirac bench-source-asm-x86_64 <source> <target-triple> <function> <iterations> <unit-work> <output-json>");
    println!(
        "  mirac bench-source-default <source> <function> <iterations> <unit-work> <output-json>"
    );
}

fn render_check_ok(path: &str, program: &ast::Program) -> String {
    format!(
        "{{\"ok\":true,\"path\":\"{}\",\"module\":\"{}\",\"functions\":[{}]}}",
        path,
        program.module,
        program
            .functions
            .iter()
            .map(|function| format!("\"{}\"", function.name))
            .collect::<Vec<_>>()
            .join(",")
    )
}

fn render_lowered_check_ok(path: &str, program: &codegen_c::LoweredProgram) -> String {
    format!(
        "{{\"ok\":true,\"path\":\"{}\",\"module\":\"{}\",\"functions\":[{}],\"artifact\":\"lowered\"}}",
        path,
        program.module,
        program
            .functions
            .iter()
            .map(|function| format!("\"{}\"", function.name))
            .collect::<Vec<_>>()
            .join(",")
    )
}

fn render_lowered_diagnostics(diagnostics: &[String]) -> String {
    diagnostics.join("\n")
}

fn verify_program_tests(program: &ast::Program, path: &PathBuf) -> Result<(), String> {
    let diagnostics = validate_program(program);
    if !diagnostics.is_empty() {
        return Err(render_diagnostics(&diagnostics));
    }
    let lowered = lower_program_for_direct_exec(program)?;
    if let Some(summary) = verify_lowered_tests_portably(&lowered)? {
        println!("{summary}");
        return Ok(());
    }
    let c_source = emit_test_harness(program)?;
    let stem = path
        .file_stem()
        .and_then(|item| item.to_str())
        .ok_or_else(|| format!("invalid source file name {}", path.display()))?;
    let binary = compile_c_source(&format!("test_{stem}"), &c_source)?;
    let output = run_binary(&binary)?;
    if !output.status.success() {
        return Err(format!(
            "native tests failed for {}:\n{}\n{}",
            path.display(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    print!("{}", String::from_utf8_lossy(&output.stdout));
    Ok(())
}

fn verify_lowered_program_tests(
    program: &codegen_c::LoweredProgram,
    path: &PathBuf,
) -> Result<(), String> {
    if let Some(summary) = verify_lowered_tests_portably(program)? {
        println!("{summary}");
        return Ok(());
    }
    let c_source = emit_test_harness_from_lowered(program);
    let stem = path
        .file_stem()
        .and_then(|item| item.to_str())
        .ok_or_else(|| format!("invalid source file name {}", path.display()))?;
    let binary = compile_c_source(&format!("test_{stem}"), &c_source)?;
    let output = run_binary(&binary)?;
    if !output.status.success() {
        return Err(format!(
            "native tests failed for {}:\n{}\n{}",
            path.display(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    print!("{}", String::from_utf8_lossy(&output.stdout));
    Ok(())
}
