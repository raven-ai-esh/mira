use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use crate::runtime_support_c::emit_portable_runtime_support_c;

use crate::ast::Program;
use crate::parser::parse_program;
use crate::validate::validate_program;

pub fn load_program(path: &Path) -> Result<Program, String> {
    let source = fs::read_to_string(path)
        .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
    parse_program(&source)
}

pub fn load_and_validate(path: &Path) -> Result<Program, String> {
    let program = load_program(path)?;
    let diagnostics = validate_program(&program);
    if diagnostics.is_empty() {
        Ok(program)
    } else {
        Err(render_diagnostics(&diagnostics))
    }
}

pub fn compile_c_source(stem: &str, c_source: &str) -> Result<PathBuf, String> {
    compile_clang_bundle_with_runtime_support(stem, &[("c", c_source)], &["-std=c11"])
}

#[allow(dead_code)]
pub fn compile_clang_bundle_for_target(
    stem: &str,
    sources: &[(&str, &str)],
    extra_args: &[&str],
    target: &str,
) -> Result<PathBuf, String> {
    compile_clang_bundle_with_target(stem, sources, extra_args, Some(target), false)
}

pub fn compile_clang_bundle(
    stem: &str,
    sources: &[(&str, &str)],
    extra_args: &[&str],
) -> Result<PathBuf, String> {
    compile_clang_bundle_with_target(stem, sources, extra_args, None, false)
}

pub fn compile_clang_bundle_with_runtime_support(
    stem: &str,
    sources: &[(&str, &str)],
    extra_args: &[&str],
) -> Result<PathBuf, String> {
    let runtime_support = emit_portable_runtime_support_c();
    let mut bundle = Vec::with_capacity(sources.len() + 1);
    bundle.extend_from_slice(sources);
    bundle.push(("c", runtime_support.as_str()));
    compile_clang_bundle(stem, &bundle, extra_args)
}

pub fn compile_clang_bundle_for_target_with_runtime_support(
    stem: &str,
    sources: &[(&str, &str)],
    extra_args: &[&str],
    target: &str,
) -> Result<PathBuf, String> {
    let runtime_support = emit_portable_runtime_support_c();
    let mut bundle = Vec::with_capacity(sources.len() + 1);
    bundle.extend_from_slice(sources);
    bundle.push(("c", runtime_support.as_str()));
    compile_clang_bundle_for_target(stem, &bundle, extra_args, target)
}

#[allow(dead_code)]
pub fn compile_clang_object_bundle(
    stem: &str,
    sources: &[(&str, &str)],
    extra_args: &[&str],
    target: &str,
) -> Result<PathBuf, String> {
    compile_clang_bundle_with_target(stem, sources, extra_args, Some(target), true)
}

pub fn compile_and_run_x86_64_bundle_in_docker_with_runtime_support(
    stem: &str,
    sources: &[(&str, &str)],
    extra_args: &[&str],
    target: &str,
) -> Result<Output, String> {
    let runtime_support = emit_portable_runtime_support_c();
    let mut bundle = Vec::with_capacity(sources.len() + 1);
    bundle.extend_from_slice(sources);
    bundle.push(("c", runtime_support.as_str()));

    let artifacts_dir = artifacts_dir()?;
    let input_paths = write_bundle_sources(&artifacts_dir, stem, &bundle)?;
    let input_names = input_paths
        .iter()
        .map(|path| {
            path.file_name()
                .and_then(|value| value.to_str())
                .ok_or_else(|| format!("invalid bundle path {}", path.display()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let output_name = if target.contains("windows") {
        format!("{stem}.exe")
    } else {
        stem.to_string()
    };
    if target.contains("linux") {
        return run_linux_x86_64_bundle_in_docker(
            &artifacts_dir,
            &input_names,
            &output_name,
            extra_args,
        );
    }
    if target.contains("windows") {
        return run_windows_x86_64_bundle_in_docker(
            &artifacts_dir,
            &input_names,
            &output_name,
            extra_args,
        );
    }
    Err(format!(
        "docker x86_64 bundle runner does not support target triple {target}"
    ))
}

fn compile_clang_bundle_with_target(
    stem: &str,
    sources: &[(&str, &str)],
    extra_args: &[&str],
    target: Option<&str>,
    compile_only: bool,
) -> Result<PathBuf, String> {
    let artifacts_dir = artifacts_dir()?;
    let output_path = if compile_only {
        let extension = match target {
            Some(triple) if triple.contains("windows") => "obj",
            _ => "o",
        };
        artifacts_dir.join(format!("{stem}.{extension}"))
    } else {
        artifacts_dir.join(stem)
    };
    let input_paths = write_bundle_sources(&artifacts_dir, stem, sources)?;

    let mut command = Command::new("clang");
    command.arg("-O3");
    if let Some(target) = target {
        command.arg("-target").arg(target);
    }
    for arg in extra_args {
        command.arg(arg);
    }
    for input_path in &input_paths {
        command.arg(input_path);
    }
    if compile_only {
        command.arg("-c");
    }
    command.arg("-o").arg(&output_path);
    let output = command
        .output()
        .map_err(|error| format!("failed to invoke clang: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "clang failed for {}:\n{}",
            input_paths
                .iter()
                .map(|path| path.display().to_string())
                .collect::<Vec<_>>()
                .join(", "),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(output_path)
}

pub fn run_binary(path: &Path) -> Result<Output, String> {
    Command::new(path)
        .output()
        .map_err(|error| format!("failed to run {}: {error}", path.display()))
}

fn artifacts_dir() -> Result<PathBuf, String> {
    let artifacts_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("tmp")
        .join("mira-native");
    fs::create_dir_all(&artifacts_dir)
        .map_err(|error| format!("failed to create artifact dir: {error}"))?;
    Ok(artifacts_dir)
}

fn write_bundle_sources(
    artifacts_dir: &Path,
    stem: &str,
    sources: &[(&str, &str)],
) -> Result<Vec<PathBuf>, String> {
    let mut input_paths = Vec::new();
    for (index, (extension, source)) in sources.iter().enumerate() {
        let suffix = if sources.len() == 1 {
            String::new()
        } else {
            format!("-{index}")
        };
        let source_path = artifacts_dir.join(format!("{stem}{suffix}.{extension}"));
        fs::write(&source_path, source)
            .map_err(|error| format!("failed to write {}: {error}", source_path.display()))?;
        input_paths.push(source_path);
    }
    Ok(input_paths)
}

fn run_linux_x86_64_bundle_in_docker(
    artifacts_dir: &Path,
    input_names: &[&str],
    output_name: &str,
    extra_args: &[&str],
) -> Result<Output, String> {
    ensure_linux_x86_64_runner_image()?;
    let extra = extra_args.join(" ");
    let inputs = input_names.join(" ");
    let script = if extra.is_empty() {
        format!(
            "gcc -O3 -D_GNU_SOURCE -pthread {inputs} -o {output_name} && chmod +x {output_name} && ./{output_name}"
        )
    } else {
        format!(
            "gcc -O3 -D_GNU_SOURCE -pthread {extra} {inputs} -o {output_name} && chmod +x {output_name} && ./{output_name}"
        )
    };
    Command::new("docker")
        .args([
            "run",
            "--rm",
            "--platform",
            "linux/amd64",
            "-v",
            &format!("{}:/work", artifacts_dir.display()),
            "-w",
            "/work",
            LINUX_X86_64_RUNNER_IMAGE,
            "sh",
            "-lc",
            &script,
        ])
        .output()
        .map_err(|error| format!("failed to run linux x86_64 bundle in docker: {error}"))
}

const LINUX_X86_64_RUNNER_IMAGE: &str = "mira-x86_64-linux-runner:1.4.0";
const WINDOWS_X86_64_RUNNER_IMAGE: &str = "mira-x86_64-win64-runner:1.4.0";

fn ensure_linux_x86_64_runner_image() -> Result<(), String> {
    let inspect = Command::new("docker")
        .args(["image", "inspect", LINUX_X86_64_RUNNER_IMAGE])
        .output()
        .map_err(|error| format!("failed to inspect docker image: {error}"))?;
    if inspect.status.success() {
        return Ok(());
    }
    let image_dir = artifacts_dir()?.join("docker-linux-runner");
    fs::create_dir_all(&image_dir)
        .map_err(|error| format!("failed to create {}: {error}", image_dir.display()))?;
    let dockerfile = image_dir.join("Dockerfile");
    fs::write(
        &dockerfile,
        r#"FROM ubuntu:24.04
RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y build-essential sqlite3 \
 && rm -rf /var/lib/apt/lists/*
"#,
    )
    .map_err(|error| format!("failed to write {}: {error}", dockerfile.display()))?;
    let output = Command::new("docker")
        .args([
            "build",
            "--platform",
            "linux/amd64",
            "-t",
            LINUX_X86_64_RUNNER_IMAGE,
            image_dir
                .to_str()
                .ok_or_else(|| format!("invalid image dir {}", image_dir.display()))?,
        ])
        .output()
        .map_err(|error| format!("failed to build docker image: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "docker build failed for {}:\n{}",
            LINUX_X86_64_RUNNER_IMAGE,
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(())
}

fn run_windows_x86_64_bundle_in_docker(
    artifacts_dir: &Path,
    input_names: &[&str],
    output_name: &str,
    extra_args: &[&str],
) -> Result<Output, String> {
    ensure_windows_x86_64_runner_image()?;
    let extra = extra_args.join(" ");
    let inputs = input_names.join(" ");
    let script = if extra.is_empty() {
        format!(
            "x86_64-w64-mingw32-gcc -O3 {inputs} -lws2_32 -o {output_name} && WINEDEBUG=-all wine ./{output_name}"
        )
    } else {
        format!(
            "x86_64-w64-mingw32-gcc -O3 {extra} {inputs} -lws2_32 -o {output_name} && WINEDEBUG=-all wine ./{output_name}"
        )
    };
    Command::new("docker")
        .args([
            "run",
            "--rm",
            "--platform",
            "linux/amd64",
            "-v",
            &format!("{}:/work", artifacts_dir.display()),
            "-w",
            "/work",
            WINDOWS_X86_64_RUNNER_IMAGE,
            "sh",
            "-lc",
            &script,
        ])
        .output()
        .map_err(|error| format!("failed to run windows x86_64 bundle in docker: {error}"))
}

fn ensure_windows_x86_64_runner_image() -> Result<(), String> {
    let inspect = Command::new("docker")
        .args(["image", "inspect", WINDOWS_X86_64_RUNNER_IMAGE])
        .output()
        .map_err(|error| format!("failed to inspect docker image: {error}"))?;
    if inspect.status.success() {
        return Ok(());
    }
    let image_dir = artifacts_dir()?.join("docker-win64-runner");
    fs::create_dir_all(&image_dir)
        .map_err(|error| format!("failed to create {}: {error}", image_dir.display()))?;
    let dockerfile = image_dir.join("Dockerfile");
    fs::write(
        &dockerfile,
        r#"FROM ubuntu:24.04
RUN apt-get update \
 && DEBIAN_FRONTEND=noninteractive apt-get install -y mingw-w64 wine64 sqlite3 \
 && rm -rf /var/lib/apt/lists/*
"#,
    )
    .map_err(|error| format!("failed to write {}: {error}", dockerfile.display()))?;
    let output = Command::new("docker")
        .args([
            "build",
            "--platform",
            "linux/amd64",
            "-t",
            WINDOWS_X86_64_RUNNER_IMAGE,
            image_dir
                .to_str()
                .ok_or_else(|| format!("invalid image dir {}", image_dir.display()))?,
        ])
        .output()
        .map_err(|error| format!("failed to build docker image: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "docker build failed for {}:\n{}",
            WINDOWS_X86_64_RUNNER_IMAGE,
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(())
}

pub fn render_diagnostics(diagnostics: &[crate::ast::Diagnostic]) -> String {
    let mut out = String::new();
    for diagnostic in diagnostics {
        out.push_str(&format!(
            "{{\"phase\":\"{}\",\"node\":\"{}\",\"error_code\":\"{}\",\"message\":\"{}\"",
            diagnostic.phase, diagnostic.node, diagnostic.error_code, diagnostic.message
        ));
        if let Some(expected) = &diagnostic.expected {
            out.push_str(&format!(",\"expected\":\"{}\"", escape_json(expected)));
        }
        if let Some(observed) = &diagnostic.observed {
            out.push_str(&format!(",\"observed\":\"{}\"", escape_json(observed)));
        }
        if let Some(fix_hint) = &diagnostic.fix_hint {
            out.push_str(&format!(",\"fix_hint\":\"{}\"", escape_json(fix_hint)));
        }
        out.push_str("}\n");
    }
    out
}

pub fn escape_json(text: &str) -> String {
    text.replace('\\', "\\\\").replace('"', "\\\"")
}
