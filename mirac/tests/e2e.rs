use std::fs;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use mirac::{
    asm_arm64::emit_arm64_library,
    asm_x86_64::{emit_x86_64_library, target_from_triple},
    ast_json::{parse_program_json, render_program_json},
    bench::{
        run_arm64_benchmark_suite, run_benchmark_suite, run_bytecode_benchmark_suite,
        run_single_source_arm64_benchmark, run_single_source_benchmark,
        run_single_source_x86_64_benchmark, run_x86_64_benchmark_suite,
    },
    binary_ir::{decode_artifact, encode_program, BinaryArtifact},
    codegen_c::{
        emit_library, emit_test_harness, emit_test_harness_from_lowered, LoweredStatement,
        LoweredTerminator,
    },
    format::format_program,
    lowered_bytecode::{
        compile_bytecode_program, run_bytecode_function, verify_lowered_tests_portably,
    },
    lowered_exec::{
        benchmark_arg_values, lower_program_for_direct_exec, run_lowered_function, RuntimeValue,
    },
    lowered_validate::validate_lowered_program,
    machine_ir::{lower_bytecode_to_machine_program, validate_machine_program},
    parser::parse_program,
    patch::apply_patch_text,
    toolchain::{
        compile_and_run_x86_64_bundle_in_docker_with_runtime_support,
        compile_c_source, compile_clang_bundle_for_target_with_runtime_support,
        compile_clang_bundle_with_runtime_support, compile_clang_object_bundle, load_and_validate,
        run_binary,
    },
    types::DataValue,
    validate::validate_program,
};

fn examples_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("mira")
        .join("examples")
}

#[test]
fn runtime_realtime_gateway_service_example_runs_across_direct_bytecode_and_native_paths() {
    let direct_port = reserve_closed_port();
    let bytecode_port = reserve_closed_port();
    let native_port = reserve_closed_port();
    let source =
        fs::read_to_string(examples_dir().join("runtime_realtime_gateway_service.mira"))
            .expect("runtime_realtime_gateway_service example should exist");
    let client = thread::spawn(move || {
        for port in [direct_port, bytecode_port, native_port] {
            let deadline = Instant::now() + Duration::from_secs(60);
            loop {
                match TcpStream::connect(("127.0.0.1", port)) {
                    Ok(mut stream) => {
                        stream
                            .write_all(b"PING")
                            .expect("gateway client should write chunk");
                        let _ = stream.shutdown(std::net::Shutdown::Write);
                        let mut response = Vec::new();
                        stream
                            .read_to_end(&mut response)
                            .expect("gateway client should read response");
                        let text = String::from_utf8_lossy(&response);
                        assert!(text.contains("ACK!"), "gateway response missing ACK chunk");
                        assert!(text.contains("HB"), "gateway response missing heartbeat chunk");
                        break;
                    }
                    Err(error) if Instant::now() < deadline => {
                        let _ = error;
                        thread::sleep(Duration::from_millis(25));
                    }
                    Err(error) => panic!("gateway client connect failed: {error}"),
                }
            }
        }
    });

    let direct_source = source.replace("127.0.0.1:39621", &format!("127.0.0.1:{direct_port}"));
    let direct_program =
        parse_program(&direct_source).expect("direct realtime gateway program should parse");
    let diagnostics = validate_program(&direct_program);
    assert!(
        diagnostics.is_empty(),
        "runtime_realtime_gateway_service direct program should validate after port rewrite: {diagnostics:?}"
    );
    let direct_lowered =
        lower_program_for_direct_exec(&direct_program).expect("direct realtime gateway should lower");
    let direct_result = run_lowered_function(
        &direct_lowered,
        "serve_gateway_once",
        &std::collections::HashMap::new(),
    )
    .expect("direct realtime gateway should execute");
    assert_eq!(RuntimeValue::U8(80), direct_result);

    let bytecode_source =
        source.replace("127.0.0.1:39621", &format!("127.0.0.1:{bytecode_port}"));
    let bytecode_program =
        parse_program(&bytecode_source).expect("bytecode realtime gateway program should parse");
    let diagnostics = validate_program(&bytecode_program);
    assert!(
        diagnostics.is_empty(),
        "runtime_realtime_gateway_service bytecode program should validate after port rewrite: {diagnostics:?}"
    );
    let bytecode_lowered = lower_program_for_direct_exec(&bytecode_program)
        .expect("bytecode realtime gateway should lower");
    let bytecode = compile_bytecode_program(&bytecode_lowered)
        .expect("realtime gateway bytecode should compile");
    let bytecode_result = run_bytecode_function(
        &bytecode,
        "serve_gateway_once",
        &std::collections::HashMap::new(),
    )
    .expect("bytecode realtime gateway should execute");
    assert_eq!(RuntimeValue::U8(80), bytecode_result);

    let native_source = source.replace("127.0.0.1:39621", &format!("127.0.0.1:{native_port}"));
    let native_program =
        parse_program(&native_source).expect("native realtime gateway program should parse");
    let diagnostics = validate_program(&native_program);
    assert!(
        diagnostics.is_empty(),
        "runtime_realtime_gateway_service native program should validate after port rewrite: {diagnostics:?}"
    );
    let mut library =
        emit_library(&native_program).expect("realtime gateway native library should emit");
    library.push_str("int main(void) {\n");
    library.push_str("  return mira_func_serve_gateway_once() == 80u ? 0 : 1;\n");
    library.push_str("}\n");
    let binary = compile_c_source("itest_runtime_realtime_gateway_service", &library)
        .expect("clang should compile realtime gateway service");
    let output = run_binary(&binary).expect("realtime gateway binary should run");
    client.join().expect("gateway client thread should finish");
    assert!(
        output.status.success(),
        "runtime_realtime_gateway_service native run failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn runtime_session_resume_client_example_runs_across_direct_bytecode_and_native_paths() {
    let source =
        fs::read_to_string(examples_dir().join("runtime_session_resume_client.mira"))
            .expect("runtime_session_resume_client example should exist");
    let spawn_resume_server = |port: u16| {
        thread::spawn(move || {
            let listener =
                TcpListener::bind(("127.0.0.1", port)).expect("resume listener should bind");
            let (mut first, _) = listener.accept().expect("resume server should accept first");
            let mut first_chunk = [0u8; 4];
            first
                .read_exact(&mut first_chunk)
                .expect("resume server should read first chunk");
            assert_eq!(&first_chunk, b"HELO");
            let _ = first.shutdown(std::net::Shutdown::Both);
            drop(first);

            let (mut second, _) = listener.accept().expect("resume server should accept second");
            let mut second_chunk = [0u8; 4];
            second
                .read_exact(&mut second_chunk)
                .expect("resume server should read second chunk");
            assert_eq!(&second_chunk, b"PING");
            second
                .write_all(b"OK")
                .expect("resume server should write response");
            let _ = second.shutdown(std::net::Shutdown::Write);
        })
    };

    let direct_port = reserve_closed_port();
    let direct_server = spawn_resume_server(direct_port);
    let direct_source = source.replace("127.0.0.1:39622", &format!("127.0.0.1:{direct_port}"));
    let direct_program =
        parse_program(&direct_source).expect("direct resume client program should parse");
    let diagnostics = validate_program(&direct_program);
    assert!(
        diagnostics.is_empty(),
        "runtime_session_resume_client direct program should validate after port rewrite: {diagnostics:?}"
    );
    let direct_lowered =
        lower_program_for_direct_exec(&direct_program).expect("direct resume client should lower");
    let direct_result = run_lowered_function(
        &direct_lowered,
        "resume_client_once",
        &std::collections::HashMap::new(),
    )
    .expect("direct resume client should execute");
    assert_eq!(RuntimeValue::U8(79), direct_result);
    direct_server.join().expect("direct resume server should finish");

    let bytecode_port = reserve_closed_port();
    let bytecode_server = spawn_resume_server(bytecode_port);
    let bytecode_source =
        source.replace("127.0.0.1:39622", &format!("127.0.0.1:{bytecode_port}"));
    let bytecode_program =
        parse_program(&bytecode_source).expect("bytecode resume client program should parse");
    let diagnostics = validate_program(&bytecode_program);
    assert!(
        diagnostics.is_empty(),
        "runtime_session_resume_client bytecode program should validate after port rewrite: {diagnostics:?}"
    );
    let bytecode_lowered = lower_program_for_direct_exec(&bytecode_program)
        .expect("bytecode resume client should lower");
    let bytecode =
        compile_bytecode_program(&bytecode_lowered).expect("resume client bytecode should compile");
    let bytecode_result = run_bytecode_function(
        &bytecode,
        "resume_client_once",
        &std::collections::HashMap::new(),
    )
    .expect("bytecode resume client should execute");
    assert_eq!(RuntimeValue::U8(79), bytecode_result);
    bytecode_server
        .join()
        .expect("bytecode resume server should finish");

    let native_port = reserve_closed_port();
    let native_server = spawn_resume_server(native_port);
    let native_source = source.replace("127.0.0.1:39622", &format!("127.0.0.1:{native_port}"));
    let native_program =
        parse_program(&native_source).expect("native resume client program should parse");
    let diagnostics = validate_program(&native_program);
    assert!(
        diagnostics.is_empty(),
        "runtime_session_resume_client native program should validate after port rewrite: {diagnostics:?}"
    );
    let mut library =
        emit_library(&native_program).expect("resume client native library should emit");
    library.push_str("int main(void) {\n");
    library.push_str("  return mira_func_resume_client_once() == 79u ? 0 : 1;\n");
    library.push_str("}\n");
    let binary = compile_c_source("itest_runtime_session_resume_client", &library)
        .expect("clang should compile resume client service");
    let output = run_binary(&binary).expect("resume client binary should run");
    native_server.join().expect("native resume server should finish");
    assert!(
        output.status.success(),
        "runtime_session_resume_client native run failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn runtime_many_idle_sessions_example_runs_across_direct_bytecode_and_native_paths() {
    let direct_port = reserve_closed_port();
    let bytecode_port = reserve_closed_port();
    let native_port = reserve_closed_port();
    let source = fs::read_to_string(examples_dir().join("runtime_many_idle_sessions.mira"))
        .expect("runtime_many_idle_sessions example should exist");
    let client = thread::spawn(move || {
        for port in [direct_port, bytecode_port, native_port] {
            let deadline = Instant::now() + Duration::from_secs(60);
            let mut connected = 0u8;
            while connected < 3u8 {
                match TcpStream::connect(("127.0.0.1", port)) {
                    Ok(stream) => {
                        thread::sleep(Duration::from_millis(10));
                        let _ = stream.shutdown(std::net::Shutdown::Both);
                        connected += 1;
                    }
                    Err(error) if Instant::now() < deadline => {
                        let _ = error;
                        thread::sleep(Duration::from_millis(25));
                    }
                    Err(error) => panic!("idle-session client connect failed: {error}"),
                }
            }
        }
    });

    let direct_source = source.replace("127.0.0.1:39623", &format!("127.0.0.1:{direct_port}"));
    let direct_program =
        parse_program(&direct_source).expect("direct idle-session program should parse");
    let diagnostics = validate_program(&direct_program);
    assert!(
        diagnostics.is_empty(),
        "runtime_many_idle_sessions direct program should validate after port rewrite: {diagnostics:?}"
    );
    let direct_lowered =
        lower_program_for_direct_exec(&direct_program).expect("direct idle-session should lower");
    let direct_result = run_lowered_function(
        &direct_lowered,
        "drain_three_idle_sessions",
        &std::collections::HashMap::new(),
    )
    .expect("direct idle-session service should execute");
    assert_eq!(RuntimeValue::U8(3), direct_result);

    let bytecode_source =
        source.replace("127.0.0.1:39623", &format!("127.0.0.1:{bytecode_port}"));
    let bytecode_program =
        parse_program(&bytecode_source).expect("bytecode idle-session program should parse");
    let diagnostics = validate_program(&bytecode_program);
    assert!(
        diagnostics.is_empty(),
        "runtime_many_idle_sessions bytecode program should validate after port rewrite: {diagnostics:?}"
    );
    let bytecode_lowered = lower_program_for_direct_exec(&bytecode_program)
        .expect("bytecode idle-session should lower");
    let bytecode =
        compile_bytecode_program(&bytecode_lowered).expect("idle-session bytecode should compile");
    let bytecode_result = run_bytecode_function(
        &bytecode,
        "drain_three_idle_sessions",
        &std::collections::HashMap::new(),
    )
    .expect("bytecode idle-session service should execute");
    assert_eq!(RuntimeValue::U8(3), bytecode_result);

    let native_source = source.replace("127.0.0.1:39623", &format!("127.0.0.1:{native_port}"));
    let native_program =
        parse_program(&native_source).expect("native idle-session program should parse");
    let diagnostics = validate_program(&native_program);
    assert!(
        diagnostics.is_empty(),
        "runtime_many_idle_sessions native program should validate after port rewrite: {diagnostics:?}"
    );
    let mut library =
        emit_library(&native_program).expect("idle-session native library should emit");
    library.push_str("int main(void) {\n");
    library.push_str("  return mira_func_drain_three_idle_sessions() == 3u ? 0 : 1;\n");
    library.push_str("}\n");
    let binary = compile_c_source("itest_runtime_many_idle_sessions", &library)
        .expect("clang should compile idle-session service");
    let output = run_binary(&binary).expect("idle-session binary should run");
    client.join().expect("idle-session client thread should finish");
    assert!(
        output.status.success(),
        "runtime_many_idle_sessions native run failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn runtime_direct_message_service_example_runs_across_direct_bytecode_and_native_paths() {
    let source =
        fs::read_to_string(examples_dir().join("runtime_direct_message_service.mira"))
            .expect("runtime_direct_message_service example should exist");

    let direct_program =
        parse_program(&source).expect("direct message program should parse");
    let diagnostics = validate_program(&direct_program);
    assert!(
        diagnostics.is_empty(),
        "runtime_direct_message_service direct program should validate: {diagnostics:?}"
    );
    let direct_lowered =
        lower_program_for_direct_exec(&direct_program).expect("direct message should lower");
    let direct_result = run_lowered_function(
        &direct_lowered,
        "direct_message_service_status",
        &std::collections::HashMap::new(),
    )
    .expect("direct message service should execute");
    assert_eq!(RuntimeValue::U8(1), direct_result);

    let bytecode_program =
        parse_program(&source).expect("bytecode direct message program should parse");
    let diagnostics = validate_program(&bytecode_program);
    assert!(
        diagnostics.is_empty(),
        "runtime_direct_message_service bytecode program should validate: {diagnostics:?}"
    );
    let bytecode_lowered = lower_program_for_direct_exec(&bytecode_program)
        .expect("bytecode direct message should lower");
    let bytecode =
        compile_bytecode_program(&bytecode_lowered).expect("direct message bytecode should compile");
    let bytecode_result = run_bytecode_function(
        &bytecode,
        "direct_message_service_status",
        &std::collections::HashMap::new(),
    )
    .expect("bytecode direct message service should execute");
    assert_eq!(RuntimeValue::U8(1), bytecode_result);

    let native_program =
        parse_program(&source).expect("native direct message program should parse");
    let diagnostics = validate_program(&native_program);
    assert!(
        diagnostics.is_empty(),
        "runtime_direct_message_service native program should validate: {diagnostics:?}"
    );
    let mut library =
        emit_library(&native_program).expect("direct message native library should emit");
    library.push_str("int main(void) {\n");
    library.push_str("  return mira_func_direct_message_service_status() == 1u ? 0 : 1;\n");
    library.push_str("}\n");
    let binary = compile_c_source("itest_runtime_direct_message_service", &library)
        .expect("clang should compile direct message service");
    let output = run_binary(&binary).expect("direct message binary should run");
    assert!(
        output.status.success(),
        "runtime_direct_message_service native run failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn runtime_room_fanout_service_example_runs_across_direct_bytecode_and_native_paths() {
    let source =
        fs::read_to_string(examples_dir().join("runtime_room_fanout_service.mira"))
            .expect("runtime_room_fanout_service example should exist");

    let direct_program =
        parse_program(&source).expect("direct room fanout program should parse");
    let diagnostics = validate_program(&direct_program);
    assert!(
        diagnostics.is_empty(),
        "runtime_room_fanout_service direct program should validate: {diagnostics:?}"
    );
    let direct_lowered =
        lower_program_for_direct_exec(&direct_program).expect("direct room fanout should lower");
    let direct_result = run_lowered_function(
        &direct_lowered,
        "room_fanout_service_status",
        &std::collections::HashMap::new(),
    )
    .expect("direct room fanout service should execute");
    assert_eq!(RuntimeValue::U8(1), direct_result);

    let bytecode_program =
        parse_program(&source).expect("bytecode room fanout program should parse");
    let diagnostics = validate_program(&bytecode_program);
    assert!(
        diagnostics.is_empty(),
        "runtime_room_fanout_service bytecode program should validate: {diagnostics:?}"
    );
    let bytecode_lowered = lower_program_for_direct_exec(&bytecode_program)
        .expect("bytecode room fanout should lower");
    let bytecode =
        compile_bytecode_program(&bytecode_lowered).expect("room fanout bytecode should compile");
    let bytecode_result = run_bytecode_function(
        &bytecode,
        "room_fanout_service_status",
        &std::collections::HashMap::new(),
    )
    .expect("bytecode room fanout service should execute");
    assert_eq!(RuntimeValue::U8(1), bytecode_result);

    let native_program =
        parse_program(&source).expect("native room fanout program should parse");
    let diagnostics = validate_program(&native_program);
    assert!(
        diagnostics.is_empty(),
        "runtime_room_fanout_service native program should validate: {diagnostics:?}"
    );
    let mut library =
        emit_library(&native_program).expect("room fanout native library should emit");
    library.push_str("int main(void) {\n");
    library.push_str("  return mira_func_room_fanout_service_status() == 1u ? 0 : 1;\n");
    library.push_str("}\n");
    let binary = compile_c_source("itest_runtime_room_fanout_service", &library)
        .expect("clang should compile room fanout service");
    let output = run_binary(&binary).expect("room fanout binary should run");
    assert!(
        output.status.success(),
        "runtime_room_fanout_service native run failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn runtime_offline_catchup_worker_example_runs_across_direct_bytecode_and_native_paths() {
    let source =
        fs::read_to_string(examples_dir().join("runtime_offline_catchup_worker.mira"))
            .expect("runtime_offline_catchup_worker example should exist");

    let direct_program =
        parse_program(&source).expect("direct offline catchup program should parse");
    let diagnostics = validate_program(&direct_program);
    assert!(
        diagnostics.is_empty(),
        "runtime_offline_catchup_worker direct program should validate: {diagnostics:?}"
    );
    let direct_lowered = lower_program_for_direct_exec(&direct_program)
        .expect("direct offline catchup should lower");
    let direct_result = run_lowered_function(
        &direct_lowered,
        "offline_catchup_worker_status",
        &std::collections::HashMap::new(),
    )
    .expect("direct offline catchup should execute");
    assert_eq!(RuntimeValue::U8(1), direct_result);

    let bytecode_program =
        parse_program(&source).expect("bytecode offline catchup program should parse");
    let diagnostics = validate_program(&bytecode_program);
    assert!(
        diagnostics.is_empty(),
        "runtime_offline_catchup_worker bytecode program should validate: {diagnostics:?}"
    );
    let bytecode_lowered = lower_program_for_direct_exec(&bytecode_program)
        .expect("bytecode offline catchup should lower");
    let bytecode = compile_bytecode_program(&bytecode_lowered)
        .expect("offline catchup bytecode should compile");
    let bytecode_result = run_bytecode_function(
        &bytecode,
        "offline_catchup_worker_status",
        &std::collections::HashMap::new(),
    )
    .expect("bytecode offline catchup should execute");
    assert_eq!(RuntimeValue::U8(1), bytecode_result);

    let native_program =
        parse_program(&source).expect("native offline catchup program should parse");
    let diagnostics = validate_program(&native_program);
    assert!(
        diagnostics.is_empty(),
        "runtime_offline_catchup_worker native program should validate: {diagnostics:?}"
    );
    let mut library =
        emit_library(&native_program).expect("offline catchup native library should emit");
    library.push_str("int main(void) {\n");
    library.push_str("  return mira_func_offline_catchup_worker_status() == 1u ? 0 : 1;\n");
    library.push_str("}\n");
    let binary = compile_c_source("itest_runtime_offline_catchup_worker", &library)
        .expect("clang should compile offline catchup service");
    let output = run_binary(&binary).expect("offline catchup binary should run");
    assert!(
        output.status.success(),
        "runtime_offline_catchup_worker native run failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn with_env_var<T>(name: &str, value: &str, f: impl FnOnce() -> T) -> T {
    static ENV_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();
    let _guard = ENV_MUTEX
        .get_or_init(|| Mutex::new(()))
        .lock()
        .expect("env mutex should lock");
    let previous = std::env::var_os(name);
    std::env::set_var(name, value);
    let result = f();
    match previous {
        Some(value) => std::env::set_var(name, value),
        None => std::env::remove_var(name),
    }
    result
}

fn reserve_closed_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("closed-port probe should bind")
        .local_addr()
        .expect("closed-port probe should have addr")
        .port()
}

fn unique_temp_dir(prefix: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "{prefix}_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos()
    ));
    fs::create_dir_all(&dir).expect("temp dir should be creatable");
    dir
}

fn assert_u8_service_source_runs_across_direct_portable_and_native(
    source: &str,
    function_name: &str,
    label: &str,
) {
    let program = parse_program(source).expect("rewritten program should parse");
    let diagnostics = validate_program(&program);
    assert!(
        diagnostics.is_empty(),
        "{label} should validate after source rewrite: {diagnostics:?}"
    );

    let lowered =
        lower_program_for_direct_exec(&program).expect("rewritten program should lower");
    let direct_result = run_lowered_function(
        &lowered,
        function_name,
        &std::collections::HashMap::new(),
    )
    .expect("direct lowered execution should succeed");
    assert_eq!(
        RuntimeValue::U8(1),
        direct_result,
        "{label} direct lowered result should be 1"
    );

    let summary = verify_lowered_tests_portably(&lowered)
        .expect("portable verification should succeed")
        .expect("rewritten program should stay on portable bytecode path");
    assert_eq!("portable bytecode tests passed: 1/1", summary);

    let harness = emit_test_harness(&program).expect("native test harness should emit");
    let binary =
        compile_c_source(&format!("itest_{label}"), &harness).expect("clang should compile");
    let output = run_binary(&binary).expect("native test harness should run");
    assert!(
        output.status.success(),
        "{label} native run failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn create_tls_materials(dir: &std::path::Path) -> (PathBuf, PathBuf) {
    let key = dir.join("server.key");
    let cert = dir.join("server.crt");
    let status = Command::new("openssl")
        .args([
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-subj",
            "/CN=localhost",
            "-keyout",
            key.to_str().expect("key path should be utf-8"),
            "-out",
            cert.to_str().expect("cert path should be utf-8"),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("openssl req should run");
    assert!(status.success(), "openssl req should succeed");
    (key, cert)
}

fn spawn_tls_http_server(port: u16, key: &std::path::Path, cert: &std::path::Path) -> Child {
    let child = Command::new("openssl")
        .args([
            "s_server",
            "-accept",
            &port.to_string(),
            "-key",
            key.to_str().expect("key path should be utf-8"),
            "-cert",
            cert.to_str().expect("cert path should be utf-8"),
            "-quiet",
            "-www",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("openssl s_server should spawn");
    thread::sleep(Duration::from_millis(350));
    child
}

fn rewrite_tls_server_source(
    source: &str,
    port: u16,
    key: &std::path::Path,
    cert: &std::path::Path,
) -> String {
    source
        .replace("127.0.0.1:39582", &format!("127.0.0.1:{port}"))
        .replace("127.0.0.1:39583", &format!("127.0.0.1:{port}"))
        .replace(
            "/tmp/mira_tls_server.key",
            key.to_str().expect("key path should be utf-8"),
        )
        .replace(
            "/tmp/mira_tls_server.crt",
            cert.to_str().expect("cert path should be utf-8"),
        )
}

fn run_tls_client_request(port: u16, request: &[u8]) -> Vec<u8> {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let child = Command::new("openssl")
            .args([
                "s_client",
                "-quiet",
                "-connect",
                &format!("127.0.0.1:{port}"),
                "-servername",
                "localhost",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn();
        match child {
            Ok(mut child) => {
                if let Some(mut stdin) = child.stdin.take() {
                    stdin
                        .write_all(request)
                        .expect("tls client should write request");
                }
                let output = child.wait_with_output().expect("tls client should finish");
                if !output.stdout.is_empty() {
                    return output.stdout;
                }
            }
            Err(error) if Instant::now() < deadline => {
                let _ = error;
            }
            Err(error) => panic!("tls client spawn failed: {error}"),
        }
        assert!(Instant::now() < deadline, "tls client deadline exceeded");
        thread::sleep(Duration::from_millis(50));
    }
}

fn postgres_dsn(port: u16) -> String {
    format!("postgresql://miratest:miratest@127.0.0.1:{port}/mira_test")
}

fn postgres_dsn_for_docker_client(port: u16) -> String {
    format!("postgresql://miratest:miratest@host.docker.internal:{port}/mira_test")
}

fn rewrite_postgres_source(source: &str, db_port: u16) -> String {
    source.replace(
        "postgresql://miratest:miratest@127.0.0.1:55432/mira_test",
        &postgres_dsn(db_port),
    )
}

fn rewrite_redis_source(source: &str, port: u16) -> String {
    source.replace("127.0.0.1:6389", &format!("127.0.0.1:{port}"))
}

fn start_postgres_container(port: u16) -> String {
    let name = format!(
        "mira-pg-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos()
    );
    let status = Command::new("docker")
        .args([
            "run",
            "-d",
            "--rm",
            "--name",
            &name,
            "-e",
            "POSTGRES_USER=miratest",
            "-e",
            "POSTGRES_PASSWORD=miratest",
            "-e",
            "POSTGRES_DB=mira_test",
            "-p",
            &format!("{port}:5432"),
            "postgres:16-alpine",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("docker run postgres should execute");
    assert!(status.success(), "docker postgres container should start");

    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        let status = Command::new("docker")
            .args([
                "exec",
                &name,
                "pg_isready",
                "-U",
                "miratest",
                "-d",
                "mira_test",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        if let Ok(status) = status {
            if status.success() {
                break;
            }
        }
        assert!(
            Instant::now() < deadline,
            "postgres container did not become ready"
        );
        thread::sleep(Duration::from_millis(250));
    }
    loop {
        let output = Command::new("docker")
            .args([
                "run",
                "--rm",
                "postgres:16-alpine",
                "psql",
                &postgres_dsn_for_docker_client(port),
                "-v",
                "ON_ERROR_STOP=1",
                "-At",
                "-c",
                "SELECT 1",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        if let Ok(status) = output {
            if status.success() {
                break;
            }
        }
        assert!(
            Instant::now() < deadline,
            "postgres container host port did not become ready"
        );
        thread::sleep(Duration::from_millis(250));
    }
    name
}

fn stop_postgres_container(name: &str) {
    let _ = Command::new("docker")
        .args(["rm", "-f", name])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}

struct PostgresContainerGuard {
    name: String,
}

impl PostgresContainerGuard {
    fn start(port: u16) -> Self {
        Self {
            name: start_postgres_container(port),
        }
    }
}

impl Drop for PostgresContainerGuard {
    fn drop(&mut self) {
        stop_postgres_container(&self.name);
    }
}

fn start_redis_container(port: u16) -> String {
    let name = format!(
        "mira-redis-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system time should be after unix epoch")
            .as_nanos()
    );
    let status = Command::new("docker")
        .args([
            "run",
            "-d",
            "--rm",
            "--name",
            &name,
            "-p",
            &format!("{port}:6379"),
            "redis:7-alpine",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("docker run redis should execute");
    assert!(status.success(), "docker redis container should start");
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let status = Command::new("docker")
            .args(["exec", &name, "redis-cli", "PING"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        if let Ok(status) = status {
            if status.success() {
                break;
            }
        }
        assert!(
            Instant::now() < deadline,
            "redis container did not become ready"
        );
        thread::sleep(Duration::from_millis(250));
    }
    loop {
        let output = Command::new("docker")
            .args([
                "run",
                "--rm",
                "redis:7-alpine",
                "redis-cli",
                "-h",
                "host.docker.internal",
                "-p",
                &port.to_string(),
                "PING",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        if let Ok(status) = output {
            if status.success() {
                break;
            }
        }
        assert!(
            Instant::now() < deadline,
            "redis host port did not become ready"
        );
        thread::sleep(Duration::from_millis(250));
    }
    name
}

fn stop_redis_container(name: &str) {
    let _ = Command::new("docker")
        .args(["rm", "-f", name])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
}

struct RedisContainerGuard {
    name: String,
}

impl RedisContainerGuard {
    fn start(port: u16) -> Self {
        Self {
            name: start_redis_container(port),
        }
    }
}

impl Drop for RedisContainerGuard {
    fn drop(&mut self) {
        stop_redis_container(&self.name);
    }
}

fn bit_ops_cases() -> [(u32, u32); 2] {
    [(10u32, 4_294_967_289u32), (0u32, 4_294_967_293u32)]
}

fn bit_ops_driver_source() -> String {
    r#"#include <stdint.h>
#include <stdio.h>

extern uint32_t mira_func_scramble_u32(uint32_t x);

int main(void) {
  struct {
    uint32_t input;
    uint32_t expected;
  } cases[] = {
    {10u, 4294967289u},
    {0u, 4294967293u},
  };
  for (unsigned i = 0; i < sizeof(cases) / sizeof(cases[0]); ++i) {
    uint32_t got = mira_func_scramble_u32(cases[i].input);
    if (got != cases[i].expected) {
      fprintf(stderr, "case %u failed: got %u expected %u\n", i, got, cases[i].expected);
      return 1;
    }
  }
  return 0;
}
"#
    .to_string()
}

fn arm64_runtime_harness_passes(file_name: &str) {
    if !cfg!(target_arch = "aarch64") || !cfg!(target_os = "macos") {
        return;
    }
    let source = examples_dir().join(file_name);
    let program = load_and_validate(&source).expect("program should validate");
    let lowered = lower_program_for_direct_exec(&program).expect("program should lower");
    let bytecode = compile_bytecode_program(&lowered).expect("bytecode should compile");
    let asm_source = emit_arm64_library(&bytecode).expect("arm64 asm should emit");
    let harness = emit_test_harness_from_lowered(&lowered);
    let stem = format!(
        "itest_{}_arm64_runtime",
        file_name.trim_end_matches(".mira")
    );
    let binary = compile_clang_bundle_with_runtime_support(
        &stem,
        &[("s", &asm_source), ("c", &harness)],
        &["-std=c11"],
    )
    .expect("arm64 runtime harness should compile");
    let output = run_binary(&binary).expect("binary should run");
    assert!(
        output.status.success(),
        "arm64 runtime harness failed for {}: stdout={} stderr={}",
        file_name,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn x86_64_runtime_harness_passes(file_name: &str) {
    if !cfg!(target_os = "macos") {
        return;
    }
    let source = examples_dir().join(file_name);
    let program = load_and_validate(&source).expect("program should validate");
    let lowered = lower_program_for_direct_exec(&program).expect("program should lower");
    let bytecode = compile_bytecode_program(&lowered).expect("bytecode should compile");
    let asm_source = emit_x86_64_library(
        &bytecode,
        target_from_triple("x86_64-apple-macos13").expect("target should resolve"),
    )
    .expect("x86_64 asm should emit");
    let harness = emit_test_harness_from_lowered(&lowered);
    let stem = format!(
        "itest_{}_x86_64_runtime",
        file_name.trim_end_matches(".mira")
    );
    let binary = compile_clang_bundle_for_target_with_runtime_support(
        &stem,
        &[("s", &asm_source), ("c", &harness)],
        &["-std=c11"],
        "x86_64-apple-macos13",
    )
    .expect("x86_64 runtime harness should compile");
    let output = run_binary(&binary).expect("binary should run");
    assert!(
        output.status.success(),
        "x86_64 runtime harness failed for {}: stdout={} stderr={}",
        file_name,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn x86_64_cross_target_runtime_harness_passes(file_name: &str, triple: &str) {
    if !cfg!(target_os = "macos") {
        return;
    }
    let source = examples_dir().join(file_name);
    let program = load_and_validate(&source).expect("program should validate");
    let lowered = lower_program_for_direct_exec(&program).expect("program should lower");
    let bytecode = compile_bytecode_program(&lowered).expect("bytecode should compile");
    let asm_source = emit_x86_64_library(
        &bytecode,
        target_from_triple(triple).expect("target should resolve"),
    )
    .expect("x86_64 asm should emit");
    let harness = emit_test_harness_from_lowered(&lowered);
    let stem = format!(
        "itest_{}_{}",
        file_name.trim_end_matches(".mira"),
        triple.replace(['-', '.'], "_")
    );
    let output = compile_and_run_x86_64_bundle_in_docker_with_runtime_support(
        &stem,
        &[("s", &asm_source), ("c", &harness)],
        &["-std=c11"],
        triple,
    )
    .expect("cross-target x86_64 runtime harness should compile and run");
    assert!(
        output.status.success(),
        "x86_64 runtime harness failed for {} on {}: stdout={} stderr={}",
        file_name,
        triple,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn arm64_runtime_net_harness_passes(source_text: &str) {
    if !cfg!(target_arch = "aarch64") || !cfg!(target_os = "macos") {
        return;
    }
    let program = parse_program(source_text).expect("runtime_net should parse");
    let diagnostics = validate_program(&program);
    assert!(
        diagnostics.is_empty(),
        "runtime_net arm64 asm program should validate: {diagnostics:?}"
    );
    let lowered = lower_program_for_direct_exec(&program).expect("runtime_net should lower");
    let bytecode = compile_bytecode_program(&lowered).expect("runtime_net bytecode");
    let asm_source = emit_arm64_library(&bytecode).expect("arm64 asm should emit");
    let harness = emit_test_harness_from_lowered(&lowered);
    let binary = compile_clang_bundle_with_runtime_support(
        "itest_runtime_net_arm64_runtime",
        &[("s", &asm_source), ("c", &harness)],
        &["-std=c11"],
    )
    .expect("arm64 runtime_net harness should compile");
    let output = run_binary(&binary).expect("binary should run");
    assert!(
        output.status.success(),
        "arm64 runtime_net harness failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn x86_64_runtime_net_harness_passes(source_text: &str) {
    if !cfg!(target_os = "macos") {
        return;
    }
    let program = parse_program(source_text).expect("runtime_net should parse");
    let diagnostics = validate_program(&program);
    assert!(
        diagnostics.is_empty(),
        "runtime_net x86_64 asm program should validate: {diagnostics:?}"
    );
    let lowered = lower_program_for_direct_exec(&program).expect("runtime_net should lower");
    let bytecode = compile_bytecode_program(&lowered).expect("runtime_net bytecode");
    let asm_source = emit_x86_64_library(
        &bytecode,
        target_from_triple("x86_64-apple-macos13").expect("target should resolve"),
    )
    .expect("x86_64 asm should emit");
    let harness = emit_test_harness_from_lowered(&lowered);
    let binary = compile_clang_bundle_for_target_with_runtime_support(
        "itest_runtime_net_x86_64_runtime",
        &[("s", &asm_source), ("c", &harness)],
        &["-std=c11"],
        "x86_64-apple-macos13",
    )
    .expect("x86_64 runtime_net harness should compile");
    let output = run_binary(&binary).expect("binary should run");
    assert!(
        output.status.success(),
        "x86_64 runtime_net harness failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn examples_compile_and_pass_native_tests() {
    for file_name in [
        "sum_abs.mira",
        "dot_product.mira",
        "fib_iter.mira",
        "match_dispatch.mira",
        "mul_add_f64.mira",
        "vec_pick.mira",
        "buf_alloc.mira",
        "bit_ops.mira",
        "runtime_caps.mira",
        "runtime_task_sleep.mira",
        "runtime_task_handle.mira",
        "runtime_fs.mira",
        "runtime_db_sqlite.mira",
        "runtime_task_handle.mira",
        "runtime_http_middleware.mira",
        "runtime_http_cookie_flow.mira",
        "runtime_http_header_cookie_json_api.mira",
        "runtime_http_response_model.mira",
        "runtime_http_crud_service.mira",
        "runtime_direct_message_service.mira",
        "runtime_room_fanout_service.mira",
        "runtime_offline_catchup_worker.mira",
        "runtime_http_header_body.mira",
        "runtime_http_query_std.mira",
        "runtime_json_extract.mira",
        "runtime_json_api_endpoint.mira",
        "runtime_service_api_template.mira",
        "runtime_service_worker_template.mira",
        "runtime_multiworker_http_service.mira",
        "runtime_deadline_job_system.mira",
        "runtime_worker_supervisor.mira",
        "runtime_spawn_bytes.mira",
        "runtime_ffi.mira",
        "runtime_spawn.mira",
        "point_manhattan.mira",
        "signal_enum.mira",
        "payload_message.mira",
        "payload_eq_literals.mira",
    ] {
        let source = examples_dir().join(file_name);
        let program = load_and_validate(&source).expect("program should validate");
        let harness = emit_test_harness(&program).expect("test harness should emit");
        let stem = file_name.trim_end_matches(".mira");
        let binary =
            compile_c_source(&format!("itest_{stem}"), &harness).expect("clang should compile");
        let output = run_binary(&binary).expect("binary should run");
        assert!(
            output.status.success(),
            "native tests failed: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[test]
fn benchmark_suite_produces_results() {
    let output = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("tmp")
        .join("mira-native")
        .join("itest-bench.json");
    let results =
        run_benchmark_suite(&examples_dir(), Some(&output)).expect("bench suite should run");
    assert_eq!(3, results.len());
    assert!(results.iter().all(|result| result.median_ns > 0));
    assert!(output.exists());
}

#[test]
fn direct_lowered_execution_runs_kernel_examples() {
    let cases = [
        ("sum_abs.mira", "sum_abs", RuntimeValue::I64(100000000)),
        (
            "dot_product.mira",
            "dot_product",
            RuntimeValue::I64(4499662502500),
        ),
        (
            "fib_iter.mira",
            "fib_iter",
            RuntimeValue::I64(1548008755920),
        ),
    ];
    for (file_name, function_name, expected) in cases {
        let source = examples_dir().join(file_name);
        let program = load_and_validate(&source).expect("program should validate");
        let lowered = lower_program_for_direct_exec(&program).expect("program should lower");
        let args = match function_name {
            "sum_abs" => benchmark_arg_values(
                &program,
                function_name,
                &[(
                    "xs".to_string(),
                    DataValue::Array(
                        (-10_000..10_000)
                            .map(|value| DataValue::Int(value as i128))
                            .collect(),
                    ),
                )],
            ),
            "dot_product" => benchmark_arg_values(
                &program,
                function_name,
                &[
                    (
                        "xs".to_string(),
                        DataValue::Array(
                            (0..15_000)
                                .map(|value| DataValue::Int(value as i128))
                                .collect(),
                        ),
                    ),
                    (
                        "ys".to_string(),
                        DataValue::Array(
                            (30_000..45_000)
                                .map(|value| DataValue::Int(value as i128))
                                .collect(),
                        ),
                    ),
                ],
            ),
            "fib_iter" => benchmark_arg_values(
                &program,
                function_name,
                &[("n".to_string(), DataValue::Int(60))],
            ),
            _ => unreachable!(),
        }
        .expect("benchmark args should lower to runtime values");
        let result =
            run_lowered_function(&lowered, function_name, &args).expect("direct execution works");
        assert_eq!(expected, result);
    }
}

#[test]
fn direct_lowered_execution_runs_bit_ops_example() {
    let source = examples_dir().join("bit_ops.mira");
    let program = load_and_validate(&source).expect("program should validate");
    let lowered = lower_program_for_direct_exec(&program).expect("program should lower");
    for (input, expected) in bit_ops_cases() {
        let args = benchmark_arg_values(
            &program,
            "scramble_u32",
            &[("x".to_string(), DataValue::Int(input as i128))],
        )
        .expect("bit op args should lower");
        let result =
            run_lowered_function(&lowered, "scramble_u32", &args).expect("direct execution works");
        assert_eq!(RuntimeValue::U32(expected), result);
    }
}

#[test]
fn direct_lowered_execution_runs_runtime_caps_example() {
    let source = examples_dir().join("runtime_caps.mira");
    let program = load_and_validate(&source).expect("program should validate");
    let lowered = lower_program_for_direct_exec(&program).expect("program should lower");

    let monotonic = run_lowered_function(
        &lowered,
        "monotonic_order",
        &std::collections::HashMap::new(),
    )
    .expect("direct runtime caps execution works");
    assert_eq!(RuntimeValue::Bool(true), monotonic);

    let seeded = run_lowered_function(
        &lowered,
        "seeded_rand_mix",
        &std::collections::HashMap::new(),
    )
    .expect("direct seeded rand execution works");
    assert_eq!(RuntimeValue::U32(471167712), seeded);
}

#[test]
fn direct_lowered_execution_runs_runtime_task_sleep_example() {
    let source = examples_dir().join("runtime_task_sleep.mira");
    let program = load_and_validate(&source).expect("program should validate");
    let lowered = lower_program_for_direct_exec(&program).expect("program should lower");
    let result = run_lowered_function(&lowered, "sleep_briefly", &std::collections::HashMap::new())
        .expect("direct task sleep execution works");
    assert_eq!(RuntimeValue::U8(1), result);
}

#[test]
fn direct_lowered_execution_runs_runtime_http_query_std_example() {
    let source = examples_dir().join("runtime_http_query_std.mira");
    let program = load_and_validate(&source).expect("program should validate");
    let lowered = lower_program_for_direct_exec(&program).expect("program should lower");
    let result = run_lowered_function(
        &lowered,
        "parse_query_payload",
        &std::collections::HashMap::new(),
    )
    .expect("direct http query std execution works");
    assert_eq!(RuntimeValue::U8(1), result);
}

#[test]
fn direct_lowered_execution_runs_runtime_stdlib_examples() {
    let runtime_json_api =
        load_and_validate(&examples_dir().join("runtime_json_api_endpoint.mira"))
            .expect("runtime_json_api_endpoint should validate");
    let lowered_json_api = lower_program_for_direct_exec(&runtime_json_api)
        .expect("runtime_json_api_endpoint should lower");
    let json_api_result = run_lowered_function(
        &lowered_json_api,
        "decode_request_and_encode_response",
        &std::collections::HashMap::new(),
    )
    .expect("direct runtime json api execution works");
    assert_eq!(RuntimeValue::U8(1), json_api_result);

    with_env_var("MIRA_REGION", "eu-central", || {
        let runtime_config =
            load_and_validate(&examples_dir().join("runtime_config_bootstrap.mira"))
                .expect("runtime_config_bootstrap should validate");
        let lowered_config = lower_program_for_direct_exec(&runtime_config)
            .expect("runtime_config_bootstrap should lower");
        let collection_result = run_lowered_function(
            &lowered_config,
            "strmap_and_strlist_helpers",
            &std::collections::HashMap::new(),
        )
        .expect("direct strmap/strlist execution works");
        assert_eq!(RuntimeValue::U8(1), collection_result);
    });
}

#[test]
fn direct_lowered_execution_runs_runtime_concurrency_examples() {
    let runtime_http_worker =
        load_and_validate(&examples_dir().join("runtime_http_worker_service.mira"))
            .expect("runtime_http_worker_service should validate");
    let lowered_http_worker = lower_program_for_direct_exec(&runtime_http_worker)
        .expect("runtime_http_worker_service should lower");
    let http_worker_result = run_lowered_function(
        &lowered_http_worker,
        "serve_request_batch",
        &std::collections::HashMap::new(),
    )
    .expect("direct runtime http worker execution works");
    assert_eq!(RuntimeValue::U32(1636), http_worker_result);

    let runtime_job_runner = load_and_validate(&examples_dir().join("runtime_job_runner.mira"))
        .expect("runtime_job_runner should validate");
    let lowered_job_runner = lower_program_for_direct_exec(&runtime_job_runner)
        .expect("runtime_job_runner should lower");
    let job_runner_result = run_lowered_function(
        &lowered_job_runner,
        "run_job_queue",
        &std::collections::HashMap::new(),
    )
    .expect("direct runtime job runner execution works");
    assert_eq!(RuntimeValue::U32(69), job_runner_result);

    let runtime_timeout_cancel =
        load_and_validate(&examples_dir().join("runtime_timeout_cancel.mira"))
            .expect("runtime_timeout_cancel should validate");
    let lowered_timeout_cancel = lower_program_for_direct_exec(&runtime_timeout_cancel)
        .expect("runtime_timeout_cancel should lower");
    let timeout_cancel_result = run_lowered_function(
        &lowered_timeout_cancel,
        "cancel_slow_job",
        &std::collections::HashMap::new(),
    )
    .expect("direct runtime timeout cancel execution works");
    assert_eq!(RuntimeValue::U32(999), timeout_cancel_result);
}

#[test]
fn direct_lowered_execution_runs_runtime_recovery_examples() {
    let runtime_multiworker =
        load_and_validate(&examples_dir().join("runtime_multiworker_http_service.mira"))
            .expect("runtime_multiworker_http_service should validate");
    let lowered_multiworker = lower_program_for_direct_exec(&runtime_multiworker)
        .expect("runtime_multiworker_http_service should lower");
    let multiworker_result = run_lowered_function(
        &lowered_multiworker,
        "serve_multiworker_http",
        &std::collections::HashMap::new(),
    )
    .expect("direct multiworker runtime execution works");
    assert_eq!(RuntimeValue::U32(26), multiworker_result);

    let runtime_deadline =
        load_and_validate(&examples_dir().join("runtime_deadline_job_system.mira"))
            .expect("runtime_deadline_job_system should validate");
    let lowered_deadline = lower_program_for_direct_exec(&runtime_deadline)
        .expect("runtime_deadline_job_system should lower");
    let deadline_result = run_lowered_function(
        &lowered_deadline,
        "run_deadline_job_system",
        &std::collections::HashMap::new(),
    )
    .expect("direct deadline runtime execution works");
    assert_eq!(RuntimeValue::U32(905), deadline_result);

    let runtime_supervisor =
        load_and_validate(&examples_dir().join("runtime_worker_supervisor.mira"))
            .expect("runtime_worker_supervisor should validate");
    let lowered_supervisor = lower_program_for_direct_exec(&runtime_supervisor)
        .expect("runtime_worker_supervisor should lower");
    let supervisor_result = run_lowered_function(
        &lowered_supervisor,
        "run_worker_supervisor",
        &std::collections::HashMap::new(),
    )
    .expect("direct supervisor runtime execution works");
    assert_eq!(RuntimeValue::U8(1), supervisor_result);
}

#[test]
fn direct_lowered_execution_runs_runtime_fs_spawn_and_ffi_examples() {
    let runtime_fs = load_and_validate(&examples_dir().join("runtime_fs.mira"))
        .expect("runtime_fs should validate");
    let lowered_fs = lower_program_for_direct_exec(&runtime_fs).expect("runtime_fs should lower");
    let fs_result = run_lowered_function(
        &lowered_fs,
        "roundtrip_u32",
        &std::collections::HashMap::new(),
    )
    .expect("direct runtime fs execution works");
    assert_eq!(RuntimeValue::U32(29), fs_result);

    let runtime_db = load_and_validate(&examples_dir().join("runtime_db_sqlite.mira"))
        .expect("runtime_db_sqlite should validate");
    let lowered_db =
        lower_program_for_direct_exec(&runtime_db).expect("runtime_db_sqlite should lower");
    let db_result = run_lowered_function(
        &lowered_db,
        "init_and_query_count",
        &std::collections::HashMap::new(),
    )
    .expect("direct runtime db execution works");
    assert_eq!(RuntimeValue::U8(1), db_result);

    let runtime_spawn = load_and_validate(&examples_dir().join("runtime_spawn.mira"))
        .expect("runtime_spawn should validate");
    let lowered_spawn =
        lower_program_for_direct_exec(&runtime_spawn).expect("runtime_spawn should lower");
    let spawn_true = run_lowered_function(
        &lowered_spawn,
        "spawn_true_status",
        &std::collections::HashMap::new(),
    )
    .expect("direct runtime spawn true execution works");
    assert_eq!(RuntimeValue::I32(0), spawn_true);
    let spawn_false = run_lowered_function(
        &lowered_spawn,
        "spawn_false_nonzero",
        &std::collections::HashMap::new(),
    )
    .expect("direct runtime spawn false execution works");
    assert_eq!(RuntimeValue::Bool(true), spawn_false);

    let runtime_spawn_bytes = load_and_validate(&examples_dir().join("runtime_spawn_bytes.mira"))
        .expect("runtime_spawn_bytes should validate");
    let lowered_spawn_bytes = lower_program_for_direct_exec(&runtime_spawn_bytes)
        .expect("runtime_spawn_bytes should lower");
    let spawn_bytes = run_lowered_function(
        &lowered_spawn_bytes,
        "capture_echo_newline",
        &std::collections::HashMap::new(),
    )
    .expect("direct runtime spawn bytes execution works");
    assert_eq!(RuntimeValue::U8(10), spawn_bytes);

    let runtime_spawn_split = load_and_validate(&examples_dir().join("runtime_spawn_split.mira"))
        .expect("runtime_spawn_split should validate");
    let lowered_spawn_split = lower_program_for_direct_exec(&runtime_spawn_split)
        .expect("runtime_spawn_split should lower");
    let spawn_split_stdout = run_lowered_function(
        &lowered_spawn_split,
        "capture_echo_arg",
        &std::collections::HashMap::new(),
    )
    .expect("direct runtime spawn split stdout execution works");
    assert_eq!(RuntimeValue::U8(72), spawn_split_stdout);
    let spawn_split_stderr = run_lowered_function(
        &lowered_spawn_split,
        "capture_cat_stderr",
        &std::collections::HashMap::new(),
    )
    .expect("direct runtime spawn split stderr execution works");
    assert_eq!(RuntimeValue::U8(99), spawn_split_stderr);

    let runtime_spawn_handle = load_and_validate(&examples_dir().join("runtime_spawn_handle.mira"))
        .expect("runtime_spawn_handle should validate");
    let lowered_spawn_handle = lower_program_for_direct_exec(&runtime_spawn_handle)
        .expect("runtime_spawn_handle should lower");
    let spawn_handle_result = run_lowered_function(
        &lowered_spawn_handle,
        "capture_echo_via_handle",
        &std::collections::HashMap::new(),
    )
    .expect("direct runtime spawn handle execution works");
    assert_eq!(RuntimeValue::U8(72), spawn_handle_result);

    let runtime_task_handle = load_and_validate(&examples_dir().join("runtime_task_handle.mira"))
        .expect("runtime_task_handle should validate");
    let lowered_task_handle = lower_program_for_direct_exec(&runtime_task_handle)
        .expect("runtime_task_handle should lower");
    let task_done_result = run_lowered_function(
        &lowered_task_handle,
        "poll_sleep_task",
        &std::collections::HashMap::new(),
    )
    .expect("direct runtime task handle polling works");
    assert_eq!(RuntimeValue::Bool(false), task_done_result);
    let task_stdout_result = run_lowered_function(
        &lowered_task_handle,
        "capture_echo_task_stdout",
        &std::collections::HashMap::new(),
    )
    .expect("direct runtime task handle stdout works");
    assert_eq!(RuntimeValue::U8(72), task_stdout_result);

    let runtime_ffi = load_and_validate(&examples_dir().join("runtime_ffi.mira"))
        .expect("runtime_ffi should validate");
    let lowered_ffi =
        lower_program_for_direct_exec(&runtime_ffi).expect("runtime_ffi should lower");
    let ffi_args = benchmark_arg_values(
        &runtime_ffi,
        "foreign_abs_delta",
        &[("x".to_string(), DataValue::Int(-7))],
    )
    .expect("ffi args should lower");
    let ffi_result = run_lowered_function(&lowered_ffi, "foreign_abs_delta", &ffi_args)
        .expect("direct runtime ffi execution works");
    assert_eq!(RuntimeValue::I32(14), ffi_result);

    let runtime_ffi_cstr = load_and_validate(&examples_dir().join("runtime_ffi_cstr.mira"))
        .expect("runtime_ffi_cstr should validate");
    let lowered_ffi_cstr =
        lower_program_for_direct_exec(&runtime_ffi_cstr).expect("runtime_ffi_cstr should lower");
    let ffi_cstr_result = run_lowered_function(
        &lowered_ffi_cstr,
        "foreign_atoi_ok",
        &std::collections::HashMap::new(),
    )
    .expect("direct runtime ffi cstr execution works");
    assert_eq!(RuntimeValue::I32(42), ffi_cstr_result);

    let runtime_ffi_lib = load_and_validate(&examples_dir().join("runtime_ffi_lib.mira"))
        .expect("runtime_ffi_lib should validate");
    let lowered_ffi_lib =
        lower_program_for_direct_exec(&runtime_ffi_lib).expect("runtime_ffi_lib should lower");
    let ffi_lib_result = run_lowered_function(
        &lowered_ffi_lib,
        "foreign_atoi_via_lib",
        &std::collections::HashMap::new(),
    )
    .expect("direct runtime ffi lib execution works");
    assert_eq!(RuntimeValue::I32(42), ffi_lib_result);

    let runtime_http_route = load_and_validate(&examples_dir().join("runtime_http_route.mira"))
        .expect("runtime_http_route should validate");
    let lowered_http_route = lower_program_for_direct_exec(&runtime_http_route)
        .expect("runtime_http_route should lower");
    let http_route_result = run_lowered_function(
        &lowered_http_route,
        "route_health",
        &std::collections::HashMap::new(),
    )
    .expect("direct runtime http route execution works");
    assert_eq!(RuntimeValue::U8(1), http_route_result);

    let runtime_http_middleware =
        load_and_validate(&examples_dir().join("runtime_http_middleware.mira"))
            .expect("runtime_http_middleware should validate");
    let lowered_http_middleware = lower_program_for_direct_exec(&runtime_http_middleware)
        .expect("runtime_http_middleware should lower");
    let http_middleware_result = run_lowered_function(
        &lowered_http_middleware,
        "authorize_request",
        &std::collections::HashMap::new(),
    )
    .expect("direct runtime http middleware execution works");
    assert_eq!(RuntimeValue::U8(1), http_middleware_result);

    let runtime_http_crud =
        load_and_validate(&examples_dir().join("runtime_http_crud_service.mira"))
            .expect("runtime_http_crud_service should validate");
    let lowered_http_crud = lower_program_for_direct_exec(&runtime_http_crud)
        .expect("runtime_http_crud_service should lower");
    let http_crud_get_result = run_lowered_function(
        &lowered_http_crud,
        "route_get_item_status",
        &std::collections::HashMap::new(),
    )
    .expect("direct runtime http crud get execution works");
    assert_eq!(RuntimeValue::U32(200), http_crud_get_result);
    let http_crud_post_result = run_lowered_function(
        &lowered_http_crud,
        "route_post_item_status",
        &std::collections::HashMap::new(),
    )
    .expect("direct runtime http crud post execution works");
    assert_eq!(RuntimeValue::U32(201), http_crud_post_result);

    let runtime_http_header_body =
        load_and_validate(&examples_dir().join("runtime_http_header_body.mira"))
            .expect("runtime_http_header_body should validate");
    let lowered_http_header_body = lower_program_for_direct_exec(&runtime_http_header_body)
        .expect("runtime_http_header_body should lower");
    let http_header_body_result = run_lowered_function(
        &lowered_http_header_body,
        "parse_header_and_body",
        &std::collections::HashMap::new(),
    )
    .expect("direct runtime http header/body execution works");
    assert_eq!(RuntimeValue::U8(1), http_header_body_result);

    let runtime_http_cookie_flow =
        load_and_validate(&examples_dir().join("runtime_http_cookie_flow.mira"))
            .expect("runtime_http_cookie_flow should validate");
    let lowered_http_cookie_flow = lower_program_for_direct_exec(&runtime_http_cookie_flow)
        .expect("runtime_http_cookie_flow should lower");
    let http_cookie_flow_result = run_lowered_function(
        &lowered_http_cookie_flow,
        "parse_cookie_and_authorize",
        &std::collections::HashMap::new(),
    )
    .expect("direct runtime http cookie flow execution works");
    assert_eq!(RuntimeValue::U8(1), http_cookie_flow_result);

    let runtime_http_header_cookie_json_api =
        load_and_validate(&examples_dir().join("runtime_http_header_cookie_json_api.mira"))
            .expect("runtime_http_header_cookie_json_api should validate");
    let lowered_http_header_cookie_json_api =
        lower_program_for_direct_exec(&runtime_http_header_cookie_json_api)
            .expect("runtime_http_header_cookie_json_api should lower");
    let http_header_cookie_json_api_result = run_lowered_function(
        &lowered_http_header_cookie_json_api,
        "decode_header_cookie_request",
        &std::collections::HashMap::new(),
    )
    .expect("direct runtime http header/cookie json api execution works");
    assert_eq!(RuntimeValue::U8(1), http_header_cookie_json_api_result);

    let runtime_json_extract = load_and_validate(&examples_dir().join("runtime_json_extract.mira"))
        .expect("runtime_json_extract should validate");
    let lowered_json_extract = lower_program_for_direct_exec(&runtime_json_extract)
        .expect("runtime_json_extract should lower");
    let json_extract_result = run_lowered_function(
        &lowered_json_extract,
        "parse_json_scalars_and_string",
        &std::collections::HashMap::new(),
    )
    .expect("direct runtime json extract execution works");
    assert_eq!(RuntimeValue::U8(1), json_extract_result);
}

#[test]
fn bytecode_execution_runs_kernel_examples() {
    let cases = [
        ("sum_abs.mira", "sum_abs", RuntimeValue::I64(100000000)),
        (
            "dot_product.mira",
            "dot_product",
            RuntimeValue::I64(4499662502500),
        ),
        (
            "fib_iter.mira",
            "fib_iter",
            RuntimeValue::I64(1548008755920),
        ),
    ];
    for (file_name, function_name, expected) in cases {
        let source = examples_dir().join(file_name);
        let program = load_and_validate(&source).expect("program should validate");
        let lowered = lower_program_for_direct_exec(&program).expect("program should lower");
        let bytecode = compile_bytecode_program(&lowered).expect("bytecode should compile");
        let args = match function_name {
            "sum_abs" => benchmark_arg_values(
                &program,
                function_name,
                &[(
                    "xs".to_string(),
                    DataValue::Array(
                        (-10_000..10_000)
                            .map(|value| DataValue::Int(value as i128))
                            .collect(),
                    ),
                )],
            ),
            "dot_product" => benchmark_arg_values(
                &program,
                function_name,
                &[
                    (
                        "xs".to_string(),
                        DataValue::Array(
                            (0..15_000)
                                .map(|value| DataValue::Int(value as i128))
                                .collect(),
                        ),
                    ),
                    (
                        "ys".to_string(),
                        DataValue::Array(
                            (30_000..45_000)
                                .map(|value| DataValue::Int(value as i128))
                                .collect(),
                        ),
                    ),
                ],
            ),
            "fib_iter" => benchmark_arg_values(
                &program,
                function_name,
                &[("n".to_string(), DataValue::Int(60))],
            ),
            _ => unreachable!(),
        }
        .expect("benchmark args should lower to runtime values");
        let result = run_bytecode_function(&bytecode, function_name, &args)
            .expect("bytecode execution works");
        assert_eq!(expected, result);
    }
}

#[test]
fn bytecode_execution_runs_bit_ops_example() {
    let source = examples_dir().join("bit_ops.mira");
    let program = load_and_validate(&source).expect("program should validate");
    let lowered = lower_program_for_direct_exec(&program).expect("program should lower");
    let bytecode = compile_bytecode_program(&lowered).expect("bytecode should compile");
    for (input, expected) in bit_ops_cases() {
        let args = benchmark_arg_values(
            &program,
            "scramble_u32",
            &[("x".to_string(), DataValue::Int(input as i128))],
        )
        .expect("bit op args should lower");
        let result = run_bytecode_function(&bytecode, "scramble_u32", &args)
            .expect("bytecode execution works");
        assert_eq!(RuntimeValue::U32(expected), result);
    }
}

#[test]
fn bytecode_execution_runs_runtime_caps_example() {
    let source = examples_dir().join("runtime_caps.mira");
    let program = load_and_validate(&source).expect("program should validate");
    let lowered = lower_program_for_direct_exec(&program).expect("program should lower");
    let bytecode = compile_bytecode_program(&lowered).expect("bytecode should compile");

    let monotonic = run_bytecode_function(
        &bytecode,
        "monotonic_order",
        &std::collections::HashMap::new(),
    )
    .expect("bytecode runtime caps execution works");
    assert_eq!(RuntimeValue::Bool(true), monotonic);

    let seeded = run_bytecode_function(
        &bytecode,
        "seeded_rand_mix",
        &std::collections::HashMap::new(),
    )
    .expect("bytecode seeded rand execution works");
    assert_eq!(RuntimeValue::U32(471167712), seeded);
}

#[test]
fn bytecode_execution_runs_runtime_task_sleep_example() {
    let source = examples_dir().join("runtime_task_sleep.mira");
    let program = load_and_validate(&source).expect("program should validate");
    let lowered = lower_program_for_direct_exec(&program).expect("program should lower");
    let bytecode = compile_bytecode_program(&lowered).expect("bytecode should compile");
    let result = run_bytecode_function(
        &bytecode,
        "sleep_briefly",
        &std::collections::HashMap::new(),
    )
    .expect("bytecode task sleep execution works");
    assert_eq!(RuntimeValue::U8(1), result);
}

#[test]
fn bytecode_execution_runs_runtime_http_query_std_example() {
    let source = examples_dir().join("runtime_http_query_std.mira");
    let program = load_and_validate(&source).expect("program should validate");
    let lowered = lower_program_for_direct_exec(&program).expect("program should lower");
    let bytecode = compile_bytecode_program(&lowered).expect("bytecode should compile");
    let result = run_bytecode_function(
        &bytecode,
        "parse_query_payload",
        &std::collections::HashMap::new(),
    )
    .expect("bytecode http query std execution works");
    assert_eq!(RuntimeValue::U8(1), result);
}

#[test]
fn bytecode_execution_runs_runtime_stdlib_examples() {
    let runtime_json_api =
        load_and_validate(&examples_dir().join("runtime_json_api_endpoint.mira"))
            .expect("runtime_json_api_endpoint should validate");
    let lowered_json_api = lower_program_for_direct_exec(&runtime_json_api)
        .expect("runtime_json_api_endpoint should lower");
    let bytecode_json_api =
        compile_bytecode_program(&lowered_json_api).expect("runtime_json_api_endpoint bytecode");
    let json_api_result = run_bytecode_function(
        &bytecode_json_api,
        "decode_request_and_encode_response",
        &std::collections::HashMap::new(),
    )
    .expect("bytecode runtime json api execution works");
    assert_eq!(RuntimeValue::U8(1), json_api_result);

    with_env_var("MIRA_REGION", "eu-central", || {
        let runtime_config =
            load_and_validate(&examples_dir().join("runtime_config_bootstrap.mira"))
                .expect("runtime_config_bootstrap should validate");
        let lowered_config = lower_program_for_direct_exec(&runtime_config)
            .expect("runtime_config_bootstrap should lower");
        let bytecode_config =
            compile_bytecode_program(&lowered_config).expect("runtime_config_bootstrap bytecode");

        let collection_result = run_bytecode_function(
            &bytecode_config,
            "strmap_and_strlist_helpers",
            &std::collections::HashMap::new(),
        )
        .expect("bytecode strmap/strlist execution works");
        assert_eq!(RuntimeValue::U8(1), collection_result);
    });
}

#[test]
fn bytecode_execution_runs_runtime_fs_spawn_and_ffi_examples() {
    let runtime_fs = load_and_validate(&examples_dir().join("runtime_fs.mira"))
        .expect("runtime_fs should validate");
    let lowered_fs = lower_program_for_direct_exec(&runtime_fs).expect("runtime_fs should lower");
    let bytecode_fs = compile_bytecode_program(&lowered_fs).expect("runtime_fs bytecode");
    let fs_result = run_bytecode_function(
        &bytecode_fs,
        "roundtrip_u32",
        &std::collections::HashMap::new(),
    )
    .expect("bytecode runtime fs execution works");
    assert_eq!(RuntimeValue::U32(29), fs_result);

    let runtime_db = load_and_validate(&examples_dir().join("runtime_db_sqlite.mira"))
        .expect("runtime_db_sqlite should validate");
    let lowered_db =
        lower_program_for_direct_exec(&runtime_db).expect("runtime_db_sqlite should lower");
    let bytecode_db = compile_bytecode_program(&lowered_db).expect("runtime_db_sqlite bytecode");
    let db_result = run_bytecode_function(
        &bytecode_db,
        "init_and_query_count",
        &std::collections::HashMap::new(),
    )
    .expect("bytecode runtime db execution works");
    assert_eq!(RuntimeValue::U8(1), db_result);

    let runtime_spawn = load_and_validate(&examples_dir().join("runtime_spawn.mira"))
        .expect("runtime_spawn should validate");
    let lowered_spawn =
        lower_program_for_direct_exec(&runtime_spawn).expect("runtime_spawn should lower");
    let bytecode_spawn = compile_bytecode_program(&lowered_spawn).expect("runtime_spawn bytecode");
    let spawn_true = run_bytecode_function(
        &bytecode_spawn,
        "spawn_true_status",
        &std::collections::HashMap::new(),
    )
    .expect("bytecode runtime spawn true execution works");
    assert_eq!(RuntimeValue::I32(0), spawn_true);
    let spawn_false = run_bytecode_function(
        &bytecode_spawn,
        "spawn_false_nonzero",
        &std::collections::HashMap::new(),
    )
    .expect("bytecode runtime spawn false execution works");
    assert_eq!(RuntimeValue::Bool(true), spawn_false);

    let runtime_spawn_bytes = load_and_validate(&examples_dir().join("runtime_spawn_bytes.mira"))
        .expect("runtime_spawn_bytes should validate");
    let lowered_spawn_bytes = lower_program_for_direct_exec(&runtime_spawn_bytes)
        .expect("runtime_spawn_bytes should lower");
    let bytecode_spawn_bytes =
        compile_bytecode_program(&lowered_spawn_bytes).expect("runtime_spawn_bytes bytecode");
    let spawn_bytes = run_bytecode_function(
        &bytecode_spawn_bytes,
        "capture_echo_newline",
        &std::collections::HashMap::new(),
    )
    .expect("bytecode runtime spawn bytes execution works");
    assert_eq!(RuntimeValue::U8(10), spawn_bytes);

    let runtime_spawn_split = load_and_validate(&examples_dir().join("runtime_spawn_split.mira"))
        .expect("runtime_spawn_split should validate");
    let lowered_spawn_split = lower_program_for_direct_exec(&runtime_spawn_split)
        .expect("runtime_spawn_split should lower");
    let bytecode_spawn_split =
        compile_bytecode_program(&lowered_spawn_split).expect("runtime_spawn_split bytecode");
    let spawn_split_stdout = run_bytecode_function(
        &bytecode_spawn_split,
        "capture_echo_arg",
        &std::collections::HashMap::new(),
    )
    .expect("bytecode runtime spawn split stdout execution works");
    assert_eq!(RuntimeValue::U8(72), spawn_split_stdout);
    let spawn_split_stderr = run_bytecode_function(
        &bytecode_spawn_split,
        "capture_cat_stderr",
        &std::collections::HashMap::new(),
    )
    .expect("bytecode runtime spawn split stderr execution works");
    assert_eq!(RuntimeValue::U8(99), spawn_split_stderr);

    let runtime_spawn_handle = load_and_validate(&examples_dir().join("runtime_spawn_handle.mira"))
        .expect("runtime_spawn_handle should validate");
    let lowered_spawn_handle = lower_program_for_direct_exec(&runtime_spawn_handle)
        .expect("runtime_spawn_handle should lower");
    let bytecode_spawn_handle =
        compile_bytecode_program(&lowered_spawn_handle).expect("runtime_spawn_handle bytecode");
    let spawn_handle_result = run_bytecode_function(
        &bytecode_spawn_handle,
        "capture_echo_via_handle",
        &std::collections::HashMap::new(),
    )
    .expect("bytecode runtime spawn handle execution works");
    assert_eq!(RuntimeValue::U8(72), spawn_handle_result);

    let runtime_task_handle = load_and_validate(&examples_dir().join("runtime_task_handle.mira"))
        .expect("runtime_task_handle should validate");
    let lowered_task_handle = lower_program_for_direct_exec(&runtime_task_handle)
        .expect("runtime_task_handle should lower");
    let bytecode_task_handle =
        compile_bytecode_program(&lowered_task_handle).expect("runtime_task_handle bytecode");
    let task_done_result = run_bytecode_function(
        &bytecode_task_handle,
        "poll_sleep_task",
        &std::collections::HashMap::new(),
    )
    .expect("bytecode runtime task handle polling works");
    assert_eq!(RuntimeValue::Bool(false), task_done_result);
    let task_stdout_result = run_bytecode_function(
        &bytecode_task_handle,
        "capture_echo_task_stdout",
        &std::collections::HashMap::new(),
    )
    .expect("bytecode runtime task handle stdout works");
    assert_eq!(RuntimeValue::U8(72), task_stdout_result);

    let runtime_ffi = load_and_validate(&examples_dir().join("runtime_ffi.mira"))
        .expect("runtime_ffi should validate");
    let lowered_ffi =
        lower_program_for_direct_exec(&runtime_ffi).expect("runtime_ffi should lower");
    let bytecode_ffi = compile_bytecode_program(&lowered_ffi).expect("runtime_ffi bytecode");
    let ffi_args = benchmark_arg_values(
        &runtime_ffi,
        "foreign_abs_delta",
        &[("x".to_string(), DataValue::Int(-7))],
    )
    .expect("ffi args should lower");
    let ffi_result = run_bytecode_function(&bytecode_ffi, "foreign_abs_delta", &ffi_args)
        .expect("bytecode runtime ffi execution works");
    assert_eq!(RuntimeValue::I32(14), ffi_result);

    let runtime_ffi_cstr = load_and_validate(&examples_dir().join("runtime_ffi_cstr.mira"))
        .expect("runtime_ffi_cstr should validate");
    let lowered_ffi_cstr =
        lower_program_for_direct_exec(&runtime_ffi_cstr).expect("runtime_ffi_cstr should lower");
    let bytecode_ffi_cstr =
        compile_bytecode_program(&lowered_ffi_cstr).expect("runtime_ffi_cstr bytecode");
    let ffi_cstr_result = run_bytecode_function(
        &bytecode_ffi_cstr,
        "foreign_atoi_ok",
        &std::collections::HashMap::new(),
    )
    .expect("bytecode runtime ffi cstr execution works");
    assert_eq!(RuntimeValue::I32(42), ffi_cstr_result);

    let runtime_ffi_lib = load_and_validate(&examples_dir().join("runtime_ffi_lib.mira"))
        .expect("runtime_ffi_lib should validate");
    let lowered_ffi_lib =
        lower_program_for_direct_exec(&runtime_ffi_lib).expect("runtime_ffi_lib should lower");
    let bytecode_ffi_lib =
        compile_bytecode_program(&lowered_ffi_lib).expect("runtime_ffi_lib bytecode");
    let ffi_lib_result = run_bytecode_function(
        &bytecode_ffi_lib,
        "foreign_atoi_via_lib",
        &std::collections::HashMap::new(),
    )
    .expect("bytecode runtime ffi lib execution works");
    assert_eq!(RuntimeValue::I32(42), ffi_lib_result);

    let runtime_http_route = load_and_validate(&examples_dir().join("runtime_http_route.mira"))
        .expect("runtime_http_route should validate");
    let lowered_http_route = lower_program_for_direct_exec(&runtime_http_route)
        .expect("runtime_http_route should lower");
    let bytecode_http_route =
        compile_bytecode_program(&lowered_http_route).expect("runtime_http_route bytecode");
    let http_route_result = run_bytecode_function(
        &bytecode_http_route,
        "route_health",
        &std::collections::HashMap::new(),
    )
    .expect("bytecode runtime http route execution works");
    assert_eq!(RuntimeValue::U8(1), http_route_result);

    let runtime_http_middleware =
        load_and_validate(&examples_dir().join("runtime_http_middleware.mira"))
            .expect("runtime_http_middleware should validate");
    let lowered_http_middleware = lower_program_for_direct_exec(&runtime_http_middleware)
        .expect("runtime_http_middleware should lower");
    let bytecode_http_middleware = compile_bytecode_program(&lowered_http_middleware)
        .expect("runtime_http_middleware bytecode");
    let http_middleware_result = run_bytecode_function(
        &bytecode_http_middleware,
        "authorize_request",
        &std::collections::HashMap::new(),
    )
    .expect("bytecode runtime http middleware execution works");
    assert_eq!(RuntimeValue::U8(1), http_middleware_result);

    let runtime_http_crud =
        load_and_validate(&examples_dir().join("runtime_http_crud_service.mira"))
            .expect("runtime_http_crud_service should validate");
    let lowered_http_crud = lower_program_for_direct_exec(&runtime_http_crud)
        .expect("runtime_http_crud_service should lower");
    let bytecode_http_crud =
        compile_bytecode_program(&lowered_http_crud).expect("runtime_http_crud_service bytecode");
    let http_crud_get_result = run_bytecode_function(
        &bytecode_http_crud,
        "route_get_item_status",
        &std::collections::HashMap::new(),
    )
    .expect("bytecode runtime http crud get execution works");
    assert_eq!(RuntimeValue::U32(200), http_crud_get_result);
    let http_crud_post_result = run_bytecode_function(
        &bytecode_http_crud,
        "route_post_item_status",
        &std::collections::HashMap::new(),
    )
    .expect("bytecode runtime http crud post execution works");
    assert_eq!(RuntimeValue::U32(201), http_crud_post_result);
}

#[test]
fn portable_bytecode_verification_runs_runtime_caps_tests() {
    let source = examples_dir().join("runtime_caps.mira");
    let program = load_and_validate(&source).expect("program should validate");
    let lowered = lower_program_for_direct_exec(&program).expect("program should lower");
    let summary = verify_lowered_tests_portably(&lowered)
        .expect("portable verification should succeed")
        .expect("runtime caps should stay on portable bytecode path");
    assert_eq!("portable bytecode tests passed: 2/2", summary);
}

#[test]
fn portable_bytecode_verification_runs_runtime_task_sleep_tests() {
    let source = examples_dir().join("runtime_task_sleep.mira");
    let program = load_and_validate(&source).expect("program should validate");
    let lowered = lower_program_for_direct_exec(&program).expect("program should lower");
    let summary = verify_lowered_tests_portably(&lowered)
        .expect("portable task sleep verification should succeed")
        .expect("runtime_task_sleep should stay on portable bytecode path");
    assert_eq!("portable bytecode tests passed: 1/1", summary);
}

#[test]
fn portable_bytecode_verification_runs_runtime_http_query_std_tests() {
    let source = examples_dir().join("runtime_http_query_std.mira");
    let program = load_and_validate(&source).expect("program should validate");
    let lowered = lower_program_for_direct_exec(&program).expect("program should lower");
    let summary = verify_lowered_tests_portably(&lowered)
        .expect("portable http query std verification should succeed")
        .expect("runtime_http_query_std should stay on portable bytecode path");
    assert_eq!("portable bytecode tests passed: 1/1", summary);
}

#[test]
fn portable_bytecode_verification_runs_runtime_http_header_cookie_json_api_tests() {
    let source = examples_dir().join("runtime_http_header_cookie_json_api.mira");
    let program = load_and_validate(&source).expect("program should validate");
    let lowered = lower_program_for_direct_exec(&program).expect("program should lower");
    let summary = verify_lowered_tests_portably(&lowered)
        .expect("portable header/cookie json api verification should succeed")
        .expect("runtime_http_header_cookie_json_api should stay on portable bytecode path");
    assert_eq!("portable bytecode tests passed: 1/1", summary);
}

#[test]
fn portable_bytecode_verification_runs_runtime_stdlib_tests() {
    let runtime_json_api =
        load_and_validate(&examples_dir().join("runtime_json_api_endpoint.mira"))
            .expect("runtime_json_api_endpoint should validate");
    let lowered_json_api = lower_program_for_direct_exec(&runtime_json_api)
        .expect("runtime_json_api_endpoint should lower");
    let json_api_summary = verify_lowered_tests_portably(&lowered_json_api)
        .expect("portable json api verification should succeed")
        .expect("runtime_json_api_endpoint should stay on portable bytecode path");
    assert_eq!("portable bytecode tests passed: 1/1", json_api_summary);

    with_env_var("MIRA_REGION", "eu-central", || {
        let runtime_config =
            load_and_validate(&examples_dir().join("runtime_config_bootstrap.mira"))
                .expect("runtime_config_bootstrap should validate");
        let lowered_config = lower_program_for_direct_exec(&runtime_config)
            .expect("runtime_config_bootstrap should lower");
        let config_summary = verify_lowered_tests_portably(&lowered_config)
            .expect("portable config bootstrap verification should succeed")
            .expect("runtime_config_bootstrap should stay on portable bytecode path");
        assert_eq!("portable bytecode tests passed: 2/2", config_summary);
    });
}

#[test]
fn runtime_config_bootstrap_example_passes_native_tests_with_env() {
    let source = examples_dir().join("runtime_config_bootstrap.mira");
    let program = load_and_validate(&source).expect("runtime_config_bootstrap should validate");
    let harness = emit_test_harness(&program).expect("runtime_config_bootstrap harness");
    let binary = compile_c_source("itest_runtime_config_bootstrap", &harness)
        .expect("runtime_config_bootstrap native binary should compile");
    let output = Command::new(&binary)
        .env("MIRA_REGION", "eu-central")
        .output()
        .expect("runtime_config_bootstrap native binary should run");
    assert!(
        output.status.success(),
        "runtime_config_bootstrap native run failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn runtime_concurrency_examples_repeat_without_deadlock_or_handle_exhaustion() {
    let worker_program =
        load_and_validate(&examples_dir().join("runtime_http_worker_service.mira"))
            .expect("runtime_http_worker_service should validate");
    let worker_lowered = lower_program_for_direct_exec(&worker_program)
        .expect("runtime_http_worker_service should lower");
    for _ in 0..12 {
        let result = run_lowered_function(
            &worker_lowered,
            "serve_request_batch",
            &std::collections::HashMap::new(),
        )
        .expect("direct worker service execution should keep succeeding");
        assert_eq!(RuntimeValue::U32(1636), result);
    }

    let timeout_program = load_and_validate(&examples_dir().join("runtime_timeout_cancel.mira"))
        .expect("runtime_timeout_cancel should validate");
    let timeout_harness = emit_test_harness(&timeout_program)
        .expect("runtime_timeout_cancel native harness should render");
    let timeout_binary = compile_c_source("itest_runtime_timeout_cancel", &timeout_harness)
        .expect("runtime_timeout_cancel native binary should compile");
    for _ in 0..6 {
        let output = Command::new(&timeout_binary)
            .output()
            .expect("runtime_timeout_cancel native binary should run");
        assert!(
            output.status.success(),
            "runtime_timeout_cancel native run failed: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[test]
fn runtime_recovery_examples_repeat_without_deadlock_or_handle_exhaustion() {
    let multiworker_program =
        load_and_validate(&examples_dir().join("runtime_multiworker_http_service.mira"))
            .expect("runtime_multiworker_http_service should validate");
    let multiworker_lowered = lower_program_for_direct_exec(&multiworker_program)
        .expect("runtime_multiworker_http_service should lower");
    for _ in 0..12 {
        let result = run_lowered_function(
            &multiworker_lowered,
            "serve_multiworker_http",
            &std::collections::HashMap::new(),
        )
        .expect("direct multiworker execution should keep succeeding");
        assert_eq!(RuntimeValue::U32(26), result);
    }

    let deadline_program =
        load_and_validate(&examples_dir().join("runtime_deadline_job_system.mira"))
            .expect("runtime_deadline_job_system should validate");
    let deadline_harness =
        emit_test_harness(&deadline_program).expect("deadline native harness should render");
    let deadline_binary = compile_c_source("itest_runtime_deadline_job_system", &deadline_harness)
        .expect("runtime_deadline_job_system native binary should compile");
    for _ in 0..6 {
        let output = Command::new(&deadline_binary)
            .output()
            .expect("runtime_deadline_job_system native binary should run");
        assert!(
            output.status.success(),
            "runtime_deadline_job_system native run failed: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let supervisor_program =
        load_and_validate(&examples_dir().join("runtime_worker_supervisor.mira"))
            .expect("runtime_worker_supervisor should validate");
    let supervisor_harness =
        emit_test_harness(&supervisor_program).expect("supervisor native harness should render");
    let supervisor_binary = compile_c_source("itest_runtime_worker_supervisor", &supervisor_harness)
        .expect("runtime_worker_supervisor native binary should compile");
    for _ in 0..6 {
        let output = Command::new(&supervisor_binary)
            .output()
            .expect("runtime_worker_supervisor native binary should run");
        assert!(
            output.status.success(),
            "runtime_worker_supervisor native run failed: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[test]
fn portable_bytecode_verification_runs_runtime_concurrency_examples() {
    let runtime_http_worker =
        load_and_validate(&examples_dir().join("runtime_http_worker_service.mira"))
            .expect("runtime_http_worker_service should validate");
    let lowered_http_worker = lower_program_for_direct_exec(&runtime_http_worker)
        .expect("runtime_http_worker_service should lower");
    let http_worker_summary = verify_lowered_tests_portably(&lowered_http_worker)
        .expect("portable concurrency verification should succeed")
        .expect("runtime_http_worker_service should stay on portable bytecode path");
    assert_eq!("portable bytecode tests passed: 1/1", http_worker_summary);

    let runtime_job_runner = load_and_validate(&examples_dir().join("runtime_job_runner.mira"))
        .expect("runtime_job_runner should validate");
    let lowered_job_runner = lower_program_for_direct_exec(&runtime_job_runner)
        .expect("runtime_job_runner should lower");
    let job_runner_summary = verify_lowered_tests_portably(&lowered_job_runner)
        .expect("portable job-runner verification should succeed")
        .expect("runtime_job_runner should stay on portable bytecode path");
    assert_eq!("portable bytecode tests passed: 1/1", job_runner_summary);

    let runtime_timeout_cancel =
        load_and_validate(&examples_dir().join("runtime_timeout_cancel.mira"))
            .expect("runtime_timeout_cancel should validate");
    let lowered_timeout_cancel = lower_program_for_direct_exec(&runtime_timeout_cancel)
        .expect("runtime_timeout_cancel should lower");
    let timeout_cancel_summary = verify_lowered_tests_portably(&lowered_timeout_cancel)
        .expect("portable timeout-cancel verification should succeed")
        .expect("runtime_timeout_cancel should stay on portable bytecode path");
    assert_eq!(
        "portable bytecode tests passed: 1/1",
        timeout_cancel_summary
    );
}

#[test]
fn portable_bytecode_verification_runs_runtime_recovery_examples() {
    let runtime_multiworker =
        load_and_validate(&examples_dir().join("runtime_multiworker_http_service.mira"))
            .expect("runtime_multiworker_http_service should validate");
    let lowered_multiworker = lower_program_for_direct_exec(&runtime_multiworker)
        .expect("runtime_multiworker_http_service should lower");
    let multiworker_summary = verify_lowered_tests_portably(&lowered_multiworker)
        .expect("portable recovery verification should succeed")
        .expect("runtime_multiworker_http_service should stay on portable bytecode path");
    assert_eq!("portable bytecode tests passed: 1/1", multiworker_summary);

    let runtime_deadline =
        load_and_validate(&examples_dir().join("runtime_deadline_job_system.mira"))
            .expect("runtime_deadline_job_system should validate");
    let lowered_deadline = lower_program_for_direct_exec(&runtime_deadline)
        .expect("runtime_deadline_job_system should lower");
    let deadline_summary = verify_lowered_tests_portably(&lowered_deadline)
        .expect("portable deadline verification should succeed")
        .expect("runtime_deadline_job_system should stay on portable bytecode path");
    assert_eq!("portable bytecode tests passed: 1/1", deadline_summary);

    let runtime_supervisor =
        load_and_validate(&examples_dir().join("runtime_worker_supervisor.mira"))
            .expect("runtime_worker_supervisor should validate");
    let lowered_supervisor = lower_program_for_direct_exec(&runtime_supervisor)
        .expect("runtime_worker_supervisor should lower");
    let supervisor_summary = verify_lowered_tests_portably(&lowered_supervisor)
        .expect("portable supervisor verification should succeed")
        .expect("runtime_worker_supervisor should stay on portable bytecode path");
    assert_eq!("portable bytecode tests passed: 1/1", supervisor_summary);
}

#[test]
fn portable_bytecode_verification_runs_runtime_fs_spawn_and_ffi_tests() {
    let runtime_fs = load_and_validate(&examples_dir().join("runtime_fs.mira"))
        .expect("runtime_fs should validate");
    let lowered_fs = lower_program_for_direct_exec(&runtime_fs).expect("runtime_fs should lower");
    let fs_summary = verify_lowered_tests_portably(&lowered_fs)
        .expect("portable fs verification should succeed")
        .expect("runtime_fs should stay on portable bytecode path");
    assert_eq!("portable bytecode tests passed: 1/1", fs_summary);

    let runtime_db = load_and_validate(&examples_dir().join("runtime_db_sqlite.mira"))
        .expect("runtime_db_sqlite should validate");
    let lowered_db =
        lower_program_for_direct_exec(&runtime_db).expect("runtime_db_sqlite should lower");
    let db_summary = verify_lowered_tests_portably(&lowered_db)
        .expect("portable db verification should succeed")
        .expect("runtime_db_sqlite should stay on portable bytecode path");
    assert_eq!("portable bytecode tests passed: 1/1", db_summary);

    let runtime_spawn = load_and_validate(&examples_dir().join("runtime_spawn.mira"))
        .expect("runtime_spawn should validate");
    let lowered_spawn =
        lower_program_for_direct_exec(&runtime_spawn).expect("runtime_spawn should lower");
    let spawn_summary = verify_lowered_tests_portably(&lowered_spawn)
        .expect("portable spawn verification should succeed")
        .expect("runtime_spawn should stay on portable bytecode path");
    assert_eq!("portable bytecode tests passed: 2/2", spawn_summary);

    let runtime_spawn_bytes = load_and_validate(&examples_dir().join("runtime_spawn_bytes.mira"))
        .expect("runtime_spawn_bytes should validate");
    let lowered_spawn_bytes = lower_program_for_direct_exec(&runtime_spawn_bytes)
        .expect("runtime_spawn_bytes should lower");
    let spawn_bytes_summary = verify_lowered_tests_portably(&lowered_spawn_bytes)
        .expect("portable spawn-bytes verification should succeed")
        .expect("runtime_spawn_bytes should stay on portable bytecode path");
    assert_eq!("portable bytecode tests passed: 1/1", spawn_bytes_summary);

    let runtime_spawn_split = load_and_validate(&examples_dir().join("runtime_spawn_split.mira"))
        .expect("runtime_spawn_split should validate");
    let lowered_spawn_split = lower_program_for_direct_exec(&runtime_spawn_split)
        .expect("runtime_spawn_split should lower");
    let spawn_split_summary = verify_lowered_tests_portably(&lowered_spawn_split)
        .expect("portable spawn-split verification should succeed")
        .expect("runtime_spawn_split should stay on portable bytecode path");
    assert_eq!("portable bytecode tests passed: 2/2", spawn_split_summary);

    let runtime_spawn_handle = load_and_validate(&examples_dir().join("runtime_spawn_handle.mira"))
        .expect("runtime_spawn_handle should validate");
    let lowered_spawn_handle = lower_program_for_direct_exec(&runtime_spawn_handle)
        .expect("runtime_spawn_handle should lower");
    let spawn_handle_summary = verify_lowered_tests_portably(&lowered_spawn_handle)
        .expect("portable spawn-handle verification should succeed")
        .expect("runtime_spawn_handle should stay on portable bytecode path");
    assert_eq!("portable bytecode tests passed: 1/1", spawn_handle_summary);

    let runtime_task_handle = load_and_validate(&examples_dir().join("runtime_task_handle.mira"))
        .expect("runtime_task_handle should validate");
    let lowered_task_handle = lower_program_for_direct_exec(&runtime_task_handle)
        .expect("runtime_task_handle should lower");
    let task_handle_summary = verify_lowered_tests_portably(&lowered_task_handle)
        .expect("portable task-handle verification should succeed")
        .expect("runtime_task_handle should stay on portable bytecode path");
    assert_eq!("portable bytecode tests passed: 2/2", task_handle_summary);

    let runtime_ffi = load_and_validate(&examples_dir().join("runtime_ffi.mira"))
        .expect("runtime_ffi should validate");
    let lowered_ffi =
        lower_program_for_direct_exec(&runtime_ffi).expect("runtime_ffi should lower");
    let ffi_summary = verify_lowered_tests_portably(&lowered_ffi)
        .expect("portable ffi verification should succeed")
        .expect("runtime_ffi should stay on portable bytecode path");
    assert_eq!("portable bytecode tests passed: 1/1", ffi_summary);

    let runtime_ffi_cstr = load_and_validate(&examples_dir().join("runtime_ffi_cstr.mira"))
        .expect("runtime_ffi_cstr should validate");
    let lowered_ffi_cstr =
        lower_program_for_direct_exec(&runtime_ffi_cstr).expect("runtime_ffi_cstr should lower");
    let ffi_cstr_summary = verify_lowered_tests_portably(&lowered_ffi_cstr)
        .expect("portable ffi cstr verification should succeed")
        .expect("runtime_ffi_cstr should stay on portable bytecode path");
    assert_eq!("portable bytecode tests passed: 1/1", ffi_cstr_summary);

    let runtime_ffi_lib = load_and_validate(&examples_dir().join("runtime_ffi_lib.mira"))
        .expect("runtime_ffi_lib should validate");
    let lowered_ffi_lib =
        lower_program_for_direct_exec(&runtime_ffi_lib).expect("runtime_ffi_lib should lower");
    let ffi_lib_summary = verify_lowered_tests_portably(&lowered_ffi_lib)
        .expect("portable ffi lib verification should succeed")
        .expect("runtime_ffi_lib should stay on portable bytecode path");
    assert_eq!("portable bytecode tests passed: 1/1", ffi_lib_summary);

    let runtime_http_route = load_and_validate(&examples_dir().join("runtime_http_route.mira"))
        .expect("runtime_http_route should validate");
    let lowered_http_route = lower_program_for_direct_exec(&runtime_http_route)
        .expect("runtime_http_route should lower");
    let http_route_summary = verify_lowered_tests_portably(&lowered_http_route)
        .expect("portable http route verification should succeed")
        .expect("runtime_http_route should stay on portable bytecode path");
    assert_eq!("portable bytecode tests passed: 1/1", http_route_summary);

    let runtime_http_middleware =
        load_and_validate(&examples_dir().join("runtime_http_middleware.mira"))
            .expect("runtime_http_middleware should validate");
    let lowered_http_middleware = lower_program_for_direct_exec(&runtime_http_middleware)
        .expect("runtime_http_middleware should lower");
    let http_middleware_summary = verify_lowered_tests_portably(&lowered_http_middleware)
        .expect("portable http middleware verification should succeed")
        .expect("runtime_http_middleware should stay on portable bytecode path");
    assert_eq!(
        "portable bytecode tests passed: 1/1",
        http_middleware_summary
    );

    let runtime_http_crud =
        load_and_validate(&examples_dir().join("runtime_http_crud_service.mira"))
            .expect("runtime_http_crud_service should validate");
    let lowered_http_crud = lower_program_for_direct_exec(&runtime_http_crud)
        .expect("runtime_http_crud_service should lower");
    let http_crud_summary = verify_lowered_tests_portably(&lowered_http_crud)
        .expect("portable http crud verification should succeed")
        .expect("runtime_http_crud_service should stay on portable bytecode path");
    assert_eq!("portable bytecode tests passed: 4/4", http_crud_summary);
}

#[test]
fn runtime_fs_bytes_example_uses_unique_path_across_native_and_portable_checks() {
    let temp_path = std::env::temp_dir().join(format!(
        "mira_runtime_fs_bytes_{}_{}.bin",
        std::process::id(),
        Instant::now().elapsed().as_nanos()
    ));
    let source = fs::read_to_string(examples_dir().join("runtime_fs_bytes.mira"))
        .expect("runtime_fs_bytes example should exist");
    let source = source.replace(
        "/tmp/mira_runtime_fs_bytes.bin",
        temp_path.to_str().expect("temp path should be valid utf-8"),
    );
    let program = parse_program(&source).expect("program should parse");
    let diagnostics = validate_program(&program);
    assert!(
        diagnostics.is_empty(),
        "runtime_fs_bytes example should validate after path rewrite: {diagnostics:?}"
    );

    let harness = emit_test_harness(&program).expect("test harness should emit");
    let binary =
        compile_c_source("itest_runtime_fs_bytes_unique", &harness).expect("clang should compile");
    let output = run_binary(&binary).expect("binary should run");
    assert!(
        output.status.success(),
        "runtime_fs_bytes native tests failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let lowered = lower_program_for_direct_exec(&program).expect("runtime_fs_bytes should lower");
    let direct_result = run_lowered_function(
        &lowered,
        "roundtrip_byte",
        &std::collections::HashMap::new(),
    )
    .expect("direct runtime fs bytes execution works");
    assert_eq!(RuntimeValue::U8(66), direct_result);

    let bytecode = compile_bytecode_program(&lowered).expect("runtime_fs_bytes bytecode");
    let bytecode_result = run_bytecode_function(
        &bytecode,
        "roundtrip_byte",
        &std::collections::HashMap::new(),
    )
    .expect("bytecode runtime fs bytes execution works");
    assert_eq!(RuntimeValue::U8(66), bytecode_result);

    let summary = verify_lowered_tests_portably(&lowered)
        .expect("portable fs-bytes verification should succeed")
        .expect("runtime_fs_bytes should stay on portable bytecode path");
    assert_eq!("portable bytecode tests passed: 1/1", summary);

    let _ = fs::remove_file(temp_path);
}

#[test]
fn machine_ir_lowering_covers_kernel_and_runtime_examples() {
    for file_name in [
        "sum_abs.mira",
        "bit_ops.mira",
        "runtime_http_worker_service.mira",
        "runtime_job_runner.mira",
        "runtime_timeout_cancel.mira",
        "runtime_fs.mira",
        "runtime_db_sqlite.mira",
        "runtime_spawn.mira",
        "runtime_spawn_bytes.mira",
        "runtime_spawn_split.mira",
        "runtime_spawn_handle.mira",
        "runtime_net.mira",
        "runtime_net_bytes.mira",
        "runtime_net_server_bytes.mira",
        "runtime_http_route.mira",
        "runtime_http_middleware.mira",
        "runtime_http_cookie_flow.mira",
        "runtime_http_header_cookie_json_api.mira",
        "runtime_http_crud_service.mira",
        "runtime_http_query_std.mira",
        "runtime_http_server_handle.mira",
        "runtime_http_server_framework.mira",
        "runtime_service_api_template.mira",
        "runtime_service_worker_template.mira",
        "runtime_ffi.mira",
        "runtime_ffi_cstr.mira",
        "runtime_ffi_lib.mira",
    ] {
        let source = examples_dir().join(file_name);
        let program = load_and_validate(&source).expect("program should validate");
        let lowered = lower_program_for_direct_exec(&program).expect("program should lower");
        let bytecode = compile_bytecode_program(&lowered).expect("bytecode should compile");
        let machine = lower_bytecode_to_machine_program(&bytecode);
        validate_machine_program(&machine).expect("machine IR should validate");
        assert_eq!(bytecode.functions.len(), machine.functions.len());
    }
}

#[test]
fn bytecode_benchmark_suite_produces_results() {
    let output = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("tmp")
        .join("mira-native")
        .join("itest-bytecode-bench.json");
    let results = run_bytecode_benchmark_suite(&examples_dir(), Some(&output))
        .expect("bytecode bench suite should run");
    assert_eq!(3, results.len());
    assert!(results.iter().all(|result| result.median_ns > 0));
    assert!(output.exists());
}

#[test]
fn arm64_asm_benchmark_suite_produces_results() {
    if !cfg!(target_arch = "aarch64") || !cfg!(target_os = "macos") {
        return;
    }
    let output = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("tmp")
        .join("mira-native")
        .join("itest-bench-asm-arm64.json");
    let results = run_arm64_benchmark_suite(&examples_dir(), Some(&output))
        .expect("arm64 asm bench suite should run");
    assert_eq!(3, results.len());
    assert!(results.iter().all(|result| result.median_ns > 0));
    assert!(output.exists());
}

#[test]
fn arm64_asm_executes_bit_ops_example() {
    if !cfg!(target_arch = "aarch64") || !cfg!(target_os = "macos") {
        return;
    }
    let source = examples_dir().join("bit_ops.mira");
    let program = load_and_validate(&source).expect("program should validate");
    let lowered = lower_program_for_direct_exec(&program).expect("program should lower");
    let bytecode = compile_bytecode_program(&lowered).expect("bytecode should compile");
    let asm_source = emit_arm64_library(&bytecode).expect("arm64 asm should emit");
    let binary = compile_clang_bundle_with_runtime_support(
        "itest_bit_ops_arm64_exec",
        &[("s", &asm_source), ("c", &bit_ops_driver_source())],
        &["-std=c11"],
    )
    .expect("arm64 mixed bundle should compile");
    let output = run_binary(&binary).expect("binary should run");
    assert!(
        output.status.success(),
        "arm64 bit-ops binary failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn arm64_asm_executes_runtime_examples() {
    arm64_runtime_harness_passes("runtime_caps.mira");
    arm64_runtime_harness_passes("runtime_fs.mira");
    arm64_runtime_harness_passes("runtime_spawn.mira");
    arm64_runtime_harness_passes("runtime_ffi.mira");
    arm64_runtime_harness_passes("runtime_http_worker_service.mira");
    arm64_runtime_harness_passes("runtime_job_runner.mira");
    arm64_runtime_harness_passes("runtime_reference_backend_service.mira");
    arm64_runtime_harness_passes("runtime_timeout_cancel.mira");
}

#[test]
fn x86_64_asm_emits_and_compiles_for_cross_platform_object_formats() {
    for (triple, file_name) in [
        ("x86_64-apple-macos13", "sum_abs.mira"),
        ("x86_64-unknown-linux-gnu", "dot_product.mira"),
        ("x86_64-pc-windows-msvc", "fib_iter.mira"),
    ] {
        let source = examples_dir().join(file_name);
        let program = load_and_validate(&source).expect("program should validate");
        let lowered = lower_program_for_direct_exec(&program).expect("program should lower");
        let bytecode = compile_bytecode_program(&lowered).expect("bytecode should compile");
        let asm_source = emit_x86_64_library(
            &bytecode,
            target_from_triple(triple).expect("triple should map to x86_64 target"),
        )
        .expect("x86_64 asm should emit");
        let stem = format!(
            "itest-{}-{}",
            file_name.trim_end_matches(".mira"),
            triple.replace(['-', '.'], "_")
        );
        let object = compile_clang_object_bundle(&stem, &[("s", &asm_source)], &[], triple)
            .expect("cross-target assembly should compile to object");
        assert!(object.exists(), "expected object artifact for {triple}");
    }
}

#[test]
fn x86_64_asm_benchmark_suite_produces_results() {
    if !cfg!(target_os = "macos") {
        return;
    }
    let output = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("tmp")
        .join("mira-native")
        .join("itest-bench-asm-x86_64.json");
    let results =
        run_x86_64_benchmark_suite(&examples_dir(), "x86_64-apple-macos13", Some(&output))
            .expect("x86_64 asm bench suite should run");
    assert_eq!(3, results.len());
    assert!(results.iter().all(|result| result.median_ns > 0));
    assert!(output.exists());
}

#[test]
fn service_benchmark_exists_for_c_and_emitted_paths() {
    let source = examples_dir().join("runtime_http_worker_service.mira");
    let c_output = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("tmp")
        .join("mira-native")
        .join("itest-bench-service-c.json");
    let c_results =
        run_single_source_benchmark(&source, "serve_request_batch", 8, 8, Some(&c_output))
            .expect("C-backed service benchmark should run");
    assert_eq!(1, c_results.len());
    assert!(c_results[0].median_ns > 0);
    assert!(c_output.exists());

    if cfg!(target_arch = "aarch64") && cfg!(target_os = "macos") {
        let arm64_output = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("tmp")
            .join("mira-native")
            .join("itest-bench-service-asm-arm64.json");
        let arm64_results = run_single_source_arm64_benchmark(
            &source,
            "serve_request_batch",
            8,
            8,
            Some(&arm64_output),
        )
        .expect("arm64 emitted service benchmark should run");
        assert_eq!(1, arm64_results.len());
        assert!(arm64_results[0].median_ns > 0);
        assert!(arm64_output.exists());
    }

    if cfg!(target_os = "macos") {
        let x86_output = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("tmp")
            .join("mira-native")
            .join("itest-bench-service-asm-x86_64.json");
        let x86_results = run_single_source_x86_64_benchmark(
            &source,
            "x86_64-apple-macos13",
            "serve_request_batch",
            8,
            8,
            Some(&x86_output),
        )
        .expect("x86_64 emitted service benchmark should run");
        assert_eq!(1, x86_results.len());
        assert!(x86_results[0].median_ns > 0);
        assert!(x86_output.exists());
    }
}

#[test]
fn runtime_recovery_benchmark_artifact_exists() {
    let source = examples_dir().join("runtime_multiworker_http_service.mira");
    let output = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("tmp")
        .join("mira-release")
        .join("concurrency-recovery-1.3.0.json");
    let results = run_single_source_benchmark(&source, "serve_multiworker_http", 8, 4, Some(&output))
        .expect("runtime recovery benchmark should run");
    assert_eq!(1, results.len());
    assert!(results[0].median_ns > 0);
    assert!(results[0].p95_ns > 0);
    assert!(results[0].units_per_second > 0.0);
    assert!(output.exists());
}

#[test]
fn analytics_benchmark_artifact_exists() {
    let json_output = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("tmp")
        .join("mira-release")
        .join("analytics-benchmark-2.3.0.json");
    let md_output = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("tmp")
        .join("mira-release")
        .join("analytics-benchmark-2.3.0.md");
    let script = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tools")
        .join("analytics_benchmark_matrix.py");
    let output = Command::new("python3")
        .arg(&script)
        .arg("--output-json")
        .arg(&json_output)
        .arg("--output-md")
        .arg(&md_output)
        .output()
        .expect("analytics benchmark script should run");
    assert!(
        output.status.success(),
        "analytics benchmark script failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(json_output.exists(), "analytics benchmark json should exist");
    assert!(md_output.exists(), "analytics benchmark markdown should exist");
    let json_text =
        fs::read_to_string(&json_output).expect("analytics benchmark json should be readable");
    assert!(
        json_text.contains("\"metrics_ingest_request\"")
            && json_text.contains("\"aggregation_worker\"")
            && json_text.contains("\"stream_analytics_pipeline\""),
        "analytics benchmark json should include all workloads"
    );
}

#[test]
fn advanced_backend_dominance_matrix_artifact_exists() {
    let json_output = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("tmp")
        .join("mira-release")
        .join("advanced-backend-matrix-2.6.0.json");
    let md_output = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("tmp")
        .join("mira-release")
        .join("advanced-backend-matrix-2.6.0.md");
    let diagnostics_output = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("tmp")
        .join("mira-release")
        .join("advanced-backend-matrix-2.6.0-diagnostics.json");
    let script = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tools")
        .join("advanced_backend_dominance_matrix.py");
    let output = Command::new("python3")
        .arg(&script)
        .arg("--output-json")
        .arg(&json_output)
        .arg("--output-md")
        .arg(&md_output)
        .arg("--diagnostics-json")
        .arg(&diagnostics_output)
        .output()
        .expect("advanced backend dominance matrix script should run");
    assert!(
        output.status.success(),
        "advanced backend dominance matrix failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(json_output.exists(), "advanced matrix json should exist");
    assert!(md_output.exists(), "advanced matrix markdown should exist");
    assert!(
        diagnostics_output.exists(),
        "advanced matrix diagnostics should exist"
    );
}

#[test]
fn distributed_benchmark_artifact_exists() {
    let json_output = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("tmp")
        .join("mira-release")
        .join("distributed-benchmark-2.4.0.json");
    let md_output = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("tmp")
        .join("mira-release")
        .join("distributed-benchmark-2.4.0.md");
    let script = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tools")
        .join("distributed_benchmark_matrix.py");
    let output = Command::new("python3")
        .arg(&script)
        .arg("--output-json")
        .arg(&json_output)
        .arg("--output-md")
        .arg(&md_output)
        .output()
        .expect("distributed benchmark script should run");
    assert!(
        output.status.success(),
        "distributed benchmark script failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(json_output.exists(), "distributed benchmark json should exist");
    assert!(md_output.exists(), "distributed benchmark markdown should exist");
    let json_text =
        fs::read_to_string(&json_output).expect("distributed benchmark json should be readable");
    assert!(
        json_text.contains("\"shard_messaging_edge\"")
            && json_text.contains("\"distributed_analytics_cluster\"")
            && json_text.contains("\"failover_rebalance_service\""),
        "distributed benchmark json should include all workloads"
    );
}

#[test]
fn x86_64_asm_executes_bit_ops_example() {
    if !cfg!(target_os = "macos") {
        return;
    }
    let source = examples_dir().join("bit_ops.mira");
    let program = load_and_validate(&source).expect("program should validate");
    let lowered = lower_program_for_direct_exec(&program).expect("program should lower");
    let bytecode = compile_bytecode_program(&lowered).expect("bytecode should compile");
    let asm_source = emit_x86_64_library(
        &bytecode,
        target_from_triple("x86_64-apple-macos13").expect("target should resolve"),
    )
    .expect("x86_64 asm should emit");
    let binary = compile_clang_bundle_for_target_with_runtime_support(
        "itest_bit_ops_x86_64_exec",
        &[("s", &asm_source), ("c", &bit_ops_driver_source())],
        &["-std=c11"],
        "x86_64-apple-macos13",
    )
    .expect("x86_64 mixed bundle should compile");
    let output = run_binary(&binary).expect("binary should run");
    assert!(
        output.status.success(),
        "x86_64 bit-ops binary failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn x86_64_asm_executes_runtime_examples() {
    x86_64_runtime_harness_passes("runtime_caps.mira");
    x86_64_runtime_harness_passes("runtime_fs.mira");
    x86_64_runtime_harness_passes("runtime_spawn.mira");
    x86_64_runtime_harness_passes("runtime_ffi.mira");
    x86_64_runtime_harness_passes("runtime_http_worker_service.mira");
    x86_64_runtime_harness_passes("runtime_job_runner.mira");
    x86_64_runtime_harness_passes("runtime_reference_backend_service.mira");
    x86_64_runtime_harness_passes("runtime_timeout_cancel.mira");
}

#[test]
fn emitted_asm_executes_agent_platform_services() {
    for name in [
        "runtime_agent_api_service.mira",
        "runtime_agent_stateful_service.mira",
        "runtime_agent_worker_queue_service.mira",
        "runtime_agent_recovery_service.mira",
    ] {
        arm64_runtime_harness_passes(name);
        x86_64_runtime_harness_passes(name);
    }
}

#[test]
fn promoted_emitted_service_examples_run_on_linux_and_windows_x86_64() {
    x86_64_cross_target_runtime_harness_passes(
        "runtime_emitted_reference_service.mira",
        "x86_64-unknown-linux-gnu",
    );
    x86_64_cross_target_runtime_harness_passes(
        "runtime_emitted_reference_service.mira",
        "x86_64-pc-windows-msvc",
    );
    x86_64_cross_target_runtime_harness_passes(
        "runtime_emitted_stateful_service.mira",
        "x86_64-unknown-linux-gnu",
    );
    x86_64_cross_target_runtime_harness_passes(
        "runtime_emitted_stateful_service.mira",
        "x86_64-pc-windows-msvc",
    );
}

#[test]
fn promoted_2_5_examples_run_on_default_native_backends() {
    for name in [
        "runtime_emitted_messaging_service.mira",
        "runtime_emitted_analytics_service.mira",
    ] {
        arm64_runtime_harness_passes(name);
        x86_64_runtime_harness_passes(name);
    }
}

#[test]
fn promoted_2_6_benchmark_examples_run_on_default_native_backends() {
    for name in [
        "runtime_advanced_messaging_benchmark.mira",
        "runtime_advanced_analytics_benchmark.mira",
    ] {
        arm64_runtime_harness_passes(name);
        x86_64_runtime_harness_passes(name);
    }
}

#[test]
fn promoted_2_6_benchmark_examples_run_on_linux_and_windows_x86_64() {
    for name in [
        "runtime_advanced_messaging_benchmark.mira",
        "runtime_advanced_analytics_benchmark.mira",
    ] {
        x86_64_cross_target_runtime_harness_passes(name, "x86_64-unknown-linux-gnu");
        x86_64_cross_target_runtime_harness_passes(name, "x86_64-pc-windows-msvc");
    }
}

#[test]
fn promoted_2_5_examples_run_on_linux_and_windows_x86_64() {
    for name in [
        "runtime_emitted_messaging_service.mira",
        "runtime_emitted_analytics_service.mira",
    ] {
        x86_64_cross_target_runtime_harness_passes(name, "x86_64-unknown-linux-gnu");
        x86_64_cross_target_runtime_harness_passes(name, "x86_64-pc-windows-msvc");
    }
}

#[test]
fn agent_platform_services_run_on_linux_x86_64() {
    for name in [
        "runtime_agent_api_service.mira",
        "runtime_agent_stateful_service.mira",
        "runtime_agent_worker_queue_service.mira",
        "runtime_agent_recovery_service.mira",
    ] {
        x86_64_cross_target_runtime_harness_passes(name, "x86_64-unknown-linux-gnu");
    }
}

#[test]
fn emitted_asm_runtime_net_examples_run_against_local_listener() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("listener should bind");
    let open_port = listener
        .local_addr()
        .expect("listener should have local addr")
        .port();
    let closed_port = reserve_closed_port();
    listener
        .set_nonblocking(true)
        .expect("listener should become nonblocking");
    let handle = thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(60);
        let mut accepted = 0usize;
        loop {
            match listener.accept() {
                Ok((mut stream, _addr)) => {
                    let mut buf = [0u8; 8];
                    let _ = stream.read(&mut buf);
                    accepted += 1;
                    if accepted >= 2 {
                        return;
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    if Instant::now() >= deadline {
                        return;
                    }
                    thread::sleep(Duration::from_millis(25));
                }
                Err(_) => return,
            }
        }
    });

    let source = fs::read_to_string(examples_dir().join("runtime_net.mira"))
        .expect("runtime_net example should exist");
    let source = source
        .replace("127.0.0.1:38417", &format!("127.0.0.1:{open_port}"))
        .replace("127.0.0.1:38418", &format!("127.0.0.1:{closed_port}"));

    arm64_runtime_net_harness_passes(&source);
    x86_64_runtime_net_harness_passes(&source);

    handle.join().expect("listener thread should finish");
}

#[test]
fn ast_json_roundtrip_is_valid() {
    let source = examples_dir().join("sum_abs.mira");
    let program = load_and_validate(&source).expect("program should validate");
    let json = render_program_json(&program).expect("ast json should render");
    let roundtrip = parse_program_json(&json).expect("ast json should parse");
    assert_eq!(program, roundtrip);
}

#[test]
fn binary_ir_roundtrip_is_valid() {
    let source = examples_dir().join("dot_product.mira");
    let program = load_and_validate(&source).expect("program should validate");
    let bytes = encode_program(&program).expect("binary ir should encode");
    let artifact = decode_artifact(&bytes).expect("binary ir should decode");
    let lowered = match artifact {
        BinaryArtifact::LoweredProgram(program) => program,
        other => panic!("expected lowered MIRB3 artifact, got {other:?}"),
    };
    assert_eq!(program.module, lowered.module);
    assert_eq!(program.functions.len(), lowered.functions.len());
    assert!(
        lowered.functions.iter().any(|function| function
            .blocks
            .iter()
            .any(|block| !block.statements.is_empty())),
        "expected MIRB3 lowered artifact to keep structured statements"
    );
    assert!(
        lowered
            .functions
            .iter()
            .flat_map(|function| function.blocks.iter())
            .any(|block| matches!(
                block.terminator,
                LoweredTerminator::Branch { .. }
                    | LoweredTerminator::Jump { .. }
                    | LoweredTerminator::Return { .. }
                    | LoweredTerminator::Match { .. }
            )),
        "expected MIRB3 lowered artifact to keep structured terminators"
    );
    assert!(
        lowered
            .tests
            .iter()
            .all(|test| !test.call.function_name.is_empty()),
        "expected MIRB3 lowered artifact to keep structured tests"
    );
    let lowered_diagnostics = validate_lowered_program(&lowered);
    assert!(
        lowered_diagnostics.is_empty(),
        "expected lowered MIRB3 artifact to validate cleanly, got {lowered_diagnostics:?}"
    );
    let harness = emit_test_harness_from_lowered(&lowered);
    let binary = compile_c_source("mirb_roundtrip_dot_product", &harness)
        .expect("clang should compile lowered MIRB harness");
    let output = run_binary(&binary).expect("binary should run");
    assert!(
        output.status.success(),
        "lowered MIRB tests failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn invalid_lowered_binary_artifact_is_reported() {
    let source = examples_dir().join("sum_abs.mira");
    let program = load_and_validate(&source).expect("program should validate");
    let bytes = encode_program(&program).expect("binary ir should encode");
    let mut lowered = match decode_artifact(&bytes).expect("binary ir should decode") {
        BinaryArtifact::LoweredProgram(program) => program,
        other => panic!("expected lowered MIRB3 artifact, got {other:?}"),
    };
    lowered.functions[0].blocks[0].label = "bx".to_string();
    let diagnostics = validate_lowered_program(&lowered);
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.contains("LOWERED_MISSING_ENTRY")),
        "expected lowered entry-block diagnostics, got {diagnostics:?}"
    );
}

#[test]
fn ast_and_binary_roundtrip_support_match_and_extended_types() {
    for file_name in [
        "match_dispatch.mira",
        "mul_add_f64.mira",
        "vec_pick.mira",
        "buf_alloc.mira",
        "bit_ops.mira",
        "runtime_caps.mira",
        "runtime_task_sleep.mira",
        "runtime_fs.mira",
        "runtime_fs_bytes.mira",
        "runtime_db_sqlite.mira",
        "runtime_spawn_bytes.mira",
        "runtime_spawn_split.mira",
        "runtime_spawn_handle.mira",
        "runtime_net_bytes.mira",
        "runtime_net_server_bytes.mira",
        "runtime_http_route.mira",
        "runtime_http_middleware.mira",
        "runtime_http_cookie_flow.mira",
        "runtime_http_header_cookie_json_api.mira",
        "runtime_http_response_model.mira",
        "runtime_http_crud_service.mira",
        "runtime_direct_message_service.mira",
        "runtime_room_fanout_service.mira",
        "runtime_offline_catchup_worker.mira",
        "runtime_production_messenger_backend.mira",
        "runtime_production_analytics_platform.mira",
        "runtime_advanced_messaging_benchmark.mira",
        "runtime_advanced_analytics_benchmark.mira",
        "runtime_service_api_template.mira",
        "runtime_service_worker_template.mira",
        "runtime_multiworker_http_service.mira",
        "runtime_deadline_job_system.mira",
        "runtime_worker_supervisor.mira",
        "runtime_http_server_handle.mira",
        "runtime_http_server_framework.mira",
        "runtime_ffi.mira",
        "runtime_ffi_cstr.mira",
        "runtime_ffi_lib.mira",
        "runtime_spawn.mira",
        "runtime_net.mira",
        "point_manhattan.mira",
        "signal_enum.mira",
        "payload_message.mira",
        "payload_eq_literals.mira",
    ] {
        let source = examples_dir().join(file_name);
        let program = load_and_validate(&source).expect("program should validate");
        let json = render_program_json(&program).expect("ast json should render");
        let json_roundtrip = parse_program_json(&json).expect("ast json should parse");
        assert_eq!(program, json_roundtrip);
        let bytes = encode_program(&program).expect("binary ir should encode");
        let artifact = decode_artifact(&bytes).expect("binary ir should decode");
        match artifact {
            BinaryArtifact::LoweredProgram(lowered) => {
                assert_eq!(program.module, lowered.module);
                assert_eq!(program.functions.len(), lowered.functions.len());
                assert!(
                    lowered
                        .functions
                        .iter()
                        .flat_map(|function| function.blocks.iter())
                        .all(|block| block
                            .statements
                            .iter()
                            .all(|statement| matches!(statement, LoweredStatement::Assign(_)))),
                    "expected lowered blocks to serialize structured statements"
                );
            }
            other => panic!("expected lowered MIRB3 artifact, got {other:?}"),
        }
    }
}

#[test]
fn invalid_type_declaration_and_usage_is_reported() {
    let source = r#"
module invalid.types@1
target native
type point = struct[x:i32,x:i32]

func bad
arg p:missing
ret i32
eff pure
block b0
  v0:point = make point 1i32
  return 0i32
end
"#;
    let program = parse_program(source).expect("program should parse");
    let diagnostics = validate_program(&program);
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.error_code == "DUPLICATE_FIELD"),
        "expected duplicate field diagnostics, got {diagnostics:?}"
    );
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.error_code == "UNKNOWN_TYPE"),
        "expected unknown type diagnostics, got {diagnostics:?}"
    );
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.error_code == "MAKE_ARITY"),
        "expected make arity diagnostics, got {diagnostics:?}"
    );
}

#[test]
fn duplicate_function_arguments_are_reported() {
    let source = r#"
module invalid.args@1
target native

func bad
arg xs:span[i32]
arg xs:span[i32]
ret i32
eff pure
block b0
  return 0i32
end
"#;
    let program = parse_program(source).expect("program should parse");
    let diagnostics = validate_program(&program);
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.error_code == "DUPLICATE_BINDING"),
        "expected duplicate function argument diagnostics, got {diagnostics:?}"
    );
}

#[test]
fn non_entry_blocks_must_receive_function_args_explicitly() {
    let source = r#"
module invalid.cfg@1
target native

func bad
arg xs:span[i32]
ret u32
eff pure
block b0
  jump b1()
block b1
  v0:u32 = len xs
  return v0
end
"#;
    let program = parse_program(source).expect("program should parse");
    let diagnostics = validate_program(&program);
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.error_code == "UNKNOWN_VALUE"),
        "expected non-entry arg access diagnostics, got {diagnostics:?}"
    );
}

#[test]
fn text_patch_updates_program() {
    let source = examples_dir().join("sum_abs.mira");
    let patch = examples_dir().join("sum_abs_sat_add.mirapatch");
    let program = load_and_validate(&source).expect("program should validate");
    let patch_text = fs::read_to_string(&patch).expect("patch should exist");
    let patched = apply_patch_text(&program, &patch_text).expect("patch should apply");
    let rendered = format_program(&patched);
    assert!(rendered.contains("sat_add acc v5"));
}

#[test]
fn invalid_effect_capability_contract_is_reported() {
    let source = r#"
module invalid.effects@1
target native

func bad
ret i32
eff pure net
cap net("api.internal:443")
block b0
  return 1i32
end
"#;
    let program = parse_program(source).expect("program should parse");
    let diagnostics = validate_program(&program);
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.error_code == "PURE_EFFECT_CONFLICT"),
        "expected pure/effect conflict diagnostics, got {diagnostics:?}"
    );
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.error_code == "PURE_CAPABILITY_CONFLICT"),
        "expected pure/capability conflict diagnostics, got {diagnostics:?}"
    );
}

#[test]
fn payload_enum_bare_literal_and_field_misuse_are_reported() {
    let source = r#"
module invalid.payload@1
target native
type message = enum[idle,data[value:i32]]

func bad
ret i32
eff pure
block b0
  v0:message = const message.data
  v1:i32 = field v0 data
  return v1
end
"#;
    let program = parse_program(source).expect("program should parse");
    let diagnostics = validate_program(&program);
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.error_code == "INVALID_ENUM_LITERAL"),
        "expected invalid enum literal diagnostics, got {diagnostics:?}"
    );
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.error_code == "FIELD_ON_ENUM"),
        "expected enum field path diagnostics, got {diagnostics:?}"
    );
}

#[test]
fn alloc_without_effect_and_bad_store_are_reported() {
    let source = r#"
module invalid.alloc@1
target native

func bad
ret i32
eff pure
block b0
  v0:own[buf[i32]] = alloc heap 2u32
  v1:edit[buf[i32]] = edit v0
  v2:edit[buf[i32]] = store v1 0u32 true
  return 0i32
end
"#;
    let program = parse_program(source).expect("program should parse");
    let diagnostics = validate_program(&program);
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.error_code == "MISSING_REQUIRED_EFFECT"),
        "expected missing alloc effect diagnostics, got {diagnostics:?}"
    );
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.error_code == "STORE_VALUE_TYPE"),
        "expected bad store value diagnostics, got {diagnostics:?}"
    );
}

#[test]
fn invalid_drop_and_arena_return_are_reported() {
    let source = r#"
module invalid.ownership@1
target native

func bad_drop
ret i32
eff alloc
block b0
  v0:own[buf[i32]] = alloc heap 1u32
  v1:b1 = drop v0
  v2:i32 = load v0 0u32
  return v2
end

func bad_double_drop
ret b1
eff alloc
block b0
  v0:own[buf[i32]] = alloc heap 1u32
  v1:b1 = drop v0
  v2:b1 = drop v0
  return v2
end

func bad_leak
ret i32
eff alloc
block b0
  v0:own[buf[i32]] = alloc heap 2u32
  v1:view[buf[i32]] = view v0
  v2:i32 = load v1 0u32
  return v2
end

func bad_arg_escape
arg buf:own[buf[i32]]
ret i32
eff alloc
block b0
  v0:view[buf[i32]] = view buf
  v1:i32 = load v0 0u32
  return v1
end

func bad_param_alias
arg buf:own[buf[i32]]
ret b1
eff alloc
block b0
  jump b1(buf, buf)
block b1(left:own[buf[i32]], right:own[buf[i32]])
  return true
end

func bad_transfer_missing
ret b1
eff alloc
block b0
  v0:own[buf[i32]] = alloc heap 1u32
  jump b1()
block b1
  return true
end

func bad_view_after_drop
ret i32
eff alloc
block b0
  v0:own[buf[i32]] = alloc heap 1u32
  v1:view[buf[i32]] = view v0
  v2:b1 = drop v0
  v3:i32 = load v1 0u32
  return v3
end

func bad_borrow_live_on_drop
ret i32
eff alloc
block b0
  v0:own[buf[i32]] = alloc heap 1u32
  v1:view[buf[i32]] = view v0
  v2:b1 = drop v0
  v3:i32 = load v1 0u32
  return v3
end

func bad_view_param_after_drop
ret i32
eff alloc
block b0
  v0:own[buf[i32]] = alloc heap 1u32
  v1:view[buf[i32]] = view v0
  jump b1(v0, v1)
block b1(buf:own[buf[i32]], seen:view[buf[i32]])
  v2:b1 = drop buf
  v3:i32 = load seen 0u32
  return v3
end

func bad_borrow_escape
ret view[buf[i32]]
eff alloc
block b0
  v0:own[buf[i32]] = alloc heap 1u32
  v1:view[buf[i32]] = view v0
  return v1
end

func bad_borrow_owner_transfer
ret i32
eff alloc
block b0
  v0:own[buf[i32]] = alloc heap 1u32
  v1:view[buf[i32]] = view v0
  jump b1(v1)
block b1(seen:view[buf[i32]])
  v2:i32 = load seen 0u32
  return v2
end

func bad_join_mismatch
arg seed:own[buf[i32]]
ret b1
eff alloc
block b0
  branch true b1(seed) b2(seed)
block b1(buf:own[buf[i32]])
  jump b3(buf)
block b2(buf:own[buf[i32]])
  v0:b1 = drop buf
  v1:own[buf[i32]] = alloc heap 1u32
  jump b3(v1)
block b3(buf:own[buf[i32]])
  v2:b1 = drop buf
  return true
end

func bad_borrow_join_mismatch
ret i32
eff alloc
block b0
  v0:own[buf[i32]] = alloc heap 1u32
  v1:own[buf[i32]] = alloc heap 1u32
  v2:view[buf[i32]] = view v0
  v3:view[buf[i32]] = view v1
  branch true b4(v0, v1, v2) b5(v0, v1, v3)
block b4(left:own[buf[i32]], right:own[buf[i32]], seen:view[buf[i32]])
  jump b6(left, right, seen)
block b5(left:own[buf[i32]], right:own[buf[i32]], seen:view[buf[i32]])
  jump b6(left, right, seen)
block b6(left:own[buf[i32]], right:own[buf[i32]], seen:view[buf[i32]])
  v4:b1 = drop left
  v5:b1 = drop right
  return 0i32
end

func bad_arena_return
ret own[buf[i32]]
eff alloc
block b0
  v0:own[buf[i32]] = alloc arena 2u32
  return v0
end

func bad_stack_return
ret own[buf[i32]]
eff alloc
block b0
  v0:own[buf[i32]] = alloc stack 2u32
  return v0
end

func wrong_drop
ret b1
eff alloc
block b0
  v0:b1 = drop 1i32
  return v0
end

func bad_runtime_handle_return
ret u64
eff spawn
cap spawn("/bin/echo")
block b0
  v0:u64 = spawn_open /bin/echo HI
  return v0
end

func bad_runtime_handle_leak
ret u8
eff db
cap db("tmp/mira-tests/runtime-handle-leak.db")
block b0
  v0:u64 = db_open tmp/mira-tests/runtime-handle-leak.db
  return 0u8
end

func bad_runtime_double_close
ret b1
eff spawn
cap spawn("/bin/echo")
block b0
  v0:u64 = spawn_open /bin/echo HI
  v1:b1 = spawn_close v0
  v2:b1 = spawn_close v0
  return v2
end

func bad_runtime_use_after_close
ret u8
eff net
cap net("127.0.0.1:38531")
block b0
  v0:u64 = net_listen
  v1:b1 = net_close v0
  v2:u64 = net_accept v0
  return 0u8
end

func bad_runtime_transfer
ret u8
eff net
cap net("127.0.0.1:38531")
block b0
  v0:u64 = net_listen
  branch true b1() b2(v0)
block b1
  return 0u8
block b2(listener:u64)
  v1:b1 = net_close listener
  return 1u8
end

func bad_runtime_alias
ret b1
eff net
cap net("127.0.0.1:38531")
block b0
  v0:u64 = net_listen
  jump b1(v0, v0)
block b1(left:u64, right:u64)
  v1:b1 = net_close left
  v2:b1 = net_close right
  return true
end

type pair = struct[left:i32,right:i32]

func bad_named_borrow_escape
arg payload:own[pair]
ret view[pair]
eff pure
block b0
  v0:view[pair] = view payload
  return v0
end

func bad_named_use_after_drop
arg payload:own[pair]
ret i32
eff pure
block b0
  v0:view[pair] = view payload
  v1:b1 = drop payload
  v2:i32 = field v0 left
  return v2
end

func bad_vec_borrow_owner_transfer
arg items:own[vec[2,i32]]
ret i32
eff pure
block b0
  v0:view[vec[2,i32]] = view items
  jump b1(v0)
block b1(seen:view[vec[2,i32]])
  v1:i32 = load seen 0u32
  return v1
end
"#;
    let program = parse_program(source).expect("program should parse");
    let diagnostics = validate_program(&program);
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.error_code == "USE_AFTER_DROP"),
        "expected use-after-drop diagnostics, got {diagnostics:?}"
    );
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.error_code == "ARENA_RETURN_UNSUPPORTED"),
        "expected arena return diagnostics, got {diagnostics:?}"
    );
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.error_code == "STACK_RETURN_UNSUPPORTED"),
        "expected stack return diagnostics, got {diagnostics:?}"
    );
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.error_code == "DROP_OPERAND_TYPE"),
        "expected drop operand diagnostics, got {diagnostics:?}"
    );
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.error_code == "DOUBLE_DROP"),
        "expected double-drop diagnostics, got {diagnostics:?}"
    );
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.error_code == "HEAP_BUFFER_LEAK"),
        "expected heap leak diagnostics, got {diagnostics:?}"
    );
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.error_code == "OWNED_ARG_ESCAPE"),
        "expected owned-arg escape diagnostics, got {diagnostics:?}"
    );
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.error_code == "OWNED_PARAM_ALIAS"),
        "expected owned-param alias diagnostics, got {diagnostics:?}"
    );
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.error_code == "OWNED_TRANSFER_MISSING"),
        "expected owned-transfer diagnostics, got {diagnostics:?}"
    );
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.error_code == "BORROW_ESCAPE"),
        "expected borrow-escape diagnostics, got {diagnostics:?}"
    );
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.error_code == "BORROW_OWNER_TRANSFER_MISSING"),
        "expected borrow-owner-transfer diagnostics, got {diagnostics:?}"
    );
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.error_code == "BORROW_STILL_LIVE_ON_DROP"),
        "expected borrow-live-on-drop diagnostics, got {diagnostics:?}"
    );
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.error_code == "OWNERSHIP_JOIN_MISMATCH"),
        "expected ownership-join diagnostics, got {diagnostics:?}"
    );
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.error_code == "BORROW_JOIN_MISMATCH"),
        "expected borrow-join diagnostics, got {diagnostics:?}"
    );
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.error_code == "RUNTIME_HANDLE_ESCAPE"),
        "expected runtime-handle escape diagnostics, got {diagnostics:?}"
    );
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.error_code == "RUNTIME_HANDLE_LEAK"),
        "expected runtime-handle leak diagnostics, got {diagnostics:?}"
    );
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.error_code == "DOUBLE_CLOSE"),
        "expected double-close diagnostics, got {diagnostics:?}"
    );
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.error_code == "USE_AFTER_CLOSE"),
        "expected use-after-close diagnostics, got {diagnostics:?}"
    );
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.error_code == "RUNTIME_HANDLE_TRANSFER_MISSING"),
        "expected runtime-handle transfer diagnostics, got {diagnostics:?}"
    );
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.error_code == "RUNTIME_HANDLE_ALIAS"),
        "expected runtime-handle alias diagnostics, got {diagnostics:?}"
    );
    assert!(
        diagnostics
            .iter()
            .filter(|diagnostic| diagnostic.error_code == "BORROW_ESCAPE")
            .any(|diagnostic| diagnostic.observed.as_deref() == Some("v0")),
        "expected non-buffer borrow-escape diagnostics, got {diagnostics:?}"
    );
    assert!(
        diagnostics
            .iter()
            .filter(|diagnostic| diagnostic.error_code == "USE_AFTER_DROP")
            .any(|diagnostic| diagnostic.observed.as_deref() == Some("v0")),
        "expected non-buffer use-after-drop diagnostics, got {diagnostics:?}"
    );
    assert!(
        diagnostics
            .iter()
            .filter(|diagnostic| diagnostic.error_code == "BORROW_OWNER_TRANSFER_MISSING")
            .any(|diagnostic| diagnostic.observed.as_deref().is_some_and(|observed| observed.contains("b1::seen"))),
        "expected non-buffer borrow-owner-transfer diagnostics, got {diagnostics:?}"
    );
}

#[test]
fn arena_usage_only_flags_actual_arena_returns() {
    let source = r#"
module precise.arena.return@1
target native

func arena_local_heap_return
ret own[buf[i32]]
eff alloc
block b0
  v0:own[buf[i32]] = alloc arena 2u32
  v1:b1 = drop v0
  v2:own[buf[i32]] = alloc heap 2u32
  return v2
end
"#;
    let program = parse_program(source).expect("program should parse");
    let diagnostics = validate_program(&program);
    assert!(
        diagnostics
            .iter()
            .all(|diagnostic| diagnostic.error_code != "ARENA_RETURN_UNSUPPORTED"),
        "did not expect coarse arena return diagnostics, got {diagnostics:?}"
    );
    assert!(
        diagnostics
            .iter()
            .all(|diagnostic| diagnostic.error_code != "STACK_RETURN_UNSUPPORTED"),
        "did not expect stack return diagnostics, got {diagnostics:?}"
    );
}

#[test]
fn aggregate_views_borrow_cleanly_beyond_buffers() {
    let source = r#"
module borrow.aggregate.views.inline@1
target native

type pair = struct[left:i32,right:i32]

func borrow_named_passthrough
arg payload:own[pair]
ret own[pair]
eff pure
block b0
  v0:view[pair] = view payload
  v1:i32 = field v0 left
  v2:b1 = eq v1 7i32
  branch v2 b1(payload) b2(payload)
block b1(out:own[pair])
  return out
block b2(out:own[pair])
  return out
end

func borrow_vec_passthrough
arg items:own[vec[2,i32]]
ret own[vec[2,i32]]
eff pure
block b0
  v0:view[vec[2,i32]] = view items
  v1:i32 = load v0 0u32
  v2:b1 = eq v1 1i32
  branch v2 b1(items) b2(items)
block b1(out:own[vec[2,i32]])
  return out
block b2(out:own[vec[2,i32]])
  return out
end
"#;
    let program = parse_program(source).expect("program should parse");
    let diagnostics = validate_program(&program);
    let ownership_errors = diagnostics
        .iter()
        .filter(|diagnostic| {
            matches!(
                diagnostic.error_code.as_str(),
                "BORROW_ESCAPE"
                    | "USE_AFTER_DROP"
                    | "OWNED_ARG_ESCAPE"
                    | "OWNED_TRANSFER_MISSING"
                    | "BORROW_OWNER_TRANSFER_MISSING"
                    | "BORROW_JOIN_MISMATCH"
                    | "OWNERSHIP_JOIN_MISMATCH"
            )
        })
        .collect::<Vec<_>>();
    assert!(
        ownership_errors.is_empty(),
        "did not expect aggregate-view borrow diagnostics, got {ownership_errors:?}"
    );
}

#[test]
fn invalid_runtime_capabilities_are_reported() {
    let source = r#"
module invalid.runtime@1
target native

func bad_clock
ret u64
eff clock
block b0
  v0:u64 = clock_now_ns
  return v0
end

func bad_rand_effect
ret u32
eff pure
cap rand("seed=7u32")
block b0
  v0:u32 = rand_u32
  return v0
end

func bad_rand_cap
ret u32
eff rand
cap rand("system")
block b0
  v0:u32 = rand_u32
  return v0
end

func bad_fs_cap
ret u32
eff fs.read
cap fs("")
block b0
  v0:u32 = fs_read_u32
  return v0
end

func bad_fs_effect
ret b1
eff pure
cap fs("/tmp/mira_runtime_fs_invalid.txt")
block b0
  v0:b1 = fs_write_u32 1u32
  return v0
end

func bad_ffi_cap
arg x:i32
ret i32
eff ffi
cap ffi("abs, bad-symbol")
block b0
  v0:i32 = ffi_call abs x
  return v0
end

func bad_ffi_symbol
arg x:i32
ret i32
eff ffi
cap ffi("labs")
block b0
  v0:i32 = ffi_call abs x
  return v0
end

func bad_ffi_type
ret span[i32]
eff ffi
cap ffi("abs")
block b0
  v0:span[i32] = ffi_call abs 1i32
  return v0
end

func bad_spawn_cap
ret i32
eff spawn
cap spawn("true, bad command")
block b0
  v0:i32 = spawn_call true
  return v0
end

func bad_spawn_command
ret i32
eff spawn
cap spawn("false")
block b0
  v0:i32 = spawn_call true
  return v0
end

func bad_spawn_type
ret u32
eff spawn
cap spawn("true")
block b0
  v0:u32 = spawn_call true
  return v0
end

func bad_net_cap
ret b1
eff net
cap net("127.0.0.1")
block b0
  v0:b1 = net_connect
  return v0
end

func bad_net_type
ret i32
eff net
cap net("127.0.0.1:80")
block b0
  v0:i32 = net_connect
  return v0
end
"#;
    let program = parse_program(source).expect("program should parse");
    let diagnostics = validate_program(&program);
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.error_code == "MISSING_REQUIRED_CAPABILITY"),
        "expected missing capability diagnostics, got {diagnostics:?}"
    );
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.error_code == "MISSING_REQUIRED_EFFECT"),
        "expected missing effect diagnostics, got {diagnostics:?}"
    );
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.error_code == "INVALID_RANDOM_CAPABILITY"),
        "expected invalid random capability diagnostics, got {diagnostics:?}"
    );
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.error_code == "INVALID_FILESYSTEM_CAPABILITY"),
        "expected invalid fs capability diagnostics, got {diagnostics:?}"
    );
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.error_code == "INVALID_FFI_CAPABILITY"),
        "expected invalid ffi capability diagnostics, got {diagnostics:?}"
    );
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.error_code == "FFI_SYMBOL_NOT_ALLOWED"),
        "expected ffi symbol allowlist diagnostics, got {diagnostics:?}"
    );
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.error_code == "FFI_RESULT_TYPE"),
        "expected ffi result type diagnostics, got {diagnostics:?}"
    );
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.error_code == "INVALID_SPAWN_CAPABILITY"),
        "expected invalid spawn capability diagnostics, got {diagnostics:?}"
    );
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.error_code == "SPAWN_COMMAND_NOT_ALLOWED"),
        "expected spawn command allowlist diagnostics, got {diagnostics:?}"
    );
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.error_code == "SPAWN_RESULT_TYPE"),
        "expected spawn result type diagnostics, got {diagnostics:?}"
    );
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.error_code == "INVALID_NET_CAPABILITY"),
        "expected invalid net capability diagnostics, got {diagnostics:?}"
    );
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.error_code == "NET_RESULT_TYPE"),
        "expected net result type diagnostics, got {diagnostics:?}"
    );
    assert!(
        diagnostics
            .iter()
            .filter(|diagnostic| diagnostic.error_code == "MISSING_REQUIRED_EFFECT")
            .count()
            >= 2,
        "expected multiple missing effect diagnostics, got {diagnostics:?}"
    );
}

#[test]
fn runtime_net_example_runs_against_local_listener() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("listener should bind");
    let open_port = listener
        .local_addr()
        .expect("listener should have local addr")
        .port();
    let closed_port = reserve_closed_port();
    listener
        .set_nonblocking(true)
        .expect("listener should become nonblocking");
    let handle = thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(60);
        let mut accepted = 0usize;
        loop {
            match listener.accept() {
                Ok((mut stream, _addr)) => {
                    let mut buf = [0u8; 8];
                    let _ = stream.read(&mut buf);
                    accepted += 1;
                    if accepted >= 1 {
                        return;
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    if Instant::now() >= deadline {
                        return;
                    }
                    thread::sleep(Duration::from_millis(25));
                }
                Err(_) => return,
            }
        }
    });

    let source = fs::read_to_string(examples_dir().join("runtime_net.mira"))
        .expect("runtime_net example should exist");
    let source = source
        .replace("127.0.0.1:38417", &format!("127.0.0.1:{open_port}"))
        .replace("127.0.0.1:38418", &format!("127.0.0.1:{closed_port}"));
    let program = parse_program(&source).expect("program should parse");
    let diagnostics = validate_program(&program);
    assert!(
        diagnostics.is_empty(),
        "runtime_net example should validate after port rewrite: {diagnostics:?}"
    );
    let harness = emit_test_harness(&program).expect("test harness should emit");
    let binary = compile_c_source("itest_runtime_net", &harness).expect("clang should compile");
    let output = run_binary(&binary).expect("binary should run");
    handle.join().expect("listener thread should finish");
    assert!(
        output.status.success(),
        "runtime_net native tests failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn runtime_net_example_runs_portably_against_local_listener() {
    let closed_port = reserve_closed_port();
    let source = fs::read_to_string(examples_dir().join("runtime_net.mira"))
        .expect("runtime_net example should exist");
    let rewrite_program = |open_port: u16| {
        let rewritten = source
            .replace("127.0.0.1:38417", &format!("127.0.0.1:{open_port}"))
            .replace("127.0.0.1:38418", &format!("127.0.0.1:{closed_port}"));
        let program = parse_program(&rewritten).expect("program should parse");
        let diagnostics = validate_program(&program);
        assert!(
            diagnostics.is_empty(),
            "runtime_net example should validate after port rewrite: {diagnostics:?}"
        );
        program
    };
    let spawn_listener = || {
        let listener = TcpListener::bind("127.0.0.1:0").expect("listener should bind");
        let open_port = listener
            .local_addr()
            .expect("listener should have local addr")
            .port();
        let handle = thread::spawn(move || {
            let (mut stream, _addr) = listener.accept().expect("listener should accept");
            let mut buf = [0u8; 8];
            let _ = stream.read(&mut buf);
        });
        (open_port, handle)
    };

    let (direct_port, direct_listener) = spawn_listener();
    let direct_program = rewrite_program(direct_port);
    let direct_lowered =
        lower_program_for_direct_exec(&direct_program).expect("runtime_net should lower");
    let direct_open = run_lowered_function(
        &direct_lowered,
        "connect_listener",
        &std::collections::HashMap::new(),
    )
    .expect("direct runtime net open-port execution works");
    assert_eq!(RuntimeValue::Bool(true), direct_open);
    let direct_closed = run_lowered_function(
        &direct_lowered,
        "connect_closed_port",
        &std::collections::HashMap::new(),
    )
    .expect("direct runtime net closed-port execution works");
    assert_eq!(RuntimeValue::Bool(false), direct_closed);
    direct_listener
        .join()
        .expect("direct listener thread should finish");

    let (bytecode_port, bytecode_listener) = spawn_listener();
    let bytecode_program = rewrite_program(bytecode_port);
    let bytecode_lowered =
        lower_program_for_direct_exec(&bytecode_program).expect("runtime_net should lower");
    let bytecode = compile_bytecode_program(&bytecode_lowered).expect("runtime_net bytecode");
    let bytecode_open = run_bytecode_function(
        &bytecode,
        "connect_listener",
        &std::collections::HashMap::new(),
    )
    .expect("bytecode runtime net open-port execution works");
    assert_eq!(RuntimeValue::Bool(true), bytecode_open);
    let bytecode_closed = run_bytecode_function(
        &bytecode,
        "connect_closed_port",
        &std::collections::HashMap::new(),
    )
    .expect("bytecode runtime net closed-port execution works");
    assert_eq!(RuntimeValue::Bool(false), bytecode_closed);
    bytecode_listener
        .join()
        .expect("bytecode listener thread should finish");

    let (verify_port, verify_listener) = spawn_listener();
    let verify_program = rewrite_program(verify_port);
    let verify_lowered =
        lower_program_for_direct_exec(&verify_program).expect("runtime_net should lower");
    let summary = verify_lowered_tests_portably(&verify_lowered)
        .expect("portable runtime_net verification should succeed")
        .expect("runtime_net should stay on portable bytecode path");
    assert_eq!("portable bytecode tests passed: 2/2", summary);
    verify_listener
        .join()
        .expect("verify listener thread should finish");
}

#[test]
fn runtime_net_bytes_example_runs_against_local_listeners() {
    let write_listener = TcpListener::bind("127.0.0.1:0").expect("write listener should bind");
    let write_port = write_listener
        .local_addr()
        .expect("write listener should have local addr")
        .port();
    let exchange_listener =
        TcpListener::bind("127.0.0.1:0").expect("exchange listener should bind");
    let exchange_port = exchange_listener
        .local_addr()
        .expect("exchange listener should have local addr")
        .port();

    let write_handle = thread::spawn(move || {
        let (mut stream, _) = write_listener
            .accept()
            .expect("write listener should accept");
        let mut buf = Vec::new();
        stream
            .read_to_end(&mut buf)
            .expect("write listener should read request");
        assert_eq!(b"PING", buf.as_slice(), "unexpected write-only payload");
    });
    let exchange_handle = thread::spawn(move || {
        let (mut stream, _) = exchange_listener
            .accept()
            .expect("exchange listener should accept");
        let mut buf = Vec::new();
        stream
            .read_to_end(&mut buf)
            .expect("exchange listener should read request");
        assert_eq!(b"ABC", buf.as_slice(), "unexpected exchange request");
        stream
            .write_all(b"XYZ")
            .expect("exchange listener should write response");
    });

    let source = fs::read_to_string(examples_dir().join("runtime_net_bytes.mira"))
        .expect("runtime_net_bytes example should exist");
    let source = source
        .replace("127.0.0.1:38421", &format!("127.0.0.1:{write_port}"))
        .replace("127.0.0.1:38422", &format!("127.0.0.1:{exchange_port}"));
    let program = parse_program(&source).expect("program should parse");
    let diagnostics = validate_program(&program);
    assert!(
        diagnostics.is_empty(),
        "runtime_net_bytes example should validate after port rewrite: {diagnostics:?}"
    );
    let harness = emit_test_harness(&program).expect("test harness should emit");
    let binary =
        compile_c_source("itest_runtime_net_bytes", &harness).expect("clang should compile");
    let output = run_binary(&binary).expect("binary should run");
    write_handle
        .join()
        .expect("write listener thread should finish");
    exchange_handle
        .join()
        .expect("exchange listener thread should finish");
    assert!(
        output.status.success(),
        "runtime_net_bytes native tests failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn runtime_net_bytes_example_runs_portably_against_local_listeners() {
    let write_listener = TcpListener::bind("127.0.0.1:0").expect("write listener should bind");
    let write_port = write_listener
        .local_addr()
        .expect("write listener should have local addr")
        .port();
    write_listener
        .set_nonblocking(true)
        .expect("write listener should become nonblocking");
    let exchange_listener =
        TcpListener::bind("127.0.0.1:0").expect("exchange listener should bind");
    let exchange_port = exchange_listener
        .local_addr()
        .expect("exchange listener should have local addr")
        .port();
    exchange_listener
        .set_nonblocking(true)
        .expect("exchange listener should become nonblocking");

    let write_handle = thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(60);
        let mut accepts_remaining = 3usize;
        loop {
            match write_listener.accept() {
                Ok((mut stream, _)) => {
                    stream
                        .set_nonblocking(false)
                        .expect("accepted write stream should become blocking");
                    let mut buf = Vec::new();
                    stream
                        .read_to_end(&mut buf)
                        .expect("write listener should read request");
                    assert_eq!(b"PING", buf.as_slice(), "unexpected write-only payload");
                    accepts_remaining = accepts_remaining.saturating_sub(1);
                    if accepts_remaining == 0 {
                        return;
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    if Instant::now() >= deadline {
                        return;
                    }
                    thread::sleep(Duration::from_millis(25));
                }
                Err(error) => panic!("write listener accept failed: {error}"),
            }
        }
    });
    let exchange_handle = thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(60);
        let mut accepts_remaining = 3usize;
        loop {
            match exchange_listener.accept() {
                Ok((mut stream, _)) => {
                    stream
                        .set_nonblocking(false)
                        .expect("accepted exchange stream should become blocking");
                    let mut buf = Vec::new();
                    stream
                        .read_to_end(&mut buf)
                        .expect("exchange listener should read request");
                    assert_eq!(b"ABC", buf.as_slice(), "unexpected exchange request");
                    stream
                        .write_all(b"XYZ")
                        .expect("exchange listener should write response");
                    accepts_remaining = accepts_remaining.saturating_sub(1);
                    if accepts_remaining == 0 {
                        return;
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    if Instant::now() >= deadline {
                        return;
                    }
                    thread::sleep(Duration::from_millis(25));
                }
                Err(error) => panic!("exchange listener accept failed: {error}"),
            }
        }
    });

    let source = fs::read_to_string(examples_dir().join("runtime_net_bytes.mira"))
        .expect("runtime_net_bytes example should exist");
    let source = source
        .replace("127.0.0.1:38421", &format!("127.0.0.1:{write_port}"))
        .replace("127.0.0.1:38422", &format!("127.0.0.1:{exchange_port}"));
    let program = parse_program(&source).expect("program should parse");
    let diagnostics = validate_program(&program);
    assert!(
        diagnostics.is_empty(),
        "runtime_net_bytes example should validate after port rewrite: {diagnostics:?}"
    );
    let lowered = lower_program_for_direct_exec(&program).expect("runtime_net_bytes should lower");
    let direct_write =
        run_lowered_function(&lowered, "write_ping", &std::collections::HashMap::new())
            .expect("direct runtime net bytes write execution works");
    assert_eq!(RuntimeValue::Bool(true), direct_write);
    let direct_exchange = run_lowered_function(
        &lowered,
        "exchange_second_byte",
        &std::collections::HashMap::new(),
    )
    .expect("direct runtime net bytes exchange execution works");
    assert_eq!(RuntimeValue::U8(89), direct_exchange);

    let bytecode = compile_bytecode_program(&lowered).expect("runtime_net_bytes bytecode");
    let bytecode_write =
        run_bytecode_function(&bytecode, "write_ping", &std::collections::HashMap::new())
            .expect("bytecode runtime net bytes write execution works");
    assert_eq!(RuntimeValue::Bool(true), bytecode_write);
    let bytecode_exchange = run_bytecode_function(
        &bytecode,
        "exchange_second_byte",
        &std::collections::HashMap::new(),
    )
    .expect("bytecode runtime net bytes exchange execution works");
    assert_eq!(RuntimeValue::U8(89), bytecode_exchange);

    let summary = verify_lowered_tests_portably(&lowered)
        .expect("portable runtime_net_bytes verification should succeed")
        .expect("runtime_net_bytes should stay on portable bytecode path");
    assert_eq!("portable bytecode tests passed: 2/2", summary);

    write_handle
        .join()
        .expect("write listener thread should finish");
    exchange_handle
        .join()
        .expect("exchange listener thread should finish");
}

#[test]
fn runtime_net_server_bytes_example_runs_against_local_client() {
    let port = reserve_closed_port();
    let source = fs::read_to_string(examples_dir().join("runtime_net_server_bytes.mira"))
        .expect("runtime_net_server_bytes example should exist");
    let source = source.replace("127.0.0.1:38423", &format!("127.0.0.1:{port}"));
    let program = parse_program(&source).expect("program should parse");
    let diagnostics = validate_program(&program);
    assert!(
        diagnostics.is_empty(),
        "runtime_net_server_bytes example should validate after port rewrite: {diagnostics:?}"
    );

    let client = thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            match TcpStream::connect(("127.0.0.1", port)) {
                Ok(mut stream) => {
                    stream
                        .write_all(b"PING")
                        .expect("client should write request");
                    let _ = stream.shutdown(std::net::Shutdown::Write);
                    let mut response = Vec::new();
                    stream
                        .read_to_end(&mut response)
                        .expect("client should read response");
                    assert_eq!(b"PONG", response.as_slice(), "unexpected server response");
                    return;
                }
                Err(error) if Instant::now() < deadline => {
                    let _ = error;
                    thread::sleep(Duration::from_millis(25));
                }
                Err(error) => panic!("client connect failed: {error}"),
            }
        }
    });

    let harness = emit_test_harness(&program).expect("test harness should emit");
    let binary =
        compile_c_source("itest_runtime_net_server_bytes", &harness).expect("clang should compile");
    let output = run_binary(&binary).expect("binary should run");
    client.join().expect("client thread should finish");
    assert!(
        output.status.success(),
        "runtime_net_server_bytes native tests failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn runtime_net_server_bytes_example_runs_portably_against_local_client() {
    let direct_port = reserve_closed_port();
    let bytecode_port = reserve_closed_port();
    let verify_port = reserve_closed_port();
    let source = fs::read_to_string(examples_dir().join("runtime_net_server_bytes.mira"))
        .expect("runtime_net_server_bytes example should exist");
    let client = thread::spawn(move || {
        for port in [direct_port, bytecode_port, verify_port] {
            let deadline = Instant::now() + Duration::from_secs(60);
            loop {
                match TcpStream::connect(("127.0.0.1", port)) {
                    Ok(mut stream) => {
                        stream
                            .write_all(b"PING")
                            .expect("client should write request");
                        let _ = stream.shutdown(std::net::Shutdown::Write);
                        let mut response = Vec::new();
                        stream
                            .read_to_end(&mut response)
                            .expect("client should read response");
                        assert_eq!(b"PONG", response.as_slice(), "unexpected server response");
                        break;
                    }
                    Err(error) if Instant::now() < deadline => {
                        let _ = error;
                        thread::sleep(Duration::from_millis(25));
                    }
                    Err(error) => panic!("client connect failed: {error}"),
                }
            }
        }
    });

    let direct_source = source.replace("127.0.0.1:38423", &format!("127.0.0.1:{direct_port}"));
    let direct_program = parse_program(&direct_source).expect("direct program should parse");
    let diagnostics = validate_program(&direct_program);
    assert!(
        diagnostics.is_empty(),
        "runtime_net_server_bytes direct program should validate after port rewrite: {diagnostics:?}"
    );
    let direct_lowered =
        lower_program_for_direct_exec(&direct_program).expect("direct program should lower");
    let direct_result = run_lowered_function(
        &direct_lowered,
        "serve_and_capture_second_byte",
        &std::collections::HashMap::new(),
    )
    .expect("direct runtime net server bytes execution works");
    assert_eq!(RuntimeValue::U8(73), direct_result);

    let bytecode_source = source.replace("127.0.0.1:38423", &format!("127.0.0.1:{bytecode_port}"));
    let bytecode_program = parse_program(&bytecode_source).expect("bytecode program should parse");
    let diagnostics = validate_program(&bytecode_program);
    assert!(
        diagnostics.is_empty(),
        "runtime_net_server_bytes bytecode program should validate after port rewrite: {diagnostics:?}"
    );
    let bytecode_lowered =
        lower_program_for_direct_exec(&bytecode_program).expect("bytecode program should lower");
    let bytecode =
        compile_bytecode_program(&bytecode_lowered).expect("runtime_net_server_bytes bytecode");
    let bytecode_result = run_bytecode_function(
        &bytecode,
        "serve_and_capture_second_byte",
        &std::collections::HashMap::new(),
    )
    .expect("bytecode runtime net server bytes execution works");
    assert_eq!(RuntimeValue::U8(73), bytecode_result);

    let verify_source = source.replace("127.0.0.1:38423", &format!("127.0.0.1:{verify_port}"));
    let verify_program = parse_program(&verify_source).expect("verify program should parse");
    let diagnostics = validate_program(&verify_program);
    assert!(
        diagnostics.is_empty(),
        "runtime_net_server_bytes verify program should validate after port rewrite: {diagnostics:?}"
    );
    let verify_lowered =
        lower_program_for_direct_exec(&verify_program).expect("verify program should lower");
    let summary = verify_lowered_tests_portably(&verify_lowered)
        .expect("portable runtime_net_server_bytes verification should succeed")
        .expect("runtime_net_server_bytes should stay on portable bytecode path");
    assert_eq!("portable bytecode tests passed: 1/1", summary);

    client.join().expect("client thread should finish");
}

#[test]
fn runtime_http_server_handle_example_runs_portably_against_local_client() {
    let direct_port = reserve_closed_port();
    let bytecode_port = reserve_closed_port();
    let verify_port = reserve_closed_port();
    let source = fs::read_to_string(examples_dir().join("runtime_http_server_handle.mira"))
        .expect("runtime_http_server_handle example should exist");
    let client = thread::spawn(move || {
        for port in [direct_port, bytecode_port, verify_port] {
            let deadline = Instant::now() + Duration::from_secs(60);
            loop {
                match TcpStream::connect(("127.0.0.1", port)) {
                    Ok(mut stream) => {
                        stream
                            .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n")
                            .expect("client should write http request");
                        let _ = stream.shutdown(std::net::Shutdown::Write);
                        let mut response = Vec::new();
                        stream
                            .read_to_end(&mut response)
                            .expect("client should read http response");
                        let text = String::from_utf8_lossy(&response);
                        assert!(
                            text.starts_with("HTTP/1.1 200 OK"),
                            "unexpected http response head: {text}"
                        );
                        assert!(
                            text.ends_with("OK"),
                            "unexpected http response body: {text}"
                        );
                        break;
                    }
                    Err(error) if Instant::now() < deadline => {
                        let _ = error;
                        thread::sleep(Duration::from_millis(25));
                    }
                    Err(error) => panic!("http client connect failed: {error}"),
                }
            }
        }
    });

    let direct_source = source.replace("127.0.0.1:38531", &format!("127.0.0.1:{direct_port}"));
    let direct_program = parse_program(&direct_source).expect("direct program should parse");
    let diagnostics = validate_program(&direct_program);
    assert!(
        diagnostics.is_empty(),
        "runtime_http_server_handle direct program should validate after port rewrite: {diagnostics:?}"
    );
    let direct_lowered =
        lower_program_for_direct_exec(&direct_program).expect("direct program should lower");
    let direct_result = run_lowered_function(
        &direct_lowered,
        "serve_health_once",
        &std::collections::HashMap::new(),
    )
    .expect("direct runtime http server handle execution works");
    assert_eq!(RuntimeValue::U8(1), direct_result);

    let bytecode_source = source.replace("127.0.0.1:38531", &format!("127.0.0.1:{bytecode_port}"));
    let bytecode_program = parse_program(&bytecode_source).expect("bytecode program should parse");
    let diagnostics = validate_program(&bytecode_program);
    assert!(
        diagnostics.is_empty(),
        "runtime_http_server_handle bytecode program should validate after port rewrite: {diagnostics:?}"
    );
    let bytecode_lowered =
        lower_program_for_direct_exec(&bytecode_program).expect("bytecode program should lower");
    let bytecode =
        compile_bytecode_program(&bytecode_lowered).expect("runtime_http_server_handle bytecode");
    let bytecode_result = run_bytecode_function(
        &bytecode,
        "serve_health_once",
        &std::collections::HashMap::new(),
    )
    .expect("bytecode runtime http server handle execution works");
    assert_eq!(RuntimeValue::U8(1), bytecode_result);

    let verify_source = source.replace("127.0.0.1:38531", &format!("127.0.0.1:{verify_port}"));
    let verify_program = parse_program(&verify_source).expect("verify program should parse");
    let diagnostics = validate_program(&verify_program);
    assert!(
        diagnostics.is_empty(),
        "runtime_http_server_handle verify program should validate after port rewrite: {diagnostics:?}"
    );
    let verify_lowered =
        lower_program_for_direct_exec(&verify_program).expect("verify program should lower");
    assert!(
        verify_lowered.tests.is_empty(),
        "dynamic server example should stay testless"
    );
    let verify_result = run_lowered_function(
        &verify_lowered,
        "serve_health_once",
        &std::collections::HashMap::new(),
    )
    .expect("portable verify path should execute http server handle example");
    assert_eq!(RuntimeValue::U8(1), verify_result);

    client.join().expect("client thread should finish");
}

#[test]
fn runtime_http_server_handle_example_runs_natively_against_local_client() {
    let port = reserve_closed_port();
    let source = fs::read_to_string(examples_dir().join("runtime_http_server_handle.mira"))
        .expect("runtime_http_server_handle example should exist");
    let source = source.replace("127.0.0.1:38531", &format!("127.0.0.1:{port}"));
    let program = parse_program(&source).expect("program should parse");
    let diagnostics = validate_program(&program);
    assert!(
        diagnostics.is_empty(),
        "runtime_http_server_handle example should validate after port rewrite: {diagnostics:?}"
    );

    let client = thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            match TcpStream::connect(("127.0.0.1", port)) {
                Ok(mut stream) => {
                    stream
                        .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n")
                        .expect("client should write http request");
                    let _ = stream.shutdown(std::net::Shutdown::Write);
                    let mut response = Vec::new();
                    stream
                        .read_to_end(&mut response)
                        .expect("client should read http response");
                    assert!(
                        response.starts_with(b"HTTP/1.1 200 OK"),
                        "unexpected status line: {:?}",
                        String::from_utf8_lossy(&response)
                    );
                    assert!(response.ends_with(b"OK"), "unexpected response body");
                    return;
                }
                Err(error) if Instant::now() < deadline => {
                    let _ = error;
                    thread::sleep(Duration::from_millis(25));
                }
                Err(error) => panic!("client connect failed: {error}"),
            }
        }
    });

    let mut source = emit_library(&program).expect("native library should emit");
    source.push_str("int main(void) {\n");
    source.push_str("  return mira_func_serve_health_once() == 1u ? 0 : 1;\n");
    source.push_str("}\n");
    let binary = compile_c_source("itest_runtime_http_server_handle", &source)
        .expect("clang should compile");
    let output = run_binary(&binary).expect("binary should run");
    client.join().expect("client thread should finish");
    assert!(
        output.status.success(),
        "runtime_http_server_handle native run failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn runtime_http_server_framework_example_runs_portably_against_local_client() {
    let direct_port = reserve_closed_port();
    let bytecode_port = reserve_closed_port();
    let source = fs::read_to_string(examples_dir().join("runtime_http_server_framework.mira"))
        .expect("runtime_http_server_framework example should exist");
    let client = thread::spawn(move || {
        for port in [direct_port, bytecode_port] {
            let deadline = Instant::now() + Duration::from_secs(60);
            loop {
                match TcpStream::connect(("127.0.0.1", port)) {
                    Ok(mut stream) => {
                        stream
                            .write_all(
                                b"GET /secure HTTP/1.1\r\nHost: localhost\r\nAuthorization: token-42\r\n\r\n",
                            )
                            .expect("client should write secure http request");
                        let _ = stream.shutdown(std::net::Shutdown::Write);
                        let mut response = Vec::new();
                        let mut chunk = [0u8; 1024];
                        loop {
                            match stream.read(&mut chunk) {
                                Ok(0) => break,
                                Ok(n) => response.extend_from_slice(&chunk[..n]),
                                Err(error)
                                    if error.kind() == std::io::ErrorKind::ConnectionReset =>
                                {
                                    break
                                }
                                Err(error) => {
                                    panic!("client should read secure http response: {error}")
                                }
                            }
                        }
                        let text = String::from_utf8_lossy(&response);
                        assert!(
                            text.starts_with("HTTP/1.1 200 OK"),
                            "unexpected http response head: {text}"
                        );
                        assert!(
                            text.contains("Content-Type: application/json"),
                            "missing content type header: {text}"
                        );
                        assert!(
                            text.ends_with("{\"ok\":true}"),
                            "unexpected http response body: {text}"
                        );
                        break;
                    }
                    Err(error) if Instant::now() < deadline => {
                        let _ = error;
                        thread::sleep(Duration::from_millis(25));
                    }
                    Err(error) => panic!("http framework client connect failed: {error}"),
                }
            }
        }
    });

    let direct_source = source.replace("127.0.0.1:39571", &format!("127.0.0.1:{direct_port}"));
    let direct_program =
        parse_program(&direct_source).expect("direct framework program should parse");
    let diagnostics = validate_program(&direct_program);
    assert!(
        diagnostics.is_empty(),
        "runtime_http_server_framework direct program should validate after port rewrite: {diagnostics:?}"
    );
    let direct_lowered = lower_program_for_direct_exec(&direct_program)
        .expect("direct framework program should lower");
    let direct_result = run_lowered_function(
        &direct_lowered,
        "serve_secure_once",
        &std::collections::HashMap::new(),
    )
    .expect("direct runtime http framework execution works");
    assert_eq!(RuntimeValue::U8(1), direct_result);

    let bytecode_source = source.replace("127.0.0.1:39571", &format!("127.0.0.1:{bytecode_port}"));
    let bytecode_program =
        parse_program(&bytecode_source).expect("bytecode framework program should parse");
    let diagnostics = validate_program(&bytecode_program);
    assert!(
        diagnostics.is_empty(),
        "runtime_http_server_framework bytecode program should validate after port rewrite: {diagnostics:?}"
    );
    let bytecode_lowered =
        lower_program_for_direct_exec(&bytecode_program).expect("bytecode framework should lower");
    let bytecode = compile_bytecode_program(&bytecode_lowered)
        .expect("runtime_http_server_framework bytecode");
    let bytecode_result = run_bytecode_function(
        &bytecode,
        "serve_secure_once",
        &std::collections::HashMap::new(),
    )
    .expect("bytecode runtime http framework execution works");
    assert_eq!(RuntimeValue::U8(1), bytecode_result);

    client
        .join()
        .expect("framework client thread should finish");
}

#[test]
fn runtime_http_cookie_flow_example_runs_portably_against_local_client() {
    let direct_port = reserve_closed_port();
    let bytecode_port = reserve_closed_port();
    let source = fs::read_to_string(examples_dir().join("runtime_http_cookie_flow.mira"))
        .expect("runtime_http_cookie_flow example should exist");
    let client = thread::spawn(move || {
        for port in [direct_port, bytecode_port] {
            let deadline = Instant::now() + Duration::from_secs(60);
            loop {
                match TcpStream::connect(("127.0.0.1", port)) {
                    Ok(mut stream) => {
                        stream
                            .write_all(
                                b"GET /profile HTTP/1.1\r\nHost: localhost\r\nCookie: sid=abc123; theme=dark\r\n\r\n",
                            )
                            .expect("client should write cookie request");
                        let _ = stream.shutdown(std::net::Shutdown::Write);
                        let mut response = Vec::new();
                        stream
                            .read_to_end(&mut response)
                            .expect("client should read cookie response");
                        let text = String::from_utf8_lossy(&response);
                        assert!(
                            text.starts_with("HTTP/1.1 200 OK"),
                            "unexpected cookie response head: {text}"
                        );
                        assert!(
                            text.contains("Content-Type: text/plain"),
                            "missing cookie content type header: {text}"
                        );
                        assert!(
                            text.contains("Set-Cookie: sid=fresh456"),
                            "missing set-cookie header: {text}"
                        );
                        assert!(
                            text.ends_with("OK"),
                            "unexpected cookie response body: {text}"
                        );
                        break;
                    }
                    Err(error) if Instant::now() < deadline => {
                        let _ = error;
                        thread::sleep(Duration::from_millis(25));
                    }
                    Err(error) => panic!("http cookie client connect failed: {error}"),
                }
            }
        }
    });

    let direct_source = source.replace("127.0.0.1:39572", &format!("127.0.0.1:{direct_port}"));
    let direct_program = parse_program(&direct_source).expect("cookie direct program should parse");
    let diagnostics = validate_program(&direct_program);
    assert!(
        diagnostics.is_empty(),
        "runtime_http_cookie_flow direct program should validate after port rewrite: {diagnostics:?}"
    );
    let direct_lowered =
        lower_program_for_direct_exec(&direct_program).expect("cookie direct program should lower");
    let direct_result = run_lowered_function(
        &direct_lowered,
        "serve_cookie_once",
        &std::collections::HashMap::new(),
    )
    .expect("direct runtime http cookie execution works");
    assert_eq!(RuntimeValue::U8(1), direct_result);

    let bytecode_source =
        source.replace("127.0.0.1:39572", &format!("127.0.0.1:{bytecode_port}"));
    let bytecode_program =
        parse_program(&bytecode_source).expect("cookie bytecode program should parse");
    let diagnostics = validate_program(&bytecode_program);
    assert!(
        diagnostics.is_empty(),
        "runtime_http_cookie_flow bytecode program should validate after port rewrite: {diagnostics:?}"
    );
    let bytecode_lowered =
        lower_program_for_direct_exec(&bytecode_program).expect("cookie bytecode should lower");
    let bytecode =
        compile_bytecode_program(&bytecode_lowered).expect("runtime_http_cookie_flow bytecode");
    let bytecode_result = run_bytecode_function(
        &bytecode,
        "serve_cookie_once",
        &std::collections::HashMap::new(),
    )
    .expect("bytecode runtime http cookie execution works");
    assert_eq!(RuntimeValue::U8(1), bytecode_result);

    client
        .join()
        .expect("cookie client thread should finish");
}

#[test]
fn runtime_http_cookie_flow_example_runs_natively_against_local_client() {
    let port = reserve_closed_port();
    let source = fs::read_to_string(examples_dir().join("runtime_http_cookie_flow.mira"))
        .expect("runtime_http_cookie_flow example should exist");
    let source = source.replace("127.0.0.1:39572", &format!("127.0.0.1:{port}"));
    let program = parse_program(&source).expect("cookie program should parse");
    let diagnostics = validate_program(&program);
    assert!(
        diagnostics.is_empty(),
        "runtime_http_cookie_flow example should validate after port rewrite: {diagnostics:?}"
    );

    let client = thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            match TcpStream::connect(("127.0.0.1", port)) {
                Ok(mut stream) => {
                    stream
                        .write_all(
                            b"GET /profile HTTP/1.1\r\nHost: localhost\r\nCookie: sid=abc123; theme=dark\r\n\r\n",
                        )
                        .expect("client should write native cookie request");
                    let _ = stream.shutdown(std::net::Shutdown::Write);
                    let mut response = Vec::new();
                    stream
                        .read_to_end(&mut response)
                        .expect("client should read native cookie response");
                    let text = String::from_utf8_lossy(&response);
                    assert!(
                        text.starts_with("HTTP/1.1 200 OK"),
                        "unexpected native cookie status line: {text}"
                    );
                    assert!(
                        text.contains("Set-Cookie: sid=fresh456"),
                        "missing native set-cookie header: {text}"
                    );
                    assert!(
                        text.ends_with("OK"),
                        "unexpected native cookie response body: {text}"
                    );
                    return;
                }
                Err(error) if Instant::now() < deadline => {
                    let _ = error;
                    thread::sleep(Duration::from_millis(25));
                }
                Err(error) => panic!("native cookie client connect failed: {error}"),
            }
        }
    });

    let mut source = emit_library(&program).expect("cookie native library should emit");
    source.push_str("int main(void) {\n");
    source.push_str("  return mira_func_serve_cookie_once() == 1u ? 0 : 1;\n");
    source.push_str("}\n");
    let binary = compile_c_source("itest_runtime_http_cookie_flow", &source)
        .expect("clang should compile cookie example");
    let output = run_binary(&binary).expect("binary should run");
    client
        .join()
        .expect("cookie native client thread should finish");
    assert!(
        output.status.success(),
        "runtime_http_cookie_flow native run failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn runtime_http_header_cookie_json_api_example_runs_portably_against_local_client() {
    let direct_port = reserve_closed_port();
    let bytecode_port = reserve_closed_port();
    let source =
        fs::read_to_string(examples_dir().join("runtime_http_header_cookie_json_api.mira"))
            .expect("runtime_http_header_cookie_json_api example should exist");
    let client = thread::spawn(move || {
        for port in [direct_port, bytecode_port] {
            let deadline = Instant::now() + Duration::from_secs(60);
            loop {
                match TcpStream::connect(("127.0.0.1", port)) {
                    Ok(mut stream) => {
                        stream
                            .write_all(
                                b"GET /api/profile HTTP/1.1\r\nHost: localhost\r\nX-Mode: fast\r\nCookie: sid=abc123; theme=dark\r\n\r\n",
                            )
                            .expect("client should write header/cookie json request");
                        let _ = stream.shutdown(std::net::Shutdown::Write);
                        let mut response = Vec::new();
                        stream
                            .read_to_end(&mut response)
                            .expect("client should read header/cookie json response");
                        let text = String::from_utf8_lossy(&response);
                        assert!(
                            text.starts_with("HTTP/1.1 200 OK"),
                            "unexpected header/cookie json response head: {text}"
                        );
                        assert!(
                            text.contains("Content-Type: application/json"),
                            "missing json content type header: {text}"
                        );
                        assert!(
                            text.contains("Cache-Control: no-store"),
                            "missing cache-control header: {text}"
                        );
                        assert!(
                            text.contains("X-Trace-Id: req-42"),
                            "missing trace header: {text}"
                        );
                        assert!(
                            text.ends_with("{\"ok\":true}"),
                            "unexpected header/cookie json response body: {text}"
                        );
                        break;
                    }
                    Err(error) if Instant::now() < deadline => {
                        let _ = error;
                        thread::sleep(Duration::from_millis(25));
                    }
                    Err(error) => panic!("http header/cookie json client connect failed: {error}"),
                }
            }
        }
    });

    let direct_source = source.replace("127.0.0.1:39574", &format!("127.0.0.1:{direct_port}"));
    let direct_program =
        parse_program(&direct_source).expect("header/cookie json direct program should parse");
    let diagnostics = validate_program(&direct_program);
    assert!(
        diagnostics.is_empty(),
        "runtime_http_header_cookie_json_api direct program should validate after port rewrite: {diagnostics:?}"
    );
    let direct_lowered = lower_program_for_direct_exec(&direct_program)
        .expect("header/cookie json direct program should lower");
    let direct_result = run_lowered_function(
        &direct_lowered,
        "serve_header_cookie_json_once",
        &std::collections::HashMap::new(),
    )
    .expect("direct runtime http header/cookie json execution works");
    assert_eq!(RuntimeValue::U8(1), direct_result);

    let bytecode_source =
        source.replace("127.0.0.1:39574", &format!("127.0.0.1:{bytecode_port}"));
    let bytecode_program =
        parse_program(&bytecode_source).expect("header/cookie json bytecode program should parse");
    let diagnostics = validate_program(&bytecode_program);
    assert!(
        diagnostics.is_empty(),
        "runtime_http_header_cookie_json_api bytecode program should validate after port rewrite: {diagnostics:?}"
    );
    let bytecode_lowered = lower_program_for_direct_exec(&bytecode_program)
        .expect("header/cookie json bytecode should lower");
    let bytecode = compile_bytecode_program(&bytecode_lowered)
        .expect("runtime_http_header_cookie_json_api bytecode");
    let bytecode_result = run_bytecode_function(
        &bytecode,
        "serve_header_cookie_json_once",
        &std::collections::HashMap::new(),
    )
    .expect("bytecode runtime http header/cookie json execution works");
    assert_eq!(RuntimeValue::U8(1), bytecode_result);

    client
        .join()
        .expect("header/cookie json client thread should finish");
}

#[test]
fn runtime_http_header_cookie_json_api_example_runs_natively_against_local_client() {
    let port = reserve_closed_port();
    let source =
        fs::read_to_string(examples_dir().join("runtime_http_header_cookie_json_api.mira"))
            .expect("runtime_http_header_cookie_json_api example should exist");
    let source = source.replace("127.0.0.1:39574", &format!("127.0.0.1:{port}"));
    let program = parse_program(&source).expect("header/cookie json program should parse");
    let diagnostics = validate_program(&program);
    assert!(
        diagnostics.is_empty(),
        "runtime_http_header_cookie_json_api example should validate after port rewrite: {diagnostics:?}"
    );

    let client = thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            match TcpStream::connect(("127.0.0.1", port)) {
                Ok(mut stream) => {
                    stream
                        .write_all(
                            b"GET /api/profile HTTP/1.1\r\nHost: localhost\r\nX-Mode: fast\r\nCookie: sid=abc123; theme=dark\r\n\r\n",
                        )
                        .expect("client should write native header/cookie json request");
                    let _ = stream.shutdown(std::net::Shutdown::Write);
                    let mut response = Vec::new();
                    stream
                        .read_to_end(&mut response)
                        .expect("client should read native header/cookie json response");
                    let text = String::from_utf8_lossy(&response);
                    assert!(
                        text.starts_with("HTTP/1.1 200 OK"),
                        "unexpected native header/cookie json status line: {text}"
                    );
                    assert!(
                        text.contains("Content-Type: application/json"),
                        "missing native json content type header: {text}"
                    );
                    assert!(
                        text.contains("Cache-Control: no-store"),
                        "missing native cache-control header: {text}"
                    );
                    assert!(
                        text.contains("X-Trace-Id: req-42"),
                        "missing native trace header: {text}"
                    );
                    assert!(
                        text.ends_with("{\"ok\":true}"),
                        "unexpected native header/cookie json response body: {text}"
                    );
                    return;
                }
                Err(error) if Instant::now() < deadline => {
                    let _ = error;
                    thread::sleep(Duration::from_millis(25));
                }
                Err(error) => panic!("native header/cookie json client connect failed: {error}"),
            }
        }
    });

    let mut source = emit_library(&program).expect("header/cookie json native library should emit");
    source.push_str("int main(void) {\n");
    source.push_str("  return mira_func_serve_header_cookie_json_once() == 1u ? 0 : 1;\n");
    source.push_str("}\n");
    let binary = compile_c_source("itest_runtime_http_header_cookie_json_api", &source)
        .expect("clang should compile header/cookie json example");
    let output = run_binary(&binary).expect("binary should run");
    client
        .join()
        .expect("header/cookie json native client thread should finish");
    assert!(
        output.status.success(),
        "runtime_http_header_cookie_json_api native run failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn runtime_http_server_framework_example_runs_natively_against_local_client() {
    let port = reserve_closed_port();
    let source = fs::read_to_string(examples_dir().join("runtime_http_server_framework.mira"))
        .expect("runtime_http_server_framework example should exist");
    let source = source.replace("127.0.0.1:39571", &format!("127.0.0.1:{port}"));
    let program = parse_program(&source).expect("framework program should parse");
    let diagnostics = validate_program(&program);
    assert!(
        diagnostics.is_empty(),
        "runtime_http_server_framework example should validate after port rewrite: {diagnostics:?}"
    );

    let client = thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            match TcpStream::connect(("127.0.0.1", port)) {
                Ok(mut stream) => {
                    stream
                        .write_all(
                            b"GET /secure HTTP/1.1\r\nHost: localhost\r\nAuthorization: token-42\r\n\r\n",
                        )
                        .expect("client should write secure http request");
                    let _ = stream.shutdown(std::net::Shutdown::Write);
                    let mut response = Vec::new();
                    stream
                        .read_to_end(&mut response)
                        .expect("client should read framework http response");
                    let text = String::from_utf8_lossy(&response);
                    assert!(
                        text.starts_with("HTTP/1.1 200 OK"),
                        "unexpected status line: {text}"
                    );
                    assert!(
                        text.contains("Content-Type: application/json"),
                        "missing content type header: {text}"
                    );
                    assert!(
                        text.ends_with("{\"ok\":true}"),
                        "unexpected response body: {text}"
                    );
                    return;
                }
                Err(error) if Instant::now() < deadline => {
                    let _ = error;
                    thread::sleep(Duration::from_millis(25));
                }
                Err(error) => panic!("framework client connect failed: {error}"),
            }
        }
    });

    let mut source = emit_library(&program).expect("framework native library should emit");
    source.push_str("int main(void) {\n");
    source.push_str("  return mira_func_serve_secure_once() == 1u ? 0 : 1;\n");
    source.push_str("}\n");
    let binary = compile_c_source("itest_runtime_http_server_framework", &source)
        .expect("clang should compile framework example");
    let output = run_binary(&binary).expect("binary should run");
    client
        .join()
        .expect("framework client thread should finish");
    assert!(
        output.status.success(),
        "runtime_http_server_framework native run failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn runtime_db_postgres_examples_run_across_portable_and_native_paths() {
    let db_port = reserve_closed_port();
    let _postgres = PostgresContainerGuard::start(db_port);

    let crud_source = rewrite_postgres_source(
        &fs::read_to_string(examples_dir().join("runtime_db_postgres_crud.mira"))
            .expect("runtime_db_postgres_crud example should exist"),
        db_port,
    );
    let crud_program = parse_program(&crud_source).expect("postgres crud program should parse");
    let crud_diagnostics = validate_program(&crud_program);
    assert!(
        crud_diagnostics.is_empty(),
        "runtime_db_postgres_crud should validate after DSN rewrite: {crud_diagnostics:?}"
    );
    let crud_lowered =
        lower_program_for_direct_exec(&crud_program).expect("postgres crud should lower");
    let crud_summary = verify_lowered_tests_portably(&crud_lowered)
        .expect("portable postgres crud verification should succeed")
        .expect("runtime_db_postgres_crud should stay on portable path");
    assert_eq!("portable bytecode tests passed: 1/1", crud_summary);
    let crud_harness = emit_test_harness(&crud_program).expect("postgres crud harness should emit");
    let crud_binary = compile_c_source("itest_runtime_db_postgres_crud", &crud_harness)
        .expect("postgres crud should compile natively");
    let crud_output = run_binary(&crud_binary).expect("postgres crud binary should run");
    assert!(
        crud_output.status.success(),
        "runtime_db_postgres_crud native run failed: stdout={} stderr={}",
        String::from_utf8_lossy(&crud_output.stdout),
        String::from_utf8_lossy(&crud_output.stderr)
    );

    let tx_source = rewrite_postgres_source(
        &fs::read_to_string(examples_dir().join("runtime_db_postgres_tx.mira"))
            .expect("runtime_db_postgres_tx example should exist"),
        db_port,
    );
    let tx_program = parse_program(&tx_source).expect("postgres tx program should parse");
    let tx_diagnostics = validate_program(&tx_program);
    assert!(
        tx_diagnostics.is_empty(),
        "runtime_db_postgres_tx should validate after DSN rewrite: {tx_diagnostics:?}"
    );
    let tx_lowered = lower_program_for_direct_exec(&tx_program).expect("postgres tx should lower");
    let tx_summary = verify_lowered_tests_portably(&tx_lowered)
        .expect("portable postgres tx verification should succeed")
        .expect("runtime_db_postgres_tx should stay on portable path");
    assert_eq!("portable bytecode tests passed: 2/2", tx_summary);
    let tx_harness = emit_test_harness(&tx_program).expect("postgres tx harness should emit");
    let tx_binary = compile_c_source("itest_runtime_db_postgres_tx", &tx_harness)
        .expect("postgres tx should compile natively");
    let tx_output = run_binary(&tx_binary).expect("postgres tx binary should run");
    assert!(
        tx_output.status.success(),
        "runtime_db_postgres_tx native run failed: stdout={} stderr={}",
        String::from_utf8_lossy(&tx_output.stdout),
        String::from_utf8_lossy(&tx_output.stderr)
    );
}

#[test]
fn runtime_redis_client_example_runs_across_portable_and_native_paths() {
    let redis_port = reserve_closed_port();
    let _redis = RedisContainerGuard::start(redis_port);
    let source = rewrite_redis_source(
        &fs::read_to_string(examples_dir().join("runtime_redis_client.mira"))
            .expect("runtime_redis_client example should exist"),
        redis_port,
    );
    let program = parse_program(&source).expect("redis client program should parse");
    let diagnostics = validate_program(&program);
    assert!(
        diagnostics.is_empty(),
        "runtime_redis_client should validate after port rewrite: {diagnostics:?}"
    );
    let lowered =
        lower_program_for_direct_exec(&program).expect("redis client program should lower");
    let summary = verify_lowered_tests_portably(&lowered)
        .expect("portable redis client verification should succeed")
        .expect("runtime_redis_client should stay on portable path");
    assert_eq!("portable bytecode tests passed: 2/2", summary);
    let harness = emit_test_harness(&program).expect("redis client harness should emit");
    let binary = compile_c_source("itest_runtime_redis_client", &harness)
        .expect("redis client should compile natively");
    let output = run_binary(&binary).expect("redis client binary should run");
    assert!(
        output.status.success(),
        "runtime_redis_client native run failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn runtime_db_transaction_service_example_uses_unique_sqlite_path_across_native_and_portable_checks()
{
    let temp_dir = unique_temp_dir("mira_runtime_db_transaction_service");
    let sqlite_path = temp_dir.join("tx.sqlite");
    let source = fs::read_to_string(examples_dir().join("runtime_db_transaction_service.mira"))
        .expect("runtime_db_transaction_service example should exist");
    let source = source.replace(
        "/tmp/mira_llm_1_2_tx.sqlite",
        sqlite_path
            .to_str()
            .expect("sqlite path should be valid utf-8"),
    );
    let program = parse_program(&source).expect("transaction service program should parse");
    let diagnostics = validate_program(&program);
    assert!(
        diagnostics.is_empty(),
        "runtime_db_transaction_service should validate after path rewrite: {diagnostics:?}"
    );

    let lowered =
        lower_program_for_direct_exec(&program).expect("transaction service should lower");
    let summary = verify_lowered_tests_portably(&lowered)
        .expect("portable transaction verification should succeed")
        .expect("transaction service should stay on portable bytecode path");
    assert_eq!("portable bytecode tests passed: 1/1", summary);

    let harness = emit_test_harness(&program).expect("transaction harness should emit");
    let binary =
        compile_c_source("itest_runtime_db_transaction_service", &harness).expect("clang");
    let output = run_binary(&binary).expect("transaction binary should run");
    assert!(
        output.status.success(),
        "runtime_db_transaction_service native run failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let _ = fs::remove_file(sqlite_path);
    let _ = fs::remove_dir_all(temp_dir);
}

#[test]
fn runtime_queue_worker_service_example_uses_unique_queue_and_cache_paths_across_native_and_portable_checks(
) {
    let temp_dir = unique_temp_dir("mira_runtime_queue_worker_service");
    let queue_path = temp_dir.join("jobs.queue");
    let cache_path = temp_dir.join("jobs.cache");
    let source = fs::read_to_string(examples_dir().join("runtime_queue_worker_service.mira"))
        .expect("runtime_queue_worker_service example should exist");
    let source = source
        .replace(
            "/tmp/mira_llm_1_2_jobs.queue",
            queue_path
                .to_str()
                .expect("queue path should be valid utf-8"),
        )
        .replace(
            "/tmp/mira_llm_1_2_jobs.cache",
            cache_path
                .to_str()
                .expect("cache path should be valid utf-8"),
        );
    let program = parse_program(&source).expect("queue worker program should parse");
    let diagnostics = validate_program(&program);
    assert!(
        diagnostics.is_empty(),
        "runtime_queue_worker_service should validate after path rewrite: {diagnostics:?}"
    );

    let lowered = lower_program_for_direct_exec(&program).expect("queue worker should lower");
    let summary = verify_lowered_tests_portably(&lowered)
        .expect("portable queue-worker verification should succeed")
        .expect("queue worker should stay on portable bytecode path");
    assert_eq!("portable bytecode tests passed: 1/1", summary);

    let harness = emit_test_harness(&program).expect("queue worker harness should emit");
    let binary = compile_c_source("itest_runtime_queue_worker_service", &harness)
        .expect("clang should compile");
    let output = run_binary(&binary).expect("queue worker binary should run");
    assert!(
        output.status.success(),
        "runtime_queue_worker_service native run failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let _ = fs::remove_file(queue_path);
    let _ = fs::remove_file(cache_path);
    let _ = fs::remove_dir_all(temp_dir);
}

#[test]
fn runtime_stateful_db_cache_service_example_uses_unique_state_paths_across_native_and_portable_checks(
) {
    let temp_dir = unique_temp_dir("mira_runtime_stateful_db_cache_service");
    let sqlite_path = temp_dir.join("state.sqlite");
    let cache_path = temp_dir.join("state.cache");
    let source = fs::read_to_string(examples_dir().join("runtime_stateful_db_cache_service.mira"))
        .expect("runtime_stateful_db_cache_service example should exist");
    let source = source
        .replace(
            "/tmp/mira_llm_1_2_state.sqlite",
            sqlite_path
                .to_str()
                .expect("sqlite path should be valid utf-8"),
        )
        .replace(
            "/tmp/mira_llm_1_2_state.cache",
            cache_path
                .to_str()
                .expect("cache path should be valid utf-8"),
        );
    let program = parse_program(&source).expect("stateful db cache program should parse");
    let diagnostics = validate_program(&program);
    assert!(
        diagnostics.is_empty(),
        "runtime_stateful_db_cache_service should validate after path rewrite: {diagnostics:?}"
    );

    let lowered =
        lower_program_for_direct_exec(&program).expect("stateful db cache should lower");
    let summary = verify_lowered_tests_portably(&lowered)
        .expect("portable stateful db cache verification should succeed")
        .expect("stateful db cache should stay on portable bytecode path");
    assert_eq!("portable bytecode tests passed: 1/1", summary);

    let harness = emit_test_harness(&program).expect("stateful db cache harness should emit");
    let binary = compile_c_source("itest_runtime_stateful_db_cache_service", &harness)
        .expect("clang should compile");
    let output = run_binary(&binary).expect("stateful db cache binary should run");
    assert!(
        output.status.success(),
        "runtime_stateful_db_cache_service native run failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let _ = fs::remove_file(sqlite_path);
    let _ = fs::remove_file(cache_path);
    let _ = fs::remove_dir_all(temp_dir);
}

#[test]
fn runtime_metrics_ingest_api_example_uses_unique_state_paths_across_native_and_portable_checks() {
    let temp_dir = unique_temp_dir("mira_runtime_metrics_ingest_api");
    let stream_path = temp_dir.join("metrics.stream");
    let cache_path = temp_dir.join("metrics.cache");
    let source = fs::read_to_string(examples_dir().join("runtime_metrics_ingest_api.mira"))
        .expect("runtime_metrics_ingest_api example should exist");
    let source = source
        .replace(
            "/tmp/mira_2_3_metrics.stream",
            stream_path
                .to_str()
                .expect("metrics stream path should be valid utf-8"),
        )
        .replace(
            "/tmp/mira_2_3_metrics.cache",
            cache_path
                .to_str()
                .expect("metrics cache path should be valid utf-8"),
        );
    assert_u8_service_source_runs_across_direct_portable_and_native(
        &source,
        "ingest_metric_request",
        "runtime_metrics_ingest_api",
    );
    let _ = fs::remove_file(stream_path);
    let _ = fs::remove_file(cache_path);
    let _ = fs::remove_dir_all(temp_dir);
}

#[test]
fn runtime_aggregation_worker_service_example_uses_unique_state_paths_across_native_and_portable_checks(
) {
    let temp_dir = unique_temp_dir("mira_runtime_aggregation_worker_service");
    let queue_path = temp_dir.join("jobs.queue");
    let cache_path = temp_dir.join("jobs.cache");
    let source = fs::read_to_string(examples_dir().join("runtime_aggregation_worker_service.mira"))
        .expect("runtime_aggregation_worker_service example should exist");
    let source = source
        .replace(
            "/tmp/mira_2_3_agg.queue",
            queue_path
                .to_str()
                .expect("aggregation queue path should be valid utf-8"),
        )
        .replace(
            "/tmp/mira_2_3_agg.cache",
            cache_path
                .to_str()
                .expect("aggregation cache path should be valid utf-8"),
        );
    assert_u8_service_source_runs_across_direct_portable_and_native(
        &source,
        "aggregate_two_jobs",
        "runtime_aggregation_worker_service",
    );
    let _ = fs::remove_file(queue_path);
    let _ = fs::remove_file(cache_path);
    let _ = fs::remove_dir_all(temp_dir);
}

#[test]
fn runtime_stream_analytics_pipeline_example_uses_unique_state_paths_across_native_and_portable_checks(
) {
    let temp_dir = unique_temp_dir("mira_runtime_stream_analytics_pipeline");
    let stream_path = temp_dir.join("events.stream");
    let cache_path = temp_dir.join("pipeline.cache");
    let source = fs::read_to_string(examples_dir().join("runtime_stream_analytics_pipeline.mira"))
        .expect("runtime_stream_analytics_pipeline example should exist");
    let source = source
        .replace(
            "/tmp/mira_2_3_pipeline.stream",
            stream_path
                .to_str()
                .expect("pipeline stream path should be valid utf-8"),
        )
        .replace(
            "/tmp/mira_2_3_pipeline.cache",
            cache_path
                .to_str()
                .expect("pipeline cache path should be valid utf-8"),
        );
    assert_u8_service_source_runs_across_direct_portable_and_native(
        &source,
        "replay_retry_pipeline",
        "runtime_stream_analytics_pipeline",
    );
    let _ = fs::remove_file(stream_path);
    let _ = fs::remove_file(cache_path);
    let _ = fs::remove_dir_all(temp_dir);
}

#[test]
fn runtime_shard_messaging_edge_example_uses_unique_state_paths_across_native_and_portable_checks() {
    let temp_dir = unique_temp_dir("mira_runtime_shard_messaging_edge");
    let placement_path = temp_dir.join("edge.place");
    let lease_path = temp_dir.join("edge.lease");
    let source = fs::read_to_string(examples_dir().join("runtime_shard_messaging_edge.mira"))
        .expect("runtime_shard_messaging_edge example should exist");
    let source = source
        .replace(
            "/tmp/mira_2_4_edge.place",
            placement_path
                .to_str()
                .expect("edge placement path should be valid utf-8"),
        )
        .replace(
            "/tmp/mira_2_4_edge.lease",
            lease_path
                .to_str()
                .expect("edge lease path should be valid utf-8"),
        );
    assert_u8_service_source_runs_across_direct_portable_and_native(
        &source,
        "shard_edge_route",
        "runtime_shard_messaging_edge",
    );
    let _ = fs::remove_file(placement_path);
    let _ = fs::remove_file(lease_path);
    let _ = fs::remove_dir_all(temp_dir);
}

#[test]
fn runtime_distributed_analytics_cluster_example_uses_unique_state_paths_across_native_and_portable_checks(
) {
    let temp_dir = unique_temp_dir("mira_runtime_distributed_analytics_cluster");
    let placement_path = temp_dir.join("cluster.place");
    let lease_path = temp_dir.join("cluster.lease");
    let coord_path = temp_dir.join("cluster.coord");
    let stream_path = temp_dir.join("cluster.stream");
    let source = fs::read_to_string(examples_dir().join("runtime_distributed_analytics_cluster.mira"))
        .expect("runtime_distributed_analytics_cluster example should exist");
    let source = source
        .replace(
            "/tmp/mira_2_4_analytics.place",
            placement_path
                .to_str()
                .expect("cluster placement path should be valid utf-8"),
        )
        .replace(
            "/tmp/mira_2_4_analytics.lease",
            lease_path
                .to_str()
                .expect("cluster lease path should be valid utf-8"),
        )
        .replace(
            "/tmp/mira_2_4_analytics.coord",
            coord_path
                .to_str()
                .expect("cluster coord path should be valid utf-8"),
        )
        .replace(
            "/tmp/mira_2_4_analytics.stream",
            stream_path
                .to_str()
                .expect("cluster stream path should be valid utf-8"),
        );
    assert_u8_service_source_runs_across_direct_portable_and_native(
        &source,
        "replay_cluster_checkpoint",
        "runtime_distributed_analytics_cluster",
    );
    let _ = fs::remove_file(placement_path);
    let _ = fs::remove_file(lease_path);
    let _ = fs::remove_file(coord_path);
    let _ = fs::remove_file(stream_path);
    let _ = fs::remove_dir_all(temp_dir);
}

#[test]
fn runtime_failover_rebalance_service_example_uses_unique_state_paths_across_native_and_portable_checks(
) {
    let temp_dir = unique_temp_dir("mira_runtime_failover_rebalance_service");
    let placement_path = temp_dir.join("rebalance.place");
    let lease_path = temp_dir.join("rebalance.lease");
    let coord_path = temp_dir.join("rebalance.coord");
    let stream_path = temp_dir.join("rebalance.stream");
    let source = fs::read_to_string(examples_dir().join("runtime_failover_rebalance_service.mira"))
        .expect("runtime_failover_rebalance_service example should exist");
    let source = source
        .replace(
            "/tmp/mira_2_4_rebalance.place",
            placement_path
                .to_str()
                .expect("rebalance placement path should be valid utf-8"),
        )
        .replace(
            "/tmp/mira_2_4_rebalance.lease",
            lease_path
                .to_str()
                .expect("rebalance lease path should be valid utf-8"),
        )
        .replace(
            "/tmp/mira_2_4_rebalance.coord",
            coord_path
                .to_str()
                .expect("rebalance coord path should be valid utf-8"),
        )
        .replace(
            "/tmp/mira_2_4_rebalance.stream",
            stream_path
                .to_str()
                .expect("rebalance stream path should be valid utf-8"),
        );
    assert_u8_service_source_runs_across_direct_portable_and_native(
        &source,
        "rebalance_without_duplicate_work",
        "runtime_failover_rebalance_service",
    );
    let _ = fs::remove_file(placement_path);
    let _ = fs::remove_file(lease_path);
    let _ = fs::remove_file(coord_path);
    let _ = fs::remove_file(stream_path);
    let _ = fs::remove_dir_all(temp_dir);
}

#[test]
fn runtime_http_postgres_service_example_runs_across_direct_bytecode_and_native_paths() {
    let db_port = reserve_closed_port();
    let _postgres = PostgresContainerGuard::start(db_port);
    let direct_port = reserve_closed_port();
    let bytecode_port = reserve_closed_port();
    let native_port = reserve_closed_port();
    let source = fs::read_to_string(examples_dir().join("runtime_http_postgres_service.mira"))
        .expect("runtime_http_postgres_service example should exist");

    let assert_client = |port: u16| {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            match TcpStream::connect(("127.0.0.1", port)) {
                Ok(mut stream) => {
                    stream
                        .write_all(
                            b"GET /items/42 HTTP/1.1\r\nHost: localhost\r\nAuthorization: token-42\r\n\r\n",
                        )
                        .expect("postgres service client should write request");
                    let _ = stream.shutdown(std::net::Shutdown::Write);
                    let mut response = Vec::new();
                    stream
                        .read_to_end(&mut response)
                        .expect("postgres service client should read response");
                    let text = String::from_utf8_lossy(&response);
                    assert!(
                        text.starts_with("HTTP/1.1 200 OK"),
                        "unexpected postgres service status line: {text}"
                    );
                    assert!(
                        text.contains("Content-Type: application/json"),
                        "missing postgres service content type: {text}"
                    );
                    assert!(
                        text.ends_with("{\"id\":42,\"value\":\"widget\"}"),
                        "unexpected postgres service body: {text}"
                    );
                    return;
                }
                Err(error) if Instant::now() < deadline => {
                    let _ = error;
                    thread::sleep(Duration::from_millis(25));
                }
                Err(error) => panic!("postgres service client connect failed: {error}"),
            }
        }
    };

    let route_source = rewrite_postgres_source(&source, db_port);
    let route_program = parse_program(&route_source).expect("postgres route program should parse");
    let route_diagnostics = validate_program(&route_program);
    assert!(
        route_diagnostics.is_empty(),
        "runtime_http_postgres_service route program should validate: {route_diagnostics:?}"
    );
    let route_lowered =
        lower_program_for_direct_exec(&route_program).expect("postgres route program should lower");
    let route_summary = verify_lowered_tests_portably(&route_lowered)
        .expect("portable postgres route verification should succeed")
        .expect("runtime_http_postgres_service route test should stay on portable path");
    assert_eq!("portable bytecode tests passed: 1/1", route_summary);

    let direct_source = rewrite_postgres_source(
        &source.replace("127.0.0.1:39583", &format!("127.0.0.1:{direct_port}")),
        db_port,
    );
    let direct_program =
        parse_program(&direct_source).expect("direct postgres service should parse");
    let direct_diagnostics = validate_program(&direct_program);
    assert!(
        direct_diagnostics.is_empty(),
        "runtime_http_postgres_service direct program should validate: {direct_diagnostics:?}"
    );
    let direct_client = thread::spawn(move || assert_client(direct_port));
    let direct_lowered = lower_program_for_direct_exec(&direct_program)
        .expect("direct postgres service should lower");
    let direct_result = run_lowered_function(
        &direct_lowered,
        "serve_lookup_once",
        &std::collections::HashMap::new(),
    )
    .expect("direct postgres service should run");
    assert_eq!(RuntimeValue::U8(1), direct_result);
    direct_client
        .join()
        .expect("direct postgres service client should finish");

    let bytecode_source = rewrite_postgres_source(
        &source.replace("127.0.0.1:39583", &format!("127.0.0.1:{bytecode_port}")),
        db_port,
    );
    let bytecode_program =
        parse_program(&bytecode_source).expect("bytecode postgres service should parse");
    let bytecode_diagnostics = validate_program(&bytecode_program);
    assert!(
        bytecode_diagnostics.is_empty(),
        "runtime_http_postgres_service bytecode program should validate: {bytecode_diagnostics:?}"
    );
    let bytecode_client = thread::spawn(move || assert_client(bytecode_port));
    let bytecode_lowered = lower_program_for_direct_exec(&bytecode_program)
        .expect("bytecode postgres service should lower");
    let bytecode = compile_bytecode_program(&bytecode_lowered)
        .expect("postgres service bytecode should compile");
    let bytecode_result = run_bytecode_function(
        &bytecode,
        "serve_lookup_once",
        &std::collections::HashMap::new(),
    )
    .expect("postgres service bytecode should run");
    assert_eq!(RuntimeValue::U8(1), bytecode_result);
    bytecode_client
        .join()
        .expect("bytecode postgres service client should finish");

    let native_source = rewrite_postgres_source(
        &source.replace("127.0.0.1:39583", &format!("127.0.0.1:{native_port}")),
        db_port,
    );
    let native_program =
        parse_program(&native_source).expect("native postgres service should parse");
    let native_diagnostics = validate_program(&native_program);
    assert!(
        native_diagnostics.is_empty(),
        "runtime_http_postgres_service native program should validate: {native_diagnostics:?}"
    );
    let native_client = thread::spawn(move || assert_client(native_port));
    let mut native_library = emit_library(&native_program).expect("postgres service should emit");
    native_library.push_str("int main(void) {\n");
    native_library.push_str("  return mira_func_serve_lookup_once() == 1u ? 0 : 1;\n");
    native_library.push_str("}\n");
    let native_binary = compile_c_source("itest_runtime_http_postgres_service", &native_library)
        .expect("postgres service native binary should compile");
    let native_output =
        run_binary(&native_binary).expect("postgres service native binary should run");
    native_client
        .join()
        .expect("native postgres service client should finish");
    assert!(
        native_output.status.success(),
        "runtime_http_postgres_service native run failed: stdout={} stderr={}",
        String::from_utf8_lossy(&native_output.stdout),
        String::from_utf8_lossy(&native_output.stderr)
    );
}

#[test]
fn runtime_service_templates_run_across_direct_bytecode_and_native_paths() {
    let api_source = examples_dir().join("runtime_service_api_template.mira");
    let api_program = load_and_validate(&api_source).expect("service api template should validate");
    let api_lowered =
        lower_program_for_direct_exec(&api_program).expect("service api template should lower");
    let api_direct = run_lowered_function(
        &api_lowered,
        "service_entry_status",
        &std::collections::HashMap::new(),
    )
    .expect("direct service api template should run");
    assert_eq!(RuntimeValue::U32(200), api_direct);
    let api_bytecode =
        compile_bytecode_program(&api_lowered).expect("service api template bytecode");
    let api_bytecode_value = run_bytecode_function(
        &api_bytecode,
        "service_entry_status",
        &std::collections::HashMap::new(),
    )
    .expect("bytecode service api template should run");
    assert_eq!(RuntimeValue::U32(200), api_bytecode_value);
    let api_harness = emit_test_harness(&api_program).expect("service api harness should emit");
    let api_binary =
        compile_c_source("itest_runtime_service_api_template", &api_harness).expect("clang");
    let api_output = run_binary(&api_binary).expect("service api binary should run");
    assert!(
        api_output.status.success(),
        "native service api template failed: stdout={} stderr={}",
        String::from_utf8_lossy(&api_output.stdout),
        String::from_utf8_lossy(&api_output.stderr)
    );

    let worker_source = examples_dir().join("runtime_service_worker_template.mira");
    let worker_program =
        load_and_validate(&worker_source).expect("service worker template should validate");
    let worker_lowered = lower_program_for_direct_exec(&worker_program)
        .expect("service worker template should lower");
    let worker_direct = run_lowered_function(
        &worker_lowered,
        "run_worker_template",
        &std::collections::HashMap::new(),
    )
    .expect("direct service worker template should run");
    assert_eq!(RuntimeValue::U32(603), worker_direct);
    let worker_summary = verify_lowered_tests_portably(&worker_lowered)
        .expect("portable service worker verification should succeed")
        .expect("service worker template should stay on portable path");
    assert_eq!("portable bytecode tests passed: 1/1", worker_summary);
    let worker_harness =
        emit_test_harness(&worker_program).expect("service worker harness should emit");
    let worker_binary =
        compile_c_source("itest_runtime_service_worker_template", &worker_harness).expect("clang");
    let worker_output = run_binary(&worker_binary).expect("service worker binary should run");
    assert!(
        worker_output.status.success(),
        "native service worker template failed: stdout={} stderr={}",
        String::from_utf8_lossy(&worker_output.stdout),
        String::from_utf8_lossy(&worker_output.stderr)
    );
}

#[test]
fn runtime_reference_backend_service_runs_across_direct_bytecode_native_and_emitted_paths() {
    let source = examples_dir().join("runtime_reference_backend_service.mira");
    let program = load_and_validate(&source).expect("reference backend service should validate");
    let lowered =
        lower_program_for_direct_exec(&program).expect("reference backend service should lower");
    let direct = run_lowered_function(
        &lowered,
        "reference_service_status",
        &std::collections::HashMap::new(),
    )
    .expect("direct reference backend service should run");
    assert_eq!(RuntimeValue::U32(200), direct);

    let portable_summary = verify_lowered_tests_portably(&lowered)
        .expect("portable reference backend verification should succeed")
        .expect("reference backend service should stay on portable path");
    assert_eq!("portable bytecode tests passed: 3/3", portable_summary);

    let harness = emit_test_harness(&program).expect("reference backend harness should emit");
    let binary = compile_c_source("itest_runtime_reference_backend_service", &harness)
        .expect("reference backend native harness should compile");
    let output = run_binary(&binary).expect("reference backend native harness should run");
    assert!(
        output.status.success(),
        "reference backend native harness failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    if cfg!(target_arch = "aarch64") && cfg!(target_os = "macos") {
        arm64_runtime_harness_passes("runtime_reference_backend_service.mira");
    }
    if cfg!(target_os = "macos") {
        x86_64_runtime_harness_passes("runtime_reference_backend_service.mira");
    }
}

#[test]
fn runtime_reference_backend_service_tls_hello_runs_across_direct_bytecode_and_native_paths() {
    let tls_dir = unique_temp_dir("mira_reference_tls");
    let (key, cert) = create_tls_materials(&tls_dir);
    let source = fs::read_to_string(examples_dir().join("runtime_reference_backend_service.mira"))
        .expect("reference backend service example should exist");

    let direct_port = reserve_closed_port();
    let direct_source = rewrite_tls_server_source(&source, direct_port, &key, &cert);
    let direct_program =
        parse_program(&direct_source).expect("direct reference tls program should parse");
    let diagnostics = validate_program(&direct_program);
    assert!(
        diagnostics.is_empty(),
        "reference tls direct program should validate after rewrite: {diagnostics:?}"
    );
    let direct_client = thread::spawn(move || {
        let response = run_tls_client_request(
            direct_port,
            b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        );
        let text = String::from_utf8_lossy(&response);
        assert!(text.starts_with("HTTP/1.1 200 OK"), "unexpected tls status: {text}");
        assert!(text.ends_with("REFERENCE_TLS"), "unexpected tls body: {text}");
    });
    let direct_lowered =
        lower_program_for_direct_exec(&direct_program).expect("reference tls direct should lower");
    let direct_result = run_lowered_function(
        &direct_lowered,
        "serve_tls_hello_once",
        &std::collections::HashMap::new(),
    )
    .expect("reference tls direct should execute");
    assert_eq!(RuntimeValue::U8(1), direct_result);
    direct_client.join().expect("direct tls client should finish");

    let bytecode_port = reserve_closed_port();
    let bytecode_source = rewrite_tls_server_source(&source, bytecode_port, &key, &cert);
    let bytecode_program =
        parse_program(&bytecode_source).expect("bytecode reference tls program should parse");
    let diagnostics = validate_program(&bytecode_program);
    assert!(
        diagnostics.is_empty(),
        "reference tls bytecode program should validate after rewrite: {diagnostics:?}"
    );
    let bytecode_client = thread::spawn(move || {
        let response = run_tls_client_request(
            bytecode_port,
            b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        );
        let text = String::from_utf8_lossy(&response);
        assert!(text.starts_with("HTTP/1.1 200 OK"), "unexpected tls status: {text}");
        assert!(text.ends_with("REFERENCE_TLS"), "unexpected tls body: {text}");
    });
    let bytecode_lowered = lower_program_for_direct_exec(&bytecode_program)
        .expect("reference tls bytecode should lower");
    let bytecode = compile_bytecode_program(&bytecode_lowered).expect("reference tls bytecode");
    let bytecode_result = run_bytecode_function(
        &bytecode,
        "serve_tls_hello_once",
        &std::collections::HashMap::new(),
    )
    .expect("reference tls bytecode should execute");
    assert_eq!(RuntimeValue::U8(1), bytecode_result);
    bytecode_client.join().expect("bytecode tls client should finish");

    let native_port = reserve_closed_port();
    let native_source = rewrite_tls_server_source(&source, native_port, &key, &cert);
    let native_program =
        parse_program(&native_source).expect("native reference tls program should parse");
    let diagnostics = validate_program(&native_program);
    assert!(
        diagnostics.is_empty(),
        "reference tls native program should validate after rewrite: {diagnostics:?}"
    );
    let native_client = thread::spawn(move || {
        let response = run_tls_client_request(
            native_port,
            b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        );
        let text = String::from_utf8_lossy(&response);
        assert!(text.starts_with("HTTP/1.1 200 OK"), "unexpected tls status: {text}");
        assert!(text.ends_with("REFERENCE_TLS"), "unexpected tls body: {text}");
    });
    let mut library =
        emit_library(&native_program).expect("native reference tls library should emit");
    library.push_str("int main(void) {\n");
    library.push_str("  return mira_func_serve_tls_hello_once() == 1u ? 0 : 1;\n");
    library.push_str("}\n");
    let binary = compile_c_source("itest_runtime_reference_backend_tls", &library)
        .expect("reference tls native binary should compile");
    let output = run_binary(&binary).expect("reference tls native binary should run");
    native_client.join().expect("native tls client should finish");
    assert!(
        output.status.success(),
        "reference tls native run failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let _ = fs::remove_file(&key);
    let _ = fs::remove_file(&cert);
    let _ = fs::remove_dir_all(&tls_dir);
}

#[test]
fn runtime_http_crud_service_example_runs_portably_against_local_client() {
    let direct_port = reserve_closed_port();
    let bytecode_port = reserve_closed_port();
    let source = fs::read_to_string(examples_dir().join("runtime_http_crud_service.mira"))
        .expect("runtime_http_crud_service example should exist");
    let client = thread::spawn(move || {
        for port in [direct_port, bytecode_port] {
            let deadline = Instant::now() + Duration::from_secs(60);
            loop {
                match TcpStream::connect(("127.0.0.1", port)) {
                    Ok(mut stream) => {
                        stream
                            .write_all(
                                b"GET /items/42 HTTP/1.1\r\nHost: localhost\r\nAuthorization: token-42\r\n\r\n",
                            )
                            .expect("client should write crud http request");
                        let _ = stream.shutdown(std::net::Shutdown::Write);
                        let mut response = Vec::new();
                        stream
                            .read_to_end(&mut response)
                            .expect("client should read crud http response");
                        let text = String::from_utf8_lossy(&response);
                        assert!(
                            text.starts_with("HTTP/1.1 200 OK"),
                            "unexpected crud response head: {text}"
                        );
                        assert!(
                            text.contains("Content-Type: application/json"),
                            "missing crud content type header: {text}"
                        );
                        assert!(
                            text.ends_with("{\"id\":42}"),
                            "unexpected crud response body: {text}"
                        );
                        break;
                    }
                    Err(error) if Instant::now() < deadline => {
                        let _ = error;
                        thread::sleep(Duration::from_millis(25));
                    }
                    Err(error) => panic!("crud client connect failed: {error}"),
                }
            }
        }
    });

    let direct_source = source.replace("127.0.0.1:39579", &format!("127.0.0.1:{direct_port}"));
    let direct_program = parse_program(&direct_source).expect("direct crud program should parse");
    let diagnostics = validate_program(&direct_program);
    assert!(
        diagnostics.is_empty(),
        "runtime_http_crud_service direct program should validate after port rewrite: {diagnostics:?}"
    );
    let direct_lowered =
        lower_program_for_direct_exec(&direct_program).expect("direct crud program should lower");
    let direct_result = run_lowered_function(
        &direct_lowered,
        "serve_items_once",
        &std::collections::HashMap::new(),
    )
    .expect("direct runtime http crud execution works");
    assert_eq!(RuntimeValue::U8(1), direct_result);

    let bytecode_source = source.replace("127.0.0.1:39579", &format!("127.0.0.1:{bytecode_port}"));
    let bytecode_program =
        parse_program(&bytecode_source).expect("bytecode crud program should parse");
    let diagnostics = validate_program(&bytecode_program);
    assert!(
        diagnostics.is_empty(),
        "runtime_http_crud_service bytecode program should validate after port rewrite: {diagnostics:?}"
    );
    let bytecode_lowered =
        lower_program_for_direct_exec(&bytecode_program).expect("bytecode crud should lower");
    let bytecode =
        compile_bytecode_program(&bytecode_lowered).expect("runtime_http_crud_service bytecode");
    let bytecode_result = run_bytecode_function(
        &bytecode,
        "serve_items_once",
        &std::collections::HashMap::new(),
    )
    .expect("bytecode runtime http crud execution works");
    assert_eq!(RuntimeValue::U8(1), bytecode_result);

    client.join().expect("crud client thread should finish");
}

#[test]
fn runtime_http_crud_service_example_runs_natively_against_local_client() {
    let port = reserve_closed_port();
    let source = fs::read_to_string(examples_dir().join("runtime_http_crud_service.mira"))
        .expect("runtime_http_crud_service example should exist");
    let source = source.replace("127.0.0.1:39579", &format!("127.0.0.1:{port}"));
    let program = parse_program(&source).expect("crud program should parse");
    let diagnostics = validate_program(&program);
    assert!(
        diagnostics.is_empty(),
        "runtime_http_crud_service example should validate after port rewrite: {diagnostics:?}"
    );

    let client = thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            match TcpStream::connect(("127.0.0.1", port)) {
                Ok(mut stream) => {
                    stream
                        .write_all(
                            b"GET /items/42 HTTP/1.1\r\nHost: localhost\r\nAuthorization: token-42\r\n\r\n",
                        )
                        .expect("client should write crud http request");
                    let _ = stream.shutdown(std::net::Shutdown::Write);
                    let mut response = Vec::new();
                    stream
                        .read_to_end(&mut response)
                        .expect("client should read crud http response");
                    let text = String::from_utf8_lossy(&response);
                    assert!(
                        text.starts_with("HTTP/1.1 200 OK"),
                        "unexpected crud status line: {text}"
                    );
                    assert!(
                        text.contains("Content-Type: application/json"),
                        "missing crud content type header: {text}"
                    );
                    assert!(
                        text.ends_with("{\"id\":42}"),
                        "unexpected crud response body: {text}"
                    );
                    return;
                }
                Err(error) if Instant::now() < deadline => {
                    let _ = error;
                    thread::sleep(Duration::from_millis(25));
                }
                Err(error) => panic!("crud client connect failed: {error}"),
            }
        }
    });

    let mut source = emit_library(&program).expect("crud native library should emit");
    source.push_str("int main(void) {\n");
    source.push_str("  return mira_func_serve_items_once() == 1u ? 0 : 1;\n");
    source.push_str("}\n");
    let binary = compile_c_source("itest_runtime_http_crud_service", &source)
        .expect("clang should compile crud example");
    let output = run_binary(&binary).expect("binary should run");
    client.join().expect("crud client thread should finish");
    assert!(
        output.status.success(),
        "runtime_http_crud_service native run failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn runtime_tls_http_client_example_runs_across_direct_bytecode_and_native_paths() {
    let tls_dir = unique_temp_dir("mira_tls_client");
    let (key, cert) = create_tls_materials(&tls_dir);
    let source = fs::read_to_string(examples_dir().join("runtime_tls_http_client.mira"))
        .expect("runtime_tls_http_client example should exist");

    let direct_port = reserve_closed_port();
    let mut direct_server = spawn_tls_http_server(direct_port, &key, &cert);
    let direct_source = source.replace("127.0.0.1:39574", &format!("127.0.0.1:{direct_port}"));
    let direct_program = parse_program(&direct_source).expect("direct tls program should parse");
    let diagnostics = validate_program(&direct_program);
    assert!(
        diagnostics.is_empty(),
        "runtime_tls_http_client direct program should validate after port rewrite: {diagnostics:?}"
    );
    let direct_lowered =
        lower_program_for_direct_exec(&direct_program).expect("direct tls program should lower");
    let direct_result = run_lowered_function(
        &direct_lowered,
        "fetch_local_https_ok",
        &std::collections::HashMap::new(),
    )
    .expect("direct tls execution should work");
    assert_eq!(RuntimeValue::U8(1), direct_result);
    let _ = direct_server.kill();
    let _ = direct_server.wait();

    let bytecode_port = reserve_closed_port();
    let mut bytecode_server = spawn_tls_http_server(bytecode_port, &key, &cert);
    let bytecode_source = source.replace("127.0.0.1:39574", &format!("127.0.0.1:{bytecode_port}"));
    let bytecode_program =
        parse_program(&bytecode_source).expect("bytecode tls program should parse");
    let diagnostics = validate_program(&bytecode_program);
    assert!(
        diagnostics.is_empty(),
        "runtime_tls_http_client bytecode program should validate after port rewrite: {diagnostics:?}"
    );
    let bytecode_lowered = lower_program_for_direct_exec(&bytecode_program)
        .expect("bytecode tls program should lower");
    let bytecode = compile_bytecode_program(&bytecode_lowered).expect("tls bytecode");
    let bytecode_result = run_bytecode_function(
        &bytecode,
        "fetch_local_https_ok",
        &std::collections::HashMap::new(),
    )
    .expect("bytecode tls execution should work");
    assert_eq!(RuntimeValue::U8(1), bytecode_result);
    let _ = bytecode_server.kill();
    let _ = bytecode_server.wait();

    let native_port = reserve_closed_port();
    let mut native_server = spawn_tls_http_server(native_port, &key, &cert);
    let native_source = source.replace("127.0.0.1:39574", &format!("127.0.0.1:{native_port}"));
    let native_program = parse_program(&native_source).expect("native tls program should parse");
    let diagnostics = validate_program(&native_program);
    assert!(
        diagnostics.is_empty(),
        "runtime_tls_http_client native program should validate after port rewrite: {diagnostics:?}"
    );
    let mut library = emit_library(&native_program).expect("native tls library should emit");
    library.push_str("int main(void) {\n");
    library.push_str("  return mira_func_fetch_local_https_ok() == 1u ? 0 : 1;\n");
    library.push_str("}\n");
    let binary = compile_c_source("itest_runtime_tls_http_client", &library)
        .expect("clang should compile tls example");
    let output = run_binary(&binary).expect("tls binary should run");
    let _ = native_server.kill();
    let _ = native_server.wait();
    assert!(
        output.status.success(),
        "runtime_tls_http_client native run failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let _ = fs::remove_file(&key);
    let _ = fs::remove_file(&cert);
    let _ = fs::remove_dir_all(&tls_dir);
}

#[test]
fn portable_bytecode_verification_runs_runtime_operational_hardening_tests() {
    for name in [
        "runtime_self_healing_api_service.mira",
        "runtime_degraded_mode_service.mira",
        "runtime_recovery_worker_service.mira",
    ] {
        let program = load_and_validate(&examples_dir().join(name))
            .unwrap_or_else(|_| panic!("{name} should validate"));
        let lowered = lower_program_for_direct_exec(&program)
            .unwrap_or_else(|_| panic!("{name} should lower"));
        let summary = verify_lowered_tests_portably(&lowered)
            .unwrap_or_else(|_| panic!("{name} portable verification should succeed"))
            .unwrap_or_else(|| panic!("{name} should stay on portable bytecode path"));
        assert_eq!("portable bytecode tests passed: 1/1", summary);
    }
}

#[test]
fn portable_bytecode_verification_runs_agent_platform_services() {
    for name in [
        "runtime_agent_api_service.mira",
        "runtime_agent_stateful_service.mira",
        "runtime_agent_worker_queue_service.mira",
        "runtime_agent_recovery_service.mira",
    ] {
        let program = load_and_validate(&examples_dir().join(name))
            .unwrap_or_else(|_| panic!("{name} should validate"));
        let lowered = lower_program_for_direct_exec(&program)
            .unwrap_or_else(|_| panic!("{name} should lower"));
        let summary = verify_lowered_tests_portably(&lowered)
            .unwrap_or_else(|_| panic!("{name} portable verification should succeed"))
            .unwrap_or_else(|| panic!("{name} should stay on portable bytecode path"));
        assert_eq!("portable bytecode tests passed: 1/1", summary);
    }
}

#[test]
fn agent_platform_services_run_across_direct_bytecode_and_native_paths() {
    let cases = [
        (
            "runtime_agent_api_service.mira",
            "maintained_agent_api_status",
            RuntimeValue::U32(242),
            "uint32_t",
            "242u",
        ),
        (
            "runtime_agent_stateful_service.mira",
            "maintained_agent_stateful_status",
            RuntimeValue::U32(249),
            "uint32_t",
            "249u",
        ),
    ];

    for (name, function, expected, c_ty, c_literal) in cases {
        let source_path = examples_dir().join(name);
        let program = load_and_validate(&source_path)
            .unwrap_or_else(|_| panic!("{name} should validate"));
        let lowered = lower_program_for_direct_exec(&program)
            .unwrap_or_else(|_| panic!("{name} should lower"));
        let direct_result = run_lowered_function(
            &lowered,
            function,
            &std::collections::HashMap::new(),
        )
        .unwrap_or_else(|_| panic!("{name} direct runtime should execute"));
        assert_eq!(expected, direct_result, "{name} direct result mismatch");

        let bytecode =
            compile_bytecode_program(&lowered).unwrap_or_else(|_| panic!("{name} should compile"));
        let bytecode_result = run_bytecode_function(
            &bytecode,
            function,
            &std::collections::HashMap::new(),
        )
        .unwrap_or_else(|_| panic!("{name} bytecode runtime should execute"));
        assert_eq!(expected, bytecode_result, "{name} bytecode result mismatch");

        let mut library = emit_library(&program).unwrap_or_else(|_| panic!("{name} should emit"));
        library.push_str("int main(void) {\n");
        library.push_str(&format!(
            "  {c_ty} result = mira_func_{function}();\n  return result == (({c_ty}){c_literal}) ? 0 : 1;\n"
        ));
        library.push_str("}\n");
        let stem = format!("itest_{}", name.trim_end_matches(".mira"));
        let binary = compile_c_source(&stem, &library)
            .unwrap_or_else(|_| panic!("{name} native library should compile"));
        let output =
            run_binary(&binary).unwrap_or_else(|_| panic!("{name} native binary should run"));
        assert!(
            output.status.success(),
            "{name} native run failed: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[test]
fn runtime_operational_hardening_examples_run_across_direct_bytecode_and_native_paths() {
    let cases = [
        (
            "runtime_self_healing_api_service.mira",
            "recover_api_status",
            RuntimeValue::U32(200),
            "uint32_t",
            "200u",
        ),
        (
            "runtime_degraded_mode_service.mira",
            "degraded_service_status",
            RuntimeValue::U8(1),
            "uint8_t",
            "1u",
        ),
        (
            "runtime_recovery_worker_service.mira",
            "recover_worker_cursor",
            RuntimeValue::U32(17),
            "uint32_t",
            "17u",
        ),
    ];

    for (name, function, expected, c_ty, c_literal) in cases {
        let source_path = examples_dir().join(name);
        let program = load_and_validate(&source_path)
            .unwrap_or_else(|_| panic!("{name} should validate"));
        let lowered = lower_program_for_direct_exec(&program)
            .unwrap_or_else(|_| panic!("{name} should lower"));
        let direct_result = run_lowered_function(
            &lowered,
            function,
            &std::collections::HashMap::new(),
        )
        .unwrap_or_else(|_| panic!("{name} direct runtime should execute"));
        assert_eq!(expected, direct_result, "{name} direct result mismatch");

        let bytecode =
            compile_bytecode_program(&lowered).unwrap_or_else(|_| panic!("{name} should compile"));
        let bytecode_result = run_bytecode_function(
            &bytecode,
            function,
            &std::collections::HashMap::new(),
        )
        .unwrap_or_else(|_| panic!("{name} bytecode runtime should execute"));
        assert_eq!(expected, bytecode_result, "{name} bytecode result mismatch");

        let mut library = emit_library(&program).unwrap_or_else(|_| panic!("{name} should emit"));
        library.push_str("int main(void) {\n");
        library.push_str(&format!(
            "  {c_ty} result = mira_func_{function}();\n  return result == (({c_ty}){c_literal}) ? 0 : 1;\n"
        ));
        library.push_str("}\n");
        let stem = format!("itest_{}", name.trim_end_matches(".mira"));
        let binary = compile_c_source(&stem, &library)
            .unwrap_or_else(|_| panic!("{name} native library should compile"));
        let output = run_binary(&binary).unwrap_or_else(|_| panic!("{name} native binary should run"));
        assert!(
            output.status.success(),
            "{name} native run failed: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[test]
fn portable_bytecode_verification_runs_2_6_production_anchors() {
    let messenger =
        load_and_validate(&examples_dir().join("runtime_production_messenger_backend.mira"))
            .expect("runtime_production_messenger_backend should validate");
    let lowered_messenger = lower_program_for_direct_exec(&messenger)
        .expect("runtime_production_messenger_backend should lower");
    let messenger_summary = verify_lowered_tests_portably(&lowered_messenger)
        .expect("portable messenger verification should succeed")
        .expect("runtime_production_messenger_backend should stay on portable bytecode path");
    assert_eq!("portable bytecode tests passed: 5/5", messenger_summary);

    let analytics =
        load_and_validate(&examples_dir().join("runtime_production_analytics_platform.mira"))
            .expect("runtime_production_analytics_platform should validate");
    let lowered_analytics = lower_program_for_direct_exec(&analytics)
        .expect("runtime_production_analytics_platform should lower");
    let analytics_summary = verify_lowered_tests_portably(&lowered_analytics)
        .expect("portable analytics verification should succeed")
        .expect("runtime_production_analytics_platform should stay on portable bytecode path");
    assert_eq!("portable bytecode tests passed: 5/5", analytics_summary);
}

#[test]
fn production_2_6_anchors_run_across_direct_bytecode_and_native_paths() {
    let cases = [
        (
            "runtime_production_messenger_backend.mira",
            "production_messenger_backend_status",
            RuntimeValue::U32(242),
            "uint32_t",
            "242u",
        ),
        (
            "runtime_production_analytics_platform.mira",
            "production_analytics_platform_status",
            RuntimeValue::U32(249),
            "uint32_t",
            "249u",
        ),
    ];

    for (name, function, expected, c_ty, c_literal) in cases {
        let source_path = examples_dir().join(name);
        let program = load_and_validate(&source_path)
            .unwrap_or_else(|_| panic!("{name} should validate"));
        let lowered = lower_program_for_direct_exec(&program)
            .unwrap_or_else(|_| panic!("{name} should lower"));
        let direct_result = run_lowered_function(
            &lowered,
            function,
            &std::collections::HashMap::new(),
        )
        .unwrap_or_else(|_| panic!("{name} direct runtime should execute"));
        assert_eq!(expected, direct_result, "{name} direct result mismatch");

        let bytecode =
            compile_bytecode_program(&lowered).unwrap_or_else(|_| panic!("{name} should compile"));
        let bytecode_result = run_bytecode_function(
            &bytecode,
            function,
            &std::collections::HashMap::new(),
        )
        .unwrap_or_else(|_| panic!("{name} bytecode runtime should execute"));
        assert_eq!(expected, bytecode_result, "{name} bytecode result mismatch");

        let mut library = emit_library(&program).unwrap_or_else(|_| panic!("{name} should emit"));
        library.push_str("int main(void) {\n");
        library.push_str(&format!(
            "  {c_ty} result = mira_func_{function}();\n  return result == (({c_ty}){c_literal}) ? 0 : 1;\n"
        ));
        library.push_str("}\n");
        let stem = format!("itest_{}", name.trim_end_matches(".mira"));
        let binary = compile_c_source(&stem, &library)
            .unwrap_or_else(|_| panic!("{name} native library should compile"));
        let output =
            run_binary(&binary).unwrap_or_else(|_| panic!("{name} native binary should run"));
        assert!(
            output.status.success(),
            "{name} native run failed: stdout={} stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[test]
fn runtime_tls_http_hello_example_runs_across_direct_bytecode_and_native_paths() {
    let tls_dir = unique_temp_dir("mira_tls_hello");
    let (key, cert) = create_tls_materials(&tls_dir);
    let source = fs::read_to_string(examples_dir().join("runtime_tls_http_hello.mira"))
        .expect("runtime_tls_http_hello example should exist");

    let direct_port = reserve_closed_port();
    let direct_source = rewrite_tls_server_source(&source, direct_port, &key, &cert);
    let direct_program =
        parse_program(&direct_source).expect("direct tls hello program should parse");
    let diagnostics = validate_program(&direct_program);
    assert!(
        diagnostics.is_empty(),
        "runtime_tls_http_hello direct program should validate after rewrite: {diagnostics:?}"
    );
    let direct_client = thread::spawn(move || {
        let response = run_tls_client_request(
            direct_port,
            b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        );
        let text = String::from_utf8_lossy(&response);
        assert!(
            text.starts_with("HTTP/1.1 200 OK"),
            "unexpected tls hello status: {text}"
        );
        assert!(
            text.ends_with("HELLO_TLS"),
            "unexpected tls hello body: {text}"
        );
    });
    let direct_lowered =
        lower_program_for_direct_exec(&direct_program).expect("direct tls hello should lower");
    let direct_result = run_lowered_function(
        &direct_lowered,
        "serve_https_hello_once",
        &std::collections::HashMap::new(),
    )
    .expect("direct tls hello should execute");
    assert_eq!(RuntimeValue::U8(1), direct_result);
    direct_client
        .join()
        .expect("direct tls client should finish");

    let bytecode_port = reserve_closed_port();
    let bytecode_source = rewrite_tls_server_source(&source, bytecode_port, &key, &cert);
    let bytecode_program =
        parse_program(&bytecode_source).expect("bytecode tls hello program should parse");
    let diagnostics = validate_program(&bytecode_program);
    assert!(
        diagnostics.is_empty(),
        "runtime_tls_http_hello bytecode program should validate after rewrite: {diagnostics:?}"
    );
    let bytecode_client = thread::spawn(move || {
        let response = run_tls_client_request(
            bytecode_port,
            b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        );
        let text = String::from_utf8_lossy(&response);
        assert!(
            text.starts_with("HTTP/1.1 200 OK"),
            "unexpected tls hello status: {text}"
        );
        assert!(
            text.ends_with("HELLO_TLS"),
            "unexpected tls hello body: {text}"
        );
    });
    let bytecode_lowered =
        lower_program_for_direct_exec(&bytecode_program).expect("bytecode tls hello should lower");
    let bytecode = compile_bytecode_program(&bytecode_lowered).expect("tls hello bytecode");
    let bytecode_result = run_bytecode_function(
        &bytecode,
        "serve_https_hello_once",
        &std::collections::HashMap::new(),
    )
    .expect("bytecode tls hello should execute");
    assert_eq!(RuntimeValue::U8(1), bytecode_result);
    bytecode_client
        .join()
        .expect("bytecode tls client should finish");

    let native_port = reserve_closed_port();
    let native_source = rewrite_tls_server_source(&source, native_port, &key, &cert);
    let native_program = parse_program(&native_source).expect("native tls hello should parse");
    let diagnostics = validate_program(&native_program);
    assert!(
        diagnostics.is_empty(),
        "runtime_tls_http_hello native program should validate after rewrite: {diagnostics:?}"
    );
    let native_client = thread::spawn(move || {
        let response = run_tls_client_request(
            native_port,
            b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        );
        let text = String::from_utf8_lossy(&response);
        assert!(
            text.starts_with("HTTP/1.1 200 OK"),
            "unexpected tls hello status: {text}"
        );
        assert!(
            text.ends_with("HELLO_TLS"),
            "unexpected tls hello body: {text}"
        );
    });
    let mut library = emit_library(&native_program).expect("native tls hello library should emit");
    library.push_str("int main(void) {\n");
    library.push_str("  return mira_func_serve_https_hello_once() == 1u ? 0 : 1;\n");
    library.push_str("}\n");
    let binary = compile_c_source("itest_runtime_tls_http_hello", &library)
        .expect("clang should compile tls hello");
    let output = run_binary(&binary).expect("tls hello binary should run");
    native_client
        .join()
        .expect("native tls client should finish");
    assert!(
        output.status.success(),
        "runtime_tls_http_hello native run failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let _ = fs::remove_file(&key);
    let _ = fs::remove_file(&cert);
    let _ = fs::remove_dir_all(&tls_dir);
}

#[test]
fn runtime_tls_auth_gateway_example_runs_across_direct_bytecode_and_native_paths() {
    let tls_dir = unique_temp_dir("mira_tls_auth");
    let (key, cert) = create_tls_materials(&tls_dir);
    let source = fs::read_to_string(examples_dir().join("runtime_tls_auth_gateway.mira"))
        .expect("runtime_tls_auth_gateway example should exist");

    let direct_port = reserve_closed_port();
    let direct_source = rewrite_tls_server_source(&source, direct_port, &key, &cert);
    let direct_program =
        parse_program(&direct_source).expect("direct tls auth program should parse");
    let diagnostics = validate_program(&direct_program);
    assert!(
        diagnostics.is_empty(),
        "runtime_tls_auth_gateway direct program should validate after rewrite: {diagnostics:?}"
    );
    let direct_client = thread::spawn(move || {
        let response = run_tls_client_request(
            direct_port,
            b"GET /secure HTTP/1.1\r\nHost: localhost\r\nAuthorization: token-42\r\nConnection: close\r\n\r\n",
        );
        let text = String::from_utf8_lossy(&response);
        assert!(
            text.starts_with("HTTP/1.1 200 OK"),
            "unexpected tls auth status: {text}"
        );
        assert!(
            text.contains("Content-Type: application/json"),
            "missing tls auth content type: {text}"
        );
        assert!(
            text.ends_with("{\"secure\":true}"),
            "unexpected tls auth body: {text}"
        );
    });
    let direct_lowered =
        lower_program_for_direct_exec(&direct_program).expect("direct tls auth should lower");
    let direct_result = run_lowered_function(
        &direct_lowered,
        "serve_tls_secure_once",
        &std::collections::HashMap::new(),
    )
    .expect("direct tls auth should execute");
    assert_eq!(RuntimeValue::U8(1), direct_result);
    direct_client
        .join()
        .expect("direct tls auth client should finish");

    let bytecode_port = reserve_closed_port();
    let bytecode_source = rewrite_tls_server_source(&source, bytecode_port, &key, &cert);
    let bytecode_program =
        parse_program(&bytecode_source).expect("bytecode tls auth program should parse");
    let diagnostics = validate_program(&bytecode_program);
    assert!(
        diagnostics.is_empty(),
        "runtime_tls_auth_gateway bytecode program should validate after rewrite: {diagnostics:?}"
    );
    let bytecode_client = thread::spawn(move || {
        let response = run_tls_client_request(
            bytecode_port,
            b"GET /secure HTTP/1.1\r\nHost: localhost\r\nAuthorization: token-42\r\nConnection: close\r\n\r\n",
        );
        let text = String::from_utf8_lossy(&response);
        assert!(
            text.starts_with("HTTP/1.1 200 OK"),
            "unexpected tls auth status: {text}"
        );
        assert!(
            text.ends_with("{\"secure\":true}"),
            "unexpected tls auth body: {text}"
        );
    });
    let bytecode_lowered =
        lower_program_for_direct_exec(&bytecode_program).expect("bytecode tls auth should lower");
    let bytecode = compile_bytecode_program(&bytecode_lowered).expect("tls auth bytecode");
    let bytecode_result = run_bytecode_function(
        &bytecode,
        "serve_tls_secure_once",
        &std::collections::HashMap::new(),
    )
    .expect("bytecode tls auth should execute");
    assert_eq!(RuntimeValue::U8(1), bytecode_result);
    bytecode_client
        .join()
        .expect("bytecode tls auth client should finish");

    let native_port = reserve_closed_port();
    let native_source = rewrite_tls_server_source(&source, native_port, &key, &cert);
    let native_program = parse_program(&native_source).expect("native tls auth should parse");
    let diagnostics = validate_program(&native_program);
    assert!(
        diagnostics.is_empty(),
        "runtime_tls_auth_gateway native program should validate after rewrite: {diagnostics:?}"
    );
    let native_client = thread::spawn(move || {
        let response = run_tls_client_request(
            native_port,
            b"GET /secure HTTP/1.1\r\nHost: localhost\r\nAuthorization: token-42\r\nConnection: close\r\n\r\n",
        );
        let text = String::from_utf8_lossy(&response);
        assert!(
            text.starts_with("HTTP/1.1 200 OK"),
            "unexpected tls auth status: {text}"
        );
        assert!(
            text.contains("Content-Type: application/json"),
            "missing tls auth content type: {text}"
        );
        assert!(
            text.ends_with("{\"secure\":true}"),
            "unexpected tls auth body: {text}"
        );
    });
    let mut library = emit_library(&native_program).expect("native tls auth library should emit");
    library.push_str("int main(void) {\n");
    library.push_str("  return mira_func_serve_tls_secure_once() == 1u ? 0 : 1;\n");
    library.push_str("}\n");
    let binary = compile_c_source("itest_runtime_tls_auth_gateway", &library)
        .expect("clang should compile tls auth");
    let output = run_binary(&binary).expect("tls auth binary should run");
    native_client
        .join()
        .expect("native tls auth client should finish");
    assert!(
        output.status.success(),
        "runtime_tls_auth_gateway native run failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let _ = fs::remove_file(&key);
    let _ = fs::remove_file(&cert);
    let _ = fs::remove_dir_all(&tls_dir);
}

#[test]
fn invalid_named_literal_order_and_payload_eq_are_reported() {
    let source = r#"
module invalid.named.literal@1
target native
type point = struct[x:i32,y:i32]
type message = enum[idle,data[value:i32]]

func bad_point
ret point
eff pure
block b0
  v0:point = const point[y=2i32,x=1i32]
  return v0
end

func bad_message
ret b1
eff pure
block b0
  v0:message = const message.data[value=true]
  v1:b1 = eq v0 message.data[value=1i32]
  return v1
end
"#;
    let program = parse_program(source).expect("program should parse");
    let diagnostics = validate_program(&program);
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.error_code == "INVALID_NAMED_LITERAL"),
        "expected invalid named literal diagnostics, got {diagnostics:?}"
    );
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.error_code == "INVALID_ENUM_LITERAL"),
        "expected invalid enum literal diagnostics, got {diagnostics:?}"
    );
}

#[test]
fn invalid_bitwise_operands_are_reported() {
    let source = r#"
module invalid.bits@1
target native

func bad
ret b1
eff pure
block b0
  v0:b1 = band true false
  return v0
end
"#;
    let program = parse_program(source).expect("program should parse");
    let diagnostics = validate_program(&program);
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.error_code == "BITWISE_OPERAND_TYPE"),
        "expected bitwise operand diagnostics, got {diagnostics:?}"
    );
}

#[test]
fn portable_bytecode_verification_runs_runtime_protocol_breadth_tests() {
    let multipart_program =
        load_and_validate(&examples_dir().join("runtime_http_multipart_upload_service.mira"))
            .expect("runtime_http_multipart_upload_service should validate");
    let multipart_lowered = lower_program_for_direct_exec(&multipart_program)
        .expect("runtime_http_multipart_upload_service should lower");
    let multipart_summary = verify_lowered_tests_portably(&multipart_lowered)
        .expect("portable multipart verification should succeed")
        .expect("runtime_http_multipart_upload_service should stay on portable bytecode path");
    assert_eq!("portable bytecode tests passed: 1/1", multipart_summary);

    let streaming_program =
        load_and_validate(&examples_dir().join("runtime_http_streaming_download_service.mira"))
            .expect("runtime_http_streaming_download_service should validate");
    let streaming_lowered = lower_program_for_direct_exec(&streaming_program)
        .expect("runtime_http_streaming_download_service should lower");
    let streaming_summary = verify_lowered_tests_portably(&streaming_lowered)
        .expect("portable streaming verification should succeed")
        .expect("runtime_http_streaming_download_service should stay on portable bytecode path");
    assert_eq!("portable bytecode tests passed: 1/1", streaming_summary);

    let upstream_program =
        load_and_validate(&examples_dir().join("runtime_http_upstream_client_service.mira"))
            .expect("runtime_http_upstream_client_service should validate");
    let upstream_lowered = lower_program_for_direct_exec(&upstream_program)
        .expect("runtime_http_upstream_client_service should lower");
    let upstream_summary = verify_lowered_tests_portably(&upstream_lowered)
        .expect("portable upstream-client verification should succeed")
        .expect("runtime_http_upstream_client_service should stay on portable bytecode path");
    assert_eq!("portable bytecode tests passed: 1/1", upstream_summary);

    let sidecar_program =
        load_and_validate(&examples_dir().join("runtime_spawn_sidecar_service.mira"))
            .expect("runtime_spawn_sidecar_service should validate");
    let sidecar_lowered = lower_program_for_direct_exec(&sidecar_program)
        .expect("runtime_spawn_sidecar_service should lower");
    let sidecar_summary = verify_lowered_tests_portably(&sidecar_lowered)
        .expect("portable sidecar verification should succeed")
        .expect("runtime_spawn_sidecar_service should stay on portable bytecode path");
    assert_eq!("portable bytecode tests passed: 2/2", sidecar_summary);
}

#[test]
fn runtime_http_multipart_upload_service_example_runs_across_portable_and_native_paths() {
    let direct_port = reserve_closed_port();
    let bytecode_port = reserve_closed_port();
    let native_port = reserve_closed_port();
    let source =
        fs::read_to_string(examples_dir().join("runtime_http_multipart_upload_service.mira"))
            .expect("runtime_http_multipart_upload_service example should exist");
    let request = b"POST /upload HTTP/1.1\r\nHost: localhost\r\nContent-Type: multipart/form-data; boundary=BOUNDARY\r\nX-Upload-Token: abc123\r\nContent-Length: 123\r\n\r\n--BOUNDARY\r\nContent-Disposition: form-data; name=\"file\"; filename=\"upload.txt\"\r\nContent-Type: text/plain\r\n\r\nPING\r\n--BOUNDARY--\r\n";
    let client = thread::spawn(move || {
        for port in [direct_port, bytecode_port, native_port] {
            let deadline = Instant::now() + Duration::from_secs(60);
            loop {
                match TcpStream::connect(("127.0.0.1", port)) {
                    Ok(mut stream) => {
                        stream
                            .write_all(request)
                            .expect("multipart client should write request");
                        let _ = stream.shutdown(std::net::Shutdown::Write);
                        let mut response = Vec::new();
                        stream
                            .read_to_end(&mut response)
                            .expect("multipart client should read response");
                        let text = String::from_utf8_lossy(&response);
                        assert!(
                            text.starts_with("HTTP/1.1 201 Created"),
                            "unexpected multipart response head: {text}"
                        );
                        assert!(text.ends_with("uploaded"), "unexpected multipart response body");
                        break;
                    }
                    Err(error) if Instant::now() < deadline => {
                        let _ = error;
                        thread::sleep(Duration::from_millis(25));
                    }
                    Err(error) => panic!("multipart client connect failed: {error}"),
                }
            }
        }
    });

    let direct_source = source.replace("127.0.0.1:39610", &format!("127.0.0.1:{direct_port}"));
    let direct_program =
        parse_program(&direct_source).expect("direct multipart program should parse");
    let diagnostics = validate_program(&direct_program);
    assert!(
        diagnostics.is_empty(),
        "runtime_http_multipart_upload_service direct program should validate after port rewrite: {diagnostics:?}"
    );
    let direct_lowered =
        lower_program_for_direct_exec(&direct_program).expect("direct multipart should lower");
    let direct_result = run_lowered_function(
        &direct_lowered,
        "serve_multipart_upload_once",
        &std::collections::HashMap::new(),
    )
    .expect("direct multipart service should execute");
    assert_eq!(RuntimeValue::U8(80), direct_result);

    let bytecode_source =
        source.replace("127.0.0.1:39610", &format!("127.0.0.1:{bytecode_port}"));
    let bytecode_program =
        parse_program(&bytecode_source).expect("bytecode multipart program should parse");
    let diagnostics = validate_program(&bytecode_program);
    assert!(
        diagnostics.is_empty(),
        "runtime_http_multipart_upload_service bytecode program should validate after port rewrite: {diagnostics:?}"
    );
    let bytecode_lowered = lower_program_for_direct_exec(&bytecode_program)
        .expect("bytecode multipart should lower");
    let bytecode =
        compile_bytecode_program(&bytecode_lowered).expect("multipart bytecode should compile");
    let bytecode_result = run_bytecode_function(
        &bytecode,
        "serve_multipart_upload_once",
        &std::collections::HashMap::new(),
    )
    .expect("bytecode multipart service should execute");
    assert_eq!(RuntimeValue::U8(80), bytecode_result);

    let native_source = source.replace("127.0.0.1:39610", &format!("127.0.0.1:{native_port}"));
    let native_program =
        parse_program(&native_source).expect("native multipart program should parse");
    let diagnostics = validate_program(&native_program);
    assert!(
        diagnostics.is_empty(),
        "runtime_http_multipart_upload_service native program should validate after port rewrite: {diagnostics:?}"
    );
    let mut library = emit_library(&native_program).expect("multipart native library should emit");
    library.push_str("int main(void) {\n");
    library.push_str("  return mira_func_serve_multipart_upload_once() == 80u ? 0 : 1;\n");
    library.push_str("}\n");
    let binary = compile_c_source("itest_runtime_http_multipart_upload_service", &library)
        .expect("clang should compile multipart service");
    let output = run_binary(&binary).expect("multipart binary should run");
    client.join().expect("multipart client thread should finish");
    assert!(
        output.status.success(),
        "runtime_http_multipart_upload_service native run failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn runtime_http_streaming_download_service_example_runs_across_portable_and_native_paths() {
    let direct_port = reserve_closed_port();
    let bytecode_port = reserve_closed_port();
    let native_port = reserve_closed_port();
    let source =
        fs::read_to_string(examples_dir().join("runtime_http_streaming_download_service.mira"))
            .expect("runtime_http_streaming_download_service example should exist");
    let request = b"POST /stream HTTP/1.1\r\nHost: localhost\r\nContent-Length: 9\r\n\r\nSTREAMING";
    let client = thread::spawn(move || {
        for port in [direct_port, bytecode_port, native_port] {
            let deadline = Instant::now() + Duration::from_secs(60);
            loop {
                match TcpStream::connect(("127.0.0.1", port)) {
                    Ok(mut stream) => {
                        stream
                            .write_all(request)
                            .expect("streaming client should write request");
                        let _ = stream.shutdown(std::net::Shutdown::Write);
                        let mut response = Vec::new();
                        stream
                            .read_to_end(&mut response)
                            .expect("streaming client should read response");
                        let text = String::from_utf8_lossy(&response);
                        assert!(
                            text.starts_with("HTTP/1.1 200 OK"),
                            "unexpected streaming response head: {text}"
                        );
                        assert!(
                            text.contains("Transfer-Encoding: chunked"),
                            "streaming response should use chunked transfer: {text}"
                        );
                        assert!(text.contains("HELLO"), "missing first streaming chunk: {text}");
                        assert!(text.contains("-WORLD"), "missing second streaming chunk: {text}");
                        break;
                    }
                    Err(error) if Instant::now() < deadline => {
                        let _ = error;
                        thread::sleep(Duration::from_millis(25));
                    }
                    Err(error) => panic!("streaming client connect failed: {error}"),
                }
            }
        }
    });

    let direct_source = source.replace("127.0.0.1:39611", &format!("127.0.0.1:{direct_port}"));
    let direct_program =
        parse_program(&direct_source).expect("direct streaming program should parse");
    let diagnostics = validate_program(&direct_program);
    assert!(
        diagnostics.is_empty(),
        "runtime_http_streaming_download_service direct program should validate after port rewrite: {diagnostics:?}"
    );
    let direct_lowered =
        lower_program_for_direct_exec(&direct_program).expect("direct streaming should lower");
    let direct_result = run_lowered_function(
        &direct_lowered,
        "serve_streaming_download_once",
        &std::collections::HashMap::new(),
    )
    .expect("direct streaming service should execute");
    assert_eq!(RuntimeValue::U8(65), direct_result);

    let bytecode_source =
        source.replace("127.0.0.1:39611", &format!("127.0.0.1:{bytecode_port}"));
    let bytecode_program =
        parse_program(&bytecode_source).expect("bytecode streaming program should parse");
    let diagnostics = validate_program(&bytecode_program);
    assert!(
        diagnostics.is_empty(),
        "runtime_http_streaming_download_service bytecode program should validate after port rewrite: {diagnostics:?}"
    );
    let bytecode_lowered = lower_program_for_direct_exec(&bytecode_program)
        .expect("bytecode streaming should lower");
    let bytecode =
        compile_bytecode_program(&bytecode_lowered).expect("streaming bytecode should compile");
    let bytecode_result = run_bytecode_function(
        &bytecode,
        "serve_streaming_download_once",
        &std::collections::HashMap::new(),
    )
    .expect("bytecode streaming service should execute");
    assert_eq!(RuntimeValue::U8(65), bytecode_result);

    let native_source = source.replace("127.0.0.1:39611", &format!("127.0.0.1:{native_port}"));
    let native_program =
        parse_program(&native_source).expect("native streaming program should parse");
    let diagnostics = validate_program(&native_program);
    assert!(
        diagnostics.is_empty(),
        "runtime_http_streaming_download_service native program should validate after port rewrite: {diagnostics:?}"
    );
    let mut library = emit_library(&native_program).expect("streaming native library should emit");
    library.push_str("int main(void) {\n");
    library.push_str("  return mira_func_serve_streaming_download_once() == 65u ? 0 : 1;\n");
    library.push_str("}\n");
    let binary = compile_c_source("itest_runtime_http_streaming_download_service", &library)
        .expect("clang should compile streaming service");
    let output = run_binary(&binary).expect("streaming binary should run");
    client.join().expect("streaming client thread should finish");
    assert!(
        output.status.success(),
        "runtime_http_streaming_download_service native run failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn runtime_http_upstream_client_service_example_runs_across_direct_bytecode_and_native_paths() {
    let source =
        fs::read_to_string(examples_dir().join("runtime_http_upstream_client_service.mira"))
            .expect("runtime_http_upstream_client_service example should exist");
    let spawn_upstream = || {
        let listener = TcpListener::bind("127.0.0.1:0").expect("upstream listener should bind");
        let port = listener
            .local_addr()
            .expect("upstream listener should have local addr")
            .port();
        let handle = thread::spawn(move || {
            let listener = listener;
            for path in ["/health", "/ready", "/pooled"] {
                let (mut stream, _) = listener.accept().expect("upstream should accept");
                let mut request = Vec::new();
                stream
                    .read_to_end(&mut request)
                    .expect("upstream should read request");
                let request_text = String::from_utf8_lossy(&request);
                assert!(
                    request_text.contains(path),
                    "expected upstream request path {path}, got {request_text}"
                );
                let body = if path == "/pooled" { "POOL" } else { "OK" };
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                stream
                    .write_all(response.as_bytes())
                    .expect("upstream should write response");
                let _ = stream.shutdown(std::net::Shutdown::Write);
            }
        });
        (port, handle)
    };
    let (direct_port, direct_server) = spawn_upstream();
    let direct_source = source.replace("127.0.0.1:39612", &format!("127.0.0.1:{direct_port}"));
    let direct_program =
        parse_program(&direct_source).expect("direct upstream program should parse");
    let diagnostics = validate_program(&direct_program);
    assert!(
        diagnostics.is_empty(),
        "runtime_http_upstream_client_service direct program should validate after port rewrite: {diagnostics:?}"
    );
    let direct_lowered =
        lower_program_for_direct_exec(&direct_program).expect("direct upstream should lower");
    let direct_result = run_lowered_function(
        &direct_lowered,
        "fetch_three_status_codes",
        &std::collections::HashMap::new(),
    )
    .expect("direct upstream client service should execute");
    assert_eq!(RuntimeValue::U32(600), direct_result);
    direct_server.join().expect("direct upstream server should finish");

    let (bytecode_port, bytecode_server) = spawn_upstream();
    let bytecode_source =
        source.replace("127.0.0.1:39612", &format!("127.0.0.1:{bytecode_port}"));
    let bytecode_program =
        parse_program(&bytecode_source).expect("bytecode upstream program should parse");
    let diagnostics = validate_program(&bytecode_program);
    assert!(
        diagnostics.is_empty(),
        "runtime_http_upstream_client_service bytecode program should validate after port rewrite: {diagnostics:?}"
    );
    let bytecode_lowered = lower_program_for_direct_exec(&bytecode_program)
        .expect("bytecode upstream should lower");
    let bytecode =
        compile_bytecode_program(&bytecode_lowered).expect("upstream bytecode should compile");
    let bytecode_result = run_bytecode_function(
        &bytecode,
        "fetch_three_status_codes",
        &std::collections::HashMap::new(),
    )
    .expect("bytecode upstream client service should execute");
    assert_eq!(RuntimeValue::U32(600), bytecode_result);
    bytecode_server
        .join()
        .expect("bytecode upstream server should finish");

    let (native_port, native_server) = spawn_upstream();
    let native_source = source.replace("127.0.0.1:39612", &format!("127.0.0.1:{native_port}"));
    let native_program =
        parse_program(&native_source).expect("native upstream program should parse");
    let diagnostics = validate_program(&native_program);
    assert!(
        diagnostics.is_empty(),
        "runtime_http_upstream_client_service native program should validate after port rewrite: {diagnostics:?}"
    );
    let mut library = emit_library(&native_program).expect("upstream native library should emit");
    library.push_str("int main(void) {\n");
    library.push_str("  return mira_func_fetch_three_status_codes() == 600u ? 0 : 1;\n");
    library.push_str("}\n");
    let binary = compile_c_source("itest_runtime_http_upstream_client_service", &library)
        .expect("clang should compile upstream service");
    let output = run_binary(&binary).expect("upstream binary should run");
    native_server.join().expect("native upstream server should finish");
    assert!(
        output.status.success(),
        "runtime_http_upstream_client_service native run failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn runtime_multiworker_http_service_shows_many_inflight_requests_natively() {
    let program = load_and_validate(&examples_dir().join("runtime_multiworker_http_service.mira"))
        .expect("runtime_multiworker_http_service should validate");
    let mut library = emit_library(&program).expect("multiworker native library should emit");
    library.push_str("int main(void) {\n");
    library.push_str("  uint32_t inflight = mira_func_observe_inflight_http();\n");
    library.push_str("  return inflight >= 4u ? 0 : 1;\n");
    library.push_str("}\n");
    let binary = compile_c_source("itest_runtime_multiworker_http_inflight", &library)
        .expect("clang should compile inflight probe");
    let output = run_binary(&binary).expect("inflight probe should run");
    assert!(
        output.status.success(),
        "runtime_multiworker_http_service native inflight probe failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn runtime_spawn_sidecar_service_example_runs_across_portable_and_native_paths() {
    let direct_port = reserve_closed_port();
    let bytecode_port = reserve_closed_port();
    let native_port = reserve_closed_port();
    let source = fs::read_to_string(examples_dir().join("runtime_spawn_sidecar_service.mira"))
        .expect("runtime_spawn_sidecar_service example should exist");
    let request = b"POST /sidecar HTTP/1.1\r\nHost: localhost\r\nContent-Length: 4\r\n\r\nPING";
    let client = thread::spawn(move || {
        for port in [direct_port, bytecode_port, native_port] {
            let deadline = Instant::now() + Duration::from_secs(60);
            loop {
                match TcpStream::connect(("127.0.0.1", port)) {
                    Ok(mut stream) => {
                        stream
                            .write_all(request)
                            .expect("sidecar client should write request");
                        let _ = stream.shutdown(std::net::Shutdown::Write);
                        let mut response = Vec::new();
                        stream
                            .read_to_end(&mut response)
                            .expect("sidecar client should read response");
                        let text = String::from_utf8_lossy(&response);
                        assert!(
                            text.starts_with("HTTP/1.1 200 OK"),
                            "unexpected sidecar response head: {text}"
                        );
                        assert!(text.ends_with("PING"), "unexpected sidecar response body");
                        break;
                    }
                    Err(error) if Instant::now() < deadline => {
                        let _ = error;
                        thread::sleep(Duration::from_millis(25));
                    }
                    Err(error) => panic!("sidecar client connect failed: {error}"),
                }
            }
        }
    });

    let direct_source = source.replace("127.0.0.1:39613", &format!("127.0.0.1:{direct_port}"));
    let direct_program =
        parse_program(&direct_source).expect("direct sidecar program should parse");
    let diagnostics = validate_program(&direct_program);
    assert!(
        diagnostics.is_empty(),
        "runtime_spawn_sidecar_service direct program should validate after port rewrite: {diagnostics:?}"
    );
    let direct_lowered =
        lower_program_for_direct_exec(&direct_program).expect("direct sidecar should lower");
    let direct_result = run_lowered_function(
        &direct_lowered,
        "serve_sidecar_echo_once",
        &std::collections::HashMap::new(),
    )
    .expect("direct sidecar service should execute");
    assert_eq!(RuntimeValue::U8(80), direct_result);

    let bytecode_source =
        source.replace("127.0.0.1:39613", &format!("127.0.0.1:{bytecode_port}"));
    let bytecode_program =
        parse_program(&bytecode_source).expect("bytecode sidecar program should parse");
    let diagnostics = validate_program(&bytecode_program);
    assert!(
        diagnostics.is_empty(),
        "runtime_spawn_sidecar_service bytecode program should validate after port rewrite: {diagnostics:?}"
    );
    let bytecode_lowered = lower_program_for_direct_exec(&bytecode_program)
        .expect("bytecode sidecar should lower");
    let bytecode =
        compile_bytecode_program(&bytecode_lowered).expect("sidecar bytecode should compile");
    let bytecode_result = run_bytecode_function(
        &bytecode,
        "serve_sidecar_echo_once",
        &std::collections::HashMap::new(),
    )
    .expect("bytecode sidecar service should execute");
    assert_eq!(RuntimeValue::U8(80), bytecode_result);

    let native_source = source.replace("127.0.0.1:39613", &format!("127.0.0.1:{native_port}"));
    let native_program =
        parse_program(&native_source).expect("native sidecar program should parse");
    let diagnostics = validate_program(&native_program);
    assert!(
        diagnostics.is_empty(),
        "runtime_spawn_sidecar_service native program should validate after port rewrite: {diagnostics:?}"
    );
    let mut library = emit_library(&native_program).expect("sidecar native library should emit");
    library.push_str("int main(void) {\n");
    library.push_str("  return mira_func_serve_sidecar_echo_once() == 80u ? 0 : 1;\n");
    library.push_str("}\n");
    let binary = compile_c_source("itest_runtime_spawn_sidecar_service", &library)
        .expect("clang should compile sidecar service");
    let output = run_binary(&binary).expect("sidecar binary should run");
    client.join().expect("sidecar client thread should finish");
    assert!(
        output.status.success(),
        "runtime_spawn_sidecar_service native run failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
