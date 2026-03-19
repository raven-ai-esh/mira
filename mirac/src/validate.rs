use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};

use crate::ast::{node_path, Block, Diagnostic, Program, Target, Terminator, TypeDeclBody};
use crate::types::{infer_literal_type, parse_data_literal, TypeRef};

type DeclMap<'a> = HashMap<String, &'a TypeDeclBody>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum OwnershipTokenKind {
    Arg,
    HeapAlloc,
    StackAlloc,
    ArenaAlloc,
    SpawnHandle,
    TaskHandle,
    SocketHandle,
    SessionHandle,
    DbHandle,
    DbPoolHandle,
    CacheHandle,
    QueueHandle,
    RuntimeHandle,
    RuntimeTaskHandle,
    ChannelHandle,
    FfiLibHandle,
    ServiceHandle,
    TraceHandle,
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OwnershipBindingKind {
    Owned,
    Borrowed,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct OwnershipState {
    live_tokens: BTreeSet<String>,
    param_tokens: BTreeMap<String, String>,
    token_kinds: BTreeMap<String, BTreeSet<OwnershipTokenKind>>,
    borrow_tokens: BTreeSet<String>,
}

pub fn validate_program(program: &Program) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();
    let type_decls = collect_type_decls(program, &mut diagnostics);
    let named_types = collect_owned_type_decls(program);
    if program.functions.is_empty() {
        diagnostics.push(Diagnostic::new(
            "validate",
            format!("module={}", program.module),
            "NO_FUNCTIONS",
            "program must contain at least one function",
        ));
    }
    for item in &program.consts {
        let node = format!("const={}", item.name);
        validate_type_ref(&item.ty, &type_decls, &node, &mut diagnostics);
        validate_const_value(
            &item.value,
            &item.ty,
            &type_decls,
            &named_types,
            &node,
            &mut diagnostics,
        );
    }
    for function in &program.functions {
        if function.effects.is_empty() {
            diagnostics.push(Diagnostic::new(
                "validate",
                format!("func={}", function.name),
                "MISSING_EFFECTS",
                "every function must declare eff",
            ));
        }
        diagnostics.extend(validate_effects_and_capabilities(function));
        diagnostics.extend(validate_instruction_effect_contracts(function));
        let mut block_map = HashMap::new();
        for block in &function.blocks {
            block_map.insert(block.label.clone(), block);
        }
        if !block_map.contains_key("b0") {
            diagnostics.push(Diagnostic::new(
                "validate",
                format!("func={}", function.name),
                "MISSING_ENTRY_BLOCK",
                "function entry block b0 is required",
            ));
        }
        let mut seen_arg_names = HashSet::new();
        for arg in &function.args {
            if !seen_arg_names.insert(arg.name.clone()) {
                diagnostics.push(
                    Diagnostic::new(
                        "validate",
                        node_path(&[
                            format!("func={}", function.name),
                            format!("arg={}", arg.name),
                        ]),
                        "DUPLICATE_BINDING",
                        format!("duplicate function argument {}", arg.name),
                    )
                    .with_observed(arg.name.clone()),
                );
            }
            validate_type_ref(
                &arg.ty,
                &type_decls,
                &node_path(&[
                    format!("func={}", function.name),
                    format!("arg={}", arg.name),
                ]),
                &mut diagnostics,
            );
        }
        validate_type_ref(
            &function.ret,
            &type_decls,
            &node_path(&[format!("func={}", function.name), "ret".to_string()]),
            &mut diagnostics,
        );
        let function_args = function
            .args
            .iter()
            .map(|arg| (arg.name.clone(), arg.ty.clone()))
            .collect::<HashMap<_, _>>();
        let mut const_bindings = HashMap::new();
        for item in &program.consts {
            const_bindings.insert(item.name.clone(), item.ty.clone());
        }
        for block in &function.blocks {
            diagnostics.extend(validate_block(
                &function.name,
                block,
                &function.ret,
                &function_args,
                &const_bindings,
                &block_map,
                &type_decls,
                &named_types,
            ));
            diagnostics.extend(validate_runtime_spawn_targets(
                function,
                block,
                &program.functions,
            ));
        }
        diagnostics.extend(validate_ownership_flow(function, &block_map));
    }
    diagnostics
}

fn validate_runtime_spawn_targets(
    function: &crate::ast::Function,
    block: &Block,
    functions: &[crate::ast::Function],
) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();
    for instruction in &block.instructions {
        if !matches!(
            instruction.op.as_str(),
            "rt_spawn_u32" | "rt_try_spawn_u32" | "rt_spawn_buf" | "rt_try_spawn_buf"
        ) {
            continue;
        }
        if instruction.args.len() < 2 {
            continue;
        }
        let target_name = &instruction.args[1];
        let node = node_path(&[
            format!("func={}", function.name),
            format!("block={}", block.label),
            format!("instr={}", instruction.bind),
        ]);
        let Some(target) = functions
            .iter()
            .find(|candidate| &candidate.name == target_name)
        else {
            diagnostics.push(
                Diagnostic::new(
                    "validate",
                    node,
                    "UNKNOWN_RUNTIME_TARGET",
                    format!("runtime task target {} does not exist", target_name),
                )
                .with_observed(target_name.clone()),
            );
            continue;
        };
        let matches_signature = match instruction.op.as_str() {
            "rt_spawn_u32" | "rt_try_spawn_u32" => {
                let expected_arg = TypeRef::Int {
                    signed: false,
                    bits: 32,
                };
                let expected_ret = TypeRef::Int {
                    signed: false,
                    bits: 32,
                };
                target.args.len() == 1
                    && target.args[0].ty == expected_arg
                    && target.ret == expected_ret
            }
            "rt_spawn_buf" | "rt_try_spawn_buf" => {
                target.args.len() == 1
                    && is_buf_u8_like_type(&target.args[0].ty)
                    && is_buf_u8_like_type(&target.ret)
            }
            _ => false,
        };
        if !matches_signature {
            diagnostics.push(
                Diagnostic::new(
                    "validate",
                    node,
                    "INVALID_RUNTIME_TARGET_SIGNATURE",
                    format!(
                        "runtime task target {} has incompatible signature for {}",
                        target_name, instruction.op
                    ),
                )
                .with_expected(match instruction.op.as_str() {
                    "rt_spawn_u32" | "rt_try_spawn_u32" => "func name arg x:u32 ret u32",
                    "rt_spawn_buf" | "rt_try_spawn_buf" => {
                        "func name arg x:buf[u8]-like ret buf[u8]-like"
                    }
                    _ => "runtime task target signature",
                })
                .with_observed(format!(
                    "args={} ret={}",
                    target
                        .args
                        .iter()
                        .map(|arg| arg.ty.to_string())
                        .collect::<Vec<_>>()
                        .join(","),
                    target.ret
                )),
            );
        }
    }
    diagnostics
}

fn validate_instruction_effect_contracts(function: &crate::ast::Function) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();
    for block in &function.blocks {
        for instruction in &block.instructions {
            if (instruction.op == "alloc"
                || instruction.op == "drop"
                || instruction.op == "buf_lit"
                || instruction.op == "buf_concat")
                && !function.effects.iter().any(|effect| effect == "alloc")
            {
                diagnostics.push(
                    Diagnostic::new(
                        "validate",
                        node_path(&[
                            format!("func={}", function.name),
                            format!("block={}", block.label),
                            format!("instr={}", instruction.bind),
                        ]),
                        "MISSING_REQUIRED_EFFECT",
                        format!("{} instruction requires eff alloc", instruction.op),
                    )
                    .with_expected("alloc")
                    .with_observed(function.effects.join(" ")),
                );
            }
            if matches!(
                instruction.op.as_str(),
                "rt_open"
                    | "rt_spawn_u32"
                    | "rt_spawn_buf"
                    | "rt_try_spawn_u32"
                    | "rt_try_spawn_buf"
                    | "rt_done"
                    | "rt_join_u32"
                    | "rt_join_buf"
                    | "rt_inflight"
                    | "rt_cancel"
                    | "rt_task_close"
                    | "rt_shutdown"
                    | "rt_close"
                    | "rt_cancelled"
                    | "chan_open_u32"
                    | "chan_open_buf"
                    | "chan_send_u32"
                    | "chan_send_buf"
                    | "chan_recv_u32"
                    | "chan_recv_buf"
                    | "chan_len"
                    | "chan_close"
                    | "deadline_open_ms"
                    | "deadline_expired"
                    | "deadline_remaining_ms"
                    | "deadline_close"
                    | "cancel_scope_open"
                    | "cancel_scope_child"
                    | "cancel_scope_bind_task"
                    | "cancel_scope_cancel"
                    | "cancel_scope_cancelled"
                    | "cancel_scope_close"
                    | "retry_open"
                    | "retry_record_failure"
                    | "retry_record_success"
                    | "retry_next_delay_ms"
                    | "retry_exhausted"
                    | "retry_close"
                    | "circuit_open"
                    | "circuit_allow"
                    | "circuit_record_failure"
                    | "circuit_record_success"
                    | "circuit_state"
                    | "circuit_close"
                    | "backpressure_open"
                    | "backpressure_acquire"
                    | "backpressure_release"
                    | "backpressure_saturated"
                    | "backpressure_close"
                    | "supervisor_open"
                    | "supervisor_record_failure"
                    | "supervisor_record_recovery"
                    | "supervisor_should_restart"
                    | "supervisor_degraded"
                    | "supervisor_close"
            ) && !function.effects.iter().any(|effect| effect == "spawn")
            {
                diagnostics.push(
                    Diagnostic::new(
                        "validate",
                        node_path(&[
                            format!("func={}", function.name),
                            format!("block={}", block.label),
                            format!("instr={}", instruction.bind),
                        ]),
                        "MISSING_REQUIRED_EFFECT",
                        format!("{} instruction requires eff spawn", instruction.op),
                    )
                    .with_expected("spawn")
                    .with_observed(function.effects.join(" ")),
                );
            }
            if instruction.op == "clock_now_ns" || instruction.op == "task_sleep_ms" {
                validate_runtime_capability(
                    function,
                    block,
                    instruction,
                    "clock",
                    "clock",
                    Some("monotonic"),
                    "clock(\"monotonic\")",
                    &mut diagnostics,
                );
            }
            if instruction.op == "rand_u32" {
                validate_runtime_capability(
                    function,
                    block,
                    instruction,
                    "rand",
                    "rand",
                    None,
                    "rand(\"seed=<u32>\")",
                    &mut diagnostics,
                );
                if let Some(payload) = capability_payload(function, "rand") {
                    if parse_rand_seed_payload(payload).is_none() {
                        diagnostics.push(
                            Diagnostic::new(
                                "validate",
                                node_path(&[
                                    format!("func={}", function.name),
                                    format!("block={}", block.label),
                                    format!("instr={}", instruction.bind),
                                ]),
                                "INVALID_RANDOM_CAPABILITY",
                                "rand_u32 requires rand capability payload seed=<u32>",
                            )
                            .with_expected("rand(\"seed=<u32>\")")
                            .with_observed(format!("rand({payload})")),
                        );
                    }
                }
            }
            if instruction.op == "fs_read_u32" || instruction.op == "fs_read_all" {
                validate_runtime_capability(
                    function,
                    block,
                    instruction,
                    "fs.read",
                    "fs",
                    None,
                    "fs(\"/absolute/or/relative/path\")",
                    &mut diagnostics,
                );
                if let Some(payload) = capability_payload(function, "fs") {
                    if parse_fs_path_payload(payload).is_none() {
                        diagnostics.push(
                            Diagnostic::new(
                                "validate",
                                node_path(&[
                                    format!("func={}", function.name),
                                    format!("block={}", block.label),
                                    format!("instr={}", instruction.bind),
                                ]),
                                "INVALID_FILESYSTEM_CAPABILITY",
                                format!(
                                    "{} requires fs capability payload with a non-empty path",
                                    instruction.op
                                ),
                            )
                            .with_expected("fs(\"/absolute/or/relative/path\")")
                            .with_observed(format!("fs({payload})")),
                        );
                    }
                }
            }
            if matches!(
                instruction.op.as_str(),
                "config_get_u32" | "config_get_bool" | "config_get_str"
            ) {
                validate_runtime_capability(
                    function,
                    block,
                    instruction,
                    "config",
                    "config",
                    None,
                    "config(\"key=value,...\")",
                    &mut diagnostics,
                );
            }
            if matches!(
                instruction.op.as_str(),
                "env_get_u32" | "env_get_bool" | "env_get_str"
            ) {
                validate_runtime_capability(
                    function,
                    block,
                    instruction,
                    "env",
                    "env",
                    None,
                    "env(\"NAME,OTHER_NAME\")",
                    &mut diagnostics,
                );
            }
            if instruction.op == "fs_write_u32" || instruction.op == "fs_write_all" {
                validate_runtime_capability(
                    function,
                    block,
                    instruction,
                    "fs.write",
                    "fs",
                    None,
                    "fs(\"/absolute/or/relative/path\")",
                    &mut diagnostics,
                );
                if let Some(payload) = capability_payload(function, "fs") {
                    if parse_fs_path_payload(payload).is_none() {
                        diagnostics.push(
                            Diagnostic::new(
                                "validate",
                                node_path(&[
                                    format!("func={}", function.name),
                                    format!("block={}", block.label),
                                    format!("instr={}", instruction.bind),
                                ]),
                                "INVALID_FILESYSTEM_CAPABILITY",
                                format!(
                                    "{} requires fs capability payload with a non-empty path",
                                    instruction.op
                                ),
                            )
                            .with_expected("fs(\"/absolute/or/relative/path\")")
                            .with_observed(format!("fs({payload})")),
                        );
                    }
                }
            }
            if instruction.op == "ffi_call"
                || instruction.op == "ffi_call_cstr"
                || instruction.op == "ffi_open_lib"
                || instruction.op == "ffi_close_lib"
                || instruction.op == "ffi_call_lib"
                || instruction.op == "ffi_call_lib_cstr"
                || instruction.op == "ffi_buf_ptr"
            {
                validate_runtime_capability(
                    function,
                    block,
                    instruction,
                    "ffi",
                    "ffi",
                    None,
                    "ffi(\"symbol[,symbol...,lib:/path/to/lib]\")",
                    &mut diagnostics,
                );
                if let Some(payload) = capability_payload(function, "ffi") {
                    match parse_ffi_capability_payload(payload) {
                        Some(ffi_cap) => {
                            if instruction.op == "ffi_open_lib" {
                                if let Some(lib) = instruction.args.first() {
                                    if !ffi_cap.libs.iter().any(|allowed| allowed == lib) {
                                        diagnostics.push(
                                            Diagnostic::new(
                                                "validate",
                                                node_path(&[
                                                    format!("func={}", function.name),
                                                    format!("block={}", block.label),
                                                    format!("instr={}", instruction.bind),
                                                ]),
                                                "FFI_LIBRARY_NOT_ALLOWED",
                                                format!(
                                                    "{} library {} is not permitted by the ffi capability",
                                                    instruction.op,
                                                    lib
                                                ),
                                            )
                                            .with_expected(ffi_cap.libs.join(", "))
                                            .with_observed(lib.clone()),
                                        );
                                    }
                                }
                            } else if instruction.op != "ffi_close_lib" && instruction.op != "ffi_buf_ptr" {
                                let symbol_index = if instruction.op == "ffi_call_lib"
                                    || instruction.op == "ffi_call_lib_cstr"
                                {
                                    1
                                } else {
                                    0
                                };
                                if let Some(symbol) = instruction.args.get(symbol_index) {
                                    if !ffi_cap.symbols.iter().any(|allowed| allowed == symbol) {
                                        diagnostics.push(
                                            Diagnostic::new(
                                                "validate",
                                                node_path(&[
                                                    format!("func={}", function.name),
                                                    format!("block={}", block.label),
                                                    format!("instr={}", instruction.bind),
                                                ]),
                                                "FFI_SYMBOL_NOT_ALLOWED",
                                                format!(
                                                    "{} symbol {} is not permitted by the ffi capability",
                                                    instruction.op,
                                                    symbol
                                                ),
                                            )
                                            .with_expected(ffi_cap.symbols.join(", "))
                                            .with_observed(symbol.clone()),
                                        );
                                    }
                                }
                                if (instruction.op == "ffi_call_lib" || instruction.op == "ffi_call_lib_cstr")
                                    && ffi_cap.libs.is_empty()
                                {
                                    diagnostics.push(
                                        Diagnostic::new(
                                            "validate",
                                            node_path(&[
                                                format!("func={}", function.name),
                                                format!("block={}", block.label),
                                                format!("instr={}", instruction.bind),
                                            ]),
                                            "FFI_LIBRARY_NOT_ALLOWED",
                                            format!(
                                                "{} requires at least one allowed ffi library in the capability payload",
                                                instruction.op,
                                            ),
                                        )
                                        .with_expected("ffi(\"symbol[,symbol...,lib:/path/to/lib]\")")
                                        .with_observed(format!("ffi({payload})")),
                                    );
                                }
                            }
                        }
                        None => diagnostics.push(
                            Diagnostic::new(
                                "validate",
                                node_path(&[
                                    format!("func={}", function.name),
                                    format!("block={}", block.label),
                                    format!("instr={}", instruction.bind),
                                ]),
                                "INVALID_FFI_CAPABILITY",
                                format!(
                                    "{} requires ffi capability payload with valid symbols and optional lib:/path entries",
                                    instruction.op
                                ),
                            )
                            .with_expected("ffi(\"symbol[,symbol...,lib:/path/to/lib]\")")
                            .with_observed(format!("ffi({payload})")),
                        ),
                    }
                }
            }
            if instruction.op == "spawn_call"
                || instruction.op == "spawn_capture_all"
                || instruction.op == "spawn_capture_stderr_all"
                || instruction.op == "spawn_open"
                || instruction.op == "spawn_wait"
                || instruction.op == "spawn_stdout_all"
                || instruction.op == "spawn_stderr_all"
                || instruction.op == "spawn_close"
                || instruction.op == "task_open"
                || instruction.op == "task_done"
                || instruction.op == "task_join"
                || instruction.op == "task_stdout_all"
                || instruction.op == "task_stderr_all"
                || instruction.op == "task_close"
            {
                validate_runtime_capability(
                    function,
                    block,
                    instruction,
                    "spawn",
                    "spawn",
                    None,
                    "spawn(\"command[,command...]\")",
                    &mut diagnostics,
                );
                if let Some(payload) = capability_payload(function, "spawn") {
                    match parse_spawn_commands_payload(payload) {
                        Some(commands) => {
                            if matches!(
                                instruction.op.as_str(),
                                "spawn_call"
                                    | "spawn_capture_all"
                                    | "spawn_capture_stderr_all"
                                    | "spawn_open"
                                    | "task_open"
                            ) {
                                if let Some(command) = instruction.args.first() {
                                    if !commands.iter().any(|allowed| allowed == command) {
                                        diagnostics.push(
                                            Diagnostic::new(
                                                "validate",
                                                node_path(&[
                                                format!("func={}", function.name),
                                                format!("block={}", block.label),
                                                format!("instr={}", instruction.bind),
                                            ]),
                                            "SPAWN_COMMAND_NOT_ALLOWED",
                                            format!(
                                                "{} command {} is not permitted by the spawn capability",
                                                instruction.op,
                                                command
                                            ),
                                        )
                                        .with_expected(commands.join(", "))
                                        .with_observed(command.clone()),
                                    );
                                }
                            }
                            }
                        }
                        None => diagnostics.push(
                            Diagnostic::new(
                                "validate",
                                node_path(&[
                                    format!("func={}", function.name),
                                    format!("block={}", block.label),
                                    format!("instr={}", instruction.bind),
                                ]),
                                "INVALID_SPAWN_CAPABILITY",
                                format!(
                                    "{} requires spawn capability payload with one or more valid command names",
                                    instruction.op
                                ),
                            )
                            .with_expected("spawn(\"command[,command...]\")")
                            .with_observed(format!("spawn({payload})")),
                        ),
                    }
                }
            }
            if instruction.op == "net_connect"
                || instruction.op == "tls_exchange_all"
                || instruction.op == "tls_listen"
                || instruction.op == "tls_server_config_u32"
                || instruction.op == "tls_server_config_buf"
                || instruction.op == "net_write_all"
                || instruction.op == "net_exchange_all"
                || instruction.op == "net_serve_exchange_all"
                || instruction.op == "net_listen"
                || instruction.op == "net_session_open"
                || instruction.op == "net_accept"
                || instruction.op == "http_session_accept"
                || instruction.op == "listener_set_timeout_ms"
                || instruction.op == "session_set_timeout_ms"
                || instruction.op == "listener_set_shutdown_grace_ms"
                || instruction.op == "net_read_all"
                || instruction.op == "session_read_chunk"
                || instruction.op == "http_session_request"
                || instruction.op == "net_write_handle_all"
                || instruction.op == "session_write_chunk"
                || instruction.op == "session_flush"
                || instruction.op == "session_alive"
                || instruction.op == "session_heartbeat"
                || instruction.op == "session_backpressure"
                || instruction.op == "session_backpressure_wait"
                || instruction.op == "session_resume_id"
                || instruction.op == "session_reconnect"
                || instruction.op == "net_close"
                || instruction.op == "http_session_close"
                || instruction.op == "http_method_eq"
                || instruction.op == "http_path_eq"
                || instruction.op == "http_request_method"
                || instruction.op == "http_request_path"
                || instruction.op == "http_route_param"
                || instruction.op == "http_header_eq"
                || instruction.op == "http_header"
                || instruction.op == "http_cookie_eq"
                || instruction.op == "http_cookie"
                || instruction.op == "http_body"
                || instruction.op == "http_body_limit"
                || instruction.op == "http_write_response"
                || instruction.op == "http_session_write_text"
                || instruction.op == "http_session_write_json"
                || instruction.op == "http_write_text_response_headers2"
                || instruction.op == "http_write_json_response_headers2"
                || instruction.op == "http_session_write_text_headers2"
                || instruction.op == "http_session_write_json_headers2"
                || instruction.op == "http_session_write_text_cookie"
                || instruction.op == "http_session_write_json_cookie"
                || instruction.op == "http_write_text_response"
                || instruction.op == "http_write_json_response"
                || instruction.op == "http_write_text_response_cookie"
                || instruction.op == "http_write_json_response_cookie"
                || instruction.op == "http_write_response_header"
            {
                validate_runtime_capability(
                    function,
                    block,
                    instruction,
                    "net",
                    "net",
                    None,
                    "net(\"host:port\")",
                    &mut diagnostics,
                );
                if let Some(payload) = capability_payload(function, "net") {
                    if parse_net_endpoint_payload(payload).is_none() {
                        diagnostics.push(
                            Diagnostic::new(
                                "validate",
                                node_path(&[
                                    format!("func={}", function.name),
                                    format!("block={}", block.label),
                                    format!("instr={}", instruction.bind),
                                ]),
                                "INVALID_NET_CAPABILITY",
                                format!(
                                    "{} requires net capability payload host:port",
                                    instruction.op
                                ),
                            )
                            .with_expected("net(\"host:port\")")
                            .with_observed(format!("net({payload})")),
                        );
                    }
                }
            }
            if instruction.op == "tls_listen"
                || instruction.op == "tls_server_config_u32"
                || instruction.op == "tls_server_config_buf"
            {
                validate_runtime_capability(
                    function,
                    block,
                    instruction,
                    "net",
                    "tls",
                    None,
                    "tls(\"cert=/path/to/cert.pem,key=/path/to/key.pem\")",
                    &mut diagnostics,
                );
                if let Some(payload) = capability_payload(function, "tls") {
                    if parse_tls_capability_payload(payload).is_none() {
                        diagnostics.push(
                            Diagnostic::new(
                                "validate",
                                node_path(&[
                                    format!("func={}", function.name),
                                    format!("block={}", block.label),
                                    format!("instr={}", instruction.bind),
                                ]),
                                "INVALID_TLS_CAPABILITY",
                                format!(
                                    "{} requires tls capability payload cert=/path,key=/path",
                                    instruction.op
                                ),
                            )
                            .with_expected("tls(\"cert=/path/to/cert.pem,key=/path/to/key.pem\")")
                            .with_observed(format!("tls({payload})")),
                        );
                    }
                }
            }
            if instruction.op == "db_open"
                || instruction.op == "db_close"
                || instruction.op == "db_exec"
                || instruction.op == "db_prepare"
                || instruction.op == "db_exec_prepared"
                || instruction.op == "db_query_u32"
                || instruction.op == "db_query_buf"
                || instruction.op == "db_query_row"
                || instruction.op == "db_query_prepared_u32"
                || instruction.op == "db_query_prepared_buf"
                || instruction.op == "db_query_prepared_row"
                || instruction.op == "db_row_found"
                || instruction.op == "db_last_error_code"
                || instruction.op == "db_last_error_retryable"
                || instruction.op == "db_begin"
                || instruction.op == "db_commit"
                || instruction.op == "db_rollback"
                || instruction.op == "db_pool_open"
                || instruction.op == "db_pool_set_max_idle"
                || instruction.op == "db_pool_leased"
                || instruction.op == "db_pool_acquire"
                || instruction.op == "db_pool_release"
                || instruction.op == "db_pool_close"
            {
                validate_runtime_capability(
                    function,
                    block,
                    instruction,
                    "db",
                    "db",
                    None,
                    "db(\"/path/to/file.sqlite\")",
                    &mut diagnostics,
                );
                if let Some(payload) = capability_payload(function, "db") {
                    if parse_db_path_payload(payload).is_none() {
                        diagnostics.push(
                            Diagnostic::new(
                                "validate",
                                node_path(&[
                                    format!("func={}", function.name),
                                    format!("block={}", block.label),
                                    format!("instr={}", instruction.bind),
                                ]),
                                "INVALID_DB_CAPABILITY",
                                format!(
                                    "{} requires db capability payload /path/to/file.sqlite",
                                    instruction.op
                                ),
                            )
                            .with_expected("db(\"/path/to/file.sqlite\")")
                            .with_observed(format!("db({payload})")),
                        );
                    }
                }
            }
            if instruction.op == "cache_open"
                || instruction.op == "cache_close"
                || instruction.op == "cache_get_buf"
                || instruction.op == "cache_set_buf"
                || instruction.op == "cache_set_buf_ttl"
                || instruction.op == "cache_del"
            {
                validate_runtime_capability(
                    function,
                    block,
                    instruction,
                    "cache",
                    "cache",
                    None,
                    "cache(\"/path/to/cache.state\")",
                    &mut diagnostics,
                );
                if let Some(payload) = capability_payload(function, "cache") {
                    if payload.trim().is_empty() {
                        diagnostics.push(
                            Diagnostic::new(
                                "validate",
                                format!("{}.{}", function.name, block.label),
                                "INVALID_CACHE_CAPABILITY",
                                format!(
                                    "{} requires non-empty cache capability payload",
                                    instruction.op
                                ),
                            )
                            .with_expected("cache(\"/path/to/cache.state\")")
                            .with_observed(format!("cache({payload})")),
                        );
                    }
                }
            }
            if instruction.op == "queue_open"
                || instruction.op == "queue_close"
                || instruction.op == "queue_push_buf"
                || instruction.op == "queue_pop_buf"
                || instruction.op == "queue_len"
                || instruction.op == "stream_open"
                || instruction.op == "stream_close"
                || instruction.op == "stream_publish_buf"
                || instruction.op == "stream_len"
                || instruction.op == "stream_replay_open"
                || instruction.op == "stream_replay_next"
                || instruction.op == "stream_replay_offset"
                || instruction.op == "stream_replay_close"
                || instruction.op == "lease_open"
                || instruction.op == "lease_acquire"
                || instruction.op == "lease_owner"
                || instruction.op == "lease_transfer"
                || instruction.op == "lease_release"
                || instruction.op == "lease_close"
                || instruction.op == "placement_open"
                || instruction.op == "placement_assign"
                || instruction.op == "placement_lookup"
                || instruction.op == "placement_close"
                || instruction.op == "coord_open"
                || instruction.op == "coord_store_u32"
                || instruction.op == "coord_load_u32"
                || instruction.op == "coord_close"
            {
                validate_runtime_capability(
                    function,
                    block,
                    instruction,
                    "queue",
                    "queue",
                    None,
                    "queue(\"/path/to/queue.state\")",
                    &mut diagnostics,
                );
                if let Some(payload) = capability_payload(function, "queue") {
                    if payload.trim().is_empty() {
                        diagnostics.push(
                            Diagnostic::new(
                                "validate",
                                format!("{}.{}", function.name, block.label),
                                "INVALID_QUEUE_CAPABILITY",
                                format!(
                                    "{} requires non-empty queue capability payload",
                                    instruction.op
                                ),
                            )
                            .with_expected("queue(\"/path/to/queue.state\")")
                            .with_observed(format!("queue({payload})")),
                        );
                    }
                }
            }
            if matches!(
                instruction.op.as_str(),
                "service_open"
                    | "service_close"
                    | "service_shutdown"
                    | "service_log"
                    | "service_trace_begin"
                    | "service_trace_end"
                    | "service_metric_count"
                    | "service_metric_count_dim"
                    | "service_metric_total"
                    | "service_health_status"
                    | "service_readiness_status"
                    | "service_set_health"
                    | "service_set_readiness"
                    | "service_set_degraded"
                    | "service_degraded"
                    | "service_event"
                    | "service_event_total"
                    | "service_trace_link"
                    | "service_trace_link_count"
                    | "service_failure_count"
                    | "service_failure_total"
                    | "service_checkpoint_save_u32"
                    | "service_checkpoint_load_u32"
                    | "service_checkpoint_exists"
                    | "service_migrate_db"
                    | "service_route"
                    | "service_require_header"
                    | "service_error_status"
            ) {
                validate_runtime_capability(
                    function,
                    block,
                    instruction,
                    "service",
                    "service",
                    None,
                    "service(\"name\")",
                    &mut diagnostics,
                );
                if let Some(payload) = capability_payload(function, "service") {
                    if payload.trim().is_empty() {
                        diagnostics.push(
                            Diagnostic::new(
                                "validate",
                                node_path(&[
                                    format!("func={}", function.name),
                                    format!("block={}", block.label),
                                    format!("instr={}", instruction.bind),
                                ]),
                                "INVALID_SERVICE_CAPABILITY",
                                format!(
                                    "{} requires service capability payload name",
                                    instruction.op
                                ),
                            )
                            .with_expected("service(\"name\")")
                            .with_observed(format!("service({payload})")),
                        );
                    }
                }
            }
        }
    }
    diagnostics
}

fn validate_runtime_capability(
    function: &crate::ast::Function,
    block: &crate::ast::Block,
    instruction: &crate::ast::Instruction,
    effect: &str,
    capability_kind: &str,
    expected_payload: Option<&str>,
    expected_capability: &str,
    diagnostics: &mut Vec<Diagnostic>,
) {
    let node = node_path(&[
        format!("func={}", function.name),
        format!("block={}", block.label),
        format!("instr={}", instruction.bind),
    ]);
    if !function.effects.iter().any(|item| item == effect) {
        diagnostics.push(
            Diagnostic::new(
                "validate",
                node.clone(),
                "MISSING_REQUIRED_EFFECT",
                format!("{} instruction requires eff {}", instruction.op, effect),
            )
            .with_expected(effect)
            .with_observed(function.effects.join(" ")),
        );
    }
    let payload = capability_payload(function, capability_kind);
    if payload.is_none() {
        diagnostics.push(
            Diagnostic::new(
                "validate",
                node,
                "MISSING_REQUIRED_CAPABILITY",
                format!(
                    "{} instruction requires capability {}",
                    instruction.op, expected_capability
                ),
            )
            .with_expected(expected_capability)
            .with_observed(function.capabilities.join(" ")),
        );
        return;
    }
    if let Some(expected) = expected_payload {
        let observed = normalize_capability_payload(payload.unwrap());
        if observed != expected {
            diagnostics.push(
                Diagnostic::new(
                    "validate",
                    node,
                    "CAPABILITY_PAYLOAD_MISMATCH",
                    format!(
                        "{} instruction requires {} capability payload {}",
                        instruction.op, capability_kind, expected
                    ),
                )
                .with_expected(expected_capability)
                .with_observed(format!("{capability_kind}({})", payload.unwrap())),
            );
        }
    }
}

fn capability_payload<'a>(function: &'a crate::ast::Function, kind: &str) -> Option<&'a str> {
    function.capabilities.iter().find_map(|capability| {
        let (cap_kind, payload) = parse_capability(capability)?;
        if cap_kind == kind {
            Some(payload)
        } else {
            None
        }
    })
}

fn parse_capability(capability: &str) -> Option<(&str, &str)> {
    let (kind, rest) = capability.split_once('(')?;
    let payload = rest.strip_suffix(')')?;
    Some((kind.trim(), payload.trim()))
}

fn normalize_capability_payload(payload: &str) -> &str {
    payload.trim().trim_matches('"')
}

fn parse_rand_seed_payload(payload: &str) -> Option<u32> {
    let normalized = normalize_capability_payload(payload);
    let seed_text = normalized.strip_prefix("seed=")?;
    let (number, _) = crate::types::split_number_suffix(seed_text)?;
    number.parse::<u32>().ok()
}

fn parse_fs_path_payload(payload: &str) -> Option<&str> {
    let normalized = normalize_capability_payload(payload);
    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }
}

fn parse_config_entries_payload(payload: &str) -> Option<HashMap<String, String>> {
    let normalized = normalize_capability_payload(payload);
    if normalized.is_empty() {
        return None;
    }
    let mut out = HashMap::new();
    for item in normalized.split(',') {
        let (name, value) = item.split_once('=')?;
        let name = name.trim();
        let value = value.trim();
        if name.is_empty() || value.is_empty() {
            return None;
        }
        out.insert(name.to_string(), value.to_string());
    }
    if out.is_empty() {
        None
    } else {
        Some(out)
    }
}

fn parse_env_names_payload(payload: &str) -> Option<Vec<String>> {
    let normalized = normalize_capability_payload(payload);
    if normalized.is_empty() {
        return None;
    }
    let mut out = Vec::new();
    for item in normalized.split(',') {
        let name = item.trim();
        if !is_valid_spawn_env_name(name) {
            return None;
        }
        out.push(name.to_string());
    }
    if out.is_empty() {
        None
    } else {
        Some(out)
    }
}

struct FfiCapability {
    symbols: Vec<String>,
    libs: Vec<String>,
}

#[allow(dead_code)]
struct TlsCapability {
    cert: String,
    key: String,
    request_timeout_ms: Option<u32>,
    session_timeout_ms: Option<u32>,
    shutdown_grace_ms: Option<u32>,
}

fn parse_ffi_capability_payload(payload: &str) -> Option<FfiCapability> {
    let normalized = normalize_capability_payload(payload);
    if normalized.is_empty() {
        return None;
    }
    let mut symbols = Vec::new();
    let mut libs = Vec::new();
    for item in normalized.split(',') {
        let item = item.trim();
        if item.is_empty() {
            return None;
        }
        if let Some(lib) = item.strip_prefix("lib:") {
            if lib.trim().is_empty() {
                return None;
            }
            libs.push(lib.trim().to_string());
            continue;
        }
        if !is_valid_ffi_symbol(item) {
            return None;
        }
        symbols.push(item.to_string());
    }
    if symbols.is_empty() && libs.is_empty() {
        None
    } else {
        Some(FfiCapability { symbols, libs })
    }
}

fn is_valid_ffi_symbol(symbol: &str) -> bool {
    let mut chars = symbol.chars();
    match chars.next() {
        Some(ch) if ch == '_' || ch.is_ascii_alphabetic() => {}
        _ => return false,
    }
    chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

fn parse_spawn_commands_payload(payload: &str) -> Option<Vec<String>> {
    let normalized = normalize_capability_payload(payload);
    if normalized.is_empty() {
        return None;
    }
    let mut out = Vec::new();
    for command in normalized.split(',') {
        let command = command.trim();
        if command.is_empty() || !is_valid_spawn_command(command) {
            return None;
        }
        out.push(command.to_string());
    }
    if out.is_empty() {
        None
    } else {
        Some(out)
    }
}

fn is_valid_spawn_command(command: &str) -> bool {
    command
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.' | '/'))
}

fn is_valid_spawn_arg(token: &str) -> bool {
    !token.is_empty()
        && token.chars().all(|ch| {
            ch.is_ascii_alphanumeric()
                || matches!(ch, '_' | '-' | '.' | '/' | ':' | '=' | '+' | '%')
        })
}

fn is_valid_spawn_env_name(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(ch) if ch == '_' || ch.is_ascii_alphabetic() => {}
        _ => return false,
    }
    chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

fn is_valid_spawn_env_value(value: &str) -> bool {
    !value.is_empty()
        && value.chars().all(|ch| {
            ch.is_ascii_alphanumeric()
                || matches!(ch, '_' | '-' | '.' | '/' | ':' | '+' | '%' | '=')
        })
}

fn validate_spawn_invocation_tokens(tokens: &[String]) -> Result<(), &'static str> {
    let command = tokens.first().ok_or("missing command")?;
    if !is_valid_spawn_command(command) {
        return Err("invalid command");
    }
    for token in tokens.iter().skip(1) {
        if let Some(payload) = token.strip_prefix("env:") {
            let Some((name, value)) = payload.split_once('=') else {
                return Err("invalid env");
            };
            if !is_valid_spawn_env_name(name) || !is_valid_spawn_env_value(value) {
                return Err("invalid env");
            }
        } else if !is_valid_spawn_arg(token) {
            return Err("invalid argv");
        }
    }
    Ok(())
}

fn parse_net_endpoint_payload(payload: &str) -> Option<(String, u16)> {
    let normalized = normalize_capability_payload(payload);
    let (host, port_text) = normalized.rsplit_once(':')?;
    let host = host.trim();
    if host.is_empty() || !is_valid_net_host(host) {
        return None;
    }
    let port = port_text.parse::<u16>().ok()?;
    if port == 0 {
        return None;
    }
    Some((host.to_string(), port))
}

fn parse_db_path_payload(payload: &str) -> Option<String> {
    let path = payload.trim();
    if path.is_empty() {
        None
    } else {
        Some(path.to_string())
    }
}

fn parse_tls_capability_payload(payload: &str) -> Option<TlsCapability> {
    let normalized = normalize_capability_payload(payload);
    if normalized.is_empty() {
        return None;
    }
    let mut cert = None;
    let mut key = None;
    let mut request_timeout_ms = None;
    let mut session_timeout_ms = None;
    let mut shutdown_grace_ms = None;
    for item in normalized.split(',') {
        let (name, value) = item.split_once('=')?;
        let name = name.trim();
        let value = value.trim();
        if value.is_empty() {
            return None;
        }
        match name {
            "cert" => cert = Some(value.to_string()),
            "key" => key = Some(value.to_string()),
            "request_timeout_ms" => request_timeout_ms = value.parse::<u32>().ok(),
            "session_timeout_ms" => session_timeout_ms = value.parse::<u32>().ok(),
            "shutdown_grace_ms" => shutdown_grace_ms = value.parse::<u32>().ok(),
            _ => return None,
        }
    }
    Some(TlsCapability {
        cert: cert?,
        key: key?,
        request_timeout_ms,
        session_timeout_ms,
        shutdown_grace_ms,
    })
}

fn is_valid_net_host(host: &str) -> bool {
    host.chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '-' | '_' | ':'))
}

fn collect_type_decls<'a>(program: &'a Program, diagnostics: &mut Vec<Diagnostic>) -> DeclMap<'a> {
    let mut decls = HashMap::new();
    for item in &program.types {
        let node = format!("type={}", item.name);
        if decls.insert(item.name.clone(), &item.body).is_some() {
            diagnostics.push(
                Diagnostic::new(
                    "validate",
                    node,
                    "DUPLICATE_TYPE",
                    "type name is already declared",
                )
                .with_observed(item.name.clone()),
            );
        }
    }
    for item in &program.types {
        validate_type_decl(item, &decls, diagnostics);
    }
    decls
}

fn collect_owned_type_decls(program: &Program) -> HashMap<String, TypeDeclBody> {
    program
        .types
        .iter()
        .map(|item| (item.name.clone(), item.body.clone()))
        .collect()
}

fn validate_type_decl(
    item: &crate::ast::TypeDecl,
    decls: &DeclMap<'_>,
    diagnostics: &mut Vec<Diagnostic>,
) {
    let node = format!("type={}", item.name);
    match &item.body {
        TypeDeclBody::Struct { fields } => {
            if fields.is_empty() {
                diagnostics.push(Diagnostic::new(
                    "validate",
                    node.clone(),
                    "EMPTY_STRUCT",
                    "struct declarations must contain at least one field",
                ));
            }
            let mut seen = HashSet::new();
            for field in fields {
                if !seen.insert(field.name.clone()) {
                    diagnostics.push(
                        Diagnostic::new(
                            "validate",
                            node.clone(),
                            "DUPLICATE_FIELD",
                            format!("field {} is already declared", field.name),
                        )
                        .with_observed(field.name.clone()),
                    );
                }
                validate_type_ref(
                    &field.ty,
                    decls,
                    &node_path(&[node.clone(), format!("field={}", field.name)]),
                    diagnostics,
                );
            }
        }
        TypeDeclBody::Enum { variants } => {
            if variants.is_empty() {
                diagnostics.push(Diagnostic::new(
                    "validate",
                    node.clone(),
                    "EMPTY_ENUM",
                    "enum declarations must contain at least one variant",
                ));
            }
            let mut seen = HashSet::new();
            for variant in variants {
                if !seen.insert(variant.name.clone()) {
                    diagnostics.push(
                        Diagnostic::new(
                            "validate",
                            node.clone(),
                            "DUPLICATE_VARIANT",
                            format!("variant {} is already declared", variant.name),
                        )
                        .with_observed(variant.name.clone()),
                    );
                }
                let mut field_names = HashSet::new();
                for field in &variant.fields {
                    if !field_names.insert(field.name.clone()) {
                        diagnostics.push(
                            Diagnostic::new(
                                "validate",
                                node_path(&[node.clone(), format!("variant={}", variant.name)]),
                                "DUPLICATE_FIELD",
                                format!("field {} is already declared", field.name),
                            )
                            .with_observed(field.name.clone()),
                        );
                    }
                    validate_type_ref(
                        &field.ty,
                        decls,
                        &node_path(&[
                            node.clone(),
                            format!("variant={}", variant.name),
                            format!("field={}", field.name),
                        ]),
                        diagnostics,
                    );
                }
            }
        }
    }
}

fn validate_type_ref(
    ty: &TypeRef,
    decls: &DeclMap<'_>,
    node: &str,
    diagnostics: &mut Vec<Diagnostic>,
) {
    match ty {
        TypeRef::Named(name) => {
            if !decls.contains_key(name) {
                diagnostics.push(
                    Diagnostic::new(
                        "validate",
                        node.to_string(),
                        "UNKNOWN_TYPE",
                        format!("unknown type {name}"),
                    )
                    .with_observed(name.clone()),
                );
            }
        }
        TypeRef::Span(inner)
        | TypeRef::Buf(inner)
        | TypeRef::Option(inner)
        | TypeRef::Own(inner)
        | TypeRef::View(inner)
        | TypeRef::Edit(inner) => validate_type_ref(inner, decls, node, diagnostics),
        TypeRef::Vec { elem, .. } => validate_type_ref(elem, decls, node, diagnostics),
        TypeRef::Result { ok, err } => {
            validate_type_ref(ok, decls, node, diagnostics);
            validate_type_ref(err, decls, node, diagnostics);
        }
        _ => {}
    }
}

fn validate_const_value(
    value: &str,
    ty: &TypeRef,
    decls: &DeclMap<'_>,
    named_types: &HashMap<String, TypeDeclBody>,
    node: &str,
    diagnostics: &mut Vec<Diagnostic>,
) {
    match parse_data_literal(value, ty, Some(named_types)) {
        Ok(_) => {}
        Err(error) => {
            let diagnostic = match ty {
                TypeRef::Named(name)
                    if matches!(decls.get(name), Some(TypeDeclBody::Enum { .. })) =>
                {
                    Diagnostic::new("typecheck", node.to_string(), "INVALID_ENUM_LITERAL", error)
                        .with_expected(format!(
                            "{name}.<variant> or {name}.<variant>[field=value,...]"
                        ))
                        .with_observed(value.to_string())
                }
                TypeRef::Named(name) => Diagnostic::new(
                    "typecheck",
                    node.to_string(),
                    "INVALID_NAMED_LITERAL",
                    error,
                )
                .with_expected(format!("{name}[field=value,...]"))
                .with_observed(value.to_string()),
                _ => Diagnostic::new("typecheck", node.to_string(), "INVALID_CONST", error)
                    .with_observed(value.to_string()),
            };
            diagnostics.push(diagnostic);
        }
    }
}

fn validate_effects_and_capabilities(function: &crate::ast::Function) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();
    let function_path = format!("func={}", function.name);
    let allowed = [
        "pure", "alloc", "fs.read", "fs.write", "net", "clock", "rand", "spawn", "ffi", "db",
        "cache", "queue", "config", "env", "service",
    ];
    for effect in &function.effects {
        if !allowed.contains(&effect.as_str()) {
            diagnostics.push(
                Diagnostic::new(
                    "validate",
                    function_path.clone(),
                    "UNKNOWN_EFFECT",
                    format!("unknown effect {effect}"),
                )
                .with_observed(effect.clone())
                .with_fix_hint(
                    "use one of pure alloc fs.read fs.write net clock rand spawn ffi db cache queue config env service",
                ),
            );
        }
    }
    if function.effects.iter().any(|effect| effect == "pure") {
        if function.effects.len() != 1 {
            diagnostics.push(
                Diagnostic::new(
                    "validate",
                    function_path.clone(),
                    "PURE_EFFECT_CONFLICT",
                    "pure functions cannot declare additional effects",
                )
                .with_expected("pure")
                .with_observed(function.effects.join(" ")),
            );
        }
        if !function.capabilities.is_empty() {
            diagnostics.push(
                Diagnostic::new(
                    "validate",
                    function_path.clone(),
                    "PURE_CAPABILITY_CONFLICT",
                    "pure functions cannot declare capabilities",
                )
                .with_observed(function.capabilities.join(" ")),
            );
        }
    }
    for capability in &function.capabilities {
        let Some((kind, _payload)) = capability.split_once('(') else {
            diagnostics.push(
                Diagnostic::new(
                    "validate",
                    function_path.clone(),
                    "INVALID_CAPABILITY",
                    "capability must use namespace(payload) syntax",
                )
                .with_observed(capability.clone()),
            );
            continue;
        };
        if !capability.ends_with(')') {
            diagnostics.push(
                Diagnostic::new(
                    "validate",
                    function_path.clone(),
                    "INVALID_CAPABILITY",
                    "capability must end with ')'",
                )
                .with_observed(capability.clone()),
            );
            continue;
        }
        let required_effect = match kind.trim() {
            "fs" => Some(["fs.read", "fs.write"].as_slice()),
            "net" => Some(["net"].as_slice()),
            "tls" => Some(["net"].as_slice()),
            "ffi" => Some(["ffi"].as_slice()),
            "clock" => Some(["clock"].as_slice()),
            "rand" => Some(["rand"].as_slice()),
            "spawn" => Some(["spawn"].as_slice()),
            "db" => Some(["db"].as_slice()),
            "cache" => Some(["cache"].as_slice()),
            "queue" => Some(["queue"].as_slice()),
            "config" => Some(["config"].as_slice()),
            "env" => Some(["env"].as_slice()),
            "service" => Some(["service"].as_slice()),
            _ => None,
        };
        match required_effect {
            Some(required) => {
                if !required
                    .iter()
                    .any(|needed| function.effects.iter().any(|effect| effect == needed))
                {
                    diagnostics.push(
                        Diagnostic::new(
                            "validate",
                            function_path.clone(),
                            "CAPABILITY_EFFECT_MISMATCH",
                            format!("capability {capability} requires a matching effect"),
                        )
                        .with_expected(required.join(" or "))
                        .with_observed(function.effects.join(" ")),
                    );
                }
            }
            None => diagnostics.push(
                Diagnostic::new(
                    "validate",
                    function_path.clone(),
                    "UNKNOWN_CAPABILITY_KIND",
                    format!("unknown capability kind {kind}"),
                )
                .with_observed(capability.clone()),
            ),
        }
        match kind.trim() {
            "config" => {
                let payload = capability
                    .strip_prefix("config(")
                    .and_then(|value| value.strip_suffix(')'));
                if payload.and_then(parse_config_entries_payload).is_none() {
                    diagnostics.push(
                        Diagnostic::new(
                            "validate",
                            function_path.clone(),
                            "INVALID_CONFIG_CAPABILITY",
                            "config capability must use key=value pairs separated by commas",
                        )
                        .with_expected("config(\"key=value,...\")")
                        .with_observed(capability.clone()),
                    );
                }
            }
            "env" => {
                let payload = capability
                    .strip_prefix("env(")
                    .and_then(|value| value.strip_suffix(')'));
                if payload.and_then(parse_env_names_payload).is_none() {
                    diagnostics.push(
                        Diagnostic::new(
                            "validate",
                            function_path.clone(),
                            "INVALID_ENV_CAPABILITY",
                            "env capability must list allowed variable names separated by commas",
                        )
                        .with_expected("env(\"NAME,OTHER_NAME\")")
                        .with_observed(capability.clone()),
                    );
                }
            }
            "cache" => {
                let payload = capability
                    .strip_prefix("cache(")
                    .and_then(|value| value.strip_suffix(')'));
                if payload.map(|value| value.trim().is_empty()).unwrap_or(true) {
                    diagnostics.push(
                        Diagnostic::new(
                            "validate",
                            function_path.clone(),
                            "INVALID_CACHE_CAPABILITY",
                            "cache capability must provide a non-empty state path",
                        )
                        .with_expected("cache(\"/path/to/cache.state\")")
                        .with_observed(capability.clone()),
                    );
                }
            }
            "queue" => {
                let payload = capability
                    .strip_prefix("queue(")
                    .and_then(|value| value.strip_suffix(')'));
                if payload.map(|value| value.trim().is_empty()).unwrap_or(true) {
                    diagnostics.push(
                        Diagnostic::new(
                            "validate",
                            function_path.clone(),
                            "INVALID_QUEUE_CAPABILITY",
                            "queue capability must provide a non-empty state path",
                        )
                        .with_expected("queue(\"/path/to/queue.state\")")
                        .with_observed(capability.clone()),
                    );
                }
            }
            _ => {}
        }
    }
    diagnostics
}

fn validate_block(
    function_name: &str,
    block: &Block,
    function_ret: &TypeRef,
    function_args: &HashMap<String, TypeRef>,
    const_bindings: &HashMap<String, TypeRef>,
    block_map: &HashMap<String, &Block>,
    type_decls: &DeclMap<'_>,
    named_types: &HashMap<String, TypeDeclBody>,
) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();
    let block_path = node_path(&[
        format!("func={function_name}"),
        format!("block={}", block.label),
    ]);
    if !block.label.starts_with('b') || block.label[1..].parse::<usize>().is_err() {
        diagnostics.push(
            Diagnostic::new(
                "validate",
                block_path.clone(),
                "INVALID_BLOCK_NAME",
                "blocks must use b0..bN naming",
            )
            .with_observed(block.label.clone()),
        );
    }

    let mut env = const_bindings.clone();
    let mut seen = HashSet::new();
    let mut consumed = HashSet::new();
    for constant in const_bindings.keys() {
        seen.insert(constant.clone());
    }
    if block.label == "b0" {
        for (name, ty) in function_args {
            env.insert(name.clone(), ty.clone());
        }
    }
    for arg in env.keys() {
        seen.insert(arg.clone());
    }
    for param in &block.params {
        validate_type_ref(
            &param.ty,
            type_decls,
            &node_path(&[block_path.clone(), format!("param={}", param.name)]),
            &mut diagnostics,
        );
        if seen.contains(&param.name) {
            diagnostics.push(
                Diagnostic::new(
                    "validate",
                    block_path.clone(),
                    "DUPLICATE_BINDING",
                    format!("duplicate block parameter {}", param.name),
                )
                .with_observed(param.name.clone()),
            );
        }
        env.insert(param.name.clone(), param.ty.clone());
        seen.insert(param.name.clone());
    }

    for instruction in &block.instructions {
        let instruction_path =
            node_path(&[block_path.clone(), format!("instr={}", instruction.bind)]);
        validate_instruction_operands_not_consumed(
            instruction,
            &instruction_path,
            &consumed,
            &mut diagnostics,
        );
        validate_type_ref(
            &instruction.ty,
            type_decls,
            &instruction_path,
            &mut diagnostics,
        );
        if !instruction.bind.starts_with('v') || instruction.bind[1..].parse::<usize>().is_err() {
            diagnostics.push(
                Diagnostic::new(
                    "validate",
                    instruction_path.clone(),
                    "INVALID_LOCAL_NAME",
                    "locals must use v0..vN naming",
                )
                .with_observed(instruction.bind.clone()),
            );
        }
        if seen.contains(&instruction.bind) {
            diagnostics.push(
                Diagnostic::new(
                    "validate",
                    instruction_path.clone(),
                    "DUPLICATE_BINDING",
                    format!("binding {} is already defined", instruction.bind),
                )
                .with_observed(instruction.bind.clone()),
            );
        }
        if let Some(inferred) = infer_instruction_type(
            instruction,
            &env,
            &instruction_path,
            &mut diagnostics,
            type_decls,
            named_types,
        ) {
            if inferred != instruction.ty {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        instruction_path.clone(),
                        "TYPE_MISMATCH",
                        format!("instruction result type mismatch for {}", instruction.op),
                    )
                    .with_expected(instruction.ty.to_string())
                    .with_observed(inferred.to_string())
                    .with_fix_hint(
                        "insert an explicit cast or change the declared instruction type",
                    ),
                );
            }
        }
        env.insert(instruction.bind.clone(), instruction.ty.clone());
        seen.insert(instruction.bind.clone());
        if instruction.op == "drop" {
            if let Some(handle) = instruction.args.first() {
                if env.contains_key(handle) {
                    consumed.insert(handle.clone());
                }
            }
        }
        for handle in runtime_close_operands(instruction) {
            if env.contains_key(handle) {
                consumed.insert(handle.clone());
            }
        }
    }

    diagnostics.extend(validate_terminator(
        &block.terminator,
        &block_path,
        function_ret,
        &env,
        block_map,
        type_decls,
        &consumed,
    ));
    diagnostics
}

fn validate_terminator(
    terminator: &Terminator,
    block_path: &str,
    function_ret: &TypeRef,
    env: &HashMap<String, TypeRef>,
    block_map: &HashMap<String, &Block>,
    type_decls: &DeclMap<'_>,
    consumed: &HashSet<String>,
) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();
    match terminator {
        Terminator::Return(value) => {
            let node = node_path(&[block_path.to_string(), "term=return".to_string()]);
            validate_operand_not_consumed(value, &node, consumed, &mut diagnostics);
            match resolve_operand_type(value, env) {
                Some(found) if &found != function_ret => diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node,
                        "RETURN_TYPE_MISMATCH",
                        "return value does not match function ret",
                    )
                    .with_expected(function_ret.to_string())
                    .with_observed(found.to_string()),
                ),
                None => diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node,
                        "UNKNOWN_VALUE",
                        format!("unknown return value {value}"),
                    )
                    .with_observed(value.clone()),
                ),
                _ => {}
            }
        }
        Terminator::Jump(target) => diagnostics.extend(validate_target(
            target, block_path, env, block_map, consumed,
        )),
        Terminator::Branch {
            condition,
            truthy,
            falsy,
        } => {
            validate_operand_not_consumed(
                condition,
                &node_path(&[block_path.to_string(), "term=branch".to_string()]),
                consumed,
                &mut diagnostics,
            );
            match resolve_operand_type(condition, env) {
                Some(found) if !found.is_bool() => diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node_path(&[block_path.to_string(), "term=branch".to_string()]),
                        "BRANCH_CONDITION_TYPE",
                        "branch condition must be b1",
                    )
                    .with_expected("b1")
                    .with_observed(found.to_string()),
                ),
                None => diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node_path(&[block_path.to_string(), "term=branch".to_string()]),
                        "UNKNOWN_VALUE",
                        format!("unknown branch condition {condition}"),
                    )
                    .with_observed(condition.clone()),
                ),
                _ => {}
            }
            diagnostics.extend(validate_target(
                truthy, block_path, env, block_map, consumed,
            ));
            diagnostics.extend(validate_target(falsy, block_path, env, block_map, consumed));
        }
        Terminator::Match { value, arms } => {
            validate_operand_not_consumed(
                value,
                &node_path(&[block_path.to_string(), "term=match".to_string()]),
                consumed,
                &mut diagnostics,
            );
            match resolve_operand_type(value, env) {
                Some(found) if is_matchable_type(&found, type_decls) => {}
                Some(found) => diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node_path(&[block_path.to_string(), "term=match".to_string()]),
                        "MATCH_VALUE_TYPE",
                        "match value must be an integer, b1, or enum",
                    )
                    .with_expected("integer or b1 or enum")
                    .with_observed(found.to_string()),
                ),
                None => diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node_path(&[block_path.to_string(), "term=match".to_string()]),
                        "UNKNOWN_VALUE",
                        format!("unknown match value {value}"),
                    )
                    .with_observed(value.clone()),
                ),
            }
            if arms.is_empty() {
                diagnostics.push(Diagnostic::new(
                    "validate",
                    node_path(&[block_path.to_string(), "term=match".to_string()]),
                    "MATCH_ARMS_MISSING",
                    "match requires at least one arm",
                ));
            }
            for arm in arms {
                diagnostics.extend(validate_target(arm, block_path, env, block_map, consumed));
            }
        }
    }
    diagnostics
}

fn validate_target(
    target: &Target,
    block_path: &str,
    env: &HashMap<String, TypeRef>,
    block_map: &HashMap<String, &Block>,
    consumed: &HashSet<String>,
) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();
    let target_path = node_path(&[block_path.to_string(), format!("target={}", target.label)]);
    let Some(block) = block_map.get(&target.label) else {
        diagnostics.push(
            Diagnostic::new(
                "validate",
                target_path,
                "UNKNOWN_BLOCK",
                format!("jump target {} does not exist", target.label),
            )
            .with_observed(target.label.clone()),
        );
        return diagnostics;
    };
    if target.args.len() != block.params.len() {
        diagnostics.push(
            Diagnostic::new(
                "typecheck",
                target_path,
                "BLOCK_ARG_ARITY",
                format!(
                    "target {} expects {} arguments",
                    target.label,
                    block.params.len()
                ),
            )
            .with_expected(block.params.len().to_string())
            .with_observed(target.args.len().to_string()),
        );
        return diagnostics;
    }
    for (operand, param) in target.args.iter().zip(block.params.iter()) {
        validate_operand_not_consumed(operand, &target_path, consumed, &mut diagnostics);
        match resolve_operand_type(operand, env) {
            Some(found) if found != param.ty => diagnostics.push(
                Diagnostic::new(
                    "typecheck",
                    node_path(&[block_path.to_string(), format!("target={}", target.label)]),
                    "BLOCK_ARG_TYPE",
                    format!(
                        "argument {operand} does not match {}:{}",
                        param.name, param.ty
                    ),
                )
                .with_expected(param.ty.to_string())
                .with_observed(found.to_string()),
            ),
            None => diagnostics.push(
                Diagnostic::new(
                    "typecheck",
                    node_path(&[block_path.to_string(), format!("target={}", target.label)]),
                    "UNKNOWN_VALUE",
                    format!("unknown value {operand} passed to {}", target.label),
                )
                .with_observed(operand.clone()),
            ),
            _ => {}
        }
    }
    diagnostics
}

fn infer_instruction_type(
    instruction: &crate::ast::Instruction,
    env: &HashMap<String, TypeRef>,
    node: &str,
    diagnostics: &mut Vec<Diagnostic>,
    type_decls: &DeclMap<'_>,
    named_types: &HashMap<String, TypeDeclBody>,
) -> Option<TypeRef> {
    match instruction.op.as_str() {
        "const" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "const expects 1 operand",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            match parse_data_literal(&instruction.args[0], &instruction.ty, Some(named_types)) {
                Ok(_) => Some(instruction.ty.clone()),
                Err(error) => {
                    let diagnostic = match &instruction.ty {
                        TypeRef::Named(name)
                            if matches!(type_decls.get(name), Some(TypeDeclBody::Enum { .. })) =>
                        {
                            Diagnostic::new(
                                "typecheck",
                                node.to_string(),
                                "INVALID_ENUM_LITERAL",
                                error,
                            )
                            .with_expected(format!(
                                "{name}.<variant> or {name}.<variant>[field=value,...]"
                            ))
                            .with_observed(instruction.args[0].clone())
                        }
                        TypeRef::Named(name) => Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "INVALID_NAMED_LITERAL",
                            error,
                        )
                        .with_expected(format!("{name}[field=value,...]"))
                        .with_observed(instruction.args[0].clone()),
                        _ => Diagnostic::new("typecheck", node.to_string(), "INVALID_CONST", error)
                            .with_observed(instruction.args[0].clone()),
                    };
                    diagnostics.push(diagnostic);
                    None
                }
            }
        }
        "len" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "len expects 1 operand",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(found) if supports_len(&found) => Some(TypeRef::Int {
                    signed: false,
                    bits: 32,
                }),
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "LEN_OPERAND_TYPE",
                            "len expects span[T], buf[T], or vec[N,T]",
                        )
                        .with_expected("span[T] | buf[T] | vec[N,T]")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_VALUE",
                            format!("unknown value {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "load" => {
            if instruction.args.len() != 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "load expects 2 operands",
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let collection = resolve_operand_type(&instruction.args[0], env);
            let index = resolve_operand_type(&instruction.args[1], env);
            match (collection, index) {
                (
                    Some(found),
                    Some(TypeRef::Int {
                        signed: false,
                        bits: 32,
                    }),
                ) => match load_result_type(&found) {
                    Some(result) => Some(result),
                    None => {
                        diagnostics.push(
                            Diagnostic::new(
                                "typecheck",
                                node.to_string(),
                                "LOAD_OPERAND_TYPE",
                                "load expects span[T], buf[T], or vec[N,T] plus u32",
                            )
                            .with_expected("span[T] | buf[T] | vec[N,T], u32")
                            .with_observed(found.to_string()),
                        );
                        None
                    }
                },
                (Some(found), Some(index_ty)) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "LOAD_OPERAND_TYPE",
                            "load expects span[T], buf[T], or vec[N,T] plus u32",
                        )
                        .with_expected("span[T] | buf[T] | vec[N,T], u32")
                        .with_observed(format!("{found}, {index_ty}")),
                    );
                    None
                }
                _ => {
                    diagnostics.push(Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "UNKNOWN_VALUE",
                        "load operands must be defined",
                    ));
                    None
                }
            }
        }
        "alloc" => {
            if instruction.args.len() != 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "alloc expects region and length",
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let TypeRef::Own(inner) = &instruction.ty else {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "ALLOC_RESULT_TYPE",
                        "alloc result must be own[buf[T]]",
                    )
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            };
            let TypeRef::Buf(_) = inner.as_ref() else {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "ALLOC_RESULT_TYPE",
                        "alloc result must be own[buf[T]]",
                    )
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            };
            if instruction.args[0] != "stack"
                && instruction.args[0] != "heap"
                && instruction.args[0] != "arena"
            {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "ALLOC_REGION",
                        "alloc region must be stack, heap, or arena",
                    )
                    .with_expected("stack | heap | arena")
                    .with_observed(instruction.args[0].clone()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[1], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 32,
                }) => Some(instruction.ty.clone()),
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "ALLOC_LENGTH_TYPE",
                            "alloc length must be u32",
                        )
                        .with_expected("u32")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_VALUE",
                            format!("unknown value {}", instruction.args[1]),
                        )
                        .with_observed(instruction.args[1].clone()),
                    );
                    None
                }
            }
        }
        "drop" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "drop expects one owned borrow-tracked operand",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "DROP_RESULT_TYPE",
                        "drop result type must be b1",
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Own(inner)) if is_borrow_trackable_inner(inner.as_ref()) => {
                    Some(TypeRef::Bool)
                }
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "DROP_OPERAND_TYPE",
                            "drop expects own[buf[T]], own[str], own[vec[N,T]], or own[named]",
                        )
                        .with_expected("own[buf[T]] | own[str] | own[vec[N,T]] | own[named]")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_VALUE",
                            format!("unknown value {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "clock_now_ns" => {
            if !instruction.args.is_empty() {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "clock_now_ns expects 0 operands",
                    )
                    .with_expected("0")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            Some(TypeRef::Int {
                signed: false,
                bits: 64,
            })
        }
        "task_sleep_ms" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "task_sleep_ms expects one u32 operand",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "CLOCK_RESULT_TYPE",
                        "task_sleep_ms result type must be b1",
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 32,
                }) => Some(TypeRef::Bool),
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "CLOCK_OPERAND_TYPE",
                            "task_sleep_ms expects u32",
                        )
                        .with_expected("u32")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_VALUE",
                            format!("unknown value {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "rt_open" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "rt_open expects one worker-count operand",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 32,
                }) => {}
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "RUNTIME_ARG_TYPE",
                            "rt_open expects u32 worker count",
                        )
                        .with_expected("u32")
                        .with_observed(found.to_string()),
                    );
                    return None;
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_VALUE",
                            format!("unknown value {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    return None;
                }
            }
            match &instruction.ty {
                TypeRef::Int {
                    signed: false,
                    bits: 64,
                } => Some(instruction.ty.clone()),
                found => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "RUNTIME_RESULT_TYPE",
                            "rt_open result type must be u64",
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    None
                }
            }
        }
        "rt_spawn_u32" | "rt_try_spawn_u32" => {
            if instruction.args.len() != 3 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!(
                            "{} expects runtime handle, function token, and u32 arg",
                            instruction.op
                        ),
                    )
                    .with_expected("3")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64,
                }) => {}
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "RUNTIME_ARG_TYPE",
                            format!("{} expects u64 runtime handle as first operand", instruction.op),
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    return None;
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_VALUE",
                            format!("unknown value {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    return None;
                }
            }
            match resolve_operand_type(&instruction.args[2], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 32,
                }) => {}
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "RUNTIME_ARG_TYPE",
                            format!("{} expects u32 payload as third operand", instruction.op),
                        )
                        .with_expected("u32")
                        .with_observed(found.to_string()),
                    );
                    return None;
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_VALUE",
                            format!("unknown value {}", instruction.args[2]),
                        )
                        .with_observed(instruction.args[2].clone()),
                    );
                    return None;
                }
            }
            match &instruction.ty {
                TypeRef::Int {
                    signed: false,
                    bits: 64,
                } => Some(instruction.ty.clone()),
                found => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "RUNTIME_RESULT_TYPE",
                            format!("{} result type must be u64", instruction.op),
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    None
                }
            }
        }
        "rt_spawn_buf" | "rt_try_spawn_buf" => {
            if instruction.args.len() != 3 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!(
                            "{} expects runtime handle, function token, and buf[u8] payload",
                            instruction.op
                        ),
                    )
                    .with_expected("3")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(found) if is_u64_type(&found) => {}
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "RUNTIME_ARG_TYPE",
                            format!("{} expects u64 runtime handle as first operand", instruction.op),
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    return None;
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_VALUE",
                            format!("unknown value {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    return None;
                }
            }
            match resolve_operand_type(&instruction.args[2], env) {
                Some(found) if is_buf_u8_like_type(&found) => {}
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "RUNTIME_ARG_TYPE",
                            format!("{} expects buf[u8]-like payload as third operand", instruction.op),
                        )
                        .with_expected("buf[u8]-like")
                        .with_observed(found.to_string()),
                    );
                    return None;
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_VALUE",
                            format!("unknown value {}", instruction.args[2]),
                        )
                        .with_observed(instruction.args[2].clone()),
                    );
                    return None;
                }
            }
            if !is_u64_type(&instruction.ty) {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "RUNTIME_RESULT_TYPE",
                        format!("{} result type must be u64", instruction.op),
                    )
                    .with_expected("u64")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            Some(instruction.ty.clone())
        }
        "rt_done"
        | "rt_cancel"
        | "rt_task_close"
        | "rt_close"
        | "chan_close"
        | "deadline_close"
        | "cancel_scope_cancel"
        | "cancel_scope_close"
        | "retry_close"
        | "circuit_close"
        | "backpressure_close"
        | "supervisor_close" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!("{} expects one handle operand", instruction.op),
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64,
                }) => {}
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "RUNTIME_ARG_TYPE",
                            format!("{} expects u64 handle operand", instruction.op),
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    return None;
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_VALUE",
                            format!("unknown value {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    return None;
                }
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "RUNTIME_RESULT_TYPE",
                        format!("{} result type must be b1", instruction.op),
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            Some(TypeRef::Bool)
        }
        "cancel_scope_bind_task" => {
            if instruction.args.len() != 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "cancel_scope_bind_task expects scope handle and task handle",
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            for operand in &instruction.args {
                match resolve_operand_type(operand, env) {
                    Some(found) if is_u64_type(&found) => {}
                    Some(found) => {
                        diagnostics.push(
                            Diagnostic::new(
                                "typecheck",
                                node.to_string(),
                                "RUNTIME_ARG_TYPE",
                                "cancel_scope_bind_task expects (u64, u64)",
                            )
                            .with_expected("u64")
                            .with_observed(found.to_string()),
                        );
                        return None;
                    }
                    None => {
                        diagnostics.push(
                            Diagnostic::new(
                                "typecheck",
                                node.to_string(),
                                "UNKNOWN_VALUE",
                                format!("unknown value {operand}"),
                            )
                            .with_observed(operand.clone()),
                        );
                        return None;
                    }
                }
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "RUNTIME_RESULT_TYPE",
                        "cancel_scope_bind_task result type must be b1",
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            Some(TypeRef::Bool)
        }
        "rt_join_u32"
        | "chan_recv_u32"
        | "chan_len"
        | "deadline_remaining_ms"
        | "retry_next_delay_ms"
        | "circuit_state"
        | "rt_inflight" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!("{} expects one handle operand", instruction.op),
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64,
                }) => {}
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "RUNTIME_ARG_TYPE",
                            format!("{} expects u64 handle operand", instruction.op),
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    return None;
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_VALUE",
                            format!("unknown value {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    return None;
                }
            }
            match &instruction.ty {
                TypeRef::Int {
                    signed: false,
                    bits: 32,
                } => Some(instruction.ty.clone()),
                found => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "RUNTIME_RESULT_TYPE",
                            format!("{} result type must be u32", instruction.op),
                        )
                        .with_expected("u32")
                        .with_observed(found.to_string()),
                    );
                    None
                }
            }
        }
        "rt_join_buf" | "chan_recv_buf" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!("{} expects one handle operand", instruction.op),
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(found) if is_u64_type(&found) => {}
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "RUNTIME_ARG_TYPE",
                            format!("{} expects u64 handle operand", instruction.op),
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    return None;
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_VALUE",
                            format!("unknown value {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    return None;
                }
            }
            if !is_buf_u8_like_type(&instruction.ty) {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "RUNTIME_RESULT_TYPE",
                        format!("{} result type must be buf[u8]-like", instruction.op),
                    )
                    .with_expected("buf[u8]-like")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            Some(instruction.ty.clone())
        }
        "rt_shutdown" => {
            if instruction.args.len() != 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "rt_shutdown expects runtime handle and grace-ms operand",
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let checks = [&instruction.args[0], &instruction.args[1]];
            let expected = [
                TypeRef::Int {
                    signed: false,
                    bits: 64,
                },
                TypeRef::Int {
                    signed: false,
                    bits: 32,
                },
            ];
            for (operand, expected_ty) in checks.into_iter().zip(expected.into_iter()) {
                match resolve_operand_type(operand, env) {
                    Some(found) if found == expected_ty => {}
                    Some(found) => {
                        diagnostics.push(
                            Diagnostic::new(
                                "typecheck",
                                node.to_string(),
                                "RUNTIME_ARG_TYPE",
                                "rt_shutdown operand types must be (u64, u32)",
                            )
                            .with_expected("u64, u32")
                            .with_observed(found.to_string()),
                        );
                        return None;
                    }
                    None => {
                        diagnostics.push(
                            Diagnostic::new(
                                "typecheck",
                                node.to_string(),
                                "UNKNOWN_VALUE",
                                format!("unknown value {operand}"),
                            )
                            .with_observed(operand.clone()),
                        );
                        return None;
                    }
                }
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "RUNTIME_RESULT_TYPE",
                        "rt_shutdown result type must be b1",
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            Some(TypeRef::Bool)
        }
        "rt_cancelled"
        | "deadline_expired"
        | "cancel_scope_cancelled"
        | "retry_record_failure"
        | "retry_record_success"
        | "retry_exhausted"
        | "circuit_allow"
        | "circuit_record_failure"
        | "circuit_record_success"
        | "backpressure_acquire"
        | "backpressure_release"
        | "backpressure_saturated"
        | "supervisor_record_failure"
        | "supervisor_record_recovery"
        | "supervisor_should_restart"
        | "supervisor_degraded" => {
            let expected_arity = match instruction.op.as_str() {
                "rt_cancelled" => 0,
                "supervisor_record_failure" => 2,
                _ => 1,
            };
            if instruction.args.len() != expected_arity {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!("{} expects {} operands", instruction.op, expected_arity),
                    )
                    .with_expected(expected_arity.to_string())
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            for (index, operand) in instruction.args.iter().enumerate() {
                match resolve_operand_type(operand, env) {
                    Some(found)
                        if (instruction.op == "supervisor_record_failure" && index == 1 && is_u32_type(&found))
                            || is_u64_type(&found) => {}
                    Some(found) => {
                        let expected = if instruction.op == "supervisor_record_failure" && index == 1 {
                            "u32"
                        } else {
                            "u64"
                        };
                        diagnostics.push(
                            Diagnostic::new(
                                "typecheck",
                                node.to_string(),
                                "RUNTIME_ARG_TYPE",
                                format!("{} expects {} operand", instruction.op, expected),
                            )
                            .with_expected(expected)
                            .with_observed(found.to_string()),
                        );
                        return None;
                    }
                    None => {
                        diagnostics.push(
                            Diagnostic::new(
                                "typecheck",
                                node.to_string(),
                                "UNKNOWN_VALUE",
                                format!("unknown value {operand}"),
                            )
                            .with_observed(operand.clone()),
                        );
                        return None;
                    }
                }
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "RUNTIME_RESULT_TYPE",
                        format!("{} result type must be b1", instruction.op),
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            Some(TypeRef::Bool)
        }
        "cancel_scope_open" => {
            if !instruction.args.is_empty() {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "cancel_scope_open expects 0 operands",
                    )
                    .with_expected("0")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if !is_u64_type(&instruction.ty) {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "RUNTIME_RESULT_TYPE",
                        "cancel_scope_open result type must be u64",
                    )
                    .with_expected("u64")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            Some(instruction.ty.clone())
        }
        "chan_open_u32" | "chan_open_buf" | "deadline_open_ms" | "cancel_scope_child" | "backpressure_open" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!("{} expects one operand", instruction.op),
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(found) if is_u64_type(&found) && instruction.op == "cancel_scope_child" => {}
                Some(found) if is_u32_type(&found) && instruction.op != "cancel_scope_child" => {}
                Some(found) => {
                    let message = if instruction.op == "cancel_scope_child" {
                        "cancel_scope_child expects u64 parent scope handle".to_string()
                    } else {
                        format!("{} expects u32 operand", instruction.op)
                    };
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "RUNTIME_ARG_TYPE",
                            message,
                        )
                        .with_expected(if instruction.op == "cancel_scope_child" { "u64" } else { "u32" })
                        .with_observed(found.to_string()),
                    );
                    return None;
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_VALUE",
                            format!("unknown value {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    return None;
                }
            }
            if !is_u64_type(&instruction.ty) {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "RUNTIME_RESULT_TYPE",
                        format!("{} result type must be u64", instruction.op),
                    )
                    .with_expected("u64")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            Some(instruction.ty.clone())
        }
        "retry_open" | "circuit_open" | "supervisor_open" => {
            if instruction.args.len() != 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!("{} expects two u32 operands", instruction.op),
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            for operand in &instruction.args {
                match resolve_operand_type(operand, env) {
                    Some(found) if is_u32_type(&found) => {}
                    Some(found) => {
                        diagnostics.push(
                            Diagnostic::new(
                                "typecheck",
                                node.to_string(),
                                "RUNTIME_ARG_TYPE",
                                format!("{} expects u32 operands", instruction.op),
                            )
                            .with_expected("u32")
                            .with_observed(found.to_string()),
                        );
                        return None;
                    }
                    None => {
                        diagnostics.push(
                            Diagnostic::new(
                                "typecheck",
                                node.to_string(),
                                "UNKNOWN_VALUE",
                                format!("unknown value {operand}"),
                            )
                            .with_observed(operand.clone()),
                        );
                        return None;
                    }
                }
            }
            if !is_u64_type(&instruction.ty) {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "RUNTIME_RESULT_TYPE",
                        format!("{} result type must be u64", instruction.op),
                    )
                    .with_expected("u64")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            Some(instruction.ty.clone())
        }
        "chan_send_u32" => {
            if instruction.args.len() != 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "chan_send_u32 expects channel handle and u32 value",
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = [
                TypeRef::Int {
                    signed: false,
                    bits: 64,
                },
                TypeRef::Int {
                    signed: false,
                    bits: 32,
                },
            ];
            for (operand, expected_ty) in instruction.args.iter().zip(expected.iter()) {
                match resolve_operand_type(operand, env) {
                    Some(found) if &found == expected_ty => {}
                    Some(found) => {
                        diagnostics.push(
                            Diagnostic::new(
                                "typecheck",
                                node.to_string(),
                                "RUNTIME_ARG_TYPE",
                                "chan_send_u32 operand types must be (u64, u32)",
                            )
                            .with_expected("u64, u32")
                            .with_observed(found.to_string()),
                        );
                        return None;
                    }
                    None => {
                        diagnostics.push(
                            Diagnostic::new(
                                "typecheck",
                                node.to_string(),
                                "UNKNOWN_VALUE",
                                format!("unknown value {operand}"),
                            )
                            .with_observed(operand.clone()),
                        );
                        return None;
                    }
                }
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "RUNTIME_RESULT_TYPE",
                        "chan_send_u32 result type must be b1",
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            Some(TypeRef::Bool)
        }
        "chan_send_buf" => {
            if instruction.args.len() != 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "chan_send_buf expects channel handle and buf[u8] payload",
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(found) if is_u64_type(&found) => {}
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "RUNTIME_ARG_TYPE",
                            "chan_send_buf expects u64 channel handle as first operand",
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    return None;
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_VALUE",
                            format!("unknown value {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    return None;
                }
            }
            match resolve_operand_type(&instruction.args[1], env) {
                Some(found) if is_buf_u8_like_type(&found) => {}
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "RUNTIME_ARG_TYPE",
                            "chan_send_buf expects buf[u8]-like payload",
                        )
                        .with_expected("buf[u8]-like")
                        .with_observed(found.to_string()),
                    );
                    return None;
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_VALUE",
                            format!("unknown value {}", instruction.args[1]),
                        )
                        .with_observed(instruction.args[1].clone()),
                    );
                    return None;
                }
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "RUNTIME_RESULT_TYPE",
                        "chan_send_buf result type must be b1",
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            Some(TypeRef::Bool)
        }
        "task_open" => {
            if instruction.args.is_empty() {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "task_open expects at least one command token",
                    )
                    .with_expected(">=1")
                    .with_observed("0"),
                );
                return None;
            }
            if validate_spawn_invocation_tokens(&instruction.args).is_err() {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "TASK_COMMAND_INVALID",
                        "task_open requires a valid command plus optional argv/env tokens",
                    )
                    .with_observed(instruction.args.join(" ")),
                );
                return None;
            }
            match &instruction.ty {
                TypeRef::Int {
                    signed: false,
                    bits: 64,
                } => Some(instruction.ty.clone()),
                found => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "TASK_RESULT_TYPE",
                            "task_open result type must be u64",
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    None
                }
            }
        }
        "task_done" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "task_done expects one task handle operand",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64,
                }) => {}
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "TASK_ARG_TYPE",
                            "task_done expects u64 handle operand",
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    return None;
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_VALUE",
                            format!("unknown value {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    return None;
                }
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "TASK_RESULT_TYPE",
                        "task_done result type must be b1",
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            Some(TypeRef::Bool)
        }
        "task_join" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "task_join expects one task handle operand",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64,
                }) => {}
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "TASK_ARG_TYPE",
                            "task_join expects u64 handle operand",
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    return None;
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_VALUE",
                            format!("unknown value {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    return None;
                }
            }
            match &instruction.ty {
                TypeRef::Int {
                    signed: true,
                    bits: 32,
                } => Some(instruction.ty.clone()),
                found => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "TASK_RESULT_TYPE",
                            "task_join result type must be i32",
                        )
                        .with_expected("i32")
                        .with_observed(found.to_string()),
                    );
                    None
                }
            }
        }
        "task_stdout_all" | "task_stderr_all" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!("{} expects one task handle operand", instruction.op),
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64,
                }) => {}
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "TASK_ARG_TYPE",
                            format!("{} expects u64 handle operand", instruction.op),
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    return None;
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_VALUE",
                            format!("unknown value {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    return None;
                }
            }
            let expected = TypeRef::Own(Box::new(TypeRef::Buf(Box::new(TypeRef::Int {
                signed: false,
                bits: 8,
            }))));
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "TASK_RESULT_TYPE",
                        format!("{} result type must be own[buf[u8]]", instruction.op),
                    )
                    .with_expected("own[buf[u8]]")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            Some(expected)
        }
        "task_close" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "task_close expects one task handle operand",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64,
                }) => {}
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "TASK_ARG_TYPE",
                            "task_close expects u64 handle operand",
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    return None;
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_VALUE",
                            format!("unknown value {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    return None;
                }
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "TASK_RESULT_TYPE",
                        "task_close result type must be b1",
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            Some(TypeRef::Bool)
        }
        "rand_u32" => {
            if !instruction.args.is_empty() {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "rand_u32 expects 0 operands",
                    )
                    .with_expected("0")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            Some(TypeRef::Int {
                signed: false,
                bits: 32,
            })
        }
        "fs_read_u32" => {
            if !instruction.args.is_empty() {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "fs_read_u32 expects 0 operands",
                    )
                    .with_expected("0")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            match &instruction.ty {
                TypeRef::Int {
                    signed: false,
                    bits: 32,
                } => Some(TypeRef::Int {
                    signed: false,
                    bits: 32,
                }),
                found => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "FS_READ_RESULT_TYPE",
                            "fs_read_u32 result type must be u32",
                        )
                        .with_expected("u32")
                        .with_observed(found.to_string()),
                    );
                    None
                }
            }
        }
        "fs_read_all" => {
            if !instruction.args.is_empty() {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "fs_read_all expects 0 operands",
                    )
                    .with_expected("0")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            match &instruction.ty {
                TypeRef::Own(inner)
                    if matches!(
                        inner.as_ref(),
                        TypeRef::Buf(elem)
                            if **elem
                                == TypeRef::Int {
                                    signed: false,
                                    bits: 8,
                                }
                    ) =>
                {
                    Some(instruction.ty.clone())
                }
                found => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "FS_READ_RESULT_TYPE",
                            "fs_read_all result type must be own[buf[u8]]",
                        )
                        .with_expected("own[buf[u8]]")
                        .with_observed(found.to_string()),
                    );
                    None
                }
            }
        }
        "fs_write_u32" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "fs_write_u32 expects one u32 operand",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "FS_WRITE_RESULT_TYPE",
                        "fs_write_u32 result type must be b1",
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 32,
                }) => Some(TypeRef::Bool),
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "FS_WRITE_OPERAND_TYPE",
                            "fs_write_u32 expects u32",
                        )
                        .with_expected("u32")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_VALUE",
                            format!("unknown value {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "fs_write_all" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "fs_write_all expects one buf[u8] handle operand",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "FS_WRITE_RESULT_TYPE",
                        "fs_write_all result type must be b1",
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Own(inner))
                | Some(TypeRef::View(inner))
                | Some(TypeRef::Edit(inner))
                    if matches!(
                        inner.as_ref(),
                        TypeRef::Buf(elem)
                            if **elem
                                == TypeRef::Int {
                                    signed: false,
                                    bits: 8,
                                }
                    ) =>
                {
                    Some(TypeRef::Bool)
                }
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "FS_WRITE_OPERAND_TYPE",
                            "fs_write_all expects own/view/edit[buf[u8]]",
                        )
                        .with_expected("own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_VALUE",
                            format!("unknown value {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "ffi_call" => {
            if instruction.args.is_empty() {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "ffi_call expects a symbol name followed by zero or more scalar operands",
                    )
                    .with_expected(">=1")
                    .with_observed("0"),
                );
                return None;
            }
            let symbol = &instruction.args[0];
            if !is_valid_ffi_symbol(symbol) {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "FFI_SYMBOL_INVALID",
                        "ffi_call requires a valid C symbol name as the first operand",
                    )
                    .with_observed(symbol.clone()),
                );
                return None;
            }
            if !is_ffi_scalar_type(&instruction.ty) {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "FFI_RESULT_TYPE",
                        "ffi_call result type must be scalar or b1",
                    )
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            for operand in instruction.args.iter().skip(1) {
                match resolve_operand_type(operand, env) {
                    Some(found) if is_ffi_scalar_type(&found) => {}
                    Some(found) => {
                        diagnostics.push(
                            Diagnostic::new(
                                "typecheck",
                                node.to_string(),
                                "FFI_ARG_TYPE",
                                "ffi_call operands must be scalar or b1",
                            )
                            .with_observed(found.to_string()),
                        );
                        return None;
                    }
                    None => {
                        diagnostics.push(
                            Diagnostic::new(
                                "typecheck",
                                node.to_string(),
                                "UNKNOWN_VALUE",
                                format!("unknown value {operand}"),
                            )
                            .with_observed(operand.clone()),
                        );
                        return None;
                    }
                }
            }
            Some(instruction.ty.clone())
        }
        "ffi_call_cstr" => {
            if instruction.args.len() != 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "ffi_call_cstr expects a symbol name and one buf[u8] operand",
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let symbol = &instruction.args[0];
            if !is_valid_ffi_symbol(symbol) {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "FFI_SYMBOL_INVALID",
                        "ffi_call_cstr requires a valid C symbol name as the first operand",
                    )
                    .with_observed(symbol.clone()),
                );
                return None;
            }
            if !is_ffi_scalar_type(&instruction.ty) {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "FFI_RESULT_TYPE",
                        "ffi_call_cstr result type must be scalar or b1",
                    )
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[1], env) {
                Some(TypeRef::Own(inner))
                | Some(TypeRef::View(inner))
                | Some(TypeRef::Edit(inner))
                    if matches!(
                        inner.as_ref(),
                        TypeRef::Buf(elem)
                            if **elem
                                == TypeRef::Int {
                                    signed: false,
                                    bits: 8,
                                }
                    ) =>
                {
                    Some(instruction.ty.clone())
                }
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "FFI_ARG_TYPE",
                            "ffi_call_cstr expects own/view/edit[buf[u8]]",
                        )
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_VALUE",
                            format!("unknown value {}", instruction.args[1]),
                        )
                        .with_observed(instruction.args[1].clone()),
                    );
                    None
                }
            }
        }
        "ffi_open_lib" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "ffi_open_lib expects one library path token",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            match &instruction.ty {
                TypeRef::Int {
                    signed: false,
                    bits: 64,
                } => Some(instruction.ty.clone()),
                found => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "FFI_RESULT_TYPE",
                            "ffi_open_lib result type must be u64",
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    None
                }
            }
        }
        "ffi_close_lib" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "ffi_close_lib expects one library handle operand",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "FFI_RESULT_TYPE",
                        "ffi_close_lib result type must be b1",
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64,
                }) => Some(TypeRef::Bool),
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "FFI_ARG_TYPE",
                            "ffi_close_lib expects u64 handle operand",
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_VALUE",
                            format!("unknown value {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "ffi_buf_ptr" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "ffi_buf_ptr expects one buf[u8] operand",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty
                != (TypeRef::Int {
                    signed: false,
                    bits: 64,
                })
            {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "FFI_RESULT_TYPE",
                        "ffi_buf_ptr result type must be u64",
                    )
                    .with_expected("u64")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Own(inner))
                | Some(TypeRef::View(inner))
                | Some(TypeRef::Edit(inner))
                    if matches!(
                        inner.as_ref(),
                        TypeRef::Buf(elem)
                            if **elem
                                == TypeRef::Int {
                                    signed: false,
                                    bits: 8,
                                }
                    ) =>
                {
                    Some(instruction.ty.clone())
                }
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "FFI_ARG_TYPE",
                            "ffi_buf_ptr expects own/view/edit[buf[u8]]",
                        )
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_VALUE",
                            format!("unknown value {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "ffi_call_lib" => {
            if instruction.args.len() < 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "ffi_call_lib expects a u64 library handle, a symbol name, and zero or more scalar operands",
                    )
                    .with_expected(">=2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64,
                }) => {}
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "FFI_ARG_TYPE",
                            "ffi_call_lib expects a u64 library handle as the first operand",
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    return None;
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_VALUE",
                            format!("unknown value {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    return None;
                }
            }
            let symbol = &instruction.args[1];
            if !is_valid_ffi_symbol(symbol) {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "FFI_SYMBOL_INVALID",
                        "ffi_call_lib requires a valid C symbol name as the second operand",
                    )
                    .with_observed(symbol.clone()),
                );
                return None;
            }
            if !is_ffi_scalar_type(&instruction.ty) {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "FFI_RESULT_TYPE",
                        "ffi_call_lib result type must be scalar or b1",
                    )
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            for operand in instruction.args.iter().skip(2) {
                match resolve_operand_type(operand, env) {
                    Some(found) if is_ffi_scalar_type(&found) => {}
                    Some(found) => {
                        diagnostics.push(
                            Diagnostic::new(
                                "typecheck",
                                node.to_string(),
                                "FFI_ARG_TYPE",
                                "ffi_call_lib operands must be scalar or b1",
                            )
                            .with_observed(found.to_string()),
                        );
                        return None;
                    }
                    None => {
                        diagnostics.push(
                            Diagnostic::new(
                                "typecheck",
                                node.to_string(),
                                "UNKNOWN_VALUE",
                                format!("unknown value {operand}"),
                            )
                            .with_observed(operand.clone()),
                        );
                        return None;
                    }
                }
            }
            Some(instruction.ty.clone())
        }
        "ffi_call_lib_cstr" => {
            if instruction.args.len() != 3 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "ffi_call_lib_cstr expects a u64 library handle, a symbol name, and one buf[u8] operand",
                    )
                    .with_expected("3")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64,
                }) => {}
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "FFI_ARG_TYPE",
                            "ffi_call_lib_cstr expects a u64 library handle as the first operand",
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    return None;
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_VALUE",
                            format!("unknown value {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    return None;
                }
            }
            let symbol = &instruction.args[1];
            if !is_valid_ffi_symbol(symbol) {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "FFI_SYMBOL_INVALID",
                        "ffi_call_lib_cstr requires a valid C symbol name as the second operand",
                    )
                    .with_observed(symbol.clone()),
                );
                return None;
            }
            if !is_ffi_scalar_type(&instruction.ty) {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "FFI_RESULT_TYPE",
                        "ffi_call_lib_cstr result type must be scalar or b1",
                    )
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[2], env) {
                Some(TypeRef::Own(inner))
                | Some(TypeRef::View(inner))
                | Some(TypeRef::Edit(inner))
                    if matches!(
                        inner.as_ref(),
                        TypeRef::Buf(elem)
                            if **elem
                                == TypeRef::Int {
                                    signed: false,
                                    bits: 8,
                                }
                    ) =>
                {
                    Some(instruction.ty.clone())
                }
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "FFI_ARG_TYPE",
                            "ffi_call_lib_cstr expects own/view/edit[buf[u8]] as the third operand",
                        )
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_VALUE",
                            format!("unknown value {}", instruction.args[2]),
                        )
                        .with_observed(instruction.args[2].clone()),
                    );
                    None
                }
            }
        }
        "net_listen" => {
            if !instruction.args.is_empty() {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "net_listen expects 0 operands",
                    )
                    .with_expected("0")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            match &instruction.ty {
                TypeRef::Int {
                    signed: false,
                    bits: 64,
                } => Some(instruction.ty.clone()),
                found => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "NET_RESULT_TYPE",
                            "net_listen result type must be u64",
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    None
                }
            }
        }
        "tls_listen" => {
            if !instruction.args.is_empty() {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "tls_listen expects 0 operands",
                    )
                    .with_expected("0")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            match &instruction.ty {
                TypeRef::Int {
                    signed: false,
                    bits: 64,
                } => Some(instruction.ty.clone()),
                found => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "TLS_RESULT_TYPE",
                            "tls_listen result type must be u64",
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    None
                }
            }
        }
        "net_accept" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "net_accept expects one listener handle operand",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64,
                }) => {}
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "NET_OPERAND_TYPE",
                            "net_accept expects a u64 listener handle",
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    return None;
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown net_accept operand {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    return None;
                }
            }
            if instruction.ty
                != (TypeRef::Int {
                    signed: false,
                    bits: 64,
                })
            {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "NET_RESULT_TYPE",
                        "net_accept result type must be u64",
                    )
                    .with_expected("u64")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            Some(instruction.ty.clone())
        }
        "net_session_open" => {
            if !instruction.args.is_empty() {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "net_session_open expects 0 operands",
                    )
                    .with_expected("0")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if !is_u64_type(&instruction.ty) {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "NET_RESULT_TYPE",
                        "net_session_open result type must be u64",
                    )
                    .with_expected("u64")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            Some(instruction.ty.clone())
        }
        "http_session_accept" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "http_session_accept expects one listener handle operand",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64,
                }) => {}
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "HTTP_OPERAND_TYPE",
                            "http_session_accept expects a u64 listener handle",
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    return None;
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!(
                                "unknown http_session_accept operand {}",
                                instruction.args[0]
                            ),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    return None;
                }
            }
            if instruction.ty
                != (TypeRef::Int {
                    signed: false,
                    bits: 64,
                })
            {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "HTTP_RESULT_TYPE",
                        "http_session_accept result type must be u64",
                    )
                    .with_expected("u64")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            Some(instruction.ty.clone())
        }
        "net_read_all" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "net_read_all expects one connection handle operand",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64,
                }) => {}
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "NET_OPERAND_TYPE",
                            "net_read_all expects a u64 connection handle",
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    return None;
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown net_read_all operand {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    return None;
                }
            }
            let expected = TypeRef::Own(Box::new(TypeRef::Buf(Box::new(TypeRef::Int {
                signed: false,
                bits: 8,
            }))));
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "NET_EXCHANGE_RESULT_TYPE",
                        "net_read_all result type must be own[buf[u8]]",
                    )
                    .with_expected("own[buf[u8]]")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            Some(expected)
        }
        "session_read_chunk" => {
            if instruction.args.len() != 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "session_read_chunk expects a session handle and u32 chunk size",
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64,
                }) => {}
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "NET_OPERAND_TYPE",
                            "session_read_chunk expects a u64 session handle",
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    return None;
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown session_read_chunk operand {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    return None;
                }
            }
            match resolve_operand_type(&instruction.args[1], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 32,
                }) => {}
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "NET_OPERAND_TYPE",
                            "session_read_chunk expects a u32 chunk size",
                        )
                        .with_expected("u32")
                        .with_observed(found.to_string()),
                    );
                    return None;
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown session_read_chunk operand {}", instruction.args[1]),
                        )
                        .with_observed(instruction.args[1].clone()),
                    );
                    return None;
                }
            }
            let expected = TypeRef::Own(Box::new(TypeRef::Buf(Box::new(TypeRef::Int {
                signed: false,
                bits: 8,
            }))));
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "NET_RESULT_TYPE",
                        "session_read_chunk result type must be own[buf[u8]]",
                    )
                    .with_expected("own[buf[u8]]")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            Some(expected)
        }
        "http_session_request" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "http_session_request expects one session handle operand",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64,
                }) => {}
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "HTTP_OPERAND_TYPE",
                            "http_session_request expects a u64 session handle",
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    return None;
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!(
                                "unknown http_session_request operand {}",
                                instruction.args[0]
                            ),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    return None;
                }
            }
            let expected = TypeRef::Own(Box::new(TypeRef::Buf(Box::new(TypeRef::Int {
                signed: false,
                bits: 8,
            }))));
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "HTTP_RESULT_TYPE",
                        "http_session_request result type must be own[buf[u8]]",
                    )
                    .with_expected("own[buf[u8]]")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            Some(expected)
        }
        "net_write_handle_all" => {
            if instruction.args.len() != 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "net_write_handle_all expects a connection handle and one buf[u8] operand",
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "NET_WRITE_RESULT_TYPE",
                        "net_write_handle_all result type must be b1",
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64,
                }) => {}
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "NET_OPERAND_TYPE",
                            "net_write_handle_all expects a u64 connection handle as the first operand",
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    return None;
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!(
                                "unknown net_write_handle_all operand {}",
                                instruction.args[0]
                            ),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    return None;
                }
            }
            match resolve_operand_type(&instruction.args[1], env) {
                Some(TypeRef::Own(inner))
                | Some(TypeRef::View(inner))
                | Some(TypeRef::Edit(inner))
                    if matches!(
                        inner.as_ref(),
                        TypeRef::Buf(elem)
                            if **elem
                                == TypeRef::Int {
                                    signed: false,
                                    bits: 8,
                                }
                    ) =>
                {
                    Some(TypeRef::Bool)
                }
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "NET_OPERAND_TYPE",
                            "net_write_handle_all expects own/view/edit[buf[u8]] as the second operand",
                        )
                        .with_expected("own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!(
                                "unknown net_write_handle_all operand {}",
                                instruction.args[1]
                            ),
                        )
                        .with_observed(instruction.args[1].clone()),
                    );
                    None
                }
            }
        }
        "session_write_chunk" | "session_heartbeat" => {
            if instruction.args.len() != 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!(
                            "{} expects a session handle and one buf[u8] operand",
                            instruction.op
                        ),
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "NET_RESULT_TYPE",
                        format!("{} result type must be b1", instruction.op),
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64,
                }) => {}
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "NET_OPERAND_TYPE",
                            format!("{} expects a u64 session handle", instruction.op),
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    return None;
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown {} operand {}", instruction.op, instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    return None;
                }
            }
            match resolve_operand_type(&instruction.args[1], env) {
                Some(TypeRef::Own(inner))
                | Some(TypeRef::View(inner))
                | Some(TypeRef::Edit(inner))
                    if matches!(
                        inner.as_ref(),
                        TypeRef::Buf(elem)
                            if **elem
                                == TypeRef::Int {
                                    signed: false,
                                    bits: 8,
                                }
                    ) =>
                {
                    Some(TypeRef::Bool)
                }
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "NET_OPERAND_TYPE",
                            format!(
                                "{} expects own/view/edit[buf[u8]] as the second operand",
                                instruction.op
                            ),
                        )
                        .with_expected("own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown {} operand {}", instruction.op, instruction.args[1]),
                        )
                        .with_observed(instruction.args[1].clone()),
                    );
                    None
                }
            }
        }
        "session_flush" | "session_alive" | "session_reconnect" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!("{} expects one session handle operand", instruction.op),
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "NET_RESULT_TYPE",
                        format!("{} result type must be b1", instruction.op),
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64,
                }) => Some(TypeRef::Bool),
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "NET_OPERAND_TYPE",
                            format!("{} expects a u64 session handle", instruction.op),
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown {} operand {}", instruction.op, instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "session_backpressure" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "session_backpressure expects one session handle operand",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if !is_u32_type(&instruction.ty) {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "NET_RESULT_TYPE",
                        "session_backpressure result type must be u32",
                    )
                    .with_expected("u32")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64,
                }) => Some(instruction.ty.clone()),
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "NET_OPERAND_TYPE",
                            "session_backpressure expects a u64 session handle",
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown session_backpressure operand {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "session_backpressure_wait" => {
            if instruction.args.len() != 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "session_backpressure_wait expects a session handle and u32 max_pending",
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "NET_RESULT_TYPE",
                        "session_backpressure_wait result type must be b1",
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64,
                }) => {}
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "NET_OPERAND_TYPE",
                            "session_backpressure_wait expects a u64 session handle",
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    return None;
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!(
                                "unknown session_backpressure_wait operand {}",
                                instruction.args[0]
                            ),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    return None;
                }
            }
            match resolve_operand_type(&instruction.args[1], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 32,
                }) => Some(TypeRef::Bool),
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "NET_OPERAND_TYPE",
                            "session_backpressure_wait expects a u32 max_pending",
                        )
                        .with_expected("u32")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!(
                                "unknown session_backpressure_wait operand {}",
                                instruction.args[1]
                            ),
                        )
                        .with_observed(instruction.args[1].clone()),
                    );
                    None
                }
            }
        }
        "session_resume_id" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "session_resume_id expects one session handle operand",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if !is_u64_type(&instruction.ty) {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "NET_RESULT_TYPE",
                        "session_resume_id result type must be u64",
                    )
                    .with_expected("u64")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64,
                }) => Some(instruction.ty.clone()),
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "NET_OPERAND_TYPE",
                            "session_resume_id expects a u64 session handle",
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown session_resume_id operand {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "net_close" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "net_close expects one handle operand",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "NET_RESULT_TYPE",
                        "net_close result type must be b1",
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64,
                }) => Some(TypeRef::Bool),
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "NET_OPERAND_TYPE",
                            "net_close expects u64 handle operand",
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown net_close operand {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "http_session_close" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "http_session_close expects one session handle operand",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "HTTP_RESULT_TYPE",
                        "http_session_close result type must be b1",
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64,
                }) => Some(TypeRef::Bool),
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "HTTP_OPERAND_TYPE",
                            "http_session_close expects u64 session handle operand",
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown http_session_close operand {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "listener_set_timeout_ms" | "session_set_timeout_ms" | "listener_set_shutdown_grace_ms" => {
            if instruction.args.len() != 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!(
                            "{} expects a handle operand and one u32 timeout value",
                            instruction.op
                        ),
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "NET_RESULT_TYPE",
                        format!("{} result type must be b1", instruction.op),
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64,
                }) => {}
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "NET_OPERAND_TYPE",
                            format!("{} expects u64 handle as the first operand", instruction.op),
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    return None;
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown {} operand {}", instruction.op, instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    return None;
                }
            }
            match resolve_operand_type(&instruction.args[1], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 32,
                }) => Some(TypeRef::Bool),
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "NET_OPERAND_TYPE",
                            format!(
                                "{} expects u32 timeout/grace as the second operand",
                                instruction.op
                            ),
                        )
                        .with_expected("u32")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown {} operand {}", instruction.op, instruction.args[1]),
                        )
                        .with_observed(instruction.args[1].clone()),
                    );
                    None
                }
            }
        }
        "http_method_eq" | "http_path_eq" => {
            if instruction.args.len() != 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!(
                            "{} expects a request buf and a literal token",
                            instruction.op
                        ),
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "HTTP_RESULT_TYPE",
                        format!("{} result type must be b1", instruction.op),
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Own(inner))
                | Some(TypeRef::View(inner))
                | Some(TypeRef::Edit(inner))
                    if matches!(
                        inner.as_ref(),
                        TypeRef::Buf(elem)
                            if **elem
                                == TypeRef::Int {
                                    signed: false,
                                    bits: 8,
                                }
                    ) =>
                {
                    Some(TypeRef::Bool)
                }
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "HTTP_OPERAND_TYPE",
                            format!(
                                "{} expects own/view/edit[buf[u8]] as the first operand",
                                instruction.op
                            ),
                        )
                        .with_expected("own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown {} operand {}", instruction.op, instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "http_request_method" | "http_request_path" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!("{} expects one request buf operand", instruction.op),
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Own(Box::new(TypeRef::Buf(Box::new(TypeRef::Int {
                signed: false,
                bits: 8,
            }))));
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "HTTP_RESULT_TYPE",
                        format!("{} result type must be own[buf[u8]]", instruction.op),
                    )
                    .with_expected("own[buf[u8]]")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Own(inner))
                | Some(TypeRef::View(inner))
                | Some(TypeRef::Edit(inner))
                    if matches!(
                        inner.as_ref(),
                        TypeRef::Buf(elem)
                            if **elem
                                == TypeRef::Int {
                                    signed: false,
                                    bits: 8,
                                }
                    ) =>
                {
                    Some(expected)
                }
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "HTTP_OPERAND_TYPE",
                            format!(
                                "{} expects own/view/edit[buf[u8]] as the first operand",
                                instruction.op
                            ),
                        )
                        .with_expected("own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown {} operand {}", instruction.op, instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "http_route_param" => {
            if instruction.args.len() != 3 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "http_route_param expects a request buf plus pattern/param literal tokens",
                    )
                    .with_expected("3")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Own(Box::new(TypeRef::Buf(Box::new(TypeRef::Int {
                signed: false,
                bits: 8,
            }))));
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "HTTP_RESULT_TYPE",
                        "http_route_param result type must be own[buf[u8]]",
                    )
                    .with_expected("own[buf[u8]]")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Own(inner))
                | Some(TypeRef::View(inner))
                | Some(TypeRef::Edit(inner))
                    if matches!(
                        inner.as_ref(),
                        TypeRef::Buf(elem)
                            if **elem
                                == TypeRef::Int {
                                    signed: false,
                                    bits: 8,
                                }
                    ) =>
                {
                    Some(expected)
                }
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "HTTP_OPERAND_TYPE",
                            "http_route_param expects own/view/edit[buf[u8]] as the first operand",
                        )
                        .with_expected("own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown http_route_param operand {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "http_header_eq" => {
            if instruction.args.len() != 3 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "http_header_eq expects a request buf plus name/value literal tokens",
                    )
                    .with_expected("3")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "HTTP_RESULT_TYPE",
                        "http_header_eq result type must be b1",
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Own(inner))
                | Some(TypeRef::View(inner))
                | Some(TypeRef::Edit(inner))
                    if matches!(
                        inner.as_ref(),
                        TypeRef::Buf(elem)
                            if **elem
                                == TypeRef::Int {
                                    signed: false,
                                    bits: 8,
                                }
                    ) =>
                {
                    Some(TypeRef::Bool)
                }
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "HTTP_OPERAND_TYPE",
                            "http_header_eq expects own/view/edit[buf[u8]] as the first operand",
                        )
                        .with_expected("own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown http_header_eq operand {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "http_cookie_eq" => {
            if instruction.args.len() != 3 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "http_cookie_eq expects a request buf plus name/value literal tokens",
                    )
                    .with_expected("3")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "HTTP_RESULT_TYPE",
                        "http_cookie_eq result type must be b1",
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Own(inner))
                | Some(TypeRef::View(inner))
                | Some(TypeRef::Edit(inner))
                    if matches!(
                        inner.as_ref(),
                        TypeRef::Buf(elem)
                            if **elem
                                == TypeRef::Int {
                                    signed: false,
                                    bits: 8,
                                }
                    ) =>
                {
                    Some(TypeRef::Bool)
                }
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "HTTP_OPERAND_TYPE",
                            "http_cookie_eq expects own/view/edit[buf[u8]] as the first operand",
                        )
                        .with_expected("own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown http_cookie_eq operand {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "http_status_u32" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "http_status_u32 expects one response buf operand",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty
                != (TypeRef::Int {
                    signed: false,
                    bits: 32,
                })
            {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "HTTP_RESULT_TYPE",
                        "http_status_u32 result type must be u32",
                    )
                    .with_expected("u32")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(found) if is_buf_u8_like_type(&found) || is_string_like_type(&found) => {
                    Some(TypeRef::Int {
                        signed: false,
                        bits: 32,
                    })
                }
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "HTTP_OPERAND_TYPE",
                            "http_status_u32 expects own/view/edit[buf[u8]] as the first operand",
                        )
                        .with_expected("own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown http_status_u32 operand {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "buf_eq_lit" => {
            if instruction.args.len() != 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "buf_eq_lit expects a buf operand and a literal token",
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "BUF_RESULT_TYPE",
                        "buf_eq_lit result type must be b1",
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Own(inner))
                | Some(TypeRef::View(inner))
                | Some(TypeRef::Edit(inner))
                    if matches!(
                        inner.as_ref(),
                        TypeRef::Buf(elem)
                            if **elem
                                == TypeRef::Int {
                                    signed: false,
                                    bits: 8,
                                }
                    ) =>
                {
                    Some(TypeRef::Bool)
                }
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "BUF_OPERAND_TYPE",
                            "buf_eq_lit expects own/view/edit[buf[u8]] as the first operand",
                        )
                        .with_expected("own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown buf_eq_lit operand {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "buf_contains_lit" => {
            if instruction.args.len() != 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "buf_contains_lit expects a buf operand and a literal token",
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "BUF_RESULT_TYPE",
                        "buf_contains_lit result type must be b1",
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Own(inner))
                | Some(TypeRef::View(inner))
                | Some(TypeRef::Edit(inner))
                    if matches!(
                        inner.as_ref(),
                        TypeRef::Buf(elem)
                            if **elem
                                == TypeRef::Int {
                                    signed: false,
                                    bits: 8,
                                }
                    ) =>
                {
                    Some(TypeRef::Bool)
                }
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "BUF_OPERAND_TYPE",
                            "buf_contains_lit expects own/view/edit[buf[u8]] as the first operand",
                        )
                        .with_expected("own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown buf_contains_lit operand {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "buf_lit" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "buf_lit expects one literal token",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Own(Box::new(TypeRef::Buf(Box::new(TypeRef::Int {
                signed: false,
                bits: 8,
            }))));
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "BUF_RESULT_TYPE",
                        "buf_lit result type must be own[buf[u8]]",
                    )
                    .with_expected("own[buf[u8]]")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            Some(expected)
        }
        "buf_concat" => {
            if instruction.args.len() != 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "buf_concat expects two buf operands",
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Own(Box::new(TypeRef::Buf(Box::new(TypeRef::Int {
                signed: false,
                bits: 8,
            }))));
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "BUF_RESULT_TYPE",
                        "buf_concat result type must be own[buf[u8]]",
                    )
                    .with_expected("own[buf[u8]]")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            for index in 0..2 {
                match resolve_operand_type(&instruction.args[index], env) {
                    Some(TypeRef::Own(inner))
                    | Some(TypeRef::View(inner))
                    | Some(TypeRef::Edit(inner))
                        if matches!(
                            inner.as_ref(),
                            TypeRef::Buf(elem)
                                if **elem
                                    == TypeRef::Int {
                                        signed: false,
                                        bits: 8,
                                    }
                        ) => {}
                    Some(found) => {
                        diagnostics.push(
                            Diagnostic::new(
                                "typecheck",
                                node.to_string(),
                                "BUF_OPERAND_TYPE",
                                "buf_concat expects own/view/edit[buf[u8]] operands",
                            )
                            .with_expected("own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                            .with_observed(found.to_string()),
                        );
                        return None;
                    }
                    None => {
                        diagnostics.push(
                            Diagnostic::new(
                                "typecheck",
                                node.to_string(),
                                "UNKNOWN_OPERAND",
                                format!("unknown buf_concat operand {}", instruction.args[index]),
                            )
                            .with_observed(instruction.args[index].clone()),
                        );
                        return None;
                    }
                }
            }
            Some(expected)
        }
        "http_header" => {
            if instruction.args.len() != 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "http_header expects a request buf plus a name literal token",
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Own(Box::new(TypeRef::Buf(Box::new(TypeRef::Int {
                signed: false,
                bits: 8,
            }))));
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "HTTP_RESULT_TYPE",
                        "http_header result type must be own[buf[u8]]",
                    )
                    .with_expected("own[buf[u8]]")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Own(inner))
                | Some(TypeRef::View(inner))
                | Some(TypeRef::Edit(inner))
                    if matches!(
                        inner.as_ref(),
                        TypeRef::Buf(elem)
                            if **elem
                                == TypeRef::Int {
                                    signed: false,
                                    bits: 8,
                                }
                    ) =>
                {
                    Some(expected)
                }
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "HTTP_OPERAND_TYPE",
                            "http_header expects own/view/edit[buf[u8]] as the first operand",
                        )
                        .with_expected("own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown http_header operand {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "http_header_count" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "http_header_count expects one request buf operand",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Int {
                signed: false,
                bits: 32,
            };
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "HTTP_RESULT_TYPE",
                        "http_header_count result type must be u32",
                    )
                    .with_expected("u32")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(found)
                    if view_source_matches(
                        &found,
                        &TypeRef::Buf(Box::new(TypeRef::Int {
                            signed: false,
                            bits: 8,
                        })),
                    ) =>
                {
                    Some(expected)
                }
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "HTTP_OPERAND_TYPE",
                            "http_header_count expects own/view/edit[buf[u8]]",
                        )
                        .with_expected("own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown http_header_count operand {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "http_header_name" | "http_header_value" => {
            if instruction.args.len() != 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!("{} expects request buf and u32 index", instruction.op),
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Own(Box::new(TypeRef::Buf(Box::new(TypeRef::Int {
                signed: false,
                bits: 8,
            }))));
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "HTTP_RESULT_TYPE",
                        format!("{} result type must be own[buf[u8]]", instruction.op),
                    )
                    .with_expected("own[buf[u8]]")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            let request_ok = match resolve_operand_type(&instruction.args[0], env) {
                Some(found)
                    if view_source_matches(
                        &found,
                        &TypeRef::Buf(Box::new(TypeRef::Int {
                            signed: false,
                            bits: 8,
                        })),
                    ) =>
                {
                    true
                }
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "HTTP_OPERAND_TYPE",
                            format!("{} expects own/view/edit[buf[u8]] as the first operand", instruction.op),
                        )
                        .with_expected("own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                        .with_observed(found.to_string()),
                    );
                    false
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown {} operand {}", instruction.op, instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    false
                }
            };
            let index_ok = matches!(
                resolve_operand_type(&instruction.args[1], env),
                Some(TypeRef::Int {
                    signed: false,
                    bits: 32
                })
            );
            if !index_ok {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "HTTP_OPERAND_TYPE",
                        format!("{} expects u32 as the second operand", instruction.op),
                    )
                    .with_expected("u32")
                    .with_observed(
                        resolve_operand_type(&instruction.args[1], env)
                            .map(|ty| ty.to_string())
                            .unwrap_or_else(|| "unknown".to_string()),
                    ),
                );
            }
            if request_ok && index_ok {
                Some(expected)
            } else {
                None
            }
        }
        "http_cookie" => {
            if instruction.args.len() != 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "http_cookie expects a request buf plus a name literal token",
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Own(Box::new(TypeRef::Buf(Box::new(TypeRef::Int {
                signed: false,
                bits: 8,
            }))));
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "HTTP_RESULT_TYPE",
                        "http_cookie result type must be own[buf[u8]]",
                    )
                    .with_expected("own[buf[u8]]")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Own(inner))
                | Some(TypeRef::View(inner))
                | Some(TypeRef::Edit(inner))
                    if matches!(
                        inner.as_ref(),
                        TypeRef::Buf(elem)
                            if **elem
                                == TypeRef::Int {
                                    signed: false,
                                    bits: 8,
                                }
                    ) =>
                {
                    Some(expected)
                }
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "HTTP_OPERAND_TYPE",
                            "http_cookie expects own/view/edit[buf[u8]] as the first operand",
                        )
                        .with_expected("own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown http_cookie operand {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "http_query_param" => {
            if instruction.args.len() != 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "http_query_param expects a request buf plus a key literal token",
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Own(Box::new(TypeRef::Buf(Box::new(TypeRef::Int {
                signed: false,
                bits: 8,
            }))));
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "HTTP_RESULT_TYPE",
                        "http_query_param result type must be own[buf[u8]]",
                    )
                    .with_expected("own[buf[u8]]")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Own(inner))
                | Some(TypeRef::View(inner))
                | Some(TypeRef::Edit(inner))
                    if matches!(
                        inner.as_ref(),
                        TypeRef::Buf(elem)
                            if **elem
                                == TypeRef::Int {
                                    signed: false,
                                    bits: 8,
                                }
                    ) =>
                {
                    Some(expected)
                }
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "HTTP_OPERAND_TYPE",
                            "http_query_param expects own/view/edit[buf[u8]] as the first operand",
                        )
                        .with_expected("own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown http_query_param operand {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "http_body" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "http_body expects one request buf operand",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Own(Box::new(TypeRef::Buf(Box::new(TypeRef::Int {
                signed: false,
                bits: 8,
            }))));
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "HTTP_RESULT_TYPE",
                        "http_body result type must be own[buf[u8]]",
                    )
                    .with_expected("own[buf[u8]]")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Own(inner))
                | Some(TypeRef::View(inner))
                | Some(TypeRef::Edit(inner))
                    if matches!(
                        inner.as_ref(),
                        TypeRef::Buf(elem)
                            if **elem
                                == TypeRef::Int {
                                    signed: false,
                                    bits: 8,
                                }
                    ) =>
                {
                    Some(expected)
                }
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "HTTP_OPERAND_TYPE",
                            "http_body expects own/view/edit[buf[u8]] as the first operand",
                        )
                        .with_expected("own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown http_body operand {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "http_multipart_part_count" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "http_multipart_part_count expects one request buf operand",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Int {
                signed: false,
                bits: 32,
            };
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "HTTP_RESULT_TYPE",
                        "http_multipart_part_count result type must be u32",
                    )
                    .with_expected("u32")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(found)
                    if view_source_matches(
                        &found,
                        &TypeRef::Buf(Box::new(TypeRef::Int {
                            signed: false,
                            bits: 8,
                        })),
                    ) =>
                {
                    Some(expected)
                }
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "HTTP_OPERAND_TYPE",
                            "http_multipart_part_count expects own/view/edit[buf[u8]]",
                        )
                        .with_expected("own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!(
                                "unknown http_multipart_part_count operand {}",
                                instruction.args[0]
                            ),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "http_multipart_part_name" | "http_multipart_part_filename" | "http_multipart_part_body" => {
            if instruction.args.len() != 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!("{} expects request buf and u32 index", instruction.op),
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Own(Box::new(TypeRef::Buf(Box::new(TypeRef::Int {
                signed: false,
                bits: 8,
            }))));
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "HTTP_RESULT_TYPE",
                        format!("{} result type must be own[buf[u8]]", instruction.op),
                    )
                    .with_expected("own[buf[u8]]")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            let request_ok = match resolve_operand_type(&instruction.args[0], env) {
                Some(found)
                    if view_source_matches(
                        &found,
                        &TypeRef::Buf(Box::new(TypeRef::Int {
                            signed: false,
                            bits: 8,
                        })),
                    ) =>
                {
                    true
                }
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "HTTP_OPERAND_TYPE",
                            format!("{} expects own/view/edit[buf[u8]] as the first operand", instruction.op),
                        )
                        .with_expected("own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                        .with_observed(found.to_string()),
                    );
                    false
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown {} operand {}", instruction.op, instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    false
                }
            };
            let index_ok = matches!(
                resolve_operand_type(&instruction.args[1], env),
                Some(TypeRef::Int {
                    signed: false,
                    bits: 32
                })
            );
            if !index_ok {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "HTTP_OPERAND_TYPE",
                        format!("{} expects u32 as the second operand", instruction.op),
                    )
                    .with_expected("u32")
                    .with_observed(
                        resolve_operand_type(&instruction.args[1], env)
                            .map(|ty| ty.to_string())
                            .unwrap_or_else(|| "unknown".to_string()),
                    ),
                );
            }
            if request_ok && index_ok {
                Some(expected)
            } else {
                None
            }
        }
        "http_body_stream_open" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "http_body_stream_open expects one request buf operand",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Int {
                signed: false,
                bits: 64,
            };
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "HTTP_RESULT_TYPE",
                        "http_body_stream_open result type must be u64",
                    )
                    .with_expected("u64")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(found)
                    if view_source_matches(
                        &found,
                        &TypeRef::Buf(Box::new(TypeRef::Int {
                            signed: false,
                            bits: 8,
                        })),
                    ) =>
                {
                    Some(expected)
                }
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "HTTP_OPERAND_TYPE",
                            "http_body_stream_open expects own/view/edit[buf[u8]]",
                        )
                        .with_expected("own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!(
                                "unknown http_body_stream_open operand {}",
                                instruction.args[0]
                            ),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "http_body_stream_next" => {
            if instruction.args.len() != 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "http_body_stream_next expects handle and chunk size",
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Own(Box::new(TypeRef::Buf(Box::new(TypeRef::Int {
                signed: false,
                bits: 8,
            }))));
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "HTTP_RESULT_TYPE",
                        "http_body_stream_next result type must be own[buf[u8]]",
                    )
                    .with_expected("own[buf[u8]]")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            let handle_ok = matches!(
                resolve_operand_type(&instruction.args[0], env),
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64
                })
            );
            let size_ok = matches!(
                resolve_operand_type(&instruction.args[1], env),
                Some(TypeRef::Int {
                    signed: false,
                    bits: 32
                })
            );
            if !handle_ok || !size_ok {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "HTTP_OPERAND_TYPE",
                        "http_body_stream_next expects (u64, u32)",
                    )
                    .with_expected("u64, u32")
                    .with_observed(format!(
                        "{}, {}",
                        resolve_operand_type(&instruction.args[0], env)
                            .map(|ty| ty.to_string())
                            .unwrap_or_else(|| "unknown".to_string()),
                        resolve_operand_type(&instruction.args[1], env)
                            .map(|ty| ty.to_string())
                            .unwrap_or_else(|| "unknown".to_string())
                    )),
                );
                return None;
            }
            Some(expected)
        }
        "http_body_stream_close" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "http_body_stream_close expects one handle operand",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "HTTP_RESULT_TYPE",
                        "http_body_stream_close result type must be b1",
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64,
                }) => Some(TypeRef::Bool),
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "HTTP_OPERAND_TYPE",
                            "http_body_stream_close expects u64 handle operand",
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!(
                                "unknown http_body_stream_close operand {}",
                                instruction.args[0]
                            ),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "http_body_limit" => {
            if instruction.args.len() != 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "http_body_limit expects a request buf and a u32 limit",
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "HTTP_RESULT_TYPE",
                        "http_body_limit result type must be b1",
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Own(inner))
                | Some(TypeRef::View(inner))
                | Some(TypeRef::Edit(inner))
                    if matches!(
                        inner.as_ref(),
                        TypeRef::Buf(elem)
                            if **elem
                                == TypeRef::Int {
                                    signed: false,
                                    bits: 8,
                                }
                    ) => {}
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "HTTP_OPERAND_TYPE",
                            "http_body_limit expects own/view/edit[buf[u8]] as the first operand",
                        )
                        .with_expected("own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                        .with_observed(found.to_string()),
                    );
                    return None;
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown http_body_limit operand {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    return None;
                }
            }
            match resolve_operand_type(&instruction.args[1], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 32,
                }) => Some(TypeRef::Bool),
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "HTTP_OPERAND_TYPE",
                            "http_body_limit expects u32 as the second operand",
                        )
                        .with_expected("u32")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown http_body_limit operand {}", instruction.args[1]),
                        )
                        .with_observed(instruction.args[1].clone()),
                    );
                    None
                }
            }
        }
        "http_server_config_u32" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "http_server_config_u32 expects one config token",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Int {
                signed: false,
                bits: 32,
            };
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "HTTP_RESULT_TYPE",
                        "http_server_config_u32 result type must be u32",
                    )
                    .with_expected("u32")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            Some(expected)
        }
        "service_open" => {
            if !instruction.args.is_empty() {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "service_open expects 0 operands",
                    )
                    .with_expected("0")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Int {
                signed: false,
                bits: 64,
            };
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "SERVICE_RESULT_TYPE",
                        "service_open result type must be u64",
                    )
                    .with_expected("u64")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            Some(expected)
        }
        "service_close" | "service_trace_end" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!("{} expects one handle operand", instruction.op),
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "SERVICE_RESULT_TYPE",
                        format!("{} result type must be b1", instruction.op),
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64,
                }) => Some(TypeRef::Bool),
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "SERVICE_OPERAND_TYPE",
                            format!("{} expects u64 handle operand", instruction.op),
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown {} operand {}", instruction.op, instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "service_shutdown" => {
            if instruction.args.len() != 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "service_shutdown expects service handle and grace timeout",
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "SERVICE_RESULT_TYPE",
                        "service_shutdown result type must be b1",
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            let handle_ok = matches!(
                resolve_operand_type(&instruction.args[0], env),
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64
                })
            );
            let timeout_ok = matches!(
                resolve_operand_type(&instruction.args[1], env),
                Some(TypeRef::Int {
                    signed: false,
                    bits: 32
                })
            );
            if !handle_ok || !timeout_ok {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "SERVICE_OPERAND_TYPE",
                        "service_shutdown expects (u64, u32)",
                    )
                    .with_expected("u64, u32")
                    .with_observed(format!(
                        "{}, {}",
                        resolve_operand_type(&instruction.args[0], env)
                            .map(|ty| ty.to_string())
                            .unwrap_or_else(|| "unknown".to_string()),
                        resolve_operand_type(&instruction.args[1], env)
                            .map(|ty| ty.to_string())
                            .unwrap_or_else(|| "unknown".to_string())
                    )),
                );
                return None;
            }
            Some(TypeRef::Bool)
        }
        "service_log" => {
            if instruction.args.len() != 3 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "service_log expects service handle, level token, and message operand",
                    )
                    .with_expected("3")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "SERVICE_RESULT_TYPE",
                        "service_log result type must be b1",
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            let handle_ok = matches!(
                resolve_operand_type(&instruction.args[0], env),
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64
                })
            );
            let message_ok = match resolve_operand_type(&instruction.args[2], env) {
                Some(found) => {
                    view_source_matches(
                        &found,
                        &TypeRef::Buf(Box::new(TypeRef::Int {
                            signed: false,
                            bits: 8,
                        })),
                    ) || view_source_matches(&found, &TypeRef::String)
                }
                None => false,
            };
            if !handle_ok || !message_ok {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "SERVICE_OPERAND_TYPE",
                        "service_log expects (u64, token, own/view/edit[buf[u8]]|own/view/edit[str])",
                    )
                    .with_expected("u64, token, own/view/edit[buf[u8]] | own/view/edit[str]")
                    .with_observed(format!(
                        "{}, {}",
                        resolve_operand_type(&instruction.args[0], env)
                            .map(|ty| ty.to_string())
                            .unwrap_or_else(|| "unknown".to_string()),
                        resolve_operand_type(&instruction.args[2], env)
                            .map(|ty| ty.to_string())
                            .unwrap_or_else(|| "unknown".to_string())
                    )),
                );
                return None;
            }
            Some(TypeRef::Bool)
        }
        "service_trace_begin" => {
            if instruction.args.len() != 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "service_trace_begin expects service handle and span token",
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Int {
                signed: false,
                bits: 64,
            };
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "SERVICE_RESULT_TYPE",
                        "service_trace_begin result type must be u64",
                    )
                    .with_expected("u64")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            if !matches!(
                resolve_operand_type(&instruction.args[0], env),
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64
                })
            ) {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "SERVICE_OPERAND_TYPE",
                        "service_trace_begin expects u64 service handle",
                    )
                    .with_expected("u64")
                    .with_observed(
                        resolve_operand_type(&instruction.args[0], env)
                            .map(|ty| ty.to_string())
                            .unwrap_or_else(|| "unknown".to_string()),
                    ),
                );
                return None;
            }
            Some(expected)
        }
        "service_metric_count" => {
            if instruction.args.len() != 3 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "service_metric_count expects service handle, metric token, and u32 value",
                    )
                    .with_expected("3")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "SERVICE_RESULT_TYPE",
                        "service_metric_count result type must be b1",
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            let handle_ok = matches!(
                resolve_operand_type(&instruction.args[0], env),
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64
                })
            );
            let value_ok = matches!(
                resolve_operand_type(&instruction.args[2], env),
                Some(TypeRef::Int {
                    signed: false,
                    bits: 32
                })
            );
            if !handle_ok || !value_ok {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "SERVICE_OPERAND_TYPE",
                        "service_metric_count expects (u64, token, u32)",
                    )
                    .with_expected("u64, token, u32")
                    .with_observed(format!(
                        "{}, {}",
                        resolve_operand_type(&instruction.args[0], env)
                            .map(|ty| ty.to_string())
                            .unwrap_or_else(|| "unknown".to_string()),
                        resolve_operand_type(&instruction.args[2], env)
                            .map(|ty| ty.to_string())
                            .unwrap_or_else(|| "unknown".to_string())
                    )),
                );
                return None;
            }
            Some(TypeRef::Bool)
        }
        "service_metric_count_dim" => {
            if instruction.args.len() != 4 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "service_metric_count_dim expects service handle, metric token, dimension token, and u32 value",
                    )
                    .with_expected("4")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "SERVICE_RESULT_TYPE",
                        "service_metric_count_dim result type must be b1",
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            let handle_ok = matches!(
                resolve_operand_type(&instruction.args[0], env),
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64
                })
            );
            let value_ok = matches!(
                resolve_operand_type(&instruction.args[3], env),
                Some(TypeRef::Int {
                    signed: false,
                    bits: 32
                })
            );
            if !handle_ok || !value_ok {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "SERVICE_OPERAND_TYPE",
                        "service_metric_count_dim expects (u64, token, token, u32)",
                    )
                    .with_expected("u64, token, token, u32")
                    .with_observed(format!(
                        "{}, {}",
                        resolve_operand_type(&instruction.args[0], env)
                            .map(|ty| ty.to_string())
                            .unwrap_or_else(|| "unknown".to_string()),
                        resolve_operand_type(&instruction.args[3], env)
                            .map(|ty| ty.to_string())
                            .unwrap_or_else(|| "unknown".to_string())
                    )),
                );
                return None;
            }
            Some(TypeRef::Bool)
        }
        "service_metric_total" => {
            if instruction.args.len() != 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "service_metric_total expects service handle and metric token",
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Int {
                signed: false,
                bits: 32,
            };
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "SERVICE_RESULT_TYPE",
                        "service_metric_total result type must be u32",
                    )
                    .with_expected("u32")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            if !matches!(
                resolve_operand_type(&instruction.args[0], env),
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64
                })
            ) {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "SERVICE_OPERAND_TYPE",
                        "service_metric_total expects u64 service handle",
                    )
                    .with_expected("u64")
                    .with_observed(
                        resolve_operand_type(&instruction.args[0], env)
                            .map(|ty| ty.to_string())
                            .unwrap_or_else(|| "unknown".to_string()),
                    ),
                );
                return None;
            }
            Some(expected)
        }
        "service_health_status" | "service_readiness_status" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!("{} expects one service handle", instruction.op),
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Int {
                signed: false,
                bits: 32,
            };
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "SERVICE_RESULT_TYPE",
                        format!("{} result type must be u32", instruction.op),
                    )
                    .with_expected("u32")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            if !matches!(
                resolve_operand_type(&instruction.args[0], env),
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64
                })
            ) {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "SERVICE_OPERAND_TYPE",
                        format!("{} expects u64 service handle", instruction.op),
                    )
                    .with_expected("u64")
                    .with_observed(
                        resolve_operand_type(&instruction.args[0], env)
                            .map(|ty| ty.to_string())
                            .unwrap_or_else(|| "unknown".to_string()),
                    ),
                );
                return None;
            }
            Some(expected)
        }
        "service_set_health" | "service_set_readiness" => {
            if instruction.args.len() != 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!("{} expects service handle and u32 status", instruction.op),
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "SERVICE_RESULT_TYPE",
                        format!("{} result type must be b1", instruction.op),
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            let handle_ok = matches!(
                resolve_operand_type(&instruction.args[0], env),
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64
                })
            );
            let status_ok = matches!(
                resolve_operand_type(&instruction.args[1], env),
                Some(TypeRef::Int {
                    signed: false,
                    bits: 32
                })
            );
            if !handle_ok || !status_ok {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "SERVICE_OPERAND_TYPE",
                        format!("{} expects (u64, u32)", instruction.op),
                    )
                    .with_expected("u64, u32")
                    .with_observed(format!(
                        "{}, {}",
                        resolve_operand_type(&instruction.args[0], env)
                            .map(|ty| ty.to_string())
                            .unwrap_or_else(|| "unknown".to_string()),
                        resolve_operand_type(&instruction.args[1], env)
                            .map(|ty| ty.to_string())
                            .unwrap_or_else(|| "unknown".to_string())
                    )),
                );
                return None;
            }
            Some(TypeRef::Bool)
        }
        "service_set_degraded" => {
            if instruction.args.len() != 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "service_set_degraded expects service handle and b1 flag",
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "SERVICE_RESULT_TYPE",
                        "service_set_degraded result type must be b1",
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            let handle_ok = matches!(
                resolve_operand_type(&instruction.args[0], env),
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64
                })
            );
            let degraded_ok = matches!(resolve_operand_type(&instruction.args[1], env), Some(TypeRef::Bool));
            if !handle_ok || !degraded_ok {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "SERVICE_OPERAND_TYPE",
                        "service_set_degraded expects (u64, b1)",
                    )
                    .with_expected("u64, b1")
                    .with_observed(format!(
                        "{}, {}",
                        resolve_operand_type(&instruction.args[0], env)
                            .map(|ty| ty.to_string())
                            .unwrap_or_else(|| "unknown".to_string()),
                        resolve_operand_type(&instruction.args[1], env)
                            .map(|ty| ty.to_string())
                            .unwrap_or_else(|| "unknown".to_string())
                    )),
                );
                return None;
            }
            Some(TypeRef::Bool)
        }
        "service_degraded" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "service_degraded expects one service handle",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "SERVICE_RESULT_TYPE",
                        "service_degraded result type must be b1",
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            if !matches!(
                resolve_operand_type(&instruction.args[0], env),
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64
                })
            ) {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "SERVICE_OPERAND_TYPE",
                        "service_degraded expects u64 service handle",
                    )
                    .with_expected("u64")
                    .with_observed(
                        resolve_operand_type(&instruction.args[0], env)
                            .map(|ty| ty.to_string())
                            .unwrap_or_else(|| "unknown".to_string()),
                    ),
                );
                return None;
            }
            Some(TypeRef::Bool)
        }
        "service_event" => {
            if instruction.args.len() != 3 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "service_event expects service handle, class token, and message operand",
                    )
                    .with_expected("3")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "SERVICE_RESULT_TYPE",
                        "service_event result type must be b1",
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            let handle_ok = matches!(
                resolve_operand_type(&instruction.args[0], env),
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64
                })
            );
            let message_ok = match resolve_operand_type(&instruction.args[2], env) {
                Some(found) => {
                    view_source_matches(
                        &found,
                        &TypeRef::Buf(Box::new(TypeRef::Int {
                            signed: false,
                            bits: 8,
                        })),
                    ) || view_source_matches(&found, &TypeRef::String)
                }
                None => false,
            };
            if !handle_ok || !message_ok {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "SERVICE_OPERAND_TYPE",
                        "service_event expects (u64, token, own/view/edit[buf[u8]]|own/view/edit[str])",
                    )
                    .with_expected("u64, token, own/view/edit[buf[u8]] | own/view/edit[str]")
                    .with_observed(format!(
                        "{}, {}",
                        resolve_operand_type(&instruction.args[0], env)
                            .map(|ty| ty.to_string())
                            .unwrap_or_else(|| "unknown".to_string()),
                        resolve_operand_type(&instruction.args[2], env)
                            .map(|ty| ty.to_string())
                            .unwrap_or_else(|| "unknown".to_string())
                    )),
                );
                return None;
            }
            Some(TypeRef::Bool)
        }
        "service_event_total" | "service_failure_total" | "service_checkpoint_load_u32" => {
            if instruction.args.len() != 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!("{} expects service handle and one token", instruction.op),
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Int {
                signed: false,
                bits: 32,
            };
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "SERVICE_RESULT_TYPE",
                        format!("{} result type must be u32", instruction.op),
                    )
                    .with_expected("u32")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            if !matches!(
                resolve_operand_type(&instruction.args[0], env),
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64
                })
            ) {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "SERVICE_OPERAND_TYPE",
                        format!("{} expects u64 service handle", instruction.op),
                    )
                    .with_expected("u64")
                    .with_observed(
                        resolve_operand_type(&instruction.args[0], env)
                            .map(|ty| ty.to_string())
                            .unwrap_or_else(|| "unknown".to_string()),
                    ),
                );
                return None;
            }
            Some(expected)
        }
        "service_trace_link" => {
            if instruction.args.len() != 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "service_trace_link expects child and parent trace handles",
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "SERVICE_RESULT_TYPE",
                        "service_trace_link result type must be b1",
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            let child_ok = matches!(
                resolve_operand_type(&instruction.args[0], env),
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64
                })
            );
            let parent_ok = matches!(
                resolve_operand_type(&instruction.args[1], env),
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64
                })
            );
            if !child_ok || !parent_ok {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "SERVICE_OPERAND_TYPE",
                        "service_trace_link expects (u64, u64)",
                    )
                    .with_expected("u64, u64")
                    .with_observed(format!(
                        "{}, {}",
                        resolve_operand_type(&instruction.args[0], env)
                            .map(|ty| ty.to_string())
                            .unwrap_or_else(|| "unknown".to_string()),
                        resolve_operand_type(&instruction.args[1], env)
                            .map(|ty| ty.to_string())
                            .unwrap_or_else(|| "unknown".to_string())
                    )),
                );
                return None;
            }
            Some(TypeRef::Bool)
        }
        "service_trace_link_count" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "service_trace_link_count expects one service handle",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Int {
                signed: false,
                bits: 32,
            };
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "SERVICE_RESULT_TYPE",
                        "service_trace_link_count result type must be u32",
                    )
                    .with_expected("u32")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            if !matches!(
                resolve_operand_type(&instruction.args[0], env),
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64
                })
            ) {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "SERVICE_OPERAND_TYPE",
                        "service_trace_link_count expects u64 service handle",
                    )
                    .with_expected("u64")
                    .with_observed(
                        resolve_operand_type(&instruction.args[0], env)
                            .map(|ty| ty.to_string())
                            .unwrap_or_else(|| "unknown".to_string()),
                    ),
                );
                return None;
            }
            Some(expected)
        }
        "service_failure_count" => {
            if instruction.args.len() != 3 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "service_failure_count expects service handle, failure token, and u32 value",
                    )
                    .with_expected("3")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "SERVICE_RESULT_TYPE",
                        "service_failure_count result type must be b1",
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            let handle_ok = matches!(
                resolve_operand_type(&instruction.args[0], env),
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64
                })
            );
            let value_ok = matches!(
                resolve_operand_type(&instruction.args[2], env),
                Some(TypeRef::Int {
                    signed: false,
                    bits: 32
                })
            );
            if !handle_ok || !value_ok {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "SERVICE_OPERAND_TYPE",
                        "service_failure_count expects (u64, token, u32)",
                    )
                    .with_expected("u64, token, u32")
                    .with_observed(format!(
                        "{}, {}",
                        resolve_operand_type(&instruction.args[0], env)
                            .map(|ty| ty.to_string())
                            .unwrap_or_else(|| "unknown".to_string()),
                        resolve_operand_type(&instruction.args[2], env)
                            .map(|ty| ty.to_string())
                            .unwrap_or_else(|| "unknown".to_string())
                    )),
                );
                return None;
            }
            Some(TypeRef::Bool)
        }
        "service_checkpoint_save_u32" => {
            if instruction.args.len() != 3 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "service_checkpoint_save_u32 expects service handle, key token, and u32 value",
                    )
                    .with_expected("3")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "SERVICE_RESULT_TYPE",
                        "service_checkpoint_save_u32 result type must be b1",
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            let handle_ok = matches!(
                resolve_operand_type(&instruction.args[0], env),
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64
                })
            );
            let value_ok = matches!(
                resolve_operand_type(&instruction.args[2], env),
                Some(TypeRef::Int {
                    signed: false,
                    bits: 32
                })
            );
            if !handle_ok || !value_ok {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "SERVICE_OPERAND_TYPE",
                        "service_checkpoint_save_u32 expects (u64, token, u32)",
                    )
                    .with_expected("u64, token, u32")
                    .with_observed(format!(
                        "{}, {}",
                        resolve_operand_type(&instruction.args[0], env)
                            .map(|ty| ty.to_string())
                            .unwrap_or_else(|| "unknown".to_string()),
                        resolve_operand_type(&instruction.args[2], env)
                            .map(|ty| ty.to_string())
                            .unwrap_or_else(|| "unknown".to_string())
                    )),
                );
                return None;
            }
            Some(TypeRef::Bool)
        }
        "service_checkpoint_exists" => {
            if instruction.args.len() != 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "service_checkpoint_exists expects service handle and key token",
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "SERVICE_RESULT_TYPE",
                        "service_checkpoint_exists result type must be b1",
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            if !matches!(
                resolve_operand_type(&instruction.args[0], env),
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64
                })
            ) {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "SERVICE_OPERAND_TYPE",
                        "service_checkpoint_exists expects u64 service handle",
                    )
                    .with_expected("u64")
                    .with_observed(
                        resolve_operand_type(&instruction.args[0], env)
                            .map(|ty| ty.to_string())
                            .unwrap_or_else(|| "unknown".to_string()),
                    ),
                );
                return None;
            }
            Some(TypeRef::Bool)
        }
        "service_migrate_db" => {
            if instruction.args.len() != 3 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "service_migrate_db expects service handle, db handle, and migration token",
                    )
                    .with_expected("3")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "SERVICE_RESULT_TYPE",
                        "service_migrate_db result type must be b1",
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            let service_ok = matches!(
                resolve_operand_type(&instruction.args[0], env),
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64
                })
            );
            let db_ok = matches!(
                resolve_operand_type(&instruction.args[1], env),
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64
                })
            );
            if !service_ok || !db_ok {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "SERVICE_OPERAND_TYPE",
                        "service_migrate_db expects (u64, u64, token)",
                    )
                    .with_expected("u64, u64, token")
                    .with_observed(format!(
                        "{}, {}",
                        resolve_operand_type(&instruction.args[0], env)
                            .map(|ty| ty.to_string())
                            .unwrap_or_else(|| "unknown".to_string()),
                        resolve_operand_type(&instruction.args[1], env)
                            .map(|ty| ty.to_string())
                            .unwrap_or_else(|| "unknown".to_string())
                    )),
                );
                return None;
            }
            Some(TypeRef::Bool)
        }
        "service_route" | "service_require_header" => {
            if instruction.args.len() != 3 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!("{} expects request plus two static tokens", instruction.op),
                    )
                    .with_expected("3")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "SERVICE_RESULT_TYPE",
                        format!("{} result type must be b1", instruction.op),
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(found)
                    if view_source_matches(
                        &found,
                        &TypeRef::Buf(Box::new(TypeRef::Int {
                            signed: false,
                            bits: 8,
                        })),
                    ) =>
                {
                    Some(TypeRef::Bool)
                }
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "SERVICE_OPERAND_TYPE",
                            format!(
                                "{} expects own/view/edit[buf[u8]] as the first operand",
                                instruction.op
                            ),
                        )
                        .with_expected("own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown {} operand {}", instruction.op, instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "service_error_status" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "service_error_status expects one status token",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Int {
                signed: false,
                bits: 32,
            };
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "SERVICE_RESULT_TYPE",
                        "service_error_status result type must be u32",
                    )
                    .with_expected("u32")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            Some(expected)
        }
        "tls_server_config_u32" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "tls_server_config_u32 expects one config token",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Int {
                signed: false,
                bits: 32,
            };
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "TLS_RESULT_TYPE",
                        "tls_server_config_u32 result type must be u32",
                    )
                    .with_expected("u32")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match instruction.args[0].as_str() {
                "request_timeout_ms" | "session_timeout_ms" | "shutdown_grace_ms" => Some(expected),
                _ => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "TLS_CONFIG_TOKEN",
                            "tls_server_config_u32 token must be request_timeout_ms, session_timeout_ms, or shutdown_grace_ms",
                        )
                        .with_expected("request_timeout_ms | session_timeout_ms | shutdown_grace_ms")
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "tls_server_config_buf" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "tls_server_config_buf expects one config token",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Own(Box::new(TypeRef::Buf(Box::new(TypeRef::Int {
                signed: false,
                bits: 8,
            }))));
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "TLS_RESULT_TYPE",
                        "tls_server_config_buf result type must be own[buf[u8]]",
                    )
                    .with_expected("own[buf[u8]]")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match instruction.args[0].as_str() {
                "cert" | "key" => Some(expected),
                _ => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "TLS_CONFIG_TOKEN",
                            "tls_server_config_buf token must be cert or key",
                        )
                        .with_expected("cert | key")
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "http_write_response" => {
            if instruction.args.len() != 3 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "http_write_response expects a connection handle, status code, and body buf",
                    )
                    .with_expected("3")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "HTTP_RESULT_TYPE",
                        "http_write_response result type must be b1",
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64,
                }) => {}
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "HTTP_OPERAND_TYPE",
                            "http_write_response expects a u64 connection handle as the first operand",
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    return None;
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!(
                                "unknown http_write_response operand {}",
                                instruction.args[0]
                            ),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    return None;
                }
            }
            match resolve_operand_type(&instruction.args[1], env) {
                Some(TypeRef::Int { bits: 32, .. }) => {}
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "HTTP_OPERAND_TYPE",
                            "http_write_response expects a 32-bit integer status code as the second operand",
                        )
                        .with_expected("u32 | i32")
                        .with_observed(found.to_string()),
                    );
                    return None;
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!(
                                "unknown http_write_response operand {}",
                                instruction.args[1]
                            ),
                        )
                        .with_observed(instruction.args[1].clone()),
                    );
                    return None;
                }
            }
            match resolve_operand_type(&instruction.args[2], env) {
                Some(TypeRef::Own(inner))
                | Some(TypeRef::View(inner))
                | Some(TypeRef::Edit(inner))
                    if matches!(
                        inner.as_ref(),
                        TypeRef::Buf(elem)
                            if **elem
                                == TypeRef::Int {
                                    signed: false,
                                    bits: 8,
                                }
                    ) =>
                {
                    Some(TypeRef::Bool)
                }
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "HTTP_OPERAND_TYPE",
                            "http_write_response expects own/view/edit[buf[u8]] as the third operand",
                        )
                        .with_expected("own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!(
                                "unknown http_write_response operand {}",
                                instruction.args[2]
                            ),
                        )
                        .with_observed(instruction.args[2].clone()),
                    );
                    None
                }
            }
        }
        "http_write_text_response"
        | "http_write_json_response"
        | "http_session_write_text"
        | "http_session_write_json" => {
            if instruction.args.len() != 3 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!(
                            "{} expects a connection handle, status code, and body buf",
                            instruction.op
                        ),
                    )
                    .with_expected("3")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "HTTP_RESULT_TYPE",
                        format!("{} result type must be b1", instruction.op),
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64,
                }) => {}
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "HTTP_OPERAND_TYPE",
                            format!(
                                "{} expects a u64 connection handle as the first operand",
                                instruction.op
                            ),
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    return None;
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown {} operand {}", instruction.op, instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    return None;
                }
            }
            match resolve_operand_type(&instruction.args[1], env) {
                Some(TypeRef::Int { bits: 32, .. }) => {}
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "HTTP_OPERAND_TYPE",
                            format!(
                                "{} expects a 32-bit integer status code as the second operand",
                                instruction.op
                            ),
                        )
                        .with_expected("u32 | i32")
                        .with_observed(found.to_string()),
                    );
                    return None;
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown {} operand {}", instruction.op, instruction.args[1]),
                        )
                        .with_observed(instruction.args[1].clone()),
                    );
                    return None;
                }
            }
            match resolve_operand_type(&instruction.args[2], env) {
                Some(TypeRef::Own(inner))
                | Some(TypeRef::View(inner))
                | Some(TypeRef::Edit(inner))
                    if matches!(
                        inner.as_ref(),
                        TypeRef::Buf(elem)
                            if **elem
                                == TypeRef::Int {
                                    signed: false,
                                    bits: 8,
                                }
                    ) =>
                {
                    Some(TypeRef::Bool)
                }
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "HTTP_OPERAND_TYPE",
                            format!(
                                "{} expects own/view/edit[buf[u8]] as the third operand",
                                instruction.op
                            ),
                        )
                        .with_expected("own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown {} operand {}", instruction.op, instruction.args[2]),
                        )
                        .with_observed(instruction.args[2].clone()),
                    );
                    None
                }
            }
        }
        "http_write_text_response_cookie"
        | "http_write_json_response_cookie"
        | "http_session_write_text_cookie"
        | "http_session_write_json_cookie" => {
            if instruction.args.len() != 5 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!(
                            "{} expects a connection handle, status code, cookie-name literal, cookie-value literal, and body buf",
                            instruction.op
                        ),
                    )
                    .with_expected("5")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "HTTP_RESULT_TYPE",
                        format!("{} result type must be b1", instruction.op),
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64,
                }) => {}
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "HTTP_OPERAND_TYPE",
                            format!(
                                "{} expects a u64 connection handle as the first operand",
                                instruction.op
                            ),
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    return None;
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown {} operand {}", instruction.op, instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    return None;
                }
            }
            match resolve_operand_type(&instruction.args[1], env) {
                Some(TypeRef::Int { bits: 32, .. }) => {}
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "HTTP_OPERAND_TYPE",
                            format!(
                                "{} expects a 32-bit integer status code as the second operand",
                                instruction.op
                            ),
                        )
                        .with_expected("u32 | i32")
                        .with_observed(found.to_string()),
                    );
                    return None;
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown {} operand {}", instruction.op, instruction.args[1]),
                        )
                        .with_observed(instruction.args[1].clone()),
                    );
                    return None;
                }
            }
            match resolve_operand_type(&instruction.args[4], env) {
                Some(TypeRef::Own(inner))
                | Some(TypeRef::View(inner))
                | Some(TypeRef::Edit(inner))
                    if matches!(
                        inner.as_ref(),
                        TypeRef::Buf(elem)
                            if **elem
                                == TypeRef::Int {
                                    signed: false,
                                    bits: 8,
                                }
                    ) =>
                {
                    Some(TypeRef::Bool)
                }
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "HTTP_OPERAND_TYPE",
                            format!(
                                "{} expects own/view/edit[buf[u8]] as the fifth operand",
                                instruction.op
                            ),
                        )
                        .with_expected("own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown {} operand {}", instruction.op, instruction.args[4]),
                        )
                        .with_observed(instruction.args[4].clone()),
                    );
                    None
                }
            }
        }
        "http_write_text_response_headers2"
        | "http_write_json_response_headers2"
        | "http_session_write_text_headers2"
        | "http_session_write_json_headers2" => {
            if instruction.args.len() != 7 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!(
                            "{} expects a connection handle, status code, two header-name/header-value literal pairs, and body buf",
                            instruction.op
                        ),
                    )
                    .with_expected("7")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "HTTP_RESULT_TYPE",
                        format!("{} result type must be b1", instruction.op),
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64,
                }) => {}
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "HTTP_OPERAND_TYPE",
                            format!(
                                "{} expects a u64 connection handle as the first operand",
                                instruction.op
                            ),
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    return None;
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown {} operand {}", instruction.op, instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    return None;
                }
            }
            match resolve_operand_type(&instruction.args[1], env) {
                Some(TypeRef::Int { bits: 32, .. }) => {}
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "HTTP_OPERAND_TYPE",
                            format!(
                                "{} expects a 32-bit integer status code as the second operand",
                                instruction.op
                            ),
                        )
                        .with_expected("u32 | i32")
                        .with_observed(found.to_string()),
                    );
                    return None;
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown {} operand {}", instruction.op, instruction.args[1]),
                        )
                        .with_observed(instruction.args[1].clone()),
                    );
                    return None;
                }
            }
            match resolve_operand_type(&instruction.args[6], env) {
                Some(TypeRef::Own(inner))
                | Some(TypeRef::View(inner))
                | Some(TypeRef::Edit(inner))
                    if matches!(
                        inner.as_ref(),
                        TypeRef::Buf(elem)
                            if **elem
                                == TypeRef::Int {
                                    signed: false,
                                    bits: 8,
                                }
                    ) =>
                {
                    Some(TypeRef::Bool)
                }
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "HTTP_OPERAND_TYPE",
                            format!(
                                "{} expects own/view/edit[buf[u8]] as the seventh operand",
                                instruction.op
                            ),
                        )
                        .with_expected("own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown {} operand {}", instruction.op, instruction.args[6]),
                        )
                        .with_observed(instruction.args[6].clone()),
                    );
                    None
                }
            }
        }
        "http_write_response_header" => {
            if instruction.args.len() != 5 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "http_write_response_header expects handle, status, header-name literal, header-value literal, and body buf",
                    )
                    .with_expected("5")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "HTTP_RESULT_TYPE",
                        "http_write_response_header result type must be b1",
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64,
                }) => {}
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "HTTP_OPERAND_TYPE",
                            "http_write_response_header expects a u64 connection handle as the first operand",
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    return None;
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!(
                                "unknown http_write_response_header operand {}",
                                instruction.args[0]
                            ),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    return None;
                }
            }
            match resolve_operand_type(&instruction.args[1], env) {
                Some(TypeRef::Int { bits: 32, .. }) => {}
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "HTTP_OPERAND_TYPE",
                            "http_write_response_header expects a 32-bit integer status code as the second operand",
                        )
                        .with_expected("u32 | i32")
                        .with_observed(found.to_string()),
                    );
                    return None;
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!(
                                "unknown http_write_response_header operand {}",
                                instruction.args[1]
                            ),
                        )
                        .with_observed(instruction.args[1].clone()),
                    );
                    return None;
                }
            }
            match resolve_operand_type(&instruction.args[4], env) {
                Some(TypeRef::Own(inner))
                | Some(TypeRef::View(inner))
                | Some(TypeRef::Edit(inner))
                    if matches!(
                        inner.as_ref(),
                        TypeRef::Buf(elem)
                            if **elem
                                == TypeRef::Int {
                                    signed: false,
                                    bits: 8,
                                }
                    ) =>
                {
                    Some(TypeRef::Bool)
                }
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "HTTP_OPERAND_TYPE",
                            "http_write_response_header expects own/view/edit[buf[u8]] as the fifth operand",
                        )
                        .with_expected("own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!(
                                "unknown http_write_response_header operand {}",
                                instruction.args[4]
                            ),
                        )
                        .with_observed(instruction.args[4].clone()),
                    );
                    None
                }
            }
        }
        "http_response_stream_open" => {
            if instruction.args.len() != 3 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "http_response_stream_open expects session handle, status, and content-type token",
                    )
                    .with_expected("3")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Int {
                signed: false,
                bits: 64,
            };
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "HTTP_RESULT_TYPE",
                        "http_response_stream_open result type must be u64",
                    )
                    .with_expected("u64")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            let handle_ok = matches!(
                resolve_operand_type(&instruction.args[0], env),
                Some(TypeRef::Int { signed: false, bits: 64 })
            );
            let status_ok = matches!(
                resolve_operand_type(&instruction.args[1], env),
                Some(TypeRef::Int { bits: 32, .. })
            );
            if !handle_ok || !status_ok {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "HTTP_OPERAND_TYPE",
                        "http_response_stream_open expects (u64, u32|i32, token)",
                    )
                    .with_expected("u64, u32|i32, token")
                    .with_observed(format!(
                        "{}, {}",
                        resolve_operand_type(&instruction.args[0], env)
                            .map(|ty| ty.to_string())
                            .unwrap_or_else(|| "unknown".to_string()),
                        resolve_operand_type(&instruction.args[1], env)
                            .map(|ty| ty.to_string())
                            .unwrap_or_else(|| "unknown".to_string())
                    )),
                );
                return None;
            }
            Some(expected)
        }
        "http_response_stream_write" => {
            if instruction.args.len() != 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "http_response_stream_write expects stream handle and body buf",
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "HTTP_RESULT_TYPE",
                        "http_response_stream_write result type must be b1",
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            let handle_ok = matches!(
                resolve_operand_type(&instruction.args[0], env),
                Some(TypeRef::Int { signed: false, bits: 64 })
            );
            let body_ok = match resolve_operand_type(&instruction.args[1], env) {
                Some(found) => view_source_matches(
                    &found,
                    &TypeRef::Buf(Box::new(TypeRef::Int {
                        signed: false,
                        bits: 8,
                    })),
                ),
                None => false,
            };
            if !handle_ok || !body_ok {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "HTTP_OPERAND_TYPE",
                        "http_response_stream_write expects (u64, own/view/edit[buf[u8]])",
                    )
                    .with_expected("u64, own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                    .with_observed(format!(
                        "{}, {}",
                        resolve_operand_type(&instruction.args[0], env)
                            .map(|ty| ty.to_string())
                            .unwrap_or_else(|| "unknown".to_string()),
                        resolve_operand_type(&instruction.args[1], env)
                            .map(|ty| ty.to_string())
                            .unwrap_or_else(|| "unknown".to_string())
                    )),
                );
                return None;
            }
            Some(TypeRef::Bool)
        }
        "http_response_stream_close" | "http_client_close" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!("{} expects one handle operand", instruction.op),
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "HTTP_RESULT_TYPE",
                        format!("{} result type must be b1", instruction.op),
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64,
                }) => Some(TypeRef::Bool),
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "HTTP_OPERAND_TYPE",
                            format!("{} expects u64 handle operand", instruction.op),
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown {} operand {}", instruction.op, instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "http_client_open" => {
            if !instruction.args.is_empty() {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "http_client_open expects 0 operands",
                    )
                    .with_expected("0")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Int {
                signed: false,
                bits: 64,
            };
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "HTTP_RESULT_TYPE",
                        "http_client_open result type must be u64",
                    )
                    .with_expected("u64")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            Some(expected)
        }
        "http_client_request" => {
            if instruction.args.len() != 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "http_client_request expects client handle and request buf",
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Own(Box::new(TypeRef::Buf(Box::new(TypeRef::Int {
                signed: false,
                bits: 8,
            }))));
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "HTTP_RESULT_TYPE",
                        "http_client_request result type must be own[buf[u8]]",
                    )
                    .with_expected("own[buf[u8]]")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            let handle_ok = matches!(
                resolve_operand_type(&instruction.args[0], env),
                Some(TypeRef::Int { signed: false, bits: 64 })
            );
            let request_ok = match resolve_operand_type(&instruction.args[1], env) {
                Some(found) => view_source_matches(
                    &found,
                    &TypeRef::Buf(Box::new(TypeRef::Int {
                        signed: false,
                        bits: 8,
                    })),
                ),
                None => false,
            };
            if !handle_ok || !request_ok {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "HTTP_OPERAND_TYPE",
                        "http_client_request expects (u64, own/view/edit[buf[u8]])",
                    )
                    .with_expected("u64, own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                    .with_observed(format!(
                        "{}, {}",
                        resolve_operand_type(&instruction.args[0], env)
                            .map(|ty| ty.to_string())
                            .unwrap_or_else(|| "unknown".to_string()),
                        resolve_operand_type(&instruction.args[1], env)
                            .map(|ty| ty.to_string())
                            .unwrap_or_else(|| "unknown".to_string())
                    )),
                );
                return None;
            }
            Some(expected)
        }
        "http_client_request_retry" => {
            if instruction.args.len() != 4 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "http_client_request_retry expects client handle, retries, backoff_ms, and request buf",
                    )
                    .with_expected("4")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Own(Box::new(TypeRef::Buf(Box::new(TypeRef::Int {
                signed: false,
                bits: 8,
            }))));
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "HTTP_RESULT_TYPE",
                        "http_client_request_retry result type must be own[buf[u8]]",
                    )
                    .with_expected("own[buf[u8]]")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            let handle_ok = matches!(
                resolve_operand_type(&instruction.args[0], env),
                Some(TypeRef::Int { signed: false, bits: 64 })
            );
            let retries_ok = matches!(
                resolve_operand_type(&instruction.args[1], env),
                Some(TypeRef::Int { signed: false, bits: 32 })
            );
            let backoff_ok = matches!(
                resolve_operand_type(&instruction.args[2], env),
                Some(TypeRef::Int { signed: false, bits: 32 })
            );
            let request_ok = match resolve_operand_type(&instruction.args[3], env) {
                Some(found) => view_source_matches(
                    &found,
                    &TypeRef::Buf(Box::new(TypeRef::Int {
                        signed: false,
                        bits: 8,
                    })),
                ),
                None => false,
            };
            if !handle_ok || !retries_ok || !backoff_ok || !request_ok {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "HTTP_OPERAND_TYPE",
                        "http_client_request_retry expects (u64, u32, u32, own/view/edit[buf[u8]])",
                    )
                    .with_expected("u64, u32, u32, own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                    .with_observed(format!(
                        "{}, {}, {}, {}",
                        resolve_operand_type(&instruction.args[0], env)
                            .map(|ty| ty.to_string())
                            .unwrap_or_else(|| "unknown".to_string()),
                        resolve_operand_type(&instruction.args[1], env)
                            .map(|ty| ty.to_string())
                            .unwrap_or_else(|| "unknown".to_string()),
                        resolve_operand_type(&instruction.args[2], env)
                            .map(|ty| ty.to_string())
                            .unwrap_or_else(|| "unknown".to_string()),
                        resolve_operand_type(&instruction.args[3], env)
                            .map(|ty| ty.to_string())
                            .unwrap_or_else(|| "unknown".to_string())
                    )),
                );
                return None;
            }
            Some(expected)
        }
        "http_client_pool_open" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "http_client_pool_open expects one u32 max-size operand",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Int {
                signed: false,
                bits: 64,
            };
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "HTTP_RESULT_TYPE",
                        "http_client_pool_open result type must be u64",
                    )
                    .with_expected("u64")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 32,
                }) => Some(expected),
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "HTTP_OPERAND_TYPE",
                            "http_client_pool_open expects u32 max-size operand",
                        )
                        .with_expected("u32")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!(
                                "unknown http_client_pool_open operand {}",
                                instruction.args[0]
                            ),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "http_client_pool_acquire" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "http_client_pool_acquire expects one pool handle operand",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Int {
                signed: false,
                bits: 64,
            };
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "HTTP_RESULT_TYPE",
                        "http_client_pool_acquire result type must be u64",
                    )
                    .with_expected("u64")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64,
                }) => Some(expected),
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "HTTP_OPERAND_TYPE",
                            "http_client_pool_acquire expects u64 pool handle",
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!(
                                "unknown http_client_pool_acquire operand {}",
                                instruction.args[0]
                            ),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "http_client_pool_release" | "http_client_pool_close" => {
            let expected_arity = if instruction.op == "http_client_pool_release" {
                2
            } else {
                1
            };
            if instruction.args.len() != expected_arity {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!("{} expects {} operand(s)", instruction.op, expected_arity),
                    )
                    .with_expected(expected_arity.to_string())
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "HTTP_RESULT_TYPE",
                        format!("{} result type must be b1", instruction.op),
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            let pool_ok = matches!(
                resolve_operand_type(&instruction.args[0], env),
                Some(TypeRef::Int { signed: false, bits: 64 })
            );
            let handle_ok = instruction.op != "http_client_pool_release"
                || matches!(
                    resolve_operand_type(&instruction.args[1], env),
                    Some(TypeRef::Int { signed: false, bits: 64 })
                );
            if !pool_ok || !handle_ok {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "HTTP_OPERAND_TYPE",
                        if instruction.op == "http_client_pool_release" {
                            "http_client_pool_release expects (u64 pool, u64 client)"
                        } else {
                            "http_client_pool_close expects u64 pool handle"
                        },
                    )
                    .with_expected(if instruction.op == "http_client_pool_release" {
                        "u64, u64"
                    } else {
                        "u64"
                    })
                    .with_observed(if instruction.op == "http_client_pool_release" {
                        format!(
                            "{}, {}",
                            resolve_operand_type(&instruction.args[0], env)
                                .map(|ty| ty.to_string())
                                .unwrap_or_else(|| "unknown".to_string()),
                            resolve_operand_type(&instruction.args[1], env)
                                .map(|ty| ty.to_string())
                                .unwrap_or_else(|| "unknown".to_string())
                        )
                    } else {
                        resolve_operand_type(&instruction.args[0], env)
                            .map(|ty| ty.to_string())
                            .unwrap_or_else(|| "unknown".to_string())
                    }),
                );
                return None;
            }
            Some(TypeRef::Bool)
        }
        "buf_parse_u32" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "buf_parse_u32 expects one buf operand",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Int {
                signed: false,
                bits: 32,
            };
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "BUF_RESULT_TYPE",
                        "buf_parse_u32 result type must be u32",
                    )
                    .with_expected("u32")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Own(inner))
                | Some(TypeRef::View(inner))
                | Some(TypeRef::Edit(inner))
                    if matches!(
                        inner.as_ref(),
                        TypeRef::Buf(elem)
                            if **elem
                                == TypeRef::Int {
                                    signed: false,
                                    bits: 8,
                                }
                    ) =>
                {
                    Some(expected)
                }
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "BUF_OPERAND_TYPE",
                            "buf_parse_u32 expects own/view/edit[buf[u8]]",
                        )
                        .with_expected("own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown buf_parse_u32 operand {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "buf_parse_bool" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "buf_parse_bool expects one buf operand",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Bool;
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "BUF_RESULT_TYPE",
                        "buf_parse_bool result type must be b1",
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Own(inner))
                | Some(TypeRef::View(inner))
                | Some(TypeRef::Edit(inner))
                    if matches!(
                        inner.as_ref(),
                        TypeRef::Buf(elem)
                            if **elem
                                == TypeRef::Int {
                                    signed: false,
                                    bits: 8,
                                }
                    ) =>
                {
                    Some(expected)
                }
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "BUF_OPERAND_TYPE",
                            "buf_parse_bool expects own/view/edit[buf[u8]]",
                        )
                        .with_expected("own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown buf_parse_bool operand {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "spawn_call" => {
            if instruction.args.is_empty() {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "spawn_call expects at least one command token",
                    )
                    .with_expected(">=1")
                    .with_observed("0"),
                );
                return None;
            }
            if validate_spawn_invocation_tokens(&instruction.args).is_err() {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "SPAWN_COMMAND_INVALID",
                        "spawn_call requires a valid command plus optional argv/env tokens",
                    )
                    .with_observed(instruction.args.join(" ")),
                );
                return None;
            }
            match &instruction.ty {
                TypeRef::Int {
                    signed: true,
                    bits: 32,
                } => Some(TypeRef::Int {
                    signed: true,
                    bits: 32,
                }),
                found => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "SPAWN_RESULT_TYPE",
                            "spawn_call result type must be i32",
                        )
                        .with_expected("i32")
                        .with_observed(found.to_string()),
                    );
                    None
                }
            }
        }
        "json_get_u32" => {
            if instruction.args.len() != 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "json_get_u32 expects a json buf and a key literal token",
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty
                != (TypeRef::Int {
                    signed: false,
                    bits: 32,
                })
            {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "JSON_RESULT_TYPE",
                        "json_get_u32 result type must be u32",
                    )
                    .with_expected("u32")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(found) if is_buf_u8_like_type(&found) || is_string_like_type(&found) => {
                    Some(TypeRef::Int {
                        signed: false,
                        bits: 32,
                    })
                }
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "JSON_OPERAND_TYPE",
                            "json_get_u32 expects own/view/edit[str] or own/view/edit[buf[u8]] as the first operand",
                        )
                        .with_expected("own[str] | view[str] | edit[str] | own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown json_get_u32 operand {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "json_get_bool" => {
            if instruction.args.len() != 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "json_get_bool expects a json buf and a key literal token",
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "JSON_RESULT_TYPE",
                        "json_get_bool result type must be b1",
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(found) if is_buf_u8_like_type(&found) || is_string_like_type(&found) => {
                    Some(TypeRef::Bool)
                }
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "JSON_OPERAND_TYPE",
                            "json_get_bool expects own/view/edit[str] or own/view/edit[buf[u8]] as the first operand",
                        )
                        .with_expected("own[str] | view[str] | edit[str] | own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown json_get_bool operand {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "json_get_buf" => {
            if instruction.args.len() != 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "json_get_buf expects a json buf and a key literal token",
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Own(Box::new(TypeRef::Buf(Box::new(TypeRef::Int {
                signed: false,
                bits: 8,
            }))));
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "JSON_RESULT_TYPE",
                        "json_get_buf result type must be own[buf[u8]]",
                    )
                    .with_expected("own[buf[u8]]")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(found) if is_buf_u8_like_type(&found) || is_string_like_type(&found) => {
                    Some(expected)
                }
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "JSON_OPERAND_TYPE",
                            "json_get_buf expects own/view/edit[str] or own/view/edit[buf[u8]] as the first operand",
                        )
                        .with_expected("own[str] | view[str] | edit[str] | own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown json_get_buf operand {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "json_get_str" => {
            if instruction.args.len() != 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "json_get_str expects a json value and a key literal token",
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Own(Box::new(TypeRef::String));
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "JSON_RESULT_TYPE",
                        "json_get_str result type must be own[str]",
                    )
                    .with_expected("own[str]")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(found) if is_buf_u8_like_type(&found) || is_string_like_type(&found) => {
                    Some(expected)
                }
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "JSON_OPERAND_TYPE",
                            "json_get_str expects own/view/edit[str] or own/view/edit[buf[u8]] as the first operand",
                        )
                        .with_expected("own[str] | view[str] | edit[str] | own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown json_get_str operand {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "json_has_key" => {
            if instruction.args.len() != 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "json_has_key expects a json value and a key literal token",
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "JSON_RESULT_TYPE",
                        "json_has_key result type must be b1",
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(found) if is_buf_u8_like_type(&found) || is_string_like_type(&found) => {
                    Some(TypeRef::Bool)
                }
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "JSON_OPERAND_TYPE",
                            "json_has_key expects own/view/edit[str] or own/view/edit[buf[u8]] as the first operand",
                        )
                        .with_expected("own[str] | view[str] | edit[str] | own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown json_has_key operand {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "json_get_u32_or" => {
            if instruction.args.len() != 3 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "json_get_u32_or expects a json value, key token, and u32 default",
                    )
                    .with_expected("3")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Int {
                signed: false,
                bits: 32,
            };
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "JSON_RESULT_TYPE",
                        "json_get_u32_or result type must be u32",
                    )
                    .with_expected("u32")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(found) if is_buf_u8_like_type(&found) || is_string_like_type(&found) => {}
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "JSON_OPERAND_TYPE",
                            "json_get_u32_or expects own/view/edit[str] or own/view/edit[buf[u8]] as the first operand",
                        )
                        .with_expected("own[str] | view[str] | edit[str] | own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                        .with_observed(found.to_string()),
                    );
                    return None;
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown json_get_u32_or operand {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    return None;
                }
            }
            match resolve_operand_type(&instruction.args[2], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 32,
                }) => Some(expected),
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "JSON_OPERAND_TYPE",
                            "json_get_u32_or expects u32 default as the third operand",
                        )
                        .with_expected("u32")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown json_get_u32_or operand {}", instruction.args[2]),
                        )
                        .with_observed(instruction.args[2].clone()),
                    );
                    None
                }
            }
        }
        "json_get_bool_or" => {
            if instruction.args.len() != 3 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "json_get_bool_or expects a json value, key token, and b1 default",
                    )
                    .with_expected("3")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "JSON_RESULT_TYPE",
                        "json_get_bool_or result type must be b1",
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(found) if is_buf_u8_like_type(&found) || is_string_like_type(&found) => {}
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "JSON_OPERAND_TYPE",
                            "json_get_bool_or expects own/view/edit[str] or own/view/edit[buf[u8]] as the first operand",
                        )
                        .with_expected("own[str] | view[str] | edit[str] | own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                        .with_observed(found.to_string()),
                    );
                    return None;
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown json_get_bool_or operand {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    return None;
                }
            }
            match resolve_operand_type(&instruction.args[2], env) {
                Some(TypeRef::Bool) => Some(TypeRef::Bool),
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "JSON_OPERAND_TYPE",
                            "json_get_bool_or expects b1 default as the third operand",
                        )
                        .with_expected("b1")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown json_get_bool_or operand {}", instruction.args[2]),
                        )
                        .with_observed(instruction.args[2].clone()),
                    );
                    None
                }
            }
        }
        "json_get_buf_or" => {
            if instruction.args.len() != 3 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "json_get_buf_or expects a json value, key token, and buf default",
                    )
                    .with_expected("3")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Own(Box::new(TypeRef::Buf(Box::new(TypeRef::Int {
                signed: false,
                bits: 8,
            }))));
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "JSON_RESULT_TYPE",
                        "json_get_buf_or result type must be own[buf[u8]]",
                    )
                    .with_expected("own[buf[u8]]")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(found) if is_buf_u8_like_type(&found) || is_string_like_type(&found) => {}
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "JSON_OPERAND_TYPE",
                            "json_get_buf_or expects own/view/edit[str] or own/view/edit[buf[u8]] as the first operand",
                        )
                        .with_expected("own[str] | view[str] | edit[str] | own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                        .with_observed(found.to_string()),
                    );
                    return None;
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown json_get_buf_or operand {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    return None;
                }
            }
            match resolve_operand_type(&instruction.args[2], env) {
                Some(found) if is_buf_u8_like_type(&found) => Some(expected),
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "JSON_OPERAND_TYPE",
                            "json_get_buf_or expects own/view/edit[buf[u8]] default as the third operand",
                        )
                        .with_expected("own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown json_get_buf_or operand {}", instruction.args[2]),
                        )
                        .with_observed(instruction.args[2].clone()),
                    );
                    None
                }
            }
        }
        "json_get_str_or" => {
            if instruction.args.len() != 3 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "json_get_str_or expects a json value, key token, and string default",
                    )
                    .with_expected("3")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Own(Box::new(TypeRef::String));
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "JSON_RESULT_TYPE",
                        "json_get_str_or result type must be own[str]",
                    )
                    .with_expected("own[str]")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(found) if is_buf_u8_like_type(&found) || is_string_like_type(&found) => {}
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "JSON_OPERAND_TYPE",
                            "json_get_str_or expects own/view/edit[str] or own/view/edit[buf[u8]] as the first operand",
                        )
                        .with_expected("own[str] | view[str] | edit[str] | own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                        .with_observed(found.to_string()),
                    );
                    return None;
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown json_get_str_or operand {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    return None;
                }
            }
            match resolve_operand_type(&instruction.args[2], env) {
                Some(found) if is_string_like_type(&found) => Some(expected),
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "JSON_OPERAND_TYPE",
                            "json_get_str_or expects own/view/edit[str] default as the third operand",
                        )
                        .with_expected("own[str] | view[str] | edit[str]")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown json_get_str_or operand {}", instruction.args[2]),
                        )
                        .with_observed(instruction.args[2].clone()),
                    );
                    None
                }
            }
        }
        "json_array_len" | "strlist_len" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!("{} expects one array/string-list operand", instruction.op),
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Int {
                signed: false,
                bits: 32,
            };
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "JSON_RESULT_TYPE",
                        format!("{} result type must be u32", instruction.op),
                    )
                    .with_expected("u32")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(found) if is_string_like_type(&found) || is_buf_u8_like_type(&found) => {
                    Some(expected)
                }
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "JSON_OPERAND_TYPE",
                            format!(
                                "{} expects own/view/edit[str] or own/view/edit[buf[u8]]",
                                instruction.op
                            ),
                        )
                        .with_expected("own[str] | view[str] | edit[str] | own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown {} operand {}", instruction.op, instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "json_index_u32" | "strlist_index_u32" => {
            if instruction.args.len() != 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!(
                            "{} expects an array/string-list operand and a u32 index",
                            instruction.op
                        ),
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Int {
                signed: false,
                bits: 32,
            };
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "JSON_RESULT_TYPE",
                        format!("{} result type must be u32", instruction.op),
                    )
                    .with_expected("u32")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(found) if is_string_like_type(&found) || is_buf_u8_like_type(&found) => {}
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "JSON_OPERAND_TYPE",
                            format!("{} expects own/view/edit[str] or own/view/edit[buf[u8]] as the first operand", instruction.op),
                        )
                        .with_expected("own[str] | view[str] | edit[str] | own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                        .with_observed(found.to_string()),
                    );
                    return None;
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown {} operand {}", instruction.op, instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    return None;
                }
            }
            match resolve_operand_type(&instruction.args[1], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 32,
                }) => Some(expected),
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "JSON_OPERAND_TYPE",
                            format!("{} expects u32 as the second operand", instruction.op),
                        )
                        .with_expected("u32")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown {} operand {}", instruction.op, instruction.args[1]),
                        )
                        .with_observed(instruction.args[1].clone()),
                    );
                    None
                }
            }
        }
        "json_index_bool" | "strlist_index_bool" => {
            if instruction.args.len() != 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!(
                            "{} expects an array/string-list operand and a u32 index",
                            instruction.op
                        ),
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "JSON_RESULT_TYPE",
                        format!("{} result type must be b1", instruction.op),
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(found) if is_string_like_type(&found) || is_buf_u8_like_type(&found) => {}
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "JSON_OPERAND_TYPE",
                            format!("{} expects own/view/edit[str] or own/view/edit[buf[u8]] as the first operand", instruction.op),
                        )
                        .with_expected("own[str] | view[str] | edit[str] | own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                        .with_observed(found.to_string()),
                    );
                    return None;
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown {} operand {}", instruction.op, instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    return None;
                }
            }
            match resolve_operand_type(&instruction.args[1], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 32,
                }) => Some(TypeRef::Bool),
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "JSON_OPERAND_TYPE",
                            format!("{} expects u32 as the second operand", instruction.op),
                        )
                        .with_expected("u32")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown {} operand {}", instruction.op, instruction.args[1]),
                        )
                        .with_observed(instruction.args[1].clone()),
                    );
                    None
                }
            }
        }
        "json_index_str" | "strlist_index_str" | "strmap_get_str" => {
            let arity = if instruction.op == "strmap_get_str" {
                2
            } else {
                2
            };
            if instruction.args.len() != arity {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!("{} expects two operands", instruction.op),
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Own(Box::new(TypeRef::String));
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "JSON_RESULT_TYPE",
                        format!("{} result type must be own[str]", instruction.op),
                    )
                    .with_expected("own[str]")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(found) if is_string_like_type(&found) || is_buf_u8_like_type(&found) => {}
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "JSON_OPERAND_TYPE",
                            format!("{} expects own/view/edit[str] or own/view/edit[buf[u8]] as the first operand", instruction.op),
                        )
                        .with_expected("own[str] | view[str] | edit[str] | own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                        .with_observed(found.to_string()),
                    );
                    return None;
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown {} operand {}", instruction.op, instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    return None;
                }
            }
            if instruction.op != "strmap_get_str" {
                match resolve_operand_type(&instruction.args[1], env) {
                    Some(TypeRef::Int {
                        signed: false,
                        bits: 32,
                    }) => Some(expected),
                    Some(found) => {
                        diagnostics.push(
                            Diagnostic::new(
                                "typecheck",
                                node.to_string(),
                                "JSON_OPERAND_TYPE",
                                format!("{} expects u32 as the second operand", instruction.op),
                            )
                            .with_expected("u32")
                            .with_observed(found.to_string()),
                        );
                        None
                    }
                    None => {
                        diagnostics.push(
                            Diagnostic::new(
                                "typecheck",
                                node.to_string(),
                                "UNKNOWN_OPERAND",
                                format!(
                                    "unknown {} operand {}",
                                    instruction.op, instruction.args[1]
                                ),
                            )
                            .with_observed(instruction.args[1].clone()),
                        );
                        None
                    }
                }
            } else {
                Some(expected)
            }
        }
        "strmap_get_u32" => {
            if instruction.args.len() != 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "strmap_get_u32 expects a string-map operand and a key token",
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Int {
                signed: false,
                bits: 32,
            };
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "JSON_RESULT_TYPE",
                        "strmap_get_u32 result type must be u32",
                    )
                    .with_expected("u32")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(found) if is_string_like_type(&found) || is_buf_u8_like_type(&found) => {
                    Some(expected)
                }
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "JSON_OPERAND_TYPE",
                            "strmap_get_u32 expects own/view/edit[str] or own/view/edit[buf[u8]]",
                        )
                        .with_expected("own[str] | view[str] | edit[str] | own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown strmap_get_u32 operand {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "strmap_get_bool" => {
            if instruction.args.len() != 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "strmap_get_bool expects a string-map operand and a key token",
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "JSON_RESULT_TYPE",
                        "strmap_get_bool result type must be b1",
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(found) if is_string_like_type(&found) || is_buf_u8_like_type(&found) => {
                    Some(TypeRef::Bool)
                }
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "JSON_OPERAND_TYPE",
                            "strmap_get_bool expects own/view/edit[str] or own/view/edit[buf[u8]]",
                        )
                        .with_expected("own[str] | view[str] | edit[str] | own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown strmap_get_bool operand {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "str_lit" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "str_lit expects one literal token",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Own(Box::new(TypeRef::String));
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "STRING_RESULT_TYPE",
                        "str_lit result type must be own[str]",
                    )
                    .with_expected("own[str]")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            Some(expected)
        }
        "str_concat" => {
            if instruction.args.len() != 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "str_concat expects two string operands",
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Own(Box::new(TypeRef::String));
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "STRING_RESULT_TYPE",
                        "str_concat result type must be own[str]",
                    )
                    .with_expected("own[str]")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            for operand in &instruction.args {
                match resolve_operand_type(operand, env) {
                    Some(found) if is_string_like_type(&found) => {}
                    Some(found) => {
                        diagnostics.push(
                            Diagnostic::new(
                                "typecheck",
                                node.to_string(),
                                "STRING_OPERAND_TYPE",
                                "str_concat expects own/view/edit[str] operands",
                            )
                            .with_expected("own[str] | view[str] | edit[str]")
                            .with_observed(found.to_string()),
                        );
                        return None;
                    }
                    None => {
                        diagnostics.push(
                            Diagnostic::new(
                                "typecheck",
                                node.to_string(),
                                "UNKNOWN_OPERAND",
                                format!("unknown str_concat operand {operand}"),
                            )
                            .with_observed(operand.clone()),
                        );
                        return None;
                    }
                }
            }
            Some(expected)
        }
        "str_from_u32" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "str_from_u32 expects one u32 operand",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Own(Box::new(TypeRef::String));
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "STRING_RESULT_TYPE",
                        "str_from_u32 result type must be own[str]",
                    )
                    .with_expected("own[str]")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 32,
                }) => Some(expected),
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "STRING_OPERAND_TYPE",
                            "str_from_u32 expects u32",
                        )
                        .with_expected("u32")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown str_from_u32 operand {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "str_from_bool" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "str_from_bool expects one b1 operand",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Own(Box::new(TypeRef::String));
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "STRING_RESULT_TYPE",
                        "str_from_bool result type must be own[str]",
                    )
                    .with_expected("own[str]")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Bool) => Some(expected),
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "STRING_OPERAND_TYPE",
                            "str_from_bool expects b1",
                        )
                        .with_expected("b1")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown str_from_bool operand {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "str_eq_lit" => {
            if instruction.args.len() != 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "str_eq_lit expects a string operand and a literal token",
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "STRING_RESULT_TYPE",
                        "str_eq_lit result type must be b1",
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(found) if is_string_like_type(&found) => Some(TypeRef::Bool),
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "STRING_OPERAND_TYPE",
                            "str_eq_lit expects own/view/edit[str]",
                        )
                        .with_expected("own[str] | view[str] | edit[str]")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown str_eq_lit operand {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "str_to_buf" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "str_to_buf expects one string operand",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Own(Box::new(TypeRef::Buf(Box::new(TypeRef::Int {
                signed: false,
                bits: 8,
            }))));
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "STRING_RESULT_TYPE",
                        "str_to_buf result type must be own[buf[u8]]",
                    )
                    .with_expected("own[buf[u8]]")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(found) if is_string_like_type(&found) => Some(expected),
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "STRING_OPERAND_TYPE",
                            "str_to_buf expects own/view/edit[str]",
                        )
                        .with_expected("own[str] | view[str] | edit[str]")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown str_to_buf operand {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "buf_to_str" | "buf_hex_str" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!("{} expects one buf[u8] operand", instruction.op),
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Own(Box::new(TypeRef::String));
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "STRING_RESULT_TYPE",
                        format!("{} result type must be own[str]", instruction.op),
                    )
                    .with_expected("own[str]")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(found) if is_buf_u8_like_type(&found) => Some(expected),
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "STRING_OPERAND_TYPE",
                            format!("{} expects own/view/edit[buf[u8]]", instruction.op),
                        )
                        .with_expected("own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown {} operand {}", instruction.op, instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "json_encode_obj" => {
            if instruction.args.len() < 2 || instruction.args.len() % 2 != 0 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "json_encode_obj expects alternating key token and value operands",
                    )
                    .with_expected("2,4,6,...")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Own(Box::new(TypeRef::String));
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "JSON_RESULT_TYPE",
                        "json_encode_obj result type must be own[str]",
                    )
                    .with_expected("own[str]")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            for operand in instruction.args.iter().skip(1).step_by(2) {
                match resolve_operand_type(operand, env) {
                    Some(
                        TypeRef::Int {
                            signed: false,
                            bits: 32,
                        }
                        | TypeRef::Bool,
                    ) => {}
                    Some(found) if is_string_like_type(&found) => {}
                    Some(found) => {
                        diagnostics.push(
                            Diagnostic::new(
                                "typecheck",
                                node.to_string(),
                                "JSON_OPERAND_TYPE",
                                "json_encode_obj values must be u32, b1, or string",
                            )
                            .with_expected("u32 | b1 | own[str] | view[str] | edit[str]")
                            .with_observed(found.to_string()),
                        );
                        return None;
                    }
                    None => {
                        diagnostics.push(
                            Diagnostic::new(
                                "typecheck",
                                node.to_string(),
                                "UNKNOWN_OPERAND",
                                format!("unknown json_encode_obj value operand {operand}"),
                            )
                            .with_observed(operand.clone()),
                        );
                        return None;
                    }
                }
            }
            Some(expected)
        }
        "json_encode_arr" => {
            if instruction.args.is_empty() {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "json_encode_arr expects at least one value operand",
                    )
                    .with_expected("1+")
                    .with_observed("0"),
                );
                return None;
            }
            let expected = TypeRef::Own(Box::new(TypeRef::Buf(Box::new(TypeRef::Int {
                signed: false,
                bits: 8,
            }))));
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "JSON_RESULT_TYPE",
                        "json_encode_arr result type must be own[buf[u8]]",
                    )
                    .with_expected("own[buf[u8]]")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            for operand in &instruction.args {
                match resolve_operand_type(operand, env) {
                    Some(
                        TypeRef::Int {
                            signed: false,
                            bits: 32,
                        }
                        | TypeRef::Bool,
                    ) => {}
                    Some(found) if is_string_like_type(&found) || is_buf_u8_like_type(&found) => {}
                    Some(found) => {
                        diagnostics.push(
                            Diagnostic::new(
                                "typecheck",
                                node.to_string(),
                                "JSON_OPERAND_TYPE",
                                "json_encode_arr values must be u32, b1, string, or buf[u8]",
                            )
                            .with_expected(
                                "u32 | b1 | own[str] | view[str] | edit[str] | own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]",
                            )
                            .with_observed(found.to_string()),
                        );
                        return None;
                    }
                    None => {
                        diagnostics.push(
                            Diagnostic::new(
                                "typecheck",
                                node.to_string(),
                                "UNKNOWN_OPERAND",
                                format!("unknown json_encode_arr operand {operand}"),
                            )
                            .with_observed(operand.clone()),
                        );
                        return None;
                    }
                }
            }
            Some(expected)
        }
        "config_get_u32" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "config_get_u32 expects one config key token",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Int {
                signed: false,
                bits: 32,
            };
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "CONFIG_RESULT_TYPE",
                        "config_get_u32 result type must be u32",
                    )
                    .with_expected("u32")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            Some(expected)
        }
        "config_get_bool" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "config_get_bool expects one config key token",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "CONFIG_RESULT_TYPE",
                        "config_get_bool result type must be b1",
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            Some(TypeRef::Bool)
        }
        "config_get_str" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "config_get_str expects one config key token",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Own(Box::new(TypeRef::String));
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "CONFIG_RESULT_TYPE",
                        "config_get_str result type must be own[str]",
                    )
                    .with_expected("own[str]")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            Some(expected)
        }
        "config_has" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "config_has expects one config key token",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "CONFIG_RESULT_TYPE",
                        "config_has result type must be b1",
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            Some(TypeRef::Bool)
        }
        "env_get_u32" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "env_get_u32 expects one variable name token",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Int {
                signed: false,
                bits: 32,
            };
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "ENV_RESULT_TYPE",
                        "env_get_u32 result type must be u32",
                    )
                    .with_expected("u32")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            Some(expected)
        }
        "env_get_bool" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "env_get_bool expects one variable name token",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "ENV_RESULT_TYPE",
                        "env_get_bool result type must be b1",
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            Some(TypeRef::Bool)
        }
        "env_get_str" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "env_get_str expects one variable name token",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Own(Box::new(TypeRef::String));
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "ENV_RESULT_TYPE",
                        "env_get_str result type must be own[str]",
                    )
                    .with_expected("own[str]")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            Some(expected)
        }
        "env_has" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "env_has expects one variable name token",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "ENV_RESULT_TYPE",
                        "env_has result type must be b1",
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            Some(TypeRef::Bool)
        }
        "buf_before_lit" | "buf_after_lit" | "buf_trim_ascii" => {
            let expected_arity = if instruction.op == "buf_trim_ascii" { 1 } else { 2 };
            if instruction.args.len() != expected_arity {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!(
                            "{} expects {} operand{}",
                            instruction.op,
                            expected_arity,
                            if expected_arity == 1 { "" } else { "s" }
                        ),
                    )
                    .with_expected(expected_arity.to_string())
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Own(Box::new(TypeRef::Buf(Box::new(TypeRef::Int {
                signed: false,
                bits: 8,
            }))));
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "BUF_RESULT_TYPE",
                        format!("{} result type must be own[buf[u8]]", instruction.op),
                    )
                    .with_expected("own[buf[u8]]")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(found) if is_buf_u8_like_type(&found) => Some(expected),
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "BUF_OPERAND_TYPE",
                            format!("{} expects own/view/edit[buf[u8]] as the first operand", instruction.op),
                        )
                        .with_expected("own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown {} operand {}", instruction.op, instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "date_parse_ymd" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "date_parse_ymd expects one string operand",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Int {
                signed: false,
                bits: 32,
            };
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "DATE_RESULT_TYPE",
                        "date_parse_ymd result type must be u32",
                    )
                    .with_expected("u32")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(found) if is_string_like_type(&found) => Some(expected),
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "DATE_OPERAND_TYPE",
                            "date_parse_ymd expects own/view/edit[str]",
                        )
                        .with_expected("own[str] | view[str] | edit[str]")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown date_parse_ymd operand {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "time_parse_hms" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "time_parse_hms expects one string operand",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Int {
                signed: false,
                bits: 32,
            };
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "TIME_RESULT_TYPE",
                        "time_parse_hms result type must be u32",
                    )
                    .with_expected("u32")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(found) if is_string_like_type(&found) => Some(expected),
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "TIME_OPERAND_TYPE",
                            "time_parse_hms expects own/view/edit[str]",
                        )
                        .with_expected("own[str] | view[str] | edit[str]")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown time_parse_hms operand {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "date_format_ymd" | "time_format_hms" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!("{} expects one u32 operand", instruction.op),
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Own(Box::new(TypeRef::String));
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "TIME_RESULT_TYPE",
                        format!("{} result type must be own[str]", instruction.op),
                    )
                    .with_expected("own[str]")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 32,
                }) => Some(expected),
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "TIME_OPERAND_TYPE",
                            format!("{} expects u32", instruction.op),
                        )
                        .with_expected("u32")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown {} operand {}", instruction.op, instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "db_open" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "db_open expects one database path token",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            match &instruction.ty {
                TypeRef::Int {
                    signed: false,
                    bits: 64,
                } => Some(instruction.ty.clone()),
                found => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "DB_RESULT_TYPE",
                            "db_open result type must be u64",
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    None
                }
            }
        }
        "tls_exchange_all" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "tls_exchange_all expects one request buf operand",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Own(Box::new(TypeRef::Buf(Box::new(TypeRef::Int {
                signed: false,
                bits: 8,
            }))));
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "NET_RESULT_TYPE",
                        "tls_exchange_all result type must be own[buf[u8]]",
                    )
                    .with_expected("own[buf[u8]]")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Own(inner))
                | Some(TypeRef::View(inner))
                | Some(TypeRef::Edit(inner))
                    if matches!(
                        inner.as_ref(),
                        TypeRef::Buf(elem)
                            if **elem
                                == TypeRef::Int {
                                    signed: false,
                                    bits: 8,
                                }
                    ) =>
                {
                    Some(expected)
                }
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "NET_OPERAND_TYPE",
                            "tls_exchange_all expects own/view/edit[buf[u8]]",
                        )
                        .with_expected("own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown tls_exchange_all operand {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "db_close" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "db_close expects one database handle operand",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "DB_RESULT_TYPE",
                        "db_close result type must be b1",
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64,
                }) => Some(TypeRef::Bool),
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "DB_OPERAND_TYPE",
                            "db_close expects u64 database handle operand",
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown db_close operand {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "db_exec" | "db_query_u32" | "db_query_buf" | "db_query_row" => {
            if instruction.args.len() != 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!(
                            "{} expects a database handle and one sql operand",
                            instruction.op
                        ),
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            match instruction.op.as_str() {
                "db_exec" if instruction.ty != TypeRef::Bool => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "DB_RESULT_TYPE",
                            "db_exec result type must be b1",
                        )
                        .with_expected("b1")
                        .with_observed(instruction.ty.to_string()),
                    );
                    return None;
                }
                "db_query_u32"
                    if instruction.ty
                        != (TypeRef::Int {
                            signed: false,
                            bits: 32,
                        }) =>
                {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "DB_RESULT_TYPE",
                            "db_query_u32 result type must be u32",
                        )
                        .with_expected("u32")
                        .with_observed(instruction.ty.to_string()),
                    );
                    return None;
                }
                "db_query_buf" => {
                    let expected = TypeRef::Own(Box::new(TypeRef::Buf(Box::new(TypeRef::Int {
                        signed: false,
                        bits: 8,
                    }))));
                    if instruction.ty != expected {
                        diagnostics.push(
                            Diagnostic::new(
                                "typecheck",
                                node.to_string(),
                                "DB_RESULT_TYPE",
                                "db_query_buf result type must be own[buf[u8]]",
                            )
                            .with_expected("own[buf[u8]]")
                            .with_observed(instruction.ty.to_string()),
                        );
                        return None;
                    }
                }
                "db_query_row" => {
                    let expected = TypeRef::Own(Box::new(TypeRef::Buf(Box::new(TypeRef::Int {
                        signed: false,
                        bits: 8,
                    }))));
                    if instruction.ty != expected {
                        diagnostics.push(
                            Diagnostic::new(
                                "typecheck",
                                node.to_string(),
                                "DB_RESULT_TYPE",
                                "db_query_row result type must be own[buf[u8]]",
                            )
                            .with_expected("own[buf[u8]]")
                            .with_observed(instruction.ty.to_string()),
                        );
                        return None;
                    }
                }
                _ => {}
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64,
                }) => {}
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "DB_OPERAND_TYPE",
                            format!(
                                "{} expects a u64 database handle as the first operand",
                                instruction.op
                            ),
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    return None;
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown {} operand {}", instruction.op, instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    return None;
                }
            }
            match resolve_operand_type(&instruction.args[1], env) {
                Some(found) if is_buf_u8_like_type(&found) || is_string_like_type(&found) => {
                    Some(instruction.ty.clone())
                }
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "DB_OPERAND_TYPE",
                            format!(
                                "{} expects own/view/edit[buf[u8]] or own/view/edit[str] as the second operand",
                                instruction.op
                            ),
                        )
                        .with_expected(
                            "own[buf[u8]] | view[buf[u8]] | edit[buf[u8]] | own[str] | view[str] | edit[str]",
                        )
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown {} operand {}", instruction.op, instruction.args[1]),
                        )
                        .with_observed(instruction.args[1].clone()),
                    );
                    None
                }
            }
        }
        "db_prepare"
        | "db_exec_prepared"
        | "db_query_prepared_u32"
        | "db_query_prepared_buf"
        | "db_query_prepared_row" => {
            if instruction.args.len() != 3 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!(
                            "{} expects a database handle, statement token, and sql/params operand",
                            instruction.op
                        ),
                    )
                    .with_expected("3")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            match instruction.op.as_str() {
                "db_prepare" | "db_exec_prepared" if instruction.ty != TypeRef::Bool => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "DB_RESULT_TYPE",
                            format!("{} result type must be b1", instruction.op),
                        )
                        .with_expected("b1")
                        .with_observed(instruction.ty.to_string()),
                    );
                    return None;
                }
                "db_query_prepared_u32"
                    if instruction.ty
                        != (TypeRef::Int {
                            signed: false,
                            bits: 32,
                        }) =>
                {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "DB_RESULT_TYPE",
                            "db_query_prepared_u32 result type must be u32",
                        )
                        .with_expected("u32")
                        .with_observed(instruction.ty.to_string()),
                    );
                    return None;
                }
                "db_query_prepared_buf" => {
                    let expected = TypeRef::Own(Box::new(TypeRef::Buf(Box::new(TypeRef::Int {
                        signed: false,
                        bits: 8,
                    }))));
                    if instruction.ty != expected {
                        diagnostics.push(
                            Diagnostic::new(
                                "typecheck",
                                node.to_string(),
                                "DB_RESULT_TYPE",
                                "db_query_prepared_buf result type must be own[buf[u8]]",
                            )
                            .with_expected("own[buf[u8]]")
                            .with_observed(instruction.ty.to_string()),
                        );
                        return None;
                    }
                }
                "db_query_prepared_row" => {
                    let expected = TypeRef::Own(Box::new(TypeRef::Buf(Box::new(TypeRef::Int {
                        signed: false,
                        bits: 8,
                    }))));
                    if instruction.ty != expected {
                        diagnostics.push(
                            Diagnostic::new(
                                "typecheck",
                                node.to_string(),
                                "DB_RESULT_TYPE",
                                "db_query_prepared_row result type must be own[buf[u8]]",
                            )
                            .with_expected("own[buf[u8]]")
                            .with_observed(instruction.ty.to_string()),
                        );
                        return None;
                    }
                }
                _ => {}
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64,
                }) => {}
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "DB_OPERAND_TYPE",
                            format!(
                                "{} expects a u64 database handle as the first operand",
                                instruction.op
                            ),
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    return None;
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown {} operand {}", instruction.op, instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    return None;
                }
            }
            match resolve_operand_type(&instruction.args[2], env) {
                Some(found) if is_buf_u8_like_type(&found) || is_string_like_type(&found) => {
                    Some(instruction.ty.clone())
                }
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "DB_OPERAND_TYPE",
                            format!(
                                "{} expects own/view/edit[buf[u8]] or own/view/edit[str] as the third operand",
                                instruction.op
                            ),
                        )
                        .with_expected(
                            "own[buf[u8]] | view[buf[u8]] | edit[buf[u8]] | own[str] | view[str] | edit[str]",
                        )
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown {} operand {}", instruction.op, instruction.args[2]),
                        )
                        .with_observed(instruction.args[2].clone()),
                    );
                    None
                }
            }
        }
        "db_row_found" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "db_row_found expects one row operand",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "DB_RESULT_TYPE",
                        "db_row_found result type must be b1",
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(found) if is_buf_u8_like_type(&found) => Some(TypeRef::Bool),
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "DB_OPERAND_TYPE",
                            "db_row_found expects own/view/edit[buf[u8]]",
                        )
                        .with_expected("own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown db_row_found operand {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "db_last_error_code" | "db_last_error_retryable" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!("{} expects one database handle operand", instruction.op),
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = if instruction.op == "db_last_error_code" {
                TypeRef::Int {
                    signed: false,
                    bits: 32,
                }
            } else {
                TypeRef::Bool
            };
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "DB_RESULT_TYPE",
                        format!("{} result type mismatch", instruction.op),
                    )
                    .with_expected(expected.to_string())
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64,
                }) => Some(expected),
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "DB_OPERAND_TYPE",
                            format!("{} expects u64 database handle operand", instruction.op),
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown {} operand {}", instruction.op, instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "db_begin"
        | "db_commit"
        | "db_rollback"
        | "db_pool_acquire"
        | "db_pool_close"
        | "db_pool_leased" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!("{} expects one handle operand", instruction.op),
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let (expected_handle, expected_result) = match instruction.op.as_str() {
                "db_pool_acquire" => (
                    TypeRef::Int {
                        signed: false,
                        bits: 64,
                    },
                    TypeRef::Int {
                        signed: false,
                        bits: 64,
                    },
                ),
                "db_pool_leased" => (
                    TypeRef::Int {
                        signed: false,
                        bits: 64,
                    },
                    TypeRef::Int {
                        signed: false,
                        bits: 32,
                    },
                ),
                _ => (
                    TypeRef::Int {
                        signed: false,
                        bits: 64,
                    },
                    TypeRef::Bool,
                ),
            };
            if instruction.ty != expected_result {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "DB_RESULT_TYPE",
                        format!("{} result type mismatch", instruction.op),
                    )
                    .with_expected(expected_result.to_string())
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(found) if found == expected_handle => Some(instruction.ty.clone()),
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "DB_OPERAND_TYPE",
                            format!("{} expects u64 handle operand", instruction.op),
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown {} operand {}", instruction.op, instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "db_pool_set_max_idle" => {
            if instruction.args.len() != 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "db_pool_set_max_idle expects pool handle and u32 max idle",
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "DB_RESULT_TYPE",
                        "db_pool_set_max_idle result type must be b1",
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64,
                }) => {}
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "DB_OPERAND_TYPE",
                            "db_pool_set_max_idle expects u64 pool handle",
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    return None;
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown db_pool_set_max_idle operand {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    return None;
                }
            }
            match resolve_operand_type(&instruction.args[1], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 32,
                }) => Some(TypeRef::Bool),
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "DB_OPERAND_TYPE",
                            "db_pool_set_max_idle expects u32 max idle",
                        )
                        .with_expected("u32")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown db_pool_set_max_idle operand {}", instruction.args[1]),
                        )
                        .with_observed(instruction.args[1].clone()),
                    );
                    None
                }
            }
        }
        "db_pool_release" => {
            if instruction.args.len() != 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "db_pool_release expects pool and db handle operands",
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "DB_RESULT_TYPE",
                        "db_pool_release result type must be b1",
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            for (index, operand) in instruction.args.iter().enumerate() {
                match resolve_operand_type(operand, env) {
                    Some(TypeRef::Int {
                        signed: false,
                        bits: 64,
                    }) => {}
                    Some(found) => {
                        diagnostics.push(
                            Diagnostic::new(
                                "typecheck",
                                node.to_string(),
                                "DB_OPERAND_TYPE",
                                format!("db_pool_release expects u64 handle operand {}", index),
                            )
                            .with_expected("u64")
                            .with_observed(found.to_string()),
                        );
                        return None;
                    }
                    None => {
                        diagnostics.push(
                            Diagnostic::new(
                                "typecheck",
                                node.to_string(),
                                "UNKNOWN_OPERAND",
                                format!("unknown db_pool_release operand {}", operand),
                            )
                            .with_observed(operand.clone()),
                        );
                        return None;
                    }
                }
            }
            Some(TypeRef::Bool)
        }
        "db_pool_open" => {
            if instruction.args.len() != 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "db_pool_open expects target token and max size",
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Int {
                signed: false,
                bits: 64,
            };
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "DB_RESULT_TYPE",
                        "db_pool_open result type must be u64",
                    )
                    .with_expected("u64")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[1], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 32,
                }) => Some(expected),
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "DB_OPERAND_TYPE",
                            "db_pool_open expects u32 max size as the second operand",
                        )
                        .with_expected("u32")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown db_pool_open operand {}", instruction.args[1]),
                        )
                        .with_observed(instruction.args[1].clone()),
                    );
                    None
                }
            }
        }
        "cache_open" | "queue_open" | "stream_open" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!("{} expects one target token", instruction.op),
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Int {
                signed: false,
                bits: 64,
            };
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "RESULT_TYPE",
                        format!("{} result type must be u64", instruction.op),
                    )
                    .with_expected("u64")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            Some(expected)
        }
        "cache_close" | "queue_close" | "queue_push_buf" | "cache_del" | "stream_close" => {
            let expected_arity = if instruction.op == "queue_push_buf"
                || instruction.op == "cache_del"
            {
                2
            } else {
                1
            };
            if instruction.args.len() != expected_arity {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!("{} arity mismatch", instruction.op),
                    )
                    .with_expected(expected_arity.to_string())
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "RESULT_TYPE",
                        format!("{} result type must be b1", instruction.op),
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64,
                }) => {}
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "OPERAND_TYPE",
                            format!("{} expects u64 handle operand", instruction.op),
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    return None;
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown {} operand {}", instruction.op, instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    return None;
                }
            }
            if expected_arity == 2 {
                match resolve_operand_type(&instruction.args[1], env) {
                    Some(found) if is_buf_u8_like_type(&found) => Some(TypeRef::Bool),
                    Some(found) => {
                        diagnostics.push(
                            Diagnostic::new(
                                "typecheck",
                                node.to_string(),
                                "OPERAND_TYPE",
                                format!("{} expects own/view/edit[buf[u8]] payload/key", instruction.op),
                            )
                            .with_expected("own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                            .with_observed(found.to_string()),
                        );
                        None
                    }
                    None => {
                        diagnostics.push(
                            Diagnostic::new(
                                "typecheck",
                                node.to_string(),
                                "UNKNOWN_OPERAND",
                                format!("unknown {} operand {}", instruction.op, instruction.args[1]),
                            )
                            .with_observed(instruction.args[1].clone()),
                        );
                        None
                    }
                }
            } else {
                Some(TypeRef::Bool)
            }
        }
        "stream_publish_buf" => {
            if instruction.args.len() != 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "stream_publish_buf expects handle and buf[u8] payload",
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Int {
                signed: false,
                bits: 32,
            };
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "RESULT_TYPE",
                        "stream_publish_buf result type must be u32",
                    )
                    .with_expected("u32")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            let handle_ok = matches!(
                resolve_operand_type(&instruction.args[0], env),
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64,
                })
            );
            let payload_ok = matches!(
                resolve_operand_type(&instruction.args[1], env),
                Some(found) if is_buf_u8_like_type(&found)
            );
            if !handle_ok || !payload_ok {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "OPERAND_TYPE",
                        "stream_publish_buf expects (u64 handle, own/view/edit[buf[u8]])",
                    )
                    .with_expected("u64, own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                    .with_observed(
                        resolve_operand_type(&instruction.args[0], env)
                            .map(|ty| ty.to_string())
                            .unwrap_or_else(|| "unknown".to_string()),
                    ),
                );
                return None;
            }
            Some(expected)
        }
        "cache_get_buf" | "queue_pop_buf" | "stream_replay_next" => {
            let expected_arity = if instruction.op == "cache_get_buf" { 2 } else { 1 };
            if instruction.args.len() != expected_arity {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!("{} arity mismatch", instruction.op),
                    )
                    .with_expected(expected_arity.to_string())
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Own(Box::new(TypeRef::Buf(Box::new(TypeRef::Int {
                signed: false,
                bits: 8,
            }))));
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "RESULT_TYPE",
                        format!("{} result type must be own[buf[u8]]", instruction.op),
                    )
                    .with_expected("own[buf[u8]]")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64,
                }) => {}
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "OPERAND_TYPE",
                            format!("{} expects u64 handle operand", instruction.op),
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    return None;
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown {} operand {}", instruction.op, instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    return None;
                }
            }
            if expected_arity == 2 {
                match resolve_operand_type(&instruction.args[1], env) {
                    Some(found) if is_buf_u8_like_type(&found) => Some(expected),
                    Some(found) => {
                        diagnostics.push(
                            Diagnostic::new(
                                "typecheck",
                                node.to_string(),
                                "OPERAND_TYPE",
                                "cache_get_buf expects own/view/edit[buf[u8]] key",
                            )
                            .with_expected("own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                            .with_observed(found.to_string()),
                        );
                        None
                    }
                    None => {
                        diagnostics.push(
                            Diagnostic::new(
                                "typecheck",
                                node.to_string(),
                                "UNKNOWN_OPERAND",
                                format!("unknown cache_get_buf operand {}", instruction.args[1]),
                            )
                            .with_observed(instruction.args[1].clone()),
                        );
                        None
                    }
                }
            } else {
                Some(expected)
            }
        }
        "cache_set_buf" | "cache_set_buf_ttl" => {
            let expected_arity = if instruction.op == "cache_set_buf" { 3 } else { 4 };
            if instruction.args.len() != expected_arity {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!("{} arity mismatch", instruction.op),
                    )
                    .with_expected(expected_arity.to_string())
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "RESULT_TYPE",
                        format!("{} result type must be b1", instruction.op),
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64,
                }) => {}
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "OPERAND_TYPE",
                            format!("{} expects u64 handle operand", instruction.op),
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    return None;
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown {} operand {}", instruction.op, instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    return None;
                }
            }
            for index in 1..instruction.args.len() {
                if instruction.op == "cache_set_buf_ttl" && index == 2 {
                    match resolve_operand_type(&instruction.args[index], env) {
                        Some(TypeRef::Int {
                            signed: false,
                            bits: 32,
                        }) => {}
                        Some(found) => {
                            diagnostics.push(
                                Diagnostic::new(
                                    "typecheck",
                                    node.to_string(),
                                    "OPERAND_TYPE",
                                    "cache_set_buf_ttl expects u32 ttl as the third operand",
                                )
                                .with_expected("u32")
                                .with_observed(found.to_string()),
                            );
                            return None;
                        }
                        None => {
                            diagnostics.push(
                                Diagnostic::new(
                                    "typecheck",
                                    node.to_string(),
                                    "UNKNOWN_OPERAND",
                                    format!("unknown {} operand {}", instruction.op, instruction.args[index]),
                                )
                                .with_observed(instruction.args[index].clone()),
                            );
                            return None;
                        }
                    }
                } else {
                    match resolve_operand_type(&instruction.args[index], env) {
                        Some(found) if is_buf_u8_like_type(&found) => {}
                        Some(found) => {
                            diagnostics.push(
                                Diagnostic::new(
                                    "typecheck",
                                    node.to_string(),
                                    "OPERAND_TYPE",
                                    format!("{} expects own/view/edit[buf[u8]] key/value operands", instruction.op),
                                )
                                .with_expected("own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                                .with_observed(found.to_string()),
                            );
                            return None;
                        }
                        None => {
                            diagnostics.push(
                                Diagnostic::new(
                                    "typecheck",
                                    node.to_string(),
                                    "UNKNOWN_OPERAND",
                                    format!("unknown {} operand {}", instruction.op, instruction.args[index]),
                                )
                                .with_observed(instruction.args[index].clone()),
                            );
                            return None;
                        }
                    }
                }
            }
            Some(TypeRef::Bool)
        }
        "queue_len" | "stream_len" | "stream_replay_offset" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!("{} expects one handle operand", instruction.op),
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Int {
                signed: false,
                bits: 32,
            };
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "RESULT_TYPE",
                        format!("{} result type must be u32", instruction.op),
                    )
                    .with_expected("u32")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64,
                }) => Some(expected),
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "OPERAND_TYPE",
                            format!("{} expects u64 handle operand", instruction.op),
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown {} operand {}", instruction.op, instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "stream_replay_open" => {
            if instruction.args.len() != 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "stream_replay_open expects handle and u32 offset",
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Int {
                signed: false,
                bits: 64,
            };
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "RESULT_TYPE",
                        "stream_replay_open result type must be u64",
                    )
                    .with_expected("u64")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            let handle_ok = matches!(
                resolve_operand_type(&instruction.args[0], env),
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64,
                })
            );
            let offset_ok = matches!(
                resolve_operand_type(&instruction.args[1], env),
                Some(TypeRef::Int {
                    signed: false,
                    bits: 32,
                })
            );
            if !handle_ok || !offset_ok {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "OPERAND_TYPE",
                        "stream_replay_open expects (u64, u32)",
                    )
                    .with_expected("u64, u32")
                    .with_observed(
                        resolve_operand_type(&instruction.args[0], env)
                            .map(|ty| ty.to_string())
                            .unwrap_or_else(|| "unknown".to_string()),
                    ),
                );
                return None;
            }
            Some(expected)
        }
        "batch_open" | "agg_open_u64" => {
            if !instruction.args.is_empty() {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!("{} expects 0 operands", instruction.op),
                    )
                    .with_expected("0")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Int {
                signed: false,
                bits: 64,
            };
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "RESULT_TYPE",
                        format!("{} result type must be u64", instruction.op),
                    )
                    .with_expected("u64")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            Some(expected)
        }
        "batch_push_u64" | "agg_add_u64" | "window_add_u64" => {
            if instruction.args.len() != 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!("{} expects handle and u64 value", instruction.op),
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "RESULT_TYPE",
                        format!("{} result type must be b1", instruction.op),
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            let handle_ok = matches!(
                resolve_operand_type(&instruction.args[0], env),
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64,
                })
            );
            let value_ok = matches!(
                resolve_operand_type(&instruction.args[1], env),
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64,
                })
            );
            if !handle_ok || !value_ok {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "OPERAND_TYPE",
                        format!("{} expects (u64 handle, u64 value)", instruction.op),
                    )
                    .with_expected("u64, u64")
                    .with_observed(
                        resolve_operand_type(&instruction.args[0], env)
                            .map(|ty| ty.to_string())
                            .unwrap_or_else(|| "unknown".to_string()),
                    ),
                );
                return None;
            }
            Some(TypeRef::Bool)
        }
        "batch_len" | "agg_count" | "window_count" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!("{} expects one handle operand", instruction.op),
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Int {
                signed: false,
                bits: 32,
            };
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "RESULT_TYPE",
                        format!("{} result type must be u32", instruction.op),
                    )
                    .with_expected("u32")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64,
                }) => Some(expected),
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "OPERAND_TYPE",
                            format!("{} expects u64 handle operand", instruction.op),
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown {} operand {}", instruction.op, instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "batch_flush_sum_u64"
        | "agg_sum_u64"
        | "agg_avg_u64"
        | "agg_min_u64"
        | "agg_max_u64"
        | "window_sum_u64"
        | "window_avg_u64"
        | "window_min_u64"
        | "window_max_u64" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!("{} expects one handle operand", instruction.op),
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Int {
                signed: false,
                bits: 64,
            };
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "RESULT_TYPE",
                        format!("{} result type must be u64", instruction.op),
                    )
                    .with_expected("u64")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64,
                }) => Some(expected),
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "OPERAND_TYPE",
                            format!("{} expects u64 handle operand", instruction.op),
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown {} operand {}", instruction.op, instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "stream_replay_close" | "batch_close" | "agg_close" | "window_close" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!("{} expects one handle operand", instruction.op),
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "RESULT_TYPE",
                        format!("{} result type must be b1", instruction.op),
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64,
                }) => Some(TypeRef::Bool),
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "OPERAND_TYPE",
                            format!("{} expects u64 handle operand", instruction.op),
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown {} operand {}", instruction.op, instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "shard_route_u32" => {
            if instruction.args.len() != 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "shard_route_u32 expects key and u32 shard count",
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Int {
                signed: false,
                bits: 32,
            };
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "RESULT_TYPE",
                        "shard_route_u32 result type must be u32",
                    )
                    .with_expected("u32")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            let key_ok = match resolve_operand_type(&instruction.args[0], env) {
                Some(found) => is_buf_u8_like_type(&found),
                None => false,
            };
            let shard_ok = matches!(
                resolve_operand_type(&instruction.args[1], env),
                Some(TypeRef::Int {
                    signed: false,
                    bits: 32,
                })
            );
            if !key_ok || !shard_ok {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "OPERAND_TYPE",
                        "shard_route_u32 expects (own/view/edit[buf[u8]], u32)",
                    )
                    .with_expected("own[buf[u8]] | view[buf[u8]] | edit[buf[u8]], u32"),
                );
                return None;
            }
            Some(expected)
        }
        "lease_open" | "placement_open" | "coord_open" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!("{} expects one target token", instruction.op),
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Int {
                signed: false,
                bits: 64,
            };
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "RESULT_TYPE",
                        format!("{} result type must be u64", instruction.op),
                    )
                    .with_expected("u64")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            Some(expected)
        }
        "lease_owner" | "placement_lookup" | "coord_load_u32" => {
            let expected_arity = if instruction.op == "lease_owner" { 1 } else { 2 };
            if instruction.args.len() != expected_arity {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!("{} arity mismatch", instruction.op),
                    )
                    .with_expected(expected_arity.to_string())
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Int {
                signed: false,
                bits: 32,
            };
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "RESULT_TYPE",
                        format!("{} result type must be u32", instruction.op),
                    )
                    .with_expected("u32")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            let handle_ok = matches!(
                resolve_operand_type(&instruction.args[0], env),
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64,
                })
            );
            let extra_ok = match instruction.op.as_str() {
                "lease_owner" => true,
                "placement_lookup" => matches!(
                    resolve_operand_type(&instruction.args[1], env),
                    Some(TypeRef::Int {
                        signed: false,
                        bits: 32,
                    })
                ),
                "coord_load_u32" => true,
                _ => false,
            };
            if !handle_ok || !extra_ok {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "OPERAND_TYPE",
                        format!("{} operand types are invalid", instruction.op),
                    )
                    .with_expected(match instruction.op.as_str() {
                        "lease_owner" => "u64",
                        "placement_lookup" => "u64, u32",
                        "coord_load_u32" => "u64, token",
                        _ => "u64",
                    }),
                );
                return None;
            }
            Some(expected)
        }
        "lease_acquire" | "lease_transfer" | "lease_release" => {
            if instruction.args.len() != 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!("{} expects handle and owner", instruction.op),
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "RESULT_TYPE",
                        format!("{} result type must be b1", instruction.op),
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            let handle_ok = matches!(
                resolve_operand_type(&instruction.args[0], env),
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64,
                })
            );
            let owner_ok = matches!(
                resolve_operand_type(&instruction.args[1], env),
                Some(TypeRef::Int {
                    signed: false,
                    bits: 32,
                })
            );
            if !handle_ok || !owner_ok {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "OPERAND_TYPE",
                        format!("{} expects (u64, u32)", instruction.op),
                    )
                    .with_expected("u64, u32"),
                );
                return None;
            }
            Some(TypeRef::Bool)
        }
        "lease_close" | "placement_close" | "coord_close" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!("{} expects one handle operand", instruction.op),
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "RESULT_TYPE",
                        format!("{} result type must be b1", instruction.op),
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64,
                }) => Some(TypeRef::Bool),
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "OPERAND_TYPE",
                            format!("{} expects u64 handle operand", instruction.op),
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown {} operand {}", instruction.op, instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "placement_assign" | "coord_store_u32" => {
            let expected_arity = if instruction.op == "placement_assign" { 3 } else { 3 };
            if instruction.args.len() != expected_arity {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!("{} arity mismatch", instruction.op),
                    )
                    .with_expected(expected_arity.to_string())
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "RESULT_TYPE",
                        format!("{} result type must be b1", instruction.op),
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            let handle_ok = matches!(
                resolve_operand_type(&instruction.args[0], env),
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64,
                })
            );
            let arg1_ok = if instruction.op == "placement_assign" {
                matches!(
                    resolve_operand_type(&instruction.args[1], env),
                    Some(TypeRef::Int {
                        signed: false,
                        bits: 32,
                    })
                )
            } else {
                true
            };
            let arg2_ok = matches!(
                resolve_operand_type(&instruction.args[2], env),
                Some(TypeRef::Int {
                    signed: false,
                    bits: 32,
                })
            );
            if !handle_ok || !arg1_ok || !arg2_ok {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "OPERAND_TYPE",
                        format!("{} operand types are invalid", instruction.op),
                    )
                    .with_expected(if instruction.op == "placement_assign" {
                        "u64, u32, u32"
                    } else {
                        "u64, token, u32"
                    }),
                );
                return None;
            }
            Some(TypeRef::Bool)
        }
        "window_open_ms" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "window_open_ms expects one u32 width operand",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Int {
                signed: false,
                bits: 64,
            };
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "RESULT_TYPE",
                        "window_open_ms result type must be u64",
                    )
                    .with_expected("u64")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 32,
                }) => Some(expected),
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "OPERAND_TYPE",
                            "window_open_ms expects u32 width operand",
                        )
                        .with_expected("u32")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown window_open_ms operand {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "msg_log_open" => {
            if !instruction.args.is_empty() {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "msg_log_open expects 0 operands",
                    )
                    .with_expected("0")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Int { signed: false, bits: 64 };
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "RESULT_TYPE",
                        "msg_log_open result type must be u64",
                    )
                    .with_expected("u64")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            Some(expected)
        }
        "msg_log_close" | "msg_subscribe" | "msg_ack" | "msg_mark_retry" | "msg_replay_close" => {
            let expected_arity = match instruction.op.as_str() {
                "msg_log_close" | "msg_replay_close" => 1,
                "msg_subscribe" => 3,
                _ => 3,
            };
            if instruction.args.len() != expected_arity {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!("{} arity mismatch", instruction.op),
                    )
                    .with_expected(expected_arity.to_string())
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "RESULT_TYPE",
                        format!("{} result type must be b1", instruction.op),
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            let handle_ok = matches!(
                resolve_operand_type(&instruction.args[0], env),
                Some(TypeRef::Int { signed: false, bits: 64 })
            );
            let seq_ok = if matches!(instruction.op.as_str(), "msg_ack" | "msg_mark_retry") {
                matches!(
                    resolve_operand_type(&instruction.args[2], env),
                    Some(TypeRef::Int { signed: false, bits: 32 })
                )
            } else {
                true
            };
            if !handle_ok || !seq_ok {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "OPERAND_TYPE",
                        format!("{} expects u64 handle and optional u32 seq", instruction.op),
                    )
                    .with_expected("u64 [, token] [, u32]")
                    .with_observed(
                        resolve_operand_type(&instruction.args[0], env)
                            .map(|ty| ty.to_string())
                            .unwrap_or_else(|| "unknown".to_string()),
                    ),
                );
                return None;
            }
            Some(TypeRef::Bool)
        }
        "msg_send" | "msg_send_dedup" | "msg_fanout" => {
            let expected_arity = match instruction.op.as_str() {
                "msg_send" => 4,
                "msg_send_dedup" => 5,
                _ => 3,
            };
            if instruction.args.len() != expected_arity {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!("{} arity mismatch", instruction.op),
                    )
                    .with_expected(expected_arity.to_string())
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Int { signed: false, bits: 32 };
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "RESULT_TYPE",
                        format!("{} result type must be u32", instruction.op),
                    )
                    .with_expected("u32")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            let handle_ok = matches!(
                resolve_operand_type(&instruction.args[0], env),
                Some(TypeRef::Int { signed: false, bits: 64 })
            );
            let payload_index = if instruction.op == "msg_send_dedup" { 4 } else { expected_arity - 1 };
            let payload_ok = match resolve_operand_type(&instruction.args[payload_index], env) {
                Some(found) => is_buf_u8_like_type(&found),
                None => false,
            };
            let key_ok = if instruction.op == "msg_send_dedup" {
                match resolve_operand_type(&instruction.args[3], env) {
                    Some(found) => is_buf_u8_like_type(&found),
                    None => false,
                }
            } else {
                true
            };
            if !handle_ok || !payload_ok || !key_ok {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "OPERAND_TYPE",
                        format!("{} expects u64 handle and buf[u8] payload operands", instruction.op),
                    )
                    .with_expected("u64, token..., own/view/edit[buf[u8]]")
                    .with_observed(
                        resolve_operand_type(&instruction.args[0], env)
                            .map(|ty| ty.to_string())
                            .unwrap_or_else(|| "unknown".to_string()),
                    ),
                );
                return None;
            }
            Some(expected)
        }
        "msg_recv_next" | "msg_replay_next" => {
            let expected_arity = if instruction.op == "msg_recv_next" { 2 } else { 1 };
            if instruction.args.len() != expected_arity {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!("{} arity mismatch", instruction.op),
                    )
                    .with_expected(expected_arity.to_string())
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Own(Box::new(TypeRef::Buf(Box::new(TypeRef::Int {
                signed: false,
                bits: 8,
            }))));
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "RESULT_TYPE",
                        format!("{} result type must be own[buf[u8]]", instruction.op),
                    )
                    .with_expected("own[buf[u8]]")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int { signed: false, bits: 64 }) => Some(expected),
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "OPERAND_TYPE",
                            "msg_recv_next expects u64 handle as first operand",
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown msg_recv_next operand {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "msg_recv_seq" | "msg_retry_count" | "msg_pending_count" | "msg_delivery_total" | "msg_failure_class" | "msg_subscriber_count" | "msg_replay_seq" => {
            let expected_arity = match instruction.op.as_str() {
                "msg_failure_class" | "msg_replay_seq" => 1,
                "msg_retry_count" => 3,
                _ => 2,
            };
            if instruction.args.len() != expected_arity {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!("{} arity mismatch", instruction.op),
                    )
                    .with_expected(expected_arity.to_string())
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Int { signed: false, bits: 32 };
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "RESULT_TYPE",
                        format!("{} result type must be u32", instruction.op),
                    )
                    .with_expected("u32")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            let handle_ok = matches!(
                resolve_operand_type(&instruction.args[0], env),
                Some(TypeRef::Int { signed: false, bits: 64 })
            );
            let seq_ok = if instruction.op == "msg_retry_count" {
                matches!(
                    resolve_operand_type(&instruction.args[2], env),
                    Some(TypeRef::Int { signed: false, bits: 32 })
                )
            } else {
                true
            };
            if !handle_ok || !seq_ok {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "OPERAND_TYPE",
                        format!("{} expects u64 handle and optional u32 seq", instruction.op),
                    )
                    .with_expected("u64 [, token] [, u32]")
                    .with_observed(
                        resolve_operand_type(&instruction.args[0], env)
                            .map(|ty| ty.to_string())
                            .unwrap_or_else(|| "unknown".to_string()),
                    ),
                );
                return None;
            }
            Some(expected)
        }
        "msg_replay_open" => {
            if instruction.args.len() != 3 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "msg_replay_open expects handle, recipient token, and u32 from_seq",
                    )
                    .with_expected("3")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Int { signed: false, bits: 64 };
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "RESULT_TYPE",
                        "msg_replay_open result type must be u64",
                    )
                    .with_expected("u64")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            let handle_ok = matches!(
                resolve_operand_type(&instruction.args[0], env),
                Some(TypeRef::Int { signed: false, bits: 64 })
            );
            let seq_ok = matches!(
                resolve_operand_type(&instruction.args[2], env),
                Some(TypeRef::Int { signed: false, bits: 32 })
            );
            if !handle_ok || !seq_ok {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "OPERAND_TYPE",
                        "msg_replay_open expects (u64, token, u32)",
                    )
                    .with_expected("u64, token, u32")
                    .with_observed(
                        resolve_operand_type(&instruction.args[0], env)
                            .map(|ty| ty.to_string())
                            .unwrap_or_else(|| "unknown".to_string()),
                    ),
                );
                return None;
            }
            Some(expected)
        }
        "spawn_capture_all" => {
            if instruction.args.is_empty() {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "spawn_capture_all expects at least one command token",
                    )
                    .with_expected(">=1")
                    .with_observed("0"),
                );
                return None;
            }
            if validate_spawn_invocation_tokens(&instruction.args).is_err() {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "SPAWN_COMMAND_INVALID",
                        "spawn_capture_all requires a valid command plus optional argv/env tokens",
                    )
                    .with_observed(instruction.args.join(" ")),
                );
                return None;
            }
            let expected = TypeRef::Own(Box::new(TypeRef::Buf(Box::new(TypeRef::Int {
                signed: false,
                bits: 8,
            }))));
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "SPAWN_CAPTURE_RESULT_TYPE",
                        "spawn_capture_all result type must be own[buf[u8]]",
                    )
                    .with_expected("own[buf[u8]]")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            Some(expected)
        }
        "spawn_capture_stderr_all" => {
            if instruction.args.is_empty() {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "spawn_capture_stderr_all expects at least one command token",
                    )
                    .with_expected(">=1")
                    .with_observed("0"),
                );
                return None;
            }
            if validate_spawn_invocation_tokens(&instruction.args).is_err() {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "SPAWN_COMMAND_INVALID",
                        "spawn_capture_stderr_all requires a valid command plus optional argv/env tokens",
                    )
                    .with_observed(instruction.args.join(" ")),
                );
                return None;
            }
            let expected = TypeRef::Own(Box::new(TypeRef::Buf(Box::new(TypeRef::Int {
                signed: false,
                bits: 8,
            }))));
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "SPAWN_CAPTURE_RESULT_TYPE",
                        "spawn_capture_stderr_all result type must be own[buf[u8]]",
                    )
                    .with_expected("own[buf[u8]]")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            Some(expected)
        }
        "spawn_open" => {
            if instruction.args.is_empty() {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "spawn_open expects at least one command token",
                    )
                    .with_expected(">=1")
                    .with_observed("0"),
                );
                return None;
            }
            if validate_spawn_invocation_tokens(&instruction.args).is_err() {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "SPAWN_COMMAND_INVALID",
                        "spawn_open requires a valid command plus optional argv/env tokens",
                    )
                    .with_observed(instruction.args.join(" ")),
                );
                return None;
            }
            match &instruction.ty {
                TypeRef::Int {
                    signed: false,
                    bits: 64,
                } => Some(instruction.ty.clone()),
                found => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "SPAWN_RESULT_TYPE",
                            "spawn_open result type must be u64",
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    None
                }
            }
        }
        "spawn_wait" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "spawn_wait expects one process handle operand",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64,
                }) => {}
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "SPAWN_ARG_TYPE",
                            "spawn_wait expects u64 handle operand",
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    return None;
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_VALUE",
                            format!("unknown value {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    return None;
                }
            }
            match &instruction.ty {
                TypeRef::Int {
                    signed: true,
                    bits: 32,
                } => Some(instruction.ty.clone()),
                found => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "SPAWN_RESULT_TYPE",
                            "spawn_wait result type must be i32",
                        )
                        .with_expected("i32")
                        .with_observed(found.to_string()),
                    );
                    None
                }
            }
        }
        "spawn_stdout_all" | "spawn_stderr_all" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!("{} expects one process handle operand", instruction.op),
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64,
                }) => {}
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "SPAWN_ARG_TYPE",
                            format!("{} expects u64 handle operand", instruction.op),
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    return None;
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_VALUE",
                            format!("unknown value {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    return None;
                }
            }
            let expected = TypeRef::Own(Box::new(TypeRef::Buf(Box::new(TypeRef::Int {
                signed: false,
                bits: 8,
            }))));
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "SPAWN_CAPTURE_RESULT_TYPE",
                        format!("{} result type must be own[buf[u8]]", instruction.op),
                    )
                    .with_expected("own[buf[u8]]")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            Some(expected)
        }
        "spawn_stdin_write_all" => {
            if instruction.args.len() != 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "spawn_stdin_write_all expects handle and buf[u8] operands",
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "SPAWN_RESULT_TYPE",
                        "spawn_stdin_write_all result type must be b1",
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            let handle_ok = matches!(
                resolve_operand_type(&instruction.args[0], env),
                Some(TypeRef::Int { signed: false, bits: 64 })
            );
            let value_ok = match resolve_operand_type(&instruction.args[1], env) {
                Some(found) => view_source_matches(
                    &found,
                    &TypeRef::Buf(Box::new(TypeRef::Int {
                        signed: false,
                        bits: 8,
                    })),
                ),
                None => false,
            };
            if !handle_ok || !value_ok {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "SPAWN_ARG_TYPE",
                        "spawn_stdin_write_all expects (u64, own/view/edit[buf[u8]])",
                    )
                    .with_expected("u64, own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                    .with_observed(format!(
                        "{}, {}",
                        resolve_operand_type(&instruction.args[0], env)
                            .map(|ty| ty.to_string())
                            .unwrap_or_else(|| "unknown".to_string()),
                        resolve_operand_type(&instruction.args[1], env)
                            .map(|ty| ty.to_string())
                            .unwrap_or_else(|| "unknown".to_string())
                    )),
                );
                return None;
            }
            Some(TypeRef::Bool)
        }
        "spawn_stdin_close" | "spawn_done" | "spawn_exit_ok" | "spawn_kill" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        format!("{} expects one process handle operand", instruction.op),
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "SPAWN_RESULT_TYPE",
                        format!("{} result type must be b1", instruction.op),
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64,
                }) => Some(TypeRef::Bool),
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "SPAWN_ARG_TYPE",
                            format!("{} expects u64 handle operand", instruction.op),
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_VALUE",
                            format!("unknown value {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "spawn_close" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "spawn_close expects one process handle operand",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "SPAWN_RESULT_TYPE",
                        "spawn_close result type must be b1",
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 64,
                }) => Some(TypeRef::Bool),
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "SPAWN_ARG_TYPE",
                            "spawn_close expects u64 handle operand",
                        )
                        .with_expected("u64")
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_VALUE",
                            format!("unknown value {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "net_connect" => {
            if !instruction.args.is_empty() {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "net_connect expects 0 operands",
                    )
                    .with_expected("0")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            match &instruction.ty {
                TypeRef::Bool => Some(TypeRef::Bool),
                found => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "NET_RESULT_TYPE",
                            "net_connect result type must be b1",
                        )
                        .with_expected("b1")
                        .with_observed(found.to_string()),
                    );
                    None
                }
            }
        }
        "net_write_all" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "net_write_all expects one buf[u8] handle operand",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            if instruction.ty != TypeRef::Bool {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "NET_WRITE_RESULT_TYPE",
                        "net_write_all result type must be b1",
                    )
                    .with_expected("b1")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            let operand_ty = match resolve_operand_type(&instruction.args[0], env) {
                Some(operand_ty) => operand_ty,
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown net_write_all operand {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    return None;
                }
            };
            if !matches!(
                &operand_ty,
                TypeRef::Own(inner) | TypeRef::View(inner) | TypeRef::Edit(inner)
                    if matches!(
                        inner.as_ref(),
                        TypeRef::Buf(elem)
                            if **elem
                                == TypeRef::Int {
                                    signed: false,
                                    bits: 8,
                                }
                    )
            ) {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "NET_OPERAND_TYPE",
                        "net_write_all expects own/view/edit[buf[u8]]",
                    )
                    .with_expected("own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                    .with_observed(operand_ty.to_string()),
                );
                return None;
            }
            Some(TypeRef::Bool)
        }
        "net_exchange_all" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "net_exchange_all expects one buf[u8] handle operand",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Own(Box::new(TypeRef::Buf(Box::new(TypeRef::Int {
                signed: false,
                bits: 8,
            }))));
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "NET_EXCHANGE_RESULT_TYPE",
                        "net_exchange_all result type must be own[buf[u8]]",
                    )
                    .with_expected("own[buf[u8]]")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            let operand_ty = match resolve_operand_type(&instruction.args[0], env) {
                Some(operand_ty) => operand_ty,
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!("unknown net_exchange_all operand {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    return None;
                }
            };
            if !matches!(
                &operand_ty,
                TypeRef::Own(inner) | TypeRef::View(inner) | TypeRef::Edit(inner)
                    if matches!(
                        inner.as_ref(),
                        TypeRef::Buf(elem)
                            if **elem
                                == TypeRef::Int {
                                    signed: false,
                                    bits: 8,
                                }
                    )
            ) {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "NET_OPERAND_TYPE",
                        "net_exchange_all expects own/view/edit[buf[u8]]",
                    )
                    .with_expected("own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                    .with_observed(operand_ty.to_string()),
                );
                return None;
            }
            Some(expected)
        }
        "net_serve_exchange_all" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "net_serve_exchange_all expects one buf[u8] response operand",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let expected = TypeRef::Own(Box::new(TypeRef::Buf(Box::new(TypeRef::Int {
                signed: false,
                bits: 8,
            }))));
            if instruction.ty != expected {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "NET_EXCHANGE_RESULT_TYPE",
                        "net_serve_exchange_all result type must be own[buf[u8]]",
                    )
                    .with_expected("own[buf[u8]]")
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            let operand_ty = match resolve_operand_type(&instruction.args[0], env) {
                Some(operand_ty) => operand_ty,
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_OPERAND",
                            format!(
                                "unknown net_serve_exchange_all operand {}",
                                instruction.args[0]
                            ),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    return None;
                }
            };
            if !matches!(
                &operand_ty,
                TypeRef::Own(inner) | TypeRef::View(inner) | TypeRef::Edit(inner)
                    if matches!(
                        inner.as_ref(),
                        TypeRef::Buf(elem)
                            if **elem
                                == TypeRef::Int {
                                    signed: false,
                                    bits: 8,
                                }
                    )
            ) {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "NET_OPERAND_TYPE",
                        "net_serve_exchange_all expects own/view/edit[buf[u8]]",
                    )
                    .with_expected("own[buf[u8]] | view[buf[u8]] | edit[buf[u8]]")
                    .with_observed(operand_ty.to_string()),
                );
                return None;
            }
            Some(expected)
        }
        "view" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "view expects one operand",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let TypeRef::View(expected_inner) = &instruction.ty else {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "VIEW_RESULT_TYPE",
                        "view result must wrap a borrow-trackable inner type",
                    )
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            };
            let matches_supported_view = is_borrow_trackable_inner(expected_inner.as_ref());
            if !matches_supported_view {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "VIEW_RESULT_TYPE",
                        "view result must be view[buf[T]], view[str], view[vec[N,T]], or view[named]",
                    )
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(found) if view_source_matches(&found, expected_inner.as_ref()) => {
                    Some(instruction.ty.clone())
                }
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "VIEW_OPERAND_TYPE",
                            "view expects own/view/edit of the same borrow-trackable inner type",
                        )
                        .with_expected(format!("* -> view[{}]", expected_inner))
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_VALUE",
                            format!("unknown value {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "edit" => {
            if instruction.args.len() != 1 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "edit expects one operand",
                    )
                    .with_expected("1")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let TypeRef::Edit(expected_inner) = &instruction.ty else {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "EDIT_RESULT_TYPE",
                        "edit result must be edit[buf[T]]",
                    )
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            };
            let TypeRef::Buf(_) = expected_inner.as_ref() else {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "EDIT_RESULT_TYPE",
                        "edit result must be edit[buf[T]]",
                    )
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            };
            match resolve_operand_type(&instruction.args[0], env) {
                Some(found) if edit_source_matches(&found, expected_inner.as_ref()) => {
                    Some(instruction.ty.clone())
                }
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "EDIT_OPERAND_TYPE",
                            "edit expects own[buf[T]] or edit[buf[T]] of the same inner type",
                        )
                        .with_expected(format!("* -> edit[{}]", expected_inner))
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_VALUE",
                            format!("unknown value {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "store" => {
            if instruction.args.len() != 3 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "store expects edit-handle, index, and value",
                    )
                    .with_expected("3")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let TypeRef::Edit(expected_inner) = &instruction.ty else {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "STORE_RESULT_TYPE",
                        "store result must be edit[buf[T]]",
                    )
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            };
            let TypeRef::Buf(expected_elem) = expected_inner.as_ref() else {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "STORE_RESULT_TYPE",
                        "store result must be edit[buf[T]]",
                    )
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            };
            match resolve_operand_type(&instruction.args[0], env) {
                Some(found) if found != instruction.ty => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "STORE_OPERAND_TYPE",
                            "store handle must match the declared result type",
                        )
                        .with_expected(instruction.ty.to_string())
                        .with_observed(found.to_string()),
                    );
                    return None;
                }
                Some(_) => {}
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_VALUE",
                            format!("unknown value {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    return None;
                }
            }
            match resolve_operand_type(&instruction.args[1], env) {
                Some(TypeRef::Int {
                    signed: false,
                    bits: 32,
                }) => {}
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "STORE_INDEX_TYPE",
                            "store index must be u32",
                        )
                        .with_expected("u32")
                        .with_observed(found.to_string()),
                    );
                    return None;
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_VALUE",
                            format!("unknown value {}", instruction.args[1]),
                        )
                        .with_observed(instruction.args[1].clone()),
                    );
                    return None;
                }
            }
            match resolve_operand_type(&instruction.args[2], env) {
                Some(found) if found == *expected_elem.as_ref() => Some(instruction.ty.clone()),
                Some(found) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "STORE_VALUE_TYPE",
                            "store value must match the buffer element type",
                        )
                        .with_expected(expected_elem.to_string())
                        .with_observed(found.to_string()),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_VALUE",
                            format!("unknown value {}", instruction.args[2]),
                        )
                        .with_observed(instruction.args[2].clone()),
                    );
                    None
                }
            }
        }
        "abs" => unary_type("abs", instruction, env, node, diagnostics).and_then(|ty| {
            if ty.is_signed_int() || ty.is_float() {
                Some(ty)
            } else {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "ABS_OPERAND_TYPE",
                        "abs expects a signed integer or float",
                    )
                    .with_observed(ty.to_string()),
                );
                None
            }
        }),
        "add" | "sub" | "mul" => {
            binary_same_type(&instruction.op, instruction, env, node, diagnostics).and_then(|ty| {
                if ty.is_numeric() {
                    Some(ty)
                } else {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "NUMERIC_OPERAND_TYPE",
                            "arithmetic ops expect numeric operands",
                        )
                        .with_observed(ty.to_string()),
                    );
                    None
                }
            })
        }
        "sat_add" => binary_same_type(&instruction.op, instruction, env, node, diagnostics)
            .and_then(|ty| {
                if ty.is_int() {
                    Some(ty)
                } else {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "NUMERIC_OPERAND_TYPE",
                            "sat_add expects integer operands",
                        )
                        .with_observed(ty.to_string()),
                    );
                    None
                }
            }),
        "band" | "bor" | "bxor" => {
            binary_same_type(&instruction.op, instruction, env, node, diagnostics).and_then(|ty| {
                if ty.is_int() {
                    Some(ty)
                } else {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "BITWISE_OPERAND_TYPE",
                            "bitwise operations expect integer operands",
                        )
                        .with_observed(ty.to_string()),
                    );
                    None
                }
            })
        }
        "shl" | "shr" => shift_type(instruction, env, node, diagnostics),
        "lt" | "le" => binary_same_type(&instruction.op, instruction, env, node, diagnostics)
            .and_then(|ty| {
                if ty.is_numeric() {
                    Some(TypeRef::Bool)
                } else {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "NUMERIC_OPERAND_TYPE",
                            "ordered comparisons expect numeric operands",
                        )
                        .with_observed(ty.to_string()),
                    );
                    None
                }
            }),
        "eq" => {
            binary_same_type(&instruction.op, instruction, env, node, diagnostics).and_then(|ty| {
                if is_equality_comparable(&ty, type_decls) {
                    Some(TypeRef::Bool)
                } else {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "EQ_OPERAND_TYPE",
                            "eq requires recursively equality-comparable operands",
                        )
                        .with_observed(ty.to_string()),
                    );
                    None
                }
            })
        }
        "make" => {
            if instruction.args.is_empty() {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "make expects a type name followed by field values",
                    )
                    .with_expected(">=1")
                    .with_observed("0"),
                );
                return None;
            }
            let TypeRef::Named(result_name) = &instruction.ty else {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "MAKE_RESULT_TYPE",
                        "make result must be a named type",
                    )
                    .with_observed(instruction.ty.to_string()),
                );
                return None;
            };
            if &instruction.args[0] != result_name {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "MAKE_TYPE_MISMATCH",
                        "make type argument must match the declared result type",
                    )
                    .with_expected(result_name.clone())
                    .with_observed(instruction.args[0].clone()),
                );
                return None;
            }
            match type_decls.get(result_name) {
                Some(TypeDeclBody::Struct { fields }) => {
                    if instruction.args.len() != fields.len() + 1 {
                        diagnostics.push(
                            Diagnostic::new(
                                "typecheck",
                                node.to_string(),
                                "MAKE_ARITY",
                                "make argument count must match struct fields",
                            )
                            .with_expected(fields.len().to_string())
                            .with_observed((instruction.args.len() - 1).to_string()),
                        );
                        return None;
                    }
                    for (field, operand) in fields.iter().zip(instruction.args.iter().skip(1)) {
                        match resolve_operand_type(operand, env) {
                            Some(found) if found != field.ty => diagnostics.push(
                                Diagnostic::new(
                                    "typecheck",
                                    node.to_string(),
                                    "MAKE_FIELD_TYPE",
                                    format!("field {} does not match {}", field.name, field.ty),
                                )
                                .with_expected(field.ty.to_string())
                                .with_observed(found.to_string()),
                            ),
                            None => diagnostics.push(
                                Diagnostic::new(
                                    "typecheck",
                                    node.to_string(),
                                    "UNKNOWN_VALUE",
                                    format!("unknown value {operand}"),
                                )
                                .with_observed(operand.clone()),
                            ),
                            _ => {}
                        }
                    }
                    Some(instruction.ty.clone())
                }
                Some(TypeDeclBody::Enum { variants }) => {
                    if instruction.args.len() < 2 {
                        diagnostics.push(
                            Diagnostic::new(
                                "typecheck",
                                node.to_string(),
                                "MAKE_ENUM_VARIANT_MISSING",
                                "make enum expects a variant name after the type",
                            )
                            .with_expected("type variant ...")
                            .with_observed(instruction.args.join(" ")),
                        );
                        return None;
                    }
                    let variant_name = &instruction.args[1];
                    let Some(variant) = variants
                        .iter()
                        .find(|variant| &variant.name == variant_name)
                    else {
                        diagnostics.push(
                            Diagnostic::new(
                                "typecheck",
                                node.to_string(),
                                "UNKNOWN_VARIANT",
                                format!(
                                    "variant {} does not exist on {}",
                                    variant_name, result_name
                                ),
                            )
                            .with_observed(variant_name.clone()),
                        );
                        return None;
                    };
                    if instruction.args.len() != variant.fields.len() + 2 {
                        diagnostics.push(
                            Diagnostic::new(
                                "typecheck",
                                node.to_string(),
                                "MAKE_ARITY",
                                "make argument count must match enum payload fields",
                            )
                            .with_expected(variant.fields.len().to_string())
                            .with_observed((instruction.args.len() - 2).to_string()),
                        );
                        return None;
                    }
                    for (field, operand) in
                        variant.fields.iter().zip(instruction.args.iter().skip(2))
                    {
                        match resolve_operand_type(operand, env) {
                            Some(found) if found != field.ty => diagnostics.push(
                                Diagnostic::new(
                                    "typecheck",
                                    node.to_string(),
                                    "MAKE_FIELD_TYPE",
                                    format!("field {} does not match {}", field.name, field.ty),
                                )
                                .with_expected(field.ty.to_string())
                                .with_observed(found.to_string()),
                            ),
                            None => diagnostics.push(
                                Diagnostic::new(
                                    "typecheck",
                                    node.to_string(),
                                    "UNKNOWN_VALUE",
                                    format!("unknown value {operand}"),
                                )
                                .with_observed(operand.clone()),
                            ),
                            _ => {}
                        }
                    }
                    Some(instruction.ty.clone())
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "validate",
                            node.to_string(),
                            "UNKNOWN_TYPE",
                            format!("unknown type {result_name}"),
                        )
                        .with_observed(result_name.clone()),
                    );
                    None
                }
            }
        }
        "field" => {
            if instruction.args.len() != 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "field expects aggregate and field name",
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            match resolve_operand_type(&instruction.args[0], env) {
                Some(found) => {
                    let Some(name) = named_reference_type_name(&found) else {
                        diagnostics.push(
                            Diagnostic::new(
                                "typecheck",
                                node.to_string(),
                                "FIELD_OPERAND_TYPE",
                                "field expects a named struct, enum, or wrapped named aggregate reference",
                            )
                            .with_expected("named | own[named] | view[named] | edit[named]")
                            .with_observed(found.to_string()),
                        );
                        return None;
                    };
                    match type_decls.get(&name) {
                    Some(TypeDeclBody::Struct { fields }) => {
                        let field_name = &instruction.args[1];
                        match fields.iter().find(|field| &field.name == field_name) {
                            Some(field) => Some(field.ty.clone()),
                            None => {
                                diagnostics.push(
                                    Diagnostic::new(
                                        "typecheck",
                                        node.to_string(),
                                        "UNKNOWN_FIELD",
                                        format!("field {} does not exist on {}", field_name, name),
                                    )
                                    .with_observed(field_name.clone()),
                                );
                                None
                            }
                        }
                    }
                    Some(TypeDeclBody::Enum { variants }) => {
                        let Some((variant_name, field_name)) = instruction.args[1].split_once('.')
                        else {
                            diagnostics.push(
                                Diagnostic::new(
                                    "typecheck",
                                    node.to_string(),
                                    "FIELD_ON_ENUM",
                                    "enum field access must use variant.field",
                                )
                                .with_expected("variant.field")
                                .with_observed(instruction.args[1].clone()),
                            );
                            return None;
                        };
                        let Some(variant) =
                            variants.iter().find(|variant| variant.name == variant_name)
                        else {
                            diagnostics.push(
                                Diagnostic::new(
                                    "typecheck",
                                    node.to_string(),
                                    "UNKNOWN_VARIANT",
                                    format!("variant {} does not exist on {}", variant_name, name),
                                )
                                .with_observed(variant_name.to_string()),
                            );
                            return None;
                        };
                        match variant.fields.iter().find(|field| field.name == field_name) {
                            Some(field) => Some(field.ty.clone()),
                            None => {
                                diagnostics.push(
                                    Diagnostic::new(
                                        "typecheck",
                                        node.to_string(),
                                        "UNKNOWN_FIELD",
                                        format!(
                                            "field {} does not exist on {}.{}",
                                            field_name, name, variant_name
                                        ),
                                    )
                                    .with_observed(field_name.to_string()),
                                );
                                None
                            }
                        }
                    }
                    None => {
                        diagnostics.push(
                            Diagnostic::new(
                                "validate",
                                node.to_string(),
                                "UNKNOWN_TYPE",
                                format!("unknown type {name}"),
                            )
                            .with_observed(name),
                        );
                        None
                    }
                }
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_VALUE",
                            format!("unknown value {}", instruction.args[0]),
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    None
                }
            }
        }
        "sext" => {
            if instruction.args.len() != 2 {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "INVALID_ARITY",
                        "sext expects target type and value",
                    )
                    .with_expected("2")
                    .with_observed(instruction.args.len().to_string()),
                );
                return None;
            }
            let target = match TypeRef::parse(&instruction.args[0]) {
                Ok(target) => target,
                Err(error) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "INVALID_CAST_TARGET",
                            error,
                        )
                        .with_observed(instruction.args[0].clone()),
                    );
                    return None;
                }
            };
            match resolve_operand_type(&instruction.args[1], env) {
                Some(source) if source.is_signed_int() && target.is_signed_int() => Some(target),
                Some(source) => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "SEXT_OPERAND_TYPE",
                            "sext requires signed integers",
                        )
                        .with_expected("iN -> iM")
                        .with_observed(format!("{source} -> {target}")),
                    );
                    None
                }
                None => {
                    diagnostics.push(
                        Diagnostic::new(
                            "typecheck",
                            node.to_string(),
                            "UNKNOWN_VALUE",
                            format!("unknown value {}", instruction.args[1]),
                        )
                        .with_observed(instruction.args[1].clone()),
                    );
                    None
                }
            }
        }
        "bnot" => unary_type("bnot", instruction, env, node, diagnostics).and_then(|ty| {
            if ty.is_int() {
                Some(ty)
            } else {
                diagnostics.push(
                    Diagnostic::new(
                        "typecheck",
                        node.to_string(),
                        "BITWISE_OPERAND_TYPE",
                        "bnot expects an integer operand",
                    )
                    .with_observed(ty.to_string()),
                );
                None
            }
        }),
        _ => {
            diagnostics.push(
                Diagnostic::new(
                    "typecheck",
                    node.to_string(),
                    "UNSUPPORTED_OP",
                    format!("unsupported instruction op {}", instruction.op),
                )
                .with_observed(instruction.op.clone()),
            );
            None
        }
    }
}

fn is_ffi_scalar_type(ty: &TypeRef) -> bool {
    matches!(
        ty,
        TypeRef::Int { .. } | TypeRef::Float { .. } | TypeRef::Bool
    )
}

fn unary_type(
    op: &str,
    instruction: &crate::ast::Instruction,
    env: &HashMap<String, TypeRef>,
    node: &str,
    diagnostics: &mut Vec<Diagnostic>,
) -> Option<TypeRef> {
    if instruction.args.len() != 1 {
        diagnostics.push(
            Diagnostic::new(
                "typecheck",
                node.to_string(),
                "INVALID_ARITY",
                format!("{op} expects 1 operand"),
            )
            .with_expected("1")
            .with_observed(instruction.args.len().to_string()),
        );
        return None;
    }
    resolve_operand_type(&instruction.args[0], env).or_else(|| {
        diagnostics.push(
            Diagnostic::new(
                "typecheck",
                node.to_string(),
                "UNKNOWN_VALUE",
                format!("unknown value {}", instruction.args[0]),
            )
            .with_observed(instruction.args[0].clone()),
        );
        None
    })
}

fn binary_same_type(
    op: &str,
    instruction: &crate::ast::Instruction,
    env: &HashMap<String, TypeRef>,
    node: &str,
    diagnostics: &mut Vec<Diagnostic>,
) -> Option<TypeRef> {
    if instruction.args.len() != 2 {
        diagnostics.push(
            Diagnostic::new(
                "typecheck",
                node.to_string(),
                "INVALID_ARITY",
                format!("{op} expects 2 operands"),
            )
            .with_expected("2")
            .with_observed(instruction.args.len().to_string()),
        );
        return None;
    }
    let left = resolve_operand_type(&instruction.args[0], env);
    let right = resolve_operand_type(&instruction.args[1], env);
    match (left, right) {
        (Some(left), Some(right)) if left == right => Some(left),
        (Some(left), Some(right)) => {
            diagnostics.push(
                Diagnostic::new(
                    "typecheck",
                    node.to_string(),
                    "OPERAND_TYPE_MISMATCH",
                    format!("{op} operands must have the same type"),
                )
                .with_expected(left.to_string())
                .with_observed(right.to_string()),
            );
            None
        }
        _ => {
            diagnostics.push(Diagnostic::new(
                "typecheck",
                node.to_string(),
                "UNKNOWN_VALUE",
                format!("{op} operands must be defined"),
            ));
            None
        }
    }
}

fn shift_type(
    instruction: &crate::ast::Instruction,
    env: &HashMap<String, TypeRef>,
    node: &str,
    diagnostics: &mut Vec<Diagnostic>,
) -> Option<TypeRef> {
    if instruction.args.len() != 2 {
        diagnostics.push(
            Diagnostic::new(
                "typecheck",
                node.to_string(),
                "INVALID_ARITY",
                format!("{} expects 2 operands", instruction.op),
            )
            .with_expected("2")
            .with_observed(instruction.args.len().to_string()),
        );
        return None;
    }
    let left = resolve_operand_type(&instruction.args[0], env);
    let right = resolve_operand_type(&instruction.args[1], env);
    match (left, right) {
        (Some(left), Some(right)) if left.is_int() && right.is_int() => Some(left),
        (Some(left), Some(right)) => {
            diagnostics.push(
                Diagnostic::new(
                    "typecheck",
                    node.to_string(),
                    "BITWISE_OPERAND_TYPE",
                    "shift operations expect integer operands",
                )
                .with_expected("iN/uN << iM/uM")
                .with_observed(format!("{left} {} {right}", instruction.op)),
            );
            None
        }
        _ => {
            diagnostics.push(Diagnostic::new(
                "typecheck",
                node.to_string(),
                "UNKNOWN_VALUE",
                format!("{} operands must be defined", instruction.op),
            ));
            None
        }
    }
}

fn is_matchable_type(ty: &TypeRef, decls: &DeclMap<'_>) -> bool {
    match ty {
        TypeRef::Int { .. } | TypeRef::Bool => true,
        TypeRef::Named(name) => matches!(decls.get(name), Some(TypeDeclBody::Enum { .. })),
        _ => false,
    }
}

fn is_equality_comparable(ty: &TypeRef, decls: &DeclMap<'_>) -> bool {
    match ty {
        TypeRef::Int { .. } | TypeRef::Float { .. } | TypeRef::Bool => true,
        TypeRef::Named(name) => match decls.get(name) {
            Some(TypeDeclBody::Struct { fields }) => fields
                .iter()
                .all(|field| is_equality_comparable(&field.ty, decls)),
            Some(TypeDeclBody::Enum { variants }) => variants.iter().all(|variant| {
                variant
                    .fields
                    .iter()
                    .all(|field| is_equality_comparable(&field.ty, decls))
            }),
            _ => false,
        },
        _ => false,
    }
}

fn supports_len(ty: &TypeRef) -> bool {
    match ty {
        TypeRef::Span(_) | TypeRef::Buf(_) | TypeRef::Vec { .. } | TypeRef::String => true,
        TypeRef::Own(inner) | TypeRef::View(inner) | TypeRef::Edit(inner) => supports_len(inner),
        _ => false,
    }
}

fn load_result_type(ty: &TypeRef) -> Option<TypeRef> {
    match ty {
        TypeRef::Span(inner) | TypeRef::Buf(inner) => Some((**inner).clone()),
        TypeRef::Vec { elem, .. } => Some((**elem).clone()),
        TypeRef::Own(inner) | TypeRef::View(inner) | TypeRef::Edit(inner) => {
            load_result_type(inner)
        }
        _ => None,
    }
}

fn is_borrow_trackable_inner(ty: &TypeRef) -> bool {
    matches!(
        ty,
        TypeRef::Buf(_) | TypeRef::String | TypeRef::Vec { .. } | TypeRef::Named(_)
    )
}

fn named_reference_type_name(ty: &TypeRef) -> Option<String> {
    match ty {
        TypeRef::Named(name) => Some(name.clone()),
        TypeRef::Own(inner) | TypeRef::View(inner) | TypeRef::Edit(inner) => {
            named_reference_type_name(inner.as_ref())
        }
        _ => None,
    }
}

fn view_source_matches(found: &TypeRef, expected_inner: &TypeRef) -> bool {
    match found {
        TypeRef::Own(inner) | TypeRef::View(inner) | TypeRef::Edit(inner) => {
            inner.as_ref() == expected_inner && is_borrow_trackable_inner(expected_inner)
        }
        _ => false,
    }
}

fn edit_source_matches(found: &TypeRef, expected_inner: &TypeRef) -> bool {
    match found {
        TypeRef::Own(inner) | TypeRef::Edit(inner) => {
            inner.as_ref() == expected_inner
                && matches!(expected_inner, TypeRef::Buf(_) | TypeRef::String)
        }
        _ => false,
    }
}

fn validate_instruction_operands_not_consumed(
    instruction: &crate::ast::Instruction,
    node: &str,
    consumed: &HashSet<String>,
    diagnostics: &mut Vec<Diagnostic>,
) {
    for operand in instruction_binding_operands(instruction) {
        validate_operand_not_consumed(operand, node, consumed, diagnostics);
    }
}

fn instruction_binding_operands(instruction: &crate::ast::Instruction) -> Vec<&String> {
    match instruction.op.as_str() {
        "ffi_call"
        | "ffi_call_cstr"
        | "spawn_call"
        | "spawn_capture_all"
        | "spawn_capture_stderr_all"
        | "spawn_open"
        | "task_open"
        | "ffi_open_lib"
        | "db_open"
        | "db_pool_open"
        | "cache_open"
        | "queue_open"
        | "stream_open"
        | "lease_open"
        | "placement_open"
        | "coord_open" => instruction.args.iter().skip(1).collect(),
        "service_open" | "service_error_status" => Vec::new(),
        "service_log" => instruction
            .args
            .iter()
            .enumerate()
            .filter_map(|(index, arg)| if index == 1 { None } else { Some(arg) })
            .collect(),
        "service_trace_begin" => instruction
            .args
            .iter()
            .enumerate()
            .filter_map(|(index, arg)| if index == 1 { None } else { Some(arg) })
            .collect(),
        "service_metric_count" => instruction
            .args
            .iter()
            .enumerate()
            .filter_map(|(index, arg)| if index == 1 { None } else { Some(arg) })
            .collect(),
        "service_metric_count_dim" => instruction
            .args
            .iter()
            .enumerate()
            .filter_map(|(index, arg)| match index {
                1 | 2 => None,
                _ => Some(arg),
            })
            .collect(),
        "service_metric_total"
        | "service_event_total"
        | "service_failure_total"
        | "service_checkpoint_load_u32"
        | "service_checkpoint_exists" => instruction
            .args
            .iter()
            .enumerate()
            .filter_map(|(index, arg)| if index == 1 { None } else { Some(arg) })
            .collect(),
        "service_set_health" | "service_set_readiness" | "service_set_degraded" => {
            instruction.args.iter().collect()
        }
        "service_degraded" | "service_trace_link_count" => instruction.args.iter().collect(),
        "service_event" => instruction
            .args
            .iter()
            .enumerate()
            .filter_map(|(index, arg)| if index == 1 { None } else { Some(arg) })
            .collect(),
        "service_trace_link" => instruction.args.iter().collect(),
        "service_failure_count" | "service_checkpoint_save_u32" => instruction
            .args
            .iter()
            .enumerate()
            .filter_map(|(index, arg)| if index == 1 { None } else { Some(arg) })
            .collect(),
        "service_migrate_db" => instruction
            .args
            .iter()
            .enumerate()
            .filter_map(|(index, arg)| if index == 2 { None } else { Some(arg) })
            .collect(),
        "service_route" | "service_require_header" => instruction.args.iter().take(1).collect(),
        "msg_log_open" | "batch_open" | "agg_open_u64" => Vec::new(),
        "msg_log_close"
        | "lease_owner"
        | "lease_close"
        | "placement_close"
        | "coord_close"
        | "msg_replay_next"
        | "msg_replay_seq"
        | "msg_replay_close"
        | "msg_failure_class"
        | "stream_close"
        | "stream_replay_next"
        | "stream_replay_offset"
        | "stream_replay_close"
        | "batch_len"
        | "batch_flush_sum_u64"
        | "batch_close"
        | "agg_count"
        | "agg_sum_u64"
        | "agg_avg_u64"
        | "agg_min_u64"
        | "agg_max_u64"
        | "agg_close"
        | "window_count"
        | "window_sum_u64"
        | "window_avg_u64"
        | "window_min_u64"
        | "window_max_u64"
        | "window_close" => {
            instruction.args.iter().take(1).collect()
        }
        "stream_publish_buf" | "batch_push_u64" | "agg_add_u64" | "window_add_u64" => {
            instruction.args.iter().collect()
        }
        "stream_replay_open"
        | "window_open_ms"
        | "shard_route_u32"
        | "lease_acquire"
        | "lease_transfer"
        | "lease_release"
        | "placement_assign"
        | "placement_lookup" => instruction.args.iter().collect(),
        "coord_store_u32" => instruction
            .args
            .iter()
            .enumerate()
            .filter_map(|(index, arg)| if index == 1 { None } else { Some(arg) })
            .collect(),
        "coord_load_u32" => instruction.args.iter().take(1).collect(),
        "msg_send" => instruction
            .args
            .iter()
            .enumerate()
            .filter_map(|(index, arg)| if index == 1 || index == 2 { None } else { Some(arg) })
            .collect(),
        "msg_send_dedup" => instruction
            .args
            .iter()
            .enumerate()
            .filter_map(|(index, arg)| if index == 1 || index == 2 { None } else { Some(arg) })
            .collect(),
        "msg_subscribe" | "msg_subscriber_count" | "msg_recv_next" | "msg_recv_seq" | "msg_pending_count" | "msg_delivery_total" => {
            instruction.args.iter().take(1).collect()
        }
        "msg_fanout" => instruction
            .args
            .iter()
            .enumerate()
            .filter_map(|(index, arg)| if index == 1 { None } else { Some(arg) })
            .collect(),
        "msg_ack" | "msg_mark_retry" | "msg_retry_count" | "msg_replay_open" => instruction
            .args
            .iter()
            .enumerate()
            .filter_map(|(index, arg)| if index == 1 { None } else { Some(arg) })
            .collect(),
        "rt_spawn_u32" | "rt_try_spawn_u32" | "rt_spawn_buf" | "rt_try_spawn_buf" => instruction
            .args
            .iter()
            .enumerate()
            .filter_map(|(index, arg)| if index == 1 { None } else { Some(arg) })
            .collect(),
        "db_prepare"
        | "db_exec_prepared"
        | "db_query_prepared_u32"
        | "db_query_prepared_buf"
        | "db_query_prepared_row" => {
            instruction
                .args
                .iter()
                .enumerate()
                .filter_map(|(index, arg)| if index == 1 { None } else { Some(arg) })
                .collect()
        }
        "ffi_call_lib" | "ffi_call_lib_cstr" => instruction
            .args
            .iter()
            .enumerate()
            .filter_map(|(index, arg)| if index == 1 { None } else { Some(arg) })
            .collect(),
        "http_method_eq"
        | "http_path_eq"
        | "http_request_method"
        | "http_request_path"
        | "http_header_eq"
        | "http_cookie_eq"
        | "http_status_u32"
        | "buf_eq_lit"
        | "buf_contains_lit"
        | "buf_lit"
        | "http_header"
        | "http_cookie"
        | "http_query_param"
        | "http_body"
        | "http_route_param"
        | "buf_parse_u32"
        | "buf_parse_bool"
        | "json_get_u32"
        | "json_get_bool"
        | "json_get_buf"
        | "json_get_str"
        | "json_has_key"
        | "json_array_len"
        | "json_index_u32"
        | "json_index_bool"
        | "json_index_str"
        | "str_lit"
        | "str_eq_lit"
        | "str_to_buf"
        | "buf_to_str"
        | "buf_hex_str"
        | "str_from_u32"
        | "str_from_bool"
        | "config_get_u32"
        | "config_get_bool"
        | "config_get_str"
        | "config_has"
        | "env_get_u32"
        | "env_get_bool"
        | "env_get_str"
        | "env_has"
        | "strmap_get_u32"
        | "strmap_get_bool"
        | "strmap_get_str"
        | "strlist_len"
        | "strlist_index_u32"
        | "strlist_index_bool"
        | "strlist_index_str"
        | "date_parse_ymd"
        | "time_parse_hms"
        | "date_format_ymd"
        | "time_format_hms"
        | "tls_exchange_all"
        | "buf_before_lit"
        | "buf_after_lit"
        | "buf_trim_ascii" => instruction.args.iter().take(1).collect(),
        "json_get_u32_or"
        | "json_get_bool_or"
        | "json_get_buf_or"
        | "json_get_str_or" => instruction
            .args
            .iter()
            .enumerate()
            .filter_map(|(index, arg)| if index == 1 { None } else { Some(arg) })
            .collect(),
        "json_encode_arr" => instruction.args.iter().collect(),
        "http_write_text_response_cookie"
        | "http_session_write_text_cookie"
        | "http_write_json_response_cookie"
        | "http_session_write_json_cookie" => instruction
            .args
            .iter()
            .enumerate()
            .filter_map(|(index, arg)| match index {
                0 | 1 | 4 => Some(arg),
                _ => None,
            })
            .collect(),
        "http_write_text_response_headers2"
        | "http_session_write_text_headers2"
        | "http_write_json_response_headers2"
        | "http_session_write_json_headers2" => instruction
            .args
            .iter()
            .enumerate()
            .filter_map(|(index, arg)| match index {
                0 | 1 | 6 => Some(arg),
                _ => None,
            })
            .collect(),
        "http_server_config_u32"
        | "tls_server_config_u32"
        | "tls_server_config_buf"
        | "rt_cancelled"
        | "cancel_scope_open" => Vec::new(),
        _ => instruction.args.iter().collect(),
    }
}

fn validate_operand_not_consumed(
    operand: &str,
    node: &str,
    consumed: &HashSet<String>,
    diagnostics: &mut Vec<Diagnostic>,
) {
    if consumed.contains(operand) {
        diagnostics.push(
            Diagnostic::new(
                "validate",
                node.to_string(),
                "USE_AFTER_DROP",
                format!("value {operand} is used after drop in the same block"),
            )
            .with_observed(operand.to_string())
            .with_fix_hint("move drop closer to the last use or keep using a pre-drop value"),
        );
    }
}

fn is_tracked_reference_type(ty: &TypeRef) -> bool {
    match ty {
        TypeRef::Own(inner) | TypeRef::View(inner) | TypeRef::Edit(inner) => {
            is_borrow_trackable_inner(inner.as_ref())
        }
        _ => false,
    }
}

fn is_owned_tracked_type(ty: &TypeRef) -> bool {
    match ty {
        TypeRef::Own(inner) => is_borrow_trackable_inner(inner.as_ref()),
        _ => false,
    }
}

fn is_u64_type(ty: &TypeRef) -> bool {
    matches!(
        ty,
        TypeRef::Int {
            signed: false,
            bits: 64,
        }
    )
}

fn is_u32_type(ty: &TypeRef) -> bool {
    matches!(
        ty,
        TypeRef::Int {
            signed: false,
            bits: 32,
        }
    )
}

fn is_runtime_handle_kind(kind: OwnershipTokenKind) -> bool {
    matches!(
        kind,
        OwnershipTokenKind::SpawnHandle
            | OwnershipTokenKind::TaskHandle
            | OwnershipTokenKind::SocketHandle
            | OwnershipTokenKind::SessionHandle
            | OwnershipTokenKind::DbHandle
            | OwnershipTokenKind::DbPoolHandle
            | OwnershipTokenKind::CacheHandle
            | OwnershipTokenKind::QueueHandle
            | OwnershipTokenKind::RuntimeHandle
            | OwnershipTokenKind::RuntimeTaskHandle
            | OwnershipTokenKind::ChannelHandle
            | OwnershipTokenKind::FfiLibHandle
            | OwnershipTokenKind::ServiceHandle
            | OwnershipTokenKind::TraceHandle
    )
}

fn token_is_runtime_handle(
    token: &str,
    token_kinds: &BTreeMap<String, BTreeSet<OwnershipTokenKind>>,
) -> bool {
    token_kinds
        .get(token)
        .is_some_and(|kinds| kinds.iter().any(|kind| is_runtime_handle_kind(*kind)))
}

fn runtime_handle_kind_for_instruction(
    instruction: &crate::ast::Instruction,
) -> Option<OwnershipTokenKind> {
    match instruction.op.as_str() {
        "spawn_open" => Some(OwnershipTokenKind::SpawnHandle),
        "task_open" => Some(OwnershipTokenKind::TaskHandle),
        "ffi_open_lib" => Some(OwnershipTokenKind::FfiLibHandle),
        "db_open" | "db_pool_acquire" => Some(OwnershipTokenKind::DbHandle),
        "db_pool_open" => Some(OwnershipTokenKind::DbPoolHandle),
        "cache_open" => Some(OwnershipTokenKind::CacheHandle),
        "queue_open" | "stream_open" => Some(OwnershipTokenKind::QueueHandle),
        "lease_open" | "placement_open" | "coord_open" => Some(OwnershipTokenKind::RuntimeHandle),
        "net_listen" | "tls_listen" | "net_accept" | "http_client_pool_open" => {
            Some(OwnershipTokenKind::SocketHandle)
        }
        "http_session_accept"
        | "http_body_stream_open"
        | "http_response_stream_open"
        | "http_client_open"
        | "http_client_pool_acquire"
        | "net_session_open" => Some(OwnershipTokenKind::SessionHandle),
        "rt_open" => Some(OwnershipTokenKind::RuntimeHandle),
        "rt_spawn_u32" | "rt_try_spawn_u32" | "rt_spawn_buf" | "rt_try_spawn_buf" => {
            Some(OwnershipTokenKind::RuntimeTaskHandle)
        }
        "chan_open_u32" | "chan_open_buf" => Some(OwnershipTokenKind::ChannelHandle),
        "msg_log_open"
        | "msg_replay_open"
        | "stream_replay_open"
        | "batch_open"
        | "agg_open_u64"
        | "window_open_ms" => Some(OwnershipTokenKind::RuntimeHandle),
        "deadline_open_ms"
        | "cancel_scope_open"
        | "cancel_scope_child"
        | "retry_open"
        | "circuit_open"
        | "backpressure_open"
        | "supervisor_open" => Some(OwnershipTokenKind::RuntimeHandle),
        "service_open" => Some(OwnershipTokenKind::ServiceHandle),
        "service_trace_begin" => Some(OwnershipTokenKind::TraceHandle),
        _ => None,
    }
}

fn runtime_close_operands<'a>(instruction: &'a crate::ast::Instruction) -> Vec<&'a String> {
    match instruction.op.as_str() {
        "spawn_close"
        | "task_close"
        | "ffi_close_lib"
        | "db_close"
        | "db_pool_close"
        | "cache_close"
        | "queue_close"
        | "stream_close"
        | "lease_close"
        | "placement_close"
        | "coord_close"
        | "http_client_close"
        | "http_client_pool_close"
        | "net_close"
        | "http_session_close"
        | "http_body_stream_close"
        | "http_response_stream_close"
        | "rt_task_close"
        | "rt_close"
        | "chan_close"
        | "deadline_close"
        | "cancel_scope_close"
        | "retry_close"
        | "circuit_close"
        | "backpressure_close"
        | "supervisor_close"
        | "msg_log_close"
        | "msg_replay_close"
        | "stream_replay_close"
        | "batch_close"
        | "agg_close"
        | "window_close"
        | "service_close"
        | "service_trace_end" => instruction.args.first().into_iter().collect(),
        "db_pool_release" | "http_client_pool_release" => {
            instruction.args.get(1).into_iter().collect()
        }
        _ => Vec::new(),
    }
}

fn is_buf_u8_like_type(ty: &TypeRef) -> bool {
    match ty {
        TypeRef::Buf(inner)
            if **inner
                == TypeRef::Int {
                    signed: false,
                    bits: 8,
                } =>
        {
            true
        }
        TypeRef::Own(inner) | TypeRef::View(inner) | TypeRef::Edit(inner) => {
            is_buf_u8_like_type(inner)
        }
        _ => false,
    }
}

fn is_string_like_type(ty: &TypeRef) -> bool {
    match ty {
        TypeRef::String => true,
        TypeRef::Own(inner) | TypeRef::View(inner) | TypeRef::Edit(inner) => {
            is_string_like_type(inner)
        }
        _ => false,
    }
}

fn ownership_binding_kind(ty: &TypeRef) -> Option<OwnershipBindingKind> {
    match ty {
        TypeRef::Own(inner) if is_borrow_trackable_inner(inner.as_ref()) => {
            Some(OwnershipBindingKind::Owned)
        }
        TypeRef::View(inner) | TypeRef::Edit(inner) if is_borrow_trackable_inner(inner.as_ref()) => {
            Some(OwnershipBindingKind::Borrowed)
        }
        _ => None,
    }
}

fn validate_ownership_flow(
    function: &crate::ast::Function,
    block_map: &HashMap<String, &Block>,
) -> Vec<Diagnostic> {
    if !block_map.contains_key("b0") {
        return Vec::new();
    }
    let mut live_tokens = BTreeSet::new();
    let mut token_kinds = BTreeMap::new();
    for arg in &function.args {
        if is_owned_tracked_type(&arg.ty) {
            live_tokens.insert(arg.name.clone());
            token_kinds.insert(arg.name.clone(), BTreeSet::from([OwnershipTokenKind::Arg]));
        }
    }
    let initial = OwnershipState {
        live_tokens,
        param_tokens: BTreeMap::new(),
        token_kinds,
        borrow_tokens: BTreeSet::new(),
    };
    let mut worklist = VecDeque::from(["b0".to_string()]);
    let mut block_states = HashMap::from([("b0".to_string(), initial)]);
    let mut diagnostics = Vec::new();
    let mut emitted = HashSet::new();
    while let Some(block_label) = worklist.pop_front() {
        let Some(state) = block_states.get(&block_label).cloned() else {
            continue;
        };
        for (target_label, next_state) in analyze_ownership_block(
            function,
            &block_label,
            &state,
            block_map,
            &mut diagnostics,
            &mut emitted,
        ) {
            merge_ownership_state(
                function,
                &target_label,
                next_state,
                block_map,
                &mut block_states,
                &mut worklist,
                &mut diagnostics,
                &mut emitted,
            );
        }
    }
    diagnostics
}

fn analyze_ownership_block(
    function: &crate::ast::Function,
    block_label: &str,
    state: &OwnershipState,
    block_map: &HashMap<String, &Block>,
    diagnostics: &mut Vec<Diagnostic>,
    emitted: &mut HashSet<(String, String, Option<String>)>,
) -> Vec<(String, OwnershipState)> {
    let block_path = node_path(&[
        format!("func={}", function.name),
        format!("block={block_label}"),
    ]);
    let Some(block) = block_map.get(block_label) else {
        return Vec::new();
    };
    let mut name_to_token = HashMap::new();
    let mut name_to_binding_kind = HashMap::new();
    if block_label == "b0" {
        for arg in &function.args {
            if let Some(kind) = ownership_binding_kind(&arg.ty) {
                name_to_binding_kind.insert(arg.name.clone(), kind);
                if is_owned_tracked_type(&arg.ty) {
                    name_to_token.insert(arg.name.clone(), arg.name.clone());
                }
            }
        }
    }
    for param in &block.params {
        if let Some(token) = state.param_tokens.get(&param.name) {
            name_to_token.insert(param.name.clone(), token.clone());
            if let Some(kind) = ownership_binding_kind(&param.ty) {
                name_to_binding_kind.insert(param.name.clone(), kind);
            }
        }
    }
    let mut live_tokens = state.live_tokens.clone();
    let mut token_kinds = state.token_kinds.clone();
    let borrow_tokens = state.borrow_tokens.clone();
    let last_uses = block_operand_last_uses(block);
    for (instruction_index, instruction) in block.instructions.iter().enumerate() {
        let instruction_path =
            node_path(&[block_path.clone(), format!("instr={}", instruction.bind)]);
        for operand in instruction_binding_operands(instruction) {
            validate_flow_operand_live(
                operand,
                &instruction_path,
                &name_to_token,
                &live_tokens,
                &token_kinds,
                diagnostics,
                emitted,
            );
        }
        if instruction.op == "drop" {
            if let Some(handle) = instruction.args.first() {
                if let Some(token) = name_to_token.get(handle) {
                    let live_borrows = active_borrow_aliases_after(
                        token,
                        instruction_index,
                        &last_uses,
                        &name_to_binding_kind,
                        &name_to_token,
                    );
                    if !live_borrows.is_empty() {
                        push_flow_diagnostic(
                            diagnostics,
                            emitted,
                            Diagnostic::new(
                                "validate",
                                instruction_path.clone(),
                                "BORROW_STILL_LIVE_ON_DROP",
                                format!(
                                    "dropping {handle} while borrowed aliases are still live later in the block: {}",
                                    live_borrows.join(", ")
                                ),
                            )
                            .with_observed(live_borrows.join(", "))
                            .with_fix_hint(
                                "move drop after the last borrow use or stop passing the borrowed handle beyond this point",
                            ),
                        );
                    }
                    if !live_tokens.remove(token) {
                        push_flow_diagnostic(
                            diagnostics,
                            emitted,
                            Diagnostic::new(
                                "validate",
                                instruction_path.clone(),
                                "DOUBLE_DROP",
                                format!("value {handle} is dropped more than once"),
                            )
                            .with_observed(handle.clone())
                            .with_fix_hint("drop the owned handle exactly once along each path"),
                        );
                    }
                }
            }
        }
        for handle in runtime_close_operands(instruction) {
            if let Some(token) = name_to_token.get(handle) {
                if !live_tokens.remove(token) {
                    push_flow_diagnostic(
                        diagnostics,
                        emitted,
                        Diagnostic::new(
                            "validate",
                            instruction_path.clone(),
                            "DOUBLE_CLOSE",
                            format!("runtime handle {handle} is closed more than once"),
                        )
                        .with_observed(handle.clone())
                        .with_fix_hint("close each runtime handle exactly once along each path"),
                    );
                }
            }
        }
        if is_owned_tracked_type(&instruction.ty) {
            let token = instruction.bind.clone();
            name_to_token.insert(instruction.bind.clone(), token.clone());
            name_to_binding_kind.insert(instruction.bind.clone(), OwnershipBindingKind::Owned);
            live_tokens.insert(token);
            let kinds = if instruction.op == "alloc" {
                let kind = match instruction.args.first().map(String::as_str) {
                    Some("heap") => OwnershipTokenKind::HeapAlloc,
                    Some("stack") => OwnershipTokenKind::StackAlloc,
                    Some("arena") => OwnershipTokenKind::ArenaAlloc,
                    _ => OwnershipTokenKind::Other,
                };
                BTreeSet::from([kind])
            } else if matches!(
                instruction.op.as_str(),
                "fs_read_all"
                    | "net_exchange_all"
                    | "net_serve_exchange_all"
                    | "net_read_all"
                    | "session_read_chunk"
                    | "spawn_capture_all"
                    | "spawn_capture_stderr_all"
                    | "spawn_stdout_all"
                    | "spawn_stderr_all"
            ) {
                BTreeSet::from([OwnershipTokenKind::HeapAlloc])
            } else {
                BTreeSet::from([OwnershipTokenKind::Other])
            };
            token_kinds.insert(instruction.bind.clone(), kinds);
        } else if is_tracked_reference_type(&instruction.ty) {
            name_to_binding_kind.insert(instruction.bind.clone(), OwnershipBindingKind::Borrowed);
            if let Some(source) = instruction.args.first() {
                if let Some(token) = name_to_token.get(source) {
                    name_to_token.insert(instruction.bind.clone(), token.clone());
                }
            }
        } else if let Some(kind) = runtime_handle_kind_for_instruction(instruction) {
            let token = instruction.bind.clone();
            name_to_token.insert(instruction.bind.clone(), token.clone());
            live_tokens.insert(token.clone());
            token_kinds.insert(token, BTreeSet::from([kind]));
        }
    }
    match &block.terminator {
        Terminator::Return(value) => {
            let node = node_path(&[block_path.clone(), "term=return".to_string()]);
            validate_flow_operand_live(
                value,
                &node,
                &name_to_token,
                &live_tokens,
                &token_kinds,
                diagnostics,
                emitted,
            );
            let returned = if name_to_binding_kind.get(value) == Some(&OwnershipBindingKind::Owned) {
                name_to_token.get(value).cloned()
            } else {
                None
            };
            if let Some(returned_token) = name_to_token.get(value) {
                if live_tokens.contains(returned_token)
                    && token_is_runtime_handle(returned_token, &token_kinds)
                {
                    push_flow_diagnostic(
                        diagnostics,
                        emitted,
                        Diagnostic::new(
                            "validate",
                            node.clone(),
                            "RUNTIME_HANDLE_ESCAPE",
                            format!("open runtime handle {value} escapes the function return"),
                        )
                        .with_observed(value.clone())
                        .with_fix_hint(
                            "close the runtime handle before returning or lower the result to a scalar value",
                        ),
                    );
                }
            }
            if let Some(returned_token) = returned.as_ref() {
                if token_kinds
                    .get(returned_token)
                    .is_some_and(|kinds| kinds.contains(&OwnershipTokenKind::ArenaAlloc))
                {
                    push_flow_diagnostic(
                        diagnostics,
                        emitted,
                        Diagnostic::new(
                            "validate",
                            node.clone(),
                            "ARENA_RETURN_UNSUPPORTED",
                            format!(
                                "returning arena-allocated buffer handle {value} is not supported"
                            ),
                        )
                        .with_observed(value.clone())
                        .with_fix_hint(
                            "return a copied scalar/aggregate value or switch that allocation to heap",
                        ),
                    );
                }
                if token_kinds
                    .get(returned_token)
                    .is_some_and(|kinds| kinds.contains(&OwnershipTokenKind::StackAlloc))
                {
                    push_flow_diagnostic(
                        diagnostics,
                        emitted,
                        Diagnostic::new(
                            "validate",
                            node.clone(),
                            "STACK_RETURN_UNSUPPORTED",
                            format!(
                                "returning stack-allocated buffer handle {value} is not supported"
                            ),
                        )
                        .with_observed(value.clone())
                        .with_fix_hint(
                            "return a heap-owned handle or copy out the needed value before returning",
                        ),
                    );
                }
            }
            if name_to_binding_kind.get(value) == Some(&OwnershipBindingKind::Borrowed)
                && name_to_token.contains_key(value)
            {
                push_flow_diagnostic(
                    diagnostics,
                    emitted,
                    Diagnostic::new(
                        "validate",
                        node.clone(),
                        "BORROW_ESCAPE",
                        format!("borrowed buffer handle {value} escapes the function return"),
                    )
                    .with_observed(value.clone())
                    .with_fix_hint(
                        "return an owned handle or copy out the needed value before returning",
                    ),
                );
            }
            let leaked_runtime_handles = live_tokens
                .iter()
                .filter(|token| Some((*token).clone()) != returned)
                .filter(|token| token_is_runtime_handle(token, &token_kinds))
                .cloned()
                .collect::<Vec<_>>();
            let leaked = live_tokens
                .iter()
                .filter(|token| Some((*token).clone()) != returned)
                .filter(|token| {
                    token_kinds
                        .get(*token)
                        .is_some_and(|kinds| kinds.contains(&OwnershipTokenKind::HeapAlloc))
                })
                .cloned()
                .collect::<Vec<_>>();
            let escaped_args = live_tokens
                .iter()
                .filter(|token| Some((*token).clone()) != returned)
                .filter(|token| {
                    token_kinds
                        .get(*token)
                        .is_some_and(|kinds| kinds.contains(&OwnershipTokenKind::Arg))
                })
                .cloned()
                .collect::<Vec<_>>();
            if !leaked.is_empty() {
                push_flow_diagnostic(
                    diagnostics,
                    emitted,
                    Diagnostic::new(
                        "validate",
                        node.clone(),
                        "HEAP_BUFFER_LEAK",
                        format!(
                            "heap-owned buffers escape this return path without drop: {}",
                            leaked.join(", ")
                        ),
                    )
                    .with_observed(leaked.join(", "))
                    .with_fix_hint(
                        "drop heap-owned buffers before returning or return the owned handle",
                    ),
                );
            }
            if !leaked_runtime_handles.is_empty() {
                push_flow_diagnostic(
                    diagnostics,
                    emitted,
                    Diagnostic::new(
                        "validate",
                        node.clone(),
                        "RUNTIME_HANDLE_LEAK",
                        format!(
                            "runtime handles escape this return path without close: {}",
                            leaked_runtime_handles.join(", ")
                        ),
                    )
                    .with_observed(leaked_runtime_handles.join(", "))
                    .with_fix_hint(
                        "close every opened runtime handle before returning or transfer it through an explicit runtime-managed API",
                    ),
                );
            }
            if !escaped_args.is_empty() {
                push_flow_diagnostic(
                    diagnostics,
                    emitted,
                    Diagnostic::new(
                        "validate",
                        node,
                        "OWNED_ARG_ESCAPE",
                        format!(
                            "owned buffer arguments escape this return path without drop: {}",
                            escaped_args.join(", ")
                        ),
                    )
                    .with_observed(escaped_args.join(", "))
                    .with_fix_hint(
                        "drop owned buffer arguments before returning or return the owned handle",
                    ),
                );
            }
            Vec::new()
        }
        Terminator::Jump(target) => propagate_ownership_target(
            function,
            &block_path,
            target,
            &name_to_token,
            &live_tokens,
            &token_kinds,
            &borrow_tokens,
            block_map,
            diagnostics,
            emitted,
        )
        .into_iter()
        .collect(),
        Terminator::Branch {
            condition,
            truthy,
            falsy,
        } => {
            let node = node_path(&[block_path.clone(), "term=branch".to_string()]);
            validate_flow_operand_live(
                condition,
                &node,
                &name_to_token,
                &live_tokens,
                &token_kinds,
                diagnostics,
                emitted,
            );
            let mut targets = Vec::new();
            if let Some(next) = propagate_ownership_target(
                function,
                &block_path,
                truthy,
                &name_to_token,
                &live_tokens,
                &token_kinds,
                &borrow_tokens,
                block_map,
                diagnostics,
                emitted,
            ) {
                targets.push(next);
            }
            if let Some(next) = propagate_ownership_target(
                function,
                &block_path,
                falsy,
                &name_to_token,
                &live_tokens,
                &token_kinds,
                &borrow_tokens,
                block_map,
                diagnostics,
                emitted,
            ) {
                targets.push(next);
            }
            targets
        }
        Terminator::Match { value, arms } => {
            let node = node_path(&[block_path.clone(), "term=match".to_string()]);
            validate_flow_operand_live(
                value,
                &node,
                &name_to_token,
                &live_tokens,
                &token_kinds,
                diagnostics,
                emitted,
            );
            let mut targets = Vec::new();
            for arm in arms {
                if let Some(next) = propagate_ownership_target(
                    function,
                    &block_path,
                    arm,
                    &name_to_token,
                    &live_tokens,
                    &token_kinds,
                    &borrow_tokens,
                    block_map,
                    diagnostics,
                    emitted,
                ) {
                    targets.push(next);
                }
            }
            targets
        }
    }
}

fn propagate_ownership_target(
    _function: &crate::ast::Function,
    block_path: &str,
    target: &crate::ast::Target,
    name_to_token: &HashMap<String, String>,
    live_tokens: &BTreeSet<String>,
    token_kinds: &BTreeMap<String, BTreeSet<OwnershipTokenKind>>,
    _borrow_tokens: &BTreeSet<String>,
    block_map: &HashMap<String, &Block>,
    diagnostics: &mut Vec<Diagnostic>,
    emitted: &mut HashSet<(String, String, Option<String>)>,
) -> Option<(String, OwnershipState)> {
    let Some(block) = block_map.get(&target.label) else {
        return None;
    };
    let mut next_params = BTreeMap::new();
    let mut next_live_tokens = BTreeSet::new();
    let mut next_token_kinds = BTreeMap::new();
    let mut next_borrow_tokens = BTreeSet::new();
    let mut target_token_bindings = BTreeMap::new();
    let mut canonical_tokens = BTreeMap::new();
    let target_path = node_path(&[block_path.to_string(), format!("target={}", target.label)]);
    for (operand, param) in target.args.iter().zip(block.params.iter()) {
        validate_flow_operand_live(
            operand,
            &target_path,
            name_to_token,
            live_tokens,
            token_kinds,
            diagnostics,
            emitted,
        );
        if let Some(token) = name_to_token.get(operand) {
            let buffer_handle = is_tracked_reference_type(&param.ty);
            let runtime_handle = token_is_runtime_handle(token, token_kinds) && is_u64_type(&param.ty);
            if buffer_handle || runtime_handle {
                let canonical = canonical_tokens
                    .entry(token.clone())
                    .or_insert_with(|| format!("{}::{}", target.label, param.name))
                    .clone();
                if is_owned_tracked_type(&param.ty) {
                    if let Some(first_param) =
                        target_token_bindings.insert(token.clone(), param.name.clone())
                    {
                        push_flow_diagnostic(
                            diagnostics,
                            emitted,
                            Diagnostic::new(
                                "validate",
                                target_path.clone(),
                                "OWNED_PARAM_ALIAS",
                                format!(
                                    "owned buffer {operand} is passed to multiple owned block params: {first_param}, {}",
                                    param.name
                                ),
                            )
                            .with_observed(operand.to_string())
                            .with_fix_hint(
                                "pass each owned buffer handle to at most one owned block param per transfer",
                            ),
                        );
                    }
                    next_live_tokens.insert(canonical.clone());
                } else if runtime_handle {
                    if let Some(first_param) =
                        target_token_bindings.insert(token.clone(), param.name.clone())
                    {
                        push_flow_diagnostic(
                            diagnostics,
                            emitted,
                            Diagnostic::new(
                                "validate",
                                target_path.clone(),
                                "RUNTIME_HANDLE_ALIAS",
                                format!(
                                    "runtime handle {operand} is passed to multiple runtime block params: {first_param}, {}",
                                    param.name
                                ),
                            )
                            .with_observed(operand.to_string())
                            .with_fix_hint(
                                "pass each runtime handle to at most one runtime target param per transfer",
                            ),
                        );
                    }
                    next_live_tokens.insert(canonical.clone());
                }
                if let Some(kinds) = token_kinds.get(token) {
                    next_token_kinds
                        .entry(canonical.clone())
                        .or_insert_with(BTreeSet::new)
                        .extend(kinds.iter().copied());
                }
                if matches!(
                    ownership_binding_kind(&param.ty),
                    Some(OwnershipBindingKind::Borrowed)
                ) {
                    next_borrow_tokens.insert(canonical.clone());
                }
                next_params.insert(param.name.clone(), canonical);
            }
        }
    }
    for (name, token) in name_to_token {
        if live_tokens.contains(token) && !target_token_bindings.contains_key(token) {
            let diagnostic = if token_is_runtime_handle(token, token_kinds) {
                Diagnostic::new(
                    "validate",
                    target_path.clone(),
                    "RUNTIME_HANDLE_TRANSFER_MISSING",
                    format!(
                        "runtime handle {name} leaves block control flow without an explicit transfer to {}",
                        target.label
                    ),
                )
                .with_observed(name.clone())
                .with_fix_hint(
                    "close the runtime handle before the terminator or pass it through a u64 target param",
                )
            } else {
                Diagnostic::new(
                    "validate",
                    target_path.clone(),
                    "OWNED_TRANSFER_MISSING",
                    format!(
                        "owned buffer {name} leaves block control flow without an explicit transfer to {}",
                        target.label
                    ),
                )
                .with_observed(name.clone())
                .with_fix_hint(
                    "drop the owned value before the terminator or pass it through an owned target param",
                )
            };
            push_flow_diagnostic(diagnostics, emitted, diagnostic);
        }
    }
    for token in &next_borrow_tokens {
        if !next_live_tokens.contains(token) {
            push_flow_diagnostic(
                diagnostics,
                emitted,
                Diagnostic::new(
                    "validate",
                    target_path.clone(),
                    "BORROW_OWNER_TRANSFER_MISSING",
                    format!(
                        "borrowed buffer crossing into {} requires the matching owner to be transferred on the same edge",
                        target.label
                    ),
                )
                .with_observed(token.clone())
                .with_fix_hint(
                    "pass the owned handle through an owned target param alongside the borrowed handle, or end the borrow before the terminator",
                ),
            );
        }
    }
    Some((
        target.label.clone(),
        OwnershipState {
            live_tokens: next_live_tokens,
            param_tokens: next_params,
            token_kinds: next_token_kinds,
            borrow_tokens: next_borrow_tokens,
        },
    ))
}

fn format_ownership_state(state: &OwnershipState) -> String {
    let live = state
        .live_tokens
        .iter()
        .cloned()
        .collect::<Vec<_>>()
        .join(",");
    let params = state
        .param_tokens
        .iter()
        .map(|(name, token)| format!("{name}->{token}"))
        .collect::<Vec<_>>()
        .join(",");
    let kinds = state
        .token_kinds
        .iter()
        .map(|(token, kinds)| {
            let names = kinds
                .iter()
                .map(|kind| match kind {
                    OwnershipTokenKind::Arg => "arg",
                    OwnershipTokenKind::HeapAlloc => "heap",
                    OwnershipTokenKind::StackAlloc => "stack",
                    OwnershipTokenKind::ArenaAlloc => "arena",
                    OwnershipTokenKind::SpawnHandle => "spawn-handle",
                    OwnershipTokenKind::TaskHandle => "task-handle",
                    OwnershipTokenKind::SocketHandle => "socket-handle",
                    OwnershipTokenKind::SessionHandle => "session-handle",
                    OwnershipTokenKind::DbHandle => "db-handle",
                    OwnershipTokenKind::DbPoolHandle => "db-pool-handle",
                    OwnershipTokenKind::CacheHandle => "cache-handle",
                    OwnershipTokenKind::QueueHandle => "queue-handle",
                    OwnershipTokenKind::RuntimeHandle => "runtime-handle",
                    OwnershipTokenKind::RuntimeTaskHandle => "runtime-task-handle",
                    OwnershipTokenKind::ChannelHandle => "channel-handle",
                    OwnershipTokenKind::FfiLibHandle => "ffi-lib-handle",
                    OwnershipTokenKind::ServiceHandle => "service-handle",
                    OwnershipTokenKind::TraceHandle => "trace-handle",
                    OwnershipTokenKind::Other => "other",
                })
                .collect::<Vec<_>>()
                .join("|");
            format!("{token}:{names}")
        })
        .collect::<Vec<_>>()
        .join(",");
    let borrows = state
        .borrow_tokens
        .iter()
        .cloned()
        .collect::<Vec<_>>()
        .join(",");
    format!("live=[{live}] params=[{params}] kinds=[{kinds}] borrows=[{borrows}]")
}

fn block_operand_last_uses(block: &Block) -> HashMap<String, usize> {
    let mut last_uses = HashMap::new();
    for (index, instruction) in block.instructions.iter().enumerate() {
        for operand in instruction_binding_operands(instruction) {
            last_uses.insert(operand.clone(), index);
        }
    }
    let terminator_index = block.instructions.len();
    match &block.terminator {
        Terminator::Return(value) => {
            last_uses.insert(value.clone(), terminator_index);
        }
        Terminator::Jump(target) => {
            for operand in &target.args {
                last_uses.insert(operand.clone(), terminator_index);
            }
        }
        Terminator::Branch {
            condition,
            truthy,
            falsy,
        } => {
            last_uses.insert(condition.clone(), terminator_index);
            for operand in &truthy.args {
                last_uses.insert(operand.clone(), terminator_index);
            }
            for operand in &falsy.args {
                last_uses.insert(operand.clone(), terminator_index);
            }
        }
        Terminator::Match { value, arms } => {
            last_uses.insert(value.clone(), terminator_index);
            for arm in arms {
                for operand in &arm.args {
                    last_uses.insert(operand.clone(), terminator_index);
                }
            }
        }
    }
    last_uses
}

fn active_borrow_aliases_after(
    token: &str,
    instruction_index: usize,
    last_uses: &HashMap<String, usize>,
    name_to_binding_kind: &HashMap<String, OwnershipBindingKind>,
    name_to_token: &HashMap<String, String>,
) -> Vec<String> {
    let mut borrows = name_to_binding_kind
        .iter()
        .filter(|(_, kind)| **kind == OwnershipBindingKind::Borrowed)
        .filter_map(|(name, _)| {
            let alias_token = name_to_token.get(name)?;
            let last_use = last_uses.get(name)?;
            if alias_token == token && *last_use > instruction_index {
                Some(name.clone())
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    borrows.sort();
    borrows.dedup();
    borrows
}

fn merge_ownership_state(
    function: &crate::ast::Function,
    block_label: &str,
    incoming: OwnershipState,
    block_map: &HashMap<String, &Block>,
    block_states: &mut HashMap<String, OwnershipState>,
    worklist: &mut VecDeque<String>,
    diagnostics: &mut Vec<Diagnostic>,
    emitted: &mut HashSet<(String, String, Option<String>)>,
) {
    let block_path = node_path(&[
        format!("func={}", function.name),
        format!("block={block_label}"),
    ]);
    let Some(existing) = block_states.get(block_label).cloned() else {
        block_states.insert(block_label.to_string(), incoming);
        worklist.push_back(block_label.to_string());
        return;
    };
    let Some(block) = block_map.get(block_label) else {
        return;
    };
    let existing_owned_params =
        filtered_param_tokens(block, &existing, OwnershipBindingKind::Owned);
    let incoming_owned_params =
        filtered_param_tokens(block, &incoming, OwnershipBindingKind::Owned);
    let existing_borrow_params =
        filtered_param_tokens(block, &existing, OwnershipBindingKind::Borrowed);
    let incoming_borrow_params =
        filtered_param_tokens(block, &incoming, OwnershipBindingKind::Borrowed);
    if existing.live_tokens == incoming.live_tokens
        && existing.token_kinds == incoming.token_kinds
        && existing_owned_params == incoming_owned_params
        && existing_borrow_params == incoming_borrow_params
    {
        let mut merged = existing.clone();
        merged
            .borrow_tokens
            .extend(incoming.borrow_tokens.iter().cloned());
        if merged != existing {
            block_states.insert(block_label.to_string(), merged);
            worklist.push_back(block_label.to_string());
        }
        return;
    }
    let (error_code, message) = if existing.live_tokens == incoming.live_tokens
        && existing.token_kinds == incoming.token_kinds
        && existing_owned_params == incoming_owned_params
        && (existing_borrow_params != incoming_borrow_params
            || existing.borrow_tokens != incoming.borrow_tokens)
    {
        (
            "BORROW_JOIN_MISMATCH",
            format!(
                "block {block_label} is reached with incompatible borrowed-handle state across CFG paths"
            ),
        )
    } else {
        (
            "OWNERSHIP_JOIN_MISMATCH",
            format!(
                "block {block_label} is reached with inconsistent ownership state across CFG paths"
            ),
        )
    };
    push_flow_diagnostic(
        diagnostics,
        emitted,
        Diagnostic::new(
            "validate",
            block_path,
            error_code,
            message,
        )
        .with_expected(format_ownership_state(&existing))
        .with_observed(format_ownership_state(&incoming))
        .with_fix_hint(
            "make every predecessor transfer the same owned and borrowed handles into matching block params or split the block",
        ),
    );
}

fn filtered_param_tokens(
    block: &Block,
    state: &OwnershipState,
    kind: OwnershipBindingKind,
) -> BTreeMap<String, String> {
    block
        .params
        .iter()
        .filter(|param| {
            if ownership_binding_kind(&param.ty) == Some(kind) {
                return true;
            }
            if kind == OwnershipBindingKind::Owned && is_u64_type(&param.ty) {
                return state
                    .param_tokens
                    .get(&param.name)
                    .is_some_and(|token| token_is_runtime_handle(token, &state.token_kinds));
            }
            false
        })
        .filter_map(|param| {
            state
                .param_tokens
                .get(&param.name)
                .map(|token| (param.name.clone(), token.clone()))
        })
        .collect()
}

fn validate_flow_operand_live(
    operand: &str,
    node: &str,
    name_to_token: &HashMap<String, String>,
    live_tokens: &BTreeSet<String>,
    token_kinds: &BTreeMap<String, BTreeSet<OwnershipTokenKind>>,
    diagnostics: &mut Vec<Diagnostic>,
    emitted: &mut HashSet<(String, String, Option<String>)>,
) {
    if let Some(token) = name_to_token.get(operand) {
        if !live_tokens.contains(token) {
            let (error_code, message, fix_hint) = if token_is_runtime_handle(token, token_kinds) {
                (
                    "USE_AFTER_CLOSE",
                    format!("runtime handle {operand} is used after close"),
                    "move close after the last use or pass a still-open handle",
                )
            } else {
                (
                    "USE_AFTER_DROP",
                    format!("value {operand} is used after drop"),
                    "move drop closer to the last use or pass a still-live handle",
                )
            };
            push_flow_diagnostic(
                diagnostics,
                emitted,
                Diagnostic::new(
                    "validate",
                    node.to_string(),
                    error_code,
                    message,
                )
                .with_observed(operand.to_string())
                .with_fix_hint(fix_hint),
            );
        }
    }
}

fn push_flow_diagnostic(
    diagnostics: &mut Vec<Diagnostic>,
    emitted: &mut HashSet<(String, String, Option<String>)>,
    diagnostic: Diagnostic,
) {
    let key = (
        diagnostic.node.clone(),
        diagnostic.error_code.clone(),
        diagnostic.observed.clone(),
    );
    if emitted.insert(key) {
        diagnostics.push(diagnostic);
    }
}

fn resolve_operand_type(token: &str, env: &HashMap<String, TypeRef>) -> Option<TypeRef> {
    env.get(token)
        .cloned()
        .or_else(|| infer_literal_type(token))
}
