use std::collections::BTreeMap;

use crate::codegen_c::LoweredExecBinaryOp;
use crate::lowered_bytecode::{BytecodeImmediate, BytecodeProgram, BytecodeValueKind};
use crate::machine_ir::{
    lower_bytecode_to_machine_program, validate_machine_program, MachineBlock as BytecodeBlock,
    MachineEdge as BytecodeEdge, MachineExpr as BytecodeExpr, MachineFunction as BytecodeFunction,
    MachineInstruction as BytecodeInstruction, MachineMatchCase as BytecodeMatchCase,
    MachineOperand as BytecodeOperand, MachineProgram, MachineTerminator as BytecodeTerminator,
};

const REG_POOL: [&str; 6] = ["x19", "x20", "x21", "x22", "x23", "x24"];

#[derive(Debug, Clone, Copy)]
enum SlotHome {
    Stack(usize),
    Reg(&'static str),
}

#[derive(Debug, Clone, Copy)]
struct SlotLayout {
    home: SlotHome,
    kind: BytecodeValueKind,
}

struct FrameLayout {
    slots: Vec<SlotLayout>,
    saved_regs: Vec<&'static str>,
    temp_base: usize,
    temp_count: usize,
    rand_state_offset: Option<usize>,
    frame_size: usize,
}

impl FrameLayout {
    fn slot(&self, index: usize) -> Result<SlotLayout, String> {
        self.slots
            .get(index)
            .copied()
            .ok_or_else(|| format!("missing frame slot {index}"))
    }

    fn temp_slot(&self, index: usize, kind: BytecodeValueKind) -> Result<SlotLayout, String> {
        if index >= self.temp_count {
            return Err(format!("missing edge temp slot {index}"));
        }
        Ok(SlotLayout {
            home: SlotHome::Stack(self.temp_base + (index * 16)),
            kind,
        })
    }
}

#[derive(Debug, Clone)]
struct RuntimeStrings {
    labels: BTreeMap<String, String>,
    ordered: Vec<(String, String)>,
}

pub fn supports_arm64_asm_backend() -> bool {
    cfg!(target_arch = "aarch64")
}

pub fn emit_arm64_library(program: &BytecodeProgram) -> Result<String, String> {
    let machine = lower_bytecode_to_machine_program(program);
    validate_machine_program(&machine)?;
    emit_arm64_machine_library(&machine)
}

fn emit_arm64_machine_library(program: &MachineProgram) -> Result<String, String> {
    let mut out = String::new();
    out.push_str(".text\n");
    out.push_str(".p2align 2\n\n");
    for function in &program.functions {
        let strings = collect_runtime_strings(function);
        out.push_str(&emit_arm64_function(function, &strings)?);
        emit_arm64_string_section(&mut out, &strings);
        out.push('\n');
    }
    Ok(out)
}

fn emit_arm64_function(
    function: &BytecodeFunction,
    strings: &RuntimeStrings,
) -> Result<String, String> {
    let frame = build_frame_layout(function);
    let symbol = exported_symbol(function);
    let mut out = String::new();
    out.push_str(&format!(".globl {symbol}\n"));
    out.push_str(&format!("{symbol}:\n"));
    out.push_str("  stp x29, x30, [sp, #-16]!\n");
    out.push_str("  mov x29, sp\n");
    if frame.frame_size > 0 {
        out.push_str(&format!("  sub sp, sp, #{}\n", frame.frame_size));
    }
    emit_save_regs(&mut out, &frame);
    emit_init_args(&mut out, function, &frame)?;
    emit_init_rand_state(&mut out, &frame, function)?;
    if function.entry_block != 0 {
        out.push_str(&format!(
            "  b {}\n",
            block_label(function, function.entry_block)
        ));
    }
    for (index, block) in function.blocks.iter().enumerate() {
        out.push_str(&format!("{}:\n", block_label(function, index)));
        emit_block(&mut out, function, block, index, &frame, strings)?;
    }
    Ok(out)
}

fn emit_save_regs(out: &mut String, frame: &FrameLayout) {
    for (index, reg) in frame.saved_regs.iter().enumerate() {
        out.push_str(&format!("  str {reg}, [sp, #{}]\n", index * 8));
    }
}

fn emit_restore_regs(out: &mut String, frame: &FrameLayout) {
    for (index, reg) in frame.saved_regs.iter().enumerate().rev() {
        out.push_str(&format!("  ldr {reg}, [sp, #{}]\n", index * 8));
    }
}

fn emit_init_args(
    out: &mut String,
    function: &BytecodeFunction,
    frame: &FrameLayout,
) -> Result<(), String> {
    let mut next_arg_reg = 0usize;
    for arg in &function.arg_slots {
        let dst = frame.slot(arg.slot)?;
        match arg.kind {
            BytecodeValueKind::SpanI32 => {
                let ptr = reg_x(next_arg_reg)?;
                let len = reg_x(next_arg_reg + 1)?;
                emit_store_span_regs(out, dst, ptr, len)?;
                next_arg_reg += 2;
            }
            _ => {
                let reg = reg_x(next_arg_reg)?;
                emit_store_scalar_reg(out, dst, reg)?;
                next_arg_reg += 1;
            }
        }
    }
    Ok(())
}

fn emit_init_rand_state(
    out: &mut String,
    frame: &FrameLayout,
    function: &BytecodeFunction,
) -> Result<(), String> {
    let Some(offset) = frame.rand_state_offset else {
        return Ok(());
    };
    let seed = function.rand_seed.unwrap_or(0);
    emit_load_immediate(out, "x9", seed as i64, false)?;
    out.push_str(&format!("  str w9, [sp, #{}]\n", offset));
    Ok(())
}

fn emit_block(
    out: &mut String,
    function: &BytecodeFunction,
    block: &BytecodeBlock,
    block_index: usize,
    frame: &FrameLayout,
    strings: &RuntimeStrings,
) -> Result<(), String> {
    for instruction in &block.instructions {
        emit_instruction(out, frame, strings, instruction)?;
    }
    emit_terminator(
        out,
        function,
        block_index,
        frame,
        strings,
        &block.terminator,
    )
}

fn emit_instruction(
    out: &mut String,
    frame: &FrameLayout,
    strings: &RuntimeStrings,
    instruction: &BytecodeInstruction,
) -> Result<(), String> {
    emit_expr_to_dst(
        out,
        frame,
        strings,
        frame.slot(instruction.dst)?,
        &instruction.expr,
    )
}

fn emit_expr_to_dst(
    out: &mut String,
    frame: &FrameLayout,
    strings: &RuntimeStrings,
    dst: SlotLayout,
    expr: &BytecodeExpr,
) -> Result<(), String> {
    let result = match expr {
        BytecodeExpr::Move(operand) => emit_move_expr(out, frame, dst, operand),
        BytecodeExpr::AllocBufU8 { .. }
        | BytecodeExpr::FsReadAllU8 { .. }
        | BytecodeExpr::FsWriteAllU8 { .. }
        | BytecodeExpr::NetWriteAllU8 { .. }
        | BytecodeExpr::NetExchangeAllU8 { .. }
        | BytecodeExpr::NetServeExchangeAllU8 { .. }
        | BytecodeExpr::NetListen { .. }
        | BytecodeExpr::NetAccept { .. }
        | BytecodeExpr::NetReadAllU8 { .. }
        | BytecodeExpr::NetWriteHandleAllU8 { .. }
        | BytecodeExpr::HttpMethodEq { .. }
        | BytecodeExpr::HttpRequestMethod { .. }
        | BytecodeExpr::HttpPathEq { .. }
        | BytecodeExpr::HttpRequestPath { .. }
        | BytecodeExpr::HttpHeaderEq { .. }
        | BytecodeExpr::HttpCookieEq { .. }
        | BytecodeExpr::HttpRouteParam { .. }
        | BytecodeExpr::HttpBodyLimit { .. }
        | BytecodeExpr::StrLit { .. }
        | BytecodeExpr::BufConcat { .. }
        | BytecodeExpr::StrConcat { .. }
        | BytecodeExpr::BufEqLit { .. }
        | BytecodeExpr::StrEqLit { .. }
        | BytecodeExpr::HttpQueryParam { .. }
        | BytecodeExpr::HttpHeader { .. }
        | BytecodeExpr::HttpCookie { .. }
        | BytecodeExpr::BufParseU32 { .. }
        | BytecodeExpr::BufParseBool { .. }
        | BytecodeExpr::StrFromU32 { .. }
        | BytecodeExpr::StrFromBool { .. }
        | BytecodeExpr::StrToBuf { .. }
        | BytecodeExpr::BufToStr { .. }
        | BytecodeExpr::BufHexStr { .. }
        | BytecodeExpr::HttpWriteResponse { .. }
        | BytecodeExpr::HttpWriteTextResponse { .. }
        | BytecodeExpr::HttpWriteTextResponseCookie { .. }
        | BytecodeExpr::HttpWriteTextResponseHeaders2 { .. }
        | BytecodeExpr::HttpWriteJsonResponse { .. }
        | BytecodeExpr::HttpWriteJsonResponseCookie { .. }
        | BytecodeExpr::HttpWriteJsonResponseHeaders2 { .. }
        | BytecodeExpr::HttpSessionWriteTextHeaders2 { .. }
        | BytecodeExpr::HttpSessionWriteTextCookie { .. }
        | BytecodeExpr::HttpSessionWriteJson { .. }
        | BytecodeExpr::HttpSessionWriteJsonCookie { .. }
        | BytecodeExpr::HttpSessionWriteJsonHeaders2 { .. }
        | BytecodeExpr::HttpWriteResponseHeader { .. }
        | BytecodeExpr::JsonGetBool { .. }
        | BytecodeExpr::JsonGetBufU8 { .. }
        | BytecodeExpr::JsonGetStr { .. }
        | BytecodeExpr::JsonArrayLen { .. }
        | BytecodeExpr::JsonIndexU32 { .. }
        | BytecodeExpr::JsonIndexBool { .. }
        | BytecodeExpr::JsonIndexStr { .. }
        | BytecodeExpr::JsonEncodeObj { .. }
        | BytecodeExpr::HttpStatusU32 { .. }
        | BytecodeExpr::HttpServerConfigU32 { .. }
        | BytecodeExpr::ConfigGetU32 { .. }
        | BytecodeExpr::ConfigGetBool { .. }
        | BytecodeExpr::ConfigGetStr { .. }
        | BytecodeExpr::EnvGetU32 { .. }
        | BytecodeExpr::EnvGetBool { .. }
        | BytecodeExpr::EnvGetStr { .. }
        | BytecodeExpr::DateParseYmd { .. }
        | BytecodeExpr::TimeParseHms { .. }
        | BytecodeExpr::DateFormatYmd { .. }
        | BytecodeExpr::TimeFormatHms { .. }
        | BytecodeExpr::DbExec { .. }
        | BytecodeExpr::DbPrepare { .. }
        | BytecodeExpr::DbExecPrepared { .. }
        | BytecodeExpr::DbQueryBufU8 { .. }
        | BytecodeExpr::DbQueryPreparedU32 { .. }
        | BytecodeExpr::DbQueryPreparedBufU8 { .. }
        | BytecodeExpr::DbBegin { .. }
        | BytecodeExpr::DbCommit { .. }
        | BytecodeExpr::DbRollback { .. }
        | BytecodeExpr::DbPoolOpen { .. }
        | BytecodeExpr::DbPoolAcquire { .. }
        | BytecodeExpr::DbPoolRelease { .. }
        | BytecodeExpr::DbPoolClose { .. }
        | BytecodeExpr::TlsExchangeAllU8 { .. }
        | BytecodeExpr::TaskOpen { .. }
        | BytecodeExpr::TaskDone { .. }
        | BytecodeExpr::TaskJoinStatus { .. }
        | BytecodeExpr::TaskStdoutAllU8 { .. }
        | BytecodeExpr::TaskStderrAllU8 { .. }
        | BytecodeExpr::TaskClose { .. }
        | BytecodeExpr::SpawnCaptureAllU8 { .. }
        | BytecodeExpr::SpawnCaptureStderrAllU8 { .. }
        | BytecodeExpr::SpawnOpen { .. }
        | BytecodeExpr::SpawnWait { .. }
        | BytecodeExpr::SpawnStdoutAllU8 { .. }
        | BytecodeExpr::SpawnStderrAllU8 { .. }
        | BytecodeExpr::SpawnClose { .. }
        | BytecodeExpr::BufContainsLit { .. }
        | BytecodeExpr::LenBufU8 { .. }
        | BytecodeExpr::StoreBufU8 { .. }
        | BytecodeExpr::LoadBufU8 { .. } => {
            Err("arm64 backend does not yet support buf[u8] filesystem/runtime ops".to_string())
        }
        BytecodeExpr::ClockNowNs => emit_clock_call(out, dst),
        BytecodeExpr::RandU32 => emit_rand_call(out, frame, dst),
        BytecodeExpr::DropBufU8 { value } => emit_drop_buf_call(out, frame, dst, value),
        BytecodeExpr::RtOpen { workers } => emit_rt_open_call(out, frame, dst, workers),
        BytecodeExpr::RtSpawnU32 {
            runtime,
            function,
            arg,
        } => emit_rt_spawn_u32_call(out, frame, dst, strings, runtime, function, arg),
        BytecodeExpr::RtDone { task } => emit_rt_done_call(out, frame, dst, task),
        BytecodeExpr::RtJoinU32 { task } => emit_rt_join_u32_call(out, frame, dst, task),
        BytecodeExpr::RtCancel { task } => emit_rt_cancel_call(out, frame, dst, task),
        BytecodeExpr::RtTaskClose { task } => emit_rt_task_close_call(out, frame, dst, task),
        BytecodeExpr::RtShutdown { runtime, grace_ms } => {
            emit_rt_shutdown_call(out, frame, dst, runtime, grace_ms)
        }
        BytecodeExpr::RtClose { runtime } => emit_rt_close_call(out, frame, dst, runtime),
        BytecodeExpr::RtCancelled => emit_rt_cancelled_call(out, dst),
        BytecodeExpr::ChanOpenU32 { capacity } => {
            emit_chan_open_u32_call(out, frame, dst, capacity)
        }
        BytecodeExpr::ChanSendU32 { channel, value } => {
            emit_chan_send_u32_call(out, frame, dst, channel, value)
        }
        BytecodeExpr::ChanRecvU32 { channel } => emit_chan_recv_u32_call(out, frame, dst, channel),
        BytecodeExpr::ChanClose { channel } => emit_chan_close_call(out, frame, dst, channel),
        BytecodeExpr::FsReadU32 { path } => emit_fs_read_call(out, dst, strings, path),
        BytecodeExpr::FsWriteU32 { path, value } => {
            emit_fs_write_call(out, frame, dst, strings, path, value)
        }
        BytecodeExpr::BufLit { literal } => emit_buf_lit_call(out, dst, strings, literal),
        BytecodeExpr::TlsServerConfigU32 { value } => {
            emit_load_immediate(out, "x0", *value as i64, false)?;
            emit_store_scalar_reg(out, dst, "x0")
        }
        BytecodeExpr::TlsServerConfigBuf { value } => emit_buf_lit_call(out, dst, strings, value),
        BytecodeExpr::TlsListen {
            host,
            port,
            cert,
            key,
            request_timeout_ms,
            session_timeout_ms,
            shutdown_grace_ms,
        } => emit_tls_listen_call(
            out,
            dst,
            strings,
            host,
            *port,
            cert,
            key,
            *request_timeout_ms,
            *session_timeout_ms,
            *shutdown_grace_ms,
        ),
        BytecodeExpr::HttpSessionAccept { listener } => {
            emit_http_session_accept_call(out, frame, dst, listener)
        }
        BytecodeExpr::ListenerSetTimeoutMs { handle, value } => {
            emit_listener_set_timeout_call(out, frame, dst, handle, value)
        }
        BytecodeExpr::SessionSetTimeoutMs { handle, value } => {
            emit_session_set_timeout_call(out, frame, dst, handle, value)
        }
        BytecodeExpr::ListenerSetShutdownGraceMs { handle, value } => {
            emit_listener_set_shutdown_grace_call(out, frame, dst, handle, value)
        }
        BytecodeExpr::HttpSessionRequest { handle } => {
            emit_http_session_request_call(out, frame, dst, handle)
        }
        BytecodeExpr::NetClose { handle } => emit_net_close_call(out, frame, dst, handle),
        BytecodeExpr::HttpSessionClose { handle } => {
            emit_http_session_close_call(out, frame, dst, handle)
        }
        BytecodeExpr::ServiceOpen { name } => emit_service_open_call(out, dst, strings, name),
        BytecodeExpr::ServiceClose { handle } => emit_service_close_call(out, frame, dst, handle),
        BytecodeExpr::ServiceShutdown { handle, grace_ms } => {
            emit_service_shutdown_call(out, frame, dst, handle, grace_ms)
        }
        BytecodeExpr::ServiceLog { handle, message } => {
            emit_service_log_call(out, frame, dst, strings, handle, message)
        }
        BytecodeExpr::ServiceTraceBegin { handle, name } => {
            emit_service_trace_begin_call(out, frame, dst, strings, handle, name)
        }
        BytecodeExpr::ServiceTraceEnd { trace } => {
            emit_service_trace_end_call(out, frame, dst, trace)
        }
        BytecodeExpr::ServiceMetricCount { handle, value } => {
            emit_service_metric_count_call(out, frame, dst, strings, handle, value)
        }
        BytecodeExpr::ServiceHealthStatus { handle } => {
            emit_service_health_status_call(out, frame, dst, handle)
        }
        BytecodeExpr::ServiceReadinessStatus { handle } => {
            emit_service_readiness_status_call(out, frame, dst, handle)
        }
        BytecodeExpr::ServiceMigrateDb { handle, db_handle } => {
            emit_service_migrate_db_call(out, frame, dst, strings, handle, db_handle)
        }
        BytecodeExpr::ServiceRoute {
            request,
            method,
            path,
        } => emit_service_route_call(out, frame, dst, strings, request, method, path),
        BytecodeExpr::ServiceRequireHeader {
            request,
            name,
            value,
        } => emit_service_require_header_call(out, frame, dst, strings, request, name, value),
        BytecodeExpr::ServiceErrorStatus { kind } => {
            emit_service_error_status_call(out, dst, strings, kind)
        }
        BytecodeExpr::HttpBody { request } => emit_http_body_call(out, frame, dst, request),
        BytecodeExpr::JsonGetU32 { value, key } => {
            emit_json_get_u32_call(out, frame, dst, strings, value, key)
        }
        BytecodeExpr::DbOpen { path } => emit_db_open_call(out, dst, strings, path),
        BytecodeExpr::DbClose { handle } => emit_db_close_call(out, frame, dst, handle),
        BytecodeExpr::DbQueryU32 { handle, sql } => {
            emit_db_query_u32_call(out, frame, dst, handle, sql)
        }
        BytecodeExpr::HttpSessionWriteText {
            handle,
            status,
            body,
        } => emit_http_session_write_text_call(out, frame, dst, handle, status, body),
        BytecodeExpr::TaskSleepMs { value } => emit_task_sleep_call(out, frame, dst, value),
        BytecodeExpr::SpawnCall { command, .. } => emit_spawn_call(out, dst, strings, command),
        BytecodeExpr::NetConnect { host, port } => emit_net_call(out, dst, strings, host, *port),
        BytecodeExpr::FfiCall {
            symbol,
            args,
            ret_kind: _,
        } => emit_ffi_call(out, frame, dst, symbol, args),
        BytecodeExpr::FfiCallCStr { .. } => {
            Err("arm64 backend does not yet support ffi_call_cstr".to_string())
        }
        BytecodeExpr::FfiOpenLib { .. }
        | BytecodeExpr::FfiCloseLib { .. }
        | BytecodeExpr::FfiBufPtr { .. }
        | BytecodeExpr::FfiCallLib { .. }
        | BytecodeExpr::FfiCallLibCStr { .. } => {
            Err("arm64 backend does not yet support dynamic library ffi ops".to_string())
        }
        BytecodeExpr::LenSpanI32 { source } => {
            let src = frame.slot(*source)?;
            emit_load_span_len(out, src, "x9")?;
            emit_store_scalar_reg(out, dst, "x9")
        }
        BytecodeExpr::LoadSpanI32 { source, index } => {
            let src = frame.slot(*source)?;
            emit_load_span_ptr(out, src, "x10")?;
            emit_load_scalar_operand(out, frame, index, "x11")?;
            out.push_str("  ldr w9, [x10, x11, lsl #2]\n");
            emit_store_scalar_reg(out, dst, "x9")
        }
        BytecodeExpr::AbsI32 { value } => {
            emit_load_scalar_operand(out, frame, value, "x9")?;
            out.push_str("  cmp w9, #0\n");
            out.push_str("  cneg w9, w9, lt\n");
            emit_store_scalar_reg(out, dst, "x9")
        }
        BytecodeExpr::Binary { op, left, right } => {
            emit_binary_expr(out, frame, dst, op, left, right)
        }
        BytecodeExpr::SextI64 { value } => {
            emit_load_scalar_operand(out, frame, value, "x9")?;
            out.push_str("  sxtw x9, w9\n");
            emit_store_scalar_reg(out, dst, "x9")
        }
        _ => Err("arm64 backend does not yet support this bytecode expr".to_string()),
    };
    result.map_err(|error| format!("{error} while lowering {expr:?}"))
}

fn emit_move_expr(
    out: &mut String,
    frame: &FrameLayout,
    dst: SlotLayout,
    operand: &BytecodeOperand,
) -> Result<(), String> {
    if let Ok(src) = operand_slot_layout(frame, operand) {
        if same_home(src, dst) {
            return Ok(());
        }
    }
    match dst.kind {
        BytecodeValueKind::SpanI32 | BytecodeValueKind::BufU8 => {
            let src = operand_slot_layout(frame, operand)?;
            emit_copy_value(out, src, dst)
        }
        _ => {
            emit_load_scalar_operand(out, frame, operand, "x9")?;
            emit_store_scalar_reg(out, dst, "x9")
        }
    }
}

fn emit_binary_expr(
    out: &mut String,
    frame: &FrameLayout,
    dst: SlotLayout,
    op: &LoweredExecBinaryOp,
    left: &BytecodeOperand,
    right: &BytecodeOperand,
) -> Result<(), String> {
    match op {
        LoweredExecBinaryOp::Add
        | LoweredExecBinaryOp::Sub
        | LoweredExecBinaryOp::Mul
        | LoweredExecBinaryOp::Band
        | LoweredExecBinaryOp::Bor
        | LoweredExecBinaryOp::Bxor => {
            emit_load_scalar_operand(out, frame, left, "x9")?;
            emit_load_scalar_operand(out, frame, right, "x10")?;
            let mnemonic = match op {
                LoweredExecBinaryOp::Add => "add",
                LoweredExecBinaryOp::Sub => "sub",
                LoweredExecBinaryOp::Mul => "mul",
                LoweredExecBinaryOp::Band => "and",
                LoweredExecBinaryOp::Bor => "orr",
                LoweredExecBinaryOp::Bxor => "eor",
                _ => unreachable!(),
            };
            match dst.kind {
                BytecodeValueKind::I32
                | BytecodeValueKind::U32
                | BytecodeValueKind::U8
                | BytecodeValueKind::Bool => {
                    out.push_str(&format!("  {mnemonic} w9, w9, w10\n"));
                }
                BytecodeValueKind::I64 | BytecodeValueKind::U64 => {
                    out.push_str(&format!("  {mnemonic} x9, x9, x10\n"));
                }
                BytecodeValueKind::SpanI32 | BytecodeValueKind::BufU8 => {
                    return Err(
                        "arm64 backend does not support binary ops on aggregate values".to_string(),
                    )
                }
            }
            emit_store_scalar_reg(out, dst, "x9")
        }
        LoweredExecBinaryOp::Shl | LoweredExecBinaryOp::Shr => {
            let kind = operand_kind(left);
            emit_load_scalar_operand(out, frame, left, "x9")?;
            emit_load_scalar_operand(out, frame, right, "x10")?;
            let (mnemonic, width) = match op {
                LoweredExecBinaryOp::Shl => match kind {
                    BytecodeValueKind::I64 | BytecodeValueKind::U64 => ("lsl", "x"),
                    BytecodeValueKind::I32
                    | BytecodeValueKind::U32
                    | BytecodeValueKind::U8
                    | BytecodeValueKind::Bool => ("lsl", "w"),
                    BytecodeValueKind::SpanI32 | BytecodeValueKind::BufU8 => {
                        return Err("arm64 backend does not support aggregate shifts".to_string())
                    }
                },
                LoweredExecBinaryOp::Shr => match kind {
                    BytecodeValueKind::I64 => ("asr", "x"),
                    BytecodeValueKind::U64 => ("lsr", "x"),
                    BytecodeValueKind::I32 => ("asr", "w"),
                    BytecodeValueKind::U32 | BytecodeValueKind::U8 | BytecodeValueKind::Bool => {
                        ("lsr", "w")
                    }
                    BytecodeValueKind::SpanI32 | BytecodeValueKind::BufU8 => {
                        return Err("arm64 backend does not support aggregate shifts".to_string())
                    }
                },
                _ => unreachable!(),
            };
            out.push_str(&format!("  {mnemonic} {width}9, {width}9, {width}10\n"));
            emit_store_scalar_reg(out, dst, "x9")
        }
        LoweredExecBinaryOp::Eq | LoweredExecBinaryOp::Lt | LoweredExecBinaryOp::Le => {
            let kind = operand_kind(left);
            emit_load_scalar_operand(out, frame, left, "x9")?;
            emit_load_scalar_operand(out, frame, right, "x10")?;
            let compare_width = match kind {
                BytecodeValueKind::I64 | BytecodeValueKind::U64 => "x",
                BytecodeValueKind::I32
                | BytecodeValueKind::U32
                | BytecodeValueKind::U8
                | BytecodeValueKind::Bool => "w",
                BytecodeValueKind::SpanI32 | BytecodeValueKind::BufU8 => {
                    return Err("arm64 backend does not support aggregate comparison".to_string())
                }
            };
            out.push_str(&format!("  cmp {compare_width}9, {compare_width}10\n"));
            let condition = match op {
                LoweredExecBinaryOp::Eq => "eq",
                LoweredExecBinaryOp::Lt => match kind {
                    BytecodeValueKind::U64
                    | BytecodeValueKind::U32
                    | BytecodeValueKind::U8
                    | BytecodeValueKind::Bool => "lo",
                    _ => "lt",
                },
                LoweredExecBinaryOp::Le => match kind {
                    BytecodeValueKind::U64 | BytecodeValueKind::U32 | BytecodeValueKind::Bool => {
                        "ls"
                    }
                    _ => "le",
                },
                _ => unreachable!(),
            };
            out.push_str(&format!("  cset w9, {condition}\n"));
            emit_store_scalar_reg(out, dst, "x9")
        }
    }
}

fn emit_clock_call(out: &mut String, dst: SlotLayout) -> Result<(), String> {
    out.push_str(&format!(
        "  bl {}\n",
        runtime_symbol("mira_rt_clock_now_ns")
    ));
    emit_store_scalar_reg(out, dst, "x0")
}

fn emit_rand_call(out: &mut String, frame: &FrameLayout, dst: SlotLayout) -> Result<(), String> {
    let offset = frame
        .rand_state_offset
        .ok_or_else(|| "arm64 rand call missing rand state slot".to_string())?;
    out.push_str(&format!("  add x0, sp, #{}\n", offset));
    out.push_str(&format!(
        "  bl {}\n",
        runtime_symbol("mira_rt_rand_next_u32")
    ));
    emit_store_scalar_reg(out, dst, "x0")
}

fn emit_rt_open_call(
    out: &mut String,
    frame: &FrameLayout,
    dst: SlotLayout,
    workers: &BytecodeOperand,
) -> Result<(), String> {
    emit_load_scalar_operand(out, frame, workers, "x0")?;
    out.push_str(&format!("  bl {}\n", runtime_symbol("mira_rt_open_handle")));
    emit_store_scalar_reg(out, dst, "x0")
}

fn emit_rt_spawn_u32_call(
    out: &mut String,
    frame: &FrameLayout,
    dst: SlotLayout,
    strings: &RuntimeStrings,
    runtime: &BytecodeOperand,
    function: &str,
    arg: &BytecodeOperand,
) -> Result<(), String> {
    emit_load_scalar_operand(out, frame, runtime, "x0")?;
    emit_load_runtime_string(out, "x1", strings, function)?;
    emit_load_scalar_operand(out, frame, arg, "x2")?;
    out.push_str(&format!(
        "  bl {}\n",
        runtime_symbol("mira_rt_spawn_u32_handle")
    ));
    emit_store_scalar_reg(out, dst, "x0")
}

fn emit_rt_done_call(
    out: &mut String,
    frame: &FrameLayout,
    dst: SlotLayout,
    task: &BytecodeOperand,
) -> Result<(), String> {
    emit_load_scalar_operand(out, frame, task, "x0")?;
    out.push_str(&format!("  bl {}\n", runtime_symbol("mira_rt_done_handle")));
    emit_store_scalar_reg(out, dst, "x0")
}

fn emit_rt_join_u32_call(
    out: &mut String,
    frame: &FrameLayout,
    dst: SlotLayout,
    task: &BytecodeOperand,
) -> Result<(), String> {
    emit_load_scalar_operand(out, frame, task, "x0")?;
    out.push_str(&format!(
        "  bl {}\n",
        runtime_symbol("mira_rt_join_u32_handle")
    ));
    emit_store_scalar_reg(out, dst, "x0")
}

fn emit_rt_cancel_call(
    out: &mut String,
    frame: &FrameLayout,
    dst: SlotLayout,
    task: &BytecodeOperand,
) -> Result<(), String> {
    emit_load_scalar_operand(out, frame, task, "x0")?;
    out.push_str(&format!(
        "  bl {}\n",
        runtime_symbol("mira_rt_cancel_handle")
    ));
    emit_store_scalar_reg(out, dst, "x0")
}

fn emit_rt_task_close_call(
    out: &mut String,
    frame: &FrameLayout,
    dst: SlotLayout,
    task: &BytecodeOperand,
) -> Result<(), String> {
    emit_load_scalar_operand(out, frame, task, "x0")?;
    out.push_str(&format!(
        "  bl {}\n",
        runtime_symbol("mira_rt_task_close_handle")
    ));
    emit_store_scalar_reg(out, dst, "x0")
}

fn emit_rt_shutdown_call(
    out: &mut String,
    frame: &FrameLayout,
    dst: SlotLayout,
    runtime: &BytecodeOperand,
    grace_ms: &BytecodeOperand,
) -> Result<(), String> {
    emit_load_scalar_operand(out, frame, runtime, "x0")?;
    emit_load_scalar_operand(out, frame, grace_ms, "x1")?;
    out.push_str(&format!(
        "  bl {}\n",
        runtime_symbol("mira_rt_shutdown_handle")
    ));
    emit_store_scalar_reg(out, dst, "x0")
}

fn emit_rt_close_call(
    out: &mut String,
    frame: &FrameLayout,
    dst: SlotLayout,
    runtime: &BytecodeOperand,
) -> Result<(), String> {
    emit_load_scalar_operand(out, frame, runtime, "x0")?;
    out.push_str(&format!(
        "  bl {}\n",
        runtime_symbol("mira_rt_close_handle")
    ));
    emit_store_scalar_reg(out, dst, "x0")
}

fn emit_rt_cancelled_call(out: &mut String, dst: SlotLayout) -> Result<(), String> {
    out.push_str(&format!("  bl {}\n", runtime_symbol("mira_rt_cancelled")));
    emit_store_scalar_reg(out, dst, "x0")
}

fn emit_chan_open_u32_call(
    out: &mut String,
    frame: &FrameLayout,
    dst: SlotLayout,
    capacity: &BytecodeOperand,
) -> Result<(), String> {
    emit_load_scalar_operand(out, frame, capacity, "x0")?;
    out.push_str(&format!(
        "  bl {}\n",
        runtime_symbol("mira_chan_open_u32_handle")
    ));
    emit_store_scalar_reg(out, dst, "x0")
}

fn emit_chan_send_u32_call(
    out: &mut String,
    frame: &FrameLayout,
    dst: SlotLayout,
    channel: &BytecodeOperand,
    value: &BytecodeOperand,
) -> Result<(), String> {
    emit_load_scalar_operand(out, frame, channel, "x0")?;
    emit_load_scalar_operand(out, frame, value, "x1")?;
    out.push_str(&format!(
        "  bl {}\n",
        runtime_symbol("mira_chan_send_u32_handle")
    ));
    emit_store_scalar_reg(out, dst, "x0")
}

fn emit_chan_recv_u32_call(
    out: &mut String,
    frame: &FrameLayout,
    dst: SlotLayout,
    channel: &BytecodeOperand,
) -> Result<(), String> {
    emit_load_scalar_operand(out, frame, channel, "x0")?;
    out.push_str(&format!(
        "  bl {}\n",
        runtime_symbol("mira_chan_recv_u32_handle")
    ));
    emit_store_scalar_reg(out, dst, "x0")
}

fn emit_chan_close_call(
    out: &mut String,
    frame: &FrameLayout,
    dst: SlotLayout,
    channel: &BytecodeOperand,
) -> Result<(), String> {
    emit_load_scalar_operand(out, frame, channel, "x0")?;
    out.push_str(&format!(
        "  bl {}\n",
        runtime_symbol("mira_chan_close_handle")
    ));
    emit_store_scalar_reg(out, dst, "x0")
}

fn emit_task_sleep_call(
    out: &mut String,
    frame: &FrameLayout,
    dst: SlotLayout,
    value: &BytecodeOperand,
) -> Result<(), String> {
    emit_load_scalar_operand(out, frame, value, "x0")?;
    emit_load_immediate(out, "x1", 1000, false)?;
    out.push_str("  mul x0, x0, x1\n");
    out.push_str(&format!("  bl {}\n", extern_symbol("usleep")));
    out.push_str("  cmp w0, #0\n");
    out.push_str("  cset w0, eq\n");
    emit_store_scalar_reg(out, dst, "x0")
}

fn emit_fs_read_call(
    out: &mut String,
    dst: SlotLayout,
    strings: &RuntimeStrings,
    path: &str,
) -> Result<(), String> {
    emit_load_runtime_string(out, "x0", strings, path)?;
    out.push_str(&format!("  bl {}\n", runtime_symbol("mira_rt_fs_read_u32")));
    emit_store_scalar_reg(out, dst, "x0")
}

fn emit_fs_write_call(
    out: &mut String,
    frame: &FrameLayout,
    dst: SlotLayout,
    strings: &RuntimeStrings,
    path: &str,
    value: &BytecodeOperand,
) -> Result<(), String> {
    emit_load_runtime_string(out, "x0", strings, path)?;
    emit_load_scalar_operand(out, frame, value, "x1")?;
    out.push_str(&format!(
        "  bl {}\n",
        runtime_symbol("mira_rt_fs_write_u32")
    ));
    emit_store_scalar_reg(out, dst, "x0")
}

fn emit_load_buf_operand(
    out: &mut String,
    frame: &FrameLayout,
    operand: &BytecodeOperand,
    ptr_reg: &str,
    len_reg: &str,
) -> Result<(), String> {
    let src = operand_slot_layout(frame, operand)?;
    emit_load_span_ptr(out, src, ptr_reg)?;
    emit_load_span_len(out, src, len_reg)
}

fn emit_buf_lit_call(
    out: &mut String,
    dst: SlotLayout,
    strings: &RuntimeStrings,
    literal: &str,
) -> Result<(), String> {
    emit_load_runtime_string(out, "x0", strings, literal)?;
    out.push_str(&format!("  bl {}\n", runtime_symbol("mira_buf_lit_u8")));
    emit_store_span_regs(out, dst, "x0", "x1")
}

fn emit_drop_buf_call(
    out: &mut String,
    frame: &FrameLayout,
    dst: SlotLayout,
    value: &BytecodeOperand,
) -> Result<(), String> {
    emit_load_buf_operand(out, frame, value, "x0", "x1")?;
    out.push_str(&format!("  bl {}\n", runtime_symbol("mira_drop_buf_u8")));
    emit_store_scalar_reg(out, dst, "x0")
}

fn emit_tls_listen_call(
    out: &mut String,
    dst: SlotLayout,
    strings: &RuntimeStrings,
    host: &str,
    port: u16,
    cert: &str,
    key: &str,
    request_timeout_ms: u32,
    session_timeout_ms: u32,
    shutdown_grace_ms: u32,
) -> Result<(), String> {
    emit_load_runtime_string(out, "x0", strings, host)?;
    emit_load_immediate(out, "x1", port as i64, false)?;
    emit_load_runtime_string(out, "x2", strings, cert)?;
    emit_load_runtime_string(out, "x3", strings, key)?;
    emit_load_immediate(out, "x4", request_timeout_ms as i64, false)?;
    emit_load_immediate(out, "x5", session_timeout_ms as i64, false)?;
    emit_load_immediate(out, "x6", shutdown_grace_ms as i64, false)?;
    out.push_str(&format!("  bl {}\n", runtime_symbol("mira_tls_listen_handle")));
    emit_store_scalar_reg(out, dst, "x0")
}

fn emit_http_session_accept_call(
    out: &mut String,
    frame: &FrameLayout,
    dst: SlotLayout,
    listener: &BytecodeOperand,
) -> Result<(), String> {
    emit_load_scalar_operand(out, frame, listener, "x0")?;
    out.push_str(&format!(
        "  bl {}\n",
        runtime_symbol("mira_http_session_accept_handle")
    ));
    emit_store_scalar_reg(out, dst, "x0")
}

fn emit_listener_set_timeout_call(
    out: &mut String,
    frame: &FrameLayout,
    dst: SlotLayout,
    handle: &BytecodeOperand,
    value: &BytecodeOperand,
) -> Result<(), String> {
    emit_load_scalar_operand(out, frame, handle, "x0")?;
    emit_load_scalar_operand(out, frame, value, "x1")?;
    out.push_str(&format!(
        "  bl {}\n",
        runtime_symbol("mira_listener_set_timeout_ms")
    ));
    emit_store_scalar_reg(out, dst, "x0")
}

fn emit_session_set_timeout_call(
    out: &mut String,
    frame: &FrameLayout,
    dst: SlotLayout,
    handle: &BytecodeOperand,
    value: &BytecodeOperand,
) -> Result<(), String> {
    emit_load_scalar_operand(out, frame, handle, "x0")?;
    emit_load_scalar_operand(out, frame, value, "x1")?;
    out.push_str(&format!(
        "  bl {}\n",
        runtime_symbol("mira_session_set_timeout_ms")
    ));
    emit_store_scalar_reg(out, dst, "x0")
}

fn emit_listener_set_shutdown_grace_call(
    out: &mut String,
    frame: &FrameLayout,
    dst: SlotLayout,
    handle: &BytecodeOperand,
    value: &BytecodeOperand,
) -> Result<(), String> {
    emit_load_scalar_operand(out, frame, handle, "x0")?;
    emit_load_scalar_operand(out, frame, value, "x1")?;
    out.push_str(&format!(
        "  bl {}\n",
        runtime_symbol("mira_listener_set_shutdown_grace_ms")
    ));
    emit_store_scalar_reg(out, dst, "x0")
}

fn emit_http_session_request_call(
    out: &mut String,
    frame: &FrameLayout,
    dst: SlotLayout,
    handle: &BytecodeOperand,
) -> Result<(), String> {
    emit_load_scalar_operand(out, frame, handle, "x0")?;
    out.push_str(&format!(
        "  bl {}\n",
        runtime_symbol("mira_http_session_request_buf_u8")
    ));
    emit_store_span_regs(out, dst, "x0", "x1")
}

fn emit_http_session_write_text_call(
    out: &mut String,
    frame: &FrameLayout,
    dst: SlotLayout,
    handle: &BytecodeOperand,
    status: &BytecodeOperand,
    body: &BytecodeOperand,
) -> Result<(), String> {
    emit_load_scalar_operand(out, frame, handle, "x0")?;
    emit_load_scalar_operand(out, frame, status, "x1")?;
    emit_load_buf_operand(out, frame, body, "x2", "x3")?;
    out.push_str(&format!(
        "  bl {}\n",
        runtime_symbol("mira_http_session_write_text_handle")
    ));
    emit_store_scalar_reg(out, dst, "x0")
}

fn emit_net_close_call(
    out: &mut String,
    frame: &FrameLayout,
    dst: SlotLayout,
    handle: &BytecodeOperand,
) -> Result<(), String> {
    emit_load_scalar_operand(out, frame, handle, "x0")?;
    out.push_str(&format!("  bl {}\n", runtime_symbol("mira_net_close_handle")));
    emit_store_scalar_reg(out, dst, "x0")
}

fn emit_http_session_close_call(
    out: &mut String,
    frame: &FrameLayout,
    dst: SlotLayout,
    handle: &BytecodeOperand,
) -> Result<(), String> {
    emit_load_scalar_operand(out, frame, handle, "x0")?;
    out.push_str(&format!(
        "  bl {}\n",
        runtime_symbol("mira_http_session_close_handle")
    ));
    emit_store_scalar_reg(out, dst, "x0")
}

fn emit_http_body_call(
    out: &mut String,
    frame: &FrameLayout,
    dst: SlotLayout,
    request: &BytecodeOperand,
) -> Result<(), String> {
    emit_load_buf_operand(out, frame, request, "x0", "x1")?;
    out.push_str(&format!("  bl {}\n", runtime_symbol("mira_http_body_buf_u8")));
    emit_store_span_regs(out, dst, "x0", "x1")
}

fn emit_json_get_u32_call(
    out: &mut String,
    frame: &FrameLayout,
    dst: SlotLayout,
    strings: &RuntimeStrings,
    value: &BytecodeOperand,
    key: &str,
) -> Result<(), String> {
    emit_load_buf_operand(out, frame, value, "x0", "x1")?;
    emit_load_runtime_string(out, "x2", strings, key)?;
    out.push_str(&format!(
        "  bl {}\n",
        runtime_symbol("mira_json_get_u32_buf_u8")
    ));
    emit_store_scalar_reg(out, dst, "x0")
}

fn emit_db_open_call(
    out: &mut String,
    dst: SlotLayout,
    strings: &RuntimeStrings,
    path: &str,
) -> Result<(), String> {
    emit_load_runtime_string(out, "x0", strings, path)?;
    out.push_str(&format!("  bl {}\n", runtime_symbol("mira_db_open_handle")));
    emit_store_scalar_reg(out, dst, "x0")
}

fn emit_db_close_call(
    out: &mut String,
    frame: &FrameLayout,
    dst: SlotLayout,
    handle: &BytecodeOperand,
) -> Result<(), String> {
    emit_load_scalar_operand(out, frame, handle, "x0")?;
    out.push_str(&format!("  bl {}\n", runtime_symbol("mira_db_close_handle")));
    emit_store_scalar_reg(out, dst, "x0")
}

fn emit_db_query_u32_call(
    out: &mut String,
    frame: &FrameLayout,
    dst: SlotLayout,
    handle: &BytecodeOperand,
    sql: &BytecodeOperand,
) -> Result<(), String> {
    emit_load_scalar_operand(out, frame, handle, "x0")?;
    emit_load_buf_operand(out, frame, sql, "x1", "x2")?;
    out.push_str(&format!(
        "  bl {}\n",
        runtime_symbol("mira_db_query_u32_handle_sql_buf_u8")
    ));
    emit_store_scalar_reg(out, dst, "x0")
}

fn emit_service_open_call(
    out: &mut String,
    dst: SlotLayout,
    strings: &RuntimeStrings,
    name: &str,
) -> Result<(), String> {
    emit_load_runtime_string(out, "x0", strings, name)?;
    out.push_str(&format!(
        "  bl {}\n",
        runtime_symbol("mira_service_open_handle")
    ));
    emit_store_scalar_reg(out, dst, "x0")
}

fn emit_service_close_call(
    out: &mut String,
    frame: &FrameLayout,
    dst: SlotLayout,
    handle: &BytecodeOperand,
) -> Result<(), String> {
    emit_load_scalar_operand(out, frame, handle, "x0")?;
    out.push_str(&format!(
        "  bl {}\n",
        runtime_symbol("mira_service_close_handle")
    ));
    emit_store_scalar_reg(out, dst, "x0")
}

fn emit_service_shutdown_call(
    out: &mut String,
    frame: &FrameLayout,
    dst: SlotLayout,
    handle: &BytecodeOperand,
    grace_ms: &BytecodeOperand,
) -> Result<(), String> {
    emit_load_scalar_operand(out, frame, handle, "x0")?;
    emit_load_scalar_operand(out, frame, grace_ms, "x1")?;
    out.push_str(&format!(
        "  bl {}\n",
        runtime_symbol("mira_service_shutdown_handle")
    ));
    emit_store_scalar_reg(out, dst, "x0")
}

fn emit_service_log_call(
    out: &mut String,
    frame: &FrameLayout,
    dst: SlotLayout,
    strings: &RuntimeStrings,
    handle: &BytecodeOperand,
    message: &BytecodeOperand,
) -> Result<(), String> {
    emit_load_scalar_operand(out, frame, handle, "x0")?;
    emit_load_runtime_string(out, "x1", strings, "info")?;
    emit_load_buf_operand(out, frame, message, "x2", "x3")?;
    out.push_str(&format!(
        "  bl {}\n",
        runtime_symbol("mira_service_log_buf_u8")
    ));
    emit_store_scalar_reg(out, dst, "x0")
}

fn emit_service_trace_begin_call(
    out: &mut String,
    frame: &FrameLayout,
    dst: SlotLayout,
    strings: &RuntimeStrings,
    handle: &BytecodeOperand,
    name: &str,
) -> Result<(), String> {
    emit_load_scalar_operand(out, frame, handle, "x0")?;
    emit_load_runtime_string(out, "x1", strings, name)?;
    out.push_str(&format!(
        "  bl {}\n",
        runtime_symbol("mira_service_trace_begin_handle")
    ));
    emit_store_scalar_reg(out, dst, "x0")
}

fn emit_service_trace_end_call(
    out: &mut String,
    frame: &FrameLayout,
    dst: SlotLayout,
    trace: &BytecodeOperand,
) -> Result<(), String> {
    emit_load_scalar_operand(out, frame, trace, "x0")?;
    out.push_str(&format!(
        "  bl {}\n",
        runtime_symbol("mira_service_trace_end_handle")
    ));
    emit_store_scalar_reg(out, dst, "x0")
}

fn emit_service_metric_count_call(
    out: &mut String,
    frame: &FrameLayout,
    dst: SlotLayout,
    strings: &RuntimeStrings,
    handle: &BytecodeOperand,
    value: &BytecodeOperand,
) -> Result<(), String> {
    emit_load_scalar_operand(out, frame, handle, "x0")?;
    emit_load_runtime_string(out, "x1", strings, "count")?;
    emit_load_scalar_operand(out, frame, value, "x2")?;
    out.push_str(&format!(
        "  bl {}\n",
        runtime_symbol("mira_service_metric_count_handle")
    ));
    emit_store_scalar_reg(out, dst, "x0")
}

fn emit_service_health_status_call(
    out: &mut String,
    frame: &FrameLayout,
    dst: SlotLayout,
    handle: &BytecodeOperand,
) -> Result<(), String> {
    emit_load_scalar_operand(out, frame, handle, "x0")?;
    out.push_str(&format!(
        "  bl {}\n",
        runtime_symbol("mira_service_health_status_handle")
    ));
    emit_store_scalar_reg(out, dst, "x0")
}

fn emit_service_readiness_status_call(
    out: &mut String,
    frame: &FrameLayout,
    dst: SlotLayout,
    handle: &BytecodeOperand,
) -> Result<(), String> {
    emit_load_scalar_operand(out, frame, handle, "x0")?;
    out.push_str(&format!(
        "  bl {}\n",
        runtime_symbol("mira_service_readiness_status_handle")
    ));
    emit_store_scalar_reg(out, dst, "x0")
}

fn emit_service_migrate_db_call(
    out: &mut String,
    frame: &FrameLayout,
    dst: SlotLayout,
    strings: &RuntimeStrings,
    handle: &BytecodeOperand,
    db_handle: &BytecodeOperand,
) -> Result<(), String> {
    emit_load_scalar_operand(out, frame, handle, "x0")?;
    emit_load_scalar_operand(out, frame, db_handle, "x1")?;
    emit_load_runtime_string(out, "x2", strings, "migration")?;
    out.push_str(&format!(
        "  bl {}\n",
        runtime_symbol("mira_service_migrate_db_handle")
    ));
    emit_store_scalar_reg(out, dst, "x0")
}

fn emit_service_route_call(
    out: &mut String,
    frame: &FrameLayout,
    dst: SlotLayout,
    strings: &RuntimeStrings,
    request: &BytecodeOperand,
    method: &str,
    path: &str,
) -> Result<(), String> {
    emit_load_buf_operand(out, frame, request, "x0", "x1")?;
    emit_load_runtime_string(out, "x2", strings, method)?;
    emit_load_runtime_string(out, "x3", strings, path)?;
    out.push_str(&format!(
        "  bl {}\n",
        runtime_symbol("mira_service_route_buf_u8")
    ));
    emit_store_scalar_reg(out, dst, "x0")
}

fn emit_service_require_header_call(
    out: &mut String,
    frame: &FrameLayout,
    dst: SlotLayout,
    strings: &RuntimeStrings,
    request: &BytecodeOperand,
    name: &str,
    value: &str,
) -> Result<(), String> {
    emit_load_buf_operand(out, frame, request, "x0", "x1")?;
    emit_load_runtime_string(out, "x2", strings, name)?;
    emit_load_runtime_string(out, "x3", strings, value)?;
    out.push_str(&format!(
        "  bl {}\n",
        runtime_symbol("mira_service_require_header_buf_u8")
    ));
    emit_store_scalar_reg(out, dst, "x0")
}

fn emit_service_error_status_call(
    out: &mut String,
    dst: SlotLayout,
    strings: &RuntimeStrings,
    kind: &str,
) -> Result<(), String> {
    emit_load_runtime_string(out, "x0", strings, kind)?;
    out.push_str(&format!(
        "  bl {}\n",
        runtime_symbol("mira_service_error_status")
    ));
    emit_store_scalar_reg(out, dst, "x0")
}

fn emit_spawn_call(
    out: &mut String,
    dst: SlotLayout,
    strings: &RuntimeStrings,
    command: &str,
) -> Result<(), String> {
    emit_load_runtime_string(out, "x0", strings, command)?;
    out.push_str(&format!(
        "  bl {}\n",
        runtime_symbol("mira_rt_spawn_status")
    ));
    emit_store_scalar_reg(out, dst, "x0")
}

fn emit_net_call(
    out: &mut String,
    dst: SlotLayout,
    strings: &RuntimeStrings,
    host: &str,
    port: u16,
) -> Result<(), String> {
    emit_load_runtime_string(out, "x0", strings, host)?;
    emit_load_immediate(out, "x1", port as i64, false)?;
    out.push_str(&format!(
        "  bl {}\n",
        runtime_symbol("mira_rt_net_connect_ok")
    ));
    emit_store_scalar_reg(out, dst, "x0")
}

fn emit_ffi_call(
    out: &mut String,
    frame: &FrameLayout,
    dst: SlotLayout,
    symbol: &str,
    args: &[BytecodeOperand],
) -> Result<(), String> {
    if args.len() > 8 {
        return Err(format!(
            "arm64 backend supports at most 8 ffi args, got {}",
            args.len()
        ));
    }
    for (index, operand) in args.iter().enumerate() {
        emit_load_scalar_operand(out, frame, operand, reg_x(index)?)?;
    }
    out.push_str(&format!("  bl {}\n", extern_symbol(symbol)));
    emit_store_scalar_reg(out, dst, "x0")
}

fn emit_load_runtime_string(
    out: &mut String,
    reg: &str,
    strings: &RuntimeStrings,
    value: &str,
) -> Result<(), String> {
    let label = strings
        .labels
        .get(value)
        .ok_or_else(|| format!("missing arm64 runtime string label for {value}"))?;
    out.push_str(&format!("  adrp {reg}, {label}@PAGE\n"));
    out.push_str(&format!("  add {reg}, {reg}, {label}@PAGEOFF\n"));
    Ok(())
}

fn emit_terminator(
    out: &mut String,
    function: &BytecodeFunction,
    block_index: usize,
    frame: &FrameLayout,
    strings: &RuntimeStrings,
    terminator: &BytecodeTerminator,
) -> Result<(), String> {
    match terminator {
        BytecodeTerminator::Return(value) => {
            emit_load_return_value(out, frame, value, function.return_kind)?;
            emit_function_epilogue(out, frame);
            Ok(())
        }
        BytecodeTerminator::Jump(edge) => emit_edge_jump(out, function, frame, strings, edge),
        BytecodeTerminator::Branch {
            condition,
            truthy,
            falsy,
        } => {
            let false_label = format!(".L_{}_{}_false", function.name, block_index);
            emit_load_scalar_operand(out, frame, condition, "x9")?;
            out.push_str(&format!("  cbz x9, {false_label}\n"));
            emit_edge_moves(out, frame, strings, truthy)?;
            out.push_str(&format!("  b {}\n", block_label(function, truthy.target)));
            out.push_str(&format!("{false_label}:\n"));
            emit_edge_moves(out, frame, strings, falsy)?;
            out.push_str(&format!("  b {}\n", block_label(function, falsy.target)));
            Ok(())
        }
        BytecodeTerminator::Match {
            value,
            cases,
            default,
        } => emit_match_terminator(
            out,
            function,
            block_index,
            frame,
            strings,
            value,
            cases,
            default,
        ),
    }
}

fn emit_match_terminator(
    out: &mut String,
    function: &BytecodeFunction,
    block_index: usize,
    frame: &FrameLayout,
    strings: &RuntimeStrings,
    value: &BytecodeOperand,
    cases: &[BytecodeMatchCase],
    default: &BytecodeEdge,
) -> Result<(), String> {
    emit_load_scalar_operand(out, frame, value, "x9")?;
    let default_label = format!(".L_{}_{}_match_default", function.name, block_index);
    let case_labels = (0..cases.len())
        .map(|index| format!(".L_{}_{}_match_case_{}", function.name, block_index, index))
        .collect::<Vec<_>>();
    for (index, case) in cases.iter().enumerate() {
        emit_load_immediate(out, "x10", case.tag_index as i64, false)?;
        out.push_str("  cmp x9, x10\n");
        out.push_str(&format!("  b.eq {}\n", case_labels[index]));
    }
    out.push_str(&format!("  b {default_label}\n"));
    for (index, case) in cases.iter().enumerate() {
        out.push_str(&format!("{}:\n", case_labels[index]));
        emit_edge_moves(out, frame, strings, &case.edge)?;
        out.push_str(&format!(
            "  b {}\n",
            block_label(function, case.edge.target)
        ));
    }
    out.push_str(&format!("{default_label}:\n"));
    emit_edge_moves(out, frame, strings, default)?;
    out.push_str(&format!("  b {}\n", block_label(function, default.target)));
    Ok(())
}

fn emit_edge_jump(
    out: &mut String,
    function: &BytecodeFunction,
    frame: &FrameLayout,
    strings: &RuntimeStrings,
    edge: &BytecodeEdge,
) -> Result<(), String> {
    emit_edge_moves(out, frame, strings, edge)?;
    out.push_str(&format!("  b {}\n", block_label(function, edge.target)));
    Ok(())
}

fn emit_edge_moves(
    out: &mut String,
    frame: &FrameLayout,
    strings: &RuntimeStrings,
    edge: &BytecodeEdge,
) -> Result<(), String> {
    if !edge_needs_staging(edge) {
        for instruction in &edge.moves {
            emit_instruction(out, frame, strings, instruction)?;
        }
        return Ok(());
    }
    for (index, instruction) in edge.moves.iter().enumerate() {
        let temp = frame.temp_slot(index, instruction.dst_kind)?;
        emit_expr_to_dst(out, frame, strings, temp, &instruction.expr)?;
    }
    for (index, instruction) in edge.moves.iter().enumerate() {
        let src = frame.temp_slot(index, instruction.dst_kind)?;
        let dst = frame.slot(instruction.dst)?;
        emit_copy_value(out, src, dst)?;
    }
    Ok(())
}

fn emit_copy_value(out: &mut String, src: SlotLayout, dst: SlotLayout) -> Result<(), String> {
    if same_home(src, dst) {
        return Ok(());
    }
    match dst.kind {
        BytecodeValueKind::SpanI32 | BytecodeValueKind::BufU8 => {
            let (ptr, len) = load_span_layout(out, src)?;
            emit_store_span_regs(out, dst, &ptr, &len)
        }
        _ => {
            emit_load_scalar_layout(out, src, "x9")?;
            emit_store_scalar_reg(out, dst, "x9")
        }
    }
}

fn emit_load_return_value(
    out: &mut String,
    frame: &FrameLayout,
    operand: &BytecodeOperand,
    kind: BytecodeValueKind,
) -> Result<(), String> {
    match kind {
        BytecodeValueKind::SpanI32 => Err("arm64 backend cannot return span values".to_string()),
        _ => emit_load_scalar_operand(out, frame, operand, "x0"),
    }
}

fn emit_load_scalar_operand(
    out: &mut String,
    frame: &FrameLayout,
    operand: &BytecodeOperand,
    reg: &str,
) -> Result<(), String> {
    match operand {
        BytecodeOperand::Slot { index, .. } => {
            emit_load_scalar_layout(out, frame.slot(*index)?, reg)
        }
        BytecodeOperand::Imm(immediate) => emit_immediate_operand(out, reg, immediate),
    }
}

fn emit_load_scalar_layout(out: &mut String, layout: SlotLayout, reg: &str) -> Result<(), String> {
    match layout.home {
        SlotHome::Reg(src) => match layout.kind {
            BytecodeValueKind::I64 | BytecodeValueKind::U64 | BytecodeValueKind::I32 => {
                if src != reg {
                    out.push_str(&format!("  mov {reg}, {src}\n"));
                }
            }
            BytecodeValueKind::U32 | BytecodeValueKind::U8 | BytecodeValueKind::Bool => {
                let dst = to_w(reg)?;
                let src_w = to_w(src)?;
                if dst != src_w {
                    out.push_str(&format!("  mov {dst}, {src_w}\n"));
                }
            }
            BytecodeValueKind::SpanI32 | BytecodeValueKind::BufU8 => {
                return Err("arm64 backend expected scalar layout, got aggregate".to_string())
            }
        },
        SlotHome::Stack(offset) => match layout.kind {
            BytecodeValueKind::I32 => {
                out.push_str(&format!("  ldrsw {reg}, [sp, #{}]\n", offset));
            }
            BytecodeValueKind::I64 | BytecodeValueKind::U64 => {
                out.push_str(&format!("  ldr {reg}, [sp, #{}]\n", offset));
            }
            BytecodeValueKind::U32 | BytecodeValueKind::U8 | BytecodeValueKind::Bool => {
                let wreg = to_w(reg)?;
                out.push_str(&format!("  ldr {wreg}, [sp, #{}]\n", offset));
            }
            BytecodeValueKind::SpanI32 | BytecodeValueKind::BufU8 => {
                return Err("arm64 backend expected scalar layout, got aggregate".to_string())
            }
        },
    }
    Ok(())
}

fn operand_slot_layout(
    frame: &FrameLayout,
    operand: &BytecodeOperand,
) -> Result<SlotLayout, String> {
    match operand {
        BytecodeOperand::Slot { index, .. } => frame.slot(*index),
        _ => Err("arm64 backend only supports span moves from span slots".to_string()),
    }
}

fn emit_load_span_ptr(out: &mut String, layout: SlotLayout, reg: &str) -> Result<(), String> {
    match layout.home {
        SlotHome::Stack(offset) => {
            out.push_str(&format!("  ldr {reg}, [sp, #{}]\n", offset));
            Ok(())
        }
        SlotHome::Reg(_) => Err("arm64 backend does not keep span values in registers".to_string()),
    }
}

fn emit_load_span_len(out: &mut String, layout: SlotLayout, reg: &str) -> Result<(), String> {
    match layout.home {
        SlotHome::Stack(offset) => {
            let wreg = to_w(reg)?;
            out.push_str(&format!("  ldr {wreg}, [sp, #{}]\n", offset + 8));
            Ok(())
        }
        SlotHome::Reg(_) => Err("arm64 backend does not keep span values in registers".to_string()),
    }
}

fn load_span_layout(out: &mut String, layout: SlotLayout) -> Result<(String, String), String> {
    match layout.home {
        SlotHome::Stack(offset) => {
            out.push_str(&format!("  ldr x9, [sp, #{}]\n", offset));
            out.push_str(&format!("  ldr x10, [sp, #{}]\n", offset + 8));
            Ok(("x9".to_string(), "x10".to_string()))
        }
        SlotHome::Reg(_) => Err("arm64 backend does not keep span values in registers".to_string()),
    }
}

fn emit_immediate_operand(
    out: &mut String,
    reg: &str,
    immediate: &BytecodeImmediate,
) -> Result<(), String> {
    match immediate {
        BytecodeImmediate::U8(value) => emit_load_immediate(out, reg, *value as i64, false),
        BytecodeImmediate::I32(value) => emit_load_immediate(out, reg, *value as i64, false),
        BytecodeImmediate::I64(value) => emit_load_immediate(out, reg, *value, true),
        BytecodeImmediate::U64(value) => emit_load_u64_immediate(out, reg, *value),
        BytecodeImmediate::U32(value) => emit_load_immediate(out, reg, *value as i64, false),
        BytecodeImmediate::Bool(value) => {
            emit_load_immediate(out, reg, if *value { 1 } else { 0 }, false)
        }
    }
}

fn emit_load_immediate(
    out: &mut String,
    reg: &str,
    value: i64,
    force_64: bool,
) -> Result<(), String> {
    let mut raw = value as u64;
    let wreg = to_w(reg)?;
    if !force_64 && (0..=u32::MAX as i64).contains(&value) {
        raw = value as u32 as u64;
    }
    let chunks = [
        (raw & 0xffff) as u16,
        ((raw >> 16) & 0xffff) as u16,
        ((raw >> 32) & 0xffff) as u16,
        ((raw >> 48) & 0xffff) as u16,
    ];
    let use_64 = force_64 || value < 0 || raw > u32::MAX as u64;
    let target = if use_64 { reg.to_string() } else { wreg };
    let width_chunks = if use_64 { 4 } else { 2 };
    let mut first = true;
    for (index, chunk) in chunks.iter().enumerate().take(width_chunks) {
        if first {
            out.push_str(&format!("  movz {target}, #{}\n", chunk));
            first = false;
        } else if *chunk != 0 {
            out.push_str(&format!(
                "  movk {target}, #{}, lsl #{}\n",
                chunk,
                index * 16
            ));
        }
    }
    if first {
        out.push_str(&format!("  movz {target}, #0\n"));
    }
    Ok(())
}

fn emit_load_u64_immediate(out: &mut String, reg: &str, value: u64) -> Result<(), String> {
    let chunks = [
        (value & 0xffff) as u16,
        ((value >> 16) & 0xffff) as u16,
        ((value >> 32) & 0xffff) as u16,
        ((value >> 48) & 0xffff) as u16,
    ];
    let mut first = true;
    for (index, chunk) in chunks.iter().enumerate() {
        if first {
            out.push_str(&format!("  movz {reg}, #{}\n", chunk));
            first = false;
        } else if *chunk != 0 {
            out.push_str(&format!("  movk {reg}, #{}, lsl #{}\n", chunk, index * 16));
        }
    }
    if first {
        out.push_str(&format!("  movz {reg}, #0\n"));
    }
    Ok(())
}

fn emit_store_scalar_reg(out: &mut String, dst: SlotLayout, reg: &str) -> Result<(), String> {
    match dst.home {
        SlotHome::Stack(offset) => match dst.kind {
            BytecodeValueKind::I64 | BytecodeValueKind::U64 => {
                out.push_str(&format!("  str {reg}, [sp, #{}]\n", offset));
            }
            BytecodeValueKind::I32
            | BytecodeValueKind::U32
            | BytecodeValueKind::U8
            | BytecodeValueKind::Bool => {
                out.push_str(&format!("  str {}, [sp, #{}]\n", to_w(reg)?, offset));
            }
            BytecodeValueKind::SpanI32 | BytecodeValueKind::BufU8 => unreachable!(),
        },
        SlotHome::Reg(dst_reg) => match dst.kind {
            BytecodeValueKind::I64 | BytecodeValueKind::U64 => {
                if dst_reg != reg {
                    out.push_str(&format!("  mov {dst_reg}, {reg}\n"));
                }
            }
            BytecodeValueKind::I32 => {
                out.push_str(&format!("  sxtw {dst_reg}, {}\n", to_w(reg)?));
            }
            BytecodeValueKind::U32 | BytecodeValueKind::U8 | BytecodeValueKind::Bool => {
                out.push_str(&format!("  mov {}, {}\n", to_w(dst_reg)?, to_w(reg)?));
            }
            BytecodeValueKind::SpanI32 | BytecodeValueKind::BufU8 => unreachable!(),
        },
    }
    Ok(())
}

fn emit_store_span_regs(
    out: &mut String,
    dst: SlotLayout,
    ptr_reg: &str,
    len_reg: &str,
) -> Result<(), String> {
    match dst.home {
        SlotHome::Stack(offset) => {
            out.push_str(&format!("  str {ptr_reg}, [sp, #{}]\n", offset));
            out.push_str(&format!("  str {len_reg}, [sp, #{}]\n", offset + 8));
            Ok(())
        }
        SlotHome::Reg(_) => Err("arm64 backend does not keep span values in registers".to_string()),
    }
}

fn emit_function_epilogue(out: &mut String, frame: &FrameLayout) {
    emit_restore_regs(out, frame);
    if frame.frame_size > 0 {
        out.push_str(&format!("  add sp, sp, #{}\n", frame.frame_size));
    }
    out.push_str("  ldp x29, x30, [sp], #16\n");
    out.push_str("  ret\n");
}

fn exported_symbol(function: &BytecodeFunction) -> String {
    if cfg!(target_vendor = "apple") {
        format!("_mira_func_{}", function.name)
    } else {
        format!("mira_func_{}", function.name)
    }
}

fn block_label(function: &BytecodeFunction, index: usize) -> String {
    format!(".L_{}_b{}", function.name, index)
}

fn operand_kind(operand: &BytecodeOperand) -> BytecodeValueKind {
    match operand {
        BytecodeOperand::Slot { kind, .. } => *kind,
        BytecodeOperand::Imm(BytecodeImmediate::I32(_)) => BytecodeValueKind::I32,
        BytecodeOperand::Imm(BytecodeImmediate::I64(_)) => BytecodeValueKind::I64,
        BytecodeOperand::Imm(BytecodeImmediate::U64(_)) => BytecodeValueKind::U64,
        BytecodeOperand::Imm(BytecodeImmediate::U32(_)) => BytecodeValueKind::U32,
        BytecodeOperand::Imm(BytecodeImmediate::U8(_)) => BytecodeValueKind::U8,
        BytecodeOperand::Imm(BytecodeImmediate::Bool(_)) => BytecodeValueKind::Bool,
    }
}

fn reg_x(index: usize) -> Result<&'static str, String> {
    match index {
        0 => Ok("x0"),
        1 => Ok("x1"),
        2 => Ok("x2"),
        3 => Ok("x3"),
        4 => Ok("x4"),
        5 => Ok("x5"),
        6 => Ok("x6"),
        7 => Ok("x7"),
        _ => Err(format!(
            "arm64 backend does not support arg register x{index}"
        )),
    }
}

fn to_w(reg: &str) -> Result<String, String> {
    if let Some(number) = reg.strip_prefix('x') {
        Ok(format!("w{number}"))
    } else {
        Err(format!("expected x register, got {reg}"))
    }
}

fn build_frame_layout(function: &BytecodeFunction) -> FrameLayout {
    let assigned = choose_register_slots(function);
    let mut saved_regs = assigned.iter().map(|(_, reg)| *reg).collect::<Vec<_>>();
    saved_regs.sort_unstable();
    saved_regs.dedup();

    let save_size = saved_regs.len() * 8;
    let temp_base = align_up(save_size, 16);
    let temp_count = max_edge_moves(function);
    let mut offset = temp_base + (temp_count * 16);
    let rand_state_offset = function_uses_rand(function).then(|| {
        let slot = offset;
        offset += 8;
        slot
    });

    let mut slots = Vec::with_capacity(function.slot_kinds.len());
    for (index, kind) in function.slot_kinds.iter().copied().enumerate() {
        if let Some(reg) = assigned
            .iter()
            .find_map(|(slot, reg)| (*slot == index).then_some(*reg))
        {
            slots.push(SlotLayout {
                home: SlotHome::Reg(reg),
                kind,
            });
        } else {
            let size = match kind {
                BytecodeValueKind::SpanI32 | BytecodeValueKind::BufU8 => 16,
                _ => 8,
            };
            slots.push(SlotLayout {
                home: SlotHome::Stack(offset),
                kind,
            });
            offset += size;
        }
    }

    FrameLayout {
        slots,
        saved_regs,
        temp_base,
        temp_count,
        rand_state_offset,
        frame_size: align_up(offset, 16),
    }
}

fn function_uses_rand(function: &BytecodeFunction) -> bool {
    function.blocks.iter().any(block_uses_rand)
}

fn block_uses_rand(block: &BytecodeBlock) -> bool {
    block
        .instructions
        .iter()
        .any(|instruction| matches!(instruction.expr, BytecodeExpr::RandU32))
        || edge_uses_rand_in_terminator(&block.terminator)
}

fn edge_uses_rand_in_terminator(terminator: &BytecodeTerminator) -> bool {
    match terminator {
        BytecodeTerminator::Return(_) => false,
        BytecodeTerminator::Jump(edge) => edge_uses_rand(edge),
        BytecodeTerminator::Branch { truthy, falsy, .. } => {
            edge_uses_rand(truthy) || edge_uses_rand(falsy)
        }
        BytecodeTerminator::Match { cases, default, .. } => {
            cases.iter().any(|case| edge_uses_rand(&case.edge)) || edge_uses_rand(default)
        }
    }
}

fn edge_uses_rand(edge: &BytecodeEdge) -> bool {
    edge.moves
        .iter()
        .any(|instruction| matches!(instruction.expr, BytecodeExpr::RandU32))
}

fn collect_runtime_strings(function: &BytecodeFunction) -> RuntimeStrings {
    let mut labels = BTreeMap::new();
    let mut ordered = Vec::new();
    let mut next = 0usize;
    for block in &function.blocks {
        collect_runtime_strings_from_instructions(
            &mut labels,
            &mut ordered,
            &mut next,
            &function.name,
            &block.instructions,
        );
        collect_runtime_strings_from_terminator(
            &mut labels,
            &mut ordered,
            &mut next,
            &function.name,
            &block.terminator,
        );
    }
    RuntimeStrings { labels, ordered }
}

fn collect_runtime_strings_from_terminator(
    labels: &mut BTreeMap<String, String>,
    ordered: &mut Vec<(String, String)>,
    next: &mut usize,
    function_name: &str,
    terminator: &BytecodeTerminator,
) {
    match terminator {
        BytecodeTerminator::Return(_) => {}
        BytecodeTerminator::Jump(edge) => collect_runtime_strings_from_instructions(
            labels,
            ordered,
            next,
            function_name,
            &edge.moves,
        ),
        BytecodeTerminator::Branch { truthy, falsy, .. } => {
            collect_runtime_strings_from_instructions(
                labels,
                ordered,
                next,
                function_name,
                &truthy.moves,
            );
            collect_runtime_strings_from_instructions(
                labels,
                ordered,
                next,
                function_name,
                &falsy.moves,
            );
        }
        BytecodeTerminator::Match { cases, default, .. } => {
            for case in cases {
                collect_runtime_strings_from_instructions(
                    labels,
                    ordered,
                    next,
                    function_name,
                    &case.edge.moves,
                );
            }
            collect_runtime_strings_from_instructions(
                labels,
                ordered,
                next,
                function_name,
                &default.moves,
            );
        }
    }
}

fn collect_runtime_strings_from_instructions(
    labels: &mut BTreeMap<String, String>,
    ordered: &mut Vec<(String, String)>,
    next: &mut usize,
    function_name: &str,
    instructions: &[BytecodeInstruction],
) {
    for instruction in instructions {
        match &instruction.expr {
            BytecodeExpr::FsReadU32 { path } | BytecodeExpr::FsWriteU32 { path, .. } => {
                insert_runtime_string(labels, ordered, next, function_name, path);
            }
            BytecodeExpr::RtSpawnU32 { function, .. } => {
                insert_runtime_string(labels, ordered, next, function_name, function);
            }
            BytecodeExpr::SpawnCall { command, .. } => {
                insert_runtime_string(labels, ordered, next, function_name, command);
            }
            BytecodeExpr::NetConnect { host, .. } => {
                insert_runtime_string(labels, ordered, next, function_name, host);
            }
            BytecodeExpr::BufLit { literal } | BytecodeExpr::TlsServerConfigBuf { value: literal } => {
                insert_runtime_string(labels, ordered, next, function_name, literal);
            }
            BytecodeExpr::TlsListen { host, cert, key, .. } => {
                insert_runtime_string(labels, ordered, next, function_name, host);
                insert_runtime_string(labels, ordered, next, function_name, cert);
                insert_runtime_string(labels, ordered, next, function_name, key);
            }
            BytecodeExpr::ServiceOpen { name }
            | BytecodeExpr::ServiceTraceBegin { name, .. }
            | BytecodeExpr::ServiceErrorStatus { kind: name }
            | BytecodeExpr::DbOpen { path: name }
            | BytecodeExpr::JsonGetU32 { key: name, .. } => {
                insert_runtime_string(labels, ordered, next, function_name, name);
            }
            BytecodeExpr::ServiceRoute { method, path, .. } => {
                insert_runtime_string(labels, ordered, next, function_name, method);
                insert_runtime_string(labels, ordered, next, function_name, path);
            }
            BytecodeExpr::ServiceRequireHeader { name, value, .. } => {
                insert_runtime_string(labels, ordered, next, function_name, name);
                insert_runtime_string(labels, ordered, next, function_name, value);
            }
            BytecodeExpr::ServiceLog { .. } => {
                insert_runtime_string(labels, ordered, next, function_name, "info");
            }
            BytecodeExpr::ServiceMetricCount { .. } => {
                insert_runtime_string(labels, ordered, next, function_name, "count");
            }
            BytecodeExpr::ServiceMigrateDb { .. } => {
                insert_runtime_string(labels, ordered, next, function_name, "migration");
            }
            _ => {}
        }
    }
}

fn insert_runtime_string(
    labels: &mut BTreeMap<String, String>,
    ordered: &mut Vec<(String, String)>,
    next: &mut usize,
    function_name: &str,
    value: &str,
) {
    if labels.contains_key(value) {
        return;
    }
    let label = format!(".L_{}_rt_str_{}", function_name, *next);
    *next += 1;
    labels.insert(value.to_string(), label.clone());
    ordered.push((label, value.to_string()));
}

fn emit_arm64_string_section(out: &mut String, strings: &RuntimeStrings) {
    if strings.ordered.is_empty() {
        return;
    }
    out.push_str(".section __TEXT,__cstring,cstring_literals\n");
    for (label, value) in &strings.ordered {
        out.push_str(&format!("{label}:\n"));
        out.push_str(&format!("  .asciz \"{}\"\n", escape_asm_string(value)));
    }
    out.push_str(".text\n");
    out.push_str(".p2align 2\n");
}

fn escape_asm_string(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\"', "\\\"")
        .replace('\n', "\\n")
}

fn runtime_symbol(name: &str) -> String {
    extern_symbol(name)
}

fn extern_symbol(name: &str) -> String {
    if cfg!(target_vendor = "apple") {
        format!("_{name}")
    } else {
        name.to_string()
    }
}

fn choose_register_slots(function: &BytecodeFunction) -> Vec<(usize, &'static str)> {
    let scores = score_slots(function);
    let mut candidates = scores
        .into_iter()
        .filter_map(|(index, score)| {
            let kind = function.slot_kinds[index];
            (!matches!(kind, BytecodeValueKind::SpanI32 | BytecodeValueKind::BufU8)
                && score >= 8)
                .then_some((index, score))
        })
        .collect::<Vec<_>>();
    candidates
        .sort_unstable_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    candidates
        .into_iter()
        .take(REG_POOL.len())
        .zip(REG_POOL)
        .map(|((slot, _), reg)| (slot, reg))
        .collect()
}

fn score_slots(function: &BytecodeFunction) -> Vec<(usize, usize)> {
    let loop_blocks = detect_loop_blocks(function);
    let mut counts = vec![0usize; function.slot_count];
    for (block_index, block) in function.blocks.iter().enumerate() {
        let weight = if loop_blocks[block_index] { 8 } else { 1 };
        for instruction in &block.instructions {
            count_expr_uses(&instruction.expr, weight, &mut counts);
        }
        count_terminator_uses(&block.terminator, weight, &mut counts);
    }
    counts.into_iter().enumerate().collect()
}

fn count_expr_uses(expr: &BytecodeExpr, weight: usize, counts: &mut [usize]) {
    match expr {
        BytecodeExpr::Move(operand)
        | BytecodeExpr::AbsI32 { value: operand }
        | BytecodeExpr::SextI64 { value: operand } => {
            count_operand_use(operand, weight, counts);
        }
        BytecodeExpr::ClockNowNs
        | BytecodeExpr::RandU32
        | BytecodeExpr::RtCancelled
        | BytecodeExpr::FsReadU32 { .. }
        | BytecodeExpr::SpawnCall { .. }
        | BytecodeExpr::SpawnCaptureAllU8 { .. }
        | BytecodeExpr::SpawnCaptureStderrAllU8 { .. }
        | BytecodeExpr::SpawnOpen { .. }
        | BytecodeExpr::TaskOpen { .. }
        | BytecodeExpr::NetConnect { .. }
        | BytecodeExpr::NetListen { .. }
        | BytecodeExpr::FsReadAllU8 { .. }
        | BytecodeExpr::StrLit { .. }
        | BytecodeExpr::FfiOpenLib { .. }
        | BytecodeExpr::DbOpen { .. }
        | BytecodeExpr::ConfigGetU32 { .. }
        | BytecodeExpr::ConfigGetBool { .. }
        | BytecodeExpr::ConfigGetStr { .. }
        | BytecodeExpr::EnvGetU32 { .. }
        | BytecodeExpr::EnvGetBool { .. }
        | BytecodeExpr::EnvGetStr { .. }
        | BytecodeExpr::ServiceOpen { .. }
        | BytecodeExpr::ServiceErrorStatus { .. }
        | BytecodeExpr::TlsListen { .. }
        | BytecodeExpr::TlsServerConfigU32 { .. }
        | BytecodeExpr::TlsServerConfigBuf { .. } => {}
        BytecodeExpr::RtOpen { workers: value }
        | BytecodeExpr::FsWriteU32 { value, .. }
        | BytecodeExpr::FsWriteAllU8 { value, .. }
        | BytecodeExpr::NetWriteAllU8 { value, .. }
        | BytecodeExpr::NetExchangeAllU8 { value, .. }
        | BytecodeExpr::NetServeExchangeAllU8 {
            response: value, ..
        }
        | BytecodeExpr::ServiceShutdown {
            grace_ms: value, ..
        }
        | BytecodeExpr::ServiceLog { message: value, .. }
        | BytecodeExpr::ServiceMetricCount { value, .. }
        | BytecodeExpr::DropBufU8 { value } => count_operand_use(value, weight, counts),
        BytecodeExpr::RtDone { task: listener }
        | BytecodeExpr::RtJoinU32 { task: listener }
        | BytecodeExpr::RtCancel { task: listener }
        | BytecodeExpr::RtTaskClose { task: listener }
        | BytecodeExpr::RtClose { runtime: listener }
        | BytecodeExpr::ChanRecvU32 { channel: listener }
        | BytecodeExpr::ChanClose { channel: listener }
        | BytecodeExpr::NetAccept { listener }
        | BytecodeExpr::HttpSessionAccept { listener }
        | BytecodeExpr::ServiceClose { handle: listener }
        | BytecodeExpr::ServiceTraceBegin {
            handle: listener, ..
        }
        | BytecodeExpr::ServiceHealthStatus { handle: listener }
        | BytecodeExpr::ServiceReadinessStatus { handle: listener } => {
            count_operand_use(listener, weight, counts)
        }
        BytecodeExpr::RtSpawnU32 { runtime, arg, .. } => {
            count_operand_use(runtime, weight, counts);
            count_operand_use(arg, weight, counts);
        }
        BytecodeExpr::RtShutdown { runtime, grace_ms } => {
            count_operand_use(runtime, weight, counts);
            count_operand_use(grace_ms, weight, counts);
        }
        BytecodeExpr::ChanOpenU32 { capacity } => count_operand_use(capacity, weight, counts),
        BytecodeExpr::ChanSendU32 { channel, value } => {
            count_operand_use(channel, weight, counts);
            count_operand_use(value, weight, counts);
        }
        BytecodeExpr::ListenerSetTimeoutMs { handle, value }
        | BytecodeExpr::SessionSetTimeoutMs { handle, value }
        | BytecodeExpr::ListenerSetShutdownGraceMs { handle, value } => {
            count_operand_use(handle, weight, counts);
            count_operand_use(value, weight, counts);
        }
        BytecodeExpr::NetReadAllU8 { handle }
        | BytecodeExpr::NetClose { handle }
        | BytecodeExpr::HttpSessionRequest { handle }
        | BytecodeExpr::HttpSessionClose { handle }
        | BytecodeExpr::DbClose { handle }
        | BytecodeExpr::SpawnWait { handle }
        | BytecodeExpr::TaskDone { handle }
        | BytecodeExpr::TaskJoinStatus { handle }
        | BytecodeExpr::SpawnStdoutAllU8 { handle }
        | BytecodeExpr::TaskStdoutAllU8 { handle }
        | BytecodeExpr::SpawnStderrAllU8 { handle }
        | BytecodeExpr::TaskStderrAllU8 { handle }
        | BytecodeExpr::SpawnClose { handle }
        | BytecodeExpr::TaskClose { handle }
        | BytecodeExpr::FfiCloseLib { handle } => count_operand_use(handle, weight, counts),
        BytecodeExpr::DbExec { handle, sql }
        | BytecodeExpr::DbPrepare { handle, sql, .. }
        | BytecodeExpr::DbQueryU32 { handle, sql }
        | BytecodeExpr::DbQueryBufU8 { handle, sql } => {
            count_operand_use(handle, weight, counts);
            count_operand_use(sql, weight, counts);
        }
        BytecodeExpr::DbExecPrepared { handle, params, .. }
        | BytecodeExpr::DbQueryPreparedU32 { handle, params, .. }
        | BytecodeExpr::DbQueryPreparedBufU8 { handle, params, .. } => {
            count_operand_use(handle, weight, counts);
            count_operand_use(params, weight, counts);
        }
        BytecodeExpr::DbBegin { handle }
        | BytecodeExpr::DbCommit { handle }
        | BytecodeExpr::DbRollback { handle }
        | BytecodeExpr::DbPoolAcquire { pool: handle }
        | BytecodeExpr::DbPoolClose { pool: handle }
        | BytecodeExpr::ServiceTraceEnd { trace: handle } => {
            count_operand_use(handle, weight, counts)
        }
        BytecodeExpr::DbPoolOpen { max_size, .. } => count_operand_use(max_size, weight, counts),
        BytecodeExpr::DbPoolRelease { pool, handle }
        | BytecodeExpr::ServiceMigrateDb {
            handle: pool,
            db_handle: handle,
        } => {
            count_operand_use(pool, weight, counts);
            count_operand_use(handle, weight, counts);
        }
        BytecodeExpr::TlsExchangeAllU8 { value, .. } | BytecodeExpr::TaskSleepMs { value } => {
            count_operand_use(value, weight, counts);
        }
        BytecodeExpr::StrFromU32 { value }
        | BytecodeExpr::StrFromBool { value }
        | BytecodeExpr::StrToBuf { value }
        | BytecodeExpr::BufToStr { value }
        | BytecodeExpr::BufHexStr { value }
        | BytecodeExpr::JsonArrayLen { value }
        | BytecodeExpr::DateParseYmd { value }
        | BytecodeExpr::TimeParseHms { value }
        | BytecodeExpr::DateFormatYmd { value }
        | BytecodeExpr::TimeFormatHms { value } => count_operand_use(value, weight, counts),
        BytecodeExpr::NetWriteHandleAllU8 { handle, value } => {
            count_operand_use(handle, weight, counts);
            count_operand_use(value, weight, counts);
        }
        BytecodeExpr::HttpMethodEq { request, .. }
        | BytecodeExpr::HttpRequestMethod { request }
        | BytecodeExpr::HttpPathEq { request, .. }
        | BytecodeExpr::HttpRequestPath { request }
        | BytecodeExpr::HttpHeaderEq { request, .. }
        | BytecodeExpr::HttpCookieEq { request, .. }
        | BytecodeExpr::HttpStatusU32 { value: request }
        | BytecodeExpr::BufEqLit { value: request, .. }
        | BytecodeExpr::BufContainsLit { value: request, .. }
        | BytecodeExpr::StrEqLit { value: request, .. }
        | BytecodeExpr::HttpQueryParam { request, .. }
        | BytecodeExpr::HttpHeader { request, .. }
        | BytecodeExpr::HttpCookie { request, .. }
        | BytecodeExpr::HttpBody { request }
        | BytecodeExpr::HttpRouteParam { request, .. }
        | BytecodeExpr::ServiceRoute { request, .. }
        | BytecodeExpr::ServiceRequireHeader { request, .. } => {
            count_operand_use(request, weight, counts);
        }
        BytecodeExpr::HttpBodyLimit { request, limit } => {
            count_operand_use(request, weight, counts);
            count_operand_use(limit, weight, counts);
        }
        BytecodeExpr::HttpServerConfigU32 { .. } => {}
        BytecodeExpr::BufLit { .. } => {}
        BytecodeExpr::BufConcat { left, right } | BytecodeExpr::StrConcat { left, right } => {
            count_operand_use(left, weight, counts);
            count_operand_use(right, weight, counts);
        }
        BytecodeExpr::HttpWriteResponse {
            handle,
            status,
            body,
        } => {
            count_operand_use(handle, weight, counts);
            count_operand_use(status, weight, counts);
            count_operand_use(body, weight, counts);
        }
        BytecodeExpr::HttpWriteTextResponse {
            handle,
            status,
            body,
        }
        | BytecodeExpr::HttpWriteTextResponseCookie {
            handle,
            status,
            body,
            ..
        }
        | BytecodeExpr::HttpWriteTextResponseHeaders2 {
            handle,
            status,
            body,
            ..
        }
        | BytecodeExpr::HttpSessionWriteText {
            handle,
            status,
            body,
        }
        | BytecodeExpr::HttpSessionWriteTextCookie {
            handle,
            status,
            body,
            ..
        }
        | BytecodeExpr::HttpSessionWriteTextHeaders2 {
            handle,
            status,
            body,
            ..
        }
        | BytecodeExpr::HttpWriteJsonResponse {
            handle,
            status,
            body,
        }
        | BytecodeExpr::HttpWriteJsonResponseCookie {
            handle,
            status,
            body,
            ..
        }
        | BytecodeExpr::HttpWriteJsonResponseHeaders2 {
            handle,
            status,
            body,
            ..
        }
        | BytecodeExpr::HttpSessionWriteJson {
            handle,
            status,
            body,
        }
        | BytecodeExpr::HttpSessionWriteJsonCookie {
            handle,
            status,
            body,
            ..
        }
        | BytecodeExpr::HttpSessionWriteJsonHeaders2 {
            handle,
            status,
            body,
            ..
        } => {
            count_operand_use(handle, weight, counts);
            count_operand_use(status, weight, counts);
            count_operand_use(body, weight, counts);
        }
        BytecodeExpr::HttpWriteResponseHeader {
            handle,
            status,
            body,
            ..
        } => {
            count_operand_use(handle, weight, counts);
            count_operand_use(status, weight, counts);
            count_operand_use(body, weight, counts);
        }
        BytecodeExpr::JsonGetU32 { value, .. }
        | BytecodeExpr::JsonGetBool { value, .. }
        | BytecodeExpr::JsonGetBufU8 { value, .. }
        | BytecodeExpr::JsonGetStr { value, .. }
        | BytecodeExpr::BufParseU32 { value }
        | BytecodeExpr::BufParseBool { value } => {
            count_operand_use(value, weight, counts);
        }
        BytecodeExpr::JsonIndexU32 { value, index }
        | BytecodeExpr::JsonIndexBool { value, index }
        | BytecodeExpr::JsonIndexStr { value, index } => {
            count_operand_use(value, weight, counts);
            count_operand_use(index, weight, counts);
        }
        BytecodeExpr::JsonEncodeObj { entries } => {
            for (_, operand) in entries {
                count_operand_use(operand, weight, counts);
            }
        }
        BytecodeExpr::AllocBufU8 { len } => count_operand_use(len, weight, counts),
        BytecodeExpr::FfiCall { args, .. } => {
            for operand in args {
                count_operand_use(operand, weight, counts);
            }
        }
        BytecodeExpr::FfiCallCStr { arg_slot, .. } => counts[*arg_slot] += weight,
        BytecodeExpr::FfiBufPtr { value } => count_operand_use(value, weight, counts),
        BytecodeExpr::FfiCallLib { handle, args, .. } => {
            count_operand_use(handle, weight, counts);
            for operand in args {
                count_operand_use(operand, weight, counts);
            }
        }
        BytecodeExpr::FfiCallLibCStr {
            handle, arg_slot, ..
        } => {
            count_operand_use(handle, weight, counts);
            counts[*arg_slot] += weight;
        }
        BytecodeExpr::LenSpanI32 { source } | BytecodeExpr::LenBufU8 { source } => {
            counts[*source] += weight
        }
        BytecodeExpr::StoreBufU8 {
            source,
            index,
            value,
        } => {
            counts[*source] += weight;
            count_operand_use(index, weight, counts);
            count_operand_use(value, weight, counts);
        }
        BytecodeExpr::LoadSpanI32 { source, index } => {
            counts[*source] += weight;
            count_operand_use(index, weight, counts);
        }
        BytecodeExpr::LoadBufU8 { source, index } => {
            counts[*source] += weight;
            count_operand_use(index, weight, counts);
        }
        BytecodeExpr::Binary { left, right, .. } => {
            count_operand_use(left, weight, counts);
            count_operand_use(right, weight, counts);
        }
        _ => {}
    }
}

fn count_terminator_uses(terminator: &BytecodeTerminator, weight: usize, counts: &mut [usize]) {
    match terminator {
        BytecodeTerminator::Return(value) => count_operand_use(value, weight, counts),
        BytecodeTerminator::Jump(edge) => count_edge_uses(edge, weight, counts),
        BytecodeTerminator::Branch {
            condition,
            truthy,
            falsy,
        } => {
            count_operand_use(condition, weight, counts);
            count_edge_uses(truthy, weight, counts);
            count_edge_uses(falsy, weight, counts);
        }
        BytecodeTerminator::Match {
            value,
            cases,
            default,
        } => {
            count_operand_use(value, weight, counts);
            for case in cases {
                count_edge_uses(&case.edge, weight, counts);
            }
            count_edge_uses(default, weight, counts);
        }
    }
}

fn count_edge_uses(edge: &BytecodeEdge, weight: usize, counts: &mut [usize]) {
    for instruction in &edge.moves {
        count_expr_uses(&instruction.expr, weight, counts);
        counts[instruction.dst] += weight;
    }
}

fn count_operand_use(operand: &BytecodeOperand, weight: usize, counts: &mut [usize]) {
    if let BytecodeOperand::Slot { index, .. } = operand {
        counts[*index] += weight;
    }
}

fn max_edge_moves(function: &BytecodeFunction) -> usize {
    let mut max_count = 0usize;
    for block in &function.blocks {
        match &block.terminator {
            BytecodeTerminator::Return(_) => {}
            BytecodeTerminator::Jump(edge) => max_count = max_count.max(edge.moves.len()),
            BytecodeTerminator::Branch { truthy, falsy, .. } => {
                max_count = max_count.max(truthy.moves.len()).max(falsy.moves.len());
            }
            BytecodeTerminator::Match { cases, default, .. } => {
                max_count = max_count.max(default.moves.len());
                for case in cases {
                    max_count = max_count.max(case.edge.moves.len());
                }
            }
        }
    }
    max_count
}

fn edge_needs_staging(edge: &BytecodeEdge) -> bool {
    for (index, instruction) in edge.moves.iter().enumerate() {
        for later in edge.moves.iter().skip(index + 1) {
            if expr_reads_slot(&later.expr, instruction.dst) {
                return true;
            }
        }
    }
    false
}

fn expr_reads_slot(expr: &BytecodeExpr, slot: usize) -> bool {
    match expr {
        BytecodeExpr::Move(operand)
        | BytecodeExpr::AbsI32 { value: operand }
        | BytecodeExpr::SextI64 { value: operand } => operand_reads_slot(operand, slot),
        BytecodeExpr::ClockNowNs
        | BytecodeExpr::RandU32
        | BytecodeExpr::RtCancelled
        | BytecodeExpr::FsReadU32 { .. }
        | BytecodeExpr::SpawnCall { .. }
        | BytecodeExpr::SpawnCaptureAllU8 { .. }
        | BytecodeExpr::SpawnCaptureStderrAllU8 { .. }
        | BytecodeExpr::SpawnOpen { .. }
        | BytecodeExpr::TaskOpen { .. }
        | BytecodeExpr::NetListen { .. }
        | BytecodeExpr::NetConnect { .. }
        | BytecodeExpr::FsReadAllU8 { .. }
        | BytecodeExpr::StrLit { .. }
        | BytecodeExpr::FfiOpenLib { .. }
        | BytecodeExpr::DbOpen { .. }
        | BytecodeExpr::ConfigGetU32 { .. }
        | BytecodeExpr::ConfigGetBool { .. }
        | BytecodeExpr::ConfigGetStr { .. }
        | BytecodeExpr::EnvGetU32 { .. }
        | BytecodeExpr::EnvGetBool { .. }
        | BytecodeExpr::EnvGetStr { .. }
        | BytecodeExpr::ServiceOpen { .. }
        | BytecodeExpr::ServiceErrorStatus { .. }
        | BytecodeExpr::TlsListen { .. }
        | BytecodeExpr::TlsServerConfigU32 { .. }
        | BytecodeExpr::TlsServerConfigBuf { .. } => false,
        BytecodeExpr::RtOpen { workers: value }
        | BytecodeExpr::FsWriteU32 { value, .. }
        | BytecodeExpr::FsWriteAllU8 { value, .. }
        | BytecodeExpr::NetWriteAllU8 { value, .. }
        | BytecodeExpr::NetExchangeAllU8 { value, .. }
        | BytecodeExpr::NetServeExchangeAllU8 {
            response: value, ..
        }
        | BytecodeExpr::ServiceShutdown {
            grace_ms: value, ..
        }
        | BytecodeExpr::ServiceLog { message: value, .. }
        | BytecodeExpr::ServiceMetricCount { value, .. }
        | BytecodeExpr::DropBufU8 { value } => operand_reads_slot(value, slot),
        BytecodeExpr::RtDone { task: listener }
        | BytecodeExpr::RtJoinU32 { task: listener }
        | BytecodeExpr::RtCancel { task: listener }
        | BytecodeExpr::RtTaskClose { task: listener }
        | BytecodeExpr::RtClose { runtime: listener }
        | BytecodeExpr::ChanRecvU32 { channel: listener }
        | BytecodeExpr::ChanClose { channel: listener }
        | BytecodeExpr::NetAccept { listener }
        | BytecodeExpr::HttpSessionAccept { listener }
        | BytecodeExpr::ServiceClose { handle: listener }
        | BytecodeExpr::ServiceTraceBegin {
            handle: listener, ..
        }
        | BytecodeExpr::ServiceHealthStatus { handle: listener }
        | BytecodeExpr::ServiceReadinessStatus { handle: listener } => {
            operand_reads_slot(listener, slot)
        }
        BytecodeExpr::RtSpawnU32 { runtime, arg, .. } => {
            operand_reads_slot(runtime, slot) || operand_reads_slot(arg, slot)
        }
        BytecodeExpr::RtShutdown { runtime, grace_ms } => {
            operand_reads_slot(runtime, slot) || operand_reads_slot(grace_ms, slot)
        }
        BytecodeExpr::ChanOpenU32 { capacity } => operand_reads_slot(capacity, slot),
        BytecodeExpr::ChanSendU32 { channel, value } => {
            operand_reads_slot(channel, slot) || operand_reads_slot(value, slot)
        }
        BytecodeExpr::ListenerSetTimeoutMs { handle, value }
        | BytecodeExpr::SessionSetTimeoutMs { handle, value }
        | BytecodeExpr::ListenerSetShutdownGraceMs { handle, value } => {
            operand_reads_slot(handle, slot) || operand_reads_slot(value, slot)
        }
        BytecodeExpr::NetReadAllU8 { handle }
        | BytecodeExpr::NetClose { handle }
        | BytecodeExpr::HttpSessionRequest { handle }
        | BytecodeExpr::HttpSessionClose { handle }
        | BytecodeExpr::DbClose { handle }
        | BytecodeExpr::SpawnWait { handle }
        | BytecodeExpr::TaskDone { handle }
        | BytecodeExpr::TaskJoinStatus { handle }
        | BytecodeExpr::SpawnStdoutAllU8 { handle }
        | BytecodeExpr::TaskStdoutAllU8 { handle }
        | BytecodeExpr::SpawnStderrAllU8 { handle }
        | BytecodeExpr::TaskStderrAllU8 { handle }
        | BytecodeExpr::SpawnClose { handle }
        | BytecodeExpr::TaskClose { handle }
        | BytecodeExpr::FfiCloseLib { handle } => operand_reads_slot(handle, slot),
        BytecodeExpr::DbExec { handle, sql }
        | BytecodeExpr::DbPrepare { handle, sql, .. }
        | BytecodeExpr::DbQueryU32 { handle, sql }
        | BytecodeExpr::DbQueryBufU8 { handle, sql } => {
            operand_reads_slot(handle, slot) || operand_reads_slot(sql, slot)
        }
        BytecodeExpr::DbExecPrepared { handle, params, .. }
        | BytecodeExpr::DbQueryPreparedU32 { handle, params, .. }
        | BytecodeExpr::DbQueryPreparedBufU8 { handle, params, .. } => {
            operand_reads_slot(handle, slot) || operand_reads_slot(params, slot)
        }
        BytecodeExpr::DbBegin { handle }
        | BytecodeExpr::DbCommit { handle }
        | BytecodeExpr::DbRollback { handle }
        | BytecodeExpr::DbPoolAcquire { pool: handle }
        | BytecodeExpr::DbPoolClose { pool: handle }
        | BytecodeExpr::ServiceTraceEnd { trace: handle } => operand_reads_slot(handle, slot),
        BytecodeExpr::DbPoolOpen { max_size, .. } => operand_reads_slot(max_size, slot),
        BytecodeExpr::DbPoolRelease { pool, handle }
        | BytecodeExpr::ServiceMigrateDb {
            handle: pool,
            db_handle: handle,
        } => operand_reads_slot(pool, slot) || operand_reads_slot(handle, slot),
        BytecodeExpr::TlsExchangeAllU8 { value, .. } | BytecodeExpr::TaskSleepMs { value } => {
            operand_reads_slot(value, slot)
        }
        BytecodeExpr::StrFromU32 { value }
        | BytecodeExpr::StrFromBool { value }
        | BytecodeExpr::StrToBuf { value }
        | BytecodeExpr::BufToStr { value }
        | BytecodeExpr::BufHexStr { value }
        | BytecodeExpr::JsonArrayLen { value }
        | BytecodeExpr::DateParseYmd { value }
        | BytecodeExpr::TimeParseHms { value }
        | BytecodeExpr::DateFormatYmd { value }
        | BytecodeExpr::TimeFormatHms { value } => operand_reads_slot(value, slot),
        BytecodeExpr::NetWriteHandleAllU8 { handle, value } => {
            operand_reads_slot(handle, slot) || operand_reads_slot(value, slot)
        }
        BytecodeExpr::HttpMethodEq { request, .. }
        | BytecodeExpr::HttpRequestMethod { request }
        | BytecodeExpr::HttpPathEq { request, .. }
        | BytecodeExpr::HttpRequestPath { request }
        | BytecodeExpr::HttpHeaderEq { request, .. }
        | BytecodeExpr::HttpCookieEq { request, .. }
        | BytecodeExpr::HttpStatusU32 { value: request }
        | BytecodeExpr::BufEqLit { value: request, .. }
        | BytecodeExpr::BufContainsLit { value: request, .. }
        | BytecodeExpr::StrEqLit { value: request, .. }
        | BytecodeExpr::HttpQueryParam { request, .. }
        | BytecodeExpr::HttpHeader { request, .. }
        | BytecodeExpr::HttpCookie { request, .. }
        | BytecodeExpr::HttpBody { request }
        | BytecodeExpr::HttpRouteParam { request, .. }
        | BytecodeExpr::ServiceRoute { request, .. }
        | BytecodeExpr::ServiceRequireHeader { request, .. } => operand_reads_slot(request, slot),
        BytecodeExpr::HttpBodyLimit { request, limit } => {
            operand_reads_slot(request, slot) || operand_reads_slot(limit, slot)
        }
        BytecodeExpr::HttpServerConfigU32 { .. } => false,
        BytecodeExpr::BufLit { .. } => false,
        BytecodeExpr::BufConcat { left, right } | BytecodeExpr::StrConcat { left, right } => {
            operand_reads_slot(left, slot) || operand_reads_slot(right, slot)
        }
        BytecodeExpr::HttpWriteResponse {
            handle,
            status,
            body,
        } => {
            operand_reads_slot(handle, slot)
                || operand_reads_slot(status, slot)
                || operand_reads_slot(body, slot)
        }
        BytecodeExpr::HttpWriteTextResponse {
            handle,
            status,
            body,
        }
        | BytecodeExpr::HttpWriteTextResponseCookie {
            handle,
            status,
            body,
            ..
        }
        | BytecodeExpr::HttpWriteTextResponseHeaders2 {
            handle,
            status,
            body,
            ..
        }
        | BytecodeExpr::HttpSessionWriteText {
            handle,
            status,
            body,
        }
        | BytecodeExpr::HttpSessionWriteTextCookie {
            handle,
            status,
            body,
            ..
        }
        | BytecodeExpr::HttpSessionWriteTextHeaders2 {
            handle,
            status,
            body,
            ..
        }
        | BytecodeExpr::HttpWriteJsonResponse {
            handle,
            status,
            body,
        }
        | BytecodeExpr::HttpWriteJsonResponseCookie {
            handle,
            status,
            body,
            ..
        }
        | BytecodeExpr::HttpWriteJsonResponseHeaders2 {
            handle,
            status,
            body,
            ..
        }
        | BytecodeExpr::HttpSessionWriteJson {
            handle,
            status,
            body,
        }
        | BytecodeExpr::HttpSessionWriteJsonCookie {
            handle,
            status,
            body,
            ..
        }
        | BytecodeExpr::HttpSessionWriteJsonHeaders2 {
            handle,
            status,
            body,
            ..
        } => {
            operand_reads_slot(handle, slot)
                || operand_reads_slot(status, slot)
                || operand_reads_slot(body, slot)
        }
        BytecodeExpr::HttpWriteResponseHeader {
            handle,
            status,
            body,
            ..
        } => {
            operand_reads_slot(handle, slot)
                || operand_reads_slot(status, slot)
                || operand_reads_slot(body, slot)
        }
        BytecodeExpr::JsonGetU32 { value, .. }
        | BytecodeExpr::JsonGetBool { value, .. }
        | BytecodeExpr::JsonGetBufU8 { value, .. }
        | BytecodeExpr::JsonGetStr { value, .. }
        | BytecodeExpr::BufParseU32 { value }
        | BytecodeExpr::BufParseBool { value } => operand_reads_slot(value, slot),
        BytecodeExpr::JsonIndexU32 { value, index }
        | BytecodeExpr::JsonIndexBool { value, index }
        | BytecodeExpr::JsonIndexStr { value, index } => {
            operand_reads_slot(value, slot) || operand_reads_slot(index, slot)
        }
        BytecodeExpr::JsonEncodeObj { entries } => entries
            .iter()
            .any(|(_, operand)| operand_reads_slot(operand, slot)),
        BytecodeExpr::AllocBufU8 { len } => operand_reads_slot(len, slot),
        BytecodeExpr::FfiCall { args, .. } => {
            args.iter().any(|operand| operand_reads_slot(operand, slot))
        }
        BytecodeExpr::FfiCallCStr { arg_slot, .. } => *arg_slot == slot,
        BytecodeExpr::FfiBufPtr { value } => operand_reads_slot(value, slot),
        BytecodeExpr::FfiCallLib { handle, args, .. } => {
            operand_reads_slot(handle, slot)
                || args.iter().any(|operand| operand_reads_slot(operand, slot))
        }
        BytecodeExpr::FfiCallLibCStr {
            handle, arg_slot, ..
        } => operand_reads_slot(handle, slot) || *arg_slot == slot,
        BytecodeExpr::LenSpanI32 { source } | BytecodeExpr::LenBufU8 { source } => *source == slot,
        BytecodeExpr::StoreBufU8 {
            source,
            index,
            value,
        } => *source == slot || operand_reads_slot(index, slot) || operand_reads_slot(value, slot),
        BytecodeExpr::LoadSpanI32 { source, index } => {
            *source == slot || operand_reads_slot(index, slot)
        }
        BytecodeExpr::LoadBufU8 { source, index } => {
            *source == slot || operand_reads_slot(index, slot)
        }
        BytecodeExpr::Binary { left, right, .. } => {
            operand_reads_slot(left, slot) || operand_reads_slot(right, slot)
        }
        _ => false,
    }
}

fn operand_reads_slot(operand: &BytecodeOperand, slot: usize) -> bool {
    matches!(operand, BytecodeOperand::Slot { index, .. } if *index == slot)
}

fn same_home(left: SlotLayout, right: SlotLayout) -> bool {
    match (left.home, right.home) {
        (SlotHome::Reg(left), SlotHome::Reg(right)) => left == right,
        (SlotHome::Stack(left), SlotHome::Stack(right)) => left == right,
        _ => false,
    }
}

fn detect_loop_blocks(function: &BytecodeFunction) -> Vec<bool> {
    let mut flags = vec![false; function.blocks.len()];
    for (block_index, block) in function.blocks.iter().enumerate() {
        for target in terminator_targets(&block.terminator) {
            if target <= block_index {
                for index in target..=block_index {
                    flags[index] = true;
                }
            }
        }
    }
    flags
}

fn terminator_targets(terminator: &BytecodeTerminator) -> Vec<usize> {
    match terminator {
        BytecodeTerminator::Return(_) => Vec::new(),
        BytecodeTerminator::Jump(edge) => vec![edge.target],
        BytecodeTerminator::Branch { truthy, falsy, .. } => vec![truthy.target, falsy.target],
        BytecodeTerminator::Match { cases, default, .. } => {
            let mut targets = cases
                .iter()
                .map(|case| case.edge.target)
                .collect::<Vec<_>>();
            targets.push(default.target);
            targets
        }
    }
}

fn align_up(value: usize, alignment: usize) -> usize {
    if value % alignment == 0 {
        value
    } else {
        value + (alignment - (value % alignment))
    }
}
