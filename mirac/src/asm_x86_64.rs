use std::collections::BTreeMap;

use crate::codegen_c::LoweredExecBinaryOp;
use crate::lowered_bytecode::{BytecodeImmediate, BytecodeProgram, BytecodeValueKind};
use crate::machine_ir::{
    lower_bytecode_to_machine_program, validate_machine_program, MachineBlock as BytecodeBlock,
    MachineEdge as BytecodeEdge, MachineExpr as BytecodeExpr, MachineFunction as BytecodeFunction,
    MachineInstruction as BytecodeInstruction, MachineMatchCase as BytecodeMatchCase,
    MachineOperand as BytecodeOperand, MachineProgram, MachineTerminator as BytecodeTerminator,
};

const SYSV_REG_POOL: [&str; 5] = ["%r12", "%r13", "%r14", "%r15", "%rbx"];
const WIN64_REG_POOL: [&str; 5] = ["%r12", "%r13", "%r14", "%r15", "%rbx"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum X86_64ObjectFlavor {
    MachO,
    Elf,
    Coff,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum X86_64Abi {
    SysV,
    Win64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct X86_64Target {
    pub flavor: X86_64ObjectFlavor,
    pub abi: X86_64Abi,
}

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

#[allow(dead_code)]
pub fn supports_x86_64_asm_backend() -> bool {
    cfg!(target_arch = "x86_64")
}

pub fn target_from_triple(triple: &str) -> Result<X86_64Target, String> {
    if !triple.starts_with("x86_64-") {
        return Err(format!(
            "x86_64 asm backend only supports x86_64 triples, got {triple}"
        ));
    }
    if triple.contains("windows") {
        Ok(X86_64Target {
            flavor: X86_64ObjectFlavor::Coff,
            abi: X86_64Abi::Win64,
        })
    } else if triple.contains("apple") {
        Ok(X86_64Target {
            flavor: X86_64ObjectFlavor::MachO,
            abi: X86_64Abi::SysV,
        })
    } else {
        Ok(X86_64Target {
            flavor: X86_64ObjectFlavor::Elf,
            abi: X86_64Abi::SysV,
        })
    }
}

pub fn emit_x86_64_library(
    program: &BytecodeProgram,
    target: X86_64Target,
) -> Result<String, String> {
    let machine = lower_bytecode_to_machine_program(program);
    validate_machine_program(&machine)?;
    emit_x86_64_machine_library(&machine, target)
}

fn emit_x86_64_machine_library(
    program: &MachineProgram,
    target: X86_64Target,
) -> Result<String, String> {
    let mut out = String::new();
    out.push_str(".text\n");
    out.push_str(".p2align 4\n\n");
    for function in &program.functions {
        let strings = collect_runtime_strings(function, target.flavor);
        out.push_str(&emit_x86_64_function(function, target, &strings)?);
        emit_x86_64_string_section(&mut out, target.flavor, &strings);
        out.push('\n');
    }
    Ok(out)
}

fn emit_x86_64_function(
    function: &BytecodeFunction,
    target: X86_64Target,
    strings: &RuntimeStrings,
) -> Result<String, String> {
    let frame = build_frame_layout(function, target);
    let symbol = exported_symbol(function, target);
    let mut out = String::new();
    out.push_str(&format!(".globl {symbol}\n"));
    out.push_str(&format!("{symbol}:\n"));
    out.push_str("  pushq %rbp\n");
    out.push_str("  movq %rsp, %rbp\n");
    if frame.frame_size > 0 {
        out.push_str(&format!("  subq ${}, %rsp\n", frame.frame_size));
    }
    emit_save_regs(&mut out, &frame);
    emit_init_args(&mut out, function, &frame, target)?;
    emit_init_rand_state(&mut out, &frame, function)?;
    if function.entry_block != 0 {
        out.push_str(&format!(
            "  jmp {}\n",
            block_label(function, target.flavor, function.entry_block)
        ));
    }
    for (index, block) in function.blocks.iter().enumerate() {
        out.push_str(&format!(
            "{}:\n",
            block_label(function, target.flavor, index)
        ));
        emit_block(&mut out, function, block, index, &frame, target, strings)?;
    }
    Ok(out)
}

fn emit_save_regs(out: &mut String, frame: &FrameLayout) {
    for (index, reg) in frame.saved_regs.iter().enumerate() {
        out.push_str(&format!("  movq {reg}, {}(%rsp)\n", index * 8));
    }
}

fn emit_restore_regs(out: &mut String, frame: &FrameLayout) {
    for (index, reg) in frame.saved_regs.iter().enumerate().rev() {
        out.push_str(&format!("  movq {}(%rsp), {reg}\n", index * 8));
    }
}

fn emit_init_args(
    out: &mut String,
    function: &BytecodeFunction,
    frame: &FrameLayout,
    target: X86_64Target,
) -> Result<(), String> {
    let arg_regs = abi_arg_regs(target.abi);
    let mut next_arg_reg = 0usize;
    for arg in &function.arg_slots {
        let dst = frame.slot(arg.slot)?;
        match arg.kind {
            BytecodeValueKind::SpanI32 => {
                let ptr = arg_regs.get(next_arg_reg).ok_or_else(|| {
                    format!("x86_64 backend does not support arg register {next_arg_reg}")
                })?;
                let len = arg_regs.get(next_arg_reg + 1).ok_or_else(|| {
                    format!(
                        "x86_64 backend does not support arg register {}",
                        next_arg_reg + 1
                    )
                })?;
                emit_store_span_regs(out, dst, ptr, len)?;
                next_arg_reg += 2;
            }
            _ => {
                let reg = arg_regs.get(next_arg_reg).ok_or_else(|| {
                    format!("x86_64 backend does not support arg register {next_arg_reg}")
                })?;
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
    out.push_str(&format!("  movl ${seed}, %eax\n"));
    out.push_str(&format!("  movl %eax, {}(%rsp)\n", offset));
    Ok(())
}

fn emit_block(
    out: &mut String,
    function: &BytecodeFunction,
    block: &BytecodeBlock,
    block_index: usize,
    frame: &FrameLayout,
    target: X86_64Target,
    strings: &RuntimeStrings,
) -> Result<(), String> {
    for instruction in &block.instructions {
        emit_instruction(out, frame, target, strings, instruction)?;
    }
    emit_terminator(
        out,
        function,
        block_index,
        frame,
        target,
        strings,
        &block.terminator,
    )
}

fn emit_instruction(
    out: &mut String,
    frame: &FrameLayout,
    target: X86_64Target,
    strings: &RuntimeStrings,
    instruction: &BytecodeInstruction,
) -> Result<(), String> {
    emit_expr_to_dst(
        out,
        frame,
        target,
        strings,
        frame.slot(instruction.dst)?,
        &instruction.expr,
    )
}

fn emit_expr_to_dst(
    out: &mut String,
    frame: &FrameLayout,
    target: X86_64Target,
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
            Err("x86_64 backend does not yet support buf[u8] filesystem/runtime ops".to_string())
        }
        BytecodeExpr::ClockNowNs => emit_clock_call(out, target, dst),
        BytecodeExpr::RandU32 => emit_rand_call(out, frame, target, dst),
        BytecodeExpr::DropBufU8 { value } => emit_drop_buf_call(out, frame, target, dst, value),
        BytecodeExpr::RtOpen { workers } => emit_rt_open_call(out, frame, target, dst, workers),
        BytecodeExpr::RtSpawnU32 {
            runtime,
            function,
            arg,
        } => emit_rt_spawn_u32_call(out, frame, target, dst, strings, runtime, function, arg),
        BytecodeExpr::RtDone { task } => emit_rt_done_call(out, frame, target, dst, task),
        BytecodeExpr::RtJoinU32 { task } => emit_rt_join_u32_call(out, frame, target, dst, task),
        BytecodeExpr::RtCancel { task } => emit_rt_cancel_call(out, frame, target, dst, task),
        BytecodeExpr::RtTaskClose { task } => {
            emit_rt_task_close_call(out, frame, target, dst, task)
        }
        BytecodeExpr::RtShutdown { runtime, grace_ms } => {
            emit_rt_shutdown_call(out, frame, target, dst, runtime, grace_ms)
        }
        BytecodeExpr::RtClose { runtime } => emit_rt_close_call(out, frame, target, dst, runtime),
        BytecodeExpr::RtCancelled => emit_rt_cancelled_call(out, target, dst),
        BytecodeExpr::ChanOpenU32 { capacity } => {
            emit_chan_open_u32_call(out, frame, target, dst, capacity)
        }
        BytecodeExpr::ChanSendU32 { channel, value } => {
            emit_chan_send_u32_call(out, frame, target, dst, channel, value)
        }
        BytecodeExpr::ChanRecvU32 { channel } => {
            emit_chan_recv_u32_call(out, frame, target, dst, channel)
        }
        BytecodeExpr::ChanClose { channel } => {
            emit_chan_close_call(out, frame, target, dst, channel)
        }
        BytecodeExpr::FsReadU32 { path } => emit_fs_read_call(out, target, dst, strings, path),
        BytecodeExpr::FsWriteU32 { path, value } => {
            emit_fs_write_call(out, frame, target, dst, strings, path, value)
        }
        BytecodeExpr::BufLit { literal } => emit_buf_lit_call(out, target, dst, strings, literal),
        BytecodeExpr::TlsServerConfigU32 { value } => {
            emit_load_immediate(out, "%rax", &BytecodeImmediate::U32(*value))?;
            emit_store_scalar_reg(out, dst, "%rax")
        }
        BytecodeExpr::TlsServerConfigBuf { value } => {
            emit_buf_lit_call(out, target, dst, strings, value)
        }
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
            target,
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
            emit_http_session_accept_call(out, frame, target, dst, listener)
        }
        BytecodeExpr::ListenerSetTimeoutMs { handle, value } => {
            emit_listener_set_timeout_call(out, frame, target, dst, handle, value)
        }
        BytecodeExpr::SessionSetTimeoutMs { handle, value } => {
            emit_session_set_timeout_call(out, frame, target, dst, handle, value)
        }
        BytecodeExpr::ListenerSetShutdownGraceMs { handle, value } => {
            emit_listener_set_shutdown_grace_call(out, frame, target, dst, handle, value)
        }
        BytecodeExpr::HttpSessionRequest { handle } => {
            emit_http_session_request_call(out, frame, target, dst, handle)
        }
        BytecodeExpr::NetClose { handle } => emit_net_close_call(out, frame, target, dst, handle),
        BytecodeExpr::HttpSessionClose { handle } => {
            emit_http_session_close_call(out, frame, target, dst, handle)
        }
        BytecodeExpr::HttpBody { request } => emit_http_body_call(out, frame, target, dst, request),
        BytecodeExpr::JsonGetU32 { value, key } => {
            emit_json_get_u32_call(out, frame, target, dst, strings, value, key)
        }
        BytecodeExpr::DbOpen { path } => emit_db_open_call(out, target, dst, strings, path),
        BytecodeExpr::DbClose { handle } => emit_db_close_call(out, frame, target, dst, handle),
        BytecodeExpr::DbQueryU32 { handle, sql } => {
            emit_db_query_u32_call(out, frame, target, dst, handle, sql)
        }
        BytecodeExpr::ServiceOpen { name } => emit_service_open_call(out, target, dst, strings, name),
        BytecodeExpr::ServiceClose { handle } => {
            emit_service_close_call(out, frame, target, dst, handle)
        }
        BytecodeExpr::ServiceShutdown { handle, grace_ms } => {
            emit_service_shutdown_call(out, frame, target, dst, handle, grace_ms)
        }
        BytecodeExpr::ServiceLog { handle, message } => {
            emit_service_log_call(out, frame, target, dst, strings, handle, message)
        }
        BytecodeExpr::ServiceTraceBegin { handle, name } => {
            emit_service_trace_begin_call(out, frame, target, dst, strings, handle, name)
        }
        BytecodeExpr::ServiceTraceEnd { trace } => {
            emit_service_trace_end_call(out, frame, target, dst, trace)
        }
        BytecodeExpr::ServiceMetricCount { handle, value } => {
            emit_service_metric_count_call(out, frame, target, dst, strings, handle, value)
        }
        BytecodeExpr::ServiceHealthStatus { handle } => {
            emit_service_health_status_call(out, frame, target, dst, handle)
        }
        BytecodeExpr::ServiceReadinessStatus { handle } => {
            emit_service_readiness_status_call(out, frame, target, dst, handle)
        }
        BytecodeExpr::ServiceMigrateDb { handle, db_handle } => {
            emit_service_migrate_db_call(out, frame, target, dst, strings, handle, db_handle)
        }
        BytecodeExpr::ServiceRoute {
            request,
            method,
            path,
        } => emit_service_route_call(out, frame, target, dst, strings, request, method, path),
        BytecodeExpr::ServiceRequireHeader {
            request,
            name,
            value,
        } => emit_service_require_header_call(out, frame, target, dst, strings, request, name, value),
        BytecodeExpr::ServiceErrorStatus { kind } => {
            emit_service_error_status_call(out, target, dst, strings, kind)
        }
        BytecodeExpr::HttpSessionWriteText {
            handle,
            status,
            body,
        } => emit_http_session_write_text_call(out, frame, target, dst, handle, status, body),
        BytecodeExpr::TaskSleepMs { value } => emit_task_sleep_call(out, frame, target, dst, value),
        BytecodeExpr::SpawnCall { command, .. } => {
            emit_spawn_call(out, target, dst, strings, command)
        }
        BytecodeExpr::NetConnect { host, port } => {
            emit_net_call(out, target, dst, strings, host, *port)
        }
        BytecodeExpr::FfiCall {
            symbol,
            args,
            ret_kind: _,
        } => emit_ffi_call(out, frame, target, dst, symbol, args),
        BytecodeExpr::FfiCallCStr { .. } => {
            Err("x86_64 backend does not yet support ffi_call_cstr".to_string())
        }
        BytecodeExpr::FfiOpenLib { .. }
        | BytecodeExpr::FfiCloseLib { .. }
        | BytecodeExpr::FfiBufPtr { .. }
        | BytecodeExpr::FfiCallLib { .. }
        | BytecodeExpr::FfiCallLibCStr { .. } => {
            Err("x86_64 backend does not yet support dynamic library ffi ops".to_string())
        }
        BytecodeExpr::LenSpanI32 { source } => {
            let src = frame.slot(*source)?;
            emit_load_span_len(out, src, "%eax")?;
            emit_store_scalar_reg(out, dst, "%rax")
        }
        BytecodeExpr::LoadSpanI32 { source, index } => {
            let src = frame.slot(*source)?;
            emit_load_span_ptr(out, src, "%r10")?;
            emit_load_scalar_operand(out, frame, index, "%r11")?;
            out.push_str("  movl (%r10,%r11,4), %eax\n");
            emit_store_scalar_reg(out, dst, "%rax")
        }
        BytecodeExpr::AbsI32 { value } => {
            emit_load_scalar_operand(out, frame, value, "%rax")?;
            out.push_str("  movl %eax, %r10d\n");
            out.push_str("  negl %r10d\n");
            out.push_str("  cmpl $0, %eax\n");
            out.push_str("  cmovl %r10d, %eax\n");
            emit_store_scalar_reg(out, dst, "%rax")
        }
        BytecodeExpr::Binary { op, left, right } => {
            emit_binary_expr(out, frame, dst, op, left, right)
        }
        BytecodeExpr::SextI64 { value } => {
            match value {
                BytecodeOperand::Slot { index, .. } => {
                    let src = frame.slot(*index)?;
                    emit_load_i32_to_i64(out, src, "%rax")?;
                }
                BytecodeOperand::Imm(BytecodeImmediate::I32(value)) => {
                    out.push_str(&format!("  movq ${}, %rax\n", *value as i64));
                }
                BytecodeOperand::Imm(BytecodeImmediate::U8(value)) => {
                    out.push_str(&format!("  movq ${}, %rax\n", *value as u64));
                }
                BytecodeOperand::Imm(BytecodeImmediate::U32(value)) => {
                    out.push_str(&format!("  movq ${}, %rax\n", *value as u64));
                }
                BytecodeOperand::Imm(BytecodeImmediate::Bool(value)) => {
                    out.push_str(&format!("  movq ${}, %rax\n", if *value { 1 } else { 0 }));
                }
                BytecodeOperand::Imm(BytecodeImmediate::I64(value)) => {
                    emit_load_immediate(out, "%rax", &BytecodeImmediate::I64(*value))?;
                }
                BytecodeOperand::Imm(BytecodeImmediate::U64(value)) => {
                    emit_load_immediate(out, "%rax", &BytecodeImmediate::U64(*value))?;
                }
            }
            emit_store_scalar_reg(out, dst, "%rax")
        }
        _ => Err("x86_64 backend does not yet support this bytecode expr".to_string()),
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
            emit_load_scalar_operand(out, frame, operand, "%rax")?;
            emit_store_scalar_reg(out, dst, "%rax")
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
            emit_load_scalar_operand(out, frame, left, "%rax")?;
            emit_load_scalar_operand(out, frame, right, "%r10")?;
            let mnemonic = match (op, dst.kind) {
                (LoweredExecBinaryOp::Add, BytecodeValueKind::I64 | BytecodeValueKind::U64) => {
                    "addq"
                }
                (LoweredExecBinaryOp::Sub, BytecodeValueKind::I64 | BytecodeValueKind::U64) => {
                    "subq"
                }
                (LoweredExecBinaryOp::Mul, BytecodeValueKind::I64 | BytecodeValueKind::U64) => {
                    "imulq"
                }
                (LoweredExecBinaryOp::Band, BytecodeValueKind::I64 | BytecodeValueKind::U64) => {
                    "andq"
                }
                (LoweredExecBinaryOp::Bor, BytecodeValueKind::I64 | BytecodeValueKind::U64) => {
                    "orq"
                }
                (LoweredExecBinaryOp::Bxor, BytecodeValueKind::I64 | BytecodeValueKind::U64) => {
                    "xorq"
                }
                (LoweredExecBinaryOp::Add, _) => "addl",
                (LoweredExecBinaryOp::Sub, _) => "subl",
                (LoweredExecBinaryOp::Mul, _) => "imull",
                (LoweredExecBinaryOp::Band, _) => "andl",
                (LoweredExecBinaryOp::Bor, _) => "orl",
                (LoweredExecBinaryOp::Bxor, _) => "xorl",
                _ => unreachable!(),
            };
            match dst.kind {
                BytecodeValueKind::I64 | BytecodeValueKind::U64 => {
                    out.push_str(&format!("  {mnemonic} %r10, %rax\n"))
                }
                BytecodeValueKind::I32
                | BytecodeValueKind::U32
                | BytecodeValueKind::U8
                | BytecodeValueKind::Bool => out.push_str(&format!("  {mnemonic} %r10d, %eax\n")),
                BytecodeValueKind::SpanI32 | BytecodeValueKind::BufU8 => {
                    return Err(
                        "x86_64 backend does not support binary ops on aggregate values"
                            .to_string(),
                    )
                }
            }
            emit_store_scalar_reg(out, dst, "%rax")
        }
        LoweredExecBinaryOp::Shl | LoweredExecBinaryOp::Shr => {
            let kind = operand_kind(left);
            emit_load_scalar_operand(out, frame, left, "%rax")?;
            emit_load_scalar_operand(out, frame, right, "%rcx")?;
            let mnemonic = match (op, kind) {
                (LoweredExecBinaryOp::Shl, BytecodeValueKind::I64 | BytecodeValueKind::U64) => {
                    "shlq"
                }
                (LoweredExecBinaryOp::Shl, BytecodeValueKind::I32)
                | (LoweredExecBinaryOp::Shl, BytecodeValueKind::U32)
                | (LoweredExecBinaryOp::Shl, BytecodeValueKind::U8)
                | (LoweredExecBinaryOp::Shl, BytecodeValueKind::Bool) => "shll",
                (LoweredExecBinaryOp::Shr, BytecodeValueKind::I64) => "sarq",
                (LoweredExecBinaryOp::Shr, BytecodeValueKind::U64) => "shrq",
                (LoweredExecBinaryOp::Shr, BytecodeValueKind::I32) => "sarl",
                (LoweredExecBinaryOp::Shr, BytecodeValueKind::U32)
                | (LoweredExecBinaryOp::Shr, BytecodeValueKind::U8)
                | (LoweredExecBinaryOp::Shr, BytecodeValueKind::Bool) => "shrl",
                (_, BytecodeValueKind::SpanI32 | BytecodeValueKind::BufU8) => {
                    return Err("x86_64 backend does not support aggregate shifts".to_string())
                }
                _ => unreachable!(),
            };
            match kind {
                BytecodeValueKind::I64 | BytecodeValueKind::U64 => {
                    out.push_str(&format!("  {mnemonic} %cl, %rax\n"))
                }
                BytecodeValueKind::I32
                | BytecodeValueKind::U32
                | BytecodeValueKind::U8
                | BytecodeValueKind::Bool => out.push_str(&format!("  {mnemonic} %cl, %eax\n")),
                BytecodeValueKind::SpanI32 | BytecodeValueKind::BufU8 => unreachable!(),
            }
            emit_store_scalar_reg(out, dst, "%rax")
        }
        LoweredExecBinaryOp::Eq | LoweredExecBinaryOp::Lt | LoweredExecBinaryOp::Le => {
            let kind = operand_kind(left);
            emit_load_scalar_operand(out, frame, left, "%rax")?;
            emit_load_scalar_operand(out, frame, right, "%r10")?;
            match kind {
                BytecodeValueKind::I64 | BytecodeValueKind::U64 => {
                    out.push_str("  cmpq %r10, %rax\n")
                }
                BytecodeValueKind::I32
                | BytecodeValueKind::U32
                | BytecodeValueKind::U8
                | BytecodeValueKind::Bool => out.push_str("  cmpl %r10d, %eax\n"),
                BytecodeValueKind::SpanI32 | BytecodeValueKind::BufU8 => {
                    return Err("x86_64 backend does not support aggregate comparison".to_string())
                }
            }
            let condition = match op {
                LoweredExecBinaryOp::Eq => "e",
                LoweredExecBinaryOp::Lt => match kind {
                    BytecodeValueKind::U64
                    | BytecodeValueKind::U32
                    | BytecodeValueKind::U8
                    | BytecodeValueKind::Bool => "b",
                    _ => "l",
                },
                LoweredExecBinaryOp::Le => match kind {
                    BytecodeValueKind::U64 | BytecodeValueKind::U32 | BytecodeValueKind::Bool => {
                        "be"
                    }
                    _ => "le",
                },
                _ => unreachable!(),
            };
            out.push_str(&format!("  set{condition} %al\n"));
            out.push_str("  movzbl %al, %eax\n");
            emit_store_scalar_reg(out, dst, "%rax")
        }
    }
}

fn emit_clock_call(out: &mut String, target: X86_64Target, dst: SlotLayout) -> Result<(), String> {
    emit_runtime_call(
        out,
        target,
        &extern_symbol("mira_rt_clock_now_ns", target.flavor),
    );
    emit_store_scalar_reg(out, dst, "%rax")
}

fn emit_rand_call(
    out: &mut String,
    frame: &FrameLayout,
    target: X86_64Target,
    dst: SlotLayout,
) -> Result<(), String> {
    let offset = frame
        .rand_state_offset
        .ok_or_else(|| "x86_64 rand call missing rand state slot".to_string())?;
    emit_load_stack_address(out, abi_arg_regs(target.abi)[0], offset);
    emit_runtime_call(
        out,
        target,
        &extern_symbol("mira_rt_rand_next_u32", target.flavor),
    );
    emit_store_scalar_reg(out, dst, "%rax")
}

fn emit_rt_open_call(
    out: &mut String,
    frame: &FrameLayout,
    target: X86_64Target,
    dst: SlotLayout,
    workers: &BytecodeOperand,
) -> Result<(), String> {
    emit_load_scalar_operand(out, frame, workers, abi_arg_regs(target.abi)[0])?;
    emit_runtime_call(
        out,
        target,
        &extern_symbol("mira_rt_open_handle", target.flavor),
    );
    emit_store_scalar_reg(out, dst, "%rax")
}

fn emit_rt_spawn_u32_call(
    out: &mut String,
    frame: &FrameLayout,
    target: X86_64Target,
    dst: SlotLayout,
    strings: &RuntimeStrings,
    runtime: &BytecodeOperand,
    function: &str,
    arg: &BytecodeOperand,
) -> Result<(), String> {
    let arg_regs = abi_arg_regs(target.abi);
    emit_load_scalar_operand(out, frame, runtime, arg_regs[0])?;
    emit_load_runtime_string(out, arg_regs[1], strings, function);
    emit_load_scalar_operand(out, frame, arg, arg_regs[2])?;
    emit_runtime_call(
        out,
        target,
        &extern_symbol("mira_rt_spawn_u32_handle", target.flavor),
    );
    emit_store_scalar_reg(out, dst, "%rax")
}

fn emit_rt_done_call(
    out: &mut String,
    frame: &FrameLayout,
    target: X86_64Target,
    dst: SlotLayout,
    task: &BytecodeOperand,
) -> Result<(), String> {
    emit_load_scalar_operand(out, frame, task, abi_arg_regs(target.abi)[0])?;
    emit_runtime_call(
        out,
        target,
        &extern_symbol("mira_rt_done_handle", target.flavor),
    );
    emit_store_scalar_reg(out, dst, "%rax")
}

fn emit_rt_join_u32_call(
    out: &mut String,
    frame: &FrameLayout,
    target: X86_64Target,
    dst: SlotLayout,
    task: &BytecodeOperand,
) -> Result<(), String> {
    emit_load_scalar_operand(out, frame, task, abi_arg_regs(target.abi)[0])?;
    emit_runtime_call(
        out,
        target,
        &extern_symbol("mira_rt_join_u32_handle", target.flavor),
    );
    emit_store_scalar_reg(out, dst, "%rax")
}

fn emit_rt_cancel_call(
    out: &mut String,
    frame: &FrameLayout,
    target: X86_64Target,
    dst: SlotLayout,
    task: &BytecodeOperand,
) -> Result<(), String> {
    emit_load_scalar_operand(out, frame, task, abi_arg_regs(target.abi)[0])?;
    emit_runtime_call(
        out,
        target,
        &extern_symbol("mira_rt_cancel_handle", target.flavor),
    );
    emit_store_scalar_reg(out, dst, "%rax")
}

fn emit_rt_task_close_call(
    out: &mut String,
    frame: &FrameLayout,
    target: X86_64Target,
    dst: SlotLayout,
    task: &BytecodeOperand,
) -> Result<(), String> {
    emit_load_scalar_operand(out, frame, task, abi_arg_regs(target.abi)[0])?;
    emit_runtime_call(
        out,
        target,
        &extern_symbol("mira_rt_task_close_handle", target.flavor),
    );
    emit_store_scalar_reg(out, dst, "%rax")
}

fn emit_rt_shutdown_call(
    out: &mut String,
    frame: &FrameLayout,
    target: X86_64Target,
    dst: SlotLayout,
    runtime: &BytecodeOperand,
    grace_ms: &BytecodeOperand,
) -> Result<(), String> {
    let arg_regs = abi_arg_regs(target.abi);
    emit_load_scalar_operand(out, frame, runtime, arg_regs[0])?;
    emit_load_scalar_operand(out, frame, grace_ms, arg_regs[1])?;
    emit_runtime_call(
        out,
        target,
        &extern_symbol("mira_rt_shutdown_handle", target.flavor),
    );
    emit_store_scalar_reg(out, dst, "%rax")
}

fn emit_rt_close_call(
    out: &mut String,
    frame: &FrameLayout,
    target: X86_64Target,
    dst: SlotLayout,
    runtime: &BytecodeOperand,
) -> Result<(), String> {
    emit_load_scalar_operand(out, frame, runtime, abi_arg_regs(target.abi)[0])?;
    emit_runtime_call(
        out,
        target,
        &extern_symbol("mira_rt_close_handle", target.flavor),
    );
    emit_store_scalar_reg(out, dst, "%rax")
}

fn emit_rt_cancelled_call(
    out: &mut String,
    target: X86_64Target,
    dst: SlotLayout,
) -> Result<(), String> {
    emit_runtime_call(
        out,
        target,
        &extern_symbol("mira_rt_cancelled", target.flavor),
    );
    emit_store_scalar_reg(out, dst, "%rax")
}

fn emit_chan_open_u32_call(
    out: &mut String,
    frame: &FrameLayout,
    target: X86_64Target,
    dst: SlotLayout,
    capacity: &BytecodeOperand,
) -> Result<(), String> {
    emit_load_scalar_operand(out, frame, capacity, abi_arg_regs(target.abi)[0])?;
    emit_runtime_call(
        out,
        target,
        &extern_symbol("mira_chan_open_u32_handle", target.flavor),
    );
    emit_store_scalar_reg(out, dst, "%rax")
}

fn emit_chan_send_u32_call(
    out: &mut String,
    frame: &FrameLayout,
    target: X86_64Target,
    dst: SlotLayout,
    channel: &BytecodeOperand,
    value: &BytecodeOperand,
) -> Result<(), String> {
    let arg_regs = abi_arg_regs(target.abi);
    emit_load_scalar_operand(out, frame, channel, arg_regs[0])?;
    emit_load_scalar_operand(out, frame, value, arg_regs[1])?;
    emit_runtime_call(
        out,
        target,
        &extern_symbol("mira_chan_send_u32_handle", target.flavor),
    );
    emit_store_scalar_reg(out, dst, "%rax")
}

fn emit_chan_recv_u32_call(
    out: &mut String,
    frame: &FrameLayout,
    target: X86_64Target,
    dst: SlotLayout,
    channel: &BytecodeOperand,
) -> Result<(), String> {
    emit_load_scalar_operand(out, frame, channel, abi_arg_regs(target.abi)[0])?;
    emit_runtime_call(
        out,
        target,
        &extern_symbol("mira_chan_recv_u32_handle", target.flavor),
    );
    emit_store_scalar_reg(out, dst, "%rax")
}

fn emit_chan_close_call(
    out: &mut String,
    frame: &FrameLayout,
    target: X86_64Target,
    dst: SlotLayout,
    channel: &BytecodeOperand,
) -> Result<(), String> {
    emit_load_scalar_operand(out, frame, channel, abi_arg_regs(target.abi)[0])?;
    emit_runtime_call(
        out,
        target,
        &extern_symbol("mira_chan_close_handle", target.flavor),
    );
    emit_store_scalar_reg(out, dst, "%rax")
}

fn emit_task_sleep_call(
    out: &mut String,
    frame: &FrameLayout,
    target: X86_64Target,
    dst: SlotLayout,
    value: &BytecodeOperand,
) -> Result<(), String> {
    let arg_reg = abi_arg_regs(target.abi)[0];
    emit_load_scalar_operand(out, frame, value, arg_reg)?;
    out.push_str(&format!("  imulq $1000, {}, {}\n", arg_reg, arg_reg));
    emit_runtime_call(out, target, &extern_symbol("usleep", target.flavor));
    out.push_str("  cmpl $0, %eax\n");
    out.push_str("  sete %al\n");
    out.push_str("  movzbl %al, %eax\n");
    emit_store_scalar_reg(out, dst, "%rax")
}

fn emit_fs_read_call(
    out: &mut String,
    target: X86_64Target,
    dst: SlotLayout,
    strings: &RuntimeStrings,
    path: &str,
) -> Result<(), String> {
    emit_load_runtime_string(out, abi_arg_regs(target.abi)[0], strings, path);
    emit_runtime_call(
        out,
        target,
        &extern_symbol("mira_rt_fs_read_u32", target.flavor),
    );
    emit_store_scalar_reg(out, dst, "%rax")
}

fn emit_fs_write_call(
    out: &mut String,
    frame: &FrameLayout,
    target: X86_64Target,
    dst: SlotLayout,
    strings: &RuntimeStrings,
    path: &str,
    value: &BytecodeOperand,
) -> Result<(), String> {
    let arg_regs = abi_arg_regs(target.abi);
    emit_load_runtime_string(out, arg_regs[0], strings, path);
    emit_load_scalar_operand(out, frame, value, arg_regs[1])?;
    emit_runtime_call(
        out,
        target,
        &extern_symbol("mira_rt_fs_write_u32", target.flavor),
    );
    emit_store_scalar_reg(out, dst, "%rax")
}

fn ensure_sysv_buf_runtime(target: X86_64Target, feature: &str) -> Result<(), String> {
    if target.abi != X86_64Abi::SysV {
        return Err(format!(
            "x86_64 backend currently supports {feature} only on SysV runtime targets"
        ));
    }
    Ok(())
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
    emit_load_span_len(out, src, reg32(len_reg)?)
}

fn emit_buf_return(out: &mut String, dst: SlotLayout) -> Result<(), String> {
    emit_store_span_regs(out, dst, "%rax", "%rdx")
}

fn emit_buf_out_address(out: &mut String, dst: SlotLayout, reg: &str) -> Result<(), String> {
    match dst.home {
        SlotHome::Stack(offset) => {
            emit_load_stack_address(out, reg, offset);
            Ok(())
        }
        SlotHome::Reg(_) => Err("x86_64 backend does not keep buf values in registers".to_string()),
    }
}

fn emit_buf_lit_call(
    out: &mut String,
    target: X86_64Target,
    dst: SlotLayout,
    strings: &RuntimeStrings,
    literal: &str,
) -> Result<(), String> {
    if target.abi == X86_64Abi::Win64 {
        emit_load_runtime_string(out, "%rcx", strings, literal);
        emit_buf_out_address(out, dst, "%rdx")?;
        emit_runtime_call(out, target, &extern_symbol("mira_buf_lit_u8_out", target.flavor));
        Ok(())
    } else {
        emit_load_runtime_string(out, abi_arg_regs(target.abi)[0], strings, literal);
        emit_runtime_call(out, target, &extern_symbol("mira_buf_lit_u8", target.flavor));
        emit_buf_return(out, dst)
    }
}

fn emit_drop_buf_call(
    out: &mut String,
    frame: &FrameLayout,
    target: X86_64Target,
    dst: SlotLayout,
    value: &BytecodeOperand,
) -> Result<(), String> {
    let arg_regs = abi_arg_regs(target.abi);
    emit_load_buf_operand(out, frame, value, arg_regs[0], arg_regs[1])?;
    let symbol = if target.abi == X86_64Abi::Win64 {
        "mira_drop_buf_u8_parts"
    } else {
        "mira_drop_buf_u8"
    };
    emit_runtime_call(out, target, &extern_symbol(symbol, target.flavor));
    emit_store_scalar_reg(out, dst, "%rax")
}

fn emit_tls_listen_call(
    out: &mut String,
    target: X86_64Target,
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
    let arg_regs = abi_arg_regs(target.abi);
    emit_load_runtime_string(out, arg_regs[0], strings, host);
    out.push_str(&format!("  movl ${}, {}\n", port, reg32(arg_regs[1])?));
    emit_load_runtime_string(out, arg_regs[2], strings, cert);
    emit_load_runtime_string(out, arg_regs[3], strings, key);
    out.push_str(&format!(
        "  movl ${}, {}\n",
        request_timeout_ms,
        reg32(arg_regs[4])?
    ));
    out.push_str(&format!(
        "  movl ${}, {}\n",
        session_timeout_ms,
        reg32(arg_regs[5])?
    ));
    if target.abi == X86_64Abi::SysV {
        out.push_str(&format!("  movl ${}, %r9d\n", shutdown_grace_ms));
    } else {
        return Err("x86_64 backend currently supports tls_listen only on SysV targets".to_string());
    }
    emit_runtime_call(out, target, &extern_symbol("mira_tls_listen_handle", target.flavor));
    emit_store_scalar_reg(out, dst, "%rax")
}

fn emit_http_session_accept_call(
    out: &mut String,
    frame: &FrameLayout,
    target: X86_64Target,
    dst: SlotLayout,
    listener: &BytecodeOperand,
) -> Result<(), String> {
    emit_load_scalar_operand(out, frame, listener, abi_arg_regs(target.abi)[0])?;
    emit_runtime_call(
        out,
        target,
        &extern_symbol("mira_http_session_accept_handle", target.flavor),
    );
    emit_store_scalar_reg(out, dst, "%rax")
}

fn emit_listener_set_timeout_call(
    out: &mut String,
    frame: &FrameLayout,
    target: X86_64Target,
    dst: SlotLayout,
    handle: &BytecodeOperand,
    value: &BytecodeOperand,
) -> Result<(), String> {
    let arg_regs = abi_arg_regs(target.abi);
    emit_load_scalar_operand(out, frame, handle, arg_regs[0])?;
    emit_load_scalar_operand(out, frame, value, arg_regs[1])?;
    emit_runtime_call(
        out,
        target,
        &extern_symbol("mira_listener_set_timeout_ms", target.flavor),
    );
    emit_store_scalar_reg(out, dst, "%rax")
}

fn emit_session_set_timeout_call(
    out: &mut String,
    frame: &FrameLayout,
    target: X86_64Target,
    dst: SlotLayout,
    handle: &BytecodeOperand,
    value: &BytecodeOperand,
) -> Result<(), String> {
    let arg_regs = abi_arg_regs(target.abi);
    emit_load_scalar_operand(out, frame, handle, arg_regs[0])?;
    emit_load_scalar_operand(out, frame, value, arg_regs[1])?;
    emit_runtime_call(
        out,
        target,
        &extern_symbol("mira_session_set_timeout_ms", target.flavor),
    );
    emit_store_scalar_reg(out, dst, "%rax")
}

fn emit_listener_set_shutdown_grace_call(
    out: &mut String,
    frame: &FrameLayout,
    target: X86_64Target,
    dst: SlotLayout,
    handle: &BytecodeOperand,
    value: &BytecodeOperand,
) -> Result<(), String> {
    let arg_regs = abi_arg_regs(target.abi);
    emit_load_scalar_operand(out, frame, handle, arg_regs[0])?;
    emit_load_scalar_operand(out, frame, value, arg_regs[1])?;
    emit_runtime_call(
        out,
        target,
        &extern_symbol("mira_listener_set_shutdown_grace_ms", target.flavor),
    );
    emit_store_scalar_reg(out, dst, "%rax")
}

fn emit_http_session_request_call(
    out: &mut String,
    frame: &FrameLayout,
    target: X86_64Target,
    dst: SlotLayout,
    handle: &BytecodeOperand,
) -> Result<(), String> {
    ensure_sysv_buf_runtime(target, "http session request")?;
    emit_load_scalar_operand(out, frame, handle, abi_arg_regs(target.abi)[0])?;
    emit_runtime_call(
        out,
        target,
        &extern_symbol("mira_http_session_request_buf_u8", target.flavor),
    );
    emit_buf_return(out, dst)
}

fn emit_http_session_write_text_call(
    out: &mut String,
    frame: &FrameLayout,
    target: X86_64Target,
    dst: SlotLayout,
    handle: &BytecodeOperand,
    status: &BytecodeOperand,
    body: &BytecodeOperand,
) -> Result<(), String> {
    ensure_sysv_buf_runtime(target, "http session write text")?;
    let arg_regs = abi_arg_regs(target.abi);
    emit_load_scalar_operand(out, frame, handle, arg_regs[0])?;
    emit_load_scalar_operand(out, frame, status, arg_regs[1])?;
    emit_load_buf_operand(out, frame, body, arg_regs[2], arg_regs[3])?;
    emit_runtime_call(
        out,
        target,
        &extern_symbol("mira_http_session_write_text_handle", target.flavor),
    );
    emit_store_scalar_reg(out, dst, "%rax")
}

fn emit_net_close_call(
    out: &mut String,
    frame: &FrameLayout,
    target: X86_64Target,
    dst: SlotLayout,
    handle: &BytecodeOperand,
) -> Result<(), String> {
    emit_load_scalar_operand(out, frame, handle, abi_arg_regs(target.abi)[0])?;
    emit_runtime_call(out, target, &extern_symbol("mira_net_close_handle", target.flavor));
    emit_store_scalar_reg(out, dst, "%rax")
}

fn emit_http_session_close_call(
    out: &mut String,
    frame: &FrameLayout,
    target: X86_64Target,
    dst: SlotLayout,
    handle: &BytecodeOperand,
) -> Result<(), String> {
    emit_load_scalar_operand(out, frame, handle, abi_arg_regs(target.abi)[0])?;
    emit_runtime_call(
        out,
        target,
        &extern_symbol("mira_http_session_close_handle", target.flavor),
    );
    emit_store_scalar_reg(out, dst, "%rax")
}

fn emit_http_body_call(
    out: &mut String,
    frame: &FrameLayout,
    target: X86_64Target,
    dst: SlotLayout,
    request: &BytecodeOperand,
) -> Result<(), String> {
    if target.abi == X86_64Abi::Win64 {
        emit_load_buf_operand(out, frame, request, "%rcx", "%rdx")?;
        emit_buf_out_address(out, dst, "%r8")?;
        emit_runtime_call(
            out,
            target,
            &extern_symbol("mira_http_body_buf_u8_parts", target.flavor),
        );
        Ok(())
    } else {
        let arg_regs = abi_arg_regs(target.abi);
        emit_load_buf_operand(out, frame, request, arg_regs[0], arg_regs[1])?;
        emit_runtime_call(out, target, &extern_symbol("mira_http_body_buf_u8", target.flavor));
        emit_buf_return(out, dst)
    }
}

fn emit_json_get_u32_call(
    out: &mut String,
    frame: &FrameLayout,
    target: X86_64Target,
    dst: SlotLayout,
    strings: &RuntimeStrings,
    value: &BytecodeOperand,
    key: &str,
) -> Result<(), String> {
    if target.abi == X86_64Abi::Win64 {
        emit_load_buf_operand(out, frame, value, "%rcx", "%rdx")?;
        emit_load_runtime_string(out, "%r8", strings, key);
        emit_runtime_call(
            out,
            target,
            &extern_symbol("mira_json_get_u32_buf_u8_parts", target.flavor),
        );
    } else {
        let arg_regs = abi_arg_regs(target.abi);
        emit_load_buf_operand(out, frame, value, arg_regs[0], arg_regs[1])?;
        emit_load_runtime_string(out, arg_regs[2], strings, key);
        emit_runtime_call(
            out,
            target,
            &extern_symbol("mira_json_get_u32_buf_u8", target.flavor),
        );
    }
    emit_store_scalar_reg(out, dst, "%rax")
}

fn emit_db_open_call(
    out: &mut String,
    target: X86_64Target,
    dst: SlotLayout,
    strings: &RuntimeStrings,
    path: &str,
) -> Result<(), String> {
    emit_load_runtime_string(out, abi_arg_regs(target.abi)[0], strings, path);
    emit_runtime_call(out, target, &extern_symbol("mira_db_open_handle", target.flavor));
    emit_store_scalar_reg(out, dst, "%rax")
}

fn emit_db_close_call(
    out: &mut String,
    frame: &FrameLayout,
    target: X86_64Target,
    dst: SlotLayout,
    handle: &BytecodeOperand,
) -> Result<(), String> {
    emit_load_scalar_operand(out, frame, handle, abi_arg_regs(target.abi)[0])?;
    emit_runtime_call(out, target, &extern_symbol("mira_db_close_handle", target.flavor));
    emit_store_scalar_reg(out, dst, "%rax")
}

fn emit_db_query_u32_call(
    out: &mut String,
    frame: &FrameLayout,
    target: X86_64Target,
    dst: SlotLayout,
    handle: &BytecodeOperand,
    sql: &BytecodeOperand,
) -> Result<(), String> {
    ensure_sysv_buf_runtime(target, "db_query_u32")?;
    let arg_regs = abi_arg_regs(target.abi);
    emit_load_scalar_operand(out, frame, handle, arg_regs[0])?;
    emit_load_buf_operand(out, frame, sql, arg_regs[1], arg_regs[2])?;
    emit_runtime_call(
        out,
        target,
        &extern_symbol("mira_db_query_u32_handle_sql_buf_u8", target.flavor),
    );
    emit_store_scalar_reg(out, dst, "%rax")
}

fn emit_service_open_call(
    out: &mut String,
    target: X86_64Target,
    dst: SlotLayout,
    strings: &RuntimeStrings,
    name: &str,
) -> Result<(), String> {
    emit_load_runtime_string(out, abi_arg_regs(target.abi)[0], strings, name);
    emit_runtime_call(
        out,
        target,
        &extern_symbol("mira_service_open_handle", target.flavor),
    );
    emit_store_scalar_reg(out, dst, "%rax")
}

fn emit_service_close_call(
    out: &mut String,
    frame: &FrameLayout,
    target: X86_64Target,
    dst: SlotLayout,
    handle: &BytecodeOperand,
) -> Result<(), String> {
    emit_load_scalar_operand(out, frame, handle, abi_arg_regs(target.abi)[0])?;
    emit_runtime_call(
        out,
        target,
        &extern_symbol("mira_service_close_handle", target.flavor),
    );
    emit_store_scalar_reg(out, dst, "%rax")
}

fn emit_service_shutdown_call(
    out: &mut String,
    frame: &FrameLayout,
    target: X86_64Target,
    dst: SlotLayout,
    handle: &BytecodeOperand,
    grace_ms: &BytecodeOperand,
) -> Result<(), String> {
    let arg_regs = abi_arg_regs(target.abi);
    emit_load_scalar_operand(out, frame, handle, arg_regs[0])?;
    emit_load_scalar_operand(out, frame, grace_ms, arg_regs[1])?;
    emit_runtime_call(
        out,
        target,
        &extern_symbol("mira_service_shutdown_handle", target.flavor),
    );
    emit_store_scalar_reg(out, dst, "%rax")
}

fn emit_service_log_call(
    out: &mut String,
    frame: &FrameLayout,
    target: X86_64Target,
    dst: SlotLayout,
    strings: &RuntimeStrings,
    handle: &BytecodeOperand,
    message: &BytecodeOperand,
) -> Result<(), String> {
    ensure_sysv_buf_runtime(target, "service_log")?;
    let arg_regs = abi_arg_regs(target.abi);
    emit_load_scalar_operand(out, frame, handle, arg_regs[0])?;
    emit_load_runtime_string(out, arg_regs[1], strings, "info");
    emit_load_buf_operand(out, frame, message, arg_regs[2], arg_regs[3])?;
    emit_runtime_call(
        out,
        target,
        &extern_symbol("mira_service_log_buf_u8", target.flavor),
    );
    emit_store_scalar_reg(out, dst, "%rax")
}

fn emit_service_trace_begin_call(
    out: &mut String,
    frame: &FrameLayout,
    target: X86_64Target,
    dst: SlotLayout,
    strings: &RuntimeStrings,
    handle: &BytecodeOperand,
    name: &str,
) -> Result<(), String> {
    let arg_regs = abi_arg_regs(target.abi);
    emit_load_scalar_operand(out, frame, handle, arg_regs[0])?;
    emit_load_runtime_string(out, arg_regs[1], strings, name);
    emit_runtime_call(
        out,
        target,
        &extern_symbol("mira_service_trace_begin_handle", target.flavor),
    );
    emit_store_scalar_reg(out, dst, "%rax")
}

fn emit_service_trace_end_call(
    out: &mut String,
    frame: &FrameLayout,
    target: X86_64Target,
    dst: SlotLayout,
    trace: &BytecodeOperand,
) -> Result<(), String> {
    emit_load_scalar_operand(out, frame, trace, abi_arg_regs(target.abi)[0])?;
    emit_runtime_call(
        out,
        target,
        &extern_symbol("mira_service_trace_end_handle", target.flavor),
    );
    emit_store_scalar_reg(out, dst, "%rax")
}

fn emit_service_metric_count_call(
    out: &mut String,
    frame: &FrameLayout,
    target: X86_64Target,
    dst: SlotLayout,
    strings: &RuntimeStrings,
    handle: &BytecodeOperand,
    value: &BytecodeOperand,
) -> Result<(), String> {
    let arg_regs = abi_arg_regs(target.abi);
    emit_load_scalar_operand(out, frame, handle, arg_regs[0])?;
    emit_load_runtime_string(out, arg_regs[1], strings, "count");
    emit_load_scalar_operand(out, frame, value, arg_regs[2])?;
    emit_runtime_call(
        out,
        target,
        &extern_symbol("mira_service_metric_count_handle", target.flavor),
    );
    emit_store_scalar_reg(out, dst, "%rax")
}

fn emit_service_health_status_call(
    out: &mut String,
    frame: &FrameLayout,
    target: X86_64Target,
    dst: SlotLayout,
    handle: &BytecodeOperand,
) -> Result<(), String> {
    emit_load_scalar_operand(out, frame, handle, abi_arg_regs(target.abi)[0])?;
    emit_runtime_call(
        out,
        target,
        &extern_symbol("mira_service_health_status_handle", target.flavor),
    );
    emit_store_scalar_reg(out, dst, "%rax")
}

fn emit_service_readiness_status_call(
    out: &mut String,
    frame: &FrameLayout,
    target: X86_64Target,
    dst: SlotLayout,
    handle: &BytecodeOperand,
) -> Result<(), String> {
    emit_load_scalar_operand(out, frame, handle, abi_arg_regs(target.abi)[0])?;
    emit_runtime_call(
        out,
        target,
        &extern_symbol("mira_service_readiness_status_handle", target.flavor),
    );
    emit_store_scalar_reg(out, dst, "%rax")
}

fn emit_service_migrate_db_call(
    out: &mut String,
    frame: &FrameLayout,
    target: X86_64Target,
    dst: SlotLayout,
    strings: &RuntimeStrings,
    handle: &BytecodeOperand,
    db_handle: &BytecodeOperand,
) -> Result<(), String> {
    let arg_regs = abi_arg_regs(target.abi);
    emit_load_scalar_operand(out, frame, handle, arg_regs[0])?;
    emit_load_scalar_operand(out, frame, db_handle, arg_regs[1])?;
    emit_load_runtime_string(out, arg_regs[2], strings, "migration");
    emit_runtime_call(
        out,
        target,
        &extern_symbol("mira_service_migrate_db_handle", target.flavor),
    );
    emit_store_scalar_reg(out, dst, "%rax")
}

fn emit_service_route_call(
    out: &mut String,
    frame: &FrameLayout,
    target: X86_64Target,
    dst: SlotLayout,
    strings: &RuntimeStrings,
    request: &BytecodeOperand,
    method: &str,
    path: &str,
) -> Result<(), String> {
    if target.abi == X86_64Abi::Win64 {
        emit_load_buf_operand(out, frame, request, "%rcx", "%rdx")?;
        emit_load_runtime_string(out, "%r8", strings, method);
        emit_load_runtime_string(out, "%r9", strings, path);
        emit_runtime_call(
            out,
            target,
            &extern_symbol("mira_service_route_buf_u8_parts", target.flavor),
        );
    } else {
        let arg_regs = abi_arg_regs(target.abi);
        emit_load_buf_operand(out, frame, request, arg_regs[0], arg_regs[1])?;
        emit_load_runtime_string(out, arg_regs[2], strings, method);
        emit_load_runtime_string(out, arg_regs[3], strings, path);
        emit_runtime_call(
            out,
            target,
            &extern_symbol("mira_service_route_buf_u8", target.flavor),
        );
    }
    emit_store_scalar_reg(out, dst, "%rax")
}

fn emit_service_require_header_call(
    out: &mut String,
    frame: &FrameLayout,
    target: X86_64Target,
    dst: SlotLayout,
    strings: &RuntimeStrings,
    request: &BytecodeOperand,
    name: &str,
    value: &str,
) -> Result<(), String> {
    ensure_sysv_buf_runtime(target, "service_require_header")?;
    let arg_regs = abi_arg_regs(target.abi);
    emit_load_buf_operand(out, frame, request, arg_regs[0], arg_regs[1])?;
    emit_load_runtime_string(out, arg_regs[2], strings, name);
    emit_load_runtime_string(out, arg_regs[3], strings, value);
    emit_runtime_call(
        out,
        target,
        &extern_symbol("mira_service_require_header_buf_u8", target.flavor),
    );
    emit_store_scalar_reg(out, dst, "%rax")
}

fn emit_service_error_status_call(
    out: &mut String,
    target: X86_64Target,
    dst: SlotLayout,
    strings: &RuntimeStrings,
    kind: &str,
) -> Result<(), String> {
    emit_load_runtime_string(out, abi_arg_regs(target.abi)[0], strings, kind);
    emit_runtime_call(
        out,
        target,
        &extern_symbol("mira_service_error_status", target.flavor),
    );
    emit_store_scalar_reg(out, dst, "%rax")
}

fn emit_spawn_call(
    out: &mut String,
    target: X86_64Target,
    dst: SlotLayout,
    strings: &RuntimeStrings,
    command: &str,
) -> Result<(), String> {
    emit_load_runtime_string(out, abi_arg_regs(target.abi)[0], strings, command);
    emit_runtime_call(
        out,
        target,
        &extern_symbol("mira_rt_spawn_status", target.flavor),
    );
    emit_store_scalar_reg(out, dst, "%rax")
}

fn emit_net_call(
    out: &mut String,
    target: X86_64Target,
    dst: SlotLayout,
    strings: &RuntimeStrings,
    host: &str,
    port: u16,
) -> Result<(), String> {
    let arg_regs = abi_arg_regs(target.abi);
    emit_load_runtime_string(out, arg_regs[0], strings, host);
    out.push_str(&format!("  movl ${}, {}\n", port, reg32(arg_regs[1])?));
    emit_runtime_call(
        out,
        target,
        &extern_symbol("mira_rt_net_connect_ok", target.flavor),
    );
    emit_store_scalar_reg(out, dst, "%rax")
}

fn emit_ffi_call(
    out: &mut String,
    frame: &FrameLayout,
    target: X86_64Target,
    dst: SlotLayout,
    symbol: &str,
    args: &[BytecodeOperand],
) -> Result<(), String> {
    let arg_regs = abi_arg_regs(target.abi);
    if args.len() > arg_regs.len() {
        return Err(format!(
            "x86_64 backend supports at most {} ffi args for {:?}, got {}",
            arg_regs.len(),
            target.abi,
            args.len()
        ));
    }
    for (index, operand) in args.iter().enumerate() {
        emit_load_scalar_operand(out, frame, operand, arg_regs[index])?;
    }
    emit_runtime_call(out, target, &extern_symbol(symbol, target.flavor));
    emit_store_scalar_reg(out, dst, "%rax")
}

fn emit_runtime_call(out: &mut String, target: X86_64Target, symbol: &str) {
    if target.abi == X86_64Abi::Win64 {
        out.push_str("  subq $32, %rsp\n");
        out.push_str(&format!("  callq {symbol}\n"));
        out.push_str("  addq $32, %rsp\n");
    } else {
        out.push_str(&format!("  callq {symbol}\n"));
    }
}

fn emit_load_runtime_string(out: &mut String, reg: &str, strings: &RuntimeStrings, value: &str) {
    let label = strings
        .labels
        .get(value)
        .unwrap_or_else(|| panic!("missing x86_64 runtime string label for {value}"));
    out.push_str(&format!("  leaq {label}(%rip), {reg}\n"));
}

fn emit_load_stack_address(out: &mut String, reg: &str, offset: usize) {
    out.push_str(&format!("  leaq {}(%rsp), {reg}\n", offset));
}

fn emit_terminator(
    out: &mut String,
    function: &BytecodeFunction,
    block_index: usize,
    frame: &FrameLayout,
    target: X86_64Target,
    strings: &RuntimeStrings,
    terminator: &BytecodeTerminator,
) -> Result<(), String> {
    match terminator {
        BytecodeTerminator::Return(value) => {
            emit_load_return_value(out, frame, value, function.return_kind)?;
            emit_function_epilogue(out, frame);
            Ok(())
        }
        BytecodeTerminator::Jump(edge) => {
            emit_edge_jump(out, function, frame, target, strings, edge)
        }
        BytecodeTerminator::Branch {
            condition,
            truthy,
            falsy,
        } => {
            let false_label = local_label(
                target.flavor,
                &format!("{}_{}_false", function.name, block_index),
            );
            emit_load_scalar_operand(out, frame, condition, "%rax")?;
            out.push_str("  testl %eax, %eax\n");
            out.push_str(&format!("  je {false_label}\n"));
            emit_edge_moves(out, frame, target, strings, truthy)?;
            out.push_str(&format!(
                "  jmp {}\n",
                block_label(function, target.flavor, truthy.target)
            ));
            out.push_str(&format!("{false_label}:\n"));
            emit_edge_moves(out, frame, target, strings, falsy)?;
            out.push_str(&format!(
                "  jmp {}\n",
                block_label(function, target.flavor, falsy.target)
            ));
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
            target,
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
    target: X86_64Target,
    strings: &RuntimeStrings,
    value: &BytecodeOperand,
    cases: &[BytecodeMatchCase],
    default: &BytecodeEdge,
) -> Result<(), String> {
    emit_load_scalar_operand(out, frame, value, "%rax")?;
    let default_label = local_label(
        target.flavor,
        &format!("{}_{}_match_default", function.name, block_index),
    );
    let case_labels = (0..cases.len())
        .map(|index| {
            local_label(
                target.flavor,
                &format!("{}_{}_match_case_{}", function.name, block_index, index),
            )
        })
        .collect::<Vec<_>>();
    for (index, case) in cases.iter().enumerate() {
        out.push_str(&format!("  cmpq ${}, %rax\n", case.tag_index));
        out.push_str(&format!("  je {}\n", case_labels[index]));
    }
    out.push_str(&format!("  jmp {default_label}\n"));
    for (index, case) in cases.iter().enumerate() {
        out.push_str(&format!("{}:\n", case_labels[index]));
        emit_edge_moves(out, frame, target, strings, &case.edge)?;
        out.push_str(&format!(
            "  jmp {}\n",
            block_label(function, target.flavor, case.edge.target)
        ));
    }
    out.push_str(&format!("{default_label}:\n"));
    emit_edge_moves(out, frame, target, strings, default)?;
    out.push_str(&format!(
        "  jmp {}\n",
        block_label(function, target.flavor, default.target)
    ));
    Ok(())
}

fn emit_edge_jump(
    out: &mut String,
    function: &BytecodeFunction,
    frame: &FrameLayout,
    target: X86_64Target,
    strings: &RuntimeStrings,
    edge: &BytecodeEdge,
) -> Result<(), String> {
    emit_edge_moves(out, frame, target, strings, edge)?;
    out.push_str(&format!(
        "  jmp {}\n",
        block_label(function, target.flavor, edge.target)
    ));
    Ok(())
}

fn emit_edge_moves(
    out: &mut String,
    frame: &FrameLayout,
    target: X86_64Target,
    strings: &RuntimeStrings,
    edge: &BytecodeEdge,
) -> Result<(), String> {
    if !edge_needs_staging(edge) {
        for instruction in &edge.moves {
            emit_instruction(out, frame, target, strings, instruction)?;
        }
        return Ok(());
    }
    for (index, instruction) in edge.moves.iter().enumerate() {
        let temp = frame.temp_slot(index, instruction.dst_kind)?;
        emit_expr_to_dst(out, frame, target, strings, temp, &instruction.expr)?;
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
            emit_load_scalar_layout(out, src, "%rax")?;
            emit_store_scalar_reg(out, dst, "%rax")
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
        BytecodeValueKind::SpanI32 => Err("x86_64 backend cannot return span values".to_string()),
        _ => emit_load_scalar_operand(out, frame, operand, "%rax"),
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
        BytecodeOperand::Imm(immediate) => emit_load_immediate(out, reg, immediate),
    }
}

fn emit_load_scalar_layout(out: &mut String, layout: SlotLayout, reg: &str) -> Result<(), String> {
    match layout.home {
        SlotHome::Reg(src) => match layout.kind {
            BytecodeValueKind::I64 | BytecodeValueKind::U64 => {
                if src != reg {
                    out.push_str(&format!("  movq {src}, {reg}\n"));
                }
            }
            BytecodeValueKind::I32 => {
                if src != reg {
                    out.push_str(&format!("  movslq {}, {reg}\n", reg32(src)?));
                }
            }
            BytecodeValueKind::U32 | BytecodeValueKind::U8 | BytecodeValueKind::Bool => {
                let dst = reg32(reg)?;
                let src32 = reg32(src)?;
                if dst != src32 {
                    out.push_str(&format!("  movl {src32}, {dst}\n"));
                }
            }
            BytecodeValueKind::SpanI32 | BytecodeValueKind::BufU8 => {
                return Err("x86_64 backend expected scalar layout, got aggregate".to_string())
            }
        },
        SlotHome::Stack(offset) => match layout.kind {
            BytecodeValueKind::I32 => out.push_str(&format!("  movslq {}(%rsp), {reg}\n", offset)),
            BytecodeValueKind::I64 | BytecodeValueKind::U64 => {
                out.push_str(&format!("  movq {}(%rsp), {reg}\n", offset))
            }
            BytecodeValueKind::U32 | BytecodeValueKind::U8 | BytecodeValueKind::Bool => {
                out.push_str(&format!("  movl {}(%rsp), {}\n", offset, reg32(reg)?))
            }
            BytecodeValueKind::SpanI32 | BytecodeValueKind::BufU8 => {
                return Err("x86_64 backend expected scalar layout, got aggregate".to_string())
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
        _ => Err("x86_64 backend only supports span moves from span slots".to_string()),
    }
}

fn emit_load_i32_to_i64(out: &mut String, layout: SlotLayout, reg: &str) -> Result<(), String> {
    match layout.home {
        SlotHome::Reg(src) => out.push_str(&format!("  movslq {}, {reg}\n", reg32(src)?)),
        SlotHome::Stack(offset) => out.push_str(&format!("  movslq {}(%rsp), {reg}\n", offset)),
    }
    Ok(())
}

fn emit_load_span_ptr(out: &mut String, layout: SlotLayout, reg: &str) -> Result<(), String> {
    match layout.home {
        SlotHome::Stack(offset) => {
            out.push_str(&format!("  movq {}(%rsp), {reg}\n", offset));
            Ok(())
        }
        SlotHome::Reg(_) => {
            Err("x86_64 backend does not keep span values in registers".to_string())
        }
    }
}

fn emit_load_span_len(
    out: &mut String,
    layout: SlotLayout,
    reg32_name: &str,
) -> Result<(), String> {
    match layout.home {
        SlotHome::Stack(offset) => {
            out.push_str(&format!("  movl {}(%rsp), {reg32_name}\n", offset + 8));
            Ok(())
        }
        SlotHome::Reg(_) => {
            Err("x86_64 backend does not keep span values in registers".to_string())
        }
    }
}

fn load_span_layout(out: &mut String, layout: SlotLayout) -> Result<(String, String), String> {
    match layout.home {
        SlotHome::Stack(offset) => {
            out.push_str(&format!("  movq {}(%rsp), %rax\n", offset));
            out.push_str(&format!("  movq {}(%rsp), %r10\n", offset + 8));
            Ok(("%rax".to_string(), "%r10".to_string()))
        }
        SlotHome::Reg(_) => {
            Err("x86_64 backend does not keep span values in registers".to_string())
        }
    }
}

fn emit_load_immediate(
    out: &mut String,
    reg: &str,
    immediate: &BytecodeImmediate,
) -> Result<(), String> {
    match immediate {
        BytecodeImmediate::U8(value) => {
            out.push_str(&format!("  movl ${value}, {}\n", reg32(reg)?))
        }
        BytecodeImmediate::I32(value) => {
            out.push_str(&format!("  movl ${value}, {}\n", reg32(reg)?))
        }
        BytecodeImmediate::I64(value) => out.push_str(&format!("  movabsq ${value}, {reg}\n")),
        BytecodeImmediate::U64(value) => out.push_str(&format!("  movabsq ${value}, {reg}\n")),
        BytecodeImmediate::U32(value) => {
            out.push_str(&format!("  movl ${value}, {}\n", reg32(reg)?))
        }
        BytecodeImmediate::Bool(value) => out.push_str(&format!(
            "  movl ${}, {}\n",
            if *value { 1 } else { 0 },
            reg32(reg)?
        )),
    }
    Ok(())
}

fn emit_store_scalar_reg(out: &mut String, dst: SlotLayout, reg: &str) -> Result<(), String> {
    match dst.home {
        SlotHome::Stack(offset) => match dst.kind {
            BytecodeValueKind::I64 | BytecodeValueKind::U64 => {
                out.push_str(&format!("  movq {reg}, {}(%rsp)\n", offset))
            }
            BytecodeValueKind::I32
            | BytecodeValueKind::U32
            | BytecodeValueKind::U8
            | BytecodeValueKind::Bool => {
                out.push_str(&format!("  movl {}, {}(%rsp)\n", reg32(reg)?, offset))
            }
            BytecodeValueKind::SpanI32 | BytecodeValueKind::BufU8 => unreachable!(),
        },
        SlotHome::Reg(dst_reg) => match dst.kind {
            BytecodeValueKind::I64 | BytecodeValueKind::U64 => {
                if dst_reg != reg {
                    out.push_str(&format!("  movq {reg}, {dst_reg}\n"));
                }
            }
            BytecodeValueKind::I32 => {
                out.push_str(&format!("  movslq {}, {dst_reg}\n", reg32(reg)?));
            }
            BytecodeValueKind::U32 | BytecodeValueKind::U8 | BytecodeValueKind::Bool => {
                out.push_str(&format!("  movl {}, {}\n", reg32(reg)?, reg32(dst_reg)?));
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
            out.push_str(&format!("  movq {ptr_reg}, {}(%rsp)\n", offset));
            out.push_str(&format!(
                "  movl {}, {}(%rsp)\n",
                reg32(len_reg)?,
                offset + 8
            ));
            Ok(())
        }
        SlotHome::Reg(_) => {
            Err("x86_64 backend does not keep span values in registers".to_string())
        }
    }
}

fn emit_function_epilogue(out: &mut String, frame: &FrameLayout) {
    emit_restore_regs(out, frame);
    if frame.frame_size > 0 {
        out.push_str(&format!("  addq ${}, %rsp\n", frame.frame_size));
    }
    out.push_str("  popq %rbp\n");
    out.push_str("  ret\n");
}

fn exported_symbol(function: &BytecodeFunction, target: X86_64Target) -> String {
    match target.flavor {
        X86_64ObjectFlavor::MachO => format!("_mira_func_{}", function.name),
        X86_64ObjectFlavor::Elf | X86_64ObjectFlavor::Coff => {
            format!("mira_func_{}", function.name)
        }
    }
}

fn block_label(function: &BytecodeFunction, flavor: X86_64ObjectFlavor, index: usize) -> String {
    local_label(flavor, &format!("{}_b{}", function.name, index))
}

fn local_label(flavor: X86_64ObjectFlavor, suffix: &str) -> String {
    match flavor {
        X86_64ObjectFlavor::MachO => format!("L_{suffix}"),
        X86_64ObjectFlavor::Elf | X86_64ObjectFlavor::Coff => format!(".L_{suffix}"),
    }
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

fn build_frame_layout(function: &BytecodeFunction, target: X86_64Target) -> FrameLayout {
    let assigned = choose_register_slots(function, target);
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

fn collect_runtime_strings(
    function: &BytecodeFunction,
    flavor: X86_64ObjectFlavor,
) -> RuntimeStrings {
    let mut labels = BTreeMap::new();
    let mut ordered = Vec::new();
    let mut next = 0usize;
    for block in &function.blocks {
        collect_runtime_strings_from_instructions(
            &mut labels,
            &mut ordered,
            &mut next,
            function,
            flavor,
            &block.instructions,
        );
        collect_runtime_strings_from_terminator(
            &mut labels,
            &mut ordered,
            &mut next,
            function,
            flavor,
            &block.terminator,
        );
    }
    RuntimeStrings { labels, ordered }
}

fn collect_runtime_strings_from_terminator(
    labels: &mut BTreeMap<String, String>,
    ordered: &mut Vec<(String, String)>,
    next: &mut usize,
    function: &BytecodeFunction,
    flavor: X86_64ObjectFlavor,
    terminator: &BytecodeTerminator,
) {
    match terminator {
        BytecodeTerminator::Return(_) => {}
        BytecodeTerminator::Jump(edge) => collect_runtime_strings_from_instructions(
            labels,
            ordered,
            next,
            function,
            flavor,
            &edge.moves,
        ),
        BytecodeTerminator::Branch { truthy, falsy, .. } => {
            collect_runtime_strings_from_instructions(
                labels,
                ordered,
                next,
                function,
                flavor,
                &truthy.moves,
            );
            collect_runtime_strings_from_instructions(
                labels,
                ordered,
                next,
                function,
                flavor,
                &falsy.moves,
            );
        }
        BytecodeTerminator::Match { cases, default, .. } => {
            for case in cases {
                collect_runtime_strings_from_instructions(
                    labels,
                    ordered,
                    next,
                    function,
                    flavor,
                    &case.edge.moves,
                );
            }
            collect_runtime_strings_from_instructions(
                labels,
                ordered,
                next,
                function,
                flavor,
                &default.moves,
            );
        }
    }
}

fn collect_runtime_strings_from_instructions(
    labels: &mut BTreeMap<String, String>,
    ordered: &mut Vec<(String, String)>,
    next: &mut usize,
    function: &BytecodeFunction,
    flavor: X86_64ObjectFlavor,
    instructions: &[BytecodeInstruction],
) {
    for instruction in instructions {
        match &instruction.expr {
            BytecodeExpr::FsReadU32 { path } | BytecodeExpr::FsWriteU32 { path, .. } => {
                insert_runtime_string(labels, ordered, next, function, flavor, path);
            }
            BytecodeExpr::RtSpawnU32 {
                function: callee, ..
            } => {
                insert_runtime_string(labels, ordered, next, function, flavor, callee);
            }
            BytecodeExpr::SpawnCall { command, .. } => {
                insert_runtime_string(labels, ordered, next, function, flavor, command);
            }
            BytecodeExpr::NetConnect { host, .. } => {
                insert_runtime_string(labels, ordered, next, function, flavor, host);
            }
            BytecodeExpr::BufLit { literal } | BytecodeExpr::TlsServerConfigBuf { value: literal } => {
                insert_runtime_string(labels, ordered, next, function, flavor, literal);
            }
            BytecodeExpr::TlsListen { host, cert, key, .. } => {
                insert_runtime_string(labels, ordered, next, function, flavor, host);
                insert_runtime_string(labels, ordered, next, function, flavor, cert);
                insert_runtime_string(labels, ordered, next, function, flavor, key);
            }
            BytecodeExpr::ServiceOpen { name }
            | BytecodeExpr::ServiceTraceBegin { name, .. }
            | BytecodeExpr::ServiceErrorStatus { kind: name }
            | BytecodeExpr::DbOpen { path: name }
            | BytecodeExpr::JsonGetU32 { key: name, .. } => {
                insert_runtime_string(labels, ordered, next, function, flavor, name);
            }
            BytecodeExpr::ServiceRoute { method, path, .. } => {
                insert_runtime_string(labels, ordered, next, function, flavor, method);
                insert_runtime_string(labels, ordered, next, function, flavor, path);
            }
            BytecodeExpr::ServiceRequireHeader { name, value, .. } => {
                insert_runtime_string(labels, ordered, next, function, flavor, name);
                insert_runtime_string(labels, ordered, next, function, flavor, value);
            }
            BytecodeExpr::ServiceLog { .. } => {
                insert_runtime_string(labels, ordered, next, function, flavor, "info");
            }
            BytecodeExpr::ServiceMetricCount { .. } => {
                insert_runtime_string(labels, ordered, next, function, flavor, "count");
            }
            BytecodeExpr::ServiceMigrateDb { .. } => {
                insert_runtime_string(labels, ordered, next, function, flavor, "migration");
            }
            _ => {}
        }
    }
}

fn insert_runtime_string(
    labels: &mut BTreeMap<String, String>,
    ordered: &mut Vec<(String, String)>,
    next: &mut usize,
    function: &BytecodeFunction,
    flavor: X86_64ObjectFlavor,
    value: &str,
) {
    if labels.contains_key(value) {
        return;
    }
    let label = local_label(flavor, &format!("{}_rt_str_{}", function.name, *next));
    *next += 1;
    labels.insert(value.to_string(), label.clone());
    ordered.push((label, value.to_string()));
}

fn emit_x86_64_string_section(
    out: &mut String,
    flavor: X86_64ObjectFlavor,
    strings: &RuntimeStrings,
) {
    if strings.ordered.is_empty() {
        return;
    }
    match flavor {
        X86_64ObjectFlavor::MachO => out.push_str(".section __TEXT,__cstring,cstring_literals\n"),
        X86_64ObjectFlavor::Elf | X86_64ObjectFlavor::Coff => out.push_str(".section .rodata\n"),
    }
    for (label, value) in &strings.ordered {
        out.push_str(&format!("{label}:\n"));
        out.push_str(&format!("  .asciz \"{}\"\n", escape_asm_string(value)));
    }
    out.push_str(".text\n");
    out.push_str(".p2align 4\n");
}

fn escape_asm_string(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\"', "\\\"")
        .replace('\n', "\\n")
}

fn extern_symbol(name: &str, flavor: X86_64ObjectFlavor) -> String {
    match flavor {
        X86_64ObjectFlavor::MachO => format!("_{name}"),
        X86_64ObjectFlavor::Elf | X86_64ObjectFlavor::Coff => name.to_string(),
    }
}

fn choose_register_slots(
    function: &BytecodeFunction,
    target: X86_64Target,
) -> Vec<(usize, &'static str)> {
    let scores = score_slots(function);
    let reg_pool = match target.abi {
        X86_64Abi::SysV => SYSV_REG_POOL.as_slice(),
        X86_64Abi::Win64 => WIN64_REG_POOL.as_slice(),
    };
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
        .take(reg_pool.len())
        .zip(reg_pool.iter().copied())
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
        | BytecodeExpr::SextI64 { value: operand } => count_operand_use(operand, weight, counts),
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
                for flag in flags.iter_mut().take(block_index + 1).skip(target) {
                    *flag = true;
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

fn abi_arg_regs(abi: X86_64Abi) -> &'static [&'static str] {
    match abi {
        X86_64Abi::SysV => &["%rdi", "%rsi", "%rdx", "%rcx", "%r8", "%r9"],
        X86_64Abi::Win64 => &["%rcx", "%rdx", "%r8", "%r9"],
    }
}

fn reg32(reg: &str) -> Result<&'static str, String> {
    match reg {
        "%rax" => Ok("%eax"),
        "%rbx" => Ok("%ebx"),
        "%rcx" => Ok("%ecx"),
        "%rdx" => Ok("%edx"),
        "%rsi" => Ok("%esi"),
        "%rdi" => Ok("%edi"),
        "%r8" => Ok("%r8d"),
        "%r9" => Ok("%r9d"),
        "%r10" => Ok("%r10d"),
        "%r11" => Ok("%r11d"),
        "%r12" => Ok("%r12d"),
        "%r13" => Ok("%r13d"),
        "%r14" => Ok("%r14d"),
        "%r15" => Ok("%r15d"),
        "%rbp" => Ok("%ebp"),
        "%rsp" => Ok("%esp"),
        other => Err(format!("expected x86_64 register, got {other}")),
    }
}

fn align_up(value: usize, alignment: usize) -> usize {
    if value % alignment == 0 {
        value
    } else {
        value + (alignment - (value % alignment))
    }
}
