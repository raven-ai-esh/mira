use std::cell::RefCell;
use std::collections::HashMap;
use std::fs;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream, ToSocketAddrs};
use std::path::Path;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::thread;
use std::time::Duration;
use std::time::Instant;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{Map, Value};

use crate::ast::Program;
use crate::codegen_c::{
    lower_program, LoweredExecBinaryOp, LoweredExecExpr, LoweredExecImmediate, LoweredExecOperand,
    LoweredProgram, LoweredStatement, LoweredTerminator,
};
use crate::types::{DataValue, TypeRef};

#[derive(Debug, Clone, PartialEq)]
pub enum RuntimeValue {
    U8(u8),
    I32(i32),
    I64(i64),
    U64(u64),
    U32(u32),
    Bool(bool),
    SpanI32(Vec<i32>),
    BufU8(Vec<u8>),
}

struct PlainListenerHandle {
    listener: TcpListener,
    timeout_ms: u32,
    shutdown_grace_ms: u32,
}

struct PlainSessionHandle {
    stream: TcpStream,
    timeout_ms: u32,
    reconnect_host: Option<String>,
    reconnect_port: u16,
    pending_bytes: u32,
    resume_id: u64,
}

struct TlsServerProcess {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout: Option<ChildStdout>,
    request_timeout_ms: u32,
    session_timeout_ms: u32,
    shutdown_grace_ms: u32,
    accepted: bool,
}

enum NetHandle {
    Listener(PlainListenerHandle),
    Stream(PlainSessionHandle),
    TlsListener(Arc<Mutex<TlsServerProcess>>),
    TlsSession(Arc<Mutex<TlsServerProcess>>),
}

struct SpawnHandle {
    child: Option<Child>,
    stdin: Option<ChildStdin>,
    waited_status: Option<i32>,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

struct HttpBodyStreamHandle {
    body: Vec<u8>,
    cursor: usize,
}

struct HttpResponseStreamHandle {
    session_handle: u64,
    closed: bool,
}

struct HttpClientHandle {
    host: String,
    port: u16,
}

struct HttpClientPoolHandle {
    host: String,
    port: u16,
    max_size: u32,
    leased: Vec<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DbBackendKind {
    Sqlite,
    Postgres,
}

#[derive(Default)]
struct DbRuntimeHandle {
    target: String,
    prepared: HashMap<String, Vec<u8>>,
    in_transaction: bool,
    tx_buffer: Vec<Vec<u8>>,
    last_error_code: u32,
    last_error_retryable: bool,
}

struct DbRuntimePool {
    target: String,
    max_size: u32,
    max_idle: u32,
    leased: Vec<u64>,
}

struct CacheHandle {
    target: String,
}

struct QueueHandle {
    target: String,
}

struct LeaseHandle {
    target: String,
}

struct PlacementHandle {
    target: String,
}

struct CoordHandle {
    target: String,
}

struct RuntimeSchedulerHandle {
    max_workers: u32,
    active_workers: Arc<(Mutex<u32>, Condvar)>,
    shutting_down: Arc<AtomicBool>,
}

struct RuntimeTaskHandle {
    join: Option<thread::JoinHandle<()>>,
    result: Arc<(Mutex<Option<Result<RuntimeValue, String>>>, Condvar)>,
    done: Arc<AtomicBool>,
    cancelled: Arc<AtomicBool>,
}

struct ChannelU32Handle {
    sender: SyncSender<u32>,
    receiver: Arc<Mutex<Receiver<u32>>>,
    len: Arc<Mutex<u32>>,
}

struct ChannelBufHandle {
    sender: SyncSender<Vec<u8>>,
    receiver: Arc<Mutex<Receiver<Vec<u8>>>>,
    len: Arc<Mutex<u32>>,
}

struct DeadlineHandle {
    deadline: Instant,
}

#[derive(Default)]
struct CancelScopeHandle {
    parent: Option<u64>,
    children: Vec<u64>,
    tasks: Vec<u64>,
    cancelled: bool,
}

struct RetryHandle {
    max_attempts: u32,
    failures: u32,
    base_backoff_ms: u32,
}

struct CircuitHandle {
    failure_threshold: u32,
    cooldown_ms: u32,
    consecutive_failures: u32,
    open_until: Option<Instant>,
    half_open: bool,
}

struct BackpressureHandle {
    limit: u32,
    inflight: u32,
}

struct SupervisorHandle {
    restart_budget: u32,
    restarts_used: u32,
    degrade_after: u32,
    failures: u32,
}

#[derive(Default)]
struct ServiceHandle {
    #[allow(dead_code)]
    name: String,
    healthy: u32,
    ready: u32,
    degraded: bool,
    shutdown: bool,
    traces_started: u64,
    trace_links: u64,
    metrics_count: u64,
    log_entries: u64,
    event_totals: HashMap<String, u32>,
    metric_totals: HashMap<String, u32>,
    metric_dim_totals: HashMap<String, u32>,
    failure_totals: HashMap<String, u32>,
    checkpoints_u32: HashMap<String, u32>,
}

struct ServiceTraceHandle {
    service_handle: u64,
    #[allow(dead_code)]
    span: String,
    linked_parent: Option<u64>,
}

#[derive(Clone)]
struct MessageDelivery {
    seq: u32,
    #[allow(dead_code)]
    conversation: String,
    recipient: String,
    payload: Vec<u8>,
    acked: bool,
    retry_count: u32,
}

#[derive(Default)]
struct MessageLogHandle {
    next_seq: u32,
    last_failure_class: u32,
    deliveries: Vec<MessageDelivery>,
    subscriptions: HashMap<String, Vec<String>>,
    dedup_keys: HashMap<String, u32>,
    delivery_totals: HashMap<String, u32>,
}

struct MessageReplayHandle {
    log_handle: u64,
    recipient: String,
    from_seq: u32,
    cursor: usize,
    last_seq: u32,
}

#[derive(Clone)]
struct StreamEntry {
    offset: u32,
    payload: Vec<u8>,
}

struct StreamHandle {
    target: String,
}

struct StreamReplayHandle {
    stream_handle: u64,
    from_offset: u32,
    cursor: usize,
    last_offset: u32,
}

#[derive(Default)]
struct BatchHandle {
    values: Vec<u64>,
}

#[derive(Default)]
struct AggregateHandle {
    count: u32,
    sum: u64,
    min: u64,
    max: u64,
    has_value: bool,
}

struct WindowHandle {
    width_ms: u32,
    entries: Vec<(u64, u64)>,
}

#[derive(Default)]
struct RuntimeState {
    next_handle: u64,
    net_handles: HashMap<u64, NetHandle>,
    spawn_handles: HashMap<u64, SpawnHandle>,
    http_body_streams: HashMap<u64, HttpBodyStreamHandle>,
    http_response_streams: HashMap<u64, HttpResponseStreamHandle>,
    http_clients: HashMap<u64, HttpClientHandle>,
    http_client_pools: HashMap<u64, HttpClientPoolHandle>,
    ffi_libs: HashMap<u64, String>,
    db_handles: HashMap<u64, DbRuntimeHandle>,
    db_pools: HashMap<u64, DbRuntimePool>,
    cache_handles: HashMap<u64, CacheHandle>,
    queue_handles: HashMap<u64, QueueHandle>,
    lease_handles: HashMap<u64, LeaseHandle>,
    placement_handles: HashMap<u64, PlacementHandle>,
    coord_handles: HashMap<u64, CoordHandle>,
    runtime_schedulers: HashMap<u64, RuntimeSchedulerHandle>,
    runtime_tasks: HashMap<u64, RuntimeTaskHandle>,
    channels_u32: HashMap<u64, ChannelU32Handle>,
    channels_buf: HashMap<u64, ChannelBufHandle>,
    deadlines: HashMap<u64, DeadlineHandle>,
    cancel_scopes: HashMap<u64, CancelScopeHandle>,
    retries: HashMap<u64, RetryHandle>,
    circuits: HashMap<u64, CircuitHandle>,
    backpressure_handles: HashMap<u64, BackpressureHandle>,
    supervisor_handles: HashMap<u64, SupervisorHandle>,
    service_handles: HashMap<u64, ServiceHandle>,
    service_traces: HashMap<u64, ServiceTraceHandle>,
    message_logs: HashMap<u64, MessageLogHandle>,
    message_replays: HashMap<u64, MessageReplayHandle>,
    stream_handles: HashMap<u64, StreamHandle>,
    stream_replays: HashMap<u64, StreamReplayHandle>,
    batch_handles: HashMap<u64, BatchHandle>,
    aggregate_handles: HashMap<u64, AggregateHandle>,
    window_handles: HashMap<u64, WindowHandle>,
}

static RUNTIME_STATE: OnceLock<Mutex<RuntimeState>> = OnceLock::new();
static RUNTIME_EXECUTION_GUARD: OnceLock<Mutex<()>> = OnceLock::new();
static CURRENT_LOWERED_PROGRAM: OnceLock<Mutex<Option<Arc<LoweredProgram>>>> = OnceLock::new();

thread_local! {
    static CURRENT_RUNTIME_CANCEL: RefCell<Option<Arc<AtomicBool>>> = const { RefCell::new(None) };
}

fn runtime_state() -> &'static Mutex<RuntimeState> {
    RUNTIME_STATE.get_or_init(|| Mutex::new(RuntimeState::default()))
}

pub(crate) fn runtime_execution_guard() -> &'static Mutex<()> {
    RUNTIME_EXECUTION_GUARD.get_or_init(|| Mutex::new(()))
}

pub(crate) fn with_lowered_program_context<T, F>(
    program: &LoweredProgram,
    f: F,
) -> Result<T, String>
where
    F: FnOnce() -> Result<T, String>,
{
    {
        let mut current = current_lowered_program()
            .lock()
            .map_err(|_| "current lowered program mutex poisoned".to_string())?;
        *current = Some(Arc::new(program.clone()));
    }
    let result = f();
    if let Ok(mut current) = current_lowered_program().lock() {
        *current = None;
    }
    result
}

fn current_lowered_program() -> &'static Mutex<Option<Arc<LoweredProgram>>> {
    CURRENT_LOWERED_PROGRAM.get_or_init(|| Mutex::new(None))
}

pub(crate) fn reset_runtime_state() -> Result<(), String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    state.net_handles.clear();
    state.spawn_handles.clear();
    state.http_body_streams.clear();
    state.http_response_streams.clear();
    state.http_clients.clear();
    state.http_client_pools.clear();
    state.ffi_libs.clear();
    state.db_handles.clear();
    state.db_pools.clear();
    state.cache_handles.clear();
    state.queue_handles.clear();
    state.lease_handles.clear();
    state.placement_handles.clear();
    state.coord_handles.clear();
    state.runtime_schedulers.clear();
    state.runtime_tasks.clear();
    state.channels_u32.clear();
    state.service_handles.clear();
    state.service_traces.clear();
    state.next_handle = 1;
    Ok(())
}

fn alloc_runtime_handle(state: &mut RuntimeState) -> u64 {
    let handle = if state.next_handle == 0 {
        1
    } else {
        state.next_handle
    };
    state.next_handle = handle.saturating_add(1);
    handle
}

fn stable_resume_id(host: &str, port: u16) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in host.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash ^= port as u64;
    hash = hash.wrapping_mul(0x100000001b3);
    hash
}

fn run_lowered_function_inner(
    program: &LoweredProgram,
    function_name: &str,
    args: &HashMap<String, RuntimeValue>,
) -> Result<RuntimeValue, String> {
    let function = program
        .functions
        .iter()
        .find(|function| function.name == function_name)
        .ok_or_else(|| format!("unknown lowered function {function_name}"))?;
    let block_map = function
        .blocks
        .iter()
        .map(|block| (block.label.as_str(), block))
        .collect::<HashMap<_, _>>();
    let mut env = args.clone();
    let mut current = "b0".to_string();
    let mut rand_state = function.rand_seed;
    let mut steps = 0usize;

    loop {
        steps += 1;
        if steps > 10_000_000 {
            return Err(format!(
                "lowered execution step limit exceeded in {function_name}"
            ));
        }
        let block = block_map
            .get(current.as_str())
            .ok_or_else(|| format!("missing lowered block {}", current))?;
        for statement in &block.statements {
            match statement {
                LoweredStatement::Assign(assignment) => {
                    let exec_expr = assignment.exec_expr.as_ref().ok_or_else(|| {
                        format!(
                            "lowered direct execution does not support assignment {} in {}",
                            assignment.target, function_name
                        )
                    })?;
                    let value = eval_exec_expr(exec_expr, &env, &mut rand_state)?;
                    env.insert(assignment.target.clone(), value);
                }
            }
        }
        match &block.terminator {
            LoweredTerminator::Return { exec_value, .. } => {
                let operand = exec_value.as_ref().ok_or_else(|| {
                    format!("lowered direct execution lacks return operand in {function_name}")
                })?;
                return eval_exec_operand(operand, &env);
            }
            LoweredTerminator::Jump { edge } => {
                apply_edge(&mut env, edge.assignments.as_slice(), &mut rand_state)?;
                current = edge.label.clone();
            }
            LoweredTerminator::Branch {
                exec_condition,
                truthy,
                falsy,
                ..
            } => {
                let operand = exec_condition.as_ref().ok_or_else(|| {
                    format!("lowered direct execution lacks branch operand in {function_name}")
                })?;
                let condition = eval_exec_operand(operand, &env)?;
                let edge = match condition {
                    RuntimeValue::Bool(true) => truthy,
                    RuntimeValue::Bool(false) => falsy,
                    other => {
                        return Err(format!(
                            "branch condition must be bool in {function_name}, got {other:?}"
                        ))
                    }
                };
                apply_edge(&mut env, edge.assignments.as_slice(), &mut rand_state)?;
                current = edge.label.clone();
            }
            LoweredTerminator::Match {
                exec_value,
                cases,
                default,
                ..
            } => {
                let operand = exec_value.as_ref().ok_or_else(|| {
                    format!("lowered direct execution lacks match operand in {function_name}")
                })?;
                let value = eval_exec_operand(operand, &env)?;
                let tag = match value {
                    RuntimeValue::I32(value) => value as i64,
                    RuntimeValue::I64(value) => value,
                    RuntimeValue::U64(value) => value as i64,
                    RuntimeValue::U32(value) => value as i64,
                    other => {
                        return Err(format!(
                            "match value must be integer in {function_name}, got {other:?}"
                        ))
                    }
                };
                let edge = cases
                    .iter()
                    .find(|case| case.tag_index as i64 == tag)
                    .map(|case| &case.edge)
                    .unwrap_or(default);
                apply_edge(&mut env, edge.assignments.as_slice(), &mut rand_state)?;
                current = edge.label.clone();
            }
        }
    }
}

pub fn run_lowered_function(
    program: &LoweredProgram,
    function_name: &str,
    args: &HashMap<String, RuntimeValue>,
) -> Result<RuntimeValue, String> {
    let _guard = runtime_execution_guard()
        .lock()
        .map_err(|_| "portable runtime execution mutex poisoned".to_string())?;
    reset_runtime_state()?;
    with_lowered_program_context(program, || {
        run_lowered_function_inner(program, function_name, args)
    })
}

pub fn benchmark_arg_values(
    program: &Program,
    function_name: &str,
    arguments: &[(String, DataValue)],
) -> Result<HashMap<String, RuntimeValue>, String> {
    let function = program
        .functions
        .iter()
        .find(|function| function.name == function_name)
        .ok_or_else(|| format!("unknown benchmark function {function_name}"))?;
    let mut out = HashMap::new();
    for arg in &function.args {
        let (_, value) = arguments
            .iter()
            .find(|(name, _)| name == &arg.name)
            .ok_or_else(|| format!("missing argument {} for {}", arg.name, function.name))?;
        out.insert(arg.name.clone(), runtime_value_from_data(&arg.ty, value)?);
    }
    Ok(out)
}

fn current_program_for_runtime() -> Result<Arc<LoweredProgram>, String> {
    current_lowered_program()
        .lock()
        .map_err(|_| "current lowered program mutex poisoned".to_string())?
        .clone()
        .ok_or_else(|| "portable runtime task API requires an active lowered program".to_string())
}

fn with_runtime_cancel_flag<T, F>(flag: Arc<AtomicBool>, f: F) -> T
where
    F: FnOnce() -> T,
{
    CURRENT_RUNTIME_CANCEL.with(|slot| {
        let previous = slot.replace(Some(flag));
        let result = f();
        slot.replace(previous);
        result
    })
}

pub fn portable_rt_open(max_workers: u32) -> Result<u64, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let handle = alloc_runtime_handle(&mut state);
    state.runtime_schedulers.insert(
        handle,
        RuntimeSchedulerHandle {
            max_workers: max_workers.max(1),
            active_workers: Arc::new((Mutex::new(0), Condvar::new())),
            shutting_down: Arc::new(AtomicBool::new(false)),
        },
    );
    Ok(handle)
}

fn current_runtime_scheduler(
    runtime: u64,
) -> Result<(Arc<(Mutex<u32>, Condvar)>, Arc<AtomicBool>, u32), String> {
    let state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let scheduler = state
        .runtime_schedulers
        .get(&runtime)
        .ok_or_else(|| format!("unknown runtime scheduler handle {runtime}"))?;
    Ok((
        scheduler.active_workers.clone(),
        scheduler.shutting_down.clone(),
        scheduler.max_workers,
    ))
}

fn portable_rt_spawn_impl(
    runtime: u64,
    function_name: &str,
    arg: RuntimeValue,
    block_until_slot: bool,
) -> Result<u64, String> {
    let program = current_program_for_runtime()?;
    let arg_name = program
        .functions
        .iter()
        .find(|function| function.name == function_name)
        .and_then(|function| function.args.first())
        .map(|arg| arg.1.clone())
        .ok_or_else(|| format!("runtime task target {function_name} missing lowered arg"))?;
    let (active_workers, shutting_down, max_workers) = current_runtime_scheduler(runtime)?;
    if !block_until_slot {
        let (count_lock, _) = &*active_workers;
        let active = count_lock
            .lock()
            .map_err(|_| "portable runtime scheduler mutex poisoned".to_string())?;
        if shutting_down.load(Ordering::SeqCst) || *active >= max_workers {
            return Ok(0);
        }
    }
    let cancelled = Arc::new(AtomicBool::new(false));
    let done = Arc::new(AtomicBool::new(false));
    let result_slot = Arc::new((Mutex::new(None), Condvar::new()));
    let thread_cancel = cancelled.clone();
    let thread_done = done.clone();
    let thread_result = result_slot.clone();
    let function_name_owned = function_name.to_string();
    let arg_value = arg.clone();
    let join = thread::spawn(move || {
        let (count_lock, count_cv) = &*active_workers;
        let mut active = match count_lock.lock() {
            Ok(guard) => guard,
            Err(_) => return,
        };
        if block_until_slot {
            while *active >= max_workers && !shutting_down.load(Ordering::SeqCst) {
                active = match count_cv.wait(active) {
                    Ok(guard) => guard,
                    Err(_) => return,
                };
            }
        } else if shutting_down.load(Ordering::SeqCst) || *active >= max_workers {
            return;
        }
        *active += 1;
        drop(active);

        let mut args = HashMap::new();
        args.insert(arg_name, arg_value);
        let result = with_runtime_cancel_flag(thread_cancel.clone(), || {
            run_lowered_function_inner(&program, &function_name_owned, &args)
        });

        let (result_lock, result_cv) = &*thread_result;
        if let Ok(mut slot) = result_lock.lock() {
            *slot = Some(result);
            thread_done.store(true, Ordering::SeqCst);
            result_cv.notify_all();
        }
        let (count_lock, count_cv) = &*active_workers;
        if let Ok(mut active) = count_lock.lock() {
            if *active > 0 {
                *active -= 1;
            }
            count_cv.notify_all();
        }
    });

    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let handle = alloc_runtime_handle(&mut state);
    state.runtime_tasks.insert(
        handle,
        RuntimeTaskHandle {
            join: Some(join),
            result: result_slot,
            done,
            cancelled,
        },
    );
    Ok(handle)
}

pub fn portable_rt_spawn_u32(runtime: u64, function_name: &str, arg: u32) -> Result<u64, String> {
    portable_rt_spawn_impl(runtime, function_name, RuntimeValue::U32(arg), true)
}

pub fn portable_rt_spawn_buf(
    runtime: u64,
    function_name: &str,
    arg: Vec<u8>,
) -> Result<u64, String> {
    portable_rt_spawn_impl(runtime, function_name, RuntimeValue::BufU8(arg), true)
}

pub fn portable_rt_try_spawn_u32(
    runtime: u64,
    function_name: &str,
    arg: u32,
) -> Result<u64, String> {
    portable_rt_spawn_impl(runtime, function_name, RuntimeValue::U32(arg), false)
}

pub fn portable_rt_try_spawn_buf(
    runtime: u64,
    function_name: &str,
    arg: Vec<u8>,
) -> Result<u64, String> {
    portable_rt_spawn_impl(runtime, function_name, RuntimeValue::BufU8(arg), false)
}

pub fn portable_rt_done(task: u64) -> Result<bool, String> {
    let state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let handle = state
        .runtime_tasks
        .get(&task)
        .ok_or_else(|| format!("unknown runtime task handle {task}"))?;
    Ok(handle.done.load(Ordering::SeqCst))
}

fn portable_rt_join_result(task: u64) -> Result<RuntimeValue, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let handle = state
        .runtime_tasks
        .get_mut(&task)
        .ok_or_else(|| format!("unknown runtime task handle {task}"))?;
    let (result_lock, result_cv) = &*handle.result;
    let mut result = result_lock
        .lock()
        .map_err(|_| "portable runtime task result mutex poisoned".to_string())?;
    while result.is_none() {
        result = result_cv
            .wait(result)
            .map_err(|_| "portable runtime task result wait poisoned".to_string())?;
    }
    let output = result
        .clone()
        .ok_or_else(|| format!("missing runtime task result for {task}"))??;
    if let Some(join) = handle.join.take() {
        let _ = join.join();
    }
    Ok(output)
}

pub fn portable_rt_join_u32(task: u64) -> Result<u32, String> {
    match portable_rt_join_result(task)? {
        RuntimeValue::U32(value) => Ok(value),
        other => Err(format!(
            "runtime task join expected u32 result, got {other:?}"
        )),
    }
}

pub fn portable_rt_join_buf(task: u64) -> Result<Vec<u8>, String> {
    match portable_rt_join_result(task)? {
        RuntimeValue::BufU8(value) => Ok(value),
        other => Err(format!(
            "runtime task join expected buf[u8] result, got {other:?}"
        )),
    }
}

pub fn portable_rt_cancel(task: u64) -> Result<bool, String> {
    let state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let handle = state
        .runtime_tasks
        .get(&task)
        .ok_or_else(|| format!("unknown runtime task handle {task}"))?;
    handle.cancelled.store(true, Ordering::SeqCst);
    Ok(true)
}

pub fn portable_rt_task_close(task: u64) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let mut handle = state
        .runtime_tasks
        .remove(&task)
        .ok_or_else(|| format!("unknown runtime task handle {task}"))?;
    if let Some(join) = handle.join.take() {
        let _ = join.join();
    }
    Ok(true)
}

pub fn portable_rt_shutdown(runtime: u64, grace_ms: u32) -> Result<bool, String> {
    let (active_workers, shutting_down) = {
        let state = runtime_state()
            .lock()
            .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
        let scheduler = state
            .runtime_schedulers
            .get(&runtime)
            .ok_or_else(|| format!("unknown runtime scheduler handle {runtime}"))?;
        (
            scheduler.active_workers.clone(),
            scheduler.shutting_down.clone(),
        )
    };
    shutting_down.store(true, Ordering::SeqCst);
    let deadline = Instant::now() + Duration::from_millis(grace_ms as u64);
    let (count_lock, count_cv) = &*active_workers;
    let mut active = count_lock
        .lock()
        .map_err(|_| "portable runtime scheduler mutex poisoned".to_string())?;
    while *active > 0 {
        let now = Instant::now();
        if now >= deadline {
            break;
        }
        let remaining = deadline.saturating_duration_since(now);
        let (next_active, _timeout) = count_cv
            .wait_timeout(active, remaining)
            .map_err(|_| "portable runtime scheduler wait poisoned".to_string())?;
        active = next_active;
    }
    Ok(*active == 0)
}

pub fn portable_rt_close(runtime: u64) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    state
        .runtime_schedulers
        .remove(&runtime)
        .ok_or_else(|| format!("unknown runtime scheduler handle {runtime}"))?;
    Ok(true)
}

pub fn portable_rt_inflight(runtime: u64) -> Result<u32, String> {
    let (active_workers, _, _) = current_runtime_scheduler(runtime)?;
    let (count_lock, _) = &*active_workers;
    let active = count_lock
        .lock()
        .map_err(|_| "portable runtime scheduler mutex poisoned".to_string())?;
    Ok(*active)
}

pub fn portable_rt_cancelled() -> bool {
    CURRENT_RUNTIME_CANCEL.with(|slot| {
        slot.borrow()
            .as_ref()
            .map(|flag| flag.load(Ordering::SeqCst))
            .unwrap_or(false)
    })
}

pub fn portable_chan_open_u32(capacity: u32) -> Result<u64, String> {
    let (sender, receiver) = sync_channel(capacity.max(1) as usize);
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let handle = alloc_runtime_handle(&mut state);
    state.channels_u32.insert(
        handle,
        ChannelU32Handle {
            sender,
            receiver: Arc::new(Mutex::new(receiver)),
            len: Arc::new(Mutex::new(0)),
        },
    );
    Ok(handle)
}

pub fn portable_chan_send_u32(channel: u64, value: u32) -> Result<bool, String> {
    let (sender, len) = {
        let state = runtime_state()
            .lock()
            .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
        let channel = state
            .channels_u32
            .get(&channel)
            .ok_or_else(|| format!("unknown u32 channel handle {channel}"))?;
        (channel.sender.clone(), channel.len.clone())
    };
    sender
        .send(value)
        .map_err(|error| format!("portable channel send failed: {error}"))?;
    if let Ok(mut count) = len.lock() {
        *count += 1;
    }
    Ok(true)
}

pub fn portable_chan_recv_u32(channel: u64) -> Result<u32, String> {
    let (receiver, len) = {
        let state = runtime_state()
            .lock()
            .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
        let channel = state
            .channels_u32
            .get(&channel)
            .ok_or_else(|| format!("unknown u32 channel handle {channel}"))?;
        (channel.receiver.clone(), channel.len.clone())
    };
    let value = receiver
        .lock()
        .map_err(|_| "portable channel receiver mutex poisoned".to_string())?
        .recv()
        .map_err(|error| format!("portable channel recv failed: {error}"))?;
    if let Ok(mut count) = len.lock() {
        if *count > 0 {
            *count -= 1;
        }
    }
    Ok(value)
}

pub fn portable_chan_open_buf(capacity: u32) -> Result<u64, String> {
    let (sender, receiver) = sync_channel(capacity.max(1) as usize);
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let handle = alloc_runtime_handle(&mut state);
    state.channels_buf.insert(
        handle,
        ChannelBufHandle {
            sender,
            receiver: Arc::new(Mutex::new(receiver)),
            len: Arc::new(Mutex::new(0)),
        },
    );
    Ok(handle)
}

pub fn portable_chan_send_buf(channel: u64, value: Vec<u8>) -> Result<bool, String> {
    let (sender, len) = {
        let state = runtime_state()
            .lock()
            .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
        let channel = state
            .channels_buf
            .get(&channel)
            .ok_or_else(|| format!("unknown buf channel handle {channel}"))?;
        (channel.sender.clone(), channel.len.clone())
    };
    sender
        .send(value)
        .map_err(|error| format!("portable buf channel send failed: {error}"))?;
    if let Ok(mut count) = len.lock() {
        *count += 1;
    }
    Ok(true)
}

pub fn portable_chan_recv_buf(channel: u64) -> Result<Vec<u8>, String> {
    let (receiver, len) = {
        let state = runtime_state()
            .lock()
            .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
        let channel = state
            .channels_buf
            .get(&channel)
            .ok_or_else(|| format!("unknown buf channel handle {channel}"))?;
        (channel.receiver.clone(), channel.len.clone())
    };
    let value = receiver
        .lock()
        .map_err(|_| "portable buf channel receiver mutex poisoned".to_string())?
        .recv()
        .map_err(|error| format!("portable buf channel recv failed: {error}"))?;
    if let Ok(mut count) = len.lock() {
        if *count > 0 {
            *count -= 1;
        }
    }
    Ok(value)
}

pub fn portable_chan_len(channel: u64) -> Result<u32, String> {
    let state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    if let Some(channel) = state.channels_u32.get(&channel) {
        return channel
            .len
            .lock()
            .map(|count| *count)
            .map_err(|_| "portable channel len mutex poisoned".to_string());
    }
    if let Some(channel) = state.channels_buf.get(&channel) {
        return channel
            .len
            .lock()
            .map(|count| *count)
            .map_err(|_| "portable buf channel len mutex poisoned".to_string());
    }
    Err(format!("unknown channel handle {channel}"))
}

pub fn portable_chan_close(channel: u64) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    if state.channels_u32.remove(&channel).is_some() || state.channels_buf.remove(&channel).is_some() {
        Ok(true)
    } else {
        Err(format!("unknown channel handle {channel}"))
    }
}

pub fn portable_deadline_open_ms(timeout_ms: u32) -> Result<u64, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let handle = alloc_runtime_handle(&mut state);
    state.deadlines.insert(
        handle,
        DeadlineHandle {
            deadline: Instant::now() + Duration::from_millis(timeout_ms.max(1) as u64),
        },
    );
    Ok(handle)
}

pub fn portable_deadline_expired(handle: u64) -> Result<bool, String> {
    let state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let deadline = state
        .deadlines
        .get(&handle)
        .ok_or_else(|| format!("unknown deadline handle {handle}"))?;
    Ok(Instant::now() >= deadline.deadline)
}

pub fn portable_deadline_remaining_ms(handle: u64) -> Result<u32, String> {
    let state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let deadline = state
        .deadlines
        .get(&handle)
        .ok_or_else(|| format!("unknown deadline handle {handle}"))?;
    Ok(deadline
        .deadline
        .saturating_duration_since(Instant::now())
        .as_millis()
        .min(u128::from(u32::MAX)) as u32)
}

pub fn portable_deadline_close(handle: u64) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    state
        .deadlines
        .remove(&handle)
        .ok_or_else(|| format!("unknown deadline handle {handle}"))?;
    Ok(true)
}

pub fn portable_cancel_scope_open() -> Result<u64, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let handle = alloc_runtime_handle(&mut state);
    state.cancel_scopes.insert(handle, CancelScopeHandle::default());
    Ok(handle)
}

pub fn portable_cancel_scope_child(parent: u64) -> Result<u64, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    if !state.cancel_scopes.contains_key(&parent) {
        return Err(format!("unknown cancel scope handle {parent}"));
    }
    let handle = alloc_runtime_handle(&mut state);
    state.cancel_scopes.insert(
        handle,
        CancelScopeHandle {
            parent: Some(parent),
            ..CancelScopeHandle::default()
        },
    );
    if let Some(parent_scope) = state.cancel_scopes.get_mut(&parent) {
        parent_scope.children.push(handle);
    }
    Ok(handle)
}

pub fn portable_cancel_scope_bind_task(scope: u64, task: u64) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    if !state.runtime_tasks.contains_key(&task) {
        return Err(format!("unknown runtime task handle {task}"));
    }
    let scope_entry = state
        .cancel_scopes
        .get_mut(&scope)
        .ok_or_else(|| format!("unknown cancel scope handle {scope}"))?;
    if !scope_entry.tasks.contains(&task) {
        scope_entry.tasks.push(task);
    }
    Ok(true)
}

fn cancel_scope_recursive(state: &mut RuntimeState, scope: u64) {
    let (children, tasks) = match state.cancel_scopes.get_mut(&scope) {
        Some(entry) => {
            entry.cancelled = true;
            (entry.children.clone(), entry.tasks.clone())
        }
        None => return,
    };
    for task in tasks {
        if let Some(handle) = state.runtime_tasks.get(&task) {
            handle.cancelled.store(true, Ordering::SeqCst);
        }
    }
    for child in children {
        cancel_scope_recursive(state, child);
    }
}

pub fn portable_cancel_scope_cancel(scope: u64) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    if !state.cancel_scopes.contains_key(&scope) {
        return Err(format!("unknown cancel scope handle {scope}"));
    }
    cancel_scope_recursive(&mut state, scope);
    Ok(true)
}

pub fn portable_cancel_scope_cancelled(scope: u64) -> Result<bool, String> {
    let state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let mut current = Some(scope);
    while let Some(handle) = current {
        let entry = state
            .cancel_scopes
            .get(&handle)
            .ok_or_else(|| format!("unknown cancel scope handle {scope}"))?;
        if entry.cancelled {
            return Ok(true);
        }
        current = entry.parent;
    }
    Ok(false)
}

pub fn portable_cancel_scope_close(scope: u64) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let parent = state
        .cancel_scopes
        .get(&scope)
        .ok_or_else(|| format!("unknown cancel scope handle {scope}"))?
        .parent;
    if let Some(parent_handle) = parent {
        if let Some(parent_entry) = state.cancel_scopes.get_mut(&parent_handle) {
            parent_entry.children.retain(|child| *child != scope);
        }
    }
    state.cancel_scopes.remove(&scope);
    Ok(true)
}

pub fn portable_retry_open(max_attempts: u32, base_backoff_ms: u32) -> Result<u64, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let handle = alloc_runtime_handle(&mut state);
    state.retries.insert(
        handle,
        RetryHandle {
            max_attempts: max_attempts.max(1),
            failures: 0,
            base_backoff_ms: base_backoff_ms.max(1),
        },
    );
    Ok(handle)
}

pub fn portable_retry_record_failure(handle: u64) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let retry = state
        .retries
        .get_mut(&handle)
        .ok_or_else(|| format!("unknown retry handle {handle}"))?;
    retry.failures = retry.failures.saturating_add(1);
    Ok(true)
}

pub fn portable_retry_record_success(handle: u64) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let retry = state
        .retries
        .get_mut(&handle)
        .ok_or_else(|| format!("unknown retry handle {handle}"))?;
    retry.failures = 0;
    Ok(true)
}

pub fn portable_retry_next_delay_ms(handle: u64) -> Result<u32, String> {
    let state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let retry = state
        .retries
        .get(&handle)
        .ok_or_else(|| format!("unknown retry handle {handle}"))?;
    if retry.failures == 0 || retry.failures > retry.max_attempts {
        return Ok(0);
    }
    let shift = retry.failures.saturating_sub(1).min(31);
    Ok(retry.base_backoff_ms.saturating_mul(1u32 << shift))
}

pub fn portable_retry_exhausted(handle: u64) -> Result<bool, String> {
    let state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let retry = state
        .retries
        .get(&handle)
        .ok_or_else(|| format!("unknown retry handle {handle}"))?;
    Ok(retry.failures >= retry.max_attempts)
}

pub fn portable_retry_close(handle: u64) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    state
        .retries
        .remove(&handle)
        .ok_or_else(|| format!("unknown retry handle {handle}"))?;
    Ok(true)
}

pub fn portable_circuit_open(threshold: u32, cooldown_ms: u32) -> Result<u64, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let handle = alloc_runtime_handle(&mut state);
    state.circuits.insert(
        handle,
        CircuitHandle {
            failure_threshold: threshold.max(1),
            cooldown_ms: cooldown_ms.max(1),
            consecutive_failures: 0,
            open_until: None,
            half_open: false,
        },
    );
    Ok(handle)
}

pub fn portable_circuit_allow(handle: u64) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let circuit = state
        .circuits
        .get_mut(&handle)
        .ok_or_else(|| format!("unknown circuit handle {handle}"))?;
    if let Some(until) = circuit.open_until {
        if Instant::now() < until {
            return Ok(false);
        }
        circuit.open_until = None;
        circuit.half_open = true;
    }
    Ok(true)
}

pub fn portable_circuit_record_failure(handle: u64) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let circuit = state
        .circuits
        .get_mut(&handle)
        .ok_or_else(|| format!("unknown circuit handle {handle}"))?;
    circuit.consecutive_failures = circuit.consecutive_failures.saturating_add(1);
    if circuit.half_open || circuit.consecutive_failures >= circuit.failure_threshold {
        circuit.open_until =
            Some(Instant::now() + Duration::from_millis(circuit.cooldown_ms as u64));
        circuit.half_open = false;
    }
    Ok(true)
}

pub fn portable_circuit_record_success(handle: u64) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let circuit = state
        .circuits
        .get_mut(&handle)
        .ok_or_else(|| format!("unknown circuit handle {handle}"))?;
    circuit.consecutive_failures = 0;
    circuit.open_until = None;
    circuit.half_open = false;
    Ok(true)
}

pub fn portable_circuit_state(handle: u64) -> Result<u32, String> {
    let state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let circuit = state
        .circuits
        .get(&handle)
        .ok_or_else(|| format!("unknown circuit handle {handle}"))?;
    if circuit.open_until.is_some() {
        Ok(1)
    } else if circuit.half_open {
        Ok(2)
    } else {
        Ok(0)
    }
}

pub fn portable_circuit_close(handle: u64) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    state
        .circuits
        .remove(&handle)
        .ok_or_else(|| format!("unknown circuit handle {handle}"))?;
    Ok(true)
}

pub fn portable_backpressure_open(limit: u32) -> Result<u64, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let handle = alloc_runtime_handle(&mut state);
    state.backpressure_handles.insert(
        handle,
        BackpressureHandle {
            limit: limit.max(1),
            inflight: 0,
        },
    );
    Ok(handle)
}

pub fn portable_backpressure_acquire(handle: u64) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let backpressure = state
        .backpressure_handles
        .get_mut(&handle)
        .ok_or_else(|| format!("unknown backpressure handle {handle}"))?;
    if backpressure.inflight >= backpressure.limit {
        return Ok(false);
    }
    backpressure.inflight += 1;
    Ok(true)
}

pub fn portable_backpressure_release(handle: u64) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let backpressure = state
        .backpressure_handles
        .get_mut(&handle)
        .ok_or_else(|| format!("unknown backpressure handle {handle}"))?;
    if backpressure.inflight > 0 {
        backpressure.inflight -= 1;
    }
    Ok(true)
}

pub fn portable_backpressure_saturated(handle: u64) -> Result<bool, String> {
    let state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let backpressure = state
        .backpressure_handles
        .get(&handle)
        .ok_or_else(|| format!("unknown backpressure handle {handle}"))?;
    Ok(backpressure.inflight >= backpressure.limit)
}

pub fn portable_backpressure_close(handle: u64) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    state
        .backpressure_handles
        .remove(&handle)
        .ok_or_else(|| format!("unknown backpressure handle {handle}"))?;
    Ok(true)
}

pub fn portable_supervisor_open(restart_budget: u32, degrade_after: u32) -> Result<u64, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let handle = alloc_runtime_handle(&mut state);
    state.supervisor_handles.insert(
        handle,
        SupervisorHandle {
            restart_budget,
            restarts_used: 0,
            degrade_after: degrade_after.max(1),
            failures: 0,
        },
    );
    Ok(handle)
}

pub fn portable_supervisor_record_failure(handle: u64, _code: u32) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let supervisor = state
        .supervisor_handles
        .get_mut(&handle)
        .ok_or_else(|| format!("unknown supervisor handle {handle}"))?;
    supervisor.failures = supervisor.failures.saturating_add(1);
    Ok(true)
}

pub fn portable_supervisor_record_recovery(handle: u64) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let supervisor = state
        .supervisor_handles
        .get_mut(&handle)
        .ok_or_else(|| format!("unknown supervisor handle {handle}"))?;
    supervisor.failures = 0;
    Ok(true)
}

pub fn portable_supervisor_should_restart(handle: u64) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let supervisor = state
        .supervisor_handles
        .get_mut(&handle)
        .ok_or_else(|| format!("unknown supervisor handle {handle}"))?;
    if supervisor.restarts_used < supervisor.restart_budget {
        supervisor.restarts_used += 1;
        Ok(true)
    } else {
        Ok(false)
    }
}

pub fn portable_supervisor_degraded(handle: u64) -> Result<bool, String> {
    let state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let supervisor = state
        .supervisor_handles
        .get(&handle)
        .ok_or_else(|| format!("unknown supervisor handle {handle}"))?;
    Ok(supervisor.failures >= supervisor.degrade_after
        || supervisor.restarts_used >= supervisor.restart_budget)
}

pub fn portable_supervisor_close(handle: u64) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    state
        .supervisor_handles
        .remove(&handle)
        .ok_or_else(|| format!("unknown supervisor handle {handle}"))?;
    Ok(true)
}

pub fn lower_program_for_direct_exec(program: &Program) -> Result<LoweredProgram, String> {
    lower_program(program)
}

fn apply_edge(
    env: &mut HashMap<String, RuntimeValue>,
    assignments: &[crate::codegen_c::LoweredAssignment],
    rand_state: &mut Option<u32>,
) -> Result<(), String> {
    let mut values = Vec::new();
    for assignment in assignments {
        let exec_expr = assignment.exec_expr.as_ref().ok_or_else(|| {
            format!(
                "lowered direct execution does not support edge assignment {}",
                assignment.target
            )
        })?;
        values.push((
            assignment.target.clone(),
            eval_exec_expr(exec_expr, env, rand_state)?,
        ));
    }
    for (target, value) in values {
        env.insert(target, value);
    }
    Ok(())
}

fn eval_exec_expr(
    expr: &LoweredExecExpr,
    env: &HashMap<String, RuntimeValue>,
    rand_state: &mut Option<u32>,
) -> Result<RuntimeValue, String> {
    match expr {
        LoweredExecExpr::Move(operand) => eval_exec_operand(operand, env),
        LoweredExecExpr::BufLit { literal } => {
            Ok(RuntimeValue::BufU8(decode_escaped_literal_bytes(literal)))
        }
        LoweredExecExpr::BufConcat { left, right } => {
            let left = match eval_exec_operand(left, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("buf_concat expects buf[u8], got {other:?}")),
            };
            let right = match eval_exec_operand(right, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("buf_concat expects buf[u8], got {other:?}")),
            };
            let mut out = left;
            out.extend_from_slice(&right);
            Ok(RuntimeValue::BufU8(out))
        }
        LoweredExecExpr::AllocBufU8 { region: _, len } => {
            let len = match eval_exec_operand(len, env)? {
                RuntimeValue::U32(value) => value as usize,
                other => return Err(format!("alloc buf[u8] expects u32 length, got {other:?}")),
            };
            Ok(RuntimeValue::BufU8(vec![0u8; len]))
        }
        LoweredExecExpr::DropBufU8 { value } => match eval_exec_operand(value, env)? {
            RuntimeValue::BufU8(_) => Ok(RuntimeValue::Bool(true)),
            other => Err(format!("drop buf[u8] expects buf value, got {other:?}")),
        },
        LoweredExecExpr::ClockNowNs => Ok(RuntimeValue::U64(mira_clock_now_ns())),
        LoweredExecExpr::RandU32 => Ok(RuntimeValue::U32(mira_rand_next_u32(rand_state))),
        LoweredExecExpr::FsReadU32 { path } => Ok(RuntimeValue::U32(portable_fs_read_u32(path)?)),
        LoweredExecExpr::FsWriteU32 { path, value } => {
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::U32(value) => value,
                other => return Err(format!("fs_write_u32 expects u32 operand, got {other:?}")),
            };
            Ok(RuntimeValue::Bool(portable_fs_write_u32(path, value)?))
        }
        LoweredExecExpr::FsReadAllU8 { path } => {
            Ok(RuntimeValue::BufU8(portable_fs_read_all_u8(path)?))
        }
        LoweredExecExpr::FsWriteAllU8 { path, value } => {
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("fs_write_all expects buf[u8], got {other:?}")),
            };
            Ok(RuntimeValue::Bool(portable_fs_write_all_u8(path, &value)?))
        }
        LoweredExecExpr::NetWriteAllU8 { host, port, value } => {
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("net_write_all expects buf[u8], got {other:?}")),
            };
            Ok(RuntimeValue::Bool(portable_net_write_all(
                host, *port, &value,
            )?))
        }
        LoweredExecExpr::NetExchangeAllU8 { host, port, value } => {
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("net_exchange_all expects buf[u8], got {other:?}")),
            };
            Ok(RuntimeValue::BufU8(portable_net_exchange_all(
                host, *port, &value,
            )?))
        }
        LoweredExecExpr::NetServeExchangeAllU8 {
            host,
            port,
            response,
        } => {
            let response = match eval_exec_operand(response, env)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "net_serve_exchange_all expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::BufU8(portable_net_serve_exchange_all(
                host, *port, &response,
            )?))
        }
        LoweredExecExpr::NetListen { host, port } => {
            Ok(RuntimeValue::U64(portable_net_listen_handle(host, *port)?))
        }
        LoweredExecExpr::TlsListen {
            host,
            port,
            cert,
            key,
            request_timeout_ms,
            session_timeout_ms,
            shutdown_grace_ms,
        } => Ok(RuntimeValue::U64(portable_tls_listen_handle(
            host,
            *port,
            cert,
            key,
            *request_timeout_ms,
            *session_timeout_ms,
            *shutdown_grace_ms,
        )?)),
        LoweredExecExpr::NetAccept { listener } => {
            let listener = eval_handle_operand(listener, env, "net_accept")?;
            Ok(RuntimeValue::U64(portable_net_accept_handle(listener)?))
        }
        LoweredExecExpr::NetSessionOpen { host, port } => Ok(RuntimeValue::U64(
            portable_net_session_open(host, *port)?,
        )),
        LoweredExecExpr::HttpSessionAccept { listener } => {
            let listener = eval_handle_operand(listener, env, "http_session_accept")?;
            Ok(RuntimeValue::U64(portable_http_session_accept(listener)?))
        }
        LoweredExecExpr::NetReadAllU8 { handle } => {
            let handle = eval_handle_operand(handle, env, "net_read_all")?;
            Ok(RuntimeValue::BufU8(portable_net_read_all_handle(handle)?))
        }
        LoweredExecExpr::SessionReadChunkU8 { handle, chunk_size } => {
            let handle = eval_handle_operand(handle, env, "session_read_chunk")?;
            let chunk_size = match eval_exec_operand(chunk_size, env)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "session_read_chunk expects u32 chunk size, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::BufU8(portable_session_read_chunk(
                handle, chunk_size,
            )?))
        }
        LoweredExecExpr::HttpSessionRequest { handle } => {
            let handle = eval_handle_operand(handle, env, "http_session_request")?;
            Ok(RuntimeValue::BufU8(portable_http_session_request(handle)?))
        }
        LoweredExecExpr::NetWriteHandleAllU8 { handle, value } => {
            let handle = eval_handle_operand(handle, env, "net_write_handle_all")?;
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "net_write_handle_all expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_net_write_all_handle(
                handle, &value,
            )?))
        }
        LoweredExecExpr::SessionWriteChunkU8 { handle, value } => {
            let handle = eval_handle_operand(handle, env, "session_write_chunk")?;
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "session_write_chunk expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_session_write_chunk(
                handle, &value,
            )?))
        }
        LoweredExecExpr::SessionFlush { handle } => {
            let handle = eval_handle_operand(handle, env, "session_flush")?;
            Ok(RuntimeValue::Bool(portable_session_flush(handle)?))
        }
        LoweredExecExpr::SessionAlive { handle } => {
            let handle = eval_handle_operand(handle, env, "session_alive")?;
            Ok(RuntimeValue::Bool(portable_session_alive(handle)?))
        }
        LoweredExecExpr::SessionHeartbeatU8 { handle, value } => {
            let handle = eval_handle_operand(handle, env, "session_heartbeat")?;
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!("session_heartbeat expects buf[u8], got {other:?}"))
                }
            };
            Ok(RuntimeValue::Bool(portable_session_heartbeat(
                handle, &value,
            )?))
        }
        LoweredExecExpr::SessionBackpressure { handle } => {
            let handle = eval_handle_operand(handle, env, "session_backpressure")?;
            Ok(RuntimeValue::U32(portable_session_backpressure(handle)?))
        }
        LoweredExecExpr::SessionBackpressureWait {
            handle,
            max_pending,
        } => {
            let handle = eval_handle_operand(handle, env, "session_backpressure_wait")?;
            let max_pending = match eval_exec_operand(max_pending, env)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "session_backpressure_wait expects u32 max pending, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_session_backpressure_wait(
                handle,
                max_pending,
            )?))
        }
        LoweredExecExpr::SessionResumeId { handle } => {
            let handle = eval_handle_operand(handle, env, "session_resume_id")?;
            Ok(RuntimeValue::U64(portable_session_resume_id(handle)?))
        }
        LoweredExecExpr::SessionReconnect { handle } => {
            let handle = eval_handle_operand(handle, env, "session_reconnect")?;
            Ok(RuntimeValue::Bool(portable_session_reconnect(handle)?))
        }
        LoweredExecExpr::NetClose { handle } => {
            let handle = eval_handle_operand(handle, env, "net_close")?;
            Ok(RuntimeValue::Bool(portable_net_close_handle(handle)?))
        }
        LoweredExecExpr::HttpSessionClose { handle } => {
            let handle = eval_handle_operand(handle, env, "http_session_close")?;
            Ok(RuntimeValue::Bool(portable_http_session_close(handle)?))
        }
        LoweredExecExpr::HttpMethodEq { request, method } => {
            let request = match eval_exec_operand(request, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("http_method_eq expects buf[u8], got {other:?}")),
            };
            Ok(RuntimeValue::Bool(portable_http_method_eq(
                &request, method,
            )))
        }
        LoweredExecExpr::HttpPathEq { request, path } => {
            let request = match eval_exec_operand(request, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("http_path_eq expects buf[u8], got {other:?}")),
            };
            Ok(RuntimeValue::Bool(portable_http_path_eq(&request, path)))
        }
        LoweredExecExpr::HttpRequestMethod { request } => {
            let request = match eval_exec_operand(request, env)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "http_request_method expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::BufU8(portable_http_request_method(&request)))
        }
        LoweredExecExpr::HttpRequestPath { request } => {
            let request = match eval_exec_operand(request, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("http_request_path expects buf[u8], got {other:?}")),
            };
            Ok(RuntimeValue::BufU8(portable_http_request_path(&request)))
        }
        LoweredExecExpr::HttpRouteParam {
            request,
            pattern,
            param,
        } => {
            let request = match eval_exec_operand(request, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("http_route_param expects buf[u8], got {other:?}")),
            };
            Ok(RuntimeValue::BufU8(portable_http_route_param(
                &request, pattern, param,
            )))
        }
        LoweredExecExpr::HttpHeaderEq {
            request,
            name,
            value,
        } => {
            let request = match eval_exec_operand(request, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("http_header_eq expects buf[u8], got {other:?}")),
            };
            Ok(RuntimeValue::Bool(portable_http_header_eq(
                &request, name, value,
            )))
        }
        LoweredExecExpr::HttpCookieEq {
            request,
            name,
            value,
        } => {
            let request = match eval_exec_operand(request, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("http_cookie_eq expects buf[u8], got {other:?}")),
            };
            Ok(RuntimeValue::Bool(portable_http_cookie_eq(
                &request, name, value,
            )))
        }
        LoweredExecExpr::HttpStatusU32 { value } => {
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("http_status_u32 expects buf[u8], got {other:?}")),
            };
            Ok(RuntimeValue::U32(portable_http_status_u32(&value)))
        }
        LoweredExecExpr::BufEqLit { value, literal } => {
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("buf_eq_lit expects buf[u8], got {other:?}")),
            };
            Ok(RuntimeValue::Bool(portable_buf_eq_lit(&value, literal)))
        }
        LoweredExecExpr::BufContainsLit { value, literal } => {
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("buf_contains_lit expects buf[u8], got {other:?}")),
            };
            Ok(RuntimeValue::Bool(portable_buf_contains_lit(
                &value, literal,
            )))
        }
        LoweredExecExpr::HttpHeader { request, name } => {
            let request = match eval_exec_operand(request, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("http_header expects buf[u8], got {other:?}")),
            };
            Ok(RuntimeValue::BufU8(portable_http_header(&request, name)))
        }
        LoweredExecExpr::HttpHeaderCount { request } => {
            let request = match eval_exec_operand(request, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("http_header_count expects buf[u8], got {other:?}")),
            };
            Ok(RuntimeValue::U32(portable_http_header_count(&request)))
        }
        LoweredExecExpr::HttpHeaderName { request, index } => {
            let request = match eval_exec_operand(request, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("http_header_name expects buf[u8], got {other:?}")),
            };
            let index = match eval_exec_operand(index, env)? {
                RuntimeValue::U32(value) => value,
                other => return Err(format!("http_header_name expects u32 index, got {other:?}")),
            };
            Ok(RuntimeValue::BufU8(portable_http_header_name(&request, index)))
        }
        LoweredExecExpr::HttpHeaderValue { request, index } => {
            let request = match eval_exec_operand(request, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("http_header_value expects buf[u8], got {other:?}")),
            };
            let index = match eval_exec_operand(index, env)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!("http_header_value expects u32 index, got {other:?}"))
                }
            };
            Ok(RuntimeValue::BufU8(portable_http_header_value(&request, index)))
        }
        LoweredExecExpr::HttpCookie { request, name } => {
            let request = match eval_exec_operand(request, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("http_cookie expects buf[u8], got {other:?}")),
            };
            Ok(RuntimeValue::BufU8(portable_http_cookie(&request, name)))
        }
        LoweredExecExpr::HttpQueryParam { request, key } => {
            let request = match eval_exec_operand(request, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("http_query_param expects buf[u8], got {other:?}")),
            };
            Ok(RuntimeValue::BufU8(portable_http_query_param(
                &request, key,
            )))
        }
        LoweredExecExpr::HttpBody { request } => {
            let request = match eval_exec_operand(request, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("http_body expects buf[u8], got {other:?}")),
            };
            Ok(RuntimeValue::BufU8(portable_http_body(&request)))
        }
        LoweredExecExpr::HttpMultipartPartCount { request } => {
            let request = match eval_exec_operand(request, env)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "http_multipart_part_count expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U32(portable_http_multipart_part_count(&request)))
        }
        LoweredExecExpr::HttpMultipartPartName { request, index } => {
            let request = match eval_exec_operand(request, env)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "http_multipart_part_name expects buf[u8], got {other:?}"
                    ))
                }
            };
            let index = match eval_exec_operand(index, env)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "http_multipart_part_name expects u32 index, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::BufU8(portable_http_multipart_part_name(
                &request, index,
            )))
        }
        LoweredExecExpr::HttpMultipartPartFilename { request, index } => {
            let request = match eval_exec_operand(request, env)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "http_multipart_part_filename expects buf[u8], got {other:?}"
                    ))
                }
            };
            let index = match eval_exec_operand(index, env)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "http_multipart_part_filename expects u32 index, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::BufU8(portable_http_multipart_part_filename(
                &request, index,
            )))
        }
        LoweredExecExpr::HttpMultipartPartBody { request, index } => {
            let request = match eval_exec_operand(request, env)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "http_multipart_part_body expects buf[u8], got {other:?}"
                    ))
                }
            };
            let index = match eval_exec_operand(index, env)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "http_multipart_part_body expects u32 index, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::BufU8(portable_http_multipart_part_body(
                &request, index,
            )))
        }
        LoweredExecExpr::HttpBodyLimit { request, limit } => {
            let request = match eval_exec_operand(request, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("http_body_limit expects buf[u8], got {other:?}")),
            };
            let limit = match eval_exec_operand(limit, env)? {
                RuntimeValue::U32(value) => value,
                other => return Err(format!("http_body_limit expects u32, got {other:?}")),
            };
            Ok(RuntimeValue::Bool(portable_http_body_limit(
                &request, limit,
            )))
        }
        LoweredExecExpr::HttpBodyStreamOpen { request } => {
            let request = match eval_exec_operand(request, env)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "http_body_stream_open expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U64(portable_http_body_stream_open(&request)?))
        }
        LoweredExecExpr::HttpBodyStreamNext { handle, chunk_size } => {
            let handle = eval_handle_operand(handle, env, "http_body_stream_next")?;
            let chunk_size = match eval_exec_operand(chunk_size, env)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "http_body_stream_next expects u32 chunk size, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::BufU8(portable_http_body_stream_next(
                handle, chunk_size,
            )?))
        }
        LoweredExecExpr::HttpBodyStreamClose { handle } => {
            let handle = eval_handle_operand(handle, env, "http_body_stream_close")?;
            Ok(RuntimeValue::Bool(portable_http_body_stream_close(handle)?))
        }
        LoweredExecExpr::HttpResponseStreamOpen {
            handle,
            status,
            content_type,
        } => {
            let handle = eval_handle_operand(handle, env, "http_response_stream_open")?;
            let status = match eval_exec_operand(status, env)? {
                RuntimeValue::U32(value) => value,
                RuntimeValue::I32(value) if value >= 0 => value as u32,
                other => {
                    return Err(format!(
                        "http_response_stream_open expects u32 status, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U64(portable_http_response_stream_open(
                handle,
                status,
                content_type,
            )?))
        }
        LoweredExecExpr::HttpResponseStreamWrite { handle, body } => {
            let handle = eval_handle_operand(handle, env, "http_response_stream_write")?;
            let body = match eval_exec_operand(body, env)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "http_response_stream_write expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_http_response_stream_write(
                handle, &body,
            )?))
        }
        LoweredExecExpr::HttpResponseStreamClose { handle } => {
            let handle = eval_handle_operand(handle, env, "http_response_stream_close")?;
            Ok(RuntimeValue::Bool(portable_http_response_stream_close(handle)?))
        }
        LoweredExecExpr::HttpClientOpen { host, port } => Ok(RuntimeValue::U64(
            portable_http_client_open(host, *port)?,
        )),
        LoweredExecExpr::HttpClientRequest { handle, request } => {
            let handle = eval_handle_operand(handle, env, "http_client_request")?;
            let request = match eval_exec_operand(request, env)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!("http_client_request expects buf[u8], got {other:?}"))
                }
            };
            Ok(RuntimeValue::BufU8(portable_http_client_request(
                handle, &request,
            )?))
        }
        LoweredExecExpr::HttpClientRequestRetry {
            handle,
            retries,
            backoff_ms,
            request,
        } => {
            let handle = eval_handle_operand(handle, env, "http_client_request_retry")?;
            let retries = match eval_exec_operand(retries, env)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "http_client_request_retry expects u32 retries, got {other:?}"
                    ))
                }
            };
            let backoff_ms = match eval_exec_operand(backoff_ms, env)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "http_client_request_retry expects u32 backoff, got {other:?}"
                    ))
                }
            };
            let request = match eval_exec_operand(request, env)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "http_client_request_retry expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::BufU8(portable_http_client_request_retry(
                handle,
                retries,
                backoff_ms,
                &request,
            )?))
        }
        LoweredExecExpr::HttpClientClose { handle } => {
            let handle = eval_handle_operand(handle, env, "http_client_close")?;
            Ok(RuntimeValue::Bool(portable_http_client_close(handle)?))
        }
        LoweredExecExpr::HttpClientPoolOpen {
            host,
            port,
            max_size,
        } => {
            let max_size = match eval_exec_operand(max_size, env)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "http_client_pool_open expects u32 max_size, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U64(portable_http_client_pool_open(
                host, *port, max_size,
            )?))
        }
        LoweredExecExpr::HttpClientPoolAcquire { pool } => {
            let pool = eval_handle_operand(pool, env, "http_client_pool_acquire")?;
            Ok(RuntimeValue::U64(portable_http_client_pool_acquire(pool)?))
        }
        LoweredExecExpr::HttpClientPoolRelease { pool, handle } => {
            let pool = eval_handle_operand(pool, env, "http_client_pool_release")?;
            let handle = eval_handle_operand(handle, env, "http_client_pool_release")?;
            Ok(RuntimeValue::Bool(portable_http_client_pool_release(
                pool, handle,
            )?))
        }
        LoweredExecExpr::HttpClientPoolClose { pool } => {
            let pool = eval_handle_operand(pool, env, "http_client_pool_close")?;
            Ok(RuntimeValue::Bool(portable_http_client_pool_close(pool)?))
        }
        LoweredExecExpr::HttpServerConfigU32 { token } => {
            Ok(RuntimeValue::U32(portable_http_server_config_u32(token)))
        }
        LoweredExecExpr::MsgLogOpen => Ok(RuntimeValue::U64(portable_msg_log_open()?)),
        LoweredExecExpr::MsgLogClose { handle } => {
            let handle = eval_handle_operand(handle, env, "msg_log_close")?;
            Ok(RuntimeValue::Bool(portable_msg_log_close(handle)?))
        }
        LoweredExecExpr::MsgSend {
            handle,
            conversation,
            recipient,
            payload,
        } => {
            let handle = eval_handle_operand(handle, env, "msg_send")?;
            let payload = match eval_exec_operand(payload, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("msg_send expects buf[u8], got {other:?}")),
            };
            Ok(RuntimeValue::U32(portable_msg_send(
                handle,
                conversation,
                recipient,
                &payload,
            )?))
        }
        LoweredExecExpr::MsgSendDedup {
            handle,
            conversation,
            recipient,
            dedup_key,
            payload,
        } => {
            let handle = eval_handle_operand(handle, env, "msg_send_dedup")?;
            let dedup_key = match eval_exec_operand(dedup_key, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("msg_send_dedup expects buf[u8] key, got {other:?}")),
            };
            let payload = match eval_exec_operand(payload, env)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!("msg_send_dedup expects buf[u8] payload, got {other:?}"))
                }
            };
            Ok(RuntimeValue::U32(portable_msg_send_dedup(
                handle,
                conversation,
                recipient,
                &dedup_key,
                &payload,
            )?))
        }
        LoweredExecExpr::MsgSubscribe {
            handle,
            room,
            recipient,
        } => {
            let handle = eval_handle_operand(handle, env, "msg_subscribe")?;
            Ok(RuntimeValue::Bool(portable_msg_subscribe(
                handle, room, recipient,
            )?))
        }
        LoweredExecExpr::MsgSubscriberCount { handle, room } => {
            let handle = eval_handle_operand(handle, env, "msg_subscriber_count")?;
            Ok(RuntimeValue::U32(portable_msg_subscriber_count(handle, room)?))
        }
        LoweredExecExpr::MsgFanout {
            handle,
            room,
            payload,
        } => {
            let handle = eval_handle_operand(handle, env, "msg_fanout")?;
            let payload = match eval_exec_operand(payload, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("msg_fanout expects buf[u8], got {other:?}")),
            };
            Ok(RuntimeValue::U32(portable_msg_fanout(handle, room, &payload)?))
        }
        LoweredExecExpr::MsgRecvNext { handle, recipient } => {
            let handle = eval_handle_operand(handle, env, "msg_recv_next")?;
            Ok(RuntimeValue::BufU8(portable_msg_recv_next(handle, recipient)?))
        }
        LoweredExecExpr::MsgRecvSeq { handle, recipient } => {
            let handle = eval_handle_operand(handle, env, "msg_recv_seq")?;
            Ok(RuntimeValue::U32(portable_msg_recv_seq(handle, recipient)?))
        }
        LoweredExecExpr::MsgAck {
            handle,
            recipient,
            seq,
        } => {
            let handle = eval_handle_operand(handle, env, "msg_ack")?;
            let seq = match eval_exec_operand(seq, env)? {
                RuntimeValue::U32(value) => value,
                other => return Err(format!("msg_ack expects u32 seq, got {other:?}")),
            };
            Ok(RuntimeValue::Bool(portable_msg_ack(handle, recipient, seq)?))
        }
        LoweredExecExpr::MsgMarkRetry {
            handle,
            recipient,
            seq,
        } => {
            let handle = eval_handle_operand(handle, env, "msg_mark_retry")?;
            let seq = match eval_exec_operand(seq, env)? {
                RuntimeValue::U32(value) => value,
                other => return Err(format!("msg_mark_retry expects u32 seq, got {other:?}")),
            };
            Ok(RuntimeValue::Bool(portable_msg_mark_retry(
                handle, recipient, seq,
            )?))
        }
        LoweredExecExpr::MsgRetryCount {
            handle,
            recipient,
            seq,
        } => {
            let handle = eval_handle_operand(handle, env, "msg_retry_count")?;
            let seq = match eval_exec_operand(seq, env)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!("msg_retry_count expects u32 seq, got {other:?}"))
                }
            };
            Ok(RuntimeValue::U32(portable_msg_retry_count(
                handle, recipient, seq,
            )?))
        }
        LoweredExecExpr::MsgPendingCount { handle, recipient } => {
            let handle = eval_handle_operand(handle, env, "msg_pending_count")?;
            Ok(RuntimeValue::U32(portable_msg_pending_count(
                handle, recipient,
            )?))
        }
        LoweredExecExpr::MsgDeliveryTotal { handle, recipient } => {
            let handle = eval_handle_operand(handle, env, "msg_delivery_total")?;
            Ok(RuntimeValue::U32(portable_msg_delivery_total(
                handle, recipient,
            )?))
        }
        LoweredExecExpr::MsgFailureClass { handle } => {
            let handle = eval_handle_operand(handle, env, "msg_failure_class")?;
            Ok(RuntimeValue::U32(portable_msg_failure_class(handle)?))
        }
        LoweredExecExpr::MsgReplayOpen {
            handle,
            recipient,
            from_seq,
        } => {
            let handle = eval_handle_operand(handle, env, "msg_replay_open")?;
            let from_seq = match eval_exec_operand(from_seq, env)? {
                RuntimeValue::U32(value) => value,
                other => return Err(format!("msg_replay_open expects u32 from_seq, got {other:?}")),
            };
            Ok(RuntimeValue::U64(portable_msg_replay_open(
                handle, recipient, from_seq,
            )?))
        }
        LoweredExecExpr::MsgReplayNext { handle } => {
            let handle = eval_handle_operand(handle, env, "msg_replay_next")?;
            Ok(RuntimeValue::BufU8(portable_msg_replay_next(handle)?))
        }
        LoweredExecExpr::MsgReplaySeq { handle } => {
            let handle = eval_handle_operand(handle, env, "msg_replay_seq")?;
            Ok(RuntimeValue::U32(portable_msg_replay_seq(handle)?))
        }
        LoweredExecExpr::MsgReplayClose { handle } => {
            let handle = eval_handle_operand(handle, env, "msg_replay_close")?;
            Ok(RuntimeValue::Bool(portable_msg_replay_close(handle)?))
        }
        LoweredExecExpr::ServiceOpen { name } => {
            Ok(RuntimeValue::U64(portable_service_open(name)?))
        }
        LoweredExecExpr::ServiceClose { handle } => {
            let handle = eval_handle_operand(handle, env, "service_close")?;
            Ok(RuntimeValue::Bool(portable_service_close(handle)?))
        }
        LoweredExecExpr::ServiceShutdown { handle, grace_ms } => {
            let handle = eval_handle_operand(handle, env, "service_shutdown")?;
            let grace_ms = match eval_exec_operand(grace_ms, env)? {
                RuntimeValue::U32(value) => value,
                other => return Err(format!("service_shutdown expects u32 grace, got {other:?}")),
            };
            Ok(RuntimeValue::Bool(portable_service_shutdown(
                handle, grace_ms,
            )?))
        }
        LoweredExecExpr::ServiceLog {
            handle,
            level: _,
            message,
        } => {
            let handle = eval_handle_operand(handle, env, "service_log")?;
            let message = match eval_exec_operand(message, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("service_log expects buf[u8], got {other:?}")),
            };
            Ok(RuntimeValue::Bool(portable_service_log(handle, &message)?))
        }
        LoweredExecExpr::ServiceTraceBegin { handle, name } => {
            let handle = eval_handle_operand(handle, env, "service_trace_begin")?;
            Ok(RuntimeValue::U64(portable_service_trace_begin(
                handle, name,
            )?))
        }
        LoweredExecExpr::ServiceTraceEnd { trace } => {
            let trace = eval_handle_operand(trace, env, "service_trace_end")?;
            Ok(RuntimeValue::Bool(portable_service_trace_end(trace)?))
        }
        LoweredExecExpr::ServiceMetricCount {
            handle,
            metric: _,
            value,
        } => {
            let handle = eval_handle_operand(handle, env, "service_metric_count")?;
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "service_metric_count expects u32 value, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_service_metric_count(
                handle, value,
            )?))
        }
        LoweredExecExpr::ServiceMetricCountDim {
            handle,
            metric,
            dimension,
            value,
        } => {
            let handle = eval_handle_operand(handle, env, "service_metric_count_dim")?;
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "service_metric_count_dim expects u32 value, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_service_metric_count_dim(
                handle, metric, dimension, value,
            )?))
        }
        LoweredExecExpr::ServiceMetricTotal { handle, metric } => {
            let handle = eval_handle_operand(handle, env, "service_metric_total")?;
            Ok(RuntimeValue::U32(portable_service_metric_total(
                handle, metric,
            )?))
        }
        LoweredExecExpr::ServiceHealthStatus { handle } => {
            let handle = eval_handle_operand(handle, env, "service_health_status")?;
            Ok(RuntimeValue::U32(portable_service_health_status(handle)?))
        }
        LoweredExecExpr::ServiceReadinessStatus { handle } => {
            let handle = eval_handle_operand(handle, env, "service_readiness_status")?;
            Ok(RuntimeValue::U32(portable_service_readiness_status(
                handle,
            )?))
        }
        LoweredExecExpr::ServiceSetHealth { handle, status } => {
            let handle = eval_handle_operand(handle, env, "service_set_health")?;
            let status = match eval_exec_operand(status, env)? {
                RuntimeValue::U32(value) => value,
                other => return Err(format!("service_set_health expects u32 status, got {other:?}")),
            };
            Ok(RuntimeValue::Bool(portable_service_set_health(
                handle, status,
            )?))
        }
        LoweredExecExpr::ServiceSetReadiness { handle, status } => {
            let handle = eval_handle_operand(handle, env, "service_set_readiness")?;
            let status = match eval_exec_operand(status, env)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "service_set_readiness expects u32 status, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_service_set_readiness(
                handle, status,
            )?))
        }
        LoweredExecExpr::ServiceSetDegraded { handle, degraded } => {
            let handle = eval_handle_operand(handle, env, "service_set_degraded")?;
            let degraded = match eval_exec_operand(degraded, env)? {
                RuntimeValue::Bool(value) => value,
                other => {
                    return Err(format!(
                        "service_set_degraded expects b1 flag, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_service_set_degraded(
                handle, degraded,
            )?))
        }
        LoweredExecExpr::ServiceDegraded { handle } => {
            let handle = eval_handle_operand(handle, env, "service_degraded")?;
            Ok(RuntimeValue::Bool(portable_service_degraded(handle)?))
        }
        LoweredExecExpr::ServiceEvent {
            handle,
            class,
            message,
        } => {
            let handle = eval_handle_operand(handle, env, "service_event")?;
            let message = match eval_exec_operand(message, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("service_event expects buf[u8], got {other:?}")),
            };
            Ok(RuntimeValue::Bool(portable_service_event(
                handle, class, &message,
            )?))
        }
        LoweredExecExpr::ServiceEventTotal { handle, class } => {
            let handle = eval_handle_operand(handle, env, "service_event_total")?;
            Ok(RuntimeValue::U32(portable_service_event_total(
                handle, class,
            )?))
        }
        LoweredExecExpr::ServiceTraceLink { trace, parent } => {
            let trace = eval_handle_operand(trace, env, "service_trace_link")?;
            let parent = eval_handle_operand(parent, env, "service_trace_link")?;
            Ok(RuntimeValue::Bool(portable_service_trace_link(
                trace, parent,
            )?))
        }
        LoweredExecExpr::ServiceTraceLinkCount { handle } => {
            let handle = eval_handle_operand(handle, env, "service_trace_link_count")?;
            Ok(RuntimeValue::U32(portable_service_trace_link_count(handle)?))
        }
        LoweredExecExpr::ServiceFailureCount {
            handle,
            class,
            value,
        } => {
            let handle = eval_handle_operand(handle, env, "service_failure_count")?;
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "service_failure_count expects u32 value, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_service_failure_count(
                handle, class, value,
            )?))
        }
        LoweredExecExpr::ServiceFailureTotal { handle, class } => {
            let handle = eval_handle_operand(handle, env, "service_failure_total")?;
            Ok(RuntimeValue::U32(portable_service_failure_total(
                handle, class,
            )?))
        }
        LoweredExecExpr::ServiceCheckpointSaveU32 { handle, key, value } => {
            let handle = eval_handle_operand(handle, env, "service_checkpoint_save_u32")?;
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "service_checkpoint_save_u32 expects u32 value, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_service_checkpoint_save_u32(
                handle, key, value,
            )?))
        }
        LoweredExecExpr::ServiceCheckpointLoadU32 { handle, key } => {
            let handle = eval_handle_operand(handle, env, "service_checkpoint_load_u32")?;
            Ok(RuntimeValue::U32(portable_service_checkpoint_load_u32(
                handle, key,
            )?))
        }
        LoweredExecExpr::ServiceCheckpointExists { handle, key } => {
            let handle = eval_handle_operand(handle, env, "service_checkpoint_exists")?;
            Ok(RuntimeValue::Bool(portable_service_checkpoint_exists(
                handle, key,
            )?))
        }
        LoweredExecExpr::ServiceMigrateDb {
            handle,
            db_handle,
            migration: _,
        } => {
            let handle = eval_handle_operand(handle, env, "service_migrate_db")?;
            let db_handle = eval_handle_operand(db_handle, env, "service_migrate_db")?;
            Ok(RuntimeValue::Bool(portable_service_migrate_db(
                handle, db_handle,
            )?))
        }
        LoweredExecExpr::ServiceRoute {
            request,
            method,
            path,
        } => {
            let request = match eval_exec_operand(request, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("service_route expects buf[u8], got {other:?}")),
            };
            Ok(RuntimeValue::Bool(portable_service_route(
                &request, method, path,
            )))
        }
        LoweredExecExpr::ServiceRequireHeader {
            request,
            name,
            value,
        } => {
            let request = match eval_exec_operand(request, env)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "service_require_header expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_service_require_header(
                &request, name, value,
            )))
        }
        LoweredExecExpr::ServiceErrorStatus { kind } => {
            Ok(RuntimeValue::U32(portable_service_error_status(kind)))
        }
        LoweredExecExpr::TlsServerConfigU32 { token: _, value } => Ok(RuntimeValue::U32(*value)),
        LoweredExecExpr::TlsServerConfigBuf { token: _, value } => {
            Ok(RuntimeValue::BufU8(value.as_bytes().to_vec()))
        }
        LoweredExecExpr::ListenerSetTimeoutMs { handle, value } => {
            let handle = eval_handle_operand(handle, env, "listener_set_timeout_ms")?;
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "listener_set_timeout_ms expects u32 timeout, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_listener_set_timeout_ms(
                handle, value,
            )?))
        }
        LoweredExecExpr::SessionSetTimeoutMs { handle, value } => {
            let handle = eval_handle_operand(handle, env, "session_set_timeout_ms")?;
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "session_set_timeout_ms expects u32 timeout, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_session_set_timeout_ms(
                handle, value,
            )?))
        }
        LoweredExecExpr::ListenerSetShutdownGraceMs { handle, value } => {
            let handle = eval_handle_operand(handle, env, "listener_set_shutdown_grace_ms")?;
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "listener_set_shutdown_grace_ms expects u32 grace, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_listener_set_shutdown_grace_ms(
                handle, value,
            )?))
        }
        LoweredExecExpr::BufParseU32 { value } => {
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("buf_parse_u32 expects buf[u8], got {other:?}")),
            };
            Ok(RuntimeValue::U32(portable_buf_parse_u32(&value)))
        }
        LoweredExecExpr::BufParseBool { value } => {
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("buf_parse_bool expects buf[u8], got {other:?}")),
            };
            Ok(RuntimeValue::Bool(portable_buf_parse_bool(&value)))
        }
        LoweredExecExpr::StrLit { literal } => Ok(RuntimeValue::BufU8(literal.as_bytes().to_vec())),
        LoweredExecExpr::StrConcat { left, right } => {
            let left = match eval_exec_operand(left, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("str_concat expects str, got {other:?}")),
            };
            let right = match eval_exec_operand(right, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("str_concat expects str, got {other:?}")),
            };
            let mut out = left;
            out.extend(right);
            Ok(RuntimeValue::BufU8(out))
        }
        LoweredExecExpr::StrFromU32 { value } => {
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::U32(value) => value,
                other => return Err(format!("str_from_u32 expects u32, got {other:?}")),
            };
            Ok(RuntimeValue::BufU8(value.to_string().into_bytes()))
        }
        LoweredExecExpr::StrFromBool { value } => {
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::Bool(value) => value,
                other => return Err(format!("str_from_bool expects b1, got {other:?}")),
            };
            Ok(RuntimeValue::BufU8(if value {
                b"true".to_vec()
            } else {
                b"false".to_vec()
            }))
        }
        LoweredExecExpr::StrEqLit { value, literal } => {
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("str_eq_lit expects str, got {other:?}")),
            };
            Ok(RuntimeValue::Bool(value == literal.as_bytes()))
        }
        LoweredExecExpr::StrToBuf { value } | LoweredExecExpr::BufToStr { value } => {
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "string/buf conversion expects buf-like value, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::BufU8(value))
        }
        LoweredExecExpr::BufHexStr { value } => {
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("buf_hex_str expects buf[u8], got {other:?}")),
            };
            Ok(RuntimeValue::BufU8(portable_buf_hex_str(&value)))
        }
        LoweredExecExpr::HttpWriteResponse {
            handle,
            status,
            body,
        } => {
            let handle = eval_handle_operand(handle, env, "http_write_response")?;
            let status = match eval_exec_operand(status, env)? {
                RuntimeValue::U32(value) => value,
                RuntimeValue::I32(value) if value >= 0 => value as u32,
                other => {
                    return Err(format!(
                        "http_write_response expects u32 status, got {other:?}"
                    ))
                }
            };
            let body = match eval_exec_operand(body, env)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "http_write_response expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_http_write_response(
                handle, status, &body,
            )?))
        }
        LoweredExecExpr::HttpWriteTextResponse {
            handle,
            status,
            body,
        } => {
            let handle = eval_handle_operand(handle, env, "http_write_text_response")?;
            let status = match eval_exec_operand(status, env)? {
                RuntimeValue::U32(value) => value,
                RuntimeValue::I32(value) if value >= 0 => value as u32,
                other => {
                    return Err(format!(
                        "http_write_text_response expects u32 status, got {other:?}"
                    ))
                }
            };
            let body = match eval_exec_operand(body, env)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "http_write_text_response expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_http_write_text_response(
                handle, status, &body,
            )?))
        }
        LoweredExecExpr::HttpWriteTextResponseCookie {
            handle,
            status,
            cookie_name,
            cookie_value,
            body,
        } => {
            let handle = eval_handle_operand(handle, env, "http_write_text_response_cookie")?;
            let status = match eval_exec_operand(status, env)? {
                RuntimeValue::U32(value) => value,
                RuntimeValue::I32(value) if value >= 0 => value as u32,
                other => {
                    return Err(format!(
                        "http_write_text_response_cookie expects u32 status, got {other:?}"
                    ))
                }
            };
            let body = match eval_exec_operand(body, env)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "http_write_text_response_cookie expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_http_write_text_response_cookie(
                handle,
                status,
                cookie_name,
                cookie_value,
                &body,
            )?))
        }
        LoweredExecExpr::HttpWriteTextResponseHeaders2 {
            handle,
            status,
            header1_name,
            header1_value,
            header2_name,
            header2_value,
            body,
        } => {
            let handle = eval_handle_operand(handle, env, "http_write_text_response_headers2")?;
            let status = match eval_exec_operand(status, env)? {
                RuntimeValue::U32(value) => value,
                RuntimeValue::I32(value) if value >= 0 => value as u32,
                other => {
                    return Err(format!(
                        "http_write_text_response_headers2 expects u32 status, got {other:?}"
                    ))
                }
            };
            let body = match eval_exec_operand(body, env)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "http_write_text_response_headers2 expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(
                portable_http_write_text_response_headers2(
                    handle,
                    status,
                    header1_name,
                    header1_value,
                    header2_name,
                    header2_value,
                    &body,
                )?,
            ))
        }
        LoweredExecExpr::HttpSessionWriteText {
            handle,
            status,
            body,
        } => {
            let handle = eval_handle_operand(handle, env, "http_session_write_text")?;
            let status = match eval_exec_operand(status, env)? {
                RuntimeValue::U32(value) => value,
                RuntimeValue::I32(value) if value >= 0 => value as u32,
                other => {
                    return Err(format!(
                        "http_session_write_text expects u32 status, got {other:?}"
                    ))
                }
            };
            let body = match eval_exec_operand(body, env)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "http_session_write_text expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_http_session_write_text(
                handle, status, &body,
            )?))
        }
        LoweredExecExpr::HttpSessionWriteTextCookie {
            handle,
            status,
            cookie_name,
            cookie_value,
            body,
        } => {
            let handle = eval_handle_operand(handle, env, "http_session_write_text_cookie")?;
            let status = match eval_exec_operand(status, env)? {
                RuntimeValue::U32(value) => value,
                RuntimeValue::I32(value) if value >= 0 => value as u32,
                other => {
                    return Err(format!(
                        "http_session_write_text_cookie expects u32 status, got {other:?}"
                    ))
                }
            };
            let body = match eval_exec_operand(body, env)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "http_session_write_text_cookie expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_http_session_write_text_cookie(
                handle,
                status,
                cookie_name,
                cookie_value,
                &body,
            )?))
        }
        LoweredExecExpr::HttpSessionWriteTextHeaders2 {
            handle,
            status,
            header1_name,
            header1_value,
            header2_name,
            header2_value,
            body,
        } => {
            let handle = eval_handle_operand(handle, env, "http_session_write_text_headers2")?;
            let status = match eval_exec_operand(status, env)? {
                RuntimeValue::U32(value) => value,
                RuntimeValue::I32(value) if value >= 0 => value as u32,
                other => {
                    return Err(format!(
                        "http_session_write_text_headers2 expects u32 status, got {other:?}"
                    ))
                }
            };
            let body = match eval_exec_operand(body, env)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "http_session_write_text_headers2 expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(
                portable_http_session_write_text_headers2(
                    handle,
                    status,
                    header1_name,
                    header1_value,
                    header2_name,
                    header2_value,
                    &body,
                )?,
            ))
        }
        LoweredExecExpr::HttpWriteJsonResponse {
            handle,
            status,
            body,
        } => {
            let handle = eval_handle_operand(handle, env, "http_write_json_response")?;
            let status = match eval_exec_operand(status, env)? {
                RuntimeValue::U32(value) => value,
                RuntimeValue::I32(value) if value >= 0 => value as u32,
                other => {
                    return Err(format!(
                        "http_write_json_response expects u32 status, got {other:?}"
                    ))
                }
            };
            let body = match eval_exec_operand(body, env)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "http_write_json_response expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_http_write_json_response(
                handle, status, &body,
            )?))
        }
        LoweredExecExpr::HttpWriteJsonResponseCookie {
            handle,
            status,
            cookie_name,
            cookie_value,
            body,
        } => {
            let handle = eval_handle_operand(handle, env, "http_write_json_response_cookie")?;
            let status = match eval_exec_operand(status, env)? {
                RuntimeValue::U32(value) => value,
                RuntimeValue::I32(value) if value >= 0 => value as u32,
                other => {
                    return Err(format!(
                        "http_write_json_response_cookie expects u32 status, got {other:?}"
                    ))
                }
            };
            let body = match eval_exec_operand(body, env)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "http_write_json_response_cookie expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_http_write_json_response_cookie(
                handle,
                status,
                cookie_name,
                cookie_value,
                &body,
            )?))
        }
        LoweredExecExpr::HttpWriteJsonResponseHeaders2 {
            handle,
            status,
            header1_name,
            header1_value,
            header2_name,
            header2_value,
            body,
        } => {
            let handle = eval_handle_operand(handle, env, "http_write_json_response_headers2")?;
            let status = match eval_exec_operand(status, env)? {
                RuntimeValue::U32(value) => value,
                RuntimeValue::I32(value) if value >= 0 => value as u32,
                other => {
                    return Err(format!(
                        "http_write_json_response_headers2 expects u32 status, got {other:?}"
                    ))
                }
            };
            let body = match eval_exec_operand(body, env)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "http_write_json_response_headers2 expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(
                portable_http_write_json_response_headers2(
                    handle,
                    status,
                    header1_name,
                    header1_value,
                    header2_name,
                    header2_value,
                    &body,
                )?,
            ))
        }
        LoweredExecExpr::HttpSessionWriteJson {
            handle,
            status,
            body,
        } => {
            let handle = eval_handle_operand(handle, env, "http_session_write_json")?;
            let status = match eval_exec_operand(status, env)? {
                RuntimeValue::U32(value) => value,
                RuntimeValue::I32(value) if value >= 0 => value as u32,
                other => {
                    return Err(format!(
                        "http_session_write_json expects u32 status, got {other:?}"
                    ))
                }
            };
            let body = match eval_exec_operand(body, env)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "http_session_write_json expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_http_session_write_json(
                handle, status, &body,
            )?))
        }
        LoweredExecExpr::HttpSessionWriteJsonCookie {
            handle,
            status,
            cookie_name,
            cookie_value,
            body,
        } => {
            let handle = eval_handle_operand(handle, env, "http_session_write_json_cookie")?;
            let status = match eval_exec_operand(status, env)? {
                RuntimeValue::U32(value) => value,
                RuntimeValue::I32(value) if value >= 0 => value as u32,
                other => {
                    return Err(format!(
                        "http_session_write_json_cookie expects u32 status, got {other:?}"
                    ))
                }
            };
            let body = match eval_exec_operand(body, env)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "http_session_write_json_cookie expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_http_session_write_json_cookie(
                handle,
                status,
                cookie_name,
                cookie_value,
                &body,
            )?))
        }
        LoweredExecExpr::HttpSessionWriteJsonHeaders2 {
            handle,
            status,
            header1_name,
            header1_value,
            header2_name,
            header2_value,
            body,
        } => {
            let handle = eval_handle_operand(handle, env, "http_session_write_json_headers2")?;
            let status = match eval_exec_operand(status, env)? {
                RuntimeValue::U32(value) => value,
                RuntimeValue::I32(value) if value >= 0 => value as u32,
                other => {
                    return Err(format!(
                        "http_session_write_json_headers2 expects u32 status, got {other:?}"
                    ))
                }
            };
            let body = match eval_exec_operand(body, env)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "http_session_write_json_headers2 expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(
                portable_http_session_write_json_headers2(
                    handle,
                    status,
                    header1_name,
                    header1_value,
                    header2_name,
                    header2_value,
                    &body,
                )?,
            ))
        }
        LoweredExecExpr::HttpWriteResponseHeader {
            handle,
            status,
            header_name,
            header_value,
            body,
        } => {
            let handle = eval_handle_operand(handle, env, "http_write_response_header")?;
            let status = match eval_exec_operand(status, env)? {
                RuntimeValue::U32(value) => value,
                RuntimeValue::I32(value) if value >= 0 => value as u32,
                other => {
                    return Err(format!(
                        "http_write_response_header expects u32 status, got {other:?}"
                    ))
                }
            };
            let body = match eval_exec_operand(body, env)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "http_write_response_header expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_http_write_response_header(
                handle,
                status,
                header_name,
                header_value,
                &body,
            )?))
        }
        LoweredExecExpr::JsonGetU32 { value, key } => {
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("json_get_u32 expects buf[u8], got {other:?}")),
            };
            Ok(RuntimeValue::U32(portable_json_get_u32(&value, key)))
        }
        LoweredExecExpr::JsonGetBool { value, key } => {
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("json_get_bool expects buf[u8], got {other:?}")),
            };
            Ok(RuntimeValue::Bool(portable_json_get_bool(&value, key)))
        }
        LoweredExecExpr::JsonHasKey { value, key } => {
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("json_has_key expects buf[u8], got {other:?}")),
            };
            Ok(RuntimeValue::Bool(portable_json_has_key(&value, key)))
        }
        LoweredExecExpr::JsonGetBufU8 { value, key } => {
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("json_get_buf expects buf[u8], got {other:?}")),
            };
            Ok(RuntimeValue::BufU8(portable_json_get_buf(&value, key)))
        }
        LoweredExecExpr::JsonGetStr { value, key } => {
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "json_get_str expects string/json bytes, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::BufU8(portable_json_get_str(&value, key)))
        }
        LoweredExecExpr::JsonGetU32Or {
            value,
            key,
            default_value,
        } => {
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("json_get_u32_or expects buf[u8], got {other:?}")),
            };
            let default_value = match eval_exec_operand(default_value, env)? {
                RuntimeValue::U32(value) => value,
                other => return Err(format!("json_get_u32_or expects u32 default, got {other:?}")),
            };
            Ok(RuntimeValue::U32(portable_json_get_u32_or(
                &value,
                key,
                default_value,
            )))
        }
        LoweredExecExpr::JsonGetBoolOr {
            value,
            key,
            default_value,
        } => {
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("json_get_bool_or expects buf[u8], got {other:?}")),
            };
            let default_value = match eval_exec_operand(default_value, env)? {
                RuntimeValue::Bool(value) => value,
                other => {
                    return Err(format!("json_get_bool_or expects b1 default, got {other:?}"))
                }
            };
            Ok(RuntimeValue::Bool(portable_json_get_bool_or(
                &value,
                key,
                default_value,
            )))
        }
        LoweredExecExpr::JsonGetBufOr {
            value,
            key,
            default_value,
        } => {
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("json_get_buf_or expects buf[u8], got {other:?}")),
            };
            let default_value = match eval_exec_operand(default_value, env)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!("json_get_buf_or expects buf[u8] default, got {other:?}"))
                }
            };
            Ok(RuntimeValue::BufU8(portable_json_get_buf_or(
                &value,
                key,
                &default_value,
            )))
        }
        LoweredExecExpr::JsonGetStrOr {
            value,
            key,
            default_value,
        } => {
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("json_get_str_or expects buf[u8], got {other:?}")),
            };
            let default_value = match eval_exec_operand(default_value, env)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!("json_get_str_or expects str default, got {other:?}"))
                }
            };
            Ok(RuntimeValue::BufU8(portable_json_get_str_or(
                &value,
                key,
                &default_value,
            )))
        }
        LoweredExecExpr::JsonArrayLen { value } => {
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "json_array_len expects string/json bytes, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U32(portable_json_array_len(&value)))
        }
        LoweredExecExpr::JsonIndexU32 { value, index } => {
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "json_index_u32 expects string/json bytes, got {other:?}"
                    ))
                }
            };
            let index = match eval_exec_operand(index, env)? {
                RuntimeValue::U32(value) => value,
                other => return Err(format!("json_index_u32 expects u32 index, got {other:?}")),
            };
            Ok(RuntimeValue::U32(portable_json_index_u32(&value, index)))
        }
        LoweredExecExpr::JsonIndexBool { value, index } => {
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "json_index_bool expects string/json bytes, got {other:?}"
                    ))
                }
            };
            let index = match eval_exec_operand(index, env)? {
                RuntimeValue::U32(value) => value,
                other => return Err(format!("json_index_bool expects u32 index, got {other:?}")),
            };
            Ok(RuntimeValue::Bool(portable_json_index_bool(&value, index)))
        }
        LoweredExecExpr::JsonIndexStr { value, index } => {
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "json_index_str expects string/json bytes, got {other:?}"
                    ))
                }
            };
            let index = match eval_exec_operand(index, env)? {
                RuntimeValue::U32(value) => value,
                other => return Err(format!("json_index_str expects u32 index, got {other:?}")),
            };
            Ok(RuntimeValue::BufU8(portable_json_index_str(&value, index)))
        }
        LoweredExecExpr::JsonEncodeObj { entries } => {
            let mut out = Vec::new();
            for (key, operand) in entries {
                out.push((key.clone(), eval_exec_operand(operand, env)?));
            }
            Ok(RuntimeValue::BufU8(portable_json_encode_object(&out)))
        }
        LoweredExecExpr::JsonEncodeArr { values } => {
            let mut out = Vec::new();
            for operand in values {
                out.push(eval_exec_operand(operand, env)?);
            }
            Ok(RuntimeValue::BufU8(portable_json_encode_array(&out)))
        }
        LoweredExecExpr::ConfigGetU32 { key: _, value } => Ok(RuntimeValue::U32(*value)),
        LoweredExecExpr::ConfigGetBool { key: _, value } => Ok(RuntimeValue::Bool(*value)),
        LoweredExecExpr::ConfigGetStr { key: _, value } => {
            Ok(RuntimeValue::BufU8(value.as_bytes().to_vec()))
        }
        LoweredExecExpr::ConfigHas { key: _, present } => Ok(RuntimeValue::Bool(*present)),
        LoweredExecExpr::EnvGetU32 { key } => Ok(RuntimeValue::U32(portable_env_get_u32(key))),
        LoweredExecExpr::EnvGetBool { key } => Ok(RuntimeValue::Bool(portable_env_get_bool(key))),
        LoweredExecExpr::EnvGetStr { key } => Ok(RuntimeValue::BufU8(portable_env_get_str(key))),
        LoweredExecExpr::EnvHas { key } => Ok(RuntimeValue::Bool(portable_env_has(key))),
        LoweredExecExpr::BufBeforeLit { value, literal } => {
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("buf_before_lit expects buf[u8], got {other:?}")),
            };
            Ok(RuntimeValue::BufU8(portable_buf_before_lit(&value, literal)))
        }
        LoweredExecExpr::BufAfterLit { value, literal } => {
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("buf_after_lit expects buf[u8], got {other:?}")),
            };
            Ok(RuntimeValue::BufU8(portable_buf_after_lit(&value, literal)))
        }
        LoweredExecExpr::BufTrimAscii { value } => {
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("buf_trim_ascii expects buf[u8], got {other:?}")),
            };
            Ok(RuntimeValue::BufU8(portable_buf_trim_ascii(&value)))
        }
        LoweredExecExpr::DateParseYmd { value } => {
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("date_parse_ymd expects str, got {other:?}")),
            };
            Ok(RuntimeValue::U32(portable_date_parse_ymd(&value)))
        }
        LoweredExecExpr::TimeParseHms { value } => {
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("time_parse_hms expects str, got {other:?}")),
            };
            Ok(RuntimeValue::U32(portable_time_parse_hms(&value)))
        }
        LoweredExecExpr::DateFormatYmd { value } => {
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::U32(value) => value,
                other => return Err(format!("date_format_ymd expects u32, got {other:?}")),
            };
            Ok(RuntimeValue::BufU8(portable_date_format_ymd(value)))
        }
        LoweredExecExpr::TimeFormatHms { value } => {
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::U32(value) => value,
                other => return Err(format!("time_format_hms expects u32, got {other:?}")),
            };
            Ok(RuntimeValue::BufU8(portable_time_format_hms(value)))
        }
        LoweredExecExpr::DbOpen { path } => Ok(RuntimeValue::U64(portable_db_open(path)?)),
        LoweredExecExpr::DbClose { handle } => {
            let handle = eval_handle_operand(handle, env, "db_close")?;
            Ok(RuntimeValue::Bool(portable_db_close(handle)?))
        }
        LoweredExecExpr::DbExec { handle, sql } => {
            let handle = eval_handle_operand(handle, env, "db_exec")?;
            let sql = match eval_exec_operand(sql, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("db_exec expects buf[u8], got {other:?}")),
            };
            Ok(RuntimeValue::Bool(portable_db_exec(handle, &sql)?))
        }
        LoweredExecExpr::DbPrepare { handle, name, sql } => {
            let handle = eval_handle_operand(handle, env, "db_prepare")?;
            let sql = match eval_exec_operand(sql, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("db_prepare expects buf[u8], got {other:?}")),
            };
            Ok(RuntimeValue::Bool(portable_db_prepare(handle, name, &sql)?))
        }
        LoweredExecExpr::DbExecPrepared {
            handle,
            name,
            params,
        } => {
            let handle = eval_handle_operand(handle, env, "db_exec_prepared")?;
            let params = match eval_exec_operand(params, env)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "db_exec_prepared expects buf[u8] params, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_db_exec_prepared(
                handle, name, &params,
            )?))
        }
        LoweredExecExpr::DbQueryU32 { handle, sql } => {
            let handle = eval_handle_operand(handle, env, "db_query_u32")?;
            let sql = match eval_exec_operand(sql, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("db_query_u32 expects buf[u8], got {other:?}")),
            };
            Ok(RuntimeValue::U32(portable_db_query_u32(handle, &sql)?))
        }
        LoweredExecExpr::DbQueryBufU8 { handle, sql } => {
            let handle = eval_handle_operand(handle, env, "db_query_buf")?;
            let sql = match eval_exec_operand(sql, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("db_query_buf expects buf[u8], got {other:?}")),
            };
            Ok(RuntimeValue::BufU8(portable_db_query_buf(handle, &sql)?))
        }
        LoweredExecExpr::DbQueryRow { handle, sql } => {
            let handle = eval_handle_operand(handle, env, "db_query_row")?;
            let sql = match eval_exec_operand(sql, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("db_query_row expects buf[u8], got {other:?}")),
            };
            Ok(RuntimeValue::BufU8(portable_db_query_row(handle, &sql)?))
        }
        LoweredExecExpr::DbQueryPreparedU32 {
            handle,
            name,
            params,
        } => {
            let handle = eval_handle_operand(handle, env, "db_query_prepared_u32")?;
            let params = match eval_exec_operand(params, env)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "db_query_prepared_u32 expects buf[u8] params, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U32(portable_db_query_prepared_u32(
                handle, name, &params,
            )?))
        }
        LoweredExecExpr::DbQueryPreparedBufU8 {
            handle,
            name,
            params,
        } => {
            let handle = eval_handle_operand(handle, env, "db_query_prepared_buf")?;
            let params = match eval_exec_operand(params, env)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "db_query_prepared_buf expects buf[u8] params, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::BufU8(portable_db_query_prepared_buf(
                handle, name, &params,
            )?))
        }
        LoweredExecExpr::DbQueryPreparedRow {
            handle,
            name,
            params,
        } => {
            let handle = eval_handle_operand(handle, env, "db_query_prepared_row")?;
            let params = match eval_exec_operand(params, env)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "db_query_prepared_row expects buf[u8] params, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::BufU8(portable_db_query_prepared_row(
                handle, name, &params,
            )?))
        }
        LoweredExecExpr::DbRowFound { row } => {
            let row = match eval_exec_operand(row, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("db_row_found expects buf[u8], got {other:?}")),
            };
            Ok(RuntimeValue::Bool(!row.is_empty()))
        }
        LoweredExecExpr::DbLastErrorCode { handle } => {
            let handle = eval_handle_operand(handle, env, "db_last_error_code")?;
            Ok(RuntimeValue::U32(portable_db_last_error_code(handle)?))
        }
        LoweredExecExpr::DbLastErrorRetryable { handle } => {
            let handle = eval_handle_operand(handle, env, "db_last_error_retryable")?;
            Ok(RuntimeValue::Bool(portable_db_last_error_retryable(handle)?))
        }
        LoweredExecExpr::DbBegin { handle } => {
            let handle = eval_handle_operand(handle, env, "db_begin")?;
            Ok(RuntimeValue::Bool(portable_db_begin(handle)?))
        }
        LoweredExecExpr::DbCommit { handle } => {
            let handle = eval_handle_operand(handle, env, "db_commit")?;
            Ok(RuntimeValue::Bool(portable_db_commit(handle)?))
        }
        LoweredExecExpr::DbRollback { handle } => {
            let handle = eval_handle_operand(handle, env, "db_rollback")?;
            Ok(RuntimeValue::Bool(portable_db_rollback(handle)?))
        }
        LoweredExecExpr::DbPoolOpen { target, max_size } => {
            let max_size = match eval_exec_operand(max_size, env)? {
                RuntimeValue::U32(value) => value,
                other => return Err(format!("db_pool_open expects u32 max size, got {other:?}")),
            };
            Ok(RuntimeValue::U64(portable_db_pool_open(target, max_size)?))
        }
        LoweredExecExpr::DbPoolSetMaxIdle { pool, value } => {
            let pool = eval_handle_operand(pool, env, "db_pool_set_max_idle")?;
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "db_pool_set_max_idle expects u32 max idle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_db_pool_set_max_idle(pool, value)?))
        }
        LoweredExecExpr::DbPoolLeased { pool } => {
            let pool = eval_handle_operand(pool, env, "db_pool_leased")?;
            Ok(RuntimeValue::U32(portable_db_pool_leased(pool)?))
        }
        LoweredExecExpr::DbPoolAcquire { pool } => {
            let pool = eval_handle_operand(pool, env, "db_pool_acquire")?;
            Ok(RuntimeValue::U64(portable_db_pool_acquire(pool)?))
        }
        LoweredExecExpr::DbPoolRelease { pool, handle } => {
            let pool = eval_handle_operand(pool, env, "db_pool_release")?;
            let handle = eval_handle_operand(handle, env, "db_pool_release")?;
            Ok(RuntimeValue::Bool(portable_db_pool_release(pool, handle)?))
        }
        LoweredExecExpr::DbPoolClose { pool } => {
            let pool = eval_handle_operand(pool, env, "db_pool_close")?;
            Ok(RuntimeValue::Bool(portable_db_pool_close(pool)?))
        }
        LoweredExecExpr::CacheOpen { target } => {
            Ok(RuntimeValue::U64(portable_cache_open(target)?))
        }
        LoweredExecExpr::CacheClose { handle } => {
            let handle = eval_handle_operand(handle, env, "cache_close")?;
            Ok(RuntimeValue::Bool(portable_cache_close(handle)?))
        }
        LoweredExecExpr::CacheGetBufU8 { handle, key } => {
            let handle = eval_handle_operand(handle, env, "cache_get_buf")?;
            let key = match eval_exec_operand(key, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("cache_get_buf expects buf[u8] key, got {other:?}")),
            };
            Ok(RuntimeValue::BufU8(portable_cache_get_buf(handle, &key)?))
        }
        LoweredExecExpr::CacheSetBufU8 { handle, key, value } => {
            let handle = eval_handle_operand(handle, env, "cache_set_buf")?;
            let key = match eval_exec_operand(key, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("cache_set_buf expects buf[u8] key, got {other:?}")),
            };
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!("cache_set_buf expects buf[u8] value, got {other:?}"))
                }
            };
            Ok(RuntimeValue::Bool(portable_cache_set_buf(handle, &key, &value, None)?))
        }
        LoweredExecExpr::CacheSetBufTtlU8 {
            handle,
            key,
            ttl_ms,
            value,
        } => {
            let handle = eval_handle_operand(handle, env, "cache_set_buf_ttl")?;
            let key = match eval_exec_operand(key, env)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!("cache_set_buf_ttl expects buf[u8] key, got {other:?}"))
                }
            };
            let ttl_ms = match eval_exec_operand(ttl_ms, env)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!("cache_set_buf_ttl expects u32 ttl, got {other:?}"))
                }
            };
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!("cache_set_buf_ttl expects buf[u8] value, got {other:?}"))
                }
            };
            Ok(RuntimeValue::Bool(portable_cache_set_buf(
                handle,
                &key,
                &value,
                Some(ttl_ms),
            )?))
        }
        LoweredExecExpr::CacheDel { handle, key } => {
            let handle = eval_handle_operand(handle, env, "cache_del")?;
            let key = match eval_exec_operand(key, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("cache_del expects buf[u8] key, got {other:?}")),
            };
            Ok(RuntimeValue::Bool(portable_cache_del(handle, &key)?))
        }
        LoweredExecExpr::QueueOpen { target } => {
            Ok(RuntimeValue::U64(portable_queue_open(target)?))
        }
        LoweredExecExpr::QueueClose { handle } => {
            let handle = eval_handle_operand(handle, env, "queue_close")?;
            Ok(RuntimeValue::Bool(portable_queue_close(handle)?))
        }
        LoweredExecExpr::QueuePushBufU8 { handle, value } => {
            let handle = eval_handle_operand(handle, env, "queue_push_buf")?;
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!("queue_push_buf expects buf[u8] payload, got {other:?}"))
                }
            };
            Ok(RuntimeValue::Bool(portable_queue_push_buf(handle, &value)?))
        }
        LoweredExecExpr::QueuePopBufU8 { handle } => {
            let handle = eval_handle_operand(handle, env, "queue_pop_buf")?;
            Ok(RuntimeValue::BufU8(portable_queue_pop_buf(handle)?))
        }
        LoweredExecExpr::QueueLen { handle } => {
            let handle = eval_handle_operand(handle, env, "queue_len")?;
            Ok(RuntimeValue::U32(portable_queue_len(handle)?))
        }
        LoweredExecExpr::StreamOpen { target } => {
            Ok(RuntimeValue::U64(portable_stream_open(target)?))
        }
        LoweredExecExpr::StreamClose { handle } => {
            let handle = eval_handle_operand(handle, env, "stream_close")?;
            Ok(RuntimeValue::Bool(portable_stream_close(handle)?))
        }
        LoweredExecExpr::StreamPublishBufU8 { handle, value } => {
            let handle = eval_handle_operand(handle, env, "stream_publish_buf")?;
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "stream_publish_buf expects buf[u8] payload, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U32(portable_stream_publish_buf(handle, &value)?))
        }
        LoweredExecExpr::StreamLen { handle } => {
            let handle = eval_handle_operand(handle, env, "stream_len")?;
            Ok(RuntimeValue::U32(portable_stream_len(handle)?))
        }
        LoweredExecExpr::StreamReplayOpen { handle, from_offset } => {
            let handle = eval_handle_operand(handle, env, "stream_replay_open")?;
            let from_offset = match eval_exec_operand(from_offset, env)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "stream_replay_open expects u32 offset, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U64(portable_stream_replay_open(
                handle,
                from_offset,
            )?))
        }
        LoweredExecExpr::StreamReplayNextU8 { handle } => {
            let handle = eval_handle_operand(handle, env, "stream_replay_next")?;
            Ok(RuntimeValue::BufU8(portable_stream_replay_next(handle)?))
        }
        LoweredExecExpr::StreamReplayOffset { handle } => {
            let handle = eval_handle_operand(handle, env, "stream_replay_offset")?;
            Ok(RuntimeValue::U32(portable_stream_replay_offset(handle)?))
        }
        LoweredExecExpr::StreamReplayClose { handle } => {
            let handle = eval_handle_operand(handle, env, "stream_replay_close")?;
            Ok(RuntimeValue::Bool(portable_stream_replay_close(handle)?))
        }
        LoweredExecExpr::ShardRouteU32 { key, shard_count } => {
            let key = match eval_exec_operand(key, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("shard_route_u32 expects buf[u8] key, got {other:?}")),
            };
            let shard_count = match eval_exec_operand(shard_count, env)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "shard_route_u32 expects u32 shard count, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U32(portable_shard_route_u32(&key, shard_count)?))
        }
        LoweredExecExpr::LeaseOpen { target } => Ok(RuntimeValue::U64(portable_lease_open(target)?)),
        LoweredExecExpr::LeaseAcquire { handle, owner } => {
            let handle = eval_handle_operand(handle, env, "lease_acquire")?;
            let owner = match eval_exec_operand(owner, env)? {
                RuntimeValue::U32(value) => value,
                other => return Err(format!("lease_acquire expects u32 owner, got {other:?}")),
            };
            Ok(RuntimeValue::Bool(portable_lease_acquire(handle, owner)?))
        }
        LoweredExecExpr::LeaseOwner { handle } => {
            let handle = eval_handle_operand(handle, env, "lease_owner")?;
            Ok(RuntimeValue::U32(portable_lease_owner(handle)?))
        }
        LoweredExecExpr::LeaseTransfer { handle, owner } => {
            let handle = eval_handle_operand(handle, env, "lease_transfer")?;
            let owner = match eval_exec_operand(owner, env)? {
                RuntimeValue::U32(value) => value,
                other => return Err(format!("lease_transfer expects u32 owner, got {other:?}")),
            };
            Ok(RuntimeValue::Bool(portable_lease_transfer(handle, owner)?))
        }
        LoweredExecExpr::LeaseRelease { handle, owner } => {
            let handle = eval_handle_operand(handle, env, "lease_release")?;
            let owner = match eval_exec_operand(owner, env)? {
                RuntimeValue::U32(value) => value,
                other => return Err(format!("lease_release expects u32 owner, got {other:?}")),
            };
            Ok(RuntimeValue::Bool(portable_lease_release(handle, owner)?))
        }
        LoweredExecExpr::LeaseClose { handle } => {
            let handle = eval_handle_operand(handle, env, "lease_close")?;
            Ok(RuntimeValue::Bool(portable_lease_close(handle)?))
        }
        LoweredExecExpr::PlacementOpen { target } => {
            Ok(RuntimeValue::U64(portable_placement_open(target)?))
        }
        LoweredExecExpr::PlacementAssign { handle, shard, node } => {
            let handle = eval_handle_operand(handle, env, "placement_assign")?;
            let shard = match eval_exec_operand(shard, env)? {
                RuntimeValue::U32(value) => value,
                other => return Err(format!("placement_assign expects u32 shard, got {other:?}")),
            };
            let node = match eval_exec_operand(node, env)? {
                RuntimeValue::U32(value) => value,
                other => return Err(format!("placement_assign expects u32 node, got {other:?}")),
            };
            Ok(RuntimeValue::Bool(portable_placement_assign(handle, shard, node)?))
        }
        LoweredExecExpr::PlacementLookup { handle, shard } => {
            let handle = eval_handle_operand(handle, env, "placement_lookup")?;
            let shard = match eval_exec_operand(shard, env)? {
                RuntimeValue::U32(value) => value,
                other => return Err(format!("placement_lookup expects u32 shard, got {other:?}")),
            };
            Ok(RuntimeValue::U32(portable_placement_lookup(handle, shard)?))
        }
        LoweredExecExpr::PlacementClose { handle } => {
            let handle = eval_handle_operand(handle, env, "placement_close")?;
            Ok(RuntimeValue::Bool(portable_placement_close(handle)?))
        }
        LoweredExecExpr::CoordOpen { target } => Ok(RuntimeValue::U64(portable_coord_open(target)?)),
        LoweredExecExpr::CoordStoreU32 { handle, key, value } => {
            let handle = eval_handle_operand(handle, env, "coord_store_u32")?;
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::U32(value) => value,
                other => return Err(format!("coord_store_u32 expects u32 value, got {other:?}")),
            };
            Ok(RuntimeValue::Bool(portable_coord_store_u32(handle, key, value)?))
        }
        LoweredExecExpr::CoordLoadU32 { handle, key } => {
            let handle = eval_handle_operand(handle, env, "coord_load_u32")?;
            Ok(RuntimeValue::U32(portable_coord_load_u32(handle, key)?))
        }
        LoweredExecExpr::CoordClose { handle } => {
            let handle = eval_handle_operand(handle, env, "coord_close")?;
            Ok(RuntimeValue::Bool(portable_coord_close(handle)?))
        }
        LoweredExecExpr::BatchOpen => Ok(RuntimeValue::U64(portable_batch_open()?)),
        LoweredExecExpr::BatchPushU64 { handle, value } => {
            let handle = eval_handle_operand(handle, env, "batch_push_u64")?;
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::U64(value) => value,
                other => return Err(format!("batch_push_u64 expects u64, got {other:?}")),
            };
            Ok(RuntimeValue::Bool(portable_batch_push_u64(handle, value)?))
        }
        LoweredExecExpr::BatchLen { handle } => {
            let handle = eval_handle_operand(handle, env, "batch_len")?;
            Ok(RuntimeValue::U32(portable_batch_len(handle)?))
        }
        LoweredExecExpr::BatchFlushSumU64 { handle } => {
            let handle = eval_handle_operand(handle, env, "batch_flush_sum_u64")?;
            Ok(RuntimeValue::U64(portable_batch_flush_sum_u64(handle)?))
        }
        LoweredExecExpr::BatchClose { handle } => {
            let handle = eval_handle_operand(handle, env, "batch_close")?;
            Ok(RuntimeValue::Bool(portable_batch_close(handle)?))
        }
        LoweredExecExpr::AggOpenU64 => Ok(RuntimeValue::U64(portable_agg_open_u64()?)),
        LoweredExecExpr::AggAddU64 { handle, value } => {
            let handle = eval_handle_operand(handle, env, "agg_add_u64")?;
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::U64(value) => value,
                other => return Err(format!("agg_add_u64 expects u64, got {other:?}")),
            };
            Ok(RuntimeValue::Bool(portable_agg_add_u64(handle, value)?))
        }
        LoweredExecExpr::AggCount { handle } => {
            let handle = eval_handle_operand(handle, env, "agg_count")?;
            Ok(RuntimeValue::U32(portable_agg_count(handle)?))
        }
        LoweredExecExpr::AggSumU64 { handle } => {
            let handle = eval_handle_operand(handle, env, "agg_sum_u64")?;
            Ok(RuntimeValue::U64(portable_agg_sum_u64(handle)?))
        }
        LoweredExecExpr::AggAvgU64 { handle } => {
            let handle = eval_handle_operand(handle, env, "agg_avg_u64")?;
            Ok(RuntimeValue::U64(portable_agg_avg_u64(handle)?))
        }
        LoweredExecExpr::AggMinU64 { handle } => {
            let handle = eval_handle_operand(handle, env, "agg_min_u64")?;
            Ok(RuntimeValue::U64(portable_agg_min_u64(handle)?))
        }
        LoweredExecExpr::AggMaxU64 { handle } => {
            let handle = eval_handle_operand(handle, env, "agg_max_u64")?;
            Ok(RuntimeValue::U64(portable_agg_max_u64(handle)?))
        }
        LoweredExecExpr::AggClose { handle } => {
            let handle = eval_handle_operand(handle, env, "agg_close")?;
            Ok(RuntimeValue::Bool(portable_agg_close(handle)?))
        }
        LoweredExecExpr::WindowOpenMs { width_ms } => {
            let width_ms = match eval_exec_operand(width_ms, env)? {
                RuntimeValue::U32(value) => value,
                other => return Err(format!("window_open_ms expects u32, got {other:?}")),
            };
            Ok(RuntimeValue::U64(portable_window_open_ms(width_ms)?))
        }
        LoweredExecExpr::WindowAddU64 { handle, value } => {
            let handle = eval_handle_operand(handle, env, "window_add_u64")?;
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::U64(value) => value,
                other => return Err(format!("window_add_u64 expects u64, got {other:?}")),
            };
            Ok(RuntimeValue::Bool(portable_window_add_u64(handle, value)?))
        }
        LoweredExecExpr::WindowCount { handle } => {
            let handle = eval_handle_operand(handle, env, "window_count")?;
            Ok(RuntimeValue::U32(portable_window_count(handle)?))
        }
        LoweredExecExpr::WindowSumU64 { handle } => {
            let handle = eval_handle_operand(handle, env, "window_sum_u64")?;
            Ok(RuntimeValue::U64(portable_window_sum_u64(handle)?))
        }
        LoweredExecExpr::WindowAvgU64 { handle } => {
            let handle = eval_handle_operand(handle, env, "window_avg_u64")?;
            Ok(RuntimeValue::U64(portable_window_avg_u64(handle)?))
        }
        LoweredExecExpr::WindowMinU64 { handle } => {
            let handle = eval_handle_operand(handle, env, "window_min_u64")?;
            Ok(RuntimeValue::U64(portable_window_min_u64(handle)?))
        }
        LoweredExecExpr::WindowMaxU64 { handle } => {
            let handle = eval_handle_operand(handle, env, "window_max_u64")?;
            Ok(RuntimeValue::U64(portable_window_max_u64(handle)?))
        }
        LoweredExecExpr::WindowClose { handle } => {
            let handle = eval_handle_operand(handle, env, "window_close")?;
            Ok(RuntimeValue::Bool(portable_window_close(handle)?))
        }
        LoweredExecExpr::TlsExchangeAllU8 { host, port, value } => {
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("tls_exchange_all expects buf[u8], got {other:?}")),
            };
            Ok(RuntimeValue::BufU8(portable_tls_exchange_all(
                host, *port, &value,
            )?))
        }
        LoweredExecExpr::RtOpen { workers } => {
            let workers = match eval_exec_operand(workers, env)? {
                RuntimeValue::U32(value) => value,
                other => return Err(format!("rt_open expects u32, got {other:?}")),
            };
            Ok(RuntimeValue::U64(portable_rt_open(workers)?))
        }
        LoweredExecExpr::RtSpawnU32 {
            runtime,
            function,
            arg,
        } => {
            let runtime = eval_handle_operand(runtime, env, "rt_spawn_u32")?;
            let arg = match eval_exec_operand(arg, env)? {
                RuntimeValue::U32(value) => value,
                other => return Err(format!("rt_spawn_u32 expects u32 arg, got {other:?}")),
            };
            Ok(RuntimeValue::U64(portable_rt_spawn_u32(
                runtime, function, arg,
            )?))
        }
        LoweredExecExpr::RtSpawnBufU8 {
            runtime,
            function,
            arg,
        } => {
            let runtime = eval_handle_operand(runtime, env, "rt_spawn_buf")?;
            let arg = match eval_exec_operand(arg, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("rt_spawn_buf expects buf[u8] arg, got {other:?}")),
            };
            Ok(RuntimeValue::U64(portable_rt_spawn_buf(
                runtime, function, arg,
            )?))
        }
        LoweredExecExpr::RtTrySpawnU32 {
            runtime,
            function,
            arg,
        } => {
            let runtime = eval_handle_operand(runtime, env, "rt_try_spawn_u32")?;
            let arg = match eval_exec_operand(arg, env)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!("rt_try_spawn_u32 expects u32 arg, got {other:?}"))
                }
            };
            Ok(RuntimeValue::U64(portable_rt_try_spawn_u32(
                runtime, function, arg,
            )?))
        }
        LoweredExecExpr::RtTrySpawnBufU8 {
            runtime,
            function,
            arg,
        } => {
            let runtime = eval_handle_operand(runtime, env, "rt_try_spawn_buf")?;
            let arg = match eval_exec_operand(arg, env)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!("rt_try_spawn_buf expects buf[u8] arg, got {other:?}"))
                }
            };
            Ok(RuntimeValue::U64(portable_rt_try_spawn_buf(
                runtime, function, arg,
            )?))
        }
        LoweredExecExpr::RtDone { task } => {
            let task = eval_handle_operand(task, env, "rt_done")?;
            Ok(RuntimeValue::Bool(portable_rt_done(task)?))
        }
        LoweredExecExpr::RtJoinU32 { task } => {
            let task = eval_handle_operand(task, env, "rt_join_u32")?;
            Ok(RuntimeValue::U32(portable_rt_join_u32(task)?))
        }
        LoweredExecExpr::RtJoinBufU8 { task } => {
            let task = eval_handle_operand(task, env, "rt_join_buf")?;
            Ok(RuntimeValue::BufU8(portable_rt_join_buf(task)?))
        }
        LoweredExecExpr::RtCancel { task } => {
            let task = eval_handle_operand(task, env, "rt_cancel")?;
            Ok(RuntimeValue::Bool(portable_rt_cancel(task)?))
        }
        LoweredExecExpr::RtTaskClose { task } => {
            let task = eval_handle_operand(task, env, "rt_task_close")?;
            Ok(RuntimeValue::Bool(portable_rt_task_close(task)?))
        }
        LoweredExecExpr::RtShutdown { runtime, grace_ms } => {
            let runtime = eval_handle_operand(runtime, env, "rt_shutdown")?;
            let grace_ms = match eval_exec_operand(grace_ms, env)? {
                RuntimeValue::U32(value) => value,
                other => return Err(format!("rt_shutdown expects u32 grace, got {other:?}")),
            };
            Ok(RuntimeValue::Bool(portable_rt_shutdown(runtime, grace_ms)?))
        }
        LoweredExecExpr::RtClose { runtime } => {
            let runtime = eval_handle_operand(runtime, env, "rt_close")?;
            Ok(RuntimeValue::Bool(portable_rt_close(runtime)?))
        }
        LoweredExecExpr::RtInFlight { runtime } => {
            let runtime = eval_handle_operand(runtime, env, "rt_inflight")?;
            Ok(RuntimeValue::U32(portable_rt_inflight(runtime)?))
        }
        LoweredExecExpr::RtCancelled => Ok(RuntimeValue::Bool(portable_rt_cancelled())),
        LoweredExecExpr::ChanOpenU32 { capacity } => {
            let capacity = match eval_exec_operand(capacity, env)? {
                RuntimeValue::U32(value) => value,
                other => return Err(format!("chan_open_u32 expects u32, got {other:?}")),
            };
            Ok(RuntimeValue::U64(portable_chan_open_u32(capacity)?))
        }
        LoweredExecExpr::ChanOpenBufU8 { capacity } => {
            let capacity = match eval_exec_operand(capacity, env)? {
                RuntimeValue::U32(value) => value,
                other => return Err(format!("chan_open_buf expects u32, got {other:?}")),
            };
            Ok(RuntimeValue::U64(portable_chan_open_buf(capacity)?))
        }
        LoweredExecExpr::ChanSendU32 { channel, value } => {
            let channel = eval_handle_operand(channel, env, "chan_send_u32")?;
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::U32(value) => value,
                other => return Err(format!("chan_send_u32 expects u32, got {other:?}")),
            };
            Ok(RuntimeValue::Bool(portable_chan_send_u32(channel, value)?))
        }
        LoweredExecExpr::ChanSendBufU8 { channel, value } => {
            let channel = eval_handle_operand(channel, env, "chan_send_buf")?;
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("chan_send_buf expects buf[u8], got {other:?}")),
            };
            Ok(RuntimeValue::Bool(portable_chan_send_buf(channel, value)?))
        }
        LoweredExecExpr::ChanRecvU32 { channel } => {
            let channel = eval_handle_operand(channel, env, "chan_recv_u32")?;
            Ok(RuntimeValue::U32(portable_chan_recv_u32(channel)?))
        }
        LoweredExecExpr::ChanRecvBufU8 { channel } => {
            let channel = eval_handle_operand(channel, env, "chan_recv_buf")?;
            Ok(RuntimeValue::BufU8(portable_chan_recv_buf(channel)?))
        }
        LoweredExecExpr::ChanLen { channel } => {
            let channel = eval_handle_operand(channel, env, "chan_len")?;
            Ok(RuntimeValue::U32(portable_chan_len(channel)?))
        }
        LoweredExecExpr::ChanClose { channel } => {
            let channel = eval_handle_operand(channel, env, "chan_close")?;
            Ok(RuntimeValue::Bool(portable_chan_close(channel)?))
        }
        LoweredExecExpr::DeadlineOpenMs { timeout_ms } => {
            let timeout_ms = match eval_exec_operand(timeout_ms, env)? {
                RuntimeValue::U32(value) => value,
                other => return Err(format!("deadline_open_ms expects u32, got {other:?}")),
            };
            Ok(RuntimeValue::U64(portable_deadline_open_ms(timeout_ms)?))
        }
        LoweredExecExpr::DeadlineExpired { handle } => {
            let handle = eval_handle_operand(handle, env, "deadline_expired")?;
            Ok(RuntimeValue::Bool(portable_deadline_expired(handle)?))
        }
        LoweredExecExpr::DeadlineRemainingMs { handle } => {
            let handle = eval_handle_operand(handle, env, "deadline_remaining_ms")?;
            Ok(RuntimeValue::U32(portable_deadline_remaining_ms(handle)?))
        }
        LoweredExecExpr::DeadlineClose { handle } => {
            let handle = eval_handle_operand(handle, env, "deadline_close")?;
            Ok(RuntimeValue::Bool(portable_deadline_close(handle)?))
        }
        LoweredExecExpr::CancelScopeOpen => {
            Ok(RuntimeValue::U64(portable_cancel_scope_open()?))
        }
        LoweredExecExpr::CancelScopeChild { parent } => {
            let parent = eval_handle_operand(parent, env, "cancel_scope_child")?;
            Ok(RuntimeValue::U64(portable_cancel_scope_child(parent)?))
        }
        LoweredExecExpr::CancelScopeBindTask { scope, task } => {
            let scope = eval_handle_operand(scope, env, "cancel_scope_bind_task")?;
            let task = eval_handle_operand(task, env, "cancel_scope_bind_task")?;
            Ok(RuntimeValue::Bool(portable_cancel_scope_bind_task(scope, task)?))
        }
        LoweredExecExpr::CancelScopeCancel { scope } => {
            let scope = eval_handle_operand(scope, env, "cancel_scope_cancel")?;
            Ok(RuntimeValue::Bool(portable_cancel_scope_cancel(scope)?))
        }
        LoweredExecExpr::CancelScopeCancelled { scope } => {
            let scope = eval_handle_operand(scope, env, "cancel_scope_cancelled")?;
            Ok(RuntimeValue::Bool(portable_cancel_scope_cancelled(scope)?))
        }
        LoweredExecExpr::CancelScopeClose { scope } => {
            let scope = eval_handle_operand(scope, env, "cancel_scope_close")?;
            Ok(RuntimeValue::Bool(portable_cancel_scope_close(scope)?))
        }
        LoweredExecExpr::RetryOpen {
            max_attempts,
            base_backoff_ms,
        } => {
            let max_attempts = match eval_exec_operand(max_attempts, env)? {
                RuntimeValue::U32(value) => value,
                other => return Err(format!("retry_open expects u32 max_attempts, got {other:?}")),
            };
            let base_backoff_ms = match eval_exec_operand(base_backoff_ms, env)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!("retry_open expects u32 base_backoff_ms, got {other:?}"))
                }
            };
            Ok(RuntimeValue::U64(portable_retry_open(
                max_attempts,
                base_backoff_ms,
            )?))
        }
        LoweredExecExpr::RetryRecordFailure { handle } => {
            let handle = eval_handle_operand(handle, env, "retry_record_failure")?;
            Ok(RuntimeValue::Bool(portable_retry_record_failure(handle)?))
        }
        LoweredExecExpr::RetryRecordSuccess { handle } => {
            let handle = eval_handle_operand(handle, env, "retry_record_success")?;
            Ok(RuntimeValue::Bool(portable_retry_record_success(handle)?))
        }
        LoweredExecExpr::RetryNextDelayMs { handle } => {
            let handle = eval_handle_operand(handle, env, "retry_next_delay_ms")?;
            Ok(RuntimeValue::U32(portable_retry_next_delay_ms(handle)?))
        }
        LoweredExecExpr::RetryExhausted { handle } => {
            let handle = eval_handle_operand(handle, env, "retry_exhausted")?;
            Ok(RuntimeValue::Bool(portable_retry_exhausted(handle)?))
        }
        LoweredExecExpr::RetryClose { handle } => {
            let handle = eval_handle_operand(handle, env, "retry_close")?;
            Ok(RuntimeValue::Bool(portable_retry_close(handle)?))
        }
        LoweredExecExpr::CircuitOpen {
            threshold,
            cooldown_ms,
        } => {
            let threshold = match eval_exec_operand(threshold, env)? {
                RuntimeValue::U32(value) => value,
                other => return Err(format!("circuit_open expects u32 threshold, got {other:?}")),
            };
            let cooldown_ms = match eval_exec_operand(cooldown_ms, env)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!("circuit_open expects u32 cooldown_ms, got {other:?}"))
                }
            };
            Ok(RuntimeValue::U64(portable_circuit_open(
                threshold,
                cooldown_ms,
            )?))
        }
        LoweredExecExpr::CircuitAllow { handle } => {
            let handle = eval_handle_operand(handle, env, "circuit_allow")?;
            Ok(RuntimeValue::Bool(portable_circuit_allow(handle)?))
        }
        LoweredExecExpr::CircuitRecordFailure { handle } => {
            let handle = eval_handle_operand(handle, env, "circuit_record_failure")?;
            Ok(RuntimeValue::Bool(portable_circuit_record_failure(handle)?))
        }
        LoweredExecExpr::CircuitRecordSuccess { handle } => {
            let handle = eval_handle_operand(handle, env, "circuit_record_success")?;
            Ok(RuntimeValue::Bool(portable_circuit_record_success(handle)?))
        }
        LoweredExecExpr::CircuitState { handle } => {
            let handle = eval_handle_operand(handle, env, "circuit_state")?;
            Ok(RuntimeValue::U32(portable_circuit_state(handle)?))
        }
        LoweredExecExpr::CircuitClose { handle } => {
            let handle = eval_handle_operand(handle, env, "circuit_close")?;
            Ok(RuntimeValue::Bool(portable_circuit_close(handle)?))
        }
        LoweredExecExpr::BackpressureOpen { limit } => {
            let limit = match eval_exec_operand(limit, env)? {
                RuntimeValue::U32(value) => value,
                other => return Err(format!("backpressure_open expects u32, got {other:?}")),
            };
            Ok(RuntimeValue::U64(portable_backpressure_open(limit)?))
        }
        LoweredExecExpr::BackpressureAcquire { handle } => {
            let handle = eval_handle_operand(handle, env, "backpressure_acquire")?;
            Ok(RuntimeValue::Bool(portable_backpressure_acquire(handle)?))
        }
        LoweredExecExpr::BackpressureRelease { handle } => {
            let handle = eval_handle_operand(handle, env, "backpressure_release")?;
            Ok(RuntimeValue::Bool(portable_backpressure_release(handle)?))
        }
        LoweredExecExpr::BackpressureSaturated { handle } => {
            let handle = eval_handle_operand(handle, env, "backpressure_saturated")?;
            Ok(RuntimeValue::Bool(portable_backpressure_saturated(handle)?))
        }
        LoweredExecExpr::BackpressureClose { handle } => {
            let handle = eval_handle_operand(handle, env, "backpressure_close")?;
            Ok(RuntimeValue::Bool(portable_backpressure_close(handle)?))
        }
        LoweredExecExpr::SupervisorOpen {
            restart_budget,
            degrade_after,
        } => {
            let restart_budget = match eval_exec_operand(restart_budget, env)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!("supervisor_open expects u32 restart_budget, got {other:?}"))
                }
            };
            let degrade_after = match eval_exec_operand(degrade_after, env)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!("supervisor_open expects u32 degrade_after, got {other:?}"))
                }
            };
            Ok(RuntimeValue::U64(portable_supervisor_open(
                restart_budget,
                degrade_after,
            )?))
        }
        LoweredExecExpr::SupervisorRecordFailure { handle, code } => {
            let handle = eval_handle_operand(handle, env, "supervisor_record_failure")?;
            let code = match eval_exec_operand(code, env)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "supervisor_record_failure expects u32 code, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_supervisor_record_failure(
                handle, code,
            )?))
        }
        LoweredExecExpr::SupervisorRecordRecovery { handle } => {
            let handle = eval_handle_operand(handle, env, "supervisor_record_recovery")?;
            Ok(RuntimeValue::Bool(portable_supervisor_record_recovery(handle)?))
        }
        LoweredExecExpr::SupervisorShouldRestart { handle } => {
            let handle = eval_handle_operand(handle, env, "supervisor_should_restart")?;
            Ok(RuntimeValue::Bool(portable_supervisor_should_restart(handle)?))
        }
        LoweredExecExpr::SupervisorDegraded { handle } => {
            let handle = eval_handle_operand(handle, env, "supervisor_degraded")?;
            Ok(RuntimeValue::Bool(portable_supervisor_degraded(handle)?))
        }
        LoweredExecExpr::SupervisorClose { handle } => {
            let handle = eval_handle_operand(handle, env, "supervisor_close")?;
            Ok(RuntimeValue::Bool(portable_supervisor_close(handle)?))
        }
        LoweredExecExpr::TaskSleepMs { value } => {
            let millis = match eval_exec_operand(value, env)? {
                RuntimeValue::U32(value) => value,
                other => return Err(format!("task_sleep_ms expects u32, got {other:?}")),
            };
            Ok(RuntimeValue::Bool(portable_task_sleep_ms(millis)))
        }
        LoweredExecExpr::TaskOpen { command, argv, env } => {
            Ok(RuntimeValue::U64(portable_task_open(command, argv, env)?))
        }
        LoweredExecExpr::TaskDone { handle } => {
            let handle = eval_handle_operand(handle, env, "task_done")?;
            Ok(RuntimeValue::Bool(portable_task_done(handle)?))
        }
        LoweredExecExpr::TaskJoinStatus { handle } => {
            let handle = eval_handle_operand(handle, env, "task_join")?;
            Ok(RuntimeValue::I32(portable_task_join(handle)?))
        }
        LoweredExecExpr::TaskStdoutAllU8 { handle } => {
            let handle = eval_handle_operand(handle, env, "task_stdout_all")?;
            Ok(RuntimeValue::BufU8(portable_task_stdout_all(handle)?))
        }
        LoweredExecExpr::TaskStderrAllU8 { handle } => {
            let handle = eval_handle_operand(handle, env, "task_stderr_all")?;
            Ok(RuntimeValue::BufU8(portable_task_stderr_all(handle)?))
        }
        LoweredExecExpr::TaskClose { handle } => {
            let handle = eval_handle_operand(handle, env, "task_close")?;
            Ok(RuntimeValue::Bool(portable_task_close(handle)?))
        }
        LoweredExecExpr::SpawnCaptureAllU8 { command, argv, env } => Ok(RuntimeValue::BufU8(
            portable_spawn_capture(command, argv, env, false)?,
        )),
        LoweredExecExpr::SpawnCaptureStderrAllU8 { command, argv, env } => Ok(RuntimeValue::BufU8(
            portable_spawn_capture(command, argv, env, true)?,
        )),
        LoweredExecExpr::SpawnCall { command, argv, env } => Ok(RuntimeValue::I32(
            portable_spawn_status(command, argv, env)?,
        )),
        LoweredExecExpr::SpawnOpen { command, argv, env } => {
            Ok(RuntimeValue::U64(portable_spawn_open(command, argv, env)?))
        }
        LoweredExecExpr::SpawnWait { handle } => {
            let handle = eval_handle_operand(handle, env, "spawn_wait")?;
            Ok(RuntimeValue::I32(portable_spawn_wait(handle)?))
        }
        LoweredExecExpr::SpawnStdoutAllU8 { handle } => {
            let handle = eval_handle_operand(handle, env, "spawn_stdout_all")?;
            Ok(RuntimeValue::BufU8(portable_spawn_stdout_all(handle)?))
        }
        LoweredExecExpr::SpawnStderrAllU8 { handle } => {
            let handle = eval_handle_operand(handle, env, "spawn_stderr_all")?;
            Ok(RuntimeValue::BufU8(portable_spawn_stderr_all(handle)?))
        }
        LoweredExecExpr::SpawnStdinWriteAllU8 { handle, value } => {
            let handle = eval_handle_operand(handle, env, "spawn_stdin_write_all")?;
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "spawn_stdin_write_all expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_spawn_stdin_write_all(
                handle, &value,
            )?))
        }
        LoweredExecExpr::SpawnStdinClose { handle } => {
            let handle = eval_handle_operand(handle, env, "spawn_stdin_close")?;
            Ok(RuntimeValue::Bool(portable_spawn_stdin_close(handle)?))
        }
        LoweredExecExpr::SpawnDone { handle } => {
            let handle = eval_handle_operand(handle, env, "spawn_done")?;
            Ok(RuntimeValue::Bool(portable_spawn_done(handle)?))
        }
        LoweredExecExpr::SpawnExitOk { handle } => {
            let handle = eval_handle_operand(handle, env, "spawn_exit_ok")?;
            Ok(RuntimeValue::Bool(portable_spawn_exit_ok(handle)?))
        }
        LoweredExecExpr::SpawnKill { handle } => {
            let handle = eval_handle_operand(handle, env, "spawn_kill")?;
            Ok(RuntimeValue::Bool(portable_spawn_kill(handle)?))
        }
        LoweredExecExpr::SpawnClose { handle } => {
            let handle = eval_handle_operand(handle, env, "spawn_close")?;
            Ok(RuntimeValue::Bool(portable_spawn_close(handle)?))
        }
        LoweredExecExpr::NetConnect { host, port } => {
            Ok(RuntimeValue::Bool(portable_net_connect_ok(host, *port)?))
        }
        LoweredExecExpr::FfiCall {
            symbol,
            args,
            ret_c_type,
        } => {
            let mut values = Vec::new();
            for operand in args {
                values.push(eval_exec_operand(operand, env)?);
            }
            portable_ffi_call(symbol, &values, ret_c_type)
        }
        LoweredExecExpr::FfiCallCStr {
            symbol,
            arg,
            ret_c_type,
        } => {
            let value = match env.get(arg) {
                Some(RuntimeValue::BufU8(value)) => value.clone(),
                Some(other) => return Err(format!("ffi_call_cstr expects buf[u8], got {other:?}")),
                None => return Err(format!("unknown ffi_call_cstr binding {arg}")),
            };
            portable_ffi_call_cstr(symbol, &value, ret_c_type)
        }
        LoweredExecExpr::FfiOpenLib { path } => Ok(RuntimeValue::U64(portable_ffi_open_lib(path)?)),
        LoweredExecExpr::FfiCloseLib { handle } => {
            let handle = eval_handle_operand(handle, env, "ffi_close_lib")?;
            Ok(RuntimeValue::Bool(portable_ffi_close_lib(handle)?))
        }
        LoweredExecExpr::FfiBufPtr { value } => {
            let value = match eval_exec_operand(value, env)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("ffi_buf_ptr expects buf[u8], got {other:?}")),
            };
            Ok(RuntimeValue::U64(value.as_ptr() as usize as u64))
        }
        LoweredExecExpr::FfiCallLib {
            handle,
            symbol,
            args,
            ret_c_type,
        } => {
            let handle = eval_handle_operand(handle, env, "ffi_call_lib")?;
            let mut values = Vec::new();
            for operand in args {
                values.push(eval_exec_operand(operand, env)?);
            }
            portable_ffi_call_lib(handle, symbol, &values, ret_c_type)
        }
        LoweredExecExpr::FfiCallLibCStr {
            handle,
            symbol,
            arg,
            ret_c_type,
        } => {
            let handle = eval_handle_operand(handle, env, "ffi_call_lib_cstr")?;
            let value = match env.get(arg) {
                Some(RuntimeValue::BufU8(value)) => value.clone(),
                Some(other) => {
                    return Err(format!("ffi_call_lib_cstr expects buf[u8], got {other:?}"))
                }
                None => return Err(format!("unknown ffi_call_lib_cstr binding {arg}")),
            };
            portable_ffi_call_lib_cstr(handle, symbol, &value, ret_c_type)
        }
        LoweredExecExpr::Len { source } => match env.get(source) {
            Some(RuntimeValue::SpanI32(values)) => Ok(RuntimeValue::U32(values.len() as u32)),
            Some(RuntimeValue::BufU8(values)) => Ok(RuntimeValue::U32(values.len() as u32)),
            Some(other) => Err(format!("len source must be span/buf, got {other:?}")),
            None => Err(format!("unknown len source {source}")),
        },
        LoweredExecExpr::StoreBufU8 {
            source,
            index,
            value,
        } => {
            let mut values = match env.get(source) {
                Some(RuntimeValue::BufU8(values)) => values.clone(),
                Some(other) => return Err(format!("store source must be buf[u8], got {other:?}")),
                None => return Err(format!("unknown store source {source}")),
            };
            let offset = match eval_exec_operand(index, env)? {
                RuntimeValue::U32(value) => value as usize,
                RuntimeValue::I32(value) if value >= 0 => value as usize,
                other => {
                    return Err(format!(
                        "store index must be non-negative integer, got {other:?}"
                    ))
                }
            };
            let byte = match eval_exec_operand(value, env)? {
                RuntimeValue::U8(value) => value,
                RuntimeValue::U32(value) if value <= u8::MAX as u32 => value as u8,
                other => return Err(format!("store value must be u8, got {other:?}")),
            };
            if offset >= values.len() {
                return Err(format!("store index {} out of bounds", offset));
            }
            values[offset] = byte;
            Ok(RuntimeValue::BufU8(values))
        }
        LoweredExecExpr::LoadU8 { source, index } => {
            let values = match env.get(source) {
                Some(RuntimeValue::BufU8(values)) => values,
                Some(other) => return Err(format!("load source must be buf[u8], got {other:?}")),
                None => return Err(format!("unknown load source {source}")),
            };
            let offset = match eval_exec_operand(index, env)? {
                RuntimeValue::U32(value) => value as usize,
                RuntimeValue::I32(value) if value >= 0 => value as usize,
                other => {
                    return Err(format!(
                        "load index must be non-negative integer, got {other:?}"
                    ))
                }
            };
            values
                .get(offset)
                .copied()
                .map(RuntimeValue::U8)
                .ok_or_else(|| format!("load index {} out of bounds", offset))
        }
        LoweredExecExpr::LoadI32 { source, index } => {
            let values = match env.get(source) {
                Some(RuntimeValue::SpanI32(values)) => values,
                Some(other) => return Err(format!("load source must be span, got {other:?}")),
                None => return Err(format!("unknown load source {source}")),
            };
            let index = eval_exec_operand(index, env)?;
            let offset = match index {
                RuntimeValue::U32(value) => value as usize,
                RuntimeValue::I32(value) if value >= 0 => value as usize,
                other => {
                    return Err(format!(
                        "load index must be non-negative integer, got {other:?}"
                    ))
                }
            };
            values
                .get(offset)
                .copied()
                .map(RuntimeValue::I32)
                .ok_or_else(|| format!("load index {} out of bounds", offset))
        }
        LoweredExecExpr::AbsI32 { value } => match eval_exec_operand(value, env)? {
            RuntimeValue::I32(value) => Ok(RuntimeValue::I32(value.abs())),
            other => Err(format!("abs expects i32, got {other:?}")),
        },
        LoweredExecExpr::Binary { op, left, right } => {
            let left = eval_exec_operand(left, env)?;
            let right = eval_exec_operand(right, env)?;
            eval_binary(op, &left, &right)
        }
        LoweredExecExpr::SextI64 { value } => match eval_exec_operand(value, env)? {
            RuntimeValue::I32(value) => Ok(RuntimeValue::I64(value as i64)),
            other => Err(format!("sext i64 expects i32 source, got {other:?}")),
        },
    }
}

fn eval_exec_operand(
    operand: &LoweredExecOperand,
    env: &HashMap<String, RuntimeValue>,
) -> Result<RuntimeValue, String> {
    match operand {
        LoweredExecOperand::Binding(name) => env
            .get(name)
            .cloned()
            .ok_or_else(|| format!("unknown lowered binding {name}")),
        LoweredExecOperand::Immediate(immediate) => Ok(match immediate {
            LoweredExecImmediate::U8(value) => RuntimeValue::U8(*value),
            LoweredExecImmediate::I32(value) => RuntimeValue::I32(*value),
            LoweredExecImmediate::I64(value) => RuntimeValue::I64(*value),
            LoweredExecImmediate::U64(value) => RuntimeValue::U64(*value),
            LoweredExecImmediate::U32(value) => RuntimeValue::U32(*value),
            LoweredExecImmediate::Bool(value) => RuntimeValue::Bool(*value),
        }),
    }
}

fn eval_handle_operand(
    operand: &LoweredExecOperand,
    env: &HashMap<String, RuntimeValue>,
    opname: &str,
) -> Result<u64, String> {
    match eval_exec_operand(operand, env)? {
        RuntimeValue::U64(value) => Ok(value),
        other => Err(format!("{opname} expects u64 handle, got {other:?}")),
    }
}

fn eval_binary(
    op: &LoweredExecBinaryOp,
    left: &RuntimeValue,
    right: &RuntimeValue,
) -> Result<RuntimeValue, String> {
    match (op, left, right) {
        (LoweredExecBinaryOp::Add, RuntimeValue::I32(left), RuntimeValue::I32(right)) => {
            Ok(RuntimeValue::I32(left + right))
        }
        (LoweredExecBinaryOp::Add, RuntimeValue::I64(left), RuntimeValue::I64(right)) => {
            Ok(RuntimeValue::I64(left + right))
        }
        (LoweredExecBinaryOp::Add, RuntimeValue::U64(left), RuntimeValue::U64(right)) => {
            Ok(RuntimeValue::U64(left + right))
        }
        (LoweredExecBinaryOp::Add, RuntimeValue::U32(left), RuntimeValue::U32(right)) => {
            Ok(RuntimeValue::U32(left + right))
        }
        (LoweredExecBinaryOp::Add, RuntimeValue::U8(left), RuntimeValue::U8(right)) => {
            Ok(RuntimeValue::U8(left.wrapping_add(*right)))
        }
        (LoweredExecBinaryOp::Sub, RuntimeValue::I32(left), RuntimeValue::I32(right)) => {
            Ok(RuntimeValue::I32(left - right))
        }
        (LoweredExecBinaryOp::Sub, RuntimeValue::I64(left), RuntimeValue::I64(right)) => {
            Ok(RuntimeValue::I64(left - right))
        }
        (LoweredExecBinaryOp::Sub, RuntimeValue::U8(left), RuntimeValue::U8(right)) => {
            Ok(RuntimeValue::U8(left.wrapping_sub(*right)))
        }
        (LoweredExecBinaryOp::Mul, RuntimeValue::I32(left), RuntimeValue::I32(right)) => {
            Ok(RuntimeValue::I32(left * right))
        }
        (LoweredExecBinaryOp::Mul, RuntimeValue::I64(left), RuntimeValue::I64(right)) => {
            Ok(RuntimeValue::I64(left * right))
        }
        (LoweredExecBinaryOp::Mul, RuntimeValue::U8(left), RuntimeValue::U8(right)) => {
            Ok(RuntimeValue::U8(left.wrapping_mul(*right)))
        }
        (LoweredExecBinaryOp::Band, RuntimeValue::I32(left), RuntimeValue::I32(right)) => {
            Ok(RuntimeValue::I32(left & right))
        }
        (LoweredExecBinaryOp::Band, RuntimeValue::I64(left), RuntimeValue::I64(right)) => {
            Ok(RuntimeValue::I64(left & right))
        }
        (LoweredExecBinaryOp::Band, RuntimeValue::U64(left), RuntimeValue::U64(right)) => {
            Ok(RuntimeValue::U64(left & right))
        }
        (LoweredExecBinaryOp::Band, RuntimeValue::U32(left), RuntimeValue::U32(right)) => {
            Ok(RuntimeValue::U32(left & right))
        }
        (LoweredExecBinaryOp::Band, RuntimeValue::U8(left), RuntimeValue::U8(right)) => {
            Ok(RuntimeValue::U8(left & right))
        }
        (LoweredExecBinaryOp::Bor, RuntimeValue::I32(left), RuntimeValue::I32(right)) => {
            Ok(RuntimeValue::I32(left | right))
        }
        (LoweredExecBinaryOp::Bor, RuntimeValue::I64(left), RuntimeValue::I64(right)) => {
            Ok(RuntimeValue::I64(left | right))
        }
        (LoweredExecBinaryOp::Bor, RuntimeValue::U64(left), RuntimeValue::U64(right)) => {
            Ok(RuntimeValue::U64(left | right))
        }
        (LoweredExecBinaryOp::Bor, RuntimeValue::U32(left), RuntimeValue::U32(right)) => {
            Ok(RuntimeValue::U32(left | right))
        }
        (LoweredExecBinaryOp::Bor, RuntimeValue::U8(left), RuntimeValue::U8(right)) => {
            Ok(RuntimeValue::U8(left | right))
        }
        (LoweredExecBinaryOp::Bxor, RuntimeValue::I32(left), RuntimeValue::I32(right)) => {
            Ok(RuntimeValue::I32(left ^ right))
        }
        (LoweredExecBinaryOp::Bxor, RuntimeValue::I64(left), RuntimeValue::I64(right)) => {
            Ok(RuntimeValue::I64(left ^ right))
        }
        (LoweredExecBinaryOp::Bxor, RuntimeValue::U64(left), RuntimeValue::U64(right)) => {
            Ok(RuntimeValue::U64(left ^ right))
        }
        (LoweredExecBinaryOp::Bxor, RuntimeValue::U32(left), RuntimeValue::U32(right)) => {
            Ok(RuntimeValue::U32(left ^ right))
        }
        (LoweredExecBinaryOp::Bxor, RuntimeValue::U8(left), RuntimeValue::U8(right)) => {
            Ok(RuntimeValue::U8(left ^ right))
        }
        (LoweredExecBinaryOp::Bxor, RuntimeValue::Bool(left), RuntimeValue::Bool(right)) => {
            Ok(RuntimeValue::Bool(*left ^ *right))
        }
        (LoweredExecBinaryOp::Shl, RuntimeValue::I32(left), RuntimeValue::U32(right)) => {
            Ok(RuntimeValue::I32(left.wrapping_shl(*right)))
        }
        (LoweredExecBinaryOp::Shl, RuntimeValue::I64(left), RuntimeValue::U32(right)) => {
            Ok(RuntimeValue::I64(left.wrapping_shl(*right)))
        }
        (LoweredExecBinaryOp::Shl, RuntimeValue::U64(left), RuntimeValue::U32(right)) => {
            Ok(RuntimeValue::U64(left.wrapping_shl(*right)))
        }
        (LoweredExecBinaryOp::Shl, RuntimeValue::U32(left), RuntimeValue::U32(right)) => {
            Ok(RuntimeValue::U32(left.wrapping_shl(*right)))
        }
        (LoweredExecBinaryOp::Shl, RuntimeValue::U8(left), RuntimeValue::U32(right)) => {
            Ok(RuntimeValue::U8(left.wrapping_shl(*right)))
        }
        (LoweredExecBinaryOp::Shr, RuntimeValue::I32(left), RuntimeValue::U32(right)) => {
            Ok(RuntimeValue::I32(left.wrapping_shr(*right)))
        }
        (LoweredExecBinaryOp::Shr, RuntimeValue::I64(left), RuntimeValue::U32(right)) => {
            Ok(RuntimeValue::I64(left.wrapping_shr(*right)))
        }
        (LoweredExecBinaryOp::Shr, RuntimeValue::U64(left), RuntimeValue::U32(right)) => {
            Ok(RuntimeValue::U64(left.wrapping_shr(*right)))
        }
        (LoweredExecBinaryOp::Shr, RuntimeValue::U32(left), RuntimeValue::U32(right)) => {
            Ok(RuntimeValue::U32(left.wrapping_shr(*right)))
        }
        (LoweredExecBinaryOp::Shr, RuntimeValue::U8(left), RuntimeValue::U32(right)) => {
            Ok(RuntimeValue::U8(left.wrapping_shr(*right)))
        }
        (LoweredExecBinaryOp::Eq, RuntimeValue::I32(left), RuntimeValue::I32(right)) => {
            Ok(RuntimeValue::Bool(left == right))
        }
        (LoweredExecBinaryOp::Eq, RuntimeValue::I64(left), RuntimeValue::I64(right)) => {
            Ok(RuntimeValue::Bool(left == right))
        }
        (LoweredExecBinaryOp::Eq, RuntimeValue::U64(left), RuntimeValue::U64(right)) => {
            Ok(RuntimeValue::Bool(left == right))
        }
        (LoweredExecBinaryOp::Eq, RuntimeValue::U32(left), RuntimeValue::U32(right)) => {
            Ok(RuntimeValue::Bool(left == right))
        }
        (LoweredExecBinaryOp::Eq, RuntimeValue::U8(left), RuntimeValue::U8(right)) => {
            Ok(RuntimeValue::Bool(left == right))
        }
        (LoweredExecBinaryOp::Eq, RuntimeValue::Bool(left), RuntimeValue::Bool(right)) => {
            Ok(RuntimeValue::Bool(left == right))
        }
        (LoweredExecBinaryOp::Lt, RuntimeValue::I32(left), RuntimeValue::I32(right)) => {
            Ok(RuntimeValue::Bool(left < right))
        }
        (LoweredExecBinaryOp::Lt, RuntimeValue::I64(left), RuntimeValue::I64(right)) => {
            Ok(RuntimeValue::Bool(left < right))
        }
        (LoweredExecBinaryOp::Lt, RuntimeValue::U64(left), RuntimeValue::U64(right)) => {
            Ok(RuntimeValue::Bool(left < right))
        }
        (LoweredExecBinaryOp::Lt, RuntimeValue::U32(left), RuntimeValue::U32(right)) => {
            Ok(RuntimeValue::Bool(left < right))
        }
        (LoweredExecBinaryOp::Lt, RuntimeValue::U8(left), RuntimeValue::U8(right)) => {
            Ok(RuntimeValue::Bool(left < right))
        }
        (LoweredExecBinaryOp::Le, RuntimeValue::I32(left), RuntimeValue::I32(right)) => {
            Ok(RuntimeValue::Bool(left <= right))
        }
        (LoweredExecBinaryOp::Le, RuntimeValue::I64(left), RuntimeValue::I64(right)) => {
            Ok(RuntimeValue::Bool(left <= right))
        }
        (LoweredExecBinaryOp::Le, RuntimeValue::U64(left), RuntimeValue::U64(right)) => {
            Ok(RuntimeValue::Bool(left <= right))
        }
        (LoweredExecBinaryOp::Le, RuntimeValue::U32(left), RuntimeValue::U32(right)) => {
            Ok(RuntimeValue::Bool(left <= right))
        }
        (LoweredExecBinaryOp::Le, RuntimeValue::U8(left), RuntimeValue::U8(right)) => {
            Ok(RuntimeValue::Bool(left <= right))
        }
        _ => Err(format!(
            "unsupported lowered binary operation {op:?} for {left:?} and {right:?}"
        )),
    }
}

pub fn runtime_value_from_data(ty: &TypeRef, value: &DataValue) -> Result<RuntimeValue, String> {
    match (ty, value) {
        (
            TypeRef::Int {
                signed: false,
                bits: 8,
            },
            DataValue::Int(value),
        ) => Ok(RuntimeValue::U8(*value as u8)),
        (
            TypeRef::Int {
                signed: true,
                bits: 32,
            },
            DataValue::Int(value),
        ) => Ok(RuntimeValue::I32(*value as i32)),
        (
            TypeRef::Int {
                signed: true,
                bits: 64,
            },
            DataValue::Int(value),
        ) => Ok(RuntimeValue::I64(*value as i64)),
        (
            TypeRef::Int {
                signed: false,
                bits: 64,
            },
            DataValue::Int(value),
        ) => Ok(RuntimeValue::U64(*value as u64)),
        (
            TypeRef::Int {
                signed: false,
                bits: 32,
            },
            DataValue::Int(value),
        ) => Ok(RuntimeValue::U32(*value as u32)),
        (TypeRef::Bool, DataValue::Bool(value)) => Ok(RuntimeValue::Bool(*value)),
        (TypeRef::Span(inner), DataValue::Array(items))
            if **inner
                == TypeRef::Int {
                    signed: true,
                    bits: 32,
                } =>
        {
            let mut values = Vec::new();
            for item in items {
                let DataValue::Int(value) = item else {
                    return Err(
                        "span[i32] direct execution only supports integer arrays".to_string()
                    );
                };
                values.push(*value as i32);
            }
            Ok(RuntimeValue::SpanI32(values))
        }
        (buffer_ty, DataValue::Array(items)) if is_u8_buffer_runtime_type(buffer_ty) => {
            let mut values = Vec::new();
            for item in items {
                let DataValue::Int(value) = item else {
                    return Err("buf[u8] direct execution only supports integer arrays".to_string());
                };
                values.push(*value as u8);
            }
            Ok(RuntimeValue::BufU8(values))
        }
        _ => Err(format!(
            "unsupported direct lowered runtime value for type {ty} and data {value:?}"
        )),
    }
}

fn is_u8_buffer_runtime_type(ty: &TypeRef) -> bool {
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
            is_u8_buffer_runtime_type(inner)
        }
        _ => false,
    }
}

pub(crate) fn mira_clock_now_ns() -> u64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    (now.as_secs() * 1_000_000_000u64).saturating_add(now.subsec_nanos() as u64)
}

pub(crate) fn mira_rand_next_u32(state: &mut Option<u32>) -> u32 {
    let mut x = state.unwrap_or(0);
    if x == 0 {
        x = 2_463_534_242u32;
    }
    x ^= x << 13;
    x ^= x >> 17;
    x ^= x << 5;
    *state = Some(x);
    x
}

pub(crate) fn portable_fs_read_u32(path: &str) -> Result<u32, String> {
    let contents = fs::read_to_string(path)
        .map_err(|error| format!("portable fs_read_u32 failed: {error}"))?;
    contents
        .trim()
        .parse::<u32>()
        .map_err(|error| format!("portable fs_read_u32 parse failed: {error}"))
}

pub(crate) fn portable_fs_write_u32(path: &str, value: u32) -> Result<bool, String> {
    fs::write(path, format!("{value}\n"))
        .map(|_| true)
        .map_err(|error| format!("portable fs_write_u32 failed: {error}"))
}

pub(crate) fn portable_fs_read_all_u8(path: &str) -> Result<Vec<u8>, String> {
    fs::read(path).map_err(|error| format!("portable fs_read_all failed: {error}"))
}

pub(crate) fn portable_fs_write_all_u8(path: &str, value: &[u8]) -> Result<bool, String> {
    fs::write(path, value)
        .map(|_| true)
        .map_err(|error| format!("portable fs_write_all failed: {error}"))
}

pub(crate) fn portable_spawn_status(
    command: &str,
    argv: &[String],
    env_vars: &[(String, String)],
) -> Result<i32, String> {
    let mut invocation = Command::new(command);
    invocation.args(argv);
    for (name, value) in env_vars {
        invocation.env(name, value);
    }
    match invocation.status() {
        Ok(status) => Ok(status.code().unwrap_or(-1)),
        Err(error) => Err(format!("portable spawn_call failed: {error}")),
    }
}

pub(crate) fn portable_spawn_capture(
    command: &str,
    argv: &[String],
    env_vars: &[(String, String)],
    stderr: bool,
) -> Result<Vec<u8>, String> {
    let mut invocation = Command::new(command);
    invocation.args(argv);
    for (name, value) in env_vars {
        invocation.env(name, value);
    }
    match invocation.output() {
        Ok(output) => Ok(if stderr { output.stderr } else { output.stdout }),
        Err(error) => Err(format!("portable spawn_capture_all failed: {error}")),
    }
}

pub(crate) fn portable_task_sleep_ms(millis: u32) -> bool {
    thread::sleep(Duration::from_millis(millis as u64));
    true
}

pub(crate) fn portable_net_connect_ok(host: &str, port: u16) -> Result<bool, String> {
    let per_attempt_timeout = Duration::from_millis(250);
    let deadline = Instant::now() + Duration::from_secs(2);
    let addrs = match (host, port).to_socket_addrs() {
        Ok(addrs) => addrs.collect::<Vec<_>>(),
        Err(_) => return Ok(false),
    };
    while Instant::now() < deadline {
        for addr in &addrs {
            if TcpStream::connect_timeout(addr, per_attempt_timeout).is_ok() {
                return Ok(true);
            }
        }
        thread::sleep(Duration::from_millis(25));
    }
    Ok(false)
}

pub(crate) fn portable_tls_exchange_all(
    host: &str,
    port: u16,
    value: &[u8],
) -> Result<Vec<u8>, String> {
    let mut child = Command::new("openssl")
        .arg("s_client")
        .arg("-quiet")
        .arg("-connect")
        .arg(format!("{host}:{port}"))
        .arg("-servername")
        .arg(host)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|error| format!("portable tls_exchange_all spawn failed: {error}"))?;
    if let Some(mut stdin) = child.stdin.take() {
        stdin
            .write_all(value)
            .map_err(|error| format!("portable tls_exchange_all write failed: {error}"))?;
    }
    let output = child
        .wait_with_output()
        .map_err(|error| format!("portable tls_exchange_all wait failed: {error}"))?;
    Ok(output.stdout)
}

pub(crate) fn portable_net_write_all(host: &str, port: u16, value: &[u8]) -> Result<bool, String> {
    let timeout = Duration::from_secs(2);
    let deadline = Instant::now() + Duration::from_secs(3);
    let addrs = match (host, port).to_socket_addrs() {
        Ok(addrs) => addrs.collect::<Vec<_>>(),
        Err(_) => return Ok(false),
    };
    while Instant::now() < deadline {
        for addr in &addrs {
            if let Ok(mut stream) = TcpStream::connect_timeout(addr, timeout) {
                stream
                    .set_write_timeout(Some(timeout))
                    .map_err(|error| format!("portable net_write_all timeout failed: {error}"))?;
                stream
                    .write_all(value)
                    .map_err(|error| format!("portable net_write_all write failed: {error}"))?;
                let _ = stream.shutdown(Shutdown::Write);
                return Ok(true);
            }
        }
        thread::sleep(Duration::from_millis(25));
    }
    Ok(false)
}

pub(crate) fn portable_net_exchange_all(
    host: &str,
    port: u16,
    value: &[u8],
) -> Result<Vec<u8>, String> {
    let timeout = Duration::from_secs(2);
    let deadline = Instant::now() + Duration::from_secs(3);
    let addrs = match (host, port).to_socket_addrs() {
        Ok(addrs) => addrs.collect::<Vec<_>>(),
        Err(_) => return Ok(Vec::new()),
    };
    while Instant::now() < deadline {
        for addr in &addrs {
            if let Ok(mut stream) = TcpStream::connect_timeout(addr, timeout) {
                stream.set_write_timeout(Some(timeout)).map_err(|error| {
                    format!("portable net_exchange_all write timeout failed: {error}")
                })?;
                stream.set_read_timeout(Some(timeout)).map_err(|error| {
                    format!("portable net_exchange_all read timeout failed: {error}")
                })?;
                stream
                    .write_all(value)
                    .map_err(|error| format!("portable net_exchange_all write failed: {error}"))?;
                let _ = stream.shutdown(Shutdown::Write);
                let mut out = Vec::new();
                stream
                    .read_to_end(&mut out)
                    .map_err(|error| format!("portable net_exchange_all read failed: {error}"))?;
                return Ok(out);
            }
        }
        thread::sleep(Duration::from_millis(25));
    }
    Ok(Vec::new())
}

pub(crate) fn portable_net_serve_exchange_all(
    host: &str,
    port: u16,
    response: &[u8],
) -> Result<Vec<u8>, String> {
    let listener = TcpListener::bind((host, port))
        .map_err(|error| format!("portable net_serve_exchange_all bind failed: {error}"))?;
    listener
        .set_nonblocking(true)
        .map_err(|error| format!("portable net_serve_exchange_all nonblocking failed: {error}"))?;
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        match listener.accept() {
            Ok((mut stream, _)) => {
                stream
                    .set_write_timeout(Some(Duration::from_secs(2)))
                    .map_err(|error| {
                        format!("portable net_serve_exchange_all write timeout failed: {error}")
                    })?;
                stream
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .map_err(|error| {
                        format!("portable net_serve_exchange_all read timeout failed: {error}")
                    })?;
                let mut request = Vec::new();
                stream.read_to_end(&mut request).map_err(|error| {
                    format!("portable net_serve_exchange_all read failed: {error}")
                })?;
                stream.write_all(response).map_err(|error| {
                    format!("portable net_serve_exchange_all write failed: {error}")
                })?;
                stream.flush().map_err(|error| {
                    format!("portable net_serve_exchange_all flush failed: {error}")
                })?;
                let _ = stream.shutdown(Shutdown::Write);
                return Ok(request);
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                if Instant::now() >= deadline {
                    return Ok(Vec::new());
                }
                thread::sleep(Duration::from_millis(25));
            }
            Err(error) => {
                return Err(format!(
                    "portable net_serve_exchange_all accept failed: {error}"
                ))
            }
        }
    }
}

pub(crate) fn portable_net_listen_handle(host: &str, port: u16) -> Result<u64, String> {
    let listener = TcpListener::bind((host, port))
        .map_err(|error| format!("portable net_listen bind failed: {error}"))?;
    listener
        .set_nonblocking(true)
        .map_err(|error| format!("portable net_listen nonblocking failed: {error}"))?;
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let handle = alloc_runtime_handle(&mut state);
    state.net_handles.insert(
        handle,
        NetHandle::Listener(PlainListenerHandle {
            listener,
            timeout_ms: 5_000,
            shutdown_grace_ms: 250,
        }),
    );
    Ok(handle)
}

pub(crate) fn portable_tls_listen_handle(
    host: &str,
    port: u16,
    cert: &str,
    key: &str,
    request_timeout_ms: u32,
    session_timeout_ms: u32,
    shutdown_grace_ms: u32,
) -> Result<u64, String> {
    let mut child = Command::new("openssl")
        .arg("s_server")
        .arg("-accept")
        .arg(format!("{host}:{port}"))
        .arg("-key")
        .arg(key)
        .arg("-cert")
        .arg(cert)
        .arg("-quiet")
        .arg("-naccept")
        .arg("1")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|error| format!("portable tls_listen spawn failed: {error}"))?;
    let stdin = child.stdin.take();
    let stdout = child.stdout.take();
    thread::sleep(Duration::from_millis(150));
    let shared = Arc::new(Mutex::new(TlsServerProcess {
        child,
        stdin,
        stdout,
        request_timeout_ms,
        session_timeout_ms,
        shutdown_grace_ms,
        accepted: false,
    }));
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let handle = alloc_runtime_handle(&mut state);
    state
        .net_handles
        .insert(handle, NetHandle::TlsListener(shared));
    Ok(handle)
}

pub(crate) fn portable_net_accept_handle(listener_handle: u64) -> Result<u64, String> {
    let deadline = {
        let state = runtime_state()
            .lock()
            .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
        match state.net_handles.get(&listener_handle) {
            Some(NetHandle::Listener(listener)) => {
                Instant::now() + Duration::from_millis(listener.timeout_ms as u64)
            }
            Some(NetHandle::TlsListener(_)) => Instant::now() + Duration::from_secs(5),
            Some(_) => {
                return Err(format!(
                    "portable net_accept handle {listener_handle} is not a listener"
                ))
            }
            None => {
                return Err(format!(
                    "portable net_accept unknown handle {listener_handle}"
                ))
            }
        }
    };
    loop {
        let maybe_plain_stream = {
            let state = runtime_state()
                .lock()
                .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
            match state.net_handles.get(&listener_handle) {
                Some(NetHandle::Listener(listener)) => match listener.listener.accept() {
                    Ok((stream, _)) => Some(Ok(stream)),
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => None,
                    Err(error) => Some(Err(format!("portable net_accept failed: {error}"))),
                },
                Some(NetHandle::TlsListener(_)) => None,
                Some(_) => {
                    return Err(format!(
                        "portable net_accept handle {listener_handle} is not a listener"
                    ))
                }
                None => {
                    return Err(format!(
                        "portable net_accept unknown handle {listener_handle}"
                    ))
                }
            }
        };
        if let Some(result) = maybe_plain_stream {
            let stream = result?;
            let timeout_ms = {
                let state = runtime_state()
                    .lock()
                    .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
                match state.net_handles.get(&listener_handle) {
                    Some(NetHandle::Listener(listener)) => listener.timeout_ms,
                    _ => 5_000,
                }
            };
            let timeout = Duration::from_millis(timeout_ms.max(1) as u64);
            stream
                .set_nonblocking(false)
                .map_err(|error| format!("portable net_accept blocking reset failed: {error}"))?;
            stream
                .set_read_timeout(Some(timeout))
                .map_err(|error| format!("portable net_accept read timeout failed: {error}"))?;
            stream
                .set_write_timeout(Some(timeout))
                .map_err(|error| format!("portable net_accept write timeout failed: {error}"))?;
            let mut state = runtime_state()
                .lock()
                .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
            let handle = alloc_runtime_handle(&mut state);
            let (resume_host, resume_port) = stream
                .peer_addr()
                .map(|addr| (addr.ip().to_string(), addr.port()))
                .unwrap_or_else(|_| ("accepted".to_string(), listener_handle as u16));
            state.net_handles.insert(
                handle,
                NetHandle::Stream(PlainSessionHandle {
                    stream,
                    timeout_ms,
                    reconnect_host: None,
                    reconnect_port: 0,
                    pending_bytes: 0,
                    resume_id: stable_resume_id(&resume_host, resume_port),
                }),
            );
            return Ok(handle);
        }
        let maybe_tls = {
            let state = runtime_state()
                .lock()
                .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
            match state.net_handles.get(&listener_handle) {
                Some(NetHandle::TlsListener(shared)) => Some(shared.clone()),
                _ => None,
            }
        };
        if let Some(shared) = maybe_tls {
            {
                let mut process = shared
                    .lock()
                    .map_err(|_| "portable tls listener mutex poisoned".to_string())?;
                if process.accepted {
                    return Err("portable tls listener already accepted one session".to_string());
                }
                if let Ok(Some(status)) = process.child.try_wait() {
                    return Err(format!(
                        "portable tls listener exited early with status {status}"
                    ));
                }
                process.accepted = true;
            }
            let mut state = runtime_state()
                .lock()
                .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
            let handle = alloc_runtime_handle(&mut state);
            state
                .net_handles
                .insert(handle, NetHandle::TlsSession(shared));
            return Ok(handle);
        }
        if Instant::now() >= deadline {
            return Err("portable net_accept timed out".to_string());
        }
        thread::sleep(Duration::from_millis(25));
    }
}

pub(crate) fn portable_net_session_open(host: &str, port: u16) -> Result<u64, String> {
    let stream = TcpStream::connect((host, port))
        .map_err(|error| format!("portable net_session_open failed: {error}"))?;
    let timeout = Duration::from_millis(5_000);
    stream
        .set_read_timeout(Some(timeout))
        .map_err(|error| format!("portable net_session_open read timeout failed: {error}"))?;
    stream
        .set_write_timeout(Some(timeout))
        .map_err(|error| format!("portable net_session_open write timeout failed: {error}"))?;
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let handle = alloc_runtime_handle(&mut state);
    state.net_handles.insert(
        handle,
        NetHandle::Stream(PlainSessionHandle {
            stream,
            timeout_ms: 5_000,
            reconnect_host: Some(host.to_string()),
            reconnect_port: port,
            pending_bytes: 0,
            resume_id: stable_resume_id(host, port),
        }),
    );
    Ok(handle)
}

fn portable_tls_shutdown(shared: &Arc<Mutex<TlsServerProcess>>) -> Result<bool, String> {
    let grace = {
        let mut process = shared
            .lock()
            .map_err(|_| "portable tls session mutex poisoned".to_string())?;
        let _ = process.stdin.take();
        process.shutdown_grace_ms
    };
    let deadline = Instant::now() + Duration::from_millis(grace.max(1) as u64);
    loop {
        let done = {
            let mut process = shared
                .lock()
                .map_err(|_| "portable tls session mutex poisoned".to_string())?;
            match process.child.try_wait() {
                Ok(Some(_)) => true,
                Ok(None) => {
                    if Instant::now() >= deadline {
                        let _ = process.child.kill();
                        let _ = process.child.wait();
                        true
                    } else {
                        false
                    }
                }
                Err(error) => return Err(format!("portable tls close wait failed: {error}")),
            }
        };
        if done {
            return Ok(true);
        }
        thread::sleep(Duration::from_millis(10));
    }
}

pub(crate) fn portable_net_read_all_handle(handle: u64) -> Result<Vec<u8>, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let stream = match state.net_handles.get_mut(&handle) {
        Some(NetHandle::Stream(stream)) => stream,
        Some(_) => {
            return Err(format!(
                "portable net_read_all handle {handle} is not a plain stream"
            ))
        }
        None => return Err(format!("portable net_read_all unknown handle {handle}")),
    };
    let mut out = Vec::new();
    stream
        .stream
        .read_to_end(&mut out)
        .map_err(|error| format!("portable net_read_all failed: {error}"))?;
    Ok(out)
}

pub(crate) fn portable_session_read_chunk(handle: u64, chunk_size: u32) -> Result<Vec<u8>, String> {
    let maybe_tls = {
        let state = runtime_state()
            .lock()
            .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
        match state.net_handles.get(&handle) {
            Some(NetHandle::TlsSession(shared)) => Some(shared.clone()),
            Some(NetHandle::Stream(_)) => None,
            Some(_) => {
                return Err(format!(
                    "portable session_read_chunk handle {handle} is not a session"
                ))
            }
            None => return Err(format!("portable session_read_chunk unknown handle {handle}")),
        }
    };
    if let Some(shared) = maybe_tls {
        let mut process = shared
            .lock()
            .map_err(|_| "portable tls session mutex poisoned".to_string())?;
        let Some(stdout) = process.stdout.as_mut() else {
            return Ok(Vec::new());
        };
        let mut out = vec![0u8; chunk_size.max(1) as usize];
        let read = stdout
            .read(&mut out)
            .map_err(|error| format!("portable session_read_chunk tls failed: {error}"))?;
        out.truncate(read);
        return Ok(out);
    }
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let stream = match state.net_handles.get_mut(&handle) {
        Some(NetHandle::Stream(stream)) => stream,
        Some(_) => {
            return Err(format!(
                "portable session_read_chunk handle {handle} is not a plain stream"
            ))
        }
        None => return Err(format!("portable session_read_chunk unknown handle {handle}")),
    };
    let mut out = vec![0u8; chunk_size.max(1) as usize];
    let read = stream
        .stream
        .read(&mut out)
        .map_err(|error| format!("portable session_read_chunk failed: {error}"))?;
    out.truncate(read);
    Ok(out)
}

pub(crate) fn portable_net_write_all_handle(handle: u64, value: &[u8]) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let stream = match state.net_handles.get_mut(&handle) {
        Some(NetHandle::Stream(stream)) => stream,
        Some(_) => {
            return Err(format!(
                "portable net_write_handle_all handle {handle} is not a plain stream"
            ))
        }
        None => {
            return Err(format!(
                "portable net_write_handle_all unknown handle {handle}"
            ))
        }
    };
    stream
        .stream
        .write_all(value)
        .map_err(|error| format!("portable net_write_handle_all failed: {error}"))?;
    Ok(true)
}

pub(crate) fn portable_session_write_chunk(handle: u64, value: &[u8]) -> Result<bool, String> {
    let maybe_tls = {
        let state = runtime_state()
            .lock()
            .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
        match state.net_handles.get(&handle) {
            Some(NetHandle::TlsSession(shared)) => Some(shared.clone()),
            Some(NetHandle::Stream(_)) => None,
            Some(_) => {
                return Err(format!(
                    "portable session_write_chunk handle {handle} is not a session"
                ))
            }
            None => return Err(format!("portable session_write_chunk unknown handle {handle}")),
        }
    };
    if let Some(shared) = maybe_tls {
        let mut process = shared
            .lock()
            .map_err(|_| "portable tls session mutex poisoned".to_string())?;
        let Some(stdin) = process.stdin.as_mut() else {
            return Ok(false);
        };
        stdin
            .write_all(value)
            .map_err(|error| format!("portable session_write_chunk tls failed: {error}"))?;
        return Ok(true);
    }
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let stream = match state.net_handles.get_mut(&handle) {
        Some(NetHandle::Stream(stream)) => stream,
        Some(_) => {
            return Err(format!(
                "portable session_write_chunk handle {handle} is not a plain stream"
            ))
        }
        None => return Err(format!("portable session_write_chunk unknown handle {handle}")),
    };
    stream
        .stream
        .write_all(value)
        .map_err(|error| format!("portable session_write_chunk failed: {error}"))?;
    stream.pending_bytes = stream
        .pending_bytes
        .saturating_add(u32::try_from(value.len()).unwrap_or(u32::MAX));
    Ok(true)
}

pub(crate) fn portable_session_flush(handle: u64) -> Result<bool, String> {
    let maybe_tls = {
        let state = runtime_state()
            .lock()
            .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
        match state.net_handles.get(&handle) {
            Some(NetHandle::TlsSession(shared)) => Some(shared.clone()),
            Some(NetHandle::Stream(_)) => None,
            Some(_) => return Err(format!("portable session_flush handle {handle} is not a session")),
            None => return Err(format!("portable session_flush unknown handle {handle}")),
        }
    };
    if let Some(shared) = maybe_tls {
        let mut process = shared
            .lock()
            .map_err(|_| "portable tls session mutex poisoned".to_string())?;
        let Some(stdin) = process.stdin.as_mut() else {
            return Ok(false);
        };
        stdin
            .flush()
            .map_err(|error| format!("portable session_flush tls failed: {error}"))?;
        return Ok(true);
    }
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let stream = match state.net_handles.get_mut(&handle) {
        Some(NetHandle::Stream(stream)) => stream,
        Some(_) => return Err(format!("portable session_flush handle {handle} is not a plain stream")),
        None => return Err(format!("portable session_flush unknown handle {handle}")),
    };
    stream
        .stream
        .flush()
        .map_err(|error| format!("portable session_flush failed: {error}"))?;
    stream.pending_bytes = 0;
    Ok(true)
}

pub(crate) fn portable_session_alive(handle: u64) -> Result<bool, String> {
    let maybe_tls = {
        let state = runtime_state()
            .lock()
            .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
        match state.net_handles.get(&handle) {
            Some(NetHandle::TlsSession(shared)) => Some(shared.clone()),
            Some(NetHandle::Stream(_)) => None,
            Some(NetHandle::Listener(_)) | Some(NetHandle::TlsListener(_)) => return Ok(false),
            None => return Ok(false),
        }
    };
    if let Some(shared) = maybe_tls {
        let mut process = shared
            .lock()
            .map_err(|_| "portable tls session mutex poisoned".to_string())?;
        return match process.child.try_wait() {
            Ok(Some(_)) => Ok(false),
            Ok(None) => Ok(true),
            Err(error) => Err(format!("portable session_alive tls failed: {error}")),
        };
    }
    let state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    Ok(matches!(state.net_handles.get(&handle), Some(NetHandle::Stream(_))))
}

pub(crate) fn portable_session_heartbeat(handle: u64, value: &[u8]) -> Result<bool, String> {
    if !portable_session_write_chunk(handle, value)? {
        return Ok(false);
    }
    portable_session_flush(handle)
}

pub(crate) fn portable_session_backpressure(handle: u64) -> Result<u32, String> {
    let state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    match state.net_handles.get(&handle) {
        Some(NetHandle::Stream(stream)) => Ok(stream.pending_bytes),
        Some(NetHandle::TlsSession(_)) => Ok(0),
        Some(_) => Err(format!("portable session_backpressure handle {handle} is not a session")),
        None => Ok(0),
    }
}

pub(crate) fn portable_session_backpressure_wait(
    handle: u64,
    max_pending: u32,
) -> Result<bool, String> {
    let pending = portable_session_backpressure(handle)?;
    if pending <= max_pending {
        return Ok(true);
    }
    portable_session_flush(handle)?;
    Ok(true)
}

pub(crate) fn portable_session_resume_id(handle: u64) -> Result<u64, String> {
    let state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    match state.net_handles.get(&handle) {
        Some(NetHandle::Stream(stream)) => Ok(stream.resume_id),
        Some(NetHandle::TlsSession(_)) => Ok(handle),
        Some(_) => Err(format!("portable session_resume_id handle {handle} is not a session")),
        None => Ok(0),
    }
}

pub(crate) fn portable_session_reconnect(handle: u64) -> Result<bool, String> {
    let (host, port, timeout_ms, resume_id) = {
        let mut state = runtime_state()
            .lock()
            .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
        let Some(NetHandle::Stream(stream)) = state.net_handles.get_mut(&handle) else {
            return Ok(false);
        };
        let Some(host) = stream.reconnect_host.clone() else {
            return Ok(false);
        };
        let _ = stream.stream.shutdown(Shutdown::Both);
        (host, stream.reconnect_port, stream.timeout_ms, stream.resume_id)
    };
    let stream = TcpStream::connect((host.as_str(), port))
        .map_err(|error| format!("portable session_reconnect failed: {error}"))?;
    let timeout = Duration::from_millis(timeout_ms.max(1) as u64);
    stream
        .set_read_timeout(Some(timeout))
        .map_err(|error| format!("portable session_reconnect read timeout failed: {error}"))?;
    stream
        .set_write_timeout(Some(timeout))
        .map_err(|error| format!("portable session_reconnect write timeout failed: {error}"))?;
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    state.net_handles.insert(
        handle,
        NetHandle::Stream(PlainSessionHandle {
            stream,
            timeout_ms,
            reconnect_host: Some(host),
            reconnect_port: port,
            pending_bytes: 0,
            resume_id,
        }),
    );
    Ok(true)
}

pub(crate) fn portable_net_close_handle(handle: u64) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    match state.net_handles.remove(&handle) {
        Some(NetHandle::Stream(stream)) => {
            let _ = stream.stream.shutdown(Shutdown::Both);
            Ok(true)
        }
        Some(NetHandle::Listener(_)) => Ok(true),
        Some(NetHandle::TlsListener(shared)) | Some(NetHandle::TlsSession(shared)) => {
            drop(state);
            portable_tls_shutdown(&shared)
        }
        None => Ok(false),
    }
}

pub(crate) fn portable_listener_set_timeout_ms(
    handle: u64,
    timeout_ms: u32,
) -> Result<bool, String> {
    let shared = {
        let mut state = runtime_state()
            .lock()
            .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
        match state.net_handles.get_mut(&handle) {
            Some(NetHandle::Listener(listener)) => {
                listener.timeout_ms = timeout_ms;
                return Ok(true);
            }
            Some(NetHandle::TlsListener(shared)) => Some(shared.clone()),
            Some(_) => {
                return Err(format!(
                    "portable listener_set_timeout_ms handle {handle} is not a listener"
                ))
            }
            None => return Ok(false),
        }
    };
    let binding = shared.expect("tls listener handle should carry shared state");
    let mut process = binding
        .lock()
        .map_err(|_| "portable tls listener mutex poisoned".to_string())?;
    process.request_timeout_ms = timeout_ms;
    Ok(true)
}

pub(crate) fn portable_session_set_timeout_ms(
    handle: u64,
    timeout_ms: u32,
) -> Result<bool, String> {
    let shared = {
        let mut state = runtime_state()
            .lock()
            .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
        match state.net_handles.get_mut(&handle) {
            Some(NetHandle::Stream(stream)) => {
                stream.timeout_ms = timeout_ms;
                let timeout = Duration::from_millis(timeout_ms.max(1) as u64);
                stream
                    .stream
                    .set_read_timeout(Some(timeout))
                    .map_err(|error| {
                        format!("portable session_set_timeout_ms read timeout failed: {error}")
                    })?;
                stream
                    .stream
                    .set_write_timeout(Some(timeout))
                    .map_err(|error| {
                        format!("portable session_set_timeout_ms write timeout failed: {error}")
                    })?;
                return Ok(true);
            }
            Some(NetHandle::TlsSession(shared)) => Some(shared.clone()),
            Some(_) => {
                return Err(format!(
                    "portable session_set_timeout_ms handle {handle} is not a session"
                ))
            }
            None => return Ok(false),
        }
    };
    let binding = shared.expect("tls session handle should carry shared state");
    let mut process = binding
        .lock()
        .map_err(|_| "portable tls session mutex poisoned".to_string())?;
    process.session_timeout_ms = timeout_ms;
    Ok(true)
}

pub(crate) fn portable_listener_set_shutdown_grace_ms(
    handle: u64,
    grace_ms: u32,
) -> Result<bool, String> {
    let shared = {
        let mut state = runtime_state()
            .lock()
            .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
        match state.net_handles.get_mut(&handle) {
            Some(NetHandle::Listener(listener)) => {
                listener.shutdown_grace_ms = grace_ms;
                return Ok(true);
            }
            Some(NetHandle::TlsListener(shared)) => Some(shared.clone()),
            Some(_) => {
                return Err(format!(
                    "portable listener_set_shutdown_grace_ms handle {handle} is not a listener"
                ))
            }
            None => return Ok(false),
        }
    };
    let binding = shared.expect("tls listener handle should carry shared state");
    let mut process = binding
        .lock()
        .map_err(|_| "portable tls listener mutex poisoned".to_string())?;
    process.shutdown_grace_ms = grace_ms;
    Ok(true)
}

pub(crate) fn portable_http_session_accept(listener_handle: u64) -> Result<u64, String> {
    portable_net_accept_handle(listener_handle)
}

pub(crate) fn portable_http_session_request(handle: u64) -> Result<Vec<u8>, String> {
    let maybe_tls = {
        let state = runtime_state()
            .lock()
            .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
        match state.net_handles.get(&handle) {
            Some(NetHandle::TlsSession(shared)) => Some(shared.clone()),
            Some(NetHandle::Stream(_)) => None,
            Some(_) => {
                return Err(format!(
                    "portable http_session_request handle {handle} is not a session"
                ))
            }
            None => {
                return Err(format!(
                    "portable http_session_request unknown handle {handle}"
                ))
            }
        }
    };
    if let Some(shared) = maybe_tls {
        let mut process = shared
            .lock()
            .map_err(|_| "portable tls session mutex poisoned".to_string())?;
        let Some(stdout) = process.stdout.as_mut() else {
            return Ok(Vec::new());
        };
        return read_http_message(stdout);
    }
    portable_net_read_all_handle(handle)
}

pub(crate) fn portable_http_session_write_text(
    handle: u64,
    status: u32,
    body: &[u8],
) -> Result<bool, String> {
    portable_http_write_text_response(handle, status, body)
}

pub(crate) fn portable_http_session_write_json(
    handle: u64,
    status: u32,
    body: &[u8],
) -> Result<bool, String> {
    portable_http_write_json_response(handle, status, body)
}

pub(crate) fn portable_http_session_close(handle: u64) -> Result<bool, String> {
    portable_net_close_handle(handle)
}

fn sqlite3_command() -> String {
    for candidate in [
        "/Users/sheremetovegor/miniconda3/bin/sqlite3",
        "/opt/homebrew/bin/sqlite3",
        "/usr/local/bin/sqlite3",
        "/usr/bin/sqlite3",
        "sqlite3",
    ] {
        if candidate == "sqlite3" || Path::new(candidate).exists() {
            return candidate.to_string();
        }
    }
    "sqlite3".to_string()
}

fn postgres_client_image() -> &'static str {
    "postgres:16-alpine"
}

fn trim_sqlite_output(mut bytes: Vec<u8>) -> Vec<u8> {
    while matches!(bytes.last(), Some(b'\n' | b'\r')) {
        bytes.pop();
    }
    bytes
}

fn portable_db_run_sql(path: &str, sql: &[u8]) -> Result<Vec<u8>, String> {
    if path.is_empty() {
        return Err("portable db path is empty".to_string());
    }
    if let Some(parent) = Path::new(path).parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent).map_err(|error| {
                format!("portable db create parent dirs for {path} failed: {error}")
            })?;
        }
    }
    let sql_text = String::from_utf8_lossy(sql).to_string();
    let output = Command::new(sqlite3_command())
        .arg("-batch")
        .arg("-noheader")
        .arg(path)
        .arg(sql_text)
        .output()
        .map_err(|error| format!("portable sqlite3 execution failed: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "portable sqlite3 execution failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(output.stdout)
}

fn db_backend_kind(target: &str) -> DbBackendKind {
    if target.starts_with("postgres://") || target.starts_with("postgresql://") {
        DbBackendKind::Postgres
    } else {
        DbBackendKind::Sqlite
    }
}

fn docker_reachable_postgres_dsn(dsn: &str) -> String {
    dsn.replace("@127.0.0.1:", "@host.docker.internal:")
        .replace("@localhost:", "@host.docker.internal:")
}

fn portable_postgres_run_sql(dsn: &str, sql: &[u8]) -> Result<Vec<u8>, String> {
    if dsn.is_empty() {
        return Err("portable postgres target is empty".to_string());
    }
    let sql_text = String::from_utf8_lossy(sql).to_string();
    let docker_dsn = docker_reachable_postgres_dsn(dsn);
    let output = Command::new("docker")
        .args([
            "run",
            "--rm",
            postgres_client_image(),
            "psql",
            &docker_dsn,
            "-v",
            "ON_ERROR_STOP=1",
            "-At",
            "-c",
            &sql_text,
        ])
        .output()
        .map_err(|error| format!("portable postgres execution failed: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "portable postgres execution failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(output.stdout)
}

fn portable_db_run_sql_target(target: &str, sql: &[u8]) -> Result<Vec<u8>, String> {
    match db_backend_kind(target) {
        DbBackendKind::Sqlite => portable_db_run_sql(target, sql),
        DbBackendKind::Postgres => portable_postgres_run_sql(target, sql),
    }
}

fn sql_literal_from_json(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Bool(true) => "TRUE".to_string(),
        Value::Bool(false) => "FALSE".to_string(),
        Value::Number(number) => number.to_string(),
        Value::String(text) => format!("'{}'", text.replace('\'', "''")),
        other => {
            let encoded = serde_json::to_string(other).unwrap_or_else(|_| "null".to_string());
            format!("'{}'", encoded.replace('\'', "''"))
        }
    }
}

fn substitute_prepared_sql(template: &[u8], params_json: &[u8]) -> Result<Vec<u8>, String> {
    let params_value: Value = serde_json::from_slice(params_json)
        .map_err(|error| format!("invalid db params json: {error}"))?;
    let params = params_value
        .as_array()
        .ok_or_else(|| "db prepared params must be a JSON array".to_string())?;
    let mut out = String::from_utf8_lossy(template).to_string();
    for index in (1..=params.len()).rev() {
        let placeholder = format!("${index}");
        out = out.replace(&placeholder, &sql_literal_from_json(&params[index - 1]));
    }
    Ok(out.into_bytes())
}

fn db_error_class(error: &str) -> (u32, bool) {
    let lower = error.to_ascii_lowercase();
    if lower.contains("timeout") {
        (3, true)
    } else if lower.contains("refused")
        || lower.contains("connect")
        || lower.contains("unreachable")
        || lower.contains("docker")
    {
        (2, true)
    } else if lower.contains("syntax") || lower.contains("parse") {
        (4, false)
    } else {
        (1, false)
    }
}

fn db_set_error(handle: u64, error: &str) {
    if let Ok(mut state) = runtime_state().lock() {
        if let Some(entry) = state.db_handles.get_mut(&handle) {
            let (code, retryable) = db_error_class(error);
            entry.last_error_code = code;
            entry.last_error_retryable = retryable;
        }
    }
}

fn db_clear_error(handle: u64) {
    if let Ok(mut state) = runtime_state().lock() {
        if let Some(entry) = state.db_handles.get_mut(&handle) {
            entry.last_error_code = 0;
            entry.last_error_retryable = false;
        }
    }
}

fn portable_db_query_row_sqlite(target: &str, sql: &[u8]) -> Result<Vec<u8>, String> {
    let sql_text = String::from_utf8_lossy(sql).to_string();
    let output = Command::new(sqlite3_command())
        .arg("-json")
        .arg(target)
        .arg(sql_text)
        .output()
        .map_err(|error| format!("portable sqlite row query failed: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "portable sqlite row query failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    let rows: Value = serde_json::from_slice(&output.stdout)
        .map_err(|error| format!("portable sqlite row decode failed: {error}"))?;
    match rows {
        Value::Array(items) => Ok(items
            .into_iter()
            .next()
            .map(|value| serde_json::to_vec(&value).unwrap_or_default())
            .unwrap_or_default()),
        _ => Ok(Vec::new()),
    }
}

fn portable_db_query_row_postgres(dsn: &str, sql: &[u8]) -> Result<Vec<u8>, String> {
    let wrapped = format!(
        "SELECT COALESCE(row_to_json(t)::text, '{{}}') FROM ({}) AS t LIMIT 1;",
        String::from_utf8_lossy(sql)
    );
    let output = portable_postgres_run_sql(dsn, wrapped.as_bytes())?;
    Ok(trim_sqlite_output(output))
}

fn portable_db_query_row_target(target: &str, sql: &[u8]) -> Result<Vec<u8>, String> {
    match db_backend_kind(target) {
        DbBackendKind::Sqlite => portable_db_query_row_sqlite(target, sql),
        DbBackendKind::Postgres => portable_db_query_row_postgres(target, sql),
    }
}

fn file_hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

fn file_hex_decode(text: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(text.len() / 2);
    let bytes = text.as_bytes();
    let mut index = 0usize;
    while index + 1 < bytes.len() {
        let chunk = &text[index..index + 2];
        if let Ok(value) = u8::from_str_radix(chunk, 16) {
            out.push(value);
        }
        index += 2;
    }
    out
}

fn unix_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn ensure_parent_dirs(path: &str) -> Result<(), String> {
    if let Some(parent) = Path::new(path).parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)
                .map_err(|error| format!("create parent dirs for {path} failed: {error}"))?;
        }
    }
    Ok(())
}

type CacheFileEntries = HashMap<Vec<u8>, (Vec<u8>, Option<u64>)>;

fn load_cache_entries(path: &str) -> Result<CacheFileEntries, String> {
    let mut out = HashMap::new();
    let Ok(text) = fs::read_to_string(path) else {
        return Ok(out);
    };
    let now = unix_time_ms();
    for line in text.lines() {
        let mut parts = line.splitn(3, '\t');
        let Some(key_hex) = parts.next() else { continue };
        let Some(expiry_text) = parts.next() else { continue };
        let Some(value_hex) = parts.next() else { continue };
        let expiry = if expiry_text == "-" {
            None
        } else {
            expiry_text.parse::<u64>().ok()
        };
        if expiry.is_some_and(|deadline| deadline <= now) {
            continue;
        }
        out.insert(file_hex_decode(key_hex), (file_hex_decode(value_hex), expiry));
    }
    Ok(out)
}

fn write_cache_entries(path: &str, entries: &CacheFileEntries) -> Result<(), String> {
    ensure_parent_dirs(path)?;
    let mut lines = Vec::new();
    for (key, (value, expiry)) in entries {
        lines.push(format!(
            "{}\t{}\t{}",
            file_hex_encode(key),
            expiry.map(|value| value.to_string()).unwrap_or_else(|| "-".to_string()),
            file_hex_encode(value)
        ));
    }
    let payload = if lines.is_empty() {
        String::new()
    } else {
        format!("{}\n", lines.join("\n"))
    };
    fs::write(path, payload).map_err(|error| format!("write cache file {path} failed: {error}"))
}

fn load_queue_entries(path: &str) -> Result<Vec<Vec<u8>>, String> {
    let Ok(text) = fs::read_to_string(path) else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    for line in text.lines() {
        if line.is_empty() {
            continue;
        }
        out.push(file_hex_decode(line));
    }
    Ok(out)
}

fn write_queue_entries(path: &str, entries: &[Vec<u8>]) -> Result<(), String> {
    ensure_parent_dirs(path)?;
    let payload = if entries.is_empty() {
        String::new()
    } else {
        format!(
            "{}\n",
            entries
                .iter()
                .map(|entry| file_hex_encode(entry))
                .collect::<Vec<_>>()
                .join("\n")
        )
    };
    fs::write(path, payload).map_err(|error| format!("write queue file {path} failed: {error}"))
}

fn load_stream_entries(path: &str) -> Result<Vec<StreamEntry>, String> {
    let Ok(text) = fs::read_to_string(path) else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    for line in text.lines() {
        if line.is_empty() {
            continue;
        }
        let Some((offset, payload_hex)) = line.split_once('\t') else {
            continue;
        };
        let Some(offset) = offset.parse::<u32>().ok() else {
            continue;
        };
        out.push(StreamEntry {
            offset,
            payload: file_hex_decode(payload_hex),
        });
    }
    Ok(out)
}

fn write_stream_entries(path: &str, entries: &[StreamEntry]) -> Result<(), String> {
    ensure_parent_dirs(path)?;
    let payload = if entries.is_empty() {
        String::new()
    } else {
        format!(
            "{}\n",
            entries
                .iter()
                .map(|entry| format!("{}\t{}", entry.offset, file_hex_encode(&entry.payload)))
                .collect::<Vec<_>>()
                .join("\n")
        )
    };
    fs::write(path, payload).map_err(|error| format!("write stream file {path} failed: {error}"))
}

fn load_lease_owner(path: &str) -> Result<u32, String> {
    let Ok(text) = fs::read_to_string(path) else {
        return Ok(0);
    };
    for line in text.lines().rev() {
        if line.is_empty() {
            continue;
        }
        let Some((kind, owner_text)) = line.split_once('\t') else {
            continue;
        };
        let Some(owner) = owner_text.parse::<u32>().ok() else {
            continue;
        };
        return match kind {
            "H" => Ok(owner),
            "R" => Ok(0),
            _ => continue,
        };
    }
    Ok(0)
}

fn append_lease_line(path: &str, kind: &str, owner: u32) -> Result<(), String> {
    ensure_parent_dirs(path)?;
    let mut text = fs::read_to_string(path).unwrap_or_default();
    text.push_str(&format!("{kind}\t{owner}\n"));
    fs::write(path, text).map_err(|error| format!("write lease file {path} failed: {error}"))
}

fn load_placement_entries(path: &str) -> Result<HashMap<u32, u32>, String> {
    let Ok(text) = fs::read_to_string(path) else {
        return Ok(HashMap::new());
    };
    let mut out = HashMap::new();
    for line in text.lines() {
        if line.is_empty() {
            continue;
        }
        let mut parts = line.splitn(2, '\t');
        let Some(shard_text) = parts.next() else {
            continue;
        };
        let Some(node_text) = parts.next() else {
            continue;
        };
        let Some(shard) = shard_text.parse::<u32>().ok() else {
            continue;
        };
        let Some(node) = node_text.parse::<u32>().ok() else {
            continue;
        };
        out.insert(shard, node);
    }
    Ok(out)
}

fn write_placement_entries(path: &str, entries: &HashMap<u32, u32>) -> Result<(), String> {
    ensure_parent_dirs(path)?;
    let mut rows = entries
        .iter()
        .map(|(shard, node)| (*shard, *node))
        .collect::<Vec<_>>();
    rows.sort_by_key(|(shard, _)| *shard);
    let payload = if rows.is_empty() {
        String::new()
    } else {
        format!(
            "{}\n",
            rows.iter()
                .map(|(shard, node)| format!("{shard}\t{node}"))
                .collect::<Vec<_>>()
                .join("\n")
        )
    };
    fs::write(path, payload)
        .map_err(|error| format!("write placement file {path} failed: {error}"))
}

fn load_coord_entries(path: &str) -> Result<HashMap<String, u32>, String> {
    let Ok(text) = fs::read_to_string(path) else {
        return Ok(HashMap::new());
    };
    let mut out = HashMap::new();
    for line in text.lines() {
        if line.is_empty() {
            continue;
        }
        let mut parts = line.splitn(2, '\t');
        let Some(key) = parts.next() else { continue };
        let Some(value_text) = parts.next() else { continue };
        let Some(value) = value_text.parse::<u32>().ok() else {
            continue;
        };
        out.insert(key.to_string(), value);
    }
    Ok(out)
}

fn write_coord_entries(path: &str, entries: &HashMap<String, u32>) -> Result<(), String> {
    ensure_parent_dirs(path)?;
    let mut rows = entries
        .iter()
        .map(|(key, value)| (key.clone(), *value))
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| left.0.cmp(&right.0));
    let payload = if rows.is_empty() {
        String::new()
    } else {
        format!(
            "{}\n",
            rows.iter()
                .map(|(key, value)| format!("{key}\t{value}"))
                .collect::<Vec<_>>()
                .join("\n")
        )
    };
    fs::write(path, payload).map_err(|error| format!("write coord file {path} failed: {error}"))
}

pub(crate) fn portable_db_open(target: &str) -> Result<u64, String> {
    if target.is_empty() {
        return Err("portable db_open requires non-empty target".to_string());
    }
    if db_backend_kind(target) == DbBackendKind::Sqlite {
        if let Some(parent) = Path::new(target).parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent).map_err(|error| {
                    format!("portable db_open create parent dirs for {target} failed: {error}")
                })?;
            }
        }
    }
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let handle = alloc_runtime_handle(&mut state);
    state.db_handles.insert(
        handle,
        DbRuntimeHandle {
            target: target.to_string(),
            ..DbRuntimeHandle::default()
        },
    );
    Ok(handle)
}

pub(crate) fn portable_db_close(handle: u64) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    Ok(state.db_handles.remove(&handle).is_some())
}

fn db_target_for_handle(handle: u64) -> Result<String, String> {
    let state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    state
        .db_handles
        .get(&handle)
        .map(|entry| entry.target.clone())
        .ok_or_else(|| format!("portable db unknown handle {handle}"))
}

fn db_prepare_sql(handle: u64, name: &str, sql: &[u8]) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let entry = state
        .db_handles
        .get_mut(&handle)
        .ok_or_else(|| format!("portable db unknown handle {handle}"))?;
    entry.prepared.insert(name.to_string(), sql.to_vec());
    Ok(true)
}

fn db_expand_prepared_sql(handle: u64, name: &str, params: &[u8]) -> Result<Vec<u8>, String> {
    let state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let entry = state
        .db_handles
        .get(&handle)
        .ok_or_else(|| format!("portable db unknown handle {handle}"))?;
    let template = entry
        .prepared
        .get(name)
        .cloned()
        .ok_or_else(|| format!("portable db unknown prepared statement {name}"))?;
    drop(state);
    substitute_prepared_sql(&template, params)
}

fn db_with_transaction_buffer(handle: u64, sql: Vec<u8>) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let entry = state
        .db_handles
        .get_mut(&handle)
        .ok_or_else(|| format!("portable db unknown handle {handle}"))?;
    entry.tx_buffer.push(sql);
    Ok(true)
}

pub(crate) fn portable_db_begin(handle: u64) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let entry = state
        .db_handles
        .get_mut(&handle)
        .ok_or_else(|| format!("portable db unknown handle {handle}"))?;
    entry.in_transaction = true;
    entry.tx_buffer.clear();
    Ok(true)
}

pub(crate) fn portable_db_commit(handle: u64) -> Result<bool, String> {
    let (target, tx_buffer) = {
        let mut state = runtime_state()
            .lock()
            .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
        let entry = state
            .db_handles
            .get_mut(&handle)
            .ok_or_else(|| format!("portable db unknown handle {handle}"))?;
        if !entry.in_transaction {
            return Ok(false);
        }
        entry.in_transaction = false;
        (entry.target.clone(), std::mem::take(&mut entry.tx_buffer))
    };
    let mut sql = b"BEGIN;".to_vec();
    for statement in tx_buffer {
        sql.extend_from_slice(&statement);
        if !sql.ends_with(b";") {
            sql.push(b';');
        }
    }
    sql.extend_from_slice(b"COMMIT;");
    let _ = portable_db_run_sql_target(&target, &sql)?;
    Ok(true)
}

pub(crate) fn portable_db_rollback(handle: u64) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let entry = state
        .db_handles
        .get_mut(&handle)
        .ok_or_else(|| format!("portable db unknown handle {handle}"))?;
    let was_open = entry.in_transaction;
    entry.in_transaction = false;
    entry.tx_buffer.clear();
    Ok(was_open)
}

fn db_tx_open(handle: u64) -> Result<bool, String> {
    let state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let entry = state
        .db_handles
        .get(&handle)
        .ok_or_else(|| format!("portable db unknown handle {handle}"))?;
    Ok(entry.in_transaction)
}

pub(crate) fn portable_db_exec(handle: u64, sql: &[u8]) -> Result<bool, String> {
    if db_tx_open(handle)? {
        return db_with_transaction_buffer(handle, sql.to_vec());
    }
    let target = db_target_for_handle(handle)?;
    match portable_db_run_sql_target(&target, sql) {
        Ok(_) => db_clear_error(handle),
        Err(error) => {
            db_set_error(handle, &error);
            return Ok(false);
        }
    }
    Ok(true)
}

pub(crate) fn portable_db_query_u32(handle: u64, sql: &[u8]) -> Result<u32, String> {
    if db_tx_open(handle)? {
        return Err("portable db_query_u32 does not support open transaction buffers".to_string());
    }
    let target = db_target_for_handle(handle)?;
    let output = match portable_db_run_sql_target(&target, sql) {
        Ok(output) => {
            db_clear_error(handle);
            trim_sqlite_output(output)
        }
        Err(error) => {
            db_set_error(handle, &error);
            return Ok(0);
        }
    };
    let text = String::from_utf8_lossy(&output);
    Ok(text.trim().parse::<u32>().unwrap_or(0))
}

pub(crate) fn portable_db_query_buf(handle: u64, sql: &[u8]) -> Result<Vec<u8>, String> {
    if db_tx_open(handle)? {
        return Err("portable db_query_buf does not support open transaction buffers".to_string());
    }
    let target = db_target_for_handle(handle)?;
    match portable_db_run_sql_target(&target, sql) {
        Ok(output) => {
            db_clear_error(handle);
            Ok(trim_sqlite_output(output))
        }
        Err(error) => {
            db_set_error(handle, &error);
            Ok(Vec::new())
        }
    }
}

pub(crate) fn portable_db_query_row(handle: u64, sql: &[u8]) -> Result<Vec<u8>, String> {
    if db_tx_open(handle)? {
        return Err("portable db_query_row does not support open transaction buffers".to_string());
    }
    let target = db_target_for_handle(handle)?;
    match portable_db_query_row_target(&target, sql) {
        Ok(output) => {
            db_clear_error(handle);
            Ok(output)
        }
        Err(error) => {
            db_set_error(handle, &error);
            Ok(Vec::new())
        }
    }
}

pub(crate) fn portable_db_exec_prepared(
    handle: u64,
    name: &str,
    params: &[u8],
) -> Result<bool, String> {
    let sql = db_expand_prepared_sql(handle, name, params)?;
    portable_db_exec(handle, &sql)
}

pub(crate) fn portable_db_query_prepared_u32(
    handle: u64,
    name: &str,
    params: &[u8],
) -> Result<u32, String> {
    let sql = db_expand_prepared_sql(handle, name, params)?;
    portable_db_query_u32(handle, &sql)
}

pub(crate) fn portable_db_query_prepared_buf(
    handle: u64,
    name: &str,
    params: &[u8],
) -> Result<Vec<u8>, String> {
    let sql = db_expand_prepared_sql(handle, name, params)?;
    portable_db_query_buf(handle, &sql)
}

pub(crate) fn portable_db_query_prepared_row(
    handle: u64,
    name: &str,
    params: &[u8],
) -> Result<Vec<u8>, String> {
    let sql = db_expand_prepared_sql(handle, name, params)?;
    portable_db_query_row(handle, &sql)
}

pub(crate) fn portable_db_last_error_code(handle: u64) -> Result<u32, String> {
    let state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    Ok(state
        .db_handles
        .get(&handle)
        .map(|entry| entry.last_error_code)
        .unwrap_or(1))
}

pub(crate) fn portable_db_last_error_retryable(handle: u64) -> Result<bool, String> {
    let state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    Ok(state
        .db_handles
        .get(&handle)
        .map(|entry| entry.last_error_retryable)
        .unwrap_or(false))
}

pub(crate) fn portable_db_pool_open(target: &str, max_size: u32) -> Result<u64, String> {
    if target.is_empty() || max_size == 0 {
        return Err(
            "portable db_pool_open requires non-empty target and non-zero max size".to_string(),
        );
    }
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let handle = alloc_runtime_handle(&mut state);
    state.db_pools.insert(
        handle,
        DbRuntimePool {
            target: target.to_string(),
            max_size,
            max_idle: max_size,
            leased: Vec::new(),
        },
    );
    Ok(handle)
}

pub(crate) fn portable_db_pool_set_max_idle(pool_handle: u64, max_idle: u32) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let Some(pool) = state.db_pools.get_mut(&pool_handle) else {
        return Ok(false);
    };
    pool.max_idle = max_idle.min(pool.max_size);
    Ok(true)
}

pub(crate) fn portable_db_pool_leased(pool_handle: u64) -> Result<u32, String> {
    let state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    Ok(state
        .db_pools
        .get(&pool_handle)
        .map(|pool| pool.leased.len() as u32)
        .unwrap_or(0))
}

pub(crate) fn portable_db_pool_acquire(pool_handle: u64) -> Result<u64, String> {
    let target = {
        let state = runtime_state()
            .lock()
            .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
        let pool = state
            .db_pools
            .get(&pool_handle)
            .ok_or_else(|| format!("portable db unknown pool handle {pool_handle}"))?;
        if pool.leased.len() as u32 >= pool.max_size {
            return Err(format!("portable db pool {pool_handle} exhausted"));
        }
        pool.target.clone()
    };
    let db_handle = portable_db_open(&target)?;
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let pool = state
        .db_pools
        .get_mut(&pool_handle)
        .ok_or_else(|| format!("portable db unknown pool handle {pool_handle}"))?;
    pool.leased.push(db_handle);
    Ok(db_handle)
}

pub(crate) fn portable_db_pool_release(pool_handle: u64, db_handle: u64) -> Result<bool, String> {
    {
        let mut state = runtime_state()
            .lock()
            .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
        let pool = state
            .db_pools
            .get_mut(&pool_handle)
            .ok_or_else(|| format!("portable db unknown pool handle {pool_handle}"))?;
        if let Some(index) = pool.leased.iter().position(|handle| *handle == db_handle) {
            pool.leased.swap_remove(index);
        } else {
            return Ok(false);
        }
    }
    portable_db_close(db_handle)
}

pub(crate) fn portable_db_pool_close(pool_handle: u64) -> Result<bool, String> {
    let leased = {
        let mut state = runtime_state()
            .lock()
            .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
        let Some(pool) = state.db_pools.remove(&pool_handle) else {
            return Ok(false);
        };
        pool.leased
    };
    for handle in leased {
        let _ = portable_db_close(handle);
    }
    Ok(true)
}

pub(crate) fn portable_cache_open(target: &str) -> Result<u64, String> {
    ensure_parent_dirs(target)?;
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let handle = alloc_runtime_handle(&mut state);
    state.cache_handles.insert(
        handle,
        CacheHandle {
            target: target.to_string(),
        },
    );
    Ok(handle)
}

fn cache_target_for_handle(handle: u64) -> Result<String, String> {
    let state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    state
        .cache_handles
        .get(&handle)
        .map(|entry| entry.target.clone())
        .ok_or_else(|| format!("portable cache unknown handle {handle}"))
}

pub(crate) fn portable_cache_close(handle: u64) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    Ok(state.cache_handles.remove(&handle).is_some())
}

pub(crate) fn portable_cache_get_buf(handle: u64, key: &[u8]) -> Result<Vec<u8>, String> {
    let target = cache_target_for_handle(handle)?;
    let entries = load_cache_entries(&target)?;
    Ok(entries.get(key).map(|(value, _)| value.clone()).unwrap_or_default())
}

pub(crate) fn portable_cache_set_buf(
    handle: u64,
    key: &[u8],
    value: &[u8],
    ttl_ms: Option<u32>,
) -> Result<bool, String> {
    let target = cache_target_for_handle(handle)?;
    let mut entries = load_cache_entries(&target)?;
    let expiry = ttl_ms.map(|ttl| unix_time_ms().saturating_add(ttl as u64));
    entries.insert(key.to_vec(), (value.to_vec(), expiry));
    write_cache_entries(&target, &entries)?;
    Ok(true)
}

pub(crate) fn portable_cache_del(handle: u64, key: &[u8]) -> Result<bool, String> {
    let target = cache_target_for_handle(handle)?;
    let mut entries = load_cache_entries(&target)?;
    let removed = entries.remove(key).is_some();
    write_cache_entries(&target, &entries)?;
    Ok(removed)
}

pub(crate) fn portable_queue_open(target: &str) -> Result<u64, String> {
    ensure_parent_dirs(target)?;
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let handle = alloc_runtime_handle(&mut state);
    state.queue_handles.insert(
        handle,
        QueueHandle {
            target: target.to_string(),
        },
    );
    Ok(handle)
}

fn queue_target_for_handle(handle: u64) -> Result<String, String> {
    let state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    state
        .queue_handles
        .get(&handle)
        .map(|entry| entry.target.clone())
        .ok_or_else(|| format!("portable queue unknown handle {handle}"))
}

pub(crate) fn portable_queue_close(handle: u64) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    Ok(state.queue_handles.remove(&handle).is_some())
}

pub(crate) fn portable_queue_push_buf(handle: u64, value: &[u8]) -> Result<bool, String> {
    let target = queue_target_for_handle(handle)?;
    let mut entries = load_queue_entries(&target)?;
    entries.push(value.to_vec());
    write_queue_entries(&target, &entries)?;
    Ok(true)
}

pub(crate) fn portable_queue_pop_buf(handle: u64) -> Result<Vec<u8>, String> {
    let target = queue_target_for_handle(handle)?;
    let mut entries = load_queue_entries(&target)?;
    let out = if entries.is_empty() {
        Vec::new()
    } else {
        entries.remove(0)
    };
    write_queue_entries(&target, &entries)?;
    Ok(out)
}

pub(crate) fn portable_queue_len(handle: u64) -> Result<u32, String> {
    let target = queue_target_for_handle(handle)?;
    Ok(load_queue_entries(&target)?.len() as u32)
}

pub(crate) fn portable_stream_open(target: &str) -> Result<u64, String> {
    ensure_parent_dirs(target)?;
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let handle = alloc_runtime_handle(&mut state);
    state.stream_handles.insert(
        handle,
        StreamHandle {
            target: target.to_string(),
        },
    );
    Ok(handle)
}

fn stream_target_for_handle(handle: u64) -> Result<String, String> {
    let state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    state
        .stream_handles
        .get(&handle)
        .map(|entry| entry.target.clone())
        .ok_or_else(|| format!("portable stream unknown handle {handle}"))
}

pub(crate) fn portable_stream_close(handle: u64) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let removed = state.stream_handles.remove(&handle).is_some();
    state
        .stream_replays
        .retain(|_, replay| replay.stream_handle != handle);
    Ok(removed)
}

pub(crate) fn portable_stream_publish_buf(handle: u64, value: &[u8]) -> Result<u32, String> {
    let target = stream_target_for_handle(handle)?;
    let mut entries = load_stream_entries(&target)?;
    let next_offset = entries
        .last()
        .map(|entry| entry.offset.saturating_add(1))
        .unwrap_or(1);
    entries.push(StreamEntry {
        offset: next_offset,
        payload: value.to_vec(),
    });
    write_stream_entries(&target, &entries)?;
    Ok(next_offset)
}

pub(crate) fn portable_stream_len(handle: u64) -> Result<u32, String> {
    let target = stream_target_for_handle(handle)?;
    Ok(load_stream_entries(&target)?.len() as u32)
}

pub(crate) fn portable_stream_replay_open(
    handle: u64,
    from_offset: u32,
) -> Result<u64, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    if !state.stream_handles.contains_key(&handle) {
        return Ok(0);
    }
    let replay_handle = alloc_runtime_handle(&mut state);
    state.stream_replays.insert(
        replay_handle,
        StreamReplayHandle {
            stream_handle: handle,
            from_offset,
            cursor: 0,
            last_offset: 0,
        },
    );
    Ok(replay_handle)
}

pub(crate) fn portable_stream_replay_next(handle: u64) -> Result<Vec<u8>, String> {
    let (stream_handle, from_offset, cursor) = {
        let state = runtime_state()
            .lock()
            .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
        let Some(replay) = state.stream_replays.get(&handle) else {
            return Ok(Vec::new());
        };
        (replay.stream_handle, replay.from_offset, replay.cursor)
    };
    let target = stream_target_for_handle(stream_handle)?;
    let entries = load_stream_entries(&target)?;
    let matches: Vec<StreamEntry> = entries
        .into_iter()
        .filter(|entry| entry.offset >= from_offset)
        .collect();
    let Some(entry) = matches.get(cursor).cloned() else {
        return Ok(Vec::new());
    };
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    if let Some(replay) = state.stream_replays.get_mut(&handle) {
        replay.cursor = replay.cursor.saturating_add(1);
        replay.last_offset = entry.offset;
    }
    Ok(entry.payload)
}

pub(crate) fn portable_stream_replay_offset(handle: u64) -> Result<u32, String> {
    let state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    Ok(state
        .stream_replays
        .get(&handle)
        .map(|replay| replay.last_offset)
        .unwrap_or(0))
}

pub(crate) fn portable_stream_replay_close(handle: u64) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    Ok(state.stream_replays.remove(&handle).is_some())
}

pub(crate) fn portable_shard_route_u32(key: &[u8], shard_count: u32) -> Result<u32, String> {
    if shard_count == 0 {
        return Ok(0);
    }
    let mut hash = 2166136261u32;
    for byte in key {
        hash ^= *byte as u32;
        hash = hash.wrapping_mul(16777619u32);
    }
    Ok(hash % shard_count)
}

pub(crate) fn portable_lease_open(target: &str) -> Result<u64, String> {
    ensure_parent_dirs(target)?;
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let handle = alloc_runtime_handle(&mut state);
    state.lease_handles.insert(
        handle,
        LeaseHandle {
            target: target.to_string(),
        },
    );
    Ok(handle)
}

fn lease_target_for_handle(handle: u64) -> Result<String, String> {
    let state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    state
        .lease_handles
        .get(&handle)
        .map(|entry| entry.target.clone())
        .ok_or_else(|| format!("portable lease unknown handle {handle}"))
}

pub(crate) fn portable_lease_acquire(handle: u64, owner: u32) -> Result<bool, String> {
    let target = lease_target_for_handle(handle)?;
    let current = load_lease_owner(&target)?;
    if current == 0 || current == owner {
        append_lease_line(&target, "H", owner)?;
        Ok(true)
    } else {
        Ok(false)
    }
}

pub(crate) fn portable_lease_owner(handle: u64) -> Result<u32, String> {
    let target = lease_target_for_handle(handle)?;
    load_lease_owner(&target)
}

pub(crate) fn portable_lease_transfer(handle: u64, owner: u32) -> Result<bool, String> {
    let target = lease_target_for_handle(handle)?;
    let current = load_lease_owner(&target)?;
    if current == 0 {
        Ok(false)
    } else {
        append_lease_line(&target, "H", owner)?;
        Ok(true)
    }
}

pub(crate) fn portable_lease_release(handle: u64, owner: u32) -> Result<bool, String> {
    let target = lease_target_for_handle(handle)?;
    let current = load_lease_owner(&target)?;
    if current == owner && owner != 0 {
        append_lease_line(&target, "R", owner)?;
        Ok(true)
    } else {
        Ok(false)
    }
}

pub(crate) fn portable_lease_close(handle: u64) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    Ok(state.lease_handles.remove(&handle).is_some())
}

pub(crate) fn portable_placement_open(target: &str) -> Result<u64, String> {
    ensure_parent_dirs(target)?;
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let handle = alloc_runtime_handle(&mut state);
    state.placement_handles.insert(
        handle,
        PlacementHandle {
            target: target.to_string(),
        },
    );
    Ok(handle)
}

fn placement_target_for_handle(handle: u64) -> Result<String, String> {
    let state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    state
        .placement_handles
        .get(&handle)
        .map(|entry| entry.target.clone())
        .ok_or_else(|| format!("portable placement unknown handle {handle}"))
}

pub(crate) fn portable_placement_assign(handle: u64, shard: u32, node: u32) -> Result<bool, String> {
    let target = placement_target_for_handle(handle)?;
    let mut entries = load_placement_entries(&target)?;
    entries.insert(shard, node);
    write_placement_entries(&target, &entries)?;
    Ok(true)
}

pub(crate) fn portable_placement_lookup(handle: u64, shard: u32) -> Result<u32, String> {
    let target = placement_target_for_handle(handle)?;
    Ok(load_placement_entries(&target)?.get(&shard).copied().unwrap_or(0))
}

pub(crate) fn portable_placement_close(handle: u64) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    Ok(state.placement_handles.remove(&handle).is_some())
}

pub(crate) fn portable_coord_open(target: &str) -> Result<u64, String> {
    ensure_parent_dirs(target)?;
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let handle = alloc_runtime_handle(&mut state);
    state.coord_handles.insert(
        handle,
        CoordHandle {
            target: target.to_string(),
        },
    );
    Ok(handle)
}

fn coord_target_for_handle(handle: u64) -> Result<String, String> {
    let state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    state
        .coord_handles
        .get(&handle)
        .map(|entry| entry.target.clone())
        .ok_or_else(|| format!("portable coord unknown handle {handle}"))
}

pub(crate) fn portable_coord_store_u32(handle: u64, key: &str, value: u32) -> Result<bool, String> {
    let target = coord_target_for_handle(handle)?;
    let mut entries = load_coord_entries(&target)?;
    entries.insert(key.to_string(), value);
    write_coord_entries(&target, &entries)?;
    Ok(true)
}

pub(crate) fn portable_coord_load_u32(handle: u64, key: &str) -> Result<u32, String> {
    let target = coord_target_for_handle(handle)?;
    Ok(load_coord_entries(&target)?
        .get(key)
        .copied()
        .unwrap_or(0))
}

pub(crate) fn portable_coord_close(handle: u64) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    Ok(state.coord_handles.remove(&handle).is_some())
}

pub(crate) fn portable_batch_open() -> Result<u64, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let handle = alloc_runtime_handle(&mut state);
    state.batch_handles.insert(handle, BatchHandle::default());
    Ok(handle)
}

pub(crate) fn portable_batch_push_u64(handle: u64, value: u64) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let Some(batch) = state.batch_handles.get_mut(&handle) else {
        return Ok(false);
    };
    batch.values.push(value);
    Ok(true)
}

pub(crate) fn portable_batch_len(handle: u64) -> Result<u32, String> {
    let state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    Ok(state
        .batch_handles
        .get(&handle)
        .map(|batch| batch.values.len() as u32)
        .unwrap_or(0))
}

pub(crate) fn portable_batch_flush_sum_u64(handle: u64) -> Result<u64, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let Some(batch) = state.batch_handles.get_mut(&handle) else {
        return Ok(0);
    };
    let sum = batch
        .values
        .iter()
        .copied()
        .fold(0u64, |acc, value| acc.saturating_add(value));
    batch.values.clear();
    Ok(sum)
}

pub(crate) fn portable_batch_close(handle: u64) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    Ok(state.batch_handles.remove(&handle).is_some())
}

pub(crate) fn portable_agg_open_u64() -> Result<u64, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let handle = alloc_runtime_handle(&mut state);
    state.aggregate_handles.insert(handle, AggregateHandle::default());
    Ok(handle)
}

pub(crate) fn portable_agg_add_u64(handle: u64, value: u64) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let Some(aggregate) = state.aggregate_handles.get_mut(&handle) else {
        return Ok(false);
    };
    aggregate.count = aggregate.count.saturating_add(1);
    aggregate.sum = aggregate.sum.saturating_add(value);
    if !aggregate.has_value {
        aggregate.min = value;
        aggregate.max = value;
        aggregate.has_value = true;
    } else {
        aggregate.min = aggregate.min.min(value);
        aggregate.max = aggregate.max.max(value);
    }
    Ok(true)
}

pub(crate) fn portable_agg_count(handle: u64) -> Result<u32, String> {
    let state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    Ok(state
        .aggregate_handles
        .get(&handle)
        .map(|aggregate| aggregate.count)
        .unwrap_or(0))
}

pub(crate) fn portable_agg_sum_u64(handle: u64) -> Result<u64, String> {
    let state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    Ok(state
        .aggregate_handles
        .get(&handle)
        .map(|aggregate| aggregate.sum)
        .unwrap_or(0))
}

pub(crate) fn portable_agg_avg_u64(handle: u64) -> Result<u64, String> {
    let state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    Ok(state
        .aggregate_handles
        .get(&handle)
        .map(|aggregate| {
            if aggregate.count == 0 {
                0
            } else {
                aggregate.sum / aggregate.count as u64
            }
        })
        .unwrap_or(0))
}

pub(crate) fn portable_agg_min_u64(handle: u64) -> Result<u64, String> {
    let state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    Ok(state
        .aggregate_handles
        .get(&handle)
        .map(|aggregate| if aggregate.has_value { aggregate.min } else { 0 })
        .unwrap_or(0))
}

pub(crate) fn portable_agg_max_u64(handle: u64) -> Result<u64, String> {
    let state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    Ok(state
        .aggregate_handles
        .get(&handle)
        .map(|aggregate| if aggregate.has_value { aggregate.max } else { 0 })
        .unwrap_or(0))
}

pub(crate) fn portable_agg_close(handle: u64) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    Ok(state.aggregate_handles.remove(&handle).is_some())
}

fn trim_window(window: &mut WindowHandle) {
    let now = unix_time_ms();
    window.entries.retain(|(timestamp, _)| {
        now.saturating_sub(*timestamp) <= window.width_ms as u64
    });
}

pub(crate) fn portable_window_open_ms(width_ms: u32) -> Result<u64, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let handle = alloc_runtime_handle(&mut state);
    state.window_handles.insert(
        handle,
        WindowHandle {
            width_ms,
            entries: Vec::new(),
        },
    );
    Ok(handle)
}

pub(crate) fn portable_window_add_u64(handle: u64, value: u64) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let Some(window) = state.window_handles.get_mut(&handle) else {
        return Ok(false);
    };
    window.entries.push((unix_time_ms(), value));
    trim_window(window);
    Ok(true)
}

pub(crate) fn portable_window_count(handle: u64) -> Result<u32, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let Some(window) = state.window_handles.get_mut(&handle) else {
        return Ok(0);
    };
    trim_window(window);
    Ok(window.entries.len() as u32)
}

pub(crate) fn portable_window_sum_u64(handle: u64) -> Result<u64, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let Some(window) = state.window_handles.get_mut(&handle) else {
        return Ok(0);
    };
    trim_window(window);
    Ok(window
        .entries
        .iter()
        .map(|(_, value)| *value)
        .fold(0u64, |acc, value| acc.saturating_add(value)))
}

pub(crate) fn portable_window_avg_u64(handle: u64) -> Result<u64, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let Some(window) = state.window_handles.get_mut(&handle) else {
        return Ok(0);
    };
    trim_window(window);
    if window.entries.is_empty() {
        return Ok(0);
    }
    let sum = window
        .entries
        .iter()
        .map(|(_, value)| *value)
        .fold(0u64, |acc, value| acc.saturating_add(value));
    Ok(sum / window.entries.len() as u64)
}

pub(crate) fn portable_window_min_u64(handle: u64) -> Result<u64, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let Some(window) = state.window_handles.get_mut(&handle) else {
        return Ok(0);
    };
    trim_window(window);
    Ok(window
        .entries
        .iter()
        .map(|(_, value)| *value)
        .min()
        .unwrap_or(0))
}

pub(crate) fn portable_window_max_u64(handle: u64) -> Result<u64, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let Some(window) = state.window_handles.get_mut(&handle) else {
        return Ok(0);
    };
    trim_window(window);
    Ok(window
        .entries
        .iter()
        .map(|(_, value)| *value)
        .max()
        .unwrap_or(0))
}

pub(crate) fn portable_window_close(handle: u64) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    Ok(state.window_handles.remove(&handle).is_some())
}

pub(crate) fn portable_db_prepare(handle: u64, name: &str, sql: &[u8]) -> Result<bool, String> {
    db_prepare_sql(handle, name, sql)
}

fn parse_http_request_line(request: &[u8]) -> Option<(&str, &str)> {
    let line_end = request
        .windows(2)
        .position(|window| window == b"\r\n")
        .unwrap_or(request.len());
    let line = std::str::from_utf8(&request[..line_end]).ok()?;
    let mut parts = line.split_whitespace();
    let method = parts.next()?;
    let path = parts.next()?;
    Some((method, path))
}

fn http_message_complete_len(buf: &[u8]) -> Option<usize> {
    let header_end = buf.windows(4).position(|window| window == b"\r\n\r\n")? + 4;
    let headers = std::str::from_utf8(&buf[..header_end]).ok()?;
    let mut content_length = 0usize;
    for line in headers.lines() {
        let line = line.trim_end_matches('\r');
        if let Some((name, value)) = line.split_once(':') {
            if name.trim().eq_ignore_ascii_case("Content-Length") {
                content_length = value.trim().parse::<usize>().ok()?;
                break;
            }
        }
    }
    let total = header_end.saturating_add(content_length);
    (buf.len() >= total).then_some(total)
}

fn read_http_message<R: Read>(reader: &mut R) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let read = reader
            .read(&mut chunk)
            .map_err(|error| format!("portable http message read failed: {error}"))?;
        if read == 0 {
            return Ok(out);
        }
        out.extend_from_slice(&chunk[..read]);
        if let Some(total) = http_message_complete_len(&out) {
            out.truncate(total);
            return Ok(out);
        }
    }
}

fn http_path_without_query(path: &str) -> &str {
    path.split_once('?').map(|(path, _)| path).unwrap_or(path)
}

pub(crate) fn portable_http_method_eq(request: &[u8], method: &str) -> bool {
    parse_http_request_line(request)
        .map(|(found, _)| found == method)
        .unwrap_or(false)
}

pub(crate) fn portable_http_path_eq(request: &[u8], path: &str) -> bool {
    parse_http_request_line(request)
        .map(|(_, found)| http_path_without_query(found) == path)
        .unwrap_or(false)
}

pub(crate) fn portable_http_request_method(request: &[u8]) -> Vec<u8> {
    parse_http_request_line(request)
        .map(|(method, _)| method.as_bytes().to_vec())
        .unwrap_or_default()
}

pub(crate) fn portable_http_request_path(request: &[u8]) -> Vec<u8> {
    parse_http_request_line(request)
        .map(|(_, path)| http_path_without_query(path).as_bytes().to_vec())
        .unwrap_or_default()
}

pub(crate) fn portable_http_route_param(request: &[u8], pattern: &str, param: &str) -> Vec<u8> {
    let Some((_, raw_path)) = parse_http_request_line(request) else {
        return Vec::new();
    };
    let path = http_path_without_query(raw_path);
    let path_segments: Vec<&str> = path
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect();
    let pattern_segments: Vec<&str> = pattern
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect();
    if path_segments.len() != pattern_segments.len() {
        return Vec::new();
    }
    for (found, expected) in path_segments.iter().zip(pattern_segments.iter()) {
        if let Some(name) = expected.strip_prefix(':') {
            if name == param {
                return found.as_bytes().to_vec();
            }
            continue;
        }
        if found != expected {
            return Vec::new();
        }
    }
    Vec::new()
}

fn http_header_lines(request: &[u8]) -> &[u8] {
    let line_end = request
        .windows(2)
        .position(|window| window == b"\r\n")
        .map(|index| index + 2)
        .unwrap_or(request.len());
    let body_start = request
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|index| index + 4)
        .unwrap_or(request.len());
    if line_end >= body_start || body_start > request.len() {
        &[]
    } else {
        &request[line_end..body_start - 2]
    }
}

pub(crate) fn decode_escaped_literal_bytes(literal: &str) -> Vec<u8> {
    let bytes = literal.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] != b'\\' {
            out.push(bytes[index]);
            index += 1;
            continue;
        }
        if index + 1 >= bytes.len() {
            out.push(b'\\');
            break;
        }
        match bytes[index + 1] {
            b'\\' => {
                out.push(b'\\');
                index += 2;
            }
            b'"' => {
                out.push(b'"');
                index += 2;
            }
            b'n' => {
                out.push(b'\n');
                index += 2;
            }
            b'r' => {
                out.push(b'\r');
                index += 2;
            }
            b't' => {
                out.push(b'\t');
                index += 2;
            }
            b'x' if index + 3 < bytes.len() => {
                let hi = bytes[index + 2];
                let lo = bytes[index + 3];
                if let (Some(hi), Some(lo)) = (hex_value(hi), hex_value(lo)) {
                    out.push((hi << 4) | lo);
                    index += 4;
                } else {
                    out.push(b'\\');
                    index += 1;
                }
            }
            _ => {
                out.push(bytes[index + 1]);
                index += 2;
            }
        }
    }
    out
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

pub(crate) fn portable_http_header_eq(request: &[u8], name: &str, value: &str) -> bool {
    let Ok(headers) = std::str::from_utf8(http_header_lines(request)) else {
        return false;
    };
    headers.lines().any(|line| {
        let line = line.trim_end_matches('\r');
        let Some((found_name, found_value)) = line.split_once(':') else {
            return false;
        };
        found_name.trim().eq_ignore_ascii_case(name) && found_value.trim() == value
    })
}

fn portable_http_cookie_value_from_header(cookie_header: &[u8], name: &str) -> Vec<u8> {
    let Ok(cookie_text) = std::str::from_utf8(cookie_header) else {
        return Vec::new();
    };
    for pair in cookie_text.split(';') {
        let trimmed = pair.trim();
        let Some((found_name, found_value)) = trimmed.split_once('=') else {
            continue;
        };
        if found_name.trim() == name {
            return found_value.trim().as_bytes().to_vec();
        }
    }
    Vec::new()
}

pub(crate) fn portable_http_cookie_eq(request: &[u8], name: &str, value: &str) -> bool {
    portable_http_cookie(request, name) == value.as_bytes()
}

pub(crate) fn portable_http_status_u32(value: &[u8]) -> u32 {
    let line_end = value
        .windows(2)
        .position(|window| window == b"\r\n")
        .unwrap_or(value.len());
    let Ok(line) = std::str::from_utf8(&value[..line_end]) else {
        return 0;
    };
    let mut parts = line.split_whitespace();
    let protocol = parts.next().unwrap_or_default();
    if !protocol.starts_with("HTTP/") {
        return 0;
    }
    parts
        .next()
        .and_then(|status| status.parse::<u32>().ok())
        .unwrap_or(0)
}

pub(crate) fn portable_buf_eq_lit(value: &[u8], literal: &str) -> bool {
    value == decode_escaped_literal_bytes(literal)
}

pub(crate) fn portable_buf_contains_lit(value: &[u8], literal: &str) -> bool {
    let needle = decode_escaped_literal_bytes(literal);
    !needle.is_empty() && value.windows(needle.len()).any(|window| window == needle)
}

pub(crate) fn portable_buf_parse_u32(value: &[u8]) -> u32 {
    String::from_utf8_lossy(value)
        .trim()
        .parse::<u32>()
        .unwrap_or(0)
}

pub(crate) fn portable_buf_parse_bool(value: &[u8]) -> bool {
    matches!(
        String::from_utf8_lossy(value)
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "true" | "1" | "yes" | "on"
    )
}

pub(crate) fn portable_http_header(request: &[u8], name: &str) -> Vec<u8> {
    let Ok(headers) = std::str::from_utf8(http_header_lines(request)) else {
        return Vec::new();
    };
    for line in headers.lines() {
        let line = line.trim_end_matches('\r');
        let Some((found_name, found_value)) = line.split_once(':') else {
            continue;
        };
        if found_name.trim().eq_ignore_ascii_case(name) {
            return found_value.trim().as_bytes().to_vec();
        }
    }
    Vec::new()
}

pub(crate) fn portable_http_cookie(request: &[u8], name: &str) -> Vec<u8> {
    let cookie_header = portable_http_header(request, "Cookie");
    if cookie_header.is_empty() {
        return Vec::new();
    }
    portable_http_cookie_value_from_header(&cookie_header, name)
}

pub(crate) fn portable_http_query_param(request: &[u8], key: &str) -> Vec<u8> {
    let request_text = String::from_utf8_lossy(request);
    let line = request_text.lines().next().unwrap_or_default();
    let mut parts = line.split_whitespace();
    let _method = parts.next();
    let path = parts.next().unwrap_or_default();
    let query = match path.split_once('?') {
        Some((_path, query)) => query,
        None => return Vec::new(),
    };
    for pair in query.split('&') {
        let (found_key, found_value) = match pair.split_once('=') {
            Some(parts) => parts,
            None => continue,
        };
        if found_key == key {
            return found_value.as_bytes().to_vec();
        }
    }
    Vec::new()
}

fn portable_http_header_pairs(request: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
    let Ok(headers) = std::str::from_utf8(http_header_lines(request)) else {
        return Vec::new();
    };
    headers
        .lines()
        .filter_map(|line| {
            let (name, value) = line.split_once(':')?;
            Some((
                name.trim().as_bytes().to_vec(),
                value.trim().as_bytes().to_vec(),
            ))
        })
        .collect()
}

pub(crate) fn portable_http_header_count(request: &[u8]) -> u32 {
    portable_http_header_pairs(request).len() as u32
}

pub(crate) fn portable_http_header_name(request: &[u8], index: u32) -> Vec<u8> {
    portable_http_header_pairs(request)
        .into_iter()
        .nth(index as usize)
        .map(|(name, _)| name)
        .unwrap_or_default()
}

pub(crate) fn portable_http_header_value(request: &[u8], index: u32) -> Vec<u8> {
    portable_http_header_pairs(request)
        .into_iter()
        .nth(index as usize)
        .map(|(_, value)| value)
        .unwrap_or_default()
}

pub(crate) fn portable_http_body(request: &[u8]) -> Vec<u8> {
    request
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|index| request[index + 4..].to_vec())
        .unwrap_or_default()
}

fn portable_http_multipart_boundary(request: &[u8]) -> Vec<u8> {
    let content_type = portable_http_header(request, "Content-Type");
    let Ok(text) = std::str::from_utf8(&content_type) else {
        return Vec::new();
    };
    for part in text.split(';') {
        let trimmed = part.trim();
        if let Some(value) = trimmed.strip_prefix("boundary=") {
            return value.trim_matches('"').as_bytes().to_vec();
        }
    }
    Vec::new()
}

fn portable_http_multipart_parts(request: &[u8]) -> Vec<(Vec<u8>, Vec<u8>, Vec<u8>)> {
    let boundary = portable_http_multipart_boundary(request);
    if boundary.is_empty() {
        return Vec::new();
    }
    let body = portable_http_body(request);
    let boundary_marker = [b"--".as_slice(), boundary.as_slice()].concat();
    let body_text = body;
    let mut parts = Vec::new();
    let payload = String::from_utf8_lossy(&body_text);
    let boundary_text = String::from_utf8_lossy(&boundary_marker);
    for segment in payload.split(boundary_text.as_ref()) {
        let trimmed = segment.trim();
        if trimmed.is_empty() || trimmed == "--" {
            continue;
        }
        let normalized = trimmed.trim_start_matches("\r\n").trim_end_matches("--");
        let Some((header_text, body_text)) = normalized.split_once("\r\n\r\n") else {
            continue;
        };
        let mut name = Vec::new();
        let mut filename = Vec::new();
        for header_line in header_text.lines() {
            if let Some((header_name, header_value)) = header_line.split_once(':') {
                if !header_name.eq_ignore_ascii_case("Content-Disposition") {
                    continue;
                }
                for token in header_value.split(';') {
                    let token = token.trim();
                    if let Some(value) = token.strip_prefix("name=") {
                        name = value.trim_matches('"').as_bytes().to_vec();
                    } else if let Some(value) = token.strip_prefix("filename=") {
                        filename = value.trim_matches('"').as_bytes().to_vec();
                    }
                }
            }
        }
        parts.push((
            name,
            filename,
            body_text
                .trim_end_matches("\r\n")
                .as_bytes()
                .to_vec(),
        ));
    }
    parts
}

pub(crate) fn portable_http_multipart_part_count(request: &[u8]) -> u32 {
    portable_http_multipart_parts(request).len() as u32
}

pub(crate) fn portable_http_multipart_part_name(request: &[u8], index: u32) -> Vec<u8> {
    portable_http_multipart_parts(request)
        .into_iter()
        .nth(index as usize)
        .map(|(name, _, _)| name)
        .unwrap_or_default()
}

pub(crate) fn portable_http_multipart_part_filename(request: &[u8], index: u32) -> Vec<u8> {
    portable_http_multipart_parts(request)
        .into_iter()
        .nth(index as usize)
        .map(|(_, filename, _)| filename)
        .unwrap_or_default()
}

pub(crate) fn portable_http_multipart_part_body(request: &[u8], index: u32) -> Vec<u8> {
    portable_http_multipart_parts(request)
        .into_iter()
        .nth(index as usize)
        .map(|(_, _, body)| body)
        .unwrap_or_default()
}

pub(crate) fn portable_http_body_limit(request: &[u8], limit: u32) -> bool {
    portable_http_body(request).len() <= limit as usize
}

pub(crate) fn portable_http_body_stream_open(request: &[u8]) -> Result<u64, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let handle = alloc_runtime_handle(&mut state);
    state.http_body_streams.insert(
        handle,
        HttpBodyStreamHandle {
            body: portable_http_body(request),
            cursor: 0,
        },
    );
    Ok(handle)
}

pub(crate) fn portable_http_body_stream_next(
    handle: u64,
    chunk_size: u32,
) -> Result<Vec<u8>, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let stream = state
        .http_body_streams
        .get_mut(&handle)
        .ok_or_else(|| format!("portable http_body_stream_next unknown handle {handle}"))?;
    let end = stream
        .cursor
        .saturating_add(chunk_size as usize)
        .min(stream.body.len());
    let out = stream.body[stream.cursor..end].to_vec();
    stream.cursor = end;
    Ok(out)
}

pub(crate) fn portable_http_body_stream_close(handle: u64) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    Ok(state.http_body_streams.remove(&handle).is_some())
}

fn portable_http_session_write_raw(handle: u64, value: &[u8]) -> Result<bool, String> {
    let maybe_tls = {
        let state = runtime_state()
            .lock()
            .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
        match state.net_handles.get(&handle) {
            Some(NetHandle::TlsSession(shared)) => Some(shared.clone()),
            Some(NetHandle::Stream(_)) => None,
            Some(_) => {
                return Err(format!(
                    "portable http write handle {handle} is not a session"
                ))
            }
            None => return Err(format!("portable http write unknown handle {handle}")),
        }
    };
    if let Some(shared) = maybe_tls {
        let mut process = shared
            .lock()
            .map_err(|_| "portable tls session mutex poisoned".to_string())?;
        let Some(stdin) = process.stdin.as_mut() else {
            return Ok(false);
        };
        stdin
            .write_all(value)
            .map_err(|error| format!("portable tls session write failed: {error}"))?;
        return Ok(true);
    }
    portable_net_write_all_handle(handle, value)
}

pub(crate) fn portable_http_response_stream_open(
    handle: u64,
    status: u32,
    content_type: &str,
) -> Result<u64, String> {
    let header = format!(
        "HTTP/1.1 {} {}\r\nTransfer-Encoding: chunked\r\nConnection: close\r\nContent-Type: {}\r\n\r\n",
        status,
        http_reason_phrase(status),
        content_type
    );
    if !portable_http_session_write_raw(handle, header.as_bytes())? {
        return Ok(0);
    }
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let stream_handle = alloc_runtime_handle(&mut state);
    state.http_response_streams.insert(
        stream_handle,
        HttpResponseStreamHandle {
            session_handle: handle,
            closed: false,
        },
    );
    Ok(stream_handle)
}

pub(crate) fn portable_http_response_stream_write(
    handle: u64,
    body: &[u8],
) -> Result<bool, String> {
    let session_handle = {
        let state = runtime_state()
            .lock()
            .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
        let stream = state
            .http_response_streams
            .get(&handle)
            .ok_or_else(|| format!("portable http_response_stream_write unknown handle {handle}"))?;
        if stream.closed {
            return Ok(false);
        }
        stream.session_handle
    };
    let mut chunk = format!("{:X}\r\n", body.len()).into_bytes();
    chunk.extend_from_slice(body);
    chunk.extend_from_slice(b"\r\n");
    portable_http_session_write_raw(session_handle, &chunk)
}

pub(crate) fn portable_http_response_stream_close(handle: u64) -> Result<bool, String> {
    let session_handle = {
        let mut state = runtime_state()
            .lock()
            .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
        let Some(mut stream) = state.http_response_streams.remove(&handle) else {
            return Ok(false);
        };
        stream.closed = true;
        stream.session_handle
    };
    portable_http_session_write_raw(session_handle, b"0\r\n\r\n")
}

pub(crate) fn portable_http_client_open(host: &str, port: u16) -> Result<u64, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let handle = alloc_runtime_handle(&mut state);
    state.http_clients.insert(
        handle,
        HttpClientHandle {
            host: host.to_string(),
            port,
        },
    );
    Ok(handle)
}

pub(crate) fn portable_http_client_request(handle: u64, request: &[u8]) -> Result<Vec<u8>, String> {
    let (host, port) = {
        let state = runtime_state()
            .lock()
            .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
        let client = state
            .http_clients
            .get(&handle)
            .ok_or_else(|| format!("portable http_client_request unknown handle {handle}"))?;
        (client.host.clone(), client.port)
    };
    portable_net_exchange_all(&host, port, request)
}

pub(crate) fn portable_http_client_request_retry(
    handle: u64,
    retries: u32,
    backoff_ms: u32,
    request: &[u8],
) -> Result<Vec<u8>, String> {
    for attempt in 0..=retries {
        let response = portable_http_client_request(handle, request)?;
        if !response.is_empty() {
            return Ok(response);
        }
        if attempt < retries {
            thread::sleep(Duration::from_millis(backoff_ms as u64));
        }
    }
    Ok(Vec::new())
}

pub(crate) fn portable_http_client_close(handle: u64) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    Ok(state.http_clients.remove(&handle).is_some())
}

pub(crate) fn portable_http_client_pool_open(
    host: &str,
    port: u16,
    max_size: u32,
) -> Result<u64, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let handle = alloc_runtime_handle(&mut state);
    state.http_client_pools.insert(
        handle,
        HttpClientPoolHandle {
            host: host.to_string(),
            port,
            max_size,
            leased: Vec::new(),
        },
    );
    Ok(handle)
}

pub(crate) fn portable_http_client_pool_acquire(pool_handle: u64) -> Result<u64, String> {
    let (host, port, max_size) = {
        let state = runtime_state()
            .lock()
            .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
        let pool = state.http_client_pools.get(&pool_handle).ok_or_else(|| {
            format!("portable http_client_pool_acquire unknown handle {pool_handle}")
        })?;
        (pool.host.clone(), pool.port, pool.max_size)
    };
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let leased_len = state
        .http_client_pools
        .get(&pool_handle)
        .ok_or_else(|| format!("portable http_client_pool_acquire unknown handle {pool_handle}"))?
        .leased
        .len();
    if leased_len >= max_size as usize {
        return Ok(0);
    }
    let handle = alloc_runtime_handle(&mut state);
    state.http_clients.insert(
        handle,
        HttpClientHandle {
            host,
            port,
        },
    );
    let pool = state.http_client_pools.get_mut(&pool_handle).ok_or_else(|| {
        format!("portable http_client_pool_acquire unknown handle {pool_handle}")
    })?;
    pool.leased.push(handle);
    Ok(handle)
}

pub(crate) fn portable_http_client_pool_release(
    pool_handle: u64,
    handle: u64,
) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let Some(pool) = state.http_client_pools.get_mut(&pool_handle) else {
        return Ok(false);
    };
    if let Some(index) = pool.leased.iter().position(|value| *value == handle) {
        pool.leased.swap_remove(index);
        state.http_clients.remove(&handle);
        return Ok(true);
    }
    Ok(false)
}

pub(crate) fn portable_http_client_pool_close(pool_handle: u64) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let Some(pool) = state.http_client_pools.remove(&pool_handle) else {
        return Ok(false);
    };
    for handle in pool.leased {
        state.http_clients.remove(&handle);
    }
    Ok(true)
}

pub(crate) fn portable_http_server_config_u32(token: &str) -> u32 {
    match token {
        "body_limit_small" => 32,
        "body_limit_default" => 1024,
        "status_ok" => 200,
        "status_created" => 201,
        "status_no_content" => 204,
        "status_bad_request" => 400,
        "status_unauthorized" => 401,
        "status_forbidden" => 403,
        "status_not_found" => 404,
        "status_method_not_allowed" => 405,
        "status_payload_too_large" => 413,
        "status_internal_error" => 500,
        _ => 0,
    }
}

fn message_record_key(scope: &str, recipient: &str, key: &[u8]) -> String {
    let hex = portable_buf_hex_str(key);
    format!("{scope}|{recipient}|{}", String::from_utf8_lossy(&hex))
}

pub(crate) fn portable_msg_log_open() -> Result<u64, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let handle = alloc_runtime_handle(&mut state);
    state.message_logs.insert(
        handle,
        MessageLogHandle {
            next_seq: 1,
            ..MessageLogHandle::default()
        },
    );
    Ok(handle)
}

pub(crate) fn portable_msg_log_close(handle: u64) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let removed = state.message_logs.remove(&handle).is_some();
    state
        .message_replays
        .retain(|_, replay| replay.log_handle != handle);
    Ok(removed)
}

fn portable_msg_send_inner(
    log: &mut MessageLogHandle,
    conversation: &str,
    recipient: &str,
    payload: &[u8],
) -> u32 {
    let seq = log.next_seq;
    log.next_seq = log.next_seq.saturating_add(1);
    log.deliveries.push(MessageDelivery {
        seq,
        conversation: conversation.to_string(),
        recipient: recipient.to_string(),
        payload: payload.to_vec(),
        acked: false,
        retry_count: 0,
    });
    let total = log
        .delivery_totals
        .get(recipient)
        .copied()
        .unwrap_or(0)
        .saturating_add(1);
    log.delivery_totals.insert(recipient.to_string(), total);
    log.last_failure_class = 0;
    seq
}

pub(crate) fn portable_msg_send(
    handle: u64,
    conversation: &str,
    recipient: &str,
    payload: &[u8],
) -> Result<u32, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let Some(log) = state.message_logs.get_mut(&handle) else {
        return Ok(0);
    };
    Ok(portable_msg_send_inner(log, conversation, recipient, payload))
}

pub(crate) fn portable_msg_send_dedup(
    handle: u64,
    conversation: &str,
    recipient: &str,
    dedup_key: &[u8],
    payload: &[u8],
) -> Result<u32, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let Some(log) = state.message_logs.get_mut(&handle) else {
        return Ok(0);
    };
    let key = message_record_key(conversation, recipient, dedup_key);
    if let Some(existing) = log.dedup_keys.get(&key).copied() {
        log.last_failure_class = 3;
        return Ok(existing);
    }
    let seq = portable_msg_send_inner(log, conversation, recipient, payload);
    log.dedup_keys.insert(key, seq);
    Ok(seq)
}

pub(crate) fn portable_msg_subscribe(
    handle: u64,
    room: &str,
    recipient: &str,
) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let Some(log) = state.message_logs.get_mut(&handle) else {
        return Ok(false);
    };
    let members = log.subscriptions.entry(room.to_string()).or_default();
    if !members.iter().any(|member| member == recipient) {
        members.push(recipient.to_string());
    }
    log.last_failure_class = 0;
    Ok(true)
}

pub(crate) fn portable_msg_subscriber_count(handle: u64, room: &str) -> Result<u32, String> {
    let state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    Ok(state
        .message_logs
        .get(&handle)
        .and_then(|log| log.subscriptions.get(room))
        .map(|members| members.len() as u32)
        .unwrap_or(0))
}

pub(crate) fn portable_msg_fanout(
    handle: u64,
    room: &str,
    payload: &[u8],
) -> Result<u32, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let Some(log) = state.message_logs.get_mut(&handle) else {
        return Ok(0);
    };
    let Some(members) = log.subscriptions.get(room).cloned() else {
        log.last_failure_class = 1;
        return Ok(0);
    };
    let mut first_seq = 0;
    for member in members {
        let seq = portable_msg_send_inner(log, room, &member, payload);
        if first_seq == 0 {
            first_seq = seq;
        }
    }
    Ok(first_seq)
}

pub(crate) fn portable_msg_recv_next(handle: u64, recipient: &str) -> Result<Vec<u8>, String> {
    let state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let Some(log) = state.message_logs.get(&handle) else {
        return Ok(Vec::new());
    };
    Ok(log
        .deliveries
        .iter()
        .filter(|delivery| delivery.recipient == recipient && !delivery.acked)
        .min_by_key(|delivery| delivery.seq)
        .map(|delivery| delivery.payload.clone())
        .unwrap_or_default())
}

pub(crate) fn portable_msg_recv_seq(handle: u64, recipient: &str) -> Result<u32, String> {
    let state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let Some(log) = state.message_logs.get(&handle) else {
        return Ok(0);
    };
    Ok(log
        .deliveries
        .iter()
        .filter(|delivery| delivery.recipient == recipient && !delivery.acked)
        .min_by_key(|delivery| delivery.seq)
        .map(|delivery| delivery.seq)
        .unwrap_or(0))
}

pub(crate) fn portable_msg_ack(handle: u64, recipient: &str, seq: u32) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let Some(log) = state.message_logs.get_mut(&handle) else {
        return Ok(false);
    };
    if let Some(delivery) = log
        .deliveries
        .iter_mut()
        .find(|delivery| delivery.recipient == recipient && delivery.seq == seq && !delivery.acked)
    {
        delivery.acked = true;
        log.last_failure_class = 0;
        return Ok(true);
    }
    log.last_failure_class = 2;
    Ok(false)
}

pub(crate) fn portable_msg_mark_retry(
    handle: u64,
    recipient: &str,
    seq: u32,
) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let Some(log) = state.message_logs.get_mut(&handle) else {
        return Ok(false);
    };
    if let Some(delivery) = log
        .deliveries
        .iter_mut()
        .find(|delivery| delivery.recipient == recipient && delivery.seq == seq && !delivery.acked)
    {
        delivery.retry_count = delivery.retry_count.saturating_add(1);
        log.last_failure_class = 4;
        return Ok(true);
    }
    log.last_failure_class = 2;
    Ok(false)
}

pub(crate) fn portable_msg_retry_count(
    handle: u64,
    recipient: &str,
    seq: u32,
) -> Result<u32, String> {
    let state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    Ok(state
        .message_logs
        .get(&handle)
        .and_then(|log| {
            log.deliveries
                .iter()
                .find(|delivery| delivery.recipient == recipient && delivery.seq == seq)
        })
        .map(|delivery| delivery.retry_count)
        .unwrap_or(0))
}

pub(crate) fn portable_msg_pending_count(handle: u64, recipient: &str) -> Result<u32, String> {
    let state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    Ok(state
        .message_logs
        .get(&handle)
        .map(|log| {
            log.deliveries
                .iter()
                .filter(|delivery| delivery.recipient == recipient && !delivery.acked)
                .count() as u32
        })
        .unwrap_or(0))
}

pub(crate) fn portable_msg_delivery_total(handle: u64, recipient: &str) -> Result<u32, String> {
    let state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    Ok(state
        .message_logs
        .get(&handle)
        .and_then(|log| log.delivery_totals.get(recipient).copied())
        .unwrap_or(0))
}

pub(crate) fn portable_msg_failure_class(handle: u64) -> Result<u32, String> {
    let state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    Ok(state
        .message_logs
        .get(&handle)
        .map(|log| log.last_failure_class)
        .unwrap_or(2))
}

pub(crate) fn portable_msg_replay_open(
    handle: u64,
    recipient: &str,
    from_seq: u32,
) -> Result<u64, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    if !state.message_logs.contains_key(&handle) {
        return Ok(0);
    }
    let replay_handle = alloc_runtime_handle(&mut state);
    state.message_replays.insert(
        replay_handle,
        MessageReplayHandle {
            log_handle: handle,
            recipient: recipient.to_string(),
            from_seq,
            cursor: 0,
            last_seq: 0,
        },
    );
    Ok(replay_handle)
}

pub(crate) fn portable_msg_replay_next(handle: u64) -> Result<Vec<u8>, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let Some((log_handle, recipient, from_seq, cursor)) = state.message_replays.get(&handle).map(
        |replay| {
            (
                replay.log_handle,
                replay.recipient.clone(),
                replay.from_seq,
                replay.cursor,
            )
        },
    ) else {
        return Ok(Vec::new());
    };
    let Some(log) = state.message_logs.get(&log_handle) else {
        return Ok(Vec::new());
    };
    let matches: Vec<(u32, Vec<u8>)> = log
        .deliveries
        .iter()
        .filter(|delivery| delivery.recipient == recipient && delivery.seq >= from_seq)
        .map(|delivery| (delivery.seq, delivery.payload.clone()))
        .collect();
    let Some((seq, payload)) = matches.get(cursor).cloned() else {
        return Ok(Vec::new());
    };
    if let Some(replay) = state.message_replays.get_mut(&handle) {
        replay.cursor = replay.cursor.saturating_add(1);
        replay.last_seq = seq;
    }
    Ok(payload)
}

pub(crate) fn portable_msg_replay_seq(handle: u64) -> Result<u32, String> {
    let state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    Ok(state
        .message_replays
        .get(&handle)
        .map(|replay| replay.last_seq)
        .unwrap_or(0))
}

pub(crate) fn portable_msg_replay_close(handle: u64) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    Ok(state.message_replays.remove(&handle).is_some())
}

pub(crate) fn portable_service_open(name: &str) -> Result<u64, String> {
    if name.trim().is_empty() {
        return Err("portable service_open requires non-empty name".to_string());
    }
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let handle = alloc_runtime_handle(&mut state);
    state.service_handles.insert(
        handle,
        ServiceHandle {
            name: name.to_string(),
            healthy: 200,
            ready: 503,
            degraded: false,
            shutdown: false,
            traces_started: 0,
            trace_links: 0,
            metrics_count: 0,
            log_entries: 0,
            event_totals: HashMap::new(),
            metric_totals: HashMap::new(),
            metric_dim_totals: HashMap::new(),
            failure_totals: HashMap::new(),
            checkpoints_u32: HashMap::new(),
        },
    );
    Ok(handle)
}

pub(crate) fn portable_service_close(handle: u64) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let removed = state.service_handles.remove(&handle).is_some();
    state
        .service_traces
        .retain(|_, trace| trace.service_handle != handle);
    Ok(removed)
}

pub(crate) fn portable_service_shutdown(handle: u64, _grace_ms: u32) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let Some(service) = state.service_handles.get_mut(&handle) else {
        return Ok(false);
    };
    service.shutdown = true;
    service.ready = 503;
    service.healthy = 503;
    Ok(true)
}

pub(crate) fn portable_service_log(handle: u64, _message: &[u8]) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let Some(service) = state.service_handles.get_mut(&handle) else {
        return Ok(false);
    };
    service.log_entries = service.log_entries.saturating_add(1);
    Ok(true)
}

pub(crate) fn portable_service_trace_begin(handle: u64, name: &str) -> Result<u64, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    if !state.service_handles.contains_key(&handle) {
        return Err(format!("portable service unknown handle {handle}"));
    }
    let trace_handle = alloc_runtime_handle(&mut state);
    if let Some(service) = state.service_handles.get_mut(&handle) {
        service.traces_started = service.traces_started.saturating_add(1);
    }
    state.service_traces.insert(
        trace_handle,
        ServiceTraceHandle {
            service_handle: handle,
            span: name.to_string(),
            linked_parent: None,
        },
    );
    Ok(trace_handle)
}

pub(crate) fn portable_service_trace_end(trace_handle: u64) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    Ok(state.service_traces.remove(&trace_handle).is_some())
}

pub(crate) fn portable_service_metric_count(handle: u64, value: u32) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let Some(service) = state.service_handles.get_mut(&handle) else {
        return Ok(false);
    };
    service.metrics_count = service.metrics_count.saturating_add(value as u64);
    Ok(true)
}

pub(crate) fn portable_service_metric_count_dim(
    handle: u64,
    metric: &str,
    dimension: &str,
    value: u32,
) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let Some(service) = state.service_handles.get_mut(&handle) else {
        return Ok(false);
    };
    service.metrics_count = service.metrics_count.saturating_add(value as u64);
    let metric_total = service
        .metric_totals
        .get(metric)
        .copied()
        .unwrap_or(0)
        .saturating_add(value);
    service.metric_totals.insert(metric.to_string(), metric_total);
    let dim_key = format!("{metric}|{dimension}");
    let dim_total = service
        .metric_dim_totals
        .get(&dim_key)
        .copied()
        .unwrap_or(0)
        .saturating_add(value);
    service.metric_dim_totals.insert(dim_key, dim_total);
    Ok(true)
}

pub(crate) fn portable_service_metric_total(handle: u64, metric: &str) -> Result<u32, String> {
    let state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    Ok(state
        .service_handles
        .get(&handle)
        .and_then(|service| service.metric_totals.get(metric).copied())
        .unwrap_or(0))
}

pub(crate) fn portable_service_health_status(handle: u64) -> Result<u32, String> {
    let state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    Ok(state
        .service_handles
        .get(&handle)
        .map(|service| service.healthy)
        .unwrap_or(503))
}

pub(crate) fn portable_service_readiness_status(handle: u64) -> Result<u32, String> {
    let state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    Ok(state
        .service_handles
        .get(&handle)
        .map(|service| service.ready)
        .unwrap_or(503))
}

pub(crate) fn portable_service_set_health(handle: u64, status: u32) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let Some(service) = state.service_handles.get_mut(&handle) else {
        return Ok(false);
    };
    service.healthy = status;
    Ok(true)
}

pub(crate) fn portable_service_set_readiness(handle: u64, status: u32) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let Some(service) = state.service_handles.get_mut(&handle) else {
        return Ok(false);
    };
    service.ready = status;
    Ok(true)
}

pub(crate) fn portable_service_set_degraded(handle: u64, degraded: bool) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let Some(service) = state.service_handles.get_mut(&handle) else {
        return Ok(false);
    };
    service.degraded = degraded;
    Ok(true)
}

pub(crate) fn portable_service_degraded(handle: u64) -> Result<bool, String> {
    let state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    Ok(state
        .service_handles
        .get(&handle)
        .map(|service| service.degraded)
        .unwrap_or(false))
}

pub(crate) fn portable_service_event(
    handle: u64,
    class: &str,
    _message: &[u8],
) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let Some(service) = state.service_handles.get_mut(&handle) else {
        return Ok(false);
    };
    let next = service
        .event_totals
        .get(class)
        .copied()
        .unwrap_or(0)
        .saturating_add(1);
    service.event_totals.insert(class.to_string(), next);
    Ok(true)
}

pub(crate) fn portable_service_event_total(handle: u64, class: &str) -> Result<u32, String> {
    let state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    Ok(state
        .service_handles
        .get(&handle)
        .and_then(|service| service.event_totals.get(class).copied())
        .unwrap_or(0))
}

pub(crate) fn portable_service_trace_link(trace: u64, parent: u64) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let Some(parent_service) = state.service_traces.get(&parent).map(|trace| trace.service_handle) else {
        return Ok(false);
    };
    let Some((trace_service, linked_parent)) = state
        .service_traces
        .get(&trace)
        .map(|trace| (trace.service_handle, trace.linked_parent))
    else {
        return Ok(false);
    };
    if trace_service != parent_service {
        return Ok(false);
    }
    let changed = linked_parent != Some(parent);
    let Some(trace_entry) = state.service_traces.get_mut(&trace) else {
        return Ok(false);
    };
    trace_entry.linked_parent = Some(parent);
    if changed {
        if let Some(service) = state.service_handles.get_mut(&parent_service) {
            service.trace_links = service.trace_links.saturating_add(1);
        }
    }
    Ok(true)
}

pub(crate) fn portable_service_trace_link_count(handle: u64) -> Result<u32, String> {
    let state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    Ok(state
        .service_handles
        .get(&handle)
        .map(|service| service.trace_links as u32)
        .unwrap_or(0))
}

pub(crate) fn portable_service_failure_count(
    handle: u64,
    class: &str,
    value: u32,
) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let Some(service) = state.service_handles.get_mut(&handle) else {
        return Ok(false);
    };
    let next = service
        .failure_totals
        .get(class)
        .copied()
        .unwrap_or(0)
        .saturating_add(value);
    service.failure_totals.insert(class.to_string(), next);
    if value > 0 {
        service.degraded = true;
    }
    Ok(true)
}

pub(crate) fn portable_service_failure_total(handle: u64, class: &str) -> Result<u32, String> {
    let state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    Ok(state
        .service_handles
        .get(&handle)
        .and_then(|service| service.failure_totals.get(class).copied())
        .unwrap_or(0))
}

pub(crate) fn portable_service_checkpoint_save_u32(
    handle: u64,
    key: &str,
    value: u32,
) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let Some(service) = state.service_handles.get_mut(&handle) else {
        return Ok(false);
    };
    service.checkpoints_u32.insert(key.to_string(), value);
    Ok(true)
}

pub(crate) fn portable_service_checkpoint_load_u32(
    handle: u64,
    key: &str,
) -> Result<u32, String> {
    let state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    Ok(state
        .service_handles
        .get(&handle)
        .and_then(|service| service.checkpoints_u32.get(key).copied())
        .unwrap_or(0))
}

pub(crate) fn portable_service_checkpoint_exists(handle: u64, key: &str) -> Result<bool, String> {
    let state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    Ok(state
        .service_handles
        .get(&handle)
        .map(|service| service.checkpoints_u32.contains_key(key))
        .unwrap_or(false))
}

pub(crate) fn portable_service_migrate_db(handle: u64, db_handle: u64) -> Result<bool, String> {
    let db_exists = {
        let state = runtime_state()
            .lock()
            .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
        state.db_handles.contains_key(&db_handle)
    };
    if !db_exists {
        return Ok(false);
    }
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let Some(service) = state.service_handles.get_mut(&handle) else {
        return Ok(false);
    };
    service.ready = 200;
    service.healthy = 200;
    Ok(true)
}

pub(crate) fn portable_service_route(request: &[u8], method: &str, path: &str) -> bool {
    portable_http_method_eq(request, method) && portable_http_path_eq(request, path)
}

pub(crate) fn portable_service_require_header(request: &[u8], name: &str, value: &str) -> bool {
    portable_http_header_eq(request, name, value)
}

pub(crate) fn portable_service_error_status(kind: &str) -> u32 {
    match kind {
        "bad_request" => 400,
        "unauthorized" => 401,
        "forbidden" => 403,
        "not_found" => 404,
        "conflict" => 409,
        "payload_too_large" => 413,
        "too_many_requests" => 429,
        "service_unavailable" => 503,
        _ => 500,
    }
}

fn http_reason_phrase(status: u32) -> &'static str {
    match status {
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        500 => "Internal Server Error",
        _ => "OK",
    }
}

pub(crate) fn portable_http_write_response(
    handle: u64,
    status: u32,
    body: &[u8],
) -> Result<bool, String> {
    let mut response = format!(
        "HTTP/1.1 {} {}\r\nContent-Length: {}\r\nConnection: close\r\nContent-Type: application/octet-stream\r\n\r\n",
        status,
        http_reason_phrase(status),
        body.len()
    )
    .into_bytes();
    response.extend_from_slice(body);
    portable_net_write_all_handle(handle, &response)
}

pub(crate) fn portable_http_write_text_response(
    handle: u64,
    status: u32,
    body: &[u8],
) -> Result<bool, String> {
    portable_http_write_response_header(handle, status, "Content-Type", "text/plain", body)
}

pub(crate) fn portable_http_write_text_response_cookie(
    handle: u64,
    status: u32,
    cookie_name: &str,
    cookie_value: &str,
    body: &[u8],
) -> Result<bool, String> {
    portable_http_write_response_cookie(
        handle,
        status,
        "text/plain",
        cookie_name,
        cookie_value,
        body,
    )
}

pub(crate) fn portable_http_write_text_response_headers2(
    handle: u64,
    status: u32,
    header1_name: &str,
    header1_value: &str,
    header2_name: &str,
    header2_value: &str,
    body: &[u8],
) -> Result<bool, String> {
    portable_http_write_response_headers2(
        handle,
        status,
        "text/plain",
        header1_name,
        header1_value,
        header2_name,
        header2_value,
        body,
    )
}

pub(crate) fn portable_http_write_json_response(
    handle: u64,
    status: u32,
    body: &[u8],
) -> Result<bool, String> {
    portable_http_write_response_header(handle, status, "Content-Type", "application/json", body)
}

pub(crate) fn portable_http_write_json_response_cookie(
    handle: u64,
    status: u32,
    cookie_name: &str,
    cookie_value: &str,
    body: &[u8],
) -> Result<bool, String> {
    portable_http_write_response_cookie(
        handle,
        status,
        "application/json",
        cookie_name,
        cookie_value,
        body,
    )
}

pub(crate) fn portable_http_write_json_response_headers2(
    handle: u64,
    status: u32,
    header1_name: &str,
    header1_value: &str,
    header2_name: &str,
    header2_value: &str,
    body: &[u8],
) -> Result<bool, String> {
    portable_http_write_response_headers2(
        handle,
        status,
        "application/json",
        header1_name,
        header1_value,
        header2_name,
        header2_value,
        body,
    )
}

pub(crate) fn portable_http_write_response_header(
    handle: u64,
    status: u32,
    header_name: &str,
    header_value: &str,
    body: &[u8],
) -> Result<bool, String> {
    let mut response = format!(
        "HTTP/1.1 {} {}\r\nContent-Length: {}\r\nConnection: close\r\n{}: {}\r\n\r\n",
        status,
        http_reason_phrase(status),
        body.len(),
        header_name,
        header_value
    )
    .into_bytes();
    response.extend_from_slice(body);
    let maybe_tls = {
        let state = runtime_state()
            .lock()
            .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
        match state.net_handles.get(&handle) {
            Some(NetHandle::TlsSession(shared)) => Some(shared.clone()),
            Some(NetHandle::Stream(_)) => None,
            Some(_) => {
                return Err(format!(
                    "portable http write handle {handle} is not a session"
                ))
            }
            None => return Err(format!("portable http write unknown handle {handle}")),
        }
    };
    if let Some(shared) = maybe_tls {
        let mut process = shared
            .lock()
            .map_err(|_| "portable tls session mutex poisoned".to_string())?;
        let Some(stdin) = process.stdin.as_mut() else {
            return Ok(false);
        };
        stdin
            .write_all(&response)
            .map_err(|error| format!("portable tls session write failed: {error}"))?;
        return Ok(true);
    }
    portable_net_write_all_handle(handle, &response)
}

pub(crate) fn portable_http_write_response_cookie(
    handle: u64,
    status: u32,
    content_type: &str,
    cookie_name: &str,
    cookie_value: &str,
    body: &[u8],
) -> Result<bool, String> {
    let mut response = format!(
        "HTTP/1.1 {} {}\r\nContent-Length: {}\r\nConnection: close\r\nContent-Type: {}\r\nSet-Cookie: {}={}\r\n\r\n",
        status,
        http_reason_phrase(status),
        body.len(),
        content_type,
        cookie_name,
        cookie_value
    )
    .into_bytes();
    response.extend_from_slice(body);
    let maybe_tls = {
        let state = runtime_state()
            .lock()
            .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
        match state.net_handles.get(&handle) {
            Some(NetHandle::TlsSession(shared)) => Some(shared.clone()),
            Some(NetHandle::Stream(_)) => None,
            Some(_) => {
                return Err(format!(
                    "portable http write handle {handle} is not a session"
                ))
            }
            None => return Err(format!("portable http write unknown handle {handle}")),
        }
    };
    if let Some(shared) = maybe_tls {
        let mut process = shared
            .lock()
            .map_err(|_| "portable tls session mutex poisoned".to_string())?;
        let Some(stdin) = process.stdin.as_mut() else {
            return Ok(false);
        };
        stdin
            .write_all(&response)
            .map_err(|error| format!("portable tls session write failed: {error}"))?;
        return Ok(true);
    }
    portable_net_write_all_handle(handle, &response)
}

fn portable_http_header_name_eq(left: &str, right: &str) -> bool {
    left.eq_ignore_ascii_case(right)
}

pub(crate) fn portable_http_write_response_headers2(
    handle: u64,
    status: u32,
    default_content_type: &str,
    header1_name: &str,
    header1_value: &str,
    header2_name: &str,
    header2_value: &str,
    body: &[u8],
) -> Result<bool, String> {
    let mut content_type = default_content_type;
    let mut header1_emit = true;
    let mut header2_emit = true;
    if portable_http_header_name_eq(header1_name, "Content-Type") {
        content_type = header1_value;
        header1_emit = false;
    }
    if portable_http_header_name_eq(header2_name, "Content-Type") {
        content_type = header2_value;
        header2_emit = false;
    }
    if header1_emit && header2_emit && portable_http_header_name_eq(header1_name, header2_name) {
        header1_emit = false;
    }
    let mut response = format!(
        "HTTP/1.1 {} {}\r\nContent-Length: {}\r\nConnection: close\r\nContent-Type: {}\r\n",
        status,
        http_reason_phrase(status),
        body.len(),
        content_type
    )
    .into_bytes();
    if header1_emit {
        response.extend_from_slice(header1_name.as_bytes());
        response.extend_from_slice(b": ");
        response.extend_from_slice(header1_value.as_bytes());
        response.extend_from_slice(b"\r\n");
    }
    if header2_emit {
        response.extend_from_slice(header2_name.as_bytes());
        response.extend_from_slice(b": ");
        response.extend_from_slice(header2_value.as_bytes());
        response.extend_from_slice(b"\r\n");
    }
    response.extend_from_slice(b"\r\n");
    response.extend_from_slice(body);
    let maybe_tls = {
        let state = runtime_state()
            .lock()
            .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
        match state.net_handles.get(&handle) {
            Some(NetHandle::TlsSession(shared)) => Some(shared.clone()),
            Some(NetHandle::Stream(_)) => None,
            Some(_) => {
                return Err(format!(
                    "portable http write handle {handle} is not a session"
                ))
            }
            None => return Err(format!("portable http write unknown handle {handle}")),
        }
    };
    if let Some(shared) = maybe_tls {
        let mut process = shared
            .lock()
            .map_err(|_| "portable tls session mutex poisoned".to_string())?;
        let Some(stdin) = process.stdin.as_mut() else {
            return Ok(false);
        };
        stdin
            .write_all(&response)
            .map_err(|error| format!("portable tls session write failed: {error}"))?;
        return Ok(true);
    }
    portable_net_write_all_handle(handle, &response)
}

pub(crate) fn portable_http_session_write_text_cookie(
    handle: u64,
    status: u32,
    cookie_name: &str,
    cookie_value: &str,
    body: &[u8],
) -> Result<bool, String> {
    portable_http_write_text_response_cookie(handle, status, cookie_name, cookie_value, body)
}

pub(crate) fn portable_http_session_write_text_headers2(
    handle: u64,
    status: u32,
    header1_name: &str,
    header1_value: &str,
    header2_name: &str,
    header2_value: &str,
    body: &[u8],
) -> Result<bool, String> {
    portable_http_write_text_response_headers2(
        handle,
        status,
        header1_name,
        header1_value,
        header2_name,
        header2_value,
        body,
    )
}

pub(crate) fn portable_http_session_write_json_cookie(
    handle: u64,
    status: u32,
    cookie_name: &str,
    cookie_value: &str,
    body: &[u8],
) -> Result<bool, String> {
    portable_http_write_json_response_cookie(handle, status, cookie_name, cookie_value, body)
}

pub(crate) fn portable_http_session_write_json_headers2(
    handle: u64,
    status: u32,
    header1_name: &str,
    header1_value: &str,
    header2_name: &str,
    header2_value: &str,
    body: &[u8],
) -> Result<bool, String> {
    portable_http_write_json_response_headers2(
        handle,
        status,
        header1_name,
        header1_value,
        header2_name,
        header2_value,
        body,
    )
}

fn json_find_value_start<'a>(value: &'a [u8], key: &str) -> Option<&'a [u8]> {
    let text = std::str::from_utf8(value).ok()?;
    let pattern = format!("\"{key}\"");
    if let Some(index) = text.find(&pattern) {
        let key_end = index + pattern.len();
        let remainder = &text[key_end..];
        let colon_index = remainder.find(':')?;
        let after_colon = &remainder[colon_index + 1..];
        let trimmed = after_colon.trim_start();
        let consumed = after_colon.len() - trimmed.len();
        let absolute = key_end + colon_index + 1 + consumed;
        Some(&value[absolute..])
    } else {
        None
    }
}

pub(crate) fn portable_json_get_u32(value: &[u8], key: &str) -> u32 {
    let Some(slice) = json_find_value_start(value, key) else {
        return 0;
    };
    let end = slice
        .iter()
        .position(|byte| !byte.is_ascii_digit())
        .unwrap_or(slice.len());
    std::str::from_utf8(&slice[..end])
        .ok()
        .and_then(|text| text.parse::<u32>().ok())
        .unwrap_or(0)
}

pub(crate) fn portable_json_get_bool(value: &[u8], key: &str) -> bool {
    let Some(slice) = json_find_value_start(value, key) else {
        return false;
    };
    slice.starts_with(b"true")
}

pub(crate) fn portable_json_has_key(value: &[u8], key: &str) -> bool {
    json_find_value_start(value, key).is_some()
}

fn decode_json_string_bytes(raw: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(raw.len());
    let mut index = 0usize;
    while index < raw.len() {
        if raw[index] == b'\\' && index + 1 < raw.len() {
            index += 1;
            match raw[index] {
                b'"' => out.push(b'"'),
                b'\\' => out.push(b'\\'),
                b'n' => out.push(b'\n'),
                b'r' => out.push(b'\r'),
                b't' => out.push(b'\t'),
                other => out.push(other),
            }
        } else {
            out.push(raw[index]);
        }
        index += 1;
    }
    out
}

pub(crate) fn portable_json_get_buf(value: &[u8], key: &str) -> Vec<u8> {
    let Some(slice) = json_find_value_start(value, key) else {
        return Vec::new();
    };
    if slice.first() != Some(&b'"') {
        return Vec::new();
    }
    let mut index = 1usize;
    let mut escaped = false;
    while index < slice.len() {
        match slice[index] {
            b'\\' if !escaped => escaped = true,
            b'"' if !escaped => return decode_json_string_bytes(&slice[1..index]),
            _ => escaped = false,
        }
        index += 1;
    }
    Vec::new()
}

fn portable_json_value(value: &[u8]) -> Option<Value> {
    serde_json::from_slice::<Value>(value).ok()
}

pub(crate) fn portable_json_get_str(value: &[u8], key: &str) -> Vec<u8> {
    portable_json_value(value)
        .and_then(|json| json.get(key).cloned())
        .and_then(|value| value.as_str().map(|text| text.as_bytes().to_vec()))
        .unwrap_or_default()
}

pub(crate) fn portable_json_get_u32_or(value: &[u8], key: &str, default_value: u32) -> u32 {
    if portable_json_has_key(value, key) {
        portable_json_get_u32(value, key)
    } else {
        default_value
    }
}

pub(crate) fn portable_json_get_bool_or(value: &[u8], key: &str, default_value: bool) -> bool {
    if portable_json_has_key(value, key) {
        portable_json_get_bool(value, key)
    } else {
        default_value
    }
}

pub(crate) fn portable_json_get_buf_or(value: &[u8], key: &str, default_value: &[u8]) -> Vec<u8> {
    if portable_json_has_key(value, key) {
        portable_json_get_buf(value, key)
    } else {
        default_value.to_vec()
    }
}

pub(crate) fn portable_json_get_str_or(value: &[u8], key: &str, default_value: &[u8]) -> Vec<u8> {
    if portable_json_has_key(value, key) {
        portable_json_get_str(value, key)
    } else {
        default_value.to_vec()
    }
}

pub(crate) fn portable_json_array_len(value: &[u8]) -> u32 {
    portable_json_value(value)
        .and_then(|json| json.as_array().map(|items| items.len() as u32))
        .unwrap_or(0)
}

pub(crate) fn portable_json_index_u32(value: &[u8], index: u32) -> u32 {
    portable_json_value(value)
        .and_then(|json| json.get(index as usize).cloned())
        .and_then(|value| value.as_u64().map(|number| number as u32))
        .unwrap_or(0)
}

pub(crate) fn portable_json_index_bool(value: &[u8], index: u32) -> bool {
    portable_json_value(value)
        .and_then(|json| json.get(index as usize).cloned())
        .and_then(|value| value.as_bool())
        .unwrap_or(false)
}

pub(crate) fn portable_json_index_str(value: &[u8], index: u32) -> Vec<u8> {
    portable_json_value(value)
        .and_then(|json| json.get(index as usize).cloned())
        .and_then(|value| value.as_str().map(|text| text.as_bytes().to_vec()))
        .unwrap_or_default()
}

pub(crate) fn portable_json_encode_object(entries: &[(String, RuntimeValue)]) -> Vec<u8> {
    let mut object = Map::new();
    for (key, value) in entries {
        let json_value = match value {
            RuntimeValue::U32(value) => Value::Number((*value).into()),
            RuntimeValue::Bool(value) => Value::Bool(*value),
            RuntimeValue::BufU8(value) => {
                Value::String(String::from_utf8_lossy(value.as_slice()).into_owned())
            }
            other => Value::String(format!("{other:?}")),
        };
        object.insert(key.clone(), json_value);
    }
    serde_json::to_vec(&Value::Object(object)).unwrap_or_default()
}

pub(crate) fn portable_json_encode_array(values: &[RuntimeValue]) -> Vec<u8> {
    let array = values
        .iter()
        .map(|value| match value {
            RuntimeValue::U8(value) => Value::Number((*value as u64).into()),
            RuntimeValue::I32(value) => Value::Number((*value).into()),
            RuntimeValue::I64(value) => Value::Number((*value).into()),
            RuntimeValue::U64(value) => Value::Number((*value).into()),
            RuntimeValue::U32(value) => Value::Number((*value).into()),
            RuntimeValue::Bool(value) => Value::Bool(*value),
            RuntimeValue::BufU8(value) => {
                Value::String(String::from_utf8_lossy(value.as_slice()).into_owned())
            }
            RuntimeValue::SpanI32(values) => Value::Array(
                values
                    .iter()
                    .map(|value| Value::Number((*value).into()))
                    .collect(),
            ),
        })
        .collect::<Vec<_>>();
    serde_json::to_vec(&Value::Array(array)).unwrap_or_default()
}

pub(crate) fn portable_env_get_str(key: &str) -> Vec<u8> {
    std::env::var(key).unwrap_or_default().into_bytes()
}

pub(crate) fn portable_env_has(key: &str) -> bool {
    std::env::var_os(key).is_some()
}

pub(crate) fn portable_env_get_u32(key: &str) -> u32 {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(0)
}

pub(crate) fn portable_env_get_bool(key: &str) -> bool {
    std::env::var(key)
        .ok()
        .and_then(|value| match value.trim() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        })
        .unwrap_or(false)
}

pub(crate) fn portable_buf_hex_str(value: &[u8]) -> Vec<u8> {
    let mut out = String::with_capacity(value.len() * 2);
    for byte in value {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out.into_bytes()
}

pub(crate) fn portable_buf_before_lit(value: &[u8], literal: &str) -> Vec<u8> {
    let needle = decode_escaped_literal_bytes(literal);
    if needle.is_empty() {
        return value.to_vec();
    }
    value
        .windows(needle.len())
        .position(|window| window == needle.as_slice())
        .map(|index| value[..index].to_vec())
        .unwrap_or_default()
}

pub(crate) fn portable_buf_after_lit(value: &[u8], literal: &str) -> Vec<u8> {
    let needle = decode_escaped_literal_bytes(literal);
    if needle.is_empty() {
        return value.to_vec();
    }
    value
        .windows(needle.len())
        .position(|window| window == needle.as_slice())
        .map(|index| value[index + needle.len()..].to_vec())
        .unwrap_or_default()
}

pub(crate) fn portable_buf_trim_ascii(value: &[u8]) -> Vec<u8> {
    let start = value
        .iter()
        .position(|byte| !byte.is_ascii_whitespace())
        .unwrap_or(value.len());
    let end = value
        .iter()
        .rposition(|byte| !byte.is_ascii_whitespace())
        .map(|index| index + 1)
        .unwrap_or(start);
    value[start..end].to_vec()
}

pub(crate) fn portable_date_parse_ymd(value: &[u8]) -> u32 {
    let text = String::from_utf8_lossy(value);
    let parts = text.split('-').collect::<Vec<_>>();
    if parts.len() != 3 {
        return 0;
    }
    let year = parts[0].parse::<u32>().ok().unwrap_or(0);
    let month = parts[1].parse::<u32>().ok().unwrap_or(0);
    let day = parts[2].parse::<u32>().ok().unwrap_or(0);
    year.saturating_mul(10_000)
        .saturating_add(month.saturating_mul(100))
        .saturating_add(day)
}

pub(crate) fn portable_time_parse_hms(value: &[u8]) -> u32 {
    let text = String::from_utf8_lossy(value);
    let parts = text.split(':').collect::<Vec<_>>();
    if parts.len() != 3 {
        return 0;
    }
    let hour = parts[0].parse::<u32>().ok().unwrap_or(0);
    let minute = parts[1].parse::<u32>().ok().unwrap_or(0);
    let second = parts[2].parse::<u32>().ok().unwrap_or(0);
    hour.saturating_mul(3600)
        .saturating_add(minute.saturating_mul(60))
        .saturating_add(second)
}

pub(crate) fn portable_date_format_ymd(value: u32) -> Vec<u8> {
    let year = value / 10_000;
    let month = (value / 100) % 100;
    let day = value % 100;
    format!("{year:04}-{month:02}-{day:02}").into_bytes()
}

pub(crate) fn portable_time_format_hms(value: u32) -> Vec<u8> {
    let hour = value / 3600;
    let minute = (value / 60) % 60;
    let second = value % 60;
    format!("{hour:02}:{minute:02}:{second:02}").into_bytes()
}

pub(crate) fn portable_spawn_open(
    command: &str,
    argv: &[String],
    env_vars: &[(String, String)],
) -> Result<u64, String> {
    let mut invocation = Command::new(command);
    invocation.args(argv);
    invocation.stdin(Stdio::piped());
    invocation.stdout(Stdio::piped());
    invocation.stderr(Stdio::piped());
    for (name, value) in env_vars {
        invocation.env(name, value);
    }
    let mut child = invocation
        .spawn()
        .map_err(|error| format!("portable spawn_open failed: {error}"))?;
    let stdin = child.stdin.take();
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let handle = alloc_runtime_handle(&mut state);
    state.spawn_handles.insert(
        handle,
        SpawnHandle {
            child: Some(child),
            stdin,
            waited_status: None,
            stdout: Vec::new(),
            stderr: Vec::new(),
        },
    );
    Ok(handle)
}

fn ensure_spawn_waited(record: &mut SpawnHandle) -> Result<i32, String> {
    if let Some(status) = record.waited_status {
        return Ok(status);
    }
    record.stdin.take();
    if poll_spawn_waited(record)? {
        return Ok(record.waited_status.unwrap_or(-1));
    }
    let child = record
        .child
        .take()
        .ok_or_else(|| "portable spawn handle has no child process".to_string())?;
    let output = child
        .wait_with_output()
        .map_err(|error| format!("portable spawn_wait failed: {error}"))?;
    let status = output.status.code().unwrap_or(-1);
    record.waited_status = Some(status);
    record.stdout = output.stdout;
    record.stderr = output.stderr;
    Ok(status)
}

fn poll_spawn_waited(record: &mut SpawnHandle) -> Result<bool, String> {
    if record.waited_status.is_some() {
        return Ok(true);
    }
    let Some(child) = record.child.as_mut() else {
        return Ok(false);
    };
    let Some(_status) = child
        .try_wait()
        .map_err(|error| format!("portable spawn poll failed: {error}"))?
    else {
        return Ok(false);
    };
    let child = record
        .child
        .take()
        .ok_or_else(|| "portable spawn handle lost child process".to_string())?;
    let output = child
        .wait_with_output()
        .map_err(|error| format!("portable spawn_wait failed: {error}"))?;
    let status = output.status.code().unwrap_or(-1);
    record.waited_status = Some(status);
    record.stdout = output.stdout;
    record.stderr = output.stderr;
    Ok(true)
}

pub(crate) fn portable_spawn_wait(handle: u64) -> Result<i32, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let record = state
        .spawn_handles
        .get_mut(&handle)
        .ok_or_else(|| format!("portable spawn_wait unknown handle {handle}"))?;
    ensure_spawn_waited(record)
}

pub(crate) fn portable_spawn_stdout_all(handle: u64) -> Result<Vec<u8>, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let record = state
        .spawn_handles
        .get_mut(&handle)
        .ok_or_else(|| format!("portable spawn_stdout_all unknown handle {handle}"))?;
    let _ = ensure_spawn_waited(record)?;
    Ok(record.stdout.clone())
}

pub(crate) fn portable_spawn_stderr_all(handle: u64) -> Result<Vec<u8>, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let record = state
        .spawn_handles
        .get_mut(&handle)
        .ok_or_else(|| format!("portable spawn_stderr_all unknown handle {handle}"))?;
    let _ = ensure_spawn_waited(record)?;
    Ok(record.stderr.clone())
}

pub(crate) fn portable_spawn_stdin_write_all(handle: u64, value: &[u8]) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let record = state
        .spawn_handles
        .get_mut(&handle)
        .ok_or_else(|| format!("portable spawn_stdin_write_all unknown handle {handle}"))?;
    if record.waited_status.is_some() {
        return Ok(false);
    }
    let Some(stdin) = record.stdin.as_mut() else {
        return Ok(false);
    };
    stdin
        .write_all(value)
        .map_err(|error| format!("portable spawn_stdin_write_all failed: {error}"))?;
    Ok(true)
}

pub(crate) fn portable_spawn_stdin_close(handle: u64) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let record = state
        .spawn_handles
        .get_mut(&handle)
        .ok_or_else(|| format!("portable spawn_stdin_close unknown handle {handle}"))?;
    Ok(record.stdin.take().is_some())
}

pub(crate) fn portable_spawn_done(handle: u64) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let record = state
        .spawn_handles
        .get_mut(&handle)
        .ok_or_else(|| format!("portable spawn_done unknown handle {handle}"))?;
    poll_spawn_waited(record)
}

pub(crate) fn portable_spawn_exit_ok(handle: u64) -> Result<bool, String> {
    Ok(portable_spawn_wait(handle)? == 0)
}

pub(crate) fn portable_spawn_kill(handle: u64) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let record = state
        .spawn_handles
        .get_mut(&handle)
        .ok_or_else(|| format!("portable spawn_kill unknown handle {handle}"))?;
    record.stdin.take();
    let Some(child) = record.child.as_mut() else {
        return Ok(false);
    };
    child
        .kill()
        .map_err(|error| format!("portable spawn_kill failed: {error}"))?;
    let _ = poll_spawn_waited(record)?;
    Ok(true)
}

pub(crate) fn portable_spawn_close(handle: u64) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    if let Some(mut record) = state.spawn_handles.remove(&handle) {
        record.stdin.take();
        if record.waited_status.is_none() {
            let _ = ensure_spawn_waited(&mut record);
        }
        Ok(true)
    } else {
        Ok(false)
    }
}

pub(crate) fn portable_task_open(
    command: &str,
    argv: &[String],
    env_vars: &[(String, String)],
) -> Result<u64, String> {
    portable_spawn_open(command, argv, env_vars)
}

pub(crate) fn portable_task_done(handle: u64) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let record = state
        .spawn_handles
        .get_mut(&handle)
        .ok_or_else(|| format!("portable task_done unknown handle {handle}"))?;
    poll_spawn_waited(record)
}

pub(crate) fn portable_task_join(handle: u64) -> Result<i32, String> {
    portable_spawn_wait(handle)
}

pub(crate) fn portable_task_stdout_all(handle: u64) -> Result<Vec<u8>, String> {
    portable_spawn_stdout_all(handle)
}

pub(crate) fn portable_task_stderr_all(handle: u64) -> Result<Vec<u8>, String> {
    portable_spawn_stderr_all(handle)
}

pub(crate) fn portable_task_close(handle: u64) -> Result<bool, String> {
    portable_spawn_close(handle)
}

pub(crate) fn portable_ffi_open_lib(path: &str) -> Result<u64, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    let handle = alloc_runtime_handle(&mut state);
    state.ffi_libs.insert(handle, path.to_string());
    Ok(handle)
}

pub(crate) fn portable_ffi_close_lib(handle: u64) -> Result<bool, String> {
    let mut state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    Ok(state.ffi_libs.remove(&handle).is_some())
}

pub(crate) fn portable_ffi_call(
    symbol: &str,
    args: &[RuntimeValue],
    ret_c_type: &str,
) -> Result<RuntimeValue, String> {
    match (symbol, args, ret_c_type) {
        ("abs", [RuntimeValue::I32(value)], "int32_t") => {
            Ok(RuntimeValue::I32(value.wrapping_abs()))
        }
        ("labs" | "llabs", [RuntimeValue::I64(value)], "int64_t") => {
            Ok(RuntimeValue::I64(value.wrapping_abs()))
        }
        _ => Err(format!(
            "unsupported portable ffi call {}({:?}) -> {}",
            symbol, args, ret_c_type
        )),
    }
}

pub(crate) fn portable_ffi_call_lib(
    handle: u64,
    symbol: &str,
    args: &[RuntimeValue],
    ret_c_type: &str,
) -> Result<RuntimeValue, String> {
    let state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    if !state.ffi_libs.contains_key(&handle) {
        return Err(format!(
            "portable ffi_call_lib unknown library handle {handle}"
        ));
    }
    drop(state);
    portable_ffi_call(symbol, args, ret_c_type)
}

pub(crate) fn portable_ffi_call_cstr(
    symbol: &str,
    value: &[u8],
    ret_c_type: &str,
) -> Result<RuntimeValue, String> {
    let c_end = value
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(value.len());
    let slice = &value[..c_end];
    match (symbol, ret_c_type) {
        ("strlen", "uint64_t") => Ok(RuntimeValue::U64(slice.len() as u64)),
        ("strlen", "uint32_t") => Ok(RuntimeValue::U32(slice.len() as u32)),
        ("strlen", "int64_t") => Ok(RuntimeValue::I64(slice.len() as i64)),
        ("strlen", "int32_t") => Ok(RuntimeValue::I32(slice.len() as i32)),
        ("atoi", "int32_t") => {
            let text = std::str::from_utf8(slice)
                .map_err(|error| format!("portable ffi cstr atoi utf8 failed: {error}"))?;
            let parsed = text
                .trim()
                .parse::<i32>()
                .map_err(|error| format!("portable ffi cstr atoi parse failed: {error}"))?;
            Ok(RuntimeValue::I32(parsed))
        }
        _ => Err(format!(
            "unsupported portable ffi cstr call {}(<buf>) -> {}",
            symbol, ret_c_type
        )),
    }
}

pub(crate) fn portable_ffi_call_lib_cstr(
    handle: u64,
    symbol: &str,
    value: &[u8],
    ret_c_type: &str,
) -> Result<RuntimeValue, String> {
    let state = runtime_state()
        .lock()
        .map_err(|_| "portable runtime state mutex poisoned".to_string())?;
    if !state.ffi_libs.contains_key(&handle) {
        return Err(format!(
            "portable ffi_call_lib_cstr unknown library handle {handle}"
        ));
    }
    drop(state);
    portable_ffi_call_cstr(symbol, value, ret_c_type)
}
