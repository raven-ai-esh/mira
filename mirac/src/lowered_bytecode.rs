use std::collections::HashMap;

use crate::codegen_c::{
    LoweredExecBinaryOp, LoweredExecExpr, LoweredExecImmediate, LoweredExecOperand, LoweredProgram,
    LoweredStatement, LoweredTerminator,
};
use crate::lowered_exec::{
    decode_escaped_literal_bytes, mira_clock_now_ns, mira_rand_next_u32, portable_buf_after_lit,
    portable_buf_before_lit, portable_buf_contains_lit, portable_buf_eq_lit,
    portable_buf_hex_str, portable_buf_parse_bool, portable_buf_parse_u32,
    portable_buf_trim_ascii,
    portable_backpressure_acquire, portable_backpressure_close, portable_backpressure_open,
    portable_backpressure_release, portable_backpressure_saturated, portable_cancel_scope_bind_task,
    portable_cancel_scope_cancel, portable_cancel_scope_cancelled, portable_cancel_scope_child,
    portable_cancel_scope_close, portable_cancel_scope_open, portable_chan_close,
    portable_chan_len, portable_chan_open_buf, portable_chan_open_u32, portable_chan_recv_buf,
    portable_chan_recv_u32, portable_chan_send_buf, portable_chan_send_u32,
    portable_circuit_allow, portable_circuit_close, portable_circuit_open,
    portable_circuit_record_failure, portable_circuit_record_success, portable_circuit_state,
    portable_date_format_ymd, portable_date_parse_ymd, portable_db_begin, portable_db_close,
    portable_deadline_close, portable_deadline_expired, portable_deadline_open_ms,
    portable_deadline_remaining_ms,
    portable_db_commit, portable_db_exec, portable_db_exec_prepared, portable_db_last_error_code,
    portable_db_last_error_retryable, portable_db_open, portable_db_pool_acquire,
    portable_db_pool_close, portable_db_pool_leased, portable_db_pool_open,
    portable_db_pool_release, portable_db_pool_set_max_idle, portable_db_prepare,
    portable_db_query_buf, portable_db_query_prepared_buf, portable_db_query_prepared_row,
    portable_db_query_prepared_u32, portable_db_query_row, portable_db_query_u32,
    portable_db_rollback, portable_cache_close, portable_cache_del, portable_cache_get_buf,
    portable_cache_open, portable_cache_set_buf, portable_env_get_bool,
    portable_env_get_str, portable_env_get_u32, portable_env_has,
    portable_ffi_call, portable_ffi_call_cstr, portable_ffi_call_lib, portable_ffi_call_lib_cstr,
    portable_ffi_close_lib, portable_ffi_open_lib, portable_fs_read_all_u8, portable_fs_read_u32,
    portable_fs_write_all_u8, portable_fs_write_u32, portable_http_body,
    portable_http_body_limit, portable_http_body_stream_close, portable_http_body_stream_next,
    portable_http_body_stream_open, portable_http_client_close, portable_http_client_open,
    portable_http_client_pool_acquire, portable_http_client_pool_close,
    portable_http_client_pool_open, portable_http_client_pool_release,
    portable_http_client_request, portable_http_client_request_retry, portable_http_cookie,
    portable_http_cookie_eq, portable_http_header, portable_http_header_count,
    portable_http_header_eq, portable_http_header_name, portable_http_header_value,
    portable_http_method_eq, portable_http_multipart_part_body,
    portable_http_multipart_part_count, portable_http_multipart_part_filename,
    portable_http_multipart_part_name, portable_http_path_eq, portable_http_query_param,
    portable_http_response_stream_close, portable_http_response_stream_open,
    portable_http_response_stream_write,
    portable_http_request_method, portable_http_request_path, portable_http_route_param,
    portable_http_server_config_u32, portable_http_session_accept,
    portable_http_session_close, portable_http_session_request, portable_http_session_write_json,
    portable_http_session_write_json_headers2,
    portable_http_session_write_json_cookie, portable_http_session_write_text,
    portable_http_session_write_text_headers2,
    portable_http_session_write_text_cookie, portable_http_status_u32,
    portable_http_write_json_response_headers2,
    portable_http_write_json_response, portable_http_write_json_response_cookie,
    portable_http_write_response, portable_http_write_response_header,
    portable_http_write_text_response_headers2,
    portable_http_write_text_response, portable_http_write_text_response_cookie,
    portable_json_array_len, portable_json_encode_array, portable_json_encode_object,
    portable_json_get_bool, portable_json_get_bool_or, portable_json_get_buf,
    portable_json_get_buf_or, portable_json_get_str, portable_json_get_str_or,
    portable_json_get_u32, portable_json_get_u32_or, portable_json_has_key,
    portable_json_index_bool, portable_json_index_str, portable_json_index_u32,
    portable_listener_set_shutdown_grace_ms, portable_listener_set_timeout_ms,
    portable_net_accept_handle, portable_net_close_handle, portable_net_connect_ok,
    portable_net_exchange_all, portable_net_listen_handle, portable_net_read_all_handle,
    portable_net_serve_exchange_all, portable_net_session_open, portable_net_write_all,
    portable_net_write_all_handle,
    portable_retry_close, portable_retry_exhausted, portable_retry_next_delay_ms,
    portable_retry_open, portable_retry_record_failure, portable_retry_record_success,
    portable_rt_cancel, portable_rt_cancelled, portable_rt_close, portable_rt_done,
    portable_rt_inflight, portable_rt_join_buf, portable_rt_join_u32, portable_rt_open,
    portable_rt_shutdown, portable_rt_spawn_buf, portable_rt_spawn_u32,
    portable_rt_task_close, portable_rt_try_spawn_buf, portable_rt_try_spawn_u32,
    portable_service_checkpoint_exists, portable_service_checkpoint_load_u32,
    portable_service_checkpoint_save_u32, portable_service_close, portable_service_degraded,
    portable_service_error_status, portable_service_event, portable_service_event_total,
    portable_service_failure_count, portable_service_failure_total,
    portable_service_health_status, portable_service_log, portable_service_metric_count,
    portable_service_metric_count_dim, portable_service_metric_total,
    portable_service_migrate_db, portable_service_open, portable_service_readiness_status,
    portable_service_require_header, portable_service_route, portable_service_set_degraded,
    portable_service_set_health, portable_service_set_readiness, portable_service_shutdown,
    portable_service_trace_begin, portable_service_trace_end, portable_service_trace_link,
    portable_service_trace_link_count, portable_session_alive, portable_session_backpressure,
    portable_shard_route_u32, portable_lease_acquire, portable_lease_close,
    portable_lease_open, portable_lease_owner, portable_lease_release,
    portable_lease_transfer, portable_placement_assign, portable_placement_close,
    portable_placement_lookup, portable_placement_open, portable_coord_close,
    portable_coord_load_u32, portable_coord_open, portable_coord_store_u32,
    portable_session_backpressure_wait, portable_session_flush, portable_session_heartbeat,
    portable_session_read_chunk, portable_session_reconnect, portable_session_resume_id,
    portable_session_set_timeout_ms, portable_session_write_chunk,
    portable_spawn_capture, portable_spawn_close, portable_spawn_done,
    portable_spawn_exit_ok, portable_spawn_kill, portable_spawn_open,
    portable_spawn_status, portable_spawn_stderr_all, portable_spawn_stdin_close,
    portable_spawn_stdin_write_all, portable_spawn_stdout_all, portable_spawn_wait,
    portable_supervisor_close, portable_supervisor_degraded, portable_supervisor_open,
    portable_supervisor_record_failure, portable_supervisor_record_recovery,
    portable_supervisor_should_restart, portable_task_close, portable_task_done,
    portable_task_join, portable_task_open, portable_task_sleep_ms, portable_stream_close,
    portable_stream_len, portable_stream_open, portable_stream_publish_buf,
    portable_stream_replay_close, portable_stream_replay_next, portable_stream_replay_offset,
    portable_stream_replay_open, portable_batch_close, portable_batch_flush_sum_u64,
    portable_batch_len, portable_batch_open, portable_batch_push_u64, portable_agg_add_u64,
    portable_agg_avg_u64, portable_agg_close, portable_agg_count, portable_agg_max_u64,
    portable_agg_min_u64, portable_agg_open_u64, portable_agg_sum_u64,
    portable_queue_close, portable_queue_len, portable_queue_open, portable_queue_pop_buf,
    portable_queue_push_buf, portable_window_add_u64, portable_window_avg_u64,
    portable_window_close, portable_window_count, portable_window_max_u64,
    portable_window_min_u64, portable_window_open_ms, portable_window_sum_u64,
    portable_msg_ack, portable_msg_delivery_total, portable_msg_failure_class,
    portable_msg_fanout, portable_msg_log_close, portable_msg_log_open,
    portable_msg_mark_retry, portable_msg_pending_count, portable_msg_recv_next,
    portable_msg_recv_seq, portable_msg_replay_close, portable_msg_replay_next,
    portable_msg_replay_open, portable_msg_replay_seq, portable_msg_retry_count,
    portable_msg_send, portable_msg_send_dedup, portable_msg_subscribe,
    portable_msg_subscriber_count,
    portable_task_stderr_all, portable_task_stdout_all, portable_time_format_hms,
    portable_time_parse_hms, portable_tls_exchange_all, portable_tls_listen_handle,
    reset_runtime_state, runtime_execution_guard, runtime_value_from_data,
    with_lowered_program_context, RuntimeValue,
};

#[derive(Debug, Clone)]
pub struct BytecodeProgram {
    #[allow(dead_code)]
    pub module: String,
    pub functions: Vec<BytecodeFunction>,
}

#[derive(Debug, Clone)]
pub struct BytecodeFunction {
    pub name: String,
    pub arg_slots: Vec<BytecodeArg>,
    pub return_kind: BytecodeValueKind,
    pub rand_seed: Option<u32>,
    pub slot_kinds: Vec<BytecodeValueKind>,
    pub slot_count: usize,
    pub entry_block: usize,
    pub blocks: Vec<BytecodeBlock>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BytecodeArg {
    pub name: String,
    pub slot: usize,
    pub kind: BytecodeValueKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BytecodeValueKind {
    U8,
    I32,
    I64,
    U64,
    U32,
    Bool,
    SpanI32,
    BufU8,
}

#[derive(Debug, Clone)]
pub struct BytecodeBlock {
    pub instructions: Vec<BytecodeInstruction>,
    pub terminator: BytecodeTerminator,
}

#[derive(Debug, Clone)]
pub struct BytecodeInstruction {
    pub dst: usize,
    #[allow(dead_code)]
    pub dst_kind: BytecodeValueKind,
    pub expr: BytecodeExpr,
}

#[derive(Debug, Clone)]
pub enum BytecodeOperand {
    Slot {
        index: usize,
        kind: BytecodeValueKind,
    },
    Imm(BytecodeImmediate),
}

#[derive(Debug, Clone)]
pub enum BytecodeImmediate {
    U8(u8),
    I32(i32),
    I64(i64),
    U64(u64),
    U32(u32),
    Bool(bool),
}

#[derive(Debug, Clone)]
pub enum BytecodeExpr {
    Move(BytecodeOperand),
    AllocBufU8 {
        len: BytecodeOperand,
    },
    DropBufU8 {
        value: BytecodeOperand,
    },
    ClockNowNs,
    RandU32,
    FsReadU32 {
        path: String,
    },
    FsWriteU32 {
        path: String,
        value: BytecodeOperand,
    },
    RtOpen {
        workers: BytecodeOperand,
    },
    RtSpawnU32 {
        runtime: BytecodeOperand,
        function: String,
        arg: BytecodeOperand,
    },
    RtSpawnBufU8 {
        runtime: BytecodeOperand,
        function: String,
        arg: BytecodeOperand,
    },
    RtTrySpawnU32 {
        runtime: BytecodeOperand,
        function: String,
        arg: BytecodeOperand,
    },
    RtTrySpawnBufU8 {
        runtime: BytecodeOperand,
        function: String,
        arg: BytecodeOperand,
    },
    RtDone {
        task: BytecodeOperand,
    },
    RtJoinU32 {
        task: BytecodeOperand,
    },
    RtJoinBufU8 {
        task: BytecodeOperand,
    },
    RtCancel {
        task: BytecodeOperand,
    },
    RtTaskClose {
        task: BytecodeOperand,
    },
    RtShutdown {
        runtime: BytecodeOperand,
        grace_ms: BytecodeOperand,
    },
    RtClose {
        runtime: BytecodeOperand,
    },
    RtInFlight {
        runtime: BytecodeOperand,
    },
    RtCancelled,
    ChanOpenU32 {
        capacity: BytecodeOperand,
    },
    ChanOpenBufU8 {
        capacity: BytecodeOperand,
    },
    ChanSendU32 {
        channel: BytecodeOperand,
        value: BytecodeOperand,
    },
    ChanSendBufU8 {
        channel: BytecodeOperand,
        value: BytecodeOperand,
    },
    ChanRecvU32 {
        channel: BytecodeOperand,
    },
    ChanRecvBufU8 {
        channel: BytecodeOperand,
    },
    ChanLen {
        channel: BytecodeOperand,
    },
    ChanClose {
        channel: BytecodeOperand,
    },
    DeadlineOpenMs {
        timeout_ms: BytecodeOperand,
    },
    DeadlineExpired {
        handle: BytecodeOperand,
    },
    DeadlineRemainingMs {
        handle: BytecodeOperand,
    },
    DeadlineClose {
        handle: BytecodeOperand,
    },
    CancelScopeOpen,
    CancelScopeChild {
        parent: BytecodeOperand,
    },
    CancelScopeBindTask {
        scope: BytecodeOperand,
        task: BytecodeOperand,
    },
    CancelScopeCancel {
        scope: BytecodeOperand,
    },
    CancelScopeCancelled {
        scope: BytecodeOperand,
    },
    CancelScopeClose {
        scope: BytecodeOperand,
    },
    RetryOpen {
        max_attempts: BytecodeOperand,
        base_backoff_ms: BytecodeOperand,
    },
    RetryRecordFailure {
        handle: BytecodeOperand,
    },
    RetryRecordSuccess {
        handle: BytecodeOperand,
    },
    RetryNextDelayMs {
        handle: BytecodeOperand,
    },
    RetryExhausted {
        handle: BytecodeOperand,
    },
    RetryClose {
        handle: BytecodeOperand,
    },
    CircuitOpen {
        threshold: BytecodeOperand,
        cooldown_ms: BytecodeOperand,
    },
    CircuitAllow {
        handle: BytecodeOperand,
    },
    CircuitRecordFailure {
        handle: BytecodeOperand,
    },
    CircuitRecordSuccess {
        handle: BytecodeOperand,
    },
    CircuitState {
        handle: BytecodeOperand,
    },
    CircuitClose {
        handle: BytecodeOperand,
    },
    BackpressureOpen {
        limit: BytecodeOperand,
    },
    BackpressureAcquire {
        handle: BytecodeOperand,
    },
    BackpressureRelease {
        handle: BytecodeOperand,
    },
    BackpressureSaturated {
        handle: BytecodeOperand,
    },
    BackpressureClose {
        handle: BytecodeOperand,
    },
    SupervisorOpen {
        restart_budget: BytecodeOperand,
        degrade_after: BytecodeOperand,
    },
    SupervisorRecordFailure {
        handle: BytecodeOperand,
        code: BytecodeOperand,
    },
    SupervisorRecordRecovery {
        handle: BytecodeOperand,
    },
    SupervisorShouldRestart {
        handle: BytecodeOperand,
    },
    SupervisorDegraded {
        handle: BytecodeOperand,
    },
    SupervisorClose {
        handle: BytecodeOperand,
    },
    FsReadAllU8 {
        path: String,
    },
    FsWriteAllU8 {
        path: String,
        value: BytecodeOperand,
    },
    NetWriteAllU8 {
        host: String,
        port: u16,
        value: BytecodeOperand,
    },
    NetExchangeAllU8 {
        host: String,
        port: u16,
        value: BytecodeOperand,
    },
    NetServeExchangeAllU8 {
        host: String,
        port: u16,
        response: BytecodeOperand,
    },
    NetListen {
        host: String,
        port: u16,
    },
    TlsListen {
        host: String,
        port: u16,
        cert: String,
        key: String,
        request_timeout_ms: u32,
        session_timeout_ms: u32,
        shutdown_grace_ms: u32,
    },
    NetAccept {
        listener: BytecodeOperand,
    },
    NetSessionOpen {
        host: String,
        port: u16,
    },
    HttpSessionAccept {
        listener: BytecodeOperand,
    },
    NetReadAllU8 {
        handle: BytecodeOperand,
    },
    SessionReadChunkU8 {
        handle: BytecodeOperand,
        chunk_size: BytecodeOperand,
    },
    HttpSessionRequest {
        handle: BytecodeOperand,
    },
    NetWriteHandleAllU8 {
        handle: BytecodeOperand,
        value: BytecodeOperand,
    },
    SessionWriteChunkU8 {
        handle: BytecodeOperand,
        value: BytecodeOperand,
    },
    SessionFlush {
        handle: BytecodeOperand,
    },
    SessionAlive {
        handle: BytecodeOperand,
    },
    SessionHeartbeatU8 {
        handle: BytecodeOperand,
        value: BytecodeOperand,
    },
    SessionBackpressure {
        handle: BytecodeOperand,
    },
    SessionBackpressureWait {
        handle: BytecodeOperand,
        max_pending: BytecodeOperand,
    },
    SessionResumeId {
        handle: BytecodeOperand,
    },
    SessionReconnect {
        handle: BytecodeOperand,
    },
    NetClose {
        handle: BytecodeOperand,
    },
    HttpSessionClose {
        handle: BytecodeOperand,
    },
    HttpMethodEq {
        request: BytecodeOperand,
        method: String,
    },
    HttpPathEq {
        request: BytecodeOperand,
        path: String,
    },
    HttpRequestMethod {
        request: BytecodeOperand,
    },
    HttpRequestPath {
        request: BytecodeOperand,
    },
    HttpRouteParam {
        request: BytecodeOperand,
        pattern: String,
        param: String,
    },
    HttpHeaderEq {
        request: BytecodeOperand,
        name: String,
        value: String,
    },
    HttpCookieEq {
        request: BytecodeOperand,
        name: String,
        value: String,
    },
    HttpStatusU32 {
        value: BytecodeOperand,
    },
    BufLit {
        literal: String,
    },
    BufConcat {
        left: BytecodeOperand,
        right: BytecodeOperand,
    },
    BufEqLit {
        value: BytecodeOperand,
        literal: String,
    },
    BufContainsLit {
        value: BytecodeOperand,
        literal: String,
    },
    HttpHeader {
        request: BytecodeOperand,
        name: String,
    },
    HttpHeaderCount {
        request: BytecodeOperand,
    },
    HttpHeaderName {
        request: BytecodeOperand,
        index: BytecodeOperand,
    },
    HttpHeaderValue {
        request: BytecodeOperand,
        index: BytecodeOperand,
    },
    HttpCookie {
        request: BytecodeOperand,
        name: String,
    },
    HttpQueryParam {
        request: BytecodeOperand,
        key: String,
    },
    HttpBody {
        request: BytecodeOperand,
    },
    HttpMultipartPartCount {
        request: BytecodeOperand,
    },
    HttpMultipartPartName {
        request: BytecodeOperand,
        index: BytecodeOperand,
    },
    HttpMultipartPartFilename {
        request: BytecodeOperand,
        index: BytecodeOperand,
    },
    HttpMultipartPartBody {
        request: BytecodeOperand,
        index: BytecodeOperand,
    },
    HttpBodyLimit {
        request: BytecodeOperand,
        limit: BytecodeOperand,
    },
    HttpBodyStreamOpen {
        request: BytecodeOperand,
    },
    HttpBodyStreamNext {
        handle: BytecodeOperand,
        chunk_size: BytecodeOperand,
    },
    HttpBodyStreamClose {
        handle: BytecodeOperand,
    },
    HttpResponseStreamOpen {
        handle: BytecodeOperand,
        status: BytecodeOperand,
        content_type: String,
    },
    HttpResponseStreamWrite {
        handle: BytecodeOperand,
        body: BytecodeOperand,
    },
    HttpResponseStreamClose {
        handle: BytecodeOperand,
    },
    HttpClientOpen {
        host: String,
        port: u16,
    },
    HttpClientRequest {
        handle: BytecodeOperand,
        request: BytecodeOperand,
    },
    HttpClientRequestRetry {
        handle: BytecodeOperand,
        retries: BytecodeOperand,
        backoff_ms: BytecodeOperand,
        request: BytecodeOperand,
    },
    HttpClientClose {
        handle: BytecodeOperand,
    },
    HttpClientPoolOpen {
        host: String,
        port: u16,
        max_size: BytecodeOperand,
    },
    HttpClientPoolAcquire {
        pool: BytecodeOperand,
    },
    HttpClientPoolRelease {
        pool: BytecodeOperand,
        handle: BytecodeOperand,
    },
    HttpClientPoolClose {
        pool: BytecodeOperand,
    },
    HttpServerConfigU32 {
        token: String,
    },
    MsgLogOpen,
    MsgLogClose {
        handle: BytecodeOperand,
    },
    MsgSend {
        handle: BytecodeOperand,
        conversation: String,
        recipient: String,
        payload: BytecodeOperand,
    },
    MsgSendDedup {
        handle: BytecodeOperand,
        conversation: String,
        recipient: String,
        dedup_key: BytecodeOperand,
        payload: BytecodeOperand,
    },
    MsgSubscribe {
        handle: BytecodeOperand,
        room: String,
        recipient: String,
    },
    MsgSubscriberCount {
        handle: BytecodeOperand,
        room: String,
    },
    MsgFanout {
        handle: BytecodeOperand,
        room: String,
        payload: BytecodeOperand,
    },
    MsgRecvNext {
        handle: BytecodeOperand,
        recipient: String,
    },
    MsgRecvSeq {
        handle: BytecodeOperand,
        recipient: String,
    },
    MsgAck {
        handle: BytecodeOperand,
        recipient: String,
        seq: BytecodeOperand,
    },
    MsgMarkRetry {
        handle: BytecodeOperand,
        recipient: String,
        seq: BytecodeOperand,
    },
    MsgRetryCount {
        handle: BytecodeOperand,
        recipient: String,
        seq: BytecodeOperand,
    },
    MsgPendingCount {
        handle: BytecodeOperand,
        recipient: String,
    },
    MsgDeliveryTotal {
        handle: BytecodeOperand,
        recipient: String,
    },
    MsgFailureClass {
        handle: BytecodeOperand,
    },
    MsgReplayOpen {
        handle: BytecodeOperand,
        recipient: String,
        from_seq: BytecodeOperand,
    },
    MsgReplayNext {
        handle: BytecodeOperand,
    },
    MsgReplaySeq {
        handle: BytecodeOperand,
    },
    MsgReplayClose {
        handle: BytecodeOperand,
    },
    ServiceOpen {
        name: String,
    },
    ServiceClose {
        handle: BytecodeOperand,
    },
    ServiceShutdown {
        handle: BytecodeOperand,
        grace_ms: BytecodeOperand,
    },
    ServiceLog {
        handle: BytecodeOperand,
        message: BytecodeOperand,
    },
    ServiceTraceBegin {
        handle: BytecodeOperand,
        name: String,
    },
    ServiceTraceEnd {
        trace: BytecodeOperand,
    },
    ServiceMetricCount {
        handle: BytecodeOperand,
        value: BytecodeOperand,
    },
    ServiceMetricCountDim {
        handle: BytecodeOperand,
        value: BytecodeOperand,
        metric: String,
        dimension: String,
    },
    ServiceMetricTotal {
        handle: BytecodeOperand,
        metric: String,
    },
    ServiceHealthStatus {
        handle: BytecodeOperand,
    },
    ServiceReadinessStatus {
        handle: BytecodeOperand,
    },
    ServiceSetHealth {
        handle: BytecodeOperand,
        status: BytecodeOperand,
    },
    ServiceSetReadiness {
        handle: BytecodeOperand,
        status: BytecodeOperand,
    },
    ServiceSetDegraded {
        handle: BytecodeOperand,
        degraded: BytecodeOperand,
    },
    ServiceDegraded {
        handle: BytecodeOperand,
    },
    ServiceEvent {
        handle: BytecodeOperand,
        class: String,
        message: BytecodeOperand,
    },
    ServiceEventTotal {
        handle: BytecodeOperand,
        class: String,
    },
    ServiceTraceLink {
        trace: BytecodeOperand,
        parent: BytecodeOperand,
    },
    ServiceTraceLinkCount {
        handle: BytecodeOperand,
    },
    ServiceFailureCount {
        handle: BytecodeOperand,
        class: String,
        value: BytecodeOperand,
    },
    ServiceFailureTotal {
        handle: BytecodeOperand,
        class: String,
    },
    ServiceCheckpointSaveU32 {
        handle: BytecodeOperand,
        key: String,
        value: BytecodeOperand,
    },
    ServiceCheckpointLoadU32 {
        handle: BytecodeOperand,
        key: String,
    },
    ServiceCheckpointExists {
        handle: BytecodeOperand,
        key: String,
    },
    ServiceMigrateDb {
        handle: BytecodeOperand,
        db_handle: BytecodeOperand,
    },
    ServiceRoute {
        request: BytecodeOperand,
        method: String,
        path: String,
    },
    ServiceRequireHeader {
        request: BytecodeOperand,
        name: String,
        value: String,
    },
    ServiceErrorStatus {
        kind: String,
    },
    TlsServerConfigU32 {
        value: u32,
    },
    TlsServerConfigBuf {
        value: String,
    },
    ListenerSetTimeoutMs {
        handle: BytecodeOperand,
        value: BytecodeOperand,
    },
    SessionSetTimeoutMs {
        handle: BytecodeOperand,
        value: BytecodeOperand,
    },
    ListenerSetShutdownGraceMs {
        handle: BytecodeOperand,
        value: BytecodeOperand,
    },
    BufParseU32 {
        value: BytecodeOperand,
    },
    BufParseBool {
        value: BytecodeOperand,
    },
    HttpWriteResponse {
        handle: BytecodeOperand,
        status: BytecodeOperand,
        body: BytecodeOperand,
    },
    HttpWriteTextResponse {
        handle: BytecodeOperand,
        status: BytecodeOperand,
        body: BytecodeOperand,
    },
    HttpWriteTextResponseCookie {
        handle: BytecodeOperand,
        status: BytecodeOperand,
        cookie_name: String,
        cookie_value: String,
        body: BytecodeOperand,
    },
    HttpWriteTextResponseHeaders2 {
        handle: BytecodeOperand,
        status: BytecodeOperand,
        header1_name: String,
        header1_value: String,
        header2_name: String,
        header2_value: String,
        body: BytecodeOperand,
    },
    HttpSessionWriteText {
        handle: BytecodeOperand,
        status: BytecodeOperand,
        body: BytecodeOperand,
    },
    HttpSessionWriteTextCookie {
        handle: BytecodeOperand,
        status: BytecodeOperand,
        cookie_name: String,
        cookie_value: String,
        body: BytecodeOperand,
    },
    HttpSessionWriteTextHeaders2 {
        handle: BytecodeOperand,
        status: BytecodeOperand,
        header1_name: String,
        header1_value: String,
        header2_name: String,
        header2_value: String,
        body: BytecodeOperand,
    },
    HttpWriteJsonResponse {
        handle: BytecodeOperand,
        status: BytecodeOperand,
        body: BytecodeOperand,
    },
    HttpWriteJsonResponseCookie {
        handle: BytecodeOperand,
        status: BytecodeOperand,
        cookie_name: String,
        cookie_value: String,
        body: BytecodeOperand,
    },
    HttpWriteJsonResponseHeaders2 {
        handle: BytecodeOperand,
        status: BytecodeOperand,
        header1_name: String,
        header1_value: String,
        header2_name: String,
        header2_value: String,
        body: BytecodeOperand,
    },
    HttpSessionWriteJson {
        handle: BytecodeOperand,
        status: BytecodeOperand,
        body: BytecodeOperand,
    },
    HttpSessionWriteJsonCookie {
        handle: BytecodeOperand,
        status: BytecodeOperand,
        cookie_name: String,
        cookie_value: String,
        body: BytecodeOperand,
    },
    HttpSessionWriteJsonHeaders2 {
        handle: BytecodeOperand,
        status: BytecodeOperand,
        header1_name: String,
        header1_value: String,
        header2_name: String,
        header2_value: String,
        body: BytecodeOperand,
    },
    HttpWriteResponseHeader {
        handle: BytecodeOperand,
        status: BytecodeOperand,
        header_name: String,
        header_value: String,
        body: BytecodeOperand,
    },
    JsonGetU32 {
        value: BytecodeOperand,
        key: String,
    },
    JsonGetBool {
        value: BytecodeOperand,
        key: String,
    },
    JsonHasKey {
        value: BytecodeOperand,
        key: String,
    },
    JsonGetBufU8 {
        value: BytecodeOperand,
        key: String,
    },
    JsonGetStr {
        value: BytecodeOperand,
        key: String,
    },
    JsonGetU32Or {
        value: BytecodeOperand,
        key: String,
        default_value: BytecodeOperand,
    },
    JsonGetBoolOr {
        value: BytecodeOperand,
        key: String,
        default_value: BytecodeOperand,
    },
    JsonGetBufOr {
        value: BytecodeOperand,
        key: String,
        default_value: BytecodeOperand,
    },
    JsonGetStrOr {
        value: BytecodeOperand,
        key: String,
        default_value: BytecodeOperand,
    },
    JsonArrayLen {
        value: BytecodeOperand,
    },
    JsonIndexU32 {
        value: BytecodeOperand,
        index: BytecodeOperand,
    },
    JsonIndexBool {
        value: BytecodeOperand,
        index: BytecodeOperand,
    },
    JsonIndexStr {
        value: BytecodeOperand,
        index: BytecodeOperand,
    },
    JsonEncodeObj {
        entries: Vec<(String, BytecodeOperand)>,
    },
    JsonEncodeArr {
        values: Vec<BytecodeOperand>,
    },
    StrLit {
        literal: String,
    },
    StrConcat {
        left: BytecodeOperand,
        right: BytecodeOperand,
    },
    StrFromU32 {
        value: BytecodeOperand,
    },
    StrFromBool {
        value: BytecodeOperand,
    },
    StrEqLit {
        value: BytecodeOperand,
        literal: String,
    },
    StrToBuf {
        value: BytecodeOperand,
    },
    BufToStr {
        value: BytecodeOperand,
    },
    BufHexStr {
        value: BytecodeOperand,
    },
    ConfigGetU32 {
        value: u32,
    },
    ConfigGetBool {
        value: bool,
    },
    ConfigGetStr {
        value: String,
    },
    ConfigHas {
        present: bool,
    },
    EnvGetU32 {
        key: String,
    },
    EnvGetBool {
        key: String,
    },
    EnvGetStr {
        key: String,
    },
    EnvHas {
        key: String,
    },
    BufBeforeLit {
        value: BytecodeOperand,
        literal: String,
    },
    BufAfterLit {
        value: BytecodeOperand,
        literal: String,
    },
    BufTrimAscii {
        value: BytecodeOperand,
    },
    DateParseYmd {
        value: BytecodeOperand,
    },
    TimeParseHms {
        value: BytecodeOperand,
    },
    DateFormatYmd {
        value: BytecodeOperand,
    },
    TimeFormatHms {
        value: BytecodeOperand,
    },
    DbOpen {
        path: String,
    },
    DbClose {
        handle: BytecodeOperand,
    },
    DbExec {
        handle: BytecodeOperand,
        sql: BytecodeOperand,
    },
    DbPrepare {
        handle: BytecodeOperand,
        name: String,
        sql: BytecodeOperand,
    },
    DbExecPrepared {
        handle: BytecodeOperand,
        name: String,
        params: BytecodeOperand,
    },
    DbQueryU32 {
        handle: BytecodeOperand,
        sql: BytecodeOperand,
    },
    DbQueryBufU8 {
        handle: BytecodeOperand,
        sql: BytecodeOperand,
    },
    DbQueryRow {
        handle: BytecodeOperand,
        sql: BytecodeOperand,
    },
    DbQueryPreparedU32 {
        handle: BytecodeOperand,
        name: String,
        params: BytecodeOperand,
    },
    DbQueryPreparedBufU8 {
        handle: BytecodeOperand,
        name: String,
        params: BytecodeOperand,
    },
    DbQueryPreparedRow {
        handle: BytecodeOperand,
        name: String,
        params: BytecodeOperand,
    },
    DbRowFound {
        row: BytecodeOperand,
    },
    DbLastErrorCode {
        handle: BytecodeOperand,
    },
    DbLastErrorRetryable {
        handle: BytecodeOperand,
    },
    DbBegin {
        handle: BytecodeOperand,
    },
    DbCommit {
        handle: BytecodeOperand,
    },
    DbRollback {
        handle: BytecodeOperand,
    },
    DbPoolOpen {
        target: String,
        max_size: BytecodeOperand,
    },
    DbPoolSetMaxIdle {
        pool: BytecodeOperand,
        value: BytecodeOperand,
    },
    DbPoolLeased {
        pool: BytecodeOperand,
    },
    DbPoolAcquire {
        pool: BytecodeOperand,
    },
    DbPoolRelease {
        pool: BytecodeOperand,
        handle: BytecodeOperand,
    },
    DbPoolClose {
        pool: BytecodeOperand,
    },
    CacheOpen {
        target: String,
    },
    CacheClose {
        handle: BytecodeOperand,
    },
    CacheGetBufU8 {
        handle: BytecodeOperand,
        key: BytecodeOperand,
    },
    CacheSetBufU8 {
        handle: BytecodeOperand,
        key: BytecodeOperand,
        value: BytecodeOperand,
    },
    CacheSetBufTtlU8 {
        handle: BytecodeOperand,
        key: BytecodeOperand,
        ttl_ms: BytecodeOperand,
        value: BytecodeOperand,
    },
    CacheDel {
        handle: BytecodeOperand,
        key: BytecodeOperand,
    },
    QueueOpen {
        target: String,
    },
    QueueClose {
        handle: BytecodeOperand,
    },
    QueuePushBufU8 {
        handle: BytecodeOperand,
        value: BytecodeOperand,
    },
    QueuePopBufU8 {
        handle: BytecodeOperand,
    },
    QueueLen {
        handle: BytecodeOperand,
    },
    StreamOpen {
        target: String,
    },
    StreamClose {
        handle: BytecodeOperand,
    },
    StreamPublishBufU8 {
        handle: BytecodeOperand,
        value: BytecodeOperand,
    },
    StreamLen {
        handle: BytecodeOperand,
    },
    StreamReplayOpen {
        handle: BytecodeOperand,
        offset: BytecodeOperand,
    },
    StreamReplayNextU8 {
        handle: BytecodeOperand,
    },
    StreamReplayOffset {
        handle: BytecodeOperand,
    },
    StreamReplayClose {
        handle: BytecodeOperand,
    },
    ShardRouteU32 {
        key: BytecodeOperand,
        shard_count: BytecodeOperand,
    },
    LeaseOpen {
        target: String,
    },
    LeaseAcquire {
        handle: BytecodeOperand,
        owner: BytecodeOperand,
    },
    LeaseOwner {
        handle: BytecodeOperand,
    },
    LeaseTransfer {
        handle: BytecodeOperand,
        owner: BytecodeOperand,
    },
    LeaseRelease {
        handle: BytecodeOperand,
        owner: BytecodeOperand,
    },
    LeaseClose {
        handle: BytecodeOperand,
    },
    PlacementOpen {
        target: String,
    },
    PlacementAssign {
        handle: BytecodeOperand,
        shard: BytecodeOperand,
        node: BytecodeOperand,
    },
    PlacementLookup {
        handle: BytecodeOperand,
        shard: BytecodeOperand,
    },
    PlacementClose {
        handle: BytecodeOperand,
    },
    CoordOpen {
        target: String,
    },
    CoordStoreU32 {
        handle: BytecodeOperand,
        key: String,
        value: BytecodeOperand,
    },
    CoordLoadU32 {
        handle: BytecodeOperand,
        key: String,
    },
    CoordClose {
        handle: BytecodeOperand,
    },
    BatchOpen,
    BatchPushU64 {
        handle: BytecodeOperand,
        value: BytecodeOperand,
    },
    BatchLen {
        handle: BytecodeOperand,
    },
    BatchFlushSumU64 {
        handle: BytecodeOperand,
    },
    BatchClose {
        handle: BytecodeOperand,
    },
    AggOpenU64,
    AggAddU64 {
        handle: BytecodeOperand,
        value: BytecodeOperand,
    },
    AggCount {
        handle: BytecodeOperand,
    },
    AggSumU64 {
        handle: BytecodeOperand,
    },
    AggAvgU64 {
        handle: BytecodeOperand,
    },
    AggMinU64 {
        handle: BytecodeOperand,
    },
    AggMaxU64 {
        handle: BytecodeOperand,
    },
    AggClose {
        handle: BytecodeOperand,
    },
    WindowOpenMs {
        width_ms: BytecodeOperand,
    },
    WindowAddU64 {
        handle: BytecodeOperand,
        value: BytecodeOperand,
    },
    WindowCount {
        handle: BytecodeOperand,
    },
    WindowSumU64 {
        handle: BytecodeOperand,
    },
    WindowAvgU64 {
        handle: BytecodeOperand,
    },
    WindowMinU64 {
        handle: BytecodeOperand,
    },
    WindowMaxU64 {
        handle: BytecodeOperand,
    },
    WindowClose {
        handle: BytecodeOperand,
    },
    TlsExchangeAllU8 {
        host: String,
        port: u16,
        value: BytecodeOperand,
    },
    TaskSleepMs {
        value: BytecodeOperand,
    },
    TaskOpen {
        command: String,
        argv: Vec<String>,
        env: Vec<(String, String)>,
    },
    TaskDone {
        handle: BytecodeOperand,
    },
    TaskJoinStatus {
        handle: BytecodeOperand,
    },
    TaskStdoutAllU8 {
        handle: BytecodeOperand,
    },
    TaskStderrAllU8 {
        handle: BytecodeOperand,
    },
    TaskClose {
        handle: BytecodeOperand,
    },
    SpawnCaptureAllU8 {
        command: String,
        argv: Vec<String>,
        env: Vec<(String, String)>,
    },
    SpawnCaptureStderrAllU8 {
        command: String,
        argv: Vec<String>,
        env: Vec<(String, String)>,
    },
    SpawnCall {
        command: String,
        argv: Vec<String>,
        env: Vec<(String, String)>,
    },
    SpawnOpen {
        command: String,
        argv: Vec<String>,
        env: Vec<(String, String)>,
    },
    SpawnWait {
        handle: BytecodeOperand,
    },
    SpawnStdoutAllU8 {
        handle: BytecodeOperand,
    },
    SpawnStderrAllU8 {
        handle: BytecodeOperand,
    },
    SpawnStdinWriteAllU8 {
        handle: BytecodeOperand,
        value: BytecodeOperand,
    },
    SpawnStdinClose {
        handle: BytecodeOperand,
    },
    SpawnDone {
        handle: BytecodeOperand,
    },
    SpawnExitOk {
        handle: BytecodeOperand,
    },
    SpawnKill {
        handle: BytecodeOperand,
    },
    SpawnClose {
        handle: BytecodeOperand,
    },
    NetConnect {
        host: String,
        port: u16,
    },
    FfiCall {
        symbol: String,
        args: Vec<BytecodeOperand>,
        ret_kind: BytecodeValueKind,
    },
    FfiCallCStr {
        symbol: String,
        arg_slot: usize,
        ret_kind: BytecodeValueKind,
    },
    FfiOpenLib {
        path: String,
    },
    FfiCloseLib {
        handle: BytecodeOperand,
    },
    FfiBufPtr {
        value: BytecodeOperand,
    },
    FfiCallLib {
        handle: BytecodeOperand,
        symbol: String,
        args: Vec<BytecodeOperand>,
        ret_kind: BytecodeValueKind,
    },
    FfiCallLibCStr {
        handle: BytecodeOperand,
        symbol: String,
        arg_slot: usize,
        ret_kind: BytecodeValueKind,
    },
    LenSpanI32 {
        source: usize,
    },
    LenBufU8 {
        source: usize,
    },
    StoreBufU8 {
        source: usize,
        index: BytecodeOperand,
        value: BytecodeOperand,
    },
    LoadBufU8 {
        source: usize,
        index: BytecodeOperand,
    },
    LoadSpanI32 {
        source: usize,
        index: BytecodeOperand,
    },
    AbsI32 {
        value: BytecodeOperand,
    },
    Binary {
        op: LoweredExecBinaryOp,
        left: BytecodeOperand,
        right: BytecodeOperand,
    },
    SextI64 {
        value: BytecodeOperand,
    },
}

#[derive(Debug, Clone)]
pub struct BytecodeEdge {
    pub moves: Vec<BytecodeInstruction>,
    pub target: usize,
}

#[derive(Debug, Clone)]
pub struct BytecodeMatchCase {
    pub tag_index: usize,
    pub edge: BytecodeEdge,
}

#[derive(Debug, Clone)]
pub enum BytecodeTerminator {
    Return(BytecodeOperand),
    Jump(BytecodeEdge),
    Branch {
        condition: BytecodeOperand,
        truthy: BytecodeEdge,
        falsy: BytecodeEdge,
    },
    Match {
        value: BytecodeOperand,
        cases: Vec<BytecodeMatchCase>,
        default: BytecodeEdge,
    },
}

pub fn compile_bytecode_program(program: &LoweredProgram) -> Result<BytecodeProgram, String> {
    Ok(BytecodeProgram {
        module: program.module.clone(),
        functions: program
            .functions
            .iter()
            .map(compile_bytecode_function)
            .collect::<Result<Vec<_>, _>>()?,
    })
}

pub fn run_bytecode_function(
    program: &BytecodeProgram,
    function_name: &str,
    args: &HashMap<String, RuntimeValue>,
) -> Result<RuntimeValue, String> {
    let _guard = runtime_execution_guard()
        .lock()
        .map_err(|_| "portable runtime execution mutex poisoned".to_string())?;
    reset_runtime_state()?;
    run_bytecode_function_inner(program, function_name, args)
}

fn run_bytecode_function_inner(
    program: &BytecodeProgram,
    function_name: &str,
    args: &HashMap<String, RuntimeValue>,
) -> Result<RuntimeValue, String> {
    let function = program
        .functions
        .iter()
        .find(|function| function.name == function_name)
        .ok_or_else(|| format!("unknown bytecode function {function_name}"))?;
    let mut slots = vec![None; function.slot_count];
    let mut rand_state = function.rand_seed;
    for arg in &function.arg_slots {
        let value = args
            .get(&arg.name)
            .cloned()
            .ok_or_else(|| format!("missing bytecode arg {} for {function_name}", arg.name))?;
        slots[arg.slot] = Some(value);
    }
    let mut current = function.entry_block;
    let mut steps = 0usize;
    loop {
        steps += 1;
        if steps > 10_000_000 {
            return Err(format!(
                "bytecode execution step limit exceeded in {function_name}"
            ));
        }
        let block = function
            .blocks
            .get(current)
            .ok_or_else(|| format!("missing bytecode block {current} in {function_name}"))?;
        for instruction in &block.instructions {
            let value = eval_bytecode_expr(&instruction.expr, &slots, &mut rand_state)?;
            slots[instruction.dst] = Some(value);
        }
        match &block.terminator {
            BytecodeTerminator::Return(operand) => return eval_bytecode_operand(operand, &slots),
            BytecodeTerminator::Jump(edge) => {
                apply_bytecode_edge(edge, &mut slots, &mut rand_state)?;
                current = edge.target;
            }
            BytecodeTerminator::Branch {
                condition,
                truthy,
                falsy,
            } => match eval_bytecode_operand(condition, &slots)? {
                RuntimeValue::Bool(true) => {
                    apply_bytecode_edge(truthy, &mut slots, &mut rand_state)?;
                    current = truthy.target;
                }
                RuntimeValue::Bool(false) => {
                    apply_bytecode_edge(falsy, &mut slots, &mut rand_state)?;
                    current = falsy.target;
                }
                other => {
                    return Err(format!(
                        "bytecode branch condition must be bool in {function_name}, got {other:?}"
                    ))
                }
            },
            BytecodeTerminator::Match {
                value,
                cases,
                default,
            } => {
                let tag = match eval_bytecode_operand(value, &slots)? {
                    RuntimeValue::I32(value) => value as i64,
                    RuntimeValue::I64(value) => value,
                    RuntimeValue::U64(value) => value as i64,
                    RuntimeValue::U32(value) => value as i64,
                    other => {
                        return Err(format!(
                            "bytecode match value must be integer in {function_name}, got {other:?}"
                        ))
                    }
                };
                let edge = cases
                    .iter()
                    .find(|case| case.tag_index as i64 == tag)
                    .map(|case| &case.edge)
                    .unwrap_or(default);
                apply_bytecode_edge(edge, &mut slots, &mut rand_state)?;
                current = edge.target;
            }
        }
    }
}

fn compile_bytecode_function(
    function: &crate::codegen_c::LoweredFunction,
) -> Result<BytecodeFunction, String> {
    let mut slot_by_name = HashMap::new();
    let mut slot_kind_by_name = HashMap::new();
    let mut next_slot = 0usize;
    let mut arg_slots = Vec::new();
    for (ty, name) in &function.args {
        let kind = bytecode_kind_for_c_type(ty)?;
        slot_by_name.insert(name.clone(), next_slot);
        slot_kind_by_name.insert(name.clone(), kind);
        arg_slots.push(BytecodeArg {
            name: name.clone(),
            slot: next_slot,
            kind,
        });
        next_slot += 1;
    }
    for (name, ty) in &function.declarations {
        if !slot_by_name.contains_key(name) {
            slot_by_name.insert(name.clone(), next_slot);
            slot_kind_by_name.insert(name.clone(), bytecode_kind_for_c_type(ty)?);
            next_slot += 1;
        }
    }
    let mut slot_kinds = vec![BytecodeValueKind::I64; next_slot];
    for (name, slot) in &slot_by_name {
        let kind = *slot_kind_by_name
            .get(name)
            .ok_or_else(|| format!("missing bytecode slot kind for {}", name))?;
        slot_kinds[*slot] = kind;
    }
    let block_by_label = function
        .blocks
        .iter()
        .enumerate()
        .map(|(index, block)| (block.label.as_str(), index))
        .collect::<HashMap<_, _>>();
    let entry_block = *block_by_label
        .get("b0")
        .ok_or_else(|| format!("missing bytecode entry block b0 for {}", function.name))?;
    let blocks = function
        .blocks
        .iter()
        .map(|block| {
            let instructions = block
                .statements
                .iter()
                .map(|statement| match statement {
                    LoweredStatement::Assign(assignment) => Ok(BytecodeInstruction {
                        dst: *slot_by_name.get(&assignment.target).ok_or_else(|| {
                            format!(
                                "unknown bytecode assignment target {} in {}",
                                assignment.target, function.name
                            )
                        })?,
                        dst_kind: *slot_kind_by_name.get(&assignment.target).ok_or_else(|| {
                            format!(
                                "missing bytecode assignment kind {} in {}",
                                assignment.target, function.name
                            )
                        })?,
                        expr: compile_bytecode_expr(
                            assignment.exec_expr.as_ref().ok_or_else(|| {
                                format!(
                                    "unsupported bytecode assignment {} in {}",
                                    assignment.target, function.name
                                )
                            })?,
                            &slot_by_name,
                            &slot_kind_by_name,
                        )?,
                    }),
                })
                .collect::<Result<Vec<_>, String>>()?;
            let terminator = compile_bytecode_terminator(
                &block.terminator,
                &slot_by_name,
                &slot_kind_by_name,
                &block_by_label,
            )?;
            Ok(BytecodeBlock {
                instructions,
                terminator,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(BytecodeFunction {
        name: function.name.clone(),
        arg_slots,
        return_kind: bytecode_kind_for_c_type(&function.ret_c_type)?,
        rand_seed: function.rand_seed,
        slot_kinds,
        slot_count: next_slot,
        entry_block,
        blocks,
    })
}

fn compile_bytecode_terminator(
    terminator: &LoweredTerminator,
    slot_by_name: &HashMap<String, usize>,
    slot_kind_by_name: &HashMap<String, BytecodeValueKind>,
    block_by_label: &HashMap<&str, usize>,
) -> Result<BytecodeTerminator, String> {
    Ok(match terminator {
        LoweredTerminator::Return {
            exec_value: Some(value),
            ..
        } => BytecodeTerminator::Return(compile_bytecode_operand(
            value,
            slot_by_name,
            slot_kind_by_name,
        )?),
        LoweredTerminator::Jump { edge } => BytecodeTerminator::Jump(compile_bytecode_edge(
            edge,
            slot_by_name,
            slot_kind_by_name,
            block_by_label,
        )?),
        LoweredTerminator::Branch {
            exec_condition: Some(condition),
            truthy,
            falsy,
            ..
        } => BytecodeTerminator::Branch {
            condition: compile_bytecode_operand(condition, slot_by_name, slot_kind_by_name)?,
            truthy: compile_bytecode_edge(truthy, slot_by_name, slot_kind_by_name, block_by_label)?,
            falsy: compile_bytecode_edge(falsy, slot_by_name, slot_kind_by_name, block_by_label)?,
        },
        LoweredTerminator::Match {
            exec_value: Some(value),
            cases,
            default,
            ..
        } => BytecodeTerminator::Match {
            value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
            cases: cases
                .iter()
                .map(|case| {
                    Ok(BytecodeMatchCase {
                        tag_index: case.tag_index,
                        edge: compile_bytecode_edge(
                            &case.edge,
                            slot_by_name,
                            slot_kind_by_name,
                            block_by_label,
                        )?,
                    })
                })
                .collect::<Result<Vec<_>, String>>()?,
            default: compile_bytecode_edge(
                default,
                slot_by_name,
                slot_kind_by_name,
                block_by_label,
            )?,
        },
        other => return Err(format!("unsupported bytecode terminator {other:?}")),
    })
}

fn compile_bytecode_edge(
    edge: &crate::codegen_c::LoweredEdge,
    slot_by_name: &HashMap<String, usize>,
    slot_kind_by_name: &HashMap<String, BytecodeValueKind>,
    block_by_label: &HashMap<&str, usize>,
) -> Result<BytecodeEdge, String> {
    Ok(BytecodeEdge {
        moves: edge
            .assignments
            .iter()
            .map(|assignment| {
                Ok(BytecodeInstruction {
                    dst: *slot_by_name.get(&assignment.target).ok_or_else(|| {
                        format!("unknown bytecode edge target {}", assignment.target)
                    })?,
                    dst_kind: *slot_kind_by_name.get(&assignment.target).ok_or_else(|| {
                        format!("unknown bytecode edge target kind {}", assignment.target)
                    })?,
                    expr: compile_bytecode_expr(
                        assignment.exec_expr.as_ref().ok_or_else(|| {
                            format!("unsupported bytecode edge {}", assignment.target)
                        })?,
                        slot_by_name,
                        slot_kind_by_name,
                    )?,
                })
            })
            .collect::<Result<Vec<_>, String>>()?,
        target: *block_by_label
            .get(edge.label.as_str())
            .ok_or_else(|| format!("unknown bytecode edge block {}", edge.label))?,
    })
}

fn compile_bytecode_expr(
    expr: &LoweredExecExpr,
    slot_by_name: &HashMap<String, usize>,
    slot_kind_by_name: &HashMap<String, BytecodeValueKind>,
) -> Result<BytecodeExpr, String> {
    Ok(match expr {
        LoweredExecExpr::Move(operand) => BytecodeExpr::Move(compile_bytecode_operand(
            operand,
            slot_by_name,
            slot_kind_by_name,
        )?),
        LoweredExecExpr::AllocBufU8 { region: _, len } => BytecodeExpr::AllocBufU8 {
            len: compile_bytecode_operand(len, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::DropBufU8 { value } => BytecodeExpr::DropBufU8 {
            value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::ClockNowNs => BytecodeExpr::ClockNowNs,
        LoweredExecExpr::RandU32 => BytecodeExpr::RandU32,
        LoweredExecExpr::FsReadU32 { path } => BytecodeExpr::FsReadU32 { path: path.clone() },
        LoweredExecExpr::FsWriteU32 { path, value } => BytecodeExpr::FsWriteU32 {
            path: path.clone(),
            value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::RtOpen { workers } => BytecodeExpr::RtOpen {
            workers: compile_bytecode_operand(workers, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::RtSpawnU32 {
            runtime,
            function,
            arg,
        } => BytecodeExpr::RtSpawnU32 {
            runtime: compile_bytecode_operand(runtime, slot_by_name, slot_kind_by_name)?,
            function: function.clone(),
            arg: compile_bytecode_operand(arg, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::RtSpawnBufU8 {
            runtime,
            function,
            arg,
        } => BytecodeExpr::RtSpawnBufU8 {
            runtime: compile_bytecode_operand(runtime, slot_by_name, slot_kind_by_name)?,
            function: function.clone(),
            arg: compile_bytecode_operand(arg, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::RtTrySpawnU32 {
            runtime,
            function,
            arg,
        } => BytecodeExpr::RtTrySpawnU32 {
            runtime: compile_bytecode_operand(runtime, slot_by_name, slot_kind_by_name)?,
            function: function.clone(),
            arg: compile_bytecode_operand(arg, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::RtTrySpawnBufU8 {
            runtime,
            function,
            arg,
        } => BytecodeExpr::RtTrySpawnBufU8 {
            runtime: compile_bytecode_operand(runtime, slot_by_name, slot_kind_by_name)?,
            function: function.clone(),
            arg: compile_bytecode_operand(arg, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::RtDone { task } => BytecodeExpr::RtDone {
            task: compile_bytecode_operand(task, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::RtJoinU32 { task } => BytecodeExpr::RtJoinU32 {
            task: compile_bytecode_operand(task, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::RtJoinBufU8 { task } => BytecodeExpr::RtJoinBufU8 {
            task: compile_bytecode_operand(task, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::RtCancel { task } => BytecodeExpr::RtCancel {
            task: compile_bytecode_operand(task, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::RtTaskClose { task } => BytecodeExpr::RtTaskClose {
            task: compile_bytecode_operand(task, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::RtShutdown { runtime, grace_ms } => BytecodeExpr::RtShutdown {
            runtime: compile_bytecode_operand(runtime, slot_by_name, slot_kind_by_name)?,
            grace_ms: compile_bytecode_operand(grace_ms, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::RtClose { runtime } => BytecodeExpr::RtClose {
            runtime: compile_bytecode_operand(runtime, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::RtInFlight { runtime } => BytecodeExpr::RtInFlight {
            runtime: compile_bytecode_operand(runtime, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::RtCancelled => BytecodeExpr::RtCancelled,
        LoweredExecExpr::ChanOpenU32 { capacity } => BytecodeExpr::ChanOpenU32 {
            capacity: compile_bytecode_operand(capacity, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::ChanOpenBufU8 { capacity } => BytecodeExpr::ChanOpenBufU8 {
            capacity: compile_bytecode_operand(capacity, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::ChanSendU32 { channel, value } => BytecodeExpr::ChanSendU32 {
            channel: compile_bytecode_operand(channel, slot_by_name, slot_kind_by_name)?,
            value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::ChanSendBufU8 { channel, value } => BytecodeExpr::ChanSendBufU8 {
            channel: compile_bytecode_operand(channel, slot_by_name, slot_kind_by_name)?,
            value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::ChanRecvU32 { channel } => BytecodeExpr::ChanRecvU32 {
            channel: compile_bytecode_operand(channel, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::ChanRecvBufU8 { channel } => BytecodeExpr::ChanRecvBufU8 {
            channel: compile_bytecode_operand(channel, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::ChanLen { channel } => BytecodeExpr::ChanLen {
            channel: compile_bytecode_operand(channel, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::ChanClose { channel } => BytecodeExpr::ChanClose {
            channel: compile_bytecode_operand(channel, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::DeadlineOpenMs { timeout_ms } => BytecodeExpr::DeadlineOpenMs {
            timeout_ms: compile_bytecode_operand(timeout_ms, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::DeadlineExpired { handle } => BytecodeExpr::DeadlineExpired {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::DeadlineRemainingMs { handle } => BytecodeExpr::DeadlineRemainingMs {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::DeadlineClose { handle } => BytecodeExpr::DeadlineClose {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::CancelScopeOpen => BytecodeExpr::CancelScopeOpen,
        LoweredExecExpr::CancelScopeChild { parent } => BytecodeExpr::CancelScopeChild {
            parent: compile_bytecode_operand(parent, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::CancelScopeBindTask { scope, task } => {
            BytecodeExpr::CancelScopeBindTask {
                scope: compile_bytecode_operand(scope, slot_by_name, slot_kind_by_name)?,
                task: compile_bytecode_operand(task, slot_by_name, slot_kind_by_name)?,
            }
        }
        LoweredExecExpr::CancelScopeCancel { scope } => BytecodeExpr::CancelScopeCancel {
            scope: compile_bytecode_operand(scope, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::CancelScopeCancelled { scope } => BytecodeExpr::CancelScopeCancelled {
            scope: compile_bytecode_operand(scope, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::CancelScopeClose { scope } => BytecodeExpr::CancelScopeClose {
            scope: compile_bytecode_operand(scope, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::RetryOpen {
            max_attempts,
            base_backoff_ms,
        } => BytecodeExpr::RetryOpen {
            max_attempts: compile_bytecode_operand(
                max_attempts,
                slot_by_name,
                slot_kind_by_name,
            )?,
            base_backoff_ms: compile_bytecode_operand(
                base_backoff_ms,
                slot_by_name,
                slot_kind_by_name,
            )?,
        },
        LoweredExecExpr::RetryRecordFailure { handle } => BytecodeExpr::RetryRecordFailure {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::RetryRecordSuccess { handle } => BytecodeExpr::RetryRecordSuccess {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::RetryNextDelayMs { handle } => BytecodeExpr::RetryNextDelayMs {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::RetryExhausted { handle } => BytecodeExpr::RetryExhausted {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::RetryClose { handle } => BytecodeExpr::RetryClose {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::CircuitOpen {
            threshold,
            cooldown_ms,
        } => BytecodeExpr::CircuitOpen {
            threshold: compile_bytecode_operand(threshold, slot_by_name, slot_kind_by_name)?,
            cooldown_ms: compile_bytecode_operand(cooldown_ms, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::CircuitAllow { handle } => BytecodeExpr::CircuitAllow {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::CircuitRecordFailure { handle } => BytecodeExpr::CircuitRecordFailure {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::CircuitRecordSuccess { handle } => BytecodeExpr::CircuitRecordSuccess {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::CircuitState { handle } => BytecodeExpr::CircuitState {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::CircuitClose { handle } => BytecodeExpr::CircuitClose {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::BackpressureOpen { limit } => BytecodeExpr::BackpressureOpen {
            limit: compile_bytecode_operand(limit, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::BackpressureAcquire { handle } => BytecodeExpr::BackpressureAcquire {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::BackpressureRelease { handle } => BytecodeExpr::BackpressureRelease {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::BackpressureSaturated { handle } => BytecodeExpr::BackpressureSaturated {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::BackpressureClose { handle } => BytecodeExpr::BackpressureClose {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::SupervisorOpen {
            restart_budget,
            degrade_after,
        } => BytecodeExpr::SupervisorOpen {
            restart_budget: compile_bytecode_operand(
                restart_budget,
                slot_by_name,
                slot_kind_by_name,
            )?,
            degrade_after: compile_bytecode_operand(
                degrade_after,
                slot_by_name,
                slot_kind_by_name,
            )?,
        },
        LoweredExecExpr::SupervisorRecordFailure { handle, code } => {
            BytecodeExpr::SupervisorRecordFailure {
                handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
                code: compile_bytecode_operand(code, slot_by_name, slot_kind_by_name)?,
            }
        }
        LoweredExecExpr::SupervisorRecordRecovery { handle } => {
            BytecodeExpr::SupervisorRecordRecovery {
                handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            }
        }
        LoweredExecExpr::SupervisorShouldRestart { handle } => {
            BytecodeExpr::SupervisorShouldRestart {
                handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            }
        }
        LoweredExecExpr::SupervisorDegraded { handle } => BytecodeExpr::SupervisorDegraded {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::SupervisorClose { handle } => BytecodeExpr::SupervisorClose {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::FsReadAllU8 { path } => BytecodeExpr::FsReadAllU8 { path: path.clone() },
        LoweredExecExpr::FsWriteAllU8 { path, value } => BytecodeExpr::FsWriteAllU8 {
            path: path.clone(),
            value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::NetWriteAllU8 { host, port, value } => BytecodeExpr::NetWriteAllU8 {
            host: host.clone(),
            port: *port,
            value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::NetExchangeAllU8 { host, port, value } => BytecodeExpr::NetExchangeAllU8 {
            host: host.clone(),
            port: *port,
            value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::NetServeExchangeAllU8 {
            host,
            port,
            response,
        } => BytecodeExpr::NetServeExchangeAllU8 {
            host: host.clone(),
            port: *port,
            response: compile_bytecode_operand(response, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::NetListen { host, port } => BytecodeExpr::NetListen {
            host: host.clone(),
            port: *port,
        },
        LoweredExecExpr::TlsListen {
            host,
            port,
            cert,
            key,
            request_timeout_ms,
            session_timeout_ms,
            shutdown_grace_ms,
        } => BytecodeExpr::TlsListen {
            host: host.clone(),
            port: *port,
            cert: cert.clone(),
            key: key.clone(),
            request_timeout_ms: *request_timeout_ms,
            session_timeout_ms: *session_timeout_ms,
            shutdown_grace_ms: *shutdown_grace_ms,
        },
        LoweredExecExpr::NetAccept { listener } => BytecodeExpr::NetAccept {
            listener: compile_bytecode_operand(listener, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::NetSessionOpen { host, port } => BytecodeExpr::NetSessionOpen {
            host: host.clone(),
            port: *port,
        },
        LoweredExecExpr::HttpSessionAccept { listener } => BytecodeExpr::HttpSessionAccept {
            listener: compile_bytecode_operand(listener, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::NetReadAllU8 { handle } => BytecodeExpr::NetReadAllU8 {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::SessionReadChunkU8 { handle, chunk_size } => {
            BytecodeExpr::SessionReadChunkU8 {
                handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
                chunk_size: compile_bytecode_operand(
                    chunk_size,
                    slot_by_name,
                    slot_kind_by_name,
                )?,
            }
        }
        LoweredExecExpr::HttpSessionRequest { handle } => BytecodeExpr::HttpSessionRequest {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::NetWriteHandleAllU8 { handle, value } => {
            BytecodeExpr::NetWriteHandleAllU8 {
                handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
                value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
            }
        }
        LoweredExecExpr::SessionWriteChunkU8 { handle, value } => {
            BytecodeExpr::SessionWriteChunkU8 {
                handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
                value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
            }
        }
        LoweredExecExpr::SessionFlush { handle } => BytecodeExpr::SessionFlush {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::SessionAlive { handle } => BytecodeExpr::SessionAlive {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::SessionHeartbeatU8 { handle, value } => {
            BytecodeExpr::SessionHeartbeatU8 {
                handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
                value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
            }
        }
        LoweredExecExpr::SessionBackpressure { handle } => BytecodeExpr::SessionBackpressure {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::SessionBackpressureWait {
            handle,
            max_pending,
        } => BytecodeExpr::SessionBackpressureWait {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            max_pending: compile_bytecode_operand(max_pending, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::SessionResumeId { handle } => BytecodeExpr::SessionResumeId {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::SessionReconnect { handle } => BytecodeExpr::SessionReconnect {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::NetClose { handle } => BytecodeExpr::NetClose {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::HttpSessionClose { handle } => BytecodeExpr::HttpSessionClose {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::HttpMethodEq { request, method } => BytecodeExpr::HttpMethodEq {
            request: compile_bytecode_operand(request, slot_by_name, slot_kind_by_name)?,
            method: method.clone(),
        },
        LoweredExecExpr::HttpPathEq { request, path } => BytecodeExpr::HttpPathEq {
            request: compile_bytecode_operand(request, slot_by_name, slot_kind_by_name)?,
            path: path.clone(),
        },
        LoweredExecExpr::HttpRequestMethod { request } => BytecodeExpr::HttpRequestMethod {
            request: compile_bytecode_operand(request, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::HttpRequestPath { request } => BytecodeExpr::HttpRequestPath {
            request: compile_bytecode_operand(request, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::HttpRouteParam {
            request,
            pattern,
            param,
        } => BytecodeExpr::HttpRouteParam {
            request: compile_bytecode_operand(request, slot_by_name, slot_kind_by_name)?,
            pattern: pattern.clone(),
            param: param.clone(),
        },
        LoweredExecExpr::HttpHeaderEq {
            request,
            name,
            value,
        } => BytecodeExpr::HttpHeaderEq {
            request: compile_bytecode_operand(request, slot_by_name, slot_kind_by_name)?,
            name: name.clone(),
            value: value.clone(),
        },
        LoweredExecExpr::HttpCookieEq {
            request,
            name,
            value,
        } => BytecodeExpr::HttpCookieEq {
            request: compile_bytecode_operand(request, slot_by_name, slot_kind_by_name)?,
            name: name.clone(),
            value: value.clone(),
        },
        LoweredExecExpr::HttpStatusU32 { value } => BytecodeExpr::HttpStatusU32 {
            value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::BufLit { literal } => BytecodeExpr::BufLit {
            literal: literal.clone(),
        },
        LoweredExecExpr::BufConcat { left, right } => BytecodeExpr::BufConcat {
            left: compile_bytecode_operand(left, slot_by_name, slot_kind_by_name)?,
            right: compile_bytecode_operand(right, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::BufEqLit { value, literal } => BytecodeExpr::BufEqLit {
            value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
            literal: literal.clone(),
        },
        LoweredExecExpr::BufContainsLit { value, literal } => BytecodeExpr::BufContainsLit {
            value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
            literal: literal.clone(),
        },
        LoweredExecExpr::HttpHeader { request, name } => BytecodeExpr::HttpHeader {
            request: compile_bytecode_operand(request, slot_by_name, slot_kind_by_name)?,
            name: name.clone(),
        },
        LoweredExecExpr::HttpHeaderCount { request } => BytecodeExpr::HttpHeaderCount {
            request: compile_bytecode_operand(request, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::HttpHeaderName { request, index } => BytecodeExpr::HttpHeaderName {
            request: compile_bytecode_operand(request, slot_by_name, slot_kind_by_name)?,
            index: compile_bytecode_operand(index, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::HttpHeaderValue { request, index } => BytecodeExpr::HttpHeaderValue {
            request: compile_bytecode_operand(request, slot_by_name, slot_kind_by_name)?,
            index: compile_bytecode_operand(index, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::HttpCookie { request, name } => BytecodeExpr::HttpCookie {
            request: compile_bytecode_operand(request, slot_by_name, slot_kind_by_name)?,
            name: name.clone(),
        },
        LoweredExecExpr::HttpQueryParam { request, key } => BytecodeExpr::HttpQueryParam {
            request: compile_bytecode_operand(request, slot_by_name, slot_kind_by_name)?,
            key: key.clone(),
        },
        LoweredExecExpr::HttpBody { request } => BytecodeExpr::HttpBody {
            request: compile_bytecode_operand(request, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::HttpMultipartPartCount { request } => {
            BytecodeExpr::HttpMultipartPartCount {
                request: compile_bytecode_operand(request, slot_by_name, slot_kind_by_name)?,
            }
        }
        LoweredExecExpr::HttpMultipartPartName { request, index } => {
            BytecodeExpr::HttpMultipartPartName {
                request: compile_bytecode_operand(request, slot_by_name, slot_kind_by_name)?,
                index: compile_bytecode_operand(index, slot_by_name, slot_kind_by_name)?,
            }
        }
        LoweredExecExpr::HttpMultipartPartFilename { request, index } => {
            BytecodeExpr::HttpMultipartPartFilename {
                request: compile_bytecode_operand(request, slot_by_name, slot_kind_by_name)?,
                index: compile_bytecode_operand(index, slot_by_name, slot_kind_by_name)?,
            }
        }
        LoweredExecExpr::HttpMultipartPartBody { request, index } => {
            BytecodeExpr::HttpMultipartPartBody {
                request: compile_bytecode_operand(request, slot_by_name, slot_kind_by_name)?,
                index: compile_bytecode_operand(index, slot_by_name, slot_kind_by_name)?,
            }
        }
        LoweredExecExpr::HttpBodyLimit { request, limit } => BytecodeExpr::HttpBodyLimit {
            request: compile_bytecode_operand(request, slot_by_name, slot_kind_by_name)?,
            limit: compile_bytecode_operand(limit, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::HttpBodyStreamOpen { request } => BytecodeExpr::HttpBodyStreamOpen {
            request: compile_bytecode_operand(request, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::HttpBodyStreamNext { handle, chunk_size } => {
            BytecodeExpr::HttpBodyStreamNext {
                handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
                chunk_size: compile_bytecode_operand(chunk_size, slot_by_name, slot_kind_by_name)?,
            }
        }
        LoweredExecExpr::HttpBodyStreamClose { handle } => BytecodeExpr::HttpBodyStreamClose {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::HttpResponseStreamOpen {
            handle,
            status,
            content_type,
        } => BytecodeExpr::HttpResponseStreamOpen {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            status: compile_bytecode_operand(status, slot_by_name, slot_kind_by_name)?,
            content_type: content_type.clone(),
        },
        LoweredExecExpr::HttpResponseStreamWrite { handle, body } => {
            BytecodeExpr::HttpResponseStreamWrite {
                handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
                body: compile_bytecode_operand(body, slot_by_name, slot_kind_by_name)?,
            }
        }
        LoweredExecExpr::HttpResponseStreamClose { handle } => {
            BytecodeExpr::HttpResponseStreamClose {
                handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            }
        }
        LoweredExecExpr::HttpClientOpen { host, port } => BytecodeExpr::HttpClientOpen {
            host: host.clone(),
            port: *port,
        },
        LoweredExecExpr::HttpClientRequest { handle, request } => BytecodeExpr::HttpClientRequest {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            request: compile_bytecode_operand(request, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::HttpClientRequestRetry {
            handle,
            retries,
            backoff_ms,
            request,
        } => BytecodeExpr::HttpClientRequestRetry {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            retries: compile_bytecode_operand(retries, slot_by_name, slot_kind_by_name)?,
            backoff_ms: compile_bytecode_operand(backoff_ms, slot_by_name, slot_kind_by_name)?,
            request: compile_bytecode_operand(request, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::HttpClientClose { handle } => BytecodeExpr::HttpClientClose {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::HttpClientPoolOpen {
            host,
            port,
            max_size,
        } => BytecodeExpr::HttpClientPoolOpen {
            host: host.clone(),
            port: *port,
            max_size: compile_bytecode_operand(max_size, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::HttpClientPoolAcquire { pool } => BytecodeExpr::HttpClientPoolAcquire {
            pool: compile_bytecode_operand(pool, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::HttpClientPoolRelease { pool, handle } => {
            BytecodeExpr::HttpClientPoolRelease {
                pool: compile_bytecode_operand(pool, slot_by_name, slot_kind_by_name)?,
                handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            }
        }
        LoweredExecExpr::HttpClientPoolClose { pool } => BytecodeExpr::HttpClientPoolClose {
            pool: compile_bytecode_operand(pool, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::HttpServerConfigU32 { token } => BytecodeExpr::HttpServerConfigU32 {
            token: token.clone(),
        },
        LoweredExecExpr::MsgLogOpen => BytecodeExpr::MsgLogOpen,
        LoweredExecExpr::MsgLogClose { handle } => BytecodeExpr::MsgLogClose {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::MsgSend {
            handle,
            conversation,
            recipient,
            payload,
        } => BytecodeExpr::MsgSend {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            conversation: conversation.clone(),
            recipient: recipient.clone(),
            payload: compile_bytecode_operand(payload, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::MsgSendDedup {
            handle,
            conversation,
            recipient,
            dedup_key,
            payload,
        } => BytecodeExpr::MsgSendDedup {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            conversation: conversation.clone(),
            recipient: recipient.clone(),
            dedup_key: compile_bytecode_operand(dedup_key, slot_by_name, slot_kind_by_name)?,
            payload: compile_bytecode_operand(payload, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::MsgSubscribe {
            handle,
            room,
            recipient,
        } => BytecodeExpr::MsgSubscribe {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            room: room.clone(),
            recipient: recipient.clone(),
        },
        LoweredExecExpr::MsgSubscriberCount { handle, room } => BytecodeExpr::MsgSubscriberCount {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            room: room.clone(),
        },
        LoweredExecExpr::MsgFanout {
            handle,
            room,
            payload,
        } => BytecodeExpr::MsgFanout {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            room: room.clone(),
            payload: compile_bytecode_operand(payload, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::MsgRecvNext { handle, recipient } => BytecodeExpr::MsgRecvNext {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            recipient: recipient.clone(),
        },
        LoweredExecExpr::MsgRecvSeq { handle, recipient } => BytecodeExpr::MsgRecvSeq {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            recipient: recipient.clone(),
        },
        LoweredExecExpr::MsgAck {
            handle,
            recipient,
            seq,
        } => BytecodeExpr::MsgAck {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            recipient: recipient.clone(),
            seq: compile_bytecode_operand(seq, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::MsgMarkRetry {
            handle,
            recipient,
            seq,
        } => BytecodeExpr::MsgMarkRetry {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            recipient: recipient.clone(),
            seq: compile_bytecode_operand(seq, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::MsgRetryCount {
            handle,
            recipient,
            seq,
        } => BytecodeExpr::MsgRetryCount {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            recipient: recipient.clone(),
            seq: compile_bytecode_operand(seq, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::MsgPendingCount { handle, recipient } => BytecodeExpr::MsgPendingCount {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            recipient: recipient.clone(),
        },
        LoweredExecExpr::MsgDeliveryTotal { handle, recipient } => BytecodeExpr::MsgDeliveryTotal {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            recipient: recipient.clone(),
        },
        LoweredExecExpr::MsgFailureClass { handle } => BytecodeExpr::MsgFailureClass {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::MsgReplayOpen {
            handle,
            recipient,
            from_seq,
        } => BytecodeExpr::MsgReplayOpen {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            recipient: recipient.clone(),
            from_seq: compile_bytecode_operand(from_seq, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::MsgReplayNext { handle } => BytecodeExpr::MsgReplayNext {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::MsgReplaySeq { handle } => BytecodeExpr::MsgReplaySeq {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::MsgReplayClose { handle } => BytecodeExpr::MsgReplayClose {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::ServiceOpen { name } => BytecodeExpr::ServiceOpen { name: name.clone() },
        LoweredExecExpr::ServiceClose { handle } => BytecodeExpr::ServiceClose {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::ServiceShutdown { handle, grace_ms } => BytecodeExpr::ServiceShutdown {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            grace_ms: compile_bytecode_operand(grace_ms, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::ServiceLog {
            handle,
            level: _,
            message,
        } => BytecodeExpr::ServiceLog {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            message: compile_bytecode_operand(message, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::ServiceTraceBegin { handle, name } => BytecodeExpr::ServiceTraceBegin {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            name: name.clone(),
        },
        LoweredExecExpr::ServiceTraceEnd { trace } => BytecodeExpr::ServiceTraceEnd {
            trace: compile_bytecode_operand(trace, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::ServiceMetricCount {
            handle,
            metric: _,
            value,
        } => BytecodeExpr::ServiceMetricCount {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::ServiceMetricCountDim {
            handle,
            value,
            metric,
            dimension,
        } => BytecodeExpr::ServiceMetricCountDim {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
            metric: metric.clone(),
            dimension: dimension.clone(),
        },
        LoweredExecExpr::ServiceMetricTotal { handle, metric } => BytecodeExpr::ServiceMetricTotal {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            metric: metric.clone(),
        },
        LoweredExecExpr::ServiceHealthStatus { handle } => BytecodeExpr::ServiceHealthStatus {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::ServiceReadinessStatus { handle } => {
            BytecodeExpr::ServiceReadinessStatus {
                handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            }
        }
        LoweredExecExpr::ServiceSetHealth { handle, status } => BytecodeExpr::ServiceSetHealth {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            status: compile_bytecode_operand(status, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::ServiceSetReadiness { handle, status } => {
            BytecodeExpr::ServiceSetReadiness {
                handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
                status: compile_bytecode_operand(status, slot_by_name, slot_kind_by_name)?,
            }
        }
        LoweredExecExpr::ServiceSetDegraded { handle, degraded } => {
            BytecodeExpr::ServiceSetDegraded {
                handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
                degraded: compile_bytecode_operand(degraded, slot_by_name, slot_kind_by_name)?,
            }
        }
        LoweredExecExpr::ServiceDegraded { handle } => BytecodeExpr::ServiceDegraded {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::ServiceEvent {
            handle,
            class,
            message,
        } => BytecodeExpr::ServiceEvent {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            class: class.clone(),
            message: compile_bytecode_operand(message, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::ServiceEventTotal { handle, class } => BytecodeExpr::ServiceEventTotal {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            class: class.clone(),
        },
        LoweredExecExpr::ServiceTraceLink { trace, parent } => BytecodeExpr::ServiceTraceLink {
            trace: compile_bytecode_operand(trace, slot_by_name, slot_kind_by_name)?,
            parent: compile_bytecode_operand(parent, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::ServiceTraceLinkCount { handle } => BytecodeExpr::ServiceTraceLinkCount {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::ServiceFailureCount {
            handle,
            class,
            value,
        } => BytecodeExpr::ServiceFailureCount {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            class: class.clone(),
            value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::ServiceFailureTotal { handle, class } => {
            BytecodeExpr::ServiceFailureTotal {
                handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
                class: class.clone(),
            }
        }
        LoweredExecExpr::ServiceCheckpointSaveU32 { handle, key, value } => {
            BytecodeExpr::ServiceCheckpointSaveU32 {
                handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
                key: key.clone(),
                value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
            }
        }
        LoweredExecExpr::ServiceCheckpointLoadU32 { handle, key } => {
            BytecodeExpr::ServiceCheckpointLoadU32 {
                handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
                key: key.clone(),
            }
        }
        LoweredExecExpr::ServiceCheckpointExists { handle, key } => {
            BytecodeExpr::ServiceCheckpointExists {
                handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
                key: key.clone(),
            }
        }
        LoweredExecExpr::ServiceMigrateDb {
            handle,
            db_handle,
            migration: _,
        } => BytecodeExpr::ServiceMigrateDb {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            db_handle: compile_bytecode_operand(db_handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::ServiceRoute {
            request,
            method,
            path,
        } => BytecodeExpr::ServiceRoute {
            request: compile_bytecode_operand(request, slot_by_name, slot_kind_by_name)?,
            method: method.clone(),
            path: path.clone(),
        },
        LoweredExecExpr::ServiceRequireHeader {
            request,
            name,
            value,
        } => BytecodeExpr::ServiceRequireHeader {
            request: compile_bytecode_operand(request, slot_by_name, slot_kind_by_name)?,
            name: name.clone(),
            value: value.clone(),
        },
        LoweredExecExpr::ServiceErrorStatus { kind } => {
            BytecodeExpr::ServiceErrorStatus { kind: kind.clone() }
        }
        LoweredExecExpr::TlsServerConfigU32 { token: _, value } => {
            BytecodeExpr::TlsServerConfigU32 { value: *value }
        }
        LoweredExecExpr::TlsServerConfigBuf { token: _, value } => {
            BytecodeExpr::TlsServerConfigBuf {
                value: value.clone(),
            }
        }
        LoweredExecExpr::ListenerSetTimeoutMs { handle, value } => {
            BytecodeExpr::ListenerSetTimeoutMs {
                handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
                value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
            }
        }
        LoweredExecExpr::SessionSetTimeoutMs { handle, value } => {
            BytecodeExpr::SessionSetTimeoutMs {
                handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
                value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
            }
        }
        LoweredExecExpr::ListenerSetShutdownGraceMs { handle, value } => {
            BytecodeExpr::ListenerSetShutdownGraceMs {
                handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
                value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
            }
        }
        LoweredExecExpr::BufParseU32 { value } => BytecodeExpr::BufParseU32 {
            value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::BufParseBool { value } => BytecodeExpr::BufParseBool {
            value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::HttpWriteResponse {
            handle,
            status,
            body,
        } => BytecodeExpr::HttpWriteResponse {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            status: compile_bytecode_operand(status, slot_by_name, slot_kind_by_name)?,
            body: compile_bytecode_operand(body, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::HttpWriteTextResponse {
            handle,
            status,
            body,
        } => BytecodeExpr::HttpWriteTextResponse {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            status: compile_bytecode_operand(status, slot_by_name, slot_kind_by_name)?,
            body: compile_bytecode_operand(body, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::HttpWriteTextResponseCookie {
            handle,
            status,
            cookie_name,
            cookie_value,
            body,
        } => BytecodeExpr::HttpWriteTextResponseCookie {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            status: compile_bytecode_operand(status, slot_by_name, slot_kind_by_name)?,
            cookie_name: cookie_name.clone(),
            cookie_value: cookie_value.clone(),
            body: compile_bytecode_operand(body, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::HttpWriteTextResponseHeaders2 {
            handle,
            status,
            header1_name,
            header1_value,
            header2_name,
            header2_value,
            body,
        } => BytecodeExpr::HttpWriteTextResponseHeaders2 {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            status: compile_bytecode_operand(status, slot_by_name, slot_kind_by_name)?,
            header1_name: header1_name.clone(),
            header1_value: header1_value.clone(),
            header2_name: header2_name.clone(),
            header2_value: header2_value.clone(),
            body: compile_bytecode_operand(body, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::HttpSessionWriteText {
            handle,
            status,
            body,
        } => BytecodeExpr::HttpSessionWriteText {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            status: compile_bytecode_operand(status, slot_by_name, slot_kind_by_name)?,
            body: compile_bytecode_operand(body, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::HttpSessionWriteTextCookie {
            handle,
            status,
            cookie_name,
            cookie_value,
            body,
        } => BytecodeExpr::HttpSessionWriteTextCookie {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            status: compile_bytecode_operand(status, slot_by_name, slot_kind_by_name)?,
            cookie_name: cookie_name.clone(),
            cookie_value: cookie_value.clone(),
            body: compile_bytecode_operand(body, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::HttpSessionWriteTextHeaders2 {
            handle,
            status,
            header1_name,
            header1_value,
            header2_name,
            header2_value,
            body,
        } => BytecodeExpr::HttpSessionWriteTextHeaders2 {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            status: compile_bytecode_operand(status, slot_by_name, slot_kind_by_name)?,
            header1_name: header1_name.clone(),
            header1_value: header1_value.clone(),
            header2_name: header2_name.clone(),
            header2_value: header2_value.clone(),
            body: compile_bytecode_operand(body, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::HttpWriteJsonResponse {
            handle,
            status,
            body,
        } => BytecodeExpr::HttpWriteJsonResponse {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            status: compile_bytecode_operand(status, slot_by_name, slot_kind_by_name)?,
            body: compile_bytecode_operand(body, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::HttpWriteJsonResponseCookie {
            handle,
            status,
            cookie_name,
            cookie_value,
            body,
        } => BytecodeExpr::HttpWriteJsonResponseCookie {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            status: compile_bytecode_operand(status, slot_by_name, slot_kind_by_name)?,
            cookie_name: cookie_name.clone(),
            cookie_value: cookie_value.clone(),
            body: compile_bytecode_operand(body, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::HttpWriteJsonResponseHeaders2 {
            handle,
            status,
            header1_name,
            header1_value,
            header2_name,
            header2_value,
            body,
        } => BytecodeExpr::HttpWriteJsonResponseHeaders2 {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            status: compile_bytecode_operand(status, slot_by_name, slot_kind_by_name)?,
            header1_name: header1_name.clone(),
            header1_value: header1_value.clone(),
            header2_name: header2_name.clone(),
            header2_value: header2_value.clone(),
            body: compile_bytecode_operand(body, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::HttpSessionWriteJson {
            handle,
            status,
            body,
        } => BytecodeExpr::HttpSessionWriteJson {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            status: compile_bytecode_operand(status, slot_by_name, slot_kind_by_name)?,
            body: compile_bytecode_operand(body, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::HttpSessionWriteJsonCookie {
            handle,
            status,
            cookie_name,
            cookie_value,
            body,
        } => BytecodeExpr::HttpSessionWriteJsonCookie {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            status: compile_bytecode_operand(status, slot_by_name, slot_kind_by_name)?,
            cookie_name: cookie_name.clone(),
            cookie_value: cookie_value.clone(),
            body: compile_bytecode_operand(body, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::HttpSessionWriteJsonHeaders2 {
            handle,
            status,
            header1_name,
            header1_value,
            header2_name,
            header2_value,
            body,
        } => BytecodeExpr::HttpSessionWriteJsonHeaders2 {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            status: compile_bytecode_operand(status, slot_by_name, slot_kind_by_name)?,
            header1_name: header1_name.clone(),
            header1_value: header1_value.clone(),
            header2_name: header2_name.clone(),
            header2_value: header2_value.clone(),
            body: compile_bytecode_operand(body, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::HttpWriteResponseHeader {
            handle,
            status,
            header_name,
            header_value,
            body,
        } => BytecodeExpr::HttpWriteResponseHeader {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            status: compile_bytecode_operand(status, slot_by_name, slot_kind_by_name)?,
            header_name: header_name.clone(),
            header_value: header_value.clone(),
            body: compile_bytecode_operand(body, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::JsonGetU32 { value, key } => BytecodeExpr::JsonGetU32 {
            value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
            key: key.clone(),
        },
        LoweredExecExpr::JsonGetBool { value, key } => BytecodeExpr::JsonGetBool {
            value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
            key: key.clone(),
        },
        LoweredExecExpr::JsonHasKey { value, key } => BytecodeExpr::JsonHasKey {
            value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
            key: key.clone(),
        },
        LoweredExecExpr::JsonGetBufU8 { value, key } => BytecodeExpr::JsonGetBufU8 {
            value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
            key: key.clone(),
        },
        LoweredExecExpr::JsonGetStr { value, key } => BytecodeExpr::JsonGetStr {
            value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
            key: key.clone(),
        },
        LoweredExecExpr::JsonGetU32Or {
            value,
            key,
            default_value,
        } => BytecodeExpr::JsonGetU32Or {
            value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
            key: key.clone(),
            default_value: compile_bytecode_operand(
                default_value,
                slot_by_name,
                slot_kind_by_name,
            )?,
        },
        LoweredExecExpr::JsonGetBoolOr {
            value,
            key,
            default_value,
        } => BytecodeExpr::JsonGetBoolOr {
            value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
            key: key.clone(),
            default_value: compile_bytecode_operand(
                default_value,
                slot_by_name,
                slot_kind_by_name,
            )?,
        },
        LoweredExecExpr::JsonGetBufOr {
            value,
            key,
            default_value,
        } => BytecodeExpr::JsonGetBufOr {
            value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
            key: key.clone(),
            default_value: compile_bytecode_operand(
                default_value,
                slot_by_name,
                slot_kind_by_name,
            )?,
        },
        LoweredExecExpr::JsonGetStrOr {
            value,
            key,
            default_value,
        } => BytecodeExpr::JsonGetStrOr {
            value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
            key: key.clone(),
            default_value: compile_bytecode_operand(
                default_value,
                slot_by_name,
                slot_kind_by_name,
            )?,
        },
        LoweredExecExpr::JsonArrayLen { value } => BytecodeExpr::JsonArrayLen {
            value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::JsonIndexU32 { value, index } => BytecodeExpr::JsonIndexU32 {
            value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
            index: compile_bytecode_operand(index, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::JsonIndexBool { value, index } => BytecodeExpr::JsonIndexBool {
            value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
            index: compile_bytecode_operand(index, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::JsonIndexStr { value, index } => BytecodeExpr::JsonIndexStr {
            value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
            index: compile_bytecode_operand(index, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::JsonEncodeObj { entries } => BytecodeExpr::JsonEncodeObj {
            entries: entries
                .iter()
                .map(|(key, operand)| {
                    Ok((
                        key.clone(),
                        compile_bytecode_operand(operand, slot_by_name, slot_kind_by_name)?,
                    ))
                })
                .collect::<Result<Vec<_>, String>>()?,
        },
        LoweredExecExpr::JsonEncodeArr { values } => BytecodeExpr::JsonEncodeArr {
            values: values
                .iter()
                .map(|operand| compile_bytecode_operand(operand, slot_by_name, slot_kind_by_name))
                .collect::<Result<Vec<_>, String>>()?,
        },
        LoweredExecExpr::StrLit { literal } => BytecodeExpr::StrLit {
            literal: literal.clone(),
        },
        LoweredExecExpr::StrConcat { left, right } => BytecodeExpr::StrConcat {
            left: compile_bytecode_operand(left, slot_by_name, slot_kind_by_name)?,
            right: compile_bytecode_operand(right, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::StrFromU32 { value } => BytecodeExpr::StrFromU32 {
            value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::StrFromBool { value } => BytecodeExpr::StrFromBool {
            value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::StrEqLit { value, literal } => BytecodeExpr::StrEqLit {
            value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
            literal: literal.clone(),
        },
        LoweredExecExpr::StrToBuf { value } => BytecodeExpr::StrToBuf {
            value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::BufToStr { value } => BytecodeExpr::BufToStr {
            value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::BufHexStr { value } => BytecodeExpr::BufHexStr {
            value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::ConfigGetU32 { key: _, value } => {
            BytecodeExpr::ConfigGetU32 { value: *value }
        }
        LoweredExecExpr::ConfigGetBool { key: _, value } => {
            BytecodeExpr::ConfigGetBool { value: *value }
        }
        LoweredExecExpr::ConfigGetStr { key: _, value } => BytecodeExpr::ConfigGetStr {
            value: value.clone(),
        },
        LoweredExecExpr::ConfigHas { key: _, present } => BytecodeExpr::ConfigHas {
            present: *present,
        },
        LoweredExecExpr::EnvGetU32 { key } => BytecodeExpr::EnvGetU32 { key: key.clone() },
        LoweredExecExpr::EnvGetBool { key } => BytecodeExpr::EnvGetBool { key: key.clone() },
        LoweredExecExpr::EnvGetStr { key } => BytecodeExpr::EnvGetStr { key: key.clone() },
        LoweredExecExpr::EnvHas { key } => BytecodeExpr::EnvHas { key: key.clone() },
        LoweredExecExpr::BufBeforeLit { value, literal } => BytecodeExpr::BufBeforeLit {
            value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
            literal: literal.clone(),
        },
        LoweredExecExpr::BufAfterLit { value, literal } => BytecodeExpr::BufAfterLit {
            value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
            literal: literal.clone(),
        },
        LoweredExecExpr::BufTrimAscii { value } => BytecodeExpr::BufTrimAscii {
            value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::DateParseYmd { value } => BytecodeExpr::DateParseYmd {
            value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::TimeParseHms { value } => BytecodeExpr::TimeParseHms {
            value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::DateFormatYmd { value } => BytecodeExpr::DateFormatYmd {
            value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::TimeFormatHms { value } => BytecodeExpr::TimeFormatHms {
            value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::DbOpen { path } => BytecodeExpr::DbOpen { path: path.clone() },
        LoweredExecExpr::DbClose { handle } => BytecodeExpr::DbClose {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::DbExec { handle, sql } => BytecodeExpr::DbExec {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            sql: compile_bytecode_operand(sql, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::DbPrepare { handle, name, sql } => BytecodeExpr::DbPrepare {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            name: name.clone(),
            sql: compile_bytecode_operand(sql, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::DbExecPrepared {
            handle,
            name,
            params,
        } => BytecodeExpr::DbExecPrepared {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            name: name.clone(),
            params: compile_bytecode_operand(params, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::DbQueryU32 { handle, sql } => BytecodeExpr::DbQueryU32 {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            sql: compile_bytecode_operand(sql, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::DbQueryBufU8 { handle, sql } => BytecodeExpr::DbQueryBufU8 {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            sql: compile_bytecode_operand(sql, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::DbQueryRow { handle, sql } => BytecodeExpr::DbQueryRow {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            sql: compile_bytecode_operand(sql, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::DbQueryPreparedU32 {
            handle,
            name,
            params,
        } => BytecodeExpr::DbQueryPreparedU32 {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            name: name.clone(),
            params: compile_bytecode_operand(params, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::DbQueryPreparedBufU8 {
            handle,
            name,
            params,
        } => BytecodeExpr::DbQueryPreparedBufU8 {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            name: name.clone(),
            params: compile_bytecode_operand(params, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::DbQueryPreparedRow {
            handle,
            name,
            params,
        } => BytecodeExpr::DbQueryPreparedRow {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            name: name.clone(),
            params: compile_bytecode_operand(params, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::DbRowFound { row } => BytecodeExpr::DbRowFound {
            row: compile_bytecode_operand(row, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::DbLastErrorCode { handle } => BytecodeExpr::DbLastErrorCode {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::DbLastErrorRetryable { handle } => {
            BytecodeExpr::DbLastErrorRetryable {
                handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            }
        }
        LoweredExecExpr::DbBegin { handle } => BytecodeExpr::DbBegin {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::DbCommit { handle } => BytecodeExpr::DbCommit {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::DbRollback { handle } => BytecodeExpr::DbRollback {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::DbPoolOpen { target, max_size } => BytecodeExpr::DbPoolOpen {
            target: target.clone(),
            max_size: compile_bytecode_operand(max_size, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::DbPoolSetMaxIdle { pool, value } => BytecodeExpr::DbPoolSetMaxIdle {
            pool: compile_bytecode_operand(pool, slot_by_name, slot_kind_by_name)?,
            value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::DbPoolLeased { pool } => BytecodeExpr::DbPoolLeased {
            pool: compile_bytecode_operand(pool, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::DbPoolAcquire { pool } => BytecodeExpr::DbPoolAcquire {
            pool: compile_bytecode_operand(pool, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::DbPoolRelease { pool, handle } => BytecodeExpr::DbPoolRelease {
            pool: compile_bytecode_operand(pool, slot_by_name, slot_kind_by_name)?,
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::DbPoolClose { pool } => BytecodeExpr::DbPoolClose {
            pool: compile_bytecode_operand(pool, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::CacheOpen { target } => BytecodeExpr::CacheOpen {
            target: target.clone(),
        },
        LoweredExecExpr::CacheClose { handle } => BytecodeExpr::CacheClose {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::CacheGetBufU8 { handle, key } => BytecodeExpr::CacheGetBufU8 {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            key: compile_bytecode_operand(key, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::CacheSetBufU8 { handle, key, value } => BytecodeExpr::CacheSetBufU8 {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            key: compile_bytecode_operand(key, slot_by_name, slot_kind_by_name)?,
            value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::CacheSetBufTtlU8 {
            handle,
            key,
            ttl_ms,
            value,
        } => BytecodeExpr::CacheSetBufTtlU8 {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            key: compile_bytecode_operand(key, slot_by_name, slot_kind_by_name)?,
            ttl_ms: compile_bytecode_operand(ttl_ms, slot_by_name, slot_kind_by_name)?,
            value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::CacheDel { handle, key } => BytecodeExpr::CacheDel {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            key: compile_bytecode_operand(key, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::QueueOpen { target } => BytecodeExpr::QueueOpen {
            target: target.clone(),
        },
        LoweredExecExpr::QueueClose { handle } => BytecodeExpr::QueueClose {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::QueuePushBufU8 { handle, value } => BytecodeExpr::QueuePushBufU8 {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::QueuePopBufU8 { handle } => BytecodeExpr::QueuePopBufU8 {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::QueueLen { handle } => BytecodeExpr::QueueLen {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::StreamOpen { target } => BytecodeExpr::StreamOpen {
            target: target.clone(),
        },
        LoweredExecExpr::StreamClose { handle } => BytecodeExpr::StreamClose {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::StreamPublishBufU8 { handle, value } => BytecodeExpr::StreamPublishBufU8 {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::StreamLen { handle } => BytecodeExpr::StreamLen {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::StreamReplayOpen {
            handle,
            from_offset,
        } => BytecodeExpr::StreamReplayOpen {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            offset: compile_bytecode_operand(from_offset, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::StreamReplayNextU8 { handle } => BytecodeExpr::StreamReplayNextU8 {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::StreamReplayOffset { handle } => BytecodeExpr::StreamReplayOffset {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::StreamReplayClose { handle } => BytecodeExpr::StreamReplayClose {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::ShardRouteU32 { key, shard_count } => BytecodeExpr::ShardRouteU32 {
            key: compile_bytecode_operand(key, slot_by_name, slot_kind_by_name)?,
            shard_count: compile_bytecode_operand(
                shard_count,
                slot_by_name,
                slot_kind_by_name,
            )?,
        },
        LoweredExecExpr::LeaseOpen { target } => BytecodeExpr::LeaseOpen {
            target: target.clone(),
        },
        LoweredExecExpr::LeaseAcquire { handle, owner } => BytecodeExpr::LeaseAcquire {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            owner: compile_bytecode_operand(owner, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::LeaseOwner { handle } => BytecodeExpr::LeaseOwner {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::LeaseTransfer { handle, owner } => BytecodeExpr::LeaseTransfer {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            owner: compile_bytecode_operand(owner, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::LeaseRelease { handle, owner } => BytecodeExpr::LeaseRelease {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            owner: compile_bytecode_operand(owner, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::LeaseClose { handle } => BytecodeExpr::LeaseClose {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::PlacementOpen { target } => BytecodeExpr::PlacementOpen {
            target: target.clone(),
        },
        LoweredExecExpr::PlacementAssign {
            handle,
            shard,
            node,
        } => BytecodeExpr::PlacementAssign {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            shard: compile_bytecode_operand(shard, slot_by_name, slot_kind_by_name)?,
            node: compile_bytecode_operand(node, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::PlacementLookup { handle, shard } => BytecodeExpr::PlacementLookup {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            shard: compile_bytecode_operand(shard, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::PlacementClose { handle } => BytecodeExpr::PlacementClose {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::CoordOpen { target } => BytecodeExpr::CoordOpen {
            target: target.clone(),
        },
        LoweredExecExpr::CoordStoreU32 { handle, key, value } => BytecodeExpr::CoordStoreU32 {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            key: key.clone(),
            value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::CoordLoadU32 { handle, key } => BytecodeExpr::CoordLoadU32 {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            key: key.clone(),
        },
        LoweredExecExpr::CoordClose { handle } => BytecodeExpr::CoordClose {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::BatchOpen => BytecodeExpr::BatchOpen,
        LoweredExecExpr::BatchPushU64 { handle, value } => BytecodeExpr::BatchPushU64 {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::BatchLen { handle } => BytecodeExpr::BatchLen {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::BatchFlushSumU64 { handle } => BytecodeExpr::BatchFlushSumU64 {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::BatchClose { handle } => BytecodeExpr::BatchClose {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::AggOpenU64 => BytecodeExpr::AggOpenU64,
        LoweredExecExpr::AggAddU64 { handle, value } => BytecodeExpr::AggAddU64 {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::AggCount { handle } => BytecodeExpr::AggCount {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::AggSumU64 { handle } => BytecodeExpr::AggSumU64 {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::AggAvgU64 { handle } => BytecodeExpr::AggAvgU64 {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::AggMinU64 { handle } => BytecodeExpr::AggMinU64 {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::AggMaxU64 { handle } => BytecodeExpr::AggMaxU64 {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::AggClose { handle } => BytecodeExpr::AggClose {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::WindowOpenMs { width_ms } => BytecodeExpr::WindowOpenMs {
            width_ms: compile_bytecode_operand(width_ms, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::WindowAddU64 { handle, value } => BytecodeExpr::WindowAddU64 {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::WindowCount { handle } => BytecodeExpr::WindowCount {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::WindowSumU64 { handle } => BytecodeExpr::WindowSumU64 {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::WindowAvgU64 { handle } => BytecodeExpr::WindowAvgU64 {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::WindowMinU64 { handle } => BytecodeExpr::WindowMinU64 {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::WindowMaxU64 { handle } => BytecodeExpr::WindowMaxU64 {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::WindowClose { handle } => BytecodeExpr::WindowClose {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::TlsExchangeAllU8 { host, port, value } => BytecodeExpr::TlsExchangeAllU8 {
            host: host.clone(),
            port: *port,
            value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::TaskSleepMs { value } => BytecodeExpr::TaskSleepMs {
            value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::TaskOpen { command, argv, env } => BytecodeExpr::TaskOpen {
            command: command.clone(),
            argv: argv.clone(),
            env: env.clone(),
        },
        LoweredExecExpr::TaskDone { handle } => BytecodeExpr::TaskDone {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::TaskJoinStatus { handle } => BytecodeExpr::TaskJoinStatus {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::TaskStdoutAllU8 { handle } => BytecodeExpr::TaskStdoutAllU8 {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::TaskStderrAllU8 { handle } => BytecodeExpr::TaskStderrAllU8 {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::TaskClose { handle } => BytecodeExpr::TaskClose {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::SpawnCaptureAllU8 { command, argv, env } => {
            BytecodeExpr::SpawnCaptureAllU8 {
                command: command.clone(),
                argv: argv.clone(),
                env: env.clone(),
            }
        }
        LoweredExecExpr::SpawnCaptureStderrAllU8 { command, argv, env } => {
            BytecodeExpr::SpawnCaptureStderrAllU8 {
                command: command.clone(),
                argv: argv.clone(),
                env: env.clone(),
            }
        }
        LoweredExecExpr::SpawnCall { command, argv, env } => BytecodeExpr::SpawnCall {
            command: command.clone(),
            argv: argv.clone(),
            env: env.clone(),
        },
        LoweredExecExpr::SpawnOpen { command, argv, env } => BytecodeExpr::SpawnOpen {
            command: command.clone(),
            argv: argv.clone(),
            env: env.clone(),
        },
        LoweredExecExpr::SpawnWait { handle } => BytecodeExpr::SpawnWait {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::SpawnStdoutAllU8 { handle } => BytecodeExpr::SpawnStdoutAllU8 {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::SpawnStderrAllU8 { handle } => BytecodeExpr::SpawnStderrAllU8 {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::SpawnStdinWriteAllU8 { handle, value } => {
            BytecodeExpr::SpawnStdinWriteAllU8 {
                handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
                value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
            }
        }
        LoweredExecExpr::SpawnStdinClose { handle } => BytecodeExpr::SpawnStdinClose {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::SpawnDone { handle } => BytecodeExpr::SpawnDone {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::SpawnExitOk { handle } => BytecodeExpr::SpawnExitOk {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::SpawnKill { handle } => BytecodeExpr::SpawnKill {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::SpawnClose { handle } => BytecodeExpr::SpawnClose {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::NetConnect { host, port } => BytecodeExpr::NetConnect {
            host: host.clone(),
            port: *port,
        },
        LoweredExecExpr::FfiCall {
            symbol,
            args,
            ret_c_type,
        } => BytecodeExpr::FfiCall {
            symbol: symbol.clone(),
            args: args
                .iter()
                .map(|operand| compile_bytecode_operand(operand, slot_by_name, slot_kind_by_name))
                .collect::<Result<Vec<_>, _>>()?,
            ret_kind: bytecode_kind_for_c_type(ret_c_type)?,
        },
        LoweredExecExpr::FfiCallCStr {
            symbol,
            arg,
            ret_c_type,
        } => BytecodeExpr::FfiCallCStr {
            symbol: symbol.clone(),
            arg_slot: *slot_by_name
                .get(arg)
                .ok_or_else(|| format!("missing ffi_call_cstr slot for {arg}"))?,
            ret_kind: bytecode_kind_for_c_type(ret_c_type)?,
        },
        LoweredExecExpr::FfiOpenLib { path } => BytecodeExpr::FfiOpenLib { path: path.clone() },
        LoweredExecExpr::FfiCloseLib { handle } => BytecodeExpr::FfiCloseLib {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::FfiBufPtr { value } => BytecodeExpr::FfiBufPtr {
            value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::FfiCallLib {
            handle,
            symbol,
            args,
            ret_c_type,
        } => BytecodeExpr::FfiCallLib {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            symbol: symbol.clone(),
            args: args
                .iter()
                .map(|operand| compile_bytecode_operand(operand, slot_by_name, slot_kind_by_name))
                .collect::<Result<Vec<_>, _>>()?,
            ret_kind: bytecode_kind_for_c_type(ret_c_type)?,
        },
        LoweredExecExpr::FfiCallLibCStr {
            handle,
            symbol,
            arg,
            ret_c_type,
        } => BytecodeExpr::FfiCallLibCStr {
            handle: compile_bytecode_operand(handle, slot_by_name, slot_kind_by_name)?,
            symbol: symbol.clone(),
            arg_slot: *slot_by_name
                .get(arg)
                .ok_or_else(|| format!("missing ffi_call_lib_cstr slot for {arg}"))?,
            ret_kind: bytecode_kind_for_c_type(ret_c_type)?,
        },
        LoweredExecExpr::Len { source } => {
            let source_slot = *slot_by_name
                .get(source)
                .ok_or_else(|| format!("unknown bytecode source {}", source))?;
            match slot_kind_by_name
                .get(source)
                .ok_or_else(|| format!("unknown bytecode source kind {}", source))?
            {
                BytecodeValueKind::SpanI32 => BytecodeExpr::LenSpanI32 {
                    source: source_slot,
                },
                BytecodeValueKind::BufU8 => BytecodeExpr::LenBufU8 {
                    source: source_slot,
                },
                other => {
                    return Err(format!("unsupported bytecode len source kind {:?}", other));
                }
            }
        }
        LoweredExecExpr::StoreBufU8 {
            source,
            index,
            value,
        } => BytecodeExpr::StoreBufU8 {
            source: *slot_by_name
                .get(source)
                .ok_or_else(|| format!("unknown bytecode source {}", source))?,
            index: compile_bytecode_operand(index, slot_by_name, slot_kind_by_name)?,
            value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::LoadU8 { source, index } => BytecodeExpr::LoadBufU8 {
            source: *slot_by_name
                .get(source)
                .ok_or_else(|| format!("unknown bytecode source {}", source))?,
            index: compile_bytecode_operand(index, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::LoadI32 { source, index } => BytecodeExpr::LoadSpanI32 {
            source: *slot_by_name
                .get(source)
                .ok_or_else(|| format!("unknown bytecode source {}", source))?,
            index: compile_bytecode_operand(index, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::AbsI32 { value } => BytecodeExpr::AbsI32 {
            value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::Binary { op, left, right } => BytecodeExpr::Binary {
            op: op.clone(),
            left: compile_bytecode_operand(left, slot_by_name, slot_kind_by_name)?,
            right: compile_bytecode_operand(right, slot_by_name, slot_kind_by_name)?,
        },
        LoweredExecExpr::SextI64 { value } => BytecodeExpr::SextI64 {
            value: compile_bytecode_operand(value, slot_by_name, slot_kind_by_name)?,
        },
    })
}

fn compile_bytecode_operand(
    operand: &LoweredExecOperand,
    slot_by_name: &HashMap<String, usize>,
    slot_kind_by_name: &HashMap<String, BytecodeValueKind>,
) -> Result<BytecodeOperand, String> {
    Ok(match operand {
        LoweredExecOperand::Binding(name) => BytecodeOperand::Slot {
            index: *slot_by_name
                .get(name)
                .ok_or_else(|| format!("unknown bytecode binding {}", name))?,
            kind: *slot_kind_by_name
                .get(name)
                .ok_or_else(|| format!("unknown bytecode binding kind {}", name))?,
        },
        LoweredExecOperand::Immediate(value) => BytecodeOperand::Imm(match value {
            LoweredExecImmediate::U8(value) => BytecodeImmediate::U8(*value),
            LoweredExecImmediate::I32(value) => BytecodeImmediate::I32(*value),
            LoweredExecImmediate::I64(value) => BytecodeImmediate::I64(*value),
            LoweredExecImmediate::U64(value) => BytecodeImmediate::U64(*value),
            LoweredExecImmediate::U32(value) => BytecodeImmediate::U32(*value),
            LoweredExecImmediate::Bool(value) => BytecodeImmediate::Bool(*value),
        }),
    })
}

fn apply_bytecode_edge(
    edge: &BytecodeEdge,
    slots: &mut [Option<RuntimeValue>],
    rand_state: &mut Option<u32>,
) -> Result<(), String> {
    let mut values = Vec::new();
    for instruction in &edge.moves {
        values.push((
            instruction.dst,
            eval_bytecode_expr(&instruction.expr, slots, rand_state)?,
        ));
    }
    for (dst, value) in values {
        slots[dst] = Some(value);
    }
    Ok(())
}

fn eval_bytecode_expr(
    expr: &BytecodeExpr,
    slots: &[Option<RuntimeValue>],
    rand_state: &mut Option<u32>,
) -> Result<RuntimeValue, String> {
    match expr {
        BytecodeExpr::Move(operand) => eval_bytecode_operand(operand, slots),
        BytecodeExpr::AllocBufU8 { len } => {
            let len = match eval_bytecode_operand(len, slots)? {
                RuntimeValue::U32(value) => value as usize,
                other => return Err(format!("bytecode alloc buf[u8] expects u32, got {other:?}")),
            };
            Ok(RuntimeValue::BufU8(vec![0u8; len]))
        }
        BytecodeExpr::DropBufU8 { value } => match eval_bytecode_operand(value, slots)? {
            RuntimeValue::BufU8(_) => Ok(RuntimeValue::Bool(true)),
            other => Err(format!("bytecode drop buf[u8] expects buf, got {other:?}")),
        },
        BytecodeExpr::ClockNowNs => Ok(RuntimeValue::U64(mira_clock_now_ns())),
        BytecodeExpr::RandU32 => Ok(RuntimeValue::U32(mira_rand_next_u32(rand_state))),
        BytecodeExpr::FsReadU32 { path } => Ok(RuntimeValue::U32(portable_fs_read_u32(path)?)),
        BytecodeExpr::FsWriteU32 { path, value } => {
            let value = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "bytecode fs_write_u32 expects u32 operand, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_fs_write_u32(path, value)?))
        }
        BytecodeExpr::RtOpen { workers } => {
            let workers = match eval_bytecode_operand(workers, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "bytecode rt_open expects u32 worker count, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U64(portable_rt_open(workers)?))
        }
        BytecodeExpr::RtSpawnU32 {
            runtime,
            function,
            arg,
        } => {
            let runtime = match eval_bytecode_operand(runtime, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode rt_spawn_u32 expects u64 runtime handle, got {other:?}"
                    ))
                }
            };
            let arg = match eval_bytecode_operand(arg, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "bytecode rt_spawn_u32 expects u32 arg, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U64(portable_rt_spawn_u32(
                runtime, function, arg,
            )?))
        }
        BytecodeExpr::RtSpawnBufU8 {
            runtime,
            function,
            arg,
        } => {
            let runtime = match eval_bytecode_operand(runtime, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode rt_spawn_buf expects u64 runtime handle, got {other:?}"
                    ))
                }
            };
            let arg = match eval_bytecode_operand(arg, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode rt_spawn_buf expects buf[u8] arg, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U64(portable_rt_spawn_buf(
                runtime, function, arg,
            )?))
        }
        BytecodeExpr::RtTrySpawnU32 {
            runtime,
            function,
            arg,
        } => {
            let runtime = match eval_bytecode_operand(runtime, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode rt_try_spawn_u32 expects u64 runtime handle, got {other:?}"
                    ))
                }
            };
            let arg = match eval_bytecode_operand(arg, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "bytecode rt_try_spawn_u32 expects u32 arg, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U64(portable_rt_try_spawn_u32(
                runtime, function, arg,
            )?))
        }
        BytecodeExpr::RtTrySpawnBufU8 {
            runtime,
            function,
            arg,
        } => {
            let runtime = match eval_bytecode_operand(runtime, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode rt_try_spawn_buf expects u64 runtime handle, got {other:?}"
                    ))
                }
            };
            let arg = match eval_bytecode_operand(arg, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode rt_try_spawn_buf expects buf[u8] arg, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U64(portable_rt_try_spawn_buf(
                runtime, function, arg,
            )?))
        }
        BytecodeExpr::RtDone { task } => {
            let task = match eval_bytecode_operand(task, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode rt_done expects u64 task handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_rt_done(task)?))
        }
        BytecodeExpr::RtJoinU32 { task } => {
            let task = match eval_bytecode_operand(task, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode rt_join_u32 expects u64 task handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U32(portable_rt_join_u32(task)?))
        }
        BytecodeExpr::RtJoinBufU8 { task } => {
            let task = match eval_bytecode_operand(task, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode rt_join_buf expects u64 task handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::BufU8(portable_rt_join_buf(task)?))
        }
        BytecodeExpr::RtCancel { task } => {
            let task = match eval_bytecode_operand(task, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode rt_cancel expects u64 task handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_rt_cancel(task)?))
        }
        BytecodeExpr::RtTaskClose { task } => {
            let task = match eval_bytecode_operand(task, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode rt_task_close expects u64 task handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_rt_task_close(task)?))
        }
        BytecodeExpr::RtShutdown { runtime, grace_ms } => {
            let runtime = match eval_bytecode_operand(runtime, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode rt_shutdown expects u64 runtime handle, got {other:?}"
                    ))
                }
            };
            let grace_ms = match eval_bytecode_operand(grace_ms, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "bytecode rt_shutdown expects u32 grace_ms, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_rt_shutdown(runtime, grace_ms)?))
        }
        BytecodeExpr::RtClose { runtime } => {
            let runtime = match eval_bytecode_operand(runtime, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode rt_close expects u64 runtime handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_rt_close(runtime)?))
        }
        BytecodeExpr::RtInFlight { runtime } => {
            let runtime = match eval_bytecode_operand(runtime, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode rt_inflight expects u64 runtime handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U32(portable_rt_inflight(runtime)?))
        }
        BytecodeExpr::RtCancelled => Ok(RuntimeValue::Bool(portable_rt_cancelled())),
        BytecodeExpr::ChanOpenU32 { capacity } => {
            let capacity = match eval_bytecode_operand(capacity, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "bytecode chan_open_u32 expects u32 capacity, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U64(portable_chan_open_u32(capacity)?))
        }
        BytecodeExpr::ChanOpenBufU8 { capacity } => {
            let capacity = match eval_bytecode_operand(capacity, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "bytecode chan_open_buf expects u32 capacity, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U64(portable_chan_open_buf(capacity)?))
        }
        BytecodeExpr::ChanSendU32 { channel, value } => {
            let channel = match eval_bytecode_operand(channel, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode chan_send_u32 expects u64 channel handle, got {other:?}"
                    ))
                }
            };
            let value = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "bytecode chan_send_u32 expects u32 value, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_chan_send_u32(channel, value)?))
        }
        BytecodeExpr::ChanSendBufU8 { channel, value } => {
            let channel = match eval_bytecode_operand(channel, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode chan_send_buf expects u64 channel handle, got {other:?}"
                    ))
                }
            };
            let value = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode chan_send_buf expects buf[u8] value, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_chan_send_buf(channel, value)?))
        }
        BytecodeExpr::ChanRecvU32 { channel } => {
            let channel = match eval_bytecode_operand(channel, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode chan_recv_u32 expects u64 channel handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U32(portable_chan_recv_u32(channel)?))
        }
        BytecodeExpr::ChanRecvBufU8 { channel } => {
            let channel = match eval_bytecode_operand(channel, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode chan_recv_buf expects u64 channel handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::BufU8(portable_chan_recv_buf(channel)?))
        }
        BytecodeExpr::ChanLen { channel } => {
            let channel = match eval_bytecode_operand(channel, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode chan_len expects u64 channel handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U32(portable_chan_len(channel)?))
        }
        BytecodeExpr::ChanClose { channel } => {
            let channel = match eval_bytecode_operand(channel, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode chan_close expects u64 channel handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_chan_close(channel)?))
        }
        BytecodeExpr::DeadlineOpenMs { timeout_ms } => {
            let timeout_ms = match eval_bytecode_operand(timeout_ms, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "bytecode deadline_open_ms expects u32 timeout, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U64(portable_deadline_open_ms(timeout_ms)?))
        }
        BytecodeExpr::DeadlineExpired { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode deadline_expired expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_deadline_expired(handle)?))
        }
        BytecodeExpr::DeadlineRemainingMs { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode deadline_remaining_ms expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U32(portable_deadline_remaining_ms(handle)?))
        }
        BytecodeExpr::DeadlineClose { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode deadline_close expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_deadline_close(handle)?))
        }
        BytecodeExpr::CancelScopeOpen => Ok(RuntimeValue::U64(portable_cancel_scope_open()?)),
        BytecodeExpr::CancelScopeChild { parent } => {
            let parent = match eval_bytecode_operand(parent, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode cancel_scope_child expects u64 parent handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U64(portable_cancel_scope_child(parent)?))
        }
        BytecodeExpr::CancelScopeBindTask { scope, task } => {
            let scope = match eval_bytecode_operand(scope, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode cancel_scope_bind_task expects u64 scope handle, got {other:?}"
                    ))
                }
            };
            let task = match eval_bytecode_operand(task, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode cancel_scope_bind_task expects u64 task handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_cancel_scope_bind_task(scope, task)?))
        }
        BytecodeExpr::CancelScopeCancel { scope } => {
            let scope = match eval_bytecode_operand(scope, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode cancel_scope_cancel expects u64 scope handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_cancel_scope_cancel(scope)?))
        }
        BytecodeExpr::CancelScopeCancelled { scope } => {
            let scope = match eval_bytecode_operand(scope, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode cancel_scope_cancelled expects u64 scope handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_cancel_scope_cancelled(scope)?))
        }
        BytecodeExpr::CancelScopeClose { scope } => {
            let scope = match eval_bytecode_operand(scope, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode cancel_scope_close expects u64 scope handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_cancel_scope_close(scope)?))
        }
        BytecodeExpr::RetryOpen {
            max_attempts,
            base_backoff_ms,
        } => {
            let max_attempts = match eval_bytecode_operand(max_attempts, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "bytecode retry_open expects u32 max_attempts, got {other:?}"
                    ))
                }
            };
            let base_backoff_ms = match eval_bytecode_operand(base_backoff_ms, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "bytecode retry_open expects u32 base_backoff_ms, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U64(portable_retry_open(
                max_attempts,
                base_backoff_ms,
            )?))
        }
        BytecodeExpr::RetryRecordFailure { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode retry_record_failure expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_retry_record_failure(handle)?))
        }
        BytecodeExpr::RetryRecordSuccess { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode retry_record_success expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_retry_record_success(handle)?))
        }
        BytecodeExpr::RetryNextDelayMs { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode retry_next_delay_ms expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U32(portable_retry_next_delay_ms(handle)?))
        }
        BytecodeExpr::RetryExhausted { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode retry_exhausted expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_retry_exhausted(handle)?))
        }
        BytecodeExpr::RetryClose { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode retry_close expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_retry_close(handle)?))
        }
        BytecodeExpr::CircuitOpen {
            threshold,
            cooldown_ms,
        } => {
            let threshold = match eval_bytecode_operand(threshold, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "bytecode circuit_open expects u32 threshold, got {other:?}"
                    ))
                }
            };
            let cooldown_ms = match eval_bytecode_operand(cooldown_ms, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "bytecode circuit_open expects u32 cooldown_ms, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U64(portable_circuit_open(
                threshold,
                cooldown_ms,
            )?))
        }
        BytecodeExpr::CircuitAllow { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode circuit_allow expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_circuit_allow(handle)?))
        }
        BytecodeExpr::CircuitRecordFailure { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode circuit_record_failure expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_circuit_record_failure(handle)?))
        }
        BytecodeExpr::CircuitRecordSuccess { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode circuit_record_success expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_circuit_record_success(handle)?))
        }
        BytecodeExpr::CircuitState { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode circuit_state expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U32(portable_circuit_state(handle)?))
        }
        BytecodeExpr::CircuitClose { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode circuit_close expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_circuit_close(handle)?))
        }
        BytecodeExpr::BackpressureOpen { limit } => {
            let limit = match eval_bytecode_operand(limit, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "bytecode backpressure_open expects u32 limit, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U64(portable_backpressure_open(limit)?))
        }
        BytecodeExpr::BackpressureAcquire { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode backpressure_acquire expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_backpressure_acquire(handle)?))
        }
        BytecodeExpr::BackpressureRelease { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode backpressure_release expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_backpressure_release(handle)?))
        }
        BytecodeExpr::BackpressureSaturated { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode backpressure_saturated expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_backpressure_saturated(handle)?))
        }
        BytecodeExpr::BackpressureClose { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode backpressure_close expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_backpressure_close(handle)?))
        }
        BytecodeExpr::SupervisorOpen {
            restart_budget,
            degrade_after,
        } => {
            let restart_budget = match eval_bytecode_operand(restart_budget, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "bytecode supervisor_open expects u32 restart_budget, got {other:?}"
                    ))
                }
            };
            let degrade_after = match eval_bytecode_operand(degrade_after, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "bytecode supervisor_open expects u32 degrade_after, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U64(portable_supervisor_open(
                restart_budget,
                degrade_after,
            )?))
        }
        BytecodeExpr::SupervisorRecordFailure { handle, code } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode supervisor_record_failure expects u64 handle, got {other:?}"
                    ))
                }
            };
            let code = match eval_bytecode_operand(code, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "bytecode supervisor_record_failure expects u32 code, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_supervisor_record_failure(
                handle, code,
            )?))
        }
        BytecodeExpr::SupervisorRecordRecovery { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode supervisor_record_recovery expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_supervisor_record_recovery(handle)?))
        }
        BytecodeExpr::SupervisorShouldRestart { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode supervisor_should_restart expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_supervisor_should_restart(handle)?))
        }
        BytecodeExpr::SupervisorDegraded { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode supervisor_degraded expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_supervisor_degraded(handle)?))
        }
        BytecodeExpr::SupervisorClose { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode supervisor_close expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_supervisor_close(handle)?))
        }
        BytecodeExpr::FsReadAllU8 { path } => {
            Ok(RuntimeValue::BufU8(portable_fs_read_all_u8(path)?))
        }
        BytecodeExpr::FsWriteAllU8 { path, value } => {
            let value = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode fs_write_all expects buf[u8] operand, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_fs_write_all_u8(path, &value)?))
        }
        BytecodeExpr::NetWriteAllU8 { host, port, value } => {
            let value = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode net_write_all expects buf[u8] operand, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_net_write_all(
                host, *port, &value,
            )?))
        }
        BytecodeExpr::NetExchangeAllU8 { host, port, value } => {
            let value = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode net_exchange_all expects buf[u8] operand, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::BufU8(portable_net_exchange_all(
                host, *port, &value,
            )?))
        }
        BytecodeExpr::NetServeExchangeAllU8 {
            host,
            port,
            response,
        } => {
            let response = match eval_bytecode_operand(response, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode net_serve_exchange_all expects buf[u8] operand, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::BufU8(portable_net_serve_exchange_all(
                host, *port, &response,
            )?))
        }
        BytecodeExpr::NetListen { host, port } => {
            Ok(RuntimeValue::U64(portable_net_listen_handle(host, *port)?))
        }
        BytecodeExpr::TlsListen {
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
        BytecodeExpr::NetSessionOpen { host, port } => {
            Ok(RuntimeValue::U64(portable_net_session_open(host, *port)?))
        }
        BytecodeExpr::NetAccept { listener } => {
            let listener = match eval_bytecode_operand(listener, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode net_accept expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U64(portable_net_accept_handle(listener)?))
        }
        BytecodeExpr::HttpSessionAccept { listener } => {
            let listener = match eval_bytecode_operand(listener, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_session_accept expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U64(portable_http_session_accept(listener)?))
        }
        BytecodeExpr::NetReadAllU8 { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode net_read_all expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::BufU8(portable_net_read_all_handle(handle)?))
        }
        BytecodeExpr::SessionReadChunkU8 { handle, chunk_size } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode session_read_chunk expects u64 handle, got {other:?}"
                    ))
                }
            };
            let chunk_size = match eval_bytecode_operand(chunk_size, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "bytecode session_read_chunk expects u32 chunk size, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::BufU8(portable_session_read_chunk(
                handle, chunk_size,
            )?))
        }
        BytecodeExpr::HttpSessionRequest { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_session_request expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::BufU8(portable_http_session_request(handle)?))
        }
        BytecodeExpr::NetWriteHandleAllU8 { handle, value } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode net_write_handle_all expects u64 handle, got {other:?}"
                    ))
                }
            };
            let value = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode net_write_handle_all expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_net_write_all_handle(
                handle, &value,
            )?))
        }
        BytecodeExpr::SessionWriteChunkU8 { handle, value } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode session_write_chunk expects u64 handle, got {other:?}"
                    ))
                }
            };
            let value = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode session_write_chunk expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_session_write_chunk(
                handle, &value,
            )?))
        }
        BytecodeExpr::SessionFlush { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode session_flush expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_session_flush(handle)?))
        }
        BytecodeExpr::SessionAlive { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode session_alive expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_session_alive(handle)?))
        }
        BytecodeExpr::SessionHeartbeatU8 { handle, value } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode session_heartbeat expects u64 handle, got {other:?}"
                    ))
                }
            };
            let value = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode session_heartbeat expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_session_heartbeat(
                handle, &value,
            )?))
        }
        BytecodeExpr::SessionBackpressure { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode session_backpressure expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U32(portable_session_backpressure(handle)?))
        }
        BytecodeExpr::SessionBackpressureWait { handle, max_pending } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode session_backpressure_wait expects u64 handle, got {other:?}"
                    ))
                }
            };
            let max_pending = match eval_bytecode_operand(max_pending, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "bytecode session_backpressure_wait expects u32 max_pending, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_session_backpressure_wait(
                handle,
                max_pending,
            )?))
        }
        BytecodeExpr::SessionResumeId { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode session_resume_id expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U64(portable_session_resume_id(handle)?))
        }
        BytecodeExpr::SessionReconnect { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode session_reconnect expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_session_reconnect(handle)?))
        }
        BytecodeExpr::NetClose { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode net_close expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_net_close_handle(handle)?))
        }
        BytecodeExpr::HttpSessionClose { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_session_close expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_http_session_close(handle)?))
        }
        BytecodeExpr::HttpMethodEq { request, method } => {
            let request = match eval_bytecode_operand(request, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_method_eq expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_http_method_eq(
                &request, method,
            )))
        }
        BytecodeExpr::HttpPathEq { request, path } => {
            let request = match eval_bytecode_operand(request, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_path_eq expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_http_path_eq(&request, path)))
        }
        BytecodeExpr::HttpRequestMethod { request } => {
            let request = match eval_bytecode_operand(request, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_request_method expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::BufU8(portable_http_request_method(&request)))
        }
        BytecodeExpr::HttpRequestPath { request } => {
            let request = match eval_bytecode_operand(request, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_request_path expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::BufU8(portable_http_request_path(&request)))
        }
        BytecodeExpr::HttpRouteParam {
            request,
            pattern,
            param,
        } => {
            let request = match eval_bytecode_operand(request, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_route_param expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::BufU8(portable_http_route_param(
                &request, pattern, param,
            )))
        }
        BytecodeExpr::HttpHeaderEq {
            request,
            name,
            value,
        } => {
            let request = match eval_bytecode_operand(request, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_header_eq expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_http_header_eq(
                &request, name, value,
            )))
        }
        BytecodeExpr::HttpCookieEq {
            request,
            name,
            value,
        } => {
            let request = match eval_bytecode_operand(request, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_cookie_eq expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_http_cookie_eq(
                &request, name, value,
            )))
        }
        BytecodeExpr::HttpStatusU32 { value } => {
            let value = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_status_u32 expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U32(portable_http_status_u32(&value)))
        }
        BytecodeExpr::BufLit { literal } => {
            Ok(RuntimeValue::BufU8(decode_escaped_literal_bytes(literal)))
        }
        BytecodeExpr::BufConcat { left, right } => {
            let left = match eval_bytecode_operand(left, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode buf_concat expects left buf[u8], got {other:?}"
                    ))
                }
            };
            let right = match eval_bytecode_operand(right, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode buf_concat expects right buf[u8], got {other:?}"
                    ))
                }
            };
            let mut out = left;
            out.extend_from_slice(&right);
            Ok(RuntimeValue::BufU8(out))
        }
        BytecodeExpr::BufEqLit { value, literal } => {
            let value = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode buf_eq_lit expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_buf_eq_lit(&value, literal)))
        }
        BytecodeExpr::BufContainsLit { value, literal } => {
            let value = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode buf_contains_lit expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_buf_contains_lit(
                &value, literal,
            )))
        }
        BytecodeExpr::HttpHeader { request, name } => {
            let request = match eval_bytecode_operand(request, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_header expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::BufU8(portable_http_header(&request, name)))
        }
        BytecodeExpr::HttpHeaderCount { request } => {
            let request = match eval_bytecode_operand(request, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_header_count expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U32(portable_http_header_count(&request)))
        }
        BytecodeExpr::HttpHeaderName { request, index } => {
            let request = match eval_bytecode_operand(request, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_header_name expects buf[u8], got {other:?}"
                    ))
                }
            };
            let index = match eval_bytecode_operand(index, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_header_name expects u32 index, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::BufU8(portable_http_header_name(&request, index)))
        }
        BytecodeExpr::HttpHeaderValue { request, index } => {
            let request = match eval_bytecode_operand(request, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_header_value expects buf[u8], got {other:?}"
                    ))
                }
            };
            let index = match eval_bytecode_operand(index, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_header_value expects u32 index, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::BufU8(portable_http_header_value(
                &request, index,
            )))
        }
        BytecodeExpr::HttpCookie { request, name } => {
            let request = match eval_bytecode_operand(request, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_cookie expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::BufU8(portable_http_cookie(&request, name)))
        }
        BytecodeExpr::HttpQueryParam { request, key } => {
            let request = match eval_bytecode_operand(request, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_query_param expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::BufU8(portable_http_query_param(
                &request, key,
            )))
        }
        BytecodeExpr::HttpBody { request } => {
            let request = match eval_bytecode_operand(request, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("bytecode http_body expects buf[u8], got {other:?}")),
            };
            Ok(RuntimeValue::BufU8(portable_http_body(&request)))
        }
        BytecodeExpr::HttpMultipartPartCount { request } => {
            let request = match eval_bytecode_operand(request, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_multipart_part_count expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U32(portable_http_multipart_part_count(&request)))
        }
        BytecodeExpr::HttpMultipartPartName { request, index } => {
            let request = match eval_bytecode_operand(request, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_multipart_part_name expects buf[u8], got {other:?}"
                    ))
                }
            };
            let index = match eval_bytecode_operand(index, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_multipart_part_name expects u32 index, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::BufU8(portable_http_multipart_part_name(
                &request, index,
            )))
        }
        BytecodeExpr::HttpMultipartPartFilename { request, index } => {
            let request = match eval_bytecode_operand(request, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_multipart_part_filename expects buf[u8], got {other:?}"
                    ))
                }
            };
            let index = match eval_bytecode_operand(index, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_multipart_part_filename expects u32 index, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::BufU8(portable_http_multipart_part_filename(
                &request, index,
            )))
        }
        BytecodeExpr::HttpMultipartPartBody { request, index } => {
            let request = match eval_bytecode_operand(request, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_multipart_part_body expects buf[u8], got {other:?}"
                    ))
                }
            };
            let index = match eval_bytecode_operand(index, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_multipart_part_body expects u32 index, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::BufU8(portable_http_multipart_part_body(
                &request, index,
            )))
        }
        BytecodeExpr::HttpBodyLimit { request, limit } => {
            let request = match eval_bytecode_operand(request, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_body_limit expects buf[u8], got {other:?}"
                    ))
                }
            };
            let limit = match eval_bytecode_operand(limit, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_body_limit expects u32, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_http_body_limit(
                &request, limit,
            )))
        }
        BytecodeExpr::HttpBodyStreamOpen { request } => {
            let request = match eval_bytecode_operand(request, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_body_stream_open expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U64(portable_http_body_stream_open(&request)?))
        }
        BytecodeExpr::HttpBodyStreamNext { handle, chunk_size } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_body_stream_next expects u64 handle, got {other:?}"
                    ))
                }
            };
            let chunk_size = match eval_bytecode_operand(chunk_size, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_body_stream_next expects u32 chunk size, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::BufU8(portable_http_body_stream_next(
                handle, chunk_size,
            )?))
        }
        BytecodeExpr::HttpBodyStreamClose { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_body_stream_close expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_http_body_stream_close(handle)?))
        }
        BytecodeExpr::HttpResponseStreamOpen {
            handle,
            status,
            content_type,
        } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_response_stream_open expects u64 handle, got {other:?}"
                    ))
                }
            };
            let status = match eval_bytecode_operand(status, slots)? {
                RuntimeValue::U32(value) => value,
                RuntimeValue::I32(value) if value >= 0 => value as u32,
                other => {
                    return Err(format!(
                        "bytecode http_response_stream_open expects u32 status, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U64(portable_http_response_stream_open(
                handle,
                status,
                content_type,
            )?))
        }
        BytecodeExpr::HttpResponseStreamWrite { handle, body } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_response_stream_write expects u64 handle, got {other:?}"
                    ))
                }
            };
            let body = match eval_bytecode_operand(body, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_response_stream_write expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_http_response_stream_write(
                handle, &body,
            )?))
        }
        BytecodeExpr::HttpResponseStreamClose { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_response_stream_close expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_http_response_stream_close(handle)?))
        }
        BytecodeExpr::HttpClientOpen { host, port } => Ok(RuntimeValue::U64(
            portable_http_client_open(host, *port)?,
        )),
        BytecodeExpr::HttpClientRequest { handle, request } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_client_request expects u64 handle, got {other:?}"
                    ))
                }
            };
            let request = match eval_bytecode_operand(request, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_client_request expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::BufU8(portable_http_client_request(
                handle, &request,
            )?))
        }
        BytecodeExpr::HttpClientRequestRetry {
            handle,
            retries,
            backoff_ms,
            request,
        } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_client_request_retry expects u64 handle, got {other:?}"
                    ))
                }
            };
            let retries = match eval_bytecode_operand(retries, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_client_request_retry expects u32 retries, got {other:?}"
                    ))
                }
            };
            let backoff_ms = match eval_bytecode_operand(backoff_ms, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_client_request_retry expects u32 backoff, got {other:?}"
                    ))
                }
            };
            let request = match eval_bytecode_operand(request, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_client_request_retry expects buf[u8], got {other:?}"
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
        BytecodeExpr::HttpClientClose { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_client_close expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_http_client_close(handle)?))
        }
        BytecodeExpr::HttpClientPoolOpen {
            host,
            port,
            max_size,
        } => {
            let max_size = match eval_bytecode_operand(max_size, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_client_pool_open expects u32 max_size, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U64(portable_http_client_pool_open(
                host, *port, max_size,
            )?))
        }
        BytecodeExpr::HttpClientPoolAcquire { pool } => {
            let pool = match eval_bytecode_operand(pool, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_client_pool_acquire expects u64 pool handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U64(portable_http_client_pool_acquire(pool)?))
        }
        BytecodeExpr::HttpClientPoolRelease { pool, handle } => {
            let pool = match eval_bytecode_operand(pool, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_client_pool_release expects u64 pool handle, got {other:?}"
                    ))
                }
            };
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_client_pool_release expects u64 client handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_http_client_pool_release(
                pool, handle,
            )?))
        }
        BytecodeExpr::HttpClientPoolClose { pool } => {
            let pool = match eval_bytecode_operand(pool, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_client_pool_close expects u64 pool handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_http_client_pool_close(pool)?))
        }
        BytecodeExpr::HttpServerConfigU32 { token } => {
            Ok(RuntimeValue::U32(portable_http_server_config_u32(token)))
        }
        BytecodeExpr::MsgLogOpen => Ok(RuntimeValue::U64(portable_msg_log_open()?)),
        BytecodeExpr::MsgLogClose { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => return Err(format!("bytecode msg_log_close expects u64 handle, got {other:?}")),
            };
            Ok(RuntimeValue::Bool(portable_msg_log_close(handle)?))
        }
        BytecodeExpr::MsgSend {
            handle,
            conversation,
            recipient,
            payload,
        } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => return Err(format!("bytecode msg_send expects u64 handle, got {other:?}")),
            };
            let payload = match eval_bytecode_operand(payload, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("bytecode msg_send expects buf[u8], got {other:?}")),
            };
            Ok(RuntimeValue::U32(portable_msg_send(
                handle, conversation, recipient, &payload,
            )?))
        }
        BytecodeExpr::MsgSendDedup {
            handle,
            conversation,
            recipient,
            dedup_key,
            payload,
        } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => return Err(format!("bytecode msg_send_dedup expects u64 handle, got {other:?}")),
            };
            let dedup_key = match eval_bytecode_operand(dedup_key, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("bytecode msg_send_dedup expects buf[u8] key, got {other:?}")),
            };
            let payload = match eval_bytecode_operand(payload, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("bytecode msg_send_dedup expects buf[u8] payload, got {other:?}")),
            };
            Ok(RuntimeValue::U32(portable_msg_send_dedup(
                handle,
                conversation,
                recipient,
                &dedup_key,
                &payload,
            )?))
        }
        BytecodeExpr::MsgSubscribe {
            handle,
            room,
            recipient,
        } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => return Err(format!("bytecode msg_subscribe expects u64 handle, got {other:?}")),
            };
            Ok(RuntimeValue::Bool(portable_msg_subscribe(
                handle, room, recipient,
            )?))
        }
        BytecodeExpr::MsgSubscriberCount { handle, room } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => return Err(format!("bytecode msg_subscriber_count expects u64 handle, got {other:?}")),
            };
            Ok(RuntimeValue::U32(portable_msg_subscriber_count(handle, room)?))
        }
        BytecodeExpr::MsgFanout {
            handle,
            room,
            payload,
        } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => return Err(format!("bytecode msg_fanout expects u64 handle, got {other:?}")),
            };
            let payload = match eval_bytecode_operand(payload, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("bytecode msg_fanout expects buf[u8], got {other:?}")),
            };
            Ok(RuntimeValue::U32(portable_msg_fanout(handle, room, &payload)?))
        }
        BytecodeExpr::MsgRecvNext { handle, recipient } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => return Err(format!("bytecode msg_recv_next expects u64 handle, got {other:?}")),
            };
            Ok(RuntimeValue::BufU8(portable_msg_recv_next(handle, recipient)?))
        }
        BytecodeExpr::MsgRecvSeq { handle, recipient } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => return Err(format!("bytecode msg_recv_seq expects u64 handle, got {other:?}")),
            };
            Ok(RuntimeValue::U32(portable_msg_recv_seq(handle, recipient)?))
        }
        BytecodeExpr::MsgAck {
            handle,
            recipient,
            seq,
        } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => return Err(format!("bytecode msg_ack expects u64 handle, got {other:?}")),
            };
            let seq = match eval_bytecode_operand(seq, slots)? {
                RuntimeValue::U32(value) => value,
                other => return Err(format!("bytecode msg_ack expects u32 seq, got {other:?}")),
            };
            Ok(RuntimeValue::Bool(portable_msg_ack(handle, recipient, seq)?))
        }
        BytecodeExpr::MsgMarkRetry {
            handle,
            recipient,
            seq,
        } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => return Err(format!("bytecode msg_mark_retry expects u64 handle, got {other:?}")),
            };
            let seq = match eval_bytecode_operand(seq, slots)? {
                RuntimeValue::U32(value) => value,
                other => return Err(format!("bytecode msg_mark_retry expects u32 seq, got {other:?}")),
            };
            Ok(RuntimeValue::Bool(portable_msg_mark_retry(handle, recipient, seq)?))
        }
        BytecodeExpr::MsgRetryCount {
            handle,
            recipient,
            seq,
        } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => return Err(format!("bytecode msg_retry_count expects u64 handle, got {other:?}")),
            };
            let seq = match eval_bytecode_operand(seq, slots)? {
                RuntimeValue::U32(value) => value,
                other => return Err(format!("bytecode msg_retry_count expects u32 seq, got {other:?}")),
            };
            Ok(RuntimeValue::U32(portable_msg_retry_count(handle, recipient, seq)?))
        }
        BytecodeExpr::MsgPendingCount { handle, recipient } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => return Err(format!("bytecode msg_pending_count expects u64 handle, got {other:?}")),
            };
            Ok(RuntimeValue::U32(portable_msg_pending_count(handle, recipient)?))
        }
        BytecodeExpr::MsgDeliveryTotal { handle, recipient } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => return Err(format!("bytecode msg_delivery_total expects u64 handle, got {other:?}")),
            };
            Ok(RuntimeValue::U32(portable_msg_delivery_total(handle, recipient)?))
        }
        BytecodeExpr::MsgFailureClass { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => return Err(format!("bytecode msg_failure_class expects u64 handle, got {other:?}")),
            };
            Ok(RuntimeValue::U32(portable_msg_failure_class(handle)?))
        }
        BytecodeExpr::MsgReplayOpen {
            handle,
            recipient,
            from_seq,
        } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => return Err(format!("bytecode msg_replay_open expects u64 handle, got {other:?}")),
            };
            let from_seq = match eval_bytecode_operand(from_seq, slots)? {
                RuntimeValue::U32(value) => value,
                other => return Err(format!("bytecode msg_replay_open expects u32 from_seq, got {other:?}")),
            };
            Ok(RuntimeValue::U64(portable_msg_replay_open(
                handle, recipient, from_seq,
            )?))
        }
        BytecodeExpr::MsgReplayNext { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => return Err(format!("bytecode msg_replay_next expects u64 handle, got {other:?}")),
            };
            Ok(RuntimeValue::BufU8(portable_msg_replay_next(handle)?))
        }
        BytecodeExpr::MsgReplaySeq { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => return Err(format!("bytecode msg_replay_seq expects u64 handle, got {other:?}")),
            };
            Ok(RuntimeValue::U32(portable_msg_replay_seq(handle)?))
        }
        BytecodeExpr::MsgReplayClose { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => return Err(format!("bytecode msg_replay_close expects u64 handle, got {other:?}")),
            };
            Ok(RuntimeValue::Bool(portable_msg_replay_close(handle)?))
        }
        BytecodeExpr::ServiceOpen { name } => Ok(RuntimeValue::U64(portable_service_open(name)?)),
        BytecodeExpr::ServiceClose { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode service_close expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_service_close(handle)?))
        }
        BytecodeExpr::ServiceShutdown { handle, grace_ms } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode service_shutdown expects u64 handle, got {other:?}"
                    ))
                }
            };
            let grace_ms = match eval_bytecode_operand(grace_ms, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "bytecode service_shutdown expects u32 grace, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_service_shutdown(
                handle, grace_ms,
            )?))
        }
        BytecodeExpr::ServiceLog { handle, message } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode service_log expects u64 handle, got {other:?}"
                    ))
                }
            };
            let message = match eval_bytecode_operand(message, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode service_log expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_service_log(handle, &message)?))
        }
        BytecodeExpr::ServiceTraceBegin { handle, name } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode service_trace_begin expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U64(portable_service_trace_begin(
                handle, name,
            )?))
        }
        BytecodeExpr::ServiceTraceEnd { trace } => {
            let trace = match eval_bytecode_operand(trace, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode service_trace_end expects u64 trace, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_service_trace_end(trace)?))
        }
        BytecodeExpr::ServiceMetricCount { handle, value } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode service_metric_count expects u64 handle, got {other:?}"
                    ))
                }
            };
            let value = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "bytecode service_metric_count expects u32 value, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_service_metric_count(
                handle, value,
            )?))
        }
        BytecodeExpr::ServiceMetricCountDim {
            handle,
            value,
            metric,
            dimension,
        } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode service_metric_count_dim expects u64 handle, got {other:?}"
                    ))
                }
            };
            let value = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "bytecode service_metric_count_dim expects u32 value, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_service_metric_count_dim(
                handle, metric, dimension, value,
            )?))
        }
        BytecodeExpr::ServiceMetricTotal { handle, metric } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode service_metric_total expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U32(portable_service_metric_total(
                handle, metric,
            )?))
        }
        BytecodeExpr::ServiceHealthStatus { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode service_health_status expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U32(portable_service_health_status(handle)?))
        }
        BytecodeExpr::ServiceReadinessStatus { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode service_readiness_status expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U32(portable_service_readiness_status(
                handle,
            )?))
        }
        BytecodeExpr::ServiceSetHealth { handle, status } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode service_set_health expects u64 handle, got {other:?}"
                    ))
                }
            };
            let status = match eval_bytecode_operand(status, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "bytecode service_set_health expects u32 status, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_service_set_health(
                handle, status,
            )?))
        }
        BytecodeExpr::ServiceSetReadiness { handle, status } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode service_set_readiness expects u64 handle, got {other:?}"
                    ))
                }
            };
            let status = match eval_bytecode_operand(status, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "bytecode service_set_readiness expects u32 status, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_service_set_readiness(
                handle, status,
            )?))
        }
        BytecodeExpr::ServiceSetDegraded { handle, degraded } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode service_set_degraded expects u64 handle, got {other:?}"
                    ))
                }
            };
            let degraded = match eval_bytecode_operand(degraded, slots)? {
                RuntimeValue::Bool(value) => value,
                other => {
                    return Err(format!(
                        "bytecode service_set_degraded expects b1 flag, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_service_set_degraded(
                handle, degraded,
            )?))
        }
        BytecodeExpr::ServiceDegraded { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode service_degraded expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_service_degraded(handle)?))
        }
        BytecodeExpr::ServiceEvent {
            handle,
            class,
            message,
        } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode service_event expects u64 handle, got {other:?}"
                    ))
                }
            };
            let message = match eval_bytecode_operand(message, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode service_event expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_service_event(
                handle, class, &message,
            )?))
        }
        BytecodeExpr::ServiceEventTotal { handle, class } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode service_event_total expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U32(portable_service_event_total(
                handle, class,
            )?))
        }
        BytecodeExpr::ServiceTraceLink { trace, parent } => {
            let trace = match eval_bytecode_operand(trace, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode service_trace_link expects u64 trace, got {other:?}"
                    ))
                }
            };
            let parent = match eval_bytecode_operand(parent, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode service_trace_link expects u64 parent, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_service_trace_link(
                trace, parent,
            )?))
        }
        BytecodeExpr::ServiceTraceLinkCount { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode service_trace_link_count expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U32(portable_service_trace_link_count(handle)?))
        }
        BytecodeExpr::ServiceFailureCount {
            handle,
            class,
            value,
        } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode service_failure_count expects u64 handle, got {other:?}"
                    ))
                }
            };
            let value = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "bytecode service_failure_count expects u32 value, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_service_failure_count(
                handle, class, value,
            )?))
        }
        BytecodeExpr::ServiceFailureTotal { handle, class } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode service_failure_total expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U32(portable_service_failure_total(
                handle, class,
            )?))
        }
        BytecodeExpr::ServiceCheckpointSaveU32 { handle, key, value } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode service_checkpoint_save_u32 expects u64 handle, got {other:?}"
                    ))
                }
            };
            let value = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "bytecode service_checkpoint_save_u32 expects u32 value, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_service_checkpoint_save_u32(
                handle, key, value,
            )?))
        }
        BytecodeExpr::ServiceCheckpointLoadU32 { handle, key } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode service_checkpoint_load_u32 expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U32(portable_service_checkpoint_load_u32(
                handle, key,
            )?))
        }
        BytecodeExpr::ServiceCheckpointExists { handle, key } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode service_checkpoint_exists expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_service_checkpoint_exists(
                handle, key,
            )?))
        }
        BytecodeExpr::ServiceMigrateDb { handle, db_handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode service_migrate_db expects u64 service, got {other:?}"
                    ))
                }
            };
            let db_handle = match eval_bytecode_operand(db_handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode service_migrate_db expects u64 db handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_service_migrate_db(
                handle, db_handle,
            )?))
        }
        BytecodeExpr::ServiceRoute {
            request,
            method,
            path,
        } => {
            let request = match eval_bytecode_operand(request, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode service_route expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_service_route(
                &request, method, path,
            )))
        }
        BytecodeExpr::ServiceRequireHeader {
            request,
            name,
            value,
        } => {
            let request = match eval_bytecode_operand(request, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode service_require_header expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_service_require_header(
                &request, name, value,
            )))
        }
        BytecodeExpr::ServiceErrorStatus { kind } => {
            Ok(RuntimeValue::U32(portable_service_error_status(kind)))
        }
        BytecodeExpr::TlsServerConfigU32 { value } => Ok(RuntimeValue::U32(*value)),
        BytecodeExpr::TlsServerConfigBuf { value } => {
            Ok(RuntimeValue::BufU8(value.as_bytes().to_vec()))
        }
        BytecodeExpr::ListenerSetTimeoutMs { handle, value } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode listener_set_timeout_ms expects u64 handle, got {other:?}"
                    ))
                }
            };
            let timeout = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "bytecode listener_set_timeout_ms expects u32 timeout, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_listener_set_timeout_ms(
                handle, timeout,
            )?))
        }
        BytecodeExpr::SessionSetTimeoutMs { handle, value } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode session_set_timeout_ms expects u64 handle, got {other:?}"
                    ))
                }
            };
            let timeout = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "bytecode session_set_timeout_ms expects u32 timeout, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_session_set_timeout_ms(
                handle, timeout,
            )?))
        }
        BytecodeExpr::ListenerSetShutdownGraceMs { handle, value } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode listener_set_shutdown_grace_ms expects u64 handle, got {other:?}"
                    ))
                }
            };
            let grace = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "bytecode listener_set_shutdown_grace_ms expects u32 grace, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_listener_set_shutdown_grace_ms(
                handle, grace,
            )?))
        }
        BytecodeExpr::BufParseU32 { value } => {
            let value = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode buf_parse_u32 expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U32(portable_buf_parse_u32(&value)))
        }
        BytecodeExpr::BufParseBool { value } => {
            let value = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode buf_parse_bool expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_buf_parse_bool(&value)))
        }
        BytecodeExpr::HttpWriteResponse {
            handle,
            status,
            body,
        } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_write_response expects u64 handle, got {other:?}"
                    ))
                }
            };
            let status = match eval_bytecode_operand(status, slots)? {
                RuntimeValue::U32(value) => value,
                RuntimeValue::I32(value) if value >= 0 => value as u32,
                other => {
                    return Err(format!(
                        "bytecode http_write_response expects u32 status, got {other:?}"
                    ))
                }
            };
            let body = match eval_bytecode_operand(body, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_write_response expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_http_write_response(
                handle, status, &body,
            )?))
        }
        BytecodeExpr::HttpWriteTextResponse {
            handle,
            status,
            body,
        } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_write_text_response expects u64 handle, got {other:?}"
                    ))
                }
            };
            let status = match eval_bytecode_operand(status, slots)? {
                RuntimeValue::U32(value) => value,
                RuntimeValue::I32(value) if value >= 0 => value as u32,
                other => {
                    return Err(format!(
                        "bytecode http_write_text_response expects u32 status, got {other:?}"
                    ))
                }
            };
            let body = match eval_bytecode_operand(body, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_write_text_response expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_http_write_text_response(
                handle, status, &body,
            )?))
        }
        BytecodeExpr::HttpWriteTextResponseCookie {
            handle,
            status,
            cookie_name,
            cookie_value,
            body,
        } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_write_text_response_cookie expects u64 handle, got {other:?}"
                    ))
                }
            };
            let status = match eval_bytecode_operand(status, slots)? {
                RuntimeValue::U32(value) => value,
                RuntimeValue::I32(value) if value >= 0 => value as u32,
                other => {
                    return Err(format!(
                        "bytecode http_write_text_response_cookie expects u32 status, got {other:?}"
                    ))
                }
            };
            let body = match eval_bytecode_operand(body, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_write_text_response_cookie expects buf[u8], got {other:?}"
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
        BytecodeExpr::HttpWriteTextResponseHeaders2 {
            handle,
            status,
            header1_name,
            header1_value,
            header2_name,
            header2_value,
            body,
        } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => return Err(format!("bytecode http_write_text_response_headers2 expects u64 handle, got {other:?}")),
            };
            let status = match eval_bytecode_operand(status, slots)? {
                RuntimeValue::U32(value) => value,
                RuntimeValue::I32(value) if value >= 0 => value as u32,
                other => return Err(format!("bytecode http_write_text_response_headers2 expects u32 status, got {other:?}")),
            };
            let body = match eval_bytecode_operand(body, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("bytecode http_write_text_response_headers2 expects buf[u8], got {other:?}")),
            };
            Ok(RuntimeValue::Bool(portable_http_write_text_response_headers2(
                handle,
                status,
                header1_name,
                header1_value,
                header2_name,
                header2_value,
                &body,
            )?))
        }
        BytecodeExpr::HttpSessionWriteText {
            handle,
            status,
            body,
        } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_session_write_text expects u64 handle, got {other:?}"
                    ))
                }
            };
            let status = match eval_bytecode_operand(status, slots)? {
                RuntimeValue::U32(value) => value,
                RuntimeValue::I32(value) if value >= 0 => value as u32,
                other => {
                    return Err(format!(
                        "bytecode http_session_write_text expects u32 status, got {other:?}"
                    ))
                }
            };
            let body = match eval_bytecode_operand(body, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_session_write_text expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_http_session_write_text(
                handle, status, &body,
            )?))
        }
        BytecodeExpr::HttpSessionWriteTextCookie {
            handle,
            status,
            cookie_name,
            cookie_value,
            body,
        } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_session_write_text_cookie expects u64 handle, got {other:?}"
                    ))
                }
            };
            let status = match eval_bytecode_operand(status, slots)? {
                RuntimeValue::U32(value) => value,
                RuntimeValue::I32(value) if value >= 0 => value as u32,
                other => {
                    return Err(format!(
                        "bytecode http_session_write_text_cookie expects u32 status, got {other:?}"
                    ))
                }
            };
            let body = match eval_bytecode_operand(body, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_session_write_text_cookie expects buf[u8], got {other:?}"
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
        BytecodeExpr::HttpSessionWriteTextHeaders2 {
            handle,
            status,
            header1_name,
            header1_value,
            header2_name,
            header2_value,
            body,
        } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => return Err(format!("bytecode http_session_write_text_headers2 expects u64 handle, got {other:?}")),
            };
            let status = match eval_bytecode_operand(status, slots)? {
                RuntimeValue::U32(value) => value,
                RuntimeValue::I32(value) if value >= 0 => value as u32,
                other => return Err(format!("bytecode http_session_write_text_headers2 expects u32 status, got {other:?}")),
            };
            let body = match eval_bytecode_operand(body, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("bytecode http_session_write_text_headers2 expects buf[u8], got {other:?}")),
            };
            Ok(RuntimeValue::Bool(portable_http_session_write_text_headers2(
                handle,
                status,
                header1_name,
                header1_value,
                header2_name,
                header2_value,
                &body,
            )?))
        }
        BytecodeExpr::HttpWriteJsonResponse {
            handle,
            status,
            body,
        } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_write_json_response expects u64 handle, got {other:?}"
                    ))
                }
            };
            let status = match eval_bytecode_operand(status, slots)? {
                RuntimeValue::U32(value) => value,
                RuntimeValue::I32(value) if value >= 0 => value as u32,
                other => {
                    return Err(format!(
                        "bytecode http_write_json_response expects u32 status, got {other:?}"
                    ))
                }
            };
            let body = match eval_bytecode_operand(body, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_write_json_response expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_http_write_json_response(
                handle, status, &body,
            )?))
        }
        BytecodeExpr::HttpWriteJsonResponseCookie {
            handle,
            status,
            cookie_name,
            cookie_value,
            body,
        } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_write_json_response_cookie expects u64 handle, got {other:?}"
                    ))
                }
            };
            let status = match eval_bytecode_operand(status, slots)? {
                RuntimeValue::U32(value) => value,
                RuntimeValue::I32(value) if value >= 0 => value as u32,
                other => {
                    return Err(format!(
                        "bytecode http_write_json_response_cookie expects u32 status, got {other:?}"
                    ))
                }
            };
            let body = match eval_bytecode_operand(body, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_write_json_response_cookie expects buf[u8], got {other:?}"
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
        BytecodeExpr::HttpWriteJsonResponseHeaders2 {
            handle,
            status,
            header1_name,
            header1_value,
            header2_name,
            header2_value,
            body,
        } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => return Err(format!("bytecode http_write_json_response_headers2 expects u64 handle, got {other:?}")),
            };
            let status = match eval_bytecode_operand(status, slots)? {
                RuntimeValue::U32(value) => value,
                RuntimeValue::I32(value) if value >= 0 => value as u32,
                other => return Err(format!("bytecode http_write_json_response_headers2 expects u32 status, got {other:?}")),
            };
            let body = match eval_bytecode_operand(body, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("bytecode http_write_json_response_headers2 expects buf[u8], got {other:?}")),
            };
            Ok(RuntimeValue::Bool(portable_http_write_json_response_headers2(
                handle,
                status,
                header1_name,
                header1_value,
                header2_name,
                header2_value,
                &body,
            )?))
        }
        BytecodeExpr::HttpSessionWriteJson {
            handle,
            status,
            body,
        } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_session_write_json expects u64 handle, got {other:?}"
                    ))
                }
            };
            let status = match eval_bytecode_operand(status, slots)? {
                RuntimeValue::U32(value) => value,
                RuntimeValue::I32(value) if value >= 0 => value as u32,
                other => {
                    return Err(format!(
                        "bytecode http_session_write_json expects u32 status, got {other:?}"
                    ))
                }
            };
            let body = match eval_bytecode_operand(body, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_session_write_json expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_http_session_write_json(
                handle, status, &body,
            )?))
        }
        BytecodeExpr::HttpSessionWriteJsonCookie {
            handle,
            status,
            cookie_name,
            cookie_value,
            body,
        } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_session_write_json_cookie expects u64 handle, got {other:?}"
                    ))
                }
            };
            let status = match eval_bytecode_operand(status, slots)? {
                RuntimeValue::U32(value) => value,
                RuntimeValue::I32(value) if value >= 0 => value as u32,
                other => {
                    return Err(format!(
                        "bytecode http_session_write_json_cookie expects u32 status, got {other:?}"
                    ))
                }
            };
            let body = match eval_bytecode_operand(body, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_session_write_json_cookie expects buf[u8], got {other:?}"
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
        BytecodeExpr::HttpSessionWriteJsonHeaders2 {
            handle,
            status,
            header1_name,
            header1_value,
            header2_name,
            header2_value,
            body,
        } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => return Err(format!("bytecode http_session_write_json_headers2 expects u64 handle, got {other:?}")),
            };
            let status = match eval_bytecode_operand(status, slots)? {
                RuntimeValue::U32(value) => value,
                RuntimeValue::I32(value) if value >= 0 => value as u32,
                other => return Err(format!("bytecode http_session_write_json_headers2 expects u32 status, got {other:?}")),
            };
            let body = match eval_bytecode_operand(body, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("bytecode http_session_write_json_headers2 expects buf[u8], got {other:?}")),
            };
            Ok(RuntimeValue::Bool(portable_http_session_write_json_headers2(
                handle,
                status,
                header1_name,
                header1_value,
                header2_name,
                header2_value,
                &body,
            )?))
        }
        BytecodeExpr::HttpWriteResponseHeader {
            handle,
            status,
            header_name,
            header_value,
            body,
        } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_write_response_header expects u64 handle, got {other:?}"
                    ))
                }
            };
            let status = match eval_bytecode_operand(status, slots)? {
                RuntimeValue::U32(value) => value,
                RuntimeValue::I32(value) if value >= 0 => value as u32,
                other => {
                    return Err(format!(
                        "bytecode http_write_response_header expects u32 status, got {other:?}"
                    ))
                }
            };
            let body = match eval_bytecode_operand(body, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode http_write_response_header expects buf[u8], got {other:?}"
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
        BytecodeExpr::JsonGetU32 { value, key } => {
            let value = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode json_get_u32 expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U32(portable_json_get_u32(&value, key)))
        }
        BytecodeExpr::JsonGetBool { value, key } => {
            let value = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode json_get_bool expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_json_get_bool(&value, key)))
        }
        BytecodeExpr::JsonHasKey { value, key } => {
            let value = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode json_has_key expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_json_has_key(&value, key)))
        }
        BytecodeExpr::JsonGetBufU8 { value, key } => {
            let value = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode json_get_buf expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::BufU8(portable_json_get_buf(&value, key)))
        }
        BytecodeExpr::JsonGetStr { value, key } => {
            let value = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode json_get_str expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::BufU8(portable_json_get_str(&value, key)))
        }
        BytecodeExpr::JsonGetU32Or {
            value,
            key,
            default_value,
        } => {
            let value = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode json_get_u32_or expects buf[u8], got {other:?}"
                    ))
                }
            };
            let default_value = match eval_bytecode_operand(default_value, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "bytecode json_get_u32_or expects u32 default, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U32(portable_json_get_u32_or(
                &value,
                key,
                default_value,
            )))
        }
        BytecodeExpr::JsonGetBoolOr {
            value,
            key,
            default_value,
        } => {
            let value = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode json_get_bool_or expects buf[u8], got {other:?}"
                    ))
                }
            };
            let default_value = match eval_bytecode_operand(default_value, slots)? {
                RuntimeValue::Bool(value) => value,
                other => {
                    return Err(format!(
                        "bytecode json_get_bool_or expects b1 default, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_json_get_bool_or(
                &value,
                key,
                default_value,
            )))
        }
        BytecodeExpr::JsonGetBufOr {
            value,
            key,
            default_value,
        } => {
            let value = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode json_get_buf_or expects buf[u8], got {other:?}"
                    ))
                }
            };
            let default_value = match eval_bytecode_operand(default_value, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode json_get_buf_or expects buf[u8] default, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::BufU8(portable_json_get_buf_or(
                &value,
                key,
                &default_value,
            )))
        }
        BytecodeExpr::JsonGetStrOr {
            value,
            key,
            default_value,
        } => {
            let value = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode json_get_str_or expects buf[u8], got {other:?}"
                    ))
                }
            };
            let default_value = match eval_bytecode_operand(default_value, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode json_get_str_or expects str default, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::BufU8(portable_json_get_str_or(
                &value,
                key,
                &default_value,
            )))
        }
        BytecodeExpr::JsonArrayLen { value } => {
            let value = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode json_array_len expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U32(portable_json_array_len(&value)))
        }
        BytecodeExpr::JsonIndexU32 { value, index } => {
            let value = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode json_index_u32 expects buf[u8], got {other:?}"
                    ))
                }
            };
            let index = match eval_bytecode_operand(index, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "bytecode json_index_u32 expects u32 index, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U32(portable_json_index_u32(&value, index)))
        }
        BytecodeExpr::JsonIndexBool { value, index } => {
            let value = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode json_index_bool expects buf[u8], got {other:?}"
                    ))
                }
            };
            let index = match eval_bytecode_operand(index, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "bytecode json_index_bool expects u32 index, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_json_index_bool(&value, index)))
        }
        BytecodeExpr::JsonIndexStr { value, index } => {
            let value = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode json_index_str expects buf[u8], got {other:?}"
                    ))
                }
            };
            let index = match eval_bytecode_operand(index, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "bytecode json_index_str expects u32 index, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::BufU8(portable_json_index_str(&value, index)))
        }
        BytecodeExpr::JsonEncodeObj { entries } => {
            let mut values = Vec::new();
            for (key, operand) in entries {
                values.push((key.clone(), eval_bytecode_operand(operand, slots)?));
            }
            Ok(RuntimeValue::BufU8(portable_json_encode_object(&values)))
        }
        BytecodeExpr::JsonEncodeArr { values } => {
            let mut out = Vec::new();
            for operand in values {
                out.push(eval_bytecode_operand(operand, slots)?);
            }
            Ok(RuntimeValue::BufU8(portable_json_encode_array(&out)))
        }
        BytecodeExpr::StrLit { literal } => Ok(RuntimeValue::BufU8(literal.as_bytes().to_vec())),
        BytecodeExpr::StrConcat { left, right } => {
            let left = match eval_bytecode_operand(left, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("bytecode str_concat expects str, got {other:?}")),
            };
            let right = match eval_bytecode_operand(right, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("bytecode str_concat expects str, got {other:?}")),
            };
            let mut out = left;
            out.extend(right);
            Ok(RuntimeValue::BufU8(out))
        }
        BytecodeExpr::StrFromU32 { value } => {
            let value = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::U32(value) => value,
                other => return Err(format!("bytecode str_from_u32 expects u32, got {other:?}")),
            };
            Ok(RuntimeValue::BufU8(value.to_string().into_bytes()))
        }
        BytecodeExpr::StrFromBool { value } => {
            let value = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::Bool(value) => value,
                other => return Err(format!("bytecode str_from_bool expects b1, got {other:?}")),
            };
            Ok(RuntimeValue::BufU8(if value {
                b"true".to_vec()
            } else {
                b"false".to_vec()
            }))
        }
        BytecodeExpr::StrEqLit { value, literal } => {
            let value = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("bytecode str_eq_lit expects str, got {other:?}")),
            };
            Ok(RuntimeValue::Bool(value == literal.as_bytes()))
        }
        BytecodeExpr::StrToBuf { value } | BytecodeExpr::BufToStr { value } => {
            let value = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode str/buf conversion expects buf-like value, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::BufU8(value))
        }
        BytecodeExpr::BufHexStr { value } => {
            let value = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode buf_hex_str expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::BufU8(portable_buf_hex_str(&value)))
        }
        BytecodeExpr::ConfigGetU32 { value } => Ok(RuntimeValue::U32(*value)),
        BytecodeExpr::ConfigGetBool { value } => Ok(RuntimeValue::Bool(*value)),
        BytecodeExpr::ConfigGetStr { value } => Ok(RuntimeValue::BufU8(value.as_bytes().to_vec())),
        BytecodeExpr::ConfigHas { present } => Ok(RuntimeValue::Bool(*present)),
        BytecodeExpr::EnvGetU32 { key } => Ok(RuntimeValue::U32(portable_env_get_u32(key))),
        BytecodeExpr::EnvGetBool { key } => Ok(RuntimeValue::Bool(portable_env_get_bool(key))),
        BytecodeExpr::EnvGetStr { key } => Ok(RuntimeValue::BufU8(portable_env_get_str(key))),
        BytecodeExpr::EnvHas { key } => Ok(RuntimeValue::Bool(portable_env_has(key))),
        BytecodeExpr::BufBeforeLit { value, literal } => {
            let value = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode buf_before_lit expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::BufU8(portable_buf_before_lit(&value, literal)))
        }
        BytecodeExpr::BufAfterLit { value, literal } => {
            let value = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode buf_after_lit expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::BufU8(portable_buf_after_lit(&value, literal)))
        }
        BytecodeExpr::BufTrimAscii { value } => {
            let value = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode buf_trim_ascii expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::BufU8(portable_buf_trim_ascii(&value)))
        }
        BytecodeExpr::DateParseYmd { value } => {
            let value = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode date_parse_ymd expects str, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U32(portable_date_parse_ymd(&value)))
        }
        BytecodeExpr::TimeParseHms { value } => {
            let value = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode time_parse_hms expects str, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U32(portable_time_parse_hms(&value)))
        }
        BytecodeExpr::DateFormatYmd { value } => {
            let value = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "bytecode date_format_ymd expects u32, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::BufU8(portable_date_format_ymd(value)))
        }
        BytecodeExpr::TimeFormatHms { value } => {
            let value = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "bytecode time_format_hms expects u32, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::BufU8(portable_time_format_hms(value)))
        }
        BytecodeExpr::DbOpen { path } => Ok(RuntimeValue::U64(portable_db_open(path)?)),
        BytecodeExpr::DbClose { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode db_close expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_db_close(handle)?))
        }
        BytecodeExpr::DbExec { handle, sql } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode db_exec expects u64 handle, got {other:?}"
                    ))
                }
            };
            let sql = match eval_bytecode_operand(sql, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("bytecode db_exec expects buf[u8], got {other:?}")),
            };
            Ok(RuntimeValue::Bool(portable_db_exec(handle, &sql)?))
        }
        BytecodeExpr::DbPrepare { handle, name, sql } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode db_prepare expects u64 handle, got {other:?}"
                    ))
                }
            };
            let sql = match eval_bytecode_operand(sql, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode db_prepare expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_db_prepare(handle, name, &sql)?))
        }
        BytecodeExpr::DbExecPrepared {
            handle,
            name,
            params,
        } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode db_exec_prepared expects u64 handle, got {other:?}"
                    ))
                }
            };
            let params = match eval_bytecode_operand(params, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode db_exec_prepared expects buf[u8] params, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_db_exec_prepared(
                handle, name, &params,
            )?))
        }
        BytecodeExpr::DbQueryU32 { handle, sql } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode db_query_u32 expects u64 handle, got {other:?}"
                    ))
                }
            };
            let sql = match eval_bytecode_operand(sql, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode db_query_u32 expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U32(portable_db_query_u32(handle, &sql)?))
        }
        BytecodeExpr::DbQueryBufU8 { handle, sql } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode db_query_buf expects u64 handle, got {other:?}"
                    ))
                }
            };
            let sql = match eval_bytecode_operand(sql, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode db_query_buf expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::BufU8(portable_db_query_buf(handle, &sql)?))
        }
        BytecodeExpr::DbQueryRow { handle, sql } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode db_query_row expects u64 handle, got {other:?}"
                    ))
                }
            };
            let sql = match eval_bytecode_operand(sql, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode db_query_row expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::BufU8(portable_db_query_row(handle, &sql)?))
        }
        BytecodeExpr::DbQueryPreparedU32 {
            handle,
            name,
            params,
        } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode db_query_prepared_u32 expects u64 handle, got {other:?}"
                    ))
                }
            };
            let params = match eval_bytecode_operand(params, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode db_query_prepared_u32 expects buf[u8] params, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U32(portable_db_query_prepared_u32(
                handle, name, &params,
            )?))
        }
        BytecodeExpr::DbQueryPreparedBufU8 {
            handle,
            name,
            params,
        } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode db_query_prepared_buf expects u64 handle, got {other:?}"
                    ))
                }
            };
            let params = match eval_bytecode_operand(params, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode db_query_prepared_buf expects buf[u8] params, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::BufU8(portable_db_query_prepared_buf(
                handle, name, &params,
            )?))
        }
        BytecodeExpr::DbQueryPreparedRow {
            handle,
            name,
            params,
        } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode db_query_prepared_row expects u64 handle, got {other:?}"
                    ))
                }
            };
            let params = match eval_bytecode_operand(params, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode db_query_prepared_row expects buf[u8] params, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::BufU8(portable_db_query_prepared_row(
                handle, name, &params,
            )?))
        }
        BytecodeExpr::DbRowFound { row } => {
            let row = match eval_bytecode_operand(row, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => return Err(format!("bytecode db_row_found expects buf[u8], got {other:?}")),
            };
            Ok(RuntimeValue::Bool(!row.is_empty()))
        }
        BytecodeExpr::DbLastErrorCode { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode db_last_error_code expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U32(portable_db_last_error_code(handle)?))
        }
        BytecodeExpr::DbLastErrorRetryable { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode db_last_error_retryable expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_db_last_error_retryable(handle)?))
        }
        BytecodeExpr::DbBegin { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode db_begin expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_db_begin(handle)?))
        }
        BytecodeExpr::DbCommit { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode db_commit expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_db_commit(handle)?))
        }
        BytecodeExpr::DbRollback { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode db_rollback expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_db_rollback(handle)?))
        }
        BytecodeExpr::DbPoolOpen { target, max_size } => {
            let max_size = match eval_bytecode_operand(max_size, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "bytecode db_pool_open expects u32 max size, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U64(portable_db_pool_open(target, max_size)?))
        }
        BytecodeExpr::DbPoolSetMaxIdle { pool, value } => {
            let pool = match eval_bytecode_operand(pool, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode db_pool_set_max_idle expects u64 pool, got {other:?}"
                    ))
                }
            };
            let value = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "bytecode db_pool_set_max_idle expects u32 max idle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_db_pool_set_max_idle(pool, value)?))
        }
        BytecodeExpr::DbPoolLeased { pool } => {
            let pool = match eval_bytecode_operand(pool, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode db_pool_leased expects u64 pool, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U32(portable_db_pool_leased(pool)?))
        }
        BytecodeExpr::DbPoolAcquire { pool } => {
            let pool = match eval_bytecode_operand(pool, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode db_pool_acquire expects u64 pool, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U64(portable_db_pool_acquire(pool)?))
        }
        BytecodeExpr::DbPoolRelease { pool, handle } => {
            let pool = match eval_bytecode_operand(pool, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode db_pool_release expects u64 pool, got {other:?}"
                    ))
                }
            };
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode db_pool_release expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_db_pool_release(pool, handle)?))
        }
        BytecodeExpr::DbPoolClose { pool } => {
            let pool = match eval_bytecode_operand(pool, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode db_pool_close expects u64 pool, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_db_pool_close(pool)?))
        }
        BytecodeExpr::CacheOpen { target } => Ok(RuntimeValue::U64(portable_cache_open(target)?)),
        BytecodeExpr::CacheClose { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode cache_close expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_cache_close(handle)?))
        }
        BytecodeExpr::CacheGetBufU8 { handle, key } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode cache_get_buf expects u64 handle, got {other:?}"
                    ))
                }
            };
            let key = match eval_bytecode_operand(key, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode cache_get_buf expects buf[u8] key, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::BufU8(portable_cache_get_buf(handle, &key)?))
        }
        BytecodeExpr::CacheSetBufU8 { handle, key, value } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode cache_set_buf expects u64 handle, got {other:?}"
                    ))
                }
            };
            let key = match eval_bytecode_operand(key, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode cache_set_buf expects buf[u8] key, got {other:?}"
                    ))
                }
            };
            let value = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode cache_set_buf expects buf[u8] value, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_cache_set_buf(handle, &key, &value, None)?))
        }
        BytecodeExpr::CacheSetBufTtlU8 {
            handle,
            key,
            ttl_ms,
            value,
        } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode cache_set_buf_ttl expects u64 handle, got {other:?}"
                    ))
                }
            };
            let key = match eval_bytecode_operand(key, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode cache_set_buf_ttl expects buf[u8] key, got {other:?}"
                    ))
                }
            };
            let ttl_ms = match eval_bytecode_operand(ttl_ms, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "bytecode cache_set_buf_ttl expects u32 ttl, got {other:?}"
                    ))
                }
            };
            let value = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode cache_set_buf_ttl expects buf[u8] value, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_cache_set_buf(
                handle,
                &key,
                &value,
                Some(ttl_ms),
            )?))
        }
        BytecodeExpr::CacheDel { handle, key } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode cache_del expects u64 handle, got {other:?}"
                    ))
                }
            };
            let key = match eval_bytecode_operand(key, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode cache_del expects buf[u8] key, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_cache_del(handle, &key)?))
        }
        BytecodeExpr::QueueOpen { target } => Ok(RuntimeValue::U64(portable_queue_open(target)?)),
        BytecodeExpr::QueueClose { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode queue_close expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_queue_close(handle)?))
        }
        BytecodeExpr::QueuePushBufU8 { handle, value } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode queue_push_buf expects u64 handle, got {other:?}"
                    ))
                }
            };
            let value = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode queue_push_buf expects buf[u8] payload, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_queue_push_buf(handle, &value)?))
        }
        BytecodeExpr::QueuePopBufU8 { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode queue_pop_buf expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::BufU8(portable_queue_pop_buf(handle)?))
        }
        BytecodeExpr::QueueLen { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode queue_len expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U32(portable_queue_len(handle)?))
        }
        BytecodeExpr::StreamOpen { target } => Ok(RuntimeValue::U64(portable_stream_open(target)?)),
        BytecodeExpr::StreamClose { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode stream_close expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_stream_close(handle)?))
        }
        BytecodeExpr::StreamPublishBufU8 { handle, value } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode stream_publish_buf expects u64 handle, got {other:?}"
                    ))
                }
            };
            let value = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode stream_publish_buf expects buf[u8] payload, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U32(portable_stream_publish_buf(handle, &value)?))
        }
        BytecodeExpr::StreamLen { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode stream_len expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U32(portable_stream_len(handle)?))
        }
        BytecodeExpr::StreamReplayOpen { handle, offset } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode stream_replay_open expects u64 handle, got {other:?}"
                    ))
                }
            };
            let offset = match eval_bytecode_operand(offset, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "bytecode stream_replay_open expects u32 offset, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U64(portable_stream_replay_open(handle, offset)?))
        }
        BytecodeExpr::StreamReplayNextU8 { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode stream_replay_next expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::BufU8(portable_stream_replay_next(handle)?))
        }
        BytecodeExpr::StreamReplayOffset { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode stream_replay_offset expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U32(portable_stream_replay_offset(handle)?))
        }
        BytecodeExpr::StreamReplayClose { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode stream_replay_close expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_stream_replay_close(handle)?))
        }
        BytecodeExpr::ShardRouteU32 { key, shard_count } => {
            let key = match eval_bytecode_operand(key, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode shard_route_u32 expects buf[u8] key, got {other:?}"
                    ))
                }
            };
            let shard_count = match eval_bytecode_operand(shard_count, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "bytecode shard_route_u32 expects u32 shard_count, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U32(portable_shard_route_u32(&key, shard_count)?))
        }
        BytecodeExpr::LeaseOpen { target } => Ok(RuntimeValue::U64(portable_lease_open(target)?)),
        BytecodeExpr::LeaseAcquire { handle, owner } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode lease_acquire expects u64 handle, got {other:?}"
                    ))
                }
            };
            let owner = match eval_bytecode_operand(owner, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!("bytecode lease_acquire expects u32 owner, got {other:?}"))
                }
            };
            Ok(RuntimeValue::Bool(portable_lease_acquire(handle, owner)?))
        }
        BytecodeExpr::LeaseOwner { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!("bytecode lease_owner expects u64 handle, got {other:?}"))
                }
            };
            Ok(RuntimeValue::U32(portable_lease_owner(handle)?))
        }
        BytecodeExpr::LeaseTransfer { handle, owner } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode lease_transfer expects u64 handle, got {other:?}"
                    ))
                }
            };
            let owner = match eval_bytecode_operand(owner, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "bytecode lease_transfer expects u32 owner, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_lease_transfer(handle, owner)?))
        }
        BytecodeExpr::LeaseRelease { handle, owner } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode lease_release expects u64 handle, got {other:?}"
                    ))
                }
            };
            let owner = match eval_bytecode_operand(owner, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "bytecode lease_release expects u32 owner, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_lease_release(handle, owner)?))
        }
        BytecodeExpr::LeaseClose { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!("bytecode lease_close expects u64 handle, got {other:?}"))
                }
            };
            Ok(RuntimeValue::Bool(portable_lease_close(handle)?))
        }
        BytecodeExpr::PlacementOpen { target } => {
            Ok(RuntimeValue::U64(portable_placement_open(target)?))
        }
        BytecodeExpr::PlacementAssign {
            handle,
            shard,
            node,
        } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode placement_assign expects u64 handle, got {other:?}"
                    ))
                }
            };
            let shard = match eval_bytecode_operand(shard, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "bytecode placement_assign expects u32 shard, got {other:?}"
                    ))
                }
            };
            let node = match eval_bytecode_operand(node, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "bytecode placement_assign expects u32 node, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_placement_assign(
                handle, shard, node,
            )?))
        }
        BytecodeExpr::PlacementLookup { handle, shard } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode placement_lookup expects u64 handle, got {other:?}"
                    ))
                }
            };
            let shard = match eval_bytecode_operand(shard, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "bytecode placement_lookup expects u32 shard, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U32(portable_placement_lookup(handle, shard)?))
        }
        BytecodeExpr::PlacementClose { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode placement_close expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_placement_close(handle)?))
        }
        BytecodeExpr::CoordOpen { target } => Ok(RuntimeValue::U64(portable_coord_open(target)?)),
        BytecodeExpr::CoordStoreU32 { handle, key, value } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode coord_store_u32 expects u64 handle, got {other:?}"
                    ))
                }
            };
            let value = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "bytecode coord_store_u32 expects u32 value, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_coord_store_u32(
                handle, key, value,
            )?))
        }
        BytecodeExpr::CoordLoadU32 { handle, key } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode coord_load_u32 expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U32(portable_coord_load_u32(handle, key)?))
        }
        BytecodeExpr::CoordClose { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!("bytecode coord_close expects u64 handle, got {other:?}"))
                }
            };
            Ok(RuntimeValue::Bool(portable_coord_close(handle)?))
        }
        BytecodeExpr::BatchOpen => Ok(RuntimeValue::U64(portable_batch_open()?)),
        BytecodeExpr::BatchPushU64 { handle, value } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode batch_push_u64 expects u64 handle, got {other:?}"
                    ))
                }
            };
            let value = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode batch_push_u64 expects u64 value, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_batch_push_u64(handle, value)?))
        }
        BytecodeExpr::BatchLen { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode batch_len expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U32(portable_batch_len(handle)?))
        }
        BytecodeExpr::BatchFlushSumU64 { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode batch_flush_sum_u64 expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U64(portable_batch_flush_sum_u64(handle)?))
        }
        BytecodeExpr::BatchClose { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode batch_close expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_batch_close(handle)?))
        }
        BytecodeExpr::AggOpenU64 => Ok(RuntimeValue::U64(portable_agg_open_u64()?)),
        BytecodeExpr::AggAddU64 { handle, value } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode agg_add_u64 expects u64 handle, got {other:?}"
                    ))
                }
            };
            let value = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode agg_add_u64 expects u64 value, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_agg_add_u64(handle, value)?))
        }
        BytecodeExpr::AggCount { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode agg_count expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U32(portable_agg_count(handle)?))
        }
        BytecodeExpr::AggSumU64 { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode agg_sum_u64 expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U64(portable_agg_sum_u64(handle)?))
        }
        BytecodeExpr::AggAvgU64 { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode agg_avg_u64 expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U64(portable_agg_avg_u64(handle)?))
        }
        BytecodeExpr::AggMinU64 { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode agg_min_u64 expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U64(portable_agg_min_u64(handle)?))
        }
        BytecodeExpr::AggMaxU64 { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode agg_max_u64 expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U64(portable_agg_max_u64(handle)?))
        }
        BytecodeExpr::AggClose { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode agg_close expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_agg_close(handle)?))
        }
        BytecodeExpr::WindowOpenMs { width_ms } => {
            let width_ms = match eval_bytecode_operand(width_ms, slots)? {
                RuntimeValue::U32(value) => value,
                other => {
                    return Err(format!(
                        "bytecode window_open_ms expects u32 width, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U64(portable_window_open_ms(width_ms)?))
        }
        BytecodeExpr::WindowAddU64 { handle, value } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode window_add_u64 expects u64 handle, got {other:?}"
                    ))
                }
            };
            let value = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode window_add_u64 expects u64 value, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_window_add_u64(handle, value)?))
        }
        BytecodeExpr::WindowCount { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode window_count expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U32(portable_window_count(handle)?))
        }
        BytecodeExpr::WindowSumU64 { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode window_sum_u64 expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U64(portable_window_sum_u64(handle)?))
        }
        BytecodeExpr::WindowAvgU64 { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode window_avg_u64 expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U64(portable_window_avg_u64(handle)?))
        }
        BytecodeExpr::WindowMinU64 { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode window_min_u64 expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U64(portable_window_min_u64(handle)?))
        }
        BytecodeExpr::WindowMaxU64 { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode window_max_u64 expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U64(portable_window_max_u64(handle)?))
        }
        BytecodeExpr::WindowClose { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode window_close expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_window_close(handle)?))
        }
        BytecodeExpr::TlsExchangeAllU8 { host, port, value } => {
            let value = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode tls_exchange_all expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::BufU8(portable_tls_exchange_all(
                host, *port, &value,
            )?))
        }
        BytecodeExpr::TaskSleepMs { value } => {
            let millis = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::U32(value) => value,
                other => return Err(format!("bytecode task_sleep_ms expects u32, got {other:?}")),
            };
            Ok(RuntimeValue::Bool(portable_task_sleep_ms(millis)))
        }
        BytecodeExpr::TaskOpen { command, argv, env } => {
            Ok(RuntimeValue::U64(portable_task_open(command, argv, env)?))
        }
        BytecodeExpr::TaskDone { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode task_done expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_task_done(handle)?))
        }
        BytecodeExpr::TaskJoinStatus { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode task_join expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::I32(portable_task_join(handle)?))
        }
        BytecodeExpr::TaskStdoutAllU8 { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode task_stdout_all expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::BufU8(portable_task_stdout_all(handle)?))
        }
        BytecodeExpr::TaskStderrAllU8 { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode task_stderr_all expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::BufU8(portable_task_stderr_all(handle)?))
        }
        BytecodeExpr::TaskClose { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode task_close expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_task_close(handle)?))
        }
        BytecodeExpr::SpawnCaptureAllU8 { command, argv, env } => Ok(RuntimeValue::BufU8(
            portable_spawn_capture(command, argv, env, false)?,
        )),
        BytecodeExpr::SpawnCaptureStderrAllU8 { command, argv, env } => Ok(RuntimeValue::BufU8(
            portable_spawn_capture(command, argv, env, true)?,
        )),
        BytecodeExpr::SpawnCall { command, argv, env } => Ok(RuntimeValue::I32(
            portable_spawn_status(command, argv, env)?,
        )),
        BytecodeExpr::SpawnOpen { command, argv, env } => {
            Ok(RuntimeValue::U64(portable_spawn_open(command, argv, env)?))
        }
        BytecodeExpr::SpawnWait { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode spawn_wait expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::I32(portable_spawn_wait(handle)?))
        }
        BytecodeExpr::SpawnStdoutAllU8 { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode spawn_stdout_all expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::BufU8(portable_spawn_stdout_all(handle)?))
        }
        BytecodeExpr::SpawnStderrAllU8 { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode spawn_stderr_all expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::BufU8(portable_spawn_stderr_all(handle)?))
        }
        BytecodeExpr::SpawnStdinWriteAllU8 { handle, value } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode spawn_stdin_write_all expects u64 handle, got {other:?}"
                    ))
                }
            };
            let value = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode spawn_stdin_write_all expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_spawn_stdin_write_all(
                handle, &value,
            )?))
        }
        BytecodeExpr::SpawnStdinClose { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode spawn_stdin_close expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_spawn_stdin_close(handle)?))
        }
        BytecodeExpr::SpawnDone { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode spawn_done expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_spawn_done(handle)?))
        }
        BytecodeExpr::SpawnExitOk { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode spawn_exit_ok expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_spawn_exit_ok(handle)?))
        }
        BytecodeExpr::SpawnKill { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode spawn_kill expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_spawn_kill(handle)?))
        }
        BytecodeExpr::SpawnClose { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode spawn_close expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_spawn_close(handle)?))
        }
        BytecodeExpr::NetConnect { host, port } => {
            Ok(RuntimeValue::Bool(portable_net_connect_ok(host, *port)?))
        }
        BytecodeExpr::FfiCall {
            symbol,
            args,
            ret_kind,
        } => {
            let mut values = Vec::new();
            for operand in args {
                values.push(eval_bytecode_operand(operand, slots)?);
            }
            portable_ffi_call(symbol, &values, ret_kind.c_type_name())
        }
        BytecodeExpr::FfiCallCStr {
            symbol,
            arg_slot,
            ret_kind,
        } => {
            let value = match slots.get(*arg_slot).and_then(|value| value.as_ref()) {
                Some(RuntimeValue::BufU8(value)) => value.clone(),
                Some(other) => {
                    return Err(format!(
                        "bytecode ffi_call_cstr expects buf[u8], got {other:?}"
                    ))
                }
                None => return Err(format!("uninitialized bytecode source slot {}", arg_slot)),
            };
            portable_ffi_call_cstr(symbol, &value, ret_kind.c_type_name())
        }
        BytecodeExpr::FfiOpenLib { path } => Ok(RuntimeValue::U64(portable_ffi_open_lib(path)?)),
        BytecodeExpr::FfiCloseLib { handle } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode ffi_close_lib expects u64 handle, got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::Bool(portable_ffi_close_lib(handle)?))
        }
        BytecodeExpr::FfiBufPtr { value } => {
            let value = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::BufU8(value) => value,
                other => {
                    return Err(format!(
                        "bytecode ffi_buf_ptr expects buf[u8], got {other:?}"
                    ))
                }
            };
            Ok(RuntimeValue::U64(value.as_ptr() as usize as u64))
        }
        BytecodeExpr::FfiCallLib {
            handle,
            symbol,
            args,
            ret_kind,
        } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode ffi_call_lib expects u64 handle, got {other:?}"
                    ))
                }
            };
            let mut values = Vec::new();
            for operand in args {
                values.push(eval_bytecode_operand(operand, slots)?);
            }
            portable_ffi_call_lib(handle, symbol, &values, ret_kind.c_type_name())
        }
        BytecodeExpr::FfiCallLibCStr {
            handle,
            symbol,
            arg_slot,
            ret_kind,
        } => {
            let handle = match eval_bytecode_operand(handle, slots)? {
                RuntimeValue::U64(value) => value,
                other => {
                    return Err(format!(
                        "bytecode ffi_call_lib_cstr expects u64 handle, got {other:?}"
                    ))
                }
            };
            let value = match slots.get(*arg_slot).and_then(|value| value.as_ref()) {
                Some(RuntimeValue::BufU8(value)) => value.clone(),
                Some(other) => {
                    return Err(format!(
                        "bytecode ffi_call_lib_cstr expects buf[u8], got {other:?}"
                    ))
                }
                None => return Err(format!("uninitialized bytecode source slot {}", arg_slot)),
            };
            portable_ffi_call_lib_cstr(handle, symbol, &value, ret_kind.c_type_name())
        }
        BytecodeExpr::LenSpanI32 { source } => {
            match slots.get(*source).and_then(|value| value.as_ref()) {
                Some(RuntimeValue::SpanI32(values)) => Ok(RuntimeValue::U32(values.len() as u32)),
                Some(other) => Err(format!("bytecode len source must be span, got {other:?}")),
                None => Err(format!("uninitialized bytecode source slot {}", source)),
            }
        }
        BytecodeExpr::LenBufU8 { source } => {
            match slots.get(*source).and_then(|value| value.as_ref()) {
                Some(RuntimeValue::BufU8(values)) => Ok(RuntimeValue::U32(values.len() as u32)),
                Some(other) => Err(format!(
                    "bytecode len source must be buf[u8], got {other:?}"
                )),
                None => Err(format!("uninitialized bytecode source slot {}", source)),
            }
        }
        BytecodeExpr::StoreBufU8 {
            source,
            index,
            value,
        } => {
            let mut values = match slots.get(*source).and_then(|value| value.as_ref()) {
                Some(RuntimeValue::BufU8(values)) => values.clone(),
                Some(other) => {
                    return Err(format!(
                        "bytecode store source must be buf[u8], got {other:?}"
                    ))
                }
                None => return Err(format!("uninitialized bytecode source slot {}", source)),
            };
            let offset = match eval_bytecode_operand(index, slots)? {
                RuntimeValue::U32(value) => value as usize,
                RuntimeValue::I32(value) if value >= 0 => value as usize,
                other => {
                    return Err(format!(
                        "bytecode store index must be non-negative integer, got {other:?}"
                    ))
                }
            };
            let byte = match eval_bytecode_operand(value, slots)? {
                RuntimeValue::U8(value) => value,
                RuntimeValue::U32(value) if value <= u8::MAX as u32 => value as u8,
                other => return Err(format!("bytecode store value must be u8, got {other:?}")),
            };
            if offset >= values.len() {
                return Err(format!("bytecode store index {} out of bounds", offset));
            }
            values[offset] = byte;
            Ok(RuntimeValue::BufU8(values))
        }
        BytecodeExpr::LoadBufU8 { source, index } => {
            let values = match slots.get(*source).and_then(|value| value.as_ref()) {
                Some(RuntimeValue::BufU8(values)) => values,
                Some(other) => {
                    return Err(format!(
                        "bytecode load source must be buf[u8], got {other:?}"
                    ))
                }
                None => return Err(format!("uninitialized bytecode source slot {}", source)),
            };
            let offset = match eval_bytecode_operand(index, slots)? {
                RuntimeValue::U32(value) => value as usize,
                RuntimeValue::I32(value) if value >= 0 => value as usize,
                other => {
                    return Err(format!(
                        "bytecode load index must be non-negative integer, got {other:?}"
                    ))
                }
            };
            values
                .get(offset)
                .copied()
                .map(RuntimeValue::U8)
                .ok_or_else(|| format!("bytecode load index {} out of bounds", offset))
        }
        BytecodeExpr::LoadSpanI32 { source, index } => {
            let values = match slots.get(*source).and_then(|value| value.as_ref()) {
                Some(RuntimeValue::SpanI32(values)) => values,
                Some(other) => {
                    return Err(format!("bytecode load source must be span, got {other:?}"))
                }
                None => return Err(format!("uninitialized bytecode source slot {}", source)),
            };
            let index = eval_bytecode_operand(index, slots)?;
            let offset = match index {
                RuntimeValue::U32(value) => value as usize,
                RuntimeValue::I32(value) if value >= 0 => value as usize,
                other => {
                    return Err(format!(
                        "bytecode load index must be non-negative integer, got {other:?}"
                    ))
                }
            };
            values
                .get(offset)
                .copied()
                .map(RuntimeValue::I32)
                .ok_or_else(|| format!("bytecode load index {} out of bounds", offset))
        }
        BytecodeExpr::AbsI32 { value } => match eval_bytecode_operand(value, slots)? {
            RuntimeValue::I32(value) => Ok(RuntimeValue::I32(value.abs())),
            other => Err(format!("bytecode abs expects i32, got {other:?}")),
        },
        BytecodeExpr::Binary { op, left, right } => {
            let left = eval_bytecode_operand(left, slots)?;
            let right = eval_bytecode_operand(right, slots)?;
            eval_binary(op, &left, &right)
        }
        BytecodeExpr::SextI64 { value } => match eval_bytecode_operand(value, slots)? {
            RuntimeValue::I32(value) => Ok(RuntimeValue::I64(value as i64)),
            other => Err(format!(
                "bytecode sext i64 expects i32 source, got {other:?}"
            )),
        },
    }
}

fn eval_bytecode_operand(
    operand: &BytecodeOperand,
    slots: &[Option<RuntimeValue>],
) -> Result<RuntimeValue, String> {
    match operand {
        BytecodeOperand::Slot { index, .. } => slots
            .get(*index)
            .and_then(|value| value.clone())
            .ok_or_else(|| format!("uninitialized bytecode slot {}", index)),
        BytecodeOperand::Imm(immediate) => Ok(match immediate {
            BytecodeImmediate::U8(value) => RuntimeValue::U8(*value),
            BytecodeImmediate::I32(value) => RuntimeValue::I32(*value),
            BytecodeImmediate::I64(value) => RuntimeValue::I64(*value),
            BytecodeImmediate::U64(value) => RuntimeValue::U64(*value),
            BytecodeImmediate::U32(value) => RuntimeValue::U32(*value),
            BytecodeImmediate::Bool(value) => RuntimeValue::Bool(*value),
        }),
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
        (LoweredExecBinaryOp::Lt, RuntimeValue::U32(left), RuntimeValue::U32(right)) => {
            Ok(RuntimeValue::Bool(left < right))
        }
        (LoweredExecBinaryOp::Lt, RuntimeValue::U8(left), RuntimeValue::U8(right)) => {
            Ok(RuntimeValue::Bool(left < right))
        }
        (LoweredExecBinaryOp::Lt, RuntimeValue::I64(left), RuntimeValue::I64(right)) => {
            Ok(RuntimeValue::Bool(left < right))
        }
        (LoweredExecBinaryOp::Lt, RuntimeValue::U64(left), RuntimeValue::U64(right)) => {
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
            "unsupported bytecode binary operation {op:?} for {left:?} and {right:?}"
        )),
    }
}

fn bytecode_kind_for_c_type(c_type: &str) -> Result<BytecodeValueKind, String> {
    match c_type {
        "uint8_t" => Ok(BytecodeValueKind::U8),
        "int32_t" => Ok(BytecodeValueKind::I32),
        "int64_t" => Ok(BytecodeValueKind::I64),
        "uint64_t" => Ok(BytecodeValueKind::U64),
        "uint32_t" => Ok(BytecodeValueKind::U32),
        "_Bool" | "bool" => Ok(BytecodeValueKind::Bool),
        "mira_span_i32" | "span_i32" => Ok(BytecodeValueKind::SpanI32),
        "buf_u8" => Ok(BytecodeValueKind::BufU8),
        other => Err(format!("unsupported bytecode c type {other}")),
    }
}

impl BytecodeValueKind {
    fn c_type_name(self) -> &'static str {
        match self {
            BytecodeValueKind::U8 => "uint8_t",
            BytecodeValueKind::I32 => "int32_t",
            BytecodeValueKind::I64 => "int64_t",
            BytecodeValueKind::U64 => "uint64_t",
            BytecodeValueKind::U32 => "uint32_t",
            BytecodeValueKind::Bool => "bool",
            BytecodeValueKind::SpanI32 => "span_i32",
            BytecodeValueKind::BufU8 => "buf_u8",
        }
    }
}

pub fn verify_lowered_tests_portably(program: &LoweredProgram) -> Result<Option<String>, String> {
    let bytecode = match compile_bytecode_program(program) {
        Ok(bytecode) => bytecode,
        Err(_) => return Ok(None),
    };
    let mut passed = 0usize;
    for test in &program.tests {
        let mut args = HashMap::new();
        for input in &test.inputs {
            let Ok(value) = runtime_value_from_data(&input.ty, &input.value) else {
                return Ok(None);
            };
            args.insert(input.name.clone(), value);
        }
        let expected = match runtime_value_from_data(&test.expected.ty, &test.expected.value) {
            Ok(value) => value,
            Err(_) => return Ok(None),
        };
        let _guard = runtime_execution_guard()
            .lock()
            .map_err(|_| "portable runtime execution mutex poisoned".to_string())?;
        reset_runtime_state()?;
        let actual = match with_lowered_program_context(program, || {
            run_bytecode_function_inner(&bytecode, &test.function_name, &args)
        }) {
            Ok(actual) => actual,
            Err(error) if error.starts_with("unsupported portable ") => return Ok(None),
            Err(error) => return Err(error),
        };
        if actual != expected {
            return Err(format!(
                "portable bytecode test {}.{} failed: expected {:?}, got {:?}",
                test.owner, test.name, expected, actual
            ));
        }
        passed += 1;
    }
    Ok(Some(format!(
        "portable bytecode tests passed: {passed}/{}",
        program.tests.len()
    )))
}
