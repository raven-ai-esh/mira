use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde::{Deserialize, Serialize};

use crate::ast::{Block, Function, Program, Target, Terminator, TestCase, TypeDeclBody};
use crate::types::{
    parse_data_literal, render_c_literal, render_data_value, sanitize_identifier, DataValue,
    NamedFieldValue, TypeRef,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoweredProgram {
    pub module: String,
    pub preamble: String,
    pub functions: Vec<LoweredFunction>,
    pub tests: Vec<LoweredTest>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoweredTest {
    pub owner: String,
    pub name: String,
    pub function_name: String,
    pub inputs: Vec<LoweredTestInput>,
    pub expected: LoweredTestExpected,
    pub declarations: Vec<LoweredVarDecl>,
    pub call: LoweredCall,
    pub assertion: LoweredAssertion,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoweredTestInput {
    pub name: String,
    pub ty: TypeRef,
    pub value: DataValue,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoweredTestExpected {
    pub ty: TypeRef,
    pub value: DataValue,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoweredFunction {
    pub name: String,
    pub ret_c_type: String,
    pub args: Vec<(String, String)>,
    pub declarations: Vec<(String, String)>,
    pub uses_arena: bool,
    pub rand_seed: Option<u32>,
    pub blocks: Vec<LoweredBlock>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoweredBlock {
    pub label: String,
    pub statements: Vec<LoweredStatement>,
    pub terminator: LoweredTerminator,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LoweredStorageClass {
    Auto,
    Static,
    StaticConst,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoweredVarDecl {
    pub storage: LoweredStorageClass,
    pub c_type: String,
    pub name: String,
    pub init: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoweredAssignment {
    pub target: String,
    pub expr: String,
    pub exec_expr: Option<LoweredExecExpr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoweredEdge {
    pub assignments: Vec<LoweredAssignment>,
    pub label: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoweredMatchCase {
    pub tag_index: usize,
    pub edge: LoweredEdge,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LoweredStatement {
    Assign(LoweredAssignment),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LoweredTerminator {
    Return {
        expr: String,
        release_arena: bool,
        exec_value: Option<LoweredExecOperand>,
    },
    Jump {
        edge: LoweredEdge,
    },
    Branch {
        condition: String,
        truthy: LoweredEdge,
        falsy: LoweredEdge,
        exec_condition: Option<LoweredExecOperand>,
    },
    Match {
        value: String,
        cases: Vec<LoweredMatchCase>,
        default: LoweredEdge,
        exec_value: Option<LoweredExecOperand>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LoweredExecImmediate {
    U8(u8),
    I32(i32),
    I64(i64),
    U64(u64),
    U32(u32),
    Bool(bool),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LoweredExecOperand {
    Binding(String),
    Immediate(LoweredExecImmediate),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LoweredExecBinaryOp {
    Add,
    Sub,
    Mul,
    Band,
    Bor,
    Bxor,
    Shl,
    Shr,
    Eq,
    Lt,
    Le,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LoweredExecExpr {
    Move(LoweredExecOperand),
    BufLit {
        literal: String,
    },
    BufConcat {
        left: LoweredExecOperand,
        right: LoweredExecOperand,
    },
    AllocBufU8 {
        region: String,
        len: LoweredExecOperand,
    },
    DropBufU8 {
        value: LoweredExecOperand,
    },
    ClockNowNs,
    RandU32,
    FsReadU32 {
        path: String,
    },
    FsWriteU32 {
        path: String,
        value: LoweredExecOperand,
    },
    FsReadAllU8 {
        path: String,
    },
    FsWriteAllU8 {
        path: String,
        value: LoweredExecOperand,
    },
    NetWriteAllU8 {
        host: String,
        port: u16,
        value: LoweredExecOperand,
    },
    NetExchangeAllU8 {
        host: String,
        port: u16,
        value: LoweredExecOperand,
    },
    NetServeExchangeAllU8 {
        host: String,
        port: u16,
        response: LoweredExecOperand,
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
        listener: LoweredExecOperand,
    },
    NetSessionOpen {
        host: String,
        port: u16,
    },
    HttpSessionAccept {
        listener: LoweredExecOperand,
    },
    NetReadAllU8 {
        handle: LoweredExecOperand,
    },
    SessionReadChunkU8 {
        handle: LoweredExecOperand,
        chunk_size: LoweredExecOperand,
    },
    HttpSessionRequest {
        handle: LoweredExecOperand,
    },
    NetWriteHandleAllU8 {
        handle: LoweredExecOperand,
        value: LoweredExecOperand,
    },
    SessionWriteChunkU8 {
        handle: LoweredExecOperand,
        value: LoweredExecOperand,
    },
    SessionFlush {
        handle: LoweredExecOperand,
    },
    SessionAlive {
        handle: LoweredExecOperand,
    },
    SessionHeartbeatU8 {
        handle: LoweredExecOperand,
        value: LoweredExecOperand,
    },
    SessionBackpressure {
        handle: LoweredExecOperand,
    },
    SessionBackpressureWait {
        handle: LoweredExecOperand,
        max_pending: LoweredExecOperand,
    },
    SessionResumeId {
        handle: LoweredExecOperand,
    },
    SessionReconnect {
        handle: LoweredExecOperand,
    },
    NetClose {
        handle: LoweredExecOperand,
    },
    HttpSessionClose {
        handle: LoweredExecOperand,
    },
    HttpMethodEq {
        request: LoweredExecOperand,
        method: String,
    },
    HttpPathEq {
        request: LoweredExecOperand,
        path: String,
    },
    HttpRequestMethod {
        request: LoweredExecOperand,
    },
    HttpRequestPath {
        request: LoweredExecOperand,
    },
    HttpRouteParam {
        request: LoweredExecOperand,
        pattern: String,
        param: String,
    },
    HttpHeaderEq {
        request: LoweredExecOperand,
        name: String,
        value: String,
    },
    HttpCookieEq {
        request: LoweredExecOperand,
        name: String,
        value: String,
    },
    HttpStatusU32 {
        value: LoweredExecOperand,
    },
    BufEqLit {
        value: LoweredExecOperand,
        literal: String,
    },
    BufContainsLit {
        value: LoweredExecOperand,
        literal: String,
    },
    HttpHeader {
        request: LoweredExecOperand,
        name: String,
    },
    HttpHeaderCount {
        request: LoweredExecOperand,
    },
    HttpHeaderName {
        request: LoweredExecOperand,
        index: LoweredExecOperand,
    },
    HttpHeaderValue {
        request: LoweredExecOperand,
        index: LoweredExecOperand,
    },
    HttpCookie {
        request: LoweredExecOperand,
        name: String,
    },
    HttpQueryParam {
        request: LoweredExecOperand,
        key: String,
    },
    HttpBody {
        request: LoweredExecOperand,
    },
    HttpMultipartPartCount {
        request: LoweredExecOperand,
    },
    HttpMultipartPartName {
        request: LoweredExecOperand,
        index: LoweredExecOperand,
    },
    HttpMultipartPartFilename {
        request: LoweredExecOperand,
        index: LoweredExecOperand,
    },
    HttpMultipartPartBody {
        request: LoweredExecOperand,
        index: LoweredExecOperand,
    },
    HttpBodyStreamOpen {
        request: LoweredExecOperand,
    },
    HttpBodyStreamNext {
        handle: LoweredExecOperand,
        chunk_size: LoweredExecOperand,
    },
    HttpBodyStreamClose {
        handle: LoweredExecOperand,
    },
    HttpBodyLimit {
        request: LoweredExecOperand,
        limit: LoweredExecOperand,
    },
    HttpServerConfigU32 {
        token: String,
    },
    TlsServerConfigU32 {
        token: String,
        value: u32,
    },
    TlsServerConfigBuf {
        token: String,
        value: String,
    },
    ListenerSetTimeoutMs {
        handle: LoweredExecOperand,
        value: LoweredExecOperand,
    },
    SessionSetTimeoutMs {
        handle: LoweredExecOperand,
        value: LoweredExecOperand,
    },
    ListenerSetShutdownGraceMs {
        handle: LoweredExecOperand,
        value: LoweredExecOperand,
    },
    BufParseU32 {
        value: LoweredExecOperand,
    },
    BufParseBool {
        value: LoweredExecOperand,
    },
    HttpWriteResponse {
        handle: LoweredExecOperand,
        status: LoweredExecOperand,
        body: LoweredExecOperand,
    },
    HttpWriteTextResponse {
        handle: LoweredExecOperand,
        status: LoweredExecOperand,
        body: LoweredExecOperand,
    },
    HttpWriteTextResponseCookie {
        handle: LoweredExecOperand,
        status: LoweredExecOperand,
        cookie_name: String,
        cookie_value: String,
        body: LoweredExecOperand,
    },
    HttpWriteTextResponseHeaders2 {
        handle: LoweredExecOperand,
        status: LoweredExecOperand,
        header1_name: String,
        header1_value: String,
        header2_name: String,
        header2_value: String,
        body: LoweredExecOperand,
    },
    HttpSessionWriteText {
        handle: LoweredExecOperand,
        status: LoweredExecOperand,
        body: LoweredExecOperand,
    },
    HttpSessionWriteTextCookie {
        handle: LoweredExecOperand,
        status: LoweredExecOperand,
        cookie_name: String,
        cookie_value: String,
        body: LoweredExecOperand,
    },
    HttpSessionWriteTextHeaders2 {
        handle: LoweredExecOperand,
        status: LoweredExecOperand,
        header1_name: String,
        header1_value: String,
        header2_name: String,
        header2_value: String,
        body: LoweredExecOperand,
    },
    HttpWriteJsonResponse {
        handle: LoweredExecOperand,
        status: LoweredExecOperand,
        body: LoweredExecOperand,
    },
    HttpWriteJsonResponseCookie {
        handle: LoweredExecOperand,
        status: LoweredExecOperand,
        cookie_name: String,
        cookie_value: String,
        body: LoweredExecOperand,
    },
    HttpWriteJsonResponseHeaders2 {
        handle: LoweredExecOperand,
        status: LoweredExecOperand,
        header1_name: String,
        header1_value: String,
        header2_name: String,
        header2_value: String,
        body: LoweredExecOperand,
    },
    HttpSessionWriteJson {
        handle: LoweredExecOperand,
        status: LoweredExecOperand,
        body: LoweredExecOperand,
    },
    HttpSessionWriteJsonCookie {
        handle: LoweredExecOperand,
        status: LoweredExecOperand,
        cookie_name: String,
        cookie_value: String,
        body: LoweredExecOperand,
    },
    HttpSessionWriteJsonHeaders2 {
        handle: LoweredExecOperand,
        status: LoweredExecOperand,
        header1_name: String,
        header1_value: String,
        header2_name: String,
        header2_value: String,
        body: LoweredExecOperand,
    },
    HttpResponseStreamOpen {
        handle: LoweredExecOperand,
        status: LoweredExecOperand,
        content_type: String,
    },
    HttpResponseStreamWrite {
        handle: LoweredExecOperand,
        body: LoweredExecOperand,
    },
    HttpResponseStreamClose {
        handle: LoweredExecOperand,
    },
    HttpClientOpen {
        host: String,
        port: u16,
    },
    HttpClientRequest {
        handle: LoweredExecOperand,
        request: LoweredExecOperand,
    },
    HttpClientRequestRetry {
        handle: LoweredExecOperand,
        retries: LoweredExecOperand,
        backoff_ms: LoweredExecOperand,
        request: LoweredExecOperand,
    },
    HttpClientClose {
        handle: LoweredExecOperand,
    },
    HttpClientPoolOpen {
        host: String,
        port: u16,
        max_size: LoweredExecOperand,
    },
    HttpClientPoolAcquire {
        pool: LoweredExecOperand,
    },
    HttpClientPoolRelease {
        pool: LoweredExecOperand,
        handle: LoweredExecOperand,
    },
    HttpClientPoolClose {
        pool: LoweredExecOperand,
    },
    HttpWriteResponseHeader {
        handle: LoweredExecOperand,
        status: LoweredExecOperand,
        header_name: String,
        header_value: String,
        body: LoweredExecOperand,
    },
    JsonGetU32 {
        value: LoweredExecOperand,
        key: String,
    },
    JsonGetBool {
        value: LoweredExecOperand,
        key: String,
    },
    JsonHasKey {
        value: LoweredExecOperand,
        key: String,
    },
    JsonGetBufU8 {
        value: LoweredExecOperand,
        key: String,
    },
    JsonGetStr {
        value: LoweredExecOperand,
        key: String,
    },
    JsonGetU32Or {
        value: LoweredExecOperand,
        key: String,
        default_value: LoweredExecOperand,
    },
    JsonGetBoolOr {
        value: LoweredExecOperand,
        key: String,
        default_value: LoweredExecOperand,
    },
    JsonGetBufOr {
        value: LoweredExecOperand,
        key: String,
        default_value: LoweredExecOperand,
    },
    JsonGetStrOr {
        value: LoweredExecOperand,
        key: String,
        default_value: LoweredExecOperand,
    },
    JsonArrayLen {
        value: LoweredExecOperand,
    },
    JsonIndexU32 {
        value: LoweredExecOperand,
        index: LoweredExecOperand,
    },
    JsonIndexBool {
        value: LoweredExecOperand,
        index: LoweredExecOperand,
    },
    JsonIndexStr {
        value: LoweredExecOperand,
        index: LoweredExecOperand,
    },
    JsonEncodeObj {
        entries: Vec<(String, LoweredExecOperand)>,
    },
    JsonEncodeArr {
        values: Vec<LoweredExecOperand>,
    },
    StrLit {
        literal: String,
    },
    StrConcat {
        left: LoweredExecOperand,
        right: LoweredExecOperand,
    },
    StrFromU32 {
        value: LoweredExecOperand,
    },
    StrFromBool {
        value: LoweredExecOperand,
    },
    StrEqLit {
        value: LoweredExecOperand,
        literal: String,
    },
    StrToBuf {
        value: LoweredExecOperand,
    },
    BufToStr {
        value: LoweredExecOperand,
    },
    BufHexStr {
        value: LoweredExecOperand,
    },
    ConfigGetU32 {
        key: String,
        value: u32,
    },
    ConfigGetBool {
        key: String,
        value: bool,
    },
    ConfigGetStr {
        key: String,
        value: String,
    },
    ConfigHas {
        key: String,
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
        value: LoweredExecOperand,
        literal: String,
    },
    BufAfterLit {
        value: LoweredExecOperand,
        literal: String,
    },
    BufTrimAscii {
        value: LoweredExecOperand,
    },
    DateParseYmd {
        value: LoweredExecOperand,
    },
    TimeParseHms {
        value: LoweredExecOperand,
    },
    DateFormatYmd {
        value: LoweredExecOperand,
    },
    TimeFormatHms {
        value: LoweredExecOperand,
    },
    DbOpen {
        path: String,
    },
    DbClose {
        handle: LoweredExecOperand,
    },
    DbExec {
        handle: LoweredExecOperand,
        sql: LoweredExecOperand,
    },
    DbPrepare {
        handle: LoweredExecOperand,
        name: String,
        sql: LoweredExecOperand,
    },
    DbExecPrepared {
        handle: LoweredExecOperand,
        name: String,
        params: LoweredExecOperand,
    },
    DbQueryU32 {
        handle: LoweredExecOperand,
        sql: LoweredExecOperand,
    },
    DbQueryBufU8 {
        handle: LoweredExecOperand,
        sql: LoweredExecOperand,
    },
    DbQueryRow {
        handle: LoweredExecOperand,
        sql: LoweredExecOperand,
    },
    DbQueryPreparedU32 {
        handle: LoweredExecOperand,
        name: String,
        params: LoweredExecOperand,
    },
    DbQueryPreparedBufU8 {
        handle: LoweredExecOperand,
        name: String,
        params: LoweredExecOperand,
    },
    DbQueryPreparedRow {
        handle: LoweredExecOperand,
        name: String,
        params: LoweredExecOperand,
    },
    DbRowFound {
        row: LoweredExecOperand,
    },
    DbLastErrorCode {
        handle: LoweredExecOperand,
    },
    DbLastErrorRetryable {
        handle: LoweredExecOperand,
    },
    DbBegin {
        handle: LoweredExecOperand,
    },
    DbCommit {
        handle: LoweredExecOperand,
    },
    DbRollback {
        handle: LoweredExecOperand,
    },
    DbPoolOpen {
        target: String,
        max_size: LoweredExecOperand,
    },
    DbPoolSetMaxIdle {
        pool: LoweredExecOperand,
        value: LoweredExecOperand,
    },
    DbPoolLeased {
        pool: LoweredExecOperand,
    },
    DbPoolAcquire {
        pool: LoweredExecOperand,
    },
    DbPoolRelease {
        pool: LoweredExecOperand,
        handle: LoweredExecOperand,
    },
    DbPoolClose {
        pool: LoweredExecOperand,
    },
    CacheOpen {
        target: String,
    },
    CacheClose {
        handle: LoweredExecOperand,
    },
    CacheGetBufU8 {
        handle: LoweredExecOperand,
        key: LoweredExecOperand,
    },
    CacheSetBufU8 {
        handle: LoweredExecOperand,
        key: LoweredExecOperand,
        value: LoweredExecOperand,
    },
    CacheSetBufTtlU8 {
        handle: LoweredExecOperand,
        key: LoweredExecOperand,
        ttl_ms: LoweredExecOperand,
        value: LoweredExecOperand,
    },
    CacheDel {
        handle: LoweredExecOperand,
        key: LoweredExecOperand,
    },
    QueueOpen {
        target: String,
    },
    QueueClose {
        handle: LoweredExecOperand,
    },
    QueuePushBufU8 {
        handle: LoweredExecOperand,
        value: LoweredExecOperand,
    },
    QueuePopBufU8 {
        handle: LoweredExecOperand,
    },
    QueueLen {
        handle: LoweredExecOperand,
    },
    StreamOpen {
        target: String,
    },
    StreamClose {
        handle: LoweredExecOperand,
    },
    StreamPublishBufU8 {
        handle: LoweredExecOperand,
        value: LoweredExecOperand,
    },
    StreamLen {
        handle: LoweredExecOperand,
    },
    StreamReplayOpen {
        handle: LoweredExecOperand,
        from_offset: LoweredExecOperand,
    },
    StreamReplayNextU8 {
        handle: LoweredExecOperand,
    },
    StreamReplayOffset {
        handle: LoweredExecOperand,
    },
    StreamReplayClose {
        handle: LoweredExecOperand,
    },
    ShardRouteU32 {
        key: LoweredExecOperand,
        shard_count: LoweredExecOperand,
    },
    LeaseOpen {
        target: String,
    },
    LeaseAcquire {
        handle: LoweredExecOperand,
        owner: LoweredExecOperand,
    },
    LeaseOwner {
        handle: LoweredExecOperand,
    },
    LeaseTransfer {
        handle: LoweredExecOperand,
        owner: LoweredExecOperand,
    },
    LeaseRelease {
        handle: LoweredExecOperand,
        owner: LoweredExecOperand,
    },
    LeaseClose {
        handle: LoweredExecOperand,
    },
    PlacementOpen {
        target: String,
    },
    PlacementAssign {
        handle: LoweredExecOperand,
        shard: LoweredExecOperand,
        node: LoweredExecOperand,
    },
    PlacementLookup {
        handle: LoweredExecOperand,
        shard: LoweredExecOperand,
    },
    PlacementClose {
        handle: LoweredExecOperand,
    },
    CoordOpen {
        target: String,
    },
    CoordStoreU32 {
        handle: LoweredExecOperand,
        key: String,
        value: LoweredExecOperand,
    },
    CoordLoadU32 {
        handle: LoweredExecOperand,
        key: String,
    },
    CoordClose {
        handle: LoweredExecOperand,
    },
    BatchOpen,
    BatchPushU64 {
        handle: LoweredExecOperand,
        value: LoweredExecOperand,
    },
    BatchLen {
        handle: LoweredExecOperand,
    },
    BatchFlushSumU64 {
        handle: LoweredExecOperand,
    },
    BatchClose {
        handle: LoweredExecOperand,
    },
    AggOpenU64,
    AggAddU64 {
        handle: LoweredExecOperand,
        value: LoweredExecOperand,
    },
    AggCount {
        handle: LoweredExecOperand,
    },
    AggSumU64 {
        handle: LoweredExecOperand,
    },
    AggAvgU64 {
        handle: LoweredExecOperand,
    },
    AggMinU64 {
        handle: LoweredExecOperand,
    },
    AggMaxU64 {
        handle: LoweredExecOperand,
    },
    AggClose {
        handle: LoweredExecOperand,
    },
    WindowOpenMs {
        width_ms: LoweredExecOperand,
    },
    WindowAddU64 {
        handle: LoweredExecOperand,
        value: LoweredExecOperand,
    },
    WindowCount {
        handle: LoweredExecOperand,
    },
    WindowSumU64 {
        handle: LoweredExecOperand,
    },
    WindowAvgU64 {
        handle: LoweredExecOperand,
    },
    WindowMinU64 {
        handle: LoweredExecOperand,
    },
    WindowMaxU64 {
        handle: LoweredExecOperand,
    },
    WindowClose {
        handle: LoweredExecOperand,
    },
    MsgLogOpen,
    MsgLogClose {
        handle: LoweredExecOperand,
    },
    MsgSend {
        handle: LoweredExecOperand,
        conversation: String,
        recipient: String,
        payload: LoweredExecOperand,
    },
    MsgSendDedup {
        handle: LoweredExecOperand,
        conversation: String,
        recipient: String,
        dedup_key: LoweredExecOperand,
        payload: LoweredExecOperand,
    },
    MsgSubscribe {
        handle: LoweredExecOperand,
        room: String,
        recipient: String,
    },
    MsgSubscriberCount {
        handle: LoweredExecOperand,
        room: String,
    },
    MsgFanout {
        handle: LoweredExecOperand,
        room: String,
        payload: LoweredExecOperand,
    },
    MsgRecvNext {
        handle: LoweredExecOperand,
        recipient: String,
    },
    MsgRecvSeq {
        handle: LoweredExecOperand,
        recipient: String,
    },
    MsgAck {
        handle: LoweredExecOperand,
        recipient: String,
        seq: LoweredExecOperand,
    },
    MsgMarkRetry {
        handle: LoweredExecOperand,
        recipient: String,
        seq: LoweredExecOperand,
    },
    MsgRetryCount {
        handle: LoweredExecOperand,
        recipient: String,
        seq: LoweredExecOperand,
    },
    MsgPendingCount {
        handle: LoweredExecOperand,
        recipient: String,
    },
    MsgDeliveryTotal {
        handle: LoweredExecOperand,
        recipient: String,
    },
    MsgFailureClass {
        handle: LoweredExecOperand,
    },
    MsgReplayOpen {
        handle: LoweredExecOperand,
        recipient: String,
        from_seq: LoweredExecOperand,
    },
    MsgReplayNext {
        handle: LoweredExecOperand,
    },
    MsgReplaySeq {
        handle: LoweredExecOperand,
    },
    MsgReplayClose {
        handle: LoweredExecOperand,
    },
    ServiceOpen {
        name: String,
    },
    ServiceClose {
        handle: LoweredExecOperand,
    },
    ServiceShutdown {
        handle: LoweredExecOperand,
        grace_ms: LoweredExecOperand,
    },
    ServiceLog {
        handle: LoweredExecOperand,
        level: String,
        message: LoweredExecOperand,
    },
    ServiceTraceBegin {
        handle: LoweredExecOperand,
        name: String,
    },
    ServiceTraceEnd {
        trace: LoweredExecOperand,
    },
    ServiceMetricCount {
        handle: LoweredExecOperand,
        metric: String,
        value: LoweredExecOperand,
    },
    ServiceMetricCountDim {
        handle: LoweredExecOperand,
        metric: String,
        dimension: String,
        value: LoweredExecOperand,
    },
    ServiceMetricTotal {
        handle: LoweredExecOperand,
        metric: String,
    },
    ServiceHealthStatus {
        handle: LoweredExecOperand,
    },
    ServiceReadinessStatus {
        handle: LoweredExecOperand,
    },
    ServiceSetHealth {
        handle: LoweredExecOperand,
        status: LoweredExecOperand,
    },
    ServiceSetReadiness {
        handle: LoweredExecOperand,
        status: LoweredExecOperand,
    },
    ServiceSetDegraded {
        handle: LoweredExecOperand,
        degraded: LoweredExecOperand,
    },
    ServiceDegraded {
        handle: LoweredExecOperand,
    },
    ServiceEvent {
        handle: LoweredExecOperand,
        class: String,
        message: LoweredExecOperand,
    },
    ServiceEventTotal {
        handle: LoweredExecOperand,
        class: String,
    },
    ServiceTraceLink {
        trace: LoweredExecOperand,
        parent: LoweredExecOperand,
    },
    ServiceTraceLinkCount {
        handle: LoweredExecOperand,
    },
    ServiceFailureCount {
        handle: LoweredExecOperand,
        class: String,
        value: LoweredExecOperand,
    },
    ServiceFailureTotal {
        handle: LoweredExecOperand,
        class: String,
    },
    ServiceCheckpointSaveU32 {
        handle: LoweredExecOperand,
        key: String,
        value: LoweredExecOperand,
    },
    ServiceCheckpointLoadU32 {
        handle: LoweredExecOperand,
        key: String,
    },
    ServiceCheckpointExists {
        handle: LoweredExecOperand,
        key: String,
    },
    ServiceMigrateDb {
        handle: LoweredExecOperand,
        db_handle: LoweredExecOperand,
        migration: String,
    },
    ServiceRoute {
        request: LoweredExecOperand,
        method: String,
        path: String,
    },
    ServiceRequireHeader {
        request: LoweredExecOperand,
        name: String,
        value: String,
    },
    ServiceErrorStatus {
        kind: String,
    },
    TlsExchangeAllU8 {
        host: String,
        port: u16,
        value: LoweredExecOperand,
    },
    RtOpen {
        workers: LoweredExecOperand,
    },
    RtSpawnU32 {
        runtime: LoweredExecOperand,
        function: String,
        arg: LoweredExecOperand,
    },
    RtSpawnBufU8 {
        runtime: LoweredExecOperand,
        function: String,
        arg: LoweredExecOperand,
    },
    RtTrySpawnU32 {
        runtime: LoweredExecOperand,
        function: String,
        arg: LoweredExecOperand,
    },
    RtTrySpawnBufU8 {
        runtime: LoweredExecOperand,
        function: String,
        arg: LoweredExecOperand,
    },
    RtDone {
        task: LoweredExecOperand,
    },
    RtJoinU32 {
        task: LoweredExecOperand,
    },
    RtJoinBufU8 {
        task: LoweredExecOperand,
    },
    RtCancel {
        task: LoweredExecOperand,
    },
    RtTaskClose {
        task: LoweredExecOperand,
    },
    RtShutdown {
        runtime: LoweredExecOperand,
        grace_ms: LoweredExecOperand,
    },
    RtClose {
        runtime: LoweredExecOperand,
    },
    RtInFlight {
        runtime: LoweredExecOperand,
    },
    RtCancelled,
    ChanOpenU32 {
        capacity: LoweredExecOperand,
    },
    ChanOpenBufU8 {
        capacity: LoweredExecOperand,
    },
    ChanSendU32 {
        channel: LoweredExecOperand,
        value: LoweredExecOperand,
    },
    ChanSendBufU8 {
        channel: LoweredExecOperand,
        value: LoweredExecOperand,
    },
    ChanRecvU32 {
        channel: LoweredExecOperand,
    },
    ChanRecvBufU8 {
        channel: LoweredExecOperand,
    },
    ChanLen {
        channel: LoweredExecOperand,
    },
    ChanClose {
        channel: LoweredExecOperand,
    },
    DeadlineOpenMs {
        timeout_ms: LoweredExecOperand,
    },
    DeadlineExpired {
        handle: LoweredExecOperand,
    },
    DeadlineRemainingMs {
        handle: LoweredExecOperand,
    },
    DeadlineClose {
        handle: LoweredExecOperand,
    },
    CancelScopeOpen,
    CancelScopeChild {
        parent: LoweredExecOperand,
    },
    CancelScopeBindTask {
        scope: LoweredExecOperand,
        task: LoweredExecOperand,
    },
    CancelScopeCancel {
        scope: LoweredExecOperand,
    },
    CancelScopeCancelled {
        scope: LoweredExecOperand,
    },
    CancelScopeClose {
        scope: LoweredExecOperand,
    },
    RetryOpen {
        max_attempts: LoweredExecOperand,
        base_backoff_ms: LoweredExecOperand,
    },
    RetryRecordFailure {
        handle: LoweredExecOperand,
    },
    RetryRecordSuccess {
        handle: LoweredExecOperand,
    },
    RetryNextDelayMs {
        handle: LoweredExecOperand,
    },
    RetryExhausted {
        handle: LoweredExecOperand,
    },
    RetryClose {
        handle: LoweredExecOperand,
    },
    CircuitOpen {
        threshold: LoweredExecOperand,
        cooldown_ms: LoweredExecOperand,
    },
    CircuitAllow {
        handle: LoweredExecOperand,
    },
    CircuitRecordFailure {
        handle: LoweredExecOperand,
    },
    CircuitRecordSuccess {
        handle: LoweredExecOperand,
    },
    CircuitState {
        handle: LoweredExecOperand,
    },
    CircuitClose {
        handle: LoweredExecOperand,
    },
    BackpressureOpen {
        limit: LoweredExecOperand,
    },
    BackpressureAcquire {
        handle: LoweredExecOperand,
    },
    BackpressureRelease {
        handle: LoweredExecOperand,
    },
    BackpressureSaturated {
        handle: LoweredExecOperand,
    },
    BackpressureClose {
        handle: LoweredExecOperand,
    },
    SupervisorOpen {
        restart_budget: LoweredExecOperand,
        degrade_after: LoweredExecOperand,
    },
    SupervisorRecordFailure {
        handle: LoweredExecOperand,
        code: LoweredExecOperand,
    },
    SupervisorRecordRecovery {
        handle: LoweredExecOperand,
    },
    SupervisorShouldRestart {
        handle: LoweredExecOperand,
    },
    SupervisorDegraded {
        handle: LoweredExecOperand,
    },
    SupervisorClose {
        handle: LoweredExecOperand,
    },
    TaskSleepMs {
        value: LoweredExecOperand,
    },
    TaskOpen {
        command: String,
        argv: Vec<String>,
        env: Vec<(String, String)>,
    },
    TaskDone {
        handle: LoweredExecOperand,
    },
    TaskJoinStatus {
        handle: LoweredExecOperand,
    },
    TaskStdoutAllU8 {
        handle: LoweredExecOperand,
    },
    TaskStderrAllU8 {
        handle: LoweredExecOperand,
    },
    TaskClose {
        handle: LoweredExecOperand,
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
        handle: LoweredExecOperand,
    },
    SpawnStdoutAllU8 {
        handle: LoweredExecOperand,
    },
    SpawnStderrAllU8 {
        handle: LoweredExecOperand,
    },
    SpawnStdinWriteAllU8 {
        handle: LoweredExecOperand,
        value: LoweredExecOperand,
    },
    SpawnStdinClose {
        handle: LoweredExecOperand,
    },
    SpawnDone {
        handle: LoweredExecOperand,
    },
    SpawnExitOk {
        handle: LoweredExecOperand,
    },
    SpawnKill {
        handle: LoweredExecOperand,
    },
    SpawnClose {
        handle: LoweredExecOperand,
    },
    NetConnect {
        host: String,
        port: u16,
    },
    FfiCall {
        symbol: String,
        args: Vec<LoweredExecOperand>,
        ret_c_type: String,
    },
    FfiCallCStr {
        symbol: String,
        arg: String,
        ret_c_type: String,
    },
    FfiOpenLib {
        path: String,
    },
    FfiCloseLib {
        handle: LoweredExecOperand,
    },
    FfiBufPtr {
        value: LoweredExecOperand,
    },
    FfiCallLib {
        handle: LoweredExecOperand,
        symbol: String,
        args: Vec<LoweredExecOperand>,
        ret_c_type: String,
    },
    FfiCallLibCStr {
        handle: LoweredExecOperand,
        symbol: String,
        arg: String,
        ret_c_type: String,
    },
    Len {
        source: String,
    },
    StoreBufU8 {
        source: String,
        index: LoweredExecOperand,
        value: LoweredExecOperand,
    },
    LoadU8 {
        source: String,
        index: LoweredExecOperand,
    },
    LoadI32 {
        source: String,
        index: LoweredExecOperand,
    },
    AbsI32 {
        value: LoweredExecOperand,
    },
    Binary {
        op: LoweredExecBinaryOp,
        left: LoweredExecOperand,
        right: LoweredExecOperand,
    },
    SextI64 {
        value: LoweredExecOperand,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoweredCall {
    pub ret_c_type: String,
    pub result_name: String,
    pub function_name: String,
    pub args: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoweredAssertion {
    pub condition: String,
    pub failure_message: String,
}

pub fn emit_library(program: &Program) -> Result<String, String> {
    let lowered = lower_program(program)?;
    Ok(emit_library_from_lowered(&lowered))
}

pub fn emit_library_from_lowered(lowered: &LoweredProgram) -> String {
    let mut out = lowered.preamble.clone();
    for function in &lowered.functions {
        out.push_str(&emit_lowered_function(function));
        out.push('\n');
    }
    if lowered_program_uses_rt_spawn_u32(lowered) {
        out.push_str(&emit_rt_dispatch_u32(lowered));
        out.push('\n');
    }
    if lowered_program_uses_rt_spawn_buf(lowered) {
        out.push_str(&emit_rt_dispatch_buf(lowered));
        out.push('\n');
    }
    out
}

fn lowered_program_uses_rt_spawn_u32(lowered: &LoweredProgram) -> bool {
    lowered.functions.iter().any(|function| {
        function.blocks.iter().any(|block| {
            block.statements.iter().any(|statement| match statement {
                LoweredStatement::Assign(assignment) => matches!(
                    assignment.exec_expr,
                    Some(LoweredExecExpr::RtSpawnU32 { .. } | LoweredExecExpr::RtTrySpawnU32 { .. })
                ),
            })
        })
    })
}

fn lowered_program_uses_rt_spawn_buf(lowered: &LoweredProgram) -> bool {
    lowered.functions.iter().any(|function| {
        function.blocks.iter().any(|block| {
            block.statements.iter().any(|statement| match statement {
                LoweredStatement::Assign(assignment) => matches!(
                    assignment.exec_expr,
                    Some(LoweredExecExpr::RtSpawnBufU8 { .. } | LoweredExecExpr::RtTrySpawnBufU8 { .. })
                ),
            })
        })
    })
}

fn emit_rt_dispatch_u32(lowered: &LoweredProgram) -> String {
    let mut out = String::new();
    out.push_str("uint32_t mira_rt_dispatch_u32(const char* function_name, uint32_t arg) {\n");
    for function in &lowered.functions {
        if function.ret_c_type == "uint32_t"
            && function.args.len() == 1
            && function.args[0].0 == "uint32_t"
        {
            out.push_str(&format!(
                "  if (strcmp(function_name, \"{}\") == 0) {{ return mira_func_{}(arg); }}\n",
                function.name, function.name
            ));
        }
    }
    out.push_str("  return 0u;\n");
    out.push_str("}\n");
    out
}

fn emit_rt_dispatch_buf(lowered: &LoweredProgram) -> String {
    let mut out = String::new();
    out.push_str("buf_u8 mira_rt_dispatch_buf(const char* function_name, buf_u8 arg) {\n");
    for function in &lowered.functions {
        if function.ret_c_type == "buf_u8"
            && function.args.len() == 1
            && function.args[0].0 == "buf_u8"
        {
            out.push_str(&format!(
                "  if (strcmp(function_name, \"{}\") == 0) {{ return mira_func_{}(arg); }}\n",
                function.name, function.name
            ));
        }
    }
    out.push_str("  return (buf_u8){0};\n");
    out.push_str("}\n");
    out
}

pub fn lower_program(program: &Program) -> Result<LoweredProgram, String> {
    let mut preamble = String::new();
    preamble.push_str("#include <stdbool.h>\n");
    preamble.push_str("#include <stdint.h>\n");
    preamble.push_str("#include <stddef.h>\n");
    preamble.push_str("#include <limits.h>\n");
    preamble.push_str("#include <inttypes.h>\n");
    preamble.push_str("#include <stdio.h>\n");
    preamble.push_str("#include <stdlib.h>\n");
    preamble.push_str("#include <string.h>\n");
    preamble.push_str("#include <time.h>\n\n");
    preamble.push_str("#ifdef _WIN32\n");
    preamble.push_str("#include <malloc.h>\n");
    preamble.push_str("#include <winsock2.h>\n");
    preamble.push_str("#include <ws2tcpip.h>\n");
    preamble.push_str("#include <io.h>\n");
    preamble.push_str("#include <process.h>\n");
    preamble.push_str("#include <fcntl.h>\n");
    preamble.push_str("#ifndef SHUT_WR\n");
    preamble.push_str("#define SHUT_WR SD_SEND\n");
    preamble.push_str("#endif\n");
    preamble.push_str("#else\n");
    preamble.push_str("#include <alloca.h>\n");
    preamble.push_str("#include <unistd.h>\n");
    preamble.push_str("#include <sys/socket.h>\n");
    preamble.push_str("#include <netinet/in.h>\n");
    preamble.push_str("#include <netdb.h>\n");
    preamble.push_str("#include <sys/wait.h>\n");
    preamble.push_str("#endif\n\n");

    let named_types = build_named_type_map(program)?;
    let lowered_functions = program
        .functions
        .iter()
        .map(|function| lower_function(function, &named_types))
        .collect::<Result<Vec<_>, _>>()?;
    for ty in collect_lowered_types(program)? {
        emit_type_decl(&mut preamble, &ty)?;
    }
    for item in &program.types {
        emit_named_type_decl(&mut preamble, item)?;
    }
    if !collect_sat_types(program)?.is_empty() {
        emit_sat_helpers(&mut preamble, &collect_sat_types(program)?)?;
    }
    if !collect_runtime_buf_types(program)?.is_empty() {
        emit_runtime_buf_helpers(&mut preamble, &collect_runtime_buf_types(program)?)?;
    }
    if program_uses_op(program, "clock_now_ns") {
        emit_clock_helpers(&mut preamble);
    }
    if program_uses_op(program, "rand_u32") {
        emit_rand_helpers(&mut preamble);
    }
    let uses_fs_scalar =
        program_uses_op(program, "fs_read_u32") || program_uses_op(program, "fs_write_u32");
    let uses_fs_bytes =
        program_uses_op(program, "fs_read_all") || program_uses_op(program, "fs_write_all");
    if uses_fs_scalar || uses_fs_bytes {
        emit_fs_helpers(&mut preamble, uses_fs_bytes);
    }
    let uses_net_bytes = program_uses_op(program, "net_write_all")
        || program_uses_op(program, "net_exchange_all")
        || program_uses_op(program, "net_serve_exchange_all");
    if program_uses_op(program, "net_connect")
        || program_uses_op(program, "tls_exchange_all")
        || program_uses_op(program, "tls_listen")
        || program_uses_op(program, "net_write_all")
        || program_uses_op(program, "net_exchange_all")
        || program_uses_op(program, "net_serve_exchange_all")
    {
        emit_net_helpers(&mut preamble, uses_net_bytes);
    }
    let uses_spawn_bytes = program_uses_op(program, "spawn_capture_all")
        || program_uses_op(program, "spawn_capture_stderr_all");
    if program_uses_op(program, "spawn_call") || uses_spawn_bytes {
        emit_spawn_helpers(&mut preamble, uses_spawn_bytes);
    }
    if program_uses_op(program, "ffi_call") || program_uses_op(program, "ffi_call_cstr") {
        emit_ffi_decls(&mut preamble, program)?;
    }
    if program_uses_op(program, "net_listen")
        || program_uses_op(program, "net_session_open")
        || program_uses_op(program, "tls_listen")
        || program_uses_op(program, "net_accept")
        || program_uses_op(program, "http_session_accept")
        || program_uses_op(program, "listener_set_timeout_ms")
        || program_uses_op(program, "session_set_timeout_ms")
        || program_uses_op(program, "listener_set_shutdown_grace_ms")
        || program_uses_op(program, "net_read_all")
        || program_uses_op(program, "session_read_chunk")
        || program_uses_op(program, "http_session_request")
        || program_uses_op(program, "net_write_handle_all")
        || program_uses_op(program, "session_write_chunk")
        || program_uses_op(program, "session_flush")
        || program_uses_op(program, "session_alive")
        || program_uses_op(program, "session_heartbeat")
        || program_uses_op(program, "session_backpressure")
        || program_uses_op(program, "session_backpressure_wait")
        || program_uses_op(program, "session_resume_id")
        || program_uses_op(program, "session_reconnect")
        || program_uses_op(program, "net_close")
        || program_uses_op(program, "http_session_close")
        || program_uses_op(program, "http_method_eq")
        || program_uses_op(program, "http_path_eq")
        || program_uses_op(program, "http_request_method")
        || program_uses_op(program, "http_request_path")
        || program_uses_op(program, "http_route_param")
        || program_uses_op(program, "http_header_eq")
        || program_uses_op(program, "http_cookie_eq")
        || program_uses_op(program, "http_status_u32")
        || program_uses_op(program, "http_header")
        || program_uses_op(program, "http_cookie")
        || program_uses_op(program, "http_query_param")
        || program_uses_op(program, "buf_contains_lit")
        || program_uses_op(program, "buf_eq_lit")
        || program_uses_op(program, "buf_parse_u32")
        || program_uses_op(program, "buf_parse_bool")
        || program_uses_op(program, "http_body")
        || program_uses_op(program, "http_body_limit")
        || program_uses_op(program, "http_server_config_u32")
        || program_uses_op(program, "tls_server_config_u32")
        || program_uses_op(program, "tls_server_config_buf")
        || program_uses_op(program, "http_write_response")
        || program_uses_op(program, "http_write_text_response")
        || program_uses_op(program, "http_write_text_response_cookie")
        || program_uses_op(program, "http_write_text_response_headers2")
        || program_uses_op(program, "http_session_write_text")
        || program_uses_op(program, "http_session_write_text_headers2")
        || program_uses_op(program, "http_session_write_text_cookie")
        || program_uses_op(program, "http_write_json_response")
        || program_uses_op(program, "http_write_json_response_cookie")
        || program_uses_op(program, "http_write_json_response_headers2")
        || program_uses_op(program, "http_session_write_json")
        || program_uses_op(program, "http_session_write_json_cookie")
        || program_uses_op(program, "http_session_write_json_headers2")
        || program_uses_op(program, "http_write_response_header")
        || program_uses_op(program, "json_get_u32")
        || program_uses_op(program, "json_has_key")
        || program_uses_op(program, "json_get_u32_or")
        || program_uses_op(program, "json_get_bool")
        || program_uses_op(program, "json_get_bool_or")
        || program_uses_op(program, "json_get_buf")
        || program_uses_op(program, "json_get_buf_or")
        || program_uses_op(program, "json_get_str")
        || program_uses_op(program, "json_get_str_or")
        || program_uses_op(program, "json_array_len")
        || program_uses_op(program, "json_index_u32")
        || program_uses_op(program, "json_index_bool")
        || program_uses_op(program, "json_index_str")
        || program_uses_op(program, "json_encode_obj")
        || program_uses_op(program, "json_encode_arr")
        || program_uses_op(program, "str_lit")
        || program_uses_op(program, "str_concat")
        || program_uses_op(program, "str_from_u32")
        || program_uses_op(program, "str_from_bool")
        || program_uses_op(program, "str_eq_lit")
        || program_uses_op(program, "str_to_buf")
        || program_uses_op(program, "buf_to_str")
        || program_uses_op(program, "buf_hex_str")
        || program_uses_op(program, "strmap_get_u32")
        || program_uses_op(program, "strmap_get_bool")
        || program_uses_op(program, "strmap_get_str")
        || program_uses_op(program, "strlist_len")
        || program_uses_op(program, "strlist_index_u32")
        || program_uses_op(program, "strlist_index_bool")
        || program_uses_op(program, "strlist_index_str")
        || program_uses_op(program, "config_get_u32")
        || program_uses_op(program, "config_get_bool")
        || program_uses_op(program, "config_get_str")
        || program_uses_op(program, "config_has")
        || program_uses_op(program, "env_get_u32")
        || program_uses_op(program, "env_get_bool")
        || program_uses_op(program, "env_get_str")
        || program_uses_op(program, "env_has")
        || program_uses_op(program, "buf_before_lit")
        || program_uses_op(program, "buf_after_lit")
        || program_uses_op(program, "buf_trim_ascii")
        || program_uses_op(program, "date_parse_ymd")
        || program_uses_op(program, "time_parse_hms")
        || program_uses_op(program, "date_format_ymd")
        || program_uses_op(program, "time_format_hms")
        || program_uses_op(program, "db_open")
        || program_uses_op(program, "db_close")
        || program_uses_op(program, "db_exec")
        || program_uses_op(program, "db_prepare")
        || program_uses_op(program, "db_exec_prepared")
        || program_uses_op(program, "db_query_u32")
        || program_uses_op(program, "db_query_buf")
        || program_uses_op(program, "db_query_row")
        || program_uses_op(program, "db_query_prepared_u32")
        || program_uses_op(program, "db_query_prepared_buf")
        || program_uses_op(program, "db_query_prepared_row")
        || program_uses_op(program, "db_row_found")
        || program_uses_op(program, "db_last_error_code")
        || program_uses_op(program, "db_last_error_retryable")
        || program_uses_op(program, "db_begin")
        || program_uses_op(program, "db_commit")
        || program_uses_op(program, "db_rollback")
        || program_uses_op(program, "db_pool_open")
        || program_uses_op(program, "db_pool_set_max_idle")
        || program_uses_op(program, "db_pool_leased")
        || program_uses_op(program, "db_pool_acquire")
        || program_uses_op(program, "db_pool_release")
        || program_uses_op(program, "db_pool_close")
        || program_uses_op(program, "cache_open")
        || program_uses_op(program, "cache_close")
        || program_uses_op(program, "cache_get_buf")
        || program_uses_op(program, "cache_set_buf")
        || program_uses_op(program, "cache_set_buf_ttl")
        || program_uses_op(program, "cache_del")
        || program_uses_op(program, "queue_open")
        || program_uses_op(program, "queue_close")
        || program_uses_op(program, "queue_push_buf")
        || program_uses_op(program, "queue_pop_buf")
        || program_uses_op(program, "queue_len")
        || program_uses_op(program, "stream_open")
        || program_uses_op(program, "stream_close")
        || program_uses_op(program, "stream_publish_buf")
        || program_uses_op(program, "stream_len")
        || program_uses_op(program, "stream_replay_open")
        || program_uses_op(program, "stream_replay_next")
        || program_uses_op(program, "stream_replay_offset")
        || program_uses_op(program, "stream_replay_close")
        || program_uses_op(program, "batch_open")
        || program_uses_op(program, "batch_push_u64")
        || program_uses_op(program, "batch_len")
        || program_uses_op(program, "batch_flush_sum_u64")
        || program_uses_op(program, "batch_close")
        || program_uses_op(program, "agg_open_u64")
        || program_uses_op(program, "agg_add_u64")
        || program_uses_op(program, "agg_count")
        || program_uses_op(program, "agg_sum_u64")
        || program_uses_op(program, "agg_avg_u64")
        || program_uses_op(program, "agg_min_u64")
        || program_uses_op(program, "agg_max_u64")
        || program_uses_op(program, "agg_close")
        || program_uses_op(program, "window_open_ms")
        || program_uses_op(program, "window_add_u64")
        || program_uses_op(program, "window_count")
        || program_uses_op(program, "window_sum_u64")
        || program_uses_op(program, "window_avg_u64")
        || program_uses_op(program, "window_min_u64")
        || program_uses_op(program, "window_max_u64")
        || program_uses_op(program, "window_close")
        || program_uses_op(program, "rt_open")
        || program_uses_op(program, "rt_spawn_u32")
        || program_uses_op(program, "rt_try_spawn_u32")
        || program_uses_op(program, "rt_spawn_buf")
        || program_uses_op(program, "rt_try_spawn_buf")
        || program_uses_op(program, "rt_done")
        || program_uses_op(program, "rt_join_u32")
        || program_uses_op(program, "rt_join_buf")
        || program_uses_op(program, "rt_cancel")
        || program_uses_op(program, "rt_task_close")
        || program_uses_op(program, "rt_shutdown")
        || program_uses_op(program, "rt_close")
        || program_uses_op(program, "rt_cancelled")
        || program_uses_op(program, "rt_inflight")
        || program_uses_op(program, "chan_open_u32")
        || program_uses_op(program, "chan_open_buf")
        || program_uses_op(program, "chan_send_u32")
        || program_uses_op(program, "chan_send_buf")
        || program_uses_op(program, "chan_recv_u32")
        || program_uses_op(program, "chan_recv_buf")
        || program_uses_op(program, "chan_len")
        || program_uses_op(program, "chan_close")
        || program_uses_op(program, "deadline_open_ms")
        || program_uses_op(program, "deadline_expired")
        || program_uses_op(program, "deadline_remaining_ms")
        || program_uses_op(program, "deadline_close")
        || program_uses_op(program, "cancel_scope_open")
        || program_uses_op(program, "cancel_scope_child")
        || program_uses_op(program, "cancel_scope_bind_task")
        || program_uses_op(program, "cancel_scope_cancel")
        || program_uses_op(program, "cancel_scope_cancelled")
        || program_uses_op(program, "cancel_scope_close")
        || program_uses_op(program, "retry_open")
        || program_uses_op(program, "retry_record_failure")
        || program_uses_op(program, "retry_record_success")
        || program_uses_op(program, "retry_next_delay_ms")
        || program_uses_op(program, "retry_exhausted")
        || program_uses_op(program, "retry_close")
        || program_uses_op(program, "circuit_open")
        || program_uses_op(program, "circuit_allow")
        || program_uses_op(program, "circuit_record_failure")
        || program_uses_op(program, "circuit_record_success")
        || program_uses_op(program, "circuit_state")
        || program_uses_op(program, "circuit_close")
        || program_uses_op(program, "backpressure_open")
        || program_uses_op(program, "backpressure_acquire")
        || program_uses_op(program, "backpressure_release")
        || program_uses_op(program, "backpressure_saturated")
        || program_uses_op(program, "backpressure_close")
        || program_uses_op(program, "supervisor_open")
        || program_uses_op(program, "supervisor_record_failure")
        || program_uses_op(program, "supervisor_record_recovery")
        || program_uses_op(program, "supervisor_should_restart")
        || program_uses_op(program, "supervisor_degraded")
        || program_uses_op(program, "supervisor_close")
        || program_uses_op(program, "task_sleep_ms")
        || program_uses_op(program, "task_open")
        || program_uses_op(program, "task_done")
        || program_uses_op(program, "task_join")
        || program_uses_op(program, "task_stdout_all")
        || program_uses_op(program, "task_stderr_all")
        || program_uses_op(program, "task_close")
        || program_uses_op(program, "service_open")
        || program_uses_op(program, "service_close")
        || program_uses_op(program, "service_shutdown")
        || program_uses_op(program, "service_log")
        || program_uses_op(program, "service_trace_begin")
        || program_uses_op(program, "service_trace_end")
        || program_uses_op(program, "service_metric_count")
        || program_uses_op(program, "service_metric_count_dim")
        || program_uses_op(program, "service_metric_total")
        || program_uses_op(program, "service_health_status")
        || program_uses_op(program, "service_readiness_status")
        || program_uses_op(program, "service_set_health")
        || program_uses_op(program, "service_set_readiness")
        || program_uses_op(program, "service_set_degraded")
        || program_uses_op(program, "service_degraded")
        || program_uses_op(program, "service_event")
        || program_uses_op(program, "service_event_total")
        || program_uses_op(program, "service_trace_link")
        || program_uses_op(program, "service_trace_link_count")
        || program_uses_op(program, "service_failure_count")
        || program_uses_op(program, "service_failure_total")
        || program_uses_op(program, "service_checkpoint_save_u32")
        || program_uses_op(program, "service_checkpoint_load_u32")
        || program_uses_op(program, "service_checkpoint_exists")
        || program_uses_op(program, "service_migrate_db")
        || program_uses_op(program, "service_route")
        || program_uses_op(program, "service_require_header")
        || program_uses_op(program, "service_error_status")
        || program_uses_op(program, "spawn_open")
        || program_uses_op(program, "spawn_wait")
        || program_uses_op(program, "spawn_stdout_all")
        || program_uses_op(program, "spawn_stderr_all")
        || program_uses_op(program, "spawn_close")
        || program_uses_op(program, "ffi_open_lib")
        || program_uses_op(program, "ffi_close_lib")
        || program_uses_op(program, "ffi_buf_ptr")
        || program_uses_op(program, "ffi_call_lib")
        || program_uses_op(program, "ffi_call_lib_cstr")
    {
        emit_runtime_bridge_decls(&mut preamble, program);
    }
    for item in &program.consts {
        preamble.push_str(&format!(
            "static const {} {} = {};\n",
            item.ty.c_type()?,
            item.name,
            render_c_literal_with_named_types(&item.value, Some(&item.ty), &named_types)?
        ));
    }
    if !program.consts.is_empty() {
        preamble.push('\n');
    }

    let mut tests = Vec::new();
    for owner in &program.functions {
        for case in &owner.tests {
            tests.push(lower_test(program, owner, case, &named_types)?);
        }
    }

    Ok(LoweredProgram {
        module: program.module.clone(),
        preamble,
        functions: lowered_functions,
        tests,
    })
}

pub fn emit_test_harness(program: &Program) -> Result<String, String> {
    let lowered = lower_program(program)?;
    Ok(emit_test_harness_from_lowered(&lowered))
}

pub fn emit_test_harness_from_lowered(lowered: &LoweredProgram) -> String {
    let mut out = emit_library_from_lowered(lowered);
    out.push_str("int main(void) {\n");
    out.push_str("  int failures = 0;\n");
    for test in &lowered.tests {
        out.push_str(&emit_lowered_test(test));
    }
    out.push_str("  if (failures != 0) {\n");
    out.push_str("    fprintf(stderr, \"tests failed: %d\\n\", failures);\n");
    out.push_str("    return 1;\n");
    out.push_str("  }\n");
    out.push_str("  puts(\"ok\");\n");
    out.push_str("  return 0;\n");
    out.push_str("}\n");
    out
}

pub fn emit_benchmark_harness(
    program: &Program,
    function_name: &str,
    arguments: &[(String, DataValue)],
    iterations: usize,
) -> Result<String, String> {
    let mut out = emit_library(program)?;
    out.push_str(&emit_benchmark_driver(
        program,
        function_name,
        arguments,
        iterations,
    )?);
    Ok(out)
}

pub fn emit_benchmark_driver(
    program: &Program,
    function_name: &str,
    arguments: &[(String, DataValue)],
    iterations: usize,
) -> Result<String, String> {
    let named_types = build_named_type_map(program)?;
    let function = program
        .functions
        .iter()
        .find(|function| function.name == function_name)
        .ok_or_else(|| format!("unknown benchmark function {function_name}"))?;
    let mut out = String::new();
    out.push_str("static uint64_t mira_now_ns(void) {\n");
    out.push_str("  struct timespec ts;\n");
    out.push_str("  clock_gettime(CLOCK_MONOTONIC, &ts);\n");
    out.push_str("  return ((uint64_t) ts.tv_sec * 1000000000ULL) + (uint64_t) ts.tv_nsec;\n");
    out.push_str("}\n\n");
    out.push_str("int main(void) {\n");
    let (decls, call_args) = emit_call_arguments("bench", function, arguments, true, &named_types)?;
    out.push_str(&decls);
    out.push_str("  volatile int64_t sink = 0;\n");
    out.push_str("  uint32_t bench_seed = 1u;\n");
    out.push_str("  for (int warm = 0; warm < 2; warm++) {\n");
    out.push_str(&emit_benchmark_updates("bench", function)?);
    out.push_str(&format!(
        "    sink ^= (int64_t) mira_func_{}({});\n",
        function.name,
        call_args.join(", ")
    ));
    out.push_str("  }\n");
    out.push_str("  uint64_t samples[5] = {0};\n");
    out.push_str("  for (int sample = 0; sample < 5; sample++) {\n");
    out.push_str("    uint64_t started = mira_now_ns();\n");
    out.push_str(&format!(
        "    for (uint32_t iter = 0; iter < {}u; iter++) {{\n",
        iterations
    ));
    out.push_str(&emit_benchmark_updates("bench", function)?);
    out.push_str(&format!(
        "      sink ^= (int64_t) mira_func_{}({});\n",
        function.name,
        call_args.join(", ")
    ));
    out.push_str("    }\n");
    out.push_str("    uint64_t ended = mira_now_ns();\n");
    out.push_str("    samples[sample] = ended - started;\n");
    out.push_str("  }\n");
    out.push_str("  fprintf(stdout, \"SINK=%\" PRId64 \"\\n\", sink);\n");
    out.push_str("  for (int sample = 0; sample < 5; sample++) {\n");
    out.push_str("    fprintf(stdout, \"SAMPLE=%\" PRIu64 \"\\n\", samples[sample]);\n");
    out.push_str("  }\n");
    out.push_str("  return 0;\n");
    out.push_str("}\n");
    Ok(out)
}

pub fn emit_benchmark_driver_from_lowered(
    lowered: &LoweredProgram,
    program: &Program,
    function_name: &str,
    arguments: &[(String, DataValue)],
    iterations: usize,
) -> Result<String, String> {
    let function = lowered
        .functions
        .iter()
        .find(|function| function.name == function_name)
        .ok_or_else(|| format!("unknown lowered benchmark function {function_name}"))?;
    let args = function
        .args
        .iter()
        .map(|(ty, name)| format!("{ty} {name}"))
        .collect::<Vec<_>>()
        .join(", ");
    let mut out = lowered.preamble.clone();
    out.push_str(&format!(
        "extern {} mira_func_{}({});\n\n",
        function.ret_c_type, function.name, args
    ));
    out.push_str(&emit_benchmark_driver(
        program,
        function_name,
        arguments,
        iterations,
    )?);
    Ok(out)
}

fn lower_function(
    function: &Function,
    named_types: &HashMap<String, TypeDeclBody>,
) -> Result<LoweredFunction, String> {
    let uses_arena = function_uses_arena(function);
    let args = function
        .args
        .iter()
        .map(|arg| Ok((arg.ty.c_type()?, arg.name.clone())))
        .collect::<Result<Vec<_>, String>>()?;

    let mut declarations = BTreeMap::new();
    for block in &function.blocks {
        for param in &block.params {
            declarations.insert(param_c_name(block, &param.name), param.ty.c_type()?);
        }
        for instruction in &block.instructions {
            declarations.insert(instruction.bind.clone(), instruction.ty.c_type()?);
        }
    }

    let lowered_blocks = function
        .blocks
        .iter()
        .map(|block| lower_block(function, block, named_types, uses_arena))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(LoweredFunction {
        name: function.name.clone(),
        ret_c_type: function.ret.c_type()?,
        args,
        declarations: declarations.into_iter().collect(),
        uses_arena,
        rand_seed: if function_uses_op(function, "rand_u32") {
            Some(rand_seed_for_function(function)?)
        } else {
            None
        },
        blocks: lowered_blocks,
    })
}

fn lower_block(
    function: &Function,
    block: &Block,
    named_types: &HashMap<String, TypeDeclBody>,
    uses_arena: bool,
) -> Result<LoweredBlock, String> {
    let mut statements = Vec::new();
    let env = build_env(function, block);
    let type_env = build_type_env(function, block);
    for instruction in &block.instructions {
        let expr =
            render_instruction_expr(function, block, instruction, &env, &type_env, named_types)?;
        statements.push(LoweredStatement::Assign(LoweredAssignment {
            target: instruction.bind.clone(),
            expr,
            exec_expr: lower_exec_expr(function, instruction, &env, &type_env, named_types)?,
        }));
    }
    let terminator = lower_terminator(function, block, &env, &type_env, named_types, uses_arena)?;
    Ok(LoweredBlock {
        label: block.label.clone(),
        statements,
        terminator,
    })
}

fn emit_lowered_function(function: &LoweredFunction) -> String {
    let mut out = String::new();
    let args = function
        .args
        .iter()
        .map(|(ty, name)| format!("{ty} {name}"))
        .collect::<Vec<_>>()
        .join(", ");
    out.push_str(&format!(
        "static __attribute__((noinline)) {} mira_func_{}({}) {{\n",
        function.ret_c_type, function.name, args
    ));
    for (name, ty) in &function.declarations {
        out.push_str(&format!("  {} {};\n", ty, name));
    }
    if function.uses_arena {
        out.push_str("  mira_arena_runtime mira_arena = {0};\n");
        out.push_str(&format!("  {} mira_ret_value;\n", function.ret_c_type));
    }
    if let Some(seed) = function.rand_seed {
        out.push_str(&format!("  uint32_t mira_rand_state = {}u;\n", seed));
    }
    out.push_str("  goto b0;\n");
    for block in &function.blocks {
        out.push_str(&format!("{}:\n", block.label));
        for statement in &block.statements {
            emit_lowered_statement(&mut out, statement, 2);
        }
        emit_lowered_terminator(&mut out, &block.terminator, 2);
    }
    out.push_str("}\n");
    out
}

fn lower_terminator(
    function: &Function,
    block: &Block,
    env: &HashMap<String, String>,
    type_env: &HashMap<String, TypeRef>,
    named_types: &HashMap<String, TypeDeclBody>,
    uses_arena: bool,
) -> Result<LoweredTerminator, String> {
    match &block.terminator {
        Terminator::Return(value) => {
            let rendered = render_operand(value, env, Some(&function.ret), named_types)?;
            Ok(LoweredTerminator::Return {
                expr: rendered,
                release_arena: uses_arena,
                exec_value: lower_exec_operand(value, env, Some(&function.ret), named_types)?,
            })
        }
        Terminator::Jump(target) => Ok(LoweredTerminator::Jump {
            edge: lower_edge(function, target, env, named_types)?,
        }),
        Terminator::Branch {
            condition,
            truthy,
            falsy,
        } => Ok(LoweredTerminator::Branch {
            condition: render_operand(condition, env, Some(&TypeRef::Bool), named_types)?,
            truthy: lower_edge(function, truthy, env, named_types)?,
            falsy: lower_edge(function, falsy, env, named_types)?,
            exec_condition: lower_exec_operand(condition, env, Some(&TypeRef::Bool), named_types)?,
        }),
        Terminator::Match { value, arms } => {
            let match_value = render_match_value(value, env, type_env, named_types)?;
            let mut cases = Vec::new();
            for (index, arm) in arms.iter().enumerate().take(arms.len().saturating_sub(1)) {
                cases.push(LoweredMatchCase {
                    tag_index: index,
                    edge: lower_edge(function, arm, env, named_types)?,
                });
            }
            let default_arm = arms
                .last()
                .ok_or_else(|| "match requires at least one arm".to_string())?;
            Ok(LoweredTerminator::Match {
                value: match_value,
                cases,
                default: lower_edge(function, default_arm, env, named_types)?,
                exec_value: lower_exec_operand(value, env, None, named_types)?,
            })
        }
    }
}

fn lower_edge(
    function: &Function,
    target: &Target,
    env: &HashMap<String, String>,
    named_types: &HashMap<String, TypeDeclBody>,
) -> Result<LoweredEdge, String> {
    let target_block = function
        .blocks
        .iter()
        .find(|candidate| candidate.label == target.label)
        .ok_or_else(|| format!("unknown target block {}", target.label))?;
    let mut assignments = Vec::new();
    for (param, operand) in target_block.params.iter().zip(target.args.iter()) {
        assignments.push(LoweredAssignment {
            target: param_c_name(target_block, &param.name),
            expr: render_operand(operand, env, Some(&param.ty), named_types)?,
            exec_expr: lower_exec_operand(operand, env, Some(&param.ty), named_types)?
                .map(LoweredExecExpr::Move),
        });
    }
    Ok(LoweredEdge {
        assignments,
        label: target.label.clone(),
    })
}

fn emit_lowered_statement(out: &mut String, statement: &LoweredStatement, indent: usize) {
    match statement {
        LoweredStatement::Assign(assignment) => {
            emit_indent(out, indent);
            out.push_str(&assignment.target);
            out.push_str(" = ");
            out.push_str(&assignment.expr);
            out.push_str(";\n");
        }
    }
}

fn emit_lowered_terminator(out: &mut String, terminator: &LoweredTerminator, indent: usize) {
    match terminator {
        LoweredTerminator::Return {
            expr,
            release_arena,
            ..
        } => {
            if *release_arena {
                emit_indent(out, indent);
                out.push_str("mira_ret_value = ");
                out.push_str(expr);
                out.push_str(";\n");
                emit_indent(out, indent);
                out.push_str("mira_arena_release(&mira_arena);\n");
                emit_indent(out, indent);
                out.push_str("return mira_ret_value;\n");
            } else {
                emit_indent(out, indent);
                out.push_str("return ");
                out.push_str(expr);
                out.push_str(";\n");
            }
        }
        LoweredTerminator::Jump { edge } => emit_lowered_edge(out, edge, indent),
        LoweredTerminator::Branch {
            condition,
            truthy,
            falsy,
            ..
        } => {
            emit_indent(out, indent);
            out.push_str("if (");
            out.push_str(condition);
            out.push_str(") {\n");
            emit_lowered_edge(out, truthy, indent + 2);
            emit_indent(out, indent);
            out.push_str("} else {\n");
            emit_lowered_edge(out, falsy, indent + 2);
            emit_indent(out, indent);
            out.push_str("}\n");
        }
        LoweredTerminator::Match {
            value,
            cases,
            default,
            ..
        } => {
            emit_indent(out, indent);
            out.push_str("switch ((uint64_t) (");
            out.push_str(value);
            out.push_str(")) {\n");
            for case in cases {
                emit_indent(out, indent + 2);
                out.push_str("case ");
                out.push_str(&format!("{}u", case.tag_index));
                out.push_str(":\n");
                emit_lowered_edge(out, &case.edge, indent + 4);
            }
            emit_indent(out, indent + 2);
            out.push_str("default:\n");
            emit_lowered_edge(out, default, indent + 4);
            emit_indent(out, indent);
            out.push_str("}\n");
        }
    }
}

fn emit_lowered_edge(out: &mut String, edge: &LoweredEdge, indent: usize) {
    for assignment in &edge.assignments {
        emit_indent(out, indent);
        out.push_str(&assignment.target);
        out.push_str(" = ");
        out.push_str(&assignment.expr);
        out.push_str(";\n");
    }
    emit_indent(out, indent);
    out.push_str("goto ");
    out.push_str(&edge.label);
    out.push_str(";\n");
}

fn emit_indent(out: &mut String, indent: usize) {
    for _ in 0..indent {
        out.push(' ');
    }
}

fn lower_exec_expr(
    function: &Function,
    instruction: &crate::ast::Instruction,
    env: &HashMap<String, String>,
    type_env: &HashMap<String, TypeRef>,
    named_types: &HashMap<String, TypeDeclBody>,
) -> Result<Option<LoweredExecExpr>, String> {
    let args = &instruction.args;
    let expr = match instruction.op.as_str() {
        "const" => {
            let operand = lower_exec_operand(&args[0], env, Some(&instruction.ty), named_types)?;
            operand.map(LoweredExecExpr::Move)
        }
        "alloc"
            if matches!(
                &instruction.ty,
                TypeRef::Own(inner)
                    if matches!(
                        inner.as_ref(),
                        TypeRef::Buf(elem)
                            if **elem
                                == TypeRef::Int {
                                    signed: false,
                                    bits: 8,
                                }
                    )
            ) =>
        {
            Some(LoweredExecExpr::AllocBufU8 {
                region: args
                    .first()
                    .ok_or_else(|| "alloc requires region".to_string())?
                    .clone(),
                len: lower_exec_operand(
                    &args[1],
                    env,
                    Some(&TypeRef::Int {
                        signed: false,
                        bits: 32,
                    }),
                    named_types,
                )?
                .ok_or_else(|| format!("unsupported alloc length {}", args[1]))?,
            })
        }
        "drop"
            if matches!(
                instruction.args.first().and_then(|token| resolve_operand_type(token, type_env)),
                Some(TypeRef::Own(inner))
                    if matches!(inner.as_ref(), TypeRef::String)
                        || matches!(
                            inner.as_ref(),
                            TypeRef::Buf(elem)
                                if **elem
                                    == TypeRef::Int {
                                        signed: false,
                                        bits: 8,
                                    }
                        )
            ) =>
        {
            Some(LoweredExecExpr::DropBufU8 {
                value: lower_exec_operand(&args[0], env, Some(&instruction.ty), named_types)?
                    .ok_or_else(|| format!("unsupported drop operand {}", args[0]))?,
            })
        }
        "clock_now_ns" if args.is_empty() => Some(LoweredExecExpr::ClockNowNs),
        "rand_u32" if args.is_empty() => Some(LoweredExecExpr::RandU32),
        "fs_read_u32" if args.is_empty() => Some(LoweredExecExpr::FsReadU32 {
            path: fs_path_for_function(function)?,
        }),
        "fs_write_u32" => Some(LoweredExecExpr::FsWriteU32 {
            path: fs_path_for_function(function)?,
            value: lower_exec_operand(
                &args[0],
                env,
                Some(&TypeRef::Int {
                    signed: false,
                    bits: 32,
                }),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported fs_write_u32 operand {}", args[0]))?,
        }),
        "fs_read_all"
            if matches!(
                &instruction.ty,
                TypeRef::Own(inner)
                    if matches!(
                        inner.as_ref(),
                        TypeRef::Buf(elem)
                            if **elem
                                == TypeRef::Int {
                                    signed: false,
                                    bits: 8,
                                }
                    )
            ) =>
        {
            Some(LoweredExecExpr::FsReadAllU8 {
                path: fs_path_for_function(function)?,
            })
        }
        "fs_write_all" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown fs_write_all operand type for {}", args[0]))?;
            if matches!(
                &value_ty,
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
                Some(LoweredExecExpr::FsWriteAllU8 {
                    path: fs_path_for_function(function)?,
                    value: lower_exec_operand(&args[0], env, Some(&value_ty), named_types)?
                        .ok_or_else(|| format!("unsupported fs_write_all operand {}", args[0]))?,
                })
            } else {
                None
            }
        }
        "net_write_all" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown net_write_all operand type for {}", args[0]))?;
            if matches!(
                &value_ty,
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
                let (host, port) = net_endpoint_for_function(function)?;
                Some(LoweredExecExpr::NetWriteAllU8 {
                    host,
                    port,
                    value: lower_exec_operand(&args[0], env, Some(&value_ty), named_types)?
                        .ok_or_else(|| format!("unsupported net_write_all operand {}", args[0]))?,
                })
            } else {
                None
            }
        }
        "net_exchange_all" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown net_exchange_all operand type for {}", args[0]))?;
            if matches!(
                &value_ty,
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
                let (host, port) = net_endpoint_for_function(function)?;
                Some(LoweredExecExpr::NetExchangeAllU8 {
                    host,
                    port,
                    value: lower_exec_operand(&args[0], env, Some(&value_ty), named_types)?
                        .ok_or_else(|| {
                            format!("unsupported net_exchange_all operand {}", args[0])
                        })?,
                })
            } else {
                None
            }
        }
        "net_serve_exchange_all" => {
            let response_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!(
                    "unknown net_serve_exchange_all operand type for {}",
                    args[0]
                )
            })?;
            if matches!(
                &response_ty,
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
                let (host, port) = net_endpoint_for_function(function)?;
                Some(LoweredExecExpr::NetServeExchangeAllU8 {
                    host,
                    port,
                    response: lower_exec_operand(&args[0], env, Some(&response_ty), named_types)?
                        .ok_or_else(|| {
                        format!("unsupported net_serve_exchange_all operand {}", args[0])
                    })?,
                })
            } else {
                None
            }
        }
        "net_listen" if args.is_empty() => {
            let (host, port) = net_endpoint_for_function(function)?;
            Some(LoweredExecExpr::NetListen { host, port })
        }
        "tls_listen" if args.is_empty() => {
            let (host, port) = net_endpoint_for_function(function)?;
            let tls = tls_capability_for_function(function)?;
            Some(LoweredExecExpr::TlsListen {
                host,
                port,
                cert: tls.cert,
                key: tls.key,
                request_timeout_ms: tls.request_timeout_ms,
                session_timeout_ms: tls.session_timeout_ms,
                shutdown_grace_ms: tls.shutdown_grace_ms,
            })
        }
        "net_accept" => Some(LoweredExecExpr::NetAccept {
            listener: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported net_accept operand {}", args[0]))?,
        }),
        "net_session_open" if args.is_empty() => {
            let (host, port) = net_endpoint_for_function(function)?;
            Some(LoweredExecExpr::NetSessionOpen { host, port })
        }
        "http_session_accept" => Some(LoweredExecExpr::HttpSessionAccept {
            listener: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported http_session_accept operand {}", args[0]))?,
        }),
        "net_read_all" => Some(LoweredExecExpr::NetReadAllU8 {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported net_read_all operand {}", args[0]))?,
        }),
        "session_read_chunk" => {
            let chunk_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown session_read_chunk operand type for {}", args[1]))?;
            Some(LoweredExecExpr::SessionReadChunkU8 {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| format!("unsupported session_read_chunk handle {}", args[0]))?,
                chunk_size: lower_exec_operand(&args[1], env, Some(&chunk_ty), named_types)?
                    .ok_or_else(|| format!("unsupported session_read_chunk chunk {}", args[1]))?,
            })
        }
        "http_session_request" => Some(LoweredExecExpr::HttpSessionRequest {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported http_session_request operand {}", args[0]))?,
        }),
        "net_write_handle_all" => {
            let value_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!("unknown net_write_handle_all operand type for {}", args[1])
            })?;
            Some(LoweredExecExpr::NetWriteHandleAllU8 {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| format!("unsupported net_write_handle_all handle {}", args[0]))?,
                value: lower_exec_operand(&args[1], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| format!("unsupported net_write_handle_all value {}", args[1]))?,
            })
        }
        "session_write_chunk" => {
            let value_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!("unknown session_write_chunk operand type for {}", args[1])
            })?;
            Some(LoweredExecExpr::SessionWriteChunkU8 {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| format!("unsupported session_write_chunk handle {}", args[0]))?,
                value: lower_exec_operand(&args[1], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| format!("unsupported session_write_chunk value {}", args[1]))?,
            })
        }
        "session_flush" => Some(LoweredExecExpr::SessionFlush {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported session_flush operand {}", args[0]))?,
        }),
        "session_alive" => Some(LoweredExecExpr::SessionAlive {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported session_alive operand {}", args[0]))?,
        }),
        "session_heartbeat" => {
            let value_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!("unknown session_heartbeat operand type for {}", args[1])
            })?;
            Some(LoweredExecExpr::SessionHeartbeatU8 {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| format!("unsupported session_heartbeat handle {}", args[0]))?,
                value: lower_exec_operand(&args[1], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| format!("unsupported session_heartbeat value {}", args[1]))?,
            })
        }
        "session_backpressure" => Some(LoweredExecExpr::SessionBackpressure {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported session_backpressure operand {}", args[0]))?,
        }),
        "session_backpressure_wait" => {
            let value_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!(
                    "unknown session_backpressure_wait operand type for {}",
                    args[1]
                )
            })?;
            Some(LoweredExecExpr::SessionBackpressureWait {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| {
                    format!("unsupported session_backpressure_wait handle {}", args[0])
                })?,
                max_pending: lower_exec_operand(&args[1], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| {
                        format!("unsupported session_backpressure_wait value {}", args[1])
                    })?,
            })
        }
        "session_resume_id" => Some(LoweredExecExpr::SessionResumeId {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported session_resume_id operand {}", args[0]))?,
        }),
        "session_reconnect" => Some(LoweredExecExpr::SessionReconnect {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported session_reconnect operand {}", args[0]))?,
        }),
        "net_close" => Some(LoweredExecExpr::NetClose {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported net_close operand {}", args[0]))?,
        }),
        "http_session_close" => Some(LoweredExecExpr::HttpSessionClose {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported http_session_close operand {}", args[0]))?,
        }),
        "listener_set_timeout_ms" => {
            let value_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!(
                    "unknown listener_set_timeout_ms operand type for {}",
                    args[1]
                )
            })?;
            Some(LoweredExecExpr::ListenerSetTimeoutMs {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| format!("unsupported listener_set_timeout_ms handle {}", args[0]))?,
                value: lower_exec_operand(&args[1], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| {
                        format!("unsupported listener_set_timeout_ms value {}", args[1])
                    })?,
            })
        }
        "session_set_timeout_ms" => {
            let value_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!(
                    "unknown session_set_timeout_ms operand type for {}",
                    args[1]
                )
            })?;
            Some(LoweredExecExpr::SessionSetTimeoutMs {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| format!("unsupported session_set_timeout_ms handle {}", args[0]))?,
                value: lower_exec_operand(&args[1], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| {
                        format!("unsupported session_set_timeout_ms value {}", args[1])
                    })?,
            })
        }
        "listener_set_shutdown_grace_ms" => {
            let value_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!(
                    "unknown listener_set_shutdown_grace_ms operand type for {}",
                    args[1]
                )
            })?;
            Some(LoweredExecExpr::ListenerSetShutdownGraceMs {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| {
                    format!(
                        "unsupported listener_set_shutdown_grace_ms handle {}",
                        args[0]
                    )
                })?,
                value: lower_exec_operand(&args[1], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| {
                        format!(
                            "unsupported listener_set_shutdown_grace_ms value {}",
                            args[1]
                        )
                    })?,
            })
        }
        "buf_lit" => Some(LoweredExecExpr::BufLit {
            literal: args[0].clone(),
        }),
        "str_lit" => Some(LoweredExecExpr::StrLit {
            literal: args[0].clone(),
        }),
        "buf_concat" => {
            let left_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown buf_concat operand type for {}", args[0]))?;
            let right_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown buf_concat operand type for {}", args[1]))?;
            Some(LoweredExecExpr::BufConcat {
                left: lower_exec_operand(&args[0], env, Some(&left_ty), named_types)?
                    .ok_or_else(|| format!("unsupported buf_concat operand {}", args[0]))?,
                right: lower_exec_operand(&args[1], env, Some(&right_ty), named_types)?
                    .ok_or_else(|| format!("unsupported buf_concat operand {}", args[1]))?,
            })
        }
        "str_concat" => {
            let left_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown str_concat operand type for {}", args[0]))?;
            let right_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown str_concat operand type for {}", args[1]))?;
            Some(LoweredExecExpr::StrConcat {
                left: lower_exec_operand(&args[0], env, Some(&left_ty), named_types)?
                    .ok_or_else(|| format!("unsupported str_concat operand {}", args[0]))?,
                right: lower_exec_operand(&args[1], env, Some(&right_ty), named_types)?
                    .ok_or_else(|| format!("unsupported str_concat operand {}", args[1]))?,
            })
        }
        "http_method_eq" => {
            let request_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown http_method_eq operand type for {}", args[0]))?;
            Some(LoweredExecExpr::HttpMethodEq {
                request: lower_exec_operand(&args[0], env, Some(&request_ty), named_types)?
                    .ok_or_else(|| format!("unsupported http_method_eq operand {}", args[0]))?,
                method: args[1].clone(),
            })
        }
        "http_path_eq" => {
            let request_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown http_path_eq operand type for {}", args[0]))?;
            Some(LoweredExecExpr::HttpPathEq {
                request: lower_exec_operand(&args[0], env, Some(&request_ty), named_types)?
                    .ok_or_else(|| format!("unsupported http_path_eq operand {}", args[0]))?,
                path: args[1].clone(),
            })
        }
        "http_request_method" => {
            let request_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown http_request_method operand type for {}", args[0])
            })?;
            Some(LoweredExecExpr::HttpRequestMethod {
                request: lower_exec_operand(&args[0], env, Some(&request_ty), named_types)?
                    .ok_or_else(|| {
                        format!("unsupported http_request_method operand {}", args[0])
                    })?,
            })
        }
        "http_request_path" => {
            let request_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown http_request_path operand type for {}", args[0]))?;
            Some(LoweredExecExpr::HttpRequestPath {
                request: lower_exec_operand(&args[0], env, Some(&request_ty), named_types)?
                    .ok_or_else(|| format!("unsupported http_request_path operand {}", args[0]))?,
            })
        }
        "http_route_param" => {
            let request_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown http_route_param operand type for {}", args[0]))?;
            Some(LoweredExecExpr::HttpRouteParam {
                request: lower_exec_operand(&args[0], env, Some(&request_ty), named_types)?
                    .ok_or_else(|| format!("unsupported http_route_param operand {}", args[0]))?,
                pattern: args[1].clone(),
                param: args[2].clone(),
            })
        }
        "http_header_eq" => {
            let request_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown http_header_eq operand type for {}", args[0]))?;
            Some(LoweredExecExpr::HttpHeaderEq {
                request: lower_exec_operand(&args[0], env, Some(&request_ty), named_types)?
                    .ok_or_else(|| format!("unsupported http_header_eq operand {}", args[0]))?,
                name: args[1].clone(),
                value: args[2].clone(),
            })
        }
        "http_cookie_eq" => {
            let request_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown http_cookie_eq operand type for {}", args[0]))?;
            Some(LoweredExecExpr::HttpCookieEq {
                request: lower_exec_operand(&args[0], env, Some(&request_ty), named_types)?
                    .ok_or_else(|| format!("unsupported http_cookie_eq operand {}", args[0]))?,
                name: args[1].clone(),
                value: args[2].clone(),
            })
        }
        "http_status_u32" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown http_status_u32 operand type for {}", args[0]))?;
            Some(LoweredExecExpr::HttpStatusU32 {
                value: lower_exec_operand(&args[0], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| format!("unsupported http_status_u32 operand {}", args[0]))?,
            })
        }
        "buf_eq_lit" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown buf_eq_lit operand type for {}", args[0]))?;
            Some(LoweredExecExpr::BufEqLit {
                value: lower_exec_operand(&args[0], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| format!("unsupported buf_eq_lit operand {}", args[0]))?,
                literal: args[1].clone(),
            })
        }
        "buf_contains_lit" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown buf_contains_lit operand type for {}", args[0]))?;
            Some(LoweredExecExpr::BufContainsLit {
                value: lower_exec_operand(&args[0], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| format!("unsupported buf_contains_lit operand {}", args[0]))?,
                literal: args[1].clone(),
            })
        }
        "http_header" => {
            let request_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown http_header operand type for {}", args[0]))?;
            Some(LoweredExecExpr::HttpHeader {
                request: lower_exec_operand(&args[0], env, Some(&request_ty), named_types)?
                    .ok_or_else(|| format!("unsupported http_header operand {}", args[0]))?,
                name: args[1].clone(),
            })
        }
        "http_header_count" => {
            let request_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown http_header_count operand type for {}", args[0]))?;
            Some(LoweredExecExpr::HttpHeaderCount {
                request: lower_exec_operand(&args[0], env, Some(&request_ty), named_types)?
                    .ok_or_else(|| format!("unsupported http_header_count operand {}", args[0]))?,
            })
        }
        "http_header_name" => {
            let request_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown http_header_name operand type for {}", args[0]))?;
            let index_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown http_header_name operand type for {}", args[1]))?;
            Some(LoweredExecExpr::HttpHeaderName {
                request: lower_exec_operand(&args[0], env, Some(&request_ty), named_types)?
                    .ok_or_else(|| format!("unsupported http_header_name operand {}", args[0]))?,
                index: lower_exec_operand(&args[1], env, Some(&index_ty), named_types)?
                    .ok_or_else(|| format!("unsupported http_header_name operand {}", args[1]))?,
            })
        }
        "http_header_value" => {
            let request_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown http_header_value operand type for {}", args[0]))?;
            let index_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown http_header_value operand type for {}", args[1]))?;
            Some(LoweredExecExpr::HttpHeaderValue {
                request: lower_exec_operand(&args[0], env, Some(&request_ty), named_types)?
                    .ok_or_else(|| format!("unsupported http_header_value operand {}", args[0]))?,
                index: lower_exec_operand(&args[1], env, Some(&index_ty), named_types)?
                    .ok_or_else(|| format!("unsupported http_header_value operand {}", args[1]))?,
            })
        }
        "http_cookie" => {
            let request_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown http_cookie operand type for {}", args[0]))?;
            Some(LoweredExecExpr::HttpCookie {
                request: lower_exec_operand(&args[0], env, Some(&request_ty), named_types)?
                    .ok_or_else(|| format!("unsupported http_cookie operand {}", args[0]))?,
                name: args[1].clone(),
            })
        }
        "http_query_param" => {
            let request_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown http_query_param operand type for {}", args[0]))?;
            Some(LoweredExecExpr::HttpQueryParam {
                request: lower_exec_operand(&args[0], env, Some(&request_ty), named_types)?
                    .ok_or_else(|| format!("unsupported http_query_param operand {}", args[0]))?,
                key: args[1].clone(),
            })
        }
        "http_body" => {
            let request_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown http_body operand type for {}", args[0]))?;
            Some(LoweredExecExpr::HttpBody {
                request: lower_exec_operand(&args[0], env, Some(&request_ty), named_types)?
                    .ok_or_else(|| format!("unsupported http_body operand {}", args[0]))?,
            })
        }
        "http_multipart_part_count" => {
            let request_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!(
                    "unknown http_multipart_part_count operand type for {}",
                    args[0]
                )
            })?;
            Some(LoweredExecExpr::HttpMultipartPartCount {
                request: lower_exec_operand(&args[0], env, Some(&request_ty), named_types)?
                    .ok_or_else(|| {
                        format!("unsupported http_multipart_part_count operand {}", args[0])
                    })?,
            })
        }
        "http_multipart_part_name" => {
            let request_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown http_multipart_part_name operand type for {}", args[0])
            })?;
            let index_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!("unknown http_multipart_part_name operand type for {}", args[1])
            })?;
            Some(LoweredExecExpr::HttpMultipartPartName {
                request: lower_exec_operand(&args[0], env, Some(&request_ty), named_types)?
                    .ok_or_else(|| {
                        format!("unsupported http_multipart_part_name operand {}", args[0])
                    })?,
                index: lower_exec_operand(&args[1], env, Some(&index_ty), named_types)?
                    .ok_or_else(|| {
                        format!("unsupported http_multipart_part_name operand {}", args[1])
                    })?,
            })
        }
        "http_multipart_part_filename" => {
            let request_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!(
                    "unknown http_multipart_part_filename operand type for {}",
                    args[0]
                )
            })?;
            let index_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!(
                    "unknown http_multipart_part_filename operand type for {}",
                    args[1]
                )
            })?;
            Some(LoweredExecExpr::HttpMultipartPartFilename {
                request: lower_exec_operand(&args[0], env, Some(&request_ty), named_types)?
                    .ok_or_else(|| {
                        format!("unsupported http_multipart_part_filename operand {}", args[0])
                    })?,
                index: lower_exec_operand(&args[1], env, Some(&index_ty), named_types)?
                    .ok_or_else(|| {
                        format!("unsupported http_multipart_part_filename operand {}", args[1])
                    })?,
            })
        }
        "http_multipart_part_body" => {
            let request_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown http_multipart_part_body operand type for {}", args[0])
            })?;
            let index_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!("unknown http_multipart_part_body operand type for {}", args[1])
            })?;
            Some(LoweredExecExpr::HttpMultipartPartBody {
                request: lower_exec_operand(&args[0], env, Some(&request_ty), named_types)?
                    .ok_or_else(|| {
                        format!("unsupported http_multipart_part_body operand {}", args[0])
                    })?,
                index: lower_exec_operand(&args[1], env, Some(&index_ty), named_types)?
                    .ok_or_else(|| {
                        format!("unsupported http_multipart_part_body operand {}", args[1])
                    })?,
            })
        }
        "http_body_stream_open" => {
            let request_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown http_body_stream_open operand type for {}", args[0])
            })?;
            Some(LoweredExecExpr::HttpBodyStreamOpen {
                request: lower_exec_operand(&args[0], env, Some(&request_ty), named_types)?
                    .ok_or_else(|| {
                        format!("unsupported http_body_stream_open operand {}", args[0])
                    })?,
            })
        }
        "http_body_stream_next" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown http_body_stream_next operand type for {}", args[0])
            })?;
            let size_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!("unknown http_body_stream_next operand type for {}", args[1])
            })?;
            Some(LoweredExecExpr::HttpBodyStreamNext {
                handle: lower_exec_operand(&args[0], env, Some(&handle_ty), named_types)?
                    .ok_or_else(|| {
                        format!("unsupported http_body_stream_next operand {}", args[0])
                    })?,
                chunk_size: lower_exec_operand(&args[1], env, Some(&size_ty), named_types)?
                    .ok_or_else(|| {
                        format!("unsupported http_body_stream_next operand {}", args[1])
                    })?,
            })
        }
        "http_body_stream_close" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown http_body_stream_close operand type for {}", args[0])
            })?;
            Some(LoweredExecExpr::HttpBodyStreamClose {
                handle: lower_exec_operand(&args[0], env, Some(&handle_ty), named_types)?
                    .ok_or_else(|| {
                        format!("unsupported http_body_stream_close operand {}", args[0])
                    })?,
            })
        }
        "http_body_limit" => {
            let request_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown http_body_limit operand type for {}", args[0]))?;
            let limit_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown http_body_limit operand type for {}", args[1]))?;
            Some(LoweredExecExpr::HttpBodyLimit {
                request: lower_exec_operand(&args[0], env, Some(&request_ty), named_types)?
                    .ok_or_else(|| format!("unsupported http_body_limit operand {}", args[0]))?,
                limit: lower_exec_operand(&args[1], env, Some(&limit_ty), named_types)?
                    .ok_or_else(|| format!("unsupported http_body_limit operand {}", args[1]))?,
            })
        }
        "http_server_config_u32" => Some(LoweredExecExpr::HttpServerConfigU32 {
            token: args[0].clone(),
        }),
        "tls_server_config_u32" => {
            let tls = tls_capability_for_function(function)?;
            let value = match args[0].as_str() {
                "request_timeout_ms" => tls.request_timeout_ms,
                "session_timeout_ms" => tls.session_timeout_ms,
                "shutdown_grace_ms" => tls.shutdown_grace_ms,
                other => return Err(format!("unsupported tls_server_config_u32 token {other}")),
            };
            Some(LoweredExecExpr::TlsServerConfigU32 {
                token: args[0].clone(),
                value,
            })
        }
        "tls_server_config_buf" => {
            let tls = tls_capability_for_function(function)?;
            let value = match args[0].as_str() {
                "cert" => tls.cert,
                "key" => tls.key,
                other => return Err(format!("unsupported tls_server_config_buf token {other}")),
            };
            Some(LoweredExecExpr::TlsServerConfigBuf {
                token: args[0].clone(),
                value,
            })
        }
        "buf_parse_u32" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown buf_parse_u32 operand type for {}", args[0]))?;
            Some(LoweredExecExpr::BufParseU32 {
                value: lower_exec_operand(&args[0], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| format!("unsupported buf_parse_u32 operand {}", args[0]))?,
            })
        }
        "buf_parse_bool" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown buf_parse_bool operand type for {}", args[0]))?;
            Some(LoweredExecExpr::BufParseBool {
                value: lower_exec_operand(&args[0], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| format!("unsupported buf_parse_bool operand {}", args[0]))?,
            })
        }
        "str_from_u32" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown str_from_u32 operand type for {}", args[0]))?;
            Some(LoweredExecExpr::StrFromU32 {
                value: lower_exec_operand(&args[0], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| format!("unsupported str_from_u32 operand {}", args[0]))?,
            })
        }
        "str_from_bool" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown str_from_bool operand type for {}", args[0]))?;
            Some(LoweredExecExpr::StrFromBool {
                value: lower_exec_operand(&args[0], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| format!("unsupported str_from_bool operand {}", args[0]))?,
            })
        }
        "str_eq_lit" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown str_eq_lit operand type for {}", args[0]))?;
            Some(LoweredExecExpr::StrEqLit {
                value: lower_exec_operand(&args[0], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| format!("unsupported str_eq_lit operand {}", args[0]))?,
                literal: args[1].clone(),
            })
        }
        "str_to_buf" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown str_to_buf operand type for {}", args[0]))?;
            Some(LoweredExecExpr::StrToBuf {
                value: lower_exec_operand(&args[0], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| format!("unsupported str_to_buf operand {}", args[0]))?,
            })
        }
        "buf_to_str" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown buf_to_str operand type for {}", args[0]))?;
            Some(LoweredExecExpr::BufToStr {
                value: lower_exec_operand(&args[0], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| format!("unsupported buf_to_str operand {}", args[0]))?,
            })
        }
        "buf_hex_str" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown buf_hex_str operand type for {}", args[0]))?;
            Some(LoweredExecExpr::BufHexStr {
                value: lower_exec_operand(&args[0], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| format!("unsupported buf_hex_str operand {}", args[0]))?,
            })
        }
        "http_write_response" => {
            let status_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!("unknown http_write_response status type for {}", args[1])
            })?;
            let body_ty = resolve_operand_type(&args[2], type_env)
                .ok_or_else(|| format!("unknown http_write_response body type for {}", args[2]))?;
            Some(LoweredExecExpr::HttpWriteResponse {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| format!("unsupported http_write_response handle {}", args[0]))?,
                status: lower_exec_operand(&args[1], env, Some(&status_ty), named_types)?
                    .ok_or_else(|| format!("unsupported http_write_response status {}", args[1]))?,
                body: lower_exec_operand(&args[2], env, Some(&body_ty), named_types)?
                    .ok_or_else(|| format!("unsupported http_write_response body {}", args[2]))?,
            })
        }
        "http_write_text_response" => {
            let status_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!(
                    "unknown http_write_text_response status type for {}",
                    args[1]
                )
            })?;
            let body_ty = resolve_operand_type(&args[2], type_env).ok_or_else(|| {
                format!("unknown http_write_text_response body type for {}", args[2])
            })?;
            Some(LoweredExecExpr::HttpWriteTextResponse {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| {
                    format!("unsupported http_write_text_response handle {}", args[0])
                })?,
                status: lower_exec_operand(&args[1], env, Some(&status_ty), named_types)?
                    .ok_or_else(|| {
                        format!("unsupported http_write_text_response status {}", args[1])
                    })?,
                body: lower_exec_operand(&args[2], env, Some(&body_ty), named_types)?.ok_or_else(
                    || format!("unsupported http_write_text_response body {}", args[2]),
                )?,
            })
        }
        "http_write_text_response_cookie" => {
            let status_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!(
                    "unknown http_write_text_response_cookie status type for {}",
                    args[1]
                )
            })?;
            let body_ty = resolve_operand_type(&args[4], type_env).ok_or_else(|| {
                format!(
                    "unknown http_write_text_response_cookie body type for {}",
                    args[4]
                )
            })?;
            Some(LoweredExecExpr::HttpWriteTextResponseCookie {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| {
                    format!("unsupported http_write_text_response_cookie handle {}", args[0])
                })?,
                status: lower_exec_operand(&args[1], env, Some(&status_ty), named_types)?
                    .ok_or_else(|| {
                        format!("unsupported http_write_text_response_cookie status {}", args[1])
                    })?,
                cookie_name: args[2].clone(),
                cookie_value: args[3].clone(),
                body: lower_exec_operand(&args[4], env, Some(&body_ty), named_types)?.ok_or_else(
                    || format!("unsupported http_write_text_response_cookie body {}", args[4]),
                )?,
            })
        }
        "http_write_text_response_headers2" => {
            let status_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!(
                    "unknown http_write_text_response_headers2 status type for {}",
                    args[1]
                )
            })?;
            let body_ty = resolve_operand_type(&args[6], type_env).ok_or_else(|| {
                format!(
                    "unknown http_write_text_response_headers2 body type for {}",
                    args[6]
                )
            })?;
            Some(LoweredExecExpr::HttpWriteTextResponseHeaders2 {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| {
                    format!("unsupported http_write_text_response_headers2 handle {}", args[0])
                })?,
                status: lower_exec_operand(&args[1], env, Some(&status_ty), named_types)?
                    .ok_or_else(|| {
                        format!("unsupported http_write_text_response_headers2 status {}", args[1])
                    })?,
                header1_name: args[2].clone(),
                header1_value: args[3].clone(),
                header2_name: args[4].clone(),
                header2_value: args[5].clone(),
                body: lower_exec_operand(&args[6], env, Some(&body_ty), named_types)?.ok_or_else(
                    || format!("unsupported http_write_text_response_headers2 body {}", args[6]),
                )?,
            })
        }
        "http_session_write_text" => {
            let status_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!(
                    "unknown http_session_write_text status type for {}",
                    args[1]
                )
            })?;
            let body_ty = resolve_operand_type(&args[2], type_env).ok_or_else(|| {
                format!("unknown http_session_write_text body type for {}", args[2])
            })?;
            Some(LoweredExecExpr::HttpSessionWriteText {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| format!("unsupported http_session_write_text handle {}", args[0]))?,
                status: lower_exec_operand(&args[1], env, Some(&status_ty), named_types)?
                    .ok_or_else(|| {
                        format!("unsupported http_session_write_text status {}", args[1])
                    })?,
                body: lower_exec_operand(&args[2], env, Some(&body_ty), named_types)?.ok_or_else(
                    || format!("unsupported http_session_write_text body {}", args[2]),
                )?,
            })
        }
        "http_session_write_text_cookie" => {
            let status_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!(
                    "unknown http_session_write_text_cookie status type for {}",
                    args[1]
                )
            })?;
            let body_ty = resolve_operand_type(&args[4], type_env).ok_or_else(|| {
                format!(
                    "unknown http_session_write_text_cookie body type for {}",
                    args[4]
                )
            })?;
            Some(LoweredExecExpr::HttpSessionWriteTextCookie {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| {
                    format!("unsupported http_session_write_text_cookie handle {}", args[0])
                })?,
                status: lower_exec_operand(&args[1], env, Some(&status_ty), named_types)?
                    .ok_or_else(|| {
                        format!("unsupported http_session_write_text_cookie status {}", args[1])
                    })?,
                cookie_name: args[2].clone(),
                cookie_value: args[3].clone(),
                body: lower_exec_operand(&args[4], env, Some(&body_ty), named_types)?.ok_or_else(
                    || format!("unsupported http_session_write_text_cookie body {}", args[4]),
                )?,
            })
        }
        "http_session_write_text_headers2" => {
            let status_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!(
                    "unknown http_session_write_text_headers2 status type for {}",
                    args[1]
                )
            })?;
            let body_ty = resolve_operand_type(&args[6], type_env).ok_or_else(|| {
                format!(
                    "unknown http_session_write_text_headers2 body type for {}",
                    args[6]
                )
            })?;
            Some(LoweredExecExpr::HttpSessionWriteTextHeaders2 {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| {
                    format!("unsupported http_session_write_text_headers2 handle {}", args[0])
                })?,
                status: lower_exec_operand(&args[1], env, Some(&status_ty), named_types)?
                    .ok_or_else(|| {
                        format!("unsupported http_session_write_text_headers2 status {}", args[1])
                    })?,
                header1_name: args[2].clone(),
                header1_value: args[3].clone(),
                header2_name: args[4].clone(),
                header2_value: args[5].clone(),
                body: lower_exec_operand(&args[6], env, Some(&body_ty), named_types)?.ok_or_else(
                    || format!("unsupported http_session_write_text_headers2 body {}", args[6]),
                )?,
            })
        }
        "http_write_json_response" => {
            let status_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!(
                    "unknown http_write_json_response status type for {}",
                    args[1]
                )
            })?;
            let body_ty = resolve_operand_type(&args[2], type_env).ok_or_else(|| {
                format!("unknown http_write_json_response body type for {}", args[2])
            })?;
            Some(LoweredExecExpr::HttpWriteJsonResponse {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| {
                    format!("unsupported http_write_json_response handle {}", args[0])
                })?,
                status: lower_exec_operand(&args[1], env, Some(&status_ty), named_types)?
                    .ok_or_else(|| {
                        format!("unsupported http_write_json_response status {}", args[1])
                    })?,
                body: lower_exec_operand(&args[2], env, Some(&body_ty), named_types)?.ok_or_else(
                    || format!("unsupported http_write_json_response body {}", args[2]),
                )?,
            })
        }
        "http_write_json_response_cookie" => {
            let status_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!(
                    "unknown http_write_json_response_cookie status type for {}",
                    args[1]
                )
            })?;
            let body_ty = resolve_operand_type(&args[4], type_env).ok_or_else(|| {
                format!(
                    "unknown http_write_json_response_cookie body type for {}",
                    args[4]
                )
            })?;
            Some(LoweredExecExpr::HttpWriteJsonResponseCookie {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| {
                    format!("unsupported http_write_json_response_cookie handle {}", args[0])
                })?,
                status: lower_exec_operand(&args[1], env, Some(&status_ty), named_types)?
                    .ok_or_else(|| {
                        format!("unsupported http_write_json_response_cookie status {}", args[1])
                    })?,
                cookie_name: args[2].clone(),
                cookie_value: args[3].clone(),
                body: lower_exec_operand(&args[4], env, Some(&body_ty), named_types)?.ok_or_else(
                    || format!("unsupported http_write_json_response_cookie body {}", args[4]),
                )?,
            })
        }
        "http_write_json_response_headers2" => {
            let status_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!(
                    "unknown http_write_json_response_headers2 status type for {}",
                    args[1]
                )
            })?;
            let body_ty = resolve_operand_type(&args[6], type_env).ok_or_else(|| {
                format!(
                    "unknown http_write_json_response_headers2 body type for {}",
                    args[6]
                )
            })?;
            Some(LoweredExecExpr::HttpWriteJsonResponseHeaders2 {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| {
                    format!("unsupported http_write_json_response_headers2 handle {}", args[0])
                })?,
                status: lower_exec_operand(&args[1], env, Some(&status_ty), named_types)?
                    .ok_or_else(|| {
                        format!("unsupported http_write_json_response_headers2 status {}", args[1])
                    })?,
                header1_name: args[2].clone(),
                header1_value: args[3].clone(),
                header2_name: args[4].clone(),
                header2_value: args[5].clone(),
                body: lower_exec_operand(&args[6], env, Some(&body_ty), named_types)?.ok_or_else(
                    || format!("unsupported http_write_json_response_headers2 body {}", args[6]),
                )?,
            })
        }
        "http_session_write_json" => {
            let status_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!(
                    "unknown http_session_write_json status type for {}",
                    args[1]
                )
            })?;
            let body_ty = resolve_operand_type(&args[2], type_env).ok_or_else(|| {
                format!("unknown http_session_write_json body type for {}", args[2])
            })?;
            Some(LoweredExecExpr::HttpSessionWriteJson {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| format!("unsupported http_session_write_json handle {}", args[0]))?,
                status: lower_exec_operand(&args[1], env, Some(&status_ty), named_types)?
                    .ok_or_else(|| {
                        format!("unsupported http_session_write_json status {}", args[1])
                    })?,
                body: lower_exec_operand(&args[2], env, Some(&body_ty), named_types)?.ok_or_else(
                    || format!("unsupported http_session_write_json body {}", args[2]),
                )?,
            })
        }
        "http_session_write_json_cookie" => {
            let status_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!(
                    "unknown http_session_write_json_cookie status type for {}",
                    args[1]
                )
            })?;
            let body_ty = resolve_operand_type(&args[4], type_env).ok_or_else(|| {
                format!(
                    "unknown http_session_write_json_cookie body type for {}",
                    args[4]
                )
            })?;
            Some(LoweredExecExpr::HttpSessionWriteJsonCookie {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| {
                    format!("unsupported http_session_write_json_cookie handle {}", args[0])
                })?,
                status: lower_exec_operand(&args[1], env, Some(&status_ty), named_types)?
                    .ok_or_else(|| {
                        format!("unsupported http_session_write_json_cookie status {}", args[1])
                    })?,
                cookie_name: args[2].clone(),
                cookie_value: args[3].clone(),
                body: lower_exec_operand(&args[4], env, Some(&body_ty), named_types)?.ok_or_else(
                    || format!("unsupported http_session_write_json_cookie body {}", args[4]),
                )?,
            })
        }
        "http_session_write_json_headers2" => {
            let status_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!(
                    "unknown http_session_write_json_headers2 status type for {}",
                    args[1]
                )
            })?;
            let body_ty = resolve_operand_type(&args[6], type_env).ok_or_else(|| {
                format!(
                    "unknown http_session_write_json_headers2 body type for {}",
                    args[6]
                )
            })?;
            Some(LoweredExecExpr::HttpSessionWriteJsonHeaders2 {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| {
                    format!("unsupported http_session_write_json_headers2 handle {}", args[0])
                })?,
                status: lower_exec_operand(&args[1], env, Some(&status_ty), named_types)?
                    .ok_or_else(|| {
                        format!("unsupported http_session_write_json_headers2 status {}", args[1])
                    })?,
                header1_name: args[2].clone(),
                header1_value: args[3].clone(),
                header2_name: args[4].clone(),
                header2_value: args[5].clone(),
                body: lower_exec_operand(&args[6], env, Some(&body_ty), named_types)?.ok_or_else(
                    || format!("unsupported http_session_write_json_headers2 body {}", args[6]),
                )?,
            })
        }
        "http_write_response_header" => {
            let status_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!(
                    "unknown http_write_response_header status type for {}",
                    args[1]
                )
            })?;
            let body_ty = resolve_operand_type(&args[4], type_env).ok_or_else(|| {
                format!(
                    "unknown http_write_response_header body type for {}",
                    args[4]
                )
            })?;
            Some(LoweredExecExpr::HttpWriteResponseHeader {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| {
                    format!("unsupported http_write_response_header handle {}", args[0])
                })?,
                status: lower_exec_operand(&args[1], env, Some(&status_ty), named_types)?
                    .ok_or_else(|| {
                        format!("unsupported http_write_response_header status {}", args[1])
                    })?,
                header_name: args[2].clone(),
                header_value: args[3].clone(),
                body: lower_exec_operand(&args[4], env, Some(&body_ty), named_types)?.ok_or_else(
                    || format!("unsupported http_write_response_header body {}", args[4]),
                )?,
            })
        }
        "http_response_stream_open" => {
            let status_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!(
                    "unknown http_response_stream_open status type for {}",
                    args[1]
                )
            })?;
            Some(LoweredExecExpr::HttpResponseStreamOpen {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| {
                    format!("unsupported http_response_stream_open handle {}", args[0])
                })?,
                status: lower_exec_operand(&args[1], env, Some(&status_ty), named_types)?
                    .ok_or_else(|| {
                        format!("unsupported http_response_stream_open status {}", args[1])
                    })?,
                content_type: args[2].clone(),
            })
        }
        "http_response_stream_write" => {
            let body_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!(
                    "unknown http_response_stream_write body type for {}",
                    args[1]
                )
            })?;
            Some(LoweredExecExpr::HttpResponseStreamWrite {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| {
                    format!("unsupported http_response_stream_write handle {}", args[0])
                })?,
                body: lower_exec_operand(&args[1], env, Some(&body_ty), named_types)?
                    .ok_or_else(|| {
                        format!("unsupported http_response_stream_write body {}", args[1])
                    })?,
            })
        }
        "http_response_stream_close" => Some(LoweredExecExpr::HttpResponseStreamClose {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported http_response_stream_close handle {}", args[0]))?,
        }),
        "http_client_open" => {
            let (host, port) = net_endpoint_for_function(function)?;
            Some(LoweredExecExpr::HttpClientOpen {
                host,
                port,
            })
        }
        "http_client_request" => {
            let request_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown http_client_request body type for {}", args[1]))?;
            Some(LoweredExecExpr::HttpClientRequest {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| format!("unsupported http_client_request handle {}", args[0]))?,
                request: lower_exec_operand(&args[1], env, Some(&request_ty), named_types)?
                    .ok_or_else(|| format!("unsupported http_client_request body {}", args[1]))?,
            })
        }
        "http_client_request_retry" => {
            let retries_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!(
                    "unknown http_client_request_retry retries type for {}",
                    args[1]
                )
            })?;
            let backoff_ty = resolve_operand_type(&args[2], type_env).ok_or_else(|| {
                format!(
                    "unknown http_client_request_retry backoff type for {}",
                    args[2]
                )
            })?;
            let request_ty = resolve_operand_type(&args[3], type_env).ok_or_else(|| {
                format!(
                    "unknown http_client_request_retry body type for {}",
                    args[3]
                )
            })?;
            Some(LoweredExecExpr::HttpClientRequestRetry {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| {
                    format!("unsupported http_client_request_retry handle {}", args[0])
                })?,
                retries: lower_exec_operand(&args[1], env, Some(&retries_ty), named_types)?
                    .ok_or_else(|| {
                        format!("unsupported http_client_request_retry retries {}", args[1])
                    })?,
                backoff_ms: lower_exec_operand(&args[2], env, Some(&backoff_ty), named_types)?
                    .ok_or_else(|| {
                        format!("unsupported http_client_request_retry backoff {}", args[2])
                    })?,
                request: lower_exec_operand(&args[3], env, Some(&request_ty), named_types)?
                    .ok_or_else(|| {
                        format!("unsupported http_client_request_retry body {}", args[3])
                    })?,
            })
        }
        "http_client_close" => Some(LoweredExecExpr::HttpClientClose {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported http_client_close handle {}", args[0]))?,
        }),
        "http_client_pool_open" => {
            let (host, port) = net_endpoint_for_function(function)?;
            let max_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown http_client_pool_open max type for {}", args[0])
            })?;
            Some(LoweredExecExpr::HttpClientPoolOpen {
                host,
                port,
                max_size: lower_exec_operand(&args[0], env, Some(&max_ty), named_types)?
                    .ok_or_else(|| format!("unsupported http_client_pool_open max {}", args[0]))?,
            })
        }
        "http_client_pool_acquire" => Some(LoweredExecExpr::HttpClientPoolAcquire {
            pool: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported http_client_pool_acquire pool {}", args[0]))?,
        }),
        "http_client_pool_release" => Some(LoweredExecExpr::HttpClientPoolRelease {
            pool: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported http_client_pool_release pool {}", args[0]))?,
            handle: lower_exec_operand(
                &args[1],
                env,
                resolve_operand_type(&args[1], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported http_client_pool_release handle {}", args[1]))?,
        }),
        "http_client_pool_close" => Some(LoweredExecExpr::HttpClientPoolClose {
            pool: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported http_client_pool_close pool {}", args[0]))?,
        }),
        "json_get_u32" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown json_get_u32 operand type for {}", args[0]))?;
            Some(LoweredExecExpr::JsonGetU32 {
                value: lower_exec_operand(&args[0], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| format!("unsupported json_get_u32 operand {}", args[0]))?,
                key: args[1].clone(),
            })
        }
        "strmap_get_u32" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown strmap_get_u32 operand type for {}", args[0]))?;
            Some(LoweredExecExpr::JsonGetU32 {
                value: lower_exec_operand(&args[0], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| format!("unsupported strmap_get_u32 operand {}", args[0]))?,
                key: args[1].clone(),
            })
        }
        "json_get_bool" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown json_get_bool operand type for {}", args[0]))?;
            Some(LoweredExecExpr::JsonGetBool {
                value: lower_exec_operand(&args[0], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| format!("unsupported json_get_bool operand {}", args[0]))?,
                key: args[1].clone(),
            })
        }
        "json_has_key" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown json_has_key operand type for {}", args[0]))?;
            Some(LoweredExecExpr::JsonHasKey {
                value: lower_exec_operand(&args[0], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| format!("unsupported json_has_key operand {}", args[0]))?,
                key: args[1].clone(),
            })
        }
        "strmap_get_bool" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown strmap_get_bool operand type for {}", args[0]))?;
            Some(LoweredExecExpr::JsonGetBool {
                value: lower_exec_operand(&args[0], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| format!("unsupported strmap_get_bool operand {}", args[0]))?,
                key: args[1].clone(),
            })
        }
        "json_get_buf" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown json_get_buf operand type for {}", args[0]))?;
            Some(LoweredExecExpr::JsonGetBufU8 {
                value: lower_exec_operand(&args[0], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| format!("unsupported json_get_buf operand {}", args[0]))?,
                key: args[1].clone(),
            })
        }
        "json_get_str" | "strmap_get_str" => {
            let value_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown {} operand type for {}", instruction.op, args[0])
            })?;
            Some(LoweredExecExpr::JsonGetStr {
                value: lower_exec_operand(&args[0], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| format!("unsupported {} operand {}", instruction.op, args[0]))?,
                key: args[1].clone(),
            })
        }
        "json_get_u32_or" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown json_get_u32_or operand type for {}", args[0]))?;
            let default_ty = resolve_operand_type(&args[2], type_env)
                .ok_or_else(|| format!("unknown json_get_u32_or operand type for {}", args[2]))?;
            Some(LoweredExecExpr::JsonGetU32Or {
                value: lower_exec_operand(&args[0], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| format!("unsupported json_get_u32_or operand {}", args[0]))?,
                key: args[1].clone(),
                default_value: lower_exec_operand(&args[2], env, Some(&default_ty), named_types)?
                    .ok_or_else(|| format!("unsupported json_get_u32_or operand {}", args[2]))?,
            })
        }
        "json_get_bool_or" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown json_get_bool_or operand type for {}", args[0]))?;
            let default_ty = resolve_operand_type(&args[2], type_env)
                .ok_or_else(|| format!("unknown json_get_bool_or operand type for {}", args[2]))?;
            Some(LoweredExecExpr::JsonGetBoolOr {
                value: lower_exec_operand(&args[0], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| format!("unsupported json_get_bool_or operand {}", args[0]))?,
                key: args[1].clone(),
                default_value: lower_exec_operand(&args[2], env, Some(&default_ty), named_types)?
                    .ok_or_else(|| format!("unsupported json_get_bool_or operand {}", args[2]))?,
            })
        }
        "json_get_buf_or" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown json_get_buf_or operand type for {}", args[0]))?;
            let default_ty = resolve_operand_type(&args[2], type_env)
                .ok_or_else(|| format!("unknown json_get_buf_or operand type for {}", args[2]))?;
            Some(LoweredExecExpr::JsonGetBufOr {
                value: lower_exec_operand(&args[0], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| format!("unsupported json_get_buf_or operand {}", args[0]))?,
                key: args[1].clone(),
                default_value: lower_exec_operand(&args[2], env, Some(&default_ty), named_types)?
                    .ok_or_else(|| format!("unsupported json_get_buf_or operand {}", args[2]))?,
            })
        }
        "json_get_str_or" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown json_get_str_or operand type for {}", args[0]))?;
            let default_ty = resolve_operand_type(&args[2], type_env)
                .ok_or_else(|| format!("unknown json_get_str_or operand type for {}", args[2]))?;
            Some(LoweredExecExpr::JsonGetStrOr {
                value: lower_exec_operand(&args[0], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| format!("unsupported json_get_str_or operand {}", args[0]))?,
                key: args[1].clone(),
                default_value: lower_exec_operand(&args[2], env, Some(&default_ty), named_types)?
                    .ok_or_else(|| format!("unsupported json_get_str_or operand {}", args[2]))?,
            })
        }
        "json_array_len" | "strlist_len" => {
            let value_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown {} operand type for {}", instruction.op, args[0])
            })?;
            Some(LoweredExecExpr::JsonArrayLen {
                value: lower_exec_operand(&args[0], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| format!("unsupported {} operand {}", instruction.op, args[0]))?,
            })
        }
        "json_index_u32" | "strlist_index_u32" => {
            let value_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown {} operand type for {}", instruction.op, args[0])
            })?;
            let index_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!("unknown {} operand type for {}", instruction.op, args[1])
            })?;
            Some(LoweredExecExpr::JsonIndexU32 {
                value: lower_exec_operand(&args[0], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| format!("unsupported {} operand {}", instruction.op, args[0]))?,
                index: lower_exec_operand(&args[1], env, Some(&index_ty), named_types)?
                    .ok_or_else(|| format!("unsupported {} operand {}", instruction.op, args[1]))?,
            })
        }
        "json_index_bool" | "strlist_index_bool" => {
            let value_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown {} operand type for {}", instruction.op, args[0])
            })?;
            let index_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!("unknown {} operand type for {}", instruction.op, args[1])
            })?;
            Some(LoweredExecExpr::JsonIndexBool {
                value: lower_exec_operand(&args[0], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| format!("unsupported {} operand {}", instruction.op, args[0]))?,
                index: lower_exec_operand(&args[1], env, Some(&index_ty), named_types)?
                    .ok_or_else(|| format!("unsupported {} operand {}", instruction.op, args[1]))?,
            })
        }
        "json_index_str" | "strlist_index_str" => {
            let value_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown {} operand type for {}", instruction.op, args[0])
            })?;
            let index_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!("unknown {} operand type for {}", instruction.op, args[1])
            })?;
            Some(LoweredExecExpr::JsonIndexStr {
                value: lower_exec_operand(&args[0], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| format!("unsupported {} operand {}", instruction.op, args[0]))?,
                index: lower_exec_operand(&args[1], env, Some(&index_ty), named_types)?
                    .ok_or_else(|| format!("unsupported {} operand {}", instruction.op, args[1]))?,
            })
        }
        "json_encode_obj" => {
            let mut entries = Vec::new();
            for pair in args.chunks(2) {
                let value_ty = resolve_operand_type(&pair[1], type_env).ok_or_else(|| {
                    format!("unknown json_encode_obj operand type for {}", pair[1])
                })?;
                let value = lower_exec_operand(&pair[1], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| format!("unsupported json_encode_obj operand {}", pair[1]))?;
                entries.push((pair[0].clone(), value));
            }
            Some(LoweredExecExpr::JsonEncodeObj { entries })
        }
        "json_encode_arr" => {
            let mut values = Vec::new();
            for operand in args {
                let value_ty = resolve_operand_type(operand, type_env)
                    .ok_or_else(|| format!("unknown json_encode_arr operand type for {operand}"))?;
                values.push(
                    lower_exec_operand(operand, env, Some(&value_ty), named_types)?
                        .ok_or_else(|| format!("unsupported json_encode_arr operand {operand}"))?,
                );
            }
            Some(LoweredExecExpr::JsonEncodeArr { values })
        }
        "config_get_u32" => {
            let value = config_entry_for_function(function, &args[0])?
                .parse::<u32>()
                .map_err(|error| {
                    format!("invalid config_get_u32 value for {}: {error}", args[0])
                })?;
            Some(LoweredExecExpr::ConfigGetU32 {
                key: args[0].clone(),
                value,
            })
        }
        "config_get_bool" => {
            let value = parse_bool_text(&config_entry_for_function(function, &args[0])?)
                .ok_or_else(|| format!("invalid config_get_bool value for {}", args[0]))?;
            Some(LoweredExecExpr::ConfigGetBool {
                key: args[0].clone(),
                value,
            })
        }
        "config_get_str" => Some(LoweredExecExpr::ConfigGetStr {
            key: args[0].clone(),
            value: config_entry_for_function(function, &args[0])?,
        }),
        "config_has" => Some(LoweredExecExpr::ConfigHas {
            key: args[0].clone(),
            present: config_entry_for_function(function, &args[0]).is_ok(),
        }),
        "env_get_u32" => Some(LoweredExecExpr::EnvGetU32 {
            key: args[0].clone(),
        }),
        "env_get_bool" => Some(LoweredExecExpr::EnvGetBool {
            key: args[0].clone(),
        }),
        "env_get_str" => Some(LoweredExecExpr::EnvGetStr {
            key: args[0].clone(),
        }),
        "env_has" => Some(LoweredExecExpr::EnvHas {
            key: args[0].clone(),
        }),
        "buf_before_lit" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown buf_before_lit operand type for {}", args[0]))?;
            Some(LoweredExecExpr::BufBeforeLit {
                value: lower_exec_operand(&args[0], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| format!("unsupported buf_before_lit operand {}", args[0]))?,
                literal: args[1].clone(),
            })
        }
        "buf_after_lit" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown buf_after_lit operand type for {}", args[0]))?;
            Some(LoweredExecExpr::BufAfterLit {
                value: lower_exec_operand(&args[0], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| format!("unsupported buf_after_lit operand {}", args[0]))?,
                literal: args[1].clone(),
            })
        }
        "buf_trim_ascii" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown buf_trim_ascii operand type for {}", args[0]))?;
            Some(LoweredExecExpr::BufTrimAscii {
                value: lower_exec_operand(&args[0], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| format!("unsupported buf_trim_ascii operand {}", args[0]))?,
            })
        }
        "date_parse_ymd" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown date_parse_ymd operand type for {}", args[0]))?;
            Some(LoweredExecExpr::DateParseYmd {
                value: lower_exec_operand(&args[0], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| format!("unsupported date_parse_ymd operand {}", args[0]))?,
            })
        }
        "time_parse_hms" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown time_parse_hms operand type for {}", args[0]))?;
            Some(LoweredExecExpr::TimeParseHms {
                value: lower_exec_operand(&args[0], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| format!("unsupported time_parse_hms operand {}", args[0]))?,
            })
        }
        "date_format_ymd" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown date_format_ymd operand type for {}", args[0]))?;
            Some(LoweredExecExpr::DateFormatYmd {
                value: lower_exec_operand(&args[0], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| format!("unsupported date_format_ymd operand {}", args[0]))?,
            })
        }
        "time_format_hms" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown time_format_hms operand type for {}", args[0]))?;
            Some(LoweredExecExpr::TimeFormatHms {
                value: lower_exec_operand(&args[0], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| format!("unsupported time_format_hms operand {}", args[0]))?,
            })
        }
        "db_open" => Some(LoweredExecExpr::DbOpen {
            path: args[0].clone(),
        }),
        "db_close" => Some(LoweredExecExpr::DbClose {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported db_close operand {}", args[0]))?,
        }),
        "db_exec" => {
            let sql_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown db_exec sql type for {}", args[1]))?;
            Some(LoweredExecExpr::DbExec {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| format!("unsupported db_exec handle {}", args[0]))?,
                sql: lower_exec_operand(&args[1], env, Some(&sql_ty), named_types)?
                    .ok_or_else(|| format!("unsupported db_exec sql {}", args[1]))?,
            })
        }
        "db_prepare" => {
            let sql_ty = resolve_operand_type(&args[2], type_env)
                .ok_or_else(|| format!("unknown db_prepare sql type for {}", args[2]))?;
            Some(LoweredExecExpr::DbPrepare {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| format!("unsupported db_prepare handle {}", args[0]))?,
                name: args[1].clone(),
                sql: lower_exec_operand(&args[2], env, Some(&sql_ty), named_types)?
                    .ok_or_else(|| format!("unsupported db_prepare sql {}", args[2]))?,
            })
        }
        "db_exec_prepared" => {
            let params_ty = resolve_operand_type(&args[2], type_env)
                .ok_or_else(|| format!("unknown db_exec_prepared params type for {}", args[2]))?;
            Some(LoweredExecExpr::DbExecPrepared {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| format!("unsupported db_exec_prepared handle {}", args[0]))?,
                name: args[1].clone(),
                params: lower_exec_operand(&args[2], env, Some(&params_ty), named_types)?
                    .ok_or_else(|| format!("unsupported db_exec_prepared params {}", args[2]))?,
            })
        }
        "db_query_u32" => {
            let sql_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown db_query_u32 sql type for {}", args[1]))?;
            Some(LoweredExecExpr::DbQueryU32 {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| format!("unsupported db_query_u32 handle {}", args[0]))?,
                sql: lower_exec_operand(&args[1], env, Some(&sql_ty), named_types)?
                    .ok_or_else(|| format!("unsupported db_query_u32 sql {}", args[1]))?,
            })
        }
        "db_query_buf" => {
            let sql_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown db_query_buf sql type for {}", args[1]))?;
            Some(LoweredExecExpr::DbQueryBufU8 {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| format!("unsupported db_query_buf handle {}", args[0]))?,
                sql: lower_exec_operand(&args[1], env, Some(&sql_ty), named_types)?
                    .ok_or_else(|| format!("unsupported db_query_buf sql {}", args[1]))?,
            })
        }
        "db_query_row" => {
            let sql_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown db_query_row sql type for {}", args[1]))?;
            Some(LoweredExecExpr::DbQueryRow {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| format!("unsupported db_query_row handle {}", args[0]))?,
                sql: lower_exec_operand(&args[1], env, Some(&sql_ty), named_types)?
                    .ok_or_else(|| format!("unsupported db_query_row sql {}", args[1]))?,
            })
        }
        "db_query_prepared_u32" => {
            let params_ty = resolve_operand_type(&args[2], type_env).ok_or_else(|| {
                format!("unknown db_query_prepared_u32 params type for {}", args[2])
            })?;
            Some(LoweredExecExpr::DbQueryPreparedU32 {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| format!("unsupported db_query_prepared_u32 handle {}", args[0]))?,
                name: args[1].clone(),
                params: lower_exec_operand(&args[2], env, Some(&params_ty), named_types)?
                    .ok_or_else(|| {
                        format!("unsupported db_query_prepared_u32 params {}", args[2])
                    })?,
            })
        }
        "db_query_prepared_buf" => {
            let params_ty = resolve_operand_type(&args[2], type_env).ok_or_else(|| {
                format!("unknown db_query_prepared_buf params type for {}", args[2])
            })?;
            Some(LoweredExecExpr::DbQueryPreparedBufU8 {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| format!("unsupported db_query_prepared_buf handle {}", args[0]))?,
                name: args[1].clone(),
                params: lower_exec_operand(&args[2], env, Some(&params_ty), named_types)?
                    .ok_or_else(|| {
                        format!("unsupported db_query_prepared_buf params {}", args[2])
                    })?,
            })
        }
        "db_query_prepared_row" => {
            let params_ty = resolve_operand_type(&args[2], type_env).ok_or_else(|| {
                format!("unknown db_query_prepared_row params type for {}", args[2])
            })?;
            Some(LoweredExecExpr::DbQueryPreparedRow {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| format!("unsupported db_query_prepared_row handle {}", args[0]))?,
                name: args[1].clone(),
                params: lower_exec_operand(&args[2], env, Some(&params_ty), named_types)?
                    .ok_or_else(|| {
                        format!("unsupported db_query_prepared_row params {}", args[2])
                    })?,
            })
        }
        "db_row_found" => Some(LoweredExecExpr::DbRowFound {
            row: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported db_row_found row {}", args[0]))?,
        }),
        "db_last_error_code" => Some(LoweredExecExpr::DbLastErrorCode {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported db_last_error_code handle {}", args[0]))?,
        }),
        "db_last_error_retryable" => Some(LoweredExecExpr::DbLastErrorRetryable {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported db_last_error_retryable handle {}", args[0]))?,
        }),
        "db_begin" => Some(LoweredExecExpr::DbBegin {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported db_begin handle {}", args[0]))?,
        }),
        "db_commit" => Some(LoweredExecExpr::DbCommit {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported db_commit handle {}", args[0]))?,
        }),
        "db_rollback" => Some(LoweredExecExpr::DbRollback {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported db_rollback handle {}", args[0]))?,
        }),
        "db_pool_open" => {
            let max_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown db_pool_open max type for {}", args[1]))?;
            Some(LoweredExecExpr::DbPoolOpen {
                target: args[0].clone(),
                max_size: lower_exec_operand(&args[1], env, Some(&max_ty), named_types)?
                    .ok_or_else(|| format!("unsupported db_pool_open max {}", args[1]))?,
            })
        }
        "db_pool_set_max_idle" => {
            let value_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown db_pool_set_max_idle value type for {}", args[1]))?;
            Some(LoweredExecExpr::DbPoolSetMaxIdle {
                pool: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| format!("unsupported db_pool_set_max_idle pool {}", args[0]))?,
                value: lower_exec_operand(&args[1], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| format!("unsupported db_pool_set_max_idle value {}", args[1]))?,
            })
        }
        "db_pool_leased" => Some(LoweredExecExpr::DbPoolLeased {
            pool: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported db_pool_leased pool {}", args[0]))?,
        }),
        "db_pool_acquire" => Some(LoweredExecExpr::DbPoolAcquire {
            pool: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported db_pool_acquire pool {}", args[0]))?,
        }),
        "db_pool_release" => Some(LoweredExecExpr::DbPoolRelease {
            pool: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported db_pool_release pool {}", args[0]))?,
            handle: lower_exec_operand(
                &args[1],
                env,
                resolve_operand_type(&args[1], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported db_pool_release handle {}", args[1]))?,
        }),
        "db_pool_close" => Some(LoweredExecExpr::DbPoolClose {
            pool: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported db_pool_close pool {}", args[0]))?,
        }),
        "cache_open" => Some(LoweredExecExpr::CacheOpen {
            target: args[0].clone(),
        }),
        "cache_close" => Some(LoweredExecExpr::CacheClose {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported cache_close handle {}", args[0]))?,
        }),
        "cache_get_buf" => {
            let key_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown cache_get_buf key type for {}", args[1]))?;
            Some(LoweredExecExpr::CacheGetBufU8 {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| format!("unsupported cache_get_buf handle {}", args[0]))?,
                key: lower_exec_operand(&args[1], env, Some(&key_ty), named_types)?
                    .ok_or_else(|| format!("unsupported cache_get_buf key {}", args[1]))?,
            })
        }
        "cache_set_buf" => {
            let key_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown cache_set_buf key type for {}", args[1]))?;
            let value_ty = resolve_operand_type(&args[2], type_env)
                .ok_or_else(|| format!("unknown cache_set_buf value type for {}", args[2]))?;
            Some(LoweredExecExpr::CacheSetBufU8 {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| format!("unsupported cache_set_buf handle {}", args[0]))?,
                key: lower_exec_operand(&args[1], env, Some(&key_ty), named_types)?
                    .ok_or_else(|| format!("unsupported cache_set_buf key {}", args[1]))?,
                value: lower_exec_operand(&args[2], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| format!("unsupported cache_set_buf value {}", args[2]))?,
            })
        }
        "cache_set_buf_ttl" => {
            let key_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown cache_set_buf_ttl key type for {}", args[1]))?;
            let ttl_ty = resolve_operand_type(&args[2], type_env)
                .ok_or_else(|| format!("unknown cache_set_buf_ttl ttl type for {}", args[2]))?;
            let value_ty = resolve_operand_type(&args[3], type_env)
                .ok_or_else(|| format!("unknown cache_set_buf_ttl value type for {}", args[3]))?;
            Some(LoweredExecExpr::CacheSetBufTtlU8 {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| format!("unsupported cache_set_buf_ttl handle {}", args[0]))?,
                key: lower_exec_operand(&args[1], env, Some(&key_ty), named_types)?
                    .ok_or_else(|| format!("unsupported cache_set_buf_ttl key {}", args[1]))?,
                ttl_ms: lower_exec_operand(&args[2], env, Some(&ttl_ty), named_types)?
                    .ok_or_else(|| format!("unsupported cache_set_buf_ttl ttl {}", args[2]))?,
                value: lower_exec_operand(&args[3], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| format!("unsupported cache_set_buf_ttl value {}", args[3]))?,
            })
        }
        "cache_del" => {
            let key_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown cache_del key type for {}", args[1]))?;
            Some(LoweredExecExpr::CacheDel {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| format!("unsupported cache_del handle {}", args[0]))?,
                key: lower_exec_operand(&args[1], env, Some(&key_ty), named_types)?
                    .ok_or_else(|| format!("unsupported cache_del key {}", args[1]))?,
            })
        }
        "queue_open" => Some(LoweredExecExpr::QueueOpen {
            target: args[0].clone(),
        }),
        "queue_close" => Some(LoweredExecExpr::QueueClose {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported queue_close handle {}", args[0]))?,
        }),
        "queue_push_buf" => {
            let value_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown queue_push_buf value type for {}", args[1]))?;
            Some(LoweredExecExpr::QueuePushBufU8 {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| format!("unsupported queue_push_buf handle {}", args[0]))?,
                value: lower_exec_operand(&args[1], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| format!("unsupported queue_push_buf value {}", args[1]))?,
            })
        }
        "queue_pop_buf" => Some(LoweredExecExpr::QueuePopBufU8 {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported queue_pop_buf handle {}", args[0]))?,
        }),
        "queue_len" => Some(LoweredExecExpr::QueueLen {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported queue_len handle {}", args[0]))?,
        }),
        "stream_open" => Some(LoweredExecExpr::StreamOpen {
            target: args[0].clone(),
        }),
        "stream_close" => Some(LoweredExecExpr::StreamClose {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported stream_close handle {}", args[0]))?,
        }),
        "stream_publish_buf" => {
            let value_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown stream_publish_buf value type for {}", args[1]))?;
            Some(LoweredExecExpr::StreamPublishBufU8 {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| format!("unsupported stream_publish_buf handle {}", args[0]))?,
                value: lower_exec_operand(&args[1], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| format!("unsupported stream_publish_buf value {}", args[1]))?,
            })
        }
        "stream_len" => Some(LoweredExecExpr::StreamLen {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported stream_len handle {}", args[0]))?,
        }),
        "stream_replay_open" => {
            let offset_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown stream_replay_open offset type for {}", args[1]))?;
            Some(LoweredExecExpr::StreamReplayOpen {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| format!("unsupported stream_replay_open handle {}", args[0]))?,
                from_offset: lower_exec_operand(&args[1], env, Some(&offset_ty), named_types)?
                    .ok_or_else(|| format!("unsupported stream_replay_open offset {}", args[1]))?,
            })
        }
        "stream_replay_next" => Some(LoweredExecExpr::StreamReplayNextU8 {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported stream_replay_next handle {}", args[0]))?,
        }),
        "stream_replay_offset" => Some(LoweredExecExpr::StreamReplayOffset {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported stream_replay_offset handle {}", args[0]))?,
        }),
        "stream_replay_close" => Some(LoweredExecExpr::StreamReplayClose {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported stream_replay_close handle {}", args[0]))?,
        }),
        "shard_route_u32" => {
            let key_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown shard_route_u32 key type for {}", args[0]))?;
            let count_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!("unknown shard_route_u32 shard count type for {}", args[1])
            })?;
            Some(LoweredExecExpr::ShardRouteU32 {
                key: lower_exec_operand(&args[0], env, Some(&key_ty), named_types)?
                    .ok_or_else(|| format!("unsupported shard_route_u32 key {}", args[0]))?,
                shard_count: lower_exec_operand(&args[1], env, Some(&count_ty), named_types)?
                    .ok_or_else(|| {
                        format!("unsupported shard_route_u32 shard count {}", args[1])
                    })?,
            })
        }
        "lease_open" => Some(LoweredExecExpr::LeaseOpen {
            target: args[0].clone(),
        }),
        "lease_acquire" => {
            let owner_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown lease_acquire owner type for {}", args[1]))?;
            Some(LoweredExecExpr::LeaseAcquire {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| format!("unsupported lease_acquire handle {}", args[0]))?,
                owner: lower_exec_operand(&args[1], env, Some(&owner_ty), named_types)?
                    .ok_or_else(|| format!("unsupported lease_acquire owner {}", args[1]))?,
            })
        }
        "lease_owner" => Some(LoweredExecExpr::LeaseOwner {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported lease_owner handle {}", args[0]))?,
        }),
        "lease_transfer" => {
            let owner_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown lease_transfer owner type for {}", args[1]))?;
            Some(LoweredExecExpr::LeaseTransfer {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| format!("unsupported lease_transfer handle {}", args[0]))?,
                owner: lower_exec_operand(&args[1], env, Some(&owner_ty), named_types)?
                    .ok_or_else(|| format!("unsupported lease_transfer owner {}", args[1]))?,
            })
        }
        "lease_release" => {
            let owner_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown lease_release owner type for {}", args[1]))?;
            Some(LoweredExecExpr::LeaseRelease {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| format!("unsupported lease_release handle {}", args[0]))?,
                owner: lower_exec_operand(&args[1], env, Some(&owner_ty), named_types)?
                    .ok_or_else(|| format!("unsupported lease_release owner {}", args[1]))?,
            })
        }
        "lease_close" => Some(LoweredExecExpr::LeaseClose {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported lease_close handle {}", args[0]))?,
        }),
        "placement_open" => Some(LoweredExecExpr::PlacementOpen {
            target: args[0].clone(),
        }),
        "placement_assign" => {
            let shard_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown placement_assign shard type for {}", args[1]))?;
            let node_ty = resolve_operand_type(&args[2], type_env)
                .ok_or_else(|| format!("unknown placement_assign node type for {}", args[2]))?;
            Some(LoweredExecExpr::PlacementAssign {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| format!("unsupported placement_assign handle {}", args[0]))?,
                shard: lower_exec_operand(&args[1], env, Some(&shard_ty), named_types)?
                    .ok_or_else(|| format!("unsupported placement_assign shard {}", args[1]))?,
                node: lower_exec_operand(&args[2], env, Some(&node_ty), named_types)?
                    .ok_or_else(|| format!("unsupported placement_assign node {}", args[2]))?,
            })
        }
        "placement_lookup" => {
            let shard_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown placement_lookup shard type for {}", args[1]))?;
            Some(LoweredExecExpr::PlacementLookup {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| format!("unsupported placement_lookup handle {}", args[0]))?,
                shard: lower_exec_operand(&args[1], env, Some(&shard_ty), named_types)?
                    .ok_or_else(|| format!("unsupported placement_lookup shard {}", args[1]))?,
            })
        }
        "placement_close" => Some(LoweredExecExpr::PlacementClose {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported placement_close handle {}", args[0]))?,
        }),
        "coord_open" => Some(LoweredExecExpr::CoordOpen {
            target: args[0].clone(),
        }),
        "coord_store_u32" => {
            let value_ty = resolve_operand_type(&args[2], type_env)
                .ok_or_else(|| format!("unknown coord_store_u32 value type for {}", args[2]))?;
            Some(LoweredExecExpr::CoordStoreU32 {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| format!("unsupported coord_store_u32 handle {}", args[0]))?,
                key: args[1].clone(),
                value: lower_exec_operand(&args[2], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| format!("unsupported coord_store_u32 value {}", args[2]))?,
            })
        }
        "coord_load_u32" => Some(LoweredExecExpr::CoordLoadU32 {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported coord_load_u32 handle {}", args[0]))?,
            key: args[1].clone(),
        }),
        "coord_close" => Some(LoweredExecExpr::CoordClose {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported coord_close handle {}", args[0]))?,
        }),
        "batch_open" => Some(LoweredExecExpr::BatchOpen),
        "batch_push_u64" => {
            let value_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown batch_push_u64 value type for {}", args[1]))?;
            Some(LoweredExecExpr::BatchPushU64 {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| format!("unsupported batch_push_u64 handle {}", args[0]))?,
                value: lower_exec_operand(&args[1], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| format!("unsupported batch_push_u64 value {}", args[1]))?,
            })
        }
        "batch_len" => Some(LoweredExecExpr::BatchLen {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported batch_len handle {}", args[0]))?,
        }),
        "batch_flush_sum_u64" => Some(LoweredExecExpr::BatchFlushSumU64 {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported batch_flush_sum_u64 handle {}", args[0]))?,
        }),
        "batch_close" => Some(LoweredExecExpr::BatchClose {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported batch_close handle {}", args[0]))?,
        }),
        "agg_open_u64" => Some(LoweredExecExpr::AggOpenU64),
        "agg_add_u64" => {
            let value_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown agg_add_u64 value type for {}", args[1]))?;
            Some(LoweredExecExpr::AggAddU64 {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| format!("unsupported agg_add_u64 handle {}", args[0]))?,
                value: lower_exec_operand(&args[1], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| format!("unsupported agg_add_u64 value {}", args[1]))?,
            })
        }
        "agg_count" => Some(LoweredExecExpr::AggCount {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported agg_count handle {}", args[0]))?,
        }),
        "agg_sum_u64" => Some(LoweredExecExpr::AggSumU64 {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported agg_sum_u64 handle {}", args[0]))?,
        }),
        "agg_avg_u64" => Some(LoweredExecExpr::AggAvgU64 {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported agg_avg_u64 handle {}", args[0]))?,
        }),
        "agg_min_u64" => Some(LoweredExecExpr::AggMinU64 {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported agg_min_u64 handle {}", args[0]))?,
        }),
        "agg_max_u64" => Some(LoweredExecExpr::AggMaxU64 {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported agg_max_u64 handle {}", args[0]))?,
        }),
        "agg_close" => Some(LoweredExecExpr::AggClose {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported agg_close handle {}", args[0]))?,
        }),
        "window_open_ms" => {
            let width_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown window_open_ms width type for {}", args[0]))?;
            Some(LoweredExecExpr::WindowOpenMs {
                width_ms: lower_exec_operand(&args[0], env, Some(&width_ty), named_types)?
                    .ok_or_else(|| format!("unsupported window_open_ms width {}", args[0]))?,
            })
        }
        "window_add_u64" => {
            let value_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown window_add_u64 value type for {}", args[1]))?;
            Some(LoweredExecExpr::WindowAddU64 {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| format!("unsupported window_add_u64 handle {}", args[0]))?,
                value: lower_exec_operand(&args[1], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| format!("unsupported window_add_u64 value {}", args[1]))?,
            })
        }
        "window_count" => Some(LoweredExecExpr::WindowCount {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported window_count handle {}", args[0]))?,
        }),
        "window_sum_u64" => Some(LoweredExecExpr::WindowSumU64 {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported window_sum_u64 handle {}", args[0]))?,
        }),
        "window_avg_u64" => Some(LoweredExecExpr::WindowAvgU64 {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported window_avg_u64 handle {}", args[0]))?,
        }),
        "window_min_u64" => Some(LoweredExecExpr::WindowMinU64 {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported window_min_u64 handle {}", args[0]))?,
        }),
        "window_max_u64" => Some(LoweredExecExpr::WindowMaxU64 {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported window_max_u64 handle {}", args[0]))?,
        }),
        "window_close" => Some(LoweredExecExpr::WindowClose {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported window_close handle {}", args[0]))?,
        }),
        "msg_log_open" => Some(LoweredExecExpr::MsgLogOpen),
        "msg_log_close" => Some(LoweredExecExpr::MsgLogClose {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported msg_log_close handle {}", args[0]))?,
        }),
        "msg_send" => {
            let payload_ty = resolve_operand_type(&args[3], type_env)
                .ok_or_else(|| format!("unknown msg_send payload type for {}", args[3]))?;
            Some(LoweredExecExpr::MsgSend {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| format!("unsupported msg_send handle {}", args[0]))?,
                conversation: args[1].clone(),
                recipient: args[2].clone(),
                payload: lower_exec_operand(&args[3], env, Some(&payload_ty), named_types)?
                    .ok_or_else(|| format!("unsupported msg_send payload {}", args[3]))?,
            })
        }
        "msg_send_dedup" => {
            let key_ty = resolve_operand_type(&args[3], type_env)
                .ok_or_else(|| format!("unknown msg_send_dedup key type for {}", args[3]))?;
            let payload_ty = resolve_operand_type(&args[4], type_env)
                .ok_or_else(|| format!("unknown msg_send_dedup payload type for {}", args[4]))?;
            Some(LoweredExecExpr::MsgSendDedup {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| format!("unsupported msg_send_dedup handle {}", args[0]))?,
                conversation: args[1].clone(),
                recipient: args[2].clone(),
                dedup_key: lower_exec_operand(&args[3], env, Some(&key_ty), named_types)?
                    .ok_or_else(|| format!("unsupported msg_send_dedup key {}", args[3]))?,
                payload: lower_exec_operand(&args[4], env, Some(&payload_ty), named_types)?
                    .ok_or_else(|| format!("unsupported msg_send_dedup payload {}", args[4]))?,
            })
        }
        "msg_subscribe" => Some(LoweredExecExpr::MsgSubscribe {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported msg_subscribe handle {}", args[0]))?,
            room: args[1].clone(),
            recipient: args[2].clone(),
        }),
        "msg_subscriber_count" => Some(LoweredExecExpr::MsgSubscriberCount {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported msg_subscriber_count handle {}", args[0]))?,
            room: args[1].clone(),
        }),
        "msg_fanout" => {
            let payload_ty = resolve_operand_type(&args[2], type_env)
                .ok_or_else(|| format!("unknown msg_fanout payload type for {}", args[2]))?;
            Some(LoweredExecExpr::MsgFanout {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| format!("unsupported msg_fanout handle {}", args[0]))?,
                room: args[1].clone(),
                payload: lower_exec_operand(&args[2], env, Some(&payload_ty), named_types)?
                    .ok_or_else(|| format!("unsupported msg_fanout payload {}", args[2]))?,
            })
        }
        "msg_recv_next" => Some(LoweredExecExpr::MsgRecvNext {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported msg_recv_next handle {}", args[0]))?,
            recipient: args[1].clone(),
        }),
        "msg_recv_seq" => Some(LoweredExecExpr::MsgRecvSeq {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported msg_recv_seq handle {}", args[0]))?,
            recipient: args[1].clone(),
        }),
        "msg_ack" => {
            let seq_ty = resolve_operand_type(&args[2], type_env)
                .ok_or_else(|| format!("unknown msg_ack seq type for {}", args[2]))?;
            Some(LoweredExecExpr::MsgAck {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| format!("unsupported msg_ack handle {}", args[0]))?,
                recipient: args[1].clone(),
                seq: lower_exec_operand(&args[2], env, Some(&seq_ty), named_types)?
                    .ok_or_else(|| format!("unsupported msg_ack seq {}", args[2]))?,
            })
        }
        "msg_mark_retry" => {
            let seq_ty = resolve_operand_type(&args[2], type_env)
                .ok_or_else(|| format!("unknown msg_mark_retry seq type for {}", args[2]))?;
            Some(LoweredExecExpr::MsgMarkRetry {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| format!("unsupported msg_mark_retry handle {}", args[0]))?,
                recipient: args[1].clone(),
                seq: lower_exec_operand(&args[2], env, Some(&seq_ty), named_types)?
                    .ok_or_else(|| format!("unsupported msg_mark_retry seq {}", args[2]))?,
            })
        }
        "msg_retry_count" => {
            let seq_ty = resolve_operand_type(&args[2], type_env)
                .ok_or_else(|| format!("unknown msg_retry_count seq type for {}", args[2]))?;
            Some(LoweredExecExpr::MsgRetryCount {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| format!("unsupported msg_retry_count handle {}", args[0]))?,
                recipient: args[1].clone(),
                seq: lower_exec_operand(&args[2], env, Some(&seq_ty), named_types)?
                    .ok_or_else(|| format!("unsupported msg_retry_count seq {}", args[2]))?,
            })
        }
        "msg_pending_count" => Some(LoweredExecExpr::MsgPendingCount {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported msg_pending_count handle {}", args[0]))?,
            recipient: args[1].clone(),
        }),
        "msg_delivery_total" => Some(LoweredExecExpr::MsgDeliveryTotal {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported msg_delivery_total handle {}", args[0]))?,
            recipient: args[1].clone(),
        }),
        "msg_failure_class" => Some(LoweredExecExpr::MsgFailureClass {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported msg_failure_class handle {}", args[0]))?,
        }),
        "msg_replay_open" => {
            let seq_ty = resolve_operand_type(&args[2], type_env)
                .ok_or_else(|| format!("unknown msg_replay_open seq type for {}", args[2]))?;
            Some(LoweredExecExpr::MsgReplayOpen {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| format!("unsupported msg_replay_open handle {}", args[0]))?,
                recipient: args[1].clone(),
                from_seq: lower_exec_operand(&args[2], env, Some(&seq_ty), named_types)?
                    .ok_or_else(|| format!("unsupported msg_replay_open seq {}", args[2]))?,
            })
        }
        "msg_replay_next" => Some(LoweredExecExpr::MsgReplayNext {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported msg_replay_next handle {}", args[0]))?,
        }),
        "msg_replay_seq" => Some(LoweredExecExpr::MsgReplaySeq {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported msg_replay_seq handle {}", args[0]))?,
        }),
        "msg_replay_close" => Some(LoweredExecExpr::MsgReplayClose {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported msg_replay_close handle {}", args[0]))?,
        }),
        "service_open" => Some(LoweredExecExpr::ServiceOpen {
            name: service_name_for_function(function)?.to_string(),
        }),
        "service_close" => Some(LoweredExecExpr::ServiceClose {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported service_close handle {}", args[0]))?,
        }),
        "service_shutdown" => {
            let grace_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown service_shutdown operand type for {}", args[1]))?;
            Some(LoweredExecExpr::ServiceShutdown {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| format!("unsupported service_shutdown handle {}", args[0]))?,
                grace_ms: lower_exec_operand(&args[1], env, Some(&grace_ty), named_types)?
                    .ok_or_else(|| format!("unsupported service_shutdown grace {}", args[1]))?,
            })
        }
        "service_log" => {
            let msg_ty = resolve_operand_type(&args[2], type_env)
                .ok_or_else(|| format!("unknown service_log operand type for {}", args[2]))?;
            Some(LoweredExecExpr::ServiceLog {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| format!("unsupported service_log handle {}", args[0]))?,
                level: args[1].clone(),
                message: lower_exec_operand(&args[2], env, Some(&msg_ty), named_types)?
                    .ok_or_else(|| format!("unsupported service_log message {}", args[2]))?,
            })
        }
        "service_trace_begin" => Some(LoweredExecExpr::ServiceTraceBegin {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported service_trace_begin handle {}", args[0]))?,
            name: args[1].clone(),
        }),
        "service_trace_end" => Some(LoweredExecExpr::ServiceTraceEnd {
            trace: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported service_trace_end trace {}", args[0]))?,
        }),
        "service_metric_count" => {
            let value_ty = resolve_operand_type(&args[2], type_env).ok_or_else(|| {
                format!("unknown service_metric_count operand type for {}", args[2])
            })?;
            Some(LoweredExecExpr::ServiceMetricCount {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| format!("unsupported service_metric_count handle {}", args[0]))?,
                metric: args[1].clone(),
                value: lower_exec_operand(&args[2], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| format!("unsupported service_metric_count value {}", args[2]))?,
            })
        }
        "service_metric_count_dim" => {
            let value_ty = resolve_operand_type(&args[3], type_env).ok_or_else(|| {
                format!("unknown service_metric_count_dim operand type for {}", args[3])
            })?;
            Some(LoweredExecExpr::ServiceMetricCountDim {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| {
                    format!("unsupported service_metric_count_dim handle {}", args[0])
                })?,
                metric: args[1].clone(),
                dimension: args[2].clone(),
                value: lower_exec_operand(&args[3], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| {
                        format!("unsupported service_metric_count_dim value {}", args[3])
                    })?,
            })
        }
        "service_metric_total" => Some(LoweredExecExpr::ServiceMetricTotal {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported service_metric_total handle {}", args[0]))?,
            metric: args[1].clone(),
        }),
        "service_health_status" => Some(LoweredExecExpr::ServiceHealthStatus {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported service_health_status handle {}", args[0]))?,
        }),
        "service_readiness_status" => Some(LoweredExecExpr::ServiceReadinessStatus {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported service_readiness_status handle {}", args[0]))?,
        }),
        "service_set_health" => {
            let status_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown service_set_health operand type for {}", args[1]))?;
            Some(LoweredExecExpr::ServiceSetHealth {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| format!("unsupported service_set_health handle {}", args[0]))?,
                status: lower_exec_operand(&args[1], env, Some(&status_ty), named_types)?
                    .ok_or_else(|| format!("unsupported service_set_health status {}", args[1]))?,
            })
        }
        "service_set_readiness" => {
            let status_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!("unknown service_set_readiness operand type for {}", args[1])
            })?;
            Some(LoweredExecExpr::ServiceSetReadiness {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| format!("unsupported service_set_readiness handle {}", args[0]))?,
                status: lower_exec_operand(&args[1], env, Some(&status_ty), named_types)?
                    .ok_or_else(|| {
                        format!("unsupported service_set_readiness status {}", args[1])
                    })?,
            })
        }
        "service_set_degraded" => {
            let degraded_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!("unknown service_set_degraded operand type for {}", args[1])
            })?;
            Some(LoweredExecExpr::ServiceSetDegraded {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| format!("unsupported service_set_degraded handle {}", args[0]))?,
                degraded: lower_exec_operand(&args[1], env, Some(&degraded_ty), named_types)?
                    .ok_or_else(|| {
                        format!("unsupported service_set_degraded flag {}", args[1])
                    })?,
            })
        }
        "service_degraded" => Some(LoweredExecExpr::ServiceDegraded {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported service_degraded handle {}", args[0]))?,
        }),
        "service_event" => {
            let msg_ty = resolve_operand_type(&args[2], type_env)
                .ok_or_else(|| format!("unknown service_event operand type for {}", args[2]))?;
            Some(LoweredExecExpr::ServiceEvent {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| format!("unsupported service_event handle {}", args[0]))?,
                class: args[1].clone(),
                message: lower_exec_operand(&args[2], env, Some(&msg_ty), named_types)?
                    .ok_or_else(|| format!("unsupported service_event message {}", args[2]))?,
            })
        }
        "service_event_total" => Some(LoweredExecExpr::ServiceEventTotal {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported service_event_total handle {}", args[0]))?,
            class: args[1].clone(),
        }),
        "service_trace_link" => Some(LoweredExecExpr::ServiceTraceLink {
            trace: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported service_trace_link trace {}", args[0]))?,
            parent: lower_exec_operand(
                &args[1],
                env,
                resolve_operand_type(&args[1], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported service_trace_link parent {}", args[1]))?,
        }),
        "service_trace_link_count" => Some(LoweredExecExpr::ServiceTraceLinkCount {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported service_trace_link_count handle {}", args[0]))?,
        }),
        "service_failure_count" => {
            let value_ty = resolve_operand_type(&args[2], type_env).ok_or_else(|| {
                format!("unknown service_failure_count operand type for {}", args[2])
            })?;
            Some(LoweredExecExpr::ServiceFailureCount {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| format!("unsupported service_failure_count handle {}", args[0]))?,
                class: args[1].clone(),
                value: lower_exec_operand(&args[2], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| {
                        format!("unsupported service_failure_count value {}", args[2])
                    })?,
            })
        }
        "service_failure_total" => Some(LoweredExecExpr::ServiceFailureTotal {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported service_failure_total handle {}", args[0]))?,
            class: args[1].clone(),
        }),
        "service_checkpoint_save_u32" => {
            let value_ty = resolve_operand_type(&args[2], type_env).ok_or_else(|| {
                format!("unknown service_checkpoint_save_u32 operand type for {}", args[2])
            })?;
            Some(LoweredExecExpr::ServiceCheckpointSaveU32 {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| {
                    format!("unsupported service_checkpoint_save_u32 handle {}", args[0])
                })?,
                key: args[1].clone(),
                value: lower_exec_operand(&args[2], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| {
                        format!("unsupported service_checkpoint_save_u32 value {}", args[2])
                    })?,
            })
        }
        "service_checkpoint_load_u32" => Some(LoweredExecExpr::ServiceCheckpointLoadU32 {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| {
                format!("unsupported service_checkpoint_load_u32 handle {}", args[0])
            })?,
            key: args[1].clone(),
        }),
        "service_checkpoint_exists" => Some(LoweredExecExpr::ServiceCheckpointExists {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported service_checkpoint_exists handle {}", args[0]))?,
            key: args[1].clone(),
        }),
        "service_migrate_db" => Some(LoweredExecExpr::ServiceMigrateDb {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported service_migrate_db handle {}", args[0]))?,
            db_handle: lower_exec_operand(
                &args[1],
                env,
                resolve_operand_type(&args[1], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported service_migrate_db db {}", args[1]))?,
            migration: args[2].clone(),
        }),
        "service_route" => Some(LoweredExecExpr::ServiceRoute {
            request: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported service_route request {}", args[0]))?,
            method: args[1].clone(),
            path: args[2].clone(),
        }),
        "service_require_header" => Some(LoweredExecExpr::ServiceRequireHeader {
            request: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported service_require_header request {}", args[0]))?,
            name: args[1].clone(),
            value: args[2].clone(),
        }),
        "service_error_status" => Some(LoweredExecExpr::ServiceErrorStatus {
            kind: args[0].clone(),
        }),
        "tls_exchange_all" => {
            let (host, port) = net_endpoint_for_function(function)?;
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown tls_exchange_all operand type for {}", args[0]))?;
            Some(LoweredExecExpr::TlsExchangeAllU8 {
                host,
                port,
                value: lower_exec_operand(&args[0], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| format!("unsupported tls_exchange_all operand {}", args[0]))?,
            })
        }
        "rt_open" => {
            let workers_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown rt_open operand type for {}", args[0]))?;
            Some(LoweredExecExpr::RtOpen {
                workers: lower_exec_operand(&args[0], env, Some(&workers_ty), named_types)?
                    .ok_or_else(|| format!("unsupported rt_open operand {}", args[0]))?,
            })
        }
        "rt_spawn_u32" => {
            let runtime_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown rt_spawn_u32 runtime type for {}", args[0]))?;
            let arg_ty = resolve_operand_type(&args[2], type_env)
                .ok_or_else(|| format!("unknown rt_spawn_u32 arg type for {}", args[2]))?;
            Some(LoweredExecExpr::RtSpawnU32 {
                runtime: lower_exec_operand(&args[0], env, Some(&runtime_ty), named_types)?
                    .ok_or_else(|| format!("unsupported rt_spawn_u32 runtime {}", args[0]))?,
                function: args[1].clone(),
                arg: lower_exec_operand(&args[2], env, Some(&arg_ty), named_types)?
                    .ok_or_else(|| format!("unsupported rt_spawn_u32 arg {}", args[2]))?,
            })
        }
        "rt_try_spawn_u32" => {
            let runtime_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown rt_try_spawn_u32 runtime type for {}", args[0]))?;
            let arg_ty = resolve_operand_type(&args[2], type_env)
                .ok_or_else(|| format!("unknown rt_try_spawn_u32 arg type for {}", args[2]))?;
            Some(LoweredExecExpr::RtTrySpawnU32 {
                runtime: lower_exec_operand(&args[0], env, Some(&runtime_ty), named_types)?
                    .ok_or_else(|| format!("unsupported rt_try_spawn_u32 runtime {}", args[0]))?,
                function: args[1].clone(),
                arg: lower_exec_operand(&args[2], env, Some(&arg_ty), named_types)?
                    .ok_or_else(|| format!("unsupported rt_try_spawn_u32 arg {}", args[2]))?,
            })
        }
        "rt_spawn_buf" => {
            let runtime_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown rt_spawn_buf runtime type for {}", args[0]))?;
            let arg_ty = resolve_operand_type(&args[2], type_env)
                .ok_or_else(|| format!("unknown rt_spawn_buf arg type for {}", args[2]))?;
            Some(LoweredExecExpr::RtSpawnBufU8 {
                runtime: lower_exec_operand(&args[0], env, Some(&runtime_ty), named_types)?
                    .ok_or_else(|| format!("unsupported rt_spawn_buf runtime {}", args[0]))?,
                function: args[1].clone(),
                arg: lower_exec_operand(&args[2], env, Some(&arg_ty), named_types)?
                    .ok_or_else(|| format!("unsupported rt_spawn_buf arg {}", args[2]))?,
            })
        }
        "rt_try_spawn_buf" => {
            let runtime_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown rt_try_spawn_buf runtime type for {}", args[0]))?;
            let arg_ty = resolve_operand_type(&args[2], type_env)
                .ok_or_else(|| format!("unknown rt_try_spawn_buf arg type for {}", args[2]))?;
            Some(LoweredExecExpr::RtTrySpawnBufU8 {
                runtime: lower_exec_operand(&args[0], env, Some(&runtime_ty), named_types)?
                    .ok_or_else(|| format!("unsupported rt_try_spawn_buf runtime {}", args[0]))?,
                function: args[1].clone(),
                arg: lower_exec_operand(&args[2], env, Some(&arg_ty), named_types)?
                    .ok_or_else(|| format!("unsupported rt_try_spawn_buf arg {}", args[2]))?,
            })
        }
        "rt_done" => Some(LoweredExecExpr::RtDone {
            task: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported rt_done operand {}", args[0]))?,
        }),
        "rt_join_u32" => Some(LoweredExecExpr::RtJoinU32 {
            task: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported rt_join_u32 operand {}", args[0]))?,
        }),
        "rt_join_buf" => Some(LoweredExecExpr::RtJoinBufU8 {
            task: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported rt_join_buf operand {}", args[0]))?,
        }),
        "rt_cancel" => Some(LoweredExecExpr::RtCancel {
            task: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported rt_cancel operand {}", args[0]))?,
        }),
        "rt_task_close" => Some(LoweredExecExpr::RtTaskClose {
            task: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported rt_task_close operand {}", args[0]))?,
        }),
        "rt_shutdown" => {
            let grace_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown rt_shutdown operand type for {}", args[1]))?;
            Some(LoweredExecExpr::RtShutdown {
                runtime: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| format!("unsupported rt_shutdown runtime {}", args[0]))?,
                grace_ms: lower_exec_operand(&args[1], env, Some(&grace_ty), named_types)?
                    .ok_or_else(|| format!("unsupported rt_shutdown grace {}", args[1]))?,
            })
        }
        "rt_close" => Some(LoweredExecExpr::RtClose {
            runtime: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported rt_close runtime {}", args[0]))?,
        }),
        "rt_cancelled" => Some(LoweredExecExpr::RtCancelled),
        "rt_inflight" => Some(LoweredExecExpr::RtInFlight {
            runtime: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported rt_inflight runtime {}", args[0]))?,
        }),
        "chan_open_u32" => {
            let capacity_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown chan_open_u32 operand type for {}", args[0]))?;
            Some(LoweredExecExpr::ChanOpenU32 {
                capacity: lower_exec_operand(&args[0], env, Some(&capacity_ty), named_types)?
                    .ok_or_else(|| format!("unsupported chan_open_u32 operand {}", args[0]))?,
            })
        }
        "chan_open_buf" => {
            let capacity_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown chan_open_buf operand type for {}", args[0]))?;
            Some(LoweredExecExpr::ChanOpenBufU8 {
                capacity: lower_exec_operand(&args[0], env, Some(&capacity_ty), named_types)?
                    .ok_or_else(|| format!("unsupported chan_open_buf operand {}", args[0]))?,
            })
        }
        "chan_send_u32" => {
            let value_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown chan_send_u32 value type for {}", args[1]))?;
            Some(LoweredExecExpr::ChanSendU32 {
                channel: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| format!("unsupported chan_send_u32 channel {}", args[0]))?,
                value: lower_exec_operand(&args[1], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| format!("unsupported chan_send_u32 value {}", args[1]))?,
            })
        }
        "chan_send_buf" => {
            let value_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown chan_send_buf value type for {}", args[1]))?;
            Some(LoweredExecExpr::ChanSendBufU8 {
                channel: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| format!("unsupported chan_send_buf channel {}", args[0]))?,
                value: lower_exec_operand(&args[1], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| format!("unsupported chan_send_buf value {}", args[1]))?,
            })
        }
        "chan_recv_u32" => Some(LoweredExecExpr::ChanRecvU32 {
            channel: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported chan_recv_u32 channel {}", args[0]))?,
        }),
        "chan_recv_buf" => Some(LoweredExecExpr::ChanRecvBufU8 {
            channel: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported chan_recv_buf channel {}", args[0]))?,
        }),
        "chan_len" => Some(LoweredExecExpr::ChanLen {
            channel: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported chan_len channel {}", args[0]))?,
        }),
        "chan_close" => Some(LoweredExecExpr::ChanClose {
            channel: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported chan_close channel {}", args[0]))?,
        }),
        "deadline_open_ms" => {
            let timeout_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown deadline_open_ms operand type for {}", args[0]))?;
            Some(LoweredExecExpr::DeadlineOpenMs {
                timeout_ms: lower_exec_operand(&args[0], env, Some(&timeout_ty), named_types)?
                    .ok_or_else(|| format!("unsupported deadline_open_ms operand {}", args[0]))?,
            })
        }
        "deadline_expired" => Some(LoweredExecExpr::DeadlineExpired {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported deadline_expired handle {}", args[0]))?,
        }),
        "deadline_remaining_ms" => Some(LoweredExecExpr::DeadlineRemainingMs {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported deadline_remaining_ms handle {}", args[0]))?,
        }),
        "deadline_close" => Some(LoweredExecExpr::DeadlineClose {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported deadline_close handle {}", args[0]))?,
        }),
        "cancel_scope_open" => Some(LoweredExecExpr::CancelScopeOpen),
        "cancel_scope_child" => Some(LoweredExecExpr::CancelScopeChild {
            parent: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported cancel_scope_child handle {}", args[0]))?,
        }),
        "cancel_scope_bind_task" => Some(LoweredExecExpr::CancelScopeBindTask {
            scope: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported cancel_scope_bind_task scope {}", args[0]))?,
            task: lower_exec_operand(
                &args[1],
                env,
                resolve_operand_type(&args[1], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported cancel_scope_bind_task task {}", args[1]))?,
        }),
        "cancel_scope_cancel" => Some(LoweredExecExpr::CancelScopeCancel {
            scope: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported cancel_scope_cancel scope {}", args[0]))?,
        }),
        "cancel_scope_cancelled" => Some(LoweredExecExpr::CancelScopeCancelled {
            scope: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported cancel_scope_cancelled scope {}", args[0]))?,
        }),
        "cancel_scope_close" => Some(LoweredExecExpr::CancelScopeClose {
            scope: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported cancel_scope_close scope {}", args[0]))?,
        }),
        "retry_open" => {
            let max_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown retry_open operand type for {}", args[0]))?;
            let backoff_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown retry_open operand type for {}", args[1]))?;
            Some(LoweredExecExpr::RetryOpen {
                max_attempts: lower_exec_operand(&args[0], env, Some(&max_ty), named_types)?
                    .ok_or_else(|| format!("unsupported retry_open max attempts {}", args[0]))?,
                base_backoff_ms: lower_exec_operand(&args[1], env, Some(&backoff_ty), named_types)?
                    .ok_or_else(|| format!("unsupported retry_open backoff {}", args[1]))?,
            })
        }
        "retry_record_failure" => Some(LoweredExecExpr::RetryRecordFailure {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported retry_record_failure handle {}", args[0]))?,
        }),
        "retry_record_success" => Some(LoweredExecExpr::RetryRecordSuccess {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported retry_record_success handle {}", args[0]))?,
        }),
        "retry_next_delay_ms" => Some(LoweredExecExpr::RetryNextDelayMs {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported retry_next_delay_ms handle {}", args[0]))?,
        }),
        "retry_exhausted" => Some(LoweredExecExpr::RetryExhausted {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported retry_exhausted handle {}", args[0]))?,
        }),
        "retry_close" => Some(LoweredExecExpr::RetryClose {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported retry_close handle {}", args[0]))?,
        }),
        "circuit_open" => {
            let threshold_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown circuit_open operand type for {}", args[0]))?;
            let cooldown_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown circuit_open operand type for {}", args[1]))?;
            Some(LoweredExecExpr::CircuitOpen {
                threshold: lower_exec_operand(&args[0], env, Some(&threshold_ty), named_types)?
                    .ok_or_else(|| format!("unsupported circuit_open threshold {}", args[0]))?,
                cooldown_ms: lower_exec_operand(&args[1], env, Some(&cooldown_ty), named_types)?
                    .ok_or_else(|| format!("unsupported circuit_open cooldown {}", args[1]))?,
            })
        }
        "circuit_allow" => Some(LoweredExecExpr::CircuitAllow {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported circuit_allow handle {}", args[0]))?,
        }),
        "circuit_record_failure" => Some(LoweredExecExpr::CircuitRecordFailure {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported circuit_record_failure handle {}", args[0]))?,
        }),
        "circuit_record_success" => Some(LoweredExecExpr::CircuitRecordSuccess {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported circuit_record_success handle {}", args[0]))?,
        }),
        "circuit_state" => Some(LoweredExecExpr::CircuitState {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported circuit_state handle {}", args[0]))?,
        }),
        "circuit_close" => Some(LoweredExecExpr::CircuitClose {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported circuit_close handle {}", args[0]))?,
        }),
        "backpressure_open" => {
            let limit_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown backpressure_open operand type for {}", args[0]))?;
            Some(LoweredExecExpr::BackpressureOpen {
                limit: lower_exec_operand(&args[0], env, Some(&limit_ty), named_types)?
                    .ok_or_else(|| format!("unsupported backpressure_open limit {}", args[0]))?,
            })
        }
        "backpressure_acquire" => Some(LoweredExecExpr::BackpressureAcquire {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported backpressure_acquire handle {}", args[0]))?,
        }),
        "backpressure_release" => Some(LoweredExecExpr::BackpressureRelease {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported backpressure_release handle {}", args[0]))?,
        }),
        "backpressure_saturated" => Some(LoweredExecExpr::BackpressureSaturated {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported backpressure_saturated handle {}", args[0]))?,
        }),
        "backpressure_close" => Some(LoweredExecExpr::BackpressureClose {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported backpressure_close handle {}", args[0]))?,
        }),
        "supervisor_open" => {
            let restart_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown supervisor_open operand type for {}", args[0]))?;
            let degrade_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown supervisor_open operand type for {}", args[1]))?;
            Some(LoweredExecExpr::SupervisorOpen {
                restart_budget: lower_exec_operand(&args[0], env, Some(&restart_ty), named_types)?
                    .ok_or_else(|| format!("unsupported supervisor_open restart budget {}", args[0]))?,
                degrade_after: lower_exec_operand(&args[1], env, Some(&degrade_ty), named_types)?
                    .ok_or_else(|| format!("unsupported supervisor_open degrade_after {}", args[1]))?,
            })
        }
        "supervisor_record_failure" => Some(LoweredExecExpr::SupervisorRecordFailure {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported supervisor_record_failure handle {}", args[0]))?,
            code: lower_exec_operand(
                &args[1],
                env,
                resolve_operand_type(&args[1], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported supervisor_record_failure code {}", args[1]))?,
        }),
        "supervisor_record_recovery" => Some(LoweredExecExpr::SupervisorRecordRecovery {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported supervisor_record_recovery handle {}", args[0]))?,
        }),
        "supervisor_should_restart" => Some(LoweredExecExpr::SupervisorShouldRestart {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported supervisor_should_restart handle {}", args[0]))?,
        }),
        "supervisor_degraded" => Some(LoweredExecExpr::SupervisorDegraded {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported supervisor_degraded handle {}", args[0]))?,
        }),
        "supervisor_close" => Some(LoweredExecExpr::SupervisorClose {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported supervisor_close handle {}", args[0]))?,
        }),
        "task_sleep_ms" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown task_sleep_ms operand type for {}", args[0]))?;
            Some(LoweredExecExpr::TaskSleepMs {
                value: lower_exec_operand(&args[0], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| format!("unsupported task_sleep_ms operand {}", args[0]))?,
            })
        }
        "task_open" => {
            let (command, argv, env_vars) = parse_spawn_invocation(args)?;
            Some(LoweredExecExpr::TaskOpen {
                command,
                argv,
                env: env_vars,
            })
        }
        "task_done" => Some(LoweredExecExpr::TaskDone {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported task_done operand {}", args[0]))?,
        }),
        "task_join" => Some(LoweredExecExpr::TaskJoinStatus {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported task_join operand {}", args[0]))?,
        }),
        "task_stdout_all" => Some(LoweredExecExpr::TaskStdoutAllU8 {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported task_stdout_all operand {}", args[0]))?,
        }),
        "task_stderr_all" => Some(LoweredExecExpr::TaskStderrAllU8 {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported task_stderr_all operand {}", args[0]))?,
        }),
        "task_close" => Some(LoweredExecExpr::TaskClose {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported task_close operand {}", args[0]))?,
        }),
        "spawn_capture_all" => {
            let (command, argv, env_vars) = parse_spawn_invocation(args)?;
            Some(LoweredExecExpr::SpawnCaptureAllU8 {
                command,
                argv,
                env: env_vars,
            })
        }
        "spawn_capture_stderr_all" => {
            let (command, argv, env_vars) = parse_spawn_invocation(args)?;
            Some(LoweredExecExpr::SpawnCaptureStderrAllU8 {
                command,
                argv,
                env: env_vars,
            })
        }
        "spawn_call" => {
            let (command, argv, env_vars) = parse_spawn_invocation(args)?;
            Some(LoweredExecExpr::SpawnCall {
                command,
                argv,
                env: env_vars,
            })
        }
        "spawn_open" => {
            let (command, argv, env_vars) = parse_spawn_invocation(args)?;
            Some(LoweredExecExpr::SpawnOpen {
                command,
                argv,
                env: env_vars,
            })
        }
        "spawn_wait" => Some(LoweredExecExpr::SpawnWait {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported spawn_wait operand {}", args[0]))?,
        }),
        "spawn_stdout_all" => Some(LoweredExecExpr::SpawnStdoutAllU8 {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported spawn_stdout_all operand {}", args[0]))?,
        }),
        "spawn_stderr_all" => Some(LoweredExecExpr::SpawnStderrAllU8 {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported spawn_stderr_all operand {}", args[0]))?,
        }),
        "spawn_stdin_write_all" => {
            let value_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!("unknown spawn_stdin_write_all operand type for {}", args[1])
            })?;
            Some(LoweredExecExpr::SpawnStdinWriteAllU8 {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| format!("unsupported spawn_stdin_write_all handle {}", args[0]))?,
                value: lower_exec_operand(&args[1], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| {
                        format!("unsupported spawn_stdin_write_all operand {}", args[1])
                    })?,
            })
        }
        "spawn_stdin_close" => Some(LoweredExecExpr::SpawnStdinClose {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported spawn_stdin_close operand {}", args[0]))?,
        }),
        "spawn_done" => Some(LoweredExecExpr::SpawnDone {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported spawn_done operand {}", args[0]))?,
        }),
        "spawn_exit_ok" => Some(LoweredExecExpr::SpawnExitOk {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported spawn_exit_ok operand {}", args[0]))?,
        }),
        "spawn_kill" => Some(LoweredExecExpr::SpawnKill {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported spawn_kill operand {}", args[0]))?,
        }),
        "spawn_close" => Some(LoweredExecExpr::SpawnClose {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported spawn_close operand {}", args[0]))?,
        }),
        "net_connect" if args.is_empty() => {
            let (host, port) = net_endpoint_for_function(function)?;
            Some(LoweredExecExpr::NetConnect { host, port })
        }
        "ffi_call" => {
            let symbol = args
                .first()
                .ok_or_else(|| "ffi_call requires a symbol".to_string())?;
            if !is_valid_ffi_symbol(symbol) {
                return Err(format!("invalid ffi symbol {symbol}"));
            }
            let mut lowered_args = Vec::new();
            for operand in args.iter().skip(1) {
                let operand_ty = resolve_operand_type(operand, type_env)
                    .ok_or_else(|| format!("unknown ffi operand type for {operand}"))?;
                lowered_args.push(
                    lower_exec_operand(operand, env, Some(&operand_ty), named_types)?
                        .ok_or_else(|| format!("unsupported ffi operand {operand}"))?,
                );
            }
            Some(LoweredExecExpr::FfiCall {
                symbol: symbol.clone(),
                args: lowered_args,
                ret_c_type: instruction.ty.c_type()?,
            })
        }
        "ffi_call_cstr" => {
            let symbol = args
                .first()
                .ok_or_else(|| "ffi_call_cstr requires a symbol".to_string())?;
            if !is_valid_ffi_symbol(symbol) {
                return Err(format!("invalid ffi symbol {symbol}"));
            }
            let buf = args
                .get(1)
                .ok_or_else(|| "ffi_call_cstr requires one buf[u8] operand".to_string())?;
            let buf_ty = resolve_operand_type(buf, type_env)
                .ok_or_else(|| format!("unknown ffi_call_cstr operand type for {buf}"))?;
            if !matches!(
                &buf_ty,
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
                return Err(format!(
                    "ffi_call_cstr requires buf[u8] operand, got {buf_ty}"
                ));
            }
            Some(LoweredExecExpr::FfiCallCStr {
                symbol: symbol.clone(),
                arg: lower_exec_binding(buf, env)?,
                ret_c_type: instruction.ty.c_type()?,
            })
        }
        "ffi_open_lib" => Some(LoweredExecExpr::FfiOpenLib {
            path: args[0].clone(),
        }),
        "ffi_close_lib" => Some(LoweredExecExpr::FfiCloseLib {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported ffi_close_lib operand {}", args[0]))?,
        }),
        "ffi_buf_ptr" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown ffi_buf_ptr operand type for {}", args[0]))?;
            Some(LoweredExecExpr::FfiBufPtr {
                value: lower_exec_operand(&args[0], env, Some(&value_ty), named_types)?
                    .ok_or_else(|| format!("unsupported ffi_buf_ptr operand {}", args[0]))?,
            })
        }
        "ffi_call_lib" => {
            let symbol = args
                .get(1)
                .ok_or_else(|| "ffi_call_lib requires a library handle and symbol".to_string())?;
            let mut lowered_args = Vec::new();
            for operand in args.iter().skip(2) {
                let operand_ty = resolve_operand_type(operand, type_env)
                    .ok_or_else(|| format!("unknown ffi_call_lib operand type for {operand}"))?;
                lowered_args.push(
                    lower_exec_operand(operand, env, Some(&operand_ty), named_types)?
                        .ok_or_else(|| format!("unsupported ffi_call_lib operand {operand}"))?,
                );
            }
            Some(LoweredExecExpr::FfiCallLib {
                handle: lower_exec_operand(
                    &args[0],
                    env,
                    resolve_operand_type(&args[0], type_env).as_ref(),
                    named_types,
                )?
                .ok_or_else(|| format!("unsupported ffi_call_lib handle {}", args[0]))?,
                symbol: symbol.clone(),
                args: lowered_args,
                ret_c_type: instruction.ty.c_type()?,
            })
        }
        "ffi_call_lib_cstr" => Some(LoweredExecExpr::FfiCallLibCStr {
            handle: lower_exec_operand(
                &args[0],
                env,
                resolve_operand_type(&args[0], type_env).as_ref(),
                named_types,
            )?
            .ok_or_else(|| format!("unsupported ffi_call_lib_cstr handle {}", args[0]))?,
            symbol: args[1].clone(),
            arg: lower_exec_binding(&args[2], env)?,
            ret_c_type: instruction.ty.c_type()?,
        }),
        "len" => {
            let source = lower_exec_binding(&args[0], env)?;
            Some(LoweredExecExpr::Len { source })
        }
        "store" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown store handle type for {}", args[0]))?;
            if matches!(
                handle_ty,
                TypeRef::Edit(inner)
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
                Some(LoweredExecExpr::StoreBufU8 {
                    source: lower_exec_binding(&args[0], env)?,
                    index: lower_exec_operand(
                        &args[1],
                        env,
                        Some(&TypeRef::Int {
                            signed: false,
                            bits: 32,
                        }),
                        named_types,
                    )?
                    .ok_or_else(|| format!("unsupported store index {}", args[1]))?,
                    value: lower_exec_operand(
                        &args[2],
                        env,
                        Some(&TypeRef::Int {
                            signed: false,
                            bits: 8,
                        }),
                        named_types,
                    )?
                    .ok_or_else(|| format!("unsupported store value {}", args[2]))?,
                })
            } else {
                None
            }
        }
        "load" => {
            let collection_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown collection type for {}", args[0]))?;
            match collection_ty {
                TypeRef::View(inner)
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
                    Some(LoweredExecExpr::LoadU8 {
                        source: lower_exec_binding(&args[0], env)?,
                        index: lower_exec_operand(
                            &args[1],
                            env,
                            Some(&TypeRef::Int {
                                signed: false,
                                bits: 32,
                            }),
                            named_types,
                        )?
                        .ok_or_else(|| format!("unsupported load index {}", args[1]))?,
                    })
                }
                TypeRef::Span(inner)
                    if *inner
                        == TypeRef::Int {
                            signed: true,
                            bits: 32,
                        } =>
                {
                    Some(LoweredExecExpr::LoadI32 {
                        source: lower_exec_binding(&args[0], env)?,
                        index: lower_exec_operand(
                            &args[1],
                            env,
                            Some(&TypeRef::Int {
                                signed: false,
                                bits: 32,
                            }),
                            named_types,
                        )?
                        .ok_or_else(|| format!("unsupported load index {}", args[1]))?,
                    })
                }
                _ => None,
            }
        }
        "abs"
            if instruction.ty
                == TypeRef::Int {
                    signed: true,
                    bits: 32,
                } =>
        {
            let value = lower_exec_operand(&args[0], env, Some(&instruction.ty), named_types)?;
            value.map(|value| LoweredExecExpr::AbsI32 { value })
        }
        "add" | "sub" | "mul" | "band" | "bor" | "bxor" | "shl" | "shr" | "eq" | "lt" | "le" => {
            let left_expected = resolve_operand_type(&args[0], type_env);
            let right_expected = resolve_operand_type(&args[1], type_env);
            let left = lower_exec_operand(&args[0], env, left_expected.as_ref(), named_types)?;
            let right = lower_exec_operand(&args[1], env, right_expected.as_ref(), named_types)?;
            match (left, right) {
                (Some(left), Some(right)) => Some(LoweredExecExpr::Binary {
                    op: match instruction.op.as_str() {
                        "add" => LoweredExecBinaryOp::Add,
                        "sub" => LoweredExecBinaryOp::Sub,
                        "mul" => LoweredExecBinaryOp::Mul,
                        "band" => LoweredExecBinaryOp::Band,
                        "bor" => LoweredExecBinaryOp::Bor,
                        "bxor" => LoweredExecBinaryOp::Bxor,
                        "shl" => LoweredExecBinaryOp::Shl,
                        "shr" => LoweredExecBinaryOp::Shr,
                        "eq" => LoweredExecBinaryOp::Eq,
                        "lt" => LoweredExecBinaryOp::Lt,
                        "le" => LoweredExecBinaryOp::Le,
                        other => return Err(format!("unsupported binary op {other}")),
                    },
                    left,
                    right,
                }),
                _ => None,
            }
        }
        "bnot" => {
            let value = lower_exec_operand(&args[0], env, Some(&instruction.ty), named_types)?;
            let mask = match &instruction.ty {
                TypeRef::Int {
                    signed: true,
                    bits: 32,
                } => LoweredExecImmediate::I32(-1),
                TypeRef::Int {
                    signed: true,
                    bits: 64,
                } => LoweredExecImmediate::I64(-1),
                TypeRef::Int {
                    signed: false,
                    bits: 32,
                } => LoweredExecImmediate::U32(u32::MAX),
                TypeRef::Bool => LoweredExecImmediate::Bool(true),
                other => return Err(format!("unsupported bnot type for lowered exec {other}")),
            };
            value.map(|value| LoweredExecExpr::Binary {
                op: LoweredExecBinaryOp::Bxor,
                left: value,
                right: LoweredExecOperand::Immediate(mask),
            })
        }
        "sext" if args.first().is_some_and(|target| target == "i64") => {
            let value = lower_exec_operand(
                &args[1],
                env,
                Some(&TypeRef::Int {
                    signed: true,
                    bits: 32,
                }),
                named_types,
            )?;
            value.map(|value| LoweredExecExpr::SextI64 { value })
        }
        "view" | "edit" => {
            let operand = lower_exec_operand(&args[0], env, Some(&instruction.ty), named_types)?;
            operand.map(LoweredExecExpr::Move)
        }
        _ => None,
    };
    Ok(expr)
}

fn lower_exec_binding(token: &str, env: &HashMap<String, String>) -> Result<String, String> {
    env.get(token)
        .cloned()
        .ok_or_else(|| format!("unsupported lowered execution binding {token}"))
}

fn lower_exec_operand(
    token: &str,
    env: &HashMap<String, String>,
    expected: Option<&TypeRef>,
    named_types: &HashMap<String, TypeDeclBody>,
) -> Result<Option<LoweredExecOperand>, String> {
    if let Some(mapped) = env.get(token) {
        return Ok(Some(LoweredExecOperand::Binding(mapped.clone())));
    }
    let Some(expected) = expected else {
        return Ok(None);
    };
    let Ok(value) = parse_data_literal(token, expected, Some(named_types)) else {
        return Ok(None);
    };
    let immediate = match (expected, value) {
        (
            TypeRef::Int {
                signed: false,
                bits: 8,
            },
            DataValue::Int(value),
        ) => LoweredExecImmediate::U8(value as u8),
        (
            TypeRef::Int {
                signed: true,
                bits: 32,
            },
            DataValue::Int(value),
        ) => LoweredExecImmediate::I32(value as i32),
        (
            TypeRef::Int {
                signed: true,
                bits: 64,
            },
            DataValue::Int(value),
        ) => LoweredExecImmediate::I64(value as i64),
        (
            TypeRef::Int {
                signed: false,
                bits: 64,
            },
            DataValue::Int(value),
        ) => LoweredExecImmediate::U64(value as u64),
        (
            TypeRef::Int {
                signed: false,
                bits: 32,
            },
            DataValue::Int(value),
        ) => LoweredExecImmediate::U32(value as u32),
        (TypeRef::Bool, DataValue::Bool(value)) => LoweredExecImmediate::Bool(value),
        _ => return Ok(None),
    };
    Ok(Some(LoweredExecOperand::Immediate(immediate)))
}

fn render_instruction_expr(
    function: &Function,
    _block: &Block,
    instruction: &crate::ast::Instruction,
    env: &HashMap<String, String>,
    type_env: &HashMap<String, TypeRef>,
    named_types: &HashMap<String, TypeDeclBody>,
) -> Result<String, String> {
    let args = &instruction.args;
    match instruction.op.as_str() {
        "const" => render_c_literal_with_named_types(&args[0], Some(&instruction.ty), named_types),
        "alloc" => render_alloc_expr(instruction, env, type_env, named_types),
        "drop" => render_drop_expr(instruction, env, type_env, named_types),
        "clock_now_ns" => Ok("mira_clock_now_ns()".to_string()),
        "rand_u32" => {
            if !function_uses_op(function, "rand_u32") {
                return Err("rand_u32 used without function rand state".to_string());
            }
            Ok("mira_rand_next_u32(&mira_rand_state)".to_string())
        }
        "fs_read_u32" => Ok(format!(
            "mira_fs_read_u32({})",
            render_c_string_literal(&fs_path_for_function(function)?)
        )),
        "fs_write_u32" => {
            let value = render_operand(
                &args[0],
                env,
                Some(&TypeRef::Int {
                    signed: false,
                    bits: 32,
                }),
                named_types,
            )?;
            Ok(format!(
                "mira_fs_write_u32({}, {})",
                render_c_string_literal(&fs_path_for_function(function)?),
                value
            ))
        }
        "fs_read_all" => Ok(format!(
            "mira_fs_read_all_buf_u8({})",
            render_c_string_literal(&fs_path_for_function(function)?)
        )),
        "fs_write_all" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown fs_write_all operand type for {}", args[0]))?;
            let value = render_operand(&args[0], env, Some(&value_ty), named_types)?;
            Ok(format!(
                "mira_fs_write_all_buf_u8({}, {})",
                render_c_string_literal(&fs_path_for_function(function)?),
                value
            ))
        }
        "net_write_all" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown net_write_all operand type for {}", args[0]))?;
            let value = render_operand(&args[0], env, Some(&value_ty), named_types)?;
            let (host, port) = net_endpoint_for_function(function)?;
            Ok(format!(
                "mira_net_write_all_buf_u8({}, {}, {})",
                render_c_string_literal(&host),
                port,
                value
            ))
        }
        "net_exchange_all" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown net_exchange_all operand type for {}", args[0]))?;
            let value = render_operand(&args[0], env, Some(&value_ty), named_types)?;
            let (host, port) = net_endpoint_for_function(function)?;
            Ok(format!(
                "mira_net_exchange_all_buf_u8({}, {}, {})",
                render_c_string_literal(&host),
                port,
                value
            ))
        }
        "net_serve_exchange_all" => {
            let response_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!(
                    "unknown net_serve_exchange_all operand type for {}",
                    args[0]
                )
            })?;
            let response = render_operand(&args[0], env, Some(&response_ty), named_types)?;
            let (host, port) = net_endpoint_for_function(function)?;
            Ok(format!(
                "mira_net_serve_exchange_all_buf_u8({}, {}, {})",
                render_c_string_literal(&host),
                port,
                response
            ))
        }
        "net_listen" => {
            let (host, port) = net_endpoint_for_function(function)?;
            Ok(format!(
                "mira_net_listen_handle({}, {})",
                render_c_string_literal(&host),
                port
            ))
        }
        "tls_listen" => {
            let (host, port) = net_endpoint_for_function(function)?;
            let tls = tls_capability_for_function(function)?;
            Ok(format!(
                "mira_tls_listen_handle({}, {}, {}, {}, {}, {}, {})",
                render_c_string_literal(&host),
                port,
                render_c_string_literal(&tls.cert),
                render_c_string_literal(&tls.key),
                tls.request_timeout_ms,
                tls.session_timeout_ms,
                tls.shutdown_grace_ms
            ))
        }
        "net_accept" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown net_accept operand type for {}", args[0]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_net_accept_handle({handle})"))
        }
        "net_session_open" => {
            let (host, port) = net_endpoint_for_function(function)?;
            Ok(format!(
                "mira_net_session_open_handle({}, {})",
                render_c_string_literal(&host),
                port
            ))
        }
        "net_read_all" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown net_read_all operand type for {}", args[0]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_net_read_all_handle_buf_u8({handle})"))
        }
        "session_read_chunk" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown session_read_chunk operand type for {}", args[0]))?;
            let value_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown session_read_chunk operand type for {}", args[1]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let value = render_operand(&args[1], env, Some(&value_ty), named_types)?;
            Ok(format!("mira_session_read_chunk_buf_u8({handle}, {value})"))
        }
        "net_write_handle_all" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown net_write_handle_all operand type for {}", args[0])
            })?;
            let value_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!("unknown net_write_handle_all operand type for {}", args[1])
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let value = render_operand(&args[1], env, Some(&value_ty), named_types)?;
            Ok(format!(
                "mira_net_write_handle_all_buf_u8({handle}, {value})"
            ))
        }
        "session_write_chunk" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown session_write_chunk operand type for {}", args[0])
            })?;
            let value_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!("unknown session_write_chunk operand type for {}", args[1])
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let value = render_operand(&args[1], env, Some(&value_ty), named_types)?;
            Ok(format!("mira_session_write_chunk_buf_u8({handle}, {value})"))
        }
        "session_flush" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown session_flush operand type for {}", args[0]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_session_flush_handle({handle})"))
        }
        "session_alive" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown session_alive operand type for {}", args[0]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_session_alive_handle({handle})"))
        }
        "session_heartbeat" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown session_heartbeat operand type for {}", args[0])
            })?;
            let value_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!("unknown session_heartbeat operand type for {}", args[1])
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let value = render_operand(&args[1], env, Some(&value_ty), named_types)?;
            Ok(format!("mira_session_heartbeat_buf_u8({handle}, {value})"))
        }
        "session_backpressure" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown session_backpressure operand type for {}", args[0])
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_session_backpressure_u32({handle})"))
        }
        "session_backpressure_wait" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown session_backpressure_wait operand type for {}", args[0])
            })?;
            let value_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!("unknown session_backpressure_wait operand type for {}", args[1])
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let value = render_operand(&args[1], env, Some(&value_ty), named_types)?;
            Ok(format!("mira_session_backpressure_wait({handle}, {value})"))
        }
        "session_resume_id" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown session_resume_id operand type for {}", args[0])
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_session_resume_id_u64({handle})"))
        }
        "session_reconnect" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown session_reconnect operand type for {}", args[0])
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_session_reconnect_handle({handle})"))
        }
        "net_close" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown net_close operand type for {}", args[0]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_net_close_handle({handle})"))
        }
        "listener_set_timeout_ms" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!(
                    "unknown listener_set_timeout_ms operand type for {}",
                    args[0]
                )
            })?;
            let value_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!(
                    "unknown listener_set_timeout_ms operand type for {}",
                    args[1]
                )
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let value = render_operand(&args[1], env, Some(&value_ty), named_types)?;
            Ok(format!("mira_listener_set_timeout_ms({handle}, {value})"))
        }
        "session_set_timeout_ms" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!(
                    "unknown session_set_timeout_ms operand type for {}",
                    args[0]
                )
            })?;
            let value_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!(
                    "unknown session_set_timeout_ms operand type for {}",
                    args[1]
                )
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let value = render_operand(&args[1], env, Some(&value_ty), named_types)?;
            Ok(format!("mira_session_set_timeout_ms({handle}, {value})"))
        }
        "listener_set_shutdown_grace_ms" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!(
                    "unknown listener_set_shutdown_grace_ms operand type for {}",
                    args[0]
                )
            })?;
            let value_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!(
                    "unknown listener_set_shutdown_grace_ms operand type for {}",
                    args[1]
                )
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let value = render_operand(&args[1], env, Some(&value_ty), named_types)?;
            Ok(format!(
                "mira_listener_set_shutdown_grace_ms({handle}, {value})"
            ))
        }
        "buf_lit" => Ok(format!(
            "mira_buf_lit_u8({})",
            render_c_string_literal(&args[0])
        )),
        "str_lit" => Ok(format!(
            "mira_buf_lit_u8({})",
            render_c_string_literal(&args[0])
        )),
        "buf_concat" => {
            let left_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown buf_concat operand type for {}", args[0]))?;
            let right_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown buf_concat operand type for {}", args[1]))?;
            let left = render_operand(&args[0], env, Some(&left_ty), named_types)?;
            let right = render_operand(&args[1], env, Some(&right_ty), named_types)?;
            Ok(format!("mira_buf_concat_u8({left}, {right})"))
        }
        "str_concat" => {
            let left_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown str_concat operand type for {}", args[0]))?;
            let right_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown str_concat operand type for {}", args[1]))?;
            let left = render_operand(&args[0], env, Some(&left_ty), named_types)?;
            let right = render_operand(&args[1], env, Some(&right_ty), named_types)?;
            Ok(format!("mira_buf_concat_u8({left}, {right})"))
        }
        "http_method_eq" => {
            let request_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown http_method_eq operand type for {}", args[0]))?;
            let request = render_operand(&args[0], env, Some(&request_ty), named_types)?;
            Ok(format!(
                "mira_http_method_eq_buf_u8({}, {})",
                request,
                render_c_string_literal(&args[1])
            ))
        }
        "http_path_eq" => {
            let request_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown http_path_eq operand type for {}", args[0]))?;
            let request = render_operand(&args[0], env, Some(&request_ty), named_types)?;
            Ok(format!(
                "mira_http_path_eq_buf_u8({}, {})",
                request,
                render_c_string_literal(&args[1])
            ))
        }
        "http_request_method" => {
            let request_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown http_request_method operand type for {}", args[0])
            })?;
            let request = render_operand(&args[0], env, Some(&request_ty), named_types)?;
            Ok(format!("mira_http_request_method_buf_u8({request})"))
        }
        "http_request_path" => {
            let request_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown http_request_path operand type for {}", args[0]))?;
            let request = render_operand(&args[0], env, Some(&request_ty), named_types)?;
            Ok(format!("mira_http_request_path_buf_u8({request})"))
        }
        "http_route_param" => {
            let request_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown http_route_param operand type for {}", args[0]))?;
            let request = render_operand(&args[0], env, Some(&request_ty), named_types)?;
            Ok(format!(
                "mira_http_route_param_buf_u8({}, {}, {})",
                request,
                render_c_string_literal(&args[1]),
                render_c_string_literal(&args[2])
            ))
        }
        "http_header_eq" => {
            let request_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown http_header_eq operand type for {}", args[0]))?;
            let request = render_operand(&args[0], env, Some(&request_ty), named_types)?;
            Ok(format!(
                "mira_http_header_eq_buf_u8({}, {}, {})",
                request,
                render_c_string_literal(&args[1]),
                render_c_string_literal(&args[2])
            ))
        }
        "http_cookie_eq" => {
            let request_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown http_cookie_eq operand type for {}", args[0]))?;
            let request = render_operand(&args[0], env, Some(&request_ty), named_types)?;
            Ok(format!(
                "mira_http_cookie_eq_buf_u8({}, {}, {})",
                request,
                render_c_string_literal(&args[1]),
                render_c_string_literal(&args[2])
            ))
        }
        "http_status_u32" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown http_status_u32 operand type for {}", args[0]))?;
            let value = render_operand(&args[0], env, Some(&value_ty), named_types)?;
            Ok(format!("mira_http_status_u32_buf_u8({value})"))
        }
        "buf_eq_lit" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown buf_eq_lit operand type for {}", args[0]))?;
            let value = render_operand(&args[0], env, Some(&value_ty), named_types)?;
            Ok(format!(
                "mira_buf_eq_lit_u8({}, {})",
                value,
                render_c_string_literal(&args[1])
            ))
        }
        "buf_contains_lit" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown buf_contains_lit operand type for {}", args[0]))?;
            let value = render_operand(&args[0], env, Some(&value_ty), named_types)?;
            Ok(format!(
                "mira_buf_contains_lit_u8({}, {})",
                value,
                render_c_string_literal(&args[1])
            ))
        }
        "http_header" => {
            let request_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown http_header operand type for {}", args[0]))?;
            let request = render_operand(&args[0], env, Some(&request_ty), named_types)?;
            Ok(format!(
                "mira_http_header_buf_u8({}, {})",
                request,
                render_c_string_literal(&args[1])
            ))
        }
        "http_header_count" => {
            let request_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown http_header_count operand type for {}", args[0])
            })?;
            let request = render_operand(&args[0], env, Some(&request_ty), named_types)?;
            Ok(format!("mira_http_header_count_buf_u8({request})"))
        }
        "http_header_name" => {
            let request_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown http_header_name operand type for {}", args[0]))?;
            let index_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown http_header_name operand type for {}", args[1]))?;
            let request = render_operand(&args[0], env, Some(&request_ty), named_types)?;
            let index = render_operand(&args[1], env, Some(&index_ty), named_types)?;
            Ok(format!("mira_http_header_name_buf_u8({request}, {index})"))
        }
        "http_header_value" => {
            let request_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown http_header_value operand type for {}", args[0])
            })?;
            let index_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!("unknown http_header_value operand type for {}", args[1])
            })?;
            let request = render_operand(&args[0], env, Some(&request_ty), named_types)?;
            let index = render_operand(&args[1], env, Some(&index_ty), named_types)?;
            Ok(format!("mira_http_header_value_buf_u8({request}, {index})"))
        }
        "http_cookie" => {
            let request_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown http_cookie operand type for {}", args[0]))?;
            let request = render_operand(&args[0], env, Some(&request_ty), named_types)?;
            Ok(format!(
                "mira_http_cookie_buf_u8({}, {})",
                request,
                render_c_string_literal(&args[1])
            ))
        }
        "http_query_param" => {
            let request_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown http_query_param operand type for {}", args[0]))?;
            let request = render_operand(&args[0], env, Some(&request_ty), named_types)?;
            Ok(format!(
                "mira_http_query_param_buf_u8({}, {})",
                request,
                render_c_string_literal(&args[1])
            ))
        }
        "http_body" => {
            let request_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown http_body operand type for {}", args[0]))?;
            let request = render_operand(&args[0], env, Some(&request_ty), named_types)?;
            Ok(format!("mira_http_body_buf_u8({request})"))
        }
        "http_multipart_part_count" => {
            let request_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown http_multipart_part_count operand type for {}", args[0])
            })?;
            let request = render_operand(&args[0], env, Some(&request_ty), named_types)?;
            Ok(format!("mira_http_multipart_part_count_buf_u8({request})"))
        }
        "http_multipart_part_name" => {
            let request_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown http_multipart_part_name operand type for {}", args[0])
            })?;
            let index_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!("unknown http_multipart_part_name operand type for {}", args[1])
            })?;
            let request = render_operand(&args[0], env, Some(&request_ty), named_types)?;
            let index = render_operand(&args[1], env, Some(&index_ty), named_types)?;
            Ok(format!("mira_http_multipart_part_name_buf_u8({request}, {index})"))
        }
        "http_multipart_part_filename" => {
            let request_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!(
                    "unknown http_multipart_part_filename operand type for {}",
                    args[0]
                )
            })?;
            let index_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!(
                    "unknown http_multipart_part_filename operand type for {}",
                    args[1]
                )
            })?;
            let request = render_operand(&args[0], env, Some(&request_ty), named_types)?;
            let index = render_operand(&args[1], env, Some(&index_ty), named_types)?;
            Ok(format!(
                "mira_http_multipart_part_filename_buf_u8({request}, {index})"
            ))
        }
        "http_multipart_part_body" => {
            let request_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown http_multipart_part_body operand type for {}", args[0])
            })?;
            let index_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!("unknown http_multipart_part_body operand type for {}", args[1])
            })?;
            let request = render_operand(&args[0], env, Some(&request_ty), named_types)?;
            let index = render_operand(&args[1], env, Some(&index_ty), named_types)?;
            Ok(format!("mira_http_multipart_part_body_buf_u8({request}, {index})"))
        }
        "http_body_limit" => {
            let request_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown http_body_limit operand type for {}", args[0]))?;
            let limit_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown http_body_limit operand type for {}", args[1]))?;
            let request = render_operand(&args[0], env, Some(&request_ty), named_types)?;
            let limit = render_operand(&args[1], env, Some(&limit_ty), named_types)?;
            Ok(format!("mira_http_body_limit_buf_u8({request}, {limit})"))
        }
        "http_body_stream_open" => {
            let request_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown http_body_stream_open operand type for {}", args[0])
            })?;
            let request = render_operand(&args[0], env, Some(&request_ty), named_types)?;
            Ok(format!("mira_http_body_stream_open_buf_u8({request})"))
        }
        "http_body_stream_next" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown http_body_stream_next operand type for {}", args[0])
            })?;
            let chunk_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!("unknown http_body_stream_next operand type for {}", args[1])
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let chunk = render_operand(&args[1], env, Some(&chunk_ty), named_types)?;
            Ok(format!("mira_http_body_stream_next_buf_u8({handle}, {chunk})"))
        }
        "http_body_stream_close" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown http_body_stream_close operand type for {}", args[0])
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_http_body_stream_close_handle({handle})"))
        }
        "http_server_config_u32" => Ok(format!(
            "mira_http_server_config_u32({})",
            render_c_string_literal(&args[0])
        )),
        "tls_server_config_u32" => {
            let tls = tls_capability_for_function(function)?;
            let value = match args[0].as_str() {
                "request_timeout_ms" => tls.request_timeout_ms,
                "session_timeout_ms" => tls.session_timeout_ms,
                "shutdown_grace_ms" => tls.shutdown_grace_ms,
                other => return Err(format!("unsupported tls_server_config_u32 token {other}")),
            };
            Ok(format!("{value}u"))
        }
        "tls_server_config_buf" => {
            let tls = tls_capability_for_function(function)?;
            let value = match args[0].as_str() {
                "cert" => tls.cert,
                "key" => tls.key,
                other => return Err(format!("unsupported tls_server_config_buf token {other}")),
            };
            Ok(format!(
                "mira_buf_lit_u8({})",
                render_c_string_literal(&value)
            ))
        }
        "buf_parse_u32" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown buf_parse_u32 operand type for {}", args[0]))?;
            let value = render_operand(&args[0], env, Some(&value_ty), named_types)?;
            Ok(format!("mira_buf_parse_u32_u8({value})"))
        }
        "buf_parse_bool" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown buf_parse_bool operand type for {}", args[0]))?;
            let value = render_operand(&args[0], env, Some(&value_ty), named_types)?;
            Ok(format!("mira_buf_parse_bool_u8({value})"))
        }
        "str_from_u32" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown str_from_u32 operand type for {}", args[0]))?;
            let value = render_operand(&args[0], env, Some(&value_ty), named_types)?;
            Ok(format!("mira_str_from_u32({value})"))
        }
        "str_from_bool" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown str_from_bool operand type for {}", args[0]))?;
            let value = render_operand(&args[0], env, Some(&value_ty), named_types)?;
            Ok(format!("mira_str_from_bool({value})"))
        }
        "str_eq_lit" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown str_eq_lit operand type for {}", args[0]))?;
            let value = render_operand(&args[0], env, Some(&value_ty), named_types)?;
            Ok(format!(
                "mira_buf_eq_lit_u8({}, {})",
                value,
                render_c_string_literal(&args[1])
            ))
        }
        "str_to_buf" | "buf_to_str" => {
            let value_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown {} operand type for {}", instruction.op, args[0])
            })?;
            render_operand(&args[0], env, Some(&value_ty), named_types)
        }
        "buf_hex_str" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown buf_hex_str operand type for {}", args[0]))?;
            let value = render_operand(&args[0], env, Some(&value_ty), named_types)?;
            Ok(format!("mira_buf_hex_str_u8({value})"))
        }
        "http_write_response" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown http_write_response operand type for {}", args[0])
            })?;
            let status_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!("unknown http_write_response operand type for {}", args[1])
            })?;
            let body_ty = resolve_operand_type(&args[2], type_env).ok_or_else(|| {
                format!("unknown http_write_response operand type for {}", args[2])
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let status = render_operand(&args[1], env, Some(&status_ty), named_types)?;
            let body = render_operand(&args[2], env, Some(&body_ty), named_types)?;
            Ok(format!(
                "mira_http_write_response_handle({handle}, {status}, {body})"
            ))
        }
        "http_write_text_response" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!(
                    "unknown http_write_text_response operand type for {}",
                    args[0]
                )
            })?;
            let status_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!(
                    "unknown http_write_text_response operand type for {}",
                    args[1]
                )
            })?;
            let body_ty = resolve_operand_type(&args[2], type_env).ok_or_else(|| {
                format!(
                    "unknown http_write_text_response operand type for {}",
                    args[2]
                )
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let status = render_operand(&args[1], env, Some(&status_ty), named_types)?;
            let body = render_operand(&args[2], env, Some(&body_ty), named_types)?;
            Ok(format!(
                "mira_http_write_text_response_handle({handle}, {status}, {body})"
            ))
        }
        "http_write_text_response_cookie" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!(
                    "unknown http_write_text_response_cookie operand type for {}",
                    args[0]
                )
            })?;
            let status_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!(
                    "unknown http_write_text_response_cookie operand type for {}",
                    args[1]
                )
            })?;
            let body_ty = resolve_operand_type(&args[4], type_env).ok_or_else(|| {
                format!(
                    "unknown http_write_text_response_cookie operand type for {}",
                    args[4]
                )
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let status = render_operand(&args[1], env, Some(&status_ty), named_types)?;
            let body = render_operand(&args[4], env, Some(&body_ty), named_types)?;
            Ok(format!(
                "mira_http_write_text_response_cookie_handle({handle}, {status}, {}, {}, {body})",
                render_c_string_literal(&args[2]),
                render_c_string_literal(&args[3]),
            ))
        }
        "http_write_text_response_headers2" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!(
                    "unknown http_write_text_response_headers2 operand type for {}",
                    args[0]
                )
            })?;
            let status_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!(
                    "unknown http_write_text_response_headers2 operand type for {}",
                    args[1]
                )
            })?;
            let body_ty = resolve_operand_type(&args[6], type_env).ok_or_else(|| {
                format!(
                    "unknown http_write_text_response_headers2 operand type for {}",
                    args[6]
                )
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let status = render_operand(&args[1], env, Some(&status_ty), named_types)?;
            let body = render_operand(&args[6], env, Some(&body_ty), named_types)?;
            Ok(format!(
                "mira_http_write_text_response_headers2_handle({handle}, {status}, {}, {}, {}, {}, {body})",
                render_c_string_literal(&args[2]),
                render_c_string_literal(&args[3]),
                render_c_string_literal(&args[4]),
                render_c_string_literal(&args[5]),
            ))
        }
        "http_session_write_text" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!(
                    "unknown http_session_write_text operand type for {}",
                    args[0]
                )
            })?;
            let status_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!(
                    "unknown http_session_write_text operand type for {}",
                    args[1]
                )
            })?;
            let body_ty = resolve_operand_type(&args[2], type_env).ok_or_else(|| {
                format!(
                    "unknown http_session_write_text operand type for {}",
                    args[2]
                )
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let status = render_operand(&args[1], env, Some(&status_ty), named_types)?;
            let body = render_operand(&args[2], env, Some(&body_ty), named_types)?;
            Ok(format!(
                "mira_http_session_write_text_handle({handle}, {status}, {body})"
            ))
        }
        "http_session_write_text_cookie" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!(
                    "unknown http_session_write_text_cookie operand type for {}",
                    args[0]
                )
            })?;
            let status_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!(
                    "unknown http_session_write_text_cookie operand type for {}",
                    args[1]
                )
            })?;
            let body_ty = resolve_operand_type(&args[4], type_env).ok_or_else(|| {
                format!(
                    "unknown http_session_write_text_cookie operand type for {}",
                    args[4]
                )
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let status = render_operand(&args[1], env, Some(&status_ty), named_types)?;
            let body = render_operand(&args[4], env, Some(&body_ty), named_types)?;
            Ok(format!(
                "mira_http_session_write_text_cookie_handle({handle}, {status}, {}, {}, {body})",
                render_c_string_literal(&args[2]),
                render_c_string_literal(&args[3]),
            ))
        }
        "http_session_write_text_headers2" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!(
                    "unknown http_session_write_text_headers2 operand type for {}",
                    args[0]
                )
            })?;
            let status_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!(
                    "unknown http_session_write_text_headers2 operand type for {}",
                    args[1]
                )
            })?;
            let body_ty = resolve_operand_type(&args[6], type_env).ok_or_else(|| {
                format!(
                    "unknown http_session_write_text_headers2 operand type for {}",
                    args[6]
                )
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let status = render_operand(&args[1], env, Some(&status_ty), named_types)?;
            let body = render_operand(&args[6], env, Some(&body_ty), named_types)?;
            Ok(format!(
                "mira_http_session_write_text_headers2_handle({handle}, {status}, {}, {}, {}, {}, {body})",
                render_c_string_literal(&args[2]),
                render_c_string_literal(&args[3]),
                render_c_string_literal(&args[4]),
                render_c_string_literal(&args[5]),
            ))
        }
        "http_write_json_response" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!(
                    "unknown http_write_json_response operand type for {}",
                    args[0]
                )
            })?;
            let status_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!(
                    "unknown http_write_json_response operand type for {}",
                    args[1]
                )
            })?;
            let body_ty = resolve_operand_type(&args[2], type_env).ok_or_else(|| {
                format!(
                    "unknown http_write_json_response operand type for {}",
                    args[2]
                )
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let status = render_operand(&args[1], env, Some(&status_ty), named_types)?;
            let body = render_operand(&args[2], env, Some(&body_ty), named_types)?;
            Ok(format!(
                "mira_http_write_json_response_handle({handle}, {status}, {body})"
            ))
        }
        "http_write_json_response_cookie" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!(
                    "unknown http_write_json_response_cookie operand type for {}",
                    args[0]
                )
            })?;
            let status_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!(
                    "unknown http_write_json_response_cookie operand type for {}",
                    args[1]
                )
            })?;
            let body_ty = resolve_operand_type(&args[4], type_env).ok_or_else(|| {
                format!(
                    "unknown http_write_json_response_cookie operand type for {}",
                    args[4]
                )
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let status = render_operand(&args[1], env, Some(&status_ty), named_types)?;
            let body = render_operand(&args[4], env, Some(&body_ty), named_types)?;
            Ok(format!(
                "mira_http_write_json_response_cookie_handle({handle}, {status}, {}, {}, {body})",
                render_c_string_literal(&args[2]),
                render_c_string_literal(&args[3]),
            ))
        }
        "http_write_json_response_headers2" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!(
                    "unknown http_write_json_response_headers2 operand type for {}",
                    args[0]
                )
            })?;
            let status_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!(
                    "unknown http_write_json_response_headers2 operand type for {}",
                    args[1]
                )
            })?;
            let body_ty = resolve_operand_type(&args[6], type_env).ok_or_else(|| {
                format!(
                    "unknown http_write_json_response_headers2 operand type for {}",
                    args[6]
                )
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let status = render_operand(&args[1], env, Some(&status_ty), named_types)?;
            let body = render_operand(&args[6], env, Some(&body_ty), named_types)?;
            Ok(format!(
                "mira_http_write_json_response_headers2_handle({handle}, {status}, {}, {}, {}, {}, {body})",
                render_c_string_literal(&args[2]),
                render_c_string_literal(&args[3]),
                render_c_string_literal(&args[4]),
                render_c_string_literal(&args[5]),
            ))
        }
        "http_session_write_json" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!(
                    "unknown http_session_write_json operand type for {}",
                    args[0]
                )
            })?;
            let status_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!(
                    "unknown http_session_write_json operand type for {}",
                    args[1]
                )
            })?;
            let body_ty = resolve_operand_type(&args[2], type_env).ok_or_else(|| {
                format!(
                    "unknown http_session_write_json operand type for {}",
                    args[2]
                )
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let status = render_operand(&args[1], env, Some(&status_ty), named_types)?;
            let body = render_operand(&args[2], env, Some(&body_ty), named_types)?;
            Ok(format!(
                "mira_http_session_write_json_handle({handle}, {status}, {body})"
            ))
        }
        "http_session_write_json_cookie" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!(
                    "unknown http_session_write_json_cookie operand type for {}",
                    args[0]
                )
            })?;
            let status_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!(
                    "unknown http_session_write_json_cookie operand type for {}",
                    args[1]
                )
            })?;
            let body_ty = resolve_operand_type(&args[4], type_env).ok_or_else(|| {
                format!(
                    "unknown http_session_write_json_cookie operand type for {}",
                    args[4]
                )
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let status = render_operand(&args[1], env, Some(&status_ty), named_types)?;
            let body = render_operand(&args[4], env, Some(&body_ty), named_types)?;
            Ok(format!(
                "mira_http_session_write_json_cookie_handle({handle}, {status}, {}, {}, {body})",
                render_c_string_literal(&args[2]),
                render_c_string_literal(&args[3]),
            ))
        }
        "http_session_write_json_headers2" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!(
                    "unknown http_session_write_json_headers2 operand type for {}",
                    args[0]
                )
            })?;
            let status_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!(
                    "unknown http_session_write_json_headers2 operand type for {}",
                    args[1]
                )
            })?;
            let body_ty = resolve_operand_type(&args[6], type_env).ok_or_else(|| {
                format!(
                    "unknown http_session_write_json_headers2 operand type for {}",
                    args[6]
                )
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let status = render_operand(&args[1], env, Some(&status_ty), named_types)?;
            let body = render_operand(&args[6], env, Some(&body_ty), named_types)?;
            Ok(format!(
                "mira_http_session_write_json_headers2_handle({handle}, {status}, {}, {}, {}, {}, {body})",
                render_c_string_literal(&args[2]),
                render_c_string_literal(&args[3]),
                render_c_string_literal(&args[4]),
                render_c_string_literal(&args[5]),
            ))
        }
        "http_write_response_header" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!(
                    "unknown http_write_response_header operand type for {}",
                    args[0]
                )
            })?;
            let status_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!(
                    "unknown http_write_response_header operand type for {}",
                    args[1]
                )
            })?;
            let body_ty = resolve_operand_type(&args[4], type_env).ok_or_else(|| {
                format!(
                    "unknown http_write_response_header operand type for {}",
                    args[4]
                )
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let status = render_operand(&args[1], env, Some(&status_ty), named_types)?;
            let body = render_operand(&args[4], env, Some(&body_ty), named_types)?;
            Ok(format!(
                "mira_http_write_response_header_handle({handle}, {status}, {}, {}, {body})",
                render_c_string_literal(&args[2]),
                render_c_string_literal(&args[3]),
            ))
        }
        "http_response_stream_open" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown http_response_stream_open operand type for {}", args[0])
            })?;
            let status_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!("unknown http_response_stream_open operand type for {}", args[1])
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let status = render_operand(&args[1], env, Some(&status_ty), named_types)?;
            Ok(format!(
                "mira_http_response_stream_open_handle({handle}, {status}, {})",
                render_c_string_literal(&args[2]),
            ))
        }
        "http_response_stream_write" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown http_response_stream_write operand type for {}", args[0])
            })?;
            let body_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!("unknown http_response_stream_write operand type for {}", args[1])
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let body = render_operand(&args[1], env, Some(&body_ty), named_types)?;
            Ok(format!("mira_http_response_stream_write_handle({handle}, {body})"))
        }
        "http_response_stream_close" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown http_response_stream_close operand type for {}", args[0])
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_http_response_stream_close_handle({handle})"))
        }
        "http_client_open" => {
            let (host, port) = net_endpoint_for_function(function)?;
            Ok(format!(
                "mira_http_client_open_handle({}, {})",
                render_c_string_literal(&host),
                port
            ))
        }
        "http_client_request" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown http_client_request operand type for {}", args[0])
            })?;
            let request_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!("unknown http_client_request operand type for {}", args[1])
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let request = render_operand(&args[1], env, Some(&request_ty), named_types)?;
            Ok(format!("mira_http_client_request_buf_u8({handle}, {request})"))
        }
        "http_client_request_retry" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!(
                    "unknown http_client_request_retry operand type for {}",
                    args[0]
                )
            })?;
            let retries_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!(
                    "unknown http_client_request_retry operand type for {}",
                    args[1]
                )
            })?;
            let backoff_ty = resolve_operand_type(&args[2], type_env).ok_or_else(|| {
                format!(
                    "unknown http_client_request_retry operand type for {}",
                    args[2]
                )
            })?;
            let request_ty = resolve_operand_type(&args[3], type_env).ok_or_else(|| {
                format!(
                    "unknown http_client_request_retry operand type for {}",
                    args[3]
                )
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let retries = render_operand(&args[1], env, Some(&retries_ty), named_types)?;
            let backoff = render_operand(&args[2], env, Some(&backoff_ty), named_types)?;
            let request = render_operand(&args[3], env, Some(&request_ty), named_types)?;
            Ok(format!(
                "mira_http_client_request_retry_buf_u8({handle}, {retries}, {backoff}, {request})"
            ))
        }
        "http_client_close" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown http_client_close operand type for {}", args[0])
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_http_client_close_handle({handle})"))
        }
        "http_client_pool_open" => {
            let (host, port) = net_endpoint_for_function(function)?;
            let max_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown http_client_pool_open operand type for {}", args[0])
            })?;
            let max = render_operand(&args[0], env, Some(&max_ty), named_types)?;
            Ok(format!(
                "mira_http_client_pool_open_handle({}, {}, {max})",
                render_c_string_literal(&host),
                port
            ))
        }
        "http_client_pool_acquire" => {
            let pool_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown http_client_pool_acquire operand type for {}", args[0])
            })?;
            let pool = render_operand(&args[0], env, Some(&pool_ty), named_types)?;
            Ok(format!("mira_http_client_pool_acquire_handle({pool})"))
        }
        "http_client_pool_release" => {
            let pool_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown http_client_pool_release operand type for {}", args[0])
            })?;
            let handle_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!("unknown http_client_pool_release operand type for {}", args[1])
            })?;
            let pool = render_operand(&args[0], env, Some(&pool_ty), named_types)?;
            let handle = render_operand(&args[1], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_http_client_pool_release_handle({pool}, {handle})"))
        }
        "http_client_pool_close" => {
            let pool_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown http_client_pool_close operand type for {}", args[0])
            })?;
            let pool = render_operand(&args[0], env, Some(&pool_ty), named_types)?;
            Ok(format!("mira_http_client_pool_close_handle({pool})"))
        }
        "json_get_u32" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown json_get_u32 operand type for {}", args[0]))?;
            let value = render_operand(&args[0], env, Some(&value_ty), named_types)?;
            Ok(format!(
                "mira_json_get_u32_buf_u8({}, {})",
                value,
                render_c_string_literal(&args[1])
            ))
        }
        "json_get_bool" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown json_get_bool operand type for {}", args[0]))?;
            let value = render_operand(&args[0], env, Some(&value_ty), named_types)?;
            Ok(format!(
                "mira_json_get_bool_buf_u8({}, {})",
                value,
                render_c_string_literal(&args[1])
            ))
        }
        "json_get_buf" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown json_get_buf operand type for {}", args[0]))?;
            let value = render_operand(&args[0], env, Some(&value_ty), named_types)?;
            Ok(format!(
                "mira_json_get_buf_buf_u8({}, {})",
                value,
                render_c_string_literal(&args[1])
            ))
        }
        "json_get_str" | "strmap_get_str" => {
            let value_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown {} operand type for {}", instruction.op, args[0])
            })?;
            let value = render_operand(&args[0], env, Some(&value_ty), named_types)?;
            Ok(format!(
                "mira_json_get_buf_buf_u8({}, {})",
                value,
                render_c_string_literal(&args[1])
            ))
        }
        "json_has_key" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown json_has_key operand type for {}", args[0]))?;
            let value = render_operand(&args[0], env, Some(&value_ty), named_types)?;
            Ok(format!(
                "mira_json_has_key_buf_u8({}, {})",
                value,
                render_c_string_literal(&args[1])
            ))
        }
        "json_get_u32_or" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown json_get_u32_or operand type for {}", args[0]))?;
            let default_ty = resolve_operand_type(&args[2], type_env)
                .ok_or_else(|| format!("unknown json_get_u32_or operand type for {}", args[2]))?;
            let value = render_operand(&args[0], env, Some(&value_ty), named_types)?;
            let default = render_operand(&args[2], env, Some(&default_ty), named_types)?;
            let key = render_c_string_literal(&args[1]);
            Ok(format!(
                "(mira_json_has_key_buf_u8({value}, {key}) ? mira_json_get_u32_buf_u8({value}, {key}) : {default})"
            ))
        }
        "json_get_bool_or" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown json_get_bool_or operand type for {}", args[0]))?;
            let default_ty = resolve_operand_type(&args[2], type_env)
                .ok_or_else(|| format!("unknown json_get_bool_or operand type for {}", args[2]))?;
            let value = render_operand(&args[0], env, Some(&value_ty), named_types)?;
            let default = render_operand(&args[2], env, Some(&default_ty), named_types)?;
            let key = render_c_string_literal(&args[1]);
            Ok(format!(
                "(mira_json_has_key_buf_u8({value}, {key}) ? mira_json_get_bool_buf_u8({value}, {key}) : {default})"
            ))
        }
        "json_get_buf_or" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown json_get_buf_or operand type for {}", args[0]))?;
            let default_ty = resolve_operand_type(&args[2], type_env)
                .ok_or_else(|| format!("unknown json_get_buf_or operand type for {}", args[2]))?;
            let value = render_operand(&args[0], env, Some(&value_ty), named_types)?;
            let default = render_operand(&args[2], env, Some(&default_ty), named_types)?;
            let key = render_c_string_literal(&args[1]);
            Ok(format!(
                "(mira_json_has_key_buf_u8({value}, {key}) ? mira_json_get_buf_buf_u8({value}, {key}) : {default})"
            ))
        }
        "json_get_str_or" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown json_get_str_or operand type for {}", args[0]))?;
            let default_ty = resolve_operand_type(&args[2], type_env)
                .ok_or_else(|| format!("unknown json_get_str_or operand type for {}", args[2]))?;
            let value = render_operand(&args[0], env, Some(&value_ty), named_types)?;
            let default = render_operand(&args[2], env, Some(&default_ty), named_types)?;
            let key = render_c_string_literal(&args[1]);
            Ok(format!(
                "(mira_json_has_key_buf_u8({value}, {key}) ? mira_json_get_buf_buf_u8({value}, {key}) : {default})"
            ))
        }
        "json_array_len" | "strlist_len" => {
            let value_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown {} operand type for {}", instruction.op, args[0])
            })?;
            let value = render_operand(&args[0], env, Some(&value_ty), named_types)?;
            Ok(format!("mira_json_array_len_buf_u8({value})"))
        }
        "json_index_u32" | "strlist_index_u32" => {
            let value_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown {} operand type for {}", instruction.op, args[0])
            })?;
            let index_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!("unknown {} operand type for {}", instruction.op, args[1])
            })?;
            let value = render_operand(&args[0], env, Some(&value_ty), named_types)?;
            let index = render_operand(&args[1], env, Some(&index_ty), named_types)?;
            Ok(format!("mira_json_index_u32_buf_u8({value}, {index})"))
        }
        "json_index_bool" | "strlist_index_bool" => {
            let value_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown {} operand type for {}", instruction.op, args[0])
            })?;
            let index_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!("unknown {} operand type for {}", instruction.op, args[1])
            })?;
            let value = render_operand(&args[0], env, Some(&value_ty), named_types)?;
            let index = render_operand(&args[1], env, Some(&index_ty), named_types)?;
            Ok(format!("mira_json_index_bool_buf_u8({value}, {index})"))
        }
        "json_index_str" | "strlist_index_str" => {
            let value_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown {} operand type for {}", instruction.op, args[0])
            })?;
            let index_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!("unknown {} operand type for {}", instruction.op, args[1])
            })?;
            let value = render_operand(&args[0], env, Some(&value_ty), named_types)?;
            let index = render_operand(&args[1], env, Some(&index_ty), named_types)?;
            Ok(format!("mira_json_index_str_buf_u8({value}, {index})"))
        }
        "strmap_get_u32" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown strmap_get_u32 operand type for {}", args[0]))?;
            let value = render_operand(&args[0], env, Some(&value_ty), named_types)?;
            Ok(format!(
                "mira_json_get_u32_buf_u8({}, {})",
                value,
                render_c_string_literal(&args[1])
            ))
        }
        "strmap_get_bool" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown strmap_get_bool operand type for {}", args[0]))?;
            let value = render_operand(&args[0], env, Some(&value_ty), named_types)?;
            Ok(format!(
                "mira_json_get_bool_buf_u8({}, {})",
                value,
                render_c_string_literal(&args[1])
            ))
        }
        "json_encode_obj" => render_json_encode_obj_expr(args, env, type_env, named_types),
        "json_encode_arr" => render_json_encode_arr_expr(args, env, type_env, named_types),
        "config_get_u32" => Ok(format!(
            "{}u",
            config_entry_for_function(function, &args[0])?
        )),
        "config_get_bool" => {
            let value = parse_bool_text(&config_entry_for_function(function, &args[0])?)
                .ok_or_else(|| format!("invalid config bool for {}", args[0]))?;
            Ok(if value {
                "true".to_string()
            } else {
                "false".to_string()
            })
        }
        "config_get_str" => Ok(format!(
            "mira_buf_lit_u8({})",
            render_c_string_literal(&config_entry_for_function(function, &args[0])?)
        )),
        "config_has" => Ok(if config_entry_for_function(function, &args[0]).is_ok() {
            "true".to_string()
        } else {
            "false".to_string()
        }),
        "env_get_u32" => Ok(format!(
            "mira_env_get_u32({})",
            render_c_string_literal(&args[0])
        )),
        "env_get_bool" => Ok(format!(
            "mira_env_get_bool({})",
            render_c_string_literal(&args[0])
        )),
        "env_get_str" => Ok(format!(
            "mira_env_get_str_u8({})",
            render_c_string_literal(&args[0])
        )),
        "env_has" => Ok(format!("mira_env_has({})", render_c_string_literal(&args[0]))),
        "buf_before_lit" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown buf_before_lit operand type for {}", args[0]))?;
            let value = render_operand(&args[0], env, Some(&value_ty), named_types)?;
            Ok(format!(
                "mira_buf_before_lit_u8({}, {})",
                value,
                render_c_string_literal(&args[1])
            ))
        }
        "buf_after_lit" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown buf_after_lit operand type for {}", args[0]))?;
            let value = render_operand(&args[0], env, Some(&value_ty), named_types)?;
            Ok(format!(
                "mira_buf_after_lit_u8({}, {})",
                value,
                render_c_string_literal(&args[1])
            ))
        }
        "buf_trim_ascii" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown buf_trim_ascii operand type for {}", args[0]))?;
            let value = render_operand(&args[0], env, Some(&value_ty), named_types)?;
            Ok(format!("mira_buf_trim_ascii_u8({value})"))
        }
        "date_parse_ymd" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown date_parse_ymd operand type for {}", args[0]))?;
            let value = render_operand(&args[0], env, Some(&value_ty), named_types)?;
            Ok(format!("mira_date_parse_ymd({value})"))
        }
        "time_parse_hms" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown time_parse_hms operand type for {}", args[0]))?;
            let value = render_operand(&args[0], env, Some(&value_ty), named_types)?;
            Ok(format!("mira_time_parse_hms({value})"))
        }
        "date_format_ymd" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown date_format_ymd operand type for {}", args[0]))?;
            let value = render_operand(&args[0], env, Some(&value_ty), named_types)?;
            Ok(format!("mira_date_format_ymd({value})"))
        }
        "time_format_hms" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown time_format_hms operand type for {}", args[0]))?;
            let value = render_operand(&args[0], env, Some(&value_ty), named_types)?;
            Ok(format!("mira_time_format_hms({value})"))
        }
        "http_session_accept" => {
            let listener_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown http_session_accept operand type for {}", args[0])
            })?;
            let listener = render_operand(&args[0], env, Some(&listener_ty), named_types)?;
            Ok(format!("mira_http_session_accept_handle({listener})"))
        }
        "http_session_request" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown http_session_request operand type for {}", args[0])
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_http_session_request_buf_u8({handle})"))
        }
        "http_session_close" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown http_session_close operand type for {}", args[0])
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_http_session_close_handle({handle})"))
        }
        "db_open" => Ok(format!(
            "mira_db_open_handle({})",
            render_c_string_literal(&args[0])
        )),
        "db_close" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown db_close operand type for {}", args[0]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_db_close_handle({handle})"))
        }
        "db_exec" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown db_exec operand type for {}", args[0]))?;
            let sql_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown db_exec operand type for {}", args[1]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let sql = render_operand(&args[1], env, Some(&sql_ty), named_types)?;
            Ok(format!("mira_db_exec_handle_sql_buf_u8({handle}, {sql})"))
        }
        "db_prepare" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown db_prepare operand type for {}", args[0]))?;
            let sql_ty = resolve_operand_type(&args[2], type_env)
                .ok_or_else(|| format!("unknown db_prepare operand type for {}", args[2]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let sql = render_operand(&args[2], env, Some(&sql_ty), named_types)?;
            Ok(format!(
                "mira_db_prepare_handle_stmt_sql_buf_u8({handle}, {}, {sql})",
                render_c_string_literal(&args[1])
            ))
        }
        "db_exec_prepared" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown db_exec_prepared operand type for {}", args[0]))?;
            let params_ty = resolve_operand_type(&args[2], type_env)
                .ok_or_else(|| format!("unknown db_exec_prepared operand type for {}", args[2]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let params = render_operand(&args[2], env, Some(&params_ty), named_types)?;
            Ok(format!(
                "mira_db_exec_prepared_handle_stmt_params_buf_u8({handle}, {}, {params})",
                render_c_string_literal(&args[1])
            ))
        }
        "db_query_u32" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown db_query_u32 operand type for {}", args[0]))?;
            let sql_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown db_query_u32 operand type for {}", args[1]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let sql = render_operand(&args[1], env, Some(&sql_ty), named_types)?;
            Ok(format!(
                "mira_db_query_u32_handle_sql_buf_u8({handle}, {sql})"
            ))
        }
        "db_query_buf" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown db_query_buf operand type for {}", args[0]))?;
            let sql_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown db_query_buf operand type for {}", args[1]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let sql = render_operand(&args[1], env, Some(&sql_ty), named_types)?;
            Ok(format!(
                "mira_db_query_buf_handle_sql_buf_u8({handle}, {sql})"
            ))
        }
        "db_query_row" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown db_query_row operand type for {}", args[0]))?;
            let sql_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown db_query_row operand type for {}", args[1]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let sql = render_operand(&args[1], env, Some(&sql_ty), named_types)?;
            Ok(format!(
                "mira_db_query_row_handle_sql_buf_u8({handle}, {sql})"
            ))
        }
        "db_query_prepared_u32" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown db_query_prepared_u32 operand type for {}", args[0])
            })?;
            let params_ty = resolve_operand_type(&args[2], type_env).ok_or_else(|| {
                format!("unknown db_query_prepared_u32 operand type for {}", args[2])
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let params = render_operand(&args[2], env, Some(&params_ty), named_types)?;
            Ok(format!(
                "mira_db_query_prepared_u32_handle_stmt_params_buf_u8({handle}, {}, {params})",
                render_c_string_literal(&args[1])
            ))
        }
        "db_query_prepared_buf" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown db_query_prepared_buf operand type for {}", args[0])
            })?;
            let params_ty = resolve_operand_type(&args[2], type_env).ok_or_else(|| {
                format!("unknown db_query_prepared_buf operand type for {}", args[2])
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let params = render_operand(&args[2], env, Some(&params_ty), named_types)?;
            Ok(format!(
                "mira_db_query_prepared_buf_handle_stmt_params_buf_u8({handle}, {}, {params})",
                render_c_string_literal(&args[1])
            ))
        }
        "db_query_prepared_row" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown db_query_prepared_row operand type for {}", args[0])
            })?;
            let params_ty = resolve_operand_type(&args[2], type_env).ok_or_else(|| {
                format!("unknown db_query_prepared_row operand type for {}", args[2])
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let params = render_operand(&args[2], env, Some(&params_ty), named_types)?;
            Ok(format!(
                "mira_db_query_prepared_row_handle_stmt_params_buf_u8({handle}, {}, {params})",
                render_c_string_literal(&args[1])
            ))
        }
        "db_row_found" => {
            let row_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown db_row_found operand type for {}", args[0]))?;
            let row = render_operand(&args[0], env, Some(&row_ty), named_types)?;
            Ok(format!("({row}.len > 0u)"))
        }
        "db_last_error_code" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown db_last_error_code operand type for {}", args[0]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_db_last_error_code_handle({handle})"))
        }
        "db_last_error_retryable" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown db_last_error_retryable operand type for {}", args[0])
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_db_last_error_retryable_handle({handle})"))
        }
        "db_begin" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown db_begin operand type for {}", args[0]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_db_begin_handle({handle})"))
        }
        "db_commit" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown db_commit operand type for {}", args[0]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_db_commit_handle({handle})"))
        }
        "db_rollback" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown db_rollback operand type for {}", args[0]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_db_rollback_handle({handle})"))
        }
        "db_pool_open" => {
            let max_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown db_pool_open operand type for {}", args[1]))?;
            let max = render_operand(&args[1], env, Some(&max_ty), named_types)?;
            Ok(format!(
                "mira_db_pool_open_handle({}, {max})",
                render_c_string_literal(&args[0])
            ))
        }
        "db_pool_set_max_idle" => {
            let pool_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown db_pool_set_max_idle operand type for {}", args[0])
            })?;
            let value_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!("unknown db_pool_set_max_idle operand type for {}", args[1])
            })?;
            let pool = render_operand(&args[0], env, Some(&pool_ty), named_types)?;
            let value = render_operand(&args[1], env, Some(&value_ty), named_types)?;
            Ok(format!("mira_db_pool_set_max_idle_handle({pool}, {value})"))
        }
        "db_pool_leased" => {
            let pool_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown db_pool_leased operand type for {}", args[0]))?;
            let pool = render_operand(&args[0], env, Some(&pool_ty), named_types)?;
            Ok(format!("mira_db_pool_leased_handle({pool})"))
        }
        "db_pool_acquire" => {
            let pool_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown db_pool_acquire operand type for {}", args[0]))?;
            let pool = render_operand(&args[0], env, Some(&pool_ty), named_types)?;
            Ok(format!("mira_db_pool_acquire_handle({pool})"))
        }
        "db_pool_release" => {
            let pool_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown db_pool_release operand type for {}", args[0]))?;
            let handle_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown db_pool_release operand type for {}", args[1]))?;
            let pool = render_operand(&args[0], env, Some(&pool_ty), named_types)?;
            let handle = render_operand(&args[1], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_db_pool_release_handle({pool}, {handle})"))
        }
        "db_pool_close" => {
            let pool_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown db_pool_close operand type for {}", args[0]))?;
            let pool = render_operand(&args[0], env, Some(&pool_ty), named_types)?;
            Ok(format!("mira_db_pool_close_handle({pool})"))
        }
        "cache_open" => Ok(format!(
            "mira_cache_open_handle({})",
            render_c_string_literal(&args[0])
        )),
        "cache_close" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown cache_close operand type for {}", args[0]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_cache_close_handle({handle})"))
        }
        "cache_get_buf" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown cache_get_buf operand type for {}", args[0]))?;
            let key_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown cache_get_buf operand type for {}", args[1]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let key = render_operand(&args[1], env, Some(&key_ty), named_types)?;
            Ok(format!("mira_cache_get_buf_handle_key_u8({handle}, {key})"))
        }
        "cache_set_buf" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown cache_set_buf operand type for {}", args[0]))?;
            let key_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown cache_set_buf operand type for {}", args[1]))?;
            let value_ty = resolve_operand_type(&args[2], type_env)
                .ok_or_else(|| format!("unknown cache_set_buf operand type for {}", args[2]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let key = render_operand(&args[1], env, Some(&key_ty), named_types)?;
            let value = render_operand(&args[2], env, Some(&value_ty), named_types)?;
            Ok(format!("mira_cache_set_buf_handle_key_value_u8({handle}, {key}, {value})"))
        }
        "cache_set_buf_ttl" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown cache_set_buf_ttl operand type for {}", args[0])
            })?;
            let key_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!("unknown cache_set_buf_ttl operand type for {}", args[1])
            })?;
            let ttl_ty = resolve_operand_type(&args[2], type_env).ok_or_else(|| {
                format!("unknown cache_set_buf_ttl operand type for {}", args[2])
            })?;
            let value_ty = resolve_operand_type(&args[3], type_env).ok_or_else(|| {
                format!("unknown cache_set_buf_ttl operand type for {}", args[3])
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let key = render_operand(&args[1], env, Some(&key_ty), named_types)?;
            let ttl = render_operand(&args[2], env, Some(&ttl_ty), named_types)?;
            let value = render_operand(&args[3], env, Some(&value_ty), named_types)?;
            Ok(format!(
                "mira_cache_set_buf_ttl_handle_key_value_u8({handle}, {key}, {ttl}, {value})"
            ))
        }
        "cache_del" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown cache_del operand type for {}", args[0]))?;
            let key_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown cache_del operand type for {}", args[1]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let key = render_operand(&args[1], env, Some(&key_ty), named_types)?;
            Ok(format!("mira_cache_del_handle_key_u8({handle}, {key})"))
        }
        "queue_open" => Ok(format!(
            "mira_queue_open_handle({})",
            render_c_string_literal(&args[0])
        )),
        "queue_close" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown queue_close operand type for {}", args[0]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_queue_close_handle({handle})"))
        }
        "queue_push_buf" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown queue_push_buf operand type for {}", args[0]))?;
            let value_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown queue_push_buf operand type for {}", args[1]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let value = render_operand(&args[1], env, Some(&value_ty), named_types)?;
            Ok(format!("mira_queue_push_buf_handle_value_u8({handle}, {value})"))
        }
        "queue_pop_buf" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown queue_pop_buf operand type for {}", args[0]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_queue_pop_buf_handle({handle})"))
        }
        "queue_len" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown queue_len operand type for {}", args[0]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_queue_len_handle({handle})"))
        }
        "stream_open" => Ok(format!(
            "mira_stream_open_handle({})",
            render_c_string_literal(&args[0])
        )),
        "stream_close" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown stream_close operand type for {}", args[0]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_stream_close_handle({handle})"))
        }
        "stream_publish_buf" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown stream_publish_buf operand type for {}", args[0])
            })?;
            let value_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!("unknown stream_publish_buf operand type for {}", args[1])
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let value = render_operand(&args[1], env, Some(&value_ty), named_types)?;
            Ok(format!("mira_stream_publish_buf_handle_value_u8({handle}, {value})"))
        }
        "stream_len" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown stream_len operand type for {}", args[0]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_stream_len_handle({handle})"))
        }
        "stream_replay_open" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown stream_replay_open operand type for {}", args[0])
            })?;
            let offset_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!("unknown stream_replay_open operand type for {}", args[1])
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let offset = render_operand(&args[1], env, Some(&offset_ty), named_types)?;
            Ok(format!("mira_stream_replay_open_handle({handle}, {offset})"))
        }
        "stream_replay_next" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown stream_replay_next operand type for {}", args[0])
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_stream_replay_next_handle({handle})"))
        }
        "stream_replay_offset" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown stream_replay_offset operand type for {}", args[0])
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_stream_replay_offset_handle({handle})"))
        }
        "stream_replay_close" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown stream_replay_close operand type for {}", args[0])
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_stream_replay_close_handle({handle})"))
        }
        "shard_route_u32" => {
            let key_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown shard_route_u32 operand type for {}", args[0]))?;
            let count_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!("unknown shard_route_u32 operand type for {}", args[1])
            })?;
            let key = render_operand(&args[0], env, Some(&key_ty), named_types)?;
            let count = render_operand(&args[1], env, Some(&count_ty), named_types)?;
            Ok(format!("mira_shard_route_u32_buf_u8({key}, {count})"))
        }
        "lease_open" => Ok(format!(
            "mira_lease_open_handle({})",
            render_c_string_literal(&args[0])
        )),
        "lease_acquire" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown lease_acquire operand type for {}", args[0]))?;
            let owner_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown lease_acquire operand type for {}", args[1]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let owner = render_operand(&args[1], env, Some(&owner_ty), named_types)?;
            Ok(format!("mira_lease_acquire_handle({handle}, {owner})"))
        }
        "lease_owner" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown lease_owner operand type for {}", args[0]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_lease_owner_handle({handle})"))
        }
        "lease_transfer" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown lease_transfer operand type for {}", args[0]))?;
            let owner_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown lease_transfer operand type for {}", args[1]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let owner = render_operand(&args[1], env, Some(&owner_ty), named_types)?;
            Ok(format!("mira_lease_transfer_handle({handle}, {owner})"))
        }
        "lease_release" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown lease_release operand type for {}", args[0]))?;
            let owner_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown lease_release operand type for {}", args[1]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let owner = render_operand(&args[1], env, Some(&owner_ty), named_types)?;
            Ok(format!("mira_lease_release_handle({handle}, {owner})"))
        }
        "lease_close" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown lease_close operand type for {}", args[0]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_lease_close_handle({handle})"))
        }
        "placement_open" => Ok(format!(
            "mira_placement_open_handle({})",
            render_c_string_literal(&args[0])
        )),
        "placement_assign" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown placement_assign operand type for {}", args[0])
            })?;
            let shard_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!("unknown placement_assign operand type for {}", args[1])
            })?;
            let node_ty = resolve_operand_type(&args[2], type_env).ok_or_else(|| {
                format!("unknown placement_assign operand type for {}", args[2])
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let shard = render_operand(&args[1], env, Some(&shard_ty), named_types)?;
            let node = render_operand(&args[2], env, Some(&node_ty), named_types)?;
            Ok(format!("mira_placement_assign_handle({handle}, {shard}, {node})"))
        }
        "placement_lookup" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown placement_lookup operand type for {}", args[0])
            })?;
            let shard_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!("unknown placement_lookup operand type for {}", args[1])
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let shard = render_operand(&args[1], env, Some(&shard_ty), named_types)?;
            Ok(format!("mira_placement_lookup_handle({handle}, {shard})"))
        }
        "placement_close" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown placement_close operand type for {}", args[0])
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_placement_close_handle({handle})"))
        }
        "coord_open" => Ok(format!(
            "mira_coord_open_handle({})",
            render_c_string_literal(&args[0])
        )),
        "coord_store_u32" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown coord_store_u32 operand type for {}", args[0])
            })?;
            let value_ty = resolve_operand_type(&args[2], type_env).ok_or_else(|| {
                format!("unknown coord_store_u32 operand type for {}", args[2])
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let value = render_operand(&args[2], env, Some(&value_ty), named_types)?;
            Ok(format!(
                "mira_coord_store_u32_handle({handle}, {}, {value})",
                render_c_string_literal(&args[1])
            ))
        }
        "coord_load_u32" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown coord_load_u32 operand type for {}", args[0])
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!(
                "mira_coord_load_u32_handle({handle}, {})",
                render_c_string_literal(&args[1])
            ))
        }
        "coord_close" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown coord_close operand type for {}", args[0]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_coord_close_handle({handle})"))
        }
        "batch_open" => Ok("mira_batch_open_handle()".to_string()),
        "batch_push_u64" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown batch_push_u64 operand type for {}", args[0]))?;
            let value_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown batch_push_u64 operand type for {}", args[1]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let value = render_operand(&args[1], env, Some(&value_ty), named_types)?;
            Ok(format!("mira_batch_push_u64_handle_value({handle}, {value})"))
        }
        "batch_len" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown batch_len operand type for {}", args[0]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_batch_len_handle({handle})"))
        }
        "batch_flush_sum_u64" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown batch_flush_sum_u64 operand type for {}", args[0])
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_batch_flush_sum_u64_handle({handle})"))
        }
        "batch_close" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown batch_close operand type for {}", args[0]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_batch_close_handle({handle})"))
        }
        "agg_open_u64" => Ok("mira_agg_open_u64_handle()".to_string()),
        "agg_add_u64" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown agg_add_u64 operand type for {}", args[0]))?;
            let value_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown agg_add_u64 operand type for {}", args[1]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let value = render_operand(&args[1], env, Some(&value_ty), named_types)?;
            Ok(format!("mira_agg_add_u64_handle_value({handle}, {value})"))
        }
        "agg_count" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown agg_count operand type for {}", args[0]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_agg_count_handle({handle})"))
        }
        "agg_sum_u64" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown agg_sum_u64 operand type for {}", args[0]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_agg_sum_u64_handle({handle})"))
        }
        "agg_avg_u64" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown agg_avg_u64 operand type for {}", args[0]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_agg_avg_u64_handle({handle})"))
        }
        "agg_min_u64" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown agg_min_u64 operand type for {}", args[0]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_agg_min_u64_handle({handle})"))
        }
        "agg_max_u64" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown agg_max_u64 operand type for {}", args[0]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_agg_max_u64_handle({handle})"))
        }
        "agg_close" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown agg_close operand type for {}", args[0]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_agg_close_handle({handle})"))
        }
        "window_open_ms" => {
            let width_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown window_open_ms operand type for {}", args[0]))?;
            let width = render_operand(&args[0], env, Some(&width_ty), named_types)?;
            Ok(format!("mira_window_open_ms_handle({width})"))
        }
        "window_add_u64" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown window_add_u64 operand type for {}", args[0]))?;
            let value_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown window_add_u64 operand type for {}", args[1]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let value = render_operand(&args[1], env, Some(&value_ty), named_types)?;
            Ok(format!("mira_window_add_u64_handle_value({handle}, {value})"))
        }
        "window_count" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown window_count operand type for {}", args[0]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_window_count_handle({handle})"))
        }
        "window_sum_u64" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown window_sum_u64 operand type for {}", args[0]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_window_sum_u64_handle({handle})"))
        }
        "window_avg_u64" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown window_avg_u64 operand type for {}", args[0]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_window_avg_u64_handle({handle})"))
        }
        "window_min_u64" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown window_min_u64 operand type for {}", args[0]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_window_min_u64_handle({handle})"))
        }
        "window_max_u64" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown window_max_u64 operand type for {}", args[0]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_window_max_u64_handle({handle})"))
        }
        "window_close" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown window_close operand type for {}", args[0]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_window_close_handle({handle})"))
        }
        "msg_log_open" => Ok("mira_msg_log_open_handle()".to_string()),
        "msg_log_close" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown msg_log_close operand type for {}", args[0]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_msg_log_close_handle({handle})"))
        }
        "msg_send" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown msg_send operand type for {}", args[0]))?;
            let payload_ty = resolve_operand_type(&args[3], type_env)
                .ok_or_else(|| format!("unknown msg_send operand type for {}", args[3]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let payload = render_operand(&args[3], env, Some(&payload_ty), named_types)?;
            Ok(format!(
                "mira_msg_send_handle_buf_u8({handle}, {}, {}, {payload})",
                render_c_string_literal(&args[1]),
                render_c_string_literal(&args[2])
            ))
        }
        "msg_send_dedup" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown msg_send_dedup operand type for {}", args[0]))?;
            let key_ty = resolve_operand_type(&args[3], type_env)
                .ok_or_else(|| format!("unknown msg_send_dedup operand type for {}", args[3]))?;
            let payload_ty = resolve_operand_type(&args[4], type_env)
                .ok_or_else(|| format!("unknown msg_send_dedup operand type for {}", args[4]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let key = render_operand(&args[3], env, Some(&key_ty), named_types)?;
            let payload = render_operand(&args[4], env, Some(&payload_ty), named_types)?;
            Ok(format!(
                "mira_msg_send_dedup_handle_buf_u8({handle}, {}, {}, {key}, {payload})",
                render_c_string_literal(&args[1]),
                render_c_string_literal(&args[2])
            ))
        }
        "msg_subscribe" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown msg_subscribe operand type for {}", args[0]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!(
                "mira_msg_subscribe_handle({handle}, {}, {})",
                render_c_string_literal(&args[1]),
                render_c_string_literal(&args[2])
            ))
        }
        "msg_subscriber_count" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown msg_subscriber_count operand type for {}", args[0])
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!(
                "mira_msg_subscriber_count_handle({handle}, {})",
                render_c_string_literal(&args[1])
            ))
        }
        "msg_fanout" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown msg_fanout operand type for {}", args[0]))?;
            let payload_ty = resolve_operand_type(&args[2], type_env)
                .ok_or_else(|| format!("unknown msg_fanout operand type for {}", args[2]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let payload = render_operand(&args[2], env, Some(&payload_ty), named_types)?;
            Ok(format!(
                "mira_msg_fanout_handle_buf_u8({handle}, {}, {payload})",
                render_c_string_literal(&args[1])
            ))
        }
        "msg_recv_next" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown msg_recv_next operand type for {}", args[0]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!(
                "mira_msg_recv_next_handle({handle}, {})",
                render_c_string_literal(&args[1])
            ))
        }
        "msg_recv_seq" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown msg_recv_seq operand type for {}", args[0]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!(
                "mira_msg_recv_seq_handle({handle}, {})",
                render_c_string_literal(&args[1])
            ))
        }
        "msg_ack" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown msg_ack operand type for {}", args[0]))?;
            let seq_ty = resolve_operand_type(&args[2], type_env)
                .ok_or_else(|| format!("unknown msg_ack operand type for {}", args[2]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let seq = render_operand(&args[2], env, Some(&seq_ty), named_types)?;
            Ok(format!(
                "mira_msg_ack_handle({handle}, {}, {seq})",
                render_c_string_literal(&args[1])
            ))
        }
        "msg_mark_retry" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown msg_mark_retry operand type for {}", args[0]))?;
            let seq_ty = resolve_operand_type(&args[2], type_env)
                .ok_or_else(|| format!("unknown msg_mark_retry operand type for {}", args[2]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let seq = render_operand(&args[2], env, Some(&seq_ty), named_types)?;
            Ok(format!(
                "mira_msg_mark_retry_handle({handle}, {}, {seq})",
                render_c_string_literal(&args[1])
            ))
        }
        "msg_retry_count" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown msg_retry_count operand type for {}", args[0]))?;
            let seq_ty = resolve_operand_type(&args[2], type_env)
                .ok_or_else(|| format!("unknown msg_retry_count operand type for {}", args[2]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let seq = render_operand(&args[2], env, Some(&seq_ty), named_types)?;
            Ok(format!(
                "mira_msg_retry_count_handle({handle}, {}, {seq})",
                render_c_string_literal(&args[1])
            ))
        }
        "msg_pending_count" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown msg_pending_count operand type for {}", args[0])
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!(
                "mira_msg_pending_count_handle({handle}, {})",
                render_c_string_literal(&args[1])
            ))
        }
        "msg_delivery_total" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown msg_delivery_total operand type for {}", args[0])
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!(
                "mira_msg_delivery_total_handle({handle}, {})",
                render_c_string_literal(&args[1])
            ))
        }
        "msg_failure_class" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown msg_failure_class operand type for {}", args[0])
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_msg_failure_class_handle({handle})"))
        }
        "msg_replay_open" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown msg_replay_open operand type for {}", args[0]))?;
            let seq_ty = resolve_operand_type(&args[2], type_env)
                .ok_or_else(|| format!("unknown msg_replay_open operand type for {}", args[2]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let from_seq = render_operand(&args[2], env, Some(&seq_ty), named_types)?;
            Ok(format!(
                "mira_msg_replay_open_handle({handle}, {}, {from_seq})",
                render_c_string_literal(&args[1])
            ))
        }
        "msg_replay_next" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown msg_replay_next operand type for {}", args[0]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_msg_replay_next_handle({handle})"))
        }
        "msg_replay_seq" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown msg_replay_seq operand type for {}", args[0]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_msg_replay_seq_handle({handle})"))
        }
        "msg_replay_close" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown msg_replay_close operand type for {}", args[0]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_msg_replay_close_handle({handle})"))
        }
        "service_open" => Ok(format!(
            "mira_service_open_handle({})",
            render_c_string_literal(service_name_for_function(function)?)
        )),
        "service_close" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown service_close operand type for {}", args[0]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_service_close_handle({handle})"))
        }
        "service_shutdown" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown service_shutdown operand type for {}", args[0]))?;
            let grace_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown service_shutdown operand type for {}", args[1]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let grace = render_operand(&args[1], env, Some(&grace_ty), named_types)?;
            Ok(format!("mira_service_shutdown_handle({handle}, {grace})"))
        }
        "service_log" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown service_log operand type for {}", args[0]))?;
            let msg_ty = resolve_operand_type(&args[2], type_env)
                .ok_or_else(|| format!("unknown service_log operand type for {}", args[2]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let message = render_operand(&args[2], env, Some(&msg_ty), named_types)?;
            Ok(format!(
                "mira_service_log_buf_u8({handle}, {}, {message})",
                render_c_string_literal(&args[1])
            ))
        }
        "service_trace_begin" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown service_trace_begin operand type for {}", args[0])
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!(
                "mira_service_trace_begin_handle({handle}, {})",
                render_c_string_literal(&args[1])
            ))
        }
        "service_trace_end" => {
            let trace_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown service_trace_end operand type for {}", args[0]))?;
            let trace = render_operand(&args[0], env, Some(&trace_ty), named_types)?;
            Ok(format!("mira_service_trace_end_handle({trace})"))
        }
        "service_metric_count" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown service_metric_count operand type for {}", args[0])
            })?;
            let value_ty = resolve_operand_type(&args[2], type_env).ok_or_else(|| {
                format!("unknown service_metric_count operand type for {}", args[2])
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let value = render_operand(&args[2], env, Some(&value_ty), named_types)?;
            Ok(format!(
                "mira_service_metric_count_handle({handle}, {}, {value})",
                render_c_string_literal(&args[1])
            ))
        }
        "service_metric_count_dim" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown service_metric_count_dim operand type for {}", args[0])
            })?;
            let value_ty = resolve_operand_type(&args[3], type_env).ok_or_else(|| {
                format!("unknown service_metric_count_dim operand type for {}", args[3])
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let value = render_operand(&args[3], env, Some(&value_ty), named_types)?;
            Ok(format!(
                "mira_service_metric_count_dim_handle({handle}, {}, {}, {value})",
                render_c_string_literal(&args[1]),
                render_c_string_literal(&args[2])
            ))
        }
        "service_metric_total" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown service_metric_total operand type for {}", args[0])
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!(
                "mira_service_metric_total_handle({handle}, {})",
                render_c_string_literal(&args[1])
            ))
        }
        "service_health_status" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown service_health_status operand type for {}", args[0])
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_service_health_status_handle({handle})"))
        }
        "service_readiness_status" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!(
                    "unknown service_readiness_status operand type for {}",
                    args[0]
                )
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_service_readiness_status_handle({handle})"))
        }
        "service_set_health" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown service_set_health operand type for {}", args[0]))?;
            let status_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown service_set_health operand type for {}", args[1]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let status = render_operand(&args[1], env, Some(&status_ty), named_types)?;
            Ok(format!("mira_service_set_health_handle({handle}, {status})"))
        }
        "service_set_readiness" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown service_set_readiness operand type for {}", args[0])
            })?;
            let status_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!("unknown service_set_readiness operand type for {}", args[1])
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let status = render_operand(&args[1], env, Some(&status_ty), named_types)?;
            Ok(format!("mira_service_set_readiness_handle({handle}, {status})"))
        }
        "service_set_degraded" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown service_set_degraded operand type for {}", args[0])
            })?;
            let degraded_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!("unknown service_set_degraded operand type for {}", args[1])
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let degraded = render_operand(&args[1], env, Some(&degraded_ty), named_types)?;
            Ok(format!("mira_service_set_degraded_handle({handle}, {degraded})"))
        }
        "service_degraded" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown service_degraded operand type for {}", args[0]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_service_degraded_handle({handle})"))
        }
        "service_event" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown service_event operand type for {}", args[0]))?;
            let msg_ty = resolve_operand_type(&args[2], type_env)
                .ok_or_else(|| format!("unknown service_event operand type for {}", args[2]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let message = render_operand(&args[2], env, Some(&msg_ty), named_types)?;
            Ok(format!(
                "mira_service_event_buf_u8({handle}, {}, {message})",
                render_c_string_literal(&args[1])
            ))
        }
        "service_event_total" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown service_event_total operand type for {}", args[0])
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!(
                "mira_service_event_total_handle({handle}, {})",
                render_c_string_literal(&args[1])
            ))
        }
        "service_trace_link" => {
            let trace_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown service_trace_link operand type for {}", args[0]))?;
            let parent_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!("unknown service_trace_link operand type for {}", args[1])
            })?;
            let trace = render_operand(&args[0], env, Some(&trace_ty), named_types)?;
            let parent = render_operand(&args[1], env, Some(&parent_ty), named_types)?;
            Ok(format!("mira_service_trace_link_handle({trace}, {parent})"))
        }
        "service_trace_link_count" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown service_trace_link_count operand type for {}", args[0])
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_service_trace_link_count_handle({handle})"))
        }
        "service_failure_count" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown service_failure_count operand type for {}", args[0])
            })?;
            let value_ty = resolve_operand_type(&args[2], type_env).ok_or_else(|| {
                format!("unknown service_failure_count operand type for {}", args[2])
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let value = render_operand(&args[2], env, Some(&value_ty), named_types)?;
            Ok(format!(
                "mira_service_failure_count_handle({handle}, {}, {value})",
                render_c_string_literal(&args[1])
            ))
        }
        "service_failure_total" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown service_failure_total operand type for {}", args[0])
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!(
                "mira_service_failure_total_handle({handle}, {})",
                render_c_string_literal(&args[1])
            ))
        }
        "service_checkpoint_save_u32" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown service_checkpoint_save_u32 operand type for {}", args[0])
            })?;
            let value_ty = resolve_operand_type(&args[2], type_env).ok_or_else(|| {
                format!("unknown service_checkpoint_save_u32 operand type for {}", args[2])
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let value = render_operand(&args[2], env, Some(&value_ty), named_types)?;
            Ok(format!(
                "mira_service_checkpoint_save_u32_handle({handle}, {}, {value})",
                render_c_string_literal(&args[1])
            ))
        }
        "service_checkpoint_load_u32" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown service_checkpoint_load_u32 operand type for {}", args[0])
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!(
                "mira_service_checkpoint_load_u32_handle({handle}, {})",
                render_c_string_literal(&args[1])
            ))
        }
        "service_checkpoint_exists" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown service_checkpoint_exists operand type for {}", args[0])
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!(
                "mira_service_checkpoint_exists_handle({handle}, {})",
                render_c_string_literal(&args[1])
            ))
        }
        "service_migrate_db" => {
            let svc_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown service_migrate_db operand type for {}", args[0])
            })?;
            let db_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!("unknown service_migrate_db operand type for {}", args[1])
            })?;
            let service = render_operand(&args[0], env, Some(&svc_ty), named_types)?;
            let db = render_operand(&args[1], env, Some(&db_ty), named_types)?;
            Ok(format!(
                "mira_service_migrate_db_handle({service}, {db}, {})",
                render_c_string_literal(&args[2])
            ))
        }
        "service_route" => {
            let req_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown service_route operand type for {}", args[0]))?;
            let request = render_operand(&args[0], env, Some(&req_ty), named_types)?;
            Ok(format!(
                "mira_service_route_buf_u8({request}, {}, {})",
                render_c_string_literal(&args[1]),
                render_c_string_literal(&args[2])
            ))
        }
        "service_require_header" => {
            let req_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!(
                    "unknown service_require_header operand type for {}",
                    args[0]
                )
            })?;
            let request = render_operand(&args[0], env, Some(&req_ty), named_types)?;
            Ok(format!(
                "mira_service_require_header_buf_u8({request}, {}, {})",
                render_c_string_literal(&args[1]),
                render_c_string_literal(&args[2])
            ))
        }
        "service_error_status" => Ok(format!(
            "mira_service_error_status({})",
            render_c_string_literal(&args[0])
        )),
        "tls_exchange_all" => {
            let (host, port) = net_endpoint_for_function(function)?;
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown tls_exchange_all operand type for {}", args[0]))?;
            let value = render_operand(&args[0], env, Some(&value_ty), named_types)?;
            Ok(format!(
                "mira_tls_exchange_all_buf_u8({}, {}, {value})",
                render_c_string_literal(&host),
                port
            ))
        }
        "rt_open" => {
            let workers_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown rt_open operand type for {}", args[0]))?;
            let workers = render_operand(&args[0], env, Some(&workers_ty), named_types)?;
            Ok(format!("mira_rt_open_handle({workers})"))
        }
        "rt_spawn_u32" => {
            let runtime_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown rt_spawn_u32 runtime type for {}", args[0]))?;
            let arg_ty = resolve_operand_type(&args[2], type_env)
                .ok_or_else(|| format!("unknown rt_spawn_u32 arg type for {}", args[2]))?;
            let runtime = render_operand(&args[0], env, Some(&runtime_ty), named_types)?;
            let arg = render_operand(&args[2], env, Some(&arg_ty), named_types)?;
            Ok(format!(
                "mira_rt_spawn_u32_handle({runtime}, {}, {arg})",
                render_c_string_literal(&args[1])
            ))
        }
        "rt_try_spawn_u32" => {
            let runtime_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown rt_try_spawn_u32 runtime type for {}", args[0]))?;
            let arg_ty = resolve_operand_type(&args[2], type_env)
                .ok_or_else(|| format!("unknown rt_try_spawn_u32 arg type for {}", args[2]))?;
            let runtime = render_operand(&args[0], env, Some(&runtime_ty), named_types)?;
            let arg = render_operand(&args[2], env, Some(&arg_ty), named_types)?;
            Ok(format!(
                "mira_rt_try_spawn_u32_handle({runtime}, {}, {arg})",
                render_c_string_literal(&args[1])
            ))
        }
        "rt_spawn_buf" => {
            let runtime_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown rt_spawn_buf runtime type for {}", args[0]))?;
            let arg_ty = resolve_operand_type(&args[2], type_env)
                .ok_or_else(|| format!("unknown rt_spawn_buf arg type for {}", args[2]))?;
            let runtime = render_operand(&args[0], env, Some(&runtime_ty), named_types)?;
            let arg = render_operand(&args[2], env, Some(&arg_ty), named_types)?;
            Ok(format!(
                "mira_rt_spawn_buf_handle({runtime}, {}, {arg})",
                render_c_string_literal(&args[1])
            ))
        }
        "rt_try_spawn_buf" => {
            let runtime_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown rt_try_spawn_buf runtime type for {}", args[0]))?;
            let arg_ty = resolve_operand_type(&args[2], type_env)
                .ok_or_else(|| format!("unknown rt_try_spawn_buf arg type for {}", args[2]))?;
            let runtime = render_operand(&args[0], env, Some(&runtime_ty), named_types)?;
            let arg = render_operand(&args[2], env, Some(&arg_ty), named_types)?;
            Ok(format!(
                "mira_rt_try_spawn_buf_handle({runtime}, {}, {arg})",
                render_c_string_literal(&args[1])
            ))
        }
        "rt_done" => {
            let task_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown rt_done operand type for {}", args[0]))?;
            let task = render_operand(&args[0], env, Some(&task_ty), named_types)?;
            Ok(format!("mira_rt_done_handle({task})"))
        }
        "rt_join_u32" => {
            let task_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown rt_join_u32 operand type for {}", args[0]))?;
            let task = render_operand(&args[0], env, Some(&task_ty), named_types)?;
            Ok(format!("mira_rt_join_u32_handle({task})"))
        }
        "rt_join_buf" => {
            let task_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown rt_join_buf operand type for {}", args[0]))?;
            let task = render_operand(&args[0], env, Some(&task_ty), named_types)?;
            Ok(format!("mira_rt_join_buf_handle({task})"))
        }
        "rt_cancel" => {
            let task_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown rt_cancel operand type for {}", args[0]))?;
            let task = render_operand(&args[0], env, Some(&task_ty), named_types)?;
            Ok(format!("mira_rt_cancel_handle({task})"))
        }
        "rt_task_close" => {
            let task_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown rt_task_close operand type for {}", args[0]))?;
            let task = render_operand(&args[0], env, Some(&task_ty), named_types)?;
            Ok(format!("mira_rt_task_close_handle({task})"))
        }
        "rt_shutdown" => {
            let runtime_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown rt_shutdown runtime type for {}", args[0]))?;
            let grace_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown rt_shutdown grace type for {}", args[1]))?;
            let runtime = render_operand(&args[0], env, Some(&runtime_ty), named_types)?;
            let grace = render_operand(&args[1], env, Some(&grace_ty), named_types)?;
            Ok(format!("mira_rt_shutdown_handle({runtime}, {grace})"))
        }
        "rt_close" => {
            let runtime_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown rt_close operand type for {}", args[0]))?;
            let runtime = render_operand(&args[0], env, Some(&runtime_ty), named_types)?;
            Ok(format!("mira_rt_close_handle({runtime})"))
        }
        "rt_cancelled" => Ok("mira_rt_cancelled()".to_string()),
        "rt_inflight" => {
            let runtime_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown rt_inflight operand type for {}", args[0]))?;
            let runtime = render_operand(&args[0], env, Some(&runtime_ty), named_types)?;
            Ok(format!("mira_rt_inflight_handle({runtime})"))
        }
        "chan_open_u32" => {
            let capacity_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown chan_open_u32 operand type for {}", args[0]))?;
            let capacity = render_operand(&args[0], env, Some(&capacity_ty), named_types)?;
            Ok(format!("mira_chan_open_u32_handle({capacity})"))
        }
        "chan_open_buf" => {
            let capacity_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown chan_open_buf operand type for {}", args[0]))?;
            let capacity = render_operand(&args[0], env, Some(&capacity_ty), named_types)?;
            Ok(format!("mira_chan_open_buf_handle({capacity})"))
        }
        "chan_send_u32" => {
            let channel_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown chan_send_u32 channel type for {}", args[0]))?;
            let value_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown chan_send_u32 value type for {}", args[1]))?;
            let channel = render_operand(&args[0], env, Some(&channel_ty), named_types)?;
            let value = render_operand(&args[1], env, Some(&value_ty), named_types)?;
            Ok(format!("mira_chan_send_u32_handle({channel}, {value})"))
        }
        "chan_send_buf" => {
            let channel_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown chan_send_buf channel type for {}", args[0]))?;
            let value_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown chan_send_buf value type for {}", args[1]))?;
            let channel = render_operand(&args[0], env, Some(&channel_ty), named_types)?;
            let value = render_operand(&args[1], env, Some(&value_ty), named_types)?;
            Ok(format!("mira_chan_send_buf_handle({channel}, {value})"))
        }
        "chan_recv_u32" => {
            let channel_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown chan_recv_u32 channel type for {}", args[0]))?;
            let channel = render_operand(&args[0], env, Some(&channel_ty), named_types)?;
            Ok(format!("mira_chan_recv_u32_handle({channel})"))
        }
        "chan_recv_buf" => {
            let channel_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown chan_recv_buf channel type for {}", args[0]))?;
            let channel = render_operand(&args[0], env, Some(&channel_ty), named_types)?;
            Ok(format!("mira_chan_recv_buf_handle({channel})"))
        }
        "chan_len" => {
            let channel_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown chan_len channel type for {}", args[0]))?;
            let channel = render_operand(&args[0], env, Some(&channel_ty), named_types)?;
            Ok(format!("mira_chan_len_handle({channel})"))
        }
        "chan_close" => {
            let channel_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown chan_close channel type for {}", args[0]))?;
            let channel = render_operand(&args[0], env, Some(&channel_ty), named_types)?;
            Ok(format!("mira_chan_close_handle({channel})"))
        }
        "deadline_open_ms" => {
            let timeout_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown deadline_open_ms operand type for {}", args[0]))?;
            let timeout = render_operand(&args[0], env, Some(&timeout_ty), named_types)?;
            Ok(format!("mira_deadline_open_ms_handle({timeout})"))
        }
        "deadline_expired" => {
            let deadline_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown deadline_expired operand type for {}", args[0]))?;
            let deadline = render_operand(&args[0], env, Some(&deadline_ty), named_types)?;
            Ok(format!("mira_deadline_expired_handle({deadline})"))
        }
        "deadline_remaining_ms" => {
            let deadline_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown deadline_remaining_ms operand type for {}", args[0]))?;
            let deadline = render_operand(&args[0], env, Some(&deadline_ty), named_types)?;
            Ok(format!("mira_deadline_remaining_ms_handle({deadline})"))
        }
        "deadline_close" => {
            let deadline_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown deadline_close operand type for {}", args[0]))?;
            let deadline = render_operand(&args[0], env, Some(&deadline_ty), named_types)?;
            Ok(format!("mira_deadline_close_handle({deadline})"))
        }
        "cancel_scope_open" => Ok("mira_cancel_scope_open_handle()".to_string()),
        "cancel_scope_child" => {
            let scope_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown cancel_scope_child operand type for {}", args[0]))?;
            let scope = render_operand(&args[0], env, Some(&scope_ty), named_types)?;
            Ok(format!("mira_cancel_scope_child_handle({scope})"))
        }
        "cancel_scope_bind_task" => {
            let scope_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown cancel_scope_bind_task operand type for {}", args[0]))?;
            let task_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown cancel_scope_bind_task operand type for {}", args[1]))?;
            let scope = render_operand(&args[0], env, Some(&scope_ty), named_types)?;
            let task = render_operand(&args[1], env, Some(&task_ty), named_types)?;
            Ok(format!("mira_cancel_scope_bind_task_handle({scope}, {task})"))
        }
        "cancel_scope_cancel" => {
            let scope_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown cancel_scope_cancel operand type for {}", args[0]))?;
            let scope = render_operand(&args[0], env, Some(&scope_ty), named_types)?;
            Ok(format!("mira_cancel_scope_cancel_handle({scope})"))
        }
        "cancel_scope_cancelled" => {
            let scope_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown cancel_scope_cancelled operand type for {}", args[0]))?;
            let scope = render_operand(&args[0], env, Some(&scope_ty), named_types)?;
            Ok(format!("mira_cancel_scope_cancelled_handle({scope})"))
        }
        "cancel_scope_close" => {
            let scope_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown cancel_scope_close operand type for {}", args[0]))?;
            let scope = render_operand(&args[0], env, Some(&scope_ty), named_types)?;
            Ok(format!("mira_cancel_scope_close_handle({scope})"))
        }
        "retry_open" => {
            let max_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown retry_open operand type for {}", args[0]))?;
            let backoff_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown retry_open operand type for {}", args[1]))?;
            let max_attempts = render_operand(&args[0], env, Some(&max_ty), named_types)?;
            let backoff = render_operand(&args[1], env, Some(&backoff_ty), named_types)?;
            Ok(format!("mira_retry_open_handle({max_attempts}, {backoff})"))
        }
        "retry_record_failure" => {
            let retry_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown retry_record_failure operand type for {}", args[0]))?;
            let retry = render_operand(&args[0], env, Some(&retry_ty), named_types)?;
            Ok(format!("mira_retry_record_failure_handle({retry})"))
        }
        "retry_record_success" => {
            let retry_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown retry_record_success operand type for {}", args[0]))?;
            let retry = render_operand(&args[0], env, Some(&retry_ty), named_types)?;
            Ok(format!("mira_retry_record_success_handle({retry})"))
        }
        "retry_next_delay_ms" => {
            let retry_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown retry_next_delay_ms operand type for {}", args[0]))?;
            let retry = render_operand(&args[0], env, Some(&retry_ty), named_types)?;
            Ok(format!("mira_retry_next_delay_ms_handle({retry})"))
        }
        "retry_exhausted" => {
            let retry_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown retry_exhausted operand type for {}", args[0]))?;
            let retry = render_operand(&args[0], env, Some(&retry_ty), named_types)?;
            Ok(format!("mira_retry_exhausted_handle({retry})"))
        }
        "retry_close" => {
            let retry_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown retry_close operand type for {}", args[0]))?;
            let retry = render_operand(&args[0], env, Some(&retry_ty), named_types)?;
            Ok(format!("mira_retry_close_handle({retry})"))
        }
        "circuit_open" => {
            let threshold_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown circuit_open operand type for {}", args[0]))?;
            let cooldown_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown circuit_open operand type for {}", args[1]))?;
            let threshold = render_operand(&args[0], env, Some(&threshold_ty), named_types)?;
            let cooldown = render_operand(&args[1], env, Some(&cooldown_ty), named_types)?;
            Ok(format!("mira_circuit_open_handle({threshold}, {cooldown})"))
        }
        "circuit_allow" => {
            let circuit_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown circuit_allow operand type for {}", args[0]))?;
            let circuit = render_operand(&args[0], env, Some(&circuit_ty), named_types)?;
            Ok(format!("mira_circuit_allow_handle({circuit})"))
        }
        "circuit_record_failure" => {
            let circuit_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown circuit_record_failure operand type for {}", args[0]))?;
            let circuit = render_operand(&args[0], env, Some(&circuit_ty), named_types)?;
            Ok(format!("mira_circuit_record_failure_handle({circuit})"))
        }
        "circuit_record_success" => {
            let circuit_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown circuit_record_success operand type for {}", args[0]))?;
            let circuit = render_operand(&args[0], env, Some(&circuit_ty), named_types)?;
            Ok(format!("mira_circuit_record_success_handle({circuit})"))
        }
        "circuit_state" => {
            let circuit_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown circuit_state operand type for {}", args[0]))?;
            let circuit = render_operand(&args[0], env, Some(&circuit_ty), named_types)?;
            Ok(format!("mira_circuit_state_handle({circuit})"))
        }
        "circuit_close" => {
            let circuit_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown circuit_close operand type for {}", args[0]))?;
            let circuit = render_operand(&args[0], env, Some(&circuit_ty), named_types)?;
            Ok(format!("mira_circuit_close_handle({circuit})"))
        }
        "backpressure_open" => {
            let limit_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown backpressure_open operand type for {}", args[0]))?;
            let limit = render_operand(&args[0], env, Some(&limit_ty), named_types)?;
            Ok(format!("mira_backpressure_open_handle({limit})"))
        }
        "backpressure_acquire" => {
            let backpressure_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown backpressure_acquire operand type for {}", args[0]))?;
            let backpressure = render_operand(&args[0], env, Some(&backpressure_ty), named_types)?;
            Ok(format!("mira_backpressure_acquire_handle({backpressure})"))
        }
        "backpressure_release" => {
            let backpressure_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown backpressure_release operand type for {}", args[0]))?;
            let backpressure = render_operand(&args[0], env, Some(&backpressure_ty), named_types)?;
            Ok(format!("mira_backpressure_release_handle({backpressure})"))
        }
        "backpressure_saturated" => {
            let backpressure_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown backpressure_saturated operand type for {}", args[0]))?;
            let backpressure = render_operand(&args[0], env, Some(&backpressure_ty), named_types)?;
            Ok(format!("mira_backpressure_saturated_handle({backpressure})"))
        }
        "backpressure_close" => {
            let backpressure_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown backpressure_close operand type for {}", args[0]))?;
            let backpressure = render_operand(&args[0], env, Some(&backpressure_ty), named_types)?;
            Ok(format!("mira_backpressure_close_handle({backpressure})"))
        }
        "supervisor_open" => {
            let restart_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown supervisor_open operand type for {}", args[0]))?;
            let degrade_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown supervisor_open operand type for {}", args[1]))?;
            let restart = render_operand(&args[0], env, Some(&restart_ty), named_types)?;
            let degrade = render_operand(&args[1], env, Some(&degrade_ty), named_types)?;
            Ok(format!("mira_supervisor_open_handle({restart}, {degrade})"))
        }
        "supervisor_record_failure" => {
            let supervisor_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown supervisor_record_failure operand type for {}", args[0]))?;
            let code_ty = resolve_operand_type(&args[1], type_env)
                .ok_or_else(|| format!("unknown supervisor_record_failure operand type for {}", args[1]))?;
            let supervisor = render_operand(&args[0], env, Some(&supervisor_ty), named_types)?;
            let code = render_operand(&args[1], env, Some(&code_ty), named_types)?;
            Ok(format!("mira_supervisor_record_failure_handle({supervisor}, {code})"))
        }
        "supervisor_record_recovery" => {
            let supervisor_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown supervisor_record_recovery operand type for {}", args[0]))?;
            let supervisor = render_operand(&args[0], env, Some(&supervisor_ty), named_types)?;
            Ok(format!("mira_supervisor_record_recovery_handle({supervisor})"))
        }
        "supervisor_should_restart" => {
            let supervisor_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown supervisor_should_restart operand type for {}", args[0]))?;
            let supervisor = render_operand(&args[0], env, Some(&supervisor_ty), named_types)?;
            Ok(format!("mira_supervisor_should_restart_handle({supervisor})"))
        }
        "supervisor_degraded" => {
            let supervisor_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown supervisor_degraded operand type for {}", args[0]))?;
            let supervisor = render_operand(&args[0], env, Some(&supervisor_ty), named_types)?;
            Ok(format!("mira_supervisor_degraded_handle({supervisor})"))
        }
        "supervisor_close" => {
            let supervisor_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown supervisor_close operand type for {}", args[0]))?;
            let supervisor = render_operand(&args[0], env, Some(&supervisor_ty), named_types)?;
            Ok(format!("mira_supervisor_close_handle({supervisor})"))
        }
        "task_sleep_ms" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown task_sleep_ms operand type for {}", args[0]))?;
            let value = render_operand(&args[0], env, Some(&value_ty), named_types)?;
            Ok(format!("mira_task_sleep_ms({value})"))
        }
        "task_open" => {
            let (command, argv, env_vars) = parse_spawn_invocation(&instruction.args)?;
            Ok(format!(
                "mira_task_open_handle({})",
                render_c_string_literal(&render_spawn_shell_command(
                    &command, &argv, &env_vars, false
                )?)
            ))
        }
        "task_done" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown task_done operand type for {}", args[0]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_task_done_handle({handle})"))
        }
        "task_join" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown task_join operand type for {}", args[0]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_task_join_handle({handle})"))
        }
        "task_stdout_all" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown task_stdout_all operand type for {}", args[0]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_task_stdout_all_handle_buf_u8({handle})"))
        }
        "task_stderr_all" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown task_stderr_all operand type for {}", args[0]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_task_stderr_all_handle_buf_u8({handle})"))
        }
        "task_close" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown task_close operand type for {}", args[0]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_task_close_handle({handle})"))
        }
        "spawn_capture_all" => {
            let (command, argv, env_vars) = parse_spawn_invocation(&instruction.args)?;
            Ok(format!(
                "mira_spawn_capture_buf_u8({})",
                render_c_string_literal(&render_spawn_shell_command(
                    &command, &argv, &env_vars, false
                )?)
            ))
        }
        "spawn_capture_stderr_all" => {
            let (command, argv, env_vars) = parse_spawn_invocation(&instruction.args)?;
            Ok(format!(
                "mira_spawn_capture_buf_u8({})",
                render_c_string_literal(&render_spawn_shell_command(
                    &command, &argv, &env_vars, true
                )?)
            ))
        }
        "spawn_call" => {
            let (command, argv, env_vars) = parse_spawn_invocation(&instruction.args)?;
            Ok(format!(
                "mira_spawn_status({})",
                render_c_string_literal(&render_spawn_shell_command(
                    &command, &argv, &env_vars, false
                )?)
            ))
        }
        "spawn_open" => {
            let (command, argv, env_vars) = parse_spawn_invocation(&instruction.args)?;
            Ok(format!(
                "mira_spawn_open_handle({})",
                render_c_string_literal(&render_spawn_shell_command(
                    &command, &argv, &env_vars, false
                )?)
            ))
        }
        "spawn_wait" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown spawn_wait operand type for {}", args[0]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_spawn_wait_handle({handle})"))
        }
        "spawn_stdout_all" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown spawn_stdout_all operand type for {}", args[0]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_spawn_stdout_all_handle_buf_u8({handle})"))
        }
        "spawn_stderr_all" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown spawn_stderr_all operand type for {}", args[0]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_spawn_stderr_all_handle_buf_u8({handle})"))
        }
        "spawn_stdin_write_all" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown spawn_stdin_write_all operand type for {}", args[0])
            })?;
            let value_ty = resolve_operand_type(&args[1], type_env).ok_or_else(|| {
                format!("unknown spawn_stdin_write_all operand type for {}", args[1])
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            let value = render_operand(&args[1], env, Some(&value_ty), named_types)?;
            Ok(format!("mira_spawn_stdin_write_all_handle({handle}, {value})"))
        }
        "spawn_stdin_close" => {
            let handle_ty = resolve_operand_type(&args[0], type_env).ok_or_else(|| {
                format!("unknown spawn_stdin_close operand type for {}", args[0])
            })?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_spawn_stdin_close_handle({handle})"))
        }
        "spawn_done" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown spawn_done operand type for {}", args[0]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_spawn_done_handle({handle})"))
        }
        "spawn_exit_ok" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown spawn_exit_ok operand type for {}", args[0]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_spawn_exit_ok_handle({handle})"))
        }
        "spawn_kill" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown spawn_kill operand type for {}", args[0]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_spawn_kill_handle({handle})"))
        }
        "spawn_close" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown spawn_close operand type for {}", args[0]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_spawn_close_handle({handle})"))
        }
        "net_connect" => {
            let (host, port) = net_endpoint_for_function(function)?;
            Ok(format!(
                "mira_net_connect_ok({}, {})",
                render_c_string_literal(&host),
                port
            ))
        }
        "ffi_call" => render_ffi_call_expr(instruction, env, type_env, named_types),
        "ffi_call_cstr" => render_ffi_call_cstr_expr(instruction, env, type_env, named_types),
        "ffi_open_lib" => Ok(format!(
            "mira_ffi_open_lib_handle({})",
            render_c_string_literal(&args[0])
        )),
        "ffi_close_lib" => {
            let handle_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown ffi_close_lib operand type for {}", args[0]))?;
            let handle = render_operand(&args[0], env, Some(&handle_ty), named_types)?;
            Ok(format!("mira_ffi_close_lib_handle({handle})"))
        }
        "ffi_buf_ptr" => {
            let value_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown ffi_buf_ptr operand type for {}", args[0]))?;
            let value = render_operand(&args[0], env, Some(&value_ty), named_types)?;
            Ok(format!("mira_ffi_buf_ptr_buf_u8({value})"))
        }
        "ffi_call_lib" => render_ffi_call_lib_expr(instruction, env, type_env, named_types),
        "ffi_call_lib_cstr" => {
            render_ffi_call_lib_cstr_expr(instruction, env, type_env, named_types)
        }
        "view" | "edit" => render_operand(&args[0], env, Some(&instruction.ty), named_types),
        "store" => render_store_expr(instruction, env, type_env, named_types),
        "len" => {
            let collection = render_operand(&args[0], env, None, named_types)?;
            let collection_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown collection type for {}", args[0]))?;
            render_len_expr(&collection, &collection_ty)
        }
        "load" => {
            let collection = render_operand(&args[0], env, None, named_types)?;
            let index = render_operand(&args[1], env, None, named_types)?;
            let collection_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown collection type for {}", args[0]))?;
            render_load_expr(&collection, &index, &collection_ty)
        }
        "abs" => {
            let value = render_operand(&args[0], env, Some(&instruction.ty), named_types)?;
            Ok(format!("(({}) < 0 ? -({}) : ({}))", value, value, value))
        }
        "add" => binary_expr("+", args, env, named_types),
        "sub" => binary_expr("-", args, env, named_types),
        "mul" => binary_expr("*", args, env, named_types),
        "band" => binary_expr("&", args, env, named_types),
        "bor" => binary_expr("|", args, env, named_types),
        "bxor" => binary_expr("^", args, env, named_types),
        "shl" => binary_expr("<<", args, env, named_types),
        "shr" => binary_expr(">>", args, env, named_types),
        "lt" => binary_expr("<", args, env, named_types),
        "le" => binary_expr("<=", args, env, named_types),
        "eq" => render_eq_expr(args, env, type_env, named_types),
        "make" => render_make_expr(instruction, env, named_types),
        "field" => {
            let aggregate = render_operand(&args[0], env, None, named_types)?;
            let aggregate_ty = resolve_operand_type(&args[0], type_env)
                .ok_or_else(|| format!("unknown aggregate type for {}", args[0]))?;
            render_field_expr(&aggregate, &aggregate_ty, &args[1], named_types)
        }
        "sat_add" => {
            let left = render_operand(&args[0], env, Some(&instruction.ty), named_types)?;
            let right = render_operand(&args[1], env, Some(&instruction.ty), named_types)?;
            Ok(format!(
                "mira_sat_add_{}({}, {})",
                instruction.ty.type_key()?,
                left,
                right
            ))
        }
        "bnot" => {
            let value = render_operand(&args[0], env, Some(&instruction.ty), named_types)?;
            Ok(format!("(~({}))", value))
        }
        "sext" => {
            let target = TypeRef::parse(&args[0])?;
            let value = render_operand(&args[1], env, Some(&target), named_types)?;
            Ok(format!("(({}) {})", target.c_type()?, value))
        }
        _ => Err(format!("unsupported op {}", instruction.op)),
    }
}

fn binary_expr(
    op: &str,
    args: &[String],
    env: &HashMap<String, String>,
    named_types: &HashMap<String, TypeDeclBody>,
) -> Result<String, String> {
    let left = render_operand(&args[0], env, None, named_types)?;
    let right = render_operand(&args[1], env, None, named_types)?;
    Ok(format!("(({}) {} ({}))", left, op, right))
}

fn render_json_encode_obj_expr(
    args: &[String],
    env: &HashMap<String, String>,
    type_env: &HashMap<String, TypeRef>,
    named_types: &HashMap<String, TypeDeclBody>,
) -> Result<String, String> {
    let mut expr = format!("mira_buf_lit_u8({})", render_c_string_literal("{"));
    let mut first = true;
    for pair in args.chunks(2) {
        let value_ty = resolve_operand_type(&pair[1], type_env)
            .ok_or_else(|| format!("unknown json_encode_obj operand type for {}", pair[1]))?;
        let value = render_operand(&pair[1], env, Some(&value_ty), named_types)?;
        if !first {
            expr = format!(
                "mira_buf_concat_u8({}, mira_buf_lit_u8({}))",
                expr,
                render_c_string_literal(",")
            );
        }
        first = false;
        let key_prefix = format!("\"{}\":", pair[0]);
        expr = format!(
            "mira_buf_concat_u8({}, mira_buf_lit_u8({}))",
            expr,
            render_c_string_literal(&key_prefix)
        );
        let value_expr = if matches!(
            value_ty,
            TypeRef::Int {
                signed: false,
                bits: 32
            }
        ) {
            format!("mira_str_from_u32({value})")
        } else if matches!(value_ty, TypeRef::Bool) {
            format!("mira_str_from_bool({value})")
        } else {
            format!("mira_json_quote_str_u8({value})")
        };
        expr = format!("mira_buf_concat_u8({}, {})", expr, value_expr);
    }
    Ok(format!(
        "mira_buf_concat_u8({}, mira_buf_lit_u8({}))",
        expr,
        render_c_string_literal("}")
    ))
}

fn render_json_encode_arr_expr(
    args: &[String],
    env: &HashMap<String, String>,
    type_env: &HashMap<String, TypeRef>,
    named_types: &HashMap<String, TypeDeclBody>,
) -> Result<String, String> {
    let mut expr = format!("mira_buf_lit_u8({})", render_c_string_literal("["));
    let mut first = true;
    for operand in args {
        let value_ty = resolve_operand_type(operand, type_env)
            .ok_or_else(|| format!("unknown json_encode_arr operand type for {operand}"))?;
        let value = render_operand(operand, env, Some(&value_ty), named_types)?;
        if !first {
            expr = format!(
                "mira_buf_concat_u8({}, mira_buf_lit_u8({}))",
                expr,
                render_c_string_literal(",")
            );
        }
        first = false;
        let value_expr = if matches!(
            value_ty,
            TypeRef::Int {
                signed: false,
                bits: 32
            }
        ) {
            format!("mira_str_from_u32({value})")
        } else if matches!(value_ty, TypeRef::Bool) {
            format!("mira_str_from_bool({value})")
        } else {
            format!("mira_json_quote_str_u8({value})")
        };
        expr = format!("mira_buf_concat_u8({}, {})", expr, value_expr);
    }
    Ok(format!(
        "mira_buf_concat_u8({}, mira_buf_lit_u8({}))",
        expr,
        render_c_string_literal("]")
    ))
}

fn parse_bool_text(text: &str) -> Option<bool> {
    match text.trim() {
        "true" | "1" | "yes" | "on" => Some(true),
        "false" | "0" | "no" | "off" => Some(false),
        _ => None,
    }
}

fn render_operand(
    token: &str,
    env: &HashMap<String, String>,
    expected: Option<&TypeRef>,
    named_types: &HashMap<String, TypeDeclBody>,
) -> Result<String, String> {
    if let Some(mapped) = env.get(token) {
        return Ok(mapped.clone());
    }
    render_c_literal_with_named_types(token, expected, named_types)
}

fn render_c_literal_with_named_types(
    token: &str,
    expected: Option<&TypeRef>,
    named_types: &HashMap<String, TypeDeclBody>,
) -> Result<String, String> {
    if let Some(expected) = expected {
        if let Ok(value) = parse_data_literal(token, expected, Some(named_types)) {
            return render_data_value_with_named_types(&value, expected, named_types);
        }
    }
    if let Some(TypeRef::Named(type_name)) = expected {
        return render_named_literal(token, type_name, named_types);
    }
    render_c_literal(token, expected)
}

fn render_named_literal(
    token: &str,
    type_name: &str,
    named_types: &HashMap<String, TypeDeclBody>,
) -> Result<String, String> {
    let Some(TypeDeclBody::Enum { variants }) = named_types.get(type_name) else {
        return Err(format!(
            "named literal rendering requires enum type {type_name}"
        ));
    };
    let prefix = format!("{type_name}.");
    let variant_name = token
        .strip_prefix(&prefix)
        .ok_or_else(|| format!("expected enum literal {type_name}.variant, got {token}"))?;
    let variant = variants
        .iter()
        .find(|variant| variant.name == variant_name)
        .ok_or_else(|| format!("unknown variant {variant_name} on {type_name}"))?;
    if !variant.fields.is_empty() {
        return Err(format!("variant {type_name}.{variant_name} carries payload and cannot be used as a bare literal"));
    }
    if enum_has_payload(variants) {
        Ok(format!(
            "(({}){{ .tag = {}, .payload = {{0}} }})",
            TypeRef::Named(type_name.to_string()).c_type()?,
            enum_tag_constant(type_name, variant_name)
        ))
    } else {
        Ok(enum_tag_constant(type_name, variant_name))
    }
}

fn render_data_value_with_named_types(
    value: &DataValue,
    ty: &TypeRef,
    named_types: &HashMap<String, TypeDeclBody>,
) -> Result<String, String> {
    match (value, ty) {
        (DataValue::Symbol(value), TypeRef::Named(name)) => {
            render_named_literal(value, name, named_types)
        }
        (DataValue::Fields(values), TypeRef::Named(name)) => match named_types.get(name) {
            Some(TypeDeclBody::Struct { fields }) => Ok(format!(
                "(({}){{ {} }})",
                ty.c_type()?,
                render_named_field_initializers(values, fields, named_types)?
            )),
            Some(TypeDeclBody::Enum { .. }) => Err(format!(
                "struct-style literal cannot initialize enum type {name}"
            )),
            None => Err(format!("unknown named type {name}")),
        },
        (
            DataValue::Variant {
                name: variant_name,
                fields,
            },
            TypeRef::Named(type_name),
        ) => render_named_variant_value(type_name, variant_name, fields, ty, named_types),
        _ => render_data_value(value, ty),
    }
}

fn render_named_variant_value(
    type_name: &str,
    variant_name: &str,
    values: &[NamedFieldValue],
    ty: &TypeRef,
    named_types: &HashMap<String, TypeDeclBody>,
) -> Result<String, String> {
    let Some(TypeDeclBody::Enum { variants }) = named_types.get(type_name) else {
        return Err(format!("unknown enum type {type_name}"));
    };
    let variant = variants
        .iter()
        .find(|variant| variant.name == variant_name)
        .ok_or_else(|| format!("unknown variant {variant_name} on {type_name}"))?;
    if variant.fields.is_empty() {
        return Err(format!(
            "variant {type_name}.{variant_name} does not carry payload"
        ));
    }
    let rendered_fields = render_named_field_initializers(values, &variant.fields, named_types)?;
    Ok(format!(
        "(({}){{ .tag = {}, .payload.{} = {{ {} }} }})",
        ty.c_type()?,
        enum_tag_constant(type_name, variant_name),
        sanitize_identifier(variant_name),
        rendered_fields
    ))
}

fn render_named_field_initializers(
    values: &[NamedFieldValue],
    fields: &[crate::ast::Field],
    named_types: &HashMap<String, TypeDeclBody>,
) -> Result<String, String> {
    if values.len() != fields.len() {
        return Err(format!(
            "expected {} named values, got {}",
            fields.len(),
            values.len()
        ));
    }
    values
        .iter()
        .zip(fields.iter())
        .map(|(value, field)| {
            if value.name != field.name {
                return Err(format!(
                    "expected field {} in canonical order, got {}",
                    field.name, value.name
                ));
            }
            Ok(format!(
                ".{} = {}",
                field.name,
                render_data_value_with_named_types(&value.value, &field.ty, named_types)?
            ))
        })
        .collect::<Result<Vec<_>, String>>()
        .map(|parts| parts.join(", "))
}

fn render_match_value(
    token: &str,
    env: &HashMap<String, String>,
    type_env: &HashMap<String, TypeRef>,
    named_types: &HashMap<String, TypeDeclBody>,
) -> Result<String, String> {
    let rendered = render_operand(token, env, None, named_types)?;
    match resolve_operand_type(token, type_env) {
        Some(TypeRef::Named(name)) => match named_types.get(&name) {
            Some(TypeDeclBody::Enum { variants }) if enum_has_payload(variants) => {
                Ok(format!("({}).tag", rendered))
            }
            _ => Ok(rendered),
        },
        _ => Ok(rendered),
    }
}

fn render_eq_expr(
    args: &[String],
    env: &HashMap<String, String>,
    type_env: &HashMap<String, TypeRef>,
    named_types: &HashMap<String, TypeDeclBody>,
) -> Result<String, String> {
    let left = render_operand(&args[0], env, None, named_types)?;
    let right = render_operand(&args[1], env, None, named_types)?;
    let ty = resolve_operand_type(&args[0], type_env)
        .ok_or_else(|| format!("unable to resolve operand type for {}", args[0]))?;
    render_type_equality_expr(&left, &right, &ty, named_types)
}

fn render_type_equality_expr(
    left: &str,
    right: &str,
    ty: &TypeRef,
    named_types: &HashMap<String, TypeDeclBody>,
) -> Result<String, String> {
    match ty {
        TypeRef::Int { .. } | TypeRef::Float { .. } | TypeRef::Bool => {
            Ok(format!("(({}) == ({}))", left, right))
        }
        TypeRef::Named(name) => match named_types.get(name) {
            Some(TypeDeclBody::Struct { fields }) => {
                if fields.is_empty() {
                    return Ok("true".to_string());
                }
                fields
                    .iter()
                    .map(|field| {
                        render_type_equality_expr(
                            &format!("({left}).{}", field.name),
                            &format!("({right}).{}", field.name),
                            &field.ty,
                            named_types,
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .map(|parts| format!("({})", parts.join(" && ")))
            }
            Some(TypeDeclBody::Enum { variants }) if enum_has_payload(variants) => {
                let tag_expr = format!("(({}).tag == ({}).tag)", left, right);
                let mut payload_expr = "true".to_string();
                for variant in variants.iter().rev() {
                    let variant_tag = enum_tag_constant(name, &variant.name);
                    let branch_expr = if variant.fields.is_empty() {
                        "true".to_string()
                    } else {
                        variant
                            .fields
                            .iter()
                            .map(|field| {
                                render_type_equality_expr(
                                    &format!(
                                        "({}).payload.{}.{}",
                                        left,
                                        sanitize_identifier(&variant.name),
                                        field.name
                                    ),
                                    &format!(
                                        "({}).payload.{}.{}",
                                        right,
                                        sanitize_identifier(&variant.name),
                                        field.name
                                    ),
                                    &field.ty,
                                    named_types,
                                )
                            })
                            .collect::<Result<Vec<_>, _>>()
                            .map(|parts| {
                                if parts.is_empty() {
                                    "true".to_string()
                                } else {
                                    format!("({})", parts.join(" && "))
                                }
                            })?
                    };
                    payload_expr = format!(
                        "((({}).tag == {}) ? ({}) : ({}))",
                        left, variant_tag, branch_expr, payload_expr
                    );
                }
                Ok(format!("({} && {})", tag_expr, payload_expr))
            }
            Some(TypeDeclBody::Enum { .. }) => Ok(format!("(({}) == ({}))", left, right)),
            None => Err(format!("unknown named type {name}")),
        },
        _ => Err(format!("eq is not implemented for {ty}")),
    }
}

fn enum_has_payload(variants: &[crate::ast::EnumVariant]) -> bool {
    variants.iter().any(|variant| !variant.fields.is_empty())
}

fn enum_tag_type(type_name: &str) -> String {
    format!("mira_tag_{}", sanitize_identifier(type_name))
}

fn enum_tag_constant(type_name: &str, variant_name: &str) -> String {
    format!(
        "mira_enum_{}_{}",
        sanitize_identifier(type_name),
        sanitize_identifier(variant_name)
    )
}

fn render_field_expr(
    aggregate: &str,
    aggregate_ty: &TypeRef,
    field_path: &str,
    named_types: &HashMap<String, TypeDeclBody>,
) -> Result<String, String> {
    match aggregate_ty {
        TypeRef::Named(name) => match named_types.get(name) {
            Some(TypeDeclBody::Struct { .. }) => Ok(format!("(({}).{})", aggregate, field_path)),
            Some(TypeDeclBody::Enum { .. }) => {
                let (variant_name, field_name) = field_path.split_once('.').ok_or_else(|| {
                    format!("enum field access requires variant.field, got {field_path}")
                })?;
                Ok(format!(
                    "mira_field_{}_{}_{}({})",
                    sanitize_identifier(name),
                    sanitize_identifier(variant_name),
                    sanitize_identifier(field_name),
                    aggregate
                ))
            }
            None => Err(format!("unknown named type {name}")),
        },
        TypeRef::Own(inner) | TypeRef::View(inner) | TypeRef::Edit(inner) => {
            render_field_expr(aggregate, inner.as_ref(), field_path, named_types)
        }
        _ => Err(format!(
            "field expects a named aggregate operand, got {aggregate_ty}"
        )),
    }
}

fn render_alloc_expr(
    instruction: &crate::ast::Instruction,
    env: &HashMap<String, String>,
    _type_env: &HashMap<String, TypeRef>,
    named_types: &HashMap<String, TypeDeclBody>,
) -> Result<String, String> {
    let TypeRef::Own(inner) = &instruction.ty else {
        return Err("alloc requires own[buf[T]] result type".to_string());
    };
    let TypeRef::Buf(_) = inner.as_ref() else {
        return Err("alloc requires own[buf[T]] result type".to_string());
    };
    let region = instruction
        .args
        .first()
        .ok_or_else(|| "alloc requires region".to_string())?;
    let len = render_operand(
        &instruction.args[1],
        env,
        Some(&TypeRef::Int {
            signed: false,
            bits: 32,
        }),
        named_types,
    )?;
    let helper = match region.as_str() {
        "heap" => "mira_alloc_heap",
        "stack" => "mira_alloc_stack",
        "arena" => "mira_alloc_arena",
        other => return Err(format!("unsupported alloc region {other}")),
    };
    if region == "arena" {
        Ok(format!(
            "{}_{}(&mira_arena, {})",
            helper,
            runtime_buf_key(inner)?,
            len
        ))
    } else {
        Ok(format!("{}_{}({})", helper, runtime_buf_key(inner)?, len))
    }
}

fn render_store_expr(
    instruction: &crate::ast::Instruction,
    env: &HashMap<String, String>,
    type_env: &HashMap<String, TypeRef>,
    named_types: &HashMap<String, TypeDeclBody>,
) -> Result<String, String> {
    let handle = render_operand(
        &instruction.args[0],
        env,
        Some(&instruction.ty),
        named_types,
    )?;
    let index = render_operand(
        &instruction.args[1],
        env,
        Some(&TypeRef::Int {
            signed: false,
            bits: 32,
        }),
        named_types,
    )?;
    let handle_ty = resolve_operand_type(&instruction.args[0], type_env)
        .ok_or_else(|| format!("unknown store handle type for {}", instruction.args[0]))?;
    let elem_ty = runtime_buf_elem_type(&handle_ty)
        .ok_or_else(|| format!("store expects edit[buf[T]], got {handle_ty}"))?;
    let value = render_operand(&instruction.args[2], env, Some(&elem_ty), named_types)?;
    Ok(format!(
        "mira_store_{}({}, {}, {})",
        runtime_buf_key_from_wrapper(&handle_ty)?,
        handle,
        index,
        value
    ))
}

fn render_drop_expr(
    instruction: &crate::ast::Instruction,
    env: &HashMap<String, String>,
    type_env: &HashMap<String, TypeRef>,
    named_types: &HashMap<String, TypeDeclBody>,
) -> Result<String, String> {
    let handle_ty = resolve_operand_type(&instruction.args[0], type_env)
        .ok_or_else(|| format!("unknown drop handle type for {}", instruction.args[0]))?;
    let handle = render_operand(&instruction.args[0], env, Some(&handle_ty), named_types)?;
    if let Ok(buf_key) = runtime_buf_key_from_wrapper(&handle_ty) {
        Ok(format!("mira_drop_{}({})", buf_key, handle))
    } else {
        Ok(format!("((void)({}), true)", handle))
    }
}

fn render_ffi_call_expr(
    instruction: &crate::ast::Instruction,
    env: &HashMap<String, String>,
    type_env: &HashMap<String, TypeRef>,
    named_types: &HashMap<String, TypeDeclBody>,
) -> Result<String, String> {
    let symbol = instruction
        .args
        .first()
        .ok_or_else(|| "ffi_call requires a symbol".to_string())?;
    if !is_valid_ffi_symbol(symbol) {
        return Err(format!("invalid ffi symbol {symbol}"));
    }
    let mut rendered_args = Vec::new();
    for operand in instruction.args.iter().skip(1) {
        let operand_ty = resolve_operand_type(operand, type_env)
            .ok_or_else(|| format!("unknown ffi operand type for {operand}"))?;
        rendered_args.push(render_operand(
            operand,
            env,
            Some(&operand_ty),
            named_types,
        )?);
    }
    Ok(format!("{}({})", symbol, rendered_args.join(", ")))
}

fn render_ffi_call_cstr_expr(
    instruction: &crate::ast::Instruction,
    env: &HashMap<String, String>,
    type_env: &HashMap<String, TypeRef>,
    named_types: &HashMap<String, TypeDeclBody>,
) -> Result<String, String> {
    let symbol = instruction
        .args
        .first()
        .ok_or_else(|| "ffi_call_cstr requires a symbol".to_string())?;
    if !is_valid_ffi_symbol(symbol) {
        return Err(format!("invalid ffi symbol {symbol}"));
    }
    let operand = instruction
        .args
        .get(1)
        .ok_or_else(|| "ffi_call_cstr requires one buf[u8] operand".to_string())?;
    let operand_ty = resolve_operand_type(operand, type_env)
        .ok_or_else(|| format!("unknown ffi_call_cstr operand type for {operand}"))?;
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
        return Err(format!(
            "ffi_call_cstr requires own/view/edit[buf[u8]], got {operand_ty}"
        ));
    }
    let rendered = render_operand(operand, env, Some(&operand_ty), named_types)?;
    Ok(format!("{}((const char*) {}.data)", symbol, rendered))
}

fn render_ffi_call_lib_expr(
    instruction: &crate::ast::Instruction,
    env: &HashMap<String, String>,
    type_env: &HashMap<String, TypeRef>,
    named_types: &HashMap<String, TypeDeclBody>,
) -> Result<String, String> {
    let handle_ty = resolve_operand_type(&instruction.args[0], type_env).ok_or_else(|| {
        format!(
            "unknown ffi_call_lib handle type for {}",
            instruction.args[0]
        )
    })?;
    let handle = render_operand(&instruction.args[0], env, Some(&handle_ty), named_types)?;
    let symbol = instruction
        .args
        .get(1)
        .ok_or_else(|| "ffi_call_lib requires a symbol".to_string())?;
    let mut rendered_args = Vec::new();
    for operand in instruction.args.iter().skip(2) {
        let operand_ty = resolve_operand_type(operand, type_env)
            .ok_or_else(|| format!("unknown ffi_call_lib operand type for {operand}"))?;
        rendered_args.push(format!(
            "(uint64_t)({})",
            render_operand(operand, env, Some(&operand_ty), named_types)?
        ));
    }
    let arg_array = if rendered_args.is_empty() {
        "NULL".to_string()
    } else {
        format!("(uint64_t[]){{{}}}", rendered_args.join(", "))
    };
    Ok(format!(
        "(({}) mira_ffi_call_lib_u64({}, {}, {}, {}))",
        instruction.ty.c_type()?,
        handle,
        render_c_string_literal(symbol),
        rendered_args.len(),
        arg_array
    ))
}

fn render_ffi_call_lib_cstr_expr(
    instruction: &crate::ast::Instruction,
    env: &HashMap<String, String>,
    type_env: &HashMap<String, TypeRef>,
    named_types: &HashMap<String, TypeDeclBody>,
) -> Result<String, String> {
    let handle_ty = resolve_operand_type(&instruction.args[0], type_env).ok_or_else(|| {
        format!(
            "unknown ffi_call_lib_cstr handle type for {}",
            instruction.args[0]
        )
    })?;
    let handle = render_operand(&instruction.args[0], env, Some(&handle_ty), named_types)?;
    let symbol = instruction
        .args
        .get(1)
        .ok_or_else(|| "ffi_call_lib_cstr requires a symbol".to_string())?;
    let operand = instruction
        .args
        .get(2)
        .ok_or_else(|| "ffi_call_lib_cstr requires a buf[u8] operand".to_string())?;
    let operand_ty = resolve_operand_type(operand, type_env)
        .ok_or_else(|| format!("unknown ffi_call_lib_cstr operand type for {operand}"))?;
    let rendered = render_operand(operand, env, Some(&operand_ty), named_types)?;
    Ok(format!(
        "(({}) mira_ffi_call_lib_cstr_u64({}, {}, (const char*) {}.data))",
        instruction.ty.c_type()?,
        handle,
        render_c_string_literal(symbol),
        rendered
    ))
}

fn parse_spawn_invocation(
    tokens: &[String],
) -> Result<(String, Vec<String>, Vec<(String, String)>), String> {
    let command = tokens
        .first()
        .ok_or_else(|| "spawn invocation requires a command".to_string())?;
    if !is_valid_spawn_command(command) {
        return Err(format!("invalid spawn command {command}"));
    }
    let mut argv = Vec::new();
    let mut env_vars = Vec::new();
    for token in tokens.iter().skip(1) {
        if let Some(payload) = token.strip_prefix("env:") {
            let (name, value) = payload
                .split_once('=')
                .ok_or_else(|| format!("invalid spawn env token {token}"))?;
            if !is_valid_spawn_env_name(name) || !is_valid_spawn_env_value(value) {
                return Err(format!("invalid spawn env token {token}"));
            }
            env_vars.push((name.to_string(), value.to_string()));
        } else {
            if !is_valid_spawn_arg(token) {
                return Err(format!("invalid spawn argv token {token}"));
            }
            argv.push(token.clone());
        }
    }
    Ok((command.clone(), argv, env_vars))
}

fn render_spawn_shell_command(
    command: &str,
    argv: &[String],
    env_vars: &[(String, String)],
    stderr_only: bool,
) -> Result<String, String> {
    let mut pieces = Vec::new();
    for (name, value) in env_vars {
        if !is_valid_spawn_env_name(name) || !is_valid_spawn_env_value(value) {
            return Err(format!("invalid spawn env pair {name}={value}"));
        }
        pieces.push(format!("{name}={value}"));
    }
    pieces.push(command.to_string());
    for arg in argv {
        if !is_valid_spawn_arg(arg) {
            return Err(format!("invalid spawn argv token {arg}"));
        }
        pieces.push(arg.clone());
    }
    let mut command_line = pieces.join(" ");
    if stderr_only {
        #[cfg(target_os = "windows")]
        {
            command_line.push_str(" 2>&1 1>nul");
        }
        #[cfg(not(target_os = "windows"))]
        {
            command_line.push_str(" 2>&1 1>/dev/null");
        }
    }
    Ok(command_line)
}

fn build_env(function: &Function, block: &Block) -> HashMap<String, String> {
    let mut env = HashMap::new();
    for arg in &function.args {
        env.insert(arg.name.clone(), arg.name.clone());
    }
    for param in &block.params {
        env.insert(param.name.clone(), param_c_name(block, &param.name));
    }
    for block in &function.blocks {
        for instruction in &block.instructions {
            env.insert(instruction.bind.clone(), instruction.bind.clone());
        }
    }
    env
}

fn build_type_env(function: &Function, block: &Block) -> HashMap<String, TypeRef> {
    let mut env = HashMap::new();
    for arg in &function.args {
        env.insert(arg.name.clone(), arg.ty.clone());
    }
    for param in &block.params {
        env.insert(param.name.clone(), param.ty.clone());
    }
    for block in &function.blocks {
        for instruction in &block.instructions {
            env.insert(instruction.bind.clone(), instruction.ty.clone());
        }
    }
    env
}

fn build_named_type_map(program: &Program) -> Result<HashMap<String, TypeDeclBody>, String> {
    let mut out = HashMap::new();
    for item in &program.types {
        if out.insert(item.name.clone(), item.body.clone()).is_some() {
            return Err(format!("duplicate type declaration {}", item.name));
        }
    }
    Ok(out)
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum FfiArgSignature {
    Scalar(TypeRef),
    CStr,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct FfiSignature {
    symbol: String,
    ret: TypeRef,
    args: Vec<FfiArgSignature>,
}

fn emit_ffi_decls(out: &mut String, program: &Program) -> Result<(), String> {
    let signatures = collect_ffi_signatures(program)?;
    for signature in signatures {
        let ret = signature.ret.c_type()?;
        let args = if signature.args.is_empty() {
            "void".to_string()
        } else {
            let mut rendered = Vec::new();
            for arg in &signature.args {
                rendered.push(match arg {
                    FfiArgSignature::Scalar(arg) => arg.c_type()?,
                    FfiArgSignature::CStr => "const char*".to_string(),
                });
            }
            rendered.join(", ")
        };
        out.push_str(&format!("extern {} {}({});\n", ret, signature.symbol, args));
    }
    out.push('\n');
    Ok(())
}

fn emit_runtime_bridge_decls(out: &mut String, program: &Program) {
    if program_uses_op(program, "net_listen") {
        out.push_str("extern uint64_t mira_net_listen_handle(const char* host, uint16_t port);\n");
    }
    if program_uses_op(program, "tls_listen") {
        out.push_str(
            "extern uint64_t mira_tls_listen_handle(const char* host, uint16_t port, const char* cert, const char* key, uint32_t request_timeout_ms, uint32_t session_timeout_ms, uint32_t shutdown_grace_ms);\n",
        );
    }
    if program_uses_op(program, "net_accept") {
        out.push_str("extern uint64_t mira_net_accept_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "net_session_open") {
        out.push_str("extern uint64_t mira_net_session_open_handle(const char* host, uint16_t port);\n");
    }
    if program_uses_op(program, "http_session_accept") {
        out.push_str("extern uint64_t mira_http_session_accept_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "net_read_all") {
        out.push_str("extern buf_u8 mira_net_read_all_handle_buf_u8(uint64_t handle);\n");
    }
    if program_uses_op(program, "session_read_chunk") {
        out.push_str("extern buf_u8 mira_session_read_chunk_buf_u8(uint64_t handle, uint32_t chunk_len);\n");
    }
    if program_uses_op(program, "http_session_request") {
        out.push_str("extern buf_u8 mira_http_session_request_buf_u8(uint64_t handle);\n");
    }
    if program_uses_op(program, "net_write_handle_all") {
        out.push_str(
            "extern bool mira_net_write_handle_all_buf_u8(uint64_t handle, buf_u8 value);\n",
        );
    }
    if program_uses_op(program, "session_write_chunk") {
        out.push_str("extern bool mira_session_write_chunk_buf_u8(uint64_t handle, buf_u8 value);\n");
    }
    if program_uses_op(program, "session_flush") {
        out.push_str("extern bool mira_session_flush_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "session_alive") {
        out.push_str("extern bool mira_session_alive_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "session_heartbeat") {
        out.push_str("extern bool mira_session_heartbeat_buf_u8(uint64_t handle, buf_u8 value);\n");
    }
    if program_uses_op(program, "session_backpressure") {
        out.push_str("extern uint32_t mira_session_backpressure_u32(uint64_t handle);\n");
    }
    if program_uses_op(program, "session_backpressure_wait") {
        out.push_str(
            "extern bool mira_session_backpressure_wait(uint64_t handle, uint32_t max_pending);\n",
        );
    }
    if program_uses_op(program, "session_resume_id") {
        out.push_str("extern uint64_t mira_session_resume_id_u64(uint64_t handle);\n");
    }
    if program_uses_op(program, "session_reconnect") {
        out.push_str("extern bool mira_session_reconnect_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "net_close") {
        out.push_str("extern bool mira_net_close_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "http_session_close") {
        out.push_str("extern bool mira_http_session_close_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "listener_set_timeout_ms") {
        out.push_str(
            "extern bool mira_listener_set_timeout_ms(uint64_t handle, uint32_t timeout_ms);\n",
        );
    }
    if program_uses_op(program, "session_set_timeout_ms") {
        out.push_str(
            "extern bool mira_session_set_timeout_ms(uint64_t handle, uint32_t timeout_ms);\n",
        );
    }
    if program_uses_op(program, "listener_set_shutdown_grace_ms") {
        out.push_str("extern bool mira_listener_set_shutdown_grace_ms(uint64_t handle, uint32_t grace_ms);\n");
    }
    if program_uses_op(program, "http_method_eq") {
        out.push_str(
            "extern bool mira_http_method_eq_buf_u8(buf_u8 request, const char* method);\n",
        );
    }
    if program_uses_op(program, "http_path_eq") {
        out.push_str("extern bool mira_http_path_eq_buf_u8(buf_u8 request, const char* path);\n");
    }
    if program_uses_op(program, "http_request_method") {
        out.push_str("extern buf_u8 mira_http_request_method_buf_u8(buf_u8 request);\n");
    }
    if program_uses_op(program, "http_request_path") {
        out.push_str("extern buf_u8 mira_http_request_path_buf_u8(buf_u8 request);\n");
    }
    if program_uses_op(program, "http_route_param") {
        out.push_str(
            "extern buf_u8 mira_http_route_param_buf_u8(buf_u8 request, const char* pattern, const char* param);\n",
        );
    }
    if program_uses_op(program, "http_header_eq") {
        out.push_str(
            "extern bool mira_http_header_eq_buf_u8(buf_u8 request, const char* name, const char* value);\n",
        );
    }
    if program_uses_op(program, "http_cookie_eq") {
        out.push_str(
            "extern bool mira_http_cookie_eq_buf_u8(buf_u8 request, const char* name, const char* value);\n",
        );
    }
    if program_uses_op(program, "http_status_u32") {
        out.push_str("extern uint32_t mira_http_status_u32_buf_u8(buf_u8 value);\n");
    }
    if program_uses_op(program, "buf_eq_lit") || program_uses_op(program, "str_eq_lit") {
        out.push_str("extern bool mira_buf_eq_lit_u8(buf_u8 value, const char* literal);\n");
    }
    if program_uses_op(program, "buf_contains_lit") {
        out.push_str("extern bool mira_buf_contains_lit_u8(buf_u8 value, const char* literal);\n");
    }
    if program_uses_op(program, "buf_lit")
        || program_uses_op(program, "str_lit")
        || program_uses_op(program, "config_get_str")
        || program_uses_op(program, "tls_server_config_buf")
    {
        out.push_str("extern buf_u8 mira_buf_lit_u8(const char* literal);\n");
    }
    if program_uses_op(program, "buf_concat")
        || program_uses_op(program, "str_concat")
        || program_uses_op(program, "json_encode_obj")
        || program_uses_op(program, "json_encode_arr")
    {
        out.push_str("extern buf_u8 mira_buf_concat_u8(buf_u8 left, buf_u8 right);\n");
    }
    if program_uses_op(program, "http_header") {
        out.push_str("extern buf_u8 mira_http_header_buf_u8(buf_u8 request, const char* name);\n");
    }
    if program_uses_op(program, "http_header_count") {
        out.push_str("extern uint32_t mira_http_header_count_buf_u8(buf_u8 request);\n");
    }
    if program_uses_op(program, "http_header_name") {
        out.push_str("extern buf_u8 mira_http_header_name_buf_u8(buf_u8 request, uint32_t index);\n");
    }
    if program_uses_op(program, "http_header_value") {
        out.push_str("extern buf_u8 mira_http_header_value_buf_u8(buf_u8 request, uint32_t index);\n");
    }
    if program_uses_op(program, "http_cookie") {
        out.push_str("extern buf_u8 mira_http_cookie_buf_u8(buf_u8 request, const char* name);\n");
    }
    if program_uses_op(program, "http_query_param") {
        out.push_str(
            "extern buf_u8 mira_http_query_param_buf_u8(buf_u8 request, const char* key);\n",
        );
    }
    if program_uses_op(program, "http_body") {
        out.push_str("extern buf_u8 mira_http_body_buf_u8(buf_u8 request);\n");
    }
    if program_uses_op(program, "http_multipart_part_count") {
        out.push_str("extern uint32_t mira_http_multipart_part_count_buf_u8(buf_u8 request);\n");
    }
    if program_uses_op(program, "http_multipart_part_name") {
        out.push_str("extern buf_u8 mira_http_multipart_part_name_buf_u8(buf_u8 request, uint32_t index);\n");
    }
    if program_uses_op(program, "http_multipart_part_filename") {
        out.push_str("extern buf_u8 mira_http_multipart_part_filename_buf_u8(buf_u8 request, uint32_t index);\n");
    }
    if program_uses_op(program, "http_multipart_part_body") {
        out.push_str("extern buf_u8 mira_http_multipart_part_body_buf_u8(buf_u8 request, uint32_t index);\n");
    }
    if program_uses_op(program, "http_body_limit") {
        out.push_str("extern bool mira_http_body_limit_buf_u8(buf_u8 request, uint32_t limit);\n");
    }
    if program_uses_op(program, "http_body_stream_open") {
        out.push_str("extern uint64_t mira_http_body_stream_open_buf_u8(buf_u8 request);\n");
    }
    if program_uses_op(program, "http_body_stream_next") {
        out.push_str("extern buf_u8 mira_http_body_stream_next_buf_u8(uint64_t handle, uint32_t chunk_size);\n");
    }
    if program_uses_op(program, "http_body_stream_close") {
        out.push_str("extern bool mira_http_body_stream_close_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "http_server_config_u32") {
        out.push_str("extern uint32_t mira_http_server_config_u32(const char* token);\n");
    }
    if program_uses_op(program, "buf_parse_u32") {
        out.push_str("extern uint32_t mira_buf_parse_u32_u8(buf_u8 value);\n");
    }
    if program_uses_op(program, "buf_parse_bool") {
        out.push_str("extern bool mira_buf_parse_bool_u8(buf_u8 value);\n");
    }
    if program_uses_op(program, "str_from_u32")
        || program_uses_op(program, "json_encode_obj")
        || program_uses_op(program, "json_encode_arr")
    {
        out.push_str("extern buf_u8 mira_str_from_u32(uint32_t value);\n");
    }
    if program_uses_op(program, "str_from_bool")
        || program_uses_op(program, "json_encode_obj")
        || program_uses_op(program, "json_encode_arr")
    {
        out.push_str("extern buf_u8 mira_str_from_bool(bool value);\n");
    }
    if program_uses_op(program, "buf_hex_str") {
        out.push_str("extern buf_u8 mira_buf_hex_str_u8(buf_u8 value);\n");
    }
    if program_uses_op(program, "json_encode_obj") || program_uses_op(program, "json_encode_arr") {
        out.push_str("extern buf_u8 mira_json_quote_str_u8(buf_u8 value);\n");
    }
    if program_uses_op(program, "http_write_response") {
        out.push_str(
            "extern bool mira_http_write_response_handle(uint64_t handle, uint32_t status, buf_u8 body);\n",
        );
    }
    if program_uses_op(program, "http_write_text_response") {
        out.push_str(
            "extern bool mira_http_write_text_response_handle(uint64_t handle, uint32_t status, buf_u8 body);\n",
        );
    }
    if program_uses_op(program, "http_write_text_response_cookie") {
        out.push_str(
            "extern bool mira_http_write_text_response_cookie_handle(uint64_t handle, uint32_t status, const char* cookie_name, const char* cookie_value, buf_u8 body);\n",
        );
    }
    if program_uses_op(program, "http_write_text_response_headers2") {
        out.push_str(
            "extern bool mira_http_write_text_response_headers2_handle(uint64_t handle, uint32_t status, const char* header1_name, const char* header1_value, const char* header2_name, const char* header2_value, buf_u8 body);\n",
        );
    }
    if program_uses_op(program, "http_session_write_text") {
        out.push_str(
            "extern bool mira_http_session_write_text_handle(uint64_t handle, uint32_t status, buf_u8 body);\n",
        );
    }
    if program_uses_op(program, "http_session_write_text_headers2") {
        out.push_str(
            "extern bool mira_http_session_write_text_headers2_handle(uint64_t handle, uint32_t status, const char* header1_name, const char* header1_value, const char* header2_name, const char* header2_value, buf_u8 body);\n",
        );
    }
    if program_uses_op(program, "http_session_write_text_cookie") {
        out.push_str(
            "extern bool mira_http_session_write_text_cookie_handle(uint64_t handle, uint32_t status, const char* cookie_name, const char* cookie_value, buf_u8 body);\n",
        );
    }
    if program_uses_op(program, "http_write_json_response") {
        out.push_str(
            "extern bool mira_http_write_json_response_handle(uint64_t handle, uint32_t status, buf_u8 body);\n",
        );
    }
    if program_uses_op(program, "http_write_json_response_cookie") {
        out.push_str(
            "extern bool mira_http_write_json_response_cookie_handle(uint64_t handle, uint32_t status, const char* cookie_name, const char* cookie_value, buf_u8 body);\n",
        );
    }
    if program_uses_op(program, "http_write_json_response_headers2") {
        out.push_str(
            "extern bool mira_http_write_json_response_headers2_handle(uint64_t handle, uint32_t status, const char* header1_name, const char* header1_value, const char* header2_name, const char* header2_value, buf_u8 body);\n",
        );
    }
    if program_uses_op(program, "http_session_write_json") {
        out.push_str(
            "extern bool mira_http_session_write_json_handle(uint64_t handle, uint32_t status, buf_u8 body);\n",
        );
    }
    if program_uses_op(program, "http_session_write_json_headers2") {
        out.push_str(
            "extern bool mira_http_session_write_json_headers2_handle(uint64_t handle, uint32_t status, const char* header1_name, const char* header1_value, const char* header2_name, const char* header2_value, buf_u8 body);\n",
        );
    }
    if program_uses_op(program, "http_session_write_json_cookie") {
        out.push_str(
            "extern bool mira_http_session_write_json_cookie_handle(uint64_t handle, uint32_t status, const char* cookie_name, const char* cookie_value, buf_u8 body);\n",
        );
    }
    if program_uses_op(program, "http_write_response_header") {
        out.push_str(
            "extern bool mira_http_write_response_header_handle(uint64_t handle, uint32_t status, const char* header_name, const char* header_value, buf_u8 body);\n",
        );
    }
    if program_uses_op(program, "http_response_stream_open") {
        out.push_str(
            "extern uint64_t mira_http_response_stream_open_handle(uint64_t handle, uint32_t status, const char* content_type);\n",
        );
    }
    if program_uses_op(program, "http_response_stream_write") {
        out.push_str("extern bool mira_http_response_stream_write_handle(uint64_t handle, buf_u8 body);\n");
    }
    if program_uses_op(program, "http_response_stream_close") {
        out.push_str("extern bool mira_http_response_stream_close_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "http_client_open") {
        out.push_str("extern uint64_t mira_http_client_open_handle(const char* host, uint16_t port);\n");
    }
    if program_uses_op(program, "http_client_request") {
        out.push_str("extern buf_u8 mira_http_client_request_buf_u8(uint64_t handle, buf_u8 request);\n");
    }
    if program_uses_op(program, "http_client_request_retry") {
        out.push_str("extern buf_u8 mira_http_client_request_retry_buf_u8(uint64_t handle, uint32_t retries, uint32_t backoff_ms, buf_u8 request);\n");
    }
    if program_uses_op(program, "http_client_close") {
        out.push_str("extern bool mira_http_client_close_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "http_client_pool_open") {
        out.push_str("extern uint64_t mira_http_client_pool_open_handle(const char* host, uint16_t port, uint32_t max_size);\n");
    }
    if program_uses_op(program, "http_client_pool_acquire") {
        out.push_str("extern uint64_t mira_http_client_pool_acquire_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "http_client_pool_release") {
        out.push_str("extern bool mira_http_client_pool_release_handle(uint64_t pool_handle, uint64_t handle);\n");
    }
    if program_uses_op(program, "http_client_pool_close") {
        out.push_str("extern bool mira_http_client_pool_close_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "json_get_u32")
        || program_uses_op(program, "json_get_u32_or")
        || program_uses_op(program, "strmap_get_u32")
    {
        out.push_str("extern uint32_t mira_json_get_u32_buf_u8(buf_u8 value, const char* key);\n");
    }
    if program_uses_op(program, "json_get_bool")
        || program_uses_op(program, "json_get_bool_or")
        || program_uses_op(program, "strmap_get_bool")
    {
        out.push_str("extern bool mira_json_get_bool_buf_u8(buf_u8 value, const char* key);\n");
    }
    if program_uses_op(program, "json_get_buf") || program_uses_op(program, "json_get_buf_or") {
        out.push_str("extern buf_u8 mira_json_get_buf_buf_u8(buf_u8 value, const char* key);\n");
    }
    if program_uses_op(program, "json_get_str")
        || program_uses_op(program, "json_get_str_or")
        || program_uses_op(program, "strmap_get_str")
    {
        out.push_str("extern buf_u8 mira_json_get_buf_buf_u8(buf_u8 value, const char* key);\n");
    }
    if program_uses_op(program, "json_has_key")
        || program_uses_op(program, "json_get_u32_or")
        || program_uses_op(program, "json_get_bool_or")
        || program_uses_op(program, "json_get_buf_or")
        || program_uses_op(program, "json_get_str_or")
    {
        out.push_str("extern bool mira_json_has_key_buf_u8(buf_u8 value, const char* key);\n");
    }
    if program_uses_op(program, "json_array_len") || program_uses_op(program, "strlist_len") {
        out.push_str("extern uint32_t mira_json_array_len_buf_u8(buf_u8 value);\n");
    }
    if program_uses_op(program, "json_index_u32") || program_uses_op(program, "strlist_index_u32") {
        out.push_str("extern uint32_t mira_json_index_u32_buf_u8(buf_u8 value, uint32_t index);\n");
    }
    if program_uses_op(program, "json_index_bool") || program_uses_op(program, "strlist_index_bool")
    {
        out.push_str("extern bool mira_json_index_bool_buf_u8(buf_u8 value, uint32_t index);\n");
    }
    if program_uses_op(program, "json_index_str") || program_uses_op(program, "strlist_index_str") {
        out.push_str("extern buf_u8 mira_json_index_str_buf_u8(buf_u8 value, uint32_t index);\n");
    }
    if program_uses_op(program, "env_get_u32") {
        out.push_str("extern uint32_t mira_env_get_u32(const char* name);\n");
    }
    if program_uses_op(program, "env_get_bool") {
        out.push_str("extern bool mira_env_get_bool(const char* name);\n");
    }
    if program_uses_op(program, "env_get_str") {
        out.push_str("extern buf_u8 mira_env_get_str_u8(const char* name);\n");
    }
    if program_uses_op(program, "env_has") {
        out.push_str("extern bool mira_env_has(const char* name);\n");
    }
    if program_uses_op(program, "buf_before_lit") {
        out.push_str("extern buf_u8 mira_buf_before_lit_u8(buf_u8 value, const char* literal);\n");
    }
    if program_uses_op(program, "buf_after_lit") {
        out.push_str("extern buf_u8 mira_buf_after_lit_u8(buf_u8 value, const char* literal);\n");
    }
    if program_uses_op(program, "buf_trim_ascii") {
        out.push_str("extern buf_u8 mira_buf_trim_ascii_u8(buf_u8 value);\n");
    }
    if program_uses_op(program, "date_parse_ymd") {
        out.push_str("extern uint32_t mira_date_parse_ymd(buf_u8 value);\n");
    }
    if program_uses_op(program, "time_parse_hms") {
        out.push_str("extern uint32_t mira_time_parse_hms(buf_u8 value);\n");
    }
    if program_uses_op(program, "date_format_ymd") {
        out.push_str("extern buf_u8 mira_date_format_ymd(uint32_t value);\n");
    }
    if program_uses_op(program, "time_format_hms") {
        out.push_str("extern buf_u8 mira_time_format_hms(uint32_t value);\n");
    }
    if program_uses_op(program, "db_open") {
        out.push_str("extern uint64_t mira_db_open_handle(const char* path);\n");
    }
    if program_uses_op(program, "db_close") {
        out.push_str("extern bool mira_db_close_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "db_exec") {
        out.push_str("extern bool mira_db_exec_handle_sql_buf_u8(uint64_t handle, buf_u8 sql);\n");
    }
    if program_uses_op(program, "db_prepare") {
        out.push_str(
            "extern bool mira_db_prepare_handle_stmt_sql_buf_u8(uint64_t handle, const char* stmt, buf_u8 sql);\n",
        );
    }
    if program_uses_op(program, "db_exec_prepared") {
        out.push_str(
            "extern bool mira_db_exec_prepared_handle_stmt_params_buf_u8(uint64_t handle, const char* stmt, buf_u8 params);\n",
        );
    }
    if program_uses_op(program, "db_query_u32") {
        out.push_str(
            "extern uint32_t mira_db_query_u32_handle_sql_buf_u8(uint64_t handle, buf_u8 sql);\n",
        );
    }
    if program_uses_op(program, "db_query_buf") {
        out.push_str(
            "extern buf_u8 mira_db_query_buf_handle_sql_buf_u8(uint64_t handle, buf_u8 sql);\n",
        );
    }
    if program_uses_op(program, "db_query_row") {
        out.push_str(
            "extern buf_u8 mira_db_query_row_handle_sql_buf_u8(uint64_t handle, buf_u8 sql);\n",
        );
    }
    if program_uses_op(program, "db_query_prepared_u32") {
        out.push_str(
            "extern uint32_t mira_db_query_prepared_u32_handle_stmt_params_buf_u8(uint64_t handle, const char* stmt, buf_u8 params);\n",
        );
    }
    if program_uses_op(program, "db_query_prepared_buf") {
        out.push_str(
            "extern buf_u8 mira_db_query_prepared_buf_handle_stmt_params_buf_u8(uint64_t handle, const char* stmt, buf_u8 params);\n",
        );
    }
    if program_uses_op(program, "db_query_prepared_row") {
        out.push_str(
            "extern buf_u8 mira_db_query_prepared_row_handle_stmt_params_buf_u8(uint64_t handle, const char* stmt, buf_u8 params);\n",
        );
    }
    if program_uses_op(program, "db_last_error_code") {
        out.push_str("extern uint32_t mira_db_last_error_code_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "db_last_error_retryable") {
        out.push_str("extern bool mira_db_last_error_retryable_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "db_begin") {
        out.push_str("extern bool mira_db_begin_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "db_commit") {
        out.push_str("extern bool mira_db_commit_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "db_rollback") {
        out.push_str("extern bool mira_db_rollback_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "db_pool_open") {
        out.push_str(
            "extern uint64_t mira_db_pool_open_handle(const char* target, uint32_t max_size);\n",
        );
    }
    if program_uses_op(program, "db_pool_set_max_idle") {
        out.push_str("extern bool mira_db_pool_set_max_idle_handle(uint64_t pool_handle, uint32_t max_idle);\n");
    }
    if program_uses_op(program, "db_pool_leased") {
        out.push_str("extern uint32_t mira_db_pool_leased_handle(uint64_t pool_handle);\n");
    }
    if program_uses_op(program, "db_pool_acquire") {
        out.push_str("extern uint64_t mira_db_pool_acquire_handle(uint64_t pool_handle);\n");
    }
    if program_uses_op(program, "db_pool_release") {
        out.push_str(
            "extern bool mira_db_pool_release_handle(uint64_t pool_handle, uint64_t db_handle);\n",
        );
    }
    if program_uses_op(program, "db_pool_close") {
        out.push_str("extern bool mira_db_pool_close_handle(uint64_t pool_handle);\n");
    }
    if program_uses_op(program, "cache_open") {
        out.push_str("extern uint64_t mira_cache_open_handle(const char* target);\n");
    }
    if program_uses_op(program, "cache_close") {
        out.push_str("extern bool mira_cache_close_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "cache_get_buf") {
        out.push_str("extern buf_u8 mira_cache_get_buf_handle_key_u8(uint64_t handle, buf_u8 key);\n");
    }
    if program_uses_op(program, "cache_set_buf") {
        out.push_str("extern bool mira_cache_set_buf_handle_key_value_u8(uint64_t handle, buf_u8 key, buf_u8 value);\n");
    }
    if program_uses_op(program, "cache_set_buf_ttl") {
        out.push_str("extern bool mira_cache_set_buf_ttl_handle_key_value_u8(uint64_t handle, buf_u8 key, uint32_t ttl_ms, buf_u8 value);\n");
    }
    if program_uses_op(program, "cache_del") {
        out.push_str("extern bool mira_cache_del_handle_key_u8(uint64_t handle, buf_u8 key);\n");
    }
    if program_uses_op(program, "queue_open") {
        out.push_str("extern uint64_t mira_queue_open_handle(const char* target);\n");
    }
    if program_uses_op(program, "queue_close") {
        out.push_str("extern bool mira_queue_close_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "queue_push_buf") {
        out.push_str("extern bool mira_queue_push_buf_handle_value_u8(uint64_t handle, buf_u8 value);\n");
    }
    if program_uses_op(program, "queue_pop_buf") {
        out.push_str("extern buf_u8 mira_queue_pop_buf_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "queue_len") {
        out.push_str("extern uint32_t mira_queue_len_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "stream_open") {
        out.push_str("extern uint64_t mira_stream_open_handle(const char* target);\n");
    }
    if program_uses_op(program, "stream_close") {
        out.push_str("extern bool mira_stream_close_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "stream_publish_buf") {
        out.push_str(
            "extern uint32_t mira_stream_publish_buf_handle_value_u8(uint64_t handle, buf_u8 value);\n",
        );
    }
    if program_uses_op(program, "stream_len") {
        out.push_str("extern uint32_t mira_stream_len_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "stream_replay_open") {
        out.push_str(
            "extern uint64_t mira_stream_replay_open_handle(uint64_t handle, uint32_t offset);\n",
        );
    }
    if program_uses_op(program, "stream_replay_next") {
        out.push_str("extern buf_u8 mira_stream_replay_next_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "stream_replay_offset") {
        out.push_str("extern uint32_t mira_stream_replay_offset_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "stream_replay_close") {
        out.push_str("extern bool mira_stream_replay_close_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "shard_route_u32") {
        out.push_str("extern uint32_t mira_shard_route_u32_buf_u8(buf_u8 key, uint32_t shard_count);\n");
    }
    if program_uses_op(program, "lease_open") {
        out.push_str("extern uint64_t mira_lease_open_handle(const char* target);\n");
    }
    if program_uses_op(program, "lease_acquire") {
        out.push_str("extern bool mira_lease_acquire_handle(uint64_t handle, uint32_t owner);\n");
    }
    if program_uses_op(program, "lease_owner") {
        out.push_str("extern uint32_t mira_lease_owner_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "lease_transfer") {
        out.push_str("extern bool mira_lease_transfer_handle(uint64_t handle, uint32_t owner);\n");
    }
    if program_uses_op(program, "lease_release") {
        out.push_str("extern bool mira_lease_release_handle(uint64_t handle, uint32_t owner);\n");
    }
    if program_uses_op(program, "lease_close") {
        out.push_str("extern bool mira_lease_close_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "placement_open") {
        out.push_str("extern uint64_t mira_placement_open_handle(const char* target);\n");
    }
    if program_uses_op(program, "placement_assign") {
        out.push_str("extern bool mira_placement_assign_handle(uint64_t handle, uint32_t shard, uint32_t node);\n");
    }
    if program_uses_op(program, "placement_lookup") {
        out.push_str("extern uint32_t mira_placement_lookup_handle(uint64_t handle, uint32_t shard);\n");
    }
    if program_uses_op(program, "placement_close") {
        out.push_str("extern bool mira_placement_close_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "coord_open") {
        out.push_str("extern uint64_t mira_coord_open_handle(const char* target);\n");
    }
    if program_uses_op(program, "coord_store_u32") {
        out.push_str("extern bool mira_coord_store_u32_handle(uint64_t handle, const char* key, uint32_t value);\n");
    }
    if program_uses_op(program, "coord_load_u32") {
        out.push_str("extern uint32_t mira_coord_load_u32_handle(uint64_t handle, const char* key);\n");
    }
    if program_uses_op(program, "coord_close") {
        out.push_str("extern bool mira_coord_close_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "batch_open") {
        out.push_str("extern uint64_t mira_batch_open_handle(void);\n");
    }
    if program_uses_op(program, "batch_push_u64") {
        out.push_str("extern bool mira_batch_push_u64_handle_value(uint64_t handle, uint64_t value);\n");
    }
    if program_uses_op(program, "batch_len") {
        out.push_str("extern uint32_t mira_batch_len_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "batch_flush_sum_u64") {
        out.push_str("extern uint64_t mira_batch_flush_sum_u64_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "batch_close") {
        out.push_str("extern bool mira_batch_close_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "agg_open_u64") {
        out.push_str("extern uint64_t mira_agg_open_u64_handle(void);\n");
    }
    if program_uses_op(program, "agg_add_u64") {
        out.push_str("extern bool mira_agg_add_u64_handle_value(uint64_t handle, uint64_t value);\n");
    }
    if program_uses_op(program, "agg_count") {
        out.push_str("extern uint32_t mira_agg_count_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "agg_sum_u64") {
        out.push_str("extern uint64_t mira_agg_sum_u64_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "agg_avg_u64") {
        out.push_str("extern uint64_t mira_agg_avg_u64_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "agg_min_u64") {
        out.push_str("extern uint64_t mira_agg_min_u64_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "agg_max_u64") {
        out.push_str("extern uint64_t mira_agg_max_u64_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "agg_close") {
        out.push_str("extern bool mira_agg_close_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "window_open_ms") {
        out.push_str("extern uint64_t mira_window_open_ms_handle(uint32_t width_ms);\n");
    }
    if program_uses_op(program, "window_add_u64") {
        out.push_str(
            "extern bool mira_window_add_u64_handle_value(uint64_t handle, uint64_t value);\n",
        );
    }
    if program_uses_op(program, "window_count") {
        out.push_str("extern uint32_t mira_window_count_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "window_sum_u64") {
        out.push_str("extern uint64_t mira_window_sum_u64_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "window_avg_u64") {
        out.push_str("extern uint64_t mira_window_avg_u64_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "window_min_u64") {
        out.push_str("extern uint64_t mira_window_min_u64_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "window_max_u64") {
        out.push_str("extern uint64_t mira_window_max_u64_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "window_close") {
        out.push_str("extern bool mira_window_close_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "msg_log_open") {
        out.push_str("extern uint64_t mira_msg_log_open_handle(void);\n");
    }
    if program_uses_op(program, "msg_log_close") {
        out.push_str("extern bool mira_msg_log_close_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "msg_send") {
        out.push_str("extern uint32_t mira_msg_send_handle_buf_u8(uint64_t handle, const char* conversation, const char* recipient, buf_u8 payload);\n");
    }
    if program_uses_op(program, "msg_send_dedup") {
        out.push_str("extern uint32_t mira_msg_send_dedup_handle_buf_u8(uint64_t handle, const char* conversation, const char* recipient, buf_u8 dedup_key, buf_u8 payload);\n");
    }
    if program_uses_op(program, "msg_subscribe") {
        out.push_str("extern bool mira_msg_subscribe_handle(uint64_t handle, const char* room, const char* recipient);\n");
    }
    if program_uses_op(program, "msg_subscriber_count") {
        out.push_str("extern uint32_t mira_msg_subscriber_count_handle(uint64_t handle, const char* room);\n");
    }
    if program_uses_op(program, "msg_fanout") {
        out.push_str("extern uint32_t mira_msg_fanout_handle_buf_u8(uint64_t handle, const char* room, buf_u8 payload);\n");
    }
    if program_uses_op(program, "msg_recv_next") {
        out.push_str("extern buf_u8 mira_msg_recv_next_handle(uint64_t handle, const char* recipient);\n");
    }
    if program_uses_op(program, "msg_recv_seq") {
        out.push_str("extern uint32_t mira_msg_recv_seq_handle(uint64_t handle, const char* recipient);\n");
    }
    if program_uses_op(program, "msg_ack") {
        out.push_str("extern bool mira_msg_ack_handle(uint64_t handle, const char* recipient, uint32_t seq);\n");
    }
    if program_uses_op(program, "msg_mark_retry") {
        out.push_str("extern bool mira_msg_mark_retry_handle(uint64_t handle, const char* recipient, uint32_t seq);\n");
    }
    if program_uses_op(program, "msg_retry_count") {
        out.push_str("extern uint32_t mira_msg_retry_count_handle(uint64_t handle, const char* recipient, uint32_t seq);\n");
    }
    if program_uses_op(program, "msg_pending_count") {
        out.push_str("extern uint32_t mira_msg_pending_count_handle(uint64_t handle, const char* recipient);\n");
    }
    if program_uses_op(program, "msg_delivery_total") {
        out.push_str("extern uint32_t mira_msg_delivery_total_handle(uint64_t handle, const char* recipient);\n");
    }
    if program_uses_op(program, "msg_failure_class") {
        out.push_str("extern uint32_t mira_msg_failure_class_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "msg_replay_open") {
        out.push_str("extern uint64_t mira_msg_replay_open_handle(uint64_t handle, const char* recipient, uint32_t from_seq);\n");
    }
    if program_uses_op(program, "msg_replay_next") {
        out.push_str("extern buf_u8 mira_msg_replay_next_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "msg_replay_seq") {
        out.push_str("extern uint32_t mira_msg_replay_seq_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "msg_replay_close") {
        out.push_str("extern bool mira_msg_replay_close_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "service_open") {
        out.push_str("extern uint64_t mira_service_open_handle(const char* name);\n");
    }
    if program_uses_op(program, "service_close") {
        out.push_str("extern bool mira_service_close_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "service_shutdown") {
        out.push_str(
            "extern bool mira_service_shutdown_handle(uint64_t handle, uint32_t grace_ms);\n",
        );
    }
    if program_uses_op(program, "service_log") {
        out.push_str(
            "extern bool mira_service_log_buf_u8(uint64_t handle, const char* level, buf_u8 message);\n",
        );
    }
    if program_uses_op(program, "service_trace_begin") {
        out.push_str(
            "extern uint64_t mira_service_trace_begin_handle(uint64_t handle, const char* name);\n",
        );
    }
    if program_uses_op(program, "service_trace_end") {
        out.push_str("extern bool mira_service_trace_end_handle(uint64_t trace_handle);\n");
    }
    if program_uses_op(program, "service_metric_count") {
        out.push_str(
            "extern bool mira_service_metric_count_handle(uint64_t handle, const char* metric, uint32_t value);\n",
        );
    }
    if program_uses_op(program, "service_metric_count_dim") {
        out.push_str(
            "extern bool mira_service_metric_count_dim_handle(uint64_t handle, const char* metric, const char* dimension, uint32_t value);\n",
        );
    }
    if program_uses_op(program, "service_metric_total") {
        out.push_str(
            "extern uint32_t mira_service_metric_total_handle(uint64_t handle, const char* metric);\n",
        );
    }
    if program_uses_op(program, "service_health_status") {
        out.push_str("extern uint32_t mira_service_health_status_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "service_readiness_status") {
        out.push_str("extern uint32_t mira_service_readiness_status_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "service_set_health") {
        out.push_str("extern bool mira_service_set_health_handle(uint64_t handle, uint32_t status);\n");
    }
    if program_uses_op(program, "service_set_readiness") {
        out.push_str(
            "extern bool mira_service_set_readiness_handle(uint64_t handle, uint32_t status);\n",
        );
    }
    if program_uses_op(program, "service_set_degraded") {
        out.push_str(
            "extern bool mira_service_set_degraded_handle(uint64_t handle, bool degraded);\n",
        );
    }
    if program_uses_op(program, "service_degraded") {
        out.push_str("extern bool mira_service_degraded_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "service_event") {
        out.push_str(
            "extern bool mira_service_event_buf_u8(uint64_t handle, const char* event_class, buf_u8 message);\n",
        );
    }
    if program_uses_op(program, "service_event_total") {
        out.push_str(
            "extern uint32_t mira_service_event_total_handle(uint64_t handle, const char* event_class);\n",
        );
    }
    if program_uses_op(program, "service_trace_link") {
        out.push_str(
            "extern bool mira_service_trace_link_handle(uint64_t trace_handle, uint64_t parent_trace);\n",
        );
    }
    if program_uses_op(program, "service_trace_link_count") {
        out.push_str("extern uint32_t mira_service_trace_link_count_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "service_failure_count") {
        out.push_str(
            "extern bool mira_service_failure_count_handle(uint64_t handle, const char* failure_class, uint32_t value);\n",
        );
    }
    if program_uses_op(program, "service_failure_total") {
        out.push_str(
            "extern uint32_t mira_service_failure_total_handle(uint64_t handle, const char* failure_class);\n",
        );
    }
    if program_uses_op(program, "service_checkpoint_save_u32") {
        out.push_str(
            "extern bool mira_service_checkpoint_save_u32_handle(uint64_t handle, const char* key, uint32_t value);\n",
        );
    }
    if program_uses_op(program, "service_checkpoint_load_u32") {
        out.push_str(
            "extern uint32_t mira_service_checkpoint_load_u32_handle(uint64_t handle, const char* key);\n",
        );
    }
    if program_uses_op(program, "service_checkpoint_exists") {
        out.push_str(
            "extern bool mira_service_checkpoint_exists_handle(uint64_t handle, const char* key);\n",
        );
    }
    if program_uses_op(program, "service_migrate_db") {
        out.push_str(
            "extern bool mira_service_migrate_db_handle(uint64_t handle, uint64_t db_handle, const char* migration_name);\n",
        );
    }
    if program_uses_op(program, "service_route") {
        out.push_str(
            "extern bool mira_service_route_buf_u8(buf_u8 request, const char* method, const char* path);\n",
        );
    }
    if program_uses_op(program, "service_require_header") {
        out.push_str(
            "extern bool mira_service_require_header_buf_u8(buf_u8 request, const char* name, const char* value);\n",
        );
    }
    if program_uses_op(program, "service_error_status") {
        out.push_str("extern uint32_t mira_service_error_status(const char* kind);\n");
    }
    if program_uses_op(program, "tls_exchange_all") {
        out.push_str(
            "extern buf_u8 mira_tls_exchange_all_buf_u8(const char* host, uint16_t port, buf_u8 request);\n",
        );
    }
    if program_uses_op(program, "rt_spawn_u32") || program_uses_op(program, "rt_try_spawn_u32") {
        out.push_str(
            "extern uint32_t mira_rt_dispatch_u32(const char* function_name, uint32_t arg);\n",
        );
    }
    if program_uses_op(program, "rt_spawn_buf") || program_uses_op(program, "rt_try_spawn_buf") {
        out.push_str(
            "extern buf_u8 mira_rt_dispatch_buf(const char* function_name, buf_u8 arg);\n",
        );
    }
    if program_uses_op(program, "rt_open") {
        out.push_str("extern uint64_t mira_rt_open_handle(uint32_t workers);\n");
    }
    if program_uses_op(program, "rt_spawn_u32") {
        out.push_str("extern uint64_t mira_rt_spawn_u32_handle(uint64_t runtime_handle, const char* function_name, uint32_t arg);\n");
    }
    if program_uses_op(program, "rt_try_spawn_u32") {
        out.push_str("extern uint64_t mira_rt_try_spawn_u32_handle(uint64_t runtime_handle, const char* function_name, uint32_t arg);\n");
    }
    if program_uses_op(program, "rt_spawn_buf") {
        out.push_str("extern uint64_t mira_rt_spawn_buf_handle(uint64_t runtime_handle, const char* function_name, buf_u8 arg);\n");
    }
    if program_uses_op(program, "rt_try_spawn_buf") {
        out.push_str("extern uint64_t mira_rt_try_spawn_buf_handle(uint64_t runtime_handle, const char* function_name, buf_u8 arg);\n");
    }
    if program_uses_op(program, "rt_done") {
        out.push_str("extern bool mira_rt_done_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "rt_join_u32") {
        out.push_str("extern uint32_t mira_rt_join_u32_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "rt_join_buf") {
        out.push_str("extern buf_u8 mira_rt_join_buf_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "rt_cancel") {
        out.push_str("extern bool mira_rt_cancel_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "rt_task_close") {
        out.push_str("extern bool mira_rt_task_close_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "rt_shutdown") {
        out.push_str(
            "extern bool mira_rt_shutdown_handle(uint64_t runtime_handle, uint32_t grace_ms);\n",
        );
    }
    if program_uses_op(program, "rt_close") {
        out.push_str("extern bool mira_rt_close_handle(uint64_t runtime_handle);\n");
    }
    if program_uses_op(program, "rt_cancelled") {
        out.push_str("extern bool mira_rt_cancelled(void);\n");
    }
    if program_uses_op(program, "rt_inflight") {
        out.push_str("extern uint32_t mira_rt_inflight_handle(uint64_t runtime_handle);\n");
    }
    if program_uses_op(program, "chan_open_u32") {
        out.push_str("extern uint64_t mira_chan_open_u32_handle(uint32_t capacity);\n");
    }
    if program_uses_op(program, "chan_open_buf") {
        out.push_str("extern uint64_t mira_chan_open_buf_handle(uint32_t capacity);\n");
    }
    if program_uses_op(program, "chan_send_u32") {
        out.push_str(
            "extern bool mira_chan_send_u32_handle(uint64_t channel_handle, uint32_t value);\n",
        );
    }
    if program_uses_op(program, "chan_send_buf") {
        out.push_str(
            "extern bool mira_chan_send_buf_handle(uint64_t channel_handle, buf_u8 value);\n",
        );
    }
    if program_uses_op(program, "chan_recv_u32") {
        out.push_str("extern uint32_t mira_chan_recv_u32_handle(uint64_t channel_handle);\n");
    }
    if program_uses_op(program, "chan_recv_buf") {
        out.push_str("extern buf_u8 mira_chan_recv_buf_handle(uint64_t channel_handle);\n");
    }
    if program_uses_op(program, "chan_len") {
        out.push_str("extern uint32_t mira_chan_len_handle(uint64_t channel_handle);\n");
    }
    if program_uses_op(program, "chan_close") {
        out.push_str("extern bool mira_chan_close_handle(uint64_t channel_handle);\n");
    }
    if program_uses_op(program, "deadline_open_ms") {
        out.push_str("extern uint64_t mira_deadline_open_ms_handle(uint32_t timeout_ms);\n");
    }
    if program_uses_op(program, "deadline_expired") {
        out.push_str("extern bool mira_deadline_expired_handle(uint64_t deadline_handle);\n");
    }
    if program_uses_op(program, "deadline_remaining_ms") {
        out.push_str("extern uint32_t mira_deadline_remaining_ms_handle(uint64_t deadline_handle);\n");
    }
    if program_uses_op(program, "deadline_close") {
        out.push_str("extern bool mira_deadline_close_handle(uint64_t deadline_handle);\n");
    }
    if program_uses_op(program, "cancel_scope_open") {
        out.push_str("extern uint64_t mira_cancel_scope_open_handle(void);\n");
    }
    if program_uses_op(program, "cancel_scope_child") {
        out.push_str("extern uint64_t mira_cancel_scope_child_handle(uint64_t parent_scope);\n");
    }
    if program_uses_op(program, "cancel_scope_bind_task") {
        out.push_str("extern bool mira_cancel_scope_bind_task_handle(uint64_t scope_handle, uint64_t task_handle);\n");
    }
    if program_uses_op(program, "cancel_scope_cancel") {
        out.push_str("extern bool mira_cancel_scope_cancel_handle(uint64_t scope_handle);\n");
    }
    if program_uses_op(program, "cancel_scope_cancelled") {
        out.push_str("extern bool mira_cancel_scope_cancelled_handle(uint64_t scope_handle);\n");
    }
    if program_uses_op(program, "cancel_scope_close") {
        out.push_str("extern bool mira_cancel_scope_close_handle(uint64_t scope_handle);\n");
    }
    if program_uses_op(program, "retry_open") {
        out.push_str("extern uint64_t mira_retry_open_handle(uint32_t max_attempts, uint32_t base_backoff_ms);\n");
    }
    if program_uses_op(program, "retry_record_failure") {
        out.push_str("extern bool mira_retry_record_failure_handle(uint64_t retry_handle);\n");
    }
    if program_uses_op(program, "retry_record_success") {
        out.push_str("extern bool mira_retry_record_success_handle(uint64_t retry_handle);\n");
    }
    if program_uses_op(program, "retry_next_delay_ms") {
        out.push_str("extern uint32_t mira_retry_next_delay_ms_handle(uint64_t retry_handle);\n");
    }
    if program_uses_op(program, "retry_exhausted") {
        out.push_str("extern bool mira_retry_exhausted_handle(uint64_t retry_handle);\n");
    }
    if program_uses_op(program, "retry_close") {
        out.push_str("extern bool mira_retry_close_handle(uint64_t retry_handle);\n");
    }
    if program_uses_op(program, "circuit_open") {
        out.push_str("extern uint64_t mira_circuit_open_handle(uint32_t threshold, uint32_t cooldown_ms);\n");
    }
    if program_uses_op(program, "circuit_allow") {
        out.push_str("extern bool mira_circuit_allow_handle(uint64_t circuit_handle);\n");
    }
    if program_uses_op(program, "circuit_record_failure") {
        out.push_str("extern bool mira_circuit_record_failure_handle(uint64_t circuit_handle);\n");
    }
    if program_uses_op(program, "circuit_record_success") {
        out.push_str("extern bool mira_circuit_record_success_handle(uint64_t circuit_handle);\n");
    }
    if program_uses_op(program, "circuit_state") {
        out.push_str("extern uint32_t mira_circuit_state_handle(uint64_t circuit_handle);\n");
    }
    if program_uses_op(program, "circuit_close") {
        out.push_str("extern bool mira_circuit_close_handle(uint64_t circuit_handle);\n");
    }
    if program_uses_op(program, "backpressure_open") {
        out.push_str("extern uint64_t mira_backpressure_open_handle(uint32_t limit);\n");
    }
    if program_uses_op(program, "backpressure_acquire") {
        out.push_str("extern bool mira_backpressure_acquire_handle(uint64_t backpressure_handle);\n");
    }
    if program_uses_op(program, "backpressure_release") {
        out.push_str("extern bool mira_backpressure_release_handle(uint64_t backpressure_handle);\n");
    }
    if program_uses_op(program, "backpressure_saturated") {
        out.push_str("extern bool mira_backpressure_saturated_handle(uint64_t backpressure_handle);\n");
    }
    if program_uses_op(program, "backpressure_close") {
        out.push_str("extern bool mira_backpressure_close_handle(uint64_t backpressure_handle);\n");
    }
    if program_uses_op(program, "supervisor_open") {
        out.push_str("extern uint64_t mira_supervisor_open_handle(uint32_t restart_budget, uint32_t degrade_after);\n");
    }
    if program_uses_op(program, "supervisor_record_failure") {
        out.push_str("extern bool mira_supervisor_record_failure_handle(uint64_t supervisor_handle, uint32_t code);\n");
    }
    if program_uses_op(program, "supervisor_record_recovery") {
        out.push_str("extern bool mira_supervisor_record_recovery_handle(uint64_t supervisor_handle);\n");
    }
    if program_uses_op(program, "supervisor_should_restart") {
        out.push_str("extern bool mira_supervisor_should_restart_handle(uint64_t supervisor_handle);\n");
    }
    if program_uses_op(program, "supervisor_degraded") {
        out.push_str("extern bool mira_supervisor_degraded_handle(uint64_t supervisor_handle);\n");
    }
    if program_uses_op(program, "supervisor_close") {
        out.push_str("extern bool mira_supervisor_close_handle(uint64_t supervisor_handle);\n");
    }
    if program_uses_op(program, "task_sleep_ms") {
        out.push_str("extern bool mira_task_sleep_ms(uint32_t millis);\n");
    }
    if program_uses_op(program, "task_open") {
        out.push_str("extern uint64_t mira_task_open_handle(const char* command);\n");
    }
    if program_uses_op(program, "task_done") {
        out.push_str("extern bool mira_task_done_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "task_join") {
        out.push_str("extern int32_t mira_task_join_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "task_stdout_all") {
        out.push_str("extern buf_u8 mira_task_stdout_all_handle_buf_u8(uint64_t handle);\n");
    }
    if program_uses_op(program, "task_stderr_all") {
        out.push_str("extern buf_u8 mira_task_stderr_all_handle_buf_u8(uint64_t handle);\n");
    }
    if program_uses_op(program, "task_close") {
        out.push_str("extern bool mira_task_close_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "spawn_open") {
        out.push_str("extern uint64_t mira_spawn_open_handle(const char* command);\n");
    }
    if program_uses_op(program, "spawn_wait") {
        out.push_str("extern int32_t mira_spawn_wait_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "spawn_stdout_all") {
        out.push_str("extern buf_u8 mira_spawn_stdout_all_handle_buf_u8(uint64_t handle);\n");
    }
    if program_uses_op(program, "spawn_stderr_all") {
        out.push_str("extern buf_u8 mira_spawn_stderr_all_handle_buf_u8(uint64_t handle);\n");
    }
    if program_uses_op(program, "spawn_stdin_write_all") {
        out.push_str("extern bool mira_spawn_stdin_write_all_handle(uint64_t handle, buf_u8 value);\n");
    }
    if program_uses_op(program, "spawn_stdin_close") {
        out.push_str("extern bool mira_spawn_stdin_close_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "spawn_done") {
        out.push_str("extern bool mira_spawn_done_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "spawn_exit_ok") {
        out.push_str("extern bool mira_spawn_exit_ok_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "spawn_kill") {
        out.push_str("extern bool mira_spawn_kill_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "spawn_close") {
        out.push_str("extern bool mira_spawn_close_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "ffi_open_lib") {
        out.push_str("extern uint64_t mira_ffi_open_lib_handle(const char* path);\n");
    }
    if program_uses_op(program, "ffi_close_lib") {
        out.push_str("extern bool mira_ffi_close_lib_handle(uint64_t handle);\n");
    }
    if program_uses_op(program, "ffi_buf_ptr") {
        out.push_str("extern uint64_t mira_ffi_buf_ptr_buf_u8(buf_u8 value);\n");
    }
    if program_uses_op(program, "ffi_call_lib") {
        out.push_str(
            "extern uint64_t mira_ffi_call_lib_u64(uint64_t handle, const char* symbol, uint32_t argc, const uint64_t* argv);\n",
        );
    }
    if program_uses_op(program, "ffi_call_lib_cstr") {
        out.push_str(
            "extern uint64_t mira_ffi_call_lib_cstr_u64(uint64_t handle, const char* symbol, const char* arg);\n",
        );
    }
    out.push('\n');
}

fn emit_spawn_helpers(out: &mut String, include_buf_u8: bool) {
    out.push_str("static inline int32_t mira_spawn_status(const char* command) {\n");
    out.push_str("  int status = system(command);\n");
    out.push_str("  if (status == -1) {\n");
    out.push_str("    return -1;\n");
    out.push_str("  }\n");
    out.push_str("#ifdef WIFEXITED\n");
    out.push_str("  if (WIFEXITED(status)) {\n");
    out.push_str("    return (int32_t) WEXITSTATUS(status);\n");
    out.push_str("  }\n");
    out.push_str("#endif\n");
    out.push_str("  return (int32_t) status;\n");
    out.push_str("}\n\n");
    if !include_buf_u8 {
        return;
    }
    out.push_str("static inline buf_u8 mira_spawn_capture_buf_u8(const char* command) {\n");
    out.push_str("#ifdef _WIN32\n");
    out.push_str("  FILE* pipe = _popen(command, \"rb\");\n");
    out.push_str("#else\n");
    out.push_str("  FILE* pipe = popen(command, \"r\");\n");
    out.push_str("#endif\n");
    out.push_str("  if (pipe == NULL) {\n");
    out.push_str("    return mira_alloc_heap_buf_u8(0u);\n");
    out.push_str("  }\n");
    out.push_str("  size_t capacity = 256u;\n");
    out.push_str("  size_t len = 0u;\n");
    out.push_str("  uint8_t* data = (uint8_t*) malloc(capacity);\n");
    out.push_str("  if (data == NULL) { abort(); }\n");
    out.push_str("  for (;;) {\n");
    out.push_str("    if (len == capacity) {\n");
    out.push_str("      capacity *= 2u;\n");
    out.push_str("      uint8_t* grown = (uint8_t*) realloc(data, capacity);\n");
    out.push_str("      if (grown == NULL) { free(data); abort(); }\n");
    out.push_str("      data = grown;\n");
    out.push_str("    }\n");
    out.push_str("    size_t read_n = fread(data + len, 1u, capacity - len, pipe);\n");
    out.push_str("    len += read_n;\n");
    out.push_str("    if (read_n == 0u) {\n");
    out.push_str("      break;\n");
    out.push_str("    }\n");
    out.push_str("  }\n");
    out.push_str("#ifdef _WIN32\n");
    out.push_str("  _pclose(pipe);\n");
    out.push_str("#else\n");
    out.push_str("  pclose(pipe);\n");
    out.push_str("#endif\n");
    out.push_str("  if (len == 0u) {\n");
    out.push_str("    free(data);\n");
    out.push_str("    return mira_alloc_heap_buf_u8(0u);\n");
    out.push_str("  }\n");
    out.push_str("  uint8_t* exact = (uint8_t*) realloc(data, len);\n");
    out.push_str("  if (exact == NULL) {\n");
    out.push_str("    exact = data;\n");
    out.push_str("  }\n");
    out.push_str(
        "  return (buf_u8){ .data = exact, .len = (uint32_t) len, .region = MIRA_REGION_HEAP };\n",
    );
    out.push_str("}\n\n");
}

fn emit_net_helpers(out: &mut String, include_buf_u8: bool) {
    out.push_str("static inline int mira_net_open_fd(const char* host, uint16_t port) {\n");
    out.push_str("  char port_buf[16];\n");
    out.push_str("  snprintf(port_buf, sizeof(port_buf), \"%u\", (unsigned) port);\n");
    out.push_str("  struct addrinfo hints;\n");
    out.push_str("  memset(&hints, 0, sizeof(hints));\n");
    out.push_str("  hints.ai_family = AF_UNSPEC;\n");
    out.push_str("  hints.ai_socktype = SOCK_STREAM;\n");
    out.push_str("  struct addrinfo* result = NULL;\n");
    out.push_str("  if (getaddrinfo(host, port_buf, &hints, &result) != 0) {\n");
    out.push_str("    return -1;\n");
    out.push_str("  }\n");
    out.push_str("  int fd = -1;\n");
    out.push_str("  for (struct addrinfo* it = result; it != NULL; it = it->ai_next) {\n");
    out.push_str("    int candidate = socket(it->ai_family, it->ai_socktype, it->ai_protocol);\n");
    out.push_str("    if (candidate < 0) {\n");
    out.push_str("      continue;\n");
    out.push_str("    }\n");
    out.push_str("    if (connect(candidate, it->ai_addr, it->ai_addrlen) == 0) {\n");
    out.push_str("      fd = candidate;\n");
    out.push_str("      break;\n");
    out.push_str("    }\n");
    out.push_str("    close(candidate);\n");
    out.push_str("  }\n");
    out.push_str("  freeaddrinfo(result);\n");
    out.push_str("  return fd;\n");
    out.push_str("}\n\n");
    out.push_str(
        "static inline bool mira_net_send_all_fd(int fd, const uint8_t* data, uint32_t len) {\n",
    );
    out.push_str("  size_t sent = 0u;\n");
    out.push_str("  while (sent < (size_t) len) {\n");
    out.push_str("    ssize_t wrote = send(fd, data + sent, (size_t) len - sent, 0);\n");
    out.push_str("    if (wrote <= 0) {\n");
    out.push_str("      return false;\n");
    out.push_str("    }\n");
    out.push_str("    sent += (size_t) wrote;\n");
    out.push_str("  }\n");
    out.push_str("  return true;\n");
    out.push_str("}\n\n");
    out.push_str("static inline bool mira_net_connect_ok(const char* host, uint16_t port) {\n");
    out.push_str("  int fd = mira_net_open_fd(host, port);\n");
    out.push_str("  if (fd < 0) {\n");
    out.push_str("    return false;\n");
    out.push_str("  }\n");
    out.push_str("  close(fd);\n");
    out.push_str("  return true;\n");
    out.push_str("}\n\n");
    if include_buf_u8 {
        out.push_str("static inline bool mira_net_write_all_buf_u8(const char* host, uint16_t port, buf_u8 value) {\n");
        out.push_str("  int fd = mira_net_open_fd(host, port);\n");
        out.push_str("  if (fd < 0) {\n");
        out.push_str("    return false;\n");
        out.push_str("  }\n");
        out.push_str("  bool ok = mira_net_send_all_fd(fd, value.data, value.len);\n");
        out.push_str("  shutdown(fd, SHUT_WR);\n");
        out.push_str("  close(fd);\n");
        out.push_str("  return ok;\n");
        out.push_str("}\n\n");
        out.push_str("static inline buf_u8 mira_net_exchange_all_buf_u8(const char* host, uint16_t port, buf_u8 value) {\n");
        out.push_str("  int fd = mira_net_open_fd(host, port);\n");
        out.push_str("  if (fd < 0) {\n");
        out.push_str("    return mira_alloc_heap_buf_u8(0u);\n");
        out.push_str("  }\n");
        out.push_str("  if (!mira_net_send_all_fd(fd, value.data, value.len)) {\n");
        out.push_str("    close(fd);\n");
        out.push_str("    return mira_alloc_heap_buf_u8(0u);\n");
        out.push_str("  }\n");
        out.push_str("  shutdown(fd, SHUT_WR);\n");
        out.push_str("  size_t capacity = 256u;\n");
        out.push_str("  size_t len = 0u;\n");
        out.push_str("  uint8_t* data = (uint8_t*) malloc(capacity);\n");
        out.push_str("  if (data == NULL) { close(fd); abort(); }\n");
        out.push_str("  for (;;) {\n");
        out.push_str("    if (len == capacity) {\n");
        out.push_str("      capacity *= 2u;\n");
        out.push_str("      uint8_t* grown = (uint8_t*) realloc(data, capacity);\n");
        out.push_str("      if (grown == NULL) { free(data); close(fd); abort(); }\n");
        out.push_str("      data = grown;\n");
        out.push_str("    }\n");
        out.push_str("    ssize_t read_n = recv(fd, data + len, capacity - len, 0);\n");
        out.push_str("    if (read_n == 0) {\n");
        out.push_str("      break;\n");
        out.push_str("    }\n");
        out.push_str("    if (read_n < 0) {\n");
        out.push_str("      free(data);\n");
        out.push_str("      close(fd);\n");
        out.push_str("      return mira_alloc_heap_buf_u8(0u);\n");
        out.push_str("    }\n");
        out.push_str("    len += (size_t) read_n;\n");
        out.push_str("  }\n");
        out.push_str("  close(fd);\n");
        out.push_str("  if (len == 0u) {\n");
        out.push_str("    free(data);\n");
        out.push_str("    return mira_alloc_heap_buf_u8(0u);\n");
        out.push_str("  }\n");
        out.push_str("  uint8_t* exact = (uint8_t*) realloc(data, len);\n");
        out.push_str("  if (exact == NULL) {\n");
        out.push_str("    exact = data;\n");
        out.push_str("  }\n");
        out.push_str("  return (buf_u8){ .data = exact, .len = (uint32_t) len, .region = MIRA_REGION_HEAP };\n");
        out.push_str("}\n\n");
        out.push_str("static inline buf_u8 mira_net_serve_exchange_all_buf_u8(const char* host, uint16_t port, buf_u8 response) {\n");
        out.push_str("  (void) host;\n");
        out.push_str("  int fd = socket(AF_INET, SOCK_STREAM, 0);\n");
        out.push_str("  if (fd < 0) { return mira_alloc_heap_buf_u8(0u); }\n");
        out.push_str("  int reuse = 1;\n");
        out.push_str("  setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));\n");
        out.push_str("  struct sockaddr_in addr;\n");
        out.push_str("  memset(&addr, 0, sizeof(addr));\n");
        out.push_str("  addr.sin_family = AF_INET;\n");
        out.push_str("  addr.sin_port = htons(port);\n");
        out.push_str("  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);\n");
        out.push_str("  if (bind(fd, (struct sockaddr*) &addr, sizeof(addr)) != 0) { close(fd); return mira_alloc_heap_buf_u8(0u); }\n");
        out.push_str(
            "  if (listen(fd, 1) != 0) { close(fd); return mira_alloc_heap_buf_u8(0u); }\n",
        );
        out.push_str("  int client = accept(fd, NULL, NULL);\n");
        out.push_str("  close(fd);\n");
        out.push_str("  if (client < 0) { return mira_alloc_heap_buf_u8(0u); }\n");
        out.push_str("  size_t capacity = 256u;\n");
        out.push_str("  size_t len = 0u;\n");
        out.push_str("  uint8_t* data = (uint8_t*) malloc(capacity);\n");
        out.push_str("  if (data == NULL) { close(client); abort(); }\n");
        out.push_str("  for (;;) {\n");
        out.push_str("    if (len == capacity) {\n");
        out.push_str("      capacity *= 2u;\n");
        out.push_str("      uint8_t* grown = (uint8_t*) realloc(data, capacity);\n");
        out.push_str("      if (grown == NULL) { free(data); close(client); abort(); }\n");
        out.push_str("      data = grown;\n");
        out.push_str("    }\n");
        out.push_str("    ssize_t read_n = recv(client, data + len, capacity - len, 0);\n");
        out.push_str("    if (read_n == 0) { break; }\n");
        out.push_str("    if (read_n < 0) { free(data); close(client); return mira_alloc_heap_buf_u8(0u); }\n");
        out.push_str("    len += (size_t) read_n;\n");
        out.push_str("  }\n");
        out.push_str(
            "  bool wrote_ok = mira_net_send_all_fd(client, response.data, response.len);\n",
        );
        out.push_str("  shutdown(client, SHUT_WR);\n");
        out.push_str("  close(client);\n");
        out.push_str("  if (!wrote_ok) { free(data); return mira_alloc_heap_buf_u8(0u); }\n");
        out.push_str("  if (len == 0u) { free(data); return mira_alloc_heap_buf_u8(0u); }\n");
        out.push_str("  uint8_t* exact = (uint8_t*) realloc(data, len);\n");
        out.push_str("  if (exact == NULL) { exact = data; }\n");
        out.push_str("  return (buf_u8){ .data = exact, .len = (uint32_t) len, .region = MIRA_REGION_HEAP };\n");
        out.push_str("}\n\n");
    }
}

fn collect_ffi_signatures(program: &Program) -> Result<BTreeSet<FfiSignature>, String> {
    let mut out = BTreeSet::new();
    let mut by_symbol = BTreeMap::<String, FfiSignature>::new();
    for function in &program.functions {
        for block in &function.blocks {
            let type_env = build_type_env(function, block);
            for instruction in &block.instructions {
                if instruction.op != "ffi_call" && instruction.op != "ffi_call_cstr" {
                    continue;
                }
                let signature = ffi_signature_for_instruction(instruction, &type_env)?;
                if let Some(existing) = by_symbol.get(&signature.symbol) {
                    if existing != &signature {
                        return Err(format!(
                            "ffi symbol {} is used with conflicting signatures",
                            signature.symbol
                        ));
                    }
                } else {
                    by_symbol.insert(signature.symbol.clone(), signature.clone());
                }
                out.insert(signature);
            }
        }
    }
    Ok(out)
}

fn ffi_signature_for_instruction(
    instruction: &crate::ast::Instruction,
    type_env: &HashMap<String, TypeRef>,
) -> Result<FfiSignature, String> {
    let symbol = instruction
        .args
        .first()
        .ok_or_else(|| "ffi_call requires a symbol".to_string())?;
    if !is_valid_ffi_symbol(symbol) {
        return Err(format!("invalid ffi symbol {symbol}"));
    }
    let mut args = Vec::new();
    if instruction.op == "ffi_call_cstr" {
        let operand = instruction
            .args
            .get(1)
            .ok_or_else(|| "ffi_call_cstr requires one buf[u8] operand".to_string())?;
        let ty = resolve_operand_type(operand, type_env)
            .ok_or_else(|| format!("unknown ffi_call_cstr operand type for {operand}"))?;
        if !matches!(
            &ty,
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
            return Err(format!("ffi_call_cstr requires buf[u8] operand, got {ty}"));
        }
        args.push(FfiArgSignature::CStr);
    } else {
        for operand in instruction.args.iter().skip(1) {
            let ty = resolve_operand_type(operand, type_env)
                .ok_or_else(|| format!("unknown ffi operand type for {operand}"))?;
            args.push(FfiArgSignature::Scalar(ty));
        }
    }
    Ok(FfiSignature {
        symbol: symbol.clone(),
        ret: instruction.ty.clone(),
        args,
    })
}

fn resolve_operand_type(token: &str, env: &HashMap<String, TypeRef>) -> Option<TypeRef> {
    env.get(token)
        .cloned()
        .or_else(|| crate::types::infer_literal_type(token))
}

fn render_len_expr(collection: &str, collection_ty: &TypeRef) -> Result<String, String> {
    match collection_ty {
        TypeRef::Span(_) | TypeRef::Buf(_) => Ok(format!("{}.len", collection)),
        TypeRef::Vec { len, .. } => Ok(format!("((uint32_t) {})", len)),
        TypeRef::Own(inner) | TypeRef::View(inner) | TypeRef::Edit(inner) => {
            render_len_expr(collection, inner)
        }
        _ => Err(format!("len is not implemented for {collection_ty}")),
    }
}

fn render_load_expr(
    collection: &str,
    index: &str,
    collection_ty: &TypeRef,
) -> Result<String, String> {
    match collection_ty {
        TypeRef::Span(_) | TypeRef::Buf(_) | TypeRef::Vec { .. } => {
            Ok(format!("{}.data[{}]", collection, index))
        }
        TypeRef::Own(inner) | TypeRef::View(inner) | TypeRef::Edit(inner) => {
            render_load_expr(collection, index, inner)
        }
        _ => Err(format!("load is not implemented for {collection_ty}")),
    }
}

fn render_make_expr(
    instruction: &crate::ast::Instruction,
    env: &HashMap<String, String>,
    named_types: &HashMap<String, TypeDeclBody>,
) -> Result<String, String> {
    let TypeRef::Named(type_name) = &instruction.ty else {
        return Err("make requires a named result type".to_string());
    };
    match named_types.get(type_name) {
        Some(TypeDeclBody::Struct { fields }) => {
            let rendered_fields = fields
                .iter()
                .zip(instruction.args.iter().skip(1))
                .map(|(field, operand)| {
                    Ok(format!(
                        ".{} = {}",
                        field.name,
                        render_operand(operand, env, Some(&field.ty), named_types)?
                    ))
                })
                .collect::<Result<Vec<_>, String>>()?;
            Ok(format!(
                "(({}){{ {} }})",
                instruction.ty.c_type()?,
                rendered_fields.join(", ")
            ))
        }
        Some(TypeDeclBody::Enum { variants }) => {
            let variant_name = instruction
                .args
                .get(1)
                .ok_or_else(|| format!("make enum requires variant name for {type_name}"))?;
            let variant = variants
                .iter()
                .find(|variant| &variant.name == variant_name)
                .ok_or_else(|| format!("unknown variant {variant_name} on {type_name}"))?;
            if enum_has_payload(variants) {
                if variant.fields.is_empty() {
                    Ok(format!(
                        "(({}){{ .tag = {}, .payload = {{0}} }})",
                        instruction.ty.c_type()?,
                        enum_tag_constant(type_name, variant_name)
                    ))
                } else {
                    let rendered_fields = variant
                        .fields
                        .iter()
                        .zip(instruction.args.iter().skip(2))
                        .map(|(field, operand)| {
                            Ok(format!(
                                ".{} = {}",
                                field.name,
                                render_operand(operand, env, Some(&field.ty), named_types)?
                            ))
                        })
                        .collect::<Result<Vec<_>, String>>()?;
                    Ok(format!(
                        "(({}){{ .tag = {}, .payload.{} = {{ {} }} }})",
                        instruction.ty.c_type()?,
                        enum_tag_constant(type_name, variant_name),
                        sanitize_identifier(variant_name),
                        rendered_fields.join(", ")
                    ))
                }
            } else {
                Ok(enum_tag_constant(type_name, variant_name))
            }
        }
        None => Err(format!("make requires a declared named type {type_name}")),
    }
}

fn collect_lowered_types(program: &Program) -> Result<BTreeSet<TypeRef>, String> {
    let mut set = BTreeSet::new();
    for item in &program.types {
        match &item.body {
            TypeDeclBody::Struct { fields } => {
                for field in fields {
                    collect_type_recursive(&field.ty, &mut set);
                }
            }
            TypeDeclBody::Enum { variants } => {
                for variant in variants {
                    for field in &variant.fields {
                        collect_type_recursive(&field.ty, &mut set);
                    }
                }
            }
        }
    }
    for item in &program.consts {
        collect_type_recursive(&item.ty, &mut set);
    }
    for function in &program.functions {
        for arg in &function.args {
            collect_type_recursive(&arg.ty, &mut set);
        }
        collect_type_recursive(&function.ret, &mut set);
        for block in &function.blocks {
            for param in &block.params {
                collect_type_recursive(&param.ty, &mut set);
            }
            for instruction in &block.instructions {
                collect_type_recursive(&instruction.ty, &mut set);
            }
        }
    }
    Ok(set)
}

fn collect_type_recursive(ty: &TypeRef, out: &mut BTreeSet<TypeRef>) {
    match ty {
        TypeRef::Span(inner)
        | TypeRef::Buf(inner)
        | TypeRef::Option(inner)
        | TypeRef::Own(inner)
        | TypeRef::View(inner)
        | TypeRef::Edit(inner) => {
            collect_type_recursive(inner, out);
            out.insert(ty.clone());
        }
        TypeRef::Vec { elem, .. } => {
            collect_type_recursive(elem, out);
            out.insert(ty.clone());
        }
        TypeRef::Result { ok, err } => {
            collect_type_recursive(ok, out);
            collect_type_recursive(err, out);
            out.insert(ty.clone());
        }
        _ => {}
    }
}

fn collect_sat_types(program: &Program) -> Result<BTreeSet<TypeRef>, String> {
    let mut set = BTreeSet::new();
    for function in &program.functions {
        for block in &function.blocks {
            for instruction in &block.instructions {
                if instruction.op == "sat_add" {
                    set.insert(instruction.ty.clone());
                }
            }
        }
    }
    Ok(set)
}

fn collect_runtime_buf_types(program: &Program) -> Result<BTreeSet<TypeRef>, String> {
    let mut set = BTreeSet::new();
    for ty in collect_lowered_types(program)? {
        if matches!(ty, TypeRef::Buf(_)) {
            set.insert(ty);
        }
    }
    Ok(set)
}

fn emit_runtime_buf_helpers(out: &mut String, types: &BTreeSet<TypeRef>) -> Result<(), String> {
    out.push_str("#define MIRA_REGION_STACK 1u\n");
    out.push_str("#define MIRA_REGION_HEAP 2u\n");
    out.push_str("#define MIRA_REGION_ARENA 3u\n\n");
    out.push_str("typedef struct mira_arena_chunk { void* ptr; struct mira_arena_chunk* next; } mira_arena_chunk;\n");
    out.push_str("typedef struct { mira_arena_chunk* head; } mira_arena_runtime;\n\n");
    out.push_str(
        "static inline void* mira_arena_alloc_bytes(mira_arena_runtime* arena, size_t bytes) {\n",
    );
    out.push_str("  if (bytes == 0u) { return NULL; }\n");
    out.push_str("  void* data = calloc(1u, bytes);\n");
    out.push_str("  if (data == NULL) { abort(); }\n");
    out.push_str(
        "  mira_arena_chunk* chunk = (mira_arena_chunk*) malloc(sizeof(mira_arena_chunk));\n",
    );
    out.push_str("  if (chunk == NULL) { free(data); abort(); }\n");
    out.push_str("  chunk->ptr = data;\n");
    out.push_str("  chunk->next = arena->head;\n");
    out.push_str("  arena->head = chunk;\n");
    out.push_str("  return data;\n");
    out.push_str("}\n\n");
    out.push_str("static inline void mira_arena_release(mira_arena_runtime* arena) {\n");
    out.push_str("  mira_arena_chunk* chunk = arena->head;\n");
    out.push_str("  while (chunk != NULL) {\n");
    out.push_str("    mira_arena_chunk* next = chunk->next;\n");
    out.push_str("    free(chunk->ptr);\n");
    out.push_str("    free(chunk);\n");
    out.push_str("    chunk = next;\n");
    out.push_str("  }\n");
    out.push_str("  arena->head = NULL;\n");
    out.push_str("}\n\n");
    for ty in types {
        let TypeRef::Buf(inner) = ty else {
            continue;
        };
        let c_ty = ty.c_type()?;
        let elem_c_ty = inner.c_type()?;
        let key = runtime_buf_key(ty)?;
        out.push_str(&format!(
            "static inline {c_ty} mira_alloc_heap_{key}(uint32_t len) {{\n  {elem_c_ty}* data = len == 0u ? NULL : ({elem_c_ty}*) calloc((size_t) len, sizeof({elem_c_ty}));\n  if (len != 0u && data == NULL) {{ abort(); }}\n  return ({c_ty}){{ .data = data, .len = len, .region = MIRA_REGION_HEAP }};\n}}\n\n"
        ));
        out.push_str(&format!(
            "static inline {c_ty} mira_alloc_stack_{key}(uint32_t len) {{\n  {elem_c_ty}* data = len == 0u ? NULL : ({elem_c_ty}*) alloca(sizeof({elem_c_ty}) * (size_t) len);\n  return ({c_ty}){{ .data = data, .len = len, .region = MIRA_REGION_STACK }};\n}}\n\n"
        ));
        out.push_str(&format!(
            "static inline {c_ty} mira_alloc_arena_{key}(mira_arena_runtime* arena, uint32_t len) {{\n  {elem_c_ty}* data = len == 0u ? NULL : ({elem_c_ty}*) mira_arena_alloc_bytes(arena, sizeof({elem_c_ty}) * (size_t) len);\n  return ({c_ty}){{ .data = data, .len = len, .region = MIRA_REGION_ARENA }};\n}}\n\n"
        ));
        out.push_str(&format!(
            "static inline {c_ty} mira_store_{key}({c_ty} buf, uint32_t index, {elem_c_ty} value) {{\n  if (index >= buf.len) {{ abort(); }}\n  buf.data[index] = value;\n  return buf;\n}}\n\n"
        ));
        out.push_str(&format!(
            "static inline bool mira_drop_{key}({c_ty} buf) {{\n  if (buf.region == MIRA_REGION_HEAP && buf.data != NULL) {{ free(buf.data); }}\n  return true;\n}}\n\n"
        ));
    }
    Ok(())
}

fn emit_clock_helpers(out: &mut String) {
    out.push_str("static inline uint64_t mira_clock_now_ns(void) {\n");
    out.push_str("  struct timespec ts;\n");
    out.push_str("  clock_gettime(CLOCK_MONOTONIC, &ts);\n");
    out.push_str("  return ((uint64_t) ts.tv_sec * 1000000000ULL) + (uint64_t) ts.tv_nsec;\n");
    out.push_str("}\n\n");
}

fn emit_rand_helpers(out: &mut String) {
    out.push_str("static inline uint32_t mira_rand_next_u32(uint32_t* state) {\n");
    out.push_str("  uint32_t x = *state;\n");
    out.push_str("  if (x == 0u) { x = 2463534242u; }\n");
    out.push_str("  x ^= x << 13;\n");
    out.push_str("  x ^= x >> 17;\n");
    out.push_str("  x ^= x << 5;\n");
    out.push_str("  *state = x;\n");
    out.push_str("  return x;\n");
    out.push_str("}\n\n");
}

fn emit_fs_helpers(out: &mut String, include_buf_u8: bool) {
    out.push_str("static inline uint32_t mira_fs_read_u32(const char* path) {\n");
    out.push_str("  FILE* file = fopen(path, \"r\");\n");
    out.push_str("  if (file == NULL) { return 0u; }\n");
    out.push_str("  uint32_t value = 0u;\n");
    out.push_str("  if (fscanf(file, \"%\" SCNu32, &value) != 1) {\n");
    out.push_str("    fclose(file);\n");
    out.push_str("    return 0u;\n");
    out.push_str("  }\n");
    out.push_str("  fclose(file);\n");
    out.push_str("  return value;\n");
    out.push_str("}\n\n");
    out.push_str("static inline bool mira_fs_write_u32(const char* path, uint32_t value) {\n");
    out.push_str("  FILE* file = fopen(path, \"w\");\n");
    out.push_str("  if (file == NULL) { return false; }\n");
    out.push_str("  int wrote = fprintf(file, \"%\" PRIu32 \"\\n\", value);\n");
    out.push_str("  int closed = fclose(file);\n");
    out.push_str("  return wrote > 0 && closed == 0;\n");
    out.push_str("}\n\n");
    if !include_buf_u8 {
        return;
    }
    out.push_str("static inline buf_u8 mira_fs_read_all_buf_u8(const char* path) {\n");
    out.push_str("  FILE* file = fopen(path, \"rb\");\n");
    out.push_str("  if (file == NULL) { return (buf_u8){ .data = NULL, .len = 0u, .region = MIRA_REGION_HEAP }; }\n");
    out.push_str("  if (fseek(file, 0, SEEK_END) != 0) { fclose(file); return (buf_u8){ .data = NULL, .len = 0u, .region = MIRA_REGION_HEAP }; }\n");
    out.push_str("  long raw_size = ftell(file);\n");
    out.push_str("  if (raw_size < 0) { fclose(file); return (buf_u8){ .data = NULL, .len = 0u, .region = MIRA_REGION_HEAP }; }\n");
    out.push_str("  if (fseek(file, 0, SEEK_SET) != 0) { fclose(file); return (buf_u8){ .data = NULL, .len = 0u, .region = MIRA_REGION_HEAP }; }\n");
    out.push_str("  uint32_t len = (uint32_t) raw_size;\n");
    out.push_str("  buf_u8 out_buf = mira_alloc_heap_buf_u8(len);\n");
    out.push_str("  if (len != 0u) {\n");
    out.push_str(
        "    size_t read_count = fread(out_buf.data, sizeof(uint8_t), (size_t) len, file);\n",
    );
    out.push_str("    if (read_count != (size_t) len) {\n");
    out.push_str("      if (out_buf.data != NULL) { free(out_buf.data); }\n");
    out.push_str("      fclose(file);\n");
    out.push_str("      return (buf_u8){ .data = NULL, .len = 0u, .region = MIRA_REGION_HEAP };\n");
    out.push_str("    }\n");
    out.push_str("  }\n");
    out.push_str("  fclose(file);\n");
    out.push_str("  return out_buf;\n");
    out.push_str("}\n\n");
    out.push_str("static inline bool mira_fs_write_all_buf_u8(const char* path, buf_u8 value) {\n");
    out.push_str("  FILE* file = fopen(path, \"wb\");\n");
    out.push_str("  if (file == NULL) { return false; }\n");
    out.push_str("  size_t wrote = value.len == 0u ? 0u : fwrite(value.data, sizeof(uint8_t), (size_t) value.len, file);\n");
    out.push_str("  int closed = fclose(file);\n");
    out.push_str("  return wrote == (size_t) value.len && closed == 0;\n");
    out.push_str("}\n\n");
}

fn runtime_buf_key(ty: &TypeRef) -> Result<String, String> {
    match ty {
        TypeRef::Buf(inner) => Ok(format!("buf_{}", inner.type_key()?)),
        TypeRef::String => Ok("buf_u8".to_string()),
        _ => Err(format!("runtime buffer helper requires buf[T], got {ty}")),
    }
}

fn runtime_buf_key_from_wrapper(ty: &TypeRef) -> Result<String, String> {
    match ty {
        TypeRef::Own(inner) | TypeRef::View(inner) | TypeRef::Edit(inner) => runtime_buf_key(inner),
        TypeRef::Buf(_) => runtime_buf_key(ty),
        _ => Err(format!(
            "runtime buffer helper requires buf[T] wrapper, got {ty}"
        )),
    }
}

fn runtime_buf_elem_type(ty: &TypeRef) -> Option<TypeRef> {
    match ty {
        TypeRef::Buf(inner) => Some((**inner).clone()),
        TypeRef::Own(inner) | TypeRef::View(inner) | TypeRef::Edit(inner) => {
            runtime_buf_elem_type(inner)
        }
        _ => None,
    }
}

fn function_uses_arena(function: &Function) -> bool {
    function.blocks.iter().any(|block| {
        block.instructions.iter().any(|instruction| {
            instruction.op == "alloc"
                && instruction
                    .args
                    .first()
                    .map(|region| region == "arena")
                    .unwrap_or(false)
        })
    })
}

fn function_uses_op(function: &Function, op: &str) -> bool {
    function.blocks.iter().any(|block| {
        block
            .instructions
            .iter()
            .any(|instruction| instruction.op == op)
    })
}

fn program_uses_op(program: &Program, op: &str) -> bool {
    program
        .functions
        .iter()
        .any(|function| function_uses_op(function, op))
}

fn rand_seed_for_function(function: &Function) -> Result<u32, String> {
    let payload = capability_payload(function, "rand")
        .ok_or_else(|| format!("function {} is missing rand capability", function.name))?;
    parse_rand_seed_payload(payload).ok_or_else(|| {
        format!(
            "function {} requires rand capability payload seed=<u32>",
            function.name
        )
    })
}

fn fs_path_for_function(function: &Function) -> Result<String, String> {
    let payload = capability_payload(function, "fs")
        .ok_or_else(|| format!("function {} is missing fs capability", function.name))?;
    parse_fs_path_payload(payload)
        .map(|path| path.to_string())
        .ok_or_else(|| {
            format!(
                "function {} requires fs capability with a non-empty path",
                function.name
            )
        })
}

fn capability_payload<'a>(function: &'a Function, kind: &str) -> Option<&'a str> {
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

fn parse_config_capability_payload(payload: &str) -> Option<HashMap<String, String>> {
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

fn config_entry_for_function(function: &Function, key: &str) -> Result<String, String> {
    let payload = capability_payload(function, "config")
        .ok_or_else(|| format!("missing config capability for key {key}"))?;
    let entries = parse_config_capability_payload(payload)
        .ok_or_else(|| format!("invalid config capability payload for key {key}"))?;
    entries
        .get(key)
        .cloned()
        .ok_or_else(|| format!("missing config entry {key}"))
}

fn service_name_for_function(function: &Function) -> Result<&str, String> {
    let payload = capability_payload(function, "service")
        .ok_or_else(|| format!("function {} is missing service capability", function.name))?;
    let normalized = normalize_capability_payload(payload);
    if normalized.is_empty() {
        return Err(format!(
            "function {} requires non-empty service capability payload",
            function.name
        ));
    }
    Ok(normalized)
}

fn parse_rand_seed_payload(payload: &str) -> Option<u32> {
    let normalized = normalize_capability_payload(payload);
    let seed_text = normalized.strip_prefix("seed=")?;
    let seed_text = seed_text.strip_suffix("u32").unwrap_or(seed_text);
    seed_text.parse::<u32>().ok()
}

fn parse_fs_path_payload(payload: &str) -> Option<&str> {
    let normalized = normalize_capability_payload(payload);
    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }
}

struct TlsCapability {
    cert: String,
    key: String,
    request_timeout_ms: u32,
    session_timeout_ms: u32,
    shutdown_grace_ms: u32,
}

fn net_endpoint_for_function(function: &Function) -> Result<(String, u16), String> {
    let payload = capability_payload(function, "net")
        .ok_or_else(|| format!("function {} is missing net capability", function.name))?;
    parse_net_endpoint_payload(payload).ok_or_else(|| {
        format!(
            "function {} requires net capability host:port",
            function.name
        )
    })
}

fn tls_capability_for_function(function: &Function) -> Result<TlsCapability, String> {
    let payload = capability_payload(function, "tls")
        .ok_or_else(|| format!("function {} is missing tls capability", function.name))?;
    parse_tls_capability_payload(payload).ok_or_else(|| {
        format!(
            "function {} requires tls capability cert=/path,key=/path",
            function.name
        )
    })
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

fn parse_tls_capability_payload(payload: &str) -> Option<TlsCapability> {
    let normalized = normalize_capability_payload(payload);
    if normalized.is_empty() {
        return None;
    }
    let mut cert = None;
    let mut key = None;
    let mut request_timeout_ms = 5000u32;
    let mut session_timeout_ms = 2000u32;
    let mut shutdown_grace_ms = 250u32;
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
            "request_timeout_ms" => request_timeout_ms = value.parse::<u32>().ok()?,
            "session_timeout_ms" => session_timeout_ms = value.parse::<u32>().ok()?,
            "shutdown_grace_ms" => shutdown_grace_ms = value.parse::<u32>().ok()?,
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

fn is_valid_ffi_symbol(symbol: &str) -> bool {
    let mut chars = symbol.chars();
    match chars.next() {
        Some(ch) if ch == '_' || ch.is_ascii_alphabetic() => {}
        _ => return false,
    }
    chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
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

fn is_valid_net_host(host: &str) -> bool {
    host.chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '-' | '_' | ':'))
}

fn render_c_string_literal(value: &str) -> String {
    let escaped = value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t");
    format!("\"{escaped}\"")
}

fn emit_type_decl(out: &mut String, ty: &TypeRef) -> Result<(), String> {
    match ty {
        TypeRef::Span(inner) => out.push_str(&format!(
            "typedef struct {{ const {}* data; uint32_t len; }} {};\n",
            inner.c_type()?,
            ty.c_type()?
        )),
        TypeRef::Buf(inner) => out.push_str(&format!(
            "typedef struct {{ {}* data; uint32_t len; uint8_t region; }} {};\n",
            inner.c_type()?,
            ty.c_type()?
        )),
        TypeRef::Vec { len, elem } => out.push_str(&format!(
            "typedef struct {{ {} data[{}]; }} {};\n",
            elem.c_type()?,
            len,
            ty.c_type()?
        )),
        TypeRef::Option(inner) => out.push_str(&format!(
            "typedef struct {{ bool has_value; {} value; }} {};\n",
            inner.c_type()?,
            ty.c_type()?
        )),
        TypeRef::Result { ok, err } => out.push_str(&format!(
            "typedef struct {{ bool is_ok; {} ok; {} err; }} {};\n",
            ok.c_type()?,
            err.c_type()?,
            ty.c_type()?
        )),
        TypeRef::Own(_) | TypeRef::View(_) | TypeRef::Edit(_) => {}
        _ => {}
    }
    Ok(())
}

fn emit_named_type_decl(out: &mut String, item: &crate::ast::TypeDecl) -> Result<(), String> {
    match &item.body {
        TypeDeclBody::Struct { fields } => {
            out.push_str(&format!("typedef struct {{ "));
            for field in fields {
                out.push_str(&format!("{} {}; ", field.ty.c_type()?, field.name));
            }
            out.push_str(&format!(
                "}} {};\n",
                TypeRef::Named(item.name.clone()).c_type()?
            ));
        }
        TypeDeclBody::Enum { variants } => {
            let tag_type = enum_tag_type(&item.name);
            out.push_str("typedef enum { ");
            for (index, variant) in variants.iter().enumerate() {
                if index > 0 {
                    out.push_str(", ");
                }
                out.push_str(&format!(
                    "{} = {}",
                    enum_tag_constant(&item.name, &variant.name),
                    index
                ));
            }
            out.push_str(&format!(" }} {};\n", tag_type));
            if enum_has_payload(variants) {
                out.push_str("typedef struct { ");
                out.push_str(&format!("{} tag; ", tag_type));
                out.push_str("union { ");
                for variant in variants.iter().filter(|variant| !variant.fields.is_empty()) {
                    out.push_str("struct { ");
                    for field in &variant.fields {
                        out.push_str(&format!("{} {}; ", field.ty.c_type()?, field.name));
                    }
                    out.push_str(&format!("}} {}; ", sanitize_identifier(&variant.name)));
                }
                out.push_str("char _empty; ");
                out.push_str(&format!(
                    "}} payload; }} {};\n",
                    TypeRef::Named(item.name.clone()).c_type()?
                ));
                for variant in variants.iter().filter(|variant| !variant.fields.is_empty()) {
                    for field in &variant.fields {
                        out.push_str(&format!(
                            "static inline {} mira_field_{}_{}_{}({} value) {{\n",
                            field.ty.c_type()?,
                            sanitize_identifier(&item.name),
                            sanitize_identifier(&variant.name),
                            sanitize_identifier(&field.name),
                            TypeRef::Named(item.name.clone()).c_type()?
                        ));
                        out.push_str(&format!(
                            "  if (value.tag != {}) {{ abort(); }}\n",
                            enum_tag_constant(&item.name, &variant.name)
                        ));
                        out.push_str(&format!(
                            "  return value.payload.{}.{};\n",
                            sanitize_identifier(&variant.name),
                            field.name
                        ));
                        out.push_str("}\n");
                    }
                }
            } else {
                out.push_str(&format!(
                    "typedef {} {};\n",
                    tag_type,
                    TypeRef::Named(item.name.clone()).c_type()?
                ));
            }
        }
    }
    Ok(())
}

fn emit_sat_helpers(out: &mut String, types: &BTreeSet<TypeRef>) -> Result<(), String> {
    for ty in types {
        match ty {
            TypeRef::Int {
                signed: true,
                bits: 64,
            } => {
                out.push_str(
                    "static inline int64_t mira_sat_add_i64(int64_t a, int64_t b) {\n  int64_t out_value;\n  if (__builtin_add_overflow(a, b, &out_value)) {\n    return b > 0 ? INT64_MAX : INT64_MIN;\n  }\n  return out_value;\n}\n\n",
                );
            }
            TypeRef::Int {
                signed: false,
                bits: 32,
            } => {
                out.push_str(
                    "static inline uint32_t mira_sat_add_u32(uint32_t a, uint32_t b) {\n  uint32_t out_value;\n  if (__builtin_add_overflow(a, b, &out_value)) {\n    return UINT32_MAX;\n  }\n  return out_value;\n}\n\n",
                );
            }
            _ => return Err(format!("sat_add helper is not implemented for {ty}")),
        }
    }
    Ok(())
}

fn lower_test(
    program: &Program,
    owner: &Function,
    case: &TestCase,
    named_types: &HashMap<String, TypeDeclBody>,
) -> Result<LoweredTest, String> {
    let target_name = case.call.clone().unwrap_or_else(|| owner.name.clone());
    let function = program
        .functions
        .iter()
        .find(|function| function.name == target_name)
        .ok_or_else(|| format!("unknown test target {}", target_name))?;
    let inputs = case
        .inputs
        .iter()
        .map(|(name, value)| {
            let ty = function
                .args
                .iter()
                .find(|arg| &arg.name == name)
                .ok_or_else(|| format!("unknown test input {name} for {}", function.name))?
                .ty
                .clone();
            Ok(LoweredTestInput {
                name: name.clone(),
                ty: ty.clone(),
                value: parse_data_literal(value, &ty, Some(named_types))?,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    let (declarations, call_args) = lower_call_arguments(
        &format!("test_{}_{}", owner.name, case.name),
        function,
        &inputs
            .iter()
            .map(|input| (input.name.clone(), input.value.clone()))
            .collect::<Vec<_>>(),
        named_types,
    )?;
    let expected_value = parse_data_literal(&case.expected, &function.ret, Some(named_types))?;
    let expected_expr =
        render_data_value_with_named_types(&expected_value, &function.ret, named_types)?;
    let result_name = format!("result_{}_{}", owner.name, case.name);
    let equality_expr =
        render_type_equality_expr(&result_name, &expected_expr, &function.ret, named_types)?;
    Ok(LoweredTest {
        owner: owner.name.clone(),
        name: case.name.clone(),
        function_name: function.name.clone(),
        inputs,
        expected: LoweredTestExpected {
            ty: function.ret.clone(),
            value: expected_value,
        },
        declarations,
        call: LoweredCall {
            ret_c_type: function.ret.c_type()?,
            result_name,
            function_name: function.name.clone(),
            args: call_args,
        },
        assertion: LoweredAssertion {
            condition: equality_expr,
            failure_message: format!("test {}.{} failed\\n", owner.name, case.name),
        },
    })
}

fn emit_lowered_test(test: &LoweredTest) -> String {
    let mut out = String::new();
    for declaration in &test.declarations {
        out.push_str(&render_var_decl(declaration, 2));
    }
    out.push_str(&format!(
        "  {} {} = mira_func_{}({});\n",
        test.call.ret_c_type,
        test.call.result_name,
        test.call.function_name,
        test.call.args.join(", ")
    ));
    out.push_str(&format!("  if (!{}) {{\n", test.assertion.condition));
    out.push_str(&format!(
        "    fprintf(stderr, {});\n",
        render_c_string_literal(&test.assertion.failure_message)
    ));
    out.push_str("    failures += 1;\n");
    out.push_str("  }\n");
    out
}

fn emit_call_arguments(
    prefix: &str,
    function: &Function,
    arguments: &[(String, DataValue)],
    mutable_arrays: bool,
    named_types: &HashMap<String, TypeDeclBody>,
) -> Result<(String, Vec<String>), String> {
    let (declarations, call_args) = lower_call_arguments_with_mutability(
        prefix,
        function,
        arguments,
        mutable_arrays,
        named_types,
    )?;
    let mut rendered = String::new();
    for declaration in &declarations {
        rendered.push_str(&render_var_decl(declaration, 2));
    }
    Ok((rendered, call_args))
}

fn lower_call_arguments(
    prefix: &str,
    function: &Function,
    arguments: &[(String, DataValue)],
    named_types: &HashMap<String, TypeDeclBody>,
) -> Result<(Vec<LoweredVarDecl>, Vec<String>), String> {
    lower_call_arguments_with_mutability(prefix, function, arguments, false, named_types)
}

fn lower_call_arguments_with_mutability(
    prefix: &str,
    function: &Function,
    arguments: &[(String, DataValue)],
    mutable_arrays: bool,
    named_types: &HashMap<String, TypeDeclBody>,
) -> Result<(Vec<LoweredVarDecl>, Vec<String>), String> {
    let mut declarations = Vec::new();
    let mut call_args = Vec::new();
    for arg in &function.args {
        let (_, value) = arguments
            .iter()
            .find(|(name, _)| name == &arg.name)
            .ok_or_else(|| format!("missing argument {} for {}", arg.name, function.name))?;
        let binding_prefix = format!("{}_{}", prefix, arg.name);
        let (decls, expr) =
            lower_value_binding(&binding_prefix, value, &arg.ty, mutable_arrays, named_types)?;
        declarations.extend(decls);
        call_args.push(expr);
    }
    Ok((declarations, call_args))
}

fn lower_value_binding(
    prefix: &str,
    value: &DataValue,
    ty: &TypeRef,
    mutable_arrays: bool,
    named_types: &HashMap<String, TypeDeclBody>,
) -> Result<(Vec<LoweredVarDecl>, String), String> {
    match (value, ty) {
        (DataValue::Int(_), TypeRef::Int { .. })
        | (DataValue::Float(_), TypeRef::Float { .. })
        | (DataValue::Bool(_), TypeRef::Bool)
        | (DataValue::Symbol(_), TypeRef::Named(_))
        | (DataValue::Fields(_), TypeRef::Named(_))
        | (DataValue::Variant { .. }, TypeRef::Named(_)) => {
            let name = prefix.to_string();
            let expr = render_data_value_with_named_types(value, ty, named_types)?;
            Ok((
                vec![LoweredVarDecl {
                    storage: LoweredStorageClass::Auto,
                    c_type: ty.c_type()?,
                    name: name.clone(),
                    init: expr,
                }],
                name,
            ))
        }
        (DataValue::Array(items), TypeRef::Span(inner))
        | (DataValue::Array(items), TypeRef::Buf(inner)) => {
            let data_name = format!("{}_data", prefix);
            let span_name = prefix.to_string();
            let mut decls = Vec::new();
            if items.is_empty() {
                decls.push(LoweredVarDecl {
                    storage: LoweredStorageClass::Auto,
                    c_type: format!(
                        "{}{}*",
                        if matches!(ty, TypeRef::Span(_)) {
                            "const "
                        } else {
                            ""
                        },
                        inner.c_type()?
                    ),
                    name: data_name.clone(),
                    init: "NULL".to_string(),
                });
            } else {
                let rendered = items
                    .iter()
                    .map(|item| render_data_value_with_named_types(item, inner, named_types))
                    .collect::<Result<Vec<_>, _>>()?;
                decls.push(LoweredVarDecl {
                    storage: if matches!(ty, TypeRef::Span(_)) && !mutable_arrays {
                        LoweredStorageClass::StaticConst
                    } else {
                        LoweredStorageClass::Static
                    },
                    c_type: inner.c_type()?,
                    name: format!("{}[]", data_name),
                    init: format!("{{ {} }}", rendered.join(", ")),
                });
            }
            decls.push(LoweredVarDecl {
                storage: LoweredStorageClass::Auto,
                c_type: ty.c_type()?,
                name: span_name.clone(),
                init: format!(
                    "{{ .data = {}, .len = ((uint32_t) {}) }}",
                    data_name,
                    items.len()
                ),
            });
            Ok((decls, span_name))
        }
        (DataValue::Array(items), TypeRef::Vec { elem, .. }) => {
            let name = prefix.to_string();
            let rendered = items
                .iter()
                .map(|item| render_data_value_with_named_types(item, elem, named_types))
                .collect::<Result<Vec<_>, _>>()?;
            Ok((
                vec![LoweredVarDecl {
                    storage: LoweredStorageClass::Auto,
                    c_type: ty.c_type()?,
                    name: name.clone(),
                    init: format!("{{ .data = {{ {} }} }}", rendered.join(", ")),
                }],
                name,
            ))
        }
        _ => Err(format!("cannot bind value {value:?} to type {ty}")),
    }
}

fn render_var_decl(decl: &LoweredVarDecl, indent: usize) -> String {
    let mut out = String::new();
    emit_indent(&mut out, indent);
    match decl.storage {
        LoweredStorageClass::Auto => {}
        LoweredStorageClass::Static => out.push_str("static "),
        LoweredStorageClass::StaticConst => out.push_str("static const "),
    }
    out.push_str(&decl.c_type);
    out.push(' ');
    out.push_str(&decl.name);
    out.push_str(" = ");
    out.push_str(&decl.init);
    out.push_str(";\n");
    out
}

fn param_c_name(block: &Block, name: &str) -> String {
    format!("param_{}_{}", block.label, name)
}

fn emit_benchmark_updates(prefix: &str, function: &Function) -> Result<String, String> {
    let mut out = String::new();
    out.push_str("    bench_seed = bench_seed * 1664525u + 1013904223u;\n");
    for (index, arg) in function.args.iter().enumerate() {
        let binding = format!("{}_{}", prefix, arg.name);
        match &arg.ty {
            TypeRef::Int {
                signed: true,
                bits: 32,
            } => {
                out.push_str(&format!(
                    "    {} = ((int32_t) (40 + ((bench_seed >> {}) & 15u)));\n",
                    binding, index
                ));
            }
            TypeRef::Int {
                signed: false,
                bits: 32,
            } => {
                out.push_str(&format!(
                    "    {} = ((uint32_t) (1 + ((bench_seed >> {}) & 31u)));\n",
                    binding, index
                ));
            }
            TypeRef::Span(inner) | TypeRef::Buf(inner)
                if **inner
                    == TypeRef::Int {
                        signed: true,
                        bits: 32,
                    } =>
            {
                out.push_str(&format!(
                    "    if ({}.len > 0u) {{ {}_data[0] = ((int32_t) (((bench_seed >> {}) & 1023u) - 511)); }}\n",
                    binding,
                    binding,
                    index
                ));
            }
            TypeRef::Vec { elem, .. }
                if **elem
                    == TypeRef::Int {
                        signed: true,
                        bits: 32,
                    } =>
            {
                out.push_str(&format!(
                    "    {}.data[0] = ((int32_t) (((bench_seed >> {}) & 1023u) - 511));\n",
                    binding, index
                ));
            }
            _ => {}
        }
    }
    Ok(out)
}
