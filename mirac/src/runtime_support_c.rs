pub fn emit_portable_runtime_support_c() -> String {
    r#"
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <inttypes.h>

#ifdef _WIN32
#include <windows.h>
#include <winsock2.h>
#include <ws2tcpip.h>
#include <process.h>
#include <io.h>
#include <fcntl.h>
#include <malloc.h>
#include <sys/stat.h>
#pragma comment(lib, "Ws2_32.lib")
#ifndef SHUT_WR
#define SHUT_WR SD_SEND
#endif
#else
#include <arpa/inet.h>
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <netdb.h>
#include <pthread.h>
#include <signal.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

typedef struct {
  uint8_t* data;
  uint32_t len;
  uint32_t cap;
} buf_u8;

#ifdef _WIN32
typedef SOCKET mira_socket_t;
#define MIRA_INVALID_SOCKET INVALID_SOCKET
#define MIRA_CLOSE_SOCKET closesocket
#else
typedef int mira_socket_t;
#define MIRA_INVALID_SOCKET (-1)
#define MIRA_CLOSE_SOCKET close
#endif

typedef struct {
  bool used;
  uint32_t kind;
  mira_socket_t fd;
  uint32_t timeout_ms;
  uint32_t shutdown_grace_ms;
  char reconnect_host[256];
  uint16_t reconnect_port;
  uint32_t pending_bytes;
  uint64_t resume_id;
  uint8_t reconnectable;
#ifdef _WIN32
  intptr_t tls_stdin_fd;
  intptr_t tls_stdout_fd;
  intptr_t tls_pid;
#else
  int tls_stdin_fd;
  int tls_stdout_fd;
  int tls_pid;
#endif
  int tls_accepted;
} mira_net_handle_entry;

typedef struct {
  bool used;
#ifdef _WIN32
  int waited;
  int exit_status;
  intptr_t stdin_fd;
#else
  int pid;
  int waited;
  int exit_status;
  int stdin_fd;
#endif
  char stdout_path[512];
  char stderr_path[512];
} mira_spawn_handle_entry;

typedef struct {
  bool used;
  buf_u8 body;
  uint32_t cursor;
} mira_http_body_stream_entry;

typedef struct {
  bool used;
  uint64_t session_handle;
  int closed;
} mira_http_response_stream_entry;

typedef struct {
  bool used;
  char host[256];
  uint16_t port;
} mira_http_client_entry;

typedef struct {
  bool used;
  char host[256];
  uint16_t port;
  uint32_t max_size;
  uint32_t leased;
} mira_http_client_pool_entry;

typedef struct {
  bool used;
#ifdef _WIN32
  HMODULE handle;
#else
  void* handle;
#endif
} mira_ffi_lib_handle_entry;

typedef struct {
  bool used;
  char name[128];
  char sql[1024];
} mira_db_stmt_entry;

typedef struct {
  bool used;
  char target[512];
  mira_db_stmt_entry prepared[32];
  uint32_t prepared_len;
  bool in_transaction;
  uint32_t last_error_code;
  int last_error_retryable;
  buf_u8 tx_sql;
} mira_db_handle_entry;

typedef struct {
  bool used;
  char target[512];
  uint32_t max_size;
  uint32_t max_idle;
  uint64_t leased_handles[32];
  uint32_t leased_len;
} mira_db_pool_entry;

typedef struct {
  bool used;
  char target[512];
} mira_cache_entry;

typedef struct {
  bool used;
  char target[512];
} mira_queue_entry;

typedef struct {
  bool used;
  uint32_t max_workers;
  uint32_t active_workers;
  int shutting_down;
#ifdef _WIN32
  int unused;
#else
  pthread_mutex_t mutex;
  pthread_cond_t cond;
#endif
} mira_rt_scheduler_entry;

typedef struct {
  bool used;
  uint64_t runtime_handle;
  char function_name[128];
  uint32_t kind;
  uint32_t arg;
  buf_u8 arg_buf;
  uint32_t result;
  buf_u8 result_buf;
  int done;
  int cancelled;
  int joined;
#ifdef _WIN32
  uintptr_t thread_handle;
  int unused;
#else
  pthread_t thread;
  pthread_mutex_t mutex;
  pthread_cond_t cond;
#endif
} mira_rt_task_entry;

typedef struct {
  bool used;
  uint32_t* values;
  uint32_t capacity;
  uint32_t head;
  uint32_t tail;
  uint32_t len;
  int closed;
#ifdef _WIN32
  int unused;
#else
  pthread_mutex_t mutex;
  pthread_cond_t not_empty;
  pthread_cond_t not_full;
#endif
} mira_chan_u32_entry;

typedef struct {
  bool used;
  buf_u8* values;
  uint32_t capacity;
  uint32_t head;
  uint32_t tail;
  uint32_t len;
  int closed;
#ifdef _WIN32
  int unused;
#else
  pthread_mutex_t mutex;
  pthread_cond_t not_empty;
  pthread_cond_t not_full;
#endif
} mira_chan_buf_entry;

typedef struct {
  bool used;
  uint64_t opened_ns;
  uint32_t timeout_ms;
} mira_deadline_entry;

typedef struct {
  bool used;
  uint64_t parent;
  int cancelled;
  uint64_t bound_tasks[64];
  uint32_t bound_len;
} mira_cancel_scope_entry;

typedef struct {
  bool used;
  uint32_t max_attempts;
  uint32_t attempts;
  uint32_t base_backoff_ms;
  uint32_t last_delay_ms;
} mira_retry_entry;

typedef struct {
  bool used;
  uint32_t threshold;
  uint32_t failures;
  uint32_t cooldown_ms;
  uint64_t open_until_ns;
} mira_circuit_entry;

typedef struct {
  bool used;
  uint32_t limit;
  uint32_t in_use;
} mira_backpressure_entry;

typedef struct {
  bool used;
  uint32_t restart_budget;
  uint32_t degrade_after;
  uint32_t failures;
  uint32_t recoveries;
  uint32_t last_code;
} mira_supervisor_entry;

typedef struct {
  bool used;
  char name[128];
  uint32_t healthy;
  uint32_t ready;
  int degraded;
  int shutdown;
  uint64_t traces_started;
  uint64_t trace_links;
  uint64_t metrics_total;
  uint64_t log_entries;
} mira_service_entry;

typedef struct {
  bool used;
  uint64_t service_handle;
  char name[128];
  uint64_t parent_trace;
} mira_service_trace_entry;

typedef struct {
  bool used;
  uint64_t service_handle;
  char kind[128];
  uint32_t total;
  char last_message[256];
} mira_service_event_entry;

typedef struct {
  bool used;
  uint64_t service_handle;
  char metric[128];
  uint32_t total;
} mira_service_metric_entry;

typedef struct {
  bool used;
  uint64_t service_handle;
  char metric[128];
  char dimension[128];
  uint32_t total;
} mira_service_metric_dim_entry;

typedef struct {
  bool used;
  uint64_t service_handle;
  char kind[128];
  uint32_t total;
} mira_service_failure_entry;

typedef struct {
  bool used;
  uint64_t service_handle;
  char key[128];
  uint32_t value;
} mira_service_checkpoint_u32_entry;

typedef struct {
  bool used;
  uint32_t seq;
  char conversation[128];
  char recipient[128];
  buf_u8 payload;
  int acked;
  uint32_t retry_count;
} mira_msg_delivery_entry;

typedef struct {
  bool used;
  char room[128];
  char recipient[128];
} mira_msg_subscription_entry;

typedef struct {
  bool used;
  char key[256];
  uint32_t seq;
} mira_msg_dedup_entry;

typedef struct {
  bool used;
  uint32_t next_seq;
  uint32_t last_failure_class;
  mira_msg_delivery_entry deliveries[512];
  mira_msg_subscription_entry subscriptions[256];
  mira_msg_dedup_entry dedup[256];
} mira_msg_log_entry;

typedef struct {
  bool used;
  uint64_t log_handle;
  char recipient[128];
  uint32_t from_seq;
  uint32_t cursor;
  uint32_t last_seq;
} mira_msg_replay_entry;

typedef struct {
  bool used;
  char target[512];
} mira_stream_entry;

typedef struct {
  bool used;
  uint64_t stream_handle;
  uint32_t offset;
  uint32_t last_offset;
} mira_stream_replay_entry;

typedef struct {
  bool used;
  char target[512];
} mira_lease_entry;

typedef struct {
  bool used;
  char target[512];
} mira_placement_entry;

typedef struct {
  bool used;
  char target[512];
} mira_coord_entry;

typedef struct {
  bool used;
  uint64_t values[1024];
  uint32_t len;
} mira_batch_entry;

typedef struct {
  bool used;
  uint64_t count;
  uint64_t sum;
  uint64_t min;
  uint64_t max;
  int has_value;
} mira_agg_u64_entry;

typedef struct {
  bool used;
  uint32_t width_ms;
  uint64_t values[1024];
  uint64_t timestamps_ms[1024];
  uint32_t len;
} mira_window_u64_entry;

static mira_net_handle_entry mira_net_handles[256];
static mira_spawn_handle_entry mira_spawn_handles[128];
static mira_http_body_stream_entry mira_http_body_streams[128];
static mira_http_response_stream_entry mira_http_response_streams[128];
static mira_http_client_entry mira_http_clients[128];
static mira_http_client_pool_entry mira_http_client_pools[64];
static mira_ffi_lib_handle_entry mira_ffi_lib_handles[128];
static mira_db_handle_entry mira_db_handles[128];
static mira_db_pool_entry mira_db_pools[64];
static mira_cache_entry mira_cache_handles[64];
static mira_queue_entry mira_queue_handles[64];
static mira_rt_scheduler_entry mira_rt_schedulers[32];
static mira_rt_task_entry mira_rt_tasks[256];
static mira_chan_u32_entry mira_chan_u32[64];
static mira_chan_buf_entry mira_chan_buf[64];
static mira_deadline_entry mira_deadlines[128];
static mira_cancel_scope_entry mira_cancel_scopes[128];
static mira_retry_entry mira_retries[128];
static mira_circuit_entry mira_circuits[128];
static mira_backpressure_entry mira_backpressure[128];
static mira_supervisor_entry mira_supervisors[128];
static mira_service_entry mira_services[64];
static mira_service_trace_entry mira_service_traces[256];
static mira_service_event_entry mira_service_events[256];
static mira_service_metric_entry mira_service_metrics[256];
static mira_service_metric_dim_entry mira_service_metric_dims[256];
static mira_service_failure_entry mira_service_failures[256];
static mira_service_checkpoint_u32_entry mira_service_checkpoints_u32[256];
static mira_msg_log_entry mira_msg_logs[64];
static mira_msg_replay_entry mira_msg_replays[128];
static mira_stream_entry mira_stream_handles[64];
static mira_stream_replay_entry mira_stream_replays[128];
static mira_lease_entry mira_lease_handles[64];
static mira_placement_entry mira_placement_handles[64];
static mira_coord_entry mira_coord_handles[64];
static mira_batch_entry mira_batch_handles[128];
static mira_agg_u64_entry mira_agg_u64_handles[128];
static mira_window_u64_entry mira_window_u64_handles[128];

buf_u8 mira_http_body_buf_u8(buf_u8 request);
int32_t mira_spawn_wait_handle(uint64_t handle);

__attribute__((weak)) uint32_t mira_rt_dispatch_u32(const char* function_name, uint32_t arg) {
  (void) function_name;
  (void) arg;
  return 0u;
}
__attribute__((weak)) buf_u8 mira_rt_dispatch_buf(const char* function_name, buf_u8 arg) {
  (void) function_name;
  (void) arg;
  return (buf_u8){0};
}

#define MIRA_NET_KIND_PLAIN_LISTENER 1u
#define MIRA_NET_KIND_PLAIN_STREAM 2u
#define MIRA_NET_KIND_TLS_LISTENER 3u
#define MIRA_NET_KIND_TLS_SESSION 4u

static uint64_t mira_alloc_handle(bool* used, size_t count) {
  for (size_t index = 0; index < count; index++) {
    if (!used[index]) {
      used[index] = true;
      return (uint64_t) (index + 1u);
    }
  }
  return 0u;
}

static mira_service_event_entry* mira_service_event_slot(uint64_t handle, const char* kind) {
  for (size_t index = 0; index < 256u; index++) {
    if (mira_service_events[index].used
        && mira_service_events[index].service_handle == handle
        && strcmp(mira_service_events[index].kind, kind) == 0) {
      return &mira_service_events[index];
    }
  }
  for (size_t index = 0; index < 256u; index++) {
    if (!mira_service_events[index].used) {
      memset(&mira_service_events[index], 0, sizeof(mira_service_events[index]));
      mira_service_events[index].used = true;
      mira_service_events[index].service_handle = handle;
      if (kind != NULL) {
        strncpy(mira_service_events[index].kind, kind, sizeof(mira_service_events[index].kind) - 1u);
      }
      return &mira_service_events[index];
    }
  }
  return NULL;
}

static mira_service_metric_entry* mira_service_metric_slot(uint64_t handle, const char* metric) {
  for (size_t index = 0; index < 256u; index++) {
    if (mira_service_metrics[index].used
        && mira_service_metrics[index].service_handle == handle
        && strcmp(mira_service_metrics[index].metric, metric) == 0) {
      return &mira_service_metrics[index];
    }
  }
  for (size_t index = 0; index < 256u; index++) {
    if (!mira_service_metrics[index].used) {
      memset(&mira_service_metrics[index], 0, sizeof(mira_service_metrics[index]));
      mira_service_metrics[index].used = true;
      mira_service_metrics[index].service_handle = handle;
      if (metric != NULL) {
        strncpy(mira_service_metrics[index].metric, metric, sizeof(mira_service_metrics[index].metric) - 1u);
      }
      return &mira_service_metrics[index];
    }
  }
  return NULL;
}

static mira_service_metric_dim_entry* mira_service_metric_dim_slot(
    uint64_t handle,
    const char* metric,
    const char* dimension
) {
  for (size_t index = 0; index < 256u; index++) {
    if (mira_service_metric_dims[index].used
        && mira_service_metric_dims[index].service_handle == handle
        && strcmp(mira_service_metric_dims[index].metric, metric) == 0
        && strcmp(mira_service_metric_dims[index].dimension, dimension) == 0) {
      return &mira_service_metric_dims[index];
    }
  }
  for (size_t index = 0; index < 256u; index++) {
    if (!mira_service_metric_dims[index].used) {
      memset(&mira_service_metric_dims[index], 0, sizeof(mira_service_metric_dims[index]));
      mira_service_metric_dims[index].used = true;
      mira_service_metric_dims[index].service_handle = handle;
      if (metric != NULL) {
        strncpy(mira_service_metric_dims[index].metric, metric, sizeof(mira_service_metric_dims[index].metric) - 1u);
      }
      if (dimension != NULL) {
        strncpy(mira_service_metric_dims[index].dimension, dimension, sizeof(mira_service_metric_dims[index].dimension) - 1u);
      }
      return &mira_service_metric_dims[index];
    }
  }
  return NULL;
}

static mira_service_failure_entry* mira_service_failure_slot(uint64_t handle, const char* kind) {
  for (size_t index = 0; index < 256u; index++) {
    if (mira_service_failures[index].used
        && mira_service_failures[index].service_handle == handle
        && strcmp(mira_service_failures[index].kind, kind) == 0) {
      return &mira_service_failures[index];
    }
  }
  for (size_t index = 0; index < 256u; index++) {
    if (!mira_service_failures[index].used) {
      memset(&mira_service_failures[index], 0, sizeof(mira_service_failures[index]));
      mira_service_failures[index].used = true;
      mira_service_failures[index].service_handle = handle;
      if (kind != NULL) {
        strncpy(mira_service_failures[index].kind, kind, sizeof(mira_service_failures[index].kind) - 1u);
      }
      return &mira_service_failures[index];
    }
  }
  return NULL;
}

static mira_service_checkpoint_u32_entry* mira_service_checkpoint_u32_slot(
    uint64_t handle,
    const char* key
) {
  for (size_t index = 0; index < 256u; index++) {
    if (mira_service_checkpoints_u32[index].used
        && mira_service_checkpoints_u32[index].service_handle == handle
        && strcmp(mira_service_checkpoints_u32[index].key, key) == 0) {
      return &mira_service_checkpoints_u32[index];
    }
  }
  for (size_t index = 0; index < 256u; index++) {
    if (!mira_service_checkpoints_u32[index].used) {
      memset(&mira_service_checkpoints_u32[index], 0, sizeof(mira_service_checkpoints_u32[index]));
      mira_service_checkpoints_u32[index].used = true;
      mira_service_checkpoints_u32[index].service_handle = handle;
      if (key != NULL) {
        strncpy(mira_service_checkpoints_u32[index].key, key, sizeof(mira_service_checkpoints_u32[index].key) - 1u);
      }
      return &mira_service_checkpoints_u32[index];
    }
  }
  return NULL;
}

static buf_u8 mira_buf_empty(void) {
  buf_u8 value;
  value.data = NULL;
  value.len = 0u;
  value.cap = 0u;
  return value;
}

static buf_u8 mira_buf_from_heap(uint8_t* data, size_t len) {
  buf_u8 value;
  value.data = data;
  value.len = (uint32_t) len;
  value.cap = (uint32_t) len;
  return value;
}

static buf_u8 mira_buf_copy_bytes(const uint8_t* data, size_t len) {
  if (len == 0) {
    return mira_buf_empty();
  }
  uint8_t* heap = (uint8_t*) malloc(len);
  if (heap == NULL) {
    return mira_buf_empty();
  }
  memcpy(heap, data, len);
  return mira_buf_from_heap(heap, len);
}

static int mira_hex_value(uint8_t ch) {
  if (ch >= '0' && ch <= '9') {
    return (int) (ch - '0');
  }
  if (ch >= 'a' && ch <= 'f') {
    return (int) (10 + (ch - 'a'));
  }
  if (ch >= 'A' && ch <= 'F') {
    return (int) (10 + (ch - 'A'));
  }
  return -1;
}

static void mira_copy_token(char* dst, size_t dst_len, const char* src) {
  if (dst == NULL || dst_len == 0u) {
    return;
  }
  dst[0] = '\0';
  if (src == NULL) {
    return;
  }
  strncpy(dst, src, dst_len - 1u);
  dst[dst_len - 1u] = '\0';
}

static void mira_msg_make_dedup_key(char* out, size_t out_len, const char* scope, const char* recipient, buf_u8 key) {
  if (out == NULL || out_len == 0u) {
    return;
  }
  size_t offset = 0u;
  int written = snprintf(out, out_len, "%s|%s|", scope == NULL ? "" : scope, recipient == NULL ? "" : recipient);
  if (written < 0) {
    out[0] = '\0';
    return;
  }
  offset = (size_t) written;
  if (offset >= out_len) {
    out[out_len - 1u] = '\0';
    return;
  }
  static const char hex[] = "0123456789abcdef";
  for (uint32_t index = 0u; index < key.len && offset + 2u < out_len; index++) {
    uint8_t byte = key.data[index];
    out[offset++] = hex[(byte >> 4u) & 0x0fu];
    out[offset++] = hex[byte & 0x0fu];
  }
  out[offset] = '\0';
}

static buf_u8 mira_decode_escaped_literal(const char* literal) {
  if (literal == NULL) {
    return mira_buf_empty();
  }
  size_t len = strlen(literal);
  if (len == 0u) {
    return mira_buf_empty();
  }
  uint8_t* data = (uint8_t*) malloc(len);
  if (data == NULL) {
    return mira_buf_empty();
  }
  size_t out = 0u;
  for (size_t index = 0u; index < len; index++) {
    uint8_t ch = (uint8_t) literal[index];
    if (ch == '\\' && index + 1u < len) {
      uint8_t next = (uint8_t) literal[index + 1u];
      if (next == 'n') {
        data[out++] = '\n';
        index += 1u;
        continue;
      }
      if (next == 'r') {
        data[out++] = '\r';
        index += 1u;
        continue;
      }
      if (next == 't') {
        data[out++] = '\t';
        index += 1u;
        continue;
      }
      if (next == '\\') {
        data[out++] = '\\';
        index += 1u;
        continue;
      }
      if (next == '"') {
        data[out++] = '"';
        index += 1u;
        continue;
      }
      if (next == 'x' && index + 3u < len) {
        int hi = mira_hex_value((uint8_t) literal[index + 2u]);
        int lo = mira_hex_value((uint8_t) literal[index + 3u]);
        if (hi >= 0 && lo >= 0) {
          data[out++] = (uint8_t) ((hi << 4) | lo);
          index += 3u;
          continue;
        }
      }
    }
    data[out++] = ch;
  }
  return mira_buf_from_heap(data, out);
}

buf_u8 mira_buf_lit_u8(const char* literal) {
  return mira_decode_escaped_literal(literal);
}

bool mira_drop_buf_u8(buf_u8 value) {
  if (value.data != NULL) {
    free(value.data);
  }
  return 1u;
}

void mira_buf_lit_u8_out(const char* literal, buf_u8* out) {
  if (out == NULL) {
    return;
  }
  *out = mira_buf_lit_u8(literal);
}

bool mira_drop_buf_u8_parts(const uint8_t* data, uint32_t len) {
  buf_u8 value;
  value.data = (uint8_t*) data;
  value.len = len;
  value.cap = len;
  return mira_drop_buf_u8(value);
}

buf_u8 mira_buf_concat_u8(buf_u8 left, buf_u8 right) {
  if (left.len == 0u && right.len == 0u) {
    return mira_buf_empty();
  }
  size_t len = (size_t) left.len + (size_t) right.len;
  uint8_t* data = (uint8_t*) malloc(len);
  if (data == NULL) {
    return mira_buf_empty();
  }
  if (left.len > 0u && left.data != NULL) {
    memcpy(data, left.data, left.len);
  }
  if (right.len > 0u && right.data != NULL) {
    memcpy(data + left.len, right.data, right.len);
  }
  return mira_buf_from_heap(data, len);
}

static uint32_t mira_write_all_fd(mira_socket_t fd, const uint8_t* data, size_t len) {
  size_t offset = 0u;
  while (offset < len) {
#ifdef _WIN32
    int wrote = send(fd, (const char*) data + offset, (int) (len - offset), 0);
#else
    ssize_t wrote = send(fd, data + offset, len - offset, 0);
#endif
    if (wrote <= 0) {
      return 0u;
    }
    offset += (size_t) wrote;
  }
  return 1u;
}

static buf_u8 mira_read_all_fd(mira_socket_t fd) {
  size_t cap = 256u;
  size_t len = 0u;
  uint8_t* data = (uint8_t*) malloc(cap);
  if (data == NULL) {
    return mira_buf_empty();
  }
  for (;;) {
    if (len == cap) {
      cap *= 2u;
      uint8_t* grown = (uint8_t*) realloc(data, cap);
      if (grown == NULL) {
        free(data);
        return mira_buf_empty();
      }
      data = grown;
    }
#ifdef _WIN32
    int got = recv(fd, (char*) data + len, (int) (cap - len), 0);
#else
    ssize_t got = recv(fd, data + len, cap - len, 0);
#endif
    if (got < 0) {
      free(data);
      return mira_buf_empty();
    }
    if (got == 0) {
      break;
    }
    len += (size_t) got;
  }
  if (len == 0u) {
    free(data);
    return mira_buf_empty();
  }
  return mira_buf_from_heap(data, len);
}

static buf_u8 mira_read_chunk_fd(mira_socket_t fd, uint32_t chunk_len) {
  size_t len = chunk_len == 0u ? 0u : (size_t) chunk_len;
  if (len == 0u) {
    return mira_buf_empty();
  }
  uint8_t* data = (uint8_t*) malloc(len);
  if (data == NULL) {
    return mira_buf_empty();
  }
#ifdef _WIN32
  int got = recv(fd, (char*) data, (int) len, 0);
#else
  ssize_t got = recv(fd, data, len, 0);
#endif
  if (got <= 0) {
    free(data);
    return mira_buf_empty();
  }
  return mira_buf_from_heap(data, (size_t) got);
}

static uint64_t mira_session_resume_hash(const char* host, uint16_t port) {
  uint64_t hash = 0xcbf29ce484222325ULL;
  if (host != NULL) {
    for (size_t index = 0u; host[index] != '\0'; index++) {
      hash ^= (uint64_t) (uint8_t) host[index];
      hash *= 0x100000001b3ULL;
    }
  }
  hash ^= (uint64_t) port;
  hash *= 0x100000001b3ULL;
  return hash;
}

static int mira_net_is_session_kind(uint32_t kind) {
  return kind == MIRA_NET_KIND_PLAIN_STREAM || kind == MIRA_NET_KIND_TLS_SESSION;
}

#ifndef _WIN32
static uint32_t mira_write_all_pipe_fd(int fd, const uint8_t* data, size_t len) {
  size_t offset = 0u;
  while (offset < len) {
    ssize_t wrote = write(fd, data + offset, len - offset);
    if (wrote <= 0) {
      return 0u;
    }
    offset += (size_t) wrote;
  }
  return 1u;
}

static size_t mira_http_message_complete_len(const uint8_t* data, size_t len) {
  for (size_t index = 0u; index + 3u < len; index++) {
    if (data[index] == '\r' && data[index + 1u] == '\n' && data[index + 2u] == '\r' && data[index + 3u] == '\n') {
      size_t header_end = index + 4u;
      size_t body_len = 0u;
      char* headers = (char*) malloc(header_end + 1u);
      if (headers == NULL) {
        return 0u;
      }
      memcpy(headers, data, header_end);
      headers[header_end] = '\0';
      const char* content_length = strstr(headers, "Content-Length:");
      if (content_length != NULL) {
        body_len = (size_t) strtoul(content_length + strlen("Content-Length:"), NULL, 10);
      }
      free(headers);
      return header_end + body_len;
    }
  }
  return 0u;
}

static buf_u8 mira_read_http_message_pipe_fd(int fd) {
  size_t cap = 1024u;
  size_t len = 0u;
  uint8_t* data = (uint8_t*) malloc(cap);
  if (data == NULL) {
    return mira_buf_empty();
  }
  for (;;) {
    if (len == cap) {
      cap *= 2u;
      uint8_t* grown = (uint8_t*) realloc(data, cap);
      if (grown == NULL) {
        free(data);
        return mira_buf_empty();
      }
      data = grown;
    }
    ssize_t got = read(fd, data + len, cap - len);
    if (got < 0) {
      free(data);
      return mira_buf_empty();
    }
    if (got == 0) {
      break;
    }
    len += (size_t) got;
    size_t total = mira_http_message_complete_len(data, len);
    if (total > 0u && len >= total) {
      len = total;
      break;
    }
  }
  if (len == 0u) {
    free(data);
    return mira_buf_empty();
  }
  return mira_buf_from_heap(data, len);
}
#endif

static buf_u8 mira_read_file_all_bytes(const char* path) {
  FILE* file = fopen(path, "rb");
  if (file == NULL) {
    return mira_buf_empty();
  }
  if (fseek(file, 0, SEEK_END) != 0) {
    fclose(file);
    return mira_buf_empty();
  }
  long size = ftell(file);
  if (size < 0) {
    fclose(file);
    return mira_buf_empty();
  }
  if (fseek(file, 0, SEEK_SET) != 0) {
    fclose(file);
    return mira_buf_empty();
  }
  if (size == 0) {
    fclose(file);
    return mira_buf_empty();
  }
  uint8_t* data = (uint8_t*) malloc((size_t) size);
  if (data == NULL) {
    fclose(file);
    return mira_buf_empty();
  }
  size_t got = fread(data, 1u, (size_t) size, file);
  fclose(file);
  if (got != (size_t) size) {
    free(data);
    return mira_buf_empty();
  }
  return mira_buf_from_heap(data, got);
}

static const char* mira_sqlite3_command(void) {
  static const char* candidates[] = {
    "/Users/sheremetovegor/miniconda3/bin/sqlite3",
    "/opt/homebrew/bin/sqlite3",
    "/usr/local/bin/sqlite3",
    "/usr/bin/sqlite3",
    "sqlite3"
  };
  for (size_t index = 0u; index < sizeof(candidates) / sizeof(candidates[0]); index++) {
    const char* candidate = candidates[index];
    if (strcmp(candidate, "sqlite3") == 0) {
      return candidate;
    }
#ifdef _WIN32
    if (_access(candidate, 0) == 0) {
      return candidate;
    }
#else
    if (access(candidate, X_OK) == 0) {
      return candidate;
    }
#endif
  }
  return "sqlite3";
}

static char* mira_shell_escape_single_quoted(const char* text) {
  if (text == NULL) {
    char* empty = (char*) malloc(3u);
    if (empty == NULL) {
      return NULL;
    }
    memcpy(empty, "''", 3u);
    return empty;
  }
  size_t len = strlen(text);
  size_t extra = 0u;
  for (size_t index = 0u; index < len; index++) {
    if (text[index] == '\'') {
      extra += 3u;
    }
  }
  char* out = (char*) malloc(len + extra + 3u);
  if (out == NULL) {
    return NULL;
  }
  size_t cursor = 0u;
  out[cursor++] = '\'';
  for (size_t index = 0u; index < len; index++) {
    if (text[index] == '\'') {
      memcpy(out + cursor, "'\\''", 4u);
      cursor += 4u;
    } else {
      out[cursor++] = text[index];
    }
  }
  out[cursor++] = '\'';
  out[cursor] = '\0';
  return out;
}

static int mira_mkstemp_path(char* path_buf, size_t path_buf_len, const char* templ) {
  if (path_buf_len == 0u || templ == NULL) {
    return -1;
  }
  strncpy(path_buf, templ, path_buf_len - 1u);
  path_buf[path_buf_len - 1u] = '\0';
#ifdef _WIN32
  char* generated = _tempnam(NULL, "msq");
  if (generated == NULL) {
    return -1;
  }
  strncpy(path_buf, generated, path_buf_len - 1u);
  path_buf[path_buf_len - 1u] = '\0';
  free(generated);
  int fd = _open(path_buf, _O_CREAT | _O_TRUNC | _O_RDWR | _O_BINARY, _S_IREAD | _S_IWRITE);
  return fd;
#else
  return mkstemp(path_buf);
#endif
}

static int mira_write_bytes_file(const char* path, const uint8_t* data, size_t len) {
  FILE* file = fopen(path, "wb");
  if (file == NULL) {
    return 0;
  }
  size_t wrote = len == 0u ? 0u : fwrite(data, 1u, len, file);
  int closed = fclose(file);
  return (len == 0u || wrote == len) && closed == 0;
}

static buf_u8 mira_sqlite_run_and_capture(const char* db_path, buf_u8 sql, int* ok) {
  if (ok != NULL) {
    *ok = 0;
  }
  char sql_path[512];
  char out_path[512];
  int sql_fd = mira_mkstemp_path(sql_path, sizeof(sql_path), "/tmp/mira_sqlite_sql_XXXXXX");
  int out_fd = mira_mkstemp_path(out_path, sizeof(out_path), "/tmp/mira_sqlite_out_XXXXXX");
  if (sql_fd < 0 || out_fd < 0) {
    if (sql_fd >= 0) {
#ifdef _WIN32
      _close(sql_fd);
#else
      close(sql_fd);
#endif
    }
    if (out_fd >= 0) {
#ifdef _WIN32
      _close(out_fd);
#else
      close(out_fd);
#endif
    }
    return mira_buf_empty();
  }
#ifdef _WIN32
  _close(sql_fd);
  _close(out_fd);
#else
  close(sql_fd);
  close(out_fd);
#endif
  if (!mira_write_bytes_file(sql_path, sql.data, sql.len)) {
    remove(sql_path);
    remove(out_path);
    return mira_buf_empty();
  }
  char command[4096];
  char* quoted_bin = mira_shell_escape_single_quoted(mira_sqlite3_command());
  char* quoted_db = mira_shell_escape_single_quoted(db_path);
  char* quoted_sql = mira_shell_escape_single_quoted(sql_path);
  char* quoted_out = mira_shell_escape_single_quoted(out_path);
  if (quoted_bin == NULL || quoted_db == NULL || quoted_sql == NULL || quoted_out == NULL) {
    free(quoted_bin);
    free(quoted_db);
    free(quoted_sql);
    free(quoted_out);
    remove(sql_path);
    remove(out_path);
    return mira_buf_empty();
  }
  snprintf(
      command,
      sizeof(command),
      "%s -batch -noheader %s < %s > %s 2>&1",
      quoted_bin,
      quoted_db,
      quoted_sql,
      quoted_out);
  free(quoted_bin);
  free(quoted_db);
  free(quoted_sql);
  free(quoted_out);
  int status = system(command);
  remove(sql_path);
  buf_u8 out = mira_read_file_all_bytes(out_path);
  remove(out_path);
  if (status != 0) {
    return out;
  }
  while (out.len > 0u && out.data != NULL &&
         (out.data[out.len - 1u] == '\n' || out.data[out.len - 1u] == '\r')) {
    out.len -= 1u;
  }
  if (ok != NULL) {
    *ok = 1;
  }
  return out;
}

static int mira_starts_with(const char* text, const char* prefix) {
  if (text == NULL || prefix == NULL) {
    return 0;
  }
  size_t prefix_len = strlen(prefix);
  return strncmp(text, prefix, prefix_len) == 0;
}

static int mira_db_target_is_postgres(const char* target) {
  return mira_starts_with(target, "postgresql://") || mira_starts_with(target, "postgres://");
}

static char* mira_postgres_dsn_for_docker(const char* dsn) {
  if (dsn == NULL) {
    return NULL;
  }
  const char* host = strstr(dsn, "@127.0.0.1:");
  const char* localhost = strstr(dsn, "@localhost:");
  const char* match = host != NULL ? host : localhost;
  if (match == NULL) {
    size_t len = strlen(dsn);
    char* out = (char*) malloc(len + 1u);
    if (out != NULL) {
      memcpy(out, dsn, len + 1u);
    }
    return out;
  }
  const char* replacement = "@host.docker.internal:";
  size_t prefix_len = (size_t) (match - dsn);
  size_t skip = host != NULL ? strlen("@127.0.0.1:") : strlen("@localhost:");
  size_t suffix_len = strlen(match + skip);
  size_t out_len = prefix_len + strlen(replacement) + suffix_len;
  char* out = (char*) malloc(out_len + 1u);
  if (out == NULL) {
    return NULL;
  }
  memcpy(out, dsn, prefix_len);
  memcpy(out + prefix_len, replacement, strlen(replacement));
  memcpy(out + prefix_len + strlen(replacement), match + skip, suffix_len);
  out[out_len] = '\0';
  return out;
}

static buf_u8 mira_trim_db_output(buf_u8 value) {
  while (value.len > 0u && value.data != NULL &&
         (value.data[value.len - 1u] == '\n' || value.data[value.len - 1u] == '\r')) {
    value.len -= 1u;
  }
  return value;
}

static char* mira_buf_to_cstring(buf_u8 value) {
  char* text = (char*) malloc((size_t) value.len + 1u);
  if (text == NULL) {
    return NULL;
  }
  if (value.len > 0u && value.data != NULL) {
    memcpy(text, value.data, value.len);
  }
  text[value.len] = '\0';
  return text;
}

static buf_u8 mira_postgres_run_and_capture(const char* dsn, buf_u8 sql, int* ok) {
  if (ok != NULL) {
    *ok = 0;
  }
#ifdef _WIN32
  (void) dsn;
  (void) sql;
  return mira_buf_empty();
#else
  char sql_path[512];
  char out_path[512];
  int sql_fd = mira_mkstemp_path(sql_path, sizeof(sql_path), "/tmp/mira_pg_sql_XXXXXX");
  int out_fd = mira_mkstemp_path(out_path, sizeof(out_path), "/tmp/mira_pg_out_XXXXXX");
  if (sql_fd < 0 || out_fd < 0) {
    if (sql_fd >= 0) {
      close(sql_fd);
    }
    if (out_fd >= 0) {
      close(out_fd);
    }
    return mira_buf_empty();
  }
  close(sql_fd);
  close(out_fd);
  if (!mira_write_bytes_file(sql_path, sql.data, sql.len)) {
    remove(sql_path);
    remove(out_path);
    return mira_buf_empty();
  }
  char* docker_dsn = mira_postgres_dsn_for_docker(dsn);
  char* quoted_dsn = mira_shell_escape_single_quoted(docker_dsn);
  char* quoted_sql = mira_shell_escape_single_quoted(sql_path);
  char* quoted_out = mira_shell_escape_single_quoted(out_path);
  free(docker_dsn);
  if (quoted_dsn == NULL || quoted_sql == NULL || quoted_out == NULL) {
    free(quoted_dsn);
    free(quoted_sql);
    free(quoted_out);
    remove(sql_path);
    remove(out_path);
    return mira_buf_empty();
  }
  char command[8192];
  snprintf(
      command,
      sizeof(command),
      "docker run --rm -v /tmp:/tmp postgres:16-alpine sh -lc \"psql %s -v ON_ERROR_STOP=1 -At -f %s > %s 2>&1\"",
      quoted_dsn,
      quoted_sql,
      quoted_out);
  free(quoted_dsn);
  free(quoted_sql);
  free(quoted_out);
  int status = system(command);
  remove(sql_path);
  buf_u8 out = mira_read_file_all_bytes(out_path);
  remove(out_path);
  if (status != 0) {
    return out;
  }
  out = mira_trim_db_output(out);
  if (ok != NULL) {
    *ok = 1;
  }
  return out;
#endif
}

static buf_u8 mira_db_run_and_capture_target(const char* target, buf_u8 sql, int* ok) {
  if (mira_db_target_is_postgres(target)) {
    return mira_postgres_run_and_capture(target, sql, ok);
  }
  return mira_sqlite_run_and_capture(target, sql, ok);
}

static void mira_db_set_error(uint64_t handle, const char* error_text) {
  if (handle == 0u || handle > 128u || !mira_db_handles[handle - 1u].used) {
    return;
  }
  mira_db_handle_entry* entry = &mira_db_handles[handle - 1u];
  if (error_text == NULL) {
    entry->last_error_code = 1u;
    entry->last_error_retryable = 0;
    return;
  }
  char lower[256];
  size_t len = strlen(error_text);
  if (len >= sizeof(lower)) {
    len = sizeof(lower) - 1u;
  }
  for (size_t index = 0u; index < len; index++) {
    char ch = error_text[index];
    if (ch >= 'A' && ch <= 'Z') {
      ch = (char) (ch - 'A' + 'a');
    }
    lower[index] = ch;
  }
  lower[len] = '\0';
  if (strstr(lower, "timeout") != NULL) {
    entry->last_error_code = 3u;
    entry->last_error_retryable = 1;
  } else if (strstr(lower, "connect") != NULL || strstr(lower, "refused") != NULL ||
             strstr(lower, "unreachable") != NULL || strstr(lower, "docker") != NULL) {
    entry->last_error_code = 2u;
    entry->last_error_retryable = 1;
  } else if (strstr(lower, "syntax") != NULL || strstr(lower, "parse") != NULL) {
    entry->last_error_code = 4u;
    entry->last_error_retryable = 0;
  } else {
    entry->last_error_code = 1u;
    entry->last_error_retryable = 0;
  }
}

static void mira_db_clear_error(uint64_t handle) {
  if (handle == 0u || handle > 128u || !mira_db_handles[handle - 1u].used) {
    return;
  }
  mira_db_handles[handle - 1u].last_error_code = 0u;
  mira_db_handles[handle - 1u].last_error_retryable = 0;
}

static buf_u8 mira_json_first_array_object(buf_u8 value) {
  if (value.data == NULL || value.len < 2u || value.data[0] != '[') {
    return mira_buf_empty();
  }
  size_t index = 1u;
  while (index < value.len && (value.data[index] == ' ' || value.data[index] == '\n' || value.data[index] == '\r' || value.data[index] == '\t')) {
    index += 1u;
  }
  if (index >= value.len || value.data[index] != '{') {
    return mira_buf_empty();
  }
  size_t start = index;
  int depth = 0;
  int in_string = 0;
  int escaped = 0;
  for (; index < value.len; index++) {
    uint8_t ch = value.data[index];
    if (in_string) {
      if (escaped) {
        escaped = 0;
      } else if (ch == '\\') {
        escaped = 1;
      } else if (ch == '"') {
        in_string = 0;
      }
      continue;
    }
    if (ch == '"') {
      in_string = 1;
      continue;
    }
    if (ch == '{') {
      depth += 1;
    } else if (ch == '}') {
      depth -= 1;
      if (depth == 0) {
        return mira_buf_copy_bytes(value.data + start, index - start + 1u);
      }
    }
  }
  return mira_buf_empty();
}

static buf_u8 mira_sqlite_query_row_capture(const char* target, buf_u8 sql, int* ok) {
#ifdef _WIN32
  (void) target;
  (void) sql;
  if (ok != NULL) {
    *ok = 0;
  }
  return mira_buf_empty();
#else
  if (ok != NULL) {
    *ok = 0;
  }
  char* quoted_target = mira_shell_escape_single_quoted(target);
  char* sql_text = mira_buf_to_cstring(sql);
  char* quoted_sql = mira_shell_escape_single_quoted(sql_text);
  free(sql_text);
  if (quoted_target == NULL || quoted_sql == NULL) {
    free(quoted_target);
    free(quoted_sql);
    return mira_buf_empty();
  }
  char out_path[512];
  int out_fd = mira_mkstemp_path(out_path, sizeof(out_path), "/tmp/mira_sqlite_row_XXXXXX");
  if (out_fd < 0) {
    free(quoted_target);
    free(quoted_sql);
    return mira_buf_empty();
  }
  close(out_fd);
  char* quoted_out = mira_shell_escape_single_quoted(out_path);
  if (quoted_out == NULL) {
    free(quoted_target);
    free(quoted_sql);
    remove(out_path);
    return mira_buf_empty();
  }
  char command[8192];
  snprintf(command, sizeof(command), "sqlite3 -json %s %s > %s", quoted_target, quoted_sql, quoted_out);
  free(quoted_target);
  free(quoted_sql);
  free(quoted_out);
  int status = system(command);
  if (status != 0) {
    remove(out_path);
    return mira_buf_empty();
  }
  buf_u8 out = mira_read_file_all_bytes(out_path);
  remove(out_path);
  out = mira_trim_db_output(out);
  out = mira_json_first_array_object(out);
  if (ok != NULL) {
    *ok = 1;
  }
  return out;
#endif
}

static buf_u8 mira_postgres_query_row_capture(const char* target, buf_u8 sql, int* ok) {
  char* sql_text = mira_buf_to_cstring(sql);
  if (sql_text == NULL) {
    if (ok != NULL) {
      *ok = 0;
    }
    return mira_buf_empty();
  }
  size_t wrapped_len = strlen(sql_text) + 96u;
  char* wrapped = (char*) malloc(wrapped_len);
  if (wrapped == NULL) {
    free(sql_text);
    if (ok != NULL) {
      *ok = 0;
    }
    return mira_buf_empty();
  }
  snprintf(wrapped, wrapped_len, "SELECT COALESCE(row_to_json(t)::text, '{}') FROM (%s) AS t LIMIT 1;", sql_text);
  free(sql_text);
  buf_u8 wrapped_buf = mira_buf_copy_bytes((const uint8_t*) wrapped, strlen(wrapped));
  free(wrapped);
  buf_u8 out = mira_postgres_run_and_capture(target, wrapped_buf, ok);
  if (wrapped_buf.data != NULL) {
    free(wrapped_buf.data);
  }
  return out;
}

static buf_u8 mira_db_query_row_capture_target(const char* target, buf_u8 sql, int* ok) {
  if (mira_db_target_is_postgres(target)) {
    return mira_postgres_query_row_capture(target, sql, ok);
  }
  return mira_sqlite_query_row_capture(target, sql, ok);
}

static uint64_t mira_now_ms(void) {
  return (uint64_t) time(NULL) * 1000u;
}

static char* mira_buf_hex_encode(buf_u8 value) {
  size_t out_len = (size_t) value.len * 2u;
  char* out = (char*) malloc(out_len + 1u);
  if (out == NULL) {
    return NULL;
  }
  static const char hex[] = "0123456789abcdef";
  for (size_t index = 0u; index < value.len; index++) {
    out[index * 2u] = hex[(value.data[index] >> 4u) & 0xFu];
    out[index * 2u + 1u] = hex[value.data[index] & 0xFu];
  }
  out[out_len] = '\0';
  return out;
}

static buf_u8 mira_hex_decode_buf(const char* text) {
  if (text == NULL) {
    return mira_buf_empty();
  }
  size_t len = strlen(text);
  if ((len % 2u) != 0u) {
    return mira_buf_empty();
  }
  buf_u8 out = mira_buf_empty();
  if (len == 0u) {
    return out;
  }
  out.data = (uint8_t*) malloc(len / 2u);
  if (out.data == NULL) {
    return mira_buf_empty();
  }
  out.len = (uint32_t) (len / 2u);
  out.cap = out.len;
  for (size_t index = 0u; index < len; index += 2u) {
    char chunk[3];
    chunk[0] = text[index];
    chunk[1] = text[index + 1u];
    chunk[2] = '\0';
    out.data[index / 2u] = (uint8_t) strtoul(chunk, NULL, 16);
  }
  return out;
}

static int mira_append_text_line(const char* path, const char* line) {
  if (path == NULL || line == NULL) {
    return 0;
  }
  FILE* file = fopen(path, "ab");
  if (file == NULL) {
    return 0;
  }
  int ok = fputs(line, file) >= 0 && fputc('\n', file) != EOF;
  fclose(file);
  return ok;
}

bool mira_cache_set_buf_ttl_handle_key_value_u8(uint64_t handle, buf_u8 key, uint32_t ttl_ms, buf_u8 value);

static void mira_db_stmt_clear(mira_db_stmt_entry* entry) {
  if (entry == NULL) {
    return;
  }
  memset(entry, 0, sizeof(*entry));
}

static void mira_db_handle_reset(mira_db_handle_entry* entry) {
  if (entry == NULL) {
    return;
  }
  if (entry->tx_sql.data != NULL) {
    free(entry->tx_sql.data);
  }
  for (uint32_t index = 0u; index < entry->prepared_len && index < 32u; index++) {
    mira_db_stmt_clear(&entry->prepared[index]);
  }
  memset(entry, 0, sizeof(*entry));
}

static int mira_db_stmt_find_index(mira_db_handle_entry* entry, const char* name) {
  if (entry == NULL || name == NULL) {
    return -1;
  }
  for (uint32_t index = 0u; index < entry->prepared_len && index < 32u; index++) {
    if (entry->prepared[index].used && strcmp(entry->prepared[index].name, name) == 0) {
      return (int) index;
    }
  }
  return -1;
}

static int mira_db_stmt_upsert(mira_db_handle_entry* entry, const char* name, const char* sql) {
  if (entry == NULL || name == NULL || sql == NULL) {
    return 0;
  }
  int existing = mira_db_stmt_find_index(entry, name);
  uint32_t index = existing >= 0 ? (uint32_t) existing : entry->prepared_len;
  if (index >= 32u) {
    return 0;
  }
  entry->prepared[index].used = true;
  strncpy(entry->prepared[index].name, name, sizeof(entry->prepared[index].name) - 1u);
  entry->prepared[index].name[sizeof(entry->prepared[index].name) - 1u] = '\0';
  strncpy(entry->prepared[index].sql, sql, sizeof(entry->prepared[index].sql) - 1u);
  entry->prepared[index].sql[sizeof(entry->prepared[index].sql) - 1u] = '\0';
  if (existing < 0) {
    entry->prepared_len += 1u;
  }
  return 1;
}

static char* mira_sql_literal_from_json_token(const char* token, size_t len) {
  while (len > 0u && (*token == ' ' || *token == '\n' || *token == '\r' || *token == '\t')) {
    token += 1u;
    len -= 1u;
  }
  while (len > 0u && (token[len - 1u] == ' ' || token[len - 1u] == '\n' || token[len - 1u] == '\r' || token[len - 1u] == '\t')) {
    len -= 1u;
  }
  if (len == 0u) {
    char* out = (char*) malloc(5u);
    if (out != NULL) {
      memcpy(out, "NULL", 5u);
    }
    return out;
  }
  if (len == 4u && strncmp(token, "null", 4u) == 0) {
    char* out = (char*) malloc(5u);
    if (out != NULL) {
      memcpy(out, "NULL", 5u);
    }
    return out;
  }
  if (len == 4u && strncmp(token, "true", 4u) == 0) {
    char* out = (char*) malloc(5u);
    if (out != NULL) {
      memcpy(out, "TRUE", 5u);
    }
    return out;
  }
  if (len == 5u && strncmp(token, "false", 5u) == 0) {
    char* out = (char*) malloc(6u);
    if (out != NULL) {
      memcpy(out, "FALSE", 6u);
    }
    return out;
  }
  if (token[0] == '"') {
    char* out = (char*) malloc(len * 2u + 3u);
    if (out == NULL) {
      return NULL;
    }
    size_t cursor = 0u;
    out[cursor++] = '\'';
    for (size_t index = 1u; index + 1u < len; index++) {
      char ch = token[index];
      if (ch == '\\' && index + 1u < len - 1u) {
        char next = token[index + 1u];
        switch (next) {
          case 'n': out[cursor++] = '\n'; break;
          case 'r': out[cursor++] = '\r'; break;
          case 't': out[cursor++] = '\t'; break;
          case '"': out[cursor++] = '"'; break;
          case '\\': out[cursor++] = '\\'; break;
          default: out[cursor++] = next; break;
        }
        index += 1u;
        continue;
      }
      if (ch == '\'') {
        out[cursor++] = '\'';
        out[cursor++] = '\'';
      } else {
        out[cursor++] = ch;
      }
    }
    out[cursor++] = '\'';
    out[cursor] = '\0';
    return out;
  }
  char* out = (char*) malloc(len + 1u);
  if (out == NULL) {
    return NULL;
  }
  memcpy(out, token, len);
  out[len] = '\0';
  return out;
}

static char* mira_db_expand_prepared_sql_text(const char* template_sql, const char* params_json) {
  if (template_sql == NULL || params_json == NULL) {
    return NULL;
  }
  size_t params_len = strlen(params_json);
  if (params_len < 2u || params_json[0] != '[' || params_json[params_len - 1u] != ']') {
    return NULL;
  }
  char* literals[32];
  size_t literal_count = 0u;
  memset(literals, 0, sizeof(literals));
  const char* cursor = params_json + 1u;
  const char* end = params_json + params_len - 1u;
  while (cursor < end) {
    while (cursor < end && (*cursor == ' ' || *cursor == '\n' || *cursor == '\r' || *cursor == '\t' || *cursor == ',')) {
      cursor += 1u;
    }
    if (cursor >= end) {
      break;
    }
    const char* token_start = cursor;
    if (*cursor == '"') {
      cursor += 1u;
      while (cursor < end) {
        if (*cursor == '\\' && cursor + 1u < end) {
          cursor += 2u;
          continue;
        }
        if (*cursor == '"') {
          cursor += 1u;
          break;
        }
        cursor += 1u;
      }
    } else {
      while (cursor < end && *cursor != ',') {
        cursor += 1u;
      }
    }
    if (literal_count >= 32u) {
      break;
    }
    literals[literal_count] = mira_sql_literal_from_json_token(token_start, (size_t) (cursor - token_start));
    if (literals[literal_count] == NULL) {
      for (size_t index = 0u; index < literal_count; index++) {
        free(literals[index]);
      }
      return NULL;
    }
    literal_count += 1u;
  }
  size_t out_cap = strlen(template_sql) + 1u;
  for (size_t index = 0u; index < literal_count; index++) {
    out_cap += strlen(literals[index]) + 8u;
  }
  char* out = (char*) malloc(out_cap);
  if (out == NULL) {
    for (size_t index = 0u; index < literal_count; index++) {
      free(literals[index]);
    }
    return NULL;
  }
  size_t cursor_out = 0u;
  for (size_t index = 0u; template_sql[index] != '\0'; index++) {
    if (template_sql[index] == '$' && template_sql[index + 1u] >= '1' && template_sql[index + 1u] <= '9') {
      size_t placeholder = 0u;
      size_t digit = index + 1u;
      while (template_sql[digit] >= '0' && template_sql[digit] <= '9') {
        placeholder = placeholder * 10u + (size_t) (template_sql[digit] - '0');
        digit += 1u;
      }
      if (placeholder > 0u && placeholder <= literal_count) {
        const char* replacement = literals[placeholder - 1u];
        size_t replacement_len = strlen(replacement);
        memcpy(out + cursor_out, replacement, replacement_len);
        cursor_out += replacement_len;
        index = digit - 1u;
        continue;
      }
    }
    out[cursor_out++] = template_sql[index];
  }
  out[cursor_out] = '\0';
  for (size_t index = 0u; index < literal_count; index++) {
    free(literals[index]);
  }
  return out;
}

static const char* mira_http_reason_phrase(uint32_t status) {
  switch (status) {
    case 200u: return "OK";
    case 201u: return "Created";
    case 204u: return "No Content";
    case 400u: return "Bad Request";
    case 401u: return "Unauthorized";
    case 403u: return "Forbidden";
    case 404u: return "Not Found";
    case 500u: return "Internal Server Error";
    default: return "OK";
  }
}

static void mira_parse_http_request_line(buf_u8 request, const char** method, size_t* method_len, const char** path, size_t* path_len) {
  *method = NULL;
  *path = NULL;
  *method_len = 0u;
  *path_len = 0u;
  size_t end = 0u;
  while (end + 1u < request.len) {
    if (request.data[end] == '\r' && request.data[end + 1u] == '\n') {
      break;
    }
    end += 1u;
  }
  size_t first_space = 0u;
  while (first_space < end && request.data[first_space] != ' ') {
    first_space += 1u;
  }
  if (first_space == 0u || first_space >= end) {
    return;
  }
  size_t second_space = first_space + 1u;
  while (second_space < end && request.data[second_space] != ' ') {
    second_space += 1u;
  }
  if (second_space <= first_space + 1u || second_space > end) {
    return;
  }
  *method = (const char*) request.data;
  *method_len = first_space;
  *path = (const char*) request.data + first_space + 1u;
  *path_len = second_space - first_space - 1u;
}

static size_t mira_http_path_without_query_len(const char* path, size_t path_len) {
  size_t index = 0u;
  while (index < path_len) {
    if (path[index] == '?') {
      return index;
    }
    index += 1u;
  }
  return path_len;
}

static int mira_http_route_match_segment(const char* actual, size_t actual_len, const char* expected, size_t expected_len) {
  return actual_len == expected_len && memcmp(actual, expected, actual_len) == 0;
}

static buf_u8 mira_http_route_param_from_path(const char* path, size_t path_len, const char* pattern, const char* param) {
  if (path == NULL || pattern == NULL || param == NULL) {
    return mira_buf_empty();
  }
  size_t actual_index = 0u;
  size_t pattern_index = 0u;
  while (actual_index < path_len || pattern[pattern_index] != '\0') {
    while (actual_index < path_len && path[actual_index] == '/') {
      actual_index += 1u;
    }
    while (pattern[pattern_index] == '/') {
      pattern_index += 1u;
    }
    if (actual_index >= path_len || pattern[pattern_index] == '\0') {
      break;
    }
    size_t actual_end = actual_index;
    while (actual_end < path_len && path[actual_end] != '/') {
      actual_end += 1u;
    }
    size_t pattern_end = pattern_index;
    while (pattern[pattern_end] != '\0' && pattern[pattern_end] != '/') {
      pattern_end += 1u;
    }
    if (pattern[pattern_index] == ':') {
      const char* pattern_name = pattern + pattern_index + 1u;
      size_t pattern_name_len = pattern_end - pattern_index - 1u;
      size_t param_len = strlen(param);
      if (pattern_name_len == param_len && memcmp(pattern_name, param, param_len) == 0) {
        return mira_buf_copy_bytes((const uint8_t*) (path + actual_index), actual_end - actual_index);
      }
    } else if (!mira_http_route_match_segment(path + actual_index, actual_end - actual_index, pattern + pattern_index, pattern_end - pattern_index)) {
      return mira_buf_empty();
    }
    actual_index = actual_end;
    pattern_index = pattern_end;
  }
  while (actual_index < path_len && path[actual_index] == '/') {
    actual_index += 1u;
  }
  while (pattern[pattern_index] == '/') {
    pattern_index += 1u;
  }
  if (actual_index != path_len || pattern[pattern_index] != '\0') {
    return mira_buf_empty();
  }
  return mira_buf_empty();
}

static int mira_http_header_section(buf_u8 request, const uint8_t** start, size_t* len, const uint8_t** body) {
  size_t line_end = 0u;
  while (line_end + 1u < request.len) {
    if (request.data[line_end] == '\r' && request.data[line_end + 1u] == '\n') {
      line_end += 2u;
      break;
    }
    line_end += 1u;
  }
  size_t split = line_end;
  while (split + 3u < request.len) {
    if (request.data[split] == '\r' && request.data[split + 1u] == '\n' &&
        request.data[split + 2u] == '\r' && request.data[split + 3u] == '\n') {
      *start = request.data + line_end;
      *len = split >= line_end ? split - line_end : 0u;
      *body = request.data + split + 4u;
      return 1;
    }
    split += 1u;
  }
  *start = NULL;
  *len = 0u;
  *body = request.data + request.len;
  return 0;
}

static const uint8_t* mira_json_find_value(buf_u8 value, const char* key) {
  char pattern[256];
  int pattern_len = snprintf(pattern, sizeof(pattern), "\"%s\"", key);
  if (pattern_len <= 0 || (size_t) pattern_len >= sizeof(pattern) || value.data == NULL) {
    return NULL;
  }
  size_t limit = value.len >= (uint32_t) pattern_len ? value.len - (size_t) pattern_len + 1u : 0u;
  for (size_t index = 0u; index < limit; index++) {
    if (memcmp(value.data + index, pattern, (size_t) pattern_len) != 0) {
      continue;
    }
    size_t cursor = index + (size_t) pattern_len;
    while (cursor < value.len && (value.data[cursor] == ' ' || value.data[cursor] == '\n' ||
                                  value.data[cursor] == '\r' || value.data[cursor] == '\t')) {
      cursor += 1u;
    }
    if (cursor >= value.len || value.data[cursor] != ':') {
      continue;
    }
    cursor += 1u;
    while (cursor < value.len && (value.data[cursor] == ' ' || value.data[cursor] == '\n' ||
                                  value.data[cursor] == '\r' || value.data[cursor] == '\t')) {
      cursor += 1u;
    }
    if (cursor < value.len) {
      return value.data + cursor;
    }
    return NULL;
  }
  return NULL;
}

static buf_u8 mira_json_copy_string(const uint8_t* value, size_t max_len) {
  if (value == NULL || max_len == 0u || value[0] != '"') {
    return mira_buf_empty();
  }
  size_t index = 1u;
  size_t out_len = 0u;
  uint8_t* data = (uint8_t*) malloc(max_len);
  if (data == NULL) {
    return mira_buf_empty();
  }
  while (index < max_len) {
    uint8_t ch = value[index];
    if (ch == '"') {
      return mira_buf_from_heap(data, out_len);
    }
    if (ch == '\\' && index + 1u < max_len) {
      index += 1u;
      switch (value[index]) {
        case '"': data[out_len++] = '"'; break;
        case '\\': data[out_len++] = '\\'; break;
        case 'n': data[out_len++] = '\n'; break;
        case 'r': data[out_len++] = '\r'; break;
        case 't': data[out_len++] = '\t'; break;
        default: data[out_len++] = value[index]; break;
      }
    } else {
      data[out_len++] = ch;
    }
    index += 1u;
  }
  free(data);
  return mira_buf_empty();
}

#ifdef _WIN32
static void mira_net_init(void) {
  static int initialized = 0;
  if (!initialized) {
    WSADATA wsa;
    WSAStartup(MAKEWORD(2, 2), &wsa);
    initialized = 1;
  }
}
#endif

uint64_t mira_rt_clock_now_ns(void) {
#if defined(_WIN32)
  LARGE_INTEGER freq;
  LARGE_INTEGER counter;
  QueryPerformanceFrequency(&freq);
  QueryPerformanceCounter(&counter);
  return (uint64_t) ((counter.QuadPart * 1000000000ULL) / freq.QuadPart);
#else
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return ((uint64_t) ts.tv_sec * 1000000000ULL) + (uint64_t) ts.tv_nsec;
#endif
}

uint32_t mira_rt_rand_next_u32(uint32_t* state) {
  uint32_t x = *state;
  if (x == 0u) { x = 2463534242u; }
  x ^= x << 13;
  x ^= x >> 17;
  x ^= x << 5;
  *state = x;
  return x;
}

uint32_t mira_rt_fs_read_u32(const char* path) {
  FILE* file = fopen(path, "r");
  if (file == NULL) { return 0u; }
  uint32_t value = 0u;
  if (fscanf(file, "%" SCNu32, &value) != 1) {
    fclose(file);
    return 0u;
  }
  fclose(file);
  return value;
}

uint32_t mira_rt_fs_write_u32(const char* path, uint32_t value) {
  FILE* file = fopen(path, "w");
  if (file == NULL) { return 0u; }
  int wrote = fprintf(file, "%" PRIu32 "\n", value);
  int closed = fclose(file);
  return (uint32_t) (wrote > 0 && closed == 0);
}

int32_t mira_rt_spawn_status(const char* command) {
  int status = system(command);
  if (status == -1) {
    return -1;
  }
#ifdef WIFEXITED
  if (WIFEXITED(status)) {
    return (int32_t) WEXITSTATUS(status);
  }
#endif
  return (int32_t) status;
}

uint32_t mira_rt_net_connect_ok(const char* host, uint32_t port) {
#ifdef _WIN32
  mira_net_init();
#endif
  char port_buf[16];
  snprintf(port_buf, sizeof(port_buf), "%u", (unsigned) port);
  struct addrinfo hints;
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  struct addrinfo* result = NULL;
  if (getaddrinfo(host, port_buf, &hints, &result) != 0) {
    return 0u;
  }
  bool ok = false;
  for (struct addrinfo* it = result; it != NULL; it = it->ai_next) {
    mira_socket_t fd = socket(it->ai_family, it->ai_socktype, it->ai_protocol);
    if (fd == MIRA_INVALID_SOCKET) {
      continue;
    }
    if (connect(fd, it->ai_addr, (int) it->ai_addrlen) == 0) {
      ok = true;
      MIRA_CLOSE_SOCKET(fd);
      break;
    }
    MIRA_CLOSE_SOCKET(fd);
  }
  freeaddrinfo(result);
  return (uint32_t) ok;
}

uint64_t mira_net_listen_handle(const char* host, uint16_t port) {
#ifdef _WIN32
  mira_net_init();
#endif
  char port_buf[16];
  snprintf(port_buf, sizeof(port_buf), "%u", (unsigned) port);
  struct addrinfo hints;
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = AI_PASSIVE;
  struct addrinfo* result = NULL;
  if (getaddrinfo(host, port_buf, &hints, &result) != 0) {
    return 0u;
  }
  mira_socket_t listener = MIRA_INVALID_SOCKET;
  for (struct addrinfo* it = result; it != NULL; it = it->ai_next) {
    listener = socket(it->ai_family, it->ai_socktype, it->ai_protocol);
    if (listener == MIRA_INVALID_SOCKET) {
      continue;
    }
    int reuse = 1;
    setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, (const char*) &reuse, sizeof(reuse));
    if (bind(listener, it->ai_addr, (int) it->ai_addrlen) == 0 && listen(listener, 4) == 0) {
      break;
    }
    MIRA_CLOSE_SOCKET(listener);
    listener = MIRA_INVALID_SOCKET;
  }
  freeaddrinfo(result);
  if (listener == MIRA_INVALID_SOCKET) {
    return 0u;
  }
  bool used_flags[256];
  for (size_t index = 0; index < 256; index++) {
    used_flags[index] = mira_net_handles[index].used;
  }
  uint64_t handle = mira_alloc_handle(used_flags, 256u);
  if (handle == 0u) {
    MIRA_CLOSE_SOCKET(listener);
    return 0u;
  }
  memset(&mira_net_handles[handle - 1u], 0, sizeof(mira_net_handles[handle - 1u]));
  mira_net_handles[handle - 1u].used = true;
  mira_net_handles[handle - 1u].kind = MIRA_NET_KIND_PLAIN_LISTENER;
  mira_net_handles[handle - 1u].fd = listener;
  mira_net_handles[handle - 1u].timeout_ms = 5000u;
  mira_net_handles[handle - 1u].shutdown_grace_ms = 250u;
  mira_net_handles[handle - 1u].pending_bytes = 0u;
  mira_net_handles[handle - 1u].resume_id = 0u;
  mira_net_handles[handle - 1u].reconnectable = 0u;
  return handle;
}

uint64_t mira_net_accept_handle(uint64_t handle) {
  if (handle == 0u || handle > 256u || !mira_net_handles[handle - 1u].used || mira_net_handles[handle - 1u].kind != MIRA_NET_KIND_PLAIN_LISTENER) {
    return 0u;
  }
  mira_socket_t accepted = accept(mira_net_handles[handle - 1u].fd, NULL, NULL);
  if (accepted == MIRA_INVALID_SOCKET) {
    return 0u;
  }
  bool used_flags[256];
  for (size_t index = 0; index < 256; index++) {
    used_flags[index] = mira_net_handles[index].used;
  }
  uint64_t out = mira_alloc_handle(used_flags, 256u);
  if (out == 0u) {
    MIRA_CLOSE_SOCKET(accepted);
    return 0u;
  }
  memset(&mira_net_handles[out - 1u], 0, sizeof(mira_net_handles[out - 1u]));
  mira_net_handles[out - 1u].used = true;
  mira_net_handles[out - 1u].kind = MIRA_NET_KIND_PLAIN_STREAM;
  mira_net_handles[out - 1u].fd = accepted;
  mira_net_handles[out - 1u].timeout_ms = mira_net_handles[handle - 1u].timeout_ms;
  mira_net_handles[out - 1u].pending_bytes = 0u;
  mira_net_handles[out - 1u].resume_id = (uint64_t) out;
  mira_net_handles[out - 1u].reconnectable = 0u;
  return out;
}

uint64_t mira_net_session_open_handle(const char* host, uint16_t port) {
#ifdef _WIN32
  mira_net_init();
#endif
  if (host == NULL) {
    return 0u;
  }
  char port_buf[16];
  snprintf(port_buf, sizeof(port_buf), "%u", (unsigned) port);
  struct addrinfo hints;
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  struct addrinfo* result = NULL;
  if (getaddrinfo(host, port_buf, &hints, &result) != 0) {
    return 0u;
  }
  mira_socket_t stream = MIRA_INVALID_SOCKET;
  for (struct addrinfo* it = result; it != NULL; it = it->ai_next) {
    stream = socket(it->ai_family, it->ai_socktype, it->ai_protocol);
    if (stream == MIRA_INVALID_SOCKET) {
      continue;
    }
    if (connect(stream, it->ai_addr, (int) it->ai_addrlen) == 0) {
      break;
    }
    MIRA_CLOSE_SOCKET(stream);
    stream = MIRA_INVALID_SOCKET;
  }
  freeaddrinfo(result);
  if (stream == MIRA_INVALID_SOCKET) {
    return 0u;
  }
  bool used_flags[256];
  for (size_t index = 0; index < 256; index++) {
    used_flags[index] = mira_net_handles[index].used;
  }
  uint64_t handle = mira_alloc_handle(used_flags, 256u);
  if (handle == 0u) {
    MIRA_CLOSE_SOCKET(stream);
    return 0u;
  }
  memset(&mira_net_handles[handle - 1u], 0, sizeof(mira_net_handles[handle - 1u]));
  mira_net_handles[handle - 1u].used = true;
  mira_net_handles[handle - 1u].kind = MIRA_NET_KIND_PLAIN_STREAM;
  mira_net_handles[handle - 1u].fd = stream;
  mira_net_handles[handle - 1u].timeout_ms = 5000u;
  mira_net_handles[handle - 1u].shutdown_grace_ms = 250u;
  strncpy(
      mira_net_handles[handle - 1u].reconnect_host,
      host,
      sizeof(mira_net_handles[handle - 1u].reconnect_host) - 1u);
  mira_net_handles[handle - 1u].reconnect_host[sizeof(mira_net_handles[handle - 1u].reconnect_host) - 1u] = '\0';
  mira_net_handles[handle - 1u].reconnect_port = port;
  mira_net_handles[handle - 1u].pending_bytes = 0u;
  mira_net_handles[handle - 1u].resume_id = mira_session_resume_hash(host, port);
  mira_net_handles[handle - 1u].reconnectable = 1u;
  return handle;
}

uint64_t mira_tls_listen_handle(const char* host, uint16_t port, const char* cert, const char* key, uint32_t request_timeout_ms, uint32_t session_timeout_ms, uint32_t shutdown_grace_ms) {
#ifdef _WIN32
  (void) host;
  (void) port;
  (void) cert;
  (void) key;
  (void) request_timeout_ms;
  (void) session_timeout_ms;
  (void) shutdown_grace_ms;
  return 0u;
#else
  if (host == NULL || cert == NULL || key == NULL) {
    return 0u;
  }
  int stdin_pipe[2];
  int stdout_pipe[2];
  if (pipe(stdin_pipe) != 0 || pipe(stdout_pipe) != 0) {
    return 0u;
  }
  int pid = fork();
  if (pid < 0) {
    close(stdin_pipe[0]);
    close(stdin_pipe[1]);
    close(stdout_pipe[0]);
    close(stdout_pipe[1]);
    return 0u;
  }
  if (pid == 0) {
    dup2(stdin_pipe[0], STDIN_FILENO);
    dup2(stdout_pipe[1], STDOUT_FILENO);
    close(stdin_pipe[0]);
    close(stdin_pipe[1]);
    close(stdout_pipe[0]);
    close(stdout_pipe[1]);
    char accept_arg[256];
    snprintf(accept_arg, sizeof(accept_arg), "%s:%u", host, (unsigned) port);
    execlp(
        "openssl",
        "openssl",
        "s_server",
        "-accept",
        accept_arg,
        "-key",
        key,
        "-cert",
        cert,
        "-quiet",
        "-naccept",
        "1",
        (char*) NULL);
    _exit(127);
  }
  close(stdin_pipe[0]);
  close(stdout_pipe[1]);
  bool used_flags[256];
  for (size_t index = 0; index < 256u; index++) {
    used_flags[index] = mira_net_handles[index].used;
  }
  uint64_t handle = mira_alloc_handle(used_flags, 256u);
  if (handle == 0u) {
    close(stdin_pipe[1]);
    close(stdout_pipe[0]);
    kill(pid, SIGKILL);
    waitpid(pid, NULL, 0);
    return 0u;
  }
  memset(&mira_net_handles[handle - 1u], 0, sizeof(mira_net_handles[handle - 1u]));
  mira_net_handles[handle - 1u].used = true;
  mira_net_handles[handle - 1u].kind = MIRA_NET_KIND_TLS_LISTENER;
  mira_net_handles[handle - 1u].tls_stdin_fd = stdin_pipe[1];
  mira_net_handles[handle - 1u].tls_stdout_fd = stdout_pipe[0];
  mira_net_handles[handle - 1u].tls_pid = pid;
  mira_net_handles[handle - 1u].timeout_ms = session_timeout_ms;
  mira_net_handles[handle - 1u].shutdown_grace_ms = shutdown_grace_ms;
  mira_net_handles[handle - 1u].fd = MIRA_INVALID_SOCKET;
  mira_net_handles[handle - 1u].tls_accepted = 0;
  mira_net_handles[handle - 1u].pending_bytes = 0u;
  mira_net_handles[handle - 1u].resume_id = mira_session_resume_hash(host, port);
  mira_net_handles[handle - 1u].reconnectable = 0u;
  if (request_timeout_ms > 0u) {
    usleep((useconds_t) (request_timeout_ms > 250u ? 250u : request_timeout_ms) * 1000u);
  }
  return handle;
#endif
}

uint64_t mira_http_session_accept_handle(uint64_t handle) {
  if (handle == 0u || handle > 256u || !mira_net_handles[handle - 1u].used) {
    return 0u;
  }
  if (mira_net_handles[handle - 1u].kind == MIRA_NET_KIND_PLAIN_LISTENER) {
    return mira_net_accept_handle(handle);
  }
  if (mira_net_handles[handle - 1u].kind != MIRA_NET_KIND_TLS_LISTENER) {
    return 0u;
  }
  if (mira_net_handles[handle - 1u].tls_accepted) {
    return 0u;
  }
  bool used_flags[256];
  for (size_t index = 0; index < 256u; index++) {
    used_flags[index] = mira_net_handles[index].used;
  }
  uint64_t out = mira_alloc_handle(used_flags, 256u);
  if (out == 0u) {
    return 0u;
  }
  memset(&mira_net_handles[out - 1u], 0, sizeof(mira_net_handles[out - 1u]));
  mira_net_handles[out - 1u].used = true;
  mira_net_handles[out - 1u].kind = MIRA_NET_KIND_TLS_SESSION;
  mira_net_handles[out - 1u].tls_stdin_fd = mira_net_handles[handle - 1u].tls_stdin_fd;
  mira_net_handles[out - 1u].tls_stdout_fd = mira_net_handles[handle - 1u].tls_stdout_fd;
  mira_net_handles[out - 1u].tls_pid = mira_net_handles[handle - 1u].tls_pid;
  mira_net_handles[out - 1u].timeout_ms = mira_net_handles[handle - 1u].timeout_ms;
  mira_net_handles[out - 1u].shutdown_grace_ms = mira_net_handles[handle - 1u].shutdown_grace_ms;
  mira_net_handles[out - 1u].pending_bytes = 0u;
  mira_net_handles[out - 1u].resume_id = mira_net_handles[handle - 1u].resume_id;
  mira_net_handles[out - 1u].reconnectable = 0u;
  mira_net_handles[handle - 1u].tls_accepted = 1;
  mira_net_handles[handle - 1u].tls_stdin_fd = -1;
  mira_net_handles[handle - 1u].tls_stdout_fd = -1;
  mira_net_handles[handle - 1u].tls_pid = 0;
  return out;
}

buf_u8 mira_net_read_all_handle_buf_u8(uint64_t handle) {
  if (handle == 0u || handle > 256u || !mira_net_handles[handle - 1u].used || mira_net_handles[handle - 1u].kind != MIRA_NET_KIND_PLAIN_STREAM) {
    return mira_buf_empty();
  }
  return mira_read_all_fd(mira_net_handles[handle - 1u].fd);
}

buf_u8 mira_http_session_request_buf_u8(uint64_t handle) {
  if (handle == 0u || handle > 256u || !mira_net_handles[handle - 1u].used) {
    return mira_buf_empty();
  }
  if (mira_net_handles[handle - 1u].kind == MIRA_NET_KIND_TLS_SESSION) {
#ifdef _WIN32
    return mira_buf_empty();
#else
    if (mira_net_handles[handle - 1u].tls_stdout_fd < 0) {
      return mira_buf_empty();
    }
    buf_u8 out = mira_read_http_message_pipe_fd(mira_net_handles[handle - 1u].tls_stdout_fd);
    close(mira_net_handles[handle - 1u].tls_stdout_fd);
    mira_net_handles[handle - 1u].tls_stdout_fd = -1;
    return out;
#endif
  }
  return mira_net_read_all_handle_buf_u8(handle);
}

bool mira_net_write_handle_all_buf_u8(uint64_t handle, buf_u8 value) {
  if (handle == 0u || handle > 256u || !mira_net_handles[handle - 1u].used) {
    return 0u;
  }
  if (mira_net_handles[handle - 1u].kind == MIRA_NET_KIND_TLS_SESSION) {
#ifdef _WIN32
    return 0u;
#else
    if (mira_net_handles[handle - 1u].tls_stdin_fd < 0) {
      return 0u;
    }
    return mira_write_all_pipe_fd(mira_net_handles[handle - 1u].tls_stdin_fd, value.data, value.len);
#endif
  }
  if (mira_net_handles[handle - 1u].kind != MIRA_NET_KIND_PLAIN_STREAM) {
    return 0u;
  }
  return mira_write_all_fd(mira_net_handles[handle - 1u].fd, value.data, value.len);
}

buf_u8 mira_session_read_chunk_buf_u8(uint64_t handle, uint32_t chunk_len) {
  if (handle == 0u || handle > 256u || !mira_net_handles[handle - 1u].used) {
    return mira_buf_empty();
  }
  if (mira_net_handles[handle - 1u].kind == MIRA_NET_KIND_TLS_SESSION) {
#ifdef _WIN32
    return mira_buf_empty();
#else
    if (mira_net_handles[handle - 1u].tls_stdout_fd < 0) {
      return mira_buf_empty();
    }
    size_t len = chunk_len == 0u ? 0u : (size_t) chunk_len;
    if (len == 0u) {
      return mira_buf_empty();
    }
    uint8_t* data = (uint8_t*) malloc(len);
    if (data == NULL) {
      return mira_buf_empty();
    }
    ssize_t got = read(mira_net_handles[handle - 1u].tls_stdout_fd, data, len);
    if (got <= 0) {
      free(data);
      return mira_buf_empty();
    }
    return mira_buf_from_heap(data, (size_t) got);
#endif
  }
  if (mira_net_handles[handle - 1u].kind != MIRA_NET_KIND_PLAIN_STREAM) {
    return mira_buf_empty();
  }
  return mira_read_chunk_fd(mira_net_handles[handle - 1u].fd, chunk_len);
}

bool mira_session_write_chunk_buf_u8(uint64_t handle, buf_u8 value) {
  if (handle == 0u || handle > 256u || !mira_net_handles[handle - 1u].used) {
    return 0u;
  }
  if (!mira_net_is_session_kind(mira_net_handles[handle - 1u].kind)) {
    return 0u;
  }
  bool ok = 0u;
  if (mira_net_handles[handle - 1u].kind == MIRA_NET_KIND_TLS_SESSION) {
#ifdef _WIN32
    ok = 0u;
#else
    if (mira_net_handles[handle - 1u].tls_stdin_fd >= 0) {
      ok = mira_write_all_pipe_fd(mira_net_handles[handle - 1u].tls_stdin_fd, value.data, value.len);
    }
#endif
  } else {
    ok = mira_write_all_fd(mira_net_handles[handle - 1u].fd, value.data, value.len);
  }
  if (ok) {
    mira_net_handles[handle - 1u].pending_bytes = mira_net_handles[handle - 1u]
                                                      .pending_bytes +
                                                  value.len;
  }
  return ok;
}

bool mira_session_flush_handle(uint64_t handle) {
  if (handle == 0u || handle > 256u || !mira_net_handles[handle - 1u].used) {
    return 0u;
  }
  if (!mira_net_is_session_kind(mira_net_handles[handle - 1u].kind)) {
    return 0u;
  }
  mira_net_handles[handle - 1u].pending_bytes = 0u;
  return 1u;
}

bool mira_session_alive_handle(uint64_t handle) {
  if (handle == 0u || handle > 256u || !mira_net_handles[handle - 1u].used) {
    return 0u;
  }
  return (uint32_t) mira_net_is_session_kind(mira_net_handles[handle - 1u].kind);
}

bool mira_session_heartbeat_buf_u8(uint64_t handle, buf_u8 value) {
  if (!mira_session_write_chunk_buf_u8(handle, value)) {
    return 0u;
  }
  return mira_session_flush_handle(handle);
}

uint32_t mira_session_backpressure_u32(uint64_t handle) {
  if (handle == 0u || handle > 256u || !mira_net_handles[handle - 1u].used) {
    return 0u;
  }
  if (!mira_net_is_session_kind(mira_net_handles[handle - 1u].kind)) {
    return 0u;
  }
  return mira_net_handles[handle - 1u].pending_bytes;
}

bool mira_session_backpressure_wait(uint64_t handle, uint32_t max_pending) {
  if (handle == 0u || handle > 256u || !mira_net_handles[handle - 1u].used) {
    return 0u;
  }
  if (!mira_net_is_session_kind(mira_net_handles[handle - 1u].kind)) {
    return 0u;
  }
  if (mira_net_handles[handle - 1u].pending_bytes > max_pending) {
    mira_session_flush_handle(handle);
#ifdef _WIN32
    Sleep(1u);
#else
    usleep(1000u);
#endif
  }
  return (uint32_t) (mira_net_handles[handle - 1u].pending_bytes <= max_pending);
}

uint64_t mira_session_resume_id_u64(uint64_t handle) {
  if (handle == 0u || handle > 256u || !mira_net_handles[handle - 1u].used) {
    return 0u;
  }
  if (!mira_net_is_session_kind(mira_net_handles[handle - 1u].kind)) {
    return 0u;
  }
  return mira_net_handles[handle - 1u].resume_id;
}

bool mira_session_reconnect_handle(uint64_t handle) {
  if (handle == 0u || handle > 256u || !mira_net_handles[handle - 1u].used) {
    return 0u;
  }
  if (mira_net_handles[handle - 1u].kind != MIRA_NET_KIND_PLAIN_STREAM ||
      !mira_net_handles[handle - 1u].reconnectable ||
      mira_net_handles[handle - 1u].reconnect_host[0] == '\0') {
    return 0u;
  }
#ifdef _WIN32
  mira_net_init();
#endif
  char port_buf[16];
  snprintf(port_buf, sizeof(port_buf), "%u", (unsigned) mira_net_handles[handle - 1u].reconnect_port);
  struct addrinfo hints;
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  struct addrinfo* result = NULL;
  if (getaddrinfo(mira_net_handles[handle - 1u].reconnect_host, port_buf, &hints, &result) != 0) {
    return 0u;
  }
  mira_socket_t stream = MIRA_INVALID_SOCKET;
  for (struct addrinfo* it = result; it != NULL; it = it->ai_next) {
    stream = socket(it->ai_family, it->ai_socktype, it->ai_protocol);
    if (stream == MIRA_INVALID_SOCKET) {
      continue;
    }
    if (connect(stream, it->ai_addr, (int) it->ai_addrlen) == 0) {
      break;
    }
    MIRA_CLOSE_SOCKET(stream);
    stream = MIRA_INVALID_SOCKET;
  }
  freeaddrinfo(result);
  if (stream == MIRA_INVALID_SOCKET) {
    return 0u;
  }
  MIRA_CLOSE_SOCKET(mira_net_handles[handle - 1u].fd);
  mira_net_handles[handle - 1u].fd = stream;
  mira_net_handles[handle - 1u].pending_bytes = 0u;
  return 1u;
}

bool mira_net_close_handle(uint64_t handle) {
  if (handle == 0u || handle > 256u || !mira_net_handles[handle - 1u].used) {
    return 0u;
  }
  if (mira_net_handles[handle - 1u].kind == MIRA_NET_KIND_PLAIN_STREAM || mira_net_handles[handle - 1u].kind == MIRA_NET_KIND_PLAIN_LISTENER) {
    MIRA_CLOSE_SOCKET(mira_net_handles[handle - 1u].fd);
  }
#ifndef _WIN32
  if ((mira_net_handles[handle - 1u].kind == MIRA_NET_KIND_TLS_SESSION || mira_net_handles[handle - 1u].kind == MIRA_NET_KIND_TLS_LISTENER) && mira_net_handles[handle - 1u].tls_pid > 0) {
    if (mira_net_handles[handle - 1u].tls_stdin_fd >= 0) {
      close(mira_net_handles[handle - 1u].tls_stdin_fd);
      mira_net_handles[handle - 1u].tls_stdin_fd = -1;
    }
    if (mira_net_handles[handle - 1u].tls_stdout_fd >= 0) {
      close(mira_net_handles[handle - 1u].tls_stdout_fd);
      mira_net_handles[handle - 1u].tls_stdout_fd = -1;
    }
    int status = 0;
    int waited = 0;
    uint32_t grace_ms = mira_net_handles[handle - 1u].shutdown_grace_ms;
    for (uint32_t elapsed = 0u; elapsed < grace_ms; elapsed += 10u) {
      int rc = waitpid(mira_net_handles[handle - 1u].tls_pid, &status, WNOHANG);
      if (rc == mira_net_handles[handle - 1u].tls_pid) {
        waited = 1;
        break;
      }
      usleep(10u * 1000u);
    }
    if (!waited) {
      kill(mira_net_handles[handle - 1u].tls_pid, SIGKILL);
      waitpid(mira_net_handles[handle - 1u].tls_pid, &status, 0);
    }
  }
#endif
  memset(&mira_net_handles[handle - 1u], 0, sizeof(mira_net_handles[handle - 1u]));
  return 1u;
}

bool mira_http_session_close_handle(uint64_t handle) {
  return mira_net_close_handle(handle);
}

bool mira_listener_set_timeout_ms(uint64_t handle, uint32_t timeout_ms) {
  if (handle == 0u || handle > 256u || !mira_net_handles[handle - 1u].used) {
    return 0u;
  }
  mira_net_handles[handle - 1u].timeout_ms = timeout_ms;
  return 1u;
}

bool mira_session_set_timeout_ms(uint64_t handle, uint32_t timeout_ms) {
  if (handle == 0u || handle > 256u || !mira_net_handles[handle - 1u].used) {
    return 0u;
  }
  mira_net_handles[handle - 1u].timeout_ms = timeout_ms;
  return 1u;
}

bool mira_listener_set_shutdown_grace_ms(uint64_t handle, uint32_t grace_ms) {
  if (handle == 0u || handle > 256u || !mira_net_handles[handle - 1u].used) {
    return 0u;
  }
  mira_net_handles[handle - 1u].shutdown_grace_ms = grace_ms;
  return 1u;
}

bool mira_http_method_eq_buf_u8(buf_u8 request, const char* method) {
  const char* found = NULL;
  const char* path = NULL;
  size_t found_len = 0u;
  size_t path_len = 0u;
  mira_parse_http_request_line(request, &found, &found_len, &path, &path_len);
  if (found == NULL) {
    return 0u;
  }
  size_t wanted_len = strlen(method);
  return found_len == wanted_len && memcmp(found, method, wanted_len) == 0;
}

bool mira_http_path_eq_buf_u8(buf_u8 request, const char* path) {
  const char* method = NULL;
  const char* found = NULL;
  size_t method_len = 0u;
  size_t found_len = 0u;
  mira_parse_http_request_line(request, &method, &method_len, &found, &found_len);
  if (found == NULL) {
    return 0u;
  }
  found_len = mira_http_path_without_query_len(found, found_len);
  size_t wanted_len = strlen(path);
  return found_len == wanted_len && memcmp(found, path, wanted_len) == 0;
}

buf_u8 mira_http_request_method_buf_u8(buf_u8 request) {
  const char* method = NULL;
  const char* path = NULL;
  size_t method_len = 0u;
  size_t path_len = 0u;
  mira_parse_http_request_line(request, &method, &method_len, &path, &path_len);
  if (method == NULL) {
    return mira_buf_empty();
  }
  return mira_buf_copy_bytes((const uint8_t*) method, method_len);
}

buf_u8 mira_http_request_path_buf_u8(buf_u8 request) {
  const char* method = NULL;
  const char* path = NULL;
  size_t method_len = 0u;
  size_t path_len = 0u;
  mira_parse_http_request_line(request, &method, &method_len, &path, &path_len);
  if (path == NULL) {
    return mira_buf_empty();
  }
  path_len = mira_http_path_without_query_len(path, path_len);
  return mira_buf_copy_bytes((const uint8_t*) path, path_len);
}

buf_u8 mira_http_route_param_buf_u8(buf_u8 request, const char* pattern, const char* param) {
  const char* method = NULL;
  const char* path = NULL;
  size_t method_len = 0u;
  size_t path_len = 0u;
  mira_parse_http_request_line(request, &method, &method_len, &path, &path_len);
  if (path == NULL) {
    return mira_buf_empty();
  }
  path_len = mira_http_path_without_query_len(path, path_len);
  return mira_http_route_param_from_path(path, path_len, pattern, param);
}

bool mira_http_header_eq_buf_u8(buf_u8 request, const char* name, const char* value) {
  const uint8_t* headers = NULL;
  const uint8_t* body = NULL;
  size_t headers_len = 0u;
  if (!mira_http_header_section(request, &headers, &headers_len, &body) || headers == NULL) {
    return 0u;
  }
  const char* cursor = (const char*) headers;
  const char* end = (const char*) headers + headers_len;
  size_t expected_name_len = strlen(name);
  size_t expected_value_len = strlen(value);
  while (cursor < end) {
    const char* line_end = strstr(cursor, "\r\n");
    if (line_end == NULL || line_end > end) {
      line_end = end;
    }
    const char* colon = memchr(cursor, ':', (size_t) (line_end - cursor));
    if (colon != NULL) {
      size_t found_name_len = (size_t) (colon - cursor);
      const char* found_value = colon + 1;
      while (found_value < line_end && (*found_value == ' ' || *found_value == '\t')) {
        found_value += 1;
      }
      size_t found_value_len = (size_t) (line_end - found_value);
#ifdef _WIN32
      if (found_name_len == expected_name_len && _strnicmp(cursor, name, found_name_len) == 0 &&
          found_value_len == expected_value_len && memcmp(found_value, value, found_value_len) == 0) {
#else
      if (found_name_len == expected_name_len && strncasecmp(cursor, name, found_name_len) == 0 &&
          found_value_len == expected_value_len && memcmp(found_value, value, found_value_len) == 0) {
#endif
        return 1u;
      }
    }
    cursor = line_end;
    if (cursor < end && end - cursor >= 2 && cursor[0] == '\r' && cursor[1] == '\n') {
      cursor += 2;
    } else {
      break;
    }
  }
  return 0u;
}

buf_u8 mira_http_cookie_buf_u8(buf_u8 request, const char* name);

bool mira_http_cookie_eq_buf_u8(buf_u8 request, const char* name, const char* value) {
  buf_u8 found = mira_http_cookie_buf_u8(request, name);
  size_t expected_len = value == NULL ? 0u : strlen(value);
  bool ok = found.data != NULL && value != NULL && found.len == expected_len &&
            memcmp(found.data, value, found.len) == 0;
  if (found.data != NULL) {
    free(found.data);
  }
  return ok ? 1u : 0u;
}

uint32_t mira_http_status_u32_buf_u8(buf_u8 value) {
  if (value.data == NULL || value.len < 12u) {
    return 0u;
  }
  size_t line_len = 0u;
  while (line_len + 1u < value.len) {
    if (value.data[line_len] == '\r' && value.data[line_len + 1u] == '\n') {
      break;
    }
    line_len += 1u;
  }
  if (line_len == 0u || line_len >= 63u) {
    return 0u;
  }
  char line[64];
  memcpy(line, value.data, line_len);
  line[line_len] = '\0';
  unsigned status = 0u;
  if (sscanf(line, "HTTP/%*s %u", &status) != 1) {
    return 0u;
  }
  return (uint32_t) status;
}

bool mira_buf_eq_lit_u8(buf_u8 value, const char* literal) {
  if (value.data == NULL || literal == NULL) {
    return 0u;
  }
  buf_u8 decoded = mira_decode_escaped_literal(literal);
  bool ok = value.len == decoded.len &&
            ((value.len == 0u) || (decoded.data != NULL && memcmp(value.data, decoded.data, value.len) == 0));
  if (decoded.data != NULL) {
    free(decoded.data);
  }
  return ok;
}

bool mira_buf_contains_lit_u8(buf_u8 value, const char* literal) {
  if (value.data == NULL || literal == NULL) {
    return 0u;
  }
  buf_u8 decoded = mira_decode_escaped_literal(literal);
  if (decoded.len == 0u || decoded.data == NULL || decoded.len > value.len) {
    if (decoded.data != NULL) {
      free(decoded.data);
    }
    return 0u;
  }
  for (size_t index = 0u; index + decoded.len <= value.len; index++) {
    if (memcmp(value.data + index, decoded.data, decoded.len) == 0) {
      free(decoded.data);
      return 1u;
    }
  }
  free(decoded.data);
  return 0u;
}

uint32_t mira_buf_parse_u32_u8(buf_u8 value) {
  char stack_buf[64];
  size_t len = value.len < sizeof(stack_buf) - 1u ? value.len : sizeof(stack_buf) - 1u;
  memcpy(stack_buf, value.data, len);
  stack_buf[len] = '\0';
  return (uint32_t) strtoul(stack_buf, NULL, 10);
}

bool mira_buf_parse_bool_u8(buf_u8 value) {
  if (value.data == NULL || value.len == 0u) {
    return 0u;
  }
  if ((value.len == 4u && strncmp((const char*) value.data, "true", 4u) == 0)
      || (value.len == 1u && value.data[0] == '1')) {
    return 1u;
  }
  if ((value.len == 3u && strncmp((const char*) value.data, "yes", 3u) == 0)
      || (value.len == 2u && strncmp((const char*) value.data, "on", 2u) == 0)) {
    return 1u;
  }
  return 0u;
}

buf_u8 mira_str_from_u32(uint32_t value) {
  char stack_buf[32];
  int len = snprintf(stack_buf, sizeof(stack_buf), "%" PRIu32, value);
  if (len <= 0) {
    return mira_buf_empty();
  }
  return mira_buf_copy_bytes((const uint8_t*) stack_buf, (size_t) len);
}

buf_u8 mira_str_from_bool(bool value) {
  return mira_buf_copy_bytes(
      (const uint8_t*) (value ? "true" : "false"),
      value ? 4u : 5u);
}

buf_u8 mira_buf_hex_str_u8(buf_u8 value) {
  if (value.len == 0u || value.data == NULL) {
    return mira_buf_empty();
  }
  size_t len = (size_t) value.len * 2u;
  uint8_t* data = (uint8_t*) malloc(len);
  if (data == NULL) {
    return mira_buf_empty();
  }
  static const char* hex = "0123456789abcdef";
  for (size_t index = 0u; index < value.len; index++) {
    data[index * 2u] = (uint8_t) hex[(value.data[index] >> 4u) & 0x0fu];
    data[index * 2u + 1u] = (uint8_t) hex[value.data[index] & 0x0fu];
  }
  return mira_buf_from_heap(data, len);
}

buf_u8 mira_json_quote_str_u8(buf_u8 value) {
  size_t extra = 2u;
  for (size_t index = 0u; index < value.len; index++) {
    uint8_t ch = value.data[index];
    extra += (ch == '"' || ch == '\\' || ch == '\n' || ch == '\r' || ch == '\t') ? 2u : 1u;
  }
  uint8_t* data = (uint8_t*) malloc(extra);
  if (data == NULL) {
    return mira_buf_empty();
  }
  size_t out = 0u;
  data[out++] = '"';
  for (size_t index = 0u; index < value.len; index++) {
    uint8_t ch = value.data[index];
    switch (ch) {
      case '"': data[out++] = '\\'; data[out++] = '"'; break;
      case '\\': data[out++] = '\\'; data[out++] = '\\'; break;
      case '\n': data[out++] = '\\'; data[out++] = 'n'; break;
      case '\r': data[out++] = '\\'; data[out++] = 'r'; break;
      case '\t': data[out++] = '\\'; data[out++] = 't'; break;
      default: data[out++] = ch; break;
    }
  }
  data[out++] = '"';
  return mira_buf_from_heap(data, out);
}

buf_u8 mira_http_header_buf_u8(buf_u8 request, const char* name) {
  const uint8_t* headers = NULL;
  const uint8_t* body = NULL;
  size_t headers_len = 0u;
  if (!mira_http_header_section(request, &headers, &headers_len, &body) || headers == NULL) {
    return mira_buf_empty();
  }
  const char* cursor = (const char*) headers;
  const char* end = (const char*) headers + headers_len;
  size_t expected_name_len = strlen(name);
  while (cursor < end) {
    const char* line_end = strstr(cursor, "\r\n");
    if (line_end == NULL || line_end > end) {
      line_end = end;
    }
    const char* colon = memchr(cursor, ':', (size_t) (line_end - cursor));
    if (colon != NULL) {
      size_t found_name_len = (size_t) (colon - cursor);
      const char* found_value = colon + 1;
      while (found_value < line_end && (*found_value == ' ' || *found_value == '\t')) {
        found_value += 1;
      }
      size_t found_value_len = (size_t) (line_end - found_value);
#ifdef _WIN32
      if (found_name_len == expected_name_len && _strnicmp(cursor, name, found_name_len) == 0) {
#else
      if (found_name_len == expected_name_len && strncasecmp(cursor, name, found_name_len) == 0) {
#endif
        return mira_buf_copy_bytes((const uint8_t*) found_value, found_value_len);
      }
    }
    cursor = line_end;
    if (cursor < end && end - cursor >= 2 && cursor[0] == '\r' && cursor[1] == '\n') {
      cursor += 2;
    } else {
      break;
    }
  }
  return mira_buf_empty();
}

static bool mira_http_header_at(
    buf_u8 request,
    uint32_t index,
    const uint8_t** name_start,
    size_t* name_len,
    const uint8_t** value_start,
    size_t* value_len) {
  const uint8_t* headers = NULL;
  const uint8_t* body = NULL;
  size_t headers_len = 0u;
  if (!mira_http_header_section(request, &headers, &headers_len, &body) || headers == NULL) {
    return false;
  }
  const char* cursor = (const char*) headers;
  const char* end = (const char*) headers + headers_len;
  uint32_t current = 0u;
  while (cursor < end) {
    const char* line_end = strstr(cursor, "\r\n");
    if (line_end == NULL || line_end > end) {
      line_end = end;
    }
    const char* colon = memchr(cursor, ':', (size_t) (line_end - cursor));
    if (colon != NULL) {
      const char* found_value = colon + 1;
      while (found_value < line_end && (*found_value == ' ' || *found_value == '\t')) {
        found_value += 1;
      }
      if (current == index) {
        *name_start = (const uint8_t*) cursor;
        *name_len = (size_t) (colon - cursor);
        *value_start = (const uint8_t*) found_value;
        *value_len = (size_t) (line_end - found_value);
        return true;
      }
      current += 1u;
    }
    cursor = line_end;
    if (cursor < end && end - cursor >= 2 && cursor[0] == '\r' && cursor[1] == '\n') {
      cursor += 2;
    } else {
      break;
    }
  }
  return false;
}

uint32_t mira_http_header_count_buf_u8(buf_u8 request) {
  const uint8_t* headers = NULL;
  const uint8_t* body = NULL;
  size_t headers_len = 0u;
  if (!mira_http_header_section(request, &headers, &headers_len, &body) || headers == NULL) {
    return 0u;
  }
  const char* cursor = (const char*) headers;
  const char* end = (const char*) headers + headers_len;
  uint32_t count = 0u;
  while (cursor < end) {
    const char* line_end = strstr(cursor, "\r\n");
    if (line_end == NULL || line_end > end) {
      line_end = end;
    }
    if (memchr(cursor, ':', (size_t) (line_end - cursor)) != NULL) {
      count += 1u;
    }
    cursor = line_end;
    if (cursor < end && end - cursor >= 2 && cursor[0] == '\r' && cursor[1] == '\n') {
      cursor += 2;
    } else {
      break;
    }
  }
  return count;
}

buf_u8 mira_http_header_name_buf_u8(buf_u8 request, uint32_t index) {
  const uint8_t* name_start = NULL;
  const uint8_t* value_start = NULL;
  size_t name_len = 0u;
  size_t value_len = 0u;
  (void) value_start;
  (void) value_len;
  if (!mira_http_header_at(request, index, &name_start, &name_len, &value_start, &value_len)) {
    return mira_buf_empty();
  }
  return mira_buf_copy_bytes(name_start, name_len);
}

buf_u8 mira_http_header_value_buf_u8(buf_u8 request, uint32_t index) {
  const uint8_t* name_start = NULL;
  const uint8_t* value_start = NULL;
  size_t name_len = 0u;
  size_t value_len = 0u;
  (void) name_start;
  (void) name_len;
  if (!mira_http_header_at(request, index, &name_start, &name_len, &value_start, &value_len)) {
    return mira_buf_empty();
  }
  return mira_buf_copy_bytes(value_start, value_len);
}

static buf_u8 mira_http_multipart_boundary_buf_u8(buf_u8 request) {
  buf_u8 content_type = mira_http_header_buf_u8(request, "Content-Type");
  if (content_type.data == NULL || content_type.len == 0u) {
    return mira_buf_empty();
  }
  const char* text = (const char*) content_type.data;
  const char* boundary = strstr(text, "boundary=");
  if (boundary == NULL) {
    free(content_type.data);
    return mira_buf_empty();
  }
  boundary += strlen("boundary=");
  const char* end = text + content_type.len;
  const char* cursor = boundary;
  if (cursor < end && *cursor == '"') {
    cursor += 1;
  }
  const char* boundary_end = cursor;
  while (boundary_end < end && *boundary_end != ';' && *boundary_end != '\r' &&
         *boundary_end != '\n' && *boundary_end != '"') {
    boundary_end += 1;
  }
  buf_u8 out = mira_buf_copy_bytes((const uint8_t*) cursor, (size_t) (boundary_end - cursor));
  free(content_type.data);
  return out;
}

static bool mira_http_multipart_part_at(
    buf_u8 request,
    uint32_t index,
    buf_u8* name_out,
    buf_u8* filename_out,
    buf_u8* body_out) {
  *name_out = mira_buf_empty();
  *filename_out = mira_buf_empty();
  *body_out = mira_buf_empty();
  buf_u8 boundary = mira_http_multipart_boundary_buf_u8(request);
  buf_u8 body = mira_http_body_buf_u8(request);
  if (boundary.data == NULL || boundary.len == 0u || body.data == NULL) {
    if (boundary.data != NULL) { free(boundary.data); }
    if (body.data != NULL) { free(body.data); }
    return false;
  }
  size_t marker_len = boundary.len + 2u;
  char* marker = (char*) malloc(marker_len + 1u);
  if (marker == NULL) {
    free(boundary.data);
    free(body.data);
    return false;
  }
  marker[0] = '-';
  marker[1] = '-';
  memcpy(marker + 2, boundary.data, boundary.len);
  marker[marker_len] = '\0';
  char* payload = (char*) malloc(body.len + 1u);
  if (payload == NULL) {
    free(marker);
    free(boundary.data);
    free(body.data);
    return false;
  }
  memcpy(payload, body.data, body.len);
  payload[body.len] = '\0';
  uint32_t current = 0u;
  char* cursor = payload;
  while ((cursor = strstr(cursor, marker)) != NULL) {
    cursor += marker_len;
    if (cursor[0] == '-' && cursor[1] == '-') {
      break;
    }
    if (cursor[0] == '\r' && cursor[1] == '\n') {
      cursor += 2;
    }
    char* part_headers_end = strstr(cursor, "\r\n\r\n");
    if (part_headers_end == NULL) {
      break;
    }
    char* part_body = part_headers_end + 4;
    char* next_marker = strstr(part_body, marker);
    if (next_marker == NULL) {
      break;
    }
    char* part_body_end = next_marker;
    while (part_body_end > part_body &&
           (part_body_end[-1] == '\r' || part_body_end[-1] == '\n')) {
      part_body_end -= 1;
    }
    if (current == index) {
      char* header_cursor = cursor;
      while (header_cursor < part_headers_end) {
        char* line_end = strstr(header_cursor, "\r\n");
        if (line_end == NULL || line_end > part_headers_end) {
          line_end = part_headers_end;
        }
        char* colon = memchr(header_cursor, ':', (size_t) (line_end - header_cursor));
        if (colon != NULL) {
          size_t header_name_len = (size_t) (colon - header_cursor);
#ifdef _WIN32
          if (header_name_len == strlen("Content-Disposition") &&
              _strnicmp(header_cursor, "Content-Disposition", header_name_len) == 0) {
#else
          if (header_name_len == strlen("Content-Disposition") &&
              strncasecmp(header_cursor, "Content-Disposition", header_name_len) == 0) {
#endif
            char* token = colon + 1;
            while (token < line_end) {
              while (token < line_end && (*token == ' ' || *token == '\t' || *token == ';')) {
                token += 1;
              }
              if (line_end - token >= 5 && strncmp(token, "name=", 5) == 0) {
                char* value_start = token + 5;
                if (*value_start == '"') { value_start += 1; }
                char* value_end = value_start;
                while (value_end < line_end && *value_end != '"' && *value_end != ';') {
                  value_end += 1;
                }
                *name_out = mira_buf_copy_bytes((const uint8_t*) value_start, (size_t) (value_end - value_start));
              } else if (line_end - token >= 9 && strncmp(token, "filename=", 9) == 0) {
                char* value_start = token + 9;
                if (*value_start == '"') { value_start += 1; }
                char* value_end = value_start;
                while (value_end < line_end && *value_end != '"' && *value_end != ';') {
                  value_end += 1;
                }
                *filename_out = mira_buf_copy_bytes((const uint8_t*) value_start, (size_t) (value_end - value_start));
              }
              char* next = strchr(token, ';');
              if (next == NULL || next >= line_end) {
                break;
              }
              token = next + 1;
            }
          }
        }
        header_cursor = line_end;
        if (header_cursor < part_headers_end && header_cursor[0] == '\r' && header_cursor[1] == '\n') {
          header_cursor += 2;
        } else {
          break;
        }
      }
      *body_out = mira_buf_copy_bytes((const uint8_t*) part_body, (size_t) (part_body_end - part_body));
      free(payload);
      free(marker);
      free(boundary.data);
      free(body.data);
      return true;
    }
    current += 1u;
  }
  free(payload);
  free(marker);
  free(boundary.data);
  free(body.data);
  return false;
}

uint32_t mira_http_multipart_part_count_buf_u8(buf_u8 request) {
  uint32_t count = 0u;
  for (;;) {
    buf_u8 name;
    buf_u8 filename;
    buf_u8 body;
    if (!mira_http_multipart_part_at(request, count, &name, &filename, &body)) {
      return count;
    }
    if (name.data != NULL) { free(name.data); }
    if (filename.data != NULL) { free(filename.data); }
    if (body.data != NULL) { free(body.data); }
    count += 1u;
  }
}

buf_u8 mira_http_multipart_part_name_buf_u8(buf_u8 request, uint32_t index) {
  buf_u8 name;
  buf_u8 filename;
  buf_u8 body;
  if (!mira_http_multipart_part_at(request, index, &name, &filename, &body)) {
    return mira_buf_empty();
  }
  if (filename.data != NULL) { free(filename.data); }
  if (body.data != NULL) { free(body.data); }
  return name;
}

buf_u8 mira_http_multipart_part_filename_buf_u8(buf_u8 request, uint32_t index) {
  buf_u8 name;
  buf_u8 filename;
  buf_u8 body;
  if (!mira_http_multipart_part_at(request, index, &name, &filename, &body)) {
    return mira_buf_empty();
  }
  if (name.data != NULL) { free(name.data); }
  if (body.data != NULL) { free(body.data); }
  return filename;
}

buf_u8 mira_http_multipart_part_body_buf_u8(buf_u8 request, uint32_t index) {
  buf_u8 name;
  buf_u8 filename;
  buf_u8 body;
  if (!mira_http_multipart_part_at(request, index, &name, &filename, &body)) {
    return mira_buf_empty();
  }
  if (name.data != NULL) { free(name.data); }
  if (filename.data != NULL) { free(filename.data); }
  return body;
}

buf_u8 mira_http_cookie_buf_u8(buf_u8 request, const char* name) {
  buf_u8 cookie_header = mira_http_header_buf_u8(request, "Cookie");
  if (cookie_header.data == NULL || cookie_header.len == 0u || name == NULL) {
    if (cookie_header.data != NULL) {
      free(cookie_header.data);
    }
    return mira_buf_empty();
  }
  const char* cursor = (const char*) cookie_header.data;
  const char* end = cursor + cookie_header.len;
  size_t name_len = strlen(name);
  while (cursor < end) {
    while (cursor < end && (*cursor == ' ' || *cursor == '\t' || *cursor == ';')) {
      cursor += 1;
    }
    const char* pair_end = memchr(cursor, ';', (size_t) (end - cursor));
    if (pair_end == NULL) {
      pair_end = end;
    }
    const char* equals = memchr(cursor, '=', (size_t) (pair_end - cursor));
    if (equals != NULL) {
      const char* found_name_end = equals;
      while (found_name_end > cursor &&
             (found_name_end[-1] == ' ' || found_name_end[-1] == '\t')) {
        found_name_end -= 1;
      }
      if ((size_t) (found_name_end - cursor) == name_len &&
          memcmp(cursor, name, name_len) == 0) {
        const char* value_start = equals + 1;
        while (value_start < pair_end && (*value_start == ' ' || *value_start == '\t')) {
          value_start += 1;
        }
        const char* value_end = pair_end;
        while (value_end > value_start &&
               (value_end[-1] == ' ' || value_end[-1] == '\t')) {
          value_end -= 1;
        }
        buf_u8 out = mira_buf_copy_bytes(
            (const uint8_t*) value_start,
            (size_t) (value_end - value_start));
        free(cookie_header.data);
        return out;
      }
    }
    cursor = pair_end < end ? pair_end + 1 : end;
  }
  free(cookie_header.data);
  return mira_buf_empty();
}

buf_u8 mira_http_query_param_buf_u8(buf_u8 request, const char* key) {
  if (request.data == NULL || key == NULL) {
    return mira_buf_empty();
  }
  const char* cursor = (const char*) request.data;
  const char* end = (const char*) request.data + request.len;
  const char* line_end = strstr(cursor, "\r\n");
  if (line_end == NULL || line_end > end) {
    line_end = end;
  }
  const char* first_space = memchr(cursor, ' ', (size_t) (line_end - cursor));
  if (first_space == NULL) {
    return mira_buf_empty();
  }
  const char* second_space = memchr(first_space + 1, ' ', (size_t) (line_end - (first_space + 1)));
  if (second_space == NULL || second_space <= first_space + 1) {
    return mira_buf_empty();
  }
  const char* path = first_space + 1;
  size_t path_len = (size_t) (second_space - path);
  const char* query = memchr(path, '?', path_len);
  if (query == NULL) {
    return mira_buf_empty();
  }
  query += 1;
  const char* query_end = path + path_len;
  size_t key_len = strlen(key);
  while (query < query_end) {
    const char* pair_end = memchr(query, '&', (size_t) (query_end - query));
    if (pair_end == NULL) {
      pair_end = query_end;
    }
    const char* equals = memchr(query, '=', (size_t) (pair_end - query));
    if (equals != NULL && (size_t) (equals - query) == key_len && strncmp(query, key, key_len) == 0) {
      return mira_buf_copy_bytes((const uint8_t*) (equals + 1), (size_t) (pair_end - (equals + 1)));
    }
    query = pair_end < query_end ? pair_end + 1 : query_end;
  }
  return mira_buf_empty();
}

buf_u8 mira_http_body_buf_u8(buf_u8 request) {
  const uint8_t* headers = NULL;
  const uint8_t* body = NULL;
  size_t headers_len = 0u;
  if (!mira_http_header_section(request, &headers, &headers_len, &body) || body == NULL) {
    return mira_buf_empty();
  }
  size_t offset = (size_t) (body - request.data);
  if (offset > request.len) {
    return mira_buf_empty();
  }
  return mira_buf_copy_bytes(body, request.len - offset);
}

void mira_http_body_buf_u8_parts(const uint8_t* data, uint32_t len, buf_u8* out) {
  if (out == NULL) {
    return;
  }
  buf_u8 request;
  request.data = (uint8_t*) data;
  request.len = len;
  request.cap = len;
  *out = mira_http_body_buf_u8(request);
}

bool mira_http_body_limit_buf_u8(buf_u8 request, uint32_t limit) {
  buf_u8 body = mira_http_body_buf_u8(request);
  bool ok = body.len <= limit;
  if (body.data != NULL) {
    free(body.data);
  }
  return ok ? 1u : 0u;
}

uint64_t mira_http_body_stream_open_buf_u8(buf_u8 request) {
  bool used_flags[128];
  for (size_t index = 0; index < 128u; index++) {
    used_flags[index] = mira_http_body_streams[index].used;
  }
  uint64_t handle = mira_alloc_handle(used_flags, 128u);
  if (handle == 0u) {
    return 0u;
  }
  mira_http_body_stream_entry* entry = &mira_http_body_streams[handle - 1u];
  memset(entry, 0, sizeof(*entry));
  entry->used = true;
  entry->body = mira_http_body_buf_u8(request);
  entry->cursor = 0u;
  return handle;
}

buf_u8 mira_http_body_stream_next_buf_u8(uint64_t handle, uint32_t chunk_size) {
  if (handle == 0u || handle > 128u || !mira_http_body_streams[handle - 1u].used) {
    return mira_buf_empty();
  }
  mira_http_body_stream_entry* entry = &mira_http_body_streams[handle - 1u];
  if (entry->body.data == NULL || entry->cursor >= entry->body.len) {
    return mira_buf_empty();
  }
  uint32_t remaining = entry->body.len - entry->cursor;
  uint32_t len = remaining < chunk_size ? remaining : chunk_size;
  buf_u8 out = mira_buf_copy_bytes(entry->body.data + entry->cursor, len);
  entry->cursor += len;
  return out;
}

bool mira_http_body_stream_close_handle(uint64_t handle) {
  if (handle == 0u || handle > 128u || !mira_http_body_streams[handle - 1u].used) {
    return 0u;
  }
  mira_http_body_stream_entry* entry = &mira_http_body_streams[handle - 1u];
  if (entry->body.data != NULL) {
    free(entry->body.data);
  }
  memset(entry, 0, sizeof(*entry));
  return 1u;
}

uint32_t mira_http_server_config_u32(const char* token) {
  if (token == NULL) {
    return 0u;
  }
  if (strcmp(token, "body_limit_small") == 0) {
    return 32u;
  }
  if (strcmp(token, "body_limit_default") == 0) {
    return 1024u;
  }
  if (strcmp(token, "status_ok") == 0) {
    return 200u;
  }
  if (strcmp(token, "status_created") == 0) {
    return 201u;
  }
  if (strcmp(token, "status_no_content") == 0) {
    return 204u;
  }
  if (strcmp(token, "status_bad_request") == 0) {
    return 400u;
  }
  if (strcmp(token, "status_unauthorized") == 0) {
    return 401u;
  }
  if (strcmp(token, "status_forbidden") == 0) {
    return 403u;
  }
  if (strcmp(token, "status_not_found") == 0) {
    return 404u;
  }
  if (strcmp(token, "status_method_not_allowed") == 0) {
    return 405u;
  }
  if (strcmp(token, "status_payload_too_large") == 0) {
    return 413u;
  }
  if (strcmp(token, "status_internal_error") == 0) {
    return 500u;
  }
  return 0u;
}

bool mira_http_write_response_handle(uint64_t handle, uint32_t status, buf_u8 body) {
  char header[256];
  int header_len = snprintf(
      header,
      sizeof(header),
      "HTTP/1.1 %u %s\r\nContent-Length: %u\r\nConnection: close\r\nContent-Type: application/octet-stream\r\n\r\n",
      (unsigned) status,
      mira_http_reason_phrase(status),
      (unsigned) body.len);
  if (header_len < 0) {
    return 0u;
  }
  buf_u8 header_buf = mira_buf_copy_bytes((const uint8_t*) header, (size_t) header_len);
  if (!mira_net_write_handle_all_buf_u8(handle, header_buf)) {
    if (header_buf.data != NULL) { free(header_buf.data); }
    return 0u;
  }
  if (header_buf.data != NULL) { free(header_buf.data); }
  return mira_net_write_handle_all_buf_u8(handle, body);
}

bool mira_http_write_response_header_handle(uint64_t handle, uint32_t status, const char* header_name, const char* header_value, buf_u8 body) {
  char header[384];
  int header_len = snprintf(
      header,
      sizeof(header),
      "HTTP/1.1 %u %s\r\nContent-Length: %u\r\nConnection: close\r\n%s: %s\r\n\r\n",
      (unsigned) status,
      mira_http_reason_phrase(status),
      (unsigned) body.len,
      header_name,
      header_value);
  if (header_len < 0) {
    return 0u;
  }
  buf_u8 header_buf = mira_buf_copy_bytes((const uint8_t*) header, (size_t) header_len);
  if (!mira_net_write_handle_all_buf_u8(handle, header_buf)) {
    if (header_buf.data != NULL) { free(header_buf.data); }
    return 0u;
  }
  if (header_buf.data != NULL) { free(header_buf.data); }
  return mira_net_write_handle_all_buf_u8(handle, body);
}

bool mira_http_write_response_cookie_handle(uint64_t handle, uint32_t status, const char* content_type, const char* cookie_name, const char* cookie_value, buf_u8 body) {
  char header[512];
  int header_len = snprintf(
      header,
      sizeof(header),
      "HTTP/1.1 %u %s\r\nContent-Length: %u\r\nConnection: close\r\nContent-Type: %s\r\nSet-Cookie: %s=%s\r\n\r\n",
      (unsigned) status,
      mira_http_reason_phrase(status),
      (unsigned) body.len,
      content_type,
      cookie_name,
      cookie_value);
  if (header_len < 0) {
    return 0u;
  }
  buf_u8 header_buf = mira_buf_copy_bytes((const uint8_t*) header, (size_t) header_len);
  if (!mira_net_write_handle_all_buf_u8(handle, header_buf)) {
    if (header_buf.data != NULL) { free(header_buf.data); }
    return 0u;
  }
  if (header_buf.data != NULL) { free(header_buf.data); }
  return mira_net_write_handle_all_buf_u8(handle, body);
}

static bool mira_ascii_case_eq_cstr(const char* left, const char* right) {
  if (left == NULL || right == NULL) {
    return false;
  }
  while (*left != '\0' && *right != '\0') {
    char a = *left;
    char b = *right;
    if (a >= 'A' && a <= 'Z') { a = (char) (a - 'A' + 'a'); }
    if (b >= 'A' && b <= 'Z') { b = (char) (b - 'A' + 'a'); }
    if (a != b) {
      return false;
    }
    left += 1;
    right += 1;
  }
  return *left == '\0' && *right == '\0';
}

bool mira_http_write_response_headers2_handle(uint64_t handle, uint32_t status, const char* default_content_type, const char* header1_name, const char* header1_value, const char* header2_name, const char* header2_value, buf_u8 body) {
  const char* content_type = default_content_type;
  bool emit_header1 = true;
  bool emit_header2 = true;
  if (mira_ascii_case_eq_cstr(header1_name, "Content-Type")) {
    content_type = header1_value;
    emit_header1 = false;
  }
  if (mira_ascii_case_eq_cstr(header2_name, "Content-Type")) {
    content_type = header2_value;
    emit_header2 = false;
  }
  if (emit_header1 && emit_header2 && mira_ascii_case_eq_cstr(header1_name, header2_name)) {
    emit_header1 = false;
  }
  char header[768];
  int header_len = snprintf(
      header,
      sizeof(header),
      "HTTP/1.1 %u %s\r\nContent-Length: %u\r\nConnection: close\r\nContent-Type: %s\r\n",
      (unsigned) status,
      mira_http_reason_phrase(status),
      (unsigned) body.len,
      content_type);
  if (header_len < 0 || (size_t) header_len >= sizeof(header)) {
    return 0u;
  }
  size_t used = (size_t) header_len;
  if (emit_header1) {
    int extra = snprintf(
        header + used,
        sizeof(header) - used,
        "%s: %s\r\n",
        header1_name,
        header1_value);
    if (extra < 0 || (size_t) extra >= sizeof(header) - used) {
      return 0u;
    }
    used += (size_t) extra;
  }
  if (emit_header2) {
    int extra = snprintf(
        header + used,
        sizeof(header) - used,
        "%s: %s\r\n",
        header2_name,
        header2_value);
    if (extra < 0 || (size_t) extra >= sizeof(header) - used) {
      return 0u;
    }
    used += (size_t) extra;
  }
  int tail = snprintf(header + used, sizeof(header) - used, "\r\n");
  if (tail < 0 || (size_t) tail >= sizeof(header) - used) {
    return 0u;
  }
  used += (size_t) tail;
  buf_u8 header_buf = mira_buf_copy_bytes((const uint8_t*) header, used);
  if (!mira_net_write_handle_all_buf_u8(handle, header_buf)) {
    if (header_buf.data != NULL) { free(header_buf.data); }
    return 0u;
  }
  if (header_buf.data != NULL) { free(header_buf.data); }
  return mira_net_write_handle_all_buf_u8(handle, body);
}

bool mira_http_write_text_response_handle(uint64_t handle, uint32_t status, buf_u8 body) {
  return mira_http_write_response_header_handle(handle, status, "Content-Type", "text/plain", body);
}

bool mira_http_write_text_response_cookie_handle(uint64_t handle, uint32_t status, const char* cookie_name, const char* cookie_value, buf_u8 body) {
  return mira_http_write_response_cookie_handle(handle, status, "text/plain", cookie_name, cookie_value, body);
}

bool mira_http_write_text_response_headers2_handle(uint64_t handle, uint32_t status, const char* header1_name, const char* header1_value, const char* header2_name, const char* header2_value, buf_u8 body) {
  return mira_http_write_response_headers2_handle(handle, status, "text/plain", header1_name, header1_value, header2_name, header2_value, body);
}

bool mira_http_write_json_response_handle(uint64_t handle, uint32_t status, buf_u8 body) {
  return mira_http_write_response_header_handle(handle, status, "Content-Type", "application/json", body);
}

bool mira_http_write_json_response_cookie_handle(uint64_t handle, uint32_t status, const char* cookie_name, const char* cookie_value, buf_u8 body) {
  return mira_http_write_response_cookie_handle(handle, status, "application/json", cookie_name, cookie_value, body);
}

bool mira_http_write_json_response_headers2_handle(uint64_t handle, uint32_t status, const char* header1_name, const char* header1_value, const char* header2_name, const char* header2_value, buf_u8 body) {
  return mira_http_write_response_headers2_handle(handle, status, "application/json", header1_name, header1_value, header2_name, header2_value, body);
}

bool mira_http_session_write_text_handle(uint64_t handle, uint32_t status, buf_u8 body) {
  return mira_http_write_text_response_handle(handle, status, body);
}

bool mira_http_session_write_text_cookie_handle(uint64_t handle, uint32_t status, const char* cookie_name, const char* cookie_value, buf_u8 body) {
  return mira_http_write_text_response_cookie_handle(handle, status, cookie_name, cookie_value, body);
}

bool mira_http_session_write_text_headers2_handle(uint64_t handle, uint32_t status, const char* header1_name, const char* header1_value, const char* header2_name, const char* header2_value, buf_u8 body) {
  return mira_http_write_text_response_headers2_handle(handle, status, header1_name, header1_value, header2_name, header2_value, body);
}

bool mira_http_session_write_json_handle(uint64_t handle, uint32_t status, buf_u8 body) {
  return mira_http_write_json_response_handle(handle, status, body);
}

bool mira_http_session_write_json_cookie_handle(uint64_t handle, uint32_t status, const char* cookie_name, const char* cookie_value, buf_u8 body) {
  return mira_http_write_json_response_cookie_handle(handle, status, cookie_name, cookie_value, body);
}

bool mira_http_session_write_json_headers2_handle(uint64_t handle, uint32_t status, const char* header1_name, const char* header1_value, const char* header2_name, const char* header2_value, buf_u8 body) {
  return mira_http_write_json_response_headers2_handle(handle, status, header1_name, header1_value, header2_name, header2_value, body);
}

uint64_t mira_http_response_stream_open_handle(uint64_t handle, uint32_t status, const char* content_type) {
  bool used_flags[128];
  for (size_t index = 0; index < 128u; index++) {
    used_flags[index] = mira_http_response_streams[index].used;
  }
  uint64_t stream_handle = mira_alloc_handle(used_flags, 128u);
  if (stream_handle == 0u) {
    return 0u;
  }
  char header[384];
  int header_len = snprintf(
      header,
      sizeof(header),
      "HTTP/1.1 %u %s\r\nTransfer-Encoding: chunked\r\nConnection: close\r\nContent-Type: %s\r\n\r\n",
      (unsigned) status,
      mira_http_reason_phrase(status),
      content_type);
  if (header_len < 0) {
    return 0u;
  }
  buf_u8 header_buf = mira_buf_copy_bytes((const uint8_t*) header, (size_t) header_len);
  if (!mira_net_write_handle_all_buf_u8(handle, header_buf)) {
    if (header_buf.data != NULL) { free(header_buf.data); }
    return 0u;
  }
  if (header_buf.data != NULL) { free(header_buf.data); }
  memset(&mira_http_response_streams[stream_handle - 1u], 0, sizeof(mira_http_response_streams[stream_handle - 1u]));
  mira_http_response_streams[stream_handle - 1u].used = true;
  mira_http_response_streams[stream_handle - 1u].session_handle = handle;
  mira_http_response_streams[stream_handle - 1u].closed = 0;
  return stream_handle;
}

bool mira_http_response_stream_write_handle(uint64_t handle, buf_u8 body) {
  if (handle == 0u || handle > 128u || !mira_http_response_streams[handle - 1u].used) {
    return 0u;
  }
  mira_http_response_stream_entry* entry = &mira_http_response_streams[handle - 1u];
  if (entry->closed) {
    return 0u;
  }
  char chunk_header[32];
  int chunk_header_len = snprintf(chunk_header, sizeof(chunk_header), "%X\r\n", (unsigned) body.len);
  if (chunk_header_len < 0) {
    return 0u;
  }
  buf_u8 header_buf = mira_buf_copy_bytes((const uint8_t*) chunk_header, (size_t) chunk_header_len);
  buf_u8 trailer_buf = mira_buf_copy_bytes((const uint8_t*) "\r\n", 2u);
  bool ok = mira_net_write_handle_all_buf_u8(entry->session_handle, header_buf)
            && mira_net_write_handle_all_buf_u8(entry->session_handle, body)
            && mira_net_write_handle_all_buf_u8(entry->session_handle, trailer_buf);
  if (header_buf.data != NULL) { free(header_buf.data); }
  if (trailer_buf.data != NULL) { free(trailer_buf.data); }
  return ok ? 1u : 0u;
}

bool mira_http_response_stream_close_handle(uint64_t handle) {
  if (handle == 0u || handle > 128u || !mira_http_response_streams[handle - 1u].used) {
    return 0u;
  }
  mira_http_response_stream_entry* entry = &mira_http_response_streams[handle - 1u];
  if (!entry->closed) {
    buf_u8 tail = mira_buf_copy_bytes((const uint8_t*) "0\r\n\r\n", 5u);
    bool ok = mira_net_write_handle_all_buf_u8(entry->session_handle, tail);
    if (tail.data != NULL) { free(tail.data); }
    if (!ok) {
      return 0u;
    }
  }
  memset(entry, 0, sizeof(*entry));
  return 1u;
}

uint64_t mira_http_client_open_handle(const char* host, uint16_t port) {
  bool used_flags[128];
  for (size_t index = 0; index < 128u; index++) {
    used_flags[index] = mira_http_clients[index].used;
  }
  uint64_t handle = mira_alloc_handle(used_flags, 128u);
  if (handle == 0u) {
    return 0u;
  }
  memset(&mira_http_clients[handle - 1u], 0, sizeof(mira_http_clients[handle - 1u]));
  mira_http_clients[handle - 1u].used = true;
  strncpy(mira_http_clients[handle - 1u].host, host, sizeof(mira_http_clients[handle - 1u].host) - 1u);
  mira_http_clients[handle - 1u].host[sizeof(mira_http_clients[handle - 1u].host) - 1u] = '\0';
  mira_http_clients[handle - 1u].port = port;
  return handle;
}

static buf_u8 mira_http_client_request_inner(uint64_t handle, buf_u8 request) {
  if (handle == 0u || handle > 128u || !mira_http_clients[handle - 1u].used) {
    return mira_buf_empty();
  }
  mira_http_client_entry* entry = &mira_http_clients[handle - 1u];
  char port_buf[16];
  snprintf(port_buf, sizeof(port_buf), "%u", (unsigned) entry->port);
  struct addrinfo hints;
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  struct addrinfo* result = NULL;
  if (getaddrinfo(entry->host, port_buf, &hints, &result) != 0) {
    return mira_buf_empty();
  }
  mira_socket_t fd = MIRA_INVALID_SOCKET;
  for (struct addrinfo* it = result; it != NULL; it = it->ai_next) {
    fd = socket(it->ai_family, it->ai_socktype, it->ai_protocol);
    if (fd == MIRA_INVALID_SOCKET) {
      continue;
    }
    if (connect(fd, it->ai_addr, (int) it->ai_addrlen) == 0) {
      break;
    }
    MIRA_CLOSE_SOCKET(fd);
    fd = MIRA_INVALID_SOCKET;
  }
  freeaddrinfo(result);
  if (fd == MIRA_INVALID_SOCKET) {
    return mira_buf_empty();
  }
  if (!mira_write_all_fd(fd, request.data, request.len)) {
    MIRA_CLOSE_SOCKET(fd);
    return mira_buf_empty();
  }
  shutdown(fd, SHUT_WR);
  buf_u8 out = mira_read_all_fd(fd);
  MIRA_CLOSE_SOCKET(fd);
  return out;
}

buf_u8 mira_http_client_request_buf_u8(uint64_t handle, buf_u8 request) {
  return mira_http_client_request_inner(handle, request);
}

buf_u8 mira_http_client_request_retry_buf_u8(uint64_t handle, uint32_t retries, uint32_t backoff_ms, buf_u8 request) {
  for (uint32_t attempt = 0u; attempt <= retries; attempt++) {
    buf_u8 out = mira_http_client_request_inner(handle, request);
    if (out.data != NULL || out.len > 0u) {
      return out;
    }
#ifndef _WIN32
    if (attempt < retries) {
      usleep((useconds_t) backoff_ms * 1000u);
    }
#endif
  }
  return mira_buf_empty();
}

bool mira_http_client_close_handle(uint64_t handle) {
  if (handle == 0u || handle > 128u || !mira_http_clients[handle - 1u].used) {
    return 0u;
  }
  memset(&mira_http_clients[handle - 1u], 0, sizeof(mira_http_clients[handle - 1u]));
  return 1u;
}

uint64_t mira_http_client_pool_open_handle(const char* host, uint16_t port, uint32_t max_size) {
  bool used_flags[64];
  for (size_t index = 0; index < 64u; index++) {
    used_flags[index] = mira_http_client_pools[index].used;
  }
  uint64_t handle = mira_alloc_handle(used_flags, 64u);
  if (handle == 0u) {
    return 0u;
  }
  memset(&mira_http_client_pools[handle - 1u], 0, sizeof(mira_http_client_pools[handle - 1u]));
  mira_http_client_pools[handle - 1u].used = true;
  strncpy(mira_http_client_pools[handle - 1u].host, host, sizeof(mira_http_client_pools[handle - 1u].host) - 1u);
  mira_http_client_pools[handle - 1u].host[sizeof(mira_http_client_pools[handle - 1u].host) - 1u] = '\0';
  mira_http_client_pools[handle - 1u].port = port;
  mira_http_client_pools[handle - 1u].max_size = max_size == 0u ? 1u : max_size;
  mira_http_client_pools[handle - 1u].leased = 0u;
  return handle;
}

uint64_t mira_http_client_pool_acquire_handle(uint64_t handle) {
  if (handle == 0u || handle > 64u || !mira_http_client_pools[handle - 1u].used) {
    return 0u;
  }
  mira_http_client_pool_entry* pool = &mira_http_client_pools[handle - 1u];
  if (pool->leased >= pool->max_size) {
    return 0u;
  }
  uint64_t client = mira_http_client_open_handle(pool->host, pool->port);
  if (client != 0u) {
    pool->leased += 1u;
  }
  return client;
}

bool mira_http_client_pool_release_handle(uint64_t pool_handle, uint64_t handle) {
  if (pool_handle == 0u || pool_handle > 64u || !mira_http_client_pools[pool_handle - 1u].used) {
    return 0u;
  }
  if (!mira_http_client_close_handle(handle)) {
    return 0u;
  }
  if (mira_http_client_pools[pool_handle - 1u].leased > 0u) {
    mira_http_client_pools[pool_handle - 1u].leased -= 1u;
  }
  return 1u;
}

bool mira_http_client_pool_close_handle(uint64_t handle) {
  if (handle == 0u || handle > 64u || !mira_http_client_pools[handle - 1u].used) {
    return 0u;
  }
  memset(&mira_http_client_pools[handle - 1u], 0, sizeof(mira_http_client_pools[handle - 1u]));
  return 1u;
}

uint32_t mira_json_get_u32_buf_u8(buf_u8 value, const char* key) {
  const uint8_t* found = mira_json_find_value(value, key);
  if (found == NULL) {
    return 0u;
  }
  return (uint32_t) strtoul((const char*) found, NULL, 10);
}

uint32_t mira_json_get_u32_buf_u8_parts(const uint8_t* data, uint32_t len, const char* key) {
  buf_u8 value;
  value.data = (uint8_t*) data;
  value.len = len;
  value.cap = len;
  return mira_json_get_u32_buf_u8(value, key);
}

bool mira_json_has_key_buf_u8(buf_u8 value, const char* key) {
  return mira_json_find_value(value, key) != NULL;
}

bool mira_json_get_bool_buf_u8(buf_u8 value, const char* key) {
  const uint8_t* found = mira_json_find_value(value, key);
  return found != NULL && strncmp((const char*) found, "true", 4u) == 0;
}

buf_u8 mira_json_get_buf_buf_u8(buf_u8 value, const char* key) {
  const uint8_t* found = mira_json_find_value(value, key);
  if (found == NULL) {
    return mira_buf_empty();
  }
  size_t offset = (size_t) (found - value.data);
  if (offset >= value.len) {
    return mira_buf_empty();
  }
  return mira_json_copy_string(found, value.len - offset);
}

static const uint8_t* mira_json_array_value_at(buf_u8 value, uint32_t target, size_t* remaining) {
  if (value.data == NULL || value.len == 0u) {
    return NULL;
  }
  size_t index = 0u;
  while (index < value.len && (value.data[index] == ' ' || value.data[index] == '\n' ||
                               value.data[index] == '\r' || value.data[index] == '\t')) {
    index += 1u;
  }
  if (index >= value.len || value.data[index] != '[') {
    return NULL;
  }
  index += 1u;
  uint32_t current = 0u;
  for (;;) {
    while (index < value.len && (value.data[index] == ' ' || value.data[index] == '\n' ||
                                 value.data[index] == '\r' || value.data[index] == '\t')) {
      index += 1u;
    }
    if (index >= value.len || value.data[index] == ']') {
      return NULL;
    }
    size_t start = index;
    if (current == target) {
      *remaining = value.len - start;
      return value.data + start;
    }
    if (value.data[index] == '"') {
      index += 1u;
      while (index < value.len) {
        if (value.data[index] == '\\' && index + 1u < value.len) {
          index += 2u;
          continue;
        }
        if (value.data[index] == '"') {
          index += 1u;
          break;
        }
        index += 1u;
      }
    } else {
      while (index < value.len && value.data[index] != ',' && value.data[index] != ']') {
        index += 1u;
      }
    }
    while (index < value.len && (value.data[index] == ' ' || value.data[index] == '\n' ||
                                 value.data[index] == '\r' || value.data[index] == '\t')) {
      index += 1u;
    }
    if (index < value.len && value.data[index] == ',') {
      index += 1u;
      current += 1u;
      continue;
    }
    return NULL;
  }
}

uint32_t mira_json_array_len_buf_u8(buf_u8 value) {
  if (value.data == NULL || value.len == 0u) {
    return 0u;
  }
  size_t index = 0u;
  while (index < value.len && (value.data[index] == ' ' || value.data[index] == '\n' ||
                               value.data[index] == '\r' || value.data[index] == '\t')) {
    index += 1u;
  }
  if (index >= value.len || value.data[index] != '[') {
    return 0u;
  }
  index += 1u;
  uint32_t count = 0u;
  for (;;) {
    while (index < value.len && (value.data[index] == ' ' || value.data[index] == '\n' ||
                                 value.data[index] == '\r' || value.data[index] == '\t')) {
      index += 1u;
    }
    if (index >= value.len || value.data[index] == ']') {
      return count;
    }
    count += 1u;
    if (value.data[index] == '"') {
      index += 1u;
      while (index < value.len) {
        if (value.data[index] == '\\' && index + 1u < value.len) {
          index += 2u;
          continue;
        }
        if (value.data[index] == '"') {
          index += 1u;
          break;
        }
        index += 1u;
      }
    } else {
      while (index < value.len && value.data[index] != ',' && value.data[index] != ']') {
        index += 1u;
      }
    }
    while (index < value.len && (value.data[index] == ' ' || value.data[index] == '\n' ||
                                 value.data[index] == '\r' || value.data[index] == '\t')) {
      index += 1u;
    }
    if (index < value.len && value.data[index] == ',') {
      index += 1u;
      continue;
    }
    return count;
  }
}

uint32_t mira_json_index_u32_buf_u8(buf_u8 value, uint32_t index) {
  size_t remaining = 0u;
  const uint8_t* found = mira_json_array_value_at(value, index, &remaining);
  if (found == NULL) {
    return 0u;
  }
  return (uint32_t) strtoul((const char*) found, NULL, 10);
}

bool mira_json_index_bool_buf_u8(buf_u8 value, uint32_t index) {
  size_t remaining = 0u;
  const uint8_t* found = mira_json_array_value_at(value, index, &remaining);
  return found != NULL && strncmp((const char*) found, "true", 4u) == 0;
}

buf_u8 mira_json_index_str_buf_u8(buf_u8 value, uint32_t index) {
  size_t remaining = 0u;
  const uint8_t* found = mira_json_array_value_at(value, index, &remaining);
  if (found == NULL) {
    return mira_buf_empty();
  }
  return mira_json_copy_string(found, remaining);
}

uint32_t mira_env_get_u32(const char* name) {
  const char* value = getenv(name);
  if (value == NULL) {
    return 0u;
  }
  return (uint32_t) strtoul(value, NULL, 10);
}

bool mira_env_get_bool(const char* name) {
  const char* value = getenv(name);
  if (value == NULL) {
    return 0u;
  }
  return strcmp(value, "1") == 0 || strcmp(value, "true") == 0 || strcmp(value, "yes") == 0 || strcmp(value, "on") == 0;
}

buf_u8 mira_env_get_str_u8(const char* name) {
  const char* value = getenv(name);
  if (value == NULL) {
    return mira_buf_empty();
  }
  return mira_buf_copy_bytes((const uint8_t*) value, strlen(value));
}

bool mira_env_has(const char* name) {
  return getenv(name) != NULL;
}

buf_u8 mira_buf_before_lit_u8(buf_u8 value, const char* literal) {
  if (literal == NULL || literal[0] == '\0' || value.data == NULL || value.len == 0u) {
    return mira_buf_empty();
  }
  size_t literal_len = strlen(literal);
  if (literal_len == 0u || literal_len > value.len) {
    return mira_buf_empty();
  }
  for (size_t index = 0u; index + literal_len <= value.len; index++) {
    if (memcmp(value.data + index, literal, literal_len) == 0) {
      return mira_buf_copy_bytes(value.data, index);
    }
  }
  return mira_buf_empty();
}

buf_u8 mira_buf_after_lit_u8(buf_u8 value, const char* literal) {
  if (literal == NULL || literal[0] == '\0' || value.data == NULL || value.len == 0u) {
    return mira_buf_empty();
  }
  size_t literal_len = strlen(literal);
  if (literal_len == 0u || literal_len > value.len) {
    return mira_buf_empty();
  }
  for (size_t index = 0u; index + literal_len <= value.len; index++) {
    if (memcmp(value.data + index, literal, literal_len) == 0) {
      size_t start = index + literal_len;
      return mira_buf_copy_bytes(value.data + start, value.len - start);
    }
  }
  return mira_buf_empty();
}

buf_u8 mira_buf_trim_ascii_u8(buf_u8 value) {
  if (value.data == NULL || value.len == 0u) {
    return mira_buf_empty();
  }
  size_t start = 0u;
  size_t end = value.len;
  while (start < end) {
    uint8_t ch = value.data[start];
    if (ch != ' ' && ch != '\n' && ch != '\r' && ch != '\t') {
      break;
    }
    start += 1u;
  }
  while (end > start) {
    uint8_t ch = value.data[end - 1u];
    if (ch != ' ' && ch != '\n' && ch != '\r' && ch != '\t') {
      break;
    }
    end -= 1u;
  }
  return mira_buf_copy_bytes(value.data + start, end - start);
}

uint32_t mira_date_parse_ymd(buf_u8 value) {
  char stack_buf[32];
  size_t len = value.len < sizeof(stack_buf) - 1u ? value.len : sizeof(stack_buf) - 1u;
  memcpy(stack_buf, value.data, len);
  stack_buf[len] = '\0';
  unsigned year = 0u;
  unsigned month = 0u;
  unsigned day = 0u;
  if (sscanf(stack_buf, "%u-%u-%u", &year, &month, &day) != 3) {
    return 0u;
  }
  return (uint32_t) (year * 10000u + month * 100u + day);
}

uint32_t mira_time_parse_hms(buf_u8 value) {
  char stack_buf[32];
  size_t len = value.len < sizeof(stack_buf) - 1u ? value.len : sizeof(stack_buf) - 1u;
  memcpy(stack_buf, value.data, len);
  stack_buf[len] = '\0';
  unsigned hour = 0u;
  unsigned minute = 0u;
  unsigned second = 0u;
  if (sscanf(stack_buf, "%u:%u:%u", &hour, &minute, &second) != 3) {
    return 0u;
  }
  return (uint32_t) (hour * 3600u + minute * 60u + second);
}

buf_u8 mira_date_format_ymd(uint32_t value) {
  char stack_buf[32];
  unsigned year = value / 10000u;
  unsigned month = (value / 100u) % 100u;
  unsigned day = value % 100u;
  int len = snprintf(stack_buf, sizeof(stack_buf), "%04u-%02u-%02u", year, month, day);
  if (len <= 0) {
    return mira_buf_empty();
  }
  return mira_buf_copy_bytes((const uint8_t*) stack_buf, (size_t) len);
}

buf_u8 mira_time_format_hms(uint32_t value) {
  char stack_buf[32];
  unsigned hour = value / 3600u;
  unsigned minute = (value / 60u) % 60u;
  unsigned second = value % 60u;
  int len = snprintf(stack_buf, sizeof(stack_buf), "%02u:%02u:%02u", hour, minute, second);
  if (len <= 0) {
    return mira_buf_empty();
  }
  return mira_buf_copy_bytes((const uint8_t*) stack_buf, (size_t) len);
}

uint64_t mira_db_open_handle(const char* target) {
  if (target == NULL || target[0] == '\0') {
    return 0u;
  }
  bool used_flags[128];
  for (size_t index = 0u; index < 128u; index++) {
    used_flags[index] = mira_db_handles[index].used;
  }
  uint64_t handle = mira_alloc_handle(used_flags, 128u);
  if (handle == 0u) {
    return 0u;
  }
  mira_db_handle_entry* entry = &mira_db_handles[handle - 1u];
  memset(entry, 0, sizeof(*entry));
  entry->used = true;
  strncpy(entry->target, target, sizeof(entry->target) - 1u);
  entry->target[sizeof(entry->target) - 1u] = '\0';
  return handle;
}

bool mira_db_close_handle(uint64_t handle) {
  if (handle == 0u || handle > 128u || !mira_db_handles[handle - 1u].used) {
    return 0u;
  }
  mira_db_handle_reset(&mira_db_handles[handle - 1u]);
  return 1u;
}

static int mira_db_handle_tx_open(uint64_t handle) {
  return handle != 0u && handle <= 128u && mira_db_handles[handle - 1u].used && mira_db_handles[handle - 1u].in_transaction;
}

static int mira_db_tx_buffer_append(mira_db_handle_entry* entry, buf_u8 sql) {
  if (entry == NULL) {
    return 0;
  }
  size_t extra = (size_t) sql.len + 1u;
  size_t current = (size_t) entry->tx_sql.len;
  size_t next = current + extra;
  uint8_t* grown = entry->tx_sql.data == NULL
      ? (uint8_t*) malloc(next)
      : (uint8_t*) realloc(entry->tx_sql.data, next);
  if (grown == NULL) {
    return 0;
  }
  entry->tx_sql.data = grown;
  entry->tx_sql.cap = (uint32_t) next;
  if (sql.len > 0u && sql.data != NULL) {
    memcpy(entry->tx_sql.data + current, sql.data, sql.len);
  }
  entry->tx_sql.data[current + sql.len] = ';';
  entry->tx_sql.len = (uint32_t) next;
  return 1;
}

bool mira_db_exec_handle_sql_buf_u8(uint64_t handle, buf_u8 sql) {
  if (handle == 0u || handle > 128u || !mira_db_handles[handle - 1u].used) {
    return 0u;
  }
  mira_db_handle_entry* entry = &mira_db_handles[handle - 1u];
  if (entry->in_transaction) {
    return mira_db_tx_buffer_append(entry, sql) ? 1u : 0u;
  }
  int ok = 0;
  buf_u8 out = mira_db_run_and_capture_target(entry->target, sql, &ok);
  if (ok) {
    mira_db_clear_error(handle);
  } else {
    char* error_text = mira_buf_to_cstring(out);
    mira_db_set_error(handle, error_text);
    free(error_text);
  }
  if (out.data != NULL) {
    free(out.data);
  }
  return ok ? 1u : 0u;
}

uint32_t mira_db_query_u32_handle_sql_buf_u8(uint64_t handle, buf_u8 sql) {
  if (handle == 0u || handle > 128u || !mira_db_handles[handle - 1u].used || mira_db_handle_tx_open(handle)) {
    return 0u;
  }
  int ok = 0;
  buf_u8 out = mira_db_run_and_capture_target(mira_db_handles[handle - 1u].target, sql, &ok);
  if (!ok) {
    char* error_text = mira_buf_to_cstring(out);
    mira_db_set_error(handle, error_text);
    free(error_text);
    if (out.data != NULL) {
      free(out.data);
    }
    return 0u;
  }
  mira_db_clear_error(handle);
  out = mira_trim_db_output(out);
  if (out.data == NULL || out.len == 0u) {
    if (out.data != NULL) {
      free(out.data);
    }
    return 0u;
  }
  char stack_buf[64];
  size_t len = out.len < sizeof(stack_buf) - 1u ? out.len : sizeof(stack_buf) - 1u;
  memcpy(stack_buf, out.data, len);
  stack_buf[len] = '\0';
  uint32_t value = (uint32_t) strtoul(stack_buf, NULL, 10);
  free(out.data);
  return value;
}

buf_u8 mira_db_query_buf_handle_sql_buf_u8(uint64_t handle, buf_u8 sql) {
  if (handle == 0u || handle > 128u || !mira_db_handles[handle - 1u].used || mira_db_handle_tx_open(handle)) {
    return mira_buf_empty();
  }
  int ok = 0;
  buf_u8 out = mira_db_run_and_capture_target(mira_db_handles[handle - 1u].target, sql, &ok);
  if (!ok) {
    char* error_text = mira_buf_to_cstring(out);
    mira_db_set_error(handle, error_text);
    free(error_text);
    if (out.data != NULL) {
      free(out.data);
    }
    return mira_buf_empty();
  }
  mira_db_clear_error(handle);
  return mira_trim_db_output(out);
}

buf_u8 mira_db_query_row_handle_sql_buf_u8(uint64_t handle, buf_u8 sql) {
  if (handle == 0u || handle > 128u || !mira_db_handles[handle - 1u].used || mira_db_handle_tx_open(handle)) {
    return mira_buf_empty();
  }
  int ok = 0;
  buf_u8 out = mira_db_query_row_capture_target(mira_db_handles[handle - 1u].target, sql, &ok);
  if (!ok) {
    char* error_text = mira_buf_to_cstring(out);
    mira_db_set_error(handle, error_text);
    free(error_text);
    if (out.data != NULL) {
      free(out.data);
    }
    return mira_buf_empty();
  }
  mira_db_clear_error(handle);
  return out;
}

bool mira_db_prepare_handle_stmt_sql_buf_u8(uint64_t handle, const char* stmt_name, buf_u8 sql) {
  if (handle == 0u || handle > 128u || !mira_db_handles[handle - 1u].used || stmt_name == NULL || stmt_name[0] == '\0') {
    return 0u;
  }
  char* text = mira_buf_to_cstring(sql);
  if (text == NULL) {
    return 0u;
  }
  int ok = mira_db_stmt_upsert(&mira_db_handles[handle - 1u], stmt_name, text);
  free(text);
  return ok ? 1u : 0u;
}

static char* mira_db_lookup_prepared_sql(uint64_t handle, const char* stmt_name) {
  if (handle == 0u || handle > 128u || !mira_db_handles[handle - 1u].used || stmt_name == NULL) {
    return NULL;
  }
  mira_db_handle_entry* entry = &mira_db_handles[handle - 1u];
  int found = mira_db_stmt_find_index(entry, stmt_name);
  if (found < 0) {
    return NULL;
  }
  return entry->prepared[found].sql;
}

bool mira_db_exec_prepared_handle_stmt_params_buf_u8(uint64_t handle, const char* stmt_name, buf_u8 params) {
  char* template_sql = mira_db_lookup_prepared_sql(handle, stmt_name);
  char* params_json = mira_buf_to_cstring(params);
  if (template_sql == NULL || params_json == NULL) {
    free(params_json);
    return 0u;
  }
  char* expanded = mira_db_expand_prepared_sql_text(template_sql, params_json);
  free(params_json);
  if (expanded == NULL) {
    return 0u;
  }
  buf_u8 sql = mira_buf_copy_bytes((const uint8_t*) expanded, strlen(expanded));
  free(expanded);
  bool ok = mira_db_exec_handle_sql_buf_u8(handle, sql);
  if (sql.data != NULL) {
    free(sql.data);
  }
  return ok;
}

uint32_t mira_db_query_prepared_u32_handle_stmt_params_buf_u8(uint64_t handle, const char* stmt_name, buf_u8 params) {
  char* template_sql = mira_db_lookup_prepared_sql(handle, stmt_name);
  char* params_json = mira_buf_to_cstring(params);
  if (template_sql == NULL || params_json == NULL) {
    free(params_json);
    return 0u;
  }
  char* expanded = mira_db_expand_prepared_sql_text(template_sql, params_json);
  free(params_json);
  if (expanded == NULL) {
    return 0u;
  }
  buf_u8 sql = mira_buf_copy_bytes((const uint8_t*) expanded, strlen(expanded));
  free(expanded);
  uint32_t out = mira_db_query_u32_handle_sql_buf_u8(handle, sql);
  if (sql.data != NULL) {
    free(sql.data);
  }
  return out;
}

buf_u8 mira_db_query_prepared_buf_handle_stmt_params_buf_u8(uint64_t handle, const char* stmt_name, buf_u8 params) {
  char* template_sql = mira_db_lookup_prepared_sql(handle, stmt_name);
  char* params_json = mira_buf_to_cstring(params);
  if (template_sql == NULL || params_json == NULL) {
    free(params_json);
    return mira_buf_empty();
  }
  char* expanded = mira_db_expand_prepared_sql_text(template_sql, params_json);
  free(params_json);
  if (expanded == NULL) {
    return mira_buf_empty();
  }
  buf_u8 sql = mira_buf_copy_bytes((const uint8_t*) expanded, strlen(expanded));
  free(expanded);
  buf_u8 out = mira_db_query_buf_handle_sql_buf_u8(handle, sql);
  if (sql.data != NULL) {
    free(sql.data);
  }
  return out;
}

buf_u8 mira_db_query_prepared_row_handle_stmt_params_buf_u8(uint64_t handle, const char* stmt_name, buf_u8 params) {
  char* template_sql = mira_db_lookup_prepared_sql(handle, stmt_name);
  char* params_json = mira_buf_to_cstring(params);
  if (template_sql == NULL || params_json == NULL) {
    free(params_json);
    return mira_buf_empty();
  }
  char* expanded = mira_db_expand_prepared_sql_text(template_sql, params_json);
  free(params_json);
  if (expanded == NULL) {
    return mira_buf_empty();
  }
  buf_u8 sql = mira_buf_copy_bytes((const uint8_t*) expanded, strlen(expanded));
  free(expanded);
  buf_u8 out = mira_db_query_row_handle_sql_buf_u8(handle, sql);
  if (sql.data != NULL) {
    free(sql.data);
  }
  return out;
}

uint32_t mira_db_last_error_code_handle(uint64_t handle) {
  if (handle == 0u || handle > 128u || !mira_db_handles[handle - 1u].used) {
    return 1u;
  }
  return mira_db_handles[handle - 1u].last_error_code;
}

bool mira_db_last_error_retryable_handle(uint64_t handle) {
  if (handle == 0u || handle > 128u || !mira_db_handles[handle - 1u].used) {
    return 0u;
  }
  return mira_db_handles[handle - 1u].last_error_retryable ? 1u : 0u;
}

bool mira_db_begin_handle(uint64_t handle) {
  if (handle == 0u || handle > 128u || !mira_db_handles[handle - 1u].used) {
    return 0u;
  }
  mira_db_handle_entry* entry = &mira_db_handles[handle - 1u];
  entry->in_transaction = true;
  if (entry->tx_sql.data != NULL) {
    free(entry->tx_sql.data);
  }
  entry->tx_sql = mira_buf_empty();
  return 1u;
}

bool mira_db_commit_handle(uint64_t handle) {
  if (handle == 0u || handle > 128u || !mira_db_handles[handle - 1u].used) {
    return 0u;
  }
  mira_db_handle_entry* entry = &mira_db_handles[handle - 1u];
  if (!entry->in_transaction) {
    return 0u;
  }
  const char* begin_sql = "BEGIN;";
  const char* commit_sql = "COMMIT;";
  size_t total = strlen(begin_sql) + (size_t) entry->tx_sql.len + strlen(commit_sql);
  uint8_t* data = (uint8_t*) malloc(total);
  if (data == NULL) {
    return 0u;
  }
  size_t cursor = 0u;
  memcpy(data + cursor, begin_sql, strlen(begin_sql));
  cursor += strlen(begin_sql);
  if (entry->tx_sql.len > 0u && entry->tx_sql.data != NULL) {
    memcpy(data + cursor, entry->tx_sql.data, entry->tx_sql.len);
    cursor += entry->tx_sql.len;
  }
  memcpy(data + cursor, commit_sql, strlen(commit_sql));
  buf_u8 sql = mira_buf_from_heap(data, total);
  entry->in_transaction = false;
  if (entry->tx_sql.data != NULL) {
    free(entry->tx_sql.data);
  }
  entry->tx_sql = mira_buf_empty();
  int ok = 0;
  buf_u8 out = mira_db_run_and_capture_target(entry->target, sql, &ok);
  if (sql.data != NULL) {
    free(sql.data);
  }
  if (out.data != NULL) {
    free(out.data);
  }
  return ok ? 1u : 0u;
}

bool mira_db_rollback_handle(uint64_t handle) {
  if (handle == 0u || handle > 128u || !mira_db_handles[handle - 1u].used) {
    return 0u;
  }
  mira_db_handle_entry* entry = &mira_db_handles[handle - 1u];
  bool was_open = entry->in_transaction;
  entry->in_transaction = false;
  if (entry->tx_sql.data != NULL) {
    free(entry->tx_sql.data);
  }
  entry->tx_sql = mira_buf_empty();
  return was_open;
}

uint64_t mira_db_pool_open_handle(const char* target, uint32_t max_size) {
  if (target == NULL || target[0] == '\0' || max_size == 0u) {
    return 0u;
  }
  bool used_flags[64];
  for (size_t index = 0u; index < 64u; index++) {
    used_flags[index] = mira_db_pools[index].used;
  }
  uint64_t handle = mira_alloc_handle(used_flags, 64u);
  if (handle == 0u) {
    return 0u;
  }
  mira_db_pool_entry* entry = &mira_db_pools[handle - 1u];
  memset(entry, 0, sizeof(*entry));
  entry->used = true;
  entry->max_size = max_size > 32u ? 32u : max_size;
  entry->max_idle = entry->max_size;
  strncpy(entry->target, target, sizeof(entry->target) - 1u);
  entry->target[sizeof(entry->target) - 1u] = '\0';
  return handle;
}

bool mira_db_pool_set_max_idle_handle(uint64_t pool_handle, uint32_t max_idle) {
  if (pool_handle == 0u || pool_handle > 64u || !mira_db_pools[pool_handle - 1u].used) {
    return 0u;
  }
  mira_db_pool_entry* pool = &mira_db_pools[pool_handle - 1u];
  pool->max_idle = max_idle > pool->max_size ? pool->max_size : max_idle;
  return 1u;
}

uint32_t mira_db_pool_leased_handle(uint64_t pool_handle) {
  if (pool_handle == 0u || pool_handle > 64u || !mira_db_pools[pool_handle - 1u].used) {
    return 0u;
  }
  return mira_db_pools[pool_handle - 1u].leased_len;
}

uint64_t mira_db_pool_acquire_handle(uint64_t pool_handle) {
  if (pool_handle == 0u || pool_handle > 64u || !mira_db_pools[pool_handle - 1u].used) {
    return 0u;
  }
  mira_db_pool_entry* pool = &mira_db_pools[pool_handle - 1u];
  if (pool->leased_len >= pool->max_size || pool->leased_len >= 32u) {
    return 0u;
  }
  uint64_t db_handle = mira_db_open_handle(pool->target);
  if (db_handle == 0u) {
    return 0u;
  }
  pool->leased_handles[pool->leased_len++] = db_handle;
  return db_handle;
}

bool mira_db_pool_release_handle(uint64_t pool_handle, uint64_t db_handle) {
  if (pool_handle == 0u || pool_handle > 64u || !mira_db_pools[pool_handle - 1u].used) {
    return 0u;
  }
  mira_db_pool_entry* pool = &mira_db_pools[pool_handle - 1u];
  for (uint32_t index = 0u; index < pool->leased_len; index++) {
    if (pool->leased_handles[index] == db_handle) {
      pool->leased_handles[index] = pool->leased_handles[pool->leased_len - 1u];
      pool->leased_len -= 1u;
      return mira_db_close_handle(db_handle);
    }
  }
  return 0u;
}

bool mira_db_pool_close_handle(uint64_t pool_handle) {
  if (pool_handle == 0u || pool_handle > 64u || !mira_db_pools[pool_handle - 1u].used) {
    return 0u;
  }
  mira_db_pool_entry* pool = &mira_db_pools[pool_handle - 1u];
  while (pool->leased_len > 0u) {
    uint64_t db_handle = pool->leased_handles[pool->leased_len - 1u];
    pool->leased_len -= 1u;
    (void) mira_db_close_handle(db_handle);
  }
  memset(pool, 0, sizeof(*pool));
  return 1u;
}

uint64_t mira_cache_open_handle(const char* target) {
  if (target == NULL || target[0] == '\0') {
    return 0u;
  }
  bool used_flags[64];
  for (size_t index = 0u; index < 64u; index++) {
    used_flags[index] = mira_cache_handles[index].used;
  }
  uint64_t handle = mira_alloc_handle(used_flags, 64u);
  if (handle == 0u) {
    return 0u;
  }
  memset(&mira_cache_handles[handle - 1u], 0, sizeof(mira_cache_handles[handle - 1u]));
  mira_cache_handles[handle - 1u].used = true;
  strncpy(mira_cache_handles[handle - 1u].target, target, sizeof(mira_cache_handles[handle - 1u].target) - 1u);
  return handle;
}

bool mira_cache_close_handle(uint64_t handle) {
  if (handle == 0u || handle > 64u || !mira_cache_handles[handle - 1u].used) {
    return 0u;
  }
  memset(&mira_cache_handles[handle - 1u], 0, sizeof(mira_cache_handles[handle - 1u]));
  return 1u;
}

static const char* mira_cache_target_for_handle(uint64_t handle) {
  if (handle == 0u || handle > 64u || !mira_cache_handles[handle - 1u].used) {
    return NULL;
  }
  return mira_cache_handles[handle - 1u].target;
}

buf_u8 mira_cache_get_buf_handle_key_u8(uint64_t handle, buf_u8 key) {
  const char* path = mira_cache_target_for_handle(handle);
  if (path == NULL) {
    return mira_buf_empty();
  }
  buf_u8 file = mira_read_file_all_bytes(path);
  char* key_hex = mira_buf_hex_encode(key);
  if (key_hex == NULL) {
    if (file.data != NULL) free(file.data);
    return mira_buf_empty();
  }
  buf_u8 out = mira_buf_empty();
  uint64_t now = mira_now_ms();
  if (file.data != NULL) {
    char* text = mira_buf_to_cstring(file);
    char* cursor = text;
    while (cursor != NULL && *cursor != '\0') {
      char* line_end = strchr(cursor, '\n');
      if (line_end != NULL) {
        *line_end = '\0';
      }
      char* kind = cursor;
      char* key_part = strchr(kind, '\t');
      char* expiry_part = NULL;
      char* value_part = NULL;
      if (key_part != NULL) {
        *key_part = '\0';
        key_part += 1;
        expiry_part = strchr(key_part, '\t');
      }
      if (expiry_part != NULL) {
        *expiry_part = '\0';
        expiry_part += 1;
        value_part = strchr(expiry_part, '\t');
      }
      if (value_part != NULL) {
        *value_part = '\0';
        value_part += 1;
      }
      if (kind != NULL && key_part != NULL && strcmp(key_part, key_hex) == 0) {
        if (strcmp(kind, "D") == 0) {
          if (out.data != NULL) {
            free(out.data);
            out = mira_buf_empty();
          }
        } else if (value_part != NULL) {
          uint64_t expiry = (expiry_part != NULL && strcmp(expiry_part, "-") != 0) ? strtoull(expiry_part, NULL, 10) : 0u;
          if (expiry == 0u || expiry > now) {
            if (out.data != NULL) {
              free(out.data);
            }
            out = mira_hex_decode_buf(value_part);
          }
        }
      }
      if (line_end == NULL) {
        break;
      }
      cursor = line_end + 1;
    }
    free(text);
    free(file.data);
  }
  free(key_hex);
  return out;
}

bool mira_cache_set_buf_handle_key_value_u8(uint64_t handle, buf_u8 key, buf_u8 value) {
  return mira_cache_set_buf_ttl_handle_key_value_u8(handle, key, 0u, value);
}

bool mira_cache_set_buf_ttl_handle_key_value_u8(uint64_t handle, buf_u8 key, uint32_t ttl_ms, buf_u8 value) {
  const char* path = mira_cache_target_for_handle(handle);
  if (path == NULL) {
    return 0u;
  }
  char* key_hex = mira_buf_hex_encode(key);
  char* value_hex = mira_buf_hex_encode(value);
  if (key_hex == NULL || value_hex == NULL) {
    free(key_hex);
    free(value_hex);
    return 0u;
  }
  char line[8192];
  if (ttl_ms == 0u) {
    snprintf(line, sizeof(line), "S\t%s\t-\t%s", key_hex, value_hex);
  } else {
    snprintf(line, sizeof(line), "S\t%s\t%" PRIu64 "\t%s", key_hex, mira_now_ms() + ttl_ms, value_hex);
  }
  free(key_hex);
  free(value_hex);
  return mira_append_text_line(path, line) ? 1u : 0u;
}

bool mira_cache_del_handle_key_u8(uint64_t handle, buf_u8 key) {
  const char* path = mira_cache_target_for_handle(handle);
  if (path == NULL) {
    return 0u;
  }
  char* key_hex = mira_buf_hex_encode(key);
  if (key_hex == NULL) {
    return 0u;
  }
  char line[4096];
  snprintf(line, sizeof(line), "D\t%s\t-\t-", key_hex);
  free(key_hex);
  return mira_append_text_line(path, line) ? 1u : 0u;
}

uint64_t mira_queue_open_handle(const char* target) {
  if (target == NULL || target[0] == '\0') {
    return 0u;
  }
  bool used_flags[64];
  for (size_t index = 0u; index < 64u; index++) {
    used_flags[index] = mira_queue_handles[index].used;
  }
  uint64_t handle = mira_alloc_handle(used_flags, 64u);
  if (handle == 0u) {
    return 0u;
  }
  memset(&mira_queue_handles[handle - 1u], 0, sizeof(mira_queue_handles[handle - 1u]));
  mira_queue_handles[handle - 1u].used = true;
  strncpy(mira_queue_handles[handle - 1u].target, target, sizeof(mira_queue_handles[handle - 1u].target) - 1u);
  return handle;
}

bool mira_queue_close_handle(uint64_t handle) {
  if (handle == 0u || handle > 64u || !mira_queue_handles[handle - 1u].used) {
    return 0u;
  }
  memset(&mira_queue_handles[handle - 1u], 0, sizeof(mira_queue_handles[handle - 1u]));
  return 1u;
}

static const char* mira_queue_target_for_handle(uint64_t handle) {
  if (handle == 0u || handle > 64u || !mira_queue_handles[handle - 1u].used) {
    return NULL;
  }
  return mira_queue_handles[handle - 1u].target;
}

bool mira_queue_push_buf_handle_value_u8(uint64_t handle, buf_u8 value) {
  const char* path = mira_queue_target_for_handle(handle);
  if (path == NULL) {
    return 0u;
  }
  char* value_hex = mira_buf_hex_encode(value);
  if (value_hex == NULL) {
    return 0u;
  }
  int ok = mira_append_text_line(path, value_hex);
  free(value_hex);
  return ok ? 1u : 0u;
}

buf_u8 mira_queue_pop_buf_handle(uint64_t handle) {
  const char* path = mira_queue_target_for_handle(handle);
  if (path == NULL) {
    return mira_buf_empty();
  }
  buf_u8 file = mira_read_file_all_bytes(path);
  if (file.data == NULL || file.len == 0u) {
    if (file.data != NULL) free(file.data);
    return mira_buf_empty();
  }
  char* text = mira_buf_to_cstring(file);
  free(file.data);
  if (text == NULL) {
    return mira_buf_empty();
  }
  char* save = NULL;
  char* line = strtok_r(text, "\n", &save);
  if (line == NULL) {
    free(text);
    return mira_buf_empty();
  }
  buf_u8 out = mira_hex_decode_buf(line);
  char* remainder = save;
  int ok = 1;
  if (remainder != NULL && remainder[0] != '\0') {
    ok = mira_write_bytes_file(path, (const uint8_t*) remainder, strlen(remainder));
  } else {
    ok = mira_write_bytes_file(path, (const uint8_t*) "", 0u);
  }
  free(text);
  if (!ok) {
    if (out.data != NULL) free(out.data);
    return mira_buf_empty();
  }
  return out;
}

uint32_t mira_queue_len_handle(uint64_t handle) {
  const char* path = mira_queue_target_for_handle(handle);
  if (path == NULL) {
    return 0u;
  }
  buf_u8 file = mira_read_file_all_bytes(path);
  if (file.data == NULL || file.len == 0u) {
    if (file.data != NULL) free(file.data);
    return 0u;
  }
  uint32_t count = 0u;
  for (uint32_t index = 0u; index < file.len; index++) {
    if (file.data[index] == '\n') {
      count += 1u;
    }
  }
  if (file.len > 0u && file.data[file.len - 1u] != '\n') {
    count += 1u;
  }
  free(file.data);
  return count;
}

uint64_t mira_stream_open_handle(const char* target) {
  if (target == NULL || target[0] == '\0') {
    return 0u;
  }
  bool used_flags[64];
  for (size_t index = 0u; index < 64u; index++) {
    used_flags[index] = mira_stream_handles[index].used;
  }
  uint64_t handle = mira_alloc_handle(used_flags, 64u);
  if (handle == 0u) {
    return 0u;
  }
  memset(&mira_stream_handles[handle - 1u], 0, sizeof(mira_stream_handles[handle - 1u]));
  mira_stream_handles[handle - 1u].used = true;
  strncpy(mira_stream_handles[handle - 1u].target, target, sizeof(mira_stream_handles[handle - 1u].target) - 1u);
  return handle;
}

static const char* mira_stream_target_for_handle(uint64_t handle) {
  if (handle == 0u || handle > 64u || !mira_stream_handles[handle - 1u].used) {
    return NULL;
  }
  return mira_stream_handles[handle - 1u].target;
}

uint32_t mira_stream_len_handle(uint64_t handle);

bool mira_stream_close_handle(uint64_t handle) {
  if (handle == 0u || handle > 64u || !mira_stream_handles[handle - 1u].used) {
    return 0u;
  }
  memset(&mira_stream_handles[handle - 1u], 0, sizeof(mira_stream_handles[handle - 1u]));
  for (size_t index = 0u; index < 128u; index++) {
    if (mira_stream_replays[index].used && mira_stream_replays[index].stream_handle == handle) {
      memset(&mira_stream_replays[index], 0, sizeof(mira_stream_replay_entry));
    }
  }
  return 1u;
}

uint32_t mira_stream_publish_buf_handle_value_u8(uint64_t handle, buf_u8 value) {
  const char* path = mira_stream_target_for_handle(handle);
  if (path == NULL) {
    return 0u;
  }
  char* value_hex = mira_buf_hex_encode(value);
  if (value_hex == NULL) {
    return 0u;
  }
  int ok = mira_append_text_line(path, value_hex);
  free(value_hex);
  return ok ? mira_stream_len_handle(handle) : 0u;
}

uint32_t mira_stream_len_handle(uint64_t handle) {
  const char* path = mira_stream_target_for_handle(handle);
  if (path == NULL) {
    return 0u;
  }
  buf_u8 file = mira_read_file_all_bytes(path);
  if (file.data == NULL || file.len == 0u) {
    if (file.data != NULL) free(file.data);
    return 0u;
  }
  uint32_t count = 0u;
  for (uint32_t index = 0u; index < file.len; index++) {
    if (file.data[index] == '\n') {
      count += 1u;
    }
  }
  if (file.len > 0u && file.data[file.len - 1u] != '\n') {
    count += 1u;
  }
  free(file.data);
  return count;
}

uint64_t mira_stream_replay_open_handle(uint64_t handle, uint32_t offset) {
  if (mira_stream_target_for_handle(handle) == NULL) {
    return 0u;
  }
  bool used_flags[128];
  for (size_t index = 0u; index < 128u; index++) {
    used_flags[index] = mira_stream_replays[index].used;
  }
  uint64_t replay = mira_alloc_handle(used_flags, 128u);
  if (replay == 0u) {
    return 0u;
  }
  memset(&mira_stream_replays[replay - 1u], 0, sizeof(mira_stream_replay_entry));
  mira_stream_replays[replay - 1u].used = true;
  mira_stream_replays[replay - 1u].stream_handle = handle;
  mira_stream_replays[replay - 1u].offset = offset;
  mira_stream_replays[replay - 1u].last_offset = 0u;
  return replay;
}

buf_u8 mira_stream_replay_next_handle(uint64_t handle) {
  if (handle == 0u || handle > 128u || !mira_stream_replays[handle - 1u].used) {
    return mira_buf_empty();
  }
  mira_stream_replay_entry* replay = &mira_stream_replays[handle - 1u];
  const char* path = mira_stream_target_for_handle(replay->stream_handle);
  if (path == NULL) {
    return mira_buf_empty();
  }
  buf_u8 file = mira_read_file_all_bytes(path);
  if (file.data == NULL || file.len == 0u) {
    if (file.data != NULL) free(file.data);
    return mira_buf_empty();
  }
  char* text = mira_buf_to_cstring(file);
  free(file.data);
  if (text == NULL) {
    return mira_buf_empty();
  }
  uint32_t index = 1u;
  buf_u8 out = mira_buf_empty();
  char* save = NULL;
  char* line = strtok_r(text, "\n", &save);
  while (line != NULL) {
    if (line[0] != '\0') {
      bool match = (replay->offset == 0u) ? (index >= 1u) : (index == replay->offset);
      if (match) {
        out = mira_hex_decode_buf(line);
        replay->last_offset = index;
        replay->offset = index + 1u;
        break;
      }
      index += 1u;
    }
    line = strtok_r(NULL, "\n", &save);
  }
  free(text);
  return out;
}

uint32_t mira_stream_replay_offset_handle(uint64_t handle) {
  if (handle == 0u || handle > 128u || !mira_stream_replays[handle - 1u].used) {
    return 0u;
  }
  return mira_stream_replays[handle - 1u].last_offset;
}

bool mira_stream_replay_close_handle(uint64_t handle) {
  if (handle == 0u || handle > 128u || !mira_stream_replays[handle - 1u].used) {
    return 0u;
  }
  memset(&mira_stream_replays[handle - 1u], 0, sizeof(mira_stream_replay_entry));
  return 1u;
}

uint32_t mira_shard_route_u32_buf_u8(buf_u8 key, uint32_t shard_count) {
  if (shard_count == 0u) {
    return 0u;
  }
  uint32_t hash = 2166136261u;
  for (uint32_t index = 0u; index < key.len; index++) {
    hash ^= (uint32_t) key.data[index];
    hash *= 16777619u;
  }
  return hash % shard_count;
}

uint64_t mira_lease_open_handle(const char* target) {
  if (target == NULL || target[0] == '\0') {
    return 0u;
  }
  bool used_flags[64];
  for (size_t index = 0u; index < 64u; index++) {
    used_flags[index] = mira_lease_handles[index].used;
  }
  uint64_t handle = mira_alloc_handle(used_flags, 64u);
  if (handle == 0u) {
    return 0u;
  }
  memset(&mira_lease_handles[handle - 1u], 0, sizeof(mira_lease_handles[handle - 1u]));
  mira_lease_handles[handle - 1u].used = true;
  strncpy(mira_lease_handles[handle - 1u].target, target, sizeof(mira_lease_handles[handle - 1u].target) - 1u);
  return handle;
}

static const char* mira_lease_target_for_handle(uint64_t handle) {
  if (handle == 0u || handle > 64u || !mira_lease_handles[handle - 1u].used) {
    return NULL;
  }
  return mira_lease_handles[handle - 1u].target;
}

static uint32_t mira_lease_read_owner(const char* path) {
  buf_u8 file = mira_read_file_all_bytes(path);
  if (file.data == NULL || file.len == 0u) {
    if (file.data != NULL) free(file.data);
    return 0u;
  }
  char* text = mira_buf_to_cstring(file);
  free(file.data);
  if (text == NULL) {
    return 0u;
  }
  uint32_t owner = 0u;
  char* save = NULL;
  char* line = strtok_r(text, "\n", &save);
  while (line != NULL) {
    char* tab = strchr(line, '\t');
    if (tab != NULL) {
      *tab = '\0';
      uint32_t parsed = (uint32_t) strtoul(tab + 1u, NULL, 10);
      if (strcmp(line, "H") == 0) {
        owner = parsed;
      } else if (strcmp(line, "R") == 0) {
        owner = 0u;
      }
    }
    line = strtok_r(NULL, "\n", &save);
  }
  free(text);
  return owner;
}

static bool mira_lease_append(const char* path, const char* kind, uint32_t owner) {
  char line[128];
  snprintf(line, sizeof(line), "%s\t%u", kind, owner);
  return mira_append_text_line(path, line) ? 1u : 0u;
}

bool mira_lease_acquire_handle(uint64_t handle, uint32_t owner) {
  const char* path = mira_lease_target_for_handle(handle);
  if (path == NULL || owner == 0u) {
    return 0u;
  }
  uint32_t current = mira_lease_read_owner(path);
  if (current != 0u && current != owner) {
    return 0u;
  }
  return mira_lease_append(path, "H", owner);
}

uint32_t mira_lease_owner_handle(uint64_t handle) {
  const char* path = mira_lease_target_for_handle(handle);
  if (path == NULL) {
    return 0u;
  }
  return mira_lease_read_owner(path);
}

bool mira_lease_transfer_handle(uint64_t handle, uint32_t owner) {
  const char* path = mira_lease_target_for_handle(handle);
  if (path == NULL || owner == 0u) {
    return 0u;
  }
  uint32_t current = mira_lease_read_owner(path);
  if (current == 0u) {
    return 0u;
  }
  return mira_lease_append(path, "H", owner);
}

bool mira_lease_release_handle(uint64_t handle, uint32_t owner) {
  const char* path = mira_lease_target_for_handle(handle);
  if (path == NULL || owner == 0u) {
    return 0u;
  }
  uint32_t current = mira_lease_read_owner(path);
  if (current != owner) {
    return 0u;
  }
  return mira_lease_append(path, "R", owner);
}

bool mira_lease_close_handle(uint64_t handle) {
  if (handle == 0u || handle > 64u || !mira_lease_handles[handle - 1u].used) {
    return 0u;
  }
  memset(&mira_lease_handles[handle - 1u], 0, sizeof(mira_lease_handles[handle - 1u]));
  return 1u;
}

uint64_t mira_placement_open_handle(const char* target) {
  if (target == NULL || target[0] == '\0') {
    return 0u;
  }
  bool used_flags[64];
  for (size_t index = 0u; index < 64u; index++) {
    used_flags[index] = mira_placement_handles[index].used;
  }
  uint64_t handle = mira_alloc_handle(used_flags, 64u);
  if (handle == 0u) {
    return 0u;
  }
  memset(&mira_placement_handles[handle - 1u], 0, sizeof(mira_placement_handles[handle - 1u]));
  mira_placement_handles[handle - 1u].used = true;
  strncpy(mira_placement_handles[handle - 1u].target, target, sizeof(mira_placement_handles[handle - 1u].target) - 1u);
  return handle;
}

static const char* mira_placement_target_for_handle(uint64_t handle) {
  if (handle == 0u || handle > 64u || !mira_placement_handles[handle - 1u].used) {
    return NULL;
  }
  return mira_placement_handles[handle - 1u].target;
}

static bool mira_placement_load(const char* path, uint32_t* shards, uint32_t* nodes, uint32_t* len) {
  *len = 0u;
  buf_u8 file = mira_read_file_all_bytes(path);
  if (file.data == NULL || file.len == 0u) {
    if (file.data != NULL) free(file.data);
    return true;
  }
  char* text = mira_buf_to_cstring(file);
  free(file.data);
  if (text == NULL) {
    return false;
  }
  char* save = NULL;
  char* line = strtok_r(text, "\n", &save);
  while (line != NULL && *len < 256u) {
    char* tab = strchr(line, '\t');
    if (tab != NULL) {
      *tab = '\0';
      shards[*len] = (uint32_t) strtoul(line, NULL, 10);
      nodes[*len] = (uint32_t) strtoul(tab + 1u, NULL, 10);
      *len += 1u;
    }
    line = strtok_r(NULL, "\n", &save);
  }
  free(text);
  return true;
}

static bool mira_placement_write(const char* path, uint32_t* shards, uint32_t* nodes, uint32_t len) {
  char buffer[8192];
  buffer[0] = '\0';
  size_t cursor = 0u;
  for (uint32_t index = 0u; index < len; index++) {
    int wrote = snprintf(buffer + cursor, sizeof(buffer) - cursor, "%u\t%u\n", shards[index], nodes[index]);
    if (wrote < 0 || (size_t) wrote >= sizeof(buffer) - cursor) {
      return false;
    }
    cursor += (size_t) wrote;
  }
  return mira_write_bytes_file(path, (const uint8_t*) buffer, cursor) ? 1u : 0u;
}

bool mira_placement_assign_handle(uint64_t handle, uint32_t shard, uint32_t node) {
  const char* path = mira_placement_target_for_handle(handle);
  if (path == NULL) {
    return 0u;
  }
  uint32_t shards[256];
  uint32_t nodes[256];
  uint32_t len = 0u;
  if (!mira_placement_load(path, shards, nodes, &len)) {
    return 0u;
  }
  for (uint32_t index = 0u; index < len; index++) {
    if (shards[index] == shard) {
      nodes[index] = node;
      return mira_placement_write(path, shards, nodes, len);
    }
  }
  if (len >= 256u) {
    return 0u;
  }
  shards[len] = shard;
  nodes[len] = node;
  len += 1u;
  return mira_placement_write(path, shards, nodes, len);
}

uint32_t mira_placement_lookup_handle(uint64_t handle, uint32_t shard) {
  const char* path = mira_placement_target_for_handle(handle);
  if (path == NULL) {
    return 0u;
  }
  uint32_t shards[256];
  uint32_t nodes[256];
  uint32_t len = 0u;
  if (!mira_placement_load(path, shards, nodes, &len)) {
    return 0u;
  }
  for (uint32_t index = 0u; index < len; index++) {
    if (shards[index] == shard) {
      return nodes[index];
    }
  }
  return 0u;
}

bool mira_placement_close_handle(uint64_t handle) {
  if (handle == 0u || handle > 64u || !mira_placement_handles[handle - 1u].used) {
    return 0u;
  }
  memset(&mira_placement_handles[handle - 1u], 0, sizeof(mira_placement_handles[handle - 1u]));
  return 1u;
}

uint64_t mira_coord_open_handle(const char* target) {
  if (target == NULL || target[0] == '\0') {
    return 0u;
  }
  bool used_flags[64];
  for (size_t index = 0u; index < 64u; index++) {
    used_flags[index] = mira_coord_handles[index].used;
  }
  uint64_t handle = mira_alloc_handle(used_flags, 64u);
  if (handle == 0u) {
    return 0u;
  }
  memset(&mira_coord_handles[handle - 1u], 0, sizeof(mira_coord_handles[handle - 1u]));
  mira_coord_handles[handle - 1u].used = true;
  strncpy(mira_coord_handles[handle - 1u].target, target, sizeof(mira_coord_handles[handle - 1u].target) - 1u);
  return handle;
}

static const char* mira_coord_target_for_handle(uint64_t handle) {
  if (handle == 0u || handle > 64u || !mira_coord_handles[handle - 1u].used) {
    return NULL;
  }
  return mira_coord_handles[handle - 1u].target;
}

static bool mira_coord_load(const char* path, char keys[][128], uint32_t* values, uint32_t* len) {
  *len = 0u;
  buf_u8 file = mira_read_file_all_bytes(path);
  if (file.data == NULL || file.len == 0u) {
    if (file.data != NULL) free(file.data);
    return true;
  }
  char* text = mira_buf_to_cstring(file);
  free(file.data);
  if (text == NULL) {
    return false;
  }
  char* save = NULL;
  char* line = strtok_r(text, "\n", &save);
  while (line != NULL && *len < 256u) {
    char* tab = strchr(line, '\t');
    if (tab != NULL) {
      *tab = '\0';
      strncpy(keys[*len], line, 127u);
      keys[*len][127u] = '\0';
      values[*len] = (uint32_t) strtoul(tab + 1u, NULL, 10);
      *len += 1u;
    }
    line = strtok_r(NULL, "\n", &save);
  }
  free(text);
  return true;
}

static bool mira_coord_write(const char* path, char keys[][128], uint32_t* values, uint32_t len) {
  char buffer[16384];
  buffer[0] = '\0';
  size_t cursor = 0u;
  for (uint32_t index = 0u; index < len; index++) {
    int wrote = snprintf(buffer + cursor, sizeof(buffer) - cursor, "%s\t%u\n", keys[index], values[index]);
    if (wrote < 0 || (size_t) wrote >= sizeof(buffer) - cursor) {
      return false;
    }
    cursor += (size_t) wrote;
  }
  return mira_write_bytes_file(path, (const uint8_t*) buffer, cursor) ? 1u : 0u;
}

bool mira_coord_store_u32_handle(uint64_t handle, const char* key, uint32_t value) {
  const char* path = mira_coord_target_for_handle(handle);
  if (path == NULL || key == NULL || key[0] == '\0') {
    return 0u;
  }
  char keys[256][128];
  uint32_t values[256];
  uint32_t len = 0u;
  if (!mira_coord_load(path, keys, values, &len)) {
    return 0u;
  }
  for (uint32_t index = 0u; index < len; index++) {
    if (strcmp(keys[index], key) == 0) {
      values[index] = value;
      return mira_coord_write(path, keys, values, len);
    }
  }
  if (len >= 256u) {
    return 0u;
  }
  strncpy(keys[len], key, 127u);
  keys[len][127u] = '\0';
  values[len] = value;
  len += 1u;
  return mira_coord_write(path, keys, values, len);
}

uint32_t mira_coord_load_u32_handle(uint64_t handle, const char* key) {
  const char* path = mira_coord_target_for_handle(handle);
  if (path == NULL || key == NULL || key[0] == '\0') {
    return 0u;
  }
  char keys[256][128];
  uint32_t values[256];
  uint32_t len = 0u;
  if (!mira_coord_load(path, keys, values, &len)) {
    return 0u;
  }
  for (uint32_t index = 0u; index < len; index++) {
    if (strcmp(keys[index], key) == 0) {
      return values[index];
    }
  }
  return 0u;
}

bool mira_coord_close_handle(uint64_t handle) {
  if (handle == 0u || handle > 64u || !mira_coord_handles[handle - 1u].used) {
    return 0u;
  }
  memset(&mira_coord_handles[handle - 1u], 0, sizeof(mira_coord_handles[handle - 1u]));
  return 1u;
}

uint64_t mira_batch_open_handle(void) {
  bool used_flags[128];
  for (size_t index = 0u; index < 128u; index++) {
    used_flags[index] = mira_batch_handles[index].used;
  }
  uint64_t handle = mira_alloc_handle(used_flags, 128u);
  if (handle == 0u) {
    return 0u;
  }
  memset(&mira_batch_handles[handle - 1u], 0, sizeof(mira_batch_entry));
  mira_batch_handles[handle - 1u].used = true;
  return handle;
}

bool mira_batch_push_u64_handle_value(uint64_t handle, uint64_t value) {
  if (handle == 0u || handle > 128u || !mira_batch_handles[handle - 1u].used) {
    return 0u;
  }
  mira_batch_entry* entry = &mira_batch_handles[handle - 1u];
  if (entry->len >= 1024u) {
    return 0u;
  }
  entry->values[entry->len++] = value;
  return 1u;
}

uint32_t mira_batch_len_handle(uint64_t handle) {
  if (handle == 0u || handle > 128u || !mira_batch_handles[handle - 1u].used) {
    return 0u;
  }
  return mira_batch_handles[handle - 1u].len;
}

uint64_t mira_batch_flush_sum_u64_handle(uint64_t handle) {
  if (handle == 0u || handle > 128u || !mira_batch_handles[handle - 1u].used) {
    return 0u;
  }
  mira_batch_entry* entry = &mira_batch_handles[handle - 1u];
  uint64_t sum = 0u;
  for (uint32_t index = 0u; index < entry->len; index++) {
    sum += entry->values[index];
  }
  entry->len = 0u;
  return sum;
}

bool mira_batch_close_handle(uint64_t handle) {
  if (handle == 0u || handle > 128u || !mira_batch_handles[handle - 1u].used) {
    return 0u;
  }
  memset(&mira_batch_handles[handle - 1u], 0, sizeof(mira_batch_entry));
  return 1u;
}

uint64_t mira_agg_open_u64_handle(void) {
  bool used_flags[128];
  for (size_t index = 0u; index < 128u; index++) {
    used_flags[index] = mira_agg_u64_handles[index].used;
  }
  uint64_t handle = mira_alloc_handle(used_flags, 128u);
  if (handle == 0u) {
    return 0u;
  }
  memset(&mira_agg_u64_handles[handle - 1u], 0, sizeof(mira_agg_u64_entry));
  mira_agg_u64_handles[handle - 1u].used = true;
  return handle;
}

bool mira_agg_add_u64_handle_value(uint64_t handle, uint64_t value) {
  if (handle == 0u || handle > 128u || !mira_agg_u64_handles[handle - 1u].used) {
    return 0u;
  }
  mira_agg_u64_entry* entry = &mira_agg_u64_handles[handle - 1u];
  entry->count += 1u;
  entry->sum += value;
  if (!entry->has_value) {
    entry->min = value;
    entry->max = value;
    entry->has_value = 1;
  } else {
    if (value < entry->min) entry->min = value;
    if (value > entry->max) entry->max = value;
  }
  return 1u;
}

uint32_t mira_agg_count_handle(uint64_t handle) {
  if (handle == 0u || handle > 128u || !mira_agg_u64_handles[handle - 1u].used) {
    return 0u;
  }
  return (uint32_t) mira_agg_u64_handles[handle - 1u].count;
}

uint64_t mira_agg_sum_u64_handle(uint64_t handle) {
  if (handle == 0u || handle > 128u || !mira_agg_u64_handles[handle - 1u].used) {
    return 0u;
  }
  return mira_agg_u64_handles[handle - 1u].sum;
}

uint64_t mira_agg_avg_u64_handle(uint64_t handle) {
  if (handle == 0u || handle > 128u || !mira_agg_u64_handles[handle - 1u].used) {
    return 0u;
  }
  mira_agg_u64_entry* entry = &mira_agg_u64_handles[handle - 1u];
  if (entry->count == 0u) {
    return 0u;
  }
  return entry->sum / entry->count;
}

uint64_t mira_agg_min_u64_handle(uint64_t handle) {
  if (handle == 0u || handle > 128u || !mira_agg_u64_handles[handle - 1u].used) {
    return 0u;
  }
  return mira_agg_u64_handles[handle - 1u].has_value ? mira_agg_u64_handles[handle - 1u].min : 0u;
}

uint64_t mira_agg_max_u64_handle(uint64_t handle) {
  if (handle == 0u || handle > 128u || !mira_agg_u64_handles[handle - 1u].used) {
    return 0u;
  }
  return mira_agg_u64_handles[handle - 1u].has_value ? mira_agg_u64_handles[handle - 1u].max : 0u;
}

bool mira_agg_close_handle(uint64_t handle) {
  if (handle == 0u || handle > 128u || !mira_agg_u64_handles[handle - 1u].used) {
    return 0u;
  }
  memset(&mira_agg_u64_handles[handle - 1u], 0, sizeof(mira_agg_u64_entry));
  return 1u;
}

static void mira_window_trim(mira_window_u64_entry* entry) {
  if (entry == NULL || entry->len == 0u) {
    return;
  }
  uint64_t now_ms = mira_now_ms();
  uint32_t keep_from = 0u;
  while (keep_from < entry->len && now_ms - entry->timestamps_ms[keep_from] > entry->width_ms) {
    keep_from += 1u;
  }
  if (keep_from == 0u) {
    return;
  }
  if (keep_from >= entry->len) {
    entry->len = 0u;
    return;
  }
  uint32_t new_len = entry->len - keep_from;
  memmove(entry->values, entry->values + keep_from, (size_t) new_len * sizeof(uint64_t));
  memmove(entry->timestamps_ms, entry->timestamps_ms + keep_from, (size_t) new_len * sizeof(uint64_t));
  entry->len = new_len;
}

uint64_t mira_window_open_ms_handle(uint32_t width_ms) {
  bool used_flags[128];
  for (size_t index = 0u; index < 128u; index++) {
    used_flags[index] = mira_window_u64_handles[index].used;
  }
  uint64_t handle = mira_alloc_handle(used_flags, 128u);
  if (handle == 0u) {
    return 0u;
  }
  memset(&mira_window_u64_handles[handle - 1u], 0, sizeof(mira_window_u64_entry));
  mira_window_u64_handles[handle - 1u].used = true;
  mira_window_u64_handles[handle - 1u].width_ms = width_ms;
  return handle;
}

bool mira_window_add_u64_handle_value(uint64_t handle, uint64_t value) {
  if (handle == 0u || handle > 128u || !mira_window_u64_handles[handle - 1u].used) {
    return 0u;
  }
  mira_window_u64_entry* entry = &mira_window_u64_handles[handle - 1u];
  mira_window_trim(entry);
  if (entry->len >= 1024u) {
    memmove(entry->values, entry->values + 1u, 1023u * sizeof(uint64_t));
    memmove(entry->timestamps_ms, entry->timestamps_ms + 1u, 1023u * sizeof(uint64_t));
    entry->len = 1023u;
  }
  entry->values[entry->len] = value;
  entry->timestamps_ms[entry->len] = mira_now_ms();
  entry->len += 1u;
  return 1u;
}

uint32_t mira_window_count_handle(uint64_t handle) {
  if (handle == 0u || handle > 128u || !mira_window_u64_handles[handle - 1u].used) {
    return 0u;
  }
  mira_window_trim(&mira_window_u64_handles[handle - 1u]);
  return mira_window_u64_handles[handle - 1u].len;
}

static uint64_t mira_window_sum_inner(mira_window_u64_entry* entry) {
  uint64_t sum = 0u;
  for (uint32_t index = 0u; index < entry->len; index++) {
    sum += entry->values[index];
  }
  return sum;
}

uint64_t mira_window_sum_u64_handle(uint64_t handle) {
  if (handle == 0u || handle > 128u || !mira_window_u64_handles[handle - 1u].used) {
    return 0u;
  }
  mira_window_u64_entry* entry = &mira_window_u64_handles[handle - 1u];
  mira_window_trim(entry);
  return mira_window_sum_inner(entry);
}

uint64_t mira_window_avg_u64_handle(uint64_t handle) {
  if (handle == 0u || handle > 128u || !mira_window_u64_handles[handle - 1u].used) {
    return 0u;
  }
  mira_window_u64_entry* entry = &mira_window_u64_handles[handle - 1u];
  mira_window_trim(entry);
  if (entry->len == 0u) {
    return 0u;
  }
  return mira_window_sum_inner(entry) / entry->len;
}

uint64_t mira_window_min_u64_handle(uint64_t handle) {
  if (handle == 0u || handle > 128u || !mira_window_u64_handles[handle - 1u].used) {
    return 0u;
  }
  mira_window_u64_entry* entry = &mira_window_u64_handles[handle - 1u];
  mira_window_trim(entry);
  if (entry->len == 0u) {
    return 0u;
  }
  uint64_t min = entry->values[0];
  for (uint32_t index = 1u; index < entry->len; index++) {
    if (entry->values[index] < min) min = entry->values[index];
  }
  return min;
}

uint64_t mira_window_max_u64_handle(uint64_t handle) {
  if (handle == 0u || handle > 128u || !mira_window_u64_handles[handle - 1u].used) {
    return 0u;
  }
  mira_window_u64_entry* entry = &mira_window_u64_handles[handle - 1u];
  mira_window_trim(entry);
  if (entry->len == 0u) {
    return 0u;
  }
  uint64_t max = entry->values[0];
  for (uint32_t index = 1u; index < entry->len; index++) {
    if (entry->values[index] > max) max = entry->values[index];
  }
  return max;
}

bool mira_window_close_handle(uint64_t handle) {
  if (handle == 0u || handle > 128u || !mira_window_u64_handles[handle - 1u].used) {
    return 0u;
  }
  memset(&mira_window_u64_handles[handle - 1u], 0, sizeof(mira_window_u64_entry));
  return 1u;
}

bool mira_task_sleep_ms(uint32_t millis) {
#ifdef _WIN32
  Sleep(millis);
#else
  usleep((useconds_t) millis * 1000u);
#endif
  return 1u;
}

buf_u8 mira_tls_exchange_all_buf_u8(const char* host, uint16_t port, buf_u8 request) {
#ifdef _WIN32
  (void) host;
  (void) port;
  (void) request;
  return mira_buf_empty();
#else
  if (host == NULL || request.data == NULL) {
    return mira_buf_empty();
  }
  char req_path[512];
  char out_path[512];
  strcpy(req_path, "/tmp/mira_tls_req_XXXXXX");
  strcpy(out_path, "/tmp/mira_tls_out_XXXXXX");
  int req_fd = mira_mkstemp_path(req_path, sizeof(req_path), req_path);
  int out_fd = mira_mkstemp_path(out_path, sizeof(out_path), out_path);
  if (req_fd < 0 || out_fd < 0) {
    if (req_fd >= 0) { close(req_fd); unlink(req_path); }
    if (out_fd >= 0) { close(out_fd); unlink(out_path); }
    return mira_buf_empty();
  }
  close(req_fd);
  close(out_fd);
  if (!mira_write_bytes_file(req_path, request.data, request.len)) {
    unlink(req_path);
    unlink(out_path);
    return mira_buf_empty();
  }
  char command[2048];
  snprintf(
      command,
      sizeof(command),
      "openssl s_client -quiet -connect '%s:%u' -servername '%s' < '%s' > '%s' 2>/dev/null",
      host,
      (unsigned) port,
      host,
      req_path,
      out_path);
  int status = system(command);
  buf_u8 out = mira_buf_empty();
  if (status != -1) {
    FILE* file = fopen(out_path, "rb");
    if (file != NULL) {
      fseek(file, 0, SEEK_END);
      long len = ftell(file);
      rewind(file);
      if (len > 0) {
        uint8_t* data = (uint8_t*) malloc((size_t) len);
        if (data != NULL && fread(data, 1u, (size_t) len, file) == (size_t) len) {
          out = mira_buf_from_heap(data, (size_t) len);
        } else if (data != NULL) {
          free(data);
        }
      }
      fclose(file);
    }
  }
  unlink(req_path);
  unlink(out_path);
  return out;
#endif
}

uint64_t mira_spawn_open_handle(const char* command) {
  bool used_flags[128];
  for (size_t index = 0; index < 128; index++) {
    used_flags[index] = mira_spawn_handles[index].used;
  }
  uint64_t handle = mira_alloc_handle(used_flags, 128u);
  if (handle == 0u) {
    return 0u;
  }
  mira_spawn_handle_entry* entry = &mira_spawn_handles[handle - 1u];
  memset(entry, 0, sizeof(*entry));
  entry->used = true;
  entry->stdin_fd = -1;
#ifdef _WIN32
  char* out_path = _tempnam(NULL, "mso");
  char* err_path = _tempnam(NULL, "mse");
  if (out_path == NULL || err_path == NULL) {
    if (out_path != NULL) { free(out_path); }
    if (err_path != NULL) { free(err_path); }
    entry->used = false;
    return 0u;
  }
  strncpy(entry->stdout_path, out_path, sizeof(entry->stdout_path) - 1u);
  strncpy(entry->stderr_path, err_path, sizeof(entry->stderr_path) - 1u);
  entry->stdout_path[sizeof(entry->stdout_path) - 1u] = '\0';
  entry->stderr_path[sizeof(entry->stderr_path) - 1u] = '\0';
  free(out_path);
  free(err_path);
  char command_line[4096];
  snprintf(
      command_line,
      sizeof(command_line),
      "cmd /C \"%s 1>\\\"%s\\\" 2>\\\"%s\\\"\"",
      command,
      entry->stdout_path,
      entry->stderr_path);
  entry->exit_status = system(command_line);
  entry->waited = 1;
  return handle;
#else
  strcpy(entry->stdout_path, "/tmp/mira_spawn_stdout_XXXXXX");
  strcpy(entry->stderr_path, "/tmp/mira_spawn_stderr_XXXXXX");
  int stdout_fd = mkstemp(entry->stdout_path);
  int stderr_fd = mkstemp(entry->stderr_path);
  int stdin_pipe[2] = {-1, -1};
  if (stdout_fd < 0 || stderr_fd < 0) {
    if (stdout_fd >= 0) { close(stdout_fd); }
    if (stderr_fd >= 0) { close(stderr_fd); }
    entry->used = false;
    return 0u;
  }
  if (pipe(stdin_pipe) != 0) {
    close(stdout_fd);
    close(stderr_fd);
    entry->used = false;
    return 0u;
  }
  int pid = fork();
  if (pid < 0) {
    close(stdout_fd);
    close(stderr_fd);
    close(stdin_pipe[0]);
    close(stdin_pipe[1]);
    entry->used = false;
    return 0u;
  }
  if (pid == 0) {
    dup2(stdin_pipe[0], STDIN_FILENO);
    dup2(stdout_fd, STDOUT_FILENO);
    dup2(stderr_fd, STDERR_FILENO);
    close(stdin_pipe[0]);
    close(stdin_pipe[1]);
    close(stdout_fd);
    close(stderr_fd);
    execl("/bin/sh", "sh", "-c", command, (char*) NULL);
    _exit(127);
  }
  close(stdin_pipe[0]);
  close(stdout_fd);
  close(stderr_fd);
  entry->stdin_fd = stdin_pipe[1];
  entry->pid = pid;
  entry->waited = 0;
  entry->exit_status = 0;
  return handle;
#endif
}

static void mira_spawn_ensure_waited(mira_spawn_handle_entry* entry) {
#ifndef _WIN32
  if (entry->stdin_fd >= 0) {
    close(entry->stdin_fd);
    entry->stdin_fd = -1;
  }
  if (!entry->waited) {
    int status = 0;
    if (waitpid(entry->pid, &status, 0) < 0) {
      entry->exit_status = -1;
    } else if (WIFEXITED(status)) {
      entry->exit_status = WEXITSTATUS(status);
    } else {
      entry->exit_status = status;
    }
    entry->waited = 1;
  }
#endif
}

static int mira_spawn_poll_done_entry(mira_spawn_handle_entry* entry) {
#ifdef _WIN32
  return entry->waited ? 1 : 0;
#else
  if (entry->waited) {
    return 1;
  }
  int status = 0;
  int rc = waitpid(entry->pid, &status, WNOHANG);
  if (rc == 0) {
    return 0;
  }
  if (rc < 0) {
    entry->exit_status = -1;
  } else if (WIFEXITED(status)) {
    entry->exit_status = WEXITSTATUS(status);
  } else {
    entry->exit_status = status;
  }
  entry->waited = 1;
  return 1;
#endif
}

bool mira_spawn_stdin_write_all_handle(uint64_t handle, buf_u8 value) {
#ifdef _WIN32
  (void) handle;
  (void) value;
  return 0u;
#else
  if (handle == 0u || handle > 128u || !mira_spawn_handles[handle - 1u].used) {
    return 0u;
  }
  mira_spawn_handle_entry* entry = &mira_spawn_handles[handle - 1u];
  if (entry->stdin_fd < 0) {
    return 0u;
  }
  return mira_write_all_pipe_fd(entry->stdin_fd, value.data, value.len);
#endif
}

bool mira_spawn_stdin_close_handle(uint64_t handle) {
#ifdef _WIN32
  (void) handle;
  return 0u;
#else
  if (handle == 0u || handle > 128u || !mira_spawn_handles[handle - 1u].used) {
    return 0u;
  }
  mira_spawn_handle_entry* entry = &mira_spawn_handles[handle - 1u];
  if (entry->stdin_fd >= 0) {
    close(entry->stdin_fd);
    entry->stdin_fd = -1;
    return 1u;
  }
  return 0u;
#endif
}

bool mira_spawn_done_handle(uint64_t handle) {
  if (handle == 0u || handle > 128u || !mira_spawn_handles[handle - 1u].used) {
    return 0u;
  }
  return mira_spawn_poll_done_entry(&mira_spawn_handles[handle - 1u]) ? 1u : 0u;
}

bool mira_spawn_exit_ok_handle(uint64_t handle) {
  if (mira_spawn_wait_handle(handle) == 0) {
    return 1u;
  }
  return 0u;
}

bool mira_spawn_kill_handle(uint64_t handle) {
#ifdef _WIN32
  (void) handle;
  return 0u;
#else
  if (handle == 0u || handle > 128u || !mira_spawn_handles[handle - 1u].used) {
    return 0u;
  }
  mira_spawn_handle_entry* entry = &mira_spawn_handles[handle - 1u];
  if (!entry->waited) {
    kill(entry->pid, SIGTERM);
  }
  mira_spawn_ensure_waited(entry);
  return 1u;
#endif
}

int32_t mira_spawn_wait_handle(uint64_t handle) {
  if (handle == 0u || handle > 128u || !mira_spawn_handles[handle - 1u].used) {
    return -1;
  }
  mira_spawn_handle_entry* entry = &mira_spawn_handles[handle - 1u];
  mira_spawn_ensure_waited(entry);
  return entry->exit_status;
}

buf_u8 mira_spawn_stdout_all_handle_buf_u8(uint64_t handle) {
  if (handle == 0u || handle > 128u || !mira_spawn_handles[handle - 1u].used) {
    return mira_buf_empty();
  }
  mira_spawn_handle_entry* entry = &mira_spawn_handles[handle - 1u];
  mira_spawn_ensure_waited(entry);
  return mira_read_file_all_bytes(entry->stdout_path);
}

buf_u8 mira_spawn_stderr_all_handle_buf_u8(uint64_t handle) {
  if (handle == 0u || handle > 128u || !mira_spawn_handles[handle - 1u].used) {
    return mira_buf_empty();
  }
  mira_spawn_handle_entry* entry = &mira_spawn_handles[handle - 1u];
  mira_spawn_ensure_waited(entry);
  return mira_read_file_all_bytes(entry->stderr_path);
}

bool mira_spawn_close_handle(uint64_t handle) {
  if (handle == 0u || handle > 128u || !mira_spawn_handles[handle - 1u].used) {
    return 0u;
  }
  mira_spawn_handle_entry* entry = &mira_spawn_handles[handle - 1u];
  mira_spawn_ensure_waited(entry);
  if (entry->stdin_fd >= 0) {
    close(entry->stdin_fd);
    entry->stdin_fd = -1;
  }
  if (entry->stdout_path[0] != '\0') {
    remove(entry->stdout_path);
  }
  if (entry->stderr_path[0] != '\0') {
    remove(entry->stderr_path);
  }
  memset(entry, 0, sizeof(*entry));
  return 1u;
}

#ifdef _WIN32
bool mira_rt_cancelled(void) { return 0u; }
uint64_t mira_rt_open_handle(uint32_t workers) { (void) workers; return 0u; }
uint64_t mira_rt_spawn_u32_handle(uint64_t runtime_handle, const char* function_name, uint32_t arg) {
  (void) runtime_handle; (void) function_name; (void) arg; return 0u;
}
uint64_t mira_rt_try_spawn_u32_handle(uint64_t runtime_handle, const char* function_name, uint32_t arg) {
  (void) runtime_handle; (void) function_name; (void) arg; return 0u;
}
uint64_t mira_rt_spawn_buf_handle(uint64_t runtime_handle, const char* function_name, buf_u8 arg) {
  (void) runtime_handle; (void) function_name; (void) arg; return 0u;
}
uint64_t mira_rt_try_spawn_buf_handle(uint64_t runtime_handle, const char* function_name, buf_u8 arg) {
  (void) runtime_handle; (void) function_name; (void) arg; return 0u;
}
bool mira_rt_done_handle(uint64_t handle) { (void) handle; return 0u; }
uint32_t mira_rt_join_u32_handle(uint64_t handle) { (void) handle; return 0u; }
buf_u8 mira_rt_join_buf_handle(uint64_t handle) { (void) handle; return (buf_u8){0}; }
bool mira_rt_cancel_handle(uint64_t handle) { (void) handle; return 0u; }
bool mira_rt_task_close_handle(uint64_t handle) { (void) handle; return 0u; }
bool mira_rt_shutdown_handle(uint64_t runtime_handle, uint32_t grace_ms) {
  (void) runtime_handle; (void) grace_ms; return 0u;
}
bool mira_rt_close_handle(uint64_t runtime_handle) { (void) runtime_handle; return 0u; }
uint32_t mira_rt_inflight_handle(uint64_t runtime_handle) { (void) runtime_handle; return 0u; }
uint64_t mira_chan_open_u32_handle(uint32_t capacity) { (void) capacity; return 0u; }
uint64_t mira_chan_open_buf_handle(uint32_t capacity) { (void) capacity; return 0u; }
bool mira_chan_send_u32_handle(uint64_t channel_handle, uint32_t value) {
  (void) channel_handle; (void) value; return 0u;
}
bool mira_chan_send_buf_handle(uint64_t channel_handle, buf_u8 value) {
  (void) channel_handle; (void) value; return 0u;
}
uint32_t mira_chan_recv_u32_handle(uint64_t channel_handle) { (void) channel_handle; return 0u; }
buf_u8 mira_chan_recv_buf_handle(uint64_t channel_handle) { (void) channel_handle; return (buf_u8){0}; }
uint32_t mira_chan_len_handle(uint64_t channel_handle) { (void) channel_handle; return 0u; }
bool mira_chan_close_handle(uint64_t channel_handle) { (void) channel_handle; return 0u; }
uint64_t mira_deadline_open_ms_handle(uint32_t timeout_ms) { (void) timeout_ms; return 0u; }
bool mira_deadline_expired_handle(uint64_t deadline_handle) { (void) deadline_handle; return 0u; }
uint32_t mira_deadline_remaining_ms_handle(uint64_t deadline_handle) { (void) deadline_handle; return 0u; }
bool mira_deadline_close_handle(uint64_t deadline_handle) { (void) deadline_handle; return 0u; }
uint64_t mira_cancel_scope_open_handle(void) { return 0u; }
uint64_t mira_cancel_scope_child_handle(uint64_t parent_scope) { (void) parent_scope; return 0u; }
bool mira_cancel_scope_bind_task_handle(uint64_t scope_handle, uint64_t task_handle) { (void) scope_handle; (void) task_handle; return 0u; }
bool mira_cancel_scope_cancel_handle(uint64_t scope_handle) { (void) scope_handle; return 0u; }
bool mira_cancel_scope_cancelled_handle(uint64_t scope_handle) { (void) scope_handle; return 0u; }
bool mira_cancel_scope_close_handle(uint64_t scope_handle) { (void) scope_handle; return 0u; }
uint64_t mira_retry_open_handle(uint32_t max_attempts, uint32_t base_backoff_ms) { (void) max_attempts; (void) base_backoff_ms; return 0u; }
bool mira_retry_record_failure_handle(uint64_t retry_handle) { (void) retry_handle; return 0u; }
bool mira_retry_record_success_handle(uint64_t retry_handle) { (void) retry_handle; return 0u; }
uint32_t mira_retry_next_delay_ms_handle(uint64_t retry_handle) { (void) retry_handle; return 0u; }
bool mira_retry_exhausted_handle(uint64_t retry_handle) { (void) retry_handle; return 0u; }
bool mira_retry_close_handle(uint64_t retry_handle) { (void) retry_handle; return 0u; }
uint64_t mira_circuit_open_handle(uint32_t threshold, uint32_t cooldown_ms) { (void) threshold; (void) cooldown_ms; return 0u; }
bool mira_circuit_allow_handle(uint64_t circuit_handle) { (void) circuit_handle; return 0u; }
bool mira_circuit_record_failure_handle(uint64_t circuit_handle) { (void) circuit_handle; return 0u; }
bool mira_circuit_record_success_handle(uint64_t circuit_handle) { (void) circuit_handle; return 0u; }
uint32_t mira_circuit_state_handle(uint64_t circuit_handle) { (void) circuit_handle; return 0u; }
bool mira_circuit_close_handle(uint64_t circuit_handle) { (void) circuit_handle; return 0u; }
uint64_t mira_backpressure_open_handle(uint32_t limit) { (void) limit; return 0u; }
bool mira_backpressure_acquire_handle(uint64_t backpressure_handle) { (void) backpressure_handle; return 0u; }
bool mira_backpressure_release_handle(uint64_t backpressure_handle) { (void) backpressure_handle; return 0u; }
bool mira_backpressure_saturated_handle(uint64_t backpressure_handle) { (void) backpressure_handle; return 0u; }
bool mira_backpressure_close_handle(uint64_t backpressure_handle) { (void) backpressure_handle; return 0u; }
uint64_t mira_supervisor_open_handle(uint32_t restart_budget, uint32_t degrade_after) { (void) restart_budget; (void) degrade_after; return 0u; }
bool mira_supervisor_record_failure_handle(uint64_t supervisor_handle, uint32_t code) { (void) supervisor_handle; (void) code; return 0u; }
bool mira_supervisor_record_recovery_handle(uint64_t supervisor_handle) { (void) supervisor_handle; return 0u; }
bool mira_supervisor_should_restart_handle(uint64_t supervisor_handle) { (void) supervisor_handle; return 0u; }
bool mira_supervisor_degraded_handle(uint64_t supervisor_handle) { (void) supervisor_handle; return 0u; }
bool mira_supervisor_close_handle(uint64_t supervisor_handle) { (void) supervisor_handle; return 0u; }
#else
static __thread volatile int* mira_rt_current_cancel = NULL;

typedef struct {
  uint64_t handle;
} mira_rt_thread_arg;

bool mira_rt_cancelled(void) {
  return mira_rt_current_cancel != NULL && *mira_rt_current_cancel != 0;
}

uint64_t mira_rt_open_handle(uint32_t workers) {
  bool used_flags[32];
  for (size_t index = 0; index < 32u; index++) {
    used_flags[index] = mira_rt_schedulers[index].used;
  }
  uint64_t handle = mira_alloc_handle(used_flags, 32u);
  if (handle == 0u) {
    return 0u;
  }
  mira_rt_scheduler_entry* entry = &mira_rt_schedulers[handle - 1u];
  memset(entry, 0, sizeof(*entry));
  entry->used = true;
  entry->max_workers = workers == 0u ? 1u : workers;
  pthread_mutex_init(&entry->mutex, NULL);
  pthread_cond_init(&entry->cond, NULL);
  return handle;
}

static void* mira_rt_thread_main(void* raw_arg) {
  mira_rt_thread_arg* arg = (mira_rt_thread_arg*) raw_arg;
  uint64_t handle = arg->handle;
  free(arg);
  if (handle == 0u || handle > 256u || !mira_rt_tasks[handle - 1u].used) {
    return NULL;
  }
  mira_rt_task_entry* task = &mira_rt_tasks[handle - 1u];
  if (task->runtime_handle == 0u || task->runtime_handle > 32u ||
      !mira_rt_schedulers[task->runtime_handle - 1u].used) {
    return NULL;
  }
  mira_rt_scheduler_entry* runtime = &mira_rt_schedulers[task->runtime_handle - 1u];
  pthread_mutex_lock(&runtime->mutex);
  while (!runtime->shutting_down && runtime->active_workers >= runtime->max_workers) {
    pthread_cond_wait(&runtime->cond, &runtime->mutex);
  }
  runtime->active_workers += 1u;
  pthread_mutex_unlock(&runtime->mutex);

  mira_rt_current_cancel = &task->cancelled;
  if (task->kind == 2u) {
    buf_u8 result_buf = mira_rt_dispatch_buf(task->function_name, task->arg_buf);
    pthread_mutex_lock(&task->mutex);
    task->result_buf = result_buf;
    task->done = 1;
    pthread_cond_broadcast(&task->cond);
    pthread_mutex_unlock(&task->mutex);
  } else {
    uint32_t result = mira_rt_dispatch_u32(task->function_name, task->arg);
    pthread_mutex_lock(&task->mutex);
    task->result = result;
    task->done = 1;
    pthread_cond_broadcast(&task->cond);
    pthread_mutex_unlock(&task->mutex);
  }
  mira_rt_current_cancel = NULL;

  pthread_mutex_lock(&runtime->mutex);
  if (runtime->active_workers > 0u) {
    runtime->active_workers -= 1u;
  }
  pthread_cond_broadcast(&runtime->cond);
  pthread_mutex_unlock(&runtime->mutex);
  return NULL;
}

static uint64_t mira_rt_spawn_task_handle(uint64_t runtime_handle, const char* function_name, uint32_t kind, uint32_t arg, buf_u8 arg_buf) {
  if (runtime_handle == 0u || runtime_handle > 32u || !mira_rt_schedulers[runtime_handle - 1u].used) {
    return 0u;
  }
  bool used_flags[256];
  for (size_t index = 0; index < 256u; index++) {
    used_flags[index] = mira_rt_tasks[index].used;
  }
  uint64_t handle = mira_alloc_handle(used_flags, 256u);
  if (handle == 0u) {
    return 0u;
  }
  mira_rt_task_entry* task = &mira_rt_tasks[handle - 1u];
  memset(task, 0, sizeof(*task));
  task->used = true;
  task->runtime_handle = runtime_handle;
  task->kind = kind;
  strncpy(task->function_name, function_name, sizeof(task->function_name) - 1u);
  task->function_name[sizeof(task->function_name) - 1u] = '\0';
  task->arg = arg;
  task->arg_buf = arg_buf;
  pthread_mutex_init(&task->mutex, NULL);
  pthread_cond_init(&task->cond, NULL);
  mira_rt_thread_arg* thread_arg = (mira_rt_thread_arg*) malloc(sizeof(mira_rt_thread_arg));
  if (thread_arg == NULL) {
    memset(task, 0, sizeof(*task));
    return 0u;
  }
  thread_arg->handle = handle;
  if (pthread_create(&task->thread, NULL, mira_rt_thread_main, thread_arg) != 0) {
    free(thread_arg);
    memset(task, 0, sizeof(*task));
    return 0u;
  }
  return handle;
}

uint64_t mira_rt_spawn_u32_handle(uint64_t runtime_handle, const char* function_name, uint32_t arg) {
  return mira_rt_spawn_task_handle(runtime_handle, function_name, 1u, arg, mira_buf_empty());
}

uint64_t mira_rt_try_spawn_u32_handle(uint64_t runtime_handle, const char* function_name, uint32_t arg) {
  return mira_rt_spawn_u32_handle(runtime_handle, function_name, arg);
}

uint64_t mira_rt_spawn_buf_handle(uint64_t runtime_handle, const char* function_name, buf_u8 arg) {
  return mira_rt_spawn_task_handle(
      runtime_handle,
      function_name,
      2u,
      0u,
      mira_buf_copy_bytes(arg.data, arg.len));
}

uint64_t mira_rt_try_spawn_buf_handle(uint64_t runtime_handle, const char* function_name, buf_u8 arg) {
  return mira_rt_spawn_buf_handle(runtime_handle, function_name, arg);
}

bool mira_rt_done_handle(uint64_t handle) {
  if (handle == 0u || handle > 256u || !mira_rt_tasks[handle - 1u].used) {
    return 0u;
  }
  return mira_rt_tasks[handle - 1u].done ? 1u : 0u;
}

uint32_t mira_rt_join_u32_handle(uint64_t handle) {
  if (handle == 0u || handle > 256u || !mira_rt_tasks[handle - 1u].used) {
    return 0u;
  }
  mira_rt_task_entry* task = &mira_rt_tasks[handle - 1u];
  pthread_mutex_lock(&task->mutex);
  while (!task->done) {
    pthread_cond_wait(&task->cond, &task->mutex);
  }
  uint32_t result = task->result;
  pthread_mutex_unlock(&task->mutex);
  if (!task->joined) {
    pthread_join(task->thread, NULL);
    task->joined = 1;
  }
  return result;
}

buf_u8 mira_rt_join_buf_handle(uint64_t handle) {
  if (handle == 0u || handle > 256u || !mira_rt_tasks[handle - 1u].used) {
    return mira_buf_empty();
  }
  mira_rt_task_entry* task = &mira_rt_tasks[handle - 1u];
  pthread_mutex_lock(&task->mutex);
  while (!task->done) {
    pthread_cond_wait(&task->cond, &task->mutex);
  }
  buf_u8 result = task->result_buf;
  pthread_mutex_unlock(&task->mutex);
  if (!task->joined) {
    pthread_join(task->thread, NULL);
    task->joined = 1;
  }
  return result;
}

bool mira_rt_cancel_handle(uint64_t handle) {
  if (handle == 0u || handle > 256u || !mira_rt_tasks[handle - 1u].used) {
    return 0u;
  }
  mira_rt_tasks[handle - 1u].cancelled = 1;
  return 1u;
}

bool mira_rt_task_close_handle(uint64_t handle) {
  if (handle == 0u || handle > 256u || !mira_rt_tasks[handle - 1u].used) {
    return 0u;
  }
  mira_rt_task_entry* task = &mira_rt_tasks[handle - 1u];
  if (!task->joined) {
    pthread_join(task->thread, NULL);
    task->joined = 1;
  }
  pthread_mutex_destroy(&task->mutex);
  pthread_cond_destroy(&task->cond);
  memset(task, 0, sizeof(*task));
  return 1u;
}

bool mira_rt_shutdown_handle(uint64_t runtime_handle, uint32_t grace_ms) {
  if (runtime_handle == 0u || runtime_handle > 32u || !mira_rt_schedulers[runtime_handle - 1u].used) {
    return 0u;
  }
  mira_rt_scheduler_entry* runtime = &mira_rt_schedulers[runtime_handle - 1u];
  runtime->shutting_down = 1;
  uint64_t start = mira_rt_clock_now_ns();
  for (;;) {
    pthread_mutex_lock(&runtime->mutex);
    uint32_t active = runtime->active_workers;
    pthread_cond_broadcast(&runtime->cond);
    pthread_mutex_unlock(&runtime->mutex);
    if (active == 0u) {
      return 1u;
    }
    if (((mira_rt_clock_now_ns() - start) / 1000000ull) >= (uint64_t) grace_ms) {
      return 0u;
    }
    usleep(1000);
  }
}

bool mira_rt_close_handle(uint64_t runtime_handle) {
  if (runtime_handle == 0u || runtime_handle > 32u || !mira_rt_schedulers[runtime_handle - 1u].used) {
    return 0u;
  }
  mira_rt_scheduler_entry* runtime = &mira_rt_schedulers[runtime_handle - 1u];
  pthread_mutex_destroy(&runtime->mutex);
  pthread_cond_destroy(&runtime->cond);
  memset(runtime, 0, sizeof(*runtime));
  return 1u;
}

uint32_t mira_rt_inflight_handle(uint64_t runtime_handle) {
  if (runtime_handle == 0u || runtime_handle > 32u || !mira_rt_schedulers[runtime_handle - 1u].used) {
    return 0u;
  }
  uint32_t count = 0u;
  for (size_t index = 0; index < 256u; index++) {
    if (mira_rt_tasks[index].used
        && mira_rt_tasks[index].runtime_handle == runtime_handle
        && !mira_rt_tasks[index].done) {
      count += 1u;
    }
  }
  return count;
}

uint64_t mira_chan_open_u32_handle(uint32_t capacity) {
  bool used_flags[64];
  for (size_t index = 0; index < 64u; index++) {
    used_flags[index] = mira_chan_u32[index].used;
  }
  uint64_t handle = mira_alloc_handle(used_flags, 64u);
  if (handle == 0u) {
    return 0u;
  }
  mira_chan_u32_entry* channel = &mira_chan_u32[handle - 1u];
  memset(channel, 0, sizeof(*channel));
  channel->used = true;
  channel->capacity = capacity == 0u ? 1u : capacity;
  channel->values = (uint32_t*) calloc(channel->capacity, sizeof(uint32_t));
  if (channel->values == NULL) {
    memset(channel, 0, sizeof(*channel));
    return 0u;
  }
  pthread_mutex_init(&channel->mutex, NULL);
  pthread_cond_init(&channel->not_empty, NULL);
  pthread_cond_init(&channel->not_full, NULL);
  return handle;
}

bool mira_chan_send_u32_handle(uint64_t channel_handle, uint32_t value) {
  if (channel_handle == 0u || channel_handle > 64u || !mira_chan_u32[channel_handle - 1u].used) {
    return 0u;
  }
  mira_chan_u32_entry* channel = &mira_chan_u32[channel_handle - 1u];
  pthread_mutex_lock(&channel->mutex);
  while (!channel->closed && channel->len >= channel->capacity) {
    pthread_cond_wait(&channel->not_full, &channel->mutex);
  }
  if (channel->closed) {
    pthread_mutex_unlock(&channel->mutex);
    return 0u;
  }
  channel->values[channel->tail] = value;
  channel->tail = (channel->tail + 1u) % channel->capacity;
  channel->len += 1u;
  pthread_cond_signal(&channel->not_empty);
  pthread_mutex_unlock(&channel->mutex);
  return 1u;
}

uint32_t mira_chan_recv_u32_handle(uint64_t channel_handle) {
  if (channel_handle == 0u || channel_handle > 64u || !mira_chan_u32[channel_handle - 1u].used) {
    return 0u;
  }
  mira_chan_u32_entry* channel = &mira_chan_u32[channel_handle - 1u];
  pthread_mutex_lock(&channel->mutex);
  while (channel->len == 0u && !channel->closed) {
    pthread_cond_wait(&channel->not_empty, &channel->mutex);
  }
  if (channel->len == 0u) {
    pthread_mutex_unlock(&channel->mutex);
    return 0u;
  }
  uint32_t value = channel->values[channel->head];
  channel->head = (channel->head + 1u) % channel->capacity;
  channel->len -= 1u;
  pthread_cond_signal(&channel->not_full);
  pthread_mutex_unlock(&channel->mutex);
  return value;
}

bool mira_chan_close_handle(uint64_t channel_handle) {
  if (channel_handle == 0u || channel_handle > 64u) {
    return 0u;
  }
  if (mira_chan_u32[channel_handle - 1u].used) {
    mira_chan_u32_entry* channel = &mira_chan_u32[channel_handle - 1u];
    pthread_mutex_lock(&channel->mutex);
    channel->closed = 1;
    pthread_cond_broadcast(&channel->not_empty);
    pthread_cond_broadcast(&channel->not_full);
    pthread_mutex_unlock(&channel->mutex);
    pthread_mutex_destroy(&channel->mutex);
    pthread_cond_destroy(&channel->not_empty);
    pthread_cond_destroy(&channel->not_full);
    free(channel->values);
    memset(channel, 0, sizeof(*channel));
    return 1u;
  }
  if (mira_chan_buf[channel_handle - 1u].used) {
    mira_chan_buf_entry* channel = &mira_chan_buf[channel_handle - 1u];
    pthread_mutex_lock(&channel->mutex);
    channel->closed = 1;
    pthread_cond_broadcast(&channel->not_empty);
    pthread_cond_broadcast(&channel->not_full);
    pthread_mutex_unlock(&channel->mutex);
    pthread_mutex_destroy(&channel->mutex);
    pthread_cond_destroy(&channel->not_empty);
    pthread_cond_destroy(&channel->not_full);
    for (uint32_t index = 0u; index < channel->capacity; index++) {
      if (channel->values[index].data != NULL) {
        free(channel->values[index].data);
      }
    }
    free(channel->values);
    memset(channel, 0, sizeof(*channel));
    return 1u;
  }
  return 0u;
}

uint64_t mira_chan_open_buf_handle(uint32_t capacity) {
  bool used_flags[64];
  for (size_t index = 0; index < 64u; index++) {
    used_flags[index] = mira_chan_buf[index].used;
  }
  uint64_t handle = mira_alloc_handle(used_flags, 64u);
  if (handle == 0u) {
    return 0u;
  }
  mira_chan_buf_entry* channel = &mira_chan_buf[handle - 1u];
  memset(channel, 0, sizeof(*channel));
  channel->used = true;
  channel->capacity = capacity == 0u ? 1u : capacity;
  channel->values = (buf_u8*) calloc(channel->capacity, sizeof(buf_u8));
  if (channel->values == NULL) {
    memset(channel, 0, sizeof(*channel));
    return 0u;
  }
  pthread_mutex_init(&channel->mutex, NULL);
  pthread_cond_init(&channel->not_empty, NULL);
  pthread_cond_init(&channel->not_full, NULL);
  return handle;
}

bool mira_chan_send_buf_handle(uint64_t channel_handle, buf_u8 value) {
  if (channel_handle == 0u || channel_handle > 64u || !mira_chan_buf[channel_handle - 1u].used) {
    return 0u;
  }
  mira_chan_buf_entry* channel = &mira_chan_buf[channel_handle - 1u];
  pthread_mutex_lock(&channel->mutex);
  while (!channel->closed && channel->len >= channel->capacity) {
    pthread_cond_wait(&channel->not_full, &channel->mutex);
  }
  if (channel->closed) {
    pthread_mutex_unlock(&channel->mutex);
    return 0u;
  }
  channel->values[channel->tail] = mira_buf_copy_bytes(value.data, value.len);
  channel->tail = (channel->tail + 1u) % channel->capacity;
  channel->len += 1u;
  pthread_cond_signal(&channel->not_empty);
  pthread_mutex_unlock(&channel->mutex);
  return 1u;
}

buf_u8 mira_chan_recv_buf_handle(uint64_t channel_handle) {
  if (channel_handle == 0u || channel_handle > 64u || !mira_chan_buf[channel_handle - 1u].used) {
    return mira_buf_empty();
  }
  mira_chan_buf_entry* channel = &mira_chan_buf[channel_handle - 1u];
  pthread_mutex_lock(&channel->mutex);
  while (channel->len == 0u && !channel->closed) {
    pthread_cond_wait(&channel->not_empty, &channel->mutex);
  }
  if (channel->len == 0u) {
    pthread_mutex_unlock(&channel->mutex);
    return mira_buf_empty();
  }
  buf_u8 value = channel->values[channel->head];
  channel->values[channel->head] = mira_buf_empty();
  channel->head = (channel->head + 1u) % channel->capacity;
  channel->len -= 1u;
  pthread_cond_signal(&channel->not_full);
  pthread_mutex_unlock(&channel->mutex);
  return value;
}

uint32_t mira_chan_len_handle(uint64_t channel_handle) {
  if (channel_handle == 0u || channel_handle > 64u) {
    return 0u;
  }
  if (mira_chan_u32[channel_handle - 1u].used) {
    return mira_chan_u32[channel_handle - 1u].len;
  }
  if (mira_chan_buf[channel_handle - 1u].used) {
    return mira_chan_buf[channel_handle - 1u].len;
  }
  return 0u;
}

uint64_t mira_msg_log_open_handle(void) {
  bool used_flags[64];
  for (size_t index = 0u; index < 64u; index++) {
    used_flags[index] = mira_msg_logs[index].used;
  }
  uint64_t handle = mira_alloc_handle(used_flags, 64u);
  if (handle == 0u) {
    return 0u;
  }
  memset(&mira_msg_logs[handle - 1u], 0, sizeof(mira_msg_log_entry));
  mira_msg_logs[handle - 1u].used = true;
  mira_msg_logs[handle - 1u].next_seq = 1u;
  return handle;
}

bool mira_msg_log_close_handle(uint64_t handle) {
  if (handle == 0u || handle > 64u || !mira_msg_logs[handle - 1u].used) {
    return 0u;
  }
  mira_msg_log_entry* log = &mira_msg_logs[handle - 1u];
  for (size_t index = 0u; index < 512u; index++) {
    if (log->deliveries[index].used && log->deliveries[index].payload.data != NULL) {
      free(log->deliveries[index].payload.data);
    }
  }
  memset(log, 0, sizeof(*log));
  for (size_t index = 0u; index < 128u; index++) {
    if (mira_msg_replays[index].used && mira_msg_replays[index].log_handle == handle) {
      memset(&mira_msg_replays[index], 0, sizeof(mira_msg_replay_entry));
    }
  }
  return 1u;
}

static uint32_t mira_msg_append_delivery(mira_msg_log_entry* log, const char* conversation, const char* recipient, buf_u8 payload) {
  if (log == NULL) {
    return 0u;
  }
  for (size_t index = 0u; index < 512u; index++) {
    if (!log->deliveries[index].used) {
      uint32_t seq = log->next_seq == 0u ? 1u : log->next_seq;
      log->next_seq = seq + 1u;
      memset(&log->deliveries[index], 0, sizeof(mira_msg_delivery_entry));
      log->deliveries[index].used = true;
      log->deliveries[index].seq = seq;
      mira_copy_token(log->deliveries[index].conversation, sizeof(log->deliveries[index].conversation), conversation);
      mira_copy_token(log->deliveries[index].recipient, sizeof(log->deliveries[index].recipient), recipient);
      log->deliveries[index].payload = mira_buf_copy_bytes(payload.data, payload.len);
      log->deliveries[index].acked = 0;
      log->deliveries[index].retry_count = 0u;
      log->last_failure_class = 0u;
      return seq;
    }
  }
  log->last_failure_class = 4u;
  return 0u;
}

uint32_t mira_msg_send_handle_buf_u8(uint64_t handle, const char* conversation, const char* recipient, buf_u8 payload) {
  if (handle == 0u || handle > 64u || !mira_msg_logs[handle - 1u].used) {
    return 0u;
  }
  return mira_msg_append_delivery(&mira_msg_logs[handle - 1u], conversation, recipient, payload);
}

uint32_t mira_msg_send_dedup_handle_buf_u8(uint64_t handle, const char* conversation, const char* recipient, buf_u8 dedup_key, buf_u8 payload) {
  if (handle == 0u || handle > 64u || !mira_msg_logs[handle - 1u].used) {
    return 0u;
  }
  mira_msg_log_entry* log = &mira_msg_logs[handle - 1u];
  char key[256];
  mira_msg_make_dedup_key(key, sizeof(key), conversation, recipient, dedup_key);
  for (size_t index = 0u; index < 256u; index++) {
    if (log->dedup[index].used && strcmp(log->dedup[index].key, key) == 0) {
      log->last_failure_class = 3u;
      return log->dedup[index].seq;
    }
  }
  uint32_t seq = mira_msg_append_delivery(log, conversation, recipient, payload);
  if (seq == 0u) {
    return 0u;
  }
  for (size_t index = 0u; index < 256u; index++) {
    if (!log->dedup[index].used) {
      log->dedup[index].used = true;
      mira_copy_token(log->dedup[index].key, sizeof(log->dedup[index].key), key);
      log->dedup[index].seq = seq;
      break;
    }
  }
  return seq;
}

bool mira_msg_subscribe_handle(uint64_t handle, const char* room, const char* recipient) {
  if (handle == 0u || handle > 64u || !mira_msg_logs[handle - 1u].used) {
    return 0u;
  }
  mira_msg_log_entry* log = &mira_msg_logs[handle - 1u];
  for (size_t index = 0u; index < 256u; index++) {
    if (log->subscriptions[index].used
        && strcmp(log->subscriptions[index].room, room) == 0
        && strcmp(log->subscriptions[index].recipient, recipient) == 0) {
      log->last_failure_class = 0u;
      return 1u;
    }
  }
  for (size_t index = 0u; index < 256u; index++) {
    if (!log->subscriptions[index].used) {
      log->subscriptions[index].used = true;
      mira_copy_token(log->subscriptions[index].room, sizeof(log->subscriptions[index].room), room);
      mira_copy_token(log->subscriptions[index].recipient, sizeof(log->subscriptions[index].recipient), recipient);
      log->last_failure_class = 0u;
      return 1u;
    }
  }
  log->last_failure_class = 4u;
  return 0u;
}

uint32_t mira_msg_subscriber_count_handle(uint64_t handle, const char* room) {
  if (handle == 0u || handle > 64u || !mira_msg_logs[handle - 1u].used) {
    return 0u;
  }
  uint32_t count = 0u;
  mira_msg_log_entry* log = &mira_msg_logs[handle - 1u];
  for (size_t index = 0u; index < 256u; index++) {
    if (log->subscriptions[index].used && strcmp(log->subscriptions[index].room, room) == 0) {
      count += 1u;
    }
  }
  return count;
}

uint32_t mira_msg_fanout_handle_buf_u8(uint64_t handle, const char* room, buf_u8 payload) {
  if (handle == 0u || handle > 64u || !mira_msg_logs[handle - 1u].used) {
    return 0u;
  }
  mira_msg_log_entry* log = &mira_msg_logs[handle - 1u];
  uint32_t first_seq = 0u;
  int found = 0;
  for (size_t index = 0u; index < 256u; index++) {
    if (log->subscriptions[index].used && strcmp(log->subscriptions[index].room, room) == 0) {
      uint32_t seq = mira_msg_append_delivery(log, room, log->subscriptions[index].recipient, payload);
      if (first_seq == 0u) {
        first_seq = seq;
      }
      found = 1;
    }
  }
  if (!found) {
    log->last_failure_class = 1u;
    return 0u;
  }
  return first_seq;
}

buf_u8 mira_msg_recv_next_handle(uint64_t handle, const char* recipient) {
  if (handle == 0u || handle > 64u || !mira_msg_logs[handle - 1u].used) {
    return mira_buf_empty();
  }
  mira_msg_log_entry* log = &mira_msg_logs[handle - 1u];
  mira_msg_delivery_entry* best = NULL;
  for (size_t index = 0u; index < 512u; index++) {
    mira_msg_delivery_entry* delivery = &log->deliveries[index];
    if (!delivery->used || delivery->acked || strcmp(delivery->recipient, recipient) != 0) {
      continue;
    }
    if (best == NULL || delivery->seq < best->seq) {
      best = delivery;
    }
  }
  if (best == NULL) {
    return mira_buf_empty();
  }
  return mira_buf_copy_bytes(best->payload.data, best->payload.len);
}

uint32_t mira_msg_recv_seq_handle(uint64_t handle, const char* recipient) {
  if (handle == 0u || handle > 64u || !mira_msg_logs[handle - 1u].used) {
    return 0u;
  }
  mira_msg_log_entry* log = &mira_msg_logs[handle - 1u];
  uint32_t best = 0u;
  for (size_t index = 0u; index < 512u; index++) {
    mira_msg_delivery_entry* delivery = &log->deliveries[index];
    if (!delivery->used || delivery->acked || strcmp(delivery->recipient, recipient) != 0) {
      continue;
    }
    if (best == 0u || delivery->seq < best) {
      best = delivery->seq;
    }
  }
  return best;
}

bool mira_msg_ack_handle(uint64_t handle, const char* recipient, uint32_t seq) {
  if (handle == 0u || handle > 64u || !mira_msg_logs[handle - 1u].used) {
    return 0u;
  }
  mira_msg_log_entry* log = &mira_msg_logs[handle - 1u];
  for (size_t index = 0u; index < 512u; index++) {
    mira_msg_delivery_entry* delivery = &log->deliveries[index];
    if (delivery->used && !delivery->acked && delivery->seq == seq && strcmp(delivery->recipient, recipient) == 0) {
      delivery->acked = 1;
      log->last_failure_class = 0u;
      return 1u;
    }
  }
  log->last_failure_class = 2u;
  return 0u;
}

bool mira_msg_mark_retry_handle(uint64_t handle, const char* recipient, uint32_t seq) {
  if (handle == 0u || handle > 64u || !mira_msg_logs[handle - 1u].used) {
    return 0u;
  }
  mira_msg_log_entry* log = &mira_msg_logs[handle - 1u];
  for (size_t index = 0u; index < 512u; index++) {
    mira_msg_delivery_entry* delivery = &log->deliveries[index];
    if (delivery->used && !delivery->acked && delivery->seq == seq && strcmp(delivery->recipient, recipient) == 0) {
      delivery->retry_count += 1u;
      log->last_failure_class = 4u;
      return 1u;
    }
  }
  log->last_failure_class = 2u;
  return 0u;
}

uint32_t mira_msg_retry_count_handle(uint64_t handle, const char* recipient, uint32_t seq) {
  if (handle == 0u || handle > 64u || !mira_msg_logs[handle - 1u].used) {
    return 0u;
  }
  mira_msg_log_entry* log = &mira_msg_logs[handle - 1u];
  for (size_t index = 0u; index < 512u; index++) {
    mira_msg_delivery_entry* delivery = &log->deliveries[index];
    if (delivery->used && delivery->seq == seq && strcmp(delivery->recipient, recipient) == 0) {
      return delivery->retry_count;
    }
  }
  return 0u;
}

uint32_t mira_msg_pending_count_handle(uint64_t handle, const char* recipient) {
  if (handle == 0u || handle > 64u || !mira_msg_logs[handle - 1u].used) {
    return 0u;
  }
  uint32_t count = 0u;
  mira_msg_log_entry* log = &mira_msg_logs[handle - 1u];
  for (size_t index = 0u; index < 512u; index++) {
    mira_msg_delivery_entry* delivery = &log->deliveries[index];
    if (delivery->used && !delivery->acked && strcmp(delivery->recipient, recipient) == 0) {
      count += 1u;
    }
  }
  return count;
}

uint32_t mira_msg_delivery_total_handle(uint64_t handle, const char* recipient) {
  if (handle == 0u || handle > 64u || !mira_msg_logs[handle - 1u].used) {
    return 0u;
  }
  uint32_t count = 0u;
  mira_msg_log_entry* log = &mira_msg_logs[handle - 1u];
  for (size_t index = 0u; index < 512u; index++) {
    mira_msg_delivery_entry* delivery = &log->deliveries[index];
    if (delivery->used && strcmp(delivery->recipient, recipient) == 0) {
      count += 1u;
    }
  }
  return count;
}

uint32_t mira_msg_failure_class_handle(uint64_t handle) {
  if (handle == 0u || handle > 64u || !mira_msg_logs[handle - 1u].used) {
    return 2u;
  }
  return mira_msg_logs[handle - 1u].last_failure_class;
}

uint64_t mira_msg_replay_open_handle(uint64_t handle, const char* recipient, uint32_t from_seq) {
  if (handle == 0u || handle > 64u || !mira_msg_logs[handle - 1u].used) {
    return 0u;
  }
  bool used_flags[128];
  for (size_t index = 0u; index < 128u; index++) {
    used_flags[index] = mira_msg_replays[index].used;
  }
  uint64_t replay = mira_alloc_handle(used_flags, 128u);
  if (replay == 0u) {
    return 0u;
  }
  memset(&mira_msg_replays[replay - 1u], 0, sizeof(mira_msg_replay_entry));
  mira_msg_replays[replay - 1u].used = true;
  mira_msg_replays[replay - 1u].log_handle = handle;
  mira_copy_token(mira_msg_replays[replay - 1u].recipient, sizeof(mira_msg_replays[replay - 1u].recipient), recipient);
  mira_msg_replays[replay - 1u].from_seq = from_seq;
  return replay;
}

buf_u8 mira_msg_replay_next_handle(uint64_t handle) {
  if (handle == 0u || handle > 128u || !mira_msg_replays[handle - 1u].used) {
    return mira_buf_empty();
  }
  mira_msg_replay_entry* replay = &mira_msg_replays[handle - 1u];
  if (replay->log_handle == 0u || replay->log_handle > 64u || !mira_msg_logs[replay->log_handle - 1u].used) {
    return mira_buf_empty();
  }
  mira_msg_log_entry* log = &mira_msg_logs[replay->log_handle - 1u];
  uint32_t seen = 0u;
  for (size_t index = 0u; index < 512u; index++) {
    mira_msg_delivery_entry* delivery = &log->deliveries[index];
    if (!delivery->used || strcmp(delivery->recipient, replay->recipient) != 0 || delivery->seq < replay->from_seq) {
      continue;
    }
    if (seen == replay->cursor) {
      replay->cursor += 1u;
      replay->last_seq = delivery->seq;
      return mira_buf_copy_bytes(delivery->payload.data, delivery->payload.len);
    }
    seen += 1u;
  }
  return mira_buf_empty();
}

uint32_t mira_msg_replay_seq_handle(uint64_t handle) {
  if (handle == 0u || handle > 128u || !mira_msg_replays[handle - 1u].used) {
    return 0u;
  }
  return mira_msg_replays[handle - 1u].last_seq;
}

bool mira_msg_replay_close_handle(uint64_t handle) {
  if (handle == 0u || handle > 128u || !mira_msg_replays[handle - 1u].used) {
    return 0u;
  }
  memset(&mira_msg_replays[handle - 1u], 0, sizeof(mira_msg_replay_entry));
  return 1u;
}

uint64_t mira_deadline_open_ms_handle(uint32_t timeout_ms) {
  bool used_flags[128];
  for (size_t index = 0; index < 128u; index++) {
    used_flags[index] = mira_deadlines[index].used;
  }
  uint64_t handle = mira_alloc_handle(used_flags, 128u);
  if (handle == 0u) {
    return 0u;
  }
  mira_deadline_entry* entry = &mira_deadlines[handle - 1u];
  memset(entry, 0, sizeof(*entry));
  entry->used = true;
  entry->opened_ns = mira_rt_clock_now_ns();
  entry->timeout_ms = timeout_ms;
  return handle;
}

bool mira_deadline_expired_handle(uint64_t deadline_handle) {
  if (deadline_handle == 0u || deadline_handle > 128u || !mira_deadlines[deadline_handle - 1u].used) {
    return 1u;
  }
  mira_deadline_entry* entry = &mira_deadlines[deadline_handle - 1u];
  uint64_t elapsed_ms = (mira_rt_clock_now_ns() - entry->opened_ns) / 1000000ull;
  return elapsed_ms >= (uint64_t) entry->timeout_ms;
}

uint32_t mira_deadline_remaining_ms_handle(uint64_t deadline_handle) {
  if (deadline_handle == 0u || deadline_handle > 128u || !mira_deadlines[deadline_handle - 1u].used) {
    return 0u;
  }
  mira_deadline_entry* entry = &mira_deadlines[deadline_handle - 1u];
  uint64_t elapsed_ms = (mira_rt_clock_now_ns() - entry->opened_ns) / 1000000ull;
  if (elapsed_ms >= (uint64_t) entry->timeout_ms) {
    return 0u;
  }
  return (uint32_t) ((uint64_t) entry->timeout_ms - elapsed_ms);
}

bool mira_deadline_close_handle(uint64_t deadline_handle) {
  if (deadline_handle == 0u || deadline_handle > 128u || !mira_deadlines[deadline_handle - 1u].used) {
    return 0u;
  }
  memset(&mira_deadlines[deadline_handle - 1u], 0, sizeof(mira_deadline_entry));
  return 1u;
}

static bool mira_cancel_scope_is_cancelled(uint64_t handle) {
  while (handle != 0u && handle <= 128u && mira_cancel_scopes[handle - 1u].used) {
    if (mira_cancel_scopes[handle - 1u].cancelled) {
      return true;
    }
    handle = mira_cancel_scopes[handle - 1u].parent;
  }
  return false;
}

uint64_t mira_cancel_scope_open_handle(void) {
  bool used_flags[128];
  for (size_t index = 0; index < 128u; index++) {
    used_flags[index] = mira_cancel_scopes[index].used;
  }
  uint64_t handle = mira_alloc_handle(used_flags, 128u);
  if (handle == 0u) {
    return 0u;
  }
  memset(&mira_cancel_scopes[handle - 1u], 0, sizeof(mira_cancel_scope_entry));
  mira_cancel_scopes[handle - 1u].used = true;
  return handle;
}

uint64_t mira_cancel_scope_child_handle(uint64_t parent_scope) {
  uint64_t handle = mira_cancel_scope_open_handle();
  if (handle == 0u) {
    return 0u;
  }
  mira_cancel_scopes[handle - 1u].parent = parent_scope;
  return handle;
}

bool mira_cancel_scope_bind_task_handle(uint64_t scope_handle, uint64_t task_handle) {
  if (scope_handle == 0u || scope_handle > 128u || !mira_cancel_scopes[scope_handle - 1u].used) {
    return 0u;
  }
  mira_cancel_scope_entry* scope = &mira_cancel_scopes[scope_handle - 1u];
  if (scope->bound_len >= 64u) {
    return 0u;
  }
  scope->bound_tasks[scope->bound_len++] = task_handle;
  return 1u;
}

bool mira_cancel_scope_cancel_handle(uint64_t scope_handle) {
  if (scope_handle == 0u || scope_handle > 128u || !mira_cancel_scopes[scope_handle - 1u].used) {
    return 0u;
  }
  mira_cancel_scope_entry* scope = &mira_cancel_scopes[scope_handle - 1u];
  scope->cancelled = 1;
  for (uint32_t index = 0u; index < scope->bound_len; index++) {
    mira_rt_cancel_handle(scope->bound_tasks[index]);
  }
  return 1u;
}

bool mira_cancel_scope_cancelled_handle(uint64_t scope_handle) {
  return mira_cancel_scope_is_cancelled(scope_handle) ? 1u : 0u;
}

bool mira_cancel_scope_close_handle(uint64_t scope_handle) {
  if (scope_handle == 0u || scope_handle > 128u || !mira_cancel_scopes[scope_handle - 1u].used) {
    return 0u;
  }
  memset(&mira_cancel_scopes[scope_handle - 1u], 0, sizeof(mira_cancel_scope_entry));
  return 1u;
}

uint64_t mira_retry_open_handle(uint32_t max_attempts, uint32_t base_backoff_ms) {
  bool used_flags[128];
  for (size_t index = 0; index < 128u; index++) {
    used_flags[index] = mira_retries[index].used;
  }
  uint64_t handle = mira_alloc_handle(used_flags, 128u);
  if (handle == 0u) {
    return 0u;
  }
  memset(&mira_retries[handle - 1u], 0, sizeof(mira_retry_entry));
  mira_retries[handle - 1u].used = true;
  mira_retries[handle - 1u].max_attempts = max_attempts == 0u ? 1u : max_attempts;
  mira_retries[handle - 1u].base_backoff_ms = base_backoff_ms;
  return handle;
}

bool mira_retry_record_failure_handle(uint64_t retry_handle) {
  if (retry_handle == 0u || retry_handle > 128u || !mira_retries[retry_handle - 1u].used) {
    return 0u;
  }
  mira_retry_entry* entry = &mira_retries[retry_handle - 1u];
  entry->attempts += 1u;
  entry->last_delay_ms = entry->base_backoff_ms * entry->attempts;
  return 1u;
}

bool mira_retry_record_success_handle(uint64_t retry_handle) {
  if (retry_handle == 0u || retry_handle > 128u || !mira_retries[retry_handle - 1u].used) {
    return 0u;
  }
  mira_retry_entry* entry = &mira_retries[retry_handle - 1u];
  entry->attempts = 0u;
  entry->last_delay_ms = 0u;
  return 1u;
}

uint32_t mira_retry_next_delay_ms_handle(uint64_t retry_handle) {
  if (retry_handle == 0u || retry_handle > 128u || !mira_retries[retry_handle - 1u].used) {
    return 0u;
  }
  return mira_retries[retry_handle - 1u].last_delay_ms;
}

bool mira_retry_exhausted_handle(uint64_t retry_handle) {
  if (retry_handle == 0u || retry_handle > 128u || !mira_retries[retry_handle - 1u].used) {
    return 1u;
  }
  mira_retry_entry* entry = &mira_retries[retry_handle - 1u];
  return entry->attempts >= entry->max_attempts ? 1u : 0u;
}

bool mira_retry_close_handle(uint64_t retry_handle) {
  if (retry_handle == 0u || retry_handle > 128u || !mira_retries[retry_handle - 1u].used) {
    return 0u;
  }
  memset(&mira_retries[retry_handle - 1u], 0, sizeof(mira_retry_entry));
  return 1u;
}

uint64_t mira_circuit_open_handle(uint32_t threshold, uint32_t cooldown_ms) {
  bool used_flags[128];
  for (size_t index = 0; index < 128u; index++) {
    used_flags[index] = mira_circuits[index].used;
  }
  uint64_t handle = mira_alloc_handle(used_flags, 128u);
  if (handle == 0u) {
    return 0u;
  }
  memset(&mira_circuits[handle - 1u], 0, sizeof(mira_circuit_entry));
  mira_circuits[handle - 1u].used = true;
  mira_circuits[handle - 1u].threshold = threshold == 0u ? 1u : threshold;
  mira_circuits[handle - 1u].cooldown_ms = cooldown_ms;
  return handle;
}

bool mira_circuit_allow_handle(uint64_t circuit_handle) {
  if (circuit_handle == 0u || circuit_handle > 128u || !mira_circuits[circuit_handle - 1u].used) {
    return 0u;
  }
  return mira_rt_clock_now_ns() >= mira_circuits[circuit_handle - 1u].open_until_ns ? 1u : 0u;
}

bool mira_circuit_record_failure_handle(uint64_t circuit_handle) {
  if (circuit_handle == 0u || circuit_handle > 128u || !mira_circuits[circuit_handle - 1u].used) {
    return 0u;
  }
  mira_circuit_entry* entry = &mira_circuits[circuit_handle - 1u];
  entry->failures += 1u;
  if (entry->failures >= entry->threshold) {
    entry->open_until_ns = mira_rt_clock_now_ns() + ((uint64_t) entry->cooldown_ms * 1000000ull);
  }
  return 1u;
}

bool mira_circuit_record_success_handle(uint64_t circuit_handle) {
  if (circuit_handle == 0u || circuit_handle > 128u || !mira_circuits[circuit_handle - 1u].used) {
    return 0u;
  }
  mira_circuits[circuit_handle - 1u].failures = 0u;
  mira_circuits[circuit_handle - 1u].open_until_ns = 0u;
  return 1u;
}

uint32_t mira_circuit_state_handle(uint64_t circuit_handle) {
  if (circuit_handle == 0u || circuit_handle > 128u || !mira_circuits[circuit_handle - 1u].used) {
    return 2u;
  }
  return mira_circuit_allow_handle(circuit_handle) ? 0u : 1u;
}

bool mira_circuit_close_handle(uint64_t circuit_handle) {
  if (circuit_handle == 0u || circuit_handle > 128u || !mira_circuits[circuit_handle - 1u].used) {
    return 0u;
  }
  memset(&mira_circuits[circuit_handle - 1u], 0, sizeof(mira_circuit_entry));
  return 1u;
}

uint64_t mira_backpressure_open_handle(uint32_t limit) {
  bool used_flags[128];
  for (size_t index = 0; index < 128u; index++) {
    used_flags[index] = mira_backpressure[index].used;
  }
  uint64_t handle = mira_alloc_handle(used_flags, 128u);
  if (handle == 0u) {
    return 0u;
  }
  memset(&mira_backpressure[handle - 1u], 0, sizeof(mira_backpressure_entry));
  mira_backpressure[handle - 1u].used = true;
  mira_backpressure[handle - 1u].limit = limit == 0u ? 1u : limit;
  return handle;
}

bool mira_backpressure_acquire_handle(uint64_t backpressure_handle) {
  if (backpressure_handle == 0u || backpressure_handle > 128u || !mira_backpressure[backpressure_handle - 1u].used) {
    return 0u;
  }
  mira_backpressure_entry* entry = &mira_backpressure[backpressure_handle - 1u];
  if (entry->in_use >= entry->limit) {
    return 0u;
  }
  entry->in_use += 1u;
  return 1u;
}

bool mira_backpressure_release_handle(uint64_t backpressure_handle) {
  if (backpressure_handle == 0u || backpressure_handle > 128u || !mira_backpressure[backpressure_handle - 1u].used) {
    return 0u;
  }
  mira_backpressure_entry* entry = &mira_backpressure[backpressure_handle - 1u];
  if (entry->in_use > 0u) {
    entry->in_use -= 1u;
  }
  return 1u;
}

bool mira_backpressure_saturated_handle(uint64_t backpressure_handle) {
  if (backpressure_handle == 0u || backpressure_handle > 128u || !mira_backpressure[backpressure_handle - 1u].used) {
    return 1u;
  }
  return mira_backpressure[backpressure_handle - 1u].in_use >= mira_backpressure[backpressure_handle - 1u].limit ? 1u : 0u;
}

bool mira_backpressure_close_handle(uint64_t backpressure_handle) {
  if (backpressure_handle == 0u || backpressure_handle > 128u || !mira_backpressure[backpressure_handle - 1u].used) {
    return 0u;
  }
  memset(&mira_backpressure[backpressure_handle - 1u], 0, sizeof(mira_backpressure_entry));
  return 1u;
}

uint64_t mira_supervisor_open_handle(uint32_t restart_budget, uint32_t degrade_after) {
  bool used_flags[128];
  for (size_t index = 0; index < 128u; index++) {
    used_flags[index] = mira_supervisors[index].used;
  }
  uint64_t handle = mira_alloc_handle(used_flags, 128u);
  if (handle == 0u) {
    return 0u;
  }
  memset(&mira_supervisors[handle - 1u], 0, sizeof(mira_supervisor_entry));
  mira_supervisors[handle - 1u].used = true;
  mira_supervisors[handle - 1u].restart_budget = restart_budget == 0u ? 1u : restart_budget;
  mira_supervisors[handle - 1u].degrade_after = degrade_after == 0u ? 1u : degrade_after;
  return handle;
}

bool mira_supervisor_record_failure_handle(uint64_t supervisor_handle, uint32_t code) {
  if (supervisor_handle == 0u || supervisor_handle > 128u || !mira_supervisors[supervisor_handle - 1u].used) {
    return 0u;
  }
  mira_supervisor_entry* entry = &mira_supervisors[supervisor_handle - 1u];
  entry->failures += 1u;
  entry->last_code = code;
  return 1u;
}

bool mira_supervisor_record_recovery_handle(uint64_t supervisor_handle) {
  if (supervisor_handle == 0u || supervisor_handle > 128u || !mira_supervisors[supervisor_handle - 1u].used) {
    return 0u;
  }
  mira_supervisor_entry* entry = &mira_supervisors[supervisor_handle - 1u];
  entry->recoveries += 1u;
  if (entry->failures > 0u) {
    entry->failures -= 1u;
  }
  return 1u;
}

bool mira_supervisor_should_restart_handle(uint64_t supervisor_handle) {
  if (supervisor_handle == 0u || supervisor_handle > 128u || !mira_supervisors[supervisor_handle - 1u].used) {
    return 0u;
  }
  return mira_supervisors[supervisor_handle - 1u].failures <= mira_supervisors[supervisor_handle - 1u].restart_budget ? 1u : 0u;
}

bool mira_supervisor_degraded_handle(uint64_t supervisor_handle) {
  if (supervisor_handle == 0u || supervisor_handle > 128u || !mira_supervisors[supervisor_handle - 1u].used) {
    return 1u;
  }
  return mira_supervisors[supervisor_handle - 1u].failures >= mira_supervisors[supervisor_handle - 1u].degrade_after ? 1u : 0u;
}

bool mira_supervisor_close_handle(uint64_t supervisor_handle) {
  if (supervisor_handle == 0u || supervisor_handle > 128u || !mira_supervisors[supervisor_handle - 1u].used) {
    return 0u;
  }
  memset(&mira_supervisors[supervisor_handle - 1u], 0, sizeof(mira_supervisor_entry));
  return 1u;
}
#endif

uint64_t mira_task_open_handle(const char* command) {
  return mira_spawn_open_handle(command);
}

bool mira_task_done_handle(uint64_t handle) {
  if (handle == 0u || handle > 128u || !mira_spawn_handles[handle - 1u].used) {
    return 0u;
  }
  return mira_spawn_poll_done_entry(&mira_spawn_handles[handle - 1u]) ? 1u : 0u;
}

int32_t mira_task_join_handle(uint64_t handle) {
  return mira_spawn_wait_handle(handle);
}

buf_u8 mira_task_stdout_all_handle_buf_u8(uint64_t handle) {
  return mira_spawn_stdout_all_handle_buf_u8(handle);
}

buf_u8 mira_task_stderr_all_handle_buf_u8(uint64_t handle) {
  return mira_spawn_stderr_all_handle_buf_u8(handle);
}

bool mira_task_close_handle(uint64_t handle) {
  return mira_spawn_close_handle(handle);
}

uint64_t mira_service_open_handle(const char* name) {
  if (name == NULL || name[0] == '\0') {
    return 0u;
  }
  bool used_flags[64];
  for (size_t index = 0; index < 64u; index++) {
    used_flags[index] = mira_services[index].used;
  }
  uint64_t handle = mira_alloc_handle(used_flags, 64u);
  if (handle == 0u) {
    return 0u;
  }
  mira_service_entry* entry = &mira_services[handle - 1u];
  memset(entry, 0, sizeof(*entry));
  entry->used = true;
  strncpy(entry->name, name, sizeof(entry->name) - 1u);
  entry->healthy = 200u;
  entry->ready = 503u;
  entry->shutdown = 0;
  return handle;
}

bool mira_service_close_handle(uint64_t handle) {
  if (handle == 0u || handle > 64u || !mira_services[handle - 1u].used) {
    return 0u;
  }
  memset(&mira_services[handle - 1u], 0, sizeof(mira_services[handle - 1u]));
  for (size_t index = 0; index < 256u; index++) {
    if (mira_service_traces[index].used && mira_service_traces[index].service_handle == handle) {
      memset(&mira_service_traces[index], 0, sizeof(mira_service_traces[index]));
    }
    if (mira_service_events[index].used && mira_service_events[index].service_handle == handle) {
      memset(&mira_service_events[index], 0, sizeof(mira_service_events[index]));
    }
    if (mira_service_metrics[index].used && mira_service_metrics[index].service_handle == handle) {
      memset(&mira_service_metrics[index], 0, sizeof(mira_service_metrics[index]));
    }
    if (mira_service_metric_dims[index].used && mira_service_metric_dims[index].service_handle == handle) {
      memset(&mira_service_metric_dims[index], 0, sizeof(mira_service_metric_dims[index]));
    }
    if (mira_service_failures[index].used && mira_service_failures[index].service_handle == handle) {
      memset(&mira_service_failures[index], 0, sizeof(mira_service_failures[index]));
    }
    if (mira_service_checkpoints_u32[index].used && mira_service_checkpoints_u32[index].service_handle == handle) {
      memset(&mira_service_checkpoints_u32[index], 0, sizeof(mira_service_checkpoints_u32[index]));
    }
  }
  return 1u;
}

bool mira_service_shutdown_handle(uint64_t handle, uint32_t grace_ms) {
  (void) grace_ms;
  if (handle == 0u || handle > 64u || !mira_services[handle - 1u].used) {
    return 0u;
  }
  mira_service_entry* entry = &mira_services[handle - 1u];
  entry->shutdown = 1;
  entry->healthy = 503u;
  entry->ready = 503u;
  return 1u;
}

bool mira_service_log_buf_u8(uint64_t handle, const char* level, buf_u8 message) {
  (void) level;
  (void) message;
  if (handle == 0u || handle > 64u || !mira_services[handle - 1u].used) {
    return 0u;
  }
  mira_services[handle - 1u].log_entries += 1u;
  return 1u;
}

uint64_t mira_service_trace_begin_handle(uint64_t handle, const char* name) {
  if (handle == 0u || handle > 64u || !mira_services[handle - 1u].used) {
    return 0u;
  }
  bool used_flags[256];
  for (size_t index = 0; index < 256u; index++) {
    used_flags[index] = mira_service_traces[index].used;
  }
  uint64_t trace = mira_alloc_handle(used_flags, 256u);
  if (trace == 0u) {
    return 0u;
  }
  mira_service_trace_entry* entry = &mira_service_traces[trace - 1u];
  memset(entry, 0, sizeof(*entry));
  entry->used = true;
  entry->service_handle = handle;
  entry->parent_trace = 0u;
  if (name != NULL) {
    strncpy(entry->name, name, sizeof(entry->name) - 1u);
  }
  mira_services[handle - 1u].traces_started += 1u;
  return trace;
}

bool mira_service_trace_end_handle(uint64_t trace_handle) {
  if (trace_handle == 0u || trace_handle > 256u || !mira_service_traces[trace_handle - 1u].used) {
    return 0u;
  }
  memset(&mira_service_traces[trace_handle - 1u], 0, sizeof(mira_service_traces[trace_handle - 1u]));
  return 1u;
}

bool mira_service_metric_count_handle(uint64_t handle, const char* metric, uint32_t value) {
  (void) metric;
  if (handle == 0u || handle > 64u || !mira_services[handle - 1u].used) {
    return 0u;
  }
  mira_services[handle - 1u].metrics_total += (uint64_t) value;
  if (metric != NULL) {
    mira_service_metric_entry* entry = mira_service_metric_slot(handle, metric);
    if (entry != NULL) {
      entry->total += value;
    }
  }
  return 1u;
}

bool mira_service_metric_count_dim_handle(
    uint64_t handle,
    const char* metric,
    const char* dimension,
    uint32_t value
) {
  if (handle == 0u || handle > 64u || !mira_services[handle - 1u].used) {
    return 0u;
  }
  mira_services[handle - 1u].metrics_total += (uint64_t) value;
  if (metric != NULL) {
    mira_service_metric_entry* metric_entry = mira_service_metric_slot(handle, metric);
    if (metric_entry != NULL) {
      metric_entry->total += value;
    }
  }
  if (metric != NULL && dimension != NULL) {
    mira_service_metric_dim_entry* dim_entry = mira_service_metric_dim_slot(handle, metric, dimension);
    if (dim_entry != NULL) {
      dim_entry->total += value;
    }
  }
  return 1u;
}

uint32_t mira_service_metric_total_handle(uint64_t handle, const char* metric) {
  if (handle == 0u || handle > 64u || !mira_services[handle - 1u].used || metric == NULL) {
    return 0u;
  }
  for (size_t index = 0; index < 256u; index++) {
    if (mira_service_metrics[index].used
        && mira_service_metrics[index].service_handle == handle
        && strcmp(mira_service_metrics[index].metric, metric) == 0) {
      return mira_service_metrics[index].total;
    }
  }
  return 0u;
}

uint32_t mira_service_health_status_handle(uint64_t handle) {
  if (handle == 0u || handle > 64u || !mira_services[handle - 1u].used) {
    return 503u;
  }
  return mira_services[handle - 1u].healthy;
}

uint32_t mira_service_readiness_status_handle(uint64_t handle) {
  if (handle == 0u || handle > 64u || !mira_services[handle - 1u].used) {
    return 503u;
  }
  return mira_services[handle - 1u].ready;
}

bool mira_service_set_health_handle(uint64_t handle, uint32_t status) {
  if (handle == 0u || handle > 64u || !mira_services[handle - 1u].used) {
    return 0u;
  }
  mira_services[handle - 1u].healthy = status;
  return 1u;
}

bool mira_service_set_readiness_handle(uint64_t handle, uint32_t status) {
  if (handle == 0u || handle > 64u || !mira_services[handle - 1u].used) {
    return 0u;
  }
  mira_services[handle - 1u].ready = status;
  return 1u;
}

bool mira_service_set_degraded_handle(uint64_t handle, bool degraded) {
  if (handle == 0u || handle > 64u || !mira_services[handle - 1u].used) {
    return 0u;
  }
  mira_services[handle - 1u].degraded = degraded ? 1 : 0;
  return 1u;
}

bool mira_service_degraded_handle(uint64_t handle) {
  if (handle == 0u || handle > 64u || !mira_services[handle - 1u].used) {
    return 0u;
  }
  return mira_services[handle - 1u].degraded ? 1u : 0u;
}

bool mira_service_event_buf_u8(uint64_t handle, const char* event_class, buf_u8 message) {
  if (handle == 0u || handle > 64u || !mira_services[handle - 1u].used || event_class == NULL) {
    return 0u;
  }
  mira_service_event_entry* entry = mira_service_event_slot(handle, event_class);
  if (entry == NULL) {
    return 0u;
  }
  entry->total += 1u;
  memset(entry->last_message, 0, sizeof(entry->last_message));
  if (message.data != NULL && message.len > 0u) {
    size_t copy_len = message.len < sizeof(entry->last_message) - 1u
        ? (size_t) message.len
        : sizeof(entry->last_message) - 1u;
    memcpy(entry->last_message, message.data, copy_len);
  }
  return 1u;
}

uint32_t mira_service_event_total_handle(uint64_t handle, const char* event_class) {
  if (handle == 0u || handle > 64u || !mira_services[handle - 1u].used || event_class == NULL) {
    return 0u;
  }
  for (size_t index = 0; index < 256u; index++) {
    if (mira_service_events[index].used
        && mira_service_events[index].service_handle == handle
        && strcmp(mira_service_events[index].kind, event_class) == 0) {
      return mira_service_events[index].total;
    }
  }
  return 0u;
}

bool mira_service_trace_link_handle(uint64_t trace_handle, uint64_t parent_trace) {
  if (trace_handle == 0u || trace_handle > 256u || !mira_service_traces[trace_handle - 1u].used) {
    return 0u;
  }
  if (parent_trace == 0u || parent_trace > 256u || !mira_service_traces[parent_trace - 1u].used) {
    return 0u;
  }
  if (mira_service_traces[trace_handle - 1u].service_handle != mira_service_traces[parent_trace - 1u].service_handle) {
    return 0u;
  }
  if (mira_service_traces[trace_handle - 1u].parent_trace != parent_trace) {
    mira_service_traces[trace_handle - 1u].parent_trace = parent_trace;
    uint64_t service_handle = mira_service_traces[parent_trace - 1u].service_handle;
    if (service_handle > 0u && service_handle <= 64u && mira_services[service_handle - 1u].used) {
      mira_services[service_handle - 1u].trace_links += 1u;
    }
  }
  return 1u;
}

uint32_t mira_service_trace_link_count_handle(uint64_t handle) {
  if (handle == 0u || handle > 64u || !mira_services[handle - 1u].used) {
    return 0u;
  }
  return (uint32_t) mira_services[handle - 1u].trace_links;
}

bool mira_service_failure_count_handle(uint64_t handle, const char* failure_class, uint32_t value) {
  if (handle == 0u || handle > 64u || !mira_services[handle - 1u].used || failure_class == NULL) {
    return 0u;
  }
  mira_service_failure_entry* entry = mira_service_failure_slot(handle, failure_class);
  if (entry == NULL) {
    return 0u;
  }
  entry->total += value;
  if (value > 0u) {
    mira_services[handle - 1u].degraded = 1;
  }
  return 1u;
}

uint32_t mira_service_failure_total_handle(uint64_t handle, const char* failure_class) {
  if (handle == 0u || handle > 64u || !mira_services[handle - 1u].used || failure_class == NULL) {
    return 0u;
  }
  for (size_t index = 0; index < 256u; index++) {
    if (mira_service_failures[index].used
        && mira_service_failures[index].service_handle == handle
        && strcmp(mira_service_failures[index].kind, failure_class) == 0) {
      return mira_service_failures[index].total;
    }
  }
  return 0u;
}

bool mira_service_checkpoint_save_u32_handle(uint64_t handle, const char* key, uint32_t value) {
  if (handle == 0u || handle > 64u || !mira_services[handle - 1u].used || key == NULL) {
    return 0u;
  }
  mira_service_checkpoint_u32_entry* entry = mira_service_checkpoint_u32_slot(handle, key);
  if (entry == NULL) {
    return 0u;
  }
  entry->value = value;
  return 1u;
}

uint32_t mira_service_checkpoint_load_u32_handle(uint64_t handle, const char* key) {
  if (handle == 0u || handle > 64u || !mira_services[handle - 1u].used || key == NULL) {
    return 0u;
  }
  for (size_t index = 0; index < 256u; index++) {
    if (mira_service_checkpoints_u32[index].used
        && mira_service_checkpoints_u32[index].service_handle == handle
        && strcmp(mira_service_checkpoints_u32[index].key, key) == 0) {
      return mira_service_checkpoints_u32[index].value;
    }
  }
  return 0u;
}

bool mira_service_checkpoint_exists_handle(uint64_t handle, const char* key) {
  if (handle == 0u || handle > 64u || !mira_services[handle - 1u].used || key == NULL) {
    return 0u;
  }
  for (size_t index = 0; index < 256u; index++) {
    if (mira_service_checkpoints_u32[index].used
        && mira_service_checkpoints_u32[index].service_handle == handle
        && strcmp(mira_service_checkpoints_u32[index].key, key) == 0) {
      return 1u;
    }
  }
  return 0u;
}

bool mira_service_migrate_db_handle(uint64_t handle, uint64_t db_handle, const char* migration_name) {
  (void) migration_name;
  if (handle == 0u || handle > 64u || !mira_services[handle - 1u].used) {
    return 0u;
  }
  if (db_handle == 0u || db_handle > 128u || !mira_db_handles[db_handle - 1u].used) {
    return 0u;
  }
  mira_services[handle - 1u].healthy = 200u;
  mira_services[handle - 1u].ready = 200u;
  return 1u;
}

bool mira_service_route_buf_u8(buf_u8 request, const char* method, const char* path) {
  return mira_http_method_eq_buf_u8(request, method) && mira_http_path_eq_buf_u8(request, path);
}

bool mira_service_route_buf_u8_parts(
    const uint8_t* data,
    uint32_t len,
    const char* method,
    const char* path
) {
  buf_u8 request;
  request.data = (uint8_t*) data;
  request.len = len;
  request.cap = len;
  return mira_service_route_buf_u8(request, method, path);
}

bool mira_service_require_header_buf_u8(buf_u8 request, const char* name, const char* value) {
  return mira_http_header_eq_buf_u8(request, name, value);
}

uint32_t mira_service_error_status(const char* kind) {
  if (kind == NULL) {
    return 500u;
  }
  if (strcmp(kind, "bad_request") == 0) return 400u;
  if (strcmp(kind, "unauthorized") == 0) return 401u;
  if (strcmp(kind, "forbidden") == 0) return 403u;
  if (strcmp(kind, "not_found") == 0) return 404u;
  if (strcmp(kind, "conflict") == 0) return 409u;
  if (strcmp(kind, "payload_too_large") == 0) return 413u;
  if (strcmp(kind, "too_many_requests") == 0) return 429u;
  if (strcmp(kind, "service_unavailable") == 0) return 503u;
  return 500u;
}

uint64_t mira_ffi_open_lib_handle(const char* path) {
  bool used_flags[128];
  for (size_t index = 0; index < 128; index++) {
    used_flags[index] = mira_ffi_lib_handles[index].used;
  }
  uint64_t handle = mira_alloc_handle(used_flags, 128u);
  if (handle == 0u) {
    return 0u;
  }
#ifdef _WIN32
  HMODULE library = LoadLibraryA(path);
  if (library == NULL) {
    return 0u;
  }
  mira_ffi_lib_handles[handle - 1u].handle = library;
#else
  void* library = dlopen(path, RTLD_LAZY);
  if (library == NULL) {
    return 0u;
  }
  mira_ffi_lib_handles[handle - 1u].handle = library;
#endif
  mira_ffi_lib_handles[handle - 1u].used = true;
  return handle;
}

bool mira_ffi_close_lib_handle(uint64_t handle) {
  if (handle == 0u || handle > 128u || !mira_ffi_lib_handles[handle - 1u].used) {
    return 0u;
  }
#ifdef _WIN32
  FreeLibrary(mira_ffi_lib_handles[handle - 1u].handle);
#else
  dlclose(mira_ffi_lib_handles[handle - 1u].handle);
#endif
  mira_ffi_lib_handles[handle - 1u].used = false;
  return 1u;
}

uint64_t mira_ffi_buf_ptr_buf_u8(buf_u8 value) {
  return (uint64_t) (uintptr_t) value.data;
}

uint64_t mira_ffi_call_lib_u64(uint64_t handle, const char* symbol, uint32_t argc, const uint64_t* argv) {
  if (handle == 0u || handle > 128u || !mira_ffi_lib_handles[handle - 1u].used) {
    return 0u;
  }
#ifdef _WIN32
  void* address = (void*) GetProcAddress(mira_ffi_lib_handles[handle - 1u].handle, symbol);
#else
  void* address = dlsym(mira_ffi_lib_handles[handle - 1u].handle, symbol);
#endif
  if (address == NULL) {
    return 0u;
  }
  switch (argc) {
    case 0u: return ((uint64_t (*)(void)) address)();
    case 1u: return ((uint64_t (*)(uint64_t)) address)(argv[0]);
    case 2u: return ((uint64_t (*)(uint64_t, uint64_t)) address)(argv[0], argv[1]);
    case 3u: return ((uint64_t (*)(uint64_t, uint64_t, uint64_t)) address)(argv[0], argv[1], argv[2]);
    case 4u: return ((uint64_t (*)(uint64_t, uint64_t, uint64_t, uint64_t)) address)(argv[0], argv[1], argv[2], argv[3]);
    case 5u: return ((uint64_t (*)(uint64_t, uint64_t, uint64_t, uint64_t, uint64_t)) address)(argv[0], argv[1], argv[2], argv[3], argv[4]);
    case 6u: return ((uint64_t (*)(uint64_t, uint64_t, uint64_t, uint64_t, uint64_t, uint64_t)) address)(argv[0], argv[1], argv[2], argv[3], argv[4], argv[5]);
    default: return 0u;
  }
}

uint64_t mira_ffi_call_lib_cstr_u64(uint64_t handle, const char* symbol, const char* arg) {
  if (handle == 0u || handle > 128u || !mira_ffi_lib_handles[handle - 1u].used) {
    return 0u;
  }
#ifdef _WIN32
  void* address = (void*) GetProcAddress(mira_ffi_lib_handles[handle - 1u].handle, symbol);
#else
  void* address = dlsym(mira_ffi_lib_handles[handle - 1u].handle, symbol);
#endif
  if (address == NULL) {
    return 0u;
  }
  return ((uint64_t (*)(const char*)) address)(arg);
}
"#
    .to_string()
}
