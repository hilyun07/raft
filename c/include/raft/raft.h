// Copyright 2026 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef ETCD_RAFT_RAFT_H
#define ETCD_RAFT_RAFT_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Reserved values in the uint64_t node-ID namespace.
//
// RAFT_NONE means "no node" only in fields whose documented domain is a
// node ID. Numeric zero in terms, indexes, enum values, and snapshot metadata
// has separate field-specific semantics.
//
// The RAFT_LOCAL_* values are process-local asynchronous-storage endpoints.
// They are never cluster members or network transport peers.
#define RAFT_NONE UINT64_C(0)
#define RAFT_LOCAL_APPEND_THREAD UINT64_MAX
#define RAFT_LOCAL_APPLY_THREAD (UINT64_MAX - UINT64_C(1))

bool raft_is_none_id(uint64_t id);
bool raft_is_local_target_id(uint64_t id);

// Reports whether id can identify a real Raft member. This does not establish
// that the ID is present in the current configuration.
bool raft_is_valid_node_id(uint64_t id);

// raft_raw_node_t is thread-unsafe. Its layout is private to the C library.
typedef struct raft_raw_node raft_raw_node_t;

// Public functions return one of these values unless their signature returns
// void or bool. Numeric assignments are part of the public C ABI once released.
typedef enum raft_error {
    RAFT_OK = 0,
    RAFT_ERR_INVALID_ARGUMENT = 1,
    RAFT_ERR_STOPPED = 2,
    RAFT_ERR_PROPOSAL_DROPPED = 3,
    RAFT_ERR_STORAGE_COMPACTED = 4,
    RAFT_ERR_STORAGE_UNAVAILABLE = 5,
    RAFT_ERR_SNAPSHOT_TEMPORARILY_UNAVAILABLE = 6,
    RAFT_ERR_STEP_LOCAL_MSG = 7,
    RAFT_ERR_STEP_PEER_NOT_FOUND_OR_IGNORED = 8,
    RAFT_ERR_PANIC_FROM_GO_CALLBACK = 9,
    RAFT_ERR_OUT_OF_MEMORY = 10,
    RAFT_ERR_FATAL = 11,
    // Consensus-dependent skeleton operations return NOT_IMPLEMENTED until
    // their corresponding later porting phase supplies real semantics.
    RAFT_ERR_NOT_IMPLEMENTED = 12,
} raft_error_t;

// These numeric values mirror the Go/raftpb values at the binding boundary.
// C names may evolve, but these assignments must remain wire-compatible.
typedef enum raft_state {
    RAFT_STATE_FOLLOWER = 0,
    RAFT_STATE_CANDIDATE = 1,
    RAFT_STATE_LEADER = 2,
    RAFT_STATE_PRE_CANDIDATE = 3,
} raft_state_t;

typedef enum raft_snapshot_status {
    RAFT_SNAPSHOT_FINISH = 1,
    RAFT_SNAPSHOT_FAILURE = 2,
} raft_snapshot_status_t;

typedef enum raft_entry_type {
    RAFT_ENTRY_NORMAL = 0,
    RAFT_ENTRY_CONF_CHANGE = 1,
    RAFT_ENTRY_CONF_CHANGE_V2 = 2,
} raft_entry_type_t;

typedef enum raft_message_type {
    RAFT_MSG_HUP = 0,
    RAFT_MSG_BEAT = 1,
    RAFT_MSG_PROP = 2,
    RAFT_MSG_APP = 3,
    RAFT_MSG_APP_RESP = 4,
    RAFT_MSG_VOTE = 5,
    RAFT_MSG_VOTE_RESP = 6,
    RAFT_MSG_SNAP = 7,
    RAFT_MSG_HEARTBEAT = 8,
    RAFT_MSG_HEARTBEAT_RESP = 9,
    RAFT_MSG_UNREACHABLE = 10,
    RAFT_MSG_SNAP_STATUS = 11,
    RAFT_MSG_CHECK_QUORUM = 12,
    RAFT_MSG_TRANSFER_LEADER = 13,
    RAFT_MSG_TIMEOUT_NOW = 14,
    RAFT_MSG_READ_INDEX = 15,
    RAFT_MSG_READ_INDEX_RESP = 16,
    RAFT_MSG_PRE_VOTE = 17,
    RAFT_MSG_PRE_VOTE_RESP = 18,
    RAFT_MSG_STORAGE_APPEND = 19,
    RAFT_MSG_STORAGE_APPEND_RESP = 20,
    RAFT_MSG_STORAGE_APPLY = 21,
    RAFT_MSG_STORAGE_APPLY_RESP = 22,
    RAFT_MSG_FORGET_LEADER = 23,
} raft_message_type_t;

typedef enum raft_conf_change_type {
    RAFT_CONF_CHANGE_ADD_NODE = 0,
    RAFT_CONF_CHANGE_REMOVE_NODE = 1,
    RAFT_CONF_CHANGE_UPDATE_NODE = 2,
    RAFT_CONF_CHANGE_ADD_LEARNER_NODE = 3,
} raft_conf_change_type_t;

typedef enum raft_conf_change_transition {
    RAFT_CONF_CHANGE_TRANSITION_AUTO = 0,
    RAFT_CONF_CHANGE_TRANSITION_JOINT_IMPLICIT = 1,
    RAFT_CONF_CHANGE_TRANSITION_JOINT_EXPLICIT = 2,
} raft_conf_change_transition_t;

typedef enum raft_progress_type {
    RAFT_PROGRESS_PEER = 0,
    RAFT_PROGRESS_LEARNER = 1,
} raft_progress_type_t;

typedef enum raft_progress_state {
    RAFT_PROGRESS_STATE_PROBE = 0,
    RAFT_PROGRESS_STATE_REPLICATE = 1,
    RAFT_PROGRESS_STATE_SNAPSHOT = 2,
} raft_progress_state_t;

typedef enum raft_read_only_option {
    RAFT_READ_ONLY_SAFE = 0,
    RAFT_READ_ONLY_LEASE_BASED = 1,
} raft_read_only_option_t;

typedef enum raft_log_level {
    RAFT_LOG_DEBUG = 0,
    RAFT_LOG_INFO = 1,
    RAFT_LOG_WARNING = 2,
    RAFT_LOG_ERROR = 3,
    RAFT_LOG_FATAL = 4,
} raft_log_level_t;

// Borrowed input view. Public C APIs receive this as const raft_byte_view_t *;
// no public API passes descriptor structs by value. C may read data only for
// the duration of the call, must never free it, and must deep-copy it into an
// owned non-view type before retaining it.
typedef struct raft_byte_view {
    const uint8_t *data;
    size_t len;
    bool is_nil;
} raft_byte_view_t;

// C-owned internal/output bytes. data is always owned by C and is released by
// raft_bytes_free. This type must never wrap borrowed or Go-owned memory.
typedef struct raft_bytes {
    uint8_t *data;
    size_t len;
    bool is_nil;
} raft_bytes_t;

// Canonical forms accepted by both helpers:
// nil:     {NULL, 0, true}
// empty:   {NULL, 0, false}
// nonempty:{non-NULL, positive length, false}
// Dummy zero-length allocations are non-canonical and rejected.
// A NULL descriptor is invalid. It never represents a Go nil []byte; callers
// represent nil with a non-NULL descriptor in the canonical nil form above.
bool raft_byte_view_valid(const raft_byte_view_t *view);
bool raft_bytes_valid(const raft_bytes_t *bytes);
int raft_bytes_copy_from_view(raft_bytes_t *dst,
                              const raft_byte_view_t *src);
int raft_bytes_copy(raft_bytes_t *dst, const raft_bytes_t *src);

// Borrowed array view. The C core may read items only during the call.
typedef struct raft_uint64_view {
    const uint64_t *items;
    size_t len;
} raft_uint64_view_t;

// C-owned array used in retained and output objects.
typedef struct raft_uint64_vec {
    uint64_t *items;
    size_t len;
} raft_uint64_vec_t;

typedef struct raft_soft_state {
    uint64_t lead;
    raft_state_t raft_state;
} raft_soft_state_t;

typedef struct raft_hard_state {
    uint64_t term;
    uint64_t vote;
    uint64_t commit;
} raft_hard_state_t;

typedef struct raft_conf_state {
    raft_uint64_vec_t voters;
    raft_uint64_vec_t voters_outgoing;
    raft_uint64_vec_t learners;
    raft_uint64_vec_t learners_next;
    bool auto_leave;
} raft_conf_state_t;

typedef struct raft_conf_state_view {
    raft_uint64_view_t voters;
    raft_uint64_view_t voters_outgoing;
    raft_uint64_view_t learners;
    raft_uint64_view_t learners_next;
    bool auto_leave;
} raft_conf_state_view_t;

typedef struct raft_snapshot_metadata {
    raft_conf_state_t conf_state;
    uint64_t index;
    uint64_t term;
} raft_snapshot_metadata_t;

typedef struct raft_snapshot_metadata_view {
    raft_conf_state_view_t conf_state;
    uint64_t index;
    uint64_t term;
} raft_snapshot_metadata_view_t;

// Borrowed snapshot input. All nested pointers are call-scoped. A Go binding
// must construct this pointer-containing descriptor in C storage, not Go
// memory.
typedef struct raft_snapshot_view {
    raft_byte_view_t data;
    raft_snapshot_metadata_view_t metadata;
} raft_snapshot_view_t;

typedef struct raft_snapshot {
    // Snapshot.Data is opaque. A valid snapshot may have empty data; metadata
    // index, not data length, determines whether the snapshot is empty.
    raft_bytes_t data;
    raft_snapshot_metadata_t metadata;
} raft_snapshot_t;

typedef struct raft_entry_view {
    raft_entry_type_t type;
    uint64_t term;
    uint64_t index;
    raft_byte_view_t data;
} raft_entry_view_t;

typedef struct raft_entry {
    raft_entry_type_t type;
    uint64_t term;
    uint64_t index;
    // Entry.Data is opaque and preserves nil versus present-empty.
    raft_bytes_t data;
} raft_entry_t;

typedef struct raft_entry_view_vec {
    const raft_entry_view_t *items;
    size_t len;
} raft_entry_view_vec_t;

typedef struct raft_entry_vec {
    raft_entry_t *items;
    size_t len;
} raft_entry_vec_t;

typedef struct raft_message raft_message_t;
typedef struct raft_message_view raft_message_view_t;

typedef struct raft_message_vec {
    raft_message_t *items;
    size_t len;
} raft_message_vec_t;

typedef struct raft_message_view_vec {
    const raft_message_view_t *items;
    size_t len;
} raft_message_view_vec_t;

// Pointer-bearing view descriptors belong in C storage. In particular, a Go
// binding must not allocate raft_entry_view_t, raft_snapshot_view_t, or
// raft_message_view_t in Go memory and pass its address to C. Use C-side
// scalar shims/builders, and deep-copy before retaining any nested pointer.
struct raft_message_view {
    raft_message_type_t type;
    uint64_t to;
    uint64_t from;
    uint64_t term;
    uint64_t log_term;
    uint64_t index;
    uint64_t commit;
    uint64_t vote;
    bool reject;
    uint64_t reject_hint;
    raft_entry_view_vec_t entries;
    bool has_snapshot;
    raft_snapshot_view_t snapshot;
    raft_byte_view_t context;
    raft_message_view_vec_t responses;
};

struct raft_message {
    raft_message_type_t type;
    uint64_t to;
    uint64_t from;
    uint64_t term;
    uint64_t log_term;
    uint64_t index;
    uint64_t commit;
    uint64_t vote;
    bool reject;
    uint64_t reject_hint;
    raft_entry_vec_t entries;
    bool has_snapshot;
    raft_snapshot_t snapshot;
    // Message.Context is opaque and preserves nil versus present-empty.
    raft_bytes_t context;
    raft_message_vec_t responses;
};

typedef struct raft_read_state {
    uint64_t index;
    // ReadIndex request context returned to the application.
    raft_bytes_t request_ctx;
} raft_read_state_t;

typedef struct raft_read_state_vec {
    raft_read_state_t *items;
    size_t len;
} raft_read_state_vec_t;

typedef struct raft_ready {
    bool has_soft_state;
    raft_soft_state_t soft_state;
    bool has_hard_state;
    raft_hard_state_t hard_state;
    raft_read_state_vec_t read_states;
    raft_entry_vec_t entries;
    bool has_snapshot;
    raft_snapshot_t snapshot;
    raft_entry_vec_t committed_entries;
    raft_message_vec_t messages;
    bool must_sync;
    // Reserved for associating preview/accept/advance without depending on
    // pointers into freed Ready output. Callers must not interpret this token.
    uint64_t opaque_token;
} raft_ready_t;

// Legacy raftpb.ConfChange representation. The id field is etcd-visible
// legacy metadata and has no ConfChangeV2 counterpart.
typedef struct raft_conf_change_view {
    uint64_t id;
    raft_conf_change_type_t type;
    uint64_t node_id;
    raft_byte_view_t context;
} raft_conf_change_view_t;

typedef struct raft_conf_change_single {
    raft_conf_change_type_t type;
    uint64_t node_id;
} raft_conf_change_single_t;

typedef struct raft_conf_change_v2_view {
    raft_conf_change_transition_t transition;
    const raft_conf_change_single_t *changes;
    size_t changes_len;
    raft_byte_view_t context;
} raft_conf_change_v2_view_t;

typedef struct raft_progress {
    // Scalar copy of the exported tracker.Progress state. The live inflights
    // queue is deliberately absent: Go RawNode.WithProgress clears Inflights
    // before invoking its visitor.
    uint64_t match_index;
    uint64_t next_index;
    raft_progress_state_t state;
    uint64_t pending_snapshot;
    bool recent_active;
    bool message_flow_paused;
    bool is_learner;
} raft_progress_t;

// One row in a C-owned point-in-time progress snapshot. This structure and all
// of its fields are copies: caller mutation cannot affect RawNode state, and no
// pointer into the internal tracker is exposed. id is always a real tracked
// member ID, never RAFT_NONE or a RAFT_LOCAL_* target.
typedef struct raft_progress_snapshot {
    uint64_t id;
    raft_progress_type_t type;
    raft_progress_t progress;
} raft_progress_snapshot_t;

typedef struct raft_basic_status {
    uint64_t id;
    raft_hard_state_t hard_state;
    raft_soft_state_t soft_state;
    uint64_t applied;
    uint64_t lead_transferee;
} raft_basic_status_t;

typedef struct raft_status {
    raft_basic_status_t basic;
    raft_conf_state_t conf_state;
    // Matching Go Status, progress is populated only for leaders. Use
    // raft_raw_node_progress_snapshot for role-independent WithProgress
    // semantics.
    raft_progress_snapshot_t *progress;
    size_t progress_len;
} raft_status_t;

typedef struct raft_peer_view {
    uint64_t id;
    // Borrowed Peer.Context is copied before Bootstrap retains it.
    raft_byte_view_t context;
} raft_peer_view_t;

typedef struct raft_config {
    uint64_t id;
    uint32_t election_tick;
    uint32_t heartbeat_tick;
    uint64_t applied;
    bool async_storage_writes;
    uint64_t max_size_per_message;
    uint64_t max_committed_size_per_ready;
    uint64_t max_uncommitted_entries_size;
    size_t max_inflight_messages;
    uint64_t max_inflight_bytes;
    bool check_quorum;
    bool pre_vote;
    raft_read_only_option_t read_only_option;
    bool disable_proposal_forwarding;
    bool disable_conf_change_validation;
    bool step_down_on_removal;
} raft_config_t;

typedef int (*raft_storage_initial_state_fn)(uintptr_t handle,
                                             raft_hard_state_t *hard_state,
                                             raft_conf_state_t *conf_state);
typedef int (*raft_storage_entries_fn)(uintptr_t handle,
                                       uint64_t lo,
                                       uint64_t hi,
                                       uint64_t max_size,
                                       raft_entry_vec_t *entries);
typedef int (*raft_storage_term_fn)(uintptr_t handle,
                                    uint64_t index,
                                    uint64_t *term);
typedef int (*raft_storage_first_index_fn)(uintptr_t handle, uint64_t *index);
typedef int (*raft_storage_last_index_fn)(uintptr_t handle, uint64_t *index);
typedef int (*raft_storage_snapshot_fn)(uintptr_t handle,
                                        raft_snapshot_t *snapshot);

// The RawNode copies this callback table; it never retains the caller's table
// pointer. handle is an opaque integer token suitable for runtime/cgo.Handle
// and must never be a raw Go pointer. Entry/Snapshot callback outputs use
// owned non-view types: the exported Go callback deep-copies into C memory
// before returning, and the C consumer frees the result with the matching
// owned free function. entries returns one owned array per range callback,
// never one Go callback per entry.
typedef struct raft_storage_ops {
    uintptr_t handle;
    raft_storage_initial_state_fn initial_state;
    raft_storage_entries_fn entries;
    raft_storage_term_fn term;
    raft_storage_first_index_fn first_index;
    raft_storage_last_index_fn last_index;
    raft_storage_snapshot_fn snapshot;
} raft_storage_ops_t;

typedef void (*raft_logger_log_fn)(uintptr_t handle,
                                   raft_log_level_t level,
                                   const raft_byte_view_t *message);

// Logger bridging is deferred. This shape reserves an opaque cgo.Handle-based
// callback ABI; the current skeleton stores no logger table and invokes no
// logger or TraceLogger callback.
typedef struct raft_logger_ops {
    uintptr_t handle;
    raft_logger_log_fn log;
} raft_logger_ops_t;

// Validators distinguish borrowed views from C-owned retained/output objects.
bool raft_entry_view_valid(const raft_entry_view_t *entry);
bool raft_entry_valid(const raft_entry_t *entry);
bool raft_snapshot_view_valid(const raft_snapshot_view_t *snapshot);
bool raft_snapshot_valid(const raft_snapshot_t *snapshot);
bool raft_message_view_valid(const raft_message_view_t *message);
bool raft_message_valid(const raft_message_t *message);

// Copy helpers require a valid source and a zero-initialized destination.
// They recursively deep-copy every byte payload and array. On failure they
// release partial output and reset the destination to a safe zero/nil state.
int raft_entry_copy_from_view(raft_entry_t *dst,
                              const raft_entry_view_t *src);
int raft_snapshot_copy_from_view(raft_snapshot_t *dst,
                                 const raft_snapshot_view_t *src);
int raft_message_copy_from_view(raft_message_t *dst,
                                const raft_message_view_t *src);

// Free functions operate only on C-owned non-view types and reset supplied
// containers. They are safe for zero-initialized owned values. Never pass a
// borrowed view or a non-view object containing borrowed memory.
// raft_bytes_free always frees data (free(NULL) is valid), then resets to nil.
void raft_bytes_free(raft_bytes_t *bytes);
void raft_uint64_vec_free(raft_uint64_vec_t *vec);
void raft_entry_free(raft_entry_t *entry);
void raft_entry_array_free(raft_entry_t *entries, size_t len);
void raft_entry_vec_free(raft_entry_vec_t *vec);
void raft_snapshot_free(raft_snapshot_t *snapshot);
void raft_message_free(raft_message_t *message);
void raft_message_array_free(raft_message_t *messages, size_t len);
void raft_message_vec_free(raft_message_vec_t *vec);
void raft_read_state_free(raft_read_state_t *read_state);
void raft_read_state_vec_free(raft_read_state_vec_t *vec);
void raft_conf_state_free(raft_conf_state_t *conf_state);
void raft_ready_free(raft_ready_t *ready);
// For heap-owned Ready output returned by raft_raw_node_ready*.
void raft_ready_destroy(raft_ready_t *ready);
// Frees the C-owned array returned by raft_raw_node_progress_snapshot.
// It is safe to call with NULL and len == 0.
void raft_progress_snapshot_array_free(raft_progress_snapshot_t *snapshots,
                                       size_t len);
void raft_status_free(raft_status_t *status);

int raft_raw_node_new(const raft_config_t *config,
                      const raft_storage_ops_t *storage,
                      raft_raw_node_t **out);
void raft_raw_node_destroy(raft_raw_node_t *raw_node);

// API layering for pointer-bearing input:
// - Native C callers may create const *_view_t descriptors on the C stack.
// - The Go binding uses *_from_parts for flat bytes.
// - For aggregate input (peers, configuration changes, messages), the Go
//   binding allocates the descriptor graph with C.malloc/C.calloc, fills it,
//   makes one RawNode cgo call, and frees all temporary descriptors
//   immediately afterward. Nested byte data may point to pointer-free Go byte
//   backing arrays only during that call. C never retains a view pointer.
// Field-by-field builder calls are intentionally not part of this skeleton.

// tick and tick_quiesced are inert in this skeleton because their required
// public signatures have no error return. They must not be mistaken for
// implemented clock semantics until the C core phase.
void raft_raw_node_tick(raft_raw_node_t *raw_node);
void raft_raw_node_tick_quiesced(raft_raw_node_t *raw_node);

// Except where noted below, valid calls to the following consensus-dependent
// skeleton operations return RAFT_ERR_NOT_IMPLEMENTED.
int raft_raw_node_bootstrap(raft_raw_node_t *raw_node,
                            const raft_peer_view_t *peers,
                            size_t peer_count);
int raft_raw_node_campaign(raft_raw_node_t *raw_node);
int raft_raw_node_propose(raft_raw_node_t *raw_node,
                          const raft_byte_view_t *data);
int raft_raw_node_propose_conf_change(
    raft_raw_node_t *raw_node,
    const raft_conf_change_v2_view_t *conf_change);
int raft_raw_node_apply_conf_change(
    raft_raw_node_t *raw_node,
    const raft_conf_change_v2_view_t *conf_change,
    raft_conf_state_t *conf_state);
// Public RawNode.Step boundary for external C callers. It applies public Step
// validation, including local-message rejection and (once the C tracker
// exists) unknown-peer response rejection, then delegates to
// raft_raw_node_step_for_node.
int raft_raw_node_step(raft_raw_node_t *raw_node,
                       const raft_message_view_t *message);
// Lower-level Go Node actor/core boundary corresponding to node.run's
// package-internal r.Step calls. It intentionally bypasses public
// RawNode.Step validation and must never delegate back to raft_raw_node_step.
// The C-backed node.run path uses this function for messages already routed
// and filtered by the Go Node layer.
int raft_raw_node_step_for_node(raft_raw_node_t *raw_node,
                                const raft_message_view_t *message);

// The skeleton has no pending work and returns false. Later Ready phases must
// implement every Ready source before enabling the C backend.
bool raft_raw_node_has_ready(const raft_raw_node_t *raw_node);
// These functions return a C-owned outer descriptor and nested graph. A Go
// binding converts the complete graph in one batch and calls
// raft_ready_destroy once. It never allocates raft_ready_t in Go memory.
int raft_raw_node_ready(raft_raw_node_t *raw_node, raft_ready_t **ready);
int raft_raw_node_ready_without_accept(raft_raw_node_t *raw_node,
                                       raft_ready_t **ready);
// ready must be the same C-resident Ready produced by the preview path; it is
// never reconstructed as a Go-allocated descriptor.
int raft_raw_node_accept_ready(raft_raw_node_t *raw_node,
                               const raft_ready_t *ready);
// Advances the internally accepted Ready. The Go binding does not reconstruct
// or pass a pointer-bearing raft_ready_t back into C.
int raft_raw_node_advance(raft_raw_node_t *raw_node);

int raft_raw_node_read_index(raft_raw_node_t *raw_node,
                             const raft_byte_view_t *request_context);

// cgo-safe scalar shims construct pointer-bearing byte-view descriptors in C
// storage. A Go binding should call these instead of allocating a
// raft_byte_view_t in Go memory and passing its address to C.
int raft_raw_node_propose_from_parts(raft_raw_node_t *raw_node,
                                     const uint8_t *data,
                                     size_t len,
                                     bool is_nil);
int raft_raw_node_read_index_from_parts(raft_raw_node_t *raw_node,
                                        const uint8_t *request_context,
                                        size_t len,
                                        bool is_nil);

// Basic/full status safely report the inert allocated skeleton state and
// return RAFT_OK. Full status has empty configuration and progress.
int raft_raw_node_basic_status(const raft_raw_node_t *raw_node,
                               raft_basic_status_t *status);
int raft_raw_node_status(const raft_raw_node_t *raw_node,
                         raft_status_t *status);

// Returns a C-owned, role-independent, point-in-time copy for implementing Go
// RawNode.WithProgress. No live tracker or inflights pointer is exposed, and C
// never calls a Go visitor. On RAFT_OK, an empty tracker is represented by
// *out == NULL and *out_len == 0. The caller owns a non-empty returned array
// and releases it with raft_progress_snapshot_array_free.
//
// The skeleton resets outputs and returns RAFT_ERR_NOT_IMPLEMENTED rather than
// pretending that its unimplemented tracker is empty.
int raft_raw_node_progress_snapshot(const raft_raw_node_t *raw_node,
                                    raft_progress_snapshot_t **out,
                                    size_t *out_len);
// Scalar boundary query used by the Go Node actor after membership changes.
// It never exposes a live Progress pointer. The skeleton has no tracker and
// returns false.
bool raft_raw_node_has_progress(const raft_raw_node_t *raw_node, uint64_t id);

int raft_raw_node_report_unreachable(raft_raw_node_t *raw_node, uint64_t id);
int raft_raw_node_report_snapshot(raft_raw_node_t *raw_node,
                                  uint64_t id,
                                  raft_snapshot_status_t status);

// Canonical mapping for Go RawNode.TransferLeader. Go
// Node.TransferLeadership remains a Go actor/channel-layer routing operation:
// it constructs MsgTransferLeader{From: transferee, To: lead} and submits that
// message to the C-backed core through raft_raw_node_step_for_node.
int raft_raw_node_transfer_leader(raft_raw_node_t *raw_node,
                                  uint64_t transferee);
int raft_raw_node_forget_leader(raft_raw_node_t *raw_node);

#ifdef __cplusplus
}
#endif

#endif  // ETCD_RAFT_RAFT_H
