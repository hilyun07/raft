#include "copy.h"

#include <stdlib.h>
#include <string.h>

static int uint64_vec_copy(raft_uint64_vec_t *dst,
                           const raft_uint64_vec_t *src) {
    if (src->len == 0) {
        return RAFT_OK;
    }
    if (src->items == NULL || src->len > SIZE_MAX / sizeof(*dst->items)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    dst->items = malloc(src->len * sizeof(*dst->items));
    if (dst->items == NULL) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    memcpy(dst->items, src->items, src->len * sizeof(*dst->items));
    dst->len = src->len;
    return RAFT_OK;
}

int rafttest_c_conf_state_copy(raft_conf_state_t *dst,
                               const raft_conf_state_t *src) {
    int result;

    if (dst == NULL || src == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(dst, 0, sizeof(*dst));
    result = uint64_vec_copy(&dst->voters, &src->voters);
    if (result == RAFT_OK) {
        result = uint64_vec_copy(&dst->voters_outgoing,
                                 &src->voters_outgoing);
    }
    if (result == RAFT_OK) {
        result = uint64_vec_copy(&dst->learners, &src->learners);
    }
    if (result == RAFT_OK) {
        result = uint64_vec_copy(&dst->learners_next,
                                 &src->learners_next);
    }
    if (result == RAFT_OK) {
        result = raft_bytes_copy(&dst->protobuf.unknown_fields,
                                 &src->protobuf.unknown_fields);
    }
    if (result != RAFT_OK) {
        raft_conf_state_free(dst);
        return result;
    }
    dst->auto_leave = src->auto_leave;
    dst->protobuf.fields = src->protobuf.fields;
    return RAFT_OK;
}

static void metadata_view(raft_protobuf_metadata_view_t *dst,
                          const raft_protobuf_metadata_t *src) {
    dst->fields = src->fields;
    dst->unknown_fields.data = src->unknown_fields.data;
    dst->unknown_fields.len = src->unknown_fields.len;
    dst->unknown_fields.is_nil = src->unknown_fields.is_nil;
}

static void entry_view(raft_entry_view_t *dst, const raft_entry_t *src) {
    memset(dst, 0, sizeof(*dst));
    dst->type = src->type;
    dst->term = src->term;
    dst->index = src->index;
    dst->data.data = src->data.data;
    dst->data.len = src->data.len;
    dst->data.is_nil = src->data.is_nil;
    metadata_view(&dst->protobuf, &src->protobuf);
}

static void snapshot_view(raft_snapshot_view_t *dst,
                          const raft_snapshot_t *src) {
    memset(dst, 0, sizeof(*dst));
    dst->data.data = src->data.data;
    dst->data.len = src->data.len;
    dst->data.is_nil = src->data.is_nil;
    dst->metadata.conf_state.voters.items =
        src->metadata.conf_state.voters.items;
    dst->metadata.conf_state.voters.len =
        src->metadata.conf_state.voters.len;
    dst->metadata.conf_state.voters_outgoing.items =
        src->metadata.conf_state.voters_outgoing.items;
    dst->metadata.conf_state.voters_outgoing.len =
        src->metadata.conf_state.voters_outgoing.len;
    dst->metadata.conf_state.learners.items =
        src->metadata.conf_state.learners.items;
    dst->metadata.conf_state.learners.len =
        src->metadata.conf_state.learners.len;
    dst->metadata.conf_state.learners_next.items =
        src->metadata.conf_state.learners_next.items;
    dst->metadata.conf_state.learners_next.len =
        src->metadata.conf_state.learners_next.len;
    dst->metadata.conf_state.auto_leave =
        src->metadata.conf_state.auto_leave;
    metadata_view(&dst->metadata.conf_state.protobuf,
                  &src->metadata.conf_state.protobuf);
    dst->metadata.index = src->metadata.index;
    dst->metadata.term = src->metadata.term;
    metadata_view(&dst->metadata.protobuf, &src->metadata.protobuf);
    metadata_view(&dst->protobuf, &src->protobuf);
}

int rafttest_c_entry_copy(raft_entry_t *dst, const raft_entry_t *src) {
    raft_entry_view_t view;

    if (dst == NULL || src == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    entry_view(&view, src);
    memset(dst, 0, sizeof(*dst));
    return raft_entry_copy_from_view(dst, &view);
}

int rafttest_c_snapshot_copy(raft_snapshot_t *dst,
                             const raft_snapshot_t *src) {
    raft_snapshot_view_t view;

    if (dst == NULL || src == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    snapshot_view(&view, src);
    memset(dst, 0, sizeof(*dst));
    return raft_snapshot_copy_from_view(dst, &view);
}

int rafttest_c_message_view_init(rafttest_c_message_view_t *owner,
                                 const raft_message_t *message) {
    size_t i;
    int result;

    if (owner == NULL || message == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(owner, 0, sizeof(*owner));
    owner->value.type = message->type;
    owner->value.to = message->to;
    owner->value.from = message->from;
    owner->value.term = message->term;
    owner->value.log_term = message->log_term;
    owner->value.index = message->index;
    owner->value.commit = message->commit;
    owner->value.vote = message->vote;
    owner->value.reject = message->reject;
    owner->value.reject_hint = message->reject_hint;
    owner->value.context.data = message->context.data;
    owner->value.context.len = message->context.len;
    owner->value.context.is_nil = message->context.is_nil;
    owner->value.has_snapshot = message->has_snapshot;
    snapshot_view(&owner->value.snapshot, &message->snapshot);
    metadata_view(&owner->value.protobuf, &message->protobuf);

    if (message->entries.len != 0) {
        if (message->entries.items == NULL ||
            message->entries.len > SIZE_MAX / sizeof(*owner->entries)) {
            result = RAFT_ERR_INVALID_ARGUMENT;
            goto fail;
        }
        owner->entries = calloc(message->entries.len,
                                sizeof(*owner->entries));
        if (owner->entries == NULL) {
            result = RAFT_ERR_OUT_OF_MEMORY;
            goto fail;
        }
        for (i = 0; i < message->entries.len; ++i) {
            entry_view(&owner->entries[i], &message->entries.items[i]);
        }
        owner->value.entries.items = owner->entries;
        owner->value.entries.len = message->entries.len;
    }

    if (message->responses.len != 0) {
        if (message->responses.items == NULL ||
            message->responses.len >
                SIZE_MAX / sizeof(*owner->response_owners) ||
            message->responses.len > SIZE_MAX / sizeof(*owner->responses)) {
            result = RAFT_ERR_INVALID_ARGUMENT;
            goto fail;
        }
        owner->response_owners = calloc(
            message->responses.len, sizeof(*owner->response_owners));
        owner->responses = calloc(message->responses.len,
                                  sizeof(*owner->responses));
        if (owner->response_owners == NULL || owner->responses == NULL) {
            result = RAFT_ERR_OUT_OF_MEMORY;
            goto fail;
        }
        owner->value.responses.items = owner->responses;
        owner->value.responses.len = message->responses.len;
        for (i = 0; i < message->responses.len; ++i) {
            result = rafttest_c_message_view_init(
                &owner->response_owners[i], &message->responses.items[i]);
            if (result != RAFT_OK) {
                goto fail;
            }
            owner->responses[i] = owner->response_owners[i].value;
        }
    }
    return RAFT_OK;

fail:
    rafttest_c_message_view_close(owner);
    return result;
}

void rafttest_c_message_view_close(rafttest_c_message_view_t *owner) {
    size_t i;

    if (owner == NULL) {
        return;
    }
    if (owner->response_owners != NULL) {
        for (i = 0; i < owner->value.responses.len; ++i) {
            rafttest_c_message_view_close(&owner->response_owners[i]);
        }
    }
    free(owner->responses);
    free(owner->response_owners);
    free(owner->entries);
    memset(owner, 0, sizeof(*owner));
}

int rafttest_c_message_copy(raft_message_t *dst,
                            const raft_message_t *src) {
    rafttest_c_message_view_t view;
    int result;

    if (dst == NULL || src == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(dst, 0, sizeof(*dst));
    result = rafttest_c_message_view_init(&view, src);
    if (result != RAFT_OK) {
        return result;
    }
    result = raft_message_copy_from_view(dst, &view.value);
    rafttest_c_message_view_close(&view);
    return result;
}
