#include "storage.h"

#include "copy.h"

#include <stdlib.h>
#include <string.h>

void rafttest_c_storage_init(rafttest_c_storage_t *storage) {
    memset(storage, 0, sizeof(*storage));
    storage->snapshot.data.is_nil = true;
}

void rafttest_c_storage_close(rafttest_c_storage_t *storage) {
    if (storage == NULL) {
        return;
    }
    raft_entry_array_free(storage->entries, storage->entries_len);
    raft_conf_state_free(&storage->conf_state);
    raft_snapshot_free(&storage->snapshot);
    memset(storage, 0, sizeof(*storage));
}

uint64_t rafttest_c_storage_first_index(
    const rafttest_c_storage_t *storage) {
    if (storage->has_snapshot) {
        return storage->snapshot.metadata.index + UINT64_C(1);
    }
    if (storage->entries_len != 0) {
        return storage->entries[0].index;
    }
    return UINT64_C(1);
}

uint64_t rafttest_c_storage_last_index(
    const rafttest_c_storage_t *storage) {
    if (storage->entries_len != 0) {
        return storage->entries[storage->entries_len - 1].index;
    }
    if (storage->has_snapshot) {
        return storage->snapshot.metadata.index;
    }
    return UINT64_C(0);
}

static int storage_initial_state(uintptr_t handle,
                                 raft_hard_state_t *hard_state,
                                 raft_conf_state_t *conf_state) {
    rafttest_c_storage_t *storage =
        (rafttest_c_storage_t *)(uintptr_t)handle;

    if (storage == NULL || hard_state == NULL || conf_state == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    *hard_state = storage->hard_state;
    return rafttest_c_conf_state_copy(conf_state, &storage->conf_state);
}

static int storage_entries(uintptr_t handle,
                           uint64_t lo,
                           uint64_t hi,
                           uint64_t max_size,
                           raft_entry_vec_t *out) {
    rafttest_c_storage_t *storage =
        (rafttest_c_storage_t *)(uintptr_t)handle;
    uint64_t first;
    uint64_t last;
    size_t count;
    size_t offset;
    size_t i;
    int result;

    (void)max_size;
    if (storage == NULL || out == NULL || lo > hi) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(out, 0, sizeof(*out));
    first = rafttest_c_storage_first_index(storage);
    last = rafttest_c_storage_last_index(storage);
    if (lo < first) {
        return RAFT_ERR_STORAGE_COMPACTED;
    }
    if (hi > last + UINT64_C(1)) {
        return RAFT_ERR_STORAGE_UNAVAILABLE;
    }
    if (lo == hi) {
        return RAFT_OK;
    }
    if (storage->entries_len == 0 || lo < storage->entries[0].index) {
        return RAFT_ERR_STORAGE_UNAVAILABLE;
    }
    count = (size_t)(hi - lo);
    offset = (size_t)(lo - storage->entries[0].index);
    if (offset > storage->entries_len ||
        count > storage->entries_len - offset ||
        count > SIZE_MAX / sizeof(*out->items)) {
        return RAFT_ERR_STORAGE_UNAVAILABLE;
    }
    out->items = calloc(count, sizeof(*out->items));
    if (out->items == NULL) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    out->len = count;
    for (i = 0; i < count; ++i) {
        result = rafttest_c_entry_copy(&out->items[i],
                                       &storage->entries[offset + i]);
        if (result != RAFT_OK) {
            raft_entry_vec_free(out);
            return result;
        }
    }
    return RAFT_OK;
}

static int storage_term(uintptr_t handle, uint64_t index, uint64_t *term) {
    rafttest_c_storage_t *storage =
        (rafttest_c_storage_t *)(uintptr_t)handle;
    uint64_t first;
    uint64_t last;
    size_t offset;

    if (storage == NULL || term == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    if (storage->has_snapshot &&
        index == storage->snapshot.metadata.index) {
        *term = storage->snapshot.metadata.term;
        return RAFT_OK;
    }
    if (index == 0 && !storage->has_snapshot) {
        *term = 0;
        return RAFT_OK;
    }
    first = rafttest_c_storage_first_index(storage);
    last = rafttest_c_storage_last_index(storage);
    if (index < first) {
        return RAFT_ERR_STORAGE_COMPACTED;
    }
    if (index > last || storage->entries_len == 0 ||
        index < storage->entries[0].index) {
        return RAFT_ERR_STORAGE_UNAVAILABLE;
    }
    offset = (size_t)(index - storage->entries[0].index);
    if (offset >= storage->entries_len) {
        return RAFT_ERR_STORAGE_UNAVAILABLE;
    }
    *term = storage->entries[offset].term;
    return RAFT_OK;
}

static int storage_first_index_cb(uintptr_t handle, uint64_t *index) {
    rafttest_c_storage_t *storage =
        (rafttest_c_storage_t *)(uintptr_t)handle;

    if (storage == NULL || index == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    *index = rafttest_c_storage_first_index(storage);
    return RAFT_OK;
}

static int storage_last_index_cb(uintptr_t handle, uint64_t *index) {
    rafttest_c_storage_t *storage =
        (rafttest_c_storage_t *)(uintptr_t)handle;

    if (storage == NULL || index == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    *index = rafttest_c_storage_last_index(storage);
    return RAFT_OK;
}

static int storage_snapshot_cb(uintptr_t handle,
                               raft_snapshot_t *snapshot) {
    rafttest_c_storage_t *storage =
        (rafttest_c_storage_t *)(uintptr_t)handle;

    if (storage == NULL || snapshot == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(snapshot, 0, sizeof(*snapshot));
    if (!storage->has_snapshot) {
        snapshot->data.is_nil = true;
        return RAFT_OK;
    }
    return rafttest_c_snapshot_copy(snapshot, &storage->snapshot);
}

raft_storage_ops_t rafttest_c_storage_ops(rafttest_c_storage_t *storage) {
    const raft_storage_ops_t ops = {
        .handle = (uintptr_t)storage,
        .initial_state = storage_initial_state,
        .entries = storage_entries,
        .term = storage_term,
        .first_index = storage_first_index_cb,
        .last_index = storage_last_index_cb,
        .snapshot = storage_snapshot_cb,
    };
    return ops;
}

int rafttest_c_storage_set_conf_state(rafttest_c_storage_t *storage,
                                      const raft_conf_state_t *state) {
    raft_conf_state_t copy;
    int result;

    if (storage == NULL || state == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    result = rafttest_c_conf_state_copy(&copy, state);
    if (result != RAFT_OK) {
        return result;
    }
    raft_conf_state_free(&storage->conf_state);
    storage->conf_state = copy;
    return RAFT_OK;
}

int rafttest_c_storage_append(rafttest_c_storage_t *storage,
                              const raft_entry_vec_t *incoming) {
    raft_entry_t *replacement;
    size_t incoming_offset = 0;
    size_t incoming_len;
    size_t prefix_len = 0;
    size_t total;
    size_t i;
    uint64_t first_new;
    uint64_t snapshot_index = 0;
    uint64_t last;
    int result;

    if (storage == NULL || incoming == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    if (incoming->len == 0) {
        return RAFT_OK;
    }
    if (incoming->items == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    for (i = 1; i < incoming->len; ++i) {
        if (incoming->items[i].index !=
            incoming->items[i - 1].index + UINT64_C(1)) {
            return RAFT_ERR_INVALID_ARGUMENT;
        }
    }
    if (storage->has_snapshot) {
        snapshot_index = storage->snapshot.metadata.index;
        while (incoming_offset < incoming->len &&
               incoming->items[incoming_offset].index <= snapshot_index) {
            ++incoming_offset;
        }
    }
    if (incoming_offset == incoming->len) {
        return RAFT_OK;
    }
    incoming_len = incoming->len - incoming_offset;
    first_new = incoming->items[incoming_offset].index;
    last = rafttest_c_storage_last_index(storage);
    if (first_new > last + UINT64_C(1)) {
        return RAFT_ERR_STORAGE_UNAVAILABLE;
    }
    while (prefix_len < storage->entries_len &&
           storage->entries[prefix_len].index < first_new) {
        ++prefix_len;
    }
    if (prefix_len > SIZE_MAX - incoming_len) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    total = prefix_len + incoming_len;
    replacement = calloc(total, sizeof(*replacement));
    if (replacement == NULL) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    for (i = 0; i < prefix_len; ++i) {
        result = rafttest_c_entry_copy(&replacement[i],
                                       &storage->entries[i]);
        if (result != RAFT_OK) {
            raft_entry_array_free(replacement, total);
            return result;
        }
    }
    for (i = 0; i < incoming_len; ++i) {
        result = rafttest_c_entry_copy(
            &replacement[prefix_len + i],
            &incoming->items[incoming_offset + i]);
        if (result != RAFT_OK) {
            raft_entry_array_free(replacement, total);
            return result;
        }
    }
    raft_entry_array_free(storage->entries, storage->entries_len);
    storage->entries = replacement;
    storage->entries_len = total;
    return RAFT_OK;
}

int rafttest_c_storage_apply_snapshot(rafttest_c_storage_t *storage,
                                      const raft_snapshot_t *snapshot) {
    raft_snapshot_t copy;
    raft_conf_state_t state;
    size_t keep = 0;
    size_t i;
    raft_entry_t *remaining = NULL;
    int result;

    if (storage == NULL || snapshot == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    if (storage->has_snapshot &&
        snapshot->metadata.index <= storage->snapshot.metadata.index) {
        return RAFT_ERR_STORAGE_COMPACTED;
    }
    result = rafttest_c_snapshot_copy(&copy, snapshot);
    if (result != RAFT_OK) {
        return result;
    }
    result = rafttest_c_conf_state_copy(&state,
                                        &snapshot->metadata.conf_state);
    if (result != RAFT_OK) {
        raft_snapshot_free(&copy);
        return result;
    }
    for (i = 0; i < storage->entries_len; ++i) {
        if (storage->entries[i].index > snapshot->metadata.index) {
            ++keep;
        }
    }
    if (keep != 0) {
        remaining = calloc(keep, sizeof(*remaining));
        if (remaining == NULL) {
            raft_snapshot_free(&copy);
            raft_conf_state_free(&state);
            return RAFT_ERR_OUT_OF_MEMORY;
        }
        keep = 0;
        for (i = 0; i < storage->entries_len; ++i) {
            if (storage->entries[i].index > snapshot->metadata.index) {
                result = rafttest_c_entry_copy(
                    &remaining[keep], &storage->entries[i]);
                if (result != RAFT_OK) {
                    raft_entry_array_free(remaining, keep + 1);
                    raft_snapshot_free(&copy);
                    raft_conf_state_free(&state);
                    return result;
                }
                ++keep;
            }
        }
    }
    raft_entry_array_free(storage->entries, storage->entries_len);
    raft_snapshot_free(&storage->snapshot);
    raft_conf_state_free(&storage->conf_state);
    storage->entries = remaining;
    storage->entries_len = keep;
    storage->snapshot = copy;
    storage->conf_state = state;
    storage->has_snapshot = true;
    return RAFT_OK;
}

int rafttest_c_storage_persist_ready(rafttest_c_storage_t *storage,
                                     const raft_ready_t *ready) {
    int result;

    if (storage == NULL || ready == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    result = rafttest_c_storage_append(storage, &ready->entries);
    if (result != RAFT_OK) {
        return result;
    }
    if (ready->has_hard_state) {
        storage->hard_state = ready->hard_state;
    }
    if (ready->has_snapshot) {
        result = rafttest_c_storage_apply_snapshot(storage,
                                                   &ready->snapshot);
        if (result != RAFT_OK) {
            return result;
        }
    }
    return RAFT_OK;
}
