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

#include "read_only.h"

#include <stdlib.h>
#include <string.h>

static uint64_t read_only_get_le64(const uint8_t *data) {
    uint64_t value = 0;
    size_t i;
    for (i = 0; i < 8; ++i) {
        value |= (uint64_t)data[i] << (8U * i);
    }
    return value;
}

static uint64_t read_only_ack_index(
    const raft_read_only_internal_t *read_only,
    uint64_t id) {
    size_t i;
    for (i = 0; i < read_only->ack_len; ++i) {
        if (read_only->acks[i].id == id) {
            return read_only->acks[i].index;
        }
    }
    return 0;
}

static uint64_t read_only_majority_confirmed(
    const raft_read_only_internal_t *read_only,
    const raft_uint64_vec_t *voters) {
    uint64_t confirmed = 0;
    size_t quorum;
    size_t i;

    if (voters->len == 0) {
        return UINT64_MAX;
    }
    quorum = voters->len / 2 + 1;
    for (i = 0; i < voters->len; ++i) {
        uint64_t candidate =
            read_only_ack_index(read_only, voters->items[i]);
        size_t acknowledged = 0;
        size_t j;

        if (candidate <= confirmed) {
            continue;
        }
        for (j = 0; j < voters->len; ++j) {
            if (read_only_ack_index(
                    read_only, voters->items[j]) >= candidate) {
                ++acknowledged;
            }
        }
        if (acknowledged >= quorum) {
            confirmed = candidate;
        }
    }
    return confirmed;
}

int raft_read_only_init(raft_read_only_internal_t *read_only,
                        raft_read_only_option_t option) {
    if (read_only == NULL ||
        (option != RAFT_READ_ONLY_SAFE &&
         option != RAFT_READ_ONLY_LEASE_BASED)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(read_only, 0, sizeof(*read_only));
    read_only->option = option;
    return RAFT_OK;
}

void raft_read_only_reset(raft_read_only_internal_t *read_only) {
    raft_read_only_option_t option;
    size_t i;

    if (read_only == NULL) {
        return;
    }
    option = read_only->option;
    for (i = 0; i < read_only->request_len; ++i) {
        raft_message_free(&read_only->requests[i].request);
    }
    free(read_only->requests);
    free(read_only->acks);
    memset(read_only, 0, sizeof(*read_only));
    read_only->option = option;
}

void raft_read_only_free(raft_read_only_internal_t *read_only) {
    if (read_only == NULL) {
        return;
    }
    raft_read_only_reset(read_only);
    memset(read_only, 0, sizeof(*read_only));
}

int raft_read_only_add_request(raft_read_only_internal_t *read_only,
                               uint64_t commit_index,
                               const raft_message_view_t *request) {
    raft_read_index_request_internal_t owned;
    raft_read_index_request_internal_t *items;
    int result;

    if (read_only == NULL || request == NULL ||
        request->type != RAFT_MSG_READ_INDEX ||
        request->entries.len != 1 ||
        request->entries.items == NULL) {
        return RAFT_ERR_FATAL;
    }
    if (read_only->request_len == SIZE_MAX ||
        read_only->request_len + 1 >
            SIZE_MAX / sizeof(*read_only->requests) ||
        read_only->confirmed_reads == UINT64_MAX ||
        (uint64_t)read_only->request_len >=
            UINT64_MAX - read_only->confirmed_reads) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    memset(&owned, 0, sizeof(owned));
    result = raft_message_copy_from_view(&owned.request, request);
    if (result != RAFT_OK) {
        return result;
    }
    owned.index = commit_index;
    items = realloc(
        read_only->requests,
        (read_only->request_len + 1) * sizeof(*items));
    if (items == NULL) {
        raft_message_free(&owned.request);
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    read_only->requests = items;
    read_only->requests[read_only->request_len] = owned;
    ++read_only->request_len;
    return RAFT_OK;
}

int raft_read_only_recv_ack(raft_read_only_internal_t *read_only,
                            uint64_t from,
                            const raft_byte_view_t *context) {
    raft_read_ack_internal_t *items;
    uint64_t index;
    size_t i;

    if (read_only == NULL || context == NULL ||
        !raft_byte_view_valid(context)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    if (context->len == 0) {
        return RAFT_OK;
    }
    if (context->len < 8) {
        return RAFT_ERR_FATAL;
    }
    index = read_only_get_le64(context->data);
    for (i = 0; i < read_only->ack_len; ++i) {
        if (read_only->acks[i].id == from) {
            if (read_only->acks[i].index < index) {
                read_only->acks[i].index = index;
            }
            return RAFT_OK;
        }
    }
    if (read_only->ack_len == SIZE_MAX ||
        read_only->ack_len + 1 >
            SIZE_MAX / sizeof(*read_only->acks)) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    items = realloc(
        read_only->acks,
        (read_only->ack_len + 1) * sizeof(*items));
    if (items == NULL) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    read_only->acks = items;
    read_only->acks[read_only->ack_len].id = from;
    read_only->acks[read_only->ack_len].index = index;
    ++read_only->ack_len;
    return RAFT_OK;
}

bool raft_read_only_heartbeat_position(
    const raft_read_only_internal_t *read_only,
    uint64_t *position) {
    if (read_only == NULL || position == NULL ||
        read_only->request_len == 0) {
        return false;
    }
    if ((uint64_t)read_only->request_len >
        UINT64_MAX - read_only->confirmed_reads) {
        return false;
    }
    *position =
        read_only->confirmed_reads + (uint64_t)read_only->request_len;
    return true;
}

int raft_read_only_confirmed(
    const raft_read_only_internal_t *read_only,
    const raft_progress_tracker_t *tracker,
    size_t *count,
    uint64_t *new_confirmed_reads) {
    uint64_t incoming;
    uint64_t outgoing;
    uint64_t confirmed;
    uint64_t delta;

    if (read_only == NULL || tracker == NULL || count == NULL ||
        new_confirmed_reads == NULL) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    *count = 0;
    *new_confirmed_reads = read_only->confirmed_reads;
    incoming = read_only_majority_confirmed(
        read_only, &tracker->config.voters);
    outgoing = read_only_majority_confirmed(
        read_only, &tracker->config.voters_outgoing);
    confirmed = incoming < outgoing ? incoming : outgoing;
    if (confirmed <= read_only->confirmed_reads) {
        return RAFT_OK;
    }
    delta = confirmed - read_only->confirmed_reads;
    if (delta > (uint64_t)read_only->request_len ||
        delta > SIZE_MAX) {
        return RAFT_ERR_FATAL;
    }
    *count = (size_t)delta;
    *new_confirmed_reads = confirmed;
    return RAFT_OK;
}

const raft_read_index_request_internal_t *raft_read_only_request_at(
    const raft_read_only_internal_t *read_only,
    size_t at) {
    if (read_only == NULL || at >= read_only->request_len) {
        return NULL;
    }
    return &read_only->requests[at];
}

void raft_read_only_advance(raft_read_only_internal_t *read_only,
                            size_t count,
                            uint64_t new_confirmed_reads) {
    size_t i;

    if (read_only == NULL || count > read_only->request_len ||
        new_confirmed_reads < read_only->confirmed_reads ||
        new_confirmed_reads - read_only->confirmed_reads !=
            (uint64_t)count) {
        return;
    }
    for (i = 0; i < count; ++i) {
        raft_message_free(&read_only->requests[i].request);
    }
    if (count < read_only->request_len) {
        memmove(read_only->requests,
                &read_only->requests[count],
                (read_only->request_len - count) *
                    sizeof(*read_only->requests));
    }
    read_only->request_len -= count;
    if (read_only->request_len == 0) {
        free(read_only->requests);
        read_only->requests = NULL;
    }
    read_only->confirmed_reads = new_confirmed_reads;
}
