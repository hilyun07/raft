#include "network.h"

#include "copy.h"

#include <stdlib.h>
#include <string.h>

struct rafttest_c_network_item {
    raft_message_t message;
    uint64_t due;
    uint64_t sequence;
    rafttest_c_network_item_t *next;
};

static bool valid_id(uint64_t id) {
    return id > 0 && id <= RAFTTEST_C_MAX_NODES;
}

static uint64_t random_next(rafttest_c_network_t *network) {
    uint64_t value = network->random_state;

    value ^= value << 13;
    value ^= value >> 7;
    value ^= value << 17;
    network->random_state = value;
    return value;
}

void rafttest_c_network_init(rafttest_c_network_t *network,
                             size_t max_len,
                             uint64_t seed,
                             uint32_t max_delay_ticks) {
    memset(network, 0, sizeof(*network));
    network->max_len = max_len;
    network->random_state = seed == 0 ? UINT64_C(1) : seed;
    network->max_delay_ticks = max_delay_ticks;
}

void rafttest_c_network_close(rafttest_c_network_t *network) {
    rafttest_c_network_item_t *item;

    if (network == NULL) {
        return;
    }
    while (network->head != NULL) {
        item = network->head;
        network->head = item->next;
        raft_message_free(&item->message);
        free(item);
    }
    memset(network, 0, sizeof(*network));
}

void rafttest_c_network_connect(rafttest_c_network_t *network,
                                uint64_t id) {
    if (network != NULL && valid_id(id)) {
        network->connected[id] = true;
    }
}

void rafttest_c_network_disconnect(rafttest_c_network_t *network,
                                   uint64_t id) {
    rafttest_c_network_item_t **link;
    rafttest_c_network_item_t *item;

    if (network == NULL || !valid_id(id)) {
        return;
    }
    network->connected[id] = false;
    network->paused[id] = false;
    link = &network->head;
    network->tail = NULL;
    while (*link != NULL) {
        item = *link;
        if (item->message.to == id || item->message.from == id) {
            *link = item->next;
            raft_message_free(&item->message);
            free(item);
            --network->len;
            continue;
        }
        network->tail = item;
        link = &item->next;
    }
}

void rafttest_c_network_pause(rafttest_c_network_t *network,
                              uint64_t id,
                              bool paused) {
    if (network != NULL && valid_id(id)) {
        network->paused[id] = paused;
    }
}

int rafttest_c_network_enqueue(rafttest_c_network_t *network,
                               const raft_message_t *message,
                               uint64_t now) {
    rafttest_c_network_item_t *item;
    uint64_t delay;
    int result;

    if (network == NULL || message == NULL || !valid_id(message->to) ||
        !valid_id(message->from)) {
        return RAFT_ERR_INVALID_ARGUMENT;
    }
    if (!network->connected[message->to] ||
        !network->connected[message->from]) {
        return RAFT_OK;
    }
    if (network->len >= network->max_len) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    item = calloc(1, sizeof(*item));
    if (item == NULL) {
        return RAFT_ERR_OUT_OF_MEMORY;
    }
    result = rafttest_c_message_copy(&item->message, message);
    if (result != RAFT_OK) {
        free(item);
        return result;
    }
    delay = network->max_delay_ticks == 0
                ? 0
                : random_next(network) %
                      ((uint64_t)network->max_delay_ticks + UINT64_C(1));
    item->due = now + delay;
    if (item->due < network->last_due[message->to]) {
        item->due = network->last_due[message->to];
    }
    network->last_due[message->to] = item->due;
    item->sequence = ++network->sequence;
    if (network->tail == NULL) {
        network->head = item;
    } else {
        network->tail->next = item;
    }
    network->tail = item;
    ++network->len;
    return RAFT_OK;
}

int rafttest_c_network_take(rafttest_c_network_t *network,
                            uint64_t now,
                            raft_message_t *message) {
    rafttest_c_network_item_t **link;
    rafttest_c_network_item_t *item;

    if (network == NULL || message == NULL) {
        return -RAFT_ERR_INVALID_ARGUMENT;
    }
    memset(message, 0, sizeof(*message));
    link = &network->head;
    while (*link != NULL) {
        item = *link;
        if (item->due <= now && network->connected[item->message.to] &&
            !network->paused[item->message.to]) {
            *link = item->next;
            if (network->tail == item) {
                network->tail = NULL;
                if (network->head != NULL) {
                    rafttest_c_network_item_t *tail = network->head;
                    while (tail->next != NULL) {
                        tail = tail->next;
                    }
                    network->tail = tail;
                }
            }
            *message = item->message;
            memset(&item->message, 0, sizeof(item->message));
            free(item);
            --network->len;
            return 1;
        }
        link = &item->next;
    }
    return 0;
}
