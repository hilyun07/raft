#include "node.h"

#include <stdio.h>
#include <string.h>

#define NODE_COUNT 5
#define LEADER_TICKS 200
#define COMMIT_TICKS 500

static const uint8_t proposal[] = "somedata";

static int check_result(rafttest_c_cluster_t *cluster,
                        int result,
                        const char *operation) {
    if (result == RAFT_OK) {
        return 0;
    }
    fprintf(stderr, "%s failed: raft error %d\n", operation, result);
    rafttest_c_cluster_dump(cluster);
    return 1;
}

static int propose_many(rafttest_c_cluster_t *cluster,
                        size_t node_index,
                        size_t count) {
    size_t i;

    for (i = 0; i < count; ++i) {
        int result = rafttest_c_cluster_propose(
            cluster, node_index, proposal, sizeof(proposal) - 1);
        if (check_result(cluster, result, "propose") != 0) {
            return 1;
        }
    }
    for (i = 0; i < 3; ++i) {
        int result = rafttest_c_cluster_tick(cluster);
        if (check_result(cluster, result, "tick after proposal batch") != 0) {
            return 1;
        }
    }
    return 0;
}

static int test_basic_progress(void) {
    rafttest_c_cluster_t cluster;
    size_t leader = 0;
    int result;

    result = rafttest_c_cluster_init(&cluster, NODE_COUNT);
    if (check_result(&cluster, result, "cluster init") != 0) {
        rafttest_c_cluster_close(&cluster);
        return 1;
    }
    result = rafttest_c_cluster_wait_leader(&cluster, LEADER_TICKS,
                                            &leader);
    if (check_result(&cluster, result, "wait leader") != 0 ||
        propose_many(&cluster, 0, 100) != 0) {
        rafttest_c_cluster_close(&cluster);
        return 1;
    }
    result = rafttest_c_cluster_wait_commit(&cluster, 100, COMMIT_TICKS);
    if (check_result(&cluster, result, "wait commit") != 0) {
        rafttest_c_cluster_close(&cluster);
        return 1;
    }
    printf("basic_progress: leader=%zu passed\n", leader + 1);
    rafttest_c_cluster_close(&cluster);
    return 0;
}

static int test_restart(void) {
    rafttest_c_cluster_t cluster;
    size_t leader = 0;
    size_t first;
    size_t second;
    int result;

    result = rafttest_c_cluster_init(&cluster, NODE_COUNT);
    if (check_result(&cluster, result, "cluster init") != 0) {
        rafttest_c_cluster_close(&cluster);
        return 1;
    }
    result = rafttest_c_cluster_wait_leader(&cluster, LEADER_TICKS,
                                            &leader);
    if (check_result(&cluster, result, "wait leader") != 0) {
        rafttest_c_cluster_close(&cluster);
        return 1;
    }
    first = (leader + 1) % NODE_COUNT;
    second = (leader + 2) % NODE_COUNT;
    if (propose_many(&cluster, leader, 30) != 0 ||
        check_result(&cluster, rafttest_c_cluster_stop(&cluster, first),
                     "stop first follower") != 0 ||
        propose_many(&cluster, (leader + 3) % NODE_COUNT, 30) != 0 ||
        check_result(&cluster, rafttest_c_cluster_stop(&cluster, second),
                     "stop second follower") != 0 ||
        propose_many(&cluster, (leader + 4) % NODE_COUNT, 30) != 0 ||
        check_result(&cluster,
                     rafttest_c_cluster_restart(&cluster, second),
                     "restart second follower") != 0 ||
        propose_many(&cluster, leader, 30) != 0 ||
        check_result(&cluster,
                     rafttest_c_cluster_restart(&cluster, first),
                     "restart first follower") != 0) {
        rafttest_c_cluster_close(&cluster);
        return 1;
    }
    result = rafttest_c_cluster_wait_commit(&cluster, 120, COMMIT_TICKS);
    if (check_result(&cluster, result, "wait commit after restart") != 0) {
        rafttest_c_cluster_close(&cluster);
        return 1;
    }
    printf("restart: leader=%zu restarted=%zu,%zu passed\n",
           leader + 1, first + 1, second + 1);
    rafttest_c_cluster_close(&cluster);
    return 0;
}

static int test_pause(void) {
    rafttest_c_cluster_t cluster;
    size_t leader = 0;
    int result;

    result = rafttest_c_cluster_init(&cluster, NODE_COUNT);
    if (check_result(&cluster, result, "cluster init") != 0) {
        rafttest_c_cluster_close(&cluster);
        return 1;
    }
    result = rafttest_c_cluster_wait_leader(&cluster, LEADER_TICKS,
                                            &leader);
    if (check_result(&cluster, result, "wait leader") != 0 ||
        propose_many(&cluster, 0, 30) != 0 ||
        check_result(&cluster, rafttest_c_cluster_pause(&cluster, 1, true),
                     "pause node 2") != 0 ||
        propose_many(&cluster, 0, 30) != 0 ||
        check_result(&cluster, rafttest_c_cluster_pause(&cluster, 2, true),
                     "pause node 3") != 0 ||
        propose_many(&cluster, 0, 30) != 0 ||
        check_result(&cluster, rafttest_c_cluster_pause(&cluster, 2, false),
                     "resume node 3") != 0 ||
        propose_many(&cluster, 0, 30) != 0 ||
        check_result(&cluster, rafttest_c_cluster_pause(&cluster, 1, false),
                     "resume node 2") != 0) {
        rafttest_c_cluster_close(&cluster);
        return 1;
    }
    result = rafttest_c_cluster_wait_commit(&cluster, 120, COMMIT_TICKS);
    if (check_result(&cluster, result, "wait commit after pause") != 0) {
        rafttest_c_cluster_close(&cluster);
        return 1;
    }
    printf("pause: initial-leader=%zu passed\n", leader + 1);
    rafttest_c_cluster_close(&cluster);
    return 0;
}

static bool selected(const char *selection, const char *name) {
    return strcmp(selection, "all") == 0 || strcmp(selection, name) == 0;
}

int main(int argc, char **argv) {
    const char *selection = argc > 1 ? argv[1] : "all";
    int failed = 0;
    bool ran = false;

    if (selected(selection, "basic_progress")) {
        ran = true;
        failed |= test_basic_progress();
    }
    if (selected(selection, "restart")) {
        ran = true;
        failed |= test_restart();
    }
    if (selected(selection, "pause")) {
        ran = true;
        failed |= test_pause();
    }
    if (!ran) {
        fprintf(stderr,
                "unknown test '%s' (use all, basic_progress, restart, pause)\n",
                selection);
        return 2;
    }
    if (failed != 0) {
        return 1;
    }
    printf("rafttest_c: all selected tests passed\n");
    return 0;
}
