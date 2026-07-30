CC ?= cc
AR ?= ar
CPPFLAGS ?=
CFLAGS ?= -O2

BUILD_DIR ?= .build
PUBLIC_INCLUDE := include
PRIVATE_INCLUDE := src
WARNINGS := -Wall -Wextra -Werror -Wpedantic
STANDARD := -std=c11
SANITIZER_CFLAGS ?= -O1 -g -fno-omit-frame-pointer -fsanitize=address,undefined

LIBRARY := $(BUILD_DIR)/libraft_c.a
OBJECTS := \
	$(BUILD_DIR)/unstable.o \
	$(BUILD_DIR)/log.o \
	$(BUILD_DIR)/tracker.o \
	$(BUILD_DIR)/confchange.o \
	$(BUILD_DIR)/read_only.o \
	$(BUILD_DIR)/raft_core.o \
	$(BUILD_DIR)/raw_node.o
TEST_BINARIES := \
	$(BUILD_DIR)/raw_node_skeleton_test \
	$(BUILD_DIR)/raft_core_test \
	$(BUILD_DIR)/snapshot_test \
	$(BUILD_DIR)/read_only_test \
	$(BUILD_DIR)/tracker_test \
	$(BUILD_DIR)/confchange_test \
	$(BUILD_DIR)/unstable_test \
	$(BUILD_DIR)/log_test

.PHONY: all
all: $(LIBRARY)

$(BUILD_DIR):
	mkdir -p $@

$(BUILD_DIR)/unstable.o: src/unstable.c src/unstable.h include/raft/raft.h | $(BUILD_DIR)
	$(CC) $(CPPFLAGS) $(CFLAGS) $(WARNINGS) $(STANDARD) \
		-I$(PUBLIC_INCLUDE) -I$(PRIVATE_INCLUDE) -c $< -o $@

$(BUILD_DIR)/log.o: src/log.c src/log.h src/unstable.h include/raft/raft.h | $(BUILD_DIR)
	$(CC) $(CPPFLAGS) $(CFLAGS) $(WARNINGS) $(STANDARD) \
		-I$(PUBLIC_INCLUDE) -I$(PRIVATE_INCLUDE) -c $< -o $@

$(BUILD_DIR)/tracker.o: src/tracker.c src/tracker.h include/raft/raft.h | $(BUILD_DIR)
	$(CC) $(CPPFLAGS) $(CFLAGS) $(WARNINGS) $(STANDARD) \
		-I$(PUBLIC_INCLUDE) -I$(PRIVATE_INCLUDE) -c $< -o $@

$(BUILD_DIR)/confchange.o: src/confchange.c src/confchange.h src/tracker.h include/raft/raft.h | $(BUILD_DIR)
	$(CC) $(CPPFLAGS) $(CFLAGS) $(WARNINGS) $(STANDARD) \
		-I$(PUBLIC_INCLUDE) -I$(PRIVATE_INCLUDE) -c $< -o $@

$(BUILD_DIR)/read_only.o: src/read_only.c src/read_only.h src/tracker.h include/raft/raft.h | $(BUILD_DIR)
	$(CC) $(CPPFLAGS) $(CFLAGS) $(WARNINGS) $(STANDARD) \
		-I$(PUBLIC_INCLUDE) -I$(PRIVATE_INCLUDE) -c $< -o $@

$(BUILD_DIR)/raft_core.o: src/raft_core.c src/raft_core.h src/read_only.h src/confchange.h src/tracker.h src/log.h src/unstable.h include/raft/raft.h | $(BUILD_DIR)
	$(CC) $(CPPFLAGS) $(CFLAGS) $(WARNINGS) $(STANDARD) \
		-I$(PUBLIC_INCLUDE) -I$(PRIVATE_INCLUDE) -c $< -o $@

$(BUILD_DIR)/raw_node.o: src/raw_node.c src/raft_internal.h src/raft_core.h src/read_only.h src/confchange.h src/tracker.h src/log.h src/unstable.h include/raft/raft.h | $(BUILD_DIR)
	$(CC) $(CPPFLAGS) $(CFLAGS) $(WARNINGS) $(STANDARD) \
		-I$(PUBLIC_INCLUDE) -I$(PRIVATE_INCLUDE) -c $< -o $@

$(LIBRARY): $(OBJECTS)
	$(AR) rcs $@ $(OBJECTS)

$(BUILD_DIR)/raw_node_skeleton_test: tests/raw_node_skeleton_test.c include/raft/raft.h $(LIBRARY)
	$(CC) $(CPPFLAGS) $(CFLAGS) $(WARNINGS) $(STANDARD) \
		-I$(PUBLIC_INCLUDE) -I$(PRIVATE_INCLUDE) $< $(LIBRARY) -o $@

$(BUILD_DIR)/raft_core_test: tests/raft_core_test.c include/raft/raft.h $(LIBRARY)
	$(CC) $(CPPFLAGS) $(CFLAGS) $(WARNINGS) $(STANDARD) \
		-I$(PUBLIC_INCLUDE) -I$(PRIVATE_INCLUDE) $< $(LIBRARY) -o $@

$(BUILD_DIR)/snapshot_test: tests/snapshot_test.c include/raft/raft.h $(LIBRARY)
	$(CC) $(CPPFLAGS) $(CFLAGS) $(WARNINGS) $(STANDARD) \
		-I$(PUBLIC_INCLUDE) -I$(PRIVATE_INCLUDE) $< $(LIBRARY) -o $@

$(BUILD_DIR)/read_only_test: tests/read_only_test.c src/read_only.h src/tracker.h include/raft/raft.h $(LIBRARY)
	$(CC) $(CPPFLAGS) $(CFLAGS) $(WARNINGS) $(STANDARD) \
		-I$(PUBLIC_INCLUDE) -I$(PRIVATE_INCLUDE) $< $(LIBRARY) -o $@

$(BUILD_DIR)/tracker_test: tests/tracker_test.c src/tracker.h include/raft/raft.h $(LIBRARY)
	$(CC) $(CPPFLAGS) $(CFLAGS) $(WARNINGS) $(STANDARD) \
		-I$(PUBLIC_INCLUDE) -I$(PRIVATE_INCLUDE) $< $(LIBRARY) -o $@

$(BUILD_DIR)/confchange_test: tests/confchange_test.c src/confchange.h src/tracker.h include/raft/raft.h $(LIBRARY)
	$(CC) $(CPPFLAGS) $(CFLAGS) $(WARNINGS) $(STANDARD) \
		-I$(PUBLIC_INCLUDE) -I$(PRIVATE_INCLUDE) $< $(LIBRARY) -o $@

$(BUILD_DIR)/unstable_test: tests/unstable_test.c src/unstable.h include/raft/raft.h $(LIBRARY)
	$(CC) $(CPPFLAGS) $(CFLAGS) $(WARNINGS) $(STANDARD) \
		-I$(PUBLIC_INCLUDE) -I$(PRIVATE_INCLUDE) $< $(LIBRARY) -o $@

$(BUILD_DIR)/log_test: tests/log_test.c src/log.h src/unstable.h include/raft/raft.h $(LIBRARY)
	$(CC) $(CPPFLAGS) $(CFLAGS) $(WARNINGS) $(STANDARD) \
		-I$(PUBLIC_INCLUDE) -I$(PRIVATE_INCLUDE) $< $(LIBRARY) -o $@

.PHONY: test
test: $(TEST_BINARIES)
	$(BUILD_DIR)/raw_node_skeleton_test
	$(BUILD_DIR)/raft_core_test
	$(BUILD_DIR)/snapshot_test
	$(BUILD_DIR)/read_only_test
	$(BUILD_DIR)/tracker_test
	$(BUILD_DIR)/confchange_test
	$(BUILD_DIR)/unstable_test
	$(BUILD_DIR)/log_test

.PHONY: test-sanitize
test-sanitize:
	$(MAKE) clean
	ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 \
	UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 \
	$(MAKE) test CFLAGS='$(SANITIZER_CFLAGS)'

.PHONY: clean
clean:
	$(RM) -r $(BUILD_DIR)
