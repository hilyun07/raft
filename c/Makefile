CC ?= cc
AR ?= ar
CPPFLAGS ?=
CFLAGS ?= -O2

BUILD_DIR ?= .build
PUBLIC_INCLUDE := include
PRIVATE_INCLUDE := src
WARNINGS := -Wall -Wextra -Werror -Wpedantic
STANDARD := -std=c11

LIBRARY := $(BUILD_DIR)/libraft_c.a
OBJECTS := \
	$(BUILD_DIR)/unstable.o \
	$(BUILD_DIR)/log.o \
	$(BUILD_DIR)/raw_node.o
TEST_BINARIES := \
	$(BUILD_DIR)/raw_node_skeleton_test \
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

$(BUILD_DIR)/raw_node.o: src/raw_node.c src/raft_internal.h src/log.h src/unstable.h include/raft/raft.h | $(BUILD_DIR)
	$(CC) $(CPPFLAGS) $(CFLAGS) $(WARNINGS) $(STANDARD) \
		-I$(PUBLIC_INCLUDE) -I$(PRIVATE_INCLUDE) -c $< -o $@

$(LIBRARY): $(OBJECTS)
	$(AR) rcs $@ $(OBJECTS)

$(BUILD_DIR)/raw_node_skeleton_test: tests/raw_node_skeleton_test.c include/raft/raft.h $(LIBRARY)
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
	$(BUILD_DIR)/unstable_test
	$(BUILD_DIR)/log_test

.PHONY: clean
clean:
	$(RM) -r $(BUILD_DIR)
