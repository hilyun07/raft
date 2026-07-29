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
OBJECT := $(BUILD_DIR)/raw_node.o
TEST_BINARY := $(BUILD_DIR)/raw_node_skeleton_test

.PHONY: all
all: $(LIBRARY)

$(BUILD_DIR):
	mkdir -p $@

$(OBJECT): src/raw_node.c src/raft_internal.h include/raft/raft.h | $(BUILD_DIR)
	$(CC) $(CPPFLAGS) $(CFLAGS) $(WARNINGS) $(STANDARD) \
		-I$(PUBLIC_INCLUDE) -I$(PRIVATE_INCLUDE) -c $< -o $@

$(LIBRARY): $(OBJECT)
	$(AR) rcs $@ $<

$(TEST_BINARY): tests/raw_node_skeleton_test.c include/raft/raft.h $(LIBRARY)
	$(CC) $(CPPFLAGS) $(CFLAGS) $(WARNINGS) $(STANDARD) \
		-I$(PUBLIC_INCLUDE) $< $(LIBRARY) -o $@

.PHONY: test
test: $(TEST_BINARY)
	$(TEST_BINARY)

.PHONY: clean
clean:
	$(RM) -r $(BUILD_DIR)
