CC ?= cc
CPPFLAGS ?=
CFLAGS ?= -O2
LDFLAGS ?=
TEST ?= all

BUILD_DIR ?= .build
C_DIR := ../c
C_BUILD_DIR ?= .build
C_LIBRARY := $(C_DIR)/$(C_BUILD_DIR)/libraft_c.a
WARNINGS := -Wall -Wextra -Werror -Wpedantic
STANDARD := -std=c11
SANITIZER_FLAGS := -O1 -g -fno-omit-frame-pointer -fsanitize=address,undefined
SANITIZER_BUILD_DIR ?= .build-sanitize

SOURCES := copy.c storage.c network.c node.c node_test.c
OBJECTS := $(SOURCES:%.c=$(BUILD_DIR)/%.o)
TEST_BINARY := $(BUILD_DIR)/node_test

.PHONY: all
all: $(TEST_BINARY)

$(BUILD_DIR):
	mkdir -p $@

.PHONY: c-library
c-library:
	$(MAKE) -C $(C_DIR) all BUILD_DIR='$(C_BUILD_DIR)'

$(BUILD_DIR)/%.o: %.c | $(BUILD_DIR)
	$(CC) $(CPPFLAGS) $(CFLAGS) $(WARNINGS) $(STANDARD) \
		-I$(C_DIR)/include -c $< -o $@

$(TEST_BINARY): $(OBJECTS) c-library
	$(CC) $(CFLAGS) $(LDFLAGS) $(OBJECTS) $(C_LIBRARY) -o $@

.PHONY: test
test: $(TEST_BINARY)
	$(TEST_BINARY) $(TEST)

.PHONY: test-sanitize
test-sanitize:
	$(MAKE) clean BUILD_DIR='$(SANITIZER_BUILD_DIR)'
	$(MAKE) -C $(C_DIR) clean BUILD_DIR='$(SANITIZER_BUILD_DIR)'
	ASAN_OPTIONS=detect_leaks=1:halt_on_error=1 \
	UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 \
	$(MAKE) test BUILD_DIR='$(SANITIZER_BUILD_DIR)' \
		C_BUILD_DIR='$(SANITIZER_BUILD_DIR)' \
		CFLAGS='$(SANITIZER_FLAGS)' LDFLAGS='$(SANITIZER_FLAGS)'

.PHONY: clean
clean:
	$(RM) -r $(BUILD_DIR)
