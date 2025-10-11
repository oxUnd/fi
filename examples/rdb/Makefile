# FI Relational Database Makefile

# Compiler and flags
CC = gcc
# CFLAGS = -Wall -Wextra -std=c99 -g -O2 -pthread
CFLAGS = -Wall -Wextra -std=c99 -g -pthread
INCLUDES = -I../../src/include -I.
LDFLAGS = -L../../src/.libs -static -lfi -lm -pthread

# Directories
SRC_DIR = .
BUILD_DIR = build
LIB_DIR = ../../src

# Source files
RDB_SOURCES = rdb.c sql_parser.c cache_system.c persistence.c cached_rdb.c
DEMO_SOURCES = rdb_demo.c multi_table_demo.c thread_safe_demo.c thread_safety_test.c interactive_sql.c cached_rdb_demo.c test_persistence.c simple_test.c
ALL_SOURCES = $(RDB_SOURCES) $(DEMO_SOURCES)

# Object files
RDB_OBJECTS = $(RDB_SOURCES:%.c=$(BUILD_DIR)/%.o)
DEMO_OBJECTS = $(DEMO_SOURCES:%.c=$(BUILD_DIR)/%.o)

# Executables
RDB_DEMO = $(BUILD_DIR)/rdb_demo
MULTI_TABLE_DEMO = $(BUILD_DIR)/multi_table_demo
THREAD_SAFE_DEMO = $(BUILD_DIR)/thread_safe_demo
THREAD_SAFETY_TEST = $(BUILD_DIR)/thread_safety_test
INTERACTIVE_SQL = $(BUILD_DIR)/interactive_sql
CACHED_RDB_DEMO = $(BUILD_DIR)/cached_rdb_demo
TEST_PERSISTENCE = $(BUILD_DIR)/test_persistence
SIMPLE_TEST = $(BUILD_DIR)/simple_test
RDB_LIB = $(BUILD_DIR)/librdb.a

# Default target
all: $(RDB_DEMO) $(MULTI_TABLE_DEMO) $(THREAD_SAFE_DEMO) $(THREAD_SAFETY_TEST) $(INTERACTIVE_SQL) $(CACHED_RDB_DEMO) $(TEST_PERSISTENCE) $(SIMPLE_TEST)

# Create build directory
$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

# Build the demo executable
$(RDB_DEMO): $(BUILD_DIR) $(RDB_OBJECTS) $(BUILD_DIR)/rdb_demo.o
	$(CC) $(BUILD_DIR)/rdb_demo.o $(RDB_OBJECTS) $(LDFLAGS) -o $@

# Build the multi-table demo executable
$(MULTI_TABLE_DEMO): $(BUILD_DIR) $(RDB_OBJECTS) $(BUILD_DIR)/multi_table_demo.o
	$(CC) $(BUILD_DIR)/multi_table_demo.o $(RDB_OBJECTS) $(LDFLAGS) -o $@

# Build the thread-safe demo executable
$(THREAD_SAFE_DEMO): $(BUILD_DIR) $(RDB_OBJECTS) $(BUILD_DIR)/thread_safe_demo.o
	$(CC) $(BUILD_DIR)/thread_safe_demo.o $(RDB_OBJECTS) $(LDFLAGS) -o $@

# Build the thread safety test executable
$(THREAD_SAFETY_TEST): $(BUILD_DIR) $(RDB_OBJECTS) $(BUILD_DIR)/thread_safety_test.o
	$(CC) $(BUILD_DIR)/thread_safety_test.o $(RDB_OBJECTS) $(LDFLAGS) -o $@

# Build the interactive SQL executable
$(INTERACTIVE_SQL): $(BUILD_DIR) $(RDB_OBJECTS) $(BUILD_DIR)/interactive_sql.o
	$(CC) $(BUILD_DIR)/interactive_sql.o $(RDB_OBJECTS) $(LDFLAGS) -o $@

# Build the cached RDB demo executable
$(CACHED_RDB_DEMO): $(BUILD_DIR) $(RDB_OBJECTS) $(BUILD_DIR)/cached_rdb_demo.o
	$(CC) $(BUILD_DIR)/cached_rdb_demo.o $(RDB_OBJECTS) $(LDFLAGS) -o $@

# Build the persistence test executable
$(TEST_PERSISTENCE): $(BUILD_DIR) $(RDB_OBJECTS) $(BUILD_DIR)/test_persistence.o
	$(CC) $(BUILD_DIR)/test_persistence.o $(RDB_OBJECTS) $(LDFLAGS) -o $@

# Build the simple test executable
$(SIMPLE_TEST): $(BUILD_DIR) $(RDB_OBJECTS) $(BUILD_DIR)/simple_test.o
	$(CC) $(BUILD_DIR)/simple_test.o $(RDB_OBJECTS) $(LDFLAGS) -o $@

# Build static library
$(RDB_LIB): $(BUILD_DIR) $(RDB_OBJECTS)
	ar rcs $@ $(RDB_OBJECTS)

# Compile object files
$(BUILD_DIR)/%.o: $(SRC_DIR)/%.c
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $@

# Run the demo
run: $(RDB_DEMO)
	./$(RDB_DEMO)

# Run the multi-table demo
run-multi: $(MULTI_TABLE_DEMO)
	./$(MULTI_TABLE_DEMO)

# Run the thread-safe demo
run-thread-safe: $(THREAD_SAFE_DEMO)
	./$(THREAD_SAFE_DEMO)

# Run the thread safety test
test-thread-safety: $(THREAD_SAFETY_TEST)
	./$(THREAD_SAFETY_TEST)

# Run the interactive SQL client
run-interactive: $(INTERACTIVE_SQL)
	./$(INTERACTIVE_SQL)

# Run the cached RDB demo
run-cached: $(CACHED_RDB_DEMO)
	./$(CACHED_RDB_DEMO)

# Run the persistence test
test-persistence: $(TEST_PERSISTENCE)
	./$(TEST_PERSISTENCE)

# Run the simple test
test-simple: $(SIMPLE_TEST)
	./$(SIMPLE_TEST)

# Run with valgrind for memory checking
valgrind: $(RDB_DEMO)
	valgrind --leak-check=full --show-leak-kinds=all ./$(RDB_DEMO)

# Clean build files
clean:
	rm -rf $(BUILD_DIR)

# Install (copy library and headers)
install: $(RDB_LIB)
	@echo "Installing RDB library..."
	@mkdir -p /usr/local/include/fi
	@mkdir -p /usr/local/lib
	cp rdb.h sql_parser.h /usr/local/include/fi/
	cp $(RDB_LIB) /usr/local/lib/
	@echo "Installation complete!"

# Uninstall
uninstall:
	@echo "Uninstalling RDB library..."
	rm -f /usr/local/include/fi/rdb.h
	rm -f /usr/local/include/fi/sql_parser.h
	rm -f /usr/local/lib/librdb.a
	@echo "Uninstallation complete!"

# Show help
help:
	@echo "Available targets:"
	@echo "  all                - Build all RDB demos and tests (default)"
	@echo "  run                - Build and run the basic demo"
	@echo "  run-multi          - Build and run the multi-table demo"
	@echo "  run-thread-safe    - Build and run the thread-safe demo"
	@echo "  test-thread-safety - Build and run the thread safety test"
	@echo "  run-interactive    - Build and run the interactive SQL client"
	@echo "  valgrind           - Run basic demo with valgrind memory checker"
	@echo "  lib                - Build static library (librdb.a)"
	@echo "  clean              - Remove build files"
	@echo "  install            - Install library and headers to system"
	@echo "  uninstall          - Remove installed files"
	@echo "  help               - Show this help message"

# Dependencies
$(BUILD_DIR)/rdb.o: rdb.h
$(BUILD_DIR)/sql_parser.o: sql_parser.h rdb.h
$(BUILD_DIR)/rdb_demo.o: rdb.h sql_parser.h
$(BUILD_DIR)/multi_table_demo.o: rdb.h sql_parser.h
$(BUILD_DIR)/thread_safe_demo.o: rdb.h sql_parser.h
$(BUILD_DIR)/thread_safety_test.o: rdb.h sql_parser.h
$(BUILD_DIR)/interactive_sql.o: rdb.h sql_parser.h

# Phony targets
.PHONY: all run run-multi run-thread-safe test-thread-safety run-interactive test-persistence test-simple valgrind clean install uninstall help

# Ensure FI library is built
check-fi-lib:
	@if [ ! -f "$(LIB_DIR)/.libs/libfi.a" ] && [ ! -f "$(LIB_DIR)/.libs/libfi.so" ]; then \
		echo "Error: FI library not found. Please build it first:"; \
		echo "  cd ../../src && make"; \
		exit 1; \
	fi

# Make all depend on FI library check
all: check-fi-lib
