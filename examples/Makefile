# Makefile for FI library examples
# 编译所有示例程序

# 编译器设置
CC = gcc
CFLAGS = -Wall -Wextra -std=c99 -g -O2
INCLUDES = -I../src/include
LIBS = -L../src/.libs -lfi

# 示例程序列表
EXAMPLES = array_basic map_simple btree_basic student_management

# 默认目标：编译所有示例
all: $(EXAMPLES)

# 数组基本操作示例
array_basic: array_basic.c
	$(CC) $(CFLAGS) $(INCLUDES) -o $@ $< $(LIBS)

# 映射基本操作示例
map_simple: map_simple.c
	$(CC) $(CFLAGS) $(INCLUDES) -o $@ $< $(LIBS)

# 二叉搜索树基本操作示例
btree_basic: btree_basic.c
	$(CC) $(CFLAGS) $(INCLUDES) -o $@ $< $(LIBS)

# 学生管理系统示例
student_management: student_management.c
	$(CC) $(CFLAGS) $(INCLUDES) -o $@ $< $(LIBS)

# 运行所有示例
run-all: $(EXAMPLES)
	@echo "=== 运行数组基本操作示例 ==="
	LD_LIBRARY_PATH=../src/.libs ./array_basic
	@echo ""
	@echo "=== 运行映射基本操作示例 ==="
	LD_LIBRARY_PATH=../src/.libs ./map_simple
	@echo ""
	@echo "=== 运行二叉搜索树基本操作示例 ==="
	LD_LIBRARY_PATH=../src/.libs ./btree_basic
	@echo ""
	@echo "=== 运行学生管理系统示例 ==="
	LD_LIBRARY_PATH=../src/.libs ./student_management

# 清理编译生成的文件
clean:
	rm -f $(EXAMPLES) *.o

# 安装示例（可选）
install: all
	@echo "示例程序已编译完成，可以运行 ./run-all 来查看所有示例"

# 帮助信息
help:
	@echo "可用的目标："
	@echo "  all        - 编译所有示例程序"
	@echo "  array_basic - 编译数组基本操作示例"
	@echo "  map_simple - 编译映射基本操作示例"
	@echo "  btree_basic - 编译二叉搜索树基本操作示例"
	@echo "  student_management - 编译学生管理系统示例"
	@echo "  run-all    - 编译并运行所有示例"
	@echo "  clean      - 清理编译生成的文件"
	@echo "  help       - 显示此帮助信息"

.PHONY: all run-all clean install help
