# FI 关系型数据库事务功能

本文档描述了FI关系型数据库系统中实现的事务功能。

## 概述

事务功能为FI关系型数据库提供了ACID属性的支持，包括：
- **原子性 (Atomicity)**: 事务中的所有操作要么全部成功，要么全部回滚
- **一致性 (Consistency)**: 事务执行前后数据库保持一致状态
- **隔离性 (Isolation)**: 支持多种隔离级别
- **持久性 (Durability)**: 提交的事务永久保存

## 主要功能

### 1. 事务管理
- **BEGIN TRANSACTION**: 开始新事务
- **COMMIT**: 提交事务
- **ROLLBACK**: 回滚事务
- **AUTOCOMMIT**: 自动提交模式控制

### 2. 隔离级别
支持四种标准隔离级别：
- `READ_UNCOMMITTED`: 读未提交
- `READ_COMMITTED`: 读已提交（默认）
- `REPEATABLE_READ`: 可重复读
- `SERIALIZABLE`: 串行化

### 3. 事务日志
- 自动记录所有数据库操作
- 支持事务回滚时的操作撤销
- 维护事务历史记录

## 使用方法

### C API 使用

```c
#include "rdb.h"

// 创建数据库
rdb_database_t *db = rdb_create_database("my_db");
rdb_open_database(db);

// 开始事务
rdb_begin_transaction(db, RDB_ISOLATION_READ_COMMITTED);

// 执行数据库操作
rdb_insert_row(db, "users", values);
rdb_update_rows(db, "users", set_columns, set_values, where_conditions);

// 提交或回滚事务
rdb_commit_transaction(db);    // 提交
// 或
rdb_rollback_transaction(db);  // 回滚

// 检查事务状态
if (rdb_is_in_transaction(db)) {
    printf("当前在事务中\n");
}

// 设置自动提交
rdb_set_autocommit(db, false);  // 禁用自动提交
rdb_set_autocommit(db, true);   // 启用自动提交

// 设置默认隔离级别
rdb_set_isolation_level(db, RDB_ISOLATION_SERIALIZABLE);
```

### SQL 命令使用

```sql
-- 开始事务
BEGIN TRANSACTION;

-- 执行SQL操作
INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com');
UPDATE users SET email = 'newemail@example.com' WHERE name = 'Bob';

-- 提交事务
COMMIT;

-- 或者回滚事务
ROLLBACK;
```

## 演示程序

运行事务演示程序来查看功能示例：

```bash
cd /home/xsd/Work/c-family/fi/examples/rdb
make transaction_demo
LD_LIBRARY_PATH=../../src/.libs ./transaction_demo
```

演示程序包含以下功能测试：
1. **基本事务演示**: 展示BEGIN、COMMIT的基本使用
2. **事务回滚演示**: 展示ROLLBACK功能
3. **隔离级别演示**: 展示不同隔离级别的使用
4. **SQL事务命令演示**: 展示通过SQL命令使用事务

## 技术实现

### 核心组件

1. **事务管理器 (Transaction Manager)**
   - 管理当前活跃事务
   - 维护事务历史记录
   - 控制自动提交模式

2. **事务日志 (Transaction Log)**
   - 记录所有数据库操作
   - 存储操作前后的数据状态
   - 支持操作回滚

3. **SQL解析器扩展**
   - 支持BEGIN、COMMIT、ROLLBACK关键词
   - 解析事务相关SQL语句

### 数据结构

```c
// 事务结构
typedef struct {
    size_t transaction_id;               // 事务ID
    rdb_transaction_state_t state;       // 事务状态
    rdb_isolation_level_t isolation;     // 隔离级别
    fi_array *log_entries;               // 日志条目
    time_t start_time;                   // 开始时间
    time_t end_time;                     // 结束时间
    bool is_autocommit;                  // 是否自动提交
} rdb_transaction_t;

// 事务管理器
struct rdb_transaction_manager {
    rdb_transaction_t *current_transaction;  // 当前事务
    fi_array *transaction_history;           // 事务历史
    size_t next_transaction_id;              // 下一个事务ID
    rdb_isolation_level_t default_isolation; // 默认隔离级别
    bool autocommit_enabled;                 // 自动提交状态
};
```

## 注意事项

1. **内存管理**: 事务日志会消耗额外内存，大型事务可能需要更多内存
2. **性能影响**: 事务功能会增加操作开销，特别是在回滚时
3. **并发限制**: 当前实现不支持并发事务，同一时间只能有一个活跃事务
4. **持久化**: 当前实现是内存数据库，重启后事务历史会丢失

## 扩展功能

未来可以考虑添加的功能：
- 并发事务支持
- 死锁检测和处理
- 更复杂的隔离级别实现
- 事务持久化到磁盘
- 嵌套事务支持
- 保存点 (Savepoint) 功能

## 编译说明

确保已构建FI库：
```bash
cd /home/xsd/Work/c-family/fi
make
```

编译事务演示程序：
```bash
cd examples/rdb
gcc -o transaction_demo transaction_demo.c rdb.c sql_parser.c \
    -I../../src/include -L../../src/.libs -lfi -lm
```
