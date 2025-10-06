# FI 关系型数据库 (RDB)

这是一个基于 FI 数据结构库实现的简易关系型数据库系统，展示了如何使用 `fi_array`、`fi_map` 和 `fi_btree` 来构建一个功能完整的数据库引擎。

## 项目状态

由于 FI 库在处理字符串键时存在一些技术问题，本项目提供了多个版本的实现：

1. **完整版本** (`rdb.h`, `rdb.c`) - 完整的关系型数据库实现，包含所有功能
2. **简化版本** (`rdb_simple.h`, `rdb_simple.c`) - 使用整数键的简化实现
3. **最小版本** (`rdb_minimal.h`, `rdb_minimal.c`) - 最基本的数据库功能演示

**推荐使用最小版本进行学习和测试**，因为它稳定可靠且易于理解。

## 功能特性

### 核心功能
- **数据库管理**: 创建、打开、关闭数据库
- **表管理**: 创建、删除表，支持多种数据类型
- **数据操作**: INSERT、SELECT、UPDATE、DELETE
- **索引支持**: 基于 BTree 的索引系统
- **SQL 解析**: 支持基本 SQL 语句解析

### 支持的数据类型
- `INT` - 整数类型
- `FLOAT` - 浮点数类型
- `VARCHAR(n)` - 可变长度字符串
- `TEXT` - 长文本
- `BOOLEAN` - 布尔类型

### 支持的 SQL 语句
- `CREATE TABLE` - 创建表
- `DROP TABLE` - 删除表
- `INSERT INTO` - 插入数据
- `SELECT` - 查询数据
- `UPDATE` - 更新数据
- `DELETE` - 删除数据
- `CREATE INDEX` - 创建索引
- `DROP INDEX` - 删除索引

## 架构设计

### 数据存储层
- **表元数据**: 使用 `fi_map` 存储表名到表结构的映射
- **行数据**: 使用 `fi_array` 存储表中的行数据
- **索引**: 使用 `fi_btree` 实现各种索引

### 核心组件
1. **rdb.h/rdb.c** - 数据库核心功能
2. **sql_parser.h/sql_parser.c** - SQL 解析器
3. **rdb_demo.c** - 演示程序

## 编译和运行

### 前提条件
确保 FI 库已经编译完成：
```bash
cd ../../src
make
```

### 编译最小版本（推荐）
```bash
# 编译最小版本
gcc -Wall -Wextra -std=c99 -g -O2 -I../../src/include -I. -c rdb_minimal.c -o build/rdb_minimal.o
gcc -Wall -Wextra -std=c99 -g -O2 -I../../src/include -I. -c minimal_demo.c -o build/minimal_demo.o
gcc build/minimal_demo.o build/rdb_minimal.o -L../../src/.libs -static -lfi -lm -o build/minimal_demo

# 运行演示
./build/minimal_demo
```

### 编译简化版本
```bash
# 编译简化版本
gcc -Wall -Wextra -std=c99 -g -O2 -I../../src/include -I. -c rdb_simple.c -o build/rdb_simple.o
gcc -Wall -Wextra -std=c99 -g -O2 -I../../src/include -I. -c simple_demo.c -o build/simple_demo.o
gcc build/simple_demo.o build/rdb_simple.o -L../../src/.libs -static -lfi -lm -o build/simple_demo

# 运行演示
./build/simple_demo
```

### 编译完整版本
```bash
make -f Makefile.simple all
make -f Makefile.simple run
```

### 清理
```bash
rm -rf build/
```

## 使用示例

### 1. 最小版本数据库操作

```c
#include "rdb_minimal.h"

// 创建数据库
rdb_database_t *db = rdb_create_database("my_db");

// 创建表
rdb_create_table(db, "users");
rdb_create_table(db, "products");

// 打印数据库信息
rdb_print_database_info(db);

// 清理
rdb_destroy_database(db);
```

### 2. 简化版本数据库操作

```c
#include "rdb_simple.h"

// 创建数据库
rdb_database_t *db = rdb_create_database("my_db");

// 创建表结构
fi_array *columns = fi_array_create(3, sizeof(rdb_column_t*));
rdb_column_t *col1 = create_column("id", RDB_TYPE_INT, true, true, false);
rdb_column_t *col2 = create_column("name", RDB_TYPE_VARCHAR, false, false, false);
rdb_column_t *col3 = create_column("age", RDB_TYPE_INT, false, false, true);

fi_array_push(columns, &col1);
fi_array_push(columns, &col2);
fi_array_push(columns, &col3);

// 创建表
rdb_create_table(db, "users", columns);

// 插入数据
fi_array *values = fi_array_create(3, sizeof(rdb_value_t*));
rdb_value_t *val1 = rdb_create_int_value(1);
rdb_value_t *val2 = rdb_create_string_value("Alice");
rdb_value_t *val3 = rdb_create_int_value(25);

fi_array_push(values, &val1);
fi_array_push(values, &val2);
fi_array_push(values, &val3);

rdb_insert_row(db, "users", values);

// 清理
fi_array_destroy(columns);
fi_array_destroy(values);
rdb_destroy_database(db);
```

### 3. 运行演示程序

```bash
# 运行最小版本演示
./build/minimal_demo

# 运行简化版本演示
./build/simple_demo
```

## 技术实现

### 数据结构使用
- **fi_array**: 存储表列表、行数据、列定义
- **fi_map**: 存储表元数据（使用整数键避免字符串问题）
- **fi_btree**: 实现索引结构（在完整版本中）

### 架构设计
```
Database
├── tables (fi_array: rdb_table_t*)
│   └── Table
│       ├── columns (fi_array: rdb_column_t*)
│       ├── rows (fi_array: rdb_row_t*)
│       └── indexes (fi_map: name -> fi_btree)
```

## API 参考

### 数据库操作
- `rdb_create_database(name)` - 创建数据库
- `rdb_open_database(db)` - 打开数据库
- `rdb_close_database(db)` - 关闭数据库
- `rdb_destroy_database(db)` - 销毁数据库

### 表操作
- `rdb_create_table(db, name, columns)` - 创建表
- `rdb_drop_table(db, name)` - 删除表
- `rdb_table_exists(db, name)` - 检查表是否存在
- `rdb_get_table(db, name)` - 获取表对象

### 数据操作
- `rdb_insert_row(db, table, values)` - 插入行
- `rdb_update_rows(db, table, columns, values, conditions)` - 更新行
- `rdb_delete_rows(db, table, conditions)` - 删除行
- `rdb_select_rows(db, table, columns, conditions)` - 查询行

### 索引操作
- `rdb_create_index(db, table, index_name, column)` - 创建索引
- `rdb_drop_index(db, table, index_name)` - 删除索引

### 值创建函数
- `rdb_create_int_value(value)` - 创建整数值
- `rdb_create_float_value(value)` - 创建浮点值
- `rdb_create_string_value(value)` - 创建字符串值
- `rdb_create_bool_value(value)` - 创建布尔值
- `rdb_create_null_value(type)` - 创建 NULL 值

## 演示程序

运行 `rdb_demo` 可以看到以下演示：

1. **基本数据库操作** - 创建、打开、关闭数据库
2. **表操作** - 创建多个表，显示表结构
3. **数据操作** - 插入、查询数据
4. **索引操作** - 创建和使用索引
5. **SQL 解析器** - 解析各种 SQL 语句

## 设计亮点

### 1. 模块化设计
- 数据库核心功能与 SQL 解析器分离
- 清晰的接口定义和错误处理
- 易于扩展和维护

### 2. 内存管理
- 自动内存管理，避免内存泄漏
- 提供完整的清理函数
- 支持 valgrind 内存检查

### 3. 类型安全
- 强类型的数据值系统
- 运行时类型检查
- 统一的类型转换接口

### 4. 索引优化
- 基于 BTree 的高效索引
- 支持多列索引
- 自动索引维护

## 扩展建议

### 功能扩展
1. **事务支持** - 添加 ACID 事务
2. **并发控制** - 多线程安全
3. **持久化存储** - 数据文件存储
4. **查询优化** - 更智能的查询计划
5. **外键约束** - 表间关系支持

### 性能优化
1. **缓冲池** - 页面缓存机制
2. **查询缓存** - 常用查询结果缓存
3. **批量操作** - 批量插入和更新
4. **压缩存储** - 数据压缩

### SQL 功能扩展
1. **JOIN 操作** - 多表连接
2. **聚合函数** - COUNT、SUM、AVG 等
3. **子查询** - 嵌套查询支持
4. **视图** - 虚拟表支持

## 技术细节

### 数据结构使用
- **fi_map**: 存储表元数据和索引映射
- **fi_array**: 存储表行数据和列定义
- **fi_btree**: 实现各种索引结构

### 内存布局
```
Database
├── tables (fi_map: name -> Table)
│   └── Table
│       ├── columns (fi_array: Column*)
│       ├── rows (fi_array: Row*)
│       └── indexes (fi_map: name -> fi_btree)
```

### 错误处理
- 统一的错误码系统
- 详细的错误信息
- 优雅的错误恢复

## 许可证

本项目遵循与 FI 库相同的许可证。

## 贡献

欢迎提交 Issue 和 Pull Request 来改进这个关系型数据库实现！

## 联系方式

如有问题或建议，请通过 GitHub Issues 联系。
