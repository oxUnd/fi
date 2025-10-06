#ifndef __RDB_H__
#define __RDB_H__

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <stdint.h>
#include <time.h>
#include <pthread.h>

/* Include FI data structures */
#include "../../src/include/fi.h"
#include "../../src/include/fi_map.h"
#include "../../src/include/fi_btree.h"

/* Data types supported by the database */
typedef enum {
    RDB_TYPE_INT = 1,
    RDB_TYPE_FLOAT,
    RDB_TYPE_VARCHAR,
    RDB_TYPE_TEXT,
    RDB_TYPE_BOOLEAN
} rdb_data_type_t;

/* Column definition */
typedef struct {
    char name[64];              /* Column name */
    rdb_data_type_t type;       /* Data type */
    size_t max_length;          /* Maximum length for VARCHAR */
    bool nullable;              /* Whether column can be NULL */
    bool primary_key;           /* Whether this is a primary key */
    bool unique;                /* Whether this column is unique */
    char default_value[256];    /* Default value */
    /* Foreign key constraints */
    char foreign_table[64];     /* Referenced table name */
    char foreign_column[64];    /* Referenced column name */
    bool is_foreign_key;        /* Whether this is a foreign key */
} rdb_column_t;

/* Table definition */
typedef struct {
    char name[64];              /* Table name */
    fi_array *columns;          /* Array of rdb_column_t */
    fi_array *rows;             /* Array of row data */
    fi_map *indexes;            /* Map of index_name -> fi_btree */
    char primary_key[64];       /* Primary key column name */
    size_t next_row_id;         /* Next available row ID */
    /* Thread safety */
    pthread_mutex_t rwlock;     /* Mutex for table operations */
    pthread_mutex_t mutex;      /* Mutex for next_row_id counter */
} rdb_table_t;

/* Row data structure */
typedef struct {
    size_t row_id;              /* Unique row identifier */
    fi_array *values;           /* Array of column values */
} rdb_row_t;

/* Value structure */
typedef struct {
    rdb_data_type_t type;       /* Value type */
    union {
        int64_t int_val;
        double float_val;
        char *string_val;
        bool bool_val;
    } data;
    bool is_null;               /* Whether this value is NULL */
} rdb_value_t;

/* Foreign key constraint */
typedef struct {
    char constraint_name[64];    /* Constraint name */
    char table_name[64];        /* Table containing the foreign key */
    char column_name[64];       /* Column name */
    char ref_table_name[64];    /* Referenced table name */
    char ref_column_name[64];   /* Referenced column name */
    bool on_delete_cascade;     /* CASCADE on delete */
    bool on_update_cascade;     /* CASCADE on update */
} rdb_foreign_key_t;

/* Forward declaration */
typedef struct rdb_transaction_manager rdb_transaction_manager_t;

/* Database instance */
typedef struct {
    char name[128];             /* Database name */
    fi_map *tables;             /* Map of table_name -> rdb_table_t */
    fi_map *foreign_keys;       /* Map of constraint_name -> rdb_foreign_key_t */
    rdb_transaction_manager_t *transaction_manager; /* Transaction manager */
    bool is_open;               /* Whether database is open */
    /* Thread safety */
    pthread_mutex_t rwlock;     /* Mutex for database operations */
    pthread_mutex_t mutex;      /* Mutex for database state changes */
} rdb_database_t;

/* SQL statement types */
typedef enum {
    RDB_STMT_CREATE_TABLE,
    RDB_STMT_DROP_TABLE,
    RDB_STMT_INSERT,
    RDB_STMT_SELECT,
    RDB_STMT_UPDATE,
    RDB_STMT_DELETE,
    RDB_STMT_CREATE_INDEX,
    RDB_STMT_DROP_INDEX,
    RDB_STMT_ADD_FOREIGN_KEY,
    RDB_STMT_DROP_FOREIGN_KEY,
    RDB_STMT_BEGIN_TRANSACTION,
    RDB_STMT_COMMIT_TRANSACTION,
    RDB_STMT_ROLLBACK_TRANSACTION
} rdb_stmt_type_t;

/* JOIN types */
typedef enum {
    RDB_JOIN_INNER,
    RDB_JOIN_LEFT,
    RDB_JOIN_RIGHT,
    RDB_JOIN_FULL
} rdb_join_type_t;

/* Transaction isolation levels */
typedef enum {
    RDB_ISOLATION_READ_UNCOMMITTED = 1,
    RDB_ISOLATION_READ_COMMITTED,
    RDB_ISOLATION_REPEATABLE_READ,
    RDB_ISOLATION_SERIALIZABLE
} rdb_isolation_level_t;

/* Transaction states */
typedef enum {
    RDB_TRANSACTION_ACTIVE = 1,
    RDB_TRANSACTION_COMMITTED,
    RDB_TRANSACTION_ABORTED,
    RDB_TRANSACTION_ROLLED_BACK
} rdb_transaction_state_t;

/* Transaction operation types */
typedef enum {
    RDB_OP_INSERT = 1,
    RDB_OP_UPDATE,
    RDB_OP_DELETE,
    RDB_OP_CREATE_TABLE,
    RDB_OP_DROP_TABLE,
    RDB_OP_CREATE_INDEX,
    RDB_OP_DROP_INDEX
} rdb_operation_type_t;

/* JOIN condition */
typedef struct {
    char left_table[64];        /* Left table name */
    char left_column[64];       /* Left column name */
    char right_table[64];       /* Right table name */
    char right_column[64];      /* Right column name */
    rdb_join_type_t join_type;  /* Type of JOIN */
} rdb_join_condition_t;

/* Query result row (for multi-table queries) */
typedef struct {
    size_t row_id;              /* Unique row identifier */
    fi_array *table_names;      /* Array of table names in result */
    fi_map *values;             /* Map of "table.column" -> rdb_value_t */
} rdb_result_row_t;

/* Transaction log entry */
typedef struct {
    rdb_operation_type_t operation_type;  /* Type of operation */
    char table_name[64];                  /* Table name */
    size_t row_id;                        /* Row ID (for row operations) */
    rdb_row_t *old_row;                   /* Original row data (for rollback) */
    rdb_row_t *new_row;                   /* New row data */
    char index_name[64];                  /* Index name (for index operations) */
    char column_name[64];                 /* Column name (for column operations) */
    rdb_column_t *column_def;             /* Column definition (for schema changes) */
    void *additional_data;                /* Additional operation-specific data */
} rdb_transaction_log_entry_t;

/* Transaction structure */
typedef struct {
    size_t transaction_id;               /* Unique transaction ID */
    rdb_transaction_state_t state;       /* Current transaction state */
    rdb_isolation_level_t isolation;     /* Isolation level */
    fi_array *log_entries;               /* Array of log entries for rollback */
    time_t start_time;                   /* Transaction start time */
    time_t end_time;                     /* Transaction end time */
    bool is_autocommit;                  /* Whether this is an autocommit transaction */
} rdb_transaction_t;

/* Transaction manager */
struct rdb_transaction_manager {
    rdb_transaction_t *current_transaction;  /* Current active transaction */
    fi_array *transaction_history;           /* History of completed transactions */
    size_t next_transaction_id;              /* Next available transaction ID */
    rdb_isolation_level_t default_isolation; /* Default isolation level */
    bool autocommit_enabled;                 /* Whether autocommit is enabled */
    /* Thread safety */
    pthread_mutex_t mutex;                   /* Mutex for transaction operations */
    pthread_mutex_t rwlock;                  /* Mutex for transaction state */
};

/* SQL statement structure */
typedef struct {
    rdb_stmt_type_t type;       /* Statement type */
    char table_name[64];        /* Target table name */
    fi_array *columns;          /* Column names */
    fi_array *values;           /* Values for INSERT/UPDATE */
    fi_array *where_conditions; /* WHERE conditions */
    fi_array *select_columns;   /* Columns to select */
    char index_name[64];        /* Index name for CREATE/DROP INDEX */
    char index_column[64];      /* Column name for index */
    /* Multi-table support */
    fi_array *from_tables;      /* Tables in FROM clause */
    fi_array *join_conditions;  /* JOIN conditions */
    fi_array *order_by;         /* ORDER BY columns */
    size_t limit_value;         /* LIMIT value */
    size_t offset_value;        /* OFFSET value */
    /* Foreign key operations */
    char foreign_key_name[64];  /* Foreign key constraint name */
    rdb_foreign_key_t *foreign_key; /* Foreign key definition */
} rdb_statement_t;

/* Database operations */
rdb_database_t* rdb_create_database(const char *name);
int rdb_open_database(rdb_database_t *db);
int rdb_close_database(rdb_database_t *db);
void rdb_destroy_database(rdb_database_t *db);

/* Table operations */
int rdb_create_table(rdb_database_t *db, const char *table_name, fi_array *columns);
int rdb_drop_table(rdb_database_t *db, const char *table_name);
rdb_table_t* rdb_get_table(rdb_database_t *db, const char *table_name);
bool rdb_table_exists(rdb_database_t *db, const char *table_name);

/* Internal table functions */
rdb_table_t* rdb_create_table_object(const char *name, fi_array *columns);
void rdb_destroy_table(rdb_table_t *table);

/* Row operations */
int rdb_insert_row(rdb_database_t *db, const char *table_name, fi_array *values);
int rdb_update_rows(rdb_database_t *db, const char *table_name, fi_array *set_columns, 
                    fi_array *set_values, fi_array *where_conditions);
int rdb_delete_rows(rdb_database_t *db, const char *table_name, fi_array *where_conditions);
fi_array* rdb_select_rows(rdb_database_t *db, const char *table_name, fi_array *columns, 
                          fi_array *where_conditions);

/* Multi-table operations */
fi_array* rdb_select_join(rdb_database_t *db, const rdb_statement_t *stmt);
int rdb_validate_foreign_key(rdb_database_t *db, const char *table_name, 
                            const char *column_name, const rdb_value_t *value);
int rdb_enforce_foreign_key_constraints(rdb_database_t *db, const char *table_name, 
                                       rdb_row_t *row);

/* Index operations */
int rdb_create_index(rdb_database_t *db, const char *table_name, const char *index_name, 
                     const char *column_name);
int rdb_drop_index(rdb_database_t *db, const char *table_name, const char *index_name);
fi_btree* rdb_get_index(rdb_database_t *db, const char *table_name, const char *index_name);

/* Column operations */
int rdb_add_column(rdb_database_t *db, const char *table_name, const rdb_column_t *column);
int rdb_drop_column(rdb_database_t *db, const char *table_name, const char *column_name);

/* Foreign key operations */
int rdb_add_foreign_key(rdb_database_t *db, const rdb_foreign_key_t *foreign_key);
int rdb_drop_foreign_key(rdb_database_t *db, const char *constraint_name);
rdb_foreign_key_t* rdb_get_foreign_key(rdb_database_t *db, const char *constraint_name);
fi_array* rdb_get_foreign_keys_by_table(rdb_database_t *db, const char *table_name);

/* Utility functions */
void rdb_print_table_info(rdb_database_t *db, const char *table_name);
void rdb_print_table_data(rdb_database_t *db, const char *table_name, size_t limit);
void rdb_print_database_info(rdb_database_t *db);
void rdb_print_join_result(fi_array *result, const rdb_statement_t *stmt);
void rdb_print_foreign_keys(rdb_database_t *db);

/* Internal utility functions */
int rdb_get_column_index(rdb_table_t *table, const char *column_name);
void rdb_update_table_indexes(rdb_table_t *table, rdb_row_t *row);

/* Memory management */
void rdb_value_free(void *value);
void rdb_row_free(void *row);
void rdb_column_free(void *column);
void rdb_result_row_free(void *result_row);
void rdb_foreign_key_free(void *foreign_key);
void rdb_join_condition_free(void *join_condition);

/* Comparison functions */
int rdb_value_compare(const void *a, const void *b);
int rdb_string_compare(const void *a, const void *b);

/* Hash functions */
uint32_t rdb_string_hash(const void *key, size_t key_size);

/* Data type conversion */
rdb_value_t* rdb_create_int_value(int64_t value);
rdb_value_t* rdb_create_float_value(double value);
rdb_value_t* rdb_create_string_value(const char *value);
rdb_value_t* rdb_create_bool_value(bool value);
rdb_value_t* rdb_create_null_value(rdb_data_type_t type);
rdb_value_t* rdb_value_copy(const rdb_value_t *original);

/* Value access */
int64_t rdb_get_int_value(const rdb_value_t *value);
double rdb_get_float_value(const rdb_value_t *value);
const char* rdb_get_string_value(const rdb_value_t *value);
bool rdb_get_bool_value(const rdb_value_t *value);

/* String representation */
char* rdb_value_to_string(const rdb_value_t *value);
char* rdb_type_to_string(rdb_data_type_t type);

/* Multi-table helper functions */
rdb_foreign_key_t* rdb_create_foreign_key(const char *constraint_name, const char *table_name, 
                                         const char *column_name, const char *ref_table_name, 
                                         const char *ref_column_name);
rdb_join_condition_t* rdb_create_join_condition(const char *left_table, const char *left_column,
                                                const char *right_table, const char *right_column,
                                                rdb_join_type_t join_type);
rdb_result_row_t* rdb_create_result_row(size_t row_id, fi_array *table_names, fi_map *values);
bool rdb_row_matches_join_condition(const rdb_row_t *left_row, const rdb_row_t *right_row,
                                   const rdb_join_condition_t *condition, 
                                   const rdb_table_t *left_table, const rdb_table_t *right_table);

/* Transaction management functions */
rdb_transaction_manager_t* rdb_create_transaction_manager(void);
void rdb_destroy_transaction_manager(rdb_transaction_manager_t *tm);
int rdb_begin_transaction(rdb_database_t *db, rdb_isolation_level_t isolation);
int rdb_commit_transaction(rdb_database_t *db);
int rdb_rollback_transaction(rdb_database_t *db);
rdb_transaction_t* rdb_get_current_transaction(rdb_database_t *db);
bool rdb_is_in_transaction(rdb_database_t *db);
int rdb_set_autocommit(rdb_database_t *db, bool enabled);
int rdb_set_isolation_level(rdb_database_t *db, rdb_isolation_level_t level);

/* Transaction logging functions */
int rdb_log_operation(rdb_database_t *db, rdb_operation_type_t op_type, const char *table_name, 
                     size_t row_id, rdb_row_t *old_row, rdb_row_t *new_row);
int rdb_rollback_operations(rdb_database_t *db, rdb_transaction_t *transaction);

/* Transaction-aware database operations */
int rdb_insert_row_transactional(rdb_database_t *db, const char *table_name, fi_array *values);
int rdb_update_rows_transactional(rdb_database_t *db, const char *table_name, fi_array *set_columns, 
                                 fi_array *set_values, fi_array *where_conditions);
int rdb_delete_rows_transactional(rdb_database_t *db, const char *table_name, fi_array *where_conditions);
int rdb_create_table_transactional(rdb_database_t *db, const char *table_name, fi_array *columns);
int rdb_drop_table_transactional(rdb_database_t *db, const char *table_name);

/* Transaction log entry management */
rdb_transaction_log_entry_t* rdb_create_transaction_log_entry(rdb_operation_type_t op_type, 
                                                             const char *table_name, size_t row_id,
                                                             rdb_row_t *old_row, rdb_row_t *new_row);
void rdb_destroy_transaction_log_entry(rdb_transaction_log_entry_t *entry);
void rdb_transaction_log_entry_free(void *entry);

/* Transaction utility functions */
const char* rdb_transaction_state_to_string(rdb_transaction_state_t state);
const char* rdb_isolation_level_to_string(rdb_isolation_level_t level);
const char* rdb_operation_type_to_string(rdb_operation_type_t op_type);
void rdb_print_transaction_status(rdb_database_t *db);

/* Thread safety functions */
int rdb_init_thread_safety(rdb_database_t *db);
void rdb_cleanup_thread_safety(rdb_database_t *db);
int rdb_table_init_thread_safety(rdb_table_t *table);
void rdb_table_cleanup_thread_safety(rdb_table_t *table);
int rdb_transaction_manager_init_thread_safety(rdb_transaction_manager_t *tm);
void rdb_transaction_manager_cleanup_thread_safety(rdb_transaction_manager_t *tm);

/* Thread-safe database operations */
int rdb_lock_database_read(rdb_database_t *db);
int rdb_lock_database_write(rdb_database_t *db);
int rdb_unlock_database(rdb_database_t *db);
int rdb_lock_table_read(rdb_table_t *table);
int rdb_lock_table_write(rdb_table_t *table);
int rdb_unlock_table(rdb_table_t *table);
int rdb_lock_transaction_manager(rdb_transaction_manager_t *tm);
int rdb_unlock_transaction_manager(rdb_transaction_manager_t *tm);

/* Thread-safe versions of core operations */
int rdb_insert_row_thread_safe(rdb_database_t *db, const char *table_name, fi_array *values);
int rdb_update_rows_thread_safe(rdb_database_t *db, const char *table_name, fi_array *set_columns, 
                               fi_array *set_values, fi_array *where_conditions);
int rdb_delete_rows_thread_safe(rdb_database_t *db, const char *table_name, fi_array *where_conditions);
fi_array* rdb_select_rows_thread_safe(rdb_database_t *db, const char *table_name, fi_array *columns, 
                                     fi_array *where_conditions);
int rdb_create_table_thread_safe(rdb_database_t *db, const char *table_name, fi_array *columns);
int rdb_drop_table_thread_safe(rdb_database_t *db, const char *table_name);

#endif //__RDB_H__
