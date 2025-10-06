#ifndef __RDB_H__
#define __RDB_H__

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <stdint.h>

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
} rdb_column_t;

/* Table definition */
typedef struct {
    char name[64];              /* Table name */
    fi_array *columns;          /* Array of rdb_column_t */
    fi_array *rows;             /* Array of row data */
    fi_map *indexes;            /* Map of index_name -> fi_btree */
    char primary_key[64];       /* Primary key column name */
    size_t next_row_id;         /* Next available row ID */
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

/* Database instance */
typedef struct {
    char name[128];             /* Database name */
    fi_map *tables;             /* Map of table_name -> rdb_table_t */
    bool is_open;               /* Whether database is open */
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
    RDB_STMT_DROP_INDEX
} rdb_stmt_type_t;

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

/* Index operations */
int rdb_create_index(rdb_database_t *db, const char *table_name, const char *index_name, 
                     const char *column_name);
int rdb_drop_index(rdb_database_t *db, const char *table_name, const char *index_name);
fi_btree* rdb_get_index(rdb_database_t *db, const char *table_name, const char *index_name);

/* Column operations */
int rdb_add_column(rdb_database_t *db, const char *table_name, const rdb_column_t *column);
int rdb_drop_column(rdb_database_t *db, const char *table_name, const char *column_name);

/* Utility functions */
void rdb_print_table_info(rdb_database_t *db, const char *table_name);
void rdb_print_table_data(rdb_database_t *db, const char *table_name, size_t limit);
void rdb_print_database_info(rdb_database_t *db);

/* Internal utility functions */
int rdb_get_column_index(rdb_table_t *table, const char *column_name);
void rdb_update_table_indexes(rdb_table_t *table, rdb_row_t *row);

/* Memory management */
void rdb_value_free(void *value);
void rdb_row_free(void *row);
void rdb_column_free(void *column);

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

/* Value access */
int64_t rdb_get_int_value(const rdb_value_t *value);
double rdb_get_float_value(const rdb_value_t *value);
const char* rdb_get_string_value(const rdb_value_t *value);
bool rdb_get_bool_value(const rdb_value_t *value);

/* String representation */
char* rdb_value_to_string(const rdb_value_t *value);
char* rdb_type_to_string(rdb_data_type_t type);

#endif //__RDB_H__
