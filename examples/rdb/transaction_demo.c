#include "rdb.h"
#include "sql_parser.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Demo functions */
void demo_basic_transaction(void);
void demo_transaction_rollback(void);
void demo_transaction_isolation(void);
void demo_sql_transaction_commands(void);

/* Helper functions */
rdb_column_t* create_column(const char *name, rdb_data_type_t type, bool primary_key, bool unique, bool nullable);
void print_separator(const char *title);
void execute_sql(rdb_database_t *db, const char *sql);

int main() {
    printf("=== FI Relational Database Transaction Demo ===\n\n");
    
    demo_basic_transaction();
    demo_transaction_rollback();
    demo_transaction_isolation();
    demo_sql_transaction_commands();
    
    printf("\n=== Transaction demo completed successfully! ===\n");
    return 0;
}

void demo_basic_transaction(void) {
    print_separator("Basic Transaction Demo");
    
    rdb_database_t *db = rdb_create_database("transaction_demo");
    if (!db) {
        printf("Error: Failed to create database\n");
        return;
    }
    
    rdb_open_database(db);
    
    /* Create a simple table */
    fi_array *columns = fi_array_create(3, sizeof(rdb_column_t*));
    rdb_column_t *id = create_column("id", RDB_TYPE_INT, true, true, false);
    rdb_column_t *name = create_column("name", RDB_TYPE_VARCHAR, false, false, false);
    rdb_column_t *balance = create_column("balance", RDB_TYPE_FLOAT, false, false, false);
    
    fi_array_push(columns, &id);
    fi_array_push(columns, &name);
    fi_array_push(columns, &balance);
    
    if (rdb_create_table(db, "accounts", columns) == 0) {
        printf("Table 'accounts' created successfully\n");
    }
    
    /* Show initial transaction status */
    rdb_print_transaction_status(db);
    
    /* Begin transaction */
    printf("\n--- Beginning transaction ---\n");
    if (rdb_begin_transaction(db, RDB_ISOLATION_READ_COMMITTED) == 0) {
        rdb_print_transaction_status(db);
        
        /* Insert some data */
        printf("\n--- Inserting data within transaction ---\n");
        fi_array *values1 = fi_array_create(3, sizeof(rdb_value_t*));
        rdb_value_t *v1_id = rdb_create_int_value(1);
        rdb_value_t *v1_name = rdb_create_string_value("Alice");
        rdb_value_t *v1_balance = rdb_create_float_value(1000.0);
        
        fi_array_push(values1, &v1_id);
        fi_array_push(values1, &v1_name);
        fi_array_push(values1, &v1_balance);
        
        rdb_insert_row(db, "accounts", values1);
        
        fi_array *values2 = fi_array_create(3, sizeof(rdb_value_t*));
        rdb_value_t *v2_id = rdb_create_int_value(2);
        rdb_value_t *v2_name = rdb_create_string_value("Bob");
        rdb_value_t *v2_balance = rdb_create_float_value(500.0);
        
        fi_array_push(values2, &v2_id);
        fi_array_push(values2, &v2_name);
        fi_array_push(values2, &v2_balance);
        
        rdb_insert_row(db, "accounts", values2);
        
        /* Show data */
        rdb_print_table_data(db, "accounts", 10);
        
        /* Commit transaction */
        printf("\n--- Committing transaction ---\n");
        rdb_commit_transaction(db);
        rdb_print_transaction_status(db);
        
        /* Show final data */
        printf("\nFinal data after commit:\n");
        rdb_print_table_data(db, "accounts", 10);
        
        /* Clean up */
        fi_array_destroy(values1);
        fi_array_destroy(values2);
    }
    
    /* Clean up */
    fi_array_destroy(columns);
    rdb_destroy_database(db);
    
    printf("Basic transaction demo completed\n");
}

void demo_transaction_rollback(void) {
    print_separator("Transaction Rollback Demo");
    
    rdb_database_t *db = rdb_create_database("rollback_demo");
    if (!db) {
        printf("Error: Failed to create database\n");
        return;
    }
    
    rdb_open_database(db);
    
    /* Create a simple table */
    fi_array *columns = fi_array_create(2, sizeof(rdb_column_t*));
    rdb_column_t *id = create_column("id", RDB_TYPE_INT, true, true, false);
    rdb_column_t *name = create_column("name", RDB_TYPE_VARCHAR, false, false, false);
    
    fi_array_push(columns, &id);
    fi_array_push(columns, &name);
    
    if (rdb_create_table(db, "users", columns) == 0) {
        printf("Table 'users' created successfully\n");
    }
    
    /* Insert initial data */
    fi_array *initial_values = fi_array_create(2, sizeof(rdb_value_t*));
    rdb_value_t *iv_id = rdb_create_int_value(1);
    rdb_value_t *iv_name = rdb_create_string_value("Initial User");
    
    fi_array_push(initial_values, &iv_id);
    fi_array_push(initial_values, &iv_name);
    
    rdb_insert_row(db, "users", initial_values);
    
    printf("Initial data:\n");
    rdb_print_table_data(db, "users", 10);
    
    /* Begin transaction and make changes */
    printf("\n--- Beginning transaction with changes ---\n");
    if (rdb_begin_transaction(db, RDB_ISOLATION_READ_COMMITTED) == 0) {
        
        /* Insert new data */
        fi_array *values = fi_array_create(2, sizeof(rdb_value_t*));
        rdb_value_t *v_id = rdb_create_int_value(2);
        rdb_value_t *v_name = rdb_create_string_value("New User");
        
        fi_array_push(values, &v_id);
        fi_array_push(values, &v_name);
        
        rdb_insert_row(db, "users", values);
        
        /* Update existing data */
        fi_array *set_columns = fi_array_create(1, sizeof(char*));
        fi_array *set_values = fi_array_create(1, sizeof(rdb_value_t*));
        fi_array *where_conditions = fi_array_create(1, sizeof(char*));
        
        const char *col_name = "name";
        rdb_value_t *new_name = rdb_create_string_value("Updated User");
        const char *where_cond = "id = 1";
        
        fi_array_push(set_columns, &col_name);
        fi_array_push(set_values, &new_name);
        fi_array_push(where_conditions, &where_cond);
        
        rdb_update_rows(db, "users", set_columns, set_values, where_conditions);
        
        printf("Data within transaction (before rollback):\n");
        rdb_print_table_data(db, "users", 10);
        
        /* Rollback transaction */
        printf("\n--- Rolling back transaction ---\n");
        rdb_rollback_transaction(db);
        
        printf("Data after rollback:\n");
        rdb_print_table_data(db, "users", 10);
        
        /* Clean up */
        fi_array_destroy(values);
        fi_array_destroy(set_columns);
        fi_array_destroy(set_values);
        fi_array_destroy(where_conditions);
    }
    
    /* Clean up */
    fi_array_destroy(columns);
    fi_array_destroy(initial_values);
    rdb_destroy_database(db);
    
    printf("Transaction rollback demo completed\n");
}

void demo_transaction_isolation(void) {
    print_separator("Transaction Isolation Levels Demo");
    
    rdb_database_t *db = rdb_create_database("isolation_demo");
    if (!db) {
        printf("Error: Failed to create database\n");
        return;
    }
    
    rdb_open_database(db);
    
    /* Test different isolation levels */
    printf("Testing different isolation levels:\n\n");
    
    /* Set default isolation level */
    rdb_set_isolation_level(db, RDB_ISOLATION_READ_COMMITTED);
    
    /* Begin transaction with specific isolation level */
    printf("--- Beginning transaction with READ COMMITTED isolation ---\n");
    if (rdb_begin_transaction(db, RDB_ISOLATION_READ_COMMITTED) == 0) {
        rdb_print_transaction_status(db);
        rdb_commit_transaction(db);
    }
    
    printf("\n--- Beginning transaction with SERIALIZABLE isolation ---\n");
    if (rdb_begin_transaction(db, RDB_ISOLATION_SERIALIZABLE) == 0) {
        rdb_print_transaction_status(db);
        rdb_commit_transaction(db);
    }
    
    /* Test autocommit */
    printf("\n--- Testing autocommit functionality ---\n");
    rdb_set_autocommit(db, false);
    rdb_print_transaction_status(db);
    
    rdb_set_autocommit(db, true);
    rdb_print_transaction_status(db);
    
    /* Clean up */
    rdb_destroy_database(db);
    
    printf("Transaction isolation demo completed\n");
}

void demo_sql_transaction_commands(void) {
    print_separator("SQL Transaction Commands Demo");
    
    rdb_database_t *db = rdb_create_database("sql_transaction_demo");
    if (!db) {
        printf("Error: Failed to create database\n");
        return;
    }
    
    rdb_open_database(db);
    
    /* Test SQL transaction commands */
    printf("Testing SQL transaction commands:\n\n");
    
    /* Test BEGIN TRANSACTION */
    printf("--- Testing BEGIN TRANSACTION ---\n");
    execute_sql(db, "BEGIN TRANSACTION");
    rdb_print_transaction_status(db);
    
    /* Test COMMIT */
    printf("\n--- Testing COMMIT ---\n");
    execute_sql(db, "COMMIT");
    rdb_print_transaction_status(db);
    
    /* Test ROLLBACK */
    printf("\n--- Testing ROLLBACK ---\n");
    execute_sql(db, "BEGIN");
    execute_sql(db, "ROLLBACK");
    rdb_print_transaction_status(db);
    
    /* Clean up */
    rdb_destroy_database(db);
    
    printf("SQL transaction commands demo completed\n");
}

/* Helper function implementations */
rdb_column_t* create_column(const char *name, rdb_data_type_t type, bool primary_key, bool unique, bool nullable) {
    rdb_column_t *column = malloc(sizeof(rdb_column_t));
    if (!column) return NULL;
    
    strncpy(column->name, name, sizeof(column->name) - 1);
    column->name[sizeof(column->name) - 1] = '\0';
    column->type = type;
    column->max_length = (type == RDB_TYPE_VARCHAR) ? 255 : 0;
    column->primary_key = primary_key;
    column->unique = unique;
    column->nullable = nullable;
    column->default_value[0] = '\0';
    column->is_foreign_key = false;
    column->foreign_table[0] = '\0';
    column->foreign_column[0] = '\0';
    
    return column;
}

void print_separator(const char *title) {
    printf("\n");
    printf("========================================\n");
    printf("  %s\n", title);
    printf("========================================\n");
}

void execute_sql(rdb_database_t *db, const char *sql) {
    if (!db || !sql) return;
    
    printf("Executing SQL: %s\n", sql);
    
    sql_parser_t *parser = sql_parser_create(sql);
    if (!parser) {
        printf("Error: Failed to create SQL parser\n");
        return;
    }
    
    rdb_statement_t *stmt = sql_parse_statement(parser);
    if (!stmt) {
        printf("Error: Failed to parse SQL statement: %s\n", sql_parser_get_error(parser));
        sql_parser_destroy(parser);
        return;
    }
    
    /* Execute the statement */
    int result = sql_execute_statement(db, stmt);
    if (result != 0) {
        printf("Error: Failed to execute SQL statement\n");
    }
    
    /* Clean up */
    sql_statement_free(stmt);
    sql_parser_destroy(parser);
}
