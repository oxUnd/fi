#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "rdb.h"
#include "persistence.h"

int main() {
    printf("=== Testing RDB Persistence ===\n");
    
    /* Create database */
    rdb_database_t *db = rdb_create_database("test_db");
    if (!db) {
        printf("Failed to create database\n");
        return 1;
    }
    
    /* Create persistence manager */
    rdb_persistence_manager_t *pm = rdb_persistence_create("./test_data", RDB_PERSISTENCE_FULL);
    if (!pm) {
        printf("Failed to create persistence manager\n");
        rdb_destroy_database(db);
        return 1;
    }
    
    /* Initialize persistence */
    if (rdb_persistence_init(pm) != 0) {
        printf("Failed to initialize persistence\n");
        rdb_persistence_destroy(pm);
        rdb_destroy_database(db);
        return 1;
    }
    
    /* Create a test table */
    fi_array *columns = fi_array_create(3, sizeof(rdb_column_t*));
    if (!columns) {
        printf("Failed to create columns array\n");
        goto cleanup;
    }
    
    /* Create columns */
    rdb_column_t *id_col = malloc(sizeof(rdb_column_t));
    strcpy(id_col->name, "id");
    id_col->type = RDB_TYPE_INT;
    id_col->max_length = 0;
    id_col->nullable = false;
    id_col->primary_key = true;
    id_col->unique = true;
    strcpy(id_col->default_value, "");
    strcpy(id_col->foreign_table, "");
    strcpy(id_col->foreign_column, "");
    id_col->is_foreign_key = false;
    fi_array_push(columns, &id_col);
    
    rdb_column_t *name_col = malloc(sizeof(rdb_column_t));
    strcpy(name_col->name, "name");
    name_col->type = RDB_TYPE_VARCHAR;
    name_col->max_length = 100;
    name_col->nullable = false;
    name_col->primary_key = false;
    name_col->unique = false;
    strcpy(name_col->default_value, "");
    strcpy(name_col->foreign_table, "");
    strcpy(name_col->foreign_column, "");
    name_col->is_foreign_key = false;
    fi_array_push(columns, &name_col);
    
    rdb_column_t *age_col = malloc(sizeof(rdb_column_t));
    strcpy(age_col->name, "age");
    age_col->type = RDB_TYPE_INT;
    age_col->max_length = 0;
    age_col->nullable = true;
    age_col->primary_key = false;
    age_col->unique = false;
    strcpy(age_col->default_value, "0");
    strcpy(age_col->foreign_table, "");
    strcpy(age_col->foreign_column, "");
    age_col->is_foreign_key = false;
    fi_array_push(columns, &age_col);
    
    /* Create table */
    const char *table_name = "users";
    if (rdb_create_table(db, table_name, columns) != 0) {
        printf("Failed to create table\n");
        goto cleanup;
    }
    
    /* Insert some test data */
    fi_array *values1 = fi_array_create(3, sizeof(rdb_value_t*));
    rdb_value_t *id_val1 = rdb_create_int_value(1);
    rdb_value_t *name_val1 = rdb_create_string_value("Alice");
    rdb_value_t *age_val1 = rdb_create_int_value(25);
    fi_array_push(values1, &id_val1);
    fi_array_push(values1, &name_val1);
    fi_array_push(values1, &age_val1);
    
    if (rdb_insert_row(db, "users", values1) != 0) {
        printf("Failed to insert row 1\n");
        goto cleanup;
    }
    
    fi_array *values2 = fi_array_create(3, sizeof(rdb_value_t*));
    rdb_value_t *id_val2 = rdb_create_int_value(2);
    rdb_value_t *name_val2 = rdb_create_string_value("Bob");
    rdb_value_t *age_val2 = rdb_create_int_value(30);
    fi_array_push(values2, &id_val2);
    fi_array_push(values2, &name_val2);
    fi_array_push(values2, &age_val2);
    
    if (rdb_insert_row(db, "users", values2) != 0) {
        printf("Failed to insert row 2\n");
        goto cleanup;
    }
    
    printf("Created table with 2 rows\n");
    
    /* Save database to disk */
    if (rdb_persistence_save_database(pm, db) != 0) {
        printf("Failed to save database\n");
        goto cleanup;
    }
    
    printf("Database saved to disk\n");
    
    /* Print persistence statistics */
    rdb_persistence_print_stats(pm);
    
    /* Clean up current database */
    rdb_destroy_database(db);
    
    /* Create new database and load from disk */
    db = rdb_create_database("test_db");
    if (!db) {
        printf("Failed to create new database\n");
        goto cleanup;
    }
    
    /* Load database from disk */
    if (rdb_persistence_load_database(pm, db) != 0) {
        printf("Failed to load database\n");
        printf("Checking if data directory exists...\n");
        system("ls -la ./test_data/");
        goto cleanup;
    }
    
    printf("Database loaded from disk\n");
    
    /* Verify data was loaded correctly */
    rdb_table_t *table = rdb_get_table(db, "users");
    if (table) {
        printf("Table 'users' found with %zu rows\n", fi_array_count(table->rows));
        
        /* Print loaded data */
        for (size_t i = 0; i < fi_array_count(table->rows); i++) {
            rdb_row_t **row_ptr = (rdb_row_t**)fi_array_get(table->rows, i);
            if (row_ptr && *row_ptr) {
                rdb_row_t *row = *row_ptr;
                printf("Row %zu: ID=%ld, Name=%s, Age=%ld\n", 
                       i, 
                       rdb_get_int_value((rdb_value_t*)fi_array_get(row->values, 0)),
                       rdb_get_string_value((rdb_value_t*)fi_array_get(row->values, 1)),
                       rdb_get_int_value((rdb_value_t*)fi_array_get(row->values, 2)));
            }
        }
    } else {
        printf("Table 'users' not found after loading\n");
    }
    
    printf("Persistence test completed successfully!\n");
    
cleanup:
    /* Clean up */
    if (pm) {
        rdb_persistence_destroy(pm);
    }
    if (db) {
        rdb_destroy_database(db);
    }
    
    /* Clean up test data directory */
    /* system("rm -rf ./test_data"); */
    
    return 0;
}
