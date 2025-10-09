#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "rdb.h"
#include "persistence.h"

int main() {
    printf("=== Simple RDB Test with Persistence ===\n");
    
    /* Create database */
    rdb_database_t *db = rdb_create_database("test_db");
    if (!db) {
        printf("Failed to create database\n");
        return 1;
    }
    
    printf("Database created successfully\n");
    
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
    
    printf("Persistence manager initialized\n");
    
    /* Test table existence check before creation */
    const char *table_name = "users";
    printf("Checking if table '%s' exists before creation...\n", table_name);
    
    bool exists = rdb_table_exists(db, table_name);
    printf("Table exists: %s\n", exists ? "true" : "false");
    
    /* Create table columns */
    fi_array *columns = fi_array_create(2, sizeof(rdb_column_t*));
    if (!columns) {
        printf("Failed to create columns array\n");
        goto cleanup;
    }
    
    /* Create ID column */
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
    
    /* Create name column */
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
    
    /* Create table */
    if (rdb_create_table(db, table_name, columns) != 0) {
        printf("Failed to create table\n");
        goto cleanup;
    }
    
    printf("Table '%s' created successfully\n", table_name);
    
    /* Test table existence check after creation */
    printf("Checking if table '%s' exists after creation...\n", table_name);
    exists = rdb_table_exists(db, table_name);
    printf("Table exists: %s\n", exists ? "true" : "false");
    
    /* Save database to disk */
    if (rdb_persistence_save_database(pm, db) != 0) {
        printf("Failed to save database to disk\n");
        goto cleanup;
    }
    
    printf("Database saved to disk successfully\n");
    
    /* Clean up current database */
    rdb_destroy_database(db);
    
    /* Create new database and load from disk */
    db = rdb_create_database("test_db");
    if (!db) {
        printf("Failed to create new database for loading\n");
        goto cleanup;
    }
    
    /* Load database from disk */
    if (rdb_persistence_load_database(pm, db) != 0) {
        printf("Failed to load database from disk\n");
        goto cleanup;
    }
    
    printf("Database loaded from disk successfully\n");
    
    /* Verify table still exists after loading */
    printf("Checking if table '%s' exists after loading from disk...\n", table_name);
    exists = rdb_table_exists(db, table_name);
    printf("Table exists: %s\n", exists ? "true" : "false");
    
    if (exists) {
        rdb_table_t *table = rdb_get_table(db, table_name);
        if (table) {
            printf("Table '%s' found with %zu columns and %zu rows\n", 
                   table_name, 
                   fi_array_count(table->columns),
                   table->rows ? fi_array_count(table->rows) : 0);
        }
    }
    
    printf("Test completed successfully!\n");
    
cleanup:
    /* Clean up */
    if (pm) {
        rdb_persistence_destroy(pm);
    }
    if (db) {
        rdb_destroy_database(db);
    }
    
    return 0;
}