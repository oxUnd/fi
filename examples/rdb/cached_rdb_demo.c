#include "cached_rdb.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

/* Helper function to create columns */
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

/* Helper function to create values */
fi_array* create_values(int64_t id, const char *name, int age, double salary) {
    fi_array *values = fi_array_create(4, sizeof(rdb_value_t*));
    if (!values) return NULL;
    
    rdb_value_t *id_val = rdb_create_int_value(id);
    rdb_value_t *name_val = rdb_create_string_value(name);
    rdb_value_t *age_val = rdb_create_int_value(age);
    rdb_value_t *salary_val = rdb_create_float_value(salary);
    
    if (!id_val || !name_val || !age_val || !salary_val) {
        if (id_val) rdb_value_free(id_val);
        if (name_val) rdb_value_free(name_val);
        if (age_val) rdb_value_free(age_val);
        if (salary_val) rdb_value_free(salary_val);
        fi_array_destroy(values);
        return NULL;
    }
    
    fi_array_push(values, &id_val);
    fi_array_push(values, &name_val);
    fi_array_push(values, &age_val);
    fi_array_push(values, &salary_val);
    
    return values;
}

/* Performance test function */
void performance_test(cached_rdb_t *cached_rdb) {
    printf("\n=== Performance Test ===\n");
    
    const int num_inserts = 1000;
    const int num_selects = 500;
    
    /* Test insert performance */
    printf("Testing insert performance with %d records...\n", num_inserts);
    clock_t start_time = clock();
    
    for (int i = 1; i <= num_inserts; i++) {
        fi_array *values = create_values(i, "Employee", 25 + (i % 50), 50000.0 + (i * 100));
        if (values) {
            cached_rdb_insert_row(cached_rdb, "employees", values);
            /* Free individual values */
            for (size_t j = 0; j < fi_array_count(values); j++) {
                rdb_value_t **val_ptr = (rdb_value_t**)fi_array_get(values, j);
                if (val_ptr && *val_ptr) {
                    rdb_value_free(*val_ptr);
                }
            }
            fi_array_destroy(values);
        }
    }
    
    clock_t end_time = clock();
    double insert_time = ((double)(end_time - start_time)) / CLOCKS_PER_SEC;
    printf("Inserted %d records in %.3f seconds (%.0f records/second)\n", 
           num_inserts, insert_time, num_inserts / insert_time);
    
    /* Test select performance */
    printf("\nTesting select performance with %d queries...\n", num_selects);
    start_time = clock();
    
    for (int i = 0; i < num_selects; i++) {
        /* Create where conditions for random employee */
        fi_array *where_conditions = fi_array_create(1, sizeof(char*));
        char condition[256];
        int random_id = (rand() % num_inserts) + 1;
        sprintf(condition, "id = %d", random_id);
        
        char *condition_ptr = malloc(strlen(condition) + 1);
        strcpy(condition_ptr, condition);
        fi_array_push(where_conditions, &condition_ptr);
        
        /* Select columns */
        fi_array *columns = fi_array_create(1, sizeof(char*));
        char *name_col = malloc(5);
        strcpy(name_col, "name");
        fi_array_push(columns, &name_col);
        
        fi_array *result = cached_rdb_select_rows(cached_rdb, "employees", columns, where_conditions);
        
        /* Clean up */
        if (result) fi_array_destroy(result);
        free(condition_ptr);
        free(name_col);
        fi_array_destroy(where_conditions);
        fi_array_destroy(columns);
    }
    
    end_time = clock();
    double select_time = ((double)(end_time - start_time)) / CLOCKS_PER_SEC;
    printf("Executed %d select queries in %.3f seconds (%.0f queries/second)\n", 
           num_selects, select_time, num_selects / select_time);
}

/* Cache hit ratio test */
void cache_hit_ratio_test(cached_rdb_t *cached_rdb) {
    printf("\n=== Cache Hit Ratio Test ===\n");
    
    /* Insert some test data */
    for (int i = 1; i <= 100; i++) {
        fi_array *values = create_values(i, "TestEmployee", 30, 60000.0);
        if (values) {
            cached_rdb_insert_row(cached_rdb, "employees", values);
            /* Free individual values */
            for (size_t j = 0; j < fi_array_count(values); j++) {
                rdb_value_t **val_ptr = (rdb_value_t**)fi_array_get(values, j);
                if (val_ptr && *val_ptr) {
                    rdb_value_free(*val_ptr);
                }
            }
            fi_array_destroy(values);
        }
    }
    
    /* First round - should be cache misses */
    printf("First round - accessing data (should be cache misses):\n");
    for (int i = 1; i <= 50; i++) {
        fi_array *where_conditions = fi_array_create(1, sizeof(char*));
        char condition[256];
        sprintf(condition, "id = %d", i);
        char *condition_ptr = malloc(strlen(condition) + 1);
        strcpy(condition_ptr, condition);
        fi_array_push(where_conditions, &condition_ptr);
        
        fi_array *columns = fi_array_create(1, sizeof(char*));
        char *name_col = malloc(5);
        strcpy(name_col, "name");
        fi_array_push(columns, &name_col);
        
        fi_array *result = cached_rdb_select_rows(cached_rdb, "employees", columns, where_conditions);
        
        if (result) fi_array_destroy(result);
        free(condition_ptr);
        free(name_col);
        fi_array_destroy(where_conditions);
        fi_array_destroy(columns);
    }
    
    /* Second round - should be cache hits */
    printf("Second round - accessing same data (should be cache hits):\n");
    for (int i = 1; i <= 50; i++) {
        fi_array *where_conditions = fi_array_create(1, sizeof(char*));
        char condition[256];
        sprintf(condition, "id = %d", i);
        char *condition_ptr = malloc(strlen(condition) + 1);
        strcpy(condition_ptr, condition);
        fi_array_push(where_conditions, &condition_ptr);
        
        fi_array *columns = fi_array_create(1, sizeof(char*));
        char *name_col = malloc(5);
        strcpy(name_col, "name");
        fi_array_push(columns, &name_col);
        
        fi_array *result = cached_rdb_select_rows(cached_rdb, "employees", columns, where_conditions);
        
        if (result) fi_array_destroy(result);
        free(condition_ptr);
        free(name_col);
        fi_array_destroy(where_conditions);
        fi_array_destroy(columns);
    }
}

/* Persistence test */
void persistence_test(cached_rdb_t *cached_rdb) {
    printf("\n=== Persistence Test ===\n");
    
    /* Insert some data */
    printf("Inserting test data...\n");
    for (int i = 1; i <= 10; i++) {
        fi_array *values = create_values(i, "PersistentEmployee", 25 + i, 50000.0 + (i * 1000));
        if (values) {
            cached_rdb_insert_row(cached_rdb, "employees", values);
            /* Free individual values */
            for (size_t j = 0; j < fi_array_count(values); j++) {
                rdb_value_t **val_ptr = (rdb_value_t**)fi_array_get(values, j);
                if (val_ptr && *val_ptr) {
                    rdb_value_free(*val_ptr);
                }
            }
            fi_array_destroy(values);
        }
    }
    
    /* Save to disk */
    printf("Saving database to disk...\n");
    if (cached_rdb_save(cached_rdb) == 0) {
        printf("Database saved successfully\n");
    } else {
        printf("Failed to save database\n");
    }
    
    /* Force checkpoint */
    printf("Performing checkpoint...\n");
    if (cached_rdb_checkpoint(cached_rdb) == 0) {
        printf("Checkpoint completed successfully\n");
    } else {
        printf("Failed to perform checkpoint\n");
    }
}

/* Configuration test */
void configuration_test(cached_rdb_t *cached_rdb) {
    printf("\n=== Configuration Test ===\n");
    
    /* Test cache level configuration */
    printf("Current cache configuration:\n");
    cached_rdb_print_stats(cached_rdb);
    
    /* Test auto-tuning */
    printf("\nTesting auto-tuning...\n");
    if (cached_rdb_set_auto_tuning(cached_rdb, true, 0.9) == 0) {
        printf("Auto-tuning enabled with 90%% target hit ratio\n");
    } else {
        printf("Failed to enable auto-tuning\n");
    }
    
    /* Test cache algorithm change */
    printf("\nTesting cache algorithm change...\n");
    if (cached_rdb_set_cache_algorithm(cached_rdb, 0, RDB_CACHE_AURA) == 0) {
        printf("Cache algorithm changed to AURA for level 0\n");
    } else {
        printf("Failed to change cache algorithm\n");
    }
}

int main() {
    printf("=== Cached RDB Demo with N-Level Cache and Persistence ===\n\n");
    
    /* Seed random number generator */
    srand(time(NULL));
    
    /* Get default configuration */
    cached_rdb_config_t *config = cached_rdb_get_default_config();
    if (!config) {
        printf("Error: Failed to create default configuration\n");
        return 1;
    }
    
    /* Customize configuration */
    config->cache_levels = 3;  /* Use 3 cache levels */
    config->enable_query_cache = true;
    config->enable_auto_tuning = true;
    config->target_hit_ratio = 0.85;
    
    /* Create cached RDB instance */
    cached_rdb_t *cached_rdb = cached_rdb_create("demo_database", config);
    if (!cached_rdb) {
        printf("Error: Failed to create cached RDB instance\n");
        cached_rdb_free_config(config);
        return 1;
    }
    
    /* Initialize cached RDB */
    if (cached_rdb_init(cached_rdb) != 0) {
        printf("Error: Failed to initialize cached RDB\n");
        cached_rdb_destroy(cached_rdb);
        cached_rdb_free_config(config);
        return 1;
    }
    
    /* Open cached RDB */
    if (cached_rdb_open(cached_rdb) != 0) {
        printf("Error: Failed to open cached RDB\n");
        cached_rdb_destroy(cached_rdb);
        cached_rdb_free_config(config);
        return 1;
    }
    
    printf("Cached RDB opened successfully\n");
    
    /* Create employees table */
    fi_array *emp_columns = fi_array_create(4, sizeof(rdb_column_t*));
    rdb_column_t *emp_id = create_column("id", RDB_TYPE_INT, true, true, false);
    rdb_column_t *emp_name = create_column("name", RDB_TYPE_VARCHAR, false, false, false);
    rdb_column_t *emp_age = create_column("age", RDB_TYPE_INT, false, false, false);
    rdb_column_t *emp_salary = create_column("salary", RDB_TYPE_FLOAT, false, false, false);
    
    fi_array_push(emp_columns, &emp_id);
    fi_array_push(emp_columns, &emp_name);
    fi_array_push(emp_columns, &emp_age);
    fi_array_push(emp_columns, &emp_salary);
    
    if (cached_rdb_create_table(cached_rdb, "employees", emp_columns) == 0) {
        printf("Table 'employees' created successfully\n");
    } else {
        printf("Error creating employees table\n");
        cached_rdb_close(cached_rdb);
        cached_rdb_destroy(cached_rdb);
        cached_rdb_free_config(config);
        return 1;
    }
    
    /* Run tests */
    printf("\n=== Running Tests ===\n");
    
    /* Basic functionality test */
    printf("1. Basic functionality test:\n");
    fi_array *values = create_values(1, "Alice Johnson", 28, 75000.0);
    if (values) {
        if (cached_rdb_insert_row(cached_rdb, "employees", values) == 0) {
            printf("   Employee inserted successfully\n");
        } else {
            printf("   Error inserting employee\n");
        }
        /* Free individual values */
        for (size_t i = 0; i < fi_array_count(values); i++) {
            rdb_value_t **val_ptr = (rdb_value_t**)fi_array_get(values, i);
            if (val_ptr && *val_ptr) {
                rdb_value_free(*val_ptr);
            }
        }
        fi_array_destroy(values);
    }
    
    /* Performance test */
    performance_test(cached_rdb);
    
    /* Cache hit ratio test */
    cache_hit_ratio_test(cached_rdb);
    
    /* Persistence test */
    persistence_test(cached_rdb);
    
    /* Configuration test */
    configuration_test(cached_rdb);
    
    /* Print final statistics */
    printf("\n=== Final Statistics ===\n");
    cached_rdb_print_stats(cached_rdb);
    
    /* Clean up */
    printf("\n=== Cleanup ===\n");
    cached_rdb_close(cached_rdb);
    cached_rdb_destroy(cached_rdb);
    cached_rdb_free_config(config);
    
    printf("Demo completed successfully!\n");
    return 0;
}
