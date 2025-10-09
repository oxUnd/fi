#include "rdb.h"
#include <pthread.h>
#include <unistd.h>
#include <time.h>
#include <sys/time.h>

#define NUM_THREADS 5
#define OPERATIONS_PER_THREAD 10

/* Global database instance */
rdb_database_t *global_db = NULL;

/* Thread function for concurrent insertions */
void* insert_thread(void* arg) {
    int thread_id = *(int*)arg;
    
    printf("Thread %d: Starting insertions\n", thread_id);
    
    for (int i = 0; i < OPERATIONS_PER_THREAD; i++) {
        /* Create values for insertion */
        fi_array *values = fi_array_create(3, sizeof(rdb_value_t*));
        if (!values) {
            printf("Thread %d: Failed to create values array\n", thread_id);
            continue;
        }
        
        /* Create row data */
        rdb_value_t *id_val = rdb_create_int_value(thread_id * 1000 + i);
        rdb_value_t *name_val = rdb_create_string_value("Thread User");
        rdb_value_t *age_val = rdb_create_int_value(20 + (i % 50));
        
        fi_array_push(values, &id_val);
        fi_array_push(values, &name_val);
        fi_array_push(values, &age_val);
        
        /* Insert using thread-safe function */
        int result = rdb_insert_row_thread_safe(global_db, "users", values);
        if (result == 0) {
            printf("Thread %d: Successfully inserted row %d\n", thread_id, i);
        } else {
            printf("Thread %d: Failed to insert row %d\n", thread_id, i);
        }
        
        /* Cleanup */
        rdb_value_free(id_val);
        rdb_value_free(name_val);
        rdb_value_free(age_val);
        fi_array_destroy(values);
        
        /* Small delay to simulate real work */
        sleep(0); /* Yield CPU */
    }
    
    printf("Thread %d: Finished insertions\n", thread_id);
    return NULL;
}

/* Thread function for concurrent reads */
void* read_thread(void* arg) {
    int thread_id = *(int*)arg;
    
    printf("Thread %d: Starting reads\n", thread_id);
    
    for (int i = 0; i < OPERATIONS_PER_THREAD; i++) {
        /* Create empty conditions for SELECT * */
        fi_array *columns = fi_array_create(0, sizeof(char*));
        fi_array *conditions = fi_array_create(0, sizeof(char*));
        
        if (!columns || !conditions) {
            printf("Thread %d: Failed to create query arrays\n", thread_id);
            continue;
        }
        
        /* Select using thread-safe function */
        fi_array *result = rdb_select_rows_thread_safe(global_db, "users", columns, conditions);
        if (result) {
            size_t count = fi_array_count(result);
            printf("Thread %d: Read %zu rows (operation %d)\n", thread_id, count, i);
            
            /* Cleanup result */
            for (size_t j = 0; j < count; j++) {
                rdb_row_t *row = *(rdb_row_t**)fi_array_get(result, j);
                if (row) {
                    rdb_row_free(row);
                }
            }
            fi_array_destroy(result);
        } else {
            printf("Thread %d: Failed to read rows (operation %d)\n", thread_id, i);
        }
        
        /* Cleanup */
        fi_array_destroy(columns);
        fi_array_destroy(conditions);
        
        /* Small delay to simulate real work */
        sleep(0); /* Yield CPU */
    }
    
    printf("Thread %d: Finished reads\n", thread_id);
    return NULL;
}

/* Thread function for concurrent updates */
void* update_thread(void* arg) {
    int thread_id = *(int*)arg;
    
    printf("Thread %d: Starting updates\n", thread_id);
    
    for (int i = 0; i < OPERATIONS_PER_THREAD; i++) {
        /* Create update data */
        fi_array *set_columns = fi_array_create(1, sizeof(char*));
        fi_array *set_values = fi_array_create(1, sizeof(rdb_value_t*));
        fi_array *conditions = fi_array_create(0, sizeof(char*));
        
        if (!set_columns || !set_values || !conditions) {
            printf("Thread %d: Failed to create update arrays\n", thread_id);
            continue;
        }
        
        /* Set age column */
        const char *age_col = "age";
        rdb_value_t *new_age = rdb_create_int_value(30 + (i % 20));
        
        fi_array_push(set_columns, &age_col);
        fi_array_push(set_values, &new_age);
        
        /* Update using thread-safe function */
        int result = rdb_update_rows_thread_safe(global_db, "users", set_columns, set_values, conditions);
        if (result >= 0) {
            printf("Thread %d: Updated %d rows (operation %d)\n", thread_id, result, i);
        } else {
            printf("Thread %d: Failed to update rows (operation %d)\n", thread_id, i);
        }
        
        /* Cleanup */
        rdb_value_free(new_age);
        fi_array_destroy(set_columns);
        fi_array_destroy(set_values);
        fi_array_destroy(conditions);
        
        /* Small delay to simulate real work */
        sleep(0); /* Yield CPU */
    }
    
    printf("Thread %d: Finished updates\n", thread_id);
    return NULL;
}

/* Helper function to create a column */
rdb_column_t* create_column(const char *name, rdb_data_type_t type, bool nullable, bool unique, bool primary_key) {
    rdb_column_t *col = malloc(sizeof(rdb_column_t));
    if (!col) return NULL;
    
    strncpy(col->name, name, sizeof(col->name) - 1);
    col->name[sizeof(col->name) - 1] = '\0';
    col->type = type;
    col->max_length = 0;
    col->nullable = nullable;
    col->primary_key = primary_key;
    col->unique = unique;
    col->default_value[0] = '\0';
    col->foreign_table[0] = '\0';
    col->foreign_column[0] = '\0';
    col->is_foreign_key = false;
    
    return col;
}

int main() {
    printf("=== FI RDB Thread Safety Demo ===\n\n");
    
    /* Create database */
    global_db = rdb_create_database("thread_safe_demo");
    if (!global_db) {
        printf("Failed to create database\n");
        return 1;
    }
    
    /* Open database */
    if (rdb_open_database(global_db) != 0) {
        printf("Failed to open database\n");
        rdb_destroy_database(global_db);
        return 1;
    }
    
    /* Create table */
    fi_array *columns = fi_array_create(3, sizeof(rdb_column_t*));
    if (!columns) {
        printf("Failed to create columns array\n");
        rdb_destroy_database(global_db);
        return 1;
    }
    
    rdb_column_t *id_col = create_column("id", RDB_TYPE_INT, false, true, true);
    rdb_column_t *name_col = create_column("name", RDB_TYPE_VARCHAR, false, false, false);
    rdb_column_t *age_col = create_column("age", RDB_TYPE_INT, false, false, false);
    
    if (!id_col || !name_col || !age_col) {
        printf("Failed to create columns\n");
        rdb_destroy_database(global_db);
        return 1;
    }
    
    fi_array_push(columns, &id_col);
    fi_array_push(columns, &name_col);
    fi_array_push(columns, &age_col);
    
    /* Create table using thread-safe function */
    if (rdb_create_table_thread_safe(global_db, "users", columns) != 0) {
        printf("Failed to create table\n");
        rdb_destroy_database(global_db);
        return 1;
    }
    
    printf("Database and table created successfully\n");
    printf("Starting concurrent operations with %d threads...\n\n", NUM_THREADS);
    
    /* Create threads */
    pthread_t threads[NUM_THREADS];
    int thread_ids[NUM_THREADS];
    
    /* Start time */
    clock_t start_time = clock();
    
    /* Create and start threads */
    for (int i = 0; i < NUM_THREADS; i++) {
        thread_ids[i] = i;
        
        /* Alternate between different operations */
        int result;
        switch (i % 3) {
            case 0:
                result = pthread_create(&threads[i], NULL, insert_thread, &thread_ids[i]);
                break;
            case 1:
                result = pthread_create(&threads[i], NULL, read_thread, &thread_ids[i]);
                break;
            case 2:
                result = pthread_create(&threads[i], NULL, update_thread, &thread_ids[i]);
                break;
        }
        
        if (result != 0) {
            printf("Failed to create thread %d\n", i);
        }
    }
    
    /* Wait for all threads to complete */
    for (int i = 0; i < NUM_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }
    
    /* End time */
    clock_t end_time = clock();
    double execution_time = ((double)(end_time - start_time)) / CLOCKS_PER_SEC;
    
    printf("\nAll threads completed!\n");
    printf("Total execution time: %.3f seconds\n", execution_time);
    
    /* Print final database state */
    printf("\n=== Final Database State ===\n");
    rdb_print_database_info(global_db);
    rdb_print_table_data(global_db, "users", 20);
    
    /* Cleanup */
    rdb_column_free(id_col);
    rdb_column_free(name_col);
    rdb_column_free(age_col);
    fi_array_destroy(columns);
    rdb_destroy_database(global_db);
    
    printf("\nThread safety demo completed successfully!\n");
    return 0;
}
