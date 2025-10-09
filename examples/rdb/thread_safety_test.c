#include "rdb.h"
#include <pthread.h>
#include <unistd.h>
#include <assert.h>
#include <sys/time.h>

#define NUM_THREADS 3
#define OPERATIONS_PER_THREAD 5

/* Global database instance */
rdb_database_t *test_db = NULL;

/* Test thread function for concurrent operations */
void* test_thread(void* arg) {
    int thread_id = *(int*)arg;
    
    printf("Test Thread %d: Starting operations\n", thread_id);
    
    for (int i = 0; i < OPERATIONS_PER_THREAD; i++) {
        /* Test INSERT */
        fi_array *values = fi_array_create(2, sizeof(rdb_value_t*));
        if (values) {
            rdb_value_t *id_val = rdb_create_int_value(thread_id * 100 + i);
            rdb_value_t *name_val = rdb_create_string_value("Test User");
            
            fi_array_push(values, &id_val);
            fi_array_push(values, &name_val);
            
            int result = rdb_insert_row_thread_safe(test_db, "test_table", values);
            assert(result == 0); /* Should succeed */
            
            rdb_value_free(id_val);
            rdb_value_free(name_val);
            fi_array_destroy(values);
        }
        
        /* Test SELECT */
        fi_array *columns = fi_array_create(0, sizeof(char*));
        fi_array *conditions = fi_array_create(0, sizeof(char*));
        
        if (columns && conditions) {
            fi_array *result = rdb_select_rows_thread_safe(test_db, "test_table", columns, conditions);
            assert(result != NULL); /* Should succeed */
            
            /* Cleanup result */
            if (result) {
                for (size_t j = 0; j < fi_array_count(result); j++) {
                    rdb_row_t *row = *(rdb_row_t**)fi_array_get(result, j);
                    if (row) {
                        rdb_row_free(row);
                    }
                }
                fi_array_destroy(result);
            }
            
            fi_array_destroy(columns);
            fi_array_destroy(conditions);
        }
        
        /* Small delay */
        sleep(0);
    }
    
    printf("Test Thread %d: Completed operations\n", thread_id);
    return NULL;
}

/* Helper function to create a column */
rdb_column_t* create_test_column(const char *name, rdb_data_type_t type, bool nullable, bool unique, bool primary_key) {
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
    printf("=== FI RDB Thread Safety Test ===\n\n");
    
    /* Create database */
    test_db = rdb_create_database("thread_safety_test");
    assert(test_db != NULL);
    
    /* Open database */
    int result = rdb_open_database(test_db);
    assert(result == 0);
    
    /* Create table */
    fi_array *columns = fi_array_create(2, sizeof(rdb_column_t*));
    assert(columns != NULL);
    
    rdb_column_t *id_col = create_test_column("id", RDB_TYPE_INT, false, true, true);
    rdb_column_t *name_col = create_test_column("name", RDB_TYPE_VARCHAR, false, false, false);
    
    assert(id_col != NULL);
    assert(name_col != NULL);
    
    fi_array_push(columns, &id_col);
    fi_array_push(columns, &name_col);
    
    /* Create table using thread-safe function */
    result = rdb_create_table_thread_safe(test_db, "test_table", columns);
    assert(result == 0);
    
    printf("Database and table created successfully\n");
    printf("Starting thread safety test with %d threads...\n\n", NUM_THREADS);
    
    /* Create threads */
    pthread_t threads[NUM_THREADS];
    int thread_ids[NUM_THREADS];
    
    /* Create and start threads */
    for (int i = 0; i < NUM_THREADS; i++) {
        thread_ids[i] = i;
        int create_result = pthread_create(&threads[i], NULL, test_thread, &thread_ids[i]);
        assert(create_result == 0);
    }
    
    /* Wait for all threads to complete */
    for (int i = 0; i < NUM_THREADS; i++) {
        int join_result = pthread_join(threads[i], NULL);
        assert(join_result == 0);
    }
    
    printf("\nAll test threads completed successfully!\n");
    
    /* Verify final state */
    printf("\n=== Final Database State ===\n");
    rdb_print_database_info(test_db);
    rdb_print_table_data(test_db, "test_table", 10);
    
    /* Cleanup */
    rdb_column_free(id_col);
    rdb_column_free(name_col);
    fi_array_destroy(columns);
    rdb_destroy_database(test_db);
    
    printf("\nThread safety test PASSED!\n");
    return 0;
}
