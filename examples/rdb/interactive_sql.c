#include "rdb.h"
#include "sql_parser.h"
#include "persistence.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

/* Global database instance */
static rdb_database_t *g_db = NULL;
static rdb_persistence_manager_t *g_pm = NULL;

/* Function prototypes */
void print_welcome_message(void);
void print_help_message(void);
void print_error_message(const char *message);
void print_success_message(const char *message);
int process_sql_command(const char *sql);
int execute_statement(const rdb_statement_t *stmt);
void print_query_result(fi_array *result, const rdb_statement_t *stmt);
void cleanup_and_exit(void);
int handle_special_commands(const char *input);
void print_database_status(void);
void print_table_list(void);
void print_table_schema(const char *table_name);
void print_persistence_status(void);
int initialize_persistence(const char *db_name, const char *data_dir, rdb_persistence_mode_t mode);
void print_usage(const char *program_name);

/* Main function */
int main(int argc, char *argv[]) {
    char *db_name = "interactive_db";
    char *data_dir = "./rdb_data";
    rdb_persistence_mode_t persistence_mode = RDB_PERSISTENCE_FULL;
    bool use_persistence = true;
    
    /* Parse command line arguments */
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--help") == 0 || strcmp(argv[i], "-h") == 0) {
            print_usage(argv[0]);
            return 0;
        } else if (strcmp(argv[i], "--no-persistence") == 0) {
            use_persistence = false;
        } else if (strcmp(argv[i], "--data-dir") == 0 && i + 1 < argc) {
            data_dir = argv[++i];
        } else if (strcmp(argv[i], "--persistence-mode") == 0 && i + 1 < argc) {
            if (strcmp(argv[i + 1], "memory") == 0) {
                persistence_mode = RDB_PERSISTENCE_MEMORY_ONLY;
            } else if (strcmp(argv[i + 1], "wal") == 0) {
                persistence_mode = RDB_PERSISTENCE_WAL_ONLY;
            } else if (strcmp(argv[i + 1], "checkpoint") == 0) {
                persistence_mode = RDB_PERSISTENCE_CHECKPOINT_ONLY;
            } else if (strcmp(argv[i + 1], "full") == 0) {
                persistence_mode = RDB_PERSISTENCE_FULL;
            } else {
                print_error_message("Invalid persistence mode. Use: memory, wal, checkpoint, or full");
                return 1;
            }
            i++;
        } else if (argv[i][0] != '-') {
            /* Database name (positional argument) */
            db_name = argv[i];
        } else {
            print_error_message("Unknown option");
            print_usage(argv[0]);
            return 1;
        }
    }
    
    /* Initialize input buffer */
    char input_buffer[1024];
    
    /* Initialize persistence if enabled */
    if (use_persistence) {
        if (initialize_persistence(db_name, data_dir, persistence_mode) != 0) {
            print_error_message("Failed to initialize persistence. Falling back to memory-only mode.");
            use_persistence = false;
        }
    }
    
    /* Create and open database */
    if (!use_persistence) {
        g_db = rdb_create_database(db_name);
        if (!g_db) {
            print_error_message("Failed to create database");
            return 1;
        }
        
        if (rdb_open_database(g_db) != 0) {
            print_error_message("Failed to open database");
            rdb_destroy_database(g_db);
            return 1;
        }
    }
    
    /* Set up cleanup on exit */
    atexit(cleanup_and_exit);
    
    /* Print welcome message */
    print_welcome_message();
    
    /* Main interactive loop */
    while (1) {
        /* Read input using fgets */
        printf("rdb> ");
        fflush(stdout);
        
        if (!fgets(input_buffer, sizeof(input_buffer), stdin)) {
            /* EOF or error */
            printf("\n");
            break;
        }
        
        /* Remove newline character */
        input_buffer[strcspn(input_buffer, "\n")] = 0;
        
        /* Skip empty lines */
        if (strlen(input_buffer) == 0) {
            continue;
        }
        
        /* Handle special commands */
        if (handle_special_commands(input_buffer)) {
            continue;
        }
        
        /* Process SQL command */
        if (process_sql_command(input_buffer) != 0) {
            /* Error already printed */
        }
    }
    
    return 0;
}

/* Print welcome message */
void print_welcome_message(void) {
    printf("========================================\n");
    printf("    FI Relational Database Interactive\n");
    printf("========================================\n");
    printf("Welcome to the interactive SQL client!\n");
    printf("Type 'help' for available commands.\n");
    printf("Type 'quit' or 'exit' to exit.\n");
    printf("========================================\n\n");
    
    print_database_status();
}

/* Print help message */
void print_help_message(void) {
    printf("\n=== Available Commands ===\n");
    printf("SQL Commands:\n");
    printf("  CREATE TABLE <name> (<column_definitions>)\n");
    printf("  DROP TABLE <name>\n");
    printf("  INSERT INTO <table> VALUES (<values>)\n");
    printf("  SELECT <columns> FROM <table> [WHERE <conditions>]\n");
    printf("  UPDATE <table> SET <column>=<value> [WHERE <conditions>]\n");
    printf("  DELETE FROM <table> [WHERE <conditions>]\n");
    printf("  CREATE INDEX <name> ON <table> (<column>)\n");
    printf("  DROP INDEX <name>\n");
    printf("  BEGIN TRANSACTION\n");
    printf("  COMMIT\n");
    printf("  ROLLBACK\n");
    printf("\nSpecial Commands:\n");
    printf("  help          - Show this help message\n");
    printf("  tables        - List all tables\n");
    printf("  schema <table> - Show table schema\n");
    printf("  status        - Show database status\n");
    printf("  persistence   - Show persistence status and statistics\n");
    printf("  checkpoint    - Force a checkpoint (if persistence enabled)\n");
    printf("  quit/exit     - Exit the program\n");
    printf("  clear         - Clear screen\n");
    printf("\nExamples:\n");
    printf("  CREATE TABLE students (id INT PRIMARY KEY, name VARCHAR(50), age INT)\n");
    printf("  INSERT INTO students VALUES (1, 'Alice', 20)\n");
    printf("  SELECT * FROM students WHERE age > 18\n");
    printf("  UPDATE students SET age = 21 WHERE name = 'Alice'\n");
    printf("  DELETE FROM students WHERE id = 1\n");
    printf("========================\n\n");
}

/* Print error message */
void print_error_message(const char *message) {
    printf("ERROR: %s\n", message);
}

/* Print success message */
void print_success_message(const char *message) {
    printf("SUCCESS: %s\n", message);
}

/* Process SQL command */
int process_sql_command(const char *sql) {
    sql_parser_t *parser = NULL;
    rdb_statement_t *stmt = NULL;
    int result = 0;
    
    /* Create parser */
    parser = sql_parser_create(sql);
    if (!parser) {
        print_error_message("Failed to create SQL parser");
        return -1;
    }
    
    /* Parse statement */
    stmt = sql_parse_statement(parser);
    if (!stmt) {
        if (sql_parser_has_error(parser)) {
            print_error_message(sql_parser_get_error(parser));
        } else {
            print_error_message("Failed to parse SQL statement");
        }
        result = -1;
        goto cleanup;
    }
    
    /* Execute statement */
    result = execute_statement(stmt);
    
cleanup:
    if (stmt) {
        sql_statement_free(stmt);
    }
    if (parser) {
        sql_parser_destroy(parser);
    }
    
    return result;
}

/* Execute parsed statement */
int execute_statement(const rdb_statement_t *stmt) {
    int result = 0;
    fi_array *query_result = NULL;
    
    switch (stmt->type) {
        case RDB_STMT_CREATE_TABLE:
            result = rdb_create_table_thread_safe(g_db, stmt->table_name, stmt->columns);
            if (result == 0) {
                print_success_message("Table created successfully");
                /* Save to persistence if enabled */
                if (g_pm) {
                    rdb_persistence_save_database(g_pm, g_db);
                }
            } else {
                print_error_message("Failed to create table");
            }
            break;
            
        case RDB_STMT_DROP_TABLE:
            result = rdb_drop_table_thread_safe(g_db, stmt->table_name);
            if (result == 0) {
                print_success_message("Table dropped successfully");
                /* Save to persistence if enabled */
                if (g_pm) {
                    rdb_persistence_save_database(g_pm, g_db);
                }
            } else {
                print_error_message("Failed to drop table");
            }
            break;
            
        case RDB_STMT_INSERT:
            result = rdb_insert_row_thread_safe(g_db, stmt->table_name, stmt->values);
            if (result == 0) {
                print_success_message("Row inserted successfully");
                /* Save to persistence if enabled */
                if (g_pm) {
                    rdb_persistence_save_database(g_pm, g_db);
                }
            } else {
                print_error_message("Failed to insert row");
            }
            break;
            
        case RDB_STMT_SELECT:
            if (stmt->from_tables && fi_array_count(stmt->from_tables) > 1) {
                /* Multi-table query */
                query_result = rdb_select_join(g_db, stmt);
            } else {
                /* Single table query */
                const char *table_name = NULL;
                if (stmt->from_tables && fi_array_count(stmt->from_tables) > 0) {
                    table_name = *(char**)fi_array_get(stmt->from_tables, 0);
                } else {
                    table_name = stmt->table_name;
                }
                query_result = rdb_select_rows_thread_safe(g_db, table_name, 
                                                          stmt->select_columns, 
                                                          stmt->where_conditions);
            }
            
            if (query_result) {
                print_query_result(query_result, stmt);
                fi_array_destroy(query_result);
                result = 0;
            } else {
                print_error_message("Query failed or returned no results");
                result = -1;
            }
            break;
            
        case RDB_STMT_UPDATE:
            result = rdb_update_rows_thread_safe(g_db, stmt->table_name, 
                                                stmt->columns, stmt->values, 
                                                stmt->where_conditions);
            if (result == 0) {
                print_success_message("Rows updated successfully");
                /* Save to persistence if enabled */
                if (g_pm) {
                    rdb_persistence_save_database(g_pm, g_db);
                }
            } else {
                print_error_message("Failed to update rows");
            }
            break;
            
        case RDB_STMT_DELETE:
            result = rdb_delete_rows_thread_safe(g_db, stmt->table_name, 
                                                stmt->where_conditions);
            if (result == 0) {
                print_success_message("Rows deleted successfully");
                /* Save to persistence if enabled */
                if (g_pm) {
                    rdb_persistence_save_database(g_pm, g_db);
                }
            } else {
                print_error_message("Failed to delete rows");
            }
            break;
            
        case RDB_STMT_CREATE_INDEX:
            result = rdb_create_index(g_db, stmt->table_name, stmt->index_name, stmt->index_column);
            if (result == 0) {
                print_success_message("Index created successfully");
                /* Save to persistence if enabled */
                if (g_pm) {
                    rdb_persistence_save_database(g_pm, g_db);
                }
            } else {
                print_error_message("Failed to create index");
            }
            break;
            
        case RDB_STMT_DROP_INDEX:
            result = rdb_drop_index(g_db, stmt->table_name, stmt->index_name);
            if (result == 0) {
                print_success_message("Index dropped successfully");
                /* Save to persistence if enabled */
                if (g_pm) {
                    rdb_persistence_save_database(g_pm, g_db);
                }
            } else {
                print_error_message("Failed to drop index");
            }
            break;
            
        case RDB_STMT_BEGIN_TRANSACTION:
            result = rdb_begin_transaction(g_db, RDB_ISOLATION_READ_COMMITTED);
            if (result == 0) {
                print_success_message("Transaction started");
            } else {
                print_error_message("Failed to start transaction");
            }
            break;
            
        case RDB_STMT_COMMIT_TRANSACTION:
            result = rdb_commit_transaction(g_db);
            if (result == 0) {
                print_success_message("Transaction committed");
            } else {
                print_error_message("Failed to commit transaction");
            }
            break;
            
        case RDB_STMT_ROLLBACK_TRANSACTION:
            result = rdb_rollback_transaction(g_db);
            if (result == 0) {
                print_success_message("Transaction rolled back");
            } else {
                print_error_message("Failed to rollback transaction");
            }
            break;
            
        default:
            print_error_message("Unsupported statement type");
            result = -1;
            break;
    }
    
    return result;
}

/* Print query result */
void print_query_result(fi_array *result, const rdb_statement_t *stmt) {
    if (!result || fi_array_count(result) == 0) {
        printf("No rows returned.\n");
        return;
    }
    
    size_t num_rows = fi_array_count(result);
    printf("\nQuery returned %zu row(s):\n", num_rows);
    printf("----------------------------------------\n");
    
    /* Print results based on statement type */
    if (stmt->from_tables && fi_array_count(stmt->from_tables) > 1) {
        /* Multi-table result */
        rdb_print_join_result(result, stmt);
    } else {
        /* Single table result - print first 20 rows */
        size_t limit = (num_rows > 20) ? 20 : num_rows;
        for (size_t i = 0; i < limit; i++) {
            rdb_row_t **row_ptr = (rdb_row_t**)fi_array_get(result, i);
            if (row_ptr && *row_ptr) {
                rdb_row_t *row = *row_ptr;
                printf("Row %zu: ", row->row_id);
                
                /* Print column values */
                for (size_t j = 0; j < fi_array_count(row->values); j++) {
                    rdb_value_t **val_ptr = (rdb_value_t**)fi_array_get(row->values, j);
                    if (val_ptr && *val_ptr) {
                        char *str = rdb_value_to_string(*val_ptr);
                        printf("%s", str);
                        free(str);
                        if (j < fi_array_count(row->values) - 1) {
                            printf(" | ");
                        }
                    }
                }
                printf("\n");
            }
        }
        
        if (num_rows > 20) {
            printf("... and %zu more rows\n", num_rows - 20);
        }
    }
    
    printf("----------------------------------------\n");
}

/* Handle special commands */
int handle_special_commands(const char *input) {
    /* Convert to lowercase for comparison */
    char *lower_input = malloc(strlen(input) + 1);
    if (!lower_input) return 0;
    
    strcpy(lower_input, input);
    for (char *p = lower_input; *p; p++) {
        *p = tolower(*p);
    }
    
    int handled = 0;
    
    if (strcmp(lower_input, "help") == 0) {
        print_help_message();
        handled = 1;
    } else if (strcmp(lower_input, "quit") == 0 || strcmp(lower_input, "exit") == 0) {
        printf("Goodbye!\n");
        exit(0);
    } else if (strcmp(lower_input, "tables") == 0) {
        print_table_list();
        handled = 1;
    } else if (strncmp(lower_input, "schema ", 7) == 0) {
        char *table_name = lower_input + 7;
        while (*table_name == ' ') table_name++; /* Skip spaces */
        print_table_schema(table_name);
        handled = 1;
    } else if (strcmp(lower_input, "status") == 0) {
        print_database_status();
        handled = 1;
    } else if (strcmp(lower_input, "clear") == 0) {
        printf("\033[2J\033[H"); /* Clear screen */
        handled = 1;
    } else if (strcmp(lower_input, "persistence") == 0) {
        print_persistence_status();
        handled = 1;
    } else if (strcmp(lower_input, "checkpoint") == 0) {
        if (g_pm) {
            int result = rdb_persistence_force_checkpoint(g_pm, g_db);
            if (result == 0) {
                print_success_message("Checkpoint completed successfully");
            } else {
                print_error_message("Failed to perform checkpoint");
            }
        } else {
            print_error_message("Persistence not enabled");
        }
        handled = 1;
    }
    
    free(lower_input);
    return handled;
}

/* Print database status */
void print_database_status(void) {
    printf("Database: %s\n", g_db->name);
    printf("Status: %s\n", g_db->is_open ? "Open" : "Closed");
    
    if (rdb_is_in_transaction(g_db)) {
        rdb_transaction_t *tx = rdb_get_current_transaction(g_db);
        if (tx) {
            printf("Transaction: Active (ID: %zu, Level: %s)\n", 
                   tx->transaction_id, 
                   rdb_isolation_level_to_string(tx->isolation));
        }
    } else {
        printf("Transaction: None\n");
    }
    
    printf("Tables: %zu\n", fi_map_size(g_db->tables));
    printf("Foreign Keys: %zu\n", fi_map_size(g_db->foreign_keys));
    
    if (g_pm) {
        printf("Persistence: Enabled (%s)\n", 
               g_pm->mode == RDB_PERSISTENCE_MEMORY_ONLY ? "Memory Only" :
               g_pm->mode == RDB_PERSISTENCE_WAL_ONLY ? "WAL Only" :
               g_pm->mode == RDB_PERSISTENCE_CHECKPOINT_ONLY ? "Checkpoint Only" :
               g_pm->mode == RDB_PERSISTENCE_FULL ? "Full" : "Unknown");
        printf("Data Directory: %s\n", g_pm->data_dir);
    } else {
        printf("Persistence: Disabled\n");
    }
    
    printf("\n");
}

/* Print table list */
void print_table_list(void) {
    if (fi_map_size(g_db->tables) == 0) {
        printf("No tables in database.\n");
        return;
    }
    
    printf("\nTables in database:\n");
    printf("-------------------\n");
    
    fi_map_iterator iter = fi_map_iterator_create(g_db->tables);
    if (iter.is_valid) {
        char **table_name = (char**)fi_map_iterator_key(&iter);
        rdb_table_t **table_ptr = (rdb_table_t**)fi_map_iterator_value(&iter);
        
        if (table_name && *table_name && table_ptr && *table_ptr) {
            rdb_table_t *table = *table_ptr;
            printf("%-20s (%zu columns, %zu rows)\n", 
                   *table_name, 
                   fi_array_count(table->columns),
                   fi_array_count(table->rows));
        }
        
        while (fi_map_iterator_next(&iter)) {
            table_name = (char**)fi_map_iterator_key(&iter);
            table_ptr = (rdb_table_t**)fi_map_iterator_value(&iter);
            
            if (table_name && *table_name && table_ptr && *table_ptr) {
                rdb_table_t *table = *table_ptr;
                printf("%-20s (%zu columns, %zu rows)\n", 
                       *table_name, 
                       fi_array_count(table->columns),
                       fi_array_count(table->rows));
            }
        }
    }
    printf("-------------------\n\n");
}

/* Print table schema */
void print_table_schema(const char *table_name) {
    if (!table_name || strlen(table_name) == 0) {
        print_error_message("Table name required");
        return;
    }
    
    rdb_table_t *table = rdb_get_table(g_db, table_name);
    if (!table) {
        print_error_message("Table not found");
        return;
    }
    
    printf("\nSchema for table '%s':\n", table_name);
    printf("----------------------------------------\n");
    
    for (size_t i = 0; i < fi_array_count(table->columns); i++) {
        rdb_column_t **col_ptr = (rdb_column_t**)fi_array_get(table->columns, i);
        if (col_ptr && *col_ptr) {
            rdb_column_t *col = *col_ptr;
            printf("%-15s %-10s", col->name, rdb_type_to_string(col->type));
            
            if (col->primary_key) printf(" PRIMARY KEY");
            if (col->unique) printf(" UNIQUE");
            if (!col->nullable) printf(" NOT NULL");
            if (col->is_foreign_key) {
                printf(" REFERENCES %s(%s)", col->foreign_table, col->foreign_column);
            }
            printf("\n");
        }
    }
    
    printf("----------------------------------------\n\n");
}

/* Cleanup and exit */
void cleanup_and_exit(void) {
    if (g_pm && g_db) {
        /* Save database state before shutdown */
        rdb_persistence_save_database(g_pm, g_db);
        rdb_persistence_close_database(g_pm, g_db);
        rdb_persistence_shutdown(g_pm);
        rdb_persistence_destroy(g_pm);
        rdb_destroy_database(g_db);
    } else if (g_db) {
        rdb_close_database(g_db);
        rdb_destroy_database(g_db);
    }
}

/* Print persistence status and statistics */
void print_persistence_status(void) {
    if (!g_pm) {
        printf("Persistence: Not enabled\n");
        return;
    }
    
    printf("\n=== Persistence Status ===\n");
    printf("Mode: %s\n", 
           g_pm->mode == RDB_PERSISTENCE_MEMORY_ONLY ? "Memory Only" :
           g_pm->mode == RDB_PERSISTENCE_WAL_ONLY ? "WAL Only" :
           g_pm->mode == RDB_PERSISTENCE_CHECKPOINT_ONLY ? "Checkpoint Only" :
           g_pm->mode == RDB_PERSISTENCE_FULL ? "Full" : "Unknown");
    printf("Data Directory: %s\n", g_pm->data_dir);
    printf("Database File: %s\n", g_pm->db_file_path ? g_pm->db_file_path : "Not set");
    
    if (g_pm->wal) {
        printf("WAL: Enabled (Path: %s)\n", g_pm->wal->wal_path);
        printf("WAL Sequence: %lu\n", g_pm->wal->sequence_number);
        printf("WAL Entries: %lu\n", g_pm->wal_entries);
    } else {
        printf("WAL: Disabled\n");
    }
    
    if (g_pm->page_cache) {
        printf("Page Cache: Enabled (Max: %zu, Current: %zu)\n", 
               g_pm->page_cache->max_pages, g_pm->page_cache->current_pages);
        printf("Cache Hits: %lu, Misses: %lu\n", 
               g_pm->page_cache->hit_count, g_pm->page_cache->miss_count);
    } else {
        printf("Page Cache: Disabled\n");
    }
    
    printf("Statistics:\n");
    printf("  Total Writes: %lu\n", g_pm->total_writes);
    printf("  Total Reads: %lu\n", g_pm->total_reads);
    printf("  Checkpoints: %lu\n", g_pm->checkpoint_count);
    printf("  Last Checkpoint: %s", ctime(&g_pm->last_checkpoint));
    printf("  Checkpoint Interval: %lu seconds\n", g_pm->checkpoint_interval);
    
    printf("========================\n\n");
}

/* Initialize persistence system */
int initialize_persistence(const char *db_name, const char *data_dir, rdb_persistence_mode_t mode) {
    /* Create persistence manager */
    g_pm = rdb_persistence_create(data_dir, mode);
    if (!g_pm) {
        return -1;
    }
    
    /* Initialize persistence manager */
    if (rdb_persistence_init(g_pm) != 0) {
        rdb_persistence_destroy(g_pm);
        g_pm = NULL;
        return -1;
    }
    
    /* Create database */
    g_db = rdb_create_database(db_name);
    if (!g_db) {
        rdb_persistence_shutdown(g_pm);
        rdb_persistence_destroy(g_pm);
        g_pm = NULL;
        return -1;
    }
    
    /* Open database with persistence */
    if (rdb_persistence_open_database(g_pm, g_db) != 0) {
        rdb_destroy_database(g_db);
        rdb_persistence_shutdown(g_pm);
        rdb_persistence_destroy(g_pm);
        g_pm = NULL;
        g_db = NULL;
        return -1;
    }
    
    return 0;
}

/* Print usage information */
void print_usage(const char *program_name) {
    printf("Usage: %s [OPTIONS] [DATABASE_NAME]\n", program_name);
    printf("\nOptions:\n");
    printf("  -h, --help                    Show this help message\n");
    printf("  --no-persistence              Disable persistence (memory-only mode)\n");
    printf("  --data-dir DIR                Set data directory for persistence (default: ./rdb_data)\n");
    printf("  --persistence-mode MODE       Set persistence mode (default: full)\n");
    printf("                                Modes: memory, wal, checkpoint, full\n");
    printf("\nExamples:\n");
    printf("  %s                                    # Use default settings\n", program_name);
    printf("  %s my_database                         # Use specific database name\n", program_name);
    printf("  %s --no-persistence                   # Disable persistence\n", program_name);
    printf("  %s --data-dir /tmp/rdb my_db          # Use custom data directory\n", program_name);
    printf("  %s --persistence-mode wal my_db       # Use WAL-only persistence\n", program_name);
    printf("\nPersistence Modes:\n");
    printf("  memory      - No persistence, data lost on exit\n");
    printf("  wal         - Write-ahead log only\n");
    printf("  checkpoint  - Periodic checkpoints only\n");
    printf("  full        - WAL + checkpoints (recommended)\n");
}
