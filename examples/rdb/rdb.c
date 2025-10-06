#include "rdb.h"

/* Memory management functions */
void rdb_value_free(void *value) {
    if (!value) return;
    
    rdb_value_t *val = (rdb_value_t*)value;
    if (val->type == RDB_TYPE_VARCHAR || val->type == RDB_TYPE_TEXT) {
        if (val->data.string_val) {
            free(val->data.string_val);
        }
    }
    free(val);
}

void rdb_row_free(void *row) {
    if (!row) return;
    
    rdb_row_t *r = (rdb_row_t*)row;
    if (r->values) {
        fi_array_destroy(r->values);
    }
    free(r);
}

void rdb_column_free(void *column) {
    if (!column) return;
    free(column);
}

/* Database operations */
rdb_database_t* rdb_create_database(const char *name) {
    if (!name) return NULL;
    
    rdb_database_t *db = malloc(sizeof(rdb_database_t));
    if (!db) return NULL;
    
    strncpy(db->name, name, sizeof(db->name) - 1);
    db->name[sizeof(db->name) - 1] = '\0';
    
    db->tables = fi_map_create(16, sizeof(char*), sizeof(rdb_table_t*), 
                               fi_map_hash_string, fi_map_compare_string);
    if (!db->tables) {
        free(db);
        return NULL;
    }
    
    db->is_open = true;
    return db;
}

int rdb_open_database(rdb_database_t *db) {
    if (!db) return -1;
    
    db->is_open = true;
    return 0;
}

int rdb_close_database(rdb_database_t *db) {
    if (!db) return -1;
    
    db->is_open = false;
    return 0;
}

void rdb_destroy_database(rdb_database_t *db) {
    if (!db) return;
    
    if (db->tables) {
        fi_map_iterator iter = fi_map_iterator_create(db->tables);
        
        /* Handle first element if iterator is valid */
        if (iter.is_valid) {
            rdb_table_t **table_ptr = (rdb_table_t**)fi_map_iterator_value(&iter);
            if (table_ptr && *table_ptr) {
                rdb_destroy_table(*table_ptr);
            }
        }
        
        /* Handle remaining elements */
        while (fi_map_iterator_next(&iter)) {
            rdb_table_t **table_ptr = (rdb_table_t**)fi_map_iterator_value(&iter);
            if (table_ptr && *table_ptr) {
                rdb_destroy_table(*table_ptr);
            }
        }
        fi_map_destroy(db->tables);
    }
    
    free(db);
}

/* Table operations */
rdb_table_t* rdb_create_table_object(const char *name, fi_array *columns) {
    if (!name || !columns) return NULL;
    
    rdb_table_t *table = malloc(sizeof(rdb_table_t));
    if (!table) return NULL;
    
    strncpy(table->name, name, sizeof(table->name) - 1);
    table->name[sizeof(table->name) - 1] = '\0';
    
    table->columns = fi_array_copy(columns);
    if (!table->columns) {
        free(table);
        return NULL;
    }
    
    table->rows = fi_array_create(100, sizeof(rdb_row_t*));
    if (!table->rows) {
        fi_array_destroy(table->columns);
        free(table);
        return NULL;
    }
    
    table->indexes = fi_map_create(8, sizeof(char*), sizeof(fi_btree*), 
                                   fi_map_hash_string, fi_map_compare_string);
    if (!table->indexes) {
        fi_array_destroy(table->columns);
        fi_array_destroy(table->rows);
        free(table);
        return NULL;
    }
    
    table->primary_key[0] = '\0';
    table->next_row_id = 1;
    
    /* Find primary key column */
    for (size_t i = 0; i < fi_array_count(table->columns); i++) {
        rdb_column_t *col = (rdb_column_t*)fi_array_get(table->columns, i);
        if (col && col->primary_key) {
            strncpy(table->primary_key, col->name, sizeof(table->primary_key) - 1);
            table->primary_key[sizeof(table->primary_key) - 1] = '\0';
            break;
        }
    }
    
    return table;
}

void rdb_destroy_table(rdb_table_t *table) {
    if (!table) return;
    
    if (table->columns) {
        fi_array_destroy(table->columns);
    }
    
    if (table->rows) {
        fi_array_destroy(table->rows);
    }
    
    if (table->indexes) {
        fi_map_iterator iter = fi_map_iterator_create(table->indexes);
        
        /* Handle first element if iterator is valid */
        if (iter.is_valid) {
            fi_btree **tree_ptr = (fi_btree**)fi_map_iterator_value(&iter);
            if (tree_ptr && *tree_ptr) {
                fi_btree_destroy(*tree_ptr);
            }
        }
        
        /* Handle remaining elements */
        while (fi_map_iterator_next(&iter)) {
            fi_btree **tree_ptr = (fi_btree**)fi_map_iterator_value(&iter);
            if (tree_ptr && *tree_ptr) {
                fi_btree_destroy(*tree_ptr);
            }
        }
        fi_map_destroy(table->indexes);
    }
    
    free(table);
}

int rdb_create_table(rdb_database_t *db, const char *table_name, fi_array *columns) {
    if (!db || !table_name || !columns) return -1;
    
    if (!db->is_open) return -1;
    
    if (rdb_table_exists(db, table_name)) {
        printf("Error: Table '%s' already exists\n", table_name);
        return -1;
    }
    
    rdb_table_t *table = rdb_create_table_object(table_name, columns);
    if (!table) return -1;
    
    if (fi_map_put(db->tables, &table_name, &table) != 0) {
        rdb_destroy_table(table);
        return -1;
    }
    
    printf("Table '%s' created successfully\n", table_name);
    return 0;
}

int rdb_drop_table(rdb_database_t *db, const char *table_name) {
    if (!db || !table_name) return -1;
    
    if (!db->is_open) return -1;
    
    rdb_table_t *table = NULL;
    if (fi_map_get(db->tables, &table_name, &table) != 0) {
        printf("Error: Table '%s' does not exist\n", table_name);
        return -1;
    }
    
    fi_map_remove(db->tables, &table_name);
    rdb_destroy_table(table);
    
    printf("Table '%s' dropped successfully\n", table_name);
    return 0;
}

rdb_table_t* rdb_get_table(rdb_database_t *db, const char *table_name) {
    if (!db || !table_name) return NULL;
    
    rdb_table_t *table = NULL;
    if (fi_map_get(db->tables, &table_name, &table) != 0) {
        return NULL;
    }
    
    return table;
}

bool rdb_table_exists(rdb_database_t *db, const char *table_name) {
    if (!db || !table_name) return false;
    return fi_map_contains(db->tables, &table_name);
}

/* Value creation functions */
rdb_value_t* rdb_create_int_value(int64_t value) {
    rdb_value_t *val = malloc(sizeof(rdb_value_t));
    if (!val) return NULL;
    
    val->type = RDB_TYPE_INT;
    val->data.int_val = value;
    val->is_null = false;
    return val;
}

rdb_value_t* rdb_create_float_value(double value) {
    rdb_value_t *val = malloc(sizeof(rdb_value_t));
    if (!val) return NULL;
    
    val->type = RDB_TYPE_FLOAT;
    val->data.float_val = value;
    val->is_null = false;
    return val;
}

rdb_value_t* rdb_create_string_value(const char *value) {
    if (!value) return NULL;
    
    rdb_value_t *val = malloc(sizeof(rdb_value_t));
    if (!val) return NULL;
    
    val->type = RDB_TYPE_VARCHAR;
    val->data.string_val = malloc(strlen(value) + 1);
    if (!val->data.string_val) {
        free(val);
        return NULL;
    }
    
    strcpy(val->data.string_val, value);
    val->is_null = false;
    return val;
}

rdb_value_t* rdb_create_bool_value(bool value) {
    rdb_value_t *val = malloc(sizeof(rdb_value_t));
    if (!val) return NULL;
    
    val->type = RDB_TYPE_BOOLEAN;
    val->data.bool_val = value;
    val->is_null = false;
    return val;
}

rdb_value_t* rdb_create_null_value(rdb_data_type_t type) {
    rdb_value_t *val = malloc(sizeof(rdb_value_t));
    if (!val) return NULL;
    
    val->type = type;
    val->is_null = true;
    memset(&val->data, 0, sizeof(val->data));
    return val;
}

/* Value access functions */
int64_t rdb_get_int_value(const rdb_value_t *value) {
    if (!value || value->is_null) return 0;
    return value->data.int_val;
}

double rdb_get_float_value(const rdb_value_t *value) {
    if (!value || value->is_null) return 0.0;
    return value->data.float_val;
}

const char* rdb_get_string_value(const rdb_value_t *value) {
    if (!value || value->is_null) return NULL;
    return value->data.string_val;
}

bool rdb_get_bool_value(const rdb_value_t *value) {
    if (!value || value->is_null) return false;
    return value->data.bool_val;
}

/* Row operations */
int rdb_insert_row(rdb_database_t *db, const char *table_name, fi_array *values) {
    if (!db || !table_name || !values) return -1;
    
    rdb_table_t *table = rdb_get_table(db, table_name);
    if (!table) {
        printf("Error: Table '%s' does not exist\n", table_name);
        return -1;
    }
    
    /* Validate number of columns */
    if (fi_array_count(values) != fi_array_count(table->columns)) {
        printf("Error: Number of values (%zu) does not match number of columns (%zu)\n",
               fi_array_count(values), fi_array_count(table->columns));
        return -1;
    }
    
    /* Create new row */
    rdb_row_t *row = malloc(sizeof(rdb_row_t));
    if (!row) return -1;
    
    row->row_id = table->next_row_id++;
    row->values = fi_array_copy(values);
    if (!row->values) {
        free(row);
        return -1;
    }
    
    /* Add row to table */
    if (fi_array_push(table->rows, &row) != 0) {
        rdb_row_free(row);
        return -1;
    }
    
    /* Update indexes */
    rdb_update_table_indexes(table, row);
    
    printf("Row inserted into table '%s' with ID %zu\n", table_name, row->row_id);
    return 0;
}

/* Index operations */
int rdb_create_index(rdb_database_t *db, const char *table_name, const char *index_name, 
                     const char *column_name) {
    if (!db || !table_name || !index_name || !column_name) return -1;
    
    rdb_table_t *table = rdb_get_table(db, table_name);
    if (!table) {
        printf("Error: Table '%s' does not exist\n", table_name);
        return -1;
    }
    
    /* Find column */
    rdb_column_t *column = NULL;
    for (size_t i = 0; i < fi_array_count(table->columns); i++) {
        rdb_column_t *col = (rdb_column_t*)fi_array_get(table->columns, i);
        if (col && strcmp(col->name, column_name) == 0) {
            column = col;
            break;
        }
    }
    
    if (!column) {
        printf("Error: Column '%s' does not exist in table '%s'\n", column_name, table_name);
        return -1;
    }
    
    /* Create index tree */
    fi_btree *index_tree = fi_btree_create(sizeof(rdb_value_t*), rdb_value_compare);
    if (!index_tree) return -1;
    
    /* Build index from existing rows */
    for (size_t i = 0; i < fi_array_count(table->rows); i++) {
        rdb_row_t *row = *(rdb_row_t**)fi_array_get(table->rows, i);
        if (row && row->values) {
            rdb_value_t *value = *(rdb_value_t**)fi_array_get(row->values, 
                rdb_get_column_index(table, column_name));
            if (value) {
                fi_btree_insert(index_tree, &value);
            }
        }
    }
    
    /* Add index to table */
    if (fi_map_put(table->indexes, &index_name, &index_tree) != 0) {
        fi_btree_destroy(index_tree);
        return -1;
    }
    
    printf("Index '%s' created on column '%s' in table '%s'\n", 
           index_name, column_name, table_name);
    return 0;
}

/* Utility functions */
int rdb_get_column_index(rdb_table_t *table, const char *column_name) {
    if (!table || !column_name) return -1;
    
    for (size_t i = 0; i < fi_array_count(table->columns); i++) {
        rdb_column_t *col = (rdb_column_t*)fi_array_get(table->columns, i);
        if (col && strcmp(col->name, column_name) == 0) {
            return i;
        }
    }
    return -1;
}

void rdb_update_table_indexes(rdb_table_t *table, rdb_row_t *row) {
    if (!table || !row) return;
    
    /* Update all indexes with new row data */
    fi_map_iterator iter = fi_map_iterator_create(table->indexes);
    while (fi_map_iterator_next(&iter)) {
        /* For simplicity, we'll rebuild the index */
        /* In a real implementation, we'd update incrementally */
        (void)fi_map_iterator_key(&iter);   /* Suppress unused variable warning */
        (void)fi_map_iterator_value(&iter); /* Suppress unused variable warning */
    }
}

/* Comparison functions */
int rdb_value_compare(const void *a, const void *b) {
    const rdb_value_t *val_a = *(const rdb_value_t**)a;
    const rdb_value_t *val_b = *(const rdb_value_t**)b;
    
    if (!val_a || !val_b) return 0;
    
    /* Handle NULL values */
    if (val_a->is_null && val_b->is_null) return 0;
    if (val_a->is_null) return -1;
    if (val_b->is_null) return 1;
    
    /* Different types */
    if (val_a->type != val_b->type) {
        return val_a->type - val_b->type;
    }
    
    /* Same type comparison */
    switch (val_a->type) {
        case RDB_TYPE_INT:
            if (val_a->data.int_val < val_b->data.int_val) return -1;
            if (val_a->data.int_val > val_b->data.int_val) return 1;
            return 0;
            
        case RDB_TYPE_FLOAT:
            if (val_a->data.float_val < val_b->data.float_val) return -1;
            if (val_a->data.float_val > val_b->data.float_val) return 1;
            return 0;
            
        case RDB_TYPE_VARCHAR:
        case RDB_TYPE_TEXT:
            if (!val_a->data.string_val && !val_b->data.string_val) return 0;
            if (!val_a->data.string_val) return -1;
            if (!val_b->data.string_val) return 1;
            return strcmp(val_a->data.string_val, val_b->data.string_val);
            
        case RDB_TYPE_BOOLEAN:
            if (val_a->data.bool_val == val_b->data.bool_val) return 0;
            return val_a->data.bool_val ? 1 : -1;
            
        default:
            return 0;
    }
}

uint32_t rdb_string_hash(const void *key, size_t key_size) {
    return fi_map_hash_string(key, key_size);
}

/* String representation */
char* rdb_value_to_string(const rdb_value_t *value) {
    if (!value) return NULL;
    
    char *str = malloc(256);
    if (!str) return NULL;
    
    if (value->is_null) {
        strcpy(str, "NULL");
        return str;
    }
    
    switch (value->type) {
        case RDB_TYPE_INT:
            snprintf(str, 256, "%ld", value->data.int_val);
            break;
        case RDB_TYPE_FLOAT:
            snprintf(str, 256, "%.2f", value->data.float_val);
            break;
        case RDB_TYPE_VARCHAR:
        case RDB_TYPE_TEXT:
            snprintf(str, 256, "'%s'", value->data.string_val ? value->data.string_val : "NULL");
            break;
        case RDB_TYPE_BOOLEAN:
            strcpy(str, value->data.bool_val ? "true" : "false");
            break;
        default:
            strcpy(str, "UNKNOWN");
            break;
    }
    
    return str;
}

char* rdb_type_to_string(rdb_data_type_t type) {
    switch (type) {
        case RDB_TYPE_INT: return "INT";
        case RDB_TYPE_FLOAT: return "FLOAT";
        case RDB_TYPE_VARCHAR: return "VARCHAR";
        case RDB_TYPE_TEXT: return "TEXT";
        case RDB_TYPE_BOOLEAN: return "BOOLEAN";
        default: return "UNKNOWN";
    }
}

/* Print functions */
void rdb_print_table_info(rdb_database_t *db, const char *table_name) {
    if (!db || !table_name) return;
    
    rdb_table_t *table = rdb_get_table(db, table_name);
    if (!table) {
        printf("Table '%s' does not exist\n", table_name);
        return;
    }
    
    printf("\n=== Table: %s ===\n", table_name);
    printf("Columns: %zu, Rows: %zu\n", fi_array_count(table->columns), fi_array_count(table->rows));
    
    printf("\nColumn Definitions:\n");
    printf("%-20s %-15s %-8s %-8s %-8s %s\n", 
           "Name", "Type", "Nullable", "Primary", "Unique", "Default");
    printf("%s\n", "------------------------------------------------------------------------");
    
    for (size_t i = 0; i < fi_array_count(table->columns); i++) {
        rdb_column_t *col = (rdb_column_t*)fi_array_get(table->columns, i);
        if (col) {
            printf("%-20s %-15s %-8s %-8s %-8s %s\n",
                   col->name,
                   rdb_type_to_string(col->type),
                   col->nullable ? "YES" : "NO",
                   col->primary_key ? "YES" : "NO",
                   col->unique ? "YES" : "NO",
                   col->default_value[0] ? col->default_value : "-");
        }
    }
    
    printf("\nIndexes:\n");
    if (fi_map_size(table->indexes) == 0) {
        printf("No indexes\n");
    } else {
        fi_map_iterator iter = fi_map_iterator_create(table->indexes);
        while (fi_map_iterator_next(&iter)) {
            const char *index_name = (const char*)fi_map_iterator_key(&iter);
            printf("- %s\n", index_name);
        }
    }
}

void rdb_print_table_data(rdb_database_t *db, const char *table_name, size_t limit) {
    if (!db || !table_name) return;
    
    rdb_table_t *table = rdb_get_table(db, table_name);
    if (!table) {
        printf("Table '%s' does not exist\n", table_name);
        return;
    }
    
    printf("\n=== Table Data: %s ===\n", table_name);
    
    size_t row_count = fi_array_count(table->rows);
    size_t display_count = (limit > 0 && limit < row_count) ? limit : row_count;
    
    if (display_count == 0) {
        printf("No data in table\n");
        return;
    }
    
    /* Print column headers */
    printf("%-8s", "Row ID");
    for (size_t i = 0; i < fi_array_count(table->columns); i++) {
        rdb_column_t *col = (rdb_column_t*)fi_array_get(table->columns, i);
        if (col) {
            printf("%-20s", col->name);
        }
    }
    printf("\n");
    printf("%s\n", "------------------------------------------------------------------------");
    
    /* Print data rows */
    for (size_t i = 0; i < display_count; i++) {
        rdb_row_t *row = *(rdb_row_t**)fi_array_get(table->rows, i);
        if (row && row->values) {
            printf("%-8zu", row->row_id);
            
            for (size_t j = 0; j < fi_array_count(row->values); j++) {
                rdb_value_t *value = *(rdb_value_t**)fi_array_get(row->values, j);
                if (value) {
                    char *str = rdb_value_to_string(value);
                    printf("%-20s", str ? str : "NULL");
                    if (str) free(str);
                } else {
                    printf("%-20s", "NULL");
                }
            }
            printf("\n");
        }
    }
    
    if (limit > 0 && row_count > limit) {
        printf("... and %zu more rows\n", row_count - limit);
    }
}

void rdb_print_database_info(rdb_database_t *db) {
    if (!db) return;
    
    printf("\n=== Database: %s ===\n", db->name);
    printf("Status: %s\n", db->is_open ? "OPEN" : "CLOSED");
    printf("Tables: %zu\n", fi_map_size(db->tables));
    
    if (fi_map_size(db->tables) > 0) {
        printf("\nTable List:\n");
        fi_map_iterator iter = fi_map_iterator_create(db->tables);
        
        /* Handle first element if iterator is valid */
        if (iter.is_valid) {
            const char **table_name_ptr = (const char**)fi_map_iterator_key(&iter);
            rdb_table_t **table_ptr = (rdb_table_t**)fi_map_iterator_value(&iter);
            if (table_name_ptr && *table_name_ptr && table_ptr && *table_ptr) {
                printf("- %s (%zu rows)\n", *table_name_ptr, fi_array_count((*table_ptr)->rows));
            }
        }
        
        /* Handle remaining elements */
        while (fi_map_iterator_next(&iter)) {
            const char **table_name_ptr = (const char**)fi_map_iterator_key(&iter);
            rdb_table_t **table_ptr = (rdb_table_t**)fi_map_iterator_value(&iter);
            if (table_name_ptr && *table_name_ptr && table_ptr && *table_ptr) {
                printf("- %s (%zu rows)\n", *table_name_ptr, fi_array_count((*table_ptr)->rows));
            }
        }
    }
}

/* Row operations - UPDATE */
int rdb_update_rows(rdb_database_t *db, const char *table_name, fi_array *set_columns, 
                    fi_array *set_values, fi_array *where_conditions) {
    if (!db || !table_name || !set_columns || !set_values) return -1;
    
    rdb_table_t *table = rdb_get_table(db, table_name);
    if (!table) {
        printf("Error: Table '%s' does not exist\n", table_name);
        return -1;
    }
    
    /* Validate number of columns and values match */
    if (fi_array_count(set_columns) != fi_array_count(set_values)) {
        printf("Error: Number of columns (%zu) does not match number of values (%zu)\n",
               fi_array_count(set_columns), fi_array_count(set_values));
        return -1;
    }
    
    int updated_count = 0;
    
    /* Update each row that matches WHERE conditions */
    for (size_t i = 0; i < fi_array_count(table->rows); i++) {
        rdb_row_t *row = *(rdb_row_t**)fi_array_get(table->rows, i);
        if (!row || !row->values) continue;
        
        /* Check WHERE conditions (simplified - just check if conditions exist) */
        bool matches = true;
        if (where_conditions && fi_array_count(where_conditions) > 0) {
            /* For now, assume all rows match if WHERE conditions are provided */
            /* In a real implementation, this would evaluate the conditions */
            matches = true;
        }
        
        if (matches) {
            /* Update the specified columns */
            for (size_t j = 0; j < fi_array_count(set_columns); j++) {
                const char *col_name = *(const char**)fi_array_get(set_columns, j);
                rdb_value_t *new_value = *(rdb_value_t**)fi_array_get(set_values, j);
                
                int col_index = rdb_get_column_index(table, col_name);
                if (col_index >= 0 && col_index < (int)fi_array_count(row->values)) {
                    rdb_value_t *old_value = *(rdb_value_t**)fi_array_get(row->values, col_index);
                    if (old_value) {
                        rdb_value_free(old_value);
                    }
                    
                    /* Create a copy of the new value */
                    rdb_value_t *value_copy = malloc(sizeof(rdb_value_t));
                    if (value_copy) {
                        *value_copy = *new_value;
                        if (new_value->type == RDB_TYPE_VARCHAR || new_value->type == RDB_TYPE_TEXT) {
                            if (new_value->data.string_val) {
                                value_copy->data.string_val = malloc(strlen(new_value->data.string_val) + 1);
                                if (value_copy->data.string_val) {
                                    strcpy(value_copy->data.string_val, new_value->data.string_val);
                                }
                            }
                        }
                        fi_array_set(row->values, col_index, &value_copy);
                    }
                }
            }
            updated_count++;
        }
    }
    
    printf("Updated %d rows in table '%s'\n", updated_count, table_name);
    return updated_count;
}

/* Row operations - DELETE */
int rdb_delete_rows(rdb_database_t *db, const char *table_name, fi_array *where_conditions) {
    if (!db || !table_name) return -1;
    
    rdb_table_t *table = rdb_get_table(db, table_name);
    if (!table) {
        printf("Error: Table '%s' does not exist\n", table_name);
        return -1;
    }
    
    int deleted_count = 0;
    
    /* Delete rows that match WHERE conditions */
    for (size_t i = fi_array_count(table->rows); i > 0; i--) {
        rdb_row_t *row = *(rdb_row_t**)fi_array_get(table->rows, i - 1);
        if (!row) continue;
        
        /* Check WHERE conditions (simplified - just check if conditions exist) */
        bool matches = true;
        if (where_conditions && fi_array_count(where_conditions) > 0) {
            /* For now, assume all rows match if WHERE conditions are provided */
            /* In a real implementation, this would evaluate the conditions */
            matches = true;
        }
        
        if (matches) {
            /* Remove from array and free memory */
            fi_array_splice(table->rows, i - 1, 1, NULL);
            rdb_row_free(row);
            deleted_count++;
        }
    }
    
    printf("Deleted %d rows from table '%s'\n", deleted_count, table_name);
    return deleted_count;
}

/* Row operations - SELECT */
fi_array* rdb_select_rows(rdb_database_t *db, const char *table_name, fi_array *columns, 
                          fi_array *where_conditions) {
    (void)columns; /* Suppress unused parameter warning - columns selection not yet implemented */
    if (!db || !table_name) return NULL;
    
    rdb_table_t *table = rdb_get_table(db, table_name);
    if (!table) {
        printf("Error: Table '%s' does not exist\n", table_name);
        return NULL;
    }
    
    /* Create result array */
    fi_array *result = fi_array_create(100, sizeof(rdb_row_t*));
    if (!result) return NULL;
    
    /* Select rows that match WHERE conditions */
    for (size_t i = 0; i < fi_array_count(table->rows); i++) {
        rdb_row_t *row = *(rdb_row_t**)fi_array_get(table->rows, i);
        if (!row) continue;
        
        /* Check WHERE conditions (simplified - just check if conditions exist) */
        bool matches = true;
        if (where_conditions && fi_array_count(where_conditions) > 0) {
            /* For now, assume all rows match if WHERE conditions are provided */
            /* In a real implementation, this would evaluate the conditions */
            matches = true;
        }
        
        if (matches) {
            /* Create a copy of the row for the result */
            rdb_row_t *row_copy = malloc(sizeof(rdb_row_t));
            if (row_copy) {
                row_copy->row_id = row->row_id;
                row_copy->values = fi_array_copy(row->values);
                if (row_copy->values) {
                    fi_array_push(result, &row_copy);
                } else {
                    free(row_copy);
                }
            }
        }
    }
    
    return result;
}

/* Index operations - DROP INDEX */
int rdb_drop_index(rdb_database_t *db, const char *table_name, const char *index_name) {
    if (!db || !table_name || !index_name) return -1;
    
    rdb_table_t *table = rdb_get_table(db, table_name);
    if (!table) {
        printf("Error: Table '%s' does not exist\n", table_name);
        return -1;
    }
    
    fi_btree *index_tree = NULL;
    if (fi_map_get(table->indexes, &index_name, &index_tree) != 0) {
        printf("Error: Index '%s' does not exist in table '%s'\n", index_name, table_name);
        return -1;
    }
    
    /* Remove index from map and destroy tree */
    fi_map_remove(table->indexes, &index_name);
    if (index_tree) {
        fi_btree_destroy(index_tree);
    }
    
    printf("Index '%s' dropped from table '%s'\n", index_name, table_name);
    return 0;
}

/* Index operations - GET INDEX */
fi_btree* rdb_get_index(rdb_database_t *db, const char *table_name, const char *index_name) {
    if (!db || !table_name || !index_name) return NULL;
    
    rdb_table_t *table = rdb_get_table(db, table_name);
    if (!table) return NULL;
    
    fi_btree *index_tree = NULL;
    if (fi_map_get(table->indexes, &index_name, &index_tree) != 0) {
        return NULL;
    }
    
    return index_tree;
}

/* Column operations - ADD COLUMN */
int rdb_add_column(rdb_database_t *db, const char *table_name, const rdb_column_t *column) {
    if (!db || !table_name || !column) return -1;
    
    rdb_table_t *table = rdb_get_table(db, table_name);
    if (!table) {
        printf("Error: Table '%s' does not exist\n", table_name);
        return -1;
    }
    
    /* Check if column already exists */
    if (rdb_get_column_index(table, column->name) >= 0) {
        printf("Error: Column '%s' already exists in table '%s'\n", column->name, table_name);
        return -1;
    }
    
    /* Create a copy of the column */
    rdb_column_t *col_copy = malloc(sizeof(rdb_column_t));
    if (!col_copy) return -1;
    
    *col_copy = *column;
    
    /* Add column to table */
    if (fi_array_push(table->columns, &col_copy) != 0) {
        free(col_copy);
        return -1;
    }
    
    /* Add default values to existing rows */
    for (size_t i = 0; i < fi_array_count(table->rows); i++) {
        rdb_row_t *row = *(rdb_row_t**)fi_array_get(table->rows, i);
        if (row && row->values) {
            rdb_value_t *default_val = NULL;
            
            if (column->default_value[0] != '\0') {
                /* Create value from default */
                switch (column->type) {
                    case RDB_TYPE_INT:
                        default_val = rdb_create_int_value(atoll(column->default_value));
                        break;
                    case RDB_TYPE_FLOAT:
                        default_val = rdb_create_float_value(atof(column->default_value));
                        break;
                    case RDB_TYPE_VARCHAR:
                    case RDB_TYPE_TEXT:
                        default_val = rdb_create_string_value(column->default_value);
                        break;
                    case RDB_TYPE_BOOLEAN:
                        default_val = rdb_create_bool_value(strcmp(column->default_value, "true") == 0);
                        break;
                }
            } else if (column->nullable) {
                default_val = rdb_create_null_value(column->type);
            } else {
                /* Non-nullable column without default - create appropriate default */
                switch (column->type) {
                    case RDB_TYPE_INT:
                        default_val = rdb_create_int_value(0);
                        break;
                    case RDB_TYPE_FLOAT:
                        default_val = rdb_create_float_value(0.0);
                        break;
                    case RDB_TYPE_VARCHAR:
                    case RDB_TYPE_TEXT:
                        default_val = rdb_create_string_value("");
                        break;
                    case RDB_TYPE_BOOLEAN:
                        default_val = rdb_create_bool_value(false);
                        break;
                }
            }
            
            if (default_val) {
                fi_array_push(row->values, &default_val);
            }
        }
    }
    
    printf("Column '%s' added to table '%s'\n", column->name, table_name);
    return 0;
}

/* Column operations - DROP COLUMN */
int rdb_drop_column(rdb_database_t *db, const char *table_name, const char *column_name) {
    if (!db || !table_name || !column_name) return -1;
    
    rdb_table_t *table = rdb_get_table(db, table_name);
    if (!table) {
        printf("Error: Table '%s' does not exist\n", table_name);
        return -1;
    }
    
    int col_index = rdb_get_column_index(table, column_name);
    if (col_index < 0) {
        printf("Error: Column '%s' does not exist in table '%s'\n", column_name, table_name);
        return -1;
    }
    
    /* Check if this is the primary key */
    rdb_column_t *col = (rdb_column_t*)fi_array_get(table->columns, col_index);
    if (col && col->primary_key) {
        printf("Error: Cannot drop primary key column '%s'\n", column_name);
        return -1;
    }
    
    /* Remove column from column definitions */
    rdb_column_t *removed_col = *(rdb_column_t**)fi_array_get(table->columns, col_index);
    fi_array_splice(table->columns, col_index, 1, NULL);
    if (removed_col) {
        free(removed_col);
    }
    
    /* Remove column values from all rows */
    for (size_t i = 0; i < fi_array_count(table->rows); i++) {
        rdb_row_t *row = *(rdb_row_t**)fi_array_get(table->rows, i);
        if (row && row->values && col_index < (int)fi_array_count(row->values)) {
            rdb_value_t *removed_val = *(rdb_value_t**)fi_array_get(row->values, col_index);
            fi_array_splice(row->values, col_index, 1, NULL);
            if (removed_val) {
                rdb_value_free(removed_val);
            }
        }
    }
    
    printf("Column '%s' dropped from table '%s'\n", column_name, table_name);
    return 0;
}

/* String comparison function */
int rdb_string_compare(const void *a, const void *b) {
    const char *str_a = (const char*)a;
    const char *str_b = (const char*)b;
    
    if (!str_a && !str_b) return 0;
    if (!str_a) return -1;
    if (!str_b) return 1;
    
    return strcmp(str_a, str_b);
}
