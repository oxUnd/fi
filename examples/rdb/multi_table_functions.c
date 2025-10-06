/* Multi-table operations */
fi_array* rdb_select_join(rdb_database_t *db, const rdb_statement_t *stmt) {
    if (!db || !stmt || !stmt->from_tables || fi_array_count(stmt->from_tables) == 0) {
        return NULL;
    }
    
    /* Create result array */
    fi_array *result = fi_array_create(100, sizeof(rdb_result_row_t*));
    if (!result) return NULL;
    
    /* Get the first table */
    const char *first_table = *(const char**)fi_array_get(stmt->from_tables, 0);
    rdb_table_t *table1 = rdb_get_table(db, first_table);
    if (!table1) {
        fi_array_destroy(result);
        return NULL;
    }
    
    /* If only one table, perform simple select */
    if (fi_array_count(stmt->from_tables) == 1) {
        for (size_t i = 0; i < fi_array_count(table1->rows); i++) {
            rdb_row_t *row = *(rdb_row_t**)fi_array_get(table1->rows, i);
            if (!row) continue;
            
            /* Create result row */
            fi_array *table_names = fi_array_create(1, sizeof(char*));
            fi_map *values = fi_map_create(16, sizeof(char*), sizeof(rdb_value_t*), 
                                          fi_map_hash_string, fi_map_compare_string);
            
            if (table_names && values) {
                fi_array_push(table_names, &first_table);
                
                /* Copy values with table.column keys */
                for (size_t j = 0; j < fi_array_count(table1->columns); j++) {
                    rdb_column_t *col = (rdb_column_t*)fi_array_get(table1->columns, j);
                    rdb_value_t *val = *(rdb_value_t**)fi_array_get(row->values, j);
                    
                    if (col && val) {
                        char key[128];
                        snprintf(key, sizeof(key), "%s.%s", first_table, col->name);
                        
                        /* Create a copy of the value */
                        rdb_value_t *val_copy = malloc(sizeof(rdb_value_t));
                        if (val_copy) {
                            *val_copy = *val;
                            if (val->type == RDB_TYPE_VARCHAR || val->type == RDB_TYPE_TEXT) {
                                if (val->data.string_val) {
                                    val_copy->data.string_val = malloc(strlen(val->data.string_val) + 1);
                                    if (val_copy->data.string_val) {
                                        strcpy(val_copy->data.string_val, val->data.string_val);
                                    }
                                }
                            }
                            fi_map_put(values, &key, &val_copy);
                        }
                    }
                }
                
                rdb_result_row_t *result_row = rdb_create_result_row(row->row_id, table_names, values);
                if (result_row) {
                    fi_array_push(result, &result_row);
                }
            }
            
            if (table_names) fi_array_destroy(table_names);
            if (values) fi_map_destroy(values);
        }
        return result;
    }
    
    /* Multi-table JOIN - simplified implementation for two tables */
    if (fi_array_count(stmt->from_tables) == 2) {
        const char *second_table = *(const char**)fi_array_get(stmt->from_tables, 1);
        rdb_table_t *table2 = rdb_get_table(db, second_table);
        if (!table2) {
            fi_array_destroy(result);
            return NULL;
        }
        
        /* Perform cartesian product with join conditions */
        for (size_t i = 0; i < fi_array_count(table1->rows); i++) {
            rdb_row_t *row1 = *(rdb_row_t**)fi_array_get(table1->rows, i);
            if (!row1) continue;
            
            for (size_t j = 0; j < fi_array_count(table2->rows); j++) {
                rdb_row_t *row2 = *(rdb_row_t**)fi_array_get(table2->rows, j);
                if (!row2) continue;
                
                /* Check join conditions if any */
                bool matches = true;
                if (stmt->join_conditions && fi_array_count(stmt->join_conditions) > 0) {
                    matches = false;
                    for (size_t k = 0; k < fi_array_count(stmt->join_conditions); k++) {
                        rdb_join_condition_t *condition = *(rdb_join_condition_t**)fi_array_get(stmt->join_conditions, k);
                        if (rdb_row_matches_join_condition(row1, row2, condition, table1, table2)) {
                            matches = true;
                            break;
                        }
                    }
                }
                
                if (matches) {
                    /* Create result row */
                    fi_array *table_names = fi_array_create(2, sizeof(char*));
                    fi_map *values = fi_map_create(32, sizeof(char*), sizeof(rdb_value_t*), 
                                                  fi_map_hash_string, fi_map_compare_string);
                    
                    if (table_names && values) {
                        fi_array_push(table_names, &first_table);
                        fi_array_push(table_names, &second_table);
                        
                        /* Add values from first table */
                        for (size_t k = 0; k < fi_array_count(table1->columns); k++) {
                            rdb_column_t *col = (rdb_column_t*)fi_array_get(table1->columns, k);
                            rdb_value_t *val = *(rdb_value_t**)fi_array_get(row1->values, k);
                            
                            if (col && val) {
                                char key[128];
                                snprintf(key, sizeof(key), "%s.%s", first_table, col->name);
                                
                                rdb_value_t *val_copy = malloc(sizeof(rdb_value_t));
                                if (val_copy) {
                                    *val_copy = *val;
                                    if (val->type == RDB_TYPE_VARCHAR || val->type == RDB_TYPE_TEXT) {
                                        if (val->data.string_val) {
                                            val_copy->data.string_val = malloc(strlen(val->data.string_val) + 1);
                                            if (val_copy->data.string_val) {
                                                strcpy(val_copy->data.string_val, val->data.string_val);
                                            }
                                        }
                                    }
                                    fi_map_put(values, &key, &val_copy);
                                }
                            }
                        }
                        
                        /* Add values from second table */
                        for (size_t k = 0; k < fi_array_count(table2->columns); k++) {
                            rdb_column_t *col = (rdb_column_t*)fi_array_get(table2->columns, k);
                            rdb_value_t *val = *(rdb_value_t**)fi_array_get(row2->values, k);
                            
                            if (col && val) {
                                char key[128];
                                snprintf(key, sizeof(key), "%s.%s", second_table, col->name);
                                
                                rdb_value_t *val_copy = malloc(sizeof(rdb_value_t));
                                if (val_copy) {
                                    *val_copy = *val;
                                    if (val->type == RDB_TYPE_VARCHAR || val->type == RDB_TYPE_TEXT) {
                                        if (val->data.string_val) {
                                            val_copy->data.string_val = malloc(strlen(val->data.string_val) + 1);
                                            if (val_copy->data.string_val) {
                                                strcpy(val_copy->data.string_val, val->data.string_val);
                                            }
                                        }
                                    }
                                    fi_map_put(values, &key, &val_copy);
                                }
                            }
                        }
                        
                        size_t combined_row_id = (row1->row_id << 16) | row2->row_id;
                        rdb_result_row_t *result_row = rdb_create_result_row(combined_row_id, table_names, values);
                        if (result_row) {
                            fi_array_push(result, &result_row);
                        }
                    }
                    
                    if (table_names) fi_array_destroy(table_names);
                    if (values) fi_map_destroy(values);
                }
            }
        }
    }
    
    return result;
}

bool rdb_row_matches_join_condition(const rdb_row_t *left_row, const rdb_row_t *right_row,
                                   const rdb_join_condition_t *condition, 
                                   const rdb_table_t *left_table, const rdb_table_t *right_table) {
    if (!left_row || !right_row || !condition || !left_table || !right_table) {
        return false;
    }
    
    /* Get column indices */
    int left_col_idx = rdb_get_column_index((rdb_table_t*)left_table, condition->left_column);
    int right_col_idx = rdb_get_column_index((rdb_table_t*)right_table, condition->right_column);
    
    if (left_col_idx < 0 || right_col_idx < 0) {
        return false;
    }
    
    /* Get values */
    rdb_value_t *left_val = *(rdb_value_t**)fi_array_get(left_row->values, left_col_idx);
    rdb_value_t *right_val = *(rdb_value_t**)fi_array_get(right_row->values, right_col_idx);
    
    if (!left_val || !right_val) {
        return false;
    }
    
    /* Compare values */
    return rdb_value_compare(&left_val, &right_val) == 0;
}

int rdb_validate_foreign_key(rdb_database_t *db, const char *table_name, 
                            const char *column_name, const rdb_value_t *value) {
    if (!db || !table_name || !column_name || !value) return -1;
    
    /* Find foreign key constraints for this table/column */
    fi_map_iterator iter = fi_map_iterator_create(db->foreign_keys);
    
    /* Handle first element if iterator is valid */
    if (iter.is_valid) {
        rdb_foreign_key_t **fk_ptr = (rdb_foreign_key_t**)fi_map_iterator_value(&iter);
        if (fk_ptr && *fk_ptr) {
            rdb_foreign_key_t *fk = *fk_ptr;
            if (strcmp(fk->table_name, table_name) == 0 && 
                strcmp(fk->column_name, column_name) == 0) {
                
                /* Check if referenced value exists */
                rdb_table_t *ref_table = rdb_get_table(db, fk->ref_table_name);
                if (!ref_table) return -1;
                
                int ref_col_idx = rdb_get_column_index(ref_table, fk->ref_column_name);
                if (ref_col_idx < 0) return -1;
                
                /* Search for matching value in referenced table */
                for (size_t i = 0; i < fi_array_count(ref_table->rows); i++) {
                    rdb_row_t *row = *(rdb_row_t**)fi_array_get(ref_table->rows, i);
                    if (!row || !row->values) continue;
                    
                    rdb_value_t *ref_val = *(rdb_value_t**)fi_array_get(row->values, ref_col_idx);
                    if (ref_val && rdb_value_compare(&value, &ref_val) == 0) {
                        return 0; /* Valid foreign key */
                    }
                }
                
                return -1; /* Foreign key violation */
            }
        }
    }
    
    /* Handle remaining elements */
    while (fi_map_iterator_next(&iter)) {
        rdb_foreign_key_t **fk_ptr = (rdb_foreign_key_t**)fi_map_iterator_value(&iter);
        if (fk_ptr && *fk_ptr) {
            rdb_foreign_key_t *fk = *fk_ptr;
            if (strcmp(fk->table_name, table_name) == 0 && 
                strcmp(fk->column_name, column_name) == 0) {
                
                /* Check if referenced value exists */
                rdb_table_t *ref_table = rdb_get_table(db, fk->ref_table_name);
                if (!ref_table) return -1;
                
                int ref_col_idx = rdb_get_column_index(ref_table, fk->ref_column_name);
                if (ref_col_idx < 0) return -1;
                
                /* Search for matching value in referenced table */
                for (size_t i = 0; i < fi_array_count(ref_table->rows); i++) {
                    rdb_row_t *row = *(rdb_row_t**)fi_array_get(ref_table->rows, i);
                    if (!row || !row->values) continue;
                    
                    rdb_value_t *ref_val = *(rdb_value_t**)fi_array_get(row->values, ref_col_idx);
                    if (ref_val && rdb_value_compare(&value, &ref_val) == 0) {
                        return 0; /* Valid foreign key */
                    }
                }
                
                return -1; /* Foreign key violation */
            }
        }
    }
    
    return 0; /* No foreign key constraint found */
}

int rdb_enforce_foreign_key_constraints(rdb_database_t *db, const char *table_name, 
                                       rdb_row_t *row) {
    if (!db || !table_name || !row) return -1;
    
    rdb_table_t *table = rdb_get_table(db, table_name);
    if (!table) return -1;
    
    /* Check each column for foreign key constraints */
    for (size_t i = 0; i < fi_array_count(table->columns); i++) {
        rdb_column_t *col = (rdb_column_t*)fi_array_get(table->columns, i);
        if (!col) continue;
        
        rdb_value_t *val = *(rdb_value_t**)fi_array_get(row->values, i);
        if (!val) continue;
        
        if (rdb_validate_foreign_key(db, table_name, col->name, val) != 0) {
            printf("Error: Foreign key constraint violation on column '%s'\n", col->name);
            return -1;
        }
    }
    
    return 0;
}
