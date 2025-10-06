#include "rdb.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Demo functions */
void demo_basic_operations(void);
void demo_table_operations(void);
void demo_data_operations(void);
void demo_index_operations(void);
void demo_sql_parser(void);

/* Helper functions */
rdb_column_t* create_column(const char *name, rdb_data_type_t type, bool primary_key, bool unique, bool nullable);
void print_separator(const char *title);

int main() {
    printf("=== FI Relational Database Demo ===\n\n");
    
    demo_basic_operations();
    demo_table_operations();
    demo_data_operations();
    demo_index_operations();
    demo_sql_parser();
    
    printf("\n=== Demo completed successfully! ===\n");
    return 0;
}

void demo_basic_operations(void) {
    print_separator("Basic Database Operations");
    
    /* Create database */
    rdb_database_t *db = rdb_create_database("test_db");
    if (!db) {
        printf("Error: Failed to create database\n");
        return;
    }
    
    printf("Database '%s' created successfully\n", db->name);
    
    /* Open database */
    if (rdb_open_database(db) == 0) {
        printf("Database opened successfully\n");
    }
    
    /* Print database info */
    rdb_print_database_info(db);
    
    /* Close and destroy database */
    rdb_close_database(db);
    rdb_destroy_database(db);
    
    printf("Database operations completed\n");
}

void demo_table_operations(void) {
    print_separator("Table Operations");
    
    rdb_database_t *db = rdb_create_database("table_demo");
    if (!db) return;
    
    rdb_open_database(db);
    
    /* Create columns for a student table */
    fi_array *columns = fi_array_create(5, sizeof(rdb_column_t*));
    
    rdb_column_t *col1 = create_column("id", RDB_TYPE_INT, true, true, false);
    rdb_column_t *col2 = create_column("name", RDB_TYPE_VARCHAR, false, false, false);
    rdb_column_t *col3 = create_column("age", RDB_TYPE_INT, false, false, true);
    rdb_column_t *col4 = create_column("gpa", RDB_TYPE_FLOAT, false, false, true);
    rdb_column_t *col5 = create_column("is_active", RDB_TYPE_BOOLEAN, false, false, false);
    
    fi_array_push(columns, &col1);
    fi_array_push(columns, &col2);
    fi_array_push(columns, &col3);
    fi_array_push(columns, &col4);
    fi_array_push(columns, &col5);
    
    /* Create table */
    if (rdb_create_table(db, "students", columns) == 0) {
        printf("Table 'students' created successfully\n");
    }
    
    /* Print table info */
    rdb_print_table_info(db, "students");
    
    /* Create another table */
    fi_array *course_columns = fi_array_create(3, sizeof(rdb_column_t*));
    rdb_column_t *c_col1 = create_column("course_id", RDB_TYPE_INT, true, true, false);
    rdb_column_t *c_col2 = create_column("title", RDB_TYPE_VARCHAR, false, false, false);
    rdb_column_t *c_col3 = create_column("credits", RDB_TYPE_INT, false, false, false);
    
    fi_array_push(course_columns, &c_col1);
    fi_array_push(course_columns, &c_col2);
    fi_array_push(course_columns, &c_col3);
    
    if (rdb_create_table(db, "courses", course_columns) == 0) {
        printf("Table 'courses' created successfully\n");
    }
    
    /* Print database info */
    rdb_print_database_info(db);
    
    /* Clean up */
    fi_array_destroy(columns);
    fi_array_destroy(course_columns);
    rdb_destroy_database(db);
    
    printf("Table operations completed\n");
}

void demo_data_operations(void) {
    print_separator("Data Operations");
    
    rdb_database_t *db = rdb_create_database("data_demo");
    if (!db) return;
    
    rdb_open_database(db);
    
    /* Create students table */
    fi_array *columns = fi_array_create(5, sizeof(rdb_column_t*));
    rdb_column_t *d_col1 = create_column("id", RDB_TYPE_INT, true, true, false);
    rdb_column_t *d_col2 = create_column("name", RDB_TYPE_VARCHAR, false, false, false);
    rdb_column_t *d_col3 = create_column("age", RDB_TYPE_INT, false, false, true);
    rdb_column_t *d_col4 = create_column("gpa", RDB_TYPE_FLOAT, false, false, true);
    rdb_column_t *d_col5 = create_column("is_active", RDB_TYPE_BOOLEAN, false, false, false);
    
    fi_array_push(columns, &d_col1);
    fi_array_push(columns, &d_col2);
    fi_array_push(columns, &d_col3);
    fi_array_push(columns, &d_col4);
    fi_array_push(columns, &d_col5);
    
    rdb_create_table(db, "students", columns);
    
    /* Insert sample data */
    fi_array *values1 = fi_array_create(5, sizeof(rdb_value_t*));
    rdb_value_t *val1_1 = rdb_create_int_value(1);
    rdb_value_t *val1_2 = rdb_create_string_value("Alice Johnson");
    rdb_value_t *val1_3 = rdb_create_int_value(20);
    rdb_value_t *val1_4 = rdb_create_float_value(3.8);
    rdb_value_t *val1_5 = rdb_create_bool_value(true);
    
    fi_array_push(values1, &val1_1);
    fi_array_push(values1, &val1_2);
    fi_array_push(values1, &val1_3);
    fi_array_push(values1, &val1_4);
    fi_array_push(values1, &val1_5);
    
    rdb_insert_row(db, "students", values1);
    
    /* Insert more data */
    fi_array *values2 = fi_array_create(5, sizeof(rdb_value_t*));
    rdb_value_t *val2_1 = rdb_create_int_value(2);
    rdb_value_t *val2_2 = rdb_create_string_value("Bob Smith");
    rdb_value_t *val2_3 = rdb_create_int_value(22);
    rdb_value_t *val2_4 = rdb_create_float_value(3.5);
    rdb_value_t *val2_5 = rdb_create_bool_value(true);
    
    fi_array_push(values2, &val2_1);
    fi_array_push(values2, &val2_2);
    fi_array_push(values2, &val2_3);
    fi_array_push(values2, &val2_4);
    fi_array_push(values2, &val2_5);
    
    rdb_insert_row(db, "students", values2);
    
    fi_array *values3 = fi_array_create(5, sizeof(rdb_value_t*));
    rdb_value_t *val3_1 = rdb_create_int_value(3);
    rdb_value_t *val3_2 = rdb_create_string_value("Carol Davis");
    rdb_value_t *val3_3 = rdb_create_int_value(19);
    rdb_value_t *val3_4 = rdb_create_float_value(3.9);
    rdb_value_t *val3_5 = rdb_create_bool_value(false);
    
    fi_array_push(values3, &val3_1);
    fi_array_push(values3, &val3_2);
    fi_array_push(values3, &val3_3);
    fi_array_push(values3, &val3_4);
    fi_array_push(values3, &val3_5);
    
    rdb_insert_row(db, "students", values3);
    
    /* Print table data */
    printf("Sample data inserted:\n");
    rdb_print_table_data(db, "students", 10);
    
    /* Clean up */
    fi_array_destroy(columns);
    fi_array_destroy(values1);
    fi_array_destroy(values2);
    fi_array_destroy(values3);
    rdb_destroy_database(db);
    
    printf("Data operations completed\n");
}

void demo_index_operations(void) {
    print_separator("Index Operations");
    
    rdb_database_t *db = rdb_create_database("index_demo");
    if (!db) return;
    
    rdb_open_database(db);
    
    /* Create table with some data */
    fi_array *columns = fi_array_create(3, sizeof(rdb_column_t*));
    rdb_column_t *i_col1 = create_column("id", RDB_TYPE_INT, true, true, false);
    rdb_column_t *i_col2 = create_column("name", RDB_TYPE_VARCHAR, false, false, false);
    rdb_column_t *i_col3 = create_column("score", RDB_TYPE_INT, false, false, false);
    
    fi_array_push(columns, &i_col1);
    fi_array_push(columns, &i_col2);
    fi_array_push(columns, &i_col3);
    
    rdb_create_table(db, "scores", columns);
    
    /* Insert sample data */
    for (int i = 1; i <= 10; i++) {
        fi_array *values = fi_array_create(3, sizeof(rdb_value_t*));
        rdb_value_t *v1 = rdb_create_int_value(i);
        
        char name[32];
        snprintf(name, sizeof(name), "Student%d", i);
        rdb_value_t *v2 = rdb_create_string_value(name);
        
        rdb_value_t *v3 = rdb_create_int_value(rand() % 100);
        
        fi_array_push(values, &v1);
        fi_array_push(values, &v2);
        fi_array_push(values, &v3);
        
        rdb_insert_row(db, "scores", values);
        fi_array_destroy(values);
    }
    
    printf("Table 'scores' created with 10 rows\n");
    
    /* Create index on name column */
    if (rdb_create_index(db, "scores", "idx_name", "name") == 0) {
        printf("Index 'idx_name' created on 'name' column\n");
    }
    
    /* Create index on score column */
    if (rdb_create_index(db, "scores", "idx_score", "score") == 0) {
        printf("Index 'idx_score' created on 'score' column\n");
    }
    
    /* Print table info to show indexes */
    rdb_print_table_info(db, "scores");
    
    /* Clean up */
    fi_array_destroy(columns);
    rdb_destroy_database(db);
    
    printf("Index operations completed\n");
}

void demo_sql_parser(void) {
    print_separator("SQL Parser Demo");
    
    printf("SQL Parser functionality is currently under development.\n");
    printf("This demo shows the basic database operations without SQL parsing.\n");
    
    /* Simple demonstration of value creation and manipulation */
    printf("\nDemonstrating value creation:\n");
    
    rdb_value_t *int_val = rdb_create_int_value(42);
    rdb_value_t *float_val = rdb_create_float_value(3.14159);
    rdb_value_t *string_val = rdb_create_string_value("Hello World");
    rdb_value_t *bool_val = rdb_create_bool_value(true);
    
    if (int_val) {
        char *str = rdb_value_to_string(int_val);
        printf("Integer value: %s\n", str);
        free(str);
        rdb_value_free(int_val);
    }
    
    if (float_val) {
        char *str = rdb_value_to_string(float_val);
        printf("Float value: %s\n", str);
        free(str);
        rdb_value_free(float_val);
    }
    
    if (string_val) {
        char *str = rdb_value_to_string(string_val);
        printf("String value: %s\n", str);
        free(str);
        rdb_value_free(string_val);
    }
    
    if (bool_val) {
        char *str = rdb_value_to_string(bool_val);
        printf("Boolean value: %s\n", str);
        free(str);
        rdb_value_free(bool_val);
    }
    
    printf("SQL parser demo completed\n");
}

/* Helper functions */
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
    
    return column;
}

void print_separator(const char *title) {
    printf("\n");
    printf("========================================\n");
    printf("  %s\n", title);
    printf("========================================\n");
}
