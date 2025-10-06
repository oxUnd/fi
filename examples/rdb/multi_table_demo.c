#include "rdb.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Demo functions */
void demo_foreign_keys(void);
void demo_join_operations(void);
void demo_multi_table_queries(void);

/* Helper functions */
rdb_column_t* create_column(const char *name, rdb_data_type_t type, bool primary_key, bool unique, bool nullable);
rdb_column_t* create_foreign_key_column(const char *name, rdb_data_type_t type, const char *ref_table, const char *ref_column);
void print_separator(const char *title);

int main() {
    printf("=== FI Relational Database Multi-Table Operations Demo ===\n\n");
    
    demo_foreign_keys();
    demo_join_operations();
    demo_multi_table_queries();
    
    printf("\n=== Multi-table demo completed successfully! ===\n");
    return 0;
}

void demo_foreign_keys(void) {
    print_separator("Foreign Key Constraints Demo");
    
    rdb_database_t *db = rdb_create_database("multi_table_demo");
    if (!db) {
        printf("Error: Failed to create database\n");
        return;
    }
    
    rdb_open_database(db);
    
    /* Create departments table */
    fi_array *dept_columns = fi_array_create(3, sizeof(rdb_column_t*));
    rdb_column_t *dept_id = create_column("dept_id", RDB_TYPE_INT, true, true, false);
    rdb_column_t *dept_name = create_column("dept_name", RDB_TYPE_VARCHAR, false, false, false);
    rdb_column_t *budget = create_column("budget", RDB_TYPE_FLOAT, false, false, false);
    
    fi_array_push(dept_columns, &dept_id);
    fi_array_push(dept_columns, &dept_name);
    fi_array_push(dept_columns, &budget);
    
    if (rdb_create_table(db, "departments", dept_columns) == 0) {
        printf("Table 'departments' created successfully\n");
    }
    
    /* Create employees table with foreign key */
    fi_array *emp_columns = fi_array_create(4, sizeof(rdb_column_t*));
    rdb_column_t *emp_id = create_column("emp_id", RDB_TYPE_INT, true, true, false);
    rdb_column_t *emp_name = create_column("emp_name", RDB_TYPE_VARCHAR, false, false, false);
    rdb_column_t *salary = create_column("salary", RDB_TYPE_FLOAT, false, false, false);
    rdb_column_t *dept_id_fk = create_foreign_key_column("dept_id", RDB_TYPE_INT, "departments", "dept_id");
    
    fi_array_push(emp_columns, &emp_id);
    fi_array_push(emp_columns, &emp_name);
    fi_array_push(emp_columns, &salary);
    fi_array_push(emp_columns, &dept_id_fk);
    
    if (rdb_create_table(db, "employees", emp_columns) == 0) {
        printf("Table 'employees' created successfully\n");
    }
    
    /* Add foreign key constraint */
    rdb_foreign_key_t *fk = rdb_create_foreign_key("fk_emp_dept", "employees", "dept_id", "departments", "dept_id");
    if (fk) {
        rdb_add_foreign_key(db, fk);
        rdb_foreign_key_free(fk);
    }
    
    /* Insert departments */
    fi_array *dept_values1 = fi_array_create(3, sizeof(rdb_value_t*));
    rdb_value_t *d1_id = rdb_create_int_value(1);
    rdb_value_t *d1_name = rdb_create_string_value("Engineering");
    rdb_value_t *d1_budget = rdb_create_float_value(500000.0);
    
    fi_array_push(dept_values1, &d1_id);
    fi_array_push(dept_values1, &d1_name);
    fi_array_push(dept_values1, &d1_budget);
    
    rdb_insert_row(db, "departments", dept_values1);
    
    fi_array *dept_values2 = fi_array_create(3, sizeof(rdb_value_t*));
    rdb_value_t *d2_id = rdb_create_int_value(2);
    rdb_value_t *d2_name = rdb_create_string_value("Sales");
    rdb_value_t *d2_budget = rdb_create_float_value(300000.0);
    
    fi_array_push(dept_values2, &d2_id);
    fi_array_push(dept_values2, &d2_name);
    fi_array_push(dept_values2, &d2_budget);
    
    rdb_insert_row(db, "departments", dept_values2);
    
    /* Insert employees with valid foreign keys */
    fi_array *emp_values1 = fi_array_create(4, sizeof(rdb_value_t*));
    rdb_value_t *e1_id = rdb_create_int_value(101);
    rdb_value_t *e1_name = rdb_create_string_value("Alice Johnson");
    rdb_value_t *e1_salary = rdb_create_float_value(75000.0);
    rdb_value_t *e1_dept = rdb_create_int_value(1); /* Engineering */
    
    fi_array_push(emp_values1, &e1_id);
    fi_array_push(emp_values1, &e1_name);
    fi_array_push(emp_values1, &e1_salary);
    fi_array_push(emp_values1, &e1_dept);
    
    rdb_insert_row(db, "employees", emp_values1);
    
    fi_array *emp_values2 = fi_array_create(4, sizeof(rdb_value_t*));
    rdb_value_t *e2_id = rdb_create_int_value(102);
    rdb_value_t *e2_name = rdb_create_string_value("Bob Smith");
    rdb_value_t *e2_salary = rdb_create_float_value(65000.0);
    rdb_value_t *e2_dept = rdb_create_int_value(2); /* Sales */
    
    fi_array_push(emp_values2, &e2_id);
    fi_array_push(emp_values2, &e2_name);
    fi_array_push(emp_values2, &e2_salary);
    fi_array_push(emp_values2, &e2_dept);
    
    rdb_insert_row(db, "employees", emp_values2);
    
    /* Try to insert employee with invalid foreign key */
    fi_array *emp_values3 = fi_array_create(4, sizeof(rdb_value_t*));
    rdb_value_t *e3_id = rdb_create_int_value(103);
    rdb_value_t *e3_name = rdb_create_string_value("Charlie Brown");
    rdb_value_t *e3_salary = rdb_create_float_value(55000.0);
    rdb_value_t *e3_dept = rdb_create_int_value(99); /* Invalid department */
    
    fi_array_push(emp_values3, &e3_id);
    fi_array_push(emp_values3, &e3_name);
    fi_array_push(emp_values3, &e3_salary);
    fi_array_push(emp_values3, &e3_dept);
    
    printf("\nAttempting to insert employee with invalid foreign key...\n");
    int result = rdb_insert_row(db, "employees", emp_values3);
    if (result != 0) {
        printf("Foreign key constraint successfully prevented invalid insert\n");
    }
    
    /* Print tables */
    printf("\nDepartments table:\n");
    rdb_print_table_data(db, "departments", 10);
    
    printf("\nEmployees table:\n");
    rdb_print_table_data(db, "employees", 10);
    
    /* Print foreign key constraints */
    rdb_print_foreign_keys(db);
    
    /* Clean up */
    fi_array_destroy(dept_columns);
    fi_array_destroy(emp_columns);
    fi_array_destroy(dept_values1);
    fi_array_destroy(dept_values2);
    fi_array_destroy(emp_values1);
    fi_array_destroy(emp_values2);
    fi_array_destroy(emp_values3);
    rdb_destroy_database(db);
    
    printf("Foreign key constraints demo completed\n");
}

void demo_join_operations(void) {
    print_separator("JOIN Operations Demo");
    
    rdb_database_t *db = rdb_create_database("join_demo");
    if (!db) {
        printf("Error: Failed to create database\n");
        return;
    }
    
    rdb_open_database(db);
    
    /* Create customers table */
    fi_array *customer_columns = fi_array_create(3, sizeof(rdb_column_t*));
    rdb_column_t *cust_id = create_column("customer_id", RDB_TYPE_INT, true, true, false);
    rdb_column_t *cust_name = create_column("customer_name", RDB_TYPE_VARCHAR, false, false, false);
    rdb_column_t *city = create_column("city", RDB_TYPE_VARCHAR, false, false, false);
    
    fi_array_push(customer_columns, &cust_id);
    fi_array_push(customer_columns, &cust_name);
    fi_array_push(customer_columns, &city);
    
    rdb_create_table(db, "customers", customer_columns);
    
    /* Create orders table */
    fi_array *order_columns = fi_array_create(4, sizeof(rdb_column_t*));
    rdb_column_t *order_id = create_column("order_id", RDB_TYPE_INT, true, true, false);
    rdb_column_t *customer_id_fk = create_foreign_key_column("customer_id", RDB_TYPE_INT, "customers", "customer_id");
    rdb_column_t *product = create_column("product", RDB_TYPE_VARCHAR, false, false, false);
    rdb_column_t *amount = create_column("amount", RDB_TYPE_FLOAT, false, false, false);
    
    fi_array_push(order_columns, &order_id);
    fi_array_push(order_columns, &customer_id_fk);
    fi_array_push(order_columns, &product);
    fi_array_push(order_columns, &amount);
    
    rdb_create_table(db, "orders", order_columns);
    
    /* Insert customers */
    fi_array *cust_values1 = fi_array_create(3, sizeof(rdb_value_t*));
    rdb_value_t *c1_id = rdb_create_int_value(1);
    rdb_value_t *c1_name = rdb_create_string_value("John Doe");
    rdb_value_t *c1_city = rdb_create_string_value("New York");
    
    fi_array_push(cust_values1, &c1_id);
    fi_array_push(cust_values1, &c1_name);
    fi_array_push(cust_values1, &c1_city);
    
    rdb_insert_row(db, "customers", cust_values1);
    
    fi_array *cust_values2 = fi_array_create(3, sizeof(rdb_value_t*));
    rdb_value_t *c2_id = rdb_create_int_value(2);
    rdb_value_t *c2_name = rdb_create_string_value("Jane Smith");
    rdb_value_t *c2_city = rdb_create_string_value("Los Angeles");
    
    fi_array_push(cust_values2, &c2_id);
    fi_array_push(cust_values2, &c2_name);
    fi_array_push(cust_values2, &c2_city);
    
    rdb_insert_row(db, "customers", cust_values2);
    
    /* Insert orders */
    fi_array *order_values1 = fi_array_create(4, sizeof(rdb_value_t*));
    rdb_value_t *o1_id = rdb_create_int_value(1001);
    rdb_value_t *o1_cust = rdb_create_int_value(1);
    rdb_value_t *o1_product = rdb_create_string_value("Laptop");
    rdb_value_t *o1_amount = rdb_create_float_value(1200.0);
    
    fi_array_push(order_values1, &o1_id);
    fi_array_push(order_values1, &o1_cust);
    fi_array_push(order_values1, &o1_product);
    fi_array_push(order_values1, &o1_amount);
    
    rdb_insert_row(db, "orders", order_values1);
    
    fi_array *order_values2 = fi_array_create(4, sizeof(rdb_value_t*));
    rdb_value_t *o2_id = rdb_create_int_value(1002);
    rdb_value_t *o2_cust = rdb_create_int_value(1);
    rdb_value_t *o2_product = rdb_create_string_value("Mouse");
    rdb_value_t *o2_amount = rdb_create_float_value(25.0);
    
    fi_array_push(order_values2, &o2_id);
    fi_array_push(order_values2, &o2_cust);
    fi_array_push(order_values2, &o2_product);
    fi_array_push(order_values2, &o2_amount);
    
    rdb_insert_row(db, "orders", order_values2);
    
    fi_array *order_values3 = fi_array_create(4, sizeof(rdb_value_t*));
    rdb_value_t *o3_id = rdb_create_int_value(1003);
    rdb_value_t *o3_cust = rdb_create_int_value(2);
    rdb_value_t *o3_product = rdb_create_string_value("Keyboard");
    rdb_value_t *o3_amount = rdb_create_float_value(75.0);
    
    fi_array_push(order_values3, &o3_id);
    fi_array_push(order_values3, &o3_cust);
    fi_array_push(order_values3, &o3_product);
    fi_array_push(order_values3, &o3_amount);
    
    rdb_insert_row(db, "orders", order_values3);
    
    /* Perform JOIN operation */
    rdb_statement_t *join_stmt = malloc(sizeof(rdb_statement_t));
    if (join_stmt) {
        join_stmt->type = RDB_STMT_SELECT;
        
        /* Set FROM tables */
        join_stmt->from_tables = fi_array_create(2, sizeof(char*));
        const char *table1 = "customers";
        const char *table2 = "orders";
        fi_array_push(join_stmt->from_tables, &table1);
        fi_array_push(join_stmt->from_tables, &table2);
        
        /* Set JOIN conditions */
        join_stmt->join_conditions = fi_array_create(1, sizeof(rdb_join_condition_t*));
        rdb_join_condition_t *join_cond = rdb_create_join_condition("customers", "customer_id", "orders", "customer_id", RDB_JOIN_INNER);
        if (join_cond) {
            fi_array_push(join_stmt->join_conditions, &join_cond);
        }
        
        /* Execute JOIN */
        fi_array *join_result = rdb_select_join(db, join_stmt);
        if (join_result) {
            printf("JOIN Query: Customers INNER JOIN Orders\n");
            rdb_print_join_result(join_result, join_stmt);
            
            /* Clean up result */
            for (size_t i = 0; i < fi_array_count(join_result); i++) {
                rdb_result_row_t *row = *(rdb_result_row_t**)fi_array_get(join_result, i);
                rdb_result_row_free(row);
            }
            fi_array_destroy(join_result);
        }
        
        /* Clean up statement */
        if (join_stmt->from_tables) fi_array_destroy(join_stmt->from_tables);
        if (join_stmt->join_conditions) {
            for (size_t i = 0; i < fi_array_count(join_stmt->join_conditions); i++) {
                rdb_join_condition_t *cond = *(rdb_join_condition_t**)fi_array_get(join_stmt->join_conditions, i);
                rdb_join_condition_free(cond);
            }
            fi_array_destroy(join_stmt->join_conditions);
        }
        free(join_stmt);
    }
    
    /* Clean up */
    fi_array_destroy(customer_columns);
    fi_array_destroy(order_columns);
    fi_array_destroy(cust_values1);
    fi_array_destroy(cust_values2);
    fi_array_destroy(order_values1);
    fi_array_destroy(order_values2);
    fi_array_destroy(order_values3);
    rdb_destroy_database(db);
    
    printf("JOIN operations demo completed\n");
}

void demo_multi_table_queries(void) {
    print_separator("Multi-Table Queries Demo");
    
    rdb_database_t *db = rdb_create_database("multi_query_demo");
    if (!db) {
        printf("Error: Failed to create database\n");
        return;
    }
    
    rdb_open_database(db);
    
    /* Create a more complex schema */
    fi_array *students_columns = fi_array_create(3, sizeof(rdb_column_t*));
    rdb_column_t *stu_id = create_column("student_id", RDB_TYPE_INT, true, true, false);
    rdb_column_t *stu_name = create_column("student_name", RDB_TYPE_VARCHAR, false, false, false);
    rdb_column_t *major = create_column("major", RDB_TYPE_VARCHAR, false, false, false);
    
    fi_array_push(students_columns, &stu_id);
    fi_array_push(students_columns, &stu_name);
    fi_array_push(students_columns, &major);
    
    rdb_create_table(db, "students", students_columns);
    
    fi_array *courses_columns = fi_array_create(3, sizeof(rdb_column_t*));
    rdb_column_t *course_id = create_column("course_id", RDB_TYPE_INT, true, true, false);
    rdb_column_t *course_name = create_column("course_name", RDB_TYPE_VARCHAR, false, false, false);
    rdb_column_t *credits = create_column("credits", RDB_TYPE_INT, false, false, false);
    
    fi_array_push(courses_columns, &course_id);
    fi_array_push(courses_columns, &course_name);
    fi_array_push(courses_columns, &credits);
    
    rdb_create_table(db, "courses", courses_columns);
    
    fi_array *enrollments_columns = fi_array_create(3, sizeof(rdb_column_t*));
    rdb_column_t *enroll_stu_id = create_foreign_key_column("student_id", RDB_TYPE_INT, "students", "student_id");
    rdb_column_t *enroll_course_id = create_foreign_key_column("course_id", RDB_TYPE_INT, "courses", "course_id");
    rdb_column_t *grade = create_column("grade", RDB_TYPE_VARCHAR, false, false, true);
    
    fi_array_push(enrollments_columns, &enroll_stu_id);
    fi_array_push(enrollments_columns, &enroll_course_id);
    fi_array_push(enrollments_columns, &grade);
    
    rdb_create_table(db, "enrollments", enrollments_columns);
    
    /* Insert sample data */
    /* Students */
    for (int i = 1; i <= 3; i++) {
        fi_array *stu_values = fi_array_create(3, sizeof(rdb_value_t*));
        rdb_value_t *s_id = rdb_create_int_value(i);
        
        char name[32];
        snprintf(name, sizeof(name), "Student%d", i);
        rdb_value_t *s_name = rdb_create_string_value(name);
        
        rdb_value_t *s_major = rdb_create_string_value(i % 2 == 0 ? "Computer Science" : "Mathematics");
        
        fi_array_push(stu_values, &s_id);
        fi_array_push(stu_values, &s_name);
        fi_array_push(stu_values, &s_major);
        
        rdb_insert_row(db, "students", stu_values);
        fi_array_destroy(stu_values);
    }
    
    /* Courses */
    const char *course_names[] = {"Database Systems", "Algorithms", "Calculus", "Linear Algebra"};
    const int course_credits[] = {3, 4, 4, 3};
    
    for (int i = 0; i < 4; i++) {
        fi_array *course_values = fi_array_create(3, sizeof(rdb_value_t*));
        rdb_value_t *c_id = rdb_create_int_value(i + 1);
        rdb_value_t *c_name = rdb_create_string_value(course_names[i]);
        rdb_value_t *c_credits = rdb_create_int_value(course_credits[i]);
        
        fi_array_push(course_values, &c_id);
        fi_array_push(course_values, &c_name);
        fi_array_push(course_values, &c_credits);
        
        rdb_insert_row(db, "courses", course_values);
        fi_array_destroy(course_values);
    }
    
    /* Enrollments */
    for (int i = 1; i <= 3; i++) {
        for (int j = 1; j <= 2; j++) {
            fi_array *enroll_values = fi_array_create(3, sizeof(rdb_value_t*));
            rdb_value_t *e_stu = rdb_create_int_value(i);
            rdb_value_t *e_course = rdb_create_int_value(j);
            
            char grade_str[3];
            snprintf(grade_str, sizeof(grade_str), "%c", 'A' + (i + j) % 4);
            rdb_value_t *e_grade = rdb_create_string_value(grade_str);
            
            fi_array_push(enroll_values, &e_stu);
            fi_array_push(enroll_values, &e_course);
            fi_array_push(enroll_values, &e_grade);
            
            rdb_insert_row(db, "enrollments", enroll_values);
            fi_array_destroy(enroll_values);
        }
    }
    
    /* Perform complex multi-table query */
    rdb_statement_t *multi_stmt = malloc(sizeof(rdb_statement_t));
    if (multi_stmt) {
        multi_stmt->type = RDB_STMT_SELECT;
        
        /* Set FROM tables */
        multi_stmt->from_tables = fi_array_create(3, sizeof(char*));
        const char *table1 = "students";
        const char *table2 = "enrollments";
        const char *table3 = "courses";
        fi_array_push(multi_stmt->from_tables, &table1);
        fi_array_push(multi_stmt->from_tables, &table2);
        fi_array_push(multi_stmt->from_tables, &table3);
        
        /* Set JOIN conditions */
        multi_stmt->join_conditions = fi_array_create(2, sizeof(rdb_join_condition_t*));
        rdb_join_condition_t *join1 = rdb_create_join_condition("students", "student_id", "enrollments", "student_id", RDB_JOIN_INNER);
        rdb_join_condition_t *join2 = rdb_create_join_condition("enrollments", "course_id", "courses", "course_id", RDB_JOIN_INNER);
        
        if (join1) fi_array_push(multi_stmt->join_conditions, &join1);
        if (join2) fi_array_push(multi_stmt->join_conditions, &join2);
        
        /* Execute multi-table query */
        fi_array *multi_result = rdb_select_join(db, multi_stmt);
        if (multi_result) {
            printf("Multi-Table Query: Students JOIN Enrollments JOIN Courses\n");
            rdb_print_join_result(multi_result, multi_stmt);
            
            /* Clean up result */
            for (size_t i = 0; i < fi_array_count(multi_result); i++) {
                rdb_result_row_t *row = *(rdb_result_row_t**)fi_array_get(multi_result, i);
                rdb_result_row_free(row);
            }
            fi_array_destroy(multi_result);
        }
        
        /* Clean up statement */
        if (multi_stmt->from_tables) fi_array_destroy(multi_stmt->from_tables);
        if (multi_stmt->join_conditions) {
            for (size_t i = 0; i < fi_array_count(multi_stmt->join_conditions); i++) {
                rdb_join_condition_t *cond = *(rdb_join_condition_t**)fi_array_get(multi_stmt->join_conditions, i);
                rdb_join_condition_free(cond);
            }
            fi_array_destroy(multi_stmt->join_conditions);
        }
        free(multi_stmt);
    }
    
    /* Print individual tables */
    printf("\nStudents table:\n");
    rdb_print_table_data(db, "students", 10);
    
    printf("\nCourses table:\n");
    rdb_print_table_data(db, "courses", 10);
    
    printf("\nEnrollments table:\n");
    rdb_print_table_data(db, "enrollments", 10);
    
    /* Clean up */
    fi_array_destroy(students_columns);
    fi_array_destroy(courses_columns);
    fi_array_destroy(enrollments_columns);
    rdb_destroy_database(db);
    
    printf("Multi-table queries demo completed\n");
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
    column->is_foreign_key = false;
    column->foreign_table[0] = '\0';
    column->foreign_column[0] = '\0';
    
    return column;
}

rdb_column_t* create_foreign_key_column(const char *name, rdb_data_type_t type, const char *ref_table, const char *ref_column) {
    rdb_column_t *column = create_column(name, type, false, false, false);
    if (!column) return NULL;
    
    column->is_foreign_key = true;
    strncpy(column->foreign_table, ref_table, sizeof(column->foreign_table) - 1);
    column->foreign_table[sizeof(column->foreign_table) - 1] = '\0';
    strncpy(column->foreign_column, ref_column, sizeof(column->foreign_column) - 1);
    column->foreign_column[sizeof(column->foreign_column) - 1] = '\0';
    
    return column;
}

void print_separator(const char *title) {
    printf("\n");
    printf("========================================\n");
    printf("  %s\n", title);
    printf("========================================\n");
}
