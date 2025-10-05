#include <check.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include "../src/include/fi.h"

/* Helper functions for testing */
static int int_compare(const void *a, const void *b) {
    // qsort passes void** pointers to array elements
    const int *ia = *(const int**)a;
    const int *ib = *(const int**)b;
    return (*ia > *ib) - (*ia < *ib);
}

static bool is_even_callback(void *element, size_t index, void *user_data) {
    (void)index;
    (void)user_data;
    int *value = (int*)element;
    return (*value % 2) == 0;
}

static void increment_callback(void *element, size_t index, void *user_data) {
    (void)index;
    int *value = (int*)element;
    int *increment = (int*)user_data;
    *value += *increment;
}

static bool reduce_sum_callback(void *element, size_t index, void *user_data) {
    (void)index;
    int *value = (int*)element;
    int *sum = (int*)user_data;
    *sum += *value;
    return true;
}

/* Basic Operations Tests */
START_TEST(test_array_create) {
    fi_array *arr = fi_array_create(10, sizeof(int));
    ck_assert_ptr_nonnull(arr);
    ck_assert_uint_eq(arr->capacity, 10);
    ck_assert_uint_eq(arr->size, 0);
    ck_assert_uint_eq(arr->element_size, sizeof(int));
    ck_assert_ptr_nonnull(arr->data);
    
    fi_array_destroy(arr);
}
END_TEST

START_TEST(test_array_create_zero_capacity) {
    fi_array *arr = fi_array_create(0, sizeof(int));
    ck_assert_ptr_nonnull(arr);
    ck_assert_uint_eq(arr->capacity, 8);  // Default capacity
    fi_array_destroy(arr);
}
END_TEST

START_TEST(test_array_destroy_null) {
    fi_array_destroy(NULL);  // Should not crash
}
END_TEST

START_TEST(test_array_copy) {
    fi_array *arr = fi_array_create(5, sizeof(int));
    ck_assert_ptr_nonnull(arr);
    
    int values[] = {1, 2, 3, 4, 5};
    for (int i = 0; i < 5; i++) {
        fi_array_push(arr, &values[i]);
    }
    
    fi_array *copy = fi_array_copy(arr);
    ck_assert_ptr_nonnull(copy);
    ck_assert_uint_eq(copy->size, arr->size);
    ck_assert_uint_eq(copy->capacity, arr->capacity);
    ck_assert_uint_eq(copy->element_size, arr->element_size);
    
    for (size_t i = 0; i < arr->size; i++) {
        int *orig = (int*)fi_array_get(arr, i);
        int *cpy = (int*)fi_array_get(copy, i);
        ck_assert_int_eq(*orig, *cpy);
        ck_assert_ptr_ne(orig, cpy);  // Different memory locations
    }
    
    fi_array_destroy(arr);
    fi_array_destroy(copy);
}
END_TEST

START_TEST(test_array_slice) {
    fi_array *arr = fi_array_create(10, sizeof(int));
    int values[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    for (int i = 0; i < 10; i++) {
        fi_array_push(arr, &values[i]);
    }
    
    fi_array *slice = fi_array_slice(arr, 2, 4);
    ck_assert_ptr_nonnull(slice);
    ck_assert_uint_eq(slice->size, 4);
    
    for (size_t i = 0; i < slice->size; i++) {
        int *value = (int*)fi_array_get(slice, i);
        ck_assert_int_eq(*value, (int)(3 + i));
    }
    
    fi_array_destroy(arr);
    fi_array_destroy(slice);
}
END_TEST

START_TEST(test_array_slice_invalid_offset) {
    fi_array *arr = fi_array_create(5, sizeof(int));
    int value = 1;
    fi_array_push(arr, &value);
    
    fi_array *slice = fi_array_slice(arr, 5, 1);
    ck_assert_ptr_null(slice);
    
    fi_array_destroy(arr);
}
END_TEST

/* Element Access Tests */
START_TEST(test_array_get_set) {
    fi_array *arr = fi_array_create(5, sizeof(int));
    
    int value = 42;
    fi_array_push(arr, &value);
    
    int *retrieved = (int*)fi_array_get(arr, 0);
    ck_assert_ptr_nonnull(retrieved);
    ck_assert_int_eq(*retrieved, 42);
    
    int new_value = 100;
    fi_array_set(arr, 0, &new_value);
    
    retrieved = (int*)fi_array_get(arr, 0);
    ck_assert_int_eq(*retrieved, 100);
    
    fi_array_destroy(arr);
}
END_TEST

START_TEST(test_array_get_invalid_index) {
    fi_array *arr = fi_array_create(5, sizeof(int));
    
    void *result = fi_array_get(arr, 0);
    ck_assert_ptr_null(result);
    
    result = fi_array_get(arr, 100);
    ck_assert_ptr_null(result);
    
    fi_array_destroy(arr);
}
END_TEST

START_TEST(test_array_key_exists) {
    fi_array *arr = fi_array_create(5, sizeof(int));
    
    ck_assert_int_eq(fi_array_key_exists(arr, 0), false);
    
    int value = 42;
    fi_array_push(arr, &value);
    
    ck_assert_int_eq(fi_array_key_exists(arr, 0), true);
    ck_assert_int_eq(fi_array_key_exists(arr, 1), false);
    
    fi_array_destroy(arr);
}
END_TEST

START_TEST(test_array_count_empty) {
    fi_array *arr = fi_array_create(5, sizeof(int));
    
    ck_assert_uint_eq(fi_array_count(arr), 0);
    ck_assert_int_eq(fi_array_empty(arr), true);
    
    int value = 42;
    fi_array_push(arr, &value);
    
    ck_assert_uint_eq(fi_array_count(arr), 1);
    ck_assert_int_eq(fi_array_empty(arr), false);
    
    fi_array_destroy(arr);
}
END_TEST

/* Stack Operations Tests */
START_TEST(test_array_push_pop) {
    fi_array *arr = fi_array_create(5, sizeof(int));
    
    int values[] = {1, 2, 3};
    for (int i = 0; i < 3; i++) {
        ck_assert_int_eq(fi_array_push(arr, &values[i]), 0);
    }
    
    ck_assert_uint_eq(fi_array_count(arr), 3);
    
    int popped_value;
    ck_assert_int_eq(fi_array_pop(arr, &popped_value), 0);
    ck_assert_int_eq(popped_value, 3);
    ck_assert_uint_eq(fi_array_count(arr), 2);
    
    ck_assert_int_eq(fi_array_pop(arr, &popped_value), 0);
    ck_assert_int_eq(popped_value, 2);
    ck_assert_uint_eq(fi_array_count(arr), 1);
    
    fi_array_destroy(arr);
}
END_TEST

START_TEST(test_array_pop_empty) {
    fi_array *arr = fi_array_create(5, sizeof(int));
    
    int value;
    ck_assert_int_eq(fi_array_pop(arr, &value), -1);
    
    fi_array_destroy(arr);
}
END_TEST

START_TEST(test_array_unshift_shift) {
    fi_array *arr = fi_array_create(5, sizeof(int));
    
    int values[] = {1, 2, 3};
    for (int i = 0; i < 3; i++) {
        ck_assert_int_eq(fi_array_unshift(arr, &values[i]), 0);
    }
    
    ck_assert_uint_eq(fi_array_count(arr), 3);
    
    int shifted_value;
    ck_assert_int_eq(fi_array_shift(arr, &shifted_value), 0);
    ck_assert_int_eq(shifted_value, 3);
    ck_assert_uint_eq(fi_array_count(arr), 2);
    
    ck_assert_int_eq(fi_array_shift(arr, &shifted_value), 0);
    ck_assert_int_eq(shifted_value, 2);
    ck_assert_uint_eq(fi_array_count(arr), 1);
    
    fi_array_destroy(arr);
}
END_TEST

START_TEST(test_array_shift_empty) {
    fi_array *arr = fi_array_create(5, sizeof(int));
    
    int value;
    ck_assert_int_eq(fi_array_shift(arr, &value), -1);
    
    fi_array_destroy(arr);
}
END_TEST

/* Array Manipulation Tests */
START_TEST(test_array_merge) {
    fi_array *dest = fi_array_create(5, sizeof(int));
    fi_array *src = fi_array_create(3, sizeof(int));
    
    int dest_values[] = {1, 2};
    int src_values[] = {3, 4, 5};
    
    for (int i = 0; i < 2; i++) {
        fi_array_push(dest, &dest_values[i]);
    }
    for (int i = 0; i < 3; i++) {
        fi_array_push(src, &src_values[i]);
    }
    
    ck_assert_int_eq(fi_array_merge(dest, src), 0);
    ck_assert_uint_eq(fi_array_count(dest), 5);
    
    for (size_t i = 0; i < 5; i++) {
        int *value = (int*)fi_array_get(dest, i);
        ck_assert_int_eq(*value, (int)(i + 1));
    }
    
    fi_array_destroy(dest);
    fi_array_destroy(src);
}
END_TEST

START_TEST(test_array_splice) {
    fi_array *arr = fi_array_create(10, sizeof(int));
    int values[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    for (int i = 0; i < 10; i++) {
        fi_array_push(arr, &values[i]);
    }
    
    int replacement = 99;
    ck_assert_int_eq(fi_array_splice(arr, 2, 3, &replacement), 0);
    ck_assert_uint_eq(fi_array_count(arr), 8);
    
    // Check remaining elements
    int *value = (int*)fi_array_get(arr, 0);
    ck_assert_int_eq(*value, 1);
    value = (int*)fi_array_get(arr, 1);
    ck_assert_int_eq(*value, 2);
    value = (int*)fi_array_get(arr, 2);
    ck_assert_int_eq(*value, 99);
    value = (int*)fi_array_get(arr, 3);
    ck_assert_int_eq(*value, 6);
    
    fi_array_destroy(arr);
}
END_TEST

START_TEST(test_array_pad) {
    fi_array *arr = fi_array_create(5, sizeof(int));
    int value = 42;
    fi_array_push(arr, &value);
    
    int pad_value = 0;
    ck_assert_int_eq(fi_array_pad(arr, 5, &pad_value), 0);
    ck_assert_uint_eq(fi_array_count(arr), 5);
    
    for (size_t i = 0; i < 5; i++) {
        int *val = (int*)fi_array_get(arr, i);
        if (i == 0) {
            ck_assert_int_eq(*val, 42);
        } else {
            ck_assert_int_eq(*val, 0);
        }
    }
    
    fi_array_destroy(arr);
}
END_TEST

START_TEST(test_array_fill) {
    fi_array *arr = fi_array_create(5, sizeof(int));
    int values[] = {1, 2, 3, 4, 5};
    for (int i = 0; i < 5; i++) {
        fi_array_push(arr, &values[i]);
    }
    
    int fill_value = 99;
    ck_assert_int_eq(fi_array_fill(arr, 1, 3, &fill_value), 0);
    
    for (size_t i = 0; i < 5; i++) {
        int *val = (int*)fi_array_get(arr, i);
        if (i >= 1 && i <= 3) {
            ck_assert_int_eq(*val, 99);
        } else {
            ck_assert_int_eq(*val, (int)(i + 1));
        }
    }
    
    fi_array_destroy(arr);
}
END_TEST

/* Search Operations Tests */
START_TEST(test_array_search) {
    fi_array *arr = fi_array_create(5, sizeof(int));
    int values[] = {10, 20, 30, 40, 50};
    for (int i = 0; i < 5; i++) {
        fi_array_push(arr, &values[i]);
    }
    
    int search_value = 30;
    ssize_t index = fi_array_search(arr, &search_value);
    ck_assert_int_eq(index, 2);
    
    search_value = 60;
    index = fi_array_search(arr, &search_value);
    ck_assert_int_eq(index, -1);
    
    fi_array_destroy(arr);
}
END_TEST

START_TEST(test_array_in_array) {
    fi_array *arr = fi_array_create(5, sizeof(int));
    int values[] = {10, 20, 30, 40, 50};
    for (int i = 0; i < 5; i++) {
        fi_array_push(arr, &values[i]);
    }
    
    int search_value = 30;
    ck_assert_int_eq(fi_array_in_array(arr, &search_value), true);
    
    search_value = 60;
    ck_assert_int_eq(fi_array_in_array(arr, &search_value), false);
    
    fi_array_destroy(arr);
}
END_TEST

START_TEST(test_array_find) {
    fi_array *arr = fi_array_create(5, sizeof(int));
    int values[] = {1, 2, 3, 4, 5};
    for (int i = 0; i < 5; i++) {
        fi_array_push(arr, &values[i]);
    }
    
    void *found = fi_array_find(arr, is_even_callback, NULL);
    ck_assert_ptr_nonnull(found);
    ck_assert_int_eq(*(int*)found, 2);
    
    fi_array_destroy(arr);
}
END_TEST

START_TEST(test_array_find_key) {
    fi_array *arr = fi_array_create(5, sizeof(int));
    int values[] = {1, 2, 3, 4, 5};
    for (int i = 0; i < 5; i++) {
        fi_array_push(arr, &values[i]);
    }
    
    size_t key = fi_array_find_key(arr, is_even_callback, NULL);
    ck_assert_uint_eq(key, 1);
    
    fi_array_destroy(arr);
}
END_TEST

/* Callback Operations Tests */
START_TEST(test_array_all) {
    fi_array *arr = fi_array_create(5, sizeof(int));
    int values[] = {2, 4, 6, 8, 10};
    for (int i = 0; i < 5; i++) {
        fi_array_push(arr, &values[i]);
    }
    
    ck_assert_int_eq(fi_array_all(arr, is_even_callback, NULL), true);
    
    int odd_value = 3;
    fi_array_set(arr, 2, &odd_value);
    ck_assert_int_eq(fi_array_all(arr, is_even_callback, NULL), false);
    
    fi_array_destroy(arr);
}
END_TEST

START_TEST(test_array_any) {
    fi_array *arr = fi_array_create(5, sizeof(int));
    int values[] = {1, 3, 5, 7, 9};
    for (int i = 0; i < 5; i++) {
        fi_array_push(arr, &values[i]);
    }
    
    ck_assert_int_eq(fi_array_any(arr, is_even_callback, NULL), false);
    
    int even_value = 4;
    fi_array_set(arr, 2, &even_value);
    ck_assert_int_eq(fi_array_any(arr, is_even_callback, NULL), true);
    
    fi_array_destroy(arr);
}
END_TEST

START_TEST(test_array_filter) {
    fi_array *arr = fi_array_create(5, sizeof(int));
    int values[] = {1, 2, 3, 4, 5};
    for (int i = 0; i < 5; i++) {
        fi_array_push(arr, &values[i]);
    }
    
    fi_array *filtered = fi_array_filter(arr, is_even_callback, NULL);
    ck_assert_ptr_nonnull(filtered);
    ck_assert_uint_eq(fi_array_count(filtered), 2);
    
    int *value = (int*)fi_array_get(filtered, 0);
    ck_assert_int_eq(*value, 2);
    value = (int*)fi_array_get(filtered, 1);
    ck_assert_int_eq(*value, 4);
    
    fi_array_destroy(arr);
    fi_array_destroy(filtered);
}
END_TEST

START_TEST(test_array_map) {
    fi_array *arr = fi_array_create(3, sizeof(int));
    int values[] = {1, 2, 3};
    for (int i = 0; i < 3; i++) {
        fi_array_push(arr, &values[i]);
    }
    
    fi_array *mapped = fi_array_map(arr, NULL, NULL);
    ck_assert_ptr_nonnull(mapped);
    ck_assert_uint_eq(fi_array_count(mapped), 3);
    
    fi_array_destroy(arr);
    fi_array_destroy(mapped);
}
END_TEST

START_TEST(test_array_reduce) {
    fi_array *arr = fi_array_create(5, sizeof(int));
    int values[] = {1, 2, 3, 4, 5};
    for (int i = 0; i < 5; i++) {
        fi_array_push(arr, &values[i]);
    }
    
    int initial = 0;
    int result = 0;
    void *reduced = fi_array_reduce(arr, reduce_sum_callback, &initial, &result);
    ck_assert_ptr_nonnull(reduced);
    ck_assert_int_eq(result, 15);
    
    fi_array_destroy(arr);
}
END_TEST

START_TEST(test_array_walk) {
    fi_array *arr = fi_array_create(3, sizeof(int));
    int values[] = {1, 2, 3};
    for (int i = 0; i < 3; i++) {
        fi_array_push(arr, &values[i]);
    }
    
    int increment = 10;
    fi_array_walk(arr, increment_callback, &increment);
    
    for (size_t i = 0; i < 3; i++) {
        int *value = (int*)fi_array_get(arr, i);
        ck_assert_int_eq(*value, (int)(i + 11));
    }
    
    fi_array_destroy(arr);
}
END_TEST

/* Comparison Operations Tests */
START_TEST(test_array_diff) {
    fi_array *arr1 = fi_array_create(5, sizeof(int));
    fi_array *arr2 = fi_array_create(3, sizeof(int));
    
    int values1[] = {1, 2, 3, 4, 5};
    int values2[] = {2, 4, 6};
    
    for (int i = 0; i < 5; i++) {
        fi_array_push(arr1, &values1[i]);
    }
    for (int i = 0; i < 3; i++) {
        fi_array_push(arr2, &values2[i]);
    }
    
    fi_array *diff = fi_array_diff(arr1, arr2);
    ck_assert_ptr_nonnull(diff);
    ck_assert_uint_eq(fi_array_count(diff), 3);
    
    int *value = (int*)fi_array_get(diff, 0);
    ck_assert_int_eq(*value, 1);
    value = (int*)fi_array_get(diff, 1);
    ck_assert_int_eq(*value, 3);
    value = (int*)fi_array_get(diff, 2);
    ck_assert_int_eq(*value, 5);
    
    fi_array_destroy(arr1);
    fi_array_destroy(arr2);
    fi_array_destroy(diff);
}
END_TEST

START_TEST(test_array_intersect) {
    fi_array *arr1 = fi_array_create(5, sizeof(int));
    fi_array *arr2 = fi_array_create(3, sizeof(int));
    
    int values1[] = {1, 2, 3, 4, 5};
    int values2[] = {2, 4, 6};
    
    for (int i = 0; i < 5; i++) {
        fi_array_push(arr1, &values1[i]);
    }
    for (int i = 0; i < 3; i++) {
        fi_array_push(arr2, &values2[i]);
    }
    
    fi_array *intersect = fi_array_intersect(arr1, arr2);
    ck_assert_ptr_nonnull(intersect);
    ck_assert_uint_eq(fi_array_count(intersect), 2);
    
    int *value = (int*)fi_array_get(intersect, 0);
    ck_assert_int_eq(*value, 2);
    value = (int*)fi_array_get(intersect, 1);
    ck_assert_int_eq(*value, 4);
    
    fi_array_destroy(arr1);
    fi_array_destroy(arr2);
    fi_array_destroy(intersect);
}
END_TEST

START_TEST(test_array_unique) {
    fi_array *arr = fi_array_create(7, sizeof(int));
    int values[] = {1, 2, 2, 3, 3, 3, 4};
    for (int i = 0; i < 7; i++) {
        fi_array_push(arr, &values[i]);
    }
    
    fi_array *unique = fi_array_unique(arr);
    ck_assert_ptr_nonnull(unique);
    ck_assert_uint_eq(fi_array_count(unique), 4);
    
    for (size_t i = 0; i < 4; i++) {
        int *value = (int*)fi_array_get(unique, i);
        ck_assert_int_eq(*value, (int)(i + 1));
    }
    
    fi_array_destroy(arr);
    fi_array_destroy(unique);
}
END_TEST

/* Sorting Operations Tests */
START_TEST(test_array_sort) {
    fi_array *arr = fi_array_create(5, sizeof(int));
    int values[] = {5, 2, 8, 1, 9};
    int expected[] = {1, 2, 5, 8, 9};
    for (int i = 0; i < 5; i++) {
        fi_array_push(arr, &values[i]);
    }
    
    fi_array_sort(arr, int_compare);
    
    for (size_t i = 0; i < 5; i++) {
        int *value = (int*)fi_array_get(arr, i);
        ck_assert_int_eq(*value, expected[i]);
    }
    
    fi_array_destroy(arr);
}
END_TEST

START_TEST(test_array_reverse) {
    fi_array *arr = fi_array_create(5, sizeof(int));
    int values[] = {1, 2, 3, 4, 5};
    for (int i = 0; i < 5; i++) {
        fi_array_push(arr, &values[i]);
    }
    
    fi_array_reverse(arr);
    
    for (size_t i = 0; i < 5; i++) {
        int *value = (int*)fi_array_get(arr, i);
        ck_assert_int_eq(*value, (int)(5 - i));
    }
    
    fi_array_destroy(arr);
}
END_TEST

START_TEST(test_array_shuffle) {
    fi_array *arr = fi_array_create(5, sizeof(int));
    int values[] = {1, 2, 3, 4, 5};
    for (int i = 0; i < 5; i++) {
        fi_array_push(arr, &values[i]);
    }
    
    fi_array_shuffle(arr);
    
    // After shuffle, we should still have the same elements but possibly in different order
    ck_assert_uint_eq(fi_array_count(arr), 5);
    
    // Check that all original values are still present
    for (int i = 1; i <= 5; i++) {
        ck_assert_int_eq(fi_array_in_array(arr, &i), true);
    }
    
    fi_array_destroy(arr);
}
END_TEST

/* Mathematical Operations Tests */
START_TEST(test_array_sum) {
    fi_array *arr = fi_array_create(5, sizeof(double));
    double values[] = {1.5, 2.5, 3.5, 4.5, 5.5};
    for (int i = 0; i < 5; i++) {
        fi_array_push(arr, &values[i]);
    }
    
    double sum = fi_array_sum(arr);
    ck_assert_double_eq_tol(sum, 17.5, 0.001);
    
    fi_array_destroy(arr);
}
END_TEST

START_TEST(test_array_product) {
    fi_array *arr = fi_array_create(4, sizeof(double));
    double values[] = {2.0, 3.0, 4.0, 5.0};
    for (int i = 0; i < 4; i++) {
        fi_array_push(arr, &values[i]);
    }
    
    double product = fi_array_product(arr);
    ck_assert_double_eq_tol(product, 120.0, 0.001);
    
    fi_array_destroy(arr);
}
END_TEST

/* Special Functions Tests */
START_TEST(test_array_range) {
    fi_array *range = fi_array_range(1, 6, 1);
    ck_assert_ptr_nonnull(range);
    ck_assert_uint_eq(fi_array_count(range), 5);
    
    for (size_t i = 0; i < 5; i++) {
        long *value = (long*)fi_array_get(range, i);
        ck_assert_int_eq(*value, (long)(i + 1));
    }
    
    fi_array_destroy(range);
}
END_TEST

START_TEST(test_array_range_negative_step) {
    fi_array *range = fi_array_range(5, 0, -1);
    ck_assert_ptr_nonnull(range);
    ck_assert_uint_eq(fi_array_count(range), 5);
    
    for (size_t i = 0; i < 5; i++) {
        long *value = (long*)fi_array_get(range, i);
        ck_assert_int_eq(*value, (long)(5 - i));
    }
    
    fi_array_destroy(range);
}
END_TEST

START_TEST(test_array_compact) {
    fi_array *arr = fi_array_create(5, sizeof(int));
    int values[] = {1, 0, 2, 0, 3};
    for (int i = 0; i < 5; i++) {
        if (values[i] != 0) {
            fi_array_push(arr, &values[i]);
        } else {
            fi_array_push(arr, NULL);
        }
    }
    
    fi_array *compact = fi_array_compact(arr);
    ck_assert_ptr_nonnull(compact);
    ck_assert_uint_eq(fi_array_count(compact), 3);
    
    for (size_t i = 0; i < 3; i++) {
        int *value = (int*)fi_array_get(compact, i);
        ck_assert_int_eq(*value, (int)(i + 1));
    }
    
    fi_array_destroy(arr);
    fi_array_destroy(compact);
}
END_TEST

/* Iterator Operations Tests */
START_TEST(test_array_current_key) {
    fi_array *arr = fi_array_create(5, sizeof(int));
    int values[] = {10, 20, 30, 40, 50};
    for (int i = 0; i < 5; i++) {
        fi_array_push(arr, &values[i]);
    }
    
    // Test current and key functions
    // The iterator starts at index 0 by default
    void *current = fi_array_current(arr);
    ck_assert_ptr_nonnull(current);
    ck_assert_int_eq(*(int*)current, 10);
    
    size_t key = fi_array_key(arr);
    ck_assert_uint_eq(key, 0);  // Should be 0 initially
    
    fi_array_destroy(arr);
}
END_TEST

START_TEST(test_array_next_prev) {
    fi_array *arr = fi_array_create(5, sizeof(int));
    int values[] = {10, 20, 30, 40, 50};
    for (int i = 0; i < 5; i++) {
        fi_array_push(arr, &values[i]);
    }
    
    // Reset to beginning
    void *first = fi_array_reset(arr);
    ck_assert_ptr_nonnull(first);
    ck_assert_int_eq(*(int*)first, 10);
    
    // Test next
    void *next = fi_array_next(arr);
    ck_assert_ptr_nonnull(next);
    ck_assert_int_eq(*(int*)next, 20);
    
    next = fi_array_next(arr);
    ck_assert_ptr_nonnull(next);
    ck_assert_int_eq(*(int*)next, 30);
    
    // Test prev
    void *prev = fi_array_prev(arr);
    ck_assert_ptr_nonnull(prev);
    ck_assert_int_eq(*(int*)prev, 20);
    
    fi_array_destroy(arr);
}
END_TEST

START_TEST(test_array_reset_end) {
    fi_array *arr = fi_array_create(5, sizeof(int));
    int values[] = {10, 20, 30, 40, 50};
    for (int i = 0; i < 5; i++) {
        fi_array_push(arr, &values[i]);
    }
    
    // Test reset
    void *first = fi_array_reset(arr);
    ck_assert_ptr_nonnull(first);
    ck_assert_int_eq(*(int*)first, 10);
    
    // Test end
    void *last = fi_array_end(arr);
    ck_assert_ptr_nonnull(last);
    ck_assert_int_eq(*(int*)last, 50);
    
    fi_array_destroy(arr);
}
END_TEST

START_TEST(test_array_iterator_boundaries) {
    fi_array *arr = fi_array_create(3, sizeof(int));
    int values[] = {10, 20, 30};
    for (int i = 0; i < 3; i++) {
        fi_array_push(arr, &values[i]);
    }
    
    // Move past end
    fi_array_end(arr);
    void *beyond_end = fi_array_next(arr);
    ck_assert_ptr_null(beyond_end);
    
    // Try to go before beginning
    fi_array_reset(arr);
    void *before_begin = fi_array_prev(arr);
    ck_assert_ptr_null(before_begin);
    
    fi_array_destroy(arr);
}
END_TEST

/* Utility Functions Tests */
START_TEST(test_array_keys) {
    fi_array *arr = fi_array_create(3, sizeof(int));
    int values[] = {10, 20, 30};
    for (int i = 0; i < 3; i++) {
        fi_array_push(arr, &values[i]);
    }
    
    fi_array *keys = fi_array_keys(arr);
    ck_assert_ptr_nonnull(keys);
    ck_assert_uint_eq(fi_array_count(keys), 3);
    
    for (size_t i = 0; i < 3; i++) {
        size_t *key = (size_t*)fi_array_get(keys, i);
        ck_assert_uint_eq(*key, i);
    }
    
    fi_array_destroy(arr);
    fi_array_destroy(keys);
}
END_TEST

START_TEST(test_array_values) {
    fi_array *arr = fi_array_create(3, sizeof(int));
    int values[] = {10, 20, 30};
    for (int i = 0; i < 3; i++) {
        fi_array_push(arr, &values[i]);
    }
    
    fi_array *values_copy = fi_array_values(arr);
    ck_assert_ptr_nonnull(values_copy);
    ck_assert_uint_eq(fi_array_count(values_copy), 3);
    
    for (size_t i = 0; i < 3; i++) {
        int *orig = (int*)fi_array_get(arr, i);
        int *copy = (int*)fi_array_get(values_copy, i);
        ck_assert_int_eq(*orig, *copy);
    }
    
    fi_array_destroy(arr);
    fi_array_destroy(values_copy);
}
END_TEST

START_TEST(test_array_chunk) {
    fi_array *arr = fi_array_create(7, sizeof(int));
    int values[] = {1, 2, 3, 4, 5, 6, 7};
    for (int i = 0; i < 7; i++) {
        fi_array_push(arr, &values[i]);
    }
    
    fi_array *chunks = fi_array_chunk(arr, 3);
    ck_assert_ptr_nonnull(chunks);
    ck_assert_uint_eq(fi_array_count(chunks), 3);  // 3 chunks: [1,2,3], [4,5,6], [7]
    
    // Check first chunk
    fi_array *chunk1 = *(fi_array**)fi_array_get(chunks, 0);
    ck_assert_uint_eq(fi_array_count(chunk1), 3);
    int *val = (int*)fi_array_get(chunk1, 0);
    ck_assert_int_eq(*val, 1);
    
    // Check last chunk (should have 1 element)
    fi_array *chunk3 = *(fi_array**)fi_array_get(chunks, 2);
    ck_assert_uint_eq(fi_array_count(chunk3), 1);
    val = (int*)fi_array_get(chunk3, 0);
    ck_assert_int_eq(*val, 7);
    
    fi_array_destroy(arr);
    fi_array_destroy(chunks);
}
END_TEST

// Create test suite
Suite *fi_array_suite(void) {
    Suite *s;
    TCase *tc_basic, *tc_access, *tc_stack, *tc_manipulation, *tc_search, *tc_callback;
    TCase *tc_comparison, *tc_sorting, *tc_math, *tc_special, *tc_utility, *tc_iterator;
    
    s = suite_create("fi_array");
    
    // Basic operations
    tc_basic = tcase_create("Basic Operations");
    tcase_add_test(tc_basic, test_array_create);
    tcase_add_test(tc_basic, test_array_create_zero_capacity);
    tcase_add_test(tc_basic, test_array_destroy_null);
    tcase_add_test(tc_basic, test_array_copy);
    tcase_add_test(tc_basic, test_array_slice);
    tcase_add_test(tc_basic, test_array_slice_invalid_offset);
    suite_add_tcase(s, tc_basic);
    
    // Element access
    tc_access = tcase_create("Element Access");
    tcase_add_test(tc_access, test_array_get_set);
    tcase_add_test(tc_access, test_array_get_invalid_index);
    tcase_add_test(tc_access, test_array_key_exists);
    tcase_add_test(tc_access, test_array_count_empty);
    suite_add_tcase(s, tc_access);
    
    // Stack operations
    tc_stack = tcase_create("Stack Operations");
    tcase_add_test(tc_stack, test_array_push_pop);
    tcase_add_test(tc_stack, test_array_pop_empty);
    tcase_add_test(tc_stack, test_array_unshift_shift);
    tcase_add_test(tc_stack, test_array_shift_empty);
    suite_add_tcase(s, tc_stack);
    
    // Array manipulation
    tc_manipulation = tcase_create("Array Manipulation");
    tcase_add_test(tc_manipulation, test_array_merge);
    tcase_add_test(tc_manipulation, test_array_splice);
    tcase_add_test(tc_manipulation, test_array_pad);
    tcase_add_test(tc_manipulation, test_array_fill);
    suite_add_tcase(s, tc_manipulation);
    
    // Search operations
    tc_search = tcase_create("Search Operations");
    tcase_add_test(tc_search, test_array_search);
    tcase_add_test(tc_search, test_array_in_array);
    tcase_add_test(tc_search, test_array_find);
    tcase_add_test(tc_search, test_array_find_key);
    suite_add_tcase(s, tc_search);
    
    // Callback operations
    tc_callback = tcase_create("Callback Operations");
    tcase_add_test(tc_callback, test_array_all);
    tcase_add_test(tc_callback, test_array_any);
    tcase_add_test(tc_callback, test_array_filter);
    tcase_add_test(tc_callback, test_array_map);
    tcase_add_test(tc_callback, test_array_reduce);
    tcase_add_test(tc_callback, test_array_walk);
    suite_add_tcase(s, tc_callback);
    
    // Comparison operations
    tc_comparison = tcase_create("Comparison Operations");
    tcase_add_test(tc_comparison, test_array_diff);
    tcase_add_test(tc_comparison, test_array_intersect);
    tcase_add_test(tc_comparison, test_array_unique);
    suite_add_tcase(s, tc_comparison);
    
    // Sorting operations
    tc_sorting = tcase_create("Sorting Operations");
    tcase_add_test(tc_sorting, test_array_sort);
    tcase_add_test(tc_sorting, test_array_reverse);
    tcase_add_test(tc_sorting, test_array_shuffle);
    suite_add_tcase(s, tc_sorting);
    
    // Mathematical operations
    tc_math = tcase_create("Mathematical Operations");
    tcase_add_test(tc_math, test_array_sum);
    tcase_add_test(tc_math, test_array_product);
    suite_add_tcase(s, tc_math);
    
    // Special functions
    tc_special = tcase_create("Special Functions");
    tcase_add_test(tc_special, test_array_range);
    tcase_add_test(tc_special, test_array_range_negative_step);
    tcase_add_test(tc_special, test_array_compact);
    suite_add_tcase(s, tc_special);
    
    // Utility functions
    tc_utility = tcase_create("Utility Functions");
    tcase_add_test(tc_utility, test_array_keys);
    tcase_add_test(tc_utility, test_array_values);
    tcase_add_test(tc_utility, test_array_chunk);
    suite_add_tcase(s, tc_utility);
    
    // Iterator operations
    tc_iterator = tcase_create("Iterator Operations");
    tcase_add_test(tc_iterator, test_array_current_key);
    tcase_add_test(tc_iterator, test_array_next_prev);
    tcase_add_test(tc_iterator, test_array_reset_end);
    tcase_add_test(tc_iterator, test_array_iterator_boundaries);
    suite_add_tcase(s, tc_iterator);
    
    return s;
}

// Main function
int main(void) {
    int number_failed;
    Suite *s;
    SRunner *sr;
    
    s = fi_array_suite();
    sr = srunner_create(s);
    
    // Run tests
    srunner_run_all(sr, CK_NORMAL);
    number_failed = srunner_ntests_failed(sr);
    srunner_free(sr);
    
    return (number_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}
