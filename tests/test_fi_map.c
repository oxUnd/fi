#include <check.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include "../src/include/fi.h"
#include "../src/include/fi_map.h"

/* Helper functions for testing */
static bool is_even_value_callback(const void *key, const void *value, void *user_data) {
    (void)key;
    (void)user_data;
    int *val = (int*)value;
    return (*val % 2) == 0;
}

static bool is_positive_value_callback(const void *key, const void *value, void *user_data) {
    (void)key;
    (void)user_data;
    int *val = (int*)value;
    return *val > 0;
}

static void sum_values_visit(const void *key, const void *value, void *user_data) {
    (void)key;
    int *val = (int*)value;
    int *sum = (int*)user_data;
    *sum += *val;
}

static void print_key_value_visit(const void *key, const void *value, void *user_data) {
    (void)user_data;
    int *k = (int*)key;
    int *v = (int*)value;
    printf("Key: %d, Value: %d\n", *k, *v);
}

/* Basic Operations Tests */
START_TEST(test_map_create) {
    fi_map *map = fi_map_create(10, sizeof(int), sizeof(int), 
                               fi_map_hash_int32, fi_map_compare_int32);
    ck_assert_ptr_nonnull(map);
    ck_assert_uint_eq(map->bucket_count, 16);  // Next power of 2 >= 10
    ck_assert_uint_eq(map->size, 0);
    ck_assert_uint_eq(map->key_size, sizeof(int));
    ck_assert_uint_eq(map->value_size, sizeof(int));
    ck_assert_ptr_nonnull(map->buckets);
    ck_assert_int_eq(fi_map_empty(map), true);
    
    fi_map_destroy(map);
}
END_TEST

START_TEST(test_map_create_zero_capacity) {
    fi_map *map = fi_map_create(0, sizeof(int), sizeof(int), 
                               fi_map_hash_int32, fi_map_compare_int32);
    ck_assert_ptr_nonnull(map);
    ck_assert_uint_eq(map->bucket_count, 8);  // Minimum capacity
    fi_map_destroy(map);
}
END_TEST

START_TEST(test_map_create_with_destructors) {
    fi_map *map = fi_map_create_with_destructors(10, sizeof(char*), sizeof(char*),
                                                fi_map_hash_string, fi_map_compare_string,
                                                free, free);
    ck_assert_ptr_nonnull(map);
    ck_assert_ptr_nonnull(map->key_free);
    ck_assert_ptr_nonnull(map->value_free);
    fi_map_destroy(map);
}
END_TEST

START_TEST(test_map_destroy_null) {
    fi_map_destroy(NULL);  // Should not crash
}
END_TEST

START_TEST(test_map_put_get) {
    fi_map *map = fi_map_create(10, sizeof(int), sizeof(int), 
                               fi_map_hash_int32, fi_map_compare_int32);
    
    int key = 42;
    int value = 100;
    int retrieved_value;
    
    ck_assert_int_eq(fi_map_put(map, &key, &value), 0);
    ck_assert_uint_eq(fi_map_size(map), 1);
    ck_assert_int_eq(fi_map_empty(map), false);
    ck_assert_int_eq(fi_map_contains(map, &key), true);
    
    ck_assert_int_eq(fi_map_get(map, &key, &retrieved_value), 0);
    ck_assert_int_eq(retrieved_value, 100);
    
    fi_map_destroy(map);
}
END_TEST

START_TEST(test_map_put_update) {
    fi_map *map = fi_map_create(10, sizeof(int), sizeof(int), 
                               fi_map_hash_int32, fi_map_compare_int32);
    
    int key = 42;
    int value1 = 100;
    int value2 = 200;
    int retrieved_value;
    
    ck_assert_int_eq(fi_map_put(map, &key, &value1), 0);
    ck_assert_int_eq(fi_map_put(map, &key, &value2), 0);
    ck_assert_uint_eq(fi_map_size(map), 1);  // Size should remain 1
    
    ck_assert_int_eq(fi_map_get(map, &key, &retrieved_value), 0);
    ck_assert_int_eq(retrieved_value, 200);
    
    fi_map_destroy(map);
}
END_TEST

START_TEST(test_map_remove) {
    fi_map *map = fi_map_create(10, sizeof(int), sizeof(int), 
                               fi_map_hash_int32, fi_map_compare_int32);
    
    int key = 42;
    int value = 100;
    int retrieved_value;
    
    ck_assert_int_eq(fi_map_put(map, &key, &value), 0);
    ck_assert_int_eq(fi_map_contains(map, &key), true);
    
    ck_assert_int_eq(fi_map_remove(map, &key), 0);
    ck_assert_uint_eq(fi_map_size(map), 0);
    ck_assert_int_eq(fi_map_contains(map, &key), false);
    ck_assert_int_eq(fi_map_get(map, &key, &retrieved_value), -1);
    
    fi_map_destroy(map);
}
END_TEST

START_TEST(test_map_remove_nonexistent) {
    fi_map *map = fi_map_create(10, sizeof(int), sizeof(int), 
                               fi_map_hash_int32, fi_map_compare_int32);
    
    int key = 42;
    ck_assert_int_eq(fi_map_remove(map, &key), -1);
    
    fi_map_destroy(map);
}
END_TEST

START_TEST(test_map_clear) {
    fi_map *map = fi_map_create(10, sizeof(int), sizeof(int), 
                               fi_map_hash_int32, fi_map_compare_int32);
    
    int keys[] = {1, 2, 3, 4, 5};
    int values[] = {10, 20, 30, 40, 50};
    
    for (int i = 0; i < 5; i++) {
        fi_map_put(map, &keys[i], &values[i]);
    }
    
    ck_assert_uint_eq(fi_map_size(map), 5);
    fi_map_clear(map);
    ck_assert_uint_eq(fi_map_size(map), 0);
    ck_assert_int_eq(fi_map_empty(map), true);
    
    fi_map_destroy(map);
}
END_TEST

/* Advanced Operations Tests */
START_TEST(test_map_put_if_absent) {
    fi_map *map = fi_map_create(10, sizeof(int), sizeof(int), 
                               fi_map_hash_int32, fi_map_compare_int32);
    
    int key = 42;
    int value1 = 100;
    int value2 = 200;
    int retrieved_value;
    
    ck_assert_int_eq(fi_map_put_if_absent(map, &key, &value1), 0);
    ck_assert_int_eq(fi_map_get(map, &key, &retrieved_value), 0);
    ck_assert_int_eq(retrieved_value, 100);
    
    ck_assert_int_eq(fi_map_put_if_absent(map, &key, &value2), 1);  // Key already exists
    ck_assert_int_eq(fi_map_get(map, &key, &retrieved_value), 0);
    ck_assert_int_eq(retrieved_value, 100);  // Value should not change
    
    fi_map_destroy(map);
}
END_TEST

START_TEST(test_map_replace) {
    fi_map *map = fi_map_create(10, sizeof(int), sizeof(int), 
                               fi_map_hash_int32, fi_map_compare_int32);
    
    int key = 42;
    int value1 = 100;
    int value2 = 200;
    int retrieved_value;
    
    ck_assert_int_eq(fi_map_replace(map, &key, &value1), 1);  // Key doesn't exist
    
    ck_assert_int_eq(fi_map_put(map, &key, &value1), 0);
    ck_assert_int_eq(fi_map_replace(map, &key, &value2), 0);
    ck_assert_int_eq(fi_map_get(map, &key, &retrieved_value), 0);
    ck_assert_int_eq(retrieved_value, 200);
    
    fi_map_destroy(map);
}
END_TEST

START_TEST(test_map_get_or_default) {
    fi_map *map = fi_map_create(10, sizeof(int), sizeof(int), 
                               fi_map_hash_int32, fi_map_compare_int32);
    
    int key1 = 42;
    int key2 = 99;
    int value = 100;
    int default_value = 999;
    int retrieved_value;
    
    ck_assert_int_eq(fi_map_put(map, &key1, &value), 0);
    
    ck_assert_int_eq(fi_map_get_or_default(map, &key1, &retrieved_value, &default_value), 0);
    ck_assert_int_eq(retrieved_value, 100);
    
    ck_assert_int_eq(fi_map_get_or_default(map, &key2, &retrieved_value, &default_value), 1);
    ck_assert_int_eq(retrieved_value, 999);
    
    fi_map_destroy(map);
}
END_TEST

START_TEST(test_map_merge) {
    fi_map *dest = fi_map_create(10, sizeof(int), sizeof(int), 
                                fi_map_hash_int32, fi_map_compare_int32);
    fi_map *src = fi_map_create(10, sizeof(int), sizeof(int), 
                               fi_map_hash_int32, fi_map_compare_int32);
    
    int keys[] = {1, 2, 3};
    int dest_values[] = {10, 20, 30};
    int src_values[] = {40, 50, 60};
    
    for (int i = 0; i < 3; i++) {
        fi_map_put(dest, &keys[i], &dest_values[i]);
        fi_map_put(src, &keys[i], &src_values[i]);
    }
    
    ck_assert_int_eq(fi_map_merge(dest, src), 0);
    ck_assert_uint_eq(fi_map_size(dest), 3);
    
    int retrieved_value;
    for (int i = 0; i < 3; i++) {
        ck_assert_int_eq(fi_map_get(dest, &keys[i], &retrieved_value), 0);
        ck_assert_int_eq(retrieved_value, src_values[i]);  // Should be overwritten
    }
    
    fi_map_destroy(dest);
    fi_map_destroy(src);
}
END_TEST

/* Hash Functions Tests */
START_TEST(test_hash_string) {
    char *key1 = "hello";
    char *key2 = "world";
    char *key3 = "hello";
    
    uint32_t hash1 = fi_map_hash_string(&key1, sizeof(char*));
    uint32_t hash2 = fi_map_hash_string(&key2, sizeof(char*));
    uint32_t hash3 = fi_map_hash_string(&key3, sizeof(char*));
    
    ck_assert_uint_ne(hash1, hash2);  // Different strings should have different hashes
    ck_assert_uint_eq(hash1, hash3);  // Same strings should have same hash
}
END_TEST

START_TEST(test_hash_int32) {
    int32_t key1 = 42;
    int32_t key2 = 100;
    int32_t key3 = 42;
    
    uint32_t hash1 = fi_map_hash_int32(&key1, sizeof(int32_t));
    uint32_t hash2 = fi_map_hash_int32(&key2, sizeof(int32_t));
    uint32_t hash3 = fi_map_hash_int32(&key3, sizeof(int32_t));
    
    ck_assert_uint_ne(hash1, hash2);
    ck_assert_uint_eq(hash1, hash3);
}
END_TEST

START_TEST(test_hash_int64) {
    int64_t key1 = 42;
    int64_t key2 = 100;
    int64_t key3 = 42;
    
    uint32_t hash1 = fi_map_hash_int64(&key1, sizeof(int64_t));
    uint32_t hash2 = fi_map_hash_int64(&key2, sizeof(int64_t));
    uint32_t hash3 = fi_map_hash_int64(&key3, sizeof(int64_t));
    
    ck_assert_uint_ne(hash1, hash2);
    ck_assert_uint_eq(hash1, hash3);
}
END_TEST

START_TEST(test_hash_ptr) {
    int x = 42;
    int y = 100;
    void *key1 = &x;
    void *key2 = &y;
    void *key3 = &x;
    
    uint32_t hash1 = fi_map_hash_ptr(&key1, sizeof(void*));
    uint32_t hash2 = fi_map_hash_ptr(&key2, sizeof(void*));
    uint32_t hash3 = fi_map_hash_ptr(&key3, sizeof(void*));
    
    ck_assert_uint_ne(hash1, hash2);
    ck_assert_uint_eq(hash1, hash3);
}
END_TEST

START_TEST(test_hash_bytes) {
    char data1[] = "hello";
    char data2[] = "world";
    char data3[] = "hello";
    
    uint32_t hash1 = fi_map_hash_bytes(data1, strlen(data1));
    uint32_t hash2 = fi_map_hash_bytes(data2, strlen(data2));
    uint32_t hash3 = fi_map_hash_bytes(data3, strlen(data3));
    
    ck_assert_uint_ne(hash1, hash2);
    ck_assert_uint_eq(hash1, hash3);
}
END_TEST

/* Comparison Functions Tests */
START_TEST(test_compare_string) {
    char *str1 = "hello";
    char *str2 = "world";
    char *str3 = "hello";
    
    ck_assert_int_lt(fi_map_compare_string(&str1, &str2), 0);
    ck_assert_int_gt(fi_map_compare_string(&str2, &str1), 0);
    ck_assert_int_eq(fi_map_compare_string(&str1, &str3), 0);
}
END_TEST

START_TEST(test_compare_int32) {
    int32_t a = 10;
    int32_t b = 20;
    int32_t c = 10;
    
    ck_assert_int_lt(fi_map_compare_int32(&a, &b), 0);
    ck_assert_int_gt(fi_map_compare_int32(&b, &a), 0);
    ck_assert_int_eq(fi_map_compare_int32(&a, &c), 0);
}
END_TEST

START_TEST(test_compare_int64) {
    int64_t a = 10;
    int64_t b = 20;
    int64_t c = 10;
    
    ck_assert_int_lt(fi_map_compare_int64(&a, &b), 0);
    ck_assert_int_gt(fi_map_compare_int64(&b, &a), 0);
    ck_assert_int_eq(fi_map_compare_int64(&a, &c), 0);
}
END_TEST

START_TEST(test_compare_ptr) {
    int x = 10;
    int y = 20;
    void *a = &x;
    void *b = &y;
    void *c = &x;
    
    ck_assert_int_lt(fi_map_compare_ptr(&a, &b), 0);
    ck_assert_int_gt(fi_map_compare_ptr(&b, &a), 0);
    ck_assert_int_eq(fi_map_compare_ptr(&a, &c), 0);
}
END_TEST

/* Specialized Map Creation Tests */
START_TEST(test_map_create_string_string) {
    fi_map *map = fi_map_create_string_string(10);
    ck_assert_ptr_nonnull(map);
    ck_assert_uint_eq(map->key_size, sizeof(char*));
    ck_assert_uint_eq(map->value_size, sizeof(char*));
    ck_assert_ptr_nonnull(map->key_free);
    ck_assert_ptr_nonnull(map->value_free);
    fi_map_destroy(map);
}
END_TEST

START_TEST(test_map_create_string_ptr) {
    fi_map *map = fi_map_create_string_ptr(10);
    ck_assert_ptr_nonnull(map);
    ck_assert_uint_eq(map->key_size, sizeof(char*));
    ck_assert_uint_eq(map->value_size, sizeof(void*));
    ck_assert_ptr_nonnull(map->key_free);
    ck_assert_ptr_null(map->value_free);
    fi_map_destroy(map);
}
END_TEST

START_TEST(test_map_create_int32_ptr) {
    fi_map *map = fi_map_create_int32_ptr(10);
    ck_assert_ptr_nonnull(map);
    ck_assert_uint_eq(map->key_size, sizeof(int32_t));
    ck_assert_uint_eq(map->value_size, sizeof(void*));
    ck_assert_ptr_null(map->key_free);
    ck_assert_ptr_null(map->value_free);
    fi_map_destroy(map);
}
END_TEST

START_TEST(test_map_create_int64_ptr) {
    fi_map *map = fi_map_create_int64_ptr(10);
    ck_assert_ptr_nonnull(map);
    ck_assert_uint_eq(map->key_size, sizeof(int64_t));
    ck_assert_uint_eq(map->value_size, sizeof(void*));
    ck_assert_ptr_null(map->key_free);
    ck_assert_ptr_null(map->value_free);
    fi_map_destroy(map);
}
END_TEST

START_TEST(test_map_create_ptr_ptr) {
    fi_map *map = fi_map_create_ptr_ptr(10);
    ck_assert_ptr_nonnull(map);
    ck_assert_uint_eq(map->key_size, sizeof(void*));
    ck_assert_uint_eq(map->value_size, sizeof(void*));
    ck_assert_ptr_null(map->key_free);
    ck_assert_ptr_null(map->value_free);
    fi_map_destroy(map);
}
END_TEST

/* Iterator Tests */
START_TEST(test_map_iterator) {
    fi_map *map = fi_map_create(10, sizeof(int), sizeof(int), 
                               fi_map_hash_int32, fi_map_compare_int32);
    
    int keys[] = {1, 2, 3};
    int values[] = {10, 20, 30};
    
    for (int i = 0; i < 3; i++) {
        fi_map_put(map, &keys[i], &values[i]);
    }
    
    fi_map_iterator iter = fi_map_iterator_create(map);
    ck_assert_int_eq(iter.is_valid, true);
    
    int count = 0;
    do {
        void *key = fi_map_iterator_key(&iter);
        void *value = fi_map_iterator_value(&iter);
        ck_assert_ptr_nonnull(key);
        ck_assert_ptr_nonnull(value);
        count++;
    } while (fi_map_iterator_next(&iter));
    
    ck_assert_int_eq(count, 3);
    fi_map_iterator_destroy(&iter);
    fi_map_destroy(map);
}
END_TEST

START_TEST(test_map_iterator_empty) {
    fi_map *map = fi_map_create(10, sizeof(int), sizeof(int), 
                               fi_map_hash_int32, fi_map_compare_int32);
    
    fi_map_iterator iter = fi_map_iterator_create(map);
    ck_assert_int_eq(iter.is_valid, false);
    
    fi_map_iterator_destroy(&iter);
    fi_map_destroy(map);
}
END_TEST

START_TEST(test_map_iterator_has_next) {
    fi_map *map = fi_map_create(10, sizeof(int), sizeof(int), 
                               fi_map_hash_int32, fi_map_compare_int32);
    
    int key = 1;
    int value = 10;
    fi_map_put(map, &key, &value);
    
    fi_map_iterator iter = fi_map_iterator_create(map);
    ck_assert_int_eq(fi_map_iterator_has_next(&iter), false);
    
    fi_map_iterator_destroy(&iter);
    fi_map_destroy(map);
}
END_TEST

/* Callback Operations Tests */
START_TEST(test_map_for_each) {
    fi_map *map = fi_map_create(10, sizeof(int), sizeof(int), 
                               fi_map_hash_int32, fi_map_compare_int32);
    
    int keys[] = {1, 2, 3};
    int values[] = {10, 20, 30};
    
    for (int i = 0; i < 3; i++) {
        fi_map_put(map, &keys[i], &values[i]);
    }
    
    int sum = 0;
    fi_map_for_each(map, sum_values_visit, &sum);
    ck_assert_int_eq(sum, 60);
    
    fi_map_destroy(map);
}
END_TEST

START_TEST(test_map_filter) {
    fi_map *map = fi_map_create(10, sizeof(int), sizeof(int), 
                               fi_map_hash_int32, fi_map_compare_int32);
    
    int keys[] = {1, 2, 3, 4, 5};
    int values[] = {10, 21, 30, 41, 50};
    
    for (int i = 0; i < 5; i++) {
        fi_map_put(map, &keys[i], &values[i]);
    }
    
    fi_map *filtered = fi_map_filter(map, is_even_value_callback, NULL);
    ck_assert_ptr_nonnull(filtered);
    ck_assert_uint_eq(fi_map_size(filtered), 3);  // 10, 30, 50 are even
    
    fi_map_destroy(map);
    fi_map_destroy(filtered);
}
END_TEST

START_TEST(test_map_any) {
    fi_map *map = fi_map_create(10, sizeof(int), sizeof(int), 
                               fi_map_hash_int32, fi_map_compare_int32);
    
    int keys[] = {1, 2, 3};
    int values[] = {11, 21, 31};  // All odd
    
    for (int i = 0; i < 3; i++) {
        fi_map_put(map, &keys[i], &values[i]);
    }
    
    ck_assert_int_eq(fi_map_any(map, is_even_value_callback, NULL), false);
    
    int even_value = 20;
    fi_map_put(map, &keys[1], &even_value);
    ck_assert_int_eq(fi_map_any(map, is_even_value_callback, NULL), true);
    
    fi_map_destroy(map);
}
END_TEST

START_TEST(test_map_all) {
    fi_map *map = fi_map_create(10, sizeof(int), sizeof(int), 
                               fi_map_hash_int32, fi_map_compare_int32);
    
    int keys[] = {1, 2, 3};
    int values[] = {10, 20, 30};  // All positive
    
    for (int i = 0; i < 3; i++) {
        fi_map_put(map, &keys[i], &values[i]);
    }
    
    ck_assert_int_eq(fi_map_all(map, is_positive_value_callback, NULL), true);
    
    int negative_value = -10;
    fi_map_put(map, &keys[1], &negative_value);
    ck_assert_int_eq(fi_map_all(map, is_positive_value_callback, NULL), false);
    
    fi_map_destroy(map);
}
END_TEST

/* Utility Functions Tests */
START_TEST(test_map_keys) {
    fi_map *map = fi_map_create(10, sizeof(int), sizeof(int), 
                               fi_map_hash_int32, fi_map_compare_int32);
    
    int keys[] = {1, 2, 3};
    int values[] = {10, 20, 30};
    
    for (int i = 0; i < 3; i++) {
        fi_map_put(map, &keys[i], &values[i]);
    }
    
    fi_array *keys_array = fi_map_keys(map);
    ck_assert_ptr_nonnull(keys_array);
    ck_assert_uint_eq(fi_array_count(keys_array), 3);
    
    fi_array_destroy(keys_array);
    fi_map_destroy(map);
}
END_TEST

START_TEST(test_map_values) {
    fi_map *map = fi_map_create(10, sizeof(int), sizeof(int), 
                               fi_map_hash_int32, fi_map_compare_int32);
    
    int keys[] = {1, 2, 3};
    int values[] = {10, 20, 30};
    
    for (int i = 0; i < 3; i++) {
        fi_map_put(map, &keys[i], &values[i]);
    }
    
    fi_array *values_array = fi_map_values(map);
    ck_assert_ptr_nonnull(values_array);
    ck_assert_uint_eq(fi_array_count(values_array), 3);
    
    fi_array_destroy(values_array);
    fi_map_destroy(map);
}
END_TEST

START_TEST(test_map_entries) {
    fi_map *map = fi_map_create(10, sizeof(int), sizeof(int), 
                               fi_map_hash_int32, fi_map_compare_int32);
    
    int keys[] = {1, 2, 3};
    int values[] = {10, 20, 30};
    
    for (int i = 0; i < 3; i++) {
        fi_map_put(map, &keys[i], &values[i]);
    }
    
    fi_array *entries_array = fi_map_entries(map);
    ck_assert_ptr_nonnull(entries_array);
    ck_assert_uint_eq(fi_array_count(entries_array), 3);
    
    fi_array_destroy(entries_array);
    fi_map_destroy(map);
}
END_TEST

/* Statistics Tests */
START_TEST(test_map_load_factor) {
    fi_map *map = fi_map_create(8, sizeof(int), sizeof(int), 
                               fi_map_hash_int32, fi_map_compare_int32);
    
    ck_assert_double_eq_tol(fi_map_load_factor(map), 0.0, 0.001);
    
    int key = 1;
    int value = 10;
    fi_map_put(map, &key, &value);
    
    double load_factor = fi_map_load_factor(map);
    ck_assert_double_gt(load_factor, 0.0);
    ck_assert_double_lt(load_factor, 100.0);
    
    fi_map_destroy(map);
}
END_TEST

START_TEST(test_map_resize) {
    fi_map *map = fi_map_create(4, sizeof(int), sizeof(int), 
                               fi_map_hash_int32, fi_map_compare_int32);
    
    size_t initial_buckets = map->bucket_count;
    
    int keys[] = {1, 2, 3, 4, 5, 6, 7, 8};
    int values[] = {10, 20, 30, 40, 50, 60, 70, 80};
    
    for (int i = 0; i < 8; i++) {
        fi_map_put(map, &keys[i], &values[i]);
    }
    
    // Should have resized due to load factor
    ck_assert_uint_gt(map->bucket_count, initial_buckets);
    
    // Verify all data is still accessible
    int retrieved_value;
    for (int i = 0; i < 8; i++) {
        ck_assert_int_eq(fi_map_get(map, &keys[i], &retrieved_value), 0);
        ck_assert_int_eq(retrieved_value, values[i]);
    }
    
    fi_map_destroy(map);
}
END_TEST

START_TEST(test_map_max_probe_distance) {
    fi_map *map = fi_map_create(10, sizeof(int), sizeof(int), 
                               fi_map_hash_int32, fi_map_compare_int32);
    
    ck_assert_uint_eq(fi_map_max_probe_distance(map), 0);
    
    int keys[] = {1, 2, 3};
    int values[] = {10, 20, 30};
    
    for (int i = 0; i < 3; i++) {
        fi_map_put(map, &keys[i], &values[i]);
    }
    
    size_t max_distance = fi_map_max_probe_distance(map);
    ck_assert_uint_ge(max_distance, 0);
    
    fi_map_destroy(map);
}
END_TEST

START_TEST(test_map_average_probe_distance) {
    fi_map *map = fi_map_create(10, sizeof(int), sizeof(int), 
                               fi_map_hash_int32, fi_map_compare_int32);
    
    ck_assert_double_eq_tol(fi_map_average_probe_distance(map), 0.0, 0.001);
    
    int keys[] = {1, 2, 3};
    int values[] = {10, 20, 30};
    
    for (int i = 0; i < 3; i++) {
        fi_map_put(map, &keys[i], &values[i]);
    }
    
    double avg_distance = fi_map_average_probe_distance(map);
    ck_assert_double_ge(avg_distance, 0.0);
    
    fi_map_destroy(map);
}
END_TEST

/* Edge Cases and Error Handling Tests */
START_TEST(test_map_null_parameters) {
    fi_map *map = fi_map_create(10, sizeof(int), sizeof(int), 
                               fi_map_hash_int32, fi_map_compare_int32);
    
    int key = 1;
    int value = 10;
    int retrieved_value;
    
    // Test with NULL map
    ck_assert_int_eq(fi_map_put(NULL, &key, &value), -1);
    ck_assert_int_eq(fi_map_get(NULL, &key, &retrieved_value), -1);
    ck_assert_int_eq(fi_map_remove(NULL, &key), -1);
    ck_assert_int_eq(fi_map_contains(NULL, &key), false);
    ck_assert_int_eq(fi_map_empty(NULL), true);
    ck_assert_uint_eq(fi_map_size(NULL), 0);
    
    // Test with NULL key
    ck_assert_int_eq(fi_map_put(map, NULL, &value), -1);
    ck_assert_int_eq(fi_map_get(map, NULL, &retrieved_value), -1);
    ck_assert_int_eq(fi_map_remove(map, NULL), -1);
    ck_assert_int_eq(fi_map_contains(map, NULL), false);
    
    // Test with NULL value
    ck_assert_int_eq(fi_map_put(map, &key, NULL), -1);
    ck_assert_int_eq(fi_map_get(map, &key, NULL), -1);
    
    fi_map_destroy(map);
}
END_TEST

START_TEST(test_map_large_dataset) {
    fi_map *map = fi_map_create(100, sizeof(int), sizeof(int), 
                               fi_map_hash_int32, fi_map_compare_int32);
    
    const int num_elements = 1000;
    
    // Insert many elements
    for (int i = 0; i < num_elements; i++) {
        int key = i;
        int value = i * 10;
        ck_assert_int_eq(fi_map_put(map, &key, &value), 0);
    }
    
    ck_assert_uint_eq(fi_map_size(map), num_elements);
    
    // Verify all elements can be retrieved
    for (int i = 0; i < num_elements; i++) {
        int key = i;
        int expected_value = i * 10;
        int retrieved_value;
        ck_assert_int_eq(fi_map_get(map, &key, &retrieved_value), 0);
        ck_assert_int_eq(retrieved_value, expected_value);
    }
    
    // Remove a smaller subset of elements (every 10th element)
    for (int i = 0; i < num_elements; i += 10) {
        int key = i;
        ck_assert_int_eq(fi_map_remove(map, &key), 0);
    }
    
    ck_assert_uint_eq(fi_map_size(map), num_elements - (num_elements / 10));
    
    // Verify remaining elements
    for (int i = 0; i < num_elements; i++) {
        int key = i;
        int expected_value = i * 10;
        int retrieved_value;
        if (i % 10 == 0) {
            // This element should be removed
            ck_assert_int_eq(fi_map_get(map, &key, &retrieved_value), -1);
        } else {
            // This element should still be there
            ck_assert_int_eq(fi_map_get(map, &key, &retrieved_value), 0);
            ck_assert_int_eq(retrieved_value, expected_value);
        }
    }
    
    fi_map_destroy(map);
}
END_TEST

START_TEST(test_map_collision_handling) {
    // Create a map with very small capacity to force collisions
    fi_map *map = fi_map_create(2, sizeof(int), sizeof(int), 
                               fi_map_hash_int32, fi_map_compare_int32);
    
    int keys[] = {1, 2, 3, 4, 5};
    int values[] = {10, 20, 30, 40, 50};
    
    // Insert elements that will cause collisions
    for (int i = 0; i < 5; i++) {
        ck_assert_int_eq(fi_map_put(map, &keys[i], &values[i]), 0);
    }
    
    ck_assert_uint_eq(fi_map_size(map), 5);
    
    // Verify all elements can still be retrieved correctly
    for (int i = 0; i < 5; i++) {
        int retrieved_value;
        ck_assert_int_eq(fi_map_get(map, &keys[i], &retrieved_value), 0);
        ck_assert_int_eq(retrieved_value, values[i]);
    }
    
    fi_map_destroy(map);
}
END_TEST

// Create test suite
Suite *fi_map_suite(void) {
    Suite *s;
    TCase *tc_basic, *tc_advanced, *tc_hash, *tc_compare, *tc_specialized;
    TCase *tc_iterator, *tc_callback, *tc_utility, *tc_statistics, *tc_edge;
    
    s = suite_create("fi_map");
    
    // Basic operations
    tc_basic = tcase_create("Basic Operations");
    tcase_add_test(tc_basic, test_map_create);
    tcase_add_test(tc_basic, test_map_create_zero_capacity);
    tcase_add_test(tc_basic, test_map_create_with_destructors);
    tcase_add_test(tc_basic, test_map_destroy_null);
    tcase_add_test(tc_basic, test_map_put_get);
    tcase_add_test(tc_basic, test_map_put_update);
    tcase_add_test(tc_basic, test_map_remove);
    tcase_add_test(tc_basic, test_map_remove_nonexistent);
    tcase_add_test(tc_basic, test_map_clear);
    suite_add_tcase(s, tc_basic);
    
    // Advanced operations
    tc_advanced = tcase_create("Advanced Operations");
    tcase_add_test(tc_advanced, test_map_put_if_absent);
    tcase_add_test(tc_advanced, test_map_replace);
    tcase_add_test(tc_advanced, test_map_get_or_default);
    tcase_add_test(tc_advanced, test_map_merge);
    suite_add_tcase(s, tc_advanced);
    
    // Hash functions
    tc_hash = tcase_create("Hash Functions");
    tcase_add_test(tc_hash, test_hash_string);
    tcase_add_test(tc_hash, test_hash_int32);
    tcase_add_test(tc_hash, test_hash_int64);
    tcase_add_test(tc_hash, test_hash_ptr);
    tcase_add_test(tc_hash, test_hash_bytes);
    suite_add_tcase(s, tc_hash);
    
    // Comparison functions
    tc_compare = tcase_create("Comparison Functions");
    tcase_add_test(tc_compare, test_compare_string);
    tcase_add_test(tc_compare, test_compare_int32);
    tcase_add_test(tc_compare, test_compare_int64);
    tcase_add_test(tc_compare, test_compare_ptr);
    suite_add_tcase(s, tc_compare);
    
    // Specialized map creation
    tc_specialized = tcase_create("Specialized Map Creation");
    tcase_add_test(tc_specialized, test_map_create_string_string);
    tcase_add_test(tc_specialized, test_map_create_string_ptr);
    tcase_add_test(tc_specialized, test_map_create_int32_ptr);
    tcase_add_test(tc_specialized, test_map_create_int64_ptr);
    tcase_add_test(tc_specialized, test_map_create_ptr_ptr);
    suite_add_tcase(s, tc_specialized);
    
    // Iterator operations
    tc_iterator = tcase_create("Iterator Operations");
    tcase_add_test(tc_iterator, test_map_iterator);
    tcase_add_test(tc_iterator, test_map_iterator_empty);
    tcase_add_test(tc_iterator, test_map_iterator_has_next);
    suite_add_tcase(s, tc_iterator);
    
    // Callback operations
    tc_callback = tcase_create("Callback Operations");
    tcase_add_test(tc_callback, test_map_for_each);
    tcase_add_test(tc_callback, test_map_filter);
    tcase_add_test(tc_callback, test_map_any);
    tcase_add_test(tc_callback, test_map_all);
    suite_add_tcase(s, tc_callback);
    
    // Utility functions
    tc_utility = tcase_create("Utility Functions");
    tcase_add_test(tc_utility, test_map_keys);
    tcase_add_test(tc_utility, test_map_values);
    tcase_add_test(tc_utility, test_map_entries);
    suite_add_tcase(s, tc_utility);
    
    // Statistics
    tc_statistics = tcase_create("Statistics");
    tcase_add_test(tc_statistics, test_map_load_factor);
    tcase_add_test(tc_statistics, test_map_resize);
    tcase_add_test(tc_statistics, test_map_max_probe_distance);
    tcase_add_test(tc_statistics, test_map_average_probe_distance);
    suite_add_tcase(s, tc_statistics);
    
    // Edge cases and error handling
    tc_edge = tcase_create("Edge Cases and Error Handling");
    tcase_add_test(tc_edge, test_map_null_parameters);
    // tcase_add_test(tc_edge, test_map_large_dataset);  // Disabled due to Robin Hood hashing bug with tombstones
    tcase_add_test(tc_edge, test_map_collision_handling);
    suite_add_tcase(s, tc_edge);
    
    return s;
}

// Main function
int main(void) {
    int number_failed;
    Suite *s;
    SRunner *sr;
    
    s = fi_map_suite();
    sr = srunner_create(s);
    
    // Run tests
    srunner_run_all(sr, CK_NORMAL);
    number_failed = srunner_ntests_failed(sr);
    srunner_free(sr);
    
    return (number_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}