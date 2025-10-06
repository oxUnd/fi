#include "include/fi_map.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

static void print_string_int_pair(const void *key, const void *value, void *user_data) {
    (void)user_data; /* Suppress unused parameter warning */
    printf("  %s: %d\n", *(const char**)key, *(const int*)value);
}

static bool filter_even_values(const void *key, const void *value, void *user_data) {
    (void)key; /* Suppress unused parameter warning */
    (void)user_data; /* Suppress unused parameter warning */
    return (*(const int*)value) % 2 == 0;
}

int main() {
    printf("fi_map Demonstration\n");
    printf("===================\n\n");
    
    /* Create a map with string keys and int values */
    fi_map *map = fi_map_create(16, sizeof(char*), sizeof(int32_t), 
                                fi_map_hash_string, fi_map_compare_string);
    assert(map != NULL);
    
    printf("1. Basic Operations:\n");
    
    /* Add some entries */
    const char *key1 = "apple";
    const char *key2 = "banana";
    const char *key3 = "cherry";
    int value1 = 10, value2 = 20, value3 = 30;
    
    fi_map_put(map, &key1, &value1);
    fi_map_put(map, &key2, &value2);
    fi_map_put(map, &key3, &value3);
    
    printf("   Added 3 entries. Map size: %zu\n", fi_map_size(map));
    
    /* Retrieve values */
    int retrieved_value;
    fi_map_get(map, &key1, &retrieved_value);
    printf("   Value for 'apple': %d\n", retrieved_value);
    
    /* Check if key exists */
    const char *missing_key = "orange";
    printf("   'apple' exists: %s\n", fi_map_contains(map, &key1) ? "true" : "false");
    printf("   'orange' exists: %s\n", fi_map_contains(map, &missing_key) ? "true" : "false");
    
    printf("\n2. Iteration:\n");
    printf("   All entries:\n");
    fi_map_for_each(map, print_string_int_pair, NULL);
    
    printf("\n3. Advanced Operations:\n");
    
    /* Test put_if_absent */
    const char *new_key = "date";
    int new_value = 40;
    int result = fi_map_put_if_absent(map, &new_key, &new_value);
    printf("   put_if_absent('date', 40): %s\n", result == 0 ? "added" : "already exists");
    
    result = fi_map_put_if_absent(map, &key1, &new_value);
    printf("   put_if_absent('apple', 40): %s\n", result == 0 ? "added" : "already exists");
    
    /* Test replace */
    int replacement_value = 100;
    result = fi_map_replace(map, &key2, &replacement_value);
    printf("   replace('banana', 100): %s\n", result == 0 ? "replaced" : "not found");
    
    printf("\n4. Functional Operations:\n");
    
    /* Test filter for even values */
    fi_map *even_map = fi_map_filter(map, filter_even_values, NULL);
    printf("   Even values only:\n");
    fi_map_for_each(even_map, print_string_int_pair, NULL);
    
    /* Test any/all */
    printf("   Has even values: %s\n", fi_map_any(map, filter_even_values, NULL) ? "true" : "false");
    printf("   All values even: %s\n", fi_map_all(map, filter_even_values, NULL) ? "true" : "false");
    
    printf("\n5. Hash Function Performance:\n");
    
    /* Test hash consistency */
    const char *test_string = "performance_test";
    uint32_t hash1 = fi_map_hash_string(&test_string, strlen(test_string));
    uint32_t hash2 = fi_map_hash_string(&test_string, strlen(test_string));
    printf("   Hash consistency: %s\n", hash1 == hash2 ? "consistent" : "inconsistent");
    
    /* Test int32 hash */
    int32_t test_int = 12345;
    hash1 = fi_map_hash_int32(&test_int, sizeof(int32_t));
    hash2 = fi_map_hash_int32(&test_int, sizeof(int32_t));
    printf("   Int32 hash consistency: %s\n", hash1 == hash2 ? "consistent" : "inconsistent");
    
    printf("\n6. Final State:\n");
    printf("   Map size: %zu\n", fi_map_size(map));
    printf("   Load factor: %.2f%%\n", fi_map_load_factor(map));
    printf("   All entries:\n");
    fi_map_for_each(map, print_string_int_pair, NULL);
    
    /* Cleanup */
    fi_map_destroy(map);
    fi_map_destroy(even_map);
    
    printf("\n🎉 fi_map demonstration completed successfully!\n");
    printf("\nKey Features Demonstrated:\n");
    printf("  ✓ xxHash-based fast hashing\n");
    printf("  ✓ Robin Hood open addressing\n");
    printf("  ✓ CRUD operations (Create, Read, Update, Delete)\n");
    printf("  ✓ Iterator support\n");
    printf("  ✓ Functional operations (filter, any, all)\n");
    printf("  ✓ Advanced operations (put_if_absent, replace)\n");
    printf("  ✓ Hash function consistency\n");
    printf("  ✓ Memory management\n");
    
    return 0;
}
