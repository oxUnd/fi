#include "fi_map.h"
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <math.h>

/* xxHash implementation - Fast non-cryptographic hash function */
/* This is a simplified but high-performance implementation of xxHash */

#define FI_MAP_PRIME32_1 0x9E3779B1U
#define FI_MAP_PRIME32_2 0x85EBCA77U
#define FI_MAP_PRIME32_3 0xC2B2AE3DU
#define FI_MAP_PRIME32_4 0x27D4EB2FU
#define FI_MAP_PRIME32_5 0x165667B1U

/* Rotate left operation */
static inline uint32_t fi_map_rotl32(uint32_t x, int r) {
    return (x << r) | (x >> (32 - r));
}

/* xxHash32 implementation */
static uint32_t fi_map_xxhash32(const void *input, size_t len, uint32_t seed) {
    const uint8_t *p = (const uint8_t *)input;
    const uint8_t *const bEnd = p + len;
    uint32_t h32;

    if (len >= 16) {
        const uint8_t *const limit = bEnd - 16;
        uint32_t v1 = seed + FI_MAP_PRIME32_1 + FI_MAP_PRIME32_2;
        uint32_t v2 = seed + FI_MAP_PRIME32_2;
        uint32_t v3 = seed + 0;
        uint32_t v4 = seed - FI_MAP_PRIME32_1;

        do {
            v1 += ((uint32_t *)p)[0] * FI_MAP_PRIME32_2;
            v1 = fi_map_rotl32(v1, 13);
            v1 *= FI_MAP_PRIME32_1;
            p += 4;

            v2 += ((uint32_t *)p)[0] * FI_MAP_PRIME32_2;
            v2 = fi_map_rotl32(v2, 13);
            v2 *= FI_MAP_PRIME32_1;
            p += 4;

            v3 += ((uint32_t *)p)[0] * FI_MAP_PRIME32_2;
            v3 = fi_map_rotl32(v3, 13);
            v3 *= FI_MAP_PRIME32_1;
            p += 4;

            v4 += ((uint32_t *)p)[0] * FI_MAP_PRIME32_2;
            v4 = fi_map_rotl32(v4, 13);
            v4 *= FI_MAP_PRIME32_1;
            p += 4;
        } while (p <= limit);

        h32 = fi_map_rotl32(v1, 1) + fi_map_rotl32(v2, 7) + 
              fi_map_rotl32(v3, 12) + fi_map_rotl32(v4, 18);
    } else {
        h32 = seed + FI_MAP_PRIME32_5;
    }

    h32 += (uint32_t)len;

    /* Process remaining bytes */
    while (p + 4 <= bEnd) {
        h32 += ((uint32_t *)p)[0] * FI_MAP_PRIME32_3;
        h32 = fi_map_rotl32(h32, 17) * FI_MAP_PRIME32_4;
        p += 4;
    }

    while (p < bEnd) {
        h32 += (*p) * FI_MAP_PRIME32_5;
        h32 = fi_map_rotl32(h32, 11) * FI_MAP_PRIME32_1;
        p++;
    }

    h32 ^= h32 >> 15;
    h32 *= FI_MAP_PRIME32_2;
    h32 ^= h32 >> 13;
    h32 *= FI_MAP_PRIME32_3;
    h32 ^= h32 >> 16;

    return h32;
}

/* Helper function to find next power of 2 */
static size_t fi_map_next_power_of_2(size_t n) {
    if (n == 0) return 1;
    n--;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
    n |= n >> 32;
    return n + 1;
}

/* Helper function to check if a number is power of 2 */
static bool fi_map_is_power_of_2(size_t n) {
    return n != 0 && (n & (n - 1)) == 0;
}

/* Calculate bucket index from hash */
static size_t fi_map_bucket_index(const fi_map *map, uint32_t hash) {
    return hash & (map->bucket_count - 1);
}

/* Find entry in hash map using Robin Hood hashing */
static fi_map_entry* fi_map_find_entry(const fi_map *map, const void *key, uint32_t hash) {
    size_t bucket = fi_map_bucket_index(map, hash);
    uint32_t distance = 0;
    
    while (distance < map->bucket_count) {
        fi_map_entry *entry = &map->buckets[bucket];
        
        if (entry->key == NULL) {
            /* Empty bucket found */
            return NULL;
        }
        
        if (!entry->is_deleted && 
            entry->hash == hash && 
            map->key_compare(entry->key, key) == 0) {
            /* Found the key */
            return entry;
        }
        
        /* Robin Hood: if current entry's distance is less than ours, keep going */
        if (entry->key != NULL && !entry->is_deleted && entry->distance < distance) {
            return NULL; /* Key should be here but isn't */
        }
        
        bucket = (bucket + 1) & (map->bucket_count - 1);
        distance++;
    }
    
    return NULL;
}

/* Resize the hash map */
static int fi_map_resize_internal(fi_map *map, size_t new_bucket_count) {
    if (!fi_map_is_power_of_2(new_bucket_count)) {
        new_bucket_count = fi_map_next_power_of_2(new_bucket_count);
    }
    
    fi_map_entry *old_buckets = map->buckets;
    size_t old_bucket_count = map->bucket_count;
    
    map->buckets = calloc(new_bucket_count, sizeof(fi_map_entry));
    if (!map->buckets) {
        return -1;
    }
    
    map->bucket_count = new_bucket_count;
    map->size = 0;
    
    /* Rehash all existing entries */
    for (size_t i = 0; i < old_bucket_count; i++) {
        fi_map_entry *old_entry = &old_buckets[i];
        if (old_entry->key != NULL && !old_entry->is_deleted) {
            fi_map_put(map, old_entry->key, old_entry->value);
        }
    }
    
    free(old_buckets);
    return 0;
}

/* Create a new hash map */
fi_map* fi_map_create(size_t initial_capacity, 
                      size_t key_size, 
                      size_t value_size,
                      uint32_t (*hash_func)(const void *key, size_t key_size),
                      int (*key_compare)(const void *key1, const void *key2)) {
    return fi_map_create_with_destructors(initial_capacity, key_size, value_size, 
                                         hash_func, key_compare, NULL, NULL);
}

fi_map* fi_map_create_with_destructors(size_t initial_capacity,
                                       size_t key_size,
                                       size_t value_size,
                                       uint32_t (*hash_func)(const void *key, size_t key_size),
                                       int (*key_compare)(const void *key1, const void *key2),
                                       void (*key_free)(void *key),
                                       void (*value_free)(void *value)) {
    fi_map *map = malloc(sizeof(fi_map));
    if (!map) return NULL;
    
    map->bucket_count = fi_map_next_power_of_2(initial_capacity);
    if (map->bucket_count < 8) map->bucket_count = 8;
    
    map->buckets = calloc(map->bucket_count, sizeof(fi_map_entry));
    if (!map->buckets) {
        free(map);
        return NULL;
    }
    
    map->size = 0;
    map->key_size = key_size;
    map->value_size = value_size;
    map->hash_func = hash_func;
    map->key_compare = key_compare;
    map->key_free = key_free;
    map->value_free = value_free;
    map->load_factor_threshold = 75; /* 75% load factor */
    
    return map;
}

/* Destroy hash map */
void fi_map_destroy(fi_map *map) {
    if (!map) return;
    
    fi_map_clear(map);
    free(map->buckets);
    free(map);
}

/* Clear all entries */
void fi_map_clear(fi_map *map) {
    if (!map) return;
    
    for (size_t i = 0; i < map->bucket_count; i++) {
        fi_map_entry *entry = &map->buckets[i];
        if (entry->key != NULL) {
            if (map->key_free) {
                map->key_free(entry->key);
            } else {
                free(entry->key);
            }
            if (map->value_free) {
                map->value_free(entry->value);
            } else {
                free(entry->value);
            }
            entry->key = NULL;
            entry->value = NULL;
        }
    }
    
    map->size = 0;
}

/* Put key-value pair into map */
int fi_map_put(fi_map *map, const void *key, const void *value) {
    if (!map || !key || !value) return -1;
    
    /* Check if we need to resize */
    if (map->size * 100 / map->bucket_count >= map->load_factor_threshold) {
        if (fi_map_resize_internal(map, map->bucket_count * 2) != 0) {
            return -1;
        }
    }
    
    uint32_t hash = map->hash_func(key, map->key_size);
    fi_map_entry *existing = fi_map_find_entry(map, key, hash);
    
    if (existing) {
        /* Update existing entry */
        if (map->value_free) {
            map->value_free(existing->value);
        } else {
            free(existing->value);
        }
        
        existing->value = malloc(map->value_size);
        if (!existing->value) return -1;
        memcpy(existing->value, value, map->value_size);
        return 0;
    }
    
    /* Insert new entry using Robin Hood hashing */
    size_t bucket = fi_map_bucket_index(map, hash);
    uint32_t distance = 0;
    
    void *new_key = malloc(map->key_size);
    void *new_value = malloc(map->value_size);
    if (!new_key || !new_value) {
        free(new_key);
        free(new_value);
        return -1;
    }
    
    memcpy(new_key, key, map->key_size);
    memcpy(new_value, value, map->value_size);
    
    while (true) {
        fi_map_entry *entry = &map->buckets[bucket];
        
        if (entry->key == NULL) {
            /* Empty bucket, insert here */
            entry->key = new_key;
            entry->value = new_value;
            entry->hash = hash;
            entry->distance = distance;
            entry->is_deleted = false;
            map->size++;
            return 0;
        }
        
        /* Robin Hood: if current entry's distance is less than ours, swap */
        if (entry->distance < distance) {
            /* Swap entries */
            void *temp_key = entry->key;
            void *temp_value = entry->value;
            uint32_t temp_hash = entry->hash;
            uint32_t temp_distance = entry->distance;
            
            entry->key = new_key;
            entry->value = new_value;
            entry->hash = hash;
            entry->distance = distance;
            
            new_key = temp_key;
            new_value = temp_value;
            hash = temp_hash;
            distance = temp_distance;
        }
        
        bucket = (bucket + 1) & (map->bucket_count - 1);
        distance++;
    }
}

/* Get value by key */
int fi_map_get(const fi_map *map, const void *key, void *value) {
    if (!map || !key || !value) return -1;
    
    uint32_t hash = map->hash_func(key, map->key_size);
    fi_map_entry *entry = fi_map_find_entry(map, key, hash);
    
    if (entry) {
        memcpy(value, entry->value, map->value_size);
        return 0;
    }
    
    return -1;
}

/* Remove key from map */
int fi_map_remove(fi_map *map, const void *key) {
    if (!map || !key) return -1;
    
    uint32_t hash = map->hash_func(key, map->key_size);
    fi_map_entry *entry = fi_map_find_entry(map, key, hash);
    
    if (entry) {
        if (map->key_free) {
            map->key_free(entry->key);
        } else {
            free(entry->key);
        }
        if (map->value_free) {
            map->value_free(entry->value);
        } else {
            free(entry->value);
        }
        
        entry->key = NULL;
        entry->value = NULL;
        entry->is_deleted = true;
        map->size--;
        return 0;
    }
    
    return -1;
}

/* Check if key exists */
bool fi_map_contains(const fi_map *map, const void *key) {
    if (!map || !key) return false;
    
    uint32_t hash = map->hash_func(key, map->key_size);
    return fi_map_find_entry(map, key, hash) != NULL;
}

/* Check if map is empty */
bool fi_map_empty(const fi_map *map) {
    return map ? map->size == 0 : true;
}

/* Get map size */
size_t fi_map_size(const fi_map *map) {
    return map ? map->size : 0;
}

/* Built-in hash functions */
uint32_t fi_map_hash_string(const void *key, size_t key_size) {
    (void)key_size; /* Suppress unused parameter warning */
    return fi_map_xxhash32(*(const char**)key, strlen(*(const char**)key), 0);
}

uint32_t fi_map_hash_int32(const void *key, size_t key_size) {
    (void)key_size; /* Suppress unused parameter warning */
    return fi_map_xxhash32(key, sizeof(int32_t), 0);
}

uint32_t fi_map_hash_int64(const void *key, size_t key_size) {
    (void)key_size; /* Suppress unused parameter warning */
    return fi_map_xxhash32(key, sizeof(int64_t), 0);
}

uint32_t fi_map_hash_ptr(const void *key, size_t key_size) {
    (void)key_size; /* Suppress unused parameter warning */
    return fi_map_xxhash32(key, sizeof(void*), 0);
}

uint32_t fi_map_hash_bytes(const void *key, size_t key_size) {
    return fi_map_xxhash32(key, key_size, 0);
}

/* Built-in comparison functions */
int fi_map_compare_string(const void *key1, const void *key2) {
    return strcmp(*(const char**)key1, *(const char**)key2);
}

int fi_map_compare_int32(const void *key1, const void *key2) {
    int32_t a = *(const int32_t*)key1;
    int32_t b = *(const int32_t*)key2;
    return (a > b) - (a < b);
}

int fi_map_compare_int64(const void *key1, const void *key2) {
    int64_t a = *(const int64_t*)key1;
    int64_t b = *(const int64_t*)key2;
    return (a > b) - (a < b);
}

int fi_map_compare_ptr(const void *key1, const void *key2) {
    void *ptr1 = *(void**)key1;
    void *ptr2 = *(void**)key2;
    if (ptr1 < ptr2) return -1;
    if (ptr1 > ptr2) return 1;
    return 0;
}

int fi_map_compare_bytes(const void *key1, const void *key2) {
    return memcmp(key1, key2, sizeof(void*));
}

/* Specialized map creation functions */
fi_map* fi_map_create_string_string(size_t initial_capacity) {
    return fi_map_create_with_destructors(initial_capacity, sizeof(char*), sizeof(char*),
                                         fi_map_hash_string, fi_map_compare_string,
                                         free, free);
}

fi_map* fi_map_create_string_ptr(size_t initial_capacity) {
    return fi_map_create_with_destructors(initial_capacity, sizeof(char*), sizeof(void*),
                                         fi_map_hash_string, fi_map_compare_string,
                                         free, NULL);
}

fi_map* fi_map_create_int32_ptr(size_t initial_capacity) {
    return fi_map_create(initial_capacity, sizeof(int32_t), sizeof(void*),
                        fi_map_hash_int32, fi_map_compare_int32);
}

fi_map* fi_map_create_int64_ptr(size_t initial_capacity) {
    return fi_map_create(initial_capacity, sizeof(int64_t), sizeof(void*),
                        fi_map_hash_int64, fi_map_compare_int64);
}

fi_map* fi_map_create_ptr_ptr(size_t initial_capacity) {
    return fi_map_create(initial_capacity, sizeof(void*), sizeof(void*),
                        fi_map_hash_ptr, fi_map_compare_ptr);
}

/* Load factor calculation */
double fi_map_load_factor(const fi_map *map) {
    if (!map || map->bucket_count == 0) return 0.0;
    return (double)map->size / map->bucket_count * 100.0;
}

/* Resize map */
void fi_map_resize(fi_map *map, size_t new_capacity) {
    if (!map) return;
    fi_map_resize_internal(map, new_capacity);
}

/* Put if absent */
int fi_map_put_if_absent(fi_map *map, const void *key, const void *value) {
    if (!map || !key || !value) return -1;
    
    uint32_t hash = map->hash_func(key, map->key_size);
    if (fi_map_find_entry(map, key, hash)) {
        return 1; /* Key already exists */
    }
    
    return fi_map_put(map, key, value);
}

/* Replace existing value */
int fi_map_replace(fi_map *map, const void *key, const void *value) {
    if (!map || !key || !value) return -1;
    
    uint32_t hash = map->hash_func(key, map->key_size);
    fi_map_entry *entry = fi_map_find_entry(map, key, hash);
    
    if (!entry) {
        return 1; /* Key doesn't exist */
    }
    
    if (map->value_free) {
        map->value_free(entry->value);
    } else {
        free(entry->value);
    }
    
    entry->value = malloc(map->value_size);
    if (!entry->value) return -1;
    memcpy(entry->value, value, map->value_size);
    return 0;
}

/* Get or default */
int fi_map_get_or_default(const fi_map *map, const void *key, void *value, const void *default_value) {
    if (!map || !key || !value) return -1;
    
    if (fi_map_get(map, key, value) == 0) {
        return 0; /* Key found */
    }
    
    /* Key not found, use default */
    if (default_value) {
        memcpy(value, default_value, map->value_size);
    }
    return 1; /* Used default */
}

/* Merge two maps */
int fi_map_merge(fi_map *dest, const fi_map *src) {
    if (!dest || !src) return -1;
    
    fi_map_iterator iter = fi_map_iterator_create((fi_map*)src);
    
    /* Handle first element if iterator is valid */
    if (iter.is_valid) {
        if (fi_map_put(dest, fi_map_iterator_key(&iter), fi_map_iterator_value(&iter)) != 0) {
            fi_map_iterator_destroy(&iter);
            return -1;
        }
    }
    
    /* Handle remaining elements */
    while (fi_map_iterator_next(&iter)) {
        if (fi_map_put(dest, fi_map_iterator_key(&iter), fi_map_iterator_value(&iter)) != 0) {
            fi_map_iterator_destroy(&iter);
            return -1;
        }
    }
    fi_map_iterator_destroy(&iter);
    return 0;
}

/* Iterator implementation */
fi_map_iterator fi_map_iterator_create(fi_map *map) {
    fi_map_iterator iter = {0};
    iter.map = map;
    iter.current_bucket = 0;
    iter.current_entry = NULL;
    iter.is_valid = false;
    
    /* Find first valid entry */
    if (map) {
        while (iter.current_bucket < map->bucket_count) {
            fi_map_entry *entry = &map->buckets[iter.current_bucket];
            if (entry->key != NULL && !entry->is_deleted) {
                iter.current_entry = entry;
                iter.is_valid = true;
                break;
            }
            iter.current_bucket++;
        }
    }
    
    return iter;
}

bool fi_map_iterator_next(fi_map_iterator *iter) {
    if (!iter || !iter->map || !iter->is_valid) {
        return false;
    }
    
    /* Move to next bucket */
    iter->current_bucket++;
    
    /* Find next valid entry */
    while (iter->current_bucket < iter->map->bucket_count) {
        fi_map_entry *entry = &iter->map->buckets[iter->current_bucket];
        if (entry->key != NULL && !entry->is_deleted) {
            iter->current_entry = entry;
            return true;
        }
        iter->current_bucket++;
    }
    
    /* No more entries */
    iter->is_valid = false;
    iter->current_entry = NULL;
    return false;
}

bool fi_map_iterator_has_next(const fi_map_iterator *iter) {
    if (!iter || !iter->map || !iter->is_valid) {
        return false;
    }
    
    /* Check if there are more valid entries after current position */
    for (size_t i = iter->current_bucket + 1; i < iter->map->bucket_count; i++) {
        fi_map_entry *entry = &iter->map->buckets[i];
        if (entry->key != NULL && !entry->is_deleted) {
            return true;
        }
    }
    
    return false;
}

void* fi_map_iterator_key(const fi_map_iterator *iter) {
    if (!iter || !iter->is_valid || !iter->current_entry) {
        return NULL;
    }
    return iter->current_entry->key;
}

void* fi_map_iterator_value(const fi_map_iterator *iter) {
    if (!iter || !iter->is_valid || !iter->current_entry) {
        return NULL;
    }
    return iter->current_entry->value;
}

void fi_map_iterator_destroy(fi_map_iterator *iter) {
    if (iter) {
        iter->is_valid = false;
        iter->current_entry = NULL;
    }
}

/* For each operation */
void fi_map_for_each(fi_map *map, fi_map_visit_func visit, void *user_data) {
    if (!map || !visit) return;
    
    fi_map_iterator iter = fi_map_iterator_create(map);
    
    /* Handle first element if iterator is valid */
    if (iter.is_valid) {
        visit(fi_map_iterator_key(&iter), fi_map_iterator_value(&iter), user_data);
    }
    
    /* Handle remaining elements */
    while (fi_map_iterator_next(&iter)) {
        visit(fi_map_iterator_key(&iter), fi_map_iterator_value(&iter), user_data);
    }
    fi_map_iterator_destroy(&iter);
}

/* Filter operation */
fi_map* fi_map_filter(fi_map *map, fi_map_callback_func callback, void *user_data) {
    if (!map || !callback) return NULL;
    
    fi_map *filtered = fi_map_create(map->bucket_count, map->key_size, map->value_size,
                                   map->hash_func, map->key_compare);
    if (!filtered) return NULL;
    
    filtered->key_free = map->key_free;
    filtered->value_free = map->value_free;
    
    fi_map_iterator iter = fi_map_iterator_create(map);
    
    /* Handle the first element if iterator is valid */
    if (iter.is_valid) {
        if (callback(fi_map_iterator_key(&iter), fi_map_iterator_value(&iter), user_data)) {
            fi_map_put(filtered, fi_map_iterator_key(&iter), fi_map_iterator_value(&iter));
        }
    }
    
    /* Handle remaining elements */
    while (fi_map_iterator_next(&iter)) {
        if (callback(fi_map_iterator_key(&iter), fi_map_iterator_value(&iter), user_data)) {
            fi_map_put(filtered, fi_map_iterator_key(&iter), fi_map_iterator_value(&iter));
        }
    }
    fi_map_iterator_destroy(&iter);
    
    return filtered;
}

/* Any operation */
bool fi_map_any(fi_map *map, fi_map_callback_func callback, void *user_data) {
    if (!map || !callback) return false;
    
    fi_map_iterator iter = fi_map_iterator_create(map);
    
    /* Check first element if iterator is valid */
    if (iter.is_valid) {
        if (callback(fi_map_iterator_key(&iter), fi_map_iterator_value(&iter), user_data)) {
            fi_map_iterator_destroy(&iter);
            return true;
        }
    }
    
    /* Check remaining elements */
    while (fi_map_iterator_next(&iter)) {
        if (callback(fi_map_iterator_key(&iter), fi_map_iterator_value(&iter), user_data)) {
            fi_map_iterator_destroy(&iter);
            return true;
        }
    }
    fi_map_iterator_destroy(&iter);
    return false;
}

/* All operation */
bool fi_map_all(fi_map *map, fi_map_callback_func callback, void *user_data) {
    if (!map || !callback) return true;
    
    fi_map_iterator iter = fi_map_iterator_create(map);
    
    /* Check first element if iterator is valid */
    if (iter.is_valid) {
        if (!callback(fi_map_iterator_key(&iter), fi_map_iterator_value(&iter), user_data)) {
            fi_map_iterator_destroy(&iter);
            return false;
        }
    }
    
    /* Check remaining elements */
    while (fi_map_iterator_next(&iter)) {
        if (!callback(fi_map_iterator_key(&iter), fi_map_iterator_value(&iter), user_data)) {
            fi_map_iterator_destroy(&iter);
            return false;
        }
    }
    fi_map_iterator_destroy(&iter);
    return true;
}

/* Get keys array */
fi_array* fi_map_keys(const fi_map *map) {
    if (!map) return NULL;
    
    fi_array *keys = fi_array_create(map->size, map->key_size);
    if (!keys) return NULL;
    
    fi_map_iterator iter = fi_map_iterator_create((fi_map*)map);
    
    /* Handle first element if iterator is valid */
    if (iter.is_valid) {
        fi_array_push(keys, fi_map_iterator_key(&iter));
    }
    
    /* Handle remaining elements */
    while (fi_map_iterator_next(&iter)) {
        fi_array_push(keys, fi_map_iterator_key(&iter));
    }
    fi_map_iterator_destroy(&iter);
    
    return keys;
}

/* Get values array */
fi_array* fi_map_values(const fi_map *map) {
    if (!map) return NULL;
    
    fi_array *values = fi_array_create(map->size, map->value_size);
    if (!values) return NULL;
    
    fi_map_iterator iter = fi_map_iterator_create((fi_map*)map);
    
    /* Handle first element if iterator is valid */
    if (iter.is_valid) {
        fi_array_push(values, fi_map_iterator_value(&iter));
    }
    
    /* Handle remaining elements */
    while (fi_map_iterator_next(&iter)) {
        fi_array_push(values, fi_map_iterator_value(&iter));
    }
    fi_map_iterator_destroy(&iter);
    
    return values;
}

/* Get entries array */
fi_array* fi_map_entries(const fi_map *map) {
    if (!map) return NULL;
    
    /* Create array of key-value pairs */
    typedef struct {
        void *key;
        void *value;
    } map_entry_pair;
    
    fi_array *entries = fi_array_create(map->size, sizeof(map_entry_pair));
    if (!entries) return NULL;
    
    fi_map_iterator iter = fi_map_iterator_create((fi_map*)map);
    
    /* Handle first element if iterator is valid */
    if (iter.is_valid) {
        map_entry_pair pair = {
            .key = fi_map_iterator_key(&iter),
            .value = fi_map_iterator_value(&iter)
        };
        fi_array_push(entries, &pair);
    }
    
    /* Handle remaining elements */
    while (fi_map_iterator_next(&iter)) {
        map_entry_pair pair = {
            .key = fi_map_iterator_key(&iter),
            .value = fi_map_iterator_value(&iter)
        };
        fi_array_push(entries, &pair);
    }
    fi_map_iterator_destroy(&iter);
    
    return entries;
}

/* Statistics functions */
void fi_map_print_stats(const fi_map *map) {
    if (!map) {
        printf("Map is NULL\n");
        return;
    }
    
    printf("Map Statistics:\n");
    printf("  Size: %zu\n", map->size);
    printf("  Buckets: %zu\n", map->bucket_count);
    printf("  Load Factor: %.2f%%\n", fi_map_load_factor(map));
    printf("  Max Probe Distance: %zu\n", fi_map_max_probe_distance(map));
    printf("  Average Probe Distance: %.2f\n", fi_map_average_probe_distance(map));
}

size_t fi_map_max_probe_distance(const fi_map *map) {
    if (!map) return 0;
    
    size_t max_distance = 0;
    for (size_t i = 0; i < map->bucket_count; i++) {
        fi_map_entry *entry = &map->buckets[i];
        if (entry->key != NULL && !entry->is_deleted) {
            if (entry->distance > max_distance) {
                max_distance = entry->distance;
            }
        }
    }
    return max_distance;
}

double fi_map_average_probe_distance(const fi_map *map) {
    if (!map || map->size == 0) return 0.0;
    
    size_t total_distance = 0;
    for (size_t i = 0; i < map->bucket_count; i++) {
        fi_map_entry *entry = &map->buckets[i];
        if (entry->key != NULL && !entry->is_deleted) {
            total_distance += entry->distance;
        }
    }
    
    return (double)total_distance / map->size;
}
