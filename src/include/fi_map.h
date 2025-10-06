#ifndef __FI_MAP_H__
#define __FI_MAP_H__

#include "fi.h"

/* Hash map entry structure */
typedef struct fi_map_entry {
    void *key;              /* Key data */
    void *value;            /* Value data */
    uint32_t hash;          /* Cached hash value */
    uint32_t distance;      /* Distance from ideal position (Robin Hood hashing) */
    bool is_deleted;        /* Tombstone flag for deleted entries */
} fi_map_entry;

/* Hash map structure */
typedef struct fi_map {
    fi_map_entry *buckets;  /* Array of hash buckets */
    size_t bucket_count;    /* Number of buckets (always power of 2) */
    size_t size;            /* Current number of elements */
    size_t key_size;        /* Size of key in bytes */
    size_t value_size;      /* Size of value in bytes */
    uint32_t (*hash_func)(const void *key, size_t key_size); /* Hash function */
    int (*key_compare)(const void *key1, const void *key2);  /* Key comparison function */
    void (*key_free)(void *key);     /* Key destructor function */
    void (*value_free)(void *value); /* Value destructor function */
    size_t load_factor_threshold;    /* Load factor threshold for resizing */
} fi_map;

/* Hash map creation and destruction */
fi_map* fi_map_create(size_t initial_capacity, 
                      size_t key_size, 
                      size_t value_size,
                      uint32_t (*hash_func)(const void *key, size_t key_size),
                      int (*key_compare)(const void *key1, const void *key2));
fi_map* fi_map_create_with_destructors(size_t initial_capacity,
                                       size_t key_size,
                                       size_t value_size,
                                       uint32_t (*hash_func)(const void *key, size_t key_size),
                                       int (*key_compare)(const void *key1, const void *key2),
                                       void (*key_free)(void *key),
                                       void (*value_free)(void *value));
void fi_map_destroy(fi_map *map);
void fi_map_clear(fi_map *map);

/* Basic operations */
int fi_map_put(fi_map *map, const void *key, const void *value);
int fi_map_get(const fi_map *map, const void *key, void *value);
int fi_map_remove(fi_map *map, const void *key);
bool fi_map_contains(const fi_map *map, const void *key);
bool fi_map_empty(const fi_map *map);
size_t fi_map_size(const fi_map *map);

/* Advanced operations */
int fi_map_put_if_absent(fi_map *map, const void *key, const void *value);
int fi_map_replace(fi_map *map, const void *key, const void *value);
int fi_map_get_or_default(const fi_map *map, const void *key, void *value, const void *default_value);
int fi_map_merge(fi_map *dest, const fi_map *src);

/* Iteration */
typedef struct fi_map_iterator {
    fi_map *map;
    size_t current_bucket;
    fi_map_entry *current_entry;
    bool is_valid;
} fi_map_iterator;

fi_map_iterator fi_map_iterator_create(fi_map *map);
bool fi_map_iterator_next(fi_map_iterator *iter);
bool fi_map_iterator_has_next(const fi_map_iterator *iter);
void* fi_map_iterator_key(const fi_map_iterator *iter);
void* fi_map_iterator_value(const fi_map_iterator *iter);
void fi_map_iterator_destroy(fi_map_iterator *iter);

/* Callback operations */
typedef bool (*fi_map_callback_func)(const void *key, const void *value, void *user_data);
typedef void (*fi_map_visit_func)(const void *key, const void *value, void *user_data);

void fi_map_for_each(fi_map *map, fi_map_visit_func visit, void *user_data);
fi_map* fi_map_filter(fi_map *map, fi_map_callback_func callback, void *user_data);
bool fi_map_any(fi_map *map, fi_map_callback_func callback, void *user_data);
bool fi_map_all(fi_map *map, fi_map_callback_func callback, void *user_data);

/* Utility functions */
fi_array* fi_map_keys(const fi_map *map);
fi_array* fi_map_values(const fi_map *map);
fi_array* fi_map_entries(const fi_map *map);
double fi_map_load_factor(const fi_map *map);
void fi_map_resize(fi_map *map, size_t new_capacity);

/* Built-in hash functions */
uint32_t fi_map_hash_string(const void *key, size_t key_size);
uint32_t fi_map_hash_int32(const void *key, size_t key_size);
uint32_t fi_map_hash_int64(const void *key, size_t key_size);
uint32_t fi_map_hash_ptr(const void *key, size_t key_size);
uint32_t fi_map_hash_bytes(const void *key, size_t key_size);

/* Built-in comparison functions */
int fi_map_compare_string(const void *key1, const void *key2);
int fi_map_compare_int32(const void *key1, const void *key2);
int fi_map_compare_int64(const void *key1, const void *key2);
int fi_map_compare_ptr(const void *key1, const void *key2);
int fi_map_compare_bytes(const void *key1, const void *key2);

/* Specialized map creation functions */
fi_map* fi_map_create_string_string(size_t initial_capacity);
fi_map* fi_map_create_string_ptr(size_t initial_capacity);
fi_map* fi_map_create_int32_ptr(size_t initial_capacity);
fi_map* fi_map_create_int64_ptr(size_t initial_capacity);
fi_map* fi_map_create_ptr_ptr(size_t initial_capacity);

/* Statistics and debugging */
void fi_map_print_stats(const fi_map *map);
size_t fi_map_max_probe_distance(const fi_map *map);
double fi_map_average_probe_distance(const fi_map *map);

#endif //__FI_MAP_H__
