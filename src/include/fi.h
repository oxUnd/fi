#ifndef __FI_H__
#define __FI_H__

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <stdarg.h>
#include <unistd.h>  /* for ssize_t */

/* Array data structure */
typedef struct fi_array {
    void **data;        /* Array of void pointers to hold any data type */
    size_t size;        /* Current number of elements */
    size_t capacity;    /* Maximum capacity before reallocation */
    size_t element_size; /* Size of each element in bytes */
} fi_array;

/* Callback function types */
typedef bool (*fi_array_callback_func)(void *element, size_t index, void *user_data);
typedef int (*fi_array_compare_func)(const void *a, const void *b);
typedef void (*fi_array_walk_func)(void *element, size_t index, void *user_data);

/* Basic array operations */
fi_array* fi_array_create(size_t initial_capacity, size_t element_size);
void fi_array_destroy(fi_array *arr);
void fi_array_free(fi_array *arr);
fi_array* fi_array_copy(const fi_array *arr);
fi_array* fi_array_slice(const fi_array *arr, size_t offset, size_t length);

/* Element access */
void* fi_array_get(const fi_array *arr, size_t index);
void fi_array_set(fi_array *arr, size_t index, const void *value);
bool fi_array_key_exists(const fi_array *arr, size_t index);
size_t fi_array_count(const fi_array *arr);
bool fi_array_empty(const fi_array *arr);

/* Stack operations */
int fi_array_push(fi_array *arr, const void *value);
int fi_array_pop(fi_array *arr, void *value);
int fi_array_unshift(fi_array *arr, const void *value);
int fi_array_shift(fi_array *arr, void *value);

/* Array manipulation */
int fi_array_merge(fi_array *dest, const fi_array *src);
int fi_array_splice(fi_array *arr, size_t offset, size_t length, const void *replacement);
int fi_array_pad(fi_array *arr, size_t size, const void *value);
int fi_array_fill(fi_array *arr, size_t start, size_t num, const void *value);

/* Search operations */
ssize_t fi_array_search(const fi_array *arr, const void *value);
bool fi_array_in_array(const fi_array *arr, const void *value);
void* fi_array_find(const fi_array *arr, fi_array_callback_func callback, void *user_data);
size_t fi_array_find_key(const fi_array *arr, fi_array_callback_func callback, void *user_data);

/* Callback operations */
bool fi_array_all(const fi_array *arr, fi_array_callback_func callback, void *user_data);
bool fi_array_any(const fi_array *arr, fi_array_callback_func callback, void *user_data);
fi_array* fi_array_filter(const fi_array *arr, fi_array_callback_func callback, void *user_data);
fi_array* fi_array_map(const fi_array *arr, fi_array_callback_func callback, void *user_data);
void* fi_array_reduce(const fi_array *arr, fi_array_callback_func callback, void *initial, void *result);
void fi_array_walk(fi_array *arr, fi_array_walk_func callback, void *user_data);

/* Comparison operations */
fi_array* fi_array_diff(const fi_array *arr1, const fi_array *arr2);
fi_array* fi_array_intersect(const fi_array *arr1, const fi_array *arr2);
fi_array* fi_array_unique(const fi_array *arr);

/* Sorting operations */
void fi_array_sort(fi_array *arr, fi_array_compare_func compare);
void fi_array_reverse(fi_array *arr);
void fi_array_shuffle(fi_array *arr);

/* Utility functions */
fi_array* fi_array_keys(const fi_array *arr);
fi_array* fi_array_values(const fi_array *arr);
fi_array* fi_array_flip(const fi_array *arr);
fi_array* fi_array_chunk(const fi_array *arr, size_t size);
fi_array* fi_array_combine(const fi_array *keys, const fi_array *values);
fi_array* fi_array_rand(const fi_array *arr, size_t num);

/* Mathematical operations */
double fi_array_sum(const fi_array *arr);
double fi_array_product(const fi_array *arr);

/* Iterator operations */
void* fi_array_current(const fi_array *arr);
size_t fi_array_key(const fi_array *arr);
void* fi_array_next(fi_array *arr);
void* fi_array_prev(fi_array *arr);
void* fi_array_reset(fi_array *arr);
void* fi_array_end(fi_array *arr);

/* Special functions */
fi_array* fi_array_range(long start, long end, long step);
fi_array* fi_array_compact(const fi_array *arr);
void fi_array_extract(const fi_array *arr, const char *prefix);

/* Aliases for compatibility */
#define fi_array_key_exists fi_array_key_exists
#define fi_count fi_array_count
#define fi_current fi_array_current
#define fi_key fi_array_key
#define fi_next fi_array_next
#define fi_prev fi_array_prev
#define fi_reset fi_array_reset
#define fi_end fi_array_end
#define fi_in_array fi_array_in_array
#define fi_sizeof fi_array_count
#define fi_pos fi_array_current

#endif //__FI_H__
