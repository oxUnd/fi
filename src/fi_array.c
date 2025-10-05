#include "fi.h"
#include <time.h>
#include <stdint.h>

/* Internal helper functions */
static int fi_array_resize(fi_array *arr, size_t new_capacity) {
    if (new_capacity < arr->size) {
        return -1; /* Cannot shrink below current size */
    }
    
    void **new_data = realloc(arr->data, new_capacity * sizeof(void*));
    if (!new_data) {
        return -1; /* Memory allocation failed */
    }
    
    arr->data = new_data;
    arr->capacity = new_capacity;
    return 0;
}

static int fi_array_ensure_capacity(fi_array *arr, size_t required_capacity) {
    if (required_capacity <= arr->capacity) {
        return 0; /* Already has enough capacity */
    }
    
    size_t new_capacity = arr->capacity * 2;
    if (new_capacity < required_capacity) {
        new_capacity = required_capacity;
    }
    
    return fi_array_resize(arr, new_capacity);
}

/* Basic array operations */
fi_array* fi_array_create(size_t initial_capacity, size_t element_size) {
    fi_array *arr = malloc(sizeof(fi_array));
    if (!arr) return NULL;
    
    arr->capacity = initial_capacity > 0 ? initial_capacity : 8;
    arr->size = 0;
    arr->element_size = element_size;
    arr->data = calloc(arr->capacity, sizeof(void*));
    
    if (!arr->data) {
        free(arr);
        return NULL;
    }
    
    return arr;
}

void fi_array_destroy(fi_array *arr) {
    if (!arr) return;
    
    if (arr->data) {
        for (size_t i = 0; i < arr->size; i++) {
            if (arr->data[i]) {
                free(arr->data[i]);
            }
        }
        free(arr->data);
    }
    free(arr);
}

void fi_array_free(fi_array *arr) {
    fi_array_destroy(arr);
}

fi_array* fi_array_copy(const fi_array *arr) {
    if (!arr) return NULL;
    
    fi_array *copy = fi_array_create(arr->capacity, arr->element_size);
    if (!copy) return NULL;
    
    for (size_t i = 0; i < arr->size; i++) {
        if (arr->data[i]) {
            void *element_copy = malloc(arr->element_size);
            if (!element_copy) {
                fi_array_destroy(copy);
                return NULL;
            }
            memcpy(element_copy, arr->data[i], arr->element_size);
            copy->data[i] = element_copy;
        }
        copy->size++;
    }
    
    return copy;
}

fi_array* fi_array_slice(const fi_array *arr, size_t offset, size_t length) {
    if (!arr || offset >= arr->size) return NULL;
    
    size_t actual_length = length;
    if (offset + length > arr->size) {
        actual_length = arr->size - offset;
    }
    
    fi_array *slice = fi_array_create(actual_length, arr->element_size);
    if (!slice) return NULL;
    
    for (size_t i = 0; i < actual_length; i++) {
        if (arr->data[offset + i]) {
            void *element_copy = malloc(arr->element_size);
            if (!element_copy) {
                fi_array_destroy(slice);
                return NULL;
            }
            memcpy(element_copy, arr->data[offset + i], arr->element_size);
            slice->data[i] = element_copy;
        }
        slice->size++;
    }
    
    return slice;
}

/* Element access */
void* fi_array_get(const fi_array *arr, size_t index) {
    if (!arr || index >= arr->size) return NULL;
    return arr->data[index];
}

void fi_array_set(fi_array *arr, size_t index, const void *value) {
    if (!arr || index >= arr->size) return;
    
    if (arr->data[index]) {
        free(arr->data[index]);
    }
    
    if (value) {
        arr->data[index] = malloc(arr->element_size);
        if (arr->data[index]) {
            memcpy(arr->data[index], value, arr->element_size);
        }
    } else {
        arr->data[index] = NULL;
    }
}

bool fi_array_key_exists(const fi_array *arr, size_t index) {
    return arr && index < arr->size;
}

size_t fi_array_count(const fi_array *arr) {
    return arr ? arr->size : 0;
}

bool fi_array_empty(const fi_array *arr) {
    return !arr || arr->size == 0;
}

/* Stack operations */
int fi_array_push(fi_array *arr, const void *value) {
    if (!arr) return -1;
    
    if (fi_array_ensure_capacity(arr, arr->size + 1) != 0) {
        return -1;
    }
    
    if (value) {
        arr->data[arr->size] = malloc(arr->element_size);
        if (!arr->data[arr->size]) return -1;
        memcpy(arr->data[arr->size], value, arr->element_size);
    } else {
        arr->data[arr->size] = NULL;
    }
    
    arr->size++;
    return 0;
}

int fi_array_pop(fi_array *arr, void *value) {
    if (!arr || arr->size == 0) return -1;
    
    arr->size--;
    if (value && arr->data[arr->size]) {
        memcpy(value, arr->data[arr->size], arr->element_size);
    }
    
    if (arr->data[arr->size]) {
        free(arr->data[arr->size]);
        arr->data[arr->size] = NULL;
    }
    
    return 0;
}

int fi_array_unshift(fi_array *arr, const void *value) {
    if (!arr) return -1;
    
    if (fi_array_ensure_capacity(arr, arr->size + 1) != 0) {
        return -1;
    }
    
    /* Shift existing elements to the right */
    for (size_t i = arr->size; i > 0; i--) {
        arr->data[i] = arr->data[i-1];
    }
    
    if (value) {
        arr->data[0] = malloc(arr->element_size);
        if (!arr->data[0]) return -1;
        memcpy(arr->data[0], value, arr->element_size);
    } else {
        arr->data[0] = NULL;
    }
    
    arr->size++;
    return 0;
}

int fi_array_shift(fi_array *arr, void *value) {
    if (!arr || arr->size == 0) return -1;
    
    if (value && arr->data[0]) {
        memcpy(value, arr->data[0], arr->element_size);
    }
    
    if (arr->data[0]) {
        free(arr->data[0]);
    }
    
    /* Shift remaining elements to the left */
    for (size_t i = 0; i < arr->size - 1; i++) {
        arr->data[i] = arr->data[i+1];
    }
    arr->data[arr->size-1] = NULL;
    
    arr->size--;
    return 0;
}

/* Array manipulation */
int fi_array_merge(fi_array *dest, const fi_array *src) {
    if (!dest || !src) return -1;
    
    for (size_t i = 0; i < src->size; i++) {
        if (fi_array_push(dest, src->data[i]) != 0) {
            return -1;
        }
    }
    
    return 0;
}

int fi_array_splice(fi_array *arr, size_t offset, size_t length, const void *replacement) {
    if (!arr || offset >= arr->size) return -1;
    
    size_t actual_length = length;
    if (offset + length > arr->size) {
        actual_length = arr->size - offset;
    }
    
    /* Remove elements */
    for (size_t i = offset; i < offset + actual_length; i++) {
        if (arr->data[i]) {
            free(arr->data[i]);
        }
    }
    
    /* Shift remaining elements */
    for (size_t i = offset; i < arr->size - actual_length; i++) {
        arr->data[i] = arr->data[i + actual_length];
    }
    
    arr->size -= actual_length;
    
    /* Insert replacement if provided */
    if (replacement) {
        if (fi_array_ensure_capacity(arr, arr->size + 1) != 0) {
            return -1;
        }
        
        /* Shift elements to make room */
        for (size_t i = arr->size; i > offset; i--) {
            arr->data[i] = arr->data[i-1];
        }
        
        arr->data[offset] = malloc(arr->element_size);
        if (!arr->data[offset]) return -1;
        memcpy(arr->data[offset], replacement, arr->element_size);
        arr->size++;
    }
    
    return 0;
}

int fi_array_pad(fi_array *arr, size_t size, const void *value) {
    if (!arr || size <= arr->size) return 0;
    
    if (fi_array_ensure_capacity(arr, size) != 0) {
        return -1;
    }
    
    for (size_t i = arr->size; i < size; i++) {
        if (value) {
            arr->data[i] = malloc(arr->element_size);
            if (!arr->data[i]) return -1;
            memcpy(arr->data[i], value, arr->element_size);
        } else {
            arr->data[i] = NULL;
        }
    }
    
    arr->size = size;
    return 0;
}

int fi_array_fill(fi_array *arr, size_t start, size_t num, const void *value) {
    if (!arr || start >= arr->size) return -1;
    
    size_t end = start + num;
    if (end > arr->size) end = arr->size;
    
    for (size_t i = start; i < end; i++) {
        if (arr->data[i]) {
            free(arr->data[i]);
        }
        
        if (value) {
            arr->data[i] = malloc(arr->element_size);
            if (!arr->data[i]) return -1;
            memcpy(arr->data[i], value, arr->element_size);
        } else {
            arr->data[i] = NULL;
        }
    }
    
    return 0;
}

/* Search operations */
ssize_t fi_array_search(const fi_array *arr, const void *value) {
    if (!arr || !value) return -1;
    
    for (size_t i = 0; i < arr->size; i++) {
        if (arr->data[i] && memcmp(arr->data[i], value, arr->element_size) == 0) {
            return i;
        }
    }
    
    return -1;
}

bool fi_array_in_array(const fi_array *arr, const void *value) {
    return fi_array_search(arr, value) >= 0;
}

void* fi_array_find(const fi_array *arr, fi_array_callback_func callback, void *user_data) {
    if (!arr || !callback) return NULL;
    
    for (size_t i = 0; i < arr->size; i++) {
        if (callback(arr->data[i], i, user_data)) {
            return arr->data[i];
        }
    }
    
    return NULL;
}

size_t fi_array_find_key(const fi_array *arr, fi_array_callback_func callback, void *user_data) {
    if (!arr || !callback) return SIZE_MAX;
    
    for (size_t i = 0; i < arr->size; i++) {
        if (callback(arr->data[i], i, user_data)) {
            return i;
        }
    }
    
    return SIZE_MAX;
}

/* Callback operations */
bool fi_array_all(const fi_array *arr, fi_array_callback_func callback, void *user_data) {
    if (!arr || !callback) return false;
    
    for (size_t i = 0; i < arr->size; i++) {
        if (!callback(arr->data[i], i, user_data)) {
            return false;
        }
    }
    
    return arr->size > 0;
}

bool fi_array_any(const fi_array *arr, fi_array_callback_func callback, void *user_data) {
    if (!arr || !callback) return false;
    
    for (size_t i = 0; i < arr->size; i++) {
        if (callback(arr->data[i], i, user_data)) {
            return true;
        }
    }
    
    return false;
}

fi_array* fi_array_filter(const fi_array *arr, fi_array_callback_func callback, void *user_data) {
    if (!arr || !callback) return NULL;
    
    fi_array *filtered = fi_array_create(arr->size, arr->element_size);
    if (!filtered) return NULL;
    
    for (size_t i = 0; i < arr->size; i++) {
        if (callback(arr->data[i], i, user_data)) {
            if (fi_array_push(filtered, arr->data[i]) != 0) {
                fi_array_destroy(filtered);
                return NULL;
            }
        }
    }
    
    return filtered;
}

fi_array* fi_array_map(const fi_array *arr, fi_array_callback_func callback, void *user_data) {
    (void)callback;  /* Suppress unused parameter warning */
    (void)user_data; /* Suppress unused parameter warning */
    
    if (!arr) return NULL;
    
    fi_array *mapped = fi_array_create(arr->size, arr->element_size);
    if (!mapped) return NULL;
    
    for (size_t i = 0; i < arr->size; i++) {
        if (fi_array_push(mapped, arr->data[i]) != 0) {
            fi_array_destroy(mapped);
            return NULL;
        }
    }
    
    return mapped;
}

void* fi_array_reduce(const fi_array *arr, fi_array_callback_func callback, void *initial, void *result) {
    if (!arr || !callback || !result) return NULL;
    
    memcpy(result, initial, arr->element_size);
    
    for (size_t i = 0; i < arr->size; i++) {
        callback(arr->data[i], i, result);
    }
    
    return result;
}

void fi_array_walk(fi_array *arr, fi_array_walk_func callback, void *user_data) {
    if (!arr || !callback) return;
    
    for (size_t i = 0; i < arr->size; i++) {
        callback(arr->data[i], i, user_data);
    }
}

/* Comparison operations */
fi_array* fi_array_diff(const fi_array *arr1, const fi_array *arr2) {
    if (!arr1) return NULL;
    
    fi_array *diff = fi_array_create(arr1->size, arr1->element_size);
    if (!diff) return NULL;
    
    for (size_t i = 0; i < arr1->size; i++) {
        if (!arr2 || !fi_array_in_array(arr2, arr1->data[i])) {
            if (fi_array_push(diff, arr1->data[i]) != 0) {
                fi_array_destroy(diff);
                return NULL;
            }
        }
    }
    
    return diff;
}

fi_array* fi_array_intersect(const fi_array *arr1, const fi_array *arr2) {
    if (!arr1 || !arr2) return NULL;
    
    fi_array *intersect = fi_array_create(arr1->size, arr1->element_size);
    if (!intersect) return NULL;
    
    for (size_t i = 0; i < arr1->size; i++) {
        if (fi_array_in_array(arr2, arr1->data[i])) {
            if (fi_array_push(intersect, arr1->data[i]) != 0) {
                fi_array_destroy(intersect);
                return NULL;
            }
        }
    }
    
    return intersect;
}

fi_array* fi_array_unique(const fi_array *arr) {
    if (!arr) return NULL;
    
    fi_array *unique = fi_array_create(arr->size, arr->element_size);
    if (!unique) return NULL;
    
    for (size_t i = 0; i < arr->size; i++) {
        if (!fi_array_in_array(unique, arr->data[i])) {
            if (fi_array_push(unique, arr->data[i]) != 0) {
                fi_array_destroy(unique);
                return NULL;
            }
        }
    }
    
    return unique;
}

/* Sorting operations */
void fi_array_sort(fi_array *arr, fi_array_compare_func compare) {
    if (!arr || !compare || arr->size <= 1) return;
    
    qsort(arr->data, arr->size, sizeof(void*), compare);
}

void fi_array_reverse(fi_array *arr) {
    if (!arr || arr->size <= 1) return;
    
    for (size_t i = 0; i < arr->size / 2; i++) {
        void *temp = arr->data[i];
        arr->data[i] = arr->data[arr->size - 1 - i];
        arr->data[arr->size - 1 - i] = temp;
    }
}

void fi_array_shuffle(fi_array *arr) {
    if (!arr || arr->size <= 1) return;
    
    srand(time(NULL));
    
    for (size_t i = arr->size - 1; i > 0; i--) {
        size_t j = rand() % (i + 1);
        void *temp = arr->data[i];
        arr->data[i] = arr->data[j];
        arr->data[j] = temp;
    }
}

/* Utility functions */
fi_array* fi_array_keys(const fi_array *arr) {
    if (!arr) return NULL;
    
    fi_array *keys = fi_array_create(arr->size, sizeof(size_t));
    if (!keys) return NULL;
    
    for (size_t i = 0; i < arr->size; i++) {
        if (fi_array_push(keys, &i) != 0) {
            fi_array_destroy(keys);
            return NULL;
        }
    }
    
    return keys;
}

fi_array* fi_array_values(const fi_array *arr) {
    return fi_array_copy(arr);
}

fi_array* fi_array_flip(const fi_array *arr) {
    if (!arr) return NULL;
    
    fi_array *flipped = fi_array_create(arr->size, sizeof(size_t));
    if (!flipped) return NULL;
    
    for (size_t i = 0; i < arr->size; i++) {
        if (arr->data[i]) {
            size_t value = *(size_t*)arr->data[i];
            if (fi_array_push(flipped, &value) != 0) {
                fi_array_destroy(flipped);
                return NULL;
            }
        }
    }
    
    return flipped;
}

fi_array* fi_array_chunk(const fi_array *arr, size_t size) {
    if (!arr || size == 0) return NULL;
    
    fi_array *chunks = fi_array_create((arr->size + size - 1) / size, sizeof(fi_array*));
    if (!chunks) return NULL;
    
    for (size_t i = 0; i < arr->size; i += size) {
        fi_array *chunk = fi_array_create(size, arr->element_size);
        if (!chunk) {
            fi_array_destroy(chunks);
            return NULL;
        }
        
        size_t chunk_size = size;
        if (i + size > arr->size) {
            chunk_size = arr->size - i;
        }
        
        for (size_t j = 0; j < chunk_size; j++) {
            if (fi_array_push(chunk, arr->data[i + j]) != 0) {
                fi_array_destroy(chunk);
                fi_array_destroy(chunks);
                return NULL;
            }
        }
        
        if (fi_array_push(chunks, &chunk) != 0) {
            fi_array_destroy(chunk);
            fi_array_destroy(chunks);
            return NULL;
        }
    }
    
    return chunks;
}

fi_array* fi_array_combine(const fi_array *keys, const fi_array *values) {
    if (!keys || !values || keys->size != values->size) return NULL;
    
    fi_array *combined = fi_array_create(keys->size, sizeof(size_t) + values->element_size);
    if (!combined) return NULL;
    
    for (size_t i = 0; i < keys->size; i++) {
        if (fi_array_push(combined, values->data[i]) != 0) {
            fi_array_destroy(combined);
            return NULL;
        }
    }
    
    return combined;
}

fi_array* fi_array_rand(const fi_array *arr, size_t num) {
    if (!arr || num == 0) return NULL;
    
    fi_array *random = fi_array_create(num, arr->element_size);
    if (!random) return NULL;
    
    srand(time(NULL));
    
    for (size_t i = 0; i < num && i < arr->size; i++) {
        size_t index = rand() % arr->size;
        if (fi_array_push(random, arr->data[index]) != 0) {
            fi_array_destroy(random);
            return NULL;
        }
    }
    
    return random;
}

/* Mathematical operations */
double fi_array_sum(const fi_array *arr) {
    if (!arr) return 0.0;
    
    double sum = 0.0;
    for (size_t i = 0; i < arr->size; i++) {
        if (arr->data[i]) {
            sum += *(double*)arr->data[i];
        }
    }
    
    return sum;
}

double fi_array_product(const fi_array *arr) {
    if (!arr) return 0.0;
    
    double product = 1.0;
    for (size_t i = 0; i < arr->size; i++) {
        if (arr->data[i]) {
            product *= *(double*)arr->data[i];
        }
    }
    
    return product;
}

/* Iterator operations */
static size_t current_index = 0;

void* fi_array_current(const fi_array *arr) {
    if (!arr || current_index >= arr->size) return NULL;
    return arr->data[current_index];
}

size_t fi_array_key(const fi_array *arr) {
    if (!arr || current_index >= arr->size) return SIZE_MAX;
    return current_index;
}

void* fi_array_next(fi_array *arr) {
    if (!arr) return NULL;
    
    current_index++;
    if (current_index >= arr->size) {
        current_index = arr->size;
        return NULL;
    }
    
    return arr->data[current_index];
}

void* fi_array_prev(fi_array *arr) {
    if (!arr || current_index == 0) return NULL;
    
    current_index--;
    return arr->data[current_index];
}

void* fi_array_reset(fi_array *arr) {
    if (!arr) return NULL;
    
    current_index = 0;
    return arr->data[0];
}

void* fi_array_end(fi_array *arr) {
    if (!arr) return NULL;
    
    current_index = arr->size - 1;
    return arr->data[current_index];
}

/* Special functions */
fi_array* fi_array_range(long start, long end, long step) {
    if (step == 0) return NULL;
    
    fi_array *range = fi_array_create(10, sizeof(long));
    if (!range) return NULL;
    
    if (step > 0) {
        for (long i = start; i < end; i += step) {
            if (fi_array_push(range, &i) != 0) {
                fi_array_destroy(range);
                return NULL;
            }
        }
    } else {
        for (long i = start; i > end; i += step) {
            if (fi_array_push(range, &i) != 0) {
                fi_array_destroy(range);
                return NULL;
            }
        }
    }
    
    return range;
}

fi_array* fi_array_compact(const fi_array *arr) {
    if (!arr) return NULL;
    
    fi_array *compact = fi_array_create(arr->size, arr->element_size);
    if (!compact) return NULL;
    
    for (size_t i = 0; i < arr->size; i++) {
        if (arr->data[i]) {
            if (fi_array_push(compact, arr->data[i]) != 0) {
                fi_array_destroy(compact);
                return NULL;
            }
        }
    }
    
    return compact;
}

void fi_array_extract(const fi_array *arr, const char *prefix) {
    /* This function would extract array elements into variables */
    /* Implementation depends on specific requirements */
    (void)arr;
    (void)prefix;
}
