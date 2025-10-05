#include <stdio.h>
#include <stdlib.h>
#include "project.h"
#include "fi.h"

void print_version(void) {
    printf("Version: %s\n", PROJECT_VERSION);
}

/* Test callback functions */
bool is_positive(void *element, size_t index, void *user_data) {
    (void)index;
    (void)user_data;
    if (!element) return false;
    int *value = (int*)element;
    return *value > 0;
}

bool is_even(void *element, size_t index, void *user_data) {
    (void)index;
    (void)user_data;
    if (!element) return false;
    int *value = (int*)element;
    return *value % 2 == 0;
}

int compare_ints(const void *a, const void *b) {
    const int *ia = (const int*)a;
    const int *ib = (const int*)b;
    return (*ia > *ib) - (*ia < *ib);
}

void print_array(fi_array *arr, const char *name) {
    printf("%s: [", name);
    for (size_t i = 0; i < fi_array_count(arr); i++) {
        int *value = (int*)fi_array_get(arr, i);
        if (value) {
            printf("%d", *value);
            if (i < fi_array_count(arr) - 1) {
                printf(", ");
            }
        }
    }
    printf("] (size: %zu)\n", fi_array_count(arr));
}

void demo_basic_operations() {
    printf("\n=== Basic Array Operations Demo ===\n");
    
    // Create array
    fi_array *arr = fi_array_create(5, sizeof(int));
    if (!arr) {
        printf("Failed to create array\n");
        return;
    }
    
    // Add elements
    int values[] = {10, 20, 30, 40, 50};
    for (int i = 0; i < 5; i++) {
        fi_array_push(arr, &values[i]);
    }
    print_array(arr, "After push");
    
    // Get element
    int *element = (int*)fi_array_get(arr, 2);
    if (element) {
        printf("Element at index 2: %d\n", *element);
    }
    
    // Set element
    int new_value = 99;
    fi_array_set(arr, 2, &new_value);
    print_array(arr, "After set index 2 to 99");
    
    // Pop element
    int popped;
    if (fi_array_pop(arr, &popped) == 0) {
        printf("Popped element: %d\n", popped);
    }
    print_array(arr, "After pop");
    
    // Unshift element
    int unshifted = 5;
    fi_array_unshift(arr, &unshifted);
    print_array(arr, "After unshift 5");
    
    fi_array_destroy(arr);
}

void demo_callback_operations() {
    printf("\n=== Callback Operations Demo ===\n");
    
    fi_array *arr = fi_array_create(10, sizeof(int));
    if (!arr) {
        printf("Failed to create array\n");
        return;
    }
    
    int values[] = {-5, 10, -3, 8, -1, 6, -2, 4, -7, 9};
    for (int i = 0; i < 10; i++) {
        fi_array_push(arr, &values[i]);
    }
    print_array(arr, "Original array");
    
    // Test all positive
    bool all_positive = fi_array_all(arr, is_positive, NULL);
    printf("All elements positive: %s\n", all_positive ? "true" : "false");
    
    // Test any positive
    bool any_positive = fi_array_any(arr, is_positive, NULL);
    printf("Any elements positive: %s\n", any_positive ? "true" : "false");
    
    // Filter positive elements
    fi_array *positive_only = fi_array_filter(arr, is_positive, NULL);
    if (positive_only) {
        print_array(positive_only, "Positive elements only");
        fi_array_destroy(positive_only);
    }
    
    // Filter even elements
    fi_array *even_only = fi_array_filter(arr, is_even, NULL);
    if (even_only) {
        print_array(even_only, "Even elements only");
        fi_array_destroy(even_only);
    }
    
    fi_array_destroy(arr);
}

void demo_sorting_operations() {
    printf("\n=== Sorting Operations Demo ===\n");
    
    fi_array *arr = fi_array_create(10, sizeof(int));
    if (!arr) {
        printf("Failed to create array\n");
        return;
    }
    
    int values[] = {64, 34, 25, 12, 22, 11, 90, 88, 76, 50};
    for (int i = 0; i < 10; i++) {
        fi_array_push(arr, &values[i]);
    }
    print_array(arr, "Original array");
    
    // Sort array
    fi_array_sort(arr, compare_ints);
    print_array(arr, "After sorting");
    
    // Reverse array
    fi_array_reverse(arr);
    print_array(arr, "After reverse");
    
    // Shuffle array
    fi_array_shuffle(arr);
    print_array(arr, "After shuffle");
    
    fi_array_destroy(arr);
}

void demo_utility_operations() {
    printf("\n=== Utility Operations Demo ===\n");
    
    fi_array *arr1 = fi_array_create(5, sizeof(int));
    fi_array *arr2 = fi_array_create(5, sizeof(int));
    
    if (!arr1 || !arr2) {
        printf("Failed to create arrays\n");
        return;
    }
    
    int values1[] = {1, 2, 3, 4, 5};
    int values2[] = {3, 4, 5, 6, 7};
    
    for (int i = 0; i < 5; i++) {
        fi_array_push(arr1, &values1[i]);
        fi_array_push(arr2, &values2[i]);
    }
    
    print_array(arr1, "Array 1");
    print_array(arr2, "Array 2");
    
    // Array difference
    fi_array *diff = fi_array_diff(arr1, arr2);
    if (diff) {
        print_array(diff, "Array 1 - Array 2 (diff)");
        fi_array_destroy(diff);
    }
    
    // Array intersection
    fi_array *intersect = fi_array_intersect(arr1, arr2);
    if (intersect) {
        print_array(intersect, "Array 1 ∩ Array 2 (intersect)");
        fi_array_destroy(intersect);
    }
    
    // Unique elements
    fi_array *unique = fi_array_unique(arr1);
    if (unique) {
        print_array(unique, "Unique elements of Array 1");
        fi_array_destroy(unique);
    }
    
    // Search for element
    int search_value = 3;
    ssize_t found_index = fi_array_search(arr1, &search_value);
    if (found_index >= 0) {
        printf("Found value %d at index %zd\n", search_value, found_index);
    }
    
    // Check if element exists
    int check_value = 10;
    bool exists = fi_array_in_array(arr1, &check_value);
    printf("Value %d exists in array: %s\n", check_value, exists ? "true" : "false");
    
    fi_array_destroy(arr1);
    fi_array_destroy(arr2);
}

void demo_range_operations() {
    printf("\n=== Range Operations Demo ===\n");
    
    // Create range 1 to 10
    fi_array *range = fi_array_range(1, 11, 1);
    if (range) {
        print_array(range, "Range 1-10");
        fi_array_destroy(range);
    }
    
    // Create range 10 to 1
    range = fi_array_range(10, 0, -1);
    if (range) {
        print_array(range, "Range 10-1");
        fi_array_destroy(range);
    }
    
    // Create range 0 to 20 step 2
    range = fi_array_range(0, 21, 2);
    if (range) {
        print_array(range, "Range 0-20 step 2");
        fi_array_destroy(range);
    }
}

int main(int argc, char *argv[]) {
    (void)argc;  /* Suppress unused parameter warning */
    (void)argv;  /* Suppress unused parameter warning */
    
    printf("=== FI Array Library Demo ===\n");
    print_version();
    
    demo_basic_operations();
    demo_callback_operations();
    demo_sorting_operations();
    demo_utility_operations();
    demo_range_operations();
    
    printf("\n=== Demo Complete ===\n");
    return 0;
}
