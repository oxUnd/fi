# fi

impl array struct and have below functions.

## functions

### array

``` 
array — Create an array
fi_array_all — Checks if all array elements satisfy a callback function
fi_array_any — Checks if at least one array element satisfies a callback function
fi_array_change_key_case — Changes the case of all keys in an array
fi_array_chunk — Split an array into chunks
fi_array_column — Return the values from a single column in the input array
fi_array_combine — Creates an array by using one array for keys and another for its values
fi_array_count_values — Counts the occurrences of each distinct value in an array
fi_array_diff — Computes the difference of arrays
fi_array_diff_assoc — Computes the difference of arrays with additional index check
fi_array_diff_key — Computes the difference of arrays using keys for comparison
fi_array_diff_uassoc — Computes the difference of arrays with additional index check which is performed by a user supplied callback function
fi_array_diff_ukey — Computes the difference of arrays using a callback function on the keys for comparison
fi_array_fill — Fill an array with values
fi_array_fill_keys — Fill an array with values, specifying keys
fi_array_filter — Filters elements of an array using a callback function
fi_array_find — Returns the first element satisfying a callback function
fi_array_find_key — Returns the key of the first element satisfying a callback function
fi_array_flip — Exchanges all keys with their associated values in an array
fi_array_intersect — Computes the intersection of arrays
fi_array_intersect_assoc — Computes the intersection of arrays with additional index check
fi_array_intersect_key — Computes the intersection of arrays using keys for comparison
fi_array_intersect_uassoc — Computes the intersection of arrays with additional index check, compares indexes by a callback function
fi_array_intersect_ukey — Computes the intersection of arrays using a callback function on the keys for comparison
fi_array_is_list — Checks whether a given array is a list
fi_array_key_exists — Checks if the given key or index exists in the array
fi_array_key_first — Gets the first key of an array
fi_array_key_last — Gets the last key of an array
fi_array_keys — Return all the keys or a subset of the keys of an array
fi_array_map — Applies the callback to the elements of the given arrays
fi_array_merge — Merge one or more arrays
fi_array_merge_recursive — Merge one or more arrays recursively
fi_array_multisort — Sort multiple or multi-dimensional arrays
fi_array_pad — Pad array to the specified length with a value
fi_array_pop — Pop the element off the end of array
fi_array_product — Calculate the product of values in an array
fi_array_push — Push one or more elements onto the end of array
fi_array_rand — Pick one or more random keys out of an array
fi_array_reduce — Iteratively reduce the array to a single value using a callback function
fi_array_replace — Replaces elements from passed arrays into the first array
fi_array_replace_recursive — Replaces elements from passed arrays into the first array recursively
fi_array_reverse — Return an array with elements in reverse order
fi_array_search — Searches the array for a given value and returns the first corresponding key if successful
fi_array_shift — Shift an element off the beginning of array
fi_array_slice — Extract a slice of the array
fi_array_splice — Remove a portion of the array and replace it with something else
fi_array_sum — Calculate the sum of values in an array
fi_array_udiff — Computes the difference of arrays by using a callback function for data comparison
fi_array_udiff_assoc — Computes the difference of arrays with additional index check, compares data by a callback function
fi_array_udiff_uassoc — Computes the difference of arrays with additional index check, compares data and indexes by a callback function
fi_array_uintersect — Computes the intersection of arrays, compares data by a callback function
fi_array_uintersect_assoc — Computes the intersection of arrays with additional index check, compares data by a callback function
fi_array_uintersect_uassoc — Computes the intersection of arrays with additional index check, compares data and indexes by separate callback functions
fi_array_unique — Removes duplicate values from an array
fi_array_unshift — Prepend one or more elements to the beginning of an array
fi_array_values — Return all the values of an array
fi_array_walk — Apply a user supplied function to every member of an array
fi_array_walk_recursive — Apply a user function recursively to every member of an array
fi_arsort — Sort an array in descending order and maintain index association
fi_asort — Sort an array in ascending order and maintain index association
fi_compact — Create array containing variables and their values
fi_count — Counts all elements in an array or in a Countable object
fi_current — Return the current element in an array
fi_each — Return the current key and value pair from an array and advance the array cursor
fi_end — Set the internal pointer of an array to its last element
fi_extract — Import variables into the current symbol table from an array
fi_in_array — Checks if a value exists in an array
fi_key — Fetch a key from an array
fi_key_exists — Alias of array_key_exists
fi_krsort — Sort an array by key in descending order
fi_ksort — Sort an array by key in ascending order
fi_list — Assign variables as if they were an array
fi_natcasesort — Sort an array using a case insensitive "natural order" algorithm
fi_natsort — Sort an array using a "natural order" algorithm
fi_next — Advance the internal pointer of an array
fi_pos — Alias of current
fi_prev — Rewind the internal array pointer
fi_range — Create an array containing a range of elements
fi_reset — Set the internal pointer of an array to its first element
fi_rsort — Sort an array in descending order
fi_shuffle — Shuffle an array
fi_sizeof — Alias of count
fi_sort — Sort an array in ascending order
fi_uasort — Sort an array with a user-defined comparison function and maintain index association
fi_uksort — Sort an array by keys using a user-defined comparison function
fi_usort — Sort an array by values using a user-defined comparison function
```

### btree

```
fi_btree_create — Create a new binary search tree
fi_btree_destroy — Destroy the entire tree and free all memory
fi_btree_clear — Clear all nodes from the tree
fi_btree_create_node — Create a new tree node
fi_btree_destroy_node — Destroy a tree node
fi_btree_insert — Insert data into the tree
fi_btree_delete — Delete data from the tree
fi_btree_delete_node — Delete a specific node from the tree
fi_btree_search — Search for data in the tree
fi_btree_find_min — Find minimum node in subtree
fi_btree_find_max — Find maximum node in subtree
fi_btree_successor — Find successor of a node
fi_btree_predecessor — Find predecessor of a node
fi_btree_size — Get the number of nodes in the tree
fi_btree_height — Get the height of the tree
fi_btree_node_height — Get the height of a specific node
fi_btree_empty — Check if tree is empty
fi_btree_contains — Check if tree contains specific data
fi_btree_inorder — Perform inorder traversal of the tree
fi_btree_preorder — Perform preorder traversal of the tree
fi_btree_postorder — Perform postorder traversal of the tree
fi_btree_level_order — Perform level-order traversal of the tree
fi_btree_to_array — Convert tree to array (inorder)
fi_btree_to_array_inorder — Convert tree to array using inorder traversal
fi_btree_to_array_preorder — Convert tree to array using preorder traversal
fi_btree_to_array_postorder — Convert tree to array using postorder traversal
fi_btree_from_array — Build tree from array
fi_btree_from_sorted_array — Build balanced tree from sorted array
fi_btree_is_bst — Check if tree is a valid binary search tree
fi_btree_print — Print tree contents
```

## Examples

### Array Usage

```c
#include "fi.h"

// Create an integer array
fi_array *arr = fi_array_create(10, sizeof(int));

// Add elements
int values[] = {10, 20, 30, 40, 50};
for (int i = 0; i < 5; i++) {
    fi_array_push(arr, &values[i]);
}

// Search for element
int search_val = 30;
ssize_t index = fi_array_search(arr, &search_val);
printf("Found at index: %zd\n", index);

// Filter elements
fi_array *filtered = fi_array_filter(arr, is_positive, NULL);

// Clean up
fi_array_destroy(arr);
```

### BTree Usage

```c
#include "fi_btree.h"

// Comparison function for integers
int compare_ints(const void *a, const void *b) {
    const int *ia = (const int*)a;
    const int *ib = (const int*)b;
    return (*ia > *ib) - (*ia < *ib);
}

// Create a BTree
fi_btree *tree = fi_btree_create(sizeof(int), compare_ints);

// Insert elements
int values[] = {50, 30, 70, 20, 40, 60, 80};
for (int i = 0; i < 7; i++) {
    fi_btree_insert(tree, &values[i]);
}

// Search for element
int search_val = 40;
fi_btree_node *found = fi_btree_search(tree, &search_val);
printf("Found: %s\n", found ? "Yes" : "No");

// Traverse tree
printf("Inorder: ");
fi_btree_inorder(tree, print_visit, NULL);

// Clean up
fi_btree_destroy(tree);
```

## Features

- **Type-agnostic**: Works with any data type using void pointers
- **Memory management**: Automatic allocation and cleanup
- **BST properties**: Maintains binary search tree ordering
- **Array integration**: Uses fi_array for level-order traversal
- **Comprehensive API**: 90+ array functions and 25+ tree functions
- **Performance**: O(log n) tree operations, O(1) array access