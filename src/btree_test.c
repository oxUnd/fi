#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "fi.h"
#include "fi_btree.h"

int compare_ints(const void *a, const void *b) {
    const int *ia = (const int*)a;
    const int *ib = (const int*)b;
    return (*ia > *ib) - (*ia < *ib);
}

void print_int_visit(void *data, size_t depth, void *user_data) {
    (void)depth;
    (void)user_data;
    const int *value = (const int*)data;
    printf("%d ", *value);
}

int main() {
    printf("=== Simple BTree Test ===\n");
    
    fi_btree *tree = fi_btree_create(sizeof(int), compare_ints);
    if (!tree) {
        printf("Failed to create BTree\n");
        return 1;
    }
    
    printf("BTree created successfully\n");
    
    // Insert a few integers
    int values[] = {5, 3, 7, 1, 4, 6, 8};
    int count = sizeof(values) / sizeof(values[0]);
    
    printf("Inserting values: ");
    for (int i = 0; i < count; i++) {
        printf("%d ", values[i]);
        if (fi_btree_insert(tree, &values[i]) != 0) {
            printf("Failed to insert %d\n", values[i]);
            return 1;
        }
    }
    printf("\n");
    
    printf("Tree size: %zu, height: %zu\n", fi_btree_size(tree), fi_btree_height(tree));
    
    // Test search
    int search_val = 5;
    fi_btree_node *found = fi_btree_search(tree, &search_val);
    printf("Search for %d: %s\n", search_val, found ? "Found" : "Not found");
    
    // Test inorder traversal
    printf("Inorder traversal: ");
    fi_btree_inorder(tree, print_int_visit, NULL);
    printf("\n");
    
    // Test min/max
    fi_btree_node *min_node = fi_btree_find_min(tree->root);
    fi_btree_node *max_node = fi_btree_find_max(tree->root);
    
    if (min_node) printf("Min: %d\n", *(int*)min_node->data);
    if (max_node) printf("Max: %d\n", *(int*)max_node->data);
    
    // Test deletion
    printf("Deleting 5...\n");
    fi_btree_delete(tree, &search_val);
    printf("After deletion - size: %zu\n", fi_btree_size(tree));
    
    printf("Inorder after deletion: ");
    fi_btree_inorder(tree, print_int_visit, NULL);
    printf("\n");
    
    fi_btree_destroy(tree);
    printf("BTree destroyed successfully\n");
    
    printf("=== Simple BTree Test Complete ===\n");
    return 0;
}
