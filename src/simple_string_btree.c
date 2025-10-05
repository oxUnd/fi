#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "fi.h"
#include "fi_btree.h"

/* String comparison function */
int compare_strings(const void *a, const void *b) {
    const char *str_a = *(const char**)a;
    const char *str_b = *(const char**)b;
    return strcmp(str_a, str_b);
}

/* Print string visit function */
void print_string_visit(void *data, size_t depth, void *user_data) {
    (void)depth; /* Suppress unused parameter warning */
    (void)user_data; /* Suppress unused parameter warning */
    
    char **str_ptr = (char**)data;
    if (str_ptr && *str_ptr) {
        printf("\"%s\" ", *str_ptr);
    }
}

int main() {
    printf("=== String BTree Demo ===\n");
    
    // Create BTree for strings (storing char* pointers)
    fi_btree *tree = fi_btree_create(sizeof(char*), compare_strings);
    if (!tree) {
        printf("Failed to create BTree\n");
        return 1;
    }
    
    printf("BTree created successfully\n");
    
    // Insert strings
    const char *words[] = {"apple", "banana", "cherry", "date", "elderberry"};
    int count = sizeof(words) / sizeof(words[0]);
    
    printf("Inserting words: ");
    for (int i = 0; i < count; i++) {
        printf("%s ", words[i]);
        
        // Create a copy of the string
        char *word_copy = malloc(strlen(words[i]) + 1);
        if (!word_copy) {
            printf("Memory allocation failed for: %s\n", words[i]);
            continue;
        }
        strcpy(word_copy, words[i]);
        
        // Insert the pointer to the string
        if (fi_btree_insert(tree, &word_copy) != 0) {
            printf("Failed to insert: %s\n", words[i]);
            free(word_copy);
        }
    }
    printf("\n");
    
    printf("Tree size: %zu, height: %zu\n", fi_btree_size(tree), fi_btree_height(tree));
    
    // Print tree contents (inorder - alphabetical order)
    printf("Tree contents (inorder): ");
    fi_btree_inorder(tree, print_string_visit, NULL);
    printf("\n");
    
    // Search for a word
    const char *search_word = "cherry";
    fi_btree_node *found = fi_btree_search(tree, &search_word);
    printf("Search for '%s': %s\n", search_word, found ? "Found" : "Not found");
    
    if (found) {
        char **found_str = (char**)found->data;
        printf("Found word: \"%s\"\n", *found_str);
    }
    
    // Find min and max (alphabetically first and last)
    fi_btree_node *min_node = fi_btree_find_min(tree->root);
    fi_btree_node *max_node = fi_btree_find_max(tree->root);
    
    if (min_node) {
        char **min_str = (char**)min_node->data;
        printf("Alphabetically first: \"%s\"\n", *min_str);
    }
    if (max_node) {
        char **max_str = (char**)max_node->data;
        printf("Alphabetically last: \"%s\"\n", *max_str);
    }
    
    // Test different traversals
    printf("\nTraversals:\n");
    printf("Preorder: ");
    fi_btree_preorder(tree, print_string_visit, NULL);
    printf("\n");
    
    printf("Inorder: ");
    fi_btree_inorder(tree, print_string_visit, NULL);
    printf("\n");
    
    printf("Postorder: ");
    fi_btree_postorder(tree, print_string_visit, NULL);
    printf("\n");
    
    // Convert to array
    fi_array *arr = fi_btree_to_array(tree);
    if (arr) {
        printf("\nTree as array: [");
        for (size_t i = 0; i < fi_array_count(arr); i++) {
            char **str_ptr = (char**)fi_array_get(arr, i);
            if (str_ptr && *str_ptr) {
                printf("\"%s\"", *str_ptr);
                if (i < fi_array_count(arr) - 1) {
                    printf(", ");
                }
            }
        }
        printf("]\n");
        fi_array_destroy(arr);
    }
    
    // Delete a word
    const char *delete_word = "banana";
    printf("\nDeleting '%s'...\n", delete_word);
    fi_btree_delete(tree, &delete_word);
    printf("After deletion - size: %zu\n", fi_btree_size(tree));
    
    printf("Tree after deletion: ");
    fi_btree_inorder(tree, print_string_visit, NULL);
    printf("\n");
    
    fi_btree_destroy(tree);
    printf("BTree destroyed successfully\n");
    
    printf("\n=== String BTree Demo Complete ===\n");
    return 0;
}
