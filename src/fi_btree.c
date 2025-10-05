#include "fi_btree.h"
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <stdint.h>

/* Forward declarations for static functions */
static void fi_btree_clear_recursive(fi_btree_node *node);
static void fi_btree_inorder_recursive(fi_btree_node *node, fi_btree_visit_func visit, void *user_data, size_t depth);
static void fi_btree_preorder_recursive(fi_btree_node *node, fi_btree_visit_func visit, void *user_data, size_t depth);
static void fi_btree_postorder_recursive(fi_btree_node *node, fi_btree_visit_func visit, void *user_data, size_t depth);
static void fi_btree_collect_data(void *data, size_t depth, void *user_data);
static fi_btree_node* fi_btree_build_from_sorted_recursive(fi_array *arr, size_t start, size_t end, size_t element_size);
static bool fi_btree_is_bst_recursive(fi_btree_node *node, const void *min, const void *max, int (*compare_func)(const void *a, const void *b));
static void fi_btree_print_visit(void *data, size_t depth, void *user_data);

/* Helper function to compare node data */
static int compare_node_data(fi_btree *tree, const void *data1, const void *data2) {
    if (!data1 || !data2) return 0;
    return tree->compare_func(data1, data2);
}

/* Create a new BTree */
fi_btree* fi_btree_create(size_t element_size, int (*compare_func)(const void *a, const void *b)) {
    fi_btree *tree = malloc(sizeof(fi_btree));
    if (!tree) return NULL;
    
    tree->root = NULL;
    tree->element_size = element_size;
    tree->count = 0;
    tree->compare_func = compare_func;
    
    return tree;
}

/* Create a new BTree node */
fi_btree_node* fi_btree_create_node(const void *data, size_t element_size) {
    fi_btree_node *node = malloc(sizeof(fi_btree_node));
    if (!node) return NULL;
    
    node->data = malloc(element_size);
    if (!node->data) {
        free(node);
        return NULL;
    }
    
    memcpy(node->data, data, element_size);
    node->left = NULL;
    node->right = NULL;
    node->parent = NULL;
    
    return node;
}

/* Destroy a BTree node */
void fi_btree_destroy_node(fi_btree_node *node) {
    if (!node) return;
    
    if (node->data) {
        free(node->data);
    }
    free(node);
}

/* Destroy the entire BTree */
void fi_btree_destroy(fi_btree *tree) {
    if (!tree) return;
    
    fi_btree_clear(tree);
    free(tree);
}

/* Clear all nodes from the tree */
void fi_btree_clear(fi_btree *tree) {
    if (!tree) return;
    
    fi_btree_clear_recursive(tree->root);
    tree->root = NULL;
    tree->count = 0;
}

/* Recursive helper to clear nodes */
static void fi_btree_clear_recursive(fi_btree_node *node) {
    if (!node) return;
    
    fi_btree_clear_recursive(node->left);
    fi_btree_clear_recursive(node->right);
    fi_btree_destroy_node(node);
}

/* Insert data into the tree */
int fi_btree_insert(fi_btree *tree, const void *data) {
    if (!tree || !data) return -1;
    
    fi_btree_node *new_node = fi_btree_create_node(data, tree->element_size);
    if (!new_node) return -1;
    
    if (!tree->root) {
        tree->root = new_node;
        tree->count = 1;
        return 0;
    }
    
    fi_btree_node *current = tree->root;
    fi_btree_node *parent = NULL;
    
    while (current) {
        parent = current;
        int cmp = compare_node_data(tree, data, current->data);
        
        if (cmp < 0) {
            current = current->left;
        } else if (cmp > 0) {
            current = current->right;
        } else {
            /* Duplicate found - replace data */
            memcpy(current->data, data, tree->element_size);
            fi_btree_destroy_node(new_node);
            return 0;
        }
    }
    
    /* Insert new node */
    new_node->parent = parent;
    int cmp = compare_node_data(tree, data, parent->data);
    
    if (cmp < 0) {
        parent->left = new_node;
    } else {
        parent->right = new_node;
    }
    
    tree->count++;
    return 0;
}

/* Search for data in the tree */
fi_btree_node* fi_btree_search(fi_btree *tree, const void *data) {
    if (!tree || !data) return NULL;
    
    fi_btree_node *current = tree->root;
    
    while (current) {
        int cmp = compare_node_data(tree, data, current->data);
        
        if (cmp < 0) {
            current = current->left;
        } else if (cmp > 0) {
            current = current->right;
        } else {
            return current;
        }
    }
    
    return NULL;
}

/* Find minimum node in subtree */
fi_btree_node* fi_btree_find_min(fi_btree_node *node) {
    if (!node) return NULL;
    
    while (node->left) {
        node = node->left;
    }
    
    return node;
}

/* Find maximum node in subtree */
fi_btree_node* fi_btree_find_max(fi_btree_node *node) {
    if (!node) return NULL;
    
    while (node->right) {
        node = node->right;
    }
    
    return node;
}

/* Find successor of a node */
fi_btree_node* fi_btree_successor(fi_btree_node *node) {
    if (!node) return NULL;
    
    if (node->right) {
        return fi_btree_find_min(node->right);
    }
    
    fi_btree_node *parent = node->parent;
    while (parent && node == parent->right) {
        node = parent;
        parent = parent->parent;
    }
    
    return parent;
}

/* Find predecessor of a node */
fi_btree_node* fi_btree_predecessor(fi_btree_node *node) {
    if (!node) return NULL;
    
    if (node->left) {
        return fi_btree_find_max(node->left);
    }
    
    fi_btree_node *parent = node->parent;
    while (parent && node == parent->left) {
        node = parent;
        parent = parent->parent;
    }
    
    return parent;
}

/* Delete a node from the tree */
fi_btree_node* fi_btree_delete_node(fi_btree *tree, fi_btree_node *node) {
    if (!tree || !node) return NULL;
    
    fi_btree_node *node_to_delete = node;
    
    if (!node->left && !node->right) {
        /* Node has no children */
        if (node->parent) {
            if (node->parent->left == node) {
                node->parent->left = NULL;
            } else {
                node->parent->right = NULL;
            }
        } else {
            tree->root = NULL;
        }
    } else if (!node->left || !node->right) {
        /* Node has one child */
        fi_btree_node *child = node->left ? node->left : node->right;
        
        if (node->parent) {
            if (node->parent->left == node) {
                node->parent->left = child;
            } else {
                node->parent->right = child;
            }
            child->parent = node->parent;
        } else {
            tree->root = child;
            child->parent = NULL;
        }
    } else {
        /* Node has two children */
        fi_btree_node *successor = fi_btree_successor(node);
        
        /* Copy successor's data to current node */
        memcpy(node->data, successor->data, tree->element_size);
        
        /* Delete the successor */
        return fi_btree_delete_node(tree, successor);
    }
    
    tree->count--;
    fi_btree_destroy_node(node);
    return node_to_delete;
}

/* Delete data from the tree */
int fi_btree_delete(fi_btree *tree, const void *data) {
    if (!tree || !data) return -1;
    
    fi_btree_node *node = fi_btree_search(tree, data);
    if (!node) return -1;
    
    fi_btree_delete_node(tree, node);
    return 0;
}

/* Get tree size */
size_t fi_btree_size(fi_btree *tree) {
    return tree ? tree->count : 0;
}

/* Get tree height */
size_t fi_btree_height(fi_btree *tree) {
    return fi_btree_node_height(tree ? tree->root : NULL);
}

/* Get height of a specific node */
size_t fi_btree_node_height(fi_btree_node *node) {
    if (!node) return 0;
    
    size_t left_height = fi_btree_node_height(node->left);
    size_t right_height = fi_btree_node_height(node->right);
    
    return 1 + (left_height > right_height ? left_height : right_height);
}

/* Check if tree is empty */
bool fi_btree_empty(fi_btree *tree) {
    return !tree || !tree->root || tree->count == 0;
}

/* Check if tree contains data */
bool fi_btree_contains(fi_btree *tree, const void *data) {
    return fi_btree_search(tree, data) != NULL;
}

/* Inorder traversal */
void fi_btree_inorder(fi_btree *tree, fi_btree_visit_func visit, void *user_data) {
    if (!tree || !visit) return;
    fi_btree_inorder_recursive(tree->root, visit, user_data, 0);
}

/* Recursive inorder helper */
static void fi_btree_inorder_recursive(fi_btree_node *node, fi_btree_visit_func visit, void *user_data, size_t depth) {
    if (!node) return;
    
    fi_btree_inorder_recursive(node->left, visit, user_data, depth + 1);
    visit(node->data, depth, user_data);
    fi_btree_inorder_recursive(node->right, visit, user_data, depth + 1);
}

/* Preorder traversal */
void fi_btree_preorder(fi_btree *tree, fi_btree_visit_func visit, void *user_data) {
    if (!tree || !visit) return;
    fi_btree_preorder_recursive(tree->root, visit, user_data, 0);
}

/* Recursive preorder helper */
static void fi_btree_preorder_recursive(fi_btree_node *node, fi_btree_visit_func visit, void *user_data, size_t depth) {
    if (!node) return;
    
    visit(node->data, depth, user_data);
    fi_btree_preorder_recursive(node->left, visit, user_data, depth + 1);
    fi_btree_preorder_recursive(node->right, visit, user_data, depth + 1);
}

/* Postorder traversal */
void fi_btree_postorder(fi_btree *tree, fi_btree_visit_func visit, void *user_data) {
    if (!tree || !visit) return;
    fi_btree_postorder_recursive(tree->root, visit, user_data, 0);
}

/* Recursive postorder helper */
static void fi_btree_postorder_recursive(fi_btree_node *node, fi_btree_visit_func visit, void *user_data, size_t depth) {
    if (!node) return;
    
    fi_btree_postorder_recursive(node->left, visit, user_data, depth + 1);
    fi_btree_postorder_recursive(node->right, visit, user_data, depth + 1);
    visit(node->data, depth, user_data);
}

/* Level order traversal using array as queue */
void fi_btree_level_order(fi_btree *tree, fi_btree_visit_func visit, void *user_data) {
    if (!tree || !visit || !tree->root) return;
    
    fi_array *queue = fi_array_create(10, sizeof(fi_btree_node*));
    if (!queue) return;
    
    fi_array_push(queue, &tree->root);
    
    while (!fi_array_empty(queue)) {
        fi_btree_node **node_ptr = (fi_btree_node**)fi_array_get(queue, 0);
        if (!node_ptr) break;
        
        fi_btree_node *node = *node_ptr;
        fi_array_shift(queue, NULL);
        
        if (node && node->data) {
            visit(node->data, 0, user_data); /* Depth not tracked in level order */
            
            if (node->left) {
                fi_array_push(queue, &node->left);
            }
            if (node->right) {
                fi_array_push(queue, &node->right);
            }
        }
    }
    
    fi_array_destroy(queue);
}

/* Convert tree to array (inorder) */
fi_array* fi_btree_to_array(fi_btree *tree) {
    return fi_btree_to_array_inorder(tree);
}

/* Convert tree to array using inorder traversal */
fi_array* fi_btree_to_array_inorder(fi_btree *tree) {
    if (!tree) return NULL;
    
    fi_array *result = fi_array_create(tree->count, tree->element_size);
    if (!result) return NULL;
    
    fi_btree_inorder(tree, fi_btree_collect_data, result);
    
    return result;
}

/* Helper function to collect data during traversal */
static void fi_btree_collect_data(void *data, size_t depth, void *user_data) {
    (void)depth; /* Suppress unused parameter warning */
    
    fi_array *arr = (fi_array*)user_data;
    fi_array_push(arr, data);
}

/* Convert tree to array using preorder traversal */
fi_array* fi_btree_to_array_preorder(fi_btree *tree) {
    if (!tree) return NULL;
    
    fi_array *result = fi_array_create(tree->count, tree->element_size);
    if (!result) return NULL;
    
    fi_btree_preorder(tree, fi_btree_collect_data, result);
    
    return result;
}

/* Convert tree to array using postorder traversal */
fi_array* fi_btree_to_array_postorder(fi_btree *tree) {
    if (!tree) return NULL;
    
    fi_array *result = fi_array_create(tree->count, tree->element_size);
    if (!result) return NULL;
    
    fi_btree_postorder(tree, fi_btree_collect_data, result);
    
    return result;
}

/* Build tree from array */
fi_btree* fi_btree_from_array(fi_array *arr, int (*compare_func)(const void *a, const void *b)) {
    if (!arr || !compare_func) return NULL;
    
    fi_btree *tree = fi_btree_create(fi_array_count(arr) > 0 ? arr->element_size : 1, compare_func);
    if (!tree) return NULL;
    
    for (size_t i = 0; i < fi_array_count(arr); i++) {
        void *data = fi_array_get(arr, i);
        if (data) {
            fi_btree_insert(tree, data);
        }
    }
    
    return tree;
}

/* Build balanced tree from sorted array */
fi_btree* fi_btree_from_sorted_array(fi_array *arr, int (*compare_func)(const void *a, const void *b)) {
    if (!arr || !compare_func || fi_array_count(arr) == 0) return NULL;
    
    fi_btree *tree = fi_btree_create(arr->element_size, compare_func);
    if (!tree) return NULL;
    
    tree->root = fi_btree_build_from_sorted_recursive(arr, 0, fi_array_count(arr) - 1, tree->element_size);
    tree->count = fi_array_count(arr);
    
    return tree;
}

/* Recursive helper to build balanced tree from sorted array */
static fi_btree_node* fi_btree_build_from_sorted_recursive(fi_array *arr, size_t start, size_t end, size_t element_size) {
    if (start > end) return NULL;
    
    size_t mid = start + (end - start) / 2;
    void *data = fi_array_get(arr, mid);
    
    fi_btree_node *node = fi_btree_create_node(data, element_size);
    if (!node) return NULL;
    
    node->left = fi_btree_build_from_sorted_recursive(arr, start, mid - 1, element_size);
    node->right = fi_btree_build_from_sorted_recursive(arr, mid + 1, end, element_size);
    
    if (node->left) node->left->parent = node;
    if (node->right) node->right->parent = node;
    
    return node;
}

/* Check if tree is a valid BST */
bool fi_btree_is_bst(fi_btree *tree) {
    if (!tree) return true;
    return fi_btree_is_bst_recursive(tree->root, NULL, NULL, tree->compare_func);
}

/* Recursive helper to check BST property */
static bool fi_btree_is_bst_recursive(fi_btree_node *node, const void *min, const void *max, int (*compare_func)(const void *a, const void *b)) {
    if (!node) return true;
    
    if (min && compare_func(node->data, min) <= 0) return false;
    if (max && compare_func(node->data, max) >= 0) return false;
    
    return fi_btree_is_bst_recursive(node->left, min, node->data, compare_func) &&
           fi_btree_is_bst_recursive(node->right, node->data, max, compare_func);
}

/* Print tree (simple) */
void fi_btree_print(fi_btree *tree, void (*print_func)(const void *data)) {
    if (!tree || !print_func) return;
    
    printf("Tree (size: %zu, height: %zu):\n", fi_btree_size(tree), fi_btree_height(tree));
    fi_btree_inorder(tree, fi_btree_print_visit, print_func);
    printf("\n");
}

/* Helper function for printing */
static void fi_btree_print_visit(void *data, size_t depth, void *user_data) {
    (void)depth; /* Suppress unused parameter warning */
    
    void (*print_func)(const void *data) = (void (*)(const void *data))user_data;
    print_func(data);
    printf(" ");
}
