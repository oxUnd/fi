#ifndef __FI_BTREE_H__
#define __FI_BTREE_H__

#include "fi.h"

/* BTree node structure */
typedef struct fi_btree_node {
    void *data;                    /* Pointer to the data */
    struct fi_btree_node *left;    /* Left child */
    struct fi_btree_node *right;   /* Right child */
    struct fi_btree_node *parent;  /* Parent node */
} fi_btree_node;

/* BTree structure */
typedef struct fi_btree {
    fi_btree_node *root;           /* Root node */
    size_t element_size;           /* Size of each element in bytes */
    size_t count;                  /* Number of nodes */
    int (*compare_func)(const void *a, const void *b); /* Comparison function */
} fi_btree;

/* BTree operations */
fi_btree* fi_btree_create(size_t element_size, int (*compare_func)(const void *a, const void *b));
void fi_btree_destroy(fi_btree *tree);
void fi_btree_clear(fi_btree *tree);

/* Node operations */
fi_btree_node* fi_btree_create_node(const void *data, size_t element_size);
void fi_btree_destroy_node(fi_btree_node *node);

/* Insertion and deletion */
int fi_btree_insert(fi_btree *tree, const void *data);
int fi_btree_delete(fi_btree *tree, const void *data);
fi_btree_node* fi_btree_delete_node(fi_btree *tree, fi_btree_node *node);

/* Search operations */
fi_btree_node* fi_btree_search(fi_btree *tree, const void *data);
fi_btree_node* fi_btree_find_min(fi_btree_node *node);
fi_btree_node* fi_btree_find_max(fi_btree_node *node);
fi_btree_node* fi_btree_successor(fi_btree_node *node);
fi_btree_node* fi_btree_predecessor(fi_btree_node *node);

/* Tree properties */
size_t fi_btree_size(fi_btree *tree);
size_t fi_btree_height(fi_btree *tree);
size_t fi_btree_node_height(fi_btree_node *node);
bool fi_btree_empty(fi_btree *tree);
bool fi_btree_contains(fi_btree *tree, const void *data);

/* Traversal operations */
typedef void (*fi_btree_visit_func)(void *data, size_t depth, void *user_data);

void fi_btree_inorder(fi_btree *tree, fi_btree_visit_func visit, void *user_data);
void fi_btree_preorder(fi_btree *tree, fi_btree_visit_func visit, void *user_data);
void fi_btree_postorder(fi_btree *tree, fi_btree_visit_func visit, void *user_data);
void fi_btree_level_order(fi_btree *tree, fi_btree_visit_func visit, void *user_data);

/* Array conversion */
fi_array* fi_btree_to_array(fi_btree *tree);
fi_array* fi_btree_to_array_inorder(fi_btree *tree);
fi_array* fi_btree_to_array_preorder(fi_btree *tree);
fi_array* fi_btree_to_array_postorder(fi_btree *tree);

/* Tree construction from array */
fi_btree* fi_btree_from_array(fi_array *arr, int (*compare_func)(const void *a, const void *b));
fi_btree* fi_btree_from_sorted_array(fi_array *arr, int (*compare_func)(const void *a, const void *b));

/* Utility functions */
bool fi_btree_is_bst(fi_btree *tree);
bool fi_btree_is_balanced(fi_btree *tree);
fi_btree_node* fi_btree_lca(fi_btree *tree, const void *data1, const void *data2);
fi_array* fi_btree_path_to_root(fi_btree *tree, const void *data);

/* Tree manipulation */
void fi_btree_rotate_left(fi_btree *tree, fi_btree_node *node);
void fi_btree_rotate_right(fi_btree *tree, fi_btree_node *node);
fi_btree* fi_btree_mirror(fi_btree *tree);

/* Visualization */
void fi_btree_print(fi_btree *tree, void (*print_func)(const void *data));
void fi_btree_print_tree(fi_btree *tree, void (*print_func)(const void *data));

#endif //__FI_BTREE_H__
