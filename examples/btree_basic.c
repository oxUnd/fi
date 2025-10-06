/**
 * @file btree_basic.c
 * @brief 基本二叉搜索树操作示例
 * 
 * 这个示例展示了如何使用fi_btree进行基本的二叉搜索树操作，
 * 包括创建、插入、删除、搜索、遍历等操作。
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../src/include/fi_btree.h"

// 整数比较函数
int compare_int(const void *a, const void *b) {
    int ia = *(int*)a;
    int ib = *(int*)b;
    return ia - ib;
}

// 打印整数（用于遍历）
void print_int(void *data, size_t depth, void *user_data) {
    (void)depth;      // 避免未使用参数警告
    (void)user_data;  // 避免未使用参数警告
    printf("%d ", *(int*)data);
}

// 打印整数（用于打印树结构）
void print_int_simple(const void *data) {
    printf("%d", *(int*)data);
}

// 遍历访问函数
void visit_node(void *data, size_t depth, void *user_data) {
    (void)user_data;  // 避免未使用参数警告
    int value = *(int*)data;
    printf("%*s%d (深度: %zu)\n", (int)depth * 2, "", value, depth);
}

// 计算节点数的访问函数
void count_nodes(void *data, size_t depth, void *user_data) {
    (void)data;   // 避免未使用参数警告
    (void)depth;  // 避免未使用参数警告
    (*(int*)user_data)++;
}

// 查找偶数的访问函数
void find_even_nodes(void *data, size_t depth, void *user_data) {
    (void)user_data;  // 避免未使用参数警告
    int value = *(int*)data;
    if (value % 2 == 0) {
        printf("找到偶数节点: %d (深度: %zu)\n", value, depth);
    }
}

int main() {
    printf("=== FI BTree 基本操作示例 ===\n\n");
    
    // 1. 创建二叉搜索树
    printf("1. 创建二叉搜索树\n");
    fi_btree *tree = fi_btree_create(sizeof(int), compare_int);
    if (!tree) {
        fprintf(stderr, "无法创建二叉搜索树\n");
        return 1;
    }
    
    // 2. 插入元素
    printf("2. 插入元素\n");
    int values[] = {50, 30, 70, 20, 40, 60, 80, 10, 25, 35, 45};
    int num_values = sizeof(values) / sizeof(values[0]);
    
    for (int i = 0; i < num_values; i++) {
        if (fi_btree_insert(tree, &values[i]) != 0) {
            fprintf(stderr, "插入失败: %d\n", values[i]);
        }
    }
    
    printf("插入的元素: ");
    for (int i = 0; i < num_values; i++) {
        printf("%d ", values[i]);
    }
    printf("\n");
    
    // 3. 树的基本信息
    printf("\n3. 树的基本信息\n");
    printf("树的大小: %zu\n", fi_btree_size(tree));
    printf("树的高度: %zu\n", fi_btree_height(tree));
    printf("树是否为空: %s\n", fi_btree_empty(tree) ? "是" : "否");
    
    // 4. 搜索操作
    printf("\n4. 搜索操作\n");
    int search_value = 40;
    fi_btree_node *found_node = fi_btree_search(tree, &search_value);
    if (found_node) {
        printf("找到值 %d\n", search_value);
    } else {
        printf("未找到值 %d\n", search_value);
    }
    
    // 5. 查找最小值和最大值
    printf("\n5. 查找最小值和最大值\n");
    fi_btree_node *min_node = fi_btree_find_min(tree->root);
    fi_btree_node *max_node = fi_btree_find_max(tree->root);
    
    if (min_node) {
        printf("最小值: %d\n", *(int*)min_node->data);
    }
    if (max_node) {
        printf("最大值: %d\n", *(int*)max_node->data);
    }
    
    // 6. 树的遍历
    printf("\n6. 树的遍历\n");
    
    printf("中序遍历 (左-根-右):\n");
    fi_btree_inorder(tree, visit_node, NULL);
    printf("\n");
    
    printf("前序遍历 (根-左-右):\n");
    fi_btree_preorder(tree, visit_node, NULL);
    printf("\n");
    
    printf("后序遍历 (左-右-根):\n");
    fi_btree_postorder(tree, visit_node, NULL);
    printf("\n");
    
    printf("层序遍历:\n");
    fi_btree_level_order(tree, visit_node, NULL);
    printf("\n");
    
    // 7. 转换为数组
    printf("7. 转换为数组\n");
    fi_array *inorder_array = fi_btree_to_array_inorder(tree);
    printf("中序遍历数组: ");
    for (size_t i = 0; i < fi_array_count(inorder_array); i++) {
        int *value = (int*)fi_array_get(inorder_array, i);
        printf("%d ", *value);
    }
    printf("\n");
    
    // 8. 删除操作
    printf("\n8. 删除操作\n");
    int delete_value = 30;
    printf("删除值 %d 前，树大小: %zu\n", delete_value, fi_btree_size(tree));
    
    if (fi_btree_delete(tree, &delete_value) == 0) {
        printf("成功删除值 %d\n", delete_value);
        printf("删除后树大小: %zu\n", fi_btree_size(tree));
        
        printf("删除后的中序遍历: ");
        fi_btree_inorder(tree, print_int, NULL);
        printf("\n");
    } else {
        printf("删除值 %d 失败\n", delete_value);
    }
    
    // 9. 后继和前驱
    printf("\n9. 后继和前驱\n");
    fi_btree_node *node_40 = fi_btree_search(tree, &(int){40});
    if (node_40) {
        fi_btree_node *successor = fi_btree_successor(node_40);
        fi_btree_node *predecessor = fi_btree_predecessor(node_40);
        
        if (successor) {
            printf("40 的后继: %d\n", *(int*)successor->data);
        }
        if (predecessor) {
            printf("40 的前驱: %d\n", *(int*)predecessor->data);
        }
    }
    
    // 10. 树的性质检查
    printf("\n10. 树的性质检查\n");
    printf("是否为有效的二叉搜索树: %s\n", 
           fi_btree_is_bst(tree) ? "是" : "否");
    
    // 11. 清理资源
    printf("\n11. 清理资源\n");
    fi_btree_destroy(tree);
    fi_array_destroy(inorder_array);
    
    printf("=== 示例完成 ===\n");
    return 0;
}
