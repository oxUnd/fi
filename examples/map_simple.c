/**
 * @file map_simple.c
 * @brief 简化的哈希映射操作示例
 * 
 * 这个示例展示了如何使用fi_map进行基本的哈希映射操作，
 * 使用整数键来避免字符串处理的复杂性。
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../src/include/fi_map.h"

// 打印映射内容
void print_map(fi_map *map, const char *title) {
    printf("%s:\n", title);
    
    fi_map_iterator iter = fi_map_iterator_create(map);
    while (fi_map_iterator_next(&iter)) {
        int *key = (int*)fi_map_iterator_key(&iter);
        int *value = (int*)fi_map_iterator_value(&iter);
        printf("  %d -> %d\n", *key, *value);
    }
    fi_map_iterator_destroy(&iter);
    printf("\n");
}

// 回调函数：打印键值对
void print_key_value(const void *key, const void *value, void *user_data) {
    (void)user_data;  // 避免未使用参数警告
    int *key_int = (int*)key;
    int *value_int = (int*)value;
    printf("  %d: %d\n", *key_int, *value_int);
}

int main() {
    printf("=== FI Map 简化示例 ===\n\n");
    
    // 1. 创建整数到整数的映射
    printf("1. 创建整数到整数的映射\n");
    fi_map *score_map = fi_map_create(10, sizeof(int), sizeof(int), 
                                     fi_map_hash_int32, fi_map_compare_int32);
    if (!score_map) {
        fprintf(stderr, "无法创建映射\n");
        return 1;
    }
    
    // 2. 插入键值对
    printf("2. 插入键值对\n");
    int keys[] = {1001, 1002, 1003, 1004, 1005};
    int scores[] = {85, 92, 78, 96, 88};
    
    for (int i = 0; i < 5; i++) {
        if (fi_map_put(score_map, &keys[i], &scores[i]) != 0) {
            fprintf(stderr, "插入失败: %d\n", keys[i]);
        }
    }
    print_map(score_map, "插入后");
    
    // 3. 查找值
    printf("3. 查找值\n");
    int search_key = 1002;
    int found_score;
    if (fi_map_get(score_map, &search_key, &found_score) == 0) {
        printf("找到键 %d 的值: %d\n", search_key, found_score);
    } else {
        printf("未找到键 %d\n", search_key);
    }
    
    // 4. 检查键是否存在
    printf("\n4. 检查键是否存在\n");
    int check_key = 1003;
    if (fi_map_contains(score_map, &check_key)) {
        printf("映射包含键: %d\n", check_key);
    } else {
        printf("映射不包含键: %d\n", check_key);
    }
    
    // 5. 更新值
    printf("\n5. 更新值\n");
    int new_score = 95;
    fi_map_put(score_map, &check_key, &new_score);
    printf("更新键 %d 的值后:\n", check_key);
    print_map(score_map, "");
    
    // 6. 映射大小和负载因子
    printf("6. 映射统计信息\n");
    printf("映射大小: %zu\n", fi_map_size(score_map));
    printf("负载因子: %.2f\n", fi_map_load_factor(score_map));
    
    // 7. 遍历映射
    printf("\n7. 遍历映射\n");
    printf("使用 for_each 遍历:\n");
    fi_map_for_each(score_map, print_key_value, NULL);
    
    // 8. 获取所有键和值
    printf("8. 获取所有键和值\n");
    fi_array *map_keys = fi_map_keys(score_map);
    fi_array *map_values = fi_map_values(score_map);
    
    printf("所有键: ");
    for (size_t i = 0; i < fi_array_count(map_keys); i++) {
        int *key = (int*)fi_array_get(map_keys, i);
        printf("%d ", *key);
    }
    printf("\n");
    
    printf("所有值: ");
    for (size_t i = 0; i < fi_array_count(map_values); i++) {
        int *value = (int*)fi_array_get(map_values, i);
        printf("%d ", *value);
    }
    printf("\n\n");
    
    // 9. 删除键值对
    printf("9. 删除键值对\n");
    int delete_key = 1004;
    if (fi_map_remove(score_map, &delete_key) == 0) {
        printf("成功删除键: %d\n", delete_key);
    }
    print_map(score_map, "删除后");
    
    // 10. 清理资源
    printf("10. 清理资源\n");
    fi_map_destroy(score_map);
    fi_array_destroy(map_keys);
    fi_array_destroy(map_values);
    
    printf("=== 示例完成 ===\n");
    return 0;
}
