/**
 * @file map_basic.c
 * @brief 基本哈希映射操作示例
 * 
 * 这个示例展示了如何使用fi_map进行基本的哈希映射操作，
 * 包括创建、插入、查找、删除、遍历等操作。
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../src/include/fi_map.h"

// 字符串比较函数
int compare_string(const void *key1, const void *key2) {
    return strcmp((char*)key1, (char*)key2);
}

// 打印映射内容
void print_map(fi_map *map, const char *title) {
    printf("%s:\n", title);
    
    fi_map_iterator iter = fi_map_iterator_create(map);
    while (fi_map_iterator_next(&iter)) {
        char *key = (char*)fi_map_iterator_key(&iter);
        int *value = (int*)fi_map_iterator_value(&iter);
        printf("  %s -> %d\n", key, *value);
    }
    fi_map_iterator_destroy(&iter);
    printf("\n");
}

// 回调函数：打印键值对
void print_key_value(const void *key, const void *value, void *user_data) {
    (void)user_data;  // 避免未使用参数警告
    char *key_str = (char*)key;
    int *value_int = (int*)value;
    printf("  %s: %d\n", key_str, *value_int);
}

// 回调函数：查找值为偶数的元素
bool find_even_values(const void *key, const void *value, void *user_data) {
    (void)key;        // 避免未使用参数警告
    (void)user_data;  // 避免未使用参数警告
    int *value_int = (int*)value;
    return (*value_int) % 2 == 0;
}

int main() {
    printf("=== FI Map 基本操作示例 ===\n\n");
    
    // 1. 创建字符串到整数的映射
    printf("1. 创建字符串到整数的映射\n");
    fi_map *age_map = fi_map_create(10, sizeof(char*), sizeof(int), 
                                   fi_map_hash_string, fi_map_compare_string);
    if (!age_map) {
        fprintf(stderr, "无法创建映射\n");
        return 1;
    }
    
    // 2. 插入键值对
    printf("2. 插入键值对\n");
    char *names[] = {"Alice", "Bob", "Charlie", "David", "Eve"};
    int ages[] = {25, 30, 35, 28, 32};
    
    for (int i = 0; i < 5; i++) {
        if (fi_map_put(age_map, names[i], &ages[i]) != 0) {
            fprintf(stderr, "插入失败: %s\n", names[i]);
        }
    }
    print_map(age_map, "插入后");
    
    // 3. 查找值
    printf("3. 查找值\n");
    char *search_name = "Bob";
    int found_age;
    if (fi_map_get(age_map, search_name, &found_age) == 0) {
        printf("找到 %s 的年龄: %d\n", search_name, found_age);
    } else {
        printf("未找到 %s\n", search_name);
    }
    
    // 4. 检查键是否存在
    printf("\n4. 检查键是否存在\n");
    char *check_name = "Charlie";
    if (fi_map_contains(age_map, check_name)) {
        printf("映射包含键: %s\n", check_name);
    } else {
        printf("映射不包含键: %s\n", check_name);
    }
    
    // 5. 更新值
    printf("\n5. 更新值\n");
    int new_age = 31;
    fi_map_put(age_map, "Bob", &new_age);
    printf("更新 Bob 的年龄后:\n");
    print_map(age_map, "");
    
    // 6. 使用 put_if_absent
    printf("6. 使用 put_if_absent\n");
    int default_age = 20;
    char *new_name = "Frank";
    if (fi_map_put_if_absent(age_map, new_name, &default_age) == 0) {
        printf("成功添加新键: %s\n", new_name);
    } else {
        printf("键 %s 已存在\n", new_name);
    }
    print_map(age_map, "使用 put_if_absent 后");
    
    // 7. 获取或设置默认值
    printf("7. 获取或设置默认值\n");
    char *missing_name = "Grace";
    int default_value = 18;
    int result_age;
    fi_map_get_or_default(age_map, missing_name, &result_age, &default_value);
    printf("获取 %s 的年龄（不存在时使用默认值）: %d\n", missing_name, result_age);
    
    // 8. 删除键值对
    printf("\n8. 删除键值对\n");
    char *delete_name = "David";
    if (fi_map_remove(age_map, delete_name) == 0) {
        printf("成功删除键: %s\n", delete_name);
    }
    print_map(age_map, "删除后");
    
    // 9. 映射大小和负载因子
    printf("9. 映射统计信息\n");
    printf("映射大小: %zu\n", fi_map_size(age_map));
    printf("负载因子: %.2f\n", fi_map_load_factor(age_map));
    printf("最大探测距离: %zu\n", fi_map_max_probe_distance(age_map));
    printf("平均探测距离: %.2f\n", fi_map_average_probe_distance(age_map));
    
    // 10. 遍历映射
    printf("\n10. 遍历映射\n");
    printf("使用 for_each 遍历:\n");
    fi_map_for_each(age_map, print_key_value, NULL);
    
    // 11. 过滤映射
    printf("11. 过滤映射（只保留年龄为偶数的）\n");
    fi_map *filtered_map = fi_map_filter(age_map, find_even_values, NULL);
    print_map(filtered_map, "过滤后（偶数年龄）");
    
    // 12. 获取所有键和值
    printf("12. 获取所有键和值\n");
    fi_array *keys = fi_map_keys(age_map);
    fi_array *values = fi_map_values(age_map);
    
    printf("所有键: ");
    for (size_t i = 0; i < fi_array_count(keys); i++) {
        char *key = *(char**)fi_array_get(keys, i);
        printf("%s ", key);
    }
    printf("\n");
    
    printf("所有值: ");
    for (size_t i = 0; i < fi_array_count(values); i++) {
        int *value = *(int**)fi_array_get(values, i);
        printf("%d ", *value);
    }
    printf("\n\n");
    
    // 13. 创建第二个映射并合并
    printf("13. 创建第二个映射并合并\n");
    fi_map *another_map = fi_map_create(5, sizeof(char*), sizeof(int),
                                       fi_map_hash_string, fi_map_compare_string);
    char *more_names[] = {"Henry", "Ivy"};
    int more_ages[] = {27, 29};
    
    for (int i = 0; i < 2; i++) {
        fi_map_put(another_map, more_names[i], &more_ages[i]);
    }
    print_map(another_map, "第二个映射");
    
    fi_map_merge(age_map, another_map);
    print_map(age_map, "合并后");
    
    // 14. 检查映射是否为空
    printf("14. 检查映射状态\n");
    printf("映射是否为空: %s\n", fi_map_empty(age_map) ? "是" : "否");
    
    // 15. 清理资源
    printf("\n15. 清理资源\n");
    fi_map_destroy(age_map);
    fi_map_destroy(filtered_map);
    fi_map_destroy(another_map);
    fi_array_destroy(keys);
    fi_array_destroy(values);
    
    printf("=== 示例完成 ===\n");
    return 0;
}
