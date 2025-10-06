/**
 * @file array_basic.c
 * @brief 基本数组操作示例
 * 
 * 这个示例展示了如何使用fi_array进行基本的数组操作，
 * 包括创建、添加、删除、搜索等操作。
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../src/include/fi.h"

// 整数比较函数
int compare_int(const void *a, const void *b) {
    int ia = *(int*)a;
    int ib = *(int*)b;
    return ia - ib;
}

// 打印整数数组
void print_int_array(fi_array *arr, const char *title) {
    printf("%s: ", title);
    for (size_t i = 0; i < fi_array_count(arr); i++) {
        int *value = (int*)fi_array_get(arr, i);
        printf("%d ", *value);
    }
    printf("\n");
}

// 回调函数：查找大于10的元素
bool find_greater_than_10(void *element, size_t index, void *user_data) {
    (void)index;  // 避免未使用参数警告
    int value = *(int*)element;
    int threshold = *(int*)user_data;
    return value > threshold;
}

// 回调函数：计算平方
bool square_element(void *element, size_t index, void *user_data) {
    (void)index;      // 避免未使用参数警告
    (void)user_data;  // 避免未使用参数警告
    int *value = (int*)element;
    *value = (*value) * (*value);
    return true;
}

int main() {
    printf("=== FI Array 基本操作示例 ===\n\n");
    
    // 1. 创建数组
    printf("1. 创建整数数组\n");
    fi_array *numbers = fi_array_create(5, sizeof(int));
    if (!numbers) {
        fprintf(stderr, "无法创建数组\n");
        return 1;
    }
    
    // 2. 添加元素
    printf("2. 添加元素\n");
    int values[] = {5, 15, 3, 20, 8, 12, 1};
    for (int i = 0; i < 7; i++) {
        fi_array_push(numbers, &values[i]);
    }
    print_int_array(numbers, "添加元素后");
    
    // 3. 数组大小和容量
    printf("3. 数组大小: %zu, 容量: %zu\n\n", 
           fi_array_count(numbers), numbers->capacity);
    
    // 4. 访问和修改元素
    printf("4. 访问和修改元素\n");
    int *first = (int*)fi_array_get(numbers, 0);
    printf("第一个元素: %d\n", *first);
    
    int new_value = 100;
    fi_array_set(numbers, 0, &new_value);
    printf("修改第一个元素为100后: ");
    print_int_array(numbers, "");
    
    // 5. 栈操作
    printf("\n5. 栈操作\n");
    int pop_value;
    if (fi_array_pop(numbers, &pop_value) == 0) {
        printf("弹出元素: %d\n", pop_value);
    }
    
    int push_value = 99;
    fi_array_push(numbers, &push_value);
    print_int_array(numbers, "弹出并推入后");
    
    // 6. 搜索操作
    printf("\n6. 搜索操作\n");
    int search_value = 20;
    ssize_t index = fi_array_search(numbers, &search_value);
    if (index >= 0) {
        printf("找到值 %d 在索引 %zd\n", search_value, index);
    } else {
        printf("未找到值 %d\n", search_value);
    }
    
    // 7. 使用回调函数查找
    int threshold = 10;
    void *found = fi_array_find(numbers, find_greater_than_10, &threshold);
    if (found) {
        printf("第一个大于10的元素: %d\n", *(int*)found);
    }
    
    // 8. 数组过滤
    printf("\n7. 数组过滤\n");
    fi_array *filtered = fi_array_filter(numbers, find_greater_than_10, &threshold);
    print_int_array(filtered, "大于10的元素");
    
    // 9. 数组排序
    printf("\n8. 数组排序\n");
    fi_array_sort(numbers, compare_int);
    print_int_array(numbers, "排序后");
    
    // 10. 数组反转
    printf("\n9. 数组反转\n");
    fi_array_reverse(numbers);
    print_int_array(numbers, "反转后");
    
    // 11. 数组切片
    printf("\n10. 数组切片\n");
    fi_array *slice = fi_array_slice(numbers, 1, 3);
    print_int_array(slice, "从索引1开始取3个元素");
    
    // 12. 数组合并
    printf("\n11. 数组合并\n");
    fi_array *another = fi_array_create(3, sizeof(int));
    int more_values[] = {50, 60, 70};
    for (int i = 0; i < 3; i++) {
        fi_array_push(another, &more_values[i]);
    }
    fi_array_merge(numbers, another);
    print_int_array(numbers, "合并后");
    
    // 13. 数学运算
    printf("\n12. 数学运算\n");
    double sum = fi_array_sum(numbers);
    double product = fi_array_product(numbers);
    printf("数组元素和: %.2f\n", sum);
    printf("数组元素积: %.2f\n", product);
    
    // 14. 数组去重
    printf("\n13. 数组去重\n");
    fi_array *unique = fi_array_unique(numbers);
    print_int_array(unique, "去重后");
    
    // 15. 清理资源
    printf("\n14. 清理资源\n");
    fi_array_destroy(numbers);
    fi_array_destroy(filtered);
    fi_array_destroy(slice);
    fi_array_destroy(another);
    fi_array_destroy(unique);
    
    printf("\n=== 示例完成 ===\n");
    return 0;
}
