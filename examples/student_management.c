/**
 * @file student_management.c
 * @brief 学生管理系统示例
 * 
 * 这个示例展示了一个完整的学生管理系统，使用fi_map来存储学生信息，
 * 使用fi_array来处理学生列表，演示了实际应用中的数据结构使用。
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../src/include/fi_map.h"
#include "../src/include/fi.h"

// 学生结构体
typedef struct {
    int id;
    char name[50];
    int age;
    float gpa;
    char major[30];
} Student;

// 创建学生
Student* create_student(int id, const char* name, int age, float gpa, const char* major) {
    Student* student = (Student*)malloc(sizeof(Student));
    if (!student) return NULL;
    
    student->id = id;
    strncpy(student->name, name, sizeof(student->name) - 1);
    student->name[sizeof(student->name) - 1] = '\0';
    student->age = age;
    student->gpa = gpa;
    strncpy(student->major, major, sizeof(student->major) - 1);
    student->major[sizeof(student->major) - 1] = '\0';
    
    return student;
}

// 释放学生内存
void free_student(void* student) {
    if (student) {
        free(student);
    }
}

// 打印学生信息
void print_student(const Student* student) {
    printf("ID: %d, 姓名: %s, 年龄: %d, GPA: %.2f, 专业: %s\n",
           student->id, student->name, student->age, student->gpa, student->major);
}

// 打印学生信息（用于回调）
void print_student_callback(const void* key, const void* value, void* user_data) {
    (void)user_data;  // 避免未使用参数警告
    int id = *(int*)key;
    Student* student = *(Student**)value;
    printf("学生ID %d: %s, 年龄: %d, GPA: %.2f, 专业: %s\n",
           id, student->name, student->age, student->gpa, student->major);
}

// 查找GPA大于3.5的学生
bool find_high_gpa_students(const void* key, const void* value, void* user_data) {
    (void)key;        // 避免未使用参数警告
    (void)user_data;  // 避免未使用参数警告
    Student* student = *(Student**)value;
    return student->gpa > 3.5f;
}

// 查找特定专业的学生
bool find_major_students(const void* key, const void* value, void* user_data) {
    (void)key;  // 避免未使用参数警告
    Student* student = *(Student**)value;
    char* target_major = (char*)user_data;
    return strcmp(student->major, target_major) == 0;
}

// 按GPA排序的比较函数
int compare_students_by_gpa(const void* a, const void* b) {
    Student* student_a = *(Student**)a;
    Student* student_b = *(Student**)b;
    
    if (student_a->gpa > student_b->gpa) return -1;  // 降序排列
    if (student_a->gpa < student_b->gpa) return 1;
    return 0;
}

int main() {
    printf("=== 学生管理系统示例 ===\n\n");
    
    // 1. 创建学生映射表（ID -> Student*）
    printf("1. 创建学生映射表\n");
    fi_map* students_map = fi_map_create_with_destructors(
        10,                          // 初始容量
        sizeof(int),                 // 键大小（学生ID）
        sizeof(Student*),            // 值大小（学生指针）
        fi_map_hash_int32,           // 整数哈希函数
        fi_map_compare_int32,        // 整数比较函数
        NULL,                        // 键释放函数（ID不需要释放）
        free_student                 // 值释放函数（释放学生结构体）
    );
    
    if (!students_map) {
        fprintf(stderr, "无法创建学生映射表\n");
        return 1;
    }
    
    // 2. 添加学生
    printf("2. 添加学生\n");
    Student* students[] = {
        create_student(1001, "张三", 20, 3.8, "计算机科学"),
        create_student(1002, "李四", 19, 3.6, "数学"),
        create_student(1003, "王五", 21, 3.9, "计算机科学"),
        create_student(1004, "赵六", 20, 3.2, "物理学"),
        create_student(1005, "钱七", 22, 3.7, "数学"),
        create_student(1006, "孙八", 19, 3.5, "计算机科学"),
        create_student(1007, "周九", 21, 3.1, "化学"),
        create_student(1008, "吴十", 20, 3.8, "物理学")
    };
    
    for (int i = 0; i < 8; i++) {
        if (fi_map_put(students_map, &students[i]->id, &students[i]) != 0) {
            fprintf(stderr, "添加学生失败: %s\n", students[i]->name);
        }
    }
    
    printf("成功添加 %zu 名学生\n", fi_map_size(students_map));
    
    // 3. 显示所有学生
    printf("\n3. 显示所有学生\n");
    fi_map_for_each(students_map, print_student_callback, NULL);
    
    // 4. 查找特定学生
    printf("\n4. 查找特定学生\n");
    int search_id = 1003;
    Student** found_student = (Student**)malloc(sizeof(Student*));
    if (fi_map_get(students_map, &search_id, found_student) == 0) {
        printf("找到学生ID %d: ", search_id);
        print_student(*found_student);
    } else {
        printf("未找到学生ID %d\n", search_id);
    }
    free(found_student);
    
    // 5. 更新学生信息
    printf("\n5. 更新学生信息\n");
    int update_id = 1001;
    Student** student_to_update = (Student**)malloc(sizeof(Student*));
    if (fi_map_get(students_map, &update_id, student_to_update) == 0) {
        (*student_to_update)->gpa = 3.9f;  // 提高GPA
        printf("更新学生ID %d 的GPA为 %.2f\n", update_id, (*student_to_update)->gpa);
    }
    free(student_to_update);
    
    // 6. 查找高GPA学生
    printf("\n6. 查找GPA大于3.5的学生\n");
    fi_map* high_gpa_students = fi_map_filter(students_map, find_high_gpa_students, NULL);
    printf("找到 %zu 名高GPA学生:\n", fi_map_size(high_gpa_students));
    fi_map_for_each(high_gpa_students, print_student_callback, NULL);
    
    // 7. 按专业查找学生
    printf("\n7. 按专业查找学生\n");
    char target_major[] = "计算机科学";
    fi_map* cs_students = fi_map_filter(students_map, find_major_students, target_major);
    printf("计算机科学专业的学生 (%zu 名):\n", fi_map_size(cs_students));
    fi_map_for_each(cs_students, print_student_callback, NULL);
    
    // 8. 获取所有学生并排序
    printf("\n8. 按GPA排序所有学生\n");
    fi_array* all_students = fi_map_values(students_map);
    
    // 将指针数组转换为学生数组
    fi_array* student_array = fi_array_create(fi_array_count(all_students), sizeof(Student*));
    for (size_t i = 0; i < fi_array_count(all_students); i++) {
        Student** student_ptr = (Student**)fi_array_get(all_students, i);
        fi_array_push(student_array, student_ptr);
    }
    
    // 按GPA排序
    fi_array_sort(student_array, compare_students_by_gpa);
    
    printf("按GPA排序的学生列表:\n");
    for (size_t i = 0; i < fi_array_count(student_array); i++) {
        Student** student_ptr = (Student**)fi_array_get(student_array, i);
        printf("%zu. ", i + 1);
        print_student(*student_ptr);
    }
    
    // 9. 计算平均GPA
    printf("\n9. 计算平均GPA\n");
    double total_gpa = 0.0;
    fi_map_iterator iter = fi_map_iterator_create(students_map);
    while (fi_map_iterator_next(&iter)) {
        Student* student = *(Student**)fi_map_iterator_value(&iter);
        total_gpa += student->gpa;
    }
    fi_map_iterator_destroy(&iter);
    
    double average_gpa = total_gpa / fi_map_size(students_map);
    printf("所有学生的平均GPA: %.2f\n", average_gpa);
    
    // 10. 统计各专业学生数量
    printf("\n10. 统计各专业学生数量\n");
    fi_array* all_values = fi_map_values(students_map);
    fi_array* majors = fi_array_create(0, 30);  // 存储专业名称
    
    // 收集所有专业
    for (size_t i = 0; i < fi_array_count(all_values); i++) {
        Student* student = *(Student**)fi_array_get(all_values, i);
        bool found = false;
        
        // 检查专业是否已存在
        for (size_t j = 0; j < fi_array_count(majors); j++) {
            char* major = (char*)fi_array_get(majors, j);
            if (strcmp(major, student->major) == 0) {
                found = true;
                break;
            }
        }
        
        if (!found) {
            fi_array_push(majors, student->major);
        }
    }
    
    // 统计每个专业的学生数量
    for (size_t i = 0; i < fi_array_count(majors); i++) {
        char* major = (char*)fi_array_get(majors, i);
        fi_map* major_students = fi_map_filter(students_map, find_major_students, major);
        printf("%s: %zu 名学生\n", major, fi_map_size(major_students));
        fi_map_destroy(major_students);
    }
    
    // 11. 删除学生
    printf("\n11. 删除学生\n");
    int delete_id = 1004;
    if (fi_map_remove(students_map, &delete_id) == 0) {
        printf("成功删除学生ID %d\n", delete_id);
        printf("剩余学生数量: %zu\n", fi_map_size(students_map));
    } else {
        printf("删除学生ID %d 失败\n", delete_id);
    }
    
    // 12. 显示最终统计信息
    printf("\n12. 最终统计信息\n");
    printf("学生总数: %zu\n", fi_map_size(students_map));
    printf("映射负载因子: %.2f\n", fi_map_load_factor(students_map));
    printf("最大探测距离: %zu\n", fi_map_max_probe_distance(students_map));
    
    // 13. 清理资源
    printf("\n13. 清理资源\n");
    fi_map_destroy(students_map);
    fi_map_destroy(high_gpa_students);
    fi_map_destroy(cs_students);
    fi_array_destroy(all_students);
    fi_array_destroy(student_array);
    fi_array_destroy(all_values);
    fi_array_destroy(majors);
    
    printf("=== 示例完成 ===\n");
    return 0;
}
