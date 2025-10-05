#include <check.h>
#include <stdio.h>
#include <stdlib.h>

// Test: Example test case for fi_btree
START_TEST(test_example) {
    // TODO: Add your test implementation
    ck_assert_int_eq(1, 1);
}
END_TEST

// Create test suite
Suite *fi_btree_suite(void) {
    Suite *s;
    TCase *tc_core;
    
    s = suite_create("fi_btree");
    
    // Core test case
    tc_core = tcase_create("Core");
    tcase_add_test(tc_core, test_example);
    suite_add_tcase(s, tc_core);
    
    return s;
}

// Main function
int main(void) {
    int number_failed;
    Suite *s;
    SRunner *sr;
    
    s = fi_btree_suite();
    sr = srunner_create(s);
    
    // Run tests
    srunner_run_all(sr, CK_NORMAL);
    number_failed = srunner_ntests_failed(sr);
    srunner_free(sr);
    
    return (number_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}
