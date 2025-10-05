if ENABLE_TESTS

# Check framework based tests
check_PROGRAMS = test_fi_array test_fi_btree

test_fi_btree_SOURCES = test_fi_btree.c
test_fi_btree_CFLAGS = -I$(top_srcdir)/src $(CHECK_CFLAGS) -Wall -Wextra -g
test_fi_btree_LDADD = $(CHECK_LIBS)
test_fi_array_SOURCES = test_fi_array.c
test_fi_array_CFLAGS = -I$(top_srcdir)/src $(CHECK_CFLAGS) -Wall -Wextra -g
test_fi_array_LDADD = $(CHECK_LIBS)
TESTS = test_fi_array test_fi_btree

endif
