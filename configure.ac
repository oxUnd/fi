AC_PREREQ([2.69])
AC_INIT([fi], [1.0.0], [support@example.com])
AM_INIT_AUTOMAKE([-Wall -Werror foreign subdir-objects])
AC_CONFIG_SRCDIR([src/main.c])
AC_CONFIG_HEADERS([config.h])

# Checks for programs
AC_PROG_CC
AM_PROG_AR
LT_INIT

# Checks for header files
AC_CHECK_HEADERS([stdlib.h string.h])

# Checks for typedefs, structures, and compiler characteristics
AC_TYPE_SIZE_T

# Checks for library functions
AC_FUNC_MALLOC

# Check for libcheck
PKG_CHECK_MODULES([CHECK], [check >= 0.9.4], [
    AC_DEFINE([HAVE_CHECK], [1], [Define to 1 if you have libcheck])
    enable_tests=yes
], [
    enable_tests=no
    AC_MSG_WARN([libcheck not found, tests will be disabled])
])

AM_CONDITIONAL([ENABLE_TESTS], [test "x$enable_tests" = "xyes"])

AC_CONFIG_FILES([
    Makefile
    src/Makefile
    tests/Makefile
])

AC_OUTPUT
