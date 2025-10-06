# Library to build
lib_LTLIBRARIES = libfi.la
libfi_la_SOURCES = fi_array.c fi_btree.c fi_map.c
libfi_la_CFLAGS = -Wall -Wextra -std=c11 -g -I$(srcdir)/include
libfi_la_LDFLAGS = -version-info 1:0:0

# Programs to build
bin_PROGRAMS = fi btree_test string_btree_demo demo_map_test

# Source files
fi_SOURCES = main.c
fi_LDADD = libfi.la
btree_test_SOURCES = btree_test.c
btree_test_LDADD = libfi.la
string_btree_demo_SOURCES = simple_string_btree.c
string_btree_demo_LDADD = libfi.la
demo_map_test_SOURCES = demo_map_test.c
demo_map_test_LDADD = libfi.la

# Compiler flags
fi_CFLAGS = -Wall -Wextra -std=c11 -g -I$(srcdir)/include
btree_test_CFLAGS = -Wall -Wextra -std=c11 -g -I$(srcdir)/include
string_btree_demo_CFLAGS = -Wall -Wextra -std=c11 -g -I$(srcdir)/include
demo_map_test_CFLAGS = -Wall -Wextra -std=c11 -g -I$(srcdir)/include

# Custom build rule to put executable in build directory
all-local:
	@mkdir -p build
	@if [ -f fi$(EXEEXT) ]; then mv fi$(EXEEXT) build/; fi
	@if [ -f btree_test$(EXEEXT) ]; then mv btree_test$(EXEEXT) build/; fi
	@if [ -f string_btree_demo$(EXEEXT) ]; then mv string_btree_demo$(EXEEXT) build/; fi
	@if [ -f demo_map_test$(EXEEXT) ]; then mv demo_map_test$(EXEEXT) build/; fi

clean-local:
	@rm -rf build
