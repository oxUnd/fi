# Programs to build
bin_PROGRAMS = fi btree_test

# Source files
fi_SOURCES = main.c fi_array.c
btree_test_SOURCES = btree_test.c fi_array.c fi_btree.c

# Compiler flags
fi_CFLAGS = -Wall -Wextra -std=c11 -g -I$(srcdir)/include
btree_test_CFLAGS = -Wall -Wextra -std=c11 -g -I$(srcdir)/include

# Custom build rule to put executable in build directory
all-local:
	@mkdir -p build
	@if [ -f fi$(EXEEXT) ]; then mv fi$(EXEEXT) build/; fi
	@if [ -f btree_test$(EXEEXT) ]; then mv btree_test$(EXEEXT) build/; fi

clean-local:
	@rm -rf build
