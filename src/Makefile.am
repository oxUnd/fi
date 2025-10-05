# Programs to build
bin_PROGRAMS = fi

# Source files
fi_SOURCES = main.c

# Compiler flags
fi_CFLAGS = -Wall -Wextra -std=c11 -g -I$(srcdir)/include

# Custom build rule to put executable in build directory
all-local:
	@mkdir -p build
	@if [ -f fi$(EXEEXT) ]; then mv fi$(EXEEXT) build/; fi

clean-local:
	@rm -rf build
