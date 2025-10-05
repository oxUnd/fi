#include <stdio.h>
#include <stdlib.h>
#include "project.h"

void print_version(void) {
    printf("Version: %s\n", PROJECT_VERSION);
}

int main(int argc, char *argv[]) {
    printf("Hello from fi!\n");
    print_version();
    return 0;
}
