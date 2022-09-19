// Some of this code is inspired by https://stackoverflow.com/a/10008252/1319998

#define _GNU_SOURCE
#include <dlfcn.h>
#include <execinfo.h>
#include <stddef.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

// Fails malloc at the fail_in-th call when search_string is in the backtrade
// -1 means never fail
static int fail_in = -1;
static char search_string[1024];

// To find the original address of malloc during malloc, we might
// dlsym will be called which might allocate memory via malloc
static char initialising_buffer[10240];
static int initialising_buffer_pos = 0;

// The pointers to original memory management functions to call
// when we don't want to fail
static void *(*original_malloc)(size_t) = NULL;
static void (*original_free)(void *ptr) = NULL;

void set_fail_in(int _fail_in, char *_search_string) {
    fail_in = _fail_in;
    strncpy(search_string, _search_string, sizeof(search_string));
}

void *
malloc(size_t size) {
    void *memory = NULL;
    int trace_size = 100;
    void *stack[trace_size];

    static int initialising = 0;
    static int level = 0;

    // Save original
    if (!original_malloc) {
        if (initialising) {
            if (size + initialising_buffer_pos >= sizeof(initialising_buffer)) {
                exit(1);
            }
            void *ptr = initialising_buffer + initialising_buffer_pos;
            initialising_buffer_pos += size;
            return ptr;
        }

        initialising = 1;
        original_malloc = dlsym(RTLD_NEXT, "malloc");
        original_free = dlsym(RTLD_NEXT, "free");
        initialising = 0;
    }

    // If we're in a nested malloc call (the backtrace functions below can call malloc)
    // then call the original malloc
    if (level) {
        return original_malloc(size);
    }
    ++level;

    if (fail_in == -1) {
        memory = original_malloc(size);
    } else {
         // Find if we're in the stack
        backtrace(stack, trace_size);
        char **symbols = backtrace_symbols(stack, trace_size);
        int found = 0;
        for (int i = 0; i < trace_size; ++i) {
            if (strstr(symbols[i], search_string) != NULL) {
                found = 1;
                break;
            }
        }
        free(symbols);

        if (!found) {
            memory = original_malloc(size);
        } else {
            if (fail_in > 0) {
                memory = original_malloc(size);
            }
            --fail_in;
        }
    }

    --level;
    return memory;
}

void free(void *ptr) {
    if (ptr < (void*) initialising_buffer || ptr > (void*)(initialising_buffer + sizeof(initialising_buffer))) {
        original_free(ptr);
    }
}
