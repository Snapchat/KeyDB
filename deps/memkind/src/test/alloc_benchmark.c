/*
 * Copyright (C) 2016 - 2018 Intel Corporation.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 1. Redistributions of source code must retain the above copyright notice(s),
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice(s),
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDER(S) ``AS IS'' AND ANY EXPRESS
 * OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO
 * EVENT SHALL THE COPYRIGHT HOLDER(S) BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
 * OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 * ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/time.h>
#include <unistd.h>
#include <stdint.h>
#include <limits.h>
#ifdef _OPENMP
#include <omp.h>
#endif
#if defined(HBWMALLOC)
#include <hbwmalloc.h>
#define MALLOC_FN hbw_malloc
#define FREE_FN hbw_free
#elif defined (TBBMALLOC)
#include "tbbmalloc.h"
#define MALLOC_FN scalable_malloc
#define FREE_FN scalable_free
#elif defined (PMEMMALLOC)
#include <sys/stat.h>
#include "memkind.h"
#define MALLOC_FN(x) memkind_malloc(pmem_bench_kind, (x))
#define FREE_FN(x) memkind_free(pmem_bench_kind, (x))

static const size_t PMEM_PART_SIZE = 0;
static const char *PMEM_DIR = "/tmp/";
static memkind_t pmem_bench_kind;
#else
#define MALLOC_FN malloc
#define FREE_FN free
#endif

double ctimer(void);
void usage(char *name);

int main(int argc, char *argv[])
{
#ifdef _OPENMP
    int nthr = omp_get_max_threads();
#else
    int nthr = 1;
#endif
    long n, size;
    size_t alloc_size;
    unsigned long i;
    double dt, t_start, t_end, t_malloc, t_free, t_first_malloc, t_first_free,
           malloc_time = 0.0, free_time = 0.0, first_malloc_time, first_free_time;
    void *ptr;
#ifdef TBBMALLOC
    int ret;

    ret = load_tbbmalloc_symbols();
    if (ret) {
        printf("Error: TBB symbols not loaded (ret: %d)\n", ret);
        return EXIT_FAILURE;
    }
#endif
#ifdef PMEMMALLOC
    struct stat st;

    /* Handle command line arguments */
    if (argc == 3 || argc == 4) {
        n = atol(argv[1]);
        size = atol(argv[2]);

        if (argc == 4) {
            if (stat(argv[3], &st) != 0 || !S_ISDIR(st.st_mode)) {
                usage(argv[0]);
                return EXIT_FAILURE;
            } else {
                PMEM_DIR = argv[3];
            }
        }
    }
    if ((argc != 3 && argc != 4) || n < 0 || size < 0 || size > (LONG_MAX >> 10)) {
        usage(argv[0]);
        return EXIT_FAILURE;
    }

    int err = memkind_create_pmem(PMEM_DIR, PMEM_PART_SIZE, &pmem_bench_kind);
    if (err) {
        printf("Error: memkind_create_pmem failed %d\n", err);
        return EXIT_FAILURE;
    }
#else
    /* Handle command line arguments */
    if (argc == 3) {
        n = atol(argv[1]);
        size = atol(argv[2]);
    }
    if (argc != 3 || n < 0 || size < 0 || size > (LONG_MAX >> 10)) {
        usage(argv[0]);
        return EXIT_FAILURE;
    }
#endif
    alloc_size = (size_t) size * 1024;

    /* Get pagesize and compute page_mask */
    const size_t page_size = sysconf(_SC_PAGESIZE);
    const size_t page_mask = ~(page_size-1);

    /* Warm up */
    t_first_malloc = ctimer();
    ptr = MALLOC_FN(alloc_size);
    first_malloc_time = ctimer() - t_first_malloc;
    if (ptr == NULL) {
        printf("Error: first allocation failed\n");
        return EXIT_FAILURE;
    }
    t_first_free = ctimer();
    FREE_FN(ptr);
    first_free_time = ctimer() - t_first_free;
    ptr = NULL;

    t_start = ctimer();
    #pragma omp parallel private(i,t_malloc,t_free,ptr) reduction(max:malloc_time,free_time)
    {
        malloc_time = 0.0;
        free_time = 0.0;
        for (i=0; i<n; i++) {
            t_malloc = ctimer();
            ptr = (void *) MALLOC_FN(alloc_size);
            malloc_time += ctimer() - t_malloc;
            #pragma omp critical
            {
                if (ptr == NULL) {
                    printf("Error: allocation failed\n");
                    exit(EXIT_FAILURE);
                }
            }

            /* Make sure to touch every page */
            char *end = ptr + alloc_size;
            char *aligned_beg = (char *)((uintptr_t)ptr & page_mask);
            while(aligned_beg < end) {
                char *temp_ptr = (char *) aligned_beg;
                char value = temp_ptr[0];
                temp_ptr[0] = value;
                aligned_beg += page_size;
            }

            t_free = ctimer();
            FREE_FN(ptr);
            free_time += ctimer() - t_free;
            ptr = NULL;
        }
    }
    t_end = ctimer();
    dt = t_end - t_start;

    printf("%d %lu %8.6f %8.6f  %8.6f  %8.6f  %8.6f\n",
           nthr, size, dt/n, malloc_time/n, free_time/n, first_malloc_time,
           first_free_time);
#ifdef PMEMMALLOC
    err = memkind_destroy_kind(pmem_bench_kind);
    if (err) {
        printf("Error: memkind_destroy_kind failed %d\n", err);
        return EXIT_FAILURE;
    }
#endif
    return EXIT_SUCCESS;
}

void usage(char *name)
{
#ifdef PMEMMALLOC
    printf("Usage: %s <N> <SIZE> [DIR], where \n"
           "N is an number of repetitions \n"
           "SIZE is an allocation size in kbytes\n"
           "DIR is a custom path for PMEM kind, (default: \"/tmp/\")\n",
           name);
#else
    printf("Usage: %s <N> <SIZE>, where \n"
           "N is an number of repetitions \n"
           "SIZE is an allocation size in kbytes\n", name);
#endif
}

inline double ctimer()
{
    struct timeval tmr;
    gettimeofday(&tmr, NULL);
    /* Return time in ms */
    return (tmr.tv_sec + tmr.tv_usec/1000000.0)*1000;
}
