/*
 * Copyright (c) 2018 Intel Corporation
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *
 *     * Neither the name of Intel Corporation nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY LOG OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include <memkind.h>

#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include <pthread.h>
#include <sys/stat.h>
#include <unistd.h>

#define PMEM_MAX_SIZE (1024 * 1024 * 32)
#define NUM_THREADS 10

static char *PMEM_DIR = "/tmp/";

void *thread_ind(void *arg);

static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t cond = PTHREAD_COND_INITIALIZER;

int main(int argc, char *argv[])
{
    int err = 0;
    struct stat st;

    if (argc > 2) {
        fprintf(stderr, "Usage: %s [pmem_kind_dir_path]", argv[0]);
        return 1;
    } else if (argc == 2) {
        if (stat(argv[1], &st) != 0 || !S_ISDIR(st.st_mode)) {
            fprintf(stderr, "%s : Invalid path to pmem kind directory", argv[1]);
            return 1;
        } else {
            PMEM_DIR = argv[1];
        }
    }

    fprintf(stdout,
            "This example shows how to use multithreading with independent pmem kinds."
            "\nPMEM kind directory: %s\n",
            PMEM_DIR);

    pthread_t pmem_threads[NUM_THREADS];
    int t;

    /* Lets create many independent threads */
    for (t = 0; t < NUM_THREADS; t++) {
        err = pthread_create(&pmem_threads[t], NULL, thread_ind, NULL);
        if (err) {
            fprintf(stderr, "Unable to create a thread\n");
            return 1;
        }
    }

    sleep(1);
    pthread_cond_broadcast(&cond);

    for (t = 0; t < NUM_THREADS; t++) {
        err = pthread_join(pmem_threads[t], NULL);
        if (err) {
            fprintf(stderr, "Thread join failed\n");
            return 1;
        }
    }

    fprintf(stdout, "Threads successfully allocated memory in the PMEM kinds.");

    return 0;
}

void *thread_ind(void *arg)
{
    struct memkind *pmem_kind;

    pthread_mutex_lock(&mutex);
    pthread_cond_wait(&cond, &mutex);
    pthread_mutex_unlock(&mutex);

    /* Create a pmem kind in thread */
    int err = memkind_create_pmem(PMEM_DIR, PMEM_MAX_SIZE, &pmem_kind);
    if (err) {
        perror("thread memkind_create_pmem()");
        fprintf(stderr, "Unable to create pmem partition err=%d errno=%d\n", err,
                errno);
        return NULL;
    }

    /* Alloc something */
    void *test = memkind_malloc(pmem_kind, 32);
    if (test == NULL) {
        perror("thread memkind_malloc()");
        fprintf(stderr, "Unable to allocate pmem (test)\n");
        return NULL;
    }

    /* Free resources */
    memkind_free(pmem_kind, test);

    /* And destroy pmem kind */
    err = memkind_destroy_kind(pmem_kind);
    if (err) {
        perror("thread memkind_pmem_destroy()");
        fprintf(stderr, "Unable to destroy pmem partition\n");
    }

    return NULL;
}
