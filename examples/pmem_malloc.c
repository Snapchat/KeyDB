/*
 * Copyright (c) 2015 - 2018 Intel Corporation
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
#include <sys/stat.h>

#define PMEM_MAX_SIZE (1024 * 1024 * 32)

static char *PMEM_DIR = "/tmp/";

int main(int argc, char *argv[])
{
    struct memkind *pmem_kind = NULL;
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
            "This example shows how to allocate memory and possibility to exceed pmem kind size."
            "\nPMEM kind directory: %s\n",
            PMEM_DIR);

    /* Create PMEM partition with specific size */
    err = memkind_create_pmem(PMEM_DIR, PMEM_MAX_SIZE, &pmem_kind);
    if (err) {
        perror("memkind_create_pmem()");
        fprintf(stderr, "Unable to create pmem partition err=%d errno=%d\n", err,
                errno);
        return errno ? -errno : 1;
    }

    char *pmem_str1 = NULL;
    char *pmem_str2 = NULL;
    char *pmem_str3 = NULL;
    char *pmem_str4 = NULL;

    // allocate 512 Bytes of 32 MB available
    pmem_str1 = (char *)memkind_malloc(pmem_kind, 512);
    if (pmem_str1 == NULL) {
        perror("memkind_malloc()");
        fprintf(stderr, "Unable to allocate pmem string (pmem_str1)\n");
        return errno ? -errno : 1;
    }

    // allocate 8 MB of 31.9 MB available
    pmem_str2 = (char *)memkind_malloc(pmem_kind, 8 * 1024 * 1024);
    if (pmem_str2 == NULL) {
        perror("memkind_malloc()");
        fprintf(stderr, "Unable to allocate pmem string (pmem_str11)\n");
        return errno ? -errno : 1;
    }

    // allocate 16 MB of 23.9 MB available
    pmem_str3 = (char *)memkind_malloc(pmem_kind, 16 * 1024 * 1024);
    if (pmem_str3 == NULL) {
        perror("memkind_malloc()");
        fprintf(stderr, "Unable to allocate pmem string (pmem_str12)\n");
        return errno ? -errno : 1;
    }

    // allocate 16 MB of 7.9 MB available -- Out Of Memory expected
    pmem_str4 = (char *)memkind_malloc(pmem_kind, 16 * 1024 * 1024);
    if (pmem_str4 != NULL) {
        perror("memkind_malloc()");
        fprintf(stderr,
                "Failure, this allocation should not be possible (expected result was NULL)\n");
        return errno ? -errno : 1;
    }

    sprintf(pmem_str1, "Hello world from pmem - pmem_str1\n");
    sprintf(pmem_str2, "Hello world from pmem - pmem_str2\n");
    sprintf(pmem_str3, "Hello world from persistent memory - pmem_str3\n");

    fprintf(stdout, "%s", pmem_str1);
    fprintf(stdout, "%s", pmem_str2);
    fprintf(stdout, "%s", pmem_str3);

    memkind_free(pmem_kind, pmem_str1);
    memkind_free(pmem_kind, pmem_str2);
    memkind_free(pmem_kind, pmem_str3);

    err = memkind_destroy_kind(pmem_kind);
    if (err) {
        perror("memkind_destroy_kind()");
        fprintf(stderr, "Unable to destroy pmem partition\n");
        return errno ? -errno : 1;
    }

    fprintf(stdout, "Memory was successfully allocated and released.\n");

    return 0;
}
