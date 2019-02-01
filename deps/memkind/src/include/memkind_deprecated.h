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

/*
 * !!!!!!!!!!!!!!!!!!!
 * !!!   WARNING   !!!
 * !!! PLEASE READ !!!
 * !!!!!!!!!!!!!!!!!!!
 *
 * This header file contains all memkind deprecated symbols.
 *
 * Please avoid usage of this API in newly developed code, as
 * eventually this code is subject for removal.
 */

#pragma once
#ifdef __cplusplus
extern "C" {
#endif

#ifndef MEMKIND_DEPRECATED

#ifdef __GNUC__
#define MEMKIND_DEPRECATED(func) func __attribute__ ((deprecated))
#elif defined(_MSC_VER)
#define MEMKIND_DEPRECATED(func) __declspec(deprecated) func
#else
#pragma message("WARNING: You need to implement MEMKIND_DEPRECATED for this compiler")
#define MEMKIND_DEPRECATED(func) func
#endif

#endif

/*
 * Symbols related to GBTLB that are no longer supported
 */
extern memkind_t MEMKIND_HBW_GBTLB;
extern memkind_t MEMKIND_HBW_PREFERRED_GBTLB;
extern memkind_t MEMKIND_GBTLB;

int MEMKIND_DEPRECATED(memkind_get_kind_by_partition(int partition,
                                                     memkind_t *kind));

enum memkind_base_partition {
    MEMKIND_PARTITION_DEFAULT = 0,
    MEMKIND_PARTITION_HBW = 1,
    MEMKIND_PARTITION_HBW_HUGETLB = 2,
    MEMKIND_PARTITION_HBW_PREFERRED = 3,
    MEMKIND_PARTITION_HBW_PREFERRED_HUGETLB = 4,
    MEMKIND_PARTITION_HUGETLB = 5,
    MEMKIND_PARTITION_HBW_GBTLB = 6,
    MEMKIND_PARTITION_HBW_PREFERRED_GBTLB = 7,
    MEMKIND_PARTITION_GBTLB = 8,
    MEMKIND_PARTITION_HBW_INTERLEAVE = 9,
    MEMKIND_PARTITION_INTERLEAVE = 10,
    MEMKIND_PARTITION_REGULAR = 11,
    MEMKIND_PARTITION_HBW_ALL = 12,
    MEMKIND_PARTITION_HBW_ALL_HUGETLB = 13,
    MEMKIND_NUM_BASE_KIND
};

#ifdef __cplusplus
}
#endif
