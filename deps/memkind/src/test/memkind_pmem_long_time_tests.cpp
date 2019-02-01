/*
 * Copyright (C) 2018 Intel Corporation.
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

#include <memkind.h>
#include "allocator_perf_tool/TimerSysTime.hpp"
#include "common.h"

#define STRESS_TIME (3*24*60*60)

extern const char  *PMEM_DIR;

static const size_t small_size[] = {8, 16, 32, 48, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320, 384,
                                    448, 512, 640, 768, 896, 1 * KB, 1280, 1536, 1792, 2 * KB, 2560, 3 * KB,
                                    3584, 4 * KB, 5 * KB, 6 * KB, 7 * KB, 8 * KB, 10 * KB, 12 * KB, 14 * KB
                                   };

static const size_t large_size[] = {16 * KB, 32 * KB, 20 * KB, 24 * KB, 28 * KB, 32 * KB, 40 * KB, 48 * KB,
                                    56 * KB, 64 * KB, 80 * KB, 96 * KB, 112 * KB, 128 * KB, 160 * KB, 192 * KB,
                                    224 * KB, 256 * KB, 320 * KB, 384 * KB, 448 * KB, 512 * KB, 640 * KB,
                                    768 * KB, 896 * KB, 1 * MB, 1280 * KB, 1536 * KB, 1792 * KB, 2 * MB,
                                    2560 * KB, 3 * MB, 3584 * KB, 4 * MB, 5 * MB, 6 * MB, 7 * MB, 8 * MB
                                   };

class MemkindPmemLongTimeStress: public :: testing::Test
{

protected:
    memkind_t pmem_kind;
    void SetUp()
    {
        int err = memkind_create_pmem(PMEM_DIR, 0, &pmem_kind);
        ASSERT_EQ(0, err);
        ASSERT_TRUE(nullptr != pmem_kind);
    }

    void TearDown()
    {
        int err = memkind_destroy_kind(pmem_kind);
        ASSERT_EQ(0, err);
    }
};

TEST_F(MemkindPmemLongTimeStress, DISABLED_test_TC_MEMKIND_PmemStressSmallSize)
{
    void *test = nullptr;
    TimerSysTime timer;
    timer.start();

    do {
        for (size_t i = 0; i < ARRAY_SIZE(small_size); i++) {
            test = memkind_malloc(pmem_kind, small_size[i]);
            ASSERT_TRUE(test != nullptr);
            memkind_free(pmem_kind, test);
        }
    } while (timer.getElapsedTime() < STRESS_TIME);
}

TEST_F(MemkindPmemLongTimeStress, DISABLED_test_TC_MEMKIND_PmemStressLargeSize)
{
    void *test = nullptr;
    TimerSysTime timer;
    timer.start();

    do {
        for (size_t i = 0; i < ARRAY_SIZE(large_size); i++) {
            test = memkind_malloc(pmem_kind, large_size[i]);
            ASSERT_TRUE(test != nullptr);
            memkind_free(pmem_kind, test);
        }
    } while (timer.getElapsedTime() < STRESS_TIME);
}

TEST_F(MemkindPmemLongTimeStress,
       DISABLED_test_TC_MEMKIND_PmemStressSmallAndLargeSize)
{
    void *test = nullptr;
    size_t i = 0, j = 0;
    TimerSysTime timer;
    timer.start();

    do {
        if (i < ARRAY_SIZE(small_size)) {
            test = memkind_malloc(pmem_kind, small_size[i]);
            ASSERT_TRUE(test != nullptr);
            memkind_free(pmem_kind, test);
            i++;
        } else
            i = 0;

        if (j < ARRAY_SIZE(large_size)) {
            test = memkind_malloc(pmem_kind, large_size[j]);
            ASSERT_TRUE(test != nullptr);
            memkind_free(pmem_kind, test);
            j++;
        } else
            j = 0;

    } while (timer.getElapsedTime() < STRESS_TIME);
}
