/*
 * Copyright (C) 2014 - 2018 Intel Corporation.
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

#include "memkind.h"

#include <fstream>
#include <algorithm>
#include <numaif.h>
#include <memkind/internal/memkind_regular.h>

#include "common.h"
#include "check.h"
#include "omp.h"
#include "trial_generator.h"
#include "allocator_perf_tool/HugePageOrganizer.hpp"
#include "Allocator.hpp"
#include "TestPolicy.hpp"

#define TEST_PREFIX "test_TC_MEMKIND_BAT_"

typedef void(*test_function)(Allocator *, size_t);

typedef std::tuple<test_function, memkind_memtype_t, memkind_policy_t, memkind_bits_t, size_t>
memtype_policy_test_params;

test_function get_function(memtype_policy_test_params params)
{
    return std::get<0>(params);
}

memkind_memtype_t get_memtype(memtype_policy_test_params params)
{
    return std::get<1>(params);
}

memkind_policy_t get_policy(memtype_policy_test_params params)
{
    return std::get<2>(params);
}

memkind_bits_t get_flags(memtype_policy_test_params params)
{
    return std::get<3>(params);
}

size_t get_size(memtype_policy_test_params params)
{
    return std::get<4>(params);
}

typedef std::tuple<test_function, hbw_policy_t, size_t> hbw_policy_test_params;

test_function get_function(hbw_policy_test_params params)
{
    return std::get<0>(params);
}

hbw_policy_t get_hbw_policy(hbw_policy_test_params params)
{
    return std::get<1>(params);
}

size_t get_size(hbw_policy_test_params params)
{
    return std::get<2>(params);
}

typedef std::tuple<hbw_policy_t, size_t> hbw_policy_huge_page_test_params;

hbw_policy_t get_hbw_policy(hbw_policy_huge_page_test_params params)
{
    return std::get<0>(params);
}

size_t get_size(hbw_policy_huge_page_test_params params)
{
    return std::get<1>(params);
}



/*
 * Set of basic acceptance tests.
 */
class BATest: public TGTest {};
class BasicAllocTest
{
public:

    BasicAllocTest(Allocator *allocator) : allocator(allocator) {}

    void check_policy_and_numa_node(void *ptr, size_t size)
    {
        int policy = allocator->get_numa_policy();

        EXPECT_NE(-1, policy);

        if (allocator->is_high_bandwidth()) {
            TestPolicy::check_hbw_numa_nodes(policy, ptr, size);
        } else {
            TestPolicy::check_all_numa_nodes(policy, ptr, size);
        }
    }

    void record_page_association(void *ptr, size_t size)
    {
        TestPolicy::record_page_association(ptr, size, allocator->get_page_size());
    }

    void malloc(size_t size)
    {
        void *ptr = allocator->malloc(size);
        ASSERT_TRUE(ptr != NULL) << "malloc() returns NULL";
        void *memset_ret = memset(ptr, 3, size);
        EXPECT_TRUE(memset_ret != NULL);
        record_page_association(ptr, size);
        check_policy_and_numa_node(ptr, size);
        allocator->free(ptr);
    }

    void calloc(size_t num, size_t size)
    {
        void *ptr = allocator->calloc(num, size);
        ASSERT_TRUE(ptr != NULL) << "calloc() returns NULL";
        void *memset_ret = memset(ptr, 3, size);
        ASSERT_TRUE(memset_ret != NULL);
        record_page_association(ptr, size);
        check_policy_and_numa_node(ptr, size);
        allocator->free(ptr);
    }

    void realloc(size_t size)
    {
        void *ptr = allocator->malloc(size);
        ASSERT_TRUE(ptr != NULL) << "malloc() returns NULL";
        size_t realloc_size = size+128;
        ptr = allocator->realloc(ptr, realloc_size);
        ASSERT_TRUE(ptr != NULL) << "realloc() returns NULL";
        void *memset_ret = memset(ptr, 3, realloc_size);
        ASSERT_TRUE(memset_ret != NULL);
        record_page_association(ptr, size);
        check_policy_and_numa_node(ptr, size);
        allocator->free(ptr);
    }

    void memalign(size_t alignment, size_t size)
    {
        void *ptr = NULL;
        int ret = allocator->memalign(&ptr, alignment, size);
        ASSERT_EQ(0, ret) << "posix_memalign() != 0";
        ASSERT_TRUE(ptr != NULL) << "posix_memalign() returns NULL pointer";
        void *memset_ret = memset(ptr, 3, size);
        ASSERT_TRUE(memset_ret != NULL);
        record_page_association(ptr, size);
        check_policy_and_numa_node(ptr, size);
        allocator->free(ptr);
    }

    void free(size_t size)
    {
        void *ptr = allocator->malloc(size);
        ASSERT_TRUE(ptr != NULL) << "malloc() returns NULL";
        allocator->free(ptr);
    }

    virtual ~BasicAllocTest() {}
private:
    Allocator *allocator;
};


static void test_malloc(Allocator *allocator, size_t size)
{
    BasicAllocTest(allocator).malloc(size);
}

static void test_calloc(Allocator *allocator, size_t size)
{
    BasicAllocTest(allocator).calloc(1, size);
}

static void test_realloc(Allocator *allocator, size_t size)
{
    BasicAllocTest(allocator).realloc(size);
}

static void test_memalign(Allocator *allocator, size_t size)
{
    BasicAllocTest(allocator).memalign(4096, size);
}

static void test_free(Allocator *allocator, size_t size)
{
    BasicAllocTest(allocator).free(size);
}


template <typename T>
std::vector<T> GetKeys(std::map<T, std::string> dict)
{
    std::vector<T> keys;
    for (auto const &item: dict) {
        keys.push_back(item.first);
    }
    return keys;
}

struct TestParameters {
    static const std::map<test_function, std::string> functions;
    static const std::map<memkind_memtype_t, std::string> memtypes;
    static const std::map<memkind_policy_t, std::string> policies;
    static const std::map<hbw_policy_t, std::string> hbw_policies;
    static const std::vector<size_t> sizes;
};

const map<test_function, std::string> TestParameters::functions = {
    {&test_malloc, "malloc"},
    {&test_calloc, "calloc"},
    {&test_realloc, "realloc"},
    {&test_memalign, "memalign"},
    {&test_free, "free"}
};
const std::map<memkind_memtype_t, std::string> TestParameters::memtypes = {{MEMKIND_MEMTYPE_DEFAULT, "DEFAULT"},
    {MEMKIND_MEMTYPE_HIGH_BANDWIDTH, "HIGH_BANDWIDTH"}
};
const std::map<memkind_policy_t, std::string> TestParameters::policies = {{MEMKIND_POLICY_PREFERRED_LOCAL, "PREFERRED_LOCAL"},
    {MEMKIND_POLICY_BIND_LOCAL, "BIND_LOCAL"},
    {MEMKIND_POLICY_BIND_ALL, "BIND_ALL"},
    {MEMKIND_POLICY_INTERLEAVE_ALL, "MEMKIND_POLICY_INTERLEAVE_ALL"}
};
const std::map<hbw_policy_t, std::string> TestParameters::hbw_policies = {{HBW_POLICY_PREFERRED, "HBW_POLICY_PREFERRED"},
    {HBW_POLICY_BIND, "HBW_POLICY_BIND"},
    {HBW_POLICY_BIND_ALL, "HBW_POLICY_BIND_ALL"},
    {HBW_POLICY_INTERLEAVE, "HBW_POLICY_INTERLEAVE"}
};
const std::vector<size_t> TestParameters::sizes = {4096, 4194305};


class MemtypePolicyTest: public TGTest,
    public ::testing::WithParamInterface<memtype_policy_test_params>
{
public:
    static std::string get_test_name_suffix(
        testing::TestParamInfo<memtype_policy_test_params> info)
    {
        return TEST_PREFIX +
               TestParameters::functions.at(get_function(info.param)) + "_" +
               TestParameters::memtypes.at(get_memtype(info.param)) + "_" +
               TestParameters::policies.at(get_policy(info.param)) + "_" +
               (get_flags(info.param) == MEMKIND_MASK_PAGE_SIZE_2MB ? "PAGE_SIZE_2MB_" : "") +
               std::to_string(get_size(info.param)) + "_bytes";
    }
};

TEST_P(MemtypePolicyTest, memkind_heap_mgmt)
{
    HugePageOrganizer huge_page_organizer(8);
    auto params = GetParam();
    auto flags = get_flags(params);
    MemkindAllocator allocator(get_memtype(GetParam()), get_policy(GetParam()),
                               flags);
    get_function(GetParam())(&allocator, get_size(GetParam()));
}

INSTANTIATE_TEST_CASE_P(DEFAULT,
                        MemtypePolicyTest,
                        ::testing::Combine(::testing::ValuesIn(GetKeys(TestParameters::functions)),
                                           ::testing::Values(MEMKIND_MEMTYPE_DEFAULT),
                                           ::testing::Values(MEMKIND_POLICY_PREFERRED_LOCAL),
                                           ::testing::Values(MEMKIND_MASK_PAGE_SIZE_2MB, memkind_bits_t()),
                                           ::testing::ValuesIn(TestParameters::sizes)),
                        MemtypePolicyTest::get_test_name_suffix
                       );

INSTANTIATE_TEST_CASE_P(HBW,
                        MemtypePolicyTest,
                        ::testing::Combine(::testing::ValuesIn(GetKeys(TestParameters::functions)),
                                           ::testing::Values(MEMKIND_MEMTYPE_HIGH_BANDWIDTH),
                                           ::testing::ValuesIn(GetKeys(TestParameters::policies)),
                                           ::testing::Values(memkind_bits_t()),
                                           ::testing::ValuesIn(TestParameters::sizes)),
                        MemtypePolicyTest::get_test_name_suffix
                       );

INSTANTIATE_TEST_CASE_P(HBW_HUGE,
                        MemtypePolicyTest,
                        ::testing::Combine(::testing::ValuesIn(GetKeys(TestParameters::functions)),
                                           ::testing::Values(MEMKIND_MEMTYPE_HIGH_BANDWIDTH),
                                           ::testing::Values(MEMKIND_POLICY_PREFERRED_LOCAL,
                                                             MEMKIND_POLICY_BIND_LOCAL,
                                                             MEMKIND_POLICY_BIND_ALL),
                                           ::testing::Values(MEMKIND_MASK_PAGE_SIZE_2MB),
                                           ::testing::ValuesIn(TestParameters::sizes)),
                        MemtypePolicyTest::get_test_name_suffix
                       );


class HBWPolicyHugePageTest: public TGTest,
    public ::testing::WithParamInterface<hbw_policy_huge_page_test_params>
{
public:
    static std::string get_test_name_suffix(
        testing::TestParamInfo<hbw_policy_huge_page_test_params> info)
    {
        return TEST_PREFIX +
               TestParameters::hbw_policies.at(get_hbw_policy(info.param)) + "_" +
               std::to_string(get_size(info.param)) + "_bytes";
    }
};

TEST_P(HBWPolicyHugePageTest, memalign)
{
    HugePageOrganizer huge_page_organizer(8);
    HbwmallocAllocator hbwmalloc_allocator(get_hbw_policy(GetParam()));
    hbwmalloc_allocator.set_memalign_page_size(HBW_PAGESIZE_2MB);
    BasicAllocTest(&hbwmalloc_allocator).memalign(4096, get_size(GetParam()));
}

INSTANTIATE_TEST_CASE_P(HBW_memalign,
                        HBWPolicyHugePageTest,
                        ::testing::Combine(::testing::Values(HBW_POLICY_PREFERRED, HBW_POLICY_BIND,
                                                             HBW_POLICY_BIND_ALL),
                                           ::testing::ValuesIn(TestParameters::sizes)),
                        HBWPolicyHugePageTest::get_test_name_suffix
                       );


class HBWPolicyTest: public TGTest,
    public ::testing::WithParamInterface<hbw_policy_test_params>
{
public:
    static std::string get_test_name_suffix(
        testing::TestParamInfo<hbw_policy_test_params> info)
    {
        return TEST_PREFIX +
               TestParameters::functions.at(get_function(info.param)) + "_" +
               TestParameters::hbw_policies.at(get_hbw_policy(info.param)) + "_" +
               std::to_string(get_size(info.param)) + "_bytes";
    }
};

TEST_P(HBWPolicyTest, memkind_heap_mgmt)
{
    HbwmallocAllocator allocator(get_hbw_policy(GetParam()));
    get_function(GetParam())(&allocator, get_size(GetParam()));
}

INSTANTIATE_TEST_CASE_P(HBWPolicy,
                        HBWPolicyTest,
                        ::testing::Combine(::testing::ValuesIn(GetKeys(TestParameters::functions)),
                                           ::testing::ValuesIn(GetKeys(TestParameters::hbw_policies)),
                                           ::testing::ValuesIn(TestParameters::sizes)),
                        HBWPolicyTest::get_test_name_suffix
                       );


class MemkindRegularTest: public TGTest,
    public ::testing::WithParamInterface<tuple<test_function, size_t>>
{
public:
    static std::string get_test_name_suffix(
        testing::TestParamInfo<std::tuple<test_function, size_t>> info)
    {
        return TEST_PREFIX +
               TestParameters::functions.at(std::get<0>(info.param)) + "_" +
               std::to_string(std::get<1>(info.param)) + "_bytes";
    }
};


TEST_P(MemkindRegularTest, memkind_heap_mgmt)
{
    MemkindAllocator allocator(MEMKIND_REGULAR);
    std::get<0>(GetParam())(&allocator, std::get<1>(GetParam()));
}

INSTANTIATE_TEST_CASE_P(Regular,
                        MemkindRegularTest,
                        ::testing::Combine(::testing::ValuesIn(GetKeys(TestParameters::functions)),
                                           ::testing::ValuesIn(TestParameters::sizes)),
                        MemkindRegularTest::get_test_name_suffix
                       );


TEST_F(BATest,
       test_TC_MEMKIND_malloc_DEFAULT_HIGH_BANDWIDTH_INTERLEAVE_ALL_4194305_bytes)
{
    MemkindAllocator allocator((memkind_memtype_t)(MEMKIND_MEMTYPE_DEFAULT |
                                                   MEMKIND_MEMTYPE_HIGH_BANDWIDTH), MEMKIND_POLICY_INTERLEAVE_ALL,
                               memkind_bits_t());
    test_malloc(&allocator, 4194305);
}

TEST_F(BATest,
       test_TC_MEMKIND_free_MEMKIND_DEFAULT_free_with_NULL_kind_4096_bytes)
{
    void *ptr = memkind_malloc(MEMKIND_DEFAULT, 4096);
    ASSERT_TRUE(ptr != NULL) << "malloc() returns NULL";
    memkind_free(0, ptr);
}

TEST_F(BATest, test_TC_MEMKIND_free_ext_MEMKIND_GBTLB_4096_bytes)
{
    HugePageOrganizer huge_page_organizer(1000);
    MemkindAllocator memkind_allocator(MEMKIND_GBTLB);
    BasicAllocTest(&memkind_allocator).free(4096);
}

TEST_F(BATest, test_TC_MEMKIND_hbwmalloc_Pref_CheckAvailable)
{
    ASSERT_EQ(0, hbw_check_available());
}

TEST_F(BATest, test_TC_MEMKIND_hbwmalloc_Pref_Policy)
{
    hbw_set_policy(HBW_POLICY_PREFERRED);
    EXPECT_EQ(HBW_POLICY_PREFERRED, hbw_get_policy());
}

TEST_F(BATest, test_TC_MEMKIND_REGULAR_nodemask)
{
    using namespace TestPolicy;
    void *mem = memkind_malloc(MEMKIND_REGULAR, 1234567);
    unique_bitmask_ptr kind_nodemask = make_nodemask_ptr();
    ASSERT_EQ(0, memkind_regular_all_get_mbind_nodemask(MEMKIND_REGULAR,
                                                        kind_nodemask->maskp,
                                                        kind_nodemask->size));

    check_numa_nodes(kind_nodemask, MPOL_BIND, mem, 1234567);
    memkind_free(MEMKIND_REGULAR, mem);
}

