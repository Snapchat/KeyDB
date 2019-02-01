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

#include "pmem_allocator.h"
#include "common.h"
#include <vector>
#include <memory>
#include <array>
#include <list>
#include <map>
#include <utility>
#include <string>
#include <algorithm>
#include <scoped_allocator>
#include <thread>

extern const char *PMEM_DIR;

// Tests for pmem::allocator class.
class PmemAllocatorTests: public ::testing::Test
{
public:
    const size_t pmem_max_size = 1024*1024*1024;

    pmem::allocator<int> alloc_source { std::string(PMEM_DIR), pmem_max_size };

    pmem::allocator<int> alloc_source_f1 { std::string(PMEM_DIR), pmem_max_size };
    pmem::allocator<int> alloc_source_f2 { std::string(PMEM_DIR), pmem_max_size };

protected:
    void SetUp()
    {}

    void TearDown()
    {}
};


//Compare two same kind different type allocators
TEST_F(PmemAllocatorTests,
       test_TC_MEMKIND_AllocatorCompare_SameKindDifferentType_Test)
{
    pmem::allocator<int> alc1 { alloc_source_f1 };
    pmem::allocator<char> alc2 { alloc_source_f1 };
    ASSERT_TRUE(alc1 == alc2);
    ASSERT_FALSE(alc1 != alc2);
}

//Compare two same kind same type
TEST_F(PmemAllocatorTests,
       test_TC_MEMKIND_AllocatorCompare_SameKindSameType_Test)
{
    pmem::allocator<int> alc1 { alloc_source_f1 };
    pmem::allocator<int> alc2 { alloc_source_f1 };
    ASSERT_TRUE(alc1 == alc2);
    ASSERT_FALSE(alc1 != alc2);
}

//Compare two different kind different type
TEST_F(PmemAllocatorTests,
       test_TC_MEMKIND_AllocatorCompare_DifferentKindDifferentType_Test)
{
    pmem::allocator<int> alc1 { alloc_source_f1 };
    pmem::allocator<char> alc2 { alloc_source_f2 };
    ASSERT_FALSE(alc1 == alc2);
    ASSERT_TRUE(alc1 != alc2);
}

//Compare two different kind same type allocators
TEST_F(PmemAllocatorTests,
       test_TC_MEMKIND_AllocatorCompare_DifferentKindSameType_Test)
{
    pmem::allocator<int> alc1 { alloc_source_f1 };
    pmem::allocator<int> alc2 { alloc_source_f2 };
    ASSERT_FALSE(alc1 == alc2);
    ASSERT_TRUE(alc1 != alc2);
}

//Copy assignment test
TEST_F(PmemAllocatorTests, test_TC_MEMKIND_Allocator_CopyAssignment_Test)
{
    pmem::allocator<int> alc1 { alloc_source_f1 };
    pmem::allocator<int> alc2 { alloc_source_f2 };
    ASSERT_TRUE(alc1 != alc2);
    alc1 = alc2;
    ASSERT_TRUE(alc1 == alc2);
}

//Move constructor test
TEST_F(PmemAllocatorTests, test_TC_MEMKIND_Allocator_MoveConstructor_Test)
{
    pmem::allocator<int> alc1 { alloc_source_f2 };
    pmem::allocator<int> alc2 { alloc_source_f2 };
    ASSERT_TRUE(alc2 == alc1);
    pmem::allocator<int> alc3 = std::move(alc1);
    ASSERT_TRUE(alc2 == alc3);
}

//Move assignment test
TEST_F(PmemAllocatorTests, test_TC_MEMKIND_Allocator_MoveAssignment_Test)
{
    pmem::allocator<int> alc1 { alloc_source_f1 };
    pmem::allocator<int> alc2 { alloc_source_f2 };
    ASSERT_TRUE(alc1 != alc2);

    {
        pmem::allocator<int> alc3 { alc2 };
        alc1 = std::move(alc3);
    }
    ASSERT_TRUE(alc1 == alc2);
}

//Single allocation-deallocation test
TEST_F(PmemAllocatorTests,
       test_TC_MEMKIND_Allocator_SingleAllocationDeallocation_Test)
{
    pmem::allocator<int> alc1 { alloc_source_f1 };
    int *created_object = alc1.allocate(1);
    alc1.deallocate(created_object, 1);
}

//Shared cross-allocation-deallocation test
TEST_F(PmemAllocatorTests,
       test_TC_MEMKIND_Allocator_SharedAllocationDeallocation_Test)
{
    pmem::allocator<int> alc1 { alloc_source_f1 };
    pmem::allocator<int> alc2 { alloc_source_f1 };
    int *created_object = nullptr;
    int *created_object_2 = nullptr;
    created_object = alc1.allocate(1);
    ASSERT_TRUE(alc1 == alc2);
    alc2.deallocate(created_object, 1);
    created_object = alc2.allocate(1);
    alc1.deallocate(created_object, 1);

    created_object = alc1.allocate(1);
    created_object_2 = alc2.allocate(1);
    alc2.deallocate(created_object, 1);
    alc1.deallocate(created_object_2, 1);
}

//Construct-destroy test
TEST_F(PmemAllocatorTests, test_TC_MEMKIND_Allocator_ConstructDestroy_Test)
{
    pmem::allocator<int> alc1 { alloc_source_f1 };
    int *created_object = alc1.allocate(1);
    alc1.construct(created_object);
    alc1.destroy(created_object);
}

//Testing thread-safety support
TEST_F(PmemAllocatorTests, test_TC_MEMKIND_Allocator_MultithreadingSupport_Test)
{
    const size_t num_threads = 1000;
    const size_t iteration_count = 5000;

    std::vector<std::thread> thds;
    for (size_t i = 0; i < num_threads; ++i) {

        thds.push_back(std::thread( [&]() {
            std::vector<pmem::allocator<int>> allocators_local;
            for (size_t j = 0; j < iteration_count; j++) {
                allocators_local.push_back(pmem::allocator<int> ( alloc_source ));
            }
            for (size_t j = 0; j < iteration_count; j++) {
                allocators_local.pop_back();
            }
        }));
    }

    for (size_t i = 0; i < num_threads; ++i) {
        thds[i].join();
    }
}

//Test multiple memkind allocator usage
TEST_F(PmemAllocatorTests, test_TC_MEMKIND_MultipleAllocatorUsage_Test)
{
    {
        pmem::allocator<int> alloc { PMEM_DIR, pmem_max_size } ;
    }

    {
        pmem::allocator<int> alloc { PMEM_DIR, pmem_max_size } ;
    }
}

//Test vector
TEST_F(PmemAllocatorTests, test_TC_MEMKIND_AllocatorUsage_Vector_Test)
{
    pmem::allocator<int> alc{ alloc_source  };
    std::vector<int, pmem::allocator<int>> vector{ alc };

    const int num = 20;

    for (int i = 0; i < num; ++i) {
        vector.push_back(0xDEAD + i);
    }

    for (int i = 0; i < num; ++i) {
        ASSERT_TRUE(vector[i] == 0xDEAD + i);
    }
}

//Test list
TEST_F(PmemAllocatorTests, test_TC_MEMKIND_AllocatorUsage_List_Test)
{
    pmem::allocator<int> alc{ alloc_source };

    std::list<int, pmem::allocator<int>> list{ alc };

    const int num = 4;

    for (int i = 0; i < num; ++i) {
        list.emplace_back(0xBEAC011 + i);
        ASSERT_TRUE(list.back() == 0xBEAC011 + i);
    }

    for (int i = 0; i < num; ++i) {
        list.pop_back();
    }
}

//Test map
TEST_F(PmemAllocatorTests, test_TC_MEMKIND_AllocatorUsage_Map_Test)
{
    pmem::allocator<std::pair<const std::string, std::string>> alc{ alloc_source };

    std::map<std::string, std::string, std::less<std::string>, pmem::allocator<std::pair<const std::string, std::string>>>
    map{ std::less<std::string>(), alc };

    const int num = 10;

    for (int i = 0; i < num; ++i) {
        map[std::to_string(i)] = std::to_string(0x0CEA11 + i);
        ASSERT_TRUE(map[std::to_string(i)] == std::to_string(0x0CEA11 + i));
        map[std::to_string((i * 997 + 83) % 113)] = std::to_string(0x0CEA11 + i);
        ASSERT_TRUE(map[std::to_string((i * 997 + 83) % 113)] == std::to_string(
                        0x0CEA11 + i));
    }
}

#if _GLIBCXX_USE_CXX11_ABI
//Test vector of strings
TEST_F(PmemAllocatorTests, test_TC_MEMKIND_AllocatorUsage_VectorOfString_Test)
{
    typedef std::basic_string<char, std::char_traits<char>, pmem::allocator<char>>
                                                                                pmem_string;
    typedef pmem::allocator<pmem_string> pmem_alloc;

    pmem_alloc alc{ alloc_source };
    pmem::allocator<char> st_alc{alc};

    std::vector<pmem_string,std::scoped_allocator_adaptor<pmem_alloc>> vec{ alc };
    pmem_string arg{ "Very very loooong striiiing", st_alc };

    vec.push_back(arg);
    ASSERT_TRUE(vec.back() == arg);
}

//Test map of int strings
TEST_F(PmemAllocatorTests,
       test_TC_MEMKIND_AllocatorScopedUsage_MapOfIntString_Test)
{
    typedef std::basic_string<char, std::char_traits<char>, pmem::allocator<char>>
                                                                                pmem_string;
    typedef int key_t;
    typedef pmem_string value_t;
    typedef std::pair<key_t, value_t> target_pair;
    typedef pmem::allocator<target_pair> pmem_alloc;
    typedef pmem::allocator<char> str_allocator_t;
    typedef std::map<key_t, value_t, std::less<key_t>, std::scoped_allocator_adaptor<pmem_alloc>>
            map_t;

    pmem_alloc map_allocator( alloc_source );

    str_allocator_t str_allocator( map_allocator );

    value_t source_str1("Lorem ipsum dolor ", str_allocator);
    value_t source_str2("sit amet consectetuer adipiscing elit", str_allocator );

    map_t target_map{ std::scoped_allocator_adaptor<pmem_alloc>(map_allocator) };

    target_map[key_t(165)] = source_str1;
    ASSERT_TRUE(target_map[key_t(165)] == source_str1);
    target_map[key_t(165)] = source_str2;
    ASSERT_TRUE(target_map[key_t(165)] == source_str2);
}
#endif // _GLIBCXX_USE_CXX11_ABI
