/*
* Copyright (C) 2017 - 2018 Intel Corporation.
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
#include <numa.h>
#include <hbwmalloc.h>
#include <memkind.h>
#include <vector>
#include <memory>
#include <gtest/gtest.h>
#include "allocator_perf_tool/Allocation_info.hpp"
#include "allocator_perf_tool/GTestAdapter.hpp"

typedef std::unique_ptr<void, void(*)(void *)> hbw_mem_ptr;


class HBWPreferredLocalityTest: public ::testing::Test
{
private:
    int find_closest_node(int node, const std::vector<int> &nodes)
    {
        int min_distance = 0;
        int closest_node = -1;
        for (int i = 0; i < nodes.size(); i++) {
            int distance = numa_distance(node, nodes[i]);
            if (distance && (distance < min_distance || min_distance == 0)) {
                min_distance = distance;
                closest_node = nodes[i];
            }
        }
        return closest_node;
    }

    bool pin_to_cpu(int cpu_id)
    {
        cpu_set_t cpu_set;
        CPU_ZERO(&cpu_set);
        CPU_SET(cpu_id, &cpu_set);
        return sched_setaffinity(0, sizeof(cpu_set_t), &cpu_set) != -1;
    }

    void check_ptr_numa(void *ptr, int expected_numa_id, int cpu_id, size_t size)
    {
        memset(ptr, 1, size);

        int numa_id = get_numa_node_id(ptr);
        EXPECT_EQ(numa_id, expected_numa_id);

        char property_name[50];
        snprintf(property_name, 50, "actual_numa_for_cpu_%d_expected_numa_%d", cpu_id,
                 expected_numa_id);
        GTestAdapter::RecordProperty(property_name, numa_id);
    }

public:
    void pin_memory_in_requesting_mem_thread(size_t size,
                                             const std::vector<int> &cpu_ids,
                                             const std::vector<int> &mcdram_nodes)
    {
        int threads_num = cpu_ids.size();
        int ret = hbw_set_policy(HBW_POLICY_PREFERRED);
        ASSERT_EQ(ret, 0);

        #pragma omp parallel for num_threads(threads_num)
        for (int i = 0; i < threads_num; i++) {
            if (!pin_to_cpu(cpu_ids[i])) {
                ADD_FAILURE();
                continue;
            }

            void *internal_ptr = hbw_malloc(size);
            if (!internal_ptr) {
                ADD_FAILURE();
                continue;
            }
            hbw_mem_ptr ptr(internal_ptr, hbw_free);
            int expected_numa_id = find_closest_node(numa_node_of_cpu(cpu_ids[i]),
                                                     mcdram_nodes);
            check_ptr_numa(ptr.get(), expected_numa_id, cpu_ids[i], size);
        }
    }

    void pin_memory_in_other_thread_than_requesting_mem(size_t size,
                                                        const std::vector<int> &cpu_ids,
                                                        const std::vector<int> &mcdram_nodes)
    {
        int threads_num = cpu_ids.size();
        int ret = hbw_set_policy(HBW_POLICY_PREFERRED);
        ASSERT_EQ(ret, 0);

        int main_thread_cpu_id = 0;
        int expected_numa_id = find_closest_node(main_thread_cpu_id, mcdram_nodes);
        ASSERT_TRUE(pin_to_cpu(main_thread_cpu_id));

        std::vector<hbw_mem_ptr> ptrs;
        for (int i = 0; i < threads_num; i++) {
            void *internal_ptr = hbw_malloc(size);
            ASSERT_TRUE(internal_ptr);
            ptrs.emplace_back(internal_ptr, hbw_free);
        }

        #pragma omp parallel for num_threads(threads_num)
        for (int i = 0; i < threads_num; i++) {
            if (!pin_to_cpu(cpu_ids[i])) {
                ADD_FAILURE();
                continue;
            }
            check_ptr_numa(ptrs[i].get(), expected_numa_id, cpu_ids[i], size);
        }
    }
};

TEST_F(HBWPreferredLocalityTest,
       test_TC_MEMKIND_KNL_SNC4_pin_memory_in_requesting_mem_thread_4_threads_100_bytes)
{
    pin_memory_in_requesting_mem_thread(100u, std::vector<int> {0, 18, 36, 54},
                                        std::vector<int> {4, 5, 6, 7});
}

TEST_F(HBWPreferredLocalityTest,
       test_TC_MEMKIND_KNL_SNC4_pin_memory_in_other_thread_than_requesting_mem_4_threads_100_bytes)
{
    pin_memory_in_other_thread_than_requesting_mem(100u, std::vector<int> {0, 18, 36, 54},
                                                   std::vector<int> {4, 5, 6, 7});
}

