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

#include "common.h"
#include "random_sizes_allocator.h"
#include "proc_stat.h"
#include "allocator_perf_tool/GTestAdapter.hpp"
#include "allocator_perf_tool/Allocation_info.hpp"

#include <memkind.h>

#include <condition_variable>
#include <functional>
#include <mutex>
#include <random>
#include <thread>

class Worker
{
public:
    Worker(RandomSizesAllocator &&allocator, double malloc_probability)
        : allocator(std::move(allocator)), malloc_probability(malloc_probability)  {}

    void work()
    {
        if (allocator.empty() || get_random_bool(malloc_probability)) {
            requested_memory_sum += allocator.malloc_random_memory();
        } else {
            requested_memory_sum -= allocator.free_random_memory();
        }
    }

    size_t get_requested_memory_sum_bytes() const
    {
        return requested_memory_sum;
    }

private:
    bool get_random_bool(double probability)
    {
        std::bernoulli_distribution distribution(probability);
        return distribution(generator);
    }

    std::default_random_engine generator;
    RandomSizesAllocator allocator;
    size_t requested_memory_sum = 0;
    double malloc_probability;
};


class MemoryFootprintStats
{
public:
    void reset()
    {
        std::lock_guard<std::mutex> lk(sample_guard);
        initial_virtual_memory = proc_stat.get_virtual_memory_size_bytes();
        initial_physical_memory = proc_stat.get_physical_memory_size_bytes();
        vm_overhead_sum = 0;
        current_vm_overhead = 0;
        max_vm_overhead = 0;

        current_phys_overhead = 0;
        phys_overhead_sum = 0;
        max_phys_overhead = 0;

        requested_memory = 0;
        sample_count = 0;
    }

    void sample(long long requested_memory_bytes)
    {
        std::lock_guard<std::mutex> lk(sample_guard);
        current_vm = proc_stat.get_virtual_memory_size_bytes();
        current_phys = proc_stat.get_physical_memory_size_bytes();

        sample_count++;

        requested_memory = requested_memory_bytes;

        current_vm_overhead = current_vm - initial_virtual_memory - requested_memory;
        vm_overhead_sum += current_vm_overhead;
        max_vm_overhead = std::max(max_vm_overhead, current_vm_overhead);

        current_phys_overhead = current_phys - initial_physical_memory;
        phys_overhead_sum += current_phys_overhead;
        max_phys_overhead = std::max(max_phys_overhead, current_phys_overhead);
    }

    void log_data() const
    {
        std::lock_guard<std::mutex> lk(sample_guard);
        GTestAdapter::RecordProperty("avg_vm_overhead_per_operation_mb",
                                     convert_bytes_to_mb(vm_overhead_sum) / sample_count);
        GTestAdapter::RecordProperty("avg_vm_overhead_growth_per_operation_mb",
                                     convert_bytes_to_mb(current_vm_overhead) / sample_count);
        GTestAdapter::RecordProperty("max_vm_overhead_mb",
                                     convert_bytes_to_mb(max_vm_overhead));
        GTestAdapter::RecordProperty("avg_phys_overhead_per_operation_mb",
                                     convert_bytes_to_mb(phys_overhead_sum) / sample_count);
        GTestAdapter::RecordProperty("max_phys_overhead_mb",
                                     convert_bytes_to_mb(max_phys_overhead));
        GTestAdapter::RecordProperty("overhead_to_requested_memory_ratio_percent",
                                     100.f * current_vm_overhead / requested_memory);
        GTestAdapter::RecordProperty("requested_memory_mb",
                                     convert_bytes_to_mb(requested_memory));
    }
private:
    long long initial_virtual_memory;
    long long vm_overhead_sum = 0;
    long long current_vm_overhead = 0;
    long long max_vm_overhead = 0;

    long long initial_physical_memory;
    long long current_phys_overhead = 0;
    long long phys_overhead_sum = 0;
    long long max_phys_overhead = 0;

    long long requested_memory = 0;
    long long sample_count = 0;

    long long current_vm;
    long long current_phys;

    ProcStat proc_stat;
    mutable std::mutex sample_guard;
};

/* Execute func calling it n_calls times in n_threads threads.
 * The execution is multithreaded but the func calls order is sequential.
 * func takes thread id, and operation id as an argument,
 * and must return thread id of the next thread, where thread ids are in range <0, n_threads-1>.
 * init_thread_id specify the initial thread id.
 */
void run_multithreaded_seq_exec(unsigned n_threads, unsigned init_thread_id,
                                unsigned n_calls, std::function<unsigned(unsigned, unsigned)> func)
{
    std::vector<std::thread> threads;
    std::mutex mutex;
    std::condition_variable turns_holder;
    unsigned current_call = 0;
    unsigned current_tid = init_thread_id;

    threads.reserve(n_threads);

    mutex.lock();

    for(int tid=0; tid<n_threads; ++tid) {
        threads.emplace_back([ &, tid]() {
            while(current_call < n_calls) {
                std::unique_lock<std::mutex> lk(mutex);
                turns_holder.wait(lk, [ &,tid] {return current_tid == tid || current_call == n_calls;});
                if(current_call == n_calls) {
                    return;
                }
                current_tid = func(tid, current_call);
                ASSERT_LT(current_tid, n_threads) << "Incorrect thread id!";
                current_call++;
                lk.unlock();
                turns_holder.notify_all();
            }
        });
    }

    mutex.unlock();

    for(int i=0; i<threads.size(); i++) {
        threads[i].join();
    }
}

/*
 * Create threads and measure the cost of maintaining allocations from threads.
 * Allocations order is sequential (otherwise the results might be very nondeterministic).
 */
void run_test(memkind_t kind, size_t min_size, size_t max_size,
              unsigned n_threads, double malloc_probability=1.0, unsigned n_calls=1000)
{
    Worker worker(RandomSizesAllocator(kind, min_size, max_size, n_calls),
                  malloc_probability);

    MemoryFootprintStats mem_footprint_stats;

    auto func = [&](unsigned tid, unsigned id) -> unsigned {
        if(id == 0)
        {
            mem_footprint_stats.reset();
        }

        worker.work();
        mem_footprint_stats.sample(worker.get_requested_memory_sum_bytes());

        return (tid + 1) % n_threads; //next thread id
    };
    run_multithreaded_seq_exec(n_threads, 0, n_calls, func);
    mem_footprint_stats.log_data();
}

class MemoryFootprintTest: public :: testing::Test
{};

TEST_F(MemoryFootprintTest,
       test_TC_MEMKIND_DEFAULT_only_malloc_small_allocations_1_thread)
{
    run_test(MEMKIND_DEFAULT, 128, 15 * KB, 1);
}

TEST_F(MemoryFootprintTest,
       test_TC_MEMKIND_DEFAULT_only_malloc_small_allocations_10_thread)
{
    run_test(MEMKIND_DEFAULT, 128, 15 * KB, 10);
}

TEST_F(MemoryFootprintTest,
       test_TC_MEMKIND_DEFAULT_only_malloc_medium_allocations_1_thread)
{
    run_test(MEMKIND_DEFAULT, 16 * KB, 1 * MB, 1);
}

TEST_F(MemoryFootprintTest,
       test_TC_MEMKIND_DEFAULT_only_malloc_medium_allocations_10_thread)
{
    run_test(MEMKIND_DEFAULT, 16 * KB, 1 * MB, 10);
}

TEST_F(MemoryFootprintTest,
       test_TC_MEMKIND_DEFAULT_only_malloc_large_allocations_1_thread)
{
    run_test(MEMKIND_DEFAULT, 2 * MB, 10 * MB, 1, 1.0, 100);
}

TEST_F(MemoryFootprintTest,
       test_TC_MEMKIND_DEFAULT_only_malloc_large_allocations_10_thread)
{
    run_test(MEMKIND_DEFAULT, 2 * MB, 10 * MB, 10, 1.0, 100);
}

TEST_F(MemoryFootprintTest,
       test_TC_MEMKIND_DEFAULT_random_malloc80_free20_random_small_allocations_1_thread)
{
    run_test(MEMKIND_DEFAULT, 128, 15 * KB, 1, 0.8);
}

TEST_F(MemoryFootprintTest,
       test_TC_MEMKIND_DEFAULT_random_malloc80_free20_random_small_allocations_10_thread)
{
    run_test(MEMKIND_DEFAULT, 128, 15 * KB, 10, 0.8);
}

TEST_F(MemoryFootprintTest,
       test_TC_MEMKIND_DEFAULT_random_malloc80_free20_random_medium_allocations_1_thread)
{
    run_test(MEMKIND_DEFAULT, 16 * KB, 1 * MB, 1, 0.8);
}

TEST_F(MemoryFootprintTest,
       test_TC_MEMKIND_DEFAULT_random_malloc80_free20_random_large_allocations_10_thread)
{
    run_test(MEMKIND_DEFAULT, 2 * MB, 10 * MB,  10, 0.8, 100);
}

TEST_F(MemoryFootprintTest,
       test_TC_MEMKIND_HBW_only_malloc_small_allocations_1_thread)
{
    run_test(MEMKIND_HBW, 128, 15 * KB, 1);
}

TEST_F(MemoryFootprintTest,
       test_TC_MEMKIND_HBW_only_malloc_small_allocations_10_thread)
{
    run_test(MEMKIND_HBW, 128, 15 * KB, 10);
}

TEST_F(MemoryFootprintTest,
       test_TC_MEMKIND_HBW_only_malloc_medium_allocations_1_thread)
{
    run_test(MEMKIND_HBW, 16 * KB, 1 * MB, 1);
}

TEST_F(MemoryFootprintTest,
       test_TC_MEMKIND_HBW_only_malloc_medium_allocations_10_thread)
{
    run_test(MEMKIND_HBW, 16 * KB, 1 * MB, 10);
}

TEST_F(MemoryFootprintTest,
       test_TC_MEMKIND_HBW_only_malloc_large_allocations_1_thread)
{
    run_test(MEMKIND_HBW, 2 * MB, 10 * MB, 1, 100);
}

TEST_F(MemoryFootprintTest,
       test_TC_MEMKIND_HBW_only_malloc_large_allocations_10_thread)
{
    run_test(MEMKIND_HBW, 2 * MB, 10 * MB, 10, 100);
}

TEST_F(MemoryFootprintTest,
       test_TC_MEMKIND_HBW_random_malloc80_free20_random_small_allocations_1_thread)
{
    run_test(MEMKIND_HBW, 128, 15 * KB, 1, 0.8);
}

TEST_F(MemoryFootprintTest,
       test_TC_MEMKIND_HBW_random_malloc80_free20_random_small_allocations_10_thread)
{
    run_test(MEMKIND_HBW, 128, 15 * KB, 10, 0.8);
}

TEST_F(MemoryFootprintTest,
       test_TC_MEMKIND_HBW_random_malloc80_free20_random_medium_allocations_1_thread)
{
    run_test(MEMKIND_HBW, 16 * KB, 1 * MB, 1, 0.8);
}

TEST_F(MemoryFootprintTest,
       test_TC_MEMKIND_HBW_random_malloc80_free20_random_medium_allocations_10_thread)
{
    run_test(MEMKIND_HBW, 16 * KB, 1 * MB, 10, 0.8);
}

TEST_F(MemoryFootprintTest,
       test_TC_MEMKIND_HBW_random_malloc80_free20_random_large_allocations_1_thread)
{
    run_test(MEMKIND_HBW, 2 * MB, 10 * MB, 1, 0.8, 100);
}

TEST_F(MemoryFootprintTest,
       test_TC_MEMKIND_HBW_random_malloc80_free20_random_large_allocations_10_thread)
{
    run_test(MEMKIND_HBW, 2 * MB, 10 * MB, 10, 0.8, 100);
}
