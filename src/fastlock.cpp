/* 
 * Copyright (c) 2019, John Sully <john at eqalpha dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "fastlock.h"
#include <unistd.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sched.h>
#include <atomic>
#include <assert.h>
#include <pthread.h>
#include <limits.h>
#include <linux/futex.h>
#include <string.h>

#ifdef __APPLE__
#include <TargetConditionals.h>
#ifdef TARGET_OS_MAC
/* The CLANG that ships with Mac OS doesn't have these builtins.
    but on x86 they are just normal reads/writes anyways */
#define __atomic_load_4(ptr, csq) (*(reinterpret_cast<const volatile uint32_t*>(ptr)))
#define __atomic_load_2(ptr, csq) (*(reinterpret_cast<const volatile uint16_t*>(ptr)))

#define __atomic_store_4(ptr, val, csq) (*(reinterpret_cast<volatile uint32_t*>(ptr)) = val)
#endif
#endif

/****************************************************
 *
 *      Implementation of a fair spinlock.  To promote fairness we
 *      use a ticket lock instead of a raw spinlock
 * 
 ****************************************************/

static_assert(sizeof(pid_t) <= sizeof(fastlock::m_pidOwner), "fastlock::m_pidOwner not large enough");
uint64_t g_longwaits = 0;

uint64_t fastlock_getlongwaitcount()
{
    return g_longwaits;
}

#ifndef ASM_SPINLOCK
static int futex(volatile unsigned *uaddr, int futex_op, int val,
    const struct timespec *timeout, int val3)
{
    return syscall(SYS_futex, uaddr, futex_op, val,
                    timeout, uaddr, val3);
}
#endif

extern "C" pid_t gettid()
{
    static thread_local int pidCache = -1;
#ifdef __linux__
    if (pidCache == -1)
        pidCache = syscall(SYS_gettid);
#else
	if (pidCache == -1) {
		uint64_t tidT;
		pthread_threadid_np(nullptr, &tidT);
		assert(tidT < UINT_MAX);
		pidCache = (int)tidT;
	}
#endif
    return pidCache;
}

extern "C" void fastlock_init(struct fastlock *lock)
{
    lock->m_ticket.m_active = 0;
    lock->m_ticket.m_avail = 0;
    lock->m_depth = 0;
    lock->m_pidOwner = -1;
    lock->futex = 0;
}

#ifndef ASM_SPINLOCK
extern "C" void fastlock_lock(struct fastlock *lock)
{
    if ((int)__atomic_load_4(&lock->m_pidOwner, __ATOMIC_ACQUIRE) == gettid())
    {
        ++lock->m_depth;
        return;
    }

    unsigned myticket = __atomic_fetch_add(&lock->m_ticket.m_avail, 1, __ATOMIC_RELEASE);
    unsigned mask = (1U << (myticket % 32));
    int cloops = 0;
    ticket ticketT;
    while (((ticketT.u = __atomic_load_4(&lock->m_ticket.m_active, __ATOMIC_ACQUIRE)) & 0xffff) != myticket)
    {
#if defined(__i386__) || defined(__amd64__)
        __asm__ ("pause");
#endif
        if ((++cloops % 1024*1024) == 0)
        {
            if (static_cast<uint16_t>(ticketT.m_active+1U) != myticket)
            {
                __atomic_fetch_or(&lock->futex, mask, __ATOMIC_ACQUIRE);
                futex(&lock->m_ticket.u, FUTEX_WAIT_BITSET_PRIVATE, ticketT.u, nullptr, mask);
                __atomic_fetch_and(&lock->futex, ~mask, __ATOMIC_RELEASE);
            }
            ++g_longwaits;
        }
    }

    lock->m_depth = 1;
    __atomic_store_4(&lock->m_pidOwner, gettid(), __ATOMIC_RELEASE);
    std::atomic_thread_fence(std::memory_order_acquire);
}

extern "C" int fastlock_trylock(struct fastlock *lock)
{
    if ((int)__atomic_load_4(&lock->m_pidOwner, __ATOMIC_ACQUIRE) == gettid())
    {
        ++lock->m_depth;
        return true;
    }

    // cheap test
    if (lock->m_ticket.m_active != lock->m_ticket.m_avail)
        return false;

    uint16_t active = __atomic_load_2(&lock->m_ticket.m_active, __ATOMIC_RELAXED);
    uint16_t next = active + 1;

    struct ticket ticket_expect { active, active };
    struct ticket ticket_setiflocked { active, next };
    if (__atomic_compare_exchange(&lock->m_ticket, &ticket_expect, &ticket_setiflocked, false /*weak*/, __ATOMIC_ACQUIRE, __ATOMIC_ACQUIRE))
    {
        lock->m_depth = 1;
        __atomic_store_4(&lock->m_pidOwner, gettid(), __ATOMIC_RELEASE);
        return true;
    }
    return false;
}

#define ROL32(v, shift) ((v << shift) | (v >> (32-shift)))
void unlock_futex(struct fastlock *lock, uint16_t ifutex)
{
    unsigned mask = (1U << (ifutex % 32));
    unsigned futexT = __atomic_load_4(&lock->futex, __ATOMIC_RELAXED) & mask;
    
    if (futexT == 0)
        return;
    
    while (__atomic_load_4(&lock->futex, __ATOMIC_ACQUIRE) & mask)
    {
        if (futex(&lock->m_ticket.u, FUTEX_WAKE_BITSET_PRIVATE, INT_MAX, nullptr, mask) == 1)
            break;
    }
}
extern "C" void fastlock_unlock(struct fastlock *lock)
{
    --lock->m_depth;
    if (lock->m_depth == 0)
    {
        assert((int)__atomic_load_4(&lock->m_pidOwner, __ATOMIC_RELAXED) >= 0);  // unlock after free
        lock->m_pidOwner = -1;
        std::atomic_thread_fence(std::memory_order_acquire);
        uint16_t activeNew = __atomic_add_fetch(&lock->m_ticket.m_active, 1, __ATOMIC_ACQ_REL);  // on x86 the atomic is not required here, but ASM handles that case
        unlock_futex(lock, activeNew);
    }
}
#endif

extern "C" void fastlock_free(struct fastlock *lock)
{
    // NOP
    assert((lock->m_ticket.m_active == lock->m_ticket.m_avail)                                        // Asser the lock is unlocked
        || (lock->m_pidOwner == gettid() && (lock->m_ticket.m_active == lock->m_ticket.m_avail-1)));  // OR we own the lock and nobody else is waiting
    lock->m_pidOwner = -2;  // sentinal value indicating free
}


bool fastlock::fOwnLock()
{
    return gettid() == m_pidOwner;
}
