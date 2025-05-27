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

#include "fmacros.h"
#include "fastlock.h"
#include <unistd.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sched.h>
#include <atomic>
#include <assert.h>
#ifdef __FreeBSD__
#include <pthread_np.h>
#else
#include <pthread.h>
#endif
#include <limits.h>
#include <map>
#ifdef __linux__
#include <linux/futex.h>
#include <sys/sysinfo.h>
#endif
#include <string.h>
#include <stdarg.h>
#include <stdio.h>
#include "config.h"
#include "serverassert.h"

#if defined(__aarch64__) && defined(__linux__)
#include <sys/auxv.h>
#ifndef HWCAP_SB
#define HWCAP_SB (1 << 29)
#endif  // HWCAP_SB
#endif  // __aarch64__  __linux__

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

#ifndef UNUSED
#define UNUSED(x) ((void)x)
#endif

#ifdef HAVE_BACKTRACE
#include <ucontext.h>
__attribute__((weak)) void logStackTrace(void *, int) {
    printf("\tFailed to generate stack trace\n");
}
#endif

extern int g_fInCrash;
extern int g_fTestMode;
int g_fHighCpuPressure = false;

/****************************************************
 *
 *      Implementation of a fair spinlock.  To promote fairness we
 *      use a ticket lock instead of a raw spinlock
 * 
 ****************************************************/


#if !defined(__has_feature)
    #define __has_feature(x) 0
#endif

#ifdef __linux__
extern "C" void unlock_futex(struct fastlock *lock, uint16_t ifutex);
#endif

#if __has_feature(thread_sanitizer)

    /* Report that a lock has been created at address "lock". */
    #define ANNOTATE_RWLOCK_CREATE(lock) \
        AnnotateRWLockCreate(__FILE__, __LINE__, lock)

    /* Report that the lock at address "lock" is about to be destroyed. */
    #define ANNOTATE_RWLOCK_DESTROY(lock) \
        AnnotateRWLockDestroy(__FILE__, __LINE__, lock)

    /* Report that the lock at address "lock" has been acquired.
       is_w=1 for writer lock, is_w=0 for reader lock. */
    #define ANNOTATE_RWLOCK_ACQUIRED(lock, is_w) \
        AnnotateRWLockAcquired(__FILE__, __LINE__, lock, is_w)

    /* Report that the lock at address "lock" is about to be released. */
    #define ANNOTATE_RWLOCK_RELEASED(lock, is_w) \
      AnnotateRWLockReleased(__FILE__, __LINE__, lock, is_w)

    #if defined(DYNAMIC_ANNOTATIONS_WANT_ATTRIBUTE_WEAK)
        #if defined(__GNUC__)
            #define DYNAMIC_ANNOTATIONS_ATTRIBUTE_WEAK __attribute__((weak))
        #else
            /* TODO(glider): for Windows support we may want to change this macro in order
               to prepend __declspec(selectany) to the annotations' declarations. */
            #error weak annotations are not supported for your compiler
        #endif
    #else
        #define DYNAMIC_ANNOTATIONS_ATTRIBUTE_WEAK
    #endif

    extern "C" {
    void AnnotateRWLockCreate(
        const char *file, int line,
        const volatile void *lock) DYNAMIC_ANNOTATIONS_ATTRIBUTE_WEAK;
    void AnnotateRWLockDestroy(
        const char *file, int line,
        const volatile void *lock) DYNAMIC_ANNOTATIONS_ATTRIBUTE_WEAK;
    void AnnotateRWLockAcquired(
        const char *file, int line,
        const volatile void *lock, long is_w) DYNAMIC_ANNOTATIONS_ATTRIBUTE_WEAK;
    void AnnotateRWLockReleased(
        const char *file, int line,
        const volatile void *lock, long is_w) DYNAMIC_ANNOTATIONS_ATTRIBUTE_WEAK;
    }

#else

    #define ANNOTATE_RWLOCK_CREATE(lock)
    #define ANNOTATE_RWLOCK_DESTROY(lock)
    #define ANNOTATE_RWLOCK_ACQUIRED(lock, is_w)
    #define ANNOTATE_RWLOCK_RELEASED(lock, is_w)

#endif

extern "C"  __attribute__((weak)) void _serverPanic(const char * /*file*/, int /*line*/, const char * /*msg*/, ...)
{
    *((char*)-1) = 'x';
}

__attribute__((weak)) void serverLog(int , const char *fmt, ...)
{
    va_list args;
    va_start(args, fmt);
    vprintf(fmt, args);
    va_end(args);
    printf("\n");
}

extern "C" pid_t gettid()
{
    static thread_local int pidCache = -1;
#ifdef __linux__
    if (pidCache == -1)
        pidCache = syscall(SYS_gettid);
#else
	if (pidCache == -1) {
		uint64_t tidT;
#ifdef __FreeBSD__
// Check https://github.com/ClickHouse/ClickHouse/commit/8d51824ddcb604b6f179a0216f0d32ba5612bd2e
                tidT = pthread_getthreadid_np();
#else
		pthread_threadid_np(nullptr, &tidT);
#endif
		serverAssert(tidT < UINT_MAX);
		pidCache = (int)tidT;
	}
#endif
    return pidCache;
}

void printTrace()
{
#ifdef HAVE_BACKTRACE
    serverLog(3 /*LL_WARNING*/, "printing backtrace for thread %d", gettid());
    logStackTrace(nullptr, 1);
#endif
}


#ifdef __linux__
static int futex(volatile unsigned *uaddr, int futex_op, int val,
    const struct timespec *timeout, int val3)
{
    return syscall(SYS_futex, uaddr, futex_op, val,
                    timeout, uaddr, val3);
}
#endif

class DeadlockDetector
{
    fastlock m_lock { "deadlock detector" };    // destruct this first
    std::map<pid_t, fastlock *> m_mapwait;

public:
    void registerwait(fastlock *lock, pid_t thispid)
    {
        static volatile bool fInDeadlock = false;

        if (lock == &m_lock || g_fInCrash)
            return;
        fastlock_lock(&m_lock);
        
        if (fInDeadlock)
        {
            printTrace();
            fastlock_unlock(&m_lock);
            return;
        }

        m_mapwait.insert(std::make_pair(thispid, lock));

        // Detect cycles
        pid_t pidCheck = thispid;
        size_t cchecks = 0;
        for (;;)
        {
            auto itr = m_mapwait.find(pidCheck);
            if (itr == m_mapwait.end())
                break;

            __atomic_load(&itr->second->m_pidOwner, &pidCheck, __ATOMIC_RELAXED);
            if (pidCheck == thispid)
            {
                // Deadlock detected, printout some debugging info and crash
                serverLog(3 /*LL_WARNING*/, "\n\n");
                serverLog(3 /*LL_WARNING*/, "!!! ERROR: Deadlock detected !!!");
                pidCheck = thispid;
                for (;;)
                {
                    auto itr = m_mapwait.find(pidCheck);
                    serverLog(3 /* LL_WARNING */, "\t%d: (%p) %s", pidCheck, itr->second, itr->second->szName);
                    __atomic_load(&itr->second->m_pidOwner, &pidCheck, __ATOMIC_RELAXED);
                    if (pidCheck == thispid)
                        break;
                }
                // Wake All sleeping threads so they can print their callstacks
#ifdef HAVE_BACKTRACE
#ifdef __linux__
                int mask = -1;
                fInDeadlock = true;
                fastlock_unlock(&m_lock);
                futex(&lock->m_ticket.u, FUTEX_WAKE_BITSET_PRIVATE, INT_MAX, nullptr, mask);
                futex(&itr->second->m_ticket.u, FUTEX_WAKE_BITSET_PRIVATE, INT_MAX, nullptr, mask);
                sleep(2);
                fastlock_lock(&m_lock);
                printTrace();
#endif
#endif
                serverLog(3 /*LL_WARNING*/, "!!! KeyDB Will Now Crash !!!");
                _serverPanic(__FILE__, __LINE__, "Deadlock detected");
            }

            if (cchecks > m_mapwait.size())
                break;  // There is a cycle but we're not in it
            ++cchecks;
        }
        fastlock_unlock(&m_lock);
    }

    void clearwait(fastlock *lock, pid_t thispid)
    {
        if (lock == &m_lock || g_fInCrash)
            return;
        fastlock_lock(&m_lock);
        m_mapwait.erase(thispid);
        fastlock_unlock(&m_lock);
    }
};

DeadlockDetector g_dlock;

static_assert(sizeof(pid_t) <= sizeof(fastlock::m_pidOwner), "fastlock::m_pidOwner not large enough");
uint64_t g_longwaits = 0;

extern "C" void fastlock_panic(struct fastlock *lock)
{
    _serverPanic(__FILE__, __LINE__, "fastlock lock/unlock mismatch for: %s", lock->szName);
}

uint64_t fastlock_getlongwaitcount()
{
    uint64_t rval;
    __atomic_load(&g_longwaits, &rval, __ATOMIC_RELAXED);
    return rval;
}

extern "C" void fastlock_sleep(fastlock *lock, pid_t pid, unsigned wake, unsigned myticket)
{
    UNUSED(lock);
    UNUSED(pid);
    UNUSED(wake);
    UNUSED(myticket);
#ifdef __linux__
    g_dlock.registerwait(lock, pid);
    unsigned mask = (1U << (myticket % 32));
    __atomic_fetch_or(&lock->futex, mask, __ATOMIC_ACQUIRE);

    // double check the lock wasn't release between the last check and us setting the futex mask
    uint32_t u;
    __atomic_load(&lock->m_ticket.u, &u, __ATOMIC_ACQUIRE);
    if ((u & 0xffff) != myticket)
    {
        futex(&lock->m_ticket.u, FUTEX_WAIT_BITSET_PRIVATE, wake, nullptr, mask);
    }
    
    __atomic_fetch_and(&lock->futex, ~mask, __ATOMIC_RELEASE);
    g_dlock.clearwait(lock, pid);
#endif
    __atomic_fetch_add(&g_longwaits, 1, __ATOMIC_RELAXED);
}

extern "C" void fastlock_init(struct fastlock *lock, const char *name)
{
    lock->m_ticket.m_active = 0;
    lock->m_ticket.m_avail = 0;
    lock->m_depth = 0;
    lock->m_pidOwner = -1;
    lock->futex = 0;
    int cch = strlen(name);
    cch = std::min<int>(cch, sizeof(lock->szName)-1);
    memcpy(lock->szName, name, cch);
    lock->szName[cch] = '\0';
    ANNOTATE_RWLOCK_CREATE(lock);
}

#ifndef ASM_SPINLOCK
extern "C" void fastlock_lock(struct fastlock *lock, spin_worker worker)
{
    int pidOwner;
    __atomic_load(&lock->m_pidOwner, &pidOwner, __ATOMIC_ACQUIRE);
    if (pidOwner == gettid())
    {
        ++lock->m_depth;
        return;
    }

    int tid = gettid();
    unsigned myticket = __atomic_fetch_add(&lock->m_ticket.m_avail, 1, __ATOMIC_RELEASE);
    unsigned cloops = 0;
    ticket ticketT;
    int fHighPressure;
    __atomic_load(&g_fHighCpuPressure, &fHighPressure, __ATOMIC_RELAXED);
    unsigned loopLimit = fHighPressure ? 0x10000 : 0x100000;

    if (worker != nullptr) {
        for (;;) {
            __atomic_load(&lock->m_ticket.u, &ticketT.u, __ATOMIC_ACQUIRE);
            if ((ticketT.u & 0xffff) == myticket)
                break;
            if (!worker())
                goto LNormalLoop;
        }
    } else {
LNormalLoop:
        for (;;)
        {
            __atomic_load(&lock->m_ticket.u, &ticketT.u, __ATOMIC_ACQUIRE);
            if ((ticketT.u & 0xffff) == myticket)
                break;

#if defined(__i386__) || defined(__amd64__)
            __asm__ __volatile__ ("pause");
#elif defined(__aarch64__)
#ifdef __linux__
            static int use_spin_delay_sb = -1;

            // Use SB instruction if available otherwise ISB
            if (__builtin_expect(use_spin_delay_sb == 1, 1)) {
                __asm__ __volatile__(".inst 0xd50330ff");   // SB instruction encoding
            } else if (use_spin_delay_sb == 0) {
                __asm__ __volatile__("isb");
            } else {
                // Initialize variable and use getauxval fuction as delay
                if (getauxval(AT_HWCAP) & HWCAP_SB)
                use_spin_delay_sb = 1;
                else
                use_spin_delay_sb = 0;
            }
#else
            __asm__ __volatile__ ("isb");
#endif  // __linux__
#endif

            if ((++cloops % loopLimit) == 0)
            {
                fastlock_sleep(lock, tid, ticketT.u, myticket);
            }
        }
    }

    lock->m_depth = 1;
    __atomic_store(&lock->m_pidOwner, &tid, __ATOMIC_RELEASE);
    ANNOTATE_RWLOCK_ACQUIRED(lock, true);
    std::atomic_thread_fence(std::memory_order_acquire);
}

extern "C" int fastlock_trylock(struct fastlock *lock, int fWeak)
{
    int tid;
    __atomic_load(&lock->m_pidOwner, &tid, __ATOMIC_ACQUIRE);
    if (tid == gettid())
    {
        ++lock->m_depth;
        return true;
    }

    // cheap test
    struct ticket ticketT;
    __atomic_load(&lock->m_ticket.u, &ticketT.u, __ATOMIC_ACQUIRE);
    if (ticketT.m_active != ticketT.m_avail)
        return false;

    uint16_t active = ticketT.m_active;
    uint16_t next = active + 1;

    struct ticket ticket_expect { { { active, active } } };
    struct ticket ticket_setiflocked { { { active, next } } };
    if (__atomic_compare_exchange(&lock->m_ticket.u, &ticket_expect.u, &ticket_setiflocked.u, fWeak /*weak*/, __ATOMIC_ACQUIRE, __ATOMIC_RELAXED))
    {
        lock->m_depth = 1;
        tid = gettid();
        __atomic_store(&lock->m_pidOwner, &tid,  __ATOMIC_RELEASE);
        ANNOTATE_RWLOCK_ACQUIRED(lock, true);
        return true;
    }
    return false;
}

extern "C" void fastlock_unlock(struct fastlock *lock)
{
    --lock->m_depth;
    if (lock->m_depth == 0)
    {
        int pidT;
        __atomic_load(&lock->m_pidOwner, &pidT, __ATOMIC_RELAXED);
        serverAssert(pidT >= 0);  // unlock after free
        int t = -1;
        __atomic_store(&lock->m_pidOwner, &t, __ATOMIC_RELEASE);
        std::atomic_thread_fence(std::memory_order_acq_rel);
        ANNOTATE_RWLOCK_RELEASED(lock, true);
        uint16_t activeNew = __atomic_add_fetch(&lock->m_ticket.m_active, 1, __ATOMIC_RELEASE);  // on x86 the atomic is not required here, but ASM handles that case
#ifdef __linux__
        unlock_futex(lock, activeNew);
#else
		UNUSED(activeNew);
#endif
    }
}
#endif

#ifdef __linux__
#define ROL32(v, shift) ((v << shift) | (v >> (32-shift)))
extern "C" void unlock_futex(struct fastlock *lock, uint16_t ifutex)
{
    unsigned mask = (1U << (ifutex % 32));
    unsigned futexT;
    
    for (;;)
    {
        __atomic_load(&lock->futex, &futexT, __ATOMIC_ACQUIRE);
        futexT &= mask;
        if (!futexT)
            break;

        if (futex(&lock->m_ticket.u, FUTEX_WAKE_BITSET_PRIVATE, INT_MAX, nullptr, mask) == 1)
            break;
    }
}
#endif

extern "C" void fastlock_free(struct fastlock *lock)
{
    // NOP
    serverAssert((lock->m_ticket.m_active == lock->m_ticket.m_avail)                             // Assert the lock is unlocked
        || (lock->m_pidOwner == gettid() 
            && (lock->m_ticket.m_active == static_cast<uint16_t>(lock->m_ticket.m_avail-1U))));  // OR we own the lock and nobody else is waiting
    lock->m_pidOwner = -2;  // sentinal value indicating free
    ANNOTATE_RWLOCK_DESTROY(lock);
}


bool fastlock::fOwnLock()
{
    int tid;
    __atomic_load(&m_pidOwner, &tid, __ATOMIC_RELAXED);
    return gettid() == tid;
}

int fastlock_unlock_recursive(struct fastlock *lock)
{
    int rval = lock->m_depth;
    lock->m_depth = 1;
    fastlock_unlock(lock);
    return rval;
}

void fastlock_lock_recursive(struct fastlock *lock, int nesting)
{
    fastlock_lock(lock);
    lock->m_depth = nesting;
}

void fastlock_auto_adjust_waits()
{
#ifdef __linux__
    struct sysinfo sysinf;
    int fHighPressurePrev, fHighPressureNew;
    __atomic_load(&g_fHighCpuPressure, &fHighPressurePrev, __ATOMIC_RELAXED);
    fHighPressureNew = fHighPressurePrev;
    memset(&sysinf, 0, sizeof sysinf);
    if (!sysinfo(&sysinf)) {
        auto avgCoreLoad = sysinf.loads[0] / get_nprocs();
        int fHighPressureNew = (avgCoreLoad > ((1 << SI_LOAD_SHIFT) * 0.9));
        __atomic_store(&g_fHighCpuPressure, &fHighPressureNew, __ATOMIC_RELEASE);
        if (fHighPressureNew)
            serverLog(!fHighPressurePrev ?  3 /*LL_WARNING*/ : 1 /* LL_VERBOSE */, "NOTICE: Detuning locks due to high load per core: %.2f%%", avgCoreLoad / (double)(1 << SI_LOAD_SHIFT)*100.0);
    }

    if (!fHighPressureNew && fHighPressurePrev) {
        serverLog(3 /*LL_WARNING*/, "NOTICE: CPU pressure reduced");
    }
#else
    g_fHighCpuPressure = g_fTestMode;
#endif
}
