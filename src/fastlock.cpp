#include "fastlock.h"
#include <unistd.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <sched.h>
#include <atomic>

static_assert(sizeof(pid_t) <= sizeof(fastlock::m_pidOwner), "fastlock::m_pidOwner not large enough");

static pid_t gettid()
{
    static thread_local int pidCache = -1;
    if (pidCache == -1)
        pidCache = syscall(SYS_gettid);
    return pidCache;
}

extern "C" void fastlock_init(struct fastlock *lock)
{
    lock->m_lock = 0;
    lock->m_depth = 0;
}

extern "C" void fastlock_lock(struct fastlock *lock)
{
    if (!__sync_bool_compare_and_swap(&lock->m_lock, 0, 1))
    {
        if (lock->m_pidOwner == gettid())
        {
            ++lock->m_depth;
            return;
        }

        while (!__sync_bool_compare_and_swap(&lock->m_lock, 0, 1))
        {
            sched_yield();
        }
    }

    lock->m_depth = 1;
    lock->m_pidOwner = gettid();
    __sync_synchronize();
}

extern "C" void fastlock_unlock(struct fastlock *lock)
{
    --lock->m_depth;
    if (lock->m_depth == 0)
    {
        lock->m_pidOwner = -1;
        __sync_synchronize();
        if (!__sync_bool_compare_and_swap(&lock->m_lock, 1, 0))
            *((volatile int*)0) = -1;
    }
}

extern "C" void fastlock_free(struct fastlock *lock)
{
    // NOP
    (void)lock;
}


bool fastlock::fOwnLock()
{
    if (__sync_bool_compare_and_swap(&m_lock, 0, 1))
    {
        __sync_bool_compare_and_swap(&m_lock, 1, 0);
        return false;   // it was never locked
    }
    return gettid() == m_pidOwner;
}