#include "fastlock.h"
#include <unistd.h>

thread_local int tls_pid = -1; 

extern "C" void fastlock_init(struct fastlock *lock)
{
    lock->m_lock = 0;
    lock->m_depth = 0;
}

extern "C" void fastlock_lock(struct fastlock *lock)
{
    while (!__sync_bool_compare_and_swap(&lock->m_lock, 0, 1))
    {
        if (lock->m_pidOwner == getpid())
        {
            ++lock->m_depth;
            return;
        }
    }
    lock->m_depth = 1;
    lock->m_pidOwner = getpid();
}

extern "C" void fastlock_unlock(struct fastlock *lock)
{
    --lock->m_depth;
    if (lock->m_depth == 0)
    {
        lock->m_pidOwner = -1;
        asm volatile ("": : :"memory");
        __sync_bool_compare_and_swap(&lock->m_lock, 1, 0);
    }
}

extern "C" void fastlock_free(struct fastlock *lock)
{
    // NOP
    (void)lock;
}
