#include "fastlock.h"
#include <unistd.h>

extern "C" void fastlock_init(struct fastlock *lock)
{
    lock->m_lock = 0;
}

extern "C" void fastlock_lock(struct fastlock *lock)
{
    while (!__sync_bool_compare_and_swap(&lock->m_lock, 0, 1))
    {
    }
}

extern "C" void fastlock_unlock(struct fastlock *lock)
{
    __sync_bool_compare_and_swap(&lock->m_lock, 1, 0);
}

extern "C" void fastlock_free(struct fastlock *lock)
{
    // NOP
    (void)lock;
}