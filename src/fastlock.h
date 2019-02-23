#pragma once
#include <inttypes.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Begin C API */
struct fastlock;
void fastlock_init(struct fastlock *lock);
void fastlock_lock(struct fastlock *lock);
int fastlock_trylock(struct fastlock *lock);
void fastlock_unlock(struct fastlock *lock);
void fastlock_free(struct fastlock *lock);

/* End C API */
#ifdef __cplusplus
}
#endif

struct ticket
{
    uint16_t m_active;
    uint16_t m_avail;
};
struct fastlock
{
    volatile struct ticket m_ticket;

    volatile int m_pidOwner;
    volatile int m_depth;

#ifdef __cplusplus
    fastlock()
    {
        fastlock_init(this);
    }

    void lock()
    {
        fastlock_lock(this);
    }

    bool try_lock()
    {
        return !!fastlock_trylock(this);
    }

    void unlock()
    {
        fastlock_unlock(this);
    }

    bool fOwnLock();   // true if this thread owns the lock, NOTE: not 100% reliable, use for debugging only
#endif
};
