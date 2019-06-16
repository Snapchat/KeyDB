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

uint64_t fastlock_getlongwaitcount();   // this is a global value

/* End C API */
#ifdef __cplusplus
}
#endif

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
struct ticket
{
    union
    {
        struct
        {
            uint16_t m_active;
            uint16_t m_avail;
        };
        unsigned u;
    };
};
#pragma GCC diagnostic pop

struct fastlock
{
    volatile struct ticket m_ticket;

    volatile int m_pidOwner;
    volatile int m_depth;
    unsigned futex;

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
