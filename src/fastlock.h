#pragma once
#include <inttypes.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Begin C API */
struct fastlock;
void fastlock_init(struct fastlock *lock, const char *name);
void fastlock_lock(struct fastlock *lock);
int fastlock_trylock(struct fastlock *lock, int fWeak);
void fastlock_unlock(struct fastlock *lock);
void fastlock_free(struct fastlock *lock);
int fastlock_unlock_recursive(struct fastlock *lock);
void fastlock_lock_recursive(struct fastlock *lock, int nesting);

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
    volatile int m_pidOwner;
    volatile int m_depth;
    char szName[56];
    /* Volatile data on seperate cache line */
    volatile struct ticket m_ticket;
    unsigned futex;
    char padding[56];   // ensure ticket and futex are on their own independent cache line

#ifdef __cplusplus
    fastlock(const char *name)
    {
        fastlock_init(this, name);
    }

    inline void lock()
    {
        fastlock_lock(this);
    }

    inline bool try_lock(bool fWeak = false)
    {
        return !!fastlock_trylock(this, fWeak);
    }

    inline void unlock()
    {
        fastlock_unlock(this);
    }

    int unlock_recursive()
    {
        return fastlock_unlock_recursive(this);
    }

    void lock_recursive(int nesting)
    {
        fastlock_lock_recursive(this, nesting);
    }

    bool fOwnLock();   // true if this thread owns the lock, NOTE: not 100% reliable, use for debugging only
#endif
};

static_assert(offsetof(struct fastlock, m_ticket) == 64, "ensure padding is correct");
