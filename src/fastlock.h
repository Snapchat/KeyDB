#pragma once

#ifdef __cplusplus
extern "C" {
#endif

/* Begin C API */
struct fastlock;
void fastlock_init(struct fastlock *lock);
void fastlock_lock(struct fastlock *lock);
void fastlock_unlock(struct fastlock *lock);
void fastlock_free(struct fastlock *lock);

/* End C API */
#ifdef __cplusplus
}
#endif

struct fastlock
{
    volatile int m_lock;
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

    void unlock()
    {
        fastlock_unlock(this);
    }

    bool fOwnLock();   // true if this thread owns the lock, NOTE: not 100% reliable, use for debugging only
#endif
};
