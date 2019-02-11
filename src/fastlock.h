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
    int m_lock;

#ifdef __cplusplus
    void lock()
    {
        fastlock_lock(this);
    }

    void unlock()
    {
        fastlock_unlock(this);
    }
#endif
};
