#pragma once

#ifdef __cplusplus
extern "C" {
#endif

struct fastlock
{
    int lock;
};

void fastlock_init(struct fastlock *lock);
void fastlock_lock(struct fastlock *lock);
void fastlock_unlock(struct fastlock *lock);
void fastlock_free(struct fastlock *lock);

#ifdef __cplusplus
}
#endif