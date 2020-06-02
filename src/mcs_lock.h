#pragma once
#include <atomic>
#include <stdio.h>
#include "serverassert.h"

#define MCS_UNLOCKED 0
#define MCS_LOCKED 1
#define MCS_FUTEX_LOCKED 2
class McsLock
{
    struct node *m_root = nullptr;
public:
    struct node 
    {
        char front_padding[64];
        struct node* pnext = nullptr;
        int depth = 0;
        unsigned locked = false;
        char back_padding[64];
    };

    void lock(node *pnode);
    bool try_lock(node *pnode, bool fWeak);
    void unlock(node *pnode);
};