/* Maxmemory directive handling (LRU eviction and other policies).
 *
 * ----------------------------------------------------------------------------
 *
 * Copyright (c) 2009-2016, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "server.h"
#include "bio.h"
#include "atomicvar.h"
#include <mutex>
#include <map>
#include <math.h>

/* ----------------------------------------------------------------------------
 * Data structures
 * --------------------------------------------------------------------------*/

/* To improve the quality of the LRU approximation we take a set of keys
 * that are good candidate for eviction across performEvictions() calls.
 *
 * Entries inside the eviction pool are taken ordered by idle time, putting
 * greater idle times to the right (ascending order).
 *
 * When an LFU policy is used instead, a reverse frequency indication is used
 * instead of the idle time, so that we still evict by larger value (larger
 * inverse frequency means to evict keys with the least frequent accesses).
 *
 * Empty entries have the key pointer set to NULL. */
#define EVPOOL_SIZE 16
#define EVPOOL_CACHED_SDS_SIZE 255
struct evictionPoolEntry {
    unsigned long long idle;    /* Object idle time (inverse frequency for LFU) */
    sds key;                    /* Key name. */
    sds cached;                 /* Cached SDS object for key name. */
    int dbid;                   /* Key DB number. */
};

static struct evictionPoolEntry *EvictionPoolLRU;

/* ----------------------------------------------------------------------------
 * Implementation of eviction, aging and LRU
 * --------------------------------------------------------------------------*/

/* Return the LRU clock, based on the clock resolution. This is a time
 * in a reduced-bits format that can be used to set and check the
 * object->lru field of redisObject structures. */
unsigned int getLRUClock(void) {
    return (mstime()/LRU_CLOCK_RESOLUTION) & LRU_CLOCK_MAX;
}

/* This function is used to obtain the current LRU clock.
 * If the current resolution is lower than the frequency we refresh the
 * LRU clock (as it should be in production servers) we return the
 * precomputed value, otherwise we need to resort to a system call. */
unsigned int LRU_CLOCK(void) {
    unsigned int lruclock;
    if (1000/g_pserver->hz <= LRU_CLOCK_RESOLUTION) {
        lruclock = g_pserver->lruclock;
    } else {
        lruclock = getLRUClock();
    }
    return lruclock;
}

/* Given an object returns the min number of milliseconds the object was never
 * requested, using an approximated LRU algorithm. */
unsigned long long estimateObjectIdleTime(robj_roptr o) {
    unsigned long long lruclock = LRU_CLOCK();
    if (lruclock >= o->lru) {
        return (lruclock - o->lru) * LRU_CLOCK_RESOLUTION;
    } else {
        return (lruclock + (LRU_CLOCK_MAX - o->lru)) *
                    LRU_CLOCK_RESOLUTION;
    }
}

/* LRU approximation algorithm
 *
 * Redis uses an approximation of the LRU algorithm that runs in constant
 * memory. Every time there is a key to expire, we sample N keys (with
 * N very small, usually in around 5) to populate a pool of best keys to
 * evict of M keys (the pool size is defined by EVPOOL_SIZE).
 *
 * The N keys sampled are added in the pool of good keys to expire (the one
 * with an old access time) if they are better than one of the current keys
 * in the pool.
 *
 * After the pool is populated, the best key we have in the pool is expired.
 * However note that we don't remove keys from the pool when they are deleted
 * so the pool may contain keys that no longer exist.
 *
 * When we try to evict a key, and all the entries in the pool don't exist
 * we populate it again. This time we'll be sure that the pool has at least
 * one key that can be evicted, if there is at least one key that can be
 * evicted in the whole database. */

/* Create a new eviction pool. */
void evictionPoolAlloc(void) {
    struct evictionPoolEntry *ep;
    int j;

    ep = (evictionPoolEntry*)zmalloc(sizeof(*ep)*EVPOOL_SIZE, MALLOC_LOCAL);
    for (j = 0; j < EVPOOL_SIZE; j++) {
        ep[j].idle = 0;
        ep[j].key = NULL;
        ep[j].cached = sdsnewlen(NULL,EVPOOL_CACHED_SDS_SIZE);
        ep[j].dbid = 0;
    }
    EvictionPoolLRU = ep;
}

void processEvictionCandidate(int dbid, sds key, robj *o, const expireEntry *e, struct evictionPoolEntry *pool)
{
    unsigned long long idle;

    /* Calculate the idle time according to the policy. This is called
        * idle just because the code initially handled LRU, but is in fact
        * just a score where an higher score means better candidate. */
    if (g_pserver->maxmemory_policy & MAXMEMORY_FLAG_LRU) {
        idle = (o != nullptr) ? estimateObjectIdleTime(o) : 0;
    } else if (g_pserver->maxmemory_policy & MAXMEMORY_FLAG_LFU) {
        /* When we use an LRU policy, we sort the keys by idle time
            * so that we expire keys starting from greater idle time.
            * However when the policy is an LFU one, we have a frequency
            * estimation, and we want to evict keys with lower frequency
            * first. So inside the pool we put objects using the inverted
            * frequency subtracting the actual frequency to the maximum
            * frequency of 255. */
        idle = 255-LFUDecrAndReturn(o);
    } else if (g_pserver->maxmemory_policy == MAXMEMORY_VOLATILE_TTL) {
        /* In this case the sooner the expire the better. */
        idle = ULLONG_MAX - e->when();
    } else {
        serverPanic("Unknown eviction policy in evictionPoolPopulate()");
    }

    /* Insert the element inside the pool.
        * First, find the first empty bucket or the first populated
        * bucket that has an idle time smaller than our idle time. */
    int k = 0;
    while (k < EVPOOL_SIZE &&
            pool[k].key &&
            pool[k].idle < idle) k++;
    if (k == 0 && pool[EVPOOL_SIZE-1].key != NULL) {
        /* Can't insert if the element is < the worst element we have
            * and there are no empty buckets. */
        return;
    } else if (k < EVPOOL_SIZE && pool[k].key == NULL) {
        /* Inserting into empty position. No setup needed before insert. */
    } else {
        /* Inserting in the middle. Now k points to the first element
            * greater than the element to insert.  */
        if (pool[EVPOOL_SIZE-1].key == NULL) {
            /* Free space on the right? Insert at k shifting
                * all the elements from k to end to the right. */

            /* Save SDS before overwriting. */
            sds cached = pool[EVPOOL_SIZE-1].cached;
            memmove(pool+k+1,pool+k,
                sizeof(pool[0])*(EVPOOL_SIZE-k-1));
            pool[k].cached = cached;
        } else {
            /* No free space on right? Insert at k-1 */
            k--;
            /* Shift all elements on the left of k (included) to the
                * left, so we discard the element with smaller idle time. */
            sds cached = pool[0].cached; /* Save SDS before overwriting. */
            if (pool[0].key != pool[0].cached) sdsfree(pool[0].key);
            memmove(pool,pool+1,sizeof(pool[0])*k);
            pool[k].cached = cached;
        }
    }

    /* Try to reuse the cached SDS string allocated in the pool entry,
        * because allocating and deallocating this object is costly
        * (according to the profiler, not my fantasy. Remember:
        * premature optimization bla bla bla bla. */
    int klen = sdslen(key);
    if (klen > EVPOOL_CACHED_SDS_SIZE) {
        pool[k].key = sdsdup(key);
    } else {
        memcpy(pool[k].cached,key,klen+1);
        sdssetlen(pool[k].cached,klen);
        pool[k].key = pool[k].cached;
    }
    pool[k].idle = idle;
    pool[k].dbid = dbid;
}

/* This is an helper function for performEvictions(), it is used in order
 * to populate the evictionPool with a few entries every time we want to
 * expire a key. Keys with idle time bigger than one of the current
 * keys are added. Keys are always added if there are free entries.
 *
 * We insert keys on place in ascending order, so keys with the smaller
 * idle time are on the left, and keys with the higher idle time on the
 * right. */

struct visitFunctor
{
    int dbid;
    dict *dbdict;
    struct evictionPoolEntry *pool;
    int count = 0;
    int tries = 0;

    bool operator()(const expireEntry &e)
    {
        dictEntry *de = dictFind(dbdict, e.key());
        if (de != nullptr)
        {
            processEvictionCandidate(dbid, (sds)dictGetKey(de), (robj*)dictGetVal(de), &e, pool);
            ++count;
        }
        ++tries;
        return tries < g_pserver->maxmemory_samples;
    }
};
int evictionPoolPopulate(int dbid, redisDb *db, expireset *setexpire, struct evictionPoolEntry *pool)
{
    if (setexpire != nullptr)
    {
        std::unique_lock<fastlock> ul(g_expireLock);
        visitFunctor visitor { dbid, db->dictUnsafeKeyOnly(), pool, 0 };
        setexpire->random_visit(visitor);
        return visitor.count;
    }
    else
    {
        int returnCount = 0;
        dictEntry **samples = (dictEntry**)alloca(g_pserver->maxmemory_samples * sizeof(dictEntry*));
        int count = dictGetSomeKeys(db->dictUnsafeKeyOnly(),samples,g_pserver->maxmemory_samples);
        for (int j = 0; j < count; j++) {
            robj *o = (robj*)dictGetVal(samples[j]);
            // If the object is in second tier storage we don't need to evict it (since it alrady is)
            if (o != nullptr)
            {
                processEvictionCandidate(dbid, (sds)dictGetKey(samples[j]), o, nullptr, pool);
                ++returnCount;
            }
        }
        return returnCount;
    }
    return 0;
}

/* ----------------------------------------------------------------------------
 * LFU (Least Frequently Used) implementation.

 * We have 24 total bits of space in each object in order to implement
 * an LFU (Least Frequently Used) eviction policy, since we re-use the
 * LRU field for this purpose.
 *
 * We split the 24 bits into two fields:
 *
 *          16 bits      8 bits
 *     +----------------+--------+
 *     + Last decr time | LOG_C  |
 *     +----------------+--------+
 *
 * LOG_C is a logarithmic counter that provides an indication of the access
 * frequency. However this field must also be decremented otherwise what used
 * to be a frequently accessed key in the past, will remain ranked like that
 * forever, while we want the algorithm to adapt to access pattern changes.
 *
 * So the remaining 16 bits are used in order to store the "decrement time",
 * a reduced-precision Unix time (we take 16 bits of the time converted
 * in minutes since we don't care about wrapping around) where the LOG_C
 * counter is halved if it has an high value, or just decremented if it
 * has a low value.
 *
 * New keys don't start at zero, in order to have the ability to collect
 * some accesses before being trashed away, so they start at COUNTER_INIT_VAL.
 * The logarithmic increment performed on LOG_C takes care of COUNTER_INIT_VAL
 * when incrementing the key, so that keys starting at COUNTER_INIT_VAL
 * (or having a smaller value) have a very high chance of being incremented
 * on access.
 *
 * During decrement, the value of the logarithmic counter is halved if
 * its current value is greater than two times the COUNTER_INIT_VAL, otherwise
 * it is just decremented by one.
 * --------------------------------------------------------------------------*/

/* Return the current time in minutes, just taking the least significant
 * 16 bits. The returned time is suitable to be stored as LDT (last decrement
 * time) for the LFU implementation. */
unsigned long LFUGetTimeInMinutes(void) {
    return (g_pserver->unixtime/60) & 65535;
}

/* Given an object last access time, compute the minimum number of minutes
 * that elapsed since the last access. Handle overflow (ldt greater than
 * the current 16 bits minutes time) considering the time as wrapping
 * exactly once. */
unsigned long LFUTimeElapsed(unsigned long ldt) {
    unsigned long now = LFUGetTimeInMinutes();
    if (now >= ldt) return now-ldt;
    return 65535-ldt+now;
}

/* Logarithmically increment a counter. The greater is the current counter value
 * the less likely is that it gets really implemented. Saturate it at 255. */
uint8_t LFULogIncr(uint8_t counter) {
    if (counter == 255) return 255;
    double r = (double)rand()/RAND_MAX;
    double baseval = counter - LFU_INIT_VAL;
    if (baseval < 0) baseval = 0;
    double p = 1.0/(baseval*g_pserver->lfu_log_factor+1);
    if (r < p) counter++;
    return counter;
}

/* If the object decrement time is reached decrement the LFU counter but
 * do not update LFU fields of the object, we update the access time
 * and counter in an explicit way when the object is really accessed.
 * And we will times halve the counter according to the times of
 * elapsed time than g_pserver->lfu_decay_time.
 * Return the object frequency counter.
 *
 * This function is used in order to scan the dataset for the best object
 * to fit: as we check for the candidate, we incrementally decrement the
 * counter of the scanned objects if needed. */
unsigned long LFUDecrAndReturn(robj_roptr o) {
    unsigned long ldt = o->lru >> 8;
    unsigned long counter = o->lru & 255;
    unsigned long num_periods = g_pserver->lfu_decay_time ? LFUTimeElapsed(ldt) / g_pserver->lfu_decay_time : 0;
    if (num_periods)
        counter = (num_periods > counter) ? 0 : counter - num_periods;
    return counter;
}

unsigned long getClientReplicationBacklogSharedUsage(client *c);

/* We don't want to count AOF buffers and slaves output buffers as
 * used memory: the eviction should use mostly data size. This function
 * returns the sum of AOF and slaves buffer. */
size_t freeMemoryGetNotCountedMemory(void) {
    serverAssert(GlobalLocksAcquired());
    size_t overhead = 0;
    int slaves = listLength(g_pserver->slaves);

    if (slaves) {
        listIter li;
        listNode *ln;

        listRewind(g_pserver->slaves,&li);
        while((ln = listNext(&li))) {
            client *replica = (client*)listNodeValue(ln);
            std::unique_lock<fastlock>(replica->lock);
            /* we don't wish to multiple count the replication backlog shared usage */
            overhead += (getClientOutputBufferMemoryUsage(replica) - getClientReplicationBacklogSharedUsage(replica));
        }
    }

    /* also don't count the replication backlog memory
     * that's where the replication clients get their memory from */
    overhead += g_pserver->repl_backlog_size - g_pserver->repl_backlog_config_size;

    if (g_pserver->aof_state != AOF_OFF) {
        overhead += sdsalloc(g_pserver->aof_buf)+aofRewriteBufferSize();
    }
    return overhead;
}

/* Get the memory status from the point of view of the maxmemory directive:
 * if the memory used is under the maxmemory setting then C_OK is returned.
 * Otherwise, if we are over the memory limit, the function returns
 * C_ERR.
 *
 * The function may return additional info via reference, only if the
 * pointers to the respective arguments is not NULL. Certain fields are
 * populated only when C_ERR is returned:
 *
 *  'total'     total amount of bytes used.
 *              (Populated both for C_ERR and C_OK)
 *
 *  'logical'   the amount of memory used minus the slaves/AOF buffers.
 *              (Populated when C_ERR is returned)
 *
 *  'tofree'    the amount of memory that should be released
 *              in order to return back into the memory limits.
 *              (Populated when C_ERR is returned)
 *
 *  'level'     this usually ranges from 0 to 1, and reports the amount of
 *              memory currently used. May be > 1 if we are over the memory
 *              limit.
 *              (Populated both for C_ERR and C_OK)
 * 
 *  'reason'    the reason why the memory limit was exceeded
 *              EVICT_REASON_USER: reported user memory exceeded maxmemory
 *              EVICT_REASON_SYS: available system memory under configurable threshold 
 *              (Populated when C_ERR is returned)
 */
int getMaxmemoryState(size_t *total, size_t *logical, size_t *tofree, float *level, EvictReason *reason, bool fQuickCycle, bool fPreSnapshot) {
    size_t mem_reported, mem_used, mem_tofree;

    /* Check if we are over the memory usage limit. If we are not, no need
     * to subtract the slaves output buffers. We can just return ASAP. */
    mem_reported = zmalloc_used_memory();
    if (total) *total = mem_reported;
    size_t maxmemory = g_pserver->maxmemory;
    if (fPreSnapshot)
        maxmemory = static_cast<size_t>(maxmemory*0.9);   // derate memory by 10% since we won't be able to free during snapshot
    if (g_pserver->FRdbSaveInProgress() && !cserver.fForkBgSave)
        maxmemory = static_cast<size_t>(maxmemory*1.2);

    /* If available system memory is below a certain threshold, force eviction */
    long long sys_available_mem_buffer = 0;
    if (g_pserver->force_eviction_percent && g_pserver->cron_malloc_stats.sys_total) {
        float available_mem_ratio = (float)(100 - g_pserver->force_eviction_percent)/100;
        size_t min_available_mem = static_cast<size_t>(g_pserver->cron_malloc_stats.sys_total * available_mem_ratio);
        sys_available_mem_buffer = static_cast<long>(g_pserver->cron_malloc_stats.sys_available - min_available_mem);
        if (sys_available_mem_buffer < 0) {
            long long mem_threshold = mem_reported + sys_available_mem_buffer;
            maxmemory = ((long long)maxmemory < mem_threshold) ? maxmemory : static_cast<size_t>(mem_threshold);
        }
    }

    /* We may return ASAP if there is no need to compute the level. */
    int return_ok_asap = !maxmemory || mem_reported <= maxmemory;
    if (return_ok_asap && !level) return C_OK;

    /* Remove the size of slaves output buffers and AOF buffer from the
     * count of used memory. */
    mem_used = mem_reported;
    size_t overhead = freeMemoryGetNotCountedMemory();
    mem_used = (mem_used > overhead) ? mem_used-overhead : 0;

     /* If system available memory is too low, we want to force evictions no matter
     * what so we also offset the overhead from maxmemory. */
    if (sys_available_mem_buffer < 0) {
        maxmemory = (maxmemory > overhead) ? maxmemory-overhead : 0;
    }

    /* Compute the ratio of memory usage. */
    if (level) {
        if (!maxmemory) {
            *level = 0;
        } else {
            *level = (float)mem_used / (float)maxmemory;
        }
    }

    if (return_ok_asap) return C_OK;

    /* Check if we are still over the memory limit. */
    if (mem_used <= maxmemory) return C_OK;

    /* Compute how much memory we need to free. */
    mem_tofree = mem_used - maxmemory;
    if (g_pserver->m_pstorageFactory && !fQuickCycle)
    {
        mem_tofree += static_cast<size_t>(maxmemory * 0.05); // if we have a storage provider be much more aggressive
    }

    if (logical) *logical = mem_used;
    if (tofree) *tofree = mem_tofree;

    if (reason) *reason = sys_available_mem_buffer < 0 ? EvictReason::System : EvictReason::User;

    return C_ERR;
}

class FreeMemoryLazyFree : public ICollectable
{    
    ssize_t m_cb = 0;
    std::vector<std::pair<dict*, std::vector<dictEntry*>>> vecdictvecde;

public:
    static std::atomic<int> s_clazyFreesInProgress;

    FreeMemoryLazyFree() {
        s_clazyFreesInProgress++;
    }

    FreeMemoryLazyFree(const FreeMemoryLazyFree&) = delete;
    FreeMemoryLazyFree(FreeMemoryLazyFree&&) = default;

    ~FreeMemoryLazyFree() {
        aeAcquireLock();
        for (auto &pair : vecdictvecde) {
            for (auto de : pair.second) {
                dictFreeUnlinkedEntry(pair.first, de);
            }
            dictRelease(pair.first);
        }
        aeReleaseLock();
        --s_clazyFreesInProgress;
    }

    ssize_t addEntry(dict *d, dictEntry *de) {
        ssize_t cbFreedNow = 0;
        ssize_t cb = sizeof(dictEntry);
        cb += sdsAllocSize((sds)dictGetKey(de));
        robj *o = (robj*)dictGetVal(de);
        switch (o->type) {
        case OBJ_STRING:
            cb += getStringObjectSdsUsedMemory(o)+sizeof(robj);
            break;

        default:
            // If we don't know about it we can't accurately track the memory so free now
            cbFreedNow = zmalloc_used_memory();
            decrRefCount(o);
            cbFreedNow -= zmalloc_used_memory();
            de->v.val = nullptr;
        }

        auto itr = std::lower_bound(vecdictvecde.begin(), vecdictvecde.end(), d, 
            [](const std::pair<dict*, std::vector<dictEntry*>> &a, dict *d) -> bool {
                return a.first < d;
            }
        );
        if (itr == vecdictvecde.end() || itr->first != d) {
            itr = vecdictvecde.insert(itr, std::make_pair(d, std::vector<dictEntry*>()));
            __atomic_fetch_add(&d->refcount, 1, __ATOMIC_ACQ_REL);
        }
        serverAssert(itr->first == d);
        itr->second.push_back(de);
        m_cb += cb;
        return cb + cbFreedNow;
    }

    size_t memory_queued() { return m_cb; }
};

std::atomic<int> FreeMemoryLazyFree::s_clazyFreesInProgress {0};

/* Return 1 if used memory is more than maxmemory after allocating more memory,
 * return 0 if not. Redis may reject user's requests or evict some keys if used
 * memory exceeds maxmemory, especially, when we allocate huge memory at once. */
int overMaxmemoryAfterAlloc(size_t moremem) {
    if (!g_pserver->maxmemory) return  0; /* No limit. */

    /* Check quickly. */
    size_t mem_used = zmalloc_used_memory();
    if (mem_used + moremem <= g_pserver->maxmemory) return 0;

    size_t overhead = freeMemoryGetNotCountedMemory();
    mem_used = (mem_used > overhead) ? mem_used - overhead : 0;
    return mem_used + moremem > g_pserver->maxmemory;
}

/* The evictionTimeProc is started when "maxmemory" has been breached and
 * could not immediately be resolved.  This will spin the event loop with short
 * eviction cycles until the "maxmemory" condition has resolved or there are no
 * more evictable items.  */
static int isEvictionProcRunning = 0;
static int evictionTimeProc(
        struct aeEventLoop *eventLoop, long long id, void *clientData) {
    UNUSED(eventLoop);
    UNUSED(id);
    UNUSED(clientData);
    serverAssert(GlobalLocksAcquired());

    if (performEvictions((bool)clientData) == EVICT_RUNNING) return 0;  /* keep evicting */

    /* For EVICT_OK - things are good, no need to keep evicting.
     * For EVICT_FAIL - there is nothing left to evict.  */
    isEvictionProcRunning = 0;
    return AE_NOMORE;
}

/* Check if it's safe to perform evictions.
 *   Returns 1 if evictions can be performed
 *   Returns 0 if eviction processing should be skipped
 */
static int isSafeToPerformEvictions(void) {
    /* - There must be no script in timeout condition.
     * - Nor we are loading data right now.  */
    if (g_pserver->shutdown_asap || g_pserver->lua_timedout || g_pserver->loading) return 0;

    /* By default replicas should ignore maxmemory
     * and just be masters exact copies. */
    if (g_pserver->m_pstorageFactory == nullptr && listLength(g_pserver->masters) && g_pserver->repl_slave_ignore_maxmemory && !g_pserver->fActiveReplica) return 0;

    /* If we have a lazy free obj pending, our amounts will be off, wait for it to go away */
    if (FreeMemoryLazyFree::s_clazyFreesInProgress > 0) return 0;

    /* When clients are paused the dataset should be static not just from the
     * POV of clients not being able to write, but also from the POV of
     * expires and evictions of keys not being performed. */
    if (checkClientPauseTimeoutAndReturnIfPaused()) return 0;

    return 1;
}

/* Algorithm for converting tenacity (0-100) to a time limit.  */
static unsigned long evictionTimeLimitUs() {
    serverAssert(g_pserver->maxmemory_eviction_tenacity >= 0);
    serverAssert(g_pserver->maxmemory_eviction_tenacity <= 100);

    if (g_pserver->maxmemory_eviction_tenacity <= 10) {
        /* A linear progression from 0..500us */
        return 50uL * g_pserver->maxmemory_eviction_tenacity;
    }

    if (g_pserver->maxmemory_eviction_tenacity < 100) {
        /* A 15% geometric progression, resulting in a limit of ~2 min at tenacity==99  */
        return (unsigned long)(500.0 * pow(1.15, g_pserver->maxmemory_eviction_tenacity - 10.0));
    }

    return ULONG_MAX;   /* No limit to eviction time */
}

static void updateSysAvailableMemory() {
    if (g_pserver->force_eviction_percent) {
        g_pserver->cron_malloc_stats.sys_available = getMemAvailable();
    }
}

/* Check that memory usage is within the current "maxmemory" limit.  If over
 * "maxmemory", attempt to free memory by evicting data (if it's safe to do so).
 *
 * It's possible for Redis to suddenly be significantly over the "maxmemory"
 * setting.  This can happen if there is a large allocation (like a hash table
 * resize) or even if the "maxmemory" setting is manually adjusted.  Because of
 * this, it's important to evict for a managed period of time - otherwise Redis
 * would become unresponsive while evicting.
 *
 * The goal of this function is to improve the memory situation - not to
 * immediately resolve it.  In the case that some items have been evicted but
 * the "maxmemory" limit has not been achieved, an aeTimeProc will be started
 * which will continue to evict items until memory limits are achieved or
 * nothing more is evictable.
 *
 * This should be called before execution of commands.  If EVICT_FAIL is
 * returned, commands which will result in increased memory usage should be
 * rejected.
 *
 * Returns:
 *   EVICT_OK       - memory is OK or it's not possible to perform evictions now
 *   EVICT_RUNNING  - memory is over the limit, but eviction is still processing
 *   EVICT_FAIL     - memory is over the limit, and there's nothing to evict
 * */
int performEvictions(bool fPreSnapshot) {
    if (!isSafeToPerformEvictions()) return EVICT_OK;
    serverAssert(GlobalLocksAcquired());

    int keys_freed = 0;
    size_t mem_reported, mem_tofree;
    long long mem_freed; /* May be negative */
    mstime_t latency, eviction_latency;
    long long delta;
    int slaves = listLength(g_pserver->slaves);
    const bool fEvictToStorage = !cserver.delete_on_evict && g_pserver->db[0]->FStorageProvider();
    int result = EVICT_FAIL;
    int ckeysFailed = 0;
    EvictReason evictReason;

    std::unique_ptr<FreeMemoryLazyFree> splazy = std::make_unique<FreeMemoryLazyFree>();

    if (getMaxmemoryState(&mem_reported,NULL,&mem_tofree,NULL,&evictReason,false,fPreSnapshot) == C_OK)
        return EVICT_OK;

    if (g_pserver->maxmemory_policy == MAXMEMORY_NO_EVICTION)
        return EVICT_FAIL;  /* We need to free memory, but policy forbids. */

    unsigned long eviction_time_limit_us = evictionTimeLimitUs();

    mem_freed = 0;

    latencyStartMonitor(latency);

    monotime evictionTimer;
    elapsedStart(&evictionTimer);

    if (g_pserver->maxstorage && g_pserver->m_pstorageFactory != nullptr && g_pserver->m_pstorageFactory->totalDiskspaceUsed() >= g_pserver->maxstorage)
        goto cant_free_storage;

    while (mem_freed < (long long)mem_tofree) {
        int j, k, i;
        static unsigned int next_db = 0;
        sds bestkey = NULL;
        int bestdbid;
        redisDb *db;
        bool fFallback = false;
        
        if (g_pserver->maxmemory_policy & (MAXMEMORY_FLAG_LRU|MAXMEMORY_FLAG_LFU) ||
            g_pserver->maxmemory_policy == MAXMEMORY_VOLATILE_TTL)
        {
            struct evictionPoolEntry *pool = EvictionPoolLRU;

            while(bestkey == NULL) {
                unsigned long total_keys = 0, keys;

                /* We don't want to make local-db choices when expiring keys,
                 * so to start populate the eviction pool sampling keys from
                 * every DB. */
                for (i = 0; i < cserver.dbnum; i++) {
                    db = g_pserver->db[i];
                    if (g_pserver->maxmemory_policy & MAXMEMORY_FLAG_ALLKEYS)
                    {
                        if ((keys = db->size()) != 0) {
                            total_keys += evictionPoolPopulate(i, db, nullptr, pool);
                        }
                    }
                    else
                    {
                        keys = db->expireSize();
                        if (keys != 0)
                            total_keys += evictionPoolPopulate(i, db, db->setexpireUnsafe(), pool);
                    }
                }
                if (!total_keys) break; /* No keys to evict. */

                /* Go backward from best to worst element to evict. */
                for (k = EVPOOL_SIZE-1; k >= 0; k--) {
                    if (pool[k].key == NULL) {
                        continue;
                    } 
                    bestdbid = pool[k].dbid;
                    sds key = nullptr;

                    auto itr = g_pserver->db[pool[k].dbid]->find(pool[k].key);
                    if (itr != nullptr && (g_pserver->maxmemory_policy & MAXMEMORY_FLAG_ALLKEYS || itr.val()->FExpires()))
                        key = itr.key();

                    /* Remove the entry from the pool. */
                    if (pool[k].key != pool[k].cached)
                        sdsfree(pool[k].key);
                    pool[k].key = NULL;
                    pool[k].idle = 0;

                    /* If the key exists, is our pick. Otherwise it is
                     * a ghost and we need to try the next element. */
                    if (key) {
                        bestkey = key;
                        break;
                    } else {
                        /* Ghost... Iterate again. */
                    }
                }
            }
            if (bestkey == nullptr && fEvictToStorage)
                fFallback = true;
        }

        /* volatile-random and allkeys-random policy */
        if (g_pserver->maxmemory_policy == MAXMEMORY_ALLKEYS_RANDOM ||
                 g_pserver->maxmemory_policy == MAXMEMORY_VOLATILE_RANDOM
                 || fFallback)
        {
            /* When evicting a random key, we try to evict a key for
             * each DB, so we use the static 'next_db' variable to
             * incrementally visit all DBs. */
            for (i = 0; i < cserver.dbnum; i++) {
                j = (++next_db) % cserver.dbnum;
                db = g_pserver->db[j];
                if (g_pserver->maxmemory_policy == MAXMEMORY_ALLKEYS_RANDOM || fFallback)
                {
                    if (db->size() != 0) {
                        auto itr = db->random_cache_threadsafe(true /*fPrimaryOnly*/);  // primary only because we can't evict a snapshot key
                        bestkey = itr.key();
                        bestdbid = j;
                        break;
                    }
                }
                else
                {
                    if (db->expireSize())
                    {
                        bestkey = (sds)db->random_expire().key();
                        bestdbid = j;
                        break;
                    }
                }
            }
        }

        /* Finally remove the selected key. */
        if (bestkey) {
            db = g_pserver->db[bestdbid];

            if (fEvictToStorage)
            {
                // This key is in the storage so we only need to free the object
                dictEntry *deT;
                if (db->removeCachedValue(bestkey, &deT)) {
                    mem_freed += splazy->addEntry(db->dictUnsafeKeyOnly(), deT);
                    ckeysFailed = 0;
		    g_pserver->stat_evictedkeys++;
                }
                else {
                    delta = 0;
                    ckeysFailed++;
                    if (ckeysFailed > 1024)
                        goto cant_free;
                }
            }
            else
            {
                robj *keyobj = createStringObject(bestkey,sdslen(bestkey));
                propagateExpire(db,keyobj,g_pserver->lazyfree_lazy_eviction);
                /* We compute the amount of memory freed by db*Delete() alone.
                * It is possible that actually the memory needed to propagate
                * the DEL in AOF and replication link is greater than the one
                * we are freeing removing the key, but we can't account for
                * that otherwise we would never exit the loop.
                *
                * AOF and Output buffer memory will be freed eventually so
                * we only care about memory used by the key space. */
                delta = (long long) zmalloc_used_memory();
                latencyStartMonitor(eviction_latency);
                if (g_pserver->lazyfree_lazy_eviction)
                    dbAsyncDelete(db,keyobj);
                else
                    dbSyncDelete(db,keyobj);
                latencyEndMonitor(eviction_latency);
                latencyAddSampleIfNeeded("eviction-del",eviction_latency);
                delta -= (long long) zmalloc_used_memory();
                mem_freed += delta;
                g_pserver->stat_evictedkeys++;
                signalModifiedKey(NULL,db,keyobj);
                notifyKeyspaceEvent(NOTIFY_EVICTED, "evicted",
                    keyobj, db->id);
                decrRefCount(keyobj);
            }
            keys_freed++;

            if (keys_freed % 16 == 0) {
                /* When the memory to free starts to be big enough, we may
                 * start spending so much time here that is impossible to
                 * deliver data to the replicas fast enough, so we force the
                 * transmission here inside the loop. */
                if (slaves) flushSlavesOutputBuffers();

                /* Normally our stop condition is the ability to release
                 * a fixed, pre-computed amount of memory. However when we
                 * are deleting objects in another thread, it's better to
                 * check, from time to time, if we already reached our target
                 * memory, since the "mem_freed" amount is computed only
                 * across the dbAsyncDelete() call, while the thread can
                 * release the memory all the time. */
                if (g_pserver->lazyfree_lazy_eviction) {
                    if (evictReason == EvictReason::System) {
                        updateSysAvailableMemory();
                    }
                    if (getMaxmemoryState(NULL,NULL,NULL,NULL) == C_OK) {
                        break;
                    }
                }

                /* After some time, exit the loop early - even if memory limit
                 * hasn't been reached.  If we suddenly need to free a lot of
                 * memory, don't want to spend too much time here.  */
                if (g_pserver->m_pstorageFactory == nullptr && elapsedUs(evictionTimer) > eviction_time_limit_us) {
                    // We still need to free memory - start eviction timer proc
                    if (!isEvictionProcRunning && serverTL->el != nullptr) {
                        isEvictionProcRunning = 1;
                        aeCreateTimeEvent(serverTL->el, 0,
                                evictionTimeProc, (void*)fPreSnapshot, NULL);
                    }
                    break;
                }
            }
        } else {
            goto cant_free; /* nothing to free... */
        }
    }
    /* at this point, the memory is OK, or we have reached the time limit */
    result = (isEvictionProcRunning) ? EVICT_RUNNING : EVICT_OK;

    if (splazy != nullptr && splazy->memory_queued() > 0 && !serverTL->gcEpoch.isReset()) {
        g_pserver->garbageCollector.enqueue(serverTL->gcEpoch, std::move(splazy));
    } 

cant_free:
    if (mem_freed > 0 && evictReason == EvictReason::System) {
        updateSysAvailableMemory();
    }

    if (g_pserver->m_pstorageFactory)
    {
        if (mem_reported < g_pserver->maxmemory*1.2) {
            return EVICT_OK;    // Allow us to temporarily go over without OOMing
        }
    }

    if (!cserver.delete_on_evict && result == EVICT_FAIL)
    {
        for (int idb = 0; idb < cserver.dbnum; ++idb)
        {
            redisDb *db = g_pserver->db[idb];
            if (db->FStorageProvider())
            {
                if (db->size() != 0 && db->size(true /*fcachedOnly*/) == 0 && db->keycacheIsEnabled()) {
                    serverLog(LL_WARNING, "Key cache exceeds maxmemory, freeing - performance may be affected increase maxmemory if possible");
                    db->disableKeyCache();
                } else if (db->size(true /*fCachedOnly*/)) {
                    serverLog(LL_WARNING, "Failed to evict keys, falling back to flushing entire cache.  Consider increasing maxmemory-samples.");
                    db->removeAllCachedValues();
                    if (((mem_reported - zmalloc_used_memory()) + mem_freed) >= mem_tofree)
                        result = EVICT_OK;
                }
            }
        }
    }

    if (result == EVICT_FAIL) {
        /* At this point, we have run out of evictable items.  It's possible
         * that some items are being freed in the lazyfree thread.  Perform a
         * short wait here if such jobs exist, but don't wait long.  */
        if (bioPendingJobsOfType(BIO_LAZY_FREE)) {
            usleep(eviction_time_limit_us);
            if (getMaxmemoryState(NULL,NULL,NULL,NULL) == C_OK) {
                result = EVICT_OK;
            }
        }
    }

    latencyEndMonitor(latency);
    latencyAddSampleIfNeeded("eviction-cycle",latency);
cant_free_storage:
    return result;
}

