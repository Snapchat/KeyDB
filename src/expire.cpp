/* Implementation of EXPIRE (keys with fixed time to live).
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
#include "cron.h"

/* Helper function for the activeExpireCycle() function.
 * This function will try to expire the key that is stored in the hash table
 * entry 'de' of the 'expires' hash table of a Redis database.
 *
 * If the key is found to be expired, it is removed from the database and
 * 1 is returned. Otherwise no operation is performed and 0 is returned.
 *
 * When a key is expired, g_pserver->stat_expiredkeys is incremented.
 *
 * The parameter 'now' is the current time in milliseconds as is passed
 * to the function to avoid too many gettimeofday() syscalls. */
void activeExpireCycleExpireFullKey(redisDb *db, const char *key) {
    robj *keyobj = createStringObject(key,sdslen(key));
    mstime_t expire_latency;

    propagateExpire(db,keyobj,g_pserver->lazyfree_lazy_expire);
    latencyStartMonitor(expire_latency);
    if (g_pserver->lazyfree_lazy_expire)
        dbAsyncDelete(db,keyobj);
    else
        dbSyncDelete(db,keyobj);
    latencyEndMonitor(expire_latency);
    latencyAddSampleIfNeeded("expire-del",expire_latency);
    notifyKeyspaceEvent(NOTIFY_EXPIRED,
        "expired",keyobj,db->id);
    signalModifiedKey(NULL, db, keyobj);
    decrRefCount(keyobj);
    g_pserver->stat_expiredkeys++;
}

/*-----------------------------------------------------------------------------
 * Incremental collection of expired keys.
 *
 * When keys are accessed they are expired on-access. However we need a
 * mechanism in order to ensure keys are eventually removed when expired even
 * if no access is performed on them.
 *----------------------------------------------------------------------------*/


int activeExpireCycleExpire(redisDb *db, const char *key, expireEntry &e, long long now, size_t &tried) {
    if (!e.FFat())
    {
        activeExpireCycleExpireFullKey(db, key);
        ++tried;
        return 1;
    }

    expireEntryFat *pfat = e.pfatentry();
    robj *val = db->find(key);
    int deleted = 0;

    redisObjectStack objKey;
    initStaticStringObject(objKey, (char*)key);

    while (!pfat->FEmpty())
    {
        ++tried;
        if (pfat->nextExpireEntry().when > now)
            break;

        // Is it the full key expiration?
        if (pfat->nextExpireEntry().spsubkey == nullptr)
        {
            activeExpireCycleExpireFullKey(db, key);
            return ++deleted;
        }

        switch (val->type)
        {
        case OBJ_SET:
            if (setTypeRemove(val,pfat->nextExpireEntry().spsubkey.get())) {
                deleted++;
                if (setTypeSize(val) == 0) {
                    activeExpireCycleExpireFullKey(db, key);
                    return deleted;
                }
            }
            break;

        case OBJ_HASH:
            if (hashTypeDelete(val,(sds)pfat->nextExpireEntry().spsubkey.get())) {
                deleted++;
                if (hashTypeLength(val) == 0) {
                    activeExpireCycleExpireFullKey(db, key);
                    return deleted;
                }
            }
            break;

        case OBJ_ZSET:
            if (zsetDel(val,(sds)pfat->nextExpireEntry().spsubkey.get())) {
                deleted++;
                if (zsetLength(val) == 0) {
                    activeExpireCycleExpireFullKey(db, key);
                    return deleted;
                }
            }
            break;

        case OBJ_CRON:
        {
            sds keyCopy = sdsdup(key);
            incrRefCount(val);
            aePostFunction(g_pserver->rgthreadvar[IDX_EVENT_LOOP_MAIN].el, [keyCopy, val]{
                executeCronJobExpireHook(keyCopy, val);
                sdsfree(keyCopy);
                decrRefCount(val);
            }, true /*fLock*/, true /*fForceQueue*/);
            break;
        }

        case OBJ_LIST:
        default:
            serverAssert(false);
        }
        
        redisObjectStack objSubkey;
        initStaticStringObject(objSubkey, (char*)pfat->nextExpireEntry().spsubkey.get());
        propagateSubkeyExpire(db, val->type, &objKey, &objSubkey);
        
        pfat->popfrontExpireEntry();
        if ((tried % ACTIVE_EXPIRE_CYCLE_SUBKEY_LOOKUPS_PER_LOOP) == 0) {
            break;
        }
    }

    if (pfat->FEmpty())
    {
        removeExpire(db, &objKey);
    }

    if (deleted)
    {
        switch (val->type)
        {
        case OBJ_SET:
            signalModifiedKey(nullptr, db,&objKey);
            notifyKeyspaceEvent(NOTIFY_SET,"srem",&objKey,db->id);
            break;
        }
    }

    return deleted;
}

int parseUnitString(const char *sz)
{
    if (strcasecmp(sz, "s") == 0)
        return UNIT_SECONDS;
    if (strcasecmp(sz, "ms") == 0)
        return UNIT_MILLISECONDS;
    return -1;
}

void expireMemberCore(client *c, robj *key, robj *subkey, long long basetime, long long when, int unit)
{
    switch (unit)
    {
    case UNIT_SECONDS:
        when *= 1000;
    case UNIT_MILLISECONDS:
        break;
    
    default:
        addReplyError(c, "Invalid unit arg");
        return;
    }
    
    when += basetime;

    /* No key, return zero. */
    robj *val = lookupKeyWriteOrReply(c, key, shared.czero);
    if (val == nullptr) {
        return;
    }

    double dblT;
    switch (val->type)
    {
    case OBJ_SET:
        if (!setTypeIsMember(val, szFromObj(subkey))) {
            addReply(c,shared.czero);
            return;
        }
        break;

    case OBJ_HASH:
        if (!hashTypeExists(val, szFromObj(subkey))) {
            addReply(c,shared.czero);
            return;
        }
        break;

    case OBJ_ZSET:
        if (zsetScore(val, szFromObj(subkey), &dblT) == C_ERR) {
            addReply(c,shared.czero);
            return;
        }
        break;

    default:
        addReplyError(c, "object type is unsupported");
        return;
    }

    setExpire(c, c->db, key, subkey, when);
    signalModifiedKey(c, c->db, key);
    g_pserver->dirty++;
    addReply(c, shared.cone);
}

void expireMemberCommand(client *c)
{
    long long when;
    if (getLongLongFromObjectOrReply(c, c->argv[3], &when, NULL) != C_OK)
        return;

    if (c->argc > 5) {
        addReplyError(c, "Invalid number of arguments");
        return;
    }

    int unit = UNIT_SECONDS;
    if (c->argc == 5) {
        unit = parseUnitString(szFromObj(c->argv[4]));
    }

    expireMemberCore(c, c->argv[1], c->argv[2], mstime(), when, unit);
}

void expireMemberAtCommand(client *c)
{
    long long when;
    if (getLongLongFromObjectOrReply(c, c->argv[3], &when, NULL) != C_OK)
        return;

    expireMemberCore(c, c->argv[1], c->argv[2], 0, when, UNIT_SECONDS);
}

void pexpireMemberAtCommand(client *c)
{
    long long when;
    if (getLongLongFromObjectOrReply(c, c->argv[3], &when, NULL) != C_OK)
        return;

    expireMemberCore(c, c->argv[1], c->argv[2], 0, when, UNIT_MILLISECONDS);
}

/* Try to expire a few timed out keys. The algorithm used is adaptive and
 * will use few CPU cycles if there are few expiring keys, otherwise
 * it will get more aggressive to avoid that too much memory is used by
 * keys that can be removed from the keyspace.
 *
 * Every expire cycle tests multiple databases: the next call will start
 * again from the next db. No more than CRON_DBS_PER_CALL databases are
 * tested at every iteration.
 *
 * The function can perform more or less work, depending on the "type"
 * argument. It can execute a "fast cycle" or a "slow cycle". The slow
 * cycle is the main way we collect expired cycles: this happens with
 * the "server.hz" frequency (usually 10 hertz).
 *
 * This kind of call is used when Redis detects that timelimit_exit is
 * true, so there is more work to do, and we do it more incrementally from
 * the beforeSleep() function of the event loop.
 *
 * Expire cycle type:
 *
 * If type is ACTIVE_EXPIRE_CYCLE_FAST the function will try to run a
 * "fast" expire cycle that takes no longer than ACTIVE_EXPIRE_CYCLE_FAST_DURATION
 * microseconds, and is not repeated again before the same amount of time.
 *
 * If type is ACTIVE_EXPIRE_CYCLE_SLOW, that normal expire cycle is
 * executed, where the time limit is a percentage of the REDIS_HZ period
 * as specified by the ACTIVE_EXPIRE_CYCLE_SLOW_TIME_PERC define. */
#define ACTIVE_EXPIRE_CYCLE_KEYS_PER_LOOP 20 /* Keys for each DB loop. */
#define ACTIVE_EXPIRE_CYCLE_FAST_DURATION 1000 /* Microseconds. */
#define ACTIVE_EXPIRE_CYCLE_SLOW_TIME_PERC 25 /* Max % of CPU to use. */
#define ACTIVE_EXPIRE_CYCLE_ACCEPTABLE_STALE 10 /* % of stale keys after which
                                                   we do extra efforts. */
/*static*/ void redisDbPersistentData::activeExpireCycleCore(int type) {
    /* Adjust the running parameters according to the configured expire
     * effort. The default effort is 1, and the maximum configurable effort
     * is 10. */
    unsigned long
    effort = g_pserver->active_expire_effort-1, /* Rescale from 0 to 9. */
    config_keys_per_loop = ACTIVE_EXPIRE_CYCLE_KEYS_PER_LOOP +
                           ACTIVE_EXPIRE_CYCLE_KEYS_PER_LOOP/4*effort,
    config_cycle_fast_duration = ACTIVE_EXPIRE_CYCLE_FAST_DURATION +
                                 ACTIVE_EXPIRE_CYCLE_FAST_DURATION/4*effort,
    config_cycle_slow_time_perc = ACTIVE_EXPIRE_CYCLE_SLOW_TIME_PERC +
                                  2*effort,
    config_cycle_acceptable_stale = ACTIVE_EXPIRE_CYCLE_ACCEPTABLE_STALE-
                                    effort;

    /* This function has some global state in order to continue the work
     * incrementally across calls. */
    static unsigned int current_db = 0; /* Next DB to test. */
    static int timelimit_exit = 0;      /* Time limit hit in previous call? */
    static long long last_fast_cycle = 0; /* When last fast cycle ran. */

    int j, iteration = 0;
    int dbs_per_call = CRON_DBS_PER_CALL;
    long long start = ustime(), timelimit, elapsed;

    /* When clients are paused the dataset should be static not just from the
     * POV of clients not being able to write, but also from the POV of
     * expires and evictions of keys not being performed. */
    if (checkClientPauseTimeoutAndReturnIfPaused()) return;

    if (type == ACTIVE_EXPIRE_CYCLE_FAST) {
        /* Don't start a fast cycle if the previous cycle did not exit
         * for time limit, unless the percentage of estimated stale keys is
         * too high. Also never repeat a fast cycle for the same period
         * as the fast cycle total duration itself. */
        if (!timelimit_exit &&
            g_pserver->stat_expired_stale_perc < config_cycle_acceptable_stale)
            return;

        if (start < last_fast_cycle + (long long)config_cycle_fast_duration*2)
            return;

        last_fast_cycle = start;
    }

    /* We usually should test CRON_DBS_PER_CALL per iteration, with
     * two exceptions:
     *
     * 1) Don't test more DBs than we have.
     * 2) If last time we hit the time limit, we want to scan all DBs
     * in this iteration, as there is work to do in some DB and we don't want
     * expired keys to use memory for too much time. */
    if (dbs_per_call > cserver.dbnum || timelimit_exit)
        dbs_per_call = cserver.dbnum;

    /* We can use at max 'config_cycle_slow_time_perc' percentage of CPU
     * time per iteration. Since this function gets called with a frequency of
     * server.hz times per second, the following is the max amount of
     * microseconds we can spend in this function. */
    timelimit = config_cycle_slow_time_perc*1000000/g_pserver->hz/100;
    timelimit_exit = 0;
    if (timelimit <= 0) timelimit = 1;

    if (type == ACTIVE_EXPIRE_CYCLE_FAST)
        timelimit = config_cycle_fast_duration; /* in microseconds. */

    /* Accumulate some global stats as we expire keys, to have some idea
     * about the number of keys that are already logically expired, but still
     * existing inside the database. */
    long total_sampled = 0;
    long total_expired = 0;

    for (j = 0; j < dbs_per_call && timelimit_exit == 0; j++) {
        /* Expired and checked in a single loop. */
        unsigned long expired, sampled;

        redisDb *db = g_pserver->db[(current_db % cserver.dbnum)];

        /* Increment the DB now so we are sure if we run out of time
         * in the current DB we'll restart from the next. This allows to
         * distribute the time evenly across DBs. */
        current_db++;

        if (g_pserver->m_pstorageFactory == nullptr) {
            /* Continue to expire if at the end of the cycle there are still
            * a big percentage of keys to expire, compared to the number of keys
            * we scanned. The percentage, stored in config_cycle_acceptable_stale
            * is not fixed, but depends on the Redis configured "expire effort". */
            do {
                unsigned long num, slots;
                long long now, ttl_sum;
                int ttl_samples;
                iteration++;

                /* If there is nothing to expire try next DB ASAP. */
                if (db->expireSize() == 0) {
                    db->avg_ttl = 0;
                    break;
                }
                num = dictSize(db->m_pdict);
                slots = dictSlots(db->m_pdict);
                now = mstime();

                /* When there are less than 1% filled slots, sampling the key
                * space is expensive, so stop here waiting for better times...
                * The dictionary will be resized asap. */
                if (slots > DICT_HT_INITIAL_SIZE &&
                    (num*100/slots < 1)) break;

                /* The main collection cycle. Sample random keys among keys
                * with an expire set, checking for expired ones. */
                expired = 0;
                sampled = 0;
                ttl_sum = 0;
                ttl_samples = 0;

                if (num > config_keys_per_loop)
                    num = config_keys_per_loop;

                /* Here we access the low level representation of the hash table
                * for speed concerns: this makes this code coupled with dict.c,
                * but it hardly changed in ten years.
                *
                * Note that certain places of the hash table may be empty,
                * so we want also a stop condition about the number of
                * buckets that we scanned. However scanning for free buckets
                * is very fast: we are in the cache line scanning a sequential
                * array of NULL pointers, so we can scan a lot more buckets
                * than keys in the same time. */
                long max_buckets = num*20;
                long checked_buckets = 0;

                while (sampled < num && checked_buckets < max_buckets) {
                    for (int table = 0; table < 2; table++) {
                        if (table == 1 && !dictIsRehashing(db->m_pdict)) break;

                        unsigned long idx = db->expires_cursor;
                        idx &= db->m_pdict->ht[table].sizemask;
                        dictEntry *de = db->m_pdict->ht[table].table[idx];
                        long long ttl;

                        /* Scan the current bucket of the current table. */
                        checked_buckets++;
                        while(de) {
                            /* Get the next entry now since this entry may get
                            * deleted. */
                            dictEntry *e = de;
                            robj *o = (robj*)dictGetVal(de);
                            de = de->next;
                            if (!o->FExpires())
                                continue;

                            expireEntry *exp = &o->expire;

                            serverAssert(exp->when() > 0);
                            ttl = exp->when()-now;
                            size_t tried = 0;
                            if (exp->when() <= now) {
                                if (activeExpireCycleExpire(db,(const char*)dictGetKey(e),*exp,now,tried)) expired++;
                                serverAssert(ttl <= 0);
                            } else {
                                serverAssert(ttl > 0);
                            }
                            if (ttl > 0) {
                                /* We want the average TTL of keys yet
                                * not expired. */
                                ttl_sum += ttl;
                                ttl_samples++;
                            }
                            sampled++;
                        }
                    }
                    db->expires_cursor++;
                }
                total_expired += expired;
                total_sampled += sampled;

                /* Update the average TTL stats for this database. */
                if (ttl_samples) {
                    long long avg_ttl = ttl_sum/ttl_samples;

                    /* Do a simple running average with a few samples.
                    * We just use the current estimate with a weight of 2%
                    * and the previous estimate with a weight of 98%. */
                    if (db->avg_ttl == 0) db->avg_ttl = avg_ttl;
                    db->avg_ttl = (db->avg_ttl/50)*49 + (avg_ttl/50);
                }

                /* We can't block forever here even if there are many keys to
                * expire. So after a given amount of milliseconds return to the
                * caller waiting for the other active expire cycle. */
                if ((iteration & 0xf) == 0) { /* check once every 16 iterations. */
                    elapsed = ustime()-start;
                    if (elapsed > timelimit) {
                        timelimit_exit = 1;
                        g_pserver->stat_expired_time_cap_reached_count++;
                        break;
                    }
                }
                /* We don't repeat the cycle for the current database if there are
                * an acceptable amount of stale keys (logically expired but yet
                * not reclaimed). */
            } while (sampled == 0 ||
                    (expired*100/sampled) > config_cycle_acceptable_stale);
        } else {
            long prev_expired;
            long long now = mstime();
            size_t tried = 0;
            std::vector<std::string> keys;
            do {
                prev_expired = total_expired;
                keys = db->getStorageCache()->getExpirationCandidates(ACTIVE_EXPIRE_CYCLE_LOOKUPS_PER_LOOP);
                for (std::string key : keys) {
                    robj* keyobj = createStringObject(key.c_str(), key.size());
                    db->find(szFromObj(keyobj));
                    expireEntry *e = db->getExpire(keyobj);
                    if (e != nullptr && e->when() < now)
                        total_expired += activeExpireCycleExpire(db, szFromObj(keyobj), *e, now, tried);
                    decrRefCount(keyobj);
                }
                total_sampled += keys.size();
                elapsed = ustime()-start;
            } while (keys.size() > 0 && (elapsed < timelimit) && (total_expired - prev_expired) > 0);

            if (ustime()-start > timelimit) {
                timelimit_exit = 1;
                g_pserver->stat_expired_time_cap_reached_count++;
            }
        }
    }

    elapsed = ustime()-start;
    g_pserver->stat_expire_cycle_time_used += elapsed;
    latencyAddSampleIfNeeded("expire-cycle",elapsed/1000);

    /* Update our estimate of keys existing but yet to be expired.
     * Running average with this sample accounting for 5%. */
    double current_perc;
    if (total_sampled) {
        current_perc = (double)total_expired/total_sampled;
    } else
        current_perc = 0;
    g_pserver->stat_expired_stale_perc = (current_perc*0.05)+
                                     (g_pserver->stat_expired_stale_perc*0.95);
}

void activeExpireCycle(int type)
{
    runAndPropogateToReplicas(redisDbPersistentData::activeExpireCycleCore, type);
}

/*-----------------------------------------------------------------------------
 * Expires of keys created in writable slaves
 *
 * Normally slaves do not process expires: they wait the masters to synthesize
 * DEL operations in order to retain consistency. However writable slaves are
 * an exception: if a key is created in the replica and an expire is assigned
 * to it, we need a way to expire such a key, since the master does not know
 * anything about such a key.
 *
 * In order to do so, we track keys created in the replica side with an expire
 * set, and call the expireSlaveKeys() function from time to time in order to
 * reclaim the keys if they already expired.
 *
 * Note that the use case we are trying to cover here, is a popular one where
 * slaves are put in writable mode in order to compute slow operations in
 * the replica side that are mostly useful to actually read data in a more
 * processed way. Think at sets intersections in a tmp key, with an expire so
 * that it is also used as a cache to avoid intersecting every time.
 *
 * This implementation is currently not perfect but a lot better than leaking
 * the keys as implemented in 3.2.
 *----------------------------------------------------------------------------*/

/* The dictionary where we remember key names and database ID of keys we may
 * want to expire from the replica. Since this function is not often used we
 * don't even care to initialize the database at startup. We'll do it once
 * the feature is used the first time, that is, when rememberSlaveKeyWithExpire()
 * is called.
 *
 * The dictionary has an SDS string representing the key as the hash table
 * key, while the value is a 64 bit unsigned integer with the bits corresponding
 * to the DB where the keys may exist set to 1. Currently the keys created
 * with a DB id > 63 are not expired, but a trivial fix is to set the bitmap
 * to the max 64 bit unsigned value when we know there is a key with a DB
 * ID greater than 63, and check all the configured DBs in such a case. */
dict *slaveKeysWithExpire = NULL;

/* Check the set of keys created by the master with an expire set in order to
 * check if they should be evicted. */
void expireSlaveKeys(void) {
    if (slaveKeysWithExpire == NULL ||
        dictSize(slaveKeysWithExpire) == 0) return;

    int cycles = 0, noexpire = 0;
    mstime_t start = mstime();
    while(1) {
        dictEntry *de = dictGetRandomKey(slaveKeysWithExpire);
        sds keyname = (sds)dictGetKey(de);
        uint64_t dbids = dictGetUnsignedIntegerVal(de);
        uint64_t new_dbids = 0;

        /* Check the key against every database corresponding to the
         * bits set in the value bitmap. */
        int dbid = 0;
        while(dbids && dbid < cserver.dbnum) {
            if ((dbids & 1) != 0) {
                redisDb *db = g_pserver->db[dbid];
                auto itrDB = db->find(keyname);
                int expired = 0;

                if (itrDB != db->end() && itrDB->FExpires())
                {
                    if (itrDB->expire.when() < start) {
                        size_t tried = 0;
                        expired = activeExpireCycleExpire(g_pserver->db[dbid],itrDB.key(),itrDB->expire,start,tried);
                    }
                }

                /* If the key was not expired in this DB, we need to set the
                 * corresponding bit in the new bitmap we set as value.
                 * At the end of the loop if the bitmap is zero, it means we
                 * no longer need to keep track of this key. */
                if (itrDB != db->end() && itrDB->FExpires() && !expired) {
                    noexpire++;
                    new_dbids |= (uint64_t)1 << dbid;
                }
            }
            dbid++;
            dbids >>= 1;
        }

        /* Set the new bitmap as value of the key, in the dictionary
         * of keys with an expire set directly in the writable replica. Otherwise
         * if the bitmap is zero, we no longer need to keep track of it. */
        if (new_dbids)
            dictSetUnsignedIntegerVal(de,new_dbids);
        else
            dictDelete(slaveKeysWithExpire,keyname);

        /* Stop conditions: found 3 keys we can't expire in a row or
         * time limit was reached. */
        cycles++;
        if (noexpire > 3) break;
        if ((cycles % 64) == 0 && mstime()-start > 1) break;
        if (dictSize(slaveKeysWithExpire) == 0) break;
    }
}

/* Track keys that received an EXPIRE or similar command in the context
 * of a writable replica. */
void rememberSlaveKeyWithExpire(redisDb *db, robj *key) {
    if (slaveKeysWithExpire == NULL) {
        static dictType dt = {
            dictSdsHash,                /* hash function */
            NULL,                       /* key dup */
            NULL,                       /* val dup */
            dictSdsKeyCompare,          /* key compare */
            dictSdsDestructor,          /* key destructor */
            NULL,                       /* val destructor */
            NULL                        /* allow to expand */
        };
        slaveKeysWithExpire = dictCreate(&dt,NULL);
    }
    if (db->id > 63) return;

    dictEntry *de = dictAddOrFind(slaveKeysWithExpire,ptrFromObj(key));
    /* If the entry was just created, set it to a copy of the SDS string
     * representing the key: we don't want to need to take those keys
     * in sync with the main DB. The keys will be removed by expireSlaveKeys()
     * as it scans to find keys to remove. */
    if (de->key == ptrFromObj(key)) {
        de->key = sdsdup(szFromObj(key));
        dictSetUnsignedIntegerVal(de,0);
    }

    uint64_t dbids = dictGetUnsignedIntegerVal(de);
    dbids |= (uint64_t)1 << db->id;
    dictSetUnsignedIntegerVal(de,dbids);
}

/* Return the number of keys we are tracking. */
size_t getSlaveKeyWithExpireCount(void) {
    if (slaveKeysWithExpire == NULL) return 0;
    return dictSize(slaveKeysWithExpire);
}

/* Remove the keys in the hash table. We need to do that when data is
 * flushed from the g_pserver-> We may receive new keys from the master with
 * the same name/db and it is no longer a good idea to expire them.
 *
 * Note: technically we should handle the case of a single DB being flushed
 * but it is not worth it since anyway race conditions using the same set
 * of key names in a writable replica and in its master will lead to
 * inconsistencies. This is just a best-effort thing we do. */
void flushSlaveKeysWithExpireList(void) {
    if (slaveKeysWithExpire) {
        dictRelease(slaveKeysWithExpire);
        slaveKeysWithExpire = NULL;
    }
}

int checkAlreadyExpired(long long when) {
    /* EXPIRE with negative TTL, or EXPIREAT with a timestamp into the past
     * should never be executed as a DEL when load the AOF or in the context
     * of a slave instance.
     *
     * Instead we add the already expired key to the database with expire time
     * (possibly in the past) and wait for an explicit DEL from the master. */
    return (when <= mstime() && !g_pserver->loading && (!listLength(g_pserver->masters) || g_pserver->fActiveReplica));
}

/*-----------------------------------------------------------------------------
 * Expires Commands
 *----------------------------------------------------------------------------*/

/* This is the generic command implementation for EXPIRE, PEXPIRE, EXPIREAT
 * and PEXPIREAT. Because the command second argument may be relative or absolute
 * the "basetime" argument is used to signal what the base time is (either 0
 * for *AT variants of the command, or the current time for relative expires).
 *
 * unit is either UNIT_SECONDS or UNIT_MILLISECONDS, and is only used for
 * the argv[2] parameter. The basetime is always specified in milliseconds. */
void expireGenericCommand(client *c, long long basetime, int unit) {
    robj *key = c->argv[1], *param = c->argv[2];
    long long when; /* unix time in milliseconds when the key will expire. */

    if (getLongLongFromObjectOrReply(c, param, &when, NULL) != C_OK)
        return;
    int negative_when = when < 0;
    if (unit == UNIT_SECONDS) when *= 1000;
    when += basetime;
    if (((when < 0) && !negative_when) || ((when-basetime > 0) && negative_when)) {
        /* EXPIRE allows negative numbers, but we can at least detect an
         * overflow by either unit conversion or basetime addition. */
        addReplyErrorFormat(c, "invalid expire time in %s", c->cmd->name);
        return;
    }
    /* No key, return zero. */
    if (lookupKeyWrite(c->db,key) == NULL) {
        addReply(c,shared.czero);
        return;
    }

    if (checkAlreadyExpired(when)) {
        robj *aux;

        int deleted = g_pserver->lazyfree_lazy_expire ? dbAsyncDelete(c->db,key) :
                                                    dbSyncDelete(c->db,key);
        serverAssertWithInfo(c,key,deleted);
        g_pserver->dirty++;

        /* Replicate/AOF this as an explicit DEL or UNLINK. */
        aux = g_pserver->lazyfree_lazy_expire ? shared.unlink : shared.del;
        rewriteClientCommandVector(c,2,aux,key);
        signalModifiedKey(c,c->db,key);
        notifyKeyspaceEvent(NOTIFY_GENERIC,"del",key,c->db->id);
        addReply(c, shared.cone);
        return;
    } else {
        setExpire(c,c->db,key,nullptr,when);
        addReply(c,shared.cone);
        signalModifiedKey(c,c->db,key);
        notifyKeyspaceEvent(NOTIFY_GENERIC,"expire",key,c->db->id);
        g_pserver->dirty++;
        return;
    }
}

/* EXPIRE key seconds */
void expireCommand(client *c) {
    expireGenericCommand(c,mstime(),UNIT_SECONDS);
}

/* EXPIREAT key time */
void expireatCommand(client *c) {
    expireGenericCommand(c,0,UNIT_SECONDS);
}

/* PEXPIRE key milliseconds */
void pexpireCommand(client *c) {
    expireGenericCommand(c,mstime(),UNIT_MILLISECONDS);
}

/* PEXPIREAT key ms_time */
void pexpireatCommand(client *c) {
    expireGenericCommand(c,0,UNIT_MILLISECONDS);
}

/* Implements TTL and PTTL */
void ttlGenericCommand(client *c, int output_ms, int output_abs) {
    long long expire = INVALID_EXPIRE, ttl = -1;

    /* If the key does not exist at all, return -2 */
    if (lookupKeyReadWithFlags(c->db,c->argv[1],LOOKUP_NOTOUCH) == nullptr) {
        addReplyLongLong(c,-2);
        return;
    }

    /* The key exists. Return -1 if it has no expire, or the actual
        * TTL value otherwise. */
    expireEntry *pexpire = c->db->getExpire(c->argv[1]);

    if (c->argc == 2) {
        // primary expire    
        if (pexpire != nullptr)
            pexpire->FGetPrimaryExpire(&expire);
    } else if (c->argc == 3) {
        // We want a subkey expire
        if (pexpire && pexpire->FFat()) {
            for (auto itr : *pexpire) {
                if (itr.subkey() == nullptr)
                    continue;
                if (sdscmp((sds)itr.subkey(), szFromObj(c->argv[2])) == 0) {
                    expire = itr.when();
                    break;
                }
            }
        }
    } else {
        addReplyError(c, "Invalid arguments");
        return;
    }

    
    if (expire != INVALID_EXPIRE) {
        /* Return absolute timestamp if output_abs is set, otherwise return TTL */
        ttl = output_abs ? expire : expire-mstime();
        if (ttl < 0 && !output_abs) ttl = 0;
    }
    
    if (ttl == -1) {
        addReplyLongLong(c,-1);
    } else {
        addReplyLongLong(c,output_ms ? ttl : ((ttl+500)/1000));
    }
}

/* TTL key */
void ttlCommand(client *c) {
    ttlGenericCommand(c, 0, 0);
}

/* PTTL key */
void pttlCommand(client *c) {
    ttlGenericCommand(c, 1, 0);
}

/* EXPIRETIME key - Returns absolute expire time in seconds (Redis 7.0+) */
void expiretimeCommand(client *c) {
    ttlGenericCommand(c, 0, 1);
}

/* PEXPIRETIME key - Returns absolute expire time in milliseconds (Redis 7.0+) */
void pexpiretimeCommand(client *c) {
    ttlGenericCommand(c, 1, 1);
}

/* PERSIST key */
void persistCommand(client *c) {
    if (lookupKeyWrite(c->db,c->argv[1])) {
        if (c->argc == 2) {
            if (removeExpire(c->db,c->argv[1])) {
                signalModifiedKey(c,c->db,c->argv[1]);
                notifyKeyspaceEvent(NOTIFY_GENERIC,"persist",c->argv[1],c->db->id);
                addReply(c,shared.cone);
                g_pserver->dirty++;
            } else {
                addReply(c,shared.czero);
            }
        } else if (c->argc == 3) {
            if (c->db->removeSubkeyExpire(c->argv[1], c->argv[2])) {
                signalModifiedKey(c,c->db,c->argv[1]);
                notifyKeyspaceEvent(NOTIFY_GENERIC,"persist",c->argv[1],c->db->id);
                addReply(c,shared.cone);
                g_pserver->dirty++;
            } else {
                addReply(c,shared.czero);
            }
        } else {
            addReplyError(c, "Invalid arguments");
        }
    } else {
        addReply(c,shared.czero);
    }
}

/* TOUCH key1 [key2 key3 ... keyN] */
void touchCommand(client *c) {
    int touched = 0;
    for (int j = 1; j < c->argc; j++)
        if (lookupKeyRead(c->db,c->argv[j]) != nullptr) touched++;
    addReplyLongLong(c,touched);
}

expireEntryFat::~expireEntryFat()
{
    if (m_dictIndex != nullptr)
        dictRelease(m_dictIndex);
}

expireEntryFat::expireEntryFat(const expireEntryFat &e)
    : m_vecexpireEntries(e.m_vecexpireEntries)
{
    // Note: dictExpires is not copied
}

void expireEntryFat::createIndex()
{
    serverAssert(m_dictIndex == nullptr);
    m_dictIndex = dictCreate(&dbExpiresDictType, nullptr);

    for (auto &entry : m_vecexpireEntries)
    {
        if (entry.spsubkey != nullptr)
        {
            dictEntry *de = dictAddRaw(m_dictIndex, (void*)entry.spsubkey.get(), nullptr);
            de->v.s64 = entry.when;
        }
    }
}

void expireEntryFat::expireSubKey(const char *szSubkey, long long when)
{
    if (m_vecexpireEntries.size() >= INDEX_THRESHOLD && m_dictIndex == nullptr)
        createIndex();

    // First check if the subkey already has an expiration
    if (m_dictIndex != nullptr && szSubkey != nullptr)
    {
        dictEntry *de = dictFind(m_dictIndex, szSubkey);
        if (de != nullptr)
        {
            auto itr = std::lower_bound(m_vecexpireEntries.begin(), m_vecexpireEntries.end(), de->v.u64);
            while (itr != m_vecexpireEntries.end() && itr->when == de->v.s64)
            {
                bool fFound = false;
                if (szSubkey == nullptr && itr->spsubkey == nullptr) {
                    fFound = true;
                } else if (szSubkey != nullptr && itr->spsubkey != nullptr && sdscmp((sds)itr->spsubkey.get(), (sds)szSubkey) == 0) {
                    fFound = true;
                }
                if (fFound) {
                    dictDelete(m_dictIndex, szSubkey);
                    m_vecexpireEntries.erase(itr);
                    break;
                }
                ++itr;
            }
        }
    }
    else
    {
        for (auto &entry : m_vecexpireEntries)
        {
            if (szSubkey != nullptr)
            {
                // if this is a subkey expiry then its not a match if the expireEntry is either for the
                //  primary key or a different subkey
                if (entry.spsubkey == nullptr || sdscmp((sds)entry.spsubkey.get(), (sds)szSubkey) != 0)
                    continue;
            }
            else
            {
                if (entry.spsubkey != nullptr)
                    continue;
            }
            m_vecexpireEntries.erase(m_vecexpireEntries.begin() + (&entry - m_vecexpireEntries.data()));
            break;
        }
    }
    auto itrInsert = std::lower_bound(m_vecexpireEntries.begin(), m_vecexpireEntries.end(), when);
    const char *subkey = (szSubkey) ? sdsdup(szSubkey) : nullptr;
    auto itr = m_vecexpireEntries.emplace(itrInsert, when, subkey);
    if (m_dictIndex && subkey) {
        dictEntry *de = dictAddRaw(m_dictIndex, (void*)itr->spsubkey.get(), nullptr);
        de->v.s64 = when;
    }
}

void expireEntryFat::popfrontExpireEntry()
{ 
    if (m_dictIndex != nullptr && m_vecexpireEntries.begin()->spsubkey) {
        int res = dictDelete(m_dictIndex, (void*)m_vecexpireEntries.begin()->spsubkey.get());
        serverAssert(res == DICT_OK);
    }
    m_vecexpireEntries.erase(m_vecexpireEntries.begin());
}
