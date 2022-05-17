/*
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
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
#include "cluster.h"
#include "atomicvar.h"
#include "aelocker.h"
#include "latency.h"

#include <signal.h>
#include <ctype.h>

// Needed for prefetch
#if defined(__x86_64__) || defined(__i386__)
#include <xmmintrin.h>
#endif

/* Database backup. */
struct dbBackup {
    const redisDbPersistentDataSnapshot **dbarray;
    rax *slots_to_keys;
    uint64_t slots_keys_count[CLUSTER_SLOTS];
};

/*-----------------------------------------------------------------------------
 * C-level DB API
 *----------------------------------------------------------------------------*/

int expireIfNeeded(redisDb *db, robj *key, robj *o);
void slotToKeyUpdateKeyCore(const char *key, size_t keylen, int add);

std::unique_ptr<expireEntry> deserializeExpire(sds key, const char *str, size_t cch, size_t *poffset);
sds serializeStoredObjectAndExpire(redisDbPersistentData *db, const char *key, robj_roptr o);

dictType dictChangeDescType {
    dictSdsHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCompare,          /* key compare */
    dictSdsDestructor,          /* key destructor */
    nullptr                     /* val destructor */
};

/* Update LFU when an object is accessed.
 * Firstly, decrement the counter if the decrement time is reached.
 * Then logarithmically increment the counter, and update the access time. */
void updateLFU(robj *val) {
    unsigned long counter = LFUDecrAndReturn(val);
    counter = LFULogIncr(counter);
    val->lru = (LFUGetTimeInMinutes()<<8) | counter;
}

void updateExpire(redisDb *db, sds key, robj *valOld, robj *valNew)
{
    serverAssert(valOld->FExpires());
    serverAssert(!valNew->FExpires());
    
    serverAssert(db->FKeyExpires((const char*)key));
    
    valNew->SetFExpires(true);
    valOld->SetFExpires(false);
    return;
}

static void lookupKeyUpdateObj(robj *val, int flags)
{
    /* Update the access time for the ageing algorithm.
     * Don't do it if we have a saving child, as this will trigger
     * a copy on write madness. */
    if (!hasActiveChildProcess() && !(flags & LOOKUP_NOTOUCH))
    {
        if (g_pserver->maxmemory_policy & MAXMEMORY_FLAG_LFU) {
            updateLFU(val);
        } else {
            val->lru = LRU_CLOCK();
        }
    }
}

/* Low level key lookup API, not actually called directly from commands
 * implementations that should instead rely on lookupKeyRead(),
 * lookupKeyWrite() and lookupKeyReadWithFlags(). */
static robj* lookupKey(redisDb *db, robj *key, int flags) {
    auto itr = db->find(key);
    if (itr) {
        robj *val = itr.val();
        lookupKeyUpdateObj(val, flags);
        if (flags & LOOKUP_UPDATEMVCC) {
            setMvccTstamp(val, getMvccTstamp());
            db->trackkey(key, true /* fUpdate */);
        }
        return val;
    } else {
        return NULL;
    }
}
static robj_roptr lookupKeyConst(redisDb *db, robj *key, int flags) {
    serverAssert((flags & LOOKUP_UPDATEMVCC) == 0);
    robj_roptr val;
    if (g_pserver->m_pstorageFactory)
        val = db->find(szFromObj(key)).val();
    else
        val = db->find_cached_threadsafe(szFromObj(key)).val();
    
    if (val != nullptr) {
        lookupKeyUpdateObj(val.unsafe_robjcast(), flags);
        return val;
    }
    return nullptr;
}

/* Lookup a key for read operations, or return NULL if the key is not found
 * in the specified DB.
 *
 * As a side effect of calling this function:
 * 1. A key gets expired if it reached it's TTL.
 * 2. The key last access time is updated.
 * 3. The global keys hits/misses stats are updated (reported in INFO).
 * 4. If keyspace notifications are enabled, a "keymiss" notification is fired.
 *
 * This API should not be used when we write to the key after obtaining
 * the object linked to the key, but only for read only operations.
 *
 * Flags change the behavior of this command:
 *
 *  LOOKUP_NONE (or zero): no special flags are passed.
 *  LOOKUP_NOTOUCH: don't alter the last access time of the key.
 *
 * Note: this function also returns NULL if the key is logically expired
 * but still existing, in case this is a replica, since this API is called only
 * for read operations. Even if the key expiry is master-driven, we can
 * correctly report a key is expired on slaves even if the master is lagging
 * expiring our key via DELs in the replication link. */
robj_roptr lookupKeyReadWithFlags(redisDb *db, robj *key, int flags) {
    robj_roptr val;

    if (expireIfNeeded(db,key) == 1) {
        /* If we are in the context of a master, expireIfNeeded() returns 1
         * when the key is no longer valid, so we can return NULL ASAP. */
        if (listLength(g_pserver->masters) == 0)
            goto keymiss;

        /* However if we are in the context of a replica, expireIfNeeded() will
         * not really try to expire the key, it only returns information
         * about the "logical" status of the key: key expiring is up to the
         * master in order to have a consistent view of master's data set.
         *
         * However, if the command caller is not the master, and as additional
         * safety measure, the command invoked is a read-only command, we can
         * safely return NULL here, and provide a more consistent behavior
         * to clients accessing expired values in a read-only fashion, that
         * will say the key as non existing.
         *
         * Notably this covers GETs when slaves are used to scale reads. */
        if (serverTL->current_client &&
            !FActiveMaster(serverTL->current_client) &&
            serverTL->current_client->cmd &&
            serverTL->current_client->cmd->flags & CMD_READONLY)
        {
            goto keymiss;
        }
    }
    val = lookupKeyConst(db,key,flags);
    if (val == nullptr)
        goto keymiss;
    g_pserver->stat_keyspace_hits++;
    return val;

keymiss:
    if (!(flags & LOOKUP_NONOTIFY)) {
        notifyKeyspaceEvent(NOTIFY_KEY_MISS, "keymiss", key, db->id);
    }
    g_pserver->stat_keyspace_misses++;
    return NULL;
}

/* Like lookupKeyReadWithFlags(), but does not use any flag, which is the
 * common case. */
robj_roptr lookupKeyRead(redisDb *db, robj *key) {
    serverAssert(GlobalLocksAcquired());
    return lookupKeyReadWithFlags(db,key,LOOKUP_NONE);
}
robj_roptr lookupKeyRead(redisDb *db, robj *key, uint64_t mvccCheckpoint, AeLocker &locker) {
    robj_roptr o;

    if (aeThreadOwnsLock()) {
        return lookupKeyReadWithFlags(db,key,LOOKUP_NONE);
    } else {
        // This is an async command
        if (keyIsExpired(db,key))
            return nullptr;
        int idb = db->id;
        if (serverTL->rgdbSnapshot[idb] == nullptr || serverTL->rgdbSnapshot[idb]->mvccCheckpoint() < mvccCheckpoint) {
            locker.arm(serverTL->current_client);
            if (serverTL->rgdbSnapshot[idb] != nullptr) {
                db->endSnapshot(serverTL->rgdbSnapshot[idb]);
                serverTL->rgdbSnapshot[idb] = nullptr;
            } else {
                serverTL->rgdbSnapshot[idb] = db->createSnapshot(mvccCheckpoint, true);
            }
            if (serverTL->rgdbSnapshot[idb] == nullptr) {
                // We still need to service the read
                o = lookupKeyReadWithFlags(db,key,LOOKUP_NONE);
                serverTL->disable_async_commands = true; // don't try this again
            }
            else {
                locker.disarm();
            }
        }
        if (serverTL->rgdbSnapshot[idb] != nullptr) {
            o = serverTL->rgdbSnapshot[idb]->find_cached_threadsafe(szFromObj(key)).val();
        }
    }

    return o;
}

/* Lookup a key for write operations, and as a side effect, if needed, expires
 * the key if its TTL is reached.
 *
 * Returns the linked value object if the key exists or NULL if the key
 * does not exist in the specified DB. */
robj *lookupKeyWriteWithFlags(redisDb *db, robj *key, int flags) {
    expireIfNeeded(db,key);
    robj *o = lookupKey(db,key,flags|LOOKUP_UPDATEMVCC);
    return o;
}

robj *lookupKeyWrite(redisDb *db, robj *key) {
    return lookupKeyWriteWithFlags(db, key, LOOKUP_NONE);
}
void SentReplyOnKeyMiss(client *c, robj *reply){
    serverAssert(sdsEncodedObject(reply));
    sds rep = szFromObj(reply);
    if (sdslen(rep) > 1 && rep[0] == '-'){
        addReplyErrorObject(c, reply);
    } else {
        addReply(c,reply);
    }
}
robj_roptr lookupKeyReadOrReply(client *c, robj *key, robj *reply) {
    robj_roptr o = lookupKeyRead(c->db, key);
    if (!o) SentReplyOnKeyMiss(c, reply);
    return o;
}
robj_roptr lookupKeyReadOrReply(client *c, robj *key, robj *reply, AeLocker &locker) {
    robj_roptr o = lookupKeyRead(c->db, key, c->mvccCheckpoint, locker);
    if (!o) SentReplyOnKeyMiss(c, reply);
    return o;
}

robj *lookupKeyWriteOrReply(client *c, robj *key, robj *reply) {
    robj *o = lookupKeyWrite(c->db, key);
    if (!o) SentReplyOnKeyMiss(c, reply);
    return o;
}

bool dbAddCore(redisDb *db, sds key, robj *val, bool fUpdateMvcc, bool fAssumeNew = false, dict_iter *piterExisting = nullptr) {
    serverAssert(!val->FExpires());
    sds copy = sdsdupshared(key);
    
    uint64_t mvcc = getMvccTstamp();
    if (fUpdateMvcc) {
        setMvccTstamp(val, mvcc);
    }

    bool fInserted = db->insert(copy, val, fAssumeNew, piterExisting);

    if (fInserted)
    {
        signalKeyAsReady(db, key, val->type);
        if (g_pserver->cluster_enabled) slotToKeyAdd(key);
    }
    else
    {
        sdsfree(copy);
    }

    return fInserted;
}

/* Add the key to the DB. It's up to the caller to increment the reference
 * counter of the value if needed.
 *
 * The program is aborted if the key already exists. */
void dbAdd(redisDb *db, robj *key, robj *val)
{
    bool fInserted = dbAddCore(db, szFromObj(key), val, true /* fUpdateMvcc */);
    serverAssertWithInfo(NULL,key,fInserted);
}

void redisDb::dbOverwriteCore(redisDb::iter itr, sds keySds, robj *val, bool fUpdateMvcc, bool fRemoveExpire)
{
    robj *old = itr.val();
    redisObjectStack keyO;
    initStaticStringObject(keyO, keySds);
    robj *key = &keyO;

    if (old->FExpires()) {
        if (fRemoveExpire) {
            ::removeExpire(this, key);
        }
        else {
            if (val->getrefcount(std::memory_order_relaxed) == OBJ_SHARED_REFCOUNT)
                val = dupStringObject(val);
            ::updateExpire(this, itr.key(), old, val);
        }
    }

    if (g_pserver->maxmemory_policy & MAXMEMORY_FLAG_LFU) {
        val->lru = old->lru;
    }
    if (fUpdateMvcc) {
        if (val->getrefcount(std::memory_order_relaxed) == OBJ_SHARED_REFCOUNT)
            val = dupStringObject(val);
        setMvccTstamp(val, getMvccTstamp());
    }

    /* Although the key is not really deleted from the database, we regard 
    overwrite as two steps of unlink+add, so we still need to call the unlink 
    callback of the module. */
    moduleNotifyKeyUnlink(key,old);
    
    if (g_pserver->lazyfree_lazy_server_del)
        freeObjAsync(key, itr.val());
    else
        decrRefCount(itr.val());

    updateValue(itr, val);
}

/* Overwrite an existing key with a new value. Incrementing the reference
 * count of the new value is up to the caller.
 * This function does not modify the expire time of the existing key.
 *
 * The program is aborted if the key was not already present. */
void dbOverwrite(redisDb *db, robj *key, robj *val, bool fRemoveExpire, dict_iter *pitrExisting) {
    redisDb::iter itr;
    if (pitrExisting != nullptr)
        itr = *pitrExisting;
    else
        itr = db->find(key);

    serverAssertWithInfo(NULL,key,itr != nullptr);
    lookupKeyUpdateObj(itr.val(), LOOKUP_NONE);
    db->dbOverwriteCore(itr, szFromObj(key), val, !!g_pserver->fActiveReplica, fRemoveExpire);
}

/* Insert a key, handling duplicate keys according to fReplace */
int dbMerge(redisDb *db, sds key, robj *val, int fReplace)
{
    if (fReplace)
    {
        auto itr = db->find(key);
        if (itr == nullptr)
            return (dbAddCore(db, key, val, false /* fUpdateMvcc */) == true);

        robj_roptr old = itr.val();
        if (mvccFromObj(old) <= mvccFromObj(val))
        {
            db->dbOverwriteCore(itr, key, val, false, true);
            return true;
        }

        return false;
    }
    else
    {
        return (dbAddCore(db, key, val, true /* fUpdateMvcc */, true /* fAssumeNew */) == true);
    }
}

/* High level Set operation. This function can be used in order to set
 * a key, whatever it was existing or not, to a new object.
 *
 * 1) The ref count of the value object is incremented.
 * 2) clients WATCHing for the destination key notified.
 * 3) The expire time of the key is reset (the key is made persistent),
 *    unless 'keepttl' is true.
 *
 * All the new keys in the database should be created via this interface.
 * The client 'c' argument may be set to NULL if the operation is performed
 * in a context where there is no clear client performing the operation. */
void genericSetKey(client *c, redisDb *db, robj *key, robj *val, int keepttl, int signal) {
    db->prepOverwriteForSnapshot(szFromObj(key));
    dict_iter iter;
    if (!dbAddCore(db, szFromObj(key), val, true /* fUpdateMvcc */, false /*fAssumeNew*/, &iter)) {
        dbOverwrite(db, key, val, !keepttl, &iter);
    }
    incrRefCount(val);
    if (signal) signalModifiedKey(c,db,key);
}

/* Common case for genericSetKey() where the TTL is not retained. */
void setKey(client *c, redisDb *db, robj *key, robj *val) {
    genericSetKey(c,db,key,val,0,1);
}

/* Return a random key, in form of a Redis object.
 * If there are no keys, NULL is returned.
 *
 * The function makes sure to return keys not already expired. */
robj *dbRandomKey(redisDb *db) {
    int maxtries = 100;
    bool allvolatile = db->expireSize() == db->size();

    while(1) {
        sds key;
        robj *keyobj;

        auto itr = db->random();
        if (itr == nullptr) return NULL;

        key = itr.key();
        keyobj = createStringObject(key,sdslen(key));

        if (itr.val()->FExpires())
        {
            if (allvolatile && listLength(g_pserver->masters) && --maxtries == 0) {
                /* If the DB is composed only of keys with an expire set,
                    * it could happen that all the keys are already logically
                    * expired in the replica, so the function cannot stop because
                    * expireIfNeeded() is false, nor it can stop because
                    * dictGetRandomKey() returns NULL (there are keys to return).
                    * To prevent the infinite loop we do some tries, but if there
                    * are the conditions for an infinite loop, eventually we
                    * return a key name that may be already expired. */
                return keyobj;
            }
        }
            
        if (itr.val()->FExpires())
        {
             if (expireIfNeeded(db,keyobj)) {
                decrRefCount(keyobj);
                continue; /* search for another key. This expired. */
             }
        }
        
        return keyobj;
    }
}

bool redisDbPersistentData::syncDelete(robj *key)
{
    /* Deleting an entry from the expires dict will not free the sds of
     * the key, because it is shared with the main dictionary. */

    auto itr = find(szFromObj(key));
    if (itr != nullptr && itr.val()->FExpires())
        removeExpire(key, itr);
    
    robj_sharedptr val(itr.val());
    bool fDeleted = false;
    if (m_spstorage != nullptr)
        fDeleted = m_spstorage->erase(szFromObj(key));
    fDeleted = (dictDelete(m_pdict,ptrFromObj(key)) == DICT_OK) || fDeleted;

    if (fDeleted) {
        /* Tells the module that the key has been unlinked from the database. */
        moduleNotifyKeyUnlink(key,val); // MODULE Compat Note: We should be giving the actual key value here
        
        dictEntry *de = dictUnlink(m_dictChanged, szFromObj(key));
        if (de != nullptr)
        {
            bool fUpdate = (bool)dictGetVal(de);
            if (!fUpdate)
                --m_cnewKeysPending;
            dictFreeUnlinkedEntry(m_dictChanged, de);
        }
        
        if (m_pdbSnapshot != nullptr)
        {
            auto itr = m_pdbSnapshot->find_cached_threadsafe(szFromObj(key));
            if (itr != nullptr)
            {
                sds keyTombstone = sdsdupshared(itr.key());
                uint64_t hash = dictGetHash(m_pdict, keyTombstone);
                if (dictAdd(m_pdictTombstone, keyTombstone, (void*)hash) != DICT_OK)
                    sdsfree(keyTombstone);
            }
        }
        if (g_pserver->cluster_enabled) slotToKeyDel(szFromObj(key));
        return 1;
    } else {
        return 0;
    }
}

/* Delete a key, value, and associated expiration entry if any, from the DB */
int dbSyncDelete(redisDb *db, robj *key) {
    return db->syncDelete(key);
}

/* This is a wrapper whose behavior depends on the Redis lazy free
 * configuration. Deletes the key synchronously or asynchronously. */
int dbDelete(redisDb *db, robj *key) {
    return g_pserver->lazyfree_lazy_server_del ? dbAsyncDelete(db,key) :
                                             dbSyncDelete(db,key);
}

/* Prepare the string object stored at 'key' to be modified destructively
 * to implement commands like SETBIT or APPEND.
 *
 * An object is usually ready to be modified unless one of the two conditions
 * are true:
 *
 * 1) The object 'o' is shared (refcount > 1), we don't want to affect
 *    other users.
 * 2) The object encoding is not "RAW".
 *
 * If the object is found in one of the above conditions (or both) by the
 * function, an unshared / not-encoded copy of the string object is stored
 * at 'key' in the specified 'db'. Otherwise the object 'o' itself is
 * returned.
 *
 * USAGE:
 *
 * The object 'o' is what the caller already obtained by looking up 'key'
 * in 'db', the usage pattern looks like this:
 *
 * o = lookupKeyWrite(db,key);
 * if (checkType(c,o,OBJ_STRING)) return;
 * o = dbUnshareStringValue(db,key,o);
 *
 * At this point the caller is ready to modify the object, for example
 * using an sdscat() call to append some data, or anything else.
 */
robj *dbUnshareStringValue(redisDb *db, robj *key, robj *o) {
    serverAssert(o->type == OBJ_STRING);
    if (o->getrefcount(std::memory_order_relaxed) != 1 || o->encoding != OBJ_ENCODING_RAW) {
        robj *decoded = getDecodedObject(o);
        o = createRawStringObject(szFromObj(decoded), sdslen(szFromObj(decoded)));
        decrRefCount(decoded);
        dbOverwrite(db,key,o);
    }
    return o;
}

/* Remove all keys from the database(s) structure. The dbarray argument
 * may not be the server main DBs (could be a backup).
 *
 * The dbnum can be -1 if all the DBs should be emptied, or the specified
 * DB index if we want to empty only a single database.
 * The function returns the number of keys removed from the database(s). */
long long emptyDbStructure(redisDb **dbarray, int dbnum, int async,
                           void(callback)(void*))
{
    long long removed = 0;
    int startdb, enddb;

    if (dbnum == -1) {
        startdb = 0;
        enddb = cserver.dbnum-1;
    } else {
        startdb = enddb = dbnum;
    }

    for (int j = startdb; j <= enddb; j++) {
        removed += dbarray[j]->size();
        dbarray[j]->clear(async, callback);
        /* Because all keys of database are removed, reset average ttl. */
        dbarray[j]->avg_ttl = 0;
    }

    return removed;
}

/* Remove all keys from all the databases in a Redis DB.
 * If callback is given the function is called from time to time to
 * signal that work is in progress.
 *
 * The dbnum can be -1 if all the DBs should be flushed, or the specified
 * DB number if we want to flush only a single Redis database number.
 *
 * Flags are be EMPTYDB_NO_FLAGS if no special flags are specified or
 * EMPTYDB_ASYNC if we want the memory to be freed in a different thread
 * and the function to return ASAP.
 *
 * On success the function returns the number of keys removed from the
 * database(s). Otherwise -1 is returned in the specific case the
 * DB number is out of range, and errno is set to EINVAL. */
long long emptyDb(int dbnum, int flags, void(callback)(void*)) {
    int async = (flags & EMPTYDB_ASYNC);
    RedisModuleFlushInfoV1 fi = {REDISMODULE_FLUSHINFO_VERSION,!async,dbnum};
    long long removed = 0;

    if (dbnum < -1 || dbnum >= cserver.dbnum) {
        errno = EINVAL;
        return -1;
    }

    /* Fire the flushdb modules event. */
    moduleFireServerEvent(REDISMODULE_EVENT_FLUSHDB,
                          REDISMODULE_SUBEVENT_FLUSHDB_START,
                          &fi);

    /* Make sure the WATCHed keys are affected by the FLUSH* commands.
     * Note that we need to call the function while the keys are still
     * there. */
    signalFlushedDb(dbnum, async);

    /* Empty redis database structure. */
    removed = emptyDbStructure(g_pserver->db, dbnum, async, callback);

    /* Flush slots to keys map if enable cluster, we can flush entire
     * slots to keys map whatever dbnum because only support one DB
     * in cluster mode. */
    if (g_pserver->cluster_enabled) slotToKeyFlush(async);

    if (dbnum == -1) flushSlaveKeysWithExpireList();

    /* Also fire the end event. Note that this event will fire almost
     * immediately after the start event if the flush is asynchronous. */
    moduleFireServerEvent(REDISMODULE_EVENT_FLUSHDB,
                          REDISMODULE_SUBEVENT_FLUSHDB_END,
                          &fi);

    return removed;
}

/* Store a backup of the database for later use, and put an empty one
 * instead of it. */
const dbBackup *backupDb(void) {
    dbBackup *backup = new dbBackup();
    
    backup->dbarray = (const redisDbPersistentDataSnapshot**)zmalloc(sizeof(redisDbPersistentDataSnapshot*) * cserver.dbnum);
    for (int i=0; i<cserver.dbnum; i++) {
        backup->dbarray[i] = g_pserver->db[i]->createSnapshot(LLONG_MAX, false);
    }

    /* Backup cluster slots to keys map if enable cluster. */
    if (g_pserver->cluster_enabled) {
        backup->slots_to_keys = g_pserver->cluster->slots_to_keys;
        memcpy(backup->slots_keys_count, g_pserver->cluster->slots_keys_count,
            sizeof(g_pserver->cluster->slots_keys_count));
        g_pserver->cluster->slots_to_keys = raxNew();
        memset(g_pserver->cluster->slots_keys_count, 0,
            sizeof(g_pserver->cluster->slots_keys_count));
    }

    moduleFireServerEvent(REDISMODULE_EVENT_REPL_BACKUP,
                          REDISMODULE_SUBEVENT_REPL_BACKUP_CREATE,
                          NULL);

    return backup;
}

/* Discard a previously created backup, this can be slow (similar to FLUSHALL)
 * Arguments are similar to the ones of emptyDb, see EMPTYDB_ flags. */
void discardDbBackup(const dbBackup *backup, int flags, void(callback)(void*)) {
    UNUSED(callback);
    int async = (flags & EMPTYDB_ASYNC);

    /* Release main DBs backup . */
    for (int i=0; i<cserver.dbnum; i++) {
        g_pserver->db[i]->endSnapshot(backup->dbarray[i]);
    }

    /* Release slots to keys map backup if enable cluster. */
    if (g_pserver->cluster_enabled) freeSlotsToKeysMap(backup->slots_to_keys, async);

    /* Release buckup. */
    zfree(backup->dbarray);
    delete backup;

    moduleFireServerEvent(REDISMODULE_EVENT_REPL_BACKUP,
                          REDISMODULE_SUBEVENT_REPL_BACKUP_DISCARD,
                          NULL);
}

/* Restore the previously created backup (discarding what currently resides
 * in the db).
 * This function should be called after the current contents of the database
 * was emptied with a previous call to emptyDb (possibly using the async mode). */
void restoreDbBackup(const dbBackup *backup) {
    /* Restore main DBs. */
    for (int i=0; i<cserver.dbnum; i++) {
        g_pserver->db[i]->restoreSnapshot(backup->dbarray[i]);
    }
    
    /* Restore slots to keys map backup if enable cluster. */
    if (g_pserver->cluster_enabled) {
        serverAssert(g_pserver->cluster->slots_to_keys->numele == 0);
        raxFree(g_pserver->cluster->slots_to_keys);
        g_pserver->cluster->slots_to_keys = backup->slots_to_keys;
        memcpy(g_pserver->cluster->slots_keys_count, backup->slots_keys_count,
                sizeof(g_pserver->cluster->slots_keys_count));
    }

    /* Release buckup. */
    zfree(backup->dbarray);
    delete backup;

    moduleFireServerEvent(REDISMODULE_EVENT_REPL_BACKUP,
                          REDISMODULE_SUBEVENT_REPL_BACKUP_RESTORE,
                          NULL);
}

int selectDb(client *c, int id) {
    if (id < 0 || id >= cserver.dbnum)
        return C_ERR;
    c->db = g_pserver->db[id];
    return C_OK;
}

long long dbTotalServerKeyCount() {
    long long total = 0;
    int j;
    for (j = 0; j < cserver.dbnum; j++) {
        total += g_pserver->db[j]->size();
    }
    return total;
}

/*-----------------------------------------------------------------------------
 * Hooks for key space changes.
 *
 * Every time a key in the database is modified the function
 * signalModifiedKey() is called.
 *
 * Every time a DB is flushed the function signalFlushDb() is called.
 *----------------------------------------------------------------------------*/

/* Note that the 'c' argument may be NULL if the key was modified out of
 * a context of a client. */
void signalModifiedKey(client *c, redisDb *db, robj *key) {
    touchWatchedKey(db,key);
    trackingInvalidateKey(c,key);
}

void signalFlushedDb(int dbid, int async) {
    int startdb, enddb;
    if (dbid == -1) {
        startdb = 0;
        enddb = cserver.dbnum-1;
    } else {
        startdb = enddb = dbid;
    }

    for (int j = startdb; j <= enddb; j++) {
        touchAllWatchedKeysInDb(g_pserver->db[j], NULL);
    }

    trackingInvalidateKeysOnFlush(async);
}

/*-----------------------------------------------------------------------------
 * Type agnostic commands operating on the key space
 *----------------------------------------------------------------------------*/

/* Return the set of flags to use for the emptyDb() call for FLUSHALL
 * and FLUSHDB commands.
 *
 * sync: flushes the database in an sync manner.
 * async: flushes the database in an async manner.
 * no option: determine sync or async according to the value of lazyfree-lazy-user-flush.
 *
 * On success C_OK is returned and the flags are stored in *flags, otherwise
 * C_ERR is returned and the function sends an error to the client. */
int getFlushCommandFlags(client *c, int *flags) {
    /* Parse the optional ASYNC option. */
    if (c->argc == 2 && !strcasecmp(szFromObj(c->argv[1]),"sync")) {
        *flags = EMPTYDB_NO_FLAGS;
    } else if (c->argc == 2 && !strcasecmp(szFromObj(c->argv[1]),"async")) {
        *flags = EMPTYDB_ASYNC;
    } else if (c->argc == 1) {
        *flags = g_pserver->lazyfree_lazy_user_flush ? EMPTYDB_ASYNC : EMPTYDB_NO_FLAGS;
    } else {
        addReplyErrorObject(c,shared.syntaxerr);
        return C_ERR;
    }
    return C_OK;
}

/* Flushes the whole server data set. */
void flushAllDataAndResetRDB(int flags) {
    g_pserver->dirty += emptyDb(-1,flags,NULL);
    if (g_pserver->FRdbSaveInProgress()) killRDBChild();
    if (g_pserver->saveparamslen > 0) {
        /* Normally rdbSave() will reset dirty, but we don't want this here
         * as otherwise FLUSHALL will not be replicated nor put into the AOF. */
        int saved_dirty = g_pserver->dirty;
        rdbSaveInfo rsi, *rsiptr;
        rsiptr = rdbPopulateSaveInfo(&rsi);
        rdbSave(nullptr, rsiptr);
        g_pserver->dirty = saved_dirty;
    }
    
    /* Without that extra dirty++, when db was already empty, FLUSHALL will
     * not be replicated nor put into the AOF. */
    g_pserver->dirty++;
#if defined(USE_JEMALLOC)
    /* jemalloc 5 doesn't release pages back to the OS when there's no traffic.
     * for large databases, flushdb blocks for long anyway, so a bit more won't
     * harm and this way the flush and purge will be synchroneus. */
    if (!(flags & EMPTYDB_ASYNC))
        jemalloc_purge();
#endif
}

/* FLUSHDB [ASYNC]
 *
 * Flushes the currently SELECTed Redis DB. */
void flushdbCommand(client *c) {
    int flags;

    if (c->argc == 2)
    {
        if (!strcasecmp(szFromObj(c->argv[1]), "cache"))
        {
            if (g_pserver->m_pstorageFactory == nullptr)
            {
                addReplyError(c, "Cannot flush cache without a storage provider set");
                return;
            }
            c->db->removeAllCachedValues();
            addReply(c,shared.ok);
            return;
        }
    }

    if (getFlushCommandFlags(c,&flags) == C_ERR) return;
    g_pserver->dirty += emptyDb(c->db->id,flags,NULL);
    addReply(c,shared.ok);
#if defined(USE_JEMALLOC)
    /* jemalloc 5 doesn't release pages back to the OS when there's no traffic.
     * for large databases, flushdb blocks for long anyway, so a bit more won't
     * harm and this way the flush and purge will be synchroneus. */
    if (!(flags & EMPTYDB_ASYNC))
        jemalloc_purge();
#endif
}

/* FLUSHALL [ASYNC]
 *
 * Flushes the whole server data set. */
void flushallCommand(client *c) {
    int flags;

    if (c->argc == 2)
    {
        if (!strcasecmp(szFromObj(c->argv[1]), "cache"))
        {
            if (g_pserver->m_pstorageFactory == nullptr)
            {
                addReplyError(c, "Cannot flush cache without a storage provider set");
                return;
            }
            for (int idb = 0; idb < cserver.dbnum; ++idb)
                g_pserver->db[idb]->removeAllCachedValues();
            addReply(c,shared.ok);
            return;
        }
    }

    if (getFlushCommandFlags(c,&flags) == C_ERR) return;
    flushAllDataAndResetRDB(flags);
    addReply(c,shared.ok);
}

/* This command implements DEL and LAZYDEL. */
void delGenericCommand(client *c, int lazy) {
    int numdel = 0, j;

    for (j = 1; j < c->argc; j++) {
        expireIfNeeded(c->db,c->argv[j]);
        int deleted  = lazy ? dbAsyncDelete(c->db,c->argv[j]) :
                              dbSyncDelete(c->db,c->argv[j]);
        if (deleted) {
            signalModifiedKey(c,c->db,c->argv[j]);
            notifyKeyspaceEvent(NOTIFY_GENERIC,
                "del",c->argv[j],c->db->id);
            g_pserver->dirty++;
            numdel++;
        }
    }
    addReplyLongLong(c,numdel);
}

void delCommand(client *c) {
    delGenericCommand(c,g_pserver->lazyfree_lazy_user_del);
}

void unlinkCommand(client *c) {
    delGenericCommand(c,1);
}

/* EXISTS key1 key2 ... key_N.
 * Return value is the number of keys existing. */
void existsCommand(client *c) {
    long long count = 0;
    int j;

    for (j = 1; j < c->argc; j++) {
        if (lookupKeyReadWithFlags(c->db,c->argv[j],LOOKUP_NOTOUCH)) count++;
    }
    addReplyLongLong(c,count);
}

void mexistsCommand(client *c) {
    addReplyArrayLen(c, c->argc - 1);
    for (int j = 1; j < c->argc; ++j) {
        addReplyBool(c, lookupKeyRead(c->db, c->argv[j]));
    }
}

void selectCommand(client *c) {
    int id;

    if (getIntFromObjectOrReply(c, c->argv[1], &id, NULL) != C_OK)
        return;

    if (g_pserver->cluster_enabled && id != 0) {
        addReplyError(c,"SELECT is not allowed in cluster mode");
        return;
    }
    if (selectDb(c,id) == C_ERR) {
        addReplyError(c,"DB index is out of range");
    } else {
        addReply(c,shared.ok);
    }
}

void randomkeyCommand(client *c) {
    robj *key;

    if ((key = dbRandomKey(c->db)) == NULL) {
        addReplyNull(c);
        return;
    }

    addReplyBulk(c,key);
    decrRefCount(key);
}

bool redisDbPersistentData::iterate(std::function<bool(const char*, robj*)> fn)
{
    dictIterator *di = dictGetSafeIterator(m_pdict);
    dictEntry *de = nullptr;
    bool fResult = true;
    while(fResult && ((de = dictNext(di)) != nullptr))
    {
        if (!fn((const char*)dictGetKey(de), (robj*)dictGetVal(de)))
            fResult = false;
    }
    dictReleaseIterator(di);

    if (m_spstorage != nullptr)
    {
        bool fSawAll = fResult && m_spstorage->enumerate([&](const char *key, size_t cchKey, const void *, size_t )->bool{
            sds sdsKey = sdsnewlen(key, cchKey);
            bool fContinue = true;
            if (dictFind(m_pdict, sdsKey) == nullptr)
            {
                ensure(sdsKey, &de);
                fContinue = fn((const char*)dictGetKey(de), (robj*)dictGetVal(de));
                removeCachedValue(sdsKey);
            }
            sdsfree(sdsKey);
            return fContinue;
        });
        return fSawAll;
    }

    if (fResult && m_pdbSnapshot != nullptr)
    {
        fResult = m_pdbSnapshot->iterate_threadsafe([&](const char *key, robj_roptr){
            // Before passing off to the user we need to make sure it's not already in the
            //  the current set, and not deleted
            dictEntry *deCurrent = dictFind(m_pdict, key);
            if (deCurrent != nullptr)
                return true;
            dictEntry *deTombstone = dictFind(m_pdictTombstone, key);
            if (deTombstone != nullptr)
                return true;

            // Alright it's a key in the use keyspace, lets ensure it and then pass it off
            ensure(key);
            deCurrent = dictFind(m_pdict, key);
            return fn(key, (robj*)dictGetVal(deCurrent));
        }, true /*fKeyOnly*/);
    }
    
    return fResult;
}

client *createAOFClient(void);
void freeFakeClient(client *);
void keysCommandCore(client *cIn, const redisDbPersistentDataSnapshot *db, sds pattern)
{
    int plen = sdslen(pattern), allkeys;
    unsigned long numkeys = 0;

    client *c = createAOFClient();
    c->flags |= CLIENT_FORCE_REPLY;

    void *replylen = addReplyDeferredLen(c);

    allkeys = (pattern[0] == '*' && plen == 1);
    db->iterate_threadsafe([&](const char *key, robj_roptr)->bool {
        robj *keyobj;

        if (allkeys || stringmatchlen(pattern,plen,key,sdslen(key),0)) {
            keyobj = createStringObject(key,sdslen(key));
            if (!keyIsExpired(db,keyobj)) {
                addReplyBulk(c,keyobj);
                numkeys++;
            }
            decrRefCount(keyobj);
        }
        return !(cIn->flags.load(std::memory_order_relaxed) & CLIENT_CLOSE_ASAP);
    }, true /*fKeyOnly*/);
    
    setDeferredArrayLen(c,replylen,numkeys);

    aeAcquireLock();
    addReplyProto(cIn, c->buf, c->bufpos);
    listIter li;
    listNode *ln;
    listRewind(c->reply, &li);
    while ((ln = listNext(&li)) != nullptr)
    {
        clientReplyBlock *block = (clientReplyBlock*)listNodeValue(ln);
        addReplyProto(cIn, block->buf(), block->used);
    }
    aeReleaseLock();
    freeFakeClient(c);
}

int prepareClientToWrite(client *c, bool fAsync);
void keysCommand(client *c) {
    sds pattern = szFromObj(c->argv[1]);

    const redisDbPersistentDataSnapshot *snapshot = nullptr;
    if (!(c->flags & (CLIENT_MULTI | CLIENT_BLOCKED)))
        snapshot = c->db->createSnapshot(c->mvccCheckpoint, true /* fOptional */);
    if (snapshot != nullptr)
    {
        sds patternCopy = sdsdup(pattern);
        aeEventLoop *el = serverTL->el;
        blockClient(c, BLOCKED_ASYNC);
        redisDb *db = c->db;
        g_pserver->asyncworkqueue->AddWorkFunction([el, c, db, patternCopy, snapshot]{
            keysCommandCore(c, snapshot, patternCopy);
            sdsfree(patternCopy);
            aePostFunction(el, [c, db, snapshot]{
                aeReleaseLock();    // we need to lock with coordination of the client

                std::unique_lock<decltype(c->lock)> lock(c->lock);
                AeLocker locker;
                locker.arm(c);

                unblockClient(c);

                locker.disarm();
                lock.unlock();
                db->endSnapshotAsync(snapshot);
                aeAcquireLock();
            });
        });
    }
    else
    {
        keysCommandCore(c, c->db, pattern);
    }
}

/* This callback is used by scanGenericCommand in order to collect elements
 * returned by the dictionary iterator into a list. */
void scanCallback(void *privdata, const dictEntry *de) {
    void **pd = (void**) privdata;
    list *keys = (list*)pd[0];
    robj *o = (robj*)pd[1];
    robj *key, *val = NULL;

    if (o == NULL) {
        sds sdskey = (sds)dictGetKey(de);
        key = createStringObject(sdskey, sdslen(sdskey));
    } else if (o->type == OBJ_SET) {
        sds keysds = (sds)dictGetKey(de);
        key = createStringObject(keysds,sdslen(keysds));
    } else if (o->type == OBJ_HASH) {
        sds sdskey = (sds)dictGetKey(de);
        sds sdsval = (sds)dictGetVal(de);
        key = createStringObject(sdskey,sdslen(sdskey));
        val = createStringObject(sdsval,sdslen(sdsval));
    } else if (o->type == OBJ_ZSET) {
        sds sdskey = (sds)dictGetKey(de);
        key = createStringObject(sdskey,sdslen(sdskey));
        val = createStringObjectFromLongDouble(*(double*)dictGetVal(de),0);
    } else {
        serverPanic("Type not handled in SCAN callback.");
    }

    listAddNodeTail(keys, key);
    if (val) listAddNodeTail(keys, val);
}

/* Try to parse a SCAN cursor stored at object 'o':
 * if the cursor is valid, store it as unsigned integer into *cursor and
 * returns C_OK. Otherwise return C_ERR and send an error to the
 * client. */
int parseScanCursorOrReply(client *c, robj *o, unsigned long *cursor) {
    char *eptr;

    /* Use strtoul() because we need an *unsigned* long, so
     * getLongLongFromObject() does not cover the whole cursor space. */
    errno = 0;
    *cursor = strtoul(szFromObj(o), &eptr, 10);
    if (isspace(((char*)ptrFromObj(o))[0]) || eptr[0] != '\0' || errno == ERANGE)
    {
        addReplyError(c, "invalid cursor");
        return C_ERR;
    }
    return C_OK;
}


static bool filterKey(robj_roptr kobj, sds pat, int patlen)
{
    bool filter = false;
    if (sdsEncodedObject(kobj)) {
        if (!stringmatchlen(pat, patlen, szFromObj(kobj), sdslen(szFromObj(kobj)), 0))
            filter = true;
    } else {
        char buf[LONG_STR_SIZE];
        int len;

        serverAssert(kobj->encoding == OBJ_ENCODING_INT);
        len = ll2string(buf,sizeof(buf),(long)ptrFromObj(kobj));
        if (!stringmatchlen(pat, patlen, buf, len, 0)) filter = true;
    }
    return filter;
}

/* This command implements SCAN, HSCAN and SSCAN commands.
 * If object 'o' is passed, then it must be a Hash, Set or Zset object, otherwise
 * if 'o' is NULL the command will operate on the dictionary associated with
 * the current database.
 *
 * When 'o' is not NULL the function assumes that the first argument in
 * the client arguments vector is a key so it skips it before iterating
 * in order to parse options.
 *
 * In the case of a Hash object the function returns both the field and value
 * of every element on the Hash. */
void scanFilterAndReply(client *c, list *keys, sds pat, sds type, int use_pattern, robj_roptr o, unsigned long cursor);
void scanGenericCommand(client *c, robj_roptr o, unsigned long cursor) {
    int i, j;
    list *keys = listCreate();
    long count = 10;
    sds pat = NULL;
    sds type = NULL;
    int patlen = 0, use_pattern = 0;
    dict *ht;

    /* Object must be NULL (to iterate keys names), or the type of the object
     * must be Set, Sorted Set, or Hash. */
    serverAssert(o == nullptr || o->type == OBJ_SET || o->type == OBJ_HASH ||
                o->type == OBJ_ZSET);

    /* Set i to the first option argument. The previous one is the cursor. */
    i = (o == nullptr) ? 2 : 3; /* Skip the key argument if needed. */

    /* Step 1: Parse options. */
    while (i < c->argc) {
        j = c->argc - i;
        if (!strcasecmp(szFromObj(c->argv[i]), "count") && j >= 2) {
            if (getLongFromObjectOrReply(c, c->argv[i+1], &count, NULL)
                != C_OK)
            {
                goto cleanup;
            }

            if (count < 1) {
                addReplyErrorObject(c,shared.syntaxerr);
                goto cleanup;
            }

            i += 2;
        } else if (!strcasecmp(szFromObj(c->argv[i]), "match") && j >= 2) {
            pat = szFromObj(c->argv[i+1]);
            patlen = sdslen(pat);

            /* The pattern always matches if it is exactly "*", so it is
             * equivalent to disabling it. */
            use_pattern = !(pat[0] == '*' && patlen == 1);

            i += 2;
        } else if (!strcasecmp(szFromObj(c->argv[i]), "type") && o == nullptr && j >= 2) {
            /* SCAN for a particular type only applies to the db dict */
            type = szFromObj(c->argv[i+1]);
            i+= 2;
        } else {
            addReplyErrorObject(c,shared.syntaxerr);
            goto cleanup;
        }
    }

    if (o == nullptr && count >= 100)
    {
        // Do an async version
        if (c->asyncCommand(
            [c, keys, pat, type, cursor, count, use_pattern] (const redisDbPersistentDataSnapshot *snapshot, const std::vector<robj_sharedptr> &) {
                sds patCopy = pat ? sdsdup(pat) : nullptr;
                sds typeCopy = type ? sdsdup(type) : nullptr;
                auto cursorResult = snapshot->scan_threadsafe(cursor, count, typeCopy, keys);
                if (use_pattern) {
                    listNode *ln = listFirst(keys);
                    int patlen = sdslen(patCopy);
                    while (ln != nullptr)
                    {
                        listNode *next = ln->next;
                        if (filterKey((robj*)listNodeValue(ln), patCopy, patlen))
                        {
                            robj *kobj = (robj*)listNodeValue(ln);
                            decrRefCount(kobj);
                            listDelNode(keys, ln);
                        }
                        ln = next;
                    }
                }
                if (patCopy != nullptr)
                    sdsfree(patCopy);
                if (typeCopy != nullptr)
                    sdsfree(typeCopy);
                mstime_t timeScanFilter;
                latencyStartMonitor(timeScanFilter);
                scanFilterAndReply(c, keys, nullptr, nullptr, false, nullptr, cursorResult);
                latencyEndMonitor(timeScanFilter);
                latencyAddSampleIfNeeded("scan-async-filter", timeScanFilter);
            },
            [keys] (const redisDbPersistentDataSnapshot *) {
                listSetFreeMethod(keys,decrRefCountVoid);
                listRelease(keys);
            }
            )) {
            return;
        }
    }

    /* Step 2: Iterate the collection.
     *
     * Note that if the object is encoded with a ziplist, intset, or any other
     * representation that is not a hash table, we are sure that it is also
     * composed of a small number of elements. So to avoid taking state we
     * just return everything inside the object in a single call, setting the
     * cursor to zero to signal the end of the iteration. */

    /* Handle the case of a hash table. */
    ht = NULL;
    if (o == nullptr) {
        ht = c->db->dictUnsafeKeyOnly();
    } else if (o->type == OBJ_SET && o->encoding == OBJ_ENCODING_HT) {
        ht = (dict*)ptrFromObj(o);
    } else if (o->type == OBJ_HASH && o->encoding == OBJ_ENCODING_HT) {
        ht = (dict*)ptrFromObj(o);
        count *= 2; /* We return key / value for this type. */
    } else if (o->type == OBJ_ZSET && o->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = (zset*)ptrFromObj(o);
        ht = zs->dict;
        count *= 2; /* We return key / value for this type. */
    }

    if (ht) {
        if (ht == c->db->dictUnsafeKeyOnly())
        {
            cursor = c->db->scan_threadsafe(cursor, count, nullptr, keys);
        }
        else
        {
            void *privdata[2];
            /* We set the max number of iterations to ten times the specified
            * COUNT, so if the hash table is in a pathological state (very
            * sparsely populated) we avoid to block too much time at the cost
            * of returning no or very few elements. */
            long maxiterations = count*10;

            /* We pass two pointers to the callback: the list to which it will
            * add new elements, and the object containing the dictionary so that
            * it is possible to fetch more data in a type-dependent way. */
            privdata[0] = keys;
            privdata[1] = o.unsafe_robjcast();
            do {
                cursor = dictScan(ht, cursor, scanCallback, NULL, privdata);
            } while (cursor &&
                maxiterations-- &&
                listLength(keys) < (unsigned long)count);
        }
    } else if (o->type == OBJ_SET) {
        int pos = 0;
        int64_t ll;

        while(intsetGet((intset*)ptrFromObj(o),pos++,&ll))
            listAddNodeTail(keys,createStringObjectFromLongLong(ll));
        cursor = 0;
    } else if (o->type == OBJ_HASH || o->type == OBJ_ZSET) {
        unsigned char *p = ziplistIndex((unsigned char*)ptrFromObj(o),0);
        unsigned char *vstr;
        unsigned int vlen;
        long long vll;

        while(p) {
            ziplistGet(p,&vstr,&vlen,&vll);
            listAddNodeTail(keys,
                (vstr != NULL) ? createStringObject((char*)vstr,vlen) :
                                 createStringObjectFromLongLong(vll));
            p = ziplistNext((unsigned char*)ptrFromObj(o),p);
        }
        cursor = 0;
    } else {
        serverPanic("Not handled encoding in SCAN.");
    }

    scanFilterAndReply(c, keys, pat, type, use_pattern, o, cursor);

cleanup:
    listSetFreeMethod(keys,decrRefCountVoid);
    listRelease(keys);
}

void scanFilterAndReply(client *c, list *keys, sds pat, sds type, int use_pattern, robj_roptr o, unsigned long cursor)
{
    listNode *node, *nextnode;
    int patlen = (pat != nullptr) ? sdslen(pat) : 0;
    
    /* Step 3: Filter elements. */
    node = listFirst(keys);
    while (node) {
        robj *kobj = (robj*)listNodeValue(node);
        nextnode = listNextNode(node);
        int filter = 0;

        /* Filter element if it does not match the pattern. */
        if (use_pattern) {
            if (filterKey(kobj, pat, patlen))
                filter = 1;
        }

        /* Filter an element if it isn't the type we want. */
        if (!filter && o == nullptr && type){
            robj_roptr typecheck = lookupKeyReadWithFlags(c->db, kobj, LOOKUP_NOTOUCH);
            const char* typeT = getObjectTypeName(typecheck);
            if (strcasecmp((char*) type, typeT)) filter = 1;
        }

        /* Filter element if it is an expired key. */
        if (!filter && o == nullptr && expireIfNeeded(c->db, kobj)) filter = 1;

        /* Remove the element and its associated value if needed. */
        if (filter) {
            decrRefCount(kobj);
            listDelNode(keys, node);
        }

        /* If this is a hash or a sorted set, we have a flat list of
         * key-value elements, so if this element was filtered, remove the
         * value, or skip it if it was not filtered: we only match keys. */
        if (o && (o->type == OBJ_ZSET || o->type == OBJ_HASH)) {
            node = nextnode;
            serverAssert(node); /* assertion for valgrind (avoid NPD) */
            nextnode = listNextNode(node);
            if (filter) {
                kobj = (robj*)listNodeValue(node);
                decrRefCount(kobj);
                listDelNode(keys, node);
            }
        }
        node = nextnode;
    }

    /* Step 4: Reply to the client. */
    addReplyArrayLen(c, 2);
    addReplyBulkLongLong(c,cursor);

    addReplyArrayLen(c, listLength(keys));
    while ((node = listFirst(keys)) != NULL) {
        robj *kobj = (robj*)listNodeValue(node);
        addReplyBulk(c, kobj);
        decrRefCount(kobj);
        listDelNode(keys, node);
    }
}

/* The SCAN command completely relies on scanGenericCommand. */
void scanCommand(client *c) {
    unsigned long cursor;
    if (parseScanCursorOrReply(c,c->argv[1],&cursor) == C_ERR) return;
    scanGenericCommand(c,nullptr,cursor);
}

void dbsizeCommand(client *c) {
    addReplyLongLong(c,c->db->size());
}

void lastsaveCommand(client *c) {
    addReplyLongLong(c,g_pserver->lastsave);
}

const char* getObjectTypeName(robj_roptr o) {
    const char* type;
    if (o == nullptr) {
        type = "none";
    } else {
        switch(o->type) {
        case OBJ_STRING: type = "string"; break;
        case OBJ_LIST: type = "list"; break;
        case OBJ_SET: type = "set"; break;
        case OBJ_ZSET: type = "zset"; break;
        case OBJ_HASH: type = "hash"; break;
        case OBJ_STREAM: type = "stream"; break;
        case OBJ_MODULE: {
            moduleValue *mv = (moduleValue*)ptrFromObj(o);
            type = mv->type->name;
        }; break;
        default: type = "unknown"; break;
        }
    }
    return type;
}

void typeCommand(client *c) {
    robj_roptr o = lookupKeyReadWithFlags(c->db,c->argv[1],LOOKUP_NOTOUCH);
    addReplyStatus(c, getObjectTypeName(o));
}

void shutdownCommand(client *c) {
    int flags = 0;

    if (c->argc > 2) {
        addReplyErrorObject(c,shared.syntaxerr);
        return;
    } else if (c->argc == 2) {
        if (!strcasecmp(szFromObj(c->argv[1]),"nosave")) {
            flags |= SHUTDOWN_NOSAVE;
        } else if (!strcasecmp(szFromObj(c->argv[1]),"save")) {
            flags |= SHUTDOWN_SAVE;
        } else {
            addReplyErrorObject(c,shared.syntaxerr);
            return;
        }
    }
    if (prepareForShutdown(flags) == C_OK) throw ShutdownException();
    addReplyError(c,"Errors trying to SHUTDOWN. Check logs.");
}

void renameGenericCommand(client *c, int nx) {
    robj *o;
    int samekey = 0;

    /* When source and dest key is the same, no operation is performed,
     * if the key exists, however we still return an error on unexisting key. */
    if (sdscmp(szFromObj(c->argv[1]),szFromObj(c->argv[2])) == 0) samekey = 1;

    if ((o = lookupKeyWriteOrReply(c,c->argv[1],shared.nokeyerr)) == NULL)
        return;

    if (samekey) {
        addReply(c,nx ? shared.czero : shared.ok);
        return;
    }

    incrRefCount(o);

    std::unique_ptr<expireEntry> spexpire;

    {   // scope pexpireOld since it will be invalid soon
    std::unique_lock<fastlock> ul(g_expireLock);
    expireEntry *pexpireOld = c->db->getExpire(c->argv[1]);
    if (pexpireOld != nullptr)
        spexpire = std::make_unique<expireEntry>(std::move(*pexpireOld));
    }

    if (lookupKeyWrite(c->db,c->argv[2]) != NULL) {
        if (nx) {
            decrRefCount(o);
            addReply(c,shared.czero);
            return;
        }
        /* Overwrite: delete the old key before creating the new one
         * with the same name. */
        dbDelete(c->db,c->argv[2]);
    }
    dbDelete(c->db,c->argv[1]);
    dbAdd(c->db,c->argv[2],o);
    if (spexpire != nullptr) 
        setExpire(c,c->db,c->argv[2],std::move(*spexpire));
    signalModifiedKey(c,c->db,c->argv[1]);
    signalModifiedKey(c,c->db,c->argv[2]);
    notifyKeyspaceEvent(NOTIFY_GENERIC,"rename_from",
        c->argv[1],c->db->id);
    notifyKeyspaceEvent(NOTIFY_GENERIC,"rename_to",
        c->argv[2],c->db->id);
    g_pserver->dirty++;
    addReply(c,nx ? shared.cone : shared.ok);
}

void renameCommand(client *c) {
    renameGenericCommand(c,0);
}

void renamenxCommand(client *c) {
    renameGenericCommand(c,1);
}

void moveCommand(client *c) {
    robj *o;
    redisDb *src, *dst;
    int srcid, dbid;

    if (g_pserver->cluster_enabled) {
        addReplyError(c,"MOVE is not allowed in cluster mode");
        return;
    }

    /* Obtain source and target DB pointers */
    src = c->db;
    srcid = c->db->id;

    if (getIntFromObjectOrReply(c, c->argv[2], &dbid, NULL) != C_OK)
        return;

    if (selectDb(c,dbid) == C_ERR) {
        addReplyError(c,"DB index is out of range");
        return;
    }
    dst = c->db;
    selectDb(c,srcid); /* Back to the source DB */

    /* If the user is moving using as target the same
     * DB as the source DB it is probably an error. */
    if (src == dst) {
        addReplyErrorObject(c,shared.sameobjecterr);
        return;
    }

    /* Check if the element exists and get a reference */
    o = lookupKeyWrite(c->db,c->argv[1]);
    if (!o) {
        addReply(c,shared.czero);
        return;
    }

    std::unique_ptr<expireEntry> spexpire;
    {   // scope pexpireOld
    std::unique_lock<fastlock> ul(g_expireLock);
    expireEntry *pexpireOld = c->db->getExpire(c->argv[1]);
    if (pexpireOld != nullptr)
        spexpire = std::make_unique<expireEntry>(std::move(*pexpireOld));
    }
    if (o->FExpires())
        removeExpire(c->db,c->argv[1]);
    serverAssert(!o->FExpires());
    incrRefCount(o);
    dbDelete(src,c->argv[1]);
    g_pserver->dirty++;

    /* Return zero if the key already exists in the target DB */
    if (lookupKeyWrite(dst,c->argv[1]) != NULL) {
        addReply(c,shared.czero);
        decrRefCount(o);
        return;
    }
    dbAdd(dst,c->argv[1],o);
    if (spexpire != nullptr) setExpire(c,dst,c->argv[1],std::move(*spexpire));

    signalModifiedKey(c,src,c->argv[1]);
    signalModifiedKey(c,dst,c->argv[1]);
    notifyKeyspaceEvent(NOTIFY_GENERIC,
                "move_from",c->argv[1],src->id);
    notifyKeyspaceEvent(NOTIFY_GENERIC,
                "move_to",c->argv[1],dst->id);

    addReply(c,shared.cone);
}

void copyCommand(client *c) {
    robj *o;
    redisDb *src, *dst;
    int srcid, dbid;
    expireEntry *expire = nullptr;
    int j, replace = 0, fdelete = 0;

    /* Obtain source and target DB pointers 
     * Default target DB is the same as the source DB 
     * Parse the REPLACE option and targetDB option. */
    src = c->db;
    dst = c->db;
    srcid = c->db->id;
    dbid = c->db->id;
    for (j = 3; j < c->argc; j++) {
        int additional = c->argc - j - 1;
        if (!strcasecmp(szFromObj(c->argv[j]),"replace")) {
            replace = 1;
        } else if (!strcasecmp(szFromObj(c->argv[j]), "db") && additional >= 1) {
            if (getIntFromObjectOrReply(c, c->argv[j+1], &dbid, NULL) != C_OK)
                return;

            if (selectDb(c, dbid) == C_ERR) {
                addReplyError(c,"DB index is out of range");
                return;
            }
            dst = c->db;
            selectDb(c,srcid); /* Back to the source DB */
            j++; /* Consume additional arg. */
        } else {
            addReplyErrorObject(c,shared.syntaxerr);
            return;
        }
    }

    if ((g_pserver->cluster_enabled == 1) && (srcid != 0 || dbid != 0)) {
        addReplyError(c,"Copying to another database is not allowed in cluster mode");
        return;
    }

    /* If the user select the same DB as
     * the source DB and using newkey as the same key
     * it is probably an error. */
    robj *key = c->argv[1];
    robj *newkey = c->argv[2];
    if (src == dst && (sdscmp(szFromObj(key), szFromObj(newkey)) == 0)) {
        addReplyErrorObject(c,shared.sameobjecterr);
        return;
    }

    /* Check if the element exists and get a reference */
    o = lookupKeyWrite(c->db, key);
    if (!o) {
        addReply(c,shared.czero);
        return;
    }
    expire = c->db->getExpire(key);

    /* Return zero if the key already exists in the target DB. 
     * If REPLACE option is selected, delete newkey from targetDB. */
    if (lookupKeyWrite(dst,newkey) != NULL) {
        if (replace) {
            fdelete = 1;
        } else {
            addReply(c,shared.czero);
            return;
        }
    }

    /* Duplicate object according to object's type. */
    robj *newobj;
    switch(o->type) {
        case OBJ_STRING: newobj = dupStringObject(o); break;
        case OBJ_LIST: newobj = listTypeDup(o); break;
        case OBJ_SET: newobj = setTypeDup(o); break;
        case OBJ_ZSET: newobj = zsetDup(o); break;
        case OBJ_HASH: newobj = hashTypeDup(o); break;
        case OBJ_STREAM: newobj = streamDup(o); break;
        case OBJ_MODULE:
            newobj = moduleTypeDupOrReply(c, key, newkey, o);
            if (!newobj) return;
            break;
        default:
            addReplyError(c, "unknown type object");
            return;
    }

    if (fdelete) {
        dbDelete(dst,newkey);
    }

    dbAdd(dst,newkey,newobj);
    if (expire != nullptr)
        setExpire(c, dst, newkey, expire->duplicate());

    /* OK! key copied */
    signalModifiedKey(c,dst,c->argv[2]);
    notifyKeyspaceEvent(NOTIFY_GENERIC,"copy_to",c->argv[2],dst->id);

    g_pserver->dirty++;
    addReply(c,shared.cone);
}

/* Helper function for dbSwapDatabases(): scans the list of keys that have
 * one or more blocked clients for B[LR]POP or other blocking commands
 * and signal the keys as ready if they are of the right type. See the comment
 * where the function is used for more info. */
void scanDatabaseForReadyLists(redisDb *db) {
    dictEntry *de;
    dictIterator *di = dictGetSafeIterator(db->blocking_keys);
    while((de = dictNext(di)) != NULL) {
        robj *key = (robj*)dictGetKey(de);
        robj *value = lookupKey(db,key,LOOKUP_NOTOUCH);
        if (value) signalKeyAsReady(db, key, value->type);
    }
    dictReleaseIterator(di);
}

/* Swap two databases at runtime so that all clients will magically see
 * the new database even if already connected. Note that the client
 * structure c->db points to a given DB, so we need to be smarter and
 * swap the underlying referenced structures, otherwise we would need
 * to fix all the references to the Redis DB structure.
 *
 * Returns C_ERR if at least one of the DB ids are out of range, otherwise
 * C_OK is returned. */
int dbSwapDatabases(int id1, int id2) {
    if (id1 < 0 || id1 >= cserver.dbnum ||
        id2 < 0 || id2 >= cserver.dbnum) return C_ERR;
    if (id1 == id2) return C_OK;
    std::swap(g_pserver->db[id1], g_pserver->db[id2]);

    /* Note that we don't swap blocking_keys,
     * ready_keys and watched_keys, since we want clients to
     * remain in the same DB they were. so put them back */
    std::swap(g_pserver->db[id1]->blocking_keys, g_pserver->db[id2]->blocking_keys);
    std::swap(g_pserver->db[id2]->ready_keys, g_pserver->db[id2]->ready_keys);
    std::swap(g_pserver->db[id2]->watched_keys, g_pserver->db[id2]->watched_keys);

    /* Now we need to handle clients blocked on lists: as an effect
     * of swapping the two DBs, a client that was waiting for list
     * X in a given DB, may now actually be unblocked if X happens
     * to exist in the new version of the DB, after the swap.
     *
     * However normally we only do this check for efficiency reasons
     * in dbAdd() when a list is created. So here we need to rescan
     * the list of clients blocked on lists and signal lists as ready
     * if needed.
     *
     * Also the swapdb should make transaction fail if there is any
     * client watching keys */
    scanDatabaseForReadyLists(g_pserver->db[id1]);
    touchAllWatchedKeysInDb(g_pserver->db[id1], g_pserver->db[id2]);
    scanDatabaseForReadyLists(g_pserver->db[id2]);
    touchAllWatchedKeysInDb(g_pserver->db[id2], g_pserver->db[id1]);
    return C_OK;
}

/* SWAPDB db1 db2 */
void swapdbCommand(client *c) {
    int id1, id2;

    /* Not allowed in cluster mode: we have just DB 0 there. */
    if (g_pserver->cluster_enabled) {
        addReplyError(c,"SWAPDB is not allowed in cluster mode");
        return;
    }

    /* Get the two DBs indexes. */
    if (getIntFromObjectOrReply(c, c->argv[1], &id1,
        "invalid first DB index") != C_OK)
        return;

    if (getIntFromObjectOrReply(c, c->argv[2], &id2,
        "invalid second DB index") != C_OK)
        return;

    /* Swap... */
    if (dbSwapDatabases(id1,id2) == C_ERR) {
        addReplyError(c,"DB index is out of range");
        return;
    } else {
        RedisModuleSwapDbInfo si = {REDISMODULE_SWAPDBINFO_VERSION,(int32_t)id1,(int32_t)id2};
        moduleFireServerEvent(REDISMODULE_EVENT_SWAPDB,0,&si);
        g_pserver->dirty++;
        addReply(c,shared.ok);
    }
}

/*-----------------------------------------------------------------------------
 * Expires API
 *----------------------------------------------------------------------------*/
int removeExpire(redisDb *db, robj *key) {
    auto itr = db->find(key);
    return db->removeExpire(key, itr);
}
int redisDbPersistentData::removeExpire(robj *key, dict_iter itr) {
    /* An expire may only be removed if there is a corresponding entry in the
     * main dict. Otherwise, the key will never be freed. */
    serverAssertWithInfo(NULL,key,itr != nullptr);
    std::unique_lock<fastlock> ul(g_expireLock);

    robj *val = itr.val();
    if (!val->FExpires())
        return 0;

    trackkey(key, true /* fUpdate */);
    auto itrExpire = m_setexpire->find(itr.key());
    serverAssert(itrExpire != m_setexpire->end());
    m_setexpire->erase(itrExpire);
    val->SetFExpires(false);
    return 1;
}

int redisDbPersistentData::removeSubkeyExpire(robj *key, robj *subkey) {
    auto de = find(szFromObj(key));
    serverAssertWithInfo(NULL,key,de != nullptr);
    std::unique_lock<fastlock> ul(g_expireLock);

    robj *val = de.val();
    if (!val->FExpires())
        return 0;
    
    auto itr = m_setexpire->find(de.key());
    serverAssert(itr != m_setexpire->end());
    serverAssert(itr->key() == de.key());
    if (!itr->FFat())
        return 0;

    int found = 0;
    for (auto subitr : *itr)
    {
        if (subitr.subkey() == nullptr)
            continue;
        if (sdscmp((sds)subitr.subkey(), szFromObj(subkey)) == 0)
        {
            itr->erase(subitr);
            found = 1;
            break;
        }
    }

    if (itr->pfatentry()->size() == 0)
        this->removeExpire(key, de);

    return found;
}

void redisDbPersistentData::resortExpire(expireEntry &e)
{
    std::unique_lock<fastlock> ul(g_expireLock);
    auto itr = m_setexpire->find(e.key());
    expireEntry eT = std::move(e);
    m_setexpire->erase(itr);
    m_setexpire->insert(eT);
}

/* Set an expire to the specified key. If the expire is set in the context
 * of an user calling a command 'c' is the client, otherwise 'c' is set
 * to NULL. The 'when' parameter is the absolute unix time in milliseconds
 * after which the key will no longer be considered valid. */
void setExpire(client *c, redisDb *db, robj *key, robj *subkey, long long when) {
    serverAssert(GlobalLocksAcquired());

    /* Update TTL stats (exponential moving average) */
    /*  Note: We never have to update this on expiry since we reduce it by the current elapsed time here */
    mstime_t now;
    __atomic_load(&g_pserver->mstime, &now, __ATOMIC_ACQUIRE);
    db->avg_ttl -= (now - db->last_expire_set); // reduce the TTL by the time that has elapsed
    if (db->expireSize() == 0)
        db->avg_ttl = 0;
    else
        db->avg_ttl -= db->avg_ttl / db->expireSize(); // slide one entry out the window
    if (db->avg_ttl < 0)
        db->avg_ttl = 0;    // TTLs are never negative
    db->avg_ttl += (double)(when-now) / (db->expireSize()+1);    // add the new entry
    db->last_expire_set = now;

    /* Update the expire set */
    db->setExpire(key, subkey, when);

    int writable_slave = listLength(g_pserver->masters) && g_pserver->repl_slave_ro == 0 && !g_pserver->fActiveReplica;
    if (c && writable_slave && !(c->flags & CLIENT_MASTER))
        rememberSlaveKeyWithExpire(db,key);
}

redisDb::~redisDb()
{
    dictRelease(watched_keys);
    dictRelease(ready_keys);
    dictRelease(blocking_keys);
    listRelease(defrag_later);
}

void setExpire(client *c, redisDb *db, robj *key, expireEntry &&e)
{
    serverAssert(GlobalLocksAcquired());

    /* Reuse the sds from the main dict in the expire dict */
    auto kde = db->find(key);
    serverAssertWithInfo(NULL,key,kde != NULL);

    if (kde.val()->getrefcount(std::memory_order_relaxed) == OBJ_SHARED_REFCOUNT)
    {
        // shared objects cannot have the expire bit set, create a real object
        db->updateValue(kde, dupStringObject(kde.val()));
    }

    if (kde.val()->FExpires())
        removeExpire(db, key);

    e.setKeyUnsafe(kde.key());
    db->setExpire(std::move(e));
    kde.val()->SetFExpires(true);


    int writable_slave = listLength(g_pserver->masters) && g_pserver->repl_slave_ro == 0 && !g_pserver->fActiveReplica;
    if (c && writable_slave && !(c->flags & CLIENT_MASTER))
        rememberSlaveKeyWithExpire(db,key);
}

/* Return the expire time of the specified key, or null if no expire
 * is associated with this key (i.e. the key is non volatile) */
expireEntry *redisDbPersistentDataSnapshot::getExpire(const char *key) {
    /* No expire? return ASAP */
    if (expireSize() == 0)
        return nullptr;

    auto itrExpire = m_setexpire->find(key);
    if (itrExpire == m_setexpire->end())
        return nullptr;
    return itrExpire.operator->();
}

const expireEntry *redisDbPersistentDataSnapshot::getExpire(const char *key) const
{
    return const_cast<redisDbPersistentDataSnapshot*>(this)->getExpire(key);
}

/* Delete the specified expired key and propagate expire. */
void deleteExpiredKeyAndPropagate(redisDb *db, robj *keyobj) {
    mstime_t expire_latency;
    latencyStartMonitor(expire_latency);
    if (g_pserver->lazyfree_lazy_expire) {
        dbAsyncDelete(db,keyobj);
    } else {
        dbSyncDelete(db,keyobj);
    }
    latencyEndMonitor(expire_latency);
    latencyAddSampleIfNeeded("expire-del",expire_latency);
    notifyKeyspaceEvent(NOTIFY_EXPIRED,"expired",keyobj,db->id);
    signalModifiedKey(NULL,db,keyobj);
    propagateExpire(db,keyobj,g_pserver->lazyfree_lazy_expire);
    g_pserver->stat_expiredkeys++;
}

/* Propagate expires into slaves and the AOF file.
 * When a key expires in the master, a DEL operation for this key is sent
 * to all the slaves and the AOF file if enabled.
 *
 * This way the key expiry is centralized in one place, and since both
 * AOF and the master->replica link guarantee operation ordering, everything
 * will be consistent even if we allow write operations against expiring
 * keys. */
void propagateExpire(redisDb *db, robj *key, int lazy) {
    serverAssert(GlobalLocksAcquired());

    robj *argv[2];

    argv[0] = lazy ? shared.unlink : shared.del;
    argv[1] = key;
    incrRefCount(argv[0]);
    incrRefCount(argv[1]);

    /* If the master decided to expire a key we must propagate it to replicas no matter what..
     * Even if module executed a command without asking for propagation. */
    int prev_replication_allowed = g_pserver->replication_allowed;
    g_pserver->replication_allowed = 1;
    if (!g_pserver->fActiveReplica) // Active replicas do their own expiries, do not propogate
        propagate(cserver.delCommand,db->id,argv,2,PROPAGATE_AOF|PROPAGATE_REPL);
    g_pserver->replication_allowed = prev_replication_allowed;

    decrRefCount(argv[0]);
    decrRefCount(argv[1]);
}

void propagateSubkeyExpire(redisDb *db, int type, robj *key, robj *subkey)
{
    robj *argv[3];
    redisCommand *cmd = nullptr;
    switch (type)
    {
    case OBJ_SET:
        argv[0] = shared.srem;
        argv[1] = key;
        argv[2] = subkey;
        cmd = cserver.sremCommand;
        break;

    case OBJ_HASH:
        argv[0] = shared.hdel;
        argv[1] = key;
        argv[2] = subkey;
        cmd = cserver.hdelCommand;
        break;

    case OBJ_ZSET:
        argv[0] = shared.zrem;
        argv[1] = key;
        argv[2] = subkey;
        cmd = cserver.zremCommand;
        break;

    case OBJ_CRON:
        return; // CRON jobs replicate in their own handler

    default:
        serverPanic("Unknown subkey type");
    }

    if (g_pserver->aof_state != AOF_OFF)
        feedAppendOnlyFile(cmd,db->id,argv,3);
    // Active replicas do their own expiries, do not propogate
    if (!g_pserver->fActiveReplica)
        replicationFeedSlaves(g_pserver->slaves,db->id,argv,3);
}

/* Check if the key is expired. Note, this does not check subexpires */
int keyIsExpired(const redisDbPersistentDataSnapshot *db, robj *key) {
    /* Don't expire anything while loading. It will be done later. */
    if (g_pserver->loading) return 0;
    
    std::unique_lock<fastlock> ul(g_expireLock);
    const expireEntry *pexpire = db->getExpire(key);
    mstime_t now;

    if (pexpire == nullptr) return 0; /* No expire for this key */

    long long when = pexpire->FGetPrimaryExpire();

    if (when == INVALID_EXPIRE)
        return 0;

    /* If we are in the context of a Lua script, we pretend that time is
     * blocked to when the Lua script started. This way a key can expire
     * only the first time it is accessed and not in the middle of the
     * script execution, making propagation to slaves / AOF consistent.
     * See issue #1525 on Github for more information. */
    if (g_pserver->lua_caller) {
        now = g_pserver->lua_time_snapshot;
    }
    /* If we are in the middle of a command execution, we still want to use
     * a reference time that does not change: in that case we just use the
     * cached time, that we update before each call in the call() function.
     * This way we avoid that commands such as RPOPLPUSH or similar, that
     * may re-open the same key multiple times, can invalidate an already
     * open object in a next call, if the next call will see the key expired,
     * while the first did not. */
    else if (serverTL->fixed_time_expire > 0) {
        __atomic_load(&g_pserver->mstime, &now, __ATOMIC_ACQUIRE);
    }
    /* For the other cases, we want to use the most fresh time we have. */
    else {
        now = mstime();
    }

    /* The key expired if the current (virtual or real) time is greater
     * than the expire time of the key. */
    return now > when;
}

/* This function is called when we are going to perform some operation
 * in a given key, but such key may be already logically expired even if
 * it still exists in the database. The main way this function is called
 * is via lookupKey*() family of functions.
 *
 * The behavior of the function depends on the replication role of the
 * instance, because replica instances do not expire keys, they wait
 * for DELs from the master for consistency matters. However even
 * slaves will try to have a coherent return value for the function,
 * so that read commands executed in the replica side will be able to
 * behave like if the key is expired even if still present (because the
 * master has yet to propagate the DEL).
 *
 * In masters as a side effect of finding a key which is expired, such
 * key will be evicted from the database. Also this may trigger the
 * propagation of a DEL/UNLINK command in AOF / replication stream.
 *
 * The return value of the function is 0 if the key is still valid,
 * otherwise the function returns 1 if the key is expired. */
int expireIfNeeded(redisDb *db, robj *key) {
    if (!keyIsExpired(db,key)) return 0;

    /* If we are running in the context of a replica, instead of
     * evicting the expired key from the database, we return ASAP:
     * the replica key expiration is controlled by the master that will
     * send us synthesized DEL operations for expired keys.
     *
     * Still we try to return the right information to the caller,
     * that is, 0 if we think the key should be still valid, 1 if
     * we think the key is expired at this time. */
    if (listLength(g_pserver->masters) && !g_pserver->fActiveReplica) return 1;

    /* If clients are paused, we keep the current dataset constant,
     * but return to the client what we believe is the right state. Typically,
     * at the end of the pause we will properly expire the key OR we will
     * have failed over and the new primary will send us the expire. */
    if (checkClientPauseTimeoutAndReturnIfPaused()) return 1;

    /* Delete the key */
    deleteExpiredKeyAndPropagate(db,key);
    return 1;
}

/* -----------------------------------------------------------------------------
 * API to get key arguments from commands
 * ---------------------------------------------------------------------------*/

/* Prepare the getKeysResult struct to hold numkeys, either by using the
 * pre-allocated keysbuf or by allocating a new array on the heap.
 *
 * This function must be called at least once before starting to populate
 * the result, and can be called repeatedly to enlarge the result array.
 */
int *getKeysPrepareResult(getKeysResult *result, int numkeys) {
    /* GETKEYS_RESULT_INIT initializes keys to NULL, point it to the pre-allocated stack
     * buffer here. */
    if (!result->keys) {
        serverAssert(!result->numkeys);
        result->keys = result->keysbuf;
    }

    /* Resize if necessary */
    if (numkeys > result->size) {
        if (result->keys != result->keysbuf) {
            /* We're not using a static buffer, just (re)alloc */
            result->keys = (int*)zrealloc(result->keys, numkeys * sizeof(int));
        } else {
            /* We are using a static buffer, copy its contents */
            result->keys = (int*)zmalloc(numkeys * sizeof(int));
            if (result->numkeys)
                memcpy(result->keys, result->keysbuf, result->numkeys * sizeof(int));
        }
        result->size = numkeys;
    }

    return result->keys;
}

/* The base case is to use the keys position as given in the command table
 * (firstkey, lastkey, step). */
int getKeysUsingCommandTable(struct redisCommand *cmd,robj **argv, int argc, getKeysResult *result) {
    int j, i = 0, last, *keys;
    UNUSED(argv);

    if (cmd->firstkey == 0) {
        result->numkeys = 0;
        return 0;
    }

    last = cmd->lastkey;
    if (last < 0) last = argc+last;

    int count = ((last - cmd->firstkey)+1);
    keys = getKeysPrepareResult(result, count);

    for (j = cmd->firstkey; j <= last; j += cmd->keystep) {
        if (j >= argc) {
            /* Modules commands, and standard commands with a not fixed number
             * of arguments (negative arity parameter) do not have dispatch
             * time arity checks, so we need to handle the case where the user
             * passed an invalid number of arguments here. In this case we
             * return no keys and expect the command implementation to report
             * an arity or syntax error. */
            if (cmd->flags & CMD_MODULE || cmd->arity < 0) {
                result->numkeys = 0;
                return 0;
            } else {
                serverPanic("KeyDB built-in command declared keys positions not matching the arity requirements.");
            }
        }
        keys[i++] = j;
    }
    result->numkeys = i;
    return i;
}

/* Return all the arguments that are keys in the command passed via argc / argv.
 *
 * The command returns the positions of all the key arguments inside the array,
 * so the actual return value is a heap allocated array of integers. The
 * length of the array is returned by reference into *numkeys.
 *
 * 'cmd' must be point to the corresponding entry into the redisCommand
 * table, according to the command name in argv[0].
 *
 * This function uses the command table if a command-specific helper function
 * is not required, otherwise it calls the command-specific function. */
int getKeysFromCommand(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result) {
    if (cmd->flags & CMD_MODULE_GETKEYS) {
        return moduleGetCommandKeysViaAPI(cmd,argv,argc,result);
    } else if (!(cmd->flags & CMD_MODULE) && cmd->getkeys_proc) {
        return cmd->getkeys_proc(cmd,argv,argc,result);
    } else {
        return getKeysUsingCommandTable(cmd,argv,argc,result);
    }
}

/* Free the result of getKeysFromCommand. */
void getKeysFreeResult(getKeysResult *result) {
    if (result && result->keys != result->keysbuf)
        zfree(result->keys);
}

/* Helper function to extract keys from following commands:
 * COMMAND [destkey] <num-keys> <key> [...] <key> [...] ... <options>
 *
 * eg:
 * ZUNION <num-keys> <key> <key> ... <key> <options>
 * ZUNIONSTORE <destkey> <num-keys> <key> <key> ... <key> <options>
 *
 * 'storeKeyOfs': destkey index, 0 means destkey not exists.
 * 'keyCountOfs': num-keys index.
 * 'firstKeyOfs': firstkey index.
 * 'keyStep': the interval of each key, usually this value is 1.
 * */
int genericGetKeys(int storeKeyOfs, int keyCountOfs, int firstKeyOfs, int keyStep,
                    robj **argv, int argc, getKeysResult *result) {
    int i, num, *keys;

    num = atoi(szFromObj(argv[keyCountOfs]));
    /* Sanity check. Don't return any key if the command is going to
     * reply with syntax error. (no input keys). */
    if (num < 1 || num > (argc - firstKeyOfs)/keyStep) {
        result->numkeys = 0;
        return 0;
    }

    int numkeys = storeKeyOfs ? num + 1 : num;
    keys = getKeysPrepareResult(result, numkeys);
    result->numkeys = numkeys;

    /* Add all key positions for argv[firstKeyOfs...n] to keys[] */
    for (i = 0; i < num; i++) keys[i] = firstKeyOfs+(i*keyStep);

    if (storeKeyOfs) keys[num] = storeKeyOfs;
    return result->numkeys;
}

int zunionInterDiffStoreGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result) {
    UNUSED(cmd);
    return genericGetKeys(1, 2, 3, 1, argv, argc, result);
}

int zunionInterDiffGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result) {
    UNUSED(cmd);
    return genericGetKeys(0, 1, 2, 1, argv, argc, result);
}

int evalGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result) {
    UNUSED(cmd);
    return genericGetKeys(0, 2, 3, 1, argv, argc, result);
}

/* Helper function to extract keys from the SORT command.
 *
 * SORT <sort-key> ... STORE <store-key> ...
 *
 * The first argument of SORT is always a key, however a list of options
 * follow in SQL-alike style. Here we parse just the minimum in order to
 * correctly identify keys in the "STORE" option. */
int sortGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result) {
    int i, j, num, *keys, found_store = 0;
    UNUSED(cmd);

    num = 0;
    keys = getKeysPrepareResult(result, 2); /* Alloc 2 places for the worst case. */
    keys[num++] = 1; /* <sort-key> is always present. */

    /* Search for STORE option. By default we consider options to don't
     * have arguments, so if we find an unknown option name we scan the
     * next. However there are options with 1 or 2 arguments, so we
     * provide a list here in order to skip the right number of args. */
    struct {
        const char *name;
        int skip;
    } skiplist[] = {
        {"limit", 2},
        {"get", 1},
        {"by", 1},
        {NULL, 0} /* End of elements. */
    };

    for (i = 2; i < argc; i++) {
        for (j = 0; skiplist[j].name != NULL; j++) {
            if (!strcasecmp(szFromObj(argv[i]),skiplist[j].name)) {
                i += skiplist[j].skip;
                break;
            } else if (!strcasecmp(szFromObj(argv[i]),"store") && i+1 < argc) {
                /* Note: we don't increment "num" here and continue the loop
                 * to be sure to process the *last* "STORE" option if multiple
                 * ones are provided. This is same behavior as SORT. */
                found_store = 1;
                keys[num] = i+1; /* <store-key> */
                break;
            }
        }
    }
    result->numkeys = num + found_store;
    return result->numkeys;
}

int migrateGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result) {
    int i, num, first, *keys;
    UNUSED(cmd);

    /* Assume the obvious form. */
    first = 3;
    num = 1;

    /* But check for the extended one with the KEYS option. */
    if (argc > 6) {
        for (i = 6; i < argc; i++) {
            if (!strcasecmp(szFromObj(argv[i]),"keys") &&
                sdslen(szFromObj(argv[3])) == 0)
            {
                first = i+1;
                num = argc-first;
                break;
            }
        }
    }

    keys = getKeysPrepareResult(result, num);
    for (i = 0; i < num; i++) keys[i] = first+i;
    result->numkeys = num;
    return num;
}

/* Helper function to extract keys from following commands:
 * GEORADIUS key x y radius unit [WITHDIST] [WITHHASH] [WITHCOORD] [ASC|DESC]
 *                             [COUNT count] [STORE key] [STOREDIST key]
 * GEORADIUSBYMEMBER key member radius unit ... options ... */
int georadiusGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result) {
    int i, num, *keys;
    UNUSED(cmd);

    /* Check for the presence of the stored key in the command */
    int stored_key = -1;
    for (i = 5; i < argc; i++) {
        char *arg = szFromObj(argv[i]);
        /* For the case when user specifies both "store" and "storedist" options, the
         * second key specified would override the first key. This behavior is kept
         * the same as in georadiusCommand method.
         */
        if ((!strcasecmp(arg, "store") || !strcasecmp(arg, "storedist")) && ((i+1) < argc)) {
            stored_key = i+1;
            i++;
        }
    }
    num = 1 + (stored_key == -1 ? 0 : 1);

    /* Keys in the command come from two places:
     * argv[1] = key,
     * argv[5...n] = stored key if present
     */
    keys = getKeysPrepareResult(result, num);

    /* Add all key positions to keys[] */
    keys[0] = 1;
    if(num > 1) {
         keys[1] = stored_key;
    }
    result->numkeys = num;
    return num;
}

/* LCS ... [KEYS <key1> <key2>] ... */
int lcsGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result) {
    int i;
    int *keys = getKeysPrepareResult(result, 2);
    UNUSED(cmd);

    /* We need to parse the options of the command in order to check for the
     * "KEYS" argument before the "STRINGS" argument. */
    for (i = 1; i < argc; i++) {
        char *arg = szFromObj(argv[i]);
        int moreargs = (argc-1) - i;

        if (!strcasecmp(arg, "strings")) {
            break;
        } else if (!strcasecmp(arg, "keys") && moreargs >= 2) {
            keys[0] = i+1;
            keys[1] = i+2;
            result->numkeys = 2;
            return result->numkeys;
        }
    }
    result->numkeys = 0;
    return result->numkeys;
}

/* Helper function to extract keys from memory command.
 * MEMORY USAGE <key> */
int memoryGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result) {
    UNUSED(cmd);

    getKeysPrepareResult(result, 1);
    if (argc >= 3 && !strcasecmp(szFromObj(argv[1]),"usage")) {
        result->keys[0] = 2;
        result->numkeys = 1;
        return result->numkeys;
    }
    result->numkeys = 0;
    return 0;
}

/* XREAD [BLOCK <milliseconds>] [COUNT <count>] [GROUP <groupname> <ttl>]
 *       STREAMS key_1 key_2 ... key_N ID_1 ID_2 ... ID_N */
int xreadGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result) {
    int i, num = 0, *keys;
    UNUSED(cmd);

    /* We need to parse the options of the command in order to seek the first
     * "STREAMS" string which is actually the option. This is needed because
     * "STREAMS" could also be the name of the consumer group and even the
     * name of the stream key. */
    int streams_pos = -1;
    for (i = 1; i < argc; i++) {
        char *arg = szFromObj(argv[i]);
        if (!strcasecmp(arg, "block")) {
            i++; /* Skip option argument. */
        } else if (!strcasecmp(arg, "count")) {
            i++; /* Skip option argument. */
        } else if (!strcasecmp(arg, "group")) {
            i += 2; /* Skip option argument. */
        } else if (!strcasecmp(arg, "noack")) {
            /* Nothing to do. */
        } else if (!strcasecmp(arg, "streams")) {
            streams_pos = i;
            break;
        } else {
            break; /* Syntax error. */
        }
    }
    if (streams_pos != -1) num = argc - streams_pos - 1;

    /* Syntax error. */
    if (streams_pos == -1 || num == 0 || num % 2 != 0) {
        result->numkeys = 0;
        return 0;
    }
    num /= 2; /* We have half the keys as there are arguments because
                 there are also the IDs, one per key. */

    keys = getKeysPrepareResult(result, num);
    for (i = streams_pos+1; i < argc-num; i++) keys[i-streams_pos-1] = i;
    result->numkeys = num;
    return num;
}

/* Slot to Key API. This is used by Redis Cluster in order to obtain in
 * a fast way a key that belongs to a specified hash slot. This is useful
 * while rehashing the cluster and in other conditions when we need to
 * understand if we have keys for a given hash slot. */
void slotToKeyUpdateKey(sds key, int add) {
    slotToKeyUpdateKeyCore(key, sdslen(key), add);
}

void slotToKeyUpdateKeyCore(const char *key, size_t keylen, int add) {
    serverAssert(GlobalLocksAcquired());

    unsigned int hashslot = keyHashSlot(key,keylen);
    unsigned char buf[64];
    unsigned char *indexed = buf;

    g_pserver->cluster->slots_keys_count[hashslot] += add ? 1 : -1;
    if (keylen+2 > 64) indexed = (unsigned char*)zmalloc(keylen+2, MALLOC_SHARED);
    indexed[0] = (hashslot >> 8) & 0xff;
    indexed[1] = hashslot & 0xff;
    memcpy(indexed+2,key,keylen);
    int fModified = false;
    if (add) {
        fModified = raxInsert(g_pserver->cluster->slots_to_keys,indexed,keylen+2,NULL,NULL);
    } else {
        fModified = raxRemove(g_pserver->cluster->slots_to_keys,indexed,keylen+2,NULL);
    }
    // This assert is disabled when a snapshot depth is >0 because prepOverwriteForSnapshot will add in a tombstone,
    //  this prevents ensure from adding the key to the dictionary which means the caller isn't aware we're already tracking
    //  the key.
    serverAssert(fModified || g_pserver->db[0]->snapshot_depth() > 0);
    if (indexed != buf) zfree(indexed);
}

void slotToKeyAdd(sds key) {
    slotToKeyUpdateKey(key,1);
}

void slotToKeyDel(sds key) {
    slotToKeyUpdateKey(key,0);
}

/* Release the radix tree mapping Redis Cluster keys to slots. If 'async'
 * is true, we release it asynchronously. */
void freeSlotsToKeysMap(rax *rt, int async) {
    if (async) {
        freeSlotsToKeysMapAsync(rt);
    } else {
        raxFree(rt);
    }
}

/* Empty the slots-keys map of Redis CLuster by creating a new empty one and
 * freeing the old one. */
void slotToKeyFlush(int async) {
    rax *old = g_pserver->cluster->slots_to_keys;

    g_pserver->cluster->slots_to_keys = raxNew();
    memset(g_pserver->cluster->slots_keys_count,0,
           sizeof(g_pserver->cluster->slots_keys_count));
    freeSlotsToKeysMap(old, async);
}

/* Populate the specified array of objects with keys in the specified slot.
 * New objects are returned to represent keys, it's up to the caller to
 * decrement the reference count to release the keys names. */
unsigned int getKeysInSlot(unsigned int hashslot, robj **keys, unsigned int count) {
    raxIterator iter;
    int j = 0;
    unsigned char indexed[2];

    indexed[0] = (hashslot >> 8) & 0xff;
    indexed[1] = hashslot & 0xff;
    raxStart(&iter,g_pserver->cluster->slots_to_keys);
    raxSeek(&iter,">=",indexed,2);
    while(count-- && raxNext(&iter)) {
        if (iter.key[0] != indexed[0] || iter.key[1] != indexed[1]) break;
        keys[j++] = createStringObject((char*)iter.key+2,iter.key_len-2);
    }
    raxStop(&iter);
    return j;
}

/* Remove all the keys in the specified hash slot.
 * The number of removed items is returned. */
unsigned int delKeysInSlot(unsigned int hashslot) {
    serverAssert(GlobalLocksAcquired());
    
    raxIterator iter;
    int j = 0;
    unsigned char indexed[2];

    indexed[0] = (hashslot >> 8) & 0xff;
    indexed[1] = hashslot & 0xff;
    raxStart(&iter,g_pserver->cluster->slots_to_keys);
    while(g_pserver->cluster->slots_keys_count[hashslot]) {
        raxSeek(&iter,">=",indexed,2);
        raxNext(&iter);

        auto count = g_pserver->cluster->slots_keys_count[hashslot];
        robj *key = createStringObject((char*)iter.key+2,iter.key_len-2);
        dbDelete(g_pserver->db[0],key);
        serverAssert(count > g_pserver->cluster->slots_keys_count[hashslot]);   // we should have deleted something or we will be in an infinite loop
        decrRefCount(key);
        j++;
    }
    raxStop(&iter);
    return j;
}

unsigned int countKeysInSlot(unsigned int hashslot) {
    return g_pserver->cluster->slots_keys_count[hashslot];
}

void redisDbPersistentData::initialize()
{
    m_pdbSnapshot = nullptr;
    m_pdict = dictCreate(&dbDictType,this);
    m_pdictTombstone = dictCreate(&dbTombstoneDictType,this);
    m_setexpire = new(MALLOC_LOCAL) expireset();
    m_fAllChanged = 0;
    m_fTrackingChanges = 0;
}

void redisDbPersistentData::setStorageProvider(StorageCache *pstorage)
{
    serverAssert(m_spstorage == nullptr);
    m_spstorage = std::unique_ptr<StorageCache>(pstorage);
}

void redisDbPersistentData::endStorageProvider()
{
    serverAssert(m_spstorage != nullptr);
    m_spstorage.reset();
}

void clusterStorageLoadCallback(const char *rgchkey, size_t cch, void *)
{
    slotToKeyUpdateKeyCore(rgchkey, cch, true /*add*/);
}

void redisDb::initialize(int id)
{
    redisDbPersistentData::initialize();
    this->expireitr = setexpire()->end();
    this->blocking_keys = dictCreate(&keylistDictType,NULL);
    this->ready_keys = dictCreate(&objectKeyPointerValueDictType,NULL);
    this->watched_keys = dictCreate(&keylistDictType,NULL);
    this->id = id;
    this->avg_ttl = 0;
    this->last_expire_set = 0;
    this->defrag_later = listCreate();
    listSetFreeMethod(this->defrag_later,(void (*)(const void*))sdsfree);
}

void redisDb::storageProviderInitialize()
{
    if (g_pserver->m_pstorageFactory != nullptr)
    {
        IStorageFactory::key_load_iterator itr = (g_pserver->cluster_enabled) ? clusterStorageLoadCallback : nullptr;
        this->setStorageProvider(StorageCache::create(g_pserver->m_pstorageFactory, id, itr, nullptr));
    }
}

void redisDb::storageProviderDelete()
{
    if (g_pserver->m_pstorageFactory != nullptr)
    {
        this->endStorageProvider();
    }
}

bool redisDbPersistentData::insert(char *key, robj *o, bool fAssumeNew, dict_iter *piterExisting)
{
    if (!fAssumeNew && (g_pserver->m_pstorageFactory != nullptr || m_pdbSnapshot != nullptr))
        ensure(key);
    dictEntry *de;
    int res = dictAdd(m_pdict, key, o, &de);
    serverAssert(FImplies(fAssumeNew, res == DICT_OK));
    if (res == DICT_OK)
    {
#ifdef CHECKED_BUILD
        if (m_pdbSnapshot != nullptr && m_pdbSnapshot->find_cached_threadsafe(key) != nullptr)
        {
            serverAssert(dictFind(m_pdictTombstone, key) != nullptr);
        }
#endif
        trackkey(key, false /* fUpdate */);
    }
    else
    {
        if (piterExisting)
            *piterExisting = dict_iter(m_pdict, de);
    }
    return (res == DICT_OK);
}

// This is a performance tool to prevent us copying over an object we're going to overwrite anyways
void redisDbPersistentData::prepOverwriteForSnapshot(char *key)
{
    if (g_pserver->maxmemory_policy & MAXMEMORY_FLAG_LFU)
        return;

    if (m_pdbSnapshot != nullptr)
    {
        auto itr = m_pdbSnapshot->find_cached_threadsafe(key);
        if (itr.key() != nullptr)
        {
            sds keyNew = sdsdupshared(itr.key());
            if (dictAdd(m_pdictTombstone, keyNew, (void*)dictHashKey(m_pdict, key)) != DICT_OK)
                sdsfree(keyNew);
        }
    }
}

void redisDbPersistentData::tryResize()
{
    if (htNeedsResize(m_pdict))
        dictResize(m_pdict);
}

size_t redisDb::clear(bool fAsync, void(callback)(void*))
{
    size_t removed = size();
    if (fAsync) {
        redisDbPersistentData::emptyDbAsync();
    } else {
        redisDbPersistentData::clear(callback);
    }
    expireitr = setexpire()->end();
    return removed;
}

void redisDbPersistentData::clear(void(callback)(void*))
{
    dictEmpty(m_pdict,callback);
    if (m_fTrackingChanges)
    {
        dictEmpty(m_dictChanged, nullptr);
        m_cnewKeysPending = 0;
        m_fAllChanged++;
    }
    delete m_setexpire;
    m_setexpire = new (MALLOC_LOCAL) expireset();
    if (m_spstorage != nullptr)
        m_spstorage->clear(callback);
    dictEmpty(m_pdictTombstone,callback);
    m_pdbSnapshot = nullptr;
}

void redisDbPersistentData::setExpire(robj *key, robj *subkey, long long when)
{
    /* Reuse the sds from the main dict in the expire dict */
    std::unique_lock<fastlock> ul(g_expireLock);
    dictEntry *kde = dictFind(m_pdict,ptrFromObj(key));
    serverAssertWithInfo(NULL,key,kde != NULL);
    trackkey(key, true /* fUpdate */);

    if (((robj*)dictGetVal(kde))->getrefcount(std::memory_order_relaxed) == OBJ_SHARED_REFCOUNT)
    {
        // shared objects cannot have the expire bit set, create a real object
        dictSetVal(m_pdict, kde, dupStringObject((robj*)dictGetVal(kde)));
    }

    const char *szSubKey = (subkey != nullptr) ? szFromObj(subkey) : nullptr;
    if (((robj*)dictGetVal(kde))->FExpires()) {
        auto itr = m_setexpire->find((sds)dictGetKey(kde));
        serverAssert(itr != m_setexpire->end());
        expireEntry eNew(std::move(*itr));
        eNew.update(szSubKey, when);
        m_setexpire->erase(itr);
        m_setexpire->insert(eNew);
    }
    else
    {
        expireEntry e((sds)dictGetKey(kde), szSubKey, when);
        ((robj*)dictGetVal(kde))->SetFExpires(true);
        m_setexpire->insert(e);
    }
}

void redisDbPersistentData::setExpire(expireEntry &&e)
{
    std::unique_lock<fastlock> ul(g_expireLock);
    trackkey(e.key(), true /* fUpdate */);
    m_setexpire->insert(e);
}

bool redisDb::FKeyExpires(const char *key)
{
    std::unique_lock<fastlock> ul(g_expireLock);
    return setexpireUnsafe()->find(key) != setexpire()->end();
}

void redisDbPersistentData::updateValue(dict_iter itr, robj *val)
{
    trackkey(itr.key(), true /* fUpdate */);
    dictSetVal(m_pdict, itr.de, val);
}

void redisDbPersistentData::ensure(const char *key)
{
    if (m_pdbSnapshot == nullptr && m_spstorage == nullptr)
        return;
    dictEntry *de = dictFind(m_pdict, key);
    ensure(key, &de);
}

void redisDbPersistentData::ensure(const char *sdsKey, dictEntry **pde)
{
    serverAssert(sdsKey != nullptr);
    serverAssert(FImplies(*pde != nullptr, dictGetVal(*pde) != nullptr));    // early versions set a NULL object, this is no longer valid
    serverAssert(m_refCount == 0);
    if (m_pdbSnapshot == nullptr && g_pserver->m_pstorageFactory == nullptr)
        return;
    std::unique_lock<fastlock> ul(g_expireLock);

    // First see if the key can be obtained from a snapshot
    if (*pde == nullptr && m_pdbSnapshot != nullptr)
    {
        dictEntry *deTombstone = dictFind(m_pdictTombstone, sdsKey);
        if (deTombstone == nullptr)
        {
            auto itr = m_pdbSnapshot->find_cached_threadsafe(sdsKey);
            if (itr == m_pdbSnapshot->end())
                goto LNotFound;

            sds keyNew = sdsdupshared(itr.key());   // note: we use the iterator's key because the sdsKey may not be a shared string
            if (itr.val() != nullptr)
            {
                if (itr.val()->getrefcount(std::memory_order_relaxed) == OBJ_SHARED_REFCOUNT)
                {
                    dictAdd(m_pdict, keyNew, itr.val());
                }
                else
                {
                    sds strT = serializeStoredObject(itr.val());
                    robj *objNew = deserializeStoredObject(this, sdsKey, strT, sdslen(strT));
                    sdsfree(strT);
                    dictAdd(m_pdict, keyNew, objNew);
                    serverAssert(objNew->getrefcount(std::memory_order_relaxed) == 1);
                    serverAssert(mvccFromObj(objNew) == mvccFromObj(itr.val()));
                }
            }
            else
            {
                dictAdd(m_pdict, keyNew, nullptr);
            }
            uint64_t hash = dictGetHash(m_pdict, sdsKey);
            dictEntry **deT;
            dictht *ht;
            *pde = dictFindWithPrev(m_pdict, sdsKey, hash, &deT, &ht);
            dictAdd(m_pdictTombstone, sdsdupshared(itr.key()), (void*)hash);
        }
    }
    
LNotFound:
    // If we haven't found it yet check our storage engine
    if (*pde == nullptr && m_spstorage != nullptr)
    {
        if (dictSize(m_pdict) != size())    // if all keys are cached then no point in looking up the database
        {
            robj *o = nullptr;
            sds sdsNewKey = sdsdupshared(sdsKey);
            std::unique_ptr<expireEntry> spexpire;
            m_spstorage->retrieve((sds)sdsKey, [&](const char *, size_t, const void *data, size_t cb){
                size_t offset = 0;
                spexpire = deserializeExpire(sdsNewKey, (const char*)data, cb, &offset);    
                o = deserializeStoredObject(this, sdsNewKey, reinterpret_cast<const char*>(data) + offset, cb - offset);
                serverAssert(o != nullptr);
            });
            
            if (o != nullptr)
            {
                dictAdd(m_pdict, sdsNewKey, o);
                o->SetFExpires(spexpire != nullptr);

                if (spexpire != nullptr)
                {
                    auto itr = m_setexpire->find(sdsKey);
                    if (itr != m_setexpire->end())
                        m_setexpire->erase(itr);
                    m_setexpire->insert(std::move(*spexpire));
                    serverAssert(m_setexpire->find(sdsKey) != m_setexpire->end());
                }
                serverAssert(o->FExpires() == (m_setexpire->find(sdsKey) != m_setexpire->end()));
                g_pserver->stat_storage_provider_read_hits++;
            } else {
                sdsfree(sdsNewKey);
                g_pserver->stat_storage_provider_read_misses++;
            }

            *pde = dictFind(m_pdict, sdsKey);
        }
    }

    if (*pde != nullptr && dictGetVal(*pde) != nullptr)
    {
        robj *o = (robj*)dictGetVal(*pde);
        serverAssert(o->FExpires() == (m_setexpire->find(sdsKey) != m_setexpire->end()));
    }
}

void redisDbPersistentData::storeKey(sds key, robj *o, bool fOverwrite)
{
    sds temp = serializeStoredObjectAndExpire(this, key, o);
    m_spstorage->insert(key, temp, sdslen(temp), fOverwrite);
    sdsfree(temp);
}

void redisDbPersistentData::storeDatabase()
{
    dictIterator *di = dictGetIterator(m_pdict);
    dictEntry *de;
    while ((de = dictNext(di)) != NULL) {
        sds key = (sds)dictGetKey(de);
        robj *o = (robj*)dictGetVal(de);
        storeKey(key, o, false);
    }
    serverAssert(dictSize(m_pdict) == m_spstorage->count());
    dictReleaseIterator(di);
}

/* static */ void redisDbPersistentData::serializeAndStoreChange(StorageCache *storage, redisDbPersistentData *db, const char *key, bool fUpdate)
{
    auto itr = db->find_cached_threadsafe(key);
    if (itr == nullptr)
        return;
    robj *o = itr.val();
    sds temp = serializeStoredObjectAndExpire(db, (const char*) itr.key(), o);
    storage->insert((sds)key, temp, sdslen(temp), fUpdate);
    sdsfree(temp);
}

bool redisDbPersistentData::processChanges(bool fSnapshot)
{
    serverAssert(GlobalLocksAcquired());

    --m_fTrackingChanges;
    serverAssert(m_fTrackingChanges >= 0);

    if (m_spstorage != nullptr)
    {
        if (!m_fAllChanged && dictSize(m_dictChanged) == 0 && m_cnewKeysPending == 0)
            return false;
        m_spstorage->beginWriteBatch();
        serverAssert(m_pdbSnapshotStorageFlush == nullptr);
        if (fSnapshot && !m_fAllChanged && dictSize(m_dictChanged) > 100)
        {
            // Do a snapshot based process if possible
            m_pdbSnapshotStorageFlush = createSnapshot(getMvccTstamp(), true /* optional */);
            if (m_pdbSnapshotStorageFlush)
            {
                if (m_dictChangedStorageFlush)
                    dictRelease(m_dictChangedStorageFlush);
                m_dictChangedStorageFlush = m_dictChanged;
                m_dictChanged = dictCreate(&dictChangeDescType, nullptr);
            }
        }
        
        if (m_pdbSnapshotStorageFlush == nullptr)
        {
            if (m_fAllChanged)
            {
                if (dictSize(m_pdict) > 0 || m_spstorage->count() > 0) { // in some cases we may have pre-sized the StorageCache's dict, and we don't want clear to ruin it
                    m_spstorage->clearAsync();
                    storeDatabase();
                }
                m_fAllChanged = 0;
            }
            else
            {
                dictIterator *di = dictGetIterator(m_dictChanged);
                dictEntry *de;
                while ((de = dictNext(di)) != nullptr)
                {
                    serializeAndStoreChange(m_spstorage.get(), this, (const char*)dictGetKey(de), (bool)dictGetVal(de));
                }
                dictReleaseIterator(di);
            }
        }
        dictEmpty(m_dictChanged, nullptr);
        m_cnewKeysPending = 0;
    }
    return (m_spstorage != nullptr);
}

void redisDbPersistentData::processChangesAsync(std::atomic<int> &pendingJobs)
{
    ++pendingJobs;
    serverAssert(!m_fAllChanged);
    dictEmpty(m_dictChanged, nullptr);
    dict *dictNew = dictCreate(&dbDictType, nullptr);
    std::swap(dictNew, m_pdict);
    m_cnewKeysPending = 0;
    g_pserver->asyncworkqueue->AddWorkFunction([dictNew, this, &pendingJobs]{
        dictIterator *di = dictGetIterator(dictNew);
        dictEntry *de;
        std::vector<sds> veckeys;
        std::vector<size_t> veccbkeys;
        std::vector<sds> vecvals;
        std::vector<size_t> veccbvals;
        while ((de = dictNext(di)) != nullptr)
        {
            robj *o = (robj*)dictGetVal(de);
            sds temp = serializeStoredObjectAndExpire(this, (const char*) dictGetKey(de), o);
            veckeys.push_back((sds)dictGetKey(de));
            veccbkeys.push_back(sdslen((sds)dictGetKey(de)));
            vecvals.push_back(temp);
            veccbvals.push_back(sdslen(temp));
        }
        m_spstorage->bulkInsert(veckeys.data(), veccbkeys.data(), vecvals.data(), veccbvals.data(), veckeys.size());
        for (auto val : vecvals)
            sdsfree(val);
        dictReleaseIterator(di);
        dictRelease(dictNew);
        --pendingJobs;
    });
}

void redisDbPersistentData::bulkStorageInsert(char **rgKeys, size_t *rgcbKeys, char **rgVals, size_t *rgcbVals, size_t celem)
{
    m_spstorage->bulkInsert(rgKeys, rgcbKeys, rgVals, rgcbVals, celem);
}

void redisDbPersistentData::commitChanges(const redisDbPersistentDataSnapshot **psnapshotFree)
{
    if (m_pdbSnapshotStorageFlush)
    {
        dictIterator *di = dictGetIterator(m_dictChangedStorageFlush);
        dictEntry *de;
        while ((de = dictNext(di)) != nullptr)
        {
            serializeAndStoreChange(m_spstorage.get(), (redisDbPersistentData*)m_pdbSnapshotStorageFlush, (const char*)dictGetKey(de), (bool)dictGetVal(de));
        }
        dictReleaseIterator(di);
        dictRelease(m_dictChangedStorageFlush);
        m_dictChangedStorageFlush = nullptr;
        *psnapshotFree = m_pdbSnapshotStorageFlush;
        m_pdbSnapshotStorageFlush = nullptr;
    }
    if (m_spstorage != nullptr)
        m_spstorage->endWriteBatch();
}

redisDbPersistentData::~redisDbPersistentData()
{
    if (m_spdbSnapshotHOLDER != nullptr)
        endSnapshot(m_spdbSnapshotHOLDER.get());
    
    //serverAssert(m_pdbSnapshot == nullptr);
    serverAssert(m_refCount == 0);
    //serverAssert(m_pdict->iterators == 0);
    serverAssert(m_pdictTombstone == nullptr || m_pdictTombstone->pauserehash == 0);
    dictRelease(m_pdict);
    if (m_pdictTombstone)
        dictRelease(m_pdictTombstone);

    if (m_dictChanged)
        dictRelease(m_dictChanged);
    if (m_dictChangedStorageFlush)
        dictRelease(m_dictChangedStorageFlush);
    
    delete m_setexpire;
}

dict_iter redisDbPersistentData::random()
{
    if (size() == 0)
        return dict_iter(nullptr);
    if (m_pdbSnapshot != nullptr && m_pdbSnapshot->size() > 0)
    {
        dict_iter iter(nullptr);
        double pctInSnapshot = (double)m_pdbSnapshot->size() / (size() + m_pdbSnapshot->size());
        double randval = (double)rand()/RAND_MAX;
        if (randval <= pctInSnapshot)
        {
            iter = m_pdbSnapshot->random_cache_threadsafe();    // BUG: RANDOM doesn't consider keys not in RAM
            ensure(iter.key());
            dictEntry *de = dictFind(m_pdict, iter.key());
            return dict_iter(m_pdict, de);
        }
    }
    dictEntry *de = dictGetRandomKey(m_pdict);
    if (de != nullptr)
        ensure((const char*)dictGetKey(de), &de);
    return dict_iter(m_pdict, de);
}

size_t redisDbPersistentData::size(bool fCachedOnly) const 
{ 
    if (m_spstorage != nullptr && !m_fAllChanged && !fCachedOnly)
        return m_spstorage->count() + m_cnewKeysPending;
    
    return dictSize(m_pdict) 
        + (m_pdbSnapshot ? (m_pdbSnapshot->size(fCachedOnly) - dictSize(m_pdictTombstone)) : 0); 
}

bool redisDbPersistentData::removeCachedValue(const char *key, dictEntry **ppde)
{
    serverAssert(m_spstorage != nullptr);
    // First ensure its not a pending key
    if (m_spstorage != nullptr)
        m_spstorage->batch_lock();
    
    dictEntry *de = dictFind(m_dictChanged, key);
    if (de != nullptr)
    {
        if (m_spstorage != nullptr)
            m_spstorage->batch_unlock();
        return false; // can't evict
    }

    // since we write ASAP the database already has a valid copy so safe to delete
    if (ppde != nullptr) {
        *ppde = dictUnlink(m_pdict, key);
    } else {
        dictDelete(m_pdict, key);
    }

    if (m_spstorage != nullptr)
        m_spstorage->batch_unlock();
    
    return true;
}

redisDbPersistentData::redisDbPersistentData() {
    m_dictChanged = dictCreate(&dictChangeDescType, nullptr);
}

void redisDbPersistentData::trackChanges(bool fBulk, size_t sizeHint)
{
    m_fTrackingChanges.fetch_add(1, std::memory_order_relaxed);
    if (fBulk)
        m_fAllChanged.fetch_add(1, std::memory_order_acq_rel);

    if (sizeHint > 0)
        dictExpand(m_dictChanged, sizeHint, false);
}

void redisDbPersistentData::removeAllCachedValues()
{
    // First we have to flush the tracked changes
    if (m_fTrackingChanges)
    {
        if (processChanges(false))
            commitChanges();
        trackChanges(false);
    }

    if (m_pdict->pauserehash == 0 && m_pdict->refcount == 1) {
        dict *dT = m_pdict;
        m_pdict = dictCreate(&dbDictType, this);
        dictExpand(m_pdict, dictSize(dT)/2, false); // Make room for about half so we don't excessively rehash
        g_pserver->asyncworkqueue->AddWorkFunction([dT]{
            dictRelease(dT);
        }, false);
    } else {
        dictEmpty(m_pdict, nullptr);
    }
}

void redisDbPersistentData::disableKeyCache()
{
    if (m_spstorage == nullptr)
        return;
    m_spstorage->emergencyFreeCache();
}

bool redisDbPersistentData::keycacheIsEnabled()
{
    if (m_spstorage == nullptr)
        return false;
    return m_spstorage->keycacheIsEnabled();
}

void redisDbPersistentData::trackkey(const char *key, bool fUpdate)
{
    if (m_fTrackingChanges && !m_fAllChanged && m_spstorage) {
        dictEntry *de = dictFind(m_dictChanged, key);
        if (de == nullptr) {
            dictAdd(m_dictChanged, (void*)sdsdupshared(key), (void*)fUpdate);
            if (!fUpdate)
                ++m_cnewKeysPending;
        }
    }
}

sds serializeExpire(const expireEntry *pexpire)
{
    sds str = sdsnewlen(nullptr, sizeof(unsigned));

    if (pexpire == nullptr)
    {
        unsigned zero = 0;
        memcpy(str, &zero, sizeof(unsigned));
        return str;
    }

    auto &e = *pexpire;
    unsigned celem = (unsigned)e.size();
    memcpy(str, &celem, sizeof(unsigned));
    
    for (auto itr = e.begin(); itr != e.end(); ++itr)
    {
        unsigned subkeylen = itr.subkey() ? (unsigned)sdslen(itr.subkey()) : 0;
        size_t strOffset = sdslen(str);
        str = sdsgrowzero(str, sdslen(str) + sizeof(unsigned) + subkeylen + sizeof(long long));
        memcpy(str + strOffset, &subkeylen, sizeof(unsigned));
        if (itr.subkey())
            memcpy(str + strOffset + sizeof(unsigned), itr.subkey(), subkeylen);
        long long when = itr.when();
        memcpy(str + strOffset + sizeof(unsigned) + subkeylen, &when, sizeof(when));
    }
    return str;
}

std::unique_ptr<expireEntry> deserializeExpire(sds key, const char *str, size_t cch, size_t *poffset)
{
    unsigned celem;
    if (cch < sizeof(unsigned))
        throw "Corrupt expire entry";
    memcpy(&celem, str, sizeof(unsigned));
    std::unique_ptr<expireEntry> spexpire;

    size_t offset = sizeof(unsigned);
    for (; celem > 0; --celem)
    {
        serverAssert(cch > (offset+sizeof(unsigned)));
        
        unsigned subkeylen;
        memcpy(&subkeylen, str + offset, sizeof(unsigned));
        offset += sizeof(unsigned);

        sds subkey = nullptr;
        if (subkeylen != 0)
        {
            serverAssert(cch > (offset + subkeylen));
            subkey = sdsnewlen(nullptr, subkeylen);
            memcpy(subkey, str + offset, subkeylen);
            offset += subkeylen;
        }
        
        long long when;
        serverAssert(cch >= (offset + sizeof(long long)));
        memcpy(&when, str + offset, sizeof(long long));
        offset += sizeof(long long);

        if (spexpire == nullptr)
            spexpire = std::make_unique<expireEntry>(key, subkey, when);
        else
            spexpire->update(subkey, when);

        if (subkey)
            sdsfree(subkey);
    }

    *poffset = offset;
    return spexpire;
}

sds serializeStoredObjectAndExpire(redisDbPersistentData *db, const char *key, robj_roptr o)
{
    auto itrExpire = db->setexpire()->find(key);
    const expireEntry *pexpire = nullptr;
    if (itrExpire != db->setexpire()->end())
        pexpire = &(*itrExpire);

    sds str = serializeExpire(pexpire);
    str = serializeStoredObject(o, str);
    return str;
}

int dbnumFromDb(redisDb *db)
{
    for (int i = 0; i < cserver.dbnum; ++i)
    {
        if (g_pserver->db[i] == db)
            return i;
    }
    serverPanic("invalid database pointer");
}

bool redisDbPersistentData::prefetchKeysAsync(client *c, parsed_command &command, bool fExecOK)
{
    if (m_spstorage == nullptr) {
#if defined(__x86_64__) || defined(__i386__)
        // We do a quick 'n dirty check for set & get.  Anything else is too slow.
        //  Should the user do something weird like remap them then the worst that will
        //  happen is we don't prefetch or we prefetch wrong data.  A mild perf hit, but
        //  not dangerous
        if (command.argc >= 2) {
            const char *cmd = szFromObj(command.argv[0]);
            if (!strcasecmp(cmd, "set") || !strcasecmp(cmd, "get")) {
                if (c->db->m_spdbSnapshotHOLDER != nullptr)
                    return false; // this is dangerous enough without a snapshot around
                auto h = dictSdsHash(szFromObj(command.argv[1]));
                for (int iht = 0; iht < 2; ++iht) {
                    auto hT = h & c->db->m_pdict->ht[iht].sizemask;
                    dictEntry **table;
                    __atomic_load(&c->db->m_pdict->ht[iht].table, &table, __ATOMIC_RELAXED);
                    if (table != nullptr) {
                        dictEntry *de;
                        __atomic_load(&table[hT], &de, __ATOMIC_ACQUIRE);
                        while (de != nullptr) {
                            _mm_prefetch(dictGetKey(de), _MM_HINT_T2);
                            __atomic_load(&de->next, &de, __ATOMIC_ACQUIRE);
                        }
                    }
                    if (!dictIsRehashing(c->db->m_pdict))
                        break;
                }
            }
        }
#endif
        return false;
    }

    AeLocker lock;

    std::vector<robj*> veckeys;
    lock.arm(c);
    getKeysResult result = GETKEYS_RESULT_INIT;
    auto cmd = lookupCommand(szFromObj(command.argv[0]));
    if (cmd == nullptr)
        return false; // Bad command? It's not for us to judge, just bail
    int numkeys = getKeysFromCommand(cmd, command.argv, command.argc, &result);
    for (int ikey = 0; ikey < numkeys; ++ikey)
    {
        robj *objKey = command.argv[result.keys[ikey]];
        if (this->find_cached_threadsafe(szFromObj(objKey)) == nullptr)
            veckeys.push_back(objKey);
    }
    lock.disarm();

    getKeysFreeResult(&result);

    std::vector<std::tuple<sds, robj*, std::unique_ptr<expireEntry>>> vecInserts;
    for (robj *objKey : veckeys)
    {
        sds sharedKey = sdsdupshared((sds)szFromObj(objKey));
        std::unique_ptr<expireEntry> spexpire;
        robj *o = nullptr;
        m_spstorage->retrieve((sds)szFromObj(objKey), [&](const char *, size_t, const void *data, size_t cb){
                size_t offset = 0;
                spexpire = deserializeExpire(sharedKey, (const char*)data, cb, &offset);    
                o = deserializeStoredObject(this, sharedKey, reinterpret_cast<const char*>(data) + offset, cb - offset);
                serverAssert(o != nullptr);
        });

        if (o != nullptr) {
            vecInserts.emplace_back(sharedKey, o, std::move(spexpire));
        } else if (sharedKey != nullptr) {
            sdsfree(sharedKey);
        }
    }

    bool fNoInsert = false;
    if (!vecInserts.empty()) {
        lock.arm(c);
        for (auto &tuple : vecInserts)
        {
            sds sharedKey = std::get<0>(tuple);
            robj *o = std::get<1>(tuple);
            std::unique_ptr<expireEntry> spexpire = std::move(std::get<2>(tuple));

            if (o != nullptr)
            {
                if (this->find_cached_threadsafe(sharedKey) != nullptr)
                {
                    // While unlocked this was already ensured
                    decrRefCount(o);
                    sdsfree(sharedKey);
                    fNoInsert = true;
                }
                else
                {
                    if (spexpire != nullptr) {
                        if (spexpire->when() < mstime()) {
                            fNoInsert = true;
                            break;
                        }
                    }
                    dictAdd(m_pdict, sharedKey, o);
                    o->SetFExpires(spexpire != nullptr);

                    if (spexpire != nullptr)
                    {
                        auto itr = m_setexpire->find(sharedKey);
                        if (itr != m_setexpire->end())
                            m_setexpire->erase(itr);
                        m_setexpire->insert(std::move(*spexpire));
                        serverAssert(m_setexpire->find(sharedKey) != m_setexpire->end());
                    }
                    serverAssert(o->FExpires() == (m_setexpire->find(sharedKey) != m_setexpire->end()));
                }
            }
            else
            {
                if (sharedKey != nullptr)
                    sdsfree(sharedKey); // BUG but don't bother crashing
            }
        }
        lock.disarm();
    }

    if (fExecOK && !fNoInsert && cmd->proc == getCommand && !vecInserts.empty()) {
        robj *o = std::get<1>(vecInserts[0]);
        if (o != nullptr) {
            addReplyBulk(c, o);
            return true;
        }
    }
    return false;
}