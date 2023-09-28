#include "server.h"
#include "bio.h"
#include "atomicvar.h"
#include "cluster.h"

static redisAtomic size_t lazyfree_objects = 0;
static redisAtomic size_t lazyfreed_objects = 0;

/* Release objects from the lazyfree thread. It's just decrRefCount()
 * updating the count of objects to release. */
void lazyfreeFreeObject(void *args[]) {
    robj *o = (robj *) args[0];
    decrRefCount(o);
    atomicDecr(lazyfree_objects,1);
    atomicIncr(lazyfreed_objects,1);
}

/* Release a database from the lazyfree thread. The 'db' pointer is the
 * database which was substituted with a fresh one in the main thread
 * when the database was logically deleted. */
void lazyfreeFreeDatabase(void *args[]) {
    dict *ht1 = (dict *) args[0];

    size_t numkeys = dictSize(ht1);
    dictRelease(ht1);
    atomicDecr(lazyfree_objects,numkeys);
    atomicIncr(lazyfreed_objects,numkeys);
}

/* Release the skiplist mapping Redis Cluster keys to slots in the
 * lazyfree thread. */
void lazyfreeFreeSlotsMap(void *args[]) {
    rax *rt = (rax*)args[0];
    size_t len = rt->numele;
    raxFree(rt);
    atomicDecr(lazyfree_objects,len);
    atomicIncr(lazyfreed_objects,len);
}

/* Release the key tracking table. */
void lazyFreeTrackingTable(void *args[]) {
    rax *rt = (rax*)args[0];
    size_t len = rt->numele;
    freeTrackingRadixTree(rt);
    atomicDecr(lazyfree_objects,len);
    atomicIncr(lazyfreed_objects,len);
}

void lazyFreeLuaScripts(void *args[]) {
    dict *lua_scripts = (dict*)args[0];
    long long len = dictSize(lua_scripts);
    dictRelease(lua_scripts);
    atomicDecr(lazyfree_objects,len);
    atomicIncr(lazyfreed_objects,len);
}

/* Return the number of currently pending objects to free. */
size_t lazyfreeGetPendingObjectsCount(void) {
    size_t aux;
    atomicGet(lazyfree_objects,aux);
    return aux;
}

/* Return the number of objects that have been freed. */
size_t lazyfreeGetFreedObjectsCount(void) {
    size_t aux;
    atomicGet(lazyfreed_objects,aux);
    return aux;
}

/* Return the amount of work needed in order to free an object.
 * The return value is not always the actual number of allocations the
 * object is composed of, but a number proportional to it.
 *
 * For strings the function always returns 1.
 *
 * For aggregated objects represented by hash tables or other data structures
 * the function just returns the number of elements the object is composed of.
 *
 * Objects composed of single allocations are always reported as having a
 * single item even if they are actually logical composed of multiple
 * elements.
 *
 * For lists the function returns the number of elements in the quicklist
 * representing the list. */
size_t lazyfreeGetFreeEffort(robj *key, robj *obj) {
    if (obj->type == OBJ_LIST) {
        quicklist *ql = (quicklist*)ptrFromObj(obj);
        return ql->len;
    } else if (obj->type == OBJ_SET && obj->encoding == OBJ_ENCODING_HT) {
        dict *ht = (dict*)ptrFromObj(obj);
        return dictSize(ht);
    } else if (obj->type == OBJ_ZSET && obj->encoding == OBJ_ENCODING_SKIPLIST){
        zset *zs = (zset*)ptrFromObj(obj);
        return zs->zsl->length;
    } else if (obj->type == OBJ_HASH && obj->encoding == OBJ_ENCODING_HT) {
        dict *ht = (dict*)ptrFromObj(obj);
        return dictSize(ht);
    } else if (obj->type == OBJ_STREAM) {
        size_t effort = 0;
        stream *s = (stream*)ptrFromObj(obj);

        /* Make a best effort estimate to maintain constant runtime. Every macro
         * node in the Stream is one allocation. */
        effort += s->rax->numnodes;

        /* Every consumer group is an allocation and so are the entries in its
         * PEL. We use size of the first group's PEL as an estimate for all
         * others. */
        if (s->cgroups && raxSize(s->cgroups)) {
            raxIterator ri;
            streamCG *cg;
            raxStart(&ri,s->cgroups);
            raxSeek(&ri,"^",NULL,0);
            /* There must be at least one group so the following should always
             * work. */
            serverAssert(raxNext(&ri));
            cg = (streamCG*)ri.data;
            effort += raxSize(s->cgroups)*(1+raxSize(cg->pel));
            raxStop(&ri);
        }
        return effort;
    } else if (obj->type == OBJ_MODULE) {
        moduleValue *mv = (moduleValue*)ptrFromObj(obj);
        moduleType *mt = mv->type;
        if (mt->free_effort != NULL) {
            size_t effort  = mt->free_effort(key,mv->value);
            /* If the module's free_effort returns 0, it will use asynchronous free
             memory by default */
            return effort == 0 ? ULONG_MAX : effort;
        } else {
            return 1;
        }
    } else {
        return 1; /* Everything else is a single allocation. */
    }
}

/* Delete a key, value, and associated expiration entry if any, from the DB.
 * If there are enough allocations to free the value object may be put into
 * a lazy free list instead of being freed synchronously. The lazy free list
 * will be reclaimed in a different bio.c thread. */
#define LAZYFREE_THRESHOLD 64
bool redisDbPersistentData::asyncDelete(robj *key) {
    /* If the value is composed of a few allocations, to free in a lazy way
     * is actually just slower... So under a certain limit we just free
     * the object synchronously. */

    if (m_spstorage != nullptr)
        return syncDelete(key); // async delte never makes sense with a storage provider

    dictEntry *de = dictUnlink(m_pdict,ptrFromObj(key));
    if (de) {
        if (m_pdbSnapshot != nullptr && m_pdbSnapshot->find_cached_threadsafe(szFromObj(key)) != nullptr)
        {
            uint64_t hash = dictGetHash(m_pdict, szFromObj(key));
            dictAdd(m_pdictTombstone, sdsdup((sds)dictGetKey(de)), (void*)hash);
        }

        robj *val = (robj*)dictGetVal(de);
        if (val->FExpires())
        {
            /* Deleting an entry from the expires dict will not free the sds of
             * the key, because it is shared with the main dictionary. */
            removeExpire(key,dict_iter(m_pdict, de));
        }

        /* Tells the module that the key has been unlinked from the database. */
        moduleNotifyKeyUnlink(key,val);

        size_t free_effort = lazyfreeGetFreeEffort(key,val);

        /* If releasing the object is too much work, do it in the background
         * by adding the object to the lazy free list.
         * Note that if the object is shared, to reclaim it now it is not
         * possible. This rarely happens, however sometimes the implementation
         * of parts of the Redis core may call incrRefCount() to protect
         * objects, and then call dbDelete(). In this case we'll fall
         * through and reach the dictFreeUnlinkedEntry() call, that will be
         * equivalent to just calling decrRefCount(). */
        if (free_effort > LAZYFREE_THRESHOLD && val->getrefcount(std::memory_order_relaxed) == 1) {
            atomicIncr(lazyfree_objects,1);
            bioCreateLazyFreeJob(lazyfreeFreeObject,1, val);
            dictSetVal(m_pdict,de,NULL);
        }
    }

    /* Release the key-val pair, or just the key if we set the val
     * field to NULL in order to lazy free it later. */
    if (de) {
        dictFreeUnlinkedEntry(m_pdict,de);
        if (g_pserver->cluster_enabled) slotToKeyDel(szFromObj(key));
        return true;
    } else {
        return false;
    }
}

int dbAsyncDelete(redisDb *db, robj *key) {
    return db->asyncDelete(key);
}

/* Free an object, if the object is huge enough, free it in async way. */
void freeObjAsync(robj *key, robj *obj) {
    size_t free_effort = lazyfreeGetFreeEffort(key,obj);
    if (free_effort > LAZYFREE_THRESHOLD && obj->getrefcount(std::memory_order_relaxed) == 1) {
        atomicIncr(lazyfree_objects,1);
        bioCreateLazyFreeJob(lazyfreeFreeObject,1,obj);
    } else {
        decrRefCount(obj);
    }
}

/* Empty a Redis DB asynchronously. What the function does actually is to
 * create a new empty set of hash tables and scheduling the old ones for
 * lazy freeing. */
void redisDbPersistentData::emptyDbAsync() {
    dict *oldht1 = m_pdict;
    m_pdict = dictCreate(&dbDictType,this);
    if (m_spstorage != nullptr)
        m_spstorage->clearAsync();
    if (m_fTrackingChanges)
        m_fAllChanged = true;
    atomicIncr(lazyfree_objects,dictSize(oldht1));
    m_numexpires = 0;
    bioCreateLazyFreeJob(lazyfreeFreeDatabase,2,oldht1,nullptr);
}

/* Release the radix tree mapping Redis Cluster keys to slots asynchronously. */
void freeSlotsToKeysMapAsync(rax *rt) {
    atomicIncr(lazyfree_objects,rt->numele);
    bioCreateLazyFreeJob(lazyfreeFreeSlotsMap,1,rt);
}

/* Free an object, if the object is huge enough, free it in async way. */
void freeTrackingRadixTreeAsync(rax *tracking) {
    atomicIncr(lazyfree_objects,tracking->numele);
    bioCreateLazyFreeJob(lazyFreeTrackingTable,1,tracking);
}

/* Free lua_scripts dict, if the dict is huge enough, free it in async way. */
void freeLuaScriptsAsync(dict *lua_scripts) {
    if (dictSize(lua_scripts) > LAZYFREE_THRESHOLD) {
        atomicIncr(lazyfree_objects,dictSize(lua_scripts));
        bioCreateLazyFreeJob(lazyFreeLuaScripts,1,lua_scripts);
    } else {
        dictRelease(lua_scripts);
    }
}
