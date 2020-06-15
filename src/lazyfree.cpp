#include "server.h"
#include "bio.h"
#include "atomicvar.h"
#include "cluster.h"

static size_t lazyfree_objects = 0;
pthread_mutex_t lazyfree_objects_mutex = PTHREAD_MUTEX_INITIALIZER;

/* Return the number of currently pending objects to free. */
size_t lazyfreeGetPendingObjectsCount(void) {
    size_t aux;
    atomicGet(lazyfree_objects,aux);
    return aux;
}

/* Return the amount of work needed in order to free an object.
 * The return value is not always the actual number of allocations the
 * object is compoesd of, but a number proportional to it.
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
size_t lazyfreeGetFreeEffort(robj *obj) {
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
            dictAdd(m_pdictTombstone, sdsdup((sds)dictGetKey(de)), nullptr);

        robj *val = (robj*)dictGetVal(de);
        if (val->FExpires())
        {
            /* Deleting an entry from the expires dict will not free the sds of
             * the key, because it is shared with the main dictionary. */
            removeExpire(key,dict_iter(de));
        }

        size_t free_effort = lazyfreeGetFreeEffort(val);

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
            bioCreateBackgroundJob(BIO_LAZY_FREE,val,NULL,NULL);
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
void freeObjAsync(robj *o) {
    size_t free_effort = lazyfreeGetFreeEffort(o);
    if (free_effort > LAZYFREE_THRESHOLD && o->getrefcount(std::memory_order_relaxed) == 1) {
        atomicIncr(lazyfree_objects,1);
        bioCreateBackgroundJob(BIO_LAZY_FREE,o,NULL,NULL);
    } else {
        decrRefCount(o);
    }
}

/* Empty a Redis DB asynchronously. What the function does actually is to
 * create a new empty set of hash tables and scheduling the old ones for
 * lazy freeing. */
void redisDbPersistentData::emptyDbAsync() {
    dict *oldht1 = m_pdict;
    auto *set = m_setexpire;
    m_setexpire = new (MALLOC_LOCAL) expireset();
    m_pdict = dictCreate(&dbDictType,this);
    if (m_spstorage != nullptr)
        m_spstorage->clear();
    if (m_fTrackingChanges)
        m_fAllChanged = true;
    atomicIncr(lazyfree_objects,dictSize(oldht1));
    bioCreateBackgroundJob(BIO_LAZY_FREE,NULL,oldht1,set);
}

/* Empty the slots-keys map of Redis CLuster by creating a new empty one
 * and scheduiling the old for lazy freeing. */
void slotToKeyFlushAsync(void) {
    rax *old = g_pserver->cluster->slots_to_keys;

    g_pserver->cluster->slots_to_keys = raxNew();
    memset(g_pserver->cluster->slots_keys_count,0,
           sizeof(g_pserver->cluster->slots_keys_count));
    atomicIncr(lazyfree_objects,old->numele);
    bioCreateBackgroundJob(BIO_LAZY_FREE,NULL,NULL,old);
}

/* Release objects from the lazyfree thread. It's just decrRefCount()
 * updating the count of objects to release. */
void lazyfreeFreeObjectFromBioThread(robj *o) {
    decrRefCount(o);
    atomicDecr(lazyfree_objects,1);
}

/* Release a database from the lazyfree thread. The 'db' pointer is the
 * database which was substitutied with a fresh one in the main thread
 * when the database was logically deleted. 'sl' is a skiplist used by
 * Redis Cluster in order to take the hash slots -> keys mapping. This
 * may be NULL if Redis Cluster is disabled. */
void lazyfreeFreeDatabaseFromBioThread(dict *ht1, expireset *set) {
    size_t numkeys = dictSize(ht1);
    dictRelease(ht1);
    delete set;
    atomicDecr(lazyfree_objects,numkeys);
}

/* Release the skiplist mapping Redis Cluster keys to slots in the
 * lazyfree thread. */
void lazyfreeFreeSlotsMapFromBioThread(rax *rt) {
    size_t len = rt->numele;
    raxFree(rt);
    atomicDecr(lazyfree_objects,len);
}
