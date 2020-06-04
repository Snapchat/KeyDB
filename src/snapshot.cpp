#include "server.h"
#include "aelocker.h"

static const size_t c_elementsSmallLimit = 500000;

const redisDbPersistentDataSnapshot *redisDbPersistentData::createSnapshot(uint64_t mvccCheckpoint, bool fOptional)
{
    serverAssert(GlobalLocksAcquired());
    serverAssert(m_refCount == 0);  // do not call this on a snapshot

    freeMemoryIfNeededAndSafe(true /*fPreSnapshot*/);

    int levels = 1;
    redisDbPersistentDataSnapshot *psnapshot = m_spdbSnapshotHOLDER.get();
    while (psnapshot != nullptr)
    {
        ++levels;
        psnapshot = psnapshot->m_spdbSnapshotHOLDER.get();
    }

    if (m_spdbSnapshotHOLDER != nullptr)
    {
        // If possible reuse an existing snapshot (we want to minimize nesting)
        if (mvccCheckpoint <= m_spdbSnapshotHOLDER->m_mvccCheckpoint)
        {
            if (!m_spdbSnapshotHOLDER->FStale())
            {
                m_spdbSnapshotHOLDER->m_refCount++;
                return m_spdbSnapshotHOLDER.get();
            }
            serverLog(LL_VERBOSE, "Existing snapshot too old, creating a new one");
        }
    }

    if (m_pdbSnapshot != nullptr && m_pdbSnapshot == m_pdbSnapshotASYNC && m_spdbSnapshotHOLDER->m_refCount == 1 && dictSize(m_pdictTombstone) < c_elementsSmallLimit)
    {
        serverLog(LL_WARNING, "Reusing old snapshot");
        // is there an existing snapshot only owned by us?
        
        dictIterator *di = dictGetIterator(m_pdictTombstone);
        dictEntry *de;
        while ((de = dictNext(di)) != nullptr)
        {
            if (dictDelete(m_pdbSnapshot->m_pdict, dictGetKey(de)) != DICT_OK)
                dictAdd(m_spdbSnapshotHOLDER->m_pdictTombstone, sdsdupshared((sds)dictGetKey(de)), nullptr);
        }
        dictReleaseIterator(di);

        dictForceRehash(m_spdbSnapshotHOLDER->m_pdictTombstone);
        dictMerge(m_pdbSnapshot->m_pdict, m_pdict);
        dictEmpty(m_pdictTombstone, nullptr);
        delete m_spdbSnapshotHOLDER->m_setexpire;
        m_spdbSnapshotHOLDER->m_setexpire =  new (MALLOC_LOCAL) expireset(*m_setexpire);

        m_pdbSnapshotASYNC = nullptr;
        serverAssert(m_pdbSnapshot->m_pdict->iterators == 1);
        serverAssert(m_spdbSnapshotHOLDER->m_refCount == 1);
        return m_pdbSnapshot;
    }

    // See if we have too many levels and can bail out of this to reduce load
    if (fOptional && (levels >= 4))
        return nullptr;

    auto spdb = std::unique_ptr<redisDbPersistentDataSnapshot>(new (MALLOC_LOCAL) redisDbPersistentDataSnapshot());
    
    spdb->m_fAllChanged = false;
    spdb->m_fTrackingChanges = 0;
    spdb->m_pdict = m_pdict;
    spdb->m_pdictTombstone = m_pdictTombstone;
    // Add a fake iterator so the dicts don't rehash (they need to be read only)
    spdb->m_pdict->iterators++;
    dictForceRehash(spdb->m_pdictTombstone);    // prevent rehashing by finishing the rehash now
    spdb->m_spdbSnapshotHOLDER = std::move(m_spdbSnapshotHOLDER);
    if (m_spstorage != nullptr)
        spdb->m_spstorage = std::shared_ptr<IStorage>(const_cast<IStorage*>(m_spstorage->clone()));
    spdb->m_pdbSnapshot = m_pdbSnapshot;
    spdb->m_refCount = 1;
    spdb->m_mvccCheckpoint = getMvccTstamp();
    if (m_setexpire != nullptr)
    {
        spdb->m_setexpire =  new (MALLOC_LOCAL) expireset(*m_setexpire);
        spdb->m_setexpire->pause_rehash();  // needs to be const
    }

    m_pdict = dictCreate(&dbDictType,this);
    m_pdictTombstone = dictCreate(&dbDictTypeTombstone, this);

    serverAssert(spdb->m_pdict->iterators == 1);

    m_spdbSnapshotHOLDER = std::move(spdb);
    m_pdbSnapshot = m_spdbSnapshotHOLDER.get();

    // Finally we need to take a ref on all our children snapshots.  This ensures they aren't free'd before we are
    redisDbPersistentData *pdbSnapshotNext = m_pdbSnapshot->m_spdbSnapshotHOLDER.get();
    while (pdbSnapshotNext != nullptr)
    {
        pdbSnapshotNext->m_refCount++;
        pdbSnapshotNext = pdbSnapshotNext->m_spdbSnapshotHOLDER.get();
    }

    if (m_pdbSnapshotASYNC != nullptr)
    {
        // free the async snapshot, it's done its job
        endSnapshot(m_pdbSnapshotASYNC);    // should be just a dec ref (FAST)
        m_pdbSnapshotASYNC = nullptr;
    }

    return m_pdbSnapshot;
}

void redisDbPersistentData::recursiveFreeSnapshots(redisDbPersistentDataSnapshot *psnapshot)
{
    std::vector<redisDbPersistentDataSnapshot*> stackSnapshots;
    // gather a stack of snapshots, we do this so we can free them in reverse
    
    // Note: we don't touch the incoming psnapshot since the parent is free'ing that one
    while ((psnapshot = psnapshot->m_spdbSnapshotHOLDER.get()) != nullptr)
    {
        stackSnapshots.push_back(psnapshot);
    }

    for (auto itr = stackSnapshots.rbegin(); itr != stackSnapshots.rend(); ++itr)
    {
        endSnapshot(*itr);
    }
}

/* static */ void redisDbPersistentDataSnapshot::gcDisposeSnapshot(redisDbPersistentDataSnapshot *psnapshot)
{
    psnapshot->m_refCount--;
    if (psnapshot->m_refCount <= 0)
    {
        serverAssert(psnapshot->m_refCount == 0);
        // Remove our ref from any children and dispose them too
        redisDbPersistentDataSnapshot *psnapshotChild = psnapshot;
        std::vector<redisDbPersistentDataSnapshot*> vecClean;
        while ((psnapshotChild = psnapshotChild->m_spdbSnapshotHOLDER.get()) != nullptr)
            vecClean.push_back(psnapshotChild);

        for (auto psnapshotChild : vecClean)
            gcDisposeSnapshot(psnapshotChild);

        //psnapshot->m_pdict->iterators--;
        psnapshot->m_spdbSnapshotHOLDER.release();
        //psnapshot->m_pdbSnapshot = nullptr;
        g_pserver->garbageCollector.enqueue(serverTL->gcEpoch, std::unique_ptr<redisDbPersistentDataSnapshot>(psnapshot));
        serverLog(LL_VERBOSE, "Garbage collected snapshot");
    }
}

void redisDbPersistentData::restoreSnapshot(const redisDbPersistentDataSnapshot *psnapshot)
{
    serverAssert(psnapshot->m_refCount == 1);
    serverAssert(m_spdbSnapshotHOLDER.get() == psnapshot);
    
    m_pdbSnapshot = psnapshot;   // if it was deleted restore it
    size_t expectedSize = psnapshot->size();
    dictEmpty(m_pdict, nullptr);
    dictEmpty(m_pdictTombstone, nullptr);
    delete m_setexpire;
    m_setexpire = new (MALLOC_LOCAL) expireset(*psnapshot->m_setexpire);
    endSnapshot(psnapshot);
    serverAssert(size() == expectedSize);
}

// This function is all about minimizing the amount of work done under global lock
//  when there has been lots of changes since snapshot creation a naive endSnapshot()
//  will block for a very long time and will cause latency spikes.
//
// Note that this function uses a lot more CPU time than a simple endSnapshot(), we
//  have some internal heuristics to do a synchronous endSnapshot if it makes sense
void redisDbPersistentData::endSnapshotAsync(const redisDbPersistentDataSnapshot *psnapshot)
{
    aeAcquireLock();
        if (m_pdbSnapshotASYNC && m_pdbSnapshotASYNC->m_mvccCheckpoint <= psnapshot->m_mvccCheckpoint)
        {
            // Free a stale async snapshot so consolidate_children can clean it up later
            endSnapshot(m_pdbSnapshotASYNC);    // FAST: just a ref decrement
            m_pdbSnapshotASYNC = nullptr;
        }

        size_t elements = dictSize(m_pdictTombstone);
        // if neither dict is rehashing then the merge is O(1) so don't count the size
        if (dictIsRehashing(psnapshot->m_pdict) || dictIsRehashing(m_pdict))
            elements += dictSize(m_pdict);
        if (elements < c_elementsSmallLimit || psnapshot != m_spdbSnapshotHOLDER.get())  // heuristic
        {
            // For small snapshots it makes more sense just to merge it directly
            endSnapshot(psnapshot);
            aeReleaseLock();
            return;
        }

        // OK this is a big snapshot so lets do the merge work outside the lock
        auto psnapshotT = createSnapshot(LLONG_MAX, false);
        endSnapshot(psnapshot); // this will just dec the ref count since our new snapshot has a ref 
        psnapshot = nullptr;
    aeReleaseLock();

    // do the expensive work of merging snapshots outside the ref
    const_cast<redisDbPersistentDataSnapshot*>(psnapshotT)->freeTombstoneObjects(1);    // depth is one because we just creted it
    const_cast<redisDbPersistentDataSnapshot*>(psnapshotT)->consolidate_children(this, true);
    
    // Final Cleanup
    aeAcquireLock();
        if (m_pdbSnapshotASYNC == nullptr)
            m_pdbSnapshotASYNC = psnapshotT;
        else
            endSnapshot(psnapshotT);    // finally clean up our temp snapshot
    aeReleaseLock();
}

void redisDbPersistentDataSnapshot::freeTombstoneObjects(int depth)
{
    if (m_pdbSnapshot == nullptr)
        return;

    const_cast<redisDbPersistentDataSnapshot*>(m_pdbSnapshot)->freeTombstoneObjects(depth+1);
    if (m_pdbSnapshot->m_refCount != depth && (m_pdbSnapshot->m_refCount != (m_refCount+1)))
        return;
    
    dictIterator *di = dictGetIterator(m_pdictTombstone);
    dictEntry *de;
    size_t freed = 0;
    while ((de = dictNext(di)) != nullptr)
    {
        dictEntry *deObj = dictFind(m_pdbSnapshot->m_pdict, dictGetKey(de));
        if (deObj != nullptr && dictGetVal(deObj) != nullptr)
        {
            decrRefCount((robj*)dictGetVal(deObj));
            deObj->v.val = nullptr;
            ++freed;
        }
    }
    dictReleaseIterator(di);
}

void redisDbPersistentData::endSnapshot(const redisDbPersistentDataSnapshot *psnapshot)
{
    serverAssert(GlobalLocksAcquired());
    
    if (m_spdbSnapshotHOLDER.get() != psnapshot)
    {
        if (m_spdbSnapshotHOLDER == nullptr)
        {
            // This is an orphaned snapshot
            redisDbPersistentDataSnapshot::gcDisposeSnapshot(const_cast<redisDbPersistentDataSnapshot*>(psnapshot));
            return;
        }
        m_spdbSnapshotHOLDER->endSnapshot(psnapshot);
        return;
    }

    // Alright we're ready to be free'd, but first dump all the refs on our child snapshots
    if (m_spdbSnapshotHOLDER->m_refCount == 1)
        recursiveFreeSnapshots(m_spdbSnapshotHOLDER.get());

    m_spdbSnapshotHOLDER->m_refCount--;
    if (m_spdbSnapshotHOLDER->m_refCount > 0)
        return;

    size_t sizeStart = size();
    serverAssert(m_spdbSnapshotHOLDER->m_refCount == 0);
    serverAssert((m_refCount == 0 && m_pdict->iterators == 0) || (m_refCount != 0 && m_pdict->iterators == 1));

    serverAssert(m_spdbSnapshotHOLDER->m_pdict->iterators == 1);  // All iterators should have been free'd except the fake one from createSnapshot
    if (m_refCount == 0)
    {
        m_spdbSnapshotHOLDER->m_pdict->iterators--;
    }

    if (m_pdbSnapshot == nullptr)
    {
        // the database was cleared so we don't need to recover the snapshot
        dictEmpty(m_pdictTombstone, nullptr);
        m_spdbSnapshotHOLDER = std::move(m_spdbSnapshotHOLDER->m_spdbSnapshotHOLDER);
        return;
    }

    mstime_t latency_endsnapshot;
    latencyStartMonitor(latency_endsnapshot);

    // Stage 1 Loop through all the tracked deletes and remove them from the snapshot DB
    dictIterator *di = dictGetIterator(m_pdictTombstone);
    dictEntry *de;
    while ((de = dictNext(di)) != NULL)
    {
        dictEntry **dePrev;
        dictht *ht;
        dictEntry *deSnapshot = dictFindWithPrev(m_spdbSnapshotHOLDER->m_pdict, dictGetKey(de), &dePrev, &ht);
        if (deSnapshot == nullptr && m_spdbSnapshotHOLDER->m_pdbSnapshot)
        {
            // The tombstone is for a grand child, propogate it (or possibly in the storage provider - but an extra tombstone won't hurt)
            serverAssert(m_spdbSnapshotHOLDER->m_pdbSnapshot->find_cached_threadsafe((const char*)dictGetKey(de)) != nullptr);
            dictAdd(m_spdbSnapshotHOLDER->m_pdictTombstone, sdsdupshared((sds)dictGetKey(de)), nullptr);
            continue;
        }
        else if (deSnapshot == nullptr)
        {
            serverAssert(m_spdbSnapshotHOLDER->m_spstorage != nullptr); // the only case where we can have a tombstone without a snapshot child is if a storage engine is set
            continue;
        }
        
        // Delete the object from the source dict, we don't use dictDelete to avoid a second search
        dictFreeKey(m_spdbSnapshotHOLDER->m_pdict, deSnapshot);
        dictFreeVal(m_spdbSnapshotHOLDER->m_pdict, deSnapshot);
        serverAssert(*dePrev == deSnapshot);
        *dePrev = deSnapshot->next;
        zfree(deSnapshot);
        ht->used--;
    }
    dictReleaseIterator(di);
    dictEmpty(m_pdictTombstone, nullptr);

    // Stage 2 Move all new keys to the snapshot DB
    dictMerge(m_spdbSnapshotHOLDER->m_pdict, m_pdict);
    
    // Stage 3 swap the databases with the snapshot
    std::swap(m_pdict, m_spdbSnapshotHOLDER->m_pdict);
    if (m_spdbSnapshotHOLDER->m_pdbSnapshot != nullptr)
        std::swap(m_pdictTombstone, m_spdbSnapshotHOLDER->m_pdictTombstone);
    
    // Finally free the snapshot
    if (m_pdbSnapshot != nullptr && m_spdbSnapshotHOLDER->m_pdbSnapshot != nullptr)
    {
        m_pdbSnapshot = m_spdbSnapshotHOLDER->m_pdbSnapshot;
        m_spdbSnapshotHOLDER->m_pdbSnapshot = nullptr;
    }
    else
    {
        m_pdbSnapshot = nullptr;
    }

    // Fixup the about to free'd snapshots iterator count so the dtor doesn't complain
    if (m_refCount)
    {
        m_spdbSnapshotHOLDER->m_pdict->iterators--;
    }

    auto spsnapshotFree = std::move(m_spdbSnapshotHOLDER);
    m_spdbSnapshotHOLDER = std::move(spsnapshotFree->m_spdbSnapshotHOLDER);
    if (serverTL != nullptr)
        g_pserver->garbageCollector.enqueue(serverTL->gcEpoch, std::move(spsnapshotFree));

    // Sanity Checks
    serverAssert(m_spdbSnapshotHOLDER != nullptr || m_pdbSnapshot == nullptr);
    serverAssert(m_pdbSnapshot == m_spdbSnapshotHOLDER.get() || m_pdbSnapshot == nullptr);
    serverAssert((m_refCount == 0 && m_pdict->iterators == 0) || (m_refCount != 0 && m_pdict->iterators == 1));
    serverAssert(m_spdbSnapshotHOLDER != nullptr || dictSize(m_pdictTombstone) == 0);
    serverAssert(sizeStart == size());

    latencyEndMonitor(latency_endsnapshot);
    latencyAddSampleIfNeeded("end-mvcc-snapshot", latency_endsnapshot);

    freeMemoryIfNeededAndSafe(false);
}

dict_iter redisDbPersistentDataSnapshot::random_cache_threadsafe(bool fPrimaryOnly) const
{
    if (size() == 0)
        return dict_iter(nullptr);
    if (!fPrimaryOnly && m_pdbSnapshot != nullptr && m_pdbSnapshot->size() > 0)
    {
        dict_iter iter(nullptr);
        double pctInSnapshot = (double)m_pdbSnapshot->size() / (size() + m_pdbSnapshot->size());
        double randval = (double)rand()/RAND_MAX;
        if (randval <= pctInSnapshot)
        {
            return m_pdbSnapshot->random_cache_threadsafe();
        }
    }
    if (dictSize(m_pdict) == 0)
        return dict_iter(nullptr);
    dictEntry *de = dictGetRandomKey(m_pdict);
    return dict_iter(de);
}

dict_iter redisDbPersistentDataSnapshot::find_cached_threadsafe(const char *key) const
{
    dictEntry *de = dictFind(m_pdict, key);
    if (de == nullptr && m_pdbSnapshot != nullptr && dictFind(m_pdictTombstone, key) == nullptr)
    {
        auto itr = m_pdbSnapshot->find_cached_threadsafe(key);
        if (itr != nullptr)
            return itr;
    }
    return dict_iter(de);
}

bool redisDbPersistentDataSnapshot::iterate_threadsafe(std::function<bool(const char*, robj_roptr o)> fn, bool fKeyOnly, bool fCacheOnly) const
{
    // Take the size so we can ensure we visited every element exactly once
    //  use volatile to ensure it's not checked too late.  This makes it more
    //  likely we'll detect races (but it won't gurantee it)
    volatile size_t celem = size();

    dictEntry *de = nullptr;
    bool fResult = true;

    dictIterator *di = dictGetSafeIterator(m_pdict);
    while(fResult && ((de = dictNext(di)) != nullptr))
    {
        --celem;
        robj *o = (robj*)dictGetVal(de);
        if (!fn((const char*)dictGetKey(de), o))
            fResult = false;
    }
    dictReleaseIterator(di);


    if (m_spstorage != nullptr && !fCacheOnly)
    {
        bool fSawAll = fResult && m_spstorage->enumerate([&](const char *key, size_t cchKey, const void *data, size_t cbData){
            sds sdsKey = sdsnewlen(key, cchKey);
            dictEntry *de = dictFind(m_pdict, sdsKey);
            bool fContinue = true;
            if (de == nullptr)
            {
                robj *o = nullptr;
                if (!fKeyOnly)
                {
                    size_t offset = 0;
                    deserializeExpire(sdsKey, (const char*)data, cbData, &offset);
                    o = deserializeStoredObject(this, sdsKey, reinterpret_cast<const char*>(data)+offset, cbData-offset);
                }
                fContinue = fn(sdsKey, o);
                if (o != nullptr)
                    decrRefCount(o);
            }
            
            sdsfree(sdsKey);
            return fContinue;
        });
        return fSawAll;
    }

    const redisDbPersistentDataSnapshot *psnapshot;
    __atomic_load(&m_pdbSnapshot, &psnapshot, __ATOMIC_ACQUIRE);
    if (fResult && psnapshot != nullptr)
    {
        fResult = psnapshot->iterate_threadsafe([this, &fn, &celem](const char *key, robj_roptr o) {
            dictEntry *deTombstone = dictFind(m_pdictTombstone, key);
            if (deTombstone != nullptr)
                return true;

            // Alright it's a key in the use keyspace, lets ensure it and then pass it off
            --celem;
            return fn(key, o);
        }, fKeyOnly, fCacheOnly);
    }

    // we should have hit all keys or had a good reason not to
    serverAssert(!fResult || celem == 0 || (m_spstorage && fCacheOnly));
    return fResult;
}

int redisDbPersistentDataSnapshot::snapshot_depth() const
{
    if (m_pdbSnapshot)
        return m_pdbSnapshot->snapshot_depth() + 1;
    return 0;
}


void redisDbPersistentData::consolidate_snapshot()
{
    aeAcquireLock();
    auto psnapshot = (m_pdbSnapshot != nullptr) ? m_spdbSnapshotHOLDER.get() : nullptr;
    if (psnapshot == nullptr)
    {
        aeReleaseLock();
        return;
    }
    psnapshot->m_refCount++;    // ensure it's not free'd
    aeReleaseLock();
    psnapshot->consolidate_children(this, false /* fForce */);
    aeAcquireLock();
    endSnapshot(psnapshot);
    aeReleaseLock();
}

// only call this on the "real" database to consolidate the first child
void redisDbPersistentDataSnapshot::consolidate_children(redisDbPersistentData *pdbPrimary, bool fForce)
{
    static fastlock s_lock {"consolidate_children"};    // this lock ensures only one thread is consolidating at a time

    std::unique_lock<fastlock> lock(s_lock, std::defer_lock);
    if (!lock.try_lock())
        return; // this is a best effort function

    if (!fForce && snapshot_depth() < 2)
        return;

    auto spdb = std::unique_ptr<redisDbPersistentDataSnapshot>(new (MALLOC_LOCAL) redisDbPersistentDataSnapshot());
    spdb->initialize();
    dictExpand(spdb->m_pdict, m_pdbSnapshot->size());

    m_pdbSnapshot->iterate_threadsafe([&](const char *key, robj_roptr o) {
        if (o != nullptr) {
            dictAdd(spdb->m_pdict, sdsdupshared(key), o.unsafe_robjcast());
            incrRefCount(o);
        }
        return true;
    }, true /*fKeyOnly*/, true /*fCacheOnly*/);
    spdb->m_spstorage = m_pdbSnapshot->m_spstorage;

    spdb->m_pdict->iterators++;

    serverAssert(spdb->size() == m_pdbSnapshot->size());

    // Now wire us in (Acquire the LOCK)
    AeLocker locker;
    locker.arm(nullptr);

    int depth = 0;
    redisDbPersistentDataSnapshot *psnapshotT = pdbPrimary->m_spdbSnapshotHOLDER.get();
    while (psnapshotT != nullptr)
    {
        ++depth;
        if (psnapshotT == this)
            break;
        psnapshotT = psnapshotT->m_spdbSnapshotHOLDER.get();
    }
    if (psnapshotT != this)
    {
        locker.disarm();    // don't run spdb's dtor in the lock
        return; // we were unlinked and this was a waste of time
    }

    serverLog(LL_VERBOSE, "cleaned %d snapshots", snapshot_depth()-1);
    spdb->m_refCount = depth;
    spdb->m_fConsolidated = true;
    // Drop our refs from this snapshot and its children
    psnapshotT = this;
    std::vector<redisDbPersistentDataSnapshot*> vecT;
    while ((psnapshotT = psnapshotT->m_spdbSnapshotHOLDER.get()) != nullptr)
    {
        vecT.push_back(psnapshotT);
    }
    for (auto itr = vecT.rbegin(); itr != vecT.rend(); ++itr)
    {
        psnapshotT = *itr;
        psnapshotT->m_refCount -= (depth-1);    // -1 because dispose will sub another
        gcDisposeSnapshot(psnapshotT);
    }
    std::atomic_thread_fence(std::memory_order_seq_cst);
    m_spdbSnapshotHOLDER.release(); // GC has responsibility for it now
    m_spdbSnapshotHOLDER = std::move(spdb);
    const redisDbPersistentDataSnapshot *ptrT = m_spdbSnapshotHOLDER.get();
    __atomic_store(&m_pdbSnapshot, &ptrT, __ATOMIC_SEQ_CST);
    locker.disarm();    // ensure we're not locked for any dtors
}

bool redisDbPersistentDataSnapshot::FStale() const
{
    // 0.5 seconds considered stale;
    static const uint64_t msStale = 500;
    return ((getMvccTstamp() - m_mvccCheckpoint) >> MVCC_MS_SHIFT) >= msStale;
}