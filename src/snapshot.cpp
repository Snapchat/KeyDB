#include "server.h"
#include "aelocker.h"

static const size_t c_elementsSmallLimit = 500000;
static fastlock s_lock {"consolidate_children"};    // this lock ensures only one thread is consolidating at a time

class LazyFree : public ICollectable
{
public:
    virtual ~LazyFree()
    {
        for (auto *de : vecde)
        {
            dbDictType.keyDestructor(nullptr, dictGetKey(de));
            dbDictType.valDestructor(nullptr, dictGetVal(de));
            zfree(de);
        }
        for (robj *o : vecobjLazyFree)
            decrRefCount(o);
        for (dict *d : vecdictLazyFree)
            dictRelease(d);
    }

    std::vector<dict*> vecdictLazyFree;
    std::vector<robj*> vecobjLazyFree;
    std::vector<dictEntry*> vecde;
};

const redisDbPersistentDataSnapshot *redisDbPersistentData::createSnapshot(uint64_t mvccCheckpoint, bool fOptional)
{
    serverAssert(GlobalLocksAcquired());
    serverAssert(m_refCount == 0);  // do not call this on a snapshot

    if (performEvictions(true /*fPreSnapshot*/) != C_OK && fOptional)
        return nullptr; // can't create snapshot due to OOM

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

    // See if we have too many levels and can bail out of this to reduce load
    if (fOptional && (levels >= 6))
    {
        serverLog(LL_DEBUG, "Snapshot nesting too deep, abondoning");
        return nullptr;
    }

    auto spdb = std::unique_ptr<redisDbPersistentDataSnapshot>(new (MALLOC_LOCAL) redisDbPersistentDataSnapshot());
    
    // We can't have async rehash modifying these.  Setting the asyncdata list to null
    //  will cause us to throw away the async work rather than modify the tables in flight
    discontinueAsyncRehash(m_pdict);
    discontinueAsyncRehash(m_pdictTombstone);

    spdb->m_fAllChanged = false;
    spdb->m_fTrackingChanges = 0;
    spdb->m_pdict = m_pdict;
    spdb->m_pdictTombstone = m_pdictTombstone;
    // Add a fake iterator so the dicts don't rehash (they need to be read only)
    dictPauseRehashing(spdb->m_pdict);
    dictForceRehash(spdb->m_pdictTombstone);    // prevent rehashing by finishing the rehash now
    spdb->m_spdbSnapshotHOLDER = std::move(m_spdbSnapshotHOLDER);
    if (m_spstorage != nullptr)
        spdb->m_spstorage = std::shared_ptr<StorageCache>(const_cast<StorageCache*>(m_spstorage->clone()));
    spdb->m_pdbSnapshot = m_pdbSnapshot;
    spdb->m_refCount = 1;
    spdb->m_mvccCheckpoint = getMvccTstamp();
    if (m_setexpire != nullptr)
    {
        std::unique_lock<fastlock> ul(g_expireLock);
        spdb->m_setexpire =  new (MALLOC_LOCAL) expireset(*m_setexpire);
        spdb->m_setexpire->pause_rehash();  // needs to be const
    }

    if (dictIsRehashing(spdb->m_pdict) || dictIsRehashing(spdb->m_pdictTombstone)) {
        serverLog(LL_VERBOSE, "NOTICE: Suboptimal snapshot");
    }

    m_pdict = dictCreate(&dbDictType,this);
    dictExpand(m_pdict, 1024);   // minimize rehash overhead
    m_pdictTombstone = dictCreate(&dbTombstoneDictType, this);

    serverAssert(spdb->m_pdict->pauserehash == 1);

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

    std::atomic_thread_fence(std::memory_order_seq_cst);
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
        psnapshot->m_pdbSnapshot = nullptr;
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
    {
    std::unique_lock<fastlock> ul(g_expireLock);
    delete m_setexpire;
    m_setexpire = new (MALLOC_LOCAL) expireset(*psnapshot->m_setexpire);
    }
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
    mstime_t latency;

    aeAcquireLock();
    while (dictIsRehashing(m_pdict) || dictIsRehashing(m_pdictTombstone)) {
        dictRehashMilliseconds(m_pdict, 1);
        dictRehashMilliseconds(m_pdictTombstone, 1);
        // Give someone else a chance
        aeReleaseLock();
        usleep(300);
        aeAcquireLock();
    }
    
    latencyStartMonitor(latency);
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
            latencyEndMonitor(latency);
            latencyAddSampleIfNeeded("end-snapshot-async-synchronous-path", latency);
            aeReleaseLock();
            return;
        }

        // OK this is a big snapshot so lets do the merge work outside the lock
        auto psnapshotT = createSnapshot(LLONG_MAX, false);
        endSnapshot(psnapshot); // this will just dec the ref count since our new snapshot has a ref 
        psnapshot = nullptr;

    latencyEndMonitor(latency);
    latencyAddSampleIfNeeded("end-snapshot-async-phase-1", latency);
    aeReleaseLock();

    // do the expensive work of merging snapshots outside the ref
    if (const_cast<redisDbPersistentDataSnapshot*>(psnapshotT)->freeTombstoneObjects(1))    // depth is one because we just creted it
    {
        aeAcquireLock();
        if (m_pdbSnapshotASYNC != nullptr)
            endSnapshot(m_pdbSnapshotASYNC);
        m_pdbSnapshotASYNC = nullptr;
        endSnapshot(psnapshotT);
        aeReleaseLock();
        return;
    }
    
    // Final Cleanup
    aeAcquireLock(); latencyStartMonitor(latency);
        if (m_pdbSnapshotASYNC == nullptr)
            m_pdbSnapshotASYNC = psnapshotT;
        else
            endSnapshot(psnapshotT);    // finally clean up our temp snapshot

    latencyEndMonitor(latency);
    latencyAddSampleIfNeeded("end-snapshot-async-phase-2", latency);
    aeReleaseLock();
}

bool redisDbPersistentDataSnapshot::freeTombstoneObjects(int depth)
{
    if (m_pdbSnapshot == nullptr)
    {
        serverAssert(dictSize(m_pdictTombstone) == 0);
        return true;
    }

    if (!const_cast<redisDbPersistentDataSnapshot*>(m_pdbSnapshot)->freeTombstoneObjects(depth+1))
        return false;

    {
    AeLocker ae;
    ae.arm(nullptr);
    if (m_pdbSnapshot->m_refCount != depth && (m_pdbSnapshot->m_refCount != (m_refCount+1)))
        return false;
    ae.disarm();
    }

    std::unique_lock<fastlock> lock(s_lock, std::defer_lock);
    if (!lock.try_lock())
        return false; // this is a best effort function
    
    std::unique_ptr<LazyFree> splazy = std::make_unique<LazyFree>();

    dict *dictTombstoneNew = dictCreate(&dbTombstoneDictType, nullptr);
    dictIterator *di = dictGetIterator(m_pdictTombstone);
    dictEntry *de;
    std::vector<dictEntry*> vecdeFree;
    vecdeFree.reserve(dictSize(m_pdictTombstone));
    unsigned rgcremoved[2] = {0};
    while ((de = dictNext(di)) != nullptr)
    {
        dictEntry **dePrev = nullptr;
        dictht *ht = nullptr;
        sds key = (sds)dictGetKey(de);
        // BUG BUG: Why can't we do a shallow search here?
        dictEntry *deObj = dictFindWithPrev(m_pdbSnapshot->m_pdict, key, (uint64_t)dictGetVal(de), &dePrev, &ht, false);

        if (deObj != nullptr)
        {
            // Now unlink the DE
            __atomic_store(dePrev, &deObj->next, __ATOMIC_RELEASE);
            if (ht == &m_pdbSnapshot->m_pdict->ht[0])
                rgcremoved[0]++;
            else
                rgcremoved[1]++;
            splazy->vecde.push_back(deObj);
        } else {
            serverAssert(dictFind(m_pdbSnapshot->m_pdict, key) == nullptr);
            serverAssert(m_pdbSnapshot->find_cached_threadsafe(key) != nullptr);
            dictAdd(dictTombstoneNew, sdsdupshared((sds)dictGetKey(de)), dictGetVal(de));
        }
    }
    dictReleaseIterator(di);

    dictForceRehash(dictTombstoneNew);
    aeAcquireLock();
    if (m_pdbSnapshot->m_pdict->asyncdata != nullptr) {
        // In this case we use the asyncdata to free us, not our own lazy free
        for (auto de : splazy->vecde)
            dictFreeUnlinkedEntry(m_pdbSnapshot->m_pdict, de);
        splazy->vecde.clear();
    }
    dict *dT = m_pdbSnapshot->m_pdict;
    splazy->vecdictLazyFree.push_back(m_pdictTombstone);
    __atomic_store(&m_pdictTombstone, &dictTombstoneNew, __ATOMIC_RELEASE);
    __atomic_fetch_sub(&dT->ht[0].used, rgcremoved[0], __ATOMIC_RELEASE);
    __atomic_fetch_sub(&dT->ht[1].used, rgcremoved[1], __ATOMIC_RELEASE);
    serverLog(LL_WARNING, "tombstones removed: %u, remain: %lu", rgcremoved[0]+rgcremoved[1], dictSize(m_pdictTombstone));
    g_pserver->garbageCollector.enqueue(serverTL->gcEpoch, std::move(splazy));
    aeReleaseLock();
    
    return true;
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

    mstime_t latency_endsnapshot;
    latencyStartMonitor(latency_endsnapshot);

    // Alright we're ready to be free'd, but first dump all the refs on our child snapshots
    if (m_spdbSnapshotHOLDER->m_refCount == 1)
        recursiveFreeSnapshots(m_spdbSnapshotHOLDER.get());

    m_spdbSnapshotHOLDER->m_refCount--;
    if (m_spdbSnapshotHOLDER->m_refCount > 0)
        return;

    size_t sizeStart = size();
    serverAssert(m_spdbSnapshotHOLDER->m_refCount == 0);
    serverAssert((m_refCount == 0 && m_pdict->pauserehash == 0) || (m_refCount != 0 && m_pdict->pauserehash == 1));

    serverAssert(m_spdbSnapshotHOLDER->m_pdict->pauserehash == 1);  // All iterators should have been free'd except the fake one from createSnapshot
    if (m_refCount == 0)
    {
        dictResumeRehashing(m_spdbSnapshotHOLDER->m_pdict);
    }

    if (m_pdbSnapshot == nullptr)
    {
        // the database was cleared so we don't need to recover the snapshot
        dictEmpty(m_pdictTombstone, nullptr);
        m_spdbSnapshotHOLDER = std::move(m_spdbSnapshotHOLDER->m_spdbSnapshotHOLDER);
        return;
    }

    // Stage 1 Loop through all the tracked deletes and remove them from the snapshot DB
    dictIterator *di = dictGetIterator(m_pdictTombstone);
    dictEntry *de;
    dictPauseRehashing(m_spdbSnapshotHOLDER->m_pdict);
    auto splazy = std::make_unique<LazyFree>();
    while ((de = dictNext(di)) != NULL)
    {
        dictEntry **dePrev;
        dictht *ht;
        // BUG BUG Why not a shallow search?
        dictEntry *deSnapshot = dictFindWithPrev(m_spdbSnapshotHOLDER->m_pdict, dictGetKey(de), (uint64_t)dictGetVal(de), &dePrev, &ht, false /*!!sdsisshared((sds)dictGetKey(de))*/);
        if (deSnapshot == nullptr && m_spdbSnapshotHOLDER->m_pdbSnapshot)
        {
            // The tombstone is for a grand child, propogate it (or possibly in the storage provider - but an extra tombstone won't hurt)
#ifdef CHECKED_BUILD
            serverAssert(m_spdbSnapshotHOLDER->m_pdbSnapshot->find_cached_threadsafe((const char*)dictGetKey(de)) != nullptr);
#endif
            dictAdd(m_spdbSnapshotHOLDER->m_pdictTombstone, sdsdupshared((sds)dictGetKey(de)), dictGetVal(de));
            continue;
        }
        else if (deSnapshot == nullptr)
        {
            serverAssert(m_spdbSnapshotHOLDER->m_spstorage != nullptr); // the only case where we can have a tombstone without a snapshot child is if a storage engine is set
            continue;
        }
        
        // Delete the object from the source dict, we don't use dictDelete to avoid a second search
        *dePrev = deSnapshot->next; // Unlink it first
        if (deSnapshot != nullptr) {
            if (m_spdbSnapshotHOLDER->m_pdict->asyncdata != nullptr) {
                dictFreeUnlinkedEntry(m_spdbSnapshotHOLDER->m_pdict, deSnapshot);
            } else {
                splazy->vecde.push_back(deSnapshot);
            }
        }
        ht->used--;
    }

    
    dictResumeRehashing(m_spdbSnapshotHOLDER->m_pdict);
    dictReleaseIterator(di);
    splazy->vecdictLazyFree.push_back(m_pdictTombstone);
    m_pdictTombstone = dictCreate(&dbTombstoneDictType, nullptr);

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
    }
    else
    {
        m_pdbSnapshot = nullptr;
    }
    m_spdbSnapshotHOLDER->m_pdbSnapshot = nullptr;

    // Fixup the about to free'd snapshots iterator count so the dtor doesn't complain
    if (m_refCount)
    {
        dictResumeRehashing(m_spdbSnapshotHOLDER->m_pdict);
    }

    auto spsnapshotFree = std::move(m_spdbSnapshotHOLDER);
    m_spdbSnapshotHOLDER = std::move(spsnapshotFree->m_spdbSnapshotHOLDER);
    if (serverTL != nullptr) {
        g_pserver->garbageCollector.enqueue(serverTL->gcEpoch, std::move(spsnapshotFree));
        g_pserver->garbageCollector.enqueue(serverTL->gcEpoch, std::move(splazy));
    }

    // Sanity Checks
    serverAssert(m_spdbSnapshotHOLDER != nullptr || m_pdbSnapshot == nullptr);
    serverAssert(m_pdbSnapshot == m_spdbSnapshotHOLDER.get() || m_pdbSnapshot == nullptr);
    serverAssert((m_refCount == 0 && m_pdict->pauserehash == 0) || (m_refCount != 0 && m_pdict->pauserehash == 1));
    serverAssert(m_spdbSnapshotHOLDER != nullptr || dictSize(m_pdictTombstone) == 0);
    serverAssert(sizeStart == size());

    latencyEndMonitor(latency_endsnapshot);
    latencyAddSampleIfNeeded("end-mvcc-snapshot", latency_endsnapshot);

    performEvictions(false);
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
    return dict_iter(m_pdict, de);
}

dict_iter redisDbPersistentData::find_cached_threadsafe(const char *key) const
{
    dict *dictTombstone;
    __atomic_load(&m_pdictTombstone, &dictTombstone, __ATOMIC_ACQUIRE);
    dictEntry *de = dictFind(m_pdict, key);
    if (de == nullptr && m_pdbSnapshot != nullptr && dictFind(dictTombstone, key) == nullptr)
    {
        auto itr = m_pdbSnapshot->find_cached_threadsafe(key);
        if (itr != nullptr)
            return itr;
    }
    return dict_iter(m_pdict, de);
}

struct scan_callback_data
{
    dict *dictTombstone;
    sds type;
    list *keys;
};
void snapshot_scan_callback(void *privdata, const dictEntry *de)
{
    scan_callback_data *data = (scan_callback_data*)privdata;
    if (data->dictTombstone != nullptr && dictFind(data->dictTombstone, dictGetKey(de)) != nullptr)
        return;
    
    sds sdskey = (sds)dictGetKey(de);
    if (data->type != nullptr)
    {
        if (strcasecmp(data->type, getObjectTypeName((robj*)dictGetVal(de))) != 0)
            return;
    }
    listAddNodeHead(data->keys, createStringObject(sdskey, sdslen(sdskey)));
}
unsigned long redisDbPersistentDataSnapshot::scan_threadsafe(unsigned long iterator, long count, sds type, list *keys) const
{
    unsigned long iteratorReturn = 0;

    scan_callback_data data;
    data.dictTombstone = m_pdictTombstone;
    data.keys = keys;
    data.type = type;

    const redisDbPersistentDataSnapshot *psnapshot;
    __atomic_load(&m_pdbSnapshot, &psnapshot, __ATOMIC_ACQUIRE);
    if (psnapshot != nullptr)
    {
        // Always process the snapshot first as we assume its bigger than we are
        iteratorReturn = psnapshot->scan_threadsafe(iterator, count, type, keys);

        // Just catch up with our snapshot
        do
        {
            iterator = dictScan(m_pdict, iterator, snapshot_scan_callback, nullptr, &data);
        } while (iterator != 0 && (iterator < iteratorReturn || iteratorReturn == 0));
    }
    else
    {
        long maxiterations = count * 10; // allow more iterations than keys for sparse tables
        iteratorReturn = iterator;
        do {
            iteratorReturn = dictScan(m_pdict, iteratorReturn, snapshot_scan_callback, NULL, &data);
        } while (iteratorReturn &&
              maxiterations-- &&
              listLength(keys) < (unsigned long)count);
    }
    
    return iteratorReturn;
}

bool redisDbPersistentDataSnapshot::iterate_threadsafe(std::function<bool(const char*, robj_roptr o)> fn, bool fKeyOnly, bool fCacheOnly) const
{
    return iterate_threadsafe_core(fn, fKeyOnly, fCacheOnly, true);
}

bool redisDbPersistentDataSnapshot::iterate_threadsafe_core(std::function<bool(const char*, robj_roptr o)> &fn, bool fKeyOnly, bool fCacheOnly, bool fFirst) const
{
    // Take the size so we can ensure we visited every element exactly once
    //  use volatile to ensure it's not checked too late.  This makes it more
    //  likely we'll detect races (but it won't gurantee it)
    aeAcquireLock();
    dict *dictTombstone;
    __atomic_load(&m_pdictTombstone, &dictTombstone, __ATOMIC_ACQUIRE);
    volatile ssize_t celem = (ssize_t)size();
    aeReleaseLock();

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
        std::function<bool(const char*, robj_roptr o)> fnNew = [&fn, &celem, dictTombstone](const char *key, robj_roptr o) {
            dictEntry *deTombstone = dictFind(dictTombstone, key);
            if (deTombstone != nullptr)
                return true;

            // Alright it's a key in the use keyspace, lets ensure it and then pass it off
            --celem;
            return fn(key, o);
        };
        fResult = psnapshot->iterate_threadsafe_core(fnNew, fKeyOnly, fCacheOnly, false);
    }

    // we should have hit all keys or had a good reason not to
    if (!(!fResult || celem == 0 || (m_spstorage && fCacheOnly)))
        serverLog(LL_WARNING, "celem: %ld", celem);
    serverAssert(!fResult || celem == 0 || (m_spstorage && fCacheOnly) || !fFirst);
    return fResult;
}

int redisDbPersistentDataSnapshot::snapshot_depth() const
{
    if (m_pdbSnapshot)
        return m_pdbSnapshot->snapshot_depth() + 1;
    return 0;
}

bool redisDbPersistentDataSnapshot::FStale() const
{
    return ((getMvccTstamp() - m_mvccCheckpoint) >> MVCC_MS_SHIFT) >= static_cast<uint64_t>(g_pserver->snapshot_slip);
}

void dictGCAsyncFree(dictAsyncRehashCtl *async) {
    if (async->deGCList != nullptr && serverTL != nullptr && !serverTL->gcEpoch.isReset()) {
        auto splazy = std::make_unique<LazyFree>();
        auto *de = async->deGCList;
        while (de != nullptr) {
            splazy->vecde.push_back(de);
            de = de->next;
        }
        async->deGCList = nullptr;
        g_pserver->garbageCollector.enqueue(serverTL->gcEpoch, std::move(splazy));
    }
    delete async;
}