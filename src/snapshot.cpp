#include "server.h"
#include "aelocker.h"

const redisDbPersistentDataSnapshot *redisDbPersistentData::createSnapshot(uint64_t mvccCheckpoint, bool fOptional)
{
    serverAssert(GlobalLocksAcquired());
    serverAssert(m_refCount == 0);  // do not call this on a snapshot

    // First see if we have too many levels and can bail out of this to reduce load
    int levels = 1;
    redisDbPersistentDataSnapshot *psnapshot = m_spdbSnapshotHOLDER.get();
    while (psnapshot != nullptr)
    {
        ++levels;
        psnapshot = psnapshot->m_spdbSnapshotHOLDER.get();
    }
    if (fOptional && (levels > 8))
        return nullptr;

    if (m_spdbSnapshotHOLDER != nullptr)
    {
        // If possible reuse an existing snapshot (we want to minimize nesting)
        if (mvccCheckpoint <= m_spdbSnapshotHOLDER->mvccCheckpoint)
        {
            if (((getMvccTstamp() - m_spdbSnapshotHOLDER->mvccCheckpoint) >> MVCC_MS_SHIFT) < 1*1000)
            {
                m_spdbSnapshotHOLDER->m_refCount++;
                return m_spdbSnapshotHOLDER.get();
            }
            serverLog(LL_WARNING, "Existing snapshot too old, creating a new one");
        }
        serverLog(levels > 5 ? LL_NOTICE : LL_VERBOSE, "Nested snapshot created: %d levels", levels);
    }
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
    spdb->mvccCheckpoint = getMvccTstamp();
    if (m_setexpire != nullptr)
        spdb->m_setexpire = m_setexpire;

    m_pdict = dictCreate(&dbDictType,this);
    m_pdictTombstone = dictCreate(&dbDictType, this);
    m_setexpire = new (MALLOC_LOCAL) expireset();

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
        serverLog(LL_WARNING, "Garbage collected snapshot");
    }
}

void redisDbPersistentData::endSnapshot(const redisDbPersistentDataSnapshot *psnapshot)
{
    // Note: This function is dependent on GlobalLocksAcquried(), but rdb background saving has a weird case where
    //  a seperate thread holds the lock for it.  Yes that's pretty crazy and should be fixed somehow...

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

    // Stage 1 Loop through all the tracked deletes and remove them from the snapshot DB
    dictIterator *di = dictGetIterator(m_pdictTombstone);
    dictEntry *de;
    while ((de = dictNext(di)) != NULL)
    {
        dictEntry *deSnapshot = dictFind(m_spdbSnapshotHOLDER->m_pdict, dictGetKey(de));
        if (deSnapshot == nullptr)
        {
            // The tombstone is for a grand child, propogate it
            serverAssert(m_spdbSnapshotHOLDER->m_pdbSnapshot->find_threadsafe((const char*)dictGetKey(de)) != nullptr);
            dictAdd(m_spdbSnapshotHOLDER->m_pdictTombstone, sdsdup((sds)dictGetKey(de)), nullptr);
            continue;
        }
        
        robj *obj = (robj*)dictGetVal(deSnapshot);
        const char *key = (const char*)dictGetKey(deSnapshot);
        if (obj == nullptr || obj->FExpires())
        {
            auto itrExpire = m_spdbSnapshotHOLDER->m_setexpire->find(key);
            if (itrExpire != m_spdbSnapshotHOLDER->m_setexpire->end())
            {
                m_spdbSnapshotHOLDER->m_setexpire->erase(itrExpire);  // Note: normally we would have to set obj::fexpire false but we're deleting it anyways...
            }
        }
        dictDelete(m_spdbSnapshotHOLDER->m_pdict, key);
    }
    dictReleaseIterator(di);
    dictEmpty(m_pdictTombstone, nullptr);

    // Stage 2 Move all new keys to the snapshot DB
    di = dictGetIterator(m_pdict);
    while ((de = dictNext(di)) != NULL)
    {
        robj *o = (robj*)dictGetVal(de);
        dictEntry *deExisting = dictFind(m_spdbSnapshotHOLDER->m_pdict, (const char*)dictGetKey(de));
        if (deExisting != nullptr)
        {
            if (dictGetVal(deExisting) != nullptr)
                decrRefCount((robj*)dictGetVal(deExisting));
            dictSetVal(m_spdbSnapshotHOLDER->m_pdict, deExisting, o);
        }
        else
        {
            dictAdd(m_spdbSnapshotHOLDER->m_pdict, sdsdup((sds)dictGetKey(de)), o);
        }
        if (dictGetVal(de) != nullptr)
            incrRefCount((robj*)dictGetVal(de));

        if (o->FExpires() || o == nullptr)
        {
            auto itr = m_setexpire->find((const char*)dictGetKey(de));
            serverAssert(o == nullptr || itr != m_setexpire->end());
            if (itr != m_setexpire->end())
                m_spdbSnapshotHOLDER->m_setexpire->insert(*itr);
        }
    }
    dictReleaseIterator(di);
    
    // Stage 3 swap the databases with the snapshot
    std::swap(m_pdict, m_spdbSnapshotHOLDER->m_pdict);
    std::swap(m_setexpire, m_spdbSnapshotHOLDER->m_setexpire);
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

    m_spdbSnapshotHOLDER = std::move(m_spdbSnapshotHOLDER->m_spdbSnapshotHOLDER);
    serverAssert(m_spdbSnapshotHOLDER != nullptr || m_pdbSnapshot == nullptr);
    serverAssert(m_pdbSnapshot == m_spdbSnapshotHOLDER.get() || m_pdbSnapshot == nullptr);
    serverAssert((m_refCount == 0 && m_pdict->iterators == 0) || (m_refCount != 0 && m_pdict->iterators == 1));
    serverAssert(m_spdbSnapshotHOLDER != nullptr || dictSize(m_pdictTombstone) == 0);
}

dict_iter redisDbPersistentDataSnapshot::random_threadsafe() const
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
            return m_pdbSnapshot->random_threadsafe();
        }
    }
    serverAssert(dictSize(m_pdict) > 0);
    dictEntry *de = dictGetRandomKey(m_pdict);
    return dict_iter(de);
}

dict_iter redisDbPersistentDataSnapshot::find_threadsafe(const char *key) const
{
    dictEntry *de = dictFind(m_pdict, key);
    if (de == nullptr && m_pdbSnapshot != nullptr)
    {
        auto itr = m_pdbSnapshot->find_threadsafe(key);
        if (itr != nullptr && dictFind(m_pdictTombstone, itr.key()) == nullptr)
            return itr;
    }
    return dict_iter(de);
}

bool redisDbPersistentDataSnapshot::iterate_threadsafe(std::function<bool(const char*, robj_roptr o)> fn, bool fKeyOnly) const
{
    dictEntry *de = nullptr;
    bool fResult = true;

    // Take the size so we can ensure we visited every element exactly once
    //  use volatile to ensure it's not checked too late.  This makes it more
    //  likely we'll detect races (but it won't gurantee it)
    volatile size_t celem = size();

    dictIterator *di = dictGetSafeIterator(m_pdict);
    while(fResult && ((de = dictNext(di)) != nullptr))
    {
        --celem;
        robj *o = (robj*)dictGetVal(de);
        if (o == nullptr && !fKeyOnly)
        {
            m_spstorage->retrieve((sds)dictGetKey(de), sdslen((sds)dictGetKey(de)), [&](const char *, size_t, const void *data, size_t cb){
                o = deserializeStoredObject(this, (const char*)dictGetKey(de), data, cb);
            });
        }

        if (!fn((const char*)dictGetKey(de), o))
            fResult = false;

        if (o != nullptr && dictGetVal(de) == nullptr)
            decrRefCount(o);
    }
    dictReleaseIterator(di);

    redisDbPersistentDataSnapshot *psnapshot;
    __atomic_load(&m_pdbSnapshot, &psnapshot, __ATOMIC_ACQUIRE);
    if (fResult && psnapshot != nullptr)
    {
        fResult = psnapshot->iterate_threadsafe([this, &fn, &celem](const char *key, robj_roptr o){
            // Before passing off to the user we need to make sure it's not already in the
            //  the current set, and not deleted
            dictEntry *deCurrent = dictFind(m_pdict, key);
            if (deCurrent != nullptr)
                return true;
            
            dictEntry *deTombstone = dictFind(m_pdictTombstone, key);
            if (deTombstone != nullptr)
                return true;

            // Alright it's a key in the use keyspace, lets ensure it and then pass it off
            --celem;
            return fn(key, o);
        }, fKeyOnly);
    }

    serverAssert(!fResult || celem == 0);
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
    psnapshot->consolidate_children(this);
    aeAcquireLock();
    endSnapshot(psnapshot);
    aeReleaseLock();
}

// only call this on the "real" database to consolidate the first child
void redisDbPersistentDataSnapshot::consolidate_children(redisDbPersistentData *pdbPrimary)
{
    static fastlock s_lock {"consolidate_children"};    // this lock ensures only one thread is consolidating at a time

    std::unique_lock<fastlock> lock(s_lock, std::defer_lock);
    if (!lock.try_lock())
        return; // this is a best effort function

    if (snapshot_depth() < 4)
        return;

    auto spdb = std::unique_ptr<redisDbPersistentDataSnapshot>(new (MALLOC_LOCAL) redisDbPersistentDataSnapshot());
    spdb->initialize();
    dictExpand(spdb->m_pdict, m_pdbSnapshot->size());

    m_pdbSnapshot->iterate_threadsafe([&](const char *key, robj_roptr o){
        if (o != nullptr)
            incrRefCount(o);
        dictAdd(spdb->m_pdict, sdsdup(key), o.unsafe_robjcast());
        return true;
    }, true /*fKeyOnly*/);
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

    serverLog(LL_WARNING, "cleaned %d snapshots", snapshot_depth()-1);
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
    auto ptrT = m_spdbSnapshotHOLDER.get();
    __atomic_store(&m_pdbSnapshot, &ptrT, __ATOMIC_SEQ_CST);
    locker.disarm();    // ensure we're not locked for any dtors
}