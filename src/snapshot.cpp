#include "server.h"

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
        if (mvccCheckpoint <= m_spdbSnapshotHOLDER->mvccCheckpoint)
        {
            m_spdbSnapshotHOLDER->m_refCount++;
            return m_spdbSnapshotHOLDER.get();
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
    spdb->m_pdbSnapshot = m_pdbSnapshot;
    spdb->m_refCount = 1;
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

void redisDbPersistentData::endSnapshot(const redisDbPersistentDataSnapshot *psnapshot)
{
    // Note: This function is dependent on GlobalLocksAcquried(), but rdb background saving has a weird case where
    //  a seperate thread holds the lock for it.  Yes that's pretty crazy and should be fixed somehow...

    if (m_spdbSnapshotHOLDER.get() != psnapshot)
    {
        serverAssert(m_spdbSnapshotHOLDER != nullptr);
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
            continue;   // sometimes we delete things that were never in the snapshot
        
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
        dictEntry *deExisting = dictFind(m_spdbSnapshotHOLDER->m_pdict, (const char*)dictGetKey(de));
        if (deExisting != nullptr)
        {
            decrRefCount((robj*)dictGetVal(deExisting));
            dictSetVal(m_spdbSnapshotHOLDER->m_pdict, deExisting, dictGetVal(de));
        }
        else
        {
            dictAdd(m_spdbSnapshotHOLDER->m_pdict, sdsdup((sds)dictGetKey(de)), dictGetVal(de));
        }
        incrRefCount((robj*)dictGetVal(de));
    }
    dictReleaseIterator(di);
    
    // Stage 3 swap the databases with the snapshot
    std::swap(m_pdict, m_spdbSnapshotHOLDER->m_pdict);

    // Stage 4 merge all expires
    // TODO
    std::swap(m_setexpire, m_spdbSnapshotHOLDER->m_setexpire);
    
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

bool redisDbPersistentDataSnapshot::iterate_threadsafe(std::function<bool(const char*, robj_roptr o)> fn) const
{
    dictEntry *de = nullptr;
    bool fResult = true;

    dictIterator *di = dictGetIterator(m_pdict);
    while((de = dictNext(di)) != nullptr)
    {
        if (!fn((const char*)dictGetKey(de), (robj*)dictGetVal(de)))
        {
            fResult = false;
            break;
        }
    }
    dictReleaseIterator(di);

    if (fResult && m_pdbSnapshot != nullptr)
    {
        fResult = m_pdbSnapshot->iterate_threadsafe([&](const char *key, robj_roptr o){
            // Before passing off to the user we need to make sure it's not already in the
            //  the current set, and not deleted
            dictEntry *deCurrent = dictFind(m_pdict, key);
            if (deCurrent != nullptr)
                return true;
            dictEntry *deTombstone = dictFind(m_pdictTombstone, key);
            if (deTombstone != nullptr)
                return true;

            // Alright it's a key in the use keyspace, lets ensure it and then pass it off
            return fn(key, o);
        });
    }

    return fResult;
}
