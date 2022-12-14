#include "server.h"

uint64_t hashPassthrough(const void *hash) {
    return static_cast<uint64_t>(reinterpret_cast<uintptr_t>(hash));
}

int hashCompare(void *, const void *key1, const void *key2) {
    auto diff = (reinterpret_cast<uintptr_t>(key1) - reinterpret_cast<uintptr_t>(key2));
    return !diff;
}

dictType dbStorageCacheType = {
    hashPassthrough,            /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    hashCompare,                /* key compare */
    NULL,                       /* key destructor */
    NULL                        /* val destructor */
};

StorageCache::StorageCache(IStorage *storage, bool fCache)
        : m_spstorage(storage)
{
    if (fCache)
        m_pdict = dictCreate(&dbStorageCacheType, nullptr);
}

StorageCache::~StorageCache()
{
    if (m_pdict != nullptr)
        dictRelease(m_pdict);
}

void StorageCache::clear(void(callback)(void*))
{
    std::unique_lock<fastlock> ul(m_lock);
    if (m_pdict != nullptr)
        dictEmpty(m_pdict, callback);
    m_spstorage->clear();
    m_collisionCount = 0;
}

void StorageCache::clearAsync()
{
    std::unique_lock<fastlock> ul(m_lock);
    if (count() == 0)
        return;
    if (m_pdict != nullptr) {
        dict *dSav = m_pdict;
        m_pdict = dictCreate(&dbStorageCacheType, nullptr);
        g_pserver->asyncworkqueue->AddWorkFunction([dSav]{
            dictEmpty(dSav, nullptr);
        });
    }
    m_spstorage->clear();
    m_collisionCount = 0;
}

void StorageCache::cacheKey(sds key)
{
    if (m_pdict == nullptr)
        return;
    uintptr_t hash = dictSdsHash(key);
    if (dictAdd(m_pdict, reinterpret_cast<void*>(hash), (void*)1) != DICT_OK) {
        dictEntry *de = dictFind(m_pdict, reinterpret_cast<void*>(hash));
        serverAssert(de != nullptr);
        de->v.s64++;
        m_collisionCount++;
    }
}

void StorageCache::cacheKey(const char *rgch, size_t cch)
{
    if (m_pdict == nullptr)
        return;
    uintptr_t hash = dictGenHashFunction(rgch, (int)cch);
    if (dictAdd(m_pdict, reinterpret_cast<void*>(hash), (void*)1) != DICT_OK) {
        dictEntry *de = dictFind(m_pdict, reinterpret_cast<void*>(hash));
        serverAssert(de != nullptr);
        de->v.s64++;
        m_collisionCount++;
    }
}

bool StorageCache::erase(sds key)
{
    bool result = m_spstorage->erase(key, sdslen(key));
    std::unique_lock<fastlock> ul(m_lock);
    if (result && m_pdict != nullptr)
    {
        uint64_t hash = dictSdsHash(key);
        dictEntry *de = dictFind(m_pdict, reinterpret_cast<void*>(hash));
        serverAssert(de != nullptr);
        de->v.s64--;
        serverAssert(de->v.s64 >= 0);
        if (de->v.s64 == 0) {
            dictDelete(m_pdict, reinterpret_cast<void*>(hash));
        } else {
            m_collisionCount--;
        }
    }
    return result;
}

void StorageCache::insert(sds key, const void *data, size_t cbdata, bool fOverwrite)
{
    std::unique_lock<fastlock> ul(m_lock);
    if (!fOverwrite && m_pdict != nullptr)
    {
        cacheKey(key);
    }
    ul.unlock();
    m_spstorage->insert(key, sdslen(key), (void*)data, cbdata, fOverwrite);
}

long _dictKeyIndex(dict *d, const void *key, uint64_t hash, dictEntry **existing);
void StorageCache::bulkInsert(char **rgkeys, size_t *rgcbkeys, char **rgvals, size_t *rgcbvals, size_t celem)
{
    std::vector<dictEntry*> vechashes;
    if (m_pdict != nullptr) {
        vechashes.reserve(celem);

        for (size_t ielem = 0; ielem < celem; ++ielem) {
            dictEntry *de = (dictEntry*)zmalloc(sizeof(dictEntry));
            de->key = (void*)dictGenHashFunction(rgkeys[ielem], (int)rgcbkeys[ielem]);
            de->v.u64 = 1;
            vechashes.push_back(de);
        }
    }

    std::unique_lock<fastlock> ul(m_lock);
    bulkInsertsInProgress++;
    if (m_pdict != nullptr) {
        for (dictEntry *de : vechashes) {
            if (dictIsRehashing(m_pdict)) dictRehash(m_pdict,1);
            /* Get the index of the new element, or -1 if
                * the element already exists. */
            long index;
            if ((index = _dictKeyIndex(m_pdict, de->key, (uint64_t)de->key, nullptr)) == -1) {
                dictEntry *deLocal = dictFind(m_pdict, de->key);
                serverAssert(deLocal != nullptr);
                deLocal->v.s64++;
                m_collisionCount++;
                zfree(de);
            } else {
                int iht = dictIsRehashing(m_pdict) ? 1 : 0;
                de->next = m_pdict->ht[iht].table[index];
                m_pdict->ht[iht].table[index] = de;
                m_pdict->ht[iht].used++;
            }
        }
    }
    ul.unlock();

    m_spstorage->bulkInsert(rgkeys, rgcbkeys, rgvals, rgcbvals, celem);

    bulkInsertsInProgress--;
}

const StorageCache *StorageCache::clone()
{
    std::unique_lock<fastlock> ul(m_lock);
    // Clones never clone the cache
    StorageCache *cacheNew = new StorageCache(const_cast<IStorage*>(m_spstorage->clone()), false /*fCache*/);
    return cacheNew;
}

void StorageCache::expand(uint64_t slots)
{
    std::unique_lock<fastlock> ul(m_lock);
    if (m_pdict) {
        dictExpand(m_pdict, slots);
    }
}

void StorageCache::retrieve(sds key, IStorage::callbackSingle fn) const
{
    std::unique_lock<fastlock> ul(m_lock);
    if (m_pdict != nullptr)
    {
        uint64_t hash = dictSdsHash(key);
        dictEntry *de = dictFind(m_pdict, reinterpret_cast<void*>(hash));
        
        if (de == nullptr)
            return; // Not found
    }
    ul.unlock();
    m_spstorage->retrieve(key, sdslen(key), fn);
}

size_t StorageCache::count() const
{
    std::unique_lock<fastlock> ul(m_lock, std::defer_lock);
    bool fLocked = ul.try_lock();
    size_t count = m_spstorage->count();
    if (m_pdict != nullptr && fLocked) {
        serverAssert(bulkInsertsInProgress.load(std::memory_order_seq_cst) || count == (dictSize(m_pdict) + m_collisionCount));
    }
    return count;
}

void StorageCache::beginWriteBatch() { 
    serverAssert(GlobalLocksAcquired());    // Otherwise we deadlock
    m_spstorage->beginWriteBatch(); 
}

void StorageCache::emergencyFreeCache() {
    std::unique_lock<fastlock> ul(m_lock);
    dict *d = m_pdict;
    m_pdict = nullptr;
    if (d != nullptr) {
        g_pserver->asyncworkqueue->AddWorkFunction([d]{
            dictRelease(d);
        });
    }
}