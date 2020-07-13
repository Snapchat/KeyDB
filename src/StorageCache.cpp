#include "server.h"

void StorageCache::clear()
{
    if (m_setkeys != nullptr)
        m_setkeys->clear();
    m_spstorage->clear();
}

void StorageCache::cacheKey(sds key)
{
    if (m_setkeys == nullptr)
        return;
    m_setkeys->insert(sdsimmutablestring(sdsdupshared(key)));
}

void StorageCache::cacheKey(const char *rgch, size_t cch)
{
    if (m_setkeys == nullptr)
        return;
    m_setkeys->insert(sdsimmutablestring(sdsnewlen(rgch, cch)));
}

bool StorageCache::erase(sds key)
{
    bool result = m_spstorage->erase(key, sdslen(key));
    if (result && m_setkeys != nullptr)
    {
        auto itr = m_setkeys->find(sdsview(key));
        serverAssert(itr != m_setkeys->end());
        m_setkeys->erase(itr);
    }
    return result;
}

void StorageCache::insert(sds key, const void *data, size_t cbdata, bool fOverwrite)
{
    if (!fOverwrite && m_setkeys != nullptr)
    {
        cacheKey(key);
    }
    m_spstorage->insert(key, sdslen(key), (void*)data, cbdata, fOverwrite);
}

const StorageCache *StorageCache::clone()
{
    // Clones never clone the cache
    StorageCache *cacheNew = new StorageCache(const_cast<IStorage*>(m_spstorage->clone()));
    return cacheNew;
}

void StorageCache::retrieve(sds key, IStorage::callbackSingle fn, sds *cachedKey) const
{
    if (m_setkeys != nullptr)
    {
        auto itr = m_setkeys->find(sdsview(key));
        if (itr == m_setkeys->end())
            return; // Not found
        if (cachedKey != nullptr)
            *cachedKey = sdsdupshared(itr->get());
    }
    m_spstorage->retrieve(key, sdslen(key), fn);
}

size_t StorageCache::count() const
{
    size_t count = m_spstorage->count();
    if (m_setkeys != nullptr)
        serverAssert(count == m_setkeys->size());
    return count;
}

void StorageCache::beginWriteBatch() { 
    serverAssert(GlobalLocksAcquired());    // Otherwise we deadlock
    m_spstorage->beginWriteBatch(); 
}