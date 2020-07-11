#pragma once
#include "sds.h"

class StorageCache
{
    std::shared_ptr<IStorage> m_spstorage;
    std::unique_ptr<semiorderedset<sdsimmutablestring, sdsview, true>> m_setkeys;

    StorageCache(IStorage *storage)
        : m_spstorage(storage)
    {}

    void cacheKey(sds key);
    void cacheKey(const char *rgchKey, size_t cchKey);

    struct load_iter_data
    {
        StorageCache *cache;
        IStorageFactory::key_load_iterator itrParent;
        void *privdataParent;
    };
    static void key_load_itr(const char *rgchKey, size_t cchKey, void *privdata)
    {
        load_iter_data *data = (load_iter_data*)privdata;
        data->cache->cacheKey(rgchKey, cchKey);
        if (data->itrParent)
            data->itrParent(rgchKey, cchKey, data->privdataParent);
    }

public:
    static StorageCache *create(IStorageFactory *pfactory, int db, IStorageFactory::key_load_iterator fn, void *privdata) {
        StorageCache *cache = new StorageCache(nullptr);
        if (pfactory->FSlow())
        {
            cache->m_setkeys = std::make_unique<semiorderedset<sdsimmutablestring, sdsview, true>>();
        }
        load_iter_data data = {cache, fn, privdata};
        cache->m_spstorage = std::shared_ptr<IStorage>(pfactory->create(db, key_load_itr, (void*)&data));
        return cache;
    }

    void clear();
    void insert(sds key, const void *data, size_t cbdata, bool fOverwrite);
    void retrieve(sds key, IStorage::callbackSingle fn, sds *sharedKeyOut) const;
    bool erase(sds key);

    bool enumerate(IStorage::callback fn) const { return m_spstorage->enumerate(fn); }

    void beginWriteBatch();
    void endWriteBatch() { m_spstorage->endWriteBatch(); }
    void batch_lock() { return m_spstorage->batch_lock(); }
    void batch_unlock() { return m_spstorage->batch_unlock(); }

    size_t count() const;

    const StorageCache *clone();
};