#pragma once
#include "sds.h"

class StorageCache
{
    std::shared_ptr<IStorage> m_spstorage;
    dict *m_pdict = nullptr;
    int m_collisionCount = 0;
    mutable fastlock m_lock {"StorageCache"};
    std::atomic<int> bulkInsertsInProgress;

    StorageCache(IStorage *storage, bool fNoCache);

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
    ~StorageCache();

    static StorageCache *create(IStorageFactory *pfactory, int db, IStorageFactory::key_load_iterator fn, void *privdata) {
        StorageCache *cache = new StorageCache(nullptr, pfactory->FSlow() /*fCache*/);
        load_iter_data data = {cache, fn, privdata};
        cache->m_spstorage = std::shared_ptr<IStorage>(pfactory->create(db, key_load_itr, (void*)&data));
        return cache;
    }

    void clear(void(callback)(void*));
    void clearAsync();
    void insert(sds key, const void *data, size_t cbdata, bool fOverwrite);
    void bulkInsert(char **rgkeys, size_t *rgcbkeys, char **rgvals, size_t *rgcbvals, size_t celem);
    void retrieve(sds key, IStorage::callbackSingle fn) const;
    bool erase(sds key);
    void emergencyFreeCache();
    bool keycacheIsEnabled() const { return m_pdict != nullptr; }
    void expand(uint64_t slots);

    bool enumerate(IStorage::callback fn) const { return m_spstorage->enumerate(fn); }

    void beginWriteBatch();
    void endWriteBatch() { m_spstorage->endWriteBatch(); }
    void batch_lock() { return m_spstorage->batch_lock(); }
    void batch_unlock() { return m_spstorage->batch_unlock(); }

    size_t count() const;

    const StorageCache *clone();
};
