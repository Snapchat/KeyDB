#pragma once
#include <functional>
#include <unordered_set>
#include "sds.h"
#include "ae.h"

#define METADATA_DB_IDENTIFIER "c299fde0-6d42-4ec4-b939-34f680ffe39f"

struct StorageToken {
    std::unordered_set<struct client *> setc;
    struct redisDbPersistentData *db;
    virtual ~StorageToken() {}
};

class IStorageFactory
{
public:
    typedef void (*key_load_iterator)(const char *rgchKey, size_t cchKey, void *privdata);

    virtual ~IStorageFactory() {}
    virtual class IStorage *create(int db, key_load_iterator itr, void *privdata) = 0;
    virtual class IStorage *createMetadataDb() = 0;
    virtual const char *name() const = 0;
    virtual size_t totalDiskspaceUsed() const = 0;
    virtual sdsstring getInfo() const = 0;
    virtual bool FSlow() const = 0;
    virtual size_t filedsRequired() const { return 0; }
};

class IStorage
{
public:
    typedef std::function<bool(const char *, size_t, const void *, size_t)> callback;
    typedef std::function<void(const char *, size_t, const void *, size_t)> callbackSingle;

    virtual ~IStorage();

    virtual void insert(const char *key, size_t cchKey, void *data, size_t cb, bool fOverwire) = 0;
    virtual bool erase(const char *key, size_t cchKey) = 0;
    virtual void retrieve(const char *key, size_t cchKey, callbackSingle fn) const = 0;
    virtual size_t clear() = 0;
    virtual bool enumerate(callback fn) const = 0;
    virtual bool enumerate_hashslot(callback fn, unsigned int hashslot) const = 0;
    virtual size_t count() const = 0;

    virtual StorageToken *begin_retrieve(struct aeEventLoop *, aePostFunctionTokenProc, sds *, size_t) {return nullptr;};
    virtual void complete_retrieve(StorageToken * /*tok*/, callbackSingle /*fn*/) {};

    virtual void bulkInsert(char **rgkeys, size_t *rgcbkeys, char **rgvals, size_t *rgcbvals, size_t celem) {
        beginWriteBatch();
        for (size_t ielem = 0; ielem < celem; ++ielem) {
            insert(rgkeys[ielem], rgcbkeys[ielem], rgvals[ielem], rgcbvals[ielem], false);
        }
        endWriteBatch();
    }

    virtual void beginWriteBatch() {} // NOP
    virtual void endWriteBatch() {} // NOP

    virtual void batch_lock() {} // NOP
    virtual void batch_unlock() {} // NOP

    virtual void flush() = 0;

    /* This is permitted to be a shallow clone */
    virtual const IStorage *clone() const = 0;
};
