#pragma once

#include <memory>
#include "../IStorage.h"
#include <rocksdb/db.h>
#include <rocksdb/utilities/write_batch_with_index.h>
#include "../fastlock.h"

#define INTERNAL_KEY_PREFIX "\x00\x04\x03\x00\x05\x02\x04"
static const char count_key[] = INTERNAL_KEY_PREFIX "__keydb__count\1";
static const char version_key[] = INTERNAL_KEY_PREFIX "__keydb__version\1";
static const char meta_key[] = INTERNAL_KEY_PREFIX "__keydb__metadata\1";
class RocksDBStorageFactory;

class RocksDBStorageProvider : public IStorage
{
    RocksDBStorageFactory *m_pfactory;
    std::shared_ptr<rocksdb::DB> m_spdb;    // Note: This must be first so it is deleted last
    std::unique_ptr<rocksdb::WriteBatchWithIndex> m_spbatch;
    const rocksdb::Snapshot *m_psnapshot = nullptr;
    std::shared_ptr<rocksdb::ColumnFamilyHandle> m_spcolfamily;
    rocksdb::ReadOptions m_readOptionsTemplate;
    size_t m_count = 0;
    mutable fastlock m_lock {"RocksDBStorageProvider"};

public:
    RocksDBStorageProvider(RocksDBStorageFactory *pfactory, std::shared_ptr<rocksdb::DB> &spdb, std::shared_ptr<rocksdb::ColumnFamilyHandle> &spcolfam, const rocksdb::Snapshot *psnapshot, size_t count);
    ~RocksDBStorageProvider();

    virtual void insert(const char *key, size_t cchKey, void *data, size_t cb, bool fOverwrite) override;
    virtual bool erase(const char *key, size_t cchKey) override;
    virtual void retrieve(const char *key, size_t cchKey, callbackSingle fn) const override;
    virtual size_t clear() override;
    virtual bool enumerate(callback fn) const override;
    virtual bool enumerate_hashslot(callback fn, unsigned int hashslot) const override;

    virtual const IStorage *clone() const override;

    virtual void beginWriteBatch() override;
    virtual void endWriteBatch() override;

    virtual void bulkInsert(char **rgkeys, size_t *rgcbkeys, char **rgvals, size_t *rgcbvals, char *rgfOverwrite, size_t celem) override;

    virtual void batch_lock() override;
    virtual void batch_unlock() override;

    virtual void flush() override;

    size_t count() const override;

protected:
    bool FKeyExists(std::string&) const;

    const rocksdb::ReadOptions &ReadOptions() const { return m_readOptionsTemplate; }
    rocksdb::WriteOptions WriteOptions() const;
};

bool FInternalKey(const char *key, size_t cch);