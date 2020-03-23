#pragma once

#include <memory>
#include "../IStorage.h"
#include <rocksdb/db.h>
#include "../fastlock.h"

#define INTERNAL_KEY_PREFIX "\x00\x04\x03\x00\x05\x02\x04"
static const char count_key[] = INTERNAL_KEY_PREFIX "__keydb__count\1";
static const char version_key[] = INTERNAL_KEY_PREFIX "__keydb__version\1";

class RocksDBStorageProvider : public IStorage
{
    std::shared_ptr<rocksdb::DB> m_spdb;    // Note: This must be first so it is deleted last
    std::unique_ptr<rocksdb::WriteBatch> m_spbatch;
    const rocksdb::Snapshot *m_psnapshot = nullptr;
    std::shared_ptr<rocksdb::ColumnFamilyHandle> m_spcolfamily;
    rocksdb::ReadOptions m_readOptionsTemplate;
    size_t m_count = 0;
    fastlock m_lock {"RocksDBStorageProvider"};

public:
    RocksDBStorageProvider(std::shared_ptr<rocksdb::DB> &spdb, std::shared_ptr<rocksdb::ColumnFamilyHandle> &spcolfam, const rocksdb::Snapshot *psnapshot, size_t count);
    ~RocksDBStorageProvider();

    virtual void insert(const char *key, size_t cchKey, void *data, size_t cb, bool fOverwrite) override;
    virtual bool erase(const char *key, size_t cchKey) override;
    virtual void retrieve(const char *key, size_t cchKey, callbackSingle fn) const override;
    virtual size_t clear() override;
    virtual bool enumerate(callback fn) const override;

    virtual const IStorage *clone() const override;

    virtual void beginWriteBatch() override;
    virtual void endWriteBatch() override;

    virtual void flush() override;

    size_t count() const;

protected:
    bool FKeyExists(const char *key, size_t cchKey) const;
    bool FInternalKey(const char *key, size_t cchKey) const;

    const rocksdb::ReadOptions &ReadOptions() const { return m_readOptionsTemplate; }
    rocksdb::WriteOptions WriteOptions() const;
};