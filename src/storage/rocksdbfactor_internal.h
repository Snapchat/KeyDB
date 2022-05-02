#pragma once
#include "rocksdb.h"

class RocksDBStorageFactory : public IStorageFactory
{
    std::shared_ptr<rocksdb::DB> m_spdb;    // Note: This must be first so it is deleted last
    std::vector<std::unique_ptr<rocksdb::ColumnFamilyHandle>> m_vecspcols;
    std::shared_ptr<rocksdb::SstFileManager> m_pfilemanager;
    std::string m_path;
    bool m_fCreatedTempFolder = false;

public:
    RocksDBStorageFactory(const char *dbfile, int dbnum, const char *rgchConfig, size_t cchConfig);
    ~RocksDBStorageFactory();

    virtual IStorage *create(int db, key_load_iterator iter, void *privdata) override;
    virtual IStorage *createMetadataDb() override;
    virtual const char *name() const override;

    virtual size_t totalDiskspaceUsed() const override;

    virtual bool FSlow() const override { return true; }

    virtual size_t filedsRequired() const override;
    std::string getTempFolder();

    rocksdb::Options RocksDbOptions();

private:
    void setVersion(rocksdb::ColumnFamilyHandle*);
};