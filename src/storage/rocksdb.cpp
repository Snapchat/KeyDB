#include "../IStorage.h"
#include <rocksdb/db.h>
#include <string>
#include <sstream>

class RocksDBStorageProvider : public IStorage
{
    std::shared_ptr<rocksdb::DB> m_spdb;
    std::unique_ptr<rocksdb::WriteBatch> m_spbatch;
    const rocksdb::Snapshot *m_psnapshot = nullptr;
    rocksdb::ReadOptions m_readOptionsTemplate;

public:
    RocksDBStorageProvider(const char *path);
    ~RocksDBStorageProvider();

    virtual void insert(const char *key, size_t cchKey, void *data, size_t cb) override;
    virtual void erase(const char *key, size_t cchKey) override;
    virtual void retrieve(const char *key, size_t cchKey, callback fn) const override;
    virtual size_t clear() override;
    virtual void enumerate(callback fn) const override;

    virtual const IStorage *clone() const override;

    virtual void beginWriteBatch() override;
    virtual void endWriteBatch() override;

    size_t count() const;

protected:
    RocksDBStorageProvider(std::shared_ptr<rocksdb::DB> &spdb);

    const rocksdb::ReadOptions &ReadOptions() const { return m_readOptionsTemplate; }
    rocksdb::WriteOptions WriteOptions() const;
};

IStorage *create_rocksdb_storage(const char *dbfile)
{
    return new RocksDBStorageProvider(dbfile);
}

RocksDBStorageProvider::RocksDBStorageProvider(const char *path)
{
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::DB *db = nullptr;
    auto status = rocksdb::DB::Open(options, path, &db);
    if (!status.ok())
        throw status.ToString();
    m_spdb = std::shared_ptr<rocksdb::DB>(db);

    m_readOptionsTemplate = rocksdb::ReadOptions();
}

RocksDBStorageProvider::RocksDBStorageProvider(std::shared_ptr<rocksdb::DB> &spdb)
    : m_spdb(spdb)
{
    m_readOptionsTemplate = rocksdb::ReadOptions();
    m_psnapshot = spdb->GetSnapshot();
    m_readOptionsTemplate.snapshot = m_psnapshot;
}

void RocksDBStorageProvider::insert(const char *key, size_t cchKey, void *data, size_t cb)
{
    rocksdb::Status status;
    if (m_spbatch != nullptr)
        status = m_spbatch->Put(rocksdb::Slice(key, cchKey), rocksdb::Slice((const char*)data, cb));
    else
        status = m_spdb->Put(WriteOptions(), rocksdb::Slice(key, cchKey), rocksdb::Slice((const char*)data, cb));
    if (!status.ok())
        throw status.ToString();
}

void RocksDBStorageProvider::erase(const char *key, size_t cchKey)
{
    rocksdb::Status status;
    if (m_spbatch != nullptr)
        status = m_spbatch->Delete(rocksdb::Slice(key, cchKey));
    else
        status = m_spdb->Delete(WriteOptions(), rocksdb::Slice(key, cchKey));
    if (!status.ok())
        throw status.ToString();
}

void RocksDBStorageProvider::retrieve(const char *key, size_t cchKey, callback fn) const
{
    std::string value;
    auto status = m_spdb->Get(ReadOptions(), rocksdb::Slice(key, cchKey), &value);
    if (!status.ok())
        throw status.ToString();
    fn(key, cchKey, value.data(), value.size());
}

size_t RocksDBStorageProvider::clear()
{
    size_t celem = count();
    auto status = m_spdb->DropColumnFamily(m_spdb->DefaultColumnFamily());
    if (!status.ok())
        throw status.ToString();
    return celem;
}

size_t RocksDBStorageProvider::count() const
{
    std::string strelem;
    if (!m_spdb->GetProperty(rocksdb::DB::Properties::kEstimateNumKeys, &strelem))
        throw "Failed to get database size";
    std::stringstream sstream(strelem);
    size_t count;
    sstream >> count;
    return count;
}

void RocksDBStorageProvider::enumerate(callback fn) const
{
    std::unique_ptr<rocksdb::Iterator> it = std::unique_ptr<rocksdb::Iterator>(m_spdb->NewIterator(ReadOptions()));
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        fn(it->key().data(), it->key().size(), it->value().data(), it->value().size());
    }
    assert(it->status().ok()); // Check for any errors found during the scan
}

const IStorage *RocksDBStorageProvider::clone() const
{
    return new RocksDBStorageProvider(const_cast<RocksDBStorageProvider*>(this)->m_spdb);
}

RocksDBStorageProvider::~RocksDBStorageProvider()
{
    if (m_spdb != nullptr)
    {
        if (m_psnapshot != nullptr)
            m_spdb->ReleaseSnapshot(m_psnapshot);
    }
}

rocksdb::WriteOptions RocksDBStorageProvider::WriteOptions() const
{
    auto opt = rocksdb::WriteOptions();
    opt.disableWAL = true;
    return opt;
}

void RocksDBStorageProvider::beginWriteBatch()
{
    m_spbatch = std::make_unique<rocksdb::WriteBatch>();
}

void RocksDBStorageProvider::endWriteBatch()
{
    m_spdb->Write(WriteOptions(), m_spbatch.get());
    m_spbatch = nullptr;
}