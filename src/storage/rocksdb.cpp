#include "rocksdb.h"
#include <string>
#include <sstream>

RocksDBStorageProvider::RocksDBStorageProvider(std::shared_ptr<rocksdb::DB> &spdb, std::shared_ptr<rocksdb::ColumnFamilyHandle> &spcolfam, const rocksdb::Snapshot *psnapshot)
    : m_spdb(spdb), m_psnapshot(psnapshot), m_spcolfamily(spcolfam)
{
    m_readOptionsTemplate = rocksdb::ReadOptions();
    m_readOptionsTemplate.snapshot = m_psnapshot;
}

void RocksDBStorageProvider::insert(const char *key, size_t cchKey, void *data, size_t cb)
{
    rocksdb::Status status;
    if (m_spbatch != nullptr)
        status = m_spbatch->Put(m_spcolfamily.get(), rocksdb::Slice(key, cchKey), rocksdb::Slice((const char*)data, cb));
    else
        status = m_spdb->Put(WriteOptions(), m_spcolfamily.get(), rocksdb::Slice(key, cchKey), rocksdb::Slice((const char*)data, cb));
    if (!status.ok())
        throw status.ToString();
}

void RocksDBStorageProvider::erase(const char *key, size_t cchKey)
{
    rocksdb::Status status;
    if (m_spbatch != nullptr)
        status = m_spbatch->Delete(m_spcolfamily.get(), rocksdb::Slice(key, cchKey));
    else
        status = m_spdb->Delete(WriteOptions(), m_spcolfamily.get(), rocksdb::Slice(key, cchKey));
    if (!status.ok())
        throw status.ToString();
}

void RocksDBStorageProvider::retrieve(const char *key, size_t cchKey, callback fn) const
{
    std::string value;
    auto status = m_spdb->Get(ReadOptions(), m_spcolfamily.get(), rocksdb::Slice(key, cchKey), &value);
    if (status.ok())
        fn(key, cchKey, value.data(), value.size());
}

size_t RocksDBStorageProvider::clear()
{
    size_t celem = count();
    auto status = m_spdb->DropColumnFamily(m_spcolfamily.get());
    auto strName = m_spcolfamily->GetName();

    rocksdb::ColumnFamilyHandle *handle = nullptr;
    m_spdb->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), strName, &handle);
    m_spcolfamily = std::shared_ptr<rocksdb::ColumnFamilyHandle>(handle);

    if (!status.ok())
        throw status.ToString();
    return celem;
}

size_t RocksDBStorageProvider::count() const
{
    std::string strelem;
    if (!m_spdb->GetProperty(m_spcolfamily.get(), rocksdb::DB::Properties::kEstimateNumKeys, &strelem))
        throw "Failed to get database size";
    std::stringstream sstream(strelem);
    size_t count;
    sstream >> count;
    return count;
}

void RocksDBStorageProvider::enumerate(callback fn) const
{
    std::unique_ptr<rocksdb::Iterator> it = std::unique_ptr<rocksdb::Iterator>(m_spdb->NewIterator(ReadOptions(), m_spcolfamily.get()));
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        fn(it->key().data(), it->key().size(), it->value().data(), it->value().size());
    }
    assert(it->status().ok()); // Check for any errors found during the scan
}

const IStorage *RocksDBStorageProvider::clone() const
{
    const rocksdb::Snapshot *psnapshot = const_cast<RocksDBStorageProvider*>(this)->m_spdb->GetSnapshot();
    return new RocksDBStorageProvider(const_cast<RocksDBStorageProvider*>(this)->m_spdb, const_cast<RocksDBStorageProvider*>(this)->m_spcolfamily, psnapshot);
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

void RocksDBStorageProvider::flush()
{
    m_spdb->SyncWAL();
}