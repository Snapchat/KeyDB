#include "rocksdb.h"
#include <string>
#include <sstream>
#include <mutex>

static const char *keyprefix = INTERNAL_KEY_PREFIX;

bool FInternalKey(const char *key, size_t cch)
{
    if (cch > strlen(INTERNAL_KEY_PREFIX))
    {
        if (memcmp(key, keyprefix, strlen(INTERNAL_KEY_PREFIX)) == 0)
            return true;
    }
    return false;
}

RocksDBStorageProvider::RocksDBStorageProvider(std::shared_ptr<rocksdb::DB> &spdb, std::shared_ptr<rocksdb::ColumnFamilyHandle> &spcolfam, const rocksdb::Snapshot *psnapshot, size_t count)
    : m_spdb(spdb), m_psnapshot(psnapshot), m_spcolfamily(spcolfam), m_count(count)
{
    m_readOptionsTemplate = rocksdb::ReadOptions();
    m_readOptionsTemplate.snapshot = m_psnapshot;
}

void RocksDBStorageProvider::insert(const char *key, size_t cchKey, void *data, size_t cb, bool fOverwrite)
{
    rocksdb::Status status;
    std::unique_lock<fastlock> l(m_lock);
    if (m_spbatch != nullptr)
        status = m_spbatch->Put(m_spcolfamily.get(), rocksdb::Slice(key, cchKey), rocksdb::Slice((const char*)data, cb));
    else
        status = m_spdb->Put(WriteOptions(), m_spcolfamily.get(), rocksdb::Slice(key, cchKey), rocksdb::Slice((const char*)data, cb));
    if (!status.ok())
        throw status.ToString();

    if (!fOverwrite)
        ++m_count;
}

bool RocksDBStorageProvider::erase(const char *key, size_t cchKey)
{
    rocksdb::Status status;
    std::unique_lock<fastlock> l(m_lock);
    if (m_spbatch != nullptr)
    {
        status = m_spbatch->Delete(m_spcolfamily.get(), rocksdb::Slice(key, cchKey));
    }
    else
    {
        if (!FKeyExists(key, cchKey))
            return false;
        status = m_spdb->Delete(WriteOptions(), m_spcolfamily.get(), rocksdb::Slice(key, cchKey));
    }
    if (status.ok())
        --m_count;
    return status.ok();
}

void RocksDBStorageProvider::retrieve(const char *key, size_t cchKey, callbackSingle fn) const
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
    m_count = 0;
    return celem;
}

size_t RocksDBStorageProvider::count() const
{
    return m_count;
}

bool RocksDBStorageProvider::enumerate(callback fn) const
{
    std::unique_ptr<rocksdb::Iterator> it = std::unique_ptr<rocksdb::Iterator>(m_spdb->NewIterator(ReadOptions(), m_spcolfamily.get()));
    size_t count = 0;
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        if (FInternalKey(it->key().data(), it->key().size()))
            continue;
        ++count;
        bool fContinue = fn(it->key().data(), it->key().size(), it->value().data(), it->value().size());
        if (!fContinue)
            break;
    }
    if (!it->Valid() && count != m_count)
    {
        const_cast<RocksDBStorageProvider*>(this)->m_count = count;    // BUG!!! but be resilient
    }
    assert(it->status().ok()); // Check for any errors found during the scan
    return !it->Valid();
}

const IStorage *RocksDBStorageProvider::clone() const
{
    const rocksdb::Snapshot *psnapshot = const_cast<RocksDBStorageProvider*>(this)->m_spdb->GetSnapshot();
    return new RocksDBStorageProvider(const_cast<RocksDBStorageProvider*>(this)->m_spdb, const_cast<RocksDBStorageProvider*>(this)->m_spcolfamily, psnapshot, m_count);
}

RocksDBStorageProvider::~RocksDBStorageProvider()
{
    if (m_spbatch != nullptr)
        endWriteBatch();
    
    if (m_spdb != nullptr && m_psnapshot == nullptr)
    {
        insert(count_key, sizeof(count_key), &m_count, sizeof(m_count), false);
        flush();
    }

    if (m_spdb != nullptr)
    {
        if (m_psnapshot != nullptr)
            m_spdb->ReleaseSnapshot(m_psnapshot);
    }
}

rocksdb::WriteOptions RocksDBStorageProvider::WriteOptions() const
{
    auto opt = rocksdb::WriteOptions();
    return opt;
}

void RocksDBStorageProvider::beginWriteBatch()
{
    m_lock.lock();
    m_spbatch = std::make_unique<rocksdb::WriteBatch>();
}

void RocksDBStorageProvider::endWriteBatch()
{
    m_spdb->Write(WriteOptions(), m_spbatch.get());
    m_spbatch = nullptr;
    m_lock.unlock();
}

void RocksDBStorageProvider::flush()
{
    m_spdb->SyncWAL();
}

bool RocksDBStorageProvider::FKeyExists(const char *key, size_t cch) const
{
    std::string strT;
    return m_spdb->Get(ReadOptions(), m_spcolfamily.get(), rocksdb::Slice(key, cch), &strT).ok();
}