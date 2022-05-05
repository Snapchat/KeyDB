#include "rocksdb.h"
#include <string>
#include <sstream>
#include <mutex>
#include <unistd.h>
#include "rocksdbfactor_internal.h"

static const char keyprefix[] = INTERNAL_KEY_PREFIX;

rocksdb::Options DefaultRocksDBOptions();
extern "C" pid_t gettid();

bool FInternalKey(const char *key, size_t cch)
{
    if (cch >= sizeof(INTERNAL_KEY_PREFIX))
    {
        if (memcmp(key, keyprefix, sizeof(INTERNAL_KEY_PREFIX)-1) == 0)
            return true;
    }
    return false;
}

RocksDBStorageProvider::RocksDBStorageProvider(RocksDBStorageFactory *pfactory, std::shared_ptr<rocksdb::DB> &spdb, std::shared_ptr<rocksdb::ColumnFamilyHandle> &spcolfam, const rocksdb::Snapshot *psnapshot, size_t count)
    : m_pfactory(pfactory), m_spdb(spdb), m_psnapshot(psnapshot), m_spcolfamily(spcolfam), m_count(count)
{
    m_readOptionsTemplate = rocksdb::ReadOptions();
    m_readOptionsTemplate.verify_checksums = false;
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

void RocksDBStorageProvider::bulkInsert(char **rgkeys, size_t *rgcbkeys, char **rgvals, size_t *rgcbvals, size_t celem)
{
    if (celem >= 16384) {
        rocksdb::Options options = DefaultRocksDBOptions();
        rocksdb::SstFileWriter sst_file_writer(rocksdb::EnvOptions(), options, options.comparator);
        std::string file_path = m_pfactory->getTempFolder() + "/tmpIngest.";
        file_path += std::to_string(gettid());
        file_path += ".sst";

        rocksdb::Status s = sst_file_writer.Open(file_path);
        if (!s.ok())
            goto LFallback;

        // Insert rows into the SST file, note that inserted keys must be 
        // strictly increasing (based on options.comparator)
        for (size_t ielem = 0; ielem < celem; ++ielem) {
            s = sst_file_writer.Put(rocksdb::Slice(rgkeys[ielem], rgcbkeys[ielem]), rocksdb::Slice(rgvals[ielem], rgcbvals[ielem]));
            if (!s.ok()) {
                unlink(file_path.c_str());
                goto LFallback;
            }
        }

        s = sst_file_writer.Finish();
        if (!s.ok()) {
            unlink(file_path.c_str());
            goto LFallback;
        }

        auto ingestOptions = rocksdb::IngestExternalFileOptions();
        ingestOptions.move_files = true;
        ingestOptions.write_global_seqno = false;
        ingestOptions.failed_move_fall_back_to_copy = false;

        // Ingest the external SST file into the DB
        s = m_spdb->IngestExternalFile(m_spcolfamily.get(), {file_path}, ingestOptions);
        if (!s.ok()) {
            unlink(file_path.c_str());
            goto LFallback;
        }
    } else {
    LFallback:
        auto spbatch = std::make_unique<rocksdb::WriteBatch>();
        for (size_t ielem = 0; ielem < celem; ++ielem) {
            spbatch->Put(m_spcolfamily.get(), rocksdb::Slice(rgkeys[ielem], rgcbkeys[ielem]), rocksdb::Slice(rgvals[ielem], rgcbvals[ielem]));
        }
        m_spdb->Write(WriteOptions(), spbatch.get());
    }

    std::unique_lock<fastlock> l(m_lock);
    m_count += celem;
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
    rocksdb::PinnableSlice slice;
    auto status = m_spdb->Get(ReadOptions(), m_spcolfamily.get(), rocksdb::Slice(key, cchKey), &slice);
    if (status.ok())
        fn(key, cchKey, slice.data(), slice.size());
}

size_t RocksDBStorageProvider::clear()
{
    size_t celem = count();
    auto status = m_spdb->DropColumnFamily(m_spcolfamily.get());
    auto strName = m_spcolfamily->GetName();

    rocksdb::ColumnFamilyHandle *handle = nullptr;
    rocksdb::ColumnFamilyOptions cf_options(m_pfactory->RocksDbOptions());
    m_spdb->CreateColumnFamily(cf_options, strName, &handle);
    m_spcolfamily = std::shared_ptr<rocksdb::ColumnFamilyHandle>(handle);

    if (!status.ok())
        throw status.ToString();
    m_count = 0;
    return celem;
}

size_t RocksDBStorageProvider::count() const
{
    std::unique_lock<fastlock> l(m_lock);
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
        if (const_cast<RocksDBStorageProvider*>(this)->m_count != count)
            printf("WARNING: rocksdb count mismatch");
        const_cast<RocksDBStorageProvider*>(this)->m_count = count;
    }
    assert(it->status().ok()); // Check for any errors found during the scan
    return !it->Valid();
}

const IStorage *RocksDBStorageProvider::clone() const
{
    std::unique_lock<fastlock> l(m_lock);
    const rocksdb::Snapshot *psnapshot = const_cast<RocksDBStorageProvider*>(this)->m_spdb->GetSnapshot();
    return new RocksDBStorageProvider(m_pfactory, const_cast<RocksDBStorageProvider*>(this)->m_spdb, const_cast<RocksDBStorageProvider*>(this)->m_spcolfamily, psnapshot, m_count);
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

void RocksDBStorageProvider::batch_lock()
{
    m_lock.lock();
}

void RocksDBStorageProvider::batch_unlock()
{
    m_lock.unlock();
}

void RocksDBStorageProvider::flush()
{
    m_spdb->SyncWAL();
}

bool RocksDBStorageProvider::FKeyExists(const char *key, size_t cch) const
{
    rocksdb::PinnableSlice slice;
    return m_spdb->Get(ReadOptions(), m_spcolfamily.get(), rocksdb::Slice(key, cch), &slice).ok();
}