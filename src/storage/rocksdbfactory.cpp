#include "rocksdb.h"
#include "../version.h"
#include <rocksdb/filter_policy.h>
#include <rocksdb/table.h>
#include <rocksdb/utilities/options_util.h>
#include <rocksdb/sst_file_manager.h>
#include <rocksdb/utilities/convenience.h>
#include <rocksdb/slice_transform.h>
#include "rocksdbfactor_internal.h"
#include <sys/types.h>
#include <sys/stat.h> 

rocksdb::Options DefaultRocksDBOptions() {
    rocksdb::Options options;
    options.max_background_compactions = 4;
    options.max_background_flushes = 2;
    options.bytes_per_sync = 1048576;
    options.compaction_pri = rocksdb::kMinOverlappingRatio;
    options.compression = rocksdb::kNoCompression;
    options.enable_pipelined_write = true;
    options.allow_mmap_reads = true;
    options.avoid_unnecessary_blocking_io = true;
    options.prefix_extractor.reset(rocksdb::NewFixedPrefixTransform(0));

    rocksdb::BlockBasedTableOptions table_options;
    table_options.block_size = 16 * 1024;
    table_options.cache_index_and_filter_blocks = true;
    table_options.pin_l0_filter_and_index_blocks_in_cache = true;
    table_options.data_block_index_type = rocksdb::BlockBasedTableOptions::kDataBlockBinaryAndHash;
    table_options.checksum = rocksdb::kNoChecksum;
    table_options.format_version = 4;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    
    return options;
}

IStorageFactory *CreateRocksDBStorageFactory(const char *path, int dbnum, const char *rgchConfig, size_t cchConfig)
{
    return new RocksDBStorageFactory(path, dbnum, rgchConfig, cchConfig);
}

rocksdb::Options RocksDBStorageFactory::RocksDbOptions()
{
    rocksdb::Options options = DefaultRocksDBOptions();
    options.max_open_files = filedsRequired();
    options.sst_file_manager = m_pfilemanager;
    options.create_if_missing = true;
    options.create_missing_column_families = true;
    options.info_log_level = rocksdb::ERROR_LEVEL;
    options.max_total_wal_size = 1 * 1024 * 1024 * 1024;
    return options;
}

RocksDBStorageFactory::RocksDBStorageFactory(const char *dbfile, int dbnum, const char *rgchConfig, size_t cchConfig)
    : m_path(dbfile)
{
    dbnum++; // create an extra db for metadata
    // Get the count of column families in the actual database
    std::vector<std::string> vecT;
    auto status = rocksdb::DB::ListColumnFamilies(rocksdb::Options(), dbfile, &vecT);
    // RocksDB requires we know the count of col families before opening, if the user only wants to see less
    //  we still have to make room for all column family handles regardless
    if (status.ok() && (int)vecT.size() > dbnum)
        dbnum = (int)vecT.size();

    std::vector<rocksdb::ColumnFamilyDescriptor> veccoldesc;
    veccoldesc.push_back(rocksdb::ColumnFamilyDescriptor(rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions()));  // ignore default col family

    m_pfilemanager = std::shared_ptr<rocksdb::SstFileManager>(rocksdb::NewSstFileManager(rocksdb::Env::Default()));

    rocksdb::DB *db = nullptr;

    auto options = RocksDbOptions();

    for (int idb = 0; idb < dbnum; ++idb)
    {
        rocksdb::ColumnFamilyOptions cf_options(options);
        cf_options.level_compaction_dynamic_level_bytes = true;
        veccoldesc.push_back(rocksdb::ColumnFamilyDescriptor(std::to_string(idb), cf_options));
    }

    if (rgchConfig != nullptr)
    {
        std::string options_string(rgchConfig, cchConfig);
        rocksdb::Status status;
        if (!(status = rocksdb::GetDBOptionsFromString(options, options_string, &options)).ok())
        {
            fprintf(stderr, "Failed to parse FLASH options: %s\r\n", status.ToString().c_str());
            exit(EXIT_FAILURE);
        }
    }
    
    std::vector<rocksdb::ColumnFamilyHandle*> handles;
    status = rocksdb::DB::Open(options, dbfile, veccoldesc, &handles, &db);
    if (!status.ok())
        throw status.ToString();

    m_spdb = std::shared_ptr<rocksdb::DB>(db);
    for (auto handle : handles)
    {
        std::string strVersion;
        auto status = m_spdb->Get(rocksdb::ReadOptions(), handle, rocksdb::Slice(version_key, sizeof(version_key)), &strVersion);
        if (!status.ok())
        {
            setVersion(handle);
        }
        else
        {
            SymVer ver = parseVersion(strVersion.c_str());
            auto cmp = compareVersion(&ver);
            if (cmp == NewerVersion)
                throw "Cannot load FLASH database created by newer version of KeyDB";
            if (cmp == OlderVersion)
                setVersion(handle);
        }
        m_vecspcols.emplace_back(handle);
    }   
}

RocksDBStorageFactory::~RocksDBStorageFactory()
{
    m_spdb->SyncWAL();
}

void RocksDBStorageFactory::setVersion(rocksdb::ColumnFamilyHandle *handle)
{
    auto status = m_spdb->Put(rocksdb::WriteOptions(), handle, rocksdb::Slice(version_key, sizeof(version_key)), rocksdb::Slice(KEYDB_REAL_VERSION, strlen(KEYDB_REAL_VERSION)+1));
    if (!status.ok())
        throw status.ToString();
}

size_t RocksDBStorageFactory::filedsRequired() const {
    return 256;
}

std::string RocksDBStorageFactory::getTempFolder()
{
    auto path = m_path + "/keydb_tmp/";
    if (!m_fCreatedTempFolder) {
        if (!mkdir(path.c_str(), 0700))
            m_fCreatedTempFolder = true;
    }
    return path;
}

IStorage *RocksDBStorageFactory::createMetadataDb()
{
    IStorage *metadataDb = this->create(-1, nullptr, nullptr);
    metadataDb->insert(meta_key, sizeof(meta_key), (void*)METADATA_DB_IDENTIFIER, strlen(METADATA_DB_IDENTIFIER), false);
    return metadataDb;
}

IStorage *RocksDBStorageFactory::create(int db, key_load_iterator iter, void *privdata)
{
    ++db;   // skip default col family
    std::shared_ptr<rocksdb::ColumnFamilyHandle> spcolfamily(m_vecspcols[db].release());
    size_t count = 0;
    bool fUnclean = false;
    
    std::string value;
    auto status = m_spdb->Get(rocksdb::ReadOptions(), spcolfamily.get(), rocksdb::Slice(count_key, sizeof(count_key)), &value);
    if (status.ok() && value.size() == sizeof(size_t))
    {
        count = *reinterpret_cast<const size_t*>(value.data());
        m_spdb->Delete(rocksdb::WriteOptions(), spcolfamily.get(), rocksdb::Slice(count_key, sizeof(count_key)));
    }
    else
    {
        fUnclean = true;
    }
    
    if (fUnclean || iter != nullptr)
    {
        count = 0;
        auto opts = rocksdb::ReadOptions();
        opts.tailing = true;
        std::unique_ptr<rocksdb::Iterator> it = std::unique_ptr<rocksdb::Iterator>(m_spdb->NewIterator(opts, spcolfamily.get()));

        it->SeekToFirst();
        bool fFirstRealKey = true;
        
        for (;it->Valid(); it->Next()) {
            if (FInternalKey(it->key().data(), it->key().size()))
                continue;
            if (fUnclean && it->Valid() && fFirstRealKey)
                printf("\tDatabase %d was not shutdown cleanly, recomputing metrics\n", db);
            fFirstRealKey = false;
            if (iter != nullptr)
                iter(it->key().data(), it->key().size(), privdata);
            ++count;
        }
    }
    return new RocksDBStorageProvider(this, m_spdb, spcolfamily, nullptr, count);
}

const char *RocksDBStorageFactory::name() const
{
    return "flash";
}

size_t RocksDBStorageFactory::totalDiskspaceUsed() const
{
    return m_pfilemanager->GetTotalSize();
}
