#include "rocksdb.h"
#include "../version.h"
#include <rocksdb/filter_policy.h>
#include <rocksdb/table.h>
#include <rocksdb/utilities/options_util.h>

class RocksDBStorageFactory : public IStorageFactory
{
    std::shared_ptr<rocksdb::DB> m_spdb;    // Note: This must be first so it is deleted last
    std::vector<std::unique_ptr<rocksdb::ColumnFamilyHandle>> m_vecspcols;

public:
    RocksDBStorageFactory(const char *dbfile, int dbnum);
    ~RocksDBStorageFactory();

    virtual IStorage *create(int db, key_load_iterator iter) override;
    virtual const char *name() const override;

private:
    void setVersion(rocksdb::ColumnFamilyHandle*);
};

IStorageFactory *CreateRocksDBStorageFactory(const char *path, int dbnum)
{
    return new RocksDBStorageFactory(path, dbnum);
}

RocksDBStorageFactory::RocksDBStorageFactory(const char *dbfile, int dbnum)
{
    // Get the count of column families in the actual database
    std::vector<std::string> vecT;
    auto status = rocksdb::DB::ListColumnFamilies(rocksdb::Options(), dbfile, &vecT);
    // RocksDB requires we know the count of col families before opening, if the user only wants to see less
    //  we still have to make room for all column family handles regardless
    if (status.ok() && (int)vecT.size() > dbnum)
        dbnum = (int)vecT.size();

    std::vector<rocksdb::ColumnFamilyDescriptor> veccoldesc;
    veccoldesc.push_back(rocksdb::ColumnFamilyDescriptor(rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions()));  // ignore default col family

    rocksdb::Options options;
    options.create_if_missing = true;
    options.create_missing_column_families = true;
    rocksdb::DB *db = nullptr;

    
    options.max_background_compactions = 4;
    options.max_background_flushes = 2;
    options.bytes_per_sync = 1048576;
    options.compaction_pri = rocksdb::kMinOverlappingRatio;
    options.compression = rocksdb::kNoCompression;
    rocksdb::BlockBasedTableOptions table_options;
    table_options.block_size = 16 * 1024;
    table_options.cache_index_and_filter_blocks = true;
    table_options.pin_l0_filter_and_index_blocks_in_cache = true;
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    options.table_factory.reset(
        rocksdb::NewBlockBasedTableFactory(table_options));

    for (int idb = 0; idb < dbnum; ++idb)
    {
        rocksdb::ColumnFamilyOptions cf_options(options);
        cf_options.level_compaction_dynamic_level_bytes = true;
        veccoldesc.push_back(rocksdb::ColumnFamilyDescriptor(std::to_string(idb), cf_options));
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

IStorage *RocksDBStorageFactory::create(int db, key_load_iterator iter)
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
        if (fUnclean)
            printf("\tDatabase was not shutdown cleanly, recomputing metrics\n");
        auto opts = rocksdb::ReadOptions();
        opts.tailing = true;
        std::unique_ptr<rocksdb::Iterator> it = std::unique_ptr<rocksdb::Iterator>(m_spdb->NewIterator(opts, spcolfamily.get()));
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            if (FInternalKey(it->key().data(), it->key().size()))
                continue;
            if (iter != nullptr)
                iter(it->key().data(), it->key().size());
            ++count;
        }
    }
    return new RocksDBStorageProvider(m_spdb, spcolfamily, nullptr, count);
}

const char *RocksDBStorageFactory::name() const
{
    return "flash";
}