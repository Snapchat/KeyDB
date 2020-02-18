#include "rocksdb.h"
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

    virtual IStorage *create(int db) override;
    virtual const char *name() const override;
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

    for (auto handle : handles)
        m_vecspcols.emplace_back(handle);
    m_spdb = std::shared_ptr<rocksdb::DB>(db);
}

RocksDBStorageFactory::~RocksDBStorageFactory()
{
    m_spdb->SyncWAL();
}

IStorage *RocksDBStorageFactory::create(int db)
{
    ++db;   // skip default col family
    std::shared_ptr<rocksdb::ColumnFamilyHandle> spcolfamily(m_vecspcols[db].release());
    size_t count = 0;
    std::unique_ptr<rocksdb::Iterator> it = std::unique_ptr<rocksdb::Iterator>(m_spdb->NewIterator(rocksdb::ReadOptions(), spcolfamily.get()));
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
        ++count;
    }
    return new RocksDBStorageProvider(m_spdb, spcolfamily, nullptr, count);
}

const char *RocksDBStorageFactory::name() const
{
    return "flash";
}