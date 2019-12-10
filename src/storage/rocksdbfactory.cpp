#include "rocksdb.h"

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
    
    for (int idb = 0; idb < dbnum; ++idb)
    {
        veccoldesc.push_back(rocksdb::ColumnFamilyDescriptor(std::to_string(idb), rocksdb::ColumnFamilyOptions()));
    }

    rocksdb::Options options;
    options.create_if_missing = true;
    options.create_missing_column_families = true;
    rocksdb::DB *db = nullptr;
    
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
    return new RocksDBStorageProvider(m_spdb, spcolfamily, nullptr);
}

const char *RocksDBStorageFactory::name() const
{
    return "flash";
}