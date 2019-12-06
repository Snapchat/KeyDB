#include "../IStorage.h"
#include <rocksdb/db.h>

class RocksDBStorageProvider : public IStorage
{
    std::unique_ptr<rocksdb::DB> m_spdb;

public:
    ~RocksDBStorageProvider();
};

RocksDBStorageProvider::~RocksDBStorageProvider()
{
}