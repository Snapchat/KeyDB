#include "teststorageprovider.h"
#include <assert.h>

IStorage *TestStorageFactory::create(int)
{
    return new TestStorageProvider();
}

const char *TestStorageFactory::name() const
{
    return "TEST Storage Provider";
}

TestStorageProvider::TestStorageProvider()
{
}

TestStorageProvider::~TestStorageProvider()
{
}

void TestStorageProvider::insert(const char *key, size_t cchKey, void *data, size_t cb)
{
    m_map.insert(std::make_pair(std::string(key, cchKey), std::string((char*)data, cb)));
}


 bool TestStorageProvider::erase(const char *key, size_t cchKey)
 {
     auto itr = m_map.find(std::string(key, cchKey));
     if (itr != m_map.end())
     {
         m_map.erase(itr);
         return true;
     }
     return false;
 }

void TestStorageProvider::retrieve(const char *key, size_t cchKey, callbackSingle fn) const
{
    auto itr = m_map.find(std::string(key, cchKey));
    if (itr != m_map.end())
        fn(key, cchKey, itr->second.data(), itr->second.size());
}

size_t TestStorageProvider::clear()
{
    size_t size = m_map.size();
    m_map.clear();
    return size;
}

bool TestStorageProvider::enumerate(callback fn) const
{
    bool fAll = true;
    for (auto &pair : m_map)
    {
        if (!fn(pair.first.data(), pair.first.size(), pair.second.data(), pair.second.size()))
        {
            fAll = false;
            break;
        }
    }
    return fAll;
}
    
size_t TestStorageProvider::count() const
{
    return m_map.size();
}

void TestStorageProvider::flush()
{
    /* NOP */
}

/* This is permitted to be a shallow clone */
const IStorage *TestStorageProvider::clone() const
{
    return new TestStorageProvider(*this);
}