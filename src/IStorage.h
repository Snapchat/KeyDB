#pragma once
#include <functional>

class IStorageFactory
{
public:
    virtual ~IStorageFactory() {}
    virtual class IStorage *create(int db) = 0;
};

class IStorage
{
public:
    typedef std::function<void(const char *, size_t, const void *, size_t)> callback;

    virtual ~IStorage() {}

    virtual void insert(const char *key, size_t cchKey, void *data, size_t cb) = 0;
    virtual void erase(const char *key, size_t cchKey) = 0;
    virtual void retrieve(const char *key, size_t cchKey, callback fn) const = 0;
    virtual size_t clear() = 0;
    virtual void enumerate(callback fn) const = 0;

    virtual void beginWriteBatch() {} // NOP
    virtual void endWriteBatch() {} // NOP

    virtual void flush();

    /* This is permitted to be a shallow clone */
    virtual const IStorage *clone() const = 0;
};
