#pragma once

class IStorageFactory *CreateRocksDBStorageFactory(const char *path, int dbnum, const char *rgchConfig, size_t cchConfig);