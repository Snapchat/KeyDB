#pragma once

class IStorageFactory *CreateRocksDBStorageFactory(const char *path, int dbnum);