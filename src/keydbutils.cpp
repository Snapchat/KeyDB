#include "server.h"

namespace keydbutils
{
    template<>
    size_t hash(const sdsview& t)
    {
        return (size_t)dictGenHashFunction(static_cast<const char*>(t), t.size());
    }
}