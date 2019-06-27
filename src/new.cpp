#include <cstddef>  // std::size_t
#include "server.h"
#include "new.h"

[[deprecated]]
void *operator new(size_t size)
{
    return zmalloc(size, MALLOC_LOCAL);
}

void operator delete(void * p) noexcept
{
    zfree(p);
}

