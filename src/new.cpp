#include <cstddef>  // std::size_t
#include "server.h"
#include "new.h"
#include <new>

#ifdef SANITIZE
void *operator new(size_t size, enum MALLOC_CLASS mclass)
{
    (void)mclass;
    return ::operator new(size);
}

#else
[[deprecated]]
void *operator new(size_t size)
{
    return zmalloc(size, MALLOC_LOCAL);
}

void *operator new(size_t size, enum MALLOC_CLASS mclass) 
{ 
    return zmalloc(size, mclass);
}

void *operator new(std::size_t size, const std::nothrow_t &) noexcept
{
    return zmalloc(size, MALLOC_LOCAL);
}

//need to do null checks for delete since the compiler can optimize out null checks in zfree
void operator delete(void * p) noexcept
{
    if (p != nullptr)
        zfree(p);
}

void operator delete(void *p, std::size_t) noexcept
{
    if (p != nullptr)
        zfree(p);
}

#endif

#if defined(USE_JEMALLOC)
extern "C" size_t malloc_usable_size(void *ptr)
{
    return zmalloc_usable_size(ptr);
}
#endif
