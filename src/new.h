#pragma once
#include <cstddef>  // std::size_t

[[deprecated]]
inline void *operator new(size_t size)
{
    return zmalloc(size, MALLOC_LOCAL);
}

inline void *operator new(size_t size, enum MALLOC_CLASS mclass) 
{ 
    return zmalloc(size, mclass);
} 

inline void operator delete(void * p) noexcept
{
    zfree(p);
}

inline void operator delete(void *p, std::size_t) noexcept
{
    zfree(p);
}