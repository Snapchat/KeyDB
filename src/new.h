#pragma once
#include <cstddef>  // std::size_t

[[deprecated]]
void *operator new(size_t size);

inline void *operator new(size_t size, enum MALLOC_CLASS mclass) 
{ 
    return zmalloc(size, mclass);
}

void operator delete(void * p) noexcept;

inline void operator delete(void *p, std::size_t) noexcept
{
    zfree(p);
}
