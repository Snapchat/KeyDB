#pragma once
#include <cstddef>  // std::size_t

[[deprecated]]
void *operator new(size_t size);

void *operator new(size_t size, enum MALLOC_CLASS mclass);

void operator delete(void * p) noexcept;
void operator delete(void *p, std::size_t) noexcept;
