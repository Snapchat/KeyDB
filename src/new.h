#pragma once
#include <cstddef>  // std::size_t
#include "storage.h"

void *operator new(size_t size, enum MALLOC_CLASS mclass);

#ifndef SANITIZE
void *operator new(size_t size);

void operator delete(void * p) noexcept;
void operator delete(void *p, std::size_t) noexcept;
#endif