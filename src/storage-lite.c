#include <stdlib.h>
#include <stdio.h>
#include <sys/ioctl.h>
#include <unistd.h>
#include <inttypes.h>
#include "storage.h"
#include <assert.h>
#ifdef __linux__
#include <malloc.h>
#endif

// initialize the memory subsystem. 
//  NOTE: This may be called twice, first with NULL specifying we should use ram
//      later, after the configuration file is loaded with a path to where we should
//      place our temporary file.
void storage_init(const char *tmpfilePath, size_t cbReserve)
{
    assert(tmpfilePath == NULL);
    (void)tmpfilePath;
    (void)cbReserve;
}

void *salloc(size_t cb, enum MALLOC_CLASS class)
{
    (void)class;
    return malloc(cb);
}

void *scalloc(size_t cb, size_t c, enum MALLOC_CLASS class)
{
    (void)class;
    return calloc(cb, c);
}

void sfree(void *pv)
{
    free(pv);
}

void *srealloc(void *pv, size_t cb, enum MALLOC_CLASS class)
{
    (void)class;
    return realloc(pv, cb);
}

#ifdef __linux__
size_t salloc_usable_size(void *ptr)
{
    return malloc_usable_size(ptr);
}
#endif
