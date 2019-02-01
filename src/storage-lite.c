#include <stdlib.h>
#include <stdio.h>
#include <memkind.h>
#include <sys/ioctl.h>
#include <linux/fs.h>
#include <unistd.h>
#include <inttypes.h>
#include "storage.h"
#include <assert.h>

// initialize the memory subsystem. 
//  NOTE: This may be called twice, first with NULL specifying we should use ram
//      later, after the configuration file is loaded with a path to where we should
//      place our temporary file.
void storage_init(const char *tmpfilePath)
{
    assert(tmpfilePath == NULL);
    (void)tmpfilePath;
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

void *srealloc(void *pv, size_t cb)
{
    return realloc(pv, cb);
}
