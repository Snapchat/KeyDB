#include "server.h"

#include <stdlib.h>
#include <stdio.h>
#include <memkind.h>
#include <sys/ioctl.h>
#include <linux/fs.h>
#include <unistd.h>
#include <inttypes.h>
#include "storage.h"

struct memkind *mkdisk = NULL;
static char *PMEM_DIR = "/mnt/btrfs_scratch/";

void handle_prefork();
void handle_postfork_parent();
void handle_postfork_child();

#define OBJECT_PAGE_BUFFER_SIZE 8192 //(size in objs)
#define OBJ_PAGE_BITS_PER_WORD 64
struct object_page
{
    uint64_t allocmap[OBJECT_PAGE_BUFFER_SIZE/(8*sizeof(uint64_t))];
    struct object_page *pnext;
    char rgb[];
};

struct alloc_pool
{
    unsigned cbObject;
    struct object_page *pobjpageHead;
};


struct object_page *pool_allocate_page(int cbObject)
{
    size_t cb = (((size_t)cbObject) * OBJECT_PAGE_BUFFER_SIZE) + sizeof(struct object_page);
    return scalloc(cb, 1, MALLOC_SHARED);
}
void pool_initialize(struct alloc_pool *ppool, int cbObject)
{
    if ((cbObject % 8) != 0)
    {
        cbObject += 8 - (cbObject % 8);
    }
    ppool->cbObject = cbObject;
    ppool->pobjpageHead = pool_allocate_page(cbObject);
}
static int IdxAllocObject(struct object_page *page)
{
    for (size_t iword = 0; iword < OBJ_PAGE_BITS_PER_WORD; ++iword)
    {
        if ((page->allocmap[iword] + 1) != 0)
        {
            int ibit = 0;
            uint64_t bitword = page->allocmap[iword];
            while (bitword & 1)
            {
                bitword >>= 1;
                ++ibit;
            }
            page->allocmap[iword] |= 1ULL << ibit;
            return (iword * OBJ_PAGE_BITS_PER_WORD) + ibit;
        }
    }
    return -1;
}
void *pool_alloc(struct alloc_pool *ppool)
{
    struct object_page *cur = ppool->pobjpageHead;
    for (;;)
    {
        int idx = IdxAllocObject(cur);
        if (idx >= 0)
        {
            return cur->rgb + (((size_t)ppool->cbObject) * idx);
        }

        if (cur->pnext == NULL)
        {
            cur->pnext = pool_allocate_page(ppool->cbObject);
        }

        cur = cur->pnext;
    }
}

#pragma weak serverLog
void serverLog(int level, const char*fmt, ...){}

void pool_free(struct alloc_pool *ppool, void *pv)
{
    struct object_page *cur = ppool->pobjpageHead;
    char *obj = pv;

    for (;cur != NULL;)
    {
        if (obj >= cur->rgb && (obj < (cur->rgb + (OBJECT_PAGE_BUFFER_SIZE * ppool->cbObject))))
        {
            // Its on this page
            int idx = (obj - cur->rgb) / ppool->cbObject;
            cur->allocmap[idx / OBJ_PAGE_BITS_PER_WORD] &= ~(1ULL << (idx % OBJ_PAGE_BITS_PER_WORD));
            return;
        }
        cur = cur->pnext;
    }
    serverLog(LOG_CRIT, "obj not from pool");
    sfree(obj); // we don't know where it came from
    return;
}

#define EMBSTR_ROBJ_SIZE (sizeof(robj)+sizeof(struct sdshdr8)+OBJ_ENCODING_EMBSTR_SIZE_LIMIT+1)
struct alloc_pool poolobj;
struct alloc_pool poolembstrobj;

void storage_init()
{
    int errv = memkind_create_pmem(PMEM_DIR, 0, &mkdisk);
    if (errv)
    {
        fprintf(stderr, "Memory pool creation failed: %d\n", errv);
        exit(EXIT_FAILURE);
    }
    pool_initialize(&poolobj, sizeof(robj));
    pool_initialize(&poolembstrobj, EMBSTR_ROBJ_SIZE);

    pthread_atfork(handle_prefork, handle_postfork_parent, handle_postfork_child);
}



struct redisObject *salloc_obj()
{
    return pool_alloc(&poolobj);
}
void sfree_obj(struct redisObject *obj)
{
    pool_free(&poolobj, obj);
}
struct redisObject *salloc_objembstr()
{
    return pool_alloc(&poolembstrobj);
}
void sfree_objembstr(robj *obj)
{
    pool_free(&poolembstrobj, obj);
}

void *salloc(size_t cb, enum MALLOC_CLASS class)
{
    switch (class)
    {
    case MALLOC_SHARED:
        return memkind_malloc(mkdisk, cb);
    default:
        return memkind_malloc(MEMKIND_DEFAULT, cb);
    }
    return NULL;
}

void *scalloc(size_t cb, size_t c, enum MALLOC_CLASS class)
{
    switch (class)
    {
    case MALLOC_SHARED:
        return memkind_calloc(mkdisk, cb, c);
    default:
        return memkind_calloc(MEMKIND_DEFAULT, cb, c);
    }
    return NULL;
}

void sfree(void *pv)
{
    memkind_free(NULL, pv);
}

void *srealloc(void *pv, size_t cb)
{
    memkind_t kind = mkdisk;
    return memkind_realloc(kind, pv, cb);
}

int fdNew = -1;
void handle_prefork()
{
    memkind_tmpfile(PMEM_DIR, &fdNew);
    if (ioctl(fdNew, FICLONE, memkind_fd(mkdisk)) == -1)
    {
        perror("failed to fork file");
        exit(EXIT_FAILURE);
    }
}

void handle_postfork_parent()
{
    // Parent, close fdNew
    close(fdNew);
    fdNew = -1;
}

void handle_postfork_child()
{
    int fdOriginal = memkind_fd(mkdisk);
    memkind_pmem_remapfd(mkdisk, fdNew);
    close(fdOriginal);
}