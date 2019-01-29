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



#define OBJECT_PAGE_BUFFER_SIZE 8192 //(size in objs)
struct object_page
{
    uint64_t allocmap[OBJECT_PAGE_BUFFER_SIZE/(8*sizeof(uint64_t))];
    robj rgobj[OBJECT_PAGE_BUFFER_SIZE];
    struct object_page *pnext;
};
#define OBJ_PAGE_BITS_PER_WORD 64
struct object_page *headObjpage = NULL;

void storage_init()
{
    int errv = memkind_create_pmem(PMEM_DIR, 0, &mkdisk);
    if (errv)
    {
        fprintf(stderr, "Memory pool creation failed: %d\n", errv);
        exit(EXIT_FAILURE);
    }
    headObjpage = memkind_calloc(MEMKIND_HUGETLB, 1, sizeof(struct object_page));
}

int IdxAllocObject(struct object_page *page)
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
            page->allocmap[iword] |= 1 << ibit;
            return (iword * OBJ_PAGE_BITS_PER_WORD) + ibit;
        }
    }
    return -1;
}

struct redisObject *salloc_obj()
{
    struct object_page *cur = headObjpage;
    for (;;)
    {
        int idx = IdxAllocObject(cur);
        if (idx >= 0)
        {
            return &cur->rgobj[idx];
        }

        if (cur->pnext == NULL)
        {
            cur->pnext = memkind_calloc(MEMKIND_HUGETLB, 1, sizeof(struct object_page));
        }

        cur = cur->pnext;
    }
}
void sfree_obj(struct redisObject *obj)
{
    struct object_page *cur = headObjpage;
    for (;;)
    {
        if (obj >= cur->rgobj && (obj < (cur->rgobj + OBJECT_PAGE_BUFFER_SIZE)))
        {
            // Its on this page
            int idx = obj - cur->rgobj;
            cur->allocmap[idx / OBJ_PAGE_BITS_PER_WORD] &= ~(1 << (idx % OBJ_PAGE_BITS_PER_WORD));
            break;
        }
        cur = cur->pnext;
    }
    return;
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

void handle_postfork(int pid)
{
    if (pid != 0)
    {
        // Parent, close fdNew
        close(fdNew);
        fdNew = -1;
    }
    else
    {
        int fdOriginal = memkind_fd(mkdisk);
        memkind_pmem_remapfd(mkdisk, fdNew);
        close(fdOriginal);
    }
    
}