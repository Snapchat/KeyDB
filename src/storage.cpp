#include "server.h"


IStorage::~IStorage() {}

#ifdef USE_MEMKIND

#include <stdlib.h>
#include <stdio.h>
#include <memkind.h>
#include <sys/ioctl.h>
#include <linux/fs.h>
#include <unistd.h>
#include <inttypes.h>
#include <fcntl.h>
#include "storage.h"
#include "IStorage.h"

struct memkind *mkdisk = NULL;
static const char *PMEM_DIR = NULL;

extern "C" int memkind_pmem_iskind(struct memkind *kind, const void *pv);

void handle_prefork();
void handle_postfork_parent();
void handle_postfork_child();

#define OBJECT_PAGE_BUFFER_SIZE 8192 //(size in objs)
#define OBJ_PAGE_BITS_PER_WORD 64
struct object_page
{
    uint64_t allocmap[OBJECT_PAGE_BUFFER_SIZE/(8*sizeof(uint64_t))];
    struct object_page *pnext;
    char *rgb()
    {
        return reinterpret_cast<char*>(this+1);
    };
};

struct alloc_pool
{
    unsigned cbObject;
    struct object_page *pobjpageHead;
};


struct object_page *pool_allocate_page(int cbObject)
{
    size_t cb = (((size_t)cbObject) * OBJECT_PAGE_BUFFER_SIZE) + sizeof(struct object_page);
    return (object_page*)scalloc(cb, 1, MALLOC_SHARED);
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
            return cur->rgb() + (((size_t)ppool->cbObject) * idx);
        }

        if (cur->pnext == NULL)
        {
            cur->pnext = pool_allocate_page(ppool->cbObject);
        }

        cur = cur->pnext;
    }
}

void pool_free(struct alloc_pool *ppool, void *pv)
{
    struct object_page *cur = ppool->pobjpageHead;
    char *obj = (char*)pv;

    for (;cur != NULL;)
    {
        if (obj >= cur->rgb() && (obj < (cur->rgb() + (OBJECT_PAGE_BUFFER_SIZE * ppool->cbObject))))
        {
            // Its on this page
            int idx = (obj - cur->rgb()) / ppool->cbObject;
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

int forkFile()
{
    int fdT;
    memkind_tmpfile(PMEM_DIR, &fdT);
    if (ioctl(fdT, FICLONE, memkind_fd(mkdisk)) == -1)
    {
        return -1;
    }
    return fdT;
}

// initialize the memory subsystem. 
//  NOTE: This may be called twice, first with NULL specifying we should use ram
//      later, after the configuration file is loaded with a path to where we should
//      place our temporary file.
void storage_init(const char *tmpfilePath, size_t cbFileReserve)
{
    if (tmpfilePath == NULL)
    {
        serverAssert(mkdisk == NULL);
        mkdisk = MEMKIND_DEFAULT;
        pool_initialize(&poolobj, sizeof(robj));
        pool_initialize(&poolembstrobj, EMBSTR_ROBJ_SIZE);
    }
    else
    {
        // First create the file
        serverAssert(mkdisk == MEMKIND_DEFAULT);
        PMEM_DIR = (char*)memkind_malloc(MEMKIND_DEFAULT, strlen(tmpfilePath));
        strcpy((char*)PMEM_DIR, tmpfilePath);
        int errv = memkind_create_pmem(PMEM_DIR, 0, &mkdisk);
        if (errv == MEMKIND_ERROR_INVALID)
        {
            serverLog(LOG_CRIT, "Memory pool creation failed: %s", strerror(errno));
            exit(EXIT_FAILURE);
        }
        else if (errv)
        {
            char msgbuf[1024];
            memkind_error_message(errv, msgbuf, 1024);
            serverLog(LOG_CRIT, "Memory pool creation failed: %s", msgbuf);
            exit(EXIT_FAILURE);
        }

        // Next test if COW is working
        int fdTest = forkFile();
        if (fdTest < 0)
        {
            serverLog(LOG_ERR, "Scratch file system does not support Copy on Write.  To fix this scratch-file-path must point to a path on a filesystem which supports copy on write, such as btrfs.");
            exit(EXIT_FAILURE);
        }
        close(fdTest);

        // Now lets make the file big
        if (cbFileReserve == 0)
            cbFileReserve = 1*1024*1024*1024;   // 1 GB (enough to be interesting)
        posix_fallocate64(memkind_fd(mkdisk), 0, cbFileReserve);

        pthread_atfork(handle_prefork, handle_postfork_parent, handle_postfork_child);
    }
}



struct redisObject *salloc_obj()
{
    return (redisObject*)pool_alloc(&poolobj);
}
void sfree_obj(struct redisObject *obj)
{
    pool_free(&poolobj, obj);
}
struct redisObject *salloc_objembstr()
{
    return (redisObject*)pool_alloc(&poolembstrobj);
}
void sfree_objembstr(robj *obj)
{
    pool_free(&poolembstrobj, obj);
}

static memkind_t kindFromPtr(const void *pv)
{
    if (mkdisk == MEMKIND_DEFAULT)
        return MEMKIND_DEFAULT;
    
    if (memkind_pmem_iskind(mkdisk, pv))
        return mkdisk;
    return MEMKIND_DEFAULT;
}

size_t salloc_usable_size(void *ptr)
{
    return memkind_malloc_usable_size(kindFromPtr(ptr), ptr);
}

static memkind_t kindFromClass(enum MALLOC_CLASS mclass)
{
    switch (mclass)
    {
    case MALLOC_SHARED:
        return mkdisk;
    default:
        break;
    }
    return MEMKIND_DEFAULT;
}

void *salloc(size_t cb, enum MALLOC_CLASS mclass)
{
    if (cb == 0) 
        cb = 1;
        
    return memkind_malloc(kindFromClass(mclass), cb);
}

void *scalloc(size_t cb, size_t c, enum MALLOC_CLASS mclass)
{
    return memkind_calloc(kindFromClass(mclass), cb, c);
}

void sfree(void *pv)
{
    memkind_free(kindFromPtr(pv), pv);
}

void *srealloc(void *pv, size_t cb, enum MALLOC_CLASS mclass)
{
    return memkind_realloc(kindFromClass(mclass), pv, cb);
}

int fdNew = -1;
void handle_prefork()
{
    fdNew = forkFile();
    if (fdNew < 0)
        serverLog(LOG_ERR, "Failed to clone scratch file");
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

#endif // USE_MEMKIND
