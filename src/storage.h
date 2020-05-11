#ifndef __STORAGE_H__
#define __STORAGE_H__

#define OBJ_ENCODING_EMBSTR_SIZE_LIMIT 48   // Note: also defined in object.c - should always match

#ifdef __cplusplus
extern "C" {
#endif

enum MALLOC_CLASS
{
    MALLOC_LOCAL,
    MALLOC_SHARED,
};

void storage_init(const char *tmpfilePath, size_t cbFileReserve);

void *salloc(size_t cb, enum MALLOC_CLASS mclass);
void *scalloc(size_t cb, size_t c, enum MALLOC_CLASS mclass);
void sfree(void*);
void *srealloc(void *pv, size_t cb, enum MALLOC_CLASS mclass);
size_t salloc_usable_size(void *ptr);

struct redisObject *salloc_objembstr();
void sfree_objembstr(struct redisObject *obj);
struct redisObject *salloc_obj();
void sfree_obj(struct redisObject *obj);

#ifdef __cplusplus
}
#endif

#endif
