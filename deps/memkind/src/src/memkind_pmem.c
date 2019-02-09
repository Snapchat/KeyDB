/*
 * Copyright (C) 2015 - 2018 Intel Corporation.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 1. Redistributions of source code must retain the above copyright notice(s),
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice(s),
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDER(S) ``AS IS'' AND ANY EXPRESS
 * OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO
 * EVENT SHALL THE COPYRIGHT HOLDER(S) BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE
 * OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
 * ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include <memkind/internal/memkind_arena.h>
#include <memkind/internal/memkind_pmem.h>
#include <memkind/internal/memkind_private.h>
#include <memkind/internal/memkind_log.h>

#include <sys/mman.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <jemalloc/jemalloc.h>
#include <assert.h>

MEMKIND_EXPORT struct memkind_ops MEMKIND_PMEM_OPS = {
    .create = memkind_pmem_create,
    .destroy = memkind_pmem_destroy,
    .malloc = memkind_arena_malloc,
    .calloc = memkind_arena_pmem_calloc,
    .posix_memalign = memkind_arena_posix_memalign,
    .realloc = memkind_arena_realloc,
    .free = memkind_arena_free,
    .mmap = memkind_pmem_mmap,
    .get_mmap_flags = memkind_pmem_get_mmap_flags,
    .get_arena = memkind_thread_get_arena,
    .finalize = memkind_pmem_destroy,
    .malloc_usable_size = memkind_default_malloc_usable_size
};

void clearextent(void *addr, struct memkind_pmem *priv)
{
	if (priv == NULL)
		return;
	for (int iextent = 0; iextent < priv->cextents; ++iextent){
		if (priv->rgextents[iextent].addrBase == addr){
			fallocate(priv->fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, priv->rgextents[iextent].offset, priv->rgextents[iextent].cb);
			priv->rgextents[iextent].addrBase = NULL;
			return;
		}
	}
}

MEMKIND_EXPORT void memkind_pmem_remapfd(struct memkind *kind, int fdNew)
{
	struct memkind_pmem *priv = kind->priv;
	for (int iextent = 0; iextent < priv->cextents; ++iextent)
	{
		struct memkind_pmem_extent *extent = priv->rgextents + iextent;
		munmap(extent->addrBase, extent->cb);
		mmap(extent->addrBase, extent->cb, PROT_READ | PROT_WRITE, MAP_SHARED, fdNew, extent->offset);
	}
	priv->fd = fdNew;
}

MEMKIND_EXPORT int memkind_pmem_iskind(struct memkind *kind, void *pv)
{
    struct memkind_pmem *priv = kind->priv;
    char *pb = pv;
	for (int iextent = 0; iextent < priv->cextents; ++iextent)
    {
        struct memkind_pmem_extent *extent = priv->rgextents + iextent;
        if ((((char*)extent->addrBase) <= pb) && (((char*)extent->addrBase)+extent->cb) > pb)
            return 1;
    }
    return 0;
}

void *pmem_extent_alloc(extent_hooks_t *extent_hooks,
                        void *new_addr,
                        size_t size,
                        size_t alignment,
                        bool *zero,
                        bool *commit,
                        unsigned arena_ind)
{
    int err;
    void *addr = NULL;

    if (new_addr != NULL) {
        /* not supported */
        goto exit;
    }

    struct memkind *kind;
    kind = get_kind_by_arena(arena_ind);
    if (kind == NULL) {
        return NULL;
    }

    err = memkind_check_available(kind);
    if (err) {
        goto exit;
    }

    addr = memkind_pmem_mmap(kind, new_addr, size);

    if (addr != MAP_FAILED) {
        *zero = true;
        *commit = true;

        /* XXX - check alignment */
    } else {
        addr = NULL;
    }
exit:
    return addr;
}

bool pmem_extent_dalloc(extent_hooks_t *extent_hooks,
                        void *addr,
                        size_t size,
                        bool committed,
                        unsigned arena_ind)
{
    // if madvise fail, it means that addr isn't mapped shared (doesn't come from pmem)
    // and it should be unmapped to avoid space exhaustion when calling large number of
    // operations like memkind_create_pmem and memkind_destroy_kind
    // EOPNOTSUPP is returned in case of filesystem doesn't support FALLOC_FL_PUNCH_HOLE
    errno = 0;
    if (madvise(addr, size, MADV_REMOVE) != 0 && errno != EOPNOTSUPP) {
        if (munmap(addr, size) == -1) {
            log_err("munmap failed!");
        }
	struct memkind *kind = get_kind_by_arena(arena_ind);
	if (kind != NULL)
		clearextent(addr, kind->priv);
    }
    return true;
}

bool pmem_extent_commit(extent_hooks_t *extent_hooks,
                        void *addr,
                        size_t size,
                        size_t offset,
                        size_t length,
                        unsigned arena_ind)
{
    /* do nothing - report success */
    return false;
}

bool pmem_extent_decommit(extent_hooks_t *extent_hooks,
                          void *addr,
                          size_t size,
                          size_t offset,
                          size_t length,
                          unsigned arena_ind)
{
    /* do nothing - report failure (opt-out) */
    return true;
}

bool pmem_extent_purge(extent_hooks_t *extent_hooks,
                       void *addr,
                       size_t size,
                       size_t offset,
                       size_t length,
                       unsigned arena_ind)
{
    /* do nothing - report failure (opt-out) */
    return true;
}

bool pmem_extent_split(extent_hooks_t *extent_hooks,
                       void *addr,
                       size_t size,
                       size_t size_a,
                       size_t size_b,
                       bool committed,
                       unsigned arena_ind)
{
    /* do nothing - report success */
    return false;
}

bool pmem_extent_merge(extent_hooks_t *extent_hooks,
                       void *addr_a,
                       size_t size_a,
                       void *addr_b,
                       size_t size_b,
                       bool committed,
                       unsigned arena_ind)
{
    /* do nothing - report success */
    return false;
}

void pmem_extent_destroy(extent_hooks_t *extent_hooks,
                         void *addr,
                         size_t size,
                         bool committed,
                         unsigned arena_ind)
{
    if (munmap(addr, size) == -1) {
        log_err("munmap failed!");
    } 
    else{
	struct memkind *kind = get_kind_by_arena(arena_ind);
	if (kind != NULL)
		clearextent(addr, kind->priv);
    }
}

static extent_hooks_t pmem_extent_hooks = {
    .alloc = pmem_extent_alloc,
    .dalloc = pmem_extent_dalloc,
    .commit = pmem_extent_commit,
    .decommit = pmem_extent_decommit,
    .purge_lazy = pmem_extent_purge,
    .split = pmem_extent_split,
    .merge = pmem_extent_merge,
    .destroy = pmem_extent_destroy
};

MEMKIND_EXPORT int memkind_fd(struct memkind *kind)
{
	struct memkind_pmem *priv = kind->priv;
	return priv->fd;
}

MEMKIND_EXPORT int memkind_pmem_create(struct memkind *kind,
                                       struct memkind_ops *ops, const char *name)
{
    struct memkind_pmem *priv;
    int err;

    priv = (struct memkind_pmem *)jemk_calloc(sizeof(struct memkind_pmem), 1);
    if (!priv) {
        log_err("cemk_malloc() failed.");
        return MEMKIND_ERROR_MALLOC;
    }

    if (pthread_mutex_init(&priv->pmem_lock, NULL) != 0) {
        err = MEMKIND_ERROR_RUNTIME;
        goto exit;
    }

    err = memkind_default_create(kind, ops, name);
    if (err) {
        goto exit;
    }

    err = memkind_arena_create_map(kind, &pmem_extent_hooks);
    if (err) {
        goto exit;
    }

    kind->priv = priv;
    return 0;

exit:
    /* err is set, please don't overwrite it with result of pthread_mutex_destroy */
    pthread_mutex_destroy(&priv->pmem_lock);
    jemk_free(priv);
    return err;
}

MEMKIND_EXPORT int memkind_pmem_destroy(struct memkind *kind)
{
    struct memkind_pmem *priv = kind->priv;

    memkind_arena_destroy(kind);

    pthread_mutex_destroy(&priv->pmem_lock);

    (void) close(priv->fd);
    jemk_free(priv);

    return 0;
}

MEMKIND_EXPORT void *memkind_pmem_mmap(struct memkind *kind, void *addr,
                                       size_t size)
{
    struct memkind_pmem *priv = kind->priv;
    void *result;

    if (pthread_mutex_lock(&priv->pmem_lock) != 0)
        assert(0 && "failed to acquire mutex");

    if (priv->max_size != 0 && (size_t)priv->offset + size > priv->max_size) {
        pthread_mutex_unlock(&priv->pmem_lock);
        return MAP_FAILED;
    }

    if ((errno = posix_fallocate(priv->fd, priv->offset, (off_t)size)) != 0) {
        pthread_mutex_unlock(&priv->pmem_lock);
        return MAP_FAILED;
    }

    if ((result = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, priv->fd,
                       priv->offset)) != MAP_FAILED) {
        
	if (priv->cextentsAlloc <= priv->cextents){
        	if (priv->cextentsAlloc == 0)
			priv->cextentsAlloc = 64;
		else
			priv->cextentsAlloc *= 2;
		priv->rgextents = jemk_realloc(priv->rgextents, priv->cextentsAlloc * sizeof(struct memkind_pmem_extent));
	}
	priv->rgextents[priv->cextents].addrBase = result;
	priv->rgextents[priv->cextents].cb = size;
	priv->rgextents[priv->cextents].offset = priv->offset;
	priv->cextents++;
        priv->offset += size;
    }

    pthread_mutex_unlock(&priv->pmem_lock);

    return result;
}

MEMKIND_EXPORT int memkind_pmem_get_mmap_flags(struct memkind *kind, int *flags)
{
    *flags = MAP_SHARED;
    return 0;
}
