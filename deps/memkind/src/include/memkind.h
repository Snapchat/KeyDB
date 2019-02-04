/*
 * Copyright (C) 2014 - 2018 Intel Corporation.
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

#pragma once
#ifdef __cplusplus
extern "C" {
#endif

#include <sys/types.h>

/**
 * Header file for the memkind heap manager.
 * More details in memkind(3) man page.
 *
 * API standards are described in memkind(3) man page.
 */

/// \warning EXPERIMENTAL API
#define _MEMKIND_BIT(N) (1ull << N)

/// \brief Memkind memory types
/// \warning EXPERIMENTAL API
typedef enum memkind_memtype_t {

    /**
     * Select standard memory, the same as process use.
     */
    MEMKIND_MEMTYPE_DEFAULT = _MEMKIND_BIT(0),

    /**
     * Select high bandwidth memory (HBM).
     * There must be at least two memories with different bandwidth to
     * determine the HBM.
     */
    MEMKIND_MEMTYPE_HIGH_BANDWIDTH = _MEMKIND_BIT(1)

} memkind_memtype_t;

#undef _MEMKIND_BIT

/// \brief Memkind policy
/// \warning EXPERIMENTAL API
typedef enum memkind_policy_t {

    /**
     * Allocate local memory.
     * If there is not enough memory to satisfy the request errno is set to
     * ENOMEM and the allocated pointer is set to NULL.
     */
    MEMKIND_POLICY_BIND_LOCAL = 0,

    /**
     * Memory locality is ignored.
     * If there is not enough memory to satisfy the request errno is set to
     * ENOMEM and the allocated pointer is set to NULL.
     */
    MEMKIND_POLICY_BIND_ALL,

    /**
     * Allocate preferred memory that is local.
     * If there is not enough preferred memory to satisfy the request or
     * preferred memory is not available, the allocation will fall back on any
     * other memory.
     */
    MEMKIND_POLICY_PREFERRED_LOCAL,

    /**
     * Interleave allocation across local memory.
     * For n memory types the allocation will be interleaved across all of
     * them.
     */
    MEMKIND_POLICY_INTERLEAVE_LOCAL,

    /**
     * Interleave allocation. Locality is ignored.
     * For n memory types the allocation will be interleaved across all of
     * them.
     */
    MEMKIND_POLICY_INTERLEAVE_ALL,

    /**
     * Max policy value.
     */
    MEMKIND_POLICY_MAX_VALUE

} memkind_policy_t;

/// \brief Memkind bits definition
/// \warning EXPERIMENTAL API
/// \note The bits specify flags and masks. Bits <0,1,2,...,7> are reserved for page size, where page sizes are encoded
///       by base-2 logarithm. If the page size bits are set to zero value, than default page size will be used.
typedef enum memkind_bits_t {
    MEMKIND_MASK_PAGE_SIZE_2MB = 21ull,  /**<  Allocations backed by 2 MB page size (2^21 = 2MB) */
} memkind_bits_t;

/// \brief Memkind type definition
/// \warning EXPERIMENTAL API
typedef struct memkind *memkind_t;


/// \brief Memkind constant values
/// \warning EXPERIMENTAL API
enum memkind_const {
    MEMKIND_MAX_KIND = 512,                     /**<  Maximum number of kinds */
    MEMKIND_ERROR_MESSAGE_SIZE = 128,           /**<  Error message size */
    MEMKIND_PMEM_MIN_SIZE = (1024 * 1024 * 16)  /**<  The minimum size which allows to limit the file-backed memory partition */
};

/// \brief Memkind operation statuses
/// \warning EXPERIMENTAL API
enum {
    MEMKIND_SUCCESS = 0,                        /**<  Operation success */
    MEMKIND_ERROR_UNAVAILABLE = -1,             /**<  Error: Memory kind is not available */
    MEMKIND_ERROR_MBIND = -2,                   /**<  Error: Call to mbind() failed */
    MEMKIND_ERROR_MMAP  = -3,                   /**<  Error: Call to mmap() failed */
    MEMKIND_ERROR_MALLOC = -6,                  /**<  Error: Call to malloc() failed */
    MEMKIND_ERROR_ENVIRON = -12,                /**<  Error: Unable to parse environment variable */
    MEMKIND_ERROR_INVALID = -13,                /**<  Error: Invalid argument */
    MEMKIND_ERROR_TOOMANY = -15,                /**<  Error: Attempt to initialize more than MEMKIND_MAX_KIND number of kinds */
    MEMKIND_ERROR_BADOPS = -17,                 /**<  Error: Invalid memkind_ops structure */
    MEMKIND_ERROR_HUGETLB = -18,                /**<  Error: Unable to allocate huge pages */
    MEMKIND_ERROR_MEMTYPE_NOT_AVAILABLE = -20,  /**<  Error: Requested memory type is not available */
    MEMKIND_ERROR_OPERATION_FAILED = -21,       /**<  Error: Operation failed */
    MEMKIND_ERROR_ARENAS_CREATE = -22,          /**<  Error: Call to jemalloc's arenas.create failed */
    MEMKIND_ERROR_RUNTIME = -255                /**<  Error: Unspecified run-time error */
};

///
/// \brief Create kind that allocates memory with specific memory type, memory binding policy and flags.
/// \warning EXPERIMENTAL API
/// \note Currently implemented memory type and policy configurations:
///.      {MEMKIND_MEMTYPE_DEFAULT, MEMKIND_POLICY_PREFERRED_LOCAL},
///.      {MEMKIND_MEMTYPE_HIGH_BANDWIDTH, MEMKIND_POLICY_BIND_LOCAL},
///       {MEMKIND_MEMTYPE_HIGH_BANDWIDTH, MEMKIND_POLICY_PREFERRED_LOCAL},
///       {MEMKIND_MEMTYPE_HIGH_BANDWIDTH, MEMKIND_POLICY_INTERLEAVE_ALL},
///       {MEMKIND_MEMTYPE_DEFAULT | MEMKIND_MEMTYPE_HIGH_BANDWIDTH, MEMKIND_POLICY_INTERLEAVE_ALL}.
/// \param memtype_flags determine the memory types to allocate from by combination of memkind_memtype_t values.
///        This field cannot have zero value.
/// \param policy specify policy for page binding to memory types selected by  memtype_flags.
///        This field must be set to memkind_policy_t value. If policy is set to MEMKIND_POLICY_PREFERRED_LOCAL then only one memory
///        type must be selected. Note: the value cannot be set to MEMKIND_POLICY_MAX_VALUE.
/// \param flags the field must be set to a combination of memkind_bits_t values.
/// \param kind pointer to kind which will be created
/// \return Memkind operation status, MEMKIND_SUCCESS on success, MEMKIND_ERROR_MEMTYPE_NOT_AVAILABLE or MEMKIND_ERROR_INVALID on failure
///
int memkind_create_kind(memkind_memtype_t memtype_flags,
                        memkind_policy_t policy,
                        memkind_bits_t flags,
                        memkind_t *kind);

///
/// \brief Destroy previously created kind object, which must have been returned
///        by a call to memkind_create_kind() or memkind_create_pmem().
///        The function has undefined behavior when the handle is invalid or
///        memkind_destroy_kind(kind) was already called before
/// \warning EXPERIMENTAL API
/// \note if the kind was returned by memkind_create_kind() all allocated memory must be freed
///       before kind is destroyed, otherwise this will cause memory leak.
/// \param kind specified memory kind
/// \return Memkind operation status, MEMKIND_SUCCESS on success, MEMKIND_ERROR_OPERATION_FAILED on failure
///
int memkind_destroy_kind(memkind_t kind);


#include "memkind_deprecated.h"

/// \warning EXPERIMENTAL API
extern memkind_t MEMKIND_REGULAR;

/// \warning EXPERIMENTAL API
extern memkind_t MEMKIND_DEFAULT;

/// \warning EXPERIMENTAL API
extern memkind_t MEMKIND_HUGETLB;

/// \warning EXPERIMENTAL API
extern memkind_t MEMKIND_HBW;

/// \warning EXPERIMENTAL API
extern memkind_t MEMKIND_HBW_ALL;

/// \warning EXPERIMENTAL API
extern memkind_t MEMKIND_HBW_PREFERRED;

/// \warning EXPERIMENTAL API
extern memkind_t MEMKIND_HBW_HUGETLB;

/// \warning EXPERIMENTAL API
extern memkind_t MEMKIND_HBW_ALL_HUGETLB;

/// \warning EXPERIMENTAL API
extern memkind_t MEMKIND_HBW_PREFERRED_HUGETLB;

/// \warning EXPERIMENTAL API
extern memkind_t MEMKIND_HBW_INTERLEAVE;

/// \warning EXPERIMENTAL API
extern memkind_t MEMKIND_INTERLEAVE;

///
/// \brief Get Memkind API version
/// \note STANDARD API
/// \return Version number represented by a single integer number(major * 1000000 + minor * 1000 + patch)
///
int memkind_get_version();

///
/// \brief Convert error number into an error message
/// \note STANDARD API
/// \param err error number
/// \param msg error message
/// \param size size of message
///
void memkind_error_message(int err, char *msg, size_t size);

///
/// \brief Create a new PMEM (file-backed) kind of given size on top of a temporary file
///        in the given directory dir
/// \note STANDARD API
/// \param dir path to specified directory to temporary file
/// \param max_size size limit for kind
/// \param kind pointer to kind which will be created
/// \return Memkind operation status, MEMKIND_SUCCESS on success, other values on failure
///
int memkind_create_pmem(const char *dir, size_t max_size, memkind_t *kind);

///
/// \brief Check if kind is available
/// \note STANDARD API
/// \param kind specified memory kind
/// \return Memkind operation status, MEMKIND_SUCCESS on success, other values on failure
///
int memkind_check_available(memkind_t kind);

/* HEAP MANAGEMENT INTERFACE */

///
/// \brief Allocates size bytes of uninitialized storage of the specified kind
/// \note STANDARD API
/// \param kind specified memory kind
/// \param size number of bytes to allocate
/// \return Pointer to the allocated memory
///
void *memkind_malloc(memkind_t kind, size_t size);

///
/// \brief Obtain size of block of memory allocated with the memkind API
/// \note STANDARD API
/// \param kind specified memory kind
/// \param ptr pointer to the allocated memory
/// \return Number of usable bytes
///
size_t memkind_malloc_usable_size(memkind_t kind, void *ptr);

///
/// \brief Allocates memory of the specified kind for an array of num elements
///        of size bytes each and initializes all bytes in the allocated storage to zero
/// \note STANDARD API
/// \param kind specified memory kind
/// \param num number of objects
/// \param size specified size of each element
/// \return Pointer to the allocated memory
///
void *memkind_calloc(memkind_t kind, size_t num, size_t size);

/// \brief Allocates size bytes of the specified kind and places the address of the allocated memory
///        in *memptr. The address of the allocated memory will be a multiple of alignment,
///        which must be a power of two and a multiple of sizeof(void *)
/// \note EXPERIMENTAL API
/// \param kind specified memory kind
/// \param memptr address of the allocated memory
/// \param alignment specified alignment of bytes
/// \param size specified size of bytes
/// \return Memkind operation status, MEMKIND_SUCCESS on success, EINVAL or ENOMEM on failure
///
int memkind_posix_memalign(memkind_t kind, void **memptr, size_t alignment,
                           size_t size);

///
/// \brief Reallocates memory of the specified kind
/// \note STANDARD API
/// \param kind specified memory kind
/// \param ptr pointer to the memory block to be reallocated
/// \param size new size for the memory block in bytes
/// \return Pointer to the allocated memory
///
void *memkind_realloc(memkind_t kind, void *ptr, size_t size);

///
/// \brief Free the memory space of the specified kind pointed by ptr
/// \note STANDARD API
/// \param kind specified memory kind
/// \param ptr pointer to the allocated memory
///
void memkind_free(memkind_t kind, void *ptr);

int memkind_fd(struct memkind *kind);
void memkind_pmem_remapfd(struct memkind *kind, int fdNew);
int memkind_tmpfile(const char *dir, int *fd);
memkind_t memkind_get_kind(void *ptr);

#ifdef __cplusplus
}
#endif
