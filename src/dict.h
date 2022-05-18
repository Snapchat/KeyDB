/* Hash Tables Implementation.
 *
 * This file implements in-memory hash tables with insert/del/replace/find/
 * get-random-element operations. Hash tables will auto-resize if needed
 * tables of power of two in size are used, collisions are handled by
 * chaining. See the source code for more information... :)
 *
 * Copyright (c) 2006-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#ifdef __cplusplus
#include <vector>
#include <atomic>
extern "C" {
#endif

#ifndef __cplusplus
#error "C++ Only"
#endif

#ifndef __DICT_H
#define __DICT_H

#include "mt19937-64.h"
#include <limits.h>
#include <stdint.h>
#include <stdlib.h>

#define DICT_OK 0
#define DICT_ERR 1

/* Unused arguments generate annoying warnings... */
#define DICT_NOTUSED(V) ((void) V)
struct dictAsyncRehashCtl;

typedef struct dictEntry {
    void *key;
    union {
        void *val;
        uint64_t u64;
        int64_t s64;
        double d;
    } v;
    struct dictEntry *next;
} dictEntry;

typedef struct dictType {
    uint64_t (*hashFunction)(const void *key);
    void *(*keyDup)(void *privdata, const void *key);
    void *(*valDup)(void *privdata, const void *obj);
    int (*keyCompare)(void *privdata, const void *key1, const void *key2);
    void (*keyDestructor)(void *privdata, void *key);
    void (*valDestructor)(void *privdata, void *obj);
    int (*expandAllowed)(size_t moreMem, double usedRatio);
    void (*asyncfree)(dictAsyncRehashCtl *);
} dictType;

/* This is our hash table structure. Every dictionary has two of this as we
 * implement incremental rehashing, for the old to the new table. */
typedef struct dictht {
    dictEntry **table;
    unsigned long size;
    unsigned long sizemask;
    unsigned long used;
} dictht;

#ifdef __cplusplus
struct dictAsyncRehashCtl {
    struct workItem {
        dictEntry *de;
        uint64_t hash;
        workItem(dictEntry *de) {
            this->de = de;
        }
    };

    static const int c_targetQueueSize = 512;
    dictEntry *deGCList = nullptr;
    struct dict *dict = nullptr;
    std::vector<workItem> queue;
    size_t hashIdx = 0;
    long rehashIdxBase;
    dictAsyncRehashCtl *next = nullptr;
    std::atomic<bool> done { false };
    std::atomic<bool> abondon { false };

    dictAsyncRehashCtl(struct dict *d, dictAsyncRehashCtl *next);
    dictAsyncRehashCtl(const dictAsyncRehashCtl&) = delete;
    dictAsyncRehashCtl(dictAsyncRehashCtl&&) = delete;
    ~dictAsyncRehashCtl();
};
#else
struct  dictAsyncRehashCtl;
#endif

void discontinueAsyncRehash(dict *d);

typedef struct dict {
    dictType *type;
    void *privdata;
    dictht ht[2];
    long rehashidx; /* rehashing not in progress if rehashidx == -1 */
    unsigned refcount;
    dictAsyncRehashCtl *asyncdata;
    int16_t pauserehash; /* If >0 rehashing is paused (<0 indicates coding error) */
    uint8_t noshrink = false;

#ifdef __cplusplus
    dict() = default;
    dict(dict &) = delete;  // No Copy Ctor

    static void swap(dict& a, dict& b) {
        discontinueAsyncRehash(&a);
        discontinueAsyncRehash(&b);
        std::swap(a.type, b.type);
        std::swap(a.privdata, b.privdata);
        std::swap(a.ht[0], b.ht[0]);
        std::swap(a.ht[1], b.ht[1]);
        std::swap(a.rehashidx, b.rehashidx);
        // Never swap refcount - they are attached to the specific dict obj
        std::swap(a.pauserehash, b.pauserehash);
        std::swap(a.noshrink, b.noshrink);
    }
#endif
} dict;

/* If safe is set to 1 this is a safe iterator, that means, you can call
 * dictAdd, dictFind, and other functions against the dictionary even while
 * iterating. Otherwise it is a non safe iterator, and only dictNext()
 * should be called while iterating. */
typedef struct dictIterator {
    dict *d;
    long index;
    int table, safe;
    dictEntry *entry, *nextEntry;
    /* unsafe iterator fingerprint for misuse detection. */
    long long fingerprint;
} dictIterator;

typedef void (dictScanFunction)(void *privdata, const dictEntry *de);
typedef void (dictScanBucketFunction)(void *privdata, dictEntry **bucketref);

/* This is the initial size of every hash table */
#define DICT_HT_INITIAL_SIZE     4

/* ------------------------------- Macros ------------------------------------*/
#define dictFreeVal(d, entry) \
    if ((d)->type->valDestructor) \
        (d)->type->valDestructor((d)->privdata, (entry)->v.val)

#define dictSetVal(d, entry, _val_) do { \
    if ((d)->type->valDup) \
        (entry)->v.val = (d)->type->valDup((d)->privdata, _val_); \
    else \
        (entry)->v.val = (_val_); \
} while(0)

#define dictSetSignedIntegerVal(entry, _val_) \
    do { (entry)->v.s64 = _val_; } while(0)

#define dictSetUnsignedIntegerVal(entry, _val_) \
    do { (entry)->v.u64 = _val_; } while(0)

#define dictSetDoubleVal(entry, _val_) \
    do { (entry)->v.d = _val_; } while(0)

#define dictFreeKey(d, entry) \
    if ((d)->type->keyDestructor) \
        (d)->type->keyDestructor((d)->privdata, (entry)->key)

#define dictSetKey(d, entry, _key_) do { \
    if ((d)->type->keyDup) \
        (entry)->key = (d)->type->keyDup((d)->privdata, _key_); \
    else \
        (entry)->key = (_key_); \
} while(0)

#define dictCompareKeys(d, key1, key2) \
    (((d)->type->keyCompare) ? \
        (d)->type->keyCompare((d)->privdata, key1, key2) : \
        (key1) == (key2))

#define dictHashKey(d, key) (d)->type->hashFunction(key)
#define dictGetKey(he) ((he)->key)
#define dictGetVal(he) ((he)->v.val)
#define dictGetSignedIntegerVal(he) ((he)->v.s64)
#define dictGetUnsignedIntegerVal(he) ((he)->v.u64)
#define dictGetDoubleVal(he) ((he)->v.d)
#define dictSlots(d) ((d)->ht[0].size+(d)->ht[1].size)
#define dictSize(d) ((d)->ht[0].used+(d)->ht[1].used)
#define dictIsRehashing(d) ((d)->rehashidx != -1)
#define dictPauseRehashing(d) __atomic_fetch_add(&(d)->pauserehash, 1, __ATOMIC_SEQ_CST)
#define dictResumeRehashing(d) __atomic_fetch_sub(&(d)->pauserehash, 1, __ATOMIC_SEQ_CST)

/* If our unsigned long type can store a 64 bit number, use a 64 bit PRNG. */
#if ULONG_MAX >= 0xffffffffffffffff
#define randomULong() ((unsigned long) genrand64_int64())
#else
#define randomULong() random()
#endif

/* API */
dict *dictCreate(dictType *type, void *privDataPtr);
int dictExpand(dict *d, unsigned long size, bool fShrink = false);
int dictTryExpand(dict *d, unsigned long size, bool fShrink);
int dictAdd(dict *d, void *key, void *val, dictEntry **existing = nullptr);
dictEntry *dictAddRaw(dict *d, void *key, dictEntry **existing);
dictEntry *dictAddOrFind(dict *d, void *key);
int dictReplace(dict *d, void *key, void *val);
int dictDelete(dict *d, const void *key);
dictEntry *dictUnlink(dict *ht, const void *key);
void dictFreeUnlinkedEntry(dict *d, dictEntry *he);
void dictRelease(dict *d);
dictEntry * dictFind(dict *d, const void *key);
dictEntry * dictFindWithPrev(dict *d, const void *key, uint64_t h, dictEntry ***dePrevPtr, dictht **ht, bool fShallowCompare = false);
void *dictFetchValue(dict *d, const void *key);
int dictResize(dict *d);
dictIterator *dictGetIterator(dict *d);
dictIterator *dictGetSafeIterator(dict *d);
dictEntry *dictNext(dictIterator *iter);
void dictReleaseIterator(dictIterator *iter);
dictEntry *dictGetRandomKey(dict *d);
dictEntry *dictGetFairRandomKey(dict *d);
unsigned int dictGetSomeKeys(dict *d, dictEntry **des, unsigned int count);
void dictGetStats(char *buf, size_t bufsize, dict *d);
uint64_t dictGenHashFunction(const void *key, int len);
uint64_t dictGenCaseHashFunction(const unsigned char *buf, int len);
void dictEmpty(dict *d, void(callback)(void*));
void dictEnableResize(void);
void dictDisableResize(void);
int dictRehash(dict *d, int n);
int dictRehashMilliseconds(dict *d, int ms);
void dictSetHashFunctionSeed(uint8_t *seed);
uint8_t *dictGetHashFunctionSeed(void);
unsigned long dictScan(dict *d, unsigned long v, dictScanFunction *fn, dictScanBucketFunction *bucketfn, void *privdata);
uint64_t dictGetHash(dict *d, const void *key);
dictEntry **dictFindEntryRefByPtrAndHash(dict *d, const void *oldptr, uint64_t hash);
void dictForceRehash(dict *d);
int dictMerge(dict *dst, dict *src);

/* Async API */
dictAsyncRehashCtl *dictRehashAsyncStart(dict *d, int buckets);
bool dictRehashSomeAsync(dictAsyncRehashCtl *ctl, size_t hashes);
void dictCompleteRehashAsync(dictAsyncRehashCtl *ctl, bool fFree);
void dictRehashAsync(dictAsyncRehashCtl *ctl);

/* Hash table types */
extern dictType dictTypeHeapStringCopyKey;
extern dictType dictTypeHeapStrings;
extern dictType dictTypeHeapStringCopyKeyValue;

#ifdef REDIS_TEST
int dictTest(int argc, char *argv[], int accurate);
#endif

#ifdef __cplusplus
}
#endif

#endif /* __DICT_H */
