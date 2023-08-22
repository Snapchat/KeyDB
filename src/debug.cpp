/*
 * Copyright (c) 2009-2020, Salvatore Sanfilippo <antirez at gmail dot com>
 * Copyright (c) 2020, Redis Labs, Inc
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

#define NO_DEPRECATE_FREE 1 // we are required to call the real free() in this CU
#include "server.h"
#include "sha1.h"   /* SHA1 is used for DEBUG DIGEST */
#include "crc64.h"
#include "cron.h"
#include "bio.h"

#include <arpa/inet.h>
#include <signal.h>
#include <dlfcn.h>
#include <fcntl.h>
#include <unistd.h>

#ifdef HAVE_BACKTRACE
#include <execinfo.h>
#ifndef __OpenBSD__
#include <ucontext.h>
#else
typedef ucontext_t sigcontext_t;
#endif
#include <cxxabi.h>
#endif /* HAVE_BACKTRACE */

//UNW_LOCAL_ONLY being set means we use libunwind for backtraces instead of execinfo
#ifdef UNW_LOCAL_ONLY
#include <libunwind.h>
#include <cxxabi.h>
#endif

#ifdef __CYGWIN__
#ifndef SA_ONSTACK
#define SA_ONSTACK 0x08000000
#endif
#endif

int g_fInCrash = false;

#if defined(__APPLE__) && defined(__arm64__)
#include <mach/mach.h>
#endif

/* Globals */
static int bug_report_start = 0; /* True if bug report header was already logged. */
static pthread_mutex_t bug_report_start_mutex = PTHREAD_MUTEX_INITIALIZER;

/* Forward declarations */
void bugReportStart(void);
void printCrashReport(void);
void bugReportEnd(int killViaSignal, int sig);
void logStackTrace(void *eip, int uplevel);
void getTempFileName(char tmpfile[], int tmpfileNum);

/* ================================= Debugging ============================== */

/* Compute the sha1 of string at 's' with 'len' bytes long.
 * The SHA1 is then xored against the string pointed by digest.
 * Since xor is commutative, this operation is used in order to
 * "add" digests relative to unordered elements.
 *
 * So digest(a,b,c,d) will be the same of digest(b,a,c,d) */
void xorDigest(unsigned char *digest, const void *ptr, size_t len) {
    SHA1_CTX ctx;
    unsigned char hash[20];
    const unsigned char *s = (const unsigned char*)ptr;
    int j;

    SHA1Init(&ctx);
    SHA1Update(&ctx,s,len);
    SHA1Final(hash,&ctx);

    for (j = 0; j < 20; j++)
        digest[j] ^= hash[j];
}

void xorStringObjectDigest(unsigned char *digest, robj *o) {
    o = getDecodedObject(o);
    xorDigest(digest,(const unsigned char*)ptrFromObj(o),sdslen(szFromObj(o)));
    decrRefCount(o);
}

/* This function instead of just computing the SHA1 and xoring it
 * against digest, also perform the digest of "digest" itself and
 * replace the old value with the new one.
 *
 * So the final digest will be:
 *
 * digest = SHA1(digest xor SHA1(data))
 *
 * This function is used every time we want to preserve the order so
 * that digest(a,b,c,d) will be different than digest(b,c,d,a)
 *
 * Also note that mixdigest("foo") followed by mixdigest("bar")
 * will lead to a different digest compared to "fo", "obar".
 */
void mixDigest(unsigned char *digest, const void *ptr, size_t len) {
    SHA1_CTX ctx;
    const char *s = (const char*)ptr;

    xorDigest(digest,s,len);
    SHA1Init(&ctx);
    SHA1Update(&ctx,digest,20);
    SHA1Final(digest,&ctx);
}

void mixStringObjectDigest(unsigned char *digest, robj_roptr o) {
    o = getDecodedObject(o);
    mixDigest(digest,ptrFromObj(o),sdslen(szFromObj(o)));
    decrRefCount(o);
}

/* This function computes the digest of a data structure stored in the
 * object 'o'. It is the core of the DEBUG DIGEST command: when taking the
 * digest of a whole dataset, we take the digest of the key and the value
 * pair, and xor all those together.
 *
 * Note that this function does not reset the initial 'digest' passed, it
 * will continue mixing this object digest to anything that was already
 * present. */
void xorObjectDigest(redisDb *db, robj_roptr keyobj, unsigned char *digest, robj_roptr o) {
    uint32_t aux = htonl(o->type);
    mixDigest(digest,&aux,sizeof(aux));
    std::unique_lock<fastlock> ul(g_expireLock);
    expireEntry *pexpire = db->getExpire(keyobj);
    long long expiretime = INVALID_EXPIRE;
    char buf[128];

    if (pexpire != nullptr)
        pexpire->FGetPrimaryExpire(&expiretime);

    /* Save the key and associated value */
    if (o->type == OBJ_STRING) {
        mixStringObjectDigest(digest,o);
    } else if (o->type == OBJ_LIST) {
        listTypeIterator *li = listTypeInitIterator(o,0,LIST_TAIL);
        listTypeEntry entry;
        while(listTypeNext(li,&entry)) {
            robj *eleobj = listTypeGet(&entry);
            mixStringObjectDigest(digest,eleobj);
            decrRefCount(eleobj);
        }
        listTypeReleaseIterator(li);
    } else if (o->type == OBJ_SET) {
        setTypeIterator *si = setTypeInitIterator(o);
        sds sdsele;
        while((sdsele = setTypeNextObject(si)) != NULL) {
            xorDigest(digest,sdsele,sdslen(sdsele));
            sdsfree(sdsele);
        }
        setTypeReleaseIterator(si);
    } else if (o->type == OBJ_ZSET) {
        unsigned char eledigest[20];

        if (o->encoding == OBJ_ENCODING_ZIPLIST) {
            unsigned char *zl = (unsigned char*)ptrFromObj(o);
            unsigned char *eptr, *sptr;
            unsigned char *vstr;
            unsigned int vlen;
            long long vll;
            double score;

            eptr = ziplistIndex(zl,0);
            serverAssert(eptr != NULL);
            sptr = ziplistNext(zl,eptr);
            serverAssert(sptr != NULL);

            while (eptr != NULL) {
                serverAssert(ziplistGet(eptr,&vstr,&vlen,&vll));
                score = zzlGetScore(sptr);

                memset(eledigest,0,20);
                if (vstr != NULL) {
                    mixDigest(eledigest,vstr,vlen);
                } else {
                    ll2string(buf,sizeof(buf),vll);
                    mixDigest(eledigest,buf,strlen(buf));
                }

                snprintf(buf,sizeof(buf),"%.17g",score);
                mixDigest(eledigest,buf,strlen(buf));
                xorDigest(digest,eledigest,20);
                zzlNext(zl,&eptr,&sptr);
            }
        } else if (o->encoding == OBJ_ENCODING_SKIPLIST) {
            zset *zs = (zset*)ptrFromObj(o);
            dictIterator *di = dictGetIterator(zs->dict);
            dictEntry *de;

            while((de = dictNext(di)) != NULL) {
                sds sdsele = (sds)dictGetKey(de);
                double *score = (double*)dictGetVal(de);

                snprintf(buf,sizeof(buf),"%.17g",*score);
                memset(eledigest,0,20);
                mixDigest(eledigest,sdsele,sdslen(sdsele));
                mixDigest(eledigest,buf,strlen(buf));
                xorDigest(digest,eledigest,20);
            }
            dictReleaseIterator(di);
        } else {
            serverPanic("Unknown sorted set encoding");
        }
    } else if (o->type == OBJ_HASH) {
        hashTypeIterator *hi = hashTypeInitIterator(o);
        while (hashTypeNext(hi) != C_ERR) {
            unsigned char eledigest[20];
            sds sdsele;

            memset(eledigest,0,20);
            sdsele = hashTypeCurrentObjectNewSds(hi,OBJ_HASH_KEY);
            mixDigest(eledigest,sdsele,sdslen(sdsele));
            sdsfree(sdsele);
            sdsele = hashTypeCurrentObjectNewSds(hi,OBJ_HASH_VALUE);
            mixDigest(eledigest,sdsele,sdslen(sdsele));
            sdsfree(sdsele);
            xorDigest(digest,eledigest,20);
        }
        hashTypeReleaseIterator(hi);
    } else if (o->type == OBJ_STREAM) {
        streamIterator si;
        streamIteratorStart(&si,(stream*)ptrFromObj(o),NULL,NULL,0);
        streamID id;
        int64_t numfields;

        while(streamIteratorGetID(&si,&id,&numfields)) {
            sds itemid = sdscatfmt(sdsempty(),"%U.%U",id.ms,id.seq);
            mixDigest(digest,itemid,sdslen(itemid));
            sdsfree(itemid);

            while(numfields--) {
                unsigned char *field, *value;
                int64_t field_len, value_len;
                streamIteratorGetField(&si,&field,&value,
                                           &field_len,&value_len);
                mixDigest(digest,field,field_len);
                mixDigest(digest,value,value_len);
            }
        }
        streamIteratorStop(&si);
    } else if (o->type == OBJ_MODULE) {
        RedisModuleDigest md = {{0},{0}};
        moduleValue *mv = (moduleValue*)ptrFromObj(o);
        moduleType *mt = mv->type;
        moduleInitDigestContext(md);
        if (mt->digest) {
            mt->digest(&md,mv->value);
            xorDigest(digest,md.x,sizeof(md.x));
        }
    } else if (o->type == OBJ_CRON) {
        cronjob *job = (cronjob*)ptrFromObj(o);
        mixDigest(digest, &job->interval, sizeof(job->interval));
        mixDigest(digest, job->script.get(), job->script.size());
    } else {
        serverPanic("Unknown object type");
    }
    /* If the key has an expire, add it to the mix */
    if (expiretime != INVALID_EXPIRE) xorDigest(digest,"!!expire!!",10);
}

/* Compute the dataset digest. Since keys, sets elements, hashes elements
 * are not ordered, we use a trick: every aggregate digest is the xor
 * of the digests of their elements. This way the order will not change
 * the result. For list instead we use a feedback entering the output digest
 * as input in order to ensure that a different ordered list will result in
 * a different digest. */
void computeDatasetDigest(unsigned char *final) {
    int j;
    uint32_t aux;

    memset(final,0,20); /* Start with a clean result */

    for (j = 0; j < cserver.dbnum; j++) {
        redisDb *db = g_pserver->db[j];

        if (db->size() == 0) continue;

        /* hash the DB id, so the same dataset moved in a different
         * DB will lead to a different digest */
        aux = htonl(j);
        mixDigest(final,&aux,sizeof(aux));

        /* Iterate this DB writing every entry */
        db->iterate_threadsafe([final, db](const char *key, robj_roptr o)->bool {
            unsigned char digest[20];
            robj *keyobj;

            memset(digest,0,20); /* This key-val digest */
            keyobj = createStringObject(key,sdslen(key));

            mixDigest(digest,key,sdslen(key));

            xorObjectDigest(db,keyobj,digest,o);

            /* We can finally xor the key-val digest to the final digest */
            xorDigest(final,digest,20);
            decrRefCount(keyobj);
            return true;
        });
    }
}

#ifdef USE_JEMALLOC
void mallctl_int(client *c, robj **argv, int argc) {
    int ret;
    /* start with the biggest size (int64), and if that fails, try smaller sizes (int32, bool) */
    int64_t old = 0, val;
    if (argc > 1) {
        long long ll;
        if (getLongLongFromObjectOrReply(c, argv[1], &ll, NULL) != C_OK)
            return;
        val = ll;
    }
    size_t sz = sizeof(old);
    while (sz > 0) {
        if ((ret=mallctl(szFromObj(argv[0]), &old, &sz, argc > 1? &val: NULL, argc > 1?sz: 0))) {
            if (ret == EPERM && argc > 1) {
                /* if this option is write only, try just writing to it. */
                if (!(ret=mallctl(szFromObj(argv[0]), NULL, 0, &val, sz))) {
                    addReply(c, shared.ok);
                    return;
                }
            }
            if (ret==EINVAL) {
                /* size might be wrong, try a smaller one */
                sz /= 2;
#if BYTE_ORDER == BIG_ENDIAN
                val <<= 8*sz;
#endif
                continue;
            }
            addReplyErrorFormat(c,"%s", strerror(ret));
            return;
        } else {
#if BYTE_ORDER == BIG_ENDIAN
            old >>= 64 - 8*sz;
#endif
            addReplyLongLong(c, old);
            return;
        }
    }
    addReplyErrorFormat(c,"%s", strerror(EINVAL));
}

void mallctl_string(client *c, robj **argv, int argc) {
    int rret, wret;
    char *old;
    size_t sz = sizeof(old);
    /* for strings, it seems we need to first get the old value, before overriding it. */
    if ((rret=mallctl(szFromObj(argv[0]), &old, &sz, NULL, 0))) {
        /* return error unless this option is write only. */
        if (!(rret == EPERM && argc > 1)) {
            addReplyErrorFormat(c,"%s", strerror(rret));
            return;
        }
    }
    if(argc > 1) {
        char *val = szFromObj(argv[1]);
        char **valref = &val;
        if ((!strcmp(val,"VOID")))
            valref = NULL, sz = 0;
        wret = mallctl(szFromObj(argv[0]), NULL, 0, valref, sz);
    }
    if (!rret)
        addReplyBulkCString(c, old);
    else if (wret)
        addReplyErrorFormat(c,"%s", strerror(wret));
    else
        addReply(c, shared.ok);
}
#endif

void debugCommand(client *c) {
    if (c->argc == 2 && !strcasecmp(szFromObj(c->argv[1]),"help")) {
        const char *help[] = {
"AOF-FLUSH-SLEEP <microsec>",
"    Server will sleep before flushing the AOF, this is used for testing.",
"ASSERT",
"    Crash by assertion failed.",
"CHANGE-REPL-ID"
"    Change the replication IDs of the instance.",
"    Dangerous: should be used only for testing the replication subsystem.",
"CONFIG-REWRITE-FORCE-ALL",
"    Like CONFIG REWRITE but writes all configuration options, including",
"    keywords not listed in original configuration file or default values.",
"CRASH-AND-RECOVER <milliseconds>",
"    Hard crash and restart after a <milliseconds> delay.",
"DIGEST",
"    Output a hex signature representing the current DB content.",
"DIGEST-VALUE <key> [<key> ...]",
"    Output a hex signature of the values of all the specified keys.",
"ERROR <string>",
"    Return a Redis protocol error with <string> as message. Useful for clients",
"    unit tests to simulate Redis errors.",
"LOG <message>",
"    Write <message> to the server log.",
"HTSTATS <dbid>",
"    Return hash table statistics of the specified Redis database.",
"HTSTATS-KEY <key>",
"    Like HTSTATS but for the hash table stored at <key>'s value.",
"LOADAOF",
"    Flush the AOF buffers on disk and reload the AOF in memory.",
"LUA-ALWAYS-REPLICATE-COMMANDS <0|1>",
"    Setting it to 1 makes Lua replication defaulting to replicating single",
"    commands, without the script having to enable effects replication.",
#ifdef USE_JEMALLOC
"MALLCTL <key> [<val>]",
"    Get or set a malloc tuning integer.",
"MALLCTL-STR <key> [<val>]",
"    Get or set a malloc tuning string.",
#endif
"OBJECT <key>",
"    Show low level info about `key` and associated value.",
"OOM",
"    Crash the server simulating an out-of-memory error.",
"PANIC",
"    Crash the server simulating a panic.",
"POPULATE <count> [<prefix>] [<size>]",
"    Create <count> string keys named key:<num>. If <prefix> is specified then",
"    it is used instead of the 'key' prefix.",
"DEBUG PROTOCOL <type>",
"    Reply with a test value of the specified type. <type> can be: string,",
"    integer, double, bignum, null, array, set, map, attrib, push, verbatim,",
"    true, false.",
"RELOAD [option ...]",
"    Save the RDB on disk and reload it back to memory. Valid <option> values:",
"    * MERGE: conflicting keys will be loaded from RDB.",
"    * NOFLUSH: the existing database will not be removed before load, but",
"      conflicting keys will generate an exception and kill the server."
"    * NOSAVE: the database will be loaded from an existing RDB file.",
"    Examples:",
"    * DEBUG RELOAD: verify that the server is able to persist, flush and reload",
"      the database.",
"    * DEBUG RELOAD NOSAVE: replace the current database with the contents of an",
"      existing RDB file.",
"    * DEBUG RELOAD NOSAVE NOFLUSH MERGE: add the contents of an existing RDB",
"      file to the database.",
"RESTART",
"    Graceful restart: save config, db, restart.",
"SDSLEN <key>",
"    Show low level SDS string info representing `key` and value.",
"SEGFAULT",
"    Crash the server with sigsegv.",
"SET-ACTIVE-EXPIRE <0|1>",
"    Setting it to 0 disables expiring keys in background when they are not",
"    accessed (otherwise the Redis behavior). Setting it to 1 reenables back the",
"    default.",
"SET-SKIP-CHECKSUM-VALIDATION <0|1>",
"    Enables or disables checksum checks for RDB files and RESTORE's payload.",
"SLEEP <seconds>",
"    Stop the server for <seconds>. Decimals allowed.",
"STRINGMATCH-TEST",
"    Run a fuzz tester against the stringmatchlen() function.",
"STRUCTSIZE",
"    Return the size of different Redis core C structures.",
"ZIPLIST <key>",
"    Show low level info about the ziplist encoding of <key>.",
NULL
        };
        addReplyHelp(c, help);
    } else if (!strcasecmp(szFromObj(c->argv[1]),"segfault")) {
        *((char*)-1) = 'x';
    } else if (!strcasecmp(szFromObj(c->argv[1]),"panic")) {
        serverPanic("DEBUG PANIC called at Unix time %lld", (long long)time(NULL));
    } else if (!strcasecmp(szFromObj(c->argv[1]),"restart") ||
               !strcasecmp(szFromObj(c->argv[1]),"crash-and-recover"))
    {
        long long delay = 0;
        if (c->argc >= 3) {
            if (getLongLongFromObjectOrReply(c, c->argv[2], &delay, NULL)
                != C_OK) return;
            if (delay < 0) delay = 0;
        }
        int flags = !strcasecmp(szFromObj(c->argv[1]),"restart") ?
            (RESTART_SERVER_GRACEFULLY|RESTART_SERVER_CONFIG_REWRITE) :
             RESTART_SERVER_NONE;
        restartServer(flags,delay);
        addReplyError(c,"failed to restart the g_pserver-> Check server logs.");
    } else if (!strcasecmp(szFromObj(c->argv[1]),"oom")) {
        void *ptr = zmalloc(ULONG_MAX, MALLOC_LOCAL); /* Should trigger an out of memory. */
        zfree(ptr);
        addReply(c,shared.ok);
    } else if (!strcasecmp(szFromObj(c->argv[1]),"assert")) {
        serverAssertWithInfo(c,c->argv[0],1 == 2);
    } else if (!strcasecmp(szFromObj(c->argv[1]),"log") && c->argc == 3) {
        serverLog(LL_WARNING, "DEBUG LOG: %s", (char*)ptrFromObj(c->argv[2]));
        addReply(c,shared.ok);
    } else if (!strcasecmp(szFromObj(c->argv[1]),"leak") && c->argc == 3) {
        sdsdup(szFromObj(c->argv[2]));
        addReply(c,shared.ok);
    } else if (!strcasecmp(szFromObj(c->argv[1]),"reload")) {
        int flush = 1, save = 1;
        int flags = RDBFLAGS_NONE;

        /* Parse the additional options that modify the RELOAD
         * behavior. */
        for (int j = 2; j < c->argc; j++) {
            char *opt = szFromObj(c->argv[j]);
            if (!strcasecmp(opt,"MERGE")) {
                flags |= RDBFLAGS_ALLOW_DUP;
            } else if (!strcasecmp(opt,"NOFLUSH")) {
                flush = 0;
            } else if (!strcasecmp(opt,"NOSAVE")) {
                save = 0;
            } else {
                addReplyError(c,"DEBUG RELOAD only supports the "
                                "MERGE, NOFLUSH and NOSAVE options.");
                return;
            }
        }

        /* The default behavior is to save the RDB file before loading
         * it back. */
        if (save) {
            rdbSaveInfo rsi, *rsiptr;
            rsiptr = rdbPopulateSaveInfo(&rsi);
            if (rdbSave(nullptr, rsiptr) != C_OK) {
                addReply(c,shared.err);
                return;
            }
        }

        /* The default behavior is to remove the current dataset from
         * memory before loading the RDB file, however when MERGE is
         * used together with NOFLUSH, we are able to merge two datasets. */
        if (flush) emptyDb(-1,EMPTYDB_NO_FLAGS,NULL);

        protectClient(c);
        int ret = rdbLoadFile(g_pserver->rdb_filename,NULL,flags);
        unprotectClient(c);
        if (ret != C_OK) {
            addReplyError(c,"Error trying to load the RDB dump");
            return;
        }
        serverLog(LL_WARNING,"DB reloaded by DEBUG RELOAD");
        addReply(c,shared.ok);
    } else if (!strcasecmp(szFromObj(c->argv[1]),"loadaof")) {
        if (g_pserver->aof_state != AOF_OFF) flushAppendOnlyFile(1);
        emptyDb(-1,EMPTYDB_NO_FLAGS,NULL);
        protectClient(c);
        int ret = loadAppendOnlyFile(g_pserver->aof_filename);
        unprotectClient(c);
        if (ret != C_OK) {
            addReplyErrorObject(c,shared.err);
            return;
        }
        g_pserver->dirty = 0; /* Prevent AOF / replication */
        serverLog(LL_WARNING,"Append Only File loaded by DEBUG LOADAOF");
        addReply(c,shared.ok);
    } else if (!strcasecmp(szFromObj(c->argv[1]),"object") && c->argc == 3) {
        robj *val;
        const char *strenc;

        val = c->db->find(c->argv[2]);
        if (val == NULL) {
            addReplyErrorObject(c,shared.nokeyerr);
            return;
        }
        strenc = strEncoding(val->encoding);

        char extra[138] = {0};
        if (val->encoding == OBJ_ENCODING_QUICKLIST) {
            char *nextra = extra;
            int remaining = sizeof(extra);
            quicklist *ql = (quicklist*)val->m_ptr;
            /* Add number of quicklist nodes */
            int used = snprintf(nextra, remaining, " ql_nodes:%lu", ql->len);
            nextra += used;
            remaining -= used;
            /* Add average quicklist fill factor */
            double avg = (double)ql->count/ql->len;
            used = snprintf(nextra, remaining, " ql_avg_node:%.2f", avg);
            nextra += used;
            remaining -= used;
            /* Add quicklist fill level / max ziplist size */
            used = snprintf(nextra, remaining, " ql_ziplist_max:%d", ql->fill);
            nextra += used;
            remaining -= used;
            /* Add isCompressed? */
            int compressed = ql->compress != 0;
            used = snprintf(nextra, remaining, " ql_compressed:%d", compressed);
            nextra += used;
            remaining -= used;
            /* Add total uncompressed size */
            unsigned long sz = 0;
            for (quicklistNode *node = ql->head; node; node = node->next) {
                sz += node->sz;
            }
            used = snprintf(nextra, remaining, " ql_uncompressed_size:%lu", sz);
            nextra += used;
            remaining -= used;
        }

        addReplyStatusFormat(c,
            "Value at:%p refcount:%d "
            "encoding:%s serializedlength:%zu "
            "lru:%d lru_seconds_idle:%llu%s",
            (void*)val, static_cast<int>(val->getrefcount(std::memory_order_relaxed)),
            strenc, rdbSavedObjectLen(val, c->argv[2]),
            val->lru, estimateObjectIdleTime(val)/1000, extra);
    } else if (!strcasecmp(szFromObj(c->argv[1]),"sdslen") && c->argc == 3) {
        auto itr = c->db->find(c->argv[2]);
        robj *val = itr.val();
        const char *key = itr.key();

        if (val == NULL) {
            addReplyErrorObject(c,shared.nokeyerr);
            return;
        }

        if (val->type != OBJ_STRING || !sdsEncodedObject(val)) {
            addReplyError(c,"Not an sds encoded string.");
        } else {
            addReplyStatusFormat(c,
                "key_sds_len:%lld, key_sds_avail:%lld, key_zmalloc: %lld, "
                "val_sds_len:%lld, val_sds_avail:%lld, val_zmalloc: %lld",
                (long long) sdslen(key),
                (long long) sdsavail(key),
                (long long) sdsZmallocSize((sds)key),
                (long long) sdslen(szFromObj(val)),
                (long long) sdsavail(szFromObj(val)),
                (long long) getStringObjectSdsUsedMemory(val));
        }
    } else if (!strcasecmp(szFromObj(c->argv[1]),"ziplist") && c->argc == 3) {
        robj_roptr o;

        if ((o = objectCommandLookupOrReply(c,c->argv[2],shared.nokeyerr))
                == nullptr) return;

        if (o->encoding != OBJ_ENCODING_ZIPLIST) {
            addReplyError(c,"Not a ziplist encoded object.");
        } else {
            ziplistRepr((unsigned char*)ptrFromObj(o));
            addReplyStatus(c,"Ziplist structure printed on stdout");
        }
    } else if (!strcasecmp(szFromObj(c->argv[1]),"populate") &&
               c->argc >= 3 && c->argc <= 5) {
        long keys, j;
        robj *key, *val;
        char buf[128];

        if (getPositiveLongFromObjectOrReply(c, c->argv[2], &keys, NULL) != C_OK)
            return;
        
        c->db->expand(keys);
        long valsize = 0;
        if ( c->argc == 5 && getPositiveLongFromObjectOrReply(c, c->argv[4], &valsize, NULL) != C_OK ) 
            return;

        for (j = 0; j < keys; j++) {
            snprintf(buf,sizeof(buf),"%s:%lu",
                (c->argc == 3) ? "key" : (char*)ptrFromObj(c->argv[3]), j);
            key = createStringObject(buf,strlen(buf));
            if (lookupKeyWrite(c->db,key) != NULL) {
                decrRefCount(key);
                continue;
            }
            snprintf(buf,sizeof(buf),"value:%lu",j);
            if (valsize==0)
                val = createStringObject(buf,strlen(buf));
            else {
                int buflen = strlen(buf);
                val = createStringObject(NULL,valsize);
                memcpy(ptrFromObj(val), buf, valsize<=buflen? valsize: buflen);
            }
            dbAdd(c->db,key,val);
            signalModifiedKey(c,c->db,key);
            decrRefCount(key);
        }
        addReply(c,shared.ok);
    } else if (!strcasecmp(szFromObj(c->argv[1]),"digest") && c->argc == 2) {
        /* DEBUG DIGEST (form without keys specified) */
        unsigned char digest[20];
        sds d = sdsempty();

        computeDatasetDigest(digest);
        for (int i = 0; i < 20; i++) d = sdscatprintf(d, "%02x",digest[i]);
        addReplyStatus(c,d);
        sdsfree(d);
    } else if (!strcasecmp(szFromObj(c->argv[1]),"digest-value") && c->argc >= 2) {
        /* DEBUG DIGEST-VALUE key key key ... key. */
        addReplyArrayLen(c,c->argc-2);
        for (int j = 2; j < c->argc; j++) {
            unsigned char digest[20];
            memset(digest,0,20); /* Start with a clean result */

            /* We don't use lookupKey because a debug command should
             * work on logically expired keys */
            auto itr = c->db->find(c->argv[j]);
            robj* o = (robj*)(itr == NULL ? NULL : itr.val());
            if (o) xorObjectDigest(c->db,c->argv[j],digest,o);

            sds d = sdsempty();
            for (int i = 0; i < 20; i++) d = sdscatprintf(d, "%02x",digest[i]);
            addReplyStatus(c,d);
            sdsfree(d);
        }
    } else if (!strcasecmp(szFromObj(c->argv[1]),"protocol") && c->argc == 3) {
        /* DEBUG PROTOCOL [string|integer|double|bignum|null|array|set|map|
         *                 attrib|push|verbatim|true|false] */
        const char *name = szFromObj(c->argv[2]);
        if (!strcasecmp(name,"string")) {
            addReplyBulkCString(c,"Hello World");
        } else if (!strcasecmp(name,"integer")) {
            addReplyLongLong(c,12345);
        } else if (!strcasecmp(name,"double")) {
            addReplyDouble(c,3.14159265359);
        } else if (!strcasecmp(name,"bignum")) {
            addReplyBigNum(c,"1234567999999999999999999999999999999",37);
        } else if (!strcasecmp(name,"null")) {
            addReplyNull(c);
        } else if (!strcasecmp(name,"array")) {
            addReplyArrayLen(c,3);
            for (int j = 0; j < 3; j++) addReplyLongLong(c,j);
        } else if (!strcasecmp(name,"set")) {
            addReplySetLen(c,3);
            for (int j = 0; j < 3; j++) addReplyLongLong(c,j);
        } else if (!strcasecmp(name,"map")) {
            addReplyMapLen(c,3);
            for (int j = 0; j < 3; j++) {
                addReplyLongLong(c,j);
                addReplyBool(c, j == 1);
            }
        } else if (!strcasecmp(name,"attrib")) {
            if (c->resp >= 3) {
                addReplyAttributeLen(c,1);
                addReplyBulkCString(c,"key-popularity");
                addReplyArrayLen(c,2);
                addReplyBulkCString(c,"key:123");
                addReplyLongLong(c,90);
            }
            /* Attributes are not real replies, so a well formed reply should
             * also have a normal reply type after the attribute. */
            addReplyBulkCString(c,"Some real reply following the attribute");
        } else if (!strcasecmp(name,"push")) {
            addReplyPushLen(c,2);
            addReplyBulkCString(c,"server-cpu-usage");
            addReplyLongLong(c,42);
            /* Push replies are not synchronous replies, so we emit also a
             * normal reply in order for blocking clients just discarding the
             * push reply, to actually consume the reply and continue. */
            addReplyBulkCString(c,"Some real reply following the push reply");
        } else if (!strcasecmp(name,"true")) {
            addReplyBool(c,1);
        } else if (!strcasecmp(name,"false")) {
            addReplyBool(c,0);
        } else if (!strcasecmp(name,"verbatim")) {
            addReplyVerbatim(c,"This is a verbatim\nstring",25,"txt");
        } else {
            addReplyError(c,"Wrong protocol type name. Please use one of the following: string|integer|double|bignum|null|array|set|map|attrib|push|verbatim|true|false");
        }
    } else if (!strcasecmp(szFromObj(c->argv[1]),"sleep") && c->argc == 3) {
        double dtime = strtod(szFromObj(c->argv[2]),NULL);
        long long utime = dtime*1000000;
        struct timespec tv;

        tv.tv_sec = utime / 1000000;
        tv.tv_nsec = (utime % 1000000) * 1000;
        nanosleep(&tv, NULL);
        addReply(c,shared.ok);
    } else if (!strcasecmp(szFromObj(c->argv[1]),"set-active-expire") &&
               c->argc == 3)
    {
        g_pserver->active_expire_enabled = atoi(szFromObj(c->argv[2]));
        addReply(c,shared.ok);
    } else if (!strcasecmp(szFromObj(c->argv[1]),"set-skip-checksum-validation") &&
               c->argc == 3)
    {
        cserver.skip_checksum_validation = atoi(szFromObj(c->argv[2]));
        addReply(c,shared.ok);
    } else if (!strcasecmp(szFromObj(c->argv[1]),"aof-flush-sleep") &&
               c->argc == 3)
    {
        g_pserver->aof_flush_sleep = atoi(szFromObj(c->argv[2]));
        addReply(c,shared.ok);
    } else if (!strcasecmp(szFromObj(c->argv[1]),"lua-always-replicate-commands") &&
               c->argc == 3)
    {
        g_pserver->lua_always_replicate_commands = atoi(szFromObj(c->argv[2]));
        addReply(c,shared.ok);
    } else if (!strcasecmp(szFromObj(c->argv[1]),"error") && c->argc == 3) {
        sds errstr = sdsnewlen("-",1);

        errstr = sdscatsds(errstr,szFromObj(c->argv[2]));
        errstr = sdsmapchars(errstr,"\n\r","  ",2); /* no newlines in errors. */
        errstr = sdscatlen(errstr,"\r\n",2);
        addReplySds(c,errstr);
    } else if (!strcasecmp(szFromObj(c->argv[1]),"structsize") && c->argc == 2) {
        sds sizes = sdsempty();
        sizes = sdscatprintf(sizes,"bits:%d ",(sizeof(void*) == 8)?64:32);
        sizes = sdscatprintf(sizes,"robj:%d ",(int)sizeof(robj));
        sizes = sdscatprintf(sizes,"dictentry:%d ",(int)sizeof(dictEntry));
        sizes = sdscatprintf(sizes,"sdshdr5:%d ",(int)sizeof(struct sdshdr5));
        sizes = sdscatprintf(sizes,"sdshdr8:%d ",(int)sizeof(struct sdshdr8));
        sizes = sdscatprintf(sizes,"sdshdr16:%d ",(int)sizeof(struct sdshdr16));
        sizes = sdscatprintf(sizes,"sdshdr32:%d ",(int)sizeof(struct sdshdr32));
        sizes = sdscatprintf(sizes,"sdshdr64:%d ",(int)sizeof(struct sdshdr64));
        addReplyBulkSds(c,sizes);
    } else if (!strcasecmp(szFromObj(c->argv[1]),"htstats") && c->argc == 3) {
        long dbid;
        sds stats = sdsempty();
        char buf[4096];

        if (getLongFromObjectOrReply(c, c->argv[2], &dbid, NULL) != C_OK) {
            sdsfree(stats);
            return;
        }
        if (dbid < 0 || dbid >= cserver.dbnum) {
            sdsfree(stats);
            addReplyError(c,"Out of range database");
            return;
        }

        stats = sdscatprintf(stats,"[Dictionary HT]\n");
        g_pserver->db[dbid]->getStats(buf,sizeof(buf));
        stats = sdscat(stats,buf);

        stats = sdscatprintf(stats,"[Expires set]\n");
        g_pserver->db[dbid]->getExpireStats(buf, sizeof(buf));
        stats = sdscat(stats, buf);

        addReplyVerbatim(c,stats,sdslen(stats),"txt");
        sdsfree(stats);
    } else if (!strcasecmp(szFromObj(c->argv[1]),"htstats-key") && c->argc == 3) {
        robj_roptr o;
        dict *ht = NULL;

        if ((o = objectCommandLookupOrReply(c,c->argv[2],shared.nokeyerr))
                == nullptr) return;

        /* Get the hash table reference from the object, if possible. */
        switch (o->encoding) {
        case OBJ_ENCODING_SKIPLIST:
            {
                zset *zs = (zset*)ptrFromObj(o);
                ht = zs->dict;
            }
            break;
        case OBJ_ENCODING_HT:
            ht = (dict*)ptrFromObj(o);
            break;
        }

        if (ht == NULL) {
            addReplyError(c,"The value stored at the specified key is not "
                            "represented using an hash table");
        } else {
            char buf[4096];
            dictGetStats(buf,sizeof(buf),ht);
            addReplyVerbatim(c,buf,strlen(buf),"txt");
        }
    } else if (!strcasecmp(szFromObj(c->argv[1]),"change-repl-id") && c->argc == 2) {
        serverLog(LL_WARNING,"Changing replication IDs after receiving DEBUG change-repl-id");
        changeReplicationId();
        clearReplicationId2();
        addReply(c,shared.ok);
    } else if (!strcasecmp(szFromObj(c->argv[1]),"stringmatch-test") && c->argc == 2) {
        stringmatchlen_fuzz_test();
        addReplyStatus(c,"Apparently Redis did not crash: test passed");
    } else if (!strcasecmp(szFromObj(c->argv[1]), "force-master") && c->argc == 3) {
        c->flags |= CLIENT_MASTER | CLIENT_MASTER_FORCE_REPLY;
        if (!strcasecmp(szFromObj(c->argv[2]), "yes"))
        {
            redisMaster *mi = (redisMaster*)zcalloc(sizeof(redisMaster), MALLOC_LOCAL);
            mi->master = c;
            listAddNodeHead(g_pserver->masters, mi);
        }
        else if (strcasecmp(szFromObj(c->argv[2]), "flagonly")) // if we didn't set flagonly assume its an unset
        {
            serverAssert(c->flags & CLIENT_MASTER);
            if (listLength(g_pserver->masters))
            {
                redisMaster *mi = (redisMaster*)listNodeValue(listFirst(g_pserver->masters));
                serverAssert(mi->master == c);
                listDelNode(g_pserver->masters, listFirst(g_pserver->masters));
                zfree(mi);
            }
            c->flags &= ~(CLIENT_MASTER | CLIENT_MASTER_FORCE_REPLY);
        }
        addReply(c, shared.ok);
    } else if (!strcasecmp(szFromObj(c->argv[1]), "get-temp-file") && c->argc == 2) {
        char tmpfile[256];
        getTempFileName(tmpfile, g_pserver->rdbThreadVars.tmpfileNum);
        addReplyBulkCString(c, tmpfile);
    } else if (!strcasecmp(szFromObj(c->argv[1]),"truncate-repl-backlog") && c->argc == 2) {
        g_pserver->repl_backlog_idx = 0;
        g_pserver->repl_backlog_off = g_pserver->master_repl_offset+1;
        g_pserver->repl_backlog_histlen = 0;
        if (g_pserver->repl_batch_idxStart >= 0) g_pserver->repl_batch_idxStart = -1;
        if (g_pserver->repl_batch_offStart >= 0) g_pserver->repl_batch_offStart = -1;
        addReply(c, shared.ok);
    } else if (!strcasecmp(szFromObj(c->argv[1]),"config-rewrite-force-all") && c->argc == 2)
    {
        if (rewriteConfig(cserver.configfile, 1) == -1)
            addReplyError(c, "CONFIG-REWRITE-FORCE-ALL failed");
        else
            addReply(c, shared.ok);
    } else if (!strcasecmp(szFromObj(c->argv[1]),"config-rewrite-force-all") && c->argc == 2)
    {
        if (rewriteConfig(cserver.configfile, 1) == -1)
            addReplyError(c, "CONFIG-REWRITE-FORCE-ALL failed");
        else
            addReply(c, shared.ok);
#ifdef USE_JEMALLOC
    } else if(!strcasecmp(szFromObj(c->argv[1]),"mallctl") && c->argc >= 3) {
        mallctl_int(c, c->argv+2, c->argc-2);
        return;
    } else if(!strcasecmp(szFromObj(c->argv[1]),"mallctl-str") && c->argc >= 3) {
        mallctl_string(c, c->argv+2, c->argc-2);
        return;
#endif
    } else {
        addReplySubcommandSyntaxError(c);
        return;
    }
}

/* =========================== Crash handling  ============================== */

void _serverAssert(const char *estr, const char *file, int line) {
    g_fInCrash = true;
    bugReportStart();
    serverLog(LL_WARNING,"=== ASSERTION FAILED ===");
    serverLog(LL_WARNING,"==> %s:%d '%s' is not true",file,line,estr);

    if (g_pserver->crashlog_enabled) {
#if defined HAVE_BACKTRACE || defined UNW_LOCAL_ONLY
        logStackTrace(NULL, 1);
#endif
        printCrashReport();
    }

    // remove the signal handler so on abort() we will output the crash report.
    removeSignalHandlers();
    bugReportEnd(0, 0);
}

void _serverAssertPrintClientInfo(const client *c) {
    int j;
    char conninfo[CONN_INFO_LEN];

    bugReportStart();
    serverLog(LL_WARNING,"=== ASSERTION FAILED CLIENT CONTEXT ===");
    serverLog(LL_WARNING,"client->flags = %llu", (unsigned long long) c->flags);
    serverLog(LL_WARNING,"client->conn = %s", connGetInfo(c->conn, conninfo, sizeof(conninfo)));
    serverLog(LL_WARNING,"client->argc = %d", c->argc);
    for (j=0; j < c->argc; j++) {
        char buf[128];
        char *arg;

        if (c->argv[j]->type == OBJ_STRING && sdsEncodedObject(c->argv[j])) {
            arg = (char*) ptrFromObj(c->argv[j]);
        } else {
            snprintf(buf,sizeof(buf),"Object type: %u, encoding: %u",
                c->argv[j]->type, c->argv[j]->encoding);
            arg = buf;
        }
        serverLog(LL_WARNING,"client->argv[%d] = \"%s\" (refcount: %d)",
            j, arg, static_cast<int>(c->argv[j]->getrefcount(std::memory_order_relaxed)));
    }
}

void serverLogObjectDebugInfo(robj_roptr o) {
    serverLog(LL_WARNING,"Object type: %d", o->type);
    serverLog(LL_WARNING,"Object encoding: %d", o->encoding);
    serverLog(LL_WARNING,"Object refcount: %d", static_cast<int>(o->getrefcount(std::memory_order_relaxed)));
#if UNSAFE_CRASH_REPORT
    /* This code is now disabled. o->ptr may be unreliable to print. in some
     * cases a ziplist could have already been freed by realloc, but not yet
     * updated to o->ptr. in other cases the call to ziplistLen may need to
     * iterate on all the items in the list (and possibly crash again).
     * For some cases it may be ok to crash here again, but these could cause
     * invalid memory access which will bother valgrind and also possibly cause
     * random memory portion to be "leaked" into the logfile. */
    if (o->type == OBJ_STRING && sdsEncodedObject(o)) {
        serverLog(LL_WARNING,"Object raw string len: %zu", sdslen(szFromObj(o)));
        if (sdslen(szFromObj(o)) < 4096) {
            sds repr = sdscatrepr(sdsempty(),szFromObj(o),sdslen(szFromObj(o)));
            serverLog(LL_WARNING,"Object raw string content: %s", repr);
            sdsfree(repr);
        }
    } else if (o->type == OBJ_LIST) {
        serverLog(LL_WARNING,"List length: %d", (int) listTypeLength(o));
    } else if (o->type == OBJ_SET) {
        serverLog(LL_WARNING,"Set size: %d", (int) setTypeSize(o));
    } else if (o->type == OBJ_HASH) {
        serverLog(LL_WARNING,"Hash size: %d", (int) hashTypeLength(o));
    } else if (o->type == OBJ_ZSET) {
        serverLog(LL_WARNING,"Sorted set size: %d", (int) zsetLength(o));
        if (o->encoding == OBJ_ENCODING_SKIPLIST)
            serverLog(LL_WARNING,"Skiplist level: %d", (int) ((const zset*)ptrFromObj(o))->zsl->level);
    } else if (o->type == OBJ_STREAM) {
        serverLog(LL_WARNING,"Stream size: %d", (int) streamLength(o));
    }
#endif
}

void _serverAssertPrintObject(robj_roptr o) {
    bugReportStart();
    serverLog(LL_WARNING,"=== ASSERTION FAILED OBJECT CONTEXT ===");
    serverLogObjectDebugInfo(o);
}

void _serverAssertWithInfo(const client *c, robj_roptr o, const char *estr, const char *file, int line) {
    if (c) _serverAssertPrintClientInfo(c);
    if (o) _serverAssertPrintObject(o);
    _serverAssert(estr,file,line);
}

void _serverPanic(const char *file, int line, const char *msg, ...) {
    g_fInCrash = true;
    va_list ap;
    va_start(ap,msg);
    char fmtmsg[256];
    vsnprintf(fmtmsg,sizeof(fmtmsg),msg,ap);
    va_end(ap);

    bugReportStart();
    serverLog(LL_WARNING,"------------------------------------------------");
    serverLog(LL_WARNING,"!!! Software Failure. Press left mouse button to continue");
    serverLog(LL_WARNING,"Guru Meditation: %s #%s:%d",fmtmsg,file,line);

    if (g_pserver->crashlog_enabled) {
#if defined HAVE_BACKTRACE || defined UNW_LOCAL_ONLY
        logStackTrace(NULL, 1);
#endif
        printCrashReport();
    }

    // remove the signal handler so on abort() we will output the crash report.
    removeSignalHandlers();
    bugReportEnd(0, 0);
}

void bugReportStart(void) {
    pthread_mutex_lock(&bug_report_start_mutex);
    if (bug_report_start == 0) {
        serverLogRaw(LL_WARNING|LL_RAW,
        "\n\n=== KEYDB BUG REPORT START: Cut & paste starting from here ===\n");
        bug_report_start = 1;
    }
    pthread_mutex_unlock(&bug_report_start_mutex);
}

#ifdef HAVE_BACKTRACE
static void *getMcontextEip(ucontext_t *uc) {
#if defined(__APPLE__) && !defined(MAC_OS_X_VERSION_10_6)
    /* OSX < 10.6 */
    #if defined(__x86_64__)
    return (void*) uc->uc_mcontext->__ss.__rip;
    #elif defined(__i386__)
    return (void*) uc->uc_mcontext->__ss.__eip;
    #else
    return (void*) uc->uc_mcontext->__ss.__srr0;
    #endif
#elif defined(__APPLE__) && defined(MAC_OS_X_VERSION_10_6)
    /* OSX >= 10.6 */
    #if defined(_STRUCT_X86_THREAD_STATE64) && !defined(__i386__)
    return (void*) uc->uc_mcontext->__ss.__rip;
    #elif defined(__i386__)
    return (void*) uc->uc_mcontext->__ss.__eip;
    #else
    /* OSX ARM64 */
    return (void*) arm_thread_state64_get_pc(uc->uc_mcontext->__ss);
    #endif
#elif defined(__linux__)
    /* Linux */
    #if defined(__i386__) || ((defined(__X86_64__) || defined(__x86_64__)) && defined(__ILP32__))
    return (void*) uc->uc_mcontext.gregs[14]; /* Linux 32 */
    #elif defined(__X86_64__) || defined(__x86_64__)
    return (void*) uc->uc_mcontext.gregs[16]; /* Linux 64 */
    #elif defined(__ia64__) /* Linux IA64 */
    return (void*) uc->uc_mcontext.sc_ip;
    #elif defined(__arm__) /* Linux ARM */
    return (void*) uc->uc_mcontext.arm_pc;
    #elif defined(__aarch64__) /* Linux AArch64 */
    return (void*) uc->uc_mcontext.pc;
    #endif
#elif defined(__FreeBSD__)
    /* FreeBSD */
    #if defined(__i386__)
    return (void*) uc->uc_mcontext.mc_eip;
    #elif defined(__x86_64__)
    return (void*) uc->uc_mcontext.mc_rip;
    #endif
#elif defined(__OpenBSD__)
    /* OpenBSD */
    #if defined(__i386__)
    return (void*) uc->sc_eip;
    #elif defined(__x86_64__)
    return (void*) uc->sc_rip;
    #endif
#elif defined(__NetBSD__)
    #if defined(__i386__)
    return (void*) uc->uc_mcontext.__gregs[_REG_EIP];
    #elif defined(__x86_64__)
    return (void*) uc->uc_mcontext.__gregs[_REG_RIP];
    #endif
#elif defined(__DragonFly__)
    return (void*) uc->uc_mcontext.mc_rip;
#else
    return NULL;
#endif
}

void logStackContent(void **sp) {
    int i;
    for (i = 15; i >= 0; i--) {
        unsigned long addr = (unsigned long) sp+i;
        unsigned long val = (unsigned long) sp[i];

        if (sizeof(long) == 4)
            serverLog(LL_WARNING, "(%08lx) -> %08lx", addr, val);
        else
            serverLog(LL_WARNING, "(%016lx) -> %016lx", addr, val);
    }
}

/* Log dump of processor registers */
void logRegisters(ucontext_t *uc) {
    serverLog(LL_WARNING|LL_RAW, "\n------ REGISTERS ------\n");

/* OSX */
#if defined(__APPLE__) && defined(MAC_OS_X_VERSION_10_6)
  /* OSX AMD64 */
    #if defined(_STRUCT_X86_THREAD_STATE64) && !defined(__i386__)
    serverLog(LL_WARNING,
    "\n"
    "RAX:%016lx RBX:%016lx\nRCX:%016lx RDX:%016lx\n"
    "RDI:%016lx RSI:%016lx\nRBP:%016lx RSP:%016lx\n"
    "R8 :%016lx R9 :%016lx\nR10:%016lx R11:%016lx\n"
    "R12:%016lx R13:%016lx\nR14:%016lx R15:%016lx\n"
    "RIP:%016lx EFL:%016lx\nCS :%016lx FS:%016lx  GS:%016lx",
        (unsigned long) uc->uc_mcontext->__ss.__rax,
        (unsigned long) uc->uc_mcontext->__ss.__rbx,
        (unsigned long) uc->uc_mcontext->__ss.__rcx,
        (unsigned long) uc->uc_mcontext->__ss.__rdx,
        (unsigned long) uc->uc_mcontext->__ss.__rdi,
        (unsigned long) uc->uc_mcontext->__ss.__rsi,
        (unsigned long) uc->uc_mcontext->__ss.__rbp,
        (unsigned long) uc->uc_mcontext->__ss.__rsp,
        (unsigned long) uc->uc_mcontext->__ss.__r8,
        (unsigned long) uc->uc_mcontext->__ss.__r9,
        (unsigned long) uc->uc_mcontext->__ss.__r10,
        (unsigned long) uc->uc_mcontext->__ss.__r11,
        (unsigned long) uc->uc_mcontext->__ss.__r12,
        (unsigned long) uc->uc_mcontext->__ss.__r13,
        (unsigned long) uc->uc_mcontext->__ss.__r14,
        (unsigned long) uc->uc_mcontext->__ss.__r15,
        (unsigned long) uc->uc_mcontext->__ss.__rip,
        (unsigned long) uc->uc_mcontext->__ss.__rflags,
        (unsigned long) uc->uc_mcontext->__ss.__cs,
        (unsigned long) uc->uc_mcontext->__ss.__fs,
        (unsigned long) uc->uc_mcontext->__ss.__gs
    );
    logStackContent((void**)uc->uc_mcontext->__ss.__rsp);
    #elif defined(__i386__)
    /* OSX x86 */
    serverLog(LL_WARNING,
    "\n"
    "EAX:%08lx EBX:%08lx ECX:%08lx EDX:%08lx\n"
    "EDI:%08lx ESI:%08lx EBP:%08lx ESP:%08lx\n"
    "SS:%08lx  EFL:%08lx EIP:%08lx CS :%08lx\n"
    "DS:%08lx  ES:%08lx  FS :%08lx GS :%08lx",
        (unsigned long) uc->uc_mcontext->__ss.__eax,
        (unsigned long) uc->uc_mcontext->__ss.__ebx,
        (unsigned long) uc->uc_mcontext->__ss.__ecx,
        (unsigned long) uc->uc_mcontext->__ss.__edx,
        (unsigned long) uc->uc_mcontext->__ss.__edi,
        (unsigned long) uc->uc_mcontext->__ss.__esi,
        (unsigned long) uc->uc_mcontext->__ss.__ebp,
        (unsigned long) uc->uc_mcontext->__ss.__esp,
        (unsigned long) uc->uc_mcontext->__ss.__ss,
        (unsigned long) uc->uc_mcontext->__ss.__eflags,
        (unsigned long) uc->uc_mcontext->__ss.__eip,
        (unsigned long) uc->uc_mcontext->__ss.__cs,
        (unsigned long) uc->uc_mcontext->__ss.__ds,
        (unsigned long) uc->uc_mcontext->__ss.__es,
        (unsigned long) uc->uc_mcontext->__ss.__fs,
        (unsigned long) uc->uc_mcontext->__ss.__gs
    );
    logStackContent((void**)uc->uc_mcontext->__ss.__esp);
    #else
    /* OSX ARM64 */
    serverLog(LL_WARNING,
    "\n"
    "x0:%016lx x1:%016lx x2:%016lx x3:%016lx\n"
    "x4:%016lx x5:%016lx x6:%016lx x7:%016lx\n"
    "x8:%016lx x9:%016lx x10:%016lx x11:%016lx\n"
    "x12:%016lx x13:%016lx x14:%016lx x15:%016lx\n"
    "x16:%016lx x17:%016lx x18:%016lx x19:%016lx\n"
    "x20:%016lx x21:%016lx x22:%016lx x23:%016lx\n"
    "x24:%016lx x25:%016lx x26:%016lx x27:%016lx\n"
    "x28:%016lx fp:%016lx lr:%016lx\n"
    "sp:%016lx pc:%016lx cpsr:%08lx\n",
        (unsigned long) uc->uc_mcontext->__ss.__x[0],
        (unsigned long) uc->uc_mcontext->__ss.__x[1],
        (unsigned long) uc->uc_mcontext->__ss.__x[2],
        (unsigned long) uc->uc_mcontext->__ss.__x[3],
        (unsigned long) uc->uc_mcontext->__ss.__x[4],
        (unsigned long) uc->uc_mcontext->__ss.__x[5],
        (unsigned long) uc->uc_mcontext->__ss.__x[6],
        (unsigned long) uc->uc_mcontext->__ss.__x[7],
        (unsigned long) uc->uc_mcontext->__ss.__x[8],
        (unsigned long) uc->uc_mcontext->__ss.__x[9],
        (unsigned long) uc->uc_mcontext->__ss.__x[10],
        (unsigned long) uc->uc_mcontext->__ss.__x[11],
        (unsigned long) uc->uc_mcontext->__ss.__x[12],
        (unsigned long) uc->uc_mcontext->__ss.__x[13],
        (unsigned long) uc->uc_mcontext->__ss.__x[14],
        (unsigned long) uc->uc_mcontext->__ss.__x[15],
        (unsigned long) uc->uc_mcontext->__ss.__x[16],
        (unsigned long) uc->uc_mcontext->__ss.__x[17],
        (unsigned long) uc->uc_mcontext->__ss.__x[18],
        (unsigned long) uc->uc_mcontext->__ss.__x[19],
        (unsigned long) uc->uc_mcontext->__ss.__x[20],
        (unsigned long) uc->uc_mcontext->__ss.__x[21],
        (unsigned long) uc->uc_mcontext->__ss.__x[22],
        (unsigned long) uc->uc_mcontext->__ss.__x[23],
        (unsigned long) uc->uc_mcontext->__ss.__x[24],
        (unsigned long) uc->uc_mcontext->__ss.__x[25],
        (unsigned long) uc->uc_mcontext->__ss.__x[26],
        (unsigned long) uc->uc_mcontext->__ss.__x[27],
        (unsigned long) uc->uc_mcontext->__ss.__x[28],
        (unsigned long) arm_thread_state64_get_fp(uc->uc_mcontext->__ss),
        (unsigned long) arm_thread_state64_get_lr(uc->uc_mcontext->__ss),
        (unsigned long) arm_thread_state64_get_sp(uc->uc_mcontext->__ss),
        (unsigned long) arm_thread_state64_get_pc(uc->uc_mcontext->__ss),
        (unsigned long) uc->uc_mcontext->__ss.__cpsr
    );
    logStackContent((void**) arm_thread_state64_get_sp(uc->uc_mcontext->__ss));
    #endif
/* Linux */
#elif defined(__linux__)
    /* Linux x86 */
    #if defined(__i386__) || ((defined(__X86_64__) || defined(__x86_64__)) && defined(__ILP32__))
    serverLog(LL_WARNING,
    "\n"
    "EAX:%08lx EBX:%08lx ECX:%08lx EDX:%08lx\n"
    "EDI:%08lx ESI:%08lx EBP:%08lx ESP:%08lx\n"
    "SS :%08lx EFL:%08lx EIP:%08lx CS:%08lx\n"
    "DS :%08lx ES :%08lx FS :%08lx GS:%08lx",
        (unsigned long) uc->uc_mcontext.gregs[11],
        (unsigned long) uc->uc_mcontext.gregs[8],
        (unsigned long) uc->uc_mcontext.gregs[10],
        (unsigned long) uc->uc_mcontext.gregs[9],
        (unsigned long) uc->uc_mcontext.gregs[4],
        (unsigned long) uc->uc_mcontext.gregs[5],
        (unsigned long) uc->uc_mcontext.gregs[6],
        (unsigned long) uc->uc_mcontext.gregs[7],
        (unsigned long) uc->uc_mcontext.gregs[18],
        (unsigned long) uc->uc_mcontext.gregs[17],
        (unsigned long) uc->uc_mcontext.gregs[14],
        (unsigned long) uc->uc_mcontext.gregs[15],
        (unsigned long) uc->uc_mcontext.gregs[3],
        (unsigned long) uc->uc_mcontext.gregs[2],
        (unsigned long) uc->uc_mcontext.gregs[1],
        (unsigned long) uc->uc_mcontext.gregs[0]
    );
    logStackContent((void**)uc->uc_mcontext.gregs[7]);
    #elif defined(__X86_64__) || defined(__x86_64__)
    /* Linux AMD64 */
    serverLog(LL_WARNING,
    "\n"
    "RAX:%016lx RBX:%016lx\nRCX:%016lx RDX:%016lx\n"
    "RDI:%016lx RSI:%016lx\nRBP:%016lx RSP:%016lx\n"
    "R8 :%016lx R9 :%016lx\nR10:%016lx R11:%016lx\n"
    "R12:%016lx R13:%016lx\nR14:%016lx R15:%016lx\n"
    "RIP:%016lx EFL:%016lx\nCSGSFS:%016lx",
        (unsigned long) uc->uc_mcontext.gregs[13],
        (unsigned long) uc->uc_mcontext.gregs[11],
        (unsigned long) uc->uc_mcontext.gregs[14],
        (unsigned long) uc->uc_mcontext.gregs[12],
        (unsigned long) uc->uc_mcontext.gregs[8],
        (unsigned long) uc->uc_mcontext.gregs[9],
        (unsigned long) uc->uc_mcontext.gregs[10],
        (unsigned long) uc->uc_mcontext.gregs[15],
        (unsigned long) uc->uc_mcontext.gregs[0],
        (unsigned long) uc->uc_mcontext.gregs[1],
        (unsigned long) uc->uc_mcontext.gregs[2],
        (unsigned long) uc->uc_mcontext.gregs[3],
        (unsigned long) uc->uc_mcontext.gregs[4],
        (unsigned long) uc->uc_mcontext.gregs[5],
        (unsigned long) uc->uc_mcontext.gregs[6],
        (unsigned long) uc->uc_mcontext.gregs[7],
        (unsigned long) uc->uc_mcontext.gregs[16],
        (unsigned long) uc->uc_mcontext.gregs[17],
        (unsigned long) uc->uc_mcontext.gregs[18]
    );
    logStackContent((void**)uc->uc_mcontext.gregs[15]);
    #elif defined(__aarch64__) /* Linux AArch64 */
    serverLog(LL_WARNING,
	      "\n"
	      "X18:%016lx X19:%016lx\nX20:%016lx X21:%016lx\n"
	      "X22:%016lx X23:%016lx\nX24:%016lx X25:%016lx\n"
	      "X26:%016lx X27:%016lx\nX28:%016lx X29:%016lx\n"
	      "X30:%016lx\n"
	      "pc:%016lx sp:%016lx\npstate:%016lx fault_address:%016lx\n",
	      (unsigned long) uc->uc_mcontext.regs[18],
	      (unsigned long) uc->uc_mcontext.regs[19],
	      (unsigned long) uc->uc_mcontext.regs[20],
	      (unsigned long) uc->uc_mcontext.regs[21],
	      (unsigned long) uc->uc_mcontext.regs[22],
	      (unsigned long) uc->uc_mcontext.regs[23],
	      (unsigned long) uc->uc_mcontext.regs[24],
	      (unsigned long) uc->uc_mcontext.regs[25],
	      (unsigned long) uc->uc_mcontext.regs[26],
	      (unsigned long) uc->uc_mcontext.regs[27],
	      (unsigned long) uc->uc_mcontext.regs[28],
	      (unsigned long) uc->uc_mcontext.regs[29],
	      (unsigned long) uc->uc_mcontext.regs[30],
	      (unsigned long) uc->uc_mcontext.pc,
	      (unsigned long) uc->uc_mcontext.sp,
	      (unsigned long) uc->uc_mcontext.pstate,
	      (unsigned long) uc->uc_mcontext.fault_address
		      );
	      logStackContent((void**)uc->uc_mcontext.sp);
    #elif defined(__arm__) /* Linux ARM */
    serverLog(LL_WARNING,
	      "\n"
	      "R10:%016lx R9 :%016lx\nR8 :%016lx R7 :%016lx\n"
	      "R6 :%016lx R5 :%016lx\nR4 :%016lx R3 :%016lx\n"
	      "R2 :%016lx R1 :%016lx\nR0 :%016lx EC :%016lx\n"
	      "fp: %016lx ip:%016lx\n"
	      "pc:%016lx sp:%016lx\ncpsr:%016lx fault_address:%016lx\n",
	      (unsigned long) uc->uc_mcontext.arm_r10,
	      (unsigned long) uc->uc_mcontext.arm_r9,
	      (unsigned long) uc->uc_mcontext.arm_r8,
	      (unsigned long) uc->uc_mcontext.arm_r7,
	      (unsigned long) uc->uc_mcontext.arm_r6,
	      (unsigned long) uc->uc_mcontext.arm_r5,
	      (unsigned long) uc->uc_mcontext.arm_r4,
	      (unsigned long) uc->uc_mcontext.arm_r3,
	      (unsigned long) uc->uc_mcontext.arm_r2,
	      (unsigned long) uc->uc_mcontext.arm_r1,
	      (unsigned long) uc->uc_mcontext.arm_r0,
	      (unsigned long) uc->uc_mcontext.error_code,
	      (unsigned long) uc->uc_mcontext.arm_fp,
	      (unsigned long) uc->uc_mcontext.arm_ip,
	      (unsigned long) uc->uc_mcontext.arm_pc,
	      (unsigned long) uc->uc_mcontext.arm_sp,
	      (unsigned long) uc->uc_mcontext.arm_cpsr,
	      (unsigned long) uc->uc_mcontext.fault_address
		      );
	      logStackContent((void**)uc->uc_mcontext.arm_sp);
    #endif
#elif defined(__FreeBSD__)
    #if defined(__x86_64__)
    serverLog(LL_WARNING,
    "\n"
    "RAX:%016lx RBX:%016lx\nRCX:%016lx RDX:%016lx\n"
    "RDI:%016lx RSI:%016lx\nRBP:%016lx RSP:%016lx\n"
    "R8 :%016lx R9 :%016lx\nR10:%016lx R11:%016lx\n"
    "R12:%016lx R13:%016lx\nR14:%016lx R15:%016lx\n"
    "RIP:%016lx EFL:%016lx\nCSGSFS:%016lx",
        (unsigned long) uc->uc_mcontext.mc_rax,
        (unsigned long) uc->uc_mcontext.mc_rbx,
        (unsigned long) uc->uc_mcontext.mc_rcx,
        (unsigned long) uc->uc_mcontext.mc_rdx,
        (unsigned long) uc->uc_mcontext.mc_rdi,
        (unsigned long) uc->uc_mcontext.mc_rsi,
        (unsigned long) uc->uc_mcontext.mc_rbp,
        (unsigned long) uc->uc_mcontext.mc_rsp,
        (unsigned long) uc->uc_mcontext.mc_r8,
        (unsigned long) uc->uc_mcontext.mc_r9,
        (unsigned long) uc->uc_mcontext.mc_r10,
        (unsigned long) uc->uc_mcontext.mc_r11,
        (unsigned long) uc->uc_mcontext.mc_r12,
        (unsigned long) uc->uc_mcontext.mc_r13,
        (unsigned long) uc->uc_mcontext.mc_r14,
        (unsigned long) uc->uc_mcontext.mc_r15,
        (unsigned long) uc->uc_mcontext.mc_rip,
        (unsigned long) uc->uc_mcontext.mc_rflags,
        (unsigned long) uc->uc_mcontext.mc_cs
    );
    logStackContent((void**)uc->uc_mcontext.mc_rsp);
    #elif defined(__i386__)
    serverLog(LL_WARNING,
    "\n"
    "EAX:%08lx EBX:%08lx ECX:%08lx EDX:%08lx\n"
    "EDI:%08lx ESI:%08lx EBP:%08lx ESP:%08lx\n"
    "SS :%08lx EFL:%08lx EIP:%08lx CS:%08lx\n"
    "DS :%08lx ES :%08lx FS :%08lx GS:%08lx",
        (unsigned long) uc->uc_mcontext.mc_eax,
        (unsigned long) uc->uc_mcontext.mc_ebx,
        (unsigned long) uc->uc_mcontext.mc_ebx,
        (unsigned long) uc->uc_mcontext.mc_edx,
        (unsigned long) uc->uc_mcontext.mc_edi,
        (unsigned long) uc->uc_mcontext.mc_esi,
        (unsigned long) uc->uc_mcontext.mc_ebp,
        (unsigned long) uc->uc_mcontext.mc_esp,
        (unsigned long) uc->uc_mcontext.mc_ss,
        (unsigned long) uc->uc_mcontext.mc_eflags,
        (unsigned long) uc->uc_mcontext.mc_eip,
        (unsigned long) uc->uc_mcontext.mc_cs,
        (unsigned long) uc->uc_mcontext.mc_es,
        (unsigned long) uc->uc_mcontext.mc_fs,
        (unsigned long) uc->uc_mcontext.mc_gs
    );
    logStackContent((void**)uc->uc_mcontext.mc_esp);
    #endif
#elif defined(__OpenBSD__)
    #if defined(__x86_64__)
    serverLog(LL_WARNING,
    "\n"
    "RAX:%016lx RBX:%016lx\nRCX:%016lx RDX:%016lx\n"
    "RDI:%016lx RSI:%016lx\nRBP:%016lx RSP:%016lx\n"
    "R8 :%016lx R9 :%016lx\nR10:%016lx R11:%016lx\n"
    "R12:%016lx R13:%016lx\nR14:%016lx R15:%016lx\n"
    "RIP:%016lx EFL:%016lx\nCSGSFS:%016lx",
        (unsigned long) uc->sc_rax,
        (unsigned long) uc->sc_rbx,
        (unsigned long) uc->sc_rcx,
        (unsigned long) uc->sc_rdx,
        (unsigned long) uc->sc_rdi,
        (unsigned long) uc->sc_rsi,
        (unsigned long) uc->sc_rbp,
        (unsigned long) uc->sc_rsp,
        (unsigned long) uc->sc_r8,
        (unsigned long) uc->sc_r9,
        (unsigned long) uc->sc_r10,
        (unsigned long) uc->sc_r11,
        (unsigned long) uc->sc_r12,
        (unsigned long) uc->sc_r13,
        (unsigned long) uc->sc_r14,
        (unsigned long) uc->sc_r15,
        (unsigned long) uc->sc_rip,
        (unsigned long) uc->sc_rflags,
        (unsigned long) uc->sc_cs
    );
    logStackContent((void**)uc->sc_rsp);
    #elif defined(__i386__)
    serverLog(LL_WARNING,
    "\n"
    "EAX:%08lx EBX:%08lx ECX:%08lx EDX:%08lx\n"
    "EDI:%08lx ESI:%08lx EBP:%08lx ESP:%08lx\n"
    "SS :%08lx EFL:%08lx EIP:%08lx CS:%08lx\n"
    "DS :%08lx ES :%08lx FS :%08lx GS:%08lx",
        (unsigned long) uc->sc_eax,
        (unsigned long) uc->sc_ebx,
        (unsigned long) uc->sc_ebx,
        (unsigned long) uc->sc_edx,
        (unsigned long) uc->sc_edi,
        (unsigned long) uc->sc_esi,
        (unsigned long) uc->sc_ebp,
        (unsigned long) uc->sc_esp,
        (unsigned long) uc->sc_ss,
        (unsigned long) uc->sc_eflags,
        (unsigned long) uc->sc_eip,
        (unsigned long) uc->sc_cs,
        (unsigned long) uc->sc_es,
        (unsigned long) uc->sc_fs,
        (unsigned long) uc->sc_gs
    );
    logStackContent((void**)uc->sc_esp);
    #endif
#elif defined(__NetBSD__)
    #if defined(__x86_64__)
    serverLog(LL_WARNING,
    "\n"
    "RAX:%016lx RBX:%016lx\nRCX:%016lx RDX:%016lx\n"
    "RDI:%016lx RSI:%016lx\nRBP:%016lx RSP:%016lx\n"
    "R8 :%016lx R9 :%016lx\nR10:%016lx R11:%016lx\n"
    "R12:%016lx R13:%016lx\nR14:%016lx R15:%016lx\n"
    "RIP:%016lx EFL:%016lx\nCSGSFS:%016lx",
        (unsigned long) uc->uc_mcontext.__gregs[_REG_RAX],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_RBX],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_RCX],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_RDX],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_RDI],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_RSI],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_RBP],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_RSP],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_R8],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_R9],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_R10],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_R11],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_R12],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_R13],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_R14],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_R15],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_RIP],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_RFLAGS],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_CS]
    );
    logStackContent((void**)uc->uc_mcontext.__gregs[_REG_RSP]);
    #elif defined(__i386__)
    serverLog(LL_WARNING,
    "\n"
    "EAX:%08lx EBX:%08lx ECX:%08lx EDX:%08lx\n"
    "EDI:%08lx ESI:%08lx EBP:%08lx ESP:%08lx\n"
    "SS :%08lx EFL:%08lx EIP:%08lx CS:%08lx\n"
    "DS :%08lx ES :%08lx FS :%08lx GS:%08lx",
        (unsigned long) uc->uc_mcontext.__gregs[_REG_EAX],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_EBX],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_EDX],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_EDI],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_ESI],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_EBP],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_ESP],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_SS],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_EFLAGS],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_EIP],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_CS],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_ES],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_FS],
        (unsigned long) uc->uc_mcontext.__gregs[_REG_GS]
    );
    #endif
#elif defined(__DragonFly__)
    serverLog(LL_WARNING,
    "\n"
    "RAX:%016lx RBX:%016lx\nRCX:%016lx RDX:%016lx\n"
    "RDI:%016lx RSI:%016lx\nRBP:%016lx RSP:%016lx\n"
    "R8 :%016lx R9 :%016lx\nR10:%016lx R11:%016lx\n"
    "R12:%016lx R13:%016lx\nR14:%016lx R15:%016lx\n"
    "RIP:%016lx EFL:%016lx\nCSGSFS:%016lx",
        (unsigned long) uc->uc_mcontext.mc_rax,
        (unsigned long) uc->uc_mcontext.mc_rbx,
        (unsigned long) uc->uc_mcontext.mc_rcx,
        (unsigned long) uc->uc_mcontext.mc_rdx,
        (unsigned long) uc->uc_mcontext.mc_rdi,
        (unsigned long) uc->uc_mcontext.mc_rsi,
        (unsigned long) uc->uc_mcontext.mc_rbp,
        (unsigned long) uc->uc_mcontext.mc_rsp,
        (unsigned long) uc->uc_mcontext.mc_r8,
        (unsigned long) uc->uc_mcontext.mc_r9,
        (unsigned long) uc->uc_mcontext.mc_r10,
        (unsigned long) uc->uc_mcontext.mc_r11,
        (unsigned long) uc->uc_mcontext.mc_r12,
        (unsigned long) uc->uc_mcontext.mc_r13,
        (unsigned long) uc->uc_mcontext.mc_r14,
        (unsigned long) uc->uc_mcontext.mc_r15,
        (unsigned long) uc->uc_mcontext.mc_rip,
        (unsigned long) uc->uc_mcontext.mc_rflags,
        (unsigned long) uc->uc_mcontext.mc_cs
    );
    logStackContent((void**)uc->uc_mcontext.mc_rsp);
#else
    serverLog(LL_WARNING,
        "  Dumping of registers not supported for this OS/arch");
#endif
}

#endif /* HAVE_BACKTRACE */

/* Return a file descriptor to write directly to the Redis log with the
 * write(2) syscall, that can be used in critical sections of the code
 * where the rest of Redis can't be trusted (for example during the memory
 * test) or when an API call requires a raw fd.
 *
 * Close it with closeDirectLogFiledes(). */
int openDirectLogFiledes(void) {
    int log_to_stdout = g_pserver->logfile[0] == '\0';
    int fd = log_to_stdout ?
        STDOUT_FILENO :
        open(g_pserver->logfile, O_APPEND|O_CREAT|O_WRONLY, 0644);
    return fd;
}

/* Used to close what closeDirectLogFiledes() returns. */
void closeDirectLogFiledes(int fd) {
    int log_to_stdout = g_pserver->logfile[0] == '\0';
    if (!log_to_stdout) close(fd);
}

void safe_write(int fd, const void *pv, ssize_t cb)
{
    ssize_t offset = 0;
    do
    {
        ssize_t cbWrite = write(fd, reinterpret_cast<const char*>(pv)+offset, cb-offset);
        if (cbWrite <= 0)
            return;
        offset += cbWrite;
    } while (offset < cb);
}

#ifdef UNW_LOCAL_ONLY

/* Logs the stack trace using the libunwind call.
 * The eip argument is unused as libunwind only gets local context.
 * The uplevel argument indicates how many of the calling functions to skip.
 */
void logStackTrace(void * eip, int uplevel) {
    (void)eip;//UNUSED
    const char *msg;
    int fd = openDirectLogFiledes();

    if (fd == -1) return; /* If we can't log there is anything to do. */

    msg = "\n------ STACK TRACE ------\n";
    if (write(fd,msg,strlen(msg)) == -1) {/* Avoid warning. */};
    unw_cursor_t cursor;
    unw_context_t context;

    unw_getcontext(&context);
    unw_init_local(&cursor, &context);

    /* Write symbols to log file */
    msg = "\nBacktrace:\n";
    if (write(fd,msg,strlen(msg)) == -1) {/* Avoid warning. */};

    for (int i = 0; i < uplevel; i++) {
        unw_step(&cursor);
    }

    while ( unw_step(&cursor) ) {
    unw_word_t ip, sp, off;

    unw_get_reg(&cursor, UNW_REG_IP, &ip);
    unw_get_reg(&cursor, UNW_REG_SP, &sp);

    char symbol[256] = {"<unknown>"};
    char *name = symbol;

    if ( !unw_get_proc_name(&cursor, symbol, sizeof(symbol), &off) ) {
        int status;
        if ( (name = abi::__cxa_demangle(symbol, NULL, NULL, &status)) == 0 )
        name = symbol;
    }

    dprintf(fd, "%s(+0x%" PRIxPTR ") [0x%016" PRIxPTR "] sp=0x%016" PRIxPTR "\n",
        name,
        static_cast<uintptr_t>(off),
        static_cast<uintptr_t>(ip),
        static_cast<uintptr_t>(sp));

    if ( name != symbol )
        free(name);
    }
}

#endif /* UNW_LOCAL_ONLY */

#ifdef HAVE_BACKTRACE

void backtrace_symbols_demangle_fd(void **trace, size_t csym, int fd)
{
    char **syms = backtrace_symbols(trace, csym);
    char symbuf[1024];
    for (size_t itrace = 0; itrace < csym; ++itrace)
    {
        int status = 0;

        // First find the symbol (preceded by a '(')
        char *pchSymStart = syms[itrace];
        while (*pchSymStart != '(' && *pchSymStart != '\0')
            ++pchSymStart;
        if (*pchSymStart != '\0')
            ++pchSymStart;  // skip the '('
        char *pchSymEnd = pchSymStart;
        while (*pchSymEnd != '+' && *pchSymEnd != '\0')
            ++pchSymEnd;

        if ((pchSymEnd - pchSymStart) < 1023)
        {
            memcpy(symbuf, pchSymStart, pchSymEnd - pchSymStart);
            symbuf[pchSymEnd - pchSymStart] = '\0';
            char *sz = abi::__cxa_demangle(symbuf, nullptr, nullptr, &status);
            if (status == 0)
            {
                safe_write(fd, syms[itrace], pchSymStart - syms[itrace]);
                safe_write(fd, sz, strlen(sz));
                safe_write(fd, pchSymEnd, (syms[itrace] + strlen(syms[itrace])-pchSymEnd));
            }
            else
            {
                safe_write(fd, syms[itrace], strlen(syms[itrace]));
            }
            free(sz);
        }
        else {
            safe_write(fd, syms[itrace], strlen(syms[itrace]));
        }
        safe_write(fd, "\n", 1);   
    }
    free(syms);
}

/* Logs the stack trace using the backtrace() call. This function is designed
 * to be called from signal handlers safely.
 * The eip argument is optional (can take NULL).
 * The uplevel argument indicates how many of the calling functions to skip.
 */
void logStackTrace(void *eip, int uplevel) {
    void *trace[100];
    int trace_size = 0, fd = openDirectLogFiledes();
    const char *msg;
    uplevel++; /* skip this function */

    if (fd == -1) return; /* If we can't log there is anything to do. */

    /* Get the stack trace first! */
    trace_size = backtrace(trace, 100);

    msg = "\n------ STACK TRACE ------\n";
    if (write(fd,msg,strlen(msg)) == -1) {/* Avoid warning. */};

    if (eip) {
        /* Write EIP to the log file*/
        msg = "EIP:\n";
        if (write(fd,msg,strlen(msg)) == -1) {/* Avoid warning. */};
        backtrace_symbols_demangle_fd(&eip, 1, fd);
    }

    /* Write symbols to log file */
    msg = "\nBacktrace:\n";
    if (write(fd,msg,strlen(msg)) == -1) {/* Avoid warning. */};
    backtrace_symbols_demangle_fd(trace+uplevel, trace_size-uplevel, fd);

    /* Cleanup */
    closeDirectLogFiledes(fd);
}

#endif /* HAVE_BACKTRACE */

/* Log global server info */
void logServerInfo(void) {
    sds infostring, clients;
    serverLogRaw(LL_WARNING|LL_RAW, "\n------ INFO OUTPUT ------\n");
    infostring = genRedisInfoString("all");
    serverLogRaw(LL_WARNING|LL_RAW, infostring);
    serverLogRaw(LL_WARNING|LL_RAW, "\n------ CLIENT LIST OUTPUT ------\n");
    clients = getAllClientsInfoString(-1);
    serverLogRaw(LL_WARNING|LL_RAW, clients);
    sdsfree(infostring);
    sdsfree(clients);
}

/* Log modules info. Something we wanna do last since we fear it may crash. */
void logModulesInfo(void) {
    serverLogRaw(LL_WARNING|LL_RAW, "\n------ MODULES INFO OUTPUT ------\n");
    sds infostring = modulesCollectInfo(sdsempty(), NULL, 1, 0);
    serverLogRaw(LL_WARNING|LL_RAW, infostring);
    sdsfree(infostring);
}

/* Log information about the "current" client, that is, the client that is
 * currently being served by Redis. May be NULL if Redis is not serving a
 * client right now. */
void logCurrentClient(void) {
    if (serverTL->current_client == NULL) return;

    client *cc = serverTL->current_client;
    sds client;
    int j;

    serverLogRaw(LL_WARNING|LL_RAW, "\n------ CURRENT CLIENT INFO ------\n");
    client = catClientInfoString(sdsempty(),cc);
    serverLog(LL_WARNING|LL_RAW,"%s\n", client);
    sdsfree(client);
    for (j = 0; j < cc->argc; j++) {
        robj *decoded;

        decoded = getDecodedObject(cc->argv[j]);
        serverLog(LL_WARNING|LL_RAW,"argv[%d]: '%s'\n", j,
            (char*)ptrFromObj(decoded));
        decrRefCount(decoded);
    }
    /* Check if the first argument, usually a key, is found inside the
     * selected DB, and if so print info about the associated object. */
    if (cc->argc > 1) {
        robj *val, *key;

        key = getDecodedObject(cc->argv[1]);
        val = cc->db->find(key);
        if (val) {
            serverLog(LL_WARNING,"key '%s' found in DB containing the following object:", (char*)ptrFromObj(key));
            serverLogObjectDebugInfo(val);
        }
        decrRefCount(key);
    }
}

#if defined(HAVE_PROC_MAPS)

#define MEMTEST_MAX_REGIONS 128

/* A non destructive memory test executed during segfault. */
int memtest_test_linux_anonymous_maps(void) {
    FILE *fp;
    char line[1024];
    char logbuf[1024];
    size_t start_addr, end_addr, size;
    size_t start_vect[MEMTEST_MAX_REGIONS];
    size_t size_vect[MEMTEST_MAX_REGIONS];
    int regions = 0, j;

    int fd = openDirectLogFiledes();
    if (!fd) return 0;

    fp = fopen("/proc/self/maps","r");
    if (!fp) return 0;
    while(fgets(line,sizeof(line),fp) != NULL) {
        char *start, *end, *p = line;

        start = p;
        p = strchr(p,'-');
        if (!p) continue;
        *p++ = '\0';
        end = p;
        p = strchr(p,' ');
        if (!p) continue;
        *p++ = '\0';
        if (strstr(p,"stack") ||
            strstr(p,"vdso") ||
            strstr(p,"vsyscall")) continue;
        if (!strstr(p,"00:00")) continue;
        if (!strstr(p,"rw")) continue;

        start_addr = strtoul(start,NULL,16);
        end_addr = strtoul(end,NULL,16);
        size = end_addr-start_addr;

        start_vect[regions] = start_addr;
        size_vect[regions] = size;
        snprintf(logbuf,sizeof(logbuf),
            "*** Preparing to test memory region %lx (%lu bytes)\n",
                (unsigned long) start_vect[regions],
                (unsigned long) size_vect[regions]);
        if (write(fd,logbuf,strlen(logbuf)) == -1) { /* Nothing to do. */ }
        regions++;
    }

    int errors = 0;
    for (j = 0; j < regions; j++) {
        if (write(fd,".",1) == -1) { /* Nothing to do. */ }
        errors += memtest_preserving_test((unsigned long*)start_vect[j],size_vect[j],1);
        if (write(fd, errors ? "E" : "O",1) == -1) { /* Nothing to do. */ }
    }
    if (write(fd,"\n",1) == -1) { /* Nothing to do. */ }

    /* NOTE: It is very important to close the file descriptor only now
     * because closing it before may result into unmapping of some memory
     * region that we are testing. */
    fclose(fp);
    closeDirectLogFiledes(fd);
    return errors;
}
#endif /* HAVE_PROC_MAPS */

static void killServerThreads(void) {
    int err;
    for (int i = 0; i < cserver.cthreads; i++) {
        if (g_pserver->rgthread[i] != pthread_self()) {
            pthread_cancel(g_pserver->rgthread[i]);
        }
    }
    if (pthread_self() != cserver.main_thread_id && pthread_cancel(cserver.main_thread_id) == 0) {
        if ((err = pthread_join(cserver.main_thread_id,NULL)) != 0) {
            serverLog(LL_WARNING, "main thread can not be joined: %s", strerror(err));
        } else {
            serverLog(LL_WARNING, "main thread terminated");
        }
    }
}

/* Kill the running threads (other than current) in an unclean way. This function
 * should be used only when it's critical to stop the threads for some reason.
 * Currently Redis does this only on crash (for instance on SIGSEGV) in order
 * to perform a fast memory check without other threads messing with memory. */
void killThreads(void) {
    killServerThreads();
    bioKillThreads();
}

void doFastMemoryTest(void) {
#if defined(HAVE_PROC_MAPS)
    if (g_pserver->memcheck_enabled) {
        /* Test memory */
        serverLogRaw(LL_WARNING|LL_RAW, "\n------ FAST MEMORY TEST ------\n");
        killThreads();
        if (memtest_test_linux_anonymous_maps()) {
            serverLogRaw(LL_WARNING|LL_RAW,
                "!!! MEMORY ERROR DETECTED! Check your memory ASAP !!!\n");
        } else {
            serverLogRaw(LL_WARNING|LL_RAW,
                "Fast memory test PASSED, however your memory can still be broken. Please run a memory test for several hours if possible.\n");
        }
    }
#endif /* HAVE_PROC_MAPS */
}

/* Scans the (assumed) x86 code starting at addr, for a max of `len`
 * bytes, searching for E8 (callq) opcodes, and dumping the symbols
 * and the call offset if they appear to be valid. */
void dumpX86Calls(void *addr, size_t len) {
    size_t j;
    unsigned char *p = (unsigned char*)addr;
    Dl_info info;
    /* Hash table to best-effort avoid printing the same symbol
     * multiple times. */
    unsigned long ht[256] = {0};

    if (len < 5) return;
    for (j = 0; j < len-4; j++) {
        if (p[j] != 0xE8) continue; /* Not an E8 CALL opcode. */
        unsigned long target = (unsigned long)addr+j+5;
        target += *((int32_t*)(p+j+1));
        if (dladdr((void*)target, &info) != 0 && info.dli_sname != NULL) {
            if (ht[target&0xff] != target) {
                printf("Function at 0x%lx is %s\n",target,info.dli_sname);
                ht[target&0xff] = target;
            }
            j += 4; /* Skip the 32 bit immediate. */
        }
    }
}

void dumpCodeAroundEIP(void *eip) {
    Dl_info info;
    if (dladdr(eip, &info) != 0) {
        serverLog(LL_WARNING|LL_RAW,
            "\n------ DUMPING CODE AROUND EIP ------\n"
            "Symbol: %s (base: %p)\n"
            "Module: %s (base %p)\n"
            "$ xxd -r -p /tmp/dump.hex /tmp/dump.bin\n"
            "$ objdump --adjust-vma=%p -D -b binary -m i386:x86-64 /tmp/dump.bin\n"
            "------\n",
            info.dli_sname, info.dli_saddr, info.dli_fname, info.dli_fbase,
            info.dli_saddr);
        size_t len = (long)eip - (long)info.dli_saddr;
        unsigned long sz = sysconf(_SC_PAGESIZE);
        if (len < 1<<13) { /* we don't have functions over 8k (verified) */
            /* Find the address of the next page, which is our "safety"
             * limit when dumping. Then try to dump just 128 bytes more
             * than EIP if there is room, or stop sooner. */
            void *base = (void *)info.dli_saddr;
            unsigned long next = ((unsigned long)eip + sz) & ~(sz-1);
            unsigned long end = (unsigned long)eip + 128;
            if (end > next) end = next;
            len = end - (unsigned long)base;
            serverLogHexDump(LL_WARNING, "dump of function",
                base, len);
            dumpX86Calls(base, len);
        }
    }
}

void sigsegvHandler(int sig, siginfo_t *info, void *secret) {
    UNUSED(secret);
    UNUSED(info);
    g_fInCrash = true;

    bugReportStart();
    serverLog(LL_WARNING,
        "KeyDB %s crashed by signal: %d, si_code: %d", KEYDB_REAL_VERSION, sig, info->si_code);
    if (sig == SIGSEGV || sig == SIGBUS) {
        serverLog(LL_WARNING,
        "Accessing address: %p", (void*)info->si_addr);
    }
    if (info->si_code <= SI_USER && info->si_pid != -1) {
        serverLog(LL_WARNING, "Killed by PID: %ld, UID: %d", (long) info->si_pid, info->si_uid);
    }

#ifdef HAVE_BACKTRACE
    ucontext_t *uc = (ucontext_t*) secret;
    void *eip = getMcontextEip(uc);
    if (eip != NULL) {
        serverLog(LL_WARNING,
        "Crashed running the instruction at: %p", eip);
    }

    logStackTrace(getMcontextEip(uc), 1);

    logRegisters(uc);
#endif
#ifdef UNW_LOCAL_ONLY
    logStackTrace(NULL, 1);
#endif 

    printCrashReport();

#ifdef HAVE_BACKTRACE
    if (eip != NULL)
        dumpCodeAroundEIP(eip);
#endif

    bugReportEnd(1, sig);
}

void printCrashReport(void) {
    g_fInCrash = true;

    /* Log INFO and CLIENT LIST */
    logServerInfo();

    /* Log the current client */
    logCurrentClient();

    /* Log modules info. Something we wanna do last since we fear it may crash. */
    logModulesInfo();

    /* Run memory test in case the crash was triggered by memory corruption. */
    doFastMemoryTest();
}

void bugReportEnd(int killViaSignal, int sig) {
    struct sigaction act;

    serverLogRaw(LL_WARNING|LL_RAW,
"\n=== KEYDB BUG REPORT END. Make sure to include from START to END. ===\n\n"
"       Please report the crash by opening an issue on github:\n\n"
"           https://github.com/JohnSully/KeyDB/issues\n\n"
"  Suspect RAM error? Use keydb-server --test-memory to verify it.\n\n"
);

    /* free(messages); Don't call free() with possibly corrupted memory. */
    if (cserver.daemonize && cserver.supervised == 0 && cserver.pidfile) unlink(cserver.pidfile);

    if (!killViaSignal) {
        if (g_pserver->use_exit_on_panic)
            exit(1);
        abort();
    }

    /* Make sure we exit with the right signal at the end. So for instance
     * the core will be dumped if enabled. */
    sigemptyset (&act.sa_mask);
    act.sa_flags = SA_NODEFER | SA_ONSTACK | SA_RESETHAND;
    act.sa_handler = SIG_DFL;
    sigaction (sig, &act, NULL);
    kill(getpid(),sig);
}

/* ==================== Logging functions for debugging ===================== */

void serverLogHexDump(int level, const char *descr, void *value, size_t len) {
    char buf[65], *b;
    unsigned char *v = (unsigned char*)value;
    char charset[] = "0123456789abcdef";

    serverLog(level,"%s (hexdump of %zu bytes):", descr, len);
    b = buf;
    while(len) {
        b[0] = charset[(*v)>>4];
        b[1] = charset[(*v)&0xf];
        b[2] = '\0';
        b += 2;
        len--;
        v++;
        if (b-buf == 64 || len == 0) {
            serverLogRaw(level|LL_RAW,buf);
            b = buf;
        }
    }
    serverLogRaw(level|LL_RAW,"\n");
}

/* =========================== Software Watchdog ============================ */
#include <sys/time.h>

void watchdogSignalHandler(int sig, siginfo_t *info, void *secret) {
#ifdef HAVE_BACKTRACE
    ucontext_t *uc = (ucontext_t*) secret;
#else
    (void)secret;
#endif
    UNUSED(info);
    UNUSED(sig);

    serverLogFromHandler(LL_WARNING,"\n--- WATCHDOG TIMER EXPIRED ---");
#ifdef HAVE_BACKTRACE
    logStackTrace(getMcontextEip(uc), 1);
#elif defined UNW_LOCAL_ONLY
    logStackTrace(NULL, 1);
#else
    serverLogFromHandler(LL_WARNING,"Sorry: no support for backtrace().");
#endif
    serverLogFromHandler(LL_WARNING,"--------\n");
}

/* Schedule a SIGALRM delivery after the specified period in milliseconds.
 * If a timer is already scheduled, this function will re-schedule it to the
 * specified time. If period is 0 the current timer is disabled. */
void watchdogScheduleSignal(int period) {
    struct itimerval it;

    /* Will stop the timer if period is 0. */
    it.it_value.tv_sec = period/1000;
    it.it_value.tv_usec = (period%1000)*1000;
    /* Don't automatically restart. */
    it.it_interval.tv_sec = 0;
    it.it_interval.tv_usec = 0;
    setitimer(ITIMER_REAL, &it, NULL);
}

/* Enable the software watchdog with the specified period in milliseconds. */
void enableWatchdog(int period) {
    int min_period;

    if (g_pserver->watchdog_period == 0) {
        struct sigaction act;

        /* Watchdog was actually disabled, so we have to setup the signal
         * handler. */
        sigemptyset(&act.sa_mask);
        act.sa_flags = SA_SIGINFO;
        act.sa_sigaction = watchdogSignalHandler;
        sigaction(SIGALRM, &act, NULL);
    }
    /* If the configured period is smaller than twice the timer period, it is
     * too short for the software watchdog to work reliably. Fix it now
     * if needed. */
    min_period = (1000/g_pserver->hz)*2;
    if (period < min_period) period = min_period;
    watchdogScheduleSignal(period); /* Adjust the current timer. */
    g_pserver->watchdog_period = period;
}

/* Disable the software watchdog. */
void disableWatchdog(void) {
    struct sigaction act;
    if (g_pserver->watchdog_period == 0) return; /* Already disabled. */
    watchdogScheduleSignal(0); /* Stop the current timer. */

    /* Set the signal handler to SIG_IGN, this will also remove pending
     * signals from the queue. */
    sigemptyset(&act.sa_mask);
    act.sa_flags = 0;
    act.sa_handler = SIG_IGN;
    sigaction(SIGALRM, &act, NULL);
    g_pserver->watchdog_period = 0;
}

/* Positive input is sleep time in microseconds. Negative input is fractions
 * of microseconds, i.e. -10 means 100 nanoseconds. */
void debugDelay(int usec) {
    /* Since even the shortest sleep results in context switch and system call,
     * the way we achive short sleeps is by statistically sleeping less often. */
    if (usec < 0) usec = (rand() % -usec) == 0 ? 1: 0;
    if (usec) usleep(usec);
}
