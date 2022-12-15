/* KeyDB diagnostic utility.
 *
 * Copyright (c) 2009-2021, Salvatore Sanfilippo <antirez at gmail dot com>
 * Copyright (c) 2021, EQ Alpha Technology Ltd. <john at eqalpha dot com>
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

#include "fmacros.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <signal.h>
#include <assert.h>
#include <math.h>
#include <pthread.h>
#include <deque>
extern "C" {
#include <sds.h> /* Use hiredis sds. */
#include <sdscompat.h>
#include "hiredis.h"
}
#include "ae.h"
#include "adlist.h"
#include "dict.h"
#include "zmalloc.h"
#include "storage.h"
#include "atomicvar.h"
#include "crc16_slottable.h"

#define UNUSED(V) ((void) V)
#define RANDPTR_INITIAL_SIZE 8
#define MAX_LATENCY_PRECISION 3
#define MAX_THREADS 500
#define CLUSTER_SLOTS 16384

#define CLIENT_GET_EVENTLOOP(c) \
    (c->thread_id >= 0 ? config.threads[c->thread_id]->el : config.el)

struct benchmarkThread;
struct clusterNode;
struct redisConfig;

int g_fTestMode = false;

static struct config {
    aeEventLoop *el;
    const char *hostip;
    int hostport;
    const char *hostsocket;
    int numclients;
    int liveclients;
    int period_ms;
    int requests;
    int requests_issued;
    int requests_finished;
    int keysize;
    int datasize;
    int randomkeys;
    int randomkeys_keyspacelen;
    int keepalive;
    int pipeline;
    int showerrors;
    long long start;
    long long totlatency;
    long long *latency;
    const char *title;
    list *clients;
    int quiet;
    int csv;
    int loop;
    int idlemode;
    int dbnum;
    sds dbnumstr;
    char *tests;
    char *auth;
    const char *user;
    int precision;
    int max_threads;
    struct benchmarkThread **threads;
    int cluster_mode;
    int cluster_node_count;
    struct clusterNode **cluster_nodes;
    struct redisConfig *redis_config;
    int is_fetching_slots;
    int is_updating_slots;
    int slots_last_update;
    int enable_tracking;
    /* Thread mutexes to be used as fallbacks by atomicvar.h */
    pthread_mutex_t requests_issued_mutex;
    pthread_mutex_t requests_finished_mutex;
    pthread_mutex_t liveclients_mutex;
    pthread_mutex_t is_fetching_slots_mutex;
    pthread_mutex_t is_updating_slots_mutex;
    pthread_mutex_t updating_slots_mutex;
    pthread_mutex_t slots_last_update_mutex;
} config;

typedef struct _client {
    redisContext *context;
    sds obuf;
    char **randptr;         /* Pointers to :rand: strings inside the command buf */
    size_t randlen;         /* Number of pointers in client->randptr */
    size_t randfree;        /* Number of unused pointers in client->randptr */
    char **stagptr;         /* Pointers to slot hashtags (cluster mode only) */
    size_t staglen;         /* Number of pointers in client->stagptr */
    size_t stagfree;        /* Number of unused pointers in client->stagptr */
    size_t written;         /* Bytes of 'obuf' already written */
    long long start;        /* Start time of a request */
    long long latency;      /* Request latency */
    int pending;            /* Number of pending requests (replies to consume) */
    int prefix_pending;     /* If non-zero, number of pending prefix commands. Commands
                               such as auth and select are prefixed to the pipeline of
                               benchmark commands and discarded after the first send. */
    int prefixlen;          /* Size in bytes of the pending prefix commands */
    int thread_id;
    struct clusterNode *cluster_node;
    int slots_last_update;
    redisReply *lastReply;
} *client;

/* Threads. */

typedef struct benchmarkThread {
    int index;
    pthread_t thread;
    aeEventLoop *el;
} benchmarkThread;

/* Cluster. */
typedef struct clusterNode {
    char *ip;
    int port;
    sds name;
    int flags;
    sds replicate;  /* Master ID if node is a replica */
    int *slots;
    int slots_count;
    int current_slot_index;
    int *updated_slots;         /* Used by updateClusterSlotsConfiguration */
    int updated_slots_count;    /* Used by updateClusterSlotsConfiguration */
    int replicas_count;
    sds *migrating; /* An array of sds where even strings are slots and odd
                     * strings are the destination node IDs. */
    sds *importing; /* An array of sds where even strings are slots and odd
                     * strings are the source node IDs. */
    int migrating_count; /* Length of the migrating array (migrating slots*2) */
    int importing_count; /* Length of the importing array (importing slots*2) */
    struct redisConfig *redis_config;
} clusterNode;

typedef struct redisConfig {
    sds save;
    sds appendonly;
} redisConfig;

int g_fInCrash = false;

/* Prototypes */
static void writeHandler(aeEventLoop *el, int fd, void *privdata, int mask);
static benchmarkThread *createBenchmarkThread(int index);
static void freeBenchmarkThread(benchmarkThread *thread);
static void freeBenchmarkThreads();
static redisContext *getRedisContext(const char *ip, int port,
                                     const char *hostsocket);

/* Implementation */
static long long ustime(void) {
    struct timeval tv;
    long long ust;

    gettimeofday(&tv, NULL);
    ust = ((long)tv.tv_sec)*1000000;
    ust += tv.tv_usec;
    return ust;
}

/* _serverAssert is needed by dict */
extern "C" void _serverAssert(const char *estr, const char *file, int line) {
    fprintf(stderr, "=== ASSERTION FAILED ===");
    fprintf(stderr, "==> %s:%d '%s' is not true",file,line,estr);
    *((char*)-1) = 'x';
}

/* asyncFreeDictTable is needed by dict */
extern "C" void asyncFreeDictTable(struct dictEntry **de) {
    zfree(de);
}

static redisContext *getRedisContext(const char *ip, int port,
                                     const char *hostsocket)
{
    redisContext *ctx = NULL;
    redisReply *reply =  NULL;
    if (hostsocket == NULL)
        ctx = redisConnect(ip, port);
    else
        ctx = redisConnectUnix(hostsocket);
    if (ctx == NULL || ctx->err) {
        fprintf(stderr,"Could not connect to Redis at ");
        const char *err = (ctx != NULL ? ctx->errstr : "");
        if (hostsocket == NULL)
            fprintf(stderr,"%s:%d: %s\n",ip,port,err);
        else
            fprintf(stderr,"%s: %s\n",hostsocket,err);
        goto cleanup;
    }
    if (config.auth == NULL)
        return ctx;
    if (config.user == NULL)
        reply = (redisReply*)redisCommand(ctx,"AUTH %s", config.auth);
    else
        reply = (redisReply*)redisCommand(ctx,"AUTH %s %s", config.user, config.auth);
    if (reply != NULL) {
        if (reply->type == REDIS_REPLY_ERROR) {
            if (hostsocket == NULL)
                fprintf(stderr, "Node %s:%d replied with error:\n%s\n", ip, port, reply->str);
            else
                fprintf(stderr, "Node %s replied with error:\n%s\n", hostsocket, reply->str);
            goto cleanup;
        }
        freeReplyObject(reply);
        return ctx;
    }
    fprintf(stderr, "ERROR: failed to fetch reply from ");
    if (hostsocket == NULL)
        fprintf(stderr, "%s:%d\n", ip, port);
    else
        fprintf(stderr, "%s\n", hostsocket);
cleanup:
    freeReplyObject(reply);
    redisFree(ctx);
    return NULL;
}

static void freeClient(client c) {
    aeEventLoop *el = CLIENT_GET_EVENTLOOP(c);
    listNode *ln;
    aeDeleteFileEvent(el,c->context->fd,AE_WRITABLE);
    aeDeleteFileEvent(el,c->context->fd,AE_READABLE);
    if (c->thread_id >= 0) {
        int requests_finished = 0;
        atomicGet(config.requests_finished, requests_finished);
        if (requests_finished >= config.requests) {
            aeStop(el);
        }
    }
    redisFree(c->context);
    sdsfree(c->obuf);
    zfree(c->randptr);
    zfree(c->stagptr);
    zfree(c);
    if (config.max_threads) pthread_mutex_lock(&(config.liveclients_mutex));
    config.liveclients--;
    ln = listSearchKey(config.clients,c);
    assert(ln != NULL);
    listDelNode(config.clients,ln);
    if (config.max_threads) pthread_mutex_unlock(&(config.liveclients_mutex));
}

static void freeAllClients(void) {
    listNode *ln = config.clients->head, *next;

    while(ln) {
        next = ln->next;
        freeClient((client)ln->value);
        ln = next;
    }
}

static void resetClient(client c) {
    aeEventLoop *el = CLIENT_GET_EVENTLOOP(c);
    aeDeleteFileEvent(el,c->context->fd,AE_WRITABLE);
    aeDeleteFileEvent(el,c->context->fd,AE_READABLE);
    aeCreateFileEvent(el,c->context->fd,AE_WRITABLE,writeHandler,c);
    c->written = 0;
    c->pending = config.pipeline;
}

static void randomizeClientKey(client c) {
    size_t i;

    for (i = 0; i < c->randlen; i++) {
        char *p = c->randptr[i]+11;
        size_t r = 0;
        if (config.randomkeys_keyspacelen != 0)
            r = random() % config.randomkeys_keyspacelen;
        size_t j;

        for (j = 0; j < 12; j++) {
            *p = '0'+r%10;
            r/=10;
            p--;
        }
    }
}

static void readHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    client c = (client)privdata;
    void *reply = NULL;
    UNUSED(el);
    UNUSED(fd);
    UNUSED(mask);

    /* Calculate latency only for the first read event. This means that the
     * server already sent the reply and we need to parse it. Parsing overhead
     * is not part of the latency, so calculate it only once, here. */
    if (c->latency < 0) c->latency = ustime()-(c->start);

    if (redisBufferRead(c->context) != REDIS_OK) {
        fprintf(stderr,"Error: %s\n",c->context->errstr);
        exit(1);
    } else {
        while(c->pending) {
            if (redisGetReply(c->context,&reply) != REDIS_OK) {
                fprintf(stderr,"Error: %s\n",c->context->errstr);
                exit(1);
            }
            if (reply != NULL) {
                if (reply == (void*)REDIS_REPLY_ERROR) {
                    fprintf(stderr,"Unexpected error reply, exiting...\n");
                    exit(1);
                }
                redisReply *r = (redisReply*)reply;
                int is_err = (r->type == REDIS_REPLY_ERROR);

                if (is_err && config.showerrors) {
                    /* TODO: static lasterr_time not thread-safe */
                    static time_t lasterr_time = 0;
                    time_t now = time(NULL);
                    if (lasterr_time != now) {
                        lasterr_time = now;
                        if (c->cluster_node) {
                            printf("Error from server %s:%d: %s\n",
                                   c->cluster_node->ip,
                                   c->cluster_node->port,
                                   r->str);
                        } else printf("Error from server: %s\n", r->str);
                    }
                }

                freeReplyObject(reply);
                /* This is an OK for prefix commands such as auth and select.*/
                if (c->prefix_pending > 0) {
                    c->prefix_pending--;
                    c->pending--;
                    /* Discard prefix commands on first response.*/
                    if (c->prefixlen > 0) {
                        size_t j;
                        sdsrange(c->obuf, c->prefixlen, -1);
                        /* We also need to fix the pointers to the strings
                        * we need to randomize. */
                        for (j = 0; j < c->randlen; j++)
                            c->randptr[j] -= c->prefixlen;
                        c->prefixlen = 0;
                    }
                    continue;
                }
                int requests_finished = 0;
                atomicGetIncr(config.requests_finished, requests_finished, 1);
                if (requests_finished < config.requests)
                    config.latency[requests_finished] = c->latency;
                c->pending--;
                if (c->pending == 0) {
                    resetClient(c);
                    break;
                }
            } else {
                break;
            }
        }
    }
}

static void writeHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    client c = (client)privdata;
    UNUSED(el);
    UNUSED(fd);
    UNUSED(mask);

    /* Initialize request when nothing was written. */
    if (c->written == 0) {
        /* Really initialize: randomize keys and set start time. */
        if (config.randomkeys) randomizeClientKey(c);
        atomicGet(config.slots_last_update, c->slots_last_update);
        c->start = ustime();
        c->latency = -1;
    }
    if (sdslen(c->obuf) > c->written) {
        void *ptr = c->obuf+c->written;
        ssize_t nwritten = write(c->context->fd,ptr,sdslen(c->obuf)-c->written);
        if (nwritten == -1) {
            if (errno != EPIPE)
                fprintf(stderr, "Writing to socket: %s\n", strerror(errno));
            freeClient(c);
            return;
        }
        c->written += nwritten;
        if (sdslen(c->obuf) == c->written) {
            aeDeleteFileEvent(el,c->context->fd,AE_WRITABLE);
            aeCreateFileEvent(el,c->context->fd,AE_READABLE,readHandler,c);
        }
    }
}

/* Create a benchmark client, configured to send the command passed as 'cmd' of
 * 'len' bytes.
 *
 * The command is copied N times in the client output buffer (that is reused
 * again and again to send the request to the server) accordingly to the configured
 * pipeline size.
 *
 * Also an initial SELECT command is prepended in order to make sure the right
 * database is selected, if needed. The initial SELECT will be discarded as soon
 * as the first reply is received.
 *
 * To create a client from scratch, the 'from' pointer is set to NULL. If instead
 * we want to create a client using another client as reference, the 'from' pointer
 * points to the client to use as reference. In such a case the following
 * information is take from the 'from' client:
 *
 * 1) The command line to use.
 * 2) The offsets of the __rand_int__ elements inside the command line, used
 *    for arguments randomization.
 *
 * Even when cloning another client, prefix commands are applied if needed.*/
static client createClient(const char *cmd, size_t len, client from, int thread_id) {
    int j;
    int is_cluster_client = (config.cluster_mode && thread_id >= 0);
    client c = (client)zmalloc(sizeof(struct _client), MALLOC_LOCAL);

    const char *ip = NULL;
    int port = 0;
    c->cluster_node = NULL;
    if (config.hostsocket == NULL || is_cluster_client) {
        if (!is_cluster_client) {
            ip = config.hostip;
            port = config.hostport;
        } else {
            int node_idx = 0;
            if (config.max_threads < config.cluster_node_count)
                node_idx = config.liveclients % config.cluster_node_count;
            else
                node_idx = thread_id % config.cluster_node_count;
            clusterNode *node = config.cluster_nodes[node_idx];
            assert(node != NULL);
            ip = (const char *) node->ip;
            port = node->port;
            c->cluster_node = node;
        }
        c->context = redisConnectNonBlock(ip,port);
    } else {
        c->context = redisConnectUnixNonBlock(config.hostsocket);
    }
    if (c->context->err) {
        fprintf(stderr,"Could not connect to Redis at ");
        if (config.hostsocket == NULL || is_cluster_client)
            fprintf(stderr,"%s:%d: %s\n",ip,port,c->context->errstr);
        else
            fprintf(stderr,"%s: %s\n",config.hostsocket,c->context->errstr);
        exit(1);
    }
    c->thread_id = thread_id;
    /* Suppress hiredis cleanup of unused buffers for max speed. */
    c->context->reader->maxbuf = 0;

    /* Build the request buffer:
     * Queue N requests accordingly to the pipeline size, or simply clone
     * the example client buffer. */
    c->obuf = sdsempty();
    /* Prefix the request buffer with AUTH and/or SELECT commands, if applicable.
     * These commands are discarded after the first response, so if the client is
     * reused the commands will not be used again. */
    c->prefix_pending = 0;
    if (config.auth) {
        char *buf = NULL;
        int len;
        if (config.user == NULL)
            len = redisFormatCommand(&buf, "AUTH %s", config.auth);
        else
            len = redisFormatCommand(&buf, "AUTH %s %s",
                                     config.user, config.auth);
        c->obuf = sdscatlen(c->obuf, buf, len);
        free(buf);
        c->prefix_pending++;
    }

    if (config.enable_tracking) {
        char *buf = NULL;
        int len = redisFormatCommand(&buf, "CLIENT TRACKING on");
        c->obuf = sdscatlen(c->obuf, buf, len);
        free(buf);
        c->prefix_pending++;
    }

    /* If a DB number different than zero is selected, prefix our request
     * buffer with the SELECT command, that will be discarded the first
     * time the replies are received, so if the client is reused the
     * SELECT command will not be used again. */
    if (config.dbnum != 0 && !is_cluster_client) {
        c->obuf = sdscatprintf(c->obuf,"*2\r\n$6\r\nSELECT\r\n$%d\r\n%s\r\n",
            (int)sdslen(config.dbnumstr),config.dbnumstr);
        c->prefix_pending++;
    }
    c->prefixlen = sdslen(c->obuf);
    /* Append the request itself. */
    if (from) {
        c->obuf = sdscatlen(c->obuf,
            from->obuf+from->prefixlen,
            sdslen(from->obuf)-from->prefixlen);
    } else {
        for (j = 0; j < config.pipeline; j++)
            c->obuf = sdscatlen(c->obuf,cmd,len);
    }

    c->written = 0;
    c->pending = config.pipeline+c->prefix_pending;
    c->randptr = NULL;
    c->randlen = 0;
    c->stagptr = NULL;
    c->staglen = 0;

    /* Find substrings in the output buffer that need to be randomized. */
    if (config.randomkeys) {
        if (from) {
            c->randlen = from->randlen;
            c->randfree = 0;
            c->randptr = (char**)zmalloc(sizeof(char*)*c->randlen, MALLOC_LOCAL);
            /* copy the offsets. */
            for (j = 0; j < (int)c->randlen; j++) {
                c->randptr[j] = c->obuf + (from->randptr[j]-from->obuf);
                /* Adjust for the different select prefix length. */
                c->randptr[j] += c->prefixlen - from->prefixlen;
            }
        } else {
            char *p = c->obuf;

            c->randlen = 0;
            c->randfree = RANDPTR_INITIAL_SIZE;
            c->randptr = (char**)zmalloc(sizeof(char*)*c->randfree, MALLOC_LOCAL);
            while ((p = strstr(p,"__rand_int__")) != NULL) {
                if (c->randfree == 0) {
                    c->randptr = (char**)zrealloc(c->randptr,sizeof(char*)*c->randlen*2, MALLOC_LOCAL);
                    c->randfree += c->randlen;
                }
                c->randptr[c->randlen++] = p;
                c->randfree--;
                p += 12; /* 12 is strlen("__rand_int__). */
            }
        }
    }
    /* If cluster mode is enabled, set slot hashtags pointers. */
    if (config.cluster_mode) {
        if (from) {
            c->staglen = from->staglen;
            c->stagfree = 0;
            c->stagptr = (char**)zmalloc(sizeof(char*)*c->staglen, MALLOC_LOCAL);
            /* copy the offsets. */
            for (j = 0; j < (int)c->staglen; j++) {
                c->stagptr[j] = c->obuf + (from->stagptr[j]-from->obuf);
                /* Adjust for the different select prefix length. */
                c->stagptr[j] += c->prefixlen - from->prefixlen;
            }
        } else {
            char *p = c->obuf;

            c->staglen = 0;
            c->stagfree = RANDPTR_INITIAL_SIZE;
            c->stagptr = (char**)zmalloc(sizeof(char*)*c->stagfree, MALLOC_LOCAL);
            while ((p = strstr(p,"{tag}")) != NULL) {
                if (c->stagfree == 0) {
                    c->stagptr = (char**)zrealloc(c->stagptr,
                                          sizeof(char*) * c->staglen*2, MALLOC_LOCAL);
                    c->stagfree += c->staglen;
                }
                c->stagptr[c->staglen++] = p;
                c->stagfree--;
                p += 5; /* 5 is strlen("{tag}"). */
            }
        }
    }
    aeEventLoop *el = NULL;
    if (thread_id < 0) el = config.el;
    else {
        benchmarkThread *thread = config.threads[thread_id];
        el = thread->el;
    }
    if (config.idlemode == 0)
        aeCreateFileEvent(el,c->context->fd,AE_WRITABLE,writeHandler,c);
    listAddNodeTail(config.clients,c);
    atomicIncr(config.liveclients, 1);
    atomicGet(config.slots_last_update, c->slots_last_update);
    return c;
}

static void initBenchmarkThreads() {
    int i;
    if (config.threads) freeBenchmarkThreads();
    config.threads = (benchmarkThread**)zmalloc(config.max_threads * sizeof(benchmarkThread*), MALLOC_LOCAL);
    for (i = 0; i < config.max_threads; i++) {
        benchmarkThread *thread = createBenchmarkThread(i);
        config.threads[i] = thread;
    }
}

/* Thread functions. */

static benchmarkThread *createBenchmarkThread(int index) {
    benchmarkThread *thread = (benchmarkThread*)zmalloc(sizeof(*thread), MALLOC_LOCAL);
    if (thread == NULL) return NULL;
    thread->index = index;
    thread->el = aeCreateEventLoop(1024*10);
    return thread;
}

static void freeBenchmarkThread(benchmarkThread *thread) {
    if (thread->el) aeDeleteEventLoop(thread->el);
    zfree(thread);
}

static void freeBenchmarkThreads() {
    int i = 0;
    for (; i < config.max_threads; i++) {
        benchmarkThread *thread = config.threads[i];
        if (thread) freeBenchmarkThread(thread);
    }
    zfree(config.threads);
    config.threads = NULL;
}

static void *execBenchmarkThread(void *ptr) {
    benchmarkThread *thread = (benchmarkThread *) ptr;
    aeMain(thread->el);
    return NULL;
}

void initConfigDefaults() {
    config.numclients = 50;
    config.requests = 100000;
    config.liveclients = 0;
    config.el = aeCreateEventLoop(1024*10);
    config.keepalive = 1;
    config.datasize = 3;
    config.pipeline = 1;
    config.period_ms = 5000;
    config.showerrors = 0;
    config.randomkeys = 0;
    config.randomkeys_keyspacelen = 0;
    config.quiet = 0;
    config.csv = 0;
    config.loop = 0;
    config.idlemode = 0;
    config.latency = NULL;
    config.clients = listCreate();
    config.hostip = "127.0.0.1";
    config.hostport = 6379;
    config.hostsocket = NULL;
    config.tests = NULL;
    config.dbnum = 0;
    config.auth = NULL;
    config.precision = 1;
    config.max_threads = MAX_THREADS;
    config.threads = NULL;
    config.cluster_mode = 0;
    config.cluster_node_count = 0;
    config.cluster_nodes = NULL;
    config.redis_config = NULL;
    config.is_fetching_slots = 0;
    config.is_updating_slots = 0;
    config.slots_last_update = 0;
    config.enable_tracking = 0;
}

/* Returns number of consumed options. */
int parseOptions(int argc, const char **argv) {
    int i;
    int lastarg;
    int exit_status = 1;

    for (i = 1; i < argc; i++) {
        lastarg = (i == (argc-1));

        if (!strcmp(argv[i],"-c") || !strcmp(argv[i],"--clients")) {
            if (lastarg) goto invalid;
            config.numclients = atoi(argv[++i]);
        } else if (!strcmp(argv[i],"--time")) {
            if (lastarg) goto invalid;
            config.period_ms = atoi(argv[++i]);
            if (config.period_ms <= 0) {
                printf("Warning: Invalid value for thread time. Defaulting to 5000ms.\n");
                config.period_ms = 5000;
            }
        } else if (!strcmp(argv[i],"-h") || !strcmp(argv[i],"--host")) {
            if (lastarg) goto invalid;
            config.hostip = strdup(argv[++i]);
        } else if (!strcmp(argv[i],"-p") || !strcmp(argv[i],"--port")) {
            if (lastarg) goto invalid;
            config.hostport = atoi(argv[++i]);
        } else if (!strcmp(argv[i],"-s")) {
            if (lastarg) goto invalid;
            config.hostsocket = strdup(argv[++i]);
        } else if (!strcmp(argv[i],"--password") ) {
            if (lastarg) goto invalid;
            config.auth = strdup(argv[++i]);
        } else if (!strcmp(argv[i],"--user")) {
            if (lastarg) goto invalid;
            config.user = argv[++i];
        } else if (!strcmp(argv[i],"--dbnum")) {
            if (lastarg) goto invalid;
            config.dbnum = atoi(argv[++i]);
            config.dbnumstr = sdsfromlonglong(config.dbnum);
        } else if (!strcmp(argv[i],"-t") || !strcmp(argv[i],"--threads")) {
            if (lastarg) goto invalid;
            config.max_threads = atoi(argv[++i]);
            if (config.max_threads > MAX_THREADS) {
                printf("Warning: Too many threads, limiting threads to %d.\n", MAX_THREADS);
                config.max_threads = MAX_THREADS;
            } else if (config.max_threads <= 0) {
                printf("Warning: Invalid value for max threads. Defaulting to %d.\n", MAX_THREADS);
                config.max_threads = MAX_THREADS;
            }
        } else if (!strcmp(argv[i],"--help")) {
            exit_status = 0;
            goto usage;
        } else {
            /* Assume the user meant to provide an option when the arg starts
             * with a dash. We're done otherwise and should use the remainder
             * as the command and arguments for running the benchmark. */
            if (argv[i][0] == '-') goto invalid;
            return i;
        }
    }

    return i;

invalid:
    printf("Invalid option \"%s\" or option argument missing\n\n",argv[i]);

usage:
    printf(
"Usage: keydb-benchmark [-h <host>] [-p <port>] [-c <clients>] [-n <requests>] [-k <boolean>]\n\n"
" -h, --host <hostname>       Server hostname (default 127.0.0.1)\n"
" -p, --port <port>           Server port (default 6379)\n"
" -c <clients>                Number of parallel connections (default 50)\n"
" -t, --threads <threads>     Maximum number of threads to start before ending\n"
" --time <time>               Time between spinning up new client threads, in milliseconds\n"
" --dbnum <db>                Select the specified DB number (default 0)\n"
" --user <username>           Used to send ACL style 'AUTH username pass'. Needs -a.\n"
" --password <password>       Password for Redis Auth\n\n"
);
    exit(exit_status);
}

int extractPropertyFromInfo(const char *info, const char *key, double &val) {
    char *line = strstr((char*)info, key);
    if (line == nullptr) return 1;
    line += strlen(key) + 1; // Skip past key name and following colon
    char *newline = strchr(line, '\n');
    *newline = 0; // Terminate string after relevant line
    val = strtod(line, nullptr);
    return 0;
}

int extractPropertyFromInfo(const char *info, const char *key, unsigned int &val) {
    char *line = strstr((char*)info, key);
    if (line == nullptr) return 1;
    line += strlen(key) + 1; // Skip past key name and following colon
    char *newline = strchr(line, '\n');
    *newline = 0; // Terminate string after relevant line
    val = atoi(line);
    return 0;
}

double getSelfCpuTime(struct rusage *self_ru) {
    getrusage(RUSAGE_SELF, self_ru);
    double user_time = self_ru->ru_utime.tv_sec + (self_ru->ru_utime.tv_usec / (double)1000000);
    double system_time = self_ru->ru_stime.tv_sec + (self_ru->ru_stime.tv_usec / (double)1000000);
    return user_time + system_time;
}

double getServerCpuTime(redisContext *ctx) {
    redisReply *reply = (redisReply*)redisCommand(ctx, "INFO CPU");
    if (reply->type != REDIS_REPLY_STRING) {
        freeReplyObject(reply);
        printf("Error executing INFO command. Exiting.\n");
        return -1;
    }

    double used_cpu_user, used_cpu_sys;
    if (extractPropertyFromInfo(reply->str, "used_cpu_user", used_cpu_user)) {
        printf("Error reading user CPU usage from INFO command. Exiting.\n");
        return -1;
    }
    if (extractPropertyFromInfo(reply->str, "used_cpu_sys", used_cpu_sys)) {
        printf("Error reading system CPU usage from INFO command. Exiting.\n");
        return -1;
    }
    freeReplyObject(reply);
    return used_cpu_user + used_cpu_sys;
}

double getMean(std::deque<double> *q) {
    double sum = 0;
    for (long unsigned int i = 0; i < q->size(); i++) {
        sum += (*q)[i];
    }
    return sum / q->size();
}

bool isAtFullLoad(double cpuPercent, unsigned int threads) {
    return cpuPercent / threads >= 96;
}

int main(int argc, const char **argv) {
    int i;
    
    storage_init(NULL, 0);

    srandom(time(NULL));
    signal(SIGHUP, SIG_IGN);
    signal(SIGPIPE, SIG_IGN);

    initConfigDefaults();

    i = parseOptions(argc,argv);
    argc -= i;
    argv += i;

    config.latency = (long long*)zmalloc(sizeof(long long)*config.requests, MALLOC_LOCAL);

    if (config.max_threads > 0) {
        int err = 0;
        err |= pthread_mutex_init(&(config.requests_issued_mutex), NULL);
        err |= pthread_mutex_init(&(config.requests_finished_mutex), NULL);
        err |= pthread_mutex_init(&(config.liveclients_mutex), NULL);
        err |= pthread_mutex_init(&(config.is_fetching_slots_mutex), NULL);
        err |= pthread_mutex_init(&(config.is_updating_slots_mutex), NULL);
        err |= pthread_mutex_init(&(config.updating_slots_mutex), NULL);
        err |= pthread_mutex_init(&(config.slots_last_update_mutex), NULL);
        if (err != 0)
        {
            perror("Failed to initialize mutex");
            exit(EXIT_FAILURE);
        }
    }

    const char *set_value = "abcdefghijklmnopqrstuvwxyz";
    int self_threads = 0;
    char command[63];

    initBenchmarkThreads();
    redisContext *ctx = getRedisContext(config.hostip, config.hostport, config.hostsocket);
    double server_cpu_time, last_server_cpu_time = getServerCpuTime(ctx);
    struct rusage self_ru;
    double self_cpu_time, last_self_cpu_time = getSelfCpuTime(&self_ru);
    double server_cpu_load, last_server_cpu_load = 0, self_cpu_load, server_cpu_gain;
    std::deque<double> load_gain_history = {};
    double current_gain_avg, peak_gain_avg = 0;

    redisReply *reply = (redisReply*)redisCommand(ctx, "INFO CPU");
    if (reply->type != REDIS_REPLY_STRING) {
        freeReplyObject(reply);
        printf("Error executing INFO command. Exiting.\r\n");
        return 1;
    }
    unsigned int server_threads;
    if (extractPropertyFromInfo(reply->str, "server_threads", server_threads)) {
        printf("Error reading server threads from INFO command. Exiting.\r\n");
        return 1;
    }
    freeReplyObject(reply);

    printf("Server has %d threads.\nStarting...\n", server_threads);
    fflush(stdout);

    while (self_threads < config.max_threads) {
        for (int i = 0; i < config.numclients; i++) {
            snprintf(command, 63, "SET %d %s\r\n", self_threads * config.numclients + i, set_value);
            createClient(command, strlen(command), NULL,self_threads);
        }

        benchmarkThread *t = config.threads[self_threads];
        if (pthread_create(&(t->thread), NULL, execBenchmarkThread, t)){
            fprintf(stderr, "FATAL: Failed to start thread %d. Exiting.\n", self_threads);
            exit(1);
        }
        self_threads++;

        usleep(config.period_ms * 1000);
        
        server_cpu_time = getServerCpuTime(ctx);
        self_cpu_time = getSelfCpuTime(&self_ru);
        server_cpu_load = (server_cpu_time - last_server_cpu_time) * 100000 / config.period_ms;
        self_cpu_load = (self_cpu_time - last_self_cpu_time) * 100000 / config.period_ms;
        if (server_cpu_time < 0) {
            break;
        }
        printf("%d threads, %d total clients. CPU Usage Self: %.1f%% (%.1f%% per thread), Server: %.1f%% (%.1f%% per thread)\r",
                self_threads,
                self_threads * config.numclients,
                self_cpu_load,
                self_cpu_load / self_threads,
                server_cpu_load,
                server_cpu_load / server_threads);
        fflush(stdout);
        server_cpu_gain = server_cpu_load - last_server_cpu_load;
        load_gain_history.push_back(server_cpu_gain);
        if (load_gain_history.size() > 5) {
            load_gain_history.pop_front();
        }
        current_gain_avg = getMean(&load_gain_history);
        if (current_gain_avg > peak_gain_avg) {
            peak_gain_avg = current_gain_avg;
        }
        last_server_cpu_time = server_cpu_time;
        last_self_cpu_time = self_cpu_time;
        last_server_cpu_load = server_cpu_load;

        if (isAtFullLoad(server_cpu_load, server_threads)) {
            printf("\nServer is at full CPU load. If higher performance is expected, check server configuration.\n");
            break;
        }

        if (current_gain_avg <= 0.05 * peak_gain_avg) {
            printf("\nServer CPU load appears to have stagnated with increasing clients.\n"
                   "Server does not appear to be at full load. Check network for throughput.\n");
            break;
        }

        if (self_threads * config.numclients > 2000) {
            printf("\nClient limit of 2000 reached. Server is not at full load and appears to be increasing.\n"
                   "2000 clients should be more than enough to reach a bottleneck. Check all configuration.\n");
        }
    }

    printf("Done.\n");

    freeAllClients();
    freeBenchmarkThreads();

    return 0;
}
 
