/*
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
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

#ifndef __REDIS_H
#define __REDIS_H

#define TRUE 1
#define FALSE 0

#include "fmacros.h"
#include "config.h"
#include "solarisfixes.h"
#include "rio.h"
#include "atomicvar.h"

#include <concurrentqueue.h>
#include <blockingconcurrentqueue.h>

#include <stdio.h>
#include <stdlib.h>
#include <cmath>
#include <string.h>
#include <string>
#include <time.h>
#include <limits.h>
#include <unistd.h>
#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <syslog.h>
#include <netinet/in.h>
#include <atomic>
#include <vector>
#include <algorithm>
#include <memory>
#include <set>
#include <map>
#include <string>
#include <mutex>
#include <unordered_set>
#ifdef __cplusplus
extern "C" {
#include <lua.h>
}
#else
#include <lua.h>
#endif
#include <sys/socket.h>
#include <signal.h>

#ifdef HAVE_LIBSYSTEMD
#include <systemd/sd-daemon.h>
#endif

typedef long long mstime_t; /* millisecond time type. */
typedef long long ustime_t; /* microsecond time type. */

#include "fastlock.h"
#include "ae.h"      /* Event driven programming library */
#include "sds.h"     /* Dynamic safe strings */
#include "dict.h"    /* Hash tables */
#include "adlist.h"  /* Linked lists */
#include "zmalloc.h" /* total memory usage aware version of malloc/free */
#include "anet.h"    /* Networking the easy way */
#include "ziplist.h" /* Compact list data structure */
#include "intset.h"  /* Compact integer set structure */
#include "version.h" /* Version macro */
#include "util.h"    /* Misc functions useful in many places */
#include "latency.h" /* Latency monitor API */
#include "sparkline.h" /* ASCII graphs API */
#include "quicklist.h"  /* Lists are encoded as linked lists of
                           N-elements flat arrays */
#include "rax.h"     /* Radix tree */
#include "uuid.h"
#include "semiorderedset.h"
#include "connection.h" /* Connection abstraction */
#include "serverassert.h"
#include "expire.h"
#include "readwritelock.h"

#define REDISMODULE_CORE 1
#include "redismodule.h"    /* Redis modules API defines. */

/* Following includes allow test functions to be called from Redis main() */
#include "zipmap.h"
#include "sha1.h"
#include "endianconv.h"
#include "crc64.h"
#include "IStorage.h"
#include "StorageCache.h"
#include "AsyncWorkQueue.h"
#include "gc.h"

#define FImplies(x, y) (!(x) || (y))

#define LOADING_BOOT 1
#define LOADING_REPLICATION 2

#define OVERLOAD_PROTECT_PERIOD_MS 10'000 // 10 seconds
#define MAX_CLIENTS_SHED_PER_PERIOD (OVERLOAD_PROTECT_PERIOD_MS / 10)  // Restrict to one client per 10ms

extern int g_fTestMode;
extern struct redisServer *g_pserver;

struct redisObject;
class robj_roptr
{
    const redisObject *m_ptr;

public:
    robj_roptr()
        : m_ptr(nullptr)
        {}
    robj_roptr(const redisObject *ptr)
        : m_ptr(ptr)
        {}
    robj_roptr(const robj_roptr&) = default;
    robj_roptr(robj_roptr&&) = default;

    robj_roptr &operator=(const robj_roptr&) = default;
    robj_roptr &operator=(const redisObject *ptr)
    {
        m_ptr = ptr;
        return *this;
    }

    bool operator==(const robj_roptr &other) const
    {
        return m_ptr == other.m_ptr;
    }

    bool operator!=(const robj_roptr &other) const
    {
        return m_ptr != other.m_ptr;
    }

    const redisObject* operator->() const
    {
        return m_ptr;
    }

    const redisObject& operator*() const
    {
        return *m_ptr;
    }

    bool operator!() const
    {
        return !m_ptr;
    }

    operator bool() const{
        return !!m_ptr;
    }

    redisObject *unsafe_robjcast()
    {
        return (redisObject*)m_ptr;
    }
};

class unique_sds_ptr
{
    sds m_str;

public:
    unique_sds_ptr()
        : m_str(nullptr)
        {}
    explicit unique_sds_ptr(sds str)
        : m_str(str)
        {}
    
    ~unique_sds_ptr()
    {
        if (m_str)
            sdsfree(m_str);
    }

    unique_sds_ptr(unique_sds_ptr &&other)
    {
        m_str = other.m_str;
        other.m_str = nullptr;
    }

    bool operator==(const unique_sds_ptr &other) const
    {
        return m_str == other.m_str;
    }

    bool operator!=(const unique_sds_ptr &other) const
    {
        return m_str != other.m_str;
    }

    sds operator->() const
    {
        return m_str;
    }

    bool operator!() const
    {
        return !m_str;
    }

    bool operator<(const unique_sds_ptr &other) const { return m_str < other.m_str; }

    sds get() const { return m_str; }
};

void decrRefCount(robj_roptr o);
void incrRefCount(robj_roptr o);
class robj_sharedptr
{
    redisObject *m_ptr;

public:
    robj_sharedptr()
    : m_ptr(nullptr)
    {}
    explicit robj_sharedptr(redisObject *ptr)
    : m_ptr(ptr)
    {
        if(m_ptr)
            incrRefCount(ptr);
    }
    ~robj_sharedptr()
    {
        if (m_ptr)
            decrRefCount(m_ptr);
    }
    robj_sharedptr(const robj_sharedptr& other)
    : m_ptr(other.m_ptr)
    {        
        if(m_ptr)
            incrRefCount(m_ptr);
    }

    robj_sharedptr(robj_sharedptr&& other)
    {
        m_ptr = other.m_ptr;
        other.m_ptr = nullptr;
    }

    robj_sharedptr &operator=(const robj_sharedptr& other)
    {
        robj_sharedptr tmp(other);
        using std::swap;
        swap(m_ptr, tmp.m_ptr);
        return *this;
    }
    robj_sharedptr &operator=(redisObject *ptr)
    {
        robj_sharedptr tmp(ptr);
        using std::swap;
        swap(m_ptr, tmp.m_ptr);
        return *this;
    }
    
    redisObject* operator->() const
    {
        return m_ptr;
    }

    bool operator!() const
    {
        return !m_ptr;
    }

    explicit operator bool() const{
        return !!m_ptr;
    }

    operator redisObject *()
    {
        return (redisObject*)m_ptr;
    }

    redisObject *get() { return m_ptr; }
    const redisObject *get() const { return m_ptr; }
};

inline bool operator==(const robj_sharedptr &lhs, const robj_sharedptr &rhs)
{
    return lhs.get() == rhs.get();
}

inline bool operator!=(const robj_sharedptr &lhs, const robj_sharedptr &rhs)
{
    return !(lhs == rhs);
}

inline bool operator==(const robj_sharedptr &lhs, const void *p)
{
    return lhs.get() == p;
}

inline bool operator==(const void *p, const robj_sharedptr &rhs)
{
    return rhs == p;
}

inline bool operator!=(const robj_sharedptr &lhs, const void *p)
{
    return !(lhs == p);
}

inline bool operator!=(const void *p, const robj_sharedptr &rhs)
{
    return !(rhs == p);
}

/* Error codes */
#define C_OK                    0
#define C_ERR                   -1

/* Static server configuration */
#define CONFIG_DEFAULT_HZ        10             /* Time interrupt calls/sec. */
#define CONFIG_MIN_HZ            1
#define CONFIG_MAX_HZ            500
#define MAX_CLIENTS_PER_CLOCK_TICK 200          /* HZ is adapted based on that. */
#define CONFIG_MAX_LINE    1024
#define CRON_DBS_PER_CALL 16
#define NET_MAX_WRITES_PER_EVENT (1024*64)
#define PROTO_SHARED_SELECT_CMDS 10
#define OBJ_SHARED_INTEGERS 10000
#define OBJ_SHARED_BULKHDR_LEN 32
#define LOG_MAX_LEN    1024 /* Default maximum length of syslog messages.*/
#define AOF_REWRITE_ITEMS_PER_CMD 64
#define AOF_READ_DIFF_INTERVAL_BYTES (1024*10)
#define CONFIG_AUTHPASS_MAX_LEN 512
#define CONFIG_RUN_ID_SIZE 40
#define RDB_EOF_MARK_SIZE 40
#define CONFIG_REPL_BACKLOG_MIN_SIZE (1024*16)          /* 16k */
#define CONFIG_BGSAVE_RETRY_DELAY 5 /* Wait a few secs before trying again. */
#define CONFIG_DEFAULT_PID_FILE "/var/run/keydb.pid"
#define CONFIG_DEFAULT_CLUSTER_CONFIG_FILE "nodes.conf"
#define CONFIG_DEFAULT_UNIX_SOCKET_PERM 0
#define CONFIG_DEFAULT_LOGFILE ""
#define NET_HOST_STR_LEN 256 /* Longest valid hostname */
#define NET_IP_STR_LEN 46 /* INET6_ADDRSTRLEN is 46, but we need to be sure */
#define NET_ADDR_STR_LEN (NET_IP_STR_LEN+32) /* Must be enough for ip:port */
#define NET_HOST_PORT_STR_LEN (NET_HOST_STR_LEN+32) /* Must be enough for hostname:port */
#define CONFIG_BINDADDR_MAX 16
#define CONFIG_MIN_RESERVED_FDS 32
#define CONFIG_DEFAULT_THREADS 1
#define CONFIG_DEFAULT_THREAD_AFFINITY 0
#define CONFIG_DEFAULT_PROC_TITLE_TEMPLATE "{title} {listen-addr} {server-mode}"

#define CONFIG_DEFAULT_ACTIVE_REPLICA 0
#define CONFIG_DEFAULT_ENABLE_MULTIMASTER 0

#define ACTIVE_EXPIRE_CYCLE_LOOKUPS_PER_LOOP 64 /* Loopkups per loop. */
#define ACTIVE_EXPIRE_CYCLE_SUBKEY_LOOKUPS_PER_LOOP 16384 /* Subkey loopkups per loop. */
#define ACTIVE_EXPIRE_CYCLE_FAST_DURATION 1000 /* Microseconds */
#define ACTIVE_EXPIRE_CYCLE_SLOW_TIME_PERC 25 /* CPU max % for keys collection */
#define ACTIVE_EXPIRE_CYCLE_SLOW 0
#define ACTIVE_EXPIRE_CYCLE_FAST 1

/* Children process will exit with this status code to signal that the
 * process terminated without an error: this is useful in order to kill
 * a saving child (RDB or AOF one), without triggering in the parent the
 * write protection that is normally turned on on write errors.
 * Usually children that are terminated with SIGUSR1 will exit with this
 * special code. */
#define SERVER_CHILD_NOERROR_RETVAL    255

/* Reading copy-on-write info is sometimes expensive and may slow down child
 * processes that report it continuously. We measure the cost of obtaining it
 * and hold back additional reading based on this factor. */
#define CHILD_COW_DUTY_CYCLE           100

/* Instantaneous metrics tracking. */
#define STATS_METRIC_SAMPLES 16     /* Number of samples per metric. */
#define STATS_METRIC_COMMAND 0      /* Number of commands executed. */
#define STATS_METRIC_NET_INPUT 1    /* Bytes read to network .*/
#define STATS_METRIC_NET_OUTPUT 2   /* Bytes written to network. */
#define STATS_METRIC_COUNT 3

/* Protocol and I/O related defines */
#define PROTO_IOBUF_LEN         (1024*16)  /* Generic I/O buffer size */
#define PROTO_REPLY_CHUNK_BYTES (16*1024) /* 16k output buffer */
#define PROTO_ASYNC_REPLY_CHUNK_BYTES (1024)
#define PROTO_INLINE_MAX_SIZE   (1024*64) /* Max size of inline reads */
#define PROTO_MBULK_BIG_ARG     (1024*32)
#define LONG_STR_SIZE      21          /* Bytes needed for long -> str + '\0' */
#define REDIS_AUTOSYNC_BYTES (1024*1024*32) /* fdatasync every 32MB */

#define LIMIT_PENDING_QUERYBUF (4*1024*1024) /* 4mb */

/* When configuring the server eventloop, we setup it so that the total number
 * of file descriptors we can handle are g_pserver->maxclients + RESERVED_FDS +
 * a few more to stay safe. Since RESERVED_FDS defaults to 32, we add 96
 * in order to make sure of not over provisioning more than 128 fds. */
#define CONFIG_FDSET_INCR (CONFIG_MIN_RESERVED_FDS+96)

/* OOM Score Adjustment classes. */
#define CONFIG_OOM_MASTER 0
#define CONFIG_OOM_REPLICA 1
#define CONFIG_OOM_BGCHILD 2
#define CONFIG_OOM_COUNT 3

extern int configOOMScoreAdjValuesDefaults[CONFIG_OOM_COUNT];

/* Hash table parameters */
#define HASHTABLE_MIN_FILL        10      /* Minimal hash table fill 10% */
#define HASHTABLE_MAX_LOAD_FACTOR 1.618   /* Maximum hash table load factor. */

/* Command flags. Please check the command table defined in the server.cpp file
 * for more information about the meaning of every flag. */
#define CMD_WRITE (1ULL<<0)            /* "write" flag */
#define CMD_READONLY (1ULL<<1)         /* "read-only" flag */
#define CMD_DENYOOM (1ULL<<2)          /* "use-memory" flag */
#define CMD_MODULE (1ULL<<3)           /* Command exported by module. */
#define CMD_ADMIN (1ULL<<4)            /* "admin" flag */
#define CMD_PUBSUB (1ULL<<5)           /* "pub-sub" flag */
#define CMD_NOSCRIPT (1ULL<<6)         /* "no-script" flag */
#define CMD_RANDOM (1ULL<<7)           /* "random" flag */
#define CMD_SORT_FOR_SCRIPT (1ULL<<8)  /* "to-sort" flag */
#define CMD_LOADING (1ULL<<9)          /* "ok-loading" flag */
#define CMD_STALE (1ULL<<10)           /* "ok-stale" flag */
#define CMD_SKIP_MONITOR (1ULL<<11)    /* "no-monitor" flag */
#define CMD_SKIP_SLOWLOG (1ULL<<12)    /* "no-slowlog" flag */
#define CMD_ASKING (1ULL<<13)          /* "cluster-asking" flag */
#define CMD_FAST (1ULL<<14)            /* "fast" flag */
#define CMD_NO_AUTH (1ULL<<15)         /* "no-auth" flag */
#define CMD_MAY_REPLICATE (1ULL<<16)   /* "may-replicate" flag */

/* Command flags used by the module system. */
#define CMD_MODULE_GETKEYS (1ULL<<17)  /* Use the modules getkeys interface. */
#define CMD_MODULE_NO_CLUSTER (1ULL<<18) /* Deny on Redis Cluster. */

/* Command flags that describe ACLs categories. */
#define CMD_CATEGORY_KEYSPACE (1ULL<<18)
#define CMD_CATEGORY_READ (1ULL<<19)
#define CMD_CATEGORY_WRITE (1ULL<<20)
#define CMD_CATEGORY_SET (1ULL<<21)
#define CMD_CATEGORY_SORTEDSET (1ULL<<22)
#define CMD_CATEGORY_LIST (1ULL<<23)
#define CMD_CATEGORY_HASH (1ULL<<24)
#define CMD_CATEGORY_STRING (1ULL<<25)
#define CMD_CATEGORY_BITMAP (1ULL<<26)
#define CMD_CATEGORY_HYPERLOGLOG (1ULL<<27)
#define CMD_CATEGORY_GEO (1ULL<<28)
#define CMD_CATEGORY_STREAM (1ULL<<29)
#define CMD_CATEGORY_PUBSUB (1ULL<<30)
#define CMD_CATEGORY_ADMIN (1ULL<<31)
#define CMD_CATEGORY_FAST (1ULL<<32)
#define CMD_CATEGORY_SLOW (1ULL<<33)
#define CMD_CATEGORY_BLOCKING (1ULL<<34)
#define CMD_CATEGORY_DANGEROUS (1ULL<<35)
#define CMD_CATEGORY_CONNECTION (1ULL<<36)
#define CMD_CATEGORY_TRANSACTION (1ULL<<37)
#define CMD_CATEGORY_SCRIPTING (1ULL<<38)
#define CMD_CATEGORY_REPLICATION (1ULL<<39)
#define CMD_SKIP_PROPOGATE (1ULL<<40)  /* "noprop" flag */
#define CMD_ASYNC_OK (1ULL<<41) /* This command is safe without a lock */

/* AOF states */
#define AOF_OFF 0             /* AOF is off */
#define AOF_ON 1              /* AOF is on */
#define AOF_WAIT_REWRITE 2    /* AOF waits rewrite to start appending */

/* Client flags */
#define CLIENT_SLAVE (1<<0)   /* This client is a replica */
#define CLIENT_MASTER (1<<1)  /* This client is a master */
#define CLIENT_MONITOR (1<<2) /* This client is a replica monitor, see MONITOR */
#define CLIENT_MULTI (1<<3)   /* This client is in a MULTI context */
#define CLIENT_BLOCKED (1<<4) /* The client is waiting in a blocking operation */
#define CLIENT_DIRTY_CAS (1<<5) /* Watched keys modified. EXEC will fail. */
#define CLIENT_CLOSE_AFTER_REPLY (1<<6) /* Close after writing entire reply. */
#define CLIENT_UNBLOCKED (1<<7) /* This client was unblocked and is stored in
                                  g_pserver->unblocked_clients */
#define CLIENT_LUA (1<<8) /* This is a non connected client used by Lua */
#define CLIENT_ASKING (1<<9)     /* Client issued the ASKING command */
#define CLIENT_CLOSE_ASAP (1<<10)/* Close this client ASAP */
#define CLIENT_UNIX_SOCKET (1<<11) /* Client connected via Unix domain socket */
#define CLIENT_DIRTY_EXEC (1<<12)  /* EXEC will fail for errors while queueing */
#define CLIENT_MASTER_FORCE_REPLY (1<<13)  /* Queue replies even if is master */
#define CLIENT_FORCE_AOF (1<<14)   /* Force AOF propagation of current cmd. */
#define CLIENT_FORCE_REPL (1<<15)  /* Force replication of current cmd. */
#define CLIENT_PRE_PSYNC (1<<16)   /* Instance don't understand PSYNC. */
#define CLIENT_READONLY (1<<17)    /* Cluster client is in read-only state. */
#define CLIENT_PUBSUB (1<<18)      /* Client is in Pub/Sub mode. */
#define CLIENT_PREVENT_AOF_PROP (1<<19)  /* Don't propagate to AOF. */
#define CLIENT_PREVENT_REPL_PROP (1<<20)  /* Don't propagate to slaves. */
#define CLIENT_PREVENT_PROP (CLIENT_PREVENT_AOF_PROP|CLIENT_PREVENT_REPL_PROP)
#define CLIENT_IGNORE_SOFT_SHUTDOWN (CLIENT_MASTER | CLIENT_SLAVE | CLIENT_BLOCKED | CLIENT_MONITOR)
#define CLIENT_PENDING_WRITE (1<<21) /* Client has output to send but a write
                                        handler is yet not installed. */
#define CLIENT_REPLY_OFF (1<<22)   /* Don't send replies to client. */
#define CLIENT_REPLY_SKIP_NEXT (1<<23)  /* Set CLIENT_REPLY_SKIP for next cmd */
#define CLIENT_REPLY_SKIP (1<<24)  /* Don't send just this reply. */
#define CLIENT_LUA_DEBUG (1<<25)  /* Run EVAL in debug mode. */
#define CLIENT_LUA_DEBUG_SYNC (1<<26)  /* EVAL debugging without fork() */
#define CLIENT_MODULE (1<<27) /* Non connected client used by some module. */
#define CLIENT_PROTECTED (1<<28) /* Client should not be freed for now. */
#define CLIENT_PENDING_COMMAND (1<<29) /* Indicates the client has a fully
                                        * parsed command ready for execution. */
#define CLIENT_EXECUTING_COMMAND (1<<30) /* Used to handle reentrency cases in processCommandWhileBlocked 
                                            to ensure we don't process a client already executing */
#define CLIENT_TRACKING (1ULL<<31) /* Client enabled keys tracking in order to
                                   perform client side caching. */
#define CLIENT_TRACKING_BROKEN_REDIR (1ULL<<32) /* Target client is invalid. */
#define CLIENT_TRACKING_BCAST (1ULL<<33) /* Tracking in BCAST mode. */
#define CLIENT_TRACKING_OPTIN (1ULL<<34)  /* Tracking in opt-in mode. */
#define CLIENT_TRACKING_OPTOUT (1ULL<<35) /* Tracking in opt-out mode. */
#define CLIENT_TRACKING_CACHING (1ULL<<36) /* CACHING yes/no was given,
                                              depending on optin/optout mode. */
#define CLIENT_TRACKING_NOLOOP (1ULL<<37) /* Don't send invalidation messages
                                             about writes performed by myself.*/
#define CLIENT_IN_TO_TABLE (1ULL<<38) /* This client is in the timeout table. */
#define CLIENT_PROTOCOL_ERROR (1ULL<<39) /* Protocol error chatting with it. */
#define CLIENT_CLOSE_AFTER_COMMAND (1ULL<<40) /* Close after executing commands
                                               * and writing entire reply. */
#define CLIENT_DENY_BLOCKING (1ULL<<41) /* Indicate that the client should not be blocked.
                                           currently, turned on inside MULTI, Lua, RM_Call,
                                           and AOF client */
#define CLIENT_REPL_RDBONLY (1ULL<<42) /* This client is a replica that only wants
                                          RDB without replication buffer. */
#define CLIENT_FORCE_REPLY (1ULL<<44) /* Should addReply be forced to write the text? */
#define CLIENT_AUDIT_LOGGING (1ULL<<45) /* Client commands required audit logging */

/* Client block type (btype field in client structure)
 * if CLIENT_BLOCKED flag is set. */
#define BLOCKED_NONE 0    /* Not blocked, no CLIENT_BLOCKED flag set. */
#define BLOCKED_LIST 1    /* BLPOP & co. */
#define BLOCKED_WAIT 2    /* WAIT for synchronous replication. */
#define BLOCKED_MODULE 3  /* Blocked by a loadable module. */
#define BLOCKED_STREAM 4  /* XREAD. */
#define BLOCKED_ZSET 5    /* BZPOP et al. */
#define BLOCKED_PAUSE 6   /* Blocked by CLIENT PAUSE */
#define BLOCKED_ASYNC 7
#define BLOCKED_STORAGE 8
#define BLOCKED_NUM 9     /* Number of blocked states. */

/* Client request types */
#define PROTO_REQ_INLINE 1
#define PROTO_REQ_MULTIBULK 2

/* Client classes for client limits, currently used only for
 * the max-client-output-buffer limit implementation. */
#define CLIENT_TYPE_NORMAL 0 /* Normal req-reply clients + MONITORs */
#define CLIENT_TYPE_SLAVE 1  /* Slaves. */
#define CLIENT_TYPE_PUBSUB 2 /* Clients subscribed to PubSub channels. */
#define CLIENT_TYPE_MASTER 3 /* Master. */
#define CLIENT_TYPE_COUNT 4  /* Total number of client types. */
#define CLIENT_TYPE_OBUF_COUNT 3 /* Number of clients to expose to output
                                    buffer configuration. Just the first
                                    three: normal, replica, pubsub. */

/* Slave replication state. Used in g_pserver->repl_state for slaves to remember
 * what to do next. */
typedef enum {
    REPL_STATE_NONE = 0,            /* No active replication */
    REPL_STATE_CONNECT,             /* Must connect to master */
    REPL_STATE_CONNECTING,          /* Connecting to master */
    REPL_STATE_RETRY_NOREPLPING,    /* Master does not support REPLPING, retry with PING */
    /* --- Handshake states, must be ordered --- */
    REPL_STATE_RECEIVE_PING_REPLY,  /* Wait for PING reply */
    REPL_STATE_SEND_HANDSHAKE,      /* Send handshake sequance to master */
    REPL_STATE_RECEIVE_AUTH_REPLY,  /* Wait for AUTH reply */
    REPL_STATE_RECEIVE_PORT_REPLY,  /* Wait for REPLCONF reply */
    REPL_STATE_RECEIVE_IP_REPLY,    /* Wait for REPLCONF reply */
    REPL_STATE_RECEIVE_CAPA_REPLY,  /* Wait for REPLCONF reply */
    REPL_STATE_RECEIVE_UUID,        /* they should ack with their UUID */
    REPL_STATE_SEND_PSYNC,          /* Send PSYNC */
    REPL_STATE_RECEIVE_PSYNC_REPLY, /* Wait for PSYNC reply */
    /* --- End of handshake states --- */
    REPL_STATE_TRANSFER,        /* Receiving .rdb from master */
    REPL_STATE_CONNECTED,       /* Connected to master */
} repl_state;

/* The state of an in progress coordinated failover */
typedef enum {
    NO_FAILOVER = 0,        /* No failover in progress */
    FAILOVER_WAIT_FOR_SYNC, /* Waiting for target replica to catch up */
    FAILOVER_IN_PROGRESS    /* Waiting for target replica to accept
                             * PSYNC FAILOVER request. */
} failover_state;

/* State of slaves from the POV of the master. Used in client->replstate.
 * In SEND_BULK and ONLINE state the replica receives new updates
 * in its output queue. In the WAIT_BGSAVE states instead the server is waiting
 * to start the next background saving in order to send updates to it. */
#define SLAVE_STATE_WAIT_BGSAVE_START 6 /* We need to produce a new RDB file. */
#define SLAVE_STATE_WAIT_BGSAVE_END 7 /* Waiting RDB file creation to finish. */
#define SLAVE_STATE_SEND_BULK 8 /* Sending RDB file to replica. */
#define SLAVE_STATE_ONLINE 9 /* RDB file transmitted, sending just updates. */
#define SLAVE_STATE_FASTSYNC_TX 10
#define SLAVE_STATE_FASTSYNC_DONE 11

/* Slave capabilities. */
#define SLAVE_CAPA_NONE 0
#define SLAVE_CAPA_EOF (1<<0)    /* Can parse the RDB EOF streaming format. */
#define SLAVE_CAPA_PSYNC2 (1<<1) /* Supports PSYNC2 protocol. */
#define SLAVE_CAPA_ACTIVE_EXPIRE (1<<2) /* Will the slave perform its own expirations? (Don't send delete) */
#define SLAVE_CAPA_ROCKSDB_SNAPSHOT (1<<3)

/* Synchronous read timeout - replica side */
#define CONFIG_REPL_SYNCIO_TIMEOUT 5

/* List related stuff */
#define LIST_HEAD 0
#define LIST_TAIL 1
#define ZSET_MIN 0
#define ZSET_MAX 1

/* Sort operations */
#define SORT_OP_GET 0

/* Log levels */
#define LL_DEBUG 0
#define LL_VERBOSE 1
#define LL_NOTICE 2
#define LL_WARNING 3
#define LL_RAW (1<<10) /* Modifier to log without timestamp */

/* Error severity levels */
#define ERR_CRITICAL 0
#define ERR_ERROR 1
#define ERR_WARNING 2
#define ERR_NOTICE 3

/* Supervision options */
#define SUPERVISED_NONE 0
#define SUPERVISED_AUTODETECT 1
#define SUPERVISED_SYSTEMD 2
#define SUPERVISED_UPSTART 3

/* Anti-warning macro... */
#define UNUSED(V) ((void) V)

#define ZSKIPLIST_MAXLEVEL 32 /* Should be enough for 2^64 elements */
#define ZSKIPLIST_P 0.25      /* Skiplist P = 1/4 */

/* Append only defines */
#define AOF_FSYNC_NO 0
#define AOF_FSYNC_ALWAYS 1
#define AOF_FSYNC_EVERYSEC 2

/* Replication diskless load defines */
#define REPL_DISKLESS_LOAD_DISABLED 0
#define REPL_DISKLESS_LOAD_WHEN_DB_EMPTY 1
#define REPL_DISKLESS_LOAD_SWAPDB 2

/* Storage Memory Model Defines */
#define STORAGE_WRITEBACK 0
#define STORAGE_WRITETHROUGH 1

/* TLS Client Authentication */
#define TLS_CLIENT_AUTH_NO 0
#define TLS_CLIENT_AUTH_YES 1
#define TLS_CLIENT_AUTH_OPTIONAL 2

/* Sanitize dump payload */
#define SANITIZE_DUMP_NO 0
#define SANITIZE_DUMP_YES 1
#define SANITIZE_DUMP_CLIENTS 2

/* Sets operations codes */
#define SET_OP_UNION 0
#define SET_OP_DIFF 1
#define SET_OP_INTER 2

/* oom-score-adj defines */
#define OOM_SCORE_ADJ_NO 0
#define OOM_SCORE_RELATIVE 1
#define OOM_SCORE_ADJ_ABSOLUTE 2

/* Redis maxmemory strategies. Instead of using just incremental number
 * for this defines, we use a set of flags so that testing for certain
 * properties common to multiple policies is faster. */
#define MAXMEMORY_FLAG_LRU (1<<0)
#define MAXMEMORY_FLAG_LFU (1<<1)
#define MAXMEMORY_FLAG_ALLKEYS (1<<2)
#define MAXMEMORY_FLAG_NO_SHARED_INTEGERS \
    (MAXMEMORY_FLAG_LRU|MAXMEMORY_FLAG_LFU)

#define MAXMEMORY_VOLATILE_LRU ((0<<8)|MAXMEMORY_FLAG_LRU)
#define MAXMEMORY_VOLATILE_LFU ((1<<8)|MAXMEMORY_FLAG_LFU)
#define MAXMEMORY_VOLATILE_TTL (2<<8)
#define MAXMEMORY_VOLATILE_RANDOM (3<<8)
#define MAXMEMORY_ALLKEYS_LRU ((4<<8)|MAXMEMORY_FLAG_LRU|MAXMEMORY_FLAG_ALLKEYS)
#define MAXMEMORY_ALLKEYS_LFU ((5<<8)|MAXMEMORY_FLAG_LFU|MAXMEMORY_FLAG_ALLKEYS)
#define MAXMEMORY_ALLKEYS_RANDOM ((6<<8)|MAXMEMORY_FLAG_ALLKEYS)
#define MAXMEMORY_NO_EVICTION (7<<8)

/* Units */
#define UNIT_SECONDS 0
#define UNIT_MILLISECONDS 1

/* SHUTDOWN flags */
#define SHUTDOWN_NOFLAGS 0      /* No flags. */
#define SHUTDOWN_SAVE 1         /* Force SAVE on SHUTDOWN even if no save
                                   points are configured. */
#define SHUTDOWN_NOSAVE 2       /* Don't SAVE on SHUTDOWN. */

/* Command call flags, see call() function */
#define CMD_CALL_NONE 0
#define CMD_CALL_SLOWLOG (1<<0)
#define CMD_CALL_STATS (1<<1)
#define CMD_CALL_PROPAGATE_AOF (1<<2)
#define CMD_CALL_PROPAGATE_REPL (1<<3)
#define CMD_CALL_PROPAGATE (CMD_CALL_PROPAGATE_AOF|CMD_CALL_PROPAGATE_REPL)
#define CMD_CALL_FULL (CMD_CALL_SLOWLOG | CMD_CALL_STATS | CMD_CALL_PROPAGATE | CMD_CALL_NOWRAP)
#define CMD_CALL_NOWRAP (1<<4)  /* Don't wrap also propagate array into
                                   MULTI/EXEC: the caller will handle it.  */
#define CMD_CALL_ASYNC (1<<5)

/* Command propagation flags, see propagate() function */
#define PROPAGATE_NONE 0
#define PROPAGATE_AOF 1
#define PROPAGATE_REPL 2

/* Client pause types, larger types are more restrictive
 * pause types than smaller pause types. */
typedef enum {
    CLIENT_PAUSE_OFF = 0, /* Pause no commands */
    CLIENT_PAUSE_WRITE,   /* Pause write commands */
    CLIENT_PAUSE_ALL      /* Pause all commands */
} pause_type;

/* RDB active child save type. */
#define RDB_CHILD_TYPE_NONE 0
#define RDB_CHILD_TYPE_DISK 1     /* RDB is written to disk. */
#define RDB_CHILD_TYPE_SOCKET 2   /* RDB is written to replica socket. */

/* Keyspace changes notification classes. Every class is associated with a
 * character for configuration purposes. */
#define NOTIFY_KEYSPACE (1<<0)    /* K */
#define NOTIFY_KEYEVENT (1<<1)    /* E */
#define NOTIFY_GENERIC (1<<2)     /* g */
#define NOTIFY_STRING (1<<3)      /* $ */
#define NOTIFY_LIST (1<<4)        /* l */
#define NOTIFY_SET (1<<5)         /* s */
#define NOTIFY_HASH (1<<6)        /* h */
#define NOTIFY_ZSET (1<<7)        /* z */
#define NOTIFY_EXPIRED (1<<8)     /* x */
#define NOTIFY_EVICTED (1<<9)     /* e */
#define NOTIFY_STREAM (1<<10)     /* t */
#define NOTIFY_KEY_MISS (1<<11)   /* m (Note: This one is excluded from NOTIFY_ALL on purpose) */
#define NOTIFY_LOADED (1<<12)     /* module only key space notification, indicate a key loaded from rdb */
#define NOTIFY_MODULE (1<<13)     /* d, module key space notification */
#define NOTIFY_ALL (NOTIFY_GENERIC | NOTIFY_STRING | NOTIFY_LIST | NOTIFY_SET | NOTIFY_HASH | NOTIFY_ZSET | NOTIFY_EXPIRED | NOTIFY_EVICTED | NOTIFY_STREAM | NOTIFY_MODULE) /* A flag */

/* Get the first bind addr or NULL */
#define NET_FIRST_BIND_ADDR (g_pserver->bindaddr_count ? g_pserver->bindaddr[0] : NULL)

/* Using the following macro you can run code inside serverCron() with the
 * specified period, specified in milliseconds.
 * The actual resolution depends on g_pserver->hz. */
#define run_with_period(_ms_) if ((_ms_ <= 1000/g_pserver->hz) || !(g_pserver->cronloops%((_ms_)/(1000/g_pserver->hz))))

/*-----------------------------------------------------------------------------
 * Data types
 *----------------------------------------------------------------------------*/

/* A redis object, that is a type able to hold a string / list / set */

/* The actual Redis Object */
#define OBJ_STRING 0     /* String object. */
#define OBJ_LIST 1       /* List object. */
#define OBJ_SET 2        /* Set object. */
#define OBJ_ZSET 3       /* Sorted set object. */
#define OBJ_HASH 4       /* Hash object. */

/* The "module" object type is a special one that signals that the object
 * is one directly managed by a Redis module. In this case the value points
 * to a moduleValue struct, which contains the object value (which is only
 * handled by the module itself) and the RedisModuleType struct which lists
 * function pointers in order to serialize, deserialize, AOF-rewrite and
 * free the object.
 *
 * Inside the RDB file, module types are encoded as OBJ_MODULE followed
 * by a 64 bit module type ID, which has a 54 bits module-specific signature
 * in order to dispatch the loading to the right module, plus a 10 bits
 * encoding version. */
#define OBJ_MODULE 5     /* Module object. */
#define OBJ_STREAM 6     /* Stream object. */
#define OBJ_CRON 7       /* CRON job */
#define OBJ_NESTEDHASH 8 /* Nested Hash Object */

/* Extract encver / signature from a module type ID. */
#define REDISMODULE_TYPE_ENCVER_BITS 10
#define REDISMODULE_TYPE_ENCVER_MASK ((1<<REDISMODULE_TYPE_ENCVER_BITS)-1)
#define REDISMODULE_TYPE_ENCVER(id) (id & REDISMODULE_TYPE_ENCVER_MASK)
#define REDISMODULE_TYPE_SIGN(id) ((id & ~((uint64_t)REDISMODULE_TYPE_ENCVER_MASK)) >>REDISMODULE_TYPE_ENCVER_BITS)

/* Bit flags for moduleTypeAuxSaveFunc */
#define REDISMODULE_AUX_BEFORE_RDB (1<<0)
#define REDISMODULE_AUX_AFTER_RDB (1<<1)

/* Number of cycles before time thread gives up fork lock */
#define MAX_CYCLES_TO_HOLD_FORK_LOCK 10

struct RedisModule;
struct RedisModuleIO;
struct RedisModuleDigest;
struct RedisModuleCtx;
struct redisObject;
struct RedisModuleDefragCtx;

/* Each module type implementation should export a set of methods in order
 * to serialize and deserialize the value in the RDB file, rewrite the AOF
 * log, create the digest for "DEBUG DIGEST", and free the value when a key
 * is deleted. */
typedef void *(*moduleTypeLoadFunc)(struct RedisModuleIO *io, int encver);
typedef void (*moduleTypeSaveFunc)(struct RedisModuleIO *io, void *value);
typedef int (*moduleTypeAuxLoadFunc)(struct RedisModuleIO *rdb, int encver, int when);
typedef void (*moduleTypeAuxSaveFunc)(struct RedisModuleIO *rdb, int when);
typedef void (*moduleTypeRewriteFunc)(struct RedisModuleIO *io, struct redisObject *key, void *value);
typedef void (*moduleTypeDigestFunc)(struct RedisModuleDigest *digest, void *value);
typedef size_t (*moduleTypeMemUsageFunc)(const void *value);
typedef void (*moduleTypeFreeFunc)(void *value);
typedef size_t (*moduleTypeFreeEffortFunc)(struct redisObject *key, const void *value);
typedef void (*moduleTypeUnlinkFunc)(struct redisObject *key, void *value);
typedef void *(*moduleTypeCopyFunc)(struct redisObject *fromkey, struct redisObject *tokey, const void *value);
typedef int (*moduleTypeDefragFunc)(struct RedisModuleDefragCtx *ctx, struct redisObject *key, void **value);

/* This callback type is called by moduleNotifyUserChanged() every time
 * a user authenticated via the module API is associated with a different
 * user or gets disconnected. This needs to be exposed since you can't cast
 * a function pointer to (void *). */
typedef void (*RedisModuleUserChangedFunc) (uint64_t client_id, void *privdata);


/* The module type, which is referenced in each value of a given type, defines
 * the methods and links to the module exporting the type. */
typedef struct RedisModuleType {
    uint64_t id; /* Higher 54 bits of type ID + 10 lower bits of encoding ver. */
    struct RedisModule *module;
    moduleTypeLoadFunc rdb_load;
    moduleTypeSaveFunc rdb_save;
    moduleTypeRewriteFunc aof_rewrite;
    moduleTypeMemUsageFunc mem_usage;
    moduleTypeDigestFunc digest;
    moduleTypeFreeFunc free;
    moduleTypeFreeEffortFunc free_effort;
    moduleTypeUnlinkFunc unlink;
    moduleTypeCopyFunc copy;
    moduleTypeDefragFunc defrag;
    moduleTypeAuxLoadFunc aux_load;
    moduleTypeAuxSaveFunc aux_save;
    int aux_save_triggers;
    char name[10]; /* 9 bytes name + null term. Charset: A-Z a-z 0-9 _- */
} moduleType;

/* In Redis objects 'robj' structures of type OBJ_MODULE, the value pointer
 * is set to the following structure, referencing the moduleType structure
 * in order to work with the value, and at the same time providing a raw
 * pointer to the value, as created by the module commands operating with
 * the module type.
 *
 * So for example in order to free such a value, it is possible to use
 * the following code:
 *
 *  if (robj->type == OBJ_MODULE) {
 *      moduleValue *mt = robj->ptr;
 *      mt->type->free(mt->value);
 *      zfree(mt); // We need to release this in-the-middle struct as well.
 *  }
 */
typedef struct moduleValue {
    moduleType *type;
    void *value;
} moduleValue;

/* This is a wrapper for the 'rio' streams used inside rdb.c in Redis, so that
 * the user does not have to take the total count of the written bytes nor
 * to care about error conditions. */
typedef struct RedisModuleIO {
    size_t bytes;       /* Bytes read / written so far. */
    rio *prio;           /* Rio stream. */
    moduleType *type;   /* Module type doing the operation. */
    int error;          /* True if error condition happened. */
    int ver;            /* Module serialization version: 1 (old),
                         * 2 (current version with opcodes annotation). */
    struct RedisModuleCtx *ctx; /* Optional context, see RM_GetContextFromIO()*/
    struct redisObject *key;    /* Optional name of key processed */
} RedisModuleIO;

/* Macro to initialize an IO context. Note that the 'ver' field is populated
 * inside rdb.c according to the version of the value to load. */
#define moduleInitIOContext(iovar,mtype,rioptr,keyptr) do { \
    iovar.prio = rioptr; \
    iovar.type = mtype; \
    iovar.bytes = 0; \
    iovar.error = 0; \
    iovar.ver = 0; \
    iovar.key = keyptr; \
    iovar.ctx = NULL; \
} while(0)

/* This is a structure used to export DEBUG DIGEST capabilities to Redis
 * modules. We want to capture both the ordered and unordered elements of
 * a data structure, so that a digest can be created in a way that correctly
 * reflects the values. See the DEBUG DIGEST command implementation for more
 * background. */
typedef struct RedisModuleDigest {
    unsigned char o[20];    /* Ordered elements. */
    unsigned char x[20];    /* Xored elements. */
} RedisModuleDigest;

/* Just start with a digest composed of all zero bytes. */
#define moduleInitDigestContext(mdvar) do { \
    memset(mdvar.o,0,sizeof(mdvar.o)); \
    memset(mdvar.x,0,sizeof(mdvar.x)); \
} while(0)

/* Objects encoding. Some kind of objects like Strings and Hashes can be
 * internally represented in multiple ways. The 'encoding' field of the object
 * is set to one of this fields for this object. */
#define OBJ_ENCODING_RAW 0     /* Raw representation */
#define OBJ_ENCODING_INT 1     /* Encoded as integer */
#define OBJ_ENCODING_HT 2      /* Encoded as hash table */
#define OBJ_ENCODING_ZIPMAP 3  /* Encoded as zipmap */
#define OBJ_ENCODING_LINKEDLIST 4 /* No longer used: old list encoding. */
#define OBJ_ENCODING_ZIPLIST 5 /* Encoded as ziplist */
#define OBJ_ENCODING_INTSET 6  /* Encoded as intset */
#define OBJ_ENCODING_SKIPLIST 7  /* Encoded as skiplist */
#define OBJ_ENCODING_EMBSTR 8  /* Embedded sds string encoding */
#define OBJ_ENCODING_QUICKLIST 9 /* Encoded as linked list of ziplists */
#define OBJ_ENCODING_STREAM 10 /* Encoded as a radix tree of listpacks */

#define LRU_BITS 24
#define LRU_CLOCK_MAX ((1<<LRU_BITS)-1) /* Max value of obj->lru */
#define LRU_CLOCK_RESOLUTION 1000 /* LRU clock resolution in ms */

#define OBJ_SHARED_REFCOUNT (0x7FFFFFFF) 
#define OBJ_STATIC_REFCOUNT (OBJ_SHARED_REFCOUNT-1)
#define OBJ_FIRST_SPECIAL_REFCOUNT OBJ_STATIC_REFCOUNT
#define OBJ_MVCC_INVALID (0xFFFFFFFFFFFFFFFFULL)

#define MVCC_MS_SHIFT 20

// This struct will be allocated ahead of the ROBJ when needed
struct redisObjectExtended {
    uint64_t mvcc_tstamp;
};

typedef struct redisObject {
    friend redisObject *createEmbeddedStringObject(const char *ptr, size_t len);
protected:
    redisObject() {}

public:
    unsigned type:4;
    unsigned encoding:4;
    unsigned lru:LRU_BITS; /* LRU time (relative to global lru_clock) or
                            * LFU data (least significant 8 bits frequency
                            * and most significant 16 bits access time). */
private:
    mutable std::atomic<unsigned> refcount {0};
public:
    void *m_ptr;

    inline bool FExpires() const { return refcount.load(std::memory_order_relaxed) >> 31; }
    void SetFExpires(bool fExpires);

    void setrefcount(unsigned ref);
    unsigned getrefcount(std::memory_order order = std::memory_order_relaxed) const { return (refcount.load(order) & ~(1U << 31)); }
    void addref() const { refcount.fetch_add(1, std::memory_order_relaxed); }
    unsigned release() const { return refcount.fetch_sub(1, std::memory_order_seq_cst) & ~(1U << 31); }
} robj;
static_assert(sizeof(redisObject) <= 16, "object size is critical, don't increase");

class redisObjectStack : public redisObjectExtended, public redisObject
{
public:
    redisObjectStack();
};

uint64_t mvccFromObj(robj_roptr o);
void setMvccTstamp(redisObject *o, uint64_t mvcc);
void *allocPtrFromObj(robj_roptr o);
robj *objFromAllocPtr(void *pv);

__attribute__((always_inline)) inline const void *ptrFromObj(robj_roptr &o)
{
    if (o->encoding == OBJ_ENCODING_EMBSTR)
        return ((char*)&(o)->m_ptr) + sizeof(struct sdshdr8);
    return o->m_ptr;
}

__attribute__((always_inline)) inline void *ptrFromObj(const robj *o)
{
    if (o->encoding == OBJ_ENCODING_EMBSTR)
        return ((char*)&((robj*)o)->m_ptr) + sizeof(struct sdshdr8);
    return o->m_ptr;
}

__attribute__((always_inline)) inline const char *szFromObj(robj_roptr o)
{
    return (const char*)ptrFromObj(o);
}

__attribute__((always_inline)) inline char *szFromObj(const robj *o)
{
    return (char*)ptrFromObj(o);
}

/* The a string name for an object's type as listed above
 * Native types are checked against the OBJ_STRING, OBJ_LIST, OBJ_* defines,
 * and Module types have their registered name returned. */
const char *getObjectTypeName(robj_roptr o);

/* Macro used to initialize a Redis object allocated on the stack.
 * Note that this macro is taken near the structure definition to make sure
 * we'll update it when the structure is changed, to avoid bugs like
 * bug #85 introduced exactly in this way. */
#define initStaticStringObject(_var,_ptr) do { \
    _var.setrefcount(OBJ_STATIC_REFCOUNT); \
    _var.type = OBJ_STRING; \
    _var.encoding = OBJ_ENCODING_RAW; \
    _var.m_ptr = _ptr; \
} while(0)

struct evictionPoolEntry; /* Defined in evict.c */

/* This structure is used in order to represent the output buffer of a client,
 * which is actually a linked list of blocks like that, that is: client->reply. */
typedef struct clientReplyBlock {
    size_t size, used;
#ifndef __cplusplus
    char buf[];
#else
    __attribute__((always_inline)) char *buf()
    {
        return reinterpret_cast<char*>(this+1);
    }
#endif
} clientReplyBlock;

struct dictEntry;
class dict_const_iter
{
    friend struct redisDb;
    friend class redisDbPersistentData;
protected:
    dictEntry *de;
public:
    explicit dict_const_iter(dictEntry *de)
        : de(de)
    {}

    const char *key() const { return de ? (const char*)dictGetKey(de) : nullptr; }
    robj_roptr val() const { return de ? (robj*)dictGetVal(de) : nullptr; }
    const robj* operator->() const { return de ? (robj*)dictGetVal(de) : nullptr; }
    operator robj_roptr() const { return de ? (robj*)dictGetVal(de) : nullptr; }

    bool operator==(std::nullptr_t) const { return de == nullptr; }
    bool operator!=(std::nullptr_t) const { return de != nullptr; }
    bool operator==(const dict_const_iter &other) { return de == other.de; }
};
class dict_iter : public dict_const_iter
{
    dict *m_dict = nullptr;
public:
    dict_iter()
        : dict_const_iter(nullptr)
    {}
    explicit dict_iter(std::nullptr_t)
        : dict_const_iter(nullptr)
    {}
    explicit dict_iter(dict *d, dictEntry *de)
        : dict_const_iter(de), m_dict(d)
    {}
    sds key() { return de ? (sds)dictGetKey(de) : nullptr; }
    robj *val() { return de ? (robj*)dictGetVal(de) : nullptr; }
    robj *operator->() { return de ? (robj*)dictGetVal(de) : nullptr; }
    operator robj*() const { return de ? (robj*)dictGetVal(de) : nullptr; }

    void setval(robj *val) {
        dictSetVal(m_dict, de, val);
    }
};

class redisDbPersistentDataSnapshot;
class redisDbPersistentData
{
    friend void dictDbKeyDestructor(void *privdata, void *key);
    friend class redisDbPersistentDataSnapshot;

public:
    redisDbPersistentData();
    virtual ~redisDbPersistentData();

    redisDbPersistentData(const redisDbPersistentData &) = delete;
    redisDbPersistentData(redisDbPersistentData &&) = delete;

    size_t slots() const { return dictSlots(m_pdict); }
    size_t size(bool fCachedOnly = false) const;
    void expand(uint64_t slots) {
        if (m_spstorage)
            m_spstorage->expand(slots);
        else
            dictExpand(m_pdict, slots); 
    }
    
    void trackkey(robj_roptr o, bool fUpdate)
    {
        trackkey(szFromObj(o), fUpdate);
    }

    void trackkey(const char *key, bool fUpdate);

    dict_iter find(const char *key) 
    {
        dictEntry *de = dictFind(m_pdict, key);
        ensure(key, &de);
        return dict_iter(m_pdict, de);
    }

    dict_iter find(robj_roptr key)
    {
        return find(szFromObj(key));
    }

    dict_iter random();

    const expireEntry &random_expire()
    {
        return m_setexpire->random_value();
    }

    dict_iter end()  { return dict_iter(nullptr, nullptr); }
    dict_const_iter end() const { return dict_const_iter(nullptr); }

    void getStats(char *buf, size_t bufsize) { dictGetStats(buf, bufsize, m_pdict); }
    void getExpireStats(char *buf, size_t bufsize) { m_setexpire->getstats(buf, bufsize); }

    bool insert(char *k, robj *o, bool fAssumeNew = false, dict_iter *existing = nullptr);
    void tryResize();
    int incrementallyRehash();
    void updateValue(dict_iter itr, robj *val);
    bool syncDelete(robj *key);
    bool asyncDelete(robj *key);
    size_t expireSize() const { return m_setexpire->size(); }
    int removeExpire(robj *key, dict_iter itr);
    int removeSubkeyExpire(robj *key, robj *subkey);
    void resortExpire(expireEntry &e);
    void clear(void(callback)(void*));
    void emptyDbAsync();
    // Note: If you do not need the obj then use the objless iterator version.  It's faster
    bool iterate(std::function<bool(const char*, robj*)> fn);
    void setExpire(robj *key, robj *subkey, long long when);
    void setExpire(expireEntry &&e);
    void initialize();
    void prepOverwriteForSnapshot(char *key);

    bool FRehashing() const { return dictIsRehashing(m_pdict) || dictIsRehashing(m_pdictTombstone); }

    void setStorageProvider(StorageCache *pstorage);
    void endStorageProvider();

    void trackChanges(bool fBulk, size_t sizeHint = 0);
    bool FTrackingChanges() const { return !!m_fTrackingChanges; }

    // Process and commit changes for secondary storage.  Note that process and commit are seperated
    //  to allow you to release the global lock before commiting.  To prevent deadlocks you *must*
    //  either release the global lock or keep the same global lock between the two functions as
    //  a second look is kept to ensure writes to secondary storage are ordered
    bool processChanges(bool fSnapshot);
    void processChangesAsync(std::atomic<int> &pendingJobs);
    void commitChanges(const redisDbPersistentDataSnapshot **psnapshotFree = nullptr);

    // This should only be used if you look at the key, we do not fixup
    //  objects stored elsewhere
    dict *dictUnsafeKeyOnly() { return m_pdict; }   

    expireset *setexpireUnsafe() { return m_setexpire; }
    const expireset *setexpire() const { return m_setexpire; }

    const redisDbPersistentDataSnapshot *createSnapshot(uint64_t mvccCheckpoint, bool fOptional);
    void endSnapshot(const redisDbPersistentDataSnapshot *psnapshot);
    void endSnapshotAsync(const redisDbPersistentDataSnapshot *psnapshot);
    void restoreSnapshot(const redisDbPersistentDataSnapshot *psnapshot);

    bool FStorageProvider() { return m_spstorage != nullptr; }
    bool removeCachedValue(const char *key, dictEntry **ppde = nullptr);
    void removeAllCachedValues();
    void disableKeyCache();
    bool keycacheIsEnabled();

    void prefetchKeysAsync(client *c, struct parsed_command &command);
    void prefetchKeysFlash(std::unordered_set<client*> &setc);
    void processStorageToken(StorageToken *tok);

    bool FSnapshot() const { return m_spdbSnapshotHOLDER != nullptr; }

    std::unique_ptr<const StorageCache> CloneStorageCache() { return std::unique_ptr<const StorageCache>(m_spstorage->clone()); }
    std::shared_ptr<StorageCache> getStorageCache() { return m_spstorage; }
    void bulkDirectStorageInsert(char **rgKeys, size_t *rgcbKeys, char **rgVals, size_t *rgcbVals, size_t celem);

    dict_iter find_cached_threadsafe(const char *key) const;

    static void storageLoadCallback(struct aeEventLoop *el, struct StorageToken *token);

protected:
    uint64_t m_mvccCheckpoint = 0;

private:
    static void serializeAndStoreChange(StorageCache *storage, redisDbPersistentData *db, const char *key, bool fUpdate);

    void ensure(const char *key);
    void ensure(const char *key, dictEntry **de);
    void storeDatabase();
    void storeKey(sds key, robj *o, bool fOverwrite);
    void recursiveFreeSnapshots(redisDbPersistentDataSnapshot *psnapshot);

    // Keyspace
    dict *m_pdict = nullptr;                 /* The keyspace for this DB */
    dict *m_pdictTombstone = nullptr;        /* Track deletes when we have a snapshot */
    std::atomic<int> m_fTrackingChanges {0};     // Note: Stack based
    std::atomic<int> m_fAllChanged {0};
    dict *m_dictChanged = nullptr;
    size_t m_cnewKeysPending = 0;
    std::shared_ptr<StorageCache> m_spstorage = nullptr;

    // Expire
    expireset *m_setexpire = nullptr;

    // These two pointers are the same, UNLESS the database has been cleared.
    //      in which case m_pdbSnapshot is NULL and we continue as though we weren'
    //      in a snapshot
    const redisDbPersistentDataSnapshot *m_pdbSnapshot = nullptr;
    std::unique_ptr<redisDbPersistentDataSnapshot> m_spdbSnapshotHOLDER;
    const redisDbPersistentDataSnapshot *m_pdbSnapshotASYNC = nullptr;
    
    const redisDbPersistentDataSnapshot *m_pdbSnapshotStorageFlush = nullptr;
    dict *m_dictChangedStorageFlush = nullptr;
    
    int m_refCount = 0;
};

class redisDbPersistentDataSnapshot : protected redisDbPersistentData
{
    friend class redisDbPersistentData;
private:
    bool iterate_threadsafe_core(std::function<bool(const char*, robj_roptr o)> &fn, bool fKeyOnly, bool fCacheOnly, bool fTop) const;

protected:
    static void gcDisposeSnapshot(redisDbPersistentDataSnapshot *psnapshot);
    bool freeTombstoneObjects(int depth);

public:
    int snapshot_depth() const;
    bool FWillFreeChildDebug() const { return m_spdbSnapshotHOLDER != nullptr; }

    bool iterate_threadsafe(std::function<bool(const char*, robj_roptr o)> fn, bool fKeyOnly = false, bool fCacheOnly = false) const;
    unsigned long scan_threadsafe(unsigned long iterator, long count, sds type, list *keys) const;
    
    using redisDbPersistentData::createSnapshot;
    using redisDbPersistentData::endSnapshot;
    using redisDbPersistentData::endSnapshotAsync;
    using redisDbPersistentData::end;
    using redisDbPersistentData::find_cached_threadsafe;
    using redisDbPersistentData::FSnapshot;

    dict_iter random_cache_threadsafe(bool fPrimaryOnly = false) const;

    expireEntry *getExpire(robj_roptr key) { return getExpire(szFromObj(key)); }
    expireEntry *getExpire(const char *key);
    const expireEntry *getExpire(const char *key) const;
    const expireEntry *getExpire(robj_roptr key) const { return getExpire(szFromObj(key)); }

    uint64_t mvccCheckpoint() const { return m_mvccCheckpoint; }

    bool FStale() const;

    // These need to be fixed
    using redisDbPersistentData::size;
    using redisDbPersistentData::expireSize;
};

/* Redis database representation. There are multiple databases identified
 * by integers from 0 (the default database) up to the max configured
 * database. The database number is the 'id' field in the structure. */
struct redisDb : public redisDbPersistentDataSnapshot 
{
    // Legacy C API, Do not add more
    friend void tryResizeHashTables(int);
    friend int dbSyncDelete(redisDb *db, robj *key);
    friend int dbAsyncDelete(redisDb *db, robj *key);
    friend long long emptyDb(int dbnum, int flags, void(callback)(void*));
    friend void scanGenericCommand(struct client *c, robj_roptr o, unsigned long cursor);
    friend int dbSwapDatabases(int id1, int id2);
    friend int removeExpire(redisDb *db, robj *key);
    friend void setExpire(struct client *c, redisDb *db, robj *key, robj *subkey, long long when);
    friend void setExpire(client *c, redisDb *db, robj *key, expireEntry &&e);
    friend int evictionPoolPopulate(int dbid, redisDb *db, expireset *setexpire, struct evictionPoolEntry *pool);
    friend void activeDefragCycle(void);
    friend void activeExpireCycle(int);
    friend void expireSlaveKeys(void);
    friend int performEvictions(bool fPreSnapshot);

    typedef ::dict_const_iter const_iter;
    typedef ::dict_iter iter;

    redisDb()
        : expireitr(nullptr)
    {}

    void initialize(int id);
    void storageProviderInitialize();
    void storageProviderDelete();
    virtual ~redisDb();

    void dbOverwriteCore(redisDb::iter itr, sds keySds, robj *val, bool fUpdateMvcc, bool fRemoveExpire);

    bool FKeyExpires(const char *key);
    size_t clear(bool fAsync, void(callback)(void*));

    // Import methods from redisDbPersistentData hidden by redisDbPersistentDataSnapshot
    using redisDbPersistentData::slots;
    using redisDbPersistentData::size;
    using redisDbPersistentData::expand;
    using redisDbPersistentData::trackkey;
    using redisDbPersistentData::find;
    using redisDbPersistentData::random;
    using redisDbPersistentData::random_expire;
    using redisDbPersistentData::end;
    using redisDbPersistentData::getStats;
    using redisDbPersistentData::getExpireStats;
    using redisDbPersistentData::insert;
    using redisDbPersistentData::tryResize;
    using redisDbPersistentData::incrementallyRehash;
    using redisDbPersistentData::updateValue;
    using redisDbPersistentData::syncDelete;
    using redisDbPersistentData::asyncDelete;
    using redisDbPersistentData::expireSize;
    using redisDbPersistentData::removeExpire;
    using redisDbPersistentData::removeSubkeyExpire;
    using redisDbPersistentData::clear;
    using redisDbPersistentData::emptyDbAsync;
    using redisDbPersistentData::iterate;
    using redisDbPersistentData::setExpire;
    using redisDbPersistentData::trackChanges;
    using redisDbPersistentData::processChanges;
    using redisDbPersistentData::processChangesAsync;
    using redisDbPersistentData::commitChanges;
    using redisDbPersistentData::setexpireUnsafe;
    using redisDbPersistentData::setexpire;
    using redisDbPersistentData::endSnapshot;
    using redisDbPersistentData::restoreSnapshot;
    using redisDbPersistentData::removeAllCachedValues;
    using redisDbPersistentData::disableKeyCache;
    using redisDbPersistentData::keycacheIsEnabled;
    using redisDbPersistentData::dictUnsafeKeyOnly;
    using redisDbPersistentData::resortExpire;
    using redisDbPersistentData::prefetchKeysAsync;
    using redisDbPersistentData::prefetchKeysFlash;
    using redisDbPersistentData::processStorageToken;
    using redisDbPersistentData::prepOverwriteForSnapshot;
    using redisDbPersistentData::FRehashing;
    using redisDbPersistentData::FTrackingChanges;
    using redisDbPersistentData::CloneStorageCache;
    using redisDbPersistentData::getStorageCache;
    using redisDbPersistentData::bulkDirectStorageInsert;

public:
    const redisDbPersistentDataSnapshot *createSnapshot(uint64_t mvccCheckpoint, bool fOptional) {
        auto psnapshot = redisDbPersistentData::createSnapshot(mvccCheckpoint, fOptional);
        if (psnapshot != nullptr)
            mvccLastSnapshot = psnapshot->mvccCheckpoint();
        return psnapshot;
    }

    expireset::setiter expireitr;
    dict *blocking_keys;        /* Keys with clients waiting for data (BLPOP)*/
    dict *ready_keys;           /* Blocked keys that received a PUSH */
    dict *watched_keys;         /* WATCHED keys for MULTI/EXEC CAS */
    int id;                     /* Database ID */
    long long last_expire_set;  /* when the last expire was set */
    double avg_ttl;             /* Average TTL, just for stats */
    list *defrag_later;         /* List of key names to attempt to defrag one by one, gradually. */
    uint64_t mvccLastSnapshot = 0;
};

/* Declare database backup that include redis main DBs and slots to keys map.
 * Definition is in db.c. We can't define it here since we define CLUSTER_SLOTS
 * in cluster.h. */
typedef struct dbBackup dbBackup;

/* Declare database backup that include redis main DBs and slots to keys map.
 * Definition is in db.c. We can't define it here since we define CLUSTER_SLOTS
 * in cluster.h. */
typedef struct dbBackup dbBackup;

/* Client MULTI/EXEC state */
typedef struct multiCmd {
    robj **argv;
    int argc;
    struct redisCommand *cmd;
} multiCmd;

typedef struct multiState {
    multiCmd *commands;     /* Array of MULTI commands */
    int count;              /* Total number of MULTI commands */
    int cmd_flags;          /* The accumulated command flags OR-ed together.
                               So if at least a command has a given flag, it
                               will be set in this field. */
    int cmd_inv_flags;      /* Same as cmd_flags, OR-ing the ~flags. so that it
                               is possible to know if all the commands have a
                               certain flag. */
} multiState;

struct listPos {
    int wherefrom;      /* Where to pop from */
    int whereto;        /* Where to push to */
};                      /* The positions in the src/dst lists
                            * where we want to pop/push an element
                            * for BLPOP, BRPOP and BLMOVE. */

/* This structure holds the blocking operation state for a client.
 * The fields used depend on client->btype. */
typedef struct blockingState {
    /* Generic fields. */
    mstime_t timeout;       /* Blocking operation timeout. If UNIX current time
                             * is > timeout then the operation timed out. */

    /* BLOCKED_LIST, BLOCKED_ZSET and BLOCKED_STREAM */
    ::dict *keys;             /* The keys we are waiting to terminate a blocking
                             * operation such as BLPOP or XREAD. Or NULL. */
    robj *target;           /* The key that should receive the element,
                             * for BLMOVE. */

    listPos listpos;

    /* BLOCK_STREAM */
    size_t xread_count;     /* XREAD COUNT option. */
    robj *xread_group;      /* XREADGROUP group name. */
    robj *xread_consumer;   /* XREADGROUP consumer name. */
    int xread_group_noack;

    /* BLOCKED_WAIT */
    int numreplicas;        /* Number of replicas we are waiting for ACK. */
    long long reploffset;   /* Replication offset to reach. */

    /* BLOCKED_MODULE */
    void *module_blocked_handle; /* RedisModuleBlockedClient structure.
                                    which is opaque for the Redis core, only
                                    handled in module.c. */
} blockingState;

/* The following structure represents a node in the g_pserver->ready_keys list,
 * where we accumulate all the keys that had clients blocked with a blocking
 * operation such as B[LR]POP, but received new data in the context of the
 * last executed command.
 *
 * After the execution of every command or script, we run this list to check
 * if as a result we should serve data to clients blocked, unblocking them.
 * Note that g_pserver->ready_keys will not have duplicates as there dictionary
 * also called ready_keys in every structure representing a Redis database,
 * where we make sure to remember if a given key was already added in the
 * g_pserver->ready_keys list. */
typedef struct readyList {
    redisDb *db;
    robj *key;
} readyList;

/* This structure represents a Redis user. This is useful for ACLs, the
 * user is associated to the connection after the connection is authenticated.
 * If there is no associated user, the connection uses the default user. */
#define USER_COMMAND_BITS_COUNT 1024    /* The total number of command bits
                                           in the user structure. The last valid
                                           command ID we can set in the user
                                           is USER_COMMAND_BITS_COUNT-1. */
#define USER_FLAG_ENABLED (1<<0)        /* The user is active. */
#define USER_FLAG_DISABLED (1<<1)       /* The user is disabled. */
#define USER_FLAG_ALLKEYS (1<<2)        /* The user can mention any key. */
#define USER_FLAG_ALLCOMMANDS (1<<3)    /* The user can run all commands. */
#define USER_FLAG_NOPASS      (1<<4)    /* The user requires no password, any
                                           provided password will work. For the
                                           default user, this also means that
                                           no AUTH is needed, and every
                                           connection is immediately
                                           authenticated. */
#define USER_FLAG_ALLCHANNELS (1<<5)    /* The user can mention any Pub/Sub
                                           channel. */
#define USER_FLAG_SANITIZE_PAYLOAD (1<<6)       /* The user require a deep RESTORE
                                                 * payload sanitization. */
#define USER_FLAG_SANITIZE_PAYLOAD_SKIP (1<<7)  /* The user should skip the
                                                 * deep sanitization of RESTORE
                                                 * payload. */

typedef struct {
    sds name;       /* The username as an SDS string. */
    uint64_t flags; /* See USER_FLAG_* */

    /* The bit in allowed_commands is set if this user has the right to
     * execute this command. In commands having subcommands, if this bit is
     * set, then all the subcommands are also available.
     *
     * If the bit for a given command is NOT set and the command has
     * subcommands, Redis will also check allowed_subcommands in order to
     * understand if the command can be executed. */
    uint64_t allowed_commands[USER_COMMAND_BITS_COUNT/64];

    /* This array points, for each command ID (corresponding to the command
     * bit set in allowed_commands), to an array of SDS strings, terminated by
     * a NULL pointer, with all the sub commands that can be executed for
     * this command. When no subcommands matching is used, the field is just
     * set to NULL to avoid allocating USER_COMMAND_BITS_COUNT pointers. */
    sds **allowed_subcommands;
    list *passwords; /* A list of SDS valid passwords for this user. */
    list *patterns;  /* A list of allowed key patterns. If this field is NULL
                        the user cannot mention any key in a command, unless
                        the flag ALLKEYS is set in the user. */
    list *channels;  /* A list of allowed Pub/Sub channel patterns. If this
                        field is NULL the user cannot mention any channel in a
                        `PUBLISH` or [P][UNSUBSCRIBE] command, unless the flag
                        ALLCHANNELS is set in the user. */
} user;

/* With multiplexing we need to take per-client state.
 * Clients are taken in a linked list. */

#define CLIENT_ID_AOF (UINT64_MAX) /* Reserved ID for the AOF client. If you
                                      need more reserved IDs use UINT64_MAX-1,
                                      -2, ... and so forth. */

struct parsed_command {
    robj** argv = nullptr;
    int argc = 0;
    int argcMax;
    long long reploff = 0;
    size_t argv_len_sum = 0;    /* Sum of lengths of objects in argv list. */

    parsed_command(int maxargs) {
        argv = (robj**)zmalloc(sizeof(robj*)*maxargs);
        argcMax = maxargs;
    }

    parsed_command &operator=(parsed_command &&o) {
        argv = o.argv;
        argc = o.argc;
        argcMax = o.argcMax;
        reploff = o.reploff;
        o.argv = nullptr;
        o.argc = 0;
        o.argcMax = 0;
        o.reploff = 0;
        return *this;
    }

    parsed_command(parsed_command &o) = delete;
    parsed_command(parsed_command &&o) {
        argv = o.argv;
        argc = o.argc;
        argcMax = o.argcMax;
        reploff = o.reploff;
        o.argv = nullptr;
        o.argc = 0;
        o.argcMax = 0;
        o.reploff = 0;
    }

    ~parsed_command() {
        if (argv != nullptr) {
            for (int i = 0; i < argc; ++i) {
                decrRefCount(argv[i]);
            }
            zfree(argv);
        }
    }
};

struct client {
    uint64_t id;            /* Client incremental unique ID. */
    connection *conn;
    int resp;               /* RESP protocol version. Can be 2 or 3. */
    redisDb *db;            /* Pointer to currently SELECTed DB. */
    robj *name;             /* As set by CLIENT SETNAME. */
    sds querybuf;           /* Buffer we use to accumulate client queries. */
    size_t qb_pos;          /* The position we have read in querybuf. */
    sds pending_querybuf;   /* If this client is flagged as master, this buffer
                               represents the yet not applied portion of the
                               replication stream that we are receiving from
                               the master. */
    size_t querybuf_peak;   /* Recent (100ms or more) peak of querybuf size. */
    int original_argc;      /* Num of arguments of original command if arguments were rewritten. */
    robj **original_argv;   /* Arguments of original command if arguments were rewritten. */
    struct redisCommand *cmd, *lastcmd;  /* Last command executed. */
    ::user *user;             /* User associated with this connection. If the
                               user is set to NULL the connection can do
                               anything (admin). */
    int reqtype;            /* Request protocol type: PROTO_REQ_* */
    int multibulklen;       /* Number of multi bulk arguments left to read. */
    long bulklen;           /* Length of bulk argument in multi bulk request. */
    list *reply;            /* List of reply objects to send to the client. */
    unsigned long long reply_bytes; /* Tot bytes of objects in reply list. */
    size_t sentlen;         /* Amount of bytes already sent in the current
                               buffer or object being sent. */
    size_t sentlenAsync;    /* same as sentlen buf for async buffers (which are a different stream) */
    time_t ctime;           /* Client creation time. */
    long duration;          /* Current command duration. Used for measuring latency of blocking/non-blocking cmds */
    time_t lastinteraction; /* Time of the last interaction, used for timeout */
    time_t obuf_soft_limit_reached_time;
    std::atomic<uint64_t> flags;              /* Client flags: CLIENT_* macros. */
    int casyncOpsPending;
    int fPendingAsyncWrite; /* NOTE: Not a flag because it is written to outside of the client lock (locked by the global lock instead) */
    std::atomic<bool> fPendingAsyncWriteHandler;
    int authenticated;      /* Needed when the default user requires auth. */
    int replstate;          /* Replication state if this is a replica. */
    int repl_put_online_on_ack; /* Install replica write handler on ACK. */
    int repldbfd;           /* Replication DB file descriptor. */
    off_t repldboff;        /* Replication DB file offset. */
    off_t repldbsize;       /* Replication DB file size. */
    sds replpreamble;       /* Replication DB preamble. */
    time_t repl_down_since; /* When client lost connection. */
    long long read_reploff; /* Read replication offset if this is a master. */
    long long reploff;      /* Applied replication offset if this is a master. */
    long long reploff_cmd;  /* The replication offset of the executing command, reploff gets set to this after the execution completes */
    long long repl_ack_off; /* Replication ack offset, if this is a replica. */
    long long repl_ack_time;/* Replication ack time, if this is a replica. */
    long long repl_last_partial_write; /* The last time the server did a partial write from the RDB child pipe to this replica  */
    long long psync_initial_offset; /* FULLRESYNC reply offset other slaves
                                       copying this replica output buffer
                                       should use. */
                                       
    long long repl_curr_off = -1;/* Replication offset of the replica, also where in the backlog we need to start from
                                  * when sending data to this replica. */
    long long repl_end_off = -1; /* Replication offset to write to, stored in the replica, as opposed to using the global offset 
                                  * to prevent needing the global lock */

    char replid[CONFIG_RUN_ID_SIZE+1]; /* Master replication ID (if master). */
    int slave_listening_port; /* As configured with: REPLCONF listening-port */
    char *slave_addr;       /* Optionally given by REPLCONF ip-address */
    int slave_capa;         /* Slave capabilities: SLAVE_CAPA_* bitwise OR. */
    multiState mstate;      /* MULTI/EXEC state */
    int btype;              /* Type of blocking op if CLIENT_BLOCKED. */
    blockingState bpop;     /* blocking state */
    long long woff;         /* Last write global replication offset. */
    list *watched_keys;     /* Keys WATCHED for MULTI/EXEC CAS */
    ::dict *pubsub_channels;  /* channels a client is interested in (SUBSCRIBE) */
    list *pubsub_patterns;  /* patterns a client is interested in (SUBSCRIBE) */
    sds peerid;             /* Cached peer ID. */
    sds sockname;           /* Cached connection target address. */
    listNode *client_list_node; /* list node in client list */
    listNode *paused_list_node; /* list node within the pause list */
    RedisModuleUserChangedFunc auth_callback; /* Module callback to execute
                                               * when the authenticated user
                                               * changes. */
    void *auth_callback_privdata; /* Private data that is passed when the auth
                                   * changed callback is executed. Opaque for
                                   * Redis Core. */
    void *auth_module;      /* The module that owns the callback, which is used
                             * to disconnect the client if the module is
                             * unloaded for cleanup. Opaque for Redis Core.*/

    /* UUID announced by the client (default nil) - used to detect multiple connections to/from the same peer */
    /* compliant servers will announce their UUIDs when a replica connection is started, and return when asked */
    /* UUIDs are transient and lost when the server is shut down */
    unsigned char uuid[UUID_BINARY_LEN];

    /* If this client is in tracking mode and this field is non zero,
     * invalidation messages for keys fetched by this client will be send to
     * the specified client ID. */
    uint64_t client_tracking_redirection;
    rax *client_tracking_prefixes; /* A dictionary of prefixes we are already
                                      subscribed to in BCAST mode, in the
                                      context of client side caching. */
    /* In clientsCronTrackClientsMemUsage() we track the memory usage of
     * each client and add it to the sum of all the clients of a given type,
     * however we need to remember what was the old contribution of each
     * client, and in which categoty the client was, in order to remove it
     * before adding it the new value. */
    uint64_t client_cron_last_memory_usage;
    int      client_cron_last_memory_type;
    /* Response buffer */
    int bufpos;
    char buf[PROTO_REPLY_CHUNK_BYTES];

    /* Async Response Buffer - other threads write here */
    clientReplyBlock *replyAsync;

    uint64_t mvccCheckpoint = 0;    // the MVCC checkpoint of our last write

    int iel; /* the event loop index we're registered with */
    struct fastlock lock {"client"};
    int master_error;
    std::vector<parsed_command> vecqueuedcmd;
    int argc;
    robj **argv;
    size_t argv_len_sumActive = 0;

    bool FPendingReplicaWrite() const {
        return repl_curr_off != repl_end_off && replstate == SLAVE_STATE_ONLINE;
    }

    // post a function from a non-client thread to run on its client thread
    bool postFunction(std::function<void(client *)> fn, bool fLock = true);
    size_t argv_len_sum() const;
    bool asyncCommand(std::function<void(const redisDbPersistentDataSnapshot *, const std::vector<robj_sharedptr> &)> &&mainFn, 
                        std::function<void(const redisDbPersistentDataSnapshot *)> &&postFn = nullptr);
    char* fprint;
};

struct saveparam {
    time_t seconds;
    int changes;
};

struct moduleLoadQueueEntry {
    sds path;
    int argc;
    robj **argv;
};

struct sentinelLoadQueueEntry {
    int argc;
    sds *argv;
    int linenum;
    sds line;
};

struct sentinelConfig {
    list *pre_monitor_cfg;
    list *monitor_cfg;
    list *post_monitor_cfg;
};

struct sharedObjectsStruct {
    robj *crlf, *ok, *err, *emptybulk, *czero, *cone, *pong, *space,
    *colon, *queued, *nullbulk, *null[4], *nullarray[4], *emptymap[4], *emptyset[4],
    *emptyarray, *wrongtypeerr, *nokeyerr, *syntaxerr, *sameobjecterr,
    *outofrangeerr, *noscripterr, *loadingerr, *slowscripterr, *bgsaveerr,
    *masterdownerr, *roslaveerr, *execaborterr, *noautherr, *noreplicaserr,
    *busykeyerr, *oomerr, *plus, *messagebulk, *pmessagebulk, *subscribebulk,
    *unsubscribebulk, *psubscribebulk, *punsubscribebulk, *del, *unlink,
    *rpop, *lpop, *lpush, *rpoplpush, *lmove, *blmove, *zpopmin, *zpopmax,
    *emptyscan, *multi, *exec, *left, *right, *hset, *srem, *xgroup, *xclaim,  
    *script, *replconf, *eval, *persist, *set, *pexpireat, *pexpire, 
    *time, *pxat, *px, *retrycount, *force, *justid, 
    *lastid, *ping, *replping, *setid, *keepttl, *load, *createconsumer,
    *getack, *special_asterick, *special_equals, *default_username,
    *hdel, *zrem, *mvccrestore, *pexpirememberat, *redacted,
    *select[PROTO_SHARED_SELECT_CMDS],
    *integers[OBJ_SHARED_INTEGERS],
    *mbulkhdr[OBJ_SHARED_BULKHDR_LEN], /* "*<value>\r\n" */
    *bulkhdr[OBJ_SHARED_BULKHDR_LEN];  /* "$<value>\r\n" */
    sds minstring, maxstring;
};

/* ZSETs use a specialized version of Skiplists */
struct zskiplistLevel {
        struct zskiplistNode *forward;
        unsigned long span;
};
typedef struct zskiplistNode {
    sds ele;
    double score;
    struct zskiplistNode *backward;
    
#ifdef __cplusplus
    zskiplistLevel *level(size_t idx) {
        return reinterpret_cast<zskiplistLevel*>(this+1) + idx;
    }
#else
    struct zskiplistLevel level[];
#endif
} zskiplistNode;

typedef struct zskiplist {
    struct zskiplistNode *header, *tail;
    unsigned long length;
    int level;
} zskiplist;

typedef struct zset {
    ::dict *dict;
    zskiplist *zsl;
} zset;

typedef struct clientBufferLimitsConfig {
    unsigned long long hard_limit_bytes;
    unsigned long long soft_limit_bytes;
    time_t soft_limit_seconds;
} clientBufferLimitsConfig;

extern clientBufferLimitsConfig clientBufferLimitsDefaults[CLIENT_TYPE_OBUF_COUNT];

/* The redisOp structure defines a Redis Operation, that is an instance of
 * a command with an argument vector, database ID, propagation target
 * (PROPAGATE_*), and command pointer.
 *
 * Currently only used to additionally propagate more commands to AOF/Replication
 * after the propagation of the executed command. */
typedef struct redisOp {
    robj **argv;
    int argc, dbid, target;
    struct redisCommand *cmd;
} redisOp;

/* Defines an array of Redis operations. There is an API to add to this
 * structure in an easy way.
 *
 * redisOpArrayInit();
 * redisOpArrayAppend();
 * redisOpArrayFree();
 */
typedef struct redisOpArray {
    redisOp *ops;
    int numops;
} redisOpArray;

/* This structure is returned by the getMemoryOverheadData() function in
 * order to return memory overhead information. */
struct redisMemOverhead {
    size_t peak_allocated;
    size_t total_allocated;
    size_t startup_allocated;
    size_t repl_backlog;
    size_t clients_slaves;
    size_t clients_normal;
    size_t aof_buffer;
    size_t lua_caches;
    size_t overhead_total;
    size_t dataset;
    size_t total_keys;
    size_t bytes_per_key;
    float dataset_perc;
    float peak_perc;
    float total_frag;
    ssize_t total_frag_bytes;
    float allocator_frag;
    ssize_t allocator_frag_bytes;
    float allocator_rss;
    ssize_t allocator_rss_bytes;
    float rss_extra;
    size_t rss_extra_bytes;
    size_t num_dbs;
    struct {
        size_t dbid;
        size_t overhead_ht_main;
        size_t overhead_ht_expires;
    } *db;
};


struct redisMaster {
    char *masteruser;               /* AUTH with this user and masterauth with master */
    char *masterauth;               /* AUTH with this password with master */
    char *masterhost;               /* Hostname of master */
    int masterport;                 /* Port of master */
    client *cached_master;          /* Cached master to be reused for PSYNC. */
    client *master;
    /* The following two fields is where we store master PSYNC replid/offset
     * while the PSYNC is in progress. At the end we'll copy the fields into
     * the server->master client structure. */
    char master_replid[CONFIG_RUN_ID_SIZE+1];  /* Master PSYNC runid. */
    long long master_initial_offset;           /* Master PSYNC offset. */

    bool isActive = false;
    bool isRocksdbSnapshotRepl = false;
    int repl_state;          /* Replication status if the instance is a replica */
    off_t repl_transfer_size; /* Size of RDB to read from master during sync. */
    off_t repl_transfer_read; /* Amount of RDB read from master during sync. */
    off_t repl_transfer_last_fsync_off; /* Offset when we fsync-ed last time. */
    connection *repl_transfer_s;     /* Replica -> Master SYNC socket */
    int repl_transfer_fd;    /* Replica -> Master SYNC temp file descriptor */
    char *repl_transfer_tmpfile; /* Replica-> master SYNC temp file name */
    time_t repl_transfer_lastio; /* Unix time of the latest read, for timeout */
    time_t repl_down_since; /* Unix time at which link with master went down */

    class SnapshotPayloadParseState *parseState;
    sds bulkreadBuffer = nullptr;

    unsigned char master_uuid[UUID_BINARY_LEN];  /* Used during sync with master, this is our master's UUID */
                                                /* After we've connected with our master use the UUID in g_pserver->master */
    uint64_t mvccLastSync;
    /* During a handshake the server may have stale keys, we track these here to share once a reciprocal connection is made */
    std::map<int, std::vector<robj_sharedptr>> *staleKeyMap;
    int ielReplTransfer = -1;
};

struct MasterSaveInfo {
    MasterSaveInfo() {
        memcpy(master_replid, "0000000000000000000000000000000000000000", sizeof(master_replid));
    }
    MasterSaveInfo(const redisMaster &mi) {
        memcpy(master_replid, mi.master_replid, sizeof(mi.master_replid));
        if (mi.master) {
            master_initial_offset = mi.master->reploff;
            selected_db = mi.master->db->id;
        } else if (mi.cached_master) {
            master_initial_offset = mi.cached_master->reploff;
            selected_db = mi.cached_master->db->id;
        } else {
            master_initial_offset = -1;
            selected_db = 0;
        }
        masterport = mi.masterport;
        if (mi.masterhost)
            masterhost = sdsstring(sdsdup(mi.masterhost));
        masterport = mi.masterport;
    }
    MasterSaveInfo(const MasterSaveInfo &other) {
        masterhost = other.masterhost;
        masterport = other.masterport;
        memcpy(master_replid, other.master_replid, sizeof(master_replid));
        master_initial_offset = other.master_initial_offset;
    }

    MasterSaveInfo &operator=(const MasterSaveInfo &other) {
        masterhost = other.masterhost;
        masterport = other.masterport;
        memcpy(master_replid, other.master_replid, sizeof(master_replid));
        master_initial_offset = other.master_initial_offset;
        return *this;
    }

    sdsstring masterhost;
    int masterport;
    char master_replid[CONFIG_RUN_ID_SIZE+1];
    long long master_initial_offset;
    int selected_db;
};

/* This structure can be optionally passed to RDB save/load functions in
 * order to implement additional functionalities, by storing and loading
 * metadata to the RDB file.
 *
 * Currently the only use is to select a DB at load time, useful in
 * replication in order to make sure that chained slaves (slaves of slaves)
 * select the correct DB and are able to accept the stream coming from the
 * top-level master. */
class rdbSaveInfo {
public:
    rdbSaveInfo() {
        repl_stream_db = -1;
        repl_id_is_set = 0;
        memcpy(repl_id, "0000000000000000000000000000000000000000", sizeof(repl_id));
        repl_id[CONFIG_RUN_ID_SIZE] = '\0';
        repl_offset = -1;
        fForceSetKey = TRUE;
        mvccMinThreshold = 0;
    }
    rdbSaveInfo(const rdbSaveInfo &other) {
        repl_stream_db = other.repl_stream_db;
        repl_id_is_set = other.repl_id_is_set;
        memcpy(repl_id, other.repl_id, sizeof(repl_id));
        repl_offset = other.repl_offset;
        fForceSetKey = other.fForceSetKey;
        mvccMinThreshold = other.mvccMinThreshold;
        vecmastersaveinfo = other.vecmastersaveinfo;
        master_repl_offset = other.master_repl_offset;
        mi = other.mi;
    }

    rdbSaveInfo &operator=(const rdbSaveInfo &other) {
        repl_stream_db = other.repl_stream_db;
        repl_id_is_set = other.repl_id_is_set;
        memcpy(repl_id, other.repl_id, sizeof(repl_id));
        repl_offset = other.repl_offset;
        fForceSetKey = other.fForceSetKey;
        mvccMinThreshold = other.mvccMinThreshold;
        vecmastersaveinfo = other.vecmastersaveinfo;
        master_repl_offset = other.master_repl_offset;
        mi = other.mi;

        return *this;
    }

    void addMaster(const MasterSaveInfo &si) {
        vecmastersaveinfo.push_back(si);
    }

    size_t numMasters() {
        return vecmastersaveinfo.size();
    }

    /* Used saving and loading. */
    int repl_stream_db;  /* DB to select in g_pserver->master client. */

    /* Used only loading. */
    int repl_id_is_set;  /* True if repl_id field is set. */
    char repl_id[CONFIG_RUN_ID_SIZE+1];     /* Replication ID. */
    long long repl_offset;                  /* Replication offset. */
    int fForceSetKey;

    /* Used In Save */
    long long master_repl_offset;

    uint64_t mvccMinThreshold;
    std::vector<MasterSaveInfo> vecmastersaveinfo;
    struct redisMaster *mi = nullptr;
};

struct malloc_stats {
    size_t zmalloc_used;
    size_t process_rss;
    size_t allocator_allocated;
    size_t allocator_active;
    size_t allocator_resident;
    size_t sys_total;
    size_t sys_available;
};

typedef struct socketFds {
    int fd[CONFIG_BINDADDR_MAX];
    int count;
} socketFds;

/*-----------------------------------------------------------------------------
 * TLS Context Configuration
 *----------------------------------------------------------------------------*/

typedef struct redisTLSContextConfig {
    char *cert_file;                /* Server side and optionally client side cert file name */
    char *key_file;                 /* Private key filename for cert_file */
    char *key_file_pass;            /* Optional password for key_file */
    char *client_cert_file;         /* Certificate to use as a client; if none, use cert_file */
    char *client_key_file;          /* Private key filename for client_cert_file */
    char *client_key_file_pass;     /* Optional password for client_key_file */
    char *dh_params_file;
    char *ca_cert_file;
    char *ca_cert_dir;
    char *protocols;
    char *ciphers;
    char *ciphersuites;
    int prefer_server_ciphers;
    int session_caching;
    int session_cache_size;
    int session_cache_timeout;
    time_t cert_file_last_modified;
    time_t key_file_last_modified;
    time_t client_cert_file_last_modified;
    time_t client_key_file_last_modified;
    time_t ca_cert_file_last_modified;
    time_t ca_cert_dir_last_modified;
} redisTLSContextConfig;

/*-----------------------------------------------------------------------------
 * Global server state
 *----------------------------------------------------------------------------*/

struct clusterState;

/* AIX defines hz to __hz, we don't use this define and in order to allow
 * Redis build on AIX we need to undef it. */
#ifdef _AIX
#undef hz
#endif

#define CHILD_TYPE_NONE 0
#define CHILD_TYPE_RDB 1
#define CHILD_TYPE_AOF 2
#define CHILD_TYPE_LDB 3
#define CHILD_TYPE_MODULE 4

typedef enum childInfoType {
    CHILD_INFO_TYPE_CURRENT_INFO,
    CHILD_INFO_TYPE_AOF_COW_SIZE,
    CHILD_INFO_TYPE_RDB_COW_SIZE,
    CHILD_INFO_TYPE_MODULE_COW_SIZE
} childInfoType;

#define MAX_EVENT_LOOPS 16
#define IDX_EVENT_LOOP_MAIN 0

class GarbageCollectorCollection
{
    GarbageCollector<redisDbPersistentDataSnapshot> garbageCollectorSnapshot;
    GarbageCollector<ICollectable> garbageCollectorGeneric;

    class CPtrCollectable : public ICollectable 
    {
        void *m_pv;

    public:
        CPtrCollectable(void *pv) 
            : m_pv(pv)
            {}

        CPtrCollectable(CPtrCollectable &&move) {
            m_pv = move.m_pv;
            move.m_pv = nullptr;
        }

        virtual ~CPtrCollectable() {
            zfree(m_pv);
        }
    };

public:
    struct Epoch
    {
        uint64_t epochSnapshot = 0;
        uint64_t epochGeneric = 0;

        void reset() {
            epochSnapshot = 0;
            epochGeneric = 0;
        }

        Epoch() = default;

        Epoch (const Epoch &other) {
            epochSnapshot = other.epochSnapshot;
            epochGeneric = other.epochGeneric;
        }

        Epoch &operator=(const Epoch &other) {
            serverAssert(isReset());
            epochSnapshot = other.epochSnapshot;
            epochGeneric = other.epochGeneric;
            return *this;
        }

        bool isReset() const {
            return epochSnapshot == 0 && epochGeneric == 0;
        }
    };

    Epoch startEpoch()
    {
        Epoch e;
        e.epochSnapshot = garbageCollectorSnapshot.startEpoch();
        e.epochGeneric = garbageCollectorGeneric.startEpoch();
        return e;
    }

    void endEpoch(Epoch &e, bool fNoFree = false)
    {
        auto epochSnapshot = e.epochSnapshot;
        auto epochGeneric = e.epochGeneric;
        e.reset();  // We must do this early as GC'd dtors can themselves try to enqueue more data
        garbageCollectorSnapshot.endEpoch(epochSnapshot, fNoFree);
        garbageCollectorGeneric.endEpoch(epochGeneric, fNoFree);
    }

    bool empty()
    {
        return garbageCollectorGeneric.empty() && garbageCollectorSnapshot.empty();
    }

    void shutdown()
    {
        garbageCollectorSnapshot.shutdown();
        garbageCollectorGeneric.shutdown();
    }

    void enqueue(Epoch e, std::unique_ptr<redisDbPersistentDataSnapshot> &&sp)
    {
        garbageCollectorSnapshot.enqueue(e.epochSnapshot, std::move(sp));
    }

    void enqueue(Epoch e, std::unique_ptr<ICollectable> &&sp)
    {
        garbageCollectorGeneric.enqueue(e.epochGeneric, std::move(sp));
    }

    template<typename T>
    void enqueueCPtr(Epoch e, T p)
    {
        auto sp = std::make_unique<CPtrCollectable>(reinterpret_cast<void*>(p));
        enqueue(e, std::move(sp));
    }
};

// Per-thread variabels that may be accessed without a lock
struct redisServerThreadVars {
    aeEventLoop *el = nullptr;
    socketFds ipfd;             /* TCP socket file descriptors */
    socketFds tlsfd;            /* TLS socket file descriptors */
    int in_eval;                /* Are we inside EVAL? */
    int in_exec;                /* Are we inside EXEC? */
    std::vector<client*> clients_pending_write; /* There is to write or install handler. */
    list *unblocked_clients;     /* list of clients to unblock before next loop NOT THREADSAFE */
    list *clients_pending_asyncwrite;
    int cclients;
    int cclientsReplica = 0;
    client *current_client; /* Current client */
    long fixed_time_expire = 0;     /* If > 0, expire keys against server.mstime. */
    client *lua_client = nullptr;   /* The "fake client" to query Redis from Lua */
    struct fastlock lockPendingWrite { "thread pending write" };
    char neterr[ANET_ERR_LEN];   /* Error buffer for anet.c */
    long unsigned commandsExecuted = 0;
    GarbageCollectorCollection::Epoch gcEpoch;
    const redisDbPersistentDataSnapshot **rgdbSnapshot = nullptr;
    long long stat_total_error_replies; /* Total number of issued error replies ( command + rejected errors ) */
    long long prev_err_count; /* per thread marker of exisiting errors during a call */
    bool fRetrySetAofEvent = false;
    bool modulesEnabledThisAeLoop = false; /* In this loop of aeMain, were modules enabled before 
                                              the thread went to sleep? */
    bool disable_async_commands = false; /* this is only valid for one cycle of the AE loop and is reset in afterSleep */
    
    int propagate_in_transaction = 0;  /* Make sure we don't propagate nested MULTI/EXEC */
    int client_pause_in_transaction = 0; /* Was a client pause executed during this Exec? */
    std::unordered_set<client*> setclientsProcess;
    std::unordered_set<client*> setclientsPrefetch;
    std::unordered_set<StorageToken*> setStorageTokensProcess;
    dictAsyncRehashCtl *rehashCtl = nullptr;

    int getRdbKeySaveDelay();
private:
    int rdb_key_save_delay = -1; // thread local cache
};

// Const vars are not changed after worker threads are launched
struct redisServerConst {
    pid_t pid;                  /* Main process pid. */
    time_t stat_starttime;          /* Server start time */
    pthread_t main_thread_id;         /* Main thread id */
    pthread_t time_thread_id;
    char *configfile;           /* Absolute config file path, or NULL */
    char *executable;           /* Absolute executable file path. */
    char **exec_argv;           /* Executable argv vector (copy). */

    int cthreads;               /* Number of main worker threads */
    int fThreadAffinity;        /* Should we pin threads to cores? */
    int threadAffinityOffset = 0; /* Where should we start pinning them? */
    char *pidfile;              /* PID file path */

    /* Fast pointers to often looked up command */
    struct redisCommand *delCommand, *multiCommand, *lpushCommand,
                        *lpopCommand, *rpopCommand, *zpopminCommand,
                        *zpopmaxCommand, *sremCommand, *execCommand,
                        *expireCommand, *pexpireCommand, *xclaimCommand,
                        *xgroupCommand, *rreplayCommand, *rpoplpushCommand,
                        *hdelCommand, *zremCommand, *lmoveCommand;

    /* Configuration */
    char *default_masteruser;               /* AUTH with this user and masterauth with master */
    char *default_masterauth;               /* AUTH with this password with master */
    int verbosity;                  /* Loglevel in keydb.conf */
    int maxidletime;                /* Client timeout in seconds */
    int tcpkeepalive;               /* Set SO_KEEPALIVE if non-zero. */
    int active_expire_enabled;      /* Can be disabled for testing purposes. */
    int active_expire_effort;       /* From 1 (default) to 10, active effort. */
    int active_defrag_enabled;
    int jemalloc_bg_thread;         /* Enable jemalloc background thread */
    size_t active_defrag_ignore_bytes; /* minimum amount of fragmentation waste to start active defrag */
    int active_defrag_threshold_lower; /* minimum percentage of fragmentation to start active defrag */
    int active_defrag_threshold_upper; /* maximum percentage of fragmentation at which we use maximum effort */
    int active_defrag_cycle_min;       /* minimal effort for defrag in CPU percentage */
    int active_defrag_cycle_max;       /* maximal effort for defrag in CPU percentage */
    unsigned long active_defrag_max_scan_fields; /* maximum number of fields of set/hash/zset/list to process from within the main dict scan */
    size_t client_max_querybuf_len; /* Limit for client query buffer length */
    int dbnum = 0;                      /* Total number of configured DBs */
    int supervised;                 /* 1 if supervised, 0 otherwise. */
    int supervised_mode;            /* See SUPERVISED_* */
    int daemonize;                  /* True if running as a daemon */
    int sanitize_dump_payload;      /* Enables deep sanitization for ziplist and listpack in RDB and RESTORE. */
    int skip_checksum_validation;   /* Disables checksum validateion for RDB and RESTORE payload. */
    int set_proc_title;             /* True if change proc title */
    char *proc_title_template;      /* Process title template format */
    clientBufferLimitsConfig client_obuf_limits[CLIENT_TYPE_OBUF_COUNT];

    /* System hardware info */
    size_t system_memory_size;  /* Total memory in system as reported by OS */

    unsigned char uuid[UUID_BINARY_LEN];         /* This server's UUID - populated on boot */

    int enable_motd;            /* Flag to retrieve the Message of today using CURL request*/

    int delete_on_evict = false;   // Only valid when a storage provider is set
    int thread_min_client_threshold = 50;
    int multimaster_no_forward;
    int storage_memory_model = STORAGE_WRITETHROUGH;
    char *storage_conf = nullptr;
    int fForkBgSave = false;
    int time_thread_priority = false;
    long long repl_backlog_disk_size = 0;
    int force_backlog_disk = 0;
};

struct redisServer {
    /* General */
    int dynamic_hz;             /* Change hz value depending on # of clients. */
    int config_hz;              /* Configured HZ value. May be different than
                                   the actual 'hz' field value if dynamic-hz
                                   is enabled. */
    mode_t umask;               /* The umask value of the process on startup */
    std::atomic<int> hz;        /* serverCron() calls frequency in hertz */
    int in_fork_child;          /* indication that this is a fork child */
    IStorage *metadataDb = nullptr;
    redisDb **db = nullptr;
    dict *commands;             /* Command table */
    dict *orig_commands;        /* Command table before command renaming. */


    struct redisServerThreadVars rgthreadvar[MAX_EVENT_LOOPS];
    struct redisServerThreadVars modulethreadvar; /* Server thread local variables to be used by module threads */
    pthread_t rgthread[MAX_EVENT_LOOPS];

    std::atomic<unsigned int> lruclock;      /* Clock for LRU eviction */
    std::atomic<int> shutdown_asap;          /* SHUTDOWN needed ASAP */
    rax *errors;                /* Errors table */
    int activerehashing;        /* Incremental rehash in serverCron() */
    int active_defrag_running;  /* Active defragmentation running (holds current scan aggressiveness) */
    int cronloops;              /* Number of times the cron function run */
    char runid[CONFIG_RUN_ID_SIZE+1];  /* ID always different at every exec. */
    int sentinel_mode;          /* True if this instance is a Sentinel. */
    size_t initial_memory_usage; /* Bytes used after initialization. */
    int always_show_logo;       /* Show logo even for non-stdout logging. */
    char *ignore_warnings;      /* Config: warnings that should be ignored. */
    pause_type client_pause_type;      /* True if clients are currently paused */
    /* Modules */
    ::dict *moduleapi;            /* Exported core APIs dictionary for modules. */
    ::dict *sharedapi;            /* Like moduleapi but containing the APIs that
                                   modules share with each other. */
    list *loadmodule_queue;     /* List of modules to load at startup. */
    pid_t child_pid;            /* PID of current child */
    int child_type;             /* Type of current child */
    client *module_client;      /* "Fake" client to call Redis from modules */
    /* Networking */
    int port;                   /* TCP listening port */
    int tls_port;               /* TLS listening port */
    int tcp_backlog;            /* TCP listen() backlog */
    char *bindaddr[CONFIG_BINDADDR_MAX]; /* Addresses we should bind to */
    int bindaddr_count;         /* Number of addresses in g_pserver->bindaddr[] */
    char *unixsocket;           /* UNIX socket path */
    mode_t unixsocketperm;      /* UNIX socket permission */
    int sofd;                   /* Unix socket file descriptor */
    socketFds cfd;              /* Cluster bus listening socket */
    list *clients;              /* List of active clients */
    list *clients_to_close;     /* Clients to close asynchronously */
    list *slaves, *monitors;    /* List of slaves and MONITORs */
    rax *clients_timeout_table; /* Radix tree for blocked clients timeouts. */
    long fixed_time_expire;     /* If > 0, expire keys against server.mstime. */
    rax *clients_index;         /* Active clients dictionary by client ID. */
    list *paused_clients;       /* List of pause clients */
    mstime_t client_pause_end_time; /* Time when we undo clients_paused */
    ::dict *migrate_cached_sockets;/* MIGRATE cached sockets */
    std::atomic<uint64_t> next_client_id; /* Next client unique ID. Incremental. */
    int protected_mode;         /* Don't accept external connections. */
    long long events_processed_while_blocked; /* processEventsWhileBlocked() */

    /* RDB / AOF loading information */
    std::atomic<int> loading; /* We are loading data from disk if true */
    off_t loading_total_bytes;
    off_t loading_rdb_used_mem;
    off_t loading_loaded_bytes;
    time_t loading_start_time;
    unsigned long loading_process_events_interval_bytes;
    unsigned int loading_process_events_interval_keys;

    int active_expire_enabled;      /* Can be disabled for testing purposes. */

    int replicaIsolationFactor = 1;

    /* Fields used only for stats */
    long long stat_numcommands;     /* Number of processed commands */
    long long stat_numconnections;  /* Number of connections received */
    long long stat_expiredkeys;     /* Number of expired keys */
    double stat_expired_stale_perc; /* Percentage of keys probably expired */
    long long stat_expired_time_cap_reached_count; /* Early expire cylce stops.*/
    long long stat_expire_cycle_time_used; /* Cumulative microseconds used. */
    long long stat_evictedkeys;     /* Number of evicted keys (maxmemory) */
    long long stat_keyspace_hits;   /* Number of successful lookups of keys */
    long long stat_keyspace_misses; /* Number of failed lookups of keys */
    long long stat_active_defrag_hits;      /* number of allocations moved */
    long long stat_active_defrag_misses;    /* number of allocations scanned but not moved */
    long long stat_active_defrag_key_hits;  /* number of keys with moved allocations */
    long long stat_active_defrag_key_misses;/* number of keys scanned and not moved */
    long long stat_active_defrag_scanned;   /* number of dictEntries scanned */
    size_t stat_peak_memory;        /* Max used memory record */
    long long stat_fork_time;       /* Time needed to perform latest fork() */
    double stat_fork_rate;          /* Fork rate in GB/sec. */
    long long stat_total_forks;     /* Total count of fork. */
    long long stat_rejected_conn;   /* Clients rejected because of maxclients */
    long long stat_sync_full;       /* Number of full resyncs with slaves. */
    long long stat_sync_partial_ok; /* Number of accepted PSYNC requests. */
    long long stat_sync_partial_err;/* Number of unaccepted PSYNC requests. */
    list *slowlog;                  /* SLOWLOG list of commands */
    long long slowlog_entry_id;     /* SLOWLOG current entry ID */
    long long slowlog_log_slower_than; /* SLOWLOG time limit (to get logged) */
    unsigned long slowlog_max_len;     /* SLOWLOG max number of items logged */
    struct malloc_stats cron_malloc_stats; /* sampled in serverCron(). */
    std::atomic<long long> stat_net_input_bytes; /* Bytes read from network. */
    std::atomic<long long> stat_net_output_bytes; /* Bytes written to network. */
    size_t stat_current_cow_bytes;  /* Copy on write bytes while child is active. */
    monotime stat_current_cow_updated;  /* Last update time of stat_current_cow_bytes */
    size_t stat_current_save_keys_processed;  /* Processed keys while child is active. */
    size_t stat_current_save_keys_total;  /* Number of keys when child started. */
    size_t stat_rdb_cow_bytes;      /* Copy on write bytes during RDB saving. */
    size_t stat_aof_cow_bytes;      /* Copy on write bytes during AOF rewrite. */
    size_t stat_module_cow_bytes;   /* Copy on write bytes during module fork. */
    double stat_module_progress;   /* Module save progress. */
    uint64_t stat_clients_type_memory[CLIENT_TYPE_COUNT];/* Mem usage by type */
    long long stat_unexpected_error_replies; /* Number of unexpected (aof-loading, replica to master, etc.) error replies */
    long long stat_dump_payload_sanitizations; /* Number deep dump payloads integrity validations. */
    std::atomic<long long> stat_total_reads_processed; /* Total number of read events processed */
    std::atomic<long long> stat_total_writes_processed; /* Total number of write events processed */
    long long stat_storage_provider_read_hits;
    long long stat_storage_provider_read_misses;
    /* The following two are used to track instantaneous metrics, like
     * number of operations per second, network traffic. */
    struct {
        long long last_sample_time; /* Timestamp of last sample in ms */
        long long last_sample_count;/* Count in last sample */
        long long samples[STATS_METRIC_SAMPLES];
        int idx;
    } inst_metric[STATS_METRIC_COUNT];
    /* AOF persistence */
    int aof_enabled;                /* AOF configuration */
    int aof_state;                  /* AOF_(ON|OFF|WAIT_REWRITE) */
    int aof_fsync;                  /* Kind of fsync() policy */
    char *aof_filename;             /* Name of the AOF file */
    int aof_no_fsync_on_rewrite;    /* Don't fsync if a rewrite is in prog. */
    int aof_rewrite_perc;           /* Rewrite AOF if % growth is > M and... */
    off_t aof_rewrite_min_size;     /* the AOF file is at least N bytes. */
    off_t aof_rewrite_base_size;    /* AOF size on latest startup or rewrite. */
    off_t aof_current_size;         /* AOF current size. */
    off_t aof_fsync_offset;         /* AOF offset which is already synced to disk. */
    int aof_flush_sleep;            /* Micros to sleep before flush. (used by tests) */
    int aof_rewrite_scheduled;      /* Rewrite once BGSAVE terminates. */
    list *aof_rewrite_buf_blocks;   /* Hold changes during an AOF rewrite. */
    sds aof_buf;      /* AOF buffer, written before entering the event loop */
    int aof_fd;       /* File descriptor of currently selected AOF file */
    int aof_selected_db; /* Currently selected DB in AOF */
    time_t aof_flush_postponed_start; /* UNIX time of postponed AOF flush */
    time_t aof_last_fsync;            /* UNIX time of last fsync() */
    time_t aof_rewrite_time_last;   /* Time used by last AOF rewrite run. */
    time_t aof_rewrite_time_start;  /* Current AOF rewrite start time. */
    int aof_lastbgrewrite_status;   /* C_OK or C_ERR */
    unsigned long aof_delayed_fsync;  /* delayed AOF fsync() counter */
    int aof_rewrite_incremental_fsync;/* fsync incrementally while aof rewriting? */
    int rdb_save_incremental_fsync;   /* fsync incrementally while rdb saving? */
    int aof_last_write_status;      /* C_OK or C_ERR */
    int aof_last_write_errno;       /* Valid if aof write/fsync status is ERR */
    int aof_load_truncated;         /* Don't stop on unexpected AOF EOF. */
    int aof_use_rdb_preamble;       /* Use RDB preamble on AOF rewrites. */
    redisAtomic int aof_bio_fsync_status; /* Status of AOF fsync in bio job. */
    redisAtomic int aof_bio_fsync_errno;  /* Errno of AOF fsync in bio job. */
    /* AOF pipes used to communicate between parent and child during rewrite. */
    int aof_pipe_write_data_to_child;
    int aof_pipe_read_data_from_parent;
    int aof_pipe_write_ack_to_parent;
    int aof_pipe_read_ack_from_child;
    aeEventLoop *el_alf_pip_read_ack_from_child;
    int aof_pipe_write_ack_to_child;
    int aof_pipe_read_ack_from_parent;
    int aof_stop_sending_diff;     /* If true stop sending accumulated diffs
                                      to child process. */
    sds aof_child_diff;             /* AOF diff accumulator child side. */
    int aof_rewrite_pending = 0;    /* is a call to aofChildWriteDiffData already queued? */
    /* RDB persistence */
    int allowRdbResizeOp;           /* Debug situations we may want rehash to be ocurring, so ignore resize */
    long long dirty;                /* Changes to DB from the last save */
    long long dirty_before_bgsave;  /* Used to restore dirty on failed BGSAVE */
    struct _rdbThreadVars
    {
        std::atomic<bool> fRdbThreadCancel {false};
        std::atomic<bool> fDone {false};
        int tmpfileNum = 0;
        pthread_t rdb_child_thread;
        int fRdbThreadActive = false;
    } rdbThreadVars;
    struct saveparam *saveparams;   /* Save points array for RDB */
    int saveparamslen;              /* Number of saving points */
    char *rdb_filename;             /* Name of RDB file */
    char *rdb_s3bucketpath;         /* Path for AWS S3 backup of RDB file */
    int rdb_compression;            /* Use compression in RDB? */
    int rdb_checksum;               /* Use RDB checksum? */
    int rdb_del_sync_files;         /* Remove RDB files used only for SYNC if
                                       the instance does not use persistence. */
    time_t lastsave;                /* Unix time of last successful save */
    time_t lastbgsave_try;          /* Unix time of last attempted bgsave */
    time_t rdb_save_time_last;      /* Time used by last RDB save run. */
    time_t rdb_save_time_start;     /* Current RDB save start time. */
    mstime_t rdb_save_latency;      /* Used to track end to end latency of rdb save*/
    pid_t rdb_child_pid = -1;       /* Used only during fork bgsave */
    int rdb_bgsave_scheduled;       /* BGSAVE when possible if true. */
    int rdb_child_type;             /* Type of save by active child. */
    int lastbgsave_status;          /* C_OK or C_ERR */
    int stop_writes_on_bgsave_err;  /* Don't allow writes if can't BGSAVE */
    int rdb_pipe_read;              /* RDB pipe used to transfer the rdb data */
                                    /* to the parent process in diskless repl. */
    int rdb_child_exit_pipe;        /* Used by the diskless parent allow child exit. */
    connection **rdb_pipe_conns;    /* Connections which are currently the */
    int rdb_pipe_numconns;          /* target of diskless rdb fork child. */
    int rdb_pipe_numconns_writing;  /* Number of rdb conns with pending writes. */
    char *rdb_pipe_buff;            /* In diskless replication, this buffer holds data */
    int rdb_pipe_bufflen;           /* that was read from the the rdb pipe. */
    int rdb_key_save_delay;         /* Delay in microseconds between keys while
                                     * writing the RDB. (for testings). negative
                                     * value means fractions of microsecons (on average). */
    int key_load_delay;             /* Delay in microseconds between keys while
                                     * loading aof or rdb. (for testings). negative
                                     * value means fractions of microsecons (on average). */
    /* Pipe and data structures for child -> parent info sharing. */
    int child_info_pipe[2];         /* Pipe used to write the child_info_data. */
    int child_info_nread;           /* Num of bytes of the last read from pipe */
    /* Propagation of commands in AOF / replication */
    redisOpArray also_propagate;    /* Additional command to propagate. */
    int replication_allowed;        /* Are we allowed to replicate? */
    /* Logging */
    char *logfile;                  /* Path of log file */
    int syslog_enabled;             /* Is syslog enabled? */
    char *syslog_ident;             /* Syslog ident */
    int syslog_facility;            /* Syslog facility */
    int crashlog_enabled;           /* Enable signal handler for crashlog.
                                     * disable for clean core dumps. */
    int memcheck_enabled;           /* Enable memory check on crash. */
    int use_exit_on_panic;          /* Use exit() on panic and assert rather than
                                     * abort(). useful for Valgrind. */
    /* Replication (master) */
    char replid[CONFIG_RUN_ID_SIZE+1];  /* My current replication ID. */
    char replid2[CONFIG_RUN_ID_SIZE+1]; /* replid inherited from master*/
    long long master_repl_offset;   /* My current replication offset */
    long long second_replid_offset; /* Accept offsets up to this for replid2. */
    int replicaseldb;                 /* Last SELECTed DB in replication output */
    int repl_ping_slave_period;     /* Master pings the replica every N seconds */
    char *repl_backlog;             /* Replication backlog for partial syncs */
    char *repl_backlog_disk = nullptr;
    long long repl_backlog_size;    /* Backlog circular buffer size */
    long long repl_backlog_config_size; /* The repl backlog may grow but we want to know what the user set it to */
    long long repl_backlog_histlen; /* Backlog actual data length */
    long long repl_backlog_idx;     /* Backlog circular buffer current offset,
                                       that is the next byte will'll write to.*/
    long long repl_backlog_off;     /* Replication "master offset" of first
                                       byte in the replication backlog buffer.*/
    long long repl_backlog_start;   /* Used to compute indicies from offsets
                                       basically, index = (offset - start) % size */
    fastlock repl_backlog_lock {"replication backlog"};
    time_t repl_backlog_time_limit; /* Time without slaves after the backlog
                                       gets released. */
    time_t repl_no_slaves_since;    /* We have no slaves since that time.
                                       Only valid if g_pserver->slaves len is 0. */
    int repl_min_slaves_to_write;   /* Min number of slaves to write. */
    int repl_min_slaves_max_lag;    /* Max lag of <count> slaves to write. */
    int repl_good_slaves_count;     /* Number of slaves with lag <= max_lag. */
    int repl_diskless_sync;         /* Master send RDB to slaves sockets directly. */
    int repl_diskless_load;         /* Slave parse RDB directly from the socket.
                                     * see REPL_DISKLESS_LOAD_* enum */
    int repl_diskless_sync_delay;   /* Delay to start a diskless repl BGSAVE. */
    std::atomic <long long> repl_lowest_off; /* The lowest offset amongst all replicas
                                                -1 if there are no replicas */
    /* Replication (replica) */
    list *masters;
    int enable_multimaster; 
    int repl_timeout;               /* Timeout after N seconds of master idle */
    int repl_syncio_timeout; /* Timeout for synchronous I/O calls */
    int repl_disable_tcp_nodelay;   /* Disable TCP_NODELAY after SYNC? */
    int repl_serve_stale_data; /* Serve stale data when link is down? */
    int repl_quorum;           /* For multimaster what do we consider a quorum? -1 means all master must be online */
    int repl_slave_ro;          /* Slave is read only? */
    int repl_slave_ignore_maxmemory;    /* If true slaves do not evict. */
    int slave_priority;             /* Reported in INFO and used by Sentinel. */
    int replica_announced;          /* If true, replica is announced by Sentinel */
    int slave_announce_port;        /* Give the master this listening port. */
    char *slave_announce_ip;        /* Give the master this ip address. */
    int repl_slave_lazy_flush;          /* Lazy FLUSHALL before loading DB? */
    /* Replication script cache. */
    ::dict *repl_scriptcache_dict;        /* SHA1 all slaves are aware of. */
    list *repl_scriptcache_fifo;        /* First in, first out LRU eviction. */
    unsigned int repl_scriptcache_size; /* Max number of elements. */
    /* Synchronous replication. */
    list *clients_waiting_acks;         /* Clients waiting in WAIT command. */
    int get_ack_from_slaves;            /* If true we send REPLCONF GETACK. */
    /* Limits */
    unsigned int maxclients;            /* Max number of simultaneous clients */
    unsigned int maxclientsReserved;    /* Reserved amount for health checks (localhost conns) */
    unsigned long long maxmemory;   /* Max number of memory bytes to use */
    unsigned long long maxstorage;  /* Max number of bytes to use in a storage provider */
    int maxmemory_policy;           /* Policy for key eviction */
    int maxmemory_samples;          /* Precision of random sampling */
    int maxmemory_eviction_tenacity;/* Aggressiveness of eviction processing */
    int force_eviction_percent;     /* Force eviction when this percent of system memory is remaining */
    int lfu_log_factor;             /* LFU logarithmic counter factor. */
    int lfu_decay_time;             /* LFU counter decay factor. */
    long long proto_max_bulk_len;   /* Protocol bulk length maximum size. */
    int oom_score_adj_base;         /* Base oom_score_adj value, as observed on startup */
    int oom_score_adj_values[CONFIG_OOM_COUNT];   /* Linux oom_score_adj configuration */
    int oom_score_adj;                            /* If true, oom_score_adj is managed */
    int disable_thp;                              /* If true, disable THP by syscall */
    /* Blocked clients */
    unsigned int blocked_clients;   /* # of clients executing a blocking cmd.*/
    unsigned int blocked_clients_by_type[BLOCKED_NUM];
    list *ready_keys;        /* List of readyList structures for BLPOP & co */
    /* Client side caching. */
    unsigned int tracking_clients;  /* # of clients with tracking enabled.*/
    size_t tracking_table_max_keys; /* Max number of keys in tracking table. */
    /* Sort parameters - qsort_r() is only available under BSD so we
     * have to take this state global, in order to pass it to sortCompare() */
    int sort_desc;
    int sort_alpha;
    int sort_bypattern;
    int sort_store;
    /* Zip structure config, see keydb.conf for more information  */
    size_t hash_max_ziplist_entries;
    size_t hash_max_ziplist_value;
    size_t set_max_intset_entries;
    size_t zset_max_ziplist_entries;
    size_t zset_max_ziplist_value;
    size_t hll_sparse_max_bytes;
    size_t stream_node_max_bytes;
    long long stream_node_max_entries;
    /* List parameters */
    int list_max_ziplist_size;
    int list_compress_depth;
    /* time cache */
    std::atomic<time_t> unixtime;    /* Unix time sampled every cron cycle. */
    time_t timezone;            /* Cached timezone. As set by tzset(). */
    int daylight_active;        /* Currently in daylight saving time. */
    mstime_t mstime;            /* 'unixtime' in milliseconds. */
    ustime_t ustime;            /* 'unixtime' in microseconds. */
    size_t blocking_op_nesting; /* Nesting level of blocking operation, used to reset blocked_last_cron. */
    long long blocked_last_cron; /* Indicate the mstime of the last time we did cron jobs from a blocking operation */
    /* Pubsub */
    dict *pubsub_channels;  /* Map channels to list of subscribed clients */
    dict *pubsub_patterns;  /* A dict of pubsub_patterns */
    int notify_keyspace_events; /* Events to propagate via Pub/Sub. This is an
                                   xor of NOTIFY_... flags. */
    /* Cluster */
    int cluster_enabled;      /* Is cluster enabled? */
    mstime_t cluster_node_timeout; /* Cluster node timeout. */
    char *cluster_configfile; /* Cluster auto-generated config file name. */
    struct clusterState *cluster;  /* State of the cluster */
    int cluster_migration_barrier; /* Cluster replicas migration barrier. */
    int cluster_allow_replica_migration; /* Automatic replica migrations to orphaned masters and from empty masters */
    int cluster_slave_validity_factor; /* Slave max data age for failover. */
    int cluster_require_full_coverage; /* If true, put the cluster down if
                                          there is at least an uncovered slot.*/
    int cluster_slave_no_failover;  /* Prevent replica from starting a failover
                                       if the master is in failure state. */
    char *cluster_announce_ip;  /* IP address to announce on cluster bus. */
    int cluster_announce_port;     /* base port to announce on cluster bus. */
    int cluster_announce_tls_port; /* TLS port to announce on cluster bus. */
    int cluster_announce_bus_port; /* bus port to announce on cluster bus. */
    int cluster_module_flags;      /* Set of flags that Redis modules are able
                                      to set in order to suppress certain
                                      native Redis Cluster features. Check the
                                      REDISMODULE_CLUSTER_FLAG_*. */
    int cluster_allow_reads_when_down; /* Are reads allowed when the cluster
                                        is down? */
    int cluster_config_file_lock_fd;   /* cluster config fd, will be flock */
    /* Scripting */
    lua_State *lua; /* The Lua interpreter. We use just one for all clients */
    client *lua_caller = nullptr;   /* The client running EVAL right now, or NULL */
    char* lua_cur_script = nullptr; /* SHA1 of the script currently running, or NULL */
    ::dict *lua_scripts;         /* A dictionary of SHA1 -> Lua scripts */
    unsigned long long lua_scripts_mem;  /* Cached scripts' memory + oh */
    mstime_t lua_time_limit;  /* Script timeout in milliseconds */
    monotime lua_time_start;  /* monotonic timer to detect timed-out script */
    mstime_t lua_time_snapshot; /* Snapshot of mstime when script is started */
    int lua_write_dirty;  /* True if a write command was called during the
                             execution of the current script. */
    int lua_random_dirty; /* True if a random command was called during the
                             execution of the current script. */
    int lua_replicate_commands; /* True if we are doing single commands repl. */
    int lua_multi_emitted;/* True if we already propagated MULTI. */
    int lua_repl;         /* Script replication flags for redis.set_repl(). */
    int lua_timedout;     /* True if we reached the time limit for script
                             execution. */
    int lua_kill;         /* Kill the script if true. */
    int lua_always_replicate_commands; /* Default replication type. */
    int lua_oom;          /* OOM detected when script start? */
    /* Lazy free */
    int lazyfree_lazy_eviction;
    int lazyfree_lazy_expire;
    int lazyfree_lazy_server_del;
    int lazyfree_lazy_user_del;
    int lazyfree_lazy_user_flush;
    /* Latency monitor */
    long long latency_monitor_threshold;
    ::dict *latency_events;
    /* ACLs */
    char *acl_filename;           /* ACL Users file. NULL if not configured. */
    unsigned long acllog_max_len; /* Maximum length of the ACL LOG list. */
    sds requirepass;              /* Remember the cleartext password set with
                                     the old "requirepass" directive for
                                     backward compatibility with Redis <= 5. */
    int acl_pubsub_default;      /* Default ACL pub/sub channels flag */
    /* Assert & bug reporting */
    int watchdog_period;  /* Software watchdog period in ms. 0 = off */

    int fActiveReplica;                          /* Can this replica also be a master? */
    int fWriteDuringActiveLoad;                  /* Can this active-replica write during an RDB load? */

    // Format:
    //  Lower 20 bits: a counter incrementing for each command executed in the same millisecond
    //  Upper 44 bits: mstime (least significant 44-bits) enough for ~500 years before rollover from date of addition
    uint64_t mvcc_tstamp;

    AsyncWorkQueue *asyncworkqueue;
    AsyncWorkQueue *asyncreadworkqueue;
    AsyncWorkQueue *asyncwriteworkqueue;

    /* System hardware info */
    size_t system_memory_size;  /* Total memory in system as reported by OS */

    GarbageCollectorCollection garbageCollector;

    IStorageFactory *m_pstorageFactory = nullptr;
    int storage_flush_period;   // The time between flushes in the CRON job

    long long snapshot_slip = 500;   // The amount of time in milliseconds we let a snapshot be behind the current database

    /* TLS Configuration */
    int tls_cluster;
    int tls_replication;
    int tls_auth_clients;
    int tls_rotation;

    std::set<sdsstring> tls_auditlog_blocklist; /* Certificates that can be excluded from audit logging */
    std::set<sdsstring> tls_allowlist;
    redisTLSContextConfig tls_ctx_config;

    /* cpu affinity */
    char *server_cpulist; /* cpu affinity list of redis server main/io thread. */
    char *bio_cpulist; /* cpu affinity list of bio thread. */
    char *aof_rewrite_cpulist; /* cpu affinity list of aof rewrite process. */
    char *bgsave_cpulist; /* cpu affinity list of bgsave process. */

    int prefetch_enabled = 1;
    /* Sentinel config */
    struct sentinelConfig *sentinel_config; /* sentinel config to load at startup time. */
    /* Coordinate failover info */
    mstime_t failover_end_time; /* Deadline for failover command. */
    int force_failover; /* If true then failover will be foreced at the
                         * deadline, otherwise failover is aborted. */
    char *target_replica_host; /* Failover target host. If null during a
                                * failover then any replica can be used. */
    int target_replica_port; /* Failover target port */
    int failover_state; /* Failover state */

    int enable_async_commands;
    int multithread_load_enabled = 0;
    int active_client_balancing = 1;

    long long repl_batch_offStart = -1;
    long long repl_batch_idxStart = -1;

    long long rand_total_threshold;

    int config_soft_shutdown = false;
    bool soft_shutdown = false;

    int flash_disable_key_cache = false;

    /* Lock Contention Ring Buffer */
    static const size_t s_lockContentionSamples = 64;
    uint16_t rglockSamples[s_lockContentionSamples];
    unsigned ilockRingHead = 0;


    sds sdsAvailabilityZone;
    int overload_protect_threshold = 0;
    int is_overloaded = 0;
    int overload_closed_clients = 0;

        int module_blocked_pipe[2]; /* Pipe used to awake the event loop if a
                            client blocked on a module command needs
                            to be processed. */

    bool FRdbSaveInProgress() const { return g_pserver->rdbThreadVars.fRdbThreadActive; }
};

inline int redisServerThreadVars::getRdbKeySaveDelay() {
    if (rdb_key_save_delay < 0) {
        __atomic_load(&g_pserver->rdb_key_save_delay, &rdb_key_save_delay, __ATOMIC_ACQUIRE);
    }
    return rdb_key_save_delay;
}


#define MAX_KEYS_BUFFER 256

/* A result structure for the various getkeys function calls. It lists the
 * keys as indices to the provided argv.
 */
typedef struct {
    int keysbuf[MAX_KEYS_BUFFER];       /* Pre-allocated buffer, to save heap allocations */
    int *keys;                          /* Key indices array, points to keysbuf or heap */
    int numkeys;                        /* Number of key indices return */
    int size;                           /* Available array size */
} getKeysResult;
#define GETKEYS_RESULT_INIT { {0}, NULL, 0, MAX_KEYS_BUFFER }

typedef void redisCommandProc(client *c);
typedef int redisGetKeysProc(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result);
struct redisCommand {
    const char *name;
    redisCommandProc *proc;
    int arity;
    const char *sflags;   /* Flags as string representation, one char per flag. */
    uint64_t flags; /* The actual flags, obtained from the 'sflags' field. */
    /* Use a function to determine keys arguments in a command line.
     * Used for Redis Cluster redirect. */
    redisGetKeysProc *getkeys_proc;
    /* What keys should be loaded in background when calling this command? */
    int firstkey; /* The first argument that's a key (0 = no keys) */
    int lastkey;  /* The last argument that's a key */
    int keystep;  /* The step between first and last key */
    long long microseconds, calls, rejected_calls, failed_calls;
    int id;     /* Command ID. This is a progressive ID starting from 0 that
                   is assigned at runtime, and is used in order to check
                   ACLs. A connection is able to execute a given command if
                   the user associated to the connection has this command
                   bit set in the bitmap of allowed commands. */
};

struct redisError {
    long long count;
};

struct redisFunctionSym {
    char *name;
    unsigned long pointer;
};

typedef struct _redisSortObject {
    robj *obj;
    union {
        double score;
        robj *cmpobj;
    } u;
} redisSortObject;

typedef struct _redisSortOperation {
    int type;
    robj *pattern;
} redisSortOperation;

/* Structure to hold list iteration abstraction. */
typedef struct {
    robj_roptr subject;
    unsigned char encoding;
    unsigned char direction; /* Iteration direction */
    quicklistIter *iter;
} listTypeIterator;

/* Structure for an entry while iterating over a list. */
typedef struct {
    listTypeIterator *li;
    quicklistEntry entry; /* Entry in quicklist */
} listTypeEntry;

/* Structure to hold set iteration abstraction. */
typedef struct {
    robj_roptr subject;
    int encoding;
    int ii; /* intset iterator */
    dictIterator *di;
} setTypeIterator;

/* Structure to hold hash iteration abstraction. Note that iteration over
 * hashes involves both fields and values. Because it is possible that
 * not both are required, store pointers in the iterator to avoid
 * unnecessary memory allocation for fields/values. */
typedef struct {
    robj_roptr subject;
    int encoding;

    unsigned char *fptr, *vptr;

    dictIterator *di;
    dictEntry *de;
} hashTypeIterator;

#include "stream.h"  /* Stream data type header file. */

#define OBJ_HASH_KEY 1
#define OBJ_HASH_VALUE 2

/* Used in evict.cpp */
enum class EvictReason {
    User,       /* User memory exceeded limit */
    System      /* System memory exceeded limit */
};

/*-----------------------------------------------------------------------------
 * Extern declarations
 *----------------------------------------------------------------------------*/

//extern struct redisServer server;
extern struct redisServerConst cserver;
extern thread_local struct redisServerThreadVars *serverTL;   // thread local server vars
extern struct sharedObjectsStruct shared;
extern dictType objectKeyPointerValueDictType;
extern dictType objectKeyHeapPointerValueDictType;
extern dictType setDictType;
extern dictType zsetDictType;
extern dictType clusterNodesDictType;
extern dictType clusterNodesBlackListDictType;
extern dictType dbDictType;
extern dictType dbTombstoneDictType;
extern dictType dbSnapshotDictType;
extern dictType shaScriptObjectDictType;
extern double R_Zero, R_PosInf, R_NegInf, R_Nan;
extern dictType hashDictType;
extern dictType keylistDictType;
extern dictType replScriptCacheDictType;
extern dictType dbExpiresDictType;
extern dictType modulesDictType;
extern dictType sdsReplyDictType;
extern fastlock g_lockasyncfree;

/*-----------------------------------------------------------------------------
 * Functions prototypes
 *----------------------------------------------------------------------------*/

/* Modules */
void moduleInitModulesSystem(void);
void moduleInitModulesSystemLast(void);
int moduleLoad(const char *path, void **argv, int argc);
void moduleLoadFromQueue(void);
int moduleGetCommandKeysViaAPI(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result);
moduleType *moduleTypeLookupModuleByID(uint64_t id);
void moduleTypeNameByID(char *name, uint64_t moduleid);
const char *moduleTypeModuleName(moduleType *mt);
void moduleFreeContext(struct RedisModuleCtx *ctx);
void unblockClientFromModule(client *c);
void moduleHandleBlockedClients(int iel);
void moduleBlockedClientTimedOut(client *c);
void moduleBlockedClientPipeReadable(aeEventLoop *el, int fd, void *privdata, int mask);
size_t moduleCount(void);
void moduleAcquireGIL(int fServerThread, int fExclusive = FALSE);
int moduleTryAcquireGIL(bool fServerThread, int fExclusive = FALSE);
void moduleReleaseGIL(int fServerThread, int fExclusive = FALSE);
void moduleNotifyKeyspaceEvent(int type, const char *event, robj *key, int dbid);
void moduleCallCommandFilters(client *c);
int moduleHasCommandFilters();
void ModuleForkDoneHandler(int exitcode, int bysignal);
int TerminateModuleForkChild(int child_pid, int wait);
ssize_t rdbSaveModulesAux(rio *rdb, int when);
int moduleAllDatatypesHandleErrors();
sds modulesCollectInfo(sds info, const char *section, int for_crash_report, int sections);
void moduleFireServerEvent(uint64_t eid, int subid, void *data);
void processModuleLoadingProgressEvent(int is_aof);
int moduleTryServeClientBlockedOnKey(client *c, robj *key);
void moduleUnblockClient(client *c);
int moduleBlockedClientMayTimeout(client *c);
int moduleClientIsBlockedOnKeys(client *c);
void moduleNotifyUserChanged(client *c);
void moduleNotifyKeyUnlink(robj *key, robj *val);
robj *moduleTypeDupOrReply(client *c, robj *fromkey, robj *tokey, robj *value);
int moduleDefragValue(robj *key, robj *obj, long *defragged);
int moduleLateDefrag(robj *key, robj *value, unsigned long *cursor, long long endtime, long long *defragged);
long moduleDefragGlobals(void);

/* Utils */
long long ustime(void);
long long mstime(void);
extern "C" void getRandomHexChars(char *p, size_t len);
extern "C" void getRandomBytes(unsigned char *p, size_t len);
uint64_t crc64(uint64_t crc, const unsigned char *s, uint64_t l);
void exitFromChild(int retcode);
long long redisPopcount(const void *s, long count);
int redisSetProcTitle(const char *title);
int validateProcTitleTemplate(const char *_template);
int redisCommunicateSystemd(const char *sd_notify_msg);
void redisSetCpuAffinity(const char *cpulist);
void saveMasterStatusToStorage(bool fShutdown);

/* networking.c -- Networking and Client related operations */
client *createClient(connection *conn, int iel);
void closeTimedoutClients(void);
bool freeClient(client *c);
void freeClientAsync(client *c);
void resetClient(client *c);
void freeClientOriginalArgv(client *c);
void sendReplyToClient(connection *conn);
void *addReplyDeferredLen(client *c);
void setDeferredArrayLen(client *c, void *node, long length);
void setDeferredMapLen(client *c, void *node, long length);
void setDeferredSetLen(client *c, void *node, long length);
void setDeferredAttributeLen(client *c, void *node, long length);
void setDeferredPushLen(client *c, void *node, long length);
void processInputBuffer(client *c, bool fParse, int callFlags);
void acceptHandler(aeEventLoop *el, int fd, void *privdata, int mask);
void acceptTcpHandler(aeEventLoop *el, int fd, void *privdata, int mask);
void acceptTLSHandler(aeEventLoop *el, int fd, void *privdata, int mask);
void acceptUnixHandler(aeEventLoop *el, int fd, void *privdata, int mask);
void readQueryFromClient(connection *conn);
void addReplyNull(client *c);
void addReplyNullArray(client *c);
void addReplyBool(client *c, int b);
void addReplyVerbatim(client *c, const char *s, size_t len, const char *ext);
void addReplyProto(client *c, const char *s, size_t len);
void addReplyProtoCString(client *c, const char *s);
void addReplyBulk(client *c, robj_roptr obj);
void AddReplyFromClient(client *c, client *src);
void addReplyBulkCString(client *c, const char *s);
void addReplyBulkCBuffer(client *c, const void *p, size_t len);
void addReplyBulkLongLong(client *c, long long ll);
void addReply(client *c, robj_roptr obj);
void addReplySds(client *c, sds s);
void addReplyBulkSds(client *c, sds s);
void setDeferredReplyBulkSds(client *c, void *node, sds s);
void addReplyErrorObject(client *c, robj *err, int severity = 0);
void addReplyErrorSds(client *c, sds err);
void addReplyError(client *c, const char *err);
void addReplyStatus(client *c, const char *status);
void addReplyDouble(client *c, double d);
void addReplyBigNum(client *c, const char* num, size_t len);
void addReplyHumanLongDouble(client *c, long double d);
void addReplyLongLong(client *c, long long ll);
#ifdef __cplusplus
void addReplyLongLongWithPrefix(client *c, long long ll, char prefix);
#endif
void addReplyArrayLen(client *c, long length);
void addReplyMapLen(client *c, long length);
void addReplySetLen(client *c, long length);
void addReplyAttributeLen(client *c, long length);
void addReplyPushLen(client *c, long length);
void addReplyHelp(client *c, const char **help);
void addReplySubcommandSyntaxError(client *c);
void addReplyLoadedModules(client *c);
void copyClientOutputBuffer(client *dst, client *src);
size_t sdsZmallocSize(sds s);
size_t getStringObjectSdsUsedMemory(robj *o);
void freeClientReplyValue(const void *o);
void *dupClientReplyValue(void *o);
void getClientsMaxBuffers(unsigned long *longest_output_list,
                          unsigned long *biggest_input_buffer);
char *getClientPeerId(client *client);
char *getClientSockName(client *client);
sds catClientInfoString(sds s, client *client);
sds getAllClientsInfoString(int type);
void rewriteClientCommandVector(client *c, int argc, ...);
void rewriteClientCommandArgument(client *c, int i, robj *newval);
void replaceClientCommandVector(client *c, int argc, robj **argv);
void redactClientCommandArgument(client *c, int argc);
unsigned long getClientOutputBufferMemoryUsage(client *c);
int freeClientsInAsyncFreeQueue(int iel);
int closeClientOnOutputBufferLimitReached(client *c, int async);
int getClientType(client *c);
int getClientTypeByName(const char *name);
const char *getClientTypeName(int cclass);
void flushSlavesOutputBuffers(void);
void disconnectSlaves(void);
void disconnectSlavesExcept(unsigned char *uuid);
int listenToPort(int port, socketFds *fds, int fReusePort, int fFirstListen);
void pauseClients(mstime_t duration, pause_type type);
void unpauseClients(void);
int areClientsPaused(void);
int checkClientPauseTimeoutAndReturnIfPaused(void);
void processEventsWhileBlocked(int iel);
void loadingCron(void);
void whileBlockedCron();
void blockingOperationStarts();
void blockingOperationEnds();
int handleClientsWithPendingWrites(int iel, int aof_state);
int clientHasPendingReplies(client *c);
void unlinkClient(client *c);
int writeToClient(client *c, int handler_installed);
void linkClient(client *c);
void protectClient(client *c);
void unprotectClient(client *c);

void ProcessPendingAsyncWrites(void);
client *lookupClientByID(uint64_t id);
int authRequired(client *c);

#ifdef __GNUC__
void addReplyErrorFormat(client *c, const char *fmt, ...)
    __attribute__((format(printf, 2, 3)));
void addReplyStatusFormat(client *c, const char *fmt, ...)
    __attribute__((format(printf, 2, 3)));
#else
void addReplyErrorFormat(client *c, const char *fmt, ...);
void addReplyStatusFormat(client *c, const char *fmt, ...);
#endif

/* Client side caching (tracking mode) */
void enableTracking(client *c, uint64_t redirect_to, uint64_t options, robj **prefix, size_t numprefix);
void disableTracking(client *c);
void trackingRememberKeys(client *c);
void trackingInvalidateKey(client *c, robj *keyobj);
void trackingInvalidateKeysOnFlush(int async);
void freeTrackingRadixTree(rax *rt);
void freeTrackingRadixTreeAsync(rax *rt);
void trackingLimitUsedSlots(void);
uint64_t trackingGetTotalItems(void);
uint64_t trackingGetTotalKeys(void);
uint64_t trackingGetTotalPrefixes(void);
void trackingBroadcastInvalidationMessages(void);
int checkPrefixCollisionsOrReply(client *c, robj **prefix, size_t numprefix);

/* List data type */
void listTypeTryConversion(robj *subject, robj *value);
void listTypePush(robj *subject, robj *value, int where);
robj *listTypePop(robj *subject, int where);
unsigned long listTypeLength(robj_roptr subject);
listTypeIterator *listTypeInitIterator(robj_roptr subject, long index, unsigned char direction);
void listTypeReleaseIterator(listTypeIterator *li);
int listTypeNext(listTypeIterator *li, listTypeEntry *entry);
robj *listTypeGet(listTypeEntry *entry);
void listTypeInsert(listTypeEntry *entry, robj *value, int where);
int listTypeEqual(listTypeEntry *entry, robj *o);
void listTypeDelete(listTypeIterator *iter, listTypeEntry *entry);
void listTypeConvert(robj *subject, int enc);
robj *listTypeDup(robj *o);
void unblockClientWaitingData(client *c);
void popGenericCommand(client *c, int where);
void listElementsRemoved(client *c, robj *key, int where, robj *o, long count);

/* MULTI/EXEC/WATCH... */
void unwatchAllKeys(client *c);
void initClientMultiState(client *c);
void freeClientMultiState(client *c);
void queueMultiCommand(client *c);
void touchWatchedKey(redisDb *db, robj *key);
int isWatchedKeyExpired(client *c);
void touchAllWatchedKeysInDb(redisDb *emptied, redisDb *replaced_with);
void discardTransaction(client *c);
void flagTransaction(client *c);
void execCommandAbort(client *c, sds error);
void execCommandPropagateMulti(int dbid);
void execCommandPropagateExec(int dbid);
void beforePropagateMulti();
void afterPropagateExec();

/* Redis object implementation */
void decrRefCount(robj_roptr o);
void decrRefCountVoid(const void *o);
void incrRefCount(robj_roptr o);
robj *makeObjectShared(robj *o);
robj *makeObjectShared(const char *rgch, size_t cch);
robj *resetRefCount(robj *obj);
void freeStringObject(robj *o);
void freeListObject(robj *o);
void freeSetObject(robj *o);
void freeZsetObject(robj *o);
void freeHashObject(robj *o);
robj *createObject(int type, void *ptr);
robj *createStringObject(const char *ptr, size_t len);
robj *createRawStringObject(const char *ptr, size_t len);
robj *createEmbeddedStringObject(const char *ptr, size_t len);
robj *tryCreateRawStringObject(const char *ptr, size_t len);
robj *tryCreateStringObject(const char *ptr, size_t len);
robj *dupStringObject(const robj *o);
int isSdsRepresentableAsLongLong(const char *s, long long *llval);
int isObjectRepresentableAsLongLong(robj *o, long long *llongval);
robj *tryObjectEncoding(robj *o);
robj *getDecodedObject(robj *o);
robj_roptr getDecodedObject(robj_roptr o);
size_t stringObjectLen(robj_roptr o);
robj *createStringObjectFromLongLong(long long value);
robj *createStringObjectFromLongLongForValue(long long value);
robj *createStringObjectFromLongDouble(long double value, int humanfriendly);
robj *createQuicklistObject(void);
robj *createZiplistObject(void);
robj *createSetObject(void);
robj *createIntsetObject(void);
robj *createHashObject(void);
robj *createZsetObject(void);
robj *createZsetZiplistObject(void);
robj *createStreamObject(void);
robj *createModuleObject(moduleType *mt, void *value);
int getLongFromObjectOrReply(client *c, robj *o, long *target, const char *msg);
int getUnsignedLongLongFromObjectOrReply(client *c, robj *o, uint64_t *target, const char *msg);
int getPositiveLongFromObjectOrReply(client *c, robj *o, long *target, const char *msg);
int getRangeLongFromObjectOrReply(client *c, robj *o, long min, long max, long *target, const char *msg);
int checkType(client *c, robj_roptr o, int type);
int getLongLongFromObjectOrReply(client *c, robj *o, long long *target, const char *msg);
int getDoubleFromObjectOrReply(client *c, robj *o, double *target, const char *msg);
int getDoubleFromObject(const robj *o, double *target);
int getLongLongFromObject(robj *o, long long *target);
int getUnsignedLongLongFromObject(robj *o, uint64_t *target);
int getLongDoubleFromObject(robj *o, long double *target);
int getLongDoubleFromObjectOrReply(client *c, robj *o, long double *target, const char *msg);
int getIntFromObjectOrReply(client *c, robj *o, int *target, const char *msg);
const char *strEncoding(int encoding);
int compareStringObjects(robj *a, robj *b);
int collateStringObjects(robj *a, robj *b);
int equalStringObjects(robj *a, robj *b);
unsigned long long estimateObjectIdleTime(robj_roptr o);
void trimStringObjectIfNeeded(robj *o);

robj *deserializeStoredObject(const redisDbPersistentData *db, const char *key, const void *data, size_t cb);
std::unique_ptr<expireEntry> deserializeExpire(sds key, const char *str, size_t cch, size_t *poffset);
sds serializeStoredObject(robj_roptr o, sds sdsPrefix = nullptr);

#define sdsEncodedObject(objptr) (objptr->encoding == OBJ_ENCODING_RAW || objptr->encoding == OBJ_ENCODING_EMBSTR)

/* Synchronous I/O with timeout */
ssize_t syncWrite(int fd, const char *ptr, ssize_t size, long long timeout);
ssize_t syncRead(int fd, char *ptr, ssize_t size, long long timeout);
ssize_t syncReadLine(int fd, char *ptr, ssize_t size, long long timeout);

/* Replication */
void initMasterInfo(struct redisMaster *master);
void replicationFeedSlaves(list *slaves, int dictid, robj **argv, int argc);
void replicationFeedSlavesFromMasterStream(char *buf, size_t buflen);
void replicationFeedMonitors(client *c, list *monitors, int dictid, robj **argv, int argc);
void updateSlavesWaitingBgsave(int bgsaveerr, int type);
void replicationCron(void);
void replicationStartPendingFork(void);
void replicationHandleMasterDisconnection(struct redisMaster *mi);
void replicationCacheMaster(struct redisMaster *mi, client *c);
void replicationCreateCachedMasterClone(redisMaster *mi);
void resizeReplicationBacklog(long long newsize);
struct redisMaster *replicationAddMaster(char *ip, int port);
void replicationUnsetMaster(struct redisMaster *mi);
void refreshGoodSlavesCount(void);
void replicationScriptCacheInit(void);
void replicationScriptCacheFlush(void);
void replicationScriptCacheAdd(sds sha1);
int replicationScriptCacheExists(sds sha1);
void processClientsWaitingReplicas(void);
void unblockClientWaitingReplicas(client *c);
int replicationCountAcksByOffset(long long offset);
void replicationSendNewlineToMaster(struct redisMaster *mi);
long long replicationGetSlaveOffset(struct redisMaster *mi);
char *replicationGetSlaveName(client *c);
long long getPsyncInitialOffset(void);
int replicationSetupSlaveForFullResync(client *replica, long long offset);
void changeReplicationId(void);
void clearReplicationId2(void);
void chopReplicationBacklog(void);
void replicationCacheMasterUsingMyself(struct redisMaster *mi);
void replicationCacheMasterUsingMaster(struct redisMaster *mi);
void feedReplicationBacklog(const void *ptr, size_t len);
void updateMasterAuth();
void showLatestBacklog();
void rdbPipeReadHandler(struct aeEventLoop *eventLoop, int fd, void *clientData, int mask);
void rdbPipeWriteHandlerConnRemoved(struct connection *conn);
void replicationNotifyLoadedKey(redisDb *db, robj_roptr key, robj_roptr val, long long expire);
void replicateSubkeyExpire(redisDb *db, robj_roptr key, robj_roptr subkey, long long expire);
void clearFailoverState(void);
void updateFailoverStatus(void);
void abortFailover(redisMaster *mi, const char *err);
const char *getFailoverStateString();
int canFeedReplicaReplBuffer(client *replica);
void trimReplicationBacklog();

/* Generic persistence functions */
void startLoadingFile(FILE* fp, const char * filename, int rdbflags);
void startLoading(size_t size, int rdbflags);
void loadingProgress(off_t pos);
void stopLoading(int success);
void startSaving(int rdbflags);
void stopSaving(int success);
int allPersistenceDisabled(void);

#define DISK_ERROR_TYPE_AOF 1       /* Don't accept writes: AOF errors. */
#define DISK_ERROR_TYPE_RDB 2       /* Don't accept writes: RDB errors. */
#define DISK_ERROR_TYPE_NONE 0      /* No problems, we can accept writes. */
int writeCommandsDeniedByDiskError(void);

/* RDB persistence */
#include "rdb.h"
void killRDBChild(bool fSynchronous = false);
int bg_unlink(const char *filename);


/* AOF persistence */
void flushAppendOnlyFile(int force);
void feedAppendOnlyFile(struct redisCommand *cmd, int dictid, robj **argv, int argc);
void aofRemoveTempFile(pid_t childpid);
int rewriteAppendOnlyFileBackground(void);
int loadAppendOnlyFile(char *filename);
void stopAppendOnly(void);
int startAppendOnly(void);
void backgroundRewriteDoneHandler(int exitcode, int bysignal);
void aofRewriteBufferReset(void);
unsigned long aofRewriteBufferSize(void);
ssize_t aofReadDiffFromParent(void);
void killAppendOnlyChild(void);
void restartAOFAfterSYNC();

/* Child info */
void openChildInfoPipe(void);
void closeChildInfoPipe(void);
void sendChildInfoGeneric(childInfoType info_type, size_t keys, double progress, const char *pname);
void sendChildCowInfo(childInfoType info_type, const char *pname);
void sendChildInfo(childInfoType info_type, size_t keys, const char *pname);
void receiveChildInfo(void);

/* Fork helpers */
void executeWithoutGlobalLock(std::function<void()> func);
int redisFork(int type);
int hasActiveChildProcess();
int hasActiveChildProcessOrBGSave();
void resetChildState();
int isMutuallyExclusiveChildType(int type);

/* acl.c -- Authentication related prototypes. */
extern rax *Users;
extern user *DefaultUser;
void ACLInit(void);
/* Return values for ACLCheckAllPerm(). */
#define ACL_OK 0
#define ACL_DENIED_CMD 1
#define ACL_DENIED_KEY 2
#define ACL_DENIED_AUTH 3 /* Only used for ACL LOG entries. */
#define ACL_DENIED_CHANNEL 4 /* Only used for pub/sub commands */
int ACLCheckUserCredentials(robj *username, robj *password);
int ACLAuthenticateUser(client *c, robj *username, robj *password);
unsigned long ACLGetCommandID(const char *cmdname);
void ACLClearCommandID(void);
user *ACLGetUserByName(const char *name, size_t namelen);
int ACLCheckAllPerm(client *c, int *idxptr);
int ACLSetUser(user *u, const char *op, ssize_t oplen);
sds ACLDefaultUserFirstPassword(void);
uint64_t ACLGetCommandCategoryFlagByName(const char *name);
int ACLAppendUserForLoading(sds *argv, int argc, int *argc_err);
const char *ACLSetUserStringError(void);
int ACLLoadConfiguredUsers(void);
sds ACLDescribeUser(user *u);
void ACLLoadUsersAtStartup(void);
void addReplyCommandCategories(client *c, struct redisCommand *cmd);
user *ACLCreateUnlinkedUser();
void ACLFreeUserAndKillClients(user *u);
void addACLLogEntry(client *c, int reason, int keypos, sds username);
void ACLUpdateDefaultUserPassword(sds password);

/* Sorted sets data type */

/* Input flags. */
#define ZADD_IN_NONE 0
#define ZADD_IN_INCR (1<<0)    /* Increment the score instead of setting it. */
#define ZADD_IN_NX (1<<1)      /* Don't touch elements not already existing. */
#define ZADD_IN_XX (1<<2)      /* Only touch elements already existing. */
#define ZADD_IN_GT (1<<3)      /* Only update existing when new scores are higher. */
#define ZADD_IN_LT (1<<4)      /* Only update existing when new scores are lower. */

/* Output flags. */
#define ZADD_OUT_NOP (1<<0)     /* Operation not performed because of conditionals.*/
#define ZADD_OUT_NAN (1<<1)     /* Only touch elements already existing. */
#define ZADD_OUT_ADDED (1<<2)   /* The element was new and was added. */
#define ZADD_OUT_UPDATED (1<<3) /* The element already existed, score updated. */

/* Struct to hold an inclusive/exclusive range spec by score comparison. */
typedef struct {
    double min, max;
    int minex, maxex; /* are min or max exclusive? */
} zrangespec;

/* Struct to hold an inclusive/exclusive range spec by lexicographic comparison. */
typedef struct {
    sds min, max;     /* May be set to shared.(minstring|maxstring) */
    int minex, maxex; /* are min or max exclusive? */
} zlexrangespec;

zskiplist *zslCreate(void);
void zslFree(zskiplist *zsl);
zskiplistNode *zslInsert(zskiplist *zsl, double score, sds ele);
unsigned char *zzlInsert(unsigned char *zl, sds ele, double score);
int zslDelete(zskiplist *zsl, double score, sds ele, zskiplistNode **node);
zskiplistNode *zslFirstInRange(zskiplist *zsl, zrangespec *range);
zskiplistNode *zslLastInRange(zskiplist *zsl, zrangespec *range);
double zzlGetScore(unsigned char *sptr);
void zzlNext(unsigned char *zl, unsigned char **eptr, unsigned char **sptr);
void zzlPrev(unsigned char *zl, unsigned char **eptr, unsigned char **sptr);
unsigned char *zzlFirstInRange(unsigned char *zl, zrangespec *range);
unsigned char *zzlLastInRange(unsigned char *zl, zrangespec *range);
unsigned long zsetLength(robj_roptr zobj);
void zsetConvert(robj *zobj, int encoding);
void zsetConvertToZiplistIfNeeded(robj *zobj, size_t maxelelen, size_t totelelen);
int zsetScore(robj_roptr zobj, sds member, double *score);
unsigned long zslGetRank(zskiplist *zsl, double score, sds o);
int zsetAdd(robj *zobj, double score, sds ele, int in_flags, int *out_flags, double *newscore);
long zsetRank(robj_roptr zobj, sds ele, int reverse);
int zsetDel(robj *zobj, sds ele);
robj *zsetDup(robj *o);
int zsetZiplistValidateIntegrity(unsigned char *zl, size_t size, int deep);
void genericZpopCommand(client *c, robj **keyv, int keyc, int where, int emitkey, robj *countarg);
sds ziplistGetObject(unsigned char *sptr);
int zslValueGteMin(double value, zrangespec *spec);
int zslValueLteMax(double value, zrangespec *spec);
void zslFreeLexRange(zlexrangespec *spec);
int zslParseLexRange(robj *min, robj *max, zlexrangespec *spec);
unsigned char *zzlFirstInLexRange(unsigned char *zl, zlexrangespec *range);
unsigned char *zzlLastInLexRange(unsigned char *zl, zlexrangespec *range);
zskiplistNode *zslFirstInLexRange(zskiplist *zsl, zlexrangespec *range);
zskiplistNode *zslLastInLexRange(zskiplist *zsl, zlexrangespec *range);
int zzlLexValueGteMin(unsigned char *p, zlexrangespec *spec);
int zzlLexValueLteMax(unsigned char *p, zlexrangespec *spec);
int zslLexValueGteMin(sds value, zlexrangespec *spec);
int zslLexValueLteMax(sds value, zlexrangespec *spec);

/* Core functions */
int getMaxmemoryState(size_t *total, size_t *logical, size_t *tofree, float *level, EvictReason *reason=nullptr, bool fQuickCycle=false, bool fPreSnapshot=false);
size_t freeMemoryGetNotCountedMemory();
int overMaxmemoryAfterAlloc(size_t moremem);
int processCommand(client *c, int callFlags);
int processPendingCommandsAndResetClient(client *c, int flags);
void setupSignalHandlers(void);
void removeSignalHandlers(void);
int createSocketAcceptHandler(socketFds *sfd, aeFileProc *accept_handler);
int changeListenPort(int port, socketFds *sfd, aeFileProc *accept_handler, bool fFirstCall);
int changeBindAddr(sds *addrlist, int addrlist_len, bool fFirstCall);
struct redisCommand *lookupCommand(sds name);
struct redisCommand *lookupCommandByCString(const char *s);
struct redisCommand *lookupCommandOrOriginal(sds name);
void call(client *c, int flags);
void propagate(struct redisCommand *cmd, int dbid, robj **argv, int argc, int flags);
void alsoPropagate(struct redisCommand *cmd, int dbid, robj **argv, int argc, int target);
void redisOpArrayInit(redisOpArray *oa);
void redisOpArrayFree(redisOpArray *oa);
void forceCommandPropagation(client *c, int flags);
void preventCommandPropagation(client *c);
void preventCommandAOF(client *c);
void preventCommandReplication(client *c);
void slowlogPushCurrentCommand(client *c, struct redisCommand *cmd, ustime_t duration);
int prepareForShutdown(int flags);
#ifdef __GNUC__
void _serverLog(int level, const char *fmt, ...)
    __attribute__((format(printf, 2, 3)));
#else
void _serverLog(int level, const char *fmt, ...);
#endif
void serverLogRaw(int level, const char *msg);
void serverLogFromHandler(int level, const char *msg);
void usage(void);
void updateDictResizePolicy(void);
int htNeedsResize(dict *dict);
void populateCommandTable(void);
void resetCommandTableStats(void);
void resetErrorTableStats(void);
void adjustOpenFilesLimit(void);
void incrementErrorCount(const char *fullerr, size_t namelen);
void closeListeningSockets(int unlink_unix_socket);
void updateCachedTime(void);
void resetServerStats(void);
void activeDefragCycle(void);
unsigned int getLRUClock(void);
unsigned int LRU_CLOCK(void);
const char *evictPolicyToString(void);
struct redisMemOverhead *getMemoryOverheadData(void);
void freeMemoryOverheadData(struct redisMemOverhead *mh);
void checkChildrenDone(void);
int setOOMScoreAdj(int process_class);
void rejectCommandFormat(client *c, const char *fmt, ...);
extern "C" void *activeDefragAlloc(void *ptr);
robj *activeDefragStringOb(robj* ob, long *defragged);

#define RESTART_SERVER_NONE 0
#define RESTART_SERVER_GRACEFULLY (1<<0)     /* Do proper shutdown. */
#define RESTART_SERVER_CONFIG_REWRITE (1<<1) /* CONFIG REWRITE before restart.*/
int restartServer(int flags, mstime_t delay);

/* Set data type */
robj *setTypeCreate(const char *value);
int setTypeAdd(robj *subject, const char *value);
int setTypeRemove(robj *subject, const char *value);
int setTypeIsMember(robj_roptr subject, const char *value);
setTypeIterator *setTypeInitIterator(robj_roptr subject);
void setTypeReleaseIterator(setTypeIterator *si);
int setTypeNext(setTypeIterator *si, const char **sdsele, int64_t *llele);
sds setTypeNextObject(setTypeIterator *si);
int setTypeRandomElement(robj *setobj, sds *sdsele, int64_t *llele);
unsigned long setTypeRandomElements(robj *set, unsigned long count, robj *aux_set);
unsigned long setTypeSize(robj_roptr subject);
void setTypeConvert(robj *subject, int enc);
robj *setTypeDup(robj *o);

/* Hash data type */
#define HASH_SET_TAKE_FIELD (1<<0)
#define HASH_SET_TAKE_VALUE (1<<1)
#define HASH_SET_COPY 0

void hashTypeConvert(robj *o, int enc);
void hashTypeTryConversion(robj *subject, robj **argv, int start, int end);
int hashTypeExists(robj_roptr o, const char *key);
int hashTypeDelete(robj *o, sds key);
unsigned long hashTypeLength(robj_roptr o);
hashTypeIterator *hashTypeInitIterator(robj_roptr subject);
void hashTypeReleaseIterator(hashTypeIterator *hi);
int hashTypeNext(hashTypeIterator *hi);
void hashTypeCurrentFromZiplist(hashTypeIterator *hi, int what,
                                unsigned char **vstr,
                                unsigned int *vlen,
                                long long *vll);
sds hashTypeCurrentFromHashTable(hashTypeIterator *hi, int what);
void hashTypeCurrentObject(hashTypeIterator *hi, int what, unsigned char **vstr, unsigned int *vlen, long long *vll);
sds hashTypeCurrentObjectNewSds(hashTypeIterator *hi, int what);
robj *hashTypeLookupWriteOrCreate(client *c, robj *key);
robj *hashTypeGetValueObject(robj_roptr o, sds field);
int hashTypeSet(robj *o, sds field, sds value, int flags);
robj *hashTypeDup(robj *o);
int hashZiplistValidateIntegrity(unsigned char *zl, size_t size, int deep);

/* Pub / Sub */
int pubsubUnsubscribeAllChannels(client *c, int notify);
int pubsubUnsubscribeAllPatterns(client *c, int notify);
int pubsubPublishMessage(robj *channel, robj *message);
void addReplyPubsubMessage(client *c, robj *channel, robj *msg);

/* Keyspace events notification */
void notifyKeyspaceEvent(int type, const char *event, robj *key, int dbid);
int keyspaceEventsStringToFlags(char *classes);
sds keyspaceEventsFlagsToString(int flags);

/* Configuration */
void loadServerConfig(char *filename, char config_from_stdin, char *options);
void appendServerSaveParams(time_t seconds, int changes);
void resetServerSaveParams(void);
struct rewriteConfigState; /* Forward declaration to export API. */
void rewriteConfigRewriteLine(struct rewriteConfigState *state, const char *option, sds line, int force);
void rewriteConfigMarkAsProcessed(struct rewriteConfigState *state, const char *option);
int rewriteConfig(char *path, int force_all);
void initConfigValues();

/* db.c -- Keyspace access API */
class AeLocker;
int removeExpire(redisDb *db, robj *key);
int removeSubkeyExpire(redisDb *db, robj *key, robj *subkey);
void propagateExpire(redisDb *db, robj *key, int lazy);
void propagateSubkeyExpire(redisDb *db, int type, robj *key, robj *subkey);
void deleteExpiredKeyAndPropagate(redisDb *db, robj *keyobj);
int keyIsExpired(const redisDbPersistentDataSnapshot *db, robj *key);
int expireIfNeeded(redisDb *db, robj *key);
void setExpire(client *c, redisDb *db, robj *key, robj *subkey, long long when);
void setExpire(client *c, redisDb *db, robj *key, expireEntry &&entry);
robj_roptr lookupKeyRead(redisDb *db, robj *key, uint64_t mvccCheckpoint, AeLocker &locker);
robj_roptr lookupKeyRead(redisDb *db, robj *key);
int checkAlreadyExpired(long long when);
robj *lookupKeyWrite(redisDb *db, robj *key);
robj_roptr lookupKeyReadOrReply(client *c, robj *key, robj *reply, AeLocker &locker);
robj_roptr lookupKeyReadOrReply(client *c, robj *key, robj *reply);
robj *lookupKeyWriteOrReply(client *c, robj *key, robj *reply);
robj_roptr lookupKeyReadWithFlags(redisDb *db, robj *key, int flags);
robj *lookupKeyWriteWithFlags(redisDb *db, robj *key, int flags);
robj_roptr objectCommandLookup(client *c, robj *key);
robj_roptr objectCommandLookupOrReply(client *c, robj *key, robj *reply);
void SentReplyOnKeyMiss(client *c, robj *reply);
int objectSetLRUOrLFU(robj *val, long long lfu_freq, long long lru_idle,
                       long long lru_clock, int lru_multiplier);
#define LOOKUP_NONE 0
#define LOOKUP_NOTOUCH (1<<0)
#define LOOKUP_NONOTIFY (1<<1)
#define LOOKUP_UPDATEMVCC (1<<2)
void dbAdd(redisDb *db, robj *key, robj *val);
void dbOverwrite(redisDb *db, robj *key, robj *val, bool fRemoveExpire = false, dict_iter *pitrExisting = nullptr);
int dbMerge(redisDb *db, sds key, robj *val, int fReplace);
void genericSetKey(client *c, redisDb *db, robj *key, robj *val, int keepttl, int signal);
void setKey(client *c, redisDb *db, robj *key, robj *val);
robj *dbRandomKey(redisDb *db);
int dbSyncDelete(redisDb *db, robj *key);
int dbDelete(redisDb *db, robj *key);
robj *dbUnshareStringValue(redisDb *db, robj *key, robj *o);
int dbnumFromDb(redisDb *db);

#define EMPTYDB_NO_FLAGS 0      /* No flags. */
#define EMPTYDB_ASYNC (1<<0)    /* Reclaim memory in another thread. */
long long emptyDb(int dbnum, int flags, void(callback)(void*));
long long emptyDbStructure(redisDb **dbarray, int dbnum, int flags, void(callback)(void*));
void flushAllDataAndResetRDB(int flags);
long long dbTotalServerKeyCount();
const dbBackup *backupDb(void);
void restoreDbBackup(const dbBackup *buckup);
void discardDbBackup(const dbBackup *buckup, int flags, void(callback)(void*));


int selectDb(client *c, int id);
void signalModifiedKey(client *c, redisDb *db, robj *key);
void signalFlushedDb(int dbid, int async);
unsigned int getKeysInSlot(unsigned int hashslot, robj **keys, unsigned int count);
unsigned int countKeysInSlot(unsigned int hashslot);
unsigned int delKeysInSlot(unsigned int hashslot);
int verifyClusterConfigWithData(void);
void scanGenericCommand(client *c, robj_roptr o, unsigned long cursor);
int parseScanCursorOrReply(client *c, robj *o, unsigned long *cursor);
void slotToKeyAdd(sds key);
void slotToKeyDel(sds key);
int dbAsyncDelete(redisDb *db, robj *key);
void slotToKeyFlush(int async);
size_t lazyfreeGetPendingObjectsCount(void);
size_t lazyfreeGetFreedObjectsCount(void);
void freeObjAsync(robj *key, robj *obj);
void freeSlotsToKeysMapAsync(rax *rt);
void freeSlotsToKeysMap(rax *rt, int async);


/* API to get key arguments from commands */
int *getKeysPrepareResult(getKeysResult *result, int numkeys);
int getKeysFromCommand(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result);
void getKeysFreeResult(getKeysResult *result);
int zunionInterDiffGetKeys(struct redisCommand *cmd,robj **argv, int argc, getKeysResult *result);
int zunionInterDiffStoreGetKeys(struct redisCommand *cmd,robj **argv, int argc, getKeysResult *result);
int evalGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result);
int sortGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result);
int migrateGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result);
int georadiusGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result);
int xreadGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result);
int memoryGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result);
int lcsGetKeys(struct redisCommand *cmd, robj **argv, int argc, getKeysResult *result);

/* Cluster */
void clusterInit(void);
extern "C" unsigned short crc16(const char *buf, int len);
unsigned int keyHashSlot(const char *key, int keylen);
void clusterCron(void);
void clusterPropagatePublish(robj *channel, robj *message);
void migrateCloseTimedoutSockets(void);
void clusterBeforeSleep(void);
int clusterSendModuleMessageToTarget(const char *target, uint64_t module_id, uint8_t type, unsigned char *payload, uint32_t len);
void createDumpPayload(rio *payload, robj_roptr o, robj *key);

/* Sentinel */
void initSentinelConfig(void);
void initSentinel(void);
void sentinelTimer(void);
const char *sentinelHandleConfiguration(char **argv, int argc);
void queueSentinelConfig(sds *argv, int argc, int linenum, sds line);
void loadSentinelConfigFromQueue(void);
void sentinelIsRunning(void);
void sentinelCheckConfigFile(void);

/* keydb-check-rdb & aof */
int redis_check_rdb(const char *rdbfilename, FILE *fp);
int redis_check_rdb_main(int argc, const char **argv, FILE *fp);
int redis_check_aof_main(int argc, char **argv);

/* Scripting */
void scriptingInit(int setup);
int ldbRemoveChild(pid_t pid);
void ldbKillForkedSessions(void);
int ldbPendingChildren(void);
sds luaCreateFunction(client *c, lua_State *lua, robj *body);
void freeLuaScriptsAsync(dict *lua_scripts);

/* Blocked clients */
void processUnblockedClients(int iel);
void blockClient(client *c, int btype);
void unblockClient(client *c);
void queueClientForReprocessing(client *c);
void replyToBlockedClientTimedOut(client *c);
int getTimeoutFromObjectOrReply(client *c, robj *object, mstime_t *timeout, int unit);
void disconnectAllBlockedClients(void);
void handleClientsBlockedOnKeys(void);
void signalKeyAsReady(redisDb *db, robj *key, int type);
void signalKeyAsReady(redisDb *db, sds key, int type);
void blockForKeys(client *c, int btype, robj **keys, int numkeys, mstime_t timeout, robj *target, struct listPos *listpos, streamID *ids);
void updateStatsOnUnblock(client *c, long blocked_us, long reply_us);

/* timeout.c -- Blocked clients timeout and connections timeout. */
void addClientToTimeoutTable(client *c);
void removeClientFromTimeoutTable(client *c);
void handleBlockedClientsTimeout(void);
int clientsCronHandleTimeout(client *c, mstime_t now_ms);

/* timeout.c -- Blocked clients timeout and connections timeout. */
void addClientToTimeoutTable(client *c);
void removeClientFromTimeoutTable(client *c);
void handleBlockedClientsTimeout(void);
int clientsCronHandleTimeout(client *c, mstime_t now_ms);

/* expire.c -- Handling of expired keys */
void activeExpireCycle(int type);
void expireSlaveKeys(void);
void rememberSlaveKeyWithExpire(redisDb *db, robj *key);
void flushSlaveKeysWithExpireList(void);
size_t getSlaveKeyWithExpireCount(void);

/* evict.c -- maxmemory handling and LRU eviction. */
void evictionPoolAlloc(void);
#define LFU_INIT_VAL 5
unsigned long LFUGetTimeInMinutes(void);
uint8_t LFULogIncr(uint8_t value);
unsigned long LFUDecrAndReturn(robj_roptr o);
#define EVICT_OK 0
#define EVICT_RUNNING 1
#define EVICT_FAIL 2
int performEvictions(bool fPreSnapshot);

/* meminfo.cpp -- get memory info from /proc/memoryinfo for linux distros */
size_t getMemAvailable();
size_t getMemTotal();

/* Keys hashing / comparison functions for dict.c hash tables. */
uint64_t dictSdsHash(const void *key);
int dictSdsKeyCompare(void *privdata, const void *key1, const void *key2);
void dictSdsDestructor(void *privdata, void *val);

/* Git SHA1 */
extern "C" char *redisGitSHA1(void);
extern "C" char *redisGitDirty(void);
extern "C" uint64_t redisBuildId(void);
extern "C" char *redisBuildIdString(void);

int parseUnitString(const char *sz);

/* Commands prototypes */
void authCommand(client *c);
void pingCommand(client *c);
void echoCommand(client *c);
void commandCommand(client *c);
void setCommand(client *c);
void setnxCommand(client *c);
void setexCommand(client *c);
void psetexCommand(client *c);
void getCommand(client *c);
void getexCommand(client *c);
void getdelCommand(client *c);
void delCommand(client *c);
void unlinkCommand(client *c);
void existsCommand(client *c);
void mexistsCommand(client *c);
void setbitCommand(client *c);
void getbitCommand(client *c);
void bitfieldCommand(client *c);
void bitfieldroCommand(client *c);
void setrangeCommand(client *c);
void getrangeCommand(client *c);
void incrCommand(client *c);
void decrCommand(client *c);
void incrbyCommand(client *c);
void decrbyCommand(client *c);
void incrbyfloatCommand(client *c);
void selectCommand(client *c);
void swapdbCommand(client *c);
void randomkeyCommand(client *c);
void keysCommand(client *c);
void scanCommand(client *c);
void dbsizeCommand(client *c);
void lastsaveCommand(client *c);
void saveCommand(client *c);
void bgsaveCommand(client *c);
void bgrewriteaofCommand(client *c);
void shutdownCommand(client *c);
void moveCommand(client *c);
void copyCommand(client *c);
void renameCommand(client *c);
void renamenxCommand(client *c);
void lpushCommand(client *c);
void rpushCommand(client *c);
void lpushxCommand(client *c);
void rpushxCommand(client *c);
void linsertCommand(client *c);
void lpopCommand(client *c);
void rpopCommand(client *c);
void llenCommand(client *c);
void lindexCommand(client *c);
void lrangeCommand(client *c);
void ltrimCommand(client *c);
void typeCommand(client *c);
void lsetCommand(client *c);
void saddCommand(client *c);
void sremCommand(client *c);
void smoveCommand(client *c);
void sismemberCommand(client *c);
void smismemberCommand(client *c);
void scardCommand(client *c);
void spopCommand(client *c);
void srandmemberCommand(client *c);
void sinterCommand(client *c);
void sinterstoreCommand(client *c);
void sunionCommand(client *c);
void sunionstoreCommand(client *c);
void sdiffCommand(client *c);
void sdiffstoreCommand(client *c);
void sscanCommand(client *c);
void syncCommand(client *c);
void flushdbCommand(client *c);
void flushallCommand(client *c);
void sortCommand(client *c);
void lremCommand(client *c);
void lposCommand(client *c);
void rpoplpushCommand(client *c);
void lmoveCommand(client *c);
void infoCommand(client *c);
void mgetCommand(client *c);
void monitorCommand(client *c);
void expireCommand(client *c);
void expireatCommand(client *c);
void expireMemberCommand(client *c);
void expireMemberAtCommand(client *c);
void pexpireMemberAtCommand(client *c);
void pexpireCommand(client *c);
void pexpireatCommand(client *c);
void getsetCommand(client *c);
void ttlCommand(client *c);
void touchCommand(client *c);
void pttlCommand(client *c);
void persistCommand(client *c);
void replicaofCommand(client *c);
void roleCommand(client *c);
void debugCommand(client *c);
void msetCommand(client *c);
void msetnxCommand(client *c);
void zaddCommand(client *c);
void zincrbyCommand(client *c);
void zrangeCommand(client *c);
void zrangebyscoreCommand(client *c);
void zrevrangebyscoreCommand(client *c);
void zrangebylexCommand(client *c);
void zrevrangebylexCommand(client *c);
void zcountCommand(client *c);
void zlexcountCommand(client *c);
void zrevrangeCommand(client *c);
void zcardCommand(client *c);
void zremCommand(client *c);
void zscoreCommand(client *c);
void zmscoreCommand(client *c);
void zremrangebyscoreCommand(client *c);
void zremrangebylexCommand(client *c);
void zpopminCommand(client *c);
void zpopmaxCommand(client *c);
void bzpopminCommand(client *c);
void bzpopmaxCommand(client *c);
void zrandmemberCommand(client *c);
void multiCommand(client *c);
void execCommand(client *c);
void discardCommand(client *c);
void blpopCommand(client *c);
void brpopCommand(client *c);
void brpoplpushCommand(client *c);
void blmoveCommand(client *c);
void appendCommand(client *c);
void strlenCommand(client *c);
void zrankCommand(client *c);
void zrevrankCommand(client *c);
void hsetCommand(client *c);
void hsetnxCommand(client *c);
void hgetCommand(client *c);
void hmsetCommand(client *c);
void hmgetCommand(client *c);
void hdelCommand(client *c);
void hlenCommand(client *c);
void hstrlenCommand(client *c);
void zremrangebyrankCommand(client *c);
void zunionstoreCommand(client *c);
void zinterstoreCommand(client *c);
void zdiffstoreCommand(client *c);
void zunionCommand(client *c);
void zinterCommand(client *c);
void zrangestoreCommand(client *c);
void zdiffCommand(client *c);
void zscanCommand(client *c);
void hkeysCommand(client *c);
void hvalsCommand(client *c);
void hgetallCommand(client *c);
void hexistsCommand(client *c);
void hscanCommand(client *c);
void hrandfieldCommand(client *c);
void configCommand(client *c);
void hincrbyCommand(client *c);
void hincrbyfloatCommand(client *c);
void subscribeCommand(client *c);
void unsubscribeCommand(client *c);
void psubscribeCommand(client *c);
void punsubscribeCommand(client *c);
void publishCommand(client *c);
void pubsubCommand(client *c);
void watchCommand(client *c);
void unwatchCommand(client *c);
void clusterCommand(client *c);
void restoreCommand(client *c);
void mvccrestoreCommand(client *c);
void migrateCommand(client *c);
void askingCommand(client *c);
void readonlyCommand(client *c);
void readwriteCommand(client *c);
void dumpCommand(client *c);
void objectCommand(client *c);
void memoryCommand(client *c);
void clientCommand(client *c);
void helloCommand(client *c);
void evalCommand(client *c);
void evalShaCommand(client *c);
void scriptCommand(client *c);
void timeCommand(client *c);
void bitopCommand(client *c);
void bitcountCommand(client *c);
void bitposCommand(client *c);
void replconfCommand(client *c);
void waitCommand(client *c);
void geoencodeCommand(client *c);
void geodecodeCommand(client *c);
void georadiusbymemberCommand(client *c);
void georadiusbymemberroCommand(client *c);
void georadiusCommand(client *c);
void georadiusroCommand(client *c);
void geoaddCommand(client *c);
void geohashCommand(client *c);
void geoposCommand(client *c);
void geodistCommand(client *c);
void geosearchCommand(client *c);
void geosearchstoreCommand(client *c);
void pfselftestCommand(client *c);
void pfaddCommand(client *c);
void pfcountCommand(client *c);
void pfmergeCommand(client *c);
void pfdebugCommand(client *c);
void latencyCommand(client *c);
void moduleCommand(client *c);
void securityWarningCommand(client *c);
void xaddCommand(client *c);
void xrangeCommand(client *c);
void xrevrangeCommand(client *c);
void xlenCommand(client *c);
void xreadCommand(client *c);
void xgroupCommand(client *c);
void xsetidCommand(client *c);
void xackCommand(client *c);
void xpendingCommand(client *c);
void xclaimCommand(client *c);
void xautoclaimCommand(client *c);
void xinfoCommand(client *c);
void xdelCommand(client *c);
void xtrimCommand(client *c);
void aclCommand(client *c);
void replicaReplayCommand(client *c);
void hrenameCommand(client *c);
void stralgoCommand(client *c);
void resetCommand(client *c);
void failoverCommand(client *c);
void lfenceCommand(client *c);


int FBrokenLinkToMaster(int *pconnectMasters = nullptr);
int FActiveMaster(client *c);
struct redisMaster *MasterInfoFromClient(client *c);
bool FInReplicaReplay();
void updateActiveReplicaMastersFromRsi(rdbSaveInfo *rsi);

/* MVCC */
uint64_t getMvccTstamp();
void incrementMvccTstamp();

#if __GNUC__ >= 7 && !defined(NO_DEPRECATE_FREE)
 [[deprecated]]
void *calloc(size_t count, size_t size) noexcept;
 [[deprecated]]
void free(void *ptr) noexcept;
 [[deprecated]]
void *malloc(size_t size) noexcept;
 [[deprecated]]
void *realloc(void *ptr, size_t size) noexcept;
#endif

/* Debugging stuff */
void bugReportStart(void);
void serverLogObjectDebugInfo(robj_roptr o);
void sigsegvHandler(int sig, siginfo_t *info, void *secret);
const char *getSafeInfoString(const char *s, size_t len, char **tmp);
sds genRedisInfoString(const char *section);
sds genModulesInfoString(sds info);
void enableWatchdog(int period);
void disableWatchdog(void);
void watchdogScheduleSignal(int period);
void serverLogHexDump(int level, const char *descr, void *value, size_t len);
extern "C" int memtest_preserving_test(unsigned long *m, size_t bytes, int passes);
void mixDigest(unsigned char *digest, const void *ptr, size_t len);
void xorDigest(unsigned char *digest, const void *ptr, size_t len);
int populateCommandTableParseFlags(struct redisCommand *c, const char *strflags);



int moduleGILAcquiredByModule(void);
extern int g_fInCrash;
static inline int GlobalLocksAcquired(void)  // Used in asserts to verify all global locks are correctly acquired for a server-thread to operate
{
    return aeThreadOwnsLock() || moduleGILAcquiredByModule() || g_fInCrash;
}

inline int ielFromEventLoop(const aeEventLoop *eventLoop)
{
    int iel = 0;
    for (; iel < cserver.cthreads; ++iel)
    {
        if (g_pserver->rgthreadvar[iel].el == eventLoop)
            break;
    }
    serverAssert(iel < cserver.cthreads);
    return iel;
}

inline int FCorrectThread(client *c)
{
    return (c->conn == nullptr)
        || (c->iel == IDX_EVENT_LOOP_MAIN && moduleGILAcquiredByModule())
        || (serverTL != NULL && (g_pserver->rgthreadvar[c->iel].el == serverTL->el));
}
#define AssertCorrectThread(c) serverAssert(FCorrectThread(c))

void flushReplBacklogToClients();

template<typename FN_PTR, class ...TARGS>
void runAndPropogateToReplicas(FN_PTR *pfn, TARGS... args) {
    // Store the replication backlog starting params, we use this to know how much data was written.
    //  these are TLS in case we need to expand the buffer and therefore need to update them
    bool fNestedProcess = (g_pserver->repl_batch_idxStart >= 0);
    if (!fNestedProcess) {
        g_pserver->repl_batch_offStart = g_pserver->master_repl_offset;
        g_pserver->repl_batch_idxStart = g_pserver->repl_backlog_idx;
    }

    pfn(args...);

    if (!fNestedProcess) {
        flushReplBacklogToClients();
        g_pserver->repl_batch_offStart = -1;
        g_pserver->repl_batch_idxStart = -1;
    }
}

void debugDelay(int usec);
void killIOThreads(void);
void killThreads(void);
void makeThreadKillable(void);

/* Use macro for checking log level to avoid evaluating arguments in cases log
 * should be ignored due to low level. */
#define serverLog(level, ...) do {\
        if (((level)&0xff) < cserver.verbosity) break;\
        _serverLog(level, __VA_ARGS__);\
    } while(0)

/* TLS stuff */
void tlsInit(void);
void tlsInitThread();
void tlsCleanupThread();
void tlsCleanup(void);
int tlsConfigure(redisTLSContextConfig *ctx_config);
void tlsReload(void);


class ShutdownException
{};

#define redisDebug(fmt, ...) \
    printf("DEBUG %s:%d > " fmt "\n", __FILE__, __LINE__, __VA_ARGS__)
#define redisDebugMark() \
    printf("-- MARK %s:%d --\n", __FILE__, __LINE__)

int iAmMaster(void);

#endif


