/* Asynchronous replication implementation.
 *
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * Copyright (c) 2019 John Sully <john at eqalpha dot com>
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


#include "server.h"
#include "cluster.h"
#include "bio.h"
#include "aelocker.h"
#include "SnapshotPayloadParseState.h"

#include <sys/time.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <mutex>
#include <algorithm>
#include <uuid/uuid.h>
#include <chrono>
#include <unordered_map>
#include <string>
#include <sys/mman.h>

void replicationDiscardCachedMaster(redisMaster *mi);
void replicationResurrectCachedMaster(redisMaster *mi, connection *conn);
void replicationSendAck(redisMaster *mi);
void putSlaveOnline(client *replica);
int cancelReplicationHandshake(redisMaster *mi, int reconnect);
static void propagateMasterStaleKeys();
extern "C" pid_t gettid();

/* We take a global flag to remember if this instance generated an RDB
 * because of replication, so that we can remove the RDB file in case
 * the instance is configured to have no persistence. */
int RDBGeneratedByReplication = 0;

/* --------------------------- Utility functions ---------------------------- */

/* Return the pointer to a string representing the replica ip:listening_port
 * pair. Mostly useful for logging, since we want to log a replica using its
 * IP address and its listening port which is more clear for the user, for
 * example: "Closing connection with replica 10.1.2.3:6380". */
char *replicationGetSlaveName(client *c) {
    static char buf[NET_HOST_PORT_STR_LEN];
    char ip[NET_IP_STR_LEN];

    ip[0] = '\0';
    buf[0] = '\0';
    if (c->slave_addr ||
        connPeerToString(c->conn,ip,sizeof(ip),NULL) != -1)
    {
        char *addr = c->slave_addr ? c->slave_addr : ip;
        if (c->slave_listening_port)
            anetFormatAddr(buf,sizeof(buf),addr,c->slave_listening_port);
        else
            snprintf(buf,sizeof(buf),"%s:<unknown-replica-port>",addr);
    } else {
        snprintf(buf,sizeof(buf),"client id #%llu",
            (unsigned long long) c->id);
    }
    return buf;
}

static bool FSameUuidNoNil(const unsigned char *a, const unsigned char *b)
{
    unsigned char zeroCheck = 0;
    for (int i = 0; i < UUID_BINARY_LEN; ++i)
    {
        if (a[i] != b[i])
            return false;
        zeroCheck |= a[i];
    }
    return (zeroCheck != 0);    // if the UUID is nil then it is never equal
}

static bool FSameHost(client *clientA, client *clientB)
{
    if (clientA == nullptr || clientB == nullptr)
        return false;

    const unsigned char *a = clientA->uuid;
    const unsigned char *b = clientB->uuid;

    return FSameUuidNoNil(a, b);
}

static bool FMasterHost(client *c)
{
    listIter li;
    listNode *ln;
    listRewind(g_pserver->masters, &li);
    while ((ln = listNext(&li)))
    {
        redisMaster *mi = (redisMaster*)listNodeValue(ln);
        if (FSameUuidNoNil(mi->master_uuid, c->uuid))
            return true;
    }
    return false;
}

static bool FAnyDisconnectedMasters()
{
    listIter li;
    listNode *ln;
    listRewind(g_pserver->masters, &li);
    while ((ln = listNext(&li)))
    {
        redisMaster *mi = (redisMaster*)listNodeValue(ln);
        if (mi->repl_state != REPL_STATE_CONNECTED)
            return true;
    }
    return false;
}

client *replicaFromMaster(redisMaster *mi)
{
    if (mi->master == nullptr)
        return nullptr;

    listIter liReplica;
    listNode *lnReplica;
    listRewind(g_pserver->slaves, &liReplica);
    while ((lnReplica = listNext(&liReplica)) != nullptr)
    {
        client *replica = (client*)listNodeValue(lnReplica);
        if (FSameHost(mi->master, replica))
            return replica;
    }
    return nullptr;
}

/* Plain unlink() can block for quite some time in order to actually apply
 * the file deletion to the filesystem. This call removes the file in a
 * background thread instead. We actually just do close() in the thread,
 * by using the fact that if there is another instance of the same file open,
 * the foreground unlink() will only remove the fs name, and deleting the
 * file's storage space will only happen once the last reference is lost. */
int bg_unlink(const char *filename) {
    int fd = open(filename,O_RDONLY|O_NONBLOCK);
    if (fd == -1) {
        /* Can't open the file? Fall back to unlinking in the main thread. */
        return unlink(filename);
    } else {
        /* The following unlink() removes the name but doesn't free the
         * file contents because a process still has it open. */
        int retval = unlink(filename);
        if (retval == -1) {
            /* If we got an unlink error, we just return it, closing the
             * new reference we have to the file. */
            int old_errno = errno;
            close(fd);  /* This would overwrite our errno. So we saved it. */
            errno = old_errno;
            return -1;
        }
        bioCreateCloseJob(fd);
        return 0; /* Success. */
    }
}

/* ---------------------------------- MASTER -------------------------------- */

bool createDiskBacklog() {
    // Lets create some disk backed pages and add them here
    std::string path = "./repl-backlog-temp" + std::to_string(gettid());
#ifdef __APPLE__
    int fd = open(path.c_str(), O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
#else
    int fd = open(path.c_str(), O_CREAT | O_RDWR | O_LARGEFILE, S_IRUSR | S_IWUSR);
#endif
    if (fd < 0) {
        return false;
    }
    size_t alloc = cserver.repl_backlog_disk_size;
    int result = truncate(path.c_str(), alloc);
    unlink(path.c_str()); // ensure the fd is the only ref
    if (result == -1) {
        close (fd);
        return false;
    }

    g_pserver->repl_backlog_disk = (char*)mmap(nullptr, alloc, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);
    if (g_pserver->repl_backlog_disk == MAP_FAILED) {
        g_pserver->repl_backlog_disk = nullptr;
        return false;
    }

    serverLog(LL_VERBOSE, "Disk Backed Replication Allocated");
    return true;
}

void createReplicationBacklog(void) {
    serverAssert(g_pserver->repl_backlog == NULL);
    if (cserver.repl_backlog_disk_size) {
        if (!createDiskBacklog()) {
            serverLog(LL_WARNING, "Failed to create disk backlog, will use memory only");
        }
    }
    if (cserver.force_backlog_disk && g_pserver->repl_backlog_disk != nullptr) {
        g_pserver->repl_backlog = g_pserver->repl_backlog_disk;
        g_pserver->repl_backlog_size = cserver.repl_backlog_disk_size;
    } else {
        g_pserver->repl_backlog = (char*)zmalloc(g_pserver->repl_backlog_size, MALLOC_LOCAL);
    }
    g_pserver->repl_backlog_histlen = 0;
    g_pserver->repl_backlog_idx = 0;
    g_pserver->repl_backlog_start = g_pserver->master_repl_offset;

    /* We don't have any data inside our buffer, but virtually the first
     * byte we have is the next byte that will be generated for the
     * replication stream. */
    g_pserver->repl_backlog_off = g_pserver->master_repl_offset+1;

    /* Allow transmission to clients */
    g_pserver->repl_batch_idxStart = 0;
    g_pserver->repl_batch_offStart = g_pserver->master_repl_offset;
}

/* Compute the corresponding index from a replication backlog offset 
 * Since this computation needs the size of the replication backlog, 
 * you need to have the repl_backlog_lock in order to call it */
long long getReplIndexFromOffset(long long offset){
    serverAssert(g_pserver->repl_backlog_lock.fOwnLock());
    long long index = (offset - g_pserver->repl_backlog_start) % g_pserver->repl_backlog_size;
    return index;
}

/* This function is called when the user modifies the replication backlog
 * size at runtime. It is up to the function to both update the
 * g_pserver->repl_backlog_size and to resize the buffer and setup it so that
 * it contains the same data as the previous one (possibly less data, but
 * the most recent bytes, or the same data and more free space in case the
 * buffer is enlarged). */
void resizeReplicationBacklog(long long newsize) {
    if (newsize < CONFIG_REPL_BACKLOG_MIN_SIZE)
        newsize = CONFIG_REPL_BACKLOG_MIN_SIZE;
    if (g_pserver->repl_backlog_size == newsize) return;

    if (g_pserver->repl_backlog != NULL) {
        std::unique_lock<fastlock> repl_backlog_lock(g_pserver->repl_backlog_lock);
        /* What we actually do is to flush the old buffer and realloc a new
         * empty one. It will refill with new data incrementally.
         * The reason is that copying a few gigabytes adds latency and even
         * worse often we need to alloc additional space before freeing the
         * old buffer. */

        /* get the critical client size, i.e. the size of the data unflushed to clients */
        long long earliest_off = g_pserver->repl_lowest_off.load();

        if (earliest_off != -1) {
            char *backlog = nullptr;
            newsize = std::max(newsize, g_pserver->master_repl_offset - earliest_off);
            
            if (cserver.repl_backlog_disk_size != 0) {
                if (newsize > g_pserver->repl_backlog_config_size || cserver.force_backlog_disk) {
                    if (g_pserver->repl_backlog == g_pserver->repl_backlog_disk)
                        return; // Can't do anything more
                    serverLog(LL_NOTICE, "Switching to disk backed replication backlog due to exceeding memory limits");
                    backlog = g_pserver->repl_backlog_disk;
                    newsize = cserver.repl_backlog_disk_size;
                }
            }

            // We need to keep critical data so we can't shrink less than the hot data in the buffer
            if (backlog == nullptr)
                backlog = (char*)zmalloc(newsize);
            g_pserver->repl_backlog_histlen = g_pserver->master_repl_offset - earliest_off;
            long long earliest_idx = getReplIndexFromOffset(earliest_off);

            if (g_pserver->repl_backlog_idx >= earliest_idx) {
                auto cbActiveBacklog = g_pserver->repl_backlog_idx - earliest_idx;
                memcpy(backlog, g_pserver->repl_backlog + earliest_idx, cbActiveBacklog);
                serverAssert(g_pserver->repl_backlog_histlen == cbActiveBacklog);
            } else {
                auto cbPhase1 = g_pserver->repl_backlog_size - earliest_idx;
                memcpy(backlog, g_pserver->repl_backlog + earliest_idx, cbPhase1);
                memcpy(backlog + cbPhase1, g_pserver->repl_backlog, g_pserver->repl_backlog_idx);
                auto cbActiveBacklog = cbPhase1 + g_pserver->repl_backlog_idx;
                serverAssert(g_pserver->repl_backlog_histlen == cbActiveBacklog);
            }
            if (g_pserver->repl_backlog != g_pserver->repl_backlog_disk)
                zfree(g_pserver->repl_backlog);
            else
                serverLog(LL_NOTICE, "Returning to memory backed replication backlog");
            g_pserver->repl_backlog = backlog;
            g_pserver->repl_backlog_idx = g_pserver->repl_backlog_histlen;
            if (g_pserver->repl_batch_idxStart >= 0) {
                g_pserver->repl_batch_idxStart -= earliest_idx;
                if (g_pserver->repl_batch_idxStart < 0)
                    g_pserver->repl_batch_idxStart += g_pserver->repl_backlog_size;
            }
            g_pserver->repl_backlog_start = earliest_off;
        } else {
            if (g_pserver->repl_backlog != g_pserver->repl_backlog_disk)
                zfree(g_pserver->repl_backlog);
            else
                serverLog(LL_NOTICE, "Returning to memory backed replication backlog");
            g_pserver->repl_backlog = (char*)zmalloc(newsize);
            g_pserver->repl_backlog_histlen = 0;
            g_pserver->repl_backlog_idx = 0;
            /* Next byte we have is... the next since the buffer is empty. */
            g_pserver->repl_backlog_off = g_pserver->master_repl_offset+1;
            g_pserver->repl_backlog_start = g_pserver->master_repl_offset;
        }
    }
    g_pserver->repl_backlog_size = newsize;
}


void freeReplicationBacklog(void) {
    serverAssert(GlobalLocksAcquired());
    listIter li;
    listNode *ln;
    listRewind(g_pserver->slaves, &li);
    while ((ln = listNext(&li))) {
        // g_pserver->slaves should be empty, or filled with clients pending close
        client *c = (client*)listNodeValue(ln);
        serverAssert(c->flags & CLIENT_CLOSE_ASAP || FMasterHost(c));
    }
    zfree(g_pserver->repl_backlog);
    g_pserver->repl_backlog = NULL;
}

/* Add data to the replication backlog.
 * This function also increments the global replication offset stored at
 * g_pserver->master_repl_offset, because there is no case where we want to feed
 * the backlog without incrementing the offset. */
void feedReplicationBacklog(const void *ptr, size_t len) {
    serverAssert(GlobalLocksAcquired());
    const unsigned char *p = (const unsigned char*)ptr;
    std::unique_lock<fastlock> repl_backlog_lock(g_pserver->repl_backlog_lock, std::defer_lock);

    if (g_pserver->repl_batch_idxStart >= 0) {
        /* We are lower bounded by the lowest replica offset, or the batch offset start if not applicable */
        long long lower_bound = g_pserver->repl_lowest_off.load(std::memory_order_seq_cst);
        if (lower_bound == -1)
            lower_bound = g_pserver->repl_batch_offStart;
        long long minimumsize = g_pserver->master_repl_offset + len - lower_bound + 1;

        if (minimumsize > g_pserver->repl_backlog_size) {
            listIter li;
            listNode *ln;
            listRewind(g_pserver->slaves, &li);
            long long maxClientBuffer = (long long)cserver.client_obuf_limits[CLIENT_TYPE_SLAVE].hard_limit_bytes;
            if (maxClientBuffer <= 0)
                maxClientBuffer = LLONG_MAX;    // infinite essentially
            if (cserver.repl_backlog_disk_size)
                maxClientBuffer = std::max(g_pserver->repl_backlog_size, cserver.repl_backlog_disk_size);
            long long min_offset = LLONG_MAX;
            int listening_replicas = 0;
            while ((ln = listNext(&li))) {
                client *replica = (client*)listNodeValue(ln);
                if (!canFeedReplicaReplBuffer(replica)) continue;
                if (replica->flags & CLIENT_CLOSE_ASAP) continue;

                std::unique_lock<fastlock> ul(replica->lock);

                // Would this client overflow?  If so close it
                long long neededBuffer = g_pserver->master_repl_offset + len - replica->repl_curr_off + 1;
                if (neededBuffer > maxClientBuffer) {
                    
                    sds clientInfo = catClientInfoString(sdsempty(),replica);
                    freeClientAsync(replica);
                    serverLog(LL_WARNING,"Client %s scheduled to be closed ASAP due to exceeding output buffer hard limit.", clientInfo);
                    sdsfree(clientInfo);
                    continue;
                }
                min_offset = std::min(min_offset, replica->repl_curr_off);
                ++listening_replicas;
            }

            if (min_offset == LLONG_MAX) {
                min_offset = g_pserver->repl_batch_offStart;
                g_pserver->repl_lowest_off = -1;
            } else {
                g_pserver->repl_lowest_off = min_offset;
            }

            minimumsize = g_pserver->master_repl_offset + len - min_offset + 1;
            serverAssert(listening_replicas == 0 || minimumsize <= maxClientBuffer);

            if (minimumsize > g_pserver->repl_backlog_size && listening_replicas) {
                // This is an emergency overflow, we better resize to fit
                long long newsize = std::max(g_pserver->repl_backlog_size*2, minimumsize);
                serverLog(LL_WARNING, "Replication backlog is too small, resizing to: %lld bytes", newsize);
                resizeReplicationBacklog(newsize);
            } else if (!listening_replicas) {
                // We need to update a few variables or later asserts will notice we dropped data
                g_pserver->repl_batch_offStart = g_pserver->master_repl_offset + len;
                g_pserver->repl_lowest_off = -1;
                if (!repl_backlog_lock.owns_lock())
                    repl_backlog_lock.lock();   // we need to acquire the lock if we'll be overwriting data that writeToClient may be reading
            }
        }
    }

    g_pserver->master_repl_offset += len;

    /* This is a circular buffer, so write as much data we can at every
     * iteration and rewind the "idx" index if we reach the limit. */
    
    while(len) {
        size_t thislen = g_pserver->repl_backlog_size - g_pserver->repl_backlog_idx;
        if (thislen > len) thislen = len;
        memcpy(g_pserver->repl_backlog+g_pserver->repl_backlog_idx,p,thislen);
        g_pserver->repl_backlog_idx += thislen;
        if (g_pserver->repl_backlog_idx == g_pserver->repl_backlog_size)
            g_pserver->repl_backlog_idx = 0;
        len -= thislen;
        p += thislen;
        g_pserver->repl_backlog_histlen += thislen;
    }
    if (g_pserver->repl_backlog_histlen > g_pserver->repl_backlog_size)
        g_pserver->repl_backlog_histlen = g_pserver->repl_backlog_size;
    /* Set the offset of the first byte we have in the backlog. */
    g_pserver->repl_backlog_off = g_pserver->master_repl_offset -
                              g_pserver->repl_backlog_histlen + 1;
}

/* Wrapper for feedReplicationBacklog() that takes Redis string objects
 * as input. */
void feedReplicationBacklogWithObject(robj *o) {
    char llstr[LONG_STR_SIZE];
    void *p;
    size_t len;

    if (o->encoding == OBJ_ENCODING_INT) {
        len = ll2string(llstr,sizeof(llstr),(long)ptrFromObj(o));
        p = llstr;
    } else {
        len = sdslen((sds)ptrFromObj(o));
        p = ptrFromObj(o);
    }
    feedReplicationBacklog(p,len);
}

sds catCommandForAofAndActiveReplication(sds buf, struct redisCommand *cmd, robj **argv, int argc);

void replicationFeedSlave(client *replica, int dictid, robj **argv, int argc, bool fSendRaw)
{
    char llstr[LONG_STR_SIZE];
    std::unique_lock<decltype(replica->lock)> lock(replica->lock);

    /* Send SELECT command to every replica if needed. */
    if (g_pserver->replicaseldb != dictid) {
        robj *selectcmd;

        /* For a few DBs we have pre-computed SELECT command. */
        if (dictid >= 0 && dictid < PROTO_SHARED_SELECT_CMDS) {
            selectcmd = shared.select[dictid];
        } else {
            int dictid_len;

            dictid_len = ll2string(llstr,sizeof(llstr),dictid);
            selectcmd = createObject(OBJ_STRING,
                sdscatprintf(sdsempty(),
                "*2\r\n$6\r\nSELECT\r\n$%d\r\n%s\r\n",
                dictid_len, llstr));
        }

        /* Add the SELECT command into the backlog. */
        /* We don't do this for advanced replication because this will be done later when it adds the whole RREPLAY command */
        if (g_pserver->repl_backlog && fSendRaw) feedReplicationBacklogWithObject(selectcmd);

        /* Send it to slaves */
        addReply(replica,selectcmd);

        if (dictid < 0 || dictid >= PROTO_SHARED_SELECT_CMDS)
            decrRefCount(selectcmd);
    }
    g_pserver->replicaseldb = dictid;

    /* Feed slaves that are waiting for the initial SYNC (so these commands
     * are queued in the output buffer until the initial SYNC completes),
     * or are already in sync with the master. */

    if (fSendRaw)
    {
        /* Add the multi bulk length. */
        addReplyArrayLen(replica,argc);

        /* Finally any additional argument that was not stored inside the
            * static buffer if any (from j to argc). */
        for (int j = 0; j < argc; j++)
            addReplyBulk(replica,argv[j]);
    }
    else
    {
        struct redisCommand *cmd = lookupCommand(szFromObj(argv[0]));
        sds buf = catCommandForAofAndActiveReplication(sdsempty(), cmd, argv, argc);
        addReplyProto(replica, buf, sdslen(buf));
        sdsfree(buf);
    }
}

static int writeProtoNum(char *dst, const size_t cchdst, long long num)
{
    if (cchdst < 1)
        return 0;
    dst[0] = '$';
    int cch = 1;
    cch += ll2string(dst + cch, cchdst - cch, digits10(num));
    int chCpyT = std::min<int>(cchdst - cch, 2);
    memcpy(dst + cch, "\r\n", chCpyT);
    cch += chCpyT;
    cch += ll2string(dst + cch, cchdst-cch, num);
    chCpyT = std::min<int>(cchdst - cch, 3);
    memcpy(dst + cch, "\r\n", chCpyT);
    if (chCpyT == 3)
        cch += 2;
    else
        cch += chCpyT;
    return cch;
}

int canFeedReplicaReplBuffer(client *replica) {
    /* Don't feed replicas that only want the RDB. */
    if (replica->flags & CLIENT_REPL_RDBONLY) return 0;

    /* Don't feed replicas that are still waiting for BGSAVE to start. */
    if (replica->replstate == SLAVE_STATE_WAIT_BGSAVE_START) return 0;

    return 1;
}

/* Propagate write commands to slaves, and populate the replication backlog
 * as well. This function is used if the instance is a master: we use
 * the commands received by our clients in order to create the replication
 * stream. Instead if the instance is a replica and has sub-slaves attached,
 * we use replicationFeedSlavesFromMasterStream() */
void replicationFeedSlavesCore(list *slaves, int dictid, robj **argv, int argc) {
    int j;
    serverAssert(GlobalLocksAcquired());
    serverAssert(g_pserver->repl_batch_offStart >= 0);

    if (dictid < 0)
        dictid = 0; // this can happen if we send a PING before any real operation

    /* If the instance is not a top level master, return ASAP: we'll just proxy
     * the stream of data we receive from our master instead, in order to
     * propagate *identical* replication stream. In this way this replica can
     * advertise the same replication ID as the master (since it shares the
     * master replication history and has the same backlog and offsets). */
    if (!g_pserver->fActiveReplica && listLength(g_pserver->masters)) return;

    /* If there aren't slaves, and there is no backlog buffer to populate,
     * we can return ASAP. */
    if (g_pserver->repl_backlog == NULL && listLength(slaves) == 0) return;

    /* We can't have slaves attached and no backlog. */
    serverAssert(!(listLength(slaves) != 0 && g_pserver->repl_backlog == NULL));

    bool fSendRaw = !g_pserver->fActiveReplica;

    /* Send SELECT command to every replica if needed. */
    if (g_pserver->replicaseldb != dictid) {
        char llstr[LONG_STR_SIZE];
        robj *selectcmd;

        /* For a few DBs we have pre-computed SELECT command. */
        if (dictid >= 0 && dictid < PROTO_SHARED_SELECT_CMDS) {
            selectcmd = shared.select[dictid];
        } else {
            int dictid_len;

            dictid_len = ll2string(llstr,sizeof(llstr),dictid);
            selectcmd = createObject(OBJ_STRING,
                sdscatprintf(sdsempty(),
                "*2\r\n$6\r\nSELECT\r\n$%d\r\n%s\r\n",
                dictid_len, llstr));
        }

        /* Add the SELECT command into the backlog. */
        /* We don't do this for advanced replication because this will be done later when it adds the whole RREPLAY command */
        if (g_pserver->repl_backlog && fSendRaw) feedReplicationBacklogWithObject(selectcmd);

        if (dictid < 0 || dictid >= PROTO_SHARED_SELECT_CMDS)
            decrRefCount(selectcmd);
    }
    g_pserver->replicaseldb = dictid;

    /* Write the command to the replication backlog if any. */
    if (g_pserver->repl_backlog) 
    {
        if (fSendRaw)
        {
            char aux[LONG_STR_SIZE+3];
            /* Add the multi bulk reply length. */
            aux[0] = '*';
            int multilen = ll2string(aux+1,sizeof(aux)-1,argc);
            aux[multilen+1] = '\r';
            aux[multilen+2] = '\n';

            feedReplicationBacklog(aux,multilen+3);

            for (j = 0; j < argc; j++) {
                long objlen = stringObjectLen(argv[j]);

                /* We need to feed the buffer with the object as a bulk reply
                * not just as a plain string, so create the $..CRLF payload len
                * and add the final CRLF */
                aux[0] = '$';
                int len = ll2string(aux+1,sizeof(aux)-1,objlen);
                aux[len+1] = '\r';
                aux[len+2] = '\n';
                feedReplicationBacklog(aux,len+3);
                feedReplicationBacklogWithObject(argv[j]);
                feedReplicationBacklog(aux+len+1,2);
            }
        }
        else
        {
            char szDbNum[128];
            int cchDbNum = 0;
            if (!fSendRaw)
                cchDbNum = writeProtoNum(szDbNum, sizeof(szDbNum), dictid);
            

            char szMvcc[128];
            int cchMvcc = 0;
            incrementMvccTstamp();	// Always increment MVCC tstamp so we're consistent with active and normal replication
            if (!fSendRaw)
                cchMvcc = writeProtoNum(szMvcc, sizeof(szMvcc), getMvccTstamp());

            //size_t cchlen = multilen+3;
            struct redisCommand *cmd = lookupCommand(szFromObj(argv[0]));
            sds buf = catCommandForAofAndActiveReplication(sdsempty(), cmd, argv, argc);
            size_t cchlen = sdslen(buf);

            // The code below used to be: snprintf(proto, sizeof(proto), "*5\r\n$7\r\nRREPLAY\r\n$%d\r\n%s\r\n$%lld\r\n", (int)strlen(uuid), uuid, cchbuf);
            //  but that was much too slow
            static const char *protoRREPLAY = "*5\r\n$7\r\nRREPLAY\r\n$36\r\n00000000-0000-0000-0000-000000000000\r\n$";
            char proto[1024];
            int cchProto = 0;
            if (!fSendRaw)
            {
                char uuid[37];
                uuid_unparse(cserver.uuid, uuid);

                cchProto = strlen(protoRREPLAY);
                memcpy(proto, protoRREPLAY, strlen(protoRREPLAY));
                memcpy(proto + 22, uuid, 36); // Note UUID_STR_LEN includes the \0 trailing byte which we don't want
                cchProto += ll2string(proto + cchProto, sizeof(proto)-cchProto, cchlen);
                memcpy(proto + cchProto, "\r\n", 3);
                cchProto += 2;
            }


            feedReplicationBacklog(proto, cchProto);            
            feedReplicationBacklog(buf, sdslen(buf));

            const char *crlf = "\r\n";
            feedReplicationBacklog(crlf, 2);
            feedReplicationBacklog(szDbNum, cchDbNum);
            feedReplicationBacklog(szMvcc, cchMvcc);

            sdsfree(buf);
        }
    }
}

void replicationFeedSlaves(list *replicas, int dictid, robj **argv, int argc) {
    runAndPropogateToReplicas(replicationFeedSlavesCore, replicas, dictid, argv, argc);
}

/* This is a debugging function that gets called when we detect something
 * wrong with the replication protocol: the goal is to peek into the
 * replication backlog and show a few final bytes to make simpler to
 * guess what kind of bug it could be. */
void showLatestBacklog(void) {
    if (g_pserver->repl_backlog == NULL) return;

    long long dumplen = 256;
    if (g_pserver->repl_backlog_histlen < dumplen)
        dumplen = g_pserver->repl_backlog_histlen;

    /* Identify the first byte to dump. */
    long long idx =
      (g_pserver->repl_backlog_idx + (g_pserver->repl_backlog_size - dumplen)) %
       g_pserver->repl_backlog_size;

    /* Scan the circular buffer to collect 'dumplen' bytes. */
    sds dump = sdsempty();
    while(dumplen) {
        long long thislen =
            ((g_pserver->repl_backlog_size - idx) < dumplen) ?
            (g_pserver->repl_backlog_size - idx) : dumplen;

        dump = sdscatrepr(dump,g_pserver->repl_backlog+idx,thislen);
        dumplen -= thislen;
        idx = 0;
    }

    /* Finally log such bytes: this is vital debugging info to
     * understand what happened. */
    serverLog(LL_WARNING,"Latest backlog is: '%s'", dump);
    sdsfree(dump);
}

/* This function is used in order to proxy what we receive from our master
 * to our sub-slaves. */
#include <ctype.h>
void replicationFeedSlavesFromMasterStream(char *buf, size_t buflen) {
    /* Debugging: this is handy to see the stream sent from master
     * to slaves. Disabled with if(0). */
    if (0) {
        printf("%zu:",buflen);
        for (size_t j = 0; j < buflen; j++) {
            printf("%c", isprint(buf[j]) ? buf[j] : '.');
        }
        printf("\n");
    }

    if (g_pserver->repl_backlog) feedReplicationBacklog(buf,buflen);
}

void replicationFeedMonitors(client *c, list *monitors, int dictid, robj **argv, int argc) {
    if (!(listLength(g_pserver->monitors) && !g_pserver->loading)) return;
    listNode *ln;
    listIter li;
    int j;
    sds cmdrepr = sdsnew("+");
    robj *cmdobj;
    struct timeval tv;
    serverAssert(GlobalLocksAcquired());

    gettimeofday(&tv,NULL);
    cmdrepr = sdscatprintf(cmdrepr,"%ld.%06ld ",(long)tv.tv_sec,(long)tv.tv_usec);
    if (c->flags & CLIENT_LUA) {
        cmdrepr = sdscatprintf(cmdrepr,"[%d lua] ",dictid);
    } else if (c->flags & CLIENT_UNIX_SOCKET) {
        cmdrepr = sdscatprintf(cmdrepr,"[%d unix:%s] ",dictid,g_pserver->unixsocket);
    } else {
        cmdrepr = sdscatprintf(cmdrepr,"[%d %s] ",dictid,getClientPeerId(c));
    }

    for (j = 0; j < argc; j++) {
        if (argv[j]->encoding == OBJ_ENCODING_INT) {
            cmdrepr = sdscatprintf(cmdrepr, "\"%ld\"", (long)ptrFromObj(argv[j]));
        } else {
            cmdrepr = sdscatrepr(cmdrepr,(char*)ptrFromObj(argv[j]),
                        sdslen((sds)ptrFromObj(argv[j])));
        }
        if (j != argc-1)
            cmdrepr = sdscatlen(cmdrepr," ",1);
    }
    cmdrepr = sdscatlen(cmdrepr,"\r\n",2);
    cmdobj = createObject(OBJ_STRING,cmdrepr);

    listRewind(monitors,&li);
    while((ln = listNext(&li))) {
        client *monitor = (client*)ln->value;
		std::unique_lock<decltype(monitor->lock)> lock(monitor->lock, std::defer_lock);
		// When writing to clients on other threads the global lock is sufficient provided we only use AddReply*Async()
		if (FCorrectThread(c))
			lock.lock();
        addReply(monitor,cmdobj);
    }
    decrRefCount(cmdobj);
}

int prepareClientToWrite(client *c);

/* Feed the replica 'c' with the replication backlog starting from the
 * specified 'offset' up to the end of the backlog. */
long long addReplyReplicationBacklog(client *c, long long offset) {
    long long skip, len;

    serverLog(LL_DEBUG, "[PSYNC] Replica request offset: %lld", offset);

    if (g_pserver->repl_backlog_histlen == 0) {
        serverLog(LL_DEBUG, "[PSYNC] Backlog history len is zero");
        c->repl_curr_off = g_pserver->master_repl_offset;
        c->repl_end_off = g_pserver->master_repl_offset;
        return 0;
    }

    serverLog(LL_DEBUG, "[PSYNC] Backlog size: %lld",
             g_pserver->repl_backlog_size);
    serverLog(LL_DEBUG, "[PSYNC] First byte: %lld",
             g_pserver->repl_backlog_off);
    serverLog(LL_DEBUG, "[PSYNC] History len: %lld",
             g_pserver->repl_backlog_histlen);
    serverLog(LL_DEBUG, "[PSYNC] Current index: %lld",
             g_pserver->repl_backlog_idx);

    /* Compute the amount of bytes we need to discard. */
    skip = offset - g_pserver->repl_backlog_off;
    serverLog(LL_DEBUG, "[PSYNC] Skipping: %lld", skip);

    len = g_pserver->repl_backlog_histlen - skip;
    serverLog(LL_DEBUG, "[PSYNC] Reply total length: %lld", len);

    /* Set the start and end offsets for the replica so that a future
     * writeToClient will send the backlog from the given offset to 
     * the current end of the backlog to said replica */
    c->repl_curr_off = offset - 1;
    c->repl_end_off = g_pserver->master_repl_offset;

    /* Force the partial sync to be queued */
    prepareClientToWrite(c);

    return len;
}

/* Return the offset to provide as reply to the PSYNC command received
 * from the replica. The returned value is only valid immediately after
 * the BGSAVE process started and before executing any other command
 * from clients. */
long long getPsyncInitialOffset(void) {
    return g_pserver->master_repl_offset;
}

/* Send a FULLRESYNC reply in the specific case of a full resynchronization,
 * as a side effect setup the replica for a full sync in different ways:
 *
 * 1) Remember, into the replica client structure, the replication offset
 *    we sent here, so that if new slaves will later attach to the same
 *    background RDB saving process (by duplicating this client output
 *    buffer), we can get the right offset from this replica.
 * 2) Set the replication state of the replica to WAIT_BGSAVE_END so that
 *    we start accumulating differences from this point.
 * 3) Force the replication stream to re-emit a SELECT statement so
 *    the new replica incremental differences will start selecting the
 *    right database number.
 *
 * Normally this function should be called immediately after a successful
 * BGSAVE for replication was started, or when there is one already in
 * progress that we attached our replica to. */
int replicationSetupSlaveForFullResync(client *replica, long long offset) {
    char buf[128];
    int buflen;

    replica->psync_initial_offset = offset;
    replica->replstate = SLAVE_STATE_WAIT_BGSAVE_END;

    replica->repl_curr_off = offset;
    replica->repl_end_off = g_pserver->master_repl_offset;

    /* We are going to accumulate the incremental changes for this
     * replica as well. Set replicaseldb to -1 in order to force to re-emit
     * a SELECT statement in the replication stream. */
    g_pserver->replicaseldb = -1;

    /* Don't send this reply to slaves that approached us with
     * the old SYNC command. */
    if (!(replica->flags & CLIENT_PRE_PSYNC)) {
        buflen = snprintf(buf,sizeof(buf),"+FULLRESYNC %s %lld\r\n",
                          g_pserver->replid,offset);
        if (connWrite(replica->conn,buf,buflen) != buflen) {
            freeClientAsync(replica);
            return C_ERR;
        }
    }
    return C_OK;
}

/* This function handles the PSYNC command from the point of view of a
 * master receiving a request for partial resynchronization.
 *
 * On success return C_OK, otherwise C_ERR is returned and we proceed
 * with the usual full resync. */
int masterTryPartialResynchronization(client *c) {
    serverAssert(GlobalLocksAcquired());
    long long psync_offset, psync_len;
    char *master_replid = (char*)ptrFromObj(c->argv[1]);
    char buf[128];
    int buflen;

    /* Parse the replication offset asked by the replica. Go to full sync
     * on parse error: this should never happen but we try to handle
     * it in a robust way compared to aborting. */
    if (getLongLongFromObjectOrReply(c,c->argv[2],&psync_offset,NULL) !=
       C_OK) goto need_full_resync;

    /* Is the replication ID of this master the same advertised by the wannabe
     * replica via PSYNC? If the replication ID changed this master has a
     * different replication history, and there is no way to continue.
     *
     * Note that there are two potentially valid replication IDs: the ID1
     * and the ID2. The ID2 however is only valid up to a specific offset. */
    if (strcasecmp(master_replid, g_pserver->replid) &&
        (strcasecmp(master_replid, g_pserver->replid2) ||
         psync_offset > g_pserver->second_replid_offset))
    {
        /* Replid "?" is used by slaves that want to force a full resync. */
        if (master_replid[0] != '?') {
            if (strcasecmp(master_replid, g_pserver->replid) &&
                strcasecmp(master_replid, g_pserver->replid2))
            {
                serverLog(LL_NOTICE,"Partial resynchronization not accepted: "
                    "Replication ID mismatch (Replica asked for '%s', my "
                    "replication IDs are '%s' and '%s')",
                    master_replid, g_pserver->replid, g_pserver->replid2);
            } else {
                serverLog(LL_NOTICE,"Partial resynchronization not accepted: "
                    "Requested offset for second ID was %lld, but I can reply "
                    "up to %lld", psync_offset, g_pserver->second_replid_offset);
            }
        } else {
            serverLog(LL_NOTICE,"Full resync requested by replica %s",
                replicationGetSlaveName(c));
        }
        goto need_full_resync;
    }

    /* We still have the data our replica is asking for? */
    if (!g_pserver->repl_backlog ||
        psync_offset < g_pserver->repl_backlog_off ||
        psync_offset > (g_pserver->repl_backlog_off + g_pserver->repl_backlog_histlen))
    {
        serverLog(LL_NOTICE,
            "Unable to partial resync with replica %s for lack of backlog (Replica request was: %lld).", replicationGetSlaveName(c), psync_offset);
        if (psync_offset > g_pserver->master_repl_offset) {
            serverLog(LL_WARNING,
                "Warning: replica %s tried to PSYNC with an offset that is greater than the master replication offset.", replicationGetSlaveName(c));
        }
        goto need_full_resync;
    }

    /* If we reached this point, we are able to perform a partial resync:
     * 1) Set client state to make it a replica.
     * 2) Inform the client we can continue with +CONTINUE
     * 3) Send the backlog data (from the offset to the end) to the replica. */
    c->flags |= CLIENT_SLAVE;
    c->replstate = SLAVE_STATE_ONLINE;
    c->repl_ack_time = g_pserver->unixtime;
    c->repl_put_online_on_ack = 0;
    listAddNodeTail(g_pserver->slaves,c);
    g_pserver->rgthreadvar[c->iel].cclientsReplica++;

    /* We can't use the connection buffers since they are used to accumulate
     * new commands at this stage. But we are sure the socket send buffer is
     * empty so this write will never fail actually. */
    if (c->slave_capa & SLAVE_CAPA_PSYNC2) {
        buflen = snprintf(buf,sizeof(buf),"+CONTINUE %s\r\n", g_pserver->replid);
    } else {
        buflen = snprintf(buf,sizeof(buf),"+CONTINUE\r\n");
    }
    if (connWrite(c->conn,buf,buflen) != buflen) {
        freeClientAsync(c);
        return C_OK;
    }
    psync_len = addReplyReplicationBacklog(c,psync_offset);
    serverLog(LL_NOTICE,
        "Partial resynchronization request from %s accepted. Sending %lld bytes of backlog starting from offset %lld.",
            replicationGetSlaveName(c),
            psync_len, psync_offset);
    /* Note that we don't need to set the selected DB at g_pserver->replicaseldb
     * to -1 to force the master to emit SELECT, since the replica already
     * has this state from the previous connection with the master. */

    refreshGoodSlavesCount();

    /* Fire the replica change modules event. */
    moduleFireServerEvent(REDISMODULE_EVENT_REPLICA_CHANGE,
                          REDISMODULE_SUBEVENT_REPLICA_CHANGE_ONLINE,
                          NULL);

    return C_OK; /* The caller can return, no full resync needed. */

need_full_resync:
    /* We need a full resync for some reason... Note that we can't
     * reply to PSYNC right now if a full SYNC is needed. The reply
     * must include the master offset at the time the RDB file we transfer
     * is generated, so we need to delay the reply to that moment. */
    return C_ERR;
}

int checkClientOutputBufferLimits(client *c);
void clientInstallAsyncWriteHandler(client *c);
class replicationBuffer {
    std::vector<client *> replicas;
    clientReplyBlock *reply = nullptr;
    size_t writtenBytesTracker = 0;

public:
    replicationBuffer() {
        reply = (clientReplyBlock*)zmalloc(sizeof(clientReplyBlock) + (PROTO_REPLY_CHUNK_BYTES*2));
        reply->size = zmalloc_usable_size(reply) - sizeof(clientReplyBlock);
        reply->used = 0;
    }

    ~replicationBuffer() {
        zfree(reply);
    }

    void addReplica(client *replica) {
        replicas.push_back(replica);
        replicationSetupSlaveForFullResync(replica,getPsyncInitialOffset());
        // Optimize the socket for bulk transfer
        //connDisableTcpNoDelay(replica->conn);
    }

    bool isActive() const { return !replicas.empty(); }

    void flushData() {
        if (reply == nullptr)
            return;
        size_t written = reply->used;

        for (auto replica : replicas) {
            std::unique_lock<fastlock> ul(replica->lock);
            while (checkClientOutputBufferLimits(replica)
              && (replica->flags.load(std::memory_order_relaxed) & CLIENT_CLOSE_ASAP) == 0) {
                ul.unlock();
                usleep(0);
                ul.lock();
            }
        }

        aeAcquireLock();
        for (size_t ireplica = 0; ireplica < replicas.size(); ++ireplica) {
            auto replica = replicas[ireplica];
            if (replica->flags.load(std::memory_order_relaxed) & CLIENT_CLOSE_ASAP) {
                replica->replstate = REPL_STATE_NONE;
                continue;
            }
            
            std::unique_lock<fastlock> lock(replica->lock);
            if (ireplica == (replicas.size()-1) && replica->replyAsync == nullptr) {
                replica->replyAsync = reply;
                reply = nullptr;
                if (!(replica->fPendingAsyncWrite)) clientInstallAsyncWriteHandler(replica);
            } else {
                addReplyProto(replica, reply->buf(), reply->used);
            }
        }
        ProcessPendingAsyncWrites();
        for (auto c : replicas) {
            if (c->flags & CLIENT_CLOSE_ASAP) {
                std::unique_lock<fastlock> ul(c->lock);
                c->replstate = REPL_STATE_NONE;   // otherwise the client can't be free'd
            }
        }
        replicas.erase(std::remove_if(replicas.begin(), replicas.end(), [](const client *c)->bool{ return c->flags.load(std::memory_order_relaxed) & CLIENT_CLOSE_ASAP;}), replicas.end());
        aeReleaseLock();
        if (reply != nullptr) {
            reply->used = 0;
        }
        writtenBytesTracker += written;
    }

    void addData(const char *data, unsigned long size) {
        if (reply != nullptr && (size + reply->used) > reply->size)
            flushData();

        if (reply != nullptr && reply->size < size) {
            serverAssert(reply->used == 0); // flush should have happened
            zfree(reply);
            reply = nullptr;
        }
        
        if (reply == nullptr) {
            reply = (clientReplyBlock*)zmalloc(sizeof(clientReplyBlock) + std::max(size, (unsigned long)(PROTO_REPLY_CHUNK_BYTES*64)));
            reply->size = zmalloc_usable_size(reply) - sizeof(clientReplyBlock);
            reply->used = 0;
        }

        serverAssert((reply->size - reply->used) >= size);
        memcpy(reply->buf() + reply->used, data, size);
        reply->used += size;
    }

    void addLongWithPrefix(long val, char prefix) {
        char buf[128];
        int len;

        buf[0] = prefix;
        len = ll2string(buf+1,sizeof(buf)-1,val);
        buf[len+1] = '\r';
        buf[len+2] = '\n';
        addData(buf, len+3);
    }

    void addArrayLen(int len) {
        addLongWithPrefix(len, '*');
    }

    void addLong(long val) {
        addLongWithPrefix(val, ':');
    }

    void addString(const char *s, unsigned long len) {
        addLongWithPrefix(len, '$');
        addData(s, len);
        addData("\r\n", 2);
    }

    size_t cbWritten() const { return writtenBytesTracker; }

    void end() {
        flushData();
        for (auto replica : replicas) {
            // Return to original settings
            if (!g_pserver->repl_disable_tcp_nodelay)
                connEnableTcpNoDelay(replica->conn);
        }
    }

    void putSlavesOnline() {
        for (auto replica : replicas) {
            replica->replstate = SLAVE_STATE_FASTSYNC_DONE;
            replica->repl_put_online_on_ack = 1;
        }
    }
};

int rdbSaveSnapshotForReplication(struct rdbSaveInfo *rsi) {
    // TODO: This needs to be on a background thread
    int retval = C_OK;
    serverAssert(GlobalLocksAcquired());
    serverLog(LL_NOTICE, "Starting storage provider fast full sync with target: %s", "disk");

    std::shared_ptr<replicationBuffer> spreplBuf = std::make_shared<replicationBuffer>();
    listNode *ln;
    listIter li;
    client *replica = nullptr;
    listRewind(g_pserver->slaves, &li);
    while (replica == nullptr && (ln = listNext(&li))) {
        client *replicaCur = (client*)listNodeValue(ln);
        if ((replicaCur->slave_capa & SLAVE_CAPA_ROCKSDB_SNAPSHOT) && (replicaCur->replstate == SLAVE_STATE_WAIT_BGSAVE_START)) {
            replica = replicaCur;
            spreplBuf->addReplica(replica);
            replicaCur->replstate = SLAVE_STATE_FASTSYNC_TX;
        }
    }
    serverAssert(replica != nullptr);

    spreplBuf->addArrayLen(2);   // Two sections: Metadata and databases

    // MetaData
    aeAcquireLock();
    spreplBuf->addArrayLen(3 + dictSize(g_pserver->lua_scripts));
        spreplBuf->addArrayLen(2);
            spreplBuf->addString("repl-stream-db", 14);
            spreplBuf->addLong(rsi->repl_stream_db);
        spreplBuf->addArrayLen(2);
            spreplBuf->addString("repl-id", 7);
            spreplBuf->addString(rsi->repl_id, CONFIG_RUN_ID_SIZE);
        spreplBuf->addArrayLen(2);
            spreplBuf->addString("repl-offset", 11);
            spreplBuf->addLong(rsi->master_repl_offset);

    if (dictSize(g_pserver->lua_scripts)) {
        dictEntry *de;
        auto di = dictGetIterator(g_pserver->lua_scripts);
        while((de = dictNext(di)) != NULL) {
            robj *body = (robj*)dictGetVal(de);

            spreplBuf->addArrayLen(2);
            spreplBuf->addString("lua", 3);
            spreplBuf->addString(szFromObj(body), sdslen(szFromObj(body)));
        }
        dictReleaseIterator(di);
        di = NULL; /* So that we don't release it again on error. */
    }

    std::shared_ptr<std::vector<std::unique_ptr<const StorageCache>>> spvecspsnapshot = std::make_shared<std::vector<std::unique_ptr<const StorageCache>>>();
    for (int idb = 0; idb < cserver.dbnum; ++idb) {
        spvecspsnapshot->emplace_back(g_pserver->db[idb]->CloneStorageCache());
    }
    aeReleaseLock();

    g_pserver->asyncworkqueue->AddWorkFunction([spreplBuf = std::move(spreplBuf), spvecspsnapshot = std::move(spvecspsnapshot)]{
        int retval = C_OK;
        auto timeStart = ustime();
        auto lastLogTime = timeStart;
        size_t cbData = 0;
        size_t cbLastUpdate = 0;
        auto &replBuf = *spreplBuf;
        // Databases
        replBuf.addArrayLen(cserver.dbnum);
        for (int idb = 0; idb < cserver.dbnum; ++idb) {
            auto &spsnapshot = (*spvecspsnapshot)[idb];
            size_t snapshotDeclaredCount = spsnapshot->count();
            replBuf.addArrayLen(snapshotDeclaredCount);
            size_t count = 0;
            bool result = spsnapshot->enumerate([&replBuf, &count, &cbData, &lastLogTime, &cbLastUpdate](const char *rgchKey, size_t cchKey, const void *rgchVal, size_t cchVal) -> bool{
                replBuf.addArrayLen(2);

                replBuf.addString(rgchKey, cchKey);
                replBuf.addString((const char *)rgchVal, cchVal);
                ++count;
                if ((count % 8092) == 0) {
                    auto curTime = ustime();
                    if ((curTime - lastLogTime) > 60000000) {
                        auto usec = curTime - lastLogTime;
                        serverLog(LL_NOTICE, "Replication status: Transferred %zuMB (%.2fGbit/s)", replBuf.cbWritten()/1024/1024, ((replBuf.cbWritten()-cbLastUpdate)*8.0)/(usec/1000000.0)/1000000000.0);
                        cbLastUpdate = replBuf.cbWritten();
                        lastLogTime = ustime();
                    }
                }
                cbData += cchKey + cchVal;
                return replBuf.isActive();
            });

            if (!result) {
                retval = C_ERR;
                break;
            }
            serverAssert(count == snapshotDeclaredCount);
        }
        
        replBuf.end();

        if (!replBuf.isActive()) {
            retval = C_ERR;
        }

        serverLog(LL_NOTICE, "Snapshot replication done: %s", (retval == C_OK) ? "success" : "failed");
        auto usec = ustime() - timeStart;
        serverLog(LL_NOTICE, "Transferred %zuMB total (%zuMB data) in %.2f seconds.  (%.2fGbit/s)", spreplBuf->cbWritten()/1024/1024, cbData/1024/1024, usec/1000000.0, (spreplBuf->cbWritten()*8.0)/(usec/1000000.0)/1000000000.0);
        if (retval == C_OK) {
            aeAcquireLock();
            replBuf.putSlavesOnline();
            aeReleaseLock();
        }
    });

    return retval;
}

/* Start a BGSAVE for replication goals, which is, selecting the disk or
 * socket target depending on the configuration, and making sure that
 * the script cache is flushed before to start.
 *
 * The mincapa argument is the bitwise AND among all the slaves capabilities
 * of the slaves waiting for this BGSAVE, so represents the replica capabilities
 * all the slaves support. Can be tested via SLAVE_CAPA_* macros.
 *
 * Side effects, other than starting a BGSAVE:
 *
 * 1) Handle the slaves in WAIT_START state, by preparing them for a full
 *    sync if the BGSAVE was successfully started, or sending them an error
 *    and dropping them from the list of slaves.
 *
 * 2) Flush the Lua scripting script cache if the BGSAVE was actually
 *    started.
 *
 * Returns C_OK on success or C_ERR otherwise. */
int startBgsaveForReplication(int mincapa) {
    serverAssert(GlobalLocksAcquired());
    int retval;
    int socket_target = g_pserver->repl_diskless_sync && (mincapa & SLAVE_CAPA_EOF);
    listIter li;
    listNode *ln;

    if (g_pserver->loading && g_pserver->fActiveReplica) {
        serverLog(LL_NOTICE, "Can't bgsave while loading in active replication mode");
        return C_ERR;
    }

    serverLog(LL_NOTICE,"Starting BGSAVE for SYNC with target: %s",
        socket_target ? "replicas sockets" : "disk");

    rdbSaveInfo rsi, *rsiptr;
    rsiptr = rdbPopulateSaveInfo(&rsi);
    /* Only do rdbSave* when rsiptr is not NULL,
     * otherwise replica will miss repl-stream-db. */
    if (rsiptr) {
        if (mincapa & SLAVE_CAPA_ROCKSDB_SNAPSHOT && g_pserver->m_pstorageFactory)
            retval = rdbSaveSnapshotForReplication(rsiptr);
        else if (socket_target)
            retval = rdbSaveToSlavesSockets(rsiptr);
        else
            retval = rdbSaveBackground(rsiptr);
    } else {
        serverLog(LL_WARNING,"BGSAVE for replication: replication information not available, can't generate the RDB file right now. Try later.");
        retval = C_ERR;
    }

    /* If we succeeded to start a BGSAVE with disk target, let's remember
     * this fact, so that we can later delete the file if needed. Note
     * that we don't set the flag to 1 if the feature is disabled, otherwise
     * it would never be cleared: the file is not deleted. This way if
     * the user enables it later with CONFIG SET, we are fine. */
    if (retval == C_OK && !socket_target && g_pserver->rdb_del_sync_files)
        RDBGeneratedByReplication = 1;

    /* If we failed to BGSAVE, remove the slaves waiting for a full
     * resynchronization from the list of slaves, inform them with
     * an error about what happened, close the connection ASAP. */
    if (retval == C_ERR) {
        serverLog(LL_WARNING,"BGSAVE for replication failed");
        listRewind(g_pserver->slaves,&li);
        while((ln = listNext(&li))) {
            client *replica = (client*)ln->value;
            std::unique_lock<decltype(replica->lock)> lock(replica->lock);

            if (replica->replstate == SLAVE_STATE_WAIT_BGSAVE_START) {
                replica->replstate = REPL_STATE_NONE;
                replica->flags &= ~CLIENT_SLAVE;
                listDelNode(g_pserver->slaves,ln);
                g_pserver->rgthreadvar[replica->iel].cclientsReplica--;
                addReplyError(replica,
                    "BGSAVE failed, replication can't continue");
                replica->flags |= CLIENT_CLOSE_AFTER_REPLY;
            }
        }
        return retval;
    }

    /* If the target is socket, rdbSaveToSlavesSockets() already setup
     * the slaves for a full resync. Otherwise for disk target do it now.*/
    if (!socket_target) {
        listRewind(g_pserver->slaves,&li);
        while((ln = listNext(&li))) {
            client *replica = (client*)ln->value;
            std::unique_lock<decltype(replica->lock)> lock(replica->lock);

            if (replica->replstate == SLAVE_STATE_WAIT_BGSAVE_START) {
                    replicationSetupSlaveForFullResync(replica,
                            getPsyncInitialOffset());
            }
        }
    }

    /* Flush the script cache, since we need that replica differences are
     * accumulated without requiring slaves to match our cached scripts. */
    if (retval == C_OK) replicationScriptCacheFlush();
    return retval;
}

/* SYNC and PSYNC command implementation. */
void syncCommand(client *c) {
    /* ignore SYNC if already replica or in monitor mode */
    if (c->flags & CLIENT_SLAVE) return;

    /* Check if this is a failover request to a replica with the same replid and
     * become a master if so. */
    if (c->argc > 3 && !strcasecmp(szFromObj(c->argv[0]),"psync") && 
        !strcasecmp(szFromObj(c->argv[3]),"failover"))
    {
        serverLog(LL_WARNING, "Failover request received for replid %s.",
            (unsigned char *)szFromObj(c->argv[1]));
        if (!listLength(g_pserver->masters)) {
            addReplyError(c, "PSYNC FAILOVER can't be sent to a master.");
            return;
        }
        if (listLength(g_pserver->masters) > 1) {
            addReplyError(c, "PSYNC FAILOVER can't be used with multi-master.");
            return;
        }

        if (!strcasecmp(szFromObj(c->argv[1]),g_pserver->replid)) {
            replicationUnsetMaster((redisMaster*)listNodeValue(listFirst(g_pserver->masters)));
            sds client = catClientInfoString(sdsempty(),c);
            serverLog(LL_NOTICE,
                "MASTER MODE enabled (failover request from '%s')",client);
            sdsfree(client);
        } else {
            addReplyError(c, "PSYNC FAILOVER replid must match my replid.");
            return;            
        }
    }

    /* Don't let replicas sync with us while we're failing over */
    if (g_pserver->failover_state != NO_FAILOVER) {
        addReplyError(c,"-NOMASTERLINK Can't SYNC while failing over");
        return;
    }

    /* Refuse SYNC requests if we are a slave but the link with our master
     * is not ok... */
    if (!g_pserver->fActiveReplica) {
        if (FAnyDisconnectedMasters()) {
            addReplyError(c,"-NOMASTERLINK Can't SYNC while not connected with my master");
            return;
        }
    }

    /* SYNC can't be issued when the server has pending data to send to
     * the client about already issued commands. We need a fresh reply
     * buffer registering the differences between the BGSAVE and the current
     * dataset, so that we can copy to other slaves if needed. */
    if (clientHasPendingReplies(c)) {
        addReplyError(c,"SYNC and PSYNC are invalid with pending output");
        return;
    }

    serverLog(LL_NOTICE,"Replica %s asks for synchronization",
        replicationGetSlaveName(c));

    /* Try a partial resynchronization if this is a PSYNC command.
     * If it fails, we continue with usual full resynchronization, however
     * when this happens masterTryPartialResynchronization() already
     * replied with:
     *
     * +FULLRESYNC <replid> <offset>
     *
     * So the replica knows the new replid and offset to try a PSYNC later
     * if the connection with the master is lost. */
    if (!strcasecmp((const char*)ptrFromObj(c->argv[0]),"psync")) {
        if (masterTryPartialResynchronization(c) == C_OK) {
            g_pserver->stat_sync_partial_ok++;
            return; /* No full resync needed, return. */
        } else {
            char *master_replid = (char*)ptrFromObj(c->argv[1]);

            /* Increment stats for failed PSYNCs, but only if the
             * replid is not "?", as this is used by slaves to force a full
             * resync on purpose when they are not albe to partially
             * resync. */
            if (master_replid[0] != '?') g_pserver->stat_sync_partial_err++;
        }
    } else {
        /* If a replica uses SYNC, we are dealing with an old implementation
         * of the replication protocol (like keydb-cli --replica). Flag the client
         * so that we don't expect to receive REPLCONF ACK feedbacks. */
        c->flags |= CLIENT_PRE_PSYNC;
    }

    /* Full resynchronization. */
    g_pserver->stat_sync_full++;

    /* Setup the replica as one waiting for BGSAVE to start. The following code
     * paths will change the state if we handle the replica differently. */
    c->replstate = SLAVE_STATE_WAIT_BGSAVE_START;
    if (g_pserver->repl_disable_tcp_nodelay)
        connDisableTcpNoDelay(c->conn); /* Non critical if it fails. */
    c->repldbfd = -1;
    c->flags |= CLIENT_SLAVE;
    listAddNodeTail(g_pserver->slaves,c);
    g_pserver->rgthreadvar[c->iel].cclientsReplica++;

    /* Create the replication backlog if needed. */
    if (listLength(g_pserver->slaves) == 1 && g_pserver->repl_backlog == NULL) {
        /* When we create the backlog from scratch, we always use a new
         * replication ID and clear the ID2, since there is no valid
         * past history. */
        changeReplicationId();
        clearReplicationId2();
        createReplicationBacklog();
        serverLog(LL_NOTICE,"Replication backlog created, my new "
                            "replication IDs are '%s' and '%s'",
                            g_pserver->replid, g_pserver->replid2);
    }

    /* CASE 0: Fast Sync */
    if ((c->slave_capa & SLAVE_CAPA_ROCKSDB_SNAPSHOT) && g_pserver->m_pstorageFactory) {
        serverLog(LL_NOTICE,"Fast SYNC on next replication cycle");
    /* CASE 1: BGSAVE is in progress, with disk target. */
    } else if (g_pserver->FRdbSaveInProgress() &&
        g_pserver->rdb_child_type == RDB_CHILD_TYPE_DISK)
    {
        /* Ok a background save is in progress. Let's check if it is a good
         * one for replication, i.e. if there is another replica that is
         * registering differences since the server forked to save. */
        client *replica;
        listNode *ln;
        listIter li;

        listRewind(g_pserver->slaves,&li);
        while((ln = listNext(&li))) {
            replica = (client*)ln->value;
            /* If the client needs a buffer of commands, we can't use
             * a replica without replication buffer. */
            if (replica->replstate == SLAVE_STATE_WAIT_BGSAVE_END &&
                (!(replica->flags & CLIENT_REPL_RDBONLY) ||
                 (c->flags & CLIENT_REPL_RDBONLY)))
                break;
        }
        
        /* To attach this replica, we check that it has at least all the
         * capabilities of the replica that triggered the current BGSAVE. */
        if (ln && ((c->slave_capa & replica->slave_capa) == replica->slave_capa)) {
            /* Perfect, the server is already registering differences for
             * another slave. Set the right state, and copy the buffer.
             * We don't copy buffer if clients don't want. */
            if (!(c->flags & CLIENT_REPL_RDBONLY)) copyClientOutputBuffer(c,replica);
            replicationSetupSlaveForFullResync(c,replica->psync_initial_offset);
            serverLog(LL_NOTICE,"Waiting for end of BGSAVE for SYNC");
        } else {
            /* No way, we need to wait for the next BGSAVE in order to
             * register differences. */
            serverLog(LL_NOTICE,"Can't attach the replica to the current BGSAVE. Waiting for next BGSAVE for SYNC");
        }

    /* CASE 2: BGSAVE is in progress, with socket target. */
    } else if (g_pserver->FRdbSaveInProgress() &&
               g_pserver->rdb_child_type == RDB_CHILD_TYPE_SOCKET)
    {
        /* There is an RDB child process but it is writing directly to
         * children sockets. We need to wait for the next BGSAVE
         * in order to synchronize. */
        serverLog(LL_NOTICE,"Current BGSAVE has socket target. Waiting for next BGSAVE for SYNC");

    /* CASE 3: There is no BGSAVE is progress. */
    } else {
        if (g_pserver->repl_diskless_sync && (c->slave_capa & SLAVE_CAPA_EOF) &&
            g_pserver->repl_diskless_sync_delay)
        {
            /* Diskless replication RDB child is created inside
             * replicationCron() since we want to delay its start a
             * few seconds to wait for more slaves to arrive. */
            serverLog(LL_NOTICE,"Delay next BGSAVE for diskless SYNC");
        } else {
            /* We don't have a BGSAVE in progress, let's start one. Diskless
             * or disk-based mode is determined by replica's capacity. */
            if (!hasActiveChildProcess()) {
                startBgsaveForReplication(c->slave_capa);
            } else {
                serverLog(LL_NOTICE,
                    "No BGSAVE in progress, but another BG operation is active. "
                    "BGSAVE for replication delayed");
            }
        }
    }
    return;
}

void processReplconfUuid(client *c, robj *arg)
{
    const char *remoteUUID = nullptr;

    if (arg->type != OBJ_STRING)
        goto LError;

    remoteUUID = (const char*)ptrFromObj(arg);
    if (strlen(remoteUUID) != 36)
        goto LError;

    if (uuid_parse(remoteUUID, c->uuid) != 0)
        goto LError;

    listIter liMi;
    listNode *lnMi;
    listRewind(g_pserver->masters, &liMi);

    // Enforce a fair ordering for connection, if they attempt to connect before us close them out
    // This must be consistent so that both make the same decision of who should proceed first
    while ((lnMi = listNext(&liMi))) {
        redisMaster *mi = (redisMaster*)listNodeValue(lnMi);
        if (mi->repl_state == REPL_STATE_CONNECTED)
            continue;
        if (FSameUuidNoNil(mi->master_uuid, c->uuid)) {
            // Decide based on UUID so both clients make the same decision of which host loses 
            //  otherwise we may entere a loop where neither client can proceed
            if (memcmp(mi->master_uuid, c->uuid, UUID_BINARY_LEN) < 0) {
                freeClientAsync(c);
            }
        }
    }

    char szServerUUID[36 + 2]; // 1 for the '+', another for '\0'
    szServerUUID[0] = '+';
    uuid_unparse(cserver.uuid, szServerUUID+1);
    addReplyProto(c, szServerUUID, 37);
    addReplyProto(c, "\r\n", 2);
    return;

LError:
    addReplyError(c, "Invalid UUID");
    return;
}

void processReplconfLicense(client *c, robj *)
{
    // Only for back-compat
    addReply(c, shared.ok);
}

/* REPLCONF <option> <value> <option> <value> ...
 * This command is used by a replica in order to configure the replication
 * process before starting it with the SYNC command.
 * This command is also used by a master in order to get the replication
 * offset from a replica.
 *
 * Currently we support these options:
 *
 * - listening-port <port>
 * - ip-address <ip>
 * What is the listening ip and port of the Replica redis instance, so that
 * the master can accurately lists replicas and their listening ports in the
 * INFO output.
 *
 * - capa <eof|psync2>
 * What is the capabilities of this instance.
 * eof: supports EOF-style RDB transfer for diskless replication.
 * psync2: supports PSYNC v2, so understands +CONTINUE <new repl ID>.
 *
 * - ack <offset>
 * Replica informs the master the amount of replication stream that it
 * processed so far.
 *
 * - getack
 * Unlike other subcommands, this is used by master to get the replication
 * offset from a replica.
 *
 * - rdb-only
 * Only wants RDB snapshot without replication buffer. */
void replconfCommand(client *c) {
    int j;
    bool fCapaCommand = false;

    if ((c->argc % 2) == 0) {
        /* Number of arguments must be odd to make sure that every
         * option has a corresponding value. */
        addReplyErrorObject(c,shared.syntaxerr);
        return;
    }

    /* Process every option-value pair. */
    for (j = 1; j < c->argc; j+=2) {
        fCapaCommand = false;
        if (!strcasecmp((const char*)ptrFromObj(c->argv[j]),"listening-port")) {
            long port;

            if ((getLongFromObjectOrReply(c,c->argv[j+1],
                    &port,NULL) != C_OK))
                return;
            c->slave_listening_port = port;
        } else if (!strcasecmp((const char*)ptrFromObj(c->argv[j]),"ip-address")) {
            sds addr = (sds)ptrFromObj(c->argv[j+1]);
            if (sdslen(addr) < NET_HOST_STR_LEN) {
                if (c->slave_addr) sdsfree(c->slave_addr);
                c->slave_addr = sdsdup(addr);
            } else {
                addReplyErrorFormat(c,"REPLCONF ip-address provided by "
                    "replica instance is too long: %zd bytes", sdslen(addr));
                return;
            }
        } else if (!strcasecmp((const char*)ptrFromObj(c->argv[j]),"capa")) {
            /* Ignore capabilities not understood by this master. */
            if (!strcasecmp((const char*)ptrFromObj(c->argv[j+1]),"eof"))
                c->slave_capa |= SLAVE_CAPA_EOF;
            else if (!strcasecmp((const char*)ptrFromObj(c->argv[j+1]),"psync2"))
                c->slave_capa |= SLAVE_CAPA_PSYNC2;
            else if (!strcasecmp((const char*)ptrFromObj(c->argv[j+1]), "activeExpire"))
                c->slave_capa |= SLAVE_CAPA_ACTIVE_EXPIRE;
            else if (!strcasecmp((const char*)ptrFromObj(c->argv[j+1]), "rocksdb-snapshot-load"))
                c->slave_capa |= SLAVE_CAPA_ROCKSDB_SNAPSHOT;

            fCapaCommand = true;
        } else if (!strcasecmp((const char*)ptrFromObj(c->argv[j]),"ack")) {
            /* REPLCONF ACK is used by replica to inform the master the amount
             * of replication stream that it processed so far. It is an
             * internal only command that normal clients should never use. */
            long long offset;

            if (!(c->flags & CLIENT_SLAVE)) return;
            if ((getLongLongFromObject(c->argv[j+1], &offset) != C_OK))
                return;
            if (offset > c->repl_ack_off)
                c->repl_ack_off = offset;
            c->repl_ack_time = g_pserver->unixtime;
            /* If this was a diskless replication, we need to really put
             * the replica online when the first ACK is received (which
             * confirms slave is online and ready to get more data). This
             * allows for simpler and less CPU intensive EOF detection
             * when streaming RDB files.
             * There's a chance the ACK got to us before we detected that the
             * bgsave is done (since that depends on cron ticks), so run a
             * quick check first (instead of waiting for the next ACK. */
            if (g_pserver->child_type == CHILD_TYPE_RDB && c->replstate == SLAVE_STATE_WAIT_BGSAVE_END)
                checkChildrenDone();
            if (c->repl_put_online_on_ack && (c->replstate == SLAVE_STATE_ONLINE || c->replstate == SLAVE_STATE_FASTSYNC_DONE))
                putSlaveOnline(c);
            /* Note: this command does not reply anything! */
            return;
        } else if (!strcasecmp((const char*)ptrFromObj(c->argv[j]),"getack")) {
            /* REPLCONF GETACK is used in order to request an ACK ASAP
             * to the replica. */
            listIter li;
            listNode *ln;
            listRewind(g_pserver->masters, &li);
            while ((ln = listNext(&li)))
            {
                replicationSendAck((redisMaster*)listNodeValue(ln));
            }
            return;
        } else if (!strcasecmp((const char*)ptrFromObj(c->argv[j]),"uuid")) {
            /* REPLCONF uuid is used to set and send the UUID of each host */
            processReplconfUuid(c, c->argv[j+1]);
            return; // the process function replies to the client for both error and success
        } else if (!strcasecmp(szFromObj(c->argv[j]),"license")) {
            processReplconfLicense(c, c->argv[j+1]);
            return;
        } else if (!strcasecmp(szFromObj(c->argv[j]),"rdb-only")) {
           /* REPLCONF RDB-ONLY is used to identify the client only wants
            * RDB snapshot without replication buffer. */
            long rdb_only = 0;
            if (getRangeLongFromObjectOrReply(c,c->argv[j+1],
                    0,1,&rdb_only,NULL) != C_OK)
                return;
            if (rdb_only == 1) c->flags |= CLIENT_REPL_RDBONLY;
            else c->flags &= ~CLIENT_REPL_RDBONLY;
        } else {
            addReplyErrorFormat(c,"Unrecognized REPLCONF option: %s",
                (char*)ptrFromObj(c->argv[j]));
            return;
        }
    }

    if (fCapaCommand) {
        sds reply = sdsnew("+OK");
        if (g_pserver->fActiveReplica)
            reply = sdscat(reply, " active-replica");
        if (g_pserver->m_pstorageFactory && (c->slave_capa & SLAVE_CAPA_ROCKSDB_SNAPSHOT) && !g_pserver->fActiveReplica)
            reply = sdscat(reply, " rocksdb-snapshot-save");
        reply = sdscat(reply, "\r\n");
        addReplySds(c, reply);
    } else {
        addReply(c,shared.ok);
    }
}

/* This function puts a replica in the online state, and should be called just
 * after a replica received the RDB file for the initial synchronization, and
 * we are finally ready to send the incremental stream of commands.
 *
 * It does a few things:
 * 1) Close the replica's connection async if it doesn't need replication
 *    commands buffer stream, since it actually isn't a valid replica.
 * 2) Put the slave in ONLINE state. Note that the function may also be called
 *    for a replicas that are already in ONLINE state, but having the flag
 *    repl_put_online_on_ack set to true: we still have to install the write
 *    handler in that case. This function will take care of that.
 * 3) Make sure the writable event is re-installed, since calling the SYNC
 *    command disables it, so that we can accumulate output buffer without
 *    sending it to the replica.
 * 4) Update the count of "good replicas". */
void putSlaveOnline(client *replica) {
    replica->replstate = SLAVE_STATE_ONLINE;
    
    replica->repl_put_online_on_ack = 0;
    replica->repl_ack_time = g_pserver->unixtime; /* Prevent false timeout. */

    if (replica->flags & CLIENT_REPL_RDBONLY) {
        serverLog(LL_NOTICE,
            "Close the connection with replica %s as RDB transfer is complete",
            replicationGetSlaveName(replica));
        freeClientAsync(replica);
        return;
    }
    if (connSetWriteHandler(replica->conn, sendReplyToClient, true) == C_ERR) {
        serverLog(LL_WARNING,"Unable to register writable event for replica bulk transfer: %s", strerror(errno));
        freeClient(replica);
        return;
    }
    refreshGoodSlavesCount();
    /* Fire the replica change modules event. */
    moduleFireServerEvent(REDISMODULE_EVENT_REPLICA_CHANGE,
                          REDISMODULE_SUBEVENT_REPLICA_CHANGE_ONLINE,
                          NULL);
    serverLog(LL_NOTICE,"Synchronization with replica %s succeeded",
        replicationGetSlaveName(replica));
    
    if (!(replica->slave_capa & SLAVE_CAPA_ACTIVE_EXPIRE) && g_pserver->fActiveReplica)
    {
        serverLog(LL_WARNING, "Warning: replica %s does not support active expiration.  This client may not correctly process key expirations."
            "\n\tThis is OK if you are in the process of an active upgrade.", replicationGetSlaveName(replica));
        serverLog(LL_WARNING, "Connections between active replicas and traditional replicas is deprecated.  This will be refused in future versions."
            "\n\tPlease fix your replica topology");
    }
}

/* We call this function periodically to remove an RDB file that was
 * generated because of replication, in an instance that is otherwise
 * without any persistence. We don't want instances without persistence
 * to take RDB files around, this violates certain policies in certain
 * environments. */
void removeRDBUsedToSyncReplicas(void) {
    serverAssert(GlobalLocksAcquired());

    /* If the feature is disabled, return ASAP but also clear the
     * RDBGeneratedByReplication flag in case it was set. Otherwise if the
     * feature was enabled, but gets disabled later with CONFIG SET, the
     * flag may remain set to one: then next time the feature is re-enabled
     * via CONFIG SET we have have it set even if no RDB was generated
     * because of replication recently. */
    if (!g_pserver->rdb_del_sync_files) {
        RDBGeneratedByReplication = 0;
        return;
    }

    if (allPersistenceDisabled() && RDBGeneratedByReplication) {
        client *slave;
        listNode *ln;
        listIter li;

        int delrdb = 1;
        listRewind(g_pserver->slaves,&li);
        while((ln = listNext(&li))) {
            slave = (client*)ln->value;
            if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_START ||
                slave->replstate == SLAVE_STATE_WAIT_BGSAVE_END ||
                slave->replstate == SLAVE_STATE_SEND_BULK)
            {
                delrdb = 0;
                break; /* No need to check the other replicas. */
            }
        }
        if (delrdb) {
            struct stat sb;
            if (lstat(g_pserver->rdb_filename,&sb) != -1) {
                RDBGeneratedByReplication = 0;
                serverLog(LL_NOTICE,
                    "Removing the RDB file used to feed replicas "
                    "in a persistence-less instance");
                bg_unlink(g_pserver->rdb_filename);
            }
        }
    }
}

void sendBulkToSlave(connection *conn) {
    serverAssert(GlobalLocksAcquired());
    
    client *replica = (client*)connGetPrivateData(conn);
    serverAssert(FCorrectThread(replica));
    ssize_t nwritten;
    AeLocker aeLock;
    std::unique_lock<fastlock> ul(replica->lock);

    /* Before sending the RDB file, we send the preamble as configured by the
     * replication process. Currently the preamble is just the bulk count of
     * the file in the form "$<length>\r\n". */
    if (replica->replpreamble) {
        nwritten = connWrite(conn,replica->replpreamble,sdslen(replica->replpreamble));
        if (nwritten == -1) {
            serverLog(LL_VERBOSE,
                "Write error sending RDB preamble to replica: %s",
                connGetLastError(conn));
            ul.unlock();
            aeLock.arm(nullptr);
            freeClient(replica);
            return;
        }
        g_pserver->stat_net_output_bytes += nwritten;
        sdsrange(replica->replpreamble,nwritten,-1);
        if (sdslen(replica->replpreamble) == 0) {
            sdsfree(replica->replpreamble);
            replica->replpreamble = NULL;
            /* fall through sending data. */
        } else {
            return;
        }
    }

    /* If the preamble was already transferred, send the RDB bulk data.
     * try to use sendfile system call if supported, unless tls is enabled.
     * fallback to normal read+write otherwise. */
    nwritten = 0;
    ssize_t buflen;
    char buf[PROTO_IOBUF_LEN];

    lseek(replica->repldbfd,replica->repldboff,SEEK_SET);
    buflen = read(replica->repldbfd,buf,PROTO_IOBUF_LEN);
    if (buflen <= 0) {
        serverLog(LL_WARNING,"Read error sending DB to replica: %s",
            (buflen == 0) ? "premature EOF" : strerror(errno));
        ul.unlock();
        aeLock.arm(nullptr);
        freeClient(replica);
        return;
    }
    if ((nwritten = connWrite(conn,buf,buflen)) == -1) {
        if (connGetState(conn) != CONN_STATE_CONNECTED) {
            serverLog(LL_WARNING,"Write error sending DB to replica: %s",
                connGetLastError(conn));
            ul.unlock();
            aeLock.arm(nullptr);
            freeClient(replica);
        }
        return;
    }

    replica->repldboff += nwritten;
    g_pserver->stat_net_output_bytes += nwritten;
    if (replica->repldboff == replica->repldbsize) {
        close(replica->repldbfd);
        replica->repldbfd = -1;
        connSetWriteHandler(replica->conn,NULL);
        putSlaveOnline(replica);
    }
}

/* Remove one write handler from the list of connections waiting to be writable
 * during rdb pipe transfer. */
void rdbPipeWriteHandlerConnRemoved(struct connection *conn) {
    if (!connHasWriteHandler(conn))
        return;
    connSetWriteHandler(conn, NULL);
    client *slave = (client*)connGetPrivateData(conn);
    slave->repl_last_partial_write = 0;
    g_pserver->rdb_pipe_numconns_writing--;
    /* if there are no more writes for now for this conn, or write error: */
    if (g_pserver->rdb_pipe_numconns_writing == 0) {
        aePostFunction(g_pserver->rgthreadvar[IDX_EVENT_LOOP_MAIN].el, []{
            if (aeCreateFileEvent(serverTL->el, g_pserver->rdb_pipe_read, AE_READABLE, rdbPipeReadHandler,NULL) == AE_ERR) {
                serverPanic("Unrecoverable error creating g_pserver->rdb_pipe_read file event.");
            }
        });
    }
}

/* Called in diskless master during transfer of data from the rdb pipe, when
 * the replica becomes writable again. */
void rdbPipeWriteHandler(struct connection *conn) {
    serverAssert(g_pserver->rdb_pipe_bufflen>0);
    client *slave = (client*)connGetPrivateData(conn);
    AssertCorrectThread(slave);
    int nwritten;
    
    if (slave->flags & CLIENT_CLOSE_ASAP) {
        rdbPipeWriteHandlerConnRemoved(conn);
        return;
    }
    
    if ((nwritten = connWrite(conn, g_pserver->rdb_pipe_buff + slave->repldboff,
                              g_pserver->rdb_pipe_bufflen - slave->repldboff)) == -1)
    {
        if (connGetState(conn) == CONN_STATE_CONNECTED)
            return; /* equivalent to EAGAIN */
        serverLog(LL_WARNING,"Write error sending DB to replica: %s",
            connGetLastError(conn));
        freeClientAsync(slave);
        return;
    } else {
        slave->repldboff += nwritten;
        g_pserver->stat_net_output_bytes += nwritten;
        if (slave->repldboff < g_pserver->rdb_pipe_bufflen) {
            slave->repl_last_partial_write = g_pserver->unixtime;
            return; /* more data to write.. */
        }
    }
    rdbPipeWriteHandlerConnRemoved(conn);
}

/* Called in diskless master, when there's data to read from the child's rdb pipe */
void rdbPipeReadHandler(struct aeEventLoop *eventLoop, int fd, void *clientData, int mask) {
    UNUSED(mask);
    UNUSED(clientData);

    int i;
    if (!g_pserver->rdb_pipe_buff)
        g_pserver->rdb_pipe_buff = (char*)zmalloc(PROTO_IOBUF_LEN);
    serverAssert(g_pserver->rdb_pipe_numconns_writing==0);
    serverAssert(eventLoop == g_pserver->rgthreadvar[IDX_EVENT_LOOP_MAIN].el);

    while (1) {
        g_pserver->rdb_pipe_bufflen = read(fd, g_pserver->rdb_pipe_buff, PROTO_IOBUF_LEN);
        if (g_pserver->rdb_pipe_bufflen < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                return;
            serverLog(LL_WARNING,"Diskless rdb transfer, read error sending DB to replicas: %s", strerror(errno));
            for (i=0; i < g_pserver->rdb_pipe_numconns; i++) {
                connection *conn = g_pserver->rdb_pipe_conns[i];
                if (!conn)
                    continue;
                client *slave = (client*)connGetPrivateData(conn);
                freeClientAsync(slave);
                g_pserver->rdb_pipe_conns[i] = NULL;
            }
            killRDBChild();
            return;
        }

        if (g_pserver->rdb_pipe_bufflen == 0 && !g_pserver->rdbThreadVars.fRdbThreadCancel) {
            /* EOF - write end was closed. */
            int stillUp = 0;
            aeDeleteFileEvent(eventLoop, g_pserver->rdb_pipe_read, AE_READABLE);
            for (i=0; i < g_pserver->rdb_pipe_numconns; i++)
            {
                connection *conn = g_pserver->rdb_pipe_conns[i];
                if (!conn)
                    continue;
                stillUp++;
            }
            serverLog(LL_WARNING,"Diskless rdb transfer, done reading from pipe, %d replicas still up.", stillUp);
            /* Now that the replicas have finished reading, notify the child that it's safe to exit. 
             * When the server detectes the child has exited, it can mark the replica as online, and
             * start streaming the replication buffers. */
            close(g_pserver->rdb_child_exit_pipe);
            g_pserver->rdb_child_exit_pipe = -1;
            return;
        }

        int stillAlive = 0;
        for (i=0; i < g_pserver->rdb_pipe_numconns; i++)
        {
            int nwritten;
            connection *conn = g_pserver->rdb_pipe_conns[i];
            if (!conn)
                continue;

            client *slave = (client*)connGetPrivateData(conn);
            std::unique_lock<fastlock> ul(slave->lock);
            serverAssert(slave->conn == conn);
            if(slave->flags.load(std::memory_order_relaxed) & CLIENT_CLOSE_ASAP)
                continue;   // if we asked to free the client don't send any more data
            
            // Normally it would be bug to talk a client conn from a different thread, but here we know nobody else will
            //  be sending anything while in this replication state so it is OK
            if ((nwritten = connWrite(conn, g_pserver->rdb_pipe_buff, g_pserver->rdb_pipe_bufflen)) == -1) {
                if (connGetState(conn) != CONN_STATE_CONNECTED) {
                    serverLog(LL_WARNING,"Diskless rdb transfer, write error sending DB to replica: %s",
                        connGetLastError(conn));
                    freeClientAsync(slave);
                    g_pserver->rdb_pipe_conns[i] = NULL;
                    continue;
                }
                /* An error and still in connected state, is equivalent to EAGAIN */
                slave->repldboff = 0;
            } else {
                /* Note: when use diskless replication, 'repldboff' is the offset
                 * of 'rdb_pipe_buff' sent rather than the offset of entire RDB. */
                slave->repldboff = nwritten;
                g_pserver->stat_net_output_bytes += nwritten;
            }
            /* If we were unable to write all the data to one of the replicas,
             * setup write handler (and disable pipe read handler, below) */
            if (nwritten != g_pserver->rdb_pipe_bufflen) {
                g_pserver->rdb_pipe_numconns_writing++;
                slave->repl_last_partial_write = g_pserver->unixtime;
                slave->postFunction([conn](client *) {
                    connSetWriteHandler(conn, rdbPipeWriteHandler);
                });
            }
            stillAlive++;
        }

        if (stillAlive == 0) {
            serverLog(LL_WARNING,"Diskless rdb transfer, last replica dropped, killing fork child.");
            killRDBChild();
        }
        /*  Remove the pipe read handler if at least one write handler was set. */
        if (g_pserver->rdb_pipe_numconns_writing || stillAlive == 0) {
            aeDeleteFileEvent(eventLoop, g_pserver->rdb_pipe_read, AE_READABLE);
            break;
        }
    }
}

/* This function is called at the end of every background saving,
 * or when the replication RDB transfer strategy is modified from
 * disk to socket or the other way around.
 *
 * The goal of this function is to handle slaves waiting for a successful
 * background saving in order to perform non-blocking synchronization, and
 * to schedule a new BGSAVE if there are slaves that attached while a
 * BGSAVE was in progress, but it was not a good one for replication (no
 * other replica was accumulating differences).
 *
 * The argument bgsaveerr is C_OK if the background saving succeeded
 * otherwise C_ERR is passed to the function.
 * The 'type' argument is the type of the child that terminated
 * (if it had a disk or socket target). */
void updateSlavesWaitingBgsave(int bgsaveerr, int type)
{
    listNode *ln;
    listIter li;
    serverAssert(GlobalLocksAcquired());

    listRewind(g_pserver->slaves,&li);
    while((ln = listNext(&li))) {
        client *replica = (client*)ln->value;

        std::unique_lock<fastlock> ul(replica->lock);

        if (replica->replstate == SLAVE_STATE_WAIT_BGSAVE_END) {
            struct redis_stat buf;

            if (bgsaveerr != C_OK) {
                ul.unlock();
                freeClientAsync(replica);
                serverLog(LL_WARNING,"SYNC failed. BGSAVE child returned an error");
                continue;
            }

            /* If this was an RDB on disk save, we have to prepare to send
             * the RDB from disk to the replica socket. Otherwise if this was
             * already an RDB -> Slaves socket transfer, used in the case of
             * diskless replication, our work is trivial, we can just put
             * the replica online. */
            if (type == RDB_CHILD_TYPE_SOCKET) {
                serverLog(LL_NOTICE,
                    "Streamed RDB transfer with replica %s succeeded (socket). Waiting for REPLCONF ACK from slave to enable streaming",
                        replicationGetSlaveName(replica));
                /* Note: we wait for a REPLCONF ACK message from the replica in
                 * order to really put it online (install the write handler
                 * so that the accumulated data can be transferred). However
                 * we change the replication state ASAP, since our slave
                 * is technically online now.
                 *
                 * So things work like that:
                 *
                 * 1. We end trasnferring the RDB file via socket.
                 * 2. The replica is put ONLINE but the write handler
                 *    is not installed.
                 * 3. The replica however goes really online, and pings us
                 *    back via REPLCONF ACK commands.
                 * 4. Now we finally install the write handler, and send
                 *    the buffers accumulated so far to the replica.
                 *
                 * But why we do that? Because the replica, when we stream
                 * the RDB directly via the socket, must detect the RDB
                 * EOF (end of file), that is a special random string at the
                 * end of the RDB (for streamed RDBs we don't know the length
                 * in advance). Detecting such final EOF string is much
                 * simpler and less CPU intensive if no more data is sent
                 * after such final EOF. So we don't want to glue the end of
                 * the RDB trasfer with the start of the other replication
                 * data. */
                replica->replstate = SLAVE_STATE_ONLINE;
                replica->repl_put_online_on_ack = 1;
                replica->repl_ack_time = g_pserver->unixtime; /* Timeout otherwise. */
            } else {
                if ((replica->repldbfd = open(g_pserver->rdb_filename,O_RDONLY)) == -1 ||
                    redis_fstat(replica->repldbfd,&buf) == -1) {
                    ul.unlock();
                    freeClientAsync(replica);
                    serverLog(LL_WARNING,"SYNC failed. Can't open/stat DB after BGSAVE: %s", strerror(errno));
                    continue;
                }
                replica->repldboff = 0;
                replica->repldbsize = buf.st_size;
                replica->replstate = SLAVE_STATE_SEND_BULK;
                replica->replpreamble = sdscatprintf(sdsempty(),"$%lld\r\n",
                    (unsigned long long) replica->repldbsize);

                if (FCorrectThread(replica))
                {
                    connSetWriteHandler(replica->conn,NULL);
                    if (connSetWriteHandler(replica->conn,sendBulkToSlave) == C_ERR) {
                        ul.unlock();
                        freeClient(replica);
                        continue;
                    }
                }
                else
                {
                    replica->postFunction([](client *replica) {
                        connSetWriteHandler(replica->conn,NULL);
                        if (connSetWriteHandler(replica->conn,sendBulkToSlave) == C_ERR) {
                            freeClient(replica);
                        }
                    });
                }
            }
        }
    }
}

/* Save the replid of yourself and any connected masters to storage.
 * Returns if no storage provider is used. */
void saveMasterStatusToStorage(bool fShutdown)
{
    if (!g_pserver->m_pstorageFactory || !g_pserver->metadataDb) return;

    g_pserver->metadataDb->insert("repl-id", 7, g_pserver->replid, sizeof(g_pserver->replid), true);
    if (fShutdown)
        g_pserver->metadataDb->insert("repl-offset", 11, &g_pserver->master_repl_offset, sizeof(g_pserver->master_repl_offset), true);
    else
        g_pserver->metadataDb->erase("repl-offset", 11);

    if (g_pserver->fActiveReplica || (!listLength(g_pserver->masters) && g_pserver->repl_backlog)) {
        int zero = 0;
        g_pserver->metadataDb->insert("repl-stream-db", 14, g_pserver->replicaseldb == -1 ? &zero : &g_pserver->replicaseldb,
                                        sizeof(g_pserver->replicaseldb), true);
    } else {
        struct redisMaster *miFirst = (redisMaster*)(listLength(g_pserver->masters) ? listNodeValue(listFirst(g_pserver->masters)) : NULL);

        if (miFirst && miFirst->master) {
            g_pserver->metadataDb->insert("repl-stream-db", 14, &miFirst->master->db->id, sizeof(miFirst->master->db->id), true);
        }
        else if (miFirst && miFirst->cached_master) {
            g_pserver->metadataDb->insert("repl-stream-db", 14, &miFirst->cached_master->db->id, sizeof(miFirst->cached_master->db->id), true);
        }
    }

    if (listLength(g_pserver->masters) == 0) {
        g_pserver->metadataDb->insert("repl-masters", 12, (void*)"", 0, true);
        return;
    }
    sds val = sds(sdsempty());
    listNode *ln;
    listIter li;
    redisMaster *mi;
    listRewind(g_pserver->masters,&li);
    while((ln = listNext(&li)) != NULL) {
        mi = (redisMaster*)listNodeValue(ln);
        if (mi->masterhost == NULL) {
            // if we don't know the host, no reason to save
            continue;
        }
        if (!mi->master) {
            // If master client is not available, use info from master struct - better than nothing
            if (mi->master_replid[0] == 0) {
                // if replid is null, there's no reason to save it
                continue;
            }
            val = sdscatfmt(val, "%s:%I:%s:%i;", mi->master_replid,
                mi->master_initial_offset,
                mi->masterhost,
                mi->masterport);
        }
        else {
            if (mi->master->replid[0] == 0) {
                // if replid is null, there's no reason to save it
                continue;
            }
            val = sdscatfmt(val, "%s:%I:%s:%i;", mi->master->replid,
                mi->master->reploff,
                mi->masterhost,
                mi->masterport);
        }
    }
    g_pserver->metadataDb->insert("repl-masters", 12, (void*)val, sdslen(val), true);
    sdsfree(val);
}

/* Change the current instance replication ID with a new, random one.
 * This will prevent successful PSYNCs between this master and other
 * slaves, so the command should be called when something happens that
 * alters the current story of the dataset. */
void changeReplicationId(void) {
    getRandomHexChars(g_pserver->replid,CONFIG_RUN_ID_SIZE);
    g_pserver->replid[CONFIG_RUN_ID_SIZE] = '\0';
    saveMasterStatusToStorage(false);
}

/* Clear (invalidate) the secondary replication ID. This happens, for
 * example, after a full resynchronization, when we start a new replication
 * history. */
void clearReplicationId2(void) {
    memset(g_pserver->replid2,'0',sizeof(g_pserver->replid));
    g_pserver->replid2[CONFIG_RUN_ID_SIZE] = '\0';
    g_pserver->second_replid_offset = -1;
}

/* Use the current replication ID / offset as secondary replication
 * ID, and change the current one in order to start a new history.
 * This should be used when an instance is switched from replica to master
 * so that it can serve PSYNC requests performed using the master
 * replication ID. */
void shiftReplicationId(void) {
    memcpy(g_pserver->replid2,g_pserver->replid,sizeof(g_pserver->replid));
    /* We set the second replid offset to the master offset + 1, since
     * the replica will ask for the first byte it has not yet received, so
     * we need to add one to the offset: for example if, as a replica, we are
     * sure we have the same history as the master for 50 bytes, after we
     * are turned into a master, we can accept a PSYNC request with offset
     * 51, since the replica asking has the same history up to the 50th
     * byte, and is asking for the new bytes starting at offset 51. */
    g_pserver->second_replid_offset = g_pserver->master_repl_offset+1;
    changeReplicationId();
    serverLog(LL_WARNING,"Setting secondary replication ID to %s, valid up to offset: %lld. New replication ID is %s", g_pserver->replid2, g_pserver->second_replid_offset, g_pserver->replid);
}

/* ----------------------------------- SLAVE -------------------------------- */

/* Returns 1 if the given replication state is a handshake state,
 * 0 otherwise. */
int slaveIsInHandshakeState(redisMaster *mi) {
    return mi->repl_state >= REPL_STATE_RECEIVE_PING_REPLY &&
           mi->repl_state <= REPL_STATE_RECEIVE_PSYNC_REPLY;
}

/* Avoid the master to detect the replica is timing out while loading the
 * RDB file in initial synchronization. We send a single newline character
 * that is valid protocol but is guaranteed to either be sent entirely or
 * not, since the byte is indivisible.
 *
 * The function is called in two contexts: while we flush the current
 * data with emptyDb(), and while we load the new data received as an
 * RDB file from the master. */
void replicationSendNewlineToMaster(redisMaster *mi) {
    static time_t newline_sent;
    if (time(NULL) != newline_sent) {
        newline_sent = time(NULL);
        /* Pinging back in this stage is best-effort. */
        if (mi->repl_transfer_s) connWrite(mi->repl_transfer_s, "\n", 1);
    }
}

/* Callback used by emptyDb() while flushing away old data to load
 * the new dataset received by the master. */
void replicationEmptyDbCallback(void *privdata) {
    UNUSED(privdata);
    listIter li;
    listNode *ln;
    listRewind(g_pserver->masters, &li);
    while ((ln = listNext(&li)))
    {
        replicationSendNewlineToMaster((redisMaster*)listNodeValue(ln));
    }
}

/* Once we have a link with the master and the synchronization was
 * performed, this function materializes the master client we store
 * at g_pserver->master, starting from the specified file descriptor. */
void replicationCreateMasterClient(redisMaster *mi, connection *conn, int dbid) {
    serverAssert(mi->master == nullptr);
    mi->master = createClient(conn, serverTL - g_pserver->rgthreadvar);
    if (conn)
    {
        serverAssert(connGetPrivateData(mi->master->conn) == mi->master);
        connSetReadHandler(mi->master->conn, readQueryFromClient, true);
    }

    /**
     * Important note:
     * The CLIENT_DENY_BLOCKING flag is not, and should not, be set here.
     * For commands like BLPOP, it makes no sense to block the master
     * connection, and such blocking attempt will probably cause deadlock and
     * break the replication. We consider such a thing as a bug because
     * commands as BLPOP should never be sent on the replication link.
     * A possible use-case for blocking the replication link is if a module wants
     * to pass the execution to a background thread and unblock after the
     * execution is done. This is the reason why we allow blocking the replication
     * connection. */
    mi->master->flags |= CLIENT_MASTER;

    mi->master->authenticated = 1;
    mi->master->reploff = mi->master_initial_offset;
    mi->master->read_reploff = mi->master->reploff;
    mi->master->user = NULL; /* This client can do everything. */
    
    memcpy(mi->master->uuid, mi->master_uuid, UUID_BINARY_LEN);
    memset(mi->master_uuid, 0, UUID_BINARY_LEN); // make sure people don't use this temp storage buffer

    memcpy(mi->master->replid, mi->master_replid,
        sizeof(mi->master_replid));
    /* If master offset is set to -1, this master is old and is not
     * PSYNC capable, so we flag it accordingly. */
    if (mi->master->reploff == -1)
        mi->master->flags |= CLIENT_PRE_PSYNC;
    if (dbid != -1) selectDb(mi->master,dbid);
}

void replicationCreateCachedMasterClone(redisMaster *mi) {
    serverAssert(mi->master != nullptr);
    serverLog(LL_NOTICE, "Creating cache clone of our master");
    if ((mi->master->flags & (CLIENT_PROTOCOL_ERROR|CLIENT_BLOCKED))) {
        freeClientAsync(mi->master);
        mi->master = nullptr;
        return;
    }
    client *c = createClient(nullptr, ielFromEventLoop(serverTL->el));

    c->flags |= mi->master->flags & ~(CLIENT_PENDING_WRITE | CLIENT_UNBLOCKED | CLIENT_CLOSE_ASAP);
    c->authenticated = mi->master->authenticated;
    c->reploff = mi->master->reploff;
    c->read_reploff = mi->master->read_reploff;
    c->user = mi->master->user;

    c->replstate = mi->master->replstate;
    c->master_error = mi->master->master_error;
    c->psync_initial_offset = mi->master->psync_initial_offset;
    c->repldboff = mi->master->repldboff;
    c->repldbsize = mi->master->repldbsize;

    memcpy(c->uuid, mi->master->uuid, UUID_BINARY_LEN);
    memcpy(c->replid, mi->master->replid,
        sizeof(mi->master->replid));
    selectDb(c, mi->master->db->id);

    // Free the old one
    mi->master->flags &= ~CLIENT_MASTER;
    freeClientAsync(mi->master);

    // Now make this one the cache
    mi->master = c;
    replicationCacheMaster(mi, c);
}

/* This function will try to re-enable the AOF file after the
 * master-replica synchronization: if it fails after multiple attempts
 * the replica cannot be considered reliable and exists with an
 * error. */
void restartAOFAfterSYNC() {
    unsigned int tries, max_tries = 10;
    for (tries = 0; tries < max_tries; ++tries) {
        if (startAppendOnly() == C_OK) break;
        serverLog(LL_WARNING,
            "Failed enabling the AOF after successful master synchronization! "
            "Trying it again in one second.");
        sleep(1);
    }
    if (tries == max_tries) {
        serverLog(LL_WARNING,
            "FATAL: this replica instance finished the synchronization with "
            "its master, but the AOF can't be turned on. Exiting now.");
        exit(1);
    }
}

static int useDisklessLoad() {
    /* compute boolean decision to use diskless load */
    int enabled = g_pserver->repl_diskless_load == REPL_DISKLESS_LOAD_SWAPDB ||
           (g_pserver->repl_diskless_load == REPL_DISKLESS_LOAD_WHEN_DB_EMPTY && dbTotalServerKeyCount()==0);
    /* Check all modules handle read errors, otherwise it's not safe to use diskless load. */
    if (enabled && !moduleAllDatatypesHandleErrors()) {
        serverLog(LL_WARNING,
            "Skipping diskless-load because there are modules that don't handle read errors.");
        enabled = 0;
    }
    return enabled;
}

/* Helper function for readSyncBulkPayload() to make backups of the current
 * databases before socket-loading the new ones. The backups may be restored
 * by disklessLoadRestoreBackup or freed by disklessLoadDiscardBackup later. */
const dbBackup *disklessLoadMakeBackup(void) {
    return backupDb();
}

/* Helper function for readSyncBulkPayload(): when replica-side diskless
 * database loading is used, Redis makes a backup of the existing databases
 * before loading the new ones from the socket.
 *
 * If the socket loading went wrong, we want to restore the old backups
 * into the server databases. */
void disklessLoadRestoreBackup(const dbBackup *buckup) {
    restoreDbBackup(buckup);
}

/* Helper function for readSyncBulkPayload() to discard our old backups
 * when the loading succeeded. */
void disklessLoadDiscardBackup(const dbBackup *buckup, int flag) {
    discardDbBackup(buckup, flag, replicationEmptyDbCallback);
}

size_t parseCount(const char *rgch, size_t cch, long long *pvalue) {
    size_t cchNumeral = 0;

    if (cch > cchNumeral+1 && rgch[cchNumeral+1] == '-') ++cchNumeral;
    while ((cch > (cchNumeral+1)) && isdigit(rgch[1 + cchNumeral])) ++cchNumeral;

    if (cch < (cchNumeral+1+2)) { // +2 is for the \r\n we expect
        throw true; // continuable
    }

    if (rgch[cchNumeral+1] != '\r' || rgch[cchNumeral+2] != '\n') {
        serverLog(LL_WARNING, "Bad protocol from MASTER: %s", rgch);
        throw false;
    }

    if (!string2ll(rgch+1, cchNumeral, pvalue)) {
        serverLog(LL_WARNING, "Bad protocol from MASTER: %s", rgch);
        throw false;
    }

    return cchNumeral + 3;
}

bool readSnapshotBulkPayload(connection *conn, redisMaster *mi, rdbSaveInfo &rsi) {
    int fUpdate = g_pserver->fActiveReplica || g_pserver->enable_multimaster;
    serverAssert(GlobalLocksAcquired());
    serverAssert(mi->master == nullptr);
    bool fFinished = false;

    if (mi->bulkreadBuffer == nullptr) {
        mi->bulkreadBuffer = sdsempty();
        mi->parseState = new SnapshotPayloadParseState();
        if (g_pserver->aof_state != AOF_OFF) stopAppendOnly();
        if (!fUpdate) {
            int empty_db_flags = g_pserver->repl_slave_lazy_flush ? EMPTYDB_ASYNC :
                EMPTYDB_NO_FLAGS;
            serverLog(LL_NOTICE, "MASTER <-> REPLICA sync: Flushing old data");
            emptyDb(-1,empty_db_flags,replicationEmptyDbCallback);
            for (int idb = 0; idb < cserver.dbnum; ++idb) {
                aeAcquireLock();
                g_pserver->db[idb]->processChanges(false);
                aeReleaseLock();
                g_pserver->db[idb]->commitChanges();
                g_pserver->db[idb]->trackChanges(false);
            }
        }
    }

    for (int iter = 0; iter < 10; ++iter) {
        if (mi->parseState->shouldThrottle())
            return false;

        auto readlen = PROTO_IOBUF_LEN;
        auto qblen = sdslen(mi->bulkreadBuffer);
        mi->bulkreadBuffer = sdsMakeRoomFor(mi->bulkreadBuffer, readlen);

        auto nread = connRead(conn, mi->bulkreadBuffer+qblen, readlen);
        if (nread <= 0) {
            if (connGetState(conn) != CONN_STATE_CONNECTED)
                cancelReplicationHandshake(mi, true);
            return false;
        }
        mi->repl_transfer_lastio = g_pserver->unixtime;
        sdsIncrLen(mi->bulkreadBuffer,nread);

        size_t offset = 0;

        try {
            if (sdslen(mi->bulkreadBuffer) > cserver.client_max_querybuf_len) {
                throw "Full Sync Streaming Buffer Exceeded (increase client_max_querybuf_len)";
            }

            while (sdslen(mi->bulkreadBuffer) > offset) {
                // Pop completed items
                mi->parseState->trimState();
                if (mi->parseState->depth() == 0)
                    break;

                if (mi->bulkreadBuffer[offset] == '*') {
                    // Starting an array
                    long long arraySize;

                    // Lets read the array length
                    offset += parseCount(mi->bulkreadBuffer + offset, sdslen(mi->bulkreadBuffer) - offset, &arraySize);
                    if (arraySize < 0)
                        throw "Invalid array size";

                    mi->parseState->pushArray(arraySize);
                } else if (mi->bulkreadBuffer[offset] == '$') {
                    // Loading in a string
                    long long payloadsize = 0;

                    // Lets read the string length
                    size_t offsetCount = parseCount(mi->bulkreadBuffer + offset, sdslen(mi->bulkreadBuffer) - offset, &payloadsize);
                    if (payloadsize < 0)
                        throw "Invalid array size";

                    // OK we know how long the string is, now lets make sure the payload is here.
                    if (sdslen(mi->bulkreadBuffer) < (offset + offsetCount + payloadsize + 2)) {
                        goto LContinue; // wait for more data (note: we could throw true here, but throw is way more expensive)
                    }

                    mi->parseState->pushValue(mi->bulkreadBuffer + offset + offsetCount, payloadsize);

                    // On to the next one
                    offset += offsetCount + payloadsize + 2;
                } else if (mi->bulkreadBuffer[offset] == ':') {
                    // Numeral
                    long long value;

                    size_t offsetValue = parseCount(mi->bulkreadBuffer + offset, sdslen(mi->bulkreadBuffer) - offset, &value);

                    mi->parseState->pushValue(value);
                    offset += offsetValue;
                } else {
                    serverLog(LL_WARNING, "Bad protocol from MASTER: %s", mi->bulkreadBuffer+offset);
                    cancelReplicationHandshake(mi, true);
                    return false;
                }
            }

            sdsrange(mi->bulkreadBuffer, offset, -1);
            offset = 0;

            // Cleanup the remaining stack
            mi->parseState->trimState();

            if (mi->parseState->depth() != 0)
                return false;

            static_assert(sizeof(long) == sizeof(long long),"");
            rsi.repl_stream_db = mi->parseState->getMetaDataLongLong("repl-stream-db");
            rsi.repl_offset = mi->parseState->getMetaDataLongLong("repl-offset");
            sds str = mi->parseState->getMetaDataStr("repl-id");
            if (sdslen(str) == CONFIG_RUN_ID_SIZE+1) {
                memcpy(rsi.repl_id, str, CONFIG_RUN_ID_SIZE+1);
            }

            fFinished = true;
            break;  // We're done!!!
        }
        catch (const char *sz) {
            serverLog(LL_WARNING, "%s", sz);
            cancelReplicationHandshake(mi, true);
            return false;
        } catch (bool fContinue) {
            if (!fContinue) {
                cancelReplicationHandshake(mi, true);
                return false;
            }
        }
    LContinue:
        sdsrange(mi->bulkreadBuffer, offset, -1);
    }

    if (!fFinished)
        return false;

    serverLog(LL_NOTICE, "Fast sync complete");
    delete mi->parseState;
    mi->parseState = nullptr;
    return true;
}

/* Asynchronously read the SYNC payload we receive from a master */
#define REPL_MAX_WRITTEN_BEFORE_FSYNC (1024*1024*8) /* 8 MB */
bool readSyncBulkPayloadRdb(connection *conn, redisMaster *mi, rdbSaveInfo &rsi, int &usemark) {
    char buf[PROTO_IOBUF_LEN];
    ssize_t nread, readlen, nwritten;
    int use_diskless_load = useDisklessLoad();
    const dbBackup *diskless_load_backup = NULL;
    rsi.fForceSetKey = !!g_pserver->fActiveReplica;
    int empty_db_flags = g_pserver->repl_slave_lazy_flush ? EMPTYDB_ASYNC :
                                                        EMPTYDB_NO_FLAGS;
    off_t left;
    // Should we update our database, or create from scratch?
    int fUpdate = g_pserver->fActiveReplica || g_pserver->enable_multimaster;

    serverAssert(GlobalLocksAcquired());
    serverAssert(mi->master == nullptr);

    /* Static vars used to hold the EOF mark, and the last bytes received
     * from the server: when they match, we reached the end of the transfer. */
    static char eofmark[CONFIG_RUN_ID_SIZE];
    static char lastbytes[CONFIG_RUN_ID_SIZE];

    /* If repl_transfer_size == -1 we still have to read the bulk length
     * from the master reply. */
    if (mi->repl_transfer_size == -1) {
        if (connSyncReadLine(conn,buf,1024,g_pserver->repl_syncio_timeout*1000) == -1) {
            serverLog(LL_WARNING,
                "I/O error reading bulk count from MASTER: %s",
                strerror(errno));
            goto error;
        }

        if (buf[0] == '-') {
            serverLog(LL_WARNING,
                "MASTER aborted replication with an error: %s",
                buf+1);
            goto error;
        } else if (buf[0] == '\0') {
            /* At this stage just a newline works as a PING in order to take
             * the connection live. So we refresh our last interaction
             * timestamp. */
            mi->repl_transfer_lastio = g_pserver->unixtime;
            return false;
        } else if (buf[0] != '$') {
            serverLog(LL_WARNING,"Bad protocol from MASTER, the first byte is not '$' (we received '%s'), are you sure the host and port are right?", buf);
            goto error;
        }

        /* There are two possible forms for the bulk payload. One is the
         * usual $<count> bulk format. The other is used for diskless transfers
         * when the master does not know beforehand the size of the file to
         * transfer. In the latter case, the following format is used:
         *
         * $EOF:<40 bytes delimiter>
         *
         * At the end of the file the announced delimiter is transmitted. The
         * delimiter is long and random enough that the probability of a
         * collision with the actual file content can be ignored. */
        if (strncmp(buf+1,"EOF:",4) == 0 && strlen(buf+5) >= CONFIG_RUN_ID_SIZE) {
            usemark = 1;
            memcpy(eofmark,buf+5,CONFIG_RUN_ID_SIZE);
            memset(lastbytes,0,CONFIG_RUN_ID_SIZE);
            /* Set any repl_transfer_size to avoid entering this code path
             * at the next call. */
            mi->repl_transfer_size = 0;
            serverLog(LL_NOTICE,
                "MASTER <-> REPLICA sync: receiving streamed RDB from master with EOF %s",
                use_diskless_load? "to parser":"to disk");
        } else {
            usemark = 0;
            mi->repl_transfer_size = strtol(buf+1,NULL,10);
            serverLog(LL_NOTICE,
                "MASTER <-> REPLICA sync: receiving %lld bytes from master %s",
                (long long) mi->repl_transfer_size,
                use_diskless_load? "to parser":"to disk");
        }
        return false;
    }

    if (!use_diskless_load) {
        /* Read the data from the socket, store it to a file and search
         * for the EOF. */
        if (usemark) {
            readlen = sizeof(buf);
        } else {
            left = mi->repl_transfer_size - mi->repl_transfer_read;
            readlen = (left < (signed)sizeof(buf)) ? left : (signed)sizeof(buf);
        }

        nread = connRead(conn,buf,readlen);
        if (nread <= 0) {
            if (connGetState(conn) == CONN_STATE_CONNECTED) {
                /* equivalent to EAGAIN */
                return false;
            }
            serverLog(LL_WARNING,"I/O error trying to sync with MASTER: %s",
                (nread == -1) ? strerror(errno) : "connection lost");
            cancelReplicationHandshake(mi, true);
            return false;
        }
        g_pserver->stat_net_input_bytes += nread;

        /* When a mark is used, we want to detect EOF asap in order to avoid
         * writing the EOF mark into the file... */
        int eof_reached = 0;

        if (usemark) {
            /* Update the last bytes array, and check if it matches our
             * delimiter. */
            if (nread >= CONFIG_RUN_ID_SIZE) {
                memcpy(lastbytes,buf+nread-CONFIG_RUN_ID_SIZE,
                       CONFIG_RUN_ID_SIZE);
            } else {
                int rem = CONFIG_RUN_ID_SIZE-nread;
                memmove(lastbytes,lastbytes+nread,rem);
                memcpy(lastbytes+rem,buf,nread);
            }
            if (memcmp(lastbytes,eofmark,CONFIG_RUN_ID_SIZE) == 0)
                eof_reached = 1;
        }

        /* Update the last I/O time for the replication transfer (used in
         * order to detect timeouts during replication), and write what we
         * got from the socket to the dump file on disk. */
        mi->repl_transfer_lastio = g_pserver->unixtime;
        if ((nwritten = write(mi->repl_transfer_fd,buf,nread)) != nread) {
            serverLog(LL_WARNING,
                "Write error or short write writing to the DB dump file "
                "needed for MASTER <-> REPLICA synchronization: %s",
                (nwritten == -1) ? strerror(errno) : "short write");
            goto error;
        }
        mi->repl_transfer_read += nread;

        /* Delete the last 40 bytes from the file if we reached EOF. */
        if (usemark && eof_reached) {
            if (ftruncate(mi->repl_transfer_fd,
                mi->repl_transfer_read - CONFIG_RUN_ID_SIZE) == -1)
            {
                serverLog(LL_WARNING,
                    "Error truncating the RDB file received from the master "
                    "for SYNC: %s", strerror(errno));
                goto error;
            }
        }

        /* Sync data on disk from time to time, otherwise at the end of the
         * transfer we may suffer a big delay as the memory buffers are copied
         * into the actual disk. */
        if (mi->repl_transfer_read >=
            mi->repl_transfer_last_fsync_off + REPL_MAX_WRITTEN_BEFORE_FSYNC)
        {
            off_t sync_size = mi->repl_transfer_read -
                              mi->repl_transfer_last_fsync_off;
            rdb_fsync_range(mi->repl_transfer_fd,
                mi->repl_transfer_last_fsync_off, sync_size);
            mi->repl_transfer_last_fsync_off += sync_size;
        }

        /* Check if the transfer is now complete */
        if (!usemark) {
            if (mi->repl_transfer_read == mi->repl_transfer_size)
                eof_reached = 1;
        }

        /* If the transfer is yet not complete, we need to read more, so
         * return ASAP and wait for the handler to be called again. */
        if (!eof_reached) return false;
    }

    /* We reach this point in one of the following cases:
     *
     * 1. The replica is using diskless replication, that is, it reads data
     *    directly from the socket to the Redis memory, without using
     *    a temporary RDB file on disk. In that case we just block and
     *    read everything from the socket.
     *
     * 2. Or when we are done reading from the socket to the RDB file, in
     *    such case we want just to read the RDB file in memory. */

    /* We need to stop any AOF rewriting child before flusing and parsing
     * the RDB, otherwise we'll create a copy-on-write disaster. */
    if (g_pserver->aof_state != AOF_OFF) stopAppendOnly();

    if (use_diskless_load &&
            g_pserver->repl_diskless_load == REPL_DISKLESS_LOAD_SWAPDB)
    {
        /* Create a backup of g_pserver->db[] and initialize to empty
         * dictionaries. */
        diskless_load_backup = disklessLoadMakeBackup();
    }
    
    /* We call to emptyDb even in case of REPL_DISKLESS_LOAD_SWAPDB
     * (Where disklessLoadMakeBackup left g_pserver->db empty) because we
     * want to execute all the auxiliary logic of emptyDb (Namely,
     * fire module events) */
    if (!fUpdate) {
        serverLog(LL_NOTICE, "MASTER <-> REPLICA sync: Flushing old data");
        emptyDb(-1,empty_db_flags,replicationEmptyDbCallback);
    }

    /* Before loading the DB into memory we need to delete the readable
     * handler, otherwise it will get called recursively since
     * rdbLoad() will call the event loop to process events from time to
     * time for non blocking loading. */
    connSetReadHandler(conn, NULL);
    serverLog(LL_NOTICE, "MASTER <-> REPLICA sync: Loading DB in memory");
    
    if (use_diskless_load) {
        rio rdb;
        rioInitWithConn(&rdb,conn,mi->repl_transfer_size);

        /* Put the socket in blocking mode to simplify RDB transfer.
         * We'll restore it when the RDB is received. */
        connBlock(conn);
        connRecvTimeout(conn, g_pserver->repl_timeout*1000);
        startLoading(mi->repl_transfer_size, RDBFLAGS_REPLICATION);

        if (rdbLoadRio(&rdb,RDBFLAGS_REPLICATION,&rsi) != C_OK) {
            /* RDB loading failed. */
            stopLoading(0);
            serverLog(LL_WARNING,
                "Failed trying to load the MASTER synchronization DB "
                "from socket");
            cancelReplicationHandshake(mi,true);
            rioFreeConn(&rdb, NULL);

            /* Remove the half-loaded data in case we started with
             * an empty replica. */
            emptyDb(-1,empty_db_flags,replicationEmptyDbCallback);

            if (g_pserver->repl_diskless_load == REPL_DISKLESS_LOAD_SWAPDB) {
                /* Restore the backed up databases. */
                disklessLoadRestoreBackup(diskless_load_backup);
            }

            /* Note that there's no point in restarting the AOF on SYNC
             * failure, it'll be restarted when sync succeeds or the replica
             * gets promoted. */
            return false;
        }
        if (g_pserver->fActiveReplica) updateActiveReplicaMastersFromRsi(&rsi);

        /* RDB loading succeeded if we reach this point. */
        if (g_pserver->repl_diskless_load == REPL_DISKLESS_LOAD_SWAPDB) {
            /* Delete the backup databases we created before starting to load
             * the new RDB. Now the RDB was loaded with success so the old
             * data is useless. */
            disklessLoadDiscardBackup(diskless_load_backup, empty_db_flags);
        }

        /* Verify the end mark is correct. */
        if (usemark) {
            if (!rioRead(&rdb,buf,CONFIG_RUN_ID_SIZE) ||
                memcmp(buf,eofmark,CONFIG_RUN_ID_SIZE) != 0)
            {
                stopLoading(0);
                serverLog(LL_WARNING,"Replication stream EOF marker is broken");
                cancelReplicationHandshake(mi,true);
                rioFreeConn(&rdb, NULL);
                return false;
            }
        }

        stopLoading(1);

        /* Cleanup and restore the socket to the original state to continue
         * with the normal replication. */
        rioFreeConn(&rdb, NULL);
        connNonBlock(conn);
        connRecvTimeout(conn,0);
    } else {
        /* Ensure background save doesn't overwrite synced data */
        if (g_pserver->FRdbSaveInProgress()) {
            serverLog(LL_NOTICE,
                "Replica is about to load the RDB file received from the "
                "master, but there is a pending RDB child running. "
                "Cancelling RDB the save and removing its temp file to avoid "
                "any race");
            killRDBChild();
        }

        const char *rdb_filename = mi->repl_transfer_tmpfile;

        /* Make sure the new file (also used for persistence) is fully synced
         * (not covered by earlier calls to rdb_fsync_range). */
        if (fsync(mi->repl_transfer_fd) == -1) {
            serverLog(LL_WARNING,
                "Failed trying to sync the temp DB to disk in "
                "MASTER <-> REPLICA synchronization: %s",
                strerror(errno));
            cancelReplicationHandshake(mi,true);
            return false;
        }

        /* Rename rdb like renaming rewrite aof asynchronously. */
        if (!fUpdate) {
            int old_rdb_fd = open(g_pserver->rdb_filename,O_RDONLY|O_NONBLOCK);
            if (rename(mi->repl_transfer_tmpfile,g_pserver->rdb_filename) == -1) {
                serverLog(LL_WARNING,
                    "Failed trying to rename the temp DB into %s in "
                    "MASTER <-> REPLICA synchronization: %s",
                    g_pserver->rdb_filename, strerror(errno));
                cancelReplicationHandshake(mi,true);
                if (old_rdb_fd != -1) close(old_rdb_fd);
                return false;
            }
            rdb_filename = g_pserver->rdb_filename;
            
            /* Close old rdb asynchronously. */
            if (old_rdb_fd != -1) bioCreateCloseJob(old_rdb_fd);
        }

        if (g_pserver->fActiveReplica)
        {
            rsi.mvccMinThreshold = mi->mvccLastSync;
            if (mi->staleKeyMap != nullptr)
                mi->staleKeyMap->clear();
            else
                mi->staleKeyMap = new (MALLOC_LOCAL) std::map<int, std::vector<robj_sharedptr>>();
            rsi.mi = mi;
        }
        if (rdbLoadFile(rdb_filename,&rsi,RDBFLAGS_REPLICATION) != C_OK) {
            serverLog(LL_WARNING,
                "Failed trying to load the MASTER synchronization "
                "DB from disk");
            cancelReplicationHandshake(mi,true);
            if (g_pserver->rdb_del_sync_files && allPersistenceDisabled()) {
                serverLog(LL_NOTICE,"Removing the RDB file obtained from "
                                    "the master. This replica has persistence "
                                    "disabled");
                bg_unlink(g_pserver->rdb_filename);
            }
            /* Note that there's no point in restarting the AOF on sync failure,
               it'll be restarted when sync succeeds or replica promoted. */
            return false;
        }
        if (g_pserver->fActiveReplica) updateActiveReplicaMastersFromRsi(&rsi);

        /* Cleanup. */
        if (g_pserver->rdb_del_sync_files && allPersistenceDisabled()) {
            serverLog(LL_NOTICE,"Removing the RDB file obtained from "
                                "the master. This replica has persistence "
                                "disabled");
            bg_unlink(g_pserver->rdb_filename);
        }
        if (fUpdate)
            unlink(mi->repl_transfer_tmpfile);
        zfree(mi->repl_transfer_tmpfile);
        close(mi->repl_transfer_fd);
        mi->repl_transfer_fd = -1;
        mi->repl_transfer_tmpfile = NULL;
    }

    return true;
error:
    cancelReplicationHandshake(mi,true);
    return false;
}

void readSyncBulkPayload(connection *conn) {
    rdbSaveInfo rsi;
    redisMaster *mi = (redisMaster*)connGetPrivateData(conn);
    static int usemark = 0;
    if (mi == nullptr) {
        // We're about to be free'd so bail out
        return;
    }

    if (mi->isRocksdbSnapshotRepl) {
        if (!readSnapshotBulkPayload(conn, mi, rsi))
            return;
    } else {
        if (!readSyncBulkPayloadRdb(conn, mi, rsi, usemark))
            return;
    }

    /* Final setup of the connected slave <- master link */
    replicationCreateMasterClient(mi,mi->repl_transfer_s,rsi.repl_stream_db);
    if (mi->isRocksdbSnapshotRepl) {
        /* We need to handle the case where the initial querybuf data was read by fast sync */
        /* This should match the work readQueryFromClient would do for a master client */
        mi->master->querybuf = sdscatsds(mi->master->querybuf, mi->bulkreadBuffer);
        sdsfree(mi->bulkreadBuffer);
        mi->bulkreadBuffer = nullptr;

        mi->master->pending_querybuf = sdscatlen(mi->master->pending_querybuf,
            mi->master->querybuf,sdslen(mi->master->querybuf));

        mi->master->read_reploff += sdslen(mi->master->querybuf);
    }
    mi->repl_transfer_s = nullptr;
    mi->repl_state = REPL_STATE_CONNECTED;
    mi->repl_down_since = 0;

    /* Fire the master link modules event. */
    moduleFireServerEvent(REDISMODULE_EVENT_MASTER_LINK_CHANGE,
                          REDISMODULE_SUBEVENT_MASTER_LINK_UP,
                          NULL);

    /* After a full resynchronization we use the replication ID and
     * offset of the master. The secondary ID / offset are cleared since
     * we are starting a new history. */
    if (!g_pserver->fActiveReplica)
    {
        /* After a full resynchroniziation we use the replication ID and
        * offset of the master. The secondary ID / offset are cleared since
        * we are starting a new history. */
        memcpy(g_pserver->replid,mi->master->replid,sizeof(g_pserver->replid));
        g_pserver->master_repl_offset = mi->master->reploff;
        if (g_pserver->repl_batch_offStart >= 0)
            g_pserver->repl_batch_offStart = g_pserver->master_repl_offset;
        saveMasterStatusToStorage(false);
    }
    clearReplicationId2();

    /* Let's create the replication backlog if needed. Slaves need to
     * accumulate the backlog regardless of the fact they have sub-slaves
     * or not, in order to behave correctly if they are promoted to
     * masters after a failover. */
    if (g_pserver->repl_backlog == NULL) runAndPropogateToReplicas(createReplicationBacklog);
    serverLog(LL_NOTICE, "MASTER <-> REPLICA sync: Finished with success");

    if (cserver.supervised_mode == SUPERVISED_SYSTEMD) {
        redisCommunicateSystemd("STATUS=MASTER <-> REPLICA sync: Finished with success. Ready to accept connections in read-write mode.\n");
    }

    /* Send the initial ACK immediately to put this replica in online state. */
    if (usemark || mi->isRocksdbSnapshotRepl) replicationSendAck(mi);

    /* Restart the AOF subsystem now that we finished the sync. This
     * will trigger an AOF rewrite, and when done will start appending
     * to the new file. */
    if (g_pserver->aof_enabled) restartAOFAfterSYNC();
    if (mi->isRocksdbSnapshotRepl)
        readQueryFromClient(conn); // There may be querybuf data we just appeneded
    return;
}

char *receiveSynchronousResponse(redisMaster *mi, connection *conn) {
    char buf[256];
    /* Read the reply from the server. */
    if (connSyncReadLine(conn,buf,sizeof(buf),g_pserver->repl_syncio_timeout*1000) == -1)
    {
        return sdscatprintf(sdsempty(),"-Reading from master: %s",
                strerror(errno));
    }
    mi->repl_transfer_lastio = g_pserver->unixtime;
    return sdsnew(buf);
}

/* Send a pre-formatted multi-bulk command to the connection. */
char* sendCommandRaw(connection *conn, sds cmd) {
    if (connSyncWrite(conn,cmd,sdslen(cmd),g_pserver->repl_syncio_timeout*1000) == -1) {
        return sdscatprintf(sdsempty(),"-Writing to master: %s",
                connGetLastError(conn));
    }
    return NULL;
}

/* Compose a multi-bulk command and send it to the connection.
 * Used to send AUTH and REPLCONF commands to the master before starting the
 * replication.
 *
 * Takes a list of char* arguments, terminated by a NULL argument.
 *
 * The command returns an sds string representing the result of the
 * operation. On error the first byte is a "-".
 */
char *sendCommand(connection *conn, ...) {
    va_list ap;
    sds cmd = sdsempty();
    sds cmdargs = sdsempty();
    size_t argslen = 0;
    char *arg;

    /* Create the command to send to the master, we use redis binary
     * protocol to make sure correct arguments are sent. This function
     * is not safe for all binary data. */
    va_start(ap,conn);
    while(1) {
        arg = va_arg(ap, char*);
        if (arg == NULL) break;
        cmdargs = sdscatprintf(cmdargs,"$%zu\r\n%s\r\n",strlen(arg),arg);
        argslen++;
    }

    cmd = sdscatprintf(cmd,"*%zu\r\n",argslen);
    cmd = sdscatsds(cmd,cmdargs);
    sdsfree(cmdargs);

    va_end(ap);
    char* err = sendCommandRaw(conn, cmd);
    sdsfree(cmd);
    if(err)
        return err;
    return NULL;
}

/* Compose a multi-bulk command and send it to the connection. 
 * Used to send AUTH and REPLCONF commands to the master before starting the
 * replication.
 *
 * argv_lens is optional, when NULL, strlen is used.
 *
 * The command returns an sds string representing the result of the
 * operation. On error the first byte is a "-".
 */
char *sendCommandArgv(connection *conn, int argc, const char **argv, size_t *argv_lens) {
    sds cmd = sdsempty();
    const char *arg;
    int i;

    /* Create the command to send to the master. */
    cmd = sdscatfmt(cmd,"*%i\r\n",argc);
    for (i=0; i<argc; i++) {
        int len;
        arg = argv[i];
        len = argv_lens ? argv_lens[i] : strlen(arg);
        cmd = sdscatfmt(cmd,"$%i\r\n",len);
        cmd = sdscatlen(cmd,arg,len);
        cmd = sdscatlen(cmd,"\r\n",2);
    }
    char* err = sendCommandRaw(conn, cmd);
    sdsfree(cmd);
    if (err)
        return err;
    return NULL;
}

/* Try a partial resynchronization with the master if we are about to reconnect.
 * If there is no cached master structure, at least try to issue a
 * "PSYNC ? -1" command in order to trigger a full resync using the PSYNC
 * command in order to obtain the master replid and the master replication
 * global offset.
 *
 * This function is designed to be called from syncWithMaster(), so the
 * following assumptions are made:
 *
 * 1) We pass the function an already connected socket "fd".
 * 2) This function does not close the file descriptor "fd". However in case
 *    of successful partial resynchronization, the function will reuse
 *    'fd' as file descriptor of the g_pserver->master client structure.
 *
 * The function is split in two halves: if read_reply is 0, the function
 * writes the PSYNC command on the socket, and a new function call is
 * needed, with read_reply set to 1, in order to read the reply of the
 * command. This is useful in order to support non blocking operations, so
 * that we write, return into the event loop, and read when there are data.
 *
 * When read_reply is 0 the function returns PSYNC_WRITE_ERR if there
 * was a write error, or PSYNC_WAIT_REPLY to signal we need another call
 * with read_reply set to 1. However even when read_reply is set to 1
 * the function may return PSYNC_WAIT_REPLY again to signal there were
 * insufficient data to read to complete its work. We should re-enter
 * into the event loop and wait in such a case.
 *
 * The function returns:
 *
 * PSYNC_CONTINUE: If the PSYNC command succeeded and we can continue.
 * PSYNC_FULLRESYNC: If PSYNC is supported but a full resync is needed.
 *                   In this case the master replid and global replication
 *                   offset is saved.
 * PSYNC_NOT_SUPPORTED: If the server does not understand PSYNC at all and
 *                      the caller should fall back to SYNC.
 * PSYNC_WRITE_ERROR: There was an error writing the command to the socket.
 * PSYNC_WAIT_REPLY: Call again the function with read_reply set to 1.
 * PSYNC_TRY_LATER: Master is currently in a transient error condition.
 *
 * Notable side effects:
 *
 * 1) As a side effect of the function call the function removes the readable
 *    event handler from "fd", unless the return value is PSYNC_WAIT_REPLY.
 * 2) g_pserver->master_initial_offset is set to the right value according
 *    to the master reply. This will be used to populate the 'g_pserver->master'
 *    structure replication offset.
 */

#define PSYNC_WRITE_ERROR 0
#define PSYNC_WAIT_REPLY 1
#define PSYNC_CONTINUE 2
#define PSYNC_FULLRESYNC 3
#define PSYNC_NOT_SUPPORTED 4
#define PSYNC_TRY_LATER 5
int slaveTryPartialResynchronization(redisMaster *mi, connection *conn, int read_reply) {
    const char *psync_replid;
    char psync_offset[32];
    sds reply;

    /* Writing half */
    if (!read_reply) {
        /* Initially set master_initial_offset to -1 to mark the current
         * master replid and offset as not valid. Later if we'll be able to do
         * a FULL resync using the PSYNC command we'll set the offset at the
         * right value, so that this information will be propagated to the
         * client structure representing the master into g_pserver->master. */
        mi->master_initial_offset = -1;

        if (mi->cached_master) {
            psync_replid = mi->cached_master->replid;
            snprintf(psync_offset,sizeof(psync_offset),"%lld", mi->cached_master->reploff+1);
            serverLog(LL_NOTICE,"Trying a partial resynchronization (request %s:%s).", psync_replid, psync_offset);
        } else {
            serverLog(LL_NOTICE,"Partial resynchronization not possible (no cached master)");
            psync_replid = "?";
            memcpy(psync_offset,"-1",3);
        }

        /* Issue the PSYNC command, if this is a master with a failover in
         * progress then send the failover argument to the replica to cause it
         * to become a master */
        if (g_pserver->failover_state == FAILOVER_IN_PROGRESS) {
            reply = sendCommand(conn,"PSYNC",psync_replid,psync_offset,"FAILOVER",NULL);
        } else {
            reply = sendCommand(conn,"PSYNC",psync_replid,psync_offset,NULL);
        }

        if (reply != NULL) {
            serverLog(LL_WARNING,"Unable to send PSYNC to master: %s",reply);
            sdsfree(reply);
            connSetReadHandler(conn, NULL);
            return PSYNC_WRITE_ERROR;
        }
        return PSYNC_WAIT_REPLY;
    }

    /* Reading half */
    reply = receiveSynchronousResponse(mi, conn);
    if (sdslen(reply) == 0) {
        /* The master may send empty newlines after it receives PSYNC
         * and before to reply, just to keep the connection alive. */
        sdsfree(reply);
        return PSYNC_WAIT_REPLY;
    }

    connSetReadHandler(conn, NULL);

    if (!strncmp(reply,"+FULLRESYNC",11)) {
        char *replid = NULL, *offset = NULL;

        /* FULL RESYNC, parse the reply in order to extract the replid
         * and the replication offset. */
        replid = strchr(reply,' ');
        if (replid) {
            replid++;
            offset = strchr(replid,' ');
            if (offset) offset++;
        }
        if (!replid || !offset || (offset-replid-1) != CONFIG_RUN_ID_SIZE) {
            serverLog(LL_WARNING,
                "Master replied with wrong +FULLRESYNC syntax.");
            /* This is an unexpected condition, actually the +FULLRESYNC
             * reply means that the master supports PSYNC, but the reply
             * format seems wrong. To stay safe we blank the master
             * replid to make sure next PSYNCs will fail. */
            memset(mi->master_replid,0,CONFIG_RUN_ID_SIZE+1);
        } else {
            memcpy(mi->master_replid, replid, offset-replid-1);
            mi->master_replid[CONFIG_RUN_ID_SIZE] = '\0';
            mi->master_initial_offset = strtoll(offset,NULL,10);
            serverLog(LL_NOTICE,"Full resync from master: %s:%lld",
                mi->master_replid,
                mi->master_initial_offset);
        }
        /* We are going to full resync, discard the cached master structure. */
        replicationDiscardCachedMaster(mi);
        sdsfree(reply);
        return PSYNC_FULLRESYNC;
    }

    if (!strncmp(reply,"+CONTINUE",9)) {
        /* Partial resync was accepted. */
        serverLog(LL_NOTICE,
            "Successful partial resynchronization with master.");

        /* Check the new replication ID advertised by the master. If it
         * changed, we need to set the new ID as primary ID, and set or
         * secondary ID as the old master ID up to the current offset, so
         * that our sub-slaves will be able to PSYNC with us after a
         * disconnection. */
        char *start = reply+10;
        char *end = reply+9;
        while(end[0] != '\r' && end[0] != '\n' && end[0] != '\0') end++;
        if (end-start == CONFIG_RUN_ID_SIZE) {
            char sznew[CONFIG_RUN_ID_SIZE+1];
            memcpy(sznew,start,CONFIG_RUN_ID_SIZE);
            sznew[CONFIG_RUN_ID_SIZE] = '\0';

            if (strcmp(sznew,mi->cached_master->replid)) {
                /* Master ID changed. */
                serverLog(LL_WARNING,"Master replication ID changed to %s",sznew);

                /* Set the old ID as our ID2, up to the current offset+1. */
                memcpy(g_pserver->replid2,mi->cached_master->replid,
                    sizeof(g_pserver->replid2));
                g_pserver->second_replid_offset = g_pserver->master_repl_offset+1;

                if (!g_pserver->fActiveReplica) {
                    /* Update the cached master ID and our own primary ID to the
                     * new one. */
                    memcpy(g_pserver->replid,sznew,sizeof(g_pserver->replid));
                    memcpy(mi->cached_master->replid,sznew,sizeof(g_pserver->replid));

                    /* Disconnect all the replicas: they need to be notified. */
                    disconnectSlaves();
                }
            }
        }

        /* Setup the replication to continue. */
        sdsfree(reply);
        replicationResurrectCachedMaster(mi, conn);

        /* If this instance was restarted and we read the metadata to
         * PSYNC from the persistence file, our replication backlog could
         * be still not initialized. Create it. */
        if (g_pserver->repl_backlog == NULL) runAndPropogateToReplicas(createReplicationBacklog);
        return PSYNC_CONTINUE;
    }

    /* If we reach this point we received either an error (since the master does
     * not understand PSYNC or because it is in a special state and cannot
     * serve our request), or an unexpected reply from the master.
     *
     * Return PSYNC_NOT_SUPPORTED on errors we don't understand, otherwise
     * return PSYNC_TRY_LATER if we believe this is a transient error. */

    if (!strncmp(reply,"-NOMASTERLINK",13) ||
        !strncmp(reply,"-LOADING",8))
    {
        serverLog(LL_NOTICE,
            "Master is currently unable to PSYNC "
            "but should be in the future: %s", reply);
        sdsfree(reply);
        return PSYNC_TRY_LATER;
    }

    if (strncmp(reply,"-ERR",4)) {
        /* If it's not an error, log the unexpected event. */
        serverLog(LL_WARNING,
            "Unexpected reply to PSYNC from master: %s", reply);
    } else {
        serverLog(LL_NOTICE,
            "Master does not support PSYNC or is in "
            "error state (reply: %s)", reply);
    }
    sdsfree(reply);
    replicationDiscardCachedMaster(mi);
    return PSYNC_NOT_SUPPORTED;
}

void parseMasterCapa(redisMaster *mi, sds strcapa)
{
    if (sdslen(strcapa) < 1 || strcapa[0] != '+')
        return;

    char *szStart = strcapa + 1;    // skip the +
    char *pchEnd = szStart;

    mi->isActive = false;
    mi->isRocksdbSnapshotRepl = false;
    for (;;)
    {
        if (*pchEnd == ' ' || *pchEnd == '\0') {
            // Parse the word
            if (strncmp(szStart, "active-replica", pchEnd - szStart) == 0) {
                mi->isActive = true;
            } else if (strncmp(szStart, "rocksdb-snapshot-save", pchEnd - szStart) == 0) {
                mi->isRocksdbSnapshotRepl = true;
            }
            szStart = pchEnd + 1;
        }
        if (*pchEnd == '\0')
            break;
        ++pchEnd;
    }
}

/* This handler fires when the non blocking connect was able to
 * establish a connection with the master. */
void syncWithMaster(connection *conn) {
    serverAssert(GlobalLocksAcquired());
    char tmpfile[256] = {0}, *err = NULL;
    int dfd = -1, maxtries = 5;
    int psync_result;

    redisMaster *mi = (redisMaster*)connGetPrivateData(conn);
    if (mi == nullptr) {
        // We're about to be closed, bail
        return;
    }

    /* If this event fired after the user turned the instance into a master
     * with SLAVEOF NO ONE we must just return ASAP. */
    if (mi->repl_state == REPL_STATE_NONE) {
        connClose(conn);
        return;
    }

    /* Check for errors in the socket: after a non blocking connect() we
     * may find that the socket is in error state. */
    if (connGetState(conn) != CONN_STATE_CONNECTED) {
        serverLog(LL_WARNING,"Error condition on socket for SYNC: %s",
                connGetLastError(conn));
        goto error;
    }

retry_connect:
    /* Send a PING to check the master is able to reply without errors. */
    if (mi->repl_state == REPL_STATE_CONNECTING || mi->repl_state == REPL_STATE_RETRY_NOREPLPING) {
        serverLog(LL_NOTICE,"Non blocking connect for SYNC fired the event.");
        /* Delete the writable event so that the readable event remains
         * registered and we can wait for the PONG reply. */
        connSetReadHandler(conn, syncWithMaster);
        connSetWriteHandler(conn, NULL);
        /* Send the PING, don't check for errors at all, we have the timeout
         * that will take care about this. */
        err = sendCommand(conn,mi->repl_state == REPL_STATE_RETRY_NOREPLPING ? "PING" : "REPLPING",NULL);
        if (err) goto write_error;
        mi->repl_state = REPL_STATE_RECEIVE_PING_REPLY;
        return;
    }

    /* Receive the PONG command. */
    if (mi->repl_state == REPL_STATE_RECEIVE_PING_REPLY) {
        err = receiveSynchronousResponse(mi, conn);

        /* We accept only two replies as valid, a positive +PONG reply
         * (we just check for "+") or an authentication error.
         * Note that older versions of Redis replied with "operation not
         * permitted" instead of using a proper error code, so we test
         * both. */
        if (strncmp(err,"-ERR unknown command",20) == 0) {
            serverLog(LL_NOTICE,"Master does not support REPLPING, sending PING instead...");
            mi->repl_state = REPL_STATE_RETRY_NOREPLPING;
            sdsfree(err);
            err = NULL;
            goto retry_connect;
        } else if (err[0] != '+' &&
            strncmp(err,"-NOAUTH",7) != 0 &&
            strncmp(err,"-NOPERM",7) != 0 &&
            strncmp(err,"-ERR operation not permitted",28) != 0)
        {
            serverLog(LL_WARNING,"Error reply to PING from master: '%s'",err);
            sdsfree(err);
            goto error;
        } else {
            serverLog(LL_NOTICE,
                "Master replied to PING, replication can continue...");
        }
        sdsfree(err);
        err = NULL;
        mi->repl_state = REPL_STATE_SEND_HANDSHAKE;
    }

    if (mi->repl_state == REPL_STATE_SEND_HANDSHAKE) {
        char szUUID[37] = {0};

        /* AUTH with the master if required. */
        if (mi->masterauth) {
            const char *args[3] = {"AUTH",NULL,NULL};
            size_t lens[3] = {4,0,0};
            int argc = 1;
            if (mi->masteruser) {
                args[argc] = mi->masteruser;
                lens[argc] = strlen(mi->masteruser);
                argc++;
            }
            args[argc] = mi->masterauth;
            lens[argc] = sdslen(mi->masterauth);
            argc++;
            err = sendCommandArgv(conn, argc, args, lens);
            if (err) goto write_error;
        }

        /* Set the slave port, so that Master's INFO command can list the
         * slave listening port correctly. */
        {
            int port;
            if (g_pserver->slave_announce_port)
                port = g_pserver->slave_announce_port;
            else if (g_pserver->tls_replication && g_pserver->tls_port)
                port = g_pserver->tls_port;
            else
                port = g_pserver->port;
            sds portstr = sdsfromlonglong(port);
            err = sendCommand(conn,"REPLCONF",
                    "listening-port",portstr, NULL);
            sdsfree(portstr);
            if (err) goto write_error;
        }

        /* Set the slave ip, so that Master's INFO command can list the
         * slave IP address port correctly in case of port forwarding or NAT.
         * Skip REPLCONF ip-address if there is no slave-announce-ip option set. */
        if (g_pserver->slave_announce_ip) {
            err = sendCommand(conn,"REPLCONF",
                    "ip-address",g_pserver->slave_announce_ip, NULL);
            if (err) goto write_error;
        }

        /* Inform the master of our (slave) capabilities.
         *
         * EOF: supports EOF-style RDB transfer for diskless replication.
         * PSYNC2: supports PSYNC v2, so understands +CONTINUE <new repl ID>.
         *
         * The master will ignore capabilities it does not understand. */


        std::vector<const char*> veccapabilities = {
            "REPLCONF",
            "capa","eof",
            "capa","psync2",
            "capa","activeExpire",
        };
        if (g_pserver->m_pstorageFactory && !g_pserver->fActiveReplica && g_pserver->repl_diskless_load != REPL_DISKLESS_LOAD_SWAPDB) {
            veccapabilities.push_back("capa");
            veccapabilities.push_back("rocksdb-snapshot-load");
        }

        err = sendCommandArgv(conn, veccapabilities.size(), veccapabilities.data(), nullptr);
        if (err) goto write_error;

        /* Send UUID */
        memset(mi->master_uuid, 0, UUID_BINARY_LEN);
        uuid_unparse((unsigned char*)cserver.uuid, szUUID);
        err = sendCommand(conn,"REPLCONF","uuid",szUUID,NULL);
        if (err) goto write_error;

        mi->repl_state = REPL_STATE_RECEIVE_AUTH_REPLY;
        return;
    }

    if (mi->repl_state == REPL_STATE_RECEIVE_AUTH_REPLY && !mi->masterauth)
        mi->repl_state = REPL_STATE_RECEIVE_PORT_REPLY;

    /* Receive AUTH reply. */
    if (mi->repl_state == REPL_STATE_RECEIVE_AUTH_REPLY) {
        err = receiveSynchronousResponse(mi, conn);
        if (err[0] == '-') {
            serverLog(LL_WARNING,"Unable to AUTH to MASTER: %s",err);
            sdsfree(err);
            goto error;
        }
        sdsfree(err);
        err = nullptr;
        mi->repl_state = REPL_STATE_RECEIVE_PORT_REPLY;
    }

    /* Receive REPLCONF listening-port reply. */
    if (mi->repl_state == REPL_STATE_RECEIVE_PORT_REPLY) {
        err = receiveSynchronousResponse(mi, conn);
        /* Ignore the error if any, not all the Redis versions support
         * REPLCONF listening-port. */
        if (err[0] == '-') {
            serverLog(LL_NOTICE,"(Non critical) Master does not understand "
                                "REPLCONF listening-port: %s", err);
        }
        sdsfree(err);
        mi->repl_state = REPL_STATE_RECEIVE_IP_REPLY;
        return;
    }

    if (mi->repl_state == REPL_STATE_RECEIVE_IP_REPLY && !g_pserver->slave_announce_ip)
        mi->repl_state = REPL_STATE_RECEIVE_CAPA_REPLY;

    /* Receive REPLCONF ip-address reply. */
    if (mi->repl_state == REPL_STATE_RECEIVE_IP_REPLY) {
        err = receiveSynchronousResponse(mi, conn);
        /* Ignore the error if any, not all the Redis versions support
         * REPLCONF listening-port. */
        if (err[0] == '-') {
            serverLog(LL_NOTICE,"(Non critical) Master does not understand "
                                "REPLCONF ip-address: %s", err);
        }
        sdsfree(err);
        mi->repl_state = REPL_STATE_RECEIVE_CAPA_REPLY;
        return;
    }

    /* Receive CAPA reply. */
    if (mi->repl_state == REPL_STATE_RECEIVE_CAPA_REPLY) {
        err = receiveSynchronousResponse(mi, conn);
        /* Ignore the error if any, not all the Redis versions support
         * REPLCONF capa. */
        if (err[0] == '-') {
            serverLog(LL_NOTICE,"(Non critical) Master does not understand "
                                  "REPLCONF capa: %s", err);
        } else {
            parseMasterCapa(mi, err);
        }
        sdsfree(err);
        err = NULL;
        mi->repl_state = REPL_STATE_RECEIVE_UUID;
    }

    /* Receive UUID */
    if (mi->repl_state == REPL_STATE_RECEIVE_UUID) {
        err = receiveSynchronousResponse(mi, conn);
        if (err[0] == '-') {
            serverLog(LL_WARNING, "non-fatal: Master doesn't understand REPLCONF uuid");
        }
        else {
            if (strlen(err) != 37   // 36-byte UUID string and the leading '+'
                || uuid_parse(err+1, mi->master_uuid) != 0)   
            {
                serverLog(LL_WARNING, "Master replied with a UUID we don't understand");
                sdsfree(err);
                goto error;
            }
        }
        sdsfree(err);
        err = NULL;
        mi->repl_state = REPL_STATE_SEND_PSYNC;
        // fallthrough
    }

    /* Try a partial resynchonization. If we don't have a cached master
     * slaveTryPartialResynchronization() will at least try to use PSYNC
     * to start a full resynchronization so that we get the master replid
     * and the global offset, to try a partial resync at the next
     * reconnection attempt. */
    if (mi->repl_state == REPL_STATE_SEND_PSYNC) {
        if (slaveTryPartialResynchronization(mi,conn,0) == PSYNC_WRITE_ERROR) {
            err = sdsnew("Write error sending the PSYNC command.");
            abortFailover(mi, "Write error to failover target");
            goto write_error;
        }
        mi->repl_state = REPL_STATE_RECEIVE_PSYNC_REPLY;
        return;
    }

    /* If reached this point, we should be in REPL_STATE_RECEIVE_PSYNC. */
    if (mi->repl_state != REPL_STATE_RECEIVE_PSYNC_REPLY) {
        serverLog(LL_WARNING,"syncWithMaster(): state machine error, "
                             "state should be RECEIVE_PSYNC but is %d",
                             mi->repl_state);
        goto error;
    }

    psync_result = slaveTryPartialResynchronization(mi,conn,1);
    if (psync_result == PSYNC_WAIT_REPLY) return; /* Try again later... */

    /* Check the status of the planned failover. We expect PSYNC_CONTINUE,
     * but there is nothing technically wrong with a full resync which
     * could happen in edge cases. */
    if (g_pserver->failover_state == FAILOVER_IN_PROGRESS) {
        if (psync_result == PSYNC_CONTINUE || psync_result == PSYNC_FULLRESYNC) {
            clearFailoverState();
        } else {
            abortFailover(mi, "Failover target rejected psync request");
            return;
        }
    }

    /* If the master is in an transient error, we should try to PSYNC
        * from scratch later, so go to the error path. This happens when
        * the server is loading the dataset or is not connected with its
        * master and so forth. */
    if (psync_result == PSYNC_TRY_LATER) goto error;

    /* Note: if PSYNC does not return WAIT_REPLY, it will take care of
        * uninstalling the read handler from the file descriptor. */

    if (psync_result == PSYNC_CONTINUE) {
        serverLog(LL_NOTICE, "MASTER <-> REPLICA sync: Master accepted a Partial Resynchronization.");
        /* Reset the bulklen information in case it is lingering from the last connection
         * The partial sync will start from the beginning of a command so these should be reset */
        mi->master->reqtype = 0;
        mi->master->multibulklen = 0;
        mi->master->bulklen = -1;
        if (cserver.supervised_mode == SUPERVISED_SYSTEMD) {
            redisCommunicateSystemd("STATUS=MASTER <-> REPLICA sync: Partial Resynchronization accepted. Ready to accept connections in read-write mode.\n");
        }
        return;
    }

    /* PSYNC failed or is not supported: we want our slaves to resync with us
     * as well, if we have any sub-slaves. The master may transfer us an
     * entirely different data set and we have no way to incrementally feed
     * our slaves after that. */
    if (!g_pserver->fActiveReplica)
    {
        disconnectSlavesExcept(mi->master_uuid); /* Force our slaves to resync with us as well. */
        freeReplicationBacklog(); /* Don't allow our chained slaves to PSYNC. */
    }

    /* Fall back to SYNC if needed. Otherwise psync_result == PSYNC_FULLRESYNC
     * and the g_pserver->master_replid and master_initial_offset are
     * already populated. */
    if (psync_result == PSYNC_NOT_SUPPORTED) {
        serverLog(LL_NOTICE,"Retrying with SYNC...");
        if (connSyncWrite(conn,"SYNC\r\n",6,g_pserver->repl_syncio_timeout*1000) == -1) {
            serverLog(LL_WARNING,"I/O error writing to MASTER: %s",
                strerror(errno));
            goto error;
        }
    }

    /* Prepare a suitable temp file for bulk transfer */
    if (!useDisklessLoad()) {
        while(maxtries--) {
            auto dt = std::chrono::system_clock::now().time_since_epoch();
            auto dtMillisecond = std::chrono::duration_cast<std::chrono::milliseconds>(dt);
            snprintf(tmpfile,256,
                "temp-%d.%ld.rdb",(int)dtMillisecond.count(),(long int)getpid());
            dfd = open(tmpfile,O_CREAT|O_WRONLY|O_EXCL,0644);
            if (dfd != -1) break;
            sleep(1);
        }
        if (dfd == -1) {
            serverLog(LL_WARNING,"Opening the temp file needed for MASTER <-> REPLICA synchronization: %s",strerror(errno));
            goto error;
        }
        mi->repl_transfer_fd = dfd;
    }

    /* Setup the non blocking download of the bulk file. */
    serverAssert(mi->master == nullptr);
    if (connSetReadHandler(conn, readSyncBulkPayload)
            == C_ERR)
    {
        char conninfo[CONN_INFO_LEN];
        serverLog(LL_WARNING,
            "Can't create readable event for SYNC: %s (%s)",
            strerror(errno), connGetInfo(conn, conninfo, sizeof(conninfo)));
        goto error;
    }

    mi->repl_state = REPL_STATE_TRANSFER;
    mi->repl_transfer_size = -1;
    mi->repl_transfer_read = 0;
    mi->repl_transfer_last_fsync_off = 0;
    mi->repl_transfer_lastio = g_pserver->unixtime;
    if (mi->repl_transfer_tmpfile)
        zfree(mi->repl_transfer_tmpfile);
    mi->repl_transfer_tmpfile = zstrdup(tmpfile);
    return;

error:
    if (dfd != -1) close(dfd);
    connClose(conn);
    mi->repl_transfer_s = NULL;
    if (mi->repl_transfer_fd != -1)
        close(mi->repl_transfer_fd);
    if (mi->repl_transfer_tmpfile)
        zfree(mi->repl_transfer_tmpfile);
    mi->repl_transfer_tmpfile = NULL;
    mi->repl_transfer_fd = -1;
    mi->repl_state = REPL_STATE_CONNECT;
    return;

write_error: /* Handle sendCommand() errors. */
    serverLog(LL_WARNING,"Sending command to master in replication handshake: %s", err);
    sdsfree(err);
    goto error;
}

int connectWithMaster(redisMaster *mi) {
    serverAssert(mi->master == nullptr);
    mi->repl_transfer_s = g_pserver->tls_replication ? connCreateTLS() : connCreateSocket();
    mi->ielReplTransfer = serverTL - g_pserver->rgthreadvar;
    connSetPrivateData(mi->repl_transfer_s, mi);
    if (connConnect(mi->repl_transfer_s, mi->masterhost, mi->masterport,
                NET_FIRST_BIND_ADDR, syncWithMaster) == C_ERR) {
        int sev = g_pserver->enable_multimaster ? LL_NOTICE : LL_WARNING;   // with multimaster its not unheard of to intentiallionall have downed masters
        serverLog(sev,"Unable to connect to MASTER: %s",
                connGetLastError(mi->repl_transfer_s));
        connClose(mi->repl_transfer_s);
        mi->repl_transfer_s = NULL;
        return C_ERR;
    }


    mi->repl_transfer_lastio = g_pserver->unixtime;
    mi->repl_state = REPL_STATE_CONNECTING;
    serverLog(LL_NOTICE,"MASTER <-> REPLICA sync started");
    return C_OK;
}

/* This function can be called when a non blocking connection is currently
 * in progress to undo it.
 * Never call this function directly, use cancelReplicationHandshake() instead.
 */
void undoConnectWithMaster(redisMaster *mi) {
    auto conn = mi->repl_transfer_s;
    connSetPrivateData(conn, nullptr);
    aePostFunction(g_pserver->rgthreadvar[mi->ielReplTransfer].el, [conn]{
        connClose(conn);
    });
    mi->repl_transfer_s = NULL;
}

/* Abort the async download of the bulk dataset while SYNC-ing with master.
 * Never call this function directly, use cancelReplicationHandshake() instead.
 */
void replicationAbortSyncTransfer(redisMaster *mi) {
    serverAssert(mi->repl_state == REPL_STATE_TRANSFER);
    undoConnectWithMaster(mi);
    if (mi->repl_transfer_fd!=-1) {
        close(mi->repl_transfer_fd);
        bg_unlink(mi->repl_transfer_tmpfile);
        zfree(mi->repl_transfer_tmpfile);
        mi->repl_transfer_tmpfile = NULL;
        mi->repl_transfer_fd = -1;
    }
}

/* This function aborts a non blocking replication attempt if there is one
 * in progress, by canceling the non-blocking connect attempt or
 * the initial bulk transfer.
 *
 * If there was a replication handshake in progress 1 is returned and
 * the replication state (g_pserver->repl_state) set to REPL_STATE_CONNECT.
 *
 * Otherwise zero is returned and no operation is performed at all. */
int cancelReplicationHandshake(redisMaster *mi, int reconnect) {
    if (mi->bulkreadBuffer != nullptr) {
        sdsfree(mi->bulkreadBuffer);
        mi->bulkreadBuffer = nullptr;
    }
    if (mi->parseState) {
        delete mi->parseState;
        mi->parseState = nullptr;
    }

    if (mi->repl_state == REPL_STATE_TRANSFER) {
        replicationAbortSyncTransfer(mi);
        mi->repl_state = REPL_STATE_CONNECT;
    } else if (mi->repl_state == REPL_STATE_CONNECTING ||
               slaveIsInHandshakeState(mi))
    {
        undoConnectWithMaster(mi);
        mi->repl_state = REPL_STATE_CONNECT;
    } else {
        return 0;
    }

    if (!reconnect || g_pserver->fActiveReplica)
        return 1;

    /* try to re-connect without waiting for replicationCron, this is needed
     * for the "diskless loading short read" test. */
    serverLog(LL_NOTICE,"Reconnecting to MASTER %s:%d after failure",
        mi->masterhost, mi->masterport);
    connectWithMaster(mi);

    return 1;
}

void disconnectMaster(redisMaster *mi)
{
    if (mi->master) {
        if (FCorrectThread(mi->master)) {
            // This will cache the master and do all that fancy stuff
            if (!freeClient(mi->master) && mi->master)
                replicationCreateCachedMasterClone(mi);
        } else {
            // We're not on the same thread so we can't use the freeClient method, instead we have to clone the master
            //  and cache that clone
            replicationCreateCachedMasterClone(mi);
        }
    }
}

/* Set replication to the specified master address and port. */
struct redisMaster *replicationAddMaster(char *ip, int port) {
    // pre-reqs: We must not already have a replica in the list with the same tuple
    listIter li;
    listNode *ln;
    listRewind(g_pserver->masters, &li);
    while ((ln = listNext(&li)))
    {
        redisMaster *miCheck = (redisMaster*)listNodeValue(ln);
        if (strcasecmp(miCheck->masterhost, ip)==0 && miCheck->masterport == port)
            return nullptr;
    }

    // Pre-req satisfied, lets continue
    int was_master = listLength(g_pserver->masters) == 0;
    redisMaster *mi = nullptr;
    if (!g_pserver->enable_multimaster && listLength(g_pserver->masters)) {
        serverAssert(listLength(g_pserver->masters) == 1);
        mi = (redisMaster*)listNodeValue(listFirst(g_pserver->masters));
    }
    else
    {
        mi = (redisMaster*)zcalloc(sizeof(redisMaster), MALLOC_LOCAL);
        initMasterInfo(mi);
        listAddNodeTail(g_pserver->masters, mi);
    }

    sdsfree(mi->masterhost);
    mi->masterhost = nullptr;
    disconnectMaster(mi);
    serverAssert(mi->master == nullptr);
    if (!g_pserver->fActiveReplica)
        disconnectAllBlockedClients(); /* Clients blocked in master, now replica. */

    /* Setting masterhost only after the call to freeClient since it calls
     * replicationHandleMasterDisconnection which can trigger a re-connect
     * directly from within that call. */
    mi->masterhost = sdsnew(ip);
    mi->masterport = port;

    /* Update oom_score_adj */
    setOOMScoreAdj(-1);

    /* Force our slaves to resync with us as well. They may hopefully be able
     * to partially resync with us, but we can notify the replid change. */
    if (!g_pserver->fActiveReplica)
        disconnectSlaves();
    cancelReplicationHandshake(mi,false);
    /* Before destroying our master state, create a cached master using
     * our own parameters, to later PSYNC with the new master. */
    if (was_master) {
        replicationDiscardCachedMaster(mi);
        replicationCacheMasterUsingMyself(mi);
    }

    /* Fire the role change modules event. */
    moduleFireServerEvent(REDISMODULE_EVENT_REPLICATION_ROLE_CHANGED,
                          REDISMODULE_EVENT_REPLROLECHANGED_NOW_REPLICA,
                          NULL);

    /* Fire the master link modules event. */
    if (mi->repl_state == REPL_STATE_CONNECTED)
        moduleFireServerEvent(REDISMODULE_EVENT_MASTER_LINK_CHANGE,
                              REDISMODULE_SUBEVENT_MASTER_LINK_DOWN,
                              NULL);

    mi->repl_state = REPL_STATE_CONNECT;
    if (!g_pserver->fActiveReplica && serverTL->el != nullptr) {    // note the event loop could be NULL if we're in boot loading a config
        serverLog(LL_NOTICE,"Connecting to MASTER %s:%d",
            mi->masterhost, mi->masterport);
        connectWithMaster(mi);
    }
    saveMasterStatusToStorage(false);
    return mi;
}

void freeMasterInfo(redisMaster *mi)
{
    sdsfree(mi->masterauth);
    zfree(mi->masteruser);
    if (mi->repl_transfer_tmpfile)
        zfree(mi->repl_transfer_tmpfile);
    delete mi->staleKeyMap;
    if (mi->cached_master != nullptr)
        freeClientAsync(mi->cached_master);
    if (mi->master != nullptr)
        freeClientAsync(mi->master);
    zfree(mi);
}


/* Cancel replication, setting the instance as a master itself. */
void replicationUnsetMaster(redisMaster *mi) {
    if (mi->masterhost == NULL) return; /* Nothing to do. */

    /* Fire the master link modules event. */
    if (mi->repl_state == REPL_STATE_CONNECTED)
        moduleFireServerEvent(REDISMODULE_EVENT_MASTER_LINK_CHANGE,
                              REDISMODULE_SUBEVENT_MASTER_LINK_DOWN,
                              NULL);

    /* Clear masterhost first, since the freeClient calls
     * replicationHandleMasterDisconnection which can attempt to re-connect. */
    sdsfree(mi->masterhost);
    mi->masterhost = NULL;
    disconnectMaster(mi);
    replicationDiscardCachedMaster(mi);
    cancelReplicationHandshake(mi,false);
    /* When a slave is turned into a master, the current replication ID
     * (that was inherited from the master at synchronization time) is
     * used as secondary ID up to the current offset, and a new replication
     * ID is created to continue with a new replication history.
     *
     * NOTE: this function MUST be called after we call
     * freeClient(g_pserver->master), since there we adjust the replication
     * offset trimming the final PINGs. See Github issue #7320. */
    shiftReplicationId();
    /* Disconnecting all the slaves is required: we need to inform slaves
     * of the replication ID change (see shiftReplicationId() call). However
     * the slaves will be able to partially resync with us, so it will be
     * a very fast reconnection. */
    if (!g_pserver->fActiveReplica)
        disconnectSlaves();
    mi->repl_state = REPL_STATE_NONE;

    /* We need to make sure the new master will start the replication stream
     * with a SELECT statement. This is forced after a full resync, but
     * with PSYNC version 2, there is no need for full resync after a
     * master switch. */
    g_pserver->replicaseldb = -1;

    /* Once we turn from replica to master, we consider the starting time without
     * slaves (that is used to count the replication backlog time to live) as
     * starting from now. Otherwise the backlog will be freed after a
     * failover if slaves do not connect immediately. */
    g_pserver->repl_no_slaves_since = g_pserver->unixtime;

    /* Reset down time so it'll be ready for when we turn into replica again. */
    mi->repl_down_since = 0;

    listNode *ln = listSearchKey(g_pserver->masters, mi);
    serverAssert(ln != nullptr);
    listDelNode(g_pserver->masters, ln);
    freeMasterInfo(mi);

    /* Update oom_score_adj */
    setOOMScoreAdj(-1);

    /* Fire the role change modules event. */
    moduleFireServerEvent(REDISMODULE_EVENT_REPLICATION_ROLE_CHANGED,
                          REDISMODULE_EVENT_REPLROLECHANGED_NOW_MASTER,
                          NULL);

    /* Restart the AOF subsystem in case we shut it down during a sync when
     * we were still a slave. */
    if (g_pserver->aof_enabled && g_pserver->aof_state == AOF_OFF) restartAOFAfterSYNC();

    saveMasterStatusToStorage(false);
}

/* This function is called when the replica lose the connection with the
 * master into an unexpected way. */
void replicationHandleMasterDisconnection(redisMaster *mi) {
    if (mi != nullptr)
    {
        /* Fire the master link modules event. */
        if (mi->repl_state == REPL_STATE_CONNECTED)
            moduleFireServerEvent(REDISMODULE_EVENT_MASTER_LINK_CHANGE,
                                REDISMODULE_SUBEVENT_MASTER_LINK_DOWN,
                                NULL);
        if (mi->master && mi->master->repl_down_since) {
            mi->repl_down_since = mi->master->repl_down_since;
        }
        else {
            mi->repl_down_since = g_pserver->unixtime;
        }
        mi->master = NULL;
        mi->repl_state = REPL_STATE_CONNECT;
        /* We lost connection with our master, don't disconnect slaves yet,
        * maybe we'll be able to PSYNC with our master later. We'll disconnect
        * the slaves only if we'll have to do a full resync with our master. */

       /* Try to re-connect immediately rather than wait for replicationCron
        * waiting 1 second may risk backlog being recycled. */
        if (mi->masterhost && !g_pserver->fActiveReplica) {
            serverLog(LL_NOTICE,"Reconnecting to MASTER %s:%d",
                mi->masterhost, mi->masterport);
            connectWithMaster(mi);
        }

        saveMasterStatusToStorage(false);
    }
}

void replicaofCommand(client *c) {
    /* SLAVEOF is not allowed in cluster mode as replication is automatically
     * configured using the current address of the master node. */
    if (g_pserver->cluster_enabled) {
        addReplyError(c,"REPLICAOF not allowed in cluster mode.");
        return;
    }

    if (g_pserver->failover_state != NO_FAILOVER) {
        addReplyError(c,"REPLICAOF not allowed while failing over.");
        return;
    }
    
    if (c->argc > 3) {
        if (c->argc != 4) {
            addReplyError(c, "Invalid arguments");
            return;
        }
        if (!strcasecmp((const char*)ptrFromObj(c->argv[1]),"remove")) {
            listIter li;
            listNode *ln;
            bool fRemoved = false;
            long port;
            string2l(szFromObj(c->argv[3]), sdslen(szFromObj(c->argv[3])), &port);
        LRestart:
            listRewind(g_pserver->masters, &li);
            while ((ln = listNext(&li))) {
                redisMaster *mi = (redisMaster*)listNodeValue(ln);
                if (mi->masterport != port)
                    continue;
                if (sdscmp(szFromObj(c->argv[2]), mi->masterhost) == 0) {
                    replicationUnsetMaster(mi);
                    fRemoved = true;
                    goto LRestart;
                }
            }
            if (!fRemoved) {
                addReplyError(c, "Master not found");
                return;
            } else if (listLength(g_pserver->masters) == 0) {
                goto LLogNoMaster;
            }
        }
    } else if (!strcasecmp((const char*)ptrFromObj(c->argv[1]),"no") &&
        !strcasecmp((const char*)ptrFromObj(c->argv[2]),"one")) {
        /* The special host/port combination "NO" "ONE" turns the instance
         * into a master. Otherwise the new master address is set. */
        if (listLength(g_pserver->masters)) {
            while (listLength(g_pserver->masters))
            {
                replicationUnsetMaster((redisMaster*)listNodeValue(listFirst(g_pserver->masters)));
            }
        LLogNoMaster:
            sds client = catClientInfoString(sdsempty(),c);
            serverLog(LL_NOTICE,"MASTER MODE enabled (user request from '%s')",
                client);
            sdsfree(client);
        }
    } else {
        long port;

        if (c->flags & CLIENT_SLAVE)
        {
            /* If a client is already a replica they cannot run this command,
             * because it involves flushing all replicas (including this
             * client) */
            addReplyError(c, "Command is not valid when client is a replica.");
            return;
        }

        if ((getLongFromObjectOrReply(c, c->argv[2], &port, NULL) != C_OK))
            return;

        redisMaster *miNew = replicationAddMaster((char*)ptrFromObj(c->argv[1]), port);
        if (miNew == nullptr)
        {
            // We have a duplicate
            serverLog(LL_NOTICE,"REPLICAOF would result into synchronization "
                                "with the master we are already connected "
                                "with. No operation performed.");
            addReplySds(c,sdsnew("+OK Already connected to specified "
                                "master\r\n"));
            return;
        }

        sds client = catClientInfoString(sdsempty(),c);
        serverLog(LL_NOTICE,"REPLICAOF %s:%d enabled (user request from '%s')",
            miNew->masterhost, miNew->masterport, client);
        sdsfree(client);
    }
    addReply(c,shared.ok);
}

/* ROLE command: provide information about the role of the instance
 * (master or replica) and additional information related to replication
 * in an easy to process format. */
void roleCommand(client *c) {
    if (listLength(g_pserver->masters) == 0) {
        listIter li;
        listNode *ln;
        void *mbcount;
        int slaves = 0;

        addReplyArrayLen(c,3);
        addReplyBulkCBuffer(c,"master",6);
        addReplyLongLong(c,g_pserver->master_repl_offset);
        mbcount = addReplyDeferredLen(c);
        listRewind(g_pserver->slaves,&li);
        while((ln = listNext(&li))) {
            client *replica = (client*)ln->value;
            char ip[NET_IP_STR_LEN], *slaveaddr = replica->slave_addr;

            if (!slaveaddr) {
                if (connPeerToString(replica->conn,ip,sizeof(ip),NULL) == -1)
                    continue;
                slaveaddr = ip;
            }
            if (replica->replstate != SLAVE_STATE_ONLINE) continue;
            addReplyArrayLen(c,3);
            addReplyBulkCString(c,slaveaddr);
            addReplyBulkLongLong(c,replica->slave_listening_port);
            addReplyBulkLongLong(c,replica->repl_ack_off);
            slaves++;
        }
        setDeferredArrayLen(c,mbcount,slaves);
    } else {
        listIter li;
        listNode *ln;
        listRewind(g_pserver->masters, &li);

        if (listLength(g_pserver->masters) > 1)
            addReplyArrayLen(c,listLength(g_pserver->masters));
        while ((ln = listNext(&li)))
        {
            redisMaster *mi = (redisMaster*)listNodeValue(ln);
            std::unique_lock<fastlock> lock;
            if (mi->master != nullptr)
                lock = std::unique_lock<fastlock>(mi->master->lock);

            const char *slavestate = NULL;
            addReplyArrayLen(c,5);
            if (g_pserver->fActiveReplica)
                addReplyBulkCBuffer(c,"active-replica",14);
            else
                addReplyBulkCBuffer(c,"slave",5);
            addReplyBulkCString(c,mi->masterhost);
            addReplyLongLong(c,mi->masterport);
            if (slaveIsInHandshakeState(mi)) {
                slavestate = "handshake";
            } else {
                switch(mi->repl_state) {
                case REPL_STATE_NONE: slavestate = "none"; break;
                case REPL_STATE_CONNECT: slavestate = "connect"; break;
                case REPL_STATE_CONNECTING: slavestate = "connecting"; break;
                case REPL_STATE_TRANSFER: slavestate = "sync"; break;
                case REPL_STATE_CONNECTED: slavestate = "connected"; break;
                default: slavestate = "unknown"; break;
                }
            }
            addReplyBulkCString(c,slavestate);
            addReplyLongLong(c,mi->master ? mi->master->reploff : -1);
        }
    }
}

/* Send a REPLCONF ACK command to the master to inform it about the current
 * processed offset. If we are not connected with a master, the command has
 * no effects. */
void replicationSendAck(redisMaster *mi) 
{
    client *c = mi->master;

    if (c != NULL) {
        std::unique_lock<fastlock> ul(c->lock);
        c->flags |= CLIENT_MASTER_FORCE_REPLY;
        addReplyArrayLen(c,3);
        addReplyBulkCString(c,"REPLCONF");
        addReplyBulkCString(c,"ACK");
        addReplyBulkLongLong(c,c->reploff);
        c->flags &= ~CLIENT_MASTER_FORCE_REPLY;
    }
}

/* ---------------------- MASTER CACHING FOR PSYNC -------------------------- */

/* In order to implement partial synchronization we need to be able to cache
 * our master's client structure after a transient disconnection.
 * It is cached into g_pserver->cached_master and flushed away using the following
 * functions. */

/* This function is called by freeClient() in order to cache the master
 * client structure instead of destroying it. freeClient() will return
 * ASAP after this function returns, so every action needed to avoid problems
 * with a client that is really "suspended" has to be done by this function.
 *
 * The other functions that will deal with the cached master are:
 *
 * replicationDiscardCachedMaster() that will make sure to kill the client
 * as for some reason we don't want to use it in the future.
 *
 * replicationResurrectCachedMaster() that is used after a successful PSYNC
 * handshake in order to reactivate the cached master.
 */
void replicationCacheMaster(redisMaster *mi, client *c) {
    serverAssert(mi->master == c);
    serverAssert(mi->master != NULL && mi->cached_master == NULL);
    serverLog(LL_NOTICE,"Caching the disconnected master state.");
    AssertCorrectThread(c);
    std::lock_guard<decltype(c->lock)> clientlock(c->lock);

    /* Unlink the client from the server structures. */
    unlinkClient(c);

    /* Reset the master client so that's ready to accept new commands:
     * we want to discard te non processed query buffers and non processed
     * offsets, including pending transactions, already populated arguments,
     * pending outputs to the master. */
    sdsclear(mi->master->querybuf);
    if (!mi->master->vecqueuedcmd.empty()) {
        mi->master->vecqueuedcmd.clear();
    }
    mi->master->multibulklen = 0;
    sdsclear(mi->master->pending_querybuf);
    mi->master->read_reploff = mi->master->reploff;
    if (c->flags & CLIENT_MULTI) discardTransaction(c);
    listEmpty(c->reply);
    c->sentlen = 0;
    c->sentlenAsync = 0;
    c->reply_bytes = 0;
    c->bufpos = 0;
    resetClient(c);

    /* Save the master. g_pserver->master will be set to null later by
     * replicationHandleMasterDisconnection(). */
    mi->cached_master = mi->master;

    /* Invalidate the Peer ID cache. */
    if (c->peerid) {
        sdsfree(c->peerid);
        c->peerid = NULL;
    }
    /* Invalidate the Sock Name cache. */
    if (c->sockname) {
        sdsfree(c->sockname);
        c->sockname = NULL;
    }

    /* Caching the master happens instead of the actual freeClient() call,
     * so make sure to adjust the replication state. This function will
     * also set g_pserver->master to NULL. */
    replicationHandleMasterDisconnection(mi);
    serverAssert(mi->master == nullptr);
}

/* This function is called when a master is turend into a slave, in order to
 * create from scratch a cached master for the new client, that will allow
 * to PSYNC with the slave that was promoted as the new master after a
 * failover.
 *
 * Assuming this instance was previously the master instance of the new master,
 * the new master will accept its replication ID, and potentiall also the
 * current offset if no data was lost during the failover. So we use our
 * current replication ID and offset in order to synthesize a cached master. */
void replicationCacheMasterUsingMyself(redisMaster *mi) {
    serverLog(LL_NOTICE,
        "Before turning into a replica, using my own master parameters "
        "to synthesize a cached master: I may be able to synchronize with "
        "the new master with just a partial transfer.");

    if (mi->cached_master != nullptr)
    {
        // This can happen on first load of the RDB, the master we created in config load is stale
        freeClient(mi->cached_master);
    }

    /* This will be used to populate the field g_pserver->master->reploff
     * by replicationCreateMasterClient(). We'll later set the created
     * master as g_pserver->cached_master, so the replica will use such
     * offset for PSYNC. */
    mi->master_initial_offset = g_pserver->master_repl_offset;

    /* The master client we create can be set to any DBID, because
     * the new master will start its replication stream with SELECT. */
    replicationCreateMasterClient(mi,NULL,-1);
    std::lock_guard<decltype(mi->master->lock)> lock(mi->master->lock);

    /* Use our own ID / offset. */
    memcpy(mi->master->replid, g_pserver->replid, sizeof(g_pserver->replid));

    /* Set as cached master. */
    unlinkClient(mi->master);
    mi->cached_master = mi->master;
    mi->master = NULL;
}

/* This function is called when reloading master info from an RDB in Active Replica mode.
 * It creates a cached master client using the info contained in the redisMaster struct.
 *
 * Assumes that the passed struct contains valid master info. */
void replicationCacheMasterUsingMaster(redisMaster *mi) {
    if (mi->cached_master) {
        freeClient(mi->cached_master);
    }

    replicationCreateMasterClient(mi, NULL, -1);
    std::lock_guard<decltype(mi->master->lock)> lock(mi->master->lock);

    memcpy(mi->master->replid, mi->master_replid, sizeof(mi->master_replid));
    mi->master->reploff = mi->master_initial_offset;

    unlinkClient(mi->master);
    mi->cached_master = mi->master;
    mi->master = NULL;
}

/* Free a cached master, called when there are no longer the conditions for
 * a partial resync on reconnection. */
void replicationDiscardCachedMaster(redisMaster *mi) {
    if (mi->cached_master == NULL) return;

    serverLog(LL_NOTICE,"Discarding previously cached master state.");
    mi->cached_master->flags &= ~CLIENT_MASTER;
    freeClientAsync(mi->cached_master);
    mi->cached_master = NULL;
}

/* Turn the cached master into the current master, using the file descriptor
 * passed as argument as the socket for the new master.
 *
 * This function is called when successfully setup a partial resynchronization
 * so the stream of data that we'll receive will start from were this
 * master left. */
void replicationResurrectCachedMaster(redisMaster *mi, connection *conn) {
    mi->master = mi->cached_master;
    mi->cached_master = NULL;
    mi->master->conn = conn;
    connSetPrivateData(mi->master->conn, mi->master);
    mi->master->flags &= ~(CLIENT_CLOSE_AFTER_REPLY|CLIENT_CLOSE_ASAP);
    mi->master->authenticated = 1;
    mi->master->lastinteraction = g_pserver->unixtime;
    mi->repl_state = REPL_STATE_CONNECTED;
    mi->repl_down_since = 0;
    mi->master->repl_down_since = 0;

    /* Normally changing the thread of a client is a BIG NONO,
        but this client was unlinked so its OK here */
    mi->master->iel = serverTL - g_pserver->rgthreadvar; // martial to this thread

    /* Fire the master link modules event. */
    moduleFireServerEvent(REDISMODULE_EVENT_MASTER_LINK_CHANGE,
                          REDISMODULE_SUBEVENT_MASTER_LINK_UP,
                          NULL);

    /* Re-add to the list of clients. */
    linkClient(mi->master);
    serverAssert(connGetPrivateData(mi->master->conn) == mi->master);
    serverAssert(mi->master->conn == conn);
    AssertCorrectThread(mi->master);
    if (connSetReadHandler(mi->master->conn, readQueryFromClient, true)) {
        serverLog(LL_WARNING,"Error resurrecting the cached master, impossible to add the readable handler: %s", strerror(errno));
        freeClientAsync(mi->master); /* Close ASAP. */
    }

    /* We may also need to install the write handler as well if there is
     * pending data in the write buffers. */
    if (clientHasPendingReplies(mi->master)) {
        if (connSetWriteHandler(mi->master->conn, sendReplyToClient, true)) {
            serverLog(LL_WARNING,"Error resurrecting the cached master, impossible to add the writable handler: %s", strerror(errno));
            freeClientAsync(mi->master); /* Close ASAP. */
        }
    }
}

/* ------------------------- MIN-SLAVES-TO-WRITE  --------------------------- */

/* This function counts the number of slaves with lag <= min-slaves-max-lag.
 * If the option is active, the server will prevent writes if there are not
 * enough connected slaves with the specified lag (or less). */
void refreshGoodSlavesCount(void) {
    listIter li;
    listNode *ln;
    int good = 0;

    if (!g_pserver->repl_min_slaves_to_write ||
        !g_pserver->repl_min_slaves_max_lag) return;

    listRewind(g_pserver->slaves,&li);
    while((ln = listNext(&li))) {
        client *replica = (client*)ln->value;
        time_t lag = g_pserver->unixtime - replica->repl_ack_time;

        if (replica->replstate == SLAVE_STATE_ONLINE &&
            lag <= g_pserver->repl_min_slaves_max_lag) good++;
    }
    g_pserver->repl_good_slaves_count = good;
}

/* ----------------------- REPLICATION SCRIPT CACHE --------------------------
 * The goal of this code is to keep track of scripts already sent to every
 * connected replica, in order to be able to replicate EVALSHA as it is without
 * translating it to EVAL every time it is possible.
 *
 * We use a capped collection implemented by a hash table for fast lookup
 * of scripts we can send as EVALSHA, plus a linked list that is used for
 * eviction of the oldest entry when the max number of items is reached.
 *
 * We don't care about taking a different cache for every different replica
 * since to fill the cache again is not very costly, the goal of this code
 * is to avoid that the same big script is transmitted a big number of times
 * per second wasting bandwidth and processor speed, but it is not a problem
 * if we need to rebuild the cache from scratch from time to time, every used
 * script will need to be transmitted a single time to reappear in the cache.
 *
 * This is how the system works:
 *
 * 1) Every time a new replica connects, we flush the whole script cache.
 * 2) We only send as EVALSHA what was sent to the master as EVALSHA, without
 *    trying to convert EVAL into EVALSHA specifically for slaves.
 * 3) Every time we transmit a script as EVAL to the slaves, we also add the
 *    corresponding SHA1 of the script into the cache as we are sure every
 *    replica knows about the script starting from now.
 * 4) On SCRIPT FLUSH command, we replicate the command to all the slaves
 *    and at the same time flush the script cache.
 * 5) When the last replica disconnects, flush the cache.
 * 6) We handle SCRIPT LOAD as well since that's how scripts are loaded
 *    in the master sometimes.
 */

/* Initialize the script cache, only called at startup. */
void replicationScriptCacheInit(void) {
    g_pserver->repl_scriptcache_size = 10000;
    g_pserver->repl_scriptcache_dict = dictCreate(&replScriptCacheDictType,NULL);
    g_pserver->repl_scriptcache_fifo = listCreate();
}

/* Empty the script cache. Should be called every time we are no longer sure
 * that every replica knows about all the scripts in our set, or when the
 * current AOF "context" is no longer aware of the script. In general we
 * should flush the cache:
 *
 * 1) Every time a new replica reconnects to this master and performs a
 *    full SYNC (PSYNC does not require flushing).
 * 2) Every time an AOF rewrite is performed.
 * 3) Every time we are left without slaves at all, and AOF is off, in order
 *    to reclaim otherwise unused memory.
 */
void replicationScriptCacheFlush(void) {
    dictEmpty(g_pserver->repl_scriptcache_dict,NULL);
    listRelease(g_pserver->repl_scriptcache_fifo);
    g_pserver->repl_scriptcache_fifo = listCreate();
}

/* Add an entry into the script cache, if we reach max number of entries the
 * oldest is removed from the list. */
void replicationScriptCacheAdd(sds sha1) {
    int retval;
    sds key = sdsdup(sha1);

    /* Evict oldest. */
    if (listLength(g_pserver->repl_scriptcache_fifo) == g_pserver->repl_scriptcache_size)
    {
        listNode *ln = listLast(g_pserver->repl_scriptcache_fifo);
        sds oldest = (sds)listNodeValue(ln);

        retval = dictDelete(g_pserver->repl_scriptcache_dict,oldest);
        serverAssert(retval == DICT_OK);
        listDelNode(g_pserver->repl_scriptcache_fifo,ln);
    }

    /* Add current. */
    retval = dictAdd(g_pserver->repl_scriptcache_dict,key,NULL);
    listAddNodeHead(g_pserver->repl_scriptcache_fifo,key);
    serverAssert(retval == DICT_OK);
}

/* Returns non-zero if the specified entry exists inside the cache, that is,
 * if all the slaves are aware of this script SHA1. */
int replicationScriptCacheExists(sds sha1) {
    return dictFind(g_pserver->repl_scriptcache_dict,sha1) != NULL;
}

/* ----------------------- SYNCHRONOUS REPLICATION --------------------------
 * Redis synchronous replication design can be summarized in points:
 *
 * - Redis masters have a global replication offset, used by PSYNC.
 * - Master increment the offset every time new commands are sent to slaves.
 * - Slaves ping back masters with the offset processed so far.
 *
 * So synchronous replication adds a new WAIT command in the form:
 *
 *   WAIT <num_replicas> <milliseconds_timeout>
 *
 * That returns the number of replicas that processed the query when
 * we finally have at least num_replicas, or when the timeout was
 * reached.
 *
 * The command is implemented in this way:
 *
 * - Every time a client processes a command, we remember the replication
 *   offset after sending that command to the slaves.
 * - When WAIT is called, we ask slaves to send an acknowledgement ASAP.
 *   The client is blocked at the same time (see blocked.c).
 * - Once we receive enough ACKs for a given offset or when the timeout
 *   is reached, the WAIT command is unblocked and the reply sent to the
 *   client.
 */

/* This just set a flag so that we broadcast a REPLCONF GETACK command
 * to all the slaves in the beforeSleep() function. Note that this way
 * we "group" all the clients that want to wait for synchronous replication
 * in a given event loop iteration, and send a single GETACK for them all. */
void replicationRequestAckFromSlaves(void) {
    g_pserver->get_ack_from_slaves = 1;
}

/* Return the number of slaves that already acknowledged the specified
 * replication offset. */
int replicationCountAcksByOffset(long long offset) {
    listIter li;
    listNode *ln;
    int count = 0;

    listRewind(g_pserver->slaves,&li);
    while((ln = listNext(&li))) {
        client *replica = (client*)ln->value;

        if (replica->replstate != SLAVE_STATE_ONLINE) continue;
        if ((replica->repl_ack_off) >= offset) count++;
    }
    return count;
}

/* WAIT for N replicas to acknowledge the processing of our latest
 * write command (and all the previous commands). */
void waitCommand(client *c) {
    mstime_t timeout;
    long numreplicas, ackreplicas;
    long long offset = c->woff;

    if (listLength(g_pserver->masters) && !g_pserver->fActiveReplica) {
        addReplyError(c,"WAIT cannot be used with replica instances. Please also note that since Redis 4.0 if a replica is configured to be writable (which is not the default) writes to replicas are just local and are not propagated.");
        return;
    }

    /* Argument parsing. */
    if (getLongFromObjectOrReply(c,c->argv[1],&numreplicas,NULL) != C_OK)
        return;
    if (getTimeoutFromObjectOrReply(c,c->argv[2],&timeout,UNIT_MILLISECONDS)
        != C_OK) return;

    /* First try without blocking at all. */
    ackreplicas = replicationCountAcksByOffset(c->woff);
    if (ackreplicas >= numreplicas || c->flags & CLIENT_MULTI) {
        addReplyLongLong(c,ackreplicas);
        return;
    }

    /* Otherwise block the client and put it into our list of clients
     * waiting for ack from slaves. */
    c->bpop.timeout = timeout;
    c->bpop.reploffset = offset;
    c->bpop.numreplicas = numreplicas;
    listAddNodeHead(g_pserver->clients_waiting_acks,c);
    blockClient(c,BLOCKED_WAIT);

    /* Make sure that the server will send an ACK request to all the slaves
     * before returning to the event loop. */
    replicationRequestAckFromSlaves();
}

/* This is called by unblockClient() to perform the blocking op type
 * specific cleanup. We just remove the client from the list of clients
 * waiting for replica acks. Never call it directly, call unblockClient()
 * instead. */
void unblockClientWaitingReplicas(client *c) {
    listNode *ln = listSearchKey(g_pserver->clients_waiting_acks,c);
    serverAssert(ln != NULL);
    listDelNode(g_pserver->clients_waiting_acks,ln);
}

/* Check if there are clients blocked in WAIT that can be unblocked since
 * we received enough ACKs from slaves. */
void processClientsWaitingReplicas(void) {
    long long last_offset = 0;
    int last_numreplicas = 0;

    listIter li;
    listNode *ln;

    listRewind(g_pserver->clients_waiting_acks,&li);
    while((ln = listNext(&li))) {
        client *c = (client*)ln->value;
        std::unique_lock<fastlock> ul(c->lock);

        /* Every time we find a client that is satisfied for a given
         * offset and number of replicas, we remember it so the next client
         * may be unblocked without calling replicationCountAcksByOffset()
         * if the requested offset / replicas were equal or less. */
        if (last_offset && last_offset >= c->bpop.reploffset &&
                           last_numreplicas >= c->bpop.numreplicas)
        {
            unblockClient(c);
            addReplyLongLong(c,last_numreplicas);
        } else {
            int numreplicas = replicationCountAcksByOffset(c->bpop.reploffset);

            if (numreplicas >= c->bpop.numreplicas) {
                last_offset = c->bpop.reploffset;
                last_numreplicas = numreplicas;
                unblockClient(c);
                addReplyLongLong(c,numreplicas);
            }
        }
    }
}

/* Return the replica replication offset for this instance, that is
 * the offset for which we already processed the master replication stream. */
long long replicationGetSlaveOffset(redisMaster *mi) {
    long long offset = 0;

    if (mi != NULL && mi->masterhost != NULL) {
        if (mi->master) {
            offset = mi->master->reploff;
        } else if (mi->cached_master) {
            offset = mi->cached_master->reploff;
        }
    }
    /* offset may be -1 when the master does not support it at all, however
     * this function is designed to return an offset that can express the
     * amount of data processed by the master, so we return a positive
     * integer. */
    if (offset < 0) offset = 0;
    return offset;
}

/* --------------------------- REPLICATION CRON  ---------------------------- */

/* Replication cron function, called 1 time per second. */
void replicationCron(void) {
    static long long replication_cron_loops = 0;
    serverAssert(GlobalLocksAcquired());

    /* Check failover status first, to see if we need to start
     * handling the failover. */
    updateFailoverStatus();

    listIter liMaster;
    listNode *lnMaster;
    listRewind(g_pserver->masters, &liMaster);

    bool fInMasterConnection = false;
    while ((lnMaster = listNext(&liMaster)) && !fInMasterConnection)
    {
        redisMaster *mi = (redisMaster*)listNodeValue(lnMaster);
        if (mi->repl_state != REPL_STATE_NONE && mi->repl_state != REPL_STATE_CONNECTED && mi->repl_state != REPL_STATE_CONNECT) {
            fInMasterConnection = true;
        }
    }

    bool fConnectionStarted = false;
    listRewind(g_pserver->masters, &liMaster);
    while ((lnMaster = listNext(&liMaster)))
    {
        redisMaster *mi = (redisMaster*)listNodeValue(lnMaster);

        std::unique_lock<decltype(mi->master->lock)> ulock;
        if (mi->master != nullptr)
            ulock = decltype(ulock)(mi->master->lock);

        /* Non blocking connection timeout? */
        if (mi->masterhost &&
            (mi->repl_state == REPL_STATE_CONNECTING ||
            slaveIsInHandshakeState(mi)) &&
            (time(NULL)-mi->repl_transfer_lastio) > g_pserver->repl_timeout)
        {
            serverLog(LL_WARNING,"Timeout connecting to the MASTER...");
            cancelReplicationHandshake(mi,true);
        }

        /* Bulk transfer I/O timeout? */
        if (mi->masterhost && mi->repl_state == REPL_STATE_TRANSFER &&
            (time(NULL)-mi->repl_transfer_lastio) > g_pserver->repl_timeout)
        {
            serverLog(LL_WARNING,"Timeout receiving bulk data from MASTER... If the problem persists try to set the 'repl-timeout' parameter in keydb.conf to a larger value.");
            cancelReplicationHandshake(mi,true);
        }

        /* Timed out master when we are an already connected replica? */
        if (mi->masterhost && mi->master && mi->repl_state == REPL_STATE_CONNECTED &&
            (time(NULL)-mi->master->lastinteraction) > g_pserver->repl_timeout)
        {
            serverLog(LL_WARNING,"MASTER timeout: no data nor PING received...");
            disconnectMaster(mi);
        }

        /* Check if we should connect to a MASTER */
        if (mi->repl_state == REPL_STATE_CONNECT && !fInMasterConnection && !g_pserver->loading && !g_pserver->FRdbSaveInProgress()) {
            serverLog(LL_NOTICE,"Connecting to MASTER %s:%d",
                mi->masterhost, mi->masterport);
            connectWithMaster(mi);
            fInMasterConnection = true;
            fConnectionStarted = true;
        }

        /* Send ACK to master from time to time.
        * Note that we do not send periodic acks to masters that don't
        * support PSYNC and replication offsets. */
        if (mi->masterhost && mi->master &&
            !(mi->master->flags & CLIENT_PRE_PSYNC))
            replicationSendAck(mi);
    }

    if (fConnectionStarted) {
        // If we cancel this handshake we want the next attempt to be a different master
        listRotateHeadToTail(g_pserver->masters);
    }

    /* If we have attached slaves, PING them from time to time.
    * So slaves can implement an explicit timeout to masters, and will
    * be able to detect a link disconnection even if the TCP connection
    * will not actually go down. */
    listIter li;
    listNode *ln;
    robj *ping_argv[1];

    /* First, send PING according to ping_slave_period. */
    if ((replication_cron_loops % g_pserver->repl_ping_slave_period) == 0 &&
        listLength(g_pserver->slaves))
    {
        /* Note that we don't send the PING if the clients are paused during
         * a Redis Cluster manual failover: the PING we send will otherwise
         * alter the replication offsets of master and replica, and will no longer
         * match the one stored into 'mf_master_offset' state. */
        int manual_failover_in_progress =
            ((g_pserver->cluster_enabled &&
              g_pserver->cluster->mf_end) ||
            g_pserver->failover_end_time) &&
            checkClientPauseTimeoutAndReturnIfPaused();

        if (!manual_failover_in_progress) {
            ping_argv[0] = shared.ping;
            replicationFeedSlaves(g_pserver->slaves, g_pserver->replicaseldb,
                ping_argv, 1);
        }
    }

    /* Second, send a newline to all the slaves in pre-synchronization
    * stage, that is, slaves waiting for the master to create the RDB file.
    *
    * Also send the a newline to all the chained slaves we have, if we lost
    * connection from our master, to keep the slaves aware that their
    * master is online. This is needed since sub-slaves only receive proxied
    * data from top-level masters, so there is no explicit pinging in order
    * to avoid altering the replication offsets. This special out of band
    * pings (newlines) can be sent, they will have no effect in the offset.
    *
    * The newline will be ignored by the replica but will refresh the
    * last interaction timer preventing a timeout. In this case we ignore the
    * ping period and refresh the connection once per second since certain
    * timeouts are set at a few seconds (example: PSYNC response). */
    listRewind(g_pserver->slaves,&li);
    while((ln = listNext(&li))) {
        client *replica = (client*)ln->value;

        int is_presync =
            (replica->replstate == SLAVE_STATE_WAIT_BGSAVE_START ||
            (replica->replstate == SLAVE_STATE_WAIT_BGSAVE_END &&
            g_pserver->rdb_child_type != RDB_CHILD_TYPE_SOCKET));

        if (is_presync) {
            connWrite(replica->conn, "\n", 1);
        }
    }

    /* Disconnect timedout slaves. */
    if (listLength(g_pserver->slaves)) {
        listIter li;
        listNode *ln;

        listRewind(g_pserver->slaves,&li);
        while((ln = listNext(&li))) {
            client *replica = (client*)ln->value;
            std::unique_lock<fastlock> ul(replica->lock);

            if (replica->replstate == SLAVE_STATE_FASTSYNC_DONE && !clientHasPendingReplies(replica)) {
                serverLog(LL_WARNING, "Putting replica online");
                replica->postFunction([](client *c){
                    putSlaveOnline(c);
                });
            }

            if (replica->replstate == SLAVE_STATE_ONLINE) {
                if (replica->flags & CLIENT_PRE_PSYNC)
                    continue;
                if ((g_pserver->unixtime - replica->repl_ack_time) > g_pserver->repl_timeout) {
                    serverLog(LL_WARNING, "Disconnecting timedout replica (streaming sync): %s",
                          replicationGetSlaveName(replica));
                    freeClientAsync(replica);
                    continue;
                }
            }
            /* We consider disconnecting only diskless replicas because disk-based replicas aren't fed
             * by the fork child so if a disk-based replica is stuck it doesn't prevent the fork child
             * from terminating. */
            if (replica->replstate == SLAVE_STATE_WAIT_BGSAVE_END && g_pserver->rdb_child_type == RDB_CHILD_TYPE_SOCKET) {
                if (replica->repl_last_partial_write != 0 &&
                    (g_pserver->unixtime - replica->repl_last_partial_write) > g_pserver->repl_timeout)
                {
                    serverLog(LL_WARNING, "Disconnecting timedout replica (full sync): %s",
                          replicationGetSlaveName(replica));
                    freeClientAsync(replica);
                    continue;
                }
            }
        }
    }

    /* If this is a master without attached slaves and there is a replication
    * backlog active, in order to reclaim memory we can free it after some
    * (configured) time. Note that this cannot be done for slaves: slaves
    * without sub-slaves attached should still accumulate data into the
    * backlog, in order to reply to PSYNC queries if they are turned into
    * masters after a failover. */
    if (listLength(g_pserver->slaves) == 0 && g_pserver->repl_backlog_time_limit &&
        g_pserver->repl_backlog && listLength(g_pserver->masters) == 0)
    {
        time_t idle = g_pserver->unixtime - g_pserver->repl_no_slaves_since;

        if (idle > g_pserver->repl_backlog_time_limit) {
            /* When we free the backlog, we always use a new
            * replication ID and clear the ID2. This is needed
            * because when there is no backlog, the master_repl_offset
            * is not updated, but we would still retain our replication
            * ID, leading to the following problem:
            *
            * 1. We are a master instance.
            * 2. Our replica is promoted to master. It's repl-id-2 will
            *    be the same as our repl-id.
            * 3. We, yet as master, receive some updates, that will not
            *    increment the master_repl_offset.
            * 4. Later we are turned into a replica, connect to the new
            *    master that will accept our PSYNC request by second
            *    replication ID, but there will be data inconsistency
            *    because we received writes. */
            changeReplicationId();
            clearReplicationId2();
            freeReplicationBacklog();
            serverLog(LL_NOTICE,
                "Replication backlog freed after %d seconds "
                "without connected replicas.",
                (int) g_pserver->repl_backlog_time_limit);
        }
    }

    /* If AOF is disabled and we no longer have attached slaves, we can
    * free our Replication Script Cache as there is no need to propagate
    * EVALSHA at all. */
    if (listLength(g_pserver->slaves) == 0 &&
        g_pserver->aof_state == AOF_OFF &&
        listLength(g_pserver->repl_scriptcache_fifo) != 0)
    {
        replicationScriptCacheFlush();
    }

    propagateMasterStaleKeys();
    
    replicationStartPendingFork();

    trimReplicationBacklog();

    /* Remove the RDB file used for replication if Redis is not running
     * with any persistence. */
    removeRDBUsedToSyncReplicas();

    /* Refresh the number of slaves with lag <= min-slaves-max-lag. */
    refreshGoodSlavesCount();
    replication_cron_loops++; /* Incremented with frequency 1 HZ. */
}

void replicationStartPendingFork(void) {
    /* Start a BGSAVE good for replication if we have slaves in
     * WAIT_BGSAVE_START state.
     *
     * In case of diskless replication, we make sure to wait the specified
     * number of seconds (according to configuration) so that other slaves
     * have the time to arrive before we start streaming. */
    if (!hasActiveChildProcess()) {
        time_t idle, max_idle = 0;
        int slaves_waiting = 0;
        int mincapa = -1;
        listNode *ln;
        listIter li;

        listRewind(g_pserver->slaves,&li);
        while((ln = listNext(&li))) {
            client *replica = (client*)ln->value;
            if (replica->replstate == SLAVE_STATE_WAIT_BGSAVE_START) {
                idle = g_pserver->unixtime - replica->lastinteraction;
                if (idle > max_idle) max_idle = idle;
                slaves_waiting++;
                mincapa = (mincapa == -1) ? replica->slave_capa :
                                            (mincapa & replica->slave_capa);
            }
        }

        if (slaves_waiting &&
            (!g_pserver->repl_diskless_sync ||
            max_idle >= g_pserver->repl_diskless_sync_delay))
        {
            /* Start the BGSAVE. The called function may start a
            * BGSAVE with socket target or disk target depending on the
            * configuration and slaves capabilities. */
            startBgsaveForReplication(mincapa);
        }
    }
}

/* Find replica at IP:PORT from replica list */
static client *findReplica(char *host, int port) {
    listIter li;
    listNode *ln;
    client *replica;

    listRewind(g_pserver->slaves,&li);
    while((ln = listNext(&li))) {
        replica = (client*)listNodeValue(ln);
        char ip[NET_IP_STR_LEN], *replicaip = replica->slave_addr;

        if (!replicaip) {
            if (connPeerToString(replica->conn, ip, sizeof(ip), NULL) == -1)
                continue;
            replicaip = ip;
        }

        if (!strcasecmp(host, replicaip) &&
                (port == replica->slave_listening_port))
            return replica;
    }

    return NULL;
}

const char *getFailoverStateString() {
    switch(g_pserver->failover_state) {
        case NO_FAILOVER: return "no-failover";
        case FAILOVER_IN_PROGRESS: return "failover-in-progress";
        case FAILOVER_WAIT_FOR_SYNC: return "waiting-for-sync";
    }
    return "unknown";
}

/* Resets the internal failover configuration, this needs
 * to be called after a failover either succeeds or fails
 * as it includes the client unpause. */
void clearFailoverState() {
    g_pserver->failover_end_time = 0;
    g_pserver->force_failover = 0;
    zfree(g_pserver->target_replica_host);
    g_pserver->target_replica_host = NULL;
    g_pserver->target_replica_port = 0;
    g_pserver->failover_state = NO_FAILOVER;
    unpauseClients();
}

/* Abort an ongoing failover if one is going on. */
void abortFailover(redisMaster *mi, const char *err) {
    if (g_pserver->failover_state == NO_FAILOVER) return;

    if (g_pserver->target_replica_host) {
        serverLog(LL_NOTICE,"FAILOVER to %s:%d aborted: %s",
            g_pserver->target_replica_host,g_pserver->target_replica_port,err);  
    } else {
        serverLog(LL_NOTICE,"FAILOVER to any replica aborted: %s",err);  
    }
    if (g_pserver->failover_state == FAILOVER_IN_PROGRESS) {
        replicationUnsetMaster(mi);
    }
    clearFailoverState();
}

/* 
 * FAILOVER [TO <HOST> <PORT> [FORCE]] [ABORT] [TIMEOUT <timeout>]
 * 
 * This command will coordinate a failover between the master and one
 * of its replicas. The happy path contains the following steps:
 * 1) The master will initiate a client pause write, to stop replication
 * traffic.
 * 2) The master will periodically check if any of its replicas has
 * consumed the entire replication stream through acks. 
 * 3) Once any replica has caught up, the master will itself become a replica.
 * 4) The master will send a PSYNC FAILOVER request to the target replica, which
 * if accepted will cause the replica to become the new master and start a sync.
 * 
 * FAILOVER ABORT is the only way to abort a failover command, as replicaof
 * will be disabled. This may be needed if the failover is unable to progress. 
 * 
 * The optional arguments [TO <HOST> <IP>] allows designating a specific replica
 * to be failed over to.
 * 
 * FORCE flag indicates that even if the target replica is not caught up,
 * failover to it anyway. This must be specified with a timeout and a target
 * HOST and IP.
 * 
 * TIMEOUT <timeout> indicates how long should the primary wait for 
 * a replica to sync up before aborting. If not specified, the failover
 * will attempt forever and must be manually aborted.
 */
void failoverCommand(client *c) {
    if (g_pserver->cluster_enabled) {
        addReplyError(c,"FAILOVER not allowed in cluster mode. "
                        "Use CLUSTER FAILOVER command instead.");
        return;
    }
    
    if (g_pserver->fActiveReplica) {
        addReplyError(c,"FAILOVER not allowed in active replication mode");
        return;
    }

    /* Handle special case for abort */
    if ((c->argc == 2) && !strcasecmp(szFromObj(c->argv[1]),"abort")) {
        if (g_pserver->failover_state == NO_FAILOVER) {
            addReplyError(c, "No failover in progress.");
            return;
        }

        redisMaster *mi = listLength(g_pserver->masters) ? (redisMaster*)listNodeValue(listFirst(g_pserver->masters)) : nullptr;
        abortFailover(mi, "Failover manually aborted");
        addReply(c,shared.ok);
        return;
    }

    long timeout_in_ms = 0;
    int force_flag = 0;
    long port = 0;
    char *host = NULL;

    /* Parse the command for syntax and arguments. */
    for (int j = 1; j < c->argc; j++) {
        if (!strcasecmp(szFromObj(c->argv[j]),"timeout") && (j + 1 < c->argc) &&
            timeout_in_ms == 0)
        {
            if (getLongFromObjectOrReply(c,c->argv[j + 1],
                        &timeout_in_ms,NULL) != C_OK) return;
            if (timeout_in_ms <= 0) {
                addReplyError(c,"FAILOVER timeout must be greater than 0");
                return;
            }
            j++;
        } else if (!strcasecmp(szFromObj(c->argv[j]),"to") && (j + 2 < c->argc) &&
            !host) 
        {
            if (getLongFromObjectOrReply(c,c->argv[j + 2],&port,NULL) != C_OK)
                return;
            host = szFromObj(c->argv[j + 1]);
            j += 2;
        } else if (!strcasecmp(szFromObj(c->argv[j]),"force") && !force_flag) {
            force_flag = 1;
        } else {
            addReplyErrorObject(c,shared.syntaxerr);
            return;
        }
    }

    if (g_pserver->failover_state != NO_FAILOVER) {
        addReplyError(c,"FAILOVER already in progress.");
        return;
    }

    if (listLength(g_pserver->masters)) {
        addReplyError(c,"FAILOVER is not valid when server is a replica.");
        return;
    }

    if (listLength(g_pserver->slaves) == 0) {
        addReplyError(c,"FAILOVER requires connected replicas.");
        return; 
    }

    if (force_flag && (!timeout_in_ms || !host)) {
        addReplyError(c,"FAILOVER with force option requires both a timeout "
            "and target HOST and IP.");
        return;     
    }

    /* If a replica address was provided, validate that it is connected. */
    if (host) {
        client *replica = findReplica(host, port);

        if (replica == NULL) {
            addReplyError(c,"FAILOVER target HOST and PORT is not "
                            "a replica.");
            return;
        }

        /* Check if requested replica is online */
        if (replica->replstate != SLAVE_STATE_ONLINE) {
            addReplyError(c,"FAILOVER target replica is not online.");
            return;
        }

        g_pserver->target_replica_host = zstrdup(host);
        g_pserver->target_replica_port = port;
        serverLog(LL_NOTICE,"FAILOVER requested to %s:%ld.",host,port);
    } else {
        serverLog(LL_NOTICE,"FAILOVER requested to any replica.");
    }

    mstime_t now = mstime();
    if (timeout_in_ms) {
        g_pserver->failover_end_time = now + timeout_in_ms;
    }
    
    g_pserver->force_failover = force_flag;
    g_pserver->failover_state = FAILOVER_WAIT_FOR_SYNC;
    /* Cluster failover will unpause eventually */
    pauseClients(LLONG_MAX,CLIENT_PAUSE_WRITE);
    addReply(c,shared.ok);
}

int FBrokenLinkToMaster(int *pmastersOnline)
{
    listIter li;
    listNode *ln;
    listRewind(g_pserver->masters, &li);

    int connected = 0;
    while ((ln = listNext(&li)))
    {
        redisMaster *mi = (redisMaster*)listNodeValue(ln);
        if (mi->repl_state == REPL_STATE_CONNECTED)
            ++connected;
    }


    if (pmastersOnline != nullptr)
        *pmastersOnline = connected;

    if (g_pserver->repl_quorum < 0) {
        return connected < (int)listLength(g_pserver->masters);
    } else {
        return connected < g_pserver->repl_quorum;
    }

    return false;
}

int FActiveMaster(client *c)
{
    if (!(c->flags & CLIENT_MASTER))
        return false;

    listIter li;
    listNode *ln;
    listRewind(g_pserver->masters, &li);
    while ((ln = listNext(&li)))
    {
        redisMaster *mi = (redisMaster*)listNodeValue(ln);
        if (mi->master == c)
            return true;
    }
    return false;
}

redisMaster *MasterInfoFromClient(client *c)
{
    listIter li;
    listNode *ln;
    listRewind(g_pserver->masters, &li);
    while ((ln = listNext(&li)))
    {
        redisMaster *mi = (redisMaster*)listNodeValue(ln);
        if (mi->master == c || mi->cached_master == c)
            return mi;
    }
    return nullptr;
}

#define REPLAY_MAX_NESTING 64
class ReplicaNestState
{
public:
    bool FPush()
    {
        if (m_cnesting == REPLAY_MAX_NESTING) {
            m_fCancelled = true;
            return false;   // overflow
        }
        
        if (m_cnesting == 0)
            m_fCancelled = false;
        ++m_cnesting;
        return true;
    }

    void Pop()
    {
        --m_cnesting;
    }

    void Cancel()
    {
        m_fCancelled = true;
    }

    bool FCancelled() const
    {
        return m_fCancelled;
    }

    bool FFirst() const
    {
        return m_cnesting == 1;
    }

    redisMaster *getMi(client *c)
    {
        if (m_mi == nullptr)
            m_mi = MasterInfoFromClient(c);
        return m_mi;
    }

    int nesting() const { return m_cnesting; }

private:
    int m_cnesting = 0;
    bool m_fCancelled = false;
    redisMaster *m_mi = nullptr;
};

static thread_local std::unique_ptr<ReplicaNestState> s_pstate;

bool FInReplicaReplay()
{
    return s_pstate != nullptr && s_pstate->nesting() > 0;
}

struct RemoteMasterState
{
    uint64_t mvcc = 0;
    client *cFake = nullptr;

    ~RemoteMasterState()
    {
        aeAcquireLock();
        freeClient(cFake);
        aeReleaseLock();
    }
};

static std::unordered_map<std::string, RemoteMasterState> g_mapremote;

void replicaReplayCommand(client *c)
{
    if (s_pstate == nullptr)
        s_pstate = std::make_unique<ReplicaNestState>();

    // the replay command contains two arguments: 
    //  1: The UUID of the source
    //  2: The raw command buffer to be replayed
    //  3: (OPTIONAL) the database ID the command should apply to
    
    if (!(c->flags & CLIENT_MASTER))
    {
        addReplyError(c, "Command must be sent from a master");
        s_pstate->Cancel();
        return;
    }

    /* First Validate Arguments */
    if (c->argc < 3)
    {
        addReplyError(c, "Invalid number of arguments");
        s_pstate->Cancel();
        return;
    }

    std::string uuid;
    uuid.resize(UUID_BINARY_LEN);
    if (c->argv[1]->type != OBJ_STRING || sdslen((sds)ptrFromObj(c->argv[1])) != 36 
        || uuid_parse((sds)ptrFromObj(c->argv[1]), (unsigned char*)uuid.data()) != 0)
    {
        addReplyError(c, "Expected UUID arg1");
        s_pstate->Cancel();
        return;
    }

    if (c->argv[2]->type != OBJ_STRING)
    {
        addReplyError(c, "Expected command buffer arg2");
        s_pstate->Cancel();
        return;
    }

    if (c->argc >= 4)
    {
        long long db;
        if (getLongLongFromObject(c->argv[3], &db) != C_OK || db >= cserver.dbnum || selectDb(c, (int)db) != C_OK)
        {
            addReplyError(c, "Invalid database ID");
            s_pstate->Cancel();
            return;
        }
    }

    uint64_t mvcc = 0;
    if (c->argc >= 5)
    {
        if (getUnsignedLongLongFromObject(c->argv[4], &mvcc) != C_OK)
        {
            addReplyError(c, "Invalid MVCC Timestamp");
            s_pstate->Cancel();
            return;
        }
    }

    if (FSameUuidNoNil((unsigned char*)uuid.data(), cserver.uuid))
    {
        addReply(c, shared.ok);
        s_pstate->Cancel();
        return; // Our own commands have come back to us.  Ignore them.
    }

    if (!s_pstate->FPush())
        return;

    RemoteMasterState &remoteState = g_mapremote[uuid];
    if (remoteState.cFake == nullptr)
        remoteState.cFake = createClient(nullptr, c->iel);
    else
        remoteState.cFake->iel = c->iel;

    client *cFake = remoteState.cFake;

    if (mvcc != 0 && remoteState.mvcc >= mvcc)
    {
        s_pstate->Cancel();
        s_pstate->Pop();
        return;
    }

    // OK We've recieved a command lets execute
    client *current_clientSave = serverTL->current_client;
    cFake->lock.lock();
    cFake->authenticated = c->authenticated;
    cFake->user = c->user;
    cFake->querybuf = sdscatsds(cFake->querybuf,(sds)ptrFromObj(c->argv[2]));
    cFake->read_reploff = sdslen(cFake->querybuf);
    cFake->reploff = 0;
    selectDb(cFake, c->db->id);
    auto ccmdPrev = serverTL->commandsExecuted;
    cFake->flags |= CLIENT_MASTER | CLIENT_PREVENT_REPL_PROP;
    processInputBuffer(cFake, true /*fParse*/, (CMD_CALL_FULL & (~CMD_CALL_PROPAGATE)));
    cFake->flags &= ~(CLIENT_MASTER | CLIENT_PREVENT_REPL_PROP);
    bool fExec = ccmdPrev != serverTL->commandsExecuted;
    bool fNoPropogate = false;
    cFake->lock.unlock();
    if (cFake->master_error)
    {
        selectDb(c, cFake->db->id);
        freeClient(cFake);
        remoteState.cFake = cFake = nullptr;
        addReplyError(c, "Error in rreplay command, please check logs.");
    }
    if (cFake != nullptr)
    {
        if (fExec || cFake->flags & CLIENT_MULTI)
        {
            addReply(c, shared.ok);
            selectDb(c, cFake->db->id);
            if (mvcc > remoteState.mvcc)
                remoteState.mvcc = mvcc;
            serverAssert(sdslen(cFake->querybuf) == 0);
        }
        else
        {
            serverLog(LL_WARNING, "Command didn't execute: %s", cFake->buf);
            addReplyError(c, "command did not execute");
            if (sdslen(cFake->querybuf)) {
                serverLog(LL_WARNING, "Closing connection to MASTER because of an unrecoverable protocol error");
                freeClientAsync(c);
                fNoPropogate = true;    // don't keep transmitting corrupt data
            }
        }
    }
    serverTL->current_client = current_clientSave;

    // call() will not propogate this for us, so we do so here
    if (!s_pstate->FCancelled() && s_pstate->FFirst() && !cserver.multimaster_no_forward && !fNoPropogate)
        alsoPropagate(cserver.rreplayCommand,c->db->id,c->argv,c->argc,PROPAGATE_AOF|PROPAGATE_REPL);
    
    s_pstate->Pop();
    return;
}

void updateMasterAuth()
{
    listIter li;
    listNode *ln;

    listRewind(g_pserver->masters, &li);
    while ((ln = listNext(&li)))
    {
        redisMaster *mi = (redisMaster*)listNodeValue(ln);
        sdsfree(mi->masterauth); mi->masterauth = nullptr;
        zfree(mi->masteruser); mi->masteruser = nullptr;

        if (cserver.default_masterauth)
            mi->masterauth = sdsdup(cserver.default_masterauth);
        if (cserver.default_masteruser)
            mi->masteruser = zstrdup(cserver.default_masteruser);
    }
}

static void propagateMasterStaleKeys()
{
    listIter li;
    listNode *ln;
    listRewind(g_pserver->masters, &li);
    robj *rgobj[2];

    rgobj[0] = createEmbeddedStringObject("DEL", 3);

    while ((ln = listNext(&li)) != nullptr)
    {
        redisMaster *mi = (redisMaster*)listNodeValue(ln);
        if (mi->staleKeyMap != nullptr)
        {
            if (mi->master != nullptr)
            {
                for (auto &pair : *mi->staleKeyMap)
                {
                    if (pair.second.empty())
                        continue;
                    
                    client *replica = replicaFromMaster(mi);
                    if (replica == nullptr)
                        continue;

                    for (auto &spkey : pair.second)
                    {
                        rgobj[1] = spkey.get();
                        replicationFeedSlave(replica, pair.first, rgobj, 2, false);
                    }
                }
                delete mi->staleKeyMap;
                mi->staleKeyMap = nullptr;
            }
        }
    }

    decrRefCount(rgobj[0]);
}

void replicationNotifyLoadedKey(redisDb *db, robj_roptr key, robj_roptr val, long long expire) {
    if (!g_pserver->fActiveReplica || listLength(g_pserver->slaves) == 0)
        return;

    // Send a digest over to the replicas
    rio r;

    createDumpPayload(&r, val, key.unsafe_robjcast());

    redisObjectStack objPayload;
    initStaticStringObject(objPayload, r.io.buffer.ptr);
    redisObjectStack objTtl;
    initStaticStringObject(objTtl, sdscatprintf(sdsempty(), "%lld", expire));
    redisObjectStack objMvcc;
    initStaticStringObject(objMvcc, sdscatprintf(sdsempty(), "%" PRIu64, mvccFromObj(val)));
    redisObject *argv[5] = {shared.mvccrestore, key.unsafe_robjcast(), &objMvcc, &objTtl, &objPayload};

    replicationFeedSlaves(g_pserver->slaves, db->id, argv, 5);

    sdsfree(szFromObj(&objTtl));
    sdsfree(szFromObj(&objMvcc));
    sdsfree(r.io.buffer.ptr);
}

void replicateSubkeyExpire(redisDb *db, robj_roptr key, robj_roptr subkey, long long expire) {
    if (!g_pserver->fActiveReplica || listLength(g_pserver->slaves) == 0)
        return;

    redisObjectStack objTtl;
    initStaticStringObject(objTtl, sdscatprintf(sdsempty(), "%lld", expire));
    redisObject *argv[4] = {shared.pexpirememberat, key.unsafe_robjcast(), subkey.unsafe_robjcast(), &objTtl};
    replicationFeedSlaves(g_pserver->slaves, db->id, argv, 4);

    sdsfree(szFromObj(&objTtl));
}

void _clientAsyncReplyBufferReserve(client *c, size_t len);

void flushReplBacklogToClients()
{
    serverAssert(GlobalLocksAcquired());
    /* If we have the repl backlog lock, we will deadlock */
    serverAssert(!g_pserver->repl_backlog_lock.fOwnLock());
    if (g_pserver->repl_batch_offStart < 0)
        return;
    
    if (g_pserver->repl_batch_offStart != g_pserver->master_repl_offset) {
        bool fAsyncWrite = false;
        long long min_offset = LLONG_MAX;
        // Ensure no overflow
        serverAssert(g_pserver->repl_batch_offStart < g_pserver->master_repl_offset);
        if (g_pserver->master_repl_offset - g_pserver->repl_batch_offStart > g_pserver->repl_backlog_size) {
            // We overflowed
            listIter li;
            listNode *ln;
            listRewind(g_pserver->slaves, &li);
            while ((ln = listNext(&li))) {
                client *c = (client*)listNodeValue(ln);
                sds sdsClient = catClientInfoString(sdsempty(),c);
                freeClientAsync(c);
                serverLog(LL_WARNING,"Client %s scheduled to be closed ASAP for overcoming of output buffer limits.", sdsClient);
                sdsfree(sdsClient);
            }
            goto LDone;
        }

        // Ensure no overflow if we get here
        serverAssert(g_pserver->master_repl_offset - g_pserver->repl_batch_offStart <= g_pserver->repl_backlog_size);
        serverAssert(g_pserver->repl_batch_idxStart != g_pserver->repl_backlog_idx);

        // Repl backlog writes must become visible to all threads at this point
        std::atomic_thread_fence(std::memory_order_release);

        listIter li;
        listNode *ln;
        listRewind(g_pserver->slaves, &li);
        /* We don't actually write any data in this function since we send data 
         * directly from the replication backlog to replicas in writeToClient.
         * 
         * What we do however, is set the end offset of each replica here. This way, 
         * future calls to writeToClient will know up to where in the replication
         * backlog is valid for writing. */  
        while ((ln = listNext(&li))) {
            client *replica = (client*)listNodeValue(ln);

            if (!canFeedReplicaReplBuffer(replica)) continue;
            if (replica->flags & CLIENT_CLOSE_ASAP) continue;

            std::unique_lock<fastlock> ul(replica->lock);
            if (!FCorrectThread(replica))
                fAsyncWrite = true;

            /* We should have set the repl_curr_off when synchronizing, so it shouldn't be -1 here */
            serverAssert(replica->repl_curr_off != -1);

            min_offset = std::min(min_offset, replica->repl_curr_off);      

            replica->repl_end_off = g_pserver->master_repl_offset;

            /* Only if the there isn't already a pending write do we prepare the client to write */
            serverAssert(replica->repl_curr_off != g_pserver->master_repl_offset);
            prepareClientToWrite(replica);
        }
        if (fAsyncWrite)
            ProcessPendingAsyncWrites();

LDone:
        // This may be called multiple times per "frame" so update with our progress flushing to clients
        g_pserver->repl_batch_idxStart = g_pserver->repl_backlog_idx;
        g_pserver->repl_batch_offStart = g_pserver->master_repl_offset;
        g_pserver->repl_lowest_off.store(min_offset == LLONG_MAX ? -1 : min_offset, std::memory_order_seq_cst);
    } 
}


/* Failover cron function, checks coordinated failover state. 
 *
 * Implementation note: The current implementation calls replicationSetMaster()
 * to start the failover request, this has some unintended side effects if the
 * failover doesn't work like blocked clients will be unblocked and replicas will
 * be disconnected. This could be optimized further.
 */
void updateFailoverStatus(void) {
    if (g_pserver->failover_state != FAILOVER_WAIT_FOR_SYNC) return;
    serverAssert(!g_pserver->fActiveReplica);
    mstime_t now = g_pserver->mstime;

    /* Check if failover operation has timed out */
    if (g_pserver->failover_end_time && g_pserver->failover_end_time <= now) {
        if (g_pserver->force_failover) {
            serverLog(LL_NOTICE,
                "FAILOVER to %s:%d time out exceeded, failing over.",
                g_pserver->target_replica_host, g_pserver->target_replica_port);
            g_pserver->failover_state = FAILOVER_IN_PROGRESS;
            /* If timeout has expired force a failover if requested. */
            replicationAddMaster(g_pserver->target_replica_host,
                g_pserver->target_replica_port);
            return;
        } else {
            /* Force was not requested, so timeout. */
            redisMaster *mi = listLength(g_pserver->masters) ? (redisMaster*)listNodeValue(listFirst(g_pserver->masters)) : nullptr;
            abortFailover(mi, "Replica never caught up before timeout");
            return;
        }
    }

    /* Check to see if the replica has caught up so failover can start */
    client *replica = NULL;
    if (g_pserver->target_replica_host) {
        replica = findReplica(g_pserver->target_replica_host, 
            g_pserver->target_replica_port);
    } else {
        listIter li;
        listNode *ln;

        listRewind(g_pserver->slaves,&li);
        /* Find any replica that has matched our repl_offset */
        while((ln = listNext(&li))) {
            replica = (client*)listNodeValue(ln);
            if (replica->repl_ack_off == g_pserver->master_repl_offset) {
                char ip[NET_IP_STR_LEN], *replicaaddr = replica->slave_addr;

                if (!replicaaddr) {
                    if (connPeerToString(replica->conn,ip,sizeof(ip),NULL) == -1)
                        continue;
                    replicaaddr = ip;
                }

                /* We are now failing over to this specific node */
                g_pserver->target_replica_host = zstrdup(replicaaddr);
                g_pserver->target_replica_port = replica->slave_listening_port;
                break;
            }
        }
    }

    /* We've found a replica that is caught up */
    if (replica && (replica->repl_ack_off == g_pserver->master_repl_offset)) {
        g_pserver->failover_state = FAILOVER_IN_PROGRESS;
        serverLog(LL_NOTICE,
                "Failover target %s:%d is synced, failing over.",
                g_pserver->target_replica_host, g_pserver->target_replica_port);
        /* Designated replica is caught up, failover to it. */
        replicationAddMaster(g_pserver->target_replica_host,
            g_pserver->target_replica_port);
    }
}

// If we automatically grew the backlog we need to trim it back to
//  the config setting when possible
void trimReplicationBacklog() {
    serverAssert(GlobalLocksAcquired());
    serverAssert(g_pserver->repl_batch_offStart < 0);   // we shouldn't be in a batch
    if (g_pserver->repl_backlog_size <= g_pserver->repl_backlog_config_size)
        return; // We're already a good size
    if (g_pserver->repl_lowest_off > 0 && (g_pserver->master_repl_offset - g_pserver->repl_lowest_off + 1) > g_pserver->repl_backlog_config_size)
        return; // There is untransmitted data we can't truncate
    if (cserver.force_backlog_disk && g_pserver->repl_backlog == g_pserver->repl_backlog_disk)
        return; // We're already in the disk backlog and we're told to stay there

    serverLog(LL_NOTICE, "Reclaiming %lld replication backlog bytes", g_pserver->repl_backlog_size - g_pserver->repl_backlog_config_size);
    resizeReplicationBacklog(g_pserver->repl_backlog_config_size);
}
