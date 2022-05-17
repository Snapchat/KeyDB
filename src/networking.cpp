/*
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
#include "atomicvar.h"
#include "cluster.h"
#include <sys/socket.h>
#include <sys/uio.h>
#include <math.h>
#include <ctype.h>
#include <vector>
#include <mutex>
#include "aelocker.h"

static void setProtocolError(const char *errstr, client *c);
__thread int ProcessingEventsWhileBlocked = 0; /* See processEventsWhileBlocked(). */

/* Return the size consumed from the allocator, for the specified SDS string,
 * including internal fragmentation. This function is used in order to compute
 * the client output buffer size. */
size_t sdsZmallocSize(sds s) {
    void *sh = sdsAllocPtr(s);
    return zmalloc_size(sh);
}

/* Return the amount of memory used by the sds string at object->ptr
 * for a string object. This includes internal fragmentation. */
size_t getStringObjectSdsUsedMemory(robj *o) {
    serverAssertWithInfo(NULL,o,o->type == OBJ_STRING);
    switch(o->encoding) {
    case OBJ_ENCODING_RAW: return sdsZmallocSize((sds)ptrFromObj(o));
    case OBJ_ENCODING_EMBSTR: return zmalloc_size(allocPtrFromObj(o))-sizeof(robj);
    default: return 0; /* Just integer encoding for now. */
    }
}

/* Return the length of a string object.
 * This does NOT includes internal fragmentation or sds unused space. */
size_t getStringObjectLen(robj *o) {
    serverAssertWithInfo(NULL,o,o->type == OBJ_STRING);
    switch(o->encoding) {
    case OBJ_ENCODING_RAW: return sdslen(szFromObj(o));
    case OBJ_ENCODING_EMBSTR: return sdslen(szFromObj(o));
    default: return 0; /* Just integer encoding for now. */
    }
}

/* Client.reply list dup and free methods. */
void *dupClientReplyValue(void *o) {
    clientReplyBlock *old = (clientReplyBlock*)o;
    clientReplyBlock *buf = (clientReplyBlock*)zmalloc(sizeof(clientReplyBlock) + old->size, MALLOC_LOCAL);
    memcpy(buf, o, sizeof(clientReplyBlock) + old->size);
    return buf;
}

void freeClientReplyValue(const void *o) {
    zfree(o);
}

int listMatchObjects(void *a, void *b) {
    return equalStringObjects((robj*)a,(robj*)b);
}

/* This function links the client to the global linked list of clients.
 * unlinkClient() does the opposite, among other things. */
void linkClient(client *c) {
    serverAssert(GlobalLocksAcquired());
    listAddNodeTail(g_pserver->clients,c);
    /* Note that we remember the linked list node where the client is stored,
     * this way removing the client in unlinkClient() will not require
     * a linear scan, but just a constant time operation. */
    c->client_list_node = listLast(g_pserver->clients);
    if (c->conn != nullptr) atomicIncr(g_pserver->rgthreadvar[c->iel].cclients, 1);
    uint64_t id = htonu64(c->id);
    raxInsert(g_pserver->clients_index,(unsigned char*)&id,sizeof(id),c,NULL);
}

/* Initialize client authentication state.
 */
static void clientSetDefaultAuth(client *c) {
    /* If the default user does not require authentication, the user is
     * directly authenticated. */
    c->user = DefaultUser;
    c->authenticated = (c->user->flags & USER_FLAG_NOPASS) &&
                       !(c->user->flags & USER_FLAG_DISABLED);
}

int authRequired(client *c) {
    /* Check if the user is authenticated. This check is skipped in case
     * the default user is flagged as "nopass" and is active. */
    int auth_required = (!(DefaultUser->flags & USER_FLAG_NOPASS) ||
                          (DefaultUser->flags & USER_FLAG_DISABLED)) &&
                        !c->authenticated;
    return auth_required;
}

client *createClient(connection *conn, int iel) {
    client *c = new client;
    serverAssert(conn == nullptr || (iel == (serverTL - g_pserver->rgthreadvar)));

    c->iel = iel;
    /* passing NULL as conn it is possible to create a non connected client.
     * This is useful since all the commands needs to be executed
     * in the context of a client. When commands are executed in other
     * contexts (for instance a Lua script) we need a non connected client. */
    if (conn) {
        serverAssert(iel == (serverTL - g_pserver->rgthreadvar));
        connNonBlock(conn);
        connEnableTcpNoDelay(conn);
        if (cserver.tcpkeepalive)
            connKeepAlive(conn,cserver.tcpkeepalive);
        connSetReadHandler(conn, readQueryFromClient, true);
        connSetPrivateData(conn, c);
    }

    selectDb(c,0);
    uint64_t client_id;
    client_id = g_pserver->next_client_id.fetch_add(1);
    c->iel = iel;
    c->id = client_id;
    sprintf(c->lock.szName, "client %" PRIu64, client_id);
    c->resp = 2;
    c->conn = conn;
    c->name = NULL;
    c->bufpos = 0;
    c->qb_pos = 0;
    c->querybuf = sdsempty();
    c->pending_querybuf = sdsempty();
    c->querybuf_peak = 0;
    c->reqtype = 0;
    c->argc = 0;
    c->argv = NULL;
    c->original_argc = 0;
    c->original_argv = NULL;
    c->cmd = c->lastcmd = NULL;
    c->multibulklen = 0;
    c->bulklen = -1;
    c->sentlen = 0;
    c->sentlenAsync = 0;
    c->flags = 0;
    c->fPendingAsyncWrite = FALSE;
    c->fPendingAsyncWriteHandler = FALSE;
    c->ctime = c->lastinteraction = g_pserver->unixtime;
    /* If the default user does not require authentication, the user is
     * directly authenticated. */
    clientSetDefaultAuth(c);
    c->replstate = REPL_STATE_NONE;
    c->repl_put_online_on_ack = 0;
    c->reploff = 0;
    c->read_reploff = 0;
    c->reploff_cmd = 0;
    c->repl_ack_off = 0;
    c->repl_ack_time = 0;
    c->repl_down_since = 0;
    c->repl_last_partial_write = 0;
    c->slave_listening_port = 0;
    c->slave_addr = NULL;
    c->slave_capa = SLAVE_CAPA_NONE;
    c->reply = listCreate();
    c->reply_bytes = 0;
    c->obuf_soft_limit_reached_time = 0;
    listSetFreeMethod(c->reply,freeClientReplyValue);
    listSetDupMethod(c->reply,dupClientReplyValue);
    c->btype = BLOCKED_NONE;
    c->bpop.timeout = 0;
    c->bpop.keys = dictCreate(&objectKeyHeapPointerValueDictType,NULL);
    c->bpop.target = NULL;
    c->bpop.xread_group = NULL;
    c->bpop.xread_consumer = NULL;
    c->bpop.xread_group_noack = 0;
    c->bpop.numreplicas = 0;
    c->bpop.reploffset = 0;
    c->woff = 0;
    c->watched_keys = listCreate();
    c->pubsub_channels = dictCreate(&objectKeyPointerValueDictType,NULL);
    c->pubsub_patterns = listCreate();
    c->peerid = NULL;
    c->sockname = NULL;
    c->client_list_node = NULL;
    c->replyAsync = NULL;
    c->paused_list_node = NULL;
    c->client_tracking_redirection = 0;
    c->casyncOpsPending = 0;
    c->mvccCheckpoint = 0;
    c->master_error = 0;
    memset(c->uuid, 0, UUID_BINARY_LEN);

    c->client_tracking_prefixes = NULL;
    c->client_cron_last_memory_usage = 0;
    c->client_cron_last_memory_type = CLIENT_TYPE_NORMAL;
    c->auth_callback = NULL;
    c->auth_callback_privdata = NULL;
    c->auth_module = NULL;
    listSetFreeMethod(c->pubsub_patterns,decrRefCountVoid);
    listSetMatchMethod(c->pubsub_patterns,listMatchObjects);
    if (conn) linkClient(c);
    initClientMultiState(c);
    AssertCorrectThread(c);
    return c;
}

size_t client::argv_len_sum() const {
    size_t sum = 0;
    for (auto &cmd : vecqueuedcmd)
        sum += cmd.argv_len_sum;
    return sum + argv_len_sumActive;
}

/* This function puts the client in the queue of clients that should write
 * their output buffers to the socket. Note that it does not *yet* install
 * the write handler, to start clients are put in a queue of clients that need
 * to write, so we try to do that before returning in the event loop (see the
 * handleClientsWithPendingWrites() function).
 * If we fail and there is more data to write, compared to what the socket
 * buffers can hold, then we'll really install the handler. */
void clientInstallWriteHandler(client *c) {
    /* Schedule the client to write the output buffers to the socket only
     * if not already done and, for slaves, if the replica can actually receive
     * writes at this stage. */

    if (!(c->flags & CLIENT_PENDING_WRITE) &&
        (c->replstate == REPL_STATE_NONE || c->replstate == SLAVE_STATE_FASTSYNC_TX || c->replstate == SLAVE_STATE_FASTSYNC_DONE ||
         (c->replstate == SLAVE_STATE_ONLINE && !c->repl_put_online_on_ack)))
    {
        AssertCorrectThread(c);
        serverAssert(c->lock.fOwnLock());
        /* Here instead of installing the write handler, we just flag the
         * client and put it into a list of clients that have something
         * to write to the socket. This way before re-entering the event
         * loop, we can try to directly write to the client sockets avoiding
         * a system call. We'll only really install the write handler if
         * we'll not be able to write the whole reply at once. */
        c->flags |= CLIENT_PENDING_WRITE;
        std::unique_lock<fastlock> lockf(g_pserver->rgthreadvar[c->iel].lockPendingWrite);
        g_pserver->rgthreadvar[c->iel].clients_pending_write.push_back(c);
    }
}

void clientInstallAsyncWriteHandler(client *c) {
    serverAssert(GlobalLocksAcquired());
    if (!(c->fPendingAsyncWrite)) {
        c->fPendingAsyncWrite = TRUE;
        listAddNodeHead(serverTL->clients_pending_asyncwrite,c);
    }
}

/* This function is called every time we are going to transmit new data
 * to the client. The behavior is the following:
 *
 * If the client should receive new data (normal clients will) the function
 * returns C_OK, and make sure to install the write handler in our event
 * loop so that when the socket is writable new data gets written.
 *
 * If the client should not receive new data, because it is a fake client
 * (used to load AOF in memory), a master or because the setup of the write
 * handler failed, the function returns C_ERR.
 *
 * The function may return C_OK without actually installing the write
 * event handler in the following cases:
 *
 * 1) The event handler should already be installed since the output buffer
 *    already contains something.
 * 2) The client is a replica but not yet online, so we want to just accumulate
 *    writes in the buffer but not actually sending them yet.
 *
 * Typically gets called every time a reply is built, before adding more
 * data to the clients output buffers. If the function returns C_ERR no
 * data should be appended to the output buffers. */
int prepareClientToWrite(client *c) {
    bool fAsync = !FCorrectThread(c);  // Not async if we're on the right thread

	if (!fAsync) {
		serverAssert(c->conn == nullptr || c->lock.fOwnLock());
	} else {
		serverAssert(GlobalLocksAcquired());
	}

    auto flags = c->flags.load(std::memory_order_relaxed);

    if (flags & CLIENT_FORCE_REPLY) return C_OK; // FORCE REPLY means we're doing something else with the buffer.
                                                // do not install a write handler

    /* If it's the Lua client we always return ok without installing any
     * handler since there is no socket at all. */
    if (flags & (CLIENT_LUA|CLIENT_MODULE)) return C_OK;

    /* If CLIENT_CLOSE_ASAP flag is set, we need not write anything. */
    if (c->flags & CLIENT_CLOSE_ASAP) return C_ERR;

    /* CLIENT REPLY OFF / SKIP handling: don't send replies. */
    if (flags & (CLIENT_REPLY_OFF|CLIENT_REPLY_SKIP)) return C_ERR;

    /* Masters don't receive replies, unless CLIENT_MASTER_FORCE_REPLY flag
     * is set. */
    if ((flags & CLIENT_MASTER) &&
        !(flags & CLIENT_MASTER_FORCE_REPLY)) return C_ERR;

    if (!c->conn) return C_ERR; /* Fake client for AOF loading. */

    /* Schedule the client to write the output buffers to the socket, unless
     * it should already be setup to do so (it has already pending data). */
    if (!fAsync && (c->flags & CLIENT_SLAVE || !clientHasPendingReplies(c))) clientInstallWriteHandler(c);
    if (fAsync && !(c->fPendingAsyncWrite)) clientInstallAsyncWriteHandler(c);

    /* Authorize the caller to queue in the output buffer of this client. */
    return C_OK;
}

/* -----------------------------------------------------------------------------
 * Low level functions to add more data to output buffers.
 * -------------------------------------------------------------------------- */

void _clientAsyncReplyBufferReserve(client *c, size_t len) {
    if (c->replyAsync != nullptr)
        return;
    size_t newsize = std::max(len, (size_t)PROTO_ASYNC_REPLY_CHUNK_BYTES);
    clientReplyBlock *replyNew = (clientReplyBlock*)zmalloc(sizeof(clientReplyBlock) + newsize);
    replyNew->size = zmalloc_usable_size(replyNew) - sizeof(clientReplyBlock);
    replyNew->used = 0;
    c->replyAsync = replyNew;
}

/* Attempts to add the reply to the static buffer in the client struct.
 * Returns C_ERR if the buffer is full, or the reply list is not empty,
 * in which case the reply must be added to the reply list. */
int _addReplyToBuffer(client *c, const char *s, size_t len) {
    if (c->flags.load(std::memory_order_relaxed) & CLIENT_CLOSE_AFTER_REPLY) return C_OK;

    bool fAsync = !FCorrectThread(c);
    if (fAsync)
    {
        serverAssert(GlobalLocksAcquired());
        if (c->replyAsync == nullptr || (c->replyAsync->size - c->replyAsync->used) < len)
        {
            if (c->replyAsync == nullptr) {
                size_t newsize = std::max(len, (size_t)PROTO_ASYNC_REPLY_CHUNK_BYTES);
                    
                clientReplyBlock *replyNew = (clientReplyBlock*)zmalloc(sizeof(clientReplyBlock) + newsize);
                replyNew->size = zmalloc_usable_size(replyNew) - sizeof(clientReplyBlock);
                replyNew->used = 0;
                c->replyAsync = replyNew;
            } else {
                size_t newsize = std::max(c->replyAsync->used + len, c->replyAsync->size*2);
                clientReplyBlock *replyNew = (clientReplyBlock*)zmalloc(sizeof(clientReplyBlock) + newsize);
                replyNew->size = zmalloc_usable_size(replyNew) - sizeof(clientReplyBlock);
                replyNew->used = c->replyAsync->used;
                memcpy(replyNew->buf(), c->replyAsync->buf(), c->replyAsync->used);
                zfree(c->replyAsync);
                c->replyAsync = replyNew;
            }
        }
        memcpy(c->replyAsync->buf() + c->replyAsync->used,s,len);
        c->replyAsync->used += len;
    }
    else
    {
        size_t available = sizeof(c->buf)-c->bufpos;

        /* If there already are entries in the reply list, we cannot
        * add anything more to the static buffer. */
        if (listLength(c->reply) > 0) return C_ERR;

        /* Check that the buffer has enough space available for this string. */
        if (len > available) return C_ERR;

        memcpy(c->buf+c->bufpos,s,len);
        c->bufpos+=len;
    }
    return C_OK;
}

/* Adds the reply to the reply linked list.
 * Note: some edits to this function need to be relayed to AddReplyFromClient. */
void _addReplyProtoToList(client *c, const char *s, size_t len) {
    if (c->flags.load(std::memory_order_relaxed) & CLIENT_CLOSE_AFTER_REPLY) return;
    AssertCorrectThread(c);

    listNode *ln = listLast(c->reply);
    clientReplyBlock *tail = (clientReplyBlock*) (ln? listNodeValue(ln): NULL);

    /* Note that 'tail' may be NULL even if we have a tail node, because when
     * addReplyDeferredLen() is used, it sets a dummy node to NULL just
     * fo fill it later, when the size of the bulk length is set. */

    /* Append to tail string when possible. */
    if (tail) {
        /* Copy the part we can fit into the tail, and leave the rest for a
         * new node */
        size_t avail = tail->size - tail->used;
        size_t copy = avail >= len? len: avail;
        memcpy(tail->buf() + tail->used, s, copy);
        tail->used += copy;
        s += copy;
        len -= copy;
    }
    if (len) {
        /* Create a new node, make sure it is allocated to at
         * least PROTO_REPLY_CHUNK_BYTES */
        size_t size = len < PROTO_REPLY_CHUNK_BYTES? PROTO_REPLY_CHUNK_BYTES: len;
        tail = (clientReplyBlock*)zmalloc(size + sizeof(clientReplyBlock), MALLOC_LOCAL);
        /* take over the allocation's internal fragmentation */
        tail->size = zmalloc_usable_size(tail) - sizeof(clientReplyBlock);
        tail->used = len;
        memcpy(tail->buf(), s, len);
        listAddNodeTail(c->reply, tail);
        c->reply_bytes += tail->size;

        closeClientOnOutputBufferLimitReached(c, 1);
    }
}

/* -----------------------------------------------------------------------------
 * Higher level functions to queue data on the client output buffer.
 * The following functions are the ones that commands implementations will call.
 * -------------------------------------------------------------------------- */
/* Add the object 'obj' string representation to the client output buffer. */
void addReply(client *c, robj_roptr obj) {
    if (prepareClientToWrite(c) != C_OK) return;

    if (sdsEncodedObject(obj)) {
        if (_addReplyToBuffer(c,(const char*)ptrFromObj(obj),sdslen((sds)ptrFromObj(obj))) != C_OK)
            _addReplyProtoToList(c,(const char*)ptrFromObj(obj),sdslen((sds)ptrFromObj(obj)));
    } else if (obj->encoding == OBJ_ENCODING_INT) {
        /* For integer encoded strings we just convert it into a string
         * using our optimized function, and attach the resulting string
         * to the output buffer. */
        char buf[32];
        size_t len = ll2string(buf,sizeof(buf),(long)ptrFromObj(obj));
        if (_addReplyToBuffer(c,buf,len) != C_OK)
            _addReplyProtoToList(c,buf,len);
    } else {
        serverPanic("Wrong obj->encoding in addReply()");
    }
}

/* Add the SDS 's' string to the client output buffer, as a side effect
 * the SDS string is freed. */
void addReplySds(client *c, sds s) {
    if (prepareClientToWrite(c) != C_OK) {
        /* The caller expects the sds to be free'd. */
        sdsfree(s);
        return;
    }
    if (_addReplyToBuffer(c,s,sdslen(s)) != C_OK)
        _addReplyProtoToList(c,s,sdslen(s));
    sdsfree(s);
}

/* This low level function just adds whatever protocol you send it to the
 * client buffer, trying the static buffer initially, and using the string
 * of objects if not possible.
 *
 * It is efficient because does not create an SDS object nor an Redis object
 * if not needed. The object will only be created by calling
 * _addReplyProtoToList() if we fail to extend the existing tail object
 * in the list of objects. */
void addReplyProto(client *c, const char *s, size_t len) {
    if (prepareClientToWrite(c) != C_OK) return;
    if (_addReplyToBuffer(c,s,len) != C_OK)
        _addReplyProtoToList(c,s,len);
}

void addReplyProtoCString(client *c, const char *s) {
    addReplyProto(c, s, strlen(s));
}

std::string escapeString(sds str)
{
    std::string newstr;
    size_t len = sdslen(str);
    for (size_t ich = 0; ich < len; ++ich)
    {
        char ch = str[ich];
        switch (ch)
        {
        case '\n':
            newstr += "\\n";
            break;

        case '\t':
            newstr += "\\t";
            break;

        case '\r':
            newstr += "\\r";
            break;
        
        default:
            newstr += ch;
        }
    }
    return newstr;
}

/* Low level function called by the addReplyError...() functions.
 * It emits the protocol for a Redis error, in the form:
 *
 * -ERRORCODE Error Message<CR><LF>
 *
 * If the error code is already passed in the string 's', the error
 * code provided is used, otherwise the string "-ERR " for the generic
 * error code is automatically added.
 * Note that 's' must NOT end with \r\n. */
void addReplyErrorLength(client *c, const char *s, size_t len) {
    /* If the string already starts with "-..." then the error code
     * is provided by the caller. Otherwise we use "-ERR". */
    if (!len || s[0] != '-') addReplyProto(c,"-ERR ",5);
    addReplyProto(c,s,len);
    addReplyProto(c,"\r\n",2);
}

/* Do some actions after an error reply was sent (Log if needed, updates stats, etc.) */
void afterErrorReply(client *c, const char *s, size_t len, int severity = ERR_CRITICAL) {
    /* Increment the thread error counter */
    serverTL->stat_total_error_replies++;
    /* Increment the error stats
     * If the string already starts with "-..." then the error prefix
     * is provided by the caller ( we limit the search to 32 chars). Otherwise we use "-ERR". */
    if (s[0] != '-') {
        incrementErrorCount("ERR", 3);
    } else {
        const char *spaceloc = (const char*)memchr(s, ' ', len < 32 ? len : 32);
        if (spaceloc) {
            const size_t errEndPos = (size_t)(spaceloc - s);
            incrementErrorCount(s+1, errEndPos-1);
        } else {
            /* Fallback to ERR if we can't retrieve the error prefix */
            incrementErrorCount("ERR", 3);
        }
    }

    int ctype = getClientType(c);
    if (ctype == CLIENT_TYPE_MASTER || ctype == CLIENT_TYPE_SLAVE || c->id == CLIENT_ID_AOF) {
        const char *to, *from;

        if (c->id == CLIENT_ID_AOF) {
            to = "AOF-loading-client";
            from = "server";
        } else if (ctype == CLIENT_TYPE_MASTER) {
            to = "master";
            from = "replica";
        } else {
            to = "replica";
            from = "master";
        }

        if (len > 4096) len = 4096;
        const char *cmdname = c->lastcmd ? c->lastcmd->name : "<unknown>";
        switch (severity) {
            case ERR_NOTICE:
                serverLog(LL_NOTICE,"== NOTICE == This %s is rejecting a command "
                    "from its %s: '%.*s' after processing the command "
                    "'%s'", from, to, (int)len, s, cmdname);
            break;
            case ERR_WARNING:
                serverLog(LL_WARNING,"== WARNING == This %s is rejecting a command "
                    "from its %s: '%.*s' after processing the command "
                    "'%s'", from, to, (int)len, s, cmdname);
            break;
            case ERR_ERROR:
                serverLog(LL_WARNING,"== ERROR == This %s is sending an error "
                    "to its %s: '%.*s' after processing the command "
                    "'%s'", from, to, (int)len, s, cmdname);
            break;
            case ERR_CRITICAL:
            default:
                serverLog(LL_WARNING,"== CRITICAL == This %s is sending an error "
                    "to its %s: '%.*s' after processing the command "
                    "'%s'", from, to, (int)len, s, cmdname);
            break;
        }

        if (ctype == CLIENT_TYPE_MASTER && g_pserver->repl_backlog &&
            g_pserver->repl_backlog_histlen > 0)
        {
            showLatestBacklog();
        }
        g_pserver->stat_unexpected_error_replies++;
    }
}

/* The 'err' object is expected to start with -ERRORCODE and end with \r\n.
 * Unlike addReplyErrorSds and others alike which rely on addReplyErrorLength. */
void addReplyErrorObject(client *c, robj *err, int severity) {
    addReply(c, err);
    afterErrorReply(c, szFromObj(err), sdslen(szFromObj(err))-2, severity); /* Ignore trailing \r\n */
}

/* See addReplyErrorLength for expectations from the input string. */
void addReplyError(client *c, const char *err) {
    addReplyErrorLength(c,err,strlen(err));
    afterErrorReply(c,err,strlen(err));
}

/* See addReplyErrorLength for expectations from the input string. */
/* As a side effect the SDS string is freed. */
void addReplyErrorSds(client *c, sds err) {
    addReplyErrorLength(c,err,sdslen(err));
    afterErrorReply(c,err,sdslen(err));
    sdsfree(err);
}

/* See addReplyErrorLength for expectations from the formatted string.
 * The formatted string is safe to contain \r and \n anywhere. */
void addReplyErrorFormat(client *c, const char *fmt, ...) {
    va_list ap;
    va_start(ap,fmt);
    sds s = sdscatvprintf(sdsempty(),fmt,ap);
    va_end(ap);
    /* Trim any newlines at the end (ones will be added by addReplyErrorLength) */
    s = sdstrim(s, "\r\n");
    /* Make sure there are no newlines in the middle of the string, otherwise
     * invalid protocol is emitted. */
    s = sdsmapchars(s, "\r\n", "  ",  2);
    addReplyErrorLength(c,s,sdslen(s));
    afterErrorReply(c,s,sdslen(s));
    sdsfree(s);
}

void addReplyStatusLength(client *c, const char *s, size_t len) {
    addReplyProto(c,"+",1);
    addReplyProto(c,s,len);
    addReplyProto(c,"\r\n",2);
}

void addReplyStatus(client *c, const char *status) {
    addReplyStatusLength(c,status,strlen(status));
}

void addReplyStatusFormat(client *c, const char *fmt, ...) {
    va_list ap;
    va_start(ap,fmt);
    sds s = sdscatvprintf(sdsempty(),fmt,ap);
    va_end(ap);
    addReplyStatusLength(c,s,sdslen(s));
    sdsfree(s);
}

/* Sometimes we are forced to create a new reply node, and we can't append to
 * the previous one, when that happens, we wanna try to trim the unused space
 * at the end of the last reply node which we won't use anymore. */
void trimReplyUnusedTailSpace(client *c) {
    listNode *ln = listLast(c->reply);
    clientReplyBlock *tail = ln? (clientReplyBlock*)listNodeValue(ln): NULL;

    /* Note that 'tail' may be NULL even if we have a tail node, becuase when
     * addReplyDeferredLen() is used */
    if (!tail) return;

    /* We only try to trim the space is relatively high (more than a 1/4 of the
     * allocation), otherwise there's a high chance realloc will NOP.
     * Also, to avoid large memmove which happens as part of realloc, we only do
     * that if the used part is small.  */
    if (tail->size - tail->used > tail->size / 4 &&
        tail->used < PROTO_REPLY_CHUNK_BYTES)
    {
        size_t old_size = tail->size;
        tail = (clientReplyBlock*)zrealloc(tail, tail->used + sizeof(clientReplyBlock));
        /* take over the allocation's internal fragmentation (at least for
         * memory usage tracking) */
        tail->size = zmalloc_usable_size(tail) - sizeof(clientReplyBlock);
        c->reply_bytes = c->reply_bytes + tail->size - old_size;
        listNodeValue(ln) = tail;
    }
}

/* Adds an empty object to the reply list that will contain the multi bulk
 * length, which is not known when this function is called. */
void *addReplyDeferredLenCore(client *c) {
    /* Note that we install the write event here even if the object is not
     * ready to be sent, since we are sure that before returning to the
     * event loop setDeferredAggregateLen() will be called. */
    if (prepareClientToWrite(c) != C_OK) return NULL;
    trimReplyUnusedTailSpace(c);
    listAddNodeTail(c->reply,NULL); /* NULL is our placeholder. */
    return listLast(c->reply);
}

void *addReplyDeferredLen(client *c) {
    if (FCorrectThread(c))
        return addReplyDeferredLenCore(c);
        
    return (void*)((ssize_t)(c->replyAsync ? c->replyAsync->used : 0));
}

void setDeferredReply(client *c, void *node, const char *s, size_t length) {
    listNode *ln = (listNode*)node;
    clientReplyBlock *next, *prev;

    /* Abort when *node is NULL: when the client should not accept writes
     * we return NULL in addReplyDeferredLen() */
    if (node == NULL) return;
    serverAssert(!listNodeValue(ln));

    /* Normally we fill this dummy NULL node, added by addReplyDeferredLen(),
     * with a new buffer structure containing the protocol needed to specify
     * the length of the array following. However sometimes there might be room
     * in the previous/next node so we can instead remove this NULL node, and
     * suffix/prefix our data in the node immediately before/after it, in order
     * to save a write(2) syscall later. Conditions needed to do it:
     *
     * - The prev node is non-NULL and has space in it or
     * - The next node is non-NULL,
     * - It has enough room already allocated
     * - And not too large (avoid large memmove) */
    if (ln->prev != NULL && (prev = (clientReplyBlock*)listNodeValue(ln->prev)) &&
        prev->size - prev->used > 0)
    {
        size_t len_to_copy = prev->size - prev->used;
        if (len_to_copy > length)
            len_to_copy = length;
        memcpy(prev->buf() + prev->used, s, len_to_copy);
        prev->used += len_to_copy;
        length -= len_to_copy;
        if (length == 0) {
            listDelNode(c->reply, ln);
            return;
        }
        s += len_to_copy;
    }

    if (ln->next != NULL && (next = (clientReplyBlock*)listNodeValue(ln->next)) &&
        next->size - next->used >= length &&
        next->used < PROTO_REPLY_CHUNK_BYTES * 4)
    {
        memmove(next->buf() + length, next->buf(), next->used);
        memcpy(next->buf(), s, length);
        next->used += length;
        listDelNode(c->reply,ln);
    } else {
        /* Create a new node */
        clientReplyBlock *buf = (clientReplyBlock*)zmalloc(length + sizeof(clientReplyBlock));
        /* Take over the allocation's internal fragmentation */
        buf->size = zmalloc_usable_size(buf) - sizeof(clientReplyBlock);
        buf->used = length;
        memcpy(buf->buf(), s, length);
        listNodeValue(ln) = buf;
        c->reply_bytes += buf->size;

        closeClientOnOutputBufferLimitReached(c, 1);
    }
}

/* Populate the length object and try gluing it to the next chunk. */
void setDeferredAggregateLen(client *c, void *node, long length, char prefix) {
    serverAssert(length >= 0);

    if (FCorrectThread(c)) {
        /* Abort when *node is NULL: when the client should not accept writes
         * we return NULL in addReplyDeferredLen() */
        if (node == NULL) return;
        char lenstr[128];
        size_t lenstr_len = sprintf(lenstr, "%c%ld\r\n", prefix, length);
        setDeferredReply(c, node, lenstr, lenstr_len);
    } else {
        char lenstr[128];
        int lenstr_len = sprintf(lenstr, "%c%ld\r\n", prefix, length);

        size_t idxSplice = (size_t)node;
        serverAssert(idxSplice <= c->replyAsync->used);
        if (c->replyAsync->size < (c->replyAsync->used + lenstr_len))
        {
            int newsize = std::max(c->replyAsync->used + lenstr_len, c->replyAsync->size*2);
            clientReplyBlock *replyNew = (clientReplyBlock*)zmalloc(sizeof(clientReplyBlock) + newsize);
            replyNew->size = zmalloc_usable_size(replyNew) - sizeof(clientReplyBlock);
            replyNew->used = c->replyAsync->used;
            memcpy(replyNew->buf(), c->replyAsync->buf(), c->replyAsync->used);
            zfree(c->replyAsync);
            c->replyAsync = replyNew;
        }
        
        memmove(c->replyAsync->buf() + idxSplice + lenstr_len, c->replyAsync->buf() + idxSplice, c->replyAsync->used - idxSplice);
        memcpy(c->replyAsync->buf() + idxSplice, lenstr, lenstr_len);
        c->replyAsync->used += lenstr_len;
    }
}

void setDeferredArrayLen(client *c, void *node, long length) {
    setDeferredAggregateLen(c,node,length,'*');
}

void setDeferredMapLen(client *c, void *node, long length) {
    int prefix = c->resp == 2 ? '*' : '%';
    if (c->resp == 2) length *= 2;
    setDeferredAggregateLen(c,node,length,prefix);
}

void setDeferredSetLen(client *c, void *node, long length) {
    int prefix = c->resp == 2 ? '*' : '~';
    setDeferredAggregateLen(c,node,length,prefix);
}

void setDeferredAttributeLen(client *c, void *node, long length) {
    serverAssert(c->resp >= 3);
    setDeferredAggregateLen(c,node,length,'|');
}

void setDeferredPushLen(client *c, void *node, long length) {
    serverAssert(c->resp >= 3);
    setDeferredAggregateLen(c,node,length,'>');
}

/* Add a double as a bulk reply */
void addReplyDouble(client *c, double d) {
    if (std::isinf(d)) {
        /* Libc in odd systems (Hi Solaris!) will format infinite in a
         * different way, so better to handle it in an explicit way. */
        if (c->resp == 2) {
            addReplyBulkCString(c, d > 0 ? "inf" : "-inf");
        } else {
            addReplyProto(c, d > 0 ? ",inf\r\n" : ",-inf\r\n",
                              d > 0 ? 6 : 7);
        }
    } else {
        char dbuf[MAX_LONG_DOUBLE_CHARS+3],
             sbuf[MAX_LONG_DOUBLE_CHARS+32];
        int dlen, slen;
        if (c->resp == 2) {
            dlen = snprintf(dbuf,sizeof(dbuf),"%.17g",d);
            slen = snprintf(sbuf,sizeof(sbuf),"$%d\r\n%s\r\n",dlen,dbuf);
            addReplyProto(c,sbuf,slen);
        } else {
            dlen = snprintf(dbuf,sizeof(dbuf),",%.17g\r\n",d);
            addReplyProto(c,dbuf,dlen);
        }
    }
}

void addReplyBigNum(client *c, const char* num, size_t len) {
    if (c->resp == 2) {
        addReplyBulkCBuffer(c, num, len);
    } else {
        addReplyProto(c,"(",1);
        addReplyProto(c,num,len);
        addReply(c,shared.crlf);
    }
}

/* Add a long double as a bulk reply, but uses a human readable formatting
 * of the double instead of exposing the crude behavior of doubles to the
 * dear user. */
void addReplyHumanLongDouble(client *c, long double d) {
    if (c->resp == 2) {
        robj *o = createStringObjectFromLongDouble(d,1);
        addReplyBulk(c,o);
        decrRefCount(o);
    } else {
        char buf[MAX_LONG_DOUBLE_CHARS];
        int len = ld2string(buf,sizeof(buf),d,LD_STR_HUMAN);
        addReplyProto(c,",",1);
        addReplyProto(c,buf,len);
        addReplyProto(c,"\r\n",2);
    }
}

/* Add a long long as integer reply or bulk len / multi bulk count.
 * Basically this is used to output <prefix><long long><crlf>. */
void addReplyLongLongWithPrefix(client *c, long long ll, char prefix) {
    char buf[128];
    int len;

    /* Things like $3\r\n or *2\r\n are emitted very often by the protocol
     * so we have a few shared objects to use if the integer is small
     * like it is most of the times. */
    if (prefix == '*' && ll < OBJ_SHARED_BULKHDR_LEN && ll >= 0) {
        addReply(c,shared.mbulkhdr[ll]);
        return;
    } else if (prefix == '$' && ll < OBJ_SHARED_BULKHDR_LEN && ll >= 0) {
        addReply(c,shared.bulkhdr[ll]);
        return;
    }

    buf[0] = prefix;
    len = ll2string(buf+1,sizeof(buf)-1,ll);
    buf[len+1] = '\r';
    buf[len+2] = '\n';
    addReplyProto(c,buf,len+3);
}

void addReplyLongLong(client *c, long long ll) {
    if (ll == 0)
        addReply(c,shared.czero);
    else if (ll == 1)
        addReply(c,shared.cone);
    else
        addReplyLongLongWithPrefix(c,ll,':');
}

void addReplyAggregateLen(client *c, long length, int prefix) {
    serverAssert(length >= 0);
    addReplyLongLongWithPrefix(c,length,prefix);
}

void addReplyArrayLen(client *c, long length) {
    addReplyAggregateLen(c,length,'*');
}

void addReplyMapLen(client *c, long length) {
    int prefix = c->resp == 2 ? '*' : '%';
    if (c->resp == 2) length *= 2;
    addReplyAggregateLen(c,length,prefix);
}

void addReplySetLen(client *c, long length) {
    int prefix = c->resp == 2 ? '*' : '~';
    addReplyAggregateLen(c,length,prefix);
}

void addReplyAttributeLen(client *c, long length) {
    serverAssert(c->resp >= 3);
    addReplyAggregateLen(c,length,'|');
}

void addReplyPushLen(client *c, long length) {
    serverAssert(c->resp >= 3);
    addReplyAggregateLen(c,length,'>');
}

void addReplyNull(client *c) {
    if (c->resp == 2) {
        addReplyProto(c,"$-1\r\n",5);
    } else {
        addReplyProto(c,"_\r\n",3);
    }
}

void addReplyBool(client *c, int b) {
    if (c->resp == 2) {
        addReply(c, b ? shared.cone : shared.czero);
    } else {
        addReplyProto(c, b ? "#t\r\n" : "#f\r\n",4);
    }
}

/* A null array is a concept that no longer exists in RESP3. However
 * RESP2 had it, so API-wise we have this call, that will emit the correct
 * RESP2 protocol, however for RESP3 the reply will always be just the
 * Null type "_\r\n". */
void addReplyNullArray(client *c) 
{
    if (c->resp == 2) {
        addReplyProto(c,"*-1\r\n",5);
    } else {
        addReplyProto(c,"_\r\n",3);
    }
}

/* Create the length prefix of a bulk reply, example: $2234 */
void addReplyBulkLen(client *c, robj_roptr obj) {
    size_t len = stringObjectLen(obj);

    addReplyLongLongWithPrefix(c,len,'$');
}

/* Add a Redis Object as a bulk reply */
void addReplyBulk(client *c, robj_roptr obj) {
    addReplyBulkLen(c,obj);
    addReply(c,obj);
    addReply(c,shared.crlf);
}

/* Add a C buffer as bulk reply */
void addReplyBulkCBuffer(client *c, const void *p, size_t len) {
    addReplyLongLongWithPrefix(c,len,'$');
    addReplyProto(c,(const char*)p,len);
    addReply(c,shared.crlf);
}

/* Add sds to reply (takes ownership of sds and frees it) */
void addReplyBulkSds(client *c, sds s)  {
    addReplyLongLongWithPrefix(c,sdslen(s),'$');
    addReplySds(c,s);
    addReply(c,shared.crlf);
}

/* Set sds to a deferred reply (for symmetry with addReplyBulkSds it also frees the sds) */
void setDeferredReplyBulkSds(client *c, void *node, sds s) {
    sds reply = sdscatprintf(sdsempty(), "$%d\r\n%s\r\n", (unsigned)sdslen(s), s);
    setDeferredReply(c, node, reply, sdslen(reply));
    sdsfree(reply);
    sdsfree(s);
}

/* Add a C null term string as bulk reply */
void addReplyBulkCString(client *c, const char *s) {
    if (s == NULL) {
        if (c->resp < 3)
            addReply(c,shared.nullbulk);
        else
            addReplyNull(c);
    } else {
        addReplyBulkCBuffer(c,s,strlen(s));
    }
}

/* Add a long long as a bulk reply */
void addReplyBulkLongLong(client *c, long long ll) {
    char buf[64];
    int len;

    len = ll2string(buf,64,ll);
    addReplyBulkCBuffer(c,buf,len);
}

/* Reply with a verbatim type having the specified extension.
 *
 * The 'ext' is the "extension" of the file, actually just a three
 * character type that describes the format of the verbatim string.
 * For instance "txt" means it should be interpreted as a text only
 * file by the receiver, "md " as markdown, and so forth. Only the
 * three first characters of the extension are used, and if the
 * provided one is shorter than that, the remaining is filled with
 * spaces. */
void addReplyVerbatim(client *c, const char *s, size_t len, const char *ext) {
    if (c->resp == 2) {
        addReplyBulkCBuffer(c,s,len);
    } else {
        char buf[32];
        size_t preflen = snprintf(buf,sizeof(buf),"=%zu\r\nxxx:",len+4);
        char *p = buf+preflen-4;
        for (int i = 0; i < 3; i++) {
            if (*ext == '\0') {
                p[i] = ' ';
            } else {
                p[i] = *ext++;
            }
        }
        addReplyProto(c,buf,preflen);
        addReplyProto(c,s,len);
        addReplyProto(c,"\r\n",2);
    }
}

/* Add an array of C strings as status replies with a heading.
 * This function is typically invoked by from commands that support
 * subcommands in response to the 'help' subcommand. The help array
 * is terminated by NULL sentinel. */
void addReplyHelp(client *c, const char **help) {
    sds cmd = sdsnew((char*) ptrFromObj(c->argv[0]));
    void *blenp = addReplyDeferredLen(c);
    int blen = 0;

    sdstoupper(cmd);
    addReplyStatusFormat(c,
        "%s <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",cmd);
    sdsfree(cmd);

    while (help[blen]) addReplyStatus(c,help[blen++]);

    addReplyStatus(c,"HELP");
    addReplyStatus(c,"    Prints this help.");

    blen += 1;  /* Account for the header. */
    blen += 2;  /* Account for the footer. */
    setDeferredArrayLen(c,blenp,blen);
}

/* Add a suggestive error reply.
 * This function is typically invoked by from commands that support
 * subcommands in response to an unknown subcommand or argument error. */
void addReplySubcommandSyntaxError(client *c) {
    sds cmd = sdsnew((char*) ptrFromObj(c->argv[0]));
    sdstoupper(cmd);
    addReplyErrorFormat(c,
        "Unknown subcommand or wrong number of arguments for '%s'. Try %s HELP.",
        (char*)ptrFromObj(c->argv[1]),cmd);
    sdsfree(cmd);
}

/* Append 'src' client output buffers into 'dst' client output buffers.
 * This function clears the output buffers of 'src' */
void AddReplyFromClient(client *dst, client *src) {
    /* If the source client contains a partial response due to client output
     * buffer limits, propagate that to the dest rather than copy a partial
     * reply. We don't wanna run the risk of copying partial response in case
     * for some reason the output limits don't reach the same decision (maybe
     * they changed) */
    if (src->flags & CLIENT_CLOSE_ASAP) {
        sds client = catClientInfoString(sdsempty(),dst);
        freeClientAsync(dst);
        serverLog(LL_WARNING,"Client %s scheduled to be closed ASAP for overcoming of output buffer limits.", client);
        sdsfree(client);
        return;
    }

    /* First add the static buffer (either into the static buffer or reply list) */
    addReplyProto(dst,src->buf, src->bufpos);

    /* We need to check with prepareClientToWrite again (after addReplyProto)
     * since addReplyProto may have changed something (like CLIENT_CLOSE_ASAP) */
    if (prepareClientToWrite(dst) != C_OK)
        return;

    /* We're bypassing _addReplyProtoToList, so we need to add the pre/post
     * checks in it. */
    if (dst->flags & CLIENT_CLOSE_AFTER_REPLY) return;

    /* Concatenate the reply list into the dest */
    if (listLength(src->reply))
        listJoin(dst->reply,src->reply);
    dst->reply_bytes += src->reply_bytes;
    src->reply_bytes = 0;
    src->bufpos = 0;

    /* Check output buffer limits */
    closeClientOnOutputBufferLimitReached(dst, 1);
}

/* Copy 'src' client output buffers into 'dst' client output buffers.
 * The function takes care of freeing the old output buffers of the
 * destination client. */
void copyClientOutputBuffer(client *dst, client *src) {
    listRelease(dst->reply);
    dst->sentlen = 0;
    dst->reply = listDup(src->reply);
    memcpy(dst->buf,src->buf,src->bufpos);
    dst->bufpos = src->bufpos;
    dst->reply_bytes = src->reply_bytes;
}

/* Return true if the specified client has pending reply buffers to write to
 * the socket. */
int clientHasPendingReplies(client *c) {
    return (c->bufpos || listLength(c->reply) || c->FPendingReplicaWrite());
}

static std::atomic<int> rgacceptsInFlight[MAX_EVENT_LOOPS];
int chooseBestThreadForAccept()
{
    int ielMinLoad = 0;
    int cclientsMin = INT_MAX;
    for (int iel = 0; iel < cserver.cthreads; ++iel)
    {
        int cclientsThread;
        atomicGet(g_pserver->rgthreadvar[iel].cclients, cclientsThread);
        cclientsThread += rgacceptsInFlight[iel].load(std::memory_order_relaxed);
        // Note: Its repl factor less one because cclients also includes replicas, so we don't want to double count
        cclientsThread += (g_pserver->rgthreadvar[iel].cclientsReplica) * (g_pserver->replicaIsolationFactor-1);
        if (cclientsThread < cserver.thread_min_client_threshold)
            return iel;
        if (cclientsThread < cclientsMin)
        {
            cclientsMin = cclientsThread;
            ielMinLoad = iel;
        }
    }
    return ielMinLoad;
}

void clientAcceptHandler(connection *conn) {
    client *c = (client*)connGetPrivateData(conn);

    if (connGetState(conn) != CONN_STATE_CONNECTED) {
        serverLog(LL_WARNING,
                "Error accepting a client connection: %s",
                connGetLastError(conn));
        freeClientAsync(c);
        return;
    }

    // Set thread affinity
    if (cserver.fThreadAffinity)
        connSetThreadAffinity(conn, c->iel);

    /* If the server is running in protected mode (the default) and there
     * is no password set, nor a specific interface is bound, we don't accept
     * requests from non loopback interfaces. Instead we try to explain the
     * user what to do to fix it if needed. */
    if (g_pserver->protected_mode &&
        g_pserver->bindaddr_count == 0 &&
        DefaultUser->flags & USER_FLAG_NOPASS &&
        !(c->flags & CLIENT_UNIX_SOCKET))
    {
        char cip[NET_IP_STR_LEN+1] = { 0 };
        connPeerToString(conn, cip, sizeof(cip)-1, NULL);

        if (strcmp(cip,"127.0.0.1") && strcmp(cip,"::1")) {
            const char *err =
                "-DENIED KeyDB is running in protected mode because protected "
                "mode is enabled, no bind address was specified, no "
                "authentication password is requested to clients. In this mode "
                "connections are only accepted from the loopback interface. "
                "If you want to connect from external computers to KeyDB you "
                "may adopt one of the following solutions: "
                "1) Just disable protected mode sending the command "
                "'CONFIG SET protected-mode no' from the loopback interface "
                "by connecting to KeyDB from the same host the server is "
                "running, however MAKE SURE KeyDB is not publicly accessible "
                "from internet if you do so. Use CONFIG REWRITE to make this "
                "change permanent. "
                "2) Alternatively you can just disable the protected mode by "
                "editing the KeyDB configuration file, and setting the protected "
                "mode option to 'no', and then restarting the server "
                "3) If you started the server manually just for testing, restart "
                "it with the '--protected-mode no' option. "
                "4) Setup a bind address or an authentication password. "
                "NOTE: You only need to do one of the above things in order for "
                "the server to start accepting connections from the outside.\r\n";
            if (connWrite(c->conn,err,strlen(err)) == -1) {
                /* Nothing to do, Just to avoid the warning... */
            }
            g_pserver->stat_rejected_conn++;
            freeClientAsync(c);
            return;
        }
    }

    g_pserver->stat_numconnections++;
    moduleFireServerEvent(REDISMODULE_EVENT_CLIENT_CHANGE,
                          REDISMODULE_SUBEVENT_CLIENT_CHANGE_CONNECTED,
                          c);
}

#define MAX_ACCEPTS_PER_CALL 1000
#define MAX_ACCEPTS_PER_CALL_TLS 100
static void acceptCommonHandler(connection *conn, int flags, char *ip, int iel) {
    client *c;
    char conninfo[100];
    UNUSED(ip);
    AeLocker locker;
    locker.arm(nullptr);

    if (connGetState(conn) != CONN_STATE_ACCEPTING) {
        serverLog(LL_VERBOSE,
            "Accepted client connection in error state: %s (conn: %s)",
            connGetLastError(conn),
            connGetInfo(conn, conninfo, sizeof(conninfo)));
        connClose(conn);
        return;
    }

    /* Limit the number of connections we take at the same time.
     *
     * Admission control will happen before a client is created and connAccept()
     * called, because we don't want to even start transport-level negotiation
     * if rejected. */
    if (listLength(g_pserver->clients) + getClusterConnectionsCount()
        >= g_pserver->maxclients)
    {
        const char *err;
        if (g_pserver->cluster_enabled)
            err = "-ERR max number of clients + cluster "
                  "connections reached\r\n";
        else
            err = "-ERR max number of clients reached\r\n";

        /* That's a best effort error message, don't check write errors.
         * Note that for TLS connections, no handshake was done yet so nothing
         * is written and the connection will just drop. */
        if (connWrite(conn,err,strlen(err)) == -1) {
            /* Nothing to do, Just to avoid the warning... */
        }
        g_pserver->stat_rejected_conn++;
        connClose(conn);
        return;
    }

    /* Create connection and client */
    if ((c = createClient(conn, iel)) == NULL) {
        serverLog(LL_WARNING,
            "Error registering fd event for the new client: %s (conn: %s)",
            connGetLastError(conn),
            connGetInfo(conn, conninfo, sizeof(conninfo)));
        connClose(conn); /* May be already closed, just ignore errors */
        return;
    }

    /* Last chance to keep flags */
    c->flags |= flags;

    /* Initiate accept.
     *
     * Note that connAccept() is free to do two things here:
     * 1. Call clientAcceptHandler() immediately;
     * 2. Schedule a future call to clientAcceptHandler().
     *
     * Because of that, we must do nothing else afterwards.
     */
    if (connAccept(conn, clientAcceptHandler) == C_ERR) {
        char conninfo[100];
        if (connGetState(conn) == CONN_STATE_ERROR)
            serverLog(LL_WARNING,
                    "Error accepting a client connection: %s (conn: %s)",
                    connGetLastError(conn), connGetInfo(conn, conninfo, sizeof(conninfo)));
        freeClient((client*)connGetPrivateData(conn));
        return;
    }
}

void acceptOnThread(connection *conn, int flags, char *cip)
{
    int ielCur = ielFromEventLoop(serverTL->el);
    bool fBootLoad = (g_pserver->loading == LOADING_BOOT);

    int ielTarget = ielCur;
    if (fBootLoad)
    {
        ielTarget = IDX_EVENT_LOOP_MAIN;    // During load only the main thread is active
    }
    else if (g_fTestMode)
    {
        // On test mode we don't want any bunching of clients
        while (cserver.cthreads > 1 && ielTarget == IDX_EVENT_LOOP_MAIN)
            ielTarget = rand() % cserver.cthreads;
    }
    else if (g_pserver->active_client_balancing)
    {
        // Cluster connections are more transient, so its not worth the cost to balance
        //  we can trust that SO_REUSEPORT is doing its job of distributing connections
        ielTarget = g_pserver->cluster_enabled ? ielCur : chooseBestThreadForAccept();
    }

    rgacceptsInFlight[ielTarget].fetch_add(1, std::memory_order_relaxed);
    if (ielTarget != ielCur)
    {
        char *szT = nullptr;
        if (cip != nullptr)
        {
            szT = (char*)zmalloc(NET_IP_STR_LEN, MALLOC_LOCAL);
            memcpy(szT, cip, NET_IP_STR_LEN);
        }
        int res = aePostFunction(g_pserver->rgthreadvar[ielTarget].el, [conn, flags, ielTarget, szT] {
            connMarshalThread(conn);
            acceptCommonHandler(conn,flags,szT,ielTarget);
            rgacceptsInFlight[ielTarget].fetch_sub(1, std::memory_order_relaxed);
            zfree(szT);
        });

        if (res == AE_OK)
            return;
        // If res != AE_OK we can still try to accept on the local thread
    }
    rgacceptsInFlight[ielTarget].fetch_sub(1, std::memory_order_relaxed);

    aeAcquireLock();
    acceptCommonHandler(conn,flags,cip,ielCur);
    aeReleaseLock();
}

void acceptTcpHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    int cport, cfd, max = MAX_ACCEPTS_PER_CALL;
    char cip[NET_IP_STR_LEN];
    UNUSED(mask);
    UNUSED(privdata);
    UNUSED(el);

    while(max--) {
        cfd = anetTcpAccept(serverTL->neterr, fd, cip, sizeof(cip), &cport);
        if (cfd == ANET_ERR) {
            if (errno != EWOULDBLOCK)
                serverLog(LL_WARNING,
                    "Accepting client connection: %s", serverTL->neterr);
            return;
        }
        anetCloexec(cfd);
        serverLog(LL_VERBOSE,"Accepted %s:%d", cip, cport);

        acceptOnThread(connCreateAcceptedSocket(cfd), 0, cip);
    }
}

void acceptTLSHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    int cport, cfd, max = MAX_ACCEPTS_PER_CALL_TLS;
    char cip[NET_IP_STR_LEN];
    UNUSED(el);
    UNUSED(mask);
    UNUSED(privdata);

    while(max--) {
        cfd = anetTcpAccept(serverTL->neterr, fd, cip, sizeof(cip), &cport);
        if (cfd == ANET_ERR) {
            if (errno != EWOULDBLOCK)
                serverLog(LL_WARNING,
                    "Accepting client connection: %s", serverTL->neterr);
            return;
        }
        anetCloexec(cfd);
        serverLog(LL_VERBOSE,"Accepted %s:%d", cip, cport);

        acceptOnThread(connCreateAcceptedTLS(cfd, g_pserver->tls_auth_clients), 0, cip);
        if (aeLockContention() >= 2)
            break;
    }
}

void acceptUnixHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    int cfd, max = MAX_ACCEPTS_PER_CALL;
    UNUSED(el);
    UNUSED(mask);
    UNUSED(privdata);

    while(max--) {
        cfd = anetUnixAccept(serverTL->neterr, fd);
        if (cfd == ANET_ERR) {
            if (errno != EWOULDBLOCK)
                serverLog(LL_WARNING,
                    "Accepting client connection: %s", serverTL->neterr);
            return;
        }
        anetCloexec(cfd);
        serverLog(LL_VERBOSE,"Accepted connection to %s", g_pserver->unixsocket);
        acceptOnThread(connCreateAcceptedSocket(cfd),CLIENT_UNIX_SOCKET,NULL);
    }
}

void freeClientOriginalArgv(client *c) {
    /* We didn't rewrite this client */
    if (!c->original_argv) return;

    for (int j = 0; j < c->original_argc; j++)
        decrRefCount(c->original_argv[j]);
    zfree(c->original_argv);
    c->original_argv = NULL;
    c->original_argc = 0;
}

static void freeClientArgv(client *c) {
    int j;
    for (j = 0; j < c->argc; j++)
        decrRefCount(c->argv[j]);
    c->argc = 0;
    c->cmd = NULL;
    c->argv_len_sumActive = 0;
}

void disconnectSlavesExcept(unsigned char *uuid)
{
    serverAssert(GlobalLocksAcquired());
    listIter li;
    listNode *ln;

    listRewind(g_pserver->slaves, &li);
    while ((ln = listNext(&li))) {
        client *c = (client*)listNodeValue(ln);
        if (uuid == nullptr || !FUuidEqual(c->uuid, uuid))
            freeClientAsync(c);
    }   
}

/* Close all the slaves connections. This is useful in chained replication
 * when we resync with our own master and want to force all our slaves to
 * resync with us as well. */
void disconnectSlaves(void) {
    disconnectSlavesExcept(nullptr);
}

/* Check if there is any other slave waiting dumping RDB finished expect me.
 * This function is useful to judge current dumping RDB can be used for full
 * synchronization or not. */
int anyOtherSlaveWaitRdb(client *except_me) {
    listIter li;
    listNode *ln;

    listRewind(g_pserver->slaves, &li);
    while((ln = listNext(&li))) {
        client *slave = (client*)listNodeValue(ln);
        if (slave != except_me &&
            slave->replstate == SLAVE_STATE_WAIT_BGSAVE_END)
        {
            return 1;
        }
    }
    return 0;
}

/* Remove the specified client from global lists where the client could
 * be referenced, not including the Pub/Sub channels.
 * This is used by freeClient() and replicationCacheMaster(). */
void unlinkClient(client *c) {
    listNode *ln;
    AssertCorrectThread(c);
    serverAssert(c->conn == nullptr || GlobalLocksAcquired());
    serverAssert(c->conn == nullptr || c->lock.fOwnLock());

    /* If this is marked as current client unset it. */
    if (serverTL && serverTL->current_client == c) serverTL->current_client = NULL;

    /* Certain operations must be done only if the client has an active connection.
     * If the client was already unlinked or if it's a "fake client" the
     * conn is already set to NULL. */
    if (c->conn) {
        /* Remove from the list of active clients. */
        if (c->client_list_node) {
            uint64_t id = htonu64(c->id);
            raxRemove(g_pserver->clients_index,(unsigned char*)&id,sizeof(id),NULL);
            listDelNode(g_pserver->clients,c->client_list_node);
            c->client_list_node = NULL;
        }

        /* Check if this is a replica waiting for diskless replication (rdb pipe),
         * in which case it needs to be cleaned from that list */
        if (c->flags & CLIENT_SLAVE &&
            c->replstate == SLAVE_STATE_WAIT_BGSAVE_END &&
            g_pserver->rdb_pipe_conns)
        {
            int i;
            for (i=0; i < g_pserver->rdb_pipe_numconns; i++) {
                if (g_pserver->rdb_pipe_conns[i] == c->conn) {
                    rdbPipeWriteHandlerConnRemoved(c->conn);
                    g_pserver->rdb_pipe_conns[i] = NULL;
                    break;
                }
            }
        }
        connClose(c->conn);
        c->conn = NULL;
        atomicDecr(g_pserver->rgthreadvar[c->iel].cclients, 1);
    }

    /* Remove from the list of pending writes if needed. */
    if (c->flags & CLIENT_PENDING_WRITE) {
        std::unique_lock<fastlock> lockf(g_pserver->rgthreadvar[c->iel].lockPendingWrite);
        auto itr = std::find(g_pserver->rgthreadvar[c->iel].clients_pending_write.begin(),
            g_pserver->rgthreadvar[c->iel].clients_pending_write.end(), c);
        serverAssert(itr != g_pserver->rgthreadvar[c->iel].clients_pending_write.end());
        g_pserver->rgthreadvar[c->iel].clients_pending_write.erase(itr);
        c->flags &= ~CLIENT_PENDING_WRITE;
    }

    /* When client was just unblocked because of a blocking operation,
     * remove it from the list of unblocked clients. */
    if (c->flags & CLIENT_UNBLOCKED) {
        ln = listSearchKey(g_pserver->rgthreadvar[c->iel].unblocked_clients,c);
        serverAssert(ln != NULL);
        listDelNode(g_pserver->rgthreadvar[c->iel].unblocked_clients,ln);
        c->flags &= ~CLIENT_UNBLOCKED;
    }

    if (c->fPendingAsyncWrite) {
        ln = NULL;
        bool fFound = false;
        for (int iel = 0; iel < cserver.cthreads; ++iel)
        {
            ln = listSearchKey(g_pserver->rgthreadvar[iel].clients_pending_asyncwrite,c);
            if (ln)
            {
                fFound = true;
                listDelNode(g_pserver->rgthreadvar[iel].clients_pending_asyncwrite,ln);
            }
        }
        fFound = g_pserver->asyncworkqueue->removeClientAsyncWrites(c) || fFound;
        serverAssert(fFound);
        c->fPendingAsyncWrite = FALSE;
    }

    serverTL->vecclientsProcess.erase(std::remove(serverTL->vecclientsProcess.begin(), serverTL->vecclientsProcess.end(), c), serverTL->vecclientsProcess.end());

    /* Clear the tracking status. */
    if (c->flags & CLIENT_TRACKING) disableTracking(c);
}

bool freeClient(client *c) {
    listNode *ln;
    serverAssert(c->conn == nullptr || GlobalLocksAcquired());
    AssertCorrectThread(c);
    std::unique_lock<decltype(c->lock)> ulock(c->lock);

    /* If a client is protected, yet we need to free it right now, make sure
     * to at least use asynchronous freeing. */
    if (c->flags & CLIENT_PROTECTED || c->casyncOpsPending || c->replstate == SLAVE_STATE_FASTSYNC_TX) {
        freeClientAsync(c);
        return false;
    }

    /* For connected clients, call the disconnection event of modules hooks. */
    if (c->conn) {
        moduleFireServerEvent(REDISMODULE_EVENT_CLIENT_CHANGE,
                              REDISMODULE_SUBEVENT_CLIENT_CHANGE_DISCONNECTED,
                              c);
    }

    /* Notify module system that this client auth status changed. */
    moduleNotifyUserChanged(c);

    /* If this client was scheduled for async freeing we need to remove it
     * from the queue. Note that we need to do this here, because later
     * we may call replicationCacheMaster() and the client should already
     * be removed from the list of clients to free. */
    if (c->flags & CLIENT_CLOSE_ASAP) {
        std::unique_lock<fastlock> ul(g_lockasyncfree);
        ln = listSearchKey(g_pserver->clients_to_close,c);
        serverAssert(ln != NULL);
        listDelNode(g_pserver->clients_to_close,ln);
    }

    /* If it is our master that's being disconnected we should make sure
     * to cache the state to try a partial resynchronization later.
     *
     * Note that before doing this we make sure that the client is not in
     * some unexpected state, by checking its flags. */
    if (FActiveMaster(c)) {
        serverLog(LL_WARNING,"Connection with master lost.");
        if (!(c->flags & (CLIENT_PROTOCOL_ERROR|CLIENT_BLOCKED))) {
            c->flags &= ~(CLIENT_CLOSE_ASAP|CLIENT_CLOSE_AFTER_REPLY);
            replicationCacheMaster(MasterInfoFromClient(c), c);
            return false;
        }
    }

    /* Log link disconnection with replica */
    if (getClientType(c) == CLIENT_TYPE_SLAVE) {
        serverLog(LL_WARNING,"Connection with replica %s lost.",
            replicationGetSlaveName(c));
    }

    /* Free the query buffer */
    sdsfree(c->querybuf);
    sdsfree(c->pending_querybuf);
    c->querybuf = NULL;

    /* Deallocate structures used to block on blocking ops. */
    if (c->flags & CLIENT_BLOCKED)
    {
        serverAssert(c->btype != BLOCKED_ASYNC);
        unblockClient(c);
    }
    dictRelease(c->bpop.keys);

    /* UNWATCH all the keys */
    unwatchAllKeys(c);
    listRelease(c->watched_keys);

    /* Unsubscribe from all the pubsub channels */
    pubsubUnsubscribeAllChannels(c,0);
    pubsubUnsubscribeAllPatterns(c,0);
    dictRelease(c->pubsub_channels);
    listRelease(c->pubsub_patterns);

    /* Free data structures. */
    listRelease(c->reply);
    freeClientArgv(c);
    freeClientOriginalArgv(c);

    /* Unlink the client: this will close the socket, remove the I/O
     * handlers, and remove references of the client from different
     * places where active clients may be referenced. */
    unlinkClient(c);

    /* Master/replica cleanup Case 1:
     * we lost the connection with a replica. */
    if (c->flags & CLIENT_SLAVE) {
        /* If there is no any other slave waiting dumping RDB finished, the
         * current child process need not continue to dump RDB, then we kill it.
         * So child process won't use more memory, and we also can fork a new
         * child process asap to dump rdb for next full synchronization or bgsave.
         * But we also need to check if users enable 'save' RDB, if enable, we
         * should not remove directly since that means RDB is important for users
         * to keep data safe and we may delay configured 'save' for full sync. */
        if (g_pserver->saveparamslen == 0 &&
            c->replstate == SLAVE_STATE_WAIT_BGSAVE_END &&
            g_pserver->child_type == CHILD_TYPE_RDB &&
            g_pserver->rdb_child_type == RDB_CHILD_TYPE_DISK &&
            anyOtherSlaveWaitRdb(c) == 0)
        {
            killRDBChild();
        }
        if (c->replstate == SLAVE_STATE_SEND_BULK) {
            if (c->repldbfd != -1) close(c->repldbfd);
            if (c->replpreamble) sdsfree(c->replpreamble);
        }
        list *l = (c->flags & CLIENT_MONITOR) ? g_pserver->monitors : g_pserver->slaves;
        ln = listSearchKey(l,c);
        serverAssert(ln != NULL);
        listDelNode(l,ln);
        g_pserver->rgthreadvar[c->iel].cclientsReplica--;
        /* We need to remember the time when we started to have zero
         * attached slaves, as after some time we'll free the replication
         * backlog. */
        if (getClientType(c) == CLIENT_TYPE_SLAVE && listLength(g_pserver->slaves) == 0)
            g_pserver->repl_no_slaves_since = g_pserver->unixtime;
        refreshGoodSlavesCount();
        /* Fire the replica change modules event. */
        if (c->replstate == SLAVE_STATE_ONLINE)
            moduleFireServerEvent(REDISMODULE_EVENT_REPLICA_CHANGE,
                                  REDISMODULE_SUBEVENT_REPLICA_CHANGE_OFFLINE,
                                  NULL);
    }

    /* Master/replica cleanup Case 2:
     * we lost the connection with the master. */
    if (c->flags & CLIENT_MASTER) replicationHandleMasterDisconnection(MasterInfoFromClient(c));

   /* Remove the contribution that this client gave to our
     * incrementally computed memory usage. */
    g_pserver->stat_clients_type_memory[c->client_cron_last_memory_type] -=
        c->client_cron_last_memory_usage;

    /* Release other dynamically allocated client structure fields,
     * and finally release the client structure itself. */
    zfree(c->replyAsync);
    if (c->name) decrRefCount(c->name);
    zfree(c->argv);
    c->argv_len_sumActive = 0;
    freeClientMultiState(c);
    sdsfree(c->peerid);
    sdsfree(c->sockname);
    sdsfree(c->slave_addr);
    ulock.unlock();
    fastlock_free(&c->lock);
    delete c;
    return true;
}

fastlock g_lockasyncfree {"async free lock"};

/* Schedule a client to free it at a safe time in the serverCron() function.
 * This function is useful when we need to terminate a client but we are in
 * a context where calling freeClient() is not possible, because the client
 * should be valid for the continuation of the flow of the program. */
void freeClientAsync(client *c) {
    /* We need to handle concurrent access to the g_pserver->clients_to_close list
     * only in the freeClientAsync() function, since it's the only function that
     * may access the list while Redis uses I/O threads. All the other accesses
     * are in the context of the main thread while the other threads are
     * idle. */
    if (c->flags & CLIENT_CLOSE_ASAP || c->flags & CLIENT_LUA) return;  // check without the lock first
    std::lock_guard<decltype(c->lock)> clientlock(c->lock);
    if (c->flags & CLIENT_CLOSE_ASAP || c->flags & CLIENT_LUA) return;  // race condition after we acquire the lock
    c->flags |= CLIENT_CLOSE_ASAP;
    c->repl_down_since = g_pserver->unixtime;
    std::unique_lock<fastlock> ul(g_lockasyncfree);
    listAddNodeTail(g_pserver->clients_to_close,c);
}

int freeClientsInAsyncFreeQueue(int iel) {
    serverAssert(GlobalLocksAcquired());
    std::unique_lock<fastlock> ul(g_lockasyncfree);
    listIter li;
    listNode *ln;
    listRewind(g_pserver->clients_to_close,&li);

    // Store the clients in a temp vector since freeClient will modify this list
    std::vector<client*> vecclientsFree;
    while((ln = listNext(&li))) 
    {
        client *c = (client*)listNodeValue(ln);
        if (c->iel == iel && !(c->flags & CLIENT_PROTECTED) && !c->casyncOpsPending)
        {
            vecclientsFree.push_back(c);
            listDelNode(g_pserver->clients_to_close, ln);
        }
    }
    ul.unlock();

    for (client *c : vecclientsFree)
    {
        c->flags &= ~CLIENT_CLOSE_ASAP;
        freeClient(c);
    }
    return (int)vecclientsFree.size();
}

/* Return a client by ID, or NULL if the client ID is not in the set
 * of registered clients. Note that "fake clients", created with -1 as FD,
 * are not registered clients. */
client *lookupClientByID(uint64_t id) {
    id = htonu64(id);
    client *c = (client*)raxFind(g_pserver->clients_index,(unsigned char*)&id,sizeof(id));
    return (c == raxNotFound) ? NULL : c;
}

long long getReplIndexFromOffset(long long offset);

/* Write data in output buffers to client. Return C_OK if the client
 * is still valid after the call, C_ERR if it was freed because of some
 * error.  If handler_installed is set, it will attempt to clear the
 * write event.
 *
 * This function is called by threads, but always with handler_installed
 * set to 0. So when handler_installed is set to 0 the function must be
 * thread safe. */
int writeToClient(client *c, int handler_installed) {
    /* Update total number of writes on server */
    g_pserver->stat_total_writes_processed.fetch_add(1, std::memory_order_relaxed);

    ssize_t nwritten = 0, totwritten = 0;
    clientReplyBlock *o;
    serverAssertDebug(FCorrectThread(c));

    std::unique_lock<decltype(c->lock)> lock(c->lock);

    /* We can only directly read from the replication backlog if the client 
       is a replica, so only attempt to do so if that's the case. */
    if (c->flags & CLIENT_SLAVE && !(c->flags & CLIENT_MONITOR) && c->replstate == SLAVE_STATE_ONLINE) {
        std::unique_lock<fastlock> repl_backlog_lock (g_pserver->repl_backlog_lock);
        // Ensure all writes to the repl backlog are visible
        std::atomic_thread_fence(std::memory_order_acquire);

        while (clientHasPendingReplies(c)) {
            long long repl_end_idx = getReplIndexFromOffset(c->repl_end_off);
            serverAssert(c->repl_curr_off != -1);

            if (c->repl_curr_off != c->repl_end_off){
                long long repl_curr_idx = getReplIndexFromOffset(c->repl_curr_off); 
                long long nwritten2ndStage = 0; /* How much was written from the start of the replication backlog
                                                * in the event of a wrap around write */
                /* normal case with no wrap around */
                if (repl_end_idx >= repl_curr_idx){
                    nwritten = connWrite(c->conn, g_pserver->repl_backlog + repl_curr_idx, repl_end_idx - repl_curr_idx);
                /* wrap around case */
                } else {
                    nwritten = connWrite(c->conn, g_pserver->repl_backlog + repl_curr_idx, g_pserver->repl_backlog_size - repl_curr_idx);
                    /* only attempt wrapping if we write the correct number of bytes */
                    if (nwritten == g_pserver->repl_backlog_size - repl_curr_idx){
                        nwritten2ndStage = connWrite(c->conn, g_pserver->repl_backlog, repl_end_idx);
                        if (nwritten2ndStage != -1)
                            nwritten += nwritten2ndStage;
                    }                
                }

                /* only increment bytes if an error didn't occur */
                if (nwritten > 0){
                    totwritten += nwritten;
                    c->repl_curr_off += nwritten;
                    serverAssert(c->repl_curr_off <= c->repl_end_off);
                }

                /* If the second part of a write didn't go through, we still need to register that */
                if (nwritten2ndStage == -1) nwritten = -1;
                if (nwritten == -1)
                    break;
            } else {
                break;
            }
        }
    } else {
        while(clientHasPendingReplies(c)) {
            if (c->bufpos > 0) {
                auto bufpos = c->bufpos;
                lock.unlock();
                nwritten = connWrite(c->conn,c->buf+c->sentlen,bufpos-c->sentlen);
                lock.lock();
                if (nwritten <= 0) break;
                c->sentlen += nwritten;
                totwritten += nwritten;

                /* If the buffer was sent, set bufpos to zero to continue with
                * the remainder of the reply. */
                if ((int)c->sentlen == c->bufpos) {
                    c->bufpos = 0;
                    c->sentlen = 0;
                }
            } else {
                o = (clientReplyBlock*)listNodeValue(listFirst(c->reply));
                if (o->used == 0) {
                    c->reply_bytes -= o->size;
                    listDelNode(c->reply,listFirst(c->reply));
                    continue;
                }

                auto used = o->used;
                lock.unlock();
                nwritten = connWrite(c->conn, o->buf() + c->sentlen, used - c->sentlen);
                lock.lock();
                if (nwritten <= 0) break;
                c->sentlen += nwritten;
                totwritten += nwritten;
                
                /* If we fully sent the object on head go to the next one */
                if (c->sentlen == o->used) {
                    c->reply_bytes -= o->size;
                    listDelNode(c->reply,listFirst(c->reply));
                    c->sentlen = 0;
                    /* If there are no longer objects in the list, we expect
                        * the count of reply bytes to be exactly zero. */
                    if (listLength(c->reply) == 0)
                        serverAssert(c->reply_bytes == 0);
                }
            }
            /* Note that we avoid to send more than NET_MAX_WRITES_PER_EVENT
            * bytes, in a single threaded server it's a good idea to serve
            * other clients as well, even if a very large request comes from
            * super fast link that is always able to accept data (in real world
            * scenario think about 'KEYS *' against the loopback interface).
            *
            * However if we are over the maxmemory limit we ignore that and
            * just deliver as much data as it is possible to deliver.
            *
            * Moreover, we also send as much as possible if the client is
            * a replica or a monitor (otherwise, on high-speed traffic, the
            * replication/output buffer will grow indefinitely) */
            if (totwritten > NET_MAX_WRITES_PER_EVENT &&
                (g_pserver->maxmemory == 0 ||
                zmalloc_used_memory() < g_pserver->maxmemory) &&
                !(c->flags & CLIENT_SLAVE)) break;
        }
    }
    g_pserver->stat_net_output_bytes += totwritten;
    if (nwritten == -1) {
        if (connGetState(c->conn) != CONN_STATE_CONNECTED) {
            serverLog(LL_VERBOSE,
                "Error writing to client: %s", connGetLastError(c->conn));
            freeClientAsync(c);
            
            return C_ERR;
        }
    }
    if (totwritten > 0) {
        /* For clients representing masters we don't count sending data
         * as an interaction, since we always send REPLCONF ACK commands
         * that take some time to just fill the socket output buffer.
         * We just rely on data / pings received for timeout detection. */
        if (!(c->flags & CLIENT_MASTER)) c->lastinteraction = g_pserver->unixtime;
    }
    if (!clientHasPendingReplies(c)) {
        c->sentlen = 0;
        if (handler_installed) connSetWriteHandler(c->conn, NULL);

        /* Close connection after entire reply has been sent. */
        if (c->flags & CLIENT_CLOSE_AFTER_REPLY) {
            freeClientAsync(c);
            return C_ERR;
        }
    }
    return C_OK;
}

/* Write event handler. Just send data to the client. */
void sendReplyToClient(connection *conn) {
    client *c = (client*)connGetPrivateData(conn);
    if (writeToClient(c,1) == C_ERR)
    {
        AeLocker ae;
        c->lock.lock();
        ae.arm(c);
        if (c->flags & CLIENT_CLOSE_ASAP)
        {
            if (!freeClient(c))
                c->lock.unlock();
        }
    }
}

void ProcessPendingAsyncWrites()
{
    if (serverTL == nullptr)
        return; // module fake call

    serverAssert(GlobalLocksAcquired());

    while(listLength(serverTL->clients_pending_asyncwrite)) {
        client *c = (client*)listNodeValue(listFirst(serverTL->clients_pending_asyncwrite));
        listDelNode(serverTL->clients_pending_asyncwrite, listFirst(serverTL->clients_pending_asyncwrite));
        std::lock_guard<decltype(c->lock)> lock(c->lock);

        serverAssert(c->fPendingAsyncWrite);
        if (c->flags & (CLIENT_CLOSE_ASAP | CLIENT_CLOSE_AFTER_REPLY))
        {
            if (c->replyAsync != nullptr){
                zfree(c->replyAsync);
                c->replyAsync = nullptr;
            }
            c->fPendingAsyncWrite = FALSE;
            continue;
        }

        /* since writes from master to replica can come directly from the replication backlog,
         * writes may have been signalled without having been copied to the replyAsync buffer,
         * thus causing the buffer to be NULL */ 
        if (c->replyAsync != nullptr){
            int size = c->replyAsync->used;

            if (listLength(c->reply) == 0 && size <= (PROTO_REPLY_CHUNK_BYTES - c->bufpos)) {
                memcpy(c->buf + c->bufpos, c->replyAsync->buf(), size);
                c->bufpos += size;
            } else {
                c->reply_bytes += c->replyAsync->size;
                listAddNodeTail(c->reply, c->replyAsync);
                c->replyAsync = nullptr;
            }

            zfree(c->replyAsync);
            c->replyAsync = nullptr;
        } else {
            /* Only replicas should have empty async reply buffers */
            serverAssert(c->flags & CLIENT_SLAVE);
        }

        c->fPendingAsyncWrite = FALSE;

        if (!((c->replstate == REPL_STATE_NONE || c->replstate == SLAVE_STATE_FASTSYNC_TX ||
         (c->replstate == SLAVE_STATE_ONLINE && !c->repl_put_online_on_ack))))
            continue;

        closeClientOnOutputBufferLimitReached(c, 1);
        if (c->flags & CLIENT_CLOSE_ASAP)
            continue;   // we will never write this so don't post an op

        std::atomic_thread_fence(std::memory_order_seq_cst);

        if (FCorrectThread(c))
        {
            prepareClientToWrite(c); // queue an event
        }
        else
        {
            bool expected = false;
            if (c->fPendingAsyncWriteHandler.compare_exchange_strong(expected, true)) {
                bool fResult = c->postFunction([](client *c) {
                    c->fPendingAsyncWriteHandler = false;
                    clientInstallWriteHandler(c);
                    c->lock.unlock();
                    handleClientsWithPendingWrites(c->iel, g_pserver->aof_state);
                    c->lock.lock();
                }, false);

                if (!fResult)
                    c->fPendingAsyncWriteHandler = false;   // if we failed to set the handler then prevent this from never being reset
            }
        }
    }
}

/* This function is called just before entering the event loop, in the hope
 * we can just write the replies to the client output buffer without any
 * need to use a syscall in order to install the writable event handler,
 * get it called, and so forth. */
int handleClientsWithPendingWrites(int iel, int aof_state) {
    int processed = 0;
    serverAssert(iel == (serverTL - g_pserver->rgthreadvar));

    if (listLength(serverTL->clients_pending_asyncwrite))
    {
        AeLocker locker;
        locker.arm(nullptr);
        ProcessPendingAsyncWrites();
    }

    int ae_flags = AE_WRITABLE|AE_WRITE_THREADSAFE;
    /* For the fsync=always policy, we want that a given FD is never
        * served for reading and writing in the same event loop iteration,
        * so that in the middle of receiving the query, and serving it
        * to the client, we'll call beforeSleep() that will do the
        * actual fsync of AOF to disk. AE_BARRIER ensures that. */
    if (aof_state == AOF_ON &&
        g_pserver->aof_fsync == AOF_FSYNC_ALWAYS)
    {
        ae_flags |= AE_BARRIER;
    }

    std::unique_lock<fastlock> lockf(g_pserver->rgthreadvar[iel].lockPendingWrite);
    auto vec = std::move(g_pserver->rgthreadvar[iel].clients_pending_write);
    lockf.unlock();
    processed += (int)vec.size();

    for (client *c : vec) {
        serverAssertDebug(FCorrectThread(c));

        uint64_t flags = c->flags.fetch_and(~CLIENT_PENDING_WRITE, std::memory_order_relaxed);

        /* If a client is protected, don't do anything,
        * that may trigger write error or recreate handler. */
        if ((flags & CLIENT_PROTECTED) && !(flags & CLIENT_SLAVE)) continue;

        /* Don't write to clients that are going to be closed anyway. */
        if (c->flags & CLIENT_CLOSE_ASAP) continue;

        /* Try to write buffers to the client socket, unless its a replica in multithread mode */
        if (writeToClient(c,0) == C_ERR) 
        {
            if (c->flags & CLIENT_CLOSE_ASAP)
            {
                AeLocker ae;
                ae.arm(nullptr);
                freeClient(c); // writeToClient will only async close, but there's no need to wait
            }
            continue;
        }

        /* If after the synchronous writes above we still have data to
        * output to the client, we need to install the writable handler. */
        std::unique_lock<decltype(c->lock)> lock(c->lock);
        if (clientHasPendingReplies(c)) {
            if (connSetWriteHandlerWithBarrier(c->conn, sendReplyToClient, ae_flags, true) == C_ERR) {
                freeClientAsync(c);
            }
        }
    }

    return processed;
}

/* resetClient prepare the client to process the next command */
void resetClient(client *c) {
    redisCommandProc *prevcmd = c->cmd ? c->cmd->proc : NULL;

    freeClientArgv(c);

    /* We clear the ASKING flag as well if we are not inside a MULTI, and
     * if what we just executed is not the ASKING command itself. */
    if (!(c->flags & CLIENT_MULTI) && prevcmd != askingCommand)
        c->flags &= ~CLIENT_ASKING;

    /* We do the same for the CACHING command as well. It also affects
     * the next command or transaction executed, in a way very similar
     * to ASKING. */
    if (!(c->flags & CLIENT_MULTI) && prevcmd != clientCommand)
        c->flags &= ~CLIENT_TRACKING_CACHING;

    /* Remove the CLIENT_REPLY_SKIP flag if any so that the reply
     * to the next command will be sent, but set the flag if the command
     * we just processed was "CLIENT REPLY SKIP". */
    c->flags &= ~CLIENT_REPLY_SKIP;
    if (c->flags & CLIENT_REPLY_SKIP_NEXT) {
        c->flags |= CLIENT_REPLY_SKIP;
        c->flags &= ~CLIENT_REPLY_SKIP_NEXT;
    }
}

/* This function is used when we want to re-enter the event loop but there
 * is the risk that the client we are dealing with will be freed in some
 * way. This happens for instance in:
 *
 * * DEBUG RELOAD and similar.
 * * When a Lua script is in -BUSY state.
 *
 * So the function will protect the client by doing two things:
 *
 * 1) It removes the file events. This way it is not possible that an
 *    error is signaled on the socket, freeing the client.
 * 2) Moreover it makes sure that if the client is freed in a different code
 *    path, it is not really released, but only marked for later release. */
void protectClient(client *c) {
    c->flags |= CLIENT_PROTECTED;
    AssertCorrectThread(c);
    if (c->conn) {
        connSetReadHandler(c->conn,NULL);
        connSetWriteHandler(c->conn,NULL);
    }
}

/* This will undo the client protection done by protectClient() */
void unprotectClient(client *c) {
    AssertCorrectThread(c);
    if (c->flags & CLIENT_PROTECTED) {
        c->flags &= ~CLIENT_PROTECTED;
        if (c->conn) {
            connSetReadHandler(c->conn,readQueryFromClient, true);
            if (clientHasPendingReplies(c)) clientInstallWriteHandler(c);
        }
    }
}

/* Like processMultibulkBuffer(), but for the inline protocol instead of RESP,
 * this function consumes the client query buffer and creates a command ready
 * to be executed inside the client structure. Returns C_OK if the command
 * is ready to be executed, or C_ERR if there is still protocol to read to
 * have a well formed command. The function also returns C_ERR when there is
 * a protocol error: in such a case the client structure is setup to reply
 * with the error and close the connection. */
int processInlineBuffer(client *c) {
    char *newline;
    int argc, j, linefeed_chars = 1;
    sds *argv, aux;
    size_t querylen;

    /* Search for end of line */
    newline = strchr(c->querybuf+c->qb_pos,'\n');

    /* Nothing to do without a \r\n */
    if (newline == NULL) {
        if (sdslen(c->querybuf)-c->qb_pos > PROTO_INLINE_MAX_SIZE) {
            addReplyError(c,"Protocol error: too big inline request");
            setProtocolError("too big inline request",c);
        }
        return C_ERR;
    }

    /* Handle the \r\n case. */
    if (newline != c->querybuf+c->qb_pos && *(newline-1) == '\r')
        newline--, linefeed_chars++;

    /* Split the input buffer up to the \r\n */
    querylen = newline-(c->querybuf+c->qb_pos);
    aux = sdsnewlen(c->querybuf+c->qb_pos,querylen);
    argv = sdssplitargs(aux,&argc);
    sdsfree(aux);
    if (argv == NULL) {
        addReplyError(c,"Protocol error: unbalanced quotes in request");
        setProtocolError("unbalanced quotes in inline request",c);
        return C_ERR;
    }

    /* Newline from slaves can be used to refresh the last ACK time.
     * This is useful for a replica to ping back while loading a big
     * RDB file. */
    if (querylen == 0 && getClientType(c) == CLIENT_TYPE_SLAVE)
        c->repl_ack_time = g_pserver->unixtime;

    /* Masters should never send us inline protocol to run actual
     * commands. If this happens, it is likely due to a bug in Redis where
     * we got some desynchronization in the protocol, for example
     * beause of a PSYNC gone bad.
     *
     * However the is an exception: masters may send us just a newline
     * to keep the connection active. */
    if (querylen != 0 && c->flags & CLIENT_MASTER) {
        sdsfreesplitres(argv,argc);
        serverLog(LL_WARNING,"WARNING: Receiving inline protocol from master, master stream corruption? Closing the master connection and discarding the cached master.");
        setProtocolError("Master using the inline protocol. Desync?",c);
        return C_ERR;
    }

    /* Move querybuffer position to the next query in the buffer. */
    c->qb_pos += querylen+linefeed_chars;

    /* Setup argv array on client structure */
    if (argc) {
        /* Create redis objects for all arguments. */
        c->vecqueuedcmd.emplace_back(argc);
        auto &cmd = c->vecqueuedcmd.back();
        for (cmd.argc = 0, j = 0; j < argc; j++) {
            cmd.argv[cmd.argc++] = createObject(OBJ_STRING,argv[j]);
            cmd.argv_len_sum += sdslen(argv[j]);
        }
    }
    sds_free(argv);
    return C_OK;
}

/* Helper function. Record protocol erro details in server log,
 * and set the client as CLIENT_CLOSE_AFTER_REPLY and
 * CLIENT_PROTOCOL_ERROR. */
#define PROTO_DUMP_LEN 128
static void setProtocolError(const char *errstr, client *c) {
    if (cserver.verbosity <= LL_VERBOSE || c->flags & CLIENT_MASTER) {
        sds client = catClientInfoString(sdsempty(),c);

        /* Sample some protocol to given an idea about what was inside. */
        char buf[256];
        if (sdslen(c->querybuf)-c->qb_pos < PROTO_DUMP_LEN) {
            snprintf(buf,sizeof(buf),"Query buffer during protocol error: '%s'", c->querybuf+c->qb_pos);
        } else {
            snprintf(buf,sizeof(buf),"Query buffer during protocol error: '%.*s' (... more %zu bytes ...) '%.*s'", PROTO_DUMP_LEN/2, c->querybuf+c->qb_pos, sdslen(c->querybuf)-c->qb_pos-PROTO_DUMP_LEN, PROTO_DUMP_LEN/2, c->querybuf+sdslen(c->querybuf)-PROTO_DUMP_LEN/2);
        }

        /* Remove non printable chars. */
        char *p = buf;
        while (*p != '\0') {
            if (!isprint(*p)) *p = '.';
            p++;
        }

        /* Log all the client and protocol info. */
        int loglevel = (c->flags & CLIENT_MASTER) ? LL_WARNING :
                                                    LL_VERBOSE;
        serverLog(loglevel,
            "Protocol error (%s) from client: %s. %s", errstr, client, buf);
        sdsfree(client);
    }
    c->flags |= (CLIENT_CLOSE_AFTER_REPLY|CLIENT_PROTOCOL_ERROR);
}

/* Process the query buffer for client 'c', setting up the client argument
 * vector for command execution. Returns C_OK if after running the function
 * the client has a well-formed ready to be processed command, otherwise
 * C_ERR if there is still to read more buffer to get the full command.
 * The function also returns C_ERR when there is a protocol error: in such a
 * case the client structure is setup to reply with the error and close
 * the connection.
 *
 * This function is called if processInputBuffer() detects that the next
 * command is in RESP format, so the first byte in the command is found
 * to be '*'. Otherwise for inline commands processInlineBuffer() is called. */
int processMultibulkBuffer(client *c) {
    char *newline = NULL;
    int ok;
    long long ll;

    if (c->multibulklen == 0) {
        /* The client should have been reset */
        serverAssertWithInfo(c,NULL,c->argc == 0);

        /* Multi bulk length cannot be read without a \r\n */
        newline = strchr(c->querybuf+c->qb_pos,'\r');
        if (newline == NULL) {
            if (sdslen(c->querybuf)-c->qb_pos > PROTO_INLINE_MAX_SIZE) {
                addReplyError(c,"Protocol error: too big mbulk count string");
                setProtocolError("too big mbulk count string",c);
            }
            return C_ERR;
        }

        /* Buffer should also contain \n */
        if (newline-(c->querybuf+c->qb_pos) > (ssize_t)(sdslen(c->querybuf)-c->qb_pos-2))
            return C_ERR;

        /* We know for sure there is a whole line since newline != NULL,
         * so go ahead and find out the multi bulk length. */
        serverAssertWithInfo(c,NULL,c->querybuf[c->qb_pos] == '*');
        ok = string2ll(c->querybuf+1+c->qb_pos,newline-(c->querybuf+1+c->qb_pos),&ll);
        if (!ok || ll > 1024*1024) {
            addReplyError(c,"Protocol error: invalid multibulk length");
            setProtocolError("invalid mbulk count",c);
            return C_ERR;
        } else if (ll > 10 && authRequired(c)) {
            addReplyError(c, "Protocol error: unauthenticated multibulk length");
            setProtocolError("unauth mbulk count", c);
            return C_ERR;
        }

        c->qb_pos = (newline-c->querybuf)+2;

        if (ll <= 0) return C_OK;

        c->multibulklen = ll;

        /* Setup argv array on client structure */
        c->vecqueuedcmd.emplace_back(c->multibulklen);
    }

    serverAssertWithInfo(c,NULL,c->multibulklen > 0);
    while(c->multibulklen) {
        /* Read bulk length if unknown */
        if (c->bulklen == -1) {
            newline = strchr(c->querybuf+c->qb_pos,'\r');
            if (newline == NULL) {
                if (sdslen(c->querybuf)-c->qb_pos > PROTO_INLINE_MAX_SIZE) {
                    addReplyError(c,
                        "Protocol error: too big bulk count string");
                    setProtocolError("too big bulk count string",c);
                    return C_ERR;
                }
                break;
            }

            /* Buffer should also contain \n */
            if (newline-(c->querybuf+c->qb_pos) > (ssize_t)(sdslen(c->querybuf)-c->qb_pos-2))
                break;

            if (c->querybuf[c->qb_pos] != '$') {
                addReplyErrorFormat(c,
                    "Protocol error: expected '$', got '%c'",
                    c->querybuf[c->qb_pos]);
                setProtocolError("expected $ but got something else",c);
                return C_ERR;
            }

            ok = string2ll(c->querybuf+c->qb_pos+1,newline-(c->querybuf+c->qb_pos+1),&ll);
            if (!ok || ll < 0 ||
                (!(c->flags & CLIENT_MASTER) && ll > g_pserver->proto_max_bulk_len)) {
                addReplyError(c,"Protocol error: invalid bulk length");
                setProtocolError("invalid bulk length",c);
                return C_ERR;
            } else if (ll > 16384 && authRequired(c)) {
                addReplyError(c, "Protocol error: unauthenticated bulk length");
                setProtocolError("unauth bulk length", c);
                return C_ERR;
            }

            c->qb_pos = newline-c->querybuf+2;
            if (ll >= PROTO_MBULK_BIG_ARG) {
                /* If we are going to read a large object from network
                 * try to make it likely that it will start at c->querybuf
                 * boundary so that we can optimize object creation
                 * avoiding a large copy of data.
                 *
                 * But only when the data we have not parsed is less than
                 * or equal to ll+2. If the data length is greater than
                 * ll+2, trimming querybuf is just a waste of time, because
                 * at this time the querybuf contains not only our bulk. */
                if (sdslen(c->querybuf)-c->qb_pos <= (size_t)ll+2) {
                    sdsrange(c->querybuf,c->qb_pos,-1);
                    c->qb_pos = 0;
                    /* Hint the sds library about the amount of bytes this string is
                     * going to contain. */
                    c->querybuf = sdsMakeRoomFor(c->querybuf,ll+2-sdslen(c->querybuf));
                }
            }
            c->bulklen = ll;
        }

        /* Read bulk argument */
        if (sdslen(c->querybuf)-c->qb_pos < (size_t)(c->bulklen+2)) {
            /* Not enough data (+2 == trailing \r\n) */
            break;
        } else {
            /* Optimization: if the buffer contains JUST our bulk element
             * instead of creating a new object by *copying* the sds we
             * just use the current sds string. */
            auto &cmd = c->vecqueuedcmd.back();
            if (c->qb_pos == 0 &&
                c->bulklen >= PROTO_MBULK_BIG_ARG &&
                sdslen(c->querybuf) == (size_t)(c->bulklen+2))
            {
                cmd.argv[cmd.argc++] = createObject(OBJ_STRING,c->querybuf);
                cmd.argv_len_sum += c->bulklen;
                sdsIncrLen(c->querybuf,-2); /* remove CRLF */
                /* Assume that if we saw a fat argument we'll see another one
                 * likely... */
                c->querybuf = sdsnewlen(SDS_NOINIT,c->bulklen+2);
                sdsclear(c->querybuf);
            } else {
                cmd.argv[cmd.argc++] =
                    createStringObject(c->querybuf+c->qb_pos,c->bulklen);
                cmd.argv_len_sum += c->bulklen;
                c->qb_pos += c->bulklen+2;
            }
            c->bulklen = -1;
            c->multibulklen--;
        }
    }

    /* We're done when c->multibulk == 0 */
    if (c->multibulklen == 0) return C_OK;

    /* Still not ready to process the command */
    return C_ERR;
}

/* Perform necessary tasks after a command was executed:
 *
 * 1. The client is reset unless there are reasons to avoid doing it. 
 * 2. In the case of master clients, the replication offset is updated.
 * 3. Propagate commands we got from our master to replicas down the line. */
void commandProcessed(client *c, int flags) {
        /* If client is blocked(including paused), just return avoid reset and replicate.
     *
     * 1. Don't reset the client structure for blocked clients, so that the reply
     *    callback will still be able to access the client argv and argc fields.
     *    The client will be reset in unblockClient().
     * 2. Don't update replication offset or propagate commands to replicas,
     *    since we have not applied the command. */
    if (c->flags & CLIENT_BLOCKED) return;

    resetClient(c);

    long long prev_offset = c->reploff;
    if (c->flags & CLIENT_MASTER && !(c->flags & CLIENT_MULTI)) {
        /* Update the applied replication offset of our master. */
        serverAssert(c->reploff <= c->reploff_cmd);
        c->reploff = c->reploff_cmd;
    }

    /* If the client is a master we need to compute the difference
     * between the applied offset before and after processing the buffer,
     * to understand how much of the replication stream was actually
     * applied to the master state: this quantity, and its corresponding
     * part of the replication stream, will be propagated to the
     * sub-replicas and to the replication backlog. */
    if (c->flags & CLIENT_MASTER) {
        AeLocker ae;
        ae.arm(c);
        long long applied = c->reploff - prev_offset;
        if (applied) {
            if (!g_pserver->fActiveReplica && (flags & CMD_CALL_PROPAGATE))
            {
                replicationFeedSlavesFromMasterStream(c->pending_querybuf, applied);
            }
            sdsrange(c->pending_querybuf,applied,-1);
        }
    }
}

/* This function calls processCommand(), but also performs a few sub tasks
 * for the client that are useful in that context:
 *
 * 1. It sets the current client to the client 'c'.
 * 2. calls commandProcessed() if the command was handled.
 *
 * The function returns C_ERR in case the client was freed as a side effect
 * of processing the command, otherwise C_OK is returned. */
int processCommandAndResetClient(client *c, int flags) {
    int deadclient = 0;
    client *old_client = serverTL->current_client;
    serverTL->current_client = c;
    serverAssert((flags & CMD_CALL_ASYNC) || GlobalLocksAcquired());
    
    if (processCommand(c, flags) == C_OK) {
        commandProcessed(c, flags);
    }
    if (serverTL->current_client == NULL) deadclient = 1;
    /*
     * Restore the old client, this is needed because when a script
     * times out, we will get into this code from processEventsWhileBlocked.
     * Which will cause to set the server.current_client. If not restored
     * we will return 1 to our caller which will falsely indicate the client
     * is dead and will stop reading from its buffer.
     */
    serverTL->current_client = old_client;
    /* performEvictions may flush slave output buffers. This may
     * result in a replica, that may be the active client, to be
     * freed. */
    return deadclient ? C_ERR : C_OK;
}

/* This function will execute any fully parsed commands pending on
 * the client. Returns C_ERR if the client is no longer valid after executing
 * the command, and C_OK for all other cases. */
int processPendingCommandsAndResetClient(client *c, int flags) {
    if (c->flags & CLIENT_PENDING_COMMAND) {
        c->flags &= ~CLIENT_PENDING_COMMAND;
        if (processCommandAndResetClient(c, flags) == C_ERR) {
            return C_ERR;
        }
    }
    return C_OK;
}

bool FClientReady(client *c) {
    /* Immediately abort if the client is in the middle of something. */
    if (c->flags & CLIENT_BLOCKED) return false;
    if (c->flags & CLIENT_PENDING_COMMAND) return false;

    if (c->flags & CLIENT_EXECUTING_COMMAND) return false;

    /* Don't process input from the master while there is a busy script
        * condition on the replica. We want just to accumulate the replication
        * stream (instead of replying -BUSY like we do with other clients) and
        * later resume the processing. */
    if (g_pserver->lua_timedout && c->flags & CLIENT_MASTER) return false;

    /* CLIENT_CLOSE_AFTER_REPLY closes the connection once the reply is
        * written to the client. Make sure to not let the reply grow after
        * this flag has been set (i.e. don't process more commands).
        *
        * The same applies for clients we want to terminate ASAP. */
    if (c->flags & (CLIENT_CLOSE_AFTER_REPLY|CLIENT_CLOSE_ASAP)) return false;

    return true;
}

void parseClientCommandBuffer(client *c) {
    if (!FClientReady(c))
        return;
    
    while(c->qb_pos < sdslen(c->querybuf)) {    
        /* Determine request type when unknown. */
        if (!c->reqtype) {
            if (c->querybuf[c->qb_pos] == '*') {
                c->reqtype = PROTO_REQ_MULTIBULK;
            } else {
                c->reqtype = PROTO_REQ_INLINE;
            }
        }

        size_t cqueriesStart = c->vecqueuedcmd.size();
        if (c->reqtype == PROTO_REQ_INLINE) {
            if (processInlineBuffer(c) != C_OK) break;
        } else if (c->reqtype == PROTO_REQ_MULTIBULK) {
            if (processMultibulkBuffer(c) != C_OK) break;
        } else {
            serverPanic("Unknown request type");
        }
        if (!c->vecqueuedcmd.empty() && (c->vecqueuedcmd.back().argc <= 0 || c->vecqueuedcmd.back().argv == nullptr)) {
            c->vecqueuedcmd.pop_back();
        } else if (!c->vecqueuedcmd.empty()) {
            if (c->flags & CLIENT_MASTER) c->vecqueuedcmd.back().reploff = c->read_reploff - sdslen(c->querybuf) + c->qb_pos;
            serverAssert(c->vecqueuedcmd.back().reploff >= 0);
        }

        /* Prefetch outside the lock for better perf */
        if (g_pserver->prefetch_enabled && (cserver.cthreads > 1 || g_pserver->m_pstorageFactory) && cqueriesStart < c->vecqueuedcmd.size() &&
            (g_pserver->m_pstorageFactory || aeLockContested(cserver.cthreads/2) || cserver.cthreads == 1) && !GlobalLocksAcquired()) {
            auto &query = c->vecqueuedcmd.back();
            if (query.argc > 0 && query.argc == query.argcMax) {
                if (c->db->prefetchKeysAsync(c, query, c->vecqueuedcmd.size() == 1)) {
                    c->vecqueuedcmd.erase(c->vecqueuedcmd.begin());
                }
            }
        }
        c->reqtype = 0;
        c->multibulklen = 0;
        c->bulklen = -1;
        c->reqtype = 0;
    }

    /* Trim to pos */
    if (c->qb_pos) {
        sdsrange(c->querybuf,c->qb_pos,-1);
        c->qb_pos = 0;
    }
}

bool FAsyncCommand(parsed_command &cmd)
{
    if (serverTL->in_eval || serverTL->in_exec)
        return false;
    auto parsedcmd = lookupCommand(szFromObj(cmd.argv[0]));
    if (parsedcmd == nullptr)
        return false;
    static const long long expectedFlags = CMD_ASYNC_OK | CMD_READONLY;
    return (parsedcmd->flags & expectedFlags) == expectedFlags;
}

/* This function is called every time, in the client structure 'c', there is
 * more query buffer to process, because we read more data from the socket
 * or because a client was blocked and later reactivated, so there could be
 * pending query buffer, already representing a full command, to process. */
void processInputBuffer(client *c, bool fParse, int callFlags) {
    AssertCorrectThread(c);
    
    if (fParse)
        parseClientCommandBuffer(c);

    /* Keep processing while there is something in the input buffer */
    while (!c->vecqueuedcmd.empty()) {
        /* Return if we're still parsing this command */
        auto &cmd = c->vecqueuedcmd.front();
        if (cmd.argc != cmd.argcMax) break;
        if (c->flags & CLIENT_EXECUTING_COMMAND) break;

        if (!FClientReady(c)) break;

        if ((callFlags & CMD_CALL_ASYNC) && !FAsyncCommand(cmd))
            break;

        zfree(c->argv);
        c->argc = cmd.argc;
        c->argv = cmd.argv;
        cmd.argv = nullptr;
        c->argv_len_sumActive = cmd.argv_len_sum;
        cmd.argv_len_sum = 0;
        c->reploff_cmd = cmd.reploff;
        serverAssert(c->argv != nullptr);

        c->vecqueuedcmd.erase(c->vecqueuedcmd.begin());

        /* Multibulk processing could see a <= 0 length. */
        if (c->argc == 0) {
            resetClient(c);
        } else {
            c->flags |= CLIENT_EXECUTING_COMMAND;
            /* We are finally ready to execute the command. */
            if (processCommandAndResetClient(c, callFlags) == C_ERR) {
                /* If the client is no longer valid, we avoid exiting this
                 * loop and trimming the client buffer later. So we return
                 * ASAP in that case. */
                c->flags &= ~CLIENT_EXECUTING_COMMAND;
                return;
            }
            c->flags &= ~CLIENT_EXECUTING_COMMAND;
        }
    }
}

void readQueryFromClient(connection *conn) {
    client *c = (client*)connGetPrivateData(conn);
    serverAssert(conn == c->conn);
    int nread, readlen;
    size_t qblen;

    serverAssertDebug(FCorrectThread(c));
    serverAssertDebug(!GlobalLocksAcquired());
    
    AeLocker aelock;
    AssertCorrectThread(c);
    std::unique_lock<decltype(c->lock)> lock(c->lock, std::defer_lock);
    if (!lock.try_lock())
        return; // Process something else while we wait

    /* Update total number of reads on server */
    g_pserver->stat_total_reads_processed.fetch_add(1, std::memory_order_relaxed);

    readlen = PROTO_IOBUF_LEN;
    /* If this is a multi bulk request, and we are processing a bulk reply
     * that is large enough, try to maximize the probability that the query
     * buffer contains exactly the SDS string representing the object, even
     * at the risk of requiring more read(2) calls. This way the function
     * processMultiBulkBuffer() can avoid copying buffers to create the
     * Redis Object representing the argument. */
    if (c->reqtype == PROTO_REQ_MULTIBULK && c->multibulklen && c->bulklen != -1
        && c->bulklen >= PROTO_MBULK_BIG_ARG)
    {
        ssize_t remaining = (size_t)(c->bulklen+2)-sdslen(c->querybuf);

        /* Note that the 'remaining' variable may be zero in some edge case,
         * for example once we resume a blocked client after CLIENT PAUSE. */
        if (remaining > 0 && remaining < readlen) readlen = remaining;
    }

    qblen = sdslen(c->querybuf);
    if (c->querybuf_peak < qblen) c->querybuf_peak = qblen;
    c->querybuf = sdsMakeRoomFor(c->querybuf, readlen);

    nread = connRead(c->conn, c->querybuf+qblen, readlen);
    
    if (nread == -1) {
        if (connGetState(conn) == CONN_STATE_CONNECTED) {
            return;
        } else {
            serverLog(LL_VERBOSE, "Reading from client: %s",connGetLastError(c->conn));
            aelock.arm(c);
            freeClientAsync(c);
            return;
        }
    } else if (nread == 0) {
        serverLog(LL_VERBOSE, "Client closed connection");
        aelock.arm(c);
        freeClientAsync(c);
        return;
    } else if (c->flags & CLIENT_MASTER) {
        /* Append the query buffer to the pending (not applied) buffer
         * of the master. We'll use this buffer later in order to have a
         * copy of the string applied by the last command executed. */
        c->pending_querybuf = sdscatlen(c->pending_querybuf,
                                        c->querybuf+qblen,nread);
    }

    sdsIncrLen(c->querybuf,nread);
    c->lastinteraction = g_pserver->unixtime;
    if (c->flags & CLIENT_MASTER) c->read_reploff += nread;
    g_pserver->stat_net_input_bytes += nread;
    if (sdslen(c->querybuf) > cserver.client_max_querybuf_len) {
        sds ci = catClientInfoString(sdsempty(),c), bytes = sdsempty();

        bytes = sdscatrepr(bytes,c->querybuf,64);
        serverLog(LL_WARNING,"Closing client that reached max query buffer length: %s (qbuf initial bytes: %s)", ci, bytes);
        sdsfree(ci);
        sdsfree(bytes);       
        freeClientAsync(c);
        return;
    }

    if (cserver.cthreads > 1 || g_pserver->m_pstorageFactory) {
        parseClientCommandBuffer(c);
        if (g_pserver->enable_async_commands && !serverTL->disable_async_commands && listLength(g_pserver->monitors) == 0 && (aeLockContention() || serverTL->rgdbSnapshot[c->db->id] || g_fTestMode)) {
            // Frequent writers aren't good candidates for this optimization, they cause us to renew the snapshot too often
            //  so we exclude them unless the snapshot we need already exists
            bool fSnapshotExists = c->db->mvccLastSnapshot >= c->mvccCheckpoint;
            bool fWriteTooRecent = (((getMvccTstamp() - c->mvccCheckpoint) >> MVCC_MS_SHIFT) < static_cast<uint64_t>(g_pserver->snapshot_slip)/2);

            // The check below avoids running async commands if this is a frequent writer unless a snapshot is already there to service it
            if (!fWriteTooRecent || fSnapshotExists) {
                processInputBuffer(c, false, CMD_CALL_SLOWLOG | CMD_CALL_STATS | CMD_CALL_ASYNC);
            }
        }
        if (!c->vecqueuedcmd.empty())
            serverTL->vecclientsProcess.push_back(c);
    } else {
        // If we're single threaded its actually better to just process the command here while the query is hot in the cache
        //  multithreaded lock contention dominates and batching is better
        AeLocker locker;
        locker.arm(c);
        runAndPropogateToReplicas(processInputBuffer, c, true /*fParse*/, CMD_CALL_FULL);
    }
}

void processClients()
{
    serverAssert(GlobalLocksAcquired());

    // Note that this function is reentrant and vecclients may be modified by code called from processInputBuffer
    while (!serverTL->vecclientsProcess.empty()) {
        client *c = serverTL->vecclientsProcess.front();
        serverTL->vecclientsProcess.erase(serverTL->vecclientsProcess.begin());

        /* There is more data in the client input buffer, continue parsing it
        * in case to check if there is a full command to execute. */
        std::unique_lock<fastlock> ul(c->lock);
        processInputBuffer(c, false /*fParse*/, CMD_CALL_FULL);
    }

    if (listLength(serverTL->clients_pending_asyncwrite))
    {
        ProcessPendingAsyncWrites();
    }
}

void getClientsMaxBuffers(unsigned long *longest_output_list,
                          unsigned long *biggest_input_buffer) {
    client *c;
    listNode *ln;
    listIter li;
    unsigned long lol = 0, bib = 0;

    listRewind(g_pserver->clients,&li);
    while ((ln = listNext(&li)) != NULL) {
        c = (client*)listNodeValue(ln);

        if (listLength(c->reply) > lol) lol = listLength(c->reply);
        if (sdslen(c->querybuf) > bib) bib = sdslen(c->querybuf);
    }
    *longest_output_list = lol;
    *biggest_input_buffer = bib;
}

/* A Redis "Address String" is a colon separated ip:port pair.
 * For IPv4 it's in the form x.y.z.k:port, example: "127.0.0.1:1234".
 * For IPv6 addresses we use [] around the IP part, like in "[::1]:1234".
 * For Unix sockets we use path:0, like in "/tmp/redis:0".
 *
 * An Address String always fits inside a buffer of NET_ADDR_STR_LEN bytes,
 * including the null term.
 *
 * On failure the function still populates 'addr' with the "?:0" string in case
 * you want to relax error checking or need to display something anyway (see
 * anetFdToString implementation for more info). */
void genClientAddrString(client *client, char *addr,
                         size_t addr_len, int fd_to_str_type) {
    if (client->flags & CLIENT_UNIX_SOCKET) {
        /* Unix socket client. */
        snprintf(addr,addr_len,"%s:0",g_pserver->unixsocket);
    } else {
        /* TCP client. */
        connFormatFdAddr(client->conn,addr,addr_len,fd_to_str_type);
    }
}

/* This function returns the client peer id, by creating and caching it
 * if client->peerid is NULL, otherwise returning the cached value.
 * The Peer ID never changes during the life of the client, however it
 * is expensive to compute. */
char *getClientPeerId(client *c) {
    char peerid[NET_ADDR_STR_LEN];

    if (c->peerid == NULL) {
        genClientAddrString(c,peerid,sizeof(peerid),FD_TO_PEER_NAME);
        c->peerid = sdsnew(peerid);
    }
    return c->peerid;
}

/* This function returns the client bound socket name, by creating and caching
 * it if client->sockname is NULL, otherwise returning the cached value.
 * The Socket Name never changes during the life of the client, however it
 * is expensive to compute. */
char *getClientSockname(client *c) {
    char sockname[NET_ADDR_STR_LEN];

    if (c->sockname == NULL) {
        genClientAddrString(c,sockname,sizeof(sockname),FD_TO_SOCK_NAME);
        c->sockname = sdsnew(sockname);
    }
    return c->sockname;
}

/* Concatenate a string representing the state of a client in a human
 * readable format, into the sds string 's'. */
sds catClientInfoString(sds s, client *client) {
    char flags[16], events[3], conninfo[CONN_INFO_LEN], *p;

    p = flags;
    if (client->flags & CLIENT_SLAVE) {
        if (client->flags & CLIENT_MONITOR)
            *p++ = 'O';
        else
            *p++ = 'S';
    }
    if (client->flags & CLIENT_MASTER) *p++ = 'M';
    if (client->flags & CLIENT_PUBSUB) *p++ = 'P';
    if (client->flags & CLIENT_MULTI) *p++ = 'x';
    if (client->flags & CLIENT_BLOCKED) *p++ = 'b';
    if (client->flags & CLIENT_TRACKING) *p++ = 't';
    if (client->flags & CLIENT_TRACKING_BROKEN_REDIR) *p++ = 'R';
    if (client->flags & CLIENT_TRACKING_BCAST) *p++ = 'B';
    if (client->flags & CLIENT_DIRTY_CAS) *p++ = 'd';
    if (client->flags & CLIENT_CLOSE_AFTER_REPLY) *p++ = 'c';
    if (client->flags & CLIENT_UNBLOCKED) *p++ = 'u';
    if (client->flags & CLIENT_CLOSE_ASAP) *p++ = 'A';
    if (client->flags & CLIENT_UNIX_SOCKET) *p++ = 'U';
    if (client->flags & CLIENT_READONLY) *p++ = 'r';
    if (p == flags) *p++ = 'N';
    *p++ = '\0';

    p = events;
    if (client->conn) {
        if (connHasReadHandler(client->conn)) *p++ = 'r';
        if (connHasWriteHandler(client->conn)) *p++ = 'w';
    }
    *p = '\0';

    /* Compute the total memory consumed by this client. */
    size_t obufmem = getClientOutputBufferMemoryUsage(client);
    size_t total_mem = obufmem;
    total_mem += zmalloc_size(client); /* includes client->buf */
    total_mem += sdsZmallocSize(client->querybuf);
    /* For efficiency (less work keeping track of the argv memory), it doesn't include the used memory
     * i.e. unused sds space and internal fragmentation, just the string length. but this is enough to
     * spot problematic clients. */
    total_mem += client->argv_len_sum();
    if (client->argv)
        total_mem += zmalloc_size(client->argv);

    return sdscatfmt(s,
        "id=%U addr=%s laddr=%s %s name=%s age=%I idle=%I flags=%s db=%i sub=%i psub=%i multi=%i qbuf=%U qbuf-free=%U argv-mem=%U obl=%U oll=%U omem=%U tot-mem=%U events=%s cmd=%s user=%s redir=%I",
        (unsigned long long) client->id,
        getClientPeerId(client),
        getClientSockname(client),
        connGetInfo(client->conn, conninfo, sizeof(conninfo)),
        client->name ? (char*)szFromObj(client->name) : "",
        (long long)(g_pserver->unixtime - client->ctime),
        (long long)(g_pserver->unixtime - client->lastinteraction),
        flags,
        client->db->id,
        (int) dictSize(client->pubsub_channels),
        (int) listLength(client->pubsub_patterns),
        (client->flags & CLIENT_MULTI) ? client->mstate.count : -1,
        (unsigned long long) sdslen(client->querybuf),
        (unsigned long long) sdsavail(client->querybuf),
        (unsigned long long) client->argv_len_sum(),
        (unsigned long long) client->bufpos,
        (unsigned long long) listLength(client->reply),
        (unsigned long long) obufmem, /* should not include client->buf since we want to see 0 for static clients. */
        (unsigned long long) total_mem,
        events,
        client->lastcmd ? client->lastcmd->name : "NULL",
        client->user ? client->user->name : "(superuser)",
        (client->flags & CLIENT_TRACKING) ? (long long) client->client_tracking_redirection : -1);
}

sds getAllClientsInfoString(int type) {
    listNode *ln;
    listIter li;
    client *client;
    sds o = sdsnewlen(SDS_NOINIT,200*listLength(g_pserver->clients));
    sdsclear(o);
    listRewind(g_pserver->clients,&li);
    while ((ln = listNext(&li)) != NULL) {
        client = reinterpret_cast<struct client*>(listNodeValue(ln));
        std::unique_lock<decltype(client->lock)> lock(client->lock);
        if (client->flags & CLIENT_CLOSE_ASAP) continue;
        if (type != -1 && getClientType(client) != type) continue;
        o = catClientInfoString(o,client);
        o = sdscatlen(o,"\n",1);
    }
    return o;
}

/* This function implements CLIENT SETNAME, including replying to the
 * user with an error if the charset is wrong (in that case C_ERR is
 * returned). If the function succeeeded C_OK is returned, and it's up
 * to the caller to send a reply if needed.
 *
 * Setting an empty string as name has the effect of unsetting the
 * currently set name: the client will remain unnamed.
 *
 * This function is also used to implement the HELLO SETNAME option. */
int clientSetNameOrReply(client *c, robj *name) {
    int len = sdslen((sds)ptrFromObj(name));
    char *p = (char*)ptrFromObj(name);

    /* Setting the client name to an empty string actually removes
     * the current name. */
    if (len == 0) {
        if (c->name) decrRefCount(c->name);
        c->name = NULL;
        return C_OK;
    }

    /* Otherwise check if the charset is ok. We need to do this otherwise
     * CLIENT LIST format will break. You should always be able to
     * split by space to get the different fields. */
    for (int j = 0; j < len; j++) {
        if (p[j] < '!' || p[j] > '~') { /* ASCII is assumed. */
            addReplyError(c,
                "Client names cannot contain spaces, "
                "newlines or special characters.");
            return C_ERR;
        }
    }
    if (c->name) decrRefCount(c->name);
    c->name = name;
    incrRefCount(name);
    return C_OK;
}

/* Reset the client state to resemble a newly connected client.
 */
void resetCommand(client *c) {
    listNode *ln;

    /* MONITOR clients are also marked with CLIENT_SLAVE, we need to
     * distinguish between the two.
     */
    if (c->flags & CLIENT_MONITOR) {
        ln = listSearchKey(g_pserver->monitors,c);
        serverAssert(ln != NULL);
        listDelNode(g_pserver->monitors,ln);

        c->flags &= ~(CLIENT_MONITOR|CLIENT_SLAVE);
    }

    if (c->flags & (CLIENT_SLAVE|CLIENT_MASTER|CLIENT_MODULE)) {
        addReplyError(c,"can only reset normal client connections");
        return;
    }

    if (c->flags & CLIENT_TRACKING) disableTracking(c);
    selectDb(c,0);
    c->resp = 2;

    clientSetDefaultAuth(c);
    moduleNotifyUserChanged(c);
    discardTransaction(c);

    pubsubUnsubscribeAllChannels(c,0);
    pubsubUnsubscribeAllPatterns(c,0);

    if (c->name) {
        decrRefCount(c->name);
        c->name = NULL;
    }

    /* Selectively clear state flags not covered above */
    c->flags &= ~(CLIENT_ASKING|CLIENT_READONLY|CLIENT_PUBSUB|
            CLIENT_REPLY_OFF|CLIENT_REPLY_SKIP_NEXT);

    addReplyStatus(c,"RESET");
}

void clientCommand(client *c) {
    listNode *ln;
    listIter li;

    if (c->argc == 2 && !strcasecmp((const char*)ptrFromObj(c->argv[1]),"help")) {
        const char *help[] = {
"CACHING (YES|NO)",
"    Enable/disable tracking of the keys for next command in OPTIN/OPTOUT modes.",
"GETREDIR",
"    Return the client ID we are redirecting to when tracking is enabled.",
"GETNAME",
"    Return the name of the current connection.",
"ID",
"    Return the ID of the current connection.",
"INFO",
"    Return information about the current client connection.",
"KILL <ip:port>",
"    Kill connection made from <ip:port>.",
"KILL <option> <value> [<option> <value> [...]]",
"    Kill connections. Options are:",
"    * ADDR (<ip:port>|<unixsocket>:0)",
"      Kill connections made from the specified address",
"    * LADDR (<ip:port>|<unixsocket>:0)",
"      Kill connections made to specified local address",
"    * TYPE (normal|master|replica|pubsub)",
"      Kill connections by type.",
"    * USER <username>",
"      Kill connections authenticated by <username>.",
"    * SKIPME (YES|NO)",
"      Skip killing current connection (default: yes).",
"LIST [options ...]",
"    Return information about client connections. Options:",
"    * TYPE (NORMAL|MASTER|REPLICA|PUBSUB)",
"      Return clients of specified type.",
"UNPAUSE",
"    Stop the current client pause, resuming traffic.",
"PAUSE <timeout> [WRITE|ALL]",
"    Suspend all, or just write, clients for <timout> milliseconds.",
"REPLY (ON|OFF|SKIP)",
"    Control the replies sent to the current connection.",
"SETNAME <name>",
"    Assign the name <name> to the current connection.",
"UNBLOCK <clientid> [TIMEOUT|ERROR]",
"    Unblock the specified blocked client.",
"TRACKING (ON|OFF) [REDIRECT <id>] [BCAST] [PREFIX <prefix> [...]]",
"         [OPTIN] [OPTOUT]",
"    Control server assisted client side caching.",
"TRACKINGINFO",
"    Report tracking status for the current connection.",
NULL
        };
        addReplyHelp(c, help);
    } else if (!strcasecmp((const char*)ptrFromObj(c->argv[1]),"id") && c->argc == 2) {
        /* CLIENT ID */
        addReplyLongLong(c,c->id);
    } else if (!strcasecmp(szFromObj(c->argv[1]),"info") && c->argc == 2) {
        /* CLIENT INFO */
        sds o = catClientInfoString(sdsempty(), c);
        o = sdscatlen(o,"\n",1);
        addReplyVerbatim(c,o,sdslen(o),"txt");
        sdsfree(o);
    } else if (!strcasecmp(szFromObj(c->argv[1]),"list")) {
        /* CLIENT LIST */
        int type = -1;
        sds o = NULL;
        if (c->argc == 4 && !strcasecmp((const char*)ptrFromObj(c->argv[2]),"type")) {
            type = getClientTypeByName((char*)ptrFromObj(c->argv[3]));
            if (type == -1) {
                addReplyErrorFormat(c,"Unknown client type '%s'",
                    (char*) ptrFromObj(c->argv[3]));
                return;
            }
        } else if (c->argc > 3 && !strcasecmp(szFromObj(c->argv[2]),"id")) {
            int j;
            o = sdsempty();
            for (j = 3; j < c->argc; j++) {
                long long cid;
                if (getLongLongFromObjectOrReply(c, c->argv[j], &cid,
                            "Invalid client ID")) {
                    sdsfree(o);
                    return;
                }
                client *cl = lookupClientByID(cid);
                if (cl) {
                    o = catClientInfoString(o, cl);
                    o = sdscatlen(o, "\n", 1);
                }
            }
        } else if (c->argc != 2) {
            addReplyErrorObject(c,shared.syntaxerr);
            return;
        }

        if (!o)
            o = getAllClientsInfoString(type);
        addReplyVerbatim(c,o,sdslen(o),"txt");
        sdsfree(o);
    } else if (!strcasecmp((const char*)ptrFromObj(c->argv[1]),"reply") && c->argc == 3) {
        /* CLIENT REPLY ON|OFF|SKIP */
        if (!strcasecmp((const char*)ptrFromObj(c->argv[2]),"on")) {
            c->flags &= ~(CLIENT_REPLY_SKIP|CLIENT_REPLY_OFF);
            addReply(c,shared.ok);
        } else if (!strcasecmp((const char*)ptrFromObj(c->argv[2]),"off")) {
            c->flags |= CLIENT_REPLY_OFF;
        } else if (!strcasecmp((const char*)ptrFromObj(c->argv[2]),"skip")) {
            if (!(c->flags & CLIENT_REPLY_OFF))
                c->flags |= CLIENT_REPLY_SKIP_NEXT;
        } else {
            addReplyErrorObject(c,shared.syntaxerr);
            return;
        }
    } else if (!strcasecmp((const char*)ptrFromObj(c->argv[1]),"kill")) {
        /* CLIENT KILL <ip:port>
         * CLIENT KILL <option> [value] ... <option> [value] */
        char *addr = NULL;
        char *laddr = NULL;
        user *user = NULL;
        int type = -1;
        uint64_t id = 0;
        int skipme = 1;
        int killed = 0, close_this_client = 0;

        if (c->argc == 3) {
            /* Old style syntax: CLIENT KILL <addr> */
            addr = (char*)ptrFromObj(c->argv[2]);
            skipme = 0; /* With the old form, you can kill yourself. */
        } else if (c->argc > 3) {
            int i = 2; /* Next option index. */

            /* New style syntax: parse options. */
            while(i < c->argc) {
                int moreargs = c->argc > i+1;

                if (!strcasecmp((const char*)ptrFromObj(c->argv[i]),"id") && moreargs) {
                    long long tmp;

                    if (getLongLongFromObjectOrReply(c,c->argv[i+1],&tmp,NULL)
                        != C_OK) return;
                    id = tmp;
                } else if (!strcasecmp((const char*)ptrFromObj(c->argv[i]),"type") && moreargs) {
                    type = getClientTypeByName((const char*)ptrFromObj(c->argv[i+1]));
                    if (type == -1) {
                        addReplyErrorFormat(c,"Unknown client type '%s'",
                            (char*) ptrFromObj(c->argv[i+1]));
                        return;
                    }
                } else if (!strcasecmp(szFromObj(c->argv[i]),"addr") && moreargs) {
                    addr = szFromObj(c->argv[i+1]);
                } else if (!strcasecmp(szFromObj(c->argv[i]),"user") && moreargs) {
                    user = ACLGetUserByName(szFromObj(c->argv[i+1]),
                                            sdslen(szFromObj(c->argv[i+1])));
                    if (user == NULL) {
                        addReplyErrorFormat(c,"No such user '%s'",
                            szFromObj(c->argv[i+1]));
                        return;
                    }
                } else if (!strcasecmp(szFromObj(c->argv[i]),"addr") && moreargs) {
                    addr = szFromObj(c->argv[i+1]);
                } else if (!strcasecmp(szFromObj(c->argv[i]),"laddr") && moreargs) {
                    laddr = szFromObj(c->argv[i+1]);
                } else if (!strcasecmp(szFromObj(c->argv[i]),"user") && moreargs) {
                    user = ACLGetUserByName(szFromObj(c->argv[i+1]),
                                            sdslen(szFromObj(c->argv[i+1])));
                    if (user == NULL) {
                        addReplyErrorFormat(c,"No such user '%s'",
                            (char*) szFromObj(c->argv[i+1]));
                        return;
                    }
                } else if (!strcasecmp(szFromObj(c->argv[i]),"skipme") && moreargs) {
                    if (!strcasecmp(szFromObj(c->argv[i+1]),"yes")) {
                        skipme = 1;
                    } else if (!strcasecmp((const char*)ptrFromObj(c->argv[i+1]),"no")) {
                        skipme = 0;
                    } else {
                        addReplyErrorObject(c,shared.syntaxerr);
                        return;
                    }
                } else {
                    addReplyErrorObject(c,shared.syntaxerr);
                    return;
                }
                i += 2;
            }
        } else {
            addReplyErrorObject(c,shared.syntaxerr);
            return;
        }

        /* Iterate clients killing all the matching clients. */
        listRewind(g_pserver->clients,&li);
        while ((ln = listNext(&li)) != NULL) {
            client *client = (struct client*)listNodeValue(ln);
            if (addr && strcmp(getClientPeerId(client),addr) != 0) continue;
            if (laddr && strcmp(getClientSockname(client),laddr) != 0) continue;
            if (type != -1 && getClientType(client) != type) continue;
            if (id != 0 && client->id != id) continue;
            if (user && client->user != user) continue;
            if (c == client && skipme) continue;

            /* Kill it. */
            if (c == client) {
                close_this_client = 1;
            } else {
                if (FCorrectThread(client))
                {
                    freeClient(client);
                }
                else
                {
                    int iel = client->iel;
                    freeClientAsync(client);
                    aePostFunction(g_pserver->rgthreadvar[client->iel].el, [iel] {  // note: failure is OK
                        freeClientsInAsyncFreeQueue(iel);
                    });
                }
            }
            killed++;
        }

        /* Reply according to old/new format. */
        if (c->argc == 3) {
            if (killed == 0)
                addReplyError(c,"No such client");
            else
                addReply(c,shared.ok);
        } else {
            addReplyLongLong(c,killed);
        }

        /* If this client has to be closed, flag it as CLOSE_AFTER_REPLY
         * only after we queued the reply to its output buffers. */
        if (close_this_client) c->flags |= CLIENT_CLOSE_AFTER_REPLY;
    } else if (!strcasecmp((const char*)ptrFromObj(c->argv[1]),"unblock") && (c->argc == 3 ||
                                                          c->argc == 4))
    {
        /* CLIENT UNBLOCK <id> [timeout|error] */
        long long id;
        int unblock_error = 0;

        if (c->argc == 4) {
            if (!strcasecmp((const char*)ptrFromObj(c->argv[3]),"timeout")) {
                unblock_error = 0;
            } else if (!strcasecmp((const char*)ptrFromObj(c->argv[3]),"error")) {
                unblock_error = 1;
            } else {
                addReplyError(c,
                    "CLIENT UNBLOCK reason should be TIMEOUT or ERROR");
                return;
            }
        }
        if (getLongLongFromObjectOrReply(c,c->argv[2],&id,NULL)
            != C_OK) return;
        struct client *target = lookupClientByID(id);
        if (target && target->flags & CLIENT_BLOCKED && moduleBlockedClientMayTimeout(target)) {
            std::unique_lock<fastlock> ul(target->lock);
            if (unblock_error)
                addReplyError(target,
                    "-UNBLOCKED client unblocked via CLIENT UNBLOCK");
            else
                replyToBlockedClientTimedOut(target);
            unblockClient(target);
            addReply(c,shared.cone);
        } else {
            addReply(c,shared.czero);
        }
    } else if (!strcasecmp(szFromObj(c->argv[1]),"setname") && c->argc == 3) {
        /* CLIENT SETNAME */
        if (clientSetNameOrReply(c,c->argv[2]) == C_OK)
            addReply(c,shared.ok);
    } else if (!strcasecmp(szFromObj(c->argv[1]),"getname") && c->argc == 2) {
        /* CLIENT GETNAME */
        if (c->name)
            addReplyBulk(c,c->name);
        else
            addReplyNull(c);
    } else if (!strcasecmp(szFromObj(c->argv[1]),"unpause") && c->argc == 2) {
        /* CLIENT UNPAUSE */
        unpauseClients();
        addReply(c,shared.ok);
    } else if (!strcasecmp(szFromObj(c->argv[1]),"pause") && (c->argc == 3 ||
                                                        c->argc == 4))
    {
        /* CLIENT PAUSE TIMEOUT [WRITE|ALL] */
        mstime_t end;
        pause_type type = CLIENT_PAUSE_ALL;
        if (c->argc == 4) {
            if (!strcasecmp(szFromObj(c->argv[3]),"write")) {
                type = CLIENT_PAUSE_WRITE;
            } else if (!strcasecmp(szFromObj(c->argv[3]),"all")) {
                type = CLIENT_PAUSE_ALL;
            } else {
                addReplyError(c,
                    "CLIENT PAUSE mode must be WRITE or ALL");  
                return;       
            }
        }

        if (getTimeoutFromObjectOrReply(c,c->argv[2],&end,
            UNIT_MILLISECONDS) != C_OK) return;
        pauseClients(end, type);
        addReply(c,shared.ok);
    } else if (!strcasecmp(szFromObj(c->argv[1]),"tracking") && c->argc >= 3) {
        /* CLIENT TRACKING (on|off) [REDIRECT <id>] [BCAST] [PREFIX first]
         *                          [PREFIX second] [OPTIN] [OPTOUT] ... */
        long long redir = 0;
        uint64_t options = 0;
        robj **prefix = NULL;
        size_t numprefix = 0;

        /* Parse the options. */
        for (int j = 3; j < c->argc; j++) {
            int moreargs = (c->argc-1) - j;

            if (!strcasecmp(szFromObj(c->argv[j]),"redirect") && moreargs) {
                j++;
                if (redir != 0) {
                    addReplyError(c,"A client can only redirect to a single "
                                    "other client");
                    zfree(prefix);
                    return;
                }

                if (getLongLongFromObjectOrReply(c,c->argv[j],&redir,NULL) !=
                    C_OK)
                {
                    zfree(prefix);
                    return;
                }
                /* We will require the client with the specified ID to exist
                 * right now, even if it is possible that it gets disconnected
                 * later. Still a valid sanity check. */
                if (lookupClientByID(redir) == NULL) {
                    addReplyError(c,"The client ID you want redirect to "
                                    "does not exist");
                    zfree(prefix);
                    return;
                }
            } else if (!strcasecmp(szFromObj(c->argv[j]),"bcast")) {
                options |= CLIENT_TRACKING_BCAST;
            } else if (!strcasecmp(szFromObj(c->argv[j]),"optin")) {
                options |= CLIENT_TRACKING_OPTIN;
            } else if (!strcasecmp(szFromObj(c->argv[j]),"optout")) {
                options |= CLIENT_TRACKING_OPTOUT;
            } else if (!strcasecmp(szFromObj(c->argv[j]),"noloop")) {
                options |= CLIENT_TRACKING_NOLOOP;
            } else if (!strcasecmp(szFromObj(c->argv[j]),"prefix") && moreargs) {
                j++;
                prefix = (robj**)zrealloc(prefix,sizeof(robj*)*(numprefix+1), MALLOC_LOCAL);
                prefix[numprefix++] = c->argv[j];
            } else {
                zfree(prefix);
                addReplyErrorObject(c,shared.syntaxerr);
                return;
            }
        }

        /* Options are ok: enable or disable the tracking for this client. */
        if (!strcasecmp(szFromObj(c->argv[2]),"on")) {
            /* Before enabling tracking, make sure options are compatible
             * among each other and with the current state of the client. */
            if (!(options & CLIENT_TRACKING_BCAST) && numprefix) {
                addReplyError(c,
                    "PREFIX option requires BCAST mode to be enabled");
                zfree(prefix);
                return;
            }

            if (c->flags & CLIENT_TRACKING) {
                int oldbcast = !!(c->flags & CLIENT_TRACKING_BCAST);
                int newbcast = !!(options & CLIENT_TRACKING_BCAST);
                if (oldbcast != newbcast) {
                    addReplyError(c,
                    "You can't switch BCAST mode on/off before disabling "
                    "tracking for this client, and then re-enabling it with "
                    "a different mode.");
                    zfree(prefix);
                    return;
                }
            }

            if (options & CLIENT_TRACKING_BCAST &&
                options & (CLIENT_TRACKING_OPTIN|CLIENT_TRACKING_OPTOUT))
            {
                addReplyError(c,
                "OPTIN and OPTOUT are not compatible with BCAST");
                zfree(prefix);
                return;
            }

            if (options & CLIENT_TRACKING_OPTIN && options & CLIENT_TRACKING_OPTOUT)
            {
                addReplyError(c,
                "You can't specify both OPTIN mode and OPTOUT mode");
                zfree(prefix);
                return;
            }

            if ((options & CLIENT_TRACKING_OPTIN && c->flags & CLIENT_TRACKING_OPTOUT) ||
                (options & CLIENT_TRACKING_OPTOUT && c->flags & CLIENT_TRACKING_OPTIN))
            {
                addReplyError(c,
                "You can't switch OPTIN/OPTOUT mode before disabling "
                "tracking for this client, and then re-enabling it with "
                "a different mode.");
                zfree(prefix);
                return;
            }

            if (options & CLIENT_TRACKING_BCAST) {
                if (!checkPrefixCollisionsOrReply(c,prefix,numprefix)) {
                    zfree(prefix);
                    return;
                }
            }

            enableTracking(c,redir,options,prefix,numprefix);
        } else if (!strcasecmp(szFromObj(c->argv[2]),"off")) {
            disableTracking(c);
        } else {
            zfree(prefix);
            addReplyErrorObject(c,shared.syntaxerr);
            return;
        }
        zfree(prefix);
        addReply(c,shared.ok);
    } else if (!strcasecmp(szFromObj(c->argv[1]),"caching") && c->argc >= 3) {
        if (!(c->flags & CLIENT_TRACKING)) {
            addReplyError(c,"CLIENT CACHING can be called only when the "
                            "client is in tracking mode with OPTIN or "
                            "OPTOUT mode enabled");
            return;
        }

        char *opt = szFromObj(c->argv[2]);
        if (!strcasecmp(opt,"yes")) {
            if (c->flags & CLIENT_TRACKING_OPTIN) {
                c->flags |= CLIENT_TRACKING_CACHING;
            } else {
                addReplyError(c,"CLIENT CACHING YES is only valid when tracking is enabled in OPTIN mode.");
                return;
            }
        } else if (!strcasecmp(opt,"no")) {
            if (c->flags & CLIENT_TRACKING_OPTOUT) {
                c->flags |= CLIENT_TRACKING_CACHING;
            } else {
                addReplyError(c,"CLIENT CACHING NO is only valid when tracking is enabled in OPTOUT mode.");
                return;
            }
        } else {
            addReplyErrorObject(c,shared.syntaxerr);
            return;
        }

        /* Common reply for when we succeeded. */
        addReply(c,shared.ok);
    } else if (!strcasecmp(szFromObj(c->argv[1]),"caching") && c->argc >= 3) {
        if (!(c->flags & CLIENT_TRACKING)) {
            addReplyError(c,"CLIENT CACHING can be called only when the "
                            "client is in tracking mode with OPTIN or "
                            "OPTOUT mode enabled");
            return;
        }

        char *opt = szFromObj(c->argv[2]);
        if (!strcasecmp(opt,"yes")) {
            if (c->flags & CLIENT_TRACKING_OPTIN) {
                c->flags |= CLIENT_TRACKING_CACHING;
            } else {
                addReplyError(c,"CLIENT CACHING YES is only valid when tracking is enabled in OPTIN mode.");
                return;
            }
        } else if (!strcasecmp(opt,"no")) {
            if (c->flags & CLIENT_TRACKING_OPTOUT) {
                c->flags |= CLIENT_TRACKING_CACHING;
            } else {
                addReplyError(c,"CLIENT CACHING NO is only valid when tracking is enabled in OPTOUT mode.");
                return;
            }
        } else {
            addReply(c,shared.syntaxerr);
            return;
        }

        /* Common reply for when we succeeded. */
        addReply(c,shared.ok);
    } else if (!strcasecmp(szFromObj(c->argv[1]),"getredir") && c->argc == 2) {
        /* CLIENT GETREDIR */
        if (c->flags & CLIENT_TRACKING) {
            addReplyLongLong(c,c->client_tracking_redirection);
        } else {
            addReplyLongLong(c,-1);
        }
    } else if (!strcasecmp(szFromObj(c->argv[1]),"trackinginfo") && c->argc == 2) {
        addReplyMapLen(c,3);

        /* Flags */
        addReplyBulkCString(c,"flags");
        void *arraylen_ptr = addReplyDeferredLen(c);
        int numflags = 0;
        addReplyBulkCString(c,c->flags & CLIENT_TRACKING ? "on" : "off");
        numflags++;
        if (c->flags & CLIENT_TRACKING_BCAST) {
            addReplyBulkCString(c,"bcast");
            numflags++;
        }
        if (c->flags & CLIENT_TRACKING_OPTIN) {
            addReplyBulkCString(c,"optin");
            numflags++;
            if (c->flags & CLIENT_TRACKING_CACHING) {
                addReplyBulkCString(c,"caching-yes");
                numflags++;        
            }
        }
        if (c->flags & CLIENT_TRACKING_OPTOUT) {
            addReplyBulkCString(c,"optout");
            numflags++;
            if (c->flags & CLIENT_TRACKING_CACHING) {
                addReplyBulkCString(c,"caching-no");
                numflags++;        
            }
        }
        if (c->flags & CLIENT_TRACKING_NOLOOP) {
            addReplyBulkCString(c,"noloop");
            numflags++;
        }
        if (c->flags & CLIENT_TRACKING_BROKEN_REDIR) {
            addReplyBulkCString(c,"broken_redirect");
            numflags++;
        }
        setDeferredSetLen(c,arraylen_ptr,numflags);

        /* Redirect */
        addReplyBulkCString(c,"redirect");
        if (c->flags & CLIENT_TRACKING) {
            addReplyLongLong(c,c->client_tracking_redirection);
        } else {
            addReplyLongLong(c,-1);
        }

        /* Prefixes */
        addReplyBulkCString(c,"prefixes");
        if (c->client_tracking_prefixes) {
            addReplyArrayLen(c,raxSize(c->client_tracking_prefixes));
            raxIterator ri;
            raxStart(&ri,c->client_tracking_prefixes);
            raxSeek(&ri,"^",NULL,0);
            while(raxNext(&ri)) {
                addReplyBulkCBuffer(c,ri.key,ri.key_len);
            }
            raxStop(&ri);
        } else {
            addReplyArrayLen(c,0);
        }
    } else {
        addReplySubcommandSyntaxError(c);
    }
}

/* HELLO [<protocol-version> [AUTH <user> <password>] [SETNAME <name>] ] */
void helloCommand(client *c) {
    long long ver = 0;
    int next_arg = 1;

    if (c->argc >= 2) {
        if (getLongLongFromObjectOrReply(c, c->argv[next_arg++], &ver,
            "Protocol version is not an integer or out of range") != C_OK) {
            return;
        }

        if (ver < 2 || ver > 3) {
            addReplyError(c,"-NOPROTO unsupported protocol version");
            return;
        }
    }

    for (int j = next_arg; j < c->argc; j++) {
        int moreargs = (c->argc-1) - j;
        const char *opt = (const char*)ptrFromObj(c->argv[j]);
        if (!strcasecmp(opt,"AUTH") && moreargs >= 2) {
            redactClientCommandArgument(c, j+1);
            redactClientCommandArgument(c, j+2);
            if (ACLAuthenticateUser(c, c->argv[j+1], c->argv[j+2]) == C_ERR) {
                addReplyError(c,"-WRONGPASS invalid username-password pair or user is disabled.");
                return;
            }
            j += 2;
        } else if (!strcasecmp(opt,"SETNAME") && moreargs) {
            if (clientSetNameOrReply(c, c->argv[j+1]) == C_ERR) return;
            j++;
        } else {
            addReplyErrorFormat(c,"Syntax error in HELLO option '%s'",opt);
            return;
        }
    }

    /* At this point we need to be authenticated to continue. */
    if (!c->authenticated) {
        addReplyError(c,"-NOAUTH HELLO must be called with the client already "
                        "authenticated, otherwise the HELLO AUTH <user> <pass> "
                        "option can be used to authenticate the client and "
                        "select the RESP protocol version at the same time");
        return;
    }

    /* Let's switch to the specified RESP mode. */
    if (ver) c->resp = ver;
    addReplyMapLen(c,6 + !g_pserver->sentinel_mode);

    addReplyBulkCString(c,"server");
    addReplyBulkCString(c,"redis");

    addReplyBulkCString(c,"version");
    addReplyBulkCString(c,KEYDB_SET_VERSION);

    addReplyBulkCString(c,"proto");
    addReplyLongLong(c,c->resp);

    addReplyBulkCString(c,"id");
    addReplyLongLong(c,c->id);

    addReplyBulkCString(c,"mode");
    if (g_pserver->sentinel_mode) addReplyBulkCString(c,"sentinel");
    else if (g_pserver->cluster_enabled) addReplyBulkCString(c,"cluster");
    else addReplyBulkCString(c,"standalone");

    if (!g_pserver->sentinel_mode) {
        addReplyBulkCString(c,"role");
        addReplyBulkCString(c,listLength(g_pserver->masters) ? 
            g_pserver->fActiveReplica ? "active-replica" : "replica" 
            : "master");
    }

    addReplyBulkCString(c,"modules");
    addReplyLoadedModules(c);
}

/* This callback is bound to POST and "Host:" command names. Those are not
 * really commands, but are used in security attacks in order to talk to
 * Redis instances via HTTP, with a technique called "cross protocol scripting"
 * which exploits the fact that services like Redis will discard invalid
 * HTTP headers and will process what follows.
 *
 * As a protection against this attack, Redis will terminate the connection
 * when a POST or "Host:" header is seen, and will log the event from
 * time to time (to avoid creating a DOS as a result of too many logs). */
void securityWarningCommand(client *c) {
    static time_t logged_time;
    time_t now = time(NULL);

    if (llabs(now-logged_time) > 60) {
        serverLog(LL_WARNING,"Possible SECURITY ATTACK detected. It looks like somebody is sending POST or Host: commands to KeyDB. This is likely due to an attacker attempting to use Cross Protocol Scripting to compromise your KeyDB instance. Connection aborted.");
        logged_time = now;
    }
    freeClientAsync(c);
}

/* Keep track of the original command arguments so that we can generate
 * an accurate slowlog entry after the command has been executed. */
static void retainOriginalCommandVector(client *c) {
    /* We already rewrote this command, so don't rewrite it again */
    if (c->original_argv) return;
    c->original_argc = c->argc;
    c->original_argv = (robj**)zmalloc(sizeof(robj*)*(c->argc));
    for (int j = 0; j < c->argc; j++) {
        c->original_argv[j] = c->argv[j];
        incrRefCount(c->argv[j]);
    }
}

/* Redact a given argument to prevent it from being shown
 * in the slowlog. This information is stored in the
 * original_argv array. */
void redactClientCommandArgument(client *c, int argc) {
    retainOriginalCommandVector(c);
    decrRefCount(c->argv[argc]);
    c->original_argv[argc] = shared.redacted;
}

/* Rewrite the command vector of the client. All the new objects ref count
 * is incremented. The old command vector is freed, and the old objects
 * ref count is decremented. */
void rewriteClientCommandVector(client *c, int argc, ...) {
    va_list ap;
    int j;
    robj **argv; /* The new argument vector */

    argv = (robj**)zmalloc(sizeof(robj*)*argc, MALLOC_LOCAL);
    va_start(ap,argc);
    for (j = 0; j < argc; j++) {
        robj *a;

        a = va_arg(ap, robj*);
        argv[j] = a;
        incrRefCount(a);
    }
    replaceClientCommandVector(c, argc, argv);
    va_end(ap);
}

/* Completely replace the client command vector with the provided one. */
void replaceClientCommandVector(client *c, int argc, robj **argv) {
    int j;
    retainOriginalCommandVector(c);
    freeClientArgv(c);
    zfree(c->argv);
    c->argv = argv;
    c->argc = argc;
    c->argv_len_sumActive = 0;
    for (j = 0; j < c->argc; j++)
        if (c->argv[j])
            c->argv_len_sumActive += getStringObjectLen(c->argv[j]);
    c->cmd = lookupCommandOrOriginal((sds)ptrFromObj(c->argv[0]));
    serverAssertWithInfo(c,NULL,c->cmd != NULL);
}

/* Rewrite a single item in the command vector.
 * The new val ref count is incremented, and the old decremented.
 *
 * It is possible to specify an argument over the current size of the
 * argument vector: in this case the array of objects gets reallocated
 * and c->argc set to the max value. However it's up to the caller to
 *
 * 1. Make sure there are no "holes" and all the arguments are set.
 * 2. If the original argument vector was longer than the one we
 *    want to end with, it's up to the caller to set c->argc and
 *    free the no longer used objects on c->argv. */
void rewriteClientCommandArgument(client *c, int i, robj *newval) {
    robj *oldval;
    retainOriginalCommandVector(c);
    if (i >= c->argc) {
        c->argv = (robj**)zrealloc(c->argv,sizeof(robj*)*(i+1), MALLOC_LOCAL);
        c->argc = i+1;
        c->argv[i] = NULL;
    }
    oldval = c->argv[i];
    if (oldval) c->argv_len_sumActive -= getStringObjectLen(oldval);
    if (newval) c->argv_len_sumActive += getStringObjectLen(newval);
    c->argv[i] = newval;
    incrRefCount(newval);
    if (oldval) decrRefCount(oldval);

    /* If this is the command name make sure to fix c->cmd. */
    if (i == 0) {
        c->cmd = lookupCommandOrOriginal((sds)ptrFromObj(c->argv[0]));
        serverAssertWithInfo(c,NULL,c->cmd != NULL);
    }
}

/* In the case of a replica client, writes to said replica are using data from the replication backlog
 * as opposed to it's own internal buffer, this number should keep track of that */
unsigned long getClientReplicationBacklogSharedUsage(client *c) {
    return (!(c->flags & CLIENT_SLAVE) || !c->FPendingReplicaWrite() ) ? 0 : g_pserver->master_repl_offset - c->repl_curr_off;
}

/* This function returns the number of bytes that Redis is
 * using to store the reply still not read by the client.
 *
 * Note: this function is very fast so can be called as many time as
 * the caller wishes. The main usage of this function currently is
 * enforcing the client output length limits. */
unsigned long getClientOutputBufferMemoryUsage(client *c) {
    unsigned long list_item_size = sizeof(listNode) + sizeof(clientReplyBlock);
    return c->reply_bytes + (list_item_size*listLength(c->reply)) + (c->replyAsync ? c->replyAsync->size : 0) + getClientReplicationBacklogSharedUsage(c);
}



/* Get the class of a client, used in order to enforce limits to different
 * classes of clients.
 *
 * The function will return one of the following:
 * CLIENT_TYPE_NORMAL -> Normal client
 * CLIENT_TYPE_SLAVE  -> Slave
 * CLIENT_TYPE_PUBSUB -> Client subscribed to Pub/Sub channels
 * CLIENT_TYPE_MASTER -> The client representing our replication master.
 */
int getClientType(client *c) {
    if (c->flags & CLIENT_MASTER) return CLIENT_TYPE_MASTER;
    /* Even though MONITOR clients are marked as replicas, we
     * want the expose them as normal clients. */
    if ((c->flags & CLIENT_SLAVE) && !(c->flags & CLIENT_MONITOR))
        return CLIENT_TYPE_SLAVE;
    if (c->flags & CLIENT_PUBSUB) return CLIENT_TYPE_PUBSUB;
    return CLIENT_TYPE_NORMAL;
}

int getClientTypeByName(const char *name) {
    if (!strcasecmp(name,"normal")) return CLIENT_TYPE_NORMAL;
    else if (!strcasecmp(name,"slave")) return CLIENT_TYPE_SLAVE;
    else if (!strcasecmp(name,"replica")) return CLIENT_TYPE_SLAVE;
    else if (!strcasecmp(name,"pubsub")) return CLIENT_TYPE_PUBSUB;
    else if (!strcasecmp(name,"master")) return CLIENT_TYPE_MASTER;
    else return -1;
}

const char *getClientTypeName(int clientType) {
    switch(clientType) {
    case CLIENT_TYPE_NORMAL: return "normal";
    case CLIENT_TYPE_SLAVE:  return "slave";
    case CLIENT_TYPE_PUBSUB: return "pubsub";
    case CLIENT_TYPE_MASTER: return "master";
    default:                       return NULL;
    }
}

/* The function checks if the client reached output buffer soft or hard
 * limit, and also update the state needed to check the soft limit as
 * a side effect.
 *
 * Return value: non-zero if the client reached the soft or the hard limit.
 *               Otherwise zero is returned. */
int checkClientOutputBufferLimits(client *c) {
    int soft = 0, hard = 0;
    unsigned long used_mem = getClientOutputBufferMemoryUsage(c);

    int clientType = getClientType(c);
    /* For the purpose of output buffer limiting, masters are handled
     * like normal clients. */
    if (clientType == CLIENT_TYPE_MASTER) clientType = CLIENT_TYPE_NORMAL;

    if (cserver.client_obuf_limits[clientType].hard_limit_bytes &&
        used_mem >= cserver.client_obuf_limits[clientType].hard_limit_bytes)
        hard = 1;
    if (cserver.client_obuf_limits[clientType].soft_limit_bytes &&
        used_mem >= cserver.client_obuf_limits[clientType].soft_limit_bytes)
        soft = 1;

    /* We need to check if the soft limit is reached continuously for the
     * specified amount of seconds. */
    if (soft) {
        if (c->obuf_soft_limit_reached_time == 0) {
            c->obuf_soft_limit_reached_time = g_pserver->unixtime;
            soft = 0; /* First time we see the soft limit reached */
        } else {
            time_t elapsed = g_pserver->unixtime - c->obuf_soft_limit_reached_time;

            if (elapsed <=
                cserver.client_obuf_limits[clientType].soft_limit_seconds) {
                soft = 0; /* The client still did not reached the max number of
                             seconds for the soft limit to be considered
                             reached. */
            }
        }
    } else {
        c->obuf_soft_limit_reached_time = 0;
    }
    return soft || hard;
}

/* Asynchronously close a client if soft or hard limit is reached on the
 * output buffer size. The caller can check if the client will be closed
 * checking if the client CLIENT_CLOSE_ASAP flag is set.
 *
 * Note: we need to close the client asynchronously because this function is
 * called from contexts where the client can't be freed safely, i.e. from the
 * lower level functions pushing data inside the client output buffers.
 * When `async` is set to 0, we close the client immediately, this is
 * useful when called from cron.
 *
 * Returns 1 if client was (flagged) closed. */
int closeClientOnOutputBufferLimitReached(client *c, int async) {
    if (!c->conn) return 0; /* It is unsafe to free fake clients. */
    serverAssert(c->reply_bytes < SIZE_MAX-(1024*64));
    if (c->reply_bytes == 0 || c->flags & CLIENT_CLOSE_ASAP) return 0;
    if (checkClientOutputBufferLimits(c) && c->replstate != SLAVE_STATE_FASTSYNC_TX) {
        sds client = catClientInfoString(sdsempty(),c);

        if (async) {
            freeClientAsync(c);
            serverLog(LL_WARNING,
                      "Client %s scheduled to be closed ASAP for overcoming of output buffer limits.",
                      client);
        } else {
            freeClient(c);
            serverLog(LL_WARNING,
                      "Client %s closed for overcoming of output buffer limits.",
                      client);
        }
        sdsfree(client);
        return  1;
    }
    return 0;
}

/* Helper function used by performEvictions() in order to flush slaves
 * output buffers without returning control to the event loop.
 * This is also called by SHUTDOWN for a best-effort attempt to send
 * slaves the latest writes. */
void flushSlavesOutputBuffers(void) {
    serverAssert(GlobalLocksAcquired());
    listIter li;
    listNode *ln;

    flushReplBacklogToClients();

    listRewind(g_pserver->slaves,&li);
    while((ln = listNext(&li))) {
        client *replica = (client*)listNodeValue(ln);

        if (!FCorrectThread(replica))
            continue;   // we cannot synchronously flush other thread's clients

        int can_receive_writes = connHasWriteHandler(replica->conn) ||
                                 (replica->flags & CLIENT_PENDING_WRITE);

        /* We don't want to send the pending data to the replica in a few
         * cases:
         *
         * 1. For some reason there is neither the write handler installed
         *    nor the client is flagged as to have pending writes: for some
         *    reason this replica may not be set to receive data. This is
         *    just for the sake of defensive programming.
         *
         * 2. The put_online_on_ack flag is true. To know why we don't want
         *    to send data to the replica in this case, please grep for the
         *    flag for this flag.
         *
         * 3. Obviously if the slave is not ONLINE.
         */
        if (replica->replstate == SLAVE_STATE_ONLINE &&
            can_receive_writes &&
            !replica->repl_put_online_on_ack &&
            clientHasPendingReplies(replica))
        {
            writeToClient(replica,0);
        }
    }
}

/* Pause clients up to the specified unixtime (in ms) for a given type of
 * commands.
 *
 * A main use case of this function is to allow pausing replication traffic
 * so that a failover without data loss to occur. Replicas will continue to receive
 * traffic to faciliate this functionality.
 * 
 * This function is also internally used by Redis Cluster for the manual
 * failover procedure implemented by CLUSTER FAILOVER.
 *
 * The function always succeed, even if there is already a pause in progress.
 * In such a case, the duration is set to the maximum and new end time and the
 * type is set to the more restrictive type of pause. */
void pauseClients(mstime_t end, pause_type type) {
    if (type > g_pserver->client_pause_type) {
        g_pserver->client_pause_type = type;
    }

    if (end > g_pserver->client_pause_end_time) {
        g_pserver->client_pause_end_time = end;
    }

    /* We allow write commands that were queued
     * up before and after to execute. We need
     * to track this state so that we don't assert
     * in propagate(). */
    if (serverTL->in_exec) {
        g_pserver->client_pause_in_transaction = 1;
    }
}

/* Unpause clients and queue them for reprocessing. */
void unpauseClients(void) {
    serverAssert(GlobalLocksAcquired());
    listNode *ln;
    listIter li;
    client *c;
    
    g_pserver->client_pause_type = CLIENT_PAUSE_OFF;
    g_pserver->client_pause_end_time = 0;

    /* Unblock all of the clients so they are reprocessed. */
    listRewind(g_pserver->paused_clients,&li);
    while ((ln = listNext(&li)) != NULL) {
        c = (client*)listNodeValue(ln);
        std::unique_lock<fastlock> ul(c->lock);
        unblockClient(c);
    }
}

/* Returns true if clients are paused and false otherwise. */ 
int areClientsPaused(void) {
    return g_pserver->client_pause_type != CLIENT_PAUSE_OFF;
}

/* Checks if the current client pause has elapsed and unpause clients
 * if it has. Also returns true if clients are now paused and false 
 * otherwise. */
int checkClientPauseTimeoutAndReturnIfPaused(void) {
    if (!areClientsPaused())
        return 0;
    if (g_pserver->client_pause_end_time < g_pserver->mstime) {
        unpauseClients();
    }
    return areClientsPaused();
}

/* This function is called by Redis in order to process a few events from
 * time to time while blocked into some not interruptible operation.
 * This allows to reply to clients with the -LOADING error while loading the
 * data set at startup or after a full resynchronization with the master
 * and so forth.
 *
 * It calls the event loop in order to process a few events. Specifically we
 * try to call the event loop 4 times as long as we receive acknowledge that
 * some event was processed, in order to go forward with the accept, read,
 * write, close sequence needed to serve a client.
 *
 * The function returns the total number of events processed. */
void processEventsWhileBlocked(int iel) {

    int eventsCount = 0;
    executeWithoutGlobalLock([&](){
        int iterations = 4; /* See the function top-comment. */
        try
        {
            ProcessingEventsWhileBlocked = 1;
            while (iterations--) {
                long long startval = g_pserver->events_processed_while_blocked;
                long long ae_events = aeProcessEvents(g_pserver->rgthreadvar[iel].el,
                    AE_FILE_EVENTS|AE_DONT_WAIT|
                    AE_CALL_BEFORE_SLEEP|AE_CALL_AFTER_SLEEP);
                /* Note that g_pserver->events_processed_while_blocked will also get
                * incremeted by callbacks called by the event loop handlers. */
                eventsCount += ae_events;
                long long events = eventsCount - startval;
                if (!events) break;
            }
            ProcessingEventsWhileBlocked = 0;
        }
        catch (...)
        {
            ProcessingEventsWhileBlocked = 0;
            throw;
        }
    });

    // Try to complete any async rehashes (this would normally happen in dbCron, but that won't run here)
    for (int idb = 0; idb < cserver.dbnum; ++idb) {
        redisDb *db = g_pserver->db[idb];
        while (db->dictUnsafeKeyOnly()->asyncdata != nullptr) {
            if (!db->dictUnsafeKeyOnly()->asyncdata->done)
                break;
            dictCompleteRehashAsync(db->dictUnsafeKeyOnly()->asyncdata, false /*fFree*/);
        }
    }
    g_pserver->events_processed_while_blocked += eventsCount;

    whileBlockedCron();

    // If a different thread processed the shutdown we need to abort the lua command or we will hang
    if (serverTL->el->stop)
        throw ShutdownException();
}

