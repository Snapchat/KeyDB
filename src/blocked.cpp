/* blocked.c - generic support for blocking operations like BLPOP & WAIT.
 *
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
 *
 * ---------------------------------------------------------------------------
 *
 * API:
 *
 * blockClient() set the CLIENT_BLOCKED flag in the client, and set the
 * specified block type 'btype' filed to one of BLOCKED_* macros.
 *
 * unblockClient() unblocks the client doing the following:
 * 1) It calls the btype-specific function to cleanup the state.
 * 2) It unblocks the client by unsetting the CLIENT_BLOCKED flag.
 * 3) It puts the client into a list of just unblocked clients that are
 *    processed ASAP in the beforeSleep() event loop callback, so that
 *    if there is some query buffer to process, we do it. This is also
 *    required because otherwise there is no 'readable' event fired, we
 *    already read the pending commands. We also set the CLIENT_UNBLOCKED
 *    flag to remember the client is in the unblocked_clients list.
 *
 * processUnblockedClients() is called inside the beforeSleep() function
 * to process the query buffer from unblocked clients and remove the clients
 * from the blocked_clients queue.
 *
 * replyToBlockedClientTimedOut() is called by the cron function when
 * a client blocked reaches the specified timeout (if the timeout is set
 * to 0, no timeout is processed).
 * It usually just needs to send a reply to the client.
 *
 * When implementing a new type of blocking operation, the implementation
 * should modify unblockClient() and replyToBlockedClientTimedOut() in order
 * to handle the btype-specific behavior of this two functions.
 * If the blocking operation waits for certain keys to change state, the
 * clusterRedirectBlockedClientIfNeeded() function should also be updated.
 */

#include "server.h"
#include "slowlog.h"
#include "latency.h"
#include "monotonic.h"
#include <mutex>

int serveClientBlockedOnList(client *receiver, robj *key, robj *dstkey, redisDb *db, robj *value, int wherefrom, int whereto);
int getListPositionFromObjectOrReply(client *c, robj *arg, int *position);

/* This structure represents the blocked key information that we store
 * in the client structure. Each client blocked on keys, has a
 * client->bpop.keys hash table. The keys of the hash table are Redis
 * keys pointers to 'robj' structures. The value is this structure.
 * The structure has two goals: firstly we store the list node that this
 * client uses to be listed in the database "blocked clients for this key"
 * list, so we can later unblock in O(1) without a list scan.
 * Secondly for certain blocking types, we have additional info. Right now
 * the only use for additional info we have is when clients are blocked
 * on streams, as we have to remember the ID it blocked for. */
typedef struct bkinfo {
    listNode *listnode;     /* List node for db->blocking_keys[key] list. */
    streamID stream_id;     /* Stream ID if we blocked in a stream. */
} bkinfo;

/* Block a client for the specific operation type. Once the CLIENT_BLOCKED
 * flag is set client query buffer is not longer processed, but accumulated,
 * and will be processed when the client is unblocked. */
void blockClient(client *c, int btype) {
    serverAssert(GlobalLocksAcquired());
    /* Master client should never be blocked unless pause or module */
    serverAssert(!(c->flags & CLIENT_MASTER &&
                   btype != BLOCKED_MODULE &&
                   btype != BLOCKED_PAUSE));

    c->flags |= CLIENT_BLOCKED;
    c->btype = btype;
    if (btype == BLOCKED_ASYNC)
        c->casyncOpsPending++;
    g_pserver->blocked_clients++;
    g_pserver->blocked_clients_by_type[btype]++;
    addClientToTimeoutTable(c);
    if (btype == BLOCKED_PAUSE) {
        listAddNodeTail(g_pserver->paused_clients, c);
        c->paused_list_node = listLast(g_pserver->paused_clients);
        /* Mark this client to execute its command */
        c->flags |= CLIENT_PENDING_COMMAND;
    }
}

/* This function is called after a client has finished a blocking operation
 * in order to update the total command duration, log the command into
 * the Slow log if needed, and log the reply duration event if needed. */
void updateStatsOnUnblock(client *c, long blocked_us, long reply_us){
    const ustime_t total_cmd_duration = c->duration + blocked_us + reply_us;
    c->lastcmd->microseconds += total_cmd_duration;

    /* Log the command into the Slow log if needed. */
    slowlogPushCurrentCommand(c, c->lastcmd, total_cmd_duration);
    /* Log the reply duration event. */
    latencyAddSampleIfNeeded("command-unblocking",reply_us/1000);
}

/* This function is called in the beforeSleep() function of the event loop
 * in order to process the pending input buffer of clients that were
 * unblocked after a blocking operation. */
void processUnblockedClients(int iel) {
    serverAssert(GlobalLocksAcquired());

    listNode *ln;
    client *c;
    list *unblocked_clients = g_pserver->rgthreadvar[iel].unblocked_clients;
    serverAssert(iel == (serverTL - g_pserver->rgthreadvar));

    while (listLength(unblocked_clients)) {
        ln = listFirst(unblocked_clients);
        serverAssert(ln != NULL);
        c = (client*)ln->value;
        listDelNode(unblocked_clients,ln);
        AssertCorrectThread(c);
        
        std::unique_lock<fastlock> ul(c->lock);
        c->flags &= ~CLIENT_UNBLOCKED;

        /* Process remaining data in the input buffer, unless the client
         * is blocked again. Actually processInputBuffer() checks that the
         * client is not blocked before to proceed, but things may change and
         * the code is conceptually more correct this way. */
        if (!(c->flags & CLIENT_BLOCKED)) {
            /* If we have a queued command, execute it now. */
            if (processPendingCommandsAndResetClient(c, CMD_CALL_FULL) == C_ERR) {
                continue;
            }
            /* Then process client if it has more data in it's buffer. */
            if (c->querybuf && sdslen(c->querybuf) > 0) {
                processInputBuffer(c, true /*fParse*/, CMD_CALL_FULL);
            }
        }
    }
}

/* This function will schedule the client for reprocessing at a safe time.
 *
 * This is useful when a client was blocked for some reason (blocking operation,
 * CLIENT PAUSE, or whatever), because it may end with some accumulated query
 * buffer that needs to be processed ASAP:
 *
 * 1. When a client is blocked, its readable handler is still active.
 * 2. However in this case it only gets data into the query buffer, but the
 *    query is not parsed or executed once there is enough to proceed as
 *    usually (because the client is blocked... so we can't execute commands).
 * 3. When the client is unblocked, without this function, the client would
 *    have to write some query in order for the readable handler to finally
 *    call processQueryBuffer*() on it.
 * 4. With this function instead we can put the client in a queue that will
 *    process it for queries ready to be executed at a safe time.
 */
void queueClientForReprocessing(client *c) {
    /* The client may already be into the unblocked list because of a previous
     * blocking operation, don't add back it into the list multiple times. */
    serverAssert(GlobalLocksAcquired());
    std::unique_lock<fastlock> ul(c->lock);
    if (!(c->flags & CLIENT_UNBLOCKED)) {
        c->flags |= CLIENT_UNBLOCKED;
        listAddNodeTail(g_pserver->rgthreadvar[c->iel].unblocked_clients,c);
    }
}

/* Unblock a client calling the right function depending on the kind
 * of operation the client is blocking for. */
void unblockClient(client *c) {
    serverAssert(GlobalLocksAcquired());
    serverAssert(c->lock.fOwnLock());
    if (c->btype == BLOCKED_LIST ||
        c->btype == BLOCKED_ZSET ||
        c->btype == BLOCKED_STREAM) {
        unblockClientWaitingData(c);
    } else if (c->btype == BLOCKED_WAIT) {
        unblockClientWaitingReplicas(c);
    } else if (c->btype == BLOCKED_MODULE) {
        if (moduleClientIsBlockedOnKeys(c)) unblockClientWaitingData(c);
        unblockClientFromModule(c);
    } else if (c->btype == BLOCKED_ASYNC) {
        serverAssert(c->casyncOpsPending > 0);
        c->casyncOpsPending--;
    } else if (c->btype == BLOCKED_PAUSE) {
        listDelNode(g_pserver->paused_clients,c->paused_list_node);
        c->paused_list_node = NULL;
    } else {
        serverPanic("Unknown btype in unblockClient().");
    }

    /* Reset the client for a new query since, for blocking commands
     * we do not do it immediately after the command returns (when the
     * client got blocked) in order to be still able to access the argument
     * vector from module callbacks and updateStatsOnUnblock. */
    if (c->btype != BLOCKED_PAUSE) {
        freeClientOriginalArgv(c);
        resetClient(c);
    }

    /* Clear the flags, and put the client in the unblocked list so that
     * we'll process new commands in its query buffer ASAP. */
    g_pserver->blocked_clients--;
    g_pserver->blocked_clients_by_type[c->btype]--;
    c->flags &= ~CLIENT_BLOCKED;
    c->btype = BLOCKED_NONE;
    removeClientFromTimeoutTable(c);
    queueClientForReprocessing(c);
}

/* This function gets called when a blocked client timed out in order to
 * send it a reply of some kind. After this function is called,
 * unblockClient() will be called with the same client as argument. */
void replyToBlockedClientTimedOut(client *c) {
    if (c->btype == BLOCKED_LIST ||
        c->btype == BLOCKED_ZSET ||
        c->btype == BLOCKED_STREAM) {
        addReplyNullArray(c);
    } else if (c->btype == BLOCKED_WAIT) {
        addReplyLongLong(c,replicationCountAcksByOffset(c->bpop.reploffset));
    } else if (c->btype == BLOCKED_MODULE) {
        moduleBlockedClientTimedOut(c);
    } else {
        serverPanic("Unknown btype in replyToBlockedClientTimedOut().");
    }
}

/* Mass-unblock clients because something changed in the instance that makes
 * blocking no longer safe. For example clients blocked in list operations
 * in an instance which turns from master to replica is unsafe, so this function
 * is called when a master turns into a replica.
 *
 * The semantics is to send an -UNBLOCKED error to the client, disconnecting
 * it at the same time. */
void disconnectAllBlockedClients(void) {
    serverAssert(GlobalLocksAcquired());
    listNode *ln;
    listIter li;

    listRewind(g_pserver->clients,&li);
    while((ln = listNext(&li))) {
        client *c = (client*)listNodeValue(ln);
        
        std::unique_lock<fastlock> ul(c->lock);
        if (c->flags & CLIENT_BLOCKED) {
            /* PAUSED clients are an exception, when they'll be unblocked, the
             * command processing will start from scratch, and the command will
             * be either executed or rejected. (unlike LIST blocked clients for
             * which the command is already in progress in a way. */
            if (c->btype == BLOCKED_PAUSE)
                continue;

            addReplyError(c,
                "-UNBLOCKED force unblock from blocking operation, "
                "instance state changed (master -> replica?)");
            unblockClient(c);
            c->flags |= CLIENT_CLOSE_AFTER_REPLY;
        }
    }
}

/* Helper function for handleClientsBlockedOnKeys(). This function is called
 * when there may be clients blocked on a list key, and there may be new
 * data to fetch (the key is ready). */
void serveClientsBlockedOnListKey(robj *o, readyList *rl) {
    /* We serve clients in the same order they blocked for
     * this key, from the first blocked to the last. */
    dictEntry *de = dictFind(rl->db->blocking_keys,rl->key);
    if (de) {
        list *clients = (list*)dictGetVal(de);
        int numclients = listLength(clients);

        while(numclients--) {
            listNode *clientnode = listFirst(clients);
            client *receiver = (client*)clientnode->value;
            std::unique_lock<decltype(receiver->lock)> lock(receiver->lock);

            if (receiver->btype != BLOCKED_LIST) {
                /* Put at the tail, so that at the next call
                 * we'll not run into it again. */
                listRotateHeadToTail(clients);
                continue;
            }

            robj *dstkey = receiver->bpop.target;
            int wherefrom = receiver->bpop.listpos.wherefrom;
            int whereto = receiver->bpop.listpos.whereto;
            robj *value = listTypePop(o, wherefrom);

            if (value) {
                /* Protect receiver->bpop.target, that will be
                 * freed by the next unblockClient()
                 * call. */
                if (dstkey) incrRefCount(dstkey);

                monotime replyTimer;
                elapsedStart(&replyTimer);
                if (serveClientBlockedOnList(receiver,
                    rl->key,dstkey,rl->db,value,
                    wherefrom, whereto) == C_ERR)
                {
                    /* If we failed serving the client we need
                     * to also undo the POP operation. */
                    listTypePush(o,value,wherefrom);
                }
                updateStatsOnUnblock(receiver, 0, elapsedUs(replyTimer));
                unblockClient(receiver);

                if (dstkey) decrRefCount(dstkey);
                decrRefCount(value);
            } else {
                break;
            }
        }
    }

    if (listTypeLength(o) == 0) {
        dbDelete(rl->db,rl->key);
        notifyKeyspaceEvent(NOTIFY_GENERIC,"del",rl->key,rl->db->id);
    }
    /* We don't call signalModifiedKey() as it was already called
     * when an element was pushed on the list. */
}

/* Helper function for handleClientsBlockedOnKeys(). This function is called
 * when there may be clients blocked on a sorted set key, and there may be new
 * data to fetch (the key is ready). */
void serveClientsBlockedOnSortedSetKey(robj *o, readyList *rl) {
    /* We serve clients in the same order they blocked for
     * this key, from the first blocked to the last. */
    dictEntry *de = dictFind(rl->db->blocking_keys,rl->key);
    if (de) {
        list *clients = (list*)dictGetVal(de);
        int numclients = listLength(clients);
        unsigned long zcard = zsetLength(o);

        while(numclients-- && zcard) {
            listNode *clientnode = listFirst(clients);
            client *receiver = (client*)clientnode->value;
            std::unique_lock<decltype(receiver->lock)> lock(receiver->lock);

            if (receiver->btype != BLOCKED_ZSET) {
                /* Put at the tail, so that at the next call
                 * we'll not run into it again. */
                listRotateHeadToTail(clients);
                continue;
            }

            int where = (receiver->lastcmd &&
                         receiver->lastcmd->proc == bzpopminCommand)
                         ? ZSET_MIN : ZSET_MAX;
            monotime replyTimer;
            elapsedStart(&replyTimer);
            genericZpopCommand(receiver,&rl->key,1,where,1,NULL);
            updateStatsOnUnblock(receiver, 0, elapsedUs(replyTimer));
            unblockClient(receiver);
            zcard--;

            /* Replicate the command. */
            robj *argv[2];
            struct redisCommand *cmd = where == ZSET_MIN ?
                                       cserver.zpopminCommand :
                                       cserver.zpopmaxCommand;
            argv[0] = createStringObject(cmd->name,strlen(cmd->name));
            argv[1] = rl->key;
            incrRefCount(rl->key);
            propagate(cmd,receiver->db->id,
                      argv,2,PROPAGATE_AOF|PROPAGATE_REPL);
            decrRefCount(argv[0]);
            decrRefCount(argv[1]);
        }
    }
}

/* Helper function for handleClientsBlockedOnKeys(). This function is called
 * when there may be clients blocked on a stream key, and there may be new
 * data to fetch (the key is ready). */
void serveClientsBlockedOnStreamKey(robj *o, readyList *rl) {
    dictEntry *de = dictFind(rl->db->blocking_keys,rl->key);
    stream *s = (stream*)ptrFromObj(o);

    /* We need to provide the new data arrived on the stream
     * to all the clients that are waiting for an offset smaller
     * than the current top item. */
    if (de) {
        list *clients = (list*)dictGetVal(de);
        listNode *ln;
        listIter li;
        listRewind(clients,&li);

        while((ln = listNext(&li))) {
            client *receiver = (client*)listNodeValue(ln);
            if (receiver->btype != BLOCKED_STREAM) continue;
            std::unique_lock<decltype(receiver->lock)> lock(receiver->lock);
            bkinfo *bki = (bkinfo*)dictFetchValue(receiver->bpop.keys,rl->key);
            streamID *gt = &bki->stream_id;

            /* If we blocked in the context of a consumer
             * group, we need to resolve the group and update the
             * last ID the client is blocked for: this is needed
             * because serving other clients in the same consumer
             * group will alter the "last ID" of the consumer
             * group, and clients blocked in a consumer group are
             * always blocked for the ">" ID: we need to deliver
             * only new messages and avoid unblocking the client
             * otherwise. */
            streamCG *group = NULL;
            if (receiver->bpop.xread_group) {
                group = streamLookupCG(s,
                        szFromObj(receiver->bpop.xread_group));
                /* If the group was not found, send an error
                 * to the consumer. */
                if (!group) {
                    addReplyError(receiver,
                        "-NOGROUP the consumer group this client "
                        "was blocked on no longer exists");
                    unblockClient(receiver);
                    continue;
                } else {
                    *gt = group->last_id;
                }
            }

            if (streamCompareID(&s->last_id, gt) > 0) {
                streamID start = *gt;
                streamIncrID(&start);

                /* Lookup the consumer for the group, if any. */
                streamConsumer *consumer = NULL;
                int noack = 0;

                if (group) {
                    int created = 0;
                    consumer =
                        streamLookupConsumer(group,
                                             szFromObj(receiver->bpop.xread_consumer),
                                             SLC_NONE,
                                             &created);
                    noack = receiver->bpop.xread_group_noack;
                    if (created && noack) {
                        streamPropagateConsumerCreation(receiver,rl->key,
                                                        receiver->bpop.xread_group,
                                                        consumer->name);
                    }
                }

                monotime replyTimer;
                elapsedStart(&replyTimer);
                /* Emit the two elements sub-array consisting of
                 * the name of the stream and the data we
                 * extracted from it. Wrapped in a single-item
                 * array, since we have just one key. */
                if (receiver->resp == 2) {
                    addReplyArrayLen(receiver,1);
                    addReplyArrayLen(receiver,2);
                } else {
                    addReplyMapLen(receiver,1);
                }
                addReplyBulk(receiver,rl->key);

                streamPropInfo pi = {
                    rl->key,
                    receiver->bpop.xread_group
                };
                streamReplyWithRange(receiver,s,&start,NULL,
                                     receiver->bpop.xread_count,
                                     0, group, consumer, noack, &pi);
                updateStatsOnUnblock(receiver, 0, elapsedUs(replyTimer));

                /* Note that after we unblock the client, 'gt'
                 * and other receiver->bpop stuff are no longer
                 * valid, so we must do the setup above before
                 * this call. */
                unblockClient(receiver);
            }
        }
    }
}

/* Helper function for handleClientsBlockedOnKeys(). This function is called
 * in order to check if we can serve clients blocked by modules using
 * RM_BlockClientOnKeys(), when the corresponding key was signaled as ready:
 * our goal here is to call the RedisModuleBlockedClient reply() callback to
 * see if the key is really able to serve the client, and in that case,
 * unblock it. */
void serveClientsBlockedOnKeyByModule(readyList *rl) {
    dictEntry *de;

    /* Optimization: If no clients are in type BLOCKED_MODULE,
     * we can skip this loop. */
    if (!g_pserver->blocked_clients_by_type[BLOCKED_MODULE]) return;

    /* We serve clients in the same order they blocked for
     * this key, from the first blocked to the last. */
    de = dictFind(rl->db->blocking_keys,rl->key);
    if (de) {
        list *clients = (list*)dictGetVal(de);
        int numclients = listLength(clients);

        while(numclients--) {
            listNode *clientnode = listFirst(clients);
            client *receiver = (client*)clientnode->value;

            /* Put at the tail, so that at the next call
             * we'll not run into it again: clients here may not be
             * ready to be served, so they'll remain in the list
             * sometimes. We want also be able to skip clients that are
             * not blocked for the MODULE type safely. */
            listRotateHeadToTail(clients);

            if (receiver->btype != BLOCKED_MODULE) continue;

            /* Note that if *this* client cannot be served by this key,
             * it does not mean that another client that is next into the
             * list cannot be served as well: they may be blocked by
             * different modules with different triggers to consider if a key
             * is ready or not. This means we can't exit the loop but need
             * to continue after the first failure. */
            monotime replyTimer;
            elapsedStart(&replyTimer);
            if (!moduleTryServeClientBlockedOnKey(receiver, rl->key)) continue;
            updateStatsOnUnblock(receiver, 0, elapsedUs(replyTimer));

            moduleUnblockClient(receiver);
        }
    }
}

/* This function should be called by Redis every time a single command,
 * a MULTI/EXEC block, or a Lua script, terminated its execution after
 * being called by a client. It handles serving clients blocked in
 * lists, streams, and sorted sets, via a blocking commands.
 *
 * All the keys with at least one client blocked that received at least
 * one new element via some write operation are accumulated into
 * the g_pserver->ready_keys list. This function will run the list and will
 * serve clients accordingly. Note that the function will iterate again and
 * again as a result of serving BLMOVE we can have new blocking clients
 * to serve because of the PUSH side of BLMOVE.
 *
 * This function is normally "fair", that is, it will server clients
 * using a FIFO behavior. However this fairness is violated in certain
 * edge cases, that is, when we have clients blocked at the same time
 * in a sorted set and in a list, for the same key (a very odd thing to
 * do client side, indeed!). Because mismatching clients (blocking for
 * a different type compared to the current key type) are moved in the
 * other side of the linked list. However as long as the key starts to
 * be used only for a single type, like virtually any Redis application will
 * do, the function is already fair. */
void handleClientsBlockedOnKeys(void) {
    serverAssert(GlobalLocksAcquired());
    while(listLength(g_pserver->ready_keys) != 0) {
        list *l;

        /* Point g_pserver->ready_keys to a fresh list and save the current one
         * locally. This way as we run the old list we are free to call
         * signalKeyAsReady() that may push new elements in g_pserver->ready_keys
         * when handling clients blocked into BRPOPLPUSH. */
        l = g_pserver->ready_keys;
        g_pserver->ready_keys = listCreate();

        while(listLength(l) != 0) {
            listNode *ln = listFirst(l);
            readyList *rl = (readyList*)ln->value;

            /* First of all remove this key from db->ready_keys so that
             * we can safely call signalKeyAsReady() against this key. */
            dictDelete(rl->db->ready_keys,rl->key);

            /* Even if we are not inside call(), increment the call depth
             * in order to make sure that keys are expired against a fixed
             * reference time, and not against the wallclock time. This
             * way we can lookup an object multiple times (BLMOVE does
             * that) without the risk of it being freed in the second
             * lookup, invalidating the first one.
             * See https://github.com/redis/redis/pull/6554. */
            serverTL->fixed_time_expire++;

            /* Serve clients blocked on the key. */
            robj *o = lookupKeyWrite(rl->db,rl->key);

            if (o != NULL) {
                if (o->type == OBJ_LIST)
                    serveClientsBlockedOnListKey(o,rl);
                else if (o->type == OBJ_ZSET)
                    serveClientsBlockedOnSortedSetKey(o,rl);
                else if (o->type == OBJ_STREAM)
                    serveClientsBlockedOnStreamKey(o,rl);
                /* We want to serve clients blocked on module keys
                 * regardless of the object type: we don't know what the
                 * module is trying to accomplish right now. */
                serveClientsBlockedOnKeyByModule(rl);
            }
            serverTL->fixed_time_expire--;

            /* Free this item. */
            decrRefCount(rl->key);
            zfree(rl);
            listDelNode(l,ln);
        }
        listRelease(l); /* We have the new list on place at this point. */
    }
}

/* This is how the current blocking lists/sorted sets/streams work, we use
 * BLPOP as example, but the concept is the same for other list ops, sorted
 * sets and XREAD.
 * - If the user calls BLPOP and the key exists and contains a non empty list
 *   then LPOP is called instead. So BLPOP is semantically the same as LPOP
 *   if blocking is not required.
 * - If instead BLPOP is called and the key does not exists or the list is
 *   empty we need to block. In order to do so we remove the notification for
 *   new data to read in the client socket (so that we'll not serve new
 *   requests if the blocking request is not served). Also we put the client
 *   in a dictionary (db->blocking_keys) mapping keys to a list of clients
 *   blocking for this keys.
 * - If a PUSH operation against a key with blocked clients waiting is
 *   performed, we mark this key as "ready", and after the current command,
 *   MULTI/EXEC block, or script, is executed, we serve all the clients waiting
 *   for this list, from the one that blocked first, to the last, accordingly
 *   to the number of elements we have in the ready list.
 */

/* Set a client in blocking mode for the specified key (list, zset or stream),
 * with the specified timeout. The 'type' argument is BLOCKED_LIST,
 * BLOCKED_ZSET or BLOCKED_STREAM depending on the kind of operation we are
 * waiting for an empty key in order to awake the client. The client is blocked
 * for all the 'numkeys' keys as in the 'keys' argument. When we block for
 * stream keys, we also provide an array of streamID structures: clients will
 * be unblocked only when items with an ID greater or equal to the specified
 * one is appended to the stream. */
void blockForKeys(client *c, int btype, robj **keys, int numkeys, mstime_t timeout, robj *target, struct listPos *listpos, streamID *ids) {
    dictEntry *de;
    list *l;
    int j;

    c->bpop.timeout = timeout;
    c->bpop.target = target;

    if (listpos != NULL) c->bpop.listpos = *listpos;

    if (target != NULL) incrRefCount(target);

    for (j = 0; j < numkeys; j++) {
        /* Allocate our bkinfo structure, associated to each key the client
         * is blocked for. */
        bkinfo *bki = (bkinfo*)zmalloc(sizeof(*bki));
        if (btype == BLOCKED_STREAM)
            bki->stream_id = ids[j];

        /* If the key already exists in the dictionary ignore it. */
        if (dictAdd(c->bpop.keys,keys[j],bki) != DICT_OK) {
            zfree(bki);
            continue;
        }
        incrRefCount(keys[j]);

        /* And in the other "side", to map keys -> clients */
        de = dictFind(c->db->blocking_keys,keys[j]);
        if (de == NULL) {
            int retval;

            /* For every key we take a list of clients blocked for it */
            l = listCreate();
            retval = dictAdd(c->db->blocking_keys,keys[j],l);
            incrRefCount(keys[j]);
            serverAssertWithInfo(c,keys[j],retval == DICT_OK);
        } else {
            l = (list*)dictGetVal(de);
        }
        listAddNodeTail(l,c);
        bki->listnode = listLast(l);
    }
    blockClient(c,btype);
}

/* Unblock a client that's waiting in a blocking operation such as BLPOP.
 * You should never call this function directly, but unblockClient() instead. */
void unblockClientWaitingData(client *c) {
    dictEntry *de;
    dictIterator *di;
    list *l;

    serverAssertWithInfo(c,NULL,dictSize(c->bpop.keys) != 0);
    di = dictGetIterator(c->bpop.keys);
    /* The client may wait for multiple keys, so unblock it for every key. */
    while((de = dictNext(di)) != NULL) {
        robj *key = (robj*)dictGetKey(de);
        bkinfo *bki = (bkinfo*)dictGetVal(de);

        /* Remove this client from the list of clients waiting for this key. */
        l = (list*)dictFetchValue(c->db->blocking_keys,key);
        serverAssertWithInfo(c,key,l != NULL);
        listDelNode(l,bki->listnode);
        /* If the list is empty we need to remove it to avoid wasting memory */
        if (listLength(l) == 0)
            dictDelete(c->db->blocking_keys,key);
    }
    dictReleaseIterator(di);

    /* Cleanup the client structure */
    dictEmpty(c->bpop.keys,NULL);
    if (c->bpop.target) {
        decrRefCount(c->bpop.target);
        c->bpop.target = NULL;
    }
    if (c->bpop.xread_group) {
        decrRefCount(c->bpop.xread_group);
        decrRefCount(c->bpop.xread_consumer);
        c->bpop.xread_group = NULL;
        c->bpop.xread_consumer = NULL;
    }
    c->bpop.timeout = 0;
}

static int getBlockedTypeByType(int type) {
    switch (type) {
        case OBJ_LIST: return BLOCKED_LIST;
        case OBJ_ZSET: return BLOCKED_ZSET;
        case OBJ_MODULE: return BLOCKED_MODULE;
        case OBJ_STREAM: return BLOCKED_STREAM;
        default: return BLOCKED_NONE;
    }
}

/* If the specified key has clients blocked waiting for list pushes, this
 * function will put the key reference into the g_pserver->ready_keys list.
 * Note that db->ready_keys is a hash table that allows us to avoid putting
 * the same key again and again in the list in case of multiple pushes
 * made by a script or in the context of MULTI/EXEC.
 *
 * The list will be finally processed by handleClientsBlockedOnKeys() */
void signalKeyAsReady(redisDb *db, robj *key, int type) {
    readyList *rl;

    /* Quick returns. */
    int btype = getBlockedTypeByType(type);
    if (btype == BLOCKED_NONE) {
        /* The type can never block. */
        return;
    }
    if (!g_pserver->blocked_clients_by_type[btype] &&
        !g_pserver->blocked_clients_by_type[BLOCKED_MODULE]) {
        /* No clients block on this type. Note: Blocked modules are represented
         * by BLOCKED_MODULE, even if the intention is to wake up by normal
         * types (list, zset, stream), so we need to check that there are no
         * blocked modules before we do a quick return here. */
        return;
    }

    /* No clients blocking for this key? No need to queue it. */
    if (dictFind(db->blocking_keys,key) == NULL) return;

    /* Key was already signaled? No need to queue it again. */
    if (dictFind(db->ready_keys,key) != NULL) return;

    if (key->getrefcount() == OBJ_STATIC_REFCOUNT) {
        // Sometimes a key may be stack allocated, we'll need to dupe it
        robj *newKey = createStringObject(szFromObj(key), sdslen(szFromObj(key)));
        newKey->setrefcount(0); // Start with 0 but don't free
        key = newKey;
    }

    /* Ok, we need to queue this key into g_pserver->ready_keys. */
    rl = (readyList*)zmalloc(sizeof(*rl), MALLOC_SHARED);
    rl->key = key;
    rl->db = db;
    incrRefCount(key);
    listAddNodeTail(g_pserver->ready_keys,rl);

    /* We also add the key in the db->ready_keys dictionary in order
     * to avoid adding it multiple times into a list with a simple O(1)
     * check. */
    incrRefCount(key);
    serverAssert(dictAdd(db->ready_keys,key,NULL) == DICT_OK);
}

void signalKeyAsReady(redisDb *db, sds key, int type) {
    redisObjectStack o;
    initStaticStringObject(o, key);
    signalKeyAsReady(db, &o, type);
}
