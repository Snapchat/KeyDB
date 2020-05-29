#include "fmacros.h"
#include "version.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <signal.h>
#include <unistd.h>
#include <time.h>
#include <ctype.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <assert.h>
#include <fcntl.h>
#include <limits.h>
#include <math.h>

extern "C" {
#include <hiredis.h>
#include <sds.h> /* use sds.h from hiredis, so that only one set of sds functions will be present in the binary */
}
#include "dict.h"
#include "adlist.h"
#include "zmalloc.h"
#include "storage.h"

#include "redis-cli.h"

static dict *clusterManagerGetLinkStatus(void);
static clusterManagerNode *clusterManagerNodeMasterRandom();

/* Used by clusterManagerFixSlotsCoverage */
struct dict *clusterManagerUncoveredSlots = NULL;

/* The Cluster Manager global structure */
struct clusterManager cluster_manager;

extern "C" uint64_t dictSdsHash(const void *key) {
    return dictGenHashFunction((unsigned char*)key, sdslen((char*)key));
}

extern "C" int dictSdsKeyCompare(void *privdata, const void *key1,
        const void *key2)
{
    int l1,l2;
    DICT_NOTUSED(privdata);

    l1 = sdslen((sds)key1);
    l2 = sdslen((sds)key2);
    if (l1 != l2) return 0;
    return memcmp(key1, key2, l1) == 0;
}

extern "C" void dictSdsDestructor(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);
    sdsfree((sds)val);
}

extern "C" void dictListDestructor(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);
    listRelease((list*)val);
}

static dictType clusterManagerDictType = {
    dictSdsHash,               /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictSdsKeyCompare,         /* key compare */
    NULL,                      /* key destructor */
    dictSdsDestructor          /* val destructor */
};

static dictType clusterManagerLinkDictType = {
    dictSdsHash,               /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictSdsKeyCompare,         /* key compare */
    dictSdsDestructor,         /* key destructor */
    dictListDestructor         /* val destructor */
};


extern "C" void freeClusterManager(void) {
    listIter li;
    listNode *ln;
    if (cluster_manager.nodes != NULL) {
        listRewind(cluster_manager.nodes,&li);
        while ((ln = listNext(&li)) != NULL) {
            clusterManagerNode *n = (clusterManagerNode*)ln->value;
            freeClusterManagerNode(n);
        }
        listRelease(cluster_manager.nodes);
        cluster_manager.nodes = NULL;
    }
    if (cluster_manager.errors != NULL) {
        listRewind(cluster_manager.errors,&li);
        while ((ln = listNext(&li)) != NULL) {
            sds err = (sds)ln->value;
            sdsfree(err);
        }
        listRelease(cluster_manager.errors);
        cluster_manager.errors = NULL;
    }
    if (clusterManagerUncoveredSlots != NULL)
        dictRelease(clusterManagerUncoveredSlots);
}

/* This function returns a random master node, return NULL if none */

static clusterManagerNode *clusterManagerNodeMasterRandom() {
    int master_count = 0;
    int idx;
    listIter li;
    listNode *ln;
    listRewind(cluster_manager.nodes, &li);
    while ((ln = listNext(&li)) != NULL) {
        clusterManagerNode *n = (clusterManagerNode*) ln->value;
        if (n->flags & CLUSTER_MANAGER_FLAG_SLAVE) continue;
        master_count++;
    }

    srand(time(NULL));
    idx = rand() % master_count;
    listRewind(cluster_manager.nodes, &li);
    while ((ln = listNext(&li)) != NULL) {
        clusterManagerNode *n = (clusterManagerNode*) ln->value;
        if (n->flags & CLUSTER_MANAGER_FLAG_SLAVE) continue;
        if (!idx--) {
            return n;
        }
    }
    /* Can not be reached */
    return NULL;
}

static int clusterManagerFixSlotsCoverage(char *all_slots) {
    int force_fix = config.cluster_manager_command.flags &
                    CLUSTER_MANAGER_CMD_FLAG_FIX_WITH_UNREACHABLE_MASTERS;
    /* we want explicit manual confirmation from users for all the fix cases */
    int force = 0;

    dictIterator *iter = nullptr;

    if (cluster_manager.unreachable_masters > 0 && !force_fix) {
        clusterManagerLogWarn("*** Fixing slots coverage with %d unreachable masters is dangerous: redis-cli will assume that slots about masters that are not reachable are not covered, and will try to reassign them to the reachable nodes. This can cause data loss and is rarely what you want to do. If you really want to proceed use the --cluster-fix-with-unreachable-masters option.\n", cluster_manager.unreachable_masters);
        exit(1);
    }

    int i, fixed = 0;
    list *none = NULL, *single = NULL, *multi = NULL;
    clusterManagerLogInfo(">>> Fixing slots coverage...\n");
    for (i = 0; i < CLUSTER_MANAGER_SLOTS; i++) {
        int covered = all_slots[i];
        if (!covered) {
            sds slot = sdsfromlonglong((long long) i);
            list *slot_nodes = listCreate();
            sds slot_nodes_str = sdsempty();
            listIter li;
            listNode *ln;
            listRewind(cluster_manager.nodes, &li);
            while ((ln = listNext(&li)) != NULL) {
                clusterManagerNode *n = (clusterManagerNode*) ln->value;
                if (n->flags & CLUSTER_MANAGER_FLAG_SLAVE || n->replicate)
                    continue;
                redisReply *reply = (redisReply*)CLUSTER_MANAGER_COMMAND(n,
                    "CLUSTER GETKEYSINSLOT %d %d", i, 1);
                if (!clusterManagerCheckRedisReply(n, reply, NULL)) {
                    fixed = -1;
                    if (reply) freeReplyObject(reply);
                    goto cleanup;
                }
                assert(reply->type == REDIS_REPLY_ARRAY);
                if (reply->elements > 0) {
                    listAddNodeTail(slot_nodes, n);
                    if (listLength(slot_nodes) > 1)
                        slot_nodes_str = sdscat(slot_nodes_str, ", ");
                    slot_nodes_str = sdscatfmt(slot_nodes_str,
                                               "%s:%u", n->ip, n->port);
                }
                freeReplyObject(reply);
            }
            sdsfree(slot_nodes_str);
            dictAdd(clusterManagerUncoveredSlots, slot, slot_nodes);
        }
    }

    /* For every slot, take action depending on the actual condition:
     * 1) No node has keys for this slot.
     * 2) A single node has keys for this slot.
     * 3) Multiple nodes have keys for this slot. */
    none = listCreate();
    single = listCreate();
    multi = listCreate();
    iter = dictGetIterator(clusterManagerUncoveredSlots);
    dictEntry *entry;
    while ((entry = dictNext(iter)) != NULL) {
        sds slot = (sds) dictGetKey(entry);
        list *nodes = (list *) dictGetVal(entry);
        switch (listLength(nodes)){
        case 0: listAddNodeTail(none, slot); break;
        case 1: listAddNodeTail(single, slot); break;
        default: listAddNodeTail(multi, slot); break;
        }
    }
    dictReleaseIterator(iter);

    /*  Handle case "1": keys in no node. */
    if (listLength(none) > 0) {
        printf("The following uncovered slots have no keys "
               "across the cluster:\n");
        clusterManagerPrintSlotsList(none);
        if (confirmWithYes("Fix these slots by covering with a random node?",
                           force)) {
            listIter li;
            listNode *ln;
            listRewind(none, &li);
            while ((ln = listNext(&li)) != NULL) {
                sds slot = (sds)ln->value;
                int s = atoi(slot);
                clusterManagerNode *n = clusterManagerNodeMasterRandom();
                clusterManagerLogInfo(">>> Covering slot %s with %s:%d\n",
                                      slot, n->ip, n->port);
                if (!clusterManagerSetSlotOwner(n, s, 0)) {
                    fixed = -1;
                    goto cleanup;
                }
                /* Since CLUSTER ADDSLOTS succeeded, we also update the slot
                 * info into the node struct, in order to keep it synced */
                n->slots[s] = 1;
                fixed++;
            }
        }
    }

    /*  Handle case "2": keys only in one node. */
    if (listLength(single) > 0) {
        printf("The following uncovered slots have keys in just one node:\n");
        clusterManagerPrintSlotsList(single);
        if (confirmWithYes("Fix these slots by covering with those nodes?",
                           force)) {
            listIter li;
            listNode *ln;
            listRewind(single, &li);
            while ((ln = listNext(&li)) != NULL) {
                sds slot = (sds)ln->value;
                int s = atoi(slot);
                dictEntry *entry = dictFind(clusterManagerUncoveredSlots, slot);
                assert(entry != NULL);
                list *nodes = (list *) dictGetVal(entry);
                listNode *fn = listFirst(nodes);
                assert(fn != NULL);
                clusterManagerNode *n = (clusterManagerNode*) fn->value;
                clusterManagerLogInfo(">>> Covering slot %s with %s:%d\n",
                                      slot, n->ip, n->port);
                if (!clusterManagerSetSlotOwner(n, s, 0)) {
                    fixed = -1;
                    goto cleanup;
                }
                /* Since CLUSTER ADDSLOTS succeeded, we also update the slot
                 * info into the node struct, in order to keep it synced */
                n->slots[atoi(slot)] = 1;
                fixed++;
            }
        }
    }

    /* Handle case "3": keys in multiple nodes. */
    if (listLength(multi) > 0) {
        printf("The following uncovered slots have keys in multiple nodes:\n");
        clusterManagerPrintSlotsList(multi);
        if (confirmWithYes("Fix these slots by moving keys "
                           "into a single node?", force)) {
            listIter li;
            listNode *ln;
            listRewind(multi, &li);
            while ((ln = listNext(&li)) != NULL) {
                sds slot = (sds)ln->value;
                dictEntry *entry = dictFind(clusterManagerUncoveredSlots, slot);
                assert(entry != NULL);
                list *nodes = (list *) dictGetVal(entry);
                int s = atoi(slot);
                clusterManagerNode *target =
                    clusterManagerGetNodeWithMostKeysInSlot(nodes, s, NULL);
                if (target == NULL) {
                    fixed = -1;
                    goto cleanup;
                }
                clusterManagerLogInfo(">>> Covering slot %s moving keys "
                                      "to %s:%d\n", slot,
                                      target->ip, target->port);
                if (!clusterManagerSetSlotOwner(target, s, 1)) {
                    fixed = -1;
                    goto cleanup;
                }
                /* Since CLUSTER ADDSLOTS succeeded, we also update the slot
                 * info into the node struct, in order to keep it synced */
                target->slots[atoi(slot)] = 1;
                listIter nli;
                listNode *nln;
                listRewind(nodes, &nli);
                while ((nln = listNext(&nli)) != NULL) {
                    clusterManagerNode *src = (clusterManagerNode*) nln->value;
                    if (src == target) continue;
                    /* Assign the slot to target node in the source node. */
                    if (!clusterManagerSetSlot(src, target, s, "NODE", NULL))
                        fixed = -1;
                    if (fixed < 0) goto cleanup;
                    /* Set the source node in 'importing' state
                     * (even if we will actually migrate keys away)
                     * in order to avoid receiving redirections
                     * for MIGRATE. */
                    if (!clusterManagerSetSlot(src, target, s,
                                               "IMPORTING", NULL)) fixed = -1;
                    if (fixed < 0) goto cleanup;
                    int opts = CLUSTER_MANAGER_OPT_VERBOSE |
                               CLUSTER_MANAGER_OPT_COLD;
                    if (!clusterManagerMoveSlot(src, target, s, opts, NULL)) {
                        fixed = -1;
                        goto cleanup;
                    }
                    if (!clusterManagerClearSlotStatus(src, s))
                        fixed = -1;
                    if (fixed < 0) goto cleanup;
                }
                fixed++;
            }
        }
    }
cleanup:
    if (none) listRelease(none);
    if (single) listRelease(single);
    if (multi) listRelease(multi);
    return fixed;
}

/* Return the anti-affinity score, which is a measure of the amount of
 * violations of anti-affinity in the current cluster layout, that is, how
 * badly the masters and slaves are distributed in the different IP
 * addresses so that slaves of the same master are not in the master
 * host and are also in different hosts.
 *
 * The score is calculated as follows:
 *
 * SAME_AS_MASTER = 10000 * each replica in the same IP of its master.
 * SAME_AS_SLAVE  = 1 * each replica having the same IP as another replica
                      of the same master.
 * FINAL_SCORE = SAME_AS_MASTER + SAME_AS_SLAVE
 *
 * So a greater score means a worse anti-affinity level, while zero
 * means perfect anti-affinity.
 *
 * The anti affinity optimizator will try to get a score as low as
 * possible. Since we do not want to sacrifice the fact that slaves should
 * not be in the same host as the master, we assign 10000 times the score
 * to this violation, so that we'll optimize for the second factor only
 * if it does not impact the first one.
 *
 * The ipnodes argument is an array of clusterManagerNodeArray, one for
 * each IP, while ip_count is the total number of IPs in the configuration.
 *
 * The function returns the above score, and the list of
 * offending slaves can be stored into the 'offending' argument,
 * so that the optimizer can try changing the configuration of the
 * slaves violating the anti-affinity goals. */
int clusterManagerGetAntiAffinityScore(clusterManagerNodeArray *ipnodes,
    int ip_count, clusterManagerNode ***offending, int *offending_len)
{
    int score = 0, i, j;
    int node_len = cluster_manager.nodes->len;
    clusterManagerNode **offending_p = NULL;
    if (offending != NULL) {
        *offending = (clusterManagerNode**)zcalloc(node_len * sizeof(clusterManagerNode*), MALLOC_LOCAL);
        offending_p = *offending;
    }
    /* For each set of nodes in the same host, split by
     * related nodes (masters and slaves which are involved in
     * replication of each other) */
    for (i = 0; i < ip_count; i++) {
        clusterManagerNodeArray *node_array = &(ipnodes[i]);
        dict *related = dictCreate(&clusterManagerDictType, NULL);
        char *ip = NULL;
        for (j = 0; j < node_array->len; j++) {
            clusterManagerNode *node = node_array->nodes[j];
            if (node == NULL) continue;
            if (!ip) ip = node->ip;
            sds types;
            /* We always use the Master ID as key. */
            sds key = (!node->replicate ? node->name : node->replicate);
            assert(key != NULL);
            dictEntry *entry = dictFind(related, key);
            if (entry) types = sdsdup((sds) dictGetVal(entry));
            else types = sdsempty();
            /* Master type 'm' is always set as the first character of the
             * types string. */
            if (!node->replicate) types = sdscatprintf(types, "m%s", types);
            else types = sdscat(types, "s");
            dictReplace(related, key, types);
        }
        /* Now it's trivial to check, for each related group having the
         * same host, what is their local score. */
        dictIterator *iter = dictGetIterator(related);
        dictEntry *entry;
        while ((entry = dictNext(iter)) != NULL) {
            sds types = (sds) dictGetVal(entry);
            sds name = (sds) dictGetKey(entry);
            int typeslen = sdslen(types);
            if (typeslen < 2) continue;
            if (types[0] == 'm') score += (10000 * (typeslen - 1));
            else score += (1 * typeslen);
            if (offending == NULL) continue;
            /* Populate the list of offending nodes. */
            listIter li;
            listNode *ln;
            listRewind(cluster_manager.nodes, &li);
            while ((ln = listNext(&li)) != NULL) {
                clusterManagerNode *n = (clusterManagerNode*)ln->value;
                if (n->replicate == NULL) continue;
                if (!strcmp(n->replicate, name) && !strcmp(n->ip, ip)) {
                    *(offending_p++) = n;
                    if (offending_len != NULL) (*offending_len)++;
                    break;
                }
            }
        }
        //if (offending_len != NULL) *offending_len = offending_p - *offending;
        dictReleaseIterator(iter);
        dictRelease(related);
    }
    return score;
}


/* Wait until the cluster configuration is consistent. */
extern "C" void clusterManagerWaitForClusterJoin(void) {
    printf("Waiting for the cluster to join\n");
    int counter = 0,
        check_after = CLUSTER_JOIN_CHECK_AFTER +
                      (int)(listLength(cluster_manager.nodes) * 0.15f);
    while(!clusterManagerIsConfigConsistent()) {
        printf(".");
        fflush(stdout);
        sleep(1);
        if (++counter > check_after) {
            dict *status = clusterManagerGetLinkStatus();
            dictIterator *iter = NULL;
            if (status != NULL && dictSize(status) > 0) {
                printf("\n");
                clusterManagerLogErr("Warning: %d node(s) may "
                                     "be unreachable\n", dictSize(status));
                iter = dictGetIterator(status);
                dictEntry *entry;
                while ((entry = dictNext(iter)) != NULL) {
                    sds nodeaddr = (sds) dictGetKey(entry);
                    char *node_ip = NULL;
                    int node_port = 0, node_bus_port = 0;
                    list *from = (list *) dictGetVal(entry);
                    if (parseClusterNodeAddress(nodeaddr, &node_ip,
                        &node_port, &node_bus_port) && node_bus_port) {
                        clusterManagerLogErr(" - The port %d of node %s may "
                                             "be unreachable from:\n",
                                             node_bus_port, node_ip);
                    } else {
                        clusterManagerLogErr(" - Node %s may be unreachable "
                                             "from:\n", nodeaddr);
                    }
                    listIter li;
                    listNode *ln;
                    listRewind(from, &li);
                    while ((ln = listNext(&li)) != NULL) {
                        sds from_addr = (sds)ln->value;
                        clusterManagerLogErr("   %s\n", from_addr);
                        sdsfree(from_addr);
                    }
                    clusterManagerLogErr("Cluster bus ports must be reachable "
                                         "by every node.\nRemember that "
                                         "cluster bus ports are different "
                                         "from standard instance ports.\n");
                    listEmpty(from);
                }
            }
            if (iter != NULL) dictReleaseIterator(iter);
            if (status != NULL) dictRelease(status);
            counter = 0;
        }
    }
    printf("\n");
}

list *clusterManagerGetDisconnectedLinks(clusterManagerNode *node) {
    list *links = NULL;
    char *lines = NULL, *p, *line;
    redisReply *reply = (redisReply*)CLUSTER_MANAGER_COMMAND(node, "CLUSTER NODES");
    if (!clusterManagerCheckRedisReply(node, reply, NULL)) goto cleanup;
    links = listCreate();
    lines = reply->str;
    while ((p = strstr(lines, "\n")) != NULL) {
        int i = 0;
        *p = '\0';
        line = lines;
        lines = p + 1;
        char *nodename = NULL, *addr = NULL, *flags = NULL, *link_status = NULL;
        while ((p = strchr(line, ' ')) != NULL) {
            *p = '\0';
            char *token = line;
            line = p + 1;
            if (i == 0) nodename = token;
            else if (i == 1) addr = token;
            else if (i == 2) flags = token;
            else if (i == 7) link_status = token;
            else if (i == 8) break;
            i++;
        }
        if (i == 7) link_status = line;
        if (nodename == NULL || addr == NULL || flags == NULL ||
            link_status == NULL) continue;
        if (strstr(flags, "myself") != NULL) continue;
        int disconnected = ((strstr(flags, "disconnected") != NULL) ||
                            (strstr(link_status, "disconnected")));
        int handshaking = (strstr(flags, "handshake") != NULL);
        if (disconnected || handshaking) {
            clusterManagerLink *link = (clusterManagerLink*)zmalloc(sizeof(*link), MALLOC_LOCAL);
            link->node_name = sdsnew(nodename);
            link->node_addr = sdsnew(addr);
            link->connected = 0;
            link->handshaking = handshaking;
            listAddNodeTail(links, link);
        }
    }
cleanup:
    if (reply != NULL) freeReplyObject(reply);
    return links;
}

/* Check for disconnected cluster links. It returns a dict whose keys
 * are the unreachable node addresses and the values are lists of
 * node addresses that cannot reach the unreachable node. */
dict *clusterManagerGetLinkStatus(void) {
    if (cluster_manager.nodes == NULL) return NULL;
    dict *status = dictCreate(&clusterManagerLinkDictType, NULL);
    listIter li;
    listNode *ln;
    listRewind(cluster_manager.nodes, &li);
    while ((ln = listNext(&li)) != NULL) {
        clusterManagerNode *node = (clusterManagerNode*)ln->value;
        list *links = clusterManagerGetDisconnectedLinks(node);
        if (links) {
            listIter lli;
            listNode *lln;
            listRewind(links, &lli);
            while ((lln = listNext(&lli)) != NULL) {
                clusterManagerLink *link = (clusterManagerLink*)lln->value;
                list *from = NULL;
                dictEntry *entry = dictFind(status, link->node_addr);
                if (entry) from = (list*)dictGetVal(entry);
                else {
                    from = listCreate();
                    dictAdd(status, sdsdup(link->node_addr), from);
                }
                sds myaddr = sdsempty();
                myaddr = sdscatfmt(myaddr, "%s:%u", node->ip, node->port);
                listAddNodeTail(from, myaddr);
                sdsfree(link->node_name);
                sdsfree(link->node_addr);
                zfree(link);
            }
            listRelease(links);
        }
    }
    return status;
}

extern "C" int clusterManagerCheckCluster(int quiet) {
    listNode *ln = listFirst(cluster_manager.nodes);
    if (!ln) return 0;
    clusterManagerNode *node = (clusterManagerNode*)ln->value;
    clusterManagerLogInfo(">>> Performing Cluster Check (using node %s:%d)\n",
                          node->ip, node->port);
    int result = 1, consistent = 0;
    int do_fix = config.cluster_manager_command.flags &
                 CLUSTER_MANAGER_CMD_FLAG_FIX;
    if (!quiet) clusterManagerShowNodes();
    consistent = clusterManagerIsConfigConsistent();
    if (!consistent) {
        sds err = sdsnew("[ERR] Nodes don't agree about configuration!");
        clusterManagerOnError(err);
        result = 0;
    } else {
        clusterManagerLogOk("[OK] All nodes agree about slots "
                            "configuration.\n");
    }
    /* Check open slots */
    clusterManagerLogInfo(">>> Check for open slots...\n");
    listIter li;
    listRewind(cluster_manager.nodes, &li);
    int i;
    dict *open_slots = NULL;
    while ((ln = listNext(&li)) != NULL) {
        clusterManagerNode *n = (clusterManagerNode*)ln->value;
        if (n->migrating != NULL) {
            if (open_slots == NULL)
                open_slots = dictCreate(&clusterManagerDictType, NULL);
            sds errstr = sdsempty();
            errstr = sdscatprintf(errstr,
                                "[WARNING] Node %s:%d has slots in "
                                "migrating state ",
                                n->ip,
                                n->port);
            for (i = 0; i < n->migrating_count; i += 2) {
                sds slot = n->migrating[i];
                dictReplace(open_slots, slot, sdsdup(n->migrating[i + 1]));
                const char *fmt = (i > 0 ? ",%S" : "%S");
                errstr = sdscatfmt(errstr, fmt, slot);
            }
            errstr = sdscat(errstr, ".");
            clusterManagerOnError(errstr);
        }
        if (n->importing != NULL) {
            if (open_slots == NULL)
                open_slots = dictCreate(&clusterManagerDictType, NULL);
            sds errstr = sdsempty();
            errstr = sdscatprintf(errstr,
                                "[WARNING] Node %s:%d has slots in "
                                "importing state ",
                                n->ip,
                                n->port);
            for (i = 0; i < n->importing_count; i += 2) {
                sds slot = n->importing[i];
                dictReplace(open_slots, slot, sdsdup(n->importing[i + 1]));
                const char *fmt = (i > 0 ? ",%S" : "%S");
                errstr = sdscatfmt(errstr, fmt, slot);
            }
            errstr = sdscat(errstr, ".");
            clusterManagerOnError(errstr);
        }
    }
    if (open_slots != NULL) {
        result = 0;
        dictIterator *iter = dictGetIterator(open_slots);
        dictEntry *entry;
        sds errstr = sdsnew("[WARNING] The following slots are open: ");
        i = 0;
        while ((entry = dictNext(iter)) != NULL) {
            sds slot = (sds) dictGetKey(entry);
            const char *fmt = (i++ > 0 ? ",%S" : "%S");
            errstr = sdscatfmt(errstr, fmt, slot);
        }
        clusterManagerLogErr("%s.\n", (char *) errstr);
        sdsfree(errstr);
        if (do_fix) {
            /* Fix open slots. */
            dictReleaseIterator(iter);
            iter = dictGetIterator(open_slots);
            while ((entry = dictNext(iter)) != NULL) {
                sds slot = (sds) dictGetKey(entry);
                result = clusterManagerFixOpenSlot(atoi(slot));
                if (!result) break;
            }
        }
        dictReleaseIterator(iter);
        dictRelease(open_slots);
    }
    clusterManagerLogInfo(">>> Check slots coverage...\n");
    char slots[CLUSTER_MANAGER_SLOTS];
    memset(slots, 0, CLUSTER_MANAGER_SLOTS);
    int coverage = clusterManagerGetCoveredSlots(slots);
    if (coverage == CLUSTER_MANAGER_SLOTS) {
        clusterManagerLogOk("[OK] All %d slots covered.\n",
                            CLUSTER_MANAGER_SLOTS);
    } else {
        sds err = sdsempty();
        err = sdscatprintf(err, "[ERR] Not all %d slots are "
                                "covered by nodes.\n",
                                CLUSTER_MANAGER_SLOTS);
        clusterManagerOnError(err);
        result = 0;
        if (do_fix/* && result*/) {
            dictType dtype = clusterManagerDictType;
            dtype.keyDestructor = dictSdsDestructor;
            dtype.valDestructor = dictListDestructor;
            clusterManagerUncoveredSlots = dictCreate(&dtype, NULL);
            int fixed = clusterManagerFixSlotsCoverage(slots);
            if (fixed > 0) result = 1;
        }
    }
    int search_multiple_owners = config.cluster_manager_command.flags &
                                 CLUSTER_MANAGER_CMD_FLAG_CHECK_OWNERS;
    if (search_multiple_owners) {
        /* Check whether there are multiple owners, even when slots are
         * fully covered and there are no open slots. */
        clusterManagerLogInfo(">>> Check for multiple slot owners...\n");
        int slot = 0, slots_with_multiple_owners = 0;
        for (; slot < CLUSTER_MANAGER_SLOTS; slot++) {
            listIter li;
            listNode *ln;
            listRewind(cluster_manager.nodes, &li);
            list *owners = listCreate();
            while ((ln = listNext(&li)) != NULL) {
                clusterManagerNode *n = (clusterManagerNode*)ln->value;
                if (n->flags & CLUSTER_MANAGER_FLAG_SLAVE) continue;
                if (n->slots[slot]) listAddNodeTail(owners, n);
                else {
                    /* Nodes having keys for the slot will be considered
                     * owners too. */
                    int count = clusterManagerCountKeysInSlot(n, slot);
                    if (count > 0) listAddNodeTail(owners, n);
                }
            }
            if (listLength(owners) > 1) {
                result = 0;
                clusterManagerLogErr("[WARNING] Slot %d has %d owners:\n",
                                     slot, listLength(owners));
                listRewind(owners, &li);
                while ((ln = listNext(&li)) != NULL) {
                    clusterManagerNode *n = (clusterManagerNode*)ln->value;
                    clusterManagerLogErr("    %s:%d\n", n->ip, n->port);
                }
                slots_with_multiple_owners++;
                if (do_fix) {
                    result = clusterManagerFixMultipleSlotOwners(slot, owners);
                    if (!result) {
                        clusterManagerLogErr("Failed to fix multiple owners "
                                             "for slot %d\n", slot);
                        listRelease(owners);
                        break;
                    } else slots_with_multiple_owners--;
                }
            }
            listRelease(owners);
        }
        if (slots_with_multiple_owners == 0)
            clusterManagerLogOk("[OK] No multiple owners found.\n");
    }
    return result;
}

static typeinfo* typeinfo_add(dict *types, const char* name, typeinfo* type_template) {
    typeinfo *info = (typeinfo*)zmalloc(sizeof(typeinfo), MALLOC_LOCAL);
    *info = *type_template;
    info->name = sdsnew(name);
    dictAdd(types, info->name, info);
    return info;
}

static dictType typeinfoDictType = {
    dictSdsHash,               /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictSdsKeyCompare,         /* key compare */
    NULL,                      /* key destructor (owned by the value)*/
    type_free                  /* val destructor */
};

static void getKeyTypes(dict *types_dict, redisReply *keys, typeinfo **types) {
    redisReply *reply;
    unsigned int i;

    /* Pipeline TYPE commands */
    for(i=0;i<keys->elements;i++) {
        redisAppendCommand(context, "TYPE %s", keys->element[i]->str);
    }

    /* Retrieve types */
    for(i=0;i<keys->elements;i++) {
        if(redisGetReply(context, (void**)&reply)!=REDIS_OK) {
            fprintf(stderr, "Error getting type for key '%s' (%d: %s)\n",
                keys->element[i]->str, context->err, context->errstr);
            exit(1);
        } else if(reply->type != REDIS_REPLY_STATUS) {
            if(reply->type == REDIS_REPLY_ERROR) {
                fprintf(stderr, "TYPE returned an error: %s\n", reply->str);
            } else {
                fprintf(stderr,
                    "Invalid reply type (%d) for TYPE on key '%s'!\n",
                    reply->type, keys->element[i]->str);
            }
            exit(1);
        }

        sds typereply = sdsnew(reply->str);
        dictEntry *de = dictFind(types_dict, typereply);
        sdsfree(typereply);
        typeinfo *type = NULL;
        if (de)
            type = (typeinfo*)dictGetVal(de);
        else if (strcmp(reply->str, "none")) /* create new types for modules, (but not for deleted keys) */
            type = typeinfo_add(types_dict, reply->str, &type_other);
        types[i] = type;
        freeReplyObject(reply);
    }
}

void findBigKeys(int memkeys, unsigned memkeys_samples) {
    unsigned long long sampled = 0, total_keys, totlen=0, *sizes=NULL, it=0;
    redisReply *reply, *keys;
    unsigned int arrsize=0, i;
    dictIterator *di;
    dictEntry *de;
    typeinfo **types = NULL;
    double pct;

    dict *types_dict = dictCreate(&typeinfoDictType, NULL);
    typeinfo_add(types_dict, "string", &type_string);
    typeinfo_add(types_dict, "list", &type_list);
    typeinfo_add(types_dict, "set", &type_set);
    typeinfo_add(types_dict, "hash", &type_hash);
    typeinfo_add(types_dict, "zset", &type_zset);
    typeinfo_add(types_dict, "stream", &type_stream);

    /* Total keys pre scanning */
    total_keys = getDbSize();

    /* Status message */
    printf("\n# Scanning the entire keyspace to find biggest keys as well as\n");
    printf("# average sizes per key type.  You can use -i 0.1 to sleep 0.1 sec\n");
    printf("# per 100 SCAN commands (not usually needed).\n\n");

    /* SCAN loop */
    do {
        /* Calculate approximate percentage completion */
        pct = 100 * (double)sampled/total_keys;

        /* Grab some keys and point to the keys array */
        reply = sendScan(&it);
        keys  = reply->element[1];

        /* Reallocate our type and size array if we need to */
        if(keys->elements > arrsize) {
            types = (typeinfo**)zrealloc(types, sizeof(int)*keys->elements, MALLOC_LOCAL);
            sizes = (unsigned long long*)zrealloc(sizes, sizeof(unsigned long long)*keys->elements, MALLOC_LOCAL);

            if(!types || !sizes) {
                fprintf(stderr, "Failed to allocate storage for keys!\n");
                exit(1);
            }

            arrsize = keys->elements;
        }

        /* Retrieve types and then sizes */
        getKeyTypes(types_dict, keys, types);
        getKeySizes(keys, types, sizes, memkeys, memkeys_samples);

        /* Now update our stats */
        for(i=0;i<keys->elements;i++) {
            typeinfo *type = types[i];
            /* Skip keys that disappeared between SCAN and TYPE */
            if(!type)
                continue;

            type->totalsize += sizes[i];
            type->count++;
            totlen += keys->element[i]->len;
            sampled++;

            if(type->biggest<sizes[i]) {
                printf(
                   "[%05.2f%%] Biggest %-6s found so far '%s' with %llu %s\n",
                   pct, type->name, keys->element[i]->str, sizes[i],
                   !memkeys? type->sizeunit: "bytes");

                /* Keep track of biggest key name for this type */
                if (type->biggest_key)
                    sdsfree(type->biggest_key);
                type->biggest_key = sdsnew(keys->element[i]->str);
                if(!type->biggest_key) {
                    fprintf(stderr, "Failed to allocate memory for key!\n");
                    exit(1);
                }

                /* Keep track of the biggest size for this type */
                type->biggest = sizes[i];
            }

            /* Update overall progress */
            if(sampled % 1000000 == 0) {
                printf("[%05.2f%%] Sampled %llu keys so far\n", pct, sampled);
            }
        }

        /* Sleep if we've been directed to do so */
        if(sampled && (sampled %100) == 0 && config.interval) {
            usleep(config.interval);
        }

        freeReplyObject(reply);
    } while(it != 0);

    if(types) zfree(types);
    if(sizes) zfree(sizes);

    /* We're done */
    printf("\n-------- summary -------\n\n");

    printf("Sampled %llu keys in the keyspace!\n", sampled);
    printf("Total key length in bytes is %llu (avg len %.2f)\n\n",
       totlen, totlen ? (double)totlen/sampled : 0);

    /* Output the biggest keys we found, for types we did find */
    di = dictGetIterator(types_dict);
    while ((de = dictNext(di))) {
        typeinfo *type = (typeinfo*)dictGetVal(de);
        if(type->biggest_key) {
            printf("Biggest %6s found '%s' has %llu %s\n", type->name, type->biggest_key,
               type->biggest, !memkeys? type->sizeunit: "bytes");
        }
    }
    dictReleaseIterator(di);

    printf("\n");

    di = dictGetIterator(types_dict);
    while ((de = dictNext(di))) {
        typeinfo *type = (typeinfo*)dictGetVal(de);
        printf("%llu %ss with %llu %s (%05.2f%% of keys, avg size %.2f)\n",
           type->count, type->name, type->totalsize, !memkeys? type->sizeunit: "bytes",
           sampled ? 100 * (double)type->count/sampled : 0,
           type->count ? (double)type->totalsize/type->count : 0);
    }
    dictReleaseIterator(di);

    dictRelease(types_dict);

    /* Success! */
    exit(0);
}
