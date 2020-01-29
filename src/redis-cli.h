#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#define UNUSED(V) ((void) V)

#define OUTPUT_STANDARD 0
#define OUTPUT_RAW 1
#define OUTPUT_CSV 2
#define REDIS_CLI_KEEPALIVE_INTERVAL 15 /* seconds */
#define REDIS_CLI_DEFAULT_PIPE_TIMEOUT 30 /* seconds */
#define REDIS_CLI_HISTFILE_ENV "REDISCLI_HISTFILE"
#define REDIS_CLI_HISTFILE_DEFAULT ".rediscli_history"
#define REDIS_CLI_RCFILE_ENV "REDISCLI_RCFILE"
#define REDIS_CLI_RCFILE_DEFAULT ".redisclirc"
#define REDIS_CLI_AUTH_ENV "REDISCLI_AUTH"

#define CLUSTER_MANAGER_SLOTS               16384
#define CLUSTER_MANAGER_MIGRATE_TIMEOUT     60000
#define CLUSTER_MANAGER_MIGRATE_PIPELINE    10
#define CLUSTER_MANAGER_REBALANCE_THRESHOLD 2

#define CLUSTER_MANAGER_INVALID_HOST_ARG \
    "[ERR] Invalid arguments: you need to pass either a valid " \
    "address (ie. 120.0.0.1:7000) or space separated IP " \
    "and port (ie. 120.0.0.1 7000)\n"
#define CLUSTER_MANAGER_MODE() (config.cluster_manager_command.name != NULL)
#define CLUSTER_MANAGER_MASTERS_COUNT(nodes, replicas) (nodes/(replicas + 1))
#define CLUSTER_MANAGER_COMMAND(n,...) \
        (redisCommand(n->context, __VA_ARGS__))

#define CLUSTER_MANAGER_NODE_ARRAY_FREE(array) zfree(array->alloc)

#define CLUSTER_MANAGER_PRINT_REPLY_ERROR(n, err) \
    clusterManagerLogErr("Node %s:%d replied with error:\n%s\n", \
                         n->ip, n->port, err);

#define clusterManagerLogInfo(...) \
    clusterManagerLog(CLUSTER_MANAGER_LOG_LVL_INFO,__VA_ARGS__)

#define clusterManagerLogErr(...) \
    clusterManagerLog(CLUSTER_MANAGER_LOG_LVL_ERR,__VA_ARGS__)

#define clusterManagerLogWarn(...) \
    clusterManagerLog(CLUSTER_MANAGER_LOG_LVL_WARN,__VA_ARGS__)

#define clusterManagerLogOk(...) \
    clusterManagerLog(CLUSTER_MANAGER_LOG_LVL_SUCCESS,__VA_ARGS__)

#define CLUSTER_MANAGER_FLAG_MYSELF     1 << 0
#define CLUSTER_MANAGER_FLAG_SLAVE      1 << 1
#define CLUSTER_MANAGER_FLAG_FRIEND     1 << 2
#define CLUSTER_MANAGER_FLAG_NOADDR     1 << 3
#define CLUSTER_MANAGER_FLAG_DISCONNECT 1 << 4
#define CLUSTER_MANAGER_FLAG_FAIL       1 << 5

#define CLUSTER_MANAGER_CMD_FLAG_FIX            1 << 0
#define CLUSTER_MANAGER_CMD_FLAG_SLAVE          1 << 1
#define CLUSTER_MANAGER_CMD_FLAG_YES            1 << 2
#define CLUSTER_MANAGER_CMD_FLAG_AUTOWEIGHTS    1 << 3
#define CLUSTER_MANAGER_CMD_FLAG_EMPTYMASTER    1 << 4
#define CLUSTER_MANAGER_CMD_FLAG_SIMULATE       1 << 5
#define CLUSTER_MANAGER_CMD_FLAG_REPLACE        1 << 6
#define CLUSTER_MANAGER_CMD_FLAG_COPY           1 << 7
#define CLUSTER_MANAGER_CMD_FLAG_COLOR          1 << 8
#define CLUSTER_MANAGER_CMD_FLAG_CHECK_OWNERS   1 << 9

#define CLUSTER_MANAGER_OPT_GETFRIENDS  1 << 0
#define CLUSTER_MANAGER_OPT_COLD        1 << 1
#define CLUSTER_MANAGER_OPT_UPDATE      1 << 2
#define CLUSTER_MANAGER_OPT_QUIET       1 << 6
#define CLUSTER_MANAGER_OPT_VERBOSE     1 << 7

#define CLUSTER_MANAGER_LOG_LVL_INFO    1
#define CLUSTER_MANAGER_LOG_LVL_WARN    2
#define CLUSTER_MANAGER_LOG_LVL_ERR     3
#define CLUSTER_MANAGER_LOG_LVL_SUCCESS 4

#define CLUSTER_JOIN_CHECK_AFTER        20

#define LOG_COLOR_BOLD      "29;1m"
#define LOG_COLOR_RED       "31;1m"
#define LOG_COLOR_GREEN     "32;1m"
#define LOG_COLOR_YELLOW    "33;1m"
#define LOG_COLOR_RESET     "0m"

/* cliConnect() flags. */
#define CC_FORCE (1<<0)         /* Re-connect if already connected. */
#define CC_QUIET (1<<1)         /* Don't log connecting errors. */

struct clusterManagerLink;
typedef struct clusterManagerLink clusterManagerLink;

/* Dict Helpers */

uint64_t dictSdsHash(const void *key);
int dictSdsKeyCompare(void *privdata, const void *key1,
    const void *key2);
void dictSdsDestructor(void *privdata, void *val);
void dictListDestructor(void *privdata, void *val);

/* Cluster Manager Command Info */
typedef struct clusterManagerCommand {
    char *name;
    int argc;
    char **argv;
    int flags;
    int replicas;
    char *from;
    char *to;
    char **weight;
    int weight_argc;
    char *master_id;
    int slots;
    int timeout;
    int pipeline;
    float threshold;
    char *backup_dir;
} clusterManagerCommand;

void createClusterManagerCommand(char *cmdname, int argc, char **argv);

extern redisContext *context;
extern struct config {
    char *hostip;
    int hostport;
    char *hostsocket;
    int tls;
    char *sni;
    char *cacert;
    char *cacertdir;
    char *cert;
    char *key;
    long repeat;
    long interval;
    int dbnum;
    int interactive;
    int shutdown;
    int monitor_mode;
    int pubsub_mode;
    int latency_mode;
    int latency_dist_mode;
    int latency_history;
    int lru_test_mode;
    long long lru_test_sample_size;
    int cluster_mode;
    int cluster_reissue_command;
    int slave_mode;
    int pipe_mode;
    int pipe_timeout;
    int getrdb_mode;
    int stat_mode;
    int scan_mode;
    int intrinsic_latency_mode;
    int intrinsic_latency_duration;
    char *pattern;
    char *rdb_filename;
    int bigkeys;
    int memkeys;
    unsigned memkeys_samples;
    int hotkeys;
    int stdinarg; /* get last arg from stdin. (-x option) */
    char *auth;
    char *user;
    int output; /* output mode, see OUTPUT_* defines */
    sds mb_delim;
    char prompt[128];
    char *eval;
    int eval_ldb;
    int eval_ldb_sync;  /* Ask for synchronous mode of the Lua debugger. */
    int eval_ldb_end;   /* Lua debugging session ended. */
    int enable_ldb_on_eval; /* Handle manual SCRIPT DEBUG + EVAL commands. */
    int last_cmd_type;
    int verbose;
    clusterManagerCommand cluster_manager_command;
    int no_auth_warning;
    int resp3;
} config;

/* The Cluster Manager global structure */
extern struct clusterManager {
    list *nodes;    /* List of nodes in the configuration. */
    list *errors;
} cluster_manager;

typedef struct clusterManagerNode {
    redisContext *context;
    sds name;
    char *ip;
    int port;
    uint64_t current_epoch;
    time_t ping_sent;
    time_t ping_recv;
    int flags;
    list *flags_str; /* Flags string representations */
    sds replicate;  /* Master ID if node is a replica */
    int dirty;      /* Node has changes that can be flushed */
    uint8_t slots[CLUSTER_MANAGER_SLOTS];
    int slots_count;
    int replicas_count;
    list *friends;
    sds *migrating; /* An array of sds where even strings are slots and odd
                     * strings are the destination node IDs. */
    sds *importing; /* An array of sds where even strings are slots and odd
                     * strings are the source node IDs. */
    int migrating_count; /* Length of the migrating array (migrating slots*2) */
    int importing_count; /* Length of the importing array (importing slots*2) */
    float weight;   /* Weight used by rebalance */
    int balance;    /* Used by rebalance */
} clusterManagerNode;

/* Data structure used to represent a sequence of cluster nodes. */
typedef struct clusterManagerNodeArray {
    clusterManagerNode **nodes; /* Actual nodes array */
    clusterManagerNode **alloc; /* Pointer to the allocated memory */
    int len;                    /* Actual length of the array */
    int count;                  /* Non-NULL nodes count */
} clusterManagerNodeArray;

/* Used for the reshard table. */
typedef struct clusterManagerReshardTableItem {
    clusterManagerNode *source;
    int slot;
} clusterManagerReshardTableItem;

typedef struct typeinfo {
    char *name;
    char *sizecmd;
    char *sizeunit;
    unsigned long long biggest;
    unsigned long long count;
    unsigned long long totalsize;
    sds biggest_key;
} typeinfo;

extern typeinfo type_string;
extern typeinfo type_list;
extern typeinfo type_set;
extern typeinfo type_hash;
extern typeinfo type_zset;
extern typeinfo type_stream;
extern typeinfo type_other;

void findBigKeys(int memkeys, unsigned memkeys_samples);
int clusterManagerGetAntiAffinityScore(clusterManagerNodeArray *ipnodes,
    int ip_count, clusterManagerNode ***offending, int *offending_len);
int clusterManagerFixMultipleSlotOwners(int slot, list *owners);
void getKeySizes(redisReply *keys, struct typeinfo **types,
    unsigned long long *sizes, int memkeys,
    unsigned memkeys_samples);
int clusterManagerFixOpenSlot(int slot);
void clusterManagerPrintSlotsList(list *slots);
int clusterManagerGetCoveredSlots(char *all_slots);
void clusterManagerOnError(sds err);
int clusterManagerIsConfigConsistent(void);
void freeClusterManagerNode(clusterManagerNode *node);
void clusterManagerLog(int level, const char* fmt, ...);
int parseClusterNodeAddress(char *addr, char **ip_ptr, int *port_ptr,
    int *bus_port_ptr);
int clusterManagerCheckRedisReply(clusterManagerNode *n,
    redisReply *r, char **err);
int confirmWithYes(const char *msg);
int clusterManagerSetSlotOwner(clusterManagerNode *owner,
    int slot,
    int do_clear);
clusterManagerNode * clusterManagerGetNodeWithMostKeysInSlot(list *nodes,
    int slot,
    char **err);
int clusterManagerSetSlot(clusterManagerNode *node1,
    clusterManagerNode *node2,
    int slot, const char *status, char **err);
int clusterManagerMoveSlot(clusterManagerNode *source,
    clusterManagerNode *target,
    int slot, int opts,  char**err);
int clusterManagerClearSlotStatus(clusterManagerNode *node, int slot);
void clusterManagerShowNodes(void);
signed int clusterManagerCountKeysInSlot(clusterManagerNode *node,
    int slot);
void type_free(void* priv_data, void* val);
int getDbSize(void);
redisReply *sendScan(unsigned long long *it);

#ifdef __cplusplus
}
#endif

