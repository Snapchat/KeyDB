/* Configuration file parsing and CONFIG GET/SET commands implementation.
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
 */

#include "server.h"
#include "storage/rocksdbfactory.h"
#include "storage/teststorageprovider.h"
#include "cluster.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/wait.h>
#ifdef __linux__
#include <sys/sysinfo.h>
#endif

const char *KEYDB_SET_VERSION = KEYDB_REAL_VERSION;
size_t g_semiOrderedSetTargetBucketSize = 0;    // Its a header only class so nowhere else for this to go

/*-----------------------------------------------------------------------------
 * Config file name-value maps.
 *----------------------------------------------------------------------------*/

typedef struct configEnum {
    const char *name;
    const int val;
} configEnum;

configEnum maxmemory_policy_enum[] = {
    {"volatile-lru", MAXMEMORY_VOLATILE_LRU},
    {"volatile-lfu", MAXMEMORY_VOLATILE_LFU},
    {"volatile-random",MAXMEMORY_VOLATILE_RANDOM},
    {"volatile-ttl",MAXMEMORY_VOLATILE_TTL},
    {"allkeys-lru",MAXMEMORY_ALLKEYS_LRU},
    {"allkeys-lfu",MAXMEMORY_ALLKEYS_LFU},
    {"allkeys-random",MAXMEMORY_ALLKEYS_RANDOM},
    {"noeviction",MAXMEMORY_NO_EVICTION},
    {NULL, 0}
};

configEnum syslog_facility_enum[] = {
    {"user",    LOG_USER},
    {"local0",  LOG_LOCAL0},
    {"local1",  LOG_LOCAL1},
    {"local2",  LOG_LOCAL2},
    {"local3",  LOG_LOCAL3},
    {"local4",  LOG_LOCAL4},
    {"local5",  LOG_LOCAL5},
    {"local6",  LOG_LOCAL6},
    {"local7",  LOG_LOCAL7},
    {NULL, 0}
};

configEnum loglevel_enum[] = {
    {"debug", LL_DEBUG},
    {"verbose", LL_VERBOSE},
    {"notice", LL_NOTICE},
    {"warning", LL_WARNING},
    {NULL,0}
};

configEnum supervised_mode_enum[] = {
    {"upstart", SUPERVISED_UPSTART},
    {"systemd", SUPERVISED_SYSTEMD},
    {"auto", SUPERVISED_AUTODETECT},
    {"no", SUPERVISED_NONE},
    {NULL, 0}
};

configEnum aof_fsync_enum[] = {
    {"everysec", AOF_FSYNC_EVERYSEC},
    {"always", AOF_FSYNC_ALWAYS},
    {"no", AOF_FSYNC_NO},
    {NULL, 0}
};

configEnum repl_diskless_load_enum[] = {
    {"disabled", REPL_DISKLESS_LOAD_DISABLED},
    {"on-empty-db", REPL_DISKLESS_LOAD_WHEN_DB_EMPTY},
    {"swapdb", REPL_DISKLESS_LOAD_SWAPDB},
    {NULL, 0}
};

configEnum storage_memory_model_enum[] = {
    {"writeback", STORAGE_WRITEBACK},
    {"writethrough", STORAGE_WRITETHROUGH},
};

configEnum tls_auth_clients_enum[] = {
    {"no", TLS_CLIENT_AUTH_NO},
    {"yes", TLS_CLIENT_AUTH_YES},
    {"optional", TLS_CLIENT_AUTH_OPTIONAL},
    {NULL, 0}
};

configEnum oom_score_adj_enum[] = {
    {"no", OOM_SCORE_ADJ_NO},
    {"yes", OOM_SCORE_RELATIVE},
    {"relative", OOM_SCORE_RELATIVE},
    {"absolute", OOM_SCORE_ADJ_ABSOLUTE},
    {NULL, 0}
};

configEnum acl_pubsub_default_enum[] = {
    {"allchannels", USER_FLAG_ALLCHANNELS},
    {"resetchannels", 0},
    {NULL, 0}
};

configEnum sanitize_dump_payload_enum[] = {
    {"no", SANITIZE_DUMP_NO},
    {"yes", SANITIZE_DUMP_YES},
    {"clients", SANITIZE_DUMP_CLIENTS},
    {NULL, 0}
};

/* Output buffer limits presets. */
clientBufferLimitsConfig clientBufferLimitsDefaults[CLIENT_TYPE_OBUF_COUNT] = {
    {0, 0, 0}, /* normal */
    {1024*1024*256, 1024*1024*64, 60}, /* replica */
    {1024*1024*32, 1024*1024*8, 60}  /* pubsub */
};

/* OOM Score defaults */
int configOOMScoreAdjValuesDefaults[CONFIG_OOM_COUNT] = { 0, 200, 800 };

/* Generic config infrastructure function pointers
 * int is_valid_fn(val, err)
 *     Return 1 when val is valid, and 0 when invalid.
 *     Optionally set err to a static error string.
 * int update_fn(val, prev, err)
 *     This function is called only for CONFIG SET command (not at config file parsing)
 *     It is called after the actual config is applied,
 *     Return 1 for success, and 0 for failure.
 *     Optionally set err to a static error string.
 *     On failure the config change will be reverted.
 */

/* Configuration values that require no special handling to set, get, load or
 * rewrite. */
typedef struct boolConfigData {
    int *config; /* The pointer to the server config this value is stored in */
    int default_value; /* The default value of the config on rewrite */
    int (*is_valid_fn)(int val, const char **err); /* Optional function to check validity of new value (generic doc above) */
    int (*update_fn)(int val, int prev, const char **err); /* Optional function to apply new value at runtime (generic doc above) */
} boolConfigData;

typedef struct stringConfigData {
    char **config; /* Pointer to the server config this value is stored in. */
    const char *default_value; /* Default value of the config on rewrite. */
    int (*is_valid_fn)(char* val, const char **err); /* Optional function to check validity of new value (generic doc above) */
    int (*update_fn)(char* val, char* prev, const char **err); /* Optional function to apply new value at runtime (generic doc above) */
    int convert_empty_to_null; /* Boolean indicating if empty strings should
                                  be stored as a NULL value. */
} stringConfigData;

typedef struct sdsConfigData {
    sds *config; /* Pointer to the server config this value is stored in. */
    const char *default_value; /* Default value of the config on rewrite. */
    int (*is_valid_fn)(sds val, const char **err); /* Optional function to check validity of new value (generic doc above) */
    int (*update_fn)(sds val, sds prev, const char **err); /* Optional function to apply new value at runtime (generic doc above) */
    int convert_empty_to_null; /* Boolean indicating if empty SDS strings should
                                  be stored as a NULL value. */
} sdsConfigData;

typedef struct enumConfigData {
    int *config; /* The pointer to the server config this value is stored in */
    configEnum *enum_value; /* The underlying enum type this data represents */
    int default_value; /* The default value of the config on rewrite */
    int (*is_valid_fn)(int val, const char **err); /* Optional function to check validity of new value (generic doc above) */
    int (*update_fn)(int val, int prev, const char **err); /* Optional function to apply new value at runtime (generic doc above) */
} enumConfigData;

typedef enum numericType {
    NUMERIC_TYPE_INT,
    NUMERIC_TYPE_UINT,
    NUMERIC_TYPE_LONG,
    NUMERIC_TYPE_ULONG,
    NUMERIC_TYPE_LONG_LONG,
    NUMERIC_TYPE_ULONG_LONG,
    NUMERIC_TYPE_SIZE_T,
    NUMERIC_TYPE_SSIZE_T,
    NUMERIC_TYPE_OFF_T,
    NUMERIC_TYPE_TIME_T,
} numericType;

typedef struct numericConfigData {
    int is_memory; /* Indicates if this value can be loaded as a memory value */
    long long lower_bound; /* The lower bound of this numeric value */
    long long upper_bound; /* The upper bound of this numeric value */
    long long default_value; /* The default value of the config on rewrite */
    int (*is_valid_fn)(long long val, const char **err); /* Optional function to check validity of new value (generic doc above) */
    int (*update_fn)(long long val, long long prev, const char **err); /* Optional function to apply new value at runtime (generic doc above) */
    numericType numeric_type; /* An enum indicating the type of this value */
    union {
        int *i;
        unsigned int *ui;
        long *l;
        unsigned long *ul;
        long long *ll;
        unsigned long long *ull;
        size_t *st;
        ssize_t *sst;
        off_t *ot;
        time_t *tt;
    } config; /* The pointer to the numeric config this value is stored in */
} numericConfigData;

typedef union typeData {
    boolConfigData yesno;
    stringConfigData string;
    sdsConfigData sds;
    enumConfigData enumd;
    numericConfigData numeric;
} typeData;

typedef struct typeInterface {
    /* Called on server start, to init the server with default value */
    void (*init)(typeData data);
    /* Called on server startup and CONFIG SET, returns 1 on success, 0 on error
     * and can set a verbose err string, update is true when called from CONFIG SET */
    int (*set)(typeData data, sds value, int update, const char **err);
    /* Called on CONFIG GET, required to add output to the client */
    void (*get)(client *c, typeData data);
    /* Called on CONFIG REWRITE, required to rewrite the config state */
    void (*rewrite)(typeData data, const char *name, struct rewriteConfigState *state);
} typeInterface;

typedef struct standardConfig {
    const char *name; /* The user visible name of this config */
    const char *alias; /* An alias that can also be used for this config */
    const unsigned int flags; /* Flags for this specific config */
    typeInterface interface; /* The function pointers that define the type interface */
    typeData data; /* The type specific data exposed used by the interface */
} standardConfig;

#define MODIFIABLE_CONFIG 0 /* This is the implied default for a standard 
                             * config, which is mutable. */
#define IMMUTABLE_CONFIG (1ULL<<0) /* Can this value only be set at startup? */
#define SENSITIVE_CONFIG (1ULL<<1) /* Does this value contain sensitive information */

extern standardConfig configs[];

/*-----------------------------------------------------------------------------
 * Enum access functions
 *----------------------------------------------------------------------------*/

/* Get enum value from name. If there is no match INT_MIN is returned. */
int configEnumGetValue(configEnum *ce, char *name) {
    while(ce->name != NULL) {
        if (!strcasecmp(ce->name,name)) return ce->val;
        ce++;
    }
    return INT_MIN;
}

/* Get enum name from value. If no match is found NULL is returned. */
const char *configEnumGetName(configEnum *ce, int val) {
    while(ce->name != NULL) {
        if (ce->val == val) return ce->name;
        ce++;
    }
    return NULL;
}

/* Wrapper for configEnumGetName() returning "unknown" instead of NULL if
 * there is no match. */
const char *configEnumGetNameOrUnknown(configEnum *ce, int val) {
    const char *name = configEnumGetName(ce,val);
    return name ? name : "unknown";
}

/* Used for INFO generation. */
const char *evictPolicyToString(void) {
    return configEnumGetNameOrUnknown(maxmemory_policy_enum,g_pserver->maxmemory_policy);
}

/*-----------------------------------------------------------------------------
 * Config file parsing
 *----------------------------------------------------------------------------*/

int truefalsetoi(char *s) {
    if (!strcasecmp(s,"true")) return 1;
    else if (!strcasecmp(s,"false")) return 0;
    else return -1;
}

int yesnotoi(char *s) {
    if (!strcasecmp(s,"yes")) return 1;
    else if (!strcasecmp(s,"no")) return 0;
    else return truefalsetoi(s);
}


void appendServerSaveParams(time_t seconds, int changes) {
    g_pserver->saveparams = (saveparam*)zrealloc(g_pserver->saveparams,sizeof(struct saveparam)*(g_pserver->saveparamslen+1), MALLOC_LOCAL);
    g_pserver->saveparams[g_pserver->saveparamslen].seconds = seconds;
    g_pserver->saveparams[g_pserver->saveparamslen].changes = changes;
    g_pserver->saveparamslen++;
}

void resetServerSaveParams(void) {
    zfree(g_pserver->saveparams);
    g_pserver->saveparams = NULL;
    g_pserver->saveparamslen = 0;
}

void queueLoadModule(sds path, sds *argv, int argc) {
    int i;
    struct moduleLoadQueueEntry *loadmod;

    loadmod = (moduleLoadQueueEntry*)zmalloc(sizeof(struct moduleLoadQueueEntry), MALLOC_LOCAL);
    loadmod->argv = (robj**)zmalloc(sizeof(robj*)*argc, MALLOC_LOCAL);
    loadmod->path = sdsnew(path);
    loadmod->argc = argc;
    for (i = 0; i < argc; i++) {
        loadmod->argv[i] = createRawStringObject(argv[i],sdslen(argv[i]));
    }
    listAddNodeTail(g_pserver->loadmodule_queue,loadmod);
}

sds g_sdsProvider = nullptr;
sds g_sdsArgs = nullptr;

bool initializeStorageProvider(const char **err)
{
    try
    {
        bool fTest = false;
        if (g_sdsProvider == nullptr)
            return true;
        if (!strcasecmp(g_sdsProvider, "flash") && g_sdsArgs != nullptr)
        {
#ifdef ENABLE_ROCKSDB
            // Create The Storage Factory (if necessary)
            serverLog(LL_NOTICE, "Initializing FLASH storage provider (this may take a long time)");
            adjustOpenFilesLimit();
            g_pserver->m_pstorageFactory = CreateRocksDBStorageFactory(g_sdsArgs, cserver.dbnum, cserver.storage_conf, cserver.storage_conf ? strlen(cserver.storage_conf) : 0);
#else
            serverLog(LL_WARNING, "To use the flash storage provider please compile KeyDB with ENABLE_FLASH=yes");
            serverLog(LL_WARNING, "Exiting due to the use of an unsupported storage provider");
            exit(EXIT_FAILURE);
#endif
	    }
        else if (!strcasecmp(g_sdsProvider, "test") && g_sdsArgs == nullptr)
        {
            g_pserver->m_pstorageFactory = new (MALLOC_LOCAL) TestStorageFactory();
            fTest = true;
        }

        if (g_pserver->m_pstorageFactory != nullptr && !fTest)
        {
            // We need to set max memory to a sane default so keys are actually evicted properly
            if (g_pserver->maxmemory == 0 && g_pserver->maxmemory_policy == MAXMEMORY_NO_EVICTION)
            {
#ifdef __linux__
                struct sysinfo sys;
                if (sysinfo(&sys) == 0)
                {
                    // By default it's a little under half the memory.  This gives sufficient room for background saving
                    g_pserver->maxmemory = static_cast<unsigned long long>(sys.totalram / 2.2);
                    g_pserver->maxmemory_policy = MAXMEMORY_ALLKEYS_LRU;
                }
#else
                serverLog(LL_WARNING, "Unable to dynamically set maxmemory, please set maxmemory and maxmemory-policy if you are using a storage provier");
#endif
            }
            else if (g_pserver->maxmemory_policy == MAXMEMORY_NO_EVICTION)
            {
                g_pserver->maxmemory_policy = MAXMEMORY_ALLKEYS_LRU;
            }
        }
        else
        {
            *err = "Unknown storage provider";
        }
        return g_pserver->m_pstorageFactory != nullptr;
    }
    catch(std::string str)
    {
        serverLog(LL_WARNING, "ERROR: Failed to initialize %s storage provider. Details to follow below.", g_sdsProvider);
        serverLog(LL_WARNING, "\t%s", str.c_str());
        serverLog(LL_WARNING, "KeyDB cannot start. Exiting.");
        exit(EXIT_FAILURE);
    }
}

/* Parse an array of CONFIG_OOM_COUNT sds strings, validate and populate
 * g_pserver->oom_score_adj_values if valid.
 */

static int updateOOMScoreAdjValues(sds *args, const char **err, int apply) {
    int i;
    int values[CONFIG_OOM_COUNT];

    for (i = 0; i < CONFIG_OOM_COUNT; i++) {
        char *eptr;
        long long val = strtoll(args[i], &eptr, 10);

        if (*eptr != '\0' || val < -2000 || val > 2000) {
            if (err) *err = "Invalid oom-score-adj-values, elements must be between -2000 and 2000.";
            return C_ERR;
        }

        values[i] = val;
    }

    /* Verify that the values make sense. If they don't omit a warning but
     * keep the configuration, which may still be valid for privileged processes.
     */

    if (values[CONFIG_OOM_REPLICA] < values[CONFIG_OOM_MASTER] ||
        values[CONFIG_OOM_BGCHILD] < values[CONFIG_OOM_REPLICA]) {
            serverLog(LL_WARNING,
                    "The oom-score-adj-values configuration may not work for non-privileged processes! "
                    "Please consult the documentation.");
    }

    /* Store values, retain previous config for rollback in case we fail. */
    int old_values[CONFIG_OOM_COUNT];
    for (i = 0; i < CONFIG_OOM_COUNT; i++) {
        old_values[i] = g_pserver->oom_score_adj_values[i];
        g_pserver->oom_score_adj_values[i] = values[i];
    }
    
    /* When parsing the config file, we want to apply only when all is done. */
    if (!apply)
        return C_OK;

    /* Update */
    if (setOOMScoreAdj(-1) == C_ERR) {
        /* Roll back */
        for (i = 0; i < CONFIG_OOM_COUNT; i++)
            g_pserver->oom_score_adj_values[i] = old_values[i];

        if (err)
            *err = "Failed to apply oom-score-adj-values configuration, check server logs.";

        return C_ERR;
    }

    return C_OK;
}

void initConfigValues() {
    for (standardConfig *config = configs; config->name != NULL; config++) {
        config->interface.init(config->data);
    }
}

void loadServerConfigFromString(char *config) {
    const char *err = NULL;
    int linenum = 0, totlines, i;
    int slaveof_linenum = 0;
    sds *lines;
    int save_loaded = 0;

    lines = sdssplitlen(config,strlen(config),"\n",1,&totlines);

    for (i = 0; i < totlines; i++) {
        sds *argv;
        int argc;

        linenum = i+1;
        lines[i] = sdstrim(lines[i]," \t\r\n");

        /* Skip comments and blank lines */
        if (lines[i][0] == '#' || lines[i][0] == '\0') continue;

        /* Split into arguments */
        argv = sdssplitargs(lines[i],&argc);
        if (argv == NULL) {
            err = "Unbalanced quotes in configuration line";
            goto loaderr;
        }

        /* Skip this line if the resulting command vector is empty. */
        if (argc == 0) {
            sdsfreesplitres(argv,argc);
            continue;
        }
        sdstolower(argv[0]);

        /* Iterate the configs that are standard */
        int match = 0;
        for (standardConfig *config = configs; config->name != NULL; config++) {
            if ((!strcasecmp(argv[0],config->name) ||
                (config->alias && !strcasecmp(argv[0],config->alias))))
            {
                if (argc != 2) {
                    err = "wrong number of arguments";
                    goto loaderr;
                }
                if (!config->interface.set(config->data, argv[1], 0, &err)) {
                    goto loaderr;
                }

                match = 1;
                break;
            }
        }

        if (match) {
            sdsfreesplitres(argv,argc);
            continue;
        }

        /* Execute config directives */
        if (!strcasecmp(argv[0],"bind") && argc >= 2) {
            int j, addresses = argc-1;

            if (addresses > CONFIG_BINDADDR_MAX) {
                err = "Too many bind addresses specified"; goto loaderr;
            }
            /* Free old bind addresses */
            for (j = 0; j < g_pserver->bindaddr_count; j++) {
                zfree(g_pserver->bindaddr[j]);
            }
            for (j = 0; j < addresses; j++)
                g_pserver->bindaddr[j] = zstrdup(argv[j+1]);
            g_pserver->bindaddr_count = addresses;
        } else if (!strcasecmp(argv[0],"unixsocketperm") && argc == 2) {
            errno = 0;
            g_pserver->unixsocketperm = (mode_t)strtol(argv[1], NULL, 8);
            if (errno || g_pserver->unixsocketperm > 0777) {
                err = "Invalid socket file permissions"; goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"save")) {
            /* We don't reset save params before loading, because if they're not part
             * of the file the defaults should be used.
             */
            if (!save_loaded) {
                save_loaded = 1;
                resetServerSaveParams();
            }

            if (argc == 3) {
                int seconds = atoi(argv[1]);
                int changes = atoi(argv[2]);
                if (seconds < 1 || changes < 0) {
                    err = "Invalid save parameters"; goto loaderr;
                }
                appendServerSaveParams(seconds,changes);
            } else if (argc == 2 && !strcasecmp(argv[1],"")) {
                resetServerSaveParams();
            }
        } else if (!strcasecmp(argv[0],"dir") && argc == 2) {
            if (chdir(argv[1]) == -1) {
                serverLog(LL_WARNING,"Can't chdir to '%s': %s",
                    argv[1], strerror(errno));
                exit(1);
            }
        } else if (!strcasecmp(argv[0],"logfile") && argc == 2) {
            FILE *logfp;

            zfree(g_pserver->logfile);
            g_pserver->logfile = zstrdup(argv[1]);
            if (g_pserver->logfile[0] != '\0') {
                /* Test if we are able to open the file. The server will not
                 * be able to abort just for this problem later... */
                logfp = fopen(g_pserver->logfile,"a");
                if (logfp == NULL) {
                    err = sdscatprintf(sdsempty(),
                        "Can't open the log file: %s", strerror(errno));
                    goto loaderr;
                }
                fclose(logfp);
            }
        } else if (!strcasecmp(argv[0],"include") && argc == 2) {
            loadServerConfig(argv[1], 0, NULL);
        } else if ((!strcasecmp(argv[0],"slaveof") ||
                    !strcasecmp(argv[0],"replicaof")) && argc == 3) {
            slaveof_linenum = linenum;
            if (!strcasecmp(argv[1], "no") && !strcasecmp(argv[2], "one")) {
                if (listLength(g_pserver->masters)) {
                    listIter li;
                    listNode *ln;
                    listRewind(g_pserver->masters, &li);
                    while ((ln = listNext(&li)))
                    {
                        struct redisMaster *mi = (struct redisMaster*)listNodeValue(ln);
                        sdsfree(mi->masterauth);
                        zfree(mi->masteruser);
                        zfree(mi->repl_transfer_tmpfile);
                        delete mi->staleKeyMap;
                        zfree(mi);
                        listDelNode(g_pserver->masters, ln);
                    }
                }
                continue;
            }
            char *ptr;
            int port = strtol(argv[2], &ptr, 10);
            if (port < 0 || port > 65535 || *ptr != '\0') {
                err= "Invalid master port"; goto loaderr;
            }
            replicationAddMaster(argv[1], port);
        } else if (!strcasecmp(argv[0],"requirepass") && argc == 2) {
            if (strlen(argv[1]) > CONFIG_AUTHPASS_MAX_LEN) {
                err = "Password is longer than CONFIG_AUTHPASS_MAX_LEN";
                goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"list-max-ziplist-entries") && argc == 2){
            /* DEAD OPTION */
        } else if (!strcasecmp(argv[0],"list-max-ziplist-value") && argc == 2) {
            /* DEAD OPTION */
        } else if (!strcasecmp(argv[0],"rename-command") && argc == 3) {
            struct redisCommand *cmd = lookupCommand(argv[1]);
            int retval;

            if (!cmd) {
                err = "No such command in rename-command";
                goto loaderr;
            }

            /* If the target command name is the empty string we just
             * remove it from the command table. */
            retval = dictDelete(g_pserver->commands, argv[1]);
            serverAssert(retval == DICT_OK);

            /* Otherwise we re-add the command under a different name. */
            if (sdslen(argv[2]) != 0) {
                sds copy = sdsdup(argv[2]);

                retval = dictAdd(g_pserver->commands, copy, cmd);
                if (retval != DICT_OK) {
                    sdsfree(copy);
                    err = "Target command name already exists"; goto loaderr;
                }
            }
        } else if (!strcasecmp(argv[0],"cluster-config-file") && argc == 2) {
            zfree(g_pserver->cluster_configfile);
            g_pserver->cluster_configfile = zstrdup(argv[1]);
        } else if (!strcasecmp(argv[0],"client-output-buffer-limit") &&
                   argc == 5)
        {
            int type = getClientTypeByName(argv[1]);
            unsigned long long hard, soft;
            int soft_seconds;

            if (type == -1 || type == CLIENT_TYPE_MASTER) {
                err = "Unrecognized client limit class: the user specified "
                "an invalid one, or 'master' which has no buffer limits.";
                goto loaderr;
            }
            hard = memtoll(argv[2],NULL);
            soft = memtoll(argv[3],NULL);
            soft_seconds = atoi(argv[4]);
            if (soft_seconds < 0) {
                err = "Negative number of seconds in soft limit is invalid";
                goto loaderr;
            }
            cserver.client_obuf_limits[type].hard_limit_bytes = hard;
            cserver.client_obuf_limits[type].soft_limit_bytes = soft;
            cserver.client_obuf_limits[type].soft_limit_seconds = soft_seconds;
        } else if (!strcasecmp(argv[0],"oom-score-adj-values") && argc == 1 + CONFIG_OOM_COUNT) {
            if (updateOOMScoreAdjValues(&argv[1], &err, 0) == C_ERR) goto loaderr;
        } else if (!strcasecmp(argv[0],"notify-keyspace-events") && argc == 2) {
            int flags = keyspaceEventsStringToFlags(argv[1]);

            if (flags == -1) {
                err = "Invalid event class character. Use 'g$lshzxeA'.";
                goto loaderr;
            }
            g_pserver->notify_keyspace_events = flags;
        } else if (!strcasecmp(argv[0],"user") && argc >= 2) {
            int argc_err;
            if (ACLAppendUserForLoading(argv,argc,&argc_err) == C_ERR) {
                char buf[1024];
                const char *errmsg = ACLSetUserStringError();
                snprintf(buf,sizeof(buf),"Error in user declaration '%s': %s",
                    argv[argc_err],errmsg);
                err = buf;
                goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"loadmodule") && argc >= 2) {
            queueLoadModule(argv[1],&argv[2],argc-2);
        } else if (!strcasecmp(argv[0],"sentinel")) {
            /* argc == 1 is handled by main() as we need to enter the sentinel
             * mode ASAP. */
            if (argc != 1) {
                if (!g_pserver->sentinel_mode) {
                    err = "sentinel directive while not in sentinel mode";
                    goto loaderr;
                }
                queueSentinelConfig(argv+1,argc-1,linenum,lines[i]);
            }
        } else if (!strcasecmp(argv[0],"scratch-file-path")) {
#ifdef USE_MEMKIND
            storage_init(argv[1], g_pserver->maxmemory);
#else
            err = "KeyDB not compliled with scratch-file support.";
            goto loaderr;
#endif
        } else if ((!strcasecmp(argv[0],"server-threads") || !strcasecmp(argv[0],"io-threads")) && argc == 2) {
            cserver.cthreads = atoi(argv[1]);
            if (cserver.cthreads <= 0 || cserver.cthreads > MAX_EVENT_LOOPS) {
                err = "Invalid number of threads specified";
                goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"server-thread-affinity") && argc == 2) {
            if (strcasecmp(argv[1], "true") == 0) {
                cserver.fThreadAffinity = TRUE;
            } else if (strcasecmp(argv[1], "false") == 0) {
                cserver.fThreadAffinity = FALSE;
            } else {
                int offset = atoi(argv[1]);
                if (offset > 0) {
                    cserver.fThreadAffinity = TRUE;
                    cserver.threadAffinityOffset = offset-1;
                } else {
                    err = "Unknown argument: server-thread-affinity expects either true or false";
                    goto loaderr;
                }
            }
        } else if (!strcasecmp(argv[0], "active-replica") && argc == 2) {
            g_pserver->fActiveReplica = yesnotoi(argv[1]);
            if (g_pserver->fActiveReplica && g_pserver->repl_slave_ro) {
                g_pserver->repl_slave_ro = FALSE;
                serverLog(LL_NOTICE, "Notice: \"active-replica yes\" implies \"replica-read-only no\"");
            }
            if (g_pserver->fActiveReplica == -1) {
                g_pserver->fActiveReplica = CONFIG_DEFAULT_ACTIVE_REPLICA;
                err = "argument must be 'yes' or 'no'"; goto loaderr;
            }
            if (listLength(g_pserver->masters) && g_pserver->fActiveReplica) {
                err = "must not set replica-of config before active-replica config"; goto loaderr;
            }
        } else if (!strcasecmp(argv[0], "multi-master") && argc == 2) {
            g_pserver->enable_multimaster = yesnotoi(argv[1]);
            if (g_pserver->enable_multimaster == -1) {
                g_pserver->enable_multimaster = CONFIG_DEFAULT_ENABLE_MULTIMASTER;
                err = "argument must be 'yes' or 'no'"; goto loaderr;
            }
            if (listLength(g_pserver->masters) && g_pserver->enable_multimaster) {
                err = "must not set replica-of config before multi-master config"; goto loaderr;
            }
        } else if (!strcasecmp(argv[0], "tls-allowlist")) {
            if (argc < 2) {
                err = "must supply at least one element in the allow list"; goto loaderr;
            }
            if (!g_pserver->tls_allowlist.empty()) {
                err = "tls-allowlist may only be set once"; goto loaderr;
            }
            for (int i = 1; i < argc; i++)
                g_pserver->tls_allowlist.emplace(argv[i], strlen(argv[i]));
        } else if (!strcasecmp(argv[0], "tls-auditlog-blocklist")) {
            if (argc < 2) {
                err = "must supply at least one element in the block list"; goto loaderr;
            }
            if (!g_pserver->tls_auditlog_blocklist.empty()) {
                err = "tls-auditlog-blocklist may only be set once"; goto loaderr;
            }
            for (int i = 1; i < argc; i++)
                g_pserver->tls_auditlog_blocklist.emplace(argv[i], strlen(argv[i]));
        } else if (!strcasecmp(argv[0], "version-override") && argc == 2) {
            KEYDB_SET_VERSION = zstrdup(argv[1]);
            serverLog(LL_WARNING, "Warning version is overriden to: %s\n", KEYDB_SET_VERSION);
        } else if (!strcasecmp(argv[0],"testmode") && argc == 2){
            g_fTestMode = yesnotoi(argv[1]);
        } else if (!strcasecmp(argv[0],"rdbfuzz-mode")) {
            // NOP, handled in main
        } else if (!strcasecmp(argv[0],"storage-provider") && argc >= 2) {
            g_sdsProvider = sdsdup(argv[1]);
            if (argc > 2)
                g_sdsArgs = sdsdup(argv[2]);
	    } else if (!strcasecmp(argv[0],"is-flash-enabled") && argc == 1) {
#ifdef ENABLE_ROCKSDB
            exit(EXIT_SUCCESS);
#else
            exit(EXIT_FAILURE);
#endif
        } else {
            err = "Bad directive or wrong number of arguments"; goto loaderr;
        }
        sdsfreesplitres(argv,argc);
    }

    /* Sanity checks. */
    if (g_pserver->cluster_enabled && listLength(g_pserver->masters)) {
        linenum = slaveof_linenum;
        i = linenum-1;
        err = "replicaof directive not allowed in cluster mode";
        goto loaderr;
    }

    /* To ensure backward compatibility and work while hz is out of range */
    if (g_pserver->config_hz < CONFIG_MIN_HZ) g_pserver->config_hz = CONFIG_MIN_HZ;
    if (g_pserver->config_hz > CONFIG_MAX_HZ) g_pserver->config_hz = CONFIG_MAX_HZ;

    sdsfreesplitres(lines,totlines);
    return;

loaderr:
    fprintf(stderr, "\n*** FATAL CONFIG FILE ERROR (KeyDB %s) ***\n",
        KEYDB_REAL_VERSION);
    fprintf(stderr, "Reading the configuration file, at line %d\n", linenum);
    fprintf(stderr, ">>> '%s'\n", lines[i]);
    fprintf(stderr, "%s\n", err);
    exit(1);
}

/* Load the server configuration from the specified filename.
 * The function appends the additional configuration directives stored
 * in the 'options' string to the config file before loading.
 *
 * Both filename and options can be NULL, in such a case are considered
 * empty. This way loadServerConfig can be used to just load a file or
 * just load a string. */
void loadServerConfig(char *filename, char config_from_stdin, char *options) {
    sds config = sdsempty();
    char buf[CONFIG_MAX_LINE+1];
    FILE *fp;

    /* Load the file content */
    if (filename) {
        if ((fp = fopen(filename,"r")) == NULL) {
            serverLog(LL_WARNING,
                    "Fatal error, can't open config file '%s': %s",
                    filename, strerror(errno));
            exit(1);
        }
        while(fgets(buf,CONFIG_MAX_LINE+1,fp) != NULL)
            config = sdscat(config,buf);
        fclose(fp);
    }
    /* Append content from stdin */
    if (config_from_stdin) {
        serverLog(LL_WARNING,"Reading config from stdin");
        fp = stdin;
        while(fgets(buf,CONFIG_MAX_LINE+1,fp) != NULL)
            config = sdscat(config,buf);
    }

    /* Append the additional options */
    if (options) {
        config = sdscat(config,"\n");
        config = sdscat(config,options);
    }
    loadServerConfigFromString(config);
    sdsfree(config);
}

/*-----------------------------------------------------------------------------
 * CONFIG SET implementation
 *----------------------------------------------------------------------------*/

#define config_set_bool_field(_name,_var) \
    } else if (!strcasecmp(szFromObj(c->argv[2]),_name)) { \
        int yn = yesnotoi(szFromObj(o)); \
        if (yn == -1) goto badfmt; \
        _var = yn;

#define config_set_numerical_field(_name,_var,min,max) \
    } else if (!strcasecmp(szFromObj(c->argv[2]),_name)) { \
        if (getLongLongFromObject(o,&ll) == C_ERR) goto badfmt; \
        if (min != LLONG_MIN && ll < min) goto badfmt; \
        if (max != LLONG_MAX && ll > max) goto badfmt; \
        _var = ll;

#define config_set_memory_field(_name,_var) \
    } else if (!strcasecmp(szFromObj(c->argv[2]),_name)) { \
        ll = memtoll(szFromObj(o),&err); \
        if (err || ll < 0) goto badfmt; \
        _var = ll;

#define config_set_special_field(_name) \
    } else if (!strcasecmp(szFromObj(c->argv[2]),_name)) {

#define config_set_special_field_with_alias(_name1,_name2) \
    } else if (!strcasecmp(szFromObj(c->argv[2]),_name1) || \
               !strcasecmp(szFromObj(c->argv[2]),_name2)) {

#define config_set_else } else

void configSetCommand(client *c) {
    robj *o;
    long long ll;
    int err;
    const char *errstr = NULL;
    serverAssertWithInfo(c,c->argv[2],sdsEncodedObject(c->argv[2]));

    if (c->argc < 4 || c->argc > 4) {
        o = nullptr;
        // Variadic set is only supported for tls-allowlist
        if (strcasecmp(szFromObj(c->argv[2]), "tls-allowlist")) {
            addReplySubcommandSyntaxError(c);
            return;
        }
    } else {
        o = c->argv[3];
        serverAssertWithInfo(c,c->argv[3],sdsEncodedObject(c->argv[3]));
    }

    /* Iterate the configs that are standard */
    for (standardConfig *config = configs; config->name != NULL; config++) {
        if (!(config->flags & IMMUTABLE_CONFIG) && 
            (!strcasecmp(szFromObj(c->argv[2]),config->name) ||
            (config->alias && !strcasecmp(szFromObj(c->argv[2]),config->alias))))
        {
            if (config->flags & SENSITIVE_CONFIG) {
                redactClientCommandArgument(c,3);
            }
            if (!config->interface.set(config->data,szFromObj(o),1,&errstr)) {
                goto badfmt;
            }
            addReply(c,shared.ok);
            return;
        }
    }

    if (0) { /* this starts the config_set macros else-if chain. */

    /* Special fields that can't be handled with general macros. */
    config_set_special_field("bind") {
        int vlen;
        sds *v = sdssplitlen(szFromObj(o),sdslen(szFromObj(o))," ",1,&vlen);

        if (vlen < 1 || vlen > CONFIG_BINDADDR_MAX) {
            addReplyError(c, "Too many bind addresses specified.");
            sdsfreesplitres(v, vlen);
            return;
        }

        if (changeBindAddr(v, vlen, true) == C_ERR) {
            addReplyError(c, "Failed to bind to specified addresses.");
            sdsfreesplitres(v, vlen);
            return;
        }
        // Now run the config change on the other threads
        for (int ithread = 0; ithread < cserver.cthreads; ++ithread) {
            if (&g_pserver->rgthreadvar[ithread] != serverTL) {
                incrRefCount(o);
                aePostFunction(g_pserver->rgthreadvar[ithread].el, [o]{
                    int vlen;
                    sds *v = sdssplitlen(szFromObj(o),sdslen(szFromObj(o))," ",1,&vlen);
                    if (changeBindAddr(v, vlen, false) == C_ERR) {
                        serverLog(LL_WARNING, "Failed to change the bind address for a thread.  Server will still be listening on old addresses.");
                    }
                    sdsfreesplitres(v, vlen);
                    decrRefCount(o);
                });
            }
        }

        sdsfreesplitres(v, vlen);
    } config_set_special_field("save") {
        int vlen, j;
        sds *v = sdssplitlen(szFromObj(o),sdslen(szFromObj(o))," ",1,&vlen);

        /* Perform sanity check before setting the new config:
         * - Even number of args
         * - Seconds >= 1, changes >= 0 */
        if (vlen & 1) {
            sdsfreesplitres(v,vlen);
            goto badfmt;
        }
        for (j = 0; j < vlen; j++) {
            char *eptr;
            long val;

            val = strtoll(v[j], &eptr, 10);
            if (eptr[0] != '\0' ||
                ((j & 1) == 0 && val < 1) ||
                ((j & 1) == 1 && val < 0)) {
                sdsfreesplitres(v,vlen);
                goto badfmt;
            }
        }
        /* Finally set the new config */
        resetServerSaveParams();
        for (j = 0; j < vlen; j += 2) {
            time_t seconds;
            int changes;

            seconds = strtoll(v[j],NULL,10);
            changes = strtoll(v[j+1],NULL,10);
            appendServerSaveParams(seconds, changes);
        }
        sdsfreesplitres(v,vlen);
    } config_set_special_field("dir") {
        if (chdir((char*)ptrFromObj(o)) == -1) {
            addReplyErrorFormat(c,"Changing directory: %s", strerror(errno));
            return;
        }
    } config_set_special_field("client-output-buffer-limit") {
        int vlen, j;
        sds *v = sdssplitlen(szFromObj(o),sdslen(szFromObj(o))," ",1,&vlen);

        /* We need a multiple of 4: <class> <hard> <soft> <soft_seconds> */
        if (vlen % 4) {
            sdsfreesplitres(v,vlen);
            goto badfmt;
        }

        /* Sanity check of single arguments, so that we either refuse the
         * whole configuration string or accept it all, even if a single
         * error in a single client class is present. */
        for (j = 0; j < vlen; j++) {
            long val;

            if ((j % 4) == 0) {
                int type = getClientTypeByName(v[j]);
                if (type == -1 || type == CLIENT_TYPE_MASTER) {
                    sdsfreesplitres(v,vlen);
                    goto badfmt;
                }
            } else {
                val = memtoll(v[j], &err);
                if (err || val < 0) {
                    sdsfreesplitres(v,vlen);
                    goto badfmt;
                }
            }
        }
        /* Finally set the new config */
        for (j = 0; j < vlen; j += 4) {
            unsigned long long hard, soft;
            int soft_seconds;

            int type = getClientTypeByName(v[j]);
            hard = memtoll(v[j+1],NULL);
            soft = memtoll(v[j+2],NULL);
            soft_seconds = strtoll(v[j+3],NULL,10);

            cserver.client_obuf_limits[type].hard_limit_bytes = hard;
            cserver.client_obuf_limits[type].soft_limit_bytes = soft;
            cserver.client_obuf_limits[type].soft_limit_seconds = soft_seconds;
        }
        sdsfreesplitres(v,vlen);
    } config_set_special_field("oom-score-adj-values") {
        int vlen;
        int success = 1;

        sds *v = sdssplitlen(szFromObj(o), sdslen(szFromObj(o)), " ", 1, &vlen);
        if (vlen != CONFIG_OOM_COUNT || updateOOMScoreAdjValues(v, &errstr, 1) == C_ERR)
            success = 0;

        sdsfreesplitres(v, vlen);
        if (!success)
            goto badfmt;
    } config_set_special_field("notify-keyspace-events") {
        int flags = keyspaceEventsStringToFlags(szFromObj(o));

        if (flags == -1) goto badfmt;
        g_pserver->notify_keyspace_events = flags;
    /* Numerical fields.
     * config_set_numerical_field(name,var,min,max) */
    } config_set_numerical_field(
      "watchdog-period",ll,0,INT_MAX) {
        if (ll)
            enableWatchdog(ll);
        else
            disableWatchdog();
    } config_set_special_field("tls-allowlist") {
        g_pserver->tls_allowlist.clear();
        for (int i = 3; i < c->argc; ++i) {
            robj *val = c->argv[i];
            g_pserver->tls_allowlist.emplace(szFromObj(val), sdslen(szFromObj(val)));
        }
    /* Everything else is an error... */
    } config_set_else {
        addReplyErrorFormat(c,"Unsupported CONFIG parameter: %s",
            (char*)ptrFromObj(c->argv[2]));
        return;
    }

    /* On success we just return a generic OK for all the options. */
    addReply(c,shared.ok);
    return;

badfmt: /* Bad format errors */
    if (errstr) {
        addReplyErrorFormat(c,"Invalid argument '%s' for CONFIG SET '%s' - %s",
                szFromObj(o),
                szFromObj(c->argv[2]),
                errstr);
    } else {
        addReplyErrorFormat(c,"Invalid argument '%s' for CONFIG SET '%s'",
                szFromObj(o),
                szFromObj(c->argv[2]));
    }
}

/*-----------------------------------------------------------------------------
 * CONFIG GET implementation
 *----------------------------------------------------------------------------*/

#define config_get_string_field(_name,_var) do { \
    if (stringmatch(pattern,_name,1)) { \
        addReplyBulkCString(c,_name); \
        addReplyBulkCString(c,_var ? _var : ""); \
        matches++; \
    } \
} while(0)

#define config_get_bool_field(_name,_var) do { \
    if (stringmatch(pattern,_name,1)) { \
        addReplyBulkCString(c,_name); \
        addReplyBulkCString(c,_var ? "yes" : "no"); \
        matches++; \
    } \
} while(0)

#define config_get_numerical_field(_name,_var) do { \
    if (stringmatch(pattern,_name,1)) { \
        ll2string(buf,sizeof(buf),_var); \
        addReplyBulkCString(c,_name); \
        addReplyBulkCString(c,buf); \
        matches++; \
    } \
} while(0)

void configGetCommand(client *c) {
    robj *o = c->argv[2];
    void *replylen = addReplyDeferredLen(c);
    char *pattern = szFromObj(o);
    char buf[128];
    int matches = 0;
    serverAssertWithInfo(c,o,sdsEncodedObject(o));

    /* Iterate the configs that are standard */
    for (standardConfig *config = configs; config->name != NULL; config++) {
        if (stringmatch(pattern,config->name,1)) {
            addReplyBulkCString(c,config->name);
            config->interface.get(c,config->data);
            matches++;
        }
        if (config->alias && stringmatch(pattern,config->alias,1)) {
            addReplyBulkCString(c,config->alias);
            config->interface.get(c,config->data);
            matches++;
        }
    }

    /* String values */
    config_get_string_field("logfile",g_pserver->logfile);

    /* Numerical values */
    config_get_numerical_field("watchdog-period",g_pserver->watchdog_period);

    /* Everything we can't handle with macros follows. */

    if (stringmatch(pattern,"dir",1)) {
        char buf[1024];

        if (getcwd(buf,sizeof(buf)) == NULL)
            buf[0] = '\0';

        addReplyBulkCString(c,"dir");
        addReplyBulkCString(c,buf);
        matches++;
    }
    if (stringmatch(pattern,"save",1)) {
        sds buf = sdsempty();
        int j;

        for (j = 0; j < g_pserver->saveparamslen; j++) {
            buf = sdscatprintf(buf,"%jd %d",
                    (intmax_t)g_pserver->saveparams[j].seconds,
                    g_pserver->saveparams[j].changes);
            if (j != g_pserver->saveparamslen-1)
                buf = sdscatlen(buf," ",1);
        }
        addReplyBulkCString(c,"save");
        addReplyBulkCString(c,buf);
        sdsfree(buf);
        matches++;
    }
    if (stringmatch(pattern,"client-output-buffer-limit",1)) {
        sds buf = sdsempty();
        int j;

        for (j = 0; j < CLIENT_TYPE_OBUF_COUNT; j++) {
            buf = sdscatprintf(buf,"%s %llu %llu %ld",
                    getClientTypeName(j),
                    cserver.client_obuf_limits[j].hard_limit_bytes,
                    cserver.client_obuf_limits[j].soft_limit_bytes,
                    (long) cserver.client_obuf_limits[j].soft_limit_seconds);
            if (j != CLIENT_TYPE_OBUF_COUNT-1)
                buf = sdscatlen(buf," ",1);
        }
        addReplyBulkCString(c,"client-output-buffer-limit");
        addReplyBulkCString(c,buf);
        sdsfree(buf);
        matches++;
    }
    if (stringmatch(pattern,"unixsocketperm",1)) {
        char buf[32];
        snprintf(buf,sizeof(buf),"%lo",(unsigned long)g_pserver->unixsocketperm);
        addReplyBulkCString(c,"unixsocketperm");
        addReplyBulkCString(c,buf);
        matches++;
    }
    if (stringmatch(pattern,"slaveof",1) ||
        stringmatch(pattern,"replicaof",1))
    {
        const char *optname = stringmatch(pattern,"slaveof",1) ?
                        "slaveof" : "replicaof";
        char buf[256];
        addReplyBulkCString(c,optname);
        if (listLength(g_pserver->masters) == 0)
        {
            buf[0] = '\0';
            addReplyBulkCString(c,buf);
        }
        else
        {
            listIter li;
            listNode *ln;
            listRewind(g_pserver->masters, &li);
            bool fFirst = true;
            while ((ln = listNext(&li)))
            {
                if (!fFirst)
                {
                    addReplyBulkCString(c,optname);
                    matches++;
                }
                fFirst = false;
                struct redisMaster *mi = (struct redisMaster*)listNodeValue(ln);
                snprintf(buf,sizeof(buf),"%s %d",
                    mi->masterhost, mi->masterport);
                addReplyBulkCString(c,buf);
            }
        }
        matches++;
    }
    if (stringmatch(pattern,"notify-keyspace-events",1)) {
        sds flags = keyspaceEventsFlagsToString(g_pserver->notify_keyspace_events);

        addReplyBulkCString(c,"notify-keyspace-events");
        addReplyBulkSds(c,flags);
        matches++;
    }
    if (stringmatch(pattern,"bind",1)) {
        sds aux = sdsjoin(g_pserver->bindaddr,g_pserver->bindaddr_count," ");

        addReplyBulkCString(c,"bind");
        addReplyBulkCString(c,aux);
        sdsfree(aux);
        matches++;
    }
    if (stringmatch(pattern,"oom-score-adj-values",0)) {
        sds buf = sdsempty();
        int j;

        for (j = 0; j < CONFIG_OOM_COUNT; j++) {
            buf = sdscatprintf(buf,"%d", g_pserver->oom_score_adj_values[j]);
            if (j != CONFIG_OOM_COUNT-1)
                buf = sdscatlen(buf," ",1);
        }

        addReplyBulkCString(c,"oom-score-adj-values");
        addReplyBulkCString(c,buf);
        sdsfree(buf);
        matches++;
    }
    if (stringmatch(pattern,"active-replica",1)) {
        addReplyBulkCString(c,"active-replica");
        addReplyBulkCString(c, g_pserver->fActiveReplica ? "yes" : "no");
        matches++;
    }
    if (stringmatch(pattern, "tls-allowlist", 1)) {
        addReplyBulkCString(c,"tls-allowlist");
        addReplyArrayLen(c, (long)g_pserver->tls_allowlist.size());
        for (auto &elem : g_pserver->tls_allowlist) {
            addReplyBulkCBuffer(c, elem.get(), elem.size()); // addReplyBulkSds will free which we absolutely don't want
        }
        matches++;
    }

    setDeferredMapLen(c,replylen,matches);
}

/*-----------------------------------------------------------------------------
 * CONFIG REWRITE implementation
 *----------------------------------------------------------------------------*/

#define REDIS_CONFIG_REWRITE_SIGNATURE "# Generated by CONFIG REWRITE"

/* We use the following dictionary type to store where a configuration
 * option is mentioned in the old configuration file, so it's
 * like "maxmemory" -> list of line numbers (first line is zero). */
uint64_t dictSdsCaseHash(const void *key);
int dictSdsKeyCaseCompare(void *privdata, const void *key1, const void *key2);
void dictSdsDestructor(void *privdata, void *val);
void dictListDestructor(void *privdata, void *val);

/* Sentinel config rewriting is implemented inside sentinel.c by
 * rewriteConfigSentinelOption(). */
void rewriteConfigSentinelOption(struct rewriteConfigState *state);

dictType optionToLineDictType = {
    dictSdsCaseHash,            /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCaseCompare,      /* key compare */
    dictSdsDestructor,          /* key destructor */
    dictListDestructor,         /* val destructor */
    NULL                        /* allow to expand */
};

dictType optionSetDictType = {
    dictSdsCaseHash,            /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCaseCompare,      /* key compare */
    dictSdsDestructor,          /* key destructor */
    NULL,                       /* val destructor */
    NULL                        /* allow to expand */
};

/* The config rewrite state. */
struct rewriteConfigState {
    dict *option_to_line; /* Option -> list of config file lines map */
    dict *rewritten;      /* Dictionary of already processed options */
    int numlines;         /* Number of lines in current config */
    sds *lines;           /* Current lines as an array of sds strings */
    int has_tail;         /* True if we already added directives that were
                             not present in the original config file. */
    int force_all;        /* True if we want all keywords to be force
                             written. Currently only used for testing. */
};

/* Append the new line to the current configuration state. */
void rewriteConfigAppendLine(struct rewriteConfigState *state, sds line) {
    state->lines = (sds*)zrealloc(state->lines, sizeof(char*) * (state->numlines+1), MALLOC_LOCAL);
    state->lines[state->numlines++] = line;
}

/* Populate the option -> list of line numbers map. */
void rewriteConfigAddLineNumberToOption(struct rewriteConfigState *state, sds option, int linenum) {
    list *l = (list*)dictFetchValue(state->option_to_line,option);

    if (l == NULL) {
        l = listCreate();
        dictAdd(state->option_to_line,sdsdup(option),l);
    }
    listAddNodeTail(l,(void*)(long)linenum);
}

/* Add the specified option to the set of processed options.
 * This is useful as only unused lines of processed options will be blanked
 * in the config file, while options the rewrite process does not understand
 * remain untouched. */
void rewriteConfigMarkAsProcessed(struct rewriteConfigState *state, const char *option) {
    sds opt = sdsnew(option);

    if (dictAdd(state->rewritten,opt,NULL) != DICT_OK) sdsfree(opt);
}

/* Read the old file, split it into lines to populate a newly created
 * config rewrite state, and return it to the caller.
 *
 * If it is impossible to read the old file, NULL is returned.
 * If the old file does not exist at all, an empty state is returned. */
struct rewriteConfigState *rewriteConfigReadOldFile(char *path) {
    FILE *fp = fopen(path,"r");
    struct rewriteConfigState *state = (rewriteConfigState*)zmalloc(sizeof(*state), MALLOC_LOCAL);
    char buf[CONFIG_MAX_LINE+1];
    int linenum = -1;

    if (fp == NULL && errno != ENOENT) return NULL;

    state->option_to_line = dictCreate(&optionToLineDictType,NULL);
    state->rewritten = dictCreate(&optionSetDictType,NULL);
    state->numlines = 0;
    state->lines = NULL;
    state->has_tail = 0;
    state->force_all = 0;
    if (fp == NULL) return state;

    /* Read the old file line by line, populate the state. */
    while(fgets(buf,CONFIG_MAX_LINE+1,fp) != NULL) {
        int argc;
        sds *argv;
        sds line = sdstrim(sdsnew(buf),"\r\n\t ");

        linenum++; /* Zero based, so we init at -1 */

        /* Handle comments and empty lines. */
        if (line[0] == '#' || line[0] == '\0') {
            if (!state->has_tail && !strcmp(line,REDIS_CONFIG_REWRITE_SIGNATURE))
                state->has_tail = 1;
            rewriteConfigAppendLine(state,line);
            continue;
        }

        /* Not a comment, split into arguments. */
        argv = sdssplitargs(line,&argc);
        if (argv == NULL) {
            /* Apparently the line is unparsable for some reason, for
             * instance it may have unbalanced quotes. Load it as a
             * comment. */
            sds aux = sdsnew("# ??? ");
            aux = sdscatsds(aux,line);
            sdsfree(line);
            rewriteConfigAppendLine(state,aux);
            continue;
        }

        sdstolower(argv[0]); /* We only want lowercase config directives. */

        /* Now we populate the state according to the content of this line.
         * Append the line and populate the option -> line numbers map. */
        rewriteConfigAppendLine(state,line);

        /* Translate options using the word "slave" to the corresponding name
         * "replica", before adding such option to the config name -> lines
         * mapping. */
        char *p = strstr(argv[0],"slave");
        if (p) {
            sds alt = sdsempty();
            alt = sdscatlen(alt,argv[0],p-argv[0]);
            alt = sdscatlen(alt,"replica",7);
            alt = sdscatlen(alt,p+5,strlen(p+5));
            sdsfree(argv[0]);
            argv[0] = alt;
        }
        /* If this is sentinel config, we use sentinel "sentinel <config>" as option 
            to avoid messing up the sequence. */
        if (g_pserver->sentinel_mode && argc > 1 && !strcasecmp(argv[0],"sentinel")) {
            sds sentinelOption = sdsempty();
            sentinelOption = sdscatfmt(sentinelOption,"%S %S",argv[0],argv[1]);
            rewriteConfigAddLineNumberToOption(state,sentinelOption,linenum);
            sdsfree(sentinelOption);
        } else {
            rewriteConfigAddLineNumberToOption(state,argv[0],linenum);
        }
        sdsfreesplitres(argv,argc);
    }
    fclose(fp);
    return state;
}

/* Rewrite the specified configuration option with the new "line".
 * It progressively uses lines of the file that were already used for the same
 * configuration option in the old version of the file, removing that line from
 * the map of options -> line numbers.
 *
 * If there are lines associated with a given configuration option and
 * "force" is non-zero, the line is appended to the configuration file.
 * Usually "force" is true when an option has not its default value, so it
 * must be rewritten even if not present previously.
 *
 * The first time a line is appended into a configuration file, a comment
 * is added to show that starting from that point the config file was generated
 * by CONFIG REWRITE.
 *
 * "line" is either used, or freed, so the caller does not need to free it
 * in any way. */
void rewriteConfigRewriteLine(struct rewriteConfigState *state, const char *option, sds line, int force) {
    sds o = sdsnew(option);
    list *l = (list*)dictFetchValue(state->option_to_line,o);

    rewriteConfigMarkAsProcessed(state,option);

    if (!l && !force && !state->force_all) {
        /* Option not used previously, and we are not forced to use it. */
        sdsfree(line);
        sdsfree(o);
        return;
    }

    if (l) {
        listNode *ln = listFirst(l);
        int linenum = (long) ln->value;

        /* There are still lines in the old configuration file we can reuse
         * for this option. Replace the line with the new one. */
        listDelNode(l,ln);
        if (listLength(l) == 0) dictDelete(state->option_to_line,o);
        sdsfree(state->lines[linenum]);
        state->lines[linenum] = line;
    } else {
        /* Append a new line. */
        if (!state->has_tail) {
            rewriteConfigAppendLine(state,
                sdsnew(REDIS_CONFIG_REWRITE_SIGNATURE));
            state->has_tail = 1;
        }
        rewriteConfigAppendLine(state,line);
    }
    sdsfree(o);
}

/* Write the long long 'bytes' value as a string in a way that is parsable
 * inside keydb.conf. If possible uses the GB, MB, KB notation. */
int rewriteConfigFormatMemory(char *buf, size_t len, long long bytes) {
    int gb = 1024*1024*1024;
    int mb = 1024*1024;
    int kb = 1024;

    if (bytes && (bytes % gb) == 0) {
        return snprintf(buf,len,"%lldgb",bytes/gb);
    } else if (bytes && (bytes % mb) == 0) {
        return snprintf(buf,len,"%lldmb",bytes/mb);
    } else if (bytes && (bytes % kb) == 0) {
        return snprintf(buf,len,"%lldkb",bytes/kb);
    } else {
        return snprintf(buf,len,"%lld",bytes);
    }
}

/* Rewrite a simple "option-name <bytes>" configuration option. */
void rewriteConfigBytesOption(struct rewriteConfigState *state, const char *option, long long value, long long defvalue) {
    char buf[64];
    int force = value != defvalue;
    sds line;

    rewriteConfigFormatMemory(buf,sizeof(buf),value);
    line = sdscatprintf(sdsempty(),"%s %s",option,buf);
    rewriteConfigRewriteLine(state,option,line,force);
}

/* Rewrite a yes/no option. */
void rewriteConfigYesNoOption(struct rewriteConfigState *state, const char *option, int value, int defvalue) {
    int force = value != defvalue;
    sds line = sdscatprintf(sdsempty(),"%s %s",option,
        value ? "yes" : "no");

    rewriteConfigRewriteLine(state,option,line,force);
}

/* Rewrite a string option. */
void rewriteConfigStringOption(struct rewriteConfigState *state, const char *option, const char *value, const char *defvalue) {
    int force = 1;
    sds line;

    /* String options set to NULL need to be not present at all in the
     * configuration file to be set to NULL again at the next reboot. */
    if (value == NULL) {
        rewriteConfigMarkAsProcessed(state,option);
        return;
    }

    /* Set force to zero if the value is set to its default. */
    if (defvalue && strcmp(value,defvalue) == 0) force = 0;

    line = sdsnew(option);
    line = sdscatlen(line, " ", 1);
    line = sdscatrepr(line, value, strlen(value));

    rewriteConfigRewriteLine(state,option,line,force);
}

/* Rewrite a SDS string option. */
void rewriteConfigSdsOption(struct rewriteConfigState *state, const char *option, sds value, const sds defvalue) {
    int force = 1;
    sds line;

    /* If there is no value set, we don't want the SDS option
     * to be present in the configuration at all. */
    if (value == NULL) {
        rewriteConfigMarkAsProcessed(state, option);
        return;
    }

    /* Set force to zero if the value is set to its default. */
    if (defvalue && sdscmp(value, defvalue) == 0) force = 0;

    line = sdsnew(option);
    line = sdscatlen(line, " ", 1);
    line = sdscatrepr(line, value, sdslen(value));

    rewriteConfigRewriteLine(state, option, line, force);
}

/* Rewrite a numerical (long long range) option. */
void rewriteConfigNumericalOption(struct rewriteConfigState *state, const char *option, long long value, long long defvalue) {
    int force = value != defvalue;
    sds line = sdscatprintf(sdsempty(),"%s %lld",option,value);

    rewriteConfigRewriteLine(state,option,line,force);
}

/* Rewrite an octal option. */
void rewriteConfigOctalOption(struct rewriteConfigState *state, const char *option, int value, int defvalue) {
    int force = value != defvalue;
    sds line = sdscatprintf(sdsempty(),"%s %o",option,value);

    rewriteConfigRewriteLine(state,option,line,force);
}

/* Rewrite an enumeration option. It takes as usually state and option name,
 * and in addition the enumeration array and the default value for the
 * option. */
void rewriteConfigEnumOption(struct rewriteConfigState *state, const char *option, int value, configEnum *ce, int defval) {
    sds line;
    const char *name = configEnumGetNameOrUnknown(ce,value);
    int force = value != defval;

    line = sdscatprintf(sdsempty(),"%s %s",option,name);
    rewriteConfigRewriteLine(state,option,line,force);
}

/* Rewrite the save option. */
void rewriteConfigSaveOption(struct rewriteConfigState *state) {
    int j;
    sds line;

    /* In Sentinel mode we don't need to rewrite the save parameters */
    if (g_pserver->sentinel_mode) {
        rewriteConfigMarkAsProcessed(state,"save");
        return;
    }

    /* Rewrite save parameters, or an empty 'save ""' line to avoid the
     * defaults from being used.
     */
    if (!g_pserver->saveparamslen) {
        rewriteConfigRewriteLine(state,"save",sdsnew("save \"\""),1);
    } else {
        for (j = 0; j < g_pserver->saveparamslen; j++) {
            line = sdscatprintf(sdsempty(),"save %ld %d",
                (long) g_pserver->saveparams[j].seconds, g_pserver->saveparams[j].changes);
            rewriteConfigRewriteLine(state,"save",line,1);
        }
    }

    /* Mark "save" as processed in case server.saveparamslen is zero. */
    rewriteConfigMarkAsProcessed(state,"save");
}

/* Rewrite the user option. */
void rewriteConfigUserOption(struct rewriteConfigState *state) {
    /* If there is a user file defined we just mark this configuration
     * directive as processed, so that all the lines containing users
     * inside the config file gets discarded. */
    if (g_pserver->acl_filename[0] != '\0') {
        rewriteConfigMarkAsProcessed(state,"user");
        return;
    }

    /* Otherwise scan the list of users and rewrite every line. Note that
     * in case the list here is empty, the effect will just be to comment
     * all the users directive inside the config file. */
    raxIterator ri;
    raxStart(&ri,Users);
    raxSeek(&ri,"^",NULL,0);
    while(raxNext(&ri)) {
        user *u = (user*)ri.data;
        sds line = sdsnew("user ");
        line = sdscatsds(line,u->name);
        line = sdscatlen(line," ",1);
        sds descr = ACLDescribeUser(u);
        line = sdscatsds(line,descr);
        sdsfree(descr);
        rewriteConfigRewriteLine(state,"user",line,1);
    }
    raxStop(&ri);

    /* Mark "user" as processed in case there are no defined users. */
    rewriteConfigMarkAsProcessed(state,"user");
}

/* Rewrite the dir option, always using absolute paths.*/
void rewriteConfigDirOption(struct rewriteConfigState *state) {
    char cwd[1024];

    if (getcwd(cwd,sizeof(cwd)) == NULL) {
        rewriteConfigMarkAsProcessed(state,"dir");
        return; /* no rewrite on error. */
    }
    rewriteConfigStringOption(state,"dir",cwd,NULL);
}

/* Rewrite the slaveof option. */
void rewriteConfigSlaveofOption(struct rewriteConfigState *state, const char *option) {
    /* If this is a master, we want all the slaveof config options
    * in the file to be removed. Note that if this is a cluster instance
    * we don't want a slaveof directive inside keydb.conf. */
    if (g_pserver->cluster_enabled || listLength(g_pserver->masters) == 0) {
        rewriteConfigMarkAsProcessed(state,option);
        return;
    }

    listIter li;
    listNode *ln;
    listRewind(g_pserver->masters, &li);
    while ((ln = listNext(&li)))
    {
        struct redisMaster *mi = (struct redisMaster*)listNodeValue(ln);
        sds line;

        line = sdscatprintf(sdsempty(),"%s %s %d", option,
            mi->masterhost, mi->masterport);
        rewriteConfigRewriteLine(state,option,line,1);
    }
}

/* Rewrite the notify-keyspace-events option. */
void rewriteConfigNotifykeyspaceeventsOption(struct rewriteConfigState *state) {
    int force = g_pserver->notify_keyspace_events != 0;
    const char *option = "notify-keyspace-events";
    sds line, flags;

    flags = keyspaceEventsFlagsToString(g_pserver->notify_keyspace_events);
    line = sdsnew(option);
    line = sdscatlen(line, " ", 1);
    line = sdscatrepr(line, flags, sdslen(flags));
    sdsfree(flags);
    rewriteConfigRewriteLine(state,option,line,force);
}

/* Rewrite the client-output-buffer-limit option. */
void rewriteConfigClientoutputbufferlimitOption(struct rewriteConfigState *state) {
    int j;
    const char *option = "client-output-buffer-limit";

    for (j = 0; j < CLIENT_TYPE_OBUF_COUNT; j++) {
        int force = (cserver.client_obuf_limits[j].hard_limit_bytes !=
                    clientBufferLimitsDefaults[j].hard_limit_bytes) ||
                    (cserver.client_obuf_limits[j].soft_limit_bytes !=
                    clientBufferLimitsDefaults[j].soft_limit_bytes) ||
                    (cserver.client_obuf_limits[j].soft_limit_seconds !=
                    clientBufferLimitsDefaults[j].soft_limit_seconds);
        sds line;
        char hard[64], soft[64];

        rewriteConfigFormatMemory(hard,sizeof(hard),
                cserver.client_obuf_limits[j].hard_limit_bytes);
        rewriteConfigFormatMemory(soft,sizeof(soft),
                cserver.client_obuf_limits[j].soft_limit_bytes);

        const char *tname = getClientTypeName(j);
        if (!strcmp(tname,"slave")) tname = "replica";
        line = sdscatprintf(sdsempty(),"%s %s %s %s %ld",
                option, tname, hard, soft,
                (long) cserver.client_obuf_limits[j].soft_limit_seconds);
        rewriteConfigRewriteLine(state,option,line,force);
    }
}

/* Rewrite the oom-score-adj-values option. */
void rewriteConfigOOMScoreAdjValuesOption(struct rewriteConfigState *state) {
    int force = 0;
    int j;
    const char *option = "oom-score-adj-values";
    sds line;

    line = sdsnew(option);
    line = sdscatlen(line, " ", 1);
    for (j = 0; j < CONFIG_OOM_COUNT; j++) {
        if (g_pserver->oom_score_adj_values[j] != configOOMScoreAdjValuesDefaults[j])
            force = 1;

        line = sdscatprintf(line, "%d", g_pserver->oom_score_adj_values[j]);
        if (j+1 != CONFIG_OOM_COUNT)
            line = sdscatlen(line, " ", 1);
    }
    rewriteConfigRewriteLine(state,option,line,force);
}

/* Rewrite the bind option. */
void rewriteConfigBindOption(struct rewriteConfigState *state) {
    int force = 1;
    sds line, addresses;
    const char *option = "bind";

    /* Nothing to rewrite if we don't have bind addresses. */
    if (g_pserver->bindaddr_count == 0) {
        rewriteConfigMarkAsProcessed(state,option);
        return;
    }

    /* Rewrite as bind <addr1> <addr2> ... <addrN> */
    addresses = sdsjoin(g_pserver->bindaddr,g_pserver->bindaddr_count," ");
    line = sdsnew(option);
    line = sdscatlen(line, " ", 1);
    line = sdscatsds(line, addresses);
    sdsfree(addresses);

    rewriteConfigRewriteLine(state,option,line,force);
}

/* Glue together the configuration lines in the current configuration
 * rewrite state into a single string, stripping multiple empty lines. */
sds rewriteConfigGetContentFromState(struct rewriteConfigState *state) {
    sds content = sdsempty();
    int j, was_empty = 0;

    for (j = 0; j < state->numlines; j++) {
        /* Every cluster of empty lines is turned into a single empty line. */
        if (sdslen(state->lines[j]) == 0) {
            if (was_empty) continue;
            was_empty = 1;
        } else {
            was_empty = 0;
        }
        content = sdscatsds(content,state->lines[j]);
        content = sdscatlen(content,"\n",1);
    }
    return content;
}

/* Free the configuration rewrite state. */
void rewriteConfigReleaseState(struct rewriteConfigState *state) {
    sdsfreesplitres(state->lines,state->numlines);
    dictRelease(state->option_to_line);
    dictRelease(state->rewritten);
    zfree(state);
}

/* At the end of the rewrite process the state contains the remaining
 * map between "option name" => "lines in the original config file".
 * Lines used by the rewrite process were removed by the function
 * rewriteConfigRewriteLine(), all the other lines are "orphaned" and
 * should be replaced by empty lines.
 *
 * This function does just this, iterating all the option names and
 * blanking all the lines still associated. */
void rewriteConfigRemoveOrphaned(struct rewriteConfigState *state) {
    dictIterator *di = dictGetIterator(state->option_to_line);
    dictEntry *de;

    while((de = dictNext(di)) != NULL) {
        list *l = (list*)dictGetVal(de);
        sds option = (sds)dictGetKey(de);

        /* Don't blank lines about options the rewrite process
         * don't understand. */
        if (dictFind(state->rewritten,option) == NULL) {
            serverLog(LL_DEBUG,"Not rewritten option: %s", option);
            continue;
        }

        while(listLength(l)) {
            listNode *ln = listFirst(l);
            int linenum = (long) ln->value;

            sdsfree(state->lines[linenum]);
            state->lines[linenum] = sdsempty();
            listDelNode(l,ln);
        }
    }
    dictReleaseIterator(di);
}

/* This function replaces the old configuration file with the new content
 * in an atomic manner.
 *
 * The function returns 0 on success, otherwise -1 is returned and errno
 * is set accordingly. */
int rewriteConfigOverwriteFile(char *configfile, sds content) {
    int fd = -1;
    int retval = -1;
    char tmp_conffile[PATH_MAX];
    const char *tmp_suffix = ".XXXXXX";
    size_t offset = 0;
    ssize_t written_bytes = 0;

    int tmp_path_len = snprintf(tmp_conffile, sizeof(tmp_conffile), "%s%s", configfile, tmp_suffix);
    if (tmp_path_len <= 0 || (unsigned int)tmp_path_len >= sizeof(tmp_conffile)) {
        serverLog(LL_WARNING, "Config file full path is too long");
        errno = ENAMETOOLONG;
        return retval;
    }

#ifdef _GNU_SOURCE
    fd = mkostemp(tmp_conffile, O_CLOEXEC);
#else
    /* There's a theoretical chance here to leak the FD if a module thread forks & execv in the middle */
    fd = mkstemp(tmp_conffile);
#endif

    if (fd == -1) {
        serverLog(LL_WARNING, "Could not create tmp config file (%s)", strerror(errno));
        return retval;
    }

    while (offset < sdslen(content)) {
         written_bytes = write(fd, content + offset, sdslen(content) - offset);
         if (written_bytes <= 0) {
             if (errno == EINTR) continue; /* FD is blocking, no other retryable errors */
             serverLog(LL_WARNING, "Failed after writing (%zd) bytes to tmp config file (%s)", offset, strerror(errno));
             goto cleanup;
         }
         offset+=written_bytes;
    }

    if (fsync(fd))
        serverLog(LL_WARNING, "Could not sync tmp config file to disk (%s)", strerror(errno));
    else if (fchmod(fd, 0644 & ~g_pserver->umask) == -1)
        serverLog(LL_WARNING, "Could not chmod config file (%s)", strerror(errno));
    else if (rename(tmp_conffile, configfile) == -1)
        serverLog(LL_WARNING, "Could not rename tmp config file (%s)", strerror(errno));
    else {
        retval = 0;
        serverLog(LL_DEBUG, "Rewritten config file (%s) successfully", configfile);
    }

cleanup:
    close(fd);
    if (retval) unlink(tmp_conffile);
    return retval;
}

/* Rewrite the configuration file at "path".
 * If the configuration file already exists, we try at best to retain comments
 * and overall structure.
 *
 * Configuration parameters that are at their default value, unless already
 * explicitly included in the old configuration file, are not rewritten.
 * The force_all flag overrides this behavior and forces everything to be
 * written. This is currently only used for testing purposes.
 *
 * On error -1 is returned and errno is set accordingly, otherwise 0. */
int rewriteConfig(char *path, int force_all) {
    struct rewriteConfigState *state;
    sds newcontent;
    int retval;

    /* Step 1: read the old config into our rewrite state. */
    if ((state = rewriteConfigReadOldFile(path)) == NULL) return -1;
    if (force_all) state->force_all = 1;

    /* Step 2: rewrite every single option, replacing or appending it inside
     * the rewrite state. */

    /* Iterate the configs that are standard */
    for (standardConfig *config = configs; config->name != NULL; config++) {
        config->interface.rewrite(config->data, config->name, state);
    }

    rewriteConfigBindOption(state);
    rewriteConfigOctalOption(state,"unixsocketperm",g_pserver->unixsocketperm,CONFIG_DEFAULT_UNIX_SOCKET_PERM);
    rewriteConfigStringOption(state,"logfile",g_pserver->logfile,CONFIG_DEFAULT_LOGFILE);
    rewriteConfigSaveOption(state);
    rewriteConfigUserOption(state);
    rewriteConfigDirOption(state);
    rewriteConfigSlaveofOption(state,"replicaof");
    rewriteConfigStringOption(state,"cluster-config-file",g_pserver->cluster_configfile,CONFIG_DEFAULT_CLUSTER_CONFIG_FILE);
    rewriteConfigNotifykeyspaceeventsOption(state);
    rewriteConfigClientoutputbufferlimitOption(state);
    rewriteConfigYesNoOption(state,"active-replica",g_pserver->fActiveReplica,CONFIG_DEFAULT_ACTIVE_REPLICA);
    rewriteConfigStringOption(state, "version-override",KEYDB_SET_VERSION,KEYDB_REAL_VERSION);
    rewriteConfigOOMScoreAdjValuesOption(state);

    if (!g_pserver->tls_allowlist.empty()) {
        sds conf = sdsnew("tls-allowlist ");
        for (auto &elem : g_pserver->tls_allowlist) {
            conf = sdscatsds(conf, (sds)elem.get());
            conf = sdscat(conf, " ");
        }
        // trim the trailing space
        sdsrange(conf, 0, -1);
        rewriteConfigRewriteLine(state,"tls-allowlist",conf,1 /*force*/);
        // note: conf is owned by rewriteConfigRewriteLine - no need to free
    } else {
        rewriteConfigMarkAsProcessed(state, "tls-allowlist"); // ensure the line is removed if it existed
    }

    /* Rewrite Sentinel config if in Sentinel mode. */
    if (g_pserver->sentinel_mode) rewriteConfigSentinelOption(state);

    /* Step 3: remove all the orphaned lines in the old file, that is, lines
     * that were used by a config option and are no longer used, like in case
     * of multiple "save" options or duplicated options. */
    rewriteConfigRemoveOrphaned(state);

    /* Step 4: generate a new configuration file from the modified state
     * and write it into the original file. */
    newcontent = rewriteConfigGetContentFromState(state);
    retval = rewriteConfigOverwriteFile(cserver.configfile,newcontent);

    sdsfree(newcontent);
    rewriteConfigReleaseState(state);
    return retval;
}

/*-----------------------------------------------------------------------------
 * Configs that fit one of the major types and require no special handling
 *----------------------------------------------------------------------------*/
#define LOADBUF_SIZE 256
static char loadbuf[LOADBUF_SIZE];

#define embedCommonConfig(config_name, config_alias, config_flags) \
    config_name, config_alias, config_flags,

#define embedConfigInterface(initfn, setfn, getfn, rewritefn) { \
    initfn, setfn, getfn, rewritefn, \
},

/* What follows is the generic config types that are supported. To add a new
 * config with one of these types, add it to the standardConfig table with
 * the creation macro for each type.
 *
 * Each type contains the following:
 * * A function defining how to load this type on startup.
 * * A function defining how to update this type on CONFIG SET.
 * * A function defining how to serialize this type on CONFIG SET.
 * * A function defining how to rewrite this type on CONFIG REWRITE.
 * * A Macro defining how to create this type.
 */

/* Bool Configs */
static void boolConfigInit(typeData data) {
    *data.yesno.config = data.yesno.default_value;
}

static int boolConfigSet(typeData data, sds value, int update, const char **err) {
    int yn = yesnotoi(value);
    if (yn == -1) {
        if ((yn = truefalsetoi(value)) == -1) {
            *err = "argument must be 'yes' or 'no'";
            return 0;
        }
    }
    if (data.yesno.is_valid_fn && !data.yesno.is_valid_fn(yn, err))
        return 0;
    int prev = *(data.yesno.config);
    *(data.yesno.config) = yn;
    if (update && data.yesno.update_fn && !data.yesno.update_fn(yn, prev, err)) {
        *(data.yesno.config) = prev;
        return 0;
    }
    return 1;
}

static void boolConfigGet(client *c, typeData data) {
    addReplyBulkCString(c, *data.yesno.config ? "yes" : "no");
}

static void boolConfigRewrite(typeData data, const char *name, struct rewriteConfigState *state) {
    rewriteConfigYesNoOption(state, name,*(data.yesno.config), data.yesno.default_value);
}

constexpr standardConfig createBoolConfig(const char *name, const char *alias, unsigned flags, int &config_addr, int defaultValue, int (*is_valid)(int val, const char **err), int (*update)(int val, int prev, const char **err))
{
    standardConfig conf = {
        embedCommonConfig(name, alias, flags)
        { boolConfigInit, boolConfigSet, boolConfigGet, boolConfigRewrite }
    };
    conf.data.yesno.config = &config_addr;
    conf.data.yesno.default_value = defaultValue;
    conf.data.yesno.is_valid_fn = is_valid;
    conf.data.yesno.update_fn = update;
    return conf;
}

/* String Configs */
static void stringConfigInit(typeData data) {
    *data.string.config = (data.string.convert_empty_to_null && !data.string.default_value) ? NULL : zstrdup(data.string.default_value);
}

static int stringConfigSet(typeData data, sds value, int update, const char **err) {
    if (data.string.is_valid_fn && !data.string.is_valid_fn(value, err))
        return 0;
    char *prev = *data.string.config;
    *data.string.config = (data.string.convert_empty_to_null && !value[0]) ? NULL : zstrdup(value);
    if (update && data.string.update_fn && !data.string.update_fn(*data.string.config, prev, err)) {
        zfree(*data.string.config);
        *data.string.config = prev;
        return 0;
    }
    zfree(prev);
    return 1;
}

static void stringConfigGet(client *c, typeData data) {
    addReplyBulkCString(c, *data.string.config ? *data.string.config : "");
}

static void stringConfigRewrite(typeData data, const char *name, struct rewriteConfigState *state) {
    rewriteConfigStringOption(state, name,*(data.string.config), data.string.default_value);
}

/* SDS Configs */
static void sdsConfigInit(typeData data) {
    *data.sds.config = (data.sds.convert_empty_to_null && !data.sds.default_value) ? NULL: sdsnew(data.sds.default_value);
}

static int sdsConfigSet(typeData data, sds value, int update, const char **err) {
    if (data.sds.is_valid_fn && !data.sds.is_valid_fn(value, err))
        return 0;
    sds prev = *data.sds.config;
    *data.sds.config = (data.sds.convert_empty_to_null && (sdslen(value) == 0)) ? NULL : sdsdup(value);
    if (update && data.sds.update_fn && !data.sds.update_fn(*data.sds.config, prev, err)) {
        sdsfree(*data.sds.config);
        *data.sds.config = prev;
        return 0;
    }
    sdsfree(prev);
    return 1;
}

static void sdsConfigGet(client *c, typeData data) {
    if (*data.sds.config) {
        addReplyBulkSds(c, sdsdup(*data.sds.config));
    } else {
        addReplyBulkCString(c, "");
    }
}

static void sdsConfigRewrite(typeData data, const char *name, struct rewriteConfigState *state) {
    sds sdsDefault = data.sds.default_value ? sdsnew(data.sds.default_value) : NULL;
    rewriteConfigSdsOption(state, name, *(data.sds.config), sdsDefault);
    if (sdsDefault)
        sdsfree(sdsDefault);
}


#define ALLOW_EMPTY_STRING 0
#define EMPTY_STRING_IS_NULL 1

constexpr standardConfig createStringConfig(const char *name, const char *alias, unsigned flags, int empty_to_null, char *&config_addr, const char *defaultValue, int (*is_valid)(char*,const char**), int (*update)(char*,char*,const char**)) {
    standardConfig conf = {
        embedCommonConfig(name, alias, flags)
        embedConfigInterface(stringConfigInit, stringConfigSet, stringConfigGet, stringConfigRewrite)
    };
    conf.data.string = {
        &(config_addr),
        (defaultValue),
        (is_valid),
        (update),
        (empty_to_null),
    };
    return conf;
}

constexpr standardConfig createSDSConfig(const char *name, const char *alias, unsigned flags, int empty_to_null, sds &config_addr, const char *defaultValue, int (*is_valid)(char*,const char**), int (*update)(char*,char*,const char**)) {
    standardConfig conf = {
        embedCommonConfig(name, alias, flags)
        embedConfigInterface(sdsConfigInit, sdsConfigSet, sdsConfigGet, sdsConfigRewrite)
    };
    conf.data.sds = {
        &(config_addr),
        (defaultValue),
        (is_valid),
        (update),
        (empty_to_null),
    };
    return conf;
}

/* Enum configs */
static void enumConfigInit(typeData data) {
    *data.enumd.config = data.enumd.default_value;
}

static int enumConfigSet(typeData data, sds value, int update, const char **err) {
    int enumval = configEnumGetValue(data.enumd.enum_value, value);
    if (enumval == INT_MIN) {
        sds enumerr = sdsnew("argument must be one of the following: ");
        configEnum *enumNode = data.enumd.enum_value;
        while(enumNode->name != NULL) {
            enumerr = sdscatlen(enumerr, enumNode->name,
                                strlen(enumNode->name));
            enumerr = sdscatlen(enumerr, ", ", 2);
            enumNode++;
        }
        sdsrange(enumerr,0,-3); /* Remove final ", ". */

        strncpy(loadbuf, enumerr, LOADBUF_SIZE);
        loadbuf[LOADBUF_SIZE - 1] = '\0';

        sdsfree(enumerr);
        *err = loadbuf;
        return 0;
    }
    if (data.enumd.is_valid_fn && !data.enumd.is_valid_fn(enumval, err))
        return 0;
    int prev = *(data.enumd.config);
    *(data.enumd.config) = enumval;
    if (update && data.enumd.update_fn && !data.enumd.update_fn(enumval, prev, err)) {
        *(data.enumd.config) = prev;
        return 0;
    }
    return 1;
}

static void enumConfigGet(client *c, typeData data) {
    addReplyBulkCString(c, configEnumGetNameOrUnknown(data.enumd.enum_value,*data.enumd.config));
}

static void enumConfigRewrite(typeData data, const char *name, struct rewriteConfigState *state) {
    rewriteConfigEnumOption(state, name,*(data.enumd.config), data.enumd.enum_value, data.enumd.default_value);
}

constexpr standardConfig createEnumConfig(const char *name, const char *alias, unsigned flags, configEnum *enumVal, int &config_addr, int defaultValue, int (*is_valid)(int,const char**), int (*update)(int,int,const char**)) {
    standardConfig c = {
        embedCommonConfig(name, alias, flags)
        embedConfigInterface(enumConfigInit, enumConfigSet, enumConfigGet, enumConfigRewrite)
    };
    c.data.enumd = {
        &(config_addr),
        (enumVal),
        (defaultValue),
        (is_valid),
        (update),
    };

    return c;
}

/* Gets a 'long long val' and sets it into the union, using a macro to get
 * compile time type check. */
#define SET_NUMERIC_TYPE(val) \
    if (data.numeric.numeric_type == NUMERIC_TYPE_INT) { \
        *(data.numeric.config.i) = (int) val; \
    } else if (data.numeric.numeric_type == NUMERIC_TYPE_UINT) { \
        *(data.numeric.config.ui) = (unsigned int) val; \
    } else if (data.numeric.numeric_type == NUMERIC_TYPE_LONG) { \
        *(data.numeric.config.l) = (long) val; \
    } else if (data.numeric.numeric_type == NUMERIC_TYPE_ULONG) { \
        *(data.numeric.config.ul) = (unsigned long) val; \
    } else if (data.numeric.numeric_type == NUMERIC_TYPE_LONG_LONG) { \
        *(data.numeric.config.ll) = (long long) val; \
    } else if (data.numeric.numeric_type == NUMERIC_TYPE_ULONG_LONG) { \
        *(data.numeric.config.ull) = (unsigned long long) val; \
    } else if (data.numeric.numeric_type == NUMERIC_TYPE_SIZE_T) { \
        *(data.numeric.config.st) = (size_t) val; \
    } else if (data.numeric.numeric_type == NUMERIC_TYPE_SSIZE_T) { \
        *(data.numeric.config.sst) = (ssize_t) val; \
    } else if (data.numeric.numeric_type == NUMERIC_TYPE_OFF_T) { \
        *(data.numeric.config.ot) = (off_t) val; \
    } else if (data.numeric.numeric_type == NUMERIC_TYPE_TIME_T) { \
        *(data.numeric.config.tt) = (time_t) val; \
    }

/* Gets a 'long long val' and sets it with the value from the union, using a
 * macro to get compile time type check. */
#define GET_NUMERIC_TYPE(val) \
    if (data.numeric.numeric_type == NUMERIC_TYPE_INT) { \
        val = *(data.numeric.config.i); \
    } else if (data.numeric.numeric_type == NUMERIC_TYPE_UINT) { \
        val = *(data.numeric.config.ui); \
    } else if (data.numeric.numeric_type == NUMERIC_TYPE_LONG) { \
        val = *(data.numeric.config.l); \
    } else if (data.numeric.numeric_type == NUMERIC_TYPE_ULONG) { \
        val = *(data.numeric.config.ul); \
    } else if (data.numeric.numeric_type == NUMERIC_TYPE_LONG_LONG) { \
        val = *(data.numeric.config.ll); \
    } else if (data.numeric.numeric_type == NUMERIC_TYPE_ULONG_LONG) { \
        val = *(data.numeric.config.ull); \
    } else if (data.numeric.numeric_type == NUMERIC_TYPE_SIZE_T) { \
        val = *(data.numeric.config.st); \
    } else if (data.numeric.numeric_type == NUMERIC_TYPE_SSIZE_T) { \
        val = *(data.numeric.config.sst); \
    } else if (data.numeric.numeric_type == NUMERIC_TYPE_OFF_T) { \
        val = *(data.numeric.config.ot); \
    } else if (data.numeric.numeric_type == NUMERIC_TYPE_TIME_T) { \
        val = *(data.numeric.config.tt); \
    }

/* Numeric configs */
static void numericConfigInit(typeData data) {
    SET_NUMERIC_TYPE(data.numeric.default_value)
}

static int numericBoundaryCheck(typeData data, long long ll, const char **err) {
    if (data.numeric.numeric_type == NUMERIC_TYPE_ULONG_LONG ||
        data.numeric.numeric_type == NUMERIC_TYPE_UINT ||
        data.numeric.numeric_type == NUMERIC_TYPE_SIZE_T) {
        /* Boundary check for unsigned types */
        unsigned long long ull = ll;
        unsigned long long upper_bound = data.numeric.upper_bound;
        unsigned long long lower_bound = data.numeric.lower_bound;
        if (ull > upper_bound || ull < lower_bound) {
            snprintf(loadbuf, LOADBUF_SIZE,
                "argument must be between %llu and %llu inclusive",
                lower_bound,
                upper_bound);
            *err = loadbuf;
            return 0;
        }
    } else {
        /* Boundary check for signed types */
        if (ll > data.numeric.upper_bound || ll < data.numeric.lower_bound) {
            snprintf(loadbuf, LOADBUF_SIZE,
                "argument must be between %lld and %lld inclusive",
                data.numeric.lower_bound,
                data.numeric.upper_bound);
            *err = loadbuf;
            return 0;
        }
    }
    return 1;
}

static int numericConfigSet(typeData data, sds value, int update, const char **err) {
    long long ll, prev = 0;
    if (data.numeric.is_memory) {
        int memerr;
        ll = memtoll(value, &memerr);
        if (memerr || ll < 0) {
            *err = "argument must be a memory value";
            return 0;
        }
    } else {
        if (!string2ll(value, sdslen(value),&ll)) {
            *err = "argument couldn't be parsed into an integer" ;
            return 0;
        }
    }

    if (!numericBoundaryCheck(data, ll, err))
        return 0;

    if (data.numeric.is_valid_fn && !data.numeric.is_valid_fn(ll, err))
        return 0;

    GET_NUMERIC_TYPE(prev)
    SET_NUMERIC_TYPE(ll)

    if (update && data.numeric.update_fn && !data.numeric.update_fn(ll, prev, err)) {
        SET_NUMERIC_TYPE(prev)
        return 0;
    }
    return 1;
}

static void numericConfigGet(client *c, typeData data) {
    char buf[128];
    long long value = 0;

    GET_NUMERIC_TYPE(value)

    ll2string(buf, sizeof(buf), value);
    addReplyBulkCString(c, buf);
}

static void numericConfigRewrite(typeData data, const char *name, struct rewriteConfigState *state) {
    long long value = 0;

    GET_NUMERIC_TYPE(value)

    if (data.numeric.is_memory) {
        rewriteConfigBytesOption(state, name, value, data.numeric.default_value);
    } else {
        rewriteConfigNumericalOption(state, name, value, data.numeric.default_value);
    }
}

#define INTEGER_CONFIG 0
#define MEMORY_CONFIG 1

constexpr standardConfig embedCommonNumericalConfig(const char *name, const char *alias, unsigned flags, long long lower, long long upper, long long defaultValue, int memory, int (*is_valid)(long long, const char**), int (*update)(long long, long long, const char**)) {
    standardConfig conf = {
        embedCommonConfig(name, alias, flags)
        embedConfigInterface(numericConfigInit, numericConfigSet, numericConfigGet, numericConfigRewrite)
    };
    conf.data.numeric.is_memory = (memory);
    conf.data.numeric.lower_bound = (lower);
    conf.data.numeric.upper_bound = (upper);
    conf.data.numeric.default_value = (defaultValue);
    conf.data.numeric.is_valid_fn = (is_valid);
    conf.data.numeric.update_fn = (update);
    return conf;
}

constexpr standardConfig createIntConfig(const char *name, const char *alias, unsigned flags, long long lower, long long upper, int &config_addr, long long defaultValue, int memory, int (*is_valid)(long long, const char**), int (*update)(long long, long long, const char**))
{
    standardConfig conf =  embedCommonNumericalConfig(name, alias, flags, lower, upper, defaultValue, memory, is_valid, update);
    conf.data.numeric.numeric_type = NUMERIC_TYPE_INT;
    conf.data.numeric.config.i = &config_addr;
    return conf;
}

constexpr standardConfig createUIntConfig(const char *name, const char *alias, unsigned flags, long long lower, long long upper, unsigned int &config_addr, long long defaultValue, int memory, int (*is_valid)(long long, const char**), int (*update)(long long, long long, const char**))
{
    auto conf = embedCommonNumericalConfig(name, alias, flags, lower, upper, defaultValue, memory, is_valid, update);
    conf.data.numeric.numeric_type = NUMERIC_TYPE_UINT;
    conf.data.numeric.config.ui = &(config_addr);
    return conf;
}

constexpr standardConfig createLongConfig(const char *name, const char *alias, unsigned flags, long long lower, long long upper, long &config_addr, long long defaultValue, int memory, int (*is_valid)(long long, const char**), int (*update)(long long, long long, const char**)) {
    auto conf = embedCommonNumericalConfig(name, alias, flags, lower, upper, defaultValue, memory, is_valid, update);
    conf.data.numeric.numeric_type = NUMERIC_TYPE_LONG;
    conf.data.numeric.config.l = &(config_addr);
    return conf;
}

constexpr standardConfig createULongConfig(const char *name, const char *alias, unsigned flags, long long lower, long long upper, unsigned long &config_addr, long long defaultValue, int memory, int (*is_valid)(long long, const char**), int (*update)(long long, long long, const char**)) {
    auto conf = embedCommonNumericalConfig(name, alias, flags, lower, upper, defaultValue, memory, is_valid, update);
    conf.data.numeric.numeric_type = NUMERIC_TYPE_ULONG;
    conf.data.numeric.config.ul = &(config_addr);
    return conf;
}

constexpr standardConfig createLongLongConfig(const char *name, const char *alias, unsigned flags, long long lower, long long upper, long long &config_addr, long long defaultValue, int memory, int (*is_valid)(long long, const char**), int (*update)(long long, long long, const char**)) {
    auto conf = embedCommonNumericalConfig(name, alias, flags, lower, upper, defaultValue, memory, is_valid, update);
    conf.data.numeric.numeric_type = NUMERIC_TYPE_LONG_LONG;
    conf.data.numeric.config.ll = &(config_addr);
    return conf;
}

constexpr standardConfig createULongLongConfig(const char *name, const char *alias, unsigned flags, long long lower, long long upper, unsigned long long &config_addr, long long defaultValue, int memory, int (*is_valid)(long long, const char**), int (*update)(long long, long long, const char**)) {
    auto conf = embedCommonNumericalConfig(name, alias, flags, lower, upper, defaultValue, memory, is_valid, update);
    conf.data.numeric.numeric_type = NUMERIC_TYPE_ULONG_LONG;
    conf.data.numeric.config.ull = &(config_addr);
    return conf;
}

constexpr standardConfig createSizeTConfig(const char *name, const char *alias, unsigned flags, long long lower, long long upper, size_t &config_addr, long long defaultValue, int memory, int (*is_valid)(long long, const char**), int (*update)(long long, long long, const char**)) {
    auto conf = embedCommonNumericalConfig(name, alias, flags, lower, upper, defaultValue, memory, is_valid, update);
    conf.data.numeric.numeric_type = NUMERIC_TYPE_SIZE_T;
    conf.data.numeric.config.st = &(config_addr);
    return conf;
}

constexpr standardConfig createSSizeTConfig(const char *name, const char *alias, unsigned flags, long long lower, long long upper, ssize_t &config_addr, long long defaultValue, int memory, int (*is_valid)(long long, const char**), int (*update)(long long, long long, const char**)) {
    auto conf = embedCommonNumericalConfig(name, alias, flags, lower, upper, defaultValue, memory, is_valid, update);
    conf.data.numeric.numeric_type = NUMERIC_TYPE_SSIZE_T;
    conf.data.numeric.config.sst = &(config_addr);
    return conf;
}

constexpr standardConfig createTimeTConfig(const char *name, const char *alias, unsigned flags, long long lower, long long upper, time_t &config_addr, long long defaultValue, int memory, int (*is_valid)(long long, const char**), int (*update)(long long, long long, const char**)) {
    auto conf = embedCommonNumericalConfig(name, alias, flags, lower, upper, defaultValue, memory, is_valid, update);
    conf.data.numeric.numeric_type = NUMERIC_TYPE_TIME_T;
    conf.data.numeric.config.tt = &(config_addr);
    return conf;
}

constexpr standardConfig createOffTConfig(const char *name, const char *alias, unsigned flags, long long lower, long long upper, off_t &config_addr, long long defaultValue, int memory, int (*is_valid)(long long, const char**), int (*update)(long long, long long, const char**)) {
    auto conf = embedCommonNumericalConfig(name, alias, flags, lower, upper, defaultValue, memory, is_valid, update);
    conf.data.numeric.numeric_type = NUMERIC_TYPE_OFF_T;
    conf.data.numeric.config.ot = &(config_addr);
    return conf;
}

static int isValidActiveDefrag(int val, const char **err) {
#ifndef HAVE_DEFRAG
    if (val) {
        *err = "Active defragmentation cannot be enabled: it "
               "requires a KeyDB server compiled with a modified Jemalloc "
               "like the one shipped by default with the KeyDB source "
               "distribution";
        return 0;
    }
#else
    UNUSED(val);
    UNUSED(err);
#endif
    return 1;
}

static int isValidDBfilename(char *val, const char **err) {
    if (!pathIsBaseName(val)) {
        *err = "dbfilename can't be a path, just a filename";
        return 0;
    }
    return 1;
}

static int isValidAOFfilename(char *val, const char **err) {
    if (!pathIsBaseName(val)) {
        *err = "appendfilename can't be a path, just a filename";
        return 0;
    }
    return 1;
}

static int isValidS3Bucket(char *s3bucket, const char **err) {
    int status = EXIT_FAILURE;
    pid_t pid = fork();
    if (pid < 0)
    {
        *err = "couldn't fork to call aws cli";
        return 0;
    }

    if (pid == 0)
    {
        execlp("aws", "aws", "s3", "ls", s3bucket, nullptr);
        exit(EXIT_FAILURE);
    }
    else 
    {
        waitpid(pid, &status, 0);
    }

    if (status != EXIT_SUCCESS) {
        *err = "could not access s3 bucket";
        return 0;
    }
    return 1;
}

/* Validate specified string is a valid proc-title-template */
static int isValidProcTitleTemplate(char *val, const char **err) {
    if (!validateProcTitleTemplate(val)) {
        *err = "template format is invalid or contains unknown variables";
        return 0;
    }
    return 1;
}

static int updateProcTitleTemplate(char *val, char *prev, const char **err) {
    UNUSED(val);
    UNUSED(prev);
    if (redisSetProcTitle(NULL) == C_ERR) {
        *err = "failed to set process title";
        return 0;
    }
    return 1;
}

static int updateHZ(long long val, long long prev, const char **err) {
    UNUSED(prev);
    UNUSED(err);
    /* Hz is more a hint from the user, so we accept values out of range
     * but cap them to reasonable values. */
    g_pserver->config_hz = val;
    if (g_pserver->config_hz < CONFIG_MIN_HZ) g_pserver->config_hz = CONFIG_MIN_HZ;
    if (g_pserver->config_hz > CONFIG_MAX_HZ) g_pserver->config_hz = CONFIG_MAX_HZ;
    g_pserver->hz = g_pserver->config_hz;
    return 1;
}

static int updatePort(long long val, long long prev, const char **err) {
    /* Do nothing if port is unchanged */
    if (val == prev) {
        return 1;
    }

    // Run this thread to make sure its valid
    if (changeListenPort(val, &serverTL->ipfd, acceptTcpHandler, true) == C_ERR) {
        *err = "Unable to listen on this port. Check server logs.";
        return 0;
    }

    // Now run the config change on the other threads
    for (int ithread = 0; ithread < cserver.cthreads; ++ithread) {
        if (&g_pserver->rgthreadvar[ithread] != serverTL) {
            aePostFunction(g_pserver->rgthreadvar[ithread].el, [val]{
                if (changeListenPort(val, &serverTL->ipfd, acceptTcpHandler, false) == C_ERR) {
                    serverLog(LL_WARNING, "Failed to change the listen port for a thread.  Server will still be listening on old ports.");
                }
            });
        }
    }

    return 1;
}

static int updateJemallocBgThread(int val, int prev, const char **err) {
    UNUSED(prev);
    UNUSED(err);
    set_jemalloc_bg_thread(val);
    return 1;
}

static int updateReplBacklogSize(long long val, long long prev, const char **err) {
    /* resizeReplicationBacklog sets g_pserver->repl_backlog_size, and relies on
     * being able to tell when the size changes, so restore prev before calling it. */
    if (cserver.repl_backlog_disk_size) {
        *err = "Unable to dynamically resize the backlog because disk backlog is enabled";
        return 0;
    }
    g_pserver->repl_backlog_size = prev;
    g_pserver->repl_backlog_config_size = val;
    resizeReplicationBacklog(val);
    return 1;
}

static int updateMaxmemory(long long val, long long prev, const char **err) {
    UNUSED(prev);
    UNUSED(err);
    if (val) {
        size_t used = zmalloc_used_memory()-freeMemoryGetNotCountedMemory();
        if ((unsigned long long)val < used) {
            serverLog(LL_WARNING,"WARNING: the new maxmemory value set via CONFIG SET (%llu) is smaller than the current memory usage (%zu). This will result in key eviction and/or the inability to accept new write commands depending on the maxmemory-policy.", g_pserver->maxmemory, used);
        }
        performEvictions(false /*fPreSnapshot*/);
    }
    return 1;
}

static int updateGoodSlaves(long long val, long long prev, const char **err) {
    UNUSED(val);
    UNUSED(prev);
    UNUSED(err);
    refreshGoodSlavesCount();
    return 1;
}

static int updateMasterAuthConfig(sds, sds, const char **) {
    updateMasterAuth();
    return 1;
}

static int updateAppendonly(int val, int prev, const char **err) {
    UNUSED(prev);
    if (val == 0 && g_pserver->aof_state != AOF_OFF) {
        stopAppendOnly();
    } else if (val && g_pserver->aof_state == AOF_OFF) {
        if (startAppendOnly() == C_ERR) {
            *err = "Unable to turn on AOF. Check server logs.";
            return 0;
        }
    }
    return 1;
}

static int updateSighandlerEnabled(int val, int prev, const char **err) {
    UNUSED(err);
    UNUSED(prev);
    if (val)
        setupSignalHandlers();
    else
        removeSignalHandlers();
    return 1;
}

static int updateMaxclients(long long val, long long prev, const char **err) {
    /* Try to check if the OS is capable of supporting so many FDs. */
    if (val > prev) {
        adjustOpenFilesLimit();
        if (g_pserver->maxclients != val) {
            static char msg[128];
            snprintf(msg, sizeof(msg), "The operating system is not able to handle the specified number of clients, try with %d", g_pserver->maxclients);
            *err = msg;
            if (g_pserver->maxclients > prev) {
                g_pserver->maxclients = prev;
                adjustOpenFilesLimit();
            }
            return 0;
        }
        /* Change the SetSize for the current thread first. If any error, return the error message to the client, 
         * otherwise, continue to do the same for other threads */
        if ((unsigned int) aeGetSetSize(aeGetCurrentEventLoop()) <
            g_pserver->maxclients + CONFIG_FDSET_INCR)
        {
            if (aeResizeSetSize(aeGetCurrentEventLoop(),
                g_pserver->maxclients + CONFIG_FDSET_INCR) == AE_ERR)
            {
                *err = "The event loop API used by KeyDB is not able to handle the specified number of clients";
                return 0;
            }
            serverLog(LL_DEBUG,"Successfully changed the setsize for current thread %d", ielFromEventLoop(aeGetCurrentEventLoop()));
        }

        for (int iel = 0; iel < cserver.cthreads; ++iel)
        {
            if (g_pserver->rgthreadvar[iel].el == aeGetCurrentEventLoop()){
                continue;
            }

            if ((unsigned int) aeGetSetSize(g_pserver->rgthreadvar[iel].el) <
                g_pserver->maxclients + CONFIG_FDSET_INCR)
            {
                int res = aePostFunction(g_pserver->rgthreadvar[iel].el, [iel] {
                    if (aeResizeSetSize(g_pserver->rgthreadvar[iel].el, g_pserver->maxclients + CONFIG_FDSET_INCR) == AE_ERR) {
                        serverLog(LL_WARNING,"Failed to change the setsize for Thread %d", iel);
                    }
                });

                if (res != AE_OK){
                    static char msg[128];
                    snprintf(msg, sizeof(msg),"Failed to post the request to change setsize for Thread %d", iel);
                    *err = msg;
                    return 0;
                }
                serverLog(LL_DEBUG,"Successfully post the request to change the setsize for thread %d", iel);
            }
        }
    }
    return 1;
}

static int validateMultiMasterNoForward(int val, const char **) {
    if (val) {
        serverLog(LL_WARNING, "WARNING: multi-master-no-forward is set, you *must* use a mesh topology or dataloss will occur");
    }
    return 1;
}

static int updateOOMScoreAdj(int val, int prev, const char **err) {
    UNUSED(prev);

    if (val) {
        if (setOOMScoreAdj(-1) == C_ERR) {
            *err = "Failed to set current oom_score_adj. Check server logs.";
            return 0;
        }
    }

    return 1;
}

int updateRequirePass(sds val, sds prev, const char **err) {
    UNUSED(prev);
    UNUSED(err);
    /* The old "requirepass" directive just translates to setting
     * a password to the default user. The only thing we do
     * additionally is to remember the cleartext password in this
     * case, for backward compatibility with Redis <= 5. */
    ACLUpdateDefaultUserPassword(val);
    return 1;
}

#ifdef USE_OPENSSL
static int updateTlsCfg(char *val, char *prev, const char **err) {
    UNUSED(val);
    UNUSED(prev);
    UNUSED(err);

    /* If TLS is enabled, try to configure OpenSSL. */
    if ((g_pserver->tls_port || g_pserver->tls_replication || g_pserver->tls_cluster)
            && tlsConfigure(&g_pserver->tls_ctx_config) == C_ERR) {
        *err = "Unable to update TLS configuration. Check server logs.";
        return 0;
    }
    return 1;
}
static int updateTlsCfgBool(int val, int prev, const char **err) {
    UNUSED(val);
    UNUSED(prev);
    return updateTlsCfg(NULL, NULL, err);
}

static int updateTlsCfgInt(long long val, long long prev, const char **err) {
    UNUSED(val);
    UNUSED(prev);
    return updateTlsCfg(NULL, NULL, err);
}

static int updateTLSPortThread(long long val, bool fFirstCall, const char **err)
{
    if (changeListenPort(val, &serverTL->tlsfd, acceptTLSHandler, fFirstCall) == C_ERR) {
        *err = "Unable to listen on this port. Check server logs.";
        return 0;
    }

    return 1;
}

static int updateTLSPort(long long val, long long prev, const char **err) {
    /* Do nothing if port is unchanged */
    if (val == prev) {
        return 1;
    }

    /* Configure TLS if tls is enabled */
    if (prev == 0 && tlsConfigure(&g_pserver->tls_ctx_config) == C_ERR) {
        *err = "Unable to update TLS configuration. Check server logs.";
        return 0;
    }

    // Do our thread first in case there is a config issue
    if (!updateTLSPortThread(val, true /*fFirstCall*/, err))
        return 0;

    for (int ithread = 0; ithread < cserver.cthreads; ++ithread) {
        if  (ithread == serverTL - g_pserver->rgthreadvar)
            continue;   // we already did our thread
        aePostFunction(g_pserver->rgthreadvar[ithread].el, [val]{
            const char **err = nullptr;
            if (!updateTLSPortThread(val, false /*fFirstCall*/, err)) {
                serverLog(LL_WARNING, "Failed to update TLS port for a thread: %s", *err);
                serverLog(LL_WARNING, "\tKeyDB will still be listening on the old port for some threads.");
            }
        });
    }

    return 1;
}

#endif  /* USE_OPENSSL */

int fDummy = false;
standardConfig configs[] = {
    /* Bool configs */
    createBoolConfig("rdbchecksum", NULL, IMMUTABLE_CONFIG, g_pserver->rdb_checksum, 1, NULL, NULL),
    createBoolConfig("daemonize", NULL, IMMUTABLE_CONFIG, cserver.daemonize, 0, NULL, NULL),
    createBoolConfig("lua-replicate-commands", NULL, MODIFIABLE_CONFIG, g_pserver->lua_always_replicate_commands, 1, NULL, NULL),
    createBoolConfig("always-show-logo", NULL, IMMUTABLE_CONFIG, g_pserver->always_show_logo, 0, NULL, NULL),
    createBoolConfig("enable-motd", NULL, IMMUTABLE_CONFIG, cserver.enable_motd, 1, NULL, NULL),
    createBoolConfig("protected-mode", NULL, MODIFIABLE_CONFIG, g_pserver->protected_mode, 1, NULL, NULL),
    createBoolConfig("rdbcompression", NULL, MODIFIABLE_CONFIG, g_pserver->rdb_compression, 1, NULL, NULL),
    createBoolConfig("rdb-del-sync-files", NULL, MODIFIABLE_CONFIG, g_pserver->rdb_del_sync_files, 0, NULL, NULL),
    createBoolConfig("activerehashing", NULL, MODIFIABLE_CONFIG, g_pserver->activerehashing, 1, NULL, NULL),
    createBoolConfig("stop-writes-on-bgsave-error", NULL, MODIFIABLE_CONFIG, g_pserver->stop_writes_on_bgsave_err, 1, NULL, NULL),
    createBoolConfig("set-proc-title", NULL, IMMUTABLE_CONFIG, cserver.set_proc_title, 1, NULL, NULL), /* Should setproctitle be used? */
    createBoolConfig("dynamic-hz", NULL, MODIFIABLE_CONFIG, g_pserver->dynamic_hz, 1, NULL, NULL), /* Adapt hz to # of clients.*/
    createBoolConfig("lazyfree-lazy-eviction", NULL, MODIFIABLE_CONFIG, g_pserver->lazyfree_lazy_eviction, 0, NULL, NULL),
    createBoolConfig("lazyfree-lazy-expire", NULL, MODIFIABLE_CONFIG, g_pserver->lazyfree_lazy_expire, 0, NULL, NULL),
    createBoolConfig("lazyfree-lazy-server-del", NULL, MODIFIABLE_CONFIG, g_pserver->lazyfree_lazy_server_del, 0, NULL, NULL),
    createBoolConfig("lazyfree-lazy-user-del", NULL, MODIFIABLE_CONFIG, g_pserver->lazyfree_lazy_user_del , 0, NULL, NULL),
    createBoolConfig("lazyfree-lazy-user-flush", NULL, MODIFIABLE_CONFIG, g_pserver->lazyfree_lazy_user_flush , 0, NULL, NULL),
    createBoolConfig("repl-disable-tcp-nodelay", NULL, MODIFIABLE_CONFIG, g_pserver->repl_disable_tcp_nodelay, 0, NULL, NULL),
    createBoolConfig("repl-diskless-sync", NULL, MODIFIABLE_CONFIG, g_pserver->repl_diskless_sync, 0, NULL, NULL),
    createBoolConfig("aof-rewrite-incremental-fsync", NULL, MODIFIABLE_CONFIG, g_pserver->aof_rewrite_incremental_fsync, 1, NULL, NULL),
    createBoolConfig("no-appendfsync-on-rewrite", NULL, MODIFIABLE_CONFIG, g_pserver->aof_no_fsync_on_rewrite, 0, NULL, NULL),
    createBoolConfig("cluster-require-full-coverage", NULL, MODIFIABLE_CONFIG, g_pserver->cluster_require_full_coverage, 1, NULL, NULL),
    createBoolConfig("rdb-save-incremental-fsync", NULL, MODIFIABLE_CONFIG, g_pserver->rdb_save_incremental_fsync, 1, NULL, NULL),
    createBoolConfig("aof-load-truncated", NULL, MODIFIABLE_CONFIG, g_pserver->aof_load_truncated, 1, NULL, NULL),
    createBoolConfig("aof-use-rdb-preamble", NULL, MODIFIABLE_CONFIG, g_pserver->aof_use_rdb_preamble, 1, NULL, NULL),
    createBoolConfig("cluster-replica-no-failover", "cluster-slave-no-failover", MODIFIABLE_CONFIG, g_pserver->cluster_slave_no_failover, 0, NULL, NULL), /* Failover by default. */
    createBoolConfig("replica-lazy-flush", "slave-lazy-flush", MODIFIABLE_CONFIG, g_pserver->repl_slave_lazy_flush, 0, NULL, NULL),
    createBoolConfig("replica-serve-stale-data", "slave-serve-stale-data", MODIFIABLE_CONFIG, g_pserver->repl_serve_stale_data, 1, NULL, NULL),
    createBoolConfig("replica-read-only", "slave-read-only", MODIFIABLE_CONFIG, g_pserver->repl_slave_ro, 1, NULL, NULL),
    createBoolConfig("replica-ignore-maxmemory", "slave-ignore-maxmemory", MODIFIABLE_CONFIG, g_pserver->repl_slave_ignore_maxmemory, 1, NULL, NULL),
    createBoolConfig("jemalloc-bg-thread", NULL, MODIFIABLE_CONFIG, cserver.jemalloc_bg_thread, 1, NULL, updateJemallocBgThread),
    createBoolConfig("activedefrag", NULL, MODIFIABLE_CONFIG, cserver.active_defrag_enabled, 0, isValidActiveDefrag, NULL),
    createBoolConfig("syslog-enabled", NULL, IMMUTABLE_CONFIG, g_pserver->syslog_enabled, 0, NULL, NULL),
    createBoolConfig("cluster-enabled", NULL, IMMUTABLE_CONFIG, g_pserver->cluster_enabled, 0, NULL, NULL),
    createBoolConfig("appendonly", NULL, MODIFIABLE_CONFIG, g_pserver->aof_enabled, 0, NULL, updateAppendonly),
    createBoolConfig("cluster-allow-reads-when-down", NULL, MODIFIABLE_CONFIG, g_pserver->cluster_allow_reads_when_down, 0, NULL, NULL),
    createBoolConfig("delete-on-evict", NULL, MODIFIABLE_CONFIG, cserver.delete_on_evict, 0, NULL, NULL),
    createBoolConfig("use-fork", NULL, IMMUTABLE_CONFIG, cserver.fForkBgSave, 1, NULL, NULL),
    createBoolConfig("io-threads-do-reads", NULL, IMMUTABLE_CONFIG, fDummy, 0, NULL, NULL),
    createBoolConfig("time-thread-priority", NULL, IMMUTABLE_CONFIG, cserver.time_thread_priority, 0, NULL, NULL),
    createBoolConfig("prefetch-enabled", NULL, MODIFIABLE_CONFIG, g_pserver->prefetch_enabled, 1, NULL, NULL),
    createBoolConfig("allow-rdb-resize-op", NULL, MODIFIABLE_CONFIG, g_pserver->allowRdbResizeOp, 1, NULL, NULL),
    createBoolConfig("crash-log-enabled", NULL, MODIFIABLE_CONFIG, g_pserver->crashlog_enabled, 1, NULL, updateSighandlerEnabled),
    createBoolConfig("crash-memcheck-enabled", NULL, MODIFIABLE_CONFIG, g_pserver->memcheck_enabled, 1, NULL, NULL),
    createBoolConfig("use-exit-on-panic", NULL, MODIFIABLE_CONFIG, g_pserver->use_exit_on_panic, 0, NULL, NULL),
    createBoolConfig("disable-thp", NULL, MODIFIABLE_CONFIG, g_pserver->disable_thp, 1, NULL, NULL),
    createBoolConfig("cluster-allow-replica-migration", NULL, MODIFIABLE_CONFIG, g_pserver->cluster_allow_replica_migration, 1, NULL, NULL),
    createBoolConfig("replica-announced", NULL, MODIFIABLE_CONFIG, g_pserver->replica_announced, 1, NULL, NULL),
    createBoolConfig("enable-async-commands", NULL, MODIFIABLE_CONFIG, g_pserver->enable_async_commands, 0, NULL, NULL),
    createBoolConfig("multithread-load-enabled", NULL, MODIFIABLE_CONFIG, g_pserver->multithread_load_enabled, 0, NULL, NULL),
    createBoolConfig("active-client-balancing", NULL, MODIFIABLE_CONFIG, g_pserver->active_client_balancing, 1, NULL, NULL),

    /* String Configs */
    createStringConfig("aclfile", NULL, IMMUTABLE_CONFIG, ALLOW_EMPTY_STRING, g_pserver->acl_filename, "", NULL, NULL),
    createStringConfig("unixsocket", NULL, IMMUTABLE_CONFIG, EMPTY_STRING_IS_NULL, g_pserver->unixsocket, NULL, NULL, NULL),
    createStringConfig("pidfile", NULL, IMMUTABLE_CONFIG, EMPTY_STRING_IS_NULL, cserver.pidfile, NULL, NULL, NULL),
    createStringConfig("replica-announce-ip", "slave-announce-ip", MODIFIABLE_CONFIG, EMPTY_STRING_IS_NULL, g_pserver->slave_announce_ip, NULL, NULL, NULL),
    createStringConfig("masteruser", NULL, MODIFIABLE_CONFIG | SENSITIVE_CONFIG, EMPTY_STRING_IS_NULL, cserver.default_masteruser, NULL, NULL, updateMasterAuthConfig),
    createStringConfig("cluster-announce-ip", NULL, MODIFIABLE_CONFIG, EMPTY_STRING_IS_NULL, g_pserver->cluster_announce_ip, NULL, NULL, NULL),
    createStringConfig("syslog-ident", NULL, IMMUTABLE_CONFIG, ALLOW_EMPTY_STRING, g_pserver->syslog_ident, "redis", NULL, NULL),
    createStringConfig("dbfilename", NULL, MODIFIABLE_CONFIG, ALLOW_EMPTY_STRING, g_pserver->rdb_filename, CONFIG_DEFAULT_RDB_FILENAME, isValidDBfilename, NULL),
    createStringConfig("db-s3-object", NULL, MODIFIABLE_CONFIG, EMPTY_STRING_IS_NULL, g_pserver->rdb_s3bucketpath, NULL, isValidS3Bucket, NULL),
    createStringConfig("appendfilename", NULL, IMMUTABLE_CONFIG, ALLOW_EMPTY_STRING, g_pserver->aof_filename, "appendonly.aof", isValidAOFfilename, NULL),
    createStringConfig("server_cpulist", NULL, IMMUTABLE_CONFIG, EMPTY_STRING_IS_NULL, g_pserver->server_cpulist, NULL, NULL, NULL),
    createStringConfig("bio_cpulist", NULL, IMMUTABLE_CONFIG, EMPTY_STRING_IS_NULL, g_pserver->bio_cpulist, NULL, NULL, NULL),
    createStringConfig("aof_rewrite_cpulist", NULL, IMMUTABLE_CONFIG, EMPTY_STRING_IS_NULL, g_pserver->aof_rewrite_cpulist, NULL, NULL, NULL),
    createStringConfig("bgsave_cpulist", NULL, IMMUTABLE_CONFIG, EMPTY_STRING_IS_NULL, g_pserver->bgsave_cpulist, NULL, NULL, NULL),
    createStringConfig("storage-provider-options", NULL, IMMUTABLE_CONFIG, EMPTY_STRING_IS_NULL, cserver.storage_conf, NULL, NULL, NULL),
    createStringConfig("ignore-warnings", NULL, MODIFIABLE_CONFIG, ALLOW_EMPTY_STRING, g_pserver->ignore_warnings, "", NULL, NULL),
    createStringConfig("proc-title-template", NULL, MODIFIABLE_CONFIG, ALLOW_EMPTY_STRING, cserver.proc_title_template, CONFIG_DEFAULT_PROC_TITLE_TEMPLATE, isValidProcTitleTemplate, updateProcTitleTemplate),

    /* SDS Configs */
    createSDSConfig("masterauth", NULL, MODIFIABLE_CONFIG | SENSITIVE_CONFIG, EMPTY_STRING_IS_NULL, cserver.default_masterauth, NULL, NULL, updateMasterAuthConfig),
    createSDSConfig("requirepass", NULL, MODIFIABLE_CONFIG | SENSITIVE_CONFIG, EMPTY_STRING_IS_NULL, g_pserver->requirepass, NULL, NULL, updateRequirePass),

    /* Enum Configs */
    createEnumConfig("supervised", NULL, IMMUTABLE_CONFIG, supervised_mode_enum, cserver.supervised_mode, SUPERVISED_NONE, NULL, NULL),
    createEnumConfig("syslog-facility", NULL, IMMUTABLE_CONFIG, syslog_facility_enum, g_pserver->syslog_facility, LOG_LOCAL0, NULL, NULL),
    createEnumConfig("repl-diskless-load", NULL, MODIFIABLE_CONFIG, repl_diskless_load_enum, g_pserver->repl_diskless_load, REPL_DISKLESS_LOAD_DISABLED, NULL, NULL),
    createEnumConfig("loglevel", NULL, MODIFIABLE_CONFIG, loglevel_enum, cserver.verbosity, LL_NOTICE, NULL, NULL),
    createEnumConfig("maxmemory-policy", NULL, MODIFIABLE_CONFIG, maxmemory_policy_enum, g_pserver->maxmemory_policy, MAXMEMORY_NO_EVICTION, NULL, NULL),
    createEnumConfig("appendfsync", NULL, MODIFIABLE_CONFIG, aof_fsync_enum, g_pserver->aof_fsync, AOF_FSYNC_EVERYSEC, NULL, NULL),
    createEnumConfig("storage-cache-mode", NULL, IMMUTABLE_CONFIG, storage_memory_model_enum, cserver.storage_memory_model, STORAGE_WRITETHROUGH, NULL, NULL),
    createEnumConfig("oom-score-adj", NULL, MODIFIABLE_CONFIG, oom_score_adj_enum, g_pserver->oom_score_adj, OOM_SCORE_ADJ_NO, NULL, updateOOMScoreAdj),
    createEnumConfig("acl-pubsub-default", NULL, MODIFIABLE_CONFIG, acl_pubsub_default_enum, g_pserver->acl_pubsub_default, USER_FLAG_ALLCHANNELS, NULL, NULL),
    createEnumConfig("sanitize-dump-payload", NULL, MODIFIABLE_CONFIG, sanitize_dump_payload_enum, cserver.sanitize_dump_payload, SANITIZE_DUMP_NO, NULL, NULL),

    /* Integer configs */
    createIntConfig("databases", NULL, IMMUTABLE_CONFIG, 1, INT_MAX, cserver.dbnum, 16, INTEGER_CONFIG, NULL, NULL),
    createIntConfig("port", NULL, MODIFIABLE_CONFIG, 0, 65535, g_pserver->port, 6379, INTEGER_CONFIG, NULL, updatePort), /* TCP port. */
    createIntConfig("auto-aof-rewrite-percentage", NULL, MODIFIABLE_CONFIG, 0, INT_MAX, g_pserver->aof_rewrite_perc, 100, INTEGER_CONFIG, NULL, NULL),
    createIntConfig("cluster-replica-validity-factor", "cluster-slave-validity-factor", MODIFIABLE_CONFIG, 0, INT_MAX, g_pserver->cluster_slave_validity_factor, 10, INTEGER_CONFIG, NULL, NULL), /* Slave max data age factor. */
    createIntConfig("list-max-ziplist-size", NULL, MODIFIABLE_CONFIG, INT_MIN, INT_MAX, g_pserver->list_max_ziplist_size, -2, INTEGER_CONFIG, NULL, NULL),
    createIntConfig("tcp-keepalive", NULL, MODIFIABLE_CONFIG, 0, INT_MAX, cserver.tcpkeepalive, 300, INTEGER_CONFIG, NULL, NULL),
    createIntConfig("cluster-migration-barrier", NULL, MODIFIABLE_CONFIG, 0, INT_MAX, g_pserver->cluster_migration_barrier, 1, INTEGER_CONFIG, NULL, NULL),
    createIntConfig("active-defrag-cycle-min", NULL, MODIFIABLE_CONFIG, 1, 99, cserver.active_defrag_cycle_min, 1, INTEGER_CONFIG, NULL, NULL), /* Default: 1% CPU min (at lower threshold) */
    createIntConfig("active-defrag-cycle-max", NULL, MODIFIABLE_CONFIG, 1, 99, cserver.active_defrag_cycle_max, 25, INTEGER_CONFIG, NULL, NULL), /* Default: 25% CPU max (at upper threshold) */
    createIntConfig("active-defrag-threshold-lower", NULL, MODIFIABLE_CONFIG, 0, 1000, cserver.active_defrag_threshold_lower, 10, INTEGER_CONFIG, NULL, NULL), /* Default: don't defrag when fragmentation is below 10% */
    createIntConfig("active-defrag-threshold-upper", NULL, MODIFIABLE_CONFIG, 0, 1000, cserver.active_defrag_threshold_upper, 100, INTEGER_CONFIG, NULL, NULL), /* Default: maximum defrag force at 100% fragmentation */
    createIntConfig("lfu-log-factor", NULL, MODIFIABLE_CONFIG, 0, INT_MAX, g_pserver->lfu_log_factor, 10, INTEGER_CONFIG, NULL, NULL),
    createIntConfig("lfu-decay-time", NULL, MODIFIABLE_CONFIG, 0, INT_MAX, g_pserver->lfu_decay_time, 1, INTEGER_CONFIG, NULL, NULL),
    createIntConfig("replica-priority", "slave-priority", MODIFIABLE_CONFIG, 0, INT_MAX, g_pserver->slave_priority, 100, INTEGER_CONFIG, NULL, NULL),
    createIntConfig("repl-diskless-sync-delay", NULL, MODIFIABLE_CONFIG, 0, INT_MAX, g_pserver->repl_diskless_sync_delay, 5, INTEGER_CONFIG, NULL, NULL),
    createIntConfig("maxmemory-samples", NULL, MODIFIABLE_CONFIG, 1, INT_MAX, g_pserver->maxmemory_samples, 16, INTEGER_CONFIG, NULL, NULL),
    createIntConfig("maxmemory-eviction-tenacity", NULL, MODIFIABLE_CONFIG, 0, 100, g_pserver->maxmemory_eviction_tenacity, 10, INTEGER_CONFIG, NULL, NULL),
    createIntConfig("timeout", NULL, MODIFIABLE_CONFIG, 0, INT_MAX, cserver.maxidletime, 0, INTEGER_CONFIG, NULL, NULL), /* Default client timeout: infinite */
    createIntConfig("replica-announce-port", "slave-announce-port", MODIFIABLE_CONFIG, 0, 65535, g_pserver->slave_announce_port, 0, INTEGER_CONFIG, NULL, NULL),
    createIntConfig("tcp-backlog", NULL, IMMUTABLE_CONFIG, 0, INT_MAX, g_pserver->tcp_backlog, 511, INTEGER_CONFIG, NULL, NULL), /* TCP listen backlog. */
    createIntConfig("cluster-announce-bus-port", NULL, MODIFIABLE_CONFIG, 0, 65535, g_pserver->cluster_announce_bus_port, 0, INTEGER_CONFIG, NULL, NULL), /* Default: Use +10000 offset. */
    createIntConfig("cluster-announce-port", NULL, MODIFIABLE_CONFIG, 0, 65535, g_pserver->cluster_announce_port, 0, INTEGER_CONFIG, NULL, NULL), /* Use g_pserver->port */
    createIntConfig("cluster-announce-tls-port", NULL, MODIFIABLE_CONFIG, 0, 65535, g_pserver->cluster_announce_tls_port, 0, INTEGER_CONFIG, NULL, NULL), /* Use server.tls_port */
    createIntConfig("repl-timeout", NULL, MODIFIABLE_CONFIG, 1, INT_MAX, g_pserver->repl_timeout, 60, INTEGER_CONFIG, NULL, NULL),
    createIntConfig("repl-ping-replica-period", "repl-ping-slave-period", MODIFIABLE_CONFIG, 1, INT_MAX, g_pserver->repl_ping_slave_period, 10, INTEGER_CONFIG, NULL, NULL),
    createIntConfig("list-compress-depth", NULL, MODIFIABLE_CONFIG, 0, INT_MAX, g_pserver->list_compress_depth, 0, INTEGER_CONFIG, NULL, NULL),
    createIntConfig("rdb-key-save-delay", NULL, MODIFIABLE_CONFIG, INT_MIN, INT_MAX, g_pserver->rdb_key_save_delay, 0, INTEGER_CONFIG, NULL, NULL),
    createIntConfig("key-load-delay", NULL, MODIFIABLE_CONFIG, INT_MIN, INT_MAX, g_pserver->key_load_delay, 0, INTEGER_CONFIG, NULL, NULL),
    createIntConfig("active-expire-effort", NULL, MODIFIABLE_CONFIG, 1, 10, cserver.active_expire_effort, 1, INTEGER_CONFIG, NULL, NULL), /* From 1 to 10. */
    createIntConfig("hz", NULL, MODIFIABLE_CONFIG, 0, INT_MAX, g_pserver->config_hz, CONFIG_DEFAULT_HZ, INTEGER_CONFIG, NULL, updateHZ),
    createIntConfig("min-replicas-to-write", "min-slaves-to-write", MODIFIABLE_CONFIG, 0, INT_MAX, g_pserver->repl_min_slaves_to_write, 0, INTEGER_CONFIG, NULL, updateGoodSlaves),
    createIntConfig("min-replicas-max-lag", "min-slaves-max-lag", MODIFIABLE_CONFIG, 0, INT_MAX, g_pserver->repl_min_slaves_max_lag, 10, INTEGER_CONFIG, NULL, updateGoodSlaves),
    createIntConfig("min-clients-per-thread", NULL, MODIFIABLE_CONFIG, 0, 400, cserver.thread_min_client_threshold, 20, INTEGER_CONFIG, NULL, NULL),
    createIntConfig("storage-flush-period", NULL, MODIFIABLE_CONFIG, 1, 10000, g_pserver->storage_flush_period, 500, INTEGER_CONFIG, NULL, NULL),
    createIntConfig("replica-quorum", NULL, MODIFIABLE_CONFIG, -1, INT_MAX, g_pserver->repl_quorum, -1, INTEGER_CONFIG, NULL, NULL),
    createIntConfig("replica-weighting-factor", NULL, MODIFIABLE_CONFIG, 1, INT_MAX, g_pserver->replicaIsolationFactor, 2, INTEGER_CONFIG, NULL, NULL),
    /* Unsigned int configs */
    createUIntConfig("maxclients", NULL, MODIFIABLE_CONFIG, 1, UINT_MAX, g_pserver->maxclients, 10000, INTEGER_CONFIG, NULL, updateMaxclients),
    createUIntConfig("loading-process-events-interval-keys", NULL, MODIFIABLE_CONFIG, 0, LONG_MAX, g_pserver->loading_process_events_interval_keys, 8192, MEMORY_CONFIG, NULL, NULL),
    createUIntConfig("maxclients-reserved", NULL, MODIFIABLE_CONFIG, 0, 100, g_pserver->maxclientsReserved, 0, INTEGER_CONFIG, NULL,  NULL),

    /* Unsigned Long configs */
    createULongConfig("active-defrag-max-scan-fields", NULL, MODIFIABLE_CONFIG, 1, LONG_MAX, cserver.active_defrag_max_scan_fields, 1000, INTEGER_CONFIG, NULL, NULL), /* Default: keys with more than 1000 fields will be processed separately */
    createULongConfig("slowlog-max-len", NULL, MODIFIABLE_CONFIG, 0, LONG_MAX, g_pserver->slowlog_max_len, 128, INTEGER_CONFIG, NULL, NULL),
    createULongConfig("acllog-max-len", NULL, MODIFIABLE_CONFIG, 0, LONG_MAX, g_pserver->acllog_max_len, 128, INTEGER_CONFIG, NULL, NULL),

    /* Long Long configs */
    createLongLongConfig("lua-time-limit", NULL, MODIFIABLE_CONFIG, 0, LONG_MAX, g_pserver->lua_time_limit, 5000, INTEGER_CONFIG, NULL, NULL),/* milliseconds */
    createLongLongConfig("cluster-node-timeout", NULL, MODIFIABLE_CONFIG, 0, LLONG_MAX, g_pserver->cluster_node_timeout, 15000, INTEGER_CONFIG, NULL, NULL),
    createLongLongConfig("slowlog-log-slower-than", NULL, MODIFIABLE_CONFIG, -1, LLONG_MAX, g_pserver->slowlog_log_slower_than, 10000, INTEGER_CONFIG, NULL, NULL),
    createLongLongConfig("latency-monitor-threshold", NULL, MODIFIABLE_CONFIG, 0, LLONG_MAX, g_pserver->latency_monitor_threshold, 0, INTEGER_CONFIG, NULL, NULL),
    createLongLongConfig("proto-max-bulk-len", NULL, MODIFIABLE_CONFIG, 1024*1024, LLONG_MAX, g_pserver->proto_max_bulk_len, 512ll*1024*1024, MEMORY_CONFIG, NULL, NULL), /* Bulk request max size */
    createLongLongConfig("stream-node-max-entries", NULL, MODIFIABLE_CONFIG, 0, LLONG_MAX, g_pserver->stream_node_max_entries, 100, INTEGER_CONFIG, NULL, NULL),
    createLongLongConfig("repl-backlog-size", NULL, MODIFIABLE_CONFIG, 1, LLONG_MAX, g_pserver->repl_backlog_config_size, 1024*1024, MEMORY_CONFIG, NULL, updateReplBacklogSize), /* Default: 1mb */
    createLongLongConfig("repl-backlog-disk-reserve", NULL, IMMUTABLE_CONFIG, 0, LLONG_MAX, cserver.repl_backlog_disk_size, 0, MEMORY_CONFIG, NULL, NULL),
    createLongLongConfig("max-snapshot-slip", NULL, MODIFIABLE_CONFIG, 0, 5000, g_pserver->snapshot_slip, 400, 0, NULL, NULL),
    createLongLongConfig("max-rand-count", NULL, MODIFIABLE_CONFIG, 0, LONG_MAX/2, g_pserver->rand_total_threshold, LONG_MAX/2, 0, NULL, NULL),

    /* Unsigned Long Long configs */
    createULongLongConfig("maxmemory", NULL, MODIFIABLE_CONFIG, 0, LLONG_MAX, g_pserver->maxmemory, 0, MEMORY_CONFIG, NULL, updateMaxmemory),
    createULongLongConfig("maxstorage", NULL, MODIFIABLE_CONFIG, 0, LLONG_MAX, g_pserver->maxstorage, 0, MEMORY_CONFIG, NULL, NULL),

    /* Size_t configs */
    createSizeTConfig("hash-max-ziplist-entries", NULL, MODIFIABLE_CONFIG, 0, LONG_MAX, g_pserver->hash_max_ziplist_entries, 512, INTEGER_CONFIG, NULL, NULL),
    createSizeTConfig("set-max-intset-entries", NULL, MODIFIABLE_CONFIG, 0, LONG_MAX, g_pserver->set_max_intset_entries, 512, INTEGER_CONFIG, NULL, NULL),
    createSizeTConfig("zset-max-ziplist-entries", NULL, MODIFIABLE_CONFIG, 0, LONG_MAX, g_pserver->zset_max_ziplist_entries, 128, INTEGER_CONFIG, NULL, NULL),
    createSizeTConfig("active-defrag-ignore-bytes", NULL, MODIFIABLE_CONFIG, 1, LLONG_MAX, cserver.active_defrag_ignore_bytes, 100<<20, MEMORY_CONFIG, NULL, NULL), /* Default: don't defrag if frag overhead is below 100mb */
    createSizeTConfig("hash-max-ziplist-value", NULL, MODIFIABLE_CONFIG, 0, LONG_MAX, g_pserver->hash_max_ziplist_value, 64, MEMORY_CONFIG, NULL, NULL),
    createSizeTConfig("stream-node-max-bytes", NULL, MODIFIABLE_CONFIG, 0, LONG_MAX, g_pserver->stream_node_max_bytes, 4096, MEMORY_CONFIG, NULL, NULL),
    createSizeTConfig("zset-max-ziplist-value", NULL, MODIFIABLE_CONFIG, 0, LONG_MAX, g_pserver->zset_max_ziplist_value, 64, MEMORY_CONFIG, NULL, NULL),
    createSizeTConfig("hll-sparse-max-bytes", NULL, MODIFIABLE_CONFIG, 0, LONG_MAX, g_pserver->hll_sparse_max_bytes, 3000, MEMORY_CONFIG, NULL, NULL),
    createSizeTConfig("tracking-table-max-keys", NULL, MODIFIABLE_CONFIG, 0, LONG_MAX, g_pserver->tracking_table_max_keys, 1000000, INTEGER_CONFIG, NULL, NULL), /* Default: 1 million keys max. */
    createSizeTConfig("client-query-buffer-limit", NULL, MODIFIABLE_CONFIG, 1024*1024, LONG_MAX, cserver.client_max_querybuf_len, 1024*1024*1024, MEMORY_CONFIG, NULL, NULL), /* Default: 1GB max query buffer. */

    /* Other configs */
    createTimeTConfig("repl-backlog-ttl", NULL, MODIFIABLE_CONFIG, 0, LONG_MAX, g_pserver->repl_backlog_time_limit, 60*60, INTEGER_CONFIG, NULL, NULL), /* Default: 1 hour */
    createOffTConfig("auto-aof-rewrite-min-size", NULL, MODIFIABLE_CONFIG, 0, LLONG_MAX, g_pserver->aof_rewrite_min_size, 64*1024*1024, MEMORY_CONFIG, NULL, NULL),

    /* KeyDB Specific Configs */
    createULongConfig("loading-process-events-interval-bytes", NULL, MODIFIABLE_CONFIG, 0, LONG_MAX, g_pserver->loading_process_events_interval_bytes, 2*1024*1024, MEMORY_CONFIG, NULL, NULL),
    createBoolConfig("multi-master-no-forward", NULL, MODIFIABLE_CONFIG, cserver.multimaster_no_forward, 0, validateMultiMasterNoForward, NULL),
    createBoolConfig("allow-write-during-load", NULL, MODIFIABLE_CONFIG, g_pserver->fWriteDuringActiveLoad, 0, NULL, NULL),
    createBoolConfig("force-backlog-disk-reserve", NULL, MODIFIABLE_CONFIG, cserver.force_backlog_disk, 0, NULL, NULL),
    createBoolConfig("soft-shutdown", NULL, MODIFIABLE_CONFIG, g_pserver->config_soft_shutdown, 0, NULL, NULL),
    createBoolConfig("flash-disable-key-cache", NULL, MODIFIABLE_CONFIG, g_pserver->flash_disable_key_cache, 0, NULL, NULL),
    createSizeTConfig("semi-ordered-set-bucket-size", NULL, MODIFIABLE_CONFIG, 0, 1024, g_semiOrderedSetTargetBucketSize, 0, INTEGER_CONFIG, NULL, NULL),
    createSDSConfig("availability-zone", NULL, MODIFIABLE_CONFIG, 0, g_pserver->sdsAvailabilityZone, "", NULL, NULL),
    createIntConfig("overload-protect-percent", NULL, MODIFIABLE_CONFIG, 0, 200, g_pserver->overload_protect_threshold, 0, INTEGER_CONFIG, NULL, NULL),

#ifdef USE_OPENSSL
    createIntConfig("tls-port", NULL, MODIFIABLE_CONFIG, 0, 65535, g_pserver->tls_port, 0, INTEGER_CONFIG, NULL, updateTLSPort), /* TCP port. */
    createIntConfig("tls-session-cache-size", NULL, MODIFIABLE_CONFIG, 0, INT_MAX, g_pserver->tls_ctx_config.session_cache_size, 20*1024, INTEGER_CONFIG, NULL, updateTlsCfgInt),
    createIntConfig("tls-session-cache-timeout", NULL, MODIFIABLE_CONFIG, 0, INT_MAX, g_pserver->tls_ctx_config.session_cache_timeout, 300, INTEGER_CONFIG, NULL, updateTlsCfgInt),
    createBoolConfig("tls-cluster", NULL, MODIFIABLE_CONFIG, g_pserver->tls_cluster, 0, NULL, updateTlsCfgBool),
    createBoolConfig("tls-replication", NULL, MODIFIABLE_CONFIG, g_pserver->tls_replication, 0, NULL, updateTlsCfgBool),
    createEnumConfig("tls-auth-clients", NULL, MODIFIABLE_CONFIG, tls_auth_clients_enum, g_pserver->tls_auth_clients, TLS_CLIENT_AUTH_YES, NULL, NULL),
    createBoolConfig("tls-prefer-server-ciphers", NULL, MODIFIABLE_CONFIG, g_pserver->tls_ctx_config.prefer_server_ciphers, 0, NULL, updateTlsCfgBool),
    createBoolConfig("tls-session-caching", NULL, MODIFIABLE_CONFIG, g_pserver->tls_ctx_config.session_caching, 1, NULL, updateTlsCfgBool),
    createBoolConfig("tls-rotation", NULL, MODIFIABLE_CONFIG, g_pserver->tls_rotation, 0, NULL, updateTlsCfgBool),
    createStringConfig("tls-cert-file", NULL, MODIFIABLE_CONFIG, EMPTY_STRING_IS_NULL, g_pserver->tls_ctx_config.cert_file, NULL, NULL, updateTlsCfg),
    createStringConfig("tls-key-file", NULL, MODIFIABLE_CONFIG, EMPTY_STRING_IS_NULL, g_pserver->tls_ctx_config.key_file, NULL, NULL, updateTlsCfg),
    createStringConfig("tls-key-file-pass", NULL, MODIFIABLE_CONFIG, EMPTY_STRING_IS_NULL, g_pserver->tls_ctx_config.key_file_pass, NULL, NULL, updateTlsCfg),
    createStringConfig("tls-client-cert-file", NULL, MODIFIABLE_CONFIG, EMPTY_STRING_IS_NULL, g_pserver->tls_ctx_config.client_cert_file, NULL, NULL, updateTlsCfg),
    createStringConfig("tls-client-key-file", NULL, MODIFIABLE_CONFIG, EMPTY_STRING_IS_NULL, g_pserver->tls_ctx_config.client_key_file, NULL, NULL, updateTlsCfg),
    createStringConfig("tls-client-key-file-pass", NULL, MODIFIABLE_CONFIG, EMPTY_STRING_IS_NULL, g_pserver->tls_ctx_config.client_key_file_pass, NULL, NULL, updateTlsCfg),
    createStringConfig("tls-dh-params-file", NULL, MODIFIABLE_CONFIG, EMPTY_STRING_IS_NULL, g_pserver->tls_ctx_config.dh_params_file, NULL, NULL, updateTlsCfg),
    createStringConfig("tls-ca-cert-file", NULL, MODIFIABLE_CONFIG, EMPTY_STRING_IS_NULL, g_pserver->tls_ctx_config.ca_cert_file, NULL, NULL, updateTlsCfg),
    createStringConfig("tls-ca-cert-dir", NULL, MODIFIABLE_CONFIG, EMPTY_STRING_IS_NULL, g_pserver->tls_ctx_config.ca_cert_dir, NULL, NULL, updateTlsCfg),
    createStringConfig("tls-protocols", NULL, MODIFIABLE_CONFIG, EMPTY_STRING_IS_NULL, g_pserver->tls_ctx_config.protocols, NULL, NULL, updateTlsCfg),
    createStringConfig("tls-ciphers", NULL, MODIFIABLE_CONFIG, EMPTY_STRING_IS_NULL, g_pserver->tls_ctx_config.ciphers, NULL, NULL, updateTlsCfg),
    createStringConfig("tls-ciphersuites", NULL, MODIFIABLE_CONFIG, EMPTY_STRING_IS_NULL, g_pserver->tls_ctx_config.ciphersuites, NULL, NULL, updateTlsCfg),
#endif

    /* NULL Terminator */
    {NULL}
};

/*-----------------------------------------------------------------------------
 * CONFIG command entry point
 *----------------------------------------------------------------------------*/

void configCommand(client *c) {
    /* Only allow CONFIG GET while loading. */
    if (g_pserver->loading && strcasecmp(szFromObj(c->argv[1]),"get")) {
        addReplyError(c,"Only CONFIG GET is allowed during loading");
        return;
    }

    if (c->argc == 2 && !strcasecmp(szFromObj(c->argv[1]),"help")) {
        const char *help[] = {
"GET <pattern>",
"    Return parameters matching the glob-like <pattern> and their values.",
"SET <directive> <value>",
"    Set the configuration <directive> to <value>.",
"RESETSTAT",
"    Reset statistics reported by the INFO command.",
"REWRITE",
"    Rewrite the configuration file.",
NULL
        };

        addReplyHelp(c, help);
    } else if (!strcasecmp(szFromObj(c->argv[1]),"set") && c->argc >= 3) {
        configSetCommand(c);
    } else if (!strcasecmp(szFromObj(c->argv[1]),"get") && c->argc == 3) {
        configGetCommand(c);
    } else if (!strcasecmp(szFromObj(c->argv[1]),"resetstat") && c->argc == 2) {
        resetServerStats();
        resetCommandTableStats();
        resetErrorTableStats();
        addReply(c,shared.ok);
    } else if (!strcasecmp(szFromObj(c->argv[1]),"rewrite") && c->argc == 2) {
        if (cserver.configfile == NULL) {
            addReplyError(c,"The server is running without a config file");
            return;
        }
        if (rewriteConfig(cserver.configfile, 0) == -1) {
            serverLog(LL_WARNING,"CONFIG REWRITE failed: %s", strerror(errno));
            addReplyErrorFormat(c,"Rewriting config file: %s", strerror(errno));
        } else {
            serverLog(LL_WARNING,"CONFIG REWRITE executed with success.");
            addReply(c,shared.ok);
        }
    } else {
        addReplySubcommandSyntaxError(c);
        return;
    }
}
