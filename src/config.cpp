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
#include "cluster.h"

#include <fcntl.h>
#include <sys/stat.h>

const char *KEYDB_SET_VERSION = KEYDB_REAL_VERSION;

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

/* Output buffer limits presets. */
clientBufferLimitsConfig clientBufferLimitsDefaults[CLIENT_TYPE_OBUF_COUNT] = {
    {0, 0, 0}, /* normal */
    {1024*1024*256, 1024*1024*64, 60}, /* replica */
    {1024*1024*32, 1024*1024*8, 60}  /* pubsub */
};

/* Configuration values that require no special handling to set, get, load or 
 * rewrite. */
typedef struct configYesNo {
    const char *name; /* The user visible name of this config */
    const char *alias; /* An alias that can also be used for this config */
    int *config; /* The pointer to the server config this value is stored in */
    const int modifiable; /* Can this value be updated by CONFIG SET? */
    const int default_value; /* The default value of the config on rewrite */
} configYesNo;

configYesNo configs_yesno[] = {
    /* Non-Modifiable */
    {"rdbchecksum",NULL,&g_pserver->rdb_checksum,0,CONFIG_DEFAULT_RDB_CHECKSUM},
    {"daemonize",NULL,&cserver.daemonize,0,0},
    {"always-show-logo",NULL,&g_pserver->always_show_logo,0,CONFIG_DEFAULT_ALWAYS_SHOW_LOGO},
    /* Modifiable */
    {"protected-mode",NULL,&g_pserver->protected_mode,1,CONFIG_DEFAULT_PROTECTED_MODE},
    {"rdbcompression",NULL,&g_pserver->rdb_compression,1,CONFIG_DEFAULT_RDB_COMPRESSION},
    {"activerehashing",NULL,&g_pserver->activerehashing,1,CONFIG_DEFAULT_ACTIVE_REHASHING},
    {"stop-writes-on-bgsave-error",NULL,&g_pserver->stop_writes_on_bgsave_err,1,CONFIG_DEFAULT_STOP_WRITES_ON_BGSAVE_ERROR},
    {"dynamic-hz",NULL,&g_pserver->dynamic_hz,1,CONFIG_DEFAULT_DYNAMIC_HZ},
    {"lazyfree-lazy-eviction",NULL,&g_pserver->lazyfree_lazy_eviction,1,CONFIG_DEFAULT_LAZYFREE_LAZY_EVICTION},
    {"lazyfree-lazy-expire",NULL,&g_pserver->lazyfree_lazy_expire,1,CONFIG_DEFAULT_LAZYFREE_LAZY_EXPIRE},
    {"lazyfree-lazy-server-del",NULL,&g_pserver->lazyfree_lazy_server_del,1,CONFIG_DEFAULT_LAZYFREE_LAZY_SERVER_DEL},
    {"repl-disable-tcp-nodelay",NULL,&g_pserver->repl_disable_tcp_nodelay,1,CONFIG_DEFAULT_REPL_DISABLE_TCP_NODELAY},
    {"repl-diskless-sync",NULL,&g_pserver->repl_diskless_sync,1,CONFIG_DEFAULT_REPL_DISKLESS_SYNC},
    {"aof-rewrite-incremental-fsync",NULL,&g_pserver->aof_rewrite_incremental_fsync,1,CONFIG_DEFAULT_AOF_REWRITE_INCREMENTAL_FSYNC},
    {"no-appendfsync-on-rewrite",NULL,&g_pserver->aof_no_fsync_on_rewrite,1,CONFIG_DEFAULT_AOF_NO_FSYNC_ON_REWRITE},
    {"cluster-require-full-coverage",NULL,&g_pserver->cluster_require_full_coverage,CLUSTER_DEFAULT_REQUIRE_FULL_COVERAGE},
    {"rdb-save-incremental-fsync",NULL,&g_pserver->rdb_save_incremental_fsync,1,CONFIG_DEFAULT_RDB_SAVE_INCREMENTAL_FSYNC},
    {"aof-load-truncated",NULL,&g_pserver->aof_load_truncated,1,CONFIG_DEFAULT_AOF_LOAD_TRUNCATED},
    {"aof-use-rdb-preamble",NULL,&g_pserver->aof_use_rdb_preamble,1,CONFIG_DEFAULT_AOF_USE_RDB_PREAMBLE},
    {"cluster-replica-no-failover","cluster-slave-no-failover",&g_pserver->cluster_slave_no_failover,1,CLUSTER_DEFAULT_SLAVE_NO_FAILOVER},
    {"replica-lazy-flush","slave-lazy-flush",&g_pserver->repl_slave_lazy_flush,1,CONFIG_DEFAULT_SLAVE_LAZY_FLUSH},
    {"replica-serve-stale-data","slave-serve-stale-data",&g_pserver->repl_serve_stale_data,1,CONFIG_DEFAULT_SLAVE_SERVE_STALE_DATA},
    {"replica-read-only","slave-read-only",&g_pserver->repl_slave_ro,1,CONFIG_DEFAULT_SLAVE_READ_ONLY},
    {"replica-ignore-maxmemory","slave-ignore-maxmemory",&g_pserver->repl_slave_ignore_maxmemory,1,CONFIG_DEFAULT_SLAVE_IGNORE_MAXMEMORY},
    {"multi-master",NULL,&g_pserver->enable_multimaster,false,CONFIG_DEFAULT_ENABLE_MULTIMASTER},
    {NULL, NULL, 0, 0}
};

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

int yesnotoi(char *s) {
    if (!strcasecmp(s,"yes")) return 1;
    else if (!strcasecmp(s,"no")) return 0;
    else return -1;
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

void loadServerConfigFromString(char *config) {
    const char *err = NULL;
    int linenum = 0, totlines, i;
    int slaveof_linenum = 0;
    sds *lines;

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
        for (configYesNo *config = configs_yesno; config->name != NULL; config++) {
            if ((!strcasecmp(argv[0],config->name) ||
                (config->alias && !strcasecmp(argv[0],config->alias))) &&
                (argc == 2)) 
            {
                if ((*(config->config) = yesnotoi(argv[1])) == -1) {
                    err = "argument must be 'yes' or 'no'"; goto loaderr;
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
        if (!strcasecmp(argv[0],"timeout") && argc == 2) {
            cserver.maxidletime = atoi(argv[1]);
            if (cserver.maxidletime < 0) {
                err = "Invalid timeout value"; goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"tcp-keepalive") && argc == 2) {
            cserver.tcpkeepalive = atoi(argv[1]);
            if (cserver.tcpkeepalive < 0) {
                err = "Invalid tcp-keepalive value"; goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"port") && argc == 2) {
            g_pserver->port = atoi(argv[1]);
            if (g_pserver->port < 0 || g_pserver->port > 65535) {
                err = "Invalid port"; goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"tcp-backlog") && argc == 2) {
            g_pserver->tcp_backlog = atoi(argv[1]);
            if (g_pserver->tcp_backlog < 0) {
                err = "Invalid backlog value"; goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"bind") && argc >= 2) {
            int j, addresses = argc-1;

            if (addresses > CONFIG_BINDADDR_MAX) {
                err = "Too many bind addresses specified"; goto loaderr;
            }
            for (j = 0; j < addresses; j++)
                g_pserver->bindaddr[j] = zstrdup(argv[j+1]);
            g_pserver->bindaddr_count = addresses;
        } else if (!strcasecmp(argv[0],"unixsocket") && argc == 2) {
            g_pserver->unixsocket = zstrdup(argv[1]);
        } else if (!strcasecmp(argv[0],"unixsocketperm") && argc == 2) {
            errno = 0;
            g_pserver->unixsocketperm = (mode_t)strtol(argv[1], NULL, 8);
            if (errno || g_pserver->unixsocketperm > 0777) {
                err = "Invalid socket file permissions"; goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"save")) {
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
        } else if (!strcasecmp(argv[0],"loglevel") && argc == 2) {
            cserver.verbosity = configEnumGetValue(loglevel_enum,argv[1]);
            if (cserver.verbosity == INT_MIN) {
                err = "Invalid log level. "
                      "Must be one of debug, verbose, notice, warning";
                goto loaderr;
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
        } else if (!strcasecmp(argv[0],"aclfile") && argc == 2) {
            zfree(g_pserver->acl_filename);
            g_pserver->acl_filename = zstrdup(argv[1]);
        } else if (!strcasecmp(argv[0],"syslog-enabled") && argc == 2) {
            if ((g_pserver->syslog_enabled = yesnotoi(argv[1])) == -1) {
                err = "argument must be 'yes' or 'no'"; goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"syslog-ident") && argc == 2) {
            if (g_pserver->syslog_ident) zfree(g_pserver->syslog_ident);
            g_pserver->syslog_ident = zstrdup(argv[1]);
        } else if (!strcasecmp(argv[0],"syslog-facility") && argc == 2) {
            g_pserver->syslog_facility =
                configEnumGetValue(syslog_facility_enum,argv[1]);
            if (g_pserver->syslog_facility == INT_MIN) {
                err = "Invalid log facility. Must be one of USER or between LOCAL0-LOCAL7";
                goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"databases") && argc == 2) {
            cserver.dbnum = atoi(argv[1]);
            if (cserver.dbnum < 1) {
                err = "Invalid number of databases"; goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"include") && argc == 2) {
            loadServerConfig(argv[1],NULL);
        } else if (!strcasecmp(argv[0],"maxclients") && argc == 2) {
            g_pserver->maxclients = atoi(argv[1]);
            if (g_pserver->maxclients < 1) {
                err = "Invalid max clients limit"; goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"maxmemory") && argc == 2) {
            g_pserver->maxmemory = memtoll(argv[1],NULL);
        } else if (!strcasecmp(argv[0],"maxmemory-policy") && argc == 2) {
            g_pserver->maxmemory_policy =
                configEnumGetValue(maxmemory_policy_enum,argv[1]);
            if (g_pserver->maxmemory_policy == INT_MIN) {
                err = "Invalid maxmemory policy";
                goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"maxmemory-samples") && argc == 2) {
            g_pserver->maxmemory_samples = atoi(argv[1]);
            if (g_pserver->maxmemory_samples <= 0) {
                err = "maxmemory-samples must be 1 or greater";
                goto loaderr;
            }
        } else if ((!strcasecmp(argv[0],"proto-max-bulk-len")) && argc == 2) {
            g_pserver->proto_max_bulk_len = memtoll(argv[1],NULL);
        } else if ((!strcasecmp(argv[0],"client-query-buffer-limit")) && argc == 2) {
            cserver.client_max_querybuf_len = memtoll(argv[1],NULL);
        } else if (!strcasecmp(argv[0],"lfu-log-factor") && argc == 2) {
            g_pserver->lfu_log_factor = atoi(argv[1]);
            if (g_pserver->lfu_log_factor < 0) {
                err = "lfu-log-factor must be 0 or greater";
                goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"lfu-decay-time") && argc == 2) {
            g_pserver->lfu_decay_time = atoi(argv[1]);
            if (g_pserver->lfu_decay_time < 0) {
                err = "lfu-decay-time must be 0 or greater";
                goto loaderr;
            }
        } else if ((!strcasecmp(argv[0],"slaveof") ||
                    !strcasecmp(argv[0],"replicaof")) && argc == 3) {
            slaveof_linenum = linenum;
            replicationAddMaster(sdsnew(argv[1]), atoi(argv[2]));
        } else if ((!strcasecmp(argv[0],"repl-ping-slave-period") ||
                    !strcasecmp(argv[0],"repl-ping-replica-period")) &&
                    argc == 2)
        {
            g_pserver->repl_ping_slave_period = atoi(argv[1]);
            if (g_pserver->repl_ping_slave_period <= 0) {
                err = "repl-ping-replica-period must be 1 or greater";
                goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"repl-timeout") && argc == 2) {
            g_pserver->repl_timeout = atoi(argv[1]);
            if (g_pserver->repl_timeout <= 0) {
                err = "repl-timeout must be 1 or greater";
                goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"repl-diskless-sync-delay") && argc==2) {
            g_pserver->repl_diskless_sync_delay = atoi(argv[1]);
            if (g_pserver->repl_diskless_sync_delay < 0) {
                err = "repl-diskless-sync-delay can't be negative";
                goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"repl-backlog-size") && argc == 2) {
            long long size = memtoll(argv[1],NULL);
            if (size <= 0) {
                err = "repl-backlog-size must be 1 or greater.";
                goto loaderr;
            }
            resizeReplicationBacklog(size);
        } else if (!strcasecmp(argv[0],"repl-backlog-ttl") && argc == 2) {
            g_pserver->repl_backlog_time_limit = atoi(argv[1]);
            if (g_pserver->repl_backlog_time_limit < 0) {
                err = "repl-backlog-ttl can't be negative ";
                goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"masteruser") && argc == 2) {
            zfree(cserver.default_masteruser);
            cserver.default_masteruser = argv[1][0] ? zstrdup(argv[1]) : NULL;
        } else if (!strcasecmp(argv[0],"masterauth") && argc == 2) {
            zfree(cserver.default_masterauth);
            cserver.default_masterauth = argv[1][0] ? zstrdup(argv[1]) : NULL;
            // Loop through all existing master infos and update them (in case this came after the replicaof config)
            updateMasterAuth();
        } else if (!strcasecmp(argv[0],"activedefrag") && argc == 2) {
            if ((cserver.active_defrag_enabled = yesnotoi(argv[1])) == -1) {
                err = "argument must be 'yes' or 'no'"; goto loaderr;
            }
            if (cserver.active_defrag_enabled) {
#ifndef HAVE_DEFRAG
                err = "active defrag can't be enabled without proper jemalloc support"; goto loaderr;
#endif
            }
        } else if (!strcasecmp(argv[0],"hz") && argc == 2) {
            g_pserver->config_hz = atoi(argv[1]);
            if (g_pserver->config_hz < CONFIG_MIN_HZ) g_pserver->config_hz = CONFIG_MIN_HZ;
            if (g_pserver->config_hz > CONFIG_MAX_HZ) g_pserver->config_hz = CONFIG_MAX_HZ;
        } else if (!strcasecmp(argv[0],"appendonly") && argc == 2) {
            int yes;

            if ((yes = yesnotoi(argv[1])) == -1) {
                err = "argument must be 'yes' or 'no'"; goto loaderr;
            }
            g_pserver->aof_state = yes ? AOF_ON : AOF_OFF;
        } else if (!strcasecmp(argv[0],"appendfilename") && argc == 2) {
            if (!pathIsBaseName(argv[1])) {
                err = "appendfilename can't be a path, just a filename";
                goto loaderr;
            }
            zfree(g_pserver->aof_filename);
            g_pserver->aof_filename = zstrdup(argv[1]);
        } else if (!strcasecmp(argv[0],"appendfsync") && argc == 2) {
            g_pserver->aof_fsync = configEnumGetValue(aof_fsync_enum,argv[1]);
            if (g_pserver->aof_fsync == INT_MIN) {
                err = "argument must be 'no', 'always' or 'everysec'";
                goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"auto-aof-rewrite-percentage") &&
                   argc == 2)
        {
            g_pserver->aof_rewrite_perc = atoi(argv[1]);
            if (g_pserver->aof_rewrite_perc < 0) {
                err = "Invalid negative percentage for AOF auto rewrite";
                goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"auto-aof-rewrite-min-size") &&
                   argc == 2)
        {
            g_pserver->aof_rewrite_min_size = memtoll(argv[1],NULL);
        } else if (!strcasecmp(argv[0],"requirepass") && argc == 2) {
            if (strlen(argv[1]) > CONFIG_AUTHPASS_MAX_LEN) {
                err = "Password is longer than CONFIG_AUTHPASS_MAX_LEN";
                goto loaderr;
            }
            /* The old "requirepass" directive just translates to setting
             * a password to the default user. */
            ACLSetUser(DefaultUser,"resetpass",-1);
            sds aclop = sdscatprintf(sdsempty(),">%s",argv[1]);
            ACLSetUser(DefaultUser,aclop,sdslen(aclop));
            sdsfree(aclop);
        } else if (!strcasecmp(argv[0],"pidfile") && argc == 2) {
            zfree(cserver.pidfile);
            cserver.pidfile = zstrdup(argv[1]);
        } else if (!strcasecmp(argv[0],"dbfilename") && argc == 2) {
            if (!pathIsBaseName(argv[1])) {
                err = "dbfilename can't be a path, just a filename";
                goto loaderr;
            }
            zfree(g_pserver->rdb_filename);
            g_pserver->rdb_filename = zstrdup(argv[1]);
        } else if(!strcasecmp(argv[0],"db-s3-object") && argc == 2) {
            zfree(g_pserver->rdb_s3bucketpath);
            g_pserver->rdb_s3bucketpath = zstrdup(argv[1]);
        } else if (!strcasecmp(argv[0],"active-defrag-threshold-lower") && argc == 2) {
            cserver.active_defrag_threshold_lower = atoi(argv[1]);
            if (cserver.active_defrag_threshold_lower < 0 ||
                cserver.active_defrag_threshold_lower > 1000) {
                err = "active-defrag-threshold-lower must be between 0 and 1000";
                goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"active-defrag-threshold-upper") && argc == 2) {
            cserver.active_defrag_threshold_upper = atoi(argv[1]);
            if (cserver.active_defrag_threshold_upper < 0 ||
                cserver.active_defrag_threshold_upper > 1000) {
                err = "active-defrag-threshold-upper must be between 0 and 1000";
                goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"active-defrag-ignore-bytes") && argc == 2) {
            cserver.active_defrag_ignore_bytes = memtoll(argv[1], NULL);
            if (cserver.active_defrag_ignore_bytes <= 0) {
                err = "active-defrag-ignore-bytes must above 0";
                goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"active-defrag-cycle-min") && argc == 2) {
            cserver.active_defrag_cycle_min = atoi(argv[1]);
            if (cserver.active_defrag_cycle_min < 1 || cserver.active_defrag_cycle_min > 99) {
                err = "active-defrag-cycle-min must be between 1 and 99";
                goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"active-defrag-cycle-max") && argc == 2) {
            cserver.active_defrag_cycle_max = atoi(argv[1]);
            if (cserver.active_defrag_cycle_max < 1 || cserver.active_defrag_cycle_max > 99) {
                err = "active-defrag-cycle-max must be between 1 and 99";
                goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"active-defrag-max-scan-fields") && argc == 2) {
            cserver.active_defrag_max_scan_fields = strtoll(argv[1],NULL,10);
            if (cserver.active_defrag_max_scan_fields < 1) {
                err = "active-defrag-max-scan-fields must be positive";
                goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"hash-max-ziplist-entries") && argc == 2) {
            g_pserver->hash_max_ziplist_entries = memtoll(argv[1], NULL);
        } else if (!strcasecmp(argv[0],"hash-max-ziplist-value") && argc == 2) {
            g_pserver->hash_max_ziplist_value = memtoll(argv[1], NULL);
        } else if (!strcasecmp(argv[0],"stream-node-max-bytes") && argc == 2) {
            g_pserver->stream_node_max_bytes = memtoll(argv[1], NULL);
        } else if (!strcasecmp(argv[0],"stream-node-max-entries") && argc == 2) {
            g_pserver->stream_node_max_entries = atoi(argv[1]);
        } else if (!strcasecmp(argv[0],"list-max-ziplist-entries") && argc == 2){
            /* DEAD OPTION */
        } else if (!strcasecmp(argv[0],"list-max-ziplist-value") && argc == 2) {
            /* DEAD OPTION */
        } else if (!strcasecmp(argv[0],"list-max-ziplist-size") && argc == 2) {
            g_pserver->list_max_ziplist_size = atoi(argv[1]);
        } else if (!strcasecmp(argv[0],"list-compress-depth") && argc == 2) {
            g_pserver->list_compress_depth = atoi(argv[1]);
        } else if (!strcasecmp(argv[0],"set-max-intset-entries") && argc == 2) {
            g_pserver->set_max_intset_entries = memtoll(argv[1], NULL);
        } else if (!strcasecmp(argv[0],"zset-max-ziplist-entries") && argc == 2) {
            g_pserver->zset_max_ziplist_entries = memtoll(argv[1], NULL);
        } else if (!strcasecmp(argv[0],"zset-max-ziplist-value") && argc == 2) {
            g_pserver->zset_max_ziplist_value = memtoll(argv[1], NULL);
        } else if (!strcasecmp(argv[0],"hll-sparse-max-bytes") && argc == 2) {
            g_pserver->hll_sparse_max_bytes = memtoll(argv[1], NULL);
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
        } else if (!strcasecmp(argv[0],"cluster-enabled") && argc == 2) {
            if ((g_pserver->cluster_enabled = yesnotoi(argv[1])) == -1) {
                err = "argument must be 'yes' or 'no'"; goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"cluster-config-file") && argc == 2) {
            zfree(g_pserver->cluster_configfile);
            g_pserver->cluster_configfile = zstrdup(argv[1]);
        } else if (!strcasecmp(argv[0],"cluster-announce-ip") && argc == 2) {
            zfree(g_pserver->cluster_announce_ip);
            g_pserver->cluster_announce_ip = zstrdup(argv[1]);
        } else if (!strcasecmp(argv[0],"cluster-announce-port") && argc == 2) {
            g_pserver->cluster_announce_port = atoi(argv[1]);
            if (g_pserver->cluster_announce_port < 0 ||
                g_pserver->cluster_announce_port > 65535)
            {
                err = "Invalid port"; goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"cluster-announce-bus-port") &&
                   argc == 2)
        {
            g_pserver->cluster_announce_bus_port = atoi(argv[1]);
            if (g_pserver->cluster_announce_bus_port < 0 ||
                g_pserver->cluster_announce_bus_port > 65535)
            {
                err = "Invalid port"; goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"cluster-node-timeout") && argc == 2) {
            g_pserver->cluster_node_timeout = strtoll(argv[1],NULL,10);
            if (g_pserver->cluster_node_timeout <= 0) {
                err = "cluster node timeout must be 1 or greater"; goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"cluster-migration-barrier")
                   && argc == 2)
        {
            g_pserver->cluster_migration_barrier = atoi(argv[1]);
            if (g_pserver->cluster_migration_barrier < 0) {
                err = "cluster migration barrier must zero or positive";
                goto loaderr;
            }
        } else if ((!strcasecmp(argv[0],"cluster-slave-validity-factor") ||
                    !strcasecmp(argv[0],"cluster-replica-validity-factor"))
                   && argc == 2)
        {
            g_pserver->cluster_slave_validity_factor = atoi(argv[1]);
            if (g_pserver->cluster_slave_validity_factor < 0) {
                err = "cluster replica validity factor must be zero or positive";
                goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"lua-time-limit") && argc == 2) {
            g_pserver->lua_time_limit = strtoll(argv[1],NULL,10);
        } else if (!strcasecmp(argv[0],"lua-replicate-commands") && argc == 2) {
            g_pserver->lua_always_replicate_commands = yesnotoi(argv[1]);
        } else if (!strcasecmp(argv[0],"slowlog-log-slower-than") &&
                   argc == 2)
        {
            g_pserver->slowlog_log_slower_than = strtoll(argv[1],NULL,10);
        } else if (!strcasecmp(argv[0],"latency-monitor-threshold") &&
                   argc == 2)
        {
            g_pserver->latency_monitor_threshold = strtoll(argv[1],NULL,10);
            if (g_pserver->latency_monitor_threshold < 0) {
                err = "The latency threshold can't be negative";
                goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"slowlog-max-len") && argc == 2) {
            g_pserver->slowlog_max_len = strtoll(argv[1],NULL,10);
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
        } else if ((!strcasecmp(argv[0],"slave-priority") ||
                    !strcasecmp(argv[0],"replica-priority")) && argc == 2)
        {
            g_pserver->slave_priority = atoi(argv[1]);
        } else if ((!strcasecmp(argv[0],"slave-announce-ip") ||
                    !strcasecmp(argv[0],"replica-announce-ip")) && argc == 2)
        {
            zfree(g_pserver->slave_announce_ip);
            g_pserver->slave_announce_ip = zstrdup(argv[1]);
        } else if ((!strcasecmp(argv[0],"slave-announce-port") ||
                    !strcasecmp(argv[0],"replica-announce-port")) && argc == 2)
        {
            g_pserver->slave_announce_port = atoi(argv[1]);
            if (g_pserver->slave_announce_port < 0 ||
                g_pserver->slave_announce_port > 65535)
            {
                err = "Invalid port"; goto loaderr;
            }
        } else if ((!strcasecmp(argv[0],"min-slaves-to-write") ||
                    !strcasecmp(argv[0],"min-replicas-to-write")) && argc == 2)
        {
            g_pserver->repl_min_slaves_to_write = atoi(argv[1]);
            if (g_pserver->repl_min_slaves_to_write < 0) {
                err = "Invalid value for min-replicas-to-write."; goto loaderr;
            }
        } else if ((!strcasecmp(argv[0],"min-slaves-max-lag") ||
                    !strcasecmp(argv[0],"min-replicas-max-lag")) && argc == 2)
        {
            g_pserver->repl_min_slaves_max_lag = atoi(argv[1]);
            if (g_pserver->repl_min_slaves_max_lag < 0) {
                err = "Invalid value for min-replicas-max-lag."; goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"notify-keyspace-events") && argc == 2) {
            int flags = keyspaceEventsStringToFlags(argv[1]);

            if (flags == -1) {
                err = "Invalid event class character. Use 'g$lshzxeA'.";
                goto loaderr;
            }
            g_pserver->notify_keyspace_events = flags;
        } else if (!strcasecmp(argv[0],"supervised") && argc == 2) {
            cserver.supervised_mode =
                configEnumGetValue(supervised_mode_enum,argv[1]);

            if (cserver.supervised_mode == INT_MIN) {
                err = "Invalid option for 'supervised'. "
                    "Allowed values: 'upstart', 'systemd', 'auto', or 'no'";
                goto loaderr;
            }
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
                err = sentinelHandleConfiguration(argv+1,argc-1);
                if (err) goto loaderr;
            }
        } else if (!strcasecmp(argv[0],"scratch-file-path")) {
#ifdef USE_MEMKIND
            storage_init(argv[1], g_pserver->maxmemory);
#else
            err = "KeyDB not compliled with scratch-file support.";
            goto loaderr;
#endif
        } else if (!strcasecmp(argv[0],"server-threads") && argc == 2) {
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
                err = "Unknown argument: server-thread-affinity expects either true or false";
                goto loaderr;
            }
        } else if (!strcasecmp(argv[0], "active-replica") && argc == 2) {
            g_pserver->fActiveReplica = yesnotoi(argv[1]);
            if (g_pserver->repl_slave_ro) {
                g_pserver->repl_slave_ro = FALSE;
                serverLog(LL_NOTICE, "Notice: \"active-replica yes\" implies \"replica-read-only no\"");
            }
            if (g_pserver->fActiveReplica == -1) {
                g_pserver->fActiveReplica = CONFIG_DEFAULT_ACTIVE_REPLICA;
                err = "argument must be 'yes' or 'no'"; goto loaderr;
            }
        } else if (!strcasecmp(argv[0], "version-override") && argc == 2) {
            KEYDB_SET_VERSION = zstrdup(argv[1]);
            serverLog(LL_WARNING, "Warning version is overriden to: %s\n", KEYDB_SET_VERSION);
        } else if (!strcasecmp(argv[0],"testmode") && argc == 2){
            g_fTestMode = yesnotoi(argv[1]);
        } else if (!strcasecmp(argv[0],"rdbfuzz-mode")) {
            // NOP, handled in main
        } else if (!strcasecmp(argv[0],"enable-pro")) {
            cserver.fUsePro = true;
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

    sdsfreesplitres(lines,totlines);
    return;

loaderr:
    fprintf(stderr, "\n*** FATAL CONFIG FILE ERROR ***\n");
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
void loadServerConfig(char *filename, char *options) {
    sds config = sdsempty();
    char buf[CONFIG_MAX_LINE+1];

    /* Load the file content */
    if (filename) {
        FILE *fp;

        if (filename[0] == '-' && filename[1] == '\0') {
            fp = stdin;
        } else {
            if ((fp = fopen(filename,"r")) == NULL) {
                serverLog(LL_WARNING,
                    "Fatal error, can't open config file '%s'", filename);
                exit(1);
            }
        }
        while(fgets(buf,CONFIG_MAX_LINE+1,fp) != NULL)
            config = sdscat(config,buf);
        if (fp != stdin) fclose(fp);
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

#define config_set_enum_field(_name,_var,_enumvar) \
    } else if (!strcasecmp(szFromObj(c->argv[2]),_name)) { \
        int enumval = configEnumGetValue(_enumvar,szFromObj(o)); \
        if (enumval == INT_MIN) goto badfmt; \
        _var = enumval;

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
    serverAssertWithInfo(c,c->argv[2],sdsEncodedObject(c->argv[2]));
    serverAssertWithInfo(c,c->argv[3],sdsEncodedObject(c->argv[3]));
    o = c->argv[3];

    /* Iterate the configs that are standard */
    for (configYesNo *config = configs_yesno; config->name != NULL; config++) {
        if(config->modifiable && (!strcasecmp(szFromObj(c->argv[2]),config->name) ||
            (config->alias && !strcasecmp(szFromObj(c->argv[2]),config->alias))))  
        {
            int yn = yesnotoi(szFromObj(o));
            if (yn == -1) goto badfmt;
            *(config->config) = yn;
            addReply(c,shared.ok);
            return;
        }
    }

    if (0) { /* this starts the config_set macros else-if chain. */

    /* Special fields that can't be handled with general macros. */
    config_set_special_field("dbfilename") {
        if (!pathIsBaseName(szFromObj(o))) {
            addReplyError(c, "dbfilename can't be a path, just a filename");
            return;
        }
        zfree(g_pserver->rdb_filename);
        g_pserver->rdb_filename = zstrdup(szFromObj(o));
    } config_set_special_field("requirepass") {
        if (sdslen(szFromObj(o)) > CONFIG_AUTHPASS_MAX_LEN) goto badfmt;
        /* The old "requirepass" directive just translates to setting
         * a password to the default user. */
        ACLSetUser(DefaultUser,"resetpass",-1);
        sds aclop = sdscatprintf(sdsempty(),">%s",(char*)ptrFromObj(o));
        ACLSetUser(DefaultUser,aclop,sdslen(aclop));
        sdsfree(aclop);
    } config_set_special_field("masteruser") {
        zfree(cserver.default_masteruser);
        cserver.default_masteruser = ((char*)ptrFromObj(o))[0] ? zstrdup(szFromObj(o)) : NULL;
    } config_set_special_field("masterauth") {
        zfree(cserver.default_masterauth);
        cserver.default_masterauth = ((char*)ptrFromObj(o))[0] ? zstrdup(szFromObj(o)) : NULL;
    } config_set_special_field("cluster-announce-ip") {
        zfree(g_pserver->cluster_announce_ip);
        g_pserver->cluster_announce_ip = ((char*)ptrFromObj(o))[0] ? zstrdup(szFromObj(o)) : NULL;
    } config_set_special_field("maxclients") {
        int orig_value = g_pserver->maxclients;

        if (getLongLongFromObject(o,&ll) == C_ERR || ll < 1) goto badfmt;

        /* Try to check if the OS is capable of supporting so many FDs. */
        g_pserver->maxclients = ll;
        serverAssert(FALSE);
        if (ll > orig_value) {
            adjustOpenFilesLimit();
            if (g_pserver->maxclients != ll) {
                addReplyErrorFormat(c,"The operating system is not able to handle the specified number of clients, try with %d", g_pserver->maxclients);
                g_pserver->maxclients = orig_value;
                return;
            }
            if ((unsigned int) aeGetSetSize(g_pserver->rgthreadvar[IDX_EVENT_LOOP_MAIN].el) <
                g_pserver->maxclients + CONFIG_FDSET_INCR)
            {
                for (int iel = 0; iel < cserver.cthreads; ++iel)
                {
                    if (aeResizeSetSize(g_pserver->rgthreadvar[iel].el,
                        g_pserver->maxclients + CONFIG_FDSET_INCR) == AE_ERR)
                    {
                        addReplyError(c,"The event loop API used by Redis is not able to handle the specified number of clients");
                        g_pserver->maxclients = orig_value;
                        return;
                    }
                }
            }
        }
    } config_set_special_field("appendonly") {
        int enable = yesnotoi(szFromObj(o));

        if (enable == -1) goto badfmt;
        if (enable == 0 && g_pserver->aof_state != AOF_OFF) {
            stopAppendOnly();
        } else if (enable && g_pserver->aof_state == AOF_OFF) {
            if (startAppendOnly() == C_ERR) {
                addReplyError(c,
                    "Unable to turn on AOF. Check server logs.");
                return;
            }
        }
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
    } config_set_special_field("notify-keyspace-events") {
        int flags = keyspaceEventsStringToFlags(szFromObj(o));

        if (flags == -1) goto badfmt;
        g_pserver->notify_keyspace_events = flags;
    } config_set_special_field_with_alias("slave-announce-ip",
                                          "replica-announce-ip")
    {
        zfree(g_pserver->slave_announce_ip);
        g_pserver->slave_announce_ip = ((char*)ptrFromObj(o))[0] ? zstrdup(szFromObj(o)) : NULL;

    /* Boolean fields.
     * config_set_bool_field(name,var). */
    } config_set_bool_field(
      "activedefrag",cserver.active_defrag_enabled) {
#ifndef HAVE_DEFRAG
        if (cserver.active_defrag_enabled) {
            cserver.active_defrag_enabled = 0;
            addReplyError(c,
                "-DISABLED Active defragmentation cannot be enabled: it "
                "requires a Redis server compiled with a modified Jemalloc "
                "like the one shipped by default with the Redis source "
                "distribution");
            return;
        }
#endif

    /* Numerical fields.
     * config_set_numerical_field(name,var,min,max) */
    } config_set_numerical_field(
      "tcp-keepalive",cserver.tcpkeepalive,0,INT_MAX) {
    } config_set_numerical_field(
      "maxmemory-samples",g_pserver->maxmemory_samples,1,INT_MAX) {
    } config_set_numerical_field(
      "lfu-log-factor",g_pserver->lfu_log_factor,0,INT_MAX) {
    } config_set_numerical_field(
      "lfu-decay-time",g_pserver->lfu_decay_time,0,INT_MAX) {
    } config_set_numerical_field(
      "timeout",cserver.maxidletime,0,INT_MAX) {
    } config_set_numerical_field(
      "active-defrag-threshold-lower",cserver.active_defrag_threshold_lower,0,1000) {
    } config_set_numerical_field(
      "active-defrag-threshold-upper",cserver.active_defrag_threshold_upper,0,1000) {
    } config_set_memory_field(
      "active-defrag-ignore-bytes",cserver.active_defrag_ignore_bytes) {
    } config_set_numerical_field(
      "active-defrag-cycle-min",cserver.active_defrag_cycle_min,1,99) {
    } config_set_numerical_field(
      "active-defrag-cycle-max",cserver.active_defrag_cycle_max,1,99) {
    } config_set_numerical_field(
      "active-defrag-max-scan-fields",cserver.active_defrag_max_scan_fields,1,LONG_MAX) {
    } config_set_numerical_field(
      "auto-aof-rewrite-percentage",g_pserver->aof_rewrite_perc,0,INT_MAX){
    } config_set_numerical_field(
      "hash-max-ziplist-entries",g_pserver->hash_max_ziplist_entries,0,LONG_MAX) {
    } config_set_numerical_field(
      "hash-max-ziplist-value",g_pserver->hash_max_ziplist_value,0,LONG_MAX) {
    } config_set_numerical_field(
      "stream-node-max-bytes",g_pserver->stream_node_max_bytes,0,LONG_MAX) {
    } config_set_numerical_field(
      "stream-node-max-entries",g_pserver->stream_node_max_entries,0,LLONG_MAX) {
    } config_set_numerical_field(
      "list-max-ziplist-size",g_pserver->list_max_ziplist_size,INT_MIN,INT_MAX) {
    } config_set_numerical_field(
      "list-compress-depth",g_pserver->list_compress_depth,0,INT_MAX) {
    } config_set_numerical_field(
      "set-max-intset-entries",g_pserver->set_max_intset_entries,0,LONG_MAX) {
    } config_set_numerical_field(
      "zset-max-ziplist-entries",g_pserver->zset_max_ziplist_entries,0,LONG_MAX) {
    } config_set_numerical_field(
      "zset-max-ziplist-value",g_pserver->zset_max_ziplist_value,0,LONG_MAX) {
    } config_set_numerical_field(
      "hll-sparse-max-bytes",g_pserver->hll_sparse_max_bytes,0,LONG_MAX) {
    } config_set_numerical_field(
      "lua-time-limit",g_pserver->lua_time_limit,0,LONG_MAX) {
    } config_set_numerical_field(
      "slowlog-log-slower-than",g_pserver->slowlog_log_slower_than,-1,LLONG_MAX) {
    } config_set_numerical_field(
      "slowlog-max-len",ll,0,LONG_MAX) {
      /* Cast to unsigned. */
        g_pserver->slowlog_max_len = (unsigned long)ll;
    } config_set_numerical_field(
      "latency-monitor-threshold",g_pserver->latency_monitor_threshold,0,LLONG_MAX){
    } config_set_numerical_field(
      "repl-ping-slave-period",g_pserver->repl_ping_slave_period,1,INT_MAX) {
    } config_set_numerical_field(
      "repl-ping-replica-period",g_pserver->repl_ping_slave_period,1,INT_MAX) {
    } config_set_numerical_field(
      "repl-timeout",g_pserver->repl_timeout,1,INT_MAX) {
    } config_set_numerical_field(
      "repl-backlog-ttl",g_pserver->repl_backlog_time_limit,0,LONG_MAX) {
    } config_set_numerical_field(
      "repl-diskless-sync-delay",g_pserver->repl_diskless_sync_delay,0,INT_MAX) {
    } config_set_numerical_field(
      "slave-priority",g_pserver->slave_priority,0,INT_MAX) {
    } config_set_numerical_field(
      "replica-priority",g_pserver->slave_priority,0,INT_MAX) {
    } config_set_numerical_field(
      "slave-announce-port",g_pserver->slave_announce_port,0,65535) {
    } config_set_numerical_field(
      "replica-announce-port",g_pserver->slave_announce_port,0,65535) {
    } config_set_numerical_field(
      "min-slaves-to-write",g_pserver->repl_min_slaves_to_write,0,INT_MAX) {
        refreshGoodSlavesCount();
    } config_set_numerical_field(
      "min-replicas-to-write",g_pserver->repl_min_slaves_to_write,0,INT_MAX) {
        refreshGoodSlavesCount();
    } config_set_numerical_field(
      "min-slaves-max-lag",g_pserver->repl_min_slaves_max_lag,0,INT_MAX) {
        refreshGoodSlavesCount();
    } config_set_numerical_field(
      "min-replicas-max-lag",g_pserver->repl_min_slaves_max_lag,0,INT_MAX) {
        refreshGoodSlavesCount();
    } config_set_numerical_field(
      "cluster-node-timeout",g_pserver->cluster_node_timeout,0,LLONG_MAX) {
    } config_set_numerical_field(
      "cluster-announce-port",g_pserver->cluster_announce_port,0,65535) {
    } config_set_numerical_field(
      "cluster-announce-bus-port",g_pserver->cluster_announce_bus_port,0,65535) {
    } config_set_numerical_field(
      "cluster-migration-barrier",g_pserver->cluster_migration_barrier,0,INT_MAX){
    } config_set_numerical_field(
      "cluster-slave-validity-factor",g_pserver->cluster_slave_validity_factor,0,INT_MAX) {
    } config_set_numerical_field(
      "cluster-replica-validity-factor",g_pserver->cluster_slave_validity_factor,0,INT_MAX) {
    } config_set_numerical_field(
      "hz",g_pserver->config_hz,0,INT_MAX) {
        /* Hz is more an hint from the user, so we accept values out of range
         * but cap them to reasonable values. */
        if (g_pserver->config_hz < CONFIG_MIN_HZ) g_pserver->config_hz = CONFIG_MIN_HZ;
        if (g_pserver->config_hz > CONFIG_MAX_HZ) g_pserver->config_hz = CONFIG_MAX_HZ;
    } config_set_numerical_field(
      "watchdog-period",ll,0,INT_MAX) {
        if (ll)
            enableWatchdog(ll);
        else
            disableWatchdog();

    /* Memory fields.
     * config_set_memory_field(name,var) */
    } config_set_memory_field("maxmemory",g_pserver->maxmemory) {
        if (g_pserver->maxmemory) {
            if (g_pserver->maxmemory < zmalloc_used_memory()) {
                serverLog(LL_WARNING,"WARNING: the new maxmemory value set via CONFIG SET is smaller than the current memory usage. This will result in key eviction and/or the inability to accept new write commands depending on the maxmemory-policy.");
            }
            freeMemoryIfNeededAndSafe();
        }
    } config_set_memory_field(
      "proto-max-bulk-len",g_pserver->proto_max_bulk_len) {
    } config_set_memory_field(
      "client-query-buffer-limit",cserver.client_max_querybuf_len) {
    } config_set_memory_field("repl-backlog-size",ll) {
        resizeReplicationBacklog(ll);
    } config_set_memory_field("auto-aof-rewrite-min-size",ll) {
        g_pserver->aof_rewrite_min_size = ll;

    /* Enumeration fields.
     * config_set_enum_field(name,var,enum_var) */
    } config_set_enum_field(
      "loglevel",cserver.verbosity,loglevel_enum) {
    } config_set_enum_field(
      "maxmemory-policy",g_pserver->maxmemory_policy,maxmemory_policy_enum) {
    } config_set_enum_field(
      "appendfsync",g_pserver->aof_fsync,aof_fsync_enum) {

    /* Everyhing else is an error... */
    } config_set_else {
        addReplyErrorFormat(c,"Unsupported CONFIG parameter: %s",
            (char*)ptrFromObj(c->argv[2]));
        return;
    }

    /* On success we just return a generic OK for all the options. */
    addReply(c,shared.ok);
    return;

badfmt: /* Bad format errors */
    addReplyErrorFormat(c,"Invalid argument '%s' for CONFIG SET '%s'",
            (char*)ptrFromObj(o),
            (char*)ptrFromObj(c->argv[2]));
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
} while(0);

#define config_get_bool_field(_name,_var) do { \
    if (stringmatch(pattern,_name,1)) { \
        addReplyBulkCString(c,_name); \
        addReplyBulkCString(c,_var ? "yes" : "no"); \
        matches++; \
    } \
} while(0);

#define config_get_numerical_field(_name,_var) do { \
    if (stringmatch(pattern,_name,1)) { \
        ll2string(buf,sizeof(buf),_var); \
        addReplyBulkCString(c,_name); \
        addReplyBulkCString(c,buf); \
        matches++; \
    } \
} while(0);

#define config_get_enum_field(_name,_var,_enumvar) do { \
    if (stringmatch(pattern,_name,1)) { \
        addReplyBulkCString(c,_name); \
        addReplyBulkCString(c,configEnumGetNameOrUnknown(_enumvar,_var)); \
        matches++; \
    } \
} while(0);

void configGetCommand(client *c) {
    robj *o = c->argv[2];
    void *replylen = addReplyDeferredLen(c);
    char *pattern = szFromObj(o);
    char buf[128];
    int matches = 0;
    serverAssertWithInfo(c,o,sdsEncodedObject(o));

    /* String values */
    config_get_string_field("dbfilename",g_pserver->rdb_filename);
    config_get_string_field("masteruser",cserver.default_masteruser);
    config_get_string_field("masterauth",cserver.default_masterauth);
    config_get_string_field("cluster-announce-ip",g_pserver->cluster_announce_ip);
    config_get_string_field("unixsocket",g_pserver->unixsocket);
    config_get_string_field("logfile",g_pserver->logfile);
    config_get_string_field("aclfile",g_pserver->acl_filename);
    config_get_string_field("pidfile",cserver.pidfile);
    config_get_string_field("slave-announce-ip",g_pserver->slave_announce_ip);
    config_get_string_field("replica-announce-ip",g_pserver->slave_announce_ip);
    config_get_string_field("version-override",KEYDB_SET_VERSION);

    /* Numerical values */
    config_get_numerical_field("maxmemory",g_pserver->maxmemory);
    config_get_numerical_field("proto-max-bulk-len",g_pserver->proto_max_bulk_len);
    config_get_numerical_field("client-query-buffer-limit",cserver.client_max_querybuf_len);
    config_get_numerical_field("maxmemory-samples",g_pserver->maxmemory_samples);
    config_get_numerical_field("lfu-log-factor",g_pserver->lfu_log_factor);
    config_get_numerical_field("lfu-decay-time",g_pserver->lfu_decay_time);
    config_get_numerical_field("timeout",cserver.maxidletime);
    config_get_numerical_field("active-defrag-threshold-lower",cserver.active_defrag_threshold_lower);
    config_get_numerical_field("active-defrag-threshold-upper",cserver.active_defrag_threshold_upper);
    config_get_numerical_field("active-defrag-ignore-bytes",cserver.active_defrag_ignore_bytes);
    config_get_numerical_field("active-defrag-cycle-min",cserver.active_defrag_cycle_min);
    config_get_numerical_field("active-defrag-cycle-max",cserver.active_defrag_cycle_max);
    config_get_numerical_field("active-defrag-max-scan-fields",cserver.active_defrag_max_scan_fields);
    config_get_numerical_field("auto-aof-rewrite-percentage",
            g_pserver->aof_rewrite_perc);
    config_get_numerical_field("auto-aof-rewrite-min-size",
            g_pserver->aof_rewrite_min_size);
    config_get_numerical_field("hash-max-ziplist-entries",
            g_pserver->hash_max_ziplist_entries);
    config_get_numerical_field("hash-max-ziplist-value",
            g_pserver->hash_max_ziplist_value);
    config_get_numerical_field("stream-node-max-bytes",
            g_pserver->stream_node_max_bytes);
    config_get_numerical_field("stream-node-max-entries",
            g_pserver->stream_node_max_entries);
    config_get_numerical_field("list-max-ziplist-size",
            g_pserver->list_max_ziplist_size);
    config_get_numerical_field("list-compress-depth",
            g_pserver->list_compress_depth);
    config_get_numerical_field("set-max-intset-entries",
            g_pserver->set_max_intset_entries);
    config_get_numerical_field("zset-max-ziplist-entries",
            g_pserver->zset_max_ziplist_entries);
    config_get_numerical_field("zset-max-ziplist-value",
            g_pserver->zset_max_ziplist_value);
    config_get_numerical_field("hll-sparse-max-bytes",
            g_pserver->hll_sparse_max_bytes);
    config_get_numerical_field("lua-time-limit",g_pserver->lua_time_limit);
    config_get_numerical_field("slowlog-log-slower-than",
            g_pserver->slowlog_log_slower_than);
    config_get_numerical_field("latency-monitor-threshold",
            g_pserver->latency_monitor_threshold);
    config_get_numerical_field("slowlog-max-len",
            g_pserver->slowlog_max_len);
    config_get_numerical_field("port",g_pserver->port);
    config_get_numerical_field("cluster-announce-port",g_pserver->cluster_announce_port);
    config_get_numerical_field("cluster-announce-bus-port",g_pserver->cluster_announce_bus_port);
    config_get_numerical_field("tcp-backlog",g_pserver->tcp_backlog);
    config_get_numerical_field("databases",cserver.dbnum);
    config_get_numerical_field("repl-ping-slave-period",g_pserver->repl_ping_slave_period);
    config_get_numerical_field("repl-ping-replica-period",g_pserver->repl_ping_slave_period);
    config_get_numerical_field("repl-timeout",g_pserver->repl_timeout);
    config_get_numerical_field("repl-backlog-size",g_pserver->repl_backlog_size);
    config_get_numerical_field("repl-backlog-ttl",g_pserver->repl_backlog_time_limit);
    config_get_numerical_field("maxclients",g_pserver->maxclients);
    config_get_numerical_field("watchdog-period",g_pserver->watchdog_period);
    config_get_numerical_field("slave-priority",g_pserver->slave_priority);
    config_get_numerical_field("replica-priority",g_pserver->slave_priority);
    config_get_numerical_field("slave-announce-port",g_pserver->slave_announce_port);
    config_get_numerical_field("replica-announce-port",g_pserver->slave_announce_port);
    config_get_numerical_field("min-slaves-to-write",g_pserver->repl_min_slaves_to_write);
    config_get_numerical_field("min-replicas-to-write",g_pserver->repl_min_slaves_to_write);
    config_get_numerical_field("min-slaves-max-lag",g_pserver->repl_min_slaves_max_lag);
    config_get_numerical_field("min-replicas-max-lag",g_pserver->repl_min_slaves_max_lag);
    config_get_numerical_field("hz",g_pserver->config_hz);
    config_get_numerical_field("cluster-node-timeout",g_pserver->cluster_node_timeout);
    config_get_numerical_field("cluster-migration-barrier",g_pserver->cluster_migration_barrier);
    config_get_numerical_field("cluster-slave-validity-factor",g_pserver->cluster_slave_validity_factor);
    config_get_numerical_field("cluster-replica-validity-factor",g_pserver->cluster_slave_validity_factor);
    config_get_numerical_field("repl-diskless-sync-delay",g_pserver->repl_diskless_sync_delay);
    config_get_numerical_field("tcp-keepalive",cserver.tcpkeepalive);

    /* Bool (yes/no) values */
    /* Iterate the configs that are standard */
    for (configYesNo *config = configs_yesno; config->name != NULL; config++) {
        config_get_bool_field(config->name, *(config->config));
        if (config->alias) {
            config_get_bool_field(config->alias, *(config->config));
        }
    }

    config_get_bool_field("activedefrag", cserver.active_defrag_enabled);

    /* Enum values */
    config_get_enum_field("maxmemory-policy",
            g_pserver->maxmemory_policy,maxmemory_policy_enum);
    config_get_enum_field("loglevel",
            cserver.verbosity,loglevel_enum);
    config_get_enum_field("supervised",
            cserver.supervised_mode,supervised_mode_enum);
    config_get_enum_field("appendfsync",
            g_pserver->aof_fsync,aof_fsync_enum);
    config_get_enum_field("syslog-facility",
            g_pserver->syslog_facility,syslog_facility_enum);

    /* Everything we can't handle with macros follows. */

    if (stringmatch(pattern,"appendonly",1)) {
        addReplyBulkCString(c,"appendonly");
        addReplyBulkCString(c,g_pserver->aof_state == AOF_OFF ? "no" : "yes");
        matches++;
    }
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
        snprintf(buf,sizeof(buf),"%o",g_pserver->unixsocketperm);
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
            while ((ln = listNext(&li)))
            {
                struct redisMaster *mi = (struct redisMaster*)listNodeValue(ln);
                snprintf(buf,sizeof(buf),"%s %d",
                    mi->masterhost, mi->masterport);
                addReplyBulkCString(c,buf);
            }
        }
        matches++;
    }
    if (stringmatch(pattern,"notify-keyspace-events",1)) {
        robj *flagsobj = createObject(OBJ_STRING,
            keyspaceEventsFlagsToString(g_pserver->notify_keyspace_events));

        addReplyBulkCString(c,"notify-keyspace-events");
        addReplyBulk(c,flagsobj);
        decrRefCount(flagsobj);
        matches++;
    }
    if (stringmatch(pattern,"bind",1)) {
        sds aux = sdsjoin(g_pserver->bindaddr,g_pserver->bindaddr_count," ");

        addReplyBulkCString(c,"bind");
        addReplyBulkCString(c,aux);
        sdsfree(aux);
        matches++;
    }
    if (stringmatch(pattern,"requirepass",1)) {
        addReplyBulkCString(c,"requirepass");
        sds password = ACLDefaultUserFirstPassword();
        if (password) {
            addReplyBulkCBuffer(c,password,sdslen(password));
        } else {
            addReplyBulkCString(c,"");
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
    dictListDestructor          /* val destructor */
};

dictType optionSetDictType = {
    dictSdsCaseHash,            /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCaseCompare,      /* key compare */
    dictSdsDestructor,          /* key destructor */
    NULL                        /* val destructor */
};

/* The config rewrite state. */
struct rewriteConfigState {
    dict *option_to_line; /* Option -> list of config file lines map */
    dict *rewritten;      /* Dictionary of already processed options */
    int numlines;         /* Number of lines in current config */
    sds *lines;           /* Current lines as an array of sds strings */
    int has_tail;         /* True if we already added directives that were
                             not present in the original config file. */
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
            alt = sdscatlen(alt,argv[0],p-argv[0]);;
            alt = sdscatlen(alt,"replica",7);
            alt = sdscatlen(alt,p+5,strlen(p+5));
            sdsfree(argv[0]);
            argv[0] = alt;
        }
        rewriteConfigAddLineNumberToOption(state,argv[0],linenum);
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

    if (!l && !force) {
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

/* Rewrite a numerical (long long range) option. */
void rewriteConfigNumericalOption(struct rewriteConfigState *state, const char *option, long long value, long long defvalue) {
    int force = value != defvalue;
    sds line = sdscatprintf(sdsempty(),"%s %lld",option,value);

    rewriteConfigRewriteLine(state,option,line,force);
}

/* Rewrite a octal option. */
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

/* Rewrite the syslog-facility option. */
void rewriteConfigSyslogfacilityOption(struct rewriteConfigState *state) {
    int value = g_pserver->syslog_facility;
    int force = value != LOG_LOCAL0;
    const char *name = NULL, *option = "syslog-facility";
    sds line;

    name = configEnumGetNameOrUnknown(syslog_facility_enum,value);
    line = sdscatprintf(sdsempty(),"%s %s",option,name);
    rewriteConfigRewriteLine(state,option,line,force);
}

/* Rewrite the save option. */
void rewriteConfigSaveOption(struct rewriteConfigState *state) {
    int j;
    sds line;

    /* Note that if there are no save parameters at all, all the current
     * config line with "save" will be detected as orphaned and deleted,
     * resulting into no RDB persistence as expected. */
    for (j = 0; j < g_pserver->saveparamslen; j++) {
        line = sdscatprintf(sdsempty(),"save %ld %d",
            (long) g_pserver->saveparams[j].seconds, g_pserver->saveparams[j].changes);
        rewriteConfigRewriteLine(state,"save",line,1);
    }
    /* Mark "save" as processed in case g_pserver->saveparamslen is zero. */
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

/* Rewrite the requirepass option. */
void rewriteConfigRequirepassOption(struct rewriteConfigState *state, const char *option) {
    int force = 1;
    sds line;
    sds password = ACLDefaultUserFirstPassword();

    /* If there is no password set, we don't want the requirepass option
     * to be present in the configuration at all. */
    if (password == NULL) {
        rewriteConfigMarkAsProcessed(state,option);
        return;
    }

    line = sdsnew(option);
    line = sdscatlen(line, " ", 1);
    line = sdscatsds(line, password);

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

/* This function overwrites the old configuration file with the new content.
 *
 * 1) The old file length is obtained.
 * 2) If the new content is smaller, padding is added.
 * 3) A single write(2) call is used to replace the content of the file.
 * 4) Later the file is truncated to the length of the new content.
 *
 * This way we are sure the file is left in a consistent state even if the
 * process is stopped between any of the four operations.
 *
 * The function returns 0 on success, otherwise -1 is returned and errno
 * set accordingly. */
int rewriteConfigOverwriteFile(char *configfile, sds content) {
    int retval = 0;
    int fd = open(configfile,O_RDWR|O_CREAT,0644);
    int content_size = sdslen(content), padding = 0;
    struct stat sb;
    sds content_padded;

    /* 1) Open the old file (or create a new one if it does not
     *    exist), get the size. */
    if (fd == -1) return -1; /* errno set by open(). */
    if (fstat(fd,&sb) == -1) {
        close(fd);
        return -1; /* errno set by fstat(). */
    }

    /* 2) Pad the content at least match the old file size. */
    content_padded = sdsdup(content);
    if (content_size < sb.st_size) {
        /* If the old file was bigger, pad the content with
         * a newline plus as many "#" chars as required. */
        padding = sb.st_size - content_size;
        content_padded = sdsgrowzero(content_padded,sb.st_size);
        content_padded[content_size] = '\n';
        memset(content_padded+content_size+1,'#',padding-1);
    }

    /* 3) Write the new content using a single write(2). */
    if (write(fd,content_padded,strlen(content_padded)) == -1) {
        retval = -1;
        goto cleanup;
    }

    /* 4) Truncate the file to the right length if we used padding. */
    if (padding) {
        if (ftruncate(fd,content_size) == -1) {
            /* Non critical error... */
        }
    }

cleanup:
    sdsfree(content_padded);
    close(fd);
    return retval;
}

/* Rewrite the configuration file at "path".
 * If the configuration file already exists, we try at best to retain comments
 * and overall structure.
 *
 * Configuration parameters that are at their default value, unless already
 * explicitly included in the old configuration file, are not rewritten.
 *
 * On error -1 is returned and errno is set accordingly, otherwise 0. */
int rewriteConfig(char *path) {
    struct rewriteConfigState *state;
    sds newcontent;
    int retval;

    /* Step 1: read the old config into our rewrite state. */
    if ((state = rewriteConfigReadOldFile(path)) == NULL) return -1;

    /* Step 2: rewrite every single option, replacing or appending it inside
     * the rewrite state. */

    /* Iterate the configs that are standard */
    for (configYesNo *config = configs_yesno; config->name != NULL; config++) {
        rewriteConfigYesNoOption(state,config->name,*(config->config),config->default_value);
    }

    rewriteConfigStringOption(state,"pidfile",cserver.pidfile,CONFIG_DEFAULT_PID_FILE);
    rewriteConfigNumericalOption(state,"port",g_pserver->port,CONFIG_DEFAULT_SERVER_PORT);
    rewriteConfigNumericalOption(state,"cluster-announce-port",g_pserver->cluster_announce_port,CONFIG_DEFAULT_CLUSTER_ANNOUNCE_PORT);
    rewriteConfigNumericalOption(state,"cluster-announce-bus-port",g_pserver->cluster_announce_bus_port,CONFIG_DEFAULT_CLUSTER_ANNOUNCE_BUS_PORT);
    rewriteConfigNumericalOption(state,"tcp-backlog",g_pserver->tcp_backlog,CONFIG_DEFAULT_TCP_BACKLOG);
    rewriteConfigBindOption(state);
    rewriteConfigStringOption(state,"unixsocket",g_pserver->unixsocket,NULL);
    rewriteConfigOctalOption(state,"unixsocketperm",g_pserver->unixsocketperm,CONFIG_DEFAULT_UNIX_SOCKET_PERM);
    rewriteConfigNumericalOption(state,"timeout",cserver.maxidletime,CONFIG_DEFAULT_CLIENT_TIMEOUT);
    rewriteConfigNumericalOption(state,"tcp-keepalive",cserver.tcpkeepalive,CONFIG_DEFAULT_TCP_KEEPALIVE);
    rewriteConfigNumericalOption(state,"replica-announce-port",g_pserver->slave_announce_port,CONFIG_DEFAULT_SLAVE_ANNOUNCE_PORT);
    rewriteConfigEnumOption(state,"loglevel",cserver.verbosity,loglevel_enum,CONFIG_DEFAULT_VERBOSITY);
    rewriteConfigStringOption(state,"logfile",g_pserver->logfile,CONFIG_DEFAULT_LOGFILE);
    rewriteConfigStringOption(state,"aclfile",g_pserver->acl_filename,CONFIG_DEFAULT_ACL_FILENAME);
    rewriteConfigYesNoOption(state,"syslog-enabled",g_pserver->syslog_enabled,CONFIG_DEFAULT_SYSLOG_ENABLED);
    rewriteConfigStringOption(state,"syslog-ident",g_pserver->syslog_ident,CONFIG_DEFAULT_SYSLOG_IDENT);
    rewriteConfigSyslogfacilityOption(state);
    rewriteConfigSaveOption(state);
    rewriteConfigUserOption(state);
    rewriteConfigNumericalOption(state,"databases",cserver.dbnum,CONFIG_DEFAULT_DBNUM);
    rewriteConfigStringOption(state,"dbfilename",g_pserver->rdb_filename,CONFIG_DEFAULT_RDB_FILENAME);
    rewriteConfigDirOption(state);
    rewriteConfigSlaveofOption(state,"replicaof");
    rewriteConfigStringOption(state,"replica-announce-ip",g_pserver->slave_announce_ip,CONFIG_DEFAULT_SLAVE_ANNOUNCE_IP);
    rewriteConfigStringOption(state,"masteruser",cserver.default_masteruser,NULL);
    rewriteConfigStringOption(state,"masterauth",cserver.default_masterauth,NULL);
    rewriteConfigStringOption(state,"cluster-announce-ip",g_pserver->cluster_announce_ip,NULL);
    rewriteConfigNumericalOption(state,"repl-ping-replica-period",g_pserver->repl_ping_slave_period,CONFIG_DEFAULT_REPL_PING_SLAVE_PERIOD);
    rewriteConfigNumericalOption(state,"repl-timeout",g_pserver->repl_timeout,CONFIG_DEFAULT_REPL_TIMEOUT);
    rewriteConfigBytesOption(state,"repl-backlog-size",g_pserver->repl_backlog_size,CONFIG_DEFAULT_REPL_BACKLOG_SIZE);
    rewriteConfigBytesOption(state,"repl-backlog-ttl",g_pserver->repl_backlog_time_limit,CONFIG_DEFAULT_REPL_BACKLOG_TIME_LIMIT);
    rewriteConfigNumericalOption(state,"repl-diskless-sync-delay",g_pserver->repl_diskless_sync_delay,CONFIG_DEFAULT_REPL_DISKLESS_SYNC_DELAY);
    rewriteConfigNumericalOption(state,"replica-priority",g_pserver->slave_priority,CONFIG_DEFAULT_SLAVE_PRIORITY);
    rewriteConfigNumericalOption(state,"min-replicas-to-write",g_pserver->repl_min_slaves_to_write,CONFIG_DEFAULT_MIN_SLAVES_TO_WRITE);
    rewriteConfigNumericalOption(state,"min-replicas-max-lag",g_pserver->repl_min_slaves_max_lag,CONFIG_DEFAULT_MIN_SLAVES_MAX_LAG);
    rewriteConfigRequirepassOption(state,"requirepass");
    rewriteConfigNumericalOption(state,"maxclients",g_pserver->maxclients,CONFIG_DEFAULT_MAX_CLIENTS);
    rewriteConfigBytesOption(state,"maxmemory",g_pserver->maxmemory,CONFIG_DEFAULT_MAXMEMORY);
    rewriteConfigBytesOption(state,"proto-max-bulk-len",g_pserver->proto_max_bulk_len,CONFIG_DEFAULT_PROTO_MAX_BULK_LEN);
    rewriteConfigBytesOption(state,"client-query-buffer-limit",cserver.client_max_querybuf_len,PROTO_MAX_QUERYBUF_LEN);
    rewriteConfigEnumOption(state,"maxmemory-policy",g_pserver->maxmemory_policy,maxmemory_policy_enum,CONFIG_DEFAULT_MAXMEMORY_POLICY);
    rewriteConfigNumericalOption(state,"maxmemory-samples",g_pserver->maxmemory_samples,CONFIG_DEFAULT_MAXMEMORY_SAMPLES);
    rewriteConfigNumericalOption(state,"lfu-log-factor",g_pserver->lfu_log_factor,CONFIG_DEFAULT_LFU_LOG_FACTOR);
    rewriteConfigNumericalOption(state,"lfu-decay-time",g_pserver->lfu_decay_time,CONFIG_DEFAULT_LFU_DECAY_TIME);
    rewriteConfigNumericalOption(state,"active-defrag-threshold-lower",cserver.active_defrag_threshold_lower,CONFIG_DEFAULT_DEFRAG_THRESHOLD_LOWER);
    rewriteConfigNumericalOption(state,"active-defrag-threshold-upper",cserver.active_defrag_threshold_upper,CONFIG_DEFAULT_DEFRAG_THRESHOLD_UPPER);
    rewriteConfigBytesOption(state,"active-defrag-ignore-bytes",cserver.active_defrag_ignore_bytes,CONFIG_DEFAULT_DEFRAG_IGNORE_BYTES);
    rewriteConfigNumericalOption(state,"active-defrag-cycle-min",cserver.active_defrag_cycle_min,CONFIG_DEFAULT_DEFRAG_CYCLE_MIN);
    rewriteConfigNumericalOption(state,"active-defrag-cycle-max",cserver.active_defrag_cycle_max,CONFIG_DEFAULT_DEFRAG_CYCLE_MAX);
    rewriteConfigNumericalOption(state,"active-defrag-max-scan-fields",cserver.active_defrag_max_scan_fields,CONFIG_DEFAULT_DEFRAG_MAX_SCAN_FIELDS);
    rewriteConfigYesNoOption(state,"appendonly",g_pserver->aof_state != AOF_OFF,0);
    rewriteConfigStringOption(state,"appendfilename",g_pserver->aof_filename,CONFIG_DEFAULT_AOF_FILENAME);
    rewriteConfigEnumOption(state,"appendfsync",g_pserver->aof_fsync,aof_fsync_enum,CONFIG_DEFAULT_AOF_FSYNC);
    rewriteConfigNumericalOption(state,"auto-aof-rewrite-percentage",g_pserver->aof_rewrite_perc,AOF_REWRITE_PERC);
    rewriteConfigBytesOption(state,"auto-aof-rewrite-min-size",g_pserver->aof_rewrite_min_size,AOF_REWRITE_MIN_SIZE);
    rewriteConfigNumericalOption(state,"lua-time-limit",g_pserver->lua_time_limit,LUA_SCRIPT_TIME_LIMIT);
    rewriteConfigYesNoOption(state,"cluster-enabled",g_pserver->cluster_enabled,0);
    rewriteConfigStringOption(state,"cluster-config-file",g_pserver->cluster_configfile,CONFIG_DEFAULT_CLUSTER_CONFIG_FILE);
    rewriteConfigNumericalOption(state,"cluster-node-timeout",g_pserver->cluster_node_timeout,CLUSTER_DEFAULT_NODE_TIMEOUT);
    rewriteConfigNumericalOption(state,"cluster-migration-barrier",g_pserver->cluster_migration_barrier,CLUSTER_DEFAULT_MIGRATION_BARRIER);
    rewriteConfigNumericalOption(state,"cluster-replica-validity-factor",g_pserver->cluster_slave_validity_factor,CLUSTER_DEFAULT_SLAVE_VALIDITY);
    rewriteConfigNumericalOption(state,"slowlog-log-slower-than",g_pserver->slowlog_log_slower_than,CONFIG_DEFAULT_SLOWLOG_LOG_SLOWER_THAN);
    rewriteConfigNumericalOption(state,"latency-monitor-threshold",g_pserver->latency_monitor_threshold,CONFIG_DEFAULT_LATENCY_MONITOR_THRESHOLD);
    rewriteConfigNumericalOption(state,"slowlog-max-len",g_pserver->slowlog_max_len,CONFIG_DEFAULT_SLOWLOG_MAX_LEN);
    rewriteConfigNotifykeyspaceeventsOption(state);
    rewriteConfigNumericalOption(state,"hash-max-ziplist-entries",g_pserver->hash_max_ziplist_entries,OBJ_HASH_MAX_ZIPLIST_ENTRIES);
    rewriteConfigNumericalOption(state,"hash-max-ziplist-value",g_pserver->hash_max_ziplist_value,OBJ_HASH_MAX_ZIPLIST_VALUE);
    rewriteConfigNumericalOption(state,"stream-node-max-bytes",g_pserver->stream_node_max_bytes,OBJ_STREAM_NODE_MAX_BYTES);
    rewriteConfigNumericalOption(state,"stream-node-max-entries",g_pserver->stream_node_max_entries,OBJ_STREAM_NODE_MAX_ENTRIES);
    rewriteConfigNumericalOption(state,"list-max-ziplist-size",g_pserver->list_max_ziplist_size,OBJ_LIST_MAX_ZIPLIST_SIZE);
    rewriteConfigNumericalOption(state,"list-compress-depth",g_pserver->list_compress_depth,OBJ_LIST_COMPRESS_DEPTH);
    rewriteConfigNumericalOption(state,"set-max-intset-entries",g_pserver->set_max_intset_entries,OBJ_SET_MAX_INTSET_ENTRIES);
    rewriteConfigNumericalOption(state,"zset-max-ziplist-entries",g_pserver->zset_max_ziplist_entries,OBJ_ZSET_MAX_ZIPLIST_ENTRIES);
    rewriteConfigNumericalOption(state,"zset-max-ziplist-value",g_pserver->zset_max_ziplist_value,OBJ_ZSET_MAX_ZIPLIST_VALUE);
    rewriteConfigNumericalOption(state,"hll-sparse-max-bytes",g_pserver->hll_sparse_max_bytes,CONFIG_DEFAULT_HLL_SPARSE_MAX_BYTES);
    rewriteConfigYesNoOption(state,"activedefrag",cserver.active_defrag_enabled,CONFIG_DEFAULT_ACTIVE_DEFRAG);
    rewriteConfigClientoutputbufferlimitOption(state);
    rewriteConfigNumericalOption(state,"hz",g_pserver->config_hz,CONFIG_DEFAULT_HZ);
    rewriteConfigEnumOption(state,"supervised",cserver.supervised_mode,supervised_mode_enum,SUPERVISED_NONE);
    rewriteConfigYesNoOption(state,"active-replica",g_pserver->fActiveReplica,CONFIG_DEFAULT_ACTIVE_REPLICA);
    rewriteConfigStringOption(state, "version-override",KEYDB_SET_VERSION,KEYDB_REAL_VERSION);

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
"GET <pattern> -- Return parameters matching the glob-like <pattern> and their values.",
"SET <parameter> <value> -- Set parameter to value.",
"RESETSTAT -- Reset statistics reported by INFO.",
"REWRITE -- Rewrite the configuration file.",
NULL
        };
        addReplyHelp(c, help);
    } else if (!strcasecmp(szFromObj(c->argv[1]),"set") && c->argc == 4) {
        configSetCommand(c);
    } else if (!strcasecmp(szFromObj(c->argv[1]),"get") && c->argc == 3) {
        configGetCommand(c);
    } else if (!strcasecmp(szFromObj(c->argv[1]),"resetstat") && c->argc == 2) {
        resetServerStats();
        resetCommandTableStats();
        addReply(c,shared.ok);
    } else if (!strcasecmp(szFromObj(c->argv[1]),"rewrite") && c->argc == 2) {
        if (cserver.configfile == NULL) {
            addReplyError(c,"The server is running without a config file");
            return;
        }
        if (rewriteConfig(cserver.configfile) == -1) {
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
