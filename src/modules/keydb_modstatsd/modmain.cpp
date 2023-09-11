#include "redismodule.h"
#include <stdlib.h>
#include <cpp-statsd-client/StatsdClient.hpp>
#include <unordered_map>
#include <sys/utsname.h>
#include <algorithm>
#ifdef __linux__
#include <sys/sysinfo.h>
#include <sys/resource.h>
#endif
#include <inttypes.h>
#include <sys/time.h>
#include <regex>
#include <sstream>
#include <iostream>
#include <fstream>

using namespace Statsd;

class StatsdClientWrapper 
{
private:
    StatsdClient *m_stats;
    StatsdClient *m_stats_noprefix;

public:
    StatsdClientWrapper(const std::string& host,
                        const uint16_t port,
                        const std::string& prefix,
                        const uint64_t batchsize,
                        const uint64_t sendInterval) {
        m_stats = new StatsdClient(host, port, prefix, batchsize, sendInterval);
        m_stats_noprefix = new StatsdClient(host, port, "keydb", batchsize, sendInterval);
    }

    ~StatsdClientWrapper() {
        delete m_stats;
        delete m_stats_noprefix;
    }

    void increment(const std::string& key, const bool prefixOnly = true) {
        m_stats->increment(key);
        if (!prefixOnly) m_stats_noprefix->increment(key);
    }

    void decrement(const std::string& key, const bool prefixOnly = true) {
        m_stats->decrement(key);
        if (!prefixOnly) m_stats_noprefix->decrement(key);
    }

    void count(const std::string& key, const int delta, const bool prefixOnly = true) {
        m_stats->count(key, delta);
        if (!prefixOnly) m_stats_noprefix->count(key, delta);
    }

    template <typename T>
    void gauge(const std::string& key, const T value, const bool prefixOnly = true) {
        m_stats->gauge(key, value);
        if (!prefixOnly) m_stats_noprefix->gauge(key, value);
    }

    void timing(const std::string& key, const unsigned int ms, const bool prefixOnly = true) {
        m_stats->timing(key, ms);
        if (!prefixOnly) m_stats_noprefix->timing(key, ms);
    }
};

/* constants */
static time_t c_infoUpdateSeconds = 10;
// the current Redis Cluster setup we configure replication factor as 2, each non-empty master node should have 2 replicas, given that there are 3 zones in each regions
static const int EXPECTED_NUMBER_OF_REPLICAS = 2;

StatsdClientWrapper *g_stats = nullptr;
std::string m_strPrefix { "keydb" };

const std::regex g_replica_or_db_info_regex { "^(slave|db)(\\d+)" };
const char *g_string_counter_separator = "__";
const uint64_t g_stats_buffer_size_bytes = 1600;
std::string nodeName;
int unameResult;

enum class StatsD_Type {
    STATSD_GAUGE_LONGLONG,
    STATSD_GAUGE_FLOAT,
    STATSD_GAUGE_BYTES,
    STATSD_DELTA,
    STATSD_COUNTER_STRING
};

struct StatsRecord {
    StatsD_Type type;
    bool prefixOnly = true;
    const char *szAlternate = nullptr;

    /* Dynamic Values */
    long long prevVal = 0;
};

std::unordered_map<std::string, StatsRecord> g_mapInfoFields = {
    // info
    { "used_memory", { StatsD_Type::STATSD_GAUGE_BYTES, false /* prefixOnly */}},
    { "used_memory_rss", { StatsD_Type::STATSD_GAUGE_BYTES }},
    { "maxmemory", { StatsD_Type::STATSD_GAUGE_BYTES, false /* prefixOnly */}},
    { "used_memory_dataset_perc", { StatsD_Type::STATSD_GAUGE_FLOAT }},
    { "avg_lock_contention", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "repl_backlog_size", { StatsD_Type::STATSD_GAUGE_BYTES }},
    { "connected_slaves", { StatsD_Type::STATSD_GAUGE_LONGLONG, true, "connected_replicas" }},
    { "errorstat_ERR", { StatsD_Type::STATSD_DELTA }},
    { "connected_clients", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "cluster_connections", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "instantaneous_ops_per_sec", { StatsD_Type::STATSD_GAUGE_LONGLONG, false /* prefixOnly */}},
    { "instantaneous_input_kbps", { StatsD_Type::STATSD_GAUGE_FLOAT, false /* prefixOnly */}},
    { "instantaneous_output_kbps", { StatsD_Type::STATSD_GAUGE_FLOAT, false /* prefixOnly */}},
    { "server_threads", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "mvcc_depth", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "sync_full", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "sync_partial_ok", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "sync_partial_err", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "rdb_bgsave_in_progress", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "rdb_last_bgsave_time_sec", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "used_memory_overhead", { StatsD_Type::STATSD_GAUGE_BYTES }},
    { "master_sync_in_progress", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "uptime_in_seconds", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "hz", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "configured_hz", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "maxclients", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "client_recent_max_input_buffer", { StatsD_Type::STATSD_GAUGE_BYTES }},
    { "client_recent_max_output_buffer", { StatsD_Type::STATSD_GAUGE_BYTES }},
    { "blocked_clients", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "tracking_clients", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "clients_in_timeout_table", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "used_memory_peak", { StatsD_Type::STATSD_GAUGE_BYTES }},
    { "used_memory_startup", { StatsD_Type::STATSD_GAUGE_BYTES }},
    { "used_memory_dataset", { StatsD_Type::STATSD_GAUGE_BYTES }},
    { "allocator_allocated", { StatsD_Type::STATSD_GAUGE_BYTES }},
    { "allocator_active", { StatsD_Type::STATSD_GAUGE_BYTES }},
    { "allocator_resident", { StatsD_Type::STATSD_GAUGE_BYTES }},
    { "total_system_memory", { StatsD_Type::STATSD_GAUGE_BYTES }},
    { "used_memory_lua", { StatsD_Type::STATSD_GAUGE_BYTES }},
    { "used_memory_scripts", { StatsD_Type::STATSD_GAUGE_BYTES }},
    { "number_of_cached_scripts", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "allocator_frag_ratio", { StatsD_Type::STATSD_GAUGE_FLOAT }},
    { "allocator_frag_bytes", { StatsD_Type::STATSD_GAUGE_BYTES }},
    { "allocator_rss_ratio", { StatsD_Type::STATSD_GAUGE_FLOAT }},
    { "allocator_rss_bytes", { StatsD_Type::STATSD_GAUGE_BYTES }},
    { "rss_overhead_ratio", { StatsD_Type::STATSD_GAUGE_FLOAT }},
    { "rss_overhead_bytes", { StatsD_Type::STATSD_GAUGE_BYTES }},
    { "mem_fragmentation_ratio", { StatsD_Type::STATSD_GAUGE_FLOAT }},
    { "mem_fragmentation_bytes", { StatsD_Type::STATSD_GAUGE_BYTES }},
    { "mem_not_counted_for_evict", { StatsD_Type::STATSD_GAUGE_BYTES }},
    { "mem_replication_backlog", { StatsD_Type::STATSD_GAUGE_BYTES }},
    { "mem_clients_slaves", { StatsD_Type::STATSD_GAUGE_BYTES }},
    { "mem_clients_normal", { StatsD_Type::STATSD_GAUGE_BYTES }},
    { "mem_aof_buffer", { StatsD_Type::STATSD_GAUGE_BYTES }},
    { "active_defrag_running", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "lazyfree_pending_objects", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "lazyfreed_objects", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "loading", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "current_cow_peak", { StatsD_Type::STATSD_GAUGE_BYTES }},
    { "current_cow_size", { StatsD_Type::STATSD_GAUGE_BYTES }},
    { "current_cow_size_age", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "current_fork_perc", { StatsD_Type::STATSD_GAUGE_FLOAT }},
    { "current_save_keys_processed", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "current_save_keys_total", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "rdb_changes_since_last_save", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "rdb_last_save_time", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "rdb_last_cow_size", { StatsD_Type::STATSD_GAUGE_BYTES }},
    { "aof_enabled", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "aof_rewrite_in_progress", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "aof_rewrite_scheduled", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "aof_last_cow_size", { StatsD_Type::STATSD_GAUGE_BYTES }},
    { "module_fork_in_progress", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "module_fork_last_cow_size", { StatsD_Type::STATSD_GAUGE_BYTES }},
    { "aof_current_size", { StatsD_Type::STATSD_GAUGE_BYTES }},
    { "aof_base_size", { StatsD_Type::STATSD_GAUGE_BYTES }},
    { "aof_pending_rewrite", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "aof_buffer_length", { StatsD_Type::STATSD_GAUGE_BYTES }},
    { "aof_rewrite_buffer_length", { StatsD_Type::STATSD_GAUGE_BYTES }},
    { "aof_pending_bio_fsync", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "aof_delayed_fsync", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "total_connections_received", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "total_commands_processed", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "total_net_input_bytes", { StatsD_Type::STATSD_GAUGE_BYTES }},
    { "total_net_output_bytes", { StatsD_Type::STATSD_GAUGE_BYTES }},
    { "rejected_connections", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "expired_keys", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "expired_stale_perc", { StatsD_Type::STATSD_GAUGE_FLOAT }},
    { "expired_time_cap_reached_count", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "expire_cycle_cpu_milliseconds", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "evicted_keys", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "keyspace_hits", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "keyspace_misses", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "pubsub_channels", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "pubsub_patterns", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "latest_fork_usec", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "total_forks", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "migrate_cached_sockets", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "slave_expires_tracked_keys", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "active_defrag_hits", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "active_defrag_misses", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "active_defrag_key_hits", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "active_defrag_key_misses", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "tracking_total_keys", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "tracking_total_items", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "tracking_total_prefixes", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "unexpected_error_replies", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "total_error_replies", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "dump_payload_sanitizations", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "total_reads_processed", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "total_writes_processed", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "instantaneous_lock_contention", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "avg_lock_contention", { StatsD_Type::STATSD_GAUGE_FLOAT }},
    { "storage_provider_read_hits", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "storage_provider_read_misses", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "repl_backlog_active", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "repl_backlog_size", { StatsD_Type::STATSD_GAUGE_BYTES }},
    { "repl_backlog_first_byte_offset", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "repl_backlog_histlen", { StatsD_Type::STATSD_GAUGE_BYTES }},
    { "used_cpu_sys", { StatsD_Type::STATSD_GAUGE_FLOAT }},
    { "used_cpu_user", { StatsD_Type::STATSD_GAUGE_FLOAT }},
    { "used_cpu_sys_children", { StatsD_Type::STATSD_GAUGE_FLOAT }},
    { "used_cpu_user_children", { StatsD_Type::STATSD_GAUGE_FLOAT }},
    { "used_cpu_user_children", { StatsD_Type::STATSD_GAUGE_FLOAT }},
    { "long_lock_waits", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "used_cpu_sys_main_thread", { StatsD_Type::STATSD_GAUGE_FLOAT }},
    { "used_cpu_user_main_thread", { StatsD_Type::STATSD_GAUGE_FLOAT }},
    { "master_sync_total_bytes", { StatsD_Type::STATSD_GAUGE_BYTES }},
    { "master_sync_read_bytes", { StatsD_Type::STATSD_GAUGE_BYTES }},
    { "master_sync_last_io_seconds_ago", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "master_link_down_since_seconds", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "maxmemory_policy", { StatsD_Type::STATSD_COUNTER_STRING }},
    { "role", { StatsD_Type::STATSD_COUNTER_STRING }},
    { "master_global_link_status", { StatsD_Type::STATSD_COUNTER_STRING }},
    { "master_link_status", { StatsD_Type::STATSD_COUNTER_STRING }},
    { "master_failover_state", { StatsD_Type::STATSD_COUNTER_STRING }},
    { "rdb_last_bgsave_status", { StatsD_Type::STATSD_COUNTER_STRING }},
    { "rdb_saves", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "instantaneous_input_repl_kbps", { StatsD_Type::STATSD_GAUGE_FLOAT }},
    { "instantaneous_output_repl_kbps", { StatsD_Type::STATSD_GAUGE_FLOAT }},
    { "master_host", { StatsD_Type::STATSD_COUNTER_STRING }},
    { "master_repl_offset", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "second_repl_offset", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "slave_repl_offset", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "redis_version", { StatsD_Type::STATSD_COUNTER_STRING }},
    { "redis_git_sha1", { StatsD_Type::STATSD_COUNTER_STRING }},
    // cluster info
    { "cluster_state", { StatsD_Type::STATSD_COUNTER_STRING }},
    { "cluster_slots_assigned", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "cluster_slots_ok", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "cluster_slots_pfail", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "cluster_slots_fail", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "cluster_known_nodes", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "cluster_size", { StatsD_Type::STATSD_GAUGE_LONGLONG }},
    { "storage_flash_available_bytes", { StatsD_Type::STATSD_GAUGE_BYTES }},
    { "storage_flash_total_bytes", { StatsD_Type::STATSD_GAUGE_BYTES }},
};

/* Globals */
static uint64_t g_cclients = 0;

long long ustime(void) {
    struct timeval tv;
    long long ust;

    gettimeofday(&tv, NULL);
    ust = ((long long)tv.tv_sec)*1000000;
    ust += tv.tv_usec;
    return ust;
}

void event_client_change_handler(struct RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent, void *data) {
    if (eid.id != REDISMODULE_EVENT_CLIENT_CHANGE)
        return;

    uint64_t clientsStart = g_cclients;
    switch (subevent) {
        case REDISMODULE_SUBEVENT_CLIENT_CHANGE_CONNECTED:
            ++g_cclients;
            g_stats->increment("clients_added");
            RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_DEBUG, "Emitting metric increment for \"clients_added\"");
            break;

        case REDISMODULE_SUBEVENT_CLIENT_CHANGE_DISCONNECTED:
            --g_cclients;
            g_stats->increment("clients_disconnected");
            RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_DEBUG, "Emitting metric increment for \"clients_disconnected\"");
            break;
    }

    if (g_cclients != clientsStart) {
        g_stats->gauge("clients", g_cclients);
        RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_DEBUG, "Emitting metric \"clients\": %" PRIu64, g_cclients);
    }
}

void handleStatItem(struct RedisModuleCtx *ctx, std::string name, StatsRecord &record, const char *pchValue) {
    switch (record.type) {
        case StatsD_Type::STATSD_GAUGE_LONGLONG: {
            long long val = strtoll(pchValue, nullptr, 10);
            g_stats->gauge(name, val, record.prefixOnly);
            RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_DEBUG, "Emitting metric \"%s\": %lld", name.c_str(), val);
            break;
        }

        case StatsD_Type::STATSD_GAUGE_FLOAT: {
            double val = strtod(pchValue, nullptr);
            g_stats->gauge(name, val, record.prefixOnly);
            RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_DEBUG, "Emitting metric \"%s\": %f", name.c_str(), val);
            break;
        }

        case StatsD_Type::STATSD_GAUGE_BYTES: {
            long long val = strtoll(pchValue, nullptr, 10);
            g_stats->gauge(name + "_MB", val / 1024/1024, record.prefixOnly);
            RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_DEBUG, "Emitting metric \"%s\": %llu", (name + "_MB").c_str(), val / 1024/1024);
            break;
        }

        case StatsD_Type::STATSD_DELTA: {
            long long val = strtoll(pchValue, nullptr, 10);
            g_stats->count(name, val - record.prevVal, record.prefixOnly);
            RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_DEBUG, "Emitting metric count for \"%s\": %lld", name.c_str() , val - record.prevVal);
            record.prevVal = val;
            break;
        }

        case StatsD_Type::STATSD_COUNTER_STRING: {
            // parse val string
            const char *pNewLine = strchr(pchValue, '\r');
            if (pNewLine == nullptr) {
                pNewLine = strchr(pchValue, '\n');
            }
            if (pNewLine == nullptr) {
                g_stats->increment("STATSD_COUNTER_STRING_failed", 1);
                return;
            }
            std::string val(pchValue, pNewLine - pchValue);
            std::replace(val.begin(), val.end(), '.', '-');
            // metrics emit
            std::string metricsName = name + g_string_counter_separator + val;
            g_stats->increment(metricsName, 1);
            RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_DEBUG, "Emitting metric \"%s\"", metricsName.c_str());
            break;
        }

        default:
            RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_WARNING, "Unknown stats record type for the key \"%s\": %u", name.c_str(), (unsigned)record.type);
            break;
    }
}

void handleErrorStatItem(struct RedisModuleCtx *ctx, std::string name, std::string rest) {
    size_t idx = rest.find('=');
    if (idx != std::string::npos) {
        std::string statValue = rest.substr(idx + 1);
        long long val = strtoll(statValue.c_str(), nullptr, 10);
        g_stats->gauge(name, val);
        RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_DEBUG, "Emitting metric \"%s\": %lld", name.c_str(), val);
    } else {
        RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_WARNING, "Unexpected errorstat line format returned by \"INFO\" command: \"%s\"", (name + rest).c_str());
    }
}

void handleReplicaOrDbInfoItem(struct RedisModuleCtx *ctx, std::string name, std::string rest) {
    //use a stringstream to extract each metric of the form <name>=<value>
    std::stringstream metrics(rest);
    while (metrics.good()) {
        std::string metric;
        std::getline(metrics, metric, ',');
        size_t idx = metric.find('=');
        if (idx != std::string::npos) {
            std::string stat = metric.substr(0, idx);
            std::string statName = name + '-' + stat;
            //idx + 1 to ignore the = sign
            std::string statValue = metric.substr(idx + 1);
            // string values
            if (stat == "ip" || stat == "state") {
                std::replace(statValue.begin(), statValue.end(), '.', '-');
                statName += g_string_counter_separator + statValue;
                g_stats->increment(statName, 1);
                RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_DEBUG, "Emitting metric \"%s\"", statName.c_str());
            } else {
                long long val = strtoll(statValue.c_str(), nullptr, 10);
                g_stats->gauge(statName, val);
                RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_DEBUG, "Emitting metric \"%s\": %lld", statName.c_str(), val);
            }
        }
    }
}

void handle_info_response(struct RedisModuleCtx *ctx, const char *szReply, size_t len, const char *command) {
    
    #define SAFETY_CHECK_POINTER(_p) ((_p) < (szReply + len))

    // Parse the INFO reply string line by line
    const char *pchLineStart = szReply;

    while (SAFETY_CHECK_POINTER(pchLineStart) && *pchLineStart != '\0') {
        // Loop Each Line
        const char *pchColon = pchLineStart;
        while (SAFETY_CHECK_POINTER(pchColon) && *pchColon != ':' && *pchColon != '\r') {
            ++pchColon;
        }
        if (!SAFETY_CHECK_POINTER(pchColon)) {
            RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_WARNING, "Unexpected line termination when parsing response from %s command: \"%s\"", command, pchLineStart);
            break; // BUG
        }
        const char *pchLineEnd = pchColon;
        while (SAFETY_CHECK_POINTER(pchLineEnd) && *pchLineEnd != '\n')
            ++pchLineEnd;
        
        std::string strCheck(pchLineStart, pchColon - pchLineStart);
        if (strCheck.find("errorstat_") != std::string::npos) {
            std::string remainder(pchColon + 1, pchLineEnd - (pchColon + 1));
            handleErrorStatItem(ctx, strCheck, remainder);
        } else if (std::regex_match(strCheck, g_replica_or_db_info_regex)) {
            std::string remainder(pchColon + 1, pchLineEnd - (pchColon + 1));
            handleReplicaOrDbInfoItem(ctx, strCheck, remainder);
        } else {
            auto itr = g_mapInfoFields.find(strCheck);
            if (itr != g_mapInfoFields.end()) {
                // This is an info field we care about
                if (itr->second.szAlternate != nullptr)
                    strCheck = itr->second.szAlternate;
                handleStatItem(ctx, strCheck, itr->second, pchColon+1);
            }
        }

        RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_DEBUG, "INFO response line: \"%s\"", std::string(pchLineStart, pchLineEnd - pchLineStart).c_str());
        pchLineStart = pchLineEnd + 1;    // start of next line, if we're over the loop will catch it
    }

    #undef SAFETY_CHECK_POINTER
}

void handle_cluster_nodes_response(struct RedisModuleCtx *ctx, const char *szReply, size_t len) {
    #define SAFETY_CHECK_POINTER(_p) ((_p) < (szReply + len))
    const char *pchLineStart = szReply;
    long long primaries = 0;
    long long replicas = 0;
    while (SAFETY_CHECK_POINTER(pchLineStart) && *pchLineStart != '\0') {
        // Loop Each Line
        const char *pchLineEnd = pchLineStart;
        while (SAFETY_CHECK_POINTER(pchLineEnd) && (*pchLineEnd != '\r') && (*pchLineEnd != '\n')) {
            ++pchLineEnd;
        }
        std::string line(pchLineStart, pchLineEnd - pchLineStart);
        RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_DEBUG, "Cluster Nodes Line: \"%s\"", line.c_str());
        if (std::string::npos != line.find("master")) {
            ++primaries;
        } else if (std::string::npos != line.find("slave")) {
            ++replicas;
        } else {
            RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_WARNING, "Unexpected NODE format returned by \"CLUSTER NODES\" command: \"%s\"", line.c_str());
        }
        // emit myself stat
        if (line.find("myself") != std::string::npos) {
            size_t firstSpaceIdx = line.find(' ');
            // emit cluster node id
            if (firstSpaceIdx != std::string::npos) {
                std::string nodeIdStat = "cluster_node_id";
                nodeIdStat += g_string_counter_separator + line.substr(0, firstSpaceIdx);
                g_stats->increment(nodeIdStat);
            }
            // emit node ip
            size_t firstColonIdx = line.find(':');
            if (firstColonIdx != std::string::npos) {
                std::string nodeIpStat = "cluster_node_ip";
                std::string nodeIP = line.substr(firstSpaceIdx+1, firstColonIdx-firstSpaceIdx-1);
                std::replace(nodeIP.begin(), nodeIP.end(), '.', '-');
                nodeIpStat += g_string_counter_separator + nodeIP;
                g_stats->increment(nodeIpStat);
            }
        }
        pchLineStart = pchLineEnd;
        while (SAFETY_CHECK_POINTER(pchLineStart) && ((*pchLineStart == '\r') || (*pchLineStart == '\n'))) {
            ++pchLineStart;
        }
    }
    g_stats->gauge("primaries", primaries);
    RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_DEBUG, "Emitting metric \"primaries\": %llu", primaries);
    g_stats->gauge("replicas", replicas);
    RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_DEBUG, "Emitting metric \"replicas\": %llu", replicas);
    #undef SAFETY_CHECK_POINTER
}

void handle_client_list_response(struct RedisModuleCtx *ctx, const char *szReply, size_t len) {
    size_t totalClientOutputBuffer = 0;
    size_t totalReplicaClientOutputBuffer = 0;
    #define SAFETY_CHECK_POINTER(_p) ((_p) < (szReply + len))
    const char *pchLineStart = szReply;
    while (SAFETY_CHECK_POINTER(pchLineStart) && *pchLineStart != '\0') {
        // Loop Each Line
        const char *pchLineEnd = pchLineStart;
        while (SAFETY_CHECK_POINTER(pchLineEnd) && (*pchLineEnd != '\r') && (*pchLineEnd != '\n')) {
            ++pchLineEnd;
        }
        std::string line(pchLineStart, pchLineEnd - pchLineStart);
        RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_DEBUG, "Client List Line: \"%s\"", line.c_str());

        // recover output buffer size for client
        bool lineFailed = false;
        bool replica = line.find("flags=S") != std::string::npos;
        size_t idx = line.find("omem");
        if (!(lineFailed = (idx == std::string::npos))) {
            std::string rest = line.substr(idx);
            size_t startIdx = rest.find("=");
            if (!(lineFailed = (startIdx == std::string::npos))) {
                size_t endIdx = rest.find(" ");
                if (!(lineFailed = (endIdx == std::string::npos))) {
                    // use startIdx + 1 and endIdx - 1 to exclude the '=' and ' ' characters
                    std::string valueString = rest.substr(startIdx + 1, (endIdx - 1) - (startIdx + 1));
                    size_t value = strtoll(valueString.c_str(), nullptr, 10);
                    totalClientOutputBuffer += value;
                    if (replica) {
                        totalReplicaClientOutputBuffer += value;
                    }
                }
            }
        }

        if (lineFailed) {
            RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_WARNING, "Unexpected CLIENT format returned by \"CLIENT LIST\" command: \"%s\"", line.c_str());
        }

        pchLineStart = pchLineEnd;
        while (SAFETY_CHECK_POINTER(pchLineStart) && ((*pchLineStart == '\r') || (*pchLineStart == '\n'))) {
            ++pchLineStart;
        }
    }
    #undef SAFETY_CHECK_POINTER
    g_stats->gauge("total_client_output_buffer", totalClientOutputBuffer);
    g_stats->gauge("total_replica_client_output_buffer", totalReplicaClientOutputBuffer);
}

void emit_system_free_memory() {
    std::ifstream meminfo("/proc/meminfo");
    std::string line;
    while (std::getline(meminfo, line)) {
        if (line.find("MemAvailable:") != std::string::npos) {
            unsigned long memAvailableInKB;
            std::sscanf(line.c_str(), "MemAvailable: %lu kB", &memAvailableInKB);
            g_stats->gauge("systemAvailableMemory_MB", memAvailableInKB / 1024);
            return;
        }
    }
}

void emit_metrics_for_insufficient_replicas(struct RedisModuleCtx *ctx, long long keys) {
    // non-empty
    if (keys <= 0) {
        return;
    }
    RedisModuleCallReply *reply = RedisModule_Call(ctx, "ROLE", "");
    if (RedisModule_CallReplyType(reply) != REDISMODULE_REPLY_ARRAY) {
        RedisModule_FreeCallReply(reply);
        return;
    }
    RedisModuleCallReply *roleReply = RedisModule_CallReplyArrayElement(reply, 0);
    if (RedisModule_CallReplyType(roleReply) != REDISMODULE_REPLY_STRING) {
        RedisModule_FreeCallReply(reply);
        return;
    }
    size_t len;
    const char *role = RedisModule_CallReplyStringPtr(roleReply, &len);
    // check if the current node is a primary
    if (strncmp(role, "master", len) == 0) {
        RedisModuleCallReply *replicasReply = RedisModule_CallReplyArrayElement(reply, 2);
        // check if there are less than 2 connected replicas
        if (RedisModule_CallReplyLength(replicasReply) < EXPECTED_NUMBER_OF_REPLICAS) {
            g_stats->increment("lessThanExpectedReplicas_error", 1);
        }
    }
    RedisModule_FreeCallReply(reply);
}

void event_cron_handler(struct RedisModuleCtx *ctx, RedisModuleEvent eid, uint64_t subevent, void *data) {
    static time_t lastTime = 0;
    time_t curTime = time(nullptr);

    if ((curTime - lastTime) > c_infoUpdateSeconds) {
        size_t startTime = ustime();

#ifdef __linux__
	/* Log CPU Usage */
	static long long s_mscpuLast = 0;
	struct rusage self_ru;
	getrusage(RUSAGE_SELF, &self_ru);

	long long mscpuCur = (self_ru.ru_utime.tv_sec * 1000) + (self_ru.ru_utime.tv_usec / 1000)
			+ (self_ru.ru_stime.tv_sec * 1000) + (self_ru.ru_stime.tv_usec / 1000);


	g_stats->gauge("cpu_load_perc", ((double)(mscpuCur - s_mscpuLast) / ((curTime - lastTime)*1000))*100, false /* prefixOnly */);   
	s_mscpuLast = mscpuCur;
#endif

        /* Log clients */
        g_stats->gauge("clients", g_cclients);

        /* node name */
        if (unameResult == 0) {
            g_stats->increment("node_name" + std::string(g_string_counter_separator) + nodeName);
        }

        /* Log INFO Fields */
        size_t commandStartTime = ustime();
        RedisModuleCallReply *reply = RedisModule_Call(ctx, "INFO", "");
        size_t len = 0;
        const char *szReply = RedisModule_CallReplyStringPtr(reply, &len);
        g_stats->timing("info_time_taken_us", ustime() - commandStartTime);
        commandStartTime = ustime();
        handle_info_response(ctx, szReply, len, "INFO");
        g_stats->timing("handle_info_time_taken_us", ustime() - commandStartTime);
        RedisModule_FreeCallReply(reply);

        /* Log CLUSTER INFO Fields */
        commandStartTime = ustime();
        reply = RedisModule_Call(ctx, "CLUSTER", "c", "INFO");
        szReply = RedisModule_CallReplyStringPtr(reply, &len);
        g_stats->timing("cluster_info_time_taken_us", ustime() - commandStartTime);
        commandStartTime = ustime();
        handle_info_response(ctx, szReply, len, "CLUSTER INFO");
        g_stats->timing("handle_cluster_info_time_taken_us", ustime() - commandStartTime);
        RedisModule_FreeCallReply(reply);

        /* Log Cluster Topology */
        commandStartTime = ustime();
        reply = RedisModule_Call(ctx, "CLUSTER", "c", "NODES");
        szReply = RedisModule_CallReplyStringPtr(reply, &len);
        g_stats->timing("cluster_nodes_time_taken_us", ustime() - commandStartTime);
        commandStartTime = ustime();
        handle_cluster_nodes_response(ctx, szReply, len);
        g_stats->timing("handle_cluster_nodes_time_taken_us", ustime() - commandStartTime);
        RedisModule_FreeCallReply(reply);

        /* Log Client Info */
        // commandStartTime = ustime();
        // reply = RedisModule_Call(ctx, "CLIENT", "c", "LIST");
        // szReply = RedisModule_CallReplyStringPtr(reply, &len);
        // g_stats->timing("client_info_time_taken_us", ustime() - commandStartTime);
        // commandStartTime = ustime();
        // handle_client_list_response(ctx, szReply, len);
        // g_stats->timing("handle_client_info_time_taken_us", ustime() - commandStartTime);
        // RedisModule_FreeCallReply(reply);

        commandStartTime = ustime();
        emit_system_free_memory();
        g_stats->timing("emit_free_system_memory_time_taken_us", ustime() - commandStartTime);

        /* Log Keys */
        commandStartTime = ustime();
        reply = RedisModule_Call(ctx, "dbsize", "");
        long long keys = RedisModule_CallReplyInteger(reply);
        RedisModule_FreeCallReply(reply);
        g_stats->gauge("keys", keys, false /* prefixOnly */);
        RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_DEBUG, "Emitting metric \"keys\": %llu", keys);
        g_stats->timing("emit_keys_metric_time_taken_us", ustime() - commandStartTime);

        emit_metrics_for_insufficient_replicas(ctx, keys);

        g_stats->timing("metrics_time_taken_us", ustime() - startTime);
        
	lastTime = curTime;
    }
}

extern "C" int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (RedisModule_Init(ctx,"statsd",1,REDISMODULE_APIVER_1) == REDISMODULE_ERR) 
        return REDISMODULE_ERR;

    RedisModule_AutoMemory(ctx);
    /* Use pod name if available*/
    const char *podName = getenv("POD_NAME");
    utsname sysName;
    unameResult = uname(&sysName);
    if (unameResult == 0) {
        nodeName = std::string(sysName.nodename);
        std::replace(nodeName.begin(), nodeName.end(), '.', '-');
    }
    if (podName != nullptr) {
        m_strPrefix = podName;
        std::replace(m_strPrefix.begin(), m_strPrefix.end(), '.', '-');
    }
    else if (unameResult == 0) {
        m_strPrefix = nodeName;
        unameResult = 1;
    }

    for (int iarg = 0; iarg < argc; ++iarg) {
        size_t len = 0;
        const char *rgchArg = RedisModule_StringPtrLen(argv[iarg], &len);
        if (len == 6 && memcmp(rgchArg, "prefix", 6) == 0) {
            if ((iarg+1) >= argc) {
                RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_WARNING, "Expected a value after 'prefix'");
                return REDISMODULE_ERR;
            }
            ++iarg;
            size_t lenPrefix = 0;
            const char *rgchPrefix = RedisModule_StringPtrLen(argv[iarg], &lenPrefix);
            m_strPrefix = std::string(rgchPrefix, lenPrefix);
        } else {
            RedisModule_Log(ctx, REDISMODULE_LOGLEVEL_WARNING, "Unrecognized configuration flag");
            return REDISMODULE_ERR;
        }
    }

    g_stats = new StatsdClientWrapper("localhost", 8125, m_strPrefix, g_stats_buffer_size_bytes, c_infoUpdateSeconds * 1000);

    if (RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_ClientChange, event_client_change_handler) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_SubscribeToServerEvent(ctx, RedisModuleEvent_CronLoop, event_cron_handler) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
    
    return REDISMODULE_OK;
}

extern "C" int RedisModule_OnUnload(RedisModuleCtx *ctx) {
    delete g_stats;
    return REDISMODULE_OK;
}
