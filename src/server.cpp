/*
 * Copyright (c) 2009-2016, Salvatore Sanfilippo <antirez at gmail dot com>
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
#include "slowlog.h"
#include "bio.h"
#include "latency.h"
#include "atomicvar.h"
#include "storage.h"
#include <thread>
#include <time.h>
#include <signal.h>
#include <sys/wait.h>
#include <errno.h>
#include <assert.h>
#include <ctype.h>
#include <stdarg.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/uio.h>
#include <sys/un.h>
#include <limits.h>
#include <float.h>
#include <math.h>
#include <sys/resource.h>
#include <sys/utsname.h>
#include <locale.h>
#include <sys/socket.h>
#include <algorithm>
#include <uuid/uuid.h>
#include <mutex>
#include "aelocker.h"

int g_fTestMode = false;

/* Our shared "common" objects */

struct sharedObjectsStruct shared;

/* Global vars that are actually used as constants. The following double
 * values are used for double on-disk serialization, and are initialized
 * at runtime to avoid strange compiler optimizations. */

double R_Zero, R_PosInf, R_NegInf, R_Nan;

/*================================= Globals ================================= */

/* Global vars */
namespace GlobalHidden {
struct redisServer server; /* Server global state */
}
redisServer *g_pserver = &GlobalHidden::server;
struct redisServerConst cserver;
thread_local struct redisServerThreadVars *serverTL = NULL;   // thread local server vars
volatile unsigned long lru_clock; /* Server global current LRU time. */

/* Our command table.
 *
 * Every entry is composed of the following fields:
 *
 * name:        A string representing the command name.
 *
 * function:    Pointer to the C function implementing the command.
 *
 * arity:       Number of arguments, it is possible to use -N to say >= N
 *
 * sflags:      Command flags as string. See below for a table of flags.
 *
 * flags:       Flags as bitmask. Computed by Redis using the 'sflags' field.
 *
 * get_keys_proc: An optional function to get key arguments from a command.
 *                This is only used when the following three fields are not
 *                enough to specify what arguments are keys.
 *
 * first_key_index: First argument that is a key
 *
 * last_key_index: Last argument that is a key
 *
 * key_step:    Step to get all the keys from first to last argument.
 *              For instance in MSET the step is two since arguments
 *              are key,val,key,val,...
 *
 * microseconds: Microseconds of total execution time for this command.
 *
 * calls:       Total number of calls of this command.
 *
 * id:          Command bit identifier for ACLs or other goals.
 *
 * The flags, microseconds and calls fields are computed by Redis and should
 * always be set to zero.
 *
 * Command flags are expressed using space separated strings, that are turned
 * into actual flags by the populateCommandTable() function.
 *
 * This is the meaning of the flags:
 *
 * write:       Write command (may modify the key space).
 *
 * read-only:   All the non special commands just reading from keys without
 *              changing the content, or returning other informations like
 *              the TIME command. Special commands such administrative commands
 *              or transaction related commands (multi, exec, discard, ...)
 *              are not flagged as read-only commands, since they affect the
 *              server or the connection in other ways.
 *
 * use-memory:  May increase memory usage once called. Don't allow if out
 *              of memory.
 *
 * admin:       Administrative command, like SAVE or SHUTDOWN.
 *
 * pub-sub:     Pub/Sub related command.
 *
 * no-script:   Command not allowed in scripts.
 *
 * random:      Random command. Command is not deterministic, that is, the same
 *              command with the same arguments, with the same key space, may
 *              have different results. For instance SPOP and RANDOMKEY are
 *              two random commands.
 *
 * to-sort:     Sort command output array if called from script, so that the
 *              output is deterministic. When this flag is used (not always
 *              possible), then the "random" flag is not needed.
 *
 * ok-loading:  Allow the command while loading the database.
 *
 * ok-stale:    Allow the command while a replica has stale data but is not
 *              allowed to serve this data. Normally no command is accepted
 *              in this condition but just a few.
 *
 * no-monitor:  Do not automatically propagate the command on MONITOR.
 *
 * cluster-asking: Perform an implicit ASKING for this command, so the
 *              command will be accepted in cluster mode if the slot is marked
 *              as 'importing'.
 *
 * fast:        Fast command: O(1) or O(log(N)) command that should never
 *              delay its execution as long as the kernel scheduler is giving
 *              us time. Note that commands that may trigger a DEL as a side
 *              effect (like SET) are not fast commands.
 *
 * The following additional flags are only used in order to put commands
 * in a specific ACL category. Commands can have multiple ACL categories.
 *
 * @keyspace, @read, @write, @set, @sortedset, @list, @hash, @string, @bitmap,
 * @hyperloglog, @stream, @admin, @fast, @slow, @pubsub, @blocking, @dangerous,
 * @connection, @transaction, @scripting, @geo.
 *
 * Note that:
 *
 * 1) The read-only flag implies the @read ACL category.
 * 2) The write flag implies the @write ACL category.
 * 3) The fast flag implies the @fast ACL category.
 * 4) The admin flag implies the @admin and @dangerous ACL category.
 * 5) The pub-sub flag implies the @pubsub ACL category.
 * 6) The lack of fast flag implies the @slow ACL category.
 * 7) The non obvious "keyspace" category includes the commands
 *    that interact with keys without having anything to do with
 *    specific data structures, such as: DEL, RENAME, MOVE, SELECT,
 *    TYPE, EXPIRE*, PEXPIRE*, TTL, PTTL, ...
 */

struct redisCommand redisCommandTable[] = {
    {"module",moduleCommand,-2,
     "admin no-script",
     0,NULL,0,0,0,0,0,0},

    {"get",getCommand,2,
     "read-only fast @string",
     0,NULL,1,1,1,0,0,0},

    /* Note that we can't flag set as fast, since it may perform an
     * implicit DEL of a large key. */
    {"set",setCommand,-3,
     "write use-memory @string",
     0,NULL,1,1,1,0,0,0},

    {"setnx",setnxCommand,3,
     "write use-memory fast @string",
     0,NULL,1,1,1,0,0,0},

    {"setex",setexCommand,4,
     "write use-memory @string",
     0,NULL,1,1,1,0,0,0},

    {"psetex",psetexCommand,4,
     "write use-memory @string",
     0,NULL,1,1,1,0,0,0},

    {"append",appendCommand,3,
     "write use-memory fast @string",
     0,NULL,1,1,1,0,0,0},

    {"strlen",strlenCommand,2,
     "read-only fast @string",
     0,NULL,1,1,1,0,0,0},

    {"del",delCommand,-2,
     "write @keyspace",
     0,NULL,1,-1,1,0,0,0},

    {"expdel",delCommand,-2,
     "write @keyspace",
     0,NULL,1,-1,1,0,0,0},

    {"unlink",unlinkCommand,-2,
     "write fast @keyspace",
     0,NULL,1,-1,1,0,0,0},

    {"exists",existsCommand,-2,
     "read-only fast @keyspace",
     0,NULL,1,-1,1,0,0,0},

    {"setbit",setbitCommand,4,
     "write use-memory @bitmap",
     0,NULL,1,1,1,0,0,0},

    {"getbit",getbitCommand,3,
     "read-only fast @bitmap",
     0,NULL,1,1,1,0,0,0},

    {"bitfield",bitfieldCommand,-2,
     "write use-memory @bitmap",
     0,NULL,1,1,1,0,0,0},

    {"setrange",setrangeCommand,4,
     "write use-memory @string",
     0,NULL,1,1,1,0,0,0},

    {"getrange",getrangeCommand,4,
     "read-only @string",
     0,NULL,1,1,1,0,0,0},

    {"substr",getrangeCommand,4,
     "read-only @string",
     0,NULL,1,1,1,0,0,0},

    {"incr",incrCommand,2,
     "write use-memory fast @string",
     0,NULL,1,1,1,0,0,0},

    {"decr",decrCommand,2,
     "write use-memory fast @string",
     0,NULL,1,1,1,0,0,0},

    {"mget",mgetCommand,-2,
     "read-only fast @string",
     0,NULL,1,-1,1,0,0,0},

    {"rpush",rpushCommand,-3,
     "write use-memory fast @list",
     0,NULL,1,1,1,0,0,0},

    {"lpush",lpushCommand,-3,
     "write use-memory fast @list",
     0,NULL,1,1,1,0,0,0},

    {"rpushx",rpushxCommand,-3,
     "write use-memory fast @list",
     0,NULL,1,1,1,0,0,0},

    {"lpushx",lpushxCommand,-3,
     "write use-memory fast @list",
     0,NULL,1,1,1,0,0,0},

    {"linsert",linsertCommand,5,
     "write use-memory @list",
     0,NULL,1,1,1,0,0,0},

    {"rpop",rpopCommand,2,
     "write fast @list",
     0,NULL,1,1,1,0,0,0},

    {"lpop",lpopCommand,2,
     "write fast @list",
     0,NULL,1,1,1,0,0,0},

    {"brpop",brpopCommand,-3,
     "write no-script @list @blocking",
     0,NULL,1,-2,1,0,0,0},

    {"brpoplpush",brpoplpushCommand,4,
     "write use-memory no-script @list @blocking",
     0,NULL,1,2,1,0,0,0},

    {"blpop",blpopCommand,-3,
     "write no-script @list @blocking",
     0,NULL,1,-2,1,0,0,0},

    {"llen",llenCommand,2,
     "read-only fast @list",
     0,NULL,1,1,1,0,0,0},

    {"lindex",lindexCommand,3,
     "read-only @list",
     0,NULL,1,1,1,0,0,0},

    {"lset",lsetCommand,4,
     "write use-memory @list",
     0,NULL,1,1,1,0,0,0},

    {"lrange",lrangeCommand,4,
     "read-only @list",
     0,NULL,1,1,1,0,0,0},

    {"ltrim",ltrimCommand,4,
     "write @list",
     0,NULL,1,1,1,0,0,0},

    {"lrem",lremCommand,4,
     "write @list",
     0,NULL,1,1,1,0,0,0},

    {"rpoplpush",rpoplpushCommand,3,
     "write use-memory @list",
     0,NULL,1,2,1,0,0,0},

    {"sadd",saddCommand,-3,
     "write use-memory fast @set",
     0,NULL,1,1,1,0,0,0},

    {"srem",sremCommand,-3,
     "write fast @set",
     0,NULL,1,1,1,0,0,0},

    {"smove",smoveCommand,4,
     "write fast @set",
     0,NULL,1,2,1,0,0,0},

    {"sismember",sismemberCommand,3,
     "read-only fast @set",
     0,NULL,1,1,1,0,0,0},

    {"scard",scardCommand,2,
     "read-only fast @set",
     0,NULL,1,1,1,0,0,0},

    {"spop",spopCommand,-2,
     "write random fast @set",
     0,NULL,1,1,1,0,0,0},

    {"srandmember",srandmemberCommand,-2,
     "read-only random @set",
     0,NULL,1,1,1,0,0,0},

    {"sinter",sinterCommand,-2,
     "read-only to-sort @set",
     0,NULL,1,-1,1,0,0,0},

    {"sinterstore",sinterstoreCommand,-3,
     "write use-memory @set",
     0,NULL,1,-1,1,0,0,0},

    {"sunion",sunionCommand,-2,
     "read-only to-sort @set",
     0,NULL,1,-1,1,0,0,0},

    {"sunionstore",sunionstoreCommand,-3,
     "write use-memory @set",
     0,NULL,1,-1,1,0,0,0},

    {"sdiff",sdiffCommand,-2,
     "read-only to-sort @set",
     0,NULL,1,-1,1,0,0,0},

    {"sdiffstore",sdiffstoreCommand,-3,
     "write use-memory @set",
     0,NULL,1,-1,1,0,0,0},

    {"smembers",sinterCommand,2,
     "read-only to-sort @set",
     0,NULL,1,1,1,0,0,0},

    {"sscan",sscanCommand,-3,
     "read-only random @set",
     0,NULL,1,1,1,0,0,0},

    {"zadd",zaddCommand,-4,
     "write use-memory fast @sortedset",
     0,NULL,1,1,1,0,0,0},

    {"zincrby",zincrbyCommand,4,
     "write use-memory fast @sortedset",
     0,NULL,1,1,1,0,0,0},

    {"zrem",zremCommand,-3,
     "write fast @sortedset",
     0,NULL,1,1,1,0,0,0},

    {"zremrangebyscore",zremrangebyscoreCommand,4,
     "write @sortedset",
     0,NULL,1,1,1,0,0,0},

    {"zremrangebyrank",zremrangebyrankCommand,4,
     "write @sortedset",
     0,NULL,1,1,1,0,0,0},

    {"zremrangebylex",zremrangebylexCommand,4,
     "write @sortedset",
     0,NULL,1,1,1,0,0,0},

    {"zunionstore",zunionstoreCommand,-4,
     "write use-memory @sortedset",
     0,zunionInterGetKeys,0,0,0,0,0,0},

    {"zinterstore",zinterstoreCommand,-4,
     "write use-memory @sortedset",
     0,zunionInterGetKeys,0,0,0,0,0,0},

    {"zrange",zrangeCommand,-4,
     "read-only @sortedset",
     0,NULL,1,1,1,0,0,0},

    {"zrangebyscore",zrangebyscoreCommand,-4,
     "read-only @sortedset",
     0,NULL,1,1,1,0,0,0},

    {"zrevrangebyscore",zrevrangebyscoreCommand,-4,
     "read-only @sortedset",
     0,NULL,1,1,1,0,0,0},

    {"zrangebylex",zrangebylexCommand,-4,
     "read-only @sortedset",
     0,NULL,1,1,1,0,0,0},

    {"zrevrangebylex",zrevrangebylexCommand,-4,
     "read-only @sortedset",
     0,NULL,1,1,1,0,0,0},

    {"zcount",zcountCommand,4,
     "read-only fast @sortedset",
     0,NULL,1,1,1,0,0,0},

    {"zlexcount",zlexcountCommand,4,
     "read-only fast @sortedset",
     0,NULL,1,1,1,0,0,0},

    {"zrevrange",zrevrangeCommand,-4,
     "read-only @sortedset",
     0,NULL,1,1,1,0,0,0},

    {"zcard",zcardCommand,2,
     "read-only fast @sortedset",
     0,NULL,1,1,1,0,0,0},

    {"zscore",zscoreCommand,3,
     "read-only fast @sortedset",
     0,NULL,1,1,1,0,0,0},

    {"zrank",zrankCommand,3,
     "read-only fast @sortedset",
     0,NULL,1,1,1,0,0,0},

    {"zrevrank",zrevrankCommand,3,
     "read-only fast @sortedset",
     0,NULL,1,1,1,0,0,0},

    {"zscan",zscanCommand,-3,
     "read-only random @sortedset",
     0,NULL,1,1,1,0,0,0},

    {"zpopmin",zpopminCommand,-2,
     "write fast @sortedset",
     0,NULL,1,1,1,0,0,0},

    {"zpopmax",zpopmaxCommand,-2,
     "write fast @sortedset",
     0,NULL,1,1,1,0,0,0},

    {"bzpopmin",bzpopminCommand,-3,
     "write no-script fast @sortedset @blocking",
     0,NULL,1,-2,1,0,0,0},

    {"bzpopmax",bzpopmaxCommand,-3,
     "write no-script fast @sortedset @blocking",
     0,NULL,1,-2,1,0,0,0},

    {"hset",hsetCommand,-4,
     "write use-memory fast @hash",
     0,NULL,1,1,1,0,0,0},

    {"hsetnx",hsetnxCommand,4,
     "write use-memory fast @hash",
     0,NULL,1,1,1,0,0,0},

    {"hget",hgetCommand,3,
     "read-only fast @hash",
     0,NULL,1,1,1,0,0,0},

    {"hmset",hsetCommand,-4,
     "write use-memory fast @hash",
     0,NULL,1,1,1,0,0,0},

    {"hmget",hmgetCommand,-3,
     "read-only fast @hash",
     0,NULL,1,1,1,0,0,0},

    {"hincrby",hincrbyCommand,4,
     "write use-memory fast @hash",
     0,NULL,1,1,1,0,0,0},

    {"hincrbyfloat",hincrbyfloatCommand,4,
     "write use-memory fast @hash",
     0,NULL,1,1,1,0,0,0},

    {"hdel",hdelCommand,-3,
     "write fast @hash",
     0,NULL,1,1,1,0,0,0},

    {"hlen",hlenCommand,2,
     "read-only fast @hash",
     0,NULL,1,1,1,0,0,0},

    {"hstrlen",hstrlenCommand,3,
     "read-only fast @hash",
     0,NULL,1,1,1,0,0,0},

    {"hkeys",hkeysCommand,2,
     "read-only to-sort @hash",
     0,NULL,1,1,1,0,0,0},

    {"hvals",hvalsCommand,2,
     "read-only to-sort @hash",
     0,NULL,1,1,1,0,0,0},

    {"hgetall",hgetallCommand,2,
     "read-only random @hash",
     0,NULL,1,1,1,0,0,0},

    {"hexists",hexistsCommand,3,
     "read-only fast @hash",
     0,NULL,1,1,1,0,0,0},

    {"hscan",hscanCommand,-3,
     "read-only random @hash",
     0,NULL,1,1,1,0,0,0},

    {"incrby",incrbyCommand,3,
     "write use-memory fast @string",
     0,NULL,1,1,1,0,0,0},

    {"decrby",decrbyCommand,3,
     "write use-memory fast @string",
     0,NULL,1,1,1,0,0,0},

    {"incrbyfloat",incrbyfloatCommand,3,
     "write use-memory fast @string",
     0,NULL,1,1,1,0,0,0},

    {"getset",getsetCommand,3,
     "write use-memory fast @string",
     0,NULL,1,1,1,0,0,0},

    {"mset",msetCommand,-3,
     "write use-memory @string",
     0,NULL,1,-1,2,0,0,0},

    {"msetnx",msetnxCommand,-3,
     "write use-memory @string",
     0,NULL,1,-1,2,0,0,0},

    {"randomkey",randomkeyCommand,1,
     "read-only random @keyspace",
     0,NULL,0,0,0,0,0,0},

    {"select",selectCommand,2,
     "ok-loading fast @keyspace",
     0,NULL,0,0,0,0,0,0},

    {"swapdb",swapdbCommand,3,
     "write fast @keyspace @dangerous",
     0,NULL,0,0,0,0,0,0},

    {"move",moveCommand,3,
     "write fast @keyspace",
     0,NULL,1,1,1,0,0,0},

    /* Like for SET, we can't mark rename as a fast command because
     * overwriting the target key may result in an implicit slow DEL. */
    {"rename",renameCommand,3,
     "write @keyspace",
     0,NULL,1,2,1,0,0,0},

    {"renamenx",renamenxCommand,3,
     "write fast @keyspace",
     0,NULL,1,2,1,0,0,0},

    {"expire",expireCommand,3,
     "write fast @keyspace",
     0,NULL,1,1,1,0,0,0},

    {"expireat",expireatCommand,3,
     "write fast @keyspace",
     0,NULL,1,1,1,0,0,0},

    {"expiremember", expireMemberCommand, -4,
     "write fast @keyspace",
     0,NULL,1,1,1,0,0,0},
    
    {"expirememberat", expireMemberAtCommand, 4,
     "write fast @keyspace",
     0,NULL,1,1,1,0,0,0},

    {"pexpire",pexpireCommand,3,
     "write fast @keyspace",
     0,NULL,1,1,1,0,0,0},

    {"pexpireat",pexpireatCommand,3,
     "write fast @keyspace",
     0,NULL,1,1,1,0,0,0},

    {"keys",keysCommand,2,
     "read-only to-sort @keyspace @dangerous",
     0,NULL,0,0,0,0,0,0},

    {"scan",scanCommand,-2,
     "read-only random @keyspace",
     0,NULL,0,0,0,0,0,0},

    {"dbsize",dbsizeCommand,1,
     "read-only fast @keyspace",
     0,NULL,0,0,0,0,0,0},

    {"auth",authCommand,-2,
     "no-script ok-loading ok-stale fast @connection",
     0,NULL,0,0,0,0,0,0},

    /* We don't allow PING during loading since in Redis PING is used as
     * failure detection, and a loading server is considered to be
     * not available. */
    {"ping",pingCommand,-1,
     "ok-stale fast @connection",
     0,NULL,0,0,0,0,0,0},

    {"echo",echoCommand,2,
     "read-only fast @connection",
     0,NULL,0,0,0,0,0,0},

    {"save",saveCommand,1,
     "admin no-script",
     0,NULL,0,0,0,0,0,0},

    {"bgsave",bgsaveCommand,-1,
     "admin no-script",
     0,NULL,0,0,0,0,0,0},

    {"bgrewriteaof",bgrewriteaofCommand,1,
     "admin no-script",
     0,NULL,0,0,0,0,0,0},

    {"shutdown",shutdownCommand,-1,
     "admin no-script ok-loading ok-stale",
     0,NULL,0,0,0,0,0,0},

    {"lastsave",lastsaveCommand,1,
     "read-only random fast @admin @dangerous",
     0,NULL,0,0,0,0,0,0},

    {"type",typeCommand,2,
     "read-only fast @keyspace",
     0,NULL,1,1,1,0,0,0},

    {"multi",multiCommand,1,
     "no-script fast @transaction",
     0,NULL,0,0,0,0,0,0},

    {"exec",execCommand,1,
     "no-script no-monitor @transaction",
     0,NULL,0,0,0,0,0,0},

    {"discard",discardCommand,1,
     "no-script fast @transaction",
     0,NULL,0,0,0,0,0,0},

    {"sync",syncCommand,1,
     "admin no-script",
     0,NULL,0,0,0,0,0,0},

    {"psync",syncCommand,3,
     "admin no-script",
     0,NULL,0,0,0,0,0,0},

    {"replconf",replconfCommand,-1,
     "admin no-script ok-loading ok-stale",
     0,NULL,0,0,0,0,0,0},

    {"flushdb",flushdbCommand,-1,
     "write @keyspace @dangerous",
     0,NULL,0,0,0,0,0,0},

    {"flushall",flushallCommand,-1,
     "write @keyspace @dangerous",
     0,NULL,0,0,0,0,0,0},

    {"sort",sortCommand,-2,
     "write use-memory @list @set @sortedset @dangerous",
     0,sortGetKeys,1,1,1,0,0,0},

    {"info",infoCommand,-1,
     "ok-loading ok-stale random @dangerous",
     0,NULL,0,0,0,0,0,0},

    {"monitor",monitorCommand,1,
     "admin no-script",
     0,NULL,0,0,0,0,0,0},

    {"ttl",ttlCommand,-2,
     "read-only fast random @keyspace",
     0,NULL,1,1,1,0,0,0},

    {"touch",touchCommand,-2,
     "read-only fast @keyspace",
     0,NULL,1,-1,1,0,0,0},

    {"pttl",pttlCommand,-2,
     "read-only fast random @keyspace",
     0,NULL,1,1,1,0,0,0},

    {"persist",persistCommand,-2,
     "write fast @keyspace",
     0,NULL,1,1,1,0,0,0},

    {"slaveof",replicaofCommand,3,
     "admin no-script ok-stale",
     0,NULL,0,0,0,0,0,0},

    {"replicaof",replicaofCommand,3,
     "admin no-script ok-stale",
     0,NULL,0,0,0,0,0,0},

    {"role",roleCommand,1,
     "ok-loading ok-stale no-script fast read-only @dangerous",
     0,NULL,0,0,0,0,0,0},

    {"debug",debugCommand,-2,
     "admin no-script",
     0,NULL,0,0,0,0,0,0},

    {"config",configCommand,-2,
     "admin ok-loading ok-stale no-script",
     0,NULL,0,0,0,0,0,0},

    {"subscribe",subscribeCommand,-2,
     "pub-sub no-script ok-loading ok-stale",
     0,NULL,0,0,0,0,0,0},

    {"unsubscribe",unsubscribeCommand,-1,
     "pub-sub no-script ok-loading ok-stale",
     0,NULL,0,0,0,0,0,0},

    {"psubscribe",psubscribeCommand,-2,
     "pub-sub no-script ok-loading ok-stale",
     0,NULL,0,0,0,0,0,0},

    {"punsubscribe",punsubscribeCommand,-1,
     "pub-sub no-script ok-loading ok-stale",
     0,NULL,0,0,0,0,0,0},

    {"publish",publishCommand,3,
     "pub-sub ok-loading ok-stale fast",
     0,NULL,0,0,0,0,0,0},

    {"pubsub",pubsubCommand,-2,
     "pub-sub ok-loading ok-stale random",
     0,NULL,0,0,0,0,0,0},

    {"watch",watchCommand,-2,
     "no-script fast @transaction",
     0,NULL,1,-1,1,0,0,0},

    {"unwatch",unwatchCommand,1,
     "no-script fast @transaction",
     0,NULL,0,0,0,0,0,0},

    {"cluster",clusterCommand,-2,
     "admin ok-stale random",
     0,NULL,0,0,0,0,0,0},

    {"restore",restoreCommand,-4,
     "write use-memory @keyspace @dangerous",
     0,NULL,1,1,1,0,0,0},

    {"restore-asking",restoreCommand,-4,
    "write use-memory cluster-asking @keyspace @dangerous",
    0,NULL,1,1,1,0,0,0},

    {"migrate",migrateCommand,-6,
     "write random @keyspace @dangerous",
     0,migrateGetKeys,0,0,0,0,0,0},

    {"asking",askingCommand,1,
     "fast @keyspace",
     0,NULL,0,0,0,0,0,0},

    {"readonly",readonlyCommand,1,
     "fast @keyspace",
     0,NULL,0,0,0,0,0,0},

    {"readwrite",readwriteCommand,1,
     "fast @keyspace",
     0,NULL,0,0,0,0,0,0},

    {"dump",dumpCommand,2,
     "read-only random @keyspace",
     0,NULL,1,1,1,0,0,0},

    {"object",objectCommand,-2,
     "read-only random @keyspace",
     0,NULL,2,2,1,0,0,0},

    {"memory",memoryCommand,-2,
     "random read-only",
     0,NULL,0,0,0,0,0,0},

    {"client",clientCommand,-2,
     "admin no-script random @connection",
     0,NULL,0,0,0,0,0,0},

    {"hello",helloCommand,-2,
     "no-script fast @connection",
     0,NULL,0,0,0,0,0,0},

    /* EVAL can modify the dataset, however it is not flagged as a write
     * command since we do the check while running commands from Lua. */
    {"eval",evalCommand,-3,
     "no-script @scripting",
     0,evalGetKeys,0,0,0,0,0,0},

    {"evalsha",evalShaCommand,-3,
     "no-script @scripting",
     0,evalGetKeys,0,0,0,0,0,0},

    {"slowlog",slowlogCommand,-2,
     "admin random",
     0,NULL,0,0,0,0,0,0},

    {"script",scriptCommand,-2,
     "no-script @scripting",
     0,NULL,0,0,0,0,0,0},

    {"time",timeCommand,1,
     "read-only random fast",
     0,NULL,0,0,0,0,0,0},

    {"bitop",bitopCommand,-4,
     "write use-memory @bitmap",
     0,NULL,2,-1,1,0,0,0},

    {"bitcount",bitcountCommand,-2,
     "read-only @bitmap",
     0,NULL,1,1,1,0,0,0},

    {"bitpos",bitposCommand,-3,
     "read-only @bitmap",
     0,NULL,1,1,1,0,0,0},

    {"wait",waitCommand,3,
     "no-script @keyspace",
     0,NULL,0,0,0,0,0,0},

    {"command",commandCommand,-1,
     "ok-loading ok-stale random @connection",
     0,NULL,0,0,0,0,0,0},

    {"geoadd",geoaddCommand,-5,
     "write use-memory @geo",
     0,NULL,1,1,1,0,0,0},

    /* GEORADIUS has store options that may write. */
    {"georadius",georadiusCommand,-6,
     "write @geo",
     0,georadiusGetKeys,1,1,1,0,0,0},

    {"georadius_ro",georadiusroCommand,-6,
     "read-only @geo",
     0,georadiusGetKeys,1,1,1,0,0,0},

    {"georadiusbymember",georadiusbymemberCommand,-5,
     "write @geo",
     0,georadiusGetKeys,1,1,1,0,0,0},

    {"georadiusbymember_ro",georadiusbymemberroCommand,-5,
     "read-only @geo",
     0,georadiusGetKeys,1,1,1,0,0,0},

    {"geohash",geohashCommand,-2,
     "read-only @geo",
     0,NULL,1,1,1,0,0,0},

    {"geopos",geoposCommand,-2,
     "read-only @geo",
     0,NULL,1,1,1,0,0,0},

    {"geodist",geodistCommand,-4,
     "read-only @geo",
     0,NULL,1,1,1,0,0,0},

    {"pfselftest",pfselftestCommand,1,
     "admin @hyperloglog",
      0,NULL,0,0,0,0,0,0},

    {"pfadd",pfaddCommand,-2,
     "write use-memory fast @hyperloglog",
     0,NULL,1,1,1,0,0,0},

    /* Technically speaking PFCOUNT may change the key since it changes the
     * final bytes in the HyperLogLog representation. However in this case
     * we claim that the representation, even if accessible, is an internal
     * affair, and the command is semantically read only. */
    {"pfcount",pfcountCommand,-2,
     "read-only @hyperloglog",
     0,NULL,1,-1,1,0,0,0},

    {"pfmerge",pfmergeCommand,-2,
     "write use-memory @hyperloglog",
     0,NULL,1,-1,1,0,0,0},

    {"pfdebug",pfdebugCommand,-3,
     "admin write",
     0,NULL,0,0,0,0,0,0},

    {"xadd",xaddCommand,-5,
     "write use-memory fast random @stream",
     0,NULL,1,1,1,0,0,0},

    {"xrange",xrangeCommand,-4,
     "read-only @stream",
     0,NULL,1,1,1,0,0,0},

    {"xrevrange",xrevrangeCommand,-4,
     "read-only @stream",
     0,NULL,1,1,1,0,0,0},

    {"xlen",xlenCommand,2,
     "read-only fast @stream",
     0,NULL,1,1,1,0,0,0},

    {"xread",xreadCommand,-4,
     "read-only no-script @stream @blocking",
     0,xreadGetKeys,1,1,1,0,0,0},

    {"xreadgroup",xreadCommand,-7,
     "write no-script @stream @blocking",
     0,xreadGetKeys,1,1,1,0,0,0},

    {"xgroup",xgroupCommand,-2,
     "write use-memory @stream",
     0,NULL,2,2,1,0,0,0},

    {"xsetid",xsetidCommand,3,
     "write use-memory fast @stream",
     0,NULL,1,1,1,0,0,0},

    {"xack",xackCommand,-4,
     "write fast random @stream",
     0,NULL,1,1,1,0,0,0},

    {"xpending",xpendingCommand,-3,
     "read-only random @stream",
     0,NULL,1,1,1,0,0,0},

    {"xclaim",xclaimCommand,-6,
     "write random fast @stream",
     0,NULL,1,1,1,0,0,0},

    {"xinfo",xinfoCommand,-2,
     "read-only random @stream",
     0,NULL,2,2,1,0,0,0},

    {"xdel",xdelCommand,-3,
     "write fast @stream",
     0,NULL,1,1,1,0,0,0},

    {"xtrim",xtrimCommand,-2,
     "write random @stream",
     0,NULL,1,1,1,0,0,0},

    {"post",securityWarningCommand,-1,
     "ok-loading ok-stale read-only",
     0,NULL,0,0,0,0,0,0},

    {"host:",securityWarningCommand,-1,
     "ok-loading ok-stale read-only",
     0,NULL,0,0,0,0,0,0},

    {"latency",latencyCommand,-2,
     "admin no-script ok-loading ok-stale",
     0,NULL,0,0,0,0,0,0},

    {"acl",aclCommand,-2,
     "admin no-script ok-loading ok-stale",
     0,NULL,0,0,0,0,0,0},

    {"rreplay",replicaReplayCommand,-3,
     "read-only fast noprop",
     0,NULL,0,0,0,0,0,0}
};

/*============================ Utility functions ============================ */

/* We use a private localtime implementation which is fork-safe. The logging
 * function of Redis may be called from other threads. */
extern "C" void nolocks_localtime(struct tm *tmp, time_t t, time_t tz, int dst);

/* Low level logging. To use only for very big messages, otherwise
 * serverLog() is to prefer. */
void serverLogRaw(int level, const char *msg) {
    const int syslogLevelMap[] = { LOG_DEBUG, LOG_INFO, LOG_NOTICE, LOG_WARNING };
    const char *c = ".-*#                                                             ";
    FILE *fp;
    char buf[64];
    int rawmode = (level & LL_RAW);
    int log_to_stdout = g_pserver->logfile[0] == '\0';

    level &= 0xff; /* clear flags */
    if (level < cserver.verbosity) return;

    fp = log_to_stdout ? stdout : fopen(g_pserver->logfile,"a");
    if (!fp) return;

    if (rawmode) {
        fprintf(fp,"%s",msg);
    } else {
        int off;
        struct timeval tv;
        int role_char;
        pid_t pid = getpid();

        gettimeofday(&tv,NULL);
        struct tm tm;
        nolocks_localtime(&tm,tv.tv_sec,g_pserver->timezone,g_pserver->daylight_active);
        off = strftime(buf,sizeof(buf),"%d %b %Y %H:%M:%S.",&tm);
        snprintf(buf+off,sizeof(buf)-off,"%03d",(int)tv.tv_usec/1000);
        if (g_pserver->sentinel_mode) {
            role_char = 'X'; /* Sentinel. */
        } else if (pid != cserver.pid) {
            role_char = 'C'; /* RDB / AOF writing child. */
        } else {
            role_char = (listLength(g_pserver->masters) ? 'S':'M'); /* Slave or Master. */
        }
        fprintf(fp,"%d:%c %s %c %s\n",
            (int)getpid(),role_char, buf,c[level],msg);
    }
    fflush(fp);

    if (!log_to_stdout) fclose(fp);
    if (g_pserver->syslog_enabled) syslog(syslogLevelMap[level], "%s", msg);
}

/* Like serverLogRaw() but with printf-alike support. This is the function that
 * is used across the code. The raw version is only used in order to dump
 * the INFO output on crash. */
void serverLog(int level, const char *fmt, ...) {
    va_list ap;
    char msg[LOG_MAX_LEN];

    if ((level&0xff) < cserver.verbosity) return;

    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);

    serverLogRaw(level,msg);
}

static void checkTrialTimeout()
{
    time_t curtime = time(NULL);
    int64_t elapsed = (int64_t)curtime - (int64_t)cserver.stat_starttime;
    int64_t remaining = (cserver.trial_timeout * 60L) - elapsed;
    if (remaining <= 0)
    {
        serverLog(LL_WARNING, "Trial timeout exceeded.  KeyDB will now exit.");
        prepareForShutdown(SHUTDOWN_SAVE);
        exit(0);
    }
    else
    {
        serverLog(LL_WARNING, "Trial timeout in %ld:%02ld minutes", remaining/60, remaining % 60);
    }
}

/* Log a fixed message without printf-alike capabilities, in a way that is
 * safe to call from a signal handler.
 *
 * We actually use this only for signals that are not fatal from the point
 * of view of Redis. Signals that are going to kill the server anyway and
 * where we need printf-alike features are served by serverLog(). */
void serverLogFromHandler(int level, const char *msg) {
    int fd;
    int log_to_stdout = g_pserver->logfile[0] == '\0';
    char buf[64];

    if ((level&0xff) < cserver.verbosity || (log_to_stdout && cserver.daemonize))
        return;
    fd = log_to_stdout ? STDOUT_FILENO :
                         open(g_pserver->logfile, O_APPEND|O_CREAT|O_WRONLY, 0644);
    if (fd == -1) return;
    ll2string(buf,sizeof(buf),getpid());
    if (write(fd,buf,strlen(buf)) == -1) goto err;
    if (write(fd,":signal-handler (",17) == -1) goto err;
    ll2string(buf,sizeof(buf),time(NULL));
    if (write(fd,buf,strlen(buf)) == -1) goto err;
    if (write(fd,") ",2) == -1) goto err;
    if (write(fd,msg,strlen(msg)) == -1) goto err;
    if (write(fd,"\n",1) == -1) goto err;
err:
    if (!log_to_stdout) close(fd);
}

/* Return the UNIX time in microseconds */
long long ustime(void) {
    struct timeval tv;
    long long ust;

    gettimeofday(&tv, NULL);
    ust = ((long long)tv.tv_sec)*1000000;
    ust += tv.tv_usec;
    return ust;
}

/* Return the UNIX time in milliseconds */
mstime_t mstime(void) {
    return ustime()/1000;
}

/* After an RDB dump or AOF rewrite we exit from children using _exit() instead of
 * exit(), because the latter may interact with the same file objects used by
 * the parent process. However if we are testing the coverage normal exit() is
 * used in order to obtain the right coverage information. */
void exitFromChild(int retcode) {
#ifdef COVERAGE_TEST
    exit(retcode);
#else
    _exit(retcode);
#endif
}

/*====================== Hash table type implementation  ==================== */

/* This is a hash table type that uses the SDS dynamic strings library as
 * keys and redis objects as values (objects can hold SDS strings,
 * lists, sets). */

void dictVanillaFree(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);
    zfree(val);
}

void dictListDestructor(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);
    listRelease((list*)val);
}

int dictSdsKeyCompare(void *privdata, const void *key1,
        const void *key2)
{
    int l1,l2;
    DICT_NOTUSED(privdata);

    l1 = sdslen((sds)key1);
    l2 = sdslen((sds)key2);
    if (l1 != l2) return 0;
    return memcmp(key1, key2, l1) == 0;
}

void dictSdsNOPDestructor(void *, void *) {}

void dictDbKeyDestructor(void *privdata, void *key)
{
    DICT_NOTUSED(privdata);
    sdsfree((sds)key);
}

/* A case insensitive version used for the command lookup table and other
 * places where case insensitive non binary-safe comparison is needed. */
int dictSdsKeyCaseCompare(void *privdata, const void *key1,
        const void *key2)
{
    DICT_NOTUSED(privdata);

    return strcasecmp((const char*)key1, (const char*)key2) == 0;
}

void dictObjectDestructor(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);

    if (val == NULL) return; /* Lazy freeing will set value to NULL. */
    decrRefCount((robj*)val);
}

void dictSdsDestructor(void *privdata, void *val)
{
    DICT_NOTUSED(privdata);

    sdsfree((sds)val);
}

int dictObjKeyCompare(void *privdata, const void *key1,
        const void *key2)
{
    const robj *o1 = (const robj*)key1, *o2 = (const robj*)key2;
    return dictSdsKeyCompare(privdata,ptrFromObj(o1),ptrFromObj(o2));
}

uint64_t dictObjHash(const void *key) {
    const robj *o = (const robj*)key;
    void *ptr = ptrFromObj(o);
    return dictGenHashFunction(ptr, sdslen((sds)ptr));
}

uint64_t dictSdsHash(const void *key) {
    return dictGenHashFunction((unsigned char*)key, sdslen((char*)key));
}

uint64_t dictSdsCaseHash(const void *key) {
    return dictGenCaseHashFunction((unsigned char*)key, sdslen((char*)key));
}

int dictEncObjKeyCompare(void *privdata, const void *key1,
        const void *key2)
{
    robj *o1 = (robj*) key1, *o2 = (robj*) key2;
    int cmp;

    if (o1->encoding == OBJ_ENCODING_INT &&
        o2->encoding == OBJ_ENCODING_INT)
            return ptrFromObj(o1) == ptrFromObj(o2);

    o1 = getDecodedObject(o1);
    o2 = getDecodedObject(o2);
    cmp = dictSdsKeyCompare(privdata,ptrFromObj(o1),ptrFromObj(o2));
    decrRefCount(o1);
    decrRefCount(o2);
    return cmp;
}

uint64_t dictEncObjHash(const void *key) {
    robj *o = (robj*) key;

    if (sdsEncodedObject(o)) {
        return dictGenHashFunction(ptrFromObj(o), sdslen((sds)ptrFromObj(o)));
    } else {
        if (o->encoding == OBJ_ENCODING_INT) {
            char buf[32];
            int len;

            len = ll2string(buf,32,(long)ptrFromObj(o));
            return dictGenHashFunction((unsigned char*)buf, len);
        } else {
            uint64_t hash;

            o = getDecodedObject(o);
            hash = dictGenHashFunction(ptrFromObj(o), sdslen((sds)ptrFromObj(o)));
            decrRefCount(o);
            return hash;
        }
    }
}

/* Generic hash table type where keys are Redis Objects, Values
 * dummy pointers. */
dictType objectKeyPointerValueDictType = {
    dictEncObjHash,            /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictEncObjKeyCompare,      /* key compare */
    dictObjectDestructor,      /* key destructor */
    NULL                       /* val destructor */
};

/* Like objectKeyPointerValueDictType(), but values can be destroyed, if
 * not NULL, calling zfree(). */
dictType objectKeyHeapPointerValueDictType = {
    dictEncObjHash,            /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictEncObjKeyCompare,      /* key compare */
    dictObjectDestructor,      /* key destructor */
    dictVanillaFree            /* val destructor */
};

/* Set dictionary type. Keys are SDS strings, values are ot used. */
dictType setDictType = {
    dictSdsHash,               /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictSdsKeyCompare,         /* key compare */
    dictSdsDestructor,         /* key destructor */
    NULL                       /* val destructor */
};

/* Sorted sets hash (note: a skiplist is used in addition to the hash table) */
dictType zsetDictType = {
    dictSdsHash,               /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictSdsKeyCompare,         /* key compare */
    NULL,                      /* Note: SDS string shared & freed by skiplist */
    NULL                       /* val destructor */
};

/* db->pdict, keys are sds strings, vals are Redis objects. */
dictType dbDictType = {
    dictSdsHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCompare,          /* key compare */
    dictDbKeyDestructor,          /* key destructor */
    dictObjectDestructor   /* val destructor */
};

dictType dbSnapshotDictType = {
    dictSdsHash,
    NULL,
    NULL,
    dictSdsKeyCompare,
    dictSdsNOPDestructor,
    dictObjectDestructor,
};

/* g_pserver->lua_scripts sha (as sds string) -> scripts (as robj) cache. */
dictType shaScriptObjectDictType = {
    dictSdsCaseHash,            /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCaseCompare,      /* key compare */
    dictSdsDestructor,          /* key destructor */
    dictObjectDestructor        /* val destructor */
};

/* Db->expires */
dictType keyptrDictType = {
    dictSdsHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCompare,          /* key compare */
    NULL,                       /* key destructor */
    NULL                        /* val destructor */
};

/* Command table. sds string -> command struct pointer. */
dictType commandTableDictType = {
    dictSdsCaseHash,            /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCaseCompare,      /* key compare */
    dictSdsDestructor,          /* key destructor */
    NULL                        /* val destructor */
};

/* Hash type hash table (note that small hashes are represented with ziplists) */
dictType hashDictType = {
    dictSdsHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCompare,          /* key compare */
    dictSdsDestructor,          /* key destructor */
    dictSdsDestructor           /* val destructor */
};

/* Keylist hash table type has unencoded redis objects as keys and
 * lists as values. It's used for blocking operations (BLPOP) and to
 * map swapped keys to a list of clients waiting for this keys to be loaded. */
dictType keylistDictType = {
    dictObjHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictObjKeyCompare,          /* key compare */
    dictObjectDestructor,       /* key destructor */
    dictListDestructor          /* val destructor */
};

/* Cluster nodes hash table, mapping nodes addresses 1.2.3.4:6379 to
 * clusterNode structures. */
dictType clusterNodesDictType = {
    dictSdsHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCompare,          /* key compare */
    dictSdsDestructor,          /* key destructor */
    NULL                        /* val destructor */
};

/* Cluster re-addition blacklist. This maps node IDs to the time
 * we can re-add this node. The goal is to avoid readding a removed
 * node for some time. */
dictType clusterNodesBlackListDictType = {
    dictSdsCaseHash,            /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCaseCompare,      /* key compare */
    dictSdsDestructor,          /* key destructor */
    NULL                        /* val destructor */
};

/* Cluster re-addition blacklist. This maps node IDs to the time
 * we can re-add this node. The goal is to avoid readding a removed
 * node for some time. */
dictType modulesDictType = {
    dictSdsCaseHash,            /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCaseCompare,      /* key compare */
    dictSdsDestructor,          /* key destructor */
    NULL                        /* val destructor */
};

/* Migrate cache dict type. */
dictType migrateCacheDictType = {
    dictSdsHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCompare,          /* key compare */
    dictSdsDestructor,          /* key destructor */
    NULL                        /* val destructor */
};

/* Replication cached script dict (g_pserver->repl_scriptcache_dict).
 * Keys are sds SHA1 strings, while values are not used at all in the current
 * implementation. */
dictType replScriptCacheDictType = {
    dictSdsCaseHash,            /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCaseCompare,      /* key compare */
    dictSdsDestructor,          /* key destructor */
    NULL                        /* val destructor */
};

int htNeedsResize(dict *dict) {
    long long size, used;

    size = dictSlots(dict);
    used = dictSize(dict);
    return (size > DICT_HT_INITIAL_SIZE &&
            (used*100/size < HASHTABLE_MIN_FILL));
}

/* If the percentage of used slots in the HT reaches HASHTABLE_MIN_FILL
 * we resize the hash table to save memory */
void tryResizeHashTables(int dbid) {
    g_pserver->db[dbid]->tryResize();
}

/* Our hash table implementation performs rehashing incrementally while
 * we write/read from the hash table. Still if the server is idle, the hash
 * table will use two tables for a long time. So we try to use 1 millisecond
 * of CPU time at every call of this function to perform some rehahsing.
 *
 * The function returns 1 if some rehashing was performed, otherwise 0
 * is returned. */
int redisDbPersistentData::incrementallyRehash() {
    /* Keys dictionary */
    if (dictIsRehashing(m_pdict)) {
        dictRehashMilliseconds(m_pdict,1);
        return 1; /* already used our millisecond for this loop... */
    }
    return 0;
}

/* This function is called once a background process of some kind terminates,
 * as we want to avoid resizing the hash tables when there is a child in order
 * to play well with copy-on-write (otherwise when a resize happens lots of
 * memory pages are copied). The goal of this function is to update the ability
 * for dict.c to resize the hash tables accordingly to the fact we have o not
 * running childs. */
void updateDictResizePolicy(void) {
    if (!g_pserver->FRdbSaveInProgress() && g_pserver->aof_child_pid == -1)
        dictEnableResize();
    else
        dictDisableResize();
}

/* ======================= Cron: called every 100 ms ======================== */

/* Add a sample to the operations per second array of samples. */
void trackInstantaneousMetric(int metric, long long current_reading) {
    long long t = mstime() - g_pserver->inst_metric[metric].last_sample_time;
    long long ops = current_reading -
                    g_pserver->inst_metric[metric].last_sample_count;
    long long ops_sec;

    ops_sec = t > 0 ? (ops*1000/t) : 0;

    g_pserver->inst_metric[metric].samples[g_pserver->inst_metric[metric].idx] =
        ops_sec;
    g_pserver->inst_metric[metric].idx++;
    g_pserver->inst_metric[metric].idx %= STATS_METRIC_SAMPLES;
    g_pserver->inst_metric[metric].last_sample_time = mstime();
    g_pserver->inst_metric[metric].last_sample_count = current_reading;
}

/* Return the mean of all the samples. */
long long getInstantaneousMetric(int metric) {
    int j;
    long long sum = 0;

    for (j = 0; j < STATS_METRIC_SAMPLES; j++)
        sum += g_pserver->inst_metric[metric].samples[j];
    return sum / STATS_METRIC_SAMPLES;
}

/* Check for timeouts. Returns non-zero if the client was terminated.
 * The function gets the current time in milliseconds as argument since
 * it gets called multiple times in a loop, so calling gettimeofday() for
 * each iteration would be costly without any actual gain. */
int clientsCronHandleTimeout(client *c, mstime_t now_ms) {
    time_t now = now_ms/1000;

    if (cserver.maxidletime &&
        !(c->flags & CLIENT_SLAVE) &&    /* no timeout for slaves */
        !(c->flags & CLIENT_MASTER) &&   /* no timeout for masters */
        !(c->flags & CLIENT_BLOCKED) &&  /* no timeout for BLPOP */
        !(c->flags & CLIENT_PUBSUB) &&   /* no timeout for Pub/Sub clients */
        (now - c->lastinteraction > cserver.maxidletime))
    {
        serverLog(LL_VERBOSE,"Closing idle client");
        freeClient(c);
        return 1;
    } else if (c->flags & CLIENT_BLOCKED) {
        /* Blocked OPS timeout is handled with milliseconds resolution.
         * However note that the actual resolution is limited by
         * g_pserver->hz. */

        if (c->bpop.timeout != 0 && c->bpop.timeout < now_ms) {
            /* Handle blocking operation specific timeout. */
            replyToBlockedClientTimedOut(c);
            unblockClient(c);
        } else if (g_pserver->cluster_enabled) {
            /* Cluster: handle unblock & redirect of clients blocked
             * into keys no longer served by this g_pserver-> */
            if (clusterRedirectBlockedClientIfNeeded(c))
                unblockClient(c);
        }
    }
    return 0;
}

/* The client query buffer is an sds.c string that can end with a lot of
 * free space not used, this function reclaims space if needed.
 *
 * The function always returns 0 as it never terminates the client. */
int clientsCronResizeQueryBuffer(client *c) {
    AssertCorrectThread(c);
    size_t querybuf_size = sdsAllocSize(c->querybuf);
    time_t idletime = g_pserver->unixtime - c->lastinteraction;

    /* There are two conditions to resize the query buffer:
     * 1) Query buffer is > BIG_ARG and too big for latest peak.
     * 2) Query buffer is > BIG_ARG and client is idle. */
    if (querybuf_size > PROTO_MBULK_BIG_ARG &&
         ((querybuf_size/(c->querybuf_peak+1)) > 2 ||
          idletime > 2))
    {
        /* Only resize the query buffer if it is actually wasting
         * at least a few kbytes. */
        if (sdsavail(c->querybuf) > 1024*4) {
            c->querybuf = sdsRemoveFreeSpace(c->querybuf);
        }
    }
    /* Reset the peak again to capture the peak memory usage in the next
     * cycle. */
    c->querybuf_peak = 0;

    /* Clients representing masters also use a "pending query buffer" that
     * is the yet not applied part of the stream we are reading. Such buffer
     * also needs resizing from time to time, otherwise after a very large
     * transfer (a huge value or a big MIGRATE operation) it will keep using
     * a lot of memory. */
    if (c->flags & CLIENT_MASTER) {
        /* There are two conditions to resize the pending query buffer:
         * 1) Pending Query buffer is > LIMIT_PENDING_QUERYBUF.
         * 2) Used length is smaller than pending_querybuf_size/2 */
        size_t pending_querybuf_size = sdsAllocSize(c->pending_querybuf);
        if(pending_querybuf_size > LIMIT_PENDING_QUERYBUF &&
           sdslen(c->pending_querybuf) < (pending_querybuf_size/2))
        {
            c->pending_querybuf = sdsRemoveFreeSpace(c->pending_querybuf);
        }
    }
    return 0;
}

/* This function is used in order to track clients using the biggest amount
 * of memory in the latest few seconds. This way we can provide such information
 * in the INFO output (clients section), without having to do an O(N) scan for
 * all the clients.
 *
 * This is how it works. We have an array of CLIENTS_PEAK_MEM_USAGE_SLOTS slots
 * where we track, for each, the biggest client output and input buffers we
 * saw in that slot. Every slot correspond to one of the latest seconds, since
 * the array is indexed by doing UNIXTIME % CLIENTS_PEAK_MEM_USAGE_SLOTS.
 *
 * When we want to know what was recently the peak memory usage, we just scan
 * such few slots searching for the maximum value. */
#define CLIENTS_PEAK_MEM_USAGE_SLOTS 8
size_t ClientsPeakMemInput[CLIENTS_PEAK_MEM_USAGE_SLOTS];
size_t ClientsPeakMemOutput[CLIENTS_PEAK_MEM_USAGE_SLOTS];

int clientsCronTrackExpansiveClients(client *c) {
    size_t in_usage = sdsAllocSize(c->querybuf);
    size_t out_usage = getClientOutputBufferMemoryUsage(c);
    int i = g_pserver->unixtime % CLIENTS_PEAK_MEM_USAGE_SLOTS;
    int zeroidx = (i+1) % CLIENTS_PEAK_MEM_USAGE_SLOTS;

    /* Always zero the next sample, so that when we switch to that second, we'll
     * only register samples that are greater in that second without considering
     * the history of such slot.
     *
     * Note: our index may jump to any random position if serverCron() is not
     * called for some reason with the normal frequency, for instance because
     * some slow command is called taking multiple seconds to execute. In that
     * case our array may end containing data which is potentially older
     * than CLIENTS_PEAK_MEM_USAGE_SLOTS seconds: however this is not a problem
     * since here we want just to track if "recently" there were very expansive
     * clients from the POV of memory usage. */
    ClientsPeakMemInput[zeroidx] = 0;
    ClientsPeakMemOutput[zeroidx] = 0;

    /* Track the biggest values observed so far in this slot. */
    if (in_usage > ClientsPeakMemInput[i]) ClientsPeakMemInput[i] = in_usage;
    if (out_usage > ClientsPeakMemOutput[i]) ClientsPeakMemOutput[i] = out_usage;

    return 0; /* This function never terminates the client. */
}

/* Return the max samples in the memory usage of clients tracked by
 * the function clientsCronTrackExpansiveClients(). */
void getExpansiveClientsInfo(size_t *in_usage, size_t *out_usage) {
    size_t i = 0, o = 0;
    for (int j = 0; j < CLIENTS_PEAK_MEM_USAGE_SLOTS; j++) {
        if (ClientsPeakMemInput[j] > i) i = ClientsPeakMemInput[j];
        if (ClientsPeakMemOutput[j] > o) o = ClientsPeakMemOutput[j];
    }
    *in_usage = i;
    *out_usage = o;
}

/* This function is called by serverCron() and is used in order to perform
 * operations on clients that are important to perform constantly. For instance
 * we use this function in order to disconnect clients after a timeout, including
 * clients blocked in some blocking command with a non-zero timeout.
 *
 * The function makes some effort to process all the clients every second, even
 * if this cannot be strictly guaranteed, since serverCron() may be called with
 * an actual frequency lower than g_pserver->hz in case of latency events like slow
 * commands.
 *
 * It is very important for this function, and the functions it calls, to be
 * very fast: sometimes Redis has tens of hundreds of connected clients, and the
 * default g_pserver->hz value is 10, so sometimes here we need to process thousands
 * of clients per second, turning this function into a source of latency.
 */
#define CLIENTS_CRON_MIN_ITERATIONS 5
void clientsCron(int iel) {
    /* Try to process at least numclients/g_pserver->hz of clients
     * per call. Since normally (if there are no big latency events) this
     * function is called g_pserver->hz times per second, in the average case we
     * process all the clients in 1 second. */
    int numclients = listLength(g_pserver->clients);
    int iterations = numclients/g_pserver->hz;
    mstime_t now = mstime();

    /* Process at least a few clients while we are at it, even if we need
     * to process less than CLIENTS_CRON_MIN_ITERATIONS to meet our contract
     * of processing each client once per second. */
    if (iterations < CLIENTS_CRON_MIN_ITERATIONS)
        iterations = (numclients < CLIENTS_CRON_MIN_ITERATIONS) ?
                     numclients : CLIENTS_CRON_MIN_ITERATIONS;

    while(listLength(g_pserver->clients) && iterations--) {
        client *c;
        listNode *head;

        /* Rotate the list, take the current head, process.
         * This way if the client must be removed from the list it's the
         * first element and we don't incur into O(N) computation. */
        listRotate(g_pserver->clients);
        head = (listNode*)listFirst(g_pserver->clients);
        c = (client*)listNodeValue(head);
        if (c->iel == iel)
        {
            fastlock_lock(&c->lock);
            /* The following functions do different service checks on the client.
            * The protocol is that they return non-zero if the client was
            * terminated. */
            if (clientsCronHandleTimeout(c,now)) goto LContinue;
            if (clientsCronResizeQueryBuffer(c)) goto LContinue;
            if (clientsCronTrackExpansiveClients(c)) goto LContinue;
        LContinue:
            fastlock_unlock(&c->lock);
        }        
    }

    /* Free any pending clients */
    freeClientsInAsyncFreeQueue(iel);
}

/* This function handles 'background' operations we are required to do
 * incrementally in Redis databases, such as active key expiring, resizing,
 * rehashing. */
void databasesCron(void) {
    /* Expire keys by random sampling. Not required for slaves
     * as master will synthesize DELs for us. */
    if (g_pserver->active_expire_enabled && (listLength(g_pserver->masters) == 0 || g_pserver->fActiveReplica)) {
        activeExpireCycle(ACTIVE_EXPIRE_CYCLE_SLOW);
    } else if (listLength(g_pserver->masters) && !g_pserver->fActiveReplica) {
        expireSlaveKeys();
    }

    /* Defrag keys gradually. */
    if (cserver.active_defrag_enabled)
        activeDefragCycle();

    /* Perform hash tables rehashing if needed, but only if there are no
     * other processes saving the DB on disk. Otherwise rehashing is bad
     * as will cause a lot of copy-on-write of memory pages. */
    if (!g_pserver->FRdbSaveInProgress() && g_pserver->aof_child_pid == -1) {
        /* We use global counters so if we stop the computation at a given
         * DB we'll be able to start from the successive in the next
         * cron loop iteration. */
        static unsigned int resize_db = 0;
        static unsigned int rehash_db = 0;
        int dbs_per_call = CRON_DBS_PER_CALL;
        int j;

        /* Don't test more DBs than we have. */
        if (dbs_per_call > cserver.dbnum) dbs_per_call = cserver.dbnum;

        /* Resize */
        for (j = 0; j < dbs_per_call; j++) {
            tryResizeHashTables(resize_db % cserver.dbnum);
            resize_db++;
        }

        /* Rehash */
        if (g_pserver->activerehashing) {
            for (j = 0; j < dbs_per_call; j++) {
                int work_done = g_pserver->db[rehash_db]->incrementallyRehash();
                if (work_done) {
                    /* If the function did some work, stop here, we'll do
                     * more at the next cron loop. */
                    break;
                } else {
                    /* If this db didn't need rehash, we'll try the next one. */
                    rehash_db++;
                    rehash_db %= cserver.dbnum;
                }
            }
        }
    }
}

/* We take a cached value of the unix time in the global state because with
 * virtual memory and aging there is to store the current time in objects at
 * every object access, and accuracy is not needed. To access a global var is
 * a lot faster than calling time(NULL) */
void updateCachedTime(void) {
    g_pserver->unixtime = time(NULL);
    long long ms = mstime();
    __atomic_store(&g_pserver->mstime, &ms, __ATOMIC_RELAXED);

    /* To get information about daylight saving time, we need to call
     * localtime_r and cache the result. However calling localtime_r in this
     * context is safe since we will never fork() while here, in the main
     * thread. The logging function will call a thread safe version of
     * localtime that has no locks. */
    struct tm tm;
    time_t ut = g_pserver->unixtime;
    localtime_r(&ut,&tm);
    g_pserver->daylight_active = tm.tm_isdst;
}

/* This is our timer interrupt, called g_pserver->hz times per second.
 * Here is where we do a number of things that need to be done asynchronously.
 * For instance:
 *
 * - Active expired keys collection (it is also performed in a lazy way on
 *   lookup).
 * - Software watchdog.
 * - Update some statistic.
 * - Incremental rehashing of the DBs hash tables.
 * - Triggering BGSAVE / AOF rewrite, and handling of terminated children.
 * - Clients timeout of different kinds.
 * - Replication reconnection.
 * - Many more...
 *
 * Everything directly called here will be called g_pserver->hz times per second,
 * so in order to throttle execution of things we want to do less frequently
 * a macro is used: run_with_period(milliseconds) { .... }
 */

int serverCron(struct aeEventLoop *eventLoop, long long id, void *clientData) {
    int j;
    UNUSED(eventLoop);
    UNUSED(id);
    UNUSED(clientData);

    /* If another threads unblocked one of our clients, and this thread has been idle
        then beforeSleep won't have a chance to process the unblocking.  So we also
        process them here in the cron job to ensure they don't starve.
    */
    if (listLength(g_pserver->rgthreadvar[IDX_EVENT_LOOP_MAIN].unblocked_clients))
    {
        processUnblockedClients(IDX_EVENT_LOOP_MAIN);
    }

    ProcessPendingAsyncWrites();    // This is really a bug, but for now catch any laggards that didn't clean up
        
    /* Software watchdog: deliver the SIGALRM that will reach the signal
     * handler if we don't return here fast enough. */
    if (g_pserver->watchdog_period) watchdogScheduleSignal(g_pserver->watchdog_period);

    /* Update the time cache. */
    updateCachedTime();

    /* Unpause clients if enough time has elapsed */
    unpauseClientsIfNecessary();

    g_pserver->hz = g_pserver->config_hz;
    /* Adapt the g_pserver->hz value to the number of configured clients. If we have
     * many clients, we want to call serverCron() with an higher frequency. */
    if (g_pserver->dynamic_hz) {
        while (listLength(g_pserver->clients) / g_pserver->hz >
               MAX_CLIENTS_PER_CLOCK_TICK)
        {
            g_pserver->hz += g_pserver->hz; // *= 2
            if (g_pserver->hz > CONFIG_MAX_HZ) {
                g_pserver->hz = CONFIG_MAX_HZ;
                break;
            }
        }
    }

    run_with_period(100) {
        trackInstantaneousMetric(STATS_METRIC_COMMAND,g_pserver->stat_numcommands);
        trackInstantaneousMetric(STATS_METRIC_NET_INPUT,
                g_pserver->stat_net_input_bytes);
        trackInstantaneousMetric(STATS_METRIC_NET_OUTPUT,
                g_pserver->stat_net_output_bytes);
    }

    /* We have just LRU_BITS bits per object for LRU information.
     * So we use an (eventually wrapping) LRU clock.
     *
     * Note that even if the counter wraps it's not a big problem,
     * everything will still work but some object will appear younger
     * to Redis. However for this to happen a given object should never be
     * touched for all the time needed to the counter to wrap, which is
     * not likely.
     *
     * Note that you can change the resolution altering the
     * LRU_CLOCK_RESOLUTION define. */
    g_pserver->lruclock = getLRUClock();

    /* Record the max memory used since the server was started. */
    if (zmalloc_used_memory() > g_pserver->stat_peak_memory)
        g_pserver->stat_peak_memory = zmalloc_used_memory();

    run_with_period(100) {
        /* Sample the RSS and other metrics here since this is a relatively slow call.
         * We must sample the zmalloc_used at the same time we take the rss, otherwise
         * the frag ratio calculate may be off (ratio of two samples at different times) */
        g_pserver->cron_malloc_stats.process_rss = zmalloc_get_rss();
        g_pserver->cron_malloc_stats.zmalloc_used = zmalloc_used_memory();
        /* Sampling the allcator info can be slow too.
         * The fragmentation ratio it'll show is potentically more accurate
         * it excludes other RSS pages such as: shared libraries, LUA and other non-zmalloc
         * allocations, and allocator reserved pages that can be pursed (all not actual frag) */
        zmalloc_get_allocator_info(&g_pserver->cron_malloc_stats.allocator_allocated,
                                   &g_pserver->cron_malloc_stats.allocator_active,
                                   &g_pserver->cron_malloc_stats.allocator_resident);
        /* in case the allocator isn't providing these stats, fake them so that
         * fragmention info still shows some (inaccurate metrics) */
        if (!g_pserver->cron_malloc_stats.allocator_resident) {
            /* LUA memory isn't part of zmalloc_used, but it is part of the process RSS,
             * so we must desuct it in order to be able to calculate correct
             * "allocator fragmentation" ratio */
            size_t lua_memory = lua_gc(g_pserver->lua,LUA_GCCOUNT,0)*1024LL;
            g_pserver->cron_malloc_stats.allocator_resident = g_pserver->cron_malloc_stats.process_rss - lua_memory;
        }
        if (!g_pserver->cron_malloc_stats.allocator_active)
            g_pserver->cron_malloc_stats.allocator_active = g_pserver->cron_malloc_stats.allocator_resident;
        if (!g_pserver->cron_malloc_stats.allocator_allocated)
            g_pserver->cron_malloc_stats.allocator_allocated = g_pserver->cron_malloc_stats.zmalloc_used;
    }

    /* We received a SIGTERM, shutting down here in a safe way, as it is
     * not ok doing so inside the signal handler. */
    if (g_pserver->shutdown_asap) {
        if (prepareForShutdown(SHUTDOWN_NOFLAGS) == C_OK) exit(0);
        serverLog(LL_WARNING,"SIGTERM received but errors trying to shut down the server, check the logs for more information");
        g_pserver->shutdown_asap = 0;
    }

    /* Show some info about non-empty databases */
    run_with_period(5000) {
        for (j = 0; j < cserver.dbnum; j++) {
            long long size, used, vkeys;

            size = g_pserver->db[j]->slots();
            used = g_pserver->db[j]->size();
            vkeys = g_pserver->db[j]->expireSize();
            if (used || vkeys) {
                serverLog(LL_VERBOSE,"DB %d: %lld keys (%lld volatile) in %lld slots HT.",j,used,vkeys,size);
                /* dictPrintStats(g_pserver->dict); */
            }
        }
    }

    /* Show information about connected clients */
    if (!g_pserver->sentinel_mode) {
        run_with_period(5000) {
            serverLog(LL_VERBOSE,
                "%lu clients connected (%lu replicas), %zu bytes in use",
                listLength(g_pserver->clients)-listLength(g_pserver->slaves),
                listLength(g_pserver->slaves),
                zmalloc_used_memory());
        }
    }

    /* We need to do a few operations on clients asynchronously. */
    clientsCron(IDX_EVENT_LOOP_MAIN);

    /* Handle background operations on Redis databases. */
    databasesCron();

    /* Start a scheduled AOF rewrite if this was requested by the user while
     * a BGSAVE was in progress. */
    if (!g_pserver->FRdbSaveInProgress() && g_pserver->aof_child_pid == -1 &&
        g_pserver->aof_rewrite_scheduled)
    {
        rewriteAppendOnlyFileBackground();
    }

    /* Check if a background saving or AOF rewrite in progress terminated. */
    if (g_pserver->FRdbSaveInProgress() || g_pserver->aof_child_pid != -1 ||
        ldbPendingChildren())
    {
        int statloc;
        pid_t pid;

        if (g_pserver->FRdbSaveInProgress())
        {
            void *rval = nullptr;
            if (pthread_tryjoin_np(g_pserver->rdbThreadVars.rdb_child_thread, &rval))
            {
                if (errno != EBUSY && errno != EAGAIN)
                    serverLog(LL_WARNING, "Error joining the background RDB save thread: %s\n", strerror(errno));
            }
            else
            {
                int exitcode = (int)reinterpret_cast<ptrdiff_t>(rval);
                backgroundSaveDoneHandler(exitcode,0);
                if (exitcode == 0) receiveChildInfo();
                updateDictResizePolicy();
                closeChildInfoPipe();
            }
        }
        else if ((pid = wait3(&statloc,WNOHANG,NULL)) != 0) {
            int exitcode = WEXITSTATUS(statloc);
            int bysignal = 0;

            if (WIFSIGNALED(statloc)) bysignal = WTERMSIG(statloc);

            if (pid == -1) {
                serverLog(LL_WARNING,"wait3() returned an error: %s. "
                    "aof_child_pid = %d",
                    strerror(errno),
                    (int) g_pserver->aof_child_pid);
            } else if (pid == g_pserver->aof_child_pid) {
                backgroundRewriteDoneHandler(exitcode,bysignal);
                if (!bysignal && exitcode == 0) receiveChildInfo();
            } else {
                if (!ldbRemoveChild(pid)) {
                    serverLog(LL_WARNING,
                        "Warning, detected child with unmatched pid: %ld",
                        (long)pid);
                }
            }
            updateDictResizePolicy();
            closeChildInfoPipe();
        }
    } else {
        /* If there is not a background saving/rewrite in progress check if
         * we have to save/rewrite now. */
        for (j = 0; j < g_pserver->saveparamslen; j++) {
            struct saveparam *sp = g_pserver->saveparams+j;

            /* Save if we reached the given amount of changes,
             * the given amount of seconds, and if the latest bgsave was
             * successful or if, in case of an error, at least
             * CONFIG_BGSAVE_RETRY_DELAY seconds already elapsed. */
            if (g_pserver->dirty >= sp->changes &&
                g_pserver->unixtime-g_pserver->lastsave > sp->seconds &&
                (g_pserver->unixtime-g_pserver->lastbgsave_try >
                 CONFIG_BGSAVE_RETRY_DELAY ||
                 g_pserver->lastbgsave_status == C_OK))
            {
                serverLog(LL_NOTICE,"%d changes in %d seconds. Saving...",
                    sp->changes, (int)sp->seconds);
                rdbSaveInfo rsi, *rsiptr;
                rsiptr = rdbPopulateSaveInfo(&rsi);
                rdbSaveBackground(rsiptr);
                break;
            }
        }

        /* Trigger an AOF rewrite if needed. */
        if (g_pserver->aof_state == AOF_ON &&
            !g_pserver->FRdbSaveInProgress() &&
            g_pserver->aof_child_pid == -1 &&
            g_pserver->aof_rewrite_perc &&
            g_pserver->aof_current_size > g_pserver->aof_rewrite_min_size)
        {
            long long base = g_pserver->aof_rewrite_base_size ?
                g_pserver->aof_rewrite_base_size : 1;
            long long growth = (g_pserver->aof_current_size*100/base) - 100;
            if (growth >= g_pserver->aof_rewrite_perc) {
                serverLog(LL_NOTICE,"Starting automatic rewriting of AOF on %lld%% growth",growth);
                rewriteAppendOnlyFileBackground();
            }
        }
    }


    /* AOF postponed flush: Try at every cron cycle if the slow fsync
     * completed. */
    if (g_pserver->aof_flush_postponed_start) flushAppendOnlyFile(0);

    /* AOF write errors: in this case we have a buffer to flush as well and
     * clear the AOF error in case of success to make the DB writable again,
     * however to try every second is enough in case of 'hz' is set to
     * an higher frequency. */
    run_with_period(1000) {
        if (g_pserver->aof_last_write_status == C_ERR)
            flushAppendOnlyFile(0);
    }

    /* Replication cron function -- used to reconnect to master,
     * detect transfer failures, start background RDB transfers and so forth. */
    run_with_period(1000) replicationCron();

    /* Run the Redis Cluster cron. */
    run_with_period(100) {
        if (g_pserver->cluster_enabled) clusterCron();
    }

    /* Run the Sentinel timer if we are in sentinel mode. */
    if (g_pserver->sentinel_mode) sentinelTimer();

    /* Cleanup expired MIGRATE cached sockets. */
    run_with_period(1000) {
        migrateCloseTimedoutSockets();
    }

    run_with_period(15000) {
        checkTrialTimeout();
    }

    /* Start a scheduled BGSAVE if the corresponding flag is set. This is
     * useful when we are forced to postpone a BGSAVE because an AOF
     * rewrite is in progress.
     *
     * Note: this code must be after the replicationCron() call above so
     * make sure when refactoring this file to keep this order. This is useful
     * because we want to give priority to RDB savings for replication. */
    if (!g_pserver->FRdbSaveInProgress() && g_pserver->aof_child_pid == -1 &&
        g_pserver->rdb_bgsave_scheduled &&
        (g_pserver->unixtime-g_pserver->lastbgsave_try > CONFIG_BGSAVE_RETRY_DELAY ||
         g_pserver->lastbgsave_status == C_OK))
    {
        rdbSaveInfo rsi, *rsiptr;
        rsiptr = rdbPopulateSaveInfo(&rsi);
        if (rdbSaveBackground(rsiptr) == C_OK)
            g_pserver->rdb_bgsave_scheduled = 0;
    }

    g_pserver->asyncworkqueue->AddWorkFunction([]{
        g_pserver->db[0]->consolidate_snapshot();
    }, true /*HiPri*/);

    g_pserver->cronloops++;
    return 1000/g_pserver->hz;
}

// serverCron for worker threads other than the main thread
int serverCronLite(struct aeEventLoop *eventLoop, long long id, void *clientData)
{
    UNUSED(id);
    UNUSED(clientData);

    int iel = ielFromEventLoop(eventLoop);
    serverAssert(iel != IDX_EVENT_LOOP_MAIN);

    /* If another threads unblocked one of our clients, and this thread has been idle
        then beforeSleep won't have a chance to process the unblocking.  So we also
        process them here in the cron job to ensure they don't starve.
    */
    if (listLength(g_pserver->rgthreadvar[iel].unblocked_clients))
    {
        processUnblockedClients(iel);
    }

    /* Unpause clients if enough time has elapsed */
    unpauseClientsIfNecessary();
    
    ProcessPendingAsyncWrites();    // A bug but leave for now, events should clean up after themselves
    clientsCron(iel);

    return 1000/g_pserver->hz;
}

/* This function gets called every time Redis is entering the
 * main loop of the event driven library, that is, before to sleep
 * for ready file descriptors. */
void beforeSleep(struct aeEventLoop *eventLoop) {
    int iel = ielFromEventLoop(eventLoop);

    /* Call the Redis Cluster before sleep function. Note that this function
     * may change the state of Redis Cluster (from ok to fail or vice versa),
     * so it's a good idea to call it before serving the unblocked clients
     * later in this function. */
    if (g_pserver->cluster_enabled) clusterBeforeSleep();

    /* Run a fast expire cycle (the called function will return
     * ASAP if a fast cycle is not needed). */
    if (g_pserver->active_expire_enabled && (listLength(g_pserver->masters) == 0 || g_pserver->fActiveReplica))
        activeExpireCycle(ACTIVE_EXPIRE_CYCLE_FAST);

    /* Send all the slaves an ACK request if at least one client blocked
     * during the previous event loop iteration. */
    if (g_pserver->get_ack_from_slaves) {
        robj *argv[3];

        argv[0] = createStringObject("REPLCONF",8);
        argv[1] = createStringObject("GETACK",6);
        argv[2] = createStringObject("*",1); /* Not used argument. */
        replicationFeedSlaves(g_pserver->slaves, g_pserver->replicaseldb, argv, 3);
        decrRefCount(argv[0]);
        decrRefCount(argv[1]);
        decrRefCount(argv[2]);
        g_pserver->get_ack_from_slaves = 0;
    }

    /* Unblock all the clients blocked for synchronous replication
     * in WAIT. */
    if (listLength(g_pserver->clients_waiting_acks))
        processClientsWaitingReplicas();

    /* Check if there are clients unblocked by modules that implement
     * blocking commands. */
    moduleHandleBlockedClients(ielFromEventLoop(eventLoop));

    /* Try to process pending commands for clients that were just unblocked. */
    if (listLength(g_pserver->rgthreadvar[iel].unblocked_clients))
    {
        processUnblockedClients(iel);
    }

    /* Write the AOF buffer on disk */
    flushAppendOnlyFile(0);

    static thread_local bool fFirstRun = true;
    // note: we also copy the DB pointer in case a DB swap is done while the lock is released
    std::vector<std::pair<redisDb*, redisDbPersistentData::changelist>> vecchanges;
    if (!fFirstRun) {
        for (int idb = 0; idb < cserver.dbnum; ++idb)
        {
            auto vec = g_pserver->db[idb]->processChanges();
            vecchanges.emplace_back(g_pserver->db[idb], std::move(vec));
        }
    }
    else {
        fFirstRun = false;
    }

    aeReleaseLock();

    for (auto &pair : vecchanges)
        pair.first->commitChanges(pair.second);

    /* Handle writes with pending output buffers. */
    handleClientsWithPendingWrites(iel);
    if (serverTL->gcEpoch != 0)
        g_pserver->garbageCollector.endEpoch(serverTL->gcEpoch, true /*fNoFree*/);
    serverTL->gcEpoch = 0;
    aeAcquireLock();

    /* Close clients that need to be closed asynchronous */
    freeClientsInAsyncFreeQueue(iel);

    /* Before we are going to sleep, let the threads access the dataset by
     * releasing the GIL. Redis main thread will not touch anything at this
     * time. */
    if (moduleCount()) moduleReleaseGIL(TRUE /*fServerThread*/);
}

void beforeSleepLite(struct aeEventLoop *eventLoop)
{
    int iel = ielFromEventLoop(eventLoop);
    
    /* Try to process pending commands for clients that were just unblocked. */
    aeAcquireLock();
    if (listLength(g_pserver->rgthreadvar[iel].unblocked_clients)) {
        processUnblockedClients(iel);
    }

    /* Check if there are clients unblocked by modules that implement
     * blocking commands. */
    moduleHandleBlockedClients(ielFromEventLoop(eventLoop));

    /* Write the AOF buffer on disk */
    flushAppendOnlyFile(0);

    static thread_local bool fFirstRun = true;
    if (!fFirstRun) {
        for (int idb = 0; idb < cserver.dbnum; ++idb)
            g_pserver->db[idb]->processChanges();
    }
    else {
        fFirstRun = false;
    }

    aeReleaseLock();

    /* Handle writes with pending output buffers. */
    handleClientsWithPendingWrites(iel);

    aeAcquireLock();
    /* Close clients that need to be closed asynchronous */
    freeClientsInAsyncFreeQueue(iel);
    aeReleaseLock();

    if (serverTL->gcEpoch != 0)
        g_pserver->garbageCollector.endEpoch(serverTL->gcEpoch, true /*fNoFree*/);
    serverTL->gcEpoch = 0;

    /* Before we are going to sleep, let the threads access the dataset by
     * releasing the GIL. Redis main thread will not touch anything at this
     * time. */
    if (moduleCount()) moduleReleaseGIL(TRUE /*fServerThread*/);
}

/* This function is called immadiately after the event loop multiplexing
 * API returned, and the control is going to soon return to Redis by invoking
 * the different events callbacks. */
void afterSleep(struct aeEventLoop *eventLoop) {
    UNUSED(eventLoop);
    if (moduleCount()) moduleAcquireGIL(TRUE /*fServerThread*/);

    serverAssert(serverTL->gcEpoch == 0);
    serverTL->gcEpoch = g_pserver->garbageCollector.startEpoch();
    aeAcquireLock();
    for (int idb = 0; idb < cserver.dbnum; ++idb)
        g_pserver->db[idb]->trackChanges();
    aeReleaseLock();
}

/* =========================== Server initialization ======================== */

void createSharedObjects(void) {
    int j;

    shared.crlf = makeObjectShared(createObject(OBJ_STRING,sdsnew("\r\n")));
    shared.ok = makeObjectShared(createObject(OBJ_STRING,sdsnew("+OK\r\n")));
    shared.err = makeObjectShared(createObject(OBJ_STRING,sdsnew("-ERR\r\n")));
    shared.emptybulk = makeObjectShared(createObject(OBJ_STRING,sdsnew("$0\r\n\r\n")));
    shared.emptymultibulk = makeObjectShared(createObject(OBJ_STRING,sdsnew("*0\r\n")));
    shared.nullbulk = makeObjectShared(createObject(OBJ_STRING,sdsnew("$0\r\n\r\n")));
    shared.czero = makeObjectShared(createObject(OBJ_STRING,sdsnew(":0\r\n")));
    shared.cone = makeObjectShared(createObject(OBJ_STRING,sdsnew(":1\r\n")));
    shared.emptyarray = makeObjectShared(createObject(OBJ_STRING,sdsnew("*0\r\n")));
    shared.pong = makeObjectShared(createObject(OBJ_STRING,sdsnew("+PONG\r\n")));
    shared.queued = makeObjectShared(createObject(OBJ_STRING,sdsnew("+QUEUED\r\n")));
    shared.emptyscan = makeObjectShared(createObject(OBJ_STRING,sdsnew("*2\r\n$1\r\n0\r\n*0\r\n")));
    shared.wrongtypeerr = makeObjectShared(createObject(OBJ_STRING,sdsnew(
        "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")));
    shared.nokeyerr = makeObjectShared(createObject(OBJ_STRING,sdsnew(
        "-ERR no such key\r\n")));
    shared.syntaxerr = makeObjectShared(createObject(OBJ_STRING,sdsnew(
        "-ERR syntax error\r\n")));
    shared.sameobjecterr = makeObjectShared(createObject(OBJ_STRING,sdsnew(
        "-ERR source and destination objects are the same\r\n")));
    shared.outofrangeerr = makeObjectShared(createObject(OBJ_STRING,sdsnew(
        "-ERR index out of range\r\n")));
    shared.noscripterr = makeObjectShared(createObject(OBJ_STRING,sdsnew(
        "-NOSCRIPT No matching script. Please use EVAL.\r\n")));
    shared.loadingerr = makeObjectShared(createObject(OBJ_STRING,sdsnew(
        "-LOADING Redis is loading the dataset in memory\r\n")));
    shared.slowscripterr = makeObjectShared(createObject(OBJ_STRING,sdsnew(
        "-BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.\r\n")));
    shared.masterdownerr = makeObjectShared(createObject(OBJ_STRING,sdsnew(
        "-MASTERDOWN Link with MASTER is down and replica-serve-stale-data is set to 'no'.\r\n")));
    shared.bgsaveerr = makeObjectShared(createObject(OBJ_STRING,sdsnew(
        "-MISCONF Redis is configured to save RDB snapshots, but it is currently not able to persist on disk. Commands that may modify the data set are disabled, because this instance is configured to report errors during writes if RDB snapshotting fails (stop-writes-on-bgsave-error option). Please check the Redis logs for details about the RDB error.\r\n")));
    shared.roslaveerr = makeObjectShared(createObject(OBJ_STRING,sdsnew(
        "-READONLY You can't write against a read only replica.\r\n")));
    shared.noautherr = makeObjectShared(createObject(OBJ_STRING,sdsnew(
        "-NOAUTH Authentication required.\r\n")));
    shared.oomerr = makeObjectShared(createObject(OBJ_STRING,sdsnew(
        "-OOM command not allowed when used memory > 'maxmemory'.\r\n")));
    shared.execaborterr = makeObjectShared(createObject(OBJ_STRING,sdsnew(
        "-EXECABORT Transaction discarded because of previous errors.\r\n")));
    shared.noreplicaserr = makeObjectShared(createObject(OBJ_STRING,sdsnew(
        "-NOREPLICAS Not enough good replicas to write.\r\n")));
    shared.busykeyerr = makeObjectShared(createObject(OBJ_STRING,sdsnew(
        "-BUSYKEY Target key name already exists.\r\n")));
    shared.space = makeObjectShared(createObject(OBJ_STRING,sdsnew(" ")));
    shared.colon = makeObjectShared(createObject(OBJ_STRING,sdsnew(":")));
    shared.plus = makeObjectShared(createObject(OBJ_STRING,sdsnew("+")));

    /* The shared NULL depends on the protocol version. */
    shared.null[0] = NULL;
    shared.null[1] = NULL;
    shared.null[2] = makeObjectShared(createObject(OBJ_STRING,sdsnew("$-1\r\n")));
    shared.null[3] = makeObjectShared(createObject(OBJ_STRING,sdsnew("_\r\n")));

    shared.nullarray[0] = NULL;
    shared.nullarray[1] = NULL;
    shared.nullarray[2] = makeObjectShared(createObject(OBJ_STRING,sdsnew("*-1\r\n")));
    shared.nullarray[3] = makeObjectShared(createObject(OBJ_STRING,sdsnew("_\r\n")));

    for (j = 0; j < PROTO_SHARED_SELECT_CMDS; j++) {
        char dictid_str[64];
        int dictid_len;

        dictid_len = ll2string(dictid_str,sizeof(dictid_str),j);
        shared.select[j] = makeObjectShared(createObject(OBJ_STRING,
            sdscatprintf(sdsempty(),
                "*2\r\n$6\r\nSELECT\r\n$%d\r\n%s\r\n",
                dictid_len, dictid_str)));
    }
    shared.messagebulk = makeObjectShared(createStringObject("$7\r\nmessage\r\n",13));
    shared.pmessagebulk = makeObjectShared(createStringObject("$8\r\npmessage\r\n",14));
    shared.subscribebulk = makeObjectShared(createStringObject("$9\r\nsubscribe\r\n",15));
    shared.unsubscribebulk = makeObjectShared(createStringObject("$11\r\nunsubscribe\r\n",18));
    shared.psubscribebulk = makeObjectShared(createStringObject("$10\r\npsubscribe\r\n",17));
    shared.punsubscribebulk = makeObjectShared(createStringObject("$12\r\npunsubscribe\r\n",19));
    shared.del = makeObjectShared(createStringObject("DEL",3));
    shared.unlink = makeObjectShared(createStringObject("UNLINK",6));
    shared.rpop = makeObjectShared(createStringObject("RPOP",4));
    shared.lpop = makeObjectShared(createStringObject("LPOP",4));
    shared.lpush = makeObjectShared(createStringObject("LPUSH",5));
    shared.rpoplpush = makeObjectShared(createStringObject("RPOPLPUSH",9));
    shared.zpopmin = makeObjectShared(createStringObject("ZPOPMIN",7));
    shared.zpopmax = makeObjectShared(createStringObject("ZPOPMAX",7));
    for (j = 0; j < OBJ_SHARED_INTEGERS; j++) {
        shared.integers[j] =
            makeObjectShared(createObject(OBJ_STRING,(void*)(long)j));
        shared.integers[j]->encoding = OBJ_ENCODING_INT;
    }
    for (j = 0; j < OBJ_SHARED_BULKHDR_LEN; j++) {
        shared.mbulkhdr[j] = makeObjectShared(createObject(OBJ_STRING,
            sdscatprintf(sdsempty(),"*%d\r\n",j)));
        shared.bulkhdr[j] = makeObjectShared(createObject(OBJ_STRING,
            sdscatprintf(sdsempty(),"$%d\r\n",j)));
    }
    /* The following two shared objects, minstring and maxstrings, are not
     * actually used for their value but as a special object meaning
     * respectively the minimum possible string and the maximum possible
     * string in string comparisons for the ZRANGEBYLEX command. */
    shared.minstring = sdsnew("minstring");
    shared.maxstring = sdsnew("maxstring");
}

void initMasterInfo(redisMaster *master)
{
    if (cserver.default_masterauth)
        master->masterauth = zstrdup(cserver.default_masterauth);
    else
        master->masterauth = NULL;

    if (cserver.default_masteruser)
        master->masteruser = zstrdup(cserver.default_masteruser);
    else
        master->masteruser = NULL;

    master->masterport = 6379;
    master->master = NULL;
    master->cached_master = NULL;
    master->master_initial_offset = -1;
    

    master->repl_state = REPL_STATE_NONE;
    master->repl_down_since = 0; /* Never connected, repl is down since EVER. */
    master->mvccLastSync = 0;
}

void initServerConfig(void) {
    int j;

    updateCachedTime();
    getRandomHexChars(g_pserver->runid,CONFIG_RUN_ID_SIZE);
    g_pserver->runid[CONFIG_RUN_ID_SIZE] = '\0';
    changeReplicationId();
    clearReplicationId2();
    g_pserver->clients = listCreate();
    g_pserver->slaves = listCreate();
    g_pserver->monitors = listCreate();
    g_pserver->timezone = getTimeZone(); /* Initialized by tzset(). */
    cserver.configfile = NULL;
    cserver.executable = NULL;
    g_pserver->hz = g_pserver->config_hz = CONFIG_DEFAULT_HZ;
    g_pserver->dynamic_hz = CONFIG_DEFAULT_DYNAMIC_HZ;
    g_pserver->port = CONFIG_DEFAULT_SERVER_PORT;
    g_pserver->tcp_backlog = CONFIG_DEFAULT_TCP_BACKLOG;
    g_pserver->bindaddr_count = 0;
    g_pserver->unixsocket = NULL;
    g_pserver->unixsocketperm = CONFIG_DEFAULT_UNIX_SOCKET_PERM;
    g_pserver->sofd = -1;
    g_pserver->protected_mode = CONFIG_DEFAULT_PROTECTED_MODE;
    cserver.dbnum = CONFIG_DEFAULT_DBNUM;
    cserver.verbosity = CONFIG_DEFAULT_VERBOSITY;
    cserver.maxidletime = CONFIG_DEFAULT_CLIENT_TIMEOUT;
    cserver.tcpkeepalive = CONFIG_DEFAULT_TCP_KEEPALIVE;
    g_pserver->active_expire_enabled = 1;
    cserver.active_defrag_enabled = CONFIG_DEFAULT_ACTIVE_DEFRAG;
    cserver.active_defrag_ignore_bytes = CONFIG_DEFAULT_DEFRAG_IGNORE_BYTES;
    cserver.active_defrag_threshold_lower = CONFIG_DEFAULT_DEFRAG_THRESHOLD_LOWER;
    cserver.active_defrag_threshold_upper = CONFIG_DEFAULT_DEFRAG_THRESHOLD_UPPER;
    cserver.active_defrag_cycle_min = CONFIG_DEFAULT_DEFRAG_CYCLE_MIN;
    cserver.active_defrag_cycle_max = CONFIG_DEFAULT_DEFRAG_CYCLE_MAX;
    cserver.active_defrag_max_scan_fields = CONFIG_DEFAULT_DEFRAG_MAX_SCAN_FIELDS;
    g_pserver->proto_max_bulk_len = CONFIG_DEFAULT_PROTO_MAX_BULK_LEN;
    cserver.client_max_querybuf_len = PROTO_MAX_QUERYBUF_LEN;
    g_pserver->saveparams = NULL;
    g_pserver->loading = 0;
    g_pserver->logfile = zstrdup(CONFIG_DEFAULT_LOGFILE);
    g_pserver->syslog_enabled = CONFIG_DEFAULT_SYSLOG_ENABLED;
    g_pserver->syslog_ident = zstrdup(CONFIG_DEFAULT_SYSLOG_IDENT);
    g_pserver->syslog_facility = LOG_LOCAL0;
    cserver.daemonize = CONFIG_DEFAULT_DAEMONIZE;
    cserver.supervised = 0;
    cserver.supervised_mode = SUPERVISED_NONE;
    g_pserver->aof_state = AOF_OFF;
    g_pserver->aof_fsync = CONFIG_DEFAULT_AOF_FSYNC;
    g_pserver->aof_no_fsync_on_rewrite = CONFIG_DEFAULT_AOF_NO_FSYNC_ON_REWRITE;
    g_pserver->aof_rewrite_perc = AOF_REWRITE_PERC;
    g_pserver->aof_rewrite_min_size = AOF_REWRITE_MIN_SIZE;
    g_pserver->aof_rewrite_base_size = 0;
    g_pserver->aof_rewrite_scheduled = 0;
    g_pserver->aof_last_fsync = time(NULL);
    g_pserver->aof_rewrite_time_last = -1;
    g_pserver->aof_rewrite_time_start = -1;
    g_pserver->aof_lastbgrewrite_status = C_OK;
    g_pserver->aof_delayed_fsync = 0;
    g_pserver->aof_fd = -1;
    g_pserver->aof_selected_db = -1; /* Make sure the first time will not match */
    g_pserver->aof_flush_postponed_start = 0;
    g_pserver->aof_rewrite_incremental_fsync = CONFIG_DEFAULT_AOF_REWRITE_INCREMENTAL_FSYNC;
    g_pserver->rdb_save_incremental_fsync = CONFIG_DEFAULT_RDB_SAVE_INCREMENTAL_FSYNC;
    g_pserver->aof_load_truncated = CONFIG_DEFAULT_AOF_LOAD_TRUNCATED;
    g_pserver->aof_use_rdb_preamble = CONFIG_DEFAULT_AOF_USE_RDB_PREAMBLE;
    cserver.pidfile = NULL;
    g_pserver->rdb_filename = NULL;
    g_pserver->rdb_s3bucketpath = NULL;
    g_pserver->aof_filename = zstrdup(CONFIG_DEFAULT_AOF_FILENAME);
    g_pserver->acl_filename = zstrdup(CONFIG_DEFAULT_ACL_FILENAME);
    g_pserver->rdb_compression = CONFIG_DEFAULT_RDB_COMPRESSION;
    g_pserver->rdb_checksum = CONFIG_DEFAULT_RDB_CHECKSUM;
    g_pserver->stop_writes_on_bgsave_err = CONFIG_DEFAULT_STOP_WRITES_ON_BGSAVE_ERROR;
    g_pserver->activerehashing = CONFIG_DEFAULT_ACTIVE_REHASHING;
    g_pserver->active_defrag_running = 0;
    g_pserver->notify_keyspace_events = 0;
    g_pserver->maxclients = CONFIG_DEFAULT_MAX_CLIENTS;
    g_pserver->blocked_clients = 0;
    memset(g_pserver->blocked_clients_by_type,0,
           sizeof(g_pserver->blocked_clients_by_type));
    g_pserver->maxmemory = CONFIG_DEFAULT_MAXMEMORY;
    g_pserver->maxmemory_policy = CONFIG_DEFAULT_MAXMEMORY_POLICY;
    g_pserver->maxmemory_samples = CONFIG_DEFAULT_MAXMEMORY_SAMPLES;
    g_pserver->lfu_log_factor = CONFIG_DEFAULT_LFU_LOG_FACTOR;
    g_pserver->lfu_decay_time = CONFIG_DEFAULT_LFU_DECAY_TIME;
    g_pserver->hash_max_ziplist_entries = OBJ_HASH_MAX_ZIPLIST_ENTRIES;
    g_pserver->hash_max_ziplist_value = OBJ_HASH_MAX_ZIPLIST_VALUE;
    g_pserver->list_max_ziplist_size = OBJ_LIST_MAX_ZIPLIST_SIZE;
    g_pserver->list_compress_depth = OBJ_LIST_COMPRESS_DEPTH;
    g_pserver->set_max_intset_entries = OBJ_SET_MAX_INTSET_ENTRIES;
    g_pserver->zset_max_ziplist_entries = OBJ_ZSET_MAX_ZIPLIST_ENTRIES;
    g_pserver->zset_max_ziplist_value = OBJ_ZSET_MAX_ZIPLIST_VALUE;
    g_pserver->hll_sparse_max_bytes = CONFIG_DEFAULT_HLL_SPARSE_MAX_BYTES;
    g_pserver->stream_node_max_bytes = OBJ_STREAM_NODE_MAX_BYTES;
    g_pserver->stream_node_max_entries = OBJ_STREAM_NODE_MAX_ENTRIES;
    g_pserver->shutdown_asap = 0;
    g_pserver->cluster_enabled = 0;
    g_pserver->cluster_node_timeout = CLUSTER_DEFAULT_NODE_TIMEOUT;
    g_pserver->cluster_migration_barrier = CLUSTER_DEFAULT_MIGRATION_BARRIER;
    g_pserver->cluster_slave_validity_factor = CLUSTER_DEFAULT_SLAVE_VALIDITY;
    g_pserver->cluster_require_full_coverage = CLUSTER_DEFAULT_REQUIRE_FULL_COVERAGE;
    g_pserver->cluster_slave_no_failover = CLUSTER_DEFAULT_SLAVE_NO_FAILOVER;
    g_pserver->cluster_configfile = zstrdup(CONFIG_DEFAULT_CLUSTER_CONFIG_FILE);
    g_pserver->cluster_announce_ip = CONFIG_DEFAULT_CLUSTER_ANNOUNCE_IP;
    g_pserver->cluster_announce_port = CONFIG_DEFAULT_CLUSTER_ANNOUNCE_PORT;
    g_pserver->cluster_announce_bus_port = CONFIG_DEFAULT_CLUSTER_ANNOUNCE_BUS_PORT;
    g_pserver->cluster_module_flags = CLUSTER_MODULE_FLAG_NONE;
    g_pserver->migrate_cached_sockets = dictCreate(&migrateCacheDictType,NULL);
    g_pserver->next_client_id = 1; /* Client IDs, start from 1 .*/
    g_pserver->loading_process_events_interval_bytes = (1024*1024*2);
    g_pserver->lazyfree_lazy_eviction = CONFIG_DEFAULT_LAZYFREE_LAZY_EVICTION;
    g_pserver->lazyfree_lazy_expire = CONFIG_DEFAULT_LAZYFREE_LAZY_EXPIRE;
    g_pserver->lazyfree_lazy_server_del = CONFIG_DEFAULT_LAZYFREE_LAZY_SERVER_DEL;
    g_pserver->always_show_logo = CONFIG_DEFAULT_ALWAYS_SHOW_LOGO;
    g_pserver->lua_time_limit = LUA_SCRIPT_TIME_LIMIT;
    g_pserver->fActiveReplica = CONFIG_DEFAULT_ACTIVE_REPLICA;

    g_pserver->lruclock = getLRUClock();
    resetServerSaveParams();

    appendServerSaveParams(60*60,1);  /* save after 1 hour and 1 change */
    appendServerSaveParams(300,100);  /* save after 5 minutes and 100 changes */
    appendServerSaveParams(60,10000); /* save after 1 minute and 10000 changes */

    /* Replication related */
    g_pserver->masters = listCreate();
    g_pserver->enable_multimaster = CONFIG_DEFAULT_ENABLE_MULTIMASTER;
    g_pserver->repl_syncio_timeout = CONFIG_REPL_SYNCIO_TIMEOUT;
    g_pserver->repl_serve_stale_data = CONFIG_DEFAULT_SLAVE_SERVE_STALE_DATA;
    g_pserver->repl_slave_ro = CONFIG_DEFAULT_SLAVE_READ_ONLY;
    g_pserver->repl_slave_ignore_maxmemory = CONFIG_DEFAULT_SLAVE_IGNORE_MAXMEMORY;
    g_pserver->repl_slave_lazy_flush = CONFIG_DEFAULT_SLAVE_LAZY_FLUSH;
    g_pserver->repl_disable_tcp_nodelay = CONFIG_DEFAULT_REPL_DISABLE_TCP_NODELAY;
    g_pserver->repl_diskless_sync = CONFIG_DEFAULT_REPL_DISKLESS_SYNC;
    g_pserver->repl_diskless_sync_delay = CONFIG_DEFAULT_REPL_DISKLESS_SYNC_DELAY;
    g_pserver->repl_ping_slave_period = CONFIG_DEFAULT_REPL_PING_SLAVE_PERIOD;
    g_pserver->repl_timeout = CONFIG_DEFAULT_REPL_TIMEOUT;
    g_pserver->repl_min_slaves_to_write = CONFIG_DEFAULT_MIN_SLAVES_TO_WRITE;
    g_pserver->repl_min_slaves_max_lag = CONFIG_DEFAULT_MIN_SLAVES_MAX_LAG;
    g_pserver->slave_priority = CONFIG_DEFAULT_SLAVE_PRIORITY;
    g_pserver->slave_announce_ip = CONFIG_DEFAULT_SLAVE_ANNOUNCE_IP;
    g_pserver->slave_announce_port = CONFIG_DEFAULT_SLAVE_ANNOUNCE_PORT;
    g_pserver->master_repl_offset = 0;

    /* Replication partial resync backlog */
    g_pserver->repl_backlog = NULL;
    g_pserver->repl_backlog_size = CONFIG_DEFAULT_REPL_BACKLOG_SIZE;
    g_pserver->repl_backlog_histlen = 0;
    g_pserver->repl_backlog_idx = 0;
    g_pserver->repl_backlog_off = 0;
    g_pserver->repl_backlog_time_limit = CONFIG_DEFAULT_REPL_BACKLOG_TIME_LIMIT;
    g_pserver->repl_no_slaves_since = time(NULL);

    /* Client output buffer limits */
    for (j = 0; j < CLIENT_TYPE_OBUF_COUNT; j++)
        cserver.client_obuf_limits[j] = clientBufferLimitsDefaults[j];

    /* Double constants initialization */
    R_Zero = 0.0;
    R_PosInf = 1.0/R_Zero;
    R_NegInf = -1.0/R_Zero;
    R_Nan = R_Zero/R_Zero;

    /* Command table -- we initiialize it here as it is part of the
     * initial configuration, since command names may be changed via
     * redis.conf using the rename-command directive. */
    g_pserver->commands = dictCreate(&commandTableDictType,NULL);
    g_pserver->orig_commands = dictCreate(&commandTableDictType,NULL);
    populateCommandTable();
    cserver.delCommand = lookupCommandByCString("del");
    cserver.multiCommand = lookupCommandByCString("multi");
    cserver.lpushCommand = lookupCommandByCString("lpush");
    cserver.lpopCommand = lookupCommandByCString("lpop");
    cserver.rpopCommand = lookupCommandByCString("rpop");
    cserver.zpopminCommand = lookupCommandByCString("zpopmin");
    cserver.zpopmaxCommand = lookupCommandByCString("zpopmax");
    cserver.sremCommand = lookupCommandByCString("srem");
    cserver.execCommand = lookupCommandByCString("exec");
    cserver.expireCommand = lookupCommandByCString("expire");
    cserver.pexpireCommand = lookupCommandByCString("pexpire");
    cserver.xclaimCommand = lookupCommandByCString("xclaim");
    cserver.xgroupCommand = lookupCommandByCString("xgroup");
    cserver.rreplayCommand = lookupCommandByCString("rreplay");

    /* Slow log */
    g_pserver->slowlog_log_slower_than = CONFIG_DEFAULT_SLOWLOG_LOG_SLOWER_THAN;
    g_pserver->slowlog_max_len = CONFIG_DEFAULT_SLOWLOG_MAX_LEN;

    /* Latency monitor */
    g_pserver->latency_monitor_threshold = CONFIG_DEFAULT_LATENCY_MONITOR_THRESHOLD;

    /* Debugging */
    g_pserver->assert_failed = "<no assertion failed>";
    g_pserver->assert_file = "<no file>";
    g_pserver->assert_line = 0;
    g_pserver->bug_report_start = 0;
    g_pserver->watchdog_period = 0;

    /* By default we want scripts to be always replicated by effects
     * (single commands executed by the script), and not by sending the
     * script to the replica / AOF. This is the new way starting from
     * Redis 5. However it is possible to revert it via redis.conf. */
    g_pserver->lua_always_replicate_commands = 1;

    /* Multithreading */
    cserver.cthreads = CONFIG_DEFAULT_THREADS;
    cserver.fThreadAffinity = CONFIG_DEFAULT_THREAD_AFFINITY;

    // This will get dereferenced before the second stage init where we have the true db count
    //  so make sure its zero and initialized
    g_pserver->db = (redisDb**)zcalloc(sizeof(redisDb*)*cserver.dbnum, MALLOC_LOCAL);
}

extern char **environ;

/* Restart the server, executing the same executable that started this
 * instance, with the same arguments and configuration file.
 *
 * The function is designed to directly call execve() so that the new
 * server instance will retain the PID of the previous one.
 *
 * The list of flags, that may be bitwise ORed together, alter the
 * behavior of this function:
 *
 * RESTART_SERVER_NONE              No flags.
 * RESTART_SERVER_GRACEFULLY        Do a proper shutdown before restarting.
 * RESTART_SERVER_CONFIG_REWRITE    Rewrite the config file before restarting.
 *
 * On success the function does not return, because the process turns into
 * a different process. On error C_ERR is returned. */
int restartServer(int flags, mstime_t delay) {
    int j;

    /* Check if we still have accesses to the executable that started this
     * server instance. */
    if (access(cserver.executable,X_OK) == -1) {
        serverLog(LL_WARNING,"Can't restart: this process has no "
                             "permissions to execute %s", cserver.executable);
        return C_ERR;
    }

    /* Config rewriting. */
    if (flags & RESTART_SERVER_CONFIG_REWRITE &&
        cserver.configfile &&
        rewriteConfig(cserver.configfile) == -1)
    {
        serverLog(LL_WARNING,"Can't restart: configuration rewrite process "
                             "failed");
        return C_ERR;
    }

    /* Perform a proper shutdown. */
    if (flags & RESTART_SERVER_GRACEFULLY &&
        prepareForShutdown(SHUTDOWN_NOFLAGS) != C_OK)
    {
        serverLog(LL_WARNING,"Can't restart: error preparing for shutdown");
        return C_ERR;
    }

    /* Close all file descriptors, with the exception of stdin, stdout, strerr
     * which are useful if we restart a Redis server which is not daemonized. */
    for (j = 3; j < (int)g_pserver->maxclients + 1024; j++) {
        /* Test the descriptor validity before closing it, otherwise
         * Valgrind issues a warning on close(). */
        if (fcntl(j,F_GETFD) != -1) close(j);
    }

    /* Execute the server with the original command line. */
    if (delay) usleep(delay*1000);
    zfree(cserver.exec_argv[0]);
    cserver.exec_argv[0] = zstrdup(cserver.executable);
    execve(cserver.executable,cserver.exec_argv,environ);

    /* If an error occurred here, there is nothing we can do, but exit. */
    _exit(1);

    return C_ERR; /* Never reached. */
}

/* This function will try to raise the max number of open files accordingly to
 * the configured max number of clients. It also reserves a number of file
 * descriptors (CONFIG_MIN_RESERVED_FDS) for extra operations of
 * persistence, listening sockets, log files and so forth.
 *
 * If it will not be possible to set the limit accordingly to the configured
 * max number of clients, the function will do the reverse setting
 * g_pserver->maxclients to the value that we can actually handle. */
void adjustOpenFilesLimit(void) {
    rlim_t maxfiles = g_pserver->maxclients+CONFIG_MIN_RESERVED_FDS;
    struct rlimit limit;

    if (getrlimit(RLIMIT_NOFILE,&limit) == -1) {
        serverLog(LL_WARNING,"Unable to obtain the current NOFILE limit (%s), assuming 1024 and setting the max clients configuration accordingly.",
            strerror(errno));
        g_pserver->maxclients = 1024-CONFIG_MIN_RESERVED_FDS;
    } else {
        rlim_t oldlimit = limit.rlim_cur;

        /* Set the max number of files if the current limit is not enough
         * for our needs. */
        if (oldlimit < maxfiles) {
            rlim_t bestlimit;
            int setrlimit_error = 0;

            /* Try to set the file limit to match 'maxfiles' or at least
             * to the higher value supported less than maxfiles. */
            bestlimit = maxfiles;
            while(bestlimit > oldlimit) {
                rlim_t decr_step = 16;

                limit.rlim_cur = bestlimit;
                limit.rlim_max = bestlimit;
                if (setrlimit(RLIMIT_NOFILE,&limit) != -1) break;
                setrlimit_error = errno;

                /* We failed to set file limit to 'bestlimit'. Try with a
                 * smaller limit decrementing by a few FDs per iteration. */
                if (bestlimit < decr_step) break;
                bestlimit -= decr_step;
            }

            /* Assume that the limit we get initially is still valid if
             * our last try was even lower. */
            if (bestlimit < oldlimit) bestlimit = oldlimit;

            if (bestlimit < maxfiles) {
                unsigned int old_maxclients = g_pserver->maxclients;
                g_pserver->maxclients = bestlimit-CONFIG_MIN_RESERVED_FDS;
                /* maxclients is unsigned so may overflow: in order
                 * to check if maxclients is now logically less than 1
                 * we test indirectly via bestlimit. */
                if (bestlimit <= CONFIG_MIN_RESERVED_FDS) {
                    serverLog(LL_WARNING,"Your current 'ulimit -n' "
                        "of %llu is not enough for the server to start. "
                        "Please increase your open file limit to at least "
                        "%llu. Exiting.",
                        (unsigned long long) oldlimit,
                        (unsigned long long) maxfiles);
                    exit(1);
                }
                serverLog(LL_WARNING,"You requested maxclients of %d "
                    "requiring at least %llu max file descriptors.",
                    old_maxclients,
                    (unsigned long long) maxfiles);
                serverLog(LL_WARNING,"Server can't set maximum open files "
                    "to %llu because of OS error: %s.",
                    (unsigned long long) maxfiles, strerror(setrlimit_error));
                serverLog(LL_WARNING,"Current maximum open files is %llu. "
                    "maxclients has been reduced to %d to compensate for "
                    "low ulimit. "
                    "If you need higher maxclients increase 'ulimit -n'.",
                    (unsigned long long) bestlimit, g_pserver->maxclients);
            } else {
                serverLog(LL_NOTICE,"Increased maximum number of open files "
                    "to %llu (it was originally set to %llu).",
                    (unsigned long long) maxfiles,
                    (unsigned long long) oldlimit);
            }
        }
    }
}

/* Check that g_pserver->tcp_backlog can be actually enforced in Linux according
 * to the value of /proc/sys/net/core/somaxconn, or warn about it. */
void checkTcpBacklogSettings(void) {
#ifdef HAVE_PROC_SOMAXCONN
    FILE *fp = fopen("/proc/sys/net/core/somaxconn","r");
    char buf[1024];
    if (!fp) return;
    if (fgets(buf,sizeof(buf),fp) != NULL) {
        int somaxconn = atoi(buf);
        if (somaxconn > 0 && somaxconn < g_pserver->tcp_backlog) {
            serverLog(LL_WARNING,"WARNING: The TCP backlog setting of %d cannot be enforced because /proc/sys/net/core/somaxconn is set to the lower value of %d.", g_pserver->tcp_backlog, somaxconn);
        }
    }
    fclose(fp);
#endif
}

/* Initialize a set of file descriptors to listen to the specified 'port'
 * binding the addresses specified in the Redis server configuration.
 *
 * The listening file descriptors are stored in the integer array 'fds'
 * and their number is set in '*count'.
 *
 * The addresses to bind are specified in the global g_pserver->bindaddr array
 * and their number is g_pserver->bindaddr_count. If the server configuration
 * contains no specific addresses to bind, this function will try to
 * bind * (all addresses) for both the IPv4 and IPv6 protocols.
 *
 * On success the function returns C_OK.
 *
 * On error the function returns C_ERR. For the function to be on
 * error, at least one of the g_pserver->bindaddr addresses was
 * impossible to bind, or no bind addresses were specified in the server
 * configuration but the function is not able to bind * for at least
 * one of the IPv4 or IPv6 protocols. */
int listenToPort(int port, int *fds, int *count, int fReusePort, int fFirstListen) {
    int j;

    /* Force binding of 0.0.0.0 if no bind address is specified, always
     * entering the loop if j == 0. */
    if (g_pserver->bindaddr_count == 0) g_pserver->bindaddr[0] = NULL;
    for (j = 0; j < g_pserver->bindaddr_count || j == 0; j++) {
        if (g_pserver->bindaddr[j] == NULL) {
            int unsupported = 0;
            /* Bind * for both IPv6 and IPv4, we enter here only if
             * g_pserver->bindaddr_count == 0. */
            fds[*count] = anetTcp6Server(serverTL->neterr,port,NULL,
                g_pserver->tcp_backlog, fReusePort, fFirstListen);
            if (fds[*count] != ANET_ERR) {
                anetNonBlock(NULL,fds[*count]);
                (*count)++;
            } else if (errno == EAFNOSUPPORT) {
                unsupported++;
                serverLog(LL_WARNING,"Not listening to IPv6: unsupported");
            }

            if (*count == 1 || unsupported) {
                /* Bind the IPv4 address as well. */
                fds[*count] = anetTcpServer(serverTL->neterr,port,NULL,
                    g_pserver->tcp_backlog, fReusePort, fFirstListen);
                if (fds[*count] != ANET_ERR) {
                    anetNonBlock(NULL,fds[*count]);
                    (*count)++;
                } else if (errno == EAFNOSUPPORT) {
                    unsupported++;
                    serverLog(LL_WARNING,"Not listening to IPv4: unsupported");
                }
            }
            /* Exit the loop if we were able to bind * on IPv4 and IPv6,
             * otherwise fds[*count] will be ANET_ERR and we'll print an
             * error and return to the caller with an error. */
            if (*count + unsupported == 2) break;
        } else if (strchr(g_pserver->bindaddr[j],':')) {
            /* Bind IPv6 address. */
            fds[*count] = anetTcp6Server(serverTL->neterr,port,g_pserver->bindaddr[j],
                g_pserver->tcp_backlog, fReusePort, fFirstListen);
        } else {
            /* Bind IPv4 address. */
            fds[*count] = anetTcpServer(serverTL->neterr,port,g_pserver->bindaddr[j],
                g_pserver->tcp_backlog, fReusePort, fFirstListen);
        }
        if (fds[*count] == ANET_ERR) {
            serverLog(LL_WARNING,
                "Could not create server TCP listening socket %s:%d: %s",
                g_pserver->bindaddr[j] ? g_pserver->bindaddr[j] : "*",
                port, serverTL->neterr);
                if (errno == ENOPROTOOPT     || errno == EPROTONOSUPPORT ||
                    errno == ESOCKTNOSUPPORT || errno == EPFNOSUPPORT ||
                    errno == EAFNOSUPPORT    || errno == EADDRNOTAVAIL)
                    continue;
            return C_ERR;
        }
        anetNonBlock(NULL,fds[*count]);
        (*count)++;
    }
    return C_OK;
}

/* Resets the stats that we expose via INFO or other means that we want
 * to reset via CONFIG RESETSTAT. The function is also used in order to
 * initialize these fields in initServer() at server startup. */
void resetServerStats(void) {
    int j;

    g_pserver->stat_numcommands = 0;
    g_pserver->stat_numconnections = 0;
    g_pserver->stat_expiredkeys = 0;
    g_pserver->stat_expired_stale_perc = 0;
    g_pserver->stat_expired_time_cap_reached_count = 0;
    g_pserver->stat_evictedkeys = 0;
    g_pserver->stat_keyspace_misses = 0;
    g_pserver->stat_keyspace_hits = 0;
    g_pserver->stat_active_defrag_hits = 0;
    g_pserver->stat_active_defrag_misses = 0;
    g_pserver->stat_active_defrag_key_hits = 0;
    g_pserver->stat_active_defrag_key_misses = 0;
    g_pserver->stat_active_defrag_scanned = 0;
    g_pserver->stat_fork_time = 0;
    g_pserver->stat_fork_rate = 0;
    g_pserver->stat_rejected_conn = 0;
    g_pserver->stat_sync_full = 0;
    g_pserver->stat_sync_partial_ok = 0;
    g_pserver->stat_sync_partial_err = 0;
    for (j = 0; j < STATS_METRIC_COUNT; j++) {
        g_pserver->inst_metric[j].idx = 0;
        g_pserver->inst_metric[j].last_sample_time = mstime();
        g_pserver->inst_metric[j].last_sample_count = 0;
        memset(g_pserver->inst_metric[j].samples,0,
            sizeof(g_pserver->inst_metric[j].samples));
    }
    g_pserver->stat_net_input_bytes = 0;
    g_pserver->stat_net_output_bytes = 0;
    g_pserver->aof_delayed_fsync = 0;
}

static void initNetworkingThread(int iel, int fReusePort)
{
    /* Open the TCP listening socket for the user commands. */
    if (fReusePort || (iel == IDX_EVENT_LOOP_MAIN))
    {
        if (g_pserver->port != 0 &&
            listenToPort(g_pserver->port,g_pserver->rgthreadvar[iel].ipfd,&g_pserver->rgthreadvar[iel].ipfd_count, fReusePort, (iel == IDX_EVENT_LOOP_MAIN)) == C_ERR)
            exit(1);
    }
    else
    {
        // We use the main threads file descriptors
        memcpy(g_pserver->rgthreadvar[iel].ipfd, g_pserver->rgthreadvar[IDX_EVENT_LOOP_MAIN].ipfd, sizeof(int)*CONFIG_BINDADDR_MAX);
        g_pserver->rgthreadvar[iel].ipfd_count = g_pserver->rgthreadvar[IDX_EVENT_LOOP_MAIN].ipfd_count;
    }

    /* Create an event handler for accepting new connections in TCP */
    for (int j = 0; j < g_pserver->rgthreadvar[iel].ipfd_count; j++) {
        if (aeCreateFileEvent(g_pserver->rgthreadvar[iel].el, g_pserver->rgthreadvar[iel].ipfd[j], AE_READABLE|AE_READ_THREADSAFE,
            acceptTcpHandler,NULL) == AE_ERR)
            {
                serverPanic(
                    "Unrecoverable error creating g_pserver->ipfd file event.");
            }
    }
}

static void initNetworking(int fReusePort)
{
    int celListen = (fReusePort) ? cserver.cthreads : 1;
    for (int iel = 0; iel < celListen; ++iel)
        initNetworkingThread(iel, fReusePort);

    /* Open the listening Unix domain socket. */
    if (g_pserver->unixsocket != NULL) {
        unlink(g_pserver->unixsocket); /* don't care if this fails */
        g_pserver->sofd = anetUnixServer(serverTL->neterr,g_pserver->unixsocket,
            g_pserver->unixsocketperm, g_pserver->tcp_backlog);
        if (g_pserver->sofd == ANET_ERR) {
            serverLog(LL_WARNING, "Opening Unix socket: %s", serverTL->neterr);
            exit(1);
        }
        anetNonBlock(NULL,g_pserver->sofd);
    }

    /* Abort if there are no listening sockets at all. */
    if (g_pserver->rgthreadvar[IDX_EVENT_LOOP_MAIN].ipfd_count == 0 && g_pserver->sofd < 0) {
        serverLog(LL_WARNING, "Configured to not listen anywhere, exiting.");
        exit(1);
    }

    if (g_pserver->sofd > 0 && aeCreateFileEvent(g_pserver->rgthreadvar[IDX_EVENT_LOOP_MAIN].el,g_pserver->sofd,AE_READABLE|AE_READ_THREADSAFE,
        acceptUnixHandler,NULL) == AE_ERR) serverPanic("Unrecoverable error creating g_pserver->sofd file event.");
}

static void initServerThread(struct redisServerThreadVars *pvar, int fMain)
{
    pvar->unblocked_clients = listCreate();
    pvar->clients_pending_asyncwrite = listCreate();
    pvar->ipfd_count = 0;
    pvar->cclients = 0;
    pvar->el = aeCreateEventLoop(g_pserver->maxclients+CONFIG_FDSET_INCR);
    pvar->current_client = nullptr;
    pvar->clients_paused = 0;
    if (pvar->el == NULL) {
        serverLog(LL_WARNING,
            "Failed creating the event loop. Error message: '%s'",
            strerror(errno));
        exit(1);
    }

    fastlock_init(&pvar->lockPendingWrite, "lockPendingWrite");

    if (!fMain)
    {
        if (aeCreateTimeEvent(pvar->el, 1, serverCronLite, NULL, NULL) == AE_ERR) {
            serverPanic("Can't create event loop timers.");
            exit(1);
        }
    }

    if (pipe(pvar->module_blocked_pipe) == -1) {
        serverLog(LL_WARNING,
            "Can't create the pipe for module blocking commands: %s",
            strerror(errno));
        exit(1);
    }

    /* Make the pipe non blocking. This is just a best effort aware mechanism
     * and we do not want to block not in the read nor in the write half. */
    anetNonBlock(NULL,pvar->module_blocked_pipe[0]);
    anetNonBlock(NULL,pvar->module_blocked_pipe[1]);

    /* Register a readable event for the pipe used to awake the event loop
     * when a blocked client in a module needs attention. */
    if (aeCreateFileEvent(pvar->el, pvar->module_blocked_pipe[0], AE_READABLE,
        moduleBlockedClientPipeReadable,NULL) == AE_ERR) {
            serverPanic(
                "Error registering the readable event for the module "
                "blocked clients subsystem.");
    }


    /* Register a readable event for the pipe used to awake the event loop
     * when a blocked client in a module needs attention. */
    if (aeCreateFileEvent(pvar->el, pvar->module_blocked_pipe[0], AE_READABLE,
        moduleBlockedClientPipeReadable,NULL) == AE_ERR) {
            serverPanic(
                "Error registering the readable event for the module "
                "blocked clients subsystem.");
    }
}

void initServer(void) {
    signal(SIGHUP, SIG_IGN);
    signal(SIGPIPE, SIG_IGN);
    setupSignalHandlers();

    zfree(g_pserver->db);   // initServerConfig created a dummy array, free that now
    g_pserver->db = (redisDb**)zmalloc(sizeof(redisDb*)*cserver.dbnum, MALLOC_LOCAL);

    /* Create the Redis databases, and initialize other internal state. */
    for (int j = 0; j < cserver.dbnum; j++) {
        g_pserver->db[j] = new (MALLOC_LOCAL) redisDb();
        g_pserver->db[j]->initialize(j);
    }

    /* Fixup Master Client Database */
    listIter li;
    listNode *ln;
    listRewind(g_pserver->masters, &li);
    while ((ln = listNext(&li)))
    {
        redisMaster *mi = (redisMaster*)listNodeValue(ln);
        serverAssert(mi->master == nullptr);
        if (mi->cached_master != nullptr)
            selectDb(mi->cached_master, 0);
    }

    if (g_pserver->syslog_enabled) {
        openlog(g_pserver->syslog_ident, LOG_PID | LOG_NDELAY | LOG_NOWAIT,
            g_pserver->syslog_facility);
    }

    g_pserver->hz = g_pserver->config_hz;
    cserver.pid = getpid();
    g_pserver->clients_index = raxNew();
    g_pserver->clients_to_close = listCreate();
    g_pserver->replicaseldb = -1; /* Force to emit the first SELECT command. */
    g_pserver->ready_keys = listCreate();
    g_pserver->clients_waiting_acks = listCreate();
    g_pserver->get_ack_from_slaves = 0;
    cserver.system_memory_size = zmalloc_get_memory_size();

    createSharedObjects();
    adjustOpenFilesLimit();

    evictionPoolAlloc(); /* Initialize the LRU keys pool. */
    g_pserver->pubsub_channels = dictCreate(&keylistDictType,NULL);
    g_pserver->pubsub_patterns = listCreate();
    listSetFreeMethod(g_pserver->pubsub_patterns,freePubsubPattern);
    listSetMatchMethod(g_pserver->pubsub_patterns,listMatchPubsubPattern);
    g_pserver->cronloops = 0;
    g_pserver->rdbThreadVars.fRdbThreadActive = false;
    g_pserver->aof_child_pid = -1;
    g_pserver->rdb_child_type = RDB_CHILD_TYPE_NONE;
    g_pserver->rdb_bgsave_scheduled = 0;
    g_pserver->child_info_pipe[0] = -1;
    g_pserver->child_info_pipe[1] = -1;
    g_pserver->child_info_data.magic = 0;
    aofRewriteBufferReset();
    g_pserver->aof_buf = sdsempty();
    g_pserver->lastsave = time(NULL); /* At startup we consider the DB saved. */
    g_pserver->lastbgsave_try = 0;    /* At startup we never tried to BGSAVE. */
    g_pserver->rdb_save_time_last = -1;
    g_pserver->rdb_save_time_start = -1;
    g_pserver->dirty = 0;
    resetServerStats();
    /* A few stats we don't want to reset: server startup time, and peak mem. */
    cserver.stat_starttime = time(NULL);
    g_pserver->stat_peak_memory = 0;
    g_pserver->stat_rdb_cow_bytes = 0;
    g_pserver->stat_aof_cow_bytes = 0;
    g_pserver->cron_malloc_stats.zmalloc_used = 0;
    g_pserver->cron_malloc_stats.process_rss = 0;
    g_pserver->cron_malloc_stats.allocator_allocated = 0;
    g_pserver->cron_malloc_stats.allocator_active = 0;
    g_pserver->cron_malloc_stats.allocator_resident = 0;
    g_pserver->lastbgsave_status = C_OK;
    g_pserver->aof_last_write_status = C_OK;
    g_pserver->aof_last_write_errno = 0;
    g_pserver->repl_good_slaves_count = 0;

    g_pserver->mvcc_tstamp = 0;

    /* Create the timer callback, this is our way to process many background
     * operations incrementally, like clients timeout, eviction of unaccessed
     * expired keys and so forth. */
    if (aeCreateTimeEvent(g_pserver->rgthreadvar[IDX_EVENT_LOOP_MAIN].el, 1, serverCron, NULL, NULL) == AE_ERR) {
        serverPanic("Can't create event loop timers.");
        exit(1);
    }

    /* Open the AOF file if needed. */
    if (g_pserver->aof_state == AOF_ON) {
        g_pserver->aof_fd = open(g_pserver->aof_filename,
                               O_WRONLY|O_APPEND|O_CREAT,0644);
        if (g_pserver->aof_fd == -1) {
            serverLog(LL_WARNING, "Can't open the append-only file: %s",
                strerror(errno));
            exit(1);
        }
    }

    /* 32 bit instances are limited to 4GB of address space, so if there is
     * no explicit limit in the user provided configuration we set a limit
     * at 3 GB using maxmemory with 'noeviction' policy'. This avoids
     * useless crashes of the Redis instance for out of memory. */
    if (sizeof(void*) == 4 && g_pserver->maxmemory == 0) {
        serverLog(LL_WARNING,"Warning: 32 bit instance detected but no memory limit set. Setting 3 GB maxmemory limit with 'noeviction' policy now.");
        g_pserver->maxmemory = 3072LL*(1024*1024); /* 3 GB */
        g_pserver->maxmemory_policy = MAXMEMORY_NO_EVICTION;
    }

    /* Generate UUID */
    static_assert(sizeof(uuid_t) == sizeof(cserver.uuid), "UUIDs are standardized at 16-bytes");
    uuid_generate((unsigned char*)cserver.uuid);

    if (g_pserver->cluster_enabled) clusterInit();
    replicationScriptCacheInit();
    scriptingInit(1);
    slowlogInit();
    latencyMonitorInit();
    bioInit();
    g_pserver->initial_memory_usage = zmalloc_used_memory();

    g_pserver->asyncworkqueue = new (MALLOC_LOCAL) AsyncWorkQueue(cserver.cthreads);
}

/* Parse the flags string description 'strflags' and set them to the
 * command 'c'. If the flags are all valid C_OK is returned, otherwise
 * C_ERR is returned (yet the recognized flags are set in the command). */
int populateCommandTableParseFlags(struct redisCommand *c, const char *strflags) {
    int argc;
    sds *argv;

    /* Split the line into arguments for processing. */
    argv = sdssplitargs(strflags,&argc);
    if (argv == NULL) return C_ERR;

    for (int j = 0; j < argc; j++) {
        char *flag = argv[j];
        if (!strcasecmp(flag,"write")) {
            c->flags |= CMD_WRITE|CMD_CATEGORY_WRITE;
        } else if (!strcasecmp(flag,"read-only")) {
            c->flags |= CMD_READONLY|CMD_CATEGORY_READ;
        } else if (!strcasecmp(flag,"use-memory")) {
            c->flags |= CMD_DENYOOM;
        } else if (!strcasecmp(flag,"admin")) {
            c->flags |= CMD_ADMIN|CMD_CATEGORY_ADMIN|CMD_CATEGORY_DANGEROUS;
        } else if (!strcasecmp(flag,"pub-sub")) {
            c->flags |= CMD_PUBSUB|CMD_CATEGORY_PUBSUB;
        } else if (!strcasecmp(flag,"no-script")) {
            c->flags |= CMD_NOSCRIPT;
        } else if (!strcasecmp(flag,"random")) {
            c->flags |= CMD_RANDOM;
        } else if (!strcasecmp(flag,"to-sort")) {
            c->flags |= CMD_SORT_FOR_SCRIPT;
        } else if (!strcasecmp(flag,"ok-loading")) {
            c->flags |= CMD_LOADING;
        } else if (!strcasecmp(flag,"ok-stale")) {
            c->flags |= CMD_STALE;
        } else if (!strcasecmp(flag,"no-monitor")) {
            c->flags |= CMD_SKIP_MONITOR;
        } else if (!strcasecmp(flag,"cluster-asking")) {
            c->flags |= CMD_ASKING;
        } else if (!strcasecmp(flag,"fast")) {
            c->flags |= CMD_FAST | CMD_CATEGORY_FAST;
        } else if (!strcasecmp(flag,"noprop")) {
            c->flags |= CMD_SKIP_PROPOGATE;
        } else {
            /* Parse ACL categories here if the flag name starts with @. */
            uint64_t catflag;
            if (flag[0] == '@' &&
                (catflag = ACLGetCommandCategoryFlagByName(flag+1)) != 0)
            {
                c->flags |= catflag;
            } else {
                sdsfreesplitres(argv,argc);
                return C_ERR;
            }
        }
    }
    /* If it's not @fast is @slow in this binary world. */
    if (!(c->flags & CMD_CATEGORY_FAST)) c->flags |= CMD_CATEGORY_SLOW;

    sdsfreesplitres(argv,argc);
    return C_OK;
}

/* Populates the Redis Command Table starting from the hard coded list
 * we have on top of redis.c file. */
void populateCommandTable(void) {
    int j;
    int numcommands = sizeof(redisCommandTable)/sizeof(struct redisCommand);

    for (j = 0; j < numcommands; j++) {
        struct redisCommand *c = redisCommandTable+j;
        int retval1, retval2;

        /* Translate the command string flags description into an actual
         * set of flags. */
        if (populateCommandTableParseFlags(c,c->sflags) == C_ERR)
            serverPanic("Unsupported command flag");

        c->id = ACLGetCommandID(c->name); /* Assign the ID used for ACL. */
        retval1 = dictAdd(g_pserver->commands, sdsnew(c->name), c);
        /* Populate an additional dictionary that will be unaffected
         * by rename-command statements in redis.conf. */
        retval2 = dictAdd(g_pserver->orig_commands, sdsnew(c->name), c);
        serverAssert(retval1 == DICT_OK && retval2 == DICT_OK);
    }
}

void resetCommandTableStats(void) {
    struct redisCommand *c;
    dictEntry *de;
    dictIterator *di;

    di = dictGetSafeIterator(g_pserver->commands);
    while((de = dictNext(di)) != NULL) {
        c = (struct redisCommand *) dictGetVal(de);
        c->microseconds = 0;
        c->calls = 0;
    }
    dictReleaseIterator(di);

}

/* ========================== Redis OP Array API ============================ */

void redisOpArrayInit(redisOpArray *oa) {
    oa->ops = NULL;
    oa->numops = 0;
}

int redisOpArrayAppend(redisOpArray *oa, struct redisCommand *cmd, int dbid,
                       robj **argv, int argc, int target)
{
    redisOp *op;

    oa->ops = (redisOp*)zrealloc(oa->ops,sizeof(redisOp)*(oa->numops+1), MALLOC_LOCAL);
    op = oa->ops+oa->numops;
    op->cmd = cmd;
    op->dbid = dbid;
    op->argv = argv;
    op->argc = argc;
    op->target = target;
    oa->numops++;
    return oa->numops;
}

void redisOpArrayFree(redisOpArray *oa) {
    while(oa->numops) {
        int j;
        redisOp *op;

        oa->numops--;
        op = oa->ops+oa->numops;
        for (j = 0; j < op->argc; j++)
            decrRefCount(op->argv[j]);
        zfree(op->argv);
    }
    zfree(oa->ops);
}

/* ====================== Commands lookup and execution ===================== */

struct redisCommand *lookupCommand(sds name) {
    return (struct redisCommand*)dictFetchValue(g_pserver->commands, name);
}

struct redisCommand *lookupCommandByCString(const char *s) {
    struct redisCommand *cmd;
    sds name = sdsnew(s);

    cmd = (struct redisCommand*)dictFetchValue(g_pserver->commands, name);
    sdsfree(name);
    return cmd;
}

/* Lookup the command in the current table, if not found also check in
 * the original table containing the original command names unaffected by
 * redis.conf rename-command statement.
 *
 * This is used by functions rewriting the argument vector such as
 * rewriteClientCommandVector() in order to set client->cmd pointer
 * correctly even if the command was renamed. */
struct redisCommand *lookupCommandOrOriginal(sds name) {
    struct redisCommand *cmd = (struct redisCommand*)dictFetchValue(g_pserver->commands, name);

    if (!cmd) cmd = (struct redisCommand*)dictFetchValue(g_pserver->orig_commands,name);
    return cmd;
}

/* Propagate the specified command (in the context of the specified database id)
 * to AOF and Slaves.
 *
 * flags are an xor between:
 * + PROPAGATE_NONE (no propagation of command at all)
 * + PROPAGATE_AOF (propagate into the AOF file if is enabled)
 * + PROPAGATE_REPL (propagate into the replication link)
 *
 * This should not be used inside commands implementation. Use instead
 * alsoPropagate(), preventCommandPropagation(), forceCommandPropagation().
 */
void propagate(struct redisCommand *cmd, int dbid, robj **argv, int argc,
               int flags)
{
    serverAssert(GlobalLocksAcquired());
    if (g_pserver->aof_state != AOF_OFF && flags & PROPAGATE_AOF)
        feedAppendOnlyFile(cmd,dbid,argv,argc);
    if (flags & PROPAGATE_REPL)
        replicationFeedSlaves(g_pserver->slaves,dbid,argv,argc);
}

/* Used inside commands to schedule the propagation of additional commands
 * after the current command is propagated to AOF / Replication.
 *
 * 'cmd' must be a pointer to the Redis command to replicate, dbid is the
 * database ID the command should be propagated into.
 * Arguments of the command to propagte are passed as an array of redis
 * objects pointers of len 'argc', using the 'argv' vector.
 *
 * The function does not take a reference to the passed 'argv' vector,
 * so it is up to the caller to release the passed argv (but it is usually
 * stack allocated).  The function autoamtically increments ref count of
 * passed objects, so the caller does not need to. */
void alsoPropagate(struct redisCommand *cmd, int dbid, robj **argv, int argc,
                   int target)
{
    robj **argvcopy;
    int j;

    if (g_pserver->loading) return; /* No propagation during loading. */

    argvcopy = (robj**)zmalloc(sizeof(robj*)*argc, MALLOC_LOCAL);
    for (j = 0; j < argc; j++) {
        argvcopy[j] = argv[j];
        incrRefCount(argv[j]);
    }
    redisOpArrayAppend(&g_pserver->also_propagate,cmd,dbid,argvcopy,argc,target);
}

/* It is possible to call the function forceCommandPropagation() inside a
 * Redis command implementation in order to to force the propagation of a
 * specific command execution into AOF / Replication. */
void forceCommandPropagation(client *c, int flags) {
    if (flags & PROPAGATE_REPL) c->flags |= CLIENT_FORCE_REPL;
    if (flags & PROPAGATE_AOF) c->flags |= CLIENT_FORCE_AOF;
}

/* Avoid that the executed command is propagated at all. This way we
 * are free to just propagate what we want using the alsoPropagate()
 * API. */
void preventCommandPropagation(client *c) {
    c->flags |= CLIENT_PREVENT_PROP;
}

/* AOF specific version of preventCommandPropagation(). */
void preventCommandAOF(client *c) {
    c->flags |= CLIENT_PREVENT_AOF_PROP;
}

/* Replication specific version of preventCommandPropagation(). */
void preventCommandReplication(client *c) {
    c->flags |= CLIENT_PREVENT_REPL_PROP;
}

/* Call() is the core of Redis execution of a command.
 *
 * The following flags can be passed:
 * CMD_CALL_NONE        No flags.
 * CMD_CALL_SLOWLOG     Check command speed and log in the slow log if needed.
 * CMD_CALL_STATS       Populate command stats.
 * CMD_CALL_PROPAGATE_AOF   Append command to AOF if it modified the dataset
 *                          or if the client flags are forcing propagation.
 * CMD_CALL_PROPAGATE_REPL  Send command to salves if it modified the dataset
 *                          or if the client flags are forcing propagation.
 * CMD_CALL_PROPAGATE   Alias for PROPAGATE_AOF|PROPAGATE_REPL.
 * CMD_CALL_FULL        Alias for SLOWLOG|STATS|PROPAGATE.
 *
 * The exact propagation behavior depends on the client flags.
 * Specifically:
 *
 * 1. If the client flags CLIENT_FORCE_AOF or CLIENT_FORCE_REPL are set
 *    and assuming the corresponding CMD_CALL_PROPAGATE_AOF/REPL is set
 *    in the call flags, then the command is propagated even if the
 *    dataset was not affected by the command.
 * 2. If the client flags CLIENT_PREVENT_REPL_PROP or CLIENT_PREVENT_AOF_PROP
 *    are set, the propagation into AOF or to slaves is not performed even
 *    if the command modified the dataset.
 *
 * Note that regardless of the client flags, if CMD_CALL_PROPAGATE_AOF
 * or CMD_CALL_PROPAGATE_REPL are not set, then respectively AOF or
 * slaves propagation will never occur.
 *
 * Client flags are modified by the implementation of a given command
 * using the following API:
 *
 * forceCommandPropagation(client *c, int flags);
 * preventCommandPropagation(client *c);
 * preventCommandAOF(client *c);
 * preventCommandReplication(client *c);
 *
 */
void call(client *c, int flags) {
    long long dirty, start, duration;
    int client_old_flags = c->flags;
    struct redisCommand *real_cmd = c->cmd;
    serverAssert(GlobalLocksAcquired());

    /* Sent the command to clients in MONITOR mode, only if the commands are
     * not generated from reading an AOF. */
    if (listLength(g_pserver->monitors) &&
        !g_pserver->loading &&
        !(c->cmd->flags & (CMD_SKIP_MONITOR|CMD_ADMIN)))
    {
        replicationFeedMonitors(c,g_pserver->monitors,c->db->id,c->argv,c->argc);
    }

    /* Initialization: clear the flags that must be set by the command on
     * demand, and initialize the array for additional commands propagation. */
    c->flags &= ~(CLIENT_FORCE_AOF|CLIENT_FORCE_REPL|CLIENT_PREVENT_PROP);
    redisOpArray prev_also_propagate = g_pserver->also_propagate;
    redisOpArrayInit(&g_pserver->also_propagate);

    /* Call the command. */
    dirty = g_pserver->dirty;
    start = ustime();
    c->cmd->proc(c);
    serverTL->commandsExecuted++;
    duration = ustime()-start;
    dirty = g_pserver->dirty-dirty;
    if (dirty < 0) dirty = 0;

    if (dirty)
        c->mvccCheckpoint = getMvccTstamp();

    /* When EVAL is called loading the AOF we don't want commands called
     * from Lua to go into the slowlog or to populate statistics. */
    if (g_pserver->loading && c->flags & CLIENT_LUA)
        flags &= ~(CMD_CALL_SLOWLOG | CMD_CALL_STATS);

    /* If the caller is Lua, we want to force the EVAL caller to propagate
     * the script if the command flag or client flag are forcing the
     * propagation. */
    if (c->flags & CLIENT_LUA && g_pserver->lua_caller) {
        if (c->flags & CLIENT_FORCE_REPL)
            g_pserver->lua_caller->flags |= CLIENT_FORCE_REPL;
        if (c->flags & CLIENT_FORCE_AOF)
            g_pserver->lua_caller->flags |= CLIENT_FORCE_AOF;
    }

    /* Log the command into the Slow log if needed, and populate the
     * per-command statistics that we show in INFO commandstats. */
    if (flags & CMD_CALL_SLOWLOG && c->cmd->proc != execCommand) {
        const char *latency_event = (c->cmd->flags & CMD_FAST) ?
                              "fast-command" : "command";
        latencyAddSampleIfNeeded(latency_event,duration/1000);
        slowlogPushEntryIfNeeded(c,c->argv,c->argc,duration);
    }

    if (flags & CMD_CALL_STATS) {
        /* use the real command that was executed (cmd and lastamc) may be
         * different, in case of MULTI-EXEC or re-written commands such as
         * EXPIRE, GEOADD, etc. */
        real_cmd->microseconds += duration;
        real_cmd->calls++;
    }

    /* Propagate the command into the AOF and replication link */
    if (flags & CMD_CALL_PROPAGATE &&
        (c->flags & CLIENT_PREVENT_PROP) != CLIENT_PREVENT_PROP)
    {
        int propagate_flags = PROPAGATE_NONE;

        /* Check if the command operated changes in the data set. If so
         * set for replication / AOF propagation. */
        if (dirty) propagate_flags |= (PROPAGATE_AOF|PROPAGATE_REPL);

        /* If the client forced AOF / replication of the command, set
         * the flags regardless of the command effects on the data set. */
        if (c->flags & CLIENT_FORCE_REPL) propagate_flags |= PROPAGATE_REPL;
        if (c->flags & CLIENT_FORCE_AOF) propagate_flags |= PROPAGATE_AOF;

        /* However prevent AOF / replication propagation if the command
         * implementations called preventCommandPropagation() or similar,
         * or if we don't have the call() flags to do so. */
        if (c->flags & CLIENT_PREVENT_REPL_PROP ||
            !(flags & CMD_CALL_PROPAGATE_REPL))
                propagate_flags &= ~PROPAGATE_REPL;
        if (c->flags & CLIENT_PREVENT_AOF_PROP ||
            !(flags & CMD_CALL_PROPAGATE_AOF))
                propagate_flags &= ~PROPAGATE_AOF;

        if (c->cmd->flags & CMD_SKIP_PROPOGATE)
            propagate_flags &= ~PROPAGATE_REPL;

        /* Call propagate() only if at least one of AOF / replication
         * propagation is needed. Note that modules commands handle replication
         * in an explicit way, so we never replicate them automatically. */
        if (propagate_flags != PROPAGATE_NONE && !(c->cmd->flags & CMD_MODULE))
            propagate(c->cmd,c->db->id,c->argv,c->argc,propagate_flags);
    }

    /* Restore the old replication flags, since call() can be executed
     * recursively. */
    c->flags &= ~(CLIENT_FORCE_AOF|CLIENT_FORCE_REPL|CLIENT_PREVENT_PROP);
    c->flags |= client_old_flags &
        (CLIENT_FORCE_AOF|CLIENT_FORCE_REPL|CLIENT_PREVENT_PROP);

    /* Handle the alsoPropagate() API to handle commands that want to propagate
     * multiple separated commands. Note that alsoPropagate() is not affected
     * by CLIENT_PREVENT_PROP flag. */
    if (g_pserver->also_propagate.numops) {
        int j;
        redisOp *rop;

        if (flags & CMD_CALL_PROPAGATE) {
            for (j = 0; j < g_pserver->also_propagate.numops; j++) {
                rop = &g_pserver->also_propagate.ops[j];
                int target = rop->target;
                /* Whatever the command wish is, we honor the call() flags. */
                if (!(flags&CMD_CALL_PROPAGATE_AOF)) target &= ~PROPAGATE_AOF;
                if (!(flags&CMD_CALL_PROPAGATE_REPL)) target &= ~PROPAGATE_REPL;
                if (target)
                    propagate(rop->cmd,rop->dbid,rop->argv,rop->argc,target);
            }
        }
        redisOpArrayFree(&g_pserver->also_propagate);
    }

    ProcessPendingAsyncWrites();
    
    g_pserver->also_propagate = prev_also_propagate;

    /* If the client has keys tracking enabled for client side caching,
     * make sure to remember the keys it fetched via this command. */
    if (c->cmd->flags & CMD_READONLY) {
        client *caller = (c->flags & CLIENT_LUA && g_pserver->lua_caller) ?
                            g_pserver->lua_caller : c;
        if (caller->flags & CLIENT_TRACKING)
            trackingRememberKeys(caller);
    }

    g_pserver->stat_numcommands++;
}

/* If this function gets called we already read a whole
 * command, arguments are in the client argv/argc fields.
 * processCommand() execute the command or prepare the
 * server for a bulk read from the client.
 *
 * If C_OK is returned the client is still alive and valid and
 * other operations can be performed by the caller. Otherwise
 * if C_ERR is returned the client was destroyed (i.e. after QUIT). */
int processCommand(client *c, int callFlags) {
    AeLocker locker;
    AssertCorrectThread(c);

    if (moduleHasCommandFilters())
    {
        locker.arm(c);
        moduleCallCommandFilters(c);
    }

    /* The QUIT command is handled separately. Normal command procs will
     * go through checking for replication and QUIT will cause trouble
     * when FORCE_REPLICATION is enabled and would be implemented in
     * a regular command proc. */
    if (!strcasecmp((const char*)ptrFromObj(c->argv[0]),"quit")) {
        addReply(c,shared.ok);
        c->flags |= CLIENT_CLOSE_AFTER_REPLY;
        return C_ERR;
    }

    /* Now lookup the command and check ASAP about trivial error conditions
     * such as wrong arity, bad command name and so forth. */
    c->cmd = c->lastcmd = lookupCommand((sds)ptrFromObj(c->argv[0]));
    if (!c->cmd) {
        flagTransaction(c);
        sds args = sdsempty();
        int i;
        for (i=1; i < c->argc && sdslen(args) < 128; i++)
            args = sdscatprintf(args, "`%.*s`, ", 128-(int)sdslen(args), (char*)ptrFromObj(c->argv[i]));
        addReplyErrorFormat(c,"unknown command `%s`, with args beginning with: %s",
            (char*)ptrFromObj(c->argv[0]), args);
        sdsfree(args);
        return C_OK;
    } else if ((c->cmd->arity > 0 && c->cmd->arity != c->argc) ||
               (c->argc < -c->cmd->arity)) {
        flagTransaction(c);
        addReplyErrorFormat(c,"wrong number of arguments for '%s' command",
            c->cmd->name);
        return C_OK;
    }

    /* Check if the user is authenticated. This check is skipped in case
     * the default user is flagged as "nopass" and is active. */
    int auth_required = !(DefaultUser->flags & USER_FLAG_NOPASS) &&
                        !c->authenticated;
    if (auth_required || DefaultUser->flags & USER_FLAG_DISABLED) {
        /* AUTH and HELLO are valid even in non authenticated state. */
        if (c->cmd->proc != authCommand || c->cmd->proc == helloCommand) {
            flagTransaction(c);
            addReply(c,shared.noautherr);
            return C_OK;
        }
    }

    /* Check if the user can run this command according to the current
     * ACLs. */
    if (c->puser && !(c->puser->flags & USER_FLAG_ALLCOMMANDS))
        locker.arm(c);  // ACLs require the lock
    int acl_retval = ACLCheckCommandPerm(c);
    if (acl_retval != ACL_OK) {
        flagTransaction(c);
        if (acl_retval == ACL_DENIED_CMD)
            addReplyErrorFormat(c,
                "-NOPERM this user has no permissions to run "
                "the '%s' command or its subcommand", c->cmd->name);
        else
            addReplyErrorFormat(c,
                "-NOPERM this user has no permissions to access "
                "one of the keys used as arguments");
        return C_OK;
    }

    /* If cluster is enabled perform the cluster redirection here.
     * However we don't perform the redirection if:
     * 1) The sender of this command is our master.
     * 2) The command has no key arguments. */
    if (g_pserver->cluster_enabled &&
        !(c->flags & CLIENT_MASTER) &&
        !(c->flags & CLIENT_LUA &&
          g_pserver->lua_caller->flags & CLIENT_MASTER) &&
        !(c->cmd->getkeys_proc == NULL && c->cmd->firstkey == 0 &&
          c->cmd->proc != execCommand))
    {
        locker.arm(c);
        int hashslot;
        int error_code;
        clusterNode *n = getNodeByQuery(c,c->cmd,c->argv,c->argc,
                                        &hashslot,&error_code);
        if (n == NULL || n != g_pserver->cluster->myself) {
            if (c->cmd->proc == execCommand) {
                discardTransaction(c);
            } else {
                flagTransaction(c);
            }
            clusterRedirectClient(c,n,hashslot,error_code);
            return C_OK;
        }
    }

    incrementMvccTstamp();
    
    if (!locker.isArmed())
        locker.arm(c);

    /* Handle the maxmemory directive.
     *
     * Note that we do not want to reclaim memory if we are here re-entering
     * the event loop since there is a busy Lua script running in timeout
     * condition, to avoid mixing the propagation of scripts with the
     * propagation of DELs due to eviction. */
    if (g_pserver->maxmemory && !g_pserver->lua_timedout) {
        int out_of_memory = freeMemoryIfNeededAndSafe() == C_ERR;
        /* freeMemoryIfNeeded may flush replica output buffers. This may result
         * into a replica, that may be the active client, to be freed. */
        if (serverTL->current_client == NULL) return C_ERR;

        /* It was impossible to free enough memory, and the command the client
         * is trying to execute is denied during OOM conditions or the client
         * is in MULTI/EXEC context? Error. */
        if (out_of_memory &&
            (c->cmd->flags & CMD_DENYOOM ||
             (c->flags & CLIENT_MULTI && c->cmd->proc != execCommand))) {
            flagTransaction(c);
            addReply(c, shared.oomerr);
            return C_OK;
        }
    }

    /* Don't accept write commands if there are problems persisting on disk
     * and if this is a master instance. */
    int deny_write_type = writeCommandsDeniedByDiskError();
    if (deny_write_type != DISK_ERROR_TYPE_NONE &&
        listLength(g_pserver->masters) == 0 &&
        (c->cmd->flags & CMD_WRITE ||
         c->cmd->proc == pingCommand))
    {
        flagTransaction(c);
        if (deny_write_type == DISK_ERROR_TYPE_RDB)
            addReply(c, shared.bgsaveerr);
        else
            addReplySds(c,
                sdscatprintf(sdsempty(),
                "-MISCONF Errors writing to the AOF file: %s\r\n",
                strerror(g_pserver->aof_last_write_errno)));
        return C_OK;
    }

    /* Don't accept write commands if there are not enough good slaves and
     * user configured the min-slaves-to-write option. */
    if (listLength(g_pserver->masters) == 0 &&
        g_pserver->repl_min_slaves_to_write &&
        g_pserver->repl_min_slaves_max_lag &&
        c->cmd->flags & CMD_WRITE &&
        g_pserver->repl_good_slaves_count < g_pserver->repl_min_slaves_to_write)
    {
        flagTransaction(c);
        addReply(c, shared.noreplicaserr);
        return C_OK;
    }

    /* Don't accept write commands if this is a read only replica. But
     * accept write commands if this is our master. */
    if (listLength(g_pserver->masters) && g_pserver->repl_slave_ro &&
        !(c->flags & CLIENT_MASTER) &&
        c->cmd->flags & CMD_WRITE)
    {
        addReply(c, shared.roslaveerr);
        return C_OK;
    }

    /* Only allow a subset of commands in the context of Pub/Sub if the
     * connection is in RESP2 mode. With RESP3 there are no limits. */
    if ((c->flags & CLIENT_PUBSUB && c->resp == 2) &&
        c->cmd->proc != pingCommand &&
        c->cmd->proc != subscribeCommand &&
        c->cmd->proc != unsubscribeCommand &&
        c->cmd->proc != psubscribeCommand &&
        c->cmd->proc != punsubscribeCommand) {
        addReplyError(c,"only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT allowed in this context");
        return C_OK;
    }

    /* Only allow commands with flag "t", such as INFO, SLAVEOF and so on,
     * when replica-serve-stale-data is no and we are a replica with a broken
     * link with master. */
    if (FBrokenLinkToMaster() &&
        g_pserver->repl_serve_stale_data == 0 &&
        !(c->cmd->flags & CMD_STALE))
    {
        flagTransaction(c);
        addReply(c, shared.masterdownerr);
        return C_OK;
    }

    /* Loading DB? Return an error if the command has not the
     * CMD_LOADING flag. */
    if (g_pserver->loading && !(c->cmd->flags & CMD_LOADING)) {
        addReply(c, shared.loadingerr);
        return C_OK;
    }

    /* Lua script too slow? Only allow a limited number of commands. */
    if (g_pserver->lua_timedout &&
          c->cmd->proc != authCommand &&
          c->cmd->proc != helloCommand &&
          c->cmd->proc != replconfCommand &&
        !(c->cmd->proc == shutdownCommand &&
          c->argc == 2 &&
          tolower(((char*)ptrFromObj(c->argv[1]))[0]) == 'n') &&
        !(c->cmd->proc == scriptCommand &&
          c->argc == 2 &&
          tolower(((char*)ptrFromObj(c->argv[1]))[0]) == 'k'))
    {
        flagTransaction(c);
        addReply(c, shared.slowscripterr);
        return C_OK;
    }

    /* Exec the command */
    if (c->flags & CLIENT_MULTI &&
        c->cmd->proc != execCommand && c->cmd->proc != discardCommand &&
        c->cmd->proc != multiCommand && c->cmd->proc != watchCommand)
    {
        queueMultiCommand(c);
        addReply(c,shared.queued);
    } else {
        call(c,callFlags);
        c->woff = g_pserver->master_repl_offset;
        if (listLength(g_pserver->ready_keys))
            handleClientsBlockedOnKeys();
    }
    return C_OK;
}

/*================================== Shutdown =============================== */

/* Close listening sockets. Also unlink the unix domain socket if
 * unlink_unix_socket is non-zero. */
void closeListeningSockets(int unlink_unix_socket) {
    int j;

    for (int iel = 0; iel < cserver.cthreads; ++iel)
    {
        for (j = 0; j < g_pserver->rgthreadvar[iel].ipfd_count; j++) 
            close(g_pserver->rgthreadvar[iel].ipfd[j]);
    }
    if (g_pserver->sofd != -1) close(g_pserver->sofd);
    if (g_pserver->cluster_enabled)
        for (j = 0; j < g_pserver->cfd_count; j++) close(g_pserver->cfd[j]);
    if (unlink_unix_socket && g_pserver->unixsocket) {
        serverLog(LL_NOTICE,"Removing the unix socket file.");
        unlink(g_pserver->unixsocket); /* don't care if this fails */
    }
}

int prepareForShutdown(int flags) {
    int save = flags & SHUTDOWN_SAVE;
    int nosave = flags & SHUTDOWN_NOSAVE;

    serverLog(LL_WARNING,"User requested shutdown...");

    /* Kill all the Lua debugger forked sessions. */
    ldbKillForkedSessions();

    /* Kill the saving child if there is a background saving in progress.
       We want to avoid race conditions, for instance our saving child may
       overwrite the synchronous saving did by SHUTDOWN. */
    if (g_pserver->FRdbSaveInProgress()) {
        serverLog(LL_WARNING,"There is a child saving an .rdb. Killing it!");
        killRDBChild();
    }

    if (g_pserver->aof_state != AOF_OFF) {
        /* Kill the AOF saving child as the AOF we already have may be longer
         * but contains the full dataset anyway. */
        if (g_pserver->aof_child_pid != -1) {
            /* If we have AOF enabled but haven't written the AOF yet, don't
             * shutdown or else the dataset will be lost. */
            if (g_pserver->aof_state == AOF_WAIT_REWRITE) {
                serverLog(LL_WARNING, "Writing initial AOF, can't exit.");
                return C_ERR;
            }
            serverLog(LL_WARNING,
                "There is a child rewriting the AOF. Killing it!");
            killAppendOnlyChild();
        }
        /* Append only file: flush buffers and fsync() the AOF at exit */
        serverLog(LL_NOTICE,"Calling fsync() on the AOF file.");
        flushAppendOnlyFile(1);
        redis_fsync(g_pserver->aof_fd);
    }

    /* Create a new RDB file before exiting. */
    if ((g_pserver->saveparamslen > 0 && !nosave) || save) {
        serverLog(LL_NOTICE,"Saving the final RDB snapshot before exiting.");
        /* Snapshotting. Perform a SYNC SAVE and exit */
        rdbSaveInfo rsi, *rsiptr;
        rsiptr = rdbPopulateSaveInfo(&rsi);
        if (rdbSave(nullptr, rsiptr) != C_OK) {
            /* Ooops.. error saving! The best we can do is to continue
             * operating. Note that if there was a background saving process,
             * in the next cron() Redis will be notified that the background
             * saving aborted, handling special stuff like slaves pending for
             * synchronization... */
            serverLog(LL_WARNING,"Error trying to save the DB, can't exit.");
            return C_ERR;
        }
    }

    /* Remove the pid file if possible and needed. */
    if (cserver.daemonize || cserver.pidfile) {
        serverLog(LL_NOTICE,"Removing the pid file.");
        unlink(cserver.pidfile);
    }

    /* Best effort flush of replica output buffers, so that we hopefully
     * send them pending writes. */
    flushSlavesOutputBuffers();

    /* Close the listening sockets. Apparently this allows faster restarts. */
    closeListeningSockets(1);

    /* free our databases */
    for (int idb = 0; idb < cserver.dbnum; ++idb) {
        delete g_pserver->db[idb];
        g_pserver->db[idb] = nullptr;
    }
    delete g_pserver->m_pstorageFactory;

    serverLog(LL_WARNING,"%s is now ready to exit, bye bye...",
        g_pserver->sentinel_mode ? "Sentinel" : "KeyDB");
    return C_OK;
}

/*================================== Commands =============================== */

/* Sometimes Redis cannot accept write commands because there is a perstence
 * error with the RDB or AOF file, and Redis is configured in order to stop
 * accepting writes in such situation. This function returns if such a
 * condition is active, and the type of the condition.
 *
 * Function return values:
 *
 * DISK_ERROR_TYPE_NONE:    No problems, we can accept writes.
 * DISK_ERROR_TYPE_AOF:     Don't accept writes: AOF errors.
 * DISK_ERROR_TYPE_RDB:     Don't accept writes: RDB errors.
 */
int writeCommandsDeniedByDiskError(void) {
    if (g_pserver->stop_writes_on_bgsave_err &&
        g_pserver->saveparamslen > 0 &&
        g_pserver->lastbgsave_status == C_ERR)
    {
        return DISK_ERROR_TYPE_RDB;
    } else if (g_pserver->aof_state != AOF_OFF &&
               g_pserver->aof_last_write_status == C_ERR)
    {
        return DISK_ERROR_TYPE_AOF;
    } else {
        return DISK_ERROR_TYPE_NONE;
    }
}

/* The PING command. It works in a different way if the client is in
 * in Pub/Sub mode. */
void pingCommand(client *c) {
    /* The command takes zero or one arguments. */
    if (c->argc > 2) {
        addReplyErrorFormat(c,"wrong number of arguments for '%s' command",
            c->cmd->name);
        return;
    }

    if (c->flags & CLIENT_PUBSUB && c->resp == 2) {
        addReply(c,shared.mbulkhdr[2]);
        addReplyBulkCBuffer(c,"pong",4);
        if (c->argc == 1)
            addReplyBulkCBuffer(c,"",0);
        else
            addReplyBulk(c,c->argv[1]);
    } else {
        if (c->argc == 1)
            addReply(c,shared.pong);
        else
            addReplyBulk(c,c->argv[1]);
    }
}

void echoCommand(client *c) {
    addReplyBulk(c,c->argv[1]);
}

void timeCommand(client *c) {
    struct timeval tv;

    /* gettimeofday() can only fail if &tv is a bad address so we
     * don't check for errors. */
    gettimeofday(&tv,NULL);
    addReplyArrayLen(c,2);
    addReplyBulkLongLong(c,tv.tv_sec);
    addReplyBulkLongLong(c,tv.tv_usec);
}

/* Helper function for addReplyCommand() to output flags. */
int addReplyCommandFlag(client *c, struct redisCommand *cmd, int f, const char *reply) {
    if (cmd->flags & f) {
        addReplyStatus(c, reply);
        return 1;
    }
    return 0;
}

/* Output the representation of a Redis command. Used by the COMMAND command. */
void addReplyCommand(client *c, struct redisCommand *cmd) {
    if (!cmd) {
        addReplyNull(c);
    } else {
        /* We are adding: command name, arg count, flags, first, last, offset, categories */
        addReplyArrayLen(c, 7);
        addReplyBulkCString(c, cmd->name);
        addReplyLongLong(c, cmd->arity);

        int flagcount = 0;
        void *flaglen = addReplyDeferredLen(c);
        flagcount += addReplyCommandFlag(c,cmd,CMD_WRITE, "write");
        flagcount += addReplyCommandFlag(c,cmd,CMD_READONLY, "readonly");
        flagcount += addReplyCommandFlag(c,cmd,CMD_DENYOOM, "denyoom");
        flagcount += addReplyCommandFlag(c,cmd,CMD_ADMIN, "admin");
        flagcount += addReplyCommandFlag(c,cmd,CMD_PUBSUB, "pubsub");
        flagcount += addReplyCommandFlag(c,cmd,CMD_NOSCRIPT, "noscript");
        flagcount += addReplyCommandFlag(c,cmd,CMD_RANDOM, "random");
        flagcount += addReplyCommandFlag(c,cmd,CMD_SORT_FOR_SCRIPT,"sort_for_script");
        flagcount += addReplyCommandFlag(c,cmd,CMD_LOADING, "loading");
        flagcount += addReplyCommandFlag(c,cmd,CMD_STALE, "stale");
        flagcount += addReplyCommandFlag(c,cmd,CMD_SKIP_MONITOR, "skip_monitor");
        flagcount += addReplyCommandFlag(c,cmd,CMD_ASKING, "asking");
        flagcount += addReplyCommandFlag(c,cmd,CMD_FAST, "fast");
        if ((cmd->getkeys_proc && !(cmd->flags & CMD_MODULE)) ||
            cmd->flags & CMD_MODULE_GETKEYS)
        {
            addReplyStatus(c, "movablekeys");
            flagcount += 1;
        }
        setDeferredSetLen(c, flaglen, flagcount);

        addReplyLongLong(c, cmd->firstkey);
        addReplyLongLong(c, cmd->lastkey);
        addReplyLongLong(c, cmd->keystep);

        addReplyCommandCategories(c,cmd);
    }
}

/* COMMAND <subcommand> <args> */
void commandCommand(client *c) {
    dictIterator *di;
    dictEntry *de;

    if (c->argc == 2 && !strcasecmp((const char*)ptrFromObj(c->argv[1]),"help")) {
        const char *help[] = {
"(no subcommand) -- Return details about all Redis commands.",
"COUNT -- Return the total number of commands in this Redis g_pserver->",
"GETKEYS <full-command> -- Return the keys from a full Redis command.",
"INFO [command-name ...] -- Return details about multiple Redis commands.",
NULL
        };
        addReplyHelp(c, help);
    } else if (c->argc == 1) {
        addReplyArrayLen(c, dictSize(g_pserver->commands));
        di = dictGetIterator(g_pserver->commands);
        while ((de = dictNext(di)) != NULL) {
            addReplyCommand(c, (redisCommand*)dictGetVal(de));
        }
        dictReleaseIterator(di);
    } else if (!strcasecmp((const char*)ptrFromObj(c->argv[1]), "info")) {
        int i;
        addReplyArrayLen(c, c->argc-2);
        for (i = 2; i < c->argc; i++) {
            addReplyCommand(c, (redisCommand*)dictFetchValue(g_pserver->commands, ptrFromObj(c->argv[i])));
        }
    } else if (!strcasecmp((const char*)ptrFromObj(c->argv[1]), "count") && c->argc == 2) {
        addReplyLongLong(c, dictSize(g_pserver->commands));
    } else if (!strcasecmp((const char*)ptrFromObj(c->argv[1]),"getkeys") && c->argc >= 3) {
        struct redisCommand *cmd = (redisCommand*)lookupCommand((sds)ptrFromObj(c->argv[2]));
        int *keys, numkeys, j;

        if (!cmd) {
            addReplyError(c,"Invalid command specified");
            return;
        } else if (cmd->getkeys_proc == NULL && cmd->firstkey == 0) {
            addReplyError(c,"The command has no key arguments");
            return;
        } else if ((cmd->arity > 0 && cmd->arity != c->argc-2) ||
                   ((c->argc-2) < -cmd->arity))
        {
            addReplyError(c,"Invalid number of arguments specified for command");
            return;
        }

        keys = getKeysFromCommand(cmd,c->argv+2,c->argc-2,&numkeys);
        if (!keys) {
            addReplyError(c,"Invalid arguments specified for command");
        } else {
            addReplyArrayLen(c,numkeys);
            for (j = 0; j < numkeys; j++) addReplyBulk(c,c->argv[keys[j]+2]);
            getKeysFreeResult(keys);
        }
    } else {
        addReplySubcommandSyntaxError(c);
    }
}

/* Convert an amount of bytes into a human readable string in the form
 * of 100B, 2G, 100M, 4K, and so forth. */
void bytesToHuman(char *s, unsigned long long n) {
    double d;

    if (n < 1024) {
        /* Bytes */
        sprintf(s,"%lluB",n);
    } else if (n < (1024*1024)) {
        d = (double)n/(1024);
        sprintf(s,"%.2fK",d);
    } else if (n < (1024LL*1024*1024)) {
        d = (double)n/(1024*1024);
        sprintf(s,"%.2fM",d);
    } else if (n < (1024LL*1024*1024*1024)) {
        d = (double)n/(1024LL*1024*1024);
        sprintf(s,"%.2fG",d);
    } else if (n < (1024LL*1024*1024*1024*1024)) {
        d = (double)n/(1024LL*1024*1024*1024);
        sprintf(s,"%.2fT",d);
    } else if (n < (1024LL*1024*1024*1024*1024*1024)) {
        d = (double)n/(1024LL*1024*1024*1024*1024);
        sprintf(s,"%.2fP",d);
    } else {
        /* Let's hope we never need this */
        sprintf(s,"%lluB",n);
    }
}

/* Create the string returned by the INFO command. This is decoupled
 * by the INFO command itself as we need to report the same information
 * on memory corruption problems. */
sds genRedisInfoString(const char *section) {
    sds info = sdsempty();
    time_t uptime = g_pserver->unixtime-cserver.stat_starttime;
    int j;
    struct rusage self_ru, c_ru;
    int allsections = 0, defsections = 0;
    int sections = 0;

    if (section == NULL) section = "default";
    allsections = strcasecmp(section,"all") == 0;
    defsections = strcasecmp(section,"default") == 0;

    getrusage(RUSAGE_SELF, &self_ru);
    getrusage(RUSAGE_CHILDREN, &c_ru);

    /* Server */
    if (allsections || defsections || !strcasecmp(section,"server")) {
        static int call_uname = 1;
        static struct utsname name;
        const char *mode;

        if (g_pserver->cluster_enabled) mode = "cluster";
        else if (g_pserver->sentinel_mode) mode = "sentinel";
        else mode = "standalone";

        if (sections++) info = sdscat(info,"\r\n");

        if (call_uname) {
            /* Uname can be slow and is always the same output. Cache it. */
            uname(&name);
            call_uname = 0;
        }

        unsigned int lruclock = g_pserver->lruclock.load();
        info = sdscatprintf(info,
            "# Server\r\n"
            "redis_version:%s\r\n"
            "redis_git_sha1:%s\r\n"
            "redis_git_dirty:%d\r\n"
            "redis_build_id:%llx\r\n"
            "redis_mode:%s\r\n"
            "os:%s %s %s\r\n"
            "arch_bits:%d\r\n"
            "multiplexing_api:%s\r\n"
            "atomicvar_api:%s\r\n"
            "gcc_version:%d.%d.%d\r\n"
            "process_id:%ld\r\n"
            "run_id:%s\r\n"
            "tcp_port:%d\r\n"
            "uptime_in_seconds:%jd\r\n"
            "uptime_in_days:%jd\r\n"
            "hz:%d\r\n"
            "configured_hz:%d\r\n"
            "lru_clock:%ld\r\n"
            "executable:%s\r\n"
            "config_file:%s\r\n",
            KEYDB_SET_VERSION,
            redisGitSHA1(),
            strtol(redisGitDirty(),NULL,10) > 0,
            (unsigned long long) redisBuildId(),
            mode,
            name.sysname, name.release, name.machine,
            (int)sizeof(void*)*8,
            aeGetApiName(),
            REDIS_ATOMIC_API,
#ifdef __GNUC__
            __GNUC__,__GNUC_MINOR__,__GNUC_PATCHLEVEL__,
#else
            0,0,0,
#endif
            (long) getpid(),
            g_pserver->runid,
            g_pserver->port,
            (intmax_t)uptime,
            (intmax_t)(uptime/(3600*24)),
            g_pserver->hz.load(),
            g_pserver->config_hz,
            (unsigned long) lruclock,
            cserver.executable ? cserver.executable : "",
            cserver.configfile ? cserver.configfile : "");
    }

    /* Clients */
    if (allsections || defsections || !strcasecmp(section,"clients")) {
        size_t maxin, maxout;
        getExpansiveClientsInfo(&maxin,&maxout);
        if (sections++) info = sdscat(info,"\r\n");
        info = sdscatprintf(info,
            "# Clients\r\n"
            "connected_clients:%lu\r\n"
            "client_recent_max_input_buffer:%zu\r\n"
            "client_recent_max_output_buffer:%zu\r\n"
            "blocked_clients:%d\r\n"
            "current_client_thread:%d\r\n",
            listLength(g_pserver->clients)-listLength(g_pserver->slaves),
            maxin, maxout,
            g_pserver->blocked_clients,
            static_cast<int>(serverTL - g_pserver->rgthreadvar));
        for (int ithread = 0; ithread < cserver.cthreads; ++ithread)
        {
            info = sdscatprintf(info,
                "thread_%d_clients:%d\r\n",
                ithread, g_pserver->rgthreadvar[ithread].cclients);
        }
    }

    /* Memory */
    if (allsections || defsections || !strcasecmp(section,"memory")) {
        char hmem[64];
        char peak_hmem[64];
        char total_system_hmem[64];
        char used_memory_lua_hmem[64];
        char used_memory_scripts_hmem[64];
        char used_memory_rss_hmem[64];
        char maxmemory_hmem[64];
        size_t zmalloc_used = zmalloc_used_memory();
        size_t total_system_mem = cserver.system_memory_size;
        const char *evict_policy = evictPolicyToString();
        long long memory_lua = (long long)lua_gc(g_pserver->lua,LUA_GCCOUNT,0)*1024;
        struct redisMemOverhead *mh = getMemoryOverheadData();

        /* Peak memory is updated from time to time by serverCron() so it
         * may happen that the instantaneous value is slightly bigger than
         * the peak value. This may confuse users, so we update the peak
         * if found smaller than the current memory usage. */
        if (zmalloc_used > g_pserver->stat_peak_memory)
            g_pserver->stat_peak_memory = zmalloc_used;

        bytesToHuman(hmem,zmalloc_used);
        bytesToHuman(peak_hmem,g_pserver->stat_peak_memory);
        bytesToHuman(total_system_hmem,total_system_mem);
        bytesToHuman(used_memory_lua_hmem,memory_lua);
        bytesToHuman(used_memory_scripts_hmem,mh->lua_caches);
        bytesToHuman(used_memory_rss_hmem,g_pserver->cron_malloc_stats.process_rss);
        bytesToHuman(maxmemory_hmem,g_pserver->maxmemory);

        if (sections++) info = sdscat(info,"\r\n");
        info = sdscatprintf(info,
            "# Memory\r\n"
            "used_memory:%zu\r\n"
            "used_memory_human:%s\r\n"
            "used_memory_rss:%zu\r\n"
            "used_memory_rss_human:%s\r\n"
            "used_memory_peak:%zu\r\n"
            "used_memory_peak_human:%s\r\n"
            "used_memory_peak_perc:%.2f%%\r\n"
            "used_memory_overhead:%zu\r\n"
            "used_memory_startup:%zu\r\n"
            "used_memory_dataset:%zu\r\n"
            "used_memory_dataset_perc:%.2f%%\r\n"
            "allocator_allocated:%zu\r\n"
            "allocator_active:%zu\r\n"
            "allocator_resident:%zu\r\n"
            "total_system_memory:%lu\r\n"
            "total_system_memory_human:%s\r\n"
            "used_memory_lua:%lld\r\n"
            "used_memory_lua_human:%s\r\n"
            "used_memory_scripts:%lld\r\n"
            "used_memory_scripts_human:%s\r\n"
            "number_of_cached_scripts:%lu\r\n"
            "maxmemory:%lld\r\n"
            "maxmemory_human:%s\r\n"
            "maxmemory_policy:%s\r\n"
            "allocator_frag_ratio:%.2f\r\n"
            "allocator_frag_bytes:%zu\r\n"
            "allocator_rss_ratio:%.2f\r\n"
            "allocator_rss_bytes:%zd\r\n"
            "rss_overhead_ratio:%.2f\r\n"
            "rss_overhead_bytes:%zd\r\n"
            "mem_fragmentation_ratio:%.2f\r\n"
            "mem_fragmentation_bytes:%zd\r\n"
            "mem_not_counted_for_evict:%zu\r\n"
            "mem_replication_backlog:%zu\r\n"
            "mem_clients_slaves:%zu\r\n"
            "mem_clients_normal:%zu\r\n"
            "mem_aof_buffer:%zu\r\n"
            "mem_allocator:%s\r\n"
            "active_defrag_running:%d\r\n"
            "lazyfree_pending_objects:%zu\r\n"
            "storage_provider:%s\r\n",
            zmalloc_used,
            hmem,
            g_pserver->cron_malloc_stats.process_rss,
            used_memory_rss_hmem,
            g_pserver->stat_peak_memory,
            peak_hmem,
            mh->peak_perc,
            mh->overhead_total,
            mh->startup_allocated,
            mh->dataset,
            mh->dataset_perc,
            g_pserver->cron_malloc_stats.allocator_allocated,
            g_pserver->cron_malloc_stats.allocator_active,
            g_pserver->cron_malloc_stats.allocator_resident,
            (unsigned long)total_system_mem,
            total_system_hmem,
            memory_lua,
            used_memory_lua_hmem,
            (long long) mh->lua_caches,
            used_memory_scripts_hmem,
            dictSize(g_pserver->lua_scripts),
            g_pserver->maxmemory,
            maxmemory_hmem,
            evict_policy,
            mh->allocator_frag,
            mh->allocator_frag_bytes,
            mh->allocator_rss,
            mh->allocator_rss_bytes,
            mh->rss_extra,
            mh->rss_extra_bytes,
            mh->total_frag, /* this is the total RSS overhead, including fragmentation, */
            mh->total_frag_bytes, /* named so for backwards compatibility */
            freeMemoryGetNotCountedMemory(),
            mh->repl_backlog,
            mh->clients_slaves,
            mh->clients_normal,
            mh->aof_buffer,
            ZMALLOC_LIB,
            g_pserver->active_defrag_running,
            lazyfreeGetPendingObjectsCount(),
            g_pserver->m_pstorageFactory ? g_pserver->m_pstorageFactory->name() : "none"
        );
        freeMemoryOverheadData(mh);
    }

    /* Persistence */
    if (allsections || defsections || !strcasecmp(section,"persistence")) {
        if (sections++) info = sdscat(info,"\r\n");
        info = sdscatprintf(info,
            "# Persistence\r\n"
            "loading:%d\r\n"
            "rdb_changes_since_last_save:%lld\r\n"
            "rdb_bgsave_in_progress:%d\r\n"
            "rdb_last_save_time:%jd\r\n"
            "rdb_last_bgsave_status:%s\r\n"
            "rdb_last_bgsave_time_sec:%jd\r\n"
            "rdb_current_bgsave_time_sec:%jd\r\n"
            "rdb_last_cow_size:%zu\r\n"
            "aof_enabled:%d\r\n"
            "aof_rewrite_in_progress:%d\r\n"
            "aof_rewrite_scheduled:%d\r\n"
            "aof_last_rewrite_time_sec:%jd\r\n"
            "aof_current_rewrite_time_sec:%jd\r\n"
            "aof_last_bgrewrite_status:%s\r\n"
            "aof_last_write_status:%s\r\n"
            "aof_last_cow_size:%zu\r\n",
            g_pserver->loading,
            g_pserver->dirty,
            g_pserver->FRdbSaveInProgress(),
            (intmax_t)g_pserver->lastsave,
            (g_pserver->lastbgsave_status == C_OK) ? "ok" : "err",
            (intmax_t)g_pserver->rdb_save_time_last,
            (intmax_t)(g_pserver->FRdbSaveInProgress() ?
                time(NULL)-g_pserver->rdb_save_time_start : -1),
            g_pserver->stat_rdb_cow_bytes,
            g_pserver->aof_state != AOF_OFF,
            g_pserver->aof_child_pid != -1,
            g_pserver->aof_rewrite_scheduled,
            (intmax_t)g_pserver->aof_rewrite_time_last,
            (intmax_t)((g_pserver->aof_child_pid == -1) ?
                -1 : time(NULL)-g_pserver->aof_rewrite_time_start),
            (g_pserver->aof_lastbgrewrite_status == C_OK) ? "ok" : "err",
            (g_pserver->aof_last_write_status == C_OK) ? "ok" : "err",
            g_pserver->stat_aof_cow_bytes);

        if (g_pserver->aof_state != AOF_OFF) {
            info = sdscatprintf(info,
                "aof_current_size:%lld\r\n"
                "aof_base_size:%lld\r\n"
                "aof_pending_rewrite:%d\r\n"
                "aof_buffer_length:%zu\r\n"
                "aof_rewrite_buffer_length:%lu\r\n"
                "aof_pending_bio_fsync:%llu\r\n"
                "aof_delayed_fsync:%lu\r\n",
                (long long) g_pserver->aof_current_size,
                (long long) g_pserver->aof_rewrite_base_size,
                g_pserver->aof_rewrite_scheduled,
                sdslen(g_pserver->aof_buf),
                aofRewriteBufferSize(),
                bioPendingJobsOfType(BIO_AOF_FSYNC),
                g_pserver->aof_delayed_fsync);
        }

        if (g_pserver->loading) {
            double perc;
            time_t eta, elapsed;
            off_t remaining_bytes = g_pserver->loading_total_bytes-
                                    g_pserver->loading_loaded_bytes;

            perc = ((double)g_pserver->loading_loaded_bytes /
                   (g_pserver->loading_total_bytes+1)) * 100;

            elapsed = time(NULL)-g_pserver->loading_start_time;
            if (elapsed == 0) {
                eta = 1; /* A fake 1 second figure if we don't have
                            enough info */
            } else {
                eta = (elapsed*remaining_bytes)/(g_pserver->loading_loaded_bytes+1);
            }

            info = sdscatprintf(info,
                "loading_start_time:%jd\r\n"
                "loading_total_bytes:%llu\r\n"
                "loading_loaded_bytes:%llu\r\n"
                "loading_loaded_perc:%.2f\r\n"
                "loading_eta_seconds:%jd\r\n",
                (intmax_t) g_pserver->loading_start_time,
                (unsigned long long) g_pserver->loading_total_bytes,
                (unsigned long long) g_pserver->loading_loaded_bytes,
                perc,
                (intmax_t)eta
            );
        }
    }

    /* Stats */
    if (allsections || defsections || !strcasecmp(section,"stats")) {
        if (sections++) info = sdscat(info,"\r\n");
        info = sdscatprintf(info,
            "# Stats\r\n"
            "total_connections_received:%lld\r\n"
            "total_commands_processed:%lld\r\n"
            "instantaneous_ops_per_sec:%lld\r\n"
            "total_net_input_bytes:%lld\r\n"
            "total_net_output_bytes:%lld\r\n"
            "instantaneous_input_kbps:%.2f\r\n"
            "instantaneous_output_kbps:%.2f\r\n"
            "rejected_connections:%lld\r\n"
            "sync_full:%lld\r\n"
            "sync_partial_ok:%lld\r\n"
            "sync_partial_err:%lld\r\n"
            "expired_keys:%lld\r\n"
            "expired_stale_perc:%.2f\r\n"
            "expired_time_cap_reached_count:%lld\r\n"
            "evicted_keys:%lld\r\n"
            "keyspace_hits:%lld\r\n"
            "keyspace_misses:%lld\r\n"
            "pubsub_channels:%ld\r\n"
            "pubsub_patterns:%lu\r\n"
            "latest_fork_usec:%lld\r\n"
            "migrate_cached_sockets:%ld\r\n"
            "slave_expires_tracked_keys:%zu\r\n"
            "active_defrag_hits:%lld\r\n"
            "active_defrag_misses:%lld\r\n"
            "active_defrag_key_hits:%lld\r\n"
            "active_defrag_key_misses:%lld\r\n",
            g_pserver->stat_numconnections,
            g_pserver->stat_numcommands,
            getInstantaneousMetric(STATS_METRIC_COMMAND),
            g_pserver->stat_net_input_bytes.load(),
            g_pserver->stat_net_output_bytes.load(),
            (float)getInstantaneousMetric(STATS_METRIC_NET_INPUT)/1024,
            (float)getInstantaneousMetric(STATS_METRIC_NET_OUTPUT)/1024,
            g_pserver->stat_rejected_conn,
            g_pserver->stat_sync_full,
            g_pserver->stat_sync_partial_ok,
            g_pserver->stat_sync_partial_err,
            g_pserver->stat_expiredkeys,
            g_pserver->stat_expired_stale_perc*100,
            g_pserver->stat_expired_time_cap_reached_count,
            g_pserver->stat_evictedkeys,
            g_pserver->stat_keyspace_hits,
            g_pserver->stat_keyspace_misses,
            dictSize(g_pserver->pubsub_channels),
            listLength(g_pserver->pubsub_patterns),
            g_pserver->stat_fork_time,
            dictSize(g_pserver->migrate_cached_sockets),
            getSlaveKeyWithExpireCount(),
            g_pserver->stat_active_defrag_hits,
            g_pserver->stat_active_defrag_misses,
            g_pserver->stat_active_defrag_key_hits,
            g_pserver->stat_active_defrag_key_misses);
    }

    /* Replication */
    if (allsections || defsections || !strcasecmp(section,"replication")) {
        if (sections++) info = sdscat(info,"\r\n");
        info = sdscatprintf(info,
            "# Replication\r\n"
            "role:%s\r\n",
            listLength(g_pserver->masters) == 0 ? "master" 
                : g_pserver->fActiveReplica ? "active-replica" : "slave");
        if (listLength(g_pserver->masters)) {
            listIter li;
            listNode *ln;
            listRewind(g_pserver->masters, &li);

            int cmasters = 0;
            while ((ln = listNext(&li)))
            {
                long long slave_repl_offset = 1;
                redisMaster *mi = (redisMaster*)listNodeValue(ln);
                info = sdscatprintf(info, "Master %d: \r\n", cmasters);
                ++cmasters;

                if (mi->master)
                    slave_repl_offset = mi->master->reploff;
                else if (mi->cached_master)
                    slave_repl_offset = mi->cached_master->reploff;

                info = sdscatprintf(info,
                    "master_host:%s\r\n"
                    "master_port:%d\r\n"
                    "master_link_status:%s\r\n"
                    "master_last_io_seconds_ago:%d\r\n"
                    "master_sync_in_progress:%d\r\n"
                    "slave_repl_offset:%lld\r\n"
                    ,mi->masterhost,
                    mi->masterport,
                    (mi->repl_state == REPL_STATE_CONNECTED) ?
                        "up" : "down",
                    mi->master ?
                    ((int)(g_pserver->unixtime-mi->master->lastinteraction)) : -1,
                    mi->repl_state == REPL_STATE_TRANSFER,
                    slave_repl_offset
                );

                if (mi->repl_state == REPL_STATE_TRANSFER) {
                    info = sdscatprintf(info,
                        "master_sync_left_bytes:%lld\r\n"
                        "master_sync_last_io_seconds_ago:%d\r\n"
                        , (long long)
                            (mi->repl_transfer_size - mi->repl_transfer_read),
                        (int)(g_pserver->unixtime-mi->repl_transfer_lastio)
                    );
                }

                if (mi->repl_state != REPL_STATE_CONNECTED) {
                    info = sdscatprintf(info,
                        "master_link_down_since_seconds:%jd\r\n",
                        (intmax_t)g_pserver->unixtime-mi->repl_down_since);
                }
            }
            info = sdscatprintf(info,
                "slave_priority:%d\r\n"
                "slave_read_only:%d\r\n",
                g_pserver->slave_priority,
                g_pserver->repl_slave_ro);
        }

        info = sdscatprintf(info,
            "connected_slaves:%lu\r\n",
            listLength(g_pserver->slaves));

        /* If min-slaves-to-write is active, write the number of slaves
         * currently considered 'good'. */
        if (g_pserver->repl_min_slaves_to_write &&
            g_pserver->repl_min_slaves_max_lag) {
            info = sdscatprintf(info,
                "min_slaves_good_slaves:%d\r\n",
                g_pserver->repl_good_slaves_count);
        }

        if (listLength(g_pserver->slaves)) {
            int slaveid = 0;
            listNode *ln;
            listIter li;

            listRewind(g_pserver->slaves,&li);
            while((ln = listNext(&li))) {
                client *replica = (client*)listNodeValue(ln);
                const char *state = NULL;
                char ip[NET_IP_STR_LEN], *slaveip = replica->slave_ip;
                int port;
                long lag = 0;

                if (slaveip[0] == '\0') {
                    if (anetPeerToString(replica->fd,ip,sizeof(ip),&port) == -1)
                        continue;
                    slaveip = ip;
                }
                switch(replica->replstate) {
                case SLAVE_STATE_WAIT_BGSAVE_START:
                case SLAVE_STATE_WAIT_BGSAVE_END:
                    state = "wait_bgsave";
                    break;
                case SLAVE_STATE_SEND_BULK:
                    state = "send_bulk";
                    break;
                case SLAVE_STATE_ONLINE:
                    state = "online";
                    break;
                }
                if (state == NULL) continue;
                if (replica->replstate == SLAVE_STATE_ONLINE)
                    lag = time(NULL) - replica->repl_ack_time;

                info = sdscatprintf(info,
                    "slave%d:ip=%s,port=%d,state=%s,"
                    "offset=%lld,lag=%ld\r\n",
                    slaveid,slaveip,replica->slave_listening_port,state,
                    (replica->repl_ack_off + replica->reploff_skipped), lag);
                slaveid++;
            }
        }
        info = sdscatprintf(info,
            "master_replid:%s\r\n"
            "master_replid2:%s\r\n"
            "master_repl_offset:%lld\r\n"
            "second_repl_offset:%lld\r\n"
            "repl_backlog_active:%d\r\n"
            "repl_backlog_size:%lld\r\n"
            "repl_backlog_first_byte_offset:%lld\r\n"
            "repl_backlog_histlen:%lld\r\n",
            g_pserver->replid,
            g_pserver->replid2,
            g_pserver->master_repl_offset,
            g_pserver->second_replid_offset,
            g_pserver->repl_backlog != NULL,
            g_pserver->repl_backlog_size,
            g_pserver->repl_backlog_off,
            g_pserver->repl_backlog_histlen);
    }

    /* CPU */
    if (allsections || defsections || !strcasecmp(section,"cpu")) {
        if (sections++) info = sdscat(info,"\r\n");
        info = sdscatprintf(info,
        "# CPU\r\n"
        "used_cpu_sys:%ld.%06ld\r\n"
        "used_cpu_user:%ld.%06ld\r\n"
        "used_cpu_sys_children:%ld.%06ld\r\n"
        "used_cpu_user_children:%ld.%06ld\r\n"
        "server_threads:%d\r\n"
        "long_lock_waits:%" PRIu64 "\r\n",
        (long)self_ru.ru_stime.tv_sec, (long)self_ru.ru_stime.tv_usec,
        (long)self_ru.ru_utime.tv_sec, (long)self_ru.ru_utime.tv_usec,
        (long)c_ru.ru_stime.tv_sec, (long)c_ru.ru_stime.tv_usec,
        (long)c_ru.ru_utime.tv_sec, (long)c_ru.ru_utime.tv_usec,
        cserver.cthreads,
        fastlock_getlongwaitcount());
    }

    /* Command statistics */
    if (allsections || !strcasecmp(section,"commandstats")) {
        if (sections++) info = sdscat(info,"\r\n");
        info = sdscatprintf(info, "# Commandstats\r\n");

        struct redisCommand *c;
        dictEntry *de;
        dictIterator *di;
        di = dictGetSafeIterator(g_pserver->commands);
        while((de = dictNext(di)) != NULL) {
            c = (struct redisCommand *) dictGetVal(de);
            if (!c->calls) continue;
            info = sdscatprintf(info,
                "cmdstat_%s:calls=%lld,usec=%lld,usec_per_call=%.2f\r\n",
                c->name, c->calls, c->microseconds,
                (c->calls == 0) ? 0 : ((float)c->microseconds/c->calls));
        }
        dictReleaseIterator(di);
    }

    /* Cluster */
    if (allsections || defsections || !strcasecmp(section,"cluster")) {
        if (sections++) info = sdscat(info,"\r\n");
        info = sdscatprintf(info,
        "# Cluster\r\n"
        "cluster_enabled:%d\r\n",
        g_pserver->cluster_enabled);
    }

    /* Key space */
    if (allsections || defsections || !strcasecmp(section,"keyspace")) {
        if (sections++) info = sdscat(info,"\r\n");
        info = sdscatprintf(info, "# Keyspace\r\n");
        for (j = 0; j < cserver.dbnum; j++) {
            long long keys, vkeys;

            keys = g_pserver->db[j]->size();
            vkeys = g_pserver->db[j]->expireSize();

            // Adjust TTL by the current time
            g_pserver->db[j]->avg_ttl -= (g_pserver->mstime - g_pserver->db[j]->last_expire_set);
            if (g_pserver->db[j]->avg_ttl < 0)
                g_pserver->db[j]->avg_ttl = 0;
            g_pserver->db[j]->last_expire_set = g_pserver->mstime;
            
            if (keys || vkeys) {
                info = sdscatprintf(info,
                    "db%d:keys=%lld,expires=%lld,avg_ttl=%lld\r\n",
                    j, keys, vkeys, static_cast<long long>(g_pserver->db[j]->avg_ttl));
            }
        }
    }
    return info;
}

void infoCommand(client *c) {
    const char *section = c->argc == 2 ? (const char*)ptrFromObj(c->argv[1]) : "default";

    if (c->argc > 2) {
        addReply(c,shared.syntaxerr);
        return;
    }
    addReplyBulkSds(c, genRedisInfoString(section));
}

void monitorCommand(client *c) {
    /* ignore MONITOR if already replica or in monitor mode */
    serverAssert(GlobalLocksAcquired());
    if (c->flags & CLIENT_SLAVE) return;

    c->flags |= (CLIENT_SLAVE|CLIENT_MONITOR);
    listAddNodeTail(g_pserver->monitors,c);
    addReply(c,shared.ok);
}

/* =================================== Main! ================================ */

#ifdef __linux__
int linuxOvercommitMemoryValue(void) {
    FILE *fp = fopen("/proc/sys/vm/overcommit_memory","r");
    char buf[64];

    if (!fp) return -1;
    if (fgets(buf,64,fp) == NULL) {
        fclose(fp);
        return -1;
    }
    fclose(fp);

    return atoi(buf);
}

void linuxMemoryWarnings(void) {
    if (linuxOvercommitMemoryValue() == 0) {
        serverLog(LL_WARNING,"WARNING overcommit_memory is set to 0! Background save may fail under low memory condition. To fix this issue add 'vm.overcommit_memory = 1' to /etc/sysctl.conf and then reboot or run the command 'sysctl vm.overcommit_memory=1' for this to take effect.");
    }
    if (THPIsEnabled()) {
        serverLog(LL_WARNING,"WARNING you have Transparent Huge Pages (THP) support enabled in your kernel. This will create latency and memory usage issues with KeyDB. To fix this issue run the command 'echo never > /sys/kernel/mm/transparent_hugepage/enabled' as root, and add it to your /etc/rc.local in order to retain the setting after a reboot. KeyDB must be restarted after THP is disabled.");
    }
}
#endif /* __linux__ */

void createPidFile(void) {
    /* If pidfile requested, but no pidfile defined, use
     * default pidfile path */
    if (!cserver.pidfile) cserver.pidfile = zstrdup(CONFIG_DEFAULT_PID_FILE);

    /* Try to write the pid file in a best-effort way. */
    FILE *fp = fopen(cserver.pidfile,"w");
    if (fp) {
        fprintf(fp,"%d\n",(int)getpid());
        fclose(fp);
    }
}

void daemonize(void) {
    int fd;

    if (fork() != 0) exit(0); /* parent exits */
    setsid(); /* create a new session */

    /* Every output goes to /dev/null. If Redis is daemonized but
     * the 'logfile' is set to 'stdout' in the configuration file
     * it will not log at all. */
    if ((fd = open("/dev/null", O_RDWR, 0)) != -1) {
        dup2(fd, STDIN_FILENO);
        dup2(fd, STDOUT_FILENO);
        dup2(fd, STDERR_FILENO);
        if (fd > STDERR_FILENO) close(fd);
    }
}

void version(void) {
    printf("Redis server v=%s sha=%s:%d malloc=%s bits=%d build=%llx\n",
        KEYDB_REAL_VERSION,
        redisGitSHA1(),
        atoi(redisGitDirty()) > 0,
        ZMALLOC_LIB,
        sizeof(long) == 4 ? 32 : 64,
        (unsigned long long) redisBuildId());
    exit(0);
}

void usage(void) {
    fprintf(stderr,"Usage: ./keydb-server [/path/to/redis.conf] [options]\n");
    fprintf(stderr,"       ./keydb-server - (read config from stdin)\n");
    fprintf(stderr,"       ./keydb-server -v or --version\n");
    fprintf(stderr,"       ./keydb-server -h or --help\n");
    fprintf(stderr,"       ./keydb-server --test-memory <megabytes>\n\n");
    fprintf(stderr,"Examples:\n");
    fprintf(stderr,"       ./keydb-server (run the server with default conf)\n");
    fprintf(stderr,"       ./keydb-server /etc/redis/6379.conf\n");
    fprintf(stderr,"       ./keydb-server --port 7777\n");
    fprintf(stderr,"       ./keydb-server --port 7777 --replicaof 127.0.0.1 8888\n");
    fprintf(stderr,"       ./keydb-server /etc/myredis.conf --loglevel verbose\n\n");
    fprintf(stderr,"Sentinel mode:\n");
    fprintf(stderr,"       ./keydb-server /etc/sentinel.conf --sentinel\n");
    exit(1);
}

void redisAsciiArt(void) {
#include "asciilogo.h"
    char *buf = (char*)zmalloc(1024*16, MALLOC_LOCAL);
    const char *mode;

    if (g_pserver->cluster_enabled) mode = "cluster";
    else if (g_pserver->sentinel_mode) mode = "sentinel";
    else mode = "standalone";

    /* Show the ASCII logo if: log file is stdout AND stdout is a
     * tty AND syslog logging is disabled. Also show logo if the user
     * forced us to do so via redis.conf. */
    int show_logo = ((!g_pserver->syslog_enabled &&
                      g_pserver->logfile[0] == '\0' &&
                      isatty(fileno(stdout))) ||
                     g_pserver->always_show_logo);

    if (!show_logo) {
        serverLog(LL_NOTICE,
            "Running mode=%s, port=%d.",
            mode, g_pserver->port
        );
    } else {
        snprintf(buf,1024*16,ascii_logo,
            KEYDB_REAL_VERSION,
            redisGitSHA1(),
            strtol(redisGitDirty(),NULL,10) > 0,
            (sizeof(long) == 8) ? "64" : "32",
            mode, g_pserver->port,
            (long) getpid()
        );
        serverLogRaw(LL_NOTICE|LL_RAW,buf);
    }

    if (cserver.license_key == nullptr)
    {
        serverLog(LL_WARNING, "!!!! KeyDB Pro is being run in trial mode  !!!!");
        serverLog(LL_WARNING, "!!!! Execution will terminate in %d minutes !!!!", cserver.trial_timeout);
    }
    zfree(buf);
}

static void sigShutdownHandler(int sig) {
    const char *msg;

    switch (sig) {
    case SIGINT:
        msg = "Received SIGINT scheduling shutdown...";
        break;
    case SIGTERM:
        msg = "Received SIGTERM scheduling shutdown...";
        break;
    default:
        msg = "Received shutdown signal, scheduling shutdown...";
    };

    /* SIGINT is often delivered via Ctrl+C in an interactive session.
     * If we receive the signal the second time, we interpret this as
     * the user really wanting to quit ASAP without waiting to persist
     * on disk. */
    if (g_pserver->shutdown_asap && sig == SIGINT) {
        serverLogFromHandler(LL_WARNING, "You insist... exiting now.");
        rdbRemoveTempFile(g_pserver->rdbThreadVars.tmpfileNum);
        exit(1); /* Exit with an error since this was not a clean shutdown. */
    } else if (g_pserver->loading) {
        serverLogFromHandler(LL_WARNING, "Received shutdown signal during loading, exiting now.");
        exit(0);
    }

    serverLogFromHandler(LL_WARNING, msg);
    g_pserver->shutdown_asap = 1;
}

void setupSignalHandlers(void) {
    struct sigaction act;

    /* When the SA_SIGINFO flag is set in sa_flags then sa_sigaction is used.
     * Otherwise, sa_handler is used. */
    sigemptyset(&act.sa_mask);
    act.sa_flags = 0;
    act.sa_handler = sigShutdownHandler;
    sigaction(SIGTERM, &act, NULL);
    sigaction(SIGINT, &act, NULL);

#ifdef HAVE_BACKTRACE
    sigemptyset(&act.sa_mask);
    act.sa_flags = SA_NODEFER | SA_RESETHAND | SA_SIGINFO;
    act.sa_sigaction = sigsegvHandler;
    sigaction(SIGSEGV, &act, NULL);
    sigaction(SIGBUS, &act, NULL);
    sigaction(SIGFPE, &act, NULL);
    sigaction(SIGILL, &act, NULL);
#endif
    return;
}

extern "C" void memtest(size_t megabytes, int passes);

/* Returns 1 if there is --sentinel among the arguments or if
 * argv[0] contains "keydb-sentinel". */
int checkForSentinelMode(int argc, char **argv) {
    int j;

    if (strstr(argv[0],"keydb-sentinel") != NULL) return 1;
    for (j = 1; j < argc; j++)
        if (!strcmp(argv[j],"--sentinel")) return 1;
    return 0;
}

/* Function called at startup to load RDB or AOF file in memory. */
void loadDataFromDisk(void) {
    long long start = ustime();
    if (g_pserver->aof_state == AOF_ON) {
        if (loadAppendOnlyFile(g_pserver->aof_filename) == C_OK)
            serverLog(LL_NOTICE,"DB loaded from append only file: %.3f seconds",(float)(ustime()-start)/1000000);
    } else if (g_pserver->rdb_filename != NULL || g_pserver->rdb_s3bucketpath != NULL) {
        rdbSaveInfo rsi = RDB_SAVE_INFO_INIT;
        if (rdbLoad(&rsi) == C_OK) {
            serverLog(LL_NOTICE,"DB loaded from disk: %.3f seconds",
                (float)(ustime()-start)/1000000);

            /* Restore the replication ID / offset from the RDB file. */
            if ((listLength(g_pserver->masters) || (g_pserver->cluster_enabled && nodeIsSlave(g_pserver->cluster->myself)))&&
                rsi.repl_id_is_set &&
                rsi.repl_offset != -1 &&
                /* Note that older implementations may save a repl_stream_db
                 * of -1 inside the RDB file in a wrong way, see more information
                 * in function rdbPopulateSaveInfo. */
                rsi.repl_stream_db != -1)
            {
                memcpy(g_pserver->replid,rsi.repl_id,sizeof(g_pserver->replid));
                g_pserver->master_repl_offset = rsi.repl_offset;
                listIter li;
                listNode *ln;
                
                listRewind(g_pserver->masters, &li);
                while ((ln = listNext(&li)))
                {
                    redisMaster *mi = (redisMaster*)listNodeValue(ln);
                    /* If we are a replica, create a cached master from this
                    * information, in order to allow partial resynchronizations
                    * with masters. */
                    replicationCacheMasterUsingMyself(mi);
                    selectDb(mi->cached_master,rsi.repl_stream_db);
                }
            }
        } else if (errno != ENOENT) {
            serverLog(LL_WARNING,"Fatal error loading the DB: %s. Exiting.",strerror(errno));
            exit(1);
        }
    }
}

void redisOutOfMemoryHandler(size_t allocation_size) {
    serverLog(LL_WARNING,"Out Of Memory allocating %zu bytes!",
        allocation_size);
    serverPanic("Redis aborting for OUT OF MEMORY");
}

void fuzzOutOfMemoryHandler(size_t allocation_size) {
    serverLog(LL_WARNING,"Out Of Memory allocating %zu bytes!",
        allocation_size);
    exit(EXIT_FAILURE); // don't crash because it causes false positives
}

void redisSetProcTitle(const char *title) {
#ifdef USE_SETPROCTITLE
    const char *server_mode = "";
    if (g_pserver->cluster_enabled) server_mode = " [cluster]";
    else if (g_pserver->sentinel_mode) server_mode = " [sentinel]";

    setproctitle("%s %s:%d%s",
        title,
        g_pserver->bindaddr_count ? g_pserver->bindaddr[0] : "*",
        g_pserver->port,
        server_mode);
#else
    UNUSED(title);
#endif
}

/*
 * Check whether systemd or upstart have been used to start redis.
 */

int redisSupervisedUpstart(void) {
    const char *upstart_job = getenv("UPSTART_JOB");

    if (!upstart_job) {
        serverLog(LL_WARNING,
                "upstart supervision requested, but UPSTART_JOB not found");
        return 0;
    }

    serverLog(LL_NOTICE, "supervised by upstart, will stop to signal readiness");
    raise(SIGSTOP);
    unsetenv("UPSTART_JOB");
    return 1;
}

int redisSupervisedSystemd(void) {
    const char *notify_socket = getenv("NOTIFY_SOCKET");
    int fd = 1;
    struct sockaddr_un su;
    struct iovec iov;
    struct msghdr hdr;
    int sendto_flags = 0;

    if (!notify_socket) {
        serverLog(LL_WARNING,
                "systemd supervision requested, but NOTIFY_SOCKET not found");
        return 0;
    }

    if ((strchr("@/", notify_socket[0])) == NULL || strlen(notify_socket) < 2) {
        return 0;
    }

    serverLog(LL_NOTICE, "supervised by systemd, will signal readiness");
    if ((fd = socket(AF_UNIX, SOCK_DGRAM, 0)) == -1) {
        serverLog(LL_WARNING,
                "Can't connect to systemd socket %s", notify_socket);
        return 0;
    }

    memset(&su, 0, sizeof(su));
    su.sun_family = AF_UNIX;
    strncpy (su.sun_path, notify_socket, sizeof(su.sun_path) -1);
    su.sun_path[sizeof(su.sun_path) - 1] = '\0';

    if (notify_socket[0] == '@')
        su.sun_path[0] = '\0';

    memset(&iov, 0, sizeof(iov));
    iov.iov_base = (void*)"READY=1";
    iov.iov_len = strlen("READY=1");

    memset(&hdr, 0, sizeof(hdr));
    hdr.msg_name = &su;
    hdr.msg_namelen = offsetof(struct sockaddr_un, sun_path) +
        strlen(notify_socket);
    hdr.msg_iov = &iov;
    hdr.msg_iovlen = 1;

    unsetenv("NOTIFY_SOCKET");
#ifdef HAVE_MSG_NOSIGNAL
    sendto_flags |= MSG_NOSIGNAL;
#endif
    if (sendmsg(fd, &hdr, sendto_flags) < 0) {
        serverLog(LL_WARNING, "Can't send notification to systemd");
        close(fd);
        return 0;
    }
    close(fd);
    return 1;
}

int redisIsSupervised(int mode) {
    if (mode == SUPERVISED_AUTODETECT) {
        const char *upstart_job = getenv("UPSTART_JOB");
        const char *notify_socket = getenv("NOTIFY_SOCKET");

        if (upstart_job) {
            redisSupervisedUpstart();
        } else if (notify_socket) {
            redisSupervisedSystemd();
        }
    } else if (mode == SUPERVISED_UPSTART) {
        return redisSupervisedUpstart();
    } else if (mode == SUPERVISED_SYSTEMD) {
        return redisSupervisedSystemd();
    }

    return 0;
}

uint64_t getMvccTstamp()
{
    uint64_t rval;
    __atomic_load(&g_pserver->mvcc_tstamp, &rval, __ATOMIC_ACQUIRE);
    return rval;
}

void incrementMvccTstamp()
{
    uint64_t msPrev;
    __atomic_load(&g_pserver->mvcc_tstamp, &msPrev, __ATOMIC_ACQUIRE);
    msPrev >>= MVCC_MS_SHIFT;  // convert to milliseconds

    long long mst;
    __atomic_load(&g_pserver->mstime, &mst, __ATOMIC_RELAXED);
    if (msPrev >= (uint64_t)mst)  // we can be greater if the count overflows
    {
        atomicIncr(g_pserver->mvcc_tstamp, 1);
    }
    else
    {
        atomicSet(g_pserver->mvcc_tstamp, ((uint64_t)mst) << MVCC_MS_SHIFT);
    }
}

void OnTerminate()
{
    /* Any uncaught exception will call std::terminate().
        We want this handled like a segfault (printing the stack trace etc).
        The easiest way to achieve that is to acutally segfault, so we assert
        here.
    */
    auto exception = std::current_exception();
    if (exception != nullptr)
    {
        try
        {
            std::rethrow_exception(exception);
        }
        catch (const char *szErr)
        {
            serverLog(LL_WARNING, "Crashing on uncaught exception: %s", szErr);
        }
        catch (std::string str)
        {
            serverLog(LL_WARNING, "Crashing on uncaught exception: %s", str.c_str());
        }
        catch (...)
        {
            // NOP
        }
    }

    serverAssert(false);
}

void *workerThreadMain(void *parg)
{
    int iel = (int)((int64_t)parg);
    serverLog(LOG_INFO, "Thread %d alive.", iel);
    serverTL = g_pserver->rgthreadvar+iel;  // set the TLS threadsafe global

    moduleAcquireGIL(true); // Normally afterSleep acquires this, but that won't be called on the first run
    aeEventLoop *el = g_pserver->rgthreadvar[iel].el;
    aeSetBeforeSleepProc(el, beforeSleep, 0);
    aeSetAfterSleepProc(el, afterSleep, AE_SLEEP_THREADSAFE);
    aeMain(el);
    aeDeleteEventLoop(el);
    return NULL;
}

int main(int argc, char **argv) {
    struct timeval tv;
    int j;

    std::set_terminate(OnTerminate);

#ifdef USE_MEMKIND
    storage_init(NULL, 0);
#endif

#ifdef REDIS_TEST
    if (argc == 3 && !strcasecmp(argv[1], "test")) {
        if (!strcasecmp(argv[2], "ziplist")) {
            return ziplistTest(argc, argv);
        } else if (!strcasecmp(argv[2], "quicklist")) {
            quicklistTest(argc, argv);
        } else if (!strcasecmp(argv[2], "intset")) {
            return intsetTest(argc, argv);
        } else if (!strcasecmp(argv[2], "zipmap")) {
            return zipmapTest(argc, argv);
        } else if (!strcasecmp(argv[2], "sha1test")) {
            return sha1Test(argc, argv);
        } else if (!strcasecmp(argv[2], "util")) {
            return utilTest(argc, argv);
        } else if (!strcasecmp(argv[2], "endianconv")) {
            return endianconvTest(argc, argv);
        } else if (!strcasecmp(argv[2], "crc64")) {
            return crc64Test(argc, argv);
        } else if (!strcasecmp(argv[2], "zmalloc")) {
            return zmalloc_test(argc, argv);
        }

        return -1; /* test not found */
    }
#endif

    /* We need to initialize our libraries, and the server configuration. */
#ifdef INIT_SETPROCTITLE_REPLACEMENT
    spt_init(argc, argv);
#endif
    setlocale(LC_COLLATE,"");
    tzset(); /* Populates 'timezone' global. */
    zmalloc_set_oom_handler(redisOutOfMemoryHandler);
    srand(time(NULL)^getpid());
    gettimeofday(&tv,NULL);

    char hashseed[16];
    getRandomHexChars(hashseed,sizeof(hashseed));
    dictSetHashFunctionSeed((uint8_t*)hashseed);
    g_pserver->sentinel_mode = checkForSentinelMode(argc,argv);
    initServerConfig();
    for (int iel = 0; iel < MAX_EVENT_LOOPS; ++iel)
    {
        initServerThread(g_pserver->rgthreadvar+iel, iel == IDX_EVENT_LOOP_MAIN);
    }
    serverTL = &g_pserver->rgthreadvar[IDX_EVENT_LOOP_MAIN];
    aeAcquireLock();    // We own the lock on boot

    ACLInit(); /* The ACL subsystem must be initialized ASAP because the
                  basic networking code and client creation depends on it. */
    moduleInitModulesSystem();

    /* Store the executable path and arguments in a safe place in order
     * to be able to restart the server later. */
    cserver.executable = getAbsolutePath(argv[0]);
    cserver.exec_argv = (char**)zmalloc(sizeof(char*)*(argc+1), MALLOC_LOCAL);
    cserver.exec_argv[argc] = NULL;
    for (j = 0; j < argc; j++) cserver.exec_argv[j] = zstrdup(argv[j]);

    /* We need to init sentinel right now as parsing the configuration file
     * in sentinel mode will have the effect of populating the sentinel
     * data structures with master nodes to monitor. */
    if (g_pserver->sentinel_mode) {
        initSentinelConfig();
        initSentinel();
    }

    /* Check if we need to start in keydb-check-rdb/aof mode. We just execute
     * the program main. However the program is part of the Redis executable
     * so that we can easily execute an RDB check on loading errors. */
    if (strstr(argv[0],"keydb-check-rdb") != NULL)
        redis_check_rdb_main(argc,(const char**)argv,NULL);
    else if (strstr(argv[0],"keydb-check-aof") != NULL)
        redis_check_aof_main(argc,argv);

    if (argc >= 2) {
        j = 1; /* First option to parse in argv[] */
        sds options = sdsempty();
        char *configfile = NULL;

        /* Handle special options --help and --version */
        if (strcmp(argv[1], "-v") == 0 ||
            strcmp(argv[1], "--version") == 0) version();
        if (strcmp(argv[1], "--help") == 0 ||
            strcmp(argv[1], "-h") == 0) usage();
        if (strcmp(argv[1], "--test-memory") == 0) {
            if (argc == 3) {
                memtest(atoi(argv[2]),50);
                exit(0);
            } else {
                fprintf(stderr,"Please specify the amount of memory to test in megabytes.\n");
                fprintf(stderr,"Example: ./keydb-server --test-memory 4096\n\n");
                exit(1);
            }
        }

        /* First argument is the config file name? */
        if (argv[j][0] != '-' || argv[j][1] != '-') {
            configfile = argv[j];
            cserver.configfile = getAbsolutePath(configfile);
            /* Replace the config file in g_pserver->exec_argv with
             * its absolute path. */
            zfree(cserver.exec_argv[j]);
            cserver.exec_argv[j] = zstrdup(cserver.configfile);
            j++;
        }

        /* All the other options are parsed and conceptually appended to the
         * configuration file. For instance --port 6380 will generate the
         * string "port 6380\n" to be parsed after the actual file name
         * is parsed, if any. */
        while(j != argc) {
            if (argv[j][0] == '-' && argv[j][1] == '-') {
                /* Option name */
                if (!strcmp(argv[j], "--check-rdb")) {
                    /* Argument has no options, need to skip for parsing. */
                    j++;
                    continue;
                }
                if (sdslen(options)) options = sdscat(options,"\n");
                options = sdscat(options,argv[j]+2);
                options = sdscat(options," ");
            } else {
                /* Option argument */
                options = sdscatrepr(options,argv[j],strlen(argv[j]));
                options = sdscat(options," ");
            }
            j++;
        }
        if (g_pserver->sentinel_mode && configfile && *configfile == '-') {
            serverLog(LL_WARNING,
                "Sentinel config from STDIN not allowed.");
            serverLog(LL_WARNING,
                "Sentinel needs config file on disk to save state.  Exiting...");
            exit(1);
        }
        resetServerSaveParams();
        loadServerConfig(configfile,options);
        sdsfree(options);
    }

    serverLog(LL_WARNING, "oO0OoO0OoO0Oo KeyDB is starting oO0OoO0OoO0Oo");
    serverLog(LL_WARNING,
        "KeyDB version=%s, bits=%d, commit=%s, modified=%d, pid=%d, just started",
            KEYDB_REAL_VERSION,
            (sizeof(long) == 8) ? 64 : 32,
            redisGitSHA1(),
            strtol(redisGitDirty(),NULL,10) > 0,
            (int)getpid());

    if (argc == 1) {
        serverLog(LL_WARNING, "WARNING: no config file specified, using the default config. In order to specify a config file use %s /path/to/%s.conf", argv[0], g_pserver->sentinel_mode ? "sentinel" : "redis");
    } else {
        serverLog(LL_WARNING, "Configuration loaded");
    }

    if (cserver.cthreads > (int)std::thread::hardware_concurrency()) {
        serverLog(LL_WARNING, "WARNING: server-threads is greater than this machine's core count.  Truncating to %u threads", std::thread::hardware_concurrency());
	cserver.cthreads = (int)std::thread::hardware_concurrency();
	cserver.cthreads = std::max(cserver.cthreads, 1);	// in case of any weird sign overflows
    }

    cserver.supervised = redisIsSupervised(cserver.supervised_mode);
    int background = cserver.daemonize && !cserver.supervised;
    if (background) daemonize();

    initServer();
    initNetworking(cserver.cthreads > 1 /* fReusePort */);

    if (background || cserver.pidfile) createPidFile();
    redisSetProcTitle(argv[0]);
    redisAsciiArt();
    checkTcpBacklogSettings();

    if (!g_pserver->sentinel_mode) {
        /* Things not needed when running in Sentinel mode. */
        serverLog(LL_WARNING,"Server initialized");
    #ifdef __linux__
        linuxMemoryWarnings();
    #endif
        moduleLoadFromQueue();
        ACLLoadUsersAtStartup();

        // special case of FUZZING load from stdin then quit
        if (argc > 1 && strstr(argv[1],"rdbfuzz-mode") != NULL)
        {
            zmalloc_set_oom_handler(fuzzOutOfMemoryHandler);
#ifdef __AFL_HAVE_MANUAL_CONTROL
            __AFL_INIT();
#endif
            rio rdb;
            rdbSaveInfo rsi = RDB_SAVE_INFO_INIT;
            startLoading(stdin);
            rioInitWithFile(&rdb,stdin);
            rdbLoadRio(&rdb,&rsi,0);
            stopLoading();
            return EXIT_SUCCESS;
        }

        loadDataFromDisk();
        if (g_pserver->cluster_enabled) {
            if (verifyClusterConfigWithData() == C_ERR) {
                serverLog(LL_WARNING,
                    "You can't have keys in a DB different than DB 0 when in "
                    "Cluster mode. Exiting.");
                exit(1);
            }
        }
        if (g_pserver->rgthreadvar[IDX_EVENT_LOOP_MAIN].ipfd_count > 0)
            serverLog(LL_NOTICE,"Ready to accept connections");
        if (g_pserver->sofd > 0)
            serverLog(LL_NOTICE,"The server is now ready to accept connections at %s", g_pserver->unixsocket);
    } else {
        sentinelIsRunning();
    }

    if (g_pserver->rdb_filename == nullptr)
    {
        if (g_pserver->rdb_s3bucketpath == nullptr)
            g_pserver->rdb_filename = zstrdup(CONFIG_DEFAULT_RDB_FILENAME);
        else
            g_pserver->repl_diskless_sync = TRUE;
    }

    if (cserver.cthreads > 4) {
        serverLog(LL_WARNING, "Warning: server-threads is set to %d.  This is above the maximum recommend value of 4, please ensure you've verified this is actually faster on your machine.", cserver.cthreads);
    }

    /* Warning the user about suspicious maxmemory setting. */
    if (g_pserver->maxmemory > 0 && g_pserver->maxmemory < 1024*1024) {
        serverLog(LL_WARNING,"WARNING: You specified a maxmemory value that is less than 1MB (current value is %llu bytes). Are you sure this is what you really want?", g_pserver->maxmemory);
    }

    aeReleaseLock();    //Finally we can dump the lock
    moduleReleaseGIL(true);
    
    serverAssert(cserver.cthreads > 0 && cserver.cthreads <= MAX_EVENT_LOOPS);
    pthread_t rgthread[MAX_EVENT_LOOPS];
    for (int iel = 0; iel < cserver.cthreads; ++iel)
    {
        pthread_create(rgthread + iel, NULL, workerThreadMain, (void*)((int64_t)iel));
        if (cserver.fThreadAffinity)
        {
#ifdef __linux__
            cpu_set_t cpuset;
            CPU_ZERO(&cpuset);
            CPU_SET(iel, &cpuset);
            if (pthread_setaffinity_np(rgthread[iel], sizeof(cpu_set_t), &cpuset) == 0)
            {
                serverLog(LOG_INFO, "Binding thread %d to cpu %d", iel, iel);
            }
#else
			serverLog(LL_WARNING, "CPU pinning not available on this platform");
#endif
        }
    }

    /* Block SIGALRM from this thread, it should only be received on a server thread */
    sigset_t sigset;
    sigemptyset(&sigset);
    sigaddset(&sigset, SIGALRM);
    pthread_sigmask(SIG_BLOCK, &sigset, nullptr);

    /* The main thread sleeps until all the workers are done.
        this is so that all worker threads are orthogonal in their startup/shutdown */
    void *pvRet;
    pthread_join(rgthread[IDX_EVENT_LOOP_MAIN], &pvRet);
    return 0;
}

/* The End */
