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
#include "monotonic.h"
#include "cluster.h"
#include "slowlog.h"
#include "bio.h"
#include "latency.h"
#include "atomicvar.h"
#include "storage.h"
#include "cron.h"
#include <thread>
#include "mt19937-64.h"

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
#include <sys/utsname.h>
#include <locale.h>
#include <sys/socket.h>
#include <algorithm>
#include <uuid/uuid.h>
#include <condition_variable>
#include "aelocker.h"
#include "motd.h"
#include "t_nhash.h"
#include "readwritelock.h"
#ifdef __linux__
#include <sys/prctl.h>
#include <sys/mman.h>
#endif

int g_fTestMode = false;
const char *motd_url = "http://api.keydb.dev/motd/motd_server.txt";
const char *motd_cache_file = "/.keydb-server-motd";

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
fastlock time_thread_lock("Time thread lock");
std::condition_variable_any time_thread_cv;
int sleeping_threads = 0;
void wakeTimeThread();

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
 * read-only:   Commands just reading from keys without changing the content.
 *              Note that commands that don't read from the keyspace such as
 *              TIME, SELECT, INFO, administrative commands, and connection
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
 * no-slowlog:  Do not automatically propagate the command to the slowlog.
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
 * may-replicate: Command may produce replication traffic, but should be 
 *                allowed under circumstances where write commands are disallowed. 
 *                Examples include PUBLISH, which replicates pubsub messages,and 
 *                EVAL, which may execute write commands, which are replicated, 
 *                or may just execute read commands. A command can not be marked 
 *                both "write" and "may-replicate"
 *
 * The following additional flags are only used in order to put commands
 * in a specific ACL category. Commands can have multiple ACL categories.
 *
 * @keyspace, @read, @write, @set, @sortedset, @list, @hash, @string, @bitmap,
 * @hyperloglog, @stream, @admin, @fast, @slow, @pubsub, @blocking, @dangerous,
 * @connection, @transaction, @scripting, @geo, @replication.
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
     "read-only fast async @string",
     0,NULL,1,1,1,0,0,0},

    {"getex",getexCommand,-2,
     "write fast @string",
     0,NULL,1,1,1,0,0,0},

    {"getdel",getdelCommand,2,
     "write fast @string",
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

    {"keydb.mexists",mexistsCommand,-2,
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

    {"bitfield_ro",bitfieldroCommand,-2,
     "read-only fast @bitmap",
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
     "read-only fast async @string",
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

    {"rpop",rpopCommand,-2,
     "write fast @list",
     0,NULL,1,1,1,0,0,0},

    {"lpop",lpopCommand,-2,
     "write fast @list",
     0,NULL,1,1,1,0,0,0},

    {"brpop",brpopCommand,-3,
     "write no-script @list @blocking",
     0,NULL,1,-2,1,0,0,0},

    {"brpoplpush",brpoplpushCommand,4,
     "write use-memory no-script @list @blocking",
     0,NULL,1,2,1,0,0,0},

    {"blmove",blmoveCommand,6,
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

    {"lpos",lposCommand,-3,
     "read-only @list",
     0,NULL,1,1,1,0,0,0},

    {"lrem",lremCommand,4,
     "write @list",
     0,NULL,1,1,1,0,0,0},

    {"rpoplpush",rpoplpushCommand,3,
     "write use-memory @list",
     0,NULL,1,2,1,0,0,0},

    {"lmove",lmoveCommand,5,
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

    {"smismember",smismemberCommand,-3,
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
     0,zunionInterDiffStoreGetKeys,1,1,1,0,0,0},

    {"zinterstore",zinterstoreCommand,-4,
     "write use-memory @sortedset",
     0,zunionInterDiffStoreGetKeys,1,1,1,0,0,0},

    {"zdiffstore",zdiffstoreCommand,-4,
     "write use-memory @sortedset",
     0,zunionInterDiffStoreGetKeys,1,1,1,0,0,0},

    {"zunion",zunionCommand,-3,
     "read-only @sortedset",
     0,zunionInterDiffGetKeys,0,0,0,0,0,0},

    {"zinter",zinterCommand,-3,
     "read-only @sortedset",
     0,zunionInterDiffGetKeys,0,0,0,0,0,0},

    {"zdiff",zdiffCommand,-3,
     "read-only @sortedset",
     0,zunionInterDiffGetKeys,0,0,0,0,0,0},

    {"zrange",zrangeCommand,-4,
     "read-only @sortedset",
     0,NULL,1,1,1,0,0,0},

    {"zrangestore",zrangestoreCommand,-5,
     "write use-memory @sortedset",
     0,NULL,1,2,1,0,0,0},

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

    {"zmscore",zmscoreCommand,-3,
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

    {"zrandmember",zrandmemberCommand,-2,
     "read-only random @sortedset",
     0,NULL,1,1,1,0,0,0},

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

    {"hrandfield",hrandfieldCommand,-2,
     "read-only random @hash",
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
     "ok-loading fast ok-stale @keyspace",
     0,NULL,0,0,0,0,0,0},

    {"swapdb",swapdbCommand,3,
     "write fast @keyspace @dangerous",
     0,NULL,0,0,0,0,0,0},

    {"move",moveCommand,3,
     "write fast @keyspace",
     0,NULL,1,1,1,0,0,0},

    {"copy",copyCommand,-3,
     "write use-memory @keyspace",
     0,NULL,1,2,1,0,0,0},

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
    
    {"pexpirememberat", pexpireMemberAtCommand, 4,
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
     "no-auth no-script ok-loading ok-stale fast @connection",
     0,NULL,0,0,0,0,0,0},

    /* We don't allow PING during loading since in Redis PING is used as
     * failure detection, and a loading server is considered to be
     * not available. */
    {"ping",pingCommand,-1,
     "ok-stale ok-loading fast @connection @replication",
     0,NULL,0,0,0,0,0,0},

     {"replping",pingCommand,-1,
     "ok-stale fast @connection @replication",
     0,NULL,0,0,0,0,0,0},

    {"echo",echoCommand,2,
     "fast @connection",
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
     "random fast ok-loading ok-stale @admin @dangerous",
     0,NULL,0,0,0,0,0,0},

    {"type",typeCommand,2,
     "read-only fast @keyspace",
     0,NULL,1,1,1,0,0,0},

    {"multi",multiCommand,1,
     "no-script fast ok-loading ok-stale @transaction",
     0,NULL,0,0,0,0,0,0},

    {"exec",execCommand,1,
     "no-script no-slowlog ok-loading ok-stale @transaction",
     0,NULL,0,0,0,0,0,0},

    {"discard",discardCommand,1,
     "no-script fast ok-loading ok-stale @transaction",
     0,NULL,0,0,0,0,0,0},

    {"sync",syncCommand,1,
     "admin no-script @replication",
     0,NULL,0,0,0,0,0,0},

    {"psync",syncCommand,-3,
     "admin no-script @replication",
     0,NULL,0,0,0,0,0,0},

    {"replconf",replconfCommand,-1,
     "admin no-script ok-loading ok-stale @replication",
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
     "admin no-script ok-loading ok-stale",
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

    {"replicaof",replicaofCommand,-3,
     "admin no-script ok-stale",
     0,NULL,0,0,0,0,0,0},

    {"role",roleCommand,1,
     "ok-loading ok-stale no-script fast @dangerous",
     0,NULL,0,0,0,0,0,0},

    {"debug",debugCommand,-2,
     "admin no-script ok-loading ok-stale",
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
     "pub-sub ok-loading ok-stale fast may-replicate",
     0,NULL,0,0,0,0,0,0},

    {"pubsub",pubsubCommand,-2,
     "pub-sub ok-loading ok-stale random",
     0,NULL,0,0,0,0,0,0},

    {"watch",watchCommand,-2,
     "no-script fast ok-loading ok-stale @transaction",
     0,NULL,1,-1,1,0,0,0},

    {"unwatch",unwatchCommand,1,
     "no-script fast ok-loading ok-stale @transaction",
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
     0,migrateGetKeys,3,3,1,0,0,0},

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
     0,memoryGetKeys,0,0,0,0,0,0},

    {"client",clientCommand,-2,
     "admin no-script random ok-loading ok-stale @connection",
     0,NULL,0,0,0,0,0,0},

    {"hello",helloCommand,-1,
     "no-auth no-script fast ok-loading ok-stale @connection",
     0,NULL,0,0,0,0,0,0},

    /* EVAL can modify the dataset, however it is not flagged as a write
     * command since we do the check while running commands from Lua.
     * 
     * EVAL and EVALSHA also feed monitors before the commands are executed,
     * as opposed to after.
      */
    {"eval",evalCommand,-3,
     "no-script no-monitor may-replicate @scripting",
     0,evalGetKeys,0,0,0,0,0,0},

    {"evalsha",evalShaCommand,-3,
     "no-script no-monitor may-replicate @scripting",
     0,evalGetKeys,0,0,0,0,0,0},

    {"slowlog",slowlogCommand,-2,
     "admin random ok-loading ok-stale",
     0,NULL,0,0,0,0,0,0},

    {"script",scriptCommand,-2,
     "no-script may-replicate @scripting",
     0,NULL,0,0,0,0,0,0},

    {"time",timeCommand,1,
     "random fast ok-loading ok-stale",
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
     "write use-memory @geo",
     0,georadiusGetKeys,1,1,1,0,0,0},

    {"georadius_ro",georadiusroCommand,-6,
     "read-only @geo",
     0,NULL,1,1,1,0,0,0},

    {"georadiusbymember",georadiusbymemberCommand,-5,
     "write use-memory @geo",
     0,georadiusGetKeys,1,1,1,0,0,0},

    {"georadiusbymember_ro",georadiusbymemberroCommand,-5,
     "read-only @geo",
     0,NULL,1,1,1,0,0,0},

    {"geohash",geohashCommand,-2,
     "read-only @geo",
     0,NULL,1,1,1,0,0,0},

    {"geopos",geoposCommand,-2,
     "read-only @geo",
     0,NULL,1,1,1,0,0,0},

    {"geodist",geodistCommand,-4,
     "read-only @geo",
     0,NULL,1,1,1,0,0,0},

    {"geosearch",geosearchCommand,-7,
     "read-only @geo",
      0,NULL,1,1,1,0,0,0},

    {"geosearchstore",geosearchstoreCommand,-8,
     "write use-memory @geo",
      0,NULL,1,2,1,0,0,0},

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
     "read-only may-replicate @hyperloglog",
     0,NULL,1,-1,1,0,0,0},

    {"pfmerge",pfmergeCommand,-2,
     "write use-memory @hyperloglog",
     0,NULL,1,-1,1,0,0,0},

    /* Unlike PFCOUNT that is considered as a read-only command (although
     * it changes a bit), PFDEBUG may change the entire key when converting
     * from sparse to dense representation */
    {"pfdebug",pfdebugCommand,-3,
     "admin write use-memory @hyperloglog",
     0,NULL,2,2,1,0,0,0},

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
     "read-only @stream @blocking",
     0,xreadGetKeys,0,0,0,0,0,0},

    {"xreadgroup",xreadCommand,-7,
     "write @stream @blocking",
     0,xreadGetKeys,0,0,0,0,0,0},

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

    {"xautoclaim",xautoclaimCommand,-6,
     "write random fast @stream",
     0,NULL,1,1,1,0,0,0},

    {"xinfo",xinfoCommand,-2,
     "read-only random @stream",
     0,NULL,2,2,1,0,0,0},

    {"xdel",xdelCommand,-3,
     "write fast @stream",
     0,NULL,1,1,1,0,0,0},

    {"xtrim",xtrimCommand,-4,
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
     "read-only fast noprop ok-stale",
     0,NULL,0,0,0,0,0,0},

    {"keydb.cron",cronCommand,-5,
     "write use-memory",
     0,NULL,1,1,1,0,0,0},

    {"keydb.hrename", hrenameCommand, 4,
     "write fast @hash",
     0,NULL,0,0,0,0,0,0},
    
    {"stralgo",stralgoCommand,-2,
     "read-only @string",
     0,lcsGetKeys,0,0,0,0,0,0},

    {"keydb.nhget",nhgetCommand,-2,
     "read-only fast @hash",
     0,NULL,1,1,1,0,0,0},
    
    {"keydb.nhset",nhsetCommand,-3,
     "read-only fast @hash",
     0,NULL,1,1,1,0,0,0},

    {"KEYDB.MVCCRESTORE",mvccrestoreCommand, 5,
     "write use-memory @keyspace @dangerous",
     0,NULL,1,1,1,0,0,0},

    {"reset",resetCommand,1,
     "no-script ok-stale ok-loading fast @connection",
     0,NULL,0,0,0,0,0,0},

    {"failover",failoverCommand,-1,
     "admin no-script ok-stale",
     0,NULL,0,0,0,0,0,0},

    {"lfence", lfenceCommand,1,
     "read-only random ok-stale",
     0,NULL,0,0,0,0,0,0}
};

/*============================ Utility functions ============================ */

/* We use a private localtime implementation which is fork-safe. The logging
 * function of Redis may be called from other threads. */
extern "C" void nolocks_localtime(struct tm *tmp, time_t t, time_t tz, int dst);
extern "C" pid_t gettid();

void processClients();

/* Low level logging. To use only for very big messages, otherwise
 * serverLog() is to prefer. */
#if defined(__has_feature)
#  if __has_feature(thread_sanitizer)
__attribute__((no_sanitize("thread")))
#  endif
#endif
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
        int daylight_active;
        __atomic_load(&g_pserver->daylight_active, &daylight_active, __ATOMIC_RELAXED);
        nolocks_localtime(&tm,tv.tv_sec,g_pserver->timezone,daylight_active);
        off = strftime(buf,sizeof(buf),"%d %b %Y %H:%M:%S.",&tm);
        snprintf(buf+off,sizeof(buf)-off,"%03d",(int)tv.tv_usec/1000);
        if (g_pserver->sentinel_mode) {
            role_char = 'X'; /* Sentinel. */
        } else if (pid != cserver.pid) {
            role_char = 'C'; /* RDB / AOF writing child. */
        } else {
            role_char = (listLength(g_pserver->masters) ? 'S':'M'); /* Slave or Master. */
        }
        fprintf(fp,"%d:%d:%c %s %c %s\n",
            (int)getpid(),(int)gettid(),role_char, buf,c[level],msg);
    }
    fflush(fp);

    if (!log_to_stdout) fclose(fp);
    if (g_pserver->syslog_enabled) syslog(syslogLevelMap[level], "%s", msg);
}

/* Like serverLogRaw() but with printf-alike support. This is the function that
 * is used across the code. The raw version is only used in order to dump
 * the INFO output on crash. */
#if defined(__has_feature)
#  if __has_feature(thread_sanitizer)
__attribute__((no_sanitize("thread")))
#  endif
#endif
void _serverLog(int level, const char *fmt, ...) {
    va_list ap;
    char msg[LOG_MAX_LEN];

    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);

    serverLogRaw(level,msg);
}

/* Log a fixed message without printf-alike capabilities, in a way that is
 * safe to call from a signal handler.
 *
 * We actually use this only for signals that are not fatal from the point
 * of view of Redis. Signals that are going to kill the server anyway and
 * where we need printf-alike features are served by serverLog(). */
#if defined(__has_feature)
#  if __has_feature(thread_sanitizer)
__attribute__((no_sanitize("thread")))
#  endif
#endif
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

    /* Due to OBJ_STATIC_REFCOUNT, we avoid calling getDecodedObject() without
     * good reasons, because it would incrRefCount() the object, which
     * is invalid. So we check to make sure dictFind() works with static
     * objects as well. */
    if (o1->getrefcount() != OBJ_STATIC_REFCOUNT) o1 = getDecodedObject(o1);
    if (o2->getrefcount() != OBJ_STATIC_REFCOUNT) o2 = getDecodedObject(o2);
    cmp = dictSdsKeyCompare(privdata,ptrFromObj(o1),ptrFromObj(o2));
    if (o1->getrefcount() != OBJ_STATIC_REFCOUNT) decrRefCount(o1);
    if (o2->getrefcount() != OBJ_STATIC_REFCOUNT) decrRefCount(o2);
    return cmp;
}

uint64_t dictEncObjHash(const void *key) {
    robj *o = (robj*) key;

    if (sdsEncodedObject(o)) {
        return dictGenHashFunction(ptrFromObj(o), sdslen((sds)ptrFromObj(o)));
    } else if (o->encoding == OBJ_ENCODING_INT) {
        char buf[32];
        int len;

        len = ll2string(buf,32,(long)ptrFromObj(o));
        return dictGenHashFunction((unsigned char*)buf, len);
    } else {
        serverPanic("Unknown string encoding");
    }
}

/* Return 1 if currently we allow dict to expand. Dict may allocate huge
 * memory to contain hash buckets when dict expands, that may lead redis
 * rejects user's requests or evicts some keys, we can stop dict to expand
 * provisionally if used memory will be over maxmemory after dict expands,
 * but to guarantee the performance of redis, we still allow dict to expand
 * if dict load factor exceeds HASHTABLE_MAX_LOAD_FACTOR. */
int dictExpandAllowed(size_t moreMem, double usedRatio) {
    if (usedRatio <= HASHTABLE_MAX_LOAD_FACTOR) {
        return !overMaxmemoryAfterAlloc(moreMem);
    } else {
        return 1;
    }
}

void dictGCAsyncFree(dictAsyncRehashCtl *async);

/* Generic hash table type where keys are Redis Objects, Values
 * dummy pointers. */
dictType objectKeyPointerValueDictType = {
    dictEncObjHash,            /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictEncObjKeyCompare,      /* key compare */
    dictObjectDestructor,      /* key destructor */
    NULL,                      /* val destructor */
    NULL                       /* allow to expand */
};

/* Like objectKeyPointerValueDictType(), but values can be destroyed, if
 * not NULL, calling zfree(). */
dictType objectKeyHeapPointerValueDictType = {
    dictEncObjHash,            /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictEncObjKeyCompare,      /* key compare */
    dictObjectDestructor,      /* key destructor */
    dictVanillaFree,           /* val destructor */
    NULL                       /* allow to expand */
};

/* Set dictionary type. Keys are SDS strings, values are not used. */
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
    NULL,                      /* val destructor */
    NULL                       /* allow to expand */
};

/* db->dict, keys are sds strings, vals are Redis objects. */
dictType dbDictType = {
    dictSdsHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCompare,          /* key compare */
    dictDbKeyDestructor,        /* key destructor */
    dictObjectDestructor,       /* val destructor */
    dictExpandAllowed,           /* allow to expand */
    dictGCAsyncFree             /* async free destructor */
};

/* db->pdict, keys are sds strings, vals are Redis objects. */
dictType dbTombstoneDictType = {
    dictSdsHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCompare,          /* key compare */
    dictDbKeyDestructor,        /* key destructor */
    NULL,                       /* val destructor */
    dictExpandAllowed           /* allow to expand */
};

dictType dbSnapshotDictType = {
    dictSdsHash,
    NULL,
    NULL,
    dictSdsKeyCompare,
    dictSdsNOPDestructor,
    dictObjectDestructor,
    dictExpandAllowed           /* allow to expand */
};

/* g_pserver->lua_scripts sha (as sds string) -> scripts (as robj) cache. */
dictType shaScriptObjectDictType = {
    dictSdsCaseHash,            /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCaseCompare,      /* key compare */
    dictSdsDestructor,          /* key destructor */
    dictObjectDestructor,       /* val destructor */
    NULL                        /* allow to expand */
};

/* Db->expires */
dictType dbExpiresDictType = {
    dictSdsHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCompare,          /* key compare */
    NULL,                       /* key destructor */
    NULL,                       /* val destructor */
    dictExpandAllowed           /* allow to expand */
};

/* Command table. sds string -> command struct pointer. */
dictType commandTableDictType = {
    dictSdsCaseHash,            /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCaseCompare,      /* key compare */
    dictSdsDestructor,          /* key destructor */
    NULL,                       /* val destructor */
    NULL                        /* allow to expand */
};

/* Hash type hash table (note that small hashes are represented with ziplists) */
dictType hashDictType = {
    dictSdsHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCompare,          /* key compare */
    dictSdsDestructor,          /* key destructor */
    dictSdsDestructor,          /* val destructor */
    NULL                        /* allow to expand */
};

/* Dict type without destructor */
dictType sdsReplyDictType = {
    dictSdsHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCompare,          /* key compare */
    NULL,                       /* key destructor */
    NULL,                       /* val destructor */
    NULL                        /* allow to expand */
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
    dictListDestructor,         /* val destructor */
    NULL                        /* allow to expand */
};

/* Cluster nodes hash table, mapping nodes addresses 1.2.3.4:6379 to
 * clusterNode structures. */
dictType clusterNodesDictType = {
    dictSdsHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCompare,          /* key compare */
    dictSdsDestructor,          /* key destructor */
    NULL,                       /* val destructor */
    NULL                        /* allow to expand */
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
    NULL,                       /* val destructor */
    NULL                        /* allow to expand */
};

/* Modules system dictionary type. Keys are module name,
 * values are pointer to RedisModule struct. */
dictType modulesDictType = {
    dictSdsCaseHash,            /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCaseCompare,      /* key compare */
    dictSdsDestructor,          /* key destructor */
    NULL,                       /* val destructor */
    NULL                        /* allow to expand */
};

/* Migrate cache dict type. */
dictType migrateCacheDictType = {
    dictSdsHash,                /* hash function */
    NULL,                       /* key dup */
    NULL,                       /* val dup */
    dictSdsKeyCompare,          /* key compare */
    dictSdsDestructor,          /* key destructor */
    NULL,                       /* val destructor */
    NULL                        /* allow to expand */
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
    NULL,                       /* val destructor */
    NULL                        /* allow to expand */
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
 * of CPU time at every call of this function to perform some rehashing.
 *
 * The function returns the number of rehashes if some rehashing was performed, otherwise 0
 * is returned. */
int redisDbPersistentData::incrementallyRehash() {
    /* Keys dictionary */
    int result = 0;
    if (dictIsRehashing(m_pdict))
        result += dictRehashMilliseconds(m_pdict,1);
    if (dictIsRehashing(m_pdictTombstone))
        dictRehashMilliseconds(m_pdictTombstone,1); // don't count this
    return result; /* already used our millisecond for this loop... */
}

/* This function is called once a background process of some kind terminates,
 * as we want to avoid resizing the hash tables when there is a child in order
 * to play well with copy-on-write (otherwise when a resize happens lots of
 * memory pages are copied). The goal of this function is to update the ability
 * for dict.c to resize the hash tables accordingly to the fact we have an
 * active fork child running. */
void updateDictResizePolicy(void) {
    if (!hasActiveChildProcess() || (g_pserver->FRdbSaveInProgress() && !cserver.fForkBgSave))
        dictEnableResize();
    else
        dictDisableResize();
}

const char *strChildType(int type) {
    switch(type) {
        case CHILD_TYPE_RDB: return "RDB";
        case CHILD_TYPE_AOF: return "AOF";
        case CHILD_TYPE_LDB: return "LDB";
        case CHILD_TYPE_MODULE: return "MODULE";
        default: return "Unknown";
    }
}

/* Return true if there are active children processes doing RDB saving,
 * AOF rewriting, or some side process spawned by a loaded module. */
int hasActiveChildProcess() {
    return g_pserver->FRdbSaveInProgress() || g_pserver->child_pid != -1;
}

void resetChildState() {
    g_pserver->child_type = CHILD_TYPE_NONE;
    g_pserver->child_pid = -1;
    g_pserver->stat_current_cow_bytes = 0;
    g_pserver->stat_current_cow_updated = 0;
    g_pserver->stat_current_save_keys_processed = 0;
    g_pserver->stat_module_progress = 0;
    g_pserver->stat_current_save_keys_total = 0;
    updateDictResizePolicy();
    closeChildInfoPipe();
    moduleFireServerEvent(REDISMODULE_EVENT_FORK_CHILD,
                          REDISMODULE_SUBEVENT_FORK_CHILD_DIED,
                          NULL);
}

/* Return if child type is mutual exclusive with other fork children */
int isMutuallyExclusiveChildType(int type) {
    return type == CHILD_TYPE_RDB || type == CHILD_TYPE_AOF || type == CHILD_TYPE_MODULE;
}

/* Return true if this instance has persistence completely turned off:
 * both RDB and AOF are disabled. */
int allPersistenceDisabled(void) {
    return g_pserver->saveparamslen == 0 && g_pserver->aof_state == AOF_OFF;
}

/* ======================= Cron: called every 100 ms ======================== */

/* Add a sample to the operations per second array of samples. */
void trackInstantaneousMetric(int metric, long long current_reading) {
    long long now = mstime();
    long long t = now - g_pserver->inst_metric[metric].last_sample_time;
    long long ops = current_reading -
                    g_pserver->inst_metric[metric].last_sample_count;
    long long ops_sec;

    ops_sec = t > 0 ? (ops*1000/t) : 0;

    g_pserver->inst_metric[metric].samples[g_pserver->inst_metric[metric].idx] =
        ops_sec;
    g_pserver->inst_metric[metric].idx++;
    g_pserver->inst_metric[metric].idx %= STATS_METRIC_SAMPLES;
    g_pserver->inst_metric[metric].last_sample_time = now;
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

SymVer parseVersion(const char *version)
{
    SymVer ver = {-1,-1,-1};
    long versions[3] = {-1,-1,-1};
    const char *start = version;
    const char *end = nullptr;

    for (int iver = 0; iver < 3; ++iver)
    {
        end = start;
        while (*end != '\0' && *end != '.')
            ++end;

        if (start >= end)
            return ver;

        if (!string2l(start, end - start, versions + iver))
            return ver;
        if (*end != '\0')
            start = end+1;
        else
            break;
    }
    ver.major = versions[0];
    ver.minor = versions[1];
    ver.build = versions[2];
    
    return ver;
}

VersionCompareResult compareVersion(SymVer *pver)
{
    SymVer symVerThis = parseVersion(KEYDB_REAL_VERSION);
    // Special case, 0.0.0 is equal to any version
    if ((symVerThis.major == 0 && symVerThis.minor == 0 && symVerThis.build == 0)
        || (pver->major == 0 && pver->minor == 0 && pver->build == 0))
        return VersionCompareResult::EqualVersion;
    
    for (int iver = 0; iver < 3; ++iver)
    {
        long verThis, verOther;
        switch (iver)
        {
        case 0:
            verThis = symVerThis.major; verOther = pver->major;
            break;
        case 1:
            verThis = symVerThis.minor; verOther = pver->minor;
            break;
        case 2:
            verThis = symVerThis.build; verOther = pver->build;
        }
        
        if (verThis < verOther)
            return VersionCompareResult::NewerVersion;
        if (verThis > verOther)
            return VersionCompareResult::OlderVersion;
    }
    return VersionCompareResult::EqualVersion;
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
size_t ClientsPeakMemInput[CLIENTS_PEAK_MEM_USAGE_SLOTS] = {0};
size_t ClientsPeakMemOutput[CLIENTS_PEAK_MEM_USAGE_SLOTS] = {0};

int clientsCronTrackExpansiveClients(client *c, int time_idx) {
    size_t in_usage = sdsZmallocSize(c->querybuf) + c->argv_len_sum() +
	              (c->argv ? zmalloc_size(c->argv) : 0);
    size_t out_usage = getClientOutputBufferMemoryUsage(c);

    /* Track the biggest values observed so far in this slot. */
    if (in_usage > ClientsPeakMemInput[time_idx]) ClientsPeakMemInput[time_idx] = in_usage;
    if (out_usage > ClientsPeakMemOutput[time_idx]) ClientsPeakMemOutput[time_idx] = out_usage;

    return 0; /* This function never terminates the client. */
}

/* Iterating all the clients in getMemoryOverheadData() is too slow and
 * in turn would make the INFO command too slow. So we perform this
 * computation incrementally and track the (not instantaneous but updated
 * to the second) total memory used by clients using clinetsCron() in
 * a more incremental way (depending on g_pserver->hz). */
int clientsCronTrackClientsMemUsage(client *c) {
    size_t mem = 0;
    int type = getClientType(c);
    mem += getClientOutputBufferMemoryUsage(c);
    mem += sdsZmallocSize(c->querybuf);
    mem += zmalloc_size(c);
    mem += c->argv_len_sum();
    if (c->argv) mem += zmalloc_size(c->argv);
    /* Now that we have the memory used by the client, remove the old
     * value from the old category, and add it back. */
    g_pserver->stat_clients_type_memory[c->client_cron_last_memory_type] -=
        c->client_cron_last_memory_usage;
    g_pserver->stat_clients_type_memory[type] += mem;
    /* Remember what we added and where, to remove it next time. */
    c->client_cron_last_memory_usage = mem;
    c->client_cron_last_memory_type = type;
    return 0;
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


    int curr_peak_mem_usage_slot = g_pserver->unixtime % CLIENTS_PEAK_MEM_USAGE_SLOTS;
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
    int zeroidx = (curr_peak_mem_usage_slot+1) % CLIENTS_PEAK_MEM_USAGE_SLOTS;
    ClientsPeakMemInput[zeroidx] = 0;
    ClientsPeakMemOutput[zeroidx] = 0;


    while(listLength(g_pserver->clients) && iterations--) {
        client *c;
        listNode *head;
        /* Rotate the list, take the current head, process.
         * This way if the client must be removed from the list it's the
         * first element and we don't incur into O(N) computation. */
        listRotateTailToHead(g_pserver->clients);
        head = (listNode*)listFirst(g_pserver->clients);
        c = (client*)listNodeValue(head);
        if (c->iel == iel)
        {
            fastlock_lock(&c->lock);
            /* The following functions do different service checks on the client.
            * The protocol is that they return non-zero if the client was
            * terminated. */
            if (clientsCronHandleTimeout(c,now)) continue;  // Client free'd so don't release the lock
            if (clientsCronResizeQueryBuffer(c)) goto LContinue;
            if (clientsCronTrackExpansiveClients(c, curr_peak_mem_usage_slot)) goto LContinue;
            if (clientsCronTrackClientsMemUsage(c)) goto LContinue;
            if (closeClientOnOutputBufferLimitReached(c, 0)) continue; // Client also free'd
        LContinue:
            fastlock_unlock(&c->lock);
        }        
    }

    /* Free any pending clients */
    freeClientsInAsyncFreeQueue(iel);
}

bool expireOwnKeys()
{
    if (iAmMaster()) {
        return true;
    } else if (!g_pserver->fActiveReplica && (listLength(g_pserver->masters) == 1)) {
        redisMaster *mi = (redisMaster*)listNodeValue(listFirst(g_pserver->masters));
        if (mi->isActive)
            return true;
    }
    return false;
}

int hash_spin_worker() {
    auto ctl = serverTL->rehashCtl;
    return dictRehashSomeAsync(ctl, 1);
}

/* This function handles 'background' operations we are required to do
 * incrementally in Redis databases, such as active key expiring, resizing,
 * rehashing. */
void databasesCron(bool fMainThread) {
    serverAssert(GlobalLocksAcquired());

    if (fMainThread) {
        /* Expire keys by random sampling. Not required for slaves
        * as master will synthesize DELs for us. */
        if (g_pserver->active_expire_enabled) {
            if (expireOwnKeys()) {
                activeExpireCycle(ACTIVE_EXPIRE_CYCLE_SLOW);
            } else {
                expireSlaveKeys();
            }
        }

        /* Defrag keys gradually. */
        activeDefragCycle();
    }

    /* Perform hash tables rehashing if needed, but only if there are no
     * other processes saving the DB on disk. Otherwise rehashing is bad
     * as will cause a lot of copy-on-write of memory pages. */
    if (!hasActiveChildProcess() || g_pserver->FRdbSaveInProgress()) {
        /* We use global counters so if we stop the computation at a given
         * DB we'll be able to start from the successive in the next
         * cron loop iteration. */
        static unsigned int resize_db = 0;
        static unsigned int rehash_db = 0;
        static int rehashes_per_ms;
        static int async_rehashes;
        int dbs_per_call = CRON_DBS_PER_CALL;
        int j;

        /* Don't test more DBs than we have. */
        if (dbs_per_call > cserver.dbnum) dbs_per_call = cserver.dbnum;

        if (fMainThread) {
            /* Resize */
            for (j = 0; j < dbs_per_call; j++) {
                tryResizeHashTables(resize_db % cserver.dbnum);
                resize_db++;
            }
        }

        /* Rehash */
        if (g_pserver->activerehashing) {
            for (j = 0; j < dbs_per_call; j++) {
                if (serverTL->rehashCtl != nullptr) {
                    if (!serverTL->rehashCtl->done.load(std::memory_order_relaxed)) {
                        aeReleaseLock();
                        if (dictRehashSomeAsync(serverTL->rehashCtl, rehashes_per_ms)) {
                            aeAcquireLock();
                            break;
                        }
                        aeAcquireLock();
                    }

                    if (serverTL->rehashCtl->done.load(std::memory_order_relaxed)) {
                        dictCompleteRehashAsync(serverTL->rehashCtl, true /*fFree*/);
                        serverTL->rehashCtl = nullptr;
                    }
                }

                serverAssert(serverTL->rehashCtl == nullptr);
                ::dict *dict = g_pserver->db[rehash_db]->dictUnsafeKeyOnly();
                /* Are we async rehashing? And if so is it time to re-calibrate? */
                /* The recalibration limit is a prime number to ensure balancing across threads */
                if (rehashes_per_ms > 0 && async_rehashes < 131 && !cserver.active_defrag_enabled && cserver.cthreads > 1 && dictSize(dict) > 2048 && dictIsRehashing(dict) && !g_pserver->loading) {
                    serverTL->rehashCtl = dictRehashAsyncStart(dict, rehashes_per_ms * ((1000 / g_pserver->hz) / 10));  // Estimate 10% CPU time spent in lock contention
                    if (serverTL->rehashCtl)
                        ++async_rehashes;
                }
                if (serverTL->rehashCtl)
                    break;

                // Before starting anything new, can we end the rehash of a blocked thread?
                while (dict->asyncdata != nullptr) {
                    auto asyncdata = dict->asyncdata;
                    if (asyncdata->done) {
                        dictCompleteRehashAsync(asyncdata, false /*fFree*/);    // Don't free because we don't own the pointer
                        serverAssert(dict->asyncdata != asyncdata);
                    } else {
                        break;
                    }
                }

                if (dict->asyncdata)
                    break;

                rehashes_per_ms = g_pserver->db[rehash_db]->incrementallyRehash();
                async_rehashes = 0;
                if (rehashes_per_ms > 0) {
                    /* If the function did some work, stop here, we'll do
                    * more at the next cron loop. */
                    if (!cserver.active_defrag_enabled) {
                        serverLog(LL_VERBOSE, "Calibrated rehashes per ms: %d", rehashes_per_ms);
                    }
                    break;
                } else if (dict->asyncdata == nullptr) {
                    /* If this db didn't need rehash and we have none in flight, we'll try the next one. */
                    rehash_db++;
                    rehash_db %= cserver.dbnum;
                }
            }
        }
    }

    if (serverTL->rehashCtl) {
        setAeLockSetThreadSpinWorker(hash_spin_worker);
    } else {
        setAeLockSetThreadSpinWorker(nullptr);
    }
}

/* We take a cached value of the unix time in the global state because with
 * virtual memory and aging there is to store the current time in objects at
 * every object access, and accuracy is not needed. To access a global var is
 * a lot faster than calling time(NULL).
 *
 * This function should be fast because it is called at every command execution
 * in call(), so it is possible to decide if to update the daylight saving
 * info or not using the 'update_daylight_info' argument. Normally we update
 * such info only when calling this function from serverCron() but not when
 * calling it from call(). */
void updateCachedTime() {
    long long t = ustime();
    __atomic_store(&g_pserver->ustime, &t, __ATOMIC_RELAXED);
    t /= 1000;
    __atomic_store(&g_pserver->mstime, &t, __ATOMIC_RELAXED);
    t /= 1000;
    g_pserver->unixtime = t;

    /* To get information about daylight saving time, we need to call
     * localtime_r and cache the result. However calling localtime_r in this
     * context is safe since we will never fork() while here, in the main
     * thread. The logging function will call a thread safe version of
     * localtime that has no locks. */
    struct tm tm;
    time_t ut = g_pserver->unixtime;
    localtime_r(&ut,&tm);
    __atomic_store(&g_pserver->daylight_active, &tm.tm_isdst, __ATOMIC_RELAXED);
}

void checkChildrenDone(void) {
    int statloc = 0;
    pid_t pid;

    if (g_pserver->FRdbSaveInProgress() && !cserver.fForkBgSave)
    {
        void *rval = nullptr;
        int err = EAGAIN;
        if (!g_pserver->rdbThreadVars.fDone || (err = pthread_join(g_pserver->rdbThreadVars.rdb_child_thread, &rval)))
        {
            if (err != EBUSY && err != EAGAIN)
                serverLog(LL_WARNING, "Error joining the background RDB save thread: %s\n", strerror(errno));
        }
        else
        {
            int exitcode = (int)reinterpret_cast<ptrdiff_t>(rval);
            backgroundSaveDoneHandler(exitcode,g_pserver->rdbThreadVars.fRdbThreadCancel);
            g_pserver->rdbThreadVars.fRdbThreadCancel = false;
            g_pserver->rdbThreadVars.fDone = false;
            if (exitcode == 0) receiveChildInfo();
        }
    }
    else if ((pid = waitpid(-1, &statloc, WNOHANG)) != 0) {
        int exitcode = WIFEXITED(statloc) ? WEXITSTATUS(statloc) : -1;
        int bysignal = 0;

        if (WIFSIGNALED(statloc)) bysignal = WTERMSIG(statloc);

        /* sigKillChildHandler catches the signal and calls exit(), but we
         * must make sure not to flag lastbgsave_status, etc incorrectly.
         * We could directly terminate the child process via SIGUSR1
         * without handling it */
        if (exitcode == SERVER_CHILD_NOERROR_RETVAL) {
            bysignal = SIGUSR1;
            exitcode = 1;
        }

        if (pid == -1) {
            serverLog(LL_WARNING,"waitpid() returned an error: %s. "
                "child_type: %s, child_pid = %d",
                strerror(errno),
                strChildType(g_pserver->child_type),
                (int) g_pserver->child_pid);
        } else if (pid == g_pserver->child_pid) {
            if (g_pserver->child_type == CHILD_TYPE_RDB) {
                backgroundSaveDoneHandler(exitcode, bysignal);
            } else if (g_pserver->child_type == CHILD_TYPE_AOF) {
                backgroundRewriteDoneHandler(exitcode, bysignal);
            } else if (g_pserver->child_type == CHILD_TYPE_MODULE) {
                ModuleForkDoneHandler(exitcode, bysignal);
            } else {
                serverPanic("Unknown child type %d for child pid %d", g_pserver->child_type, g_pserver->child_pid);
                exit(1);
            }
            if (!bysignal && exitcode == 0) receiveChildInfo();
            resetChildState();
        } else {
            if (!ldbRemoveChild(pid)) {
                serverLog(LL_WARNING,
                          "Warning, detected child with unmatched pid: %ld",
                          (long) pid);
            }
        }

        /* start any pending forks immediately. */
        replicationStartPendingFork();
    }
}

/* Called from serverCron and loadingCron to update cached memory metrics. */
void cronUpdateMemoryStats() {
    /* Record the max memory used since the server was started. */
    if (zmalloc_used_memory() > g_pserver->stat_peak_memory)
        g_pserver->stat_peak_memory = zmalloc_used_memory();

    run_with_period(100) {
        /* Sample the RSS and other metrics here since this is a relatively slow call.
         * We must sample the zmalloc_used at the same time we take the rss, otherwise
         * the frag ratio calculate may be off (ratio of two samples at different times) */
        g_pserver->cron_malloc_stats.process_rss = zmalloc_get_rss();
        g_pserver->cron_malloc_stats.zmalloc_used = zmalloc_used_memory();
        /* Sampling the allocator info can be slow too.
         * The fragmentation ratio it'll show is potentially more accurate
         * it excludes other RSS pages such as: shared libraries, LUA and other non-zmalloc
         * allocations, and allocator reserved pages that can be pursed (all not actual frag) */
        zmalloc_get_allocator_info(&g_pserver->cron_malloc_stats.allocator_allocated,
                                   &g_pserver->cron_malloc_stats.allocator_active,
                                   &g_pserver->cron_malloc_stats.allocator_resident);
        /* in case the allocator isn't providing these stats, fake them so that
         * fragmentation info still shows some (inaccurate metrics) */
        if (!g_pserver->cron_malloc_stats.allocator_resident) {
            /* LUA memory isn't part of zmalloc_used, but it is part of the process RSS,
             * so we must deduct it in order to be able to calculate correct
             * "allocator fragmentation" ratio */
            size_t lua_memory = lua_gc(g_pserver->lua,LUA_GCCOUNT,0)*1024LL;
            g_pserver->cron_malloc_stats.allocator_resident = g_pserver->cron_malloc_stats.process_rss - lua_memory;
        }
        if (!g_pserver->cron_malloc_stats.allocator_active)
            g_pserver->cron_malloc_stats.allocator_active = g_pserver->cron_malloc_stats.allocator_resident;
        if (!g_pserver->cron_malloc_stats.allocator_allocated)
            g_pserver->cron_malloc_stats.allocator_allocated = g_pserver->cron_malloc_stats.zmalloc_used;
    }
}

static std::atomic<bool> s_fFlushInProgress { false };
void flushStorageWeak()
{
    bool fExpected = false;
    if (s_fFlushInProgress.compare_exchange_strong(fExpected, true /* desired */, std::memory_order_seq_cst, std::memory_order_relaxed))
    {
        g_pserver->asyncworkqueue->AddWorkFunction([]{
            aeAcquireLock();
            mstime_t storage_process_latency;
            latencyStartMonitor(storage_process_latency);
            std::vector<redisDb*> vecdb;
            for (int idb = 0; idb < cserver.dbnum; ++idb) {
                if (g_pserver->db[idb]->processChanges(true))
                    vecdb.push_back(g_pserver->db[idb]);
            }
            latencyEndMonitor(storage_process_latency);
            latencyAddSampleIfNeeded("storage-process-changes", storage_process_latency);
            aeReleaseLock();

            std::vector<const redisDbPersistentDataSnapshot*> vecsnapshotFree;
            vecsnapshotFree.resize(vecdb.size());
            for (size_t idb = 0; idb < vecdb.size(); ++idb)
                vecdb[idb]->commitChanges(&vecsnapshotFree[idb]);

            for (size_t idb = 0; idb < vecsnapshotFree.size(); ++idb) {
                if (vecsnapshotFree[idb] != nullptr)
                    vecdb[idb]->endSnapshotAsync(vecsnapshotFree[idb]);
            }
            s_fFlushInProgress = false;
        }, true /* fHiPri */);
    }
    else
    {
        serverLog(LOG_INFO, "Missed storage flush due to existing flush still in flight.  Consider increasing storage-weak-flush-period");
    }
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

void unblockChildThreadIfNecessary();
int serverCron(struct aeEventLoop *eventLoop, long long id, void *clientData) {
    int j;
    UNUSED(eventLoop);
    UNUSED(id);
    UNUSED(clientData);

    if (g_pserver->maxmemory && g_pserver->m_pstorageFactory)
        performEvictions(false);

    /* If another threads unblocked one of our clients, and this thread has been idle
        then beforeSleep won't have a chance to process the unblocking.  So we also
        process them here in the cron job to ensure they don't starve.
    */
    if (listLength(g_pserver->rgthreadvar[IDX_EVENT_LOOP_MAIN].unblocked_clients))
    {
        processUnblockedClients(IDX_EVENT_LOOP_MAIN);
    }
        
    /* Software watchdog: deliver the SIGALRM that will reach the signal
     * handler if we don't return here fast enough. */
    if (g_pserver->watchdog_period) watchdogScheduleSignal(g_pserver->watchdog_period);

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

    /* A cancelled child thread could be hung waiting for us to read from a pipe */
    unblockChildThreadIfNecessary();

    run_with_period(100) {
        long long stat_net_input_bytes, stat_net_output_bytes;
        stat_net_input_bytes = g_pserver->stat_net_input_bytes.load(std::memory_order_relaxed);
        stat_net_output_bytes = g_pserver->stat_net_output_bytes.load(std::memory_order_relaxed);

        long long stat_numcommands;
        __atomic_load(&g_pserver->stat_numcommands, &stat_numcommands, __ATOMIC_RELAXED);
        trackInstantaneousMetric(STATS_METRIC_COMMAND,stat_numcommands);
        trackInstantaneousMetric(STATS_METRIC_NET_INPUT,
                stat_net_input_bytes);
        trackInstantaneousMetric(STATS_METRIC_NET_OUTPUT,
                stat_net_output_bytes);
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

    cronUpdateMemoryStats();

    /* We received a SIGTERM, shutting down here in a safe way, as it is
     * not ok doing so inside the signal handler. */
    if (g_pserver->shutdown_asap) {
        if (prepareForShutdown(SHUTDOWN_NOFLAGS) == C_OK) throw ShutdownException();
        serverLog(LL_WARNING,"SIGTERM received but errors trying to shut down the server, check the logs for more information");
        g_pserver->shutdown_asap = 0;
    }

    /* Show some info about non-empty databases */
    if (cserver.verbosity <= LL_VERBOSE) {
        run_with_period(5000) {
            for (j = 0; j < cserver.dbnum; j++) {
                long long size, used, vkeys;

                size = g_pserver->db[j]->slots();
                used = g_pserver->db[j]->size();
                vkeys = g_pserver->db[j]->expireSize();
                if (used || vkeys) {
                    serverLog(LL_VERBOSE,"DB %d: %lld keys (%lld volatile) in %lld slots HT.",j,used,vkeys,size);
                }
            }
        }
    }

    /* Show information about connected clients */
    if (!g_pserver->sentinel_mode) {
        run_with_period(5000) {
            serverLog(LL_DEBUG,
                "%lu clients connected (%lu replicas), %zu bytes in use",
                listLength(g_pserver->clients)-listLength(g_pserver->slaves),
                listLength(g_pserver->slaves),
                zmalloc_used_memory());
        }
    }

    /* We need to do a few operations on clients asynchronously. */
    clientsCron(IDX_EVENT_LOOP_MAIN);

    /* Handle background operations on Redis databases. */
    databasesCron(true /* fMainThread */);

    /* Start a scheduled AOF rewrite if this was requested by the user while
     * a BGSAVE was in progress. */
    if (!hasActiveChildProcess() &&
        g_pserver->aof_rewrite_scheduled)
    {
        rewriteAppendOnlyFileBackground();
    }

    /* Check if a background saving or AOF rewrite in progress terminated. */
    if (hasActiveChildProcess() || ldbPendingChildren())
    {
        run_with_period(1000) receiveChildInfo();
        checkChildrenDone();
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
                // Ensure rehashing is complete
                bool fRehashInProgress = false;
                if (g_pserver->activerehashing) {
                    for (int idb = 0; idb < cserver.dbnum && !fRehashInProgress; ++idb) {
                        if (g_pserver->db[idb]->FRehashing())
                            fRehashInProgress = true;
                    }
                }

                if (!fRehashInProgress) {
                    serverLog(LL_NOTICE,"%d changes in %d seconds. Saving...",
                        sp->changes, (int)sp->seconds);
                    rdbSaveInfo rsi, *rsiptr;
                    rsiptr = rdbPopulateSaveInfo(&rsi);
                    rdbSaveBackground(rsiptr);
                }
                break;
            }
        }

        /* Trigger an AOF rewrite if needed. */
        if (g_pserver->aof_state == AOF_ON &&
            !hasActiveChildProcess() &&
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
    /* Just for the sake of defensive programming, to avoid forgeting to
     * call this function when need. */
    updateDictResizePolicy();


    /* AOF postponed flush: Try at every cron cycle if the slow fsync
     * completed. */
    if (g_pserver->aof_state == AOF_ON && g_pserver->aof_flush_postponed_start)
        flushAppendOnlyFile(0);

    /* AOF write errors: in this case we have a buffer to flush as well and
     * clear the AOF error in case of success to make the DB writable again,
     * however to try every second is enough in case of 'hz' is set to
     * a higher frequency. */
    run_with_period(1000) {
        if (g_pserver->aof_state == AOF_ON && g_pserver->aof_last_write_status == C_ERR)
            flushAppendOnlyFile(0);
    }

    /* Clear the paused clients state if needed. */
    checkClientPauseTimeoutAndReturnIfPaused();

    /* Replication cron function -- used to reconnect to master,
     * detect transfer failures, start background RDB transfers and so forth. 
     * 
     * If Redis is trying to failover then run the replication cron faster so
     * progress on the handshake happens more quickly. */
    if (g_pserver->failover_state != NO_FAILOVER) {
        run_with_period(100) replicationCron();
    } else {
        run_with_period(1000) replicationCron();
    }

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

    run_with_period(30000) {
        /* Tune the fastlock to CPU load */
        fastlock_auto_adjust_waits();
    }

    /* Reload the TLS cert if neccessary. This effectively rotates the 
     * cert if a change has been made on disk, but the KeyDB server hasn't
     * been notified. */
    run_with_period(1000){
        tlsReload();
    }

    /* Resize tracking keys table if needed. This is also done at every
     * command execution, but we want to be sure that if the last command
     * executed changes the value via CONFIG SET, the server will perform
     * the operation even if completely idle. */
    if (g_pserver->tracking_clients) trackingLimitUsedSlots();

    /* Start a scheduled BGSAVE if the corresponding flag is set. This is
     * useful when we are forced to postpone a BGSAVE because an AOF
     * rewrite is in progress.
     *
     * Note: this code must be after the replicationCron() call above so
     * make sure when refactoring this file to keep this order. This is useful
     * because we want to give priority to RDB savings for replication. */
    if (!hasActiveChildProcess() &&
        g_pserver->rdb_bgsave_scheduled &&
        (g_pserver->unixtime-g_pserver->lastbgsave_try > CONFIG_BGSAVE_RETRY_DELAY ||
         g_pserver->lastbgsave_status == C_OK))
    {
        rdbSaveInfo rsi, *rsiptr;
        rsiptr = rdbPopulateSaveInfo(&rsi);
        if (rdbSaveBackground(rsiptr) == C_OK)
            g_pserver->rdb_bgsave_scheduled = 0;
    }

    if (cserver.storage_memory_model == STORAGE_WRITEBACK && g_pserver->m_pstorageFactory && !g_pserver->loading) {
        run_with_period(g_pserver->storage_flush_period) {
            flushStorageWeak();
        }
    }

    /* Fire the cron loop modules event. */
    RedisModuleCronLoopV1 ei = {REDISMODULE_CRON_LOOP_VERSION,g_pserver->hz};
    moduleFireServerEvent(REDISMODULE_EVENT_CRON_LOOP,
                          0,
                          &ei);


    /* CRON functions may trigger async writes, so do this last */
    ProcessPendingAsyncWrites();

    // Measure lock contention from a different thread to be more accurate
    g_pserver->asyncworkqueue->AddWorkFunction([]{
        g_pserver->rglockSamples[g_pserver->ilockRingHead] = (uint16_t)aeLockContention();
        ++g_pserver->ilockRingHead;
        if (g_pserver->ilockRingHead >= redisServer::s_lockContentionSamples)
            g_pserver->ilockRingHead = 0;
    });

    run_with_period(10) {
        if (!g_pserver->garbageCollector.empty()) {
            // Server threads don't free the GC, but if we don't have a
            //  a bgsave or some other async task then we'll hold onto the
            //  data for too long
            g_pserver->asyncworkqueue->AddWorkFunction([]{
                auto epoch = g_pserver->garbageCollector.startEpoch();
                g_pserver->garbageCollector.endEpoch(epoch);
            });
        }
    }

    g_pserver->cronloops++;
    return 1000/g_pserver->hz;
}

// serverCron for worker threads other than the main thread
int serverCronLite(struct aeEventLoop *eventLoop, long long id, void *clientData)
{
    UNUSED(id);
    UNUSED(clientData);

    if (g_pserver->maxmemory && g_pserver->m_pstorageFactory)
        performEvictions(false);

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

    /* Handle background operations on Redis databases. */
    databasesCron(false /* fMainThread */);

    /* Unpause clients if enough time has elapsed */
    checkClientPauseTimeoutAndReturnIfPaused();
    
    ProcessPendingAsyncWrites();    // A bug but leave for now, events should clean up after themselves
    clientsCron(iel);

    return 1000/g_pserver->hz;
}

extern "C" void asyncFreeDictTable(dictEntry **de)
{
    if (de == nullptr || serverTL == nullptr || serverTL->gcEpoch.isReset()) {
        zfree(de);
    } else {
        g_pserver->garbageCollector.enqueueCPtr(serverTL->gcEpoch, de);
    }
}

void blockingOperationStarts() {
    if(!g_pserver->blocking_op_nesting++){
        __atomic_load(&g_pserver->mstime, &g_pserver->blocked_last_cron, __ATOMIC_ACQUIRE);
    }
}

void blockingOperationEnds() {
    if(!(--g_pserver->blocking_op_nesting)){
        g_pserver->blocked_last_cron = 0;
    }
}

/* This function fill in the role of serverCron during RDB or AOF loading, and
 * also during blocked scripts.
 * It attempts to do its duties at a similar rate as the configured g_pserver->hz,
 * and updates cronloops variable so that similarly to serverCron, the
 * run_with_period can be used. */
void whileBlockedCron() {
    /* Here we may want to perform some cron jobs (normally done g_pserver->hz times
     * per second). */

    /* Since this function depends on a call to blockingOperationStarts, let's
     * make sure it was done. */
    serverAssert(g_pserver->blocked_last_cron);

    /* In case we where called too soon, leave right away. This way one time
     * jobs after the loop below don't need an if. and we don't bother to start
     * latency monitor if this function is called too often. */
    if (g_pserver->blocked_last_cron >= g_pserver->mstime)
        return;

    mstime_t latency;
    latencyStartMonitor(latency);

    /* In some cases we may be called with big intervals, so we may need to do
     * extra work here. This is because some of the functions in serverCron rely
     * on the fact that it is performed every 10 ms or so. For instance, if
     * activeDefragCycle needs to utilize 25% cpu, it will utilize 2.5ms, so we
     * need to call it multiple times. */
    long hz_ms = 1000/g_pserver->hz;
    while (g_pserver->blocked_last_cron < g_pserver->mstime) {

        /* Defrag keys gradually. */
        activeDefragCycle();

        g_pserver->blocked_last_cron += hz_ms;

        /* Increment cronloop so that run_with_period works. */
        g_pserver->cronloops++;
    }

    /* Other cron jobs do not need to be done in a loop. No need to check
     * g_pserver->blocked_last_cron since we have an early exit at the top. */

    /* Update memory stats during loading (excluding blocked scripts) */
    if (g_pserver->loading) cronUpdateMemoryStats();

    latencyEndMonitor(latency);
    latencyAddSampleIfNeeded("while-blocked-cron",latency);
}

extern __thread int ProcessingEventsWhileBlocked;

/* This function gets called every time Redis is entering the
 * main loop of the event driven library, that is, before to sleep
 * for ready file descriptors.
 *
 * Note: This function is (currently) called from two functions:
 * 1. aeMain - The main server loop
 * 2. processEventsWhileBlocked - Process clients during RDB/AOF load
 *
 * If it was called from processEventsWhileBlocked we don't want
 * to perform all actions (For example, we don't want to expire
 * keys), but we do need to perform some actions.
 *
 * The most important is freeClientsInAsyncFreeQueue but we also
 * call some other low-risk functions. */
void beforeSleep(struct aeEventLoop *eventLoop) {
    AeLocker locker;
    int iel = ielFromEventLoop(eventLoop);

    locker.arm();

    /* end any snapshots created by fast async commands */
    for (int idb = 0; idb < cserver.dbnum; ++idb) {
        if (serverTL->rgdbSnapshot[idb] != nullptr && serverTL->rgdbSnapshot[idb]->FStale()) {
            g_pserver->db[idb]->endSnapshot(serverTL->rgdbSnapshot[idb]);
            serverTL->rgdbSnapshot[idb] = nullptr;
        }
    }

    size_t zmalloc_used = zmalloc_used_memory();
    if (zmalloc_used > g_pserver->stat_peak_memory)
        g_pserver->stat_peak_memory = zmalloc_used;
    
    serverAssert(g_pserver->repl_batch_offStart < 0);

    runAndPropogateToReplicas(processClients);

    /* Handle precise timeouts of blocked clients. */
    handleBlockedClientsTimeout();

    /* Just call a subset of vital functions in case we are re-entering
     * the event loop from processEventsWhileBlocked(). Note that in this
     * case we keep track of the number of events we are processing, since
     * processEventsWhileBlocked() wants to stop ASAP if there are no longer
     * events to handle. */
    if (ProcessingEventsWhileBlocked) {
        uint64_t processed = 0;
        int aof_state = g_pserver->aof_state;
        locker.disarm();
        processed += tlsProcessPendingData();
        processed += handleClientsWithPendingWrites(iel, aof_state);
        locker.arm();
        processed += freeClientsInAsyncFreeQueue(iel);
        g_pserver->events_processed_while_blocked += processed;
        return;
    }

    /* Handle precise timeouts of blocked clients. */
    handleBlockedClientsTimeout();

    /* Handle TLS pending data. (must be done before flushAppendOnlyFile) */
    if (tlsHasPendingData()) {
        locker.disarm();
        tlsProcessPendingData();
        locker.arm();
    }

    /* If tls still has pending unread data don't sleep at all. */
    aeSetDontWait(eventLoop, tlsHasPendingData());

    /* Call the Redis Cluster before sleep function. Note that this function
     * may change the state of Redis Cluster (from ok to fail or vice versa),
     * so it's a good idea to call it before serving the unblocked clients
     * later in this function. */
    if (g_pserver->cluster_enabled) clusterBeforeSleep();

    /* Run a fast expire cycle (the called function will return
     * ASAP if a fast cycle is not needed). */
    if (g_pserver->active_expire_enabled && (listLength(g_pserver->masters) == 0 || g_pserver->fActiveReplica))
        activeExpireCycle(ACTIVE_EXPIRE_CYCLE_FAST);

    /* Unblock all the clients blocked for synchronous replication
     * in WAIT. */
    if (listLength(g_pserver->clients_waiting_acks))
        processClientsWaitingReplicas();

    /* Check if there are clients unblocked by modules that implement
     * blocking commands. */
    if (moduleCount()) moduleHandleBlockedClients(ielFromEventLoop(eventLoop));

    /* Try to process pending commands for clients that were just unblocked. */
    if (listLength(g_pserver->rgthreadvar[iel].unblocked_clients))
    {
        processUnblockedClients(iel);
    }

    /* Send all the slaves an ACK request if at least one client blocked
     * during the previous event loop iteration. Note that we do this after
     * processUnblockedClients(), so if there are multiple pipelined WAITs
     * and the just unblocked WAIT gets blocked again, we don't have to wait
     * a server cron cycle in absence of other event loop events. See #6623.
     * 
     * We also don't send the ACKs while clients are paused, since it can
     * increment the replication backlog, they'll be sent after the pause
     * if we are still the master. */
    if (g_pserver->get_ack_from_slaves && !checkClientPauseTimeoutAndReturnIfPaused()) {
        robj *argv[3];

        argv[0] = shared.replconf;
        argv[1] = shared.getack;
        argv[2] = shared.special_asterick; /* Not used argument. */
        replicationFeedSlaves(g_pserver->slaves, g_pserver->replicaseldb, argv, 3);
        g_pserver->get_ack_from_slaves = 0;
    }

    /* We may have recieved updates from clients about their current offset. NOTE:
     * this can't be done where the ACK is recieved since failover will disconnect 
     * our clients. */
    if (iel == IDX_EVENT_LOOP_MAIN)
        updateFailoverStatus();

    /* Send the invalidation messages to clients participating to the
     * client side caching protocol in broadcasting (BCAST) mode. */
    trackingBroadcastInvalidationMessages();

    /* Write the AOF buffer on disk */
    if (g_pserver->aof_state == AOF_ON)
        flushAppendOnlyFile(0);

    static thread_local bool fFirstRun = true;
    // note: we also copy the DB pointer in case a DB swap is done while the lock is released
    std::vector<redisDb*> vecdb;    // note we cache the database pointer in case a dbswap is done while the lock is released
    if (cserver.storage_memory_model == STORAGE_WRITETHROUGH && g_pserver->m_pstorageFactory != nullptr && !g_pserver->loading)
    {
        if (!fFirstRun) {
            mstime_t storage_process_latency;
            latencyStartMonitor(storage_process_latency);
            for (int idb = 0; idb < cserver.dbnum; ++idb) {
                if (g_pserver->db[idb]->processChanges(false))
                    vecdb.push_back(g_pserver->db[idb]);
            }
            latencyEndMonitor(storage_process_latency);
            latencyAddSampleIfNeeded("storage-process-changes", storage_process_latency);
        } else {
            fFirstRun = false;
        }
    }

    int aof_state = g_pserver->aof_state;

    mstime_t commit_latency;
    latencyStartMonitor(commit_latency);
    if (g_pserver->m_pstorageFactory != nullptr)
    {
        locker.disarm();
        for (redisDb *db : vecdb)
            db->commitChanges();
        locker.arm();
    }
    latencyEndMonitor(commit_latency);
    latencyAddSampleIfNeeded("storage-commit", commit_latency);

    /* We try to handle writes at the end so we don't have to reacquire the lock,
        but if there is a pending async close we need to ensure the writes happen
        first so perform it here */
    bool fSentReplies = false;

    std::unique_lock<fastlock> ul(g_lockasyncfree);
    if (listLength(g_pserver->clients_to_close)) {
        ul.unlock();
        locker.disarm();
        handleClientsWithPendingWrites(iel, aof_state);
        locker.arm();
        fSentReplies = true;
    } else {
        ul.unlock();
    }
    
    if (!serverTL->gcEpoch.isReset())
        g_pserver->garbageCollector.endEpoch(serverTL->gcEpoch, true /*fNoFree*/);
    serverTL->gcEpoch.reset();

    /* Close clients that need to be closed asynchronous */
    freeClientsInAsyncFreeQueue(iel);

    if (!serverTL->gcEpoch.isReset())
        g_pserver->garbageCollector.endEpoch(serverTL->gcEpoch, true /*fNoFree*/);
    serverTL->gcEpoch.reset();

    /* Try to process blocked clients every once in while. Example: A module
     * calls RM_SignalKeyAsReady from within a timer callback (So we don't
     * visit processCommand() at all). */
    handleClientsBlockedOnKeys();

    /* Before we are going to sleep, let the threads access the dataset by
     * releasing the GIL. Redis main thread will not touch anything at this
     * time. */
    serverAssert(g_pserver->repl_batch_offStart < 0);
    locker.disarm();
    if (!fSentReplies)
        handleClientsWithPendingWrites(iel, aof_state);

    aeThreadOffline();
    // Scope lock_guard
    {
        std::unique_lock<fastlock> lock(time_thread_lock);
        sleeping_threads++;
        serverAssert(sleeping_threads <= cserver.cthreads);
    }

    if (!g_pserver->garbageCollector.empty()) {
        // Server threads don't free the GC, but if we don't have a
        //  a bgsave or some other async task then we'll hold onto the
        //  data for too long
        g_pserver->asyncworkqueue->AddWorkFunction([]{
            auto epoch = g_pserver->garbageCollector.startEpoch();
            g_pserver->garbageCollector.endEpoch(epoch);
        }, true /*fHiPri*/);
    }
    
    /* Determine whether the modules are enabled before sleeping, and use that result
       both here, and after wakeup to avoid double acquire or release of the GIL */
    serverTL->modulesEnabledThisAeLoop = !!moduleCount();
    if (serverTL->modulesEnabledThisAeLoop) moduleReleaseGIL(TRUE /*fServerThread*/);

    /* Do NOT add anything below moduleReleaseGIL !!! */
}

/* This function is called immediately after the event loop multiplexing
 * API returned, and the control is going to soon return to Redis by invoking
 * the different events callbacks. */
void afterSleep(struct aeEventLoop *eventLoop) {
    UNUSED(eventLoop);
    /* Do NOT add anything above moduleAcquireGIL !!! */

    /* Aquire the modules GIL so that their threads won't touch anything. 
       Don't check here that modules are enabled, rather use the result from beforeSleep
       Otherwise you may double acquire the GIL and cause deadlocks in the module */
    if (!ProcessingEventsWhileBlocked) {
        if (serverTL->modulesEnabledThisAeLoop) moduleAcquireGIL(TRUE /*fServerThread*/);
        aeThreadOnline();
        wakeTimeThread();

        serverAssert(serverTL->gcEpoch.isReset());
        serverTL->gcEpoch = g_pserver->garbageCollector.startEpoch();
        for (int idb = 0; idb < cserver.dbnum; ++idb)
            g_pserver->db[idb]->trackChanges(false);

        serverTL->disable_async_commands = false;
    }
}

/* =========================== Server initialization ======================== */

void createSharedObjects(void) {
    int j;

    /* Shared command responses */
    shared.crlf = makeObjectShared(createObject(OBJ_STRING,sdsnew("\r\n")));
    shared.ok = makeObjectShared(createObject(OBJ_STRING,sdsnew("+OK\r\n")));
    shared.emptybulk = makeObjectShared(createObject(OBJ_STRING,sdsnew("$0\r\n\r\n")));
    shared.czero = makeObjectShared(createObject(OBJ_STRING,sdsnew(":0\r\n")));
    shared.cone = makeObjectShared(createObject(OBJ_STRING,sdsnew(":1\r\n")));
    shared.emptyarray = makeObjectShared(createObject(OBJ_STRING,sdsnew("*0\r\n")));
    shared.pong = makeObjectShared(createObject(OBJ_STRING,sdsnew("+PONG\r\n")));
    shared.queued = makeObjectShared(createObject(OBJ_STRING,sdsnew("+QUEUED\r\n")));
    shared.emptyscan = makeObjectShared(createObject(OBJ_STRING,sdsnew("*2\r\n$1\r\n0\r\n*0\r\n")));
    shared.space = makeObjectShared(createObject(OBJ_STRING,sdsnew(" ")));
    shared.colon = makeObjectShared(createObject(OBJ_STRING,sdsnew(":")));
    shared.plus = makeObjectShared(createObject(OBJ_STRING,sdsnew("+")));
    shared.nullbulk = makeObjectShared(createObject(OBJ_STRING,sdsnew("$0\r\n\r\n")));
    
    /* Shared command error responses */   
    shared.wrongtypeerr = makeObjectShared(createObject(OBJ_STRING,sdsnew(
        "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n")));
    shared.err = makeObjectShared(createObject(OBJ_STRING,sdsnew("-ERR\r\n")));
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
        "-LOADING KeyDB is loading the dataset in memory\r\n")));
    shared.slowscripterr = makeObjectShared(createObject(OBJ_STRING,sdsnew(
        "-BUSY KeyDB is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.\r\n")));
    shared.masterdownerr = makeObjectShared(createObject(OBJ_STRING,sdsnew(
        "-MASTERDOWN Link with MASTER is down and replica-serve-stale-data is set to 'no'.\r\n")));
    shared.bgsaveerr = makeObjectShared(createObject(OBJ_STRING,sdsnew(
        "-MISCONF KeyDB is configured to save RDB snapshots, but it is currently not able to persist on disk. Commands that may modify the data set are disabled, because this instance is configured to report errors during writes if RDB snapshotting fails (stop-writes-on-bgsave-error option). Please check the KeyDB logs for details about the RDB error.\r\n")));
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
    

    /* The shared NULL depends on the protocol version. */
    shared.null[0] = NULL;
    shared.null[1] = NULL;
    shared.null[2] = makeObjectShared(createObject(OBJ_STRING,sdsnew("$-1\r\n")));
    shared.null[3] = makeObjectShared(createObject(OBJ_STRING,sdsnew("_\r\n")));

    shared.nullarray[0] = NULL;
    shared.nullarray[1] = NULL;
    shared.nullarray[2] = makeObjectShared(createObject(OBJ_STRING,sdsnew("*-1\r\n")));
    shared.nullarray[3] = makeObjectShared(createObject(OBJ_STRING,sdsnew("_\r\n")));

    shared.emptymap[0] = NULL;
    shared.emptymap[1] = NULL;
    shared.emptymap[2] = createObject(OBJ_STRING,sdsnew("*0\r\n"));
    shared.emptymap[3] = createObject(OBJ_STRING,sdsnew("%0\r\n"));

    shared.emptyset[0] = NULL;
    shared.emptyset[1] = NULL;
    shared.emptyset[2] = createObject(OBJ_STRING,sdsnew("*0\r\n"));
    shared.emptyset[3] = createObject(OBJ_STRING,sdsnew("~0\r\n"));

    for (j = 0; j < PROTO_SHARED_SELECT_CMDS; j++) {
        char dictid_str[64];
        int dictid_len;

        dictid_len = ll2string(dictid_str,sizeof(dictid_str),j);
        shared.select[j] = makeObjectShared(createObject(OBJ_STRING,
            sdscatprintf(sdsempty(),
                "*2\r\n$6\r\nSELECT\r\n$%d\r\n%s\r\n",
                dictid_len, dictid_str)));
    }
    shared.messagebulk = makeObjectShared("$7\r\nmessage\r\n",13);
    shared.pmessagebulk = makeObjectShared("$8\r\npmessage\r\n",14);
    shared.subscribebulk = makeObjectShared("$9\r\nsubscribe\r\n",15);
    shared.unsubscribebulk = makeObjectShared("$11\r\nunsubscribe\r\n",18);
    shared.psubscribebulk = makeObjectShared("$10\r\npsubscribe\r\n",17);
    shared.punsubscribebulk = makeObjectShared("$12\r\npunsubscribe\r\n",19);

    /* Shared command names */
    shared.del = makeObjectShared("DEL",3);
    shared.unlink = makeObjectShared("UNLINK",6);
    shared.rpop = makeObjectShared("RPOP",4);
    shared.lpop = makeObjectShared("LPOP",4);
    shared.lpush = makeObjectShared("LPUSH",5);
    shared.rpoplpush = makeObjectShared("RPOPLPUSH",9);
    shared.lmove = makeObjectShared("LMOVE",5);
    shared.blmove = makeObjectShared("BLMOVE",6);
    shared.zpopmin = makeObjectShared("ZPOPMIN",7);
    shared.zpopmax = makeObjectShared("ZPOPMAX",7);
    shared.multi = makeObjectShared("MULTI",5);
    shared.exec = makeObjectShared("EXEC",4);
    shared.hset = makeObjectShared("HSET",4);
    shared.srem = makeObjectShared("SREM",4);
    shared.xgroup = makeObjectShared("XGROUP",6);
    shared.xclaim = makeObjectShared("XCLAIM",6);
    shared.script = makeObjectShared("SCRIPT",6);
    shared.replconf = makeObjectShared("REPLCONF",8);
    shared.pexpireat = makeObjectShared("PEXPIREAT",9);
    shared.pexpire = makeObjectShared("PEXPIRE",7);
    shared.persist = makeObjectShared("PERSIST",7);
    shared.set = makeObjectShared("SET",3);
    shared.eval = makeObjectShared("EVAL",4);

    /* Shared command argument */
    shared.left = makeObjectShared("left",4);
    shared.right = makeObjectShared("right",5);
    shared.pxat = makeObjectShared("PXAT", 4);
    shared.px = makeObjectShared("PX",2);
    shared.time = makeObjectShared("TIME",4);
    shared.retrycount = makeObjectShared("RETRYCOUNT",10);
    shared.force = makeObjectShared("FORCE",5);
    shared.justid = makeObjectShared("JUSTID",6);
    shared.lastid = makeObjectShared("LASTID",6);
    shared.default_username = makeObjectShared("default",7);
    shared.ping = makeObjectShared("ping",4);
    shared.replping = makeObjectShared("replping", 8);
    shared.setid = makeObjectShared("SETID",5);
    shared.keepttl = makeObjectShared("KEEPTTL",7);
    shared.load = makeObjectShared("LOAD",4);
    shared.createconsumer = makeObjectShared("CREATECONSUMER",14);
    shared.getack = makeObjectShared("GETACK",6);
    shared.special_asterick = makeObjectShared("*",1);
    shared.special_equals = makeObjectShared("=",1);
    shared.redacted = makeObjectShared("(redacted)",10);

    /* KeyDB Specific */
    shared.hdel = makeObjectShared(createStringObject("HDEL", 4));
    shared.zrem = makeObjectShared(createStringObject("ZREM", 4));
    shared.mvccrestore = makeObjectShared(createStringObject("KEYDB.MVCCRESTORE", 17));
    shared.pexpirememberat = makeObjectShared(createStringObject("PEXPIREMEMBERAT",15));

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
        master->masterauth = sdsdup(cserver.default_masterauth);
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
    
    master->isActive = false;

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
    g_pserver->hz = CONFIG_DEFAULT_HZ; /* Initialize it ASAP, even if it may get
                                      updated later after loading the config.
                                      This value may be used before the server
                                      is initialized. */
    g_pserver->clients = listCreate();
    g_pserver->slaves = listCreate();
    g_pserver->monitors = listCreate();
    g_pserver->clients_timeout_table = raxNew();
    g_pserver->events_processed_while_blocked = 0;
    g_pserver->timezone = getTimeZone(); /* Initialized by tzset(). */
    cserver.configfile = NULL;
    cserver.executable = NULL;
    g_pserver->hz = g_pserver->config_hz = CONFIG_DEFAULT_HZ;
    g_pserver->bindaddr_count = 0;
    g_pserver->unixsocket = NULL;
    g_pserver->unixsocketperm = CONFIG_DEFAULT_UNIX_SOCKET_PERM;
    g_pserver->sofd = -1;
    g_pserver->active_expire_enabled = 1;
    cserver.skip_checksum_validation = 0;
    g_pserver->saveparams = NULL;
    g_pserver->loading = 0;
    g_pserver->loading_rdb_used_mem = 0;
    g_pserver->logfile = zstrdup(CONFIG_DEFAULT_LOGFILE);
    g_pserver->syslog_facility = LOG_LOCAL0;
    cserver.supervised = 0;
    cserver.supervised_mode = SUPERVISED_NONE;
    g_pserver->aof_state = AOF_OFF;
    g_pserver->aof_rewrite_base_size = 0;
    g_pserver->aof_rewrite_scheduled = 0;
    g_pserver->aof_flush_sleep = 0;
    g_pserver->aof_last_fsync = time(NULL);
    atomicSet(g_pserver->aof_bio_fsync_status,C_OK);
    g_pserver->aof_rewrite_time_last = -1;
    g_pserver->aof_rewrite_time_start = -1;
    g_pserver->aof_lastbgrewrite_status = C_OK;
    g_pserver->aof_delayed_fsync = 0;
    g_pserver->aof_fd = -1;
    g_pserver->aof_selected_db = -1; /* Make sure the first time will not match */
    g_pserver->aof_flush_postponed_start = 0;
    cserver.pidfile = NULL;
    g_pserver->rdb_filename = NULL;
    g_pserver->rdb_s3bucketpath = NULL;
    g_pserver->active_defrag_running = 0;
    g_pserver->notify_keyspace_events = 0;
    g_pserver->blocked_clients = 0;
    memset(g_pserver->blocked_clients_by_type,0,
           sizeof(g_pserver->blocked_clients_by_type));
    g_pserver->shutdown_asap = 0;
    g_pserver->cluster_enabled = 0;
    g_pserver->cluster_configfile = zstrdup(CONFIG_DEFAULT_CLUSTER_CONFIG_FILE);
    g_pserver->migrate_cached_sockets = dictCreate(&migrateCacheDictType,NULL);
    g_pserver->next_client_id = 1; /* Client IDs, start from 1 .*/

    g_pserver->lruclock = getLRUClock();
    resetServerSaveParams();

    appendServerSaveParams(60*60,1);  /* save after 1 hour and 1 change */
    appendServerSaveParams(300,100);  /* save after 5 minutes and 100 changes */
    appendServerSaveParams(60,10000); /* save after 1 minute and 10000 changes */

    /* Replication related */
    g_pserver->masters = listCreate();
    g_pserver->enable_multimaster = CONFIG_DEFAULT_ENABLE_MULTIMASTER;
    g_pserver->repl_syncio_timeout = CONFIG_REPL_SYNCIO_TIMEOUT;
    g_pserver->master_repl_offset = 0;
    g_pserver->repl_lowest_off.store(-1, std::memory_order_seq_cst);

    /* Replication partial resync backlog */
    g_pserver->repl_backlog = NULL;
    g_pserver->repl_backlog_histlen = 0;
    g_pserver->repl_backlog_idx = 0;
    g_pserver->repl_backlog_off = 0;
    g_pserver->repl_no_slaves_since = time(NULL);

    /* Failover related */
    g_pserver->failover_end_time = 0;
    g_pserver->force_failover = 0;
    g_pserver->target_replica_host = NULL;
    g_pserver->target_replica_port = 0;
    g_pserver->failover_state = NO_FAILOVER;

    /* Client output buffer limits */
    for (j = 0; j < CLIENT_TYPE_OBUF_COUNT; j++)
        cserver.client_obuf_limits[j] = clientBufferLimitsDefaults[j];

    /* Linux OOM Score config */
    for (j = 0; j < CONFIG_OOM_COUNT; j++)
        g_pserver->oom_score_adj_values[j] = configOOMScoreAdjValuesDefaults[j];

    /* Double constants initialization */
    R_Zero = 0.0;
    R_PosInf = 1.0/R_Zero;
    R_NegInf = -1.0/R_Zero;
    R_Nan = R_Zero/R_Zero;

    /* Command table -- we initialize it here as it is part of the
     * initial configuration, since command names may be changed via
     * keydb.conf using the rename-command directive. */
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
    cserver.rpoplpushCommand = lookupCommandByCString("rpoplpush");
    cserver.hdelCommand = lookupCommandByCString("hdel");
    cserver.zremCommand = lookupCommandByCString("zrem");
    cserver.lmoveCommand = lookupCommandByCString("lmove");

    /* Debugging */
    g_pserver->watchdog_period = 0;

    /* By default we want scripts to be always replicated by effects
     * (single commands executed by the script), and not by sending the
     * script to the replica / AOF. This is the new way starting from
     * Redis 5. However it is possible to revert it via keydb.conf. */
    g_pserver->lua_always_replicate_commands = 1;

    /* Multithreading */
    cserver.cthreads = CONFIG_DEFAULT_THREADS;
    cserver.fThreadAffinity = CONFIG_DEFAULT_THREAD_AFFINITY;

    // This will get dereferenced before the second stage init where we have the true db count
    //  so make sure its zero and initialized
    g_pserver->db = (redisDb**)zcalloc(sizeof(redisDb*)*std::max(cserver.dbnum, 1), MALLOC_LOCAL);

    cserver.threadAffinityOffset = 0;

    /* Client Pause related */
    g_pserver->client_pause_type = CLIENT_PAUSE_OFF;
    g_pserver->client_pause_end_time = 0; 
    initConfigValues();
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
        rewriteConfig(cserver.configfile, 0) == -1)
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
        if (fcntl(j,F_GETFD) != -1)
        {
            /* This user to just close() here, but sanitizers detected that as an FD race.
                The race doesn't matter since we're about to call exec() however we want
                to cut down on noise, so instead we ask the kernel to close when we call
                exec(), and only do it ourselves if that fails. */
            if (fcntl(j, F_SETFD, FD_CLOEXEC) == -1)
            {
                close(j);   // failed to set close on exec, close here
            }
        }
    }

    if (flags & RESTART_SERVER_GRACEFULLY) {
        if (g_pserver->m_pstorageFactory) {
            for (int idb = 0; idb < cserver.dbnum; ++idb) {
                g_pserver->db[idb]->storageProviderDelete();
            }
            delete g_pserver->metadataDb;
        }
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

static void readOOMScoreAdj(void) {
#ifdef HAVE_PROC_OOM_SCORE_ADJ
    char buf[64];
    int fd = open("/proc/self/oom_score_adj", O_RDONLY);

    if (fd < 0) return;
    if (read(fd, buf, sizeof(buf)) > 0)
        g_pserver->oom_score_adj_base = atoi(buf);
    close(fd);
#endif
}

/* This function will configure the current process's oom_score_adj according
 * to user specified configuration. This is currently implemented on Linux
 * only.
 *
 * A process_class value of -1 implies OOM_CONFIG_MASTER or OOM_CONFIG_REPLICA,
 * depending on current role.
 */
int setOOMScoreAdj(int process_class) {

    if (g_pserver->oom_score_adj == OOM_SCORE_ADJ_NO) return C_OK;
    if (process_class == -1)
        process_class = (listLength(g_pserver->masters) ? CONFIG_OOM_REPLICA : CONFIG_OOM_MASTER);

    serverAssert(process_class >= 0 && process_class < CONFIG_OOM_COUNT);

#ifdef HAVE_PROC_OOM_SCORE_ADJ
    int fd;
    int val;
    char buf[64];

    val = g_pserver->oom_score_adj_values[process_class];
    if (g_pserver->oom_score_adj == OOM_SCORE_RELATIVE)
        val += g_pserver->oom_score_adj_base;
    if (val > 1000) val = 1000;
    if (val < -1000) val = -1000;

    snprintf(buf, sizeof(buf) - 1, "%d\n", val);

    fd = open("/proc/self/oom_score_adj", O_WRONLY);
    if (fd < 0 || write(fd, buf, strlen(buf)) < 0) {
        serverLog(LL_WARNING, "Unable to write oom_score_adj: %s", strerror(errno));
        if (fd != -1) close(fd);
        return C_ERR;
    }

    close(fd);
    return C_OK;
#else
    /* Unsupported */
    return C_ERR;
#endif
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
    if (g_pserver->m_pstorageFactory)
        maxfiles += g_pserver->m_pstorageFactory->filedsRequired();
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

void closeSocketListeners(socketFds *sfd) {
    int j;

    for (j = 0; j < sfd->count; j++) {
        if (sfd->fd[j] == -1) continue;

        aeDeleteFileEvent(serverTL->el, sfd->fd[j], AE_READABLE);
        close(sfd->fd[j]);
    }

    sfd->count = 0;
}

/* Create an event handler for accepting new connections in TCP or TLS domain sockets.
 * This works atomically for all socket fds */
int createSocketAcceptHandler(socketFds *sfd, aeFileProc *accept_handler) {
    int j;

    for (j = 0; j < sfd->count; j++) {
        if (aeCreateFileEvent(serverTL->el, sfd->fd[j], AE_READABLE, accept_handler,NULL) == AE_ERR) {
            /* Rollback */
            for (j = j-1; j >= 0; j--) aeDeleteFileEvent(serverTL->el, sfd->fd[j], AE_READABLE);
            return C_ERR;
        }
    }
    return C_OK;
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
int listenToPort(int port, socketFds *sfd, int fReusePort, int fFirstListen) {
    int j;
    const char **bindaddr = (const char**)g_pserver->bindaddr;
    int bindaddr_count = g_pserver->bindaddr_count;
    const char *default_bindaddr[2] = {"*", "-::*"};

    /* Force binding of 0.0.0.0 if no bind address is specified. */
    if (g_pserver->bindaddr_count == 0) {
        bindaddr_count = 2;
        bindaddr = default_bindaddr;
    }

    for (j = 0; j < bindaddr_count; j++) {
        const char* addr = bindaddr[j];
        int optional = *addr == '-';
        if (optional) addr++;
        if (strchr(addr,':')) {
            /* Bind IPv6 address. */
            sfd->fd[sfd->count] = anetTcp6Server(serverTL->neterr,port,addr,g_pserver->tcp_backlog,fReusePort,fFirstListen);
        } else {
            /* Bind IPv4 address. */
            sfd->fd[sfd->count] = anetTcpServer(serverTL->neterr,port,addr,g_pserver->tcp_backlog,fReusePort,fFirstListen);
        }
        if (sfd->fd[sfd->count] == ANET_ERR) {
            int net_errno = errno;
            serverLog(LL_WARNING,
                "Warning: Could not create server TCP listening socket %s:%d: %s",
                addr, port, serverTL->neterr);
            if (net_errno == EADDRNOTAVAIL && optional)
                continue;
            if (net_errno == ENOPROTOOPT     || net_errno == EPROTONOSUPPORT ||
                net_errno == ESOCKTNOSUPPORT || net_errno == EPFNOSUPPORT ||
                net_errno == EAFNOSUPPORT)
                continue;

            /* Rollback successful listens before exiting */
            closeSocketListeners(sfd);
            return C_ERR;
        }
        anetNonBlock(NULL,sfd->fd[sfd->count]);
        anetCloexec(sfd->fd[sfd->count]);
        sfd->count++;
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
    g_pserver->stat_expire_cycle_time_used = 0;
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
    g_pserver->stat_total_forks = 0;
    g_pserver->stat_rejected_conn = 0;
    g_pserver->stat_sync_full = 0;
    g_pserver->stat_sync_partial_ok = 0;
    g_pserver->stat_sync_partial_err = 0;
    g_pserver->stat_total_reads_processed = 0;
    g_pserver->stat_total_writes_processed = 0;
    for (j = 0; j < STATS_METRIC_COUNT; j++) {
        g_pserver->inst_metric[j].idx = 0;
        g_pserver->inst_metric[j].last_sample_time = mstime();
        g_pserver->inst_metric[j].last_sample_count = 0;
        memset(g_pserver->inst_metric[j].samples,0,
            sizeof(g_pserver->inst_metric[j].samples));
    }
    g_pserver->stat_net_input_bytes = 0;
    g_pserver->stat_net_output_bytes = 0;
    g_pserver->stat_unexpected_error_replies = 0;
    for (int iel = 0; iel < cserver.cthreads; ++iel)
        g_pserver->rgthreadvar[iel].stat_total_error_replies = 0;
    g_pserver->stat_dump_payload_sanitizations = 0;
    g_pserver->aof_delayed_fsync = 0;
}

/* Make the thread killable at any time, so that kill threads functions
 * can work reliably (default cancelability type is PTHREAD_CANCEL_DEFERRED).
 * Needed for pthread_cancel used by the fast memory test used by the crash report. */
void makeThreadKillable(void) {
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);
}

static void initNetworkingThread(int iel, int fReusePort)
{
    /* Open the TCP listening socket for the user commands. */
    if (fReusePort || (iel == IDX_EVENT_LOOP_MAIN))
    {
        if (g_pserver->port != 0 &&
            listenToPort(g_pserver->port,&g_pserver->rgthreadvar[iel].ipfd, fReusePort, (iel == IDX_EVENT_LOOP_MAIN)) == C_ERR) {
            serverLog(LL_WARNING, "Failed listening on port %u (TCP), aborting.", g_pserver->port);
            exit(1);
        }
        if (g_pserver->tls_port != 0 &&
            listenToPort(g_pserver->tls_port,&g_pserver->rgthreadvar[iel].tlsfd, fReusePort, (iel == IDX_EVENT_LOOP_MAIN)) == C_ERR) {
            serverLog(LL_WARNING, "Failed listening on port %u (TLS), aborting.", g_pserver->port);
            exit(1);
        }
    }
    else
    {
        // We use the main threads file descriptors
        memcpy(&g_pserver->rgthreadvar[iel].ipfd, &g_pserver->rgthreadvar[IDX_EVENT_LOOP_MAIN].ipfd, sizeof(socketFds));
        g_pserver->rgthreadvar[iel].ipfd.count = g_pserver->rgthreadvar[IDX_EVENT_LOOP_MAIN].ipfd.count;
    }

    /* Create an event handler for accepting new connections in TCP */
    for (int j = 0; j < g_pserver->rgthreadvar[iel].ipfd.count; j++) {
        if (aeCreateFileEvent(g_pserver->rgthreadvar[iel].el, g_pserver->rgthreadvar[iel].ipfd.fd[j], AE_READABLE|AE_READ_THREADSAFE,
            acceptTcpHandler,NULL) == AE_ERR)
            {
                serverPanic(
                    "Unrecoverable error creating g_pserver->ipfd file event.");
            }
    }

    makeThreadKillable();

    for (int j = 0; j < g_pserver->rgthreadvar[iel].tlsfd.count; j++) {
        if (aeCreateFileEvent(g_pserver->rgthreadvar[iel].el, g_pserver->rgthreadvar[iel].tlsfd.fd[j], AE_READABLE|AE_READ_THREADSAFE,
            acceptTLSHandler,NULL) == AE_ERR)
            {
                serverPanic(
                    "Unrecoverable error creating g_pserver->tlsfd file event.");
            }
    }
}

static void initNetworking(int fReusePort)
{
    // We only initialize the main thread here, since RDB load is a special case that processes
    //  clients before our server threads are launched.
    initNetworkingThread(IDX_EVENT_LOOP_MAIN, fReusePort);

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
    if (g_pserver->rgthreadvar[IDX_EVENT_LOOP_MAIN].ipfd.count == 0 && g_pserver->rgthreadvar[IDX_EVENT_LOOP_MAIN].tlsfd.count == 0 && g_pserver->sofd < 0) {
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
    pvar->ipfd.count = 0;
    pvar->tlsfd.count = 0;
    pvar->cclients = 0;
    pvar->in_eval = 0;
    pvar->in_exec = 0;
    pvar->el = aeCreateEventLoop(g_pserver->maxclients+CONFIG_FDSET_INCR);
    pvar->current_client = nullptr;
    pvar->fRetrySetAofEvent = false;
    if (pvar->el == NULL) {
        serverLog(LL_WARNING,
            "Failed creating the event loop. Error message: '%s'",
            strerror(errno));
        exit(1);
    }
    aeSetBeforeSleepProc(pvar->el, beforeSleep, AE_SLEEP_THREADSAFE);
    aeSetAfterSleepProc(pvar->el, afterSleep, AE_SLEEP_THREADSAFE);

    fastlock_init(&pvar->lockPendingWrite, "lockPendingWrite");

    if (!fMain)
    {
        if (aeCreateTimeEvent(pvar->el, 1, serverCronLite, NULL, NULL) == AE_ERR) {
            serverPanic("Can't create event loop timers.");
            exit(1);
        }
    }

    /* Register a readable event for the pipe used to awake the event loop
     * when a blocked client in a module needs attention. */
    if (aeCreateFileEvent(pvar->el, g_pserver->module_blocked_pipe[0], AE_READABLE,
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
    makeThreadKillable();

    zfree(g_pserver->db);   // initServerConfig created a dummy array, free that now
    g_pserver->db = (redisDb**)zmalloc(sizeof(redisDb*)*cserver.dbnum, MALLOC_LOCAL);

    /* Create the Redis databases, and initialize other internal state. */
    for (int j = 0; j < cserver.dbnum; j++) {
        g_pserver->db[j] = new (MALLOC_LOCAL) redisDb();
        g_pserver->db[j]->initialize(j);
    }

    for (int i = 0; i < MAX_EVENT_LOOPS; ++i)
    {
        g_pserver->rgthreadvar[i].rgdbSnapshot = (const redisDbPersistentDataSnapshot**)zcalloc(sizeof(redisDbPersistentDataSnapshot*)*cserver.dbnum, MALLOC_LOCAL);
        serverAssert(g_pserver->rgthreadvar[i].rgdbSnapshot != nullptr);
    }
    g_pserver->modulethreadvar.rgdbSnapshot = (const redisDbPersistentDataSnapshot**)zcalloc(sizeof(redisDbPersistentDataSnapshot*)*cserver.dbnum, MALLOC_LOCAL);
    serverAssert(g_pserver->modulethreadvar.rgdbSnapshot != nullptr);

    serverAssert(g_pserver->rgthreadvar[0].rgdbSnapshot != nullptr);

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

    g_pserver->aof_state = g_pserver->aof_enabled ? AOF_ON : AOF_OFF;
    g_pserver->hz = g_pserver->config_hz;
    cserver.pid = getpid();
    g_pserver->in_fork_child = CHILD_TYPE_NONE;
    cserver.main_thread_id = pthread_self();
    g_pserver->errors = raxNew();
    g_pserver->clients_index = raxNew();
    g_pserver->clients_to_close = listCreate();
    g_pserver->replicaseldb = -1; /* Force to emit the first SELECT command. */
    g_pserver->ready_keys = listCreate();
    g_pserver->clients_waiting_acks = listCreate();
    g_pserver->get_ack_from_slaves = 0;
    cserver.system_memory_size = zmalloc_get_memory_size();
    g_pserver->paused_clients = listCreate();
    g_pserver->events_processed_while_blocked = 0;
    g_pserver->blocked_last_cron = 0;
    g_pserver->replication_allowed = 1;
    g_pserver->blocking_op_nesting = 0;
    g_pserver->rdb_pipe_read = -1;
    g_pserver->client_pause_type = CLIENT_PAUSE_OFF;


    if ((g_pserver->tls_port || g_pserver->tls_replication || g_pserver->tls_cluster)
            && tlsConfigure(&g_pserver->tls_ctx_config) == C_ERR) {
        serverLog(LL_WARNING, "Failed to configure TLS. Check logs for more info.");
        exit(1);
    }

    createSharedObjects();
    adjustOpenFilesLimit();
    const char *clk_msg = monotonicInit();
    serverLog(LL_NOTICE, "monotonic clock: %s", clk_msg);

    evictionPoolAlloc(); /* Initialize the LRU keys pool. */
    g_pserver->pubsub_channels = dictCreate(&keylistDictType,NULL);
    g_pserver->pubsub_patterns = dictCreate(&keylistDictType,NULL);
    g_pserver->cronloops = 0;
    g_pserver->propagate_in_transaction = 0;
    g_pserver->client_pause_in_transaction = 0;
    g_pserver->child_pid = -1;
    g_pserver->child_type = CHILD_TYPE_NONE;
    g_pserver->rdbThreadVars.fRdbThreadCancel = false;
    g_pserver->rdb_child_type = RDB_CHILD_TYPE_NONE;
    g_pserver->rdb_pipe_conns = NULL;
    g_pserver->rdb_pipe_numconns = 0;
    g_pserver->rdb_pipe_numconns_writing = 0;
    g_pserver->rdb_pipe_buff = NULL;
    g_pserver->rdb_pipe_bufflen = 0;
    g_pserver->rdb_bgsave_scheduled = 0;
    g_pserver->child_info_pipe[0] = -1;
    g_pserver->child_info_pipe[1] = -1;
    g_pserver->child_info_nread = 0;
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
    g_pserver->stat_current_cow_bytes = 0;
    g_pserver->stat_current_cow_updated = 0;
    g_pserver->stat_current_save_keys_processed = 0;
    g_pserver->stat_current_save_keys_total = 0;
    g_pserver->stat_rdb_cow_bytes = 0;
    g_pserver->stat_aof_cow_bytes = 0;
    g_pserver->stat_module_cow_bytes = 0;
    g_pserver->stat_module_progress = 0;
    for (int j = 0; j < CLIENT_TYPE_COUNT; j++)
        g_pserver->stat_clients_type_memory[j] = 0;
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

    if (g_pserver->m_pstorageFactory) {
        g_pserver->metadataDb = g_pserver->m_pstorageFactory->createMetadataDb();
        if (g_pserver->metadataDb) {
            g_pserver->metadataDb->retrieve("repl-id", 7, [&](const char *, size_t, const void *data, size_t cb){
                if (cb == sizeof(g_pserver->replid)) {
                    memcpy(g_pserver->replid, data, cb);
                }
            });
            g_pserver->metadataDb->retrieve("repl-offset", 11, [&](const char *, size_t, const void *data, size_t cb){
                if (cb == sizeof(g_pserver->master_repl_offset)) {
                    g_pserver->master_repl_offset = *(long long*)data;
                }
            });

            int repl_stream_db = -1;
            g_pserver->metadataDb->retrieve("repl-stream-db", 14, [&](const char *, size_t, const void *data, size_t){
                repl_stream_db = *(int*)data;
            });

            /* !!! AFTER THIS POINT WE CAN NO LONGER READ FROM THE META DB AS IT WILL BE OVERWRITTEN !!! */
            // replicationCacheMasterUsingMyself triggers the overwrite 

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
                selectDb(mi->cached_master, repl_stream_db);
            }
        }
    }

    /* We have to initialize storage providers after the cluster has been initialized */
    for (int idb = 0; idb < cserver.dbnum; ++idb)
    {
        g_pserver->db[idb]->storageProviderInitialize();
    }

    saveMasterStatusToStorage(false); // eliminate the repl-offset field
    
    /* Initialize ACL default password if it exists */
    ACLUpdateDefaultUserPassword(g_pserver->requirepass);
}

/* Some steps in server initialization need to be done last (after modules
 * are loaded).
 * Specifically, creation of threads due to a race bug in ld.so, in which
 * Thread Local Storage initialization collides with dlopen call.
 * see: https://sourceware.org/bugzilla/show_bug.cgi?id=19329 */
void InitServerLast() {
    bioInit();
    set_jemalloc_bg_thread(cserver.jemalloc_bg_thread);
    g_pserver->initial_memory_usage = zmalloc_used_memory();

    g_pserver->asyncworkqueue = new (MALLOC_LOCAL) AsyncWorkQueue(cserver.cthreads);

    // Allocate the repl backlog
    
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
        } else if (!strcasecmp(flag,"no-slowlog")) {
            c->flags |= CMD_SKIP_SLOWLOG;
        } else if (!strcasecmp(flag,"cluster-asking")) {
            c->flags |= CMD_ASKING;
        } else if (!strcasecmp(flag,"fast")) {
            c->flags |= CMD_FAST | CMD_CATEGORY_FAST;
        } else if (!strcasecmp(flag,"noprop")) {
            c->flags |= CMD_SKIP_PROPOGATE;
        } else if (!strcasecmp(flag,"no-auth")) {
            c->flags |= CMD_NO_AUTH;
        } else if (!strcasecmp(flag,"may-replicate")) {
            c->flags |= CMD_MAY_REPLICATE;
        } else if (!strcasecmp(flag,"async")) {
            c->flags |= CMD_ASYNC_OK;
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

/* Populates the KeyDB Command Table starting from the hard coded list
 * we have on top of server.cpp file. */
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
         * by rename-command statements in keydb.conf. */
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
        c->rejected_calls = 0;
        c->failed_calls = 0;
    }
    dictReleaseIterator(di);

}

static void zfree_noconst(void *p) {
    zfree(p);
}

void fuzzOutOfMemoryHandler(size_t allocation_size) {
    serverLog(LL_WARNING,"Out Of Memory allocating %zu bytes!",
        allocation_size);
    exit(EXIT_FAILURE); // don't crash because it causes false positives
}

void resetErrorTableStats(void) {
    raxFreeWithCallback(g_pserver->errors, zfree_noconst);
    g_pserver->errors = raxNew();
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
    oa->ops = NULL;
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
 * keydb.conf rename-command statement.
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
 * This should not be used inside commands implementation since it will not
 * wrap the resulting commands in MULTI/EXEC. Use instead alsoPropagate(),
 * preventCommandPropagation(), forceCommandPropagation().
 *
 * However for functions that need to (also) propagate out of the context of a
 * command execution, for example when serving a blocked client, you
 * want to use propagate().
 */
void propagate(struct redisCommand *cmd, int dbid, robj **argv, int argc,
               int flags)
{
    serverAssert(GlobalLocksAcquired());
    if (!g_pserver->replication_allowed)
        return;

    /* Propagate a MULTI request once we encounter the first command which
     * is a write command.
     * This way we'll deliver the MULTI/..../EXEC block as a whole and
     * both the AOF and the replication link will have the same consistency
     * and atomicity guarantees. */
    if (serverTL->in_exec && !g_pserver->propagate_in_transaction)
        execCommandPropagateMulti(dbid);

    /* This needs to be unreachable since the dataset should be fixed during 
     * client pause, otherwise data may be lossed during a failover. */
    serverAssert(!(areClientsPaused() && !g_pserver->client_pause_in_transaction));

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
 * Arguments of the command to propagate are passed as an array of redis
 * objects pointers of len 'argc', using the 'argv' vector.
 *
 * The function does not take a reference to the passed 'argv' vector,
 * so it is up to the caller to release the passed argv (but it is usually
 * stack allocated).  The function automatically increments ref count of
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
    serverAssert(c->cmd->flags & (CMD_WRITE | CMD_MAY_REPLICATE));
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

/* Log the last command a client executed into the slowlog. */
void slowlogPushCurrentCommand(client *c, struct redisCommand *cmd, ustime_t duration) {
    /* Some commands may contain sensitive data that should not be available in the slowlog. */
    if (cmd->flags & CMD_SKIP_SLOWLOG)
        return;

    /* If command argument vector was rewritten, use the original
     * arguments. */
    robj **argv = c->original_argv ? c->original_argv : c->argv;
    int argc = c->original_argv ? c->original_argc : c->argc;
    slowlogPushEntryIfNeeded(c,argv,argc,duration);
}

/* Call() is the core of Redis execution of a command.
 *
 * The following flags can be passed:
 * CMD_CALL_NONE        No flags.
 * CMD_CALL_SLOWLOG     Check command speed and log in the slow log if needed.
 * CMD_CALL_STATS       Populate command stats.
 * CMD_CALL_PROPAGATE_AOF   Append command to AOF if it modified the dataset
 *                          or if the client flags are forcing propagation.
 * CMD_CALL_PROPAGATE_REPL  Send command to slaves if it modified the dataset
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
    long long dirty;
    monotime call_timer;
    int client_old_flags = c->flags;
    struct redisCommand *real_cmd = c->cmd;
    serverAssert(((flags & CMD_CALL_ASYNC) && (c->cmd->flags & CMD_READONLY)) || GlobalLocksAcquired());

    /* We need to transfer async writes before a client's repl state gets changed.  Otherwise
        we won't be able to propogate them correctly. */
    if (c->cmd->flags & CMD_CATEGORY_REPLICATION) {
        flushReplBacklogToClients();
        ProcessPendingAsyncWrites();
    }

    /* Initialization: clear the flags that must be set by the command on
     * demand, and initialize the array for additional commands propagation. */
    c->flags &= ~(CLIENT_FORCE_AOF|CLIENT_FORCE_REPL|CLIENT_PREVENT_PROP);
    redisOpArray prev_also_propagate;
    if (!(flags & CMD_CALL_ASYNC)) {
        prev_also_propagate = g_pserver->also_propagate;
        redisOpArrayInit(&g_pserver->also_propagate);
    }

    /* Call the command. */
    dirty = g_pserver->dirty;
    serverTL->prev_err_count = serverTL->stat_total_error_replies;
    g_pserver->fixed_time_expire++;
    incrementMvccTstamp();
    elapsedStart(&call_timer);
    try {
        c->cmd->proc(c);
    } catch (robj_roptr o) {
        addReply(c, o);
    } catch (robj *o) {
        addReply(c, o);
    } catch (const char *sz) {
        addReplyError(c, sz);
    }
    serverTL->commandsExecuted++;
    const long duration = elapsedUs(call_timer);
    c->duration = duration;
    if (flags & CMD_CALL_ASYNC)
        dirty = 0;  // dirty is bogus in this case as there's no synchronization
    else
        dirty = g_pserver->dirty-dirty;
    if (dirty < 0) dirty = 0;

    if (dirty)
        c->mvccCheckpoint = getMvccTstamp();

    /* Update failed command calls if required.
     * We leverage a static variable (prev_err_count) to retain
     * the counter across nested function calls and avoid logging
     * the same error twice. */
    if ((serverTL->stat_total_error_replies - serverTL->prev_err_count) > 0) {
        real_cmd->failed_calls++;
    }

    /* After executing command, we will close the client after writing entire
     * reply if it is set 'CLIENT_CLOSE_AFTER_COMMAND' flag. */
    if (c->flags & CLIENT_CLOSE_AFTER_COMMAND) {
        c->flags &= ~CLIENT_CLOSE_AFTER_COMMAND;
        c->flags |= CLIENT_CLOSE_AFTER_REPLY;
    }

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

    /* Note: the code below uses the real command that was executed
     * c->cmd and c->lastcmd may be different, in case of MULTI-EXEC or
     * re-written commands such as EXPIRE, GEOADD, etc. */

    /* Record the latency this command induced on the main thread.
     * unless instructed by the caller not to log. (happens when processing
     * a MULTI-EXEC from inside an AOF). */
    if (flags & CMD_CALL_SLOWLOG) {
        const char *latency_event = (real_cmd->flags & CMD_FAST) ?
                               "fast-command" : "command";
        latencyAddSampleIfNeeded(latency_event,duration/1000);
    }

    /* Log the command into the Slow log if needed.
     * If the client is blocked we will handle slowlog when it is unblocked. */
    if ((flags & CMD_CALL_SLOWLOG) && !(c->flags & CLIENT_BLOCKED)) {
        if (duration >= g_pserver->slowlog_log_slower_than) {
            AeLocker locker;
            locker.arm(c);
            slowlogPushCurrentCommand(c, real_cmd, duration);
        }
    }

    /* Send the command to clients in MONITOR mode if applicable.
     * Administrative commands are considered too dangerous to be shown. */
    if (!(c->cmd->flags & (CMD_SKIP_MONITOR|CMD_ADMIN))) {
        robj **argv = c->original_argv ? c->original_argv : c->argv;
        int argc = c->original_argv ? c->original_argc : c->argc;
        replicationFeedMonitors(c,g_pserver->monitors,c->db->id,argv,argc);
    }

    /* Clear the original argv.
     * If the client is blocked we will handle slowlog when it is unblocked. */
    if (!(c->flags & CLIENT_BLOCKED))
        freeClientOriginalArgv(c);

    /* populate the per-command statistics that we show in INFO commandstats. */
    if (flags & CMD_CALL_STATS) {
        __atomic_fetch_add(&real_cmd->microseconds, duration, __ATOMIC_RELAXED);
        __atomic_fetch_add(&real_cmd->calls, 1, __ATOMIC_RELAXED);
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
         * implementation called preventCommandPropagation() or similar,
         * or if we don't have the call() flags to do so. */
        if (c->flags & CLIENT_PREVENT_REPL_PROP ||
            !(flags & CMD_CALL_PROPAGATE_REPL))
                propagate_flags &= ~PROPAGATE_REPL;
        if (c->flags & CLIENT_PREVENT_AOF_PROP ||
            !(flags & CMD_CALL_PROPAGATE_AOF))
                propagate_flags &= ~PROPAGATE_AOF;

        if ((c->cmd->flags & CMD_SKIP_PROPOGATE) && g_pserver->fActiveReplica)
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

    if (!(flags & CMD_CALL_ASYNC)) {
        /* Handle the alsoPropagate() API to handle commands that want to propagate
        * multiple separated commands. Note that alsoPropagate() is not affected
        * by CLIENT_PREVENT_PROP flag. */
        if (g_pserver->also_propagate.numops) {
            int j;
            redisOp *rop;

            if (flags & CMD_CALL_PROPAGATE) {
                bool multi_emitted = false;
                /* Wrap the commands in g_pserver->also_propagate array,
                * but don't wrap it if we are already in MULTI context,
                * in case the nested MULTI/EXEC.
                *
                * And if the array contains only one command, no need to
                * wrap it, since the single command is atomic. */
                if (g_pserver->also_propagate.numops > 1 &&
                    !(c->cmd->flags & CMD_MODULE) &&
                    !(c->flags & CLIENT_MULTI) &&
                    !(flags & CMD_CALL_NOWRAP))
                {
                    execCommandPropagateMulti(c->db->id);
                    multi_emitted = true;
                }
                
                for (j = 0; j < g_pserver->also_propagate.numops; j++) {
                    rop = &g_pserver->also_propagate.ops[j];
                    int target = rop->target;
                    /* Whatever the command wish is, we honor the call() flags. */
                    if (!(flags&CMD_CALL_PROPAGATE_AOF)) target &= ~PROPAGATE_AOF;
                    if (!(flags&CMD_CALL_PROPAGATE_REPL)) target &= ~PROPAGATE_REPL;
                    if (target)
                        propagate(rop->cmd,rop->dbid,rop->argv,rop->argc,target);
                }

                if (multi_emitted) {
                    execCommandPropagateExec(c->db->id);
                }
            }
            redisOpArrayFree(&g_pserver->also_propagate);
        }
        
        g_pserver->also_propagate = prev_also_propagate;
    }

    /* Client pause takes effect after a transaction has finished. This needs
     * to be located after everything is propagated. */
    if (!serverTL->in_exec && g_pserver->client_pause_in_transaction) {
        g_pserver->client_pause_in_transaction = 0;
    }

    /* If the client has keys tracking enabled for client side caching,
     * make sure to remember the keys it fetched via this command. */
    if (c->cmd->flags & CMD_READONLY) {
        client *caller = (c->flags & CLIENT_LUA && g_pserver->lua_caller) ?
                            g_pserver->lua_caller : c;
        if (caller->flags & CLIENT_TRACKING &&
            !(caller->flags & CLIENT_TRACKING_BCAST))
        {
            trackingRememberKeys(caller);
        }
    }

    __atomic_fetch_add(&g_pserver->stat_numcommands, 1, __ATOMIC_RELAXED);
    serverTL->fixed_time_expire--;
    serverTL->prev_err_count = serverTL->stat_total_error_replies;

    if (!(flags & CMD_CALL_ASYNC)) {
        /* Record peak memory after each command and before the eviction that runs
        * before the next command. */
        size_t zmalloc_used = zmalloc_used_memory();
        if (zmalloc_used > g_pserver->stat_peak_memory)
            g_pserver->stat_peak_memory = zmalloc_used;
    }
}

/* Used when a command that is ready for execution needs to be rejected, due to
 * varios pre-execution checks. it returns the appropriate error to the client.
 * If there's a transaction is flags it as dirty, and if the command is EXEC,
 * it aborts the transaction.
 * Note: 'reply' is expected to end with \r\n */
void rejectCommand(client *c, robj *reply, int severity = ERR_CRITICAL) {
    flagTransaction(c);
    if (c->cmd) c->cmd->rejected_calls++;
    if (c->cmd && c->cmd->proc == execCommand) {
        execCommandAbort(c, szFromObj(reply));
    }
    else {
        /* using addReplyError* rather than addReply so that the error can be logged. */
        addReplyErrorObject(c, reply, severity);
    }
}

void lfenceCommand(client *c) {
    c->mvccCheckpoint = getMvccTstamp();
    addReply(c, shared.ok);
}

void rejectCommandFormat(client *c, const char *fmt, ...) {
    if (c->cmd) c->cmd->rejected_calls++;
    flagTransaction(c);
    va_list ap;
    va_start(ap,fmt);
    sds s = sdscatvprintf(sdsempty(),fmt,ap);
    va_end(ap);
    /* Make sure there are no newlines in the string, otherwise invalid protocol
     * is emitted (The args come from the user, they may contain any character). */
    sdsmapchars(s, "\r\n", "  ",  2);
    if (c->cmd && c->cmd->proc == execCommand) {
        execCommandAbort(c, s);
        sdsfree(s);
    } else {
        /* The following frees 's'. */
        addReplyErrorSds(c, s);
    }
}

/* Returns 1 for commands that may have key names in their arguments, but have
 * no pre-determined key positions. */
static int cmdHasMovableKeys(struct redisCommand *cmd) {
    return (cmd->getkeys_proc && !(cmd->flags & CMD_MODULE)) ||
            cmd->flags & CMD_MODULE_GETKEYS;
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
    AssertCorrectThread(c);
    serverAssert((callFlags & CMD_CALL_ASYNC) || GlobalLocksAcquired());
    if (!g_pserver->lua_timedout) {
        /* Both EXEC and EVAL call call() directly so there should be
         * no way in_exec or in_eval or propagate_in_transaction is 1.
         * That is unless lua_timedout, in which case client may run
         * some commands. */
        serverAssert(!g_pserver->propagate_in_transaction);
        serverAssert(!serverTL->in_exec);
        serverAssert(!serverTL->in_eval);
    }

    if (moduleHasCommandFilters())
    {
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
        sds args = sdsempty();
        int i;
        for (i=1; i < c->argc && sdslen(args) < 128; i++)
            args = sdscatprintf(args, "`%.*s`, ", 128-(int)sdslen(args), (char*)ptrFromObj(c->argv[i]));
        rejectCommandFormat(c,"unknown command `%s`, with args beginning with: %s",
            (char*)ptrFromObj(c->argv[0]), args);
        sdsfree(args);
        return C_OK;
    } else if ((c->cmd->arity > 0 && c->cmd->arity != c->argc) ||
               (c->argc < -c->cmd->arity)) {
        rejectCommandFormat(c,"wrong number of arguments for '%s' command",
            c->cmd->name);
        return C_OK;
    }

    int is_read_command = (c->cmd->flags & CMD_READONLY) ||
                           (c->cmd->proc == execCommand && (c->mstate.cmd_flags & CMD_READONLY));
    int is_write_command = (c->cmd->flags & CMD_WRITE) ||
                           (c->cmd->proc == execCommand && (c->mstate.cmd_flags & CMD_WRITE));
    int is_denyoom_command = (c->cmd->flags & CMD_DENYOOM) ||
                             (c->cmd->proc == execCommand && (c->mstate.cmd_flags & CMD_DENYOOM));
    int is_denystale_command = !(c->cmd->flags & CMD_STALE) ||
                               (c->cmd->proc == execCommand && (c->mstate.cmd_inv_flags & CMD_STALE));
    int is_denyloading_command = !(c->cmd->flags & CMD_LOADING) ||
                                 (c->cmd->proc == execCommand && (c->mstate.cmd_inv_flags & CMD_LOADING));
    int is_may_replicate_command = (c->cmd->flags & (CMD_WRITE | CMD_MAY_REPLICATE)) ||
                                   (c->cmd->proc == execCommand && (c->mstate.cmd_flags & (CMD_WRITE | CMD_MAY_REPLICATE)));

    if (authRequired(c)) {
        /* AUTH and HELLO and no auth commands are valid even in
         * non-authenticated state. */
        if (!(c->cmd->flags & CMD_NO_AUTH)) {
            rejectCommand(c,shared.noautherr);
            return C_OK;
        }
    }

    /* Check if the user can run this command according to the current
     * ACLs. */
    int acl_errpos;
    int acl_retval = ACLCheckAllPerm(c,&acl_errpos);
    if (acl_retval != ACL_OK) {
        addACLLogEntry(c,acl_retval,acl_errpos,NULL);
        switch (acl_retval) {
        case ACL_DENIED_CMD:
            rejectCommandFormat(c,
                "-NOPERM this user has no permissions to run "
                "the '%s' command or its subcommand", c->cmd->name);
            break;
        case ACL_DENIED_KEY:
            rejectCommandFormat(c,
                "-NOPERM this user has no permissions to access "
                "one of the keys used as arguments");
            break;
        case ACL_DENIED_CHANNEL:
            rejectCommandFormat(c,
                "-NOPERM this user has no permissions to access "
                "one of the channels used as arguments");
            break;
        default:
            rejectCommandFormat(c, "no permission");
            break;
        }
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
        !(!cmdHasMovableKeys(c->cmd) && c->cmd->firstkey == 0 &&
          c->cmd->proc != execCommand))
    {
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
            c->cmd->rejected_calls++;
            return C_OK;
        }
    }

    /* Handle the maxmemory directive.
     *
     * Note that we do not want to reclaim memory if we are here re-entering
     * the event loop since there is a busy Lua script running in timeout
     * condition, to avoid mixing the propagation of scripts with the
     * propagation of DELs due to eviction. */
    if (g_pserver->maxmemory && !g_pserver->lua_timedout && !(callFlags & CMD_CALL_ASYNC)) {
        int out_of_memory = (performEvictions(false /*fPreSnapshot*/) == EVICT_FAIL);
        /* freeMemoryIfNeeded may flush replica output buffers. This may result
         * into a replica, that may be the active client, to be freed. */
        if (serverTL->current_client == NULL) return C_ERR;

        int reject_cmd_on_oom = is_denyoom_command;
        /* If client is in MULTI/EXEC context, queuing may consume an unlimited
         * amount of memory, so we want to stop that.
         * However, we never want to reject DISCARD, or even EXEC (unless it
         * contains denied commands, in which case is_denyoom_command is already
         * set. */
        if (c->flags & CLIENT_MULTI &&
            c->cmd->proc != execCommand &&
            c->cmd->proc != discardCommand &&
            c->cmd->proc != resetCommand) {
            reject_cmd_on_oom = 1;
        }

        if (out_of_memory && reject_cmd_on_oom) {
            rejectCommand(c, shared.oomerr);
            return C_OK;
        }

        /* Save out_of_memory result at script start, otherwise if we check OOM
         * until first write within script, memory used by lua stack and
         * arguments might interfere. */
        if (c->cmd->proc == evalCommand || c->cmd->proc == evalShaCommand) {
            g_pserver->lua_oom = out_of_memory;
        }
    }

    /* Make sure to use a reasonable amount of memory for client side
     * caching metadata. */
    if (g_pserver->tracking_clients) trackingLimitUsedSlots();

    
    /* Don't accept write commands if there are problems persisting on disk
        * and if this is a master instance. */
    int deny_write_type = writeCommandsDeniedByDiskError();
    if (deny_write_type != DISK_ERROR_TYPE_NONE &&
        listLength(g_pserver->masters) == 0 &&
        (is_write_command ||c->cmd->proc == pingCommand))
    {
        if (deny_write_type == DISK_ERROR_TYPE_RDB)
            rejectCommand(c, shared.bgsaveerr);
        else
            rejectCommandFormat(c,
                "-MISCONF Errors writing to the AOF file: %s",
                strerror(g_pserver->aof_last_write_errno));
        return C_OK;
    }    

    /* Don't accept write commands if there are not enough good slaves and
    * user configured the min-slaves-to-write option. */
    if (listLength(g_pserver->masters) == 0 &&
        g_pserver->repl_min_slaves_to_write &&
        g_pserver->repl_min_slaves_max_lag &&
        is_write_command &&
        g_pserver->repl_good_slaves_count < g_pserver->repl_min_slaves_to_write)
    {
        rejectCommand(c, shared.noreplicaserr);
        return C_OK;
    }

    /* Don't accept write commands if this is a read only replica. But
    * accept write commands if this is our master. */
    if (listLength(g_pserver->masters) && g_pserver->repl_slave_ro &&
        !(c->flags & CLIENT_MASTER) &&
        is_write_command)
    {
        rejectCommand(c, shared.roslaveerr);
        return C_OK;
    }

    /* Only allow a subset of commands in the context of Pub/Sub if the
     * connection is in RESP2 mode. With RESP3 there are no limits. */
    if ((c->flags & CLIENT_PUBSUB && c->resp == 2) &&
        c->cmd->proc != pingCommand &&
        c->cmd->proc != subscribeCommand &&
        c->cmd->proc != unsubscribeCommand &&
        c->cmd->proc != psubscribeCommand &&
        c->cmd->proc != punsubscribeCommand &&
        c->cmd->proc != resetCommand) {
        rejectCommandFormat(c,
            "Can't execute '%s': only (P)SUBSCRIBE / "
            "(P)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context",
            c->cmd->name);
        return C_OK;
    }

    if (listLength(g_pserver->masters))
    {
        /* Only allow commands with flag "t", such as INFO, SLAVEOF and so on,
        * when replica-serve-stale-data is no and we are a replica with a broken
        * link with master. */
        if (FBrokenLinkToMaster() &&
            g_pserver->repl_serve_stale_data == 0 &&
            is_denystale_command &&
            !(g_pserver->fActiveReplica && c->cmd->proc == syncCommand)
            && !FInReplicaReplay())
        {
            rejectCommand(c, shared.masterdownerr);
            return C_OK;
        }
    }

    /* Loading DB? Return an error if the command has not the
     * CMD_LOADING flag. */
    if (g_pserver->loading && is_denyloading_command) {
        /* Active Replicas can execute read only commands, and optionally write commands */
        if (!(g_pserver->loading == LOADING_REPLICATION && g_pserver->fActiveReplica && ((c->cmd->flags & CMD_READONLY) || g_pserver->fWriteDuringActiveLoad)))
        {
            rejectCommand(c, shared.loadingerr, ERR_WARNING);
            return C_OK;
        }
    }

    /* Lua script too slow? Only allow a limited number of commands.
     * Note that we need to allow the transactions commands, otherwise clients
     * sending a transaction with pipelining without error checking, may have
     * the MULTI plus a few initial commands refused, then the timeout
     * condition resolves, and the bottom-half of the transaction gets
     * executed, see Github PR #7022. */
    if (g_pserver->lua_timedout &&
          c->cmd->proc != authCommand &&
          c->cmd->proc != helloCommand &&
          c->cmd->proc != replconfCommand &&
          c->cmd->proc != multiCommand &&
          c->cmd->proc != discardCommand &&
          c->cmd->proc != watchCommand &&
          c->cmd->proc != unwatchCommand &&
          c->cmd->proc != resetCommand &&
        !(c->cmd->proc == shutdownCommand &&
          c->argc == 2 &&
          tolower(((char*)ptrFromObj(c->argv[1]))[0]) == 'n') &&
        !(c->cmd->proc == scriptCommand &&
          c->argc == 2 &&
          tolower(((char*)ptrFromObj(c->argv[1]))[0]) == 'k'))
    {
        rejectCommand(c, shared.slowscripterr);
        return C_OK;
    }

    /* Prevent a replica from sending commands that access the keyspace.
     * The main objective here is to prevent abuse of client pause check
     * from which replicas are exempt. */
    if ((c->flags & CLIENT_SLAVE) && (is_may_replicate_command || is_write_command || is_read_command)) {
        rejectCommandFormat(c, "Replica can't interract with the keyspace");
        return C_OK;
    }

    /* If the server is paused, block the client until
     * the pause has ended. Replicas are never paused. */
    if (!(c->flags & CLIENT_SLAVE) && 
        ((g_pserver->client_pause_type == CLIENT_PAUSE_ALL) ||
        (g_pserver->client_pause_type == CLIENT_PAUSE_WRITE && is_may_replicate_command)))
    {
        c->bpop.timeout = 0;
        blockClient(c,BLOCKED_PAUSE);
        return C_OK;       
    }

    /* Exec the command */
    if (c->flags & CLIENT_MULTI &&
        c->cmd->proc != execCommand && c->cmd->proc != discardCommand &&
        c->cmd->proc != multiCommand && c->cmd->proc != watchCommand &&
        c->cmd->proc != resetCommand)
    {
        queueMultiCommand(c);
        addReply(c,shared.queued);
    } else {
        /* If the command was replication or admin related we *must* flush our buffers first.  This is in case
            something happens which would modify what we would send to replicas */

        if (c->cmd->flags & (CMD_MODULE | CMD_ADMIN))
            flushReplBacklogToClients();

        call(c,callFlags);
        c->woff = g_pserver->master_repl_offset;

        if (c->cmd->flags & (CMD_MODULE | CMD_ADMIN))
            flushReplBacklogToClients();
        
        if (listLength(g_pserver->ready_keys))
            handleClientsBlockedOnKeys();
    }

    return C_OK;
}

bool client::postFunction(std::function<void(client *)> fn, bool fLock) {
    this->casyncOpsPending++;
    return aePostFunction(g_pserver->rgthreadvar[this->iel].el, [this, fn]{
        std::lock_guard<decltype(this->lock)> lock(this->lock);
        fn(this);
        --casyncOpsPending;
    }, fLock) == AE_OK;
}

std::vector<robj_sharedptr> clientArgs(client *c) {
    std::vector<robj_sharedptr> args;
    for (int j = 0; j < c->argc; j++) {
        args.push_back(robj_sharedptr(c->argv[j]));
    }
    return args;
}

bool client::asyncCommand(std::function<void(const redisDbPersistentDataSnapshot *, const std::vector<robj_sharedptr> &)> &&mainFn, 
                            std::function<void(const redisDbPersistentDataSnapshot *)> &&postFn) 
{
    serverAssert(FCorrectThread(this));
    const redisDbPersistentDataSnapshot *snapshot = nullptr;
    if (!(this->flags & (CLIENT_MULTI | CLIENT_BLOCKED)))
        snapshot = this->db->createSnapshot(this->mvccCheckpoint, false /* fOptional */);
    if (snapshot == nullptr) {
        return false;
    }
    aeEventLoop *el = serverTL->el;
    blockClient(this, BLOCKED_ASYNC);
    g_pserver->asyncworkqueue->AddWorkFunction([el, this, mainFn, postFn, snapshot] {
        std::vector<robj_sharedptr> args = clientArgs(this);
        aePostFunction(el, [this, mainFn, postFn, snapshot, args] {
            aeReleaseLock();
            std::unique_lock<decltype(this->lock)> lock(this->lock);
            AeLocker locker;
            locker.arm(this);
            unblockClient(this);
            mainFn(snapshot, args);
            locker.disarm();
            lock.unlock();
            if (postFn)
                postFn(snapshot);
            this->db->endSnapshotAsync(snapshot);
            aeAcquireLock();
        });
    });
    return true;
}

/* ====================== Error lookup and execution ===================== */

void incrementErrorCount(const char *fullerr, size_t namelen) {
    struct redisError *error = (struct redisError*)raxFind(g_pserver->errors,(unsigned char*)fullerr,namelen);
    if (error == raxNotFound) {
        error = (struct redisError*)zmalloc(sizeof(*error));
        error->count = 0;
        raxInsert(g_pserver->errors,(unsigned char*)fullerr,namelen,error,NULL);
    }
    error->count++;
}

/*================================== Shutdown =============================== */

/* Close listening sockets. Also unlink the unix domain socket if
 * unlink_unix_socket is non-zero. */
void closeListeningSockets(int unlink_unix_socket) {
    int j;

    for (int iel = 0; iel < cserver.cthreads; ++iel)
    {
        for (j = 0; j < g_pserver->rgthreadvar[iel].ipfd.count; j++) 
            close(g_pserver->rgthreadvar[iel].ipfd.fd[j]);
        for (j = 0; j < g_pserver->rgthreadvar[iel].tlsfd.count; j++)
            close(g_pserver->rgthreadvar[iel].tlsfd.fd[j]);
    }
    if (g_pserver->sofd != -1) close(g_pserver->sofd);
    if (g_pserver->cluster_enabled)
        for (j = 0; j < g_pserver->cfd.count; j++) close(g_pserver->cfd.fd[j]);
    if (unlink_unix_socket && g_pserver->unixsocket) {
        serverLog(LL_NOTICE,"Removing the unix socket file.");
        unlink(g_pserver->unixsocket); /* don't care if this fails */
    }
}

int prepareForShutdown(int flags) {
    /* When SHUTDOWN is called while the server is loading a dataset in
     * memory we need to make sure no attempt is performed to save
     * the dataset on shutdown (otherwise it could overwrite the current DB
     * with half-read data).
     *
     * Also when in Sentinel mode clear the SAVE flag and force NOSAVE. */
    if (g_pserver->loading || g_pserver->sentinel_mode)
        flags = (flags & ~SHUTDOWN_SAVE) | SHUTDOWN_NOSAVE;

    int save = flags & SHUTDOWN_SAVE;
    int nosave = flags & SHUTDOWN_NOSAVE;

    serverLog(LL_WARNING,"User requested shutdown...");
    if (cserver.supervised_mode == SUPERVISED_SYSTEMD)
        redisCommunicateSystemd("STOPPING=1\n");

    /* Kill all the Lua debugger forked sessions. */
    ldbKillForkedSessions();

    /* Kill the saving child if there is a background saving in progress.
       We want to avoid race conditions, for instance our saving child may
       overwrite the synchronous saving did by SHUTDOWN. */
    if (g_pserver->FRdbSaveInProgress()) {
        serverLog(LL_WARNING,"There is a child saving an .rdb. Killing it!");
        killRDBChild();
        /* Note that, in killRDBChild normally has backgroundSaveDoneHandler
         * doing it's cleanup, but in this case this code will not be reached,
         * so we need to call rdbRemoveTempFile which will close fd(in order
         * to unlink file actully) in background thread.
         * The temp rdb file fd may won't be closed when redis exits quickly,
         * but OS will close this fd when process exits. */
        rdbRemoveTempFile(g_pserver->child_pid, 0);
    }

    /* Kill module child if there is one. */
    if (g_pserver->child_type == CHILD_TYPE_MODULE) {
        serverLog(LL_WARNING,"There is a module fork child. Killing it!");
        TerminateModuleForkChild(g_pserver->child_pid,0);
    }

    if (g_pserver->aof_state != AOF_OFF) {
        /* Kill the AOF saving child as the AOF we already have may be longer
         * but contains the full dataset anyway. */
        if (g_pserver->child_type == CHILD_TYPE_AOF) {
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
        if (redis_fsync(g_pserver->aof_fd) == -1) {
            serverLog(LL_WARNING,"Fail to fsync the AOF file: %s.",
                                 strerror(errno));
        }
    }

    /* Create a new RDB file before exiting. */
    if ((g_pserver->saveparamslen > 0 && !nosave) || save) {
        serverLog(LL_NOTICE,"Saving the final RDB snapshot before exiting.");
        if (cserver.supervised_mode == SUPERVISED_SYSTEMD)
            redisCommunicateSystemd("STATUS=Saving the final RDB snapshot\n");
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
            if (cserver.supervised_mode == SUPERVISED_SYSTEMD)
                redisCommunicateSystemd("STATUS=Error trying to save the DB, can't exit.\n");
            return C_ERR;
        }

        // Also Dump To FLASH if Applicable
        for (int idb = 0; idb < cserver.dbnum; ++idb) {
            if (g_pserver->db[idb]->processChanges(false))
                g_pserver->db[idb]->commitChanges();
        }
        saveMasterStatusToStorage(true);
    }

    /* Fire the shutdown modules event. */
    moduleFireServerEvent(REDISMODULE_EVENT_SHUTDOWN,0,NULL);

    /* Remove the pid file if possible and needed. */
    if (cserver.daemonize || cserver.pidfile) {
        serverLog(LL_NOTICE,"Removing the pid file.");
        unlink(cserver.pidfile);
    }

    if (g_pserver->repl_batch_idxStart >= 0) {
        flushReplBacklogToClients();
        g_pserver->repl_batch_offStart = -1;
        g_pserver->repl_batch_idxStart = -1;
    }

    /* Best effort flush of replica output buffers, so that we hopefully
     * send them pending writes. */
    flushSlavesOutputBuffers();
    g_pserver->repl_batch_idxStart = -1;
    g_pserver->repl_batch_offStart = -1;

    /* Close the listening sockets. Apparently this allows faster restarts. */
    closeListeningSockets(1);

    if (g_pserver->asyncworkqueue)
    {
        aeReleaseLock();
        g_pserver->asyncworkqueue->shutdown();
        aeAcquireLock();
    }

    for (int iel = 0; iel < cserver.cthreads; ++iel)
    {
        aePostFunction(g_pserver->rgthreadvar[iel].el, [iel]{
            g_pserver->rgthreadvar[iel].el->stop = 1;
        });
    }

    serverLog(LL_WARNING,"%s is now ready to exit, bye bye...",
        g_pserver->sentinel_mode ? "Sentinel" : "KeyDB");

    return C_OK;
}

/*================================== Commands =============================== */

/* Sometimes Redis cannot accept write commands because there is a persistence
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
    } else if (g_pserver->aof_state != AOF_OFF) {
        if (g_pserver->aof_last_write_status == C_ERR) {
            return DISK_ERROR_TYPE_AOF;
        }
        /* AOF fsync error. */
        int aof_bio_fsync_status;
        atomicGet(g_pserver->aof_bio_fsync_status,aof_bio_fsync_status);
        if (aof_bio_fsync_status == C_ERR) {
            atomicGet(g_pserver->aof_bio_fsync_errno,g_pserver->aof_last_write_errno);
            return DISK_ERROR_TYPE_AOF;
        }
    }

    return DISK_ERROR_TYPE_NONE;
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
        flagcount += addReplyCommandFlag(c,cmd,CMD_SKIP_SLOWLOG, "skip_slowlog");
        flagcount += addReplyCommandFlag(c,cmd,CMD_ASKING, "asking");
        flagcount += addReplyCommandFlag(c,cmd,CMD_FAST, "fast");
        flagcount += addReplyCommandFlag(c,cmd,CMD_NO_AUTH, "no_auth");
        flagcount += addReplyCommandFlag(c,cmd,CMD_MAY_REPLICATE, "may_replicate");
        if (cmdHasMovableKeys(cmd)) {
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
"(no subcommand)",
"    Return details about all KeyDB commands.",
"COUNT",
"    Return the total number of commands in this KeyDB server.",
"GETKEYS <full-command>",
"    Return the keys from a full KeyDB command.",
"INFO [<command-name> ...]",
"    Return details about multiple KeyDB commands.",
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
        getKeysResult result = GETKEYS_RESULT_INIT;
        int j;

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

        if (!getKeysFromCommand(cmd,c->argv+2,c->argc-2,&result)) {
            addReplyError(c,"Invalid arguments specified for command");
        } else {
            addReplyArrayLen(c,result.numkeys);
            for (j = 0; j < result.numkeys; j++) addReplyBulk(c,c->argv[result.keys[j]+2]);
        }
        getKeysFreeResult(&result);
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

/* Characters we sanitize on INFO output to maintain expected format. */
static char unsafe_info_chars[] = "#:\n\r";
static char unsafe_info_chars_substs[] = "____";   /* Must be same length as above */

/* Returns a sanitized version of s that contains no unsafe info string chars.
 * If no unsafe characters are found, simply returns s. Caller needs to
 * free tmp if it is non-null on return.
 */
const char *getSafeInfoString(const char *s, size_t len, char **tmp) {
    *tmp = NULL;
    if (mempbrk(s, len, unsafe_info_chars,sizeof(unsafe_info_chars)-1)
        == NULL) return s;
    char *_new = *tmp = (char*)zmalloc(len + 1);
    memcpy(_new, s, len);
    _new[len] = '\0';
    return memmapchars(_new, len, unsafe_info_chars, unsafe_info_chars_substs,
                       sizeof(unsafe_info_chars)-1);
}

/* Create the string returned by the INFO command. This is decoupled
 * by the INFO command itself as we need to report the same information
 * on memory corruption problems. */
sds genRedisInfoString(const char *section) {
    sds info = sdsempty();
    time_t uptime = g_pserver->unixtime-cserver.stat_starttime;
    int j;
    int allsections = 0, defsections = 0, everything = 0, modules = 0;
    int sections = 0;

    if (section == NULL) section = "default";
    allsections = strcasecmp(section,"all") == 0;
    defsections = strcasecmp(section,"default") == 0;
    everything = strcasecmp(section,"everything") == 0;
    modules = strcasecmp(section,"modules") == 0;
    if (everything) allsections = 1;

    /* Server */
    if (allsections || defsections || !strcasecmp(section,"server")) {
        static int call_uname = 1;
        static struct utsname name;
        const char *mode;
        const char *supervised;

        if (g_pserver->cluster_enabled) mode = "cluster";
        else if (g_pserver->sentinel_mode) mode = "sentinel";
        else mode = "standalone";

        if (cserver.supervised) {
            if (cserver.supervised_mode == SUPERVISED_UPSTART) supervised = "upstart";
            else if (cserver.supervised_mode == SUPERVISED_SYSTEMD) supervised = "systemd";
            else supervised = "unknown";
        } else {
            supervised = "no";
        }

        if (sections++) info = sdscat(info,"\r\n");

        if (call_uname) {
            /* Uname can be slow and is always the same output. Cache it. */
            uname(&name);
            call_uname = 0;
        }

        unsigned int lruclock = g_pserver->lruclock.load();
        ustime_t ustime;
        __atomic_load(&g_pserver->ustime, &ustime, __ATOMIC_RELAXED);
        info = sdscatfmt(info,
            "# Server\r\n"
            "redis_version:%s\r\n"
            "redis_git_sha1:%s\r\n"
            "redis_git_dirty:%i\r\n"
            "redis_build_id:%s\r\n"
            "redis_mode:%s\r\n"
            "os:%s %s %s\r\n"
            "arch_bits:%i\r\n"
            "multiplexing_api:%s\r\n"
            "atomicvar_api:%s\r\n"
            "gcc_version:%i.%i.%i\r\n"
            "process_id:%I\r\n"
            "process_supervised:%s\r\n"
            "run_id:%s\r\n"
            "tcp_port:%i\r\n"
            "server_time_usec:%I\r\n"
            "uptime_in_seconds:%I\r\n"
            "uptime_in_days:%I\r\n"
            "hz:%i\r\n"
            "configured_hz:%i\r\n"
            "lru_clock:%u\r\n"
            "executable:%s\r\n"
            "config_file:%s\r\n",
            KEYDB_SET_VERSION,
            redisGitSHA1(),
            strtol(redisGitDirty(),NULL,10) > 0,
            redisBuildIdString(),
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
            (int64_t) getpid(),
            supervised,
            g_pserver->runid,
            g_pserver->port ? g_pserver->port : g_pserver->tls_port,
            (int64_t)ustime,
            (int64_t)uptime,
            (int64_t)(uptime/(3600*24)),
            g_pserver->hz.load(),
            g_pserver->config_hz,
            lruclock,
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
            "cluster_connections:%lu\r\n"
            "maxclients:%u\r\n"
            "client_recent_max_input_buffer:%zu\r\n"
            "client_recent_max_output_buffer:%zu\r\n"
            "blocked_clients:%d\r\n"
            "tracking_clients:%d\r\n"
            "clients_in_timeout_table:%" PRIu64 "\r\n"
            "current_client_thread:%d\r\n",
            listLength(g_pserver->clients)-listLength(g_pserver->slaves),
            getClusterConnectionsCount(),
            g_pserver->maxclients,
            maxin, maxout,
            g_pserver->blocked_clients,
            g_pserver->tracking_clients,
            raxSize(g_pserver->clients_timeout_table),
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
        long long memory_lua = g_pserver->lua ? (long long)lua_gc(g_pserver->lua,LUA_GCCOUNT,0)*1024 : 0;
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
            "lazyfreed_objects:%zu\r\n"
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
            mh->total_frag,       /* This is the total RSS overhead, including
                                     fragmentation, but not just it. This field
                                     (and the next one) is named like that just
                                     for backward compatibility. */
            mh->total_frag_bytes,
            freeMemoryGetNotCountedMemory(),
            mh->repl_backlog,
            mh->clients_slaves,
            mh->clients_normal,
            mh->aof_buffer,
            ZMALLOC_LIB,
            g_pserver->active_defrag_running,
            lazyfreeGetPendingObjectsCount(),
            lazyfreeGetFreedObjectsCount(),
            g_pserver->m_pstorageFactory ? g_pserver->m_pstorageFactory->name() : "none"
        );
        freeMemoryOverheadData(mh);

        if (g_pserver->m_pstorageFactory)
        {
            info = sdscatprintf(info, 
                "%s_memory:%zu\r\n",
                g_pserver->m_pstorageFactory->name(),
                g_pserver->m_pstorageFactory->totalDiskspaceUsed()
            );
        }
    }

    /* Persistence */
    if (allsections || defsections || !strcasecmp(section,"persistence")) {
        if (sections++) info = sdscat(info,"\r\n");
        double fork_perc = 0;
        if (g_pserver->stat_module_progress) {
            fork_perc = g_pserver->stat_module_progress * 100;
        } else if (g_pserver->stat_current_save_keys_total) {
            fork_perc = ((double)g_pserver->stat_current_save_keys_processed / g_pserver->stat_current_save_keys_total) * 100;
        }
        int aof_bio_fsync_status;
        atomicGet(g_pserver->aof_bio_fsync_status,aof_bio_fsync_status);

        info = sdscatprintf(info,
            "# Persistence\r\n"
            "loading:%d\r\n"
            "current_cow_size:%zu\r\n"
            "current_cow_size_age:%lu\r\n"
            "current_fork_perc:%.2f\r\n"
            "current_save_keys_processed:%zu\r\n"
            "current_save_keys_total:%zu\r\n"
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
            "aof_last_cow_size:%zu\r\n"
            "module_fork_in_progress:%d\r\n"
            "module_fork_last_cow_size:%zu\r\n",
            !!g_pserver->loading.load(std::memory_order_relaxed),   /* Note: libraries expect 1 or 0 here so coerce our enum */
            g_pserver->stat_current_cow_bytes,
            g_pserver->stat_current_cow_updated ? (unsigned long) elapsedMs(g_pserver->stat_current_cow_updated) / 1000 : 0,
            fork_perc,
            g_pserver->stat_current_save_keys_processed,
            g_pserver->stat_current_save_keys_total,
            g_pserver->dirty,
            g_pserver->FRdbSaveInProgress(),
            (intmax_t)g_pserver->lastsave,
            (g_pserver->lastbgsave_status == C_OK) ? "ok" : "err",
            (intmax_t)g_pserver->rdb_save_time_last,
            (intmax_t)(g_pserver->FRdbSaveInProgress() ?
                time(NULL)-g_pserver->rdb_save_time_start : -1),
            g_pserver->stat_rdb_cow_bytes,
            g_pserver->aof_state != AOF_OFF,
            g_pserver->child_type == CHILD_TYPE_AOF,
            g_pserver->aof_rewrite_scheduled,
            (intmax_t)g_pserver->aof_rewrite_time_last,
            (intmax_t)((g_pserver->child_type != CHILD_TYPE_AOF) ?
                -1 : time(NULL)-g_pserver->aof_rewrite_time_start),
            (g_pserver->aof_lastbgrewrite_status == C_OK) ? "ok" : "err",
            (g_pserver->aof_last_write_status == C_OK &&
                aof_bio_fsync_status == C_OK) ? "ok" : "err",
            g_pserver->stat_aof_cow_bytes,
            g_pserver->child_type == CHILD_TYPE_MODULE,
            g_pserver->stat_module_cow_bytes);

        if (g_pserver->aof_enabled) {
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
            double perc = 0;
            time_t eta, elapsed;
            off_t remaining_bytes = 1;

            if (g_pserver->loading_total_bytes) {
                perc = ((double)g_pserver->loading_loaded_bytes / g_pserver->loading_total_bytes) * 100;
                remaining_bytes = g_pserver->loading_total_bytes - g_pserver->loading_loaded_bytes;
            } else if(g_pserver->loading_rdb_used_mem) {
                perc = ((double)g_pserver->loading_loaded_bytes / g_pserver->loading_rdb_used_mem) * 100;
                remaining_bytes = g_pserver->loading_rdb_used_mem - g_pserver->loading_loaded_bytes;
                /* used mem is only a (bad) estimation of the rdb file size, avoid going over 100% */
                if (perc > 99.99) perc = 99.99;
                if (remaining_bytes < 1) remaining_bytes = 1;
            }

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
                "loading_rdb_used_mem:%llu\r\n"
                "loading_loaded_bytes:%llu\r\n"
                "loading_loaded_perc:%.2f\r\n"
                "loading_eta_seconds:%jd\r\n",
                (intmax_t) g_pserver->loading_start_time,
                (unsigned long long) g_pserver->loading_total_bytes,
                (unsigned long long) g_pserver->loading_rdb_used_mem,
                (unsigned long long) g_pserver->loading_loaded_bytes,
                perc,
                (intmax_t)eta
            );
        }
    }

    /* Stats */
    if (allsections || defsections || !strcasecmp(section,"stats")) {
        double avgLockContention = 0;
        for (unsigned i = 0; i < redisServer::s_lockContentionSamples; ++i)
            avgLockContention += g_pserver->rglockSamples[i];
        avgLockContention /= redisServer::s_lockContentionSamples;

        long long stat_total_reads_processed, stat_total_writes_processed;
        long long stat_net_input_bytes, stat_net_output_bytes;
        stat_total_reads_processed = g_pserver->stat_total_reads_processed.load(std::memory_order_relaxed);
        stat_total_writes_processed = g_pserver->stat_total_writes_processed.load(std::memory_order_relaxed);
        stat_net_input_bytes = g_pserver->stat_net_input_bytes.load(std::memory_order_relaxed);
        stat_net_output_bytes = g_pserver->stat_net_output_bytes.load(std::memory_order_relaxed);

        long long stat_total_error_replies = 0;
        for (int iel = 0; iel < cserver.cthreads; ++iel)
            stat_total_error_replies += g_pserver->rgthreadvar[iel].stat_total_error_replies;

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
            "expire_cycle_cpu_milliseconds:%lld\r\n"
            "evicted_keys:%lld\r\n"
            "keyspace_hits:%lld\r\n"
            "keyspace_misses:%lld\r\n"
            "pubsub_channels:%ld\r\n"
            "pubsub_patterns:%lu\r\n"
            "latest_fork_usec:%lld\r\n"
            "total_forks:%lld\r\n"
            "migrate_cached_sockets:%ld\r\n"
            "slave_expires_tracked_keys:%zu\r\n"
            "active_defrag_hits:%lld\r\n"
            "active_defrag_misses:%lld\r\n"
            "active_defrag_key_hits:%lld\r\n"
            "active_defrag_key_misses:%lld\r\n"
            "tracking_total_keys:%lld\r\n"
            "tracking_total_items:%lld\r\n"
            "tracking_total_prefixes:%lld\r\n"
            "unexpected_error_replies:%lld\r\n"
            "total_error_replies:%lld\r\n"
            "dump_payload_sanitizations:%lld\r\n"
            "total_reads_processed:%lld\r\n"
            "total_writes_processed:%lld\r\n"
            "instantaneous_lock_contention:%d\r\n"
            "avg_lock_contention:%f\r\n"
            "storage_provider_read_hits:%lld\r\n"
            "storage_provider_read_misses:%lld\r\n",
            g_pserver->stat_numconnections,
            g_pserver->stat_numcommands,
            getInstantaneousMetric(STATS_METRIC_COMMAND),
            stat_net_input_bytes,
            stat_net_output_bytes,
            (float)getInstantaneousMetric(STATS_METRIC_NET_INPUT)/1024,
            (float)getInstantaneousMetric(STATS_METRIC_NET_OUTPUT)/1024,
            g_pserver->stat_rejected_conn,
            g_pserver->stat_sync_full,
            g_pserver->stat_sync_partial_ok,
            g_pserver->stat_sync_partial_err,
            g_pserver->stat_expiredkeys,
            g_pserver->stat_expired_stale_perc*100,
            g_pserver->stat_expired_time_cap_reached_count,
            g_pserver->stat_expire_cycle_time_used/1000,
            g_pserver->stat_evictedkeys,
            g_pserver->stat_keyspace_hits,
            g_pserver->stat_keyspace_misses,
            dictSize(g_pserver->pubsub_channels),
            dictSize(g_pserver->pubsub_patterns),
            g_pserver->stat_fork_time,
            g_pserver->stat_total_forks,
            dictSize(g_pserver->migrate_cached_sockets),
            getSlaveKeyWithExpireCount(),
            g_pserver->stat_active_defrag_hits,
            g_pserver->stat_active_defrag_misses,
            g_pserver->stat_active_defrag_key_hits,
            g_pserver->stat_active_defrag_key_misses,
            (unsigned long long) trackingGetTotalKeys(),
            (unsigned long long) trackingGetTotalItems(),
            (unsigned long long) trackingGetTotalPrefixes(),
            g_pserver->stat_unexpected_error_replies,
            stat_total_error_replies,
            g_pserver->stat_dump_payload_sanitizations,
            stat_total_reads_processed,
            stat_total_writes_processed,
            aeLockContention(),
            avgLockContention,
            g_pserver->stat_storage_provider_read_hits,
            g_pserver->stat_storage_provider_read_misses);
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
            int connectedMasters = 0;
            info = sdscatprintf(info, "master_global_link_status:%s\r\n",
                FBrokenLinkToMaster(&connectedMasters) ? "down" : "up");

            info = sdscatprintf(info, "connected_masters:%d\r\n", connectedMasters);

            int cmasters = 0;
            listIter li;
            listNode *ln;
            listRewind(g_pserver->masters, &li);
            while ((ln = listNext(&li)))
            {
                long long slave_repl_offset = 1;
                long long slave_read_repl_offset = 1;
                redisMaster *mi = (redisMaster*)listNodeValue(ln);

                if (mi->master){
                    slave_repl_offset = mi->master->reploff;
                    slave_read_repl_offset = mi->master->read_reploff;
                } else if (mi->cached_master){
                    slave_repl_offset = mi->cached_master->reploff;
                    slave_read_repl_offset = mi->cached_master->read_reploff;
                }

                char master_prefix[128] = "";
                if (cmasters != 0) {
                    snprintf(master_prefix, sizeof(master_prefix), "_%d", cmasters);
                }

                info = sdscatprintf(info,
                    "master%s_host:%s\r\n"
                    "master%s_port:%d\r\n"
                    "master%s_link_status:%s\r\n"
                    "master%s_last_io_seconds_ago:%d\r\n"
                    "master%s_sync_in_progress:%d\r\n"
                    "slave_read_repl_offset:%lld\r\n"
                    "slave_repl_offset:%lld\r\n"
                    ,master_prefix, mi->masterhost,
                    master_prefix, mi->masterport,
                    master_prefix, (mi->repl_state == REPL_STATE_CONNECTED) ?
                        "up" : "down",
                    master_prefix, mi->master ?
                    ((int)(g_pserver->unixtime-mi->master->lastinteraction)) : -1,
                    master_prefix, mi->repl_state == REPL_STATE_TRANSFER,
                    slave_read_repl_offset, 
                    slave_repl_offset
                );

                if (mi->repl_state == REPL_STATE_TRANSFER) {
                    double perc = 0;
                    if (mi->repl_transfer_size) {
                        perc = ((double)mi->repl_transfer_read / mi->repl_transfer_size) * 100;
                    }
                    info = sdscatprintf(info,
                        "master%s_sync_total_bytes:%lld\r\n"
                        "master%s_sync_read_bytes:%lld\r\n"
                        "master%s_sync_left_bytes:%lld\r\n"
                        "master%s_sync_perc:%.2f\r\n"
                        "master%s_sync_last_io_seconds_ago:%d\r\n",
                        master_prefix, (long long) mi->repl_transfer_size,
                        master_prefix, (long long) mi->repl_transfer_read,
                        master_prefix, (long long) (mi->repl_transfer_size - mi->repl_transfer_read),
                        master_prefix, perc,
                        master_prefix, (int)(g_pserver->unixtime-mi->repl_transfer_lastio)
                    );
                }

                if (mi->repl_state != REPL_STATE_CONNECTED) {
                    info = sdscatprintf(info,
                        "master%s_link_down_since_seconds:%jd\r\n",
                        master_prefix, mi->repl_down_since ? 
                            (intmax_t)(g_pserver->unixtime-mi->repl_down_since) : -1);
                }
                ++cmasters;
            }
            info = sdscatprintf(info,
                "slave_priority:%d\r\n"
                "slave_read_only:%d\r\n"
                "replica_announced:%d\r\n",
                g_pserver->slave_priority,
                g_pserver->repl_slave_ro,
                g_pserver->replica_announced);
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
                char ip[NET_IP_STR_LEN], *slaveip = replica->slave_addr;
                int port;
                long lag = 0;

                if (!slaveip) {
                    if (connPeerToString(replica->conn,ip,sizeof(ip),&port) == -1)
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
                    (replica->repl_ack_off), lag);
                slaveid++;
            }
        }
        info = sdscatprintf(info,
            "master_failover_state:%s\r\n"
            "master_replid:%s\r\n"
            "master_replid2:%s\r\n"
            "master_repl_offset:%lld\r\n"
            "second_repl_offset:%lld\r\n"
            "repl_backlog_active:%d\r\n"
            "repl_backlog_size:%lld\r\n"
            "repl_backlog_first_byte_offset:%lld\r\n"
            "repl_backlog_histlen:%lld\r\n",
            getFailoverStateString(),
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

        struct rusage self_ru, c_ru;
        getrusage(RUSAGE_SELF, &self_ru);
        getrusage(RUSAGE_CHILDREN, &c_ru);
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
#ifdef RUSAGE_THREAD
        struct rusage m_ru;
        getrusage(RUSAGE_THREAD, &m_ru);
        info = sdscatprintf(info,
            "used_cpu_sys_main_thread:%ld.%06ld\r\n"
            "used_cpu_user_main_thread:%ld.%06ld\r\n",
            (long)m_ru.ru_stime.tv_sec, (long)m_ru.ru_stime.tv_usec,
            (long)m_ru.ru_utime.tv_sec, (long)m_ru.ru_utime.tv_usec);
#endif  /* RUSAGE_THREAD */
    }

    /* Modules */
    if (allsections || defsections || !strcasecmp(section,"modules")) {
        if (sections++) info = sdscat(info,"\r\n");
        info = sdscatprintf(info,"# Modules\r\n");
        info = genModulesInfoString(info);
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
            char *tmpsafe;
            c = (struct redisCommand *) dictGetVal(de);
            if (!c->calls && !c->failed_calls && !c->rejected_calls)
                continue;
            info = sdscatprintf(info,
                "cmdstat_%s:calls=%lld,usec=%lld,usec_per_call=%.2f"
                ",rejected_calls=%lld,failed_calls=%lld\r\n",
                getSafeInfoString(c->name, strlen(c->name), &tmpsafe), c->calls, c->microseconds,
                (c->calls == 0) ? 0 : ((float)c->microseconds/c->calls),
                c->rejected_calls, c->failed_calls);
            if (tmpsafe != NULL) zfree(tmpsafe);
        }
        dictReleaseIterator(di);
    }
    /* Error statistics */
    if (allsections || defsections || !strcasecmp(section,"errorstats")) {
        if (sections++) info = sdscat(info,"\r\n");
        info = sdscat(info, "# Errorstats\r\n");
        raxIterator ri;
        raxStart(&ri,g_pserver->errors);
        raxSeek(&ri,"^",NULL,0);
        struct redisError *e;
        while(raxNext(&ri)) {
            char *tmpsafe;
            e = (struct redisError *) ri.data;
            info = sdscatprintf(info,
                "errorstat_%.*s:count=%lld\r\n",
                (int)ri.key_len, getSafeInfoString((char *) ri.key, ri.key_len, &tmpsafe), e->count);
            if (tmpsafe != NULL) zfree(tmpsafe);
        }
        raxStop(&ri);
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
            long long keys, vkeys, cachedKeys;

            keys = g_pserver->db[j]->size();
            vkeys = g_pserver->db[j]->expireSize();
            cachedKeys = g_pserver->db[j]->size(true /* fCachedOnly */);

            // Adjust TTL by the current time
            mstime_t mstime;
            __atomic_load(&g_pserver->mstime, &mstime, __ATOMIC_ACQUIRE);
            g_pserver->db[j]->avg_ttl -= (mstime - g_pserver->db[j]->last_expire_set);
            if (g_pserver->db[j]->avg_ttl < 0)
                g_pserver->db[j]->avg_ttl = 0;
            g_pserver->db[j]->last_expire_set = mstime;
            
            if (keys || vkeys) {
                info = sdscatprintf(info,
                    "db%d:keys=%lld,expires=%lld,avg_ttl=%lld,cached_keys=%lld\r\n",
                    j, keys, vkeys, static_cast<long long>(g_pserver->db[j]->avg_ttl), cachedKeys);
            }
        }
    }

    if (allsections || defsections || !strcasecmp(section,"keydb")) {
        // Compute the MVCC depth
        int mvcc_depth = 0;
        for (int idb = 0; idb < cserver.dbnum; ++idb) {
            mvcc_depth = std::max(mvcc_depth, g_pserver->db[idb]->snapshot_depth());
        }

        if (sections++) info = sdscat(info,"\r\n");
        info = sdscatprintf(info, 
            "# KeyDB\r\n"
            "mvcc_depth:%d\r\n",
            mvcc_depth
        );
    }

    /* Get info from modules.
     * if user asked for "everything" or "modules", or a specific section
     * that's not found yet. */
    if (everything || modules ||
        (!allsections && !defsections && sections==0)) {
        info = modulesCollectInfo(info,
                                  everything || modules ? NULL: section,
                                  0, /* not a crash report */
                                  sections);
    }
    return info;
}

void infoCommand(client *c) {
    const char *section = c->argc == 2 ? (const char*)ptrFromObj(c->argv[1]) : "default";

    if (c->argc > 2) {
        addReplyErrorObject(c,shared.syntaxerr);
        return;
    }
    sds info = genRedisInfoString(section);
    addReplyVerbatim(c,info,sdslen(info),"txt");
    sdsfree(info);
}

void monitorCommand(client *c) {
    serverAssert(GlobalLocksAcquired());

    if (c->flags & CLIENT_DENY_BLOCKING) {
        /**
         * A client that has CLIENT_DENY_BLOCKING flag on
         * expects a reply per command and so can't execute MONITOR. */
        addReplyError(c, "MONITOR isn't allowed for DENY BLOCKING client");
        return;
    }

    /* ignore MONITOR if already slave or in monitor mode */
    if (c->flags & CLIENT_SLAVE) return;

    c->flags |= (CLIENT_SLAVE|CLIENT_MONITOR);
    listAddNodeTail(g_pserver->monitors,c);
    addReply(c,shared.ok);
}

/* =================================== Main! ================================ */

int checkIgnoreWarning(const char *warning) {
    int argc, j;
    sds *argv = sdssplitargs(g_pserver->ignore_warnings, &argc);
    if (argv == NULL)
        return 0;

    for (j = 0; j < argc; j++) {
        char *flag = argv[j];
        if (!strcasecmp(flag, warning))
            break;
    }
    sdsfreesplitres(argv,argc);
    return j < argc;
}

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
    if (THPIsEnabled() && THPDisable()) {
        serverLog(LL_WARNING,"WARNING you have Transparent Huge Pages (THP) support enabled in your kernel. This will create latency and memory usage issues with KeyDB. To fix this issue run the command 'echo madvise > /sys/kernel/mm/transparent_hugepage/enabled' as root, and add it to your /etc/rc.local in order to retain the setting after a reboot. KeyDB must be restarted after THP is disabled (set to 'madvise' or 'never').");
    }
}

#ifdef __arm64__

/* Get size in kilobytes of the Shared_Dirty pages of the calling process for the
 * memory map corresponding to the provided address, or -1 on error. */
static int smapsGetSharedDirty(unsigned long addr) {
    int ret, in_mapping = 0, val = -1;
    unsigned long from, to;
    char buf[64];
    FILE *f;

    f = fopen("/proc/self/smaps", "r");
    serverAssert(f);

    while (1) {
        if (!fgets(buf, sizeof(buf), f))
            break;

        ret = sscanf(buf, "%lx-%lx", &from, &to);
        if (ret == 2)
            in_mapping = from <= addr && addr < to;

        if (in_mapping && !memcmp(buf, "Shared_Dirty:", 13)) {
            ret = sscanf(buf, "%*s %d", &val);
            serverAssert(ret == 1);
            break;
        }
    }

    fclose(f);
    return val;
}

/* Older arm64 Linux kernels have a bug that could lead to data corruption
 * during background save in certain scenarios. This function checks if the
 * kernel is affected.
 * The bug was fixed in commit ff1712f953e27f0b0718762ec17d0adb15c9fd0b
 * titled: "arm64: pgtable: Ensure dirty bit is preserved across pte_wrprotect()"
 * Return 1 if the kernel seems to be affected, and 0 otherwise. */
int linuxMadvFreeForkBugCheck(void) {
    int ret, pipefd[2];
    pid_t pid;
    char *p, *q, bug_found = 0;
    const long map_size = 3 * 4096;

    /* Create a memory map that's in our full control (not one used by the allocator). */
    p = (char*)mmap(NULL, map_size, PROT_READ, MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
    serverAssert(p != MAP_FAILED);

    q = p + 4096;

    /* Split the memory map in 3 pages by setting their protection as RO|RW|RO to prevent
     * Linux from merging this memory map with adjacent VMAs. */
    ret = mprotect(q, 4096, PROT_READ | PROT_WRITE);
    serverAssert(!ret);

    /* Write to the page once to make it resident */
    *(volatile char*)q = 0;

    /* Tell the kernel that this page is free to be reclaimed. */
#ifndef MADV_FREE
#define MADV_FREE 8
#endif
    ret = madvise(q, 4096, MADV_FREE);
    serverAssert(!ret);

    /* Write to the page after being marked for freeing, this is supposed to take
     * ownership of that page again. */
    *(volatile char*)q = 0;

    /* Create a pipe for the child to return the info to the parent. */
    ret = pipe(pipefd);
    serverAssert(!ret);

    /* Fork the process. */
    pid = fork();
    serverAssert(pid >= 0);
    if (!pid) {
        /* Child: check if the page is marked as dirty, expecing 4 (kB).
         * A value of 0 means the kernel is affected by the bug. */
        if (!smapsGetSharedDirty((unsigned long)q))
            bug_found = 1;

        ret = write(pipefd[1], &bug_found, 1);
        serverAssert(ret == 1);

        exit(0);
    } else {
        /* Read the result from the child. */
        ret = read(pipefd[0], &bug_found, 1);
        serverAssert(ret == 1);

        /* Reap the child pid. */
        serverAssert(waitpid(pid, NULL, 0) == pid);
    }

    /* Cleanup */
    ret = close(pipefd[0]);
    serverAssert(!ret);
    ret = close(pipefd[1]);
    serverAssert(!ret);
    ret = munmap(p, map_size);
    serverAssert(!ret);

    return bug_found;
}
#endif /* __arm64__ */
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
    printf("KeyDB server v=%s sha=%s:%d malloc=%s bits=%d build=%llx\n",
        KEYDB_REAL_VERSION,
        redisGitSHA1(),
        atoi(redisGitDirty()) > 0,
        ZMALLOC_LIB,
        sizeof(long) == 4 ? 32 : 64,
        (unsigned long long) redisBuildId());
    exit(0);
}

void usage(void) {
    fprintf(stderr,"Usage: ./keydb-server [/path/to/keydb.conf] [options] [-]\n");
    fprintf(stderr,"       ./keydb-server - (read config from stdin)\n");
    fprintf(stderr,"       ./keydb-server -v or --version\n");
    fprintf(stderr,"       ./keydb-server -h or --help\n");
    fprintf(stderr,"       ./keydb-server --test-memory <megabytes>\n\n");
    fprintf(stderr,"Examples:\n");
    fprintf(stderr,"       ./keydb-server (run the server with default conf)\n");
    fprintf(stderr,"       ./keydb-server /etc/keydb/6379.conf\n");
    fprintf(stderr,"       ./keydb-server --port 7777\n");
    fprintf(stderr,"       ./keydb-server --port 7777 --replicaof 127.0.0.1 8888\n");
    fprintf(stderr,"       ./keydb-server /etc/mykeydb.conf --loglevel verbose -\n");
    fprintf(stderr,"       ./keydb-server /etc/mykeydb.conf --loglevel verbose\n\n");
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
     * forced us to do so via keydb.conf. */
    int show_logo = ((!g_pserver->syslog_enabled &&
                      g_pserver->logfile[0] == '\0' &&
                      isatty(fileno(stdout))) ||
                     g_pserver->always_show_logo);

    if (!show_logo) {
        serverLog(LL_NOTICE,
            "Running mode=%s, port=%d.",
            mode, g_pserver->port ? g_pserver->port : g_pserver->tls_port
        );
    } else {
        sds motd = fetchMOTD(true, cserver.enable_motd);
        snprintf(buf,1024*16,ascii_logo,
            KEYDB_REAL_VERSION,
            redisGitSHA1(),
            strtol(redisGitDirty(),NULL,10) > 0,
            (sizeof(long) == 8) ? "64" : "32",
            mode, g_pserver->port ? g_pserver->port : g_pserver->tls_port,
            (long) getpid(),
            motd ? motd : ""
        );
        if (motd)
            freeMOTD(motd);
        serverLogRaw(LL_NOTICE|LL_RAW,buf);
    }

    zfree(buf);
}

int changeBindAddr(sds *addrlist, int addrlist_len, bool fFirstCall) {
    int i;
    int result = C_OK;

    char *prev_bindaddr[CONFIG_BINDADDR_MAX];
    int prev_bindaddr_count;

    /* Close old TCP and TLS servers */
    closeSocketListeners(&serverTL->ipfd);
    closeSocketListeners(&serverTL->tlsfd);

    /* Keep previous settings */
    prev_bindaddr_count = g_pserver->bindaddr_count;
    memcpy(prev_bindaddr, g_pserver->bindaddr, sizeof(g_pserver->bindaddr));

    /* Copy new settings */
    memset(g_pserver->bindaddr, 0, sizeof(g_pserver->bindaddr));
    for (i = 0; i < addrlist_len; i++) {
        g_pserver->bindaddr[i] = zstrdup(addrlist[i]);
    }
    g_pserver->bindaddr_count = addrlist_len;

    /* Bind to the new port */
    if ((g_pserver->port != 0 && listenToPort(g_pserver->port, &serverTL->ipfd, (cserver.cthreads > 1), fFirstCall) != C_OK) ||
        (g_pserver->tls_port != 0 && listenToPort(g_pserver->tls_port, &serverTL->tlsfd, (cserver.cthreads > 1), fFirstCall) != C_OK)) {
        serverLog(LL_WARNING, "Failed to bind, trying to restore old listening sockets.");

        /* Restore old bind addresses */
        for (i = 0; i < addrlist_len; i++) {
            zfree(g_pserver->bindaddr[i]);
        }
        memcpy(g_pserver->bindaddr, prev_bindaddr, sizeof(g_pserver->bindaddr));
        g_pserver->bindaddr_count = prev_bindaddr_count;

        /* Re-Listen TCP and TLS */
        serverTL->ipfd.count = 0;
        if (g_pserver->port != 0 && listenToPort(g_pserver->port, &serverTL->ipfd, (cserver.cthreads > 1), false) != C_OK) {
            serverPanic("Failed to restore old listening sockets.");
        }

        serverTL->tlsfd.count = 0;
        if (g_pserver->tls_port != 0 && listenToPort(g_pserver->tls_port, &serverTL->tlsfd, (cserver.cthreads > 1), false) != C_OK) {
            serverPanic("Failed to restore old listening sockets.");
        }

        result = C_ERR;
    } else {
        /* Free old bind addresses */
        for (i = 0; i < prev_bindaddr_count; i++) {
            zfree(prev_bindaddr[i]);
        }
    }

    /* Create TCP and TLS event handlers */
    if (createSocketAcceptHandler(&serverTL->ipfd, acceptTcpHandler) != C_OK) {
        serverPanic("Unrecoverable error creating TCP socket accept handler.");
    }
    if (createSocketAcceptHandler(&serverTL->tlsfd, acceptTLSHandler) != C_OK) {
        serverPanic("Unrecoverable error creating TLS socket accept handler.");
    }

    if (cserver.set_proc_title && fFirstCall) redisSetProcTitle(NULL);

    return result;
}

int changeListenPort(int port, socketFds *sfd, aeFileProc *accept_handler, bool fFirstCall) {
    socketFds new_sfd = {{0}};

    /* Just close the server if port disabled */
    if (port == 0) {
        closeSocketListeners(sfd);
        if (cserver.set_proc_title && fFirstCall) redisSetProcTitle(NULL);
        return C_OK;
    }

    /* Bind to the new port */
    if (listenToPort(port, &new_sfd, (cserver.cthreads > 1), fFirstCall) != C_OK) {
        return C_ERR;
    }

    /* Create event handlers */
    if (createSocketAcceptHandler(&new_sfd, accept_handler) != C_OK) {
        closeSocketListeners(&new_sfd);
        return C_ERR;
    }

    /* Close old servers */
    closeSocketListeners(sfd);

    /* Copy new descriptors */
    sfd->count = new_sfd.count;
    memcpy(sfd->fd, new_sfd.fd, sizeof(new_sfd.fd));

    if (cserver.set_proc_title && fFirstCall) redisSetProcTitle(NULL);

    return C_OK;
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
        rdbRemoveTempFile(g_pserver->rdbThreadVars.tmpfileNum, 1);
        g_pserver->garbageCollector.shutdown();
        _Exit(1); /* Exit with an error since this was not a clean shutdown. */
    } else if (g_pserver->loading) {
        serverLogFromHandler(LL_WARNING, "Received shutdown signal during loading, exiting now.");
        _Exit(0);   // calling dtors is undesirable, exit immediately
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

    sigemptyset(&act.sa_mask);
    act.sa_flags = SA_NODEFER | SA_RESETHAND | SA_SIGINFO;
    act.sa_sigaction = sigsegvHandler;
    if(g_pserver->crashlog_enabled) {
        sigaction(SIGSEGV, &act, NULL);
        sigaction(SIGBUS, &act, NULL);
        sigaction(SIGFPE, &act, NULL);
        sigaction(SIGILL, &act, NULL);
        sigaction(SIGABRT, &act, NULL);
    }
    return;
}

void removeSignalHandlers(void) {
    struct sigaction act;
    sigemptyset(&act.sa_mask);
    act.sa_flags = SA_NODEFER | SA_RESETHAND;
    act.sa_handler = SIG_DFL;
    sigaction(SIGSEGV, &act, NULL);
    sigaction(SIGBUS, &act, NULL);
    sigaction(SIGFPE, &act, NULL);
    sigaction(SIGILL, &act, NULL);
    sigaction(SIGABRT, &act, NULL);
}

/* This is the signal handler for children process. It is currently useful
 * in order to track the SIGUSR1, that we send to a child in order to terminate
 * it in a clean way, without the parent detecting an error and stop
 * accepting writes because of a write error condition. */
static void sigKillChildHandler(int sig) {
    UNUSED(sig);
    int level = g_pserver->in_fork_child == CHILD_TYPE_MODULE? LL_VERBOSE: LL_WARNING;
    serverLogFromHandler(level, "Received SIGUSR1 in child, exiting now.");
    exitFromChild(SERVER_CHILD_NOERROR_RETVAL);
}

void setupChildSignalHandlers(void) {
    struct sigaction act;

    /* When the SA_SIGINFO flag is set in sa_flags then sa_sigaction is used.
     * Otherwise, sa_handler is used. */
    sigemptyset(&act.sa_mask);
    act.sa_flags = 0;
    act.sa_handler = sigKillChildHandler;
    sigaction(SIGUSR1, &act, NULL);
    return;
}

/* After fork, the child process will inherit the resources
 * of the parent process, e.g. fd(socket or flock) etc.
 * should close the resources not used by the child process, so that if the
 * parent restarts it can bind/lock despite the child possibly still running. */
void closeChildUnusedResourceAfterFork() {
    closeListeningSockets(0);
    if (g_pserver->cluster_enabled && g_pserver->cluster_config_file_lock_fd != -1)
        close(g_pserver->cluster_config_file_lock_fd);  /* don't care if this fails */

    for (int iel = 0; iel < cserver.cthreads; ++iel) {
        aeClosePipesForForkChild(g_pserver->rgthreadvar[iel].el);
    }
    aeClosePipesForForkChild(g_pserver->modulethreadvar.el);

    /* Clear cserver.pidfile, this is the parent pidfile which should not
     * be touched (or deleted) by the child (on exit / crash) */
    zfree(cserver.pidfile);
    cserver.pidfile = NULL;
}

void executeWithoutGlobalLock(std::function<void()> func) {
    serverAssert(GlobalLocksAcquired());

    std::vector<client*> vecclients;
    listIter li;
    listNode *ln;
    listRewind(g_pserver->clients, &li);

    // All client locks must be acquired *after* the global lock is reacquired to prevent deadlocks
    //  so unlock here, and save them for reacquisition later
    while ((ln = listNext(&li)) != nullptr)
    {
        client *c = (client*)listNodeValue(ln);
        if (c->lock.fOwnLock()) {
            serverAssert(c->flags & CLIENT_PROTECTED || c->flags & CLIENT_EXECUTING_COMMAND);  // If the client is not protected we have no gurantee they won't be free'd in the event loop
            c->lock.unlock();
            vecclients.push_back(c);
        }
    }
    
    /* Since we're about to release our lock we need to flush the repl backlog queue */
    bool fReplBacklog = g_pserver->repl_batch_offStart >= 0;
    if (fReplBacklog) {
        flushReplBacklogToClients();
        g_pserver->repl_batch_idxStart = -1;
        g_pserver->repl_batch_offStart = -1;
    }

    aeReleaseLock();
    serverAssert(!GlobalLocksAcquired());
    try {
        func();
    }
    catch (...) {
        // Caller expects us to be locked so fix and rethrow
        AeLocker locker;
        locker.arm(nullptr);
        locker.release();
        for (client *c : vecclients)
            c->lock.lock();
        throw;
    }
    
    AeLocker locker;
    locker.arm(nullptr);
    locker.release();

    // Restore it so the calling code is not confused
    if (fReplBacklog) {
        g_pserver->repl_batch_idxStart = g_pserver->repl_backlog_idx;
        g_pserver->repl_batch_offStart = g_pserver->master_repl_offset;
    }

    for (client *c : vecclients)
        c->lock.lock();
}

/* purpose is one of CHILD_TYPE_ types */
int redisFork(int purpose) {
    int childpid;
    long long start = ustime();
    
    if (isMutuallyExclusiveChildType(purpose)) {
        if (hasActiveChildProcess())
            return -1;

        openChildInfoPipe();
    }
    long long startWriteLock = ustime();
    aeAcquireForkLock();
    latencyAddSampleIfNeeded("fork-lock",(ustime()-startWriteLock)/1000);
    if ((childpid = fork()) == 0) {
        /* Child */
        aeReleaseForkLock();
        g_pserver->in_fork_child = purpose;
        setOOMScoreAdj(CONFIG_OOM_BGCHILD);
        setupChildSignalHandlers();
        closeChildUnusedResourceAfterFork();
    } else {
        /* Parent */
        aeReleaseForkLock();
        g_pserver->stat_total_forks++;
        g_pserver->stat_fork_time = ustime()-start;
        g_pserver->stat_fork_rate = (double) zmalloc_used_memory() * 1000000 / g_pserver->stat_fork_time / (1024*1024*1024); /* GB per second. */
        latencyAddSampleIfNeeded("fork",g_pserver->stat_fork_time/1000);
        if (childpid == -1) {
            if (isMutuallyExclusiveChildType(purpose)) closeChildInfoPipe();
            return -1;
        }

        /* The child_pid and child_type are only for mutual exclusive children.
         * other child types should handle and store their pid's in dedicated variables.
         *
         * Today, we allows CHILD_TYPE_LDB to run in parallel with the other fork types:
         * - it isn't used for production, so it will not make the server be less efficient
         * - used for debugging, and we don't want to block it from running while other
         *   forks are running (like RDB and AOF) */
        if (isMutuallyExclusiveChildType(purpose)) {
            g_pserver->child_pid = childpid;
            g_pserver->child_type = purpose;
            g_pserver->stat_current_cow_bytes = 0;
            g_pserver->stat_current_cow_updated = 0;
            g_pserver->stat_current_save_keys_processed = 0;
            g_pserver->stat_module_progress = 0;
            g_pserver->stat_current_save_keys_total = dbTotalServerKeyCount();
        }

        updateDictResizePolicy();
        moduleFireServerEvent(REDISMODULE_EVENT_FORK_CHILD,
                              REDISMODULE_SUBEVENT_FORK_CHILD_BORN,
                              NULL);
    }
    return childpid;
}

void sendChildCowInfo(childInfoType info_type, const char *pname) {
    sendChildInfoGeneric(info_type, 0, -1, pname);
}

void sendChildInfo(childInfoType info_type, size_t keys, const char *pname) {
    sendChildInfoGeneric(info_type, keys, -1, pname);
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

    if (g_pserver->m_pstorageFactory)
    {
        for (int idb = 0; idb < cserver.dbnum; ++idb)
        {
            if (g_pserver->db[idb]->size() > 0)
            {
                serverLog(LL_NOTICE, "Not loading the RDB because a storage provider is set and the database is not empty");
                return;
            }
        }
        serverLog(LL_NOTICE, "Loading the RDB even though we have a storage provider because the database is empty");
    }
    
    serverTL->gcEpoch = g_pserver->garbageCollector.startEpoch();
    if (g_pserver->aof_state == AOF_ON) {
        if (loadAppendOnlyFile(g_pserver->aof_filename) == C_OK)
            serverLog(LL_NOTICE,"DB loaded from append only file: %.3f seconds",(float)(ustime()-start)/1000000);
    } else if (g_pserver->rdb_filename != NULL || g_pserver->rdb_s3bucketpath != NULL) {
        rdbSaveInfo rsi;
        rsi.fForceSetKey = false;
        errno = 0; /* Prevent a stale value from affecting error checking */
        if (rdbLoad(&rsi,RDBFLAGS_NONE) == C_OK) {
            serverLog(LL_NOTICE,"DB loaded from disk: %.3f seconds",
                (float)(ustime()-start)/1000000);

            /* Restore the replication ID / offset from the RDB file. */
            if ((listLength(g_pserver->masters) || 
                (g_pserver->cluster_enabled && 
                nodeIsSlave(g_pserver->cluster->myself))) &&
                rsi.repl_id_is_set &&
                rsi.repl_offset != -1 &&
                /* Note that older implementations may save a repl_stream_db
                 * of -1 inside the RDB file in a wrong way, see more
                 * information in function rdbPopulateSaveInfo. */
                rsi.repl_stream_db != -1)
            {
                memcpy(g_pserver->replid,rsi.repl_id,sizeof(g_pserver->replid));
                g_pserver->master_repl_offset = rsi.repl_offset;
                if (g_pserver->repl_batch_offStart >= 0)
                    g_pserver->repl_batch_offStart = g_pserver->master_repl_offset;
            }
            updateActiveReplicaMastersFromRsi(&rsi);
            if (!g_pserver->fActiveReplica && listLength(g_pserver->masters)) {
                redisMaster *mi = (redisMaster*)listNodeValue(listFirst(g_pserver->masters));
                /* If we are a replica, create a cached master from this
                * information, in order to allow partial resynchronizations
                * with masters. */
                replicationCacheMasterUsingMyself(mi);
                selectDb(mi->cached_master,rsi.repl_stream_db);
            }
        } else if (errno != ENOENT) {
            serverLog(LL_WARNING,"Fatal error loading the DB: %s. Exiting.",strerror(errno));
            exit(1);
        }
    }
    g_pserver->garbageCollector.endEpoch(serverTL->gcEpoch);
    serverTL->gcEpoch.reset();
}

void redisOutOfMemoryHandler(size_t allocation_size) {
    serverLog(LL_WARNING,"Out Of Memory allocating %zu bytes!",
        allocation_size);
    serverPanic("KeyDB aborting for OUT OF MEMORY. Allocating %zu bytes!", 
        allocation_size);
}

/* Callback for sdstemplate on proc-title-template. See redis.conf for
 * supported variables.
 */
static sds redisProcTitleGetVariable(const sds varname, void *arg)
{
    if (!strcmp(varname, "title")) {
        return sdsnew((const char*)arg);
    } else if (!strcmp(varname, "listen-addr")) {
        if (g_pserver->port || g_pserver->tls_port)
            return sdscatprintf(sdsempty(), "%s:%u",
                                g_pserver->bindaddr_count ? g_pserver->bindaddr[0] : "*",
                                g_pserver->port ? g_pserver->port : g_pserver->tls_port);
        else
            return sdscatprintf(sdsempty(), "unixsocket:%s", g_pserver->unixsocket);
    } else if (!strcmp(varname, "server-mode")) {
        if (g_pserver->cluster_enabled) return sdsnew("[cluster]");
        else if (g_pserver->sentinel_mode) return sdsnew("[sentinel]");
        else return sdsempty();
    } else if (!strcmp(varname, "config-file")) {
        return sdsnew(cserver.configfile ? cserver.configfile : "-");
    } else if (!strcmp(varname, "port")) {
        return sdscatprintf(sdsempty(), "%u", g_pserver->port);
    } else if (!strcmp(varname, "tls-port")) {
        return sdscatprintf(sdsempty(), "%u", g_pserver->tls_port);
    } else if (!strcmp(varname, "unixsocket")) {
        return sdsnew(g_pserver->unixsocket);
    } else
        return NULL;    /* Unknown variable name */
}

/* Expand the specified proc-title-template string and return a newly
 * allocated sds, or NULL. */
static sds expandProcTitleTemplate(const char *_template, const char *title) {
    sds res = sdstemplate(_template, redisProcTitleGetVariable, (void *) title);
    if (!res)
        return NULL;
    return sdstrim(res, " ");
}
/* Validate the specified template, returns 1 if valid or 0 otherwise. */
int validateProcTitleTemplate(const char *_template) {
    int ok = 1;
    sds res = expandProcTitleTemplate(_template, "");
    if (!res)
        return 0;
    if (sdslen(res) == 0) ok = 0;
    sdsfree(res);
    return ok;
}

int redisSetProcTitle(const char *title) {
#ifdef USE_SETPROCTITLE
    if (!title) title = cserver.exec_argv[0];
    sds proc_title = expandProcTitleTemplate(cserver.proc_title_template, title);
    if (!proc_title) return C_ERR;  /* Not likely, proc_title_template is validated */

    setproctitle("%s", proc_title);
    sdsfree(proc_title);
#else
    UNUSED(title);
#endif

    return C_OK;
}

void redisSetCpuAffinity(const char *cpulist) {
#ifdef USE_SETCPUAFFINITY
    setcpuaffinity(cpulist);
#else
    UNUSED(cpulist);
#endif
}

/* Send a notify message to systemd. Returns sd_notify return code which is
 * a positive number on success. */
int redisCommunicateSystemd(const char *sd_notify_msg) {
#ifdef HAVE_LIBSYSTEMD
    int ret = sd_notify(0, sd_notify_msg);

    if (ret == 0)
        serverLog(LL_WARNING, "systemd supervision error: NOTIFY_SOCKET not found!");
    else if (ret < 0)
        serverLog(LL_WARNING, "systemd supervision error: sd_notify: %d", ret);
    return ret;
#else
    UNUSED(sd_notify_msg);
    return 0;
#endif
}

/* Attempt to set up upstart supervision. Returns 1 if successful. */
static int redisSupervisedUpstart(void) {
    const char *upstart_job = getenv("UPSTART_JOB");

    if (!upstart_job) {
        serverLog(LL_WARNING,
                "upstart supervision requested, but UPSTART_JOB not found!");
        return 0;
    }

    serverLog(LL_NOTICE, "supervised by upstart, will stop to signal readiness.");
    raise(SIGSTOP);
    unsetenv("UPSTART_JOB");
    return 1;
}

/* Attempt to set up systemd supervision. Returns 1 if successful. */
static int redisSupervisedSystemd(void) {
#ifndef HAVE_LIBSYSTEMD
    serverLog(LL_WARNING,
            "systemd supervision requested or auto-detected, but Redis is compiled without libsystemd support!");
    return 0;
#else
    if (redisCommunicateSystemd("STATUS=Redis is loading...\n") <= 0)
        return 0;
    serverLog(LL_NOTICE,
        "Supervised by systemd. Please make sure you set appropriate values for TimeoutStartSec and TimeoutStopSec in your service unit.");
    return 1;
#endif
}

int redisIsSupervised(int mode) {
    int ret = 0;

    if (mode == SUPERVISED_AUTODETECT) {
        if (getenv("UPSTART_JOB")) {
            serverLog(LL_VERBOSE, "Upstart supervision detected.");
            mode = SUPERVISED_UPSTART;
        } else if (getenv("NOTIFY_SOCKET")) {
            serverLog(LL_VERBOSE, "Systemd supervision detected.");
            mode = SUPERVISED_SYSTEMD;
        }
    } else if (mode == SUPERVISED_UPSTART) {
        return redisSupervisedUpstart();
    } else if (mode == SUPERVISED_SYSTEMD) {
        serverLog(LL_WARNING,
            "WARNING supervised by systemd - you MUST set appropriate values for TimeoutStartSec and TimeoutStopSec in your service unit.");
        return redisCommunicateSystemd("STATUS=KeyDB is loading...\n");
    }

    switch (mode) {
        case SUPERVISED_UPSTART:
            ret = redisSupervisedUpstart();
            break;
        case SUPERVISED_SYSTEMD:
            ret = redisSupervisedSystemd();
            break;
        default:
            break;
    }

    if (ret)
        cserver.supervised_mode = mode;

    return ret;
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
    __atomic_load(&g_pserver->mstime, &mst, __ATOMIC_ACQUIRE);
    if (msPrev >= (uint64_t)mst)  // we can be greater if the count overflows
    {
        __atomic_fetch_add(&g_pserver->mvcc_tstamp, 1, __ATOMIC_RELEASE);
    }
    else
    {
        uint64_t val = ((uint64_t)mst) << MVCC_MS_SHIFT;
        __atomic_store(&g_pserver->mvcc_tstamp, &val, __ATOMIC_RELEASE);
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

    serverPanic("std::teminate() called");
}

void wakeTimeThread() {
    updateCachedTime();
    aeThreadOffline();
    std::unique_lock<fastlock> lock(time_thread_lock);
    aeThreadOnline();
    if (sleeping_threads >= cserver.cthreads)
        time_thread_cv.notify_one();
    sleeping_threads--;
    serverAssert(sleeping_threads >= 0);
}

void *timeThreadMain(void*) {
    timespec delay;
    delay.tv_sec = 0;
    delay.tv_nsec = 100;
    int cycle_count = 0;
    aeThreadOnline();
    while (true) {
        {
            aeThreadOffline();
            std::unique_lock<fastlock> lock(time_thread_lock);
            aeThreadOnline();
            if (sleeping_threads >= cserver.cthreads) {
                aeThreadOffline();
                time_thread_cv.wait(lock);
                aeThreadOnline();
                cycle_count = 0;
            }
        }
        updateCachedTime();
        if (cycle_count == MAX_CYCLES_TO_HOLD_FORK_LOCK) {
            aeThreadOffline();
            aeThreadOnline();
            cycle_count = 0;
        }
#if defined(__APPLE__)
        nanosleep(&delay, nullptr);
#else
        clock_nanosleep(CLOCK_MONOTONIC, 0, &delay, NULL);
#endif
        cycle_count++;
    }
    aeThreadOffline();
}

void *workerThreadMain(void *parg)
{
    int iel = (int)((int64_t)parg);
    serverLog(LL_NOTICE, "Thread %d alive.", iel);
    serverTL = g_pserver->rgthreadvar+iel;  // set the TLS threadsafe global
    tlsInitThread();

    if (iel != IDX_EVENT_LOOP_MAIN)
    {
        aeThreadOnline();
        aeAcquireLock();
        initNetworkingThread(iel, cserver.cthreads > 1);
        aeReleaseLock();
        aeThreadOffline();
    }

    moduleAcquireGIL(true); // Normally afterSleep acquires this, but that won't be called on the first run
    aeThreadOnline();
    aeEventLoop *el = g_pserver->rgthreadvar[iel].el;
    try
    {
        aeMain(el);
    }
    catch (ShutdownException)
    {
    }
    aeThreadOffline();
    moduleReleaseGIL(true);
    serverAssert(!GlobalLocksAcquired());
    aeDeleteEventLoop(el);

    tlsCleanupThread();
    return NULL;
}

static void validateConfiguration()
{
    updateMasterAuth();
    
    if (cserver.cthreads > (int)std::thread::hardware_concurrency()) {
        serverLog(LL_WARNING, "WARNING: server-threads is greater than this machine's core count.  Truncating to %u threads", std::thread::hardware_concurrency());
        cserver.cthreads = (int)std::thread::hardware_concurrency();
        cserver.cthreads = std::max(cserver.cthreads, 1);	// in case of any weird sign overflows
    }

    if (g_pserver->enable_multimaster && !g_pserver->fActiveReplica) {
        serverLog(LL_WARNING, "ERROR: Multi Master requires active replication to be enabled.");
        serverLog(LL_WARNING, "\tKeyDB will now exit.  Please update your configuration file.");
        exit(EXIT_FAILURE);
    }

    g_pserver->repl_backlog_config_size = g_pserver->repl_backlog_size; // this is normally set in the update logic, but not on initial config
}

int iAmMaster(void) {
    return ((!g_pserver->cluster_enabled && (listLength(g_pserver->masters) == 0 || g_pserver->fActiveReplica)) ||
            (g_pserver->cluster_enabled && nodeIsMaster(g_pserver->cluster->myself)));
}

bool initializeStorageProvider(const char **err);

#ifdef REDIS_TEST
typedef int redisTestProc(int argc, char **argv, int accurate);
struct redisTest {
    char *name;
    redisTestProc *proc;
    int failed;
} redisTests[] = {
    {"ziplist", ziplistTest},
    {"quicklist", quicklistTest},
    {"intset", intsetTest},
    {"zipmap", zipmapTest},
    {"sha1test", sha1Test},
    {"util", utilTest},
    {"endianconv", endianconvTest},
    {"crc64", crc64Test},
    {"zmalloc", zmalloc_test},
    {"sds", sdsTest},
    {"dict", dictTest}
};
redisTestProc *getTestProcByName(const char *name) {
    int numtests = sizeof(redisTests)/sizeof(struct redisTest);
    for (int j = 0; j < numtests; j++) {
        if (!strcasecmp(name,redisTests[j].name)) {
            return redisTests[j].proc;
        }
    }
    return NULL;
}
#endif

int main(int argc, char **argv) {
    struct timeval tv;
    int j;
    char config_from_stdin = 0;

    std::set_terminate(OnTerminate);

    {
    SymVer version;
    version = parseVersion(KEYDB_REAL_VERSION);
    serverAssert(version.major >= 0 && version.minor >= 0 && version.build >= 0);
    serverAssert(compareVersion(&version) == VersionCompareResult::EqualVersion);
    }

#ifdef USE_MEMKIND
    storage_init(NULL, 0);
#endif

#ifdef REDIS_TEST
    if (argc >= 3 && !strcasecmp(argv[1], "test")) {
        int accurate = 0;
        for (j = 3; j < argc; j++) {
            if (!strcasecmp(argv[j], "--accurate")) {
                accurate = 1;
            }
        }

        if (!strcasecmp(argv[2], "all")) {
            int numtests = sizeof(redisTests)/sizeof(struct redisTest);
            for (j = 0; j < numtests; j++) {
                redisTests[j].failed = (redisTests[j].proc(argc,argv,accurate) != 0);
            }

            /* Report tests result */
            int failed_num = 0;
            for (j = 0; j < numtests; j++) {
                if (redisTests[j].failed) {
                    failed_num++;
                    printf("[failed] Test - %s\n", redisTests[j].name);
                } else {
                    printf("[ok] Test - %s\n", redisTests[j].name);
                }
            }

            printf("%d tests, %d passed, %d failed\n", numtests,
                   numtests-failed_num, failed_num);

            return failed_num == 0 ? 0 : 1;
        } else {
            redisTestProc *proc = getTestProcByName(argv[2]);
            if (!proc) return -1; /* test not found */
            return proc(argc,argv,accurate);
        }

        return 0;
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
    srandom(time(NULL)^getpid());
    gettimeofday(&tv,NULL);
    init_genrand64(((long long) tv.tv_sec * 1000000 + tv.tv_usec) ^ getpid());
    crc64_init();

    /* Store umask value. Because umask(2) only offers a set-and-get API we have
     * to reset it and restore it back. We do this early to avoid a potential
     * race condition with threads that could be creating files or directories.
     */
    umask(g_pserver->umask = umask(0777));
    
    serverAssert(g_pserver->repl_batch_offStart < 0);

    uint8_t hashseed[16];
    getRandomHexChars((char*)hashseed,sizeof(hashseed));
    dictSetHashFunctionSeed(hashseed);
    g_pserver->sentinel_mode = checkForSentinelMode(argc,argv);
    initServerConfig();
    serverTL = &g_pserver->rgthreadvar[IDX_EVENT_LOOP_MAIN];
    aeThreadOnline();
    aeAcquireLock();    // We own the lock on boot
    ACLInit(); /* The ACL subsystem must be initialized ASAP because the
                  basic networking code and client creation depends on it. */
    moduleInitModulesSystem();
    tlsInit();

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
        /* Parse command line options
         * Precedence wise, File, stdin, explicit options -- last config is the one that matters.
         *
         * First argument is the config file name? */
        if (argv[1][0] != '-') {
            /* Replace the config file in g_pserver->exec_argv with its absolute path. */
            cserver.configfile = getAbsolutePath(argv[1]);
            zfree(cserver.exec_argv[1]);
            cserver.exec_argv[1] = zstrdup(cserver.configfile);
            j = 2; // Skip this arg when parsing options
        }
        while(j < argc) {
            /* Either first or last argument - Should we read config from stdin? */
            if (argv[j][0] == '-' && argv[j][1] == '\0' && (j == 1 || j == argc-1)) {
                config_from_stdin = 1;
            }
            /* All the other options are parsed and conceptually appended to the
             * configuration file. For instance --port 6380 will generate the
             * string "port 6380\n" to be parsed after the actual config file
             * and stdin input are parsed (if they exist). */
            else if (argv[j][0] == '-' && argv[j][1] == '-') {
                /* Option name */
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

        loadServerConfig(cserver.configfile, config_from_stdin, options);
        if (g_pserver->sentinel_mode) loadSentinelConfigFromQueue();
        sdsfree(options);
    }

    if (g_pserver->syslog_enabled) {
        openlog(g_pserver->syslog_ident, LOG_PID | LOG_NDELAY | LOG_NOWAIT,
            g_pserver->syslog_facility);
    }
    
    if (g_pserver->sentinel_mode) sentinelCheckConfigFile();

    cserver.supervised = redisIsSupervised(cserver.supervised_mode);
    int background = cserver.daemonize && !cserver.supervised;
    if (background) daemonize();

    serverLog(LL_WARNING, "oO0OoO0OoO0Oo KeyDB is starting oO0OoO0OoO0Oo");
    serverLog(LL_WARNING,
        "KeyDB version=%s, bits=%d, commit=%s, modified=%d, pid=%d, just started",
            KEYDB_REAL_VERSION,
            (sizeof(long) == 8) ? 64 : 32,
            redisGitSHA1(),
            strtol(redisGitDirty(),NULL,10) > 0,
            (int)getpid());

    if (argc == 1) {
        serverLog(LL_WARNING, "Warning: no config file specified, using the default config. In order to specify a config file use %s /path/to/keydb.conf", argv[0]);
    } else {
        serverLog(LL_WARNING, "Configuration loaded");
    }

    validateConfiguration();

    const char *err;
    if (!initializeStorageProvider(&err))
    {
        serverLog(LL_WARNING, "Failed to initialize storage provider: %s",err);
        exit(EXIT_FAILURE);
    }

    for (int iel = 0; iel < cserver.cthreads; ++iel)
    {
        initServerThread(g_pserver->rgthreadvar+iel, iel == IDX_EVENT_LOOP_MAIN);
    }

    initServerThread(&g_pserver->modulethreadvar, false);
    readOOMScoreAdj();
    initServer();
    initNetworking(cserver.cthreads > 1 /* fReusePort */);

    if (background || cserver.pidfile) createPidFile();
    if (cserver.set_proc_title) redisSetProcTitle(NULL);
    redisAsciiArt();
    checkTcpBacklogSettings();

    if (!g_pserver->sentinel_mode) {
        /* Things not needed when running in Sentinel mode. */
        serverLog(LL_WARNING,"Server initialized");
    #ifdef __linux__
        linuxMemoryWarnings();
    #if defined (__arm64__)
        int ret;
        if ((ret = linuxMadvFreeForkBugCheck())) {
            if (ret == 1)
                serverLog(LL_WARNING,"WARNING Your kernel has a bug that could lead to data corruption during background save. "
                                     "Please upgrade to the latest stable kernel.");
            else
                serverLog(LL_WARNING, "Failed to test the kernel for a bug that could lead to data corruption during background save. "
                                      "Your system could be affected, please report this error.");
            if (!checkIgnoreWarning("ARM64-COW-BUG")) {
                serverLog(LL_WARNING,"KeyDB will now exit to prevent data corruption. "
                                     "Note that it is possible to suppress this warning by setting the following config: ignore-warnings ARM64-COW-BUG");
                exit(1);
            }
        }
    #endif /* __arm64__ */
    #endif /* __linux__ */
        moduleInitModulesSystemLast();
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
            rdbSaveInfo rsi;
            startLoadingFile(stdin, (char*)"stdin", 0);
            rioInitWithFile(&rdb,stdin);
            rdbLoadRio(&rdb,0,&rsi);
            stopLoading(true);
            return EXIT_SUCCESS;
        }

        InitServerLast();

        try {
            loadDataFromDisk();
        } catch (ShutdownException) {
            _Exit(EXIT_SUCCESS);
        }

        if (g_pserver->cluster_enabled) {
            if (verifyClusterConfigWithData() == C_ERR) {
                serverLog(LL_WARNING,
                    "You can't have keys in a DB different than DB 0 when in "
                    "Cluster mode. Exiting.");
                exit(1);
            }
        }
        if (g_pserver->rgthreadvar[IDX_EVENT_LOOP_MAIN].ipfd.count > 0 && g_pserver->rgthreadvar[IDX_EVENT_LOOP_MAIN].tlsfd.count > 0)
            serverLog(LL_NOTICE,"Ready to accept connections");
        if (g_pserver->sofd > 0)
            serverLog(LL_NOTICE,"The server is now ready to accept connections at %s", g_pserver->unixsocket);
        if (cserver.supervised_mode == SUPERVISED_SYSTEMD) {
            if (!listLength(g_pserver->masters)) {
                redisCommunicateSystemd("STATUS=Ready to accept connections\n");
            } else {
                redisCommunicateSystemd("STATUS=Ready to accept connections in read-only mode. Waiting for MASTER <-> REPLICA sync\n");
            }
            redisCommunicateSystemd("READY=1\n");
        }
    } else {
        ACLLoadUsersAtStartup();
        InitServerLast();
        sentinelIsRunning();
        if (cserver.supervised_mode == SUPERVISED_SYSTEMD) {
            redisCommunicateSystemd("STATUS=Ready to accept connections\n");
            redisCommunicateSystemd("READY=1\n");
        }
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

    redisSetCpuAffinity(g_pserver->server_cpulist);
    aeReleaseLock();    //Finally we can dump the lock
    aeThreadOffline();
    moduleReleaseGIL(true);
    
    setOOMScoreAdj(-1);
    serverAssert(cserver.cthreads > 0 && cserver.cthreads <= MAX_EVENT_LOOPS);

    pthread_create(&cserver.time_thread_id, nullptr, timeThreadMain, nullptr);
    if (cserver.time_thread_priority) {
        struct sched_param time_thread_priority;
        time_thread_priority.sched_priority = sched_get_priority_max(SCHED_FIFO);
        pthread_setschedparam(cserver.time_thread_id, SCHED_FIFO, &time_thread_priority);
    }

    pthread_attr_t tattr;
    pthread_attr_init(&tattr);
    pthread_attr_setstacksize(&tattr, 1 << 23); // 8 MB
    for (int iel = 0; iel < cserver.cthreads; ++iel)
    {
        pthread_create(g_pserver->rgthread + iel, &tattr, workerThreadMain, (void*)((int64_t)iel));
        if (cserver.fThreadAffinity)
        {
#ifdef __linux__
            cpu_set_t cpuset;
            CPU_ZERO(&cpuset);
            CPU_SET(iel + cserver.threadAffinityOffset, &cpuset);
            if (pthread_setaffinity_np(g_pserver->rgthread[iel], sizeof(cpu_set_t), &cpuset) == 0)
            {
                serverLog(LL_NOTICE, "Binding thread %d to cpu %d", iel, iel + cserver.threadAffinityOffset + 1);
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
    for (int iel = 0; iel < cserver.cthreads; ++iel)
        pthread_join(g_pserver->rgthread[iel], &pvRet);

    /* free our databases */
    bool fLockAcquired = aeTryAcquireLock(false);
    g_pserver->shutdown_asap = true;    // flag that we're in shutdown
    if (!fLockAcquired)
        g_fInCrash = true;  // We don't actually crash right away, because we want to sync any storage providers
    
    saveMasterStatusToStorage(true);
    for (int idb = 0; idb < cserver.dbnum; ++idb) {
        g_pserver->db[idb]->storageProviderDelete();
    }
    delete g_pserver->metadataDb;

    // If we couldn't acquire the global lock it means something wasn't shutdown and we'll probably deadlock
    serverAssert(fLockAcquired);

    g_pserver->garbageCollector.shutdown();
    delete g_pserver->m_pstorageFactory;

    // Don't return because we don't want to run any global dtors
    _Exit(EXIT_SUCCESS);
    return 0;   // Ensure we're well formed even though this won't get hit
}

/* The End */
