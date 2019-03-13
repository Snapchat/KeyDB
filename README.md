What is KeyDB?
--------------

KeyDB is a high performance fork of Redis focusing on multithreading, memory efficiency, and high throughput.  In addition to multithreading KeyDB also has features only available in Redis Enterprise such as FLASH storage support, and some not available at all such as direct backup to AWS S3.

On the same hardware KeyDB can perform twice as many queries per second as Redis, with 60% lower latency.

KeyDB has full compatibility with the Redis protocol, modules, and scripts.  This includes full support for transactions, and atomic execution of scripts.  For more information see our architecture section below.

Try our docker container: https://hub.docker.com/r/eqalpha/keydb

Why fork Redis?
---------------

The Redis maintainers have continually reiterated that they do not plan to support multithreading.  While we have great respect for the redis team, we feel the analysis justifying this decision is incorrect.  In addition we wanted open source implementations of features currently only available in proprietary modules.   We feel a fork is the best way to accelerate development in the areas of most interest to us.

We plan to track the Redis repo closely and hope our projects can learn from each other.

Benchmarking KeyDB
------------------

<img src=https://cdn-images-1.medium.com/max/1400/1*s7mTb7Qb0kxc951mz8bdgA.png width=420 height=300/><img src=https://cdn-images-1.medium.com/max/1400/1*R00A5U4AFGohGOYHMfT6fA.png height=300/>

Please note keydb-benchmark and redis-benchmark are currently single threaded and too slow to properly benchmark KeyDB.  We recommend using a redis cluster benchmark tool such as [memtier](https://github.com/RedisLabs/memtier_benchmark).  Please ensure your machine has enough cores for both KeyDB and memteir if testing locally.  KeyDB expects exclusive use of any cores assigned to it.

For more details on how we benchmarked KeyDB along with performance numbers check out our blog post: [Redis Should Be Multithreaded](https://medium.com/@john_63123/redis-should-be-multi-threaded-e28319cab744?source=friends_link&sk=7ce8e9fe3ec8224a4d27ef075d085457)

New Configuration Options
-------------------------

With new features comes new options:

    server-threads N
    server-thread-affinity [true/false]

The number of threads used to serve requests.  This should be related to the number of queues available in your network hardware, *not* the number of cores on your machine.  Because KeyDB uses spinlocks to reduce latency; making this too high will reduce performance.  We recommend using 4 here.  By default this is set to one.

    scratch-file-path /path

If you would like to use the FLASH backed storage this option configures the directory for KeyDB's temporary files.  This feature relies on snapshotting to work so must be used on a BTRFS filesystem.  ZFS may also work but is untested.  With this feature KeyDB will use RAM as a cache and page to disk as necessary.  NOTE: This requires special compilation options, see Building KeyDB below.
    
    db-s3-object /path/to/bucket

If you would like KeyDB to dump and load directly to AWS S3 this option specifies the bucket.  Using this option with the traditional RDB options will result in KeyDB backing up twice to both locations.  If both are specified KeyDB will first attempt to load from the local dump file and if that fails load from S3.  This requires the AWS CLI tools to be installed and configured which are used under the hood to transfer the data.

All other configuration options behave as you'd expect.  Your existing configuration files should continue to work unchanged.

Building KeyDB
--------------

KeyDB can be compiled and is tested for use on Linux.  KeyDB currently relies on SO_REUSEADDR's load balancing behavior which is available only in Linux.  When we support marshalling connections across threads we plan to support other operating systems such as FreeBSD.

Compiling is as simple as:

    % make

You can enable flash support with (Note: autoconf and autotools must be installed):

    % make MALLOC=memkind

Fixing build problems with dependencies or cached build options
---------

KeyDB has some dependencies which are included into the `deps` directory.
`make` does not automatically rebuild dependencies even if something in
the source code of dependencies changes.

When you update the source code with `git pull` or when code inside the
dependencies tree is modified in any other way, make sure to use the following
command in order to really clean everything and rebuild from scratch:

    make distclean

This will clean: jemalloc, lua, hiredis, linenoise.

Also if you force certain build options like 32bit target, no C compiler
optimizations (for debugging purposes), and other similar build time options,
those options are cached indefinitely until you issue a `make distclean`
command.

Fixing problems building 32 bit binaries
---------

If after building KeyDB with a 32 bit target you need to rebuild it
with a 64 bit target, or the other way around, you need to perform a
`make distclean` in the root directory of the KeyDB distribution.

In case of build errors when trying to build a 32 bit binary of KeyDB, try
the following steps:

* Install the packages libc6-dev-i386 (also try g++-multilib).
* Try using the following command line instead of `make 32bit`:
  `make CFLAGS="-m32 -march=native" LDFLAGS="-m32"`

Allocator
---------

Selecting a non-default memory allocator when building KeyDB is done by setting
the `MALLOC` environment variable. KeyDB is compiled and linked against libc
malloc by default, with the exception of jemalloc being the default on Linux
systems. This default was picked because jemalloc has proven to have fewer
fragmentation problems than libc malloc.

To force compiling against libc malloc, use:

    % make MALLOC=libc

To compile against jemalloc on Mac OS X systems, use:

    % make MALLOC=jemalloc

Verbose build
-------------

KeyDB will build with a user friendly colorized output by default.
If you want to see a more verbose output use the following:

    % make V=1

Running KeyDB
-------------

To run KeyDB with the default configuration just type:

    % cd src
    % ./keydb-server

If you want to provide your keydb.conf, you have to run it using an additional
parameter (the path of the configuration file):

    % cd src
    % ./keydb-server /path/to/keydb.conf

It is possible to alter the KeyDB configuration by passing parameters directly
as options using the command line. Examples:

    % ./keydb-server --port 9999 --replicaof 127.0.0.1 6379
    % ./keydb-server /etc/keydb/6379.conf --loglevel debug

All the options in keydb.conf are also supported as options using the command
line, with exactly the same name.

Playing with KeyDB
------------------

You can use keydb-cli to play with KeyDB. Start a keydb-server instance,
then in another terminal try the following:

    % cd src
    % ./keydb-cli
    keydb> ping
    PONG
    keydb> set foo bar
    OK
    keydb> get foo
    "bar"
    keydb> incr mycounter
    (integer) 1
    keydb> incr mycounter
    (integer) 2
    keydb>

You can find the list of all the available commands at http://redis.io/commands.

Installing KeyDB
-----------------

In order to install KeyDB binaries into /usr/local/bin just use:

    % make install

You can use `make PREFIX=/some/other/directory install` if you wish to use a
different destination.

Make install will just install binaries in your system, but will not configure
init scripts and configuration files in the appropriate place. This is not
needed if you want just to play a bit with KeyDB, but if you are installing
it the proper way for a production system, we have a script doing this
for Ubuntu and Debian systems:

    % cd utils
    % ./install_server.sh

The script will ask you a few questions and will setup everything you need
to run KeyDB properly as a background daemon that will start again on
system reboots.

You'll be able to stop and start KeyDB using the script named
`/etc/init.d/keydb_<portnumber>`, for instance `/etc/init.d/keydb_6379`.

Multithreading Architecture
---------------------------

KeyDB works by running the normal Redis event loop on multiple threads.  Network IO, and query parsing are done concurrently.  Each connection is assigned a thread on accept().  Access to the core hash table is guarded by spinlock.  Because the hashtable access is extremely fast this lock has low contention.  Transactions hold the lock for the duration of the EXEC command.  Modules work in concert with the GIL which is only acquired when all server threads are paused.  This maintains the atomicity guarantees modules expect.

Unlike most databases the core data structure is the fastest part of the system.  Most of the query time comes from parsing the REPL protocol and copying data to/from the network.

Future work:
 - Allow rebalancing of connections to different threads after the connection
 - Allow multiple readers access to the hashtable concurrently

Code contributions
-----------------

Note: by contributing code to the KeyDB project in any form, including sending
a pull request via Github, a code fragment or patch via private email or
public discussion groups, you agree to release your code under the terms
of the BSD license that you can find in the COPYING file included in the KeyDB
source distribution.

Please see the CONTRIBUTING file in this source distribution for more
information.


