![Current Release](https://img.shields.io/github/release/JohnSully/KeyDB.svg)
[![Build Status](https://travis-ci.org/JohnSully/KeyDB.svg?branch=unstable)](https://travis-ci.org/JohnSully/KeyDB) [![Join the chat at https://gitter.im/KeyDB/community](https://badges.gitter.im/KeyDB/community.svg)](https://gitter.im/KeyDB/community?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![StackShare](http://img.shields.io/badge/tech-stack-0690fa.svg?style=flat)](https://stackshare.io/eq-alpha-technology-inc/eq-alpha-technology-inc)

##### Need Help? Check out our extensive [documentation](https://docs.keydb.dev).

What is KeyDB?
--------------

KeyDB is a high performance fork of Redis with a focus on multithreading, memory efficiency, and high throughput.  In addition to multithreading, KeyDB also has features only available in Redis Enterprise such as [Active Replication](https://github.com/JohnSully/KeyDB/wiki/Active-Replication), [FLASH storage](https://github.com/JohnSully/KeyDB/wiki/FLASH-Storage) support, and some not available at all such as direct backup to AWS S3. 

KeyDB maintains full compatibility with the Redis protocol, modules, and scripts.  This includes the atomicity gurantees for scripts and transactions.  Because KeyDB keeps in sync with Redis development KeyDB is a superset of Redis functionality, making KeyDB a drop in replacement for existing Redis deployments.

On the same hardware KeyDB can perform twice as many queries per second as Redis, with 60% lower latency. Active-Replication simplifies hot-spare failover allowing you to easily distribute writes over replicas and use simple TCP based load balancing/failover. KeyDB's higher performance allows you to do more on less hardware which reduces operation costs and complexity.

<img src=https://cdn-images-1.medium.com/max/1400/1*s7mTb7Qb0kxc951mz8bdgA.png width=420 height=300/><img src=https://cdn-images-1.medium.com/max/1400/1*R00A5U4AFGohGOYHMfT6fA.png height=300/>

Why fork Redis?
---------------

KeyDB has a different philosophy on how the codebase should evolve.  We feel that ease of use, high performance, and a "batteries included" approach is the best way to create a good user experience.  While we have great respect for the Redis maintainers it is our opinion that the Redis approach focuses too much on simplicity of the code base at the expense of complexity for the user.  This results in the need for external components and workarounds to solve common problems - resulting in more complexity overall.

Because of this difference of opinion features which are right for KeyDB may not be appropriate for Redis.  A fork allows us to explore this new development path and implement features which may never be a part of Redis.  KeyDB keeps in sync with upstream Redis changes, and where applicable we upstream bug fixes and changes. It is our hope that the two projects can continue to grow and learn from each other.

Additional Resources
--------------------

Try our docker container: https://hub.docker.com/r/eqalpha/keydb

Talk on Gitter: https://gitter.im/KeyDB

Visit our Website: https://keydb.dev

See options for channel partners and support contracts: https://keydb.dev/support.html

Learn with KeyDB’s official documentation site: https://docs.keydb.dev

[Subscribe to the KeyDB mailing list](https://eqalpha.us20.list-manage.com/subscribe/post?u=978f486c2f95589b24591a9cc&id=4ab9220500)

Management GUI: We recommend [FastoNoSQL](https://fastonosql.com/) which has official KeyDB support.


Benchmarking KeyDB
------------------

Please note keydb-benchmark and redis-benchmark are currently single threaded and too slow to properly benchmark KeyDB.  We recommend using a redis cluster benchmark tool such as [memtier](https://github.com/RedisLabs/memtier_benchmark).  Please ensure your machine has enough cores for both KeyDB and memteir if testing locally.  KeyDB expects exclusive use of any cores assigned to it.

For more details on how we benchmarked KeyDB along with performance numbers check out our blog post: [Redis Should Be Multithreaded](https://medium.com/@john_63123/redis-should-be-multi-threaded-e28319cab744?source=friends_link&sk=7ce8e9fe3ec8224a4d27ef075d085457)

New Configuration Options
-------------------------

With new features comes new options:

    server-threads N
    server-thread-affinity [true/false]

The number of threads used to serve requests.  This should be related to the number of queues available in your network hardware, *not* the number of cores on your machine.  Because KeyDB uses spinlocks to reduce latency; making this too high will reduce performance.  We recommend using 4 here.  By default this is set to one.

    scratch-file-path /path

If you would like to use the [FLASH backed](https://github.com/JohnSully/KeyDB/wiki/FLASH-Storage) storage this option configures the directory for KeyDB's temporary files.  This feature relies on snapshotting to work so must be used on a BTRFS filesystem.  ZFS may also work but is untested.  With this feature KeyDB will use RAM as a cache and page to disk as necessary.  NOTE: This requires special compilation options, see Building KeyDB below.
    
    db-s3-object /path/to/bucket

If you would like KeyDB to dump and load directly to AWS S3 this option specifies the bucket.  Using this option with the traditional RDB options will result in KeyDB backing up twice to both locations.  If both are specified KeyDB will first attempt to load from the local dump file and if that fails load from S3.  This requires the AWS CLI tools to be installed and configured which are used under the hood to transfer the data.

    active-replica yes

If you are using active-active replication set `active-replica` option to “yes”. This will enable both instances to accept reads and writes while remaining synced. [Click here](https://docs.keydb.dev/docs/active-rep/) to see more on active-rep in our docs section. There are also [docker examples]( https://docs.keydb.dev/docs/docker-active-rep/) on docs.

All other configuration options behave as you'd expect.  Your existing configuration files should continue to work unchanged.

Building KeyDB
--------------

KeyDB can be compiled and is tested for use on Linux.  KeyDB currently relies on SO_REUSEPORT's load balancing behavior which is available only in Linux.  When we support marshalling connections across threads we plan to support other operating systems such as FreeBSD.

Install dependencies:

    % sudo apt install build-essential nasm autotools-dev autoconf libjemalloc-dev tcl tcl-dev uuid-dev libcurl4-openssl-dev

Compiling is as simple as:

    % make

You can enable flash support with:

    % make MALLOC=memkind

***Note that the following dependencies may be needed: 
    % sudo apt-get install autoconf autotools-dev libnuma-dev libtool

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

You can find the list of all the available commands at https://docs.keydb.dev/docs/commands/

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

_Note_: `install_server.sh` will not work on Mac OSX; it is built for Linux only.

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

Docker Build
------------
Build the latest binaries from the github unstable branch within a docker container. Note this is built for Ubuntu 18.04.
Simply make a directory you would like to have the latest binaries dumped in, then run the following commmand with your updated path:
```
$ docker run -it --rm -v /path-to-dump-binaries:/keydb_bin eqalpha/keydb-build-bin
```
You should receive the following files: keydb-benchmark,  keydb-check-aof,  keydb-check-rdb,  keydb-cli,  keydb-sentinel,  keydb-server

If you are looking to enable flash support with the build (make MALLOC=memkind) then use the following command:
```
$ docker run -it --rm -v /path-to-dump-binaries:/keydb_bin eqalpha/keydb-build-bin:flash
```
Please note that you will need libcurl4-openssl-dev in order to run keydb. With flash version you may need libnuma-dev and libtool installed in order to run the binaries. Keep this in mind especially when running in a container. For a copy of all our Dockerfiles, please see them on [docs]( https://docs.keydb.dev/docs/dockerfiles/).

Code contributions
-----------------

Note: by contributing code to the KeyDB project in any form, including sending
a pull request via Github, a code fragment or patch via private email or
public discussion groups, you agree to release your code under the terms
of the BSD license that you can find in the COPYING file included in the KeyDB
source distribution.

Please see the CONTRIBUTING file in this source distribution for more
information.


