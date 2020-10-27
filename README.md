<p align="center">
  <a href="https://fluidb.icu"><img src="/img/Logo.png" alt="fluidB"></a>
</p>
<p align="center">
    

##### Need Help? Check out our extensive [documentation](https://docs.fluidB.dev).


## What is fluidB?


Fluidb is an open source (BSD-3-Clause License), in-memory database management system, distributed uder BSD-3-Clause License using code of [KeyDB](https://github.com/JohnSully/KeyDB), which also distributed uder BSD-3-Clause License. 
The purpose of our project is to fix fundamental flaws in Redis, such as scaling, creating a multi-threaded server.

fluidB is often referred to as a *data structures* server. What this means is that fluidB provides access to mutable data structures via a set of commands, which are sent using a *server-client* model with TCP sockets and a simple protocol. So different processes can query and modify the same data structures in a shared way.
The storage of fluidB is implemented as follows: data can be stored according to the "key-value" model, or can be stored as a graph, which is a chain of interrelated events (which are similar to frames from an old film strip), which together represent a description of some event.

Good example is to think of fluidB as a more complex version of memcached, where the operations are not just SETs and GETs, but operations that work with complex data types like Lists, Sets, ordered data structures, and so forth.
<br/><br/>

## Gratitudes

* **Salvatore Sanfilippo (antirez)**, I would like to express our gratitude for all that you have done!!! Thank you.
* **Oran Agra**, **Yossi Gottlieb** I wish to express my appreciation for all your efforts!!!
* **John Sully** Many thanks for everything you have done for us!!!



New Configuration Options
-------------------------

With new features comes new options:

    server-threads N
    server-thread-affinity [true/false]

The number of threads used to serve requests.  This should be related to the number of queues available in your network hardware, *not* the number of cores on your machine.  Because fluidB uses spinlocks to reduce latency; making this too high will reduce performance.  We recommend using 4 here.  By default this is set to one.

    scratch-file-path /path

If you would like to use the [FLASH backed](https://github.com/JohnSully/fluidB/wiki/FLASH-Storage) storage this option configures the directory for fluidB's temporary files.  This feature relies on snapshotting to work so must be used on a BTRFS filesystem.  ZFS may also work but is untested.  With this feature fluidB will use RAM as a cache and page to disk as necessary.  NOTE: This requires special compilation options, see Building fluidB below.
    
    db-s3-object /path/to/bucket

If you would like fluidB to dump and load directly to AWS S3 this option specifies the bucket.  Using this option with the traditional RDB options will result in fluidB backing up twice to both locations.  If both are specified fluidB will first attempt to load from the local dump file and if that fails load from S3.  This requires the AWS CLI tools to be installed and configured which are used under the hood to transfer the data.

    active-replica yes

If you are using active-active replication set `active-replica` option to “yes”. This will enable both instances to accept reads and writes while remaining synced. [Click here](https://docs.fluidB.dev/docs/active-rep/) to see more on active-rep in our docs section. There are also [docker examples]( https://docs.fluidB.dev/docs/docker-active-rep/) on docs.

All other configuration options behave as you'd expect.  Your existing configuration files should continue to work unchanged.

Building fluidB
--------------

fluidB can be compiled and is tested for use on Linux.  fluidB currently relies on SO_REUSEPORT's load balancing behavior which is available only in Linux.  When we support marshalling connections across threads we plan to support other operating systems such as FreeBSD.

Install dependencies:

    % sudo apt install build-essential nasm autotools-dev autoconf libjemalloc-dev tcl tcl-dev uuid-dev libcurl4-openssl-dev

Compiling is as simple as:

    % make

To build with TLS support, you'll need OpenSSL development libraries (e.g.
libssl-dev on Debian/Ubuntu) and run:

    % make BUILD_TLS=yes

To build with systemd support, you'll need systemd development libraries (such 
as libsystemd-dev on Debian/Ubuntu or systemd-devel on CentOS) and run:

    % make USE_SYSTEMD=yes


***Note that the following dependencies may be needed: 
    % sudo apt-get install autoconf autotools-dev libnuma-dev libtool

If TLS is built, running the tests with TLS enabled (you will need `tcl-tls`
installed):

    % ./utils/gen-test-certs.sh
    % ./runtest --tls


Fixing build problems with dependencies or cached build options
---------

fluidB has some dependencies which are included into the `deps` directory.
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

If after building fluidB with a 32 bit target you need to rebuild it
with a 64 bit target, or the other way around, you need to perform a
`make distclean` in the root directory of the fluidB distribution.

In case of build errors when trying to build a 32 bit binary of fluidB, try
the following steps:

* Install the packages libc6-dev-i386 (also try g++-multilib).
* Try using the following command line instead of `make 32bit`:
  `make CFLAGS="-m32 -march=native" LDFLAGS="-m32"`

Allocator
---------

Selecting a non-default memory allocator when building fluidB is done by setting
the `MALLOC` environment variable. fluidB is compiled and linked against libc
malloc by default, with the exception of jemalloc being the default on Linux
systems. This default was picked because jemalloc has proven to have fewer
fragmentation problems than libc malloc.

To force compiling against libc malloc, use:

    % make MALLOC=libc

To compile against jemalloc on Mac OS X systems, use:

    % make MALLOC=jemalloc

Verbose build
-------------

fluidB will build with a user friendly colorized output by default.
If you want to see a more verbose output use the following:

    % make V=1

Running fluidB
-------------

To run fluidB with the default configuration just type:

    % cd src
    % ./fluidB-server

If you want to provide your fluidB.conf, you have to run it using an additional
parameter (the path of the configuration file):

    % cd src
    % ./fluidB-server /path/to/fluidB.conf

It is possible to alter the fluidB configuration by passing parameters directly
as options using the command line. Examples:

    % ./fluidB-server --port 9999 --replicaof 127.0.0.1 6379
    % ./fluidB-server /etc/fluidB/6379.conf --loglevel debug

All the options in fluidB.conf are also supported as options using the command
line, with exactly the same name.


Running Redis with TLS:
------------------

Please consult the [TLS.md](TLS.md) file for more information on
how to use Redis with TLS.


Playing with fluidB
------------------

You can use fluidB-cli to play with fluidB. Start a fluidB-server instance,
then in another terminal try the following:

    % cd src
    % ./fluidB-cli
    fluidB> ping
    PONG
    fluidB> set foo bar
    OK
    fluidB> get foo
    "bar"
    fluidB> incr mycounter
    (integer) 1
    fluidB> incr mycounter
    (integer) 2
    fluidB>

You can find the list of all the available commands at https://docs.fluidB.dev/docs/commands/

Installing fluidB
-----------------

In order to install fluidB binaries into /usr/local/bin just use:

    % make install

You can use `make PREFIX=/some/other/directory install` if you wish to use a
different destination.

Make install will just install binaries in your system, but will not configure
init scripts and configuration files in the appropriate place. This is not
needed if you want just to play a bit with fluidB, but if you are installing
it the proper way for a production system, we have a script doing this
for Ubuntu and Debian systems:

    % cd utils
    % ./install_server.sh

_Note_: `install_server.sh` will not work on Mac OSX; it is built for Linux only.

The script will ask you a few questions and will setup everything you need
to run fluidB properly as a background daemon that will start again on
system reboots.

You'll be able to stop and start fluidB using the script named
`/etc/init.d/fluidB_<portnumber>`, for instance `/etc/init.d/fluidB_6379`.

Multithreading Architecture
---------------------------

fluidB works by running the normal Redis event loop on multiple threads.  Network IO, and query parsing are done concurrently.  Each connection is assigned a thread on accept().  Access to the core hash table is guarded by spinlock.  Because the hashtable access is extremely fast this lock has low contention.  Transactions hold the lock for the duration of the EXEC command.  Modules work in concert with the GIL which is only acquired when all server threads are paused.  This maintains the atomicity guarantees modules expect.

Unlike most databases the core data structure is the fastest part of the system.  Most of the query time comes from parsing the REPL protocol and copying data to/from the network.

Future work:
 - Allow rebalancing of connections to different threads after the connection
 - Allow multiple readers access to the hashtable concurrently

Docker Build
------------
Build the latest binaries from the github unstable branch within a docker container. Note this is built for Ubuntu 18.04.
Simply make a directory you would like to have the latest binaries dumped in, then run the following commmand with your updated path:
```
$ docker run -it --rm -v /path-to-dump-binaries:/fluidB_bin eqalpha/fluidB-build-bin
```
You should receive the following files: fluidB-benchmark,  fluidB-check-aof,  fluidB-check-rdb,  fluidB-cli,  fluidB-sentinel,  fluidB-server

If you are looking to enable flash support with the build (make MALLOC=memkind) then use the following command:
```
$ docker run -it --rm -v /path-to-dump-binaries:/fluidB_bin eqalpha/fluidB-build-bin:flash
```
Please note that you will need libcurl4-openssl-dev in order to run fluidB. With flash version you may need libnuma-dev and libtool installed in order to run the binaries. Keep this in mind especially when running in a container. For a copy of all our Dockerfiles, please see them on [docs]( https://docs.fluidB.dev/docs/dockerfiles/).

Code contributions
-----------------

Note: by contributing code to the fluidB project in any form, including sending
a pull request via Github, a code fragment or patch via private email or
public discussion groups, you agree to release your code under the terms
of the BSD license that you can find in the COPYING file included in the fluidB
source distribution.

Please see the CONTRIBUTING file in this source distribution for more
information.


