MEMKIND
=======

[![Build Status](https://travis-ci.org/memkind/memkind.svg)](https://travis-ci.org/memkind/memkind)
[![MEMKIND version](https://img.shields.io/github/tag/memkind/memkind.svg)](https://github.com/memkind/memkind/releases/latest)
[![Coverage Status](http://codecov.io/github/memkind/memkind/coverage.svg?branch=master)](http://codecov.io/gh/memkind/memkind?branch=master)

DISCLAIMER
----------
SEE COPYING FILE FOR LICENSE INFORMATION.

THIS SOFTWARE IS PROVIDED AS A DEVELOPMENT SNAPSHOT TO AID
COLLABORATION AND WAS NOT ISSUED AS A RELEASED PRODUCT BY INTEL.


LAST UPDATE
-----------
Krzysztof Kulakowski <krzysztof.kulakowski@intel.com>
Tuesday Mar 1 2016

SUMMARY
-------
The memkind library is a user extensible heap manager built on top of
jemalloc which enables control of memory characteristics and a
partitioning of the heap between kinds of memory.  The kinds of memory
are defined by operating system memory policies that have been applied
to virtual address ranges.  Memory characteristics supported by
memkind without user extension include control of NUMA and page size
features.  The jemalloc non-standard interface has been extended to
enable specialized arenas to make requests for virtual memory from the
operating system through the memkind partition interface.  Through the
other memkind interfaces the user can control and extend memory
partition features and allocate memory while selecting enabled
features.

API
---
The memkind library delivers two interfaces:
 * hbwmalloc.h - recommended for high-bandwidth memory use cases (stable)
 * memkind.h - generic interface for more complex use cases (partially unstable)

For more detailed information about those interfaces see
corresponding manpages (located in man/ subdir):

    man memkind

or

    man hbwmalloc

BUILD REQUIREMENTS
------------------
To build the memkind libraries on a RHEL Linux system first install
the required packages with the following command:

        sudo yum install numactl-devel gcc-c++ unzip

On a SLES Linux system install the dependencies with the following
command:

        sudo zypper install libnuma-devel gcc-c++ unzip

The memkind library uses the GNU autotools (Autoconf, Automake,
Libtool and m4) for configuration and build.  The configure scripts
and gtest source code are distributed with the source tarball included
in the source RPM, and this tarball is created with the memkind "make
dist" target.  In contrast to the distributed source tarball, the git
repository does not include any generated files nor the gtest source
code.  For this reason some additional steps are required when
building from a checkout of the git repo.  Those steps include running
the bash script called "autogen.sh" prior to configure.  This script
will populate a VERSION file based on "git describe", and use
autoreconf to generate a configure script.  The gtest library is
required for building the test content.  This is downloaded
automatically prior to building the test content from the googlecode
website based on a target describe in memkind/test/Makefile.mk.

BUILDING
--------

a) Building jemalloc

The memkind library has a dependency on a related fork of jemalloc.

The jemalloc source was forked from jemalloc version 5.0.  This source tree
is located within the jemalloc subdirectory of the memkind source.  The jemalloc
source code has been kept close to the original form, and in particular
the build system has been lightly modified.
The developer must configure and build jemalloc prior to configuring
and building memkind.  You can do that using included shell script:

    export JE_PREFIX=jemk_
    ./build_jemalloc.sh

Alternatively you can follow this step-by-step instruction:

    cd jemalloc
    autoconf
    mkdir obj
    cd obj
    ../configure --enable-autogen --with-jemalloc-prefix=jemk_ --without-export \
                 --disable-stats --disable-fill \
                 --with-malloc-conf="narenas:256,lg_tcache_max:12"
    make
    cd ../..

Note: JE_PREFIX can be set to arbitrary value, including empty one.

b) Building memkind

Building and installing memkind can be as simple as typing the following while
in the root directory of the source tree:

    ./build.sh
    make install

Alternatively you can follow this step-by-step instruction:

    export JE_PREFIX=jemk_
    ./autogen.sh
    ./configure
    make
    make install

this should configure, build, and install this package.

See the output of:

    ./configure --help

for more information about either the memkind or the jemalloc
configuration options.  Some useful information about building with autotools
can also be found in the INSTALL file.

**Important Notes:**

If you are using build.sh script and later want to call 'make' command directly,
then you need to call firstly:

    export JE_PREFIX=jemk_

otherwise you will get an error like:

    undefined reference to mallocx

RUN REQUIREMENTS
----------------
Requires kernel patch introduced in Linux v3.11 that impacts
functionality of the NUMA system calls.  This is patch is commit
3964acd0dbec123aa0a621973a2a0580034b4788 in the linux-stable git
repository from kernel.org.  Red Hat has back-ported this patch to the
v3.10 kernel in the RHEL 7.0 GA release, so RHEL 7.0 onward supports
memkind even though this kernel version predates v3.11.

Functionality related to hugepages allocation require patches
e0ec90ee7e6f6cbaa6d59ffb48d2a7af5e80e61d and
099730d67417dfee273e9b10ac2560ca7fac7eb9 from kernel org. Without them
physical memory may end up being located on incorrect NUMA node.

Applications using the memkind library require that libnuma and
libpthread be available for dynamic linking at run time.  Both of
these libraries should be available by default, but they can be
installed on RHEL Linux with the following command:

    sudo yum install pthread numactl

and on a SLES Linux system with:

    sudo zypper install pthread libnuma

To use the interfaces for obtaining 2MB pages please be sure
to follow the instructions here:
    https://www.kernel.org/doc/Documentation/vm/hugetlbpage.txt
and pay particular attention to the use of the procfs files:
    /proc/sys/vm/nr_hugepages
    /proc/sys/vm/nr_overcommit_hugepages
for enabling the kernel's huge page pool.

To use the file-backed kind of memory (PMEM), please be sure that
filesystem which is used for PMEM kind supports FALLOC_FL_PUNCH_HOLE flag:
    http://man7.org/linux/man-pages/man2/fallocate.2.html

SETTING LOGGING MECHANISM
-------------------------
In memkind library logging mechanism could be enabled by setting MEMKIND_DEBUG
environment variable. Setting MEMKIND_DEBUG to "1" enables printing messages
like errors and general information about environment to stderr.

SETTING HEAP MANAGER
--------------------
In memkind library heap management can be adjusted with MEMKIND_HEAP_MANAGER
environment variable, which allows for switching to one of the available
heap managers.
Values:
    JEMALLOC – sets the jemalloc heap manager
    TBB – sets Intel Threading Building Blocks heap manager. This option requires installed
    Intel Threading Building Blocks library.
If the MEMKIND_HEAP_MANAGER is not set then the jemalloc heap manager will be used by default.

TESTING
-------
All existing tests pass. For more information on how to execute tests
see the CONTRIBUTING file.

When tests are run on a NUMA platform without high bandwidth memory
the MEMKIND_HBW_NODES environment variable is used in conjunction with
"numactl --membind" to force standard allocations to one NUMA node and
high bandwidth allocations through a different NUMA node.  See next
section for more details.


SIMULATE HIGH BANDWIDTH MEMORY
------------------------------
A method for testing for the benefit of high bandwidth memory on a
dual socket Intel(R) Xeon(TM) system is to use the QPI bus to simulate
slow memory.  This is not an accurate model of the bandwidth and
latency characteristics of the Intel's 2nd generation Intel(R) Xeon Phi(TM)
Product Family on package memory, but is a reasonable way to determine
which data structures rely critically on bandwidth.

If the application a.out has been modified to use high bandwidth
memory with the memkind library then this can be done with numactl as
follows with the bash shell:

    export MEMKIND_HBW_NODES=0
    numactl --membind=1 --cpunodebind=0 a.out

or with csh:

    setenv MEMKIND_HBW_NODES 0
    numactl --membind=1 --cpunodebind=0 a.out

The MEMKIND_HBW_NODES environment variable set to zero will bind high
bandwidth allocations to NUMA node 0.  The --membind=1 flag to numactl
will bind standard allocations, static and stack variables to NUMA
node 1.  The --cpunodebind=0 option to numactl will bind the process
threads to CPUs associated with NUMA node 0.  With this configuration
standard allocations will be fetched across the QPI bus, and high
bandwidth allocations will be local to the process CPU.

NOTES
-----
* Using memkind with Transparent Huge Pages enabled may result in
 undesirably high memory footprint. To avoid that disable THP using following
 instruction: https://www.kernel.org/doc/Documentation/vm/transhuge.txt

STATUS
------
Different interfaces can represent different maturity level
(as described in corresponding man pages).
Feedback on design and implementation is greatly appreciated.
