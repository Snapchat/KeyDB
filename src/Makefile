# Redis Makefile
# Copyright (C) 2009 Salvatore Sanfilippo <antirez at gmail dot com>
# This file is released under the BSD license, see the COPYING file
#
# The Makefile composes the final FINAL_CFLAGS and FINAL_LDFLAGS using
# what is needed for KeyDB plus the standard CFLAGS and LDFLAGS passed.
# However when building the dependencies (Jemalloc, Lua, Hiredis, ...)
# CFLAGS and LDFLAGS are propagated to the dependencies, so to pass
# flags only to be used when compiling / linking KeyDB itself KEYDB_CFLAGS
# and KEYDB_LDFLAGS are used instead (this is the case of 'make gcov').
#
# Dependencies are stored in the Makefile.dep file. To rebuild this file
# Just use 'make dep', but this is only needed by developers.

# we ship KeyDB with TLS by default
# export it here as both this file and deps/Makefile uses it
export BUILD_TLS ?= yes

release_hdr := $(shell sh -c './mkreleasehdr.sh')
uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')
uname_M := $(shell sh -c 'uname -m 2>/dev/null || echo not')
OPTIMIZATION?=-O2 -flto
DEPENDENCY_TARGETS=hiredis linenoise lua hdr_histogram rocksdb
NODEPS:=clean distclean

# Default settings
STD=-pedantic -DREDIS_STATIC=''
CXX_STD=-std=c++14 -pedantic -fno-rtti -D__STDC_FORMAT_MACROS  
ifneq (,$(findstring clang,$(CC)))
  STD+=-Wno-c11-extensions
else
ifneq (,$(findstring FreeBSD,$(uname_S)))
  STD+=-Wno-c11-extensions
endif
endif
WARN=-Wall -W -Wno-missing-field-initializers -Wno-address-of-packed-member -Wno-atomic-alignment
OPT=$(OPTIMIZATION)

# Detect if the compiler supports C11 _Atomic
C11_ATOMIC := $(shell sh -c 'echo "\#include <stdatomic.h>" > foo.c; \
	$(CC) -std=c11 -c foo.c -o foo.o > /dev/null 2>&1; \
	if [ -f foo.o ]; then echo "yes"; rm foo.o; fi; rm foo.c')
ifeq ($(C11_ATOMIC),yes)
	STD+=-std=c11
else
	STD+=-std=c99
endif

PREFIX?=/usr/local
INSTALL_BIN=$(PREFIX)/bin
INSTALL=install
PKG_CONFIG?=pkg-config

# Default allocator defaults to Jemalloc if it's not an ARM
MALLOC=libc
ifneq ($(uname_M),armv6l)
ifneq ($(uname_M),armv7l)
ifeq ($(uname_S),Linux)
	MALLOC=jemalloc
endif
endif
endif

USEASM?=true
ENABLE_FLASH?=no

ifneq ($(strip $(SANITIZE)),)
	CFLAGS+= -fsanitize=$(SANITIZE) -DSANITIZE  -fno-omit-frame-pointer
	CXXFLAGS+= -fsanitize=$(SANITIZE) -DSANITIZE  -fno-omit-frame-pointer
	LDFLAGS+= -fsanitize=$(SANITIZE)
	MALLOC=libc
	USEASM=false
endif

ifeq ($(ENABLE_FLASH),yes)
	FINAL_LIBS+= -lz -lcrypto -lbz2 -lzstd -llz4 -lsnappy
	CXXFLAGS+= -I../deps/rocksdb/include/ -DENABLE_ROCKSDB
	STORAGE_OBJ+= storage/rocksdb.o storage/rocksdbfactory.o
	FINAL_LIBS+= ../deps/rocksdb/librocksdb.a 
	DEPENDENCY_TARGETS+= rocksdb
endif


ifeq ($(CHECKED),true)
	CXXFLAGS+= -DCHECKED_BUILD
endif

# Do we use our assembly spinlock? X64 only
ifeq ($(uname_S),Linux)
ifeq ($(uname_M),x86_64)
ifneq ($(TARGET32), true)
ifeq ($(USEASM),true)
	ASM_OBJ+= fastlock_x64.o
	CFLAGS+= -DASM_SPINLOCK
	CXXFLAGS+= -DASM_SPINLOCK
endif
endif
endif
endif

ifeq ($(COMPILER_NAME),clang)
	CXXFLAGS+= -stdlib=libc++ 
endif

# To get ARM stack traces if KeyDB crashes we need a special C flag.
ifneq (,$(filter aarch64 armv,$(uname_M)))
        CFLAGS+=-funwind-tables
	CXXFLAGS+=-funwind-tables
else
ifneq (,$(findstring armv,$(uname_M)))
        CFLAGS+=-funwind-tables
	CXXFLAGS+=-funwind-tables
endif
endif

# Backwards compatibility for selecting an allocator
ifeq ($(USE_TCMALLOC),yes)
	MALLOC=tcmalloc
endif

ifeq ($(USE_TCMALLOC_MINIMAL),yes)
	MALLOC=tcmalloc_minimal
endif

ifeq ($(USE_JEMALLOC),yes)
	MALLOC=jemalloc
endif

ifeq ($(USE_JEMALLOC),no)
	MALLOC=libc
endif


ifeq ($(NO_LICENSE_CHECK),yes)
	CXXFLAGS+=-DNO_LICENSE_CHECK=1
endif	

# Override default settings if possible
-include .make-settings

DEBUG=-g -ggdb
FINAL_CFLAGS=$(STD) $(WARN) $(OPT) $(DEBUG) $(CFLAGS) $(KEYDB_CFLAGS) $(REDIS_CFLAGS)
FINAL_CXXFLAGS=$(CXX_STD) $(WARN) $(OPT) $(DEBUG) $(CXXFLAGS) $(KEYDB_CFLAGS) $(REDIS_CFLAGS)
FINAL_LDFLAGS=$(LDFLAGS) $(KEYDB_LDFLAGS) $(DEBUG)
FINAL_LIBS+=-lm -lz -lcrypto -lbz2 -lzstd -llz4 -lsnappy

ifneq ($(uname_S),Darwin)
	FINAL_LIBS+=-latomic
endif
# Linux ARM32 needs -latomic at linking time
ifneq (,$(findstring armv,$(uname_M)))
	FINAL_LIBS+=-latomic
endif


ifeq ($(uname_S),SunOS)
	# SunOS
	ifeq ($(findstring -m32,$(FINAL_CFLAGS)),)
		CFLAGS+=-m64
		CXXFLAGS+= -m64
	endif
	ifeq ($(findstring -m32,$(FINAL_LDFLAGS)),)
		LDFLAGS+=-m64
	endif
	DEBUG=-g
	DEBUG_FLAGS=-g
	export CFLAGS CXXFLAGS LDFLAGS DEBUG DEBUG_FLAGS
	INSTALL=cp -pf
	FINAL_CFLAGS+= -D__EXTENSIONS__ -D_XPG6
	FINAL_CXXFLAGS+= -D__EXTENSIONS__ -D_XPG6
	FINAL_LIBS+= -ldl -lnsl -lsocket -lresolv -lpthread -lrt
else
ifeq ($(uname_S),Darwin)
	# Darwin
	FINAL_LIBS+= -ldl
	# Homebrew's OpenSSL is not linked to /usr/local to avoid
	# conflicts with the system's LibreSSL installation so it
	# must be referenced explicitly during build.
ifeq ($(uname_M),arm64)
	# Homebrew arm64 uses /opt/homebrew as HOMEBREW_PREFIX
	OPENSSL_PREFIX?=/opt/homebrew/opt/openssl
else
	# Homebrew x86/ppc uses /usr/local as HOMEBREW_PREFIX
	OPENSSL_PREFIX?=/usr/local/opt/openssl
endif
else
ifeq ($(uname_S),AIX)
        # AIX
        FINAL_LDFLAGS+= -Wl,-bexpall
        FINAL_LIBS+=-ldl -pthread -lcrypt -lbsd
else
ifeq ($(uname_S),OpenBSD)
	# OpenBSD
	FINAL_LIBS+= -lpthread
	ifeq ($(USE_BACKTRACE),yes)
	    FINAL_CFLAGS+= -DUSE_BACKTRACE -I/usr/local/include
	    FINAL_CXXFLAGS+= -DUSE_BACKTRACE -I/usr/local/include
	    FINAL_LDFLAGS+= -L/usr/local/lib
	    FINAL_LIBS+= -lexecinfo
	endif

else
ifeq ($(uname_S),NetBSD)
	# NetBSD
	FINAL_LIBS+= -lpthread
	ifeq ($(USE_BACKTRACE),yes)
	    FINAL_CFLAGS+= -DUSE_BACKTRACE -I/usr/pkg/include
	    FINAL_LDFLAGS+= -L/usr/pkg/lib
	    FINAL_LIBS+= -lexecinfo
	endif
else
ifeq ($(uname_S),FreeBSD)
	# FreeBSD
	FINAL_LIBS+= -lpthread -lexecinfo
else
ifeq ($(uname_S),DragonFly)
	# DragonFly
	FINAL_LIBS+= -lpthread -lexecinfo
else
ifeq ($(uname_S),OpenBSD)
	# OpenBSD
	FINAL_LIBS+= -lpthread -lexecinfo
else
ifeq ($(uname_S),NetBSD)
	# NetBSD
	FINAL_LIBS+= -lpthread -lexecinfo
else
ifeq ($(uname_S),Haiku)
	# Haiku
	FINAL_CFLAGS+= -DBSD_SOURCE
	FINAL_LDFLAGS+= -lbsd -lnetwork
	FINAL_LIBS+= -lpthread
else
	# All the other OSes (notably Linux)
	FINAL_LDFLAGS+= -rdynamic
	FINAL_LIBS+=-ldl -pthread -lrt -luuid
ifneq ($(NO_MOTD),yes)
	FINAL_CFLAGS += -DMOTD
	FINAL_CXXFLAGS += -DMOTD
	FINAL_LIBS+=-lcurl
endif
endif
endif
endif
endif
endif
endif
endif
endif
endif
endif

ifdef OPENSSL_PREFIX
	OPENSSL_CFLAGS=-I$(OPENSSL_PREFIX)/include
	OPENSSL_CXXFLAGS=-I$(OPENSSL_PREFIX)/include
	OPENSSL_LDFLAGS=-L$(OPENSSL_PREFIX)/lib
	# Also export OPENSSL_PREFIX so it ends up in deps sub-Makefiles
	export OPENSSL_PREFIX
endif

# Include paths to dependencies
FINAL_CFLAGS+= -I../deps/hiredis -I../deps/linenoise -I../deps/lua/src -I../deps/hdr_histogram
FINAL_CXXFLAGS+= -I../deps/hiredis -I../deps/linenoise -I../deps/lua/src -I../deps/hdr_histogram -I../deps/rocksdb/include/ -I../deps/concurrentqueue

# Determine systemd support and/or build preference (defaulting to auto-detection)
BUILD_WITH_SYSTEMD=no
LIBSYSTEMD_LIBS=-lsystemd

# If 'USE_SYSTEMD' in the environment is neither "no" nor "yes", try to
# auto-detect libsystemd's presence and link accordingly.
ifneq ($(USE_SYSTEMD),no)
	LIBSYSTEMD_PKGCONFIG := $(shell $(PKG_CONFIG) --exists libsystemd && echo $$?)
# If libsystemd cannot be detected, continue building without support for it
# (unless a later check tells us otherwise)
ifeq ($(LIBSYSTEMD_PKGCONFIG),0)
	BUILD_WITH_SYSTEMD=yes
	LIBSYSTEMD_LIBS=$(shell $(PKG_CONFIG) --libs libsystemd)
endif
endif

# If 'USE_SYSTEMD' is set to "yes" use pkg-config if available or fall back to
# default -lsystemd.
ifeq ($(USE_SYSTEMD),yes)
	BUILD_WITH_SYSTEMD=yes
endif

ifeq ($(BUILD_WITH_SYSTEMD),yes)
	FINAL_LIBS+=$(LIBSYSTEMD_LIBS)
	FINAL_CFLAGS+= -DHAVE_LIBSYSTEMD
	FINAL_CXXFLAGS+= -DHAVE_LIBSYSTEMD
endif

ifeq ($(MALLOC),tcmalloc)
	FINAL_CFLAGS+= -DUSE_TCMALLOC
	FINAL_CXXFLAGS+= -DUSE_TCMALLOC
	FINAL_LIBS+= -ltcmalloc
endif

ifeq ($(MALLOC),tcmalloc_minimal)
	FINAL_CFLAGS+= -DUSE_TCMALLOC
	FINAL_CXXFLAGS+= -DUSE_TCMALLOC
	FINAL_LIBS+= -ltcmalloc_minimal
endif

ifeq ($(MALLOC),jemalloc)
	DEPENDENCY_TARGETS+= jemalloc
	FINAL_CFLAGS+= -DUSE_JEMALLOC -I../deps/jemalloc/include
	FINAL_CXXFLAGS+= -DUSE_JEMALLOC -I../deps/jemalloc/include
	FINAL_LIBS := ../deps/jemalloc/lib/libjemalloc.a $(FINAL_LIBS)
endif

ifeq ($(MALLOC),memkind)
	DEPENDENCY_TARGETS+= memkind
	FINAL_CFLAGS+= -DUSE_MEMKIND -I../deps/memkind/src/include
	FINAL_CXXFLAGS+= -DUSE_MEMKIND -I../deps/memkind/src/include
	FINAL_LIBS := ../deps/memkind/src/.libs/libmemkind.a -lnuma $(FINAL_LIBS)
endif

ifeq ($(BUILD_TLS),yes)
	FINAL_CFLAGS+=-DUSE_OPENSSL $(OPENSSL_CFLAGS)
	FINAL_CXXFLAGS+=-DUSE_OPENSSL $(OPENSSL_CXXFLAGS)
	FINAL_LDFLAGS+=$(OPENSSL_LDFLAGS)
	LIBSSL_PKGCONFIG := $(shell $(PKG_CONFIG) --exists libssl && echo $$?)
ifeq ($(LIBSSL_PKGCONFIG),0)
	LIBSSL_LIBS=$(shell $(PKG_CONFIG) --libs libssl)
else
	LIBSSL_LIBS=-lssl
endif
	LIBCRYPTO_PKGCONFIG := $(shell $(PKG_CONFIG) --exists libcrypto && echo $$?)
ifeq ($(LIBCRYPTO_PKGCONFIG),0)
	LIBCRYPTO_LIBS=$(shell $(PKG_CONFIG) --libs libcrypto)
else
	LIBCRYPTO_LIBS=-lcrypto
endif
	FINAL_LIBS += ../deps/hiredis/libhiredis_ssl.a $(LIBSSL_LIBS) $(LIBCRYPTO_LIBS)
endif

ifndef V
    define MAKE_INSTALL
        @printf '    %b %b\n' $(LINKCOLOR)INSTALL$(ENDCOLOR) $(BINCOLOR)$(1)$(ENDCOLOR) 1>&2
        @$(INSTALL) $(1) $(2)
    endef
else
    define MAKE_INSTALL
        $(INSTALL) $(1) $(2)
    endef
endif

# Alpine OS doesn't have support for the execinfo backtrace library we use for debug, so we provide an alternate implementation using libwunwind.
OS := $(shell cat /etc/os-release | grep ID= | head -n 1 | cut -d'=' -f2)
ifeq ($(OS),alpine)
    FINAL_CXXFLAGS+=-DUNW_LOCAL_ONLY
    FINAL_LIBS += -lunwind
endif


REDIS_CC=$(QUIET_CC)$(CC) $(FINAL_CFLAGS)
REDIS_CXX=$(QUIET_CC)$(CXX) $(FINAL_CXXFLAGS)
KEYDB_AS=$(QUIET_CC) as --64 -g
REDIS_LD=$(QUIET_LINK)$(CXX) $(FINAL_LDFLAGS)
REDIS_INSTALL=$(QUIET_INSTALL)$(INSTALL)

CCCOLOR="\033[34m"
LINKCOLOR="\033[34;1m"
SRCCOLOR="\033[33m"
BINCOLOR="\033[37;1m"
MAKECOLOR="\033[32;1m"
ENDCOLOR="\033[0m"

ifndef V
QUIET_CC = @printf '    %b %b\n' $(CCCOLOR)CC$(ENDCOLOR) $(SRCCOLOR)$@$(ENDCOLOR);
QUIET_CP = @printf '    %b %b\n' $(CCCOLOR)COPY$(ENDCOLOR) $(SRCCOLOR)$@$(ENDCOLOR);
QUIET_LINK = @printf '    %b %b\n' $(LINKCOLOR)LINK$(ENDCOLOR) $(BINCOLOR)$@$(ENDCOLOR);
QUIET_INSTALL = @printf '    %b %b\n' $(LINKCOLOR)INSTALL$(ENDCOLOR) $(BINCOLOR)$@$(ENDCOLOR);
endif

REDIS_SERVER_NAME=keydb-server$(PROG_SUFFIX)
REDIS_SENTINEL_NAME=keydb-sentinel$(PROG_SUFFIX)
REDIS_SERVER_OBJ=adlist.o quicklist.o ae.o anet.o dict.o server.o sds.o zmalloc.o lzf_c.o lzf_d.o pqsort.o zipmap.o sha1.o ziplist.o release.o networking.o util.o object.o db.o replication.o rdb.o t_string.o t_list.o t_set.o t_zset.o t_hash.o t_nhash.o config.o aof.o pubsub.o multi.o debug.o sort.o intset.o syncio.o cluster.o crc16.o endianconv.o slowlog.o scripting.o bio.o rio.o rand.o memtest.o crcspeed.o crc64.o bitops.o sentinel.o notify.o setproctitle.o blocked.o hyperloglog.o latency.o sparkline.o redis-check-rdb.o redis-check-aof.o geo.o lazyfree.o module.o evict.o expire.o geohash.o geohash_helper.o childinfo.o defrag.o siphash.o rax.o t_stream.o listpack.o localtime.o acl.o storage.o rdb-s3.o fastlock.o new.o tracking.o cron.o connection.o tls.o sha256.o motd_server.o timeout.o setcpuaffinity.o AsyncWorkQueue.o snapshot.o storage/rocksdb.o storage/rocksdbfactory.o storage/teststorageprovider.o keydbutils.o StorageCache.o monotonic.o cli_common.o mt19937-64.o $(ASM_OBJ)
KEYDB_SERVER_OBJ=SnapshotPayloadParseState.o
REDIS_CLI_NAME=keydb-cli$(PROG_SUFFIX)
REDIS_CLI_OBJ=anet.o adlist.o dict.o redis-cli.o redis-cli-cpphelper.o zmalloc.o release.o anet.o ae.o crcspeed.o crc64.o siphash.o crc16.o storage-lite.o fastlock.o motd_client.o monotonic.o cli_common.o mt19937-64.o $(ASM_OBJ)
REDIS_BENCHMARK_NAME=keydb-benchmark$(PROG_SUFFIX)
REDIS_BENCHMARK_OBJ=ae.o anet.o redis-benchmark.o adlist.o dict.o zmalloc.o release.o crcspeed.o crc64.o siphash.o redis-benchmark.o storage-lite.o fastlock.o new.o monotonic.o cli_common.o mt19937-64.o $(ASM_OBJ)
REDIS_CHECK_RDB_NAME=keydb-check-rdb$(PROG_SUFFIX)
REDIS_CHECK_AOF_NAME=keydb-check-aof$(PROG_SUFFIX)
KEYDB_DIAGNOSTIC_NAME=keydb-diagnostic-tool$(PROG_SUFFIX)
KEYDB_DIAGNOSTIC_OBJ=ae.o anet.o keydb-diagnostic-tool.o adlist.o dict.o zmalloc.o release.o crcspeed.o crc64.o siphash.o keydb-diagnostic-tool.o storage-lite.o fastlock.o new.o monotonic.o cli_common.o mt19937-64.o $(ASM_OBJ)

all: $(REDIS_SERVER_NAME) $(REDIS_SENTINEL_NAME) $(REDIS_CLI_NAME) $(REDIS_BENCHMARK_NAME) $(REDIS_CHECK_RDB_NAME) $(REDIS_CHECK_AOF_NAME) $(KEYDB_DIAGNOSTIC_NAME)
	@echo ""
	@echo "Hint: It's a good idea to run 'make test' ;)"
	@echo ""

Makefile.dep:
	-$(REDIS_CC) -MM *.c > Makefile.dep 2> /dev/null || true

ifeq (0, $(words $(findstring $(MAKECMDGOALS), $(NODEPS))))
-include Makefile.dep
endif

.PHONY: all

persist-settings: distclean
	echo STD=$(STD) >> .make-settings
	echo WARN=$(WARN) >> .make-settings
	echo OPT=$(OPT) >> .make-settings
	echo MALLOC=$(MALLOC) >> .make-settings
	echo BUILD_TLS=$(BUILD_TLS) >> .make-settings
	echo USE_SYSTEMD=$(USE_SYSTEMD) >> .make-settings
	echo CFLAGS=$(CFLAGS) >> .make-settings
	echo CXXFLAGS=$(CXXFLAGS) >> .make-settings
	echo LDFLAGS=$(LDFLAGS) >> .make-settings
	echo KEYDB_CFLAGS=$(KEYDB_CFLAGS) >> .make-settings
	echo KEYDB_CXXFLAGS=$(KEYDB_CXXFLAGS) >> .make-settings
	echo KEYDB_LDFLAGS=$(KEYDB_LDFLAGS) >> .make-settings
	echo PREV_FINAL_CFLAGS=$(FINAL_CFLAGS) >> .make-settings
	echo PREV_FINAL_CXXFLAGS=$(FINAL_CXXFLAGS) >> .make-settings
	echo PREV_FINAL_LDFLAGS=$(FINAL_LDFLAGS) >> .make-settings
	-(cd modules && $(MAKE))
	-(cd ../deps && $(MAKE) $(DEPENDENCY_TARGETS))

.PHONY: persist-settings

# Prerequisites target
# Clean everything, persist settings and build dependencies if anything changed
ifneq ($(strip $(PREV_FINAL_CFLAGS)), $(strip $(FINAL_CFLAGS)))
.make-prerequisites: persist-settings
else ifneq ($(strip $(PREV_FINAL_CXXFLAGS)), $(strip $(FINAL_CXXFLAGS)))
.make-prerequisites: persist-settings
else ifneq ($(strip $(PREV_FINAL_LDFLAGS)), $(strip $(FINAL_LDFLAGS)))
.make-prerequisites: persist-settings
else
.make-prerequisites:
endif
	@touch $@

# keydb-server
$(REDIS_SERVER_NAME): $(REDIS_SERVER_OBJ) $(KEYDB_SERVER_OBJ)
	$(REDIS_LD) -o $@ $^ ../deps/hiredis/libhiredis.a ../deps/lua/src/liblua.a ../deps/rocksdb/librocksdb.a $(FINAL_LIBS)

# keydb-sentinel
$(REDIS_SENTINEL_NAME): $(REDIS_SERVER_NAME)
	$(REDIS_INSTALL) $(REDIS_SERVER_NAME) $(REDIS_SENTINEL_NAME)

# keydb-check-rdb
$(REDIS_CHECK_RDB_NAME): $(REDIS_SERVER_NAME)
	$(REDIS_INSTALL) $(REDIS_SERVER_NAME) $(REDIS_CHECK_RDB_NAME)

# keydb-check-aof
$(REDIS_CHECK_AOF_NAME): $(REDIS_SERVER_NAME)
	$(REDIS_INSTALL) $(REDIS_SERVER_NAME) $(REDIS_CHECK_AOF_NAME)

# keydb-cli
$(REDIS_CLI_NAME): $(REDIS_CLI_OBJ)
	$(REDIS_LD) -o $@ $^ ../deps/hiredis/libhiredis.a ../deps/linenoise/linenoise.o $(FINAL_LIBS)

# keydb-benchmark
$(REDIS_BENCHMARK_NAME): $(REDIS_BENCHMARK_OBJ)
	$(REDIS_LD) -o $@ $^ ../deps/hiredis/libhiredis.a ../deps/hdr_histogram/hdr_histogram.o $(FINAL_LIBS)

# keydb-diagnostic-tool
$(KEYDB_DIAGNOSTIC_NAME): $(KEYDB_DIAGNOSTIC_OBJ)
	$(REDIS_LD) -o $@ $^ ../deps/hiredis/libhiredis.a $(FINAL_LIBS)

DEP = $(REDIS_SERVER_OBJ:%.o=%.d) $(KEYDB_SERVER_OBJ:%.o=%.d) $(REDIS_CLI_OBJ:%.o=%.d) $(REDIS_BENCHMARK_OBJ:%.o=%.d)
-include $(DEP)

# Because the jemalloc.h header is generated as a part of the jemalloc build,
# building it should complete before building any other object. Instead of
# depending on a single artifact, build all dependencies first.
motd_client.o: motd.cpp .make-prerequisites
	$(REDIS_CXX) -MMD -o motd_client.o -c $< -DCLIENT -fno-lto

motd_server.o: motd.cpp .make-prerequisites
	$(REDIS_CXX) -MMD -o motd_server.o -c $< -DSERVER

%.o: %.c .make-prerequisites
	$(REDIS_CC) -MMD -o $@ -c $<

%.o: %.cpp .make-prerequisites
	$(REDIS_CXX) -MMD -o $@ -c $<

%.o: %.asm .make-prerequisites
	$(KEYDB_AS) $< -o $@

clean:
	rm -rf $(REDIS_SERVER_NAME) $(REDIS_SENTINEL_NAME) $(REDIS_CLI_NAME) $(REDIS_BENCHMARK_NAME) $(REDIS_CHECK_RDB_NAME) $(REDIS_CHECK_AOF_NAME) $(KEYDB_DIAGNOSTIC_NAME) *.o *.gcda *.gcno *.gcov KeyDB.info lcov-html Makefile.dep
	rm -rf storage/*.o
	rm -rf keydb-server
	rm -f $(DEP)

.PHONY: clean

distclean: clean
	-(cd ../deps && $(MAKE) distclean)
	-(cd modules && $(MAKE) clean)
	-(cd ../tests/modules && $(MAKE) clean)
	-(rm -f .make-*)

.PHONY: distclean

test: $(REDIS_SERVER_NAME) $(REDIS_CHECK_AOF_NAME) $(REDIS_CLI_NAME) $(REDIS_BENCHMARK_NAME)
	@(cd ..; ./runtest)

test-modules: $(REDIS_SERVER_NAME)
	@(cd ..; ./runtest-moduleapi)

test-sentinel: $(REDIS_SENTINEL_NAME) $(REDIS_CLI_NAME)
	@(cd ..; ./runtest-sentinel)

check: test

lcov:
	$(MAKE) gcov
	@(set -e; cd ..; ./runtest --config server-threads 3; ./runtest-sentinel; ./runtest-cluster; ./runtest-moduleapi)
	@geninfo -o KeyDB.info --no-external .
	@genhtml --legend -o lcov-html KeyDB.info
	@genhtml --legend -o lcov-html KeyDB.info | grep lines | awk '{print $$2;}' | sed 's/%//g'

.PHONY: lcov

bench: $(REDIS_BENCHMARK_NAME)
	./$(REDIS_BENCHMARK_NAME)

32bit:
	@echo ""
	@echo "WARNING: if it fails under Linux you probably need to install libc6-dev-i386"
	@echo ""
	$(MAKE) CXXFLAGS="-m32" CFLAGS="-m32" LDFLAGS="-m32" TARGET32=true

gcov:
	$(MAKE) KEYDB_CXXFLAGS="-fprofile-arcs -ftest-coverage -DCOVERAGE_TEST" KEYDB_CFLAGS="-fprofile-arcs -ftest-coverage -DCOVERAGE_TEST" KEYDB_LDFLAGS="-fprofile-arcs -ftest-coverage"

noopt:
	$(MAKE) OPTIMIZATION="-O0"

valgrind:
	$(MAKE) OPTIMIZATION="-O0" USEASM="false" MALLOC="libc" CFLAGS="-DSANITIZE" CXXFLAGS="-DSANITIZE"

helgrind:
	$(MAKE) OPTIMIZATION="-O0" MALLOC="libc" CFLAGS="-D__ATOMIC_VAR_FORCE_SYNC_MACROS" KEYDB_CFLAGS="-I/usr/local/include" KEYDB_LDFLAGS="-L/usr/local/lib"

src/help.h:
	@../utils/generate-command-help.rb > help.h

install: all
	@mkdir -p $(INSTALL_BIN)
	$(call MAKE_INSTALL,$(REDIS_SERVER_NAME),$(INSTALL_BIN))
	$(call MAKE_INSTALL,$(REDIS_BENCHMARK_NAME),$(INSTALL_BIN))
	$(call MAKE_INSTALL,$(REDIS_CLI_NAME),$(INSTALL_BIN))
	@ln -sf $(REDIS_SERVER_NAME) $(INSTALL_BIN)/$(REDIS_CHECK_RDB_NAME)
	@ln -sf $(REDIS_SERVER_NAME) $(INSTALL_BIN)/$(REDIS_CHECK_AOF_NAME)
	@ln -sf $(REDIS_SERVER_NAME) $(INSTALL_BIN)/$(REDIS_SENTINEL_NAME)

uninstall:
	rm -f $(INSTALL_BIN)/{$(REDIS_SERVER_NAME),$(REDIS_BENCHMARK_NAME),$(REDIS_CLI_NAME),$(REDIS_CHECK_RDB_NAME),$(REDIS_CHECK_AOF_NAME),$(REDIS_SENTINEL_NAME),$(KEYDB_DIAGNOSTIC_NAME)}
