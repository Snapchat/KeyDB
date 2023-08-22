get_filename_component(LIBRARY_NAME "${CMAKE_CURRENT_SOURCE_DIR}" NAME)

set(Header_Files
        "AsyncWorkQueue.h"
        "IStorage.h"
        "SnapshotPayloadParseState.h"
        "StorageCache.h"
        "adlist.h"
        "ae.h"
        "aelocker.h"
        "anet.h"
        "asciilogo.h"
        "atomicvar.h"
        "bio.h"
        "cli_common.h"
        "cluster.h"
        "compactvector.h"
        "config.h"
        "connection.h"
        "connhelpers.h"
        "cowptr.h"
        "crc16_slottable.h"
        "crc64.h"
        "crcspeed.h"
        "cron.h"
        "debugmacro.h"
        "dict.h"
        "endianconv.h"
        "expire.h"
        "fastlock.h"
        "fmacros.h"
        "gc.h"
        "geo.h"
        "geohash.h"
        "geohash_helper.h"
        "help.h"
        "intset.h"
        "latency.h"
        "listpack.h"
        "listpack_malloc.h"
        "lolwut.h"
        "lzf.h"
        "lzfP.h"
        "monotonic.h"
        "motd.h"
        "mt19937-64.h"
        "new.h"
        "pqsort.h"
        "quicklist.h"
        "rand.h"
        "rax.h"
        "rax_malloc.h"
        "rdb.h"
        "readwritelock.h"
        "redis-cli.h"
        "redisassert.h"
        "redismodule.h"
        "rio.h"
        "sds.h"
        "sdsalloc.h"
        "semiorderedset.h"
        "server.h"
        "serverassert.h"
        "sha1.h"
        "sha256.h"
        "slowlog.h"
        "solarisfixes.h"
        "sparkline.h"
        "storage.h"
        "stream.h"
        "t_nhash.h"
        "testhelp.h"
        "util.h"
        "uuid.h"
        "version.h"
        "ziplist.h"
        "zipmap.h"
        "zmalloc.h"
        )
source_group("Header Files" FILES "${Header_Files}")

set(Source_Files
        "AsyncWorkQueue.cpp"
        "SnapshotPayloadParseState.cpp"
        "StorageCache.cpp"
        "acl.cpp"
        "adlist.c"
        "ae.cpp"
        "ae_epoll.cpp"
        "ae_evport.c"
        "ae_kqueue.c"
        "ae_select.c"
        "anet.c"
        "aof.cpp"
        "bio.cpp"
        "bitops.cpp"
        "blocked.cpp"
        "childinfo.cpp"
        "cli_common.c"
        "cluster.cpp"
        "config.cpp"
        "connection.cpp"
        "crc16.c"
        "crc64.c"
        "crcspeed.c"
        "cron.cpp"
        "db.cpp"
        "debug.cpp"
        "defrag.cpp"
        "dict.cpp"
        "endianconv.c"
        "evict.cpp"
        "expire.cpp"
        "fastlock.cpp"
        "geo.cpp"
        "geohash.c"
        "geohash_helper.cpp"
        "hyperloglog.cpp"
        "intset.c"
        "keydb-diagnostic-tool.cpp"
        "keydbutils.cpp"
        "latency.cpp"
        "lazyfree.cpp"
        "listpack.c"
        "localtime.c"
        "lolwut.c"
        "lolwut5.c"
        "lolwut6.c"
        "lzf_c.c"
        "lzf_d.c"
        "meminfo.cpp"
        "memtest.c"
        "module.cpp"
        "monotonic.c"
        "motd.cpp"
        "mt19937-64.c"
        "multi.cpp"
        "networking.cpp"
        "new.cpp"
        "notify.cpp"
        "object.cpp"
        "pqsort.c"
        "pubsub.cpp"
        "quicklist.c"
        "rand.c"
        "rax.c"
        "rdb-s3.cpp"
        "rdb.cpp"
        "redis-benchmark.cpp"
        "redis-check-aof.cpp"
        "redis-check-rdb.cpp"
        "redis-cli-cpphelper.cpp"
        "redis-cli.c"
        "release.c"
        "replication.cpp"
        "rio.cpp"
        "scripting.cpp"
        "sds.c"
        "sentinel.cpp"
        "server.cpp"
        "setcpuaffinity.c"
        "setproctitle.c"
        "sha1.c"
        "sha256.c"
        "siphash.c"
        "slowlog.cpp"
        "snapshot.cpp"
        "sort.cpp"
        "sparkline.cpp"
        "storage-lite.c"
        "storage.cpp"
        "syncio.cpp"
        "t_hash.cpp"
        "t_list.cpp"
        "t_nhash.cpp"
        "t_set.cpp"
        "t_stream.cpp"
        "t_string.cpp"
        "t_zset.cpp"
        "timeout.cpp"
        "tls.cpp"
        "tracking.cpp"
        "util.c"
        "ziplist.c"
        "zipmap.c"
        "zmalloc.cpp"
        )
source_group("Source Files" FILES "${Source_Files}")

add_library("${LIBRARY_NAME}" "${Header_Files}" "${Source_Files}")

target_link_libraries("${LIBRARY_NAME}" PRIVATE "${PROJECT_NAME}_compiler_flags")

include(GenerateExportHeader)
set(_export_file "${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}_export.h")
generate_export_header("${LIBRARY_NAME}" EXPORT_FILE_NAME "${_export_file}")

include(GNUInstallDirs)
target_include_directories(
        "${LIBRARY_NAME}"
        PUBLIC
        "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>"
        "$<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>"
        "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>"
)

#=========#
# Install #
#=========#

# setup the version numbering
set_property(TARGET "${LIBRARY_NAME}" PROPERTY VERSION "1.0.0")
set_property(TARGET "${LIBRARY_NAME}" PROPERTY SOVERSION "1")

install(FILES ${Header_Files} "${_export_file}"
        TYPE "INCLUDE")


#====================#
# Package to install #
#====================#

set(installable_libs "${LIBRARY_NAME}" "${PROJECT_NAME}_compiler_flags")
add_subdirectory("modules")
if (TARGET "${DEPENDANT_LIBRARY}")
    list(APPEND installable_libs "${DEPENDANT_LIBRARY}")
endif ()
install(TARGETS ${installable_libs}
        EXPORT "${LIBRARY_NAME}Targets"
        ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
        LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
        RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}")
install(EXPORT "${LIBRARY_NAME}Targets" DESTINATION "${CMAKE_INSTALL_DATADIR}/${PROJECT_NAME}")
