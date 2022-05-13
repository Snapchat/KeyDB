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
        "adlist.c"
        "ae_evport.c"
        "ae_kqueue.c"
        "ae_select.c"
        "anet.c"
        "cli_common.c"
        "crc16.c"
        "crc64.c"
        "crcspeed.c"
        "endianconv.c"
        "geohash.c"
        "intset.c"
        "listpack.c"
        "localtime.c"
        "lolwut.c"
        "lolwut5.c"
        "lolwut6.c"
        "lzf_c.c"
        "lzf_d.c"
        "memtest.c"
        "monotonic.c"
        "mt19937-64.c"
        "pqsort.c"
        "quicklist.c"
        "rand.c"
        "rax.c"
        "redis-cli.c"
        "release.c"
        "sds.c"
        "setcpuaffinity.c"
        "setproctitle.c"
        "sha1.c"
        "sha256.c"
        "siphash.c"
        "storage-lite.c"
        "util.c"
        "ziplist.c"
        "zipmap.c"
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
if (TARGET "${DEPENDANT_LIBRARY}")
    list(APPEND installable_libs "${DEPENDANT_LIBRARY}")
endif ()
install(TARGETS ${installable_libs}
        EXPORT "${LIBRARY_NAME}Targets"
        ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
        LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
        RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}")
install(EXPORT "${LIBRARY_NAME}Targets" DESTINATION "${CMAKE_INSTALL_DATADIR}/${PROJECT_NAME}")
