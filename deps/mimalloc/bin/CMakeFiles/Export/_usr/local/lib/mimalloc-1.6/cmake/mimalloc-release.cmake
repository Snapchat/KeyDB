#----------------------------------------------------------------
# Generated CMake target import file for configuration "Release".
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "mimalloc" for configuration "Release"
set_property(TARGET mimalloc APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(mimalloc PROPERTIES
  IMPORTED_LOCATION_RELEASE "/usr/local/lib/mimalloc-1.6/libmimalloc.so.1.6"
  IMPORTED_SONAME_RELEASE "libmimalloc.so.1.6"
  )

list(APPEND _IMPORT_CHECK_TARGETS mimalloc )
list(APPEND _IMPORT_CHECK_FILES_FOR_mimalloc "/usr/local/lib/mimalloc-1.6/libmimalloc.so.1.6" )

# Import target "mimalloc-static" for configuration "Release"
set_property(TARGET mimalloc-static APPEND PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
set_target_properties(mimalloc-static PROPERTIES
  IMPORTED_LINK_INTERFACE_LANGUAGES_RELEASE "C"
  IMPORTED_LOCATION_RELEASE "/usr/local/lib/mimalloc-1.6/libmimalloc.a"
  )

list(APPEND _IMPORT_CHECK_TARGETS mimalloc-static )
list(APPEND _IMPORT_CHECK_FILES_FOR_mimalloc-static "/usr/local/lib/mimalloc-1.6/libmimalloc.a" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
