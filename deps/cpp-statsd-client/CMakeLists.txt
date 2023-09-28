# Basic project setup
cmake_minimum_required(VERSION 3.5)
project(cpp-statsd-client
        VERSION 1.0.2
        LANGUAGES CXX
        DESCRIPTION "A header-only StatsD client implemented in C++"
        HOMEPAGE_URL "https://github.com/vthiery/cpp-statsd-client")

option(CPP_STATSD_STANDALONE "Allows configuration of targets for verifying library functionality" ON)
option(ENABLE_TESTS "Build tests" ON)
option(ENABLE_COVERAGE "Build with coverage instrumentalisation" OFF)

if(NOT CPP_STATSD_STANDALONE)
  set(ENABLE_TESTS OFF)
  set(ENABLE_COVERAGE OFF)
endif()

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
find_package(Threads)

# Optional code coverage targets
if(ENABLE_COVERAGE)
  set(COVERAGE_EXCLUDES /usr/*)
  include(${PROJECT_SOURCE_DIR}/cmake/CodeCoverage.cmake)
  APPEND_COVERAGE_COMPILER_FLAGS()
  SETUP_TARGET_FOR_COVERAGE_LCOV(NAME coverage
                                 EXECUTABLE testStatsdClient
                                 DEPENDENCIES ${PROJECT_NAME}
  )
endif()

# The library target
add_library(${PROJECT_NAME} INTERFACE)
target_include_directories(
  ${PROJECT_NAME}
  INTERFACE $<BUILD_INTERFACE:${${PROJECT_NAME}_SOURCE_DIR}/include>
            $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
target_link_libraries(${PROJECT_NAME} INTERFACE Threads::Threads)
if(WIN32)
  target_link_libraries(${PROJECT_NAME} INTERFACE ws2_32)
endif()

# The installation and pkg-config-like cmake config
install(TARGETS ${PROJECT_NAME}
        EXPORT ${PROJECT_NAME}_Targets
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
write_basic_package_version_file("${PROJECT_NAME}ConfigVersion.cmake"
                                 VERSION ${PROJECT_VERSION}
                                 COMPATIBILITY SameMajorVersion)
configure_package_config_file(
  "${PROJECT_SOURCE_DIR}/cmake/${PROJECT_NAME}Config.cmake.in"
  "${PROJECT_BINARY_DIR}/${PROJECT_NAME}Config.cmake"
  INSTALL_DESTINATION
  ${CMAKE_INSTALL_DATAROOTDIR}/${PROJECT_NAME}/cmake)
install(EXPORT ${PROJECT_NAME}_Targets
        FILE ${PROJECT_NAME}Targets.cmake
        NAMESPACE ${PROJECT_NAME}::
        DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/${PROJECT_NAME}/cmake)
install(FILES "${PROJECT_BINARY_DIR}/${PROJECT_NAME}Config.cmake"
              "${PROJECT_BINARY_DIR}/${PROJECT_NAME}ConfigVersion.cmake"
        DESTINATION ${CMAKE_INSTALL_DATAROOTDIR}/${PROJECT_NAME}/cmake)
install(DIRECTORY ${PROJECT_SOURCE_DIR}/include DESTINATION include)

if(ENABLE_TESTS)
  # The test targets
  add_executable(testStatsdClient ${CMAKE_CURRENT_SOURCE_DIR}/tests/testStatsdClient.cpp)
  if(WIN32)
    target_compile_options(testStatsdClient PRIVATE -W4 -WX /external:W0)
  else()
    target_compile_options(testStatsdClient PRIVATE -Wall -Wextra -pedantic -Werror)
  endif()
  target_include_directories(testStatsdClient PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
  target_link_libraries(testStatsdClient ${PROJECT_NAME})

  set_property(TARGET testStatsdClient PROPERTY CXX_STANDARD 11)
  set_property(TARGET testStatsdClient PROPERTY CXX_EXTENSIONS OFF)

  # The test suite
  enable_testing()
  add_test(ctestTestStatsdClient testStatsdClient)
  add_custom_target(check COMMAND ${CMAKE_CTEST_COMMAND} DEPENDS testStatsdClient)
endif()
