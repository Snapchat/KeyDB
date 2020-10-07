# CMake generated Testfile for 
# Source directory: /home/hbina/git/KeyDB/deps/mimalloc
# Build directory: /home/hbina/git/KeyDB/deps/mimalloc/bin
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test(test_api, "mimalloc-test-api")
set_tests_properties(test_api, PROPERTIES  _BACKTRACE_TRIPLES "/home/hbina/git/KeyDB/deps/mimalloc/CMakeLists.txt;347;add_test;/home/hbina/git/KeyDB/deps/mimalloc/CMakeLists.txt;0;")
add_test(test_stress, "mimalloc-test-stress")
set_tests_properties(test_stress, PROPERTIES  _BACKTRACE_TRIPLES "/home/hbina/git/KeyDB/deps/mimalloc/CMakeLists.txt;348;add_test;/home/hbina/git/KeyDB/deps/mimalloc/CMakeLists.txt;0;")
