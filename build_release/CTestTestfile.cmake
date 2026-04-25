# CMake generated Testfile for 
# Source directory: /home/cloudshell-user/proj2-543
# Build directory: /home/cloudshell-user/proj2-543/build_release
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test([=[leveldb_tests]=] "/home/cloudshell-user/proj2-543/build_release/leveldb_tests")
set_tests_properties([=[leveldb_tests]=] PROPERTIES  _BACKTRACE_TRIPLES "/home/cloudshell-user/proj2-543/CMakeLists.txt;383;add_test;/home/cloudshell-user/proj2-543/CMakeLists.txt;0;")
add_test([=[c_test]=] "/home/cloudshell-user/proj2-543/build_release/c_test")
set_tests_properties([=[c_test]=] PROPERTIES  _BACKTRACE_TRIPLES "/home/cloudshell-user/proj2-543/CMakeLists.txt;409;add_test;/home/cloudshell-user/proj2-543/CMakeLists.txt;412;leveldb_test;/home/cloudshell-user/proj2-543/CMakeLists.txt;0;")
add_test([=[env_posix_test]=] "/home/cloudshell-user/proj2-543/build_release/env_posix_test")
set_tests_properties([=[env_posix_test]=] PROPERTIES  _BACKTRACE_TRIPLES "/home/cloudshell-user/proj2-543/CMakeLists.txt;409;add_test;/home/cloudshell-user/proj2-543/CMakeLists.txt;421;leveldb_test;/home/cloudshell-user/proj2-543/CMakeLists.txt;0;")
subdirs("third_party/googletest")
subdirs("third_party/benchmark")
