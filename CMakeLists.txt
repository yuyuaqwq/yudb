cmake_minimum_required(VERSION 3.10)
project(yudb)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)

add_subdirectory(db)
add_subdirectory(tests)
# add_subdirectory(benchmarks)