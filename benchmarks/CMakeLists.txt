# benchmarks/CMakeLists.txt

find_package(LMDB QUIET)
if(LMDB_FOUND OR WIN32)
    message(STATUS "LMDB found. Version: ${LMDB_VERSION}")
    
    add_executable(lmdb_benchmark lmdb_benchmark.cpp)
    target_include_directories(lmdb_benchmark PUBLIC ${LMDB_INCLUDE_DIRS})
    target_include_directories(lmdb_benchmark PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/..)
    
    if(LMDB_FOUND)
        target_link_libraries(lmdb_benchmark ${LMDB_LIBRARIES})
    endif()
else()
    message(FATAL_ERROR "LMDB not found. Please install LMDB or check your CMake configuration.")
endif()

add_executable(yudb_benchmark yudb_benchmark.cpp)
target_include_directories(yudb_benchmark PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/..)
target_include_directories(yudb_benchmark PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/../include)
target_link_libraries(yudb_benchmark PRIVATE yudb)