#The MIT License(MIT)
#Copyright ? 2024 https://github.com/yuyuaqwq
#
#Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files(the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and /or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
#
#The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
#
#THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

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

add_executable(atomkv_benchmark atomkv_benchmark.cpp)
target_include_directories(atomkv_benchmark PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/..)
target_include_directories(atomkv_benchmark PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/../include)
target_link_libraries(atomkv_benchmark PRIVATE atomkv)