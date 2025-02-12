#The MIT License(MIT)
#Copyright © 2024 https://github.com/yuyuaqwq
#
#Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files(the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and /or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
#
#The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
#
#THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.


# db/CMakeLists.txt

if(WIN32)
    file(GLOB_RECURSE PRIVATE_HEADERS 
        ./*.h
        ./*.hpp
        ./**/*.h
        ./**/*.hpp)
    source_group("Private Header Files" FILES ${PRIVATE_HEADERS})
    
    file(GLOB_RECURSE PUBLIC_HEADERS 
        ../include/yudb/*.h
        ../include/yudb/*.hpp
        ../include/yudb/**/*.h
        ../include/yudb/**/*.hpp)
    source_group("Public Header Files" FILES ${PUBLIC_HEADERS})
endif()

file(GLOB SRC
    ./*.cpp
    ./**/*.cpp)

add_library(yudb ${SRC} ${PRIVATE_HEADERS} ${PUBLIC_HEADERS})

target_include_directories(yudb PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(yudb PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
target_include_directories(yudb PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../third_party)

target_include_directories(yudb PRIVATE ${WAL_HEADER_DIR})
target_link_libraries(yudb PRIVATE ${WAL_LIB})