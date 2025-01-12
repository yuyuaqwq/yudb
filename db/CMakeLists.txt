#The MIT License(MIT)
#Copyright © 2024 https://github.com/yuyuaqwq
#
#Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files(the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and /or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
#
#The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
#
#THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.


# db/CMakeLists.txt

set(SOURCES
    btree.cpp
    btree_iterator.cpp
    bucket_impl.cpp
    db_impl.cpp
    logger.cpp
    meta.cpp
    node.cpp
    page.cpp
    pager.cpp
    tx_impl.cpp
    tx_manager.cpp
)

add_library(yudb ${SOURCES})

target_include_directories(yudb PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(yudb PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
target_include_directories(yudb PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../third_party)

target_include_directories(yudb PRIVATE ${WAL_HEADER_DIR})
target_link_libraries(yudb PRIVATE ${WAL_LIB})