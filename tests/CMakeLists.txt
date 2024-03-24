#The MIT License(MIT)
#Copyright ? 2024 https://github.com/yuyuaqwq
#
#Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files(the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and /or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
#
#The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
#
#THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

# tests/CMakeLists.txt

find_package(GTest QUIET)
if(LMDB_FOUND OR WIN32)
    message(STATUS "Building tests.")
    set(SOURCES
		btree_test.cpp
		db_test.cpp
		log_test.cpp
		logger_test.cpp
		node_test.cpp
		pager_test.cpp
		tx_manager_test.cpp
	)
    add_executable(tests ${SOURCES})

    target_include_directories(tests PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/../include)
    target_include_directories(tests PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/../third_party)

    target_link_libraries(tests PRIVATE yudb)
    if(GTEST_FOUND)
        target_link_libraries(tests GTest::GTest)
    endif()
else()
    message(WARNING "GTest not found. Skipping tests.")
endif()