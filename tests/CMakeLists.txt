# tests/CMakeLists.txt

find_package(GTest QUIET)
if(GTEST_FOUND OR WIN32)
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