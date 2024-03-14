# tests/CMakeLists.txt

set(SOURCES
    btree_test.cpp
    db_test.cpp
    log_test.cpp
    logger_test.cpp
    node_test.cpp
    pager_test.cpp
    tx_manager_test.cpp
    test.cpp
)

add_executable(tests ${SOURCES})

target_include_directories(tests PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/../include)
target_include_directories(tests PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/../third_party)

target_link_libraries(tests PRIVATE yudb)