# db/CMakeLists.txt

set(SOURCES
    btree.cpp
    btree_iterator.cpp
    bucket_impl.cpp
    db_impl.cpp
    log_reader.cpp
    log_writer.cpp
    logger.cpp
    meta.cpp
    node.cpp
    page.cpp
    pager.cpp
    tx_impl.cpp
    tx_manager.cpp
    ../util/crc32.cpp
)

add_library(db ${SOURCES})

# 设置包含目录
target_include_directories(db PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/../include)
target_include_directories(db PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/../third_party)