cmake_minimum_required(VERSION 3.10) 

project(yudb VERSION 0.0.1) 

set(SRC_LIST src/free_manager/free_manager.c src/bucket.c src/cacher.c src/data_pool.c src/db_file.c src/error.c src/import.c src/meta_info.c src/page.c src/pager.c src/transaction.c src/wal.c src/yudb.c src/test.cpp)

include_directories(include)

add_executable(${PROJECT_NAME} ${SRC_LIST}) 