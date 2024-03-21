#include <gtest/gtest.h>

#include "yudb/db_impl.h"
#include "yudb/logger.h"

namespace yudb {

class LoggerTest : public testing::Test {
public:
    std::unique_ptr<yudb::DB> db_;
    Pager* pager_{ nullptr };
    Logger* logger_{ nullptr };

public:
    LoggerTest() {
        Open();
    }

    void Open() {
        yudb::Options options{
            .max_wal_size = 1024 * 1024 * 64,
        };
        db_.reset();
        //std::string path = testing::TempDir() + "pager_test.ydb";
        const std::string path = "Z:/logger_test.ydb";
        std::filesystem::remove(path);
        std::filesystem::remove(path + "-shm");
        std::filesystem::remove(path + "-wal");
        db_ = yudb::DB::Open(options, path);
        ASSERT_FALSE(!db_);

        auto db_impl = static_cast<DBImpl*>(db_.get());
        pager_ = &db_impl->pager();
        logger_ = &db_impl->logger();
    }

};

TEST_F(LoggerTest, CheckPoint) {
    logger_->Checkpoint();
}

TEST_F(LoggerTest, Recover) {
    logger_->Recover();
}


} // namespace yudb