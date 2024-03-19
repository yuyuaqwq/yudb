#include <thread>

#include <gtest/gtest.h>

#include "yudb/db_impl.h"
#include "yudb/logger.h"

namespace yudb {

class TxManagerTest : public testing::Test {
public:
    std::unique_ptr<yudb::DB> db_;
    Pager* pager_{ nullptr };
    TxManager* tx_manager_{ nullptr };

public:
    TxManagerTest() {
        Open();
    }

    void Open() {
        yudb::Options options{
            .max_wal_size = 1024 * 1024 * 64,
        };
        db_.reset();
        //std::string path = testing::TempDir() + "pager_test.ydb";
        const std::string path = "Z:/tx_manager_test.ydb";
        std::filesystem::remove(path);
        std::filesystem::remove(path + "-shm");
        std::filesystem::remove(path + "-wal");
        db_ = yudb::DB::Open(options, path);
        ASSERT_FALSE(!db_);

        auto db_impl = static_cast<DBImpl*>(db_.get());
        pager_ = &db_impl->pager();
        tx_manager_ = &db_impl->tx_manager();
    }

};

TEST_F(TxManagerTest, Version) {
    auto update_tx = tx_manager_->Update();
    auto view_tx1 = tx_manager_->View();
    auto view_tx2 = tx_manager_->View();

    auto update_bucket = update_tx.UserBucket();
    auto view_bucket1 = view_tx1.UserBucket();
    auto view_bucket2 = view_tx2.UserBucket();

    update_bucket.Put("123", "123");
    auto iter = view_bucket1.Get("123");
    ASSERT_EQ(iter, view_bucket1.end());
    iter = view_bucket2.Get("123");
    ASSERT_EQ(iter, view_bucket2.end());

    update_tx.Commit();

    iter = view_bucket1.Get("123");
    ASSERT_EQ(iter, view_bucket1.end());
    iter = view_bucket2.Get("123");
    ASSERT_EQ(iter, view_bucket2.end());

    auto view_tx3 = tx_manager_->View();
    auto view_bucket3 = view_tx3.UserBucket();
    iter = view_bucket3.Get("123");
    ASSERT_NE(iter, view_bucket3.end());
}

TEST_F(TxManagerTest, TxExpired) {
    auto update_tx1 = tx_manager_->Update();
    auto view_tx1 = tx_manager_->View();

    auto& view_tx1_impl = tx_manager_->view_tx(&view_tx1);
    auto view_tx1_txid = view_tx1_impl.txid();
    ASSERT_FALSE(tx_manager_->IsTxExpired(view_tx1_txid));

    update_tx1.Commit();

    auto update_tx2 = tx_manager_->Update();
    ASSERT_FALSE(tx_manager_->IsTxExpired(view_tx1_txid));

    view_tx1_impl.RollBack();
    update_tx2.Commit();
    auto update_tx3 = tx_manager_->Update();

    ASSERT_TRUE(tx_manager_->IsTxExpired(view_tx1_txid));
}



} // namespace yudb