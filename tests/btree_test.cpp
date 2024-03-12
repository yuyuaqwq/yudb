#include "gtest/gtest.h"

#include "yudb/db_impl.h"
#include "yudb/node.h"

namespace yudb {

class BTreeTest : public testing::Test {
public:
    std::unique_ptr<yudb::DB> db_;
    int seed_{ 0 };
    int count_{ 1000000 };
    Pager* pager_{ nullptr };
    TxManager* tx_manager_{ nullptr };
    std::optional<UpdateTx> update_tx_;
    BucketImpl* bucket_;
    BTree* btree_;

public:
    BTreeTest() {
        Open();
    }

    ~BTreeTest() {
        update_tx_.reset();
        db_.reset();
    }

    void Open() {
        yudb::Options options{
            .max_wal_size = 1024 * 1024 * 64,
        };
        //std::string path = testing::TempDir() + "btree_test.ydb";
        const std::string path = "Z:/btree_test.ydb";
        std::filesystem::remove(path);
        std::filesystem::remove(path + "-shm");
        std::filesystem::remove(path + "-wal");
        db_ = yudb::DB::Open(options, path);
        ASSERT_FALSE(!db_);

        auto db_impl = static_cast<DBImpl*>(db_.get());
        pager_ = &db_impl->pager();
        tx_manager_ = &db_impl->tx_manager();
        auto& tx_impl = tx_manager_->Update();
        update_tx_.emplace(&tx_impl);
        bucket_ = &tx_impl.user_bucket();
        btree_ = &bucket_->btree();
    }

    std::span<const uint8_t> FromString(std::string_view str) {
        return { reinterpret_cast<const uint8_t*>(str.data()), str.size() };
    }

    const std::string_view ToString(std::span<const uint8_t> span) {
        return { reinterpret_cast<const char*>(span.data()), span.size() };
    }
};

TEST_F(BTreeTest, LeafPut) {
    const std::string key1 = "k1";
    const std::string value1 = "v1";
    btree_->Put(FromString(key1), FromString(value1), false);

    const std::string key2 = "k2";
    const std::string value2 = "v2";
    btree_->Put(FromString(key2), FromString(value2), false);

    auto iter = btree_->Get(FromString(key1));
    ASSERT_EQ(iter->key(), key1);
    ASSERT_EQ(iter->value(), value1);

    iter = btree_->Get(FromString(key2));
    ASSERT_EQ(iter->key(), key2);
    ASSERT_EQ(iter->value(), value2);
}



} // namespace yudb