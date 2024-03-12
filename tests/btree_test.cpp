#include "gtest/gtest.h"

#include "yudb/db_impl.h"
#include "yudb/node.h"

namespace yudb {

class BTreeTest : public testing::Test {
public:
    std::unique_ptr<yudb::DB> db_;
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

TEST_F(BTreeTest, LeafSplit) {
    for (int i = 0; i < 1000; ++i) {
        std::span span = { reinterpret_cast<uint8_t*>(&i) ,sizeof(i)};
        btree_->Put(span, span, true);
    }
    for (int i = 0; i < 1000; ++i) {
        std::span span = { reinterpret_cast<uint8_t*>(&i) ,sizeof(i) };
        auto iter = btree_->Get(span);
        ASSERT_EQ(iter.is_bucket(), true);
        ASSERT_EQ(iter.key<int>(), i);
        ASSERT_EQ(iter.value<int>(), i);
    }
}

TEST_F(BTreeTest, LeafSplit2) {
    const std::string key1(1000, '1');
    const std::string value1(1031, 'a');
    btree_->Put(FromString(key1), FromString(value1), true);

    const std::string key2(1000, '2');
    const std::string value2(1031, 'b');
    btree_->Put(FromString(key2), FromString(value2), true);

    const std::string key3(1000, '3');
    const std::string value3(1031, 'c');
    btree_->Put(FromString(key3), FromString(value3), true);

    auto iter = btree_->Get(FromString(key1));
    ASSERT_EQ(iter.is_bucket(), true);
    ASSERT_EQ(iter.key(), key1);
    ASSERT_EQ(iter.value(), value1);

    iter = btree_->Get(FromString(key2));
    ASSERT_EQ(iter.is_bucket(), true);
    ASSERT_EQ(iter.key(), key2);
    ASSERT_EQ(iter.value(), value2);

    iter = btree_->Get(FromString(key3));
    ASSERT_EQ(iter.is_bucket(), true);
    ASSERT_EQ(iter.key(), key3);
    ASSERT_EQ(iter.value(), value3);
}

TEST_F(BTreeTest, LeafSplit3) {
    const std::string key1(1000, '1');
    const std::string value1(1031, 'a');
    btree_->Put(FromString(key1), FromString(value1), true);

    const std::string key2(1000, '2');
    const std::string value2(1031, 'b');
    btree_->Put(FromString(key2), FromString(value2), true);

    const std::string key3(1000, '0');
    const std::string value3(1031, 'c');
    btree_->Put(FromString(key3), FromString(value3), true);

    auto iter = btree_->Get(FromString(key1));
    ASSERT_EQ(iter.is_bucket(), true);
    ASSERT_EQ(iter.key(), key1);
    ASSERT_EQ(iter.value(), value1);

    iter = btree_->Get(FromString(key2));
    ASSERT_EQ(iter.is_bucket(), true);
    ASSERT_EQ(iter.key(), key2);
    ASSERT_EQ(iter.value(), value2);

    iter = btree_->Get(FromString(key3));
    ASSERT_EQ(iter.is_bucket(), true);
    ASSERT_EQ(iter.key(), key3);
    ASSERT_EQ(iter.value(), value3);
}

TEST_F(BTreeTest, LeafSplit4) {
    const std::string key1(27, '1');
    const std::string value1(27, 'a');
    btree_->Put(FromString(key1), FromString(value1), true);

    const std::string key2(1000, '2');
    const std::string value2(1000, 'b');
    btree_->Put(FromString(key2), FromString(value2), true);

    const std::string key3(1000, '3');
    const std::string value3(1000, 'c');
    btree_->Put(FromString(key3), FromString(value3), true);

    const std::string key4(1000, '4');
    const std::string value4(1000, 'd');
    btree_->Put(FromString(key4), FromString(value4), true);

    auto iter = btree_->Get(FromString(key1));
    ASSERT_EQ(iter.is_bucket(), true);
    ASSERT_EQ(iter.key(), key1);
    ASSERT_EQ(iter.value(), value1);

    iter = btree_->Get(FromString(key2));
    ASSERT_EQ(iter.is_bucket(), true);
    ASSERT_EQ(iter.key(), key2);
    ASSERT_EQ(iter.value(), value2);

    iter = btree_->Get(FromString(key3));
    ASSERT_EQ(iter.is_bucket(), true);
    ASSERT_EQ(iter.key(), key3);
    ASSERT_EQ(iter.value(), value3);

    iter = btree_->Get(FromString(key4));
    ASSERT_EQ(iter.is_bucket(), true);
    ASSERT_EQ(iter.key(), key4);
    ASSERT_EQ(iter.value(), value4);

}

TEST_F(BTreeTest, LeafSplit5) {
    const std::string key1(1000, '1');
    const std::string value1(1000, 'a');
    btree_->Put(FromString(key1), FromString(value1), true);

    const std::string key2(1000, '2');
    const std::string value2(1000, 'b');
    btree_->Put(FromString(key2), FromString(value2), true);

    const std::string key3(27, '3');
    const std::string value3(27, 'c');
    btree_->Put(FromString(key3), FromString(value3), true);

    const std::string key4(1000, '0');
    const std::string value4(1000, 'd');
    btree_->Put(FromString(key4), FromString(value4), true);

    auto iter = btree_->Get(FromString(key1));
    ASSERT_EQ(iter.is_bucket(), true);
    ASSERT_EQ(iter.key(), key1);
    ASSERT_EQ(iter.value(), value1);

    iter = btree_->Get(FromString(key2));
    ASSERT_EQ(iter.is_bucket(), true);
    ASSERT_EQ(iter.key(), key2);
    ASSERT_EQ(iter.value(), value2);

    iter = btree_->Get(FromString(key3));
    ASSERT_EQ(iter.is_bucket(), true);
    ASSERT_EQ(iter.key(), key3);
    ASSERT_EQ(iter.value(), value3);
}

TEST_F(BTreeTest, LeafDelete) {
    const std::string key1 = "k1";
    const std::string value1 = "v1";
    btree_->Put(FromString(key1), FromString(value1), false);

    const std::string key2 = "k2";
    const std::string value2 = "v2";
    btree_->Put(FromString(key2), FromString(value2), false);

    ASSERT_TRUE(btree_->Delete(FromString(key1)));
    ASSERT_TRUE(btree_->Delete(FromString(key2)));

    auto iter = btree_->Get(FromString(key1));
    ASSERT_EQ(iter, btree_->end());

    iter = btree_->Get(FromString(key2));
    ASSERT_EQ(iter, btree_->end());
}

TEST_F(BTreeTest, LeafMerge) {
    for (int i = 0; i < 1000; ++i) {
        std::span span = { reinterpret_cast<uint8_t*>(&i) ,sizeof(i) };
        btree_->Put(span, span, false);
    }
    for (int i = 0; i < 1000; ++i) {
        std::span span = { reinterpret_cast<uint8_t*>(&i) ,sizeof(i) };
        ASSERT_TRUE(btree_->Delete(span));
    }
    ASSERT_EQ(btree_->begin(), btree_->end());
}

TEST_F(BTreeTest, BranchPut) {

}

TEST_F(BTreeTest, EmptyIterator) {
    ASSERT_EQ(btree_->begin(), btree_->end());
}

TEST_F(BTreeTest, Iteration) {
    for (int i = 0; i < 10000; ++i) {
        std::span span = { reinterpret_cast<uint8_t*>(&i) ,sizeof(i) };
        btree_->Put(span, span, true);
    }
    int i = 0;
    for (auto& iter : *btree_) {
        ASSERT_EQ(iter.is_bucket(), true);
        ASSERT_EQ(iter.key<int>(), i);
        ASSERT_EQ(iter.value<int>(), i);
        ++i;
    }
    ASSERT_EQ(i, 10000);
}

} // namespace yudb