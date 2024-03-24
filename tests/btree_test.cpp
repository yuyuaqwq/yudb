//The MIT License(MIT)
//Copyright ? 2024 https://github.com/yuyuaqwq
//
//Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files(the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and /or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
//
//The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
//THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

#include <gtest/gtest.h>

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
        Open({ ByteArrayComparator });
    }

    ~BTreeTest() {
        update_tx_.reset();
        db_.reset();
    }

    void Open(Comparator comparator) {
        yudb::Options options{
            .max_wal_size = 1024 * 1024 * 64,
            .comparator = comparator,
        };
        if (update_tx_.has_value()) {
            update_tx_->RollBack();
        }
        db_.reset();
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
        update_tx_.emplace(tx_manager_->Update());
        auto& tx = tx_manager_->update_tx();
        bucket_ = &tx.user_bucket();
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

TEST_F(BTreeTest, LeafPut2) {
    const std::string key1 = "k1";
    const std::string value1 = "v1";
    btree_->Put(FromString(key1), FromString(value1), false);

    const std::string key2 = "k2";
    const std::string value2 = "v2";
    btree_->Put(FromString(key2), FromString(value2), false);


    const std::string new_value1 = "new_v1";
    btree_->Put(FromString(key1), FromString(new_value1), false);

    const std::string new_value2 = "new_v2";
    btree_->Put(FromString(key2), FromString(new_value2), false);

    auto iter = btree_->Get(FromString(key1));
    ASSERT_EQ(iter->key(), key1);
    ASSERT_EQ(iter->value(), new_value1);

    iter = btree_->Get(FromString(key2));
    ASSERT_EQ(iter->key(), key2);
    ASSERT_EQ(iter->value(), new_value2);
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

TEST_F(BTreeTest, LeafUpdateParentFailed) {
    const std::string key1(1300, '1');
    const std::string value = "";
    btree_->Put(FromString(key1), FromString(value), false);
    const std::string key2(1300, '2');
    btree_->Put(FromString(key2), FromString(value), false);
    const std::string key3(1300, '3');
    btree_->Put(FromString(key3), FromString(value), false);
    const std::string key4(1300, '4');
    btree_->Put(FromString(key4), FromString(value), false);
    const std::string key5(1300, '5');
    btree_->Put(FromString(key5), FromString(value), false);
    const std::string key6(2000, '6');
    btree_->Put(FromString(key6), FromString(value), false);

    btree_->Delete(FromString(key4));

    auto iter = btree_->Get(FromString(key1));
    ASSERT_EQ(iter->key(), key1);
    ASSERT_EQ(iter->value(), value);
    iter = btree_->Get(FromString(key2));
    ASSERT_EQ(iter->key(), key2);
    ASSERT_EQ(iter->value(), value);
    iter = btree_->Get(FromString(key3));
    ASSERT_EQ(iter->key(), key3);
    ASSERT_EQ(iter->value(), value);
    iter = btree_->Get(FromString(key4));
    ASSERT_EQ(iter, btree_->end());
    iter = btree_->Get(FromString(key5));
    ASSERT_EQ(iter->key(), key5);
    ASSERT_EQ(iter->value(), value);
    iter = btree_->Get(FromString(key6));
    ASSERT_EQ(iter->key(), key6);
    ASSERT_EQ(iter->value(), value);
}

TEST_F(BTreeTest, LeafUpdateParentFailed2) {
    const std::string key1(1300, '6');
    const std::string value = "";
    btree_->Put(FromString(key1), FromString(value), false);
    const std::string key2(1300, '5');
    btree_->Put(FromString(key2), FromString(value), false);
    const std::string key3(1300, '4');
    btree_->Put(FromString(key3), FromString(value), false);
    const std::string key4(1300, '3');
    btree_->Put(FromString(key4), FromString(value), false);
    const std::string key5(2000, '2');
    btree_->Put(FromString(key5), FromString(value), false);
    const std::string key6(1300, '1');
    btree_->Put(FromString(key6), FromString(value), false);
    const std::string key7(2000, '4');
    btree_->Put(FromString(key7), FromString(value), false);

    btree_->Delete(FromString(key1));

    auto iter = btree_->Get(FromString(key1));
    ASSERT_EQ(iter, btree_->end());
    iter = btree_->Get(FromString(key2));
    ASSERT_EQ(iter->key(), key2);
    ASSERT_EQ(iter->value(), value);
    iter = btree_->Get(FromString(key3));
    ASSERT_EQ(iter->key(), key3);
    ASSERT_EQ(iter->value(), value);
    iter = btree_->Get(FromString(key4));
    ASSERT_EQ(iter->key(), key4);
    ASSERT_EQ(iter->value(), value);
    iter = btree_->Get(FromString(key5));
    ASSERT_EQ(iter->key(), key5);
    ASSERT_EQ(iter->value(), value);
    iter = btree_->Get(FromString(key6));
    ASSERT_EQ(iter->key(), key6);
    ASSERT_EQ(iter->value(), value);
}

TEST_F(BTreeTest, LeafMerge) {
    Open(UInt32Comparator);
    for (uint32_t i = 0; i < 1000; ++i) {
        std::span span = { reinterpret_cast<uint8_t*>(&i) ,sizeof(i) };
        btree_->Put(span, span, false);
    }
    for (uint32_t i = 0; i < 1000; ++i) {
        std::span span = { reinterpret_cast<uint8_t*>(&i) ,sizeof(i) };
        ASSERT_TRUE(btree_->Delete(span));
    }
    ASSERT_EQ(btree_->begin(), btree_->end());
}

TEST_F(BTreeTest, LeafMerge2) {
    Open(UInt32Comparator);
    for (int32_t i = 1000; i >= 0; --i) {
        std::span span = { reinterpret_cast<uint8_t*>(&i) ,sizeof(i) };
        btree_->Put(span, span, false);
    }
    for (int32_t i = 1000; i >= 0; --i) {
        std::span span = { reinterpret_cast<uint8_t*>(&i) ,sizeof(i) };
        ASSERT_TRUE(btree_->Delete(span));
    }
    ASSERT_EQ(btree_->begin(), btree_->end());
}

TEST_F(BTreeTest, BranchSplit) {
    const std::string key1(2031, '1');
    const std::string value = "";
    btree_->Put(FromString(key1), FromString(value), true);
    const std::string key2(2031, '2');
    btree_->Put(FromString(key2), FromString(value), true);
    const std::string key3(2031, '3');
    btree_->Put(FromString(key3), FromString(value), true);
    const std::string key4(2031, '4');
    btree_->Put(FromString(key4), FromString(value), true);
    const std::string key5(2031, '5');
    btree_->Put(FromString(key5), FromString(value), true);
    const std::string key6(2031, '6');
    btree_->Put(FromString(key6), FromString(value), true);
    const std::string key7(2031, '7');
    btree_->Put(FromString(key7), FromString(value), true);

    auto iter = btree_->Get(FromString(key1));
    ASSERT_EQ(iter->key(), key1);
    ASSERT_EQ(iter->value(), value);
    iter = btree_->Get(FromString(key2));
    ASSERT_EQ(iter->key(), key2);
    ASSERT_EQ(iter->value(), value);
    iter = btree_->Get(FromString(key3));
    ASSERT_EQ(iter->key(), key3);
    ASSERT_EQ(iter->value(), value);
    iter = btree_->Get(FromString(key4));
    ASSERT_EQ(iter->key(), key4);
    ASSERT_EQ(iter->value(), value);
    iter = btree_->Get(FromString(key5));
    ASSERT_EQ(iter->key(), key5);
    ASSERT_EQ(iter->value(), value);
    iter = btree_->Get(FromString(key6));
    ASSERT_EQ(iter->key(), key6);
    ASSERT_EQ(iter->value(), value);
    iter = btree_->Get(FromString(key7));
    ASSERT_EQ(iter->key(), key7);
    ASSERT_EQ(iter->value(), value);
}

TEST_F(BTreeTest, BranchSplit2) {
    const std::string key1(2031, '7');
    const std::string value = "";
    btree_->Put(FromString(key1), FromString(value), true);
    const std::string key2(2031, '6');
    btree_->Put(FromString(key2), FromString(value), true);
    const std::string key3(2031, '5');
    btree_->Put(FromString(key3), FromString(value), true);
    const std::string key4(2031, '4');
    btree_->Put(FromString(key4), FromString(value), true);
    const std::string key5(2031, '3');
    btree_->Put(FromString(key5), FromString(value), true);
    const std::string key6(2031, '2');
    btree_->Put(FromString(key6), FromString(value), true);
    const std::string key7(2031, '1');
    btree_->Put(FromString(key7), FromString(value), true);

    auto iter = btree_->Get(FromString(key1));
    ASSERT_EQ(iter->key(), key1);
    ASSERT_EQ(iter->value(), value);
    iter = btree_->Get(FromString(key2));
    ASSERT_EQ(iter->key(), key2);
    ASSERT_EQ(iter->value(), value);
    iter = btree_->Get(FromString(key3));
    ASSERT_EQ(iter->key(), key3);
    ASSERT_EQ(iter->value(), value);
    iter = btree_->Get(FromString(key4));
    ASSERT_EQ(iter->key(), key4);
    ASSERT_EQ(iter->value(), value);
    iter = btree_->Get(FromString(key5));
    ASSERT_EQ(iter->key(), key5);
    ASSERT_EQ(iter->value(), value);
    iter = btree_->Get(FromString(key6));
    ASSERT_EQ(iter->key(), key6);
    ASSERT_EQ(iter->value(), value);
    iter = btree_->Get(FromString(key7));
    ASSERT_EQ(iter->key(), key7);
    ASSERT_EQ(iter->value(), value);
}

TEST_F(BTreeTest, BranchMerge) {
    Open(UInt32Comparator);
    for (uint32_t i = 0; i < 100000; ++i) {
        std::span span = { reinterpret_cast<uint8_t*>(&i) ,sizeof(i) };
        btree_->Put(span, span, false);
    }
    for (uint32_t i = 0; i < 100000; ++i) {
        std::span span = { reinterpret_cast<uint8_t*>(&i) ,sizeof(i) };
        ASSERT_TRUE(btree_->Delete(span));
    }
    ASSERT_EQ(btree_->begin(), btree_->end());
}

TEST_F(BTreeTest, BranchMerge2) {
    Open(UInt32Comparator);
    for (int32_t i = 100000; i >= 0; --i) {
        std::span span = { reinterpret_cast<uint8_t*>(&i) ,sizeof(i) };
        btree_->Put(span, span, false);
    }
    for (int32_t i = 100000; i >= 0; --i) {
        std::span span = { reinterpret_cast<uint8_t*>(&i) ,sizeof(i) };
        ASSERT_TRUE(btree_->Delete(span));
    }
    ASSERT_EQ(btree_->begin(), btree_->end());
}

TEST_F(BTreeTest, EmptyIterator) {
    ASSERT_EQ(btree_->begin(), btree_->end());
}

TEST_F(BTreeTest, Iteration) {
    Open(UInt32Comparator);
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