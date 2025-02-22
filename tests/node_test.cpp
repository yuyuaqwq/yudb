//The MIT License(MIT)
//Copyright © 2024 https://github.com/yuyuaqwq
//
//Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files(the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and /or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
//
//The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
//THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

#include <gtest/gtest.h>

#include <atomkv/node.h>

#include "src/db_impl.h"

namespace atomkv {

class NodeTest : public testing::Test {
public:
    std::unique_ptr<atomkv::DB> db_;
    Pager* pager_{ nullptr };
    TxManager* tx_manager_{ nullptr };
    std::optional<UpdateTx> update_tx_;
    BucketImpl* bucket_;

public:
    NodeTest() {
        Open();
    }

    ~NodeTest() {
        update_tx_.reset();
        db_.reset();
    }

    void Open() {
        atomkv::Options options{
            .max_wal_size = 1024 * 1024 * 64,
        };
        //std::string path = testing::TempDir() + "node_test.ydb";
        const std::string path = "Z:/node_test.ydb";
        std::filesystem::remove(path);
        std::filesystem::remove(path + "-shm");
        std::filesystem::remove(path + "-wal");
        db_ = atomkv::DB::Open(options, path);
        ASSERT_FALSE(!db_);

        auto db_impl = static_cast<DBImpl*>(db_.get());
        pager_ = &db_impl->pager();
        tx_manager_ = &db_impl->tx_manager();
        update_tx_.emplace(tx_manager_->Update());
        auto& tx = tx_manager_->update_tx();
        bucket_ = &tx.user_bucket();
    }

    std::span<const uint8_t> FromString(std::string_view str) {
        return { reinterpret_cast<const uint8_t*>(str.data()), str.size() };
    }

    const std::string_view ToString(std::span<const uint8_t> span) {
        return { reinterpret_cast<const char*>(span.data()), span.size() };
    }
};

TEST_F(NodeTest, LeafEmpty) {
    bool success;
    LeafNode node{ &bucket_->btree(),pager_->Alloc(1), true };
    node.Build();

    const std::string key1 = "";
    const std::string value1 = "";
    success = node.Append(FromString(key1), FromString(value1));

    auto get_key1 = ToString(node.GetKey(0));
    ASSERT_EQ(get_key1, key1);
    auto get_value1 = ToString(node.GetValue(0));
    ASSERT_EQ(get_value1, value1);
}

TEST_F(NodeTest, LeafAppend) {
    bool success;
    LeafNode node{ &bucket_->btree(),pager_->Alloc(1), true };
    node.Build();

    const std::string key1 = "k1";
    const std::string value1 = "v1";
    success = node.Append(FromString(key1), FromString(value1));
    ASSERT_TRUE(success);

    const std::string key2 = "k2";
    const std::string value2 = "v2";
    success = node.Append(FromString(key2), FromString(value2));
    ASSERT_TRUE(success);

    auto get_key1 = ToString(node.GetKey(0));
    ASSERT_EQ(get_key1, key1);
    auto get_value1 = ToString(node.GetValue(0));
    ASSERT_EQ(get_value1, value1);

    auto get_key2 = ToString(node.GetKey(1));
    ASSERT_EQ(get_key2, key2);
    auto get_value2 = ToString(node.GetValue(1));
    ASSERT_EQ(get_value2, value2);
}

TEST_F(NodeTest, LeafAppendLongData) {
    bool success;
    LeafNode node{ &bucket_->btree(),pager_->Alloc(1), true };
    node.Build();
    const std::string key1(1000, 'a');
    const std::string value1(10000, 'b');
    success = node.Append(FromString(key1), FromString(value1));
    ASSERT_TRUE(success);

    const std::string key2(1000, 'c');
    const std::string value2(10000, 'd');
    success = node.Append(FromString(key2), FromString(value2));
    ASSERT_TRUE(success);

    const std::string key3(1000, 'e');
    const std::string value3(10000, 'f');
    success = node.Append(FromString(key3), FromString(value3));
    ASSERT_TRUE(success);

    auto get_key1 = ToString(node.GetKey(0));
    ASSERT_EQ(get_key1, key1);
    auto get_value1 = ToString(node.GetValue(0));
    ASSERT_EQ(get_value1, value1);

    auto get_key2 = ToString(node.GetKey(1));
    ASSERT_EQ(get_key2, key2);
    auto get_value2 = ToString(node.GetValue(1));
    ASSERT_EQ(get_value2, value2);

    auto get_key3 = ToString(node.GetKey(2));
    ASSERT_EQ(get_key3, key3);
    auto get_value3 = ToString(node.GetValue(2));
    ASSERT_EQ(get_value3, value3);
}

TEST_F(NodeTest, LeafMinimumRecords) {
    bool success;
    LeafNode node{ &bucket_->btree(), pager_->Alloc(1), true };
    node.Build();

    const std::string key1(1000, 'a');
    const std::string value1(1031, 'b');
    success = node.Append(FromString(key1), FromString(value1));
    ASSERT_TRUE(success);
    const std::string key2(1000, 'c');
    const std::string value2(1031, 'd');
    success = node.Append(FromString(key2), FromString(value2));
    ASSERT_TRUE(success);

    const std::string key3 = "a";
    const std::string value3 = "b";
    success = node.Append(FromString(key3), FromString(value3));
    ASSERT_FALSE(success);

    auto get_key1 = ToString(node.GetKey(0));
    ASSERT_EQ(get_key1, key1);
    auto get_value1 = ToString(node.GetValue(0));
    ASSERT_EQ(get_value1, value1);

    auto get_key2 = ToString(node.GetKey(1));
    ASSERT_EQ(get_key2, key2);
    auto get_value2 = ToString(node.GetValue(1));
    ASSERT_EQ(get_value2, value2);

    ASSERT_EQ(node.header().space_used + node.header().data_offset, pager_->page_size());
}

TEST_F(NodeTest, LeafInsert) {
    bool success;
    LeafNode node{ &bucket_->btree(), pager_->Alloc(1), true };
    node.Build();

    const std::string key1 = "k1";
    const std::string value1 = "v1";
    success = node.Insert(0, FromString(key1), FromString(value1));
    ASSERT_TRUE(success);

    const std::string key2 = "k2";
    const std::string value2 = "v2";
    success = node.Insert(0, FromString(key2), FromString(value2));
    ASSERT_TRUE(success);

    const std::string key3 = "k3";
    const std::string value3 = "v3";
    success = node.Insert(1, FromString(key3), FromString(value3));
    ASSERT_TRUE(success);

    auto get_key1 = ToString(node.GetKey(2));
    ASSERT_EQ(get_key1, key1);
    auto get_value1 = ToString(node.GetValue(2));
    ASSERT_EQ(get_value1, value1);

    auto get_key2 = ToString(node.GetKey(0));
    ASSERT_EQ(get_key2, key2);
    auto get_value2 = ToString(node.GetValue(0));
    ASSERT_EQ(get_value2, value2);

    auto get_key3 = ToString(node.GetKey(1));
    ASSERT_EQ(get_key3, key3);
    auto get_value3 = ToString(node.GetValue(1));
    ASSERT_EQ(get_value3, value3);
}

TEST_F(NodeTest, LeafLowerBound) {
    bool success;
    LeafNode node{ &bucket_->btree(), pager_->Alloc(1), true };
    node.Build();

    const std::string key1 = "k1";
    const std::string value1 = "v1";
    success = node.Append(FromString(key1), FromString(value1));
    ASSERT_TRUE(success);


    const std::string key2 = "k3";
    const std::string value2 = "v3";
    success = node.Append(FromString(key2), FromString(value2));
    ASSERT_TRUE(success);

    const std::string key3 = "k5";
    const std::string value3 = "v5";
    success = node.Append(FromString(key3), FromString(value3));
    ASSERT_TRUE(success);

    auto pos = node.LowerBound(FromString("k0"));
    ASSERT_FALSE(pos.second);
    ASSERT_EQ(pos.first, 0);

    pos = node.LowerBound(FromString("k1"));
    ASSERT_TRUE(pos.second);
    ASSERT_EQ(pos.first, 0);

    pos = node.LowerBound(FromString("k2"));
    ASSERT_FALSE(pos.second);
    ASSERT_EQ(pos.first, 1);

    pos = node.LowerBound(FromString("k3"));
    ASSERT_TRUE(pos.second);
    ASSERT_EQ(pos.first, 1);

    pos = node.LowerBound(FromString("k4"));
    ASSERT_FALSE(pos.second);
    ASSERT_EQ(pos.first, 2);

    pos = node.LowerBound(FromString("k5"));
    ASSERT_TRUE(pos.second);
    ASSERT_EQ(pos.first, 2);

    pos = node.LowerBound(FromString("k6"));
    ASSERT_FALSE(pos.second);
    ASSERT_EQ(pos.first, 3);
}

TEST_F(NodeTest, LeafUpdate) {
    bool success;
    LeafNode node{ &bucket_->btree(), pager_->Alloc(1), true };
    node.Build();

    const std::string key1 = "k1";
    const std::string value1 = "v1";
    success = node.Append(FromString(key1), FromString(value1));
    ASSERT_TRUE(success);

    const std::string new_value1 = "new_v1";
    success = node.Update(0, FromString(key1), FromString(new_value1));
    ASSERT_TRUE(success);

    auto get_key1 = ToString(node.GetKey(0));
    ASSERT_EQ(get_key1, key1);
    auto get_value1 = ToString(node.GetValue(0));
    ASSERT_EQ(get_value1, new_value1);
}

TEST_F(NodeTest, LeafUpdateFailure) {
    bool success;
    LeafNode node{ &bucket_->btree(), pager_->Alloc(1), true };
    node.Build();

    const std::string key1(1000, 'a');
    const std::string value1(1031, 'b');
    success = node.Append(FromString(key1), FromString(value1));
    ASSERT_TRUE(success);
    const std::string key2(1000, 'c');
    const std::string value2(1031, 'd');
    success = node.Append(FromString(key2), FromString(value2));
    ASSERT_TRUE(success);

    const std::string key3 = "k";
    const std::string value3 = "v";
    success = node.Append(FromString(key2), FromString(value2));
    ASSERT_FALSE(success);

    std::string new_value1(1021, 'z');
    success = node.Update(0, FromString(key1), FromString(new_value1));
    ASSERT_TRUE(success);

    success = node.Append(FromString(key3), FromString(value3));
    ASSERT_TRUE(success);

    new_value1 = std::string(1032, 'y');
    success = node.Update(2, FromString(key1), FromString(new_value1));
    ASSERT_FALSE(success);

    success = node.Update(0, FromString(key1), FromString(new_value1));
    ASSERT_TRUE(success);

    auto get_key1 = ToString(node.GetKey(0));
    ASSERT_EQ(get_key1, key1);
    auto get_value1 = ToString(node.GetValue(0));
    ASSERT_EQ(get_value1, new_value1);

    success = node.Update(2, FromString(key1), FromString(new_value1));
    ASSERT_TRUE(success);

}

TEST_F(NodeTest, LeafDelete) {
    bool success;
    LeafNode node{ &bucket_->btree(), pager_->Alloc(1), true };
    node.Build();

    const std::string key1 = "k1";
    const std::string value1 = "v1";
    success = node.Insert(0, FromString(key1), FromString(value1));
    ASSERT_TRUE(success);

    const std::string key2 = "k2";
    const std::string value2 = "v2";
    success = node.Insert(0, FromString(key2), FromString(value2));
    ASSERT_TRUE(success);

    const std::string key3 = "k3";
    const std::string value3 = "v3";
    success = node.Insert(1, FromString(key3), FromString(value3));
    ASSERT_TRUE(success);

    node.Delete(0);

    auto get_key3 = ToString(node.GetKey(0));
    ASSERT_EQ(get_key3, key3);
    auto get_value3 = ToString(node.GetValue(0));
    ASSERT_EQ(get_value3, value3);

    auto get_key1 = ToString(node.GetKey(1));
    ASSERT_EQ(get_key1, key1);
    auto get_value1 = ToString(node.GetValue(1));
    ASSERT_EQ(get_value1, value1);
}

TEST_F(NodeTest, LeafDeleteLongData) {
    bool success;
    LeafNode node{ &bucket_->btree(), pager_->Alloc(1), true };
    node.Build();

    const std::string key1(1000, 'a');
    const std::string value1(10000, 'b');
    success = node.Append(FromString(key1), FromString(value1));
    ASSERT_TRUE(success);

    const std::string key2(1000, 'c');
    const std::string value2(10000, 'd');
    success = node.Append(FromString(key2), FromString(value2));
    ASSERT_TRUE(success);

    const std::string key3(1000, 'e');
    const std::string value3(10000, 'f');
    success = node.Append(FromString(key3), FromString(value3));
    ASSERT_TRUE(success);

    node.Delete(0);
    node.Delete(0);
    node.Delete(0);

    ASSERT_EQ(node.count(), 0);
    ASSERT_EQ(node.header().space_used, 0);
}

TEST_F(NodeTest, LeafCompactify) {
    bool success;
    LeafNode node{ &bucket_->btree(), pager_->Alloc(1), true };
    node.Build();

    const std::string key1(1000, 'a');
    const std::string value1(1000, 'b');
    success = node.Append(FromString(key1), FromString(value1));
    ASSERT_TRUE(success);
    const std::string key2(1000, 'c');
    const std::string value2(1000, 'd');
    success = node.Append(FromString(key2), FromString(value2));
    ASSERT_TRUE(success);
    const std::string key3(27, 'e');
    const std::string value3(27, 'f');
    success = node.Append(FromString(key3), FromString(value3));
    ASSERT_TRUE(success);

    node.Delete(0);
    success = node.Append(FromString(key1), FromString(value1));
    ASSERT_TRUE(success);

    auto get_key1 = ToString(node.GetKey(2));
    ASSERT_EQ(get_key1, key1);
    auto get_value1 = ToString(node.GetValue(2));
    ASSERT_EQ(get_value1, value1);

    auto get_key2 = ToString(node.GetKey(0));
    ASSERT_EQ(get_key2, key2);
    auto get_value2 = ToString(node.GetValue(0));
    ASSERT_EQ(get_value2, value2);

    auto get_key3 = ToString(node.GetKey(1));
    ASSERT_EQ(get_key3, key3);
    auto get_value3 = ToString(node.GetValue(1));
    ASSERT_EQ(get_value3, value3);
}

TEST_F(NodeTest, BranchAppend) {
    bool success;
    BranchNode node{ &bucket_->btree(),pager_->Alloc(1), true };
    auto tail_child = pager_->Alloc(1);
    node.Build(tail_child);

    const std::string key1 = "k1";
    auto k1_child = pager_->Alloc(1);
    success = node.Append(FromString(key1), k1_child, false);
    ASSERT_TRUE(success);
    ASSERT_EQ(node.GetLeftChild(0), k1_child);
    ASSERT_EQ(node.GetRightChild(0), tail_child);
    ASSERT_EQ(node.GetLeftChild(1), tail_child);
    ASSERT_EQ(node.GetTailChild(), tail_child);
    auto get_key1 = ToString(node.GetKey(0));
    ASSERT_EQ(get_key1, key1);

    const std::string key2 = "k2";
    auto k2_child = pager_->Alloc(1);
    success = node.Append(FromString(key2), k2_child, true);
    ASSERT_TRUE(success);
    ASSERT_EQ(node.GetLeftChild(0), k1_child);
    ASSERT_EQ(node.GetRightChild(0), tail_child);
    ASSERT_EQ(node.GetLeftChild(1), tail_child);
    ASSERT_EQ(node.GetRightChild(1), k2_child);
    ASSERT_EQ(node.GetLeftChild(2), k2_child);
    ASSERT_EQ(node.GetTailChild(), k2_child);
    get_key1 = ToString(node.GetKey(0));
    ASSERT_EQ(get_key1, key1);
    auto get_key2 = ToString(node.GetKey(1));
    ASSERT_EQ(get_key2, key2);
}

TEST_F(NodeTest, BranchInsert) {
    bool success;
    BranchNode node{ &bucket_->btree(),pager_->Alloc(1), true };
    auto tail_child = pager_->Alloc(1);
    node.Build(tail_child);

    const std::string key1 = "k1";
    auto k1_child = pager_->Alloc(1);
    success = node.Insert(0, FromString(key1), k1_child, true);
    ASSERT_TRUE(success);
    ASSERT_EQ(node.GetLeftChild(0), tail_child);
    ASSERT_EQ(node.GetRightChild(0), k1_child);
    ASSERT_EQ(node.GetLeftChild(1), k1_child);
    ASSERT_EQ(node.GetTailChild(), k1_child);
    auto get_key1 = ToString(node.GetKey(0));
    ASSERT_EQ(get_key1, key1);

    const std::string key2 = "k2";
    auto k2_child = pager_->Alloc(1);
    success = node.Insert(0, FromString(key2), k2_child, false);
    ASSERT_TRUE(success);
    ASSERT_EQ(node.GetLeftChild(0), k2_child);
    ASSERT_EQ(node.GetRightChild(0), tail_child);
    ASSERT_EQ(node.GetLeftChild(1), tail_child);
    ASSERT_EQ(node.GetRightChild(1), k1_child);
    ASSERT_EQ(node.GetLeftChild(2), k1_child);
    ASSERT_EQ(node.GetTailChild(), k1_child);
    get_key1 = ToString(node.GetKey(1));
    ASSERT_EQ(get_key1, key1);
    auto get_key2 = ToString(node.GetKey(0));
    ASSERT_EQ(get_key2, key2);

    const std::string key3 = "k3";
    auto k3_child = pager_->Alloc(1);
    success = node.Insert(0, FromString(key3), k3_child, true);
    ASSERT_TRUE(success);
    ASSERT_EQ(node.GetLeftChild(0), k2_child);
    ASSERT_EQ(node.GetRightChild(0), k3_child);
    ASSERT_EQ(node.GetLeftChild(1), k3_child);
    ASSERT_EQ(node.GetRightChild(1), tail_child);
    ASSERT_EQ(node.GetLeftChild(2), tail_child);
    ASSERT_EQ(node.GetRightChild(2), k1_child);
    ASSERT_EQ(node.GetLeftChild(3), k1_child);
    ASSERT_EQ(node.GetTailChild(), k1_child);
    get_key1 = ToString(node.GetKey(2));
    ASSERT_EQ(get_key1, key1);
    get_key2 = ToString(node.GetKey(1));
    ASSERT_EQ(get_key2, key2);
    auto get_key3 = ToString(node.GetKey(0));
    ASSERT_EQ(get_key3, key3);
}

TEST_F(NodeTest, BranchDelete) {
    bool success;
    BranchNode node{ &bucket_->btree(),pager_->Alloc(1), true };
    auto tail_child = pager_->Alloc(1);
    node.Build(tail_child);

    const std::string key1 = "k1";
    auto k1_child = pager_->Alloc(1);
    success = node.Append(FromString(key1), k1_child, false);
    ASSERT_TRUE(success);

    node.Delete(0, false);
    ASSERT_EQ(node.GetLeftChild(0), tail_child);
    ASSERT_EQ(node.GetTailChild(), tail_child);

    success = node.Append(FromString(key1), k1_child, false);
    ASSERT_TRUE(success);

    node.Delete(0, true);
    ASSERT_EQ(node.GetLeftChild(0), k1_child);
    ASSERT_EQ(node.GetTailChild(), k1_child);

    node.SetTailChild(tail_child);
    success = node.Append(FromString(key1), k1_child, false);
    ASSERT_TRUE(success);
    const std::string key2 = "k2";
    auto k2_child = pager_->Alloc(1);
    success = node.Append(FromString(key2), k2_child, false);
    ASSERT_TRUE(success);

    const std::string key3 = "k3";
    auto k3_child = pager_->Alloc(1);
    success = node.Append(FromString(key3), k3_child, false);
    ASSERT_TRUE(success);

    node.Delete(0, true);
    ASSERT_EQ(node.GetLeftChild(0), k1_child);
    ASSERT_EQ(node.GetRightChild(0), k3_child);
    ASSERT_EQ(node.GetLeftChild(1), k3_child);
    ASSERT_EQ(node.GetRightChild(1), tail_child);
    ASSERT_EQ(node.GetLeftChild(2), tail_child);
    ASSERT_EQ(node.GetTailChild(), tail_child);
}

} // namespace atomkv