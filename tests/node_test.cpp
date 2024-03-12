#include "gtest/gtest.h"

#include "yudb/db_impl.h"
#include "yudb/node.h"

namespace yudb {

class NodeTest : public testing::Test {
public:
    std::unique_ptr<yudb::DB> db_;
    int seed_{ 0 };
    int count_{ 1000000 };
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
        yudb::Options options{
            .max_wal_size = 1024 * 1024 * 64,
        };
        //std::string path = testing::TempDir() + "node_test.ydb";
        const std::string path = "Z:/node_test.ydb";
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
    }

    std::span<const uint8_t> FromString(std::string_view str) {
        return { reinterpret_cast<const uint8_t*>(str.data()), str.size() };
    }

    const std::string_view ToString(std::span<const uint8_t> span) {
        return { reinterpret_cast<const char*>(span.data()), span.size() };
    }
};

TEST_F(NodeTest, LeafNodeEmpty) {
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

TEST_F(NodeTest, LeafNodeAppend) {
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

TEST_F(NodeTest, LeafNodeAppendLongData) {
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

TEST_F(NodeTest, LeafNodeMinimumRecords) {
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

TEST_F(NodeTest, LeafNodeInsert) {
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

TEST_F(NodeTest, LeafNodeLowerBound) {
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

TEST_F(NodeTest, LeafNodeUpdate) {
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

TEST_F(NodeTest, LeafNodeUpdateFailure) {
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

TEST_F(NodeTest, LeafNodeDelete) {
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

TEST_F(NodeTest, LeafNodeDeleteLongData) {
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

TEST_F(NodeTest, LeafNodeCompactify) {
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

TEST_F(NodeTest, BranchNodeAppend) {
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

TEST_F(NodeTest, BranchNodeInsert) {
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

TEST_F(NodeTest, BranchNodeDelete) {
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

} // namespace yudb