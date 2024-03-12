#include "gtest/gtest.h"

#include "yudb/db_impl.h"
#include "yudb/node.h"

namespace yudb {

class NodeExternal : public Node {
public:
    using Node::Node;

    bool RequestSpaceFor(std::span<const uint8_t> key, std::span<const uint8_t> value) {
        return Node::RequestSpaceFor(key, value);
    }

    size_t SpaceNeeded(size_t record_size) {
        return Node::SpaceNeeded(record_size);
    }

    size_t SlotSpace() {
        return Node::SlotSpace();
    }

    size_t FreeSpace() {
        return Node::FreeSpace();
    }

    size_t FreeSpaceAfterCompaction() {
        return Node::FreeSpaceAfterCompaction();
    }

    void Compactify() {
        return Node::Compactify();
    }

    uint8_t* Ptr() {
        return Node::Ptr();
    }

    uint8_t* GetRecordPtr(SlotId slot_id) {
        return Node::GetRecordPtr(slot_id);
    }

    PageId StoreRecordToOverflowPages(SlotId slot_id, std::span<const uint8_t> key, std::span<const uint8_t> value) {
        return Node::StoreRecordToOverflowPages(slot_id, key, value);
    }

    void StoreRecord(SlotId slot_id, std::span<const uint8_t> key, std::span<const uint8_t> value) {
        return Node::StoreRecord(slot_id, key, value);
    }

    void CopyRecordRange(Node* dst) {
        return Node::CopyRecordRange(dst);
    }

    void DeleteRecord(SlotId slot_id) {
        return Node::DeleteRecord(slot_id);
    }

    void RestoreRecord(SlotId slot_id, const Slot& saved_slot) {
        return Node::RestoreRecord(slot_id, saved_slot);
    }
};

class NodeTest : public testing::Test {
public:
    std::unique_ptr<yudb::DB> db_;
    int seed_{ 0 };
    int count_{ 1000000 };

public:
    NodeTest() {
        Open();
    }

    void Open() {
        yudb::Options options{
            .max_wal_size = 1024 * 1024 * 64,
        };
        //std::string path = testing::TempDir() + "node_test.ydb";
        std::string path = "Z:/node_test.ydb";
        std::filesystem::remove(path);
        std::filesystem::remove(path + "-shm");
        std::filesystem::remove(path + "-wal");
        db_ = yudb::DB::Open(options, path);
        ASSERT_FALSE(!db_);
    }
};

TEST_F(NodeTest, StoreRecord) {
    auto db_impl = static_cast<DBImpl*>(db_.get());
    auto& pager = db_impl->pager();
    auto& tx_manager = db_impl->tx_manager();

    auto tx = db_impl->Update();
    auto& update_tx = tx_manager.update_tx();
    auto& user_bucket = update_tx.user_bucket();
    
    NodeExternal node{ &user_bucket.btree(),pager.Alloc(1), true };
    
    auto size = pager.page_size() -
        sizeof(NodeStruct::header) -
        sizeof(NodeStruct::padding);
    auto count = size / sizeof(uint32_t);
    for (uint32_t i = 0; i < count; ++i) {
        node.StoreRecord(0, { reinterpret_cast<uint8_t*>(&i) ,sizeof(i)}, { reinterpret_cast<uint8_t*>(&i) ,sizeof(i)});
        
    }

}

TEST_F(NodeTest, LeafNodeAppend) {
    auto db_impl = static_cast<DBImpl*>(db_.get());
    auto& pager = db_impl->pager();
    auto& tx_manager = db_impl->tx_manager();
    
    auto tx = db_impl->Update();
    auto& update_tx = tx_manager.update_tx();
    auto& user_bucket = update_tx.user_bucket();

    bool success;
    LeafNode leaf_node{ &user_bucket.btree(),pager.Alloc(1), true };
    leaf_node.Build();
    std::string key1(1024, 'a');
    std::string value1(1024, 'b');
    success = leaf_node.Append({ reinterpret_cast<uint8_t*>(key1.data()), key1.size() },
        { reinterpret_cast<uint8_t*>(value1.data()), value1.size() });
    ASSERT_TRUE(success);
    std::string key2(1024, 'c');
    std::string value2(1024, 'd');
    success = leaf_node.Append({ reinterpret_cast<uint8_t*>(key2.data()), key2.size() },
        { reinterpret_cast<uint8_t*>(value2.data()), value2.size() });
    ASSERT_TRUE(success);
    auto get_key1 = leaf_node.GetKey(0);
    ASSERT_EQ(std::string(reinterpret_cast<const char*>(get_key1.data()), get_key1.size()), std::string(1024, 'a'));
    auto get_value1 = leaf_node.GetValue(0);
    ASSERT_EQ(std::string(reinterpret_cast<const char*>(get_value1.data()), get_value1.size()), std::string(1024, 'b'));

    auto get_key2 = leaf_node.GetKey(1);
    ASSERT_EQ(std::string(reinterpret_cast<const char*>(get_key2.data()), get_key2.size()), std::string(1024, 'c'));
    auto get_value2 = leaf_node.GetValue(1);
    ASSERT_EQ(std::string(reinterpret_cast<const char*>(get_value2.data()), get_value2.size()), std::string(1024, 'd'));

}

TEST_F(NodeTest, LeafNodeAppendLongData) {
    auto db_impl = static_cast<DBImpl*>(db_.get());
    auto& pager = db_impl->pager();
    auto& tx_manager = db_impl->tx_manager();

    auto tx = db_impl->Update();
    auto& update_tx = tx_manager.update_tx();
    auto& user_bucket = update_tx.user_bucket();

    bool success;
    LeafNode leaf_node{ &user_bucket.btree(),pager.Alloc(1), true };
    leaf_node.Build();
    std::string key1(1000, 'a');
    std::string value1(1031, 'b');
    success = leaf_node.Append({ reinterpret_cast<uint8_t*>(key1.data()), key1.size() },
        { reinterpret_cast<uint8_t*>(value1.data()), value1.size() });
    ASSERT_TRUE(success);
    std::string key2(1000, 'c');
    std::string value2(1031, 'd');
    success = leaf_node.Append({ reinterpret_cast<uint8_t*>(key2.data()), key2.size() },
        { reinterpret_cast<uint8_t*>(value2.data()), value2.size() });
    ASSERT_TRUE(success);
    success = leaf_node.Append({ reinterpret_cast<uint8_t*>(key2.data()), key2.size() },
        { reinterpret_cast<uint8_t*>(value2.data()), value2.size() });
    ASSERT_FALSE(success);

    auto get_key1 = leaf_node.GetKey(0);
    ASSERT_EQ(std::string(reinterpret_cast<const char*>(get_key1.data()), get_key1.size()), std::string(1000, 'a'));
    auto get_value1 = leaf_node.GetValue(0);
    ASSERT_EQ(std::string(reinterpret_cast<const char*>(get_value1.data()), get_value1.size()), std::string(1031, 'b'));

    auto get_key2 = leaf_node.GetKey(1);
    ASSERT_EQ(std::string(reinterpret_cast<const char*>(get_key2.data()), get_key2.size()), std::string(1000, 'c'));
    auto get_value2 = leaf_node.GetValue(1);
    ASSERT_EQ(std::string(reinterpret_cast<const char*>(get_value2.data()), get_value2.size()), std::string(1031, 'd'));
}

} // namespace yudb