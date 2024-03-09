#include "gtest/gtest.h"

#include "yudb/db_impl.h"
#include "yudb/node.h"

namespace yudb {

TEST(NodeTest, Slot) {
    yudb::Options options{
        .max_wal_size = 1024 * 1024 * 64,
    };
    auto db = yudb::DB::Open(options, "Z:/node_test.ydb");
    ASSERT_TRUE(db.operator bool());
    auto db_impl = static_cast<DBImpl*>(db.get());
    auto& pager = db_impl->pager();
    auto& tx_manager = db_impl->tx_manager();
    {
        auto tx = db_impl->Update();
        auto& update_tx = tx_manager.update_tx();
        auto& user_bucket = update_tx.user_bucket();


        {
            LeafNode leaf_node{ &user_bucket.btree(),pager.Alloc(1), true };
            leaf_node.Build();
            std::string key1(1024, 'a');
            std::string value1(1024, 'b');
            leaf_node.Append({ reinterpret_cast<uint8_t*>(key1.data()), key1.size() },
                { reinterpret_cast<uint8_t*>(value1.data()), value1.size() });
            std::string key2(1024, 'c');
            std::string value2(1024, 'd');
            leaf_node.Append({ reinterpret_cast<uint8_t*>(key2.data()), key2.size() },
                { reinterpret_cast<uint8_t*>(value2.data()), value2.size() });
            auto get_key1 = leaf_node.GetKey(0);
            assert(std::string(reinterpret_cast<const char*>(get_key1.data()), get_key1.size()) == std::string(1024, 'a'));
            auto get_value1 = leaf_node.GetValue(0);
            assert(std::string(reinterpret_cast<const char*>(get_value1.data()), get_value1.size()) == std::string(1024, 'b'));

            auto get_key2 = leaf_node.GetKey(1);
            assert(std::string(reinterpret_cast<const char*>(get_key2.data()), get_key2.size()) == std::string(1024, 'c'));
            auto get_value2 = leaf_node.GetValue(1);
            assert(std::string(reinterpret_cast<const char*>(get_value2.data()), get_value2.size()) == std::string(1024, 'd'));
        }

        {
            LeafNode leaf_node{ &user_bucket.btree(),pager.Alloc(1), true };
            leaf_node.Build();
            std::string key1(1000, 'a');
            std::string value1(1031, 'b');
            leaf_node.Append({ reinterpret_cast<uint8_t*>(key1.data()), key1.size() },
                { reinterpret_cast<uint8_t*>(value1.data()), value1.size() });
            std::string key2(1000, 'c');
            std::string value2(1031, 'd');
            leaf_node.Append({ reinterpret_cast<uint8_t*>(key2.data()), key2.size() },
                { reinterpret_cast<uint8_t*>(value2.data()), value2.size() });

            auto get_key1 = leaf_node.GetKey(0);
            assert(std::string(reinterpret_cast<const char*>(get_key1.data()), get_key1.size()) == std::string(1000, 'a'));
            auto get_value1 = leaf_node.GetValue(0);
            assert(std::string(reinterpret_cast<const char*>(get_value1.data()), get_value1.size()) == std::string(1031, 'b'));

            auto get_key2 = leaf_node.GetKey(1);
            assert(std::string(reinterpret_cast<const char*>(get_key2.data()), get_key2.size()) == std::string(1000, 'c'));
            auto get_value2 = leaf_node.GetValue(1);
            assert(std::string(reinterpret_cast<const char*>(get_value2.data()), get_value2.size()) == std::string(1031, 'd'));
        }
    }

}

} // namespace yudb