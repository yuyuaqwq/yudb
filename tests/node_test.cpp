#include "gtest/gtest.h"

#include "yudb/db_impl.h"
#include "yudb/node.h"

namespace yudb {

TEST(NodeTest, Slot) {
    yudb::Options options{
        .checkpoint_wal_threshold = 1024 * 1024 * 64,
    };
    auto db = yudb::DB::Open(options, "Z:/pager_test.ydb");
    ASSERT_TRUE(db.operator bool());
    auto db_impl = static_cast<DBImpl*>(db.get());
    auto& pager = db_impl->pager();
    auto& tx_manager = db_impl->tx_manager();
    {
        auto tx = db_impl->Update();
        auto& update_tx = tx_manager.update_tx();
        auto& user_bucket = update_tx.user_bucket();

        LeafNode leaf_node{ &user_bucket.btree(),pager.Alloc(1), true };

        //leaf_node.Append();
    }

}

} // namespace yudb