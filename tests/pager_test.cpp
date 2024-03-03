#include "gtest/gtest.h"

#include "yudb/db_impl.h"

namespace yudb {

static std::unique_ptr<yudb::DB> db;

TEST(PagerTest, AllocAndPending) {
    yudb::Options options{
        .page_size = 1024,
        .cache_pool_page_count = size_t(options.page_size) * 1024,
        .log_file_max_bytes = 1024 * 1024 * 64,
    };
    db = yudb::DB::Open(options, "Z:/pager_test.ydb");
    ASSERT_TRUE(db.operator bool());
    auto db_impl = static_cast<DBImpl*>(db.get());
    auto& pager = db_impl->pager();
    {
        auto tx = db_impl->Update();

        // 这里分配了pgid为2的页面作为root page
        auto root = tx.UserBucket();

        auto pgid = pager.Alloc(1);
        ASSERT_EQ(pgid, 3);

        pgid = pager.Alloc(1);
        ASSERT_EQ(pgid, 4);

        pgid = pager.Alloc(10);
        ASSERT_EQ(pgid, 5);

        pgid = pager.Alloc(100);
        ASSERT_EQ(pgid, 15);

        pgid = pager.Alloc(1000);
        ASSERT_EQ(pgid, 115);

        pager.Free(3, 1);
        pgid = pager.Alloc(1);
        ASSERT_EQ(pgid, 1115);

        pager.Free(115, 1000);

        // 为PendingDB的root分配了1116，Pending了3和115
        tx.Commit();
    }
    {
        auto tx = db_impl->Update();

        auto pgid = pager.Alloc(1);
        ASSERT_EQ(pgid, 1117);

        // root触发了Copy分配了1118，Pending了2
        tx.Commit();
    }
    {
        // FreeDB释放3、115，为FreeDB分配了1119
        auto tx = db_impl->Update();

        auto pgid = pager.Alloc(1);
        ASSERT_EQ(pgid, 3);

        pgid = pager.Alloc(1);
        ASSERT_EQ(pgid, 115);
        pgid = pager.Alloc(1);
        ASSERT_EQ(pgid, 116);
        pgid = pager.Alloc(998);
        ASSERT_EQ(pgid, 117);

        pgid = pager.Alloc(1);
        ASSERT_EQ(pgid, 1120);

        // root触发了Copy，分配了1121.Pending了1118
        tx.Commit();
    }

    {
        // FreeDB释放2时触发了Copy，分配了1122，Pending了1119
        auto tx = db_impl->Update();

        auto pgid = pager.Alloc(1);
        ASSERT_EQ(pgid, 2);

        pgid = pager.Alloc(1);
        ASSERT_EQ(pgid, 1123);
        
        // PendingDB的Root触发了Copy，分配了1124，Pending了1116
        // root触发了Copy，分配了1124，Pending了1121
        tx.Commit();
    }
}

TEST(PagerTest, Clear) {

}

} // namespace yudb