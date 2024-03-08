#include "gtest/gtest.h"

#include "yudb/db_impl.h"

namespace yudb {

static std::unique_ptr<yudb::DB> db;

TEST(PagerTest, AllocAndFree) {
    yudb::Options options{
        .max_wal_size = 1024 * 1024 * 64,
    };
    db = yudb::DB::Open(options, "Z:/pager_test.ydb");
    ASSERT_TRUE(db.operator bool());
    auto db_impl = static_cast<DBImpl*>(db.get());
    auto& pager = db_impl->pager();

    {
        auto tx = db_impl->Update();
        auto pgid = pager.Alloc(1);
        ASSERT_EQ(pgid, 2);
        pgid = pager.Alloc(1);
        ASSERT_EQ(pgid, 3);
        pgid = pager.Alloc(1);
        ASSERT_EQ(pgid, 4);
        pgid = pager.Alloc(10);
        ASSERT_EQ(pgid, 5);
        pgid = pager.Alloc(100);
        ASSERT_EQ(pgid, 15);
        pgid = pager.Alloc(1000);
        ASSERT_EQ(pgid, 115);
        pager.Free(2, 1);
        pager.Free(3, 1);
        pager.Free(4, 1);
        pgid = pager.Alloc(1);
        ASSERT_EQ(pgid, 1115);
        pager.Free(115, 1000);
        tx.Commit();
    }
    {
        auto tx = db_impl->Update();
        auto pgid = pager.Alloc(1);
        ASSERT_EQ(pgid, 1116);
        tx.Commit();
    }
    {
        auto tx = db_impl->Update();
        auto pgid = pager.Alloc(1);
        ASSERT_EQ(pgid, 2);
        pgid = pager.Alloc(1);
        ASSERT_EQ(pgid, 3);
        pgid = pager.Alloc(1);
        ASSERT_EQ(pgid, 4);
        pgid = pager.Alloc(998);
        ASSERT_EQ(pgid, 115);
        pgid = pager.Alloc(2);
        ASSERT_EQ(pgid, 1113);
        pgid = pager.Alloc(1);
        ASSERT_EQ(pgid, 1117);
        tx.Commit();
    }
    {
        auto tx = db_impl->Update();
        auto pgid = pager.Alloc(1);
        ASSERT_EQ(pgid, 1118);
        pgid = pager.Alloc(1);
        ASSERT_EQ(pgid, 1119);
        tx.Commit();
    }

    {
        auto tx = db_impl->Update();
        auto pgid = pager.Alloc(100);
        ASSERT_EQ(pgid, 1120);
        tx.Commit();
    }
    {
        auto tx = db_impl->Update();
        pager.Free(1120, 50);
        tx.Commit();
    }
    {
        auto tx = db_impl->Update();
        pager.Free(1170, 50);
        tx.Commit();
    }
    {
        auto tx = db_impl->Update();
        auto pgid = pager.Alloc(1);
        ASSERT_EQ(pgid, 1120);
        db_impl->logger()->Checkpoint();
        tx.Commit();
    }
    {
        auto tx = db_impl->Update();
        auto pgid = pager.Alloc(48);
        ASSERT_EQ(pgid, 1122);
        db_impl->logger()->Checkpoint();
        tx.Commit();
    }
}

} // namespace yudb