//The MIT License(MIT)
//Copyright © 2024 https://github.com/yuyuaqwq
//
//Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files(the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and /or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
//
//The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
//THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

#include <filesystem>

#include <gtest/gtest.h>

#include "src/db_impl.h"

namespace atomkv {

class PagerTest : public testing::Test {
public:
    std::unique_ptr<atomkv::DB> db_;
    Pager* pager_{ nullptr };
    Logger* logger_{ nullptr };

public:
    PagerTest() {
        Open();
    }

    void Open() {
        atomkv::Options options{
            .max_wal_size = 1024 * 1024 * 64,
        };
        db_.reset();
        //std::string path = testing::TempDir() + "pager_test.ydb";
        const std::string path = "Z:/pager_test.ydb";
        std::filesystem::remove(path);
        std::filesystem::remove(path + "-shm");
        std::filesystem::remove(path + "-wal");
        db_ = atomkv::DB::Open(options, path);
        ASSERT_FALSE(!db_);

        auto db_impl = static_cast<DBImpl*>(db_.get());
        pager_ = &db_impl->pager();
        logger_ = &db_impl->logger();
    }

    std::span<const uint8_t> FromString(std::string_view str) {
        return { reinterpret_cast<const uint8_t*>(str.data()), str.size() };
    }

    const std::string_view ToString(std::span<const uint8_t> span) {
        return { reinterpret_cast<const char*>(span.data()), span.size() };
    }
};

TEST_F(PagerTest, AllocAndFree) {
    {
        auto tx = db_->Update();
        auto pgid = pager_->Alloc(1);
        ASSERT_EQ(pgid, 2);
        pgid = pager_->Alloc(1);
        ASSERT_EQ(pgid, 3);
        pgid = pager_->Alloc(1);
        ASSERT_EQ(pgid, 4);
        pgid = pager_->Alloc(10);
        ASSERT_EQ(pgid, 5);
        pgid = pager_->Alloc(100);
        ASSERT_EQ(pgid, 15);
        pgid = pager_->Alloc(1000);
        ASSERT_EQ(pgid, 115);
        pager_->Free(2, 1);
        pager_->Free(3, 1);
        pager_->Free(4, 1);
        pgid = pager_->Alloc(1);
        ASSERT_EQ(pgid, 1115);
        pager_->Free(115, 1000);
        tx.Commit();
    }
    {
        auto tx = db_->Update();
        auto pgid = pager_->Alloc(1);
        ASSERT_EQ(pgid, 1116);
        tx.Commit();
    }
    {
        auto tx = db_->Update();
        auto pgid = pager_->Alloc(1);
        ASSERT_EQ(pgid, 2);
        pgid = pager_->Alloc(1);
        ASSERT_EQ(pgid, 3);
        pgid = pager_->Alloc(1);
        ASSERT_EQ(pgid, 4);
        pgid = pager_->Alloc(998);
        ASSERT_EQ(pgid, 115);
        pgid = pager_->Alloc(2);
        ASSERT_EQ(pgid, 1113);
        pgid = pager_->Alloc(1);
        ASSERT_EQ(pgid, 1117);
        tx.Commit();
    }
    {
        auto tx = db_->Update();
        auto pgid = pager_->Alloc(1);
        ASSERT_EQ(pgid, 1118);
        pgid = pager_->Alloc(1);
        ASSERT_EQ(pgid, 1119);
        tx.Commit();
    }

    {
        auto tx = db_->Update();
        auto pgid = pager_->Alloc(100);
        ASSERT_EQ(pgid, 1120);
        tx.Commit();
    }
    {
        auto tx = db_->Update();
        pager_->Free(1120, 50);
        tx.Commit();
    }
    {
        auto tx = db_->Update();
        pager_->Free(1170, 50);
        tx.Commit();
    }
    {
        auto tx = db_->Update();
        auto pgid = pager_->Alloc(1);
        ASSERT_EQ(pgid, 1120);
        logger_->Checkpoint();
        tx.Commit();
    }
    {
        auto tx = db_->Update();
        auto pgid = pager_->Alloc(48);
        ASSERT_EQ(pgid, 1122);
        logger_->Checkpoint();
        tx.Commit();
    }
}

TEST_F(PagerTest, FreeListSaveAndLoad) {
    {
        auto tx = db_->Update();
        auto pgid = pager_->Alloc(1);
        ASSERT_EQ(pgid, 2);
        pgid = pager_->Alloc(1);
        ASSERT_EQ(pgid, 3);
        pgid = pager_->Alloc(1);
        ASSERT_EQ(pgid, 4);
        tx.Commit();
    }
    {
        auto tx = db_->Update();
        pager_->Free(2, 1);
        pager_->Free(3, 1);
        pager_->Free(4, 1);
        tx.Commit();
    }
    {

    }
    pager_->SaveFreeList();


}

} // namespace atomkv