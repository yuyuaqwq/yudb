// yudbpp.cpp : 此文件包含 "main" 函数。程序执行将在此处开始并结束。
//

#include <iostream>
#include <thread>
#include <set>
#include <map>
#include <chrono>
#include <algorithm>
#include <unordered_set>
#include <span>
#include <filesystem>

#include <gtest/gtest.h>

#include "yudb/db.h"

namespace yudb {

class DBTest : public testing::Test {
public:
    std::unique_ptr<yudb::DB> db_;
    int seed_{ 0 };
    int count_{ 1000000 };

public:
    DBTest() {
        Open();
    }

    void Open() {
        yudb::Options options{
            .max_wal_size = 1024 * 1024 * 64,
        };
        //std::string path = testing::TempDir() + "db_test.ydb";
        std::string path = "Z:/db_test.ydb";
        std::filesystem::remove(path);
        std::filesystem::remove(path + "-shm");
        std::filesystem::remove(path + "-wal");
        db_ = yudb::DB::Open(options, path);
        ASSERT_FALSE(!db_);
    }

    auto Update() {
        return db_->Update();
    }

    auto View() {
        return db_->View();
    }

    std::string RandomString(size_t min_size, size_t max_size) {
        int size;
        if (min_size == max_size) {
            size = min_size;
        }
        else {
            size = (rand() % (max_size - min_size)) + min_size;
        }
        std::string str(size, ' ');
        for (auto i = 0; i < size; i++) {
            str[i] = rand() % 26 + 'a';
        }
        return str;
    }
};

TEST_F(DBTest, UpdateAndView) {
    auto update_tx = Update();
    auto update_bucket = update_tx.UserBucket();
    auto view_tx1 = View();
    auto view_tx2 = View();
    auto view_bucket1 = view_tx1.UserBucket();
    auto view_bucket2 = view_tx2.UserBucket();

    update_bucket.Put("k1", "v1");

    auto iter = view_bucket1.Get("k1");
    ASSERT_EQ(iter, view_bucket1.end());
    iter = view_bucket2.Get("k1");
    ASSERT_EQ(iter, view_bucket2.end());

    update_tx.Commit();

    auto view_tx3 = View();
    auto view_tx4 = View();
    auto view_bucket3 = view_tx3.UserBucket();
    auto view_bucket4 = view_tx4.UserBucket();

    iter = view_bucket3.Get("k1");
    ASSERT_EQ(iter.key(), "k1");
    ASSERT_EQ(iter.value(), "v1");
    iter = view_bucket4.Get("k1");
    ASSERT_EQ(iter.key(), "k1");
    ASSERT_EQ(iter.value(), "v1");

    iter = view_bucket1.Get("k1");
    ASSERT_EQ(iter, view_bucket1.end());
    iter = view_bucket2.Get("k1");
    ASSERT_EQ(iter, view_bucket2.end());
}

TEST_F(DBTest, EmptyKey) {
    auto tx = Update();
    auto bucket = tx.UserBucket();

    bucket.Put("", "v");
    
    auto iter = bucket.Get("");
    ASSERT_EQ(iter.key(), "");
    ASSERT_EQ(iter.value(), "v");

    bucket.Delete("");

    iter = bucket.Get("");
    ASSERT_EQ(iter, bucket.end());

    tx.Commit();
}

TEST_F(DBTest, EmptyValue) {
    auto tx = Update();
    auto bucket = tx.UserBucket();

    bucket.Put("k", "");

    auto iter = bucket.Get("k");
    ASSERT_EQ(iter.key(), "k");
    ASSERT_EQ(iter.value(), "");

    bucket.Delete("k");

    iter = bucket.Get("k");
    ASSERT_EQ(iter, bucket.end());

    tx.Commit();
}

TEST_F(DBTest, EmptyKeyValue) {
    auto tx = Update();
    auto bucket = tx.UserBucket();

    bucket.Put("", "");

    auto iter = bucket.Get("");
    ASSERT_EQ(iter.key(), "");
    ASSERT_EQ(iter.value(), "");

    bucket.Delete("");

    iter = bucket.Get("");
    ASSERT_EQ(iter, bucket.end());

    tx.Commit();
}

TEST_F(DBTest, Put) {
    auto tx = Update();
    auto bucket = tx.UserBucket();

    bucket.Put("ABC", "123");
    bucket.Put("!@#$%^&*(", "999888777");
    auto iter = bucket.Get("ABC");
    ASSERT_EQ(iter.key(), "ABC");
    ASSERT_EQ(iter.value(), "123");
    
    iter = bucket.Get("!@#$%^&*(");
    ASSERT_EQ(iter.key(), "!@#$%^&*(");
    ASSERT_EQ(iter.value(), "999888777");

    bucket.Put("ABC", "0xCC");
    iter = bucket.Get("ABC");
    ASSERT_EQ(iter.key(), "ABC");
    ASSERT_EQ(iter.value(), "0xCC");

    tx.Commit();
}

TEST_F(DBTest, Delete) {
    auto tx = Update();
    auto bucket = tx.UserBucket();

    bucket.Put("k1", "v1");
    bucket.Put("k2", "v2");
    auto iter = bucket.Get("k1");
    ASSERT_EQ(iter.key(), "k1");
    ASSERT_EQ(iter.value(), "v1");

    bucket.Delete("k1");

    iter = bucket.Get("k1");
    ASSERT_EQ(iter, bucket.end());

    iter = bucket.Get("k2");
    ASSERT_EQ(iter.key(), "k2");
    ASSERT_EQ(iter.value(), "v2");

    tx.Commit();
}

TEST_F(DBTest, PutLongData) {
    auto long_key1 = RandomString(4096, 4096);
    auto long_value1 = RandomString(1024 * 1024, 1024 * 1024);
    auto long_value2 = RandomString(1024 * 1024, 1024 * 1024);

    auto tx = Update();
    auto bucket = tx.UserBucket();

    bucket.Put(long_key1, long_value1);
    auto iter = bucket.Get(long_key1);
    ASSERT_EQ(iter.key(), long_key1);
    ASSERT_EQ(iter.value(), long_value1);

    bucket.Put(long_key1, long_value2);
    iter = bucket.Get(long_key1);
    ASSERT_EQ(iter.key(), long_key1);
    ASSERT_EQ(iter.value(), long_value2);

    tx.Commit();
}

TEST_F(DBTest, DeleteLongData) {
    auto long_key1 = RandomString(4096, 4096);
    auto long_value1 = RandomString(1024 * 1024, 1024 * 1024);
    auto long_value2 = RandomString(1024 * 1024, 1024 * 1024);

    auto tx = Update();
    auto bucket = tx.UserBucket();

    bucket.Put(long_key1, long_value1);
    auto iter = bucket.Get(long_key1);
    ASSERT_EQ(iter.key(), long_key1);
    ASSERT_EQ(iter.value(), long_value1);

    bucket.Delete(long_key1);
    iter = bucket.Get(long_key1);
    ASSERT_EQ(iter, bucket.end());

    tx.Commit();
}

TEST_F(DBTest, SubBucket) {
    auto tx = Update();
    auto bucket = tx.UserBucket();

    auto sub_bucket1 = bucket.SubUpdateBucket("sub1");
    sub_bucket1.Put("sub1_key1", "sub1_value1");
    sub_bucket1.Put("sub1_key2", "sub1_value2");
    auto iter = sub_bucket1.Get("sub1_key1");
    ASSERT_EQ(iter.key(), "sub1_key1");
    ASSERT_EQ(iter.value(), "sub1_value1");

    auto sub_bucket2 = bucket.SubUpdateBucket("sub2");
    sub_bucket2.Put("sub2_key1", "sub2_value1");
    sub_bucket2.Put("sub2_key2", "sub2_value2");
    iter = sub_bucket2.Get("sub2_key1");
    ASSERT_EQ(iter.key(), "sub2_key1");
    ASSERT_EQ(iter.value(), "sub2_value1");

    auto sub_bucket3 = sub_bucket1.SubUpdateBucket("sub3");
    sub_bucket3.Put("sub3_key1", "sub3_value1");
    sub_bucket3.Put("sub3_key2", "sub3_value2");
    iter = sub_bucket3.Get("sub3_key1");
    ASSERT_EQ(iter.key(), "sub3_key1");
    ASSERT_EQ(iter.value(), "sub3_value1");

    bucket.Put("k", "v");

    iter = bucket.Get("sub1");
    ASSERT_NE(iter, bucket.end());
    ASSERT_TRUE(iter.is_bucket());
    iter = bucket.Get("sub2");
    ASSERT_NE(iter, bucket.end());
    ASSERT_TRUE(iter.is_bucket());
    iter = bucket.Get("sub3");
    ASSERT_EQ(iter, bucket.end());
    iter = sub_bucket1.Get("sub3");
    ASSERT_NE(iter, bucket.end());
    iter = bucket.Get("k");
    ASSERT_NE(iter, bucket.end());
    ASSERT_FALSE(iter.is_bucket());

    ASSERT_FALSE(bucket.DeleteSubBucket("sub3"));
    ASSERT_TRUE(bucket.DeleteSubBucket("sub2"));
    ASSERT_TRUE(bucket.DeleteSubBucket("sub1"));

    tx.Commit();
}

TEST_F(DBTest, BatchPutAndDeleteInOrder) {
    std::vector<int64_t> arr(count_);
    for (auto i = 0; i < count_; i++) {
        arr[i] = i;
    }

    auto tx = Update();
    auto bucket = tx.UserBucket();

    auto j = 0;
    for (auto& data : arr) {
        bucket.Put(&data, sizeof(data), &data, sizeof(data));
        ++j;
    }

    j = 0;
    for (auto& data : arr) {
        auto iter = bucket.Get(&data, sizeof(data));
        ASSERT_NE(iter, bucket.end());
        ++j;
    }

    j = 0;
    for (auto& data : arr) {
        auto success = bucket.Delete(&data, sizeof(data));
        ASSERT_TRUE(success);
        ++j;
    }

    j = 0;
    for (auto& data : arr) {
        auto iter = bucket.Get(&data, sizeof(data));
        ASSERT_EQ(iter, bucket.end());
        ++j;
    }

    tx.Commit();
}

TEST_F(DBTest, BatchPutAndDeleteInReverseOrder) {
    std::vector<int64_t> arr(count_);
    for (auto i = count_ - 1; i >= 0; --i) {
        arr[i] = i;
    }

    auto tx = Update();
    auto bucket = tx.UserBucket();
    auto j = 0;
    for (auto& data : arr) {
        ++j;
        bucket.Put(&data, sizeof(data), &data, sizeof(data));
    }
   
    j = 0;
    for (auto& data : arr) {
        auto iter = bucket.Get(&data, sizeof(data));
        ASSERT_NE(iter, bucket.end());
        ++j;
    }

    j = 0;
    for (auto& data : arr) {
        auto success = bucket.Delete(&data, sizeof(data));
        ASSERT_TRUE(success);
        ++j;
    }

    j = 0;
    for (auto& data : arr) {
        auto iter = bucket.Get(&data, sizeof(data));
        ASSERT_EQ(iter, bucket.end());
        ++j;
    }

    tx.Commit();
}

TEST_F(DBTest, BatchPutAndDeleteInRandom) {
    srand(seed_);

    std::vector<std::string> arr(count_);
    for (auto i = 0; i < count_; i++) {
        arr[i] = RandomString(16, 100);
    }

    auto tx = Update();
    auto bucket = tx.UserBucket();

    auto i = 0;
    for (auto& iter : arr) {
        bucket.Put(iter, iter);
        ++i;
    }

    i = 0;
    for (auto& iter : arr) {
        auto res = bucket.Get(iter);
        ASSERT_NE(res, bucket.end());
        ASSERT_EQ(res.key(), iter);
        ASSERT_EQ(res.key(), res.value());
        ++i;
    }

    i = 0;
    for (auto& iter : arr) {
        auto res = bucket.Delete(iter);
        ASSERT_TRUE(res);
        ++i;
    }

    i = 0;
    for (auto& iter : arr) {
        auto res = bucket.Get(iter);
        ASSERT_EQ(res, bucket.end());
        ++i;
    }

    tx.Commit();

}

TEST_F(DBTest, PutAndDeleteInOrder) {
    std::vector<int64_t> arr(count_);
    for (auto i = 0; i < count_; i++) {
        arr[i] = i;
    }

    auto j = 0;
    for (auto& iter : arr) {
        auto tx = Update();
        auto bucket = tx.UserBucket();
        bucket.Put(&iter, sizeof(iter), &iter, sizeof(iter));
        ++j;
        tx.Commit();
    }

    j = 0;
    for (auto& iter : arr) {
        auto tx = View();
        auto bucket = tx.UserBucket();
        auto res = bucket.Get(&iter, sizeof(iter));
        ASSERT_NE(res, bucket.end());
        ++j;
    }

    j = 0;
    for (auto& iter : arr) {
        auto tx = Update();
        auto bucket = tx.UserBucket();
        auto res = bucket.Delete(&iter, sizeof(iter));
        ASSERT_TRUE(res);
        ++j;
        tx.Commit();
    }

    j = 0;
    for (auto& iter : arr) {
        auto tx = View();
        auto bucket = tx.UserBucket();
        auto res = bucket.Get(&iter, sizeof(iter));
        ASSERT_EQ(res, bucket.end());
        ++j;
    }

}

TEST_F(DBTest, PutAndDeleteInReverseOrder) {
    std::vector<int64_t> arr(count_);
    for (auto i = count_ - 1; i >= 0; --i) {
        arr[i] = i;
    }

    auto j = 0;
    for (auto& iter : arr) {
        auto tx = Update();
        auto bucket = tx.UserBucket();
        bucket.Put(&iter, sizeof(iter), &iter, sizeof(iter));
        ++j;
        tx.Commit();
    }

    j = 0;
    for (auto& iter : arr) {
        auto tx = View();
        auto bucket = tx.UserBucket();
        auto res = bucket.Get(&iter, sizeof(iter));
        ASSERT_NE(res, bucket.end());
        ++j;
    }

    j = 0;
    for (auto& iter : arr) {
        auto tx = Update();
        auto bucket = tx.UserBucket();
        auto res = bucket.Delete(&iter, sizeof(iter));
        ASSERT_TRUE(res);
        ++j;
        tx.Commit();
    }

    j = 0;
    for (auto& iter : arr) {
        auto tx = View();
        auto bucket = tx.UserBucket();
        auto res = bucket.Get(&iter, sizeof(iter));
        ASSERT_EQ(res, bucket.end());
        ++j;
    }

}

TEST_F(DBTest, PutAndDeleteInRandom) {
    srand(seed_);

    std::vector<std::string> arr(count_);
    for (auto i = 0; i < count_; i++) {
        arr[i] = RandomString(16, 100);
    }

    auto j = 0;
    for (auto& iter : arr) {
        auto tx = Update();
        auto bucket = tx.UserBucket();
        bucket.Put(iter, iter);
        ++j;
        tx.Commit();
    }

    j = 0;
    for (auto& iter : arr) {
        auto tx = View();
        auto bucket = tx.UserBucket();
        auto res = bucket.Get(iter);
        ASSERT_NE(res, bucket.end());
        ASSERT_EQ(res.key(), iter);
        ASSERT_EQ(res.key(), res.value());
        ++j;
    }

    j = 0;
    for (auto& iter : arr) {
        auto tx = Update();
        auto bucket = tx.UserBucket();
        auto res = bucket.Delete(iter);
        ASSERT_TRUE(res);
        ++j;
        tx.Commit();
    }

    j = 0;
    for (auto& iter : arr) {
        auto tx = View();
        auto bucket = tx.UserBucket();
        auto res = bucket.Get(iter);
        ASSERT_EQ(res, bucket.end());
        ++j;
    }

}

TEST_F(DBTest, Recover) {
    // todo:
}

} // namespace yudb

