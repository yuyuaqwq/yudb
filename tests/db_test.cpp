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

#include <gtest/gtest.h>

#include "yudb/db.h"

namespace yudb {

static std::string RandomString(size_t min_size, size_t max_size) {
    int size;
    if (min_size == max_size) {
        size = min_size;
    } else {
        size = (rand() % (max_size - min_size)) + min_size;
    }
    std::string str(size, ' ');
    for (auto i = 0; i < size; i++) {
        str[i] = rand() % 26 + 'a';
    }
    return str;
}

static std::unique_ptr<yudb::DB> db;

TEST(DBTest, Open) {
    yudb::Options options{
        .checkpoint_wal_threshold = 1024 * 1024 * 64,
    };
    db = yudb::DB::Open(options, "Z:/db_test.ydb");
    ASSERT_FALSE(!db);
}

TEST(DBTest, BatchSequential) {
    auto count = 1000000;

    std::vector<int64_t> arr(count);
    for (auto i = 0; i < count; i++) {
        arr[i] = i;
    }

    for (auto i = 0; i < 2; i++) {
        auto start_time = std::chrono::high_resolution_clock::now();
        {
            auto tx = db->Update();
            auto bucket = tx.UserBucket(yudb::UInt64Comparator);

            auto j = 0;
            std::string_view value{ nullptr, 0 };
            for (auto& iter : arr) {
                //printf("%d\n", i);
                bucket.Put(&iter, sizeof(iter), &value, sizeof(value));
                //bucket.Print(); printf("\n\n\n\n");
                ++j;
            }
            //bucket.Print();
            tx.Commit();
        }
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        std::cout << "insert: " << duration.count() << " ms" << std::endl;

        start_time = std::chrono::high_resolution_clock::now();
        {
            auto tx = db->View();
            auto bucket = tx.UserBucket(yudb::UInt64Comparator);
            auto j = 0;
            for (auto& iter : arr) {
                auto res = bucket.Get(&iter, sizeof(iter));
                ASSERT_NE(res, bucket.end());
                //assert(res.value() == iter);
                ++j;
                //bucket.Put(&arr[i], sizeof(arr[i]), &arr[i], sizeof(arr[i]));
                //bucket.Print(); printf("\n\n\n\n\n");
            }
        }
        end_time = std::chrono::high_resolution_clock::now();
        duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        std::cout << "get: " << duration.count() << " ms" << std::endl;

        start_time = std::chrono::high_resolution_clock::now();
        {
            auto tx = db->Update();
            auto bucket = tx.UserBucket(yudb::UInt64Comparator);
            auto j = 0;
            for (auto& iter : arr) {
                auto res = bucket.Delete(&iter, sizeof(iter));
                ASSERT_TRUE(res);
                //assert(res.value() == iter);
                ++j;
                //bucket.Put(&arr[i], sizeof(arr[i]), &arr[i], sizeof(arr[i]));
                //bucket.Print(); printf("\n\n\n\n\n");
            }
            tx.Commit();
        }
        end_time = std::chrono::high_resolution_clock::now();
        duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        std::cout << "delete: " << duration.count() << " ms" << std::endl;

        std::reverse(arr.begin(), arr.end());
    }
}


TEST(DBTest, BatchRandom) {
    // 需要注意这里用的比较器不同了

    srand(10);

    auto count = 1000000;
    std::vector<std::string> arr(count);

    for (auto i = 0; i < count; i++) {
        arr[i] = yudb::RandomString(16, 100);
    }

    auto start_time = std::chrono::high_resolution_clock::now();
    {
        auto tx = db->Update();
        auto bucket = tx.UserBucket();

        auto i = 0;
        std::string_view value{ nullptr, 0 };
        for (auto& iter : arr) {
            //printf("%d\n", i);
            bucket.Put(iter, value);
            //bucket.Print(); printf("\n\n\n\n");
            ++i;
        }
        //bucket.Print();
        tx.Commit();
    }
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    std::cout << "insert: " << duration.count() << " ms" << std::endl;

    start_time = std::chrono::high_resolution_clock::now();
    {
        auto tx = db->View();
        auto bucket = tx.UserBucket();
        auto i = 0;
        for (auto& iter : arr) {
            auto res = bucket.Get(iter.c_str(), iter.size());
            ASSERT_NE(res, bucket.end());
            //assert(res.value() == iter);
            ++i;
            //bucket.Put(&arr[i], sizeof(arr[i]), &arr[i], sizeof(arr[i]));
            //bucket.Print(); printf("\n\n\n\n\n");
        }
    }
    end_time = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    std::cout << "get: " << duration.count() << " ms" << std::endl;

    start_time = std::chrono::high_resolution_clock::now();
    {
        auto tx = db->Update();
        auto bucket = tx.UserBucket();
        auto i = 0;
        for (auto& iter : arr) {
            auto res = bucket.Delete(iter.c_str(), iter.size());
            ASSERT_TRUE(res);
            //assert(res.value() == iter);
            ++i;
            //bucket.Put(&arr[i], sizeof(arr[i]), &arr[i], sizeof(arr[i]));
            //bucket.Print(); printf("\n\n\n\n\n");
        }
        tx.Commit();
    }
    end_time = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    std::cout << "delete: " << duration.count() << " ms" << std::endl;
}

TEST(DBTest, Sequential) {
    auto count = 1000000;

    std::vector<int64_t> arr(count);
    for (auto i = 0; i < count; i++) {
        arr[i] = i;
    }

    for (auto i = 0; i < 2; i++) {
        auto start_time = std::chrono::high_resolution_clock::now();
        {
            auto j = 0;
            std::string_view value{ nullptr, 0 };
            for (auto& iter : arr) {
                auto tx = db->Update();
                auto bucket = tx.UserBucket(yudb::UInt64Comparator);
                //printf("%d\n", i);
                bucket.Put(&iter, sizeof(iter), &value, sizeof(value));
                //bucket.Print(); printf("\n\n\n\n");
                ++j;
                tx.Commit();
            }
            //bucket.Print();

        }
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        std::cout << "insert: " << duration.count() << " ms" << std::endl;

        start_time = std::chrono::high_resolution_clock::now();
        {
            auto j = 0;
            for (auto& iter : arr) {
                auto tx = db->View();
                auto bucket = tx.UserBucket(yudb::UInt64Comparator);
                auto res = bucket.Get(&iter, sizeof(iter));
                ASSERT_NE(res, bucket.end());
                //assert(res.value() == iter);
                ++j;
                //bucket.Put(&arr[i], sizeof(arr[i]), &arr[i], sizeof(arr[i]));
                //bucket.Print(); printf("\n\n\n\n\n");
            }
        }
        end_time = std::chrono::high_resolution_clock::now();
        duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        std::cout << "get: " << duration.count() << " ms" << std::endl;

        start_time = std::chrono::high_resolution_clock::now();
        {
            auto j = 0;
            for (auto& iter : arr) {
                auto tx = db->Update();
                auto bucket = tx.UserBucket(yudb::UInt64Comparator);
                auto res = bucket.Delete(&iter, sizeof(iter));
                ASSERT_TRUE(res);
                //assert(res.value() == iter);
                ++j;
                //bucket.Put(&arr[i], sizeof(arr[i]), &arr[i], sizeof(arr[i]));
                //bucket.Print(); printf("\n\n\n\n\n");
                tx.Commit();
            }

        }
        end_time = std::chrono::high_resolution_clock::now();
        duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
        std::cout << "delete: " << duration.count() << " ms" << std::endl;

        std::reverse(arr.begin(), arr.end());
    }
}

} // namespace yudb

