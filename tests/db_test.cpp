﻿// yudbpp.cpp : 此文件包含 "main" 函数。程序执行将在此处开始并结束。
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

#include "yudb\db.h"

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
        .page_size = 1024,
        .cache_pool_page_count = size_t(options.page_size) * 1024,
        .log_file_max_bytes = 1024 * 1024 * 64,
    };
    db = yudb::DB::Open(options, "Z:/test.ydb");
}

TEST(DBTest, SequentialFixedLength) {

}

} // namespace yudb



//void TestPager(yudb::DB* db) {
//    std::unordered_set<yudb::PageId> set;
//
//    for (auto i = 0; i < 100000; i++) {
//        auto pgid = db->pager_->Alloc(1);
//        set.insert(pgid);
//        auto page_ref = db->pager_->Reference(pgid, true);
//        for (auto i = 0; i < db->pager_->page_size(); i+=4) {
//            std::memcpy(&(&page_ref.content<uint8_t>())[i], & pgid, sizeof(pgid));
//        }
//        
//    }
//    for (auto pgid : set) {
//        auto page_ref = db->pager_->Reference(pgid, false);
//        for (auto i = 0; i < db->pager_->page_size(); i += 4) {
//            assert(std::memcmp(&(&page_ref.content<uint8_t>())[i], &pgid, sizeof(pgid)) == 0);
//        }
//    }
//}

void TestBTree(yudb::DB* db) {
    srand(10);

    auto count = 1;
    std::vector<int> arr(count);

    for (auto i = 0; i < count; i++) {
        arr[i] = i; // (rand() << 16) | rand();//
    }

    {
        auto tx = db->Update();
        auto bucket = tx.UserBucket();
        //printf("\n\n\n\n");
        //bucket.Put("c", "aa");
        //bucket.Put("e", "aaa");
        //auto aa = bucket.LowerBound("a");
        //if (aa == bucket.begin()) {
        //    printf("??");
        //}
        //bucket.Print();
        //auto bb = aa.key();
        //if (aa == bucket.end()) {
        //    printf("??");
        //}
        //auto sub_bucket = bucket.SubUpdateBucket("abcd");
        //for (auto& iter : bucket) {
        //    std::cout << "is_bucket:" << iter.key<int>() << " " << iter.value<int>() << "" << iter.is_bucket() << std::endl;
        //    //assert(iter.key<int>() > old_key);
        //}

        //for (auto i = 0; i < count; i++) {
        //    bucket.Put(&arr[i], sizeof(arr[i]), &arr[i], sizeof(arr[i]));
        //    //bucket.Print(); printf("\n\n\n\n\n");
        //}

        for (auto i = count - 1; i >= 0; i--) {
            bucket.Put(&arr[i], sizeof(arr[i]), &arr[i], sizeof(arr[i]));
            //bucket.Print(); printf("\n\n\n\n\n");
        }


        //bucket.Print(); printf("\n\n\n\n\n");

        /*sub_bucket.Put("abc", "def");
        sub_bucket.Print();  printf("\n\n\n\n\n");*/
        for (auto i = 0; i < count; i++) {
            auto res = bucket.Get(&arr[i], sizeof(arr[i]));
            assert(res != bucket.end());
            assert(res.value<int>() == arr[i]);
        }
        //bucket.Print(); printf("\n\n\n\n\n");
        int old_key = -1;
        for (auto& iter : bucket) {
            //std::cout << "is_bucket:" << iter.key<int>() << " " << iter.value<int>() << "" << iter.is_bucket() << std::endl;
            assert(iter.key<int>() > old_key);
        }

        tx.Commit();
    }



    {
        auto view_tx = db->View();
        auto view_bucket = view_tx.RootBucket();
        //view_bucket.Print(); printf("\n\n\n\n\n");
        //if (count <= 100000) {
        //    bucket.Print();
        //    printf("\n\n\n\n\n");
        //}

        //for (auto& iter : view_bucket) {
        //    //std::cout << "is_bucket:" << iter.key<int>() << " " << iter.value<int>() << "" << iter.is_bucket() << std::endl;
        //    //assert(iter.key<int>() > old_key);
        //}
        //auto sub_bucket = view_bucket.SubViewBucket("abcd");

        //auto iter = sub_bucket.Get("abc");
        //auto value = iter.value();

        for (auto i = 0; i < count; i++) {
            auto res = view_bucket.Get(&arr[i], sizeof(arr[i]));
            assert(res != view_bucket.end());
        }

    }

    {
        auto tx = db->Update();
        auto bucket = tx.UserBucket();

        //for (auto i = 0; i < count; i++) {
        //    auto res = bucket.Delete(&arr[i], sizeof(arr[i]));
        //    //bucket.Print(); printf("\n\n\n\n\n");
        //    assert(res);
        //}

        for (auto i = count - 1; i >= 0; --i) {
            auto res = bucket.Delete(&arr[i], sizeof(arr[i]));
            //bucket.Print(); printf("\n\n\n\n\n");
            assert(res);
        }

        //bucket.Print(); printf("\n\n\n\n\n");
        tx.Commit();
    }

    //{
    //    auto tx = db->Update();
    //    auto bucket = tx.RootBucket();

    //    bucket.Print(); printf("\n\n\n\n\n");

    //    for (auto i = count - 1; i >= 0; i--) {
    //        //if (i == 236) DebugBreak();
    //        bucket.Put(&arr[i], sizeof(arr[i]), &arr[i], sizeof(arr[i]));
    //        //bucket.Print(); printf("\n\n\n\n\n");
    //        //auto res = bucket.Get(&arr[299], sizeof(arr[i]));
    //        //assert(res != bucket.end());
    //    }

    //    int old_key = -1;
    //    for (auto& iter : bucket) {
    //        assert(iter.key<int>() > old_key);
    //    }

    //    //auto i = 0;
    //    //for (auto& iter : bucket) {
    //    //    std::cout << std::hex << i++ << " " << std::hex << iter.key<uint32_t>() << "    ";
    //    //    //assert(iter.key<uint32_t>() == i++);
    //    //}


    //    //if (count <= 100000) {
    //    //    bucket.Print();
    //    //    printf("\n\n\n\n\n");
    //    //}


    //    bucket.Print(); printf("\n\n\n\n\n");
    //    for (auto i = count - 1; i >= 0; i--) {
    //        //if (i == 239) DebugBreak();
    //        auto res = bucket.Get(&arr[i], sizeof(arr[i]));
    //        assert(res != bucket.end());
    //    }

    //    //bucket.Print(); printf("\n\n\n\n\n");

    //    for (auto i = count - 1; i >= 0; i--) {
    //        auto res = bucket.Delete(&arr[i], sizeof(arr[i]));
    //        //bucket.Print(); printf("\n\n\n\n\n");
    //        assert(res);
    //    }


    //    std::unordered_set<int> set;
    //    for (auto i = 0; i < count; i++) {
    //        //std::swap(arr[rand() % count], arr[rand() % count]);
    //        arr[i] = (rand() << 16) | rand();
    //        set.insert(arr[i]);
    //    }

    //    for (auto i = 0; i < count; i++) {
    //        bucket.Put(&arr[i], sizeof(arr[i]), &arr[i], sizeof(arr[i]));
    //    }

    //    bucket.Print(); printf("\n\n\n\n\n");

    //    auto view_tx = db->View();
    //    auto view_bucket = view_tx.RootBucket();

    //    //bucket.Print(); printf("\n\n\n\n\n");

    //    for (auto i = 0; i < count; i++) {
    //        auto res = bucket.Get(&arr[i], sizeof(arr[i]));
    //        assert(res != bucket.end());
    //    }

    //    for (auto val : set) {
    //        auto res = bucket.Delete(&val, sizeof(val));
    //        //printf("del:%x\n", val);
    //        //bucket.Print(); printf("\n\n\n\n\n");
    //        assert(res);
    //    }

    //    //bucket.Print(); printf("\n\n\n\n\n");
    //    auto k = 0;
    //    bucket.Delete(&k, sizeof(k));
    //}
}

void TestBlock(yudb::DB* db) {

    //bucket.Put("hello world!", "Cpp yyds!");
    //bucket.Put("This is yudb", "value!");
    //bucket.Put("123123", "123123");
    //bucket.Put("456456", "456456");
    //bucket.Put("789789", "789789");
    //bucket.Put("abcabc", "abcabc");
    //auto res = bucket.Get("hello world!");
    //assert(res != bucket.end());
    //for (auto& iter : bucket) {
    //    auto key = iter.key();
    //    auto value = iter.value();
    //    std::cout << key << ":";
    //    std::cout << value << std::endl;
    //}

    srand(10);

    auto count = 1000000;
    std::vector<std::string> arr(count);


    for (auto i = 0; i < count; i++) {
        arr[i] = yudb::RandomString(32, 32);
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

    // 获取程序结束时间点
    auto end_time = std::chrono::high_resolution_clock::now();

    // 计算时间差
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    // 打印运行时间
    std::cout << "写: " << duration.count() << " ms" << std::endl;

    

    start_time = std::chrono::high_resolution_clock::now();
    {
        auto tx = db->View();
        auto bucket = tx.RootBucket();
        auto i = 0;
        for (auto& iter : arr) {
            auto res = bucket.Get(iter.c_str(), iter.size());
            assert(res != bucket.end());
            //assert(res.value() == iter);
            ++i;
            //bucket.Put(&arr[i], sizeof(arr[i]), &arr[i], sizeof(arr[i]));
            //bucket.Print(); printf("\n\n\n\n\n");
        }
    }
    end_time = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    std::cout << "读: " << duration.count() << " ms" << std::endl;
}



// 运行程序: Ctrl + F5 或调试 >“开始执行(不调试)”菜单
// 调试程序: F5 或调试 >“开始调试”菜单

// 入门使用技巧: 
//   1. 使用解决方案资源管理器窗口添加/管理文件
//   2. 使用团队资源管理器窗口连接到源代码管理
//   3. 使用输出窗口查看生成输出和其他消息
//   4. 使用错误列表窗口查看错误
//   5. 转到“项目”>“添加新项”以创建新的代码文件，或转到“项目”>“添加现有项”以将现有代码文件添加到项目
//   6. 将来，若要再次打开此项目，请转到“文件”>“打开”>“项目”并选择 .sln 文件