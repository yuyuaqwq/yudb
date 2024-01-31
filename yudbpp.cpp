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

#include "lru_list.h"
#include "log_reader.h"
#include "log_writer.h"

#include "db.h"

void TestLru() {
    yudb::LruList<int, int> aa{ 3 };

    auto evict = aa.Put(100, 200);
    assert(!evict);

    auto f = aa.Front();
    assert(f == 200);

    evict = aa.Put(200, 400);
    assert(!evict);
    f = aa.Front();
    assert(f == 400);

    aa.Print();

    auto get = aa.Get(100);
    assert(get.first != nullptr && *(get.first) == 200);

    f = aa.Front();
    assert(f == 200);

    aa.Print();


    evict = aa.Put(400, 123);
    assert(!evict);

    evict = aa.Put(666, 123);
    assert(evict);
    auto [cache_id, k, v] = *evict;
    assert(k = 400);

}

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

    auto count = 20;
    std::vector<int> arr(count);

    for (auto i = 0; i < count; i++) {
        arr[i] = i; // (rand() << 16) | rand();//
    }

    {
        auto tx = db->Update();
        auto bucket = tx.RootBucket();

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

        /*sub_bucket.Put("abc", "def");
        sub_bucket.Print();  printf("\n\n\n\n\n");*/
        for (auto i = 0; i < count; i++) {
            auto res = bucket.Get(&arr[i], sizeof(arr[i])); assert(res != bucket.end());
            //bucket.Put(&arr[i], sizeof(arr[i]), &arr[i], sizeof(arr[i]));
            //bucket.Print(); printf("\n\n\n\n\n");
        }
        bucket.Print(); printf("\n\n\n\n\n");
        int old_key = -1;
        for (auto& iter : bucket) {
            //std::cout << "is_bucket:" << iter.key<int>() << " " << iter.value<int>() << "" << iter.is_bucket() << std::endl;
            assert(iter.key<int>() > old_key);
        }

        tx.Commit();
    }



    //{
    //    auto view_tx = db->View();
    //    auto view_bucket = view_tx.RootBucket();
    //    //view_bucket.Print(); printf("\n\n\n\n\n");
    //    //if (count <= 100000) {
    //    //    bucket.Print();
    //    //    printf("\n\n\n\n\n");
    //    //}

    //    //for (auto& iter : view_bucket) {
    //    //    //std::cout << "is_bucket:" << iter.key<int>() << " " << iter.value<int>() << "" << iter.is_bucket() << std::endl;
    //    //    //assert(iter.key<int>() > old_key);
    //    //}
    //    //auto sub_bucket = view_bucket.SubViewBucket("abcd");

    //    //auto iter = sub_bucket.Get("abc");
    //    //auto value = iter.value();

    //    for (auto i = 0; i < count; i++) {
    //        auto res = view_bucket.Get(&arr[i], sizeof(arr[i]));
    //        assert(res != view_bucket.end());
    //    }

    //}

    //{
    //    auto tx = db->Update();
    //    auto bucket = tx.RootBucket();

    //    for (auto i = 0; i < count; i++) {
    //        auto res = bucket.Delete(&arr[i], sizeof(arr[i]));
    //        bucket.Print(); printf("\n\n\n\n\n");
    //        assert(res);
    //    }

    //    bucket.Print(); printf("\n\n\n\n\n");
    //    tx.Commit();
    //}
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

std::string RandomString(size_t min_size, size_t max_size) {
    int size;
    if (min_size == max_size) {
        size = min_size;
    }
    else {
        size = (rand() % (max_size - min_size)) + min_size;
    }
    std::string str(size , ' ');
    for (auto i = 0; i < size; i++) {
        str[i] = rand() % 26 + 'a';
    }
    return str;
}

void TestBlock(yudb::DB* db) {
    auto tx = db->Update();
    auto bucket = tx.RootBucket();

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

    auto count = 10000;
    std::vector<std::string> arr(count);
    arr.begin();

    for (auto i = 0; i < count; i++) {
        arr[i] = RandomString(8, 16);
    }
    auto i = 0;
    for(auto& iter : arr) {
        //printf("%d\n", i);
        bucket.Put(iter, iter);
        //bucket.Print(true); printf("\n\n\n\n");
        ++i;
    }
    
}

void TestFreer() {
    //yudb::Pager pager{ nullptr, 4096 };
    //yudb::Freer freer{ &pager };
    //auto test = freer.Alloc(100);
}

void TestLog() {
    //yudb::File file;
    //file.Open("Z:/test.ydb-wal", true);
    yudb::log::Writer writer;
    writer.Open("Z:/test.ydb-wal");
    writer.AppendRecord("abc");
    writer.AppendRecord("aedaed");
    writer.AppendRecord("awdwaaa");
    writer.AppendRecord("123213123321");
    writer.AppendRecord(std::string(100000, 'a'));

    /*yudb::log::Reader reader{ &file };
    auto res = reader.ReadRecord();
    res = reader.ReadRecord();
    res = reader.ReadRecord();
    res = reader.ReadRecord();
    res = reader.ReadRecord();
    assert(res->size() == 100000);
    assert(std::string(100000, 'a') == *res);*/
}

int main() {
    TestLru();
    //TestLog();


    auto db = yudb::DB::Open("Z:/test.ydb");
    if (!db) {
        std::cout << "yudb::Db::Open failed!\n";
        return -1;
    }


    //TestPager(db.get());

    //TestBlock(db.get());

    TestBTree(db.get());

    
    //TestFreer();

    //printf("emm");
    //std::this_thread::sleep_for(std::chrono::seconds(10));
    //

    //std::cout << "Hello World!\n";
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
