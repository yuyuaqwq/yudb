// yudbpp.cpp : 此文件包含 "main" 函数。程序执行将在此处开始并结束。
//

#include <iostream>
#include <thread>
#include <map>
#include <chrono>
#include <algorithm>

#include <span>

#include "lru_list.h"
#include "freer.h"

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

void TestBTree(yudb::Db* db) {
    auto tx = db->Begin();

    srand(10);

    auto count = 100000;
    std::vector<int> arr(count);

    //if (count <= 100) {
    //    tx.Print();
    //    printf("\n\n\n\n\n");
    //}

    for (auto i = 0; i < count; i++) {
        arr[i] = i;
    }
    for (auto i = 0; i < count; i++) {
        tx.Put(&arr[i], sizeof(arr[i]), &arr[i], sizeof(arr[i]));
    }
    for (auto i = 0; i < count; i++) {
        auto res = tx.Get(&arr[i], sizeof(arr[i]));
        assert(res != tx.end());
    }
    for (auto i = 0; i < count; i++) {
        auto res = tx.Delete(&arr[i], sizeof(arr[i]));
        assert(res);
    }

    for (auto i = count - 1; i >= 0; i--) {
        tx.Put(&arr[i], sizeof(arr[i]), &arr[i], sizeof(arr[i]));
    }
    for (auto i = count - 1; i >= 0; i--) {
        auto res = tx.Get(&arr[i], sizeof(arr[i]));
        assert(res != tx.end());
    }
    for (auto i = count - 1; i >= 0; i--) {
        auto res = tx.Delete(&arr[i], sizeof(arr[i]));
        assert(res);
    }


    for (auto i = 0; i < count; i++) {
        std::swap(arr[rand() % count], arr[rand() % count]);
    }

    for (auto i = 0; i < count; i++) {
        tx.Put(&arr[i], sizeof(arr[i]), &arr[i], sizeof(arr[i]));
    }
    for (auto i = 0; i < count; i++) {
        auto res = tx.Get(&arr[i], sizeof(arr[i]));
        assert(res != tx.end());
    }
    for (auto i = 0; i < count; i++) {
        auto res = tx.Delete(&arr[i], sizeof(arr[i]));
        assert(res);
    }

    tx.Print();
    printf("\n\n\n\n\n");
    auto k = 0;
    tx.Delete(&k, sizeof(k));
}

void TestOverflower(yudb::Db* db) {
    auto tx = db->Begin();

    tx.Put("hello world!", "Cpp yyds!");
    tx.Put("This is yudb", "value!");
    tx.Put("123123", "123123");
    tx.Put("456456", "456456");
    tx.Put("789789", "789789");
    tx.Put("abcabc", "abcabc");
    auto res = tx.Get("hello world!");

    for (auto& iter : tx) {
        auto key = iter.key();
        auto value = iter.value();
        std::cout << std::string_view{ reinterpret_cast<char*>(key.data()), key.size() } << ":";
        std::cout << std::string_view{ reinterpret_cast<char*>(value.data()), value.size() } << std::endl;
    }
    
}

void TestFreer() {
    //yudb::Pager pager{ nullptr, 4096 };
    //yudb::Freer freer{ &pager };
    //auto test = freer.Alloc(100);
}

int main() {


    auto db = yudb::Db::Open("Z:/test.ydb");
    if (!db) {
        std::cout << "yudb::Db::Open failed!\n";
        return -1;
    }

    TestOverflower(db.get());

    //TestBTree(db.get());

    
    //TestFreer();

    //printf("emm");
    //std::this_thread::sleep_for(std::chrono::seconds(10));
    //TestLru();

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
