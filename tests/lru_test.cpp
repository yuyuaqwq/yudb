#include "gtest/gtest.h"

#include "db/lru_list.h"

namespace yudb {

TEST(LruListTest, HitAndMiss) {
    yudb::LruList<int, int> lru_list{ 3 };

    auto evict = lru_list.push_front(100, 200);
    ASSERT_FALSE(evict);

    auto front = lru_list.front();
    ASSERT_EQ(front, 200);

    evict = lru_list.push_front(200, 400);
    ASSERT_FALSE(evict);

    front = lru_list.front();
    ASSERT_EQ(front, 400);

    //lru_list.Print();

    auto get = lru_list.get(100);
    ASSERT_NE(get.first, nullptr);
    ASSERT_EQ(*(get.first), 200);
   
    front = lru_list.front();
    ASSERT_EQ(front, 200);
    //lru_list.Print();

    evict = lru_list.push_front(400, 123);
    ASSERT_FALSE(evict);

    evict = lru_list.push_front(666, 123);
    ASSERT_TRUE(evict);
    auto [cache_id, k, v] = *evict;
    ASSERT_EQ(k, 200);
    ASSERT_EQ(v, 400);
}

} // namespace yudb