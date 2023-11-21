#pragma once

#include <vector>
#include <optional>
#include <unordered_map>

namespace yudb {

template<typename K, typename V>
class LruList {
private:
    static constexpr int32_t kInvalidIndex = -1;

    struct List {
        struct Node {
            int32_t prev;
            int32_t next;
        };
        int32_t front = kInvalidIndex;
        int32_t tail = kInvalidIndex;
    };

    struct Node {
        union {
            List::Node free_node;
            V value;
        };
        List::Node lru_node;
    };


public:
    LruList(size_t max_count) : pool_(max_count) {
        for (int32_t i = 0; i < max_count; i++) {
            ListPushFront(free_list_, i);
        }
    }

    std::optional<V> Put(const K& key, V&& value) {
        auto i = ListFront(free_list_);

        if (i == kInvalidIndex) {
            //ListPopFront();
        }

        pool_[i].lru_node.next;

        V& value = PoolValue(i);
        std::construct_at<V>(&value);

        ListPushFront(lru_first_, i);
        map_.insert(std::pair{ key, i });

        //list_.push_front(std::pair{ key, value });
        //map_.insert(std::pair{ key, list_.begin() });
        if (map_.size() > max_count_) {
            auto tail = --list_.end();
            V res = tail->second;
            map_.erase(tail->first);
            list_.erase(tail);
            return res;
        }
        else {
            return {};
        }
    }

    //V* Get(const K& key, bool put_front = true) {
    //    auto iter = map_.find(key);
    //    if (iter == map_.end()) {
    //        return nullptr;
    //    }
    //    if (put_front) {
    //        list_.splice(list_.begin(), list_, iter->second);
    //    }
    //    return &iter->second->second;
    //}

    //bool Del(const K& key) {
    //    auto iter = map_.find(key);
    //    if (iter == map_.end()) {
    //        return false;
    //    }
    //    list_.erase(iter->second);
    //    map_.erase(iter);
    //    return true;
    //}

    //V& Front() {
    //    return list_.front().second;
    //}

private:
    List::Node& PoolListNode(int32_t i) {
        return pool_[i].free_node;
    }

    V& PoolValue(int32_t i) {
        return pool_[i].value;
    }


    void ListPushFront(List& list, int32_t i) {
        auto node = PoolListNode(i);
        if (list.front == kInvalidIndex) {
            node.prev = i;
            node.next = i;
        }
        else {
            auto old_first_node = PoolListNode(list.front);
            node.prev = old_first_node.prev;
            node.next = list.front;
            auto last_node = PoolListNode(old_first_node.prev);
            old_first_node.prev = i;
            last_node.next = i;
        }
        list.front = i;
    }

    void ListPopFront(List& list) {
        if (list.front == list.tail) {
            if (list.front == kInvalidIndex) {
                return;
            }
            list.front = kInvalidIndex;
            list.tail = kInvalidIndex;
        }

        auto pop_node = PoolListNode(list.front);
        auto prev = pop_node.prev;
        auto next = pop_node.next;
        auto prev_node = PoolListNode(prev);
        auto next_node = PoolListNode(next);
        next_node.prev = next;
        prev_node.next = prev;

        list.front = next;
    }

    int32_t ListFront(List& list) {
        return list.front;
    }

    int32_t ListTail(List& list) {
        return list.tail;
    }


private:
    List free_list_;
    List lru_list_;

    std::vector<Node> pool_;
    std::unordered_map<K, int32_t> map_;
};

} // namespace yudb