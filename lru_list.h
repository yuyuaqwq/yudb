#pragma once

#include <cassert>

#include <vector>
#include <optional>
#include <unordered_map>
#include <functional>

namespace yudb {

template<typename K, typename V>
class LruList {
private:
    static constexpr uint32_t kInvalidIndex = 0xffffffff;

    struct List {
        struct Node {
            uint32_t prev;
            uint32_t next;
        };

        uint32_t front{ kInvalidIndex };
        uint32_t tail{ kInvalidIndex };
    };

    using GetListNodeFunc = List::Node&(LruList::*)(uint32_t);

    struct Node {
        K key;
        union {
            List::Node free_node;
            V value;
        };
        List::Node lru_node;
    };


public:
    LruList(size_t max_count) : pool_(max_count) {
        for (uint32_t i = 0; i < max_count; i++) {
            auto& node = PoolNode(i);
            std::construct_at<V>(&node.value);
            ListPushFront(free_list_, &LruList::FreeListGetNode, i);
        }
    }

    std::optional<V> Put(const K& key, V&& value) {
        auto free = ListFront(free_list_);
        std::optional<V> ret{};
        if (free == kInvalidIndex) {
            auto tail = ListTail(lru_list_);
            assert(tail != kInvalidIndex);
            auto& tail_node = PoolNode(tail);
            ret = tail_node.value;
            free = tail;
            map_.erase(tail_node.key);
            ListPopTail(lru_list_, &LruList::LruListGetNode);
        }
        else {
            ListPopFront(free_list_, &LruList::FreeListGetNode);
        }

        auto& node = PoolNode(free);
        node.key = key;
        node.value = std::move(value);

        ListPushFront(lru_list_, &LruList::LruListGetNode, free);
        map_.insert(std::pair{ key, free });
        return ret;
    }

    V* Get(const K& key, bool put_front = true) {
        auto iter = map_.find(key);
        if (iter == map_.end()) {
            return nullptr;
        }
        if (put_front) {
            ListDelete(lru_list_, &LruList::LruListGetNode, iter->second);
            ListPushFront(lru_list_, &LruList::LruListGetNode, iter->second);
        }
        return &PoolNode(iter->second).value;
    }

    bool Del(const K& key) {
        auto iter = map_.find(key);
        if (iter == map_.end()) {
            return false;
        }
        ListDelete(lru_list_, &LruList::LruListGetNode, iter->second);
        ListPushFront(free_list_, &LruList::FreeListGetNode, iter->second);
        map_.erase(iter);
        return true;
    }

    V& Front() {
        return PoolNode(ListFront(lru_list_)).value;
    }

private:
    Node& PoolNode(uint32_t i) {
        return pool_[i];
    }

    List::Node& LruListGetNode (uint32_t i) {
        return PoolNode(i).lru_node;
    }

    List::Node& FreeListGetNode(uint32_t i) {
        return PoolNode(i).free_node;
    }

    void ListPushFront(List& list, GetListNodeFunc get_node, uint32_t i) {
        auto& node = (this->*get_node)(i);
        if (list.front == kInvalidIndex) {
            node.prev = i;
            node.next = i;
            list.tail = i;
        }
        else {
            auto& old_front_node = (this->*get_node)(list.front);
            node.prev = old_front_node.prev;
            node.next = list.front;
            auto& tail_node = (this->*get_node)(old_front_node.prev);
            old_front_node.prev = i;
            tail_node.next = i;
        }
        list.front = i;
    }

    void ListDelete(List& list, GetListNodeFunc get_node, uint32_t i) {
        if (list.front == list.tail) {
            if (list.front == kInvalidIndex) {
                return;
            }
            list.front = kInvalidIndex;
            list.tail = kInvalidIndex;
            return;
        }

        auto& pop_node = (this->*get_node)(i);
        auto prev = pop_node.prev;
        auto next = pop_node.next;
        auto& prev_node = (this->*get_node)(prev);
        auto& next_node = (this->*get_node)(next);
        next_node.prev = next;
        prev_node.next = prev;

        if (i == list.front) {
            list.front = next;
        }
        else if (i == list.tail) {
            list.tail = prev;
        }
    }

    void ListPopFront(List& list, GetListNodeFunc get_node) {
        ListDelete(list, get_node, list.front);
    }

    void ListPopTail(List& list, GetListNodeFunc get_node) {
        ListDelete(list, get_node, list.tail);
    }

    uint32_t ListFront(List& list) {
        return list.front;
    }

    uint32_t ListTail(List& list) {
        return list.tail;
    }


private:
    List free_list_;
    List lru_list_;


    std::vector<Node> pool_;
    std::unordered_map<K, uint32_t> map_;
};

} // namespace yudb