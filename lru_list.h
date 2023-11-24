#pragma once

#include <cassert>

#include <vector>
#include <optional>
#include <unordered_map>
#include <functional>

#include "cache.h"

namespace yudb {

template<typename K, typename V>
class LruList {
private:
    struct List {
        struct Node {
            CacheId prev;
            CacheId next;
        };

        CacheId front{ kCacheInvalidId };
        CacheId tail{ kCacheInvalidId };
    };

    using GetListNodeFunc = std::function<typename List::Node&(CacheId)>; // List::Node& (CacheId)

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
        for (CacheId i = 0; i < max_count; i++) {
            auto& node = PoolNode(i);
            std::construct_at<V>(&node.value);
            ListPushFront(free_list_, free_list_get_node_, i);
        }
    }

    std::optional<V> Put(const K& key, V&& value) {
        auto free = ListFront(free_list_);
        std::optional<V> ret{};
        if (free == kCacheInvalidId) {
            auto tail = ListTail(lru_list_);
            assert(tail != kCacheInvalidId);
            auto& tail_node = PoolNode(tail);
            ret = tail_node.value;
            free = tail;
            map_.erase(tail_node.key);
            ListPopTail(lru_list_, lru_list_get_node_);
        }
        else {
            ListPopFront(free_list_, free_list_get_node_);
        }

        auto& node = PoolNode(free);
        node.key = key;
        node.value = std::move(value);

        ListPushFront(lru_list_, lru_list_get_node_, free);
        map_.insert(std::pair{ key, free });
        return ret;
    }

    std::pair<V*, CacheId> Get(const K& key, bool put_front = true) {
        auto iter = map_.find(key);
        if (iter == map_.end()) {
            return { nullptr, kCacheInvalidId };
        }
        if (put_front) {
            ListDelete(lru_list_, lru_list_get_node_, iter->second);
            ListPushFront(lru_list_, lru_list_get_node_, iter->second);
        }
        return { &PoolNode(iter->second).value, iter->second };
    }

    bool Del(const K& key) {
        auto iter = map_.find(key);
        if (iter == map_.end()) {
            return false;
        }
        ListDelete(lru_list_, lru_list_get_node_, iter->second);
        ListPushFront(free_list_, free_list_get_node_, iter->second);
        map_.erase(iter);
        return true;
    }

    V& Front() {
        return PoolNode(ListFront(lru_list_)).value;
    }

    V* GetByCacheId(CacheId cache_id) {
        return &PoolNode(cache_id).value;
    }

private:
    Node& PoolNode(CacheId i) {
        return pool_[i];
    }

    void ListPushFront(List& list, GetListNodeFunc get_node, CacheId i) {
        auto& node = get_node(i);
        if (list.front == kCacheInvalidId) {
            node.prev = i;
            node.next = i;
            list.tail = i;
        }
        else {
            auto& old_front_node = get_node(list.front);
            node.prev = old_front_node.prev;
            node.next = list.front;
            auto& tail_node = get_node(old_front_node.prev);
            old_front_node.prev = i;
            tail_node.next = i;
        }
        list.front = i;
    }

    void ListDelete(List& list, GetListNodeFunc get_node, CacheId i) {
        if (list.front == list.tail) {
            if (list.front == kCacheInvalidId) {
                return;
            }
            list.front = kCacheInvalidId;
            list.tail = kCacheInvalidId;
            return;
        }

        auto& pop_node = get_node(i);
        auto prev = pop_node.prev;
        auto next = pop_node.next;
        auto& prev_node = get_node(prev);
        auto& next_node = get_node(next);
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

    CacheId ListFront(List& list) {
        return list.front;
    }

    CacheId ListTail(List& list) {
        return list.tail;
    }


private:
    List free_list_;
    GetListNodeFunc free_list_get_node_ = [this](CacheId i) -> List::Node& {
        return PoolNode(i).free_node;
    };
    List lru_list_;
    GetListNodeFunc lru_list_get_node_ = [this](CacheId i) -> List::Node& {
        return PoolNode(i).lru_node;
    };

    std::vector<Node> pool_;
    std::unordered_map<K, CacheId> map_;
};

} // namespace yudb