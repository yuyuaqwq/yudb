#pragma once

#include <cassert>

#include <vector>
#include <optional>
#include <unordered_map>
#include <functional>

#include "db/cache.h"
#include "third_party/unordered_dense.h"
#include "util/noncopyable.h"

namespace yudb {

namespace internal {
template <typename T, typename Member>
size_t offset_of(const Member T::*member) {
    return reinterpret_cast<size_t>(&(((T*)nullptr)->*member));
}
}

struct ListNode {
    CacheId prev;
    CacheId next;
};

template<typename Pool>
class List : noncopyable {
public:
    List(Pool* pool, size_t offset) :
        pool_{ pool }, offset_{ offset } {}
    ~List() = default;

    void push_front(CacheId i) {
        auto& node = GetNode(i);
        if (front_ == kCacheInvalidId) {
            node.prev = kCacheInvalidId;
            node.next = kCacheInvalidId;
            back_ = i;
        } else {
            auto& old_front_node = GetNode(front_);
            node.prev = kCacheInvalidId;
            node.next = front_;
            old_front_node.prev = i;
        }
        front_ = i;
    }
    void erase(CacheId i) {
        assert(i != kCacheInvalidId);
        if (front_ == i && back_ == i) {
            front_ = kCacheInvalidId;
            back_ = kCacheInvalidId;
            return;
        }
        auto& del_node = GetNode(i);
        auto prev = del_node.prev;
        auto next = del_node.next;

        if (prev != kCacheInvalidId) {
            auto& prev_node = GetNode(prev);
            prev_node.next = next;
        } else {
            front_ = next;
        }

        if (next != kCacheInvalidId) {
            auto& next_node = GetNode(next);
            next_node.prev = prev;
        } else {
            back_ = prev;
        }
    }
    void pop_front() {
        erase(front_);
    }
    void pop_back() {
        erase(back_);
    }
    CacheId front() {
        return front_;
    }
    CacheId back() {
        return back_;
    }

private:
    ListNode& GetNode(CacheId i) {
        return *reinterpret_cast<ListNode*>(reinterpret_cast<uint8_t*>(&pool_[i]) + offset_);
    }

private:
    Pool* pool_;
    size_t offset_;
    CacheId front_{ kCacheInvalidId };
    CacheId back_{ kCacheInvalidId };
};

template<typename LruList, typename K, typename V>
class LruListIterator {
public:
    using iterator_category = std::bidirectional_iterator_tag;

    using value_type = LruListIterator;
    using difference_type = typename std::ptrdiff_t;
    using pointer = LruListIterator*;
    using reference = const value_type&;

    LruListIterator(LruList* list, CacheId id) : 
        list_{ list }, id_{ id } {}

    reference operator*() const noexcept {
        return *this;
    }

    pointer operator->() const noexcept {
        return this;
    }

    LruListIterator& operator++() noexcept {
        auto node = list_->PoolNode(id_).lru_node;
        id_ = node.next;
        return *this;
    }

    LruListIterator operator++(int) noexcept {
        auto node = list_->PoolNode(id_).lru_node;
        LruListIterator tmp = *this;
        id_ = node.next;
        return tmp;
    }

    LruListIterator& operator--() noexcept {
        auto node = list_->PoolNode(id_).lru_node;
        id_ = node.prev;
        return *this;
    }

    LruListIterator operator--(int) noexcept {
        auto node = list_->PoolNode(id_).lru_node;
        LruListIterator tmp = *this;
        id_ = node.prev;
        return tmp;
    }

    bool operator==(const LruListIterator& right) const noexcept {
        return id_ == right.id_;
    }

    const K& key() const {
        return list_->PoolNode(id_).key;
    }

    const V& value() const {
        return list_->PoolNode(id_).value;
    }

private:
    LruList* list_;
    CacheId id_;
};

template<typename K, typename V>
class LruList : noncopyable {
private:
    struct Node {
        K key;
        union {
            ListNode free_node;
            V value;
        };
        ListNode lru_node;
    };
    using List = List<Node>;
    using Iterator = LruListIterator<LruList<K, V>, K, V>;
public:

    LruList(size_t max_count) : 
        pool_(max_count), 
        free_list_{ pool_.data(), internal::offset_of(&Node::free_node)},
        lru_list_{ pool_.data(), internal::offset_of(&Node::lru_node)}
    {
        for (CacheId i = max_count - 1; i > 0; i--) {
            auto& node = PoolNode(i);
            std::construct_at<V>(&node.value);
            free_list_.push_front(i);
        }
        free_list_.push_front(0);
    }
    ~LruList() = default;

    /*
    * 返回被淘汰的对象
    */
    void set_front(CacheId cache_id) {
        if (lru_list_.front() == cache_id) return;
        lru_list_.erase(cache_id);
        lru_list_.push_front(cache_id);
    }
    std::optional<std::tuple<CacheId, K, V>> push_front(const K& key, V&& value) {
        auto free = free_list_.front();
        std::optional<std::tuple<CacheId, K, V>> evict{};
        if (free == kCacheInvalidId) {
            auto back = lru_list_.back();
            assert(back != kCacheInvalidId);
            auto& back_node = PoolNode(back);
            auto iter = map_.find(back_node.key);
            evict = { iter->second, back_node.key, back_node.value };
            free = back;
            map_.erase(iter);
            lru_list_.pop_back();
        } else {
            free_list_.pop_front();
        }
        auto& node = PoolNode(free);
        node.key = key;
        node.value = std::move(value);

        lru_list_.push_front(free);
        map_.insert(std::pair{ key, free });
        return evict;
    }
    std::pair<V*, CacheId> get(const K& key, bool put_front = true) {
        auto iter = map_.find(key);
        if (iter == map_.end()) {
            return { nullptr, kCacheInvalidId };
        }
        if (put_front) {
            set_front(iter->second);
        }
        return { &PoolNode(iter->second).value, iter->second };
    }
    bool erase(const K& key) {
        auto iter = map_.find(key);
        if (iter == map_.end()) {
            return false;
        }
        lru_list_.erase(iter->second);
        free_list_.push_front(iter->second);
        map_.erase(iter);
        return true;
    }
    V& front() {
        return PoolNode(lru_list_.front()).value;
    }

    Iterator begin() noexcept {
        return Iterator{ this, lru_list_.front() };
    }
    Iterator end() noexcept {
        return Iterator{ this, kCacheInvalidId };
    }

    const Node& GetNodeByCacheId(CacheId cache_id) const { return PoolNode(cache_id); }
    Node& GetNodeByCacheId(CacheId cache_id) { return PoolNode(cache_id); }

    void Print() {
        std::cout << "LruList:";
        for (auto& iter : *this) {
            std::cout << iter.key() << " ";
        }
        std::cout << std::endl;
    }

private:
    Node& PoolNode(CacheId i) {
        return pool_[i];
    }
    const Node& PoolNode(CacheId i) const {
        return pool_[i];
    }

    template <typename T, typename Member>
    size_t OffsetOf(const Member T::* member) {
        return reinterpret_cast<size_t>(&(((T*)nullptr)->*member));
    }

private:
    friend Iterator;

    std::vector<Node> pool_;

    List free_list_;
    List lru_list_;

    //std::unordered_map<K, CacheId> map_;
    ankerl::unordered_dense::map<K, CacheId> map_;
};

} // namespace yudb