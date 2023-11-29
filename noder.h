#pragma once

#include <cassert>
#include <span>

#include "noncopyable.h"
#include "node.h"
#include "spaner.h"
#include "overflower.h"

namespace yudb {

class BTree;

class Noder : noncopyable {
private:
    class Iterator {
    public:
        using iterator_category = std::random_access_iterator_tag;

        using difference_type = uint16_t;
        using value_type = Span;
        using difference_type = uint16_t;
        using pointer = Span*;
        using reference = Span&;

    public:
        Iterator(Node* node, uint16_t index) : node_{ node }, index_{ index } {}

        reference operator*() const { 
            if (node_->type == Node::Type::kBranch) {
                return node_->branch[index_].key;
            }
            else {
                assert(node_->type == Node::Type::kLeaf);
                return node_->leaf[index_].key;
            }
        }

        Iterator& operator--() noexcept {
            --index_;
            return *this;
        }

        Iterator& operator++() noexcept {
            ++index_;
            return *this;
        }

        Iterator operator+(const difference_type n) const {
            return Iterator{ node_, uint16_t(index_ + n) };
        }

        Iterator operator-(const difference_type n) const {
            return Iterator{ node_, uint16_t(index_ - n) };
        }

        difference_type operator-(const Iterator& right) const noexcept {
            return index_ - right.index_;
        }

        Iterator& operator-=(const difference_type off) noexcept {
            index_ -= off;
            return *this;
        }

        Iterator& operator+=(const difference_type off) noexcept {
            index_ += off;
            return *this;
        }

        uint16_t index() { return index_; }

    private:
        Node* node_;
        uint16_t index_;
    };

public:
    Noder(BTree* btree, uint8_t* page) :
        btree_{ btree },
        pager_{ btree_->pager_ },
        node_{ reinterpret_cast<Node*>(page) },
        overflower_{ this, &node_->overflow } {}


    /*
    * 将数据保存到Node中，但所有权在内存中
    */
    Spaner SpanAlloc(std::span<const uint8_t> data) {
        Spaner spaner{ this, {} };
        auto& span = spaner.span();
        if (data.size() <= sizeof(Span::embed.data)) {
            span.type = Span::Type::kEmbed;
            memcpy(span.embed.data, data.data(), data.size());
            span.embed.size = data.size();
        }
        return spaner;
    }

    void SpanFree(Spaner&& spaner) {
        auto& span = spaner.span();
        if (span.type == Span::Type::kEmbed) {

        }
    }

    /*
    * 将span所有权转移到内存中
    */
    Spaner SpanRelease(Span&& span) {
        return Spaner{ this, std::move(span) };
    }

    /*
    * 将spaner保存到新节点
    */
    Spaner SpanMove(Noder* new_noder, Spaner&& spaner) {
        auto [buf, size] = SpanLoad(spaner.span());
        auto new_spaner =  new_noder->SpanAlloc({ buf, size });
        SpanFree(std::move(spaner));
        return new_spaner;
    }

    std::pair<const uint8_t*, size_t> SpanLoad(const Span& span) {
        if (span.type == Span::Type::kEmbed) {
            return { span.embed.data, span.embed.size };
        }
    }


    void LeafBuild() {
        node_->type = Node::Type::kLeaf;
        node_->element_count = 0;
    }

    void LeafSet(uint16_t pos, Spaner&& key, Spaner&& value) {
        assert(key.noder() == this);
        assert(value.noder() == this);

        auto key_spaner = Spaner{ this, std::move(node_->leaf[pos].key) };
        auto value_spaner = Spaner{ this, std::move(node_->leaf[pos].value) };

        node_->leaf[pos].key = key.Release();
        node_->leaf[pos].value = value.Release();
    }

    void LeafInsert(uint16_t pos, Spaner&& key, Spaner&& value) {
        assert(key.noder() == this);
        assert(value.noder() == this);

        std::memmove(&node_->leaf[pos + 1], &node_->leaf[pos], node_->element_count - pos);
        node_->leaf[pos].key = key.Release();
        node_->leaf[pos].value = value.Release();
        ++node_->element_count;
    }

    void LeafDelete(uint16_t pos) {
        auto key_spaner = Spaner{ this, std::move(node_->leaf[pos].key) };
        auto value_spaner = Spaner{ this, std::move(node_->leaf[pos].value) };

        std::memmove(&node_->leaf[pos], &node_->leaf[pos + 1], node_->element_count - pos - 1);
        --node_->element_count;
    }


    bool IsLeaf() {
        return node_->type == Node::Type::kLeaf;
    }

    bool IsBranch() {
        return node_->type == Node::Type::kBranch;
    }


    Node* node() { return node_; }

    Iterator begin() { return Iterator{ node_, 0 }; }
    Iterator end() { return Iterator{ node_, node_->element_count }; }

private:
    

private:
    friend class Overflower;

    Pager* pager_;
    BTree* btree_;
    Node* node_;

    Overflower overflower_;
};  

} // namespace yudb