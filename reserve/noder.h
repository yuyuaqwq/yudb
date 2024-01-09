#pragma once

#include <cassert>

#include <span>
#include <stdexcept>
#include <optional>
#include <variant>
#include <vector>

#include "noncopyable.h"
#include "node.h"
#include "blocker.h"
#include "page_referencer.h"

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
                return node_->body.branch[index_].key;
            }
            else {
                assert(node_->type == Node::Type::kLeaf);
                return node_->body.leaf[index_].key;
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
    Noder(const BTree* btree, PageId page_id);

    Noder(const BTree* btree, PageReferencer page_ref);


    Noder(Noder&& right) noexcept : 
        page_ref_{ std::move(right.page_ref_) },
        btree_{ right.btree_ },
        node_{ right.node_ },
        blocker_{ std::move(right.blocker_) }
    {
        blocker_.set_noder(this);
    }

    void operator=(Noder&& right) noexcept {
        page_ref_ = std::move(right.page_ref_);
        btree_ = right.btree_;
        node_ = right.node_;
        blocker_ = std::move(right.blocker_);
        blocker_.set_noder(this);
    }


    Noder Copy() const;


    /*
    * 将数据保存到Node中
    */
    Span SpanAlloc(std::span<const uint8_t> data);

    void SpanFree(Span&& span) {
        if (span.type == Span::Type::kInvalid) { }
        else if (span.type == Span::Type::kEmbed) { }
        else if (span.type == Span::Type::kBlock) {
            blocker_.BlockFree({ span.block.record_index, span.block.offset, span.block.size });
            //printf("free\n"); blocker_.Print(); printf("\n");
        }
        else {
            throw std::runtime_error("unrealized types.");
        }
        span.type = Span::Type::kInvalid;
    }

    /*
    * 将span移动到新节点，设计上假定不会分配失败
    */
    Span SpanMove(Noder* new_noder, Span&& span) {
        auto [buf, size, ref] = SpanLoad(span);
        auto new_span =  new_noder->SpanAlloc({ buf, size });
        SpanFree(std::move(span));
        return new_span;
    }

    /*
    * 将span拷贝到新节点，设计上假定不会分配失败
    */
    Span SpanCopy(Noder* new_noder, const Span& span) {
        auto [buf, size, ref] = SpanLoad(span);
        auto new_span = new_noder->SpanAlloc({ buf, size });
        return new_span;
    }

    std::tuple<const uint8_t*, size_t, std::optional<std::variant<PageReferencer, std::vector<uint8_t>>>>
    SpanLoad(const Span& span) {
        if (span.type == Span::Type::kEmbed) {
            return { span.embed.data, span.embed.size, std::nullopt };
        }
        else if (span.type == Span::Type::kBlock) {
            auto [buf, page] = blocker_.BlockLoad(span.block.record_index, span.block.offset);
            return { buf, span.block.size, std::move(page) };
        }
        else {
            throw std::runtime_error("unrealized types.");
        }
    }

    void SpanClear();

    bool SpanNeed(size_t size);

    size_t SpanSize(const Span& span);


    void FreeSizeInit();


    void BlockPrint() {
        blocker_.Print();
    }


    void LeafBuild() {
        node_->type = Node::Type::kLeaf;
        node_->element_count = 0;
        node_->block_record_count = 0;
        FreeSizeInit();
    }

    bool LeafAlloc(uint16_t count) {
        if (node_->free_size < count * sizeof(Node::LeafElement)) {
            return false;
        }
        node_->free_size -= count * sizeof(Node::LeafElement);
        node_->element_count += count;
        return true;
    }

    void LeafFree(uint16_t count) {
        node_->free_size += count * sizeof(Node::LeafElement);
        node_->element_count -= count;
    }

    // 需要先通过LeafAlloc分配，对原span视作未初始化资源
    void LeafSet(uint16_t pos, Span&& key, Span&& value) {
        assert(pos < node_->element_count);
        node_->body.leaf[pos].key = std::move(key);
        node_->body.leaf[pos].value = std::move(value);
    }
    
    // 需要先通过LeafAlloc分配，对原span视作未初始化资源
    void LeafInsert(uint16_t pos, Span&& key, Span&& value) {
        assert(pos < node_->element_count);
        auto old_element_count = node_->element_count - 1;
        std::memmove(&node_->body.leaf[pos + 1], &node_->body.leaf[pos], (old_element_count - pos) * sizeof(node_->body.leaf[pos]));
        node_->body.leaf[pos].key = std::move(key);
        node_->body.leaf[pos].value = std::move(value);
    }

    void LeafDelete(uint16_t pos) {
        assert(pos < node_->element_count);
        SpanFree(std::move(node_->body.leaf[pos].key));
        SpanFree(std::move(node_->body.leaf[pos].value));
        std::memmove(&node_->body.leaf[pos], &node_->body.leaf[pos + 1], (node_->element_count - pos - 1) * sizeof(node_->body.leaf[pos]));
        LeafFree(1);
    }


    void BranchBuild() {
        node_->type = Node::Type::kBranch;
        node_->element_count = 0;
        node_->block_record_count = 0;
        FreeSizeInit();
    }

    bool BranchAlloc(uint16_t count) {
        if (node_->free_size < sizeof(BlockRecord) + count) {
            return false;
        }
        node_->free_size -= count * sizeof(Node::BranchElement);
        node_->element_count += count;
        return true;
    }

    void BranchFree(uint16_t count) {
        node_->free_size += count * sizeof(Node::BranchElement);
        node_->element_count -= count;
    }

    /*
    * 需要先通过BranchAlloc分配，对原span视作未初始化资源
    * 不处理tail_child的关系，需要自己保证tail_child的正确性
    */
    void BranchSet(uint16_t pos, Span&& key, PageId left_child) {
        assert(pos < node_->element_count);
        node_->body.branch[pos].key = std::move(key);
        node_->body.branch[pos].left_child = left_child;
    }

    PageId BranchGetLeftChild(uint16_t pos) {
        assert(pos <= node_->element_count);
        if (pos == node_->element_count) {
            return node_->body.tail_child;
        }
        return node_->body.branch[pos].left_child;
    }

    PageId BranchGetRightChild(uint16_t pos) {
        assert(pos < node_->element_count);
        if (pos == node_->element_count - 1) {
            return node_->body.tail_child;
        }
        return node_->body.branch[pos + 1].left_child;
    }

    void BranchSetLeftChild(uint16_t pos, PageId left_child) {
        assert(pos <= node_->element_count);
        if (pos == node_->element_count) {
            node_->body.tail_child = left_child;
            return;
        }
        node_->body.branch[pos].left_child = left_child;
    }

    void BranchSetRightChild(uint16_t pos, PageId right_child) {
        assert(pos < node_->element_count);
        if (pos == node_->element_count - 1) {
            node_->body.tail_child = right_child;
            return;
        }
        node_->body.branch[pos + 1].left_child = right_child;
    }

    // 需要先通过BranchAlloc分配
    void BranchInsert(uint16_t pos, Span&& key, PageId child, bool right_child) {
        assert(pos < node_->element_count);
        auto old_element_count = node_->element_count - 1;
        if (right_child) {
            if (pos == old_element_count) {
                node_->body.branch[pos].left_child = node_->body.tail_child;
                node_->body.tail_child = child;
            }
            else {
                std::memmove(&node_->body.branch[pos + 1], &node_->body.branch[pos], (old_element_count - pos) * sizeof(node_->body.branch[pos]));
                node_->body.branch[pos].left_child = node_->body.branch[pos + 1].left_child;
                node_->body.branch[pos + 1].left_child = child;
            }
        }
        else {
            std::memmove(&node_->body.branch[pos + 1], &node_->body.branch[pos], (old_element_count - pos) * sizeof(node_->body.branch[pos]));
            node_->body.branch[pos].left_child = child;
        }
        node_->body.branch[pos].key = std::move(key);
    }

    void BranchDelete(uint16_t pos, bool right_child) {
        assert(pos < node_->element_count);
        auto copy_count = node_->element_count - pos - 1;
        if (right_child) {
            if (pos + 1 < node_->element_count) {
                node_->body.branch[pos].key = std::move(node_->body.branch[pos + 1].key);
            }
            else {
                node_->body.tail_child = node_->body.branch[node_->element_count - 1].left_child;
            }
            if (copy_count > 0) {
                SpanFree(std::move(node_->body.branch[pos + 1].key));
                std::memmove(&node_->body.branch[pos + 1], &node_->body.branch[pos + 2], (copy_count - 1) * sizeof(node_->body.branch[pos]));
            }
        }
        else {
            SpanFree(std::move(node_->body.branch[pos].key));
            std::memmove(&node_->body.branch[pos], &node_->body.branch[pos + 1], copy_count * sizeof(node_->body.branch[pos]));
        }
        BranchFree(1);
    }


    bool IsLeaf() const {
        return node_->type == Node::Type::kLeaf;
    }

    bool IsBranch() const {
        return node_->type == Node::Type::kBranch;
    }


    BlockRecord* BlockRecordArray();

    void BlockRecordAlloc();


    const BTree& btree() const { return *btree_; }

    Node& node() { return *node_; }

    PageId page_id() const { return page_ref_.page_id(); }

    uint8_t* page_cache() const { return page_ref_.page_cache(); }

    Iterator begin() { return Iterator{ node_, 0 }; }

    Iterator end() { return Iterator{ node_, node_->element_count }; }

private:
    const BTree* btree_;

    PageReferencer page_ref_;
    Node* node_;

    Blocker blocker_;
};  

} // namespace yudb