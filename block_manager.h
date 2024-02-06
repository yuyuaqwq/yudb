#pragma once

#include <optional>
#include <variant>

#include "noncopyable.h"
#include "block_page.h"

namespace yudb {

class Node;
class PageReference;

class BlockManager : noncopyable {
public:
    BlockManager(Node* node, BlockTableDescriptor* block_info) : node_{ node }, descriptor_{ block_info } {}
    ~BlockManager() = default;

    BlockManager(BlockManager&& right) noexcept {
        operator=(std::move(right));
    }
    void operator=(BlockManager&& right) noexcept {
        node_ = nullptr;
        descriptor_ = right.descriptor_;
        
        right.node_ = nullptr;
        right.descriptor_ = nullptr;
    }

    Node& node() { return *node_; }
    void set_node(Node* node) { node_ = node; }

    std::pair<uint8_t*, PageReference> Load(uint16_t index, PageOffset pos);
    std::optional<std::pair<uint16_t, PageOffset>> Alloc(PageSize size);
    void Free(const std::tuple<uint16_t, PageOffset, uint16_t>& block);

    void Build();
    void Clear();
    PageSize MaxSize() const;

    void TryDefragmentSpace();
    BlockPage PageRebuildBegin(uint16_t index);
    PageOffset PageRebuildAppend(BlockPage* new_page, const std::tuple<uint16_t, PageOffset, uint16_t>& block);
    void PageRebuildEnd(BlockPage* new_page, uint16_t index);

    void Print();

private:
    void BuildTableEntry(BlockTableEntry* entry, BlockPage* block_page);
    void CopyPageOfBlockTable();

    void AppendPage(BlockPage* page_of_table);
    void DeletePage();

    PageSize MaxFragmentSize();

protected:
    Node* node_;
    BlockTableDescriptor* descriptor_;
};

} // namespace yudb