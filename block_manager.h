#pragma once

#include <optional>
#include <variant>
#include <span>

#include "noncopyable.h"
#include "block_page.h"

namespace yudb {

class NodeImpl;
class Page;

class BlockManager : noncopyable {
public:
    BlockManager(NodeImpl* node, BlockTableDescriptor* block_info) : node_{ node }, descriptor_{ block_info } {}
    ~BlockManager() = default;

    std::optional<std::pair<uint16_t, PageOffset>> Alloc(PageSize size);
    void Free(const std::tuple<uint16_t, PageOffset, uint16_t>& block);
    std::pair<const uint8_t*, ConstPage> ConstLoad(uint16_t index, PageOffset pos);
    std::pair<uint8_t*, Page> Load(uint16_t index, PageOffset pos);

    void Build();
    void Clear();
    PageSize MaxSize() const;

    void TryDefragmentSpace();
    BlockPage PageRebuildBegin(uint16_t index);
    PageOffset PageRebuildAppend(BlockPage* new_page, const std::tuple<uint16_t, PageOffset, uint16_t>& block);
    void PageRebuildEnd(BlockPage* new_page, uint16_t index);

    void Print();

    auto& node() { return *node_; }
    void set_descriptor(BlockTableDescriptor* descriptor) { descriptor_ = descriptor; }

private:
    void BuildTableEntry(BlockTableEntry* entry, BlockPageImpl* block_page);
    void CopyPageOfBlockTable();

    void AppendPage(BlockPageImpl* page_of_table);
    void DeletePage();

    PageSize MaxFragmentSize();

protected:
    NodeImpl* const node_;
    BlockTableDescriptor* descriptor_;
};

} // namespace yudb