#pragma once

#include <optional>
#include <variant>

#include "noncopyable.h"
#include "block.h"

namespace yudb {

class NodeOperator;
class PageReference;

class BlockManager : noncopyable {
public:
    BlockManager(NodeOperator* node_operator, BlockTableDescriptor* block_info) : node_operator_{ node_operator }, block_table_descriptor_{ block_info } {}

    ~BlockManager() = default;

    BlockManager(BlockManager&& right) noexcept {
        operator=(std::move(right));
    }

    void operator=(BlockManager&& right) noexcept {
        node_operator_ = right.node_operator_;
        block_table_descriptor_ = right.block_table_descriptor_;
        
        right.node_operator_ = nullptr;
        right.block_table_descriptor_ = nullptr;
    }


    void Clear();


    PageSize MaxSize();

    std::pair<uint8_t*, PageReference> Load(uint16_t index, PageOffset pos);

    std::optional<std::pair<uint16_t, PageOffset>> Alloc(PageSize size);

    void Free(const std::tuple<uint16_t, PageOffset, uint16_t>& block);


    void Print();


    void set_node_operator(NodeOperator* node_operator) { node_operator_ = node_operator; }

private:
    void TableEntryBuild(BlockTableEntry* entry, uint16_t index, PageReference* page);

    void TableEntryUpdateMaxFreeSize(BlockTableEntry* entry, BlockPage* block_page);

    void TablePageCopy();


    void Build();


    BlockPage& PageBuild(PageReference* page_ref);

    void PageAppend(PageReference* page_ref);

    void PageDelete();

    void PageCopy(BlockTableEntry* entry);

protected:
    NodeOperator* node_operator_;
    BlockTableDescriptor* block_table_descriptor_;
};

} // namespace yudb