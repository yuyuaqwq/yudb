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
    BlockManager(NodeOperator* node_operator, BlockTable* block_info) : node_operator_{ node_operator }, block_info_{ block_info } {}

    ~BlockManager() = default;

    BlockManager(BlockManager&& right) noexcept {
        operator=(std::move(right));
    }

    void operator=(BlockManager&& right) noexcept {
        node_operator_ = right.node_operator_;
        block_info_ = right.block_info_;
        
        right.node_operator_ = nullptr;
        right.block_info_ = nullptr;
    }


    void Clear();


    PageSize MaxSize();

    std::pair<uint8_t*, PageReference> Load(uint16_t record_index, PageOffset offset);

    std::optional<std::pair<uint16_t, PageOffset>> Alloc(PageSize size);

    void Free(const std::tuple<uint16_t, PageOffset, uint16_t>& block);


    void Print();


    void set_node_operator(NodeOperator* node_operator) { node_operator_ = node_operator; }

private:
    void RecordBuild(BlockTableEntry* record_element, uint16_t record_index, PageReference* page);

    void RecordUpdateMaxFreeSize(BlockTableEntry* record_element, BlockPage* cache);

    void RecordPageCopy();


    void Build();


    BlockPage& PageBuild(PageReference* page_ref);

    void PageAppend(PageReference* record_page);

    void PageDelete();

    void PageCopy(BlockTableEntry* record_element);

protected:
    NodeOperator* node_operator_;
    BlockTable* block_info_;
};

} // namespace yudb