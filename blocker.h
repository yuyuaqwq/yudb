#pragma once

#include <optional>

#include "noncopyable.h"
#include "block_info.h"

namespace yudb {

class Noder;
class PageReferencer;

class Blocker : noncopyable {
public:
    Blocker(Noder* noder, BlockInfo* block_info) : noder_{ noder }, block_info_{ block_info } {}

    ~Blocker() = default;
    
    Blocker(Blocker&& right) noexcept {
        operator=(std::move(right));
    }

    void operator=(Blocker&& right) noexcept {
        noder_ = right.noder_;
        block_info_ = right.block_info_;
        right.noder_ = nullptr;
        right.block_info_ = nullptr;
    }


    void BlockInfoClear();


    std::optional<std::pair<uint16_t, PageOffset>> BlockAlloc(PageSize size, BlockRecord* record_arr = nullptr);
    
    void BlockFree(const std::tuple<uint16_t, PageOffset, uint16_t>& block, BlockRecord* record_element = nullptr);

    std::pair<uint8_t*, PageReferencer> BlockLoad(uint16_t record_index, PageOffset offset);

    PageSize BlockMaxSize();


    void Print();


    void set_noder(Noder* noder) { noder_ = noder;; }

private:
    void BlockRecordBuild(BlockRecord* record_element, PageReferencer* page, uint16_t init_block_size);

    void BlockRecordUpdateMaxFreeSize(BlockRecord* record_element, BlockPage* cache);

    void BlockRecordPageCopy();


    void BlockInfoBuild();


    void BlockPageAppend(PageReferencer* record_page);

    void BlockPageDelete();

    void BlockPageCopy(BlockRecord* record_element);

private:
    Noder* noder_;
    BlockInfo* block_info_;
};

} // namespace yudb