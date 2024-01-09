#pragma once

#include <optional>

#include "noncopyable.h"
#include "block_info.h"

namespace yudb {

class Noder;
class PageReferencer;

class Blocker : noncopyable {
public:
    Blocker(Noder* noder) : noder_{ noder } {}

    ~Blocker() = default;
    
    Blocker(Blocker&& right) noexcept {
        operator=(std::move(right));
    }

    void operator=(Blocker&& right) noexcept {
        noder_ = right.noder_;
        right.noder_ = nullptr;
    }

    bool BlockNeed(PageSize size);

    std::pair<uint16_t, PageOffset> BlockAlloc(PageSize size, bool alloc_new_page = true);
    
    void BlockFree(const std::tuple<uint16_t, PageOffset, uint16_t>& block);

    std::pair<uint8_t*, PageReferencer> BlockLoad(uint16_t record_index, PageOffset offset);



    void BlockRecordClear();


    void Print();


    void set_noder(Noder* noder) { noder_ = noder; }

private:
    void RecordBuild(BlockRecord* record_element, PageReferencer* page, uint16_t init_block_size);

    void RecordUpdateMaxFreeSize(BlockRecord* record_element, uint8_t* cache);

    uint16_t RecordIndexToArrayIndex(uint16_t record_index);

    void BlockPageAppend();

    void BlockPageDelete();

    void BlockPageCopy(BlockRecord* record_element);

private:
    Noder* noder_;
};

} // namespace yudb