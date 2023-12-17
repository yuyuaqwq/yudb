#pragma once

#include <optional>

#include "overflow.h"

namespace yudb {

class Noder;
class PageReferencer;

class Overflower {
public:
    Overflower(Noder* noder, Overflow* overflow) : noder_{ noder }, overflow_{ overflow } {}

    std::optional<std::pair<uint16_t, PageOffset>> Alloc(PageSize size, bool alloc_new_page = true);
    
    void Free(const std::tuple<uint16_t, PageOffset, uint16_t>& block);

    std::pair<uint8_t*, PageReferencer> Load(uint16_t record_index, PageOffset offset);

private:
    void RecordBuild(Overflow::Record* record, PageReferencer* page, uint16_t init_block_size);

    void OverflowBuild();

    void OverflowAppend(PageReferencer* record_page);

    void OverflowDelete();


private:
    Noder* noder_;
    Overflow* overflow_;
};

} // namespace yudb