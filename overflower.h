#pragma once

#include <optional>

#include "overflow_info.h"

namespace yudb {

class Noder;
class PageReferencer;

class Overflower {
public:
    Overflower(Noder* noder, OverflowInfo* overflow_info) : noder_{ noder }, overflow_info_{ overflow_info } {}

    ~Overflower() = default;

    std::optional<std::pair<uint16_t, PageOffset>> Alloc(PageSize size, OverflowRecord* record_arr = nullptr);
    
    void Free(const std::tuple<uint16_t, PageOffset, uint16_t>& block, OverflowRecord* temp_record_element = nullptr);

    std::pair<uint8_t*, PageReferencer> Load(uint16_t record_index, PageOffset offset);


private:
    void RecordBuild(OverflowRecord* record_element, PageReferencer* page, uint16_t init_block_size);

    void RecordUpdateMaxFreeSize(OverflowRecord* record_element, uint8_t* cache);

    void RecordCopy();


    void OverflowPageBuild();
    
    void OverflowPageAppend(PageReferencer* record_page);

    void OverflowPageDelete();

    void OverflowPageCopy(OverflowRecord* record_element);

private:
    Noder* noder_;
    OverflowInfo* overflow_info_;
};

} // namespace yudb