#pragma once

#include <optional>
#include <variant>

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


    void InfoClear();


    PageSize MaxSize();

    std::pair<uint8_t*, PageReferencer> Load(uint16_t record_index, PageOffset offset);

    std::optional<std::pair<uint16_t, PageOffset>> Alloc(PageSize size);

    void Free(const std::tuple<uint16_t, PageOffset, uint16_t>& block);


    void Print();


    void set_noder(Noder* noder) { noder_ = noder; }

private:
    void RecordBuild(BlockRecord* record_element, PageReferencer* page);

    void RecordUpdateMaxFreeSize(BlockRecord* record_element, BlockPage* cache);

    void RecordPageCopy();


    void InfoBuild();


    void PageAppend(PageReferencer* record_page);

    void PageDelete();

    void PageCopy(BlockRecord* record_element);

protected:
    Noder* noder_;
    BlockInfo* block_info_;
};

} // namespace yudb