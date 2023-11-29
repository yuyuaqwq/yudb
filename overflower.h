#pragma once

#include <optional>

#include "noncopyable.h"
#include "overflow.h"

namespace yudb {

class Noder;

class Overflower : noncopyable {
public:
    Overflower(Noder* noder, Overflow* overflow) : noder_{ noder }, overflow_{ overflow } {}

    std::optional<std::pair<PageId, PageOffset>> Alloc(PageSize size, bool alloc_new_page = true);
    
    void Free(const std::pair<PageId, PageOffset>& block) {

    }

private:


private:
    Noder* noder_;
    Overflow* overflow_;
};

} // namespace yudb