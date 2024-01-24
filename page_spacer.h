#pragma once

#include <optional>

#include "page_space.h"

namespace yudb {

class Pager;

class PageSpacer {
public:
    PageSpacer(Pager* pager, PageSpace* space) : pager_{ pager }, space_{ space } {}
    
    void Build();

    std::optional<PageOffset> AllocLeft(size_t size);

    std::optional<PageOffset> AllocRight(size_t size);

    void FreeLeft(size_t size);

    void FreeRight(size_t size);

    PageSize rest_size() { return space_->rest_size; }

private:
    Pager* pager_;
    PageSpace* space_;
};

} // namespace yudb