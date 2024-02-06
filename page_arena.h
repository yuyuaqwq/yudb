#pragma once

#include <optional>

#include "noncopyable.h"
#include "page_arena_format.h"

namespace yudb {

class Pager;

class PageArena : noncopyable {
public:
    PageArena(Pager* pager, PageArenaFormat* arena_format) : 
        pager_{ pager }, format_{ arena_format } {}

    const auto& rest_size() const { return format_->rest_size; }
    auto& format() { return *format_; }
    void set_arena_format(PageArenaFormat* arena_format) { format_ = arena_format; }

    void Build();
    std::optional<PageOffset> AllocLeft(size_t size);
    std::optional<PageOffset> AllocRight(size_t size);
    void FreeLeft(size_t size);
    void FreeRight(size_t size);

private:
    PageOffset left_size();
    PageOffset right_size();

private:
    Pager* pager_;
    PageArenaFormat* format_;
};

} // namespace yudb