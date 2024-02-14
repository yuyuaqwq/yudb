#pragma once

#include <optional>

#include "noncopyable.h"
#include "page_arena_format.h"

namespace yudb {

class Pager;

class PageArena : noncopyable {
public:
    PageArena(Pager* pager, PageArenaFormat* format);
    ~PageArena();

    void Build();
    std::optional<PageOffset> AllocLeft(PageSize size);
    std::optional<PageOffset> AllocRight(PageSize size);
    void FreeLeft(PageSize size);
    void FreeRight(PageSize size);

    auto& rest_size() const { return format_->rest_size; }
    auto& format() { return *format_; }
    void set_format(PageArenaFormat* format) { format_ = format; }

private:
    PageOffset left_size();
    PageOffset right_size();

private:
    Pager* const pager_;
    PageArenaFormat* format_;
};

} // namespace yudb