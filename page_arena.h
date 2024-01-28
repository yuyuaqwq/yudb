#pragma once

#include <optional>

#include "noncopyable.h"
#include "page_arena_format.h"

namespace yudb {

class Pager;

class PageArena : noncopyable {
public:
    PageArena(Pager* pager, PageArenaFormat* arena_format) : pager_{ pager }, arena_format_{ arena_format } {}


    void Build();

    std::optional<PageOffset> AllocLeft(size_t size);

    std::optional<PageOffset> AllocRight(size_t size);

    void FreeLeft(size_t size);

    void FreeRight(size_t size);

    PageSize rest_size() { return arena_format_->rest_size; }

    void set_arena_format(PageArenaFormat* arena_format) { arena_format_ = arena_format; }

private:
    Pager* pager_;
    PageArenaFormat* arena_format_;
};

} // namespace yudb