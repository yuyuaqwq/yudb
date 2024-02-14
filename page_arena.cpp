#include "page_arena.h"

#include "pager.h"

namespace yudb {

PageArena::PageArena(Pager* pager, PageArenaFormat* format) : 
    pager_{ pager }, format_{ format } {}

PageArena::~PageArena() = default;

void PageArena::Build() {
    format_->rest_size = pager_->page_size();
    format_->right_size = 0;
}

std::optional<PageOffset> PageArena::AllocLeft(PageSize size) {
    assert(pager_->page_size() == rest_size() + left_size() + right_size());
    if (format_->rest_size < size) {
        return {};
    }
    const auto left_offset = left_size();
    format_->rest_size -= size;
    return left_offset;
}

std::optional<PageOffset> PageArena::AllocRight(PageSize size) {
    assert(pager_->page_size() == rest_size() + left_size() + right_size());
    if (format_->rest_size < size) {
        return {};
    }
    format_->right_size += size;
    const auto right_offset = pager_->page_size() - format_->right_size;
    format_->rest_size -= size;
    return right_offset;
}

void PageArena::FreeLeft(PageSize size) {
    assert(pager_->page_size() == rest_size() + left_size() + right_size());
    format_->rest_size += size;
}

void PageArena::FreeRight(PageSize size) {
    assert(pager_->page_size() == rest_size() + left_size() + right_size());
    format_->rest_size += size;
    format_->right_size -= size;
}


PageOffset PageArena::left_size() {
    return pager_->page_size() - format_->rest_size - format_->right_size;
}
PageOffset PageArena::right_size() {
    return format_->right_size;
}

} // namespace yudb