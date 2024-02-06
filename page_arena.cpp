#include "page_arena.h"

#include "pager.h"

namespace yudb {

void PageArena::Build() {
    format_->rest_size = pager_->page_size();
    format_->right_size = 0;
}

std::optional<PageOffset> PageArena::AllocLeft(size_t size) {
    assert(pager_->page_size() == rest_size() + left_size() + right_size());
    if (format_->rest_size < size) {
        return {};
    }
    auto left_offset = left_size();
    format_->rest_size -= size;
    return left_offset;
}

std::optional<PageOffset> PageArena::AllocRight(size_t size) {
    assert(pager_->page_size() == rest_size() + left_size() + right_size());
    if (format_->rest_size < size) {
        return {};
    }
    format_->right_size += size;
    auto right_offset = pager_->page_size() - format_->right_size;
    format_->rest_size -= size;
    return right_offset;
}

void PageArena::FreeLeft(size_t size) {
    assert(pager_->page_size() == rest_size() + left_size() + right_size());
    format_->rest_size += size;
}

void PageArena::FreeRight(size_t size) {
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