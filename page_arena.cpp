#include "page_arena.h"

#include "pager.h"

namespace yudb {

void PageArena::Build() {
    arena_format_->rest_size = pager_->page_size();
    arena_format_->right_size = 0;
}

std::optional<PageOffset> PageArena::AllocLeft(size_t size) {
    if (arena_format_->rest_size < size) {
        return {};
    }
    auto left_size = pager_->page_size() - arena_format_->rest_size - arena_format_->right_size;
    arena_format_->rest_size -= size;
    return left_size;
}

std::optional<PageOffset> PageArena::AllocRight(size_t size) {
    if (arena_format_->rest_size < size) {
        return {};
    }
    arena_format_->right_size += size;
    auto right_offset = pager_->page_size() - arena_format_->right_size;
    arena_format_->rest_size -= size;
    return right_offset;
}

void PageArena::FreeLeft(size_t size) {
    arena_format_->rest_size += size;
}

void PageArena::FreeRight(size_t size) {
    arena_format_->rest_size += size;
    arena_format_->right_size -= size;
}

} // namespace yudb