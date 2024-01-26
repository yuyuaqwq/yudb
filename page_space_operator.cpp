#include "page_space_operator.h"

#include "pager.h"

namespace yudb {

void PageSpaceOperator::Build() {
    space_->rest_size = pager_->page_size();
    space_->right_size = 0;
}

std::optional<PageOffset> PageSpaceOperator::AllocLeft(size_t size) {
    if (space_->rest_size < size) {
        return {};
    }
    auto left_size = pager_->page_size() - space_->rest_size - space_->right_size;
    space_->rest_size -= size;
    return left_size;
}

std::optional<PageOffset> PageSpaceOperator::AllocRight(size_t size) {
    if (space_->rest_size < size) {
        return {};
    }
    space_->right_size += size;
    auto right_offset = pager_->page_size() - space_->right_size;
    space_->rest_size -= size;
    return right_offset;
}

void PageSpaceOperator::FreeLeft(size_t size) {
    space_->rest_size += size;
}

void PageSpaceOperator::FreeRight(size_t size) {
    space_->rest_size += size;
    space_->right_size -= size;
}

} // namespace yudb