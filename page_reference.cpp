#include "page_reference.h"

#include "pager.h"

namespace yudb {

PageId PageReference::id() const {
    return pager_->CacheToPageId(page_buff_);
}

PageReference::~PageReference() {
    Dereference();
}

void PageReference::Dereference() {
    if (page_buff_) {
        pager_->Dereference(page_buff_);
        page_buff_ = nullptr;
    }
}

} // namespace yudb