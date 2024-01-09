#include "page_referencer.h"

#include "pager.h"

namespace yudb {

PageId PageReferencer::page_id() const {
    return pager_->CacheToPageId(page_cache_);
}

PageReferencer::~PageReferencer() {
    Dereference();
}

void PageReferencer::Dereference() {
    if (page_cache_) {
        pager_->Dereference(page_cache_);
        page_cache_ = nullptr;
    }
}

} // namespace yudb