#include "page_referencer.h"

#include "pager.h"

namespace yudb {

PageId PageReferencer::page_id() {
    return pager_->CacheToPageId(page_cache_);
}

PageReferencer::~PageReferencer() {
    if (page_cache_) {
        pager_->Dereference(page_cache_);
    }
}

} // namespace yudb