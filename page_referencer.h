#pragma once

#include <cstdint>

#include <utility>


#include "noncopyable.h"
#include "page.h"

namespace yudb {

class Pager;

class PageReferencer : noncopyable {
public:
    PageReferencer(Pager* pager, uint8_t* page_cache) :
        pager_{ pager },
        page_cache_{ page_cache } {}

    ~PageReferencer();

    PageReferencer(PageReferencer&& other) noexcept {
        operator=(std::move(other));
    }

    void operator=(PageReferencer&& other) noexcept {
        pager_ = other.pager_;
        page_cache_ = other.page_cache_;
        other.page_cache_ = nullptr;
    }

public:
    uint8_t* page_cache() { return page_cache_; }

    PageId page_id();

private:
    Pager* pager_;
    uint8_t* page_cache_;
};

} // namespace yudb