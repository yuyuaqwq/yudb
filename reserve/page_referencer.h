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
        page_cache_ = nullptr;
        operator=(std::move(other));
    }

    void operator=(PageReferencer&& other) noexcept {
        Dereference();
        pager_ = other.pager_;
        page_cache_ = other.page_cache_;
        other.page_cache_ = nullptr;
    }


    uint8_t* page_cache() const { return page_cache_; }

    PageId page_id() const;

private:
    void Dereference();

private:
    Pager* pager_;
    uint8_t* page_cache_;
};

} // namespace yudb