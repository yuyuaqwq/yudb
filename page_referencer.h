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

    PageReferencer(PageReferencer&& right) noexcept {
        page_cache_ = nullptr;
        operator=(std::move(right));
    }

    void operator=(PageReferencer&& right) noexcept {
        Dereference();
        pager_ = right.pager_;
        page_cache_ = right.page_cache_;
        right.page_cache_ = nullptr;
    }

    template <typename T>
    const T& page_cache() const { return *reinterpret_cast<T*>(page_cache_); }

    template <typename T>
    T& page_cache() { return *reinterpret_cast<T*>(page_cache_); }

    PageId page_id() const;

protected:
    void Dereference();

protected:
    Pager* pager_;
    uint8_t* page_cache_;
};


} // namespace yudb