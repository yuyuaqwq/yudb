#pragma once

#include <cstdint>

#include <utility>


#include "noncopyable.h"
#include "page_format.h"

namespace yudb {

class Pager;

class PageReference : noncopyable {
public:
    PageReference(Pager* pager, uint8_t* page_buff) :
        pager_{ pager },
        page_buff_{ page_buff } {}

    ~PageReference();

    PageReference(PageReference&& right) noexcept {
        page_buff_ = nullptr;
        operator=(std::move(right));
    }
    void operator=(PageReference&& right) noexcept {
        Dereference();
        pager_ = right.pager_;
        page_buff_ = right.page_buff_;
        right.page_buff_ = nullptr;
    }

    template <typename T> const T& content() const { return *reinterpret_cast<T*>(page_buff_); }
    template <typename T> T& content() { return *reinterpret_cast<T*>(page_buff_); }

    PageId page_id() const;

protected:
    void Dereference();

protected:
    Pager* pager_;
    uint8_t* page_buff_;
};


} // namespace yudb