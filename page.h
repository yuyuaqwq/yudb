#pragma once

#include <cstdint>
#include <cassert>

#include <utility>


#include "noncopyable.h"
#include "page_format.h"

namespace yudb {

class Pager;

class Page : noncopyable {
public:
    Page(Pager* pager, uint8_t* page_buff);
    ~Page();

    Page(Page&& right) noexcept;
    void operator=(Page&& right) noexcept;

    template <typename T> T& content() const { return *reinterpret_cast<T*>(page_buff_); }
    template <typename T> const T& const_content() const { return *reinterpret_cast<T*>(page_buff_); }

    PageId page_id() const;

protected:
    void Dereference();

protected:
    Pager* const pager_;
    uint8_t* page_buff_;
};

class ConstPage {
public:
    ConstPage(Page&& right) noexcept;

private:
    Page page_;
};

} // namespace yudb