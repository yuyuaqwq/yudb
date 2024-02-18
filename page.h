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
    Page(Pager* pager, uint8_t* page_buf);
    ~Page();

    Page(Page&& right) noexcept;
    void operator=(Page&& right) noexcept;

    Page AddReference();

    auto& page_buf() { return page_buf_; }
    auto& page_buf() const { return page_buf_; }
    PageId page_id() const;

protected:
    void Dereference();

protected:
    Pager* const pager_;
    uint8_t* page_buf_;
};

class ConstPage {
public:
    ConstPage(Page&& right) noexcept;

private:
    Page page_;
};

} // namespace yudb