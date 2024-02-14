#include "page.h"

#include "pager.h"

namespace yudb {


Page::Page(Pager* pager, uint8_t* page_buff) :
    pager_{ pager },
    page_buff_{ page_buff } {}

Page::~Page() {
    Dereference();
}

Page::Page(Page&& right) noexcept :
    pager_{ right.pager_ },
    page_buff_{ right.page_buff_ }
{
    right.page_buff_ = nullptr;
}

void Page::operator=(Page&& right) noexcept {
    Dereference();
    assert(pager_ == right.pager_);
    page_buff_ = right.page_buff_;
    right.page_buff_ = nullptr;
}

PageId Page::page_id() const {
    return pager_->CacheToPageId(page_buff_);
}

void Page::Dereference() {
    if (page_buff_) {
        pager_->Dereference(page_buff_);
        page_buff_ = nullptr;
    }
}


ConstPage::ConstPage(Page&& right) noexcept : 
    page_{ std::move(right) } {}

} // namespace yudb