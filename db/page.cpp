#include "page.h"

#include "pager.h"

namespace yudb {


Page::Page(Pager* pager, uint8_t* page_buf) :
    pager_{ pager },
    page_buf_{ page_buf } {}

Page::~Page() {
    Dereference();
}

Page::Page(Page&& right) noexcept :
    pager_{ right.pager_ },
    page_buf_{ right.page_buf_ }
{
    right.page_buf_ = nullptr;
}

void Page::operator=(Page&& right) noexcept {
    Dereference();
    assert(pager_ == right.pager_);
    page_buf_ = right.page_buf_;
    right.page_buf_ = nullptr;
}

Page Page::AddReference() const {
    return pager_->AddReference(page_buf_);
}

PageId Page::page_id() const {
    return pager_->GetPageIdByCache(page_buf_);
}

void Page::Dereference() {
    if (page_buf_) {
        pager_->Dereference(page_buf_);
        page_buf_ = nullptr;
    }
}


ConstPage::ConstPage(Page&& right) noexcept : 
    page_{ std::move(right) } {}

} // namespace yudb