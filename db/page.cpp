//The MIT License(MIT)
//Copyright ? 2024 https://github.com/yuyuaqwq
//
//Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files(the ��Software��), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and /or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
//
//The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
//THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

#include <yudb/page.h>

#include "pager.h"

namespace yudb {

Page::Page(Pager* pager, uint8_t* page_buf)
    : pager_(pager)
    , page_buf_(page_buf)
    , page_id_(pager->GetPageIdByPtr(page_buf)) {}

Page::~Page() {
    Dereference();
}

Page::Page(Page&& right) noexcept :
    pager_{ right.pager_ },
    page_buf_{ right.page_buf_ },
    page_id_{ right.page_id_ }
{
    right.page_buf_ = nullptr;
}

void Page::operator=(Page&& right) noexcept {
    Dereference();
    assert(pager_ == right.pager_);
    page_buf_ = right.page_buf_;
    page_id_ = right.page_id_;
    right.page_buf_ = nullptr;
}

Page Page::AddReference() const {
    return pager_->AddReference(page_buf_);
}

void Page::Dereference() {
    if (page_buf_) {
        pager_->Dereference(page_buf_);
        page_buf_ = nullptr;
    }
}

} // namespace yudb