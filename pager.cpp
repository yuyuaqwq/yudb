#include "pager.h"

#include "db.h"

namespace yudb {

void Pager::Read(PageId pgid, void* cache, PageCount count) {
    db_->file_.Seek(pgid * page_size());
    if (!db_->file_.Read(cache, count * page_size())) {
        memset(cache, 0, count * page_size());
    }
}

void Pager::Write(PageId pgid, void* cache, PageCount count) {
    db_->file_.Seek(pgid * page_size());
    db_->file_.Write(cache, count * page_size());
}

} // namespace yudb