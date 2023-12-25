#include "pager.h"

#include "db.h"
#include "noder.h"
#include "tx.h"

namespace yudb {

void Pager::Read(PageId pgid, void* cache, PageCount count) {
    db_->file_.Seek(pgid * page_size());
    auto read_size = db_->file_.Read(cache, count * page_size());
    assert(read_size == 0 || read_size == count * page_size());
    if (read_size == 0) {
        memset(cache, 0, count * page_size());
    }
}

void Pager::Write(PageId pgid, void* cache, PageCount count) {
    db_->file_.Seek(pgid * page_size());
    db_->file_.Write(cache, count * page_size());
}


PageId Pager::Alloc(PageCount count) {
    PageId pgid = update_tx_->meta_.page_count;
    update_tx_->meta_.page_count += count;
    auto [cache_info, page_cache] = cacher_.Reference(pgid);
    cache_info->dirty = true;

    Noder noder{ &update_tx_->RootBucket().btree_, pgid};
    noder.node()->last_write_txid = update_tx_->txid();

    cacher_.Dereference(page_cache);
    return pgid;
}

void Pager::Free(PageId pgid, PageCount count) {
    // Ê×ÏÈpending
}

} // namespace yudb