#pragma once

#include <memory>
#include <map>
#include <vector>

#include "noncopyable.h"
#include "page.h"
#include "page_reference.h"
#include "cacher.h"
#include "bucket.h"

namespace yudb {

class Db;
class UpdateTx;

class Pager : noncopyable {
public:
    Pager(Db* db, PageSize page_size) : db_{ db },
        page_size_{ page_size },
        cacher_{ this } {}

    Pager(Pager&& right) noexcept : 
        db_{ right.db_ },
        page_size_{right.page_size_},
        cacher_{ std::move(right.cacher_) },
        pending_{ std::move(right.pending_) }
    {
        cacher_.set_pager(this);
    }
    void operator=(Pager&& right) noexcept {
        db_ = right.db_;
        page_size_ = right.page_size_;
        cacher_ = std::move(right.cacher_);
        cacher_.set_pager(this);
        pending_ = std::move(right.pending_);
    }

public:
    /*
    * 非线程安全，仅写事务使用
    */
    void Read(PageId pgid, void* cache, PageCount count);

    void Write(PageId pgid, void* cache, PageCount count);

    PageId Alloc(PageCount count);

    void Free(PageId pgid, PageCount count);

    void RollbackPending();

    void CommitPending();

    void ClearPending(TxId min_view_txid);

    PageReference Copy(const PageReference& page_ref) {
        auto new_pgid = Alloc(1);
        auto new_page = Reference(new_pgid, true);
        std::memcpy(&new_page.page_content<uint8_t>(), &page_ref.page_content<uint8_t>(), page_size());
        Free(page_ref.page_id(), 1);    // Pending
        return new_page;
    }

    PageReference Copy(PageId pgid) {
        auto page = Reference(pgid, false);
        return Copy(std::move(page));
    }


    // 线程安全
    PageReference Reference(PageId pgid, bool dirty) {
        assert(pgid != kPageInvalidId);
        auto [cache_info, page_cache] = cacher_.Reference(pgid);
        if (cache_info->dirty == false && cache_info->dirty != dirty) {
            cache_info->dirty = dirty;
        }
        return PageReference{ this, page_cache };
    }

    void Dereference(uint8_t* page_cache) {
        cacher_.Dereference(page_cache);
    }


    PageId CacheToPageId(uint8_t* page_cache) const {
        return cacher_.CacheToPageId(page_cache);
    }


    PageSize page_size() { return page_size_; }

private:
    Db* db_;
    PageSize page_size_;
    Cacher cacher_;
    std::map<TxId, std::vector<std::pair<PageId, PageCount>>> pending_;
    
};

} // namespace yudb