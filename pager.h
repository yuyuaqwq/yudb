#pragma once

#include <memory>
#include <map>
#include <vector>

#include "noncopyable.h"
#include "page.h"
#include "page_referencer.h"
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

    PageReferencer Copy(const PageReferencer& page_ref) {
        auto new_pgid = Alloc(1);
        auto new_page = Reference(new_pgid, true);
        std::memcpy(&new_page.page_cache<uint8_t>(), &page_ref.page_cache<uint8_t>(), page_size());
        Free(page_ref.page_id(), 1);    // Pending
        return new_page;
    }

    PageReferencer Copy(PageId pgid) {
        auto page = Reference(pgid, false);
        return Copy(std::move(page));
    }


    // 线程安全
    PageReferencer Reference(PageId pgid, bool dirty) {
        assert(pgid != kPageInvalidId);
        auto [cache_info, page_cache] = cacher_.Reference(pgid);
        if (cache_info->dirty == false && cache_info->dirty != dirty) {
            cache_info->dirty = dirty;
        }
        return PageReferencer{ this, page_cache };
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