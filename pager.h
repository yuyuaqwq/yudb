#pragma once

#include <memory>

#include "noncopyable.h"
#include "page.h"
#include "page_referencer.h"
#include "cacher.h"

namespace yudb {

class Db;
class UpdateTx;

class Pager : noncopyable {
public:
    Pager(Db* db, PageSize page_size) : db_{ db }, 
        page_size_{ page_size }, 
        cacher_{ this },
        update_tx_{ nullptr } {}

    Pager(const Pager&) = delete;
    void operator=(const Pager&) = delete;

public:
    PageSize page_size() { return page_size_; }

    void set_update_tx(UpdateTx* update_tx) { update_tx_ = update_tx; }

    /*
    * 非线程安全，仅写事务使用
    */
    void Read(PageId pgid, void* cache, PageCount count);

    void Write(PageId pgid, void* cache, PageCount count);

    PageId Alloc(PageCount count);

    void Free(PageId pgid, PageCount count);

    // 线程安全
    PageReferencer Reference(PageId pgid) {
        auto [cache_info, page_cache] = cacher_.Reference(pgid);
        return PageReferencer{ this, page_cache };
    }

    void Dereference(uint8_t* page_cache) {
        cacher_.Dereference(page_cache);
    }

    PageId CacheToPageId(uint8_t* page_cache) const {
        return cacher_.CacheToPageId(page_cache);
    }

private:
    Db* db_;

    PageSize page_size_;

    UpdateTx* update_tx_;

    Cacher cacher_;
};

} // namespace yudb