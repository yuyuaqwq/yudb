#pragma once

#include <memory>

#include "noncopyable.h"
#include "page.h"
#include "page_referencer.h"
#include "cacher.h"

namespace yudb {

class Db;

class Pager : noncopyable {
public:
    Pager(Db* db, PageSize page_size) : db_{ db }, 
        page_size_{ page_size }, 
        cacher_{ this } {}

    Pager(const Pager&) = delete;
    void operator=(const Pager&) = delete;

public:
    PageSize page_size() { return page_size_; }
    PageCount page_count() { return page_count_; }
    void set_page_count(PageCount page_count) { page_count_ = page_count; }

    /*
    * 非线程安全，仅写事务使用
    */
    void Read(PageId pgid, void* cache, PageCount count);

    void Write(PageId pgid, void* cache, PageCount count);

    PageId Alloc(PageCount count) {
        PageId pgid = page_count_;
        page_count_ += count;
        auto [cache_info, page_cache] = cacher_.Reference(pgid);
        cache_info->dirty = true;
        cacher_.Dereference(page_cache);
        return pgid;
    }

    void Free(PageId pgid, PageCount count) {

    }

    // 线程安全
    PageReferencer Reference(PageId pgid) {
        auto [cache_info, page_cache] = cacher_.Reference(pgid);
        return PageReferencer{ this, page_cache };
    }

    void Dereference(uint8_t* page_cache) {
        cacher_.Dereference(page_cache);
    }

    PageId CacheToPageId(uint8_t* page_cache) {
        return cacher_.CacheToPageId(page_cache);
    }

private:
    Db* db_;

    PageSize page_size_;
    PageCount page_count_{ 0 };

    Cacher cacher_;
};

} // namespace yudb