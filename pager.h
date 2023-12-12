#pragma once

#include <memory>

#include "noncopyable.h"
#include "page.h"
#include "cacher.h"

namespace yudb {

class Db;

class Pager : noncopyable {
public:
    class PageReference : noncopyable {
    public:
        PageReference(Pager* pager, uint8_t* page_cache) : 
            pager_{ pager }, 
            page_cache_ { page_cache } {}

        ~PageReference() {
            if (page_cache_) {
                pager_->Dereference(page_cache_);
            }
        }

        PageReference(PageReference&& other) noexcept {
            operator=(std::move(other));
        }

        void operator=(PageReference&& other) noexcept {
            pager_ = other.pager_;
            page_cache_ = other.page_cache_;
            other.page_cache_ = nullptr;
        }

    public:
        uint8_t* page_cache() { return page_cache_; }

        PageId page_id();

    private:
        Pager* pager_;
        uint8_t* page_cache_;
    };

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

    void Read(PageId pgid, void* cache, PageCount count);

    void Write(PageId pgid, void* cache, PageCount count);

    PageId Alloc(PageCount count) {
        PageId pgid = page_count_;
        page_count_ += count;
        return pgid;
    }

    void Free(PageId pgid, PageCount count) {

    }

    std::pair<PageReference, uint8_t*> Reference(PageId pgid) {
        auto page_cache = cacher_.Reference(pgid);
        return { PageReference{ this, page_cache }, page_cache };
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