#pragma once

#include "page.h"
#include "cacher.h"

namespace yudb {

class Db;

class Pager {
public:
    class PageReference {
    public:
        PageReference(Pager* pager, uint8_t* page_cache) : pager_{ pager }, page_cache_ { page_cache } {}
        ~PageReference() {
            if (page_cache_) {
                pager_->Dereference(page_cache_);
            }
        }

        PageReference(PageReference&& other) {
            operator=(std::move(other));
        }
        void operator=(PageReference&& other) {
            page_cache_ = other.page_cache_;
            other.page_cache_ = nullptr;
        }

        PageReference(const PageReference&) = delete;
        void operator=(const PageReference&) = delete;

    public:
        uint8_t* page_cache() { return page_cache_; }

    private:
        Pager* pager_;
        uint8_t* page_cache_;
    };

public:
    Pager(Db* db) : db_{ db } {}

public:
    PageSize page_size() { return page_size_; }
    PageCount page_count() { return page_count_; }

    void Read(PageId pgid, void* buf, PageCount count) {

    }

    PageId Alloc(PageCount count) {
        
    }

    PageReference Reference(PageId pgid) {
        return PageReference{ this, cacher_.Reference(pgid) };
    }

    void Dereference(uint8_t* page_cache) {
        cacher_.Dereference(page_cache);
    }

private:
    Db* db_;

    PageSize page_size_;
    PageCount page_count_;

    Cacher cacher_{ this };
};

} // namespace yudb