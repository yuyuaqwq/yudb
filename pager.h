#pragma once

#include "page.h"
#include "cacher.h"

namespace yudb {

class Db;

class Pager {
public:
    Pager(Db* db) : db_{ db } {}

    PageSize page_size() { return page_size_; }
    PageCount page_count() { return page_count_; }

    void Read(PageId pgid, void* buf, PageCount count) {

    }

    uint8_t* Reference(PageId pgid) {
        return cacher_.Reference(pgid);
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