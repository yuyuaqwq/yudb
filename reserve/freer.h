#pragma once

#include <cstdint>
#include <vector>

#include "page.h"
#include "db.h"

namespace yudb {

class Db;

class Freer {
public:
    Freer(Db* db) : db_{ db } {}

    PageId Alloc(PageCount count) {
        

    }

    void Free(PageId pgid) {

    }

private:
    Db* db_;
};

} // namespace yudb