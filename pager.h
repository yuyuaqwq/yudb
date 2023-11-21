#pragma once

#include "page.h"

namespace yudb {

class Db;

class Pager {
public:
    
private:
    PageSize page_size_;
    PageCount page_count_;

    Db* db_;
};

} // namespace yudb