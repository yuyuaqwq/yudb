#pragma once

#include "page.h"
#include "pager.h"

namespace yudb {

/*
* b+tree
* |        |
*/

constexpr size_t kDataSize = sizeof(Data);

class BTree {
public:


public:


private:
    Pager* pager_;
    // Tx* tx_;

    PageId root_; // PageId&
};

} // namespace yudb