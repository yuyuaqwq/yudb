#pragma once

#include "page.h"

namespace yudb {

struct Overflow {
    struct Element {
        PageId pgid;
        PageOffset free_list;
        uint16_t max_free_size;

        void Init(PageId new_pgid, PageOffset new_free_list, uint16_t new_max_free_size) {
            pgid = new_pgid;
            free_list = new_free_list;
            max_free_size = new_max_free_size;
        }
    };

    PageId pgid;
    PageOffset offset;
    uint16_t element_count;
};

} // namespace yudb