#include "cacher.h"

#include "pager.h"

namespace yudb {

Cacher::Cacher(Pager* pager) :
    pager_ { pager },
    lru_list_{ pager->page_count() }
{
    page_pool_ = reinterpret_cast<uint8_t*>(operator new(pager->page_count() * pager->page_size()));
}

Cacher::~Cacher() {
    operator delete(page_pool_);
}

} // namespace yudb