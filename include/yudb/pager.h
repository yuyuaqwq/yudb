//The MIT License(MIT)
//Copyright © 2024 https://github.com/yuyuaqwq
//
//Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files(the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and /or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
//
//The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
//THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

#pragma once

#include <memory>
#include <map>
#include <unordered_set>
#include <vector>
#include <forward_list>

#include "yudb/page.h"
#include "yudb/tx_format.h"
#include "yudb/noncopyable.h"

namespace yudb {

class DBImpl;
class UpdateTx;

class Pager : noncopyable {
public:
    Pager(DBImpl* db, PageSize page_size);
    ~Pager();

    uint8_t* GetPtr(PageId pgid, size_t offset);
    void Write(PageId pgid, const uint8_t* cache, PageCount count);
    void WriteByBytes(PageId pgid, size_t offset, const uint8_t* buf, size_t bytes);
    void WriteAllDirtyPages();

    void Rollback();

    PageId Alloc(PageCount count);
    void Free(PageId pgid, PageCount count);
    Page Copy(const Page& page_ref);
    Page Copy(PageId pgid);

    void Release(TxId releasable_txid);

    void LoadFreeList();
    void SaveFreeList();

    PageId GetPageIdByPtr(const uint8_t* page_ptr) const;
    PageCount GetPageCount(const size_t bytes) const;

    Page Reference(PageId pgid, bool dirty);
    Page AddReference(uint8_t* page_buf);
    void Dereference(const uint8_t* page_buf);
    
    auto& db() const { return *db_; }
    auto& page_size() const { return page_size_; }
    auto& tmp_page() { return tmp_page_; }

private:
    PageId AllocFromMap(PageCount count);
    void FreeToMap(PageId pgid, PageCount count);

private:
    DBImpl* const db_;
    const PageSize page_size_;

    using PagePair = std::pair<PageId, PageCount>;
    std::map<TxId, std::vector<PagePair>> pending_map_;
    std::map<PageId, PageCount> free_map_;
    std::vector<PagePair> alloc_records_;

    uint8_t* tmp_page_;

#ifndef NDEBUG
    std::unordered_set<PageId> debug_free_set_;
#endif
};

} // namespace yudb