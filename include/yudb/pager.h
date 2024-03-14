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