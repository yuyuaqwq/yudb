#pragma once

#include <memory>
#include <map>
#include <unordered_set>
#include <vector>

#include "yudb/page.h"
#include "yudb/tx_format.h"
#include "yudb/cache_manager.h"
#include "yudb/noncopyable.h"

namespace yudb {

class DBImpl;
class UpdateTx;

class Pager : noncopyable {
public:
    Pager(DBImpl* db, PageSize page_size);
    ~Pager();

    // 非线程安全函数，仅写事务使用
    void Read(PageId pgid, uint8_t* cache, PageCount count);
    void ReadByBytes(PageId pgid, size_t offset, uint8_t* cache, size_t bytes);
    void Write(PageId pgid, const uint8_t* cache, PageCount count);
    void WriteByBytes(PageId pgid, size_t offset, const uint8_t* cache, size_t bytes);
    void WriteAllDirtyPages();

    void Rollback();

    PageId Alloc(PageCount count);
    void Free(PageId pgid, PageCount count);
    Page Copy(const Page& page_ref);
    Page Copy(PageId pgid);

    void FreePending(TxId min_view_txid);

    void BuildFreeMap();
    void UpdateFreeList();

    // 线程安全函数
    Page Reference(PageId pgid, bool dirty);
    Page AddReference(uint8_t* page_cache);
    void Dereference(const uint8_t* page_cache);
    PageId GetPageIdByCache(const uint8_t* page_cache);

    auto& db() const { return *db_; }
    auto& page_size() const { return page_size_; }
    auto& tmp_page() { return tmp_page_; }

private:
    void FreeToFreeMap(PageId pgid, PageCount count);
    void FreeToFreeDB(PageId pgid, PageCount count);

private:
    DBImpl* const db_;
    const PageSize page_size_;
    CacheManager cache_manager_;

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