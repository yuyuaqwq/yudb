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

constexpr const char* kFreeDBKey = "free_db";
constexpr const char* kPendingDBKey = "pending_db";

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

    PageId Alloc(PageCount count);
    void Free(PageId pgid, PageCount count);
    void RollbackPending();
    void CommitPending();
    void FreePending(TxId min_view_txid);
    void ClearPending();
    Page Copy(const Page& page_ref);
    Page Copy(PageId pgid);

    // 线程安全函数
    Page Reference(PageId pgid, bool dirty);
    Page AddReference(uint8_t* page_cache);
    void Dereference(const uint8_t* page_cache);
    PageId GetPageIdByCache(const uint8_t* page_cache);

    auto& db() const { return *db_; }
    auto& page_size() const { return page_size_; }
    auto& tmp_page() { return tmp_page_; }

private:
    void FreeToFreeDB(PageId pgid, PageCount count);

private:
    DBImpl* const db_;
    const PageSize page_size_;
    CacheManager cache_manager_;
    std::map<TxId, std::vector<std::pair<PageId, PageCount>>> pending_;

    uint8_t* tmp_page_;

    bool free_db_lock_{ false };     // prevent idle databases from allocating pages during self modification.


#ifndef NDEBUG
    std::unordered_set<PageId> free_page_;
#endif
};

} // namespace yudb