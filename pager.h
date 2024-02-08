#pragma once

#include <memory>
#include <map>
#include <vector>

#include "noncopyable.h"
#include "page_format.h"
#include "page_reference.h"
#include "cache_manager.h"
#include "bucket_impl.h"

namespace yudb {

class DBImpl;
class UpdateTx;

class Pager : noncopyable {
public:
    Pager(DBImpl* db, PageSize page_size) : db_{ db },
        page_size_{ page_size },
        cache_manager_{ this } {}

    auto& page_size() const { return page_size_; }
    auto& db() const { return *db_; }

    /*
    * 非线程安全，仅写事务使用
    */
    void Read(PageId pgid, void* cache, PageCount count);
    void Write(PageId pgid, const void* cache, PageCount count);
    void SyncAllPage();

    PageId Alloc(PageCount count);
    void Free(PageId pgid, PageCount count);
    void RollbackPending();
    void CommitPending();
    void ClearPending(TxId min_view_txid);
    PageReference Copy(const PageReference& page_ref) {
        auto new_pgid = Alloc(1);
        auto new_page = Reference(new_pgid, true);
        std::memcpy(&new_page.content<uint8_t>(), &page_ref.content<uint8_t>(), page_size());
        Free(page_ref.page_id(), 1);    // Pending
        return new_page;
    }
    PageReference Copy(PageId pgid) {
        auto page = Reference(pgid, false);
        return Copy(std::move(page));
    }

    // 线程安全
    PageReference Reference(PageId pgid, bool dirty);
    void Dereference(uint8_t* page_cache);
    PageId CacheToPageId(uint8_t* page_cache) {
        return cache_manager_.CacheToPageId(page_cache);
    }

private:
    DBImpl* const db_;
    const PageSize page_size_;
    CacheManager cache_manager_;
    std::map<TxId, std::vector<std::pair<PageId, PageCount>>> pending_;
    
};

} // namespace yudb