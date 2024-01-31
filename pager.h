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
    Pager(Pager&& right) noexcept : 
        db_{ nullptr },
        page_size_{right.page_size_},
        cache_manager_{ std::move(right.cache_manager_) },
        pending_{ std::move(right.pending_) }
    {
        cache_manager_.set_pager(this);
    }
    void operator=(Pager&& right) noexcept {
        db_ = nullptr;
        page_size_ = right.page_size_;
        cache_manager_ = std::move(right.cache_manager_);
        pending_ = std::move(right.pending_);
        cache_manager_.set_pager(this);
    }

    const auto& page_size() const { return page_size_; }
    const auto& db() const { return *db_; }
    void set_db(DBImpl* db) { db_ = db; }

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
        Free(page_ref.id(), 1);    // Pending
        return new_page;
    }
    PageReference Copy(PageId pgid) {
        auto page = Reference(pgid, false);
        return Copy(std::move(page));
    }

    // 线程安全
    PageReference Reference(PageId pgid, bool dirty) {
        assert(pgid != kPageInvalidId);
        auto [cache_info, page_cache] = cache_manager_.Reference(pgid);
        if (cache_info->dirty == false && cache_info->dirty != dirty) {
            cache_info->dirty = dirty;
        }
        return PageReference{ this, page_cache };
    }
    void Dereference(uint8_t* page_cache) {
        cache_manager_.Dereference(page_cache);
    }
    PageId CacheToPageId(uint8_t* page_cache) {
        return cache_manager_.CacheToPageId(page_cache);
    }

private:
    DBImpl* db_;
    PageSize page_size_;
    CacheManager cache_manager_;
    std::map<TxId, std::vector<std::pair<PageId, PageCount>>> pending_;
    
};

} // namespace yudb