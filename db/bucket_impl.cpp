#include "bucket_impl.h"

#include "tx_manager.h"
#include "pager.h"
#include "db_impl.h"

namespace yudb {

inline std::strong_ordering DefaultComparator(std::span<const uint8_t> key1, std::span<const uint8_t> key2) {
    auto res = std::memcmp(key1.data(), key2.data(), std::min(key1.size(), key2.size()));
    if (res == 0) {
        if (key1.size() == key2.size()) {
            return std::strong_ordering::equal;
        }
        res = key1.size() - key2.size();
    } else if (res < 0) {
        return std::strong_ordering::less;
    } else {
        return std::strong_ordering::greater;
    }
}

BucketImpl::BucketImpl(TxImpl* tx, BucketId bucket_id, PageId* root_pgid, bool writable) :
    tx_{ tx },
    bucket_id_{ bucket_id },
    writable_{ writable }
{
    btree_.emplace(this, root_pgid, DefaultComparator);
}


BucketImpl::Iterator BucketImpl::Get(const void* key_buf, size_t key_size) {
    return Iterator{ btree_->Get({ reinterpret_cast<const uint8_t*>(key_buf), key_size }) };
}

BucketImpl::Iterator BucketImpl::LowerBound(const void* key_buf, size_t key_size) {
    return Iterator{ btree_->LowerBound({ reinterpret_cast<const uint8_t*>(key_buf), key_size }) };
}

void BucketImpl::Insert(const void* key_buf, size_t key_size, const void* value_buf, size_t value_size) {
    std::span<const uint8_t> key_span{ reinterpret_cast<const uint8_t*>(key_buf), key_size };
    std::span<const uint8_t> value_span{ reinterpret_cast<const uint8_t*>(value_buf), value_size };
    tx_->AppendInsertLog(bucket_id_, key_span, value_span);

    btree_->Insert(key_span, value_span);
}

void BucketImpl::Put(const void* key_buf, size_t key_size, const void* value_buf, size_t value_size) {
    std::span<const uint8_t> key_span{ reinterpret_cast<const uint8_t*>(key_buf), key_size };
    std::span<const uint8_t> value_span{ reinterpret_cast<const uint8_t*>(value_buf), value_size };
    tx_->AppendPutLog(bucket_id_, key_span, value_span);

    btree_->Put(key_span, value_span);
}

void BucketImpl::Update(Iterator* iter, const void* value_buf, size_t value_size) {
    auto key = iter->key();
    std::span<const uint8_t> key_span{ reinterpret_cast<const uint8_t*>(key.data()), key.size()};
    tx_->AppendPutLog(bucket_id_, key_span, { reinterpret_cast<const uint8_t*>(value_buf), value_size });
    btree_->Update(&iter->iterator_, { reinterpret_cast<const uint8_t*>(value_buf), value_size });
}


bool BucketImpl::Delete(const void* key_buf, size_t key_size) {
    std::span<const uint8_t> key_span{ reinterpret_cast<const uint8_t*>(key_buf), key_size };
    tx_->AppendDeleteLog(bucket_id_, key_span);
    return btree_->Delete(key_span);
}


void BucketImpl::Delete(Iterator* iter) {
    btree_->Delete(&iter->iterator_);
}

BucketImpl& BucketImpl::SubBucket(std::string_view key, bool writable) {
    auto map_iter = sub_bucket_map_.find({ key.data(), key.size() });
    BucketId bucket_id;
    if (map_iter == sub_bucket_map_.end()) {
        auto res = sub_bucket_map_.insert({ { key.data(), key.size() }, { 0, kPageInvalidId } });
        map_iter = res.first;
        auto iter = Get(key.data(), key.size());
        if (iter == end()) {
            // 提前预留空间，以避免Commit时的Put触发分裂
            PageId pgid = kPageInvalidId;
            Put(key.data(), key.size(), &pgid, sizeof(pgid));
            iter = Get(key.data(), key.size());
        }
        else {
            auto data = iter.value();
            auto pgid = *reinterpret_cast<const PageId*>(data.data());
            map_iter->second.second = pgid;
        }
        bucket_id = tx_->NewSubBucket(&map_iter->second.second, writable);
        map_iter->second.first = bucket_id;
    }
    else {
        bucket_id = map_iter->second.first;
    }
    return tx_->AtSubBucket(bucket_id);
}


BucketImpl::Iterator BucketImpl::begin() noexcept {
    return Iterator{ btree_->begin() };
}
BucketImpl::Iterator BucketImpl::end() noexcept {
    return Iterator{ btree_->end() };
}

void BucketImpl::Print(bool str) { btree_->Print(str); }



Pager& BucketImpl::pager() const { return tx_->pager(); }


} // namespace yudb