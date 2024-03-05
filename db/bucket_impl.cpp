#include "yudb/bucket_impl.h"

#include "yudb/tx_manager.h"
#include "yudb/pager.h"
#include "yudb/db_impl.h"
#include "yudb/bucket.h"

namespace yudb {

BucketImpl::BucketImpl(TxImpl* tx, BucketId bucket_id, PageId* root_pgid, bool writable, const Comparator& comparator) :
    tx_{ tx },
    bucket_id_{ bucket_id },
    writable_{ writable },
    btree_{ this, root_pgid, comparator }
{}
BucketImpl::~BucketImpl() = default;

bool BucketImpl::Empty() const {
    return btree_.Empty();
}

BucketImpl::Iterator BucketImpl::Get(const void* key_buf, size_t key_size) {
    return Iterator{ btree_.Get({ reinterpret_cast<const uint8_t*>(key_buf), key_size }) };
}

BucketImpl::Iterator BucketImpl::LowerBound(const void* key_buf, size_t key_size) {
    return Iterator{ btree_.LowerBound({ reinterpret_cast<const uint8_t*>(key_buf), key_size }) };
}

void BucketImpl::Put(const void* key_buf, size_t key_size, const void* value_buf, size_t value_size) {
    std::span<const uint8_t> key_span{ reinterpret_cast<const uint8_t*>(key_buf), key_size };
    std::span<const uint8_t> value_span{ reinterpret_cast<const uint8_t*>(value_buf), value_size };
    tx_->AppendPutLog(bucket_id_, key_span, value_span);

    btree_.Put(key_span, value_span);
}

void BucketImpl::Update(Iterator* iter, const void* value_buf, size_t value_size) {
    auto key = iter->key();
    std::span<const uint8_t> key_span{ reinterpret_cast<const uint8_t*>(key.data()), key.size()};
    tx_->AppendPutLog(bucket_id_, key_span, { reinterpret_cast<const uint8_t*>(value_buf), value_size });
    btree_.Update(&iter->iterator_, { reinterpret_cast<const uint8_t*>(value_buf), value_size });
}

bool BucketImpl::Delete(const void* key_buf, size_t key_size) {
    std::span<const uint8_t> key_span{ reinterpret_cast<const uint8_t*>(key_buf), key_size };
    tx_->AppendDeleteLog(bucket_id_, key_span);
    return btree_.Delete(key_span);
}

void BucketImpl::Delete(Iterator* iter) {
    btree_.Delete(&iter->iterator_);
}

BucketImpl& BucketImpl::SubBucket(std::string_view key, bool writable, Comparator comparator) {
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
        } else {
            map_iter->second.second = iter.value<PageId>();
        }
        bucket_id = tx_->NewSubBucket(&map_iter->second.second, writable, comparator);
        map_iter->second.first = bucket_id;
    } else {
        bucket_id = map_iter->second.first;
    }
    return tx_->AtSubBucket(bucket_id);
}

BucketImpl::Iterator BucketImpl::begin() noexcept {
    return Iterator{ btree_.begin() };
}

BucketImpl::Iterator BucketImpl::end() noexcept {
    return Iterator{ btree_.end() };
}

void BucketImpl::Print(bool str) { btree_.Print(str); }

Pager& BucketImpl::pager() const { return tx_->pager(); }


ViewBucket::ViewBucket(BucketImpl* bucket) : bucket_{ bucket } {};

ViewBucket::~ViewBucket() = default;

ViewBucket ViewBucket::SubViewBucket(std::string_view key, const Comparator& comparator) {
    return ViewBucket{ &bucket_->SubBucket(key, false, comparator) };
}

ViewBucket::Iterator ViewBucket::Get(const void* key_buf, size_t key_size) const {
    return bucket_->Get(key_buf, key_size);
}

ViewBucket::Iterator ViewBucket::Get(std::string_view key) const {
    return Get(key.data(), key.size());
}

ViewBucket::Iterator ViewBucket::LowerBound(const void* key_buf, size_t key_size) const {
    return bucket_->LowerBound(key_buf, key_size);
}

ViewBucket::Iterator ViewBucket::LowerBound(std::string_view key) const {
    return LowerBound(key.data(), key.size());
}

ViewBucket::Iterator ViewBucket::begin() const noexcept {
    return bucket_->begin();
}

ViewBucket::Iterator ViewBucket::end() const noexcept {
    return bucket_->end();
}


UpdateBucket::~UpdateBucket() = default;

UpdateBucket UpdateBucket::SubUpdateBucket(std::string_view key, const Comparator& comparator) {
    return UpdateBucket{ &bucket_->SubBucket(key, true, comparator) };
}

void UpdateBucket::Put(const void* key_buf, size_t key_size, const void* value_buf, size_t value_size) {
    bucket_->Put(key_buf, key_size, value_buf, value_size);
}

void UpdateBucket::Put(std::string_view key, std::string_view value) {
    Put(key.data(), key.size(), value.data(), value.size());
}

bool UpdateBucket::Delete(const void* key_buf, size_t key_size) {
    return bucket_->Delete(key_buf, key_size);
}

bool UpdateBucket::Delete(std::string_view key) {
    return bucket_->Delete(key.data(), key.size());
}

} // namespace yudb