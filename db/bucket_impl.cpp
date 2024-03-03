#include "db/bucket_impl.h"

#include "db/tx_manager.h"
#include "db/pager.h"
#include "db/db_impl.h"
#include "yudb/bucket.h"

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
    writable_{ writable },
    btree_{ this, root_pgid, DefaultComparator }
{}


BucketImpl::Iterator BucketImpl::Get(const void* key_buf, size_t key_size) {
    return Iterator{ btree_.Get({ reinterpret_cast<const uint8_t*>(key_buf), key_size }) };
}

BucketImpl::Iterator BucketImpl::LowerBound(const void* key_buf, size_t key_size) {
    return Iterator{ btree_.LowerBound({ reinterpret_cast<const uint8_t*>(key_buf), key_size }) };
}

//void BucketImpl::Insert(const void* key_buf, size_t key_size, const void* value_buf, size_t value_size) {
//    std::span<const uint8_t> key_span{ reinterpret_cast<const uint8_t*>(key_buf), key_size };
//    std::span<const uint8_t> value_span{ reinterpret_cast<const uint8_t*>(value_buf), value_size };
//    tx_->AppendInsertLog(bucket_id_, key_span, value_span);
//
//    btree_.Insert(key_span, value_span);
//}

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

BucketImpl& BucketImpl::SubBucket(std::string_view key, bool writable) {
    auto map_iter = sub_bucket_map_.find({ key.data(), key.size() });
    BucketId bucket_id;
    if (map_iter == sub_bucket_map_.end()) {
        auto res = sub_bucket_map_.insert({ { key.data(), key.size() }, { 0, kPageInvalidId } });
        map_iter = res.first;
        auto iter = Get(key.data(), key.size());
        if (iter == end()) {
            // ��ǰԤ���ռ䣬�Ա���Commitʱ��Put��������
            PageId pgid = kPageInvalidId;
            Put(key.data(), key.size(), &pgid, sizeof(pgid));
            iter = Get(key.data(), key.size());
        } else {
            map_iter->second.second = iter.value<PageId>();
        }
        bucket_id = tx_->NewSubBucket(&map_iter->second.second, writable);
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

ViewBucket ViewBucket::SubViewBucket(std::string_view key) {
    return ViewBucket{ &bucket_->SubBucket(key, false) };
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

void ViewBucket::Print(bool str) const {
    bucket_->Print(str);
}


UpdateBucket::~UpdateBucket() = default;

UpdateBucket UpdateBucket::SubUpdateBucket(std::string_view key) {
    return UpdateBucket{ &bucket_->SubBucket(key, true) };
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