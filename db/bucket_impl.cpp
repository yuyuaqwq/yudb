//The MIT License(MIT)
//Copyright © 2024 https://github.com/yuyuaqwq
//
//Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files(the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and /or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
//
//The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
//THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

#include "yudb/bucket_impl.h"

#include "yudb/bucket.h"

#include "db_impl.h"
#include "tx_manager.h"
#include "pager.h"

namespace yudb {

BucketImpl::BucketImpl(TxImpl* tx, BucketId bucket_id, PageId* root_pgid, bool writable, Comparator comparator) :
    tx_(tx),
    bucket_id_(bucket_id),
    writable_(writable),
    btree_(this, root_pgid, comparator)
{}

BucketImpl::~BucketImpl() = default;

bool BucketImpl::Empty() const {
    return btree_.Empty();
}

BucketImpl::Iterator BucketImpl::Get(const void* key_buf, size_t key_size) {
    return Iterator(btree_.Get({ reinterpret_cast<const uint8_t*>(key_buf), key_size }));
}

BucketImpl::Iterator BucketImpl::LowerBound(const void* key_buf, size_t key_size) {
    return Iterator(btree_.LowerBound({ reinterpret_cast<const uint8_t*>(key_buf), key_size }));
}

void BucketImpl::Put(const void* key_buf, size_t key_size, const void* value_buf, size_t value_size, bool is_bucket) {
    auto key_span = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(key_buf), key_size);
    auto value_span = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(value_buf), value_size);
    tx_->AppendPutLog(bucket_id_, key_span, value_span, is_bucket);

    btree_.Put(key_span, value_span, is_bucket);
}

void BucketImpl::Update(Iterator* iter, const void* value_buf, size_t value_size) {
    auto key = iter->key();
    auto key_span = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(key.data()), key.size());
    tx_->AppendPutLog(bucket_id_, key_span, 
        { reinterpret_cast<const uint8_t*>(value_buf), value_size }, iter->is_bucket());
    btree_.Update(&iter->iter_, { reinterpret_cast<const uint8_t*>(value_buf), value_size });
}

bool BucketImpl::Delete(const void* key_buf, size_t key_size) {
    auto key_span = std::span<const uint8_t>(reinterpret_cast<const uint8_t*>(key_buf), key_size);
    tx_->AppendDeleteLog(bucket_id_, key_span);
    return btree_.Delete(key_span);
}

void BucketImpl::Delete(Iterator* iter) {
    btree_.Delete(&iter->iter_);
}

BucketImpl& BucketImpl::SubBucket(std::string_view key, bool writable) {
    if (!sub_bucket_map_.has_value()) {
        sub_bucket_map_.emplace();
    }
    auto map_iter = sub_bucket_map_->find({ key.data(), key.size() });
    BucketId bucket_id;
    if (map_iter == sub_bucket_map_->end()) {
        auto res = sub_bucket_map_->insert({ { key.data(), key.size() }, { 0, kPageInvalidId } });
        map_iter = res.first;
        auto iter = Get(key.data(), key.size());
        if (iter == end()) {
            // Reserve enough space in advance to avoid triggering a split during Commit's Put operation
            PageId pgid = kPageInvalidId;
            Put(key.data(), key.size(), &pgid, sizeof(pgid), true);
            iter = Get(key.data(), key.size());
            assert(iter.is_bucket());
        } else {
            if (iter.is_bucket() == false) {
                throw InvalidArgumentError{ "attempt to open a key value pair that is not a sub bucket." };
            }
            map_iter->second.second = iter.value<PageId>();
        }
        bucket_id = tx_->NewSubBucket(&map_iter->second.second, writable, tx().tx_manager().db().options()->comparator);
        map_iter->second.first = bucket_id;
    } else {
        bucket_id = map_iter->second.first;
    }
    return tx_->AtSubBucket(bucket_id);
}

bool BucketImpl::DeleteSubBucket(std::string_view key) {
    auto iter = Get(key.data(), key.size());
    if (iter == end()) {
        return false;
    }
    DeleteSubBucket(&iter);
    return true;
}

void BucketImpl::DeleteSubBucket(Iterator* iter) {
    if (!iter->is_bucket()) {
        throw InvalidArgumentError{ "attempt to delete a key value pair that is not a sub bucket." };
    }
    auto& sub_bucket = SubBucket(iter->key(), true);
    auto map_iter = sub_bucket_map_->find({ iter->key().data(), iter->key().size() });
    do  {
        auto first = sub_bucket.begin();
        if (first == sub_bucket.end()) {
            break;
        }
        if (first.is_bucket()) {
            auto map_iter = sub_bucket.sub_bucket_map_->find({ first->key().data(), first->key().size() });
            DeleteSubBucket(&first);
            tx_->DeleteSubBucket(map_iter->second.first);
            sub_bucket.sub_bucket_map_->erase(map_iter);
            auto invalid_pgid = kPageInvalidId;
            sub_bucket.Update(&first, &invalid_pgid, sizeof(invalid_pgid));
        }
        sub_bucket.Delete(&first);
    } while (true);
    auto pgid = map_iter->second.second;
    if (pgid != kPageInvalidId) {
        pager().Free(map_iter->second.second, 1);
    }
}

BucketImpl::Iterator BucketImpl::begin() noexcept {
    return Iterator(btree_.begin());
}

BucketImpl::Iterator BucketImpl::end() noexcept {
    return Iterator(btree_.end());
}

//void BucketImpl::Print(bool str) { btree_.Print(str); }

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


UpdateBucket::~UpdateBucket() = default;

UpdateBucket UpdateBucket::SubUpdateBucket(std::string_view key) {
    return UpdateBucket(&bucket_->SubBucket(key, true));
}

bool UpdateBucket::DeleteSubBucket(std::string_view key) {
    return bucket_->DeleteSubBucket(key);
}

void UpdateBucket::Put(const void* key_buf, size_t key_size, const void* value_buf, size_t value_size) {
    bucket_->Put(key_buf, key_size, value_buf, value_size, false);
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