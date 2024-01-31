#include "bucket_impl.h"

#include "tx_manager.h"
#include "pager.h"

namespace yudb {

std::strong_ordering DefalutComparator(std::span<const uint8_t> key1, std::span<const uint8_t> key2) {
    auto res = std::memcmp(key1.data(), key2.data(), std::min(key1.size(), key2.size()));
    if (res == 0) {
        return std::strong_ordering::equal;
    } else if (res < 0) {
        return std::strong_ordering::less;
    } else {
        return std::strong_ordering::greater;
    }
}

BucketImpl::BucketImpl(TxImpl* tx, PageId* root_pgid, bool writable) :
    tx_{ tx },
    btree_{ this, root_pgid, DefalutComparator },
    writable_{ writable }
{
    auto free_size = pager().page_size() - (sizeof(NodeFormat) - sizeof(NodeFormat::body));

    max_leaf_ele_count_ = free_size / sizeof(NodeFormat::LeafElement);
    max_branch_ele_count_ = (free_size - sizeof(PageId)) / sizeof(NodeFormat::BranchElement);

    inlineable_ = false;
}

BucketImpl::BucketImpl(TxImpl* tx, std::span<const uint8_t> inline_bucket_data, bool writable) :
    tx_{ tx },
    btree_{ this, nullptr, DefalutComparator },
    writable_{ writable }
{
    auto free_size = pager().page_size() - (sizeof(NodeFormat) - sizeof(NodeFormat::body));
    max_leaf_ele_count_ = free_size / sizeof(NodeFormat::LeafElement);
    max_branch_ele_count_ = (free_size - sizeof(PageId)) / sizeof(NodeFormat::BranchElement);

    inlineable_ = true;
    inline_bucket_.Deserialize(inline_bucket_data);
}


BucketImpl::BucketImpl(BucketImpl&& right) noexcept :
    tx_{ nullptr },
    btree_{ std::move(right.btree_) },
    writable_{ right.writable_ },
    sub_bucket_map_{ std::move(right.sub_bucket_map_) },
    inlineable_{ right.inlineable_ },
    max_branch_ele_count_{ right.max_branch_ele_count_ },
    max_leaf_ele_count_{ right.max_leaf_ele_count_ }
{
    btree_.set_bucket(this);
}

void BucketImpl::operator=(BucketImpl&& right) noexcept {
    tx_ = nullptr;
    btree_ = std::move(right.btree_);
    btree_.set_bucket(this);
    writable_ = right.writable_;
    sub_bucket_map_ = std::move(right.sub_bucket_map_);
    inlineable_ = right.inlineable_;
    max_branch_ele_count_ = right.max_branch_ele_count_;
    max_leaf_ele_count_ = right.max_leaf_ele_count_;
}

BucketImpl& BucketImpl::SubBucket(std::string_view key, bool writable) {
    auto map_iter = sub_bucket_map_.find({ key.data(), key.size() });
    uint32_t index;
    if (map_iter == sub_bucket_map_.end()) {
        auto res = sub_bucket_map_.insert({ { key.data(), key.size() }, { 0, kPageInvalidId } });
        map_iter = res.first;
        auto iter = Get(key);
        if (iter == end()) {
            // 提前预留空间，以避免Commit时的Put触发分裂
            PageId pgid = kPageInvalidId;
            Put(key.data(), key.size(), &pgid, sizeof(pgid));
            iter = Get(key);
            iter.set_is_bucket();
            assert(iter.is_bucket());
        }
        else {
            auto data = iter->value();
            auto pgid = *reinterpret_cast<PageId*>(data.data());
            map_iter->second.second = pgid;
            if (!iter.is_bucket()) {
                throw std::runtime_error("This is not a bucket.");
            }
        }
        if (!iter.is_inline_bucket()) {
            index = tx_->NewSubBucket(&map_iter->second.second, writable);
        }
        //else {
        //    map_iter->second.second = kPageInvalidId;
        //    index = tx_->NewSubBucket({ bucket_info->data, data.size() - 1 }, writable);
        //}
        map_iter->second.first = index;
    }
    else {
        index = map_iter->second.first;
    }

    return tx_->AtSubBucket(index);
}


void BucketImpl::set_tx(TxImpl* tx) {
    tx_ = tx;
}

void BucketImpl::set_root_pgid(PageId* root_pgid) {
    btree_.set_root_pgid(root_pgid);
}

const Pager& BucketImpl::pager() const { return tx_->pager(); }

Pager& BucketImpl::pager() { return tx_->pager(); }


} // namespace yudb