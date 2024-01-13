#include "bucket.h"

#include "txer.h"
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

Bucket::Bucket(Pager* pager, Tx* tx, PageId* btree_root, bool writable) :
    pager_{ pager },
    tx_{ tx },
    btree_{ this, btree_root, DefalutComparator },
    writable_{ writable }
{
    auto free_size = pager_->page_size() - (sizeof(Node) - sizeof(Node::body));

    max_leaf_ele_count_ = free_size / sizeof(Node::LeafElement);
    max_branch_ele_count_ = (free_size - sizeof(PageId)) / sizeof(Node::BranchElement);

    inlineable_ = false;
}

Bucket::Bucket(Pager* pager, Tx* tx, std::span<const uint8_t> inline_bucket_data, bool writable) :
    pager_{ pager },
    tx_{ tx },
    btree_{ this, nullptr, DefalutComparator },
    writable_{ writable }
{
    auto free_size = pager_->page_size() - (sizeof(Node) - sizeof(Node::body));

    max_leaf_ele_count_ = free_size / sizeof(Node::LeafElement);
    max_branch_ele_count_ = (free_size - sizeof(PageId)) / sizeof(Node::BranchElement);

    inlineable_ = true;
    inline_bucket_.Deserialize(inline_bucket_data);
}


Bucket& Bucket::SubBucket(std::string_view key, bool writable) {
    auto map_iter = sub_bucket_map_.find(key.data());
    uint32_t index;
    if (map_iter == sub_bucket_map_.end()) {
        auto iter = Get(key);
        if (iter == end()) {
            auto res = sub_bucket_map_.insert({ key.data(), { 0, kPageInvalidId } });
            map_iter = res.first;
            // 提前预留空间，以避免Commit时的Put触发分裂
            PageId pgid = kPageInvalidId;
            Put(key.data(), key.size(), &pgid, sizeof(pgid));
        }
        else {
            auto data = iter->value();
            auto pgid = *reinterpret_cast<PageId*>(data.data());
            map_iter->second.second = pgid;
            if (!iter.is_bucket()) {
                throw std::runtime_error("not a sub bucket.");
            }
        }
        if (iter.is_bucket()) {
            index = tx_->NewSubBucket(&map_iter->second.second, writable);
        }
        //else {
        //    map_iter->second.second = kPageInvalidId;
        //    index = tx_->NewSubBucket({ bucket_info->data, data.size() - 1 }, writable);
        //}
    }
    else {
        index = map_iter->second.first;
    }
    return tx_->AsSubBucket(index);
}


} // namespace yudb