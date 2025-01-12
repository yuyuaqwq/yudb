//The MIT License(MIT)
//Copyright © 2024 https://github.com/yuyuaqwq
//
//Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files(the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and /or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
//
//The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
//THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

#pragma once

#include <optional>
#include <map>

#include <yudb/btree.h>
#include <yudb/bucket_iterator.h>

namespace yudb {

class TxImpl;

class BucketImpl : noncopyable {
public:
    using Iterator = BucketIterator;

public:
    BucketImpl(TxImpl* tx, BucketId bucket_id, PageId* root_pgid, bool writable, Comparator comparator);
    ~BucketImpl();

    bool Empty() const;
    Iterator Get(const void* key_buf, size_t key_size);
    Iterator LowerBound(const void* key_buf, size_t key_size);
    void Put(const void* key_buf, size_t key_size, const void* value_buf, size_t value_size, bool is_bucket);
    void Update(Iterator* iter, const void* value_buf, size_t value_size);
    bool Delete(const void* key_buf, size_t key_size);
    void Delete(Iterator* iter);

    BucketImpl& SubBucket(std::string_view key, bool writable);
    BucketImpl& SubBucket(Iterator* iter, bool writable);
    bool DeleteSubBucket(std::string_view key);
    void DeleteSubBucket(Iterator* iter);

    Iterator begin() noexcept;
    Iterator end() noexcept;

    //void Print(bool str = false);

    Pager& pager() const;
    auto& tx() const { return *tx_; }
    auto& writable() const { return writable_; }
    auto& btree() { return btree_; }
    auto& btree() const { return btree_; }
    bool has_sub_bucket_map() const { return sub_bucket_map_.has_value(); }
    auto& sub_bucket_map() const { assert(sub_bucket_map_.has_value()); return *sub_bucket_map_; }
    auto& sub_bucket_map() { assert(sub_bucket_map_.has_value()); return *sub_bucket_map_; }

protected:
    TxImpl* const tx_;
    BucketId bucket_id_;
    const bool writable_;
    BTree btree_;
    std::optional<std::map<std::string, std::pair<BucketId, PageId>>> sub_bucket_map_;
};

} // namespace yudb