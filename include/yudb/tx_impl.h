//The MIT License(MIT)
//Copyright ?? 2024 https://github.com/yuyuaqwq
//
//Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files(the ¡°Software¡±), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and /or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
//
//The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
//THE SOFTWARE IS PROVIDED ¡°AS IS¡±, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

#pragma once

#include <cstdint>

#include <array>
#include <memory>
#include <string_view>

#include <yudb/noncopyable.h>
#include <yudb/tx_format.h>
#include <yudb/meta_format.h>
#include <yudb/bucket_impl.h>

namespace yudb {

class TxManager;

class TxImpl : noncopyable {
public:
    TxImpl(TxManager* tx_manager, const MetaStruct& meta, bool writable);
    ~TxImpl() = default;

    BucketId NewSubBucket(PageId* root_pgid, bool writable, Comparator comparator);
    BucketImpl& AtSubBucket(BucketId bucket_id);
    void DeleteSubBucket(BucketId bucket_id);

    void RollBack();
    void Commit();

    // Specifies whether the page needs to be copied.
    bool CopyNeeded(TxId txid) const;

    void AppendSubBucketLog(BucketId bucket_id, std::span<const uint8_t> key);
    void AppendPutLog(BucketId bucket_id, std::span<const uint8_t> key, std::span<const uint8_t> value, bool is_bucket);
    void AppendDeleteLog(BucketId bucket_id, std::span<const uint8_t> key);

    auto& user_bucket() { return user_bucket_; }
    auto& user_bucket() const { return user_bucket_; }
    auto& txid() const { return meta_format_.txid; }
    void set_txid(TxId txid) { meta_format_.txid = txid; }
    Pager& pager() const;
    auto& tx_manager() const { return *tx_manager_; }
    auto& meta_struct() const { return meta_format_; }
    auto& meta_struct() { return meta_format_; }
    auto& sub_bucket_cache() const { return sub_bucket_cache_; }
    auto& sub_bucket_cache() { return sub_bucket_cache_; }

protected:
    TxManager* const tx_manager_;
    MetaStruct meta_format_;

    const bool writable_;
    BucketImpl user_bucket_;
    std::vector<std::unique_ptr<BucketImpl>> sub_bucket_cache_;
};

} // namespace yudb