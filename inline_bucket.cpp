#include "inline_bucket.h"

#include "bucket_impl.h"

namespace yudb {

size_t InlineBucket::SerializedSize() {
    size_t size = sizeof(PageSize); // count
    for (auto& iter : leaf_) {
        size += sizeof(PageSize); // key_size
        size += iter.first.size();
        size += sizeof(PageSize); // value_size
        size += iter.second.size();
    }
    return size;
}

} // namespace yudb