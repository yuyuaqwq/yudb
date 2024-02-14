#pragma once

#include <map>
#include <string>
#include <span>

#include "noncopyable.h"
#include "inline_bucket_iterator.h"
#include "page_format.h"

namespace yudb {
    
class BucketImpl;

class InlineBucket : noncopyable {
public:
    using Iterator = InlineBucketIterator;

    size_t SerializedSize();
    void Deserialize(std::span<const uint8_t>) {

    }

    Iterator Get(const void* key_buf, size_t key_size) const {
        return leaf_.find(reinterpret_cast<const char*>(key_buf));
    }
    void Put(const void* key_buf, size_t key_size, const void* value_buf, size_t value_size) {
        leaf_.insert({});
    }
    bool Delete(const void* key_buf, size_t key_size) {
        
    }

private:
    BucketImpl* bucket_;
    std::map<std::string, std::string> leaf_;
    size_t serialized_size_;
};

} // namespace yudb