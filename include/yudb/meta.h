#pragma once

#include <cstdint>

#include "yudb/meta_format.h"
#include "yudb/noncopyable.h"

namespace yudb {

class DBImpl;

class Meta : noncopyable {
public:
    Meta(DBImpl* db);
    ~Meta() = default;

    bool Load();
    void Save();
    void Switch();

    const auto& meta_format() const { return meta_struct_; }
    auto& meta_format() { return meta_struct_; }

private:
    DBImpl* const db_;
    MetaStruct meta_struct_{ 0 };
    uint32_t cur_meta_index_{ 0 };
};

} // namespace yudb