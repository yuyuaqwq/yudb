#pragma once

#include <cstdint>

#include "noncopyable.h"
#include "meta_format.h"

namespace yudb {

class DBImpl;

class Meta : noncopyable {
public:
    Meta(DBImpl* db) : db_{ db } {};

    bool Load();
    void Save();
    void Switch() { meta_index_ = !meta_index_; }

    const auto& meta_format() const { return meta_format_; }
    auto& meta_format() { return meta_format_; }

private:
    DBImpl* db_;
    MetaFormat meta_format_{ 0 };
    uint32_t meta_index_{ 0 };
};

} // namespace yudb