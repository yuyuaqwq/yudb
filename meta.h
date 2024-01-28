#pragma once

#include <cstdint>

#include "noncopyable.h"
#include "meta_format.h"

namespace yudb {

class DB;

class Meta : noncopyable {
public:
    Meta(DB* db) : db_{ db } {};

    bool Load();

    void Save();

    MetaFormat& meta_format() { return meta_format_; }

private:
    DB* db_;
    MetaFormat meta_format_{ 0 };
    size_t meta_index_{ 0 };
};

} // namespace yudb