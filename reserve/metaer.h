#pragma once

#include <cstdint>

#include "noncopyable.h"
#include "meta.h"

namespace yudb {

class Db;

class Metaer : noncopyable {
public:
    Metaer(Db* db) : db_{ db } {};

    bool Load();

    void Save();

    Meta& meta() { return meta_; }

private:
    Db* db_;
    Meta meta_{ 0 };
    size_t meta_index_{ 0 };
};

} // namespace yudb