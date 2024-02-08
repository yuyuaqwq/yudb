#include "db_impl.h"

namespace yudb{

std::unique_ptr<DB> DB::Open(std::string_view path) {
    auto db = std::make_unique<DBImpl>();
    if (!db->file().Open(path, false)) {
        return {};
    }
    if (!db->meta().Load()) {
        return {};
    }
    db->BuildPager(db.get(), db->meta().meta_format().page_size);
    return db;
}

} // namespace yudb