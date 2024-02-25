#include "db_impl.h"

namespace yudb{

 DB::~DB() = default;


 DBImpl::~DBImpl() {
     if (pager_.has_value()) {
         //pager_->SyncAllPage();
     }
 }

 UpdateTx DBImpl::Update() {
     return tx_manager_.Update();
 }
 ViewTx DBImpl::View() {
     return tx_manager_.View();
 }


std::unique_ptr<DB> DB::Open(const Options& options, std::string_view path) {
    auto db = std::make_unique<DBImpl>();
    db->options_.emplace(options);
    if (!db->file().Open(path, false)) {
        return {};
    }
    if (!db->meta().Load()) {
        return {};
    }
    db->pager_.emplace(db.get(), db->meta().meta_format().page_size);
    std::string log_path = path.data();
    log_path += "-wal";
    db->log_writer().Open(log_path);
    return db;
}

} // namespace yudb