#include "tx.h"

#include "txer.h"
#include "db.h"

namespace yudb {

Tx::Tx(Txer* txer, const Meta& meta) :
    txer_{ txer }, 
    meta_{ meta } {}

Bucket Tx::Bucket(std::string_view bucket_name) {
    return Bucket{ txer_->db_->pager_.get(), this, meta_.root };
}

} // yudb