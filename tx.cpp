#include "tx.h"

#include "txer.h"
#include "db.h"

namespace yudb {

Tx::Tx(Txer* txer, PageId& btree_root_pgid) :
    txer_{ txer }, 
    btree_{ std::make_unique<BTree>(txer->db_->pager_.get() , btree_root_pgid)} {}

} // yudb