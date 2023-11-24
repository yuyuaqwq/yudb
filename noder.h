#pragma once

#include "node.h"

namespace yudb {

class BTree;

class Noder {
public:
    Noder(Node* node) : node_{ node } { }

    void Init() {
        node_->element_count = 0;
        //node_->free_lits.front = sizeof(node_);

        //auto rem = page_->free_lits.front % 4;
        //if (rem != 0) {
        //    page_->free_lits.front += 4 - rem;
        //}
        //page_->free_lits.front_size = btree_->pager_->page_size() - page_->free_lits.front;

    }


private:
    BTree* btree_;
    Node* node_;
};  

} // namespace yudb