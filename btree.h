#pragma once

#include "page.h"
#include "pager.h"

namespace yudb {

/*
* b+tree
* |        |
*/

constexpr size_t kDataSize = sizeof(Data);

class BTree {
public:
    class Node {
    public:
        Node(Page* page) : page_{page} { }

        void Init() {
            page_->element_count = 0;
            page_->free_lits.front = sizeof(Page);

            auto rem = page_->free_lits.front % 4;
            if (rem != 0) {
                page_->free_lits.front += 4 - rem;
            }
            page_->free_lits.front_size = btree_->pager_->page_size() - page_->free_lits.front;

        }

        Page::Index* AllocIndex() {
            return &page_->index[page_->element_count++];
        }

        Page::Leaf* AllocLeaf() {
            return &page_->leaf[page_->element_count++];
        }



        PageOffset AllocBlock(uint16_t size) {
            PageOffset ;
            do {
                page_->data[];
            } while (false);
        }

    private:
        BTree* btree_;
        Page* page_;
    };

public:


private:
    Pager* pager_;
    // Tx* tx_;

    PageId root_; // PageId&

};

} // namespace yudb