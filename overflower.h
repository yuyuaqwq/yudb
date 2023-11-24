#pragma once

#include "overflow.h"
#include "pager.h"

namespace yudb {

class Overflower {
public:
    Overflower(Pager* pager, Overflow* overflow) : pager_{ pager }, overflow_{ overflow } {}

    std::optional<std::pair<PageId, PageOffset>> Alloc(PageSize size, bool alloc_new_page = true) {
        auto page = pager_->Reference(overflow_->pgid);
        auto cache = page.page_cache();
        auto overflow_arr = reinterpret_cast<Overflow::Element*>(cache[overflow_->offset]);
        for (uint16_t i = 0; i < overflow_->element_count; i++) {
            if (overflow_arr[i].max_free_size >= size) {
                auto ret_page = pager_->Reference(overflow_arr[i].pgid);
                auto cache = ret_page.page_cache();
                cache[overflow_arr[i].free_list];
                

                return std::pair{ overflow_arr[i].pgid, 0 };
            }
        }
        if (!alloc_new_page) return {};

        PageOffset ret_offset = 0;

        // 当前的overflow数组中没有足够分配的空间，分配新页
        auto new_pgid = pager_->Alloc(1);
        auto new_page = pager_->Reference(overflow_->pgid);
        auto new_cache = page.page_cache();

        // 先扩展overflow数组
        auto new_overflow_arr_block_size = sizeof(Overflow::Element) * ++overflow_->element_count;
        auto new_overflow_arr_block = Alloc(new_overflow_arr_block_size, false);
        if (!new_overflow_arr_block) {
            // 当前的空闲overflow空间也不足以扩展overflow数组了，使用新分配的页存放
            Free(std::pair{ overflow_->pgid, overflow_->offset });
            overflow_->pgid = new_pgid;
            overflow_->offset = 0;

            memcpy(new_cache, overflow_arr, new_overflow_arr_block_size - sizeof(Overflow::Element));
            overflow_arr = reinterpret_cast<Overflow::Element*>(new_cache);
            auto& tail_overflow_ele = overflow_arr[overflow_->element_count];
            tail_overflow_ele.Init(
                new_pgid, 
                pager_->page_size() - new_overflow_arr_block_size,
                new_overflow_arr_block_size
            );

            // 若分配完overflow数组后，不足以分配新数据，则再次创建新页
            if (tail_overflow_ele.max_free_size < size) {
                tail_overflow_ele.max_free_size -= sizeof(Overflow::Element);
                tail_overflow_ele.free_list += sizeof(Overflow::Element);
                
                new_pgid = pager_->Alloc(1);
                new_page = pager_->Reference(overflow_->pgid);
                new_cache = page.page_cache();

                // 可以原地扩展
                overflow_arr[++overflow_->element_count].Init(
                    new_pgid,
                    pager_->page_size() - size,
                    size
                );

            }
            else {
                ret_offset = overflow_arr[overflow_->element_count].free_list;
            }
        }
        return std::pair{ new_pgid, ret_offset };
        
    }
    
    void Free(const std::pair<PageId, PageOffset>& block) {

    }

private:


private:
    Pager* pager_;
    Overflow* overflow_;
};

} // namespace yudb