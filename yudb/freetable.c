#include "yudb/freetable.h"


const uint16_t kFreeTableInvalidOffset = 0;

/*
* 初始化
*/
void Free1TableInit_(Free1Table_* table, int16_t size) {
	table->first_block = 4;
	Free1Block* block = (Free1Block*)((uintptr_t)table + 4);
	block->next_block_offset = kFreeTableInvalidOffset;
	block->len = size - 4;
}

/*
* 分配块，返回偏移
*/
uint16_t Free1TableAlloc_(Free1Table_* table, uint16_t len) {
	if (len & 0x3) len = (len & (~(uint16_t)0x3)) + 4;
	Free1Block* prev_block = (Free1Block*)((uintptr_t)&table->first_block);
	uint16_t free_offset = table->first_block;
	while (free_offset != kFreeTableInvalidOffset) {
		Free1Block* block = (Free1Block*)((uintptr_t)table + free_offset);
		if (block->len > len) {
			void* mem = (void*)(&block->next_block_offset);
			Free1Block* new_block = (Free1Block*)((uintptr_t)table + free_offset + len);
			new_block->next_block_offset = block->next_block_offset;
			new_block->len = block->len - len;
			prev_block->next_block_offset += len;
			return free_offset;
		}
		else if (block->len == len) {
			prev_block->next_block_offset = block->next_block_offset;
			return free_offset;
		}
		free_offset = block->next_block_offset;
		prev_block = block;
	};
	return kFreeTableInvalidOffset;
}

/*
* 释放块
*/
void Free1TableFree_(Free1Table_* page, uint16_t free_offset, uint16_t len) {
	if (len & 0x3) len = (len & (~(uint16_t)0x3)) + 4;
	Free1Block* free_block = (Free1Block*)((uintptr_t)page + free_offset);
	// 尝试合并与当前块连续的前后空闲块
	bool prev = false, next = false;
	uint16_t cur_offset = page->first_block;
	Free1Block* prev_block = (Free1Block*)((uintptr_t)&page->first_block);
	while (cur_offset != kFreeTableInvalidOffset) {
		Free1Block* cur_block = (Free1Block*)((uintptr_t)page + cur_offset);
		if (!next && free_offset + len == cur_offset) {
			// 找到连续的空闲下一块
			len += cur_block->len;
			free_block->len = len;
			free_block->next_block_offset = cur_block->next_block_offset;
			prev_block->next_block_offset = free_offset;
			cur_block = free_block;
			next = true;
		}
		else if (!prev && cur_offset + cur_block->len == free_offset) {
			// 找到连续的空闲上一块
			free_offset = cur_offset;
			len += cur_block->len;
			cur_block->len = len;
			free_block = cur_block;
			prev_block->next_block_offset = free_offset;
			prev = true;
		}
		else {
			// 没有合并才更新，找到连续的空闲下一块也不更新这块逻辑比较复杂
			// 主要为了使得下一次循环若找到连续的空闲上一块时，prev能够正确的指向合并后的free_block
			prev_block = cur_block;
		}
		if (prev && next) break;
		cur_offset = cur_block->next_block_offset;
	}
	if (!prev && !next) {
		free_block->next_block_offset = page->first_block;
		free_block->len = len;
		page->first_block = free_offset;
	}
}



/*
* 溢出头页面初始化
*/
//void OverflowHeadInit(Tx* tx, OverflowHead* head) {
//	Pager* pager = &tx->db->pager;
//	for (uint16_t i = 1; i < pager->page_size / sizeof(OverflowPageInfo); i++) {
//		head->overflow_page_info[i].overflow_pgid = kPagerPageInvalidId;
//	}
//}

/*
* 查找可供分配的OverflowPage，返回页面Id
*/
//PageId OverflowHeadFind(Tx* tx, OverflowHead* head, uint16_t size) {
//	Pager* pager = &tx->db->pager;
//	for (uint16_t i = 1; i < pager->page_size / sizeof(OverflowPageInfo); i++) {
//		OverflowPage* overflow_page;
//		if (head->overflow_page_info[i].overflow_pgid == kPagerPageInvalidId) {
//			head->overflow_page_info[i].overflow_pgid = PageAlloc(tx, true, 1);
//			overflow_page = (OverflowPage*)PageGet(tx, head->overflow_page_info[i].overflow_pgid, true);
//			OverflowPageInit(tx, overflow_page);
//			OverflowBlock* block = (OverflowBlock*)((uintptr_t)overflow_page + overflow_page->free_head);
//			head->overflow_page_info[i].overflow_max_free_block = block->len;
//		}
//		else {
//			overflow_page = (OverflowPage*)PageGet(tx, head->overflow_page_info[i].overflow_pgid, true);
//		}
//		if (head->overflow_page_info[i].overflow_max_free_block >= size) {
//			PageDereference(tx, head->overflow_page_info[i].overflow_pgid);
//			return head->overflow_page_info[i].overflow_pgid;
//		}
//		PageDereference(tx, head->overflow_page_info[i].overflow_pgid);
//	}
//	return kPagerPageInvalidId;
//}

/*
* 更新最大空闲块
*/
//void OverflowHeadUpdateMaxFree(Tx* tx, OverflowHead* head) {
//	OverflowBlock* prev_block = (OverflowBlock*)((uintptr_t)&page->free_head);
//	uint16_t free_offset = page->free_head;
//	uint16_t max = 0;
//	while (free_offset != kOverflowInvalidOffset) {
//		OverflowBlock* block = (OverflowBlock*)((uintptr_t)page + free_offset);
//		if (block->len > max) {
//			max = block->len;
//		}
//	}
//	CacheId cache_id = CacheGetId(&bucket->db->pager, page);
//	
//}



