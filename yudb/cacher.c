#include "yudb/cacher.h"

#include "yudb/pager.h"
#include "yudb/yudb.h"

#include <CUtils/algorithm/hash_map.h>

const CacheId kCacheInvalidId = -1;

#define YUDB_CACHE_LRU_LIST_ACCESSOR_GetKey(LIST, ENTRY) (ObjectGetFromField(&ENTRY, CacheInfo, lru_entry)->pgid)
#define YUDB_CACHE_LRU_LIST_ACCESSOR YUDB_CACHE_LRU_LIST_ACCESSOR
//CUTILS_CONTAINER_LRU_LIST_DEFINE(Cache, PageId, YUDB_CACHE_LRU_LIST_ACCESSOR, CUTILS_OBJECT_ALLOCATOR_DEFALUT, Hashmap_hashint, CUTILS_OBJECT_COMPARER_DEFALUT)
__forceinline PageId CacheLruHashEntryAccessor_GetKey(CacheLruListHashTable* table, CacheLruHashEntry hash_entry) {
	return (((CacheInfo*)((uintptr_t)(&*(hash_entry.lru_entry)) - ((int)&(((CacheInfo*)0)->lru_entry))))->pgid);
}
static CacheLruListHashLinkStaticList* CacheLruListHashLinkGetStaticList(CacheLruListHashLinkVector* link_vector) {
	return (CacheLruListHashLinkStaticList*)((uintptr_t)&link_vector->obj_arr[1] - sizeof(CacheLruListHashLinkStaticList));
}
void CacheLruListHashLinkStaticListInit(CacheLruListHashLinkStaticList* list, int32_t count) {
	list->list_first[0] = 0;
	int32_t i = 0;
	for (; i < count - 1; i++) {
		(list->obj_arr[i].next = i + 1);
	}
	(list->obj_arr[i].next = (-1));
	for (i = 1; i < 1; i++) {
		list->list_first[i] = (-1);
	}
}
void CacheLruListHashLinkStaticListExpand(CacheLruListHashLinkStaticList* list, int32_t old_count, int32_t new_count) {
	int32_t old_first = list->list_first[0];
	list->list_first[0] = new_count - 1;
	int32_t i = old_count;
	for (; i < new_count - 1; i++) {
		(list->obj_arr[i].next = i + 1);
	}
	(list->obj_arr[i].next = old_first);
}
int32_t CacheLruListHashLinkStaticListPop(CacheLruListHashLinkStaticList* list, int32_t list_order) {
	if (list->list_first[list_order] == (-1)) {
		return (-1);
	}
	int32_t index = list->list_first[list_order];
	list->list_first[list_order] = (list->obj_arr[index].next);
	return index;
}
void CacheLruListHashLinkStaticListPush(CacheLruListHashLinkStaticList* list, int32_t list_order, int32_t index) {
	(list->obj_arr[index].next = list->list_first[list_order]);
	list->list_first[list_order] = index;
}
int32_t CacheLruListHashLinkStaticListDelete(CacheLruListHashLinkStaticList* list, int32_t list_order, int32_t prev_id, int32_t delete_id) {
	if (prev_id == (-1)) {
		list->list_first[list_order] = (list->obj_arr[delete_id].next);
	}
	else {
		(list->obj_arr[prev_id].next = (list->obj_arr[delete_id].next));
	}
	return delete_id;
}
void CacheLruListHashLinkStaticListSwitch(CacheLruListHashLinkStaticList* list, int32_t list_order, int32_t prev_id, int32_t id, int32_t new_list_order) {
	CacheLruListHashLinkStaticListDelete(list, list_order, prev_id, id);
	CacheLruListHashLinkStaticListPush(list, new_list_order, id);
}
int32_t CacheLruListHashLinkStaticListIteratorFirst(CacheLruListHashLinkStaticList* list, int32_t list_order) {
	return list->list_first[list_order];
}
int32_t CacheLruListHashLinkStaticListIteratorNext(CacheLruListHashLinkStaticList* list, int32_t cur_id) {
	return (list->obj_arr[cur_id].next);
}
__forceinline void CacheLruListHashLinkVectorCallbacker_Expand(CacheLruListHashLinkVector* arr, size_t old_capacity, size_t new_capacity) {
	CacheLruListHashLinkStaticListExpand(CacheLruListHashLinkGetStaticList(arr), old_capacity, new_capacity);
}
void CacheLruListHashLinkVectorResetCapacity(CacheLruListHashLinkVector* arr, size_t capacity) {
	CacheLruListHashLinkEntry* new_buf = ((CacheLruListHashLinkEntry*)MemoryAlloc(sizeof(CacheLruListHashLinkEntry) * (capacity)));
	if (arr->obj_arr) {
		memcpy((void*)(new_buf), (void*)(arr->obj_arr), (sizeof(CacheLruListHashLinkEntry) * arr->count));
		(MemoryFree(arr->obj_arr));
	}
	arr->obj_arr = new_buf;
	arr->capacity = capacity;
}
void CacheLruListHashLinkVectorExpand(CacheLruListHashLinkVector* arr, size_t add_count) {
	size_t old_capacity = arr->capacity;
	size_t cur_capacity = old_capacity;
	size_t target_count = cur_capacity + add_count;
	if (cur_capacity == 0) {
		cur_capacity = 1;
	}
	while (cur_capacity < target_count) {
		cur_capacity *= 2;
	}
	CacheLruListHashLinkVectorResetCapacity(arr, cur_capacity);
	CacheLruListHashLinkVectorCallbacker_Expand(arr, old_capacity, cur_capacity);
}
void CacheLruListHashLinkVectorInit(CacheLruListHashLinkVector* arr, size_t count, _Bool create) {
	arr->count = count;
	arr->obj_arr = ((void*)0);
	if (count != 0 && create) {
		CacheLruListHashLinkVectorResetCapacity(arr, count);
	}
	else {
		arr->capacity = count;
	}
}
void CacheLruListHashLinkVectorRelease(CacheLruListHashLinkVector* arr) {
	if (arr->obj_arr) {
		(MemoryFree(arr->obj_arr));
		arr->obj_arr = ((void*)0);
	}
	arr->capacity = 0;
	arr->count = 0;
}
ptrdiff_t CacheLruListHashLinkVectorPushTail(CacheLruListHashLinkVector* arr, const CacheLruListHashLinkEntry* obj) {
	if (arr->capacity <= arr->count) {
		CacheLruListHashLinkVectorExpand(arr, 1);
	}
	memcpy((void*)(&arr->obj_arr[arr->count++]), (void*)(obj), (sizeof(CacheLruListHashLinkEntry)));
	return arr->count - 1;
}
CacheLruListHashLinkEntry* CacheLruListHashLinkVectorPopTail(CacheLruListHashLinkVector* arr) {
	if (arr->count == 0) {
		return ((void*)0);
	}
	return &arr->obj_arr[--arr->count];
}
void CacheLruListHashBucketVectorResetCapacity(CacheLruListHashBucketVector* arr, size_t capacity) {
	CacheLruListHashEntry* new_buf = ((CacheLruListHashEntry*)MemoryAlloc(sizeof(CacheLruListHashEntry) * (capacity)));
	if (arr->obj_arr) {
		memcpy((void*)(new_buf), (void*)(arr->obj_arr), (sizeof(CacheLruListHashEntry) * arr->count));
		(MemoryFree(arr->obj_arr));
	}
	arr->obj_arr = new_buf;
	arr->capacity = capacity;
}
void CacheLruListHashBucketVectorExpand(CacheLruListHashBucketVector* arr, size_t add_count) {
	size_t old_capacity = arr->capacity;
	size_t cur_capacity = old_capacity;
	size_t target_count = cur_capacity + add_count;
	if (cur_capacity == 0) {
		cur_capacity = 1;
	}
	while (cur_capacity < target_count) {
		cur_capacity *= 2;
	}
	CacheLruListHashBucketVectorResetCapacity(arr, cur_capacity);
	;
}
void CacheLruListHashBucketVectorInit(CacheLruListHashBucketVector* arr, size_t count, _Bool create) {
	arr->count = count;
	arr->obj_arr = ((void*)0);
	if (count != 0 && create) {
		CacheLruListHashBucketVectorResetCapacity(arr, count);
	}
	else {
		arr->capacity = count;
	}
}
void CacheLruListHashBucketVectorRelease(CacheLruListHashBucketVector* arr) {
	if (arr->obj_arr) {
		(MemoryFree(arr->obj_arr));
		arr->obj_arr = ((void*)0);
	}
	arr->capacity = 0;
	arr->count = 0;
}
ptrdiff_t CacheLruListHashBucketVectorPushTail(CacheLruListHashBucketVector* arr, const CacheLruListHashEntry* obj) {
	if (arr->capacity <= arr->count) {
		CacheLruListHashBucketVectorExpand(arr, 1);
	}
	memcpy((void*)(&arr->obj_arr[arr->count++]), (void*)(obj), (sizeof(CacheLruListHashEntry)));
	return arr->count - 1;
}
CacheLruListHashEntry* CacheLruListHashBucketVectorPopTail(CacheLruListHashBucketVector* arr) {
	if (arr->count == 0) {
		return ((void*)0);
	}
	return &arr->obj_arr[--arr->count];
}
static const int32_t CacheLruListHashLinkRbReferencer_InvalidId = (-1);
__forceinline CacheLruListHashLinkRbEntry* CacheLruListHashLinkRbReferencer_Reference(CacheLruListHashLinkRbTree* tree, int32_t entry_id) {
	if (entry_id == CacheLruListHashLinkRbReferencer_InvalidId) {
		return ((void*)0);
	}
	CacheLruListHashLinkRbObj* rb_obj = (CacheLruListHashLinkRbObj*)tree;
	return &rb_obj->table->link.obj_arr[entry_id + 1].rb_entry;
}
__forceinline void CacheLruListHashLinkRbReferencer_Dereference(CacheLruListHashLinkRbTree* tree, CacheLruListHashLinkRbEntry* entry) {
}
typedef struct {
	int32_t color : 1;
	int32_t parent : sizeof(int32_t) * 8 - 1;
}
CacheLruListHashLinkRbParentColor;
__forceinline PageId CacheLruListHashLinkRbAccessor_GetKey(CacheLruListHashLinkRbTree* tree, CacheLruListHashLinkRbBsEntry* bs_entry) {
	CacheLruListHashLinkRbObj* rb_obj = (CacheLruListHashLinkRbObj*)tree;
	return CacheLruHashEntryAccessor_GetKey(rb_obj->table, ((CacheLruListHashLinkEntry*)bs_entry)->obj);
}
__forceinline int32_t CacheLruListHashLinkRbAccessor_GetParent(CacheLruListHashLinkRbTree* tree, CacheLruListHashLinkRbBsEntry* bs_entry) {
	return (((CacheLruListHashLinkRbParentColor*)&(((CacheLruListHashLinkRbEntry*)bs_entry)->parent_color))->parent);
}
__forceinline RbColor CacheLruListHashLinkRbAccessor_GetColor(CacheLruListHashLinkRbTree* tree, CacheLruListHashLinkRbBsEntry* bs_entry) {
	return (((CacheLruListHashLinkRbParentColor*)&(((CacheLruListHashLinkRbEntry*)bs_entry)->parent_color))->color == -1 ? 1 : 0);
}
__forceinline void CacheLruListHashLinkRbAccessor_SetParent(CacheLruListHashLinkRbTree* tree, CacheLruListHashLinkRbBsEntry* bs_entry, int32_t new_id) {
	((CacheLruListHashLinkRbParentColor*)&(((CacheLruListHashLinkRbEntry*)bs_entry)->parent_color))->parent = new_id;
}
__forceinline void CacheLruListHashLinkRbAccessor_SetColor(CacheLruListHashLinkRbTree* tree, CacheLruListHashLinkRbBsEntry* bs_entry, RbColor new_color) {
	return ((CacheLruListHashLinkRbParentColor*)&(((CacheLruListHashLinkRbEntry*)bs_entry)->parent_color))->color = new_color;
}
static void CacheLruListHashLinkRbBsTreeHitchEntry(CacheLruListHashLinkRbBsTree* tree, int32_t entry_id, int32_t new_entry_id) {
	CacheLruListHashLinkRbBsEntry* entry = CacheLruListHashLinkRbReferencer_Reference(tree, entry_id);
	CacheLruListHashLinkRbBsEntry* new_entry = CacheLruListHashLinkRbReferencer_Reference(tree, new_entry_id);
	if (CacheLruListHashLinkRbAccessor_GetParent(tree, entry) != CacheLruListHashLinkRbReferencer_InvalidId) {
		CacheLruListHashLinkRbBsEntry* entry_parent = CacheLruListHashLinkRbReferencer_Reference(tree, CacheLruListHashLinkRbAccessor_GetParent(tree, entry));
		if (entry_parent->left == entry_id) {
			entry_parent->left = new_entry_id;
		}
		else {
			entry_parent->right = new_entry_id;
		}
		CacheLruListHashLinkRbReferencer_Dereference(tree, entry_parent);
	}
	if (new_entry_id != CacheLruListHashLinkRbReferencer_InvalidId) {
		CacheLruListHashLinkRbAccessor_SetParent(tree, new_entry, CacheLruListHashLinkRbAccessor_GetParent(tree, entry));
	}
	if (tree->root == entry_id) {
		tree->root = new_entry_id;
	}
	CacheLruListHashLinkRbReferencer_Dereference(tree, entry);
	CacheLruListHashLinkRbReferencer_Dereference(tree, new_entry);
}
static int32_t CacheLruListHashLinkRbRotateLeft(CacheLruListHashLinkRbBsTree* tree, int32_t sub_root_id, CacheLruListHashLinkRbBsEntry* sub_root) {
	int32_t new_sub_root_id = sub_root->right;
	if (new_sub_root_id == CacheLruListHashLinkRbReferencer_InvalidId) {
		return sub_root_id;
	}
	CacheLruListHashLinkRbBsEntry* new_sub_root = CacheLruListHashLinkRbReferencer_Reference(tree, new_sub_root_id);
	CacheLruListHashLinkRbAccessor_SetParent(tree, new_sub_root, CacheLruListHashLinkRbAccessor_GetParent(tree, sub_root));
	if (CacheLruListHashLinkRbAccessor_GetParent(tree, sub_root) != CacheLruListHashLinkRbReferencer_InvalidId) {
		CacheLruListHashLinkRbBsEntry* sub_root_parent = CacheLruListHashLinkRbReferencer_Reference(tree, CacheLruListHashLinkRbAccessor_GetParent(tree, sub_root));
		if (sub_root_parent->left == sub_root_id) {
			sub_root_parent->left = new_sub_root_id;
		}
		else {
			sub_root_parent->right = new_sub_root_id;
		}
		CacheLruListHashLinkRbReferencer_Dereference(tree, sub_root_parent);
	}
	CacheLruListHashLinkRbAccessor_SetParent(tree, sub_root, new_sub_root_id);
	sub_root->right = new_sub_root->left;
	if (sub_root->right != CacheLruListHashLinkRbReferencer_InvalidId) {
		CacheLruListHashLinkRbBsEntry* sub_root_right = CacheLruListHashLinkRbReferencer_Reference(tree, sub_root->right);
		CacheLruListHashLinkRbAccessor_SetParent(tree, sub_root_right, sub_root_id);
		CacheLruListHashLinkRbReferencer_Dereference(tree, sub_root_right);
	}
	new_sub_root->left = sub_root_id;
	CacheLruListHashLinkRbReferencer_Dereference(tree, new_sub_root);
	return new_sub_root_id;
}
static int32_t CacheLruListHashLinkRbRotateRight(CacheLruListHashLinkRbBsTree* tree, int32_t sub_root_id, CacheLruListHashLinkRbBsEntry* sub_root) {
	int32_t new_sub_root_id = sub_root->left;
	if (new_sub_root_id == CacheLruListHashLinkRbReferencer_InvalidId) {
		return sub_root_id;
	}
	CacheLruListHashLinkRbBsEntry* new_sub_root = CacheLruListHashLinkRbReferencer_Reference(tree, new_sub_root_id);
	CacheLruListHashLinkRbAccessor_SetParent(tree, new_sub_root, CacheLruListHashLinkRbAccessor_GetParent(tree, sub_root));
	if (CacheLruListHashLinkRbAccessor_GetParent(tree, sub_root) != CacheLruListHashLinkRbReferencer_InvalidId) {
		CacheLruListHashLinkRbBsEntry* sub_root_parent = CacheLruListHashLinkRbReferencer_Reference(tree, CacheLruListHashLinkRbAccessor_GetParent(tree, sub_root));
		if (sub_root_parent->left == sub_root_id) {
			sub_root_parent->left = new_sub_root_id;
		}
		else {
			sub_root_parent->right = new_sub_root_id;
		}
		CacheLruListHashLinkRbReferencer_Dereference(tree, sub_root_parent);
	}
	CacheLruListHashLinkRbAccessor_SetParent(tree, sub_root, new_sub_root_id);
	sub_root->left = new_sub_root->right;
	if (sub_root->left != CacheLruListHashLinkRbReferencer_InvalidId) {
		CacheLruListHashLinkRbBsEntry* sub_root_left = CacheLruListHashLinkRbReferencer_Reference(tree, sub_root->left);
		CacheLruListHashLinkRbAccessor_SetParent(tree, sub_root_left, sub_root_id);
		CacheLruListHashLinkRbReferencer_Dereference(tree, sub_root_left);
	}
	new_sub_root->right = sub_root_id;
	CacheLruListHashLinkRbReferencer_Dereference(tree, new_sub_root);
	return new_sub_root_id;
}
static void CacheLruListHashLinkRbBsEntryInit(CacheLruListHashLinkRbBsTree* tree, CacheLruListHashLinkRbBsEntry* entry) {
	entry->left = CacheLruListHashLinkRbReferencer_InvalidId;
	entry->right = CacheLruListHashLinkRbReferencer_InvalidId;
	entry->parent = CacheLruListHashLinkRbReferencer_InvalidId;
}
void CacheLruListHashLinkRbBsTreeInit(CacheLruListHashLinkRbBsTree* tree) {
	tree->root = CacheLruListHashLinkRbReferencer_InvalidId;
}
int32_t CacheLruListHashLinkRbBsTreeFind(CacheLruListHashLinkRbBsTree* tree, PageId* key) {
	int8_t status;
	int32_t id = CacheLruListHashLinkRbBsTreeIteratorLocate(tree, key, &status);
	return status == 0 ? id : CacheLruListHashLinkRbReferencer_InvalidId;
}
void CacheLruListHashLinkRbBsTreeInsert(CacheLruListHashLinkRbBsTree* tree, int32_t entry_id) {
	CacheLruListHashLinkRbBsEntry* entry = CacheLruListHashLinkRbReferencer_Reference(tree, entry_id);
	CacheLruListHashLinkRbBsEntryInit(tree, entry);
	if (tree->root == CacheLruListHashLinkRbReferencer_InvalidId) {
		tree->root = entry_id;
		return;
	}
	int32_t cur_id = tree->root;
	while (cur_id != CacheLruListHashLinkRbReferencer_InvalidId) {
		CacheLruListHashLinkRbBsEntry* cur = CacheLruListHashLinkRbReferencer_Reference(tree, cur_id);
		if (((CacheLruListHashLinkRbAccessor_GetKey(tree, cur)) < (CacheLruListHashLinkRbAccessor_GetKey(tree, entry)))) {
			if (cur->right == CacheLruListHashLinkRbReferencer_InvalidId) {
				cur->right = entry_id;
				break;
			}
			cur_id = cur->right;
		}
		else {
			if (cur->left == CacheLruListHashLinkRbReferencer_InvalidId) {
				cur->left = entry_id;
				break;
			}
			cur_id = cur->left;
		}
		CacheLruListHashLinkRbReferencer_Dereference(tree, cur);
	}
	CacheLruListHashLinkRbAccessor_SetParent(tree, entry, cur_id);
	CacheLruListHashLinkRbReferencer_Dereference(tree, entry);
	return;
}
_Bool CacheLruListHashLinkRbBsTreePut(CacheLruListHashLinkRbBsTree* tree, int32_t entry_id) {
	CacheLruListHashLinkRbBsEntry* entry = CacheLruListHashLinkRbReferencer_Reference(tree, entry_id);
	CacheLruListHashLinkRbBsEntryInit(tree, entry);
	if (tree->root == CacheLruListHashLinkRbReferencer_InvalidId) {
		tree->root = entry_id;
		return 1;
	}
	int32_t cur_id = tree->root;
	while (cur_id != CacheLruListHashLinkRbReferencer_InvalidId) {
		CacheLruListHashLinkRbBsEntry* cur = CacheLruListHashLinkRbReferencer_Reference(tree, cur_id);
		if (((CacheLruListHashLinkRbAccessor_GetKey(tree, cur)) < (CacheLruListHashLinkRbAccessor_GetKey(tree, entry)))) {
			if (cur->right == CacheLruListHashLinkRbReferencer_InvalidId) {
				cur->right = entry_id;
				break;
			}
			cur_id = cur->right;
		}
		else if (((CacheLruListHashLinkRbAccessor_GetKey(tree, cur)) > (CacheLruListHashLinkRbAccessor_GetKey(tree, entry)))) {
			if (cur->left == CacheLruListHashLinkRbReferencer_InvalidId) {
				cur->left = entry_id;
				break;
			}
			cur_id = cur->left;
		}
		else {
			CacheLruListHashLinkRbReferencer_Dereference(tree, cur);
			return 0;
		}
		CacheLruListHashLinkRbReferencer_Dereference(tree, cur);
	}
	CacheLruListHashLinkRbAccessor_SetParent(tree, entry, cur_id);
	CacheLruListHashLinkRbReferencer_Dereference(tree, entry);
	return 1;
}
int32_t CacheLruListHashLinkRbBsTreeDelete(CacheLruListHashLinkRbBsTree* tree, int32_t entry_id, _Bool* is_parent_left) {
	int32_t backtrack_id;
	CacheLruListHashLinkRbBsEntry* entry = CacheLruListHashLinkRbReferencer_Reference(tree, entry_id);
	if (entry->left != CacheLruListHashLinkRbReferencer_InvalidId && entry->right != CacheLruListHashLinkRbReferencer_InvalidId) {
		int32_t min_entry_id = entry->right;
		CacheLruListHashLinkRbBsEntry* min_entry = CacheLruListHashLinkRbReferencer_Reference(tree, min_entry_id);
		while (min_entry->left != CacheLruListHashLinkRbReferencer_InvalidId) {
			min_entry_id = min_entry->left;
			CacheLruListHashLinkRbReferencer_Dereference(tree, min_entry);
			min_entry = CacheLruListHashLinkRbReferencer_Reference(tree, min_entry_id);
		}
		CacheLruListHashLinkRbBsEntry* min_entry_parent = CacheLruListHashLinkRbReferencer_Reference(tree, CacheLruListHashLinkRbAccessor_GetParent(tree, min_entry));
		if (is_parent_left) {
			*is_parent_left = min_entry_parent->left == min_entry_id;
		}
		min_entry->left = entry->left;
		if (entry->left != CacheLruListHashLinkRbReferencer_InvalidId) {
			CacheLruListHashLinkRbBsEntry* entry_left = CacheLruListHashLinkRbReferencer_Reference(tree, entry->left);
			CacheLruListHashLinkRbAccessor_SetParent(tree, entry_left, min_entry_id);
			CacheLruListHashLinkRbReferencer_Dereference(tree, entry_left);
		}
		int32_t old_right_id = min_entry->right;
		if (entry->right != min_entry_id) {
			min_entry_parent->left = min_entry->right;
			if (min_entry->right != CacheLruListHashLinkRbReferencer_InvalidId) {
				CacheLruListHashLinkRbBsEntry* min_entry_right = CacheLruListHashLinkRbReferencer_Reference(tree, min_entry->right);
				CacheLruListHashLinkRbAccessor_SetParent(tree, min_entry_right, CacheLruListHashLinkRbAccessor_GetParent(tree, min_entry));
				CacheLruListHashLinkRbReferencer_Dereference(tree, min_entry_right);
			}
			min_entry->right = entry->right;
			if (entry->right != CacheLruListHashLinkRbReferencer_InvalidId) {
				CacheLruListHashLinkRbBsEntry* entry_right = CacheLruListHashLinkRbReferencer_Reference(tree, entry->right);
				CacheLruListHashLinkRbAccessor_SetParent(tree, entry_right, min_entry_id);
				CacheLruListHashLinkRbReferencer_Dereference(tree, entry_right);
			}
			backtrack_id = CacheLruListHashLinkRbAccessor_GetParent(tree, min_entry);
		}
		else {
			backtrack_id = min_entry_id;
		}
		CacheLruListHashLinkRbReferencer_Dereference(tree, min_entry_parent);
		CacheLruListHashLinkRbBsTreeHitchEntry(tree, entry_id, min_entry_id);
		entry_id = min_entry_id;
		entry->left = CacheLruListHashLinkRbReferencer_InvalidId;
		entry->right = old_right_id;
		CacheLruListHashLinkRbAccessor_SetParent(tree, entry, backtrack_id);
	}
	else {
		if (is_parent_left) {
			int32_t parent_id = CacheLruListHashLinkRbAccessor_GetParent(tree, entry);
			if (parent_id != CacheLruListHashLinkRbReferencer_InvalidId) {
				CacheLruListHashLinkRbBsEntry* parent = CacheLruListHashLinkRbReferencer_Reference(tree, parent_id);
				*is_parent_left = parent->left == entry_id;
				CacheLruListHashLinkRbReferencer_Dereference(tree, parent);
			}
			else {
				*is_parent_left = 0;
			}
		}
		if (entry->right != CacheLruListHashLinkRbReferencer_InvalidId) {
			CacheLruListHashLinkRbBsTreeHitchEntry(tree, entry_id, entry->right);
		}
		else if (entry->left != CacheLruListHashLinkRbReferencer_InvalidId) {
			CacheLruListHashLinkRbBsTreeHitchEntry(tree, entry_id, entry->left);
		}
		else {
			CacheLruListHashLinkRbBsTreeHitchEntry(tree, entry_id, CacheLruListHashLinkRbReferencer_InvalidId);
		}
	}
	return entry_id;
}
size_t CacheLruListHashLinkRbBsTreeGetCount(CacheLruListHashLinkRbBsTree* tree) {
	size_t count = 0;
	int32_t cur_id = CacheLruListHashLinkRbBsTreeIteratorFirst(tree);
	while (cur_id != CacheLruListHashLinkRbReferencer_InvalidId) {
		count++;
		cur_id = CacheLruListHashLinkRbBsTreeIteratorNext(tree, cur_id);
	}
	return count;
}
int32_t CacheLruListHashLinkRbBsTreeIteratorLocate(CacheLruListHashLinkRbBsTree* tree, PageId* key, int8_t* cmp_status) {
	int32_t cur_id = tree->root;
	int32_t perv_id = CacheLruListHashLinkRbReferencer_InvalidId;
	while (cur_id != CacheLruListHashLinkRbReferencer_InvalidId) {
		perv_id = cur_id;
		CacheLruListHashLinkRbBsEntry* cur = CacheLruListHashLinkRbReferencer_Reference(tree, cur_id);
		if (((CacheLruListHashLinkRbAccessor_GetKey(tree, cur)) < (*key))) {
			*cmp_status = 1;
			cur_id = cur->right;
		}
		else if (((CacheLruListHashLinkRbAccessor_GetKey(tree, cur)) > (*key))) {
			*cmp_status = -1;
			cur_id = cur->left;
		}
		else {
			CacheLruListHashLinkRbReferencer_Dereference(tree, cur);
			*cmp_status = 0;
			return cur_id;
		}
		CacheLruListHashLinkRbReferencer_Dereference(tree, cur);
	}
	return perv_id;
}
int32_t CacheLruListHashLinkRbBsTreeIteratorFirst(CacheLruListHashLinkRbBsTree* tree) {
	int32_t cur_id = tree->root;
	if (cur_id == CacheLruListHashLinkRbReferencer_InvalidId) {
		return CacheLruListHashLinkRbReferencer_InvalidId;
	}
	CacheLruListHashLinkRbBsEntry* cur = CacheLruListHashLinkRbReferencer_Reference(tree, cur_id);
	while (cur->left != CacheLruListHashLinkRbReferencer_InvalidId) {
		cur_id = cur->left;
		CacheLruListHashLinkRbReferencer_Dereference(tree, cur);
		cur = CacheLruListHashLinkRbReferencer_Reference(tree, cur_id);
	}
	CacheLruListHashLinkRbReferencer_Dereference(tree, cur);
	return cur_id;
}
int32_t CacheLruListHashLinkRbBsTreeIteratorLast(CacheLruListHashLinkRbBsTree* tree) {
	int32_t cur_id = tree->root;
	if (cur_id == CacheLruListHashLinkRbReferencer_InvalidId) {
		return CacheLruListHashLinkRbReferencer_InvalidId;
	}
	CacheLruListHashLinkRbBsEntry* cur = CacheLruListHashLinkRbReferencer_Reference(tree, cur_id);
	while (cur->right != CacheLruListHashLinkRbReferencer_InvalidId) {
		cur_id = cur->right;
		CacheLruListHashLinkRbReferencer_Dereference(tree, cur);
		cur = CacheLruListHashLinkRbReferencer_Reference(tree, cur_id);
	}
	CacheLruListHashLinkRbReferencer_Dereference(tree, cur);
	return cur_id;
}
int32_t CacheLruListHashLinkRbBsTreeIteratorNext(CacheLruListHashLinkRbBsTree* tree, int32_t cur_id) {
	CacheLruListHashLinkRbBsEntry* cur = CacheLruListHashLinkRbReferencer_Reference(tree, cur_id);
	if (cur->right != CacheLruListHashLinkRbReferencer_InvalidId) {
		cur_id = cur->right;
		cur = CacheLruListHashLinkRbReferencer_Reference(tree, cur_id);
		while (cur->left != CacheLruListHashLinkRbReferencer_InvalidId) {
			cur_id = cur->left;
			CacheLruListHashLinkRbReferencer_Dereference(tree, cur);
			cur = CacheLruListHashLinkRbReferencer_Reference(tree, cur_id);
		}
		CacheLruListHashLinkRbReferencer_Dereference(tree, cur);
		return cur_id;
	}
	int32_t parent_id = CacheLruListHashLinkRbAccessor_GetParent(tree, cur);
	CacheLruListHashLinkRbBsEntry* parent = CacheLruListHashLinkRbReferencer_Reference(tree, parent_id);
	while (parent_id != CacheLruListHashLinkRbReferencer_InvalidId && cur_id == parent->right) {
		CacheLruListHashLinkRbReferencer_Dereference(tree, cur);
		cur = parent;
		cur_id = parent_id;
		parent_id = CacheLruListHashLinkRbAccessor_GetParent(tree, cur);
		parent = CacheLruListHashLinkRbReferencer_Reference(tree, parent_id);
	}
	CacheLruListHashLinkRbReferencer_Dereference(tree, cur);
	CacheLruListHashLinkRbReferencer_Dereference(tree, parent);
	return parent_id;
}
int32_t CacheLruListHashLinkRbBsTreeIteratorPrev(CacheLruListHashLinkRbBsTree* tree, int32_t cur_id) {
	CacheLruListHashLinkRbBsEntry* cur = CacheLruListHashLinkRbReferencer_Reference(tree, cur_id);
	if (cur->left != CacheLruListHashLinkRbReferencer_InvalidId) {
		cur_id = cur->left;
		cur = CacheLruListHashLinkRbReferencer_Reference(tree, cur_id);
		while (cur->right != CacheLruListHashLinkRbReferencer_InvalidId) {
			cur_id = cur->right;
			CacheLruListHashLinkRbReferencer_Dereference(tree, cur);
			cur = CacheLruListHashLinkRbReferencer_Reference(tree, cur_id);
		}
		CacheLruListHashLinkRbReferencer_Dereference(tree, cur);
		return cur_id;
	}
	int32_t parent_id = CacheLruListHashLinkRbAccessor_GetParent(tree, cur);
	CacheLruListHashLinkRbBsEntry* parent = CacheLruListHashLinkRbReferencer_Reference(tree, parent_id);
	while (parent_id != CacheLruListHashLinkRbReferencer_InvalidId && cur_id == parent->left) {
		CacheLruListHashLinkRbReferencer_Dereference(tree, cur);
		cur = parent;
		cur_id = parent_id;
		parent_id = CacheLruListHashLinkRbAccessor_GetParent(tree, cur);
		parent = CacheLruListHashLinkRbReferencer_Reference(tree, parent_id);
	}
	CacheLruListHashLinkRbReferencer_Dereference(tree, cur);
	CacheLruListHashLinkRbReferencer_Dereference(tree, parent);
	return parent_id;
}
static int32_t CacheLruListHashLinkGetSiblingEntry(CacheLruListHashLinkRbTree* tree, int32_t entry_id, CacheLruListHashLinkRbEntry* entry) {
	int32_t parent_id = CacheLruListHashLinkRbAccessor_GetParent(tree, entry);
	CacheLruListHashLinkRbEntry* parent = CacheLruListHashLinkRbReferencer_Reference(tree, parent_id);
	int32_t ret;
	if (parent->left == entry_id) {
		ret = parent->right;
	}
	else {
		ret = parent->left;
	}
	CacheLruListHashLinkRbReferencer_Dereference(tree, parent);
	return ret;
}
static void CacheLruListHashLinkRbTreeInsertFixup(CacheLruListHashLinkRbTree* tree, int32_t ins_entry_id) {
	CacheLruListHashLinkRbEntry* ins_entry = CacheLruListHashLinkRbReferencer_Reference(tree, ins_entry_id);
	CacheLruListHashLinkRbAccessor_SetColor(tree, ins_entry, kRbBlack);
	int32_t cur_id = CacheLruListHashLinkRbAccessor_GetParent(tree, ins_entry);
	if (cur_id == CacheLruListHashLinkRbReferencer_InvalidId) {
		CacheLruListHashLinkRbAccessor_SetColor(tree, ins_entry, kRbBlack);
		CacheLruListHashLinkRbReferencer_Dereference(tree, ins_entry);
		return;
	}
	CacheLruListHashLinkRbAccessor_SetColor(tree, ins_entry, kRbRed);
	CacheLruListHashLinkRbReferencer_Dereference(tree, ins_entry);
	CacheLruListHashLinkRbEntry* cur = ((void*)0);
	while (cur_id != CacheLruListHashLinkRbReferencer_InvalidId) {
		cur = CacheLruListHashLinkRbReferencer_Reference(tree, cur_id);
		if (CacheLruListHashLinkRbAccessor_GetColor(tree, cur) == kRbBlack) {
			break;
		}
		if (CacheLruListHashLinkRbAccessor_GetParent(tree, cur) == CacheLruListHashLinkRbReferencer_InvalidId) {
			CacheLruListHashLinkRbAccessor_SetColor(tree, cur, kRbBlack);
			break;
		}
		int32_t sibling_id = CacheLruListHashLinkGetSiblingEntry(tree, cur_id, cur);
		CacheLruListHashLinkRbEntry* sibling = CacheLruListHashLinkRbReferencer_Reference(tree, sibling_id);
		if (sibling_id != CacheLruListHashLinkRbReferencer_InvalidId && CacheLruListHashLinkRbAccessor_GetColor(tree, sibling) == kRbRed) {
			CacheLruListHashLinkRbAccessor_SetColor(tree, cur, kRbBlack);
			CacheLruListHashLinkRbAccessor_SetColor(tree, sibling, kRbBlack);
			ins_entry_id = CacheLruListHashLinkRbAccessor_GetParent(tree, cur);
			ins_entry = CacheLruListHashLinkRbReferencer_Reference(tree, ins_entry_id);
			if (CacheLruListHashLinkRbAccessor_GetParent(tree, ins_entry) == CacheLruListHashLinkRbReferencer_InvalidId) {
				CacheLruListHashLinkRbAccessor_SetColor(tree, ins_entry, kRbBlack);
				break;
			}
			CacheLruListHashLinkRbAccessor_SetColor(tree, ins_entry, kRbRed);
			cur = ins_entry;
		}
		else {
			{
				if (!(sibling_id == CacheLruListHashLinkRbReferencer_InvalidId || CacheLruListHashLinkRbAccessor_GetColor(tree, sibling) == kRbBlack)) {
					*(int*)0 = 0;
				}
			}
			;
			int32_t new_sub_root_id;
			int32_t old_sub_root_id = CacheLruListHashLinkRbAccessor_GetParent(tree, cur);
			CacheLruListHashLinkRbEntry* old_sub_root = CacheLruListHashLinkRbReferencer_Reference(tree, old_sub_root_id);
			if (old_sub_root->left == cur_id) {
				if (cur->right == ins_entry_id) {
					CacheLruListHashLinkRbRotateLeft(tree, cur_id, cur);
				}
				new_sub_root_id = CacheLruListHashLinkRbRotateRight(tree, old_sub_root_id, old_sub_root);
			}
			else {
				if (cur->left == ins_entry_id) {
					CacheLruListHashLinkRbRotateRight(tree, cur_id, cur);
				}
				new_sub_root_id = CacheLruListHashLinkRbRotateLeft(tree, old_sub_root_id, old_sub_root);
			}
			CacheLruListHashLinkRbEntry* new_sub_root = CacheLruListHashLinkRbReferencer_Reference(tree, new_sub_root_id);
			CacheLruListHashLinkRbAccessor_SetColor(tree, new_sub_root, kRbBlack);
			CacheLruListHashLinkRbAccessor_SetColor(tree, old_sub_root, kRbRed);
			CacheLruListHashLinkRbReferencer_Dereference(tree, new_sub_root);
			if (tree->root == old_sub_root_id) {
				tree->root = new_sub_root_id;
			}
			break;
		}
		cur_id = CacheLruListHashLinkRbAccessor_GetParent(tree, cur);
		CacheLruListHashLinkRbReferencer_Dereference(tree, cur);
	}
	CacheLruListHashLinkRbReferencer_Dereference(tree, cur);
}
static void CacheLruListHashLinkRbTreeDeleteFixup(CacheLruListHashLinkRbTree* tree, int32_t del_entry_id, _Bool is_parent_left) {
	CacheLruListHashLinkRbEntry* del_entry = CacheLruListHashLinkRbReferencer_Reference(tree, del_entry_id);
	int32_t cur_id = CacheLruListHashLinkRbReferencer_InvalidId;
	RbColor del_color = CacheLruListHashLinkRbAccessor_GetColor(tree, del_entry);
	if (del_color == kRbRed) {
	}
	else if (del_entry->left != CacheLruListHashLinkRbReferencer_InvalidId) {
		CacheLruListHashLinkRbEntry* del_entry_left = CacheLruListHashLinkRbReferencer_Reference(tree, del_entry->left);
		CacheLruListHashLinkRbAccessor_SetColor(tree, del_entry_left, kRbBlack);
		CacheLruListHashLinkRbReferencer_Dereference(tree, del_entry_left);
	}
	else if (del_entry->right != CacheLruListHashLinkRbReferencer_InvalidId) {
		CacheLruListHashLinkRbEntry* del_entry_right = CacheLruListHashLinkRbReferencer_Reference(tree, del_entry->right);
		CacheLruListHashLinkRbAccessor_SetColor(tree, del_entry_right, kRbBlack);
		CacheLruListHashLinkRbReferencer_Dereference(tree, del_entry_right);
	}
	else {
		cur_id = CacheLruListHashLinkRbAccessor_GetParent(tree, del_entry);
	}
	int32_t new_sub_root_id;
	CacheLruListHashLinkRbEntry* cur = ((void*)0), * sibling = ((void*)0);
	cur = CacheLruListHashLinkRbReferencer_Reference(tree, cur_id);
	while (cur_id != CacheLruListHashLinkRbReferencer_InvalidId) {
		int32_t sibling_id = is_parent_left ? cur->right : cur->left;
		sibling = CacheLruListHashLinkRbReferencer_Reference(tree, sibling_id);
		if (CacheLruListHashLinkRbAccessor_GetColor(tree, sibling) == kRbRed) {
			int32_t old_sub_root_id = CacheLruListHashLinkRbAccessor_GetParent(tree, sibling);
			CacheLruListHashLinkRbEntry* old_sub_root = CacheLruListHashLinkRbReferencer_Reference(tree, old_sub_root_id);
			CacheLruListHashLinkRbAccessor_SetColor(tree, old_sub_root, kRbRed);
			CacheLruListHashLinkRbAccessor_SetColor(tree, sibling, kRbBlack);
			if (old_sub_root->left == sibling_id) {
				new_sub_root_id = CacheLruListHashLinkRbRotateRight(tree, old_sub_root_id, old_sub_root);
				sibling_id = old_sub_root->left;
				CacheLruListHashLinkRbReferencer_Dereference(tree, sibling);
				sibling = CacheLruListHashLinkRbReferencer_Reference(tree, sibling_id);
			}
			else {
				new_sub_root_id = CacheLruListHashLinkRbRotateLeft(tree, old_sub_root_id, old_sub_root);
				sibling_id = old_sub_root->right;
				CacheLruListHashLinkRbReferencer_Dereference(tree, sibling);
				sibling = CacheLruListHashLinkRbReferencer_Reference(tree, sibling_id);
			}
			if (tree->root == old_sub_root_id) {
				tree->root = new_sub_root_id;
			}
			CacheLruListHashLinkRbReferencer_Dereference(tree, old_sub_root);
		}
		CacheLruListHashLinkRbEntry* sibling_right = CacheLruListHashLinkRbReferencer_Reference(tree, sibling->right);
		CacheLruListHashLinkRbEntry* sibling_left = CacheLruListHashLinkRbReferencer_Reference(tree, sibling->left);
		if (sibling->right != CacheLruListHashLinkRbReferencer_InvalidId && CacheLruListHashLinkRbAccessor_GetColor(tree, sibling_right) == kRbRed || sibling->left != CacheLruListHashLinkRbReferencer_InvalidId && CacheLruListHashLinkRbAccessor_GetColor(tree, sibling_left) == kRbRed) {
			RbColor parent_color = CacheLruListHashLinkRbAccessor_GetColor(tree, cur);
			CacheLruListHashLinkRbAccessor_SetColor(tree, cur, kRbBlack);
			int32_t old_sub_root_id = cur_id;
			if (cur->left == sibling_id) {
				if (sibling->left == CacheLruListHashLinkRbReferencer_InvalidId || CacheLruListHashLinkRbAccessor_GetColor(tree, sibling_left) == kRbBlack) {
					CacheLruListHashLinkRbAccessor_SetColor(tree, sibling_right, kRbBlack);
					sibling_id = CacheLruListHashLinkRbRotateLeft(tree, sibling_id, sibling);
				}
				else {
					CacheLruListHashLinkRbAccessor_SetColor(tree, sibling_left, kRbBlack);
				}
				new_sub_root_id = CacheLruListHashLinkRbRotateRight(tree, cur_id, cur);
			}
			else {
				if (sibling->right == CacheLruListHashLinkRbReferencer_InvalidId || CacheLruListHashLinkRbAccessor_GetColor(tree, sibling_right) == kRbBlack) {
					CacheLruListHashLinkRbAccessor_SetColor(tree, sibling_left, kRbBlack);
					sibling_id = CacheLruListHashLinkRbRotateRight(tree, sibling_id, sibling);
				}
				else {
					CacheLruListHashLinkRbAccessor_SetColor(tree, sibling_right, kRbBlack);
				}
				new_sub_root_id = CacheLruListHashLinkRbRotateLeft(tree, cur_id, cur);
			}
			CacheLruListHashLinkRbReferencer_Dereference(tree, sibling);
			sibling = CacheLruListHashLinkRbReferencer_Reference(tree, sibling_id);
			CacheLruListHashLinkRbAccessor_SetColor(tree, sibling, parent_color);
			if (tree->root == old_sub_root_id) {
				tree->root = new_sub_root_id;
			}
			CacheLruListHashLinkRbReferencer_Dereference(tree, sibling_right);
			CacheLruListHashLinkRbReferencer_Dereference(tree, sibling_left);
			break;
		}
		CacheLruListHashLinkRbReferencer_Dereference(tree, sibling_right);
		CacheLruListHashLinkRbReferencer_Dereference(tree, sibling_left);
		if (CacheLruListHashLinkRbAccessor_GetColor(tree, cur) == kRbRed) {
			CacheLruListHashLinkRbAccessor_SetColor(tree, sibling, kRbRed);
			CacheLruListHashLinkRbAccessor_SetColor(tree, cur, kRbBlack);
			break;
		}
		else {
			CacheLruListHashLinkRbAccessor_SetColor(tree, sibling, kRbRed);
		}
		int32_t child_id = cur_id;
		cur_id = CacheLruListHashLinkRbAccessor_GetParent(tree, cur);
		if (cur_id != CacheLruListHashLinkRbReferencer_InvalidId) {
			CacheLruListHashLinkRbReferencer_Dereference(tree, cur);
			cur = CacheLruListHashLinkRbReferencer_Reference(tree, cur_id);
			is_parent_left = cur->left == child_id;
		}
	}
	CacheLruListHashLinkRbReferencer_Dereference(tree, sibling);
	CacheLruListHashLinkRbReferencer_Dereference(tree, cur);
	CacheLruListHashLinkRbEntry* root = CacheLruListHashLinkRbReferencer_Reference(tree, tree->root);
	if (root && CacheLruListHashLinkRbAccessor_GetColor(tree, root) == kRbRed) {
		CacheLruListHashLinkRbAccessor_SetColor(tree, root, kRbBlack);
	}
	CacheLruListHashLinkRbReferencer_Dereference(tree, root);
}
void CacheLruListHashLinkRbTreeInit(CacheLruListHashLinkRbTree* tree) {
	CacheLruListHashLinkRbBsTreeInit(&tree->bs_tree);
}
int32_t CacheLruListHashLinkRbTreeFind(CacheLruListHashLinkRbTree* tree, PageId* key) {
	return CacheLruListHashLinkRbBsTreeFind(&tree->bs_tree, key);
}
void CacheLruListHashLinkRbTreeInsert(CacheLruListHashLinkRbTree* tree, int32_t insert_entry_id) {
	CacheLruListHashLinkRbBsTreeInsert(&tree->bs_tree, insert_entry_id);
	CacheLruListHashLinkRbTreeInsertFixup(tree, insert_entry_id);
}
_Bool CacheLruListHashLinkRbTreePut(CacheLruListHashLinkRbTree* tree, int32_t put_entry_id) {
	if (!CacheLruListHashLinkRbBsTreePut(&tree->bs_tree, put_entry_id)) {
		return 0;
	}
	CacheLruListHashLinkRbTreeInsertFixup(tree, put_entry_id);
	return 1;
}
_Bool CacheLruListHashLinkRbTreeDelete(CacheLruListHashLinkRbTree* tree, int32_t del_entry_id) {
	_Bool is_parent_left;
	int32_t del_min_entry_id = CacheLruListHashLinkRbBsTreeDelete(&tree->bs_tree, del_entry_id, &is_parent_left);
	if (del_min_entry_id == CacheLruListHashLinkRbReferencer_InvalidId) {
		return 0;
	}
	if (del_min_entry_id != del_entry_id) {
		CacheLruListHashLinkRbEntry* del_entry = CacheLruListHashLinkRbReferencer_Reference(tree, del_entry_id);
		CacheLruListHashLinkRbEntry* del_min_entry = CacheLruListHashLinkRbReferencer_Reference(tree, del_min_entry_id);
		;
		RbColor old_color = CacheLruListHashLinkRbAccessor_GetColor(tree, del_min_entry);
		CacheLruListHashLinkRbAccessor_SetColor(tree, del_min_entry, CacheLruListHashLinkRbAccessor_GetColor(tree, del_entry));
		CacheLruListHashLinkRbAccessor_SetColor(tree, del_entry, old_color);
	}
	CacheLruListHashLinkRbTreeDeleteFixup(tree, del_entry_id, is_parent_left);
	return 1;
}
int32_t CacheLruListHashLinkRbTreeIteratorLocate(CacheLruListHashLinkRbTree* tree, PageId* key, int8_t* cmp_status) {
	return CacheLruListHashLinkRbBsTreeIteratorLocate((CacheLruListHashLinkRbBsTree*)tree, key, cmp_status);
}
int32_t CacheLruListHashLinkRbTreeIteratorFirst(CacheLruListHashLinkRbTree* tree) {
	return CacheLruListHashLinkRbBsTreeIteratorFirst((CacheLruListHashLinkRbBsTree*)tree);
}
int32_t CacheLruListHashLinkRbTreeIteratorLast(CacheLruListHashLinkRbTree* tree) {
	return CacheLruListHashLinkRbBsTreeIteratorLast((CacheLruListHashLinkRbBsTree*)tree);
}
int32_t CacheLruListHashLinkRbTreeIteratorNext(CacheLruListHashLinkRbTree* tree, int32_t cur_id) {
	return CacheLruListHashLinkRbBsTreeIteratorNext((CacheLruListHashLinkRbBsTree*)tree, cur_id);
}
int32_t CacheLruListHashLinkRbTreeIteratorPrev(CacheLruListHashLinkRbTree* tree, int32_t cur_id) {
	return CacheLruListHashLinkRbBsTreeIteratorPrev((CacheLruListHashLinkRbBsTree*)tree, cur_id);
}
static __forceinline uint32_t CacheLruListHashGetIndex(CacheLruListHashTable* table, const PageId* key) {
	return Hashmap_hashint(table, *key) % table->bucket.capacity;
}
static __forceinline uint32_t CacheLruListHashGetCurrentLoadFator(CacheLruListHashTable* table) {
	return table->bucket.count * 100 / table->bucket.capacity;
}
static int32_t CacheLruListHashTableAllocTreeEntry(CacheLruListHashTable* table) {
	CacheLruListHashLinkStaticList* static_list = CacheLruListHashLinkGetStaticList(&table->link);
	int32_t id = CacheLruListHashLinkStaticListPop(static_list, 0);
	if (id == (-1)) {
	}
	return id;
}
static void CacheLruListHashTableFreeTreeEntry(CacheLruListHashTable* table, int32_t id) {
	CacheLruListHashLinkStaticList* static_list = CacheLruListHashLinkGetStaticList(&table->link);
	CacheLruListHashLinkStaticListPush(static_list, 0, id);
}
size_t CacheLruListHashTableGetCount(CacheLruListHashTable* table) {
	return table->bucket.count;
}
static void CacheLruListHashRehash(CacheLruListHashTable* table, size_t new_capacity) {
	CacheLruListHashTable temp_table;
	CacheLruListHashTableInit(&temp_table, new_capacity, table->load_fator);
	CacheLruListHashTableIterator iter;
	CacheLruHashEntry* obj = CacheLruListHashTableIteratorFirst(table, &iter);
	while (obj) {
		CacheLruListHashTablePut(&temp_table, obj);
		PageId key = CacheLruHashEntryAccessor_GetKey(table, *obj);
		obj = CacheLruListHashTableIteratorNext(table, &iter);
		CacheLruListHashTableDelete(table, &key);
	}
	CacheLruListHashBucketVectorRelease(&table->bucket);
	memcpy((void*)(table), (void*)(&temp_table), (sizeof(temp_table)));
}
void CacheLruListHashTableInit(CacheLruListHashTable* table, size_t capacity, uint32_t load_fator) {
	if (capacity == 0) {
		capacity = 16;
	}
	CacheLruListHashBucketVectorInit(&table->bucket, capacity, 1);
	CacheLruListHashLinkVectorInit(&table->link, capacity + 1, 1);
	CacheLruListHashLinkStaticListInit(CacheLruListHashLinkGetStaticList(&table->link), capacity);
	table->bucket.count = 0;
	for (int i = 0; i < table->bucket.capacity; i++) {
		CacheLruListHashLinkRbTreeInit(&table->bucket.obj_arr[i].rb_tree);
	}
	if (load_fator == 0) {
		load_fator = 75;
	}
	table->load_fator = load_fator;
}
void CacheLruListHashTableRelease(CacheLruListHashTable* table) {
	CacheLruListHashBucketVectorRelease(&table->bucket);
	CacheLruListHashLinkVectorRelease(&table->link);
}
CacheLruHashEntry* CacheLruListHashTableFind(CacheLruListHashTable* table, const PageId* key) {
	CacheLruListHashEntry* entry = &table->bucket.obj_arr[CacheLruListHashGetIndex(table, key)];
	CacheLruListHashLinkRbObj rb_obj;
	rb_obj.rb_tree = entry->rb_tree;
	rb_obj.table = table;
	int32_t rb_id = CacheLruListHashLinkRbTreeFind(&rb_obj.rb_tree, key);
	if (rb_id != CacheLruListHashLinkRbReferencer_InvalidId) {
		return &table->link.obj_arr[rb_id + 1].obj;
	}
	return ((void*)0);
}
_Bool CacheLruListHashTablePut(CacheLruListHashTable* table, const CacheLruHashEntry* obj) {
	PageId key = CacheLruHashEntryAccessor_GetKey(table, *obj);
	CacheLruListHashEntry* entry = &table->bucket.obj_arr[CacheLruListHashGetIndex(table, &key)];
	CacheLruListHashLinkRbObj rb_obj;
	rb_obj.rb_tree = entry->rb_tree;
	rb_obj.table = table;
	int32_t rb_id = CacheLruListHashLinkRbTreeFind(&rb_obj.rb_tree, &key);
	if (rb_id != CacheLruListHashLinkRbReferencer_InvalidId) {
		CacheLruListHashLinkRbTreeDelete(&rb_obj.rb_tree, rb_id);
		CacheLruListHashTableFreeTreeEntry(table, rb_id);
	}
	rb_id = CacheLruListHashTableAllocTreeEntry(table);
	((table->link.obj_arr[rb_id + 1].obj) = (*obj));
	CacheLruListHashLinkRbTreePut(&rb_obj.rb_tree, rb_id);
	entry->rb_tree = rb_obj.rb_tree;
	table->bucket.count++;
	if (CacheLruListHashGetCurrentLoadFator(table) >= table->load_fator) {
		CacheLruListHashRehash(table, table->bucket.capacity * 2);
	}
	return 1;
}
_Bool CacheLruListHashTableDelete(CacheLruListHashTable* table, const PageId* key) {
	CacheLruListHashEntry* entry = &table->bucket.obj_arr[CacheLruListHashGetIndex(table, key)];
	CacheLruListHashLinkRbObj rb_obj;
	rb_obj.rb_tree = entry->rb_tree;
	rb_obj.table = table;
	int32_t rb_id = CacheLruListHashLinkRbTreeFind(&rb_obj.rb_tree, key);
	if (rb_id == CacheLruListHashLinkRbReferencer_InvalidId) return 0;
	_Bool success = CacheLruListHashLinkRbTreeDelete(&rb_obj.rb_tree, rb_id);
	if (success) {
		table->bucket.count--;
		entry->rb_tree = rb_obj.rb_tree;
	}
	return success;
}
CacheLruHashEntry* CacheLruListHashTableIteratorFirst(CacheLruListHashTable* table, CacheLruListHashTableIterator* iter) {
	iter->cur_index = -1;
	iter->rb_cur_id = CacheLruListHashLinkRbReferencer_InvalidId;
	return CacheLruListHashTableIteratorNext(table, iter);
}
CacheLruHashEntry* CacheLruListHashTableIteratorNext(CacheLruListHashTable* table, CacheLruListHashTableIterator* iter) {
	CacheLruListHashLinkRbObj rb_obj;
	rb_obj.table = table;
	do {
		if (iter->rb_cur_id == CacheLruListHashLinkRbReferencer_InvalidId) {
			if ((int32_t)iter->cur_index >= (int32_t)table->bucket.capacity - 1) {
				break;
			}
			rb_obj.rb_tree = table->bucket.obj_arr[++iter->cur_index].rb_tree;
			iter->rb_cur_id = CacheLruListHashLinkRbTreeIteratorFirst(&rb_obj.rb_tree);
		}
		else {
			rb_obj.rb_tree = table->bucket.obj_arr[iter->cur_index].rb_tree;
			iter->rb_cur_id = CacheLruListHashLinkRbTreeIteratorNext(&rb_obj.rb_tree, iter->rb_cur_id);
		}
	} while (iter->rb_cur_id == CacheLruListHashLinkRbReferencer_InvalidId);
	if (iter->rb_cur_id == CacheLruListHashLinkRbReferencer_InvalidId) return ((void*)0);
	CacheLruListHashLinkRbEntry* entry = CacheLruListHashLinkRbReferencer_Reference(&rb_obj.rb_tree, iter->rb_cur_id);
	return &((CacheLruListHashLinkEntry*)entry)->obj;
}
void CacheLruListInit(CacheLruList* list, size_t max_count) {
	CacheLruListHashTableInit(&list->hash_table, max_count, 0);
	ListInit(&list->list_head);
	list->max_count = max_count;
}
CacheLruListEntry* CacheLruListGet(CacheLruList* list, PageId* key, _Bool put_first) {
	CacheLruHashEntry* hash_entry = CacheLruListHashTableFind(&list->hash_table, key);
	if (!hash_entry) {
		return ((void*)0);
	}
	if (put_first) {
		ListPutFirst(&list->list_head, ListDeleteEntry(&list->list_head, &hash_entry->lru_entry->list_entry));
	}
	return hash_entry->lru_entry;
}
CacheLruListEntry* CacheLruListPut(CacheLruList* list, CacheLruListEntry* entry) {
	PageId key = (((CacheInfo*)((uintptr_t)(&*entry) - ((int)&(((CacheInfo*)0)->lru_entry))))->pgid);
	CacheLruHashEntry* hash_entry = CacheLruListHashTableFind(&list->hash_table, &key);
	if (hash_entry) {
		CacheLruListDelete(list, &key);
	}
	else if (CacheLruListHashTableGetCount(&list->hash_table) >= list->max_count) {
		hash_entry = CacheLruListPop(list);
	}
	CacheLruHashEntry put_hash_entry;
	put_hash_entry.lru_entry = entry;
	CacheLruListHashTablePut(&list->hash_table, &put_hash_entry);
	ListPutFirst(&list->list_head, &entry->list_entry);
	return hash_entry ? hash_entry->lru_entry : ((void*)0);
}
CacheLruListEntry* CacheLruListPop(CacheLruList* list) {
	ListEntry* del_list_entry = ListDeleteLast(&list->list_head);
	PageId key = (((CacheInfo*)((uintptr_t)(&*(CacheLruListEntry*)del_list_entry) - ((int)&(((CacheInfo*)0)->lru_entry))))->pgid);
	CacheLruListHashTableDelete(&list->hash_table, &key);
	return (CacheLruListEntry*)del_list_entry;
}
CacheLruListEntry* CacheLruListDelete(CacheLruList* list, PageId* key) {
	CacheLruListEntry* lru_entry = ((void*)0);
	CacheLruHashEntry* del_hash_entry = CacheLruListHashTableFind(&list->hash_table, key);
	if (del_hash_entry) {
		lru_entry = del_hash_entry->lru_entry;
		ListDeleteEntry(&list->list_head, &del_hash_entry->lru_entry->list_entry);
		CacheLruListHashTableDelete(&list->hash_table, key);
	}
	return del_hash_entry ? lru_entry : ((void*)0);
}

#define YUDB_CACHER_DOUBLY_STATIC_LIST_ACCESSOR_GetNext(list, cache_info) ((cache_info).entry.next)
#define YUDB_CACHER_DOUBLY_STATIC_LIST_ACCESSOR_GetPrev(list, cache_info) ((cache_info).entry.prev)
#define YUDB_CACHER_DOUBLY_STATIC_LIST_ACCESSOR_SetNext(list, cache_info, new_next) ((cache_info).entry.next = (new_next))
#define YUDB_CACHER_DOUBLY_STATIC_LIST_ACCESSOR_SetPrev(list, cache_info, new_prev) ((cache_info).entry.prev = (new_prev))
#define YUDB_CACHER_DOUBLY_STATIC_LIST_ACCESSOR YUDB_CACHER_DOUBLY_STATIC_LIST_ACCESSOR

CUTILS_CONTAINER_DOUBLY_STATIC_LIST_DEFINE(Cache, int16_t, CacheInfo, CUTILS_CONTAINER_DOUBLY_STATIC_LIST_DEFAULT_REFERENCER, YUDB_CACHER_DOUBLY_STATIC_LIST_ACCESSOR, 3)


static inline CacheId CacherGetIdFromInfo(Cacher* cacher, CacheInfo* info) {
	return info - (CacheInfo*)cacher->cache_info_pool->obj_arr;
}

static void CacherEvict(Cacher* cacher, CacheId cache_id) {
	Pager* pager = ObjectGetFromField(cacher, Pager, cacher);
	CacheInfo* cache_info = CacherGetInfo(&pager->cacher, cache_id);
	  assert(cache_info->reference_count == 0);		// 被驱逐的缓存引用计数必须为0
	if (cache_info->type == kCacheListDirty) {
		// 是脏页则写回磁盘
		void* cache = CacherGet(&pager->cacher, cache_id);
		PagerWrite(pager, cache_info->pgid, cache, 1);
		// 会从缓存列表中移除，不需要再解引用了
	}
	CacherFree(&pager->cacher, cache_id);
}

void CacherInit(Cacher* cacher, size_t count) {
	Pager* pager = ObjectGetFromField(cacher, Pager, cacher);
	cacher->cache_pool = malloc(pager->page_size * count);
	cacher->cache_info_pool = malloc(sizeof(CacheDoublyStaticList) + sizeof(CacheInfo) * count);
	CacheDoublyStaticListInit(cacher->cache_info_pool, count);
	CacheLruListInit(&cacher->cache_lru_list, count);
}

/*
* 从缓存管理器中分配一页缓存
*/
CacheId CacherAlloc(Cacher* cacher, PageId pgid) {
	CacheId cache_id = CacheDoublyStaticListPop(cacher->cache_info_pool, kCacheListFree);
	CacheInfo* evict_cache_info;
	if (cache_id == kCacheInvalidId) {
		// 缓存已满，驱逐lur末尾缓存
		CacheLruListEntry* lru_entry = CacheLruListPop(&cacher->cache_lru_list);
		  assert(lru_entry != NULL);
		evict_cache_info = ObjectGetFromField(lru_entry, CacheInfo, lru_entry);
		CacherEvict(cacher, CacherGetIdFromInfo(cacher, evict_cache_info));
		cache_id = CacheDoublyStaticListPop(cacher->cache_info_pool, kCacheListFree);
		  assert(cache_id != kCacheInvalidId);
	}
	// 新分配的缓存挂到干净的链表上
	CacheDoublyStaticListPush(cacher->cache_info_pool, kCacheListClean, cache_id);
	// 同时挂到Lru中
	CacheInfo* cache_info = CacherGetInfo(cacher, cache_id);
	cache_info->pgid = pgid;
	cache_info->reference_count = 0;
	cache_info->type = kCacheListClean;
	CacheLruListEntry* lru_entry = CacheLruListPut(&cacher->cache_lru_list, &cache_info->lru_entry);
	  assert(lru_entry == NULL);		// Lru不应该还会驱逐
	evict_cache_info = ObjectGetFromField(lru_entry, CacheInfo, lru_entry);
	return cache_id;
}

void CacherFree(Cacher* cacher, CacheId cache_id) {
	// 原子比较等待引用计数归0
	CacheInfo* cache_info = CacherGetInfo(cacher, cache_id);
	CacheLruListDelete(&cacher->cache_lru_list, &cache_info->pgid);
	CacheDoublyStaticListSwitch(cacher->cache_info_pool, cache_info->type, cache_id, kCacheListFree);
	cache_info->type = kCacheListFree;
}

CacheId CacherFind(Cacher* cacher, PageId pgid, bool put_first) {
	CacheLruListEntry* lru_entry = CacheLruListGet(&cacher->cache_lru_list, &pgid, put_first);
	if (!lru_entry) {
		return kCacheInvalidId;
	}
	CacheInfo* cache_info = ObjectGetFromField(lru_entry, CacheInfo, lru_entry);
	return CacherGetIdFromInfo(cacher, cache_info);
}

void* CacherGet(Cacher* cacher, CacheId cache_id) {
	Pager* pager = ObjectGetFromField(cacher, Pager, cacher);
	CacheInfo* cache_info = CacherGetInfo(cacher, cache_id);
	cache_info->reference_count++;
	return (void*)(((uintptr_t)cacher->cache_pool) + cache_id * pager->page_size);
}

void CacherDereference(Cacher* cacher, CacheId cache_id) {
	CacheInfo* cache_info = CacherGetInfo(cacher, cache_id);
	cache_info->reference_count--;
}

inline CacheInfo* CacherGetInfo(Cacher* cacher, CacheId id) {
	return (CacheInfo*)&cacher->cache_info_pool->obj_arr[id];
}

CacheId CacherGetIdByBuf(Cacher* cacher, void* cache) {
	Pager* pager = ObjectGetFromField(cacher, Pager, cacher);
	uintptr_t offset = (uintptr_t)cache - (uintptr_t)cacher->cache_pool;
	return (CacheId)(offset / pager->page_size);
}

CacheId CacherGetIdByInfo(Cacher* cacher, CacheInfo* info) {
	Pager* pager = ObjectGetFromField(cacher, Pager, cacher);
	uintptr_t offset = (uintptr_t)info - (uintptr_t)cacher->cache_info_pool->obj_arr;
	return (CacheId)(offset / sizeof(CacheInfo));
}

PageId CacherGetPageIdById(Cacher* cacher, CacheId id) {
	CacheInfo* info = CacherGetInfo(cacher, id);
	return info->pgid;
}

void CacherMarkDirty(Cacher* cacher, CacheId cache_id) {
	CacheInfo* cache_info = CacherGetInfo(cacher, cache_id);
	if (cache_info->type != kCacheListDirty) {
		CacheDoublyStaticListSwitch(cacher->cache_info_pool, cache_info->type, cache_id, kCacheListDirty);
		cache_info->type = kCacheListDirty;
	}
}

