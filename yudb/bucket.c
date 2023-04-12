#include <yudb/bucket.h>

#include <yudb/yudb.h>
#include <yudb/pager.h>

static inline Tx* BPlusTreeToTx(YuDbBPlusTree* tree) {
	Bucket* bucket = ObjectGetFromField(tree, Bucket, bp_tree);
	MetaInfo* meta_info = ObjectGetFromField(bucket, MetaInfo, bucket);
	Tx* tx = ObjectGetFromField(meta_info, Tx, meta_info);
	return tx;
}

/*
* B+树引用器
*/
#define YUDB_BUCKET_BPLUS_REFERENCER_InvalidId -1
inline YuDbBPlusEntry* YUDB_BUCKET_BPLUS_REFERENCER_Reference(YuDbBPlusTree* tree, PageId pgid) {
	Tx* tx = BPlusTreeToTx(tree);
	BucketEntry* entry = (BucketEntry*)PagerReference(&tx->db->pager, pgid);
	return &entry->bp_entry;
}
inline void YUDB_BUCKET_BPLUS_REFERENCER_Dereference(YuDbBPlusTree* tree, YuDbBPlusEntry* bp_entry) {
	Tx* tx = BPlusTreeToTx(tree);
	BucketEntry* entry = ObjectGetFromField(bp_entry, BucketEntry, bp_entry);
	PagerDereference(&tx->db->pager, entry);
}
#define YUDB_BUCKET_BPLUS_REFERENCER YUDB_BUCKET_BPLUS_REFERENCER

/*
* B+树分配器
*/
inline PageId YUDB_BUCKET_BPLUS_ALLOCATOR_CreateBySize(YuDbBPlusTree* tree, size_t size) {
	Tx* tx = BPlusTreeToTx(tree);
	PageId pgid = PagerAlloc(&tx->db->pager, true, 1);
	BucketEntry* entry = (BucketEntry*)PagerReference(&tx->db->pager, pgid);
	PagerMarkDirty(&tx->db->pager, entry);
	entry->last_write_tx_id = tx->meta_info.txid;
	PagerDereference(&tx->db->pager, entry);
	return pgid;
}
inline void YUDB_BUCKET_BPLUS_ALLOCATOR_Release(YuDbBPlusTree* tree, PageId pgid) {
	Tx* tx = BPlusTreeToTx(tree); 
	PagerFree(&tx->db->pager, pgid, 1);
}
#define YUDB_BUCKET_BPLUS_ALLOCATOR YUDB_BUCKET_BPLUS_ALLOCATOR

/*
* B+树访问器
*/
forceinline int32_t YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetKey(YuDbBPlusEntryRbTree* tree, YuDbBPlusEntryRbBsEntry* bs_entry) {
	if (((YuDbBPlusEntry*)tree)->type == kBPlusEntryLeaf) {
		return ((YuDbBPlusLeafElement*)bs_entry)->key;
	}
	else {
		return ((YuDbBPlusIndexElement*)bs_entry)->key;
	}
}
#define YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR
//CUTILS_CONTAINER_BPLUS_TREE_DEFINE(YuDb, PageId, int32_t, int32_t, CUTILS_OBJECT_ALLOCATOR_DEFALUT, YUDB_BUCKET_BPLUS_ALLOCATOR, YUDB_BUCKET_BPLUS_REFERENCER, YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR, CUTILS_OBJECT_COMPARER_DEFALUT)
static const PageId YuDbBPlusLeafEntryReferencer_InvalidId = -1;
__forceinline YuDbBPlusLeafListEntry* YuDbBPlusLeafEntryReferencer_Reference(YuDbBPlusLeafListHead* head, PageId entry_id) {
	YuDbBPlusTree* tree = ((YuDbBPlusTree*)((uintptr_t)(head)-((int)&(((YuDbBPlusTree*)0)->leaf_list))));
	YuDbBPlusEntry* entry = YUDB_BUCKET_BPLUS_REFERENCER_Reference(tree, entry_id);
	return &entry->leaf.list_entry;
}
__forceinline void YuDbBPlusLeafEntryReferencer_Dereference(YuDbBPlusLeafListHead* head, YuDbBPlusLeafListEntry* list_entry) {
	YuDbBPlusTree* tree = ((YuDbBPlusTree*)((uintptr_t)(head)-((int)&(((YuDbBPlusTree*)0)->leaf_list))));
	YuDbBPlusLeafEntry* entry = ((YuDbBPlusLeafEntry*)((uintptr_t)(list_entry)-((int)&(((YuDbBPlusLeafEntry*)0)->list_entry))));
	YUDB_BUCKET_BPLUS_REFERENCER_Dereference(tree, (YuDbBPlusEntry*)entry);
}
void YuDbBPlusLeafListInit(YuDbBPlusLeafListHead* head) {
	head->last = YuDbBPlusLeafEntryReferencer_InvalidId;
	head->first = YuDbBPlusLeafEntryReferencer_InvalidId;
}
void YuDbBPlusLeafListPutEntryNext(YuDbBPlusLeafListHead* head, PageId prev_id, PageId entry_id) {
	YuDbBPlusLeafListEntry* entry = YuDbBPlusLeafEntryReferencer_Reference(head, entry_id);
	YuDbBPlusLeafListEntry* prev = YuDbBPlusLeafEntryReferencer_Reference(head, prev_id);
	entry->prev = prev_id;
	entry->next = prev->next;
	YuDbBPlusLeafListEntry* prev_next = YuDbBPlusLeafEntryReferencer_Reference(head, prev->next);
	prev->next = entry_id;
	prev_next->prev = entry_id;
	if (head->last == prev_id) head->last = entry_id;
	YuDbBPlusLeafEntryReferencer_Dereference(head, prev_next);
	YuDbBPlusLeafEntryReferencer_Dereference(head, prev);
	YuDbBPlusLeafEntryReferencer_Dereference(head, entry);
}
void YuDbBPlusLeafListPutFirst(YuDbBPlusLeafListHead* head, PageId entry_id) {
	YuDbBPlusLeafListEntry* entry = YuDbBPlusLeafEntryReferencer_Reference(head, entry_id);
	PageId old_first_id = head->first;
	head->first = entry_id;
	if (old_first_id == YuDbBPlusLeafEntryReferencer_InvalidId) {
		{
			if (!(head->last == YuDbBPlusLeafEntryReferencer_InvalidId)) {
				*(int*)0 = 0;
			}
		}
		;
		entry->prev = entry_id;
		entry->next = entry_id;
		head->last = entry_id;
	}
	else {
		YuDbBPlusLeafListEntry* old_first = YuDbBPlusLeafEntryReferencer_Reference(head, old_first_id);
		entry->prev = old_first->prev;
		entry->next = old_first_id;
		YuDbBPlusLeafListEntry* last = YuDbBPlusLeafEntryReferencer_Reference(head, old_first->prev);
		old_first->prev = entry_id;
		last->next = entry_id;
		YuDbBPlusLeafEntryReferencer_Dereference(head, last);
		YuDbBPlusLeafEntryReferencer_Dereference(head, old_first);
	}
	YuDbBPlusLeafEntryReferencer_Dereference(head, entry);
}
void YuDbBPlusLeafListPutLast(YuDbBPlusLeafListHead* head, PageId entry_id) {
	YuDbBPlusLeafListEntry* entry = YuDbBPlusLeafEntryReferencer_Reference(head, entry_id);
	PageId old_last_id = head->last;
	head->last = entry_id;
	if (old_last_id == YuDbBPlusLeafEntryReferencer_InvalidId) {
		{
			if (!(head->first == YuDbBPlusLeafEntryReferencer_InvalidId)) {
				*(int*)0 = 0;
			}
		}
		;
		entry->prev = entry_id;
		entry->next = entry_id;
		head->first = entry_id;
	}
	else {
		YuDbBPlusLeafListEntry* old_last = YuDbBPlusLeafEntryReferencer_Reference(head, old_last_id);
		entry->prev = old_last_id;
		entry->next = old_last->next;
		YuDbBPlusLeafListEntry* first = YuDbBPlusLeafEntryReferencer_Reference(head, old_last->next);
		old_last->next = entry_id;
		first->prev = entry_id;
		YuDbBPlusLeafEntryReferencer_Dereference(head, first);
		YuDbBPlusLeafEntryReferencer_Dereference(head, old_last);
	}
	YuDbBPlusLeafEntryReferencer_Dereference(head, entry);
}
PageId YuDbBPlusLeafListDeleteEntry(YuDbBPlusLeafListHead* head, PageId entry_id) {
	if (head->first == head->last) {
		entry_id = head->first;
		YuDbBPlusLeafListInit(head);
		return entry_id;
	}
	YuDbBPlusLeafListEntry* entry = YuDbBPlusLeafEntryReferencer_Reference(head, entry_id);
	PageId prev_id = entry->prev;
	PageId next_id = entry->next;
	YuDbBPlusLeafListEntry* prev = YuDbBPlusLeafEntryReferencer_Reference(head, prev_id);
	YuDbBPlusLeafListEntry* next = YuDbBPlusLeafEntryReferencer_Reference(head, next_id);
	prev->next = next_id;
	next->prev = prev_id;
	YuDbBPlusLeafEntryReferencer_Dereference(head, prev);
	YuDbBPlusLeafEntryReferencer_Dereference(head, next);
	if (entry_id == head->first) {
		head->first = next_id;
	}
	else if (entry_id == head->last) {
		head->last = prev_id;
	}
	YuDbBPlusLeafEntryReferencer_Dereference(head, entry);
	return entry_id;
}
PageId YuDbBPlusLeafListDeleteFirst(YuDbBPlusLeafListHead* head) {
	return YuDbBPlusLeafListDeleteEntry(head, head->first);
}
PageId YuDbBPlusLeafListDeleteLast(YuDbBPlusLeafListHead* head) {
	return YuDbBPlusLeafListDeleteEntry(head, head->last);
}
void YuDbBPlusLeafListReplaceEntry(YuDbBPlusLeafListHead* head, PageId entry_id, PageId new_entry_id) {
	YuDbBPlusLeafListEntry* entry = YuDbBPlusLeafEntryReferencer_Reference(head, entry_id);
	YuDbBPlusLeafListEntry* new_entry = YuDbBPlusLeafEntryReferencer_Reference(head, new_entry_id);
	PageId prev_id = entry->prev;
	PageId next_id = entry->next;
	if (prev_id == entry_id) {
		prev_id = new_entry_id;
	}
	if (next_id == entry_id) {
		next_id = new_entry_id;
	}
	new_entry->prev = prev_id;
	new_entry->next = next_id;
	YuDbBPlusLeafListEntry* prev = YuDbBPlusLeafEntryReferencer_Reference(head, prev_id);
	YuDbBPlusLeafListEntry* next = YuDbBPlusLeafEntryReferencer_Reference(head, next_id);
	prev->next = new_entry_id;
	next->prev = new_entry_id;
	YuDbBPlusLeafEntryReferencer_Dereference(head, prev);
	YuDbBPlusLeafEntryReferencer_Dereference(head, next);
	if (entry_id == head->first) {
		head->first = new_entry_id;
	}
	if (entry_id == head->last) {
		head->last = new_entry_id;
	}
	YuDbBPlusLeafEntryReferencer_Dereference(head, entry);
	YuDbBPlusLeafEntryReferencer_Dereference(head, new_entry);
}
size_t YuDbBPlusLeafListGetCount(YuDbBPlusLeafListHead* head) {
	size_t count = 0;
	PageId cur_id = YuDbBPlusLeafListFirst(head);
	while (cur_id != YuDbBPlusLeafEntryReferencer_InvalidId) {
		count++;
		YuDbBPlusLeafListEntry* cur = YuDbBPlusLeafEntryReferencer_Reference(head, cur_id);
		cur_id = YuDbBPlusLeafListNext(head, cur_id);
		YuDbBPlusLeafEntryReferencer_Dereference(head, cur);
	}
	return count;
}
PageId YuDbBPlusLeafListFirst(YuDbBPlusLeafListHead* head) {
	return head->first;
}
PageId YuDbBPlusLeafListLast(YuDbBPlusLeafListHead* head) {
	return head->last;
}
PageId YuDbBPlusLeafListPrev(YuDbBPlusLeafListHead* head, PageId cur_id) {
	YuDbBPlusLeafListEntry* cur = YuDbBPlusLeafEntryReferencer_Reference(head, cur_id);
	PageId prev_id = cur->prev;
	YuDbBPlusLeafEntryReferencer_Dereference(head, cur);
	if (prev_id == head->last) {
		return YuDbBPlusLeafEntryReferencer_InvalidId;
	}
	return prev_id;
}
PageId YuDbBPlusLeafListNext(YuDbBPlusLeafListHead* head, PageId cur_id) {
	YuDbBPlusLeafListEntry* cur = YuDbBPlusLeafEntryReferencer_Reference(head, cur_id);
	PageId next_id = cur->next;
	YuDbBPlusLeafEntryReferencer_Dereference(head, cur);
	if (next_id == head->first) {
		return YuDbBPlusLeafEntryReferencer_InvalidId;
	}
	return next_id;
}
void YuDbBPlusCursorStackVectorResetCapacity(YuDbBPlusCursorStackVector* arr, size_t capacity) {
	YuDbBPlusElementPos* new_buf = ((YuDbBPlusElementPos*)MemoryAlloc(sizeof(YuDbBPlusElementPos) * (capacity)));
	if (arr->obj_arr) {
		memcpy((void*)(new_buf), (void*)(arr->obj_arr), (sizeof(YuDbBPlusElementPos) * arr->count));
		(MemoryFree(arr->obj_arr));
	}
	arr->obj_arr = new_buf;
	arr->capacity = capacity;
}
void YuDbBPlusCursorStackVectorExpand(YuDbBPlusCursorStackVector* arr, size_t add_count) {
	size_t old_capacity = arr->capacity;
	size_t cur_capacity = old_capacity;
	size_t target_count = cur_capacity + add_count;
	if (cur_capacity == 0) {
		cur_capacity = 1;
	}
	while (cur_capacity < target_count) {
		cur_capacity *= 2;
	}
	YuDbBPlusCursorStackVectorResetCapacity(arr, cur_capacity);
	;
}
void YuDbBPlusCursorStackVectorInit(YuDbBPlusCursorStackVector* arr, size_t count, _Bool create) {
	arr->count = count;
	arr->obj_arr = ((void*)0);
	if (count != 0 && create) {
		YuDbBPlusCursorStackVectorResetCapacity(arr, count);
	}
	else {
		arr->capacity = count;
	}
}
void YuDbBPlusCursorStackVectorRelease(YuDbBPlusCursorStackVector* arr) {
	if (arr->obj_arr) {
		(MemoryFree(arr->obj_arr));
		arr->obj_arr = ((void*)0);
	}
	arr->capacity = 0;
	arr->count = 0;
}
ptrdiff_t YuDbBPlusCursorStackVectorPushTail(YuDbBPlusCursorStackVector* arr, const YuDbBPlusElementPos* obj) {
	if (arr->capacity <= arr->count) {
		YuDbBPlusCursorStackVectorExpand(arr, 1);
	}
	memcpy((void*)(&arr->obj_arr[arr->count++]), (void*)(obj), (sizeof(YuDbBPlusElementPos)));
	return arr->count - 1;
}
YuDbBPlusElementPos* YuDbBPlusCursorStackVectorPopTail(YuDbBPlusCursorStackVector* arr) {
	if (arr->count == 0) {
		return ((void*)0);
	}
	return &arr->obj_arr[--arr->count];
}
static const int16_t YuDbBPlusEntryRbReferencer_InvalidId = (-1);
__forceinline YuDbBPlusEntryRbEntry* YuDbBPlusEntryRbReferencer_Reference(YuDbBPlusEntryRbTree* tree, int16_t element_id) {
	if (element_id == YuDbBPlusEntryRbReferencer_InvalidId) {
		return ((void*)0);
	}
	YuDbBPlusEntry* entry = ((YuDbBPlusEntry*)((uintptr_t)(tree)-((int)&(((YuDbBPlusEntry*)0)->rb_tree))));
	if (entry->type == kBPlusEntryIndex) {
		return &entry->index.element_space.obj_arr[element_id].rb_entry;
	}
	else {
		return &entry->leaf.element_space.obj_arr[element_id].rb_entry;
	}
}
__forceinline void YuDbBPlusEntryRbReferencer_Dereference(YuDbBPlusEntryRbTree* tree, YuDbBPlusEntryRbEntry* entry) {
}
typedef struct {
	int16_t color : 1;
	int16_t parent : sizeof(int16_t) * 8 - 1;
}
YuDbBPlusEntryRbParentColor;
__forceinline int16_t YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(YuDbBPlusEntryRbTree* tree, YuDbBPlusEntryRbBsEntry* bs_entry) {
	return (((YuDbBPlusEntryRbParentColor*)&(((YuDbBPlusEntryRbEntry*)bs_entry)->parent_color))->parent);
}
__forceinline RbColor YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetColor(YuDbBPlusEntryRbTree* tree, YuDbBPlusEntryRbBsEntry* bs_entry) {
	return (((YuDbBPlusEntryRbParentColor*)&(((YuDbBPlusEntryRbEntry*)bs_entry)->parent_color))->color == -1 ? 1 : 0);
}
__forceinline void YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetParent(YuDbBPlusEntryRbTree* tree, YuDbBPlusEntryRbBsEntry* bs_entry, int16_t new_id) {
	((YuDbBPlusEntryRbParentColor*)&(((YuDbBPlusEntryRbEntry*)bs_entry)->parent_color))->parent = new_id;
}
__forceinline void YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetColor(YuDbBPlusEntryRbTree* tree, YuDbBPlusEntryRbBsEntry* bs_entry, RbColor new_color) {
	return ((YuDbBPlusEntryRbParentColor*)&(((YuDbBPlusEntryRbEntry*)bs_entry)->parent_color))->color = new_color;
}
static void YuDbBPlusEntryRbBsTreeHitchEntry(YuDbBPlusEntryRbBsTree* tree, int16_t entry_id, int16_t new_entry_id) {
	YuDbBPlusEntryRbBsEntry* entry = YuDbBPlusEntryRbReferencer_Reference(tree, entry_id);
	YuDbBPlusEntryRbBsEntry* new_entry = YuDbBPlusEntryRbReferencer_Reference(tree, new_entry_id);
	if (YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, entry) != YuDbBPlusEntryRbReferencer_InvalidId) {
		YuDbBPlusEntryRbBsEntry* entry_parent = YuDbBPlusEntryRbReferencer_Reference(tree, YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, entry));
		if (entry_parent->left == entry_id) {
			entry_parent->left = new_entry_id;
		}
		else {
			entry_parent->right = new_entry_id;
		}
		YuDbBPlusEntryRbReferencer_Dereference(tree, entry_parent);
	}
	if (new_entry_id != YuDbBPlusEntryRbReferencer_InvalidId) {
		YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetParent(tree, new_entry, YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, entry));
	}
	if (tree->root == entry_id) {
		tree->root = new_entry_id;
	}
	YuDbBPlusEntryRbReferencer_Dereference(tree, entry);
	YuDbBPlusEntryRbReferencer_Dereference(tree, new_entry);
}
static int16_t YuDbBPlusEntryRbRotateLeft(YuDbBPlusEntryRbBsTree* tree, int16_t sub_root_id, YuDbBPlusEntryRbBsEntry* sub_root) {
	int16_t new_sub_root_id = sub_root->right;
	if (new_sub_root_id == YuDbBPlusEntryRbReferencer_InvalidId) {
		return sub_root_id;
	}
	YuDbBPlusEntryRbBsEntry* new_sub_root = YuDbBPlusEntryRbReferencer_Reference(tree, new_sub_root_id);
	YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetParent(tree, new_sub_root, YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, sub_root));
	if (YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, sub_root) != YuDbBPlusEntryRbReferencer_InvalidId) {
		YuDbBPlusEntryRbBsEntry* sub_root_parent = YuDbBPlusEntryRbReferencer_Reference(tree, YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, sub_root));
		if (sub_root_parent->left == sub_root_id) {
			sub_root_parent->left = new_sub_root_id;
		}
		else {
			sub_root_parent->right = new_sub_root_id;
		}
		YuDbBPlusEntryRbReferencer_Dereference(tree, sub_root_parent);
	}
	YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetParent(tree, sub_root, new_sub_root_id);
	sub_root->right = new_sub_root->left;
	if (sub_root->right != YuDbBPlusEntryRbReferencer_InvalidId) {
		YuDbBPlusEntryRbBsEntry* sub_root_right = YuDbBPlusEntryRbReferencer_Reference(tree, sub_root->right);
		YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetParent(tree, sub_root_right, sub_root_id);
		YuDbBPlusEntryRbReferencer_Dereference(tree, sub_root_right);
	}
	new_sub_root->left = sub_root_id;
	YuDbBPlusEntryRbReferencer_Dereference(tree, new_sub_root);
	return new_sub_root_id;
}
static int16_t YuDbBPlusEntryRbRotateRight(YuDbBPlusEntryRbBsTree* tree, int16_t sub_root_id, YuDbBPlusEntryRbBsEntry* sub_root) {
	int16_t new_sub_root_id = sub_root->left;
	if (new_sub_root_id == YuDbBPlusEntryRbReferencer_InvalidId) {
		return sub_root_id;
	}
	YuDbBPlusEntryRbBsEntry* new_sub_root = YuDbBPlusEntryRbReferencer_Reference(tree, new_sub_root_id);
	YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetParent(tree, new_sub_root, YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, sub_root));
	if (YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, sub_root) != YuDbBPlusEntryRbReferencer_InvalidId) {
		YuDbBPlusEntryRbBsEntry* sub_root_parent = YuDbBPlusEntryRbReferencer_Reference(tree, YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, sub_root));
		if (sub_root_parent->left == sub_root_id) {
			sub_root_parent->left = new_sub_root_id;
		}
		else {
			sub_root_parent->right = new_sub_root_id;
		}
		YuDbBPlusEntryRbReferencer_Dereference(tree, sub_root_parent);
	}
	YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetParent(tree, sub_root, new_sub_root_id);
	sub_root->left = new_sub_root->right;
	if (sub_root->left != YuDbBPlusEntryRbReferencer_InvalidId) {
		YuDbBPlusEntryRbBsEntry* sub_root_left = YuDbBPlusEntryRbReferencer_Reference(tree, sub_root->left);
		YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetParent(tree, sub_root_left, sub_root_id);
		YuDbBPlusEntryRbReferencer_Dereference(tree, sub_root_left);
	}
	new_sub_root->right = sub_root_id;
	YuDbBPlusEntryRbReferencer_Dereference(tree, new_sub_root);
	return new_sub_root_id;
}
static void YuDbBPlusEntryRbBsEntryInit(YuDbBPlusEntryRbBsTree* tree, YuDbBPlusEntryRbBsEntry* entry) {
	entry->left = YuDbBPlusEntryRbReferencer_InvalidId;
	entry->right = YuDbBPlusEntryRbReferencer_InvalidId;
	entry->parent = YuDbBPlusEntryRbReferencer_InvalidId;
}
void YuDbBPlusEntryRbBsTreeInit(YuDbBPlusEntryRbBsTree* tree) {
	tree->root = YuDbBPlusEntryRbReferencer_InvalidId;
}
int16_t YuDbBPlusEntryRbBsTreeFind(YuDbBPlusEntryRbBsTree* tree, int32_t* key) {
	int8_t status;
	int16_t id = YuDbBPlusEntryRbBsTreeIteratorLocate(tree, key, &status);
	return status == 0 ? id : YuDbBPlusEntryRbReferencer_InvalidId;
}
void YuDbBPlusEntryRbBsTreeInsert(YuDbBPlusEntryRbBsTree* tree, int16_t entry_id) {
	YuDbBPlusEntryRbBsEntry* entry = YuDbBPlusEntryRbReferencer_Reference(tree, entry_id);
	YuDbBPlusEntryRbBsEntryInit(tree, entry);
	if (tree->root == YuDbBPlusEntryRbReferencer_InvalidId) {
		tree->root = entry_id;
		return;
	}
	int16_t cur_id = tree->root;
	YuDbBPlusEntryRbBsEntry* cur = ((void*)0);
	while (cur_id != YuDbBPlusEntryRbReferencer_InvalidId) {
		YuDbBPlusEntryRbBsEntry* cur = YuDbBPlusEntryRbReferencer_Reference(tree, cur_id);
		if (((YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetKey(tree, cur)) < (YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetKey(tree, entry)))) {
			if (cur->right == YuDbBPlusEntryRbReferencer_InvalidId) {
				cur->right = entry_id;
				break;
			}
			cur_id = cur->right;
		}
		else {
			if (cur->left == YuDbBPlusEntryRbReferencer_InvalidId) {
				cur->left = entry_id;
				break;
			}
			cur_id = cur->left;
		}
		YuDbBPlusEntryRbReferencer_Dereference(tree, cur);
	}
	if (cur) YuDbBPlusEntryRbReferencer_Dereference(tree, cur);
	YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetParent(tree, entry, cur_id);
	YuDbBPlusEntryRbReferencer_Dereference(tree, entry);
	return;
}
int16_t YuDbBPlusEntryRbBsTreePut(YuDbBPlusEntryRbBsTree* tree, int16_t entry_id) {
	YuDbBPlusEntryRbBsEntry* entry = YuDbBPlusEntryRbReferencer_Reference(tree, entry_id);
	YuDbBPlusEntryRbBsEntryInit(tree, entry);
	if (tree->root == YuDbBPlusEntryRbReferencer_InvalidId) {
		tree->root = entry_id;
		return 1;
	}
	int16_t cur_id = tree->root;
	int16_t prev_id = YuDbBPlusEntryRbReferencer_InvalidId;
	YuDbBPlusEntryRbBsEntry* cur = ((void*)0);
	int16_t old_id = YuDbBPlusEntryRbReferencer_InvalidId;
	while (cur_id != YuDbBPlusEntryRbReferencer_InvalidId) {
		cur = YuDbBPlusEntryRbReferencer_Reference(tree, cur_id);
		if (((YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetKey(tree, cur)) < (YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetKey(tree, entry)))) {
			if (cur->right == YuDbBPlusEntryRbReferencer_InvalidId) {
				cur->right = entry_id;
				break;
			}
			prev_id = cur_id;
			cur_id = cur->right;
		}
		else if (((YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetKey(tree, cur)) > (YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetKey(tree, entry)))) {
			if (cur->left == YuDbBPlusEntryRbReferencer_InvalidId) {
				cur->left = entry_id;
				break;
			}
			prev_id = cur_id;
			cur_id = cur->left;
		}
		else {
			YuDbBPlusEntryRbBsEntry* prev = YuDbBPlusEntryRbReferencer_Reference(tree, prev_id);
			old_id = cur_id;
			if (prev->left == cur_id) {
				prev->left = entry_id;
			}
			else {
				prev->right = entry_id;
			}
			if (cur->left != YuDbBPlusEntryRbReferencer_InvalidId) {
				YuDbBPlusEntryRbBsEntry* left = YuDbBPlusEntryRbReferencer_Reference(tree, cur->left);
				YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetParent(tree, left, entry_id);
				YuDbBPlusEntryRbReferencer_Dereference(tree, left);
			}
			if (cur->right != YuDbBPlusEntryRbReferencer_InvalidId) {
				YuDbBPlusEntryRbBsEntry* right = YuDbBPlusEntryRbReferencer_Reference(tree, cur->right);
				YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetParent(tree, right, entry_id);
				YuDbBPlusEntryRbReferencer_Dereference(tree, right);
			}
			break;
		}
		YuDbBPlusEntryRbReferencer_Dereference(tree, cur);
	}
	if (cur) YuDbBPlusEntryRbReferencer_Dereference(tree, cur);
	YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetParent(tree, entry, cur_id);
	YuDbBPlusEntryRbReferencer_Dereference(tree, entry);
	return old_id;
}
int16_t YuDbBPlusEntryRbBsTreeDelete(YuDbBPlusEntryRbBsTree* tree, int16_t entry_id, _Bool* is_parent_left) {
	int16_t backtrack_id;
	YuDbBPlusEntryRbBsEntry* entry = YuDbBPlusEntryRbReferencer_Reference(tree, entry_id);
	if (entry->left != YuDbBPlusEntryRbReferencer_InvalidId && entry->right != YuDbBPlusEntryRbReferencer_InvalidId) {
		int16_t min_entry_id = entry->right;
		YuDbBPlusEntryRbBsEntry* min_entry = YuDbBPlusEntryRbReferencer_Reference(tree, min_entry_id);
		while (min_entry->left != YuDbBPlusEntryRbReferencer_InvalidId) {
			min_entry_id = min_entry->left;
			YuDbBPlusEntryRbReferencer_Dereference(tree, min_entry);
			min_entry = YuDbBPlusEntryRbReferencer_Reference(tree, min_entry_id);
		}
		YuDbBPlusEntryRbBsEntry* min_entry_parent = YuDbBPlusEntryRbReferencer_Reference(tree, YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, min_entry));
		if (is_parent_left) {
			*is_parent_left = min_entry_parent->left == min_entry_id;
		}
		min_entry->left = entry->left;
		if (entry->left != YuDbBPlusEntryRbReferencer_InvalidId) {
			YuDbBPlusEntryRbBsEntry* entry_left = YuDbBPlusEntryRbReferencer_Reference(tree, entry->left);
			YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetParent(tree, entry_left, min_entry_id);
			YuDbBPlusEntryRbReferencer_Dereference(tree, entry_left);
		}
		int16_t old_right_id = min_entry->right;
		if (entry->right != min_entry_id) {
			min_entry_parent->left = min_entry->right;
			if (min_entry->right != YuDbBPlusEntryRbReferencer_InvalidId) {
				YuDbBPlusEntryRbBsEntry* min_entry_right = YuDbBPlusEntryRbReferencer_Reference(tree, min_entry->right);
				YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetParent(tree, min_entry_right, YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, min_entry));
				YuDbBPlusEntryRbReferencer_Dereference(tree, min_entry_right);
			}
			min_entry->right = entry->right;
			if (entry->right != YuDbBPlusEntryRbReferencer_InvalidId) {
				YuDbBPlusEntryRbBsEntry* entry_right = YuDbBPlusEntryRbReferencer_Reference(tree, entry->right);
				YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetParent(tree, entry_right, min_entry_id);
				YuDbBPlusEntryRbReferencer_Dereference(tree, entry_right);
			}
			backtrack_id = YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, min_entry);
		}
		else {
			backtrack_id = min_entry_id;
		}
		YuDbBPlusEntryRbReferencer_Dereference(tree, min_entry_parent);
		YuDbBPlusEntryRbBsTreeHitchEntry(tree, entry_id, min_entry_id);
		entry_id = min_entry_id;
		entry->left = YuDbBPlusEntryRbReferencer_InvalidId;
		entry->right = old_right_id;
		YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetParent(tree, entry, backtrack_id);
	}
	else {
		if (is_parent_left) {
			int16_t parent_id = YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, entry);
			if (parent_id != YuDbBPlusEntryRbReferencer_InvalidId) {
				YuDbBPlusEntryRbBsEntry* parent = YuDbBPlusEntryRbReferencer_Reference(tree, parent_id);
				*is_parent_left = parent->left == entry_id;
				YuDbBPlusEntryRbReferencer_Dereference(tree, parent);
			}
			else {
				*is_parent_left = 0;
			}
		}
		if (entry->right != YuDbBPlusEntryRbReferencer_InvalidId) {
			YuDbBPlusEntryRbBsTreeHitchEntry(tree, entry_id, entry->right);
		}
		else if (entry->left != YuDbBPlusEntryRbReferencer_InvalidId) {
			YuDbBPlusEntryRbBsTreeHitchEntry(tree, entry_id, entry->left);
		}
		else {
			YuDbBPlusEntryRbBsTreeHitchEntry(tree, entry_id, YuDbBPlusEntryRbReferencer_InvalidId);
		}
	}
	return entry_id;
}
size_t YuDbBPlusEntryRbBsTreeGetCount(YuDbBPlusEntryRbBsTree* tree) {
	size_t count = 0;
	int16_t cur_id = YuDbBPlusEntryRbBsTreeIteratorFirst(tree);
	while (cur_id != YuDbBPlusEntryRbReferencer_InvalidId) {
		count++;
		cur_id = YuDbBPlusEntryRbBsTreeIteratorNext(tree, cur_id);
	}
	return count;
}
int16_t YuDbBPlusEntryRbBsTreeIteratorLocate(YuDbBPlusEntryRbBsTree* tree, int32_t* key, int8_t* cmp_status) {
	int16_t cur_id = tree->root;
	int16_t perv_id = YuDbBPlusEntryRbReferencer_InvalidId;
	while (cur_id != YuDbBPlusEntryRbReferencer_InvalidId) {
		perv_id = cur_id;
		YuDbBPlusEntryRbBsEntry* cur = YuDbBPlusEntryRbReferencer_Reference(tree, cur_id);
		int32_t cur_key = YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetKey(tree, cur);
		if (((cur_key) < (*key))) {
			*cmp_status = 1;
			cur_id = cur->right;
		}
		else if (((cur_key) > (*key))) {
			*cmp_status = -1;
			cur_id = cur->left;
		}
		else {
			YuDbBPlusEntryRbReferencer_Dereference(tree, cur);
			*cmp_status = 0;
			return cur_id;
		}
		YuDbBPlusEntryRbReferencer_Dereference(tree, cur);
	}
	return perv_id;
}
int16_t YuDbBPlusEntryRbBsTreeIteratorFirst(YuDbBPlusEntryRbBsTree* tree) {
	int16_t cur_id = tree->root;
	if (cur_id == YuDbBPlusEntryRbReferencer_InvalidId) {
		return YuDbBPlusEntryRbReferencer_InvalidId;
	}
	YuDbBPlusEntryRbBsEntry* cur = YuDbBPlusEntryRbReferencer_Reference(tree, cur_id);
	while (cur->left != YuDbBPlusEntryRbReferencer_InvalidId) {
		cur_id = cur->left;
		YuDbBPlusEntryRbReferencer_Dereference(tree, cur);
		cur = YuDbBPlusEntryRbReferencer_Reference(tree, cur_id);
	}
	YuDbBPlusEntryRbReferencer_Dereference(tree, cur);
	return cur_id;
}
int16_t YuDbBPlusEntryRbBsTreeIteratorLast(YuDbBPlusEntryRbBsTree* tree) {
	int16_t cur_id = tree->root;
	if (cur_id == YuDbBPlusEntryRbReferencer_InvalidId) {
		return YuDbBPlusEntryRbReferencer_InvalidId;
	}
	YuDbBPlusEntryRbBsEntry* cur = YuDbBPlusEntryRbReferencer_Reference(tree, cur_id);
	while (cur->right != YuDbBPlusEntryRbReferencer_InvalidId) {
		cur_id = cur->right;
		YuDbBPlusEntryRbReferencer_Dereference(tree, cur);
		cur = YuDbBPlusEntryRbReferencer_Reference(tree, cur_id);
	}
	YuDbBPlusEntryRbReferencer_Dereference(tree, cur);
	return cur_id;
}
int16_t YuDbBPlusEntryRbBsTreeIteratorNext(YuDbBPlusEntryRbBsTree* tree, int16_t cur_id) {
	YuDbBPlusEntryRbBsEntry* cur = YuDbBPlusEntryRbReferencer_Reference(tree, cur_id);
	if (cur->right != YuDbBPlusEntryRbReferencer_InvalidId) {
		cur_id = cur->right;
		cur = YuDbBPlusEntryRbReferencer_Reference(tree, cur_id);
		while (cur->left != YuDbBPlusEntryRbReferencer_InvalidId) {
			cur_id = cur->left;
			YuDbBPlusEntryRbReferencer_Dereference(tree, cur);
			cur = YuDbBPlusEntryRbReferencer_Reference(tree, cur_id);
		}
		YuDbBPlusEntryRbReferencer_Dereference(tree, cur);
		return cur_id;
	}
	int16_t parent_id = YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, cur);
	YuDbBPlusEntryRbBsEntry* parent = YuDbBPlusEntryRbReferencer_Reference(tree, parent_id);
	while (parent_id != YuDbBPlusEntryRbReferencer_InvalidId && cur_id == parent->right) {
		YuDbBPlusEntryRbReferencer_Dereference(tree, cur);
		cur = parent;
		cur_id = parent_id;
		parent_id = YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, cur);
		parent = YuDbBPlusEntryRbReferencer_Reference(tree, parent_id);
	}
	YuDbBPlusEntryRbReferencer_Dereference(tree, cur);
	YuDbBPlusEntryRbReferencer_Dereference(tree, parent);
	return parent_id;
}
int16_t YuDbBPlusEntryRbBsTreeIteratorPrev(YuDbBPlusEntryRbBsTree* tree, int16_t cur_id) {
	YuDbBPlusEntryRbBsEntry* cur = YuDbBPlusEntryRbReferencer_Reference(tree, cur_id);
	if (cur->left != YuDbBPlusEntryRbReferencer_InvalidId) {
		cur_id = cur->left;
		cur = YuDbBPlusEntryRbReferencer_Reference(tree, cur_id);
		while (cur->right != YuDbBPlusEntryRbReferencer_InvalidId) {
			cur_id = cur->right;
			YuDbBPlusEntryRbReferencer_Dereference(tree, cur);
			cur = YuDbBPlusEntryRbReferencer_Reference(tree, cur_id);
		}
		YuDbBPlusEntryRbReferencer_Dereference(tree, cur);
		return cur_id;
	}
	int16_t parent_id = YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, cur);
	YuDbBPlusEntryRbBsEntry* parent = YuDbBPlusEntryRbReferencer_Reference(tree, parent_id);
	while (parent_id != YuDbBPlusEntryRbReferencer_InvalidId && cur_id == parent->left) {
		YuDbBPlusEntryRbReferencer_Dereference(tree, cur);
		cur = parent;
		cur_id = parent_id;
		parent_id = YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, cur);
		parent = YuDbBPlusEntryRbReferencer_Reference(tree, parent_id);
	}
	YuDbBPlusEntryRbReferencer_Dereference(tree, cur);
	YuDbBPlusEntryRbReferencer_Dereference(tree, parent);
	return parent_id;
}
static int16_t YuDbBPlusEntryGetSiblingEntry(YuDbBPlusEntryRbTree* tree, int16_t entry_id, YuDbBPlusEntryRbEntry* entry) {
	int16_t parent_id = YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, entry);
	YuDbBPlusEntryRbEntry* parent = YuDbBPlusEntryRbReferencer_Reference(tree, parent_id);
	int16_t ret;
	if (parent->left == entry_id) {
		ret = parent->right;
	}
	else {
		ret = parent->left;
	}
	YuDbBPlusEntryRbReferencer_Dereference(tree, parent);
	return ret;
}
static void YuDbBPlusEntryRbTreeInsertFixup(YuDbBPlusEntryRbTree* tree, int16_t ins_entry_id) {
	YuDbBPlusEntryRbEntry* ins_entry = YuDbBPlusEntryRbReferencer_Reference(tree, ins_entry_id);
	YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetColor(tree, ins_entry, kRbBlack);
	int16_t cur_id = YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, ins_entry);
	if (cur_id == YuDbBPlusEntryRbReferencer_InvalidId) {
		YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetColor(tree, ins_entry, kRbBlack);
		YuDbBPlusEntryRbReferencer_Dereference(tree, ins_entry);
		return;
	}
	YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetColor(tree, ins_entry, kRbRed);
	YuDbBPlusEntryRbReferencer_Dereference(tree, ins_entry);
	YuDbBPlusEntryRbEntry* cur = ((void*)0);
	while (cur_id != YuDbBPlusEntryRbReferencer_InvalidId) {
		cur = YuDbBPlusEntryRbReferencer_Reference(tree, cur_id);
		if (YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetColor(tree, cur) == kRbBlack) {
			break;
		}
		if (YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, cur) == YuDbBPlusEntryRbReferencer_InvalidId) {
			YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetColor(tree, cur, kRbBlack);
			break;
		}
		int16_t sibling_id = YuDbBPlusEntryGetSiblingEntry(tree, cur_id, cur);
		YuDbBPlusEntryRbEntry* sibling = YuDbBPlusEntryRbReferencer_Reference(tree, sibling_id);
		if (sibling_id != YuDbBPlusEntryRbReferencer_InvalidId && YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetColor(tree, sibling) == kRbRed) {
			YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetColor(tree, cur, kRbBlack);
			YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetColor(tree, sibling, kRbBlack);
			ins_entry_id = YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, cur);
			ins_entry = YuDbBPlusEntryRbReferencer_Reference(tree, ins_entry_id);
			if (YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, ins_entry) == YuDbBPlusEntryRbReferencer_InvalidId) {
				YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetColor(tree, ins_entry, kRbBlack);
				break;
			}
			YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetColor(tree, ins_entry, kRbRed);
			cur = ins_entry;
		}
		else {
			{
				if (!(sibling_id == YuDbBPlusEntryRbReferencer_InvalidId || YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetColor(tree, sibling) == kRbBlack)) {
					*(int*)0 = 0;
				}
			}
			;
			int16_t new_sub_root_id;
			int16_t old_sub_root_id = YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, cur);
			YuDbBPlusEntryRbEntry* old_sub_root = YuDbBPlusEntryRbReferencer_Reference(tree, old_sub_root_id);
			if (old_sub_root->left == cur_id) {
				if (cur->right == ins_entry_id) {
					YuDbBPlusEntryRbRotateLeft(tree, cur_id, cur);
				}
				new_sub_root_id = YuDbBPlusEntryRbRotateRight(tree, old_sub_root_id, old_sub_root);
			}
			else {
				if (cur->left == ins_entry_id) {
					YuDbBPlusEntryRbRotateRight(tree, cur_id, cur);
				}
				new_sub_root_id = YuDbBPlusEntryRbRotateLeft(tree, old_sub_root_id, old_sub_root);
			}
			YuDbBPlusEntryRbEntry* new_sub_root = YuDbBPlusEntryRbReferencer_Reference(tree, new_sub_root_id);
			YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetColor(tree, new_sub_root, kRbBlack);
			YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetColor(tree, old_sub_root, kRbRed);
			YuDbBPlusEntryRbReferencer_Dereference(tree, new_sub_root);
			if (tree->root == old_sub_root_id) {
				tree->root = new_sub_root_id;
			}
			break;
		}
		cur_id = YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, cur);
		YuDbBPlusEntryRbReferencer_Dereference(tree, cur);
	}
	YuDbBPlusEntryRbReferencer_Dereference(tree, cur);
}
static void YuDbBPlusEntryRbTreeDeleteFixup(YuDbBPlusEntryRbTree* tree, int16_t del_entry_id, _Bool is_parent_left) {
	YuDbBPlusEntryRbEntry* del_entry = YuDbBPlusEntryRbReferencer_Reference(tree, del_entry_id);
	int16_t cur_id = YuDbBPlusEntryRbReferencer_InvalidId;
	RbColor del_color = YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetColor(tree, del_entry);
	if (del_color == kRbRed) {
	}
	else if (del_entry->left != YuDbBPlusEntryRbReferencer_InvalidId) {
		YuDbBPlusEntryRbEntry* del_entry_left = YuDbBPlusEntryRbReferencer_Reference(tree, del_entry->left);
		YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetColor(tree, del_entry_left, kRbBlack);
		YuDbBPlusEntryRbReferencer_Dereference(tree, del_entry_left);
	}
	else if (del_entry->right != YuDbBPlusEntryRbReferencer_InvalidId) {
		YuDbBPlusEntryRbEntry* del_entry_right = YuDbBPlusEntryRbReferencer_Reference(tree, del_entry->right);
		YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetColor(tree, del_entry_right, kRbBlack);
		YuDbBPlusEntryRbReferencer_Dereference(tree, del_entry_right);
	}
	else {
		cur_id = YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, del_entry);
	}
	int16_t new_sub_root_id;
	YuDbBPlusEntryRbEntry* cur = ((void*)0), * sibling = ((void*)0);
	cur = YuDbBPlusEntryRbReferencer_Reference(tree, cur_id);
	while (cur_id != YuDbBPlusEntryRbReferencer_InvalidId) {
		int16_t sibling_id = is_parent_left ? cur->right : cur->left;
		sibling = YuDbBPlusEntryRbReferencer_Reference(tree, sibling_id);
		if (YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetColor(tree, sibling) == kRbRed) {
			int16_t old_sub_root_id = YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, sibling);
			YuDbBPlusEntryRbEntry* old_sub_root = YuDbBPlusEntryRbReferencer_Reference(tree, old_sub_root_id);
			YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetColor(tree, old_sub_root, kRbRed);
			YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetColor(tree, sibling, kRbBlack);
			if (old_sub_root->left == sibling_id) {
				new_sub_root_id = YuDbBPlusEntryRbRotateRight(tree, old_sub_root_id, old_sub_root);
				sibling_id = old_sub_root->left;
				YuDbBPlusEntryRbReferencer_Dereference(tree, sibling);
				sibling = YuDbBPlusEntryRbReferencer_Reference(tree, sibling_id);
			}
			else {
				new_sub_root_id = YuDbBPlusEntryRbRotateLeft(tree, old_sub_root_id, old_sub_root);
				sibling_id = old_sub_root->right;
				YuDbBPlusEntryRbReferencer_Dereference(tree, sibling);
				sibling = YuDbBPlusEntryRbReferencer_Reference(tree, sibling_id);
			}
			if (tree->root == old_sub_root_id) {
				tree->root = new_sub_root_id;
			}
			YuDbBPlusEntryRbReferencer_Dereference(tree, old_sub_root);
		}
		YuDbBPlusEntryRbEntry* sibling_right = YuDbBPlusEntryRbReferencer_Reference(tree, sibling->right);
		YuDbBPlusEntryRbEntry* sibling_left = YuDbBPlusEntryRbReferencer_Reference(tree, sibling->left);
		if (sibling->right != YuDbBPlusEntryRbReferencer_InvalidId && YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetColor(tree, sibling_right) == kRbRed || sibling->left != YuDbBPlusEntryRbReferencer_InvalidId && YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetColor(tree, sibling_left) == kRbRed) {
			RbColor parent_color = YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetColor(tree, cur);
			YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetColor(tree, cur, kRbBlack);
			int16_t old_sub_root_id = cur_id;
			if (cur->left == sibling_id) {
				if (sibling->left == YuDbBPlusEntryRbReferencer_InvalidId || YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetColor(tree, sibling_left) == kRbBlack) {
					YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetColor(tree, sibling_right, kRbBlack);
					sibling_id = YuDbBPlusEntryRbRotateLeft(tree, sibling_id, sibling);
				}
				else {
					YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetColor(tree, sibling_left, kRbBlack);
				}
				new_sub_root_id = YuDbBPlusEntryRbRotateRight(tree, cur_id, cur);
			}
			else {
				if (sibling->right == YuDbBPlusEntryRbReferencer_InvalidId || YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetColor(tree, sibling_right) == kRbBlack) {
					YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetColor(tree, sibling_left, kRbBlack);
					sibling_id = YuDbBPlusEntryRbRotateRight(tree, sibling_id, sibling);
				}
				else {
					YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetColor(tree, sibling_right, kRbBlack);
				}
				new_sub_root_id = YuDbBPlusEntryRbRotateLeft(tree, cur_id, cur);
			}
			YuDbBPlusEntryRbReferencer_Dereference(tree, sibling);
			sibling = YuDbBPlusEntryRbReferencer_Reference(tree, sibling_id);
			YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetColor(tree, sibling, parent_color);
			if (tree->root == old_sub_root_id) {
				tree->root = new_sub_root_id;
			}
			YuDbBPlusEntryRbReferencer_Dereference(tree, sibling_right);
			YuDbBPlusEntryRbReferencer_Dereference(tree, sibling_left);
			break;
		}
		YuDbBPlusEntryRbReferencer_Dereference(tree, sibling_right);
		YuDbBPlusEntryRbReferencer_Dereference(tree, sibling_left);
		if (YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetColor(tree, cur) == kRbRed) {
			YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetColor(tree, sibling, kRbRed);
			YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetColor(tree, cur, kRbBlack);
			break;
		}
		else {
			YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetColor(tree, sibling, kRbRed);
		}
		int16_t child_id = cur_id;
		cur_id = YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetParent(tree, cur);
		if (cur_id != YuDbBPlusEntryRbReferencer_InvalidId) {
			YuDbBPlusEntryRbReferencer_Dereference(tree, cur);
			cur = YuDbBPlusEntryRbReferencer_Reference(tree, cur_id);
			is_parent_left = cur->left == child_id;
		}
	}
	YuDbBPlusEntryRbReferencer_Dereference(tree, sibling);
	YuDbBPlusEntryRbReferencer_Dereference(tree, cur);
	YuDbBPlusEntryRbEntry* root = YuDbBPlusEntryRbReferencer_Reference(tree, tree->root);
	if (root && YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetColor(tree, root) == kRbRed) {
		YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetColor(tree, root, kRbBlack);
	}
	YuDbBPlusEntryRbReferencer_Dereference(tree, root);
}
void YuDbBPlusEntryRbTreeInit(YuDbBPlusEntryRbTree* tree) {
	YuDbBPlusEntryRbBsTreeInit(&tree->bs_tree);
}
int16_t YuDbBPlusEntryRbTreeFind(YuDbBPlusEntryRbTree* tree, int32_t* key) {
	return YuDbBPlusEntryRbBsTreeFind(&tree->bs_tree, key);
}
void YuDbBPlusEntryRbTreeInsert(YuDbBPlusEntryRbTree* tree, int16_t insert_entry_id) {
	YuDbBPlusEntryRbBsTreeInsert(&tree->bs_tree, insert_entry_id);
	YuDbBPlusEntryRbTreeInsertFixup(tree, insert_entry_id);
}
int16_t YuDbBPlusEntryRbTreePut(YuDbBPlusEntryRbTree* tree, int16_t put_entry_id) {
	int16_t old_id = YuDbBPlusEntryRbBsTreePut(&tree->bs_tree, put_entry_id);
	if (old_id != YuDbBPlusEntryRbReferencer_InvalidId) YuDbBPlusEntryRbTreeInsertFixup(tree, put_entry_id);
	return old_id;
}
_Bool YuDbBPlusEntryRbTreeDelete(YuDbBPlusEntryRbTree* tree, int16_t del_entry_id) {
	_Bool is_parent_left;
	int16_t del_min_entry_id = YuDbBPlusEntryRbBsTreeDelete(&tree->bs_tree, del_entry_id, &is_parent_left);
	if (del_min_entry_id == YuDbBPlusEntryRbReferencer_InvalidId) {
		return 0;
	}
	if (del_min_entry_id != del_entry_id) {
		YuDbBPlusEntryRbEntry* del_entry = YuDbBPlusEntryRbReferencer_Reference(tree, del_entry_id);
		YuDbBPlusEntryRbEntry* del_min_entry = YuDbBPlusEntryRbReferencer_Reference(tree, del_min_entry_id);
		;
		RbColor old_color = YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetColor(tree, del_min_entry);
		YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetColor(tree, del_min_entry, YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_GetColor(tree, del_entry));
		YUDB_BUCKET_BPLUS_RB_TREE_ACCESSOR_SetColor(tree, del_entry, old_color);
	}
	YuDbBPlusEntryRbTreeDeleteFixup(tree, del_entry_id, is_parent_left);
	return 1;
}
int16_t YuDbBPlusEntryRbTreeIteratorLocate(YuDbBPlusEntryRbTree* tree, int32_t* key, int8_t* cmp_status) {
	return YuDbBPlusEntryRbBsTreeIteratorLocate((YuDbBPlusEntryRbBsTree*)tree, key, cmp_status);
}
int16_t YuDbBPlusEntryRbTreeIteratorFirst(YuDbBPlusEntryRbTree* tree) {
	return YuDbBPlusEntryRbBsTreeIteratorFirst((YuDbBPlusEntryRbBsTree*)tree);
}
int16_t YuDbBPlusEntryRbTreeIteratorLast(YuDbBPlusEntryRbTree* tree) {
	return YuDbBPlusEntryRbBsTreeIteratorLast((YuDbBPlusEntryRbBsTree*)tree);
}
int16_t YuDbBPlusEntryRbTreeIteratorNext(YuDbBPlusEntryRbTree* tree, int16_t cur_id) {
	return YuDbBPlusEntryRbBsTreeIteratorNext((YuDbBPlusEntryRbBsTree*)tree, cur_id);
}
int16_t YuDbBPlusEntryRbTreeIteratorPrev(YuDbBPlusEntryRbTree* tree, int16_t cur_id) {
	return YuDbBPlusEntryRbBsTreeIteratorPrev((YuDbBPlusEntryRbBsTree*)tree, cur_id);
}
static const int16_t YuDbBPlusElementStaticListReferencer_InvalidId = (-1);
__forceinline int16_t YuDbBPlusIndexStaticAccessor_GetNext(YuDbBPlusIndexStaticList* list, YuDbBPlusIndexElement element) {
	return element.next.next;
}
__forceinline void YuDbBPlusIndexStaticAccessor_SetNext(YuDbBPlusIndexStaticList* list, YuDbBPlusIndexElement element, int16_t new_next) {
	element.next.next = new_next;
}
void YuDbBPlusIndexStaticListInit(YuDbBPlusIndexStaticList* list, int16_t count) {
	list->list_first[0] = 0;
	int16_t i = 0;
	for (; i < count - 1; i++) {
		YuDbBPlusIndexStaticAccessor_SetNext(list, list->obj_arr[i], i + 1);
	}
	YuDbBPlusIndexStaticAccessor_SetNext(list, list->obj_arr[i], YuDbBPlusElementStaticListReferencer_InvalidId);
	for (i = 1; i < 1; i++) {
		list->list_first[i] = YuDbBPlusElementStaticListReferencer_InvalidId;
	}
}
void YuDbBPlusIndexStaticListExpand(YuDbBPlusIndexStaticList* list, int16_t old_count, int16_t new_count) {
	int16_t old_first = list->list_first[0];
	list->list_first[0] = new_count - 1;
	int16_t i = old_count;
	for (; i < new_count - 1; i++) {
		YuDbBPlusIndexStaticAccessor_SetNext(list, list->obj_arr[i], i + 1);
	}
	YuDbBPlusIndexStaticAccessor_SetNext(list, list->obj_arr[i], old_first);
}
int16_t YuDbBPlusIndexStaticListPop(YuDbBPlusIndexStaticList* list, int16_t list_order) {
	if (list->list_first[list_order] == YuDbBPlusElementStaticListReferencer_InvalidId) {
		return YuDbBPlusElementStaticListReferencer_InvalidId;
	}
	int16_t index = list->list_first[list_order];
	list->list_first[list_order] = YuDbBPlusIndexStaticAccessor_GetNext(list, list->obj_arr[index]);
	return index;
}
void YuDbBPlusIndexStaticListPush(YuDbBPlusIndexStaticList* list, int16_t list_order, int16_t index) {
	YuDbBPlusIndexStaticAccessor_SetNext(list, list->obj_arr[index], list->list_first[list_order]);
	list->list_first[list_order] = index;
}
int16_t YuDbBPlusIndexStaticListDelete(YuDbBPlusIndexStaticList* list, int16_t list_order, int16_t prev_id, int16_t delete_id) {
	if (prev_id == YuDbBPlusElementStaticListReferencer_InvalidId) {
		list->list_first[list_order] = YuDbBPlusIndexStaticAccessor_GetNext(list, list->obj_arr[delete_id]);
	}
	else {
		YuDbBPlusIndexStaticAccessor_SetNext(list, list->obj_arr[prev_id], YuDbBPlusIndexStaticAccessor_GetNext(list, list->obj_arr[delete_id]));
	}
	return delete_id;
}
void YuDbBPlusIndexStaticListSwitch(YuDbBPlusIndexStaticList* list, int16_t list_order, int16_t prev_id, int16_t id, int16_t new_list_order) {
	YuDbBPlusIndexStaticListDelete(list, list_order, prev_id, id);
	YuDbBPlusIndexStaticListPush(list, new_list_order, id);
}
int16_t YuDbBPlusIndexStaticListIteratorFirst(YuDbBPlusIndexStaticList* list, int16_t list_order) {
	return list->list_first[list_order];
}
int16_t YuDbBPlusIndexStaticListIteratorNext(YuDbBPlusIndexStaticList* list, int16_t cur_id) {
	return YuDbBPlusIndexStaticAccessor_GetNext(list, list->obj_arr[cur_id]);
}
__forceinline int16_t YuDbBPlusLeafStaticAccessor_GetNext(YuDbBPlusLeafStaticList* list, YuDbBPlusLeafElement element) {
	return element.next.next;
}
__forceinline void YuDbBPlusLeafStaticAccessor_SetNext(YuDbBPlusLeafStaticList* list, YuDbBPlusLeafElement element, int16_t new_next) {
	element.next.next = new_next;
}
void YuDbBPlusLeafStaticListInit(YuDbBPlusLeafStaticList* list, int16_t count) {
	list->list_first[0] = 0;
	int16_t i = 0;
	for (; i < count - 1; i++) {
		YuDbBPlusLeafStaticAccessor_SetNext(list, list->obj_arr[i], i + 1);
	}
	YuDbBPlusLeafStaticAccessor_SetNext(list, list->obj_arr[i], YuDbBPlusElementStaticListReferencer_InvalidId);
	for (i = 1; i < 1; i++) {
		list->list_first[i] = YuDbBPlusElementStaticListReferencer_InvalidId;
	}
}
void YuDbBPlusLeafStaticListExpand(YuDbBPlusLeafStaticList* list, int16_t old_count, int16_t new_count) {
	int16_t old_first = list->list_first[0];
	list->list_first[0] = new_count - 1;
	int16_t i = old_count;
	for (; i < new_count - 1; i++) {
		YuDbBPlusLeafStaticAccessor_SetNext(list, list->obj_arr[i], i + 1);
	}
	YuDbBPlusLeafStaticAccessor_SetNext(list, list->obj_arr[i], old_first);
}
int16_t YuDbBPlusLeafStaticListPop(YuDbBPlusLeafStaticList* list, int16_t list_order) {
	if (list->list_first[list_order] == YuDbBPlusElementStaticListReferencer_InvalidId) {
		return YuDbBPlusElementStaticListReferencer_InvalidId;
	}
	int16_t index = list->list_first[list_order];
	list->list_first[list_order] = YuDbBPlusLeafStaticAccessor_GetNext(list, list->obj_arr[index]);
	return index;
}
void YuDbBPlusLeafStaticListPush(YuDbBPlusLeafStaticList* list, int16_t list_order, int16_t index) {
	YuDbBPlusLeafStaticAccessor_SetNext(list, list->obj_arr[index], list->list_first[list_order]);
	list->list_first[list_order] = index;
}
int16_t YuDbBPlusLeafStaticListDelete(YuDbBPlusLeafStaticList* list, int16_t list_order, int16_t prev_id, int16_t delete_id) {
	if (prev_id == YuDbBPlusElementStaticListReferencer_InvalidId) {
		list->list_first[list_order] = YuDbBPlusLeafStaticAccessor_GetNext(list, list->obj_arr[delete_id]);
	}
	else {
		YuDbBPlusLeafStaticAccessor_SetNext(list, list->obj_arr[prev_id], YuDbBPlusLeafStaticAccessor_GetNext(list, list->obj_arr[delete_id]));
	}
	return delete_id;
}
void YuDbBPlusLeafStaticListSwitch(YuDbBPlusLeafStaticList* list, int16_t list_order, int16_t prev_id, int16_t id, int16_t new_list_order) {
	YuDbBPlusLeafStaticListDelete(list, list_order, prev_id, id);
	YuDbBPlusLeafStaticListPush(list, new_list_order, id);
}
int16_t YuDbBPlusLeafStaticListIteratorFirst(YuDbBPlusLeafStaticList* list, int16_t list_order) {
	return list->list_first[list_order];
}
int16_t YuDbBPlusLeafStaticListIteratorNext(YuDbBPlusLeafStaticList* list, int16_t cur_id) {
	return YuDbBPlusLeafStaticAccessor_GetNext(list, list->obj_arr[cur_id]);
}
static YuDbBPlusElement* YuDbBPlusElementGet(YuDbBPlusTree* tree, YuDbBPlusEntry* entry, int16_t element_id) {
	{
		if (!(element_id >= 0)) {
			*(int*)0 = 0;
		}
	}
	;
	if (entry->type == kBPlusEntryLeaf) {
		return (YuDbBPlusElement*)&entry->leaf.element_space.obj_arr[element_id];
	}
	else {
		return (YuDbBPlusElement*)&entry->index.element_space.obj_arr[element_id];
	}
}
static void YuDbBPlusElementSet(YuDbBPlusTree* tree, YuDbBPlusEntry* entry, int16_t element_id, YuDbBPlusElement* element) {
	{
		if (!(element_id >= 0)) {
			*(int*)0 = 0;
		}
	}
	;
	if (entry->type == kBPlusEntryLeaf) {
		entry->leaf.element_space.obj_arr[element_id].key = element->leaf.key;
		entry->leaf.element_space.obj_arr[element_id].value = element->leaf.value;
	}
	else if (entry->type == kBPlusEntryIndex) {
		entry->index.element_space.obj_arr[element_id].key = element->index.key;
		entry->index.element_space.obj_arr[element_id].child_id = element->index.child_id;
	}
}
static PageId YuDbBPlusElementGetChildId(YuDbBPlusTree* tree, const YuDbBPlusEntry* index, int16_t element_id) {
	if (element_id == -1) {
		return index->index.tail_child_id;
	}
	return index->index.element_space.obj_arr[element_id].child_id;
}
static void YuDbBPlusElementSetChildId(YuDbBPlusTree* tree, YuDbBPlusEntry* index, int16_t element_id, PageId entry_id) {
	if (element_id == -1) {
		index->index.tail_child_id = entry_id;
		return;
	}
	index->index.element_space.obj_arr[element_id].child_id = entry_id;
}
static int16_t YuDbBPlusElementCreate(YuDbBPlusTree* tree, YuDbBPlusEntry* entry) {
	int16_t element_id;
	if (entry->type == kBPlusEntryLeaf) {
		element_id = YuDbBPlusLeafStaticListPop(&entry->leaf.element_space, 0);
	}
	else {
		element_id = YuDbBPlusIndexStaticListPop(&entry->index.element_space, 0);
	} {
		if (!(element_id >= 0)) {
			*(int*)0 = 0;
		}
	}
	;
	return element_id;
}
static YuDbBPlusElement* YuDbBPlusElementRelease(YuDbBPlusTree* tree, YuDbBPlusEntry* entry, int16_t element_id) {
	{
		if (!(element_id >= 0)) {
			*(int*)0 = 0;
		}
	}
	;
	if (entry->type == kBPlusEntryLeaf) {
		YuDbBPlusLeafStaticListPush(&entry->leaf.element_space, 0, element_id);
		return (YuDbBPlusElement*)&entry->leaf.element_space.obj_arr[element_id];
	}
	else {
		YuDbBPlusIndexStaticListPush(&entry->index.element_space, 0, element_id);
		return (YuDbBPlusElement*)&entry->index.element_space.obj_arr[element_id];
	}
}
YuDbBPlusElementPos* YuDbBPlusCursorCur(YuDbBPlusTree* tree, YuDbBPlusCursor* cursor) {
	if (cursor->level < 0) {
		return ((void*)0);
	}
	return &cursor->stack.obj_arr[cursor->level];
}
YuDbBPlusElementPos* YuDbBPlusCursorUp(YuDbBPlusTree* tree, YuDbBPlusCursor* cursor) {
	if (cursor->level <= 0) {
		return ((void*)0);
	}
	return &cursor->stack.obj_arr[--cursor->level];
}
YuDbBPlusElementPos* YuDbBPlusCursorDown(YuDbBPlusTree* tree, YuDbBPlusCursor* cursor) {
	if (cursor->level + 1 >= cursor->stack.count) {
		return ((void*)0);
	}
	return &cursor->stack.obj_arr[++cursor->level];
}
BPlusCursorStatus YuDbBPlusCursorFirst(YuDbBPlusTree* tree, YuDbBPlusCursor* cursor, int32_t* key) {
	YuDbBPlusCursorStackVectorInit(&cursor->stack, 8, 1);
	cursor->stack.count = 0;
	cursor->level = -1;
	return YuDbBPlusCursorNext(tree, cursor, key);
}
void YuDbBPlusCursorRelease(YuDbBPlusTree* tree, YuDbBPlusCursor* cursor) {
	if (cursor->stack.capacity != 0) {
		YuDbBPlusCursorStackVectorRelease(&cursor->stack);
	}
}
BPlusCursorStatus YuDbBPlusCursorNext(YuDbBPlusTree* tree, YuDbBPlusCursor* cursor, int32_t* key) {
	YuDbBPlusElementPos cur;
	YuDbBPlusElementPos* parent = YuDbBPlusCursorCur(tree, cursor);
	if (parent) {
		YuDbBPlusEntry* parent_entry = YUDB_BUCKET_BPLUS_REFERENCER_Reference(tree, parent->entry_id);
		if (parent_entry->type == kBPlusEntryLeaf) {
			return kBPlusCursorEnd;
		}
		cur.entry_id = YuDbBPlusElementGetChildId(tree, parent_entry, parent->element_id);
		YUDB_BUCKET_BPLUS_REFERENCER_Dereference(tree, parent_entry);
	}
	else {
		cur.entry_id = tree->root_id;
	}
	YuDbBPlusEntry* cur_entry = YUDB_BUCKET_BPLUS_REFERENCER_Reference(tree, cur.entry_id);
	int8_t cmp_status = -1;
	if (cur_entry->element_count > 0) {
		cur.element_id = YuDbBPlusEntryRbTreeIteratorLocate(&cur_entry->rb_tree, key, &cmp_status);
		if (cmp_status == -1) {
		}
		else {
			if (cur_entry->type == kBPlusEntryIndex || cmp_status == 1) {
				cur.element_id = YuDbBPlusEntryRbTreeIteratorNext(&cur_entry->rb_tree, cur.element_id);
			}
		}
	}
	else {
		cur.element_id = -1;
	}
	YuDbBPlusCursorStackVectorPushTail(&cursor->stack, &cur);
	BPlusCursorStatus status = kBPlusCursorNext;
	if (cur_entry->type == kBPlusEntryLeaf) {
		if (cmp_status != 0) {
			status = kBPlusCursorNe;
		}
		else {
			status = kBPlusCursorEq;
		}
		cursor->leaf_status = status;
	}
	++cursor->level;
	YUDB_BUCKET_BPLUS_REFERENCER_Dereference(tree, cur_entry);
	return status;
}
static int16_t YuDbBPlusEntryInsertElement(YuDbBPlusTree* tree, YuDbBPlusEntry* entry, YuDbBPlusElement* insert_element) {
	int16_t element_id = YuDbBPlusElementCreate(tree, entry); {
		if (!(element_id != -1)) {
			*(int*)0 = 0;
		}
	}
	;
	YuDbBPlusElementSet(tree, entry, element_id, insert_element);
	YuDbBPlusEntryRbTreePut(&entry->rb_tree, element_id);
	entry->element_count++;
	return element_id;
}
static YuDbBPlusElement* YuDbBPlusEntryDeleteElement(YuDbBPlusTree* tree, YuDbBPlusEntry* entry, int16_t element_id) {
	{
		if (!(element_id != -1)) {
			*(int*)0 = 0;
		}
	}
	;
	YuDbBPlusEntryRbTreeDelete(&entry->rb_tree, element_id);
	entry->element_count--;
	return YuDbBPlusElementRelease(tree, entry, element_id);
}
PageId YuDbBPlusEntryCreate(YuDbBPlusTree* tree, BPlusEntryType type) {
	size_t size;
	if (type == kBPlusEntryIndex) {
		size = (tree->index_m - 1) * sizeof(YuDbBPlusIndexElement);
	}
	else {
		size = (tree->leaf_m - 1) * sizeof(YuDbBPlusLeafElement);
	}
	PageId entry_id = YUDB_BUCKET_BPLUS_ALLOCATOR_CreateBySize(tree, sizeof(YuDbBPlusEntry) + size);
	YuDbBPlusEntry* entry = YUDB_BUCKET_BPLUS_REFERENCER_Reference(tree, entry_id);
	entry->type = type;
	entry->element_count = 0;
	YUDB_BUCKET_BPLUS_REFERENCER_Dereference(tree, entry);
	YuDbBPlusEntryRbTreeInit(&entry->rb_tree);
	if (type == kBPlusEntryIndex) {
		YuDbBPlusIndexStaticListInit(&entry->index.element_space, tree->index_m - 1);
	}
	else {
		YuDbBPlusLeafStaticListInit(&entry->leaf.element_space, tree->leaf_m - 1);
	}
	return entry_id;
}
void YuDbBPlusEntryRelease(YuDbBPlusTree* tree, YuDbBPlusEntry* entry) {
	YUDB_BUCKET_BPLUS_ALLOCATOR_Release(tree, entry);
}
static YuDbBPlusElement YuDbBPlusEntrySplit(YuDbBPlusTree* tree, YuDbBPlusEntry* left, PageId left_id, YuDbBPlusEntry* parent, int16_t parent_element_id, YuDbBPlusElement* insert_element, int16_t insert_id, PageId* out_right_id) {
	PageId right_id = YuDbBPlusEntryCreate(tree, left->type);
	YuDbBPlusEntry* right = YUDB_BUCKET_BPLUS_REFERENCER_Reference(tree, right_id);
	YuDbBPlusElement up_element;
	int32_t mid;
	int32_t right_count;
	if (left->type == kBPlusEntryLeaf) {
		YuDbBPlusLeafListPutEntryNext(&tree->leaf_list, left_id, right_id);
		mid = tree->leaf_m / 2;
		right_count = left->element_count + 1 - mid;
	}
	else {
		mid = (tree->index_m - 1) / 2;
		right_count = left->element_count - mid;
	}
	int32_t i = right_count - 1;
	int16_t left_elemeng_id = YuDbBPlusEntryRbTreeIteratorLast(&left->rb_tree);
	_Bool insert = 0;
	for (; i >= 0; i--) {
		if (!insert && left_elemeng_id == insert_id) {
			YuDbBPlusEntryInsertElement(tree, right, insert_element);
			insert = 1;
			continue;
		} {
			if (!(left_elemeng_id != YuDbBPlusEntryRbReferencer_InvalidId)) {
				*(int*)0 = 0;
			}
		}
		;
		int16_t next_elemeng_id = YuDbBPlusEntryRbTreeIteratorPrev(&left->rb_tree, left_elemeng_id);
		YuDbBPlusEntryInsertElement(tree, right, YuDbBPlusEntryDeleteElement(tree, left, left_elemeng_id));
		left_elemeng_id = next_elemeng_id;
	}
	if (!insert) {
		YuDbBPlusEntryInsertElement(tree, left, insert_element);
	}
	if (left->type == kBPlusEntryLeaf) {
		up_element = *YuDbBPlusElementGet(tree, right, YuDbBPlusEntryRbTreeIteratorFirst(&right->rb_tree));
		int32_t key = up_element.leaf.key;
		up_element.index.key = key;
	}
	else {
		right->index.tail_child_id = left->index.tail_child_id;
		up_element = *YuDbBPlusEntryDeleteElement(tree, left, YuDbBPlusEntryRbTreeIteratorLast(&left->rb_tree));
		left->index.tail_child_id = up_element.index.child_id;
	}
	up_element.index.child_id = left_id;
	YuDbBPlusElementSetChildId(tree, parent, parent_element_id, right_id);
	*out_right_id = right_id;
	YUDB_BUCKET_BPLUS_REFERENCER_Dereference(tree, right);
	return up_element;
}
static void YuDbBPlusEntryMerge(YuDbBPlusTree* tree, YuDbBPlusEntry* left, PageId left_id, YuDbBPlusEntry* right, PageId right_id, YuDbBPlusEntry* parent, int16_t parent_index) {
	int16_t right_elemeng_id = YuDbBPlusEntryRbTreeIteratorLast(&right->rb_tree);
	for (int32_t i = 0; i < right->element_count; i++) {
		{
			if (!(right_elemeng_id != YuDbBPlusEntryRbReferencer_InvalidId)) {
				*(int*)0 = 0;
			}
		}
		;
		YuDbBPlusEntryInsertElement(tree, left, YuDbBPlusElementGet(tree, right, right_elemeng_id));
		right_elemeng_id = YuDbBPlusEntryRbTreeIteratorPrev(&right->rb_tree, right_elemeng_id);
	}
	if (left->type == kBPlusEntryLeaf) {
		YuDbBPlusLeafListDeleteEntry(&tree->leaf_list, right_id);
	}
	else {
		int16_t left_element_id = YuDbBPlusEntryInsertElement(tree, left, YuDbBPlusElementGet(tree, parent, parent_index));
		YuDbBPlusElementSetChildId(tree, left, left_element_id, left->index.tail_child_id);
		YuDbBPlusElementSetChildId(tree, left, -1, right->index.tail_child_id);
	}
	YuDbBPlusElementSetChildId(tree, parent, YuDbBPlusEntryRbTreeIteratorNext(&parent->rb_tree, parent_index), left_id);
	YUDB_BUCKET_BPLUS_REFERENCER_Dereference(tree, right);
	YuDbBPlusEntryRelease(tree, right_id);
}
static _Bool YuDbBPlusTreeInsertElement(YuDbBPlusTree* tree, YuDbBPlusCursor* cursor, YuDbBPlusElement* insert_element) {
	YuDbBPlusElementPos* cur_pos = YuDbBPlusCursorCur(tree, cursor);
	YuDbBPlusElementPos* parent_pos = YuDbBPlusCursorUp(tree, cursor);
	PageId right_id;
	YuDbBPlusEntry* cur = YUDB_BUCKET_BPLUS_REFERENCER_Reference(tree, cur_pos->entry_id);
	_Bool success = 1, insert_up = 0;
	YuDbBPlusElement up_element;
	do {
		if (cursor->leaf_status == kBPlusCursorEq) {
			YuDbBPlusElementSet(tree, cur, cur_pos->element_id, insert_element);
			break;
		}
		uint32_t m = cur->type == kBPlusEntryIndex ? tree->index_m : tree->leaf_m;
		if (cur->element_count < m - 1) {
			YuDbBPlusEntryInsertElement(tree, cur, insert_element);
			break;
		}
		if (cur_pos->element_id == -1) {
			cur_pos->element_id = YuDbBPlusEntryRbTreeIteratorLast(&cur->rb_tree);
		}
		else {
			cur_pos->element_id = YuDbBPlusEntryRbTreeIteratorPrev(&cur->rb_tree, cur_pos->element_id);
		}
		if (!parent_pos) {
			PageId parent_id = YuDbBPlusEntryCreate(tree, kBPlusEntryIndex);
			YuDbBPlusEntry* parent = YUDB_BUCKET_BPLUS_REFERENCER_Reference(tree, parent_id);
			up_element = YuDbBPlusEntrySplit(tree, cur, cur_pos->entry_id, parent, -1, insert_element, cur_pos->element_id, &right_id);
			YuDbBPlusEntryInsertElement(tree, parent, &up_element);
			tree->root_id = parent_id;
			YUDB_BUCKET_BPLUS_REFERENCER_Dereference(tree, parent);
			break;
		}
		YuDbBPlusEntry* parent = YUDB_BUCKET_BPLUS_REFERENCER_Reference(tree, parent_pos->entry_id);
		up_element = YuDbBPlusEntrySplit(tree, cur, cur_pos->entry_id, parent, parent_pos->element_id, insert_element, cur_pos->element_id, &right_id);
		YUDB_BUCKET_BPLUS_REFERENCER_Dereference(tree, parent);
		insert_up = 1;
	} while (0);
	YUDB_BUCKET_BPLUS_REFERENCER_Dereference(tree, cur);
	if (insert_up) {
		return YuDbBPlusTreeInsertElement(tree, cursor, &up_element);
	}
	return success;
}
static _Bool YuDbBPlusTreeDeleteElement(YuDbBPlusTree* tree, YuDbBPlusCursor* cursor) {
	YuDbBPlusElementPos* cur_pos = YuDbBPlusCursorCur(tree, cursor);
	YuDbBPlusElementPos* parent_pos = YuDbBPlusCursorUp(tree, cursor);
	YuDbBPlusEntry* entry = YUDB_BUCKET_BPLUS_REFERENCER_Reference(tree, cur_pos->entry_id);
	PageId sibling_entry_id = 0;
	YuDbBPlusEntry* sibling = ((void*)0);
	YuDbBPlusEntry* parent = ((void*)0);
	_Bool success = 1, delete_up = 0;
	YuDbBPlusEntryDeleteElement(tree, entry, cur_pos->element_id);
	do {
		uint32_t m = entry->type == kBPlusEntryIndex ? tree->index_m : tree->leaf_m;
		if (entry->element_count >= (m - 1) / 2) {
			break;
		}
		if (!parent_pos) {
			if (entry->type == kBPlusEntryIndex && entry->element_count == 0) {
				PageId childId = entry->index.tail_child_id;
				tree->root_id = childId;
				YUDB_BUCKET_BPLUS_REFERENCER_Dereference(tree, entry);
				YuDbBPlusEntryRelease(tree, cur_pos->entry_id);
				return 1;
			}
			else {
				break;
			}
		}
		parent = YUDB_BUCKET_BPLUS_REFERENCER_Reference(tree, parent_pos->entry_id);
		_Bool left_sibling = 1;
		int16_t common_parent_element_id = parent_pos->element_id;
		int16_t sibling_element_id;
		if (common_parent_element_id == -1) {
			sibling_element_id = YuDbBPlusEntryRbTreeIteratorLast(&parent->rb_tree);
		}
		else {
			sibling_element_id = YuDbBPlusEntryRbTreeIteratorPrev(&parent->rb_tree, common_parent_element_id);
			if (sibling_element_id == -1) {
				left_sibling = 0;
				sibling_element_id = YuDbBPlusEntryRbTreeIteratorNext(&parent->rb_tree, common_parent_element_id);
				if (sibling_element_id == -1) {
					sibling_entry_id = parent->index.tail_child_id;
				}
			}
		}
		if (sibling_element_id != -1) {
			sibling_entry_id = parent->index.element_space.obj_arr[sibling_element_id].child_id;
		}
		if (left_sibling) {
			common_parent_element_id = sibling_element_id;
			parent_pos->element_id = sibling_element_id;
		} {
			if (!(common_parent_element_id != -1)) {
				*(int*)0 = 0;
			}
		}
		; {
			if (!(sibling_entry_id != -1)) {
				*(int*)0 = 0;
			}
		}
		;
		sibling = YUDB_BUCKET_BPLUS_REFERENCER_Reference(tree, sibling_entry_id);
		if (sibling->element_count > (m - 1) / 2) {
			if (entry->type == kBPlusEntryLeaf) {
				if (left_sibling) {
					int16_t last = YuDbBPlusEntryRbTreeIteratorLast(&sibling->rb_tree); {
						if (!(last != -1)) {
							*(int*)0 = 0;
						}
					}
					;
					YuDbBPlusElement* element = YuDbBPlusEntryDeleteElement(tree, sibling, last);
					YuDbBPlusEntryInsertElement(tree, entry, element);
					parent->index.element_space.obj_arr[common_parent_element_id].key = element->leaf.key;
				}
				else {
					int16_t first = YuDbBPlusEntryRbTreeIteratorFirst(&sibling->rb_tree);
					int16_t new_first = YuDbBPlusEntryRbTreeIteratorNext(&sibling->rb_tree, first); {
						if (!(first != -1)) {
							*(int*)0 = 0;
						}
					}
					; {
						if (!(new_first != -1)) {
							*(int*)0 = 0;
						}
					}
					;
					YuDbBPlusElement* element = YuDbBPlusEntryDeleteElement(tree, sibling, first);
					YuDbBPlusEntryInsertElement(tree, entry, element);
					parent->index.element_space.obj_arr[common_parent_element_id].key = sibling->leaf.element_space.obj_arr[new_first].key;
				}
			}
			else {
				if (left_sibling) {
					int16_t last = YuDbBPlusEntryRbTreeIteratorLast(&sibling->rb_tree); {
						if (!(last != -1)) {
							*(int*)0 = 0;
						}
					}
					;
					YuDbBPlusElement* left_element = YuDbBPlusEntryDeleteElement(tree, sibling, last); {
						PageId temp = left_element->index.child_id;
						left_element->index.child_id = sibling->index.tail_child_id;
						sibling->index.tail_child_id = temp;
					}
					;
					YuDbBPlusElement* par_element = YuDbBPlusEntryDeleteElement(tree, parent, common_parent_element_id);
					par_element->index.child_id = left_element->index.child_id;
					YuDbBPlusEntryInsertElement(tree, entry, par_element);
					left_element->index.child_id = sibling_entry_id;
					YuDbBPlusEntryInsertElement(tree, parent, left_element);
				}
				else {
					int16_t first = YuDbBPlusEntryRbTreeIteratorFirst(&sibling->rb_tree); {
						if (!(first != -1)) {
							*(int*)0 = 0;
						}
					}
					;
					YuDbBPlusElement* right_element = YuDbBPlusEntryDeleteElement(tree, sibling, first);
					YuDbBPlusElement* par_element = YuDbBPlusEntryDeleteElement(tree, parent, common_parent_element_id);
					par_element->index.child_id = right_element->index.child_id; {
						PageId temp = par_element->index.child_id;
						par_element->index.child_id = entry->index.tail_child_id;
						entry->index.tail_child_id = temp;
					}
					;
					YuDbBPlusEntryInsertElement(tree, entry, par_element);
					right_element->index.child_id = cur_pos->entry_id;
					YuDbBPlusEntryInsertElement(tree, parent, right_element);
				}
			}
			break;
		}
		if (left_sibling) {
			YuDbBPlusEntryMerge(tree, sibling, sibling_entry_id, entry, cur_pos->entry_id, parent, common_parent_element_id);
			entry = ((void*)0);
		}
		else {
			YuDbBPlusEntryMerge(tree, entry, cur_pos->entry_id, sibling, sibling_entry_id, parent, common_parent_element_id);
			sibling = ((void*)0);
		}
		delete_up = 1;
	} while (0);
	if (parent) {
		YUDB_BUCKET_BPLUS_REFERENCER_Dereference(tree, parent);
	}
	if (sibling) {
		YUDB_BUCKET_BPLUS_REFERENCER_Dereference(tree, sibling);
	}
	if (entry) {
		YUDB_BUCKET_BPLUS_REFERENCER_Dereference(tree, entry);
	}
	if (delete_up) {
		return YuDbBPlusTreeDeleteElement(tree, cursor);
	}
	return success;
}
void YuDbBPlusTreeInit(YuDbBPlusTree* tree, uint32_t index_m, uint32_t leaf_m) {
	if (index_m < 3) {
		index_m = 3;
	}
	if (leaf_m < 3) {
		leaf_m = 3;
	}
	tree->index_m = index_m;
	tree->leaf_m = leaf_m;
	tree->root_id = YuDbBPlusEntryCreate(tree, kBPlusEntryLeaf);
	YuDbBPlusLeafListInit(&tree->leaf_list);
	YuDbBPlusLeafListPutFirst(&tree->leaf_list, tree->root_id);
}
_Bool YuDbBPlusTreeFind(YuDbBPlusTree* tree, int32_t* key) {
	YuDbBPlusCursor cursor;
	BPlusCursorStatus status = YuDbBPlusCursorFirst(tree, &cursor, key);
	while (status == kBPlusCursorNext) {
		status = YuDbBPlusCursorNext(tree, &cursor, key);
	}
	return status == kBPlusCursorEq;
}
_Bool YuDbBPlusTreeInsert(YuDbBPlusTree* tree, YuDbBPlusLeafElement* element) {
	YuDbBPlusCursor cursor;
	BPlusCursorStatus status = YuDbBPlusCursorFirst(tree, &cursor, &element->key);
	while (status == kBPlusCursorNext) {
		status = YuDbBPlusCursorNext(tree, &cursor, &element->key);
	}
	_Bool success = YuDbBPlusTreeInsertElement(tree, &cursor, (YuDbBPlusElement*)element);
	YuDbBPlusCursorRelease(tree, &cursor);
	return success;
}
_Bool YuDbBPlusTreeDelete(YuDbBPlusTree* tree, int32_t* key) {
	YuDbBPlusCursor cursor;
	BPlusCursorStatus status = YuDbBPlusCursorFirst(tree, &cursor, key);
	while (status == kBPlusCursorNext) {
		status = YuDbBPlusCursorNext(tree, &cursor, key);
	}
	if (status == kBPlusCursorNe) {
		return 0;
	}
	_Bool success = YuDbBPlusTreeDeleteElement(tree, &cursor);
	return success;
}


static PageId BucketEntryCopy(Bucket* bucket, BucketEntry* entry, PageId entry_pgid) {
	Tx* tx = BPlusTreeToTx(&bucket->bp_tree);
	PageId copy_pgid = YuDbBPlusEntryCreate(tx, entry->bp_entry.type);
	if (copy_pgid == kPageInvalidId) {
		return kPageInvalidId;
	}
	BucketEntry* copy_entry = PagerReference(&tx->db->pager, copy_pgid);
	memcpy(copy_entry, entry, tx->db->pager.page_size);
	copy_entry->last_write_tx_id = tx->meta_info.txid;
	if (copy_entry->bp_entry.type == kBPlusEntryLeaf) {
		// 当前是叶子节点，需要处理一下叶子节点的前后连接链表
		YuDbBPlusLeafListReplaceEntry(&bucket->bp_tree.leaf_list, entry_pgid, copy_pgid);

		// copy->next和prev已经拷贝了entry
		//PagerMarkDirty(&tx->db->pager, prev);
		//PagerMarkDirty(&tx->db->pager, next);
	}
	PagerDereference(&tx->db->pager, copy_entry);
	return copy_pgid;
}

/*
key最大只支持1页以内的长度
*/
void BucketInit(YuDb* db, Bucket* bucket) {
	uint32_t index_m = (db->pager.page_size - (sizeof(BucketEntry) - max(sizeof(YuDbBPlusLeafEntry), sizeof(YuDbBPlusIndexEntry)) + sizeof(YuDbBPlusIndexEntry))) / sizeof(YuDbBPlusIndexElement) + 1;
	uint32_t leaf_m = (db->pager.page_size - (sizeof(BucketEntry) - max(sizeof(YuDbBPlusLeafEntry), sizeof(YuDbBPlusIndexEntry)) + sizeof(YuDbBPlusLeafEntry))) / sizeof(YuDbBPlusLeafElement) + 1;
    YuDbBPlusTreeInit(&bucket->bp_tree, index_m, leaf_m);
}

bool BucketPut(Bucket* bucket, void* key_buf, int16_t key_size, void* value_buf, size_t value_size) {
	Tx* tx = BPlusTreeToTx(&bucket->bp_tree);
	if (tx->type != kTxReadWrite) {
		return false;
	}
	YuDbBPlusTree* tree = &tx->meta_info.bucket.bp_tree;

	//MemoryData key;
	//key.buf = key_buf;
	//key.size = key_size;
	//MemoryData value;
	//value.buf = value_buf;
	//value.size = value_size;
	YuDbBPlusLeafElement element;
	//element.key.memory.type = kDataMemory;
	//element.value.memory.type = kDataMemory;
	//element.key.memory.mem_data = ((uintptr_t)&key) >> 2;
	//element.value.memory.mem_data = ((uintptr_t)&value) >> 2;
	element.key = *(int32_t*)value_buf;

    YuDbBPlusCursor cursor;
    BPlusCursorStatus status = YuDbBPlusCursorFirst(tree, &cursor, &element.key);
	bool success = true;
    do  {
		YuDbBPlusElementPos* cur = YuDbBPlusCursorCur(tree, &cursor);
		BucketEntry* entry = (BucketEntry*)PagerReference(&tx->db->pager, cur->entry_id);
		if (tx->meta_info.txid != entry->last_write_tx_id) {
			PageId copy_id = BucketEntryCopy(&tx->meta_info.bucket, entry, cur->entry_id);
			if (copy_id == kPageInvalidId) {
				success = false;
				break;
			}
			PagerDereference(&tx->db->pager, &entry->bp_entry);
			PagerFree(&tx->db->pager, cur->entry_id, 1);

			// 回溯的id修改为拷贝的节点
			cur->entry_id = copy_id;

			// 需要修改上层的节点的元素指向拷贝的节点
			YuDbBPlusElementPos* up = YuDbBPlusCursorUp(tree, &cursor);
			if (up) {
				BucketEntry* up_entry = (BucketEntry*)PagerReference(&tx->db->pager, up->entry_id);
				YuDbBPlusElementSetChildId(tree, &up_entry->bp_entry, up->element_id, copy_id);
				PagerDereference(&tx->db->pager, up_entry);
				YuDbBPlusCursorDown(tree, &cursor);
			}
			else {
				tx->meta_info.bucket.bp_tree.root_id = copy_id;
			}
		}
		else {
			PagerDereference(&tx->db->pager, entry);
		}
		
		if (status != kBPlusCursorNext) {
			break;
		}
        status = YuDbBPlusCursorNext(tree, &cursor, &element.key);
	} while (true);
	if (success == false) {
		return false;
	}
    success = YuDbBPlusTreeInsertElement(tree, &cursor, &element);
    YuDbBPlusCursorRelease(tree, &cursor);
    return success;
}

bool BucketFind(Bucket* bucket, void* key_buf, int16_t key_size) {
	MemoryData key_data;
	key_data.buf = key_buf;
	key_data.size = key_size;
	//Key key;
	//key.memory.type = kDataMemory;
	//key.memory.mem_data = ((uintptr_t)&key_data) >> 2;;
	return YuDbBPlusTreeFind(&bucket->bp_tree, key_buf);
}


//
//
//void BPlusEntryDelete(Tx* tx, PageId pgid) {
//	BPlusEntry* entry = BPlusEntryGet(tx, pgid);
//	// wal模式时最后持久化的pending不能释放，要想个办法
//	if (entry->last_write_tx_id == tx->meta_info.txid) {
//		BPlusEntryDereference(tx, entry);
//		PagerFree(&tx->db->pager, pgid, 1);
//	}
//	else {
//		BPlusEntryDereference(tx, entry);
//		TxPendingListEntry* free_list_entry = (TxPendingListEntry*)RbTreeFindEntryByKey(&tx->db->tx_manager.pending_page_list, &tx->meta_info.txid);
//		PagerPending(&tx->db->pager, pgid, 1, free_list_entry->first_pending_pgid);
//		free_list_entry->first_pending_pgid = pgid;
//	}
//}
//


//void* GetDataBuf(Tx* tx, Data* data, void** data_buf, size_t* data_size) {
//	Bucket* bucket = (Bucket*)tx;
//	if (data->block.type == kDataBlock) {
//		void* page = PagerGet(&tx->db->pager, data->block.pgid);
//		*data_buf = (void*)((uintptr_t)page + (data->block.offset << 2));
//		*data_size = data->block.size;
//		return page;
//	}
//	else if (data->embed.type == kDataEmbed) {
//		*data_buf = data->embed.data;
//		*data_size = data->embed.size;
//	}
//	else if (data->each.type == kDataEach) {
//		*data_buf = NULL;
//		*data_size = data->each.size;
//		// 独立页面返回数据大小，要求调用ReadData提供缓冲区读取(允许分段)
//	}
//	else {		// data->memory.type == kDataMemory
//		MemoryData* mem_data = (MemoryData*)(data->memory.mem_data << 2);
//		*data_buf = mem_data->buf;
//		*data_size = mem_data->size;
//	}
//	return NULL;
//}
//
//static void SetDataBuf(Tx* tx, BPlusEntry* entry, Data* data, void* data_buf, size_t data_size) {
//	if (data_size <= sizeof(data->embed.data)) {
//		// 可以内嵌
//		data->embed.type = kDataEmbed;
//		data->embed.size = data_size;
//		memcpy(data->embed.data, data_buf, data_size);
//	}
//	else if (data_size <= sizeof(data->embed.data)) {
//		// 可以申请块来存放
//		// OverflowPageBlockAlloc(bucket, );
//		data->block.type = kDataBlock;
//		data->block.size = data_size;
//
//	}
//	else {
//		// 需要单独使用一个或多个连续页面存放
//		uint32_t page_count = data_size / tx->db->pager.page_size;
//		if (data_size % tx->db->pager.page_size) {
//			page_count++;
//		}
//		PageId pgid = PagerAlloc(&tx->db->pager, true, page_count);
//		PagerWrite(&tx->db->pager, pgid, data_buf, page_count);
//		data->each.type = kDataEach;
//		data->each.pgid = pgid;
//		data->each.size = data_size;
//	}
//}
//
//void BPlusElementSet(Tx* tx, BPlusEntry* entry, int i, BPlusElement* element) {
//	Key* key = NULL;
//	Value* value = NULL;
//	if (entry->type == kBPlusEntryLeaf) {
//		if (element->leaf.key.memory.type == kDataMemory) {
//			key = &entry->leaf.element[i].key;
//			value = &entry->leaf.element[i].value;
//		}
//		else {
//			entry->leaf.element[i] = element->leaf;
//		}
//	}
//	else if (entry->type == kBPlusEntryIndex) {
//		if (element->index.key.memory.type == kDataMemory) {
//			key = &entry->index.element[i].key;
//		}
//		else {
//			entry->index.element[i] = element->index;
//		}
//	}
//	if (key) {
//		if (value) {
//			MemoryData* mem_data = (MemoryData*)(element->leaf.key.memory.mem_data << 2);
//			SetDataBuf(tx, entry, key, mem_data->buf, mem_data->size);
//			mem_data = (MemoryData*)(element->leaf.value.memory.mem_data << 2);
//			SetDataBuf(tx, entry, value, mem_data->buf, mem_data->size);
//		}
//		else {
//			MemoryData* mem_data = (MemoryData*)(element->index.key.memory.mem_data << 2);
//			SetDataBuf(tx, entry, key, mem_data->buf, mem_data->size);
//		}
//	}
//}
//
//ptrdiff_t BPlusKeyCmp(Tx* tx, const Key* key1, const Key* key2) {
//	size_t key1_size, key2_size;
//	void* key1_buf, * key2_buf;
//	void* cache1 = GetDataBuf(tx, key1, &key1_buf, &key1_size);
//	void* cache2 = GetDataBuf(tx, key2, &key2_buf, &key2_size);
//	ptrdiff_t res = MemoryCmpR2(key1_buf, key1_size, key2_buf, key2_size);
//	if (cache1) {
//		PagerDereference(&tx->db->pager, cache1);
//	}
//	if (cache2) {
//		PagerDereference(&tx->db->pager, cache2);
//	}
//	return res;
//}
//
