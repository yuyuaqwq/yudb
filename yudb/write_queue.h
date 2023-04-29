#ifndef YUDB_WRITE_QUEUE_H_
#define YUDB_WRITE_QUEUE_H_

#include <stdio.h>
#include <stdint.h>

#include <CUtils/container/rb_tree.h>
#include <CUtils/concurrency/mutex_lock.h>
#include <CUtils/concurrency/thread.h>

#include <yudb/page.h>

#ifdef  __cplusplus
extern "C" {
#endif //  __cplusplus

CUTILS_CONTAINER_RB_TREE_DECLARATION(WriteQueue, struct _WalQueueRbEntry*, PageId)


typedef struct _WriteQueuePool {
	void* pool_buf;
} WriteQueuePool;

typedef struct _WriteQueueEntry {
	WriteQueueRbEntry rb_entry;
	PageId pgid;
} WriteQueueEntry;

typedef struct _WriteQueue {
	WriteQueuePool pool;

	WriteQueueRbTree queue;
	WriteQueueRbTree immutable_queue;
	MutexLock mutex;
	enum _WriteQueueStatus status;

	int32_t tx_count;		// 当前已进行的事务数量，达到一定数量时就会触发队列封存
} WriteQueue;


typedef enum _WriteQueueStatus {
	kReady,
	kRunning,
	kSuspend,
	kStop,
	kDestroy,
} WriteQueueStatus;


#define CUTILS_CONTINUE_RB_TREE_ACCESSOR_DEFALUT_GetKey(TREE, ENTRY) (((WriteQueueEntry*)ENTRY)->pgid)
#define YUDB_WRITE_QUEUE_RB_TREE_ACCESSOR CUTILS_CONTINUE_RB_TREE_ACCESSOR_DEFALUT
CUTILS_CONTAINER_RB_TREE_DEFINE(WriteQueue, struct _WalQueueRbEntry*, PageId, CUTILS_OBJECT_REFERENCER_DEFALUT, CUTILS_CONTINUE_RB_TREE_ACCESSOR_DEFALUT, CUTILS_OBJECT_COMPARER_DEFALUT)


void WriteQueueInit(WriteQueue* queue) {
	WriteQueueRbTreeInit(&queue->queue);
	WriteQueueRbTreeInit(&queue->immutable_queue);
	MutexLockInit(&queue->mutex); 
	queue->tx_count = 0;
}

void WriteQueueThread(WriteQueue* queue, int duration) {
	do {
		ThreadSleep(duration);
		MutexLockAcquire(&queue->mutex);
		// 优先处理不可变队列，不可变队列清空时表示检查点落盘任务完成
		if (queue->queue.root != NULL) {

			if (queue->queue.root == NULL) {
				// 检查点落盘任务完成，进行同步收尾

			}
		}
		else {

		}

		MutexLockRelease(&queue->mutex);
	} while (queue->status != kStop);
	queue->status = kDestroy;
}

void WriteQueuePut(WriteQueue* queue, PageId pgid, void* cache) {
	MutexLockAcquire(&queue->mutex);
	WriteQueueRbEntry* entry = WriteQueueRbTreeFind(&queue->queue, &pgid);
	if (entry == NULL) {
		//WriteQueueRbTreePut(&queue->queue, );
	}
	MutexLockRelease(&queue->mutex);
}

/*
* 将当前队列封存为不可变队列，切换到新队列
* 若有上一个未完成落盘任务的不可变队列则会阻塞
*/
void WriteQueueImmutable(WriteQueue* queue) {
	MutexLockAcquire(&queue->mutex);
	while (queue->immutable_queue.root != NULL) {
		MutexLockRelease(&queue->mutex);
		ThreadSwitch();
		MutexLockAcquire(&queue->mutex);
	}
	queue->immutable_queue.root = queue->queue.root;
	queue->queue.root = NULL;
	MutexLockRelease(&queue->mutex);
}

#ifdef __cplusplus
}
#endif //  __cplusplus

#endif // YUDB_WRITE_QUEUE_H_
