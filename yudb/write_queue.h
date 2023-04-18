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

typedef struct _WriteQueue {
	WriteQueuePool pool;

	WriteQueueRbTree queue;
	WriteQueueRbTree immutable_queue;
	MutexLock mutex;
	enum _WriteQueueStatus status;
} WriteQueue;

void WriteQueueInit(WriteQueue* queue) {
	WriteQueueRbTreeInit(&queue->queue);
	WriteQueueRbTreeInit(&queue->immutable_queue);
	MutexLockInit(&queue->mutex);
}

typedef enum _WriteQueueStatus {
	kReady,
	kRunning,
	kSuspend,
	kStop,
	kDestroy,
} WriteQueueStatus;

void WriteThread(WriteQueue* queue, int duration) {
	do {
		Dormancy(duration);
		MutexLockAcquire(&queue->mutex);
		// 优先不可变队列，不可变队列清空时表示检查点落盘任务完成
		if (queue->queue.root != NULL) {

			if (queue->queue.root == NULL) {
				// 检查点落盘完成，同步
			}
		}
		else {

		}

		MutexLockRelease(&queue->mutex);
	} while (queue->status != kStop);
	queue->status = kDestroy;
}

void WriteQueuePush(WriteQueue* queue, PageId pgid, void* cache) {
	MutexLockAcquire(&queue->mutex);
	WriteQueueRbEntry* entry = WriteQueueRbTreeFind(&queue->queue, &pgid);
	if (entry == NULL) {
		WriteQueueRbTreePut(&queue->queue, );
	}
	MutexLockRelease(&queue->mutex);
}

/*
* 将当前队列封存为不可变队列，切换到新队列
* 若已有不可变队列则会阻塞
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
