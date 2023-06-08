#ifndef YUDB_CONFIG_H_
#define YUDB_CONFIG_H_

#include <stdbool.h>
#include <stdint.h>

#ifdef  __cplusplus
extern "C" {
#endif //  __cplusplus


typedef enum _ConfigSyncMode {
	kConfigSyncOff = 0,
	kConfigSyncNormal = 1,
	kConfigSyncFull = 2,
} ConfigSyncMode;

typedef enum _ConfigUpdateMode {
	kConfigUpdateInPlace = 0,
	kConfigUpdateWal = 1,
} ConfigUpdateMode;

typedef struct _Config {
	ConfigSyncMode sync_mode;
	ConfigUpdateMode update_mode;

	uint16_t page_size;					// 页面尺寸，若该尺寸与数据库尺寸不匹配则无法打开数据库
	uint16_t cacher_page_count;			// 最大缓存页面数量

	uint8_t hotspot_queue_full_percentage;		// 热点队列队满百分比，UpdateInPlace模式下置为100，Wal模式下建议为50

	uint32_t wal_max_page_count;		// wal记录的最大连续页面数量，超过这个数量则不会写到wal文件，而是直接写回对应的页面
	uint32_t wal_max_tx_count;			// 同一wal记录的最大事务数量，超过这个数量就会进行封存为不可变日志，启用另一份日志继续记录
	uint32_t wal_write_thread_disk_drop_interval;		// wal后台线程落盘间隔，单位毫秒
} Config;

#ifdef __cplusplus
}
#endif //  __cplusplus

#endif // YUDB_CONFIG_H_