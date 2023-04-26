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

	uint16_t wal_max_page_count;		// wal记录的最大连续页面数量，超过这个数量则不会写到wal文件，而是直接写回对应的页面
} Config;

#ifdef __cplusplus
}
#endif //  __cplusplus

#endif // YUDB_CONFIG_H_