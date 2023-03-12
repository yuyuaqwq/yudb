#ifndef YUDB_FREETABLE_H_
#define YUDB_FREETABLE_H_

#include <stdint.h>
#include <stdbool.h>

#ifdef  __cplusplus
extern "C" {
#endif //  __cplusplus

typedef struct _Free1Block {
	uint16_t next_block_offset;
	uint16_t len;
} Free1Block;

typedef struct _Free1Table_ {
	uint16_t first_block;		// 前16字节指向第一个空闲块
} Free1Table_;

extern const uint16_t kFreeTableInvalidOffset;


void Free1TableInit_(Free1Table_* table, int16_t size);
uint16_t Free1TableAlloc_(Free1Table_* table, uint16_t len);
void Free1TableFree_(Free1Table_* page, uint16_t free_offset, uint16_t len);

#ifdef __cplusplus
}
#endif //  __cplusplus

#endif // YUDB_FREETABLE_H_