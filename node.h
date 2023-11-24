#pragma once

#include "page.h"
#include "overflow.h"

namespace yudb {

#pragma pack(push, 1)
struct Node {
    enum class Type : uint16_t {
        kBranch = 0,
        kLeaf,
    };

    struct Cell {
        enum class Type : uint16_t {
            kEmbed = 0,
            kBlock,
            kPageTable,
        };

        union {
            Type type : 2;
            struct {
                uint8_t type : 2;
                uint8_t invalid : 2;
                uint8_t size : 4;
                uint8_t data[5];
            } embed;
            struct {
                Type type : 2;
                uint16_t size : 14;
                uint16_t overflow_index;
                PageOffset offset;
            } block;
            struct {
                Type type : 2;
                uint16_t size : 14;
                uint16_t overflow_index;
                PageOffset offset;
            } page_table;
        };
    };

    struct BranchElement {
        Cell key;
        PageId min_child;
    };

    struct LeafElement {
        Cell key;
        Cell value;
    };

    // 当分配第二页时，同时在第二页前部分创建一个最大溢出页面大小的空间管理(最大溢出)，负责分配空闲空间
    // 管理溢出页面的页号、最大剩余空间，其他溢出的数据页就各自管理各自的页内空闲空间即可
    // 空间管理动态扩展，在溢出页中分配新的

    union {
        struct {
            Type type : 2;
            uint16_t element_count : 14;

            Overflow overflow;

            //TxId last_write_txid;
            union {
                BranchElement branch[];
                LeafElement leaf[];
            };
        };
        uint8_t full[];
    };
};
#pragma pack(pop)

} // namespace