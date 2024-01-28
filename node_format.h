#pragma once

#include "tx_format.h"
#include "page_format.h"
#include "cell.h"
#include "block.h"
#include "page_arena_format.h"

namespace yudb {

#pragma pack(push, 1)
struct NodeFormat {
    enum class Type : uint16_t {
        kInvalid = 0,
        kBranch,
        kLeaf,
    };

    struct BranchElement {
        Cell key;
        PageId left_child;
    };

    struct LeafElement {
        Cell key;
        Cell value;
    };

    NodeFormat(const NodeFormat&) = delete;
    void operator=(const NodeFormat&) = delete;

    // 当分配第二页时，同时在第二页前部分创建一个最大溢出页面大小的空间管理(最大溢出)，负责分配空闲空间
    // 管理溢出页面的页号、最大剩余空间，其他溢出的数据页就各自管理各自的页内空闲空间即可
    // 空间管理动态扩展，在溢出页中分配新的

    union {
        struct {
            TxId last_modified_txid;
            Type type : 2;
            uint16_t element_count : 14;
            BlockTableDescriptor block_table_descriptor;
            PageArenaFormat page_arena_format;
            uint16_t padding;
            union {
                struct {
                    PageId tail_child;
                    BranchElement branch[1];
                };
                LeafElement leaf[1];
            } body;
        };
        uint8_t page[1];
    };
};
#pragma pack(pop)


constexpr auto aaa = sizeof(NodeFormat) - sizeof(NodeFormat::body);
static_assert(sizeof(NodeFormat) - sizeof(NodeFormat::body) >= sizeof(NodeFormat::LeafElement) * 2, "abnormal length of head node.");

} // namespace