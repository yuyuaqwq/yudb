#pragma once

#include <cstdint>
#include <cstring>
#include <cassert>

#include <utility>

#include "page_format.h"

namespace yudb {

#pragma pack(push, 1)
struct Cell {
    enum class Type : uint8_t {
        kInvalid = 0,
        kEmbed,
        kBlock,
        kPage,
    };

    Cell() = default;

    Cell(const Cell&) = delete;
    void operator=(const Cell&) = delete;

    Cell(Cell&& right) noexcept {
        operator=(std::move(right));
    }

    void operator=(Cell&& right) noexcept {
        std::memcpy(this, &right, sizeof(Cell));
        right.type = Type::kInvalid;
    }

    union {
        struct {
            Type type : 2;
            uint8_t bucket_flag : 1;
            uint8_t invalid_1 : 5;
            uint8_t invalid_2[5];
        };
        struct {
            uint8_t reserve : 4;
            uint8_t size : 4;
            uint8_t data[5];
        } embed;
        struct {
            uint8_t reserve : 3;
            uint8_t entry_index_high : 5;
            uint8_t entry_index_low;
            uint16_t size;
            PageOffset offset;
            const uint16_t entry_index() const {
                return (entry_index_high << 8) | entry_index_low;
            }
            void set_entry_index(uint16_t entry_index) {
                assert(entry_index < (1 << 13));
                entry_index_high = (entry_index << 8) & 0xff;
                entry_index_low = entry_index & 0xff;
            }
        } block;
    };
};
#pragma pack(pop)

static_assert(sizeof(Cell) == 6, "");

} // namespace yudb 