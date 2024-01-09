#pragma once

#include <cstdint>

#include "page.h"

namespace yudb {

#pragma pack(push, 1)
struct Span {
    enum class Type : uint16_t {
        kInvalid = 0,
        kEmbed,
        kBlock,
        kPageRecord,
    };

    Span() = default;

    Span(const Span&) = delete;
    void operator=(const Span&) = delete;

    Span(Span&& right) noexcept {
        operator=(std::move(right));
    }

    void operator=(Span&& right) noexcept {
        std::memcpy(this, &right, sizeof(Span));
        right.type = Type::kInvalid;
    }

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
            uint16_t record_index;
            PageOffset offset;
        } block;
        struct {
            Type type : 2;
            uint16_t size : 14;
            uint16_t record_index;
            PageOffset offset;
        } page_record;
    };
};
#pragma pack(pop)


} // namespace yudb 