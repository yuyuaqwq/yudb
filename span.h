#pragma once

#include <cstdint>

#include "page.h"

namespace yudb {

#pragma pack(push, 1)
struct Span {
    enum class Type : uint8_t {
        kInvalid = 0,
        kEmbed,
        kBlock,
        kPage,
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
        uint8_t is_bucket_or_is_inline_bucket : 1;
        struct {
            uint8_t reserve : 4;
            uint8_t size : 4;
            uint8_t data[5];
        } embed;
        struct {
            uint8_t reserve : 3;
            uint8_t record_index_high : 5;
            uint8_t record_index_low;
            uint16_t size;
            PageOffset offset;

            uint16_t record_index() const {
                return (record_index_high << 8) | record_index_low;
            }

            void set_record_index(uint16_t record_index) {
                assert(record_index < (1 << 13));
                record_index_high = (record_index << 8) & 0xff;
                record_index_low = record_index & 0xff;
            }

        } block;
    };
};
#pragma pack(pop)

static_assert(sizeof(Span) == 6, "");

} // namespace yudb 