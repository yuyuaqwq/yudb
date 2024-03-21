#ifndef POOL_DETAIL_HPP_
#define POOL_DETAIL_HPP_

namespace pool {
namespace detail {

consteval size_t bit_shift(size_t base) {
    size_t index = 0;
    while (base >>= 1) {
        ++index;
    }
    return index;
}

consteval size_t bit_fill(size_t bit_count) {
    size_t val = 0;
    for (size_t i = 0; i < bit_count; i++) {
        val <<= 1;
        val |= 1;
    }
    return val;
}

} // namespace detail
} // namespace mempool

#endif  // POOL_DETAIL_HPP_