#pragma once

#include <array>

namespace yudb {

namespace detail {

template <typename T, size_t kSize>
class Stack {
public:
    T& front() noexcept {
        return array_[cur_pos_ - 1];
    }

    constexpr void push_back(const T& value) {
        array_[cur_pos_++] = value;
    }

    void pop_back() {
        --cur_pos_;
    }

    void clear() {
        cur_pos_ = 0;
    }

    bool empty() {
        return cur_pos_ == 0;
    }

private:
    std::array<T, kSize> array_;
    uint32_t cur_pos_{ 0 };
};

} // namespace detail

} // namespace yudb