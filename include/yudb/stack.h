#pragma once

#include <cassert>

#include <array>

namespace yudb {

namespace detail {

template <typename T, size_t kSize>
class Stack {
public:
    Stack() = default;
    ~Stack() = default;

    //Stack(const Stack& right) {
    //    operator=(right);
    //}
    //void operator=(const Stack& right) {
    //    cur_pos_ = right.cur_pos_;
    //    for (auto i = 0; i < cur_pos_; i++) {
    //        array_[i] = right.array_[i];
    //    }
    //}

    //Stack(Stack&& right) {
    //    operator=(std::move(right));
    //}
    //void operator=(Stack&& right) {
    //    cur_pos_ = right.cur_pos_;
    //    for (auto i = 0; i < cur_pos_; i++) {
    //        array_[i] = std::move(right.array_[i]);
    //    }
    //    right.cur_pos_ = 0;
    //}


    T& front() noexcept {
        assert(!empty());
        return array_[cur_pos_ - 1];
    }

    const T& front() const noexcept {
        return array_[cur_pos_ - 1];
    }

    constexpr void push_back(const T& value) {
        assert(cur_pos_ < kSize);
        array_[cur_pos_++] = value;
    }

    void pop_back() {
        assert(cur_pos_ > 0);
        --cur_pos_;
    }

    void clear() {
        cur_pos_ = 0;
    }

    bool empty() const {
        return cur_pos_ == 0;
    }

    size_t size() const { return cur_pos_; }


    const T& operator[](size_t pos) const noexcept  { return array_[pos]; }

    T& operator[](size_t pos) noexcept { return array_[pos]; }

private:
    std::array<T, kSize> array_;
    ptrdiff_t cur_pos_{ 0 };
};

} // namespace detail

} // namespace yudb