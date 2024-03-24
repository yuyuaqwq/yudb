//The MIT License(MIT)
//Copyright © 2024 https://github.com/yuyuaqwq
//
//Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files(the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and /or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
//
//The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
//THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

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

    Stack(const Stack& right) {
        operator=(right);
    }
    void operator=(const Stack& right) {
        cur_pos_ = right.cur_pos_;
        for (auto i = 0; i < cur_pos_; i++) {
            array_[i] = right.array_[i];
        }
    }

    Stack(Stack&& right) {
        operator=(std::move(right));
    }
    void operator=(Stack&& right) {
        cur_pos_ = right.cur_pos_;
        for (auto i = 0; i < cur_pos_; i++) {
            array_[i] = std::move(right.array_[i]);
        }
        right.cur_pos_ = 0;
    }


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