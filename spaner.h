#pragma once

#include "noncopyable.h"

namespace yudb {

class Noder;

/*
* 所有权在内存中的Span
*/
class Spaner : noncopyable {
public:
    Spaner(Noder* noder, Span&& span) : noder_{ noder }, span_{ std::move(span) } {
        span.type = Span::Type::kInvalid;
    }

    Spaner(Spaner&& other) noexcept {
        *this = std::move(other);
    }
    void operator=(Spaner&& other) noexcept {
        noder_ = other.noder_;
        span_ = other.span_;
        other.span_.type == Span::Type::kInvalid;
    }

    /*
    * 释放持有的所有权
    */
    Span Release() {
        auto temp = span_;
        span_.type = Span::Type::kInvalid;
        return temp;
    }

    Span& span() { return span_; }
    Noder* noder() { return noder_; }

private:
    Noder* noder_;

    Span span_;
};

} // namespace yudb