#pragma once

namespace yudb {

class noncopyable {
public:
    noncopyable() = default;
    noncopyable(const noncopyable&) = delete;
    void operator=(const noncopyable&) = delete;
};

} // namespace yudb