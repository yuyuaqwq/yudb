//The MIT License(MIT)
//Copyright © 2024 https://github.com/yuyuaqwq
//
//Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files(the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and /or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
//
//The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
//THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

#pragma once

#include <atomic>
#include <mutex>

#include <yudb/noncopyable.h>
#include <yudb/meta_format.h>

namespace yudb {

#pragma pack(push, 1)
struct ShmStruct {
    std::atomic<uint32_t> connections{ 0 };
    std::mutex update_lock;
    std::mutex meta_lock;
    MetaStruct meta_struct;
};
#pragma pack(pop)

class Shm : noncopyable {
public:
    Shm(ShmStruct* shm_struct) : 
        shm_struct_{ shm_struct }
    {
        ++shm_struct_->connections;
    }

    ~Shm() {
        --shm_struct_->connections;
    }

    auto& connections() const { return shm_struct_->connections; }
    auto& connections() { return shm_struct_->connections; }
    auto& meta_struct() const { return shm_struct_->meta_struct; }
    auto& meta_struct() { return shm_struct_->meta_struct; }
    auto& update_lock() { return shm_struct_->update_lock; }
    auto& meta_lock() { return shm_struct_->meta_lock; }
    
private:
    ShmStruct* const shm_struct_;
};

} // namespace yudb
