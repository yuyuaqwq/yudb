#pragma once

#include <atomic>

#include "yudb/noncopyable.h"
#include "yudb/meta_format.h"

namespace yudb {

#pragma pack(push, 1)
struct ShmStruct {
    std::atomic_flag update_lock;
    MetaStruct meta;
};
#pragma pack(pop)

class Shm : noncopyable {
public:
    Shm(ShmStruct* shm_struct) : shm_struct_{ shm_struct } {}
    ~Shm() = default;

    void Init() {
        UnlockUpdate();
    }

    void LockUpdate() {
        while (shm_struct_->update_lock.test_and_set(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
    }

    void UnlockUpdate() {
        shm_struct_->update_lock.clear(std::memory_order_release);
    }

    auto& shm_struct() const { return *shm_struct_; }

private:
    ShmStruct* shm_struct_;
};

} // namespace yudb