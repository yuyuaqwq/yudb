#pragma once

#include <atomic>

#include "yudb/noncopyable.h"

namespace yudb {

#pragma pack(push, 1)
struct ShmStruct {
    std::atomic_flag update_lock_;
};
#pragma pack(pop)

class Shm : noncopyable {
public:
    Shm(ShmStruct* shm) : shm_{ shm } {}
    ~Shm() = default;

    void Init() {
        UnlockUpdate();
    }

    void LockUpdate() {
        while (shm_->update_lock_.test_and_set(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
    }

    void UnlockUpdate() {
        shm_->update_lock_.clear(std::memory_order_release);
    }

private:
    ShmStruct* shm_;
};

} // namespace yudb