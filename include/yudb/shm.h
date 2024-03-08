#pragma once

#include <mutex>

#include "yudb/noncopyable.h"
#include "yudb/meta_format.h"


namespace yudb {

#pragma pack(push, 1)
struct ShmStruct {
    uint32_t connections{ 0 };
    std::mutex update_lock;
    std::mutex meta_lock;
    MetaStruct meta_struct;
};
#pragma pack(pop)

class Shm : noncopyable {
public:
    Shm(ShmStruct* shm_struct) : 
        shm_struct_{ shm_struct } {}
    ~Shm() = default;

    void Recover() {
        std::construct_at(&shm_struct_->connections);
        std::construct_at(&shm_struct_->meta_lock);
        std::construct_at(&shm_struct_->update_lock);
    }
    
    auto& meta_struct() { return shm_struct_->meta_struct; }
    auto& update_lock() { return shm_struct_->update_lock; }
    auto& meta_lock() { return shm_struct_->meta_lock; }
    
private:
    ShmStruct* shm_struct_;
};

} // namespace yudb