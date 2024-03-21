#ifndef POOL_MEMORY_POOL_HPP_
#define POOL_MEMORY_POOL_HPP_

#include <vector>

#include <pool/detail.hpp>

namespace pool {

template <class T, size_t block_size = 4096>
class MemoryPool {
public:
    static_assert(!std::is_const_v<T>, "The C++ Standard forbids containers of const elements "
        "because allocator<const T> is ill-formed.");

    using value_type = T;
    using size_type = size_t;
    using difference_type = ptrdiff_t;
    using propagate_on_container_move_assignment = std::true_type;
    using is_always_equal = std::true_type;

    template <typename U> struct rebind {
        using other =  MemoryPool<U>;
    };

private:
	union Slot {
        Slot* next;
        value_type element;
    };
    static constexpr size_t kBlockSlotCount = block_size / sizeof(Slot);

public:
    MemoryPool() noexcept {
        free_slot_ = nullptr;
        current_slot_ = nullptr;
        end_slot_ = nullptr;

        current_block_ = nullptr;
    }
    constexpr ~MemoryPool() noexcept {
        while (current_block_) {
            auto next = current_block_->next;
            operator delete(reinterpret_cast<void*>(current_block_));
            current_block_ = next;
        }
    }

    MemoryPool(MemoryPool&& other) noexcept {
        operator=(std::move(other));
    }
    void operator=(MemoryPool&& other) noexcept {
        if (other != this) {
            free_slot_ = other.free_slot_;
            current_slot_ = other.current_slot_;
            end_slot_ = other.end_slot_;
            other.current_block_ = nullptr;
            other.free_slot_ = nullptr;
            other.current_slot_ = nullptr;
            other.end_slot_ = nullptr;
        }
    }

    MemoryPool(const MemoryPool&) = delete;
    void operator=(const MemoryPool&) = delete;

    template <class U> MemoryPool(const MemoryPool<U>& pool) noexcept :
        MemoryPool()
    {}

    [[nodiscard]] T* allocate(std::size_t n = 1) {
        //if (n > 1) {
        //    throw std::runtime_error("memory pool does not support allocating multiple elements.");
        //}
        if (free_slot_ != nullptr) {
            auto res = free_slot_;
            free_slot_ = res->next;
            return &res->element;
        } else {
            if (current_slot_ >= end_slot_) {
                CreateBlock();
            }
            return &(current_slot_++)->element;
        }
    }
    void deallocate(T* free_ptr, std::size_t n = 1) {
        if (free_ptr != nullptr) {
            auto free_slot = reinterpret_cast<Slot*>(free_ptr);
            free_slot->next = free_slot_;
            free_slot_ = free_slot;
        }
    }

private:
    inline void CreateBlock() {
        auto new_block = reinterpret_cast<Slot*>(operator new(kBlockSlotCount * sizeof(Slot)));
        new_block[0].next = current_block_;
        current_block_ = new_block;
        current_slot_ = &new_block[1];
        end_slot_ = &new_block[kBlockSlotCount];
    }

private:
    Slot* current_block_;

    Slot* free_slot_;
    Slot* current_slot_;
    Slot* end_slot_;
};

template< class T1, class T2 >
constexpr bool operator==(const MemoryPool<T1>& lhs, const MemoryPool<T2>& rhs) noexcept {
    return true;
}

}  // namespace pool

#endif  // POOL_MEMORY_POOL_HPP_