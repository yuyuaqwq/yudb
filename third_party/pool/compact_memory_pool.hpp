#ifndef POOL_COMPACT_MEMORY_POOL_HPP_
#define POOL_COMPACT_MEMORY_POOL_HPP_

#include <vector>

#include <pool/detail.hpp>

namespace pool {

template <class T, size_t block_size = 4096>
class CompactMemoryPool {
public:
    using value_type = T;
    using size_type = uint32_t;
    using difference_type = uint32_t;
    using propagate_on_container_move_assignment = std::true_type;

    template <typename U> struct rebind {
        using other = CompactMemoryPool<U>;
    };

private:
    using SlotPos = uint32_t;
    using BlockId = uint32_t;
    using SlotId = uint32_t;
	union Slot {
        SlotPos next;
        T element;
    };
    struct BlockInfo {
        Slot* slot_array;
    };

    static constexpr size_t kBlockSlotCount = block_size / sizeof(Slot);
    static constexpr size_t kBlockShift = detail::bit_shift(kBlockSlotCount);
    static constexpr size_t kSlotMark = kBlockSlotCount - 1;
    static constexpr SlotPos kSlotPosInvalid = detail::bit_fill(sizeof(SlotPos));
  
public:
    CompactMemoryPool() noexcept {
        free_slot_ = kSlotPosInvalid;
        current_slot_ = kSlotPosInvalid;
        end_slot_ = kSlotPosInvalid;
    }
    constexpr ~CompactMemoryPool() noexcept {
        for (auto& block : block_table_) {
            operator delete(reinterpret_cast<void*>(block.slot_array));
        }
    }

    CompactMemoryPool(const CompactMemoryPool&) = delete;
    void operator=(const CompactMemoryPool&) = delete;

    template <class U> CompactMemoryPool(const CompactMemoryPool<U>& pool) noexcept :
        CompactMemoryPool()
    {}

    [[nodiscard]] SlotPos allocate(std::size_t n = 1) {
        //if (n > 1) {
        //    throw std::runtime_error("memory pool does not support allocating multiple elements.");
        //}
        SlotPos res{};
        if (free_slot_ != kSlotPosInvalid) {
            res = free_slot_;
            auto [block_id, slot_id] = SplitId(res);
            auto& alloc_slot = block_table_[block_id].slot_array[slot_id];
            free_slot_ = alloc_slot.next;
            return res;
        } else {
            if (current_slot_ >= end_slot_) {
                CreateBlock();
            }
            return current_slot_++;
        }
    }
    void deallocate(SlotPos free_pos, std::size_t n = 1) {
        if (free_pos != kSlotPosInvalid) {
            auto [block_id, slot_id] = SplitId(free_pos);
            auto& slot = block_table_[block_id].slot_array[slot_id];
            slot.next = free_slot_;
            free_slot_ = free_pos;
        }
    }

    T* reference(SlotPos pos) {
        auto [block_id, slot_id] = SplitId(pos);
        auto& slot = block_table_[block_id].slot_array[slot_id];
        return &slot.element;
    }
    void dereference(T*) {

    }

private:
    std::tuple<BlockId, SlotId> SplitId(const SlotPos& slot_pos) const noexcept {
        BlockId block_id = slot_pos / kBlockSlotCount;
        SlotId slot_id = slot_pos % kBlockSlotCount;
        //BlockId block_id = slot_pos >> kBlockShift;
        //SlotId slot_id = slot_pos & kSlotMark;
        return std::tuple{ block_id, slot_id };
    }

    void CreateBlock() {
        auto new_block = reinterpret_cast<Slot*>(operator new(kBlockSlotCount * sizeof(Slot)));
        auto slot_pos = block_table_.size() * kBlockSlotCount;
        block_table_.emplace_back(new_block);

        current_slot_ = slot_pos;
        end_slot_ = slot_pos + kBlockSlotCount;
    }

private:
    std::vector<BlockInfo> block_table_;

    SlotPos free_slot_;

    SlotPos current_slot_;
    SlotPos end_slot_;
};

template< class T1, class T2 >
constexpr bool operator==(const CompactMemoryPool<T1>& lhs, const CompactMemoryPool<T2>& rhs) noexcept {
    return true;
}

}  // namespace pool

#endif  // POOL_COMPACT_MEMORY_POOL_HPP_