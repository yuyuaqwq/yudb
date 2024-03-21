#ifndef POOL_STATIC_MEMORY_POOL_HPP_
#define POOL_STATIC_MEMORY_POOL_HPP_

#include <pool/memory_pool.hpp>

namespace pool {

template <class T, size_t block_size = 4096>
class StaticMemoryPool {
public:
    using value_type = T;
    using size_type = size_t;
    using difference_type = ptrdiff_t;
    using propagate_on_container_move_assignment = std::true_type;
    using is_always_equal = std::true_type;

    template <typename U> struct rebind {
        using other = StaticMemoryPool<U>;
    };


    StaticMemoryPool() noexcept {}
    constexpr ~StaticMemoryPool() {}
    template <class U> StaticMemoryPool(const StaticMemoryPool<U>& pool) noexcept :
        StaticMemoryPool()
    {}

    [[nodiscard]] T* allocate(std::size_t n = 1) {
        return s_pool_.allocate(n);
    }

    void deallocate(T* free_ptr, std::size_t n = 1) {
        s_pool_.deallocate(free_ptr, n);
    }

private:
    static MemoryPool<T, block_size> s_pool_;
};

template <typename T, size_t block_size>
MemoryPool<T, block_size> StaticMemoryPool<T, block_size>::s_pool_;

template< class T1, class T2 >
constexpr bool operator==(const StaticMemoryPool<T1>& lhs, const StaticMemoryPool<T2>& rhs) noexcept {
    return true;
}

} // namespace pool

#endif // POOL_STATIC_MEMORY_POOL_HPP_