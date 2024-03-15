#include <string>

namespace yudb {

inline std::string RandomString(size_t min_size, size_t max_size) {
    int size;
    if (min_size == max_size) {
        size = min_size;
    } else {
        size = (rand() % (max_size - min_size)) + min_size;
    }
    std::string str(size, ' ');
    for (auto i = 0; i < size; i++) {
        str[i] = rand() % 26 + 'a';
    }
    return str;
}

inline std::string RandomByteArray(size_t min_size, size_t max_size) {
    int size;
    if (min_size == max_size) {
        size = min_size;
    } else {
        size = (rand() % (max_size - min_size)) + min_size;
    }
    std::string str(size, ' ');
    for (auto i = 0; i < size; i++) {
        str[i] = rand() % 256;
    }
    return str;
}

} // namespace yudb