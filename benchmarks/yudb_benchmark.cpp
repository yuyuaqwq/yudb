#include <filesystem>

#define BENCHMARK_STATIC_DEFINE
#include <benchmark/benchmark.h>

#include "yudb/db.h"

std::string RandomString(size_t min_size, size_t max_size) {
    int size;
    if (min_size == max_size) {
        size = min_size;
    }
    else {
        size = (rand() % (max_size - min_size)) + min_size;
    }
    std::string str(size, ' ');
    for (auto i = 0; i < size; i++) {
        str[i] = rand() % 26 + 'a';
    }
    return str;
}

class Benchmark {
public:
    std::unique_ptr<yudb::DB> db_;
    int seed_{ 0 };
    int count_{ 1000000 };

    void Run() {
        yudb::Options options{
            .max_wal_size = 1024 * 1024 * 64,
        };

        srand(seed_);

        std::string path = "Z:/yudb_benchmark.ydb";
        std::filesystem::remove(path);
        std::filesystem::remove(path + "-shm");
        std::filesystem::remove(path + "-wal");
        db_ = yudb::DB::Open(options, path);

        std::vector<std::string> key(count_);
        std::vector<std::string> value(count_);
        for (auto i = 0; i < count_; i++) {
            key[i] = RandomString(16, 16);
            value[i] = RandomString(100, 100);
        }


        {
            auto start_time = std::chrono::high_resolution_clock::now();
            auto tx = db_->Update();
            auto bucket = tx.UserBucket();
            for (int i = 0; i < count_; ++i) {
                bucket.Put(key[i].data(), key[i].size(), value[i].data(), value[i].size());
                ++i;
            }
            tx.Commit();
            auto end_time = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
            std::cout << "put: " << duration.count() << " microseconds" << std::endl;
        }

        {
            auto start_time = std::chrono::high_resolution_clock::now();
            auto tx = db_->View();
            auto bucket = tx.UserBucket();
            for (int i = 0; i < count_; ++i) {
                auto iter = bucket.Get(key[i].data(), key[i].size());
                if (iter == bucket.end() || iter->value() != value[i]) {
                    printf("%d error\n", i);
                }
                ++i;
            }
            auto end_time = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
            std::cout << "get: " << duration.count() << " microseconds" << std::endl;
        }

        {
            auto start_time = std::chrono::high_resolution_clock::now();
            auto tx = db_->Update();
            auto bucket = tx.UserBucket();
            for (int i = 0; i < count_; ++i) {
                bucket.Delete(key[i].data(), key[i].size());
                ++i;
            }
            tx.Commit();
            auto end_time = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
            std::cout << "delete: " << duration.count() << " microseconds" << std::endl;
        }
    }
};

int main() {
    Benchmark benchmark;
    benchmark.Run();
}