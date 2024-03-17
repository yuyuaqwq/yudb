#include <filesystem>
#include <iostream>

#define BENCHMARK_STATIC_DEFINE
#include <benchmark/benchmark.h>

#include <leveldb/db.h>
#include <leveldb/write_batch.h>
#include "util/test_util.h"

class Benchmark {
public:
    leveldb::DB* db_;
    int seed_{ 0 };
    int count_{ 1000000 };

    void Run() {


        srand(seed_);

        std::string path = "Z:/leveldb_benchmark";
        std::filesystem::remove_all(path);
        leveldb::Options options;
        options.create_if_missing = true;
        leveldb::Status s = leveldb::DB::Open(options, path, &db_);
        if (!s.ok()) {
            printf("??");
        }
        std::vector<std::string> key(count_);
        std::vector<std::string> value(count_);
        for (auto i = 0; i < count_; i++) {
            key[i] = yudb::RandomByteArray(16, 16);
            value[i] = yudb::RandomByteArray(100, 100);
        }


        {
            auto start_time = std::chrono::high_resolution_clock::now();
            //leveldb::WriteBatch batch;
            for (int i = 0; i < count_; ++i) {
                //batch.Put(key[i], value[i]);
                db_->Put({}, key[i], value[i]);
                ++i;
            }
            //db_->Write({}, &batch);
            auto end_time = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
            std::cout << "put: " << duration.count() << " microseconds" << std::endl;
        }

        {
            auto start_time = std::chrono::high_resolution_clock::now();
            for (int i = 0; i < count_; ++i) {
                std::string value_;
                auto s = db_->Get({}, key[i], &value_);
                if (!s.ok() || value_ != value[i]) {
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
            leveldb::WriteBatch batch;
            for (int i = 0; i < count_; ++i) {
                batch.Delete(key[i]);
                ++i;
            }
            db_->Write({}, &batch);
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