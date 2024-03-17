#include <filesystem>

#define BENCHMARK_STATIC_DEFINE
#include <benchmark/benchmark.h>

#include "yudb/version.h"
#include "yudb/db.h"
#include "util/test_util.h"

namespace yudb {

static const char* FLAGS_benchmarks =
    "fillseq,"
    "fillseqsync,"
    "fillseqbatch,"
    "fillrandom,"
    "fillrandsync,"
    "fillrandbatch,"
    "overwrite,"
    "overwritebatch,"
    "readrandom,"
    "readseq,"
    "fillrand100K,"
    "fillseq100K,"
    "readseq,"
    "readrand100K,";

class Benchmark {
private:
    std::unique_ptr<yudb::DB> db_;
    int seed_{ 0 };
    int count_{ 1000000 };
    std::chrono::steady_clock::time_point start_;
    const int kKeySize = 16;
    const int kValueSize = 100;

    std::vector<std::string> key_;
    std::vector<std::string> value_;

    void PrintEnvironment() {
        std::fprintf(stderr, "yudb:     version %s\n", YUDB_VERSION_STR);
    }

    void PrintHeader() {
        PrintEnvironment();
        std::fprintf(stdout, "Keys:       %d bytes each\n", kKeySize);
        std::fprintf(stdout, "Values:     %d bytes each\n", kValueSize);
        std::fprintf(stdout, "------------------------------------------------\n");
    }

    void Open() {
        yudb::Options options{
            .max_wal_size = 1024 * 1024 * 1024,
        };
        std::string path = "Z:/yudb_benchmark.ydb";
        std::filesystem::remove(path);
        std::filesystem::remove(path + "-shm");
        std::filesystem::remove(path + "-wal");
        db_ = yudb::DB::Open({}, path);
    }


public:
    enum Order { SEQUENTIAL, RANDOM };

    void Run() {
        Open();

        srand(seed_);
        key_.resize(count_);
        value_.resize(count_);
        for (auto i = 0; i < count_; i++) {
            key_[i] = RandomString(kKeySize, kKeySize);
            value_[i] = RandomString(kValueSize, kValueSize);
        }
        
        start_ = std::chrono::high_resolution_clock::now();
        Write(false, SEQUENTIAL, count_, 1);
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_);
        std::cout << "put: " << duration.count() << " microseconds" << std::endl;

        start_ = std::chrono::high_resolution_clock::now();
        Read(SEQUENTIAL, count_, 1);
        end_time = std::chrono::high_resolution_clock::now();
        duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_);
        std::cout << "get: " << duration.count() << " microseconds" << std::endl;

        //{
        //    auto start_time = std::chrono::high_resolution_clock::now();
        //    for (int i = 0; i < count_; ++i) {
        //        auto tx = db_->Update();
        //        auto bucket = tx.UserBucket();
        //        
        //        ++i;
        //        
        //    }
        //    auto end_time = std::chrono::high_resolution_clock::now();
        //    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
        //    std::cout << "put: " << duration.count() << " microseconds" << std::endl;
        //}

        //{
        //    auto start_time = std::chrono::high_resolution_clock::now();
        //    auto tx = db_->View();
        //    auto bucket = tx.UserBucket();
        //    for (int i = 0; i < count_; ++i) {
        //        auto iter = bucket.Get(key_[i].data(), key_[i].size());
        //        if (iter == bucket.end() || iter->value() != value_[i]) {
        //            printf("%d error\n", i);
        //        }
        //        ++i;
        //    }
        //    auto end_time = std::chrono::high_resolution_clock::now();
        //    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
        //    std::cout << "get: " << duration.count() << " microseconds" << std::endl;
        //}

        //{
        //    auto start_time = std::chrono::high_resolution_clock::now();
        //    auto tx = db_->Update();
        //    auto bucket = tx.UserBucket();
        //    for (int i = 0; i < count_; ++i) {
        //        bucket.Delete(key_[i].data(), key_[i].size());
        //        ++i;
        //    }
        //    //tx.Commit();
        //    auto end_time = std::chrono::high_resolution_clock::now();
        //    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
        //    std::cout << "delete: " << duration.count() << " microseconds" << std::endl;
        //}
    }

    void Write(bool write_sync, Order order, int num_entries, int entries_per_batch) {
        for (int i = 0; i < num_entries; i += entries_per_batch) {
            auto tx = db_->Update();
            auto bucket = tx.UserBucket();
            for (int j = 0; j < entries_per_batch; j++) {
                bucket.Put(key_[i].data(), key_[i].size(), value_[i].data(), value_[i].size());
            }
            tx.Commit();
        }
    }

    void Read(Order order, int num_entries, int entries_per_batch) {
        for (int i = 0; i < num_entries; i += entries_per_batch) {
            auto tx = db_->View();
            auto bucket = tx.UserBucket();
            for (int j = 0; j < entries_per_batch; j++) {
                auto iter = bucket.Get(key_[i].data(), key_[i].size());
                if (iter == bucket.end() || iter->value() != value_[i]) {
                    fprintf(stderr, " %d error\n", i);
                }
            }
        }
    }
};
} // namespace yudb

int main() {
    yudb::Benchmark benchmark;
    benchmark.Run();
}