#include <filesystem>
#include <iostream>

#define BENCHMARK_STATIC_DEFINE
#include <benchmark/benchmark.h>

#include <lmdb.h>

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

#define E(expr) CHECK((rc = (expr)) == MDB_SUCCESS, #expr)
#define RES(err, expr) ((rc = expr) == (err) || (CHECK(!rc, #expr), 0))
#define CHECK(test, msg) ((test) ? (void)0 : ((void)fprintf(stderr, \
	"%s:%d: %s: %s\n", __FILE__, __LINE__, msg, mdb_strerror(rc)), abort()))

class Benchmark {
public:
    MDB_env* env_;
    int seed_{ 0 };
    int count_{ 1000000 };

    void Run() {


        srand(seed_);

        std::string path = "Z:/lmdb_benchmark";
        std::filesystem::remove_all(path);
        std::filesystem::create_directory(path);

        int rc;
        E(mdb_env_create(&env_));
        E(mdb_env_set_maxreaders(env_, 1));
        E(mdb_env_set_mapsize(env_, 10485760 * 8 * 4));
        E(mdb_env_open(env_, path.c_str(), MDB_FIXEDMAP /*|MDB_NOSYNC*/, 0664));

        std::vector<std::string> key(count_);
        std::vector<std::string> value(count_);
        for (auto i = 0; i < count_; i++) {
            key[i] = RandomString(16, 16);
            value[i] = RandomString(100, 100);
        }


        {
            auto start_time = std::chrono::high_resolution_clock::now();

            MDB_dbi dbi;
            MDB_val mdb_key, mdb_data;
            MDB_txn* txn;
            MDB_stat mst;

            E(mdb_txn_begin(env_, NULL, 0, &txn));
            E(mdb_dbi_open(txn, NULL, 0, &dbi));


            for (int i = 0; i < count_; ++i) {
                mdb_key.mv_size = key[i].size();
                mdb_key.mv_data = const_cast<void*>(reinterpret_cast<const void*>(key[i].data()));
                mdb_data.mv_size = value[i].size();
                mdb_data.mv_data = const_cast<void*>(reinterpret_cast<const void*>(value[i].data()));

                if (RES(MDB_KEYEXIST, mdb_put(txn, dbi, &mdb_key, &mdb_data, MDB_NOOVERWRITE))) {
                    //printf("error");
                }
                ++i;
            }
            E(mdb_txn_commit(txn));
            E(mdb_env_stat(env_, &mst));

            auto end_time = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
            std::cout << "put: " << duration.count() << " microseconds" << std::endl;
        }

        {
            auto start_time = std::chrono::high_resolution_clock::now();


            MDB_dbi dbi;
            MDB_val mdb_key, mdb_data;
            MDB_txn* txn;
            MDB_stat mst;
            E(mdb_txn_begin(env_, NULL, 0, &txn));
            E(mdb_dbi_open(txn, NULL, 0, &dbi));

            for (int i = 0; i < count_; ++i) {
                mdb_key.mv_size = key[i].size();
                mdb_key.mv_data = const_cast<void*>(reinterpret_cast<const void*>(key[i].data()));
                mdb_data.mv_size = value[i].size();
                mdb_data.mv_data = const_cast<void*>(reinterpret_cast<const void*>(value[i].data()));

                if (mdb_get(txn, dbi, &mdb_key, &mdb_data) || std::string_view{ reinterpret_cast<char*>(mdb_data.mv_data), mdb_data.mv_size } != value[i]) {
                    printf("%d error\n", i);
                }
                ++i;
            }

            E(mdb_txn_commit(txn));
            E(mdb_env_stat(env_, &mst));

            auto end_time = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
            std::cout << "get: " << duration.count() << " microseconds" << std::endl;

        }

        {
            auto start_time = std::chrono::high_resolution_clock::now();

            MDB_dbi dbi;
            MDB_val mdb_key, mdb_data;
            MDB_txn* txn;
            MDB_stat mst;
            E(mdb_txn_begin(env_, NULL, 0, &txn));
            E(mdb_dbi_open(txn, NULL, 0, &dbi));

            for (int i = 0; i < count_; ++i) {
                mdb_key.mv_size = key[i].size();
                mdb_key.mv_data = const_cast<void*>(reinterpret_cast<const void*>(key[i].data()));
                mdb_data.mv_size = value[i].size();
                mdb_data.mv_data = const_cast<void*>(reinterpret_cast<const void*>(value[i].data()));

                if (mdb_del(txn, dbi, &mdb_key, &mdb_data)) {
                    //printf("%d error\n", i);
                }
                ++i;
            }
            
            E(mdb_txn_commit(txn));
            E(mdb_env_stat(env_, &mst));

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