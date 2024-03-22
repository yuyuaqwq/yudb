## yudb

yudb是一个使用B-Tree作为索引，基于MMap的嵌入式键值数据库，使用C++开发，旨在改进LMDB的写事务的性能问题，并简化使用。

## 特征

- key-value是任意字节数组，按key排序
- 对数时间的查找/插入/删除
- 支持事务，遵循ACID特性
- 基于MVCC的读写事务并发
- 支持Bucket的嵌套
- 使用mmap，但自动扩展大小
- 可配置的Comparator

## 限制

- key最长存储为一页
- 同一时间仅允许一个写事务
- 支持多进程打开数据库，但使用读写锁
  - 未来可能得到改进

## 陷阱

- 过长的写事务可能会使日志文件无法及时清理
- 更新后迭代器会失效(Put、Delete、Update)
- Checkpoint是同步执行的，会导致周期性的性能下降
  - 未来可能得到改进

## 表现
### 环境
```
System: Windows11 x64
CPU: 11th Gen Intel(R) Core(TM) i5-11260H @ 2.60GHz   2.61 GHz
RAM: 32GB
Compiler: MSVC 1939, Release, /O2
```

### yudb
```
yudb:     version 0.0.1
Keys:       16 bytes each
Values:     100 bytes each
Entries:    1000000
------------------------------------------------
fillseq      :       4.235 micros/op;   26.1 MB/s
readseq      :       0.110 micros/op; 1006.6 MB/s
fillsync     :     440.804 micros/op;    0.3 MB/s (10000ops)
fillseqbatch :       1.427 micros/op;   77.5 MB/s
fillrandom   :       4.937 micros/op;   22.4 MB/s
readrandom   :       1.201 micros/op;   92.1 MB/s
fillrandbatch :       2.964 micros/op;   37.3 MB/s
readrandbatch :       1.160 micros/op;   95.4 MB/s
overwrite    :       4.347 micros/op;   25.5 MB/s
overwritebatch :       2.354 micros/op;   47.0 MB/s
fillrand100K :     729.656 micros/op;  130.7 MB/s (1000ops)
fillseq100K  :     508.038 micros/op;  187.7 MB/s (1000ops)
```

### lmdb
```
lmdb:     version LMDB 0.9.31: (July 10, 2023)
Keys:       16 bytes each
Values:     100 bytes each
Entries:    1000000
------------------------------------------------
fillseq      :     617.089 micros/op;    0.2 MB/s
readseq      :       0.053 micros/op; 2070.5 MB/s
fillsync     :     684.248 micros/op;    0.2 MB/s (10000ops)
fillseqbatch :       2.753 micros/op;   40.2 MB/s
fillrandom   :     647.463 micros/op;    0.2 MB/s
readrandom   :       0.833 micros/op;  132.8 MB/s
fillrandbatch :      42.630 micros/op;    2.6 MB/s
readrandbatch :       0.788 micros/op;  140.3 MB/s
overwrite    :     798.395 micros/op;    0.1 MB/s
overwritebatch :      57.114 micros/op;    1.9 MB/s
fillrand100K :     908.148 micros/op;  105.0 MB/s (1000ops)
fillseq100K  :     796.055 micros/op;  119.8 MB/s (1000ops)
```

## 构建

### Linux

```
mkdir build
cd build
cmake ..
make
```

### Windows

```
mkdir build
cd build
cmake ..
```

