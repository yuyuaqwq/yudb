基于B+树索引的嵌入式键值数据库

# 特征

- 提供键值映射关系的存储
- 键值是任意字节数组，按键排序
- 对数时间的查找/插入/删除
- 支持事务，遵循ACID特性
- 实现MVCC以支持读写事务并发进行
- 支持Bucket的嵌套
- 可配置的页面尺寸、缓存大小、比较器

# 局限性

- 不直接支持多进程访问
- 键的长度最长是一页的尺寸
- 不能同时开启多个写事务

# 陷阱

- 过长的写事务可能会使日志文件无法及时清理
- 过多的并发读事务可能会导致从缓存池分配页面时失败
- 更新后迭代器会失效(Put、Delete、Update)
- Checkpoint是同步执行的，会导致周期性的性能下降
