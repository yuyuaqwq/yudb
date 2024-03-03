基于B+树索引的单机键值存储引擎

# Features

- 提供键值映射关系的存储
- 键值是任意字节数组，按键排序
- 对数时间的查找/插入/删除
- 支持可序列化事务，符合ACID
- 读事务与写事务可以并发
- 支持Bucket的嵌套
- 可配置的页面尺寸、缓存大小、比较器

# Limitations

- 不直接支持跨进程访问
- 键的长度最长是一页的尺寸
- 不能同时开启多个写事务

# Gotchas

- 长事务可能会使日志文件无法及时清理
- 过多的并发读事务可能会使缓存池分配失败
