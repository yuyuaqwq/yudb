data:
len     1~4byte，高1位为0表示1(0~127)，高1位为1表示4(0~2147483647)
data


首页面：
head:
count 2byte     // 当前Element计数
overflow_pgid       // 溢出时连接下一个溢出页面

index_block_info:
type 1byte         // 元信息类型；embed(0)还是block(1)还是each(2，具体长度存放到另一端的3位)
key_info 5byte     // k的元信息；如果是key的offset，那么高2字节(高少1位，左移1位，即2字节对齐)是block_offset，低4字节是pgid；过长的数据就需要访问单独的页，低4字节是pgid；数据<=5字节key则会直接放到这里
left_child_pgid 4byte       // 左孩子页面id


leaf_block_info:
type 1byte
key_info 5byte
type 1byte
value_info 5byte


block_info每次从free_block中从前向后分配，kv从后向前分配，中间是free_block
如果在申请block_info/k/v失败时，就扩展一个溢出页面，将数据迁移到溢出页面，首页面仅存放block_info

| head | block_info -> | free_block | <- k/v_block |


简化实现：
直接将所有长度超过5的数据会放到溢出数据页面。


溢出数据页面：
free_block:
next 2byte      // 下一空闲块偏移
len 2byte       // 当前空闲块长度


head:
free_next 2byte          // 下一个空闲块的偏移


同时实现一个overflow_head页面，因为一个Entry最大的block数就是最大的k+v数量，因此一个overflow_head_page足够描述所有的overflow_page
8字节一个字段，| pgid | max_len |，描述这个溢出页面剩余可分配的最大连续空间


独立数据页面：
| len | data | ...
分配独立数据页面的原因是key/value过长，单个溢出数据页面装不下，可以是多个页面(连续的)



溢出数据页面和独立数据页面可能还需要头部位置来存储引用计数