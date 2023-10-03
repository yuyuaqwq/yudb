7种宽度的内存块，元信息存储头(亦或者是entry的头部？好像是不错的想法，相比全局能够减少随机io)，每次是一个页面，通过空闲链表链接

m+1记得修改

union free_block_entry {
    PageId pgid;
    uint16_t offset;
};


meta:
free_block_entry block_16_head
free_block_entry block_32_head
...



page:
| free_block_entry 或 已分配的数据 |  |  |
