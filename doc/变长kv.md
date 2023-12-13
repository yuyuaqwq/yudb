1.内联数据

2.overflow数据，在overflow页面中分配

​	不超过一页的数据都由overflow管理

​	默认没有overflow页面，需要才开辟

​	而多个overflow页面号是记录在某个overflow页面中的，也就是overflow数组，也被overflow的free管理

​	overflow数组同时也记录overflow所有页面的最大空闲尺寸

3.独立页面

​	一页或多页的数据		

​	在overflow中分配其描述信息(起始页号，size等)

​	独立页面在超过指定长度时，可以针对wal的写放大进行优化，即采用直接写到指定页面，wal不记录原始数据解决