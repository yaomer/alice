# Alice

### 目前已实现的功能：
+ 支持字符串、列表、哈希、集合、有序集合
+ 过期键
+ rdb持久化
+ aof持久化
+ 主从复制
+ sentinel
+ proxy
+ 事务
+ 发布订阅
+ 排序
+ 慢查询日志
+ 内存淘汰

### 依赖
+ 网络模块使用[Angel](https://github.com/yaomer/Angel)
+ 客户端的命令提示使用[linenoise](https://github.com/antirez/linenoise)
+ 我们默认libangel.a和liblinenoise.a都安装到了/usr/local/lib目录

### 压测
+ 压测可使用[redis](https://github.com/antirez/redis)自带的benchmark
