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
+ 压测可使用[redis](https://github.com/antirez/redis)自带的benchmark(有一点需要注意的是：必须指定-t cmd)

#### 与redis的测试对比
+ 单线程
![](https://github.com/yaomer/pictures/blob/master/alice_bench.png?raw=true)

+ 多线程
![](https://github.com/yaomer/pictures/blob/master/alice_bench1.png?raw=true)

+ 功能性测试
![](https://github.com/yaomer/pictures/blob/master/redis-bench-all.png?raw=true)
![](https://github.com/yaomer/pictures/blob/master/alice-bench-all.png?raw=true)

### 和Alice一起玩
![](https://github.com/yaomer/pictures/blob/master/alice_play.png?raw=true)

### 用户API
我提供了一个简单的同步API，使用方法如下：
```cpp
#include <iostream>                                                                                                        
#include "alice.h"
 
int main()
{
    Alice::AliceContext alice;
    alice.connect("127.0.0.1", 1296);
    if (alice.err()) {
        std::cout << alice.errStr() << "\n";
        return 1;
    }   
    alice.executor("set key hello");
    std::cout << alice.reply()[0] << "\n";
    alice.executor("get key");
    std::cout << alice.reply()[0] << "\n";
    alice.executor("del name");
    for (int i = 0; i < 10; i++)
        alice.executor("lpush name #%d#", i); 
    alice.executor("lrange name 0 -1");
    std::cout << "name(list):\n";
    for (auto& it : alice.reply())
        std::cout << it << "\n";
    alice.close();
}
```
