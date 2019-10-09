#ifndef _ALICE_SRC_CONFIG_H
#define _ALICE_SRC_CONFIG_H

#include <vector>
#include <string>
#include <unordered_map>
#include <tuple>

#include "sentinel_instance.h"

namespace Alice {

void readServerConf(const char *server_conf_file);
void readSentinelConf(const char *sentinel_conf_file);

#define AOF_ALWAYS 1
#define AOF_EVERYSEC 2
#define AOF_NO 3

#define EVICT_ALLKEYS_LRU 1
#define EVICT_VOLATILE_LRU 2
#define EVICT_ALLKEYS_RANDOM 3
#define EVICT_VOLATILE_RANDOM 4
#define EVICT_VOLATILE_TTL 5
#define EVICT_NO 6

struct ServerConf {
    // 服务器监听的端口
    int port = 1296;
    // 服务器的IP
    std::string addr = "127.0.0.1";
    // 创建的数据库数目
    int databases = 16;
    // 保存rdb持久化触发的条件
    std::vector<std::tuple<time_t, int>> save_params;
    // 是否开启aof持久化
    bool enable_appendonly = false;
    // aof持久化的模式
    int aof_mode = AOF_EVERYSEC;
    std::string rdb_file = "dump.rdb";
    std::string appendonly_file = "appendonly.aof";
    // master-slave连接超时时间
    int repl_timeout = 60 * 1000;
    // 从服务器向主服务器发送PING的周期
    int repl_ping_period = 10 * 1000;
    // 复制积压缓冲区的大小
    size_t repl_backlog_size = 1024 * 1024;
    // 每次定期删除过期键时检查的数据库个数
    int expire_check_dbnums = 16;
    // 每个数据库检查的键数
    int expire_check_keys = 20;
    // 服务器可使用的最大内存
    size_t maxmemory = 0;
    // 内存淘汰策略
    int maxmemory_policy = EVICT_NO;
    // 内存淘汰时的随机取样精度
    int maxmemory_samples = 5;
    int slowlog_log_slower_than = 10000;
    int slowlog_max_len = 128;
};

struct SentinelConf {
    int port = 12960;
    std::string addr = "127.0.0.1";
    SentinelInstance::SentinelInstanceMap masters;
};

extern struct ServerConf g_server_conf;
extern struct SentinelConf g_sentinel_conf;

}

#endif // _ALICE_SRC_CONFIG_H
