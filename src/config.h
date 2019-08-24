#ifndef _ALICE_SRC_CONFIG_H
#define _ALICE_SRC_CONFIG_H

#include <vector>
#include <string>
#include <unordered_map>
#include <tuple>

#include "sentinel_instance.h"

namespace Alice {

void readServerConf();
void readSentinelConf();

#define AOF_ALWAYS 1
#define AOF_EVERYSEC 2
#define AOF_NO 3

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
    // master-slave连接超时时间
    int repl_timeout = 60 * 1000;
    // 从服务器向主服务器发送PING的周期
    int repl_ping_preiod = 10 * 1000;
    // 复制积压缓冲区的大小
    size_t repl_backlog_size = 1024 * 1024;
    // 每次定期删除过期键时检查的数据库个数
    int expire_check_dbnums = 16;
    // 每个数据库检查的键数
    int expire_check_keys = 20;
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
