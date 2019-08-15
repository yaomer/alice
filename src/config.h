#ifndef _ALICE_SRC_CONFIG_H
#define _ALICE_SRC_CONFIG_H

#include <vector>
#include <string>
#include <unordered_map>

#include "sentinel_master.h"

namespace Alice {

void readServerConf();
void readSentinelConf();

class SaveParam {
public:
    SaveParam(time_t seconds, int changes)
        : _seconds(seconds),
        _changes(changes)
    {
    }
    time_t seconds() const { return _seconds; }
    int changes() const { return _changes; }
private:
    time_t _seconds;
    int _changes;
};

#define AOF_ALWAYS 1
#define AOF_EVERYSEC 2
#define AOF_NO 3

struct ServerConf {
    // 服务器监听的端口
    int port = 1296;
    // 服务器的IP
    const char *addr = "127.0.0.1";
    // 保存rdb持久化触发的条件
    std::vector<SaveParam> save_params;
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
};

using MasterMap = std::unordered_map<std::string, SentinelMaster>;

struct SentinelConf {
    int port = 12960;
    const char *addr = "127.0.0.1";
    MasterMap masters;
};

extern struct ServerConf g_server_conf;
extern struct SentinelConf g_sentinel_conf;

}

#endif // _ALICE_SRC_CONFIG_H
