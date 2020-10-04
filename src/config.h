#ifndef _ALICE_SRC_CONFIG_H
#define _ALICE_SRC_CONFIG_H

#include <vector>
#include <string>
#include <unordered_map>
#include <tuple>

namespace alice {

void read_server_conf(const std::string& filename);
void read_sentinel_conf(const std::string& filename);

#define ENGINE_MMDB 1
#define ENGINE_SSDB 2

#define AOF_ALWAYS 1
#define AOF_EVERYSEC 2
#define AOF_NO 3

#define EVICT_ALLKEYS_LRU 1
#define EVICT_VOLATILE_LRU 2
#define EVICT_ALLKEYS_RANDOM 3
#define EVICT_VOLATILE_RANDOM 4
#define EVICT_VOLATILE_TTL 5
#define EVICT_NO 6

struct server_conf_t {
    int port = 1296;
    std::string ip = "127.0.0.1";
    int engine = ENGINE_MMDB;
    // master-slave连接超时时间
    int repl_timeout = 60 * 1000;
    // 从服务器向主服务器发送PING的周期
    int repl_ping_period = 10 * 1000;
    // 复制积压缓冲区的大小
    int repl_backlog_size = 1024 * 1024;
    int slowlog_log_slower_than = 10000;
    int slowlog_max_len = 128;
    // 将要去复制的主服务器
    std::string master_ip;
    int master_port;
    // 要创建多少个数据库
    int mmdb_databases = 16;
    // 每次定期删除过期键时检查的数据库个数
    int mmdb_expire_check_dbnums = 16;
    // 每个数据库检查的键数
    int mmdb_expire_check_keys = 20;
    // 服务器可使用的最大内存
    int mmdb_maxmemory = 0;
    // 内存淘汰策略
    int mmdb_maxmemory_policy = EVICT_NO;
    // 内存淘汰时的随机取样精度
    int mmdb_maxmemory_samples = 5;
    std::vector<std::tuple<time_t, int>> mmdb_save_params;
    // 是否压缩rdb文件
    bool mmdb_rdb_compress = true;
    // len(value)大于多少时进行压缩
    int mmdb_rdb_compress_limit = 20;
    // rdb文件的存储位置
    std::string mmdb_rdb_file = "dump.rdb";
    // 是否开启aof持久化
    bool mmdb_enable_appendonly = false;
    // aof持久化的模式
    int mmdb_aof_mode = AOF_EVERYSEC;
    // aof文件的存储位置
    std::string mmdb_appendonly_file = "appendonly.aof";
    // ssdb-options
    std::string ssdb_leveldb_dbname = ".testdb";
    int ssdb_expire_check_keys = 20;
    bool ssdb_leveldb_create_if_missing = true;
    int ssdb_leveldb_write_buffer_size = 4 * 1024 * 1024;
    int ssdb_leveldb_max_open_files = 65535;
    int ssdb_leveldb_max_file_size = 2 * 1024 * 1024;
};

struct sentinel_instance_conf_t {
    std::string name;
    std::string ip;
    int port;
    int quorum;
    int down_after_period;
};

struct sentinel_conf_t {
    int port = 12960;
    std::string ip = "127.0.0.1";
    std::unordered_map<std::string, sentinel_instance_conf_t> insmap;
};

extern struct server_conf_t server_conf;
extern struct sentinel_conf_t sentinel_conf;

}

#endif // _ALICE_SRC_CONFIG_H
