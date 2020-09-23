#ifndef _ALICE_SRC_DB_BASE_H
#define _ALICE_SRC_DB_BASE_H

#include <string>
#include <vector>
#include <functional>

#include <angel/inet_addr.h>
#include <angel/connection.h>

#include "util.h"

namespace alice {

enum Perm {
    IS_READ = 01,
    IS_WRITE = 02,
};

enum ReturnCode {
    C_OK,   // 函数执行成功
    C_ERR,  // 函数执行出错
};

struct context_t {
    enum Flag{
        // 主服务器中设置该标志的连接表示与从服务器相连
        // SLAVE = 0x001, // for master
        CONNECT_WITH_SLAVE = 0x001, // for master
        // 从服务器中设置该标志的连接表示与主服务器相连
        // MASTER = 0x400, // for slave
        CONNECT_WITH_MASTER = 0x002, // for slave
        // SYNC_RECV_PING = 0x200,
        SYNC_PING = 0x004,
        // 主服务器向设置SYNC_RDB_FILE标志的连接发送rdb文件
        // 从服务器设置该标志表示该连接处于接收同步文件的状态
        // SYNC_RDB_FILE = 0x002, // for master and slave
        SYNC_SNAPSHOT = 0x008, // for master and slave
        // 主服务器向设置SYNC_COMMAND标志的连接传播同步命令
        // 从服务器设置该标志表示该连接处于接收同步命令的状态
        SYNC_COMMAND = 0x010, // for master and slave
        // 从服务器处于等待接收主服务器的同步信息的状态
        SYNC_WAIT = 0x020, // for slave
        // 将要进行完全重同步
        SYNC_FULL = 0x040, // for slave
        // 客户端正在执行事务
        EXEC_MULTI = 0x080,
        // 事务的安全性被破坏
        EXEC_MULTI_ERR = 0x100,
        // 事务中有写操作
        EXEC_MULTI_WRITE = 0x200,
        CON_BLOCK = 0x400,
    };
    context_t()
        : flags(0),
        perms(IS_READ | IS_WRITE),
        conn(nullptr),
        block_start_time(0),
        blocked_time(0),
        block_db_num(0),
        priv(nullptr)
    {
    }
    context_t(angel::connection *conn, void *priv)
        : flags(0),
        perms(IS_READ | IS_WRITE),
        conn(conn),
        block_start_time(0),
        blocked_time(0),
        block_db_num(0),
        priv(priv)
    {
    }
    bool iscmd(const std::string& name)
    {
        return isequal(0, name);
    }
    bool isequal(int i, const std::string& name)
    {
        return strcasecmp(argv[i].c_str(), name.c_str()) == 0;
    }
    std::string& append(const std::string& s)
    {
        return buf.append(s);
    }
    std::string& append(const char *s, size_t len)
    {
        return buf.append(s, len);
    }
    template <typename T>
    void append_reply_multi(T count)
    {
        append("*");
        append(i2s(count));
        append("\r\n");
    }
    void append_reply_string(const std::string& s)
    {
        append("$");
        append(i2s(s.size()));
        append("\r\n");
        append(s);
        append("\r\n");
    }
    template <typename T>
    void append_reply_number(T number)
    {
        append(":");
        append(i2s(number));
        append("\r\n");
    }
    void append_reply_double(double dval)
    {
        append_reply_string(d2s(dval));
    }

    int flags;
    int perms;
    angel::connection *conn;
    argv_t argv; // 请求参数表 argv[0]为命令名
    std::string buf; // 回复缓冲区
    std::vector<argv_t> transaction_list; // 事务队列
    argv_t watch_keys; // 客户监视的键
    angel::inet_addr slave_addr; // 从服务器的地址
    argv_t blocking_keys;
    time_t block_start_time;
    time_t blocked_time;
    int block_db_num;
    std::string des; // for brpoplpush
    std::string last_cmd;
    void *priv;
};

struct command_t {
    typedef std::function<void(context_t&)> command_callback_t;
    command_t(int arity, int perm, const command_callback_t cb)
        : arity(arity), perm(perm), command_cb(cb)
    {
    }
    int arity;
    int perm;
    command_callback_t command_cb;
};

using CommandTable = std::unordered_map<std::string, command_t>;

struct db_base_t {
    virtual ~db_base_t() {  }
    virtual void start() {  }
    virtual void exit() {  }
    virtual void server_cron() {  }
    virtual command_t *find_command(const std::string& name) = 0;
    virtual void connection_handler(const angel::connection_ptr&) {  }
    virtual void close_handler(const angel::connection_ptr&) {  }
    virtual void slave_connection_handler(const angel::connection_ptr&) {  }
    virtual void slave_close_handler(const angel::connection_ptr&) {  }
    virtual void do_after_exec_write_cmd(const argv_t& argv, const char *query, size_t len) {  }
    virtual void free_memory_if_needed() {  }
    virtual void creat_snapshot() = 0;
    virtual bool is_creating_snapshot() = 0;
    virtual bool is_created_snapshot() = 0;
    virtual std::string get_snapshot_name() = 0;
    virtual void load_snapshot() = 0;
};

struct shared_obj {
    const char *ok = "+OK\r\n";
    const char *nil = "$-1\r\n";
    const char *n0 = ":0\r\n";
    const char *n1 = ":1\r\n";
    const char *n_1 = ":-1\r\n";
    const char *n_2 = ":-2\r\n";
    const char *multi_empty = "*0\r\n";
    const char *type_err = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    const char *integer_err = "-ERR value is not an integer or out of range\r\n";
    const char *float_err = "-ERR value is not a valid float\r\n";
    const char *syntax_err = "-ERR syntax error\r\n";
    const char *no_such_key = "-ERR no such key\r\n";
    const char *subcommand_err = "-ERR Unknown subcommand or wrong argument\r\n";
    const char *argnumber_err = "-ERR wrong number of arguments\r\n";
    const char *timeout_err = "-ERR invalid expire timeout\r\n";
    const char *timeout_out_of_range = "-ERR timeout is out of range\r\n";
    const char *invalid_db_index = "-ERR invalid DB index\r\n";
    const char *db_index_out_of_range = "-ERR DB index is out of range\r\n";
    const char *index_out_of_range = "-ERR index is out of range\r\n";
    const char *unknown_option = "-ERR unknown option\r\n";
    const char *none_type = "+none\r\n";
    const char *string_type = "+string\r\n";
    const char *list_type = "+list\r\n";
    const char *hash_type = "+hash\r\n";
    const char *set_type = "+set\r\n";
    const char *zset_type = "+zset\r\n";
};

extern shared_obj shared;
extern int64_t lru_clock;

#define ret(con, str) \
    do { (con).append(str); return; } while(0)

}

#endif
