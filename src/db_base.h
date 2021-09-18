#ifndef _ALICE_SRC_DB_BASE_H
#define _ALICE_SRC_DB_BASE_H

#include <string>
#include <vector>
#include <functional>
#include <limits.h>

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
    enum Flags {
        // 从服务器中设置该标志的连接表示与主服务器相连
        CONNECT_WITH_MASTER = 0x02, // for slave
        // 客户端正在执行事务
        EXEC_MULTI = 0x04,
        // 事务的安全性被破坏
        EXEC_MULTI_ERR = 0x08,
        // 事务中有写操作
        EXEC_MULTI_WRITE = 0x10,
        // 客户端处于阻塞状态
        CON_BLOCK = 0x20,
    };
    enum ReplState {
        // 从服务器向主服务器发送了PING，正在等待接收PONG
        SYNC_PING = 1, // for slave
        // 从服务器向主服务器发送了一些配置信息，等待主服务器确认
        SYNC_CONF = 2, // for slave
        // 从服务器处于等待接收主服务器的同步信息的状态
        SYNC_WAIT = 3, // for slave
        // 将要进行完全重同步
        SYNC_FULL = 4, // for slave
        // 主服务器向设置SYNC_SNAPSHOT标志的连接发送快照
        SYNC_SNAPSHOT = 5, // for master
        // 主服务器向设置SYNC_COMMAND标志的连接传播同步命令
        SYNC_COMMAND = 6, // for master
    };
    context_t() {  }
    context_t(angel::connection *conn, void *priv) : conn(conn), priv(priv) {  }
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
    std::string& append(std::string&& s)
    {
        return buf.append(std::move(s));
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
    void append_reply_string(std::string&& s)
    {
        append("$");
        append(i2s(s.size()));
        append("\r\n");
        append(std::move(s));
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
    void append_error(const std::string& s)
    {
        append("-ERR ");
        append(s);
        append("\r\n");
    }
    void reserve_multi_head()
    {
        buf_resize = buf.size();
        // *(1) count(10) \r\n(2)
        buf.resize(buf_resize + 13);
    }
    template <typename T>
    void set_multi_head(T count)
    {
        std::string head = "*";
        std::string cs = i2s(count);
        int spaces = 10 - cs.size();
        while (spaces-- > 0)
            head.push_back(' ');
        head.append(cs);
        head.append("\r\n");
        assert(head.size() == 13);
        std::copy(head.begin(), head.end(), buf.data()+buf_resize);
    }

    int flags = 0;
    int perms = IS_READ | IS_WRITE;
    int repl_state = 0;
    angel::connection *conn = nullptr;
    argv_t argv; // 请求参数表 argv[0]为命令名
    std::string buf; // 回复缓冲区
    size_t buf_resize = 0;
    std::vector<argv_t> transaction_list; // 事务队列
    argv_t watch_keys; // 客户监视的键
    angel::inet_addr slave_addr; // 从服务器的地址
    argv_t blocking_keys;
    time_t block_start_time = 0;
    time_t blocked_time = 0;
    int block_db_num = 0;
    std::string des; // for brpoplpush
    std::string last_cmd;
    void *priv = nullptr;
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
    virtual void watch(context_t&) = 0;
    virtual void unwatch(context_t&) = 0;
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
    const char *min_or_max_err = "-ERR min or max is not a valid float\r\n";
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
    do { (con).append(str); return; } while (0)

#define retval(con, str, val) \
    do { (con).append(str); return (val); } while (0)

}

#endif
