#ifndef _ALICE_SRC_SENTINEL_H
#define _ALICE_SRC_SENTINEL_H

#include <angel/server.h>
#include <angel/client.h>
#include <angel/util.h>

#include <string>
#include <memory>
#include <unordered_map>

#include "db_base.h"
#include "util.h"

namespace alice {

class Sentinel;

struct SentinelInstance {
    enum Flag {
        MASTER = 0x01, // 主服务器实例
        SLAVE = 0x02, // 从服务器实例
        SENTINEL = 0x04, // sentinel实例
        // 如果单个sentinel认为一个主服务器下线，则该主服务器被标记为S_DOWN
        // 如果多数sentinel认为一个主服务器下线，则该主服务器被标记为O_DOWN
        // 而只要从服务器或sentinel被标记为S_DOWN，就会被移除，
        // 并不需要和其他sentinel进行协商
        S_DOWN = 0x08,
        O_DOWN = 0x10,
        FOLLOWER = 0x20,
        HAVE_LEADER = 0x40,
        DELETE = 0x80,
    };
    SentinelInstance()
        : flags(0),
        run_id(generate_run_id()),
        offset(0),
        config_epoch(0),
        down_after_period(0),
        votes(0),
        quorum(0),
        leader_epoch(0),
        failover_epoch(0),
        elect_timeout_timer_id(0),
        last_heartbeat_time(angel::util::get_cur_time_ms())
    {
    }
    using SentinelInstanceMap = std::unordered_map<std::string,
          std::unique_ptr<SentinelInstance>>;
    void creat_cmd_connection();
    void creat_sub_connection();
    void close_connection_handler(const angel::connection_ptr& conn);
    void pub_message(const angel::connection_ptr& conn);
    void recv_reply_from_server(const angel::connection_ptr& conn, angel::buffer& buf);
    void parse_info_reply_from_master(const char *s, const char *es);
    void parse_info_reply_from_slave(const char *s, const char *es);
    void update_slaves(const char *s, const char *es);
    void parse_reply_from_sentinel(const char *s, const char *es);
    void ask_for_sentinels(const char *runid);
    void ask_master_state_for_others();
    void start_failover();
    void set_elect_timeout_timer();
    void elect_timeout_handler();
    void cancel_elect_timeout_timer();
    void notice_leader_to_others();
    void select_new_master(); // call by master
    void stop_replicate_master(); // call by slave
    void replicate_master(angel::inet_addr& masterAddr); // call by slave
    void convert_slave_to_master(SentinelInstance *slave); // call by master

    int flags;
    std::string master; // for sentinel or slave
    std::string run_id;
    angel::inet_addr inet_addr;
    // one command connection, one pubsub connection
    std::unique_ptr<angel::client> client[2];
    SentinelInstanceMap slaves; // for master
    SentinelInstanceMap sentinels; // for master
    std::string name;
    size_t offset;
    uint64_t config_epoch;
    int64_t down_after_period;
    unsigned votes; // for master
    unsigned quorum; // for master
    std::string leader; // for master
    uint64_t leader_epoch; // for master
    uint64_t failover_epoch; // for master
    size_t elect_timeout_timer_id; // for master
    int64_t last_heartbeat_time;
    Sentinel *sentinel;
};

class Sentinel {
public:
    explicit Sentinel(angel::evloop *loop, angel::inet_addr listen_addr);
    const char *get_run_id() { return run_id.c_str(); }
    SentinelInstance::SentinelInstanceMap& get_masters() { return masters; }
    void connection_handler(const angel::connection_ptr& conn)
    {
        // 只使用context_t.[argv, buf]
        conn->set_context(context_t());
    }
    void message_handler(const angel::connection_ptr& conn, angel::buffer& buf)
    {
        auto& con = std::any_cast<context_t&>(conn->get_context());
        while (true) {
            ssize_t n = parse_request(con.argv, buf);
            if (n < 0) {
                log_error("conn %d protocol error: %s", conn->id(), buf.c_str());
                conn->close();
                return;
            }
            if (n == 0) break;
            executor(con);
            buf.retrieve(n);
        }
        conn->send(con.buf);
        con.buf.clear();
    }
    void executor(context_t& con);
    void send_ping_to_servers();
    void send_info_to_servers();
    void send_pub_message_to_servers();
    void recv_pub_message_from_server(const angel::connection_ptr& conn, angel::buffer& buf);
    void update_sentinels(const char *s, const char *es);
    void sub_server(const angel::connection_ptr& conn);
    SentinelInstance *get_master_by_addr(const std::string& ip, int port);
    void sentinel_cron();
    void info(context_t& con);
    void sentinel(context_t& con);
    void ping(context_t& con);
    void start();
    typedef std::function<void(const std::unique_ptr<SentinelInstance>&)> functor;
    void for_each_instance(functor&& func, bool have_sentinel);

private:
    angel::evloop *loop;
    angel::server server;
    SentinelInstance::SentinelInstanceMap masters;
    std::string run_id;
    CommandTable cmdtable;
    // 相当于一个逻辑时间计数器，保证每次选举只会选出一个领头sentinel
    uint64_t current_epoch;
    friend SentinelInstance;
};
}

#endif
