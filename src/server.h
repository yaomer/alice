#ifndef _ALICE_SRC_SERVER_H
#define _ALICE_SRC_SERVER_H

#include <angel/server.h>
#include <angel/client.h>

#include <iostream>
#include <memory>

#include "config.h"
#include "db_base.h"
#include "ring_buffer.h"
#include "slowlog.h"
#include "util.h"

#include "mmdb/mmdb.h"
#include "ssdb/ssdb.h"

namespace alice {

class dbserver {
public:
    enum FLAG {
        MASTER = 0x01,
        SLAVE = 0x02,
        // 有slave正在等待生成快照
        PSYNC = 0x04,
        // 有psync请求被延迟
        PSYNC_DELAY = 0x08,
        DISCONNECT_WITH_MASTER = 0x10,
    };
    dbserver(angel::evloop *loop, angel::inet_addr listen_addr)
        : loop(loop),
        server(loop, listen_addr),
        copy_backlog_buffer(server_conf.repl_backlog_size)
    {
        if (server_conf.engine == ENGINE_MMDB)
            db.reset(new mmdb::engine());
        else if (server_conf.engine == ENGINE_SSDB)
            db.reset(new ssdb::engine());
        server.set_connection_handler([this](const angel::connection_ptr& conn){
                this->connection_handler(conn); });
        server.set_message_handler([this](const angel::connection_ptr& conn, angel::buffer& buf){
                this->message_handler(conn, buf);
                });
        server.set_close_handler([this](const angel::connection_ptr& conn){
                this->close_handler(conn);
                });
        server.start_task_threads(1);
        run_id = generate_run_id();
    }
    void connection_handler(const angel::connection_ptr& conn)
    {
        db->connection_handler(conn);
        if (flags & SLAVE) {
            auto& con = get_context(conn);
            con.perms &= ~IS_WRITE;
        }
    }
    void message_handler(const angel::connection_ptr& conn, angel::buffer& buf)
    {
        auto& con = get_context(conn);
        while (true) {
            ssize_t n = parse_request(con.argv, buf);
            if (n < 0) {
                log_error("conn %d protocol error: %s", conn->id(), buf.c_str());
                conn->close();
                return;
            }
            if (n == 0) break;
            executor(con, buf.peek(), n);
            buf.retrieve(n);
        }
        send_response(conn);
    }
    void close_handler(const angel::connection_ptr& conn)
    {
        auto it = slaves.find(conn->id());
        if (it != slaves.end()) slaves.erase(conn->id());
        db->close_handler(conn);
    }
    void slave_message_handler(const angel::connection_ptr& conn, angel::buffer& buf)
    {
        auto& con = get_context(conn);
        while (true) {
            while (true) {
                if (!buf.starts_with("+PONG\r\n")) break;
                update_heartbeat_time();
                buf.retrieve(7);
            }
            ssize_t n = parse_request(con.argv, buf);
            if (n < 0) {
                log_error("conn %d protocol error: %s", conn->id(), buf.c_str());
                conn->close();
                return;
            }
            if (n == 0) return;
            executor(con, buf.peek(), n);
            buf.retrieve(n);
            send_response(conn);
        }
    }
    void send_response(const angel::connection_ptr& conn)
    {
        auto& con = get_context(conn);
        // 从服务器不应向主服务器发送回复信息
        if (!(con.flags & context_t::CONNECT_WITH_MASTER))
            conn->send(con.buf);
        con.buf.clear();
    }
    command_t *find_command(const std::string& name)
    {
        auto it = cmdtable.find(name);
        if (it != cmdtable.end())
            return &it->second;
        else
            return nullptr;
    }
    angel::evloop *get_loop() { return loop; }
    angel::server& get_server() { return server; }
    const std::unique_ptr<db_base_t>& get_db() { return db; }
    const char *get_run_id() { return run_id.c_str(); }
    context_t& get_context(const angel::connection_ptr& conn)
    {
        return std::any_cast<context_t&>(conn->get_context());
    }
    void start();
    void server_cron();
    void executor(context_t& con, const char *query, size_t len);
    void do_write_command(const argv_t& argv, const char *query, size_t len);
    void append_write_command(const argv_t& argv, const char *query, size_t len);
    void sync_command_to_slaves(const char *query, size_t len);
    void append_copy_backlog_buffer(const char *query, size_t len);
    void append_partial_resync_data(context_t& con, size_t off);
    void fsync(int fd)
    {
        server.executor([fd]{ ::fsync(fd); ::close(fd); });
    }
    static void conv2resp_with_expire(std::string& buffer, const argv_t& argv,
                                      const char *query, size_t len);
    // master-slave replication
    void connect_master_server();
    void reset_connection_with_master();
    void recv_sync_from_master(const angel::connection_ptr& conn, angel::buffer& buf);
    void slave_sync_ping(const angel::connection_ptr& conn, angel::buffer& buf);
    void slave_sync_conf(const angel::connection_ptr& conn, angel::buffer& buf);
    void slave_sync_wait(const angel::connection_ptr& conn, angel::buffer& buf);
    void slave_close_handler(const angel::connection_ptr& conn);
    void send_ping_to_master(const angel::connection_ptr& conn);
    void send_info_to_master(const angel::connection_ptr& conn);
    void send_sync_command_to_master(const angel::connection_ptr& conn);
    void set_heartbeat_timer(const angel::connection_ptr& conn);
    void send_ack_to_master(const angel::connection_ptr& conn);
    void recv_snapshot_from_master(const angel::connection_ptr& conn, angel::buffer& buf);
    void send_snapshot_to_slaves();
    void reset_slave_message_handler();
    void update_heartbeat_time();
    // replication command
    void slaveof(context_t& con);
    void psync(context_t& con);
    void replconf(context_t& con);
    void ping(context_t& con);
    // transaction command
    void multi(context_t& con);
    void exec(context_t& con);
    void discard(context_t& con);
    void watch(context_t& con);
    void unwatch(context_t& con);
    // pub-sub command
    void publish(context_t& con);
    void subscribe(context_t& con);
    // config command
    void config(context_t& con);
    // info command
    void info(context_t& con);
private:
    size_t pub_message(const std::string& message, const std::string& channel);
    void sub_channel(const std::string& channel, size_t id);

    angel::evloop *loop;
    angel::server server;
    std::unique_ptr<db_base_t> db;
    int flags = MASTER;
    std::string run_id;
    size_t sync_file_size = 0;
    char sync_tmp_file[16];
    int sync_fd = -1;
    size_t heartbeat_timer_id = 0;
    time_t last_recv_heartbeat_time = 0;
    size_t repl_timeout_timer_id = 0;
    // for master
    // <conn_id, slave_offset>
    std::unordered_map<size_t, size_t> slaves;
    size_t master_offset = 0;
    // for slave
    angel::inet_addr master_addr;
    std::unique_ptr<angel::client> master_cli;
    size_t slave_offset = 0;
    std::string master_run_id;
    std::string sync_buffer;
    ring_buffer copy_backlog_buffer;
    std::unordered_map<std::string, std::vector<size_t>> pubsub_channels;
    slowlog_t slowlog;
    std::unordered_map<std::string, command_t> cmdtable;
};

extern dbserver *__server;
}

#endif
