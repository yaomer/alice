#include <fcntl.h>

#include "server.h"
#include "sentinel.h"
#include "util.h"

using namespace alice;

namespace alice {

    shared_obj shared;
    int64_t lru_clock = angel::util::get_cur_time_ms();
    dbserver *__server;
}

void dbserver::executor(context_t& con, const char *query, size_t len)
{
    size_t pos;
    time_t start, end;
    std::transform(con.argv[0].begin(), con.argv[0].end(), con.argv[0].begin(), ::toupper);
    auto c = db->find_command(con.argv[0]);
    if (c == nullptr) {
        c = find_command(con.argv[0]);
        if (c == nullptr) {
            con.append("-ERR unknown command `" + con.argv[0] + "`\r\n");
            goto err;
        }
    }
    if (!(con.perms & c->perm)) {
        con.append("-ERR permission denied\r\n");
        goto err;
    }
    if ((c->arity > 0 && con.argv.size() < c->arity) ||
        (c->arity < 0 && con.argv.size() != -c->arity)) {
        con.append("-ERR wrong number of arguments for '" + con.argv[0] + "'\r\n");
        goto err;
    }
    con.last_cmd = con.argv[0];
    if (con.flags & context_t::EXEC_MULTI) {
        if (con.argv[0].compare("MULTI") &&
            con.argv[0].compare("EXEC") &&
            con.argv[0].compare("WATCH") &&
            con.argv[0].compare("DISCARD")) {
            if (!(con.flags & context_t::EXEC_MULTI_WRITE) && (c->perm & IS_WRITE))
                con.flags |= context_t::EXEC_MULTI_WRITE;
            con.transaction_list.emplace_back(con.argv);
            con.append("+QUEUED\r\n");
            goto end;
        }
    }
    if (c->perm & IS_WRITE)
        db->free_memory_if_needed();
    pos = con.buf.size();
    start = angel::util::get_cur_time_us();
    c->command_cb(con);
    end = angel::util::get_cur_time_us();
    slowlog.add_slowlog_if_needed(con.argv, start, end);
    if ((c->perm & IS_WRITE) && con.buf[pos] != '-') {
        do_write_command(con.argv, query, len);
    }
    goto end;
err:
    if (con.flags & context_t::EXEC_MULTI) {
        con.flags &= ~context_t::EXEC_MULTI;
        con.flags |= context_t::EXEC_MULTI_ERR;
    }
end:
    con.argv.clear();
}

void dbserver::server_cron()
{
    lru_clock = angel::util::get_cur_time_ms();

    if (db->is_created_snapshot()) {
        if (flags & PSYNC) {
            flags &= ~PSYNC;
            send_snapshot_to_slaves();
        }
        if (flags & PSYNC_DELAY) {
            flags &= ~PSYNC_DELAY;
            flags |= PSYNC;
            db->creat_snapshot();
        }
    }

    if (flags & DISCONNECT_WITH_MASTER) {
        flags &= ~DISCONNECT_WITH_MASTER;
        connect_master_server();
    }

    db->server_cron();
}

// [query, len]是RESP形式的请求字符串，如果query为真，则使用[query, len]，这样可以避免许多低效的拷贝；
// 如果query为假，则使用argv，然后将其转换为RESP形式的字符串，不过这种情况很少见
void dbserver::do_write_command(const argv_t& argv, const char *query, size_t len)
{
    append_write_command(argv, query, len);
    if (!slaves.empty()) {
        if (!query) {
            std::string buffer;
            conv2resp(buffer, argv);
            sync_command_to_slaves(buffer.data(), buffer.size());
        } else
            sync_command_to_slaves(query, len);
    }
    if (flags & SLAVE) slave_offset += len;
}

void dbserver::append_write_command(const argv_t& argv, const char *query, size_t len)
{
    db->do_after_exec_write_cmd(argv, query, len);
    if (flags & PSYNC) {
        if (query) {
            sync_buffer.append(query, len);
        } else {
            conv2resp(sync_buffer, argv);
        }
    }
    if (!slaves.empty()) {
        if (!query) {
            std::string buffer;
            conv2resp(buffer, argv);
            append_copy_backlog_buffer(buffer.data(), buffer.size());
        } else
            append_copy_backlog_buffer(query, len);
    }
}

void dbserver::sync_command_to_slaves(const char *query, size_t len)
{
    for (auto& it : slaves) {
        auto conn = server.get_connection(it.first);
        if (!conn) continue;
        auto& con = get_context(conn);
        if (con.flags & context_t::SYNC_COMMAND) {
            std::string s(query, len);
            log_warn("send: %s", s.c_str());
            conn->send(query, len);
        }
    }
}

// 主服务器将命令写入到复制积压缓冲区中
void dbserver::append_copy_backlog_buffer(const char *query, size_t len)
{
    master_offset += len;
    copy_backlog_buffer.put(query, len);
}

void dbserver::append_partial_resync_data(context_t& con, size_t off)
{
    std::string buffer;
    buffer.reserve(off);
    copy_backlog_buffer.get(buffer.data(), off);
    con.append(buffer);
}

static void append_timestamp(context_t& con, const std::string& timestr, bool is_seconds)
{
    auto tv = atoll(timestr.c_str());
    if (is_seconds) tv *= 1000;
    std::string value = i2s(tv + angel::util::get_cur_time_ms());
    con.append_reply_string(value);
}

// 将解析后的命令转换成RESP形式的命令
// EXPIRE命令将被转换为PEXPIRE
void dbserver::conv2resp_with_expire(std::string& buffer, const argv_t& argv,
                                     const char *query, size_t len)
{
    context_t con;
    size_t size = con.argv.size();
    if (con.iscmd("SET") && size >= 5) {
        con.append_reply_multi(size);
        for (size_t i = 0; i < size; i++) {
            if (i > 3 && con.isequal(i-1, "EX")) {
                append_timestamp(con, con.argv[i], true);
            } else if (i > 3 && con.isequal(i-1, "PX")) {
                append_timestamp(con, con.argv[i], false);
            } else {
                con.append_reply_string(con.argv[i]);
            }
        }
    } else if (con.iscmd("EXPIRE")) {
        con.append_reply_multi(size);
        con.append_reply_string("PEXPIRE");
        con.append_reply_string(con.argv[1]);
        append_timestamp(con, con.argv[2], true);
    } else if (con.iscmd("PEXPIRE")) {
        con.append_reply_multi(size);
        con.append_reply_string("PEXPIRE");
        con.append_reply_string(con.argv[1]);
        append_timestamp(con, con.argv[2], true);
    } else {
        if (query)
            con.append(query, len);
        else
            conv2resp(con.buf, argv);
    }
    con.buf.swap(buffer);
}

void dbserver::connect_master_server()
{
    reset_connection_with_master();
    master_cli.reset(new angel::client(loop, master_addr));
    master_cli->not_exit_loop();
    master_cli->set_connection_handler([this](const angel::connection_ptr& conn){
            this->send_ping_to_master(conn);
            });
    master_cli->set_message_handler([this](const angel::connection_ptr& conn, angel::buffer& buf){
            this->recv_sync_from_master(conn, buf);
            });
    master_cli->set_close_handler([this](const angel::connection_ptr& conn){
            this->slave_close_handler(conn);
            });
    master_cli->start();
}

void dbserver::reset_connection_with_master()
{
    if (!master_cli) return;
    if (heartbeat_timer_id > 0)
        loop->cancel_timer(heartbeat_timer_id);
    if (repl_timeout_timer_id > 0)
        loop->cancel_timer(repl_timeout_timer_id);
    master_cli.reset();
}

void dbserver::send_ping_to_master(const angel::connection_ptr& conn)
{
    db->slave_connection_handler(conn);
    auto& con = get_context(conn);
    con.flags |= context_t::SYNC_PING;
    conn->send("*1\r\n$4\r\nPING\r\n");
    // 从服务器向主服务器发送完PING之后，如果repl_timeout时间后没有收到
    // 有效回复，就会认为此次复制失败，然后会重连主服务器
    repl_timeout_timer_id = loop->run_after(server_conf.repl_timeout,
            [this, &con]{
            if (con.flags & context_t::SYNC_PING)
                this->connect_master_server();
            });
}

// +FULLRESYNC\r\n<runid>\r\n<offset>\r\n
// +CONTINUE\r\n
// <snapshot-file-size>\r\n\r\n<snapshot>
// <sync command>
// SYNC_PING -> SYNC_WAIT -> SYNC_FULL -> SYNC_COMMAND
//                             -> SYNC_COMMAND
void dbserver::recv_sync_from_master(const angel::connection_ptr& conn, angel::buffer& buf)
{
    auto& con = get_context(conn);
    while (true) {
        if (con.flags & context_t::SYNC_PING) {
            // 等待接收PING的有效回复PONG，如果长时间没有收到就会
            // 触发repl_timeout_timer超时
            if (!buf.starts_with("+PONG\r\n"))
                return;
            log_info("recvd +PONG");
            con.flags &= ~context_t::SYNC_PING;
            con.flags |= context_t::SYNC_WAIT;
            send_addr_to_master(conn);
            send_sync_command_to_master(conn);
            buf.retrieve(7);
        } else if (con.flags & context_t::SYNC_WAIT) {
            if (buf.starts_with("+FULLRESYNC\r\n")) {
                log_info("recvd +FULLRESYNC");
                // 进行完全重同步
                char *s = buf.peek();
                const char *ps = s;
                s += 13;
                int crlf = buf.find(s, "\r\n");
                if (crlf < 0) return;
                set_run_id(master_run_id);
                s += crlf + 2;
                crlf = buf.find(s, "\r\n");
                if (crlf < 0) return;
                slave_offset = atoll(s);
                s += crlf + 2;
                buf.retrieve(s - ps);
                con.flags &= ~context_t::SYNC_WAIT;
                con.flags |= context_t::SYNC_FULL;
            } else if (buf.starts_with("+CONTINUE\r\n")) {
                log_info("recvd +CONTINUE");
                // 进行部分重同步
                buf.retrieve(11);
                con.flags &= ~context_t::SYNC_WAIT;
                con.flags |= context_t::CONNECT_WITH_MASTER | context_t::SYNC_COMMAND;
                master_cli->conn()->set_message_handler(
                        [this](const angel::connection_ptr& conn, angel::buffer& buf){
                        this->slave_message_handler(conn, buf);
                        });
                set_heartbeat_timer(conn);
            } else
                break;
        } else if (con.flags & context_t::SYNC_FULL) {
            recv_snapshot_from_master(conn, buf);
            break;
        } else if (con.flags & context_t::SYNC_COMMAND) {
            slave_message_handler(conn, buf);
            break;
        }
    }
}

void dbserver::slave_close_handler(const angel::connection_ptr& conn)
{
    db->slave_close_handler(conn);
}

// 从服务器向主服务器发送自己的ip和port
void dbserver::send_addr_to_master(const angel::connection_ptr& conn)
{
    std::string message;
    message += "*4\r\n$8\r\nreplconf\r\n$4\r\naddr\r\n$";
    message += i2s(strlen(i2s(server.listen_addr().to_host_port())));
    message += "\r\n";
    message += i2s(server.listen_addr().to_host_port());
    message += "\r\n$";
    message += i2s(strlen(server.listen_addr().to_host_ip()));
    message += "\r\n";
    message += server.listen_addr().to_host_ip();
    message += "\r\n";
    conn->send(message);
}

// 从服务器向主服务器发送PSYNC命令
void dbserver::send_sync_command_to_master(const angel::connection_ptr& conn)
{
    if (flags & SLAVE) {
        std::string message;
        message += "*3\r\n$5\r\nPSYNC\r\n$32\r\n";
        message += master_run_id;
        message += "\r\n$";
        message += i2s(strlen(i2s(slave_offset)));
        message += "\r\n";
        message += i2s(slave_offset);
        message += "\r\n";
        conn->send(message);
    } else {
        // slave -> master: 第一次复制
        flags &= ~MASTER;
        flags |= SLAVE;
        // setAllSlavesToReadonly();
        const char *sync = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
        conn->send(sync);
    }
}

void dbserver::set_heartbeat_timer(const angel::connection_ptr& conn)
{
    heartbeat_timer_id = loop->run_every(server_conf.repl_ping_period, [this, conn]{
            this->send_ack_to_master(conn);
            conn->send("*1\r\n$4\r\nPING\r\n");
            });
}

void dbserver::update_heartbeat_time()
{
    auto now = angel::util::get_cur_time_ms();
    if (last_recv_heartbeat_time == 0) {
        last_recv_heartbeat_time = now;
        return;
    }
    if (now - last_recv_heartbeat_time > server_conf.repl_timeout) {
        connect_master_server();
    } else
        last_recv_heartbeat_time = now;
}

// 从服务器定时向主服务器发送ACK
void dbserver::send_ack_to_master(const angel::connection_ptr& conn)
{
    std::string message;
    message += "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$";
    message += i2s(strlen(i2s(slave_offset)));
    message += "\r\n";
    message += i2s(slave_offset);
    message += "\r\n";
    conn->send(message);
}

// 从服务器接收来自主服务器的快照
void dbserver::recv_snapshot_from_master(const angel::connection_ptr& conn, angel::buffer& buf)
{
    auto& con = get_context(conn);
    if (sync_file_size == 0) {
        int crlf = buf.find("\r\n\r\n");
        if (crlf <= 0) return;
        sync_file_size = atoll(buf.peek());
        buf.retrieve(crlf + 4);
        strcpy(sync_tmp_file, "tmp.XXXXX");
        mktemp(sync_tmp_file);
        sync_fd = open(sync_tmp_file, O_RDWR | O_APPEND | O_CREAT, 0660);
        if (buf.readable() == 0) return;
    }
    size_t write_bytes = buf.readable();
    if (write_bytes >= sync_file_size) {
        write_bytes = sync_file_size;
        sync_file_size = 0;
    } else {
        sync_file_size -= write_bytes;
    }
    ssize_t n = write(sync_fd, buf.peek(), write_bytes);
    if (n > 0) buf.retrieve(n);
    if (sync_file_size > 0) return;
    fsync(sync_fd);
    rename(sync_tmp_file, db->get_snapshot_name().c_str());
    db->load_snapshot();
    con.flags &= ~context_t::SYNC_FULL;
    con.flags |= context_t::CONNECT_WITH_MASTER | context_t::SYNC_COMMAND;
    master_cli->conn()->set_message_handler(
            [this, conn](const angel::connection_ptr& conn, angel::buffer& buf){
            this->slave_message_handler(conn, buf);
            });
    set_heartbeat_timer(conn);
    log_info("recvd snapshot from master");
}

// 主服务器将生成的rdb快照发送给从服务器
void dbserver::send_snapshot_to_slaves()
{
    int fd = open(db->get_snapshot_name().c_str(), O_RDONLY);
    if (fd < 0) {
        // logError("can't open %s:%s", server_conf.rdb_file.c_str(), angel::strerrno());
        return;
    }
    off_t filesize = get_filesize(fd);
    for (auto& it : slaves) {
        auto conn = server.get_connection(it.first);
        if (!conn) continue;
        auto& con = get_context(conn);
        if (con.flags & context_t::SYNC_SNAPSHOT) {
            conn->send(i2s(filesize));
            conn->send("\r\n\r\n");
        }
    }
    angel::buffer buf;
    while (buf.read_fd(fd) > 0) {
        for (auto& it : slaves) {
            auto conn = server.get_connection(it.first);
            if (!conn) continue;
            auto& con = get_context(conn);
            if (con.flags & context_t::SYNC_SNAPSHOT) {
                conn->send(buf.peek(), buf.readable());
            }
        }
    }
    for (auto& it : slaves) {
        auto conn = server.get_connection(it.first);
        if (!conn) continue;
        auto& con = get_context(conn);
        if (con.flags & context_t::SYNC_SNAPSHOT) {
            con.flags &= ~context_t::SYNC_SNAPSHOT;
            con.flags |= context_t::SYNC_COMMAND;
            if (sync_buffer.size() > 0)
                conn->send(sync_buffer);
        }
    }
    sync_buffer.clear();
    close(fd);
}

// SLAVEOF host port
// SLAVEOF no one
void dbserver::slaveof(context_t& con)
{
    if (con.isequal(1, "no") && con.isequal(2, "one")) {
        reset_connection_with_master();
        flags &= ~SLAVE;
        ret(con, shared.ok);
    }
    int port = str2l(con.argv[2]);
    if (str2numerr()) ret(con, shared.integer_err);
    con.append(shared.ok);
    if (port == server_conf.port && con.argv[1].compare(server_conf.ip) == 0) {
        log_warn("try connect to self");
        return;
    }
    log_info("connect to %s:%d", con.argv[1].c_str(), port);
    master_addr = angel::inet_addr(con.argv[1].c_str(), port);
    connect_master_server();
}

// PSYNC ? -1
// PSYNC run_id repl_off
void dbserver::psync(context_t& con)
{
    if (!con.isequal(1, "?") && !con.isequal(2, "-1")) {
        // try partial sync
        if (con.isequal(1, run_id)) goto sync;
        size_t slave_off = atoll(con.argv[2].c_str());
        size_t off = master_offset - slave_off;
        if (off > copy_backlog_buffer.size()) goto sync;
        // 执行部分重同步
        con.flags |= context_t::SYNC_COMMAND;
        con.append("+CONTINUE\r\n");
        append_partial_resync_data(con, off);
        return;
    }
sync:
    // 执行完整重同步
    con.flags |= context_t::CONNECT_WITH_SLAVE | context_t::SYNC_SNAPSHOT;
    con.append("+FULLRESYNC\r\n");
    con.append(run_id);
    con.append("\r\n");
    con.append(i2s(master_offset));
    con.append("\r\n");
    if (db->is_creating_snapshot()) {
        // 虽然服务器后台正在生成快照，但没有从服务器在等待，即服务器并没有
        // 记录此期间执行的写命令，所以之后仍然需要重新生成一次快照
        if (!(flags & PSYNC)) flags |= PSYNC_DELAY;
        return;
    }
    flags |= MASTER | PSYNC;
    db->creat_snapshot();
}

// REPLCONF addr <port> <ip>
// REPLCONF ack repl_off
void dbserver::replconf(context_t& con)
{
    if (con.isequal(1, "addr")) {
        auto it = slaves.find(con.conn->id());
        if (it == slaves.end()) {
            log_info("found a new slave %s:%s", con.argv[3].c_str(), con.argv[2].c_str());
            slaves.emplace(con.conn->id(), master_offset);
        }
        con.slave_addr = angel::inet_addr(con.argv[3].c_str(), atoi(con.argv[2].c_str()));
    } else if (con.isequal(1, "ack")) {
        size_t slave_off = atoll(con.argv[2].c_str());
        auto it = slaves.find(con.conn->id());
        assert(it != slaves.end());
        it->second = slave_off;
    }
}

void dbserver::ping(context_t& con)
{
    con.append("+PONG\r\n");
}

// PUBLISH channel message
void dbserver::publish(context_t& con)
{
    auto& channel = con.argv[1];
    auto& message = con.argv[2];
    size_t sub_clients = pub_message(message, channel);
    con.append_reply_number(sub_clients);
}

// SUBSCRIBE channel [channel ...]
void dbserver::subscribe(context_t& con)
{
    con.append("+Reading messages... (press Ctrl-C to quit)\r\n");
    for (size_t i = 1; i < con.argv.size(); i++) {
        sub_channel(con.argv[i], con.conn->id());
        con.append_reply_multi(3);
        con.append_reply_string("SUBSCRIBE");
        con.append_reply_string(con.argv[i]);
        con.append_reply_number(i);
    }
}

size_t dbserver::pub_message(const std::string& msg, const std::string& channel)
{
    auto idlist = pubsub_channels.find(channel);
    if (idlist == pubsub_channels.end()) return 0;
    std::string message;
    size_t pub_clients = 0;
    for (auto& id : idlist->second) {
        auto conn = server.get_connection(id);
        if (!conn) continue;
        message.append("*3\r\n$7\r\nmessage\r\n$");
        message.append(i2s(channel.size()));
        message.append("\r\n" + channel + "\r\n$");
        message.append(i2s(msg.size()));
        message.append("\r\n");
        conn->send(message);
        conn->send(msg);
        conn->send("\r\n");
        message.clear();
        pub_clients++;
    }
    return pub_clients;
}

void dbserver::sub_channel(const std::string& channel, size_t id)
{
    auto idlist = pubsub_channels.find(channel);
    if (idlist == pubsub_channels.end()) {
        pubsub_channels[channel] = { id };
    } else {
        idlist->second.push_back(id);
    }
}

void dbserver::info(context_t& con)
{
    int i = 0;
    con.append("+run_id:");
    con.append(run_id);
    con.append("\n");
    con.append("role:");
    if ((flags & MASTER) && (flags & SLAVE)) con.append("master-slave\n");
    else if (flags & MASTER) con.append("master\n");
    else if (flags & SLAVE) ret(con, "slave\r\n");
    con.append("connected_slaves:");
    con.append(i2s(slaves.size()));
    con.append("\n");
    for (auto& it : slaves) {
        auto conn = server.get_connection(it.first);
        if (!conn) continue;
        auto& context = get_context(conn);
        con.append("slave");
        con.append(i2s(i));
        con.append(":ip=");
        con.append(context.slave_addr.to_host_ip());
        con.append(",port=");
        con.append(i2s(context.slave_addr.to_host_port()));
        con.append(",offset=");
        con.append(i2s(it.second));
        con.append("\n");
        i++;
    }
    con.append("\r\n");
}

int dbserver::check_range(context_t& con, int& start, int& stop, int lower, int upper)
{
    if (start > upper || stop < lower) {
        con.append(shared.nil);
        return C_ERR;
    }
    if (start < 0 && start >= lower) {
        start += upper + 1;
    }
    if (stop < 0 && stop >= lower) {
        stop += upper + 1;
    }
    if (start < lower) {
        start = 0;
    }
    if (stop > upper) {
        stop = upper;
    }
    if (start > stop) {
        con.append(shared.nil);
        return C_ERR;
    }
    return C_OK;
}

#define BIND(f) std::bind(&dbserver::f, this, std::placeholders::_1)

void dbserver::start()
{
    __server = this;
    cmdtable = {
        { "SLAVEOF",    { -3, IS_READ, BIND(slaveof) } },
        { "PSYNC",      { -3, IS_READ, BIND(psync) } },
        { "REPLCONF",   {  3, IS_READ, BIND(replconf) } },
        { "PING",       { -1, IS_READ, BIND(ping) } },
        { "PUBLISH",    { -3, IS_READ, BIND(publish) } },
        { "SUBSCRIBE",   {  2, IS_READ, BIND(subscribe) } },
        { "CONFIG",     {  3, IS_READ, BIND(config) } },
        { "INFO",       { -1, IS_READ, BIND(info) } }
    };
    db->start();
    loop->run_every(100, [this]{ this->server_cron(); });
    server.set_exit_handler([this]{ this->db->exit(); });
    // 服务器以从服务器方式运行
    if (server_conf.master_port > 0) {
        context_t con(nullptr, db.get());
        con.argv.emplace_back("SLAVEOF");
        con.argv.emplace_back(server_conf.master_ip);
        con.argv.emplace_back(i2s(server_conf.master_port));
        executor(con, nullptr, 0);
    }
    server.start();
}

///////////////////////main function/////////////////////////////

#include <getopt.h>

static struct option opts[] = {
    { "serverconf", 1, NULL, 'a' },
    { "sentinel", 0, NULL, 'b' },
    { "sentinelconf", 1, NULL, 'c' },
    { "help", 0, NULL, 'h' },
};

static void help()
{
    fprintf(stderr, "default <server-conf-file=alice.conf>\n"
                    "--serverconf <file>\n"
                    "--sentinel [run as a sentinel] <sentinel-conf-file=sentinel.conf>\n"
                    "--sentinelconf <file> [run as a sentinel] <sentinel-conf-file=file>\n");
}

int main(int argc, char *argv[])
{
    int c;
    bool startup_sentinel = false;
    std::string server_conf_file = "alice.conf";
    std::string sentinel_conf_file = "sentinel.conf";
    while ((c = getopt_long(argc, argv, "a:bc:h", opts, NULL)) != -1) {
        switch (c) {
        case 'a': server_conf_file = optarg; break;
        case 'b': startup_sentinel = true; break;
        case 'c': startup_sentinel = true; sentinel_conf_file = optarg; break;
        case 'h': help(); return 0;
        }
    }

    angel::evloop loop;
    alice::read_server_conf(server_conf_file);
    if (startup_sentinel) {
        alice::read_sentinel_conf(sentinel_conf_file);
        std::cout << sentinel_conf_file << "\n";
        angel::inet_addr listen_addr(sentinel_conf.ip, sentinel_conf.port);
        Sentinel sentinel(&loop, listen_addr);
        log_info("sentinel %s runid is %s", listen_addr.to_host(), sentinel.get_run_id());
        sentinel.start();
        loop.run();
        return 0;
    }
    angel::inet_addr listen_addr(server_conf.ip, server_conf.port);
    dbserver server(&loop, listen_addr);
    log_info("server %s runid is %s", listen_addr.to_host(), server.get_run_id());
    server.start();
    loop.run();
}
