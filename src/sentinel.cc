//
// Raft算法中有关领头选举部分的总结：
//
// 在任意的时间，每一个服务器一定会处于以下三种状态中的一个：
// 领导人(leader)、候选人(candidate)、追随者(follower)
//
// 任期(term)在Raft中充当逻辑时钟的角色，它允许服务器检测过期的消息
// term会单调递增，当服务器之间进行通信时，会互换当前的term，如果有一台服务器的
// term较小，则更新为较大的
// 如果一个candidate或leader意识到自己的term过期了，它会立刻转换为follower状态
// 如果一台服务器收到的请求的term是过期的，则它会拒绝此次请求
//
// 服务器启动时都会初始化为follower，leader向所有follower周期性发送心跳消息
// 如果一个follower在一个周期内没有收到心跳消息，就叫做选举超时，自身会转为
// candidate，从而发起新一轮选举
// 开始选举后，它会首先自增它的current term，重置选举超时时间，然后，先给自己
// 投上一票，并且给其他服务器发送请求，请求它们将自己设置为leader
// 接下来，它会一直处于candidate状态，直到以下事件发生：
// 1）它赢得了选举
// 2）另一台服务器赢得了选举
// 3）一段时间后没有任何一台服务器赢得了选举
//
// 一个candidate如果在一个term内收到了集群中大多数服务器的投票就会赢得选举
// 在一个term内，一台服务器最多只能投出一票，遵循先到先投的原则
//
// 当一个candidate在等待别人的选票时，它有可能会收到别的服务器发送来的声明其
// 为leader的消息。如果leader.term >= candidate.term，则它认为该leader合法，
// 并且将自身转为follower，否则的话，它会拒绝此次服务，继续保持candidate状态
//
// 当没有人赢得选举时(如果多个candidate同时发起选举，则选票就会被分散，导致
// 没有candidate获得大多数选票)，每个candidate都会超时，会在一段时间后发起
// 新的一轮选举。为了避免它们同时开始进行选举，以至于无限重复上述过程，我们
// 会随机选出一个超时时间，这使得在大多数情况下只有一个服务器会率先超时，
// 并且会率先赢得选举
//
// Sentinel的领头选举大体上遵循这个过程，但在些微细节上可能有所不同
// note: term <==> epoch
//

#include <assert.h>

#include "sentinel.h"
#include "config.h"

namespace alice {

#define BIND(f) std::bind(&Sentinel::f, this, std::placeholders::_1)

// 选举领头和投票时设置的超时定时器的基值
#define ELECT_TIMEOUT 1000

Sentinel::Sentinel(angel::evloop *loop, angel::inet_addr listen_addr)
    : loop(loop),
    server(loop, listen_addr),
    current_epoch(0)
{
    server.set_connection_handler([this](const angel::connection_ptr& conn){
            this->connection_handler(conn);
            });
    server.set_message_handler([this](const angel::connection_ptr& conn, angel::buffer& buf){
            this->message_handler(conn, buf);
            });
    cmdtable = {
        { "PING",       { -1, 0, BIND(ping) } },
        { "INFO",       { -3, 0, BIND(info) } },
        { "SENTINEL",   {  2, 0, BIND(sentinel) } },
    };
    run_id = generate_run_id();
    // server.daemon();
}

void Sentinel::executor(context_t& con)
{
    std::transform(con.argv[0].begin(), con.argv[0].end(), con.argv[0].begin(), ::toupper);
    auto c = cmdtable.find(con.argv[0]);
    if (c == cmdtable.end()) {
        con.append("-ERR unknown command `" + con.argv[0] + "`\r\n");
        goto end;
    }
    if ((c->second.arity > 0 && con.argv.size() < c->second.arity) ||
        (c->second.arity < 0 && con.argv.size() != -c->second.arity)) {
        con.append("-ERR wrong number of arguments for '" + con.argv[0] + "'\r\n");
        goto end;
    }
    c->second.command_cb(con);
end:
    con.argv.clear();
}

// sentinel在连接主服务器或从服务器时，需要同时创建命令连接和订阅连接，
// 这是因为sentinel需要通过主服务器或从服务器发送来的频道信息来发现新的sentinel
// 而在连接新发现的sentinel时，只需要创建命令连接即可
void Sentinel::start()
{
    server.start();
    for (auto& ins : sentinel_conf.insmap) {
        auto master = new SentinelInstance;
        master->sentinel = this;
        master->flags |= SentinelInstance::MASTER;
        master->name = ins.second.name;
        master->inet_addr = angel::inet_addr(ins.second.ip, ins.second.port);
        master->quorum = ins.second.quorum;
        master->down_after_period = ins.second.down_after_period;
        master->creat_cmd_connection();
        master->creat_sub_connection();
        masters.emplace(master->name, master);
    }
    loop->run_every(1000, [this]{ this->send_ping_to_servers(); });
    loop->run_every(1000 * 2, [this]{ this->send_pub_message_to_servers(); });
    loop->run_every(1000 * 2, [this]{ this->send_info_to_servers(); });
    loop->run_every(100, [this]{ this->sentinel_cron(); });
}

// 向(master slave or sentinel)创建一条命令连接
void SentinelInstance::creat_cmd_connection()
{
    client[0].reset(new angel::client(sentinel->loop, inet_addr));
    client[0]->set_message_handler([this](const angel::connection_ptr& conn, angel::buffer& buf){
            this->recv_reply_from_server(conn, buf);
            });
    client[0]->set_close_handler([this](const angel::connection_ptr& conn){
            this->close_connection_handler(conn);
            });
    client[0]->start();
}

// 向(master slave or sentinel)创建一条订阅连接
void SentinelInstance::creat_sub_connection()
{
    client[1].reset(new angel::client(sentinel->loop, inet_addr));
    client[1]->set_connection_handler([this](const angel::connection_ptr& conn){
            this->sentinel->sub_server(conn);
            });
    client[1]->set_message_handler([this](const angel::connection_ptr& conn, angel::buffer& buf){
            this->sentinel->recv_pub_message_from_server(conn, buf);
            });
    client[1]->start();
}

// 对端断开连接时被调用
void SentinelInstance::close_connection_handler(const angel::connection_ptr& conn)
{
    flags |= S_DOWN;
    if (flags & MASTER)
        ask_master_state_for_others();
    log_info("connection reset by %s", conn->get_peer_addr().to_host());
}

// 向监视同一master的所有sentinel发送SENTINEL is-master-down-by-addr命令
void SentinelInstance::ask_for_sentinels(const char *runid)
{
    std::string message;
    argv_t argv = { "SENTINEL", "is-master-down-by-addr" };
    argv.emplace_back(inet_addr.to_host_ip());
    argv.emplace_back(i2s(inet_addr.to_host_port()));
    argv.emplace_back(i2s(sentinel->current_epoch));
    argv.emplace_back(runid);
    conv2resp(message, argv);
    for (auto& it : sentinels) {
        auto& cli = it.second->client[0];
        if (cli->is_connected())
            cli->conn()->send(message);
    }
}

// 向其他sentinel询问当前已进入主观下线状态的master的状态
void SentinelInstance::ask_master_state_for_others()
{
    if (sentinels.empty()) {
        // 只有一个sentinel，不必询问master的状态和选举leader
        flags |= O_DOWN | HAVE_LEADER;
        select_new_master();
        return;
    }
    log_info("%s ask master %s state for other sentinels",
            sentinel->get_run_id(), name.c_str());
    ask_for_sentinels("*");
}

static void send_ping(const std::unique_ptr<SentinelInstance>& si)
{
    auto& cli = si->client[0];
    if (cli->is_connected()) {
        cli->conn()->send("*1\r\n$4\r\nPING\r\n");
    }
}

static void send_pub_message(const std::unique_ptr<SentinelInstance>& si)
{
    auto& cli = si->client[0];
    if (cli->is_connected()) {
        si->pub_message(cli->conn());
    }
}

static void send_info(const std::unique_ptr<SentinelInstance>& si)
{
    auto& cli = si->client[0];
    if (cli->is_connected()) {
        cli->conn()->send("*1\r\n$4\r\nINFO\r\n");
    }
}

void Sentinel::for_each_instance(functor&& func, bool have_sentinel)
{
    for (auto it = masters.begin(); it != masters.end(); ) {
        auto master = it++;
        if (!(master->second->flags & SentinelInstance::S_DOWN))
            func(master->second);
        if (master->second->flags & SentinelInstance::DELETE) {
            masters.erase(master);
            continue;
        }
        auto& slaves = master->second->slaves;
        for (auto it = slaves.begin(); it != slaves.end(); ) {
            auto slave = it++;
            if (slave->second->flags & SentinelInstance::S_DOWN) {
                slaves.erase(slave);
                continue;
            }
            func(slave->second);
        }
        if (!have_sentinel) continue;
        auto& sentinels = master->second->sentinels;
        for (auto it = sentinels.begin(); it != sentinels.end(); ) {
            auto sentinel = it++;
            if (sentinel->second->flags & SentinelInstance::S_DOWN) {
                sentinels.erase(sentinel);
                continue;
            }
            func(sentinel->second);
        }
    }
}

// 向所有被监控的主从服务器以及对应的sentinel发送PING，
// 以维持心跳连接
void Sentinel::send_ping_to_servers()
{
    for_each_instance(send_ping, true);
}

// 向所有被监控的主从服务器发送频道消息
void Sentinel::send_pub_message_to_servers()
{
    for_each_instance(send_pub_message, false);
}

// 向所有被监控的主从服务器发送INFO命令
void Sentinel::send_info_to_servers()
{
    for_each_instance(send_info, false);
}

void SentinelInstance::pub_message(const angel::connection_ptr& conn)
{
    std::string message;
    message += sentinel->server.listen_addr().to_host_ip();
    message += ",";
    message += i2s(sentinel->server.listen_addr().to_host_port());
    message += ",";
    message += sentinel->get_run_id();
    message += ",";
    message += i2s(sentinel->current_epoch);
    message += ",";
    message += name;
    message += ",";
    message += inet_addr.to_host_ip();
    message += ",";
    message += i2s(inet_addr.to_host_port());
    message += ",";
    message += i2s(config_epoch);
    argv_t argv = { "PUBLISH", "__sentinel__:hello", message };
    conv2resp(message, argv);
    conn->send(message);
}

// 接收来自主从服务器的回复信息
void SentinelInstance::recv_reply_from_server(const angel::connection_ptr& conn,
                                              angel::buffer& buf)
{
    while (buf.readable() >= 2) {
        int crlf = buf.find_crlf();
        if (crlf >= 0) {
            if (buf.starts_with("+PONG\r\n")) {
                last_heartbeat_time = angel::util::get_cur_time_ms();
            } else if (buf.starts_with("+LOADING\r\n")) {
                last_heartbeat_time = angel::util::get_cur_time_ms();
            } else if (buf.starts_with("+MASTERDOWN\r\n")) {
                last_heartbeat_time = angel::util::get_cur_time_ms();
            } else {
                const char *s = buf.peek();
                const char *es = s + crlf + 2;
                if (flags & MASTER)
                    parse_info_reply_from_master(s, es);
                else if (flags & SLAVE)
                    parse_info_reply_from_slave(s, es);
                else if (flags & SENTINEL)
                    parse_reply_from_sentinel(s, es);
            }
            buf.retrieve(crlf + 2);
        } else
            break;
    }
}

void SentinelInstance::parse_info_reply_from_master(const char *s, const char *es)
{
    if (s[0] != '+') return;
    s += 1;
    while (1) {
        const char *p = std::find(s, es, '\n');
        if (p[-1] == '\r') p -= 1;
        if (strncasecmp(s, "run_id:", 7) == 0) {
            run_id = std::string(s + 7, p);
        } else if (strncasecmp(s, "role:", 5) == 0) {

        } else if (strncasecmp(s, "connected_slaves:", 17) == 0) {

        } else if (strncasecmp(s, "slave", 5) == 0) {
            update_slaves(s, es);
        }
        if (p[0] == '\r') break;
        s = p + 1;
    }
}

void SentinelInstance::parse_info_reply_from_slave(const char *s, const char *es)
{
    if (s[0] != '+') return;
    s += 1;
    while (1) {
        const char *p = std::find(s, es, '\n');
        if (p[-1] == '\r') p -= 1;
        if (strncasecmp(s, "run_id:", 7) == 0) {
            run_id = std::string(s + 7, p);
        } else if (strncasecmp(s, "role:", 5) == 0) {
            std::string role;
            role.assign(s + 5, p);
            // 将此次选举中由leader选出的从服务器被提升为主服务器
            if (strcasecmp(role.c_str(), "master") == 0) {
                log_info("new master is %s", name.c_str());
                auto master = sentinel->masters.find(this->master);
                assert(master != sentinel->masters.end());
                master->second->convert_slave_to_master(this);
                master->second->flags &= ~(S_DOWN | O_DOWN);
                return;
            }
        } else if (strncasecmp(s, "master_host:", 12) == 0) {
            inet_addr = angel::inet_addr(std::string(s+12,p).c_str(), inet_addr.to_host_port());
        } else if (strncasecmp(s, "master_port:", 12) == 0) {
            inet_addr = angel::inet_addr(inet_addr.to_host_ip(), atoi(s+12));
        } else if (strncasecmp(s, "master_link_status:", 20) == 0) {

        } else if (strncasecmp(s, "slave_repl_offset:", 18) == 0) {
            offset = atol(s + 18);
        } else if (strncasecmp(s, "slave_priority:", 15) == 0) {

        }
        if (p[0] == '\r') break;
        s = p + 1;
    }
}

void SentinelInstance::update_slaves(const char *s, const char *es)
{
    s = std::find(s, es, ':') + 1;
    std::string ip, name;
    int port;
    while (1) {
        const char *f = std::find(s, es, ',');
        if (strncasecmp(s, "ip=", 3) == 0) {
            ip.assign(s + 3, f - s - 3);
        } else if (strncasecmp(s, "port=", 5) == 0) {
            port = atoi(s + 5);
        } else if (strncasecmp(s, "offset=", 7) == 0) {
            size_t offset = atoll(s + 7);
            name = ip + ":";
            name.append(i2s(port));
            auto it = slaves.find(name);
            if (it == slaves.end()) {
                log_info("found a new slave %s", name.c_str());
                auto slave = new SentinelInstance;
                slave->sentinel = sentinel;
                slave->flags |= SentinelInstance::SLAVE;
                slave->master = name;
                slave->name = name;
                slave->inet_addr = angel::inet_addr(ip, port);
                slave->offset = offset;
                slave->down_after_period = down_after_period;
                slave->creat_cmd_connection();
                slave->creat_sub_connection();
                slaves.emplace(name, slave);
            } else {
                it->second->offset = offset;
            }
        }
        if (f == es) break;
        s = f + 1;
    }
}

// 处理SENTINEL is-master-down-by-addr命令的回复
void SentinelInstance::parse_reply_from_sentinel(const char *s, const char *es)
{
    auto master = sentinel->masters.find(this->master);
    assert(master != sentinel->masters.end());
    auto half = (master->second->sentinels.size() + 1) / 2 + 1;

    argv_t argv;
    split_line(argv, s + 1, es, ',');
    auto& down_state = argv[0], &leader_runid = argv[1];
    uint64_t leader_epoch = atoll(argv[2].c_str());

    // 有关master是否进入主观下线状态的回复
    if (leader_runid.compare("*") == 0) {
        if (master->second->flags & O_DOWN) return;
        // 对方同意该master进入主观下线状态
        if (down_state.compare("1") == 0) {
            master->second->votes++;
            // 多数sentinel达成一致，该master进入客观下线状态，并开始进行
            // 故障转移操作
            if (master->second->votes >= half
                    && master->second->votes >= master->second->quorum) {
                master->second->votes = 0;
                master->second->flags |= O_DOWN;
                if (!(master->second->flags & FOLLOWER))
                    master->second->start_failover();
            }
        }
    } else if (leader_runid.compare(sentinel->get_run_id()) == 0) {
        if (master->second->flags & HAVE_LEADER) return;
        // 选举领头的回复
        if (master->second->failover_epoch == leader_epoch) {
            master->second->votes++;
            if (master->second->votes >= half
                    && master->second->votes >= master->second->quorum) {
                // 当前sentinel成为领头
                master->second->votes = 0;
                master->second->flags |= HAVE_LEADER;
                master->second->notice_leader_to_others();
                master->second->cancel_elect_timeout_timer();
                log_info("%s become leader", sentinel->get_run_id());
                master->second->select_new_master();
            }
        }
    }
}

// 请求被其他sentinel选举为领头
void SentinelInstance::start_failover()
{
    log_info("%s start electing", sentinel->get_run_id());
    sentinel->current_epoch++;
    leader = sentinel->get_run_id();
    leader_epoch = sentinel->current_epoch;
    failover_epoch = sentinel->current_epoch;
    votes++;
    set_elect_timeout_timer();
    ask_for_sentinels(sentinel->get_run_id());
}

// 设置一个随机的超时定时器
void SentinelInstance::set_elect_timeout_timer()
{
    srand(clock());
    int64_t timeout = ELECT_TIMEOUT + rand() % 300;
    elect_timeout_timer_id = sentinel->loop->run_after(timeout,
            [this]{ this->elect_timeout_handler(); });
}

void SentinelInstance::elect_timeout_handler()
{
    start_failover();
}

void SentinelInstance::cancel_elect_timeout_timer()
{
    if (elect_timeout_timer_id > 0)
        sentinel->loop->cancel_timer(elect_timeout_timer_id);
}

void Sentinel::sub_server(const angel::connection_ptr& conn)
{
    std::string message;
    argv_t argv = { "SUBSCRIBE", "__sentinel__:hello" };
    conv2resp(message, argv);
    conn->send(message);
}

void Sentinel::recv_pub_message_from_server(const angel::connection_ptr& conn,
                                            angel::buffer& buf)
{
    argv_t argv;
    while (buf.readable() >= 2) {
        // skip subscribe's reply
        if (buf[0] == '+') {
            int crlf = buf.find_crlf();
            if (crlf >= 0) {
                buf.retrieve(crlf + 2);
                continue;
            } else
                break;
        }
        ssize_t n = parse_request(argv, buf);
        if (n == 0) break;
        if (n < 0) {
            buf.retrieve_all();
            break;
        }
        auto& message = argv[2];
        update_sentinels(message.data(), message.data() + message.size());
        buf.retrieve(n);
    }
}

void Sentinel::update_sentinels(const char *s, const char *es)
{
    argv_t argv;
    split_line(argv, s, es, ',');
    auto& s_ip = argv[0], &s_port = argv[1], &s_runid = argv[2], &s_epoch = argv[3];
    auto& m_name = argv[4], &m_ip = argv[5], &m_port = argv[6], &m_epoch = argv[7];
    UNUSED(m_ip);
    UNUSED(m_port);
    UNUSED(m_epoch);
    if (s_runid.compare(get_run_id()) == 0)
        return;
    auto s_name = s_ip + ":" + s_port;
    auto master = masters.find(m_name);
    if (master == masters.end()) return;
    auto it = master->second->sentinels.find(s_name);
    if (it == master->second->sentinels.end()) {
        log_info("found a new sentinel %s", s_name.c_str());
        auto sentinel = new SentinelInstance;
        sentinel->sentinel = this;
        sentinel->flags |= SentinelInstance::SENTINEL;
        sentinel->master = m_name;
        sentinel->name = s_name;
        sentinel->inet_addr = angel::inet_addr(s_ip, atoi(s_port.c_str()));
        sentinel->run_id = s_runid;
        sentinel->config_epoch = atoll(s_epoch.c_str());
        sentinel->down_after_period = master->second->down_after_period;
        sentinel->creat_cmd_connection();
        master->second->sentinels.emplace(s_name, sentinel);
    } else {
        it->second->config_epoch = atoll(s_epoch.c_str());
    }
}

// 检查是否有服务器下线
static void check_subjective_down(const std::unique_ptr<SentinelInstance>& si, int64_t now)
{
    if (si->flags & SentinelInstance::O_DOWN) return;
    if (si->flags & SentinelInstance::S_DOWN) return;
    if (now - si->last_heartbeat_time >= si->down_after_period) {
        si->flags |= SentinelInstance::S_DOWN;
        if (si->flags & SentinelInstance::MASTER)
            si->ask_master_state_for_others();
    }
}

void Sentinel::sentinel_cron()
{
    int64_t now = angel::util::get_cur_time_ms();
    for (auto& master : masters) {
        check_subjective_down(master.second, now);
        for (auto& slave : master.second->slaves) {
            check_subjective_down(slave.second, now);
        }
        for (auto& sentinel : master.second->sentinels) {
            check_subjective_down(sentinel.second, now);
        }
    }
}

void Sentinel::ping(context_t& con)
{
    con.append("+PONG\r\n");
}

void Sentinel::info(context_t& con)
{

}

SentinelInstance *Sentinel::get_master_by_addr(const std::string& ip, int port)
{
    for (auto& master : masters) {
        if (ip.compare(master.second->inet_addr.to_host_ip()) == 0
                && port == master.second->inet_addr.to_host_port()) {
            return master.second.get();
        }
    }
    return nullptr;
}

// is-master-down-by-addr <ip> <port> <epoch> <runid>
// 如果runid为*，则是询问master的状态，如果runid为0，则是告知
// 已经选出的leader，否则的话，就是在选举领头，请求投票
void Sentinel::sentinel(context_t& con)
{
    if (con.isequal(1, "is-master-down-by-addr")) {
        auto& ip = con.argv[2];
        int port = atoi(con.argv[3].c_str());
        uint64_t epoch = atoll(con.argv[4].c_str());
        auto& runid = con.argv[5];
        auto master = get_master_by_addr(ip, port);
        if (!master) return;
        // 回复询问的master是否进入主观下线状态
        if (runid.compare("*") == 0) {
            con.append((master->flags & SentinelInstance::S_DOWN)
                    ? "+1,*,0\r\n" : "+0,*,0\r\n");
        } else { // 向请求的sentinel投票
            if (runid.compare("0") == 0) {
                master->cancel_elect_timeout_timer();
                master->flags &= ~SentinelInstance::FOLLOWER;
                if (master->slaves.empty()) {
                    masters.erase(master->name);
                }
                log_info("has already elected leader");
                return;
            }
            if (current_epoch < epoch) {
                current_epoch = epoch;
            }
            con.append("+0,");
            if (master->leader_epoch < epoch && current_epoch <= epoch) {
                master->flags |= SentinelInstance::FOLLOWER;
                master->cancel_elect_timeout_timer();
                master->leader = runid;
                master->leader_epoch = epoch;
                master->set_elect_timeout_timer();
                con.append(runid);
                con.append(",");
                con.append(con.argv[4]);
                con.append("\r\n");
                log_info("%s voted for %s", get_run_id(), runid.c_str());
            } else { // 返回投过票的局部领头
                con.append(master->leader);
                con.append(",");
                con.append(i2s(master->leader_epoch));
                con.append("\r\n");
            }
        }
    }
}

// 将选出的领头通知给其他sentinel
void SentinelInstance::notice_leader_to_others()
{
    ask_for_sentinels("0");
}

// 从下线master的所有从服务器中挑选出合适的一个作为新的master
void SentinelInstance::select_new_master()
{
    if (slaves.empty()) {
        flags |= DELETE;
        return;
    }
    // 暂时随便选一个
    auto randkey = get_rand_hash_key(slaves);
    size_t bucket = std::get<0>(randkey);
    size_t where = std::get<1>(randkey);
    for (auto it = slaves.begin(bucket); it != slaves.end(bucket); ++it)
        if (where-- == 0) {
            log_info("selected new master is %s by %s",
                    it->second->name.c_str(), name.c_str());
            it->second->stop_replicate_master();
            convert_slave_to_master(it->second.get());
            flags &= ~(S_DOWN | O_DOWN | HAVE_LEADER);
            break;
        }
    // 让其他slave复制新的master
    for (auto& slave : slaves) {
        slave.second->replicate_master(inet_addr);
    }
}

void SentinelInstance::stop_replicate_master()
{
    context_t con;
    con.append_reply_multi(3);
    con.append_reply_string("SLAVEOF");
    con.append_reply_string("NO");
    con.append_reply_string("ONE");
    client[0]->conn()->send(con.buf);
}

void SentinelInstance::replicate_master(angel::inet_addr& master_addr)
{
    context_t con;
    con.append_reply_multi(3);
    con.append_reply_string("SLAVEOF");
    con.append_reply_string(master_addr.to_host_ip());
    con.append_reply_string(i2s(master_addr.to_host_port()));
    client[0]->conn()->send(con.buf);
}

// 将slave转换成一个master
void SentinelInstance::convert_slave_to_master(SentinelInstance *slave)
{
    flags |= DELETE;
    // 新的master的地址仍会被保存到将要被移除的已下线的master中，
    // 因为我们之后会用到这个地址，如果不保存的话，我们就需要在
    // sentinel->masters中去找这个新的master，而这通常需要引入
    // 一些额外的标志位
    inet_addr = slave->inet_addr;
    // 这个master将会替代那个已下线的master成为new master
    auto master = new SentinelInstance;
    master->sentinel = sentinel;
    master->flags |= MASTER;
    master->name = slave->name;
    master->run_id = slave->run_id;
    master->config_epoch = slave->config_epoch;
    master->inet_addr = slave->inet_addr;
    master->creat_cmd_connection();
    master->creat_sub_connection();
    master->offset = slave->offset;
    master->last_heartbeat_time = slave->last_heartbeat_time;
    master->down_after_period = slave->down_after_period;
    // then, there is still something to do
    // 1) move this->slaves and this->sentinels to new master
    // 2) new master can discovery sentinel and slave automatically
    // there, we choose 2), so we do nothing
    sentinel->masters.emplace(master->name, master);
    slaves.erase(slave->name);
}

}
