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

#include <Angel/TcpClient.h>
#include <assert.h>
#include "sentinel.h"

#define ELECT_TIMEOUT 1000

using namespace Alice;

Sentinel::Sentinel(Angel::EventLoop *loop, Angel::InetAddr inetAddr)
    : _loop(loop),
    _server(loop, inetAddr),
    _masters(&g_sentinel_conf.masters),
    _currentEpoch(0)
{
    for (auto& db : _server.dbServer().dbs()) {
        db->commandMap() = {
            { "PING",       { -1, IS_READ, std::bind(&DB::pingCommand, db.get(), _1) } },
            { "INFO",       { -3, IS_READ, std::bind(&Sentinel::infoCommand, this, _1) } },
            { "SENTINEL",   {  2, IS_READ, std::bind(&Sentinel::sentinelCommand, this, _1) } },
        };
    }
}

// sentinel在连接主服务器或从服务器时，需要同时创建命令连接和订阅连接，
// 这是因为sentinel需要通过主服务器或从服务器发送来的频道信息来发现新的sentinel
// 而在连接新发现的sentinel时，只需要创建命令连接即可
void Sentinel::init()
{
    for (auto& it : *_masters) {
        it.second->creatCmdConnection();
        it.second->creatPubConnection();
    }
    _loop->runEvery(1000, [this]{ this->sendPingToServers(); });
    _loop->runEvery(1000 * 2, [this]{ this->sendPubMessageToServers(); });
    _loop->runEvery(1000 * 10, [this]{ this->sendInfoToServers(); });
    _loop->runEvery(100, [this]{ this->sentinelCron(); });
}

// 对于SentinelInstance对象来说，当它调用creatCmdConnection时，
// 需要保存当前对象的this指针，如果该对象是在栈上分配的，则该对象很快就会
// 被销毁，而this指针则会失效，这样，当延后回调发生的时候，就会访问到错误
// 的内存空间，从而造成一些意想不到的结果
// 因此，这种情况下，需要new一个对象，这样才能保证在该对象被释放前this指针
// 一定是有效的
// 这也是SentinelInstanceMap为什么要保存unique_ptr的原因
// @@@ debug this for two weeks @@@
void SentinelInstance::creatCmdConnection()
{
    std::string name(inetAddr().toIpAddr() +
            std::string(":") + convert(inetAddr().toIpPort()) + "-cmd");
    if (_clients[0]) _clients[0].reset();
    _clients[0].reset(
            new Angel::TcpClient(g_sentinel->loop(), inetAddr(), name.c_str()));
    _clients[0]->setMessageCb(
            std::bind(&SentinelInstance::recvReplyFromServer, this, _1, _2));
    _clients[0]->setCloseCb(
            std::bind(&SentinelInstance::closeConnection, this, _1));
    _clients[0]->notExitLoop();
    _clients[0]->start();
}

void SentinelInstance::creatPubConnection()
{
    std::string name(inetAddr().toIpAddr()
            + std::string(":") + convert(inetAddr().toIpPort()) + "-sub");
    if (_clients[1]) _clients[1].reset();
    _clients[1].reset(
            new Angel::TcpClient(g_sentinel->loop(), inetAddr(), name.c_str()));
    _clients[1]->setConnectionCb(
            std::bind(&Sentinel::subServer, g_sentinel, _1));
    _clients[1]->setMessageCb(
            std::bind(&Sentinel::recvPubMessageFromServer, g_sentinel, _1, _2));
    _clients[1]->notExitLoop();
    _clients[1]->start();
}

void SentinelInstance::closeConnection(const Angel::TcpConnectionPtr& conn)
{
    setFlag(S_DOWN);
    // master进入主观下线状态，开始向监视同一master的其他sentinel询问
    // 是否同意它进入主观下线状态
    if (flag() & MASTER) askMasterStateForOtherSentinels();
}

// 向监视同一master的所有sentinel发送SENTINEL is-master-down-by-addr命令
void SentinelInstance::askForSentinels(const char *runid)
{
    std::string message;
    Context::CommandList cmdlist = { "SENTINEL", "is-master-down-by-addr" };
    cmdlist.emplace_back(inetAddr().toIpAddr());
    cmdlist.emplace_back(convert(inetAddr().toIpPort()));
    cmdlist.emplace_back(convert(g_sentinel->currentEpoch()));
    cmdlist.emplace_back(runid);
    DBServer::appendCommand(message, cmdlist, false);
    for (auto& sentinel : sentinels()) {
        auto& cli = sentinel.second->clients()[0];
        if (cli->isConnected()) {
            cli->conn()->send(message);
        }
    }
}

void SentinelInstance::askMasterStateForOtherSentinels()
{
    logInfo("%s ask master %s state for other sentinels",
            g_sentinel->runId(), name().c_str());
    askForSentinels("*");
}

// 判断并移除已下线的从服务器和sentinel
static bool isSubjectiveDownForSlaveOrSentinel(SentinelInstance::SentinelInstanceMap& smap,
                                               const std::unique_ptr<SentinelInstance>& si)
{
    if (si->flag() & SentinelInstance::S_DOWN) {
        smap.erase(si->name());
        return true;
    } else
        return false;
}

static bool isSubjectiveDownForMaster(const std::unique_ptr<SentinelInstance>& si)
{
    return si->flag() & SentinelInstance::S_DOWN;
}

static void sendPing(const std::unique_ptr<Angel::TcpClient>& cli)
{
    if (cli->isConnected()) {
        cli->conn()->send("*1\r\n$4\r\nPING\r\n");
    }
}

// 向所有被监控的主从服务器以及对应的sentinel发送PING，
// 以维持心跳连接
void Sentinel::sendPingToServers()
{
    for (auto it = masters().begin(); it != masters().end(); ) {
        auto master = it++;
        if (isSubjectiveDownForMaster(master->second))
            continue;
        sendPing(master->second->clients()[0]);
        auto& slaves = master->second->slaves();
        for (auto it = slaves.begin(); it != slaves.end(); ) {
            auto slave = it++;
            if (isSubjectiveDownForSlaveOrSentinel(slaves, slave->second))
                continue;
            sendPing(slave->second->clients()[0]);
        }
        auto& sentinels = master->second->sentinels();
        for (auto it = sentinels.begin(); it != sentinels.end(); ) {
            auto sentinel = it++;
            if (isSubjectiveDownForSlaveOrSentinel(sentinels, sentinel->second))
                continue;
            sendPing(sentinel->second->clients()[0]);
        }
    }
}

static void sendPubMessage(const std::unique_ptr<SentinelInstance>& si)
{
    auto& cli = si->clients()[0];
    if (cli->isConnected()) si->pubMessage(cli->conn());
}

// 向所有被监控的主从服务器发送频道消息
void Sentinel::sendPubMessageToServers()
{
    for (auto it = masters().begin(); it != masters().end(); ) {
        auto master = it++;
        if (isSubjectiveDownForMaster(master->second))
            continue;
        sendPubMessage(master->second);
        auto& slaves = master->second->slaves();
        for (auto it = slaves.begin(); it != slaves.end(); ) {
            auto slave = it++;
            if (isSubjectiveDownForSlaveOrSentinel(slaves, slave->second))
                continue;
            sendPubMessage(slave->second);
        }
    }
}

static void sendInfo(const std::unique_ptr<SentinelInstance>& si)
{
    auto& cli = si->clients()[0];
    if (cli->isConnected()) cli->conn()->send("*1\r\n$4\r\nINFO\r\n");
}

// 向所有被监控的主从服务器发送INFO命令
void Sentinel::sendInfoToServers()
{
    for (auto it = masters().begin(); it != masters().end(); ) {
        auto master = it++;
        if (isSubjectiveDownForMaster(master->second))
            continue;
        sendInfo(master->second);
        auto& slaves = master->second->slaves();
        for (auto it = slaves.begin(); it != slaves.end(); ) {
            auto slave = it++;
            if (isSubjectiveDownForSlaveOrSentinel(slaves, slave->second))
                continue;
            sendInfo(master->second);
        }
    }
}

void SentinelInstance::pubMessage(const Angel::TcpConnectionPtr& conn)
{
    std::string buffer, message;
    Server& server = g_sentinel->server();
    conn->send("*3\r\n$7\r\nPUBLISH\r\n$18\r\n__sentinel__:hello\r\n$");
    buffer += server.server().inetAddr()->toIpAddr();
    buffer += ",";
    buffer += convert(server.server().inetAddr()->toIpPort());
    buffer += ",";
    buffer += server.dbServer().selfRunId();
    buffer += ",";
    buffer += convert(g_sentinel->currentEpoch());
    buffer += ",";
    buffer += name();
    buffer += ",";
    buffer += inetAddr().toIpAddr();
    buffer += ",";
    buffer += convert(inetAddr().toIpPort());
    buffer += ",";
    buffer += convert(configEpoch());
    message += convert(buffer.size());
    message += "\r\n";
    message += buffer;
    message += "\r\n";
    conn->send(message);
}

// 接收来自主从服务器的回复信息
void SentinelInstance::recvReplyFromServer(const Angel::TcpConnectionPtr& conn,
                                           Angel::Buffer& buf)
{
    while (buf.readable() >= 2) {
        int crlf = buf.findCrlf();
        if (crlf >= 0) {
            if (strncasecmp(buf.peek(), "+PONG\r\n", 7) == 0) {
                setLastHeartBeatTime(Angel::TimeStamp::now());
            } else if (strncasecmp(buf.peek(), "+LOADING\r\n", 10) == 0) {
                setLastHeartBeatTime(Angel::TimeStamp::now());
            } else if (strncasecmp(buf.peek(), "+MASTERDOWN\r\n", 13) == 0) {
                setLastHeartBeatTime(Angel::TimeStamp::now());
            } else {
                const char *s = buf.peek();
                const char *es = s + crlf + 2;
                if (flag() & MASTER)
                    parseInfoReplyFromMaster(s, es);
                else if (flag() & SLAVE)
                    parseInfoReplyFromSlave(s, es);
                else if (flag() & SENTINEL)
                    parseReplyFromSentinel(s, es);
            }
            buf.retrieve(crlf + 2);
        } else
            break;
    }
}

void SentinelInstance::parseInfoReplyFromMaster(const char *s, const char *es)
{
    if (s[0] != '+') return;
    s += 1;
    while (1) {
        const char *p = std::find(s, es, '\n');
        if (p[-1] == '\r') p -= 1;
        if (strncasecmp(s, "run_id:", 7) == 0) {
            setRunId(std::string(s + 7, p));
        } else if (strncasecmp(s, "role:", 5) == 0) {

        } else if (strncasecmp(s, "connected_slaves:", 17) == 0) {

        } else if (strncasecmp(s, "slave", 5) == 0) {
            updateSlaves(s, es);
        }
        if (p[0] == '\r') break;
        s = p + 1;
    }
}

void SentinelInstance::parseInfoReplyFromSlave(const char *s, const char *es)
{
    if (s[0] != '+') return;
    s += 1;
    while (1) {
        const char *p = std::find(s, es, '\n');
        if (p[-1] == '\r') p -= 1;
        if (strncasecmp(s, "run_id:", 7) == 0) {
            setRunId(std::string(s + 7, p));
        } else if (strncasecmp(s, "role:", 5) == 0) {

        } else if (strncasecmp(s, "master_host:", 12) == 0) {
            setInetAddr(Angel::InetAddr(inetAddr().toIpPort(), std::string(s + 12, p).c_str()));
        } else if (strncasecmp(s, "master_port:", 12) == 0) {
            setInetAddr(Angel::InetAddr(atoi(s + 12), inetAddr().toIpAddr()));
        } else if (strncasecmp(s, "master_link_status:", 20) == 0) {

        } else if (strncasecmp(s, "slave_repl_offset:", 18) == 0) {
            setOffset(atol(s + 18));
        } else if (strncasecmp(s, "slave_priority:", 15) == 0) {

        }
        if (p[0] == '\r') break;
        s = p + 1;
    }
}

void SentinelInstance::updateSlaves(const char *s, const char *es)
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
            name.append(convert(port));
            auto it = _slaves.find(name);
            if (it == _slaves.end()) {
                logInfo("found a new slave %s", name.c_str());
                auto slave = new SentinelInstance;
                slave->setFlag(SentinelInstance::SLAVE);
                slave->setMaster(this->name());
                slave->setName(name);
                slave->setInetAddr(Angel::InetAddr(port, ip.c_str()));
                slave->setOffset(offset);
                slave->setDownAfterPeriod(downAfterPeriod());
                slave->creatCmdConnection();
                slave->creatPubConnection();
                _slaves.emplace(name, slave);
            } else {
                it->second->setOffset(offset);
            }
        }
        if (f == es) break;
        s = f + 1;
    }
}

// 处理SENTINEL is-master-down-by-addr命令的回复
void SentinelInstance::parseReplyFromSentinel(const char *s, const char *es)
{
    auto master = g_sentinel->masters().find(this->master());
    assert(master != g_sentinel->masters().end());
    auto half = (master->second->sentinels().size() + 1) / 2 + 1;

    std::vector<std::string> argv;
    parseLineWithSeparator(argv, s + 1, es, ',');
    auto& down_state = argv[0], &leader_runid = argv[1];
    uint64_t leader_epoch = atoll(argv[2].c_str());

    // 有关master是否进入主观下线状态的回复
    if (leader_runid.compare("*") == 0) {
        if (master->second->flag() & O_DOWN) return;
        // 对方同意该master进入主观下线状态
        if (down_state.compare("1") == 0) {
            master->second->votesIncr();
            // 有超过半数的sentinel同意该master进入主观下线状态，
            // 该master进入客观下线状态，并开始进行故障转移操作
            if (master->second->votes() >= half) {
                master->second->votesReset();
                master->second->setFlag(O_DOWN);
                if (!(master->second->flag() & FOLLOWER))
                    master->second->startFailover();
            }
        }
    } else if (leader_runid.compare(g_sentinel->runId()) == 0) {
        if (master->second->flag() & HAVE_LEADER) return;
        // 选举领头的回复
        if (master->second->failoverEpoch() == leader_epoch) {
            master->second->votesIncr();
            if (master->second->votes() >= half
                    && master->second->votes() >= master->second->quorum()) {
                // 当前sentinel成为领头
                master->second->votesReset();
                master->second->setFlag(HAVE_LEADER);
                master->second->noticeLeaderToOtherSentinels();
                master->second->cancelElectTimeoutTimer();
                logInfo("%s become leader", g_sentinel->runId());
            }
        }
    }
}

// 请求被其他sentinel选举为领头
void SentinelInstance::startFailover()
{
    logInfo("%s start electing", g_sentinel->runId());
    g_sentinel->currentEpochIncr();
    setLeader(g_sentinel->runId());
    setLeaderEpoch(g_sentinel->currentEpoch());
    setFailoverEpoch(g_sentinel->currentEpoch());
    votesIncr();
    setElectTimeoutTimer();
    askForSentinels(g_sentinel->runId());
}

// 设置一个随机的超时定时器
void SentinelInstance::setElectTimeoutTimer()
{
    std::srand(::clock());
    int64_t timeout = ELECT_TIMEOUT + std::rand() % 100;
    _electTimeoutTimerId = g_sentinel->loop()->runAfter(timeout,
            [this]{ this->electTimeout(); });
}

void SentinelInstance::electTimeout()
{
    startFailover();
}

void SentinelInstance::cancelElectTimeoutTimer()
{
    if (_electTimeoutTimerId > 0)
        g_sentinel->loop()->cancelTimer(_electTimeoutTimerId);
}

void Sentinel::subServer(const Angel::TcpConnectionPtr& conn)
{
    conn->send("*2\r\n$9\r\nSUBSCRIBE\r\n$18\r\n__sentinel__:hello\r\n");
}

void Sentinel::recvPubMessageFromServer(const Angel::TcpConnectionPtr& conn,
                                        Angel::Buffer& buf)
{
    Context context(nullptr, nullptr);
    while (buf.readable() >= 2) {
        int crlf = buf.findCrlf();
        if (crlf >= 0) {
            // skip subscribe's reply
            if (buf[0] == '+') {
                buf.retrieve(crlf + 2);
                continue;
            }
            Server::parseRequest(context, buf);
            if (context.state() == Context::PARSING)
                break;
            auto& message = context.commandList()[2];
            updateSentinels(message.data(), message.data() + message.size());
            buf.retrieve(crlf + 2);
        }
    }
}

void Sentinel::updateSentinels(const char *s, const char *es)
{
    std::vector<std::string> argv;
    parseLineWithSeparator(argv, s, es, ',');
    auto& s_ip = argv[0], &s_port = argv[1], &s_runid = argv[2], &s_epoch = argv[3];
    auto& m_name = argv[4], &m_ip = argv[5], &m_port = argv[6], &m_epoch = argv[7];
    UNUSED(m_ip);
    UNUSED(m_port);
    UNUSED(m_epoch);
    if (s_runid.compare(_server.dbServer().selfRunId()) == 0)
        return;
    auto s_name = s_ip + ":" + s_port;
    auto master = masters().find(m_name);
    if (master == masters().end()) return;
    auto it = master->second->sentinels().find(s_name);
    if (it == master->second->sentinels().end()) {
        logInfo("found a new sentinel %s", s_name.c_str());
        auto sentinel = new SentinelInstance;
        sentinel->setFlag(SentinelInstance::SENTINEL);
        sentinel->setMaster(m_name);
        sentinel->setName(s_name);
        sentinel->setInetAddr(Angel::InetAddr(atoi(s_port.c_str()), s_ip.c_str()));
        sentinel->setRunId(s_runid);
        sentinel->setConfigEpoch(atoll(s_epoch.c_str()));
        sentinel->setDownAfterPeriod(master->second->downAfterPeriod());
        sentinel->creatCmdConnection();
        master->second->sentinels().emplace(s_name, sentinel);
    } else {
        it->second->setConfigEpoch(atoll(s_epoch.c_str()));
    }
}

// 检查是否有服务器下线
static void checkSubjectiveDown(const std::unique_ptr<SentinelInstance>& si, int64_t now)
{
    if (si->flag() & SentinelInstance::O_DOWN) return;
    if (si->flag() & SentinelInstance::S_DOWN) return;
    if (now - si->lastHeartBeatTime() >= si->downAfterPeriod()) {
        si->setFlag(SentinelInstance::S_DOWN);
        if (si->flag() & SentinelInstance::MASTER)
            si->askMasterStateForOtherSentinels();
    }
}

void Sentinel::sentinelCron()
{
    int64_t now = Angel::TimeStamp::now();
    for (auto& master : masters()) {
        checkSubjectiveDown(master.second, now);
        for (auto& slave : master.second->slaves()) {
            checkSubjectiveDown(slave.second, now);
        }
        for (auto& sentinel : master.second->sentinels()) {
            checkSubjectiveDown(sentinel.second, now);
        }
    }
}

void Sentinel::infoCommand(Context& con)
{

}

bool SentinelInstance::isThisMaster(const std::string& ip, int port)
{
    return ip.compare(inetAddr().toIpAddr()) == 0 && port == inetAddr().toIpPort();
}

// is-master-down-by-addr <ip> <port> <epoch> <runid>
// 如果runid为*，则是询问master的状态，如果runid为0，则是告知
// 已经选出的leader，否则的话，就是在选举领头，请求投票
void Sentinel::sentinelCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    if (strcasecmp(cmdlist[1].c_str(), "is-master-down-by-addr") == 0) {
        auto& ip = cmdlist[2];
        int port = atoi(cmdlist[3].c_str());
        uint64_t epoch = atoll(cmdlist[4].c_str());
        auto& runid = cmdlist[5];
        // 回复询问的master是否进入主观下线状态
        if (runid.compare("*") == 0) {
            for (auto& master : masters()) {
                if (master.second->isThisMaster(ip, port)) {
                    con.append((master.second->flag() & SentinelInstance::S_DOWN)
                            ? "+1,*,0\r\n" : "+0,*,0\r\n");
                }
            }
        } else { // 向请求的sentinel投票
            for (auto& master : masters()) {
                if (master.second->isThisMaster(ip, port)) {
                    if (runid.compare("0") == 0) {
                        master.second->cancelElectTimeoutTimer();
                        logInfo("has already elected leader");
                        return;
                    }
                    if (g_sentinel->currentEpoch() < epoch) {
                        g_sentinel->setCurrentEpoch(epoch);
                    }
                    con.append("+0,");
                    if (master.second->leaderEpoch() < epoch && g_sentinel->currentEpoch() <= epoch) {
                        master.second->setFlag(SentinelInstance::FOLLOWER);
                        master.second->cancelElectTimeoutTimer();
                        master.second->setLeader(runid);
                        master.second->setLeaderEpoch(epoch);
                        master.second->setElectTimeoutTimer();
                        con.append(runid);
                        con.append(",");
                        con.append(cmdlist[4]);
                        con.append("\r\n");
                        logInfo("%s voted for %s", g_sentinel->runId(), runid.c_str());
                    } else { // 返回投过票的局部领头
                        con.append(master.second->leader());
                        con.append(",");
                        con.append(convert(master.second->leaderEpoch()));
                        con.append("\r\n");
                    }
                }
            }
        }
    }
}

// 将选出的领头通知给其他sentinel
void SentinelInstance::noticeLeaderToOtherSentinels()
{
    askForSentinels("0");
}
