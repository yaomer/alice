#include <Angel/TcpClient.h>
#include "sentinel.h"

using namespace Alice;

Sentinel::Sentinel(Angel::EventLoop *loop, Angel::InetAddr inetAddr)
    : _loop(loop),
    _server(loop, inetAddr),
    _currentEpoch(0),
    _masters(&g_sentinel_conf.masters)
{
    for (auto& db : _server.dbServer().dbs()) {
        db->commandMap() = {
            { "PING",       { -1, IS_READ, std::bind(&DB::pingCommand, db.get(), _1) } },
            { "INFO",       { -3, IS_READ, std::bind(&Sentinel::infoCommand, this, _1) } },
            { "SENTINEL",   { -3, IS_READ, std::bind(&Sentinel::sentinelCommand, this, _1) } },
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
    // _loop->runEvery(100, [this]{ this->sentinelCron(); });
}

// 对于SentinelInstance对象来说，当它调用creatCmdConnection时，
// 需要保存当前对象的this指针，如果该对象是在栈上分配的，则该对象很快就会
// 被销毁，而this指针则会失效，这样，当延后回调发生的时候，就会访问到错误
// 的内存空间，从而造成一些意想不到的结果
// 因此，这种情况下，需要new一个对象，这样才能保证在该对象被释放前this指针
// 一定是有效的，
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

static void sendPing(const std::unique_ptr<Angel::TcpClient>& cli,
                     const std::unique_ptr<SentinelInstance>& si,
                     int64_t now)
{
    if (cli->isConnected()) {
        cli->conn()->send("*1\r\n$4\r\nPING\r\n");
        if (si->lastHeartBeatTime() == 0)
            si->setLastHeartBeatTime(now);
    }
}

// 向所有被监控的主从服务器以及对应的sentinel发送PING，
// 以维持心跳连接
void Sentinel::sendPingToServers()
{
    int64_t now = Angel::TimeStamp::now();
    for (auto it = masters().begin(); it != masters().end(); ) {
        auto master = it++;
        sendPing(master->second->clients()[0], master->second, now);
        auto& slaves = master->second->slaves();
        for (auto it = slaves.begin(); it != slaves.end(); ) {
            auto slave = it++;
            if (isSubjectiveDownForSlaveOrSentinel(slaves, slave->second))
                continue;
            sendPing(slave->second->clients()[0], slave->second, now);
        }
        auto& sentinels = master->second->sentinels();
        for (auto it = sentinels.begin(); it != sentinels.end(); ) {
            auto sentinel = it++;
            if (isSubjectiveDownForSlaveOrSentinel(sentinels, sentinel->second))
                continue;
            sendPing(sentinel->second->clients()[0], sentinel->second, now);
        }
    }
}

static void sendPubMessage(const std::unique_ptr<Angel::TcpClient>& cli,
                           const std::unique_ptr<SentinelInstance>& si)
{
    std::cout << cli->isConnected() << "\n";
    if (cli->isConnected()) si->pubMessage(cli->conn());
}

// 向所有被监控的主从服务器发送频道消息
void Sentinel::sendPubMessageToServers()
{
    for (auto it = masters().begin(); it != masters().end(); ) {
        auto master = it++;
        sendPubMessage(master->second->clients()[0], master->second);
        auto& slaves = master->second->slaves();
        for (auto it = slaves.begin(); it != slaves.end(); ) {
            auto slave = it++;
            if (isSubjectiveDownForSlaveOrSentinel(slaves, slave->second))
                continue;
            sendPubMessage(slave->second->clients()[0], slave->second);
        }
    }
}

static void sendInfo(const std::unique_ptr<Angel::TcpClient>& cli)
{
    if (cli->isConnected()) cli->conn()->send("*1\r\n$4\r\nINFO\r\n");
}

// 向所有被监控的主从服务器发送INFO命令
void Sentinel::sendInfoToServers()
{
    for (auto it = masters().begin(); it != masters().end(); ) {
        auto master = it++;
        sendInfo(master->second->clients()[0]);
        auto& slaves = master->second->slaves();
        for (auto it = slaves.begin(); it != slaves.end(); ) {
            auto slave = it++;
            if (isSubjectiveDownForSlaveOrSentinel(slaves, slave->second))
                continue;
            sendInfo(master->second->clients()[0]);
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
                if (flag() & MASTER)
                    parseInfoReplyFromMaster(buf.peek(), buf.peek() + crlf + 2);
                else if (flag() & SLAVE)
                    parseInfoReplyFromSlave(buf.peek(), buf.peek() + crlf + 2);
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
                std::cout << "have a new slave " << name << "\n";
                auto slave = new SentinelInstance;
                slave->setFlag(SentinelInstance::SLAVE);
                slave->setName(name);
                slave->setInetAddr(Angel::InetAddr(port, ip.c_str()));
                slave->setOffset(offset);
                slave->setDownAfterPeriod(downAfterPeriod());
                slave->setQuorum(quorum());
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
    std::string s_name, s_ip, s_port, s_runid, s_epoch;
    std::string m_name, m_ip, m_port, m_epoch;
    int i = 0;
    while (1) {
        const char *p = std::find(s, es, ',');
        if (p == es) break;
        switch (i++) {
        case 0: s_ip.assign(s, p); break;
        case 1: s_port.assign(s, p); break;
        case 2: s_runid.assign(s, p); break;
        case 3: s_epoch.assign(s, p); break;
        case 4: m_name.assign(s, p); break;
        case 5: m_ip.assign(s, p); break;
        case 6: m_port.assign(s, p); break;
        case 7: m_epoch.assign(s, p); break;
        }
        s = p + 1;
    }
    if (s_runid.compare(_server.dbServer().selfRunId()) == 0)
        return;
    s_name = s_ip + ":" + s_port;
    auto master = masters().find(m_name);
    if (master == masters().end()) return;
    auto it = master->second->sentinels().find(s_name);
    if (it == master->second->sentinels().end()) {
        std::cout << "have a new sentinel " << s_name << "\n";
        auto sentinel = new SentinelInstance;
        sentinel->setFlag(SentinelInstance::SENTINEL);
        sentinel->setName(s_name);
        sentinel->setInetAddr(Angel::InetAddr(atoi(s_port.c_str()), s_ip.c_str()));
        sentinel->setRunId(s_runid);
        sentinel->setConfigEpoch(atoll(s_epoch.c_str()));
        sentinel->setDownAfterPeriod(master->second->downAfterPeriod());
        sentinel->setQuorum(master->second->quorum());
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

void Sentinel::sentinelCommand(Context& con)
{

}
