#include <Angel/TcpClient.h>
#include "sentinel.h"

using namespace Alice;

Sentinel::Sentinel(Angel::EventLoop *loop, Angel::InetAddr inetAddr)
    : _loop(loop),
    _server(loop, inetAddr),
    _currentEpoch(0),
    _masters(&g_sentinel_conf.masters)
{
    /* auto& db = _server.dbServer().db();
    db.commandMap() = {
        { "PING",       { -1, IS_READ, std::bind(&DB::pingCommand, &db, _1) } },
        { "INFO",       { -3, IS_READ, std::bind(&Sentinel::infoCommand, this, _1) } },
        { "SENTINEL",   { -3, IS_READ, std::bind(&Sentinel::sentinelCommand, this, _1) } },
    }; */
}

void Sentinel::init()
{
    for (auto& it : *_masters) {
        it.second.creatCmdConnection();
        it.second.creatPubConnection();
    }
    _loop->runEvery(1000, [this]{ this->sendPingToMasters(); });
    _loop->runEvery(1000 * 2, [this]{ this->sendPubMessageToMasters(); });
    _loop->runEvery(1000 * 10, [this]{ this->sendInfoToMasters(); });
}

void SentinelInstance::creatCmdConnection()
{
    _clients[0].reset(
            new Angel::TcpClient(g_sentinel->loop(), *inetAddr(), "command"));
    _clients[0]->setMessageCb(
            std::bind(&SentinelInstance::recvReplyFromMaster, this, _1, _2));
    _clients[0]->setCloseCb(
            std::bind(&SentinelInstance::closeConnection, this, _1));
    _clients[0]->notExitLoop();
    _clients[0]->start();
}

void SentinelInstance::creatPubConnection()
{
    _clients[1].reset(
            new Angel::TcpClient(g_sentinel->loop(), *inetAddr(), "pubsub"));
    _clients[1]->setConnectionCb(
            std::bind(&SentinelInstance::subMaster, this, _1));
    _clients[1]->setMessageCb(
            std::bind(&SentinelInstance::recvSubMessageFromMaster, this, _1, _2));
    _clients[1]->notExitLoop();
    _clients[1]->start();
}

void SentinelInstance::closeConnection(const Angel::TcpConnectionPtr& conn)
{
    this->setFlag(CLOSED);
}

void Sentinel::sendPingToMasters()
{
    for (auto& master : masters()) {
        if (master.second.flag() & SentinelInstance::CLOSED) {
            masters().erase(master.second.name());
            continue;
        }
        auto& client = master.second.clients()[0];
        if (client->isConnected()) {
            client->conn()->send("*1\r\n$4\r\nPING\r\n");
        }
        for (auto& slave : master.second.slaves()) {
            if (slave.second.flag() & SentinelInstance::CLOSED) {
                master.second.slaves().erase(slave.first);
                continue;
            }
            auto& client = slave.second.clients()[0];
            if (client->isConnected()) {
                client->conn()->send("*1\r\n$4\r\nPING\r\n");
            }
        }
        for (auto& sentinel : master.second.sentinels()) {
            if (sentinel.second.flag() & SentinelInstance::CLOSED) {
                master.second.sentinels().erase(sentinel.first);
                continue;
            }
            auto& client = sentinel.second.clients()[0];
            if (client->isConnected()) {
                client->conn()->send("*1\r\n$4\r\nPING\r\n");
            }
        }
    }
}

void Sentinel::sendPubMessageToMasters()
{
    for (auto& master : masters()) {
        if (master.second.flag() & SentinelInstance::CLOSED) {
            masters().erase(master.second.name());
            continue;
        }
        auto& client = master.second.clients()[0];
        if (client->isConnected()) {
            master.second.pubMessage(client->conn());
        }
        for (auto& slave : master.second.slaves()) {
            if (slave.second.flag() & SentinelInstance::CLOSED) {
                master.second.slaves().erase(slave.first);
                continue;
            }
            auto& client = slave.second.clients()[0];
            if (client->isConnected()) {
                client->conn()->send("*1\r\n$4\r\nPING\r\n");
            }
        }
    }
}

void Sentinel::sendInfoToMasters()
{
    for (auto& master : masters()) {
        if (master.second.flag() & SentinelInstance::CLOSED) {
            masters().erase(master.second.name());
            continue;
        }
        auto& client = master.second.clients()[0];
        if (client->isConnected()) {
            client->conn()->send("*1\r\n$4\r\nINFO\r\n");
        }
        for (auto& slave : master.second.slaves()) {
            if (slave.second.flag() & SentinelInstance::CLOSED) {
                master.second.slaves().erase(slave.first);
                continue;
            }
            auto& client = slave.second.clients()[0];
            if (client->isConnected()) {
                client->conn()->send("*1\r\n$4\r\nPING\r\n");
            }
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
    buffer += inetAddr()->toIpAddr();
    buffer += ",";
    buffer += convert(inetAddr()->toIpPort());
    buffer += ",";
    buffer += convert(configEpoch());
    message += convert(buffer.size());
    message += "\r\n";
    message += buffer;
    message += "\r\n";
    conn->send(message);
}

// <run_id> <role> <connected_slaves> <slave:ip=,port=,offset=>
void SentinelInstance::recvReplyFromMaster(const Angel::TcpConnectionPtr& conn,
                                           Angel::Buffer& buf)
{
    while (buf.readable() >= 2) {
        int crlf = buf.findCrlf();
        if (crlf >= 0) {
            parseInfoReply(buf.peek(), buf.peek() + crlf + 2);
            buf.retrieve(crlf + 2);
        } else
            break;
    }
}

void SentinelInstance::parseInfoReply(const char *s, const char *es)
{
    if (s[0] != '+') return;
    s += 1;
    while (1) {
        const char *p = std::find(s, es, '\n');
        if (p[-1] == '\r') p -= 1;
        if (strncasecmp(s, "run_id:", 7) == 0) {

        } else if (strncasecmp(s, "role:", 5) == 0) {

        } else if (strncasecmp(s, "connected_slaves:", 17) == 0) {

        } else if (strncasecmp(s, "slave", 5) == 0) {
            s = std::find(s, p, ':') + 1;
            std::string ip, slaveName;
            int port;
            while (1) {
                const char *f = std::find(s, p, ',');
                if (strncasecmp(s, "ip=", 3) == 0) {
                    ip.assign(s + 3, f - s - 3);
                    slaveName += ip + ":";
                } else if (strncasecmp(s, "port=", 5) == 0) {
                    port = atoi(s + 5);
                    slaveName.append(s + 5, f - s - 5);
                } else if (strncasecmp(s, "offset=", 7) == 0) {
                    size_t offset = atoll(s + 7);
                    auto it = _slaves.find(slaveName);
                    if (it == _slaves.end()) {
                        std::cout << "have a new slave " << slaveName << "\n";
                        SentinelInstance slave;
                        slave.setFlag(SentinelInstance::SLAVE);
                        slave.setName(slaveName);
                        slave.setInetAddr(Angel::InetAddr(port, ip.c_str()));
                        slave.setOffset(offset);
                        slave.creatCmdConnection();
                        slave.creatPubConnection();
                        _slaves[slaveName] = std::move(slave);
                    } else {
                        it->second.setOffset(offset);
                    }
                }
                if (f == p) break;
                s = f + 1;
            }
        }
        if (p[0] == '\r') break;
        s = p + 1;
    }
}

void SentinelInstance::subMaster(const Angel::TcpConnectionPtr& conn)
{
    conn->send("*2\r\n$9\r\nSUBSCRIBE\r\n$18\r\n__sentinel__:hello\r\n");
}

void SentinelInstance::recvSubMessageFromMaster(const Angel::TcpConnectionPtr& conn,
                                                Angel::Buffer& buf)
{
    Context context(nullptr, nullptr);
    while (buf.readable() >= 2) {
        int crlf = buf.findCrlf();
        if (crlf >= 0) {
            if (buf[0] == '+') {
                buf.retrieve(crlf + 2);
                continue;
            }
            Server::parseRequest(context, buf);
            if (context.state() == Context::PARSING)
                break;
            auto& message = context.commandList()[2];
            g_sentinel->updateSentinels(message.data(), message.data() + message.size());
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
        case 0: s_ip.assign(s, p - s); break;
        case 1: s_port.assign(s, p - s); break;
        case 2: s_runid.assign(s, p - s); break;
        case 3: s_epoch.assign(s, p - s); break;
        case 4: m_name.assign(s, p - s); break;
        case 5: m_ip.assign(s, p - s); break;
        case 6: m_port.assign(s, p - s); break;
        case 7: m_epoch.assign(s, p - s); break;
        }
        s = p + 1;
    }
    if (s_runid.compare(_server.dbServer().selfRunId()) == 0)
        return;
    s_name = s_ip + ":" + s_port;
    auto master = masters().find(m_name);
    if (master == masters().end()) return;
    auto it = master->second.sentinels().find(s_name);
    if (it == master->second.sentinels().end()) {
        std::cout << "have a new sentinel " << s_name << "\n";
        SentinelInstance sentinel;
        sentinel.setFlag(SentinelInstance::SENTINEL);
        sentinel.setName(s_name);
        sentinel.setInetAddr(Angel::InetAddr(atoi(s_port.c_str()), s_ip.c_str()));
        sentinel.setRunId(s_runid);
        sentinel.setConfigEpoch(atoll(s_epoch.c_str()));
        sentinel.creatCmdConnection();
        master->second.sentinels()[s_name] = std::move(sentinel);
    } else {
        it->second.setConfigEpoch(atoll(s_epoch.c_str()));
    }
}

void Sentinel::infoCommand(Context& con)
{

}

void Sentinel::sentinelCommand(Context& con)
{

}
