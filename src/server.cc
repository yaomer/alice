#include <Angel/EventLoop.h>
#include <Angel/TcpServer.h>
#include <Angel/TcpClient.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/wait.h>
#include <errno.h>
#include <algorithm>
#include <vector>
#include <string>
#include <tuple>
#include "server.h"
#include "sentinel.h"
#include "db.h"
#include "util.h"
#include "config.h"

using namespace Alice;

void Server::parseRequest(Context& con, Angel::Buffer& buf)
{
    const char *s = buf.peek();
    const char *es = s + buf.readable();
    const char *ps = s;
    size_t argc, len;
    // 解析命令个数
    const char *next = std::find(s, es, '\n');
    if (next == es) return;
    if (s[0] != '*' || next[-1] != '\r') goto err;
    s += 1;
    argc = atol(s);
    s = std::find_if_not(s, es, ::isnumber);
    if (s[0] != '\r' || s[1] != '\n') goto err;
    s += 2;
    // 解析各个命令
    while (argc > 0) {
        next = std::find(s, es, '\n');
        if (next == es) goto clr;
        if (s[0] != '$' || next[-1] != '\r') goto err;
        len = atol(s + 1);
        s = next + 1;
        if (es - s < len + 2) goto clr;
        if (s[len] != '\r' || s[len+1] != '\n') goto err;
        con.commandList().emplace_back(s, len);
        s += len + 2;
        argc--;
    }
    buf.retrieve(s - ps);
    con.setState(Context::SUCCEED);
    return;
clr:
    con.commandList().clear();
    return;
err:
    con.setState(Context::PROTOCOLERR);
}

void Server::executeCommand(Context& con)
{
    int arity;
    int64_t start, end;
    auto& cmdlist = con.commandList();
    std::transform(cmdlist[0].begin(), cmdlist[0].end(), cmdlist[0].begin(), ::toupper);
    auto it = _dbServer.db()->commandMap().find(cmdlist[0]);
    if (it == _dbServer.db()->commandMap().end()) {
        con.append("-ERR unknown command `" + cmdlist[0] + "`\r\n");
        goto err;
    }
    // 校验客户端是否有权限执行该命令
    if (!(con.perm() & it->second.perm())) {
        con.append("-ERR permission denied\r\n");
        goto err;
    }
    // 校验命令参数是否合法
    arity = it->second.arity();
    if ((arity > 0 && cmdlist.size() < arity) || (arity < 0 && cmdlist.size() != -arity)) {
        con.append("-ERR wrong number of arguments for '" + it->first + "'\r\n");
        goto err;
    }
    con.setLastcmd(it->first);
    // 执行事务时，只有遇见MULTI，EXEC，WATCH，DISCARD命令时才会直接执行，
    // 其他命令则加入事务队列
    if (con.flag() & Context::EXEC_MULTI) {
        if (cmdlist[0].compare("MULTI")
         && cmdlist[0].compare("EXEC")
         && cmdlist[0].compare("WATCH")
         && cmdlist[0].compare("DISCARD")) {
            // 检查事务中是否有写命令，如果有写命令，并且该服务器是主服务器，
            // 则进行必要的命令传播操作
            if (!(con.flag() & Context::EXEC_MULTI_WRITE) && (it->second.perm() & IS_WRITE))
                con.setFlag(Context::EXEC_MULTI_WRITE);
            con.transactionList().emplace_back(cmdlist);
            con.append("+QUEUED\r\n");
            goto end;
        }
    }
    if (it->second.perm() & IS_WRITE) {
        _dbServer.freeMemoryIfNeeded();
        _dbServer.doWriteCommand(cmdlist);
    }
    start = Angel::TimeStamp::nowUs();
    it->second._commandCb(con);
    end = Angel::TimeStamp::nowUs();
    _dbServer.addSlowlogIfNeeded(cmdlist, start, end);
    goto end;
err:
    if (con.flag() & Context::EXEC_MULTI) {
        con.clearFlag(Context::EXEC_MULTI);
        con.setFlag(Context::EXEC_MULTI_ERR);
    }
end:
    con.setState(Context::REPLY);
    cmdlist.clear();
}

void Server::replyResponse(const Angel::TcpConnectionPtr& conn)
{
    auto& context = std::any_cast<Context&>(conn->getContext());
    // 从服务器不应向主服务器发送回复信息
    if (!(context.flag() & Context::MASTER))
        conn->send(context.message());
    context.message().clear();
    context.setState(Context::PARSING);
}

void DBServer::doWriteCommand(Context::CommandList& cmdlist)
{
    dirtyIncr();
    appendWriteCommand(cmdlist);
    sendSyncCommandToSlave(cmdlist);
    // 更新从服务器的复制偏移量
    if (flag() & SLAVE) {
        std::string buffer;
        appendCommand(buffer, cmdlist, false);
        slaveOffsetIncr(buffer.size());
    }
}

namespace Alice {

    // 键的空转时间不需要十分精确
    thread_local int64_t _lru_cache = Angel::TimeStamp::now();
    Alice::Server *g_server;
    Alice::Sentinel *g_sentinel;
}

void Server::serverCron()
{
    int64_t now = Angel::TimeStamp::now();
    _lru_cache = now;

    // 服务器后台正在进行rdb持久化
    if (_dbServer.rdb()->childPid() != -1) {
        pid_t pid = waitpid(_dbServer.rdb()->childPid(), nullptr, WNOHANG);
        if (pid > 0) {
            _dbServer.rdb()->childPidReset();
            if (_dbServer.flag() & DBServer::PSYNC) {
                _dbServer.clearFlag(DBServer::PSYNC);
                _dbServer.sendRdbfileToSlave();
            }
            if (_dbServer.flag() & DBServer::PSYNC_DELAY) {
                _dbServer.clearFlag(DBServer::PSYNC_DELAY);
                _dbServer.setFlag(DBServer::PSYNC);
                _dbServer.rdb()->saveBackground();
            }
            logInfo("DB saved on disk");
        }
    }
    // 服务器后台正在进行aof持久化
    if (_dbServer.aof()->childPid() != -1) {
        pid_t pid = waitpid(_dbServer.aof()->childPid(), nullptr, WNOHANG);
        if (pid > 0) {
            _dbServer.aof()->childPidReset();
            _dbServer.aof()->appendRewriteBufferToAof();
            logInfo("Background AOF rewrite finished successfully");
        }
    }
    if (_dbServer.rdb()->childPid() == -1 && _dbServer.aof()->childPid() == -1) {
        if (_dbServer.flag() & DBServer::REWRITEAOF_DELAY) {
            _dbServer.clearFlag(DBServer::REWRITEAOF_DELAY);
            _dbServer.aof()->rewriteBackground();
        }
    }

    // 是否需要进行rdb持久化
    int saveInterval = (now - _dbServer.lastSaveTime()) / 1000;
    for (auto& it : g_server_conf.save_params) {
        int seconds = std::get<0>(it);
        int changes = std::get<1>(it);
        if (saveInterval >= seconds && _dbServer.dirty() >= changes) {
            if (_dbServer.rdb()->childPid() == -1 && _dbServer.aof()->childPid() == -1) {
                logInfo("%d changes in %d seconds. Saving...", changes, seconds);
                _dbServer.rdb()->saveBackground();
                _dbServer.setLastSaveTime(now);
                _dbServer.dirtyReset();
            }
            break;
        }
    }

    // 是否需要进行aof持久化
    if (_dbServer.aof()->rewriteIsOk()) {
        if (_dbServer.rdb()->childPid() == -1 && _dbServer.aof()->childPid() == -1) {
            _dbServer.aof()->rewriteBackground();
            _dbServer.setLastSaveTime(now);
        }
    }

    _dbServer.checkBlockedClients(now);

    _dbServer.checkExpireCycle(now);

    _dbServer.aof()->appendAof(now);
}

void DBServer::checkExpireCycle(int64_t now)
{
    int dbnums = g_server_conf.expire_check_dbnums;
    int keys = g_server_conf.expire_check_keys;
    if (dbs().size() < dbnums) dbnums = dbs().size();
    for (int i = 0; i < dbnums; i++) {
        if (_curCheckDb == dbs().size())
            _curCheckDb = 0;
        DB *db = selectDb(_curCheckDb);
        _curCheckDb++;
        if (keys > db->expireMap().size())
            keys = db->expireMap().size();
        for (int j = 0; j < keys; j++) {
            if (db->expireMap().empty()) break;
            auto randkey = getRandHashKey(db->expireMap());
            size_t bucketNumber = std::get<0>(randkey);
            size_t where = std::get<1>(randkey);
            for (auto it = db->expireMap().cbegin(bucketNumber);
                    it != db->expireMap().cend(bucketNumber); it++) {
                if (where-- == 0) {
                    if (it->second <= now) {
                        db->delKeyWithExpire(it->first);
                    }
                    break;
                }
            }
        }
    }
}

// 检查是否有阻塞的客户端超时
void DBServer::checkBlockedClients(int64_t now)
{
    std::string message;
    auto& maps = g_server->server().connectionMaps();
    for (auto it = _blockedClients.begin(); it != _blockedClients.end(); ) {
        auto e = it++;
        auto conn = maps.find(*e);
        if (conn == maps.end()) continue;
        auto& context = std::any_cast<Context&>(conn->second->getContext());
        DB *db = selectDb(context.blockDbnum());
        if (context.blockTimeout() != 0 && context.blockStartTime() + context.blockTimeout() <= now) {
            message.append("*-1\r\n+(");
            double seconds = 1.0 * (now - context.blockStartTime()) / 1000;
            message.append(convert2f(seconds));
            message.append("s)\r\n");
            conn->second->send(message);
            message.clear();
            _blockedClients.erase(e);
            db->clearBlockingKeysForContext(context);
        }
    }
}

void DBServer::removeBlockedClient(size_t id)
{
    for (auto c = _blockedClients.begin(); c != _blockedClients.end(); ++c)
        if (*c == id) break;
}

// 将目前已连接的所有客户端设置为只读
void DBServer::setSlaveToReadonly()
{
    auto& maps = g_server->server().connectionMaps();
    for (auto& it : maps) {
        if (it.second->getContext().has_value()) {
            auto& context = std::any_cast<Context&>(it.second->getContext());
            if (!(context.flag() & Context::SYNC_WAIT))
                context.clearPerm(IS_WRITE);
        }
    }
}

// 从服务器创建向主服务器的连接
void DBServer::connectMasterServer()
{
    if (_client) {
        if (_heartBeatTimerId > 0)
            g_server->loop()->cancelTimer(_heartBeatTimerId);
        _client->quit();
        _client.reset();
    }
    _client.reset(new Angel::TcpClient(g_server->loop(), *_masterAddr.get(), "slave"));
    _client->notExitLoop();
    _client->setConnectionCb(
            std::bind(&DBServer::sendPingToMaster, this, _1));
    _client->setConnectTimeoutCb([this]{
            if (!this->_client->isConnected())
                this->connectMasterServer();
            });
    _client->setMessageCb(
            std::bind(&DBServer::recvSyncFromMaster, this, _1, _2));
    _client->setCloseCb(
            std::bind(&DBServer::slaveClientCloseCb, this, _1));
    _client->start();
}

// 在主从服务器之间的连接断开时，取消向主服务器发送心跳包的定时器
void DBServer::slaveClientCloseCb(const Angel::TcpConnectionPtr& conn)
{
    g_server->loop()->cancelTimer(_heartBeatTimerId);
}

// 从服务器向主服务器定时发送PING
void DBServer::sendPingToMaster(const Angel::TcpConnectionPtr& conn)
{
    Context context(this, conn);
    context.setFlag(Context::SYNC_RECV_PING);
    conn->setContext(context);
    conn->send("*1\r\n$4\r\nPING\r\n");
    g_server->loop()->runAfter(g_server_conf.repl_timeout, [this, &context]{
            if (context.flag() & Context::SYNC_RECV_PING)
                this->connectMasterServer();
            });
}

// 从服务器向主服务器发送自己的ip和port
void DBServer::sendInetAddrToMaster(const Angel::TcpConnectionPtr& conn)
{
    std::string buffer;
    Angel::InetAddr *inetAddr = g_server->server().inetAddr();
    buffer += "*5\r\n$8\r\nreplconf\r\n$4\r\nport\r\n$";
    buffer += convert(strlen(convert(inetAddr->toIpPort())));
    buffer += "\r\n";
    buffer += convert(inetAddr->toIpPort());
    buffer += "\r\n";
    buffer += "$4\r\naddr\r\n$";
    buffer += convert(strlen(inetAddr->toIpAddr()));
    buffer += "\r\n";
    buffer += inetAddr->toIpAddr();
    buffer += "\r\n";
    conn->send(buffer);
}

// 从服务器向主服务器发送PSYNC命令
void DBServer::sendSyncToMaster(const Angel::TcpConnectionPtr& conn)
{
    auto& context = std::any_cast<Context&>(conn->getContext());
    context.setFlag(Context::SYNC_WAIT);
    if (!(_flag & SLAVE)) {
        // slave -> master: 第一次复制
        setFlag(SLAVE);
        setSlaveToReadonly();
        const char *sync = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
        conn->send(sync);
    } else {
        std::string buffer;
        buffer += "*3\r\n$5\r\nPSYNC\r\n$32\r\n";
        buffer += masterRunId();
        buffer += "\r\n$";
        buffer += convert(strlen(convert(slaveOffset())));
        buffer += "\r\n";
        buffer += convert(slaveOffset());
        buffer += "\r\n";
        conn->send(buffer);
    }
}

// +FULLRESYNC\r\n<runid>\r\n<offset>\r\n
// +CONTINUE\r\n
// <rdbfilesize>\r\n\r\n<rdbfile>
// <sync command>
void DBServer::recvSyncFromMaster(const Angel::TcpConnectionPtr& conn, Angel::Buffer& buf)
{
    auto& context = std::any_cast<Context&>(conn->getContext());
    while (true) {
        if (context.flag() & Context::SYNC_RECV_PING) {
            if (strncasecmp(buf.c_str(), "+PONG\r\n", 7) == 0) {
                buf.retrieve(7);
                context.clearFlag(Context::SYNC_RECV_PING);
                context.setFlag(Context::SYNC_WAIT);
                sendInetAddrToMaster(conn);
                sendSyncToMaster(conn);
                break;
            }
        } else if (context.flag() & Context::SYNC_WAIT) {
            if (strncasecmp(buf.c_str(), "+FULLRESYNC\r\n", 13) == 0) {
                char *s = buf.peek();
                const char *ps = s;
                s += 13;
                int crlf = buf.findStr(s, "\r\n");
                if (crlf < 0) return;
                setMasterRunId(s);
                s += crlf + 2;
                crlf = buf.findStr(s, "\r\n");
                if (crlf < 0) return;
                _slaveOffset = atoll(s);
                s += crlf + 2;
                buf.retrieve(s - ps);
                context.clearFlag(Context::SYNC_WAIT);
                context.setFlag(Context::SYNC_FULL);
            } else if (strncasecmp(buf.c_str(), "+CONTINUE\r\n", 11) == 0) {
                buf.retrieve(11);
                context.clearFlag(Context::SYNC_WAIT);
                context.setFlag(Context::MASTER | Context::SYNC_COMMAND);
                setHeartBeatTimer(conn);
            } else
                break;
        } else if (context.flag() & Context::SYNC_OK){
            context.clearFlag(Context::SYNC_OK);
            break;
        } else if (context.flag() & Context::SYNC_FULL) {
            recvRdbfileFromMaster(conn, buf);
        } else if (context.flag() & Context::SYNC_COMMAND) {
            if (strncasecmp(buf.peek(), "+PONG\r\n", 7) == 0) {
                recvPingFromMaster();
                buf.retrieve(7);
            }
            g_server->onMessage(conn, buf);
            break;
        }
    }
}

void DBServer::recvPingFromMaster()
{
    int64_t now = Angel::TimeStamp::now();
    if (lastRecvHeartBeatTime() == 0) {
        setLastRecvHeartBeatTime(now);
        return;
    }
    if (now - lastRecvHeartBeatTime() > g_server_conf.repl_timeout) {
        connectMasterServer();
    } else
        setLastRecvHeartBeatTime(now);
}

// 从服务器接受来自主服务器的rdb快照
void DBServer::recvRdbfileFromMaster(const Angel::TcpConnectionPtr& conn, Angel::Buffer& buf)
{
    auto& context = std::any_cast<Context&>(conn->getContext());
    if (_syncRdbFilesize == 0) {
        int crlf = buf.findStr("\r\n\r\n");
        if (crlf <= 0) goto jump;
        _syncRdbFilesize = atoll(buf.peek());
        buf.retrieve(crlf + 4);
        strcpy(_tmpfile, "tmp.XXXXX");
        mktemp(_tmpfile);
        _syncFd = open(_tmpfile, O_RDWR | O_APPEND | O_CREAT, 0660);
        if (buf.readable() == 0) goto jump;
        goto next;
    } else {
next:
        size_t writeBytes = buf.readable();
        if (writeBytes >= _syncRdbFilesize) {
            writeBytes = _syncRdbFilesize;
            _syncRdbFilesize = 0;
        } else {
            _syncRdbFilesize -= writeBytes;
        }
        ssize_t n = write(_syncFd, buf.peek(), writeBytes);
        if (n > 0) buf.retrieve(n);
        if (_syncRdbFilesize > 0) goto jump;
        fsync(_syncFd);
        close(_syncFd);
        _syncFd = -1;
        rename(_tmpfile, "dump.rdb");
        db()->hashMap().clear();
        rdb()->load();
        context.clearFlag(Context::SYNC_FULL);
        context.setFlag(Context::MASTER | Context::SYNC_COMMAND);
        setHeartBeatTimer(conn);
    }
    return;
jump:
    context.setFlag(Context::SYNC_OK);
}

// 主服务器将生成的rdb快照发送给从服务器
void DBServer::sendRdbfileToSlave()
{
    int fd = open("dump.rdb", O_RDONLY);
    Angel::Buffer buf;
    while (buf.readFd(fd) > 0) {
    }
    auto& maps = g_server->server().connectionMaps();
    for (auto& id : slaveIds()) {
        auto conn = maps.find(id);
        if (conn != maps.end()) {
            auto& context = std::any_cast<Context&>(conn->second->getContext());
            if (context.flag() & Context::SYNC_RDB_FILE) {
                conn->second->send(convert(buf.readable()));
                conn->second->send("\r\n\r\n");
                conn->second->send(buf.peek(), buf.readable());
                if (rdb()->syncBuffer().size() > 0)
                    conn->second->send(rdb()->syncBuffer());
                context.clearFlag(Context::SYNC_RDB_FILE);
                context.setFlag(Context::SYNC_COMMAND);
            }
        }
    }
    rdb()->syncBuffer().clear();
    close(fd);
}

// 主服务器将写命令传播给所有从服务器
void DBServer::sendSyncCommandToSlave(Context::CommandList& cmdlist)
{
    std::string buffer;
    appendCommand(buffer, cmdlist, false);
    auto& maps = g_server->server().connectionMaps();
    for (auto& id : slaveIds()) {
        auto conn = maps.find(id);
        if (conn != maps.end()) {
            auto& context = std::any_cast<Context&>(conn->second->getContext());
            if (context.flag() & Context::SYNC_COMMAND)
                conn->second->send(buffer);
        }
    }
}

// 从服务器定时向主服务器发送ACK
void DBServer::sendAckToMaster(const Angel::TcpConnectionPtr& conn)
{
    std::string buffer;
    buffer += "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$";
    buffer += convert(strlen(convert(slaveOffset())));
    buffer += "\r\n";
    buffer += convert(slaveOffset());
    buffer += "\r\n";
    conn->send(buffer);
}

// 主服务器将命令写入到复制积压缓冲区中
void DBServer::appendCopyBacklogBuffer(Context::CommandList& cmdlist)
{
    std::string buffer;
    appendCommand(buffer, cmdlist, false);
    masterOffsetIncr(buffer.size());
    size_t remainBytes = g_server_conf.repl_backlog_size - _copyBacklogBuffer.size();
    if (remainBytes < buffer.size()) {
        size_t popsize = buffer.size() - remainBytes;
        _copyBacklogBuffer.erase(0, popsize);
    }
    _copyBacklogBuffer.append(buffer);
}

void DBServer::appendWriteCommand(Context::CommandList& cmdlist)
{
    if (g_server_conf.enable_appendonly)
        aof()->append(cmdlist);
    if (aof()->childPid() != -1)
        aof()->appendRewriteBuffer(cmdlist);
    if (flag() & DBServer::PSYNC)
        rdb()->appendSyncBuffer(cmdlist);
    if (!slaveIds().empty())
        appendCopyBacklogBuffer(cmdlist);
}

// 将解析后的命令转换成redis协议形式的命令
void DBServer::appendCommand(std::string& buffer, Context::CommandList& cmdlist,
        bool repl_set)
{
    size_t size = cmdlist.size();
    bool set6 = false;
    if (repl_set && strncasecmp(cmdlist[0].c_str(), "SET", 3) == 0) {
        if (size == 5 || size == 6) {
            if (size == 6) set6 = true;
            buffer += "*3\r\n$7\r\nPEXPIRE\r\n$";
            buffer += convert(cmdlist[1].size());
            buffer += "\r\n";
            buffer += cmdlist[1] + "\r\n$";
            int64_t milliseconds = atoll(cmdlist[4].c_str()) + Angel::TimeStamp::now();
            buffer += convert(strlen(convert(milliseconds)));
            buffer += "\r\n";
            buffer += convert(milliseconds);
            buffer += "\r\n";
            size -= 2;
        }
    }
    buffer += "*";
    buffer += convert(size);
    buffer += "\r\n";
    for (auto& it : cmdlist) {
        buffer += "$";
        buffer += convert(it.size());
        buffer += "\r\n";
        buffer += it;
        buffer += "\r\n";
        if (--size == 1 && set6) {
            buffer += "$";
            buffer += convert(cmdlist[5].size());
            buffer += "\r\n";
            buffer += cmdlist[5];
            buffer += "\r\n";
            break;
        }
        if (size == 0)
            break;
    }
}

void DBServer::setHeartBeatTimer(const Angel::TcpConnectionPtr& conn)
{
    _heartBeatTimerId = g_server->loop()->runEvery(g_server_conf.repl_ping_period, [this, conn]{
            this->sendAckToMaster(conn);
            conn->send("*1\r\n$4\r\nPING\r\n");
            });
}

void DBServer::subChannel(const Key& key, size_t id)
{
    auto channel = _pubsubChannels.find(key);
    if (channel == _pubsubChannels.end()) {
        std::vector<size_t> idlist = { id };
        _pubsubChannels[key] = std::move(idlist);
    } else {
        channel->second.push_back(id);
    }
}

size_t DBServer::pubMessage(const std::string& msg, const std::string& channel, size_t id)
{
    auto idlist = _pubsubChannels.find(channel);
    if (idlist == _pubsubChannels.end()) return 0;
    std::string buffer;
    size_t pubClients = 0;
    auto& maps = g_server->server().connectionMaps();
    for (auto& id : idlist->second) {
        auto conn = maps.find(id);
        if (conn != maps.end()) {
            buffer.append("*3\r\n$7\r\nmessage\r\n$");
            buffer.append(convert(channel.size()));
            buffer.append("\r\n" + channel + "\r\n$");
            buffer.append(convert(msg.size()));
            buffer.append("\r\n");
            conn->second->send(buffer);
            conn->second->send(msg);
            conn->second->send("\r\n");
            buffer.clear();
            pubClients++;
        }
    }
    return pubClients;
}

void DBServer::addSlowlogIfNeeded(Context::CommandList& cmdlist, int64_t start, int64_t end)
{
    int64_t duration = end - start;
    if (g_server_conf.slowlog_max_len == 0) return;
    if (duration < g_server_conf.slowlog_log_slower_than) return;
    Slowlog slowlog;
    slowlog._id = _slowlogId++;
    slowlog._time = start;
    slowlog._duration = duration;
    for (auto& it : cmdlist)
        slowlog._args.emplace_back(it);
    if (slowlogQueue().size() >= g_server_conf.slowlog_max_len)
        slowlogQueue().pop_front();
    slowlogQueue().emplace_back(std::move(slowlog));
}

int main(int argc, char *argv[])
{
    Angel::setLoggerLevel(Angel::Logger::INFO);
    Alice::readServerConf();
    if (argv[1] && strcasecmp(argv[1], "--sentinel") == 0) {
        Alice::readSentinelConf();
        Angel::EventLoop loop;
        Angel::InetAddr listenAddr(g_sentinel_conf.port, g_sentinel_conf.addr.c_str());
        Sentinel sentinel(&loop, listenAddr);
        g_sentinel = &sentinel;
        sentinel.start();
        loop.run();
        return 0;
    }
    Angel::EventLoop loop;
    Angel::InetAddr listenAddr(g_server_conf.port, g_server_conf.addr.c_str());
    Alice::Server server(&loop, listenAddr);
    g_server = &server;
    loop.runEvery(100, []{ g_server->serverCron(); });
    if (g_server_conf.enable_appendonly)
        server.dbServer().aof()->load();
    else
        server.dbServer().rdb()->load();
    Angel::addSignal(SIGINT, []{
            g_server->dbServer().rdb()->save();
            g_server->dbServer().aof()->rewriteBackground();
            g_server->loop()->quit();
            });
    server.start();
    loop.run();
}
