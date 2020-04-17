#include <fcntl.h>
#include <sys/wait.h>
#include <errno.h>
#include <getopt.h>
#include <algorithm>
#include <vector>
#include <tuple>

#include "server.h"
#include "sentinel.h"

using namespace Alice;

// if parse error, return -1
// if not enough data, return 0
// else return length of request
ssize_t Server::parseRequest(Context& con, Angel::Buffer& buf)
{
    const char *s = buf.peek();
    const char *es = s + buf.readable();
    const char *ps = s;
    size_t argc, len;
    // 解析命令个数
    const char *next = std::find(s, es, '\n');
    if (next == es) goto clr;
    if (s[0] != '*' || next[-1] != '\r') goto err;
    argc = atol(s + 1);
    s = next + 1;
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
    return s - ps;
clr:
    con.commandList().clear();
    return 0;
err:
    return -1;
}

void Server::executeCommand(Context& con, const char *query, size_t len)
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
    if (it->second.perm() & IS_WRITE) _dbServer.freeMemoryIfNeeded();
    start = Angel::nowUs();
    it->second._commandCb(con);
    end = Angel::nowUs();
    _dbServer.addSlowlogIfNeeded(cmdlist, start, end);
    // 命令执行未出错
    if ((it->second.perm() & IS_WRITE) && (con.message()[0] != '-')) {
        _dbServer.doWriteCommand(cmdlist, query, len);
    }
    goto end;
err:
    if (con.flag() & Context::EXEC_MULTI) {
        con.clearFlag(Context::EXEC_MULTI);
        con.setFlag(Context::EXEC_MULTI_ERR);
    }
end:
    cmdlist.clear();
}

void Server::replyResponse(const Angel::TcpConnectionPtr& conn)
{
    auto& context = std::any_cast<Context&>(conn->getContext());
    // 从服务器不应向主服务器发送回复信息
    if (!(context.flag() & Context::MASTER))
        conn->send(context.message());
    context.message().clear();
}

// [query, len]是RESP形式的请求字符串，如果query为真，则使用[query, len]，这样可以避免许多低效的拷贝；
// 如果query为假，则使用cmdlist，然后将其转换为RESP形式的字符串，不过这种情况很少见
void DBServer::doWriteCommand(const Context::CommandList& cmdlist,
                              const char *query, size_t len)
{
    dirtyIncr();
    appendWriteCommand(cmdlist, query, len);
    if (!_slaves.empty()) {
        if (!query) {
            std::string buffer;
            CONVERT2RESP(buffer, cmdlist);
            sendSyncCommandToSlave(buffer.data(), buffer.size());
        } else
            sendSyncCommandToSlave(query, len);
    }
    if (flag() & SLAVE) {
        slaveOffsetIncr(len);
    }
}

namespace Alice {

    // 键的空转时间不需要十分精确
    thread_local int64_t _lru_cache = Angel::nowMs();
    Alice::Server *g_server;
    Alice::Sentinel *g_sentinel;
}

void Server::serverCron()
{
    int64_t now = Angel::nowMs();
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

    // 如果slave与master意外断开连接，则slave会尝试重连master
    if (_dbServer.flag() & DBServer::CONNECT_RESET_BY_MASTER) {
        _dbServer.clearFlag(DBServer::CONNECT_RESET_BY_MASTER);
        _dbServer.connectMasterServer();
    }

    _dbServer.checkBlockedClients(now);

    _dbServer.checkExpireCycle(now);

    _dbServer.aof()->appendAof(now);
}

// 随机删除一定数量的过期键
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
    for (auto it = _blockedClients.begin(); it != _blockedClients.end(); ) {
        auto e = it++;
        auto conn = g_server->server().getConnection(*e);
        if (!conn) continue;
        auto& context = std::any_cast<Context&>(conn->getContext());
        DB *db = selectDb(context.blockDbnum());
        if (context.blockTimeout() != 0 && context.blockStartTime() + context.blockTimeout() <= now) {
            message.append("*-1\r\n+(");
            double seconds = 1.0 * (now - context.blockStartTime()) / 1000;
            message.append(convert2f(seconds));
            message.append("s)\r\n");
            conn->send(message);
            message.clear();
            _blockedClients.erase(e);
            db->clearBlockingKeysForContext(context);
        }
    }
}

void DBServer::removeBlockedClient(size_t id)
{
    for (auto c : _blockedClients)
        if (c == id)
            break;
}

// 将目前已连接的所有客户端设置为只读
void DBServer::setAllSlavesToReadonly()
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
    disconnectMasterServer();
    _client.reset(new Angel::TcpClient(g_server->loop(), *_masterAddr.get()));
    _client->notExitLoop();
    _client->setConnectionCb(
            std::bind(&DBServer::sendPingToMaster, this, _1));
    _client->setMessageCb(
            std::bind(&DBServer::recvSyncFromMaster, this, _1, _2));
    _client->setCloseCb(
            std::bind(&DBServer::slaveClientCloseCb, this, _1));
    _client->start();
}

void DBServer::disconnectMasterServer()
{
    if (_client) {
        if (_heartBeatTimerId > 0)
            g_server->loop()->cancelTimer(_heartBeatTimerId);
        if (_replTimeoutTimerId > 0)
            g_server->loop()->cancelTimer(_replTimeoutTimerId);
        _client.reset();
    }
}

void DBServer::slaveClientCloseCb(const Angel::TcpConnectionPtr& conn)
{
    g_server->loop()->cancelTimer(_heartBeatTimerId);
    g_server->loop()->cancelTimer(_replTimeoutTimerId);
    _flag |= CONNECT_RESET_BY_MASTER;
}

void DBServer::sendPingToMaster(const Angel::TcpConnectionPtr& conn)
{
    Context context(this, conn.get());
    context.setFlag(Context::SYNC_RECV_PING);
    conn->setContext(context);
    conn->send("*1\r\n$4\r\nPING\r\n");
    // 从服务器向主服务器发送完PING之后，如果repl_timeout时间后没有收到
    // 有效回复，就会认为此次复制失败，然后会重连主服务器
    _replTimeoutTimerId = g_server->loop()->runAfter(g_server_conf.repl_timeout,
            [this, &context]{
            if (context.flag() & Context::SYNC_RECV_PING)
                this->connectMasterServer();
            });
}

// 从服务器向主服务器发送自己的ip和port
void DBServer::sendInetAddrToMaster(const Angel::TcpConnectionPtr& conn)
{
    std::string buffer;
    Angel::InetAddr *inetAddr = g_server->server().listenAddr();
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
        setAllSlavesToReadonly();
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
// SYNC_RECV_PING -> SYNC_WAIT -> SYNC_FULL -> SYNC_COMMAND
//                             -> SYNC_COMMAND
void DBServer::recvSyncFromMaster(const Angel::TcpConnectionPtr& conn, Angel::Buffer& buf)
{
    auto& context = std::any_cast<Context&>(conn->getContext());
    while (true) {
        if (context.flag() & Context::SYNC_RECV_PING) {
            if (!buf.strcasecmp("+PONG\r\n")) return;
            buf.retrieve(7);
            context.clearFlag(Context::SYNC_RECV_PING);
            context.setFlag(Context::SYNC_WAIT);
            sendInetAddrToMaster(conn);
            sendSyncToMaster(conn);
        } else if (context.flag() & Context::SYNC_WAIT) {
            if (buf.strcasecmp("+FULLRESYNC\r\n")) {
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
            } else if (buf.strcasecmp("+CONTINUE\r\n")) {
                buf.retrieve(11);
                context.clearFlag(Context::SYNC_WAIT);
                context.setFlag(Context::MASTER | Context::SYNC_COMMAND);
                _client->conn()->setMessageCb(
                        std::bind(&Server::slaveOnMessage, g_server, _1, _2));
                setHeartBeatTimer(conn);
            } else
                break;
        } else if (context.flag() & Context::SYNC_FULL) {
            recvRdbfileFromMaster(conn, buf);
            break;
        } else if (context.flag() & Context::SYNC_COMMAND) {
            g_server->slaveOnMessage(conn, buf);
            break;
        }
    }
}

void DBServer::recvPingFromMaster()
{
    int64_t now = Angel::nowMs();
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
        if (crlf <= 0) return;
        _syncRdbFilesize = atoll(buf.peek());
        buf.retrieve(crlf + 4);
        strcpy(_tmpfile, "tmp.XXXXX");
        mktemp(_tmpfile);
        _syncFd = open(_tmpfile, O_RDWR | O_APPEND | O_CREAT, 0660);
        if (buf.readable() == 0) return;
    }
    size_t writeBytes = buf.readable();
    if (writeBytes >= _syncRdbFilesize) {
        writeBytes = _syncRdbFilesize;
        _syncRdbFilesize = 0;
    } else {
        _syncRdbFilesize -= writeBytes;
    }
    ssize_t n = write(_syncFd, buf.peek(), writeBytes);
    if (n > 0) buf.retrieve(n);
    if (_syncRdbFilesize > 0) return;
    g_server->fsyncBackground(_syncFd);
    rename(_tmpfile, g_server_conf.rdb_file.c_str());
    clear();
    rdb()->load();
    context.clearFlag(Context::SYNC_FULL);
    context.setFlag(Context::MASTER | Context::SYNC_COMMAND);
    _client->conn()->setMessageCb(
            std::bind(&Server::slaveOnMessage, g_server, _1, _2));
    setHeartBeatTimer(conn);
}

// 主服务器将生成的rdb快照发送给从服务器
void DBServer::sendRdbfileToSlave()
{
    int fd = open(g_server_conf.rdb_file.c_str(), O_RDONLY);
    if (fd < 0) {
        logError("can't open %s:%s", g_server_conf.rdb_file.c_str(), Angel::strerrno());
        return;
    }
    Angel::Buffer buf;
    while (buf.readFd(fd) > 0) {
    }
    for (auto& it : slaves()) {
        auto conn = g_server->server().getConnection(it.first);
        if (!conn) continue;
        auto& context = std::any_cast<Context&>(conn->getContext());
        if (context.flag() & Context::SYNC_RDB_FILE) {
            conn->send(convert(buf.readable()));
            conn->send("\r\n\r\n");
            conn->send(buf.peek(), buf.readable());
            if (rdb()->syncBuffer().size() > 0)
                conn->send(rdb()->syncBuffer());
            context.clearFlag(Context::SYNC_RDB_FILE);
            context.setFlag(Context::SYNC_COMMAND);
        }
    }
    rdb()->syncBuffer().clear();
    close(fd);
}

// 主服务器将写命令传播给所有从服务器
void DBServer::sendSyncCommandToSlave(const char *query, size_t len)
{
    for (auto& it : _slaves) {
        auto conn = g_server->server().getConnection(it.first);
        if (!conn) continue;
        auto& context = std::any_cast<Context&>(conn->getContext());
        if (context.flag() & Context::SYNC_COMMAND)
            conn->send(query, len);
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
void DBServer::appendCopyBacklogBuffer(const char *query, size_t len)
{
    masterOffsetIncr(len);
    _copyBacklogBuffer.put(query, len);
}

void DBServer::appendPartialResyncData(Context& con, size_t off)
{
    std::string buffer;
    buffer.reserve(off);
    _copyBacklogBuffer.get(buffer.data(), off);
    con.append(std::move(buffer));
}

void DBServer::appendWriteCommand(const Context::CommandList& cmdlist,
                                  const char *query, size_t len)
{
    if (g_server_conf.enable_appendonly)
        aof()->append(cmdlist, query, len);
    if (aof()->childPid() != -1)
        aof()->appendRewriteBuffer(cmdlist, query, len);
    if (flag() & DBServer::PSYNC)
        rdb()->appendSyncBuffer(cmdlist, query, len);
    if (!slaves().empty()) {
        if (!query) {
            std::string buffer;
            CONVERT2RESP(buffer, cmdlist);
            appendCopyBacklogBuffer(buffer.data(), buffer.size());
        } else
            appendCopyBacklogBuffer(query, len);
    }
}

static void appendCommandHead(std::string& buffer, size_t len)
{
    buffer += "*";
    buffer += convert(len);
    buffer += "\r\n";
}

static void appendCommandArg(std::string& buffer, const std::string& s1,
        const std::string& s2)
{
    buffer += "$";
    buffer += s1 + "\r\n" + s2 + "\r\n";
}

static void appendTimeStamp(std::string& buffer, int64_t timeval, bool is_seconds)
{
    if (is_seconds) timeval *= 1000;
    int64_t milliseconds = timeval + Angel::nowMs();
    appendCommandArg(buffer, convert(strlen(convert(milliseconds))), convert(milliseconds));
}

// 将解析后的命令转换成RESP形式的命令
void DBServer::CONVERT2RESP(std::string& buffer, const Context::CommandList& cmdlist)
{
    appendCommandHead(buffer, cmdlist.size());
    for (auto& it : cmdlist) {
        appendCommandArg(buffer, convert(it.size()), it);
    }
}

// 将解析后的命令转换成RESP形式的命令
// EXPIRE命令将被转换为PEXPIRE
void DBServer::CONVERT2RESP(std::string& buffer, const Context::CommandList& cmdlist,
                             const char *query, size_t len)
{
    size_t size = cmdlist.size();
    if (strcasecmp(cmdlist[0].c_str(), "SET") == 0 && size >= 5) {
        appendCommandHead(buffer, size);
        for (size_t i = 0; i < size; i++) {
            if (i > 3 && strcasecmp(cmdlist[i-1].c_str(), "EX") == 0) {
                appendTimeStamp(buffer, atoll(cmdlist[i].c_str()), true);
            } else if (i > 3 && strcasecmp(cmdlist[i-1].c_str(), "PX") == 0) {
                appendTimeStamp(buffer, atoll(cmdlist[i].c_str()), false);
            } else {
                appendCommandArg(buffer, convert(cmdlist[i].size()), cmdlist[i]);
            }
        }
    } else if (strcasecmp(cmdlist[0].c_str(), "EXPIRE") == 0) {
        appendCommandHead(buffer, size);
        appendCommandArg(buffer, "7", "PEXPIRE");
        appendCommandArg(buffer, convert(cmdlist[1].size()), cmdlist[1]);
        appendTimeStamp(buffer, atoll(cmdlist[2].c_str()), true);
    } else if (strcasecmp(cmdlist[0].c_str(), "PEXPIRE") == 0) {
        appendCommandHead(buffer, size);
        appendCommandArg(buffer, "7", "PEXPIRE");
        appendCommandArg(buffer, convert(cmdlist[1].size()), cmdlist[1]);
        appendTimeStamp(buffer, atoll(cmdlist[2].c_str()), false);
    } else {
        if (!query) CONVERT2RESP(buffer, cmdlist);
        else buffer.append(query, len);
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
    for (auto& id : idlist->second) {
        auto conn = g_server->server().getConnection(id);
        if (!conn) continue;
        buffer.append("*3\r\n$7\r\nmessage\r\n$");
        buffer.append(convert(channel.size()));
        buffer.append("\r\n" + channel + "\r\n$");
        buffer.append(convert(msg.size()));
        buffer.append("\r\n");
        conn->send(buffer);
        conn->send(msg);
        conn->send("\r\n");
        buffer.clear();
        pubClients++;
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

void DBServer::clear()
{
    for (auto& db : dbs()) {
        db->hashMap().clear();
        db->expireMap().clear();
    }
}

void Server::fsyncBackground(int fd)
{
    _server.executor([fd]{ ::fsync(fd); ::close(fd); });
}

void Server::start()
{
    _loop->runEvery(100, []{ g_server->serverCron(); });
    Angel::addSignal(SIGINT, []{
            g_server->dbServer().rdb()->saveBackground();
            g_server->dbServer().aof()->rewriteBackground();
            g_server->loop()->quit();
            });
    // 优先使用AOF文件来载入数据
    if (fileExists(g_server_conf.appendonly_file.c_str()))
        _dbServer.aof()->load();
    else
        _dbServer.rdb()->load();
    // 服务器以从服务器方式运行
    if (g_server_conf.master_port > 0) {
        Context con(nullptr, nullptr);
        auto& cmdlist = con.commandList();
        cmdlist.emplace_back("SLAVEOF");
        cmdlist.emplace_back(g_server_conf.master_ip);
        cmdlist.emplace_back(convert(g_server_conf.master_port));
        executeCommand(con, nullptr, 0);
    }
    _server.start();
}

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
    abort();
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
        case 'h': help();
        }
    }

    Angel::setLoggerLevel(Angel::Logger::INFO);
    Alice::readServerConf(server_conf_file.c_str());
    if (startup_sentinel) {
        Alice::readSentinelConf(sentinel_conf_file.c_str());
        Angel::EventLoop loop;
        Angel::InetAddr listenAddr(g_sentinel_conf.port, g_sentinel_conf.addr.c_str());
        Sentinel sentinel(&loop, listenAddr);
        logInfo("sentinel %s:%d runId is %s", g_sentinel_conf.addr.c_str(),
                g_sentinel_conf.port, sentinel.server().dbServer().selfRunId());
        g_sentinel = &sentinel;
        sentinel.start();
        loop.run();
        return 0;
    }
    Angel::EventLoop loop;
    Angel::InetAddr listenAddr(g_server_conf.port, g_server_conf.addr.c_str());
    Alice::Server server(&loop, listenAddr);
    logInfo("server %s:%d runId is %s", g_server_conf.addr.c_str(),
            g_server_conf.port, server.dbServer().selfRunId());
    g_server = &server;
    server.start();
    loop.run();
}
