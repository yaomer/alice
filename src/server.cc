#include <Angel/EventLoop.h>
#include <Angel/TcpServer.h>
#include <Angel/TcpClient.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/wait.h>
#include <algorithm>
#include <vector>
#include <string>
#include "server.h"
#include "db.h"

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
    s = std::find_if(s, es, [](char c){ return !isnumber(c); });
    if (s[0] != '\r' || s[1] != '\n') goto err;
    s += 2;
    // 解析各个命令
    while (argc > 0) {
        next = std::find(s, es, '\n');
        if (next == es) goto clr;
        if (s[0] != '$' || next[-1] != '\r') goto err;

        s += 1;
        len = atol(s);
        s = std::find_if(s, es, [](char c){ return !isnumber(c); });
        if (s[0] != '\r' || s[1] != '\n') goto err;

        s += 2;
        next = std::find(s, es, '\r');
        if (next == es) goto clr;
        if (next[1] != '\n' || next - s != len) goto err;
        con.addArg(s, next);
        s = next + 2;
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
    auto& cmdlist = con.commandList();
    std::transform(cmdlist[0].begin(), cmdlist[0].end(), cmdlist[0].begin(), ::toupper);
    auto it = _dbServer.db().commandMap().find(cmdlist[0]);
    if (it == _dbServer.db().commandMap().end()) {
        con.append("-ERR unknown command `" + cmdlist[0] + "`\r\n");
        goto err;
    }
    arity = it->second.arity();
    if ((arity > 0 && cmdlist.size() < arity) || (arity < 0 && cmdlist.size() != -arity)) {
        con.append("-ERR wrong number of arguments for '" + it->first + "'\r\n");
        goto err;
    }
    if (it->second.isWrite()) {
        _dbServer.dirtyIncr();
        _dbServer.appendWriteCommand(cmdlist);
        _dbServer.sendSyncCommandToSlave(cmdlist);
    }
    it->second._commandCb(con);
err:
    con.setState(Context::REPLY);
    cmdlist.clear();
}

void Server::replyResponse(const Angel::TcpConnectionPtr& conn)
{
    auto& client = std::any_cast<Context&>(conn->getContext());
    // 从服务器不应向主服务器发送回复信息
    if (!(client.flag() & Context::SLAVE))
        conn->send(client.message());
    client.message().clear();
    client.setState(Context::PARSING);
}

namespace Alice {

    // 键的空转时间不需要十分精确
    thread_local int64_t _lru_cache;
    Alice::Server *g_server;
}

void Server::serverCron()
{
    int64_t now = Angel::TimeStamp::now();
    _lru_cache = now;

    if (_dbServer.rdb()->childPid() != -1) {
        pid_t pid = waitpid(_dbServer.rdb()->childPid(), nullptr, WNOHANG);
        if (pid > 0) {
            _dbServer.rdb()->childPidReset();
            if (_dbServer.flag() & DBServer::PSYNC) {
                _dbServer.clearFlag(DBServer::PSYNC);
                _dbServer.sendRdbfileToSlave();
                _dbServer.setLastSaveTime(now);
            }
        }
    }
    if (_dbServer.aof()->childPid() != -1) {
        pid_t pid = waitpid(_dbServer.aof()->childPid(), nullptr, WNOHANG);
        if (pid > 0) {
            _dbServer.aof()->childPidReset();
            _dbServer.aof()->appendRewriteBufferToAof();
            _dbServer.setLastSaveTime(now);
        }
    }
    if (_dbServer.rdb()->childPid() == -1 && _dbServer.aof()->childPid() == -1) {
        if (_dbServer.flag() & DBServer::REWRITEAOF_DELAY) {
            _dbServer.clearFlag(DBServer::REWRITEAOF_DELAY);
            _dbServer.aof()->rewriteBackground();
        }
    }

    int saveInterval = (now - _dbServer.lastSaveTime()) / 1000;
    for (auto& it : _dbServer.saveParams()) {
        if (saveInterval >= it.seconds() && _dbServer.dirty() >= it.changes()) {
            if (_dbServer.rdb()->childPid() == -1 && _dbServer.aof()->childPid() == -1) {
                _dbServer.rdb()->saveBackground();
                _dbServer.setLastSaveTime(now);
                _dbServer.dirtyReset();
            }
            break;
        }
    }

    if (_dbServer.aof()->rewriteIsOk()) {
        if (_dbServer.rdb()->childPid() == -1 && _dbServer.aof()->childPid() == -1) {
            _dbServer.aof()->rewriteBackground();
        }
    }

    for (auto& it : _dbServer.expireMap()) {
        if (it.second <= now) {
            _dbServer.db().delKey(it.first);
            _dbServer.delExpireKey(it.first);
        }
    }

    _dbServer.aof()->appendAof(now);
}

void DBServer::connectMasterServer()
{
    if (_client) {
        _client->quit();
        _client.reset();
    }
    _client.reset(new Angel::TcpClient(g_server->loop(), *_masterAddr.get(), "slave"));
    _client->notExitFromLoop();
    _client->setConnectionCb(
            std::bind(&DBServer::sendSyncToMaster, this, _1));
    _client->setMessageCb(
            std::bind(&DBServer::recvRdbfileFromMaster, this, _1, _2));
    _client->start();
}

void DBServer::sendSyncToMaster(const Angel::TcpConnectionPtr& conn)
{
    conn->setContext(Context(this, conn));
    const char *sync = "*3\r\n$5\r\nPSYNC\r\n$1\r\n0\r\n$1\r\n0\r\n";
    conn->send(sync);
}

void DBServer::recvRdbfileFromMaster(const Angel::TcpConnectionPtr& conn, Angel::Buffer& buf)
{
    auto& con = std::any_cast<Context&>(conn->getContext());
    if (con._syncRdbFilesize == 0) {
        int crlf = buf.findStr("\r\n\r\n", 4);
        if (crlf > 0) {
            con._syncRdbFilesize = atoll(buf.peek());
            buf.retrieve(crlf + 4);
            strcpy(con.tmpfile, "tmp.XXXXX");
            mktemp(con.tmpfile);
            con._fd = open(con.tmpfile, O_RDWR | O_APPEND | O_CREAT, 0660);
            if (buf.readable() > 0)
                goto next;
        }
    } else {
next:
        size_t writeBytes = buf.readable();
        if (writeBytes >= con._syncRdbFilesize) {
            writeBytes = con._syncRdbFilesize;
            con._syncRdbFilesize = 0;
        } else {
            con._syncRdbFilesize -= writeBytes;
        }
        write(con._fd, buf.peek(), writeBytes);
        buf.retrieve(writeBytes);
        if (con._syncRdbFilesize == 0) {
            fsync(con._fd);
            close(con._fd);
            con._fd = -1;
            rename(con.tmpfile, "dump.rdb");
            rdb()->load();
            if (buf.readable() > 0)
                g_server->onMessage(conn, buf);
            auto& context = std::any_cast<Context&>(conn->getContext());
            context.setFlag(Context::SLAVE);
            conn->setMessageCb(
                    std::bind(&Server::onMessage, g_server, _1, _2));
        }
    }
}

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
                conn->second->send(rdb()->syncBuffer());
                context.clearFlag(Context::SYNC_RDB_FILE);
                context.setFlag(Context::SYNC_COMMAND);
            }
        }
    }
    rdb()->syncBuffer().c_str();
    close(fd);
}

void DBServer::sendSyncCommandToSlave(Context::CommandList& cmdlist)
{
    std::string buffer;
    buffer.append("*");
    buffer.append(convert(cmdlist.size()));
    buffer.append("\r\n");
    for (auto& it : cmdlist) {
        buffer.append("$");
        buffer.append(convert(it.size()));
        buffer.append("\r\n");
        buffer.append(it);
        buffer.append("\r\n");
    }
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

bool DBServer::isExpiredKey(const Key& key)
{
    auto it = _expireMap.find(key);
    if (it == _expireMap.end()) return false;
    int64_t now = Angel::TimeStamp::now();
    if (it->second > now) return false;
    delExpireKey(key);
    _db.delKey(key);
    Context::CommandList cmdlist = { "DEL", key };
    appendWriteCommand(cmdlist);
    return true;
}

void DBServer::delExpireKey(const Key& key)
{
    auto it = _expireMap.find(key);
    if (it != _expireMap.end())
        _expireMap.erase(it);
}

void DBServer::appendWriteCommand(Context::CommandList& cmdlist)
{
    aof()->append(cmdlist);
    if (aof()->childPid() != -1)
        aof()->appendRewriteBuffer(cmdlist);
    if (flag() & DBServer::PSYNC)
        rdb()->appendSyncBuffer(cmdlist);
}

void DBServer::appendCommand(std::string& buffer, Context::CommandList& cmdlist)
{
    size_t size = cmdlist.size();
    if (strncasecmp(cmdlist[0].c_str(), "SET", 3) == 0) {
        if (size > 3) {
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
        if (--size == 0)
            break;
    }
}

int main(int argc, char *argv[])
{
    if (argc != 2) {
        fprintf(stderr, "usage: ./serv [port]\n");
        return 1;
    }
    Angel::EventLoop loop;
    Angel::InetAddr listenAddr(atoi(argv[1]));
    Alice::Server server(&loop, listenAddr);
    g_server = &server;
    Angel::addSignal(SIGINT, []{
            g_server->dbServer().rdb()->save();
            g_server->dbServer().aof()->rewriteBackground();
            g_server->loop()->quit();
            });
    server.start();
    loop.run();
}
