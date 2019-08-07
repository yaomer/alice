#include <Angel/EventLoop.h>
#include <Angel/TcpServer.h>
#include <Angel/TcpClient.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/wait.h>
#include <time.h>
#include <algorithm>
#include <vector>
#include <string>
#include <random>
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
    if (con.flag() & Context::SLAVE) {
        if (strncasecmp(buf.c_str(), "*1\r\n$4\r\nPONG\r\n", 14)) {
            con._offset += s - ps;
        }
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
    if (!(con.perm() & it->second.perm())) {
        con.append("-ERR permission denied\r\n");
        goto err;
    }
    arity = it->second.arity();
    if ((arity > 0 && cmdlist.size() < arity) || (arity < 0 && cmdlist.size() != -arity)) {
        con.append("-ERR wrong number of arguments for '" + it->first + "'\r\n");
        goto err;
    }
    if (it->second.perm() & IS_WRITE) {
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
    auto& context = std::any_cast<Context&>(conn->getContext());
    // 从服务器不应向主服务器发送回复信息
    if (!(context.flag() & Context::SLAVE))
        conn->send(context.message());
    context.message().clear();
    context.setState(Context::PARSING);
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
            }
        }
    }
    if (_dbServer.aof()->childPid() != -1) {
        pid_t pid = waitpid(_dbServer.aof()->childPid(), nullptr, WNOHANG);
        if (pid > 0) {
            _dbServer.aof()->childPidReset();
            _dbServer.aof()->appendRewriteBufferToAof();
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
            _dbServer.setLastSaveTime(now);
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

void DBServer::setSlaveToReadonly()
{
    auto& maps = g_server->server().connectionMaps();
    for (auto& it : maps) {
        if (it.second->getContext().has_value()) {
            auto& context = std::any_cast<Context&>(it.second->getContext());
            context.clearPerm(IS_WRITE);
        }
    }
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
            std::bind(&DBServer::recvSyncFromMaster, this, _1, _2));
    _client->start();
}

void DBServer::sendSyncToMaster(const Angel::TcpConnectionPtr& conn)
{
    if (!conn->getContext().has_value()) {
        // slave -> master: 第一次复制
        setSlaveToReadonly();
        const char *sync = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
        Context context(this, conn);
        context.setFlag(Context::SYNC_WAIT);
        context.setPerm(IS_INTER);
        conn->setContext(context);
        conn->send(sync);
    } else {
        auto& context = std::any_cast<Context&>(conn->getContext());
        context.setFlag(Context::SYNC_WAIT);
        std::string buffer;
        buffer += "*3\r\n$5\r\nPSYNC\r\n$32\r\n";
        buffer += context.masterRunId();
        buffer += "\r\n$";
        buffer += convert(strlen(convert(context._offset)));
        buffer += "\r\n";
        buffer += convert(context._offset);
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
        if (context.flag() & Context::SYNC_WAIT) {
            if (strncasecmp(buf.c_str(), "+FULLRESYNC\r\n", 13) == 0) {
                char *s = buf.peek();
                const char *ps = s;
                s += 13;
                int crlf = buf.findStr(s, "\r\n", 2);
                if (crlf < 0) return;
                context.setMasterRunId(s);
                s += crlf + 2;
                crlf = buf.findStr(s, "\r\n", 2);
                if (crlf < 0) return;
                context._offset = atoll(s);
                s += crlf + 2;
                buf.retrieve(s - ps);
                context.clearFlag(Context::SYNC_WAIT);
                context.setFlag(Context::SYNC_FULL);
            } else if (strncasecmp(buf.c_str(), "+CONTINUE\r\n", 11) == 0) {
                buf.retrieve(11);
                context.clearFlag(Context::SYNC_WAIT);
                context.setFlag(Context::SYNC_PART);
            } else
                break;
        } else if (context.flag() & Context::SYNC_OK){
            context.clearFlag(Context::SYNC_OK);
            break;
        } else if (context.flag() & Context::SYNC_FULL) {
            recvRdbfileFromMaster(conn, buf);
        } else if (context.flag() & Context::SYNC_PART) {
            context.clearFlag(Context::SYNC_PART);
            context.setFlag(Context::SYNC_COMMAND);
        } else if (context.flag() & Context::SYNC_COMMAND) {
            g_server->onMessage(conn, buf);
            break;
        }
    }
}

void DBServer::recvRdbfileFromMaster(const Angel::TcpConnectionPtr& conn, Angel::Buffer& buf)
{
    auto& context = std::any_cast<Context&>(conn->getContext());
    if (context._syncRdbFilesize == 0) {
        int crlf = buf.findStr("\r\n\r\n", 4);
        if (crlf > 0) {
            context._syncRdbFilesize = atoll(buf.peek());
            buf.retrieve(crlf + 4);
            strcpy(context.tmpfile, "tmp.XXXXX");
            mktemp(context.tmpfile);
            context._fd = open(context.tmpfile, O_RDWR | O_APPEND | O_CREAT, 0660);
            if (buf.readable() > 0)
                goto next;
            else
                goto jump;
        } else
            goto jump;
    } else {
next:
        size_t writeBytes = buf.readable();
        if (writeBytes >= context._syncRdbFilesize) {
            writeBytes = context._syncRdbFilesize;
            context._syncRdbFilesize = 0;
        } else {
            context._syncRdbFilesize -= writeBytes;
        }
        write(context._fd, buf.peek(), writeBytes);
        buf.retrieve(writeBytes);
        if (context._syncRdbFilesize == 0) {
            fsync(context._fd);
            close(context._fd);
            context._fd = -1;
            rename(context.tmpfile, "dump.rdb");
            db().hashMap().clear();
            rdb()->load();
            context.clearFlag(Context::SYNC_FULL);
            context.setFlag(Context::SLAVE | Context::SYNC_COMMAND);
            g_server->loop()->runEvery(1000, [this, conn]{
                    this->sendAckToMaster(conn);
                    this->sendPingToMaster(conn);
                    });
        } else {
            goto jump;
        }
    }
    return;
jump:
    context.setFlag(Context::SYNC_OK);
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
    rdb()->syncBuffer().clear();
    close(fd);
}

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

void DBServer::sendPingToMaster(const Angel::TcpConnectionPtr& conn)
{
    conn->send("*1\r\n$4\r\nPING\r\n");
}

void DBServer::sendAckToMaster(const Angel::TcpConnectionPtr& conn)
{
    auto& context = std::any_cast<Context&>(conn->getContext());
    std::string buffer;
    buffer += "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$";
    buffer += convert(strlen(convert(context._offset)));
    buffer += "\r\n";
    buffer += convert(context._offset);
    buffer += "\r\n";
    conn->send(buffer);
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

void DBServer::appendCopyBacklogBuffer(Context::CommandList& cmdlist)
{
    std::string buffer;
    appendCommand(buffer, cmdlist, false);
    _offset += buffer.size();
    size_t remainBytes = copy_backlog_buffer_size - _copyBacklogBuffer.size();
    if (remainBytes < buffer.size()) {
        size_t popsize = buffer.size() - remainBytes;
        _copyBacklogBuffer.erase(0, popsize);
    }
    _copyBacklogBuffer.append(buffer);
}

void DBServer::appendWriteCommand(Context::CommandList& cmdlist)
{
    aof()->append(cmdlist);
    if (aof()->childPid() != -1)
        aof()->appendRewriteBuffer(cmdlist);
    if (flag() & DBServer::PSYNC)
        rdb()->appendSyncBuffer(cmdlist);
    if (!slaveIds().empty())
        appendCopyBacklogBuffer(cmdlist);
}

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

void DBServer::setRunId()
{
    struct timespec tsp;
    clock_gettime(_CLOCK_REALTIME, &tsp);
    std::uniform_int_distribution<size_t> u;
    std::mt19937_64 e(tsp.tv_sec * 1000000000 + tsp.tv_nsec);
    snprintf(_runId, 17, "%lx", u(e));
    snprintf(_runId + 16, 17, "%lx", u(e));
    _runId[6] &= 0x0f;
    _runId[6] |= 0x40;
    _runId[8] &= 0x3f;
    _runId[8] |= 0x80;
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
