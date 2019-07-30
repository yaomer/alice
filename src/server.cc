#include <unistd.h>
#include <sys/wait.h>
#include <Angel/EventLoop.h>
#include <Angel/TcpServer.h>
#include <algorithm>
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
        if (next == es) return;
        if (s[0] != '$' || next[-1] != '\r') goto err;
        s += 1;
        len = atol(s);
        s = std::find_if(s, es, [](char c){ return !isnumber(c); });
        if (s[0] != '\r' || s[1] != '\n') goto err;
        s += 2;
        next = std::find(s, es, '\r');
        if (next == es) return;
        if (next[1] != '\n' || next - s != len) {
            goto err;
        }
        con.addArg(s, next);
        s = next + 2;
        argc--;
    }
    buf.retrieve(s - ps);
    con.setFlag(Context::SUCCEED);
    return;
err:
    con.setFlag(Context::PROTOCOLERR);
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
    if (it->second.isWrite()) _dbServer.dirtyIncr();
    it->second._commandCb(con);
err:
    con.setFlag(Context::REPLY);
    cmdlist.clear();
}

void Server::replyResponse(const Angel::TcpConnectionPtr& conn)
{
    auto& client = std::any_cast<Context&>(conn->getContext());
    conn->send(client.message());
    client.message().clear();
    client.setFlag(Context::PARSING);
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

    if (_dbServer.flag() & DBServer::RDB_BGSAVE) {
        pid_t childPid = _dbServer.rdb()->bgSavePid();
        if (getpid() != childPid) {
            pid_t pid = waitpid(childPid, nullptr, WNOHANG);
            if (pid > 0) {
                _dbServer.clearFlag(DBServer::RDB_BGSAVE);
            }
        }
    }
    int saveInterval = (now - _dbServer.lastSaveTime()) / 1000;
    for (auto& it : _dbServer.saveParams()) {
        if (saveInterval >= it.seconds() && _dbServer.dirty() >= it.changes()) {
            if (!(_dbServer.flag() & DBServer::RDB_BGSAVE)) {
                _dbServer.rdb()->backgroundSave();
                _dbServer.setLastSaveTime(Angel::TimeStamp::now());
                _dbServer.dirtyReset();
                break;
            }
        }
    }

    for (auto& it : _dbServer.expireMap()) {
        if (it.second <= now) {
            _dbServer.db().delKey(it.first);
            _dbServer.delExpireKey(it.first);
        }
    }
}

void Server::readConf()
{
    FILE *fp = fopen("alice.conf", "r");
    char buf[1024];
    while (fgets(buf, sizeof(buf), fp)) {
        const char *s = buf;
        const char *es = buf + strlen(buf);
        s = std::find_if(s, es, [](char c){ return !isspace(c); });
        if (s == es || s[0] == '#') continue;
        if (strncasecmp(s, "save", 4) == 0) {
            time_t seconds = atoi(&s[5]);
            s = std::find(s + 5, es, ':');
            int changes = atoi(&s[1]);
            _dbServer.addSaveParam(seconds, changes);
        }
    }
    fclose(fp);
}

int main()
{
    Angel::EventLoop loop;
    Angel::InetAddr listenAddr(8000);
    Alice::Server server(&loop, listenAddr);
    g_server = &server;
    Angel::addSignal(SIGINT, []{
            g_server->dbServer().rdb()->save();
            g_server->loop()->quit();
            });
    server.start();
    loop.run();
}
