#include <Angel/EventLoop.h>
#include <Angel/TcpServer.h>
#include <algorithm>
#include "server.h"
#include "db.h"

using namespace Alice;

// *1\r\n$8\r\njkjljljl\r\n
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
        next = std::find(s, es, '\n');
        if (next == es) return;
        next = std::find_if(s, es, isspace);
        if (next[0] != '\r' || next[1] != '\n' || next - s != len) {
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
    // for (auto& xx : cmdlist)
        // std::cout << xx << " ";
    // std::cout << "\n";
    std::transform(cmdlist[0].begin(), cmdlist[0].end(), cmdlist[0].begin(), ::toupper);
    auto it = _db.db().commandMap().find(cmdlist[0]);
    if (it == _db.db().commandMap().end()) {
        con.append("-ERR unknown command `" + cmdlist[0] + "`\r\n");
        goto err;
    }
    arity = it->second.arity();
    if ((arity > 0 && cmdlist.size() < arity) || (arity < 0 && cmdlist.size() != -arity)) {
        con.append("-ERR wrong number of arguments for '" + it->first + "'\r\n");
        goto err;
    }
    if (cmdlist.size() > 1) {
        auto it = _db.db().hashMap().find(cmdlist[1]);
        if (it != _db.db().hashMap().end())
            it->second.setLru(_lru_cache);
    }
    it->second._commandCb(con);
err:
    con.setFlag(Context::REPLY);
    cmdlist.clear();
}

void Server::replyResponse(const Angel::TcpConnectionPtr& conn)
{
    auto& client = std::any_cast<Context&>(conn->getContext());
    conn->send(client.message());
    // std::cout << client.message();
    client.message().clear();
    client.setFlag(Context::PARSING);
}

namespace Alice {

    // 键的空转时间不需要十分精确
    thread_local int64_t _lru_cache;
}

void Server::serverCron()
{
    int64_t now = Angel::TimeStamp::now();
    _lru_cache = now;
    for (auto& it : _db.expireMap()) {
        if (it.second <= now) {
            _db.db().delKey(it.first);
            _db.delExpireKey(it.first);
        }
    }
}

int main()
{
    Angel::EventLoop loop;
    Angel::InetAddr listenAddr(8000);
    Alice::Server server(&loop, listenAddr);
    server.start();
    loop.run();
}
