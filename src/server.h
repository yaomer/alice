#ifndef _ALICE_SRC_SERVER_H
#define _ALICE_SRC_SERVER_H

#include <Angel/EventLoop.h>
#include <Angel/TcpServer.h>
#include <unordered_map>
#include <string>
#include "db.h"

using std::placeholders::_1;
using std::placeholders::_2;

namespace Alice {

class DBServer {
public:
    using Key = std::string;
    using ExpireMap = std::unordered_map<Key, int64_t>;
    DB& db() { return _db; }
    ExpireMap& expireMap() { return _expireMap; }
    void addExpireKey(const Key& key, int64_t expire)
    {
        _expireMap.insert(std::make_pair(key, expire + Angel::TimeStamp::now()));
    }
    bool isExpiredMap(const Key& key)
    {
        return _expireMap.find(key) != _expireMap.end();
    }
    void delExpireKey(const Key& key)
    {
        auto it = _expireMap.find(key);
        if (it != _expireMap.end())
            _expireMap.erase(it);
    }
    bool isExpiredKey(const Key& key)
    {
        if (!isExpiredMap(key))
            return false;
        int64_t now = Angel::TimeStamp::now();
        auto it = _expireMap.find(key);
        if (it->second <= now) {
            delExpireKey(key);
            _db.delKey(key);
            return true;
        } else
            return false;
    }
private:
    DB _db;
    ExpireMap _expireMap;
};

class Server {
public:
    Server(Angel::EventLoop *loop, Angel::InetAddr& inetAddr)
        : _loop(loop),
        _server(loop, inetAddr)
    {
        _server.setConnectionCb(
                std::bind(&Server::onConnection, this, _1));
        _server.setMessageCb(
                std::bind(&Server::onMessage, this, _1, _2));
        _loop->runEvery(100, [this]{ this->serverCron(); });
    }
    void onConnection(const Angel::TcpConnectionPtr& conn)
    {
        conn->setContext(Context(&_db));
    }
    void onMessage(const Angel::TcpConnectionPtr& conn, Angel::Buffer& buf)
    {
        auto& client = std::any_cast<Context&>(conn->getContext());
        // std::cout << buf.c_str();
        while (true) {
            switch (client.flag()) {
            case Context::PARSING:
                parseRequest(client, buf);
                if (client.flag() == Context::PARSING)
                    return;
                break;
            case Context::PROTOCOLERR: 
                conn->close(); 
                return;
            case Context::SUCCEED: 
                executeCommand(client); 
                break;
            case Context::REPLY: 
                replyResponse(conn); 
                return;
            }
        }
    }
    void serverCron();
    void parseRequest(Context& con, Angel::Buffer& buf);
    void executeCommand(Context& con);
    void replyResponse(const Angel::TcpConnectionPtr& conn);
    void execError(const Angel::TcpConnectionPtr& conn);
    void start() { _server.start(); }
private:
    Angel::EventLoop *_loop;
    Angel::TcpServer _server;
    DBServer _db;
};

extern thread_local int64_t _lru_cache;
}

#endif
