#ifndef _ALICE_SRC_SERVER_H
#define _ALICE_SRC_SERVER_H

#include <Angel/EventLoop.h>
#include <Angel/TcpServer.h>
#include <unordered_map>
#include <string>
#include <memory>
#include "db.h"
#include "rdb.h"

using std::placeholders::_1;
using std::placeholders::_2;

namespace Alice {

class SaveParam {
public:
    SaveParam(time_t seconds, int changes)
        : _seconds(seconds),
        _changes(changes)
    {
    }
    time_t seconds() const { return _seconds; }
    int changes() const { return _changes; }
private:
    time_t _seconds;
    int _changes;
};

class DBServer {
public:
    using Key = std::string;
    using ExpireMap = std::unordered_map<Key, int64_t>;
    enum FLAG {
        RDB_BGSAVE = 001,
    };
    DBServer() 
        : _db(this),
        _flag(0),
        _lastSaveTime(Angel::TimeStamp::now())
    { 
        _rdb.reset(new Rdb(this)); 
    }
    DB& db() { return _db; }
    Rdb *rdb() { return _rdb.get(); }
    int flag() { return _flag; }
    void setFlag(int flag) { _flag |= flag; }
    void clearFlag(int flag) { _flag &= ~flag; }
    void addSaveParam(time_t seconds, int changes)
    { _saveParams.push_back(SaveParam(seconds, changes)); }
    std::vector<SaveParam>& saveParams() { return _saveParams; }
    int64_t lastSaveTime() const { return _lastSaveTime; }
    void setLastSaveTime(int64_t now) { _lastSaveTime = now; }
    int dirty() const { return _dirty; }
    void dirtyIncr() { _dirty++; }
    void dirtyReset() { _dirty = 0; }
    ExpireMap& expireMap() { return _expireMap; }
    void addExpireKey(const Key& key, int64_t expire)
    { _expireMap[key] = expire + Angel::TimeStamp::now(); }
    void delExpireKey(const Key& key)
    {
        auto it = _expireMap.find(key);
        if (it != _expireMap.end())
            _expireMap.erase(it);
    }
    bool isExpiredKey(const Key& key)
    {
        auto it = _expireMap.find(key);
        if (it == _expireMap.end())
            return false;
        int64_t now = Angel::TimeStamp::now();
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
    std::unique_ptr<Rdb> _rdb;
    std::vector<SaveParam> _saveParams;
    int _flag;
    int64_t _lastSaveTime;
    int _dirty;
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
        readConf();
        _loop->runEvery(100, [this]{ this->serverCron(); });
        _dbServer.rdb()->load();
    }
    void onConnection(const Angel::TcpConnectionPtr& conn)
    {
        conn->setContext(Context(&_dbServer));
    }
    void onMessage(const Angel::TcpConnectionPtr& conn, Angel::Buffer& buf)
    {
        auto& client = std::any_cast<Context&>(conn->getContext());
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
    void readConf();
    void serverCron();
    void parseRequest(Context& con, Angel::Buffer& buf);
    void executeCommand(Context& con);
    void replyResponse(const Angel::TcpConnectionPtr& conn);
    void execError(const Angel::TcpConnectionPtr& conn);
    void start() { _server.start(); }
    DBServer& dbServer() { return _dbServer; }
    Angel::EventLoop *loop() { return _loop; }
private:
    Angel::EventLoop *_loop;
    Angel::TcpServer _server;
    DBServer _dbServer;
};

extern thread_local int64_t _lru_cache;
extern Alice::Server *g_server;

}

#endif
