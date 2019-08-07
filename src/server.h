#ifndef _ALICE_SRC_SERVER_H
#define _ALICE_SRC_SERVER_H

#include <Angel/EventLoop.h>
#include <Angel/TcpServer.h>
#include <Angel/TcpClient.h>
#include <unordered_map>
#include <set>
#include <string>
#include <memory>
#include "db.h"
#include "rdb.h"
#include "aof.h"

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
        APPENDONLY = 0x01,
        REWRITEAOF_DELAY = 0x02,
        PSYNC = 0x04,
    };
    DBServer() 
        : _db(this),
        _rdb(new Rdb(this)),
        _aof(new Aof(this)),
        _flag(0),
        _lastSaveTime(Angel::TimeStamp::now()),
        _dirty(0),
        _copyBacklogBuffer(copy_backlog_buffer_size, 0),
        _offset(0)
    { 
        setRunId();
    }
    DB& db() { return _db; }
    Rdb *rdb() { return _rdb.get(); }
    Aof *aof() { return _aof.get(); }
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
    void setMasterAddr(Angel::InetAddr addr)
    { _masterAddr.reset(new Angel::InetAddr(addr.inetAddr())); }
    void connectMasterServer();
    void setSlaveToReadonly();
    void sendSyncToMaster(const Angel::TcpConnectionPtr& conn);
    void recvSyncFromMaster(const Angel::TcpConnectionPtr& conn, Angel::Buffer& buf);
    void recvRdbfileFromMaster(const Angel::TcpConnectionPtr& conn, Angel::Buffer& buf);
    void sendRdbfileToSlave();
    void sendSyncCommandToSlave(Context::CommandList& cmdlist);
    void sendAckToMaster(const Angel::TcpConnectionPtr& conn);
    void sendPingToMaster(const Angel::TcpConnectionPtr& conn);
    ExpireMap& expireMap() { return _expireMap; }
    void addExpireKey(const Key& key, int64_t expire)
    { _expireMap[key] = expire + Angel::TimeStamp::now(); }
    void delExpireKey(const Key& key);
    bool isExpiredKey(const Key& key);
    static void appendCommand(std::string& buffer, Context::CommandList& cmdlist,
            bool repl_set);
    void appendWriteCommand(Context::CommandList& cmdlist);
    std::set<size_t>& slaveIds() { return _slaveIds; }
    void addSlaveId(size_t id) { _slaveIds.insert(id); }
    size_t offset() { return _offset; }
    std::string& copyBacklogBuffer() { return _copyBacklogBuffer; }
    void appendCopyBacklogBuffer(Context::CommandList& cmdlist);
    const char *runId() const { return _runId; }

    static const size_t copy_backlog_buffer_size = 1024 * 1024;
private:
    void setRunId();

    DB _db;
    ExpireMap _expireMap;
    std::unique_ptr<Rdb> _rdb;
    std::unique_ptr<Aof> _aof;
    std::vector<SaveParam> _saveParams;
    int _flag;
    int64_t _lastSaveTime;
    int _dirty;
    std::unique_ptr<Angel::InetAddr> _masterAddr;
    std::unique_ptr<Angel::TcpClient> _client;
    std::set<size_t> _slaveIds;
    std::string _copyBacklogBuffer;
    size_t _offset;
    char _runId[33];
};

class Server {
public:
    using ConfParamList = std::vector<std::vector<std::string>>;

    Server(Angel::EventLoop *loop, Angel::InetAddr& inetAddr)
        : _loop(loop),
        _server(loop, inetAddr)
    {
        _server.setConnectionCb(
                std::bind(&Server::onConnection, this, _1));
        _server.setMessageCb(
                std::bind(&Server::onMessage, this, _1, _2));
        _server.setCloseCb(
                std::bind(&Server::onClose, this, _1));
        readConf();
        _loop->runEvery(100, [this]{ this->serverCron(); });
        if (_dbServer.flag() & DBServer::APPENDONLY)
            _dbServer.aof()->load();
        else
            _dbServer.rdb()->load();
    }
    void onClose(const Angel::TcpConnectionPtr& conn)
    {
        auto it = _dbServer.slaveIds().find(conn->id());
        if (it != _dbServer.slaveIds().end())
            _dbServer.slaveIds().erase(conn->id());
    }
    void onConnection(const Angel::TcpConnectionPtr& conn)
    {
        conn->setContext(Context(&_dbServer, conn));
    }
    void onMessage(const Angel::TcpConnectionPtr& conn, Angel::Buffer& buf)
    {
        auto& context = std::any_cast<Context&>(conn->getContext());
        while (true) {
            switch (context.state()) {
            case Context::PARSING:
                parseRequest(context, buf);
                if (context.state() == Context::PARSING)
                    return;
                break;
            case Context::PROTOCOLERR: 
                conn->close();
                return;
            case Context::SUCCEED: 
                executeCommand(context); 
                break;
            case Context::REPLY: 
                replyResponse(conn); 
                break;
            }
        }
    }
    void readConf();
    void serverCron();
    static void parseRequest(Context& con, Angel::Buffer& buf);
    void executeCommand(Context& con);
    void replyResponse(const Angel::TcpConnectionPtr& conn);
    void execError(const Angel::TcpConnectionPtr& conn);
    void start() { _server.start(); }
    DBServer& dbServer() { return _dbServer; }
    Angel::TcpServer& server() { return _server; }
    Angel::EventLoop *loop() { return _loop; }
private:
    void parseConf();

    Angel::EventLoop *_loop;
    Angel::TcpServer _server;
    DBServer _dbServer;
    ConfParamList _confParamList;
};

extern thread_local int64_t _lru_cache;
extern Alice::Server *g_server;

}

#endif
