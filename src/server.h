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
    using WatchMap = std::unordered_map<Key, std::vector<size_t>>; 
    using PubsubChannels = std::unordered_map<Key, std::vector<size_t>>;
    enum FLAG {
        // 开启aof持久化
        APPENDONLY = 0x01,
        // 有rewriteaof请求被延迟
        REWRITEAOF_DELAY = 0x02,
        // 有slave正在等待生成rdb快照
        PSYNC = 0x04,
        // 标志该服务器是从服务器
        SLAVE = 0x08,
        // 有psync请求被延迟
        PSYNC_DELAY = 0x10,
    };
    DBServer() 
        : _db(this),
        _rdb(new Rdb(this)),
        _aof(new Aof(this)),
        _flag(0),
        _lastSaveTime(Angel::TimeStamp::now()),
        _dirty(0),
        _copyBacklogBuffer(copy_backlog_buffer_size, 0),
        _masterOffset(0),
        _slaveOffset(0),
        _syncRdbFilesize(0),
        _syncFd(-1),
        _lastRecvHeartBeatTime(0),
        _heartBeatTimerId(0)
    { 
        setSelfRunId();
        bzero(_masterRunId, sizeof(_masterRunId));
        bzero(_tmpfile, sizeof(_tmpfile));
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
    void connectMasterServer();
    void slaveClientCloseCb(const Angel::TcpConnectionPtr& conn);
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
    void doWriteCommand(Context::CommandList& cmdlist);
    static void appendCommand(std::string& buffer, Context::CommandList& cmdlist,
            bool repl_set);
    void appendWriteCommand(Context::CommandList& cmdlist);
    std::set<size_t>& slaveIds() { return _slaveIds; }
    void addSlaveId(size_t id) { _slaveIds.insert(id); }
    size_t masterOffset() const { return _masterOffset; }
    void masterOffsetIncr(ssize_t incr) { _masterOffset += incr; }
    size_t slaveOffset() const { return _slaveOffset; }
    void slaveOffsetIncr(ssize_t incr) { _slaveOffset += incr; }
    int64_t lastRecvHeartBeatTime() const { return _lastRecvHeartBeatTime; }
    void setLastRecvHeartBeatTime(int64_t tv) { _lastRecvHeartBeatTime = tv; }
    std::string& copyBacklogBuffer() { return _copyBacklogBuffer; }
    void appendCopyBacklogBuffer(Context::CommandList& cmdlist);
    const char *selfRunId() const { return _selfRunId; }
    const char *masterRunId() const { return _masterRunId; }
    void setMasterRunId(const char *s)
    { memcpy(_masterRunId, s, 32); _masterRunId[32] = '\0'; }
    void setHeartBeatTimer(const Angel::TcpConnectionPtr& conn);
    void watchKeyForClient(const Key& key, size_t id);
    void unwatchKeys() { _watchMap.clear(); }
    void touchWatchKey(const Key& key);
    void subChannel(const Key& key, size_t id);
    size_t pubMessage(const std::string& msg, const std::string& channel,  size_t id);
    void setMasterAddr(Angel::InetAddr addr)
    { 
        if (_masterAddr) _masterAddr.reset();
        _masterAddr.reset(new Angel::InetAddr(addr.inetAddr())); 
    }

    static const size_t copy_backlog_buffer_size = 1024 * 1024;
private:
    void setSelfRunId();

    DB _db;
    ExpireMap _expireMap;
    std::unique_ptr<Rdb> _rdb;
    std::unique_ptr<Aof> _aof;
    std::vector<SaveParam> _saveParams;
    int _flag;
    // 上一次执行持久化的时间
    int64_t _lastSaveTime;
    // 自上一次rdb持久化以后，执行了多少次写操作
    int _dirty;
    // 服务器作为从服务器运行时的主服务器inetAddr
    std::unique_ptr<Angel::InetAddr> _masterAddr;
    // 服务器作为从服务器时，与主服务器的连接
    std::unique_ptr<Angel::TcpClient> _client;
    // 服务器作为主服务器运行时，与之同步的所有从服务器id
    std::set<size_t> _slaveIds;
    // 复制积压缓冲区
    std::string _copyBacklogBuffer;
    // 服务器作为主服务器运行时的复制偏移量
    size_t _masterOffset;
    // 服务器作为从服务器运行时的复制偏移量
    size_t _slaveOffset;
    // 服务器自身的运行ID
    char _selfRunId[33];
    // 服务器作为从服务器去复制主服务器时的主服务器运行ID
    char _masterRunId[33];
    // 要发送给从服务器的rdb文件大小 
    size_t _syncRdbFilesize;
    char _tmpfile[16];
    int _syncFd;
    // 最后一次接收到主服务发送来的心跳包的时间
    int64_t _lastRecvHeartBeatTime;
    // 发送心跳包的定时器ID
    size_t _heartBeatTimerId;
    // watch命令使用的监视表
    WatchMap _watchMap;
    // 保存所有频道的订阅关系
    PubsubChannels _pubsubChannels;
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
    }
    void onClose(const Angel::TcpConnectionPtr& conn)
    {
        auto it = _dbServer.slaveIds().find(conn->id());
        if (it != _dbServer.slaveIds().end())
            _dbServer.slaveIds().erase(conn->id());
    }
    void onConnection(const Angel::TcpConnectionPtr& conn)
    {
        Context context(&_dbServer, conn);
        if (_dbServer.flag() & DBServer::SLAVE)
            context.clearFlag(IS_WRITE);
        conn->setContext(context);
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

extern Alice::Server *g_server;
extern thread_local int64_t _lru_cache;

}

#endif
