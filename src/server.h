#ifndef _ALICE_SRC_SERVER_H
#define _ALICE_SRC_SERVER_H

#include <Angel/EventLoop.h>
#include <Angel/TcpServer.h>
#include <Angel/TcpClient.h>
#include <Angel/SockOps.h>
#include <Angel/LogStream.h>

#include <unistd.h>
#include <unordered_map>
#include <map>
#include <set>
#include <string>
#include <memory>
#include "db.h"
#include "rdb.h"
#include "aof.h"
#include "config.h"
#include "util.h"

using std::placeholders::_1;
using std::placeholders::_2;

namespace Alice {

struct Slowlog {
    Slowlog() = default;
    int _id;
    int64_t _time;
    int64_t _duration;
    std::vector<std::string> _args;
};

class DBServer {
public:
    using DBS = std::vector<std::unique_ptr<DB>>;
    using Key = std::string;
    using PubsubChannels = std::unordered_map<Key, std::vector<size_t>>;
    using BlockedClients = std::list<size_t>;
    using SlowlogQueue = std::list<Slowlog>;
    enum FLAG {
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
        : _curDbnum(0),
        _rdb(new Rdb(this)),
        _aof(new Aof(this)),
        _flag(0),
        _lastSaveTime(Angel::TimeStamp::now()),
        _dirty(0),
        _masterOffset(0),
        _slaveOffset(0),
        _syncRdbFilesize(0),
        _syncFd(-1),
        _lastRecvHeartBeatTime(0),
        _heartBeatTimerId(0),
        _curCheckDb(0),
        _slowlogId(0)
    {
        for (int i = 0; i < g_server_conf.databases; i++) {
            std::unique_ptr<DB> db(new DB(this));
            _dbs.push_back(std::move(db));
        }
        _copyBacklogBuffer.reserve(g_server_conf.repl_backlog_size);
        setSelfRunId(_selfRunId);
        bzero(_masterRunId, sizeof(_masterRunId));
        bzero(_tmpfile, sizeof(_tmpfile));
    }
    DBS& dbs() { return _dbs; }
    int curDbnum() const { return _curDbnum; }
    void switchDb(int dbnum) { _curDbnum = dbnum; }
    DB *db() { return _dbs[_curDbnum].get(); }
    DB *selectDb(int dbnum) { return _dbs[dbnum].get(); }
    Rdb *rdb() { return _rdb.get(); }
    Aof *aof() { return _aof.get(); }
    int flag() { return _flag; }
    void setFlag(int flag) { _flag |= flag; }
    void clearFlag(int flag) { _flag &= ~flag; }
    int64_t lastSaveTime() const { return _lastSaveTime; }
    void setLastSaveTime(int64_t now) { _lastSaveTime = now; }
    int dirty() const { return _dirty; }
    void dirtyIncr() { _dirty++; }
    void dirtyReset() { _dirty = 0; }
    void connectMasterServer();
    void slaveClientCloseCb(const Angel::TcpConnectionPtr& conn);
    void setSlaveToReadonly();
    void sendPingToMaster(const Angel::TcpConnectionPtr& conn);
    void sendInetAddrToMaster(const Angel::TcpConnectionPtr& conn);
    void sendSyncToMaster(const Angel::TcpConnectionPtr& conn);
    void recvSyncFromMaster(const Angel::TcpConnectionPtr& conn, Angel::Buffer& buf);
    void recvRdbfileFromMaster(const Angel::TcpConnectionPtr& conn, Angel::Buffer& buf);
    void recvPingFromMaster();
    void sendRdbfileToSlave();
    void sendSyncCommandToSlave(Context::CommandList& cmdlist);
    void sendAckToMaster(const Angel::TcpConnectionPtr& conn);
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
    void subChannel(const Key& key, size_t id);
    size_t pubMessage(const std::string& msg, const std::string& channel,  size_t id);
    void checkExpireCycle(int64_t now);
    void checkBlockedClients(int64_t now);
    BlockedClients& blockedClients() { return _blockedClients; }
    void removeBlockedClient(size_t id);
    void setMasterAddr(Angel::InetAddr addr)
    {
        if (_masterAddr) _masterAddr.reset();
        _masterAddr.reset(new Angel::InetAddr(addr.inetAddr()));
    }
    void freeMemoryIfNeeded();
    SlowlogQueue& slowlogQueue() { return _slowlogQueue; }
    void addSlowlogIfNeeded(Context::CommandList& cmdlist, int64_t start, int64_t end);
    static ssize_t getProcMemory();
private:
    void evictAllkeysWithLru();
    void evictVolatileWithLru();
    void evictAllkeysWithRandom();
    void evictVolatileWithRandom();
    void evictVolatileWithTtl();
    template <typename T>
    void evictKey(const T& hash, int (evict)[2])
    {
        for (auto it = hash.cbegin(evict[0]); it != hash.end(evict[0]); ++it) {
            if (evict[1]-- == 0) {
                evictKey(it->first);
                break;
            }
        }
    }
    void evictKey(const std::string& key);

    DBS _dbs;
    // 当前选择的数据库号码
    int _curDbnum;
    std::unique_ptr<Rdb> _rdb;
    std::unique_ptr<Aof> _aof;
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
    // 保存所有频道的订阅关系
    PubsubChannels _pubsubChannels;
    // 记录过期键删除进度
    int _curCheckDb;
    // 保存所有执行阻塞命令阻塞的客户端
    BlockedClients _blockedClients;
    // 记录慢查询日志
    SlowlogQueue _slowlogQueue;
    // 慢查询日志的ID
    int _slowlogId;
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
        _server.setCloseCb(
                std::bind(&Server::onClose, this, _1));
    }
    void onClose(const Angel::TcpConnectionPtr& conn)
    {
        auto it = _dbServer.slaveIds().find(conn->id());
        if (it != _dbServer.slaveIds().end())
            _dbServer.slaveIds().erase(conn->id());
        auto& context = std::any_cast<Context&>(conn->getContext());
        if (context.flag() & Context::CON_BLOCK) {
            DB *db = _dbServer.selectDb(context.blockDbnum());
            db->clearBlockingKeysForContext(context);
            _dbServer.removeBlockedClient(conn->id());
        }
    }
    void onConnection(const Angel::TcpConnectionPtr& conn)
    {
        Context context(&_dbServer, conn.get());
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
                if (buf.readable() == 0)
                    return;
                break;
            }
        }
    }
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
    Angel::EventLoop *_loop;
    Angel::TcpServer _server;
    DBServer _dbServer;
};

extern Alice::Server *g_server;
extern thread_local int64_t _lru_cache;

}

#endif
