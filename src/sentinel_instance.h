#ifndef _ALICE_SRC_SENTINEL_INSTANCE_H
#define _ALICE_SRC_SENTINEL_INSTANCE_H

#include <Angel/TcpClient.h>

#include <string>
#include <memory>

#include "util.h"

namespace Alice {

class SentinelInstance {
public:
    enum Flag {
        MASTER = 0x01, // 主服务器实例
        SLAVE = 0x02, // 从服务器实例
        SENTINEL = 0x04, // sentinel实例
        // 如果单个sentinel认为一个主服务器下线，则该主服务器被标记为S_DOWN
        // 如果多数sentinel认为一个主服务器下线，则该主服务器被标记为O_DOWN
        // 而只要从服务器或sentinel被标记为S_DOWN，就会被移除，
        // 并不需要和其他sentinel进行协商
        S_DOWN = 0x08,
        O_DOWN = 0x10,
    };
    SentinelInstance()
        : _flag(0),
        _configEpoch(0),
        _downAfterPeriod(0),
        _quorum(0),
        _offset(0),
        _lastHeartBeatTime(Angel::TimeStamp::now()),
        _downQuorum(0)
    {
        setSelfRunId(_runId);
    }
    using SentinelInstanceMap = std::unordered_map<std::string,
          std::unique_ptr<SentinelInstance>>;
    int flag() const { return _flag; }
    void setFlag(int flag) { _flag |= flag; }
    void clearFlag(int flag) { _flag &= ~flag; }
    const std::string& name() const { return _name; }
    void setName(const std::string& name) { _name = std::move(name); }
    void setRunId(const std::string& runId)
    { memcpy(_runId, runId.data(), 32); _runId[32] = '\0'; }
    const char *runId() const { return _runId; }
    void setConfigEpoch(uint64_t epoch) { _configEpoch = epoch; }
    uint64_t configEpoch() const { return _configEpoch; }
    Angel::InetAddr& inetAddr() { return *_inetAddr.get(); }
    int64_t downAfterPeriod() const { return _downAfterPeriod; }
    void setDownAfterPeriod(int64_t tv) { _downAfterPeriod = tv; }
    int quorum() const { return _quorum; }
    void setQuorum(int quorum) { _quorum = quorum; }
    std::unique_ptr<Angel::TcpClient> *clients() { return _clients; }
    SentinelInstanceMap& slaves() { return _slaves; }
    SentinelInstanceMap& sentinels() { return _sentinels; }
    size_t offset() const { return _offset; }
    void setOffset(size_t offset) { _offset = offset; }
    int64_t lastHeartBeatTime() const { return _lastHeartBeatTime; }
    void setLastHeartBeatTime(int64_t time) { _lastHeartBeatTime = time; }
    const std::string& masterName() const { return _masterName; }
    void setMasterName(const std::string& name) { _masterName = std::move(name); }
    int downQuorum() const { return _downQuorum; }
    void downQuorumIncr() { _downQuorum++; }
    void downQuorumReset() { _downQuorum = 0; }
    void creatCmdConnection();
    void creatPubConnection();
    void closeConnection(const Angel::TcpConnectionPtr& conn);
    void pubMessage(const Angel::TcpConnectionPtr& conn);
    void recvReplyFromServer(const Angel::TcpConnectionPtr& conn, Angel::Buffer& buf);
    void parseInfoReplyFromMaster(const char *s, const char *es);
    void parseInfoReplyFromSlave(const char *s, const char *es);
    void updateSlaves(const char *s, const char *es);
    void parseReplyFromSentinel(const char *s, const char *es);
    void setInetAddr(Angel::InetAddr inetAddr)
    {
        if (_inetAddr) _inetAddr.reset();
        _inetAddr.reset(new Angel::InetAddr(inetAddr.inetAddr()));
    }
    void askMasterDownForOtherSentinels();
private:
    int _flag;
    std::string _name;
    char _runId[33];
    uint64_t _configEpoch;
    std::unique_ptr<Angel::InetAddr> _inetAddr;
    int64_t _downAfterPeriod;
    int _quorum;
    std::unique_ptr<Angel::TcpClient> _clients[2];
    SentinelInstanceMap _slaves;
    SentinelInstanceMap _sentinels;
    size_t _offset;
    int64_t _lastHeartBeatTime;
    // sentinel实例使用，记录它们所监视的主服务器名字
    std::string _masterName;
    // master实例使用，记录同意其主观下线的sentinel数目
    int _downQuorum;
};
}

#endif // _ALICE_SRC_SENTINEL_INSTANCE_H
