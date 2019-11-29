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
        FOLLOWER = 0x20,
        HAVE_LEADER = 0x40,
        DELETE = 0x80,
    };
    SentinelInstance()
        : _flag(0),
        _configEpoch(0),
        _downAfterPeriod(0),
        _quorum(0),
        _offset(0),
        _lastHeartBeatTime(Angel::nowMs()),
        _votes(0),
        _leaderEpoch(0),
        _failoverEpoch(0),
        _electTimeoutTimerId(0)
    {
        setSelfRunId(_runId);
    }
    using SentinelInstanceMap = std::unordered_map<std::string,
          std::unique_ptr<SentinelInstance>>;
    int flag() const { return _flag; }
    void setFlag(int flag) { _flag |= flag; }
    void clearFlag(int flag) { _flag &= ~flag; }
    const std::string& master() const { return _master; }
    void setMaster(const std::string& master) { _master = master; }
    const std::string& name() const { return _name; }
    void setName(const std::string& name) { _name = std::move(name); }
    const char *runId() const { return _runId; }
    void setRunId(const std::string& runId)
    { memcpy(_runId, runId.data(), 32); _runId[32] = '\0'; }
    void setConfigEpoch(uint64_t epoch) { _configEpoch = epoch; }
    uint64_t configEpoch() const { return _configEpoch; }
    Angel::InetAddr& inetAddr() { return *_inetAddr.get(); }
    int64_t downAfterPeriod() const { return _downAfterPeriod; }
    void setDownAfterPeriod(int64_t tv) { _downAfterPeriod = tv; }
    unsigned quorum() const { return _quorum; }
    void setQuorum(unsigned quorum) { _quorum = quorum; }
    unsigned votes() const { return _votes; }
    void votesIncr() { _votes++; }
    void votesReset() { _votes = 0; }
    std::unique_ptr<Angel::TcpClient> *clients() { return _clients; }
    SentinelInstanceMap& slaves() { return _slaves; }
    SentinelInstanceMap& sentinels() { return _sentinels; }
    size_t offset() const { return _offset; }
    void setOffset(size_t offset) { _offset = offset; }
    int64_t lastHeartBeatTime() const { return _lastHeartBeatTime; }
    void setLastHeartBeatTime(int64_t time) { _lastHeartBeatTime = time; }
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
        _inetAddr.reset(new Angel::InetAddr(inetAddr.inetAddr()));
    }
    void askForSentinels(const char *runid);
    void askMasterStateForOtherSentinels();
    void startFailover();
    void setElectTimeoutTimer();
    void electTimeout();
    void cancelElectTimeoutTimer();
    void noticeLeaderToOtherSentinels();
    void selectNewMaster(); // call by master
    void stopToReplicateMaster(); // call by slave
    void replicateMaster(Angel::InetAddr& masterAddr); // call by slave
    void convertSlaveToMaster(SentinelInstance *slave); // call by master
    const std::string& leader() const { return _leader; }
    void setLeader(const std::string& leader) { _leader = leader; }
    uint64_t leaderEpoch() const { return _leaderEpoch; }
    void setLeaderEpoch(uint64_t epoch) { _leaderEpoch = epoch; }
    uint64_t failoverEpoch() const { return _failoverEpoch; }
    void setFailoverEpoch(uint64_t epoch) { _failoverEpoch = epoch; }
private:
    int _flag;
    std::string _master; // for sentinel or slave
    std::string _name;
    char _runId[RUNID_LEN];
    uint64_t _configEpoch;
    std::unique_ptr<Angel::InetAddr> _inetAddr;
    int64_t _downAfterPeriod;
    unsigned _quorum; // for master
    std::unique_ptr<Angel::TcpClient> _clients[2];
    SentinelInstanceMap _slaves; // for master
    SentinelInstanceMap _sentinels; // for master
    size_t _offset;
    int64_t _lastHeartBeatTime;
    unsigned _votes; // for master
    std::string _leader; // for master
    uint64_t _leaderEpoch; // for master
    uint64_t _failoverEpoch; // for master
    size_t _electTimeoutTimerId; // for master
};
}

#endif // _ALICE_SRC_SENTINEL_INSTANCE_H
