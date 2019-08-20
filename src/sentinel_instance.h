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
        MASTER = 0x01,
        SLAVE = 0x02,
        SENTINEL = 0x04,
        CLOSED = 0x08,
    };
    SentinelInstance()
        : _flag(0),
        _configEpoch(0),
        _downAfterPeriod(0),
        _quorum(0),
        _offset(0)
    {
        setSelfRunId(_runId);
    }
    using SentinelInstanceMap = std::unordered_map<std::string, SentinelInstance>;
    int flag() const { return _flag; }
    void setFlag(int flag) { _flag |= flag; }
    void clearFlag(int flag) { _flag &= ~flag; }
    const std::string& name() const { return _name; }
    void setName(const std::string& name) 
    { _name = std::move(name); }
    void setRunId(const std::string& runId)
    { memcpy(_runId, runId.data(), 32); _runId[32] = '\0'; }
    const char *runId() const { return _runId; }
    void setConfigEpoch(uint64_t epoch) { _configEpoch = epoch; }
    uint64_t configEpoch() const { return _configEpoch; }
    Angel::InetAddr *inetAddr() const { return _inetAddr.get(); }
    int64_t downAfterPeriod() const { return _downAfterPeriod; }
    void setDownAfterPeriod(int64_t tv) { _downAfterPeriod = tv; }
    int quorum() const { return _quorum; }
    void setQuorum(int quorum) { _quorum = quorum; }
    std::unique_ptr<Angel::TcpClient> *clients() { return _clients; }
    SentinelInstanceMap& slaves() { return _slaves; }
    SentinelInstanceMap& sentinels() { return _sentinels; }
    size_t offset() const { return _offset; }
    void setOffset(size_t offset) { _offset = offset; }
    void creatCmdConnection();
    void creatPubConnection();
    void closeConnection(const Angel::TcpConnectionPtr& conn);
    void subMaster(const Angel::TcpConnectionPtr& conn);
    void recvSubMessageFromMaster(const Angel::TcpConnectionPtr& conn, Angel::Buffer& buf);
    void pubMessage(const Angel::TcpConnectionPtr& conn);
    void recvReplyFromMaster(const Angel::TcpConnectionPtr& conn, Angel::Buffer& buf);
    void parseInfoReply(const char *s, const char *es);
    void setInetAddr(Angel::InetAddr inetAddr)
    { 
        if (_inetAddr) _inetAddr.reset();
        _inetAddr.reset(new Angel::InetAddr(inetAddr.inetAddr())); 
    }
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
};
}

#endif // _ALICE_SRC_SENTINEL_INSTANCE_H
