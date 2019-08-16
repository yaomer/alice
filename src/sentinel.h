#ifndef _ALICE_SRC_SENTINEL_H
#define _ALICE_SRC_SENTINEL_H

#include <Angel/TcpClient.h>
#include <Angel/EventLoop.h>

#include <unordered_map>

#include "server.h"
#include "config.h"
#include "sentinel_instance.h"

namespace Alice {

class Sentinel {
public:
    explicit Sentinel(Angel::EventLoop *loop, Angel::InetAddr inetAddr)
        : _loop(loop),
        _server(loop, inetAddr),
        _currentEpoch(0),
        _masters(&g_sentinel_conf.masters)
    {
    }
    void init();
    uint64_t currentEpoch() const { return _currentEpoch; }
    SentinelInstance::SentinelInstanceMap& masters() { return *_masters; }
    void sendInfoToMasters();
    void sendPubMessageToMasters();
    Angel::EventLoop *loop() { return _loop; }
    Alice::Server& server() { return _server; }
    void start() 
    {  
        _server.start();
        init();
    }
private:
    Angel::EventLoop *_loop;
    Alice::Server _server;
    uint64_t _currentEpoch;
    SentinelInstance::SentinelInstanceMap *_masters;
};

extern Alice::Sentinel *g_sentinel;

}

#endif // _ALICE_SRC_SENTINEL_H
