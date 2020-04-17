#ifndef _ALICE_CLIENT_H
#define _ALICE_CLIENT_H

#include <Angel/Buffer.h>

#include <string>
#include <vector>

namespace Alice {

class AliceContext {
public:
    AliceContext()
        : _err(0),
        _fd(-1)
    {
    }
    enum {
        CONNECT_ERR = 1,
        PROTOCOL_ERR,
        OTHER_ERR,
    };
    using List = std::vector<std::string>;
    void connect(const char *ip, int port);
    void executor(const char *fmt, ...);
    List& reply() { return _reply; }
    int err() { return _err; }
    std::string& errStr() { return _errStr; }
    void close();
    // bool lock(const std::string& key);
    // void release(const std::string& key);
private:
    void sendRequest();
    void parseStatusReply();
    void parseIntegerReply();
    void parseBulkReply();
    void parseMultiBulkReply();
    void recvResponse();
    void read() { _buf.readFd(_fd); }

    List _reply;
    int _err;
    std::string _errStr;
    List _argv;
    int _fd;
    Angel::Buffer _buf;
};
}

#endif // _ALICE_CLIENT_H
