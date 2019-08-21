#ifndef _ALICE_SRC_ALICE_H
#define _ALICE_SRC_ALICE_H

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
private:
    void sendRequest();
    void recvResponse();

    List _reply;
    int _err;
    std::string _errStr;
    List _argv;
    int _fd;
};
}

#endif // _ALICE_SRC_ALICE_H
