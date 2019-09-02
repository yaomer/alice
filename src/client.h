#ifndef _ALICE_SRC_CLIENT_H
#define _ALICE_SRC_CLIENT_H

#include <Angel/SockOps.h>
#include <Angel/InetAddr.h>

#include <vector>
#include <string>

namespace Alice {

class Client {
public:
    enum Flag {
        PROTOCOLERR = 1,
        PUBSUB,
    };
    using Arglist = std::vector<std::string>;
    Client()
        : _fd(-1),
        _flag(0)
    {
    }
    void parseResponse();
    void send();
    void connect(const char *ip, int port)
    {
        _fd = Angel::SockOps::socket();
        int ret = Angel::SockOps::connect(
                _fd, &Angel::InetAddr(port, ip).inetAddr());
        if (ret < 0) {
            perror("connect:");
            abort();
        }
    }
    int flag() const { return _flag; }
    Arglist& argv() { return _argv; }
private:
    void parseStatusReply();
    void parseIntegerReply();
    void parseBulkReply();
    void parseMultiBulkReply();
    void read() { _buf.readFd(_fd); }

    int _fd;
    Arglist _argv;
    int _flag;
    Angel::Buffer _buf;
    std::string _message;
};
}

#endif
