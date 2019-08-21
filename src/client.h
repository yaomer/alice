#ifndef _ALICE_SRC_CLIENT_H
#define _ALICE_SRC_CLIENT_H

#include <Angel/EventLoop.h>
#include <Angel/TcpClient.h>

#include <vector>
#include <string>

using std::placeholders::_1;
using std::placeholders::_2;

namespace Alice {

class Client {
public:
    enum State {
        NOENOUGH = 1,
        PARSEERR,
    };
    enum Flag {
        PUBSUB = 0x01,
    };
    using Arglist = std::vector<std::string>;
    Client(Angel::EventLoop *loop, Angel::InetAddr& inetAddr)
        : _loop(loop),
        _client(loop, inetAddr, "Alice"),
        _state(0),
        _flag(0)
    {
        _client.setMessageCb(
                std::bind(&Client::onMessage, this, _1, _2));
    }
    void onMessage(const Angel::TcpConnectionPtr& conn, Angel::Buffer& buf)
    {
        while (buf.readable() > 0) {
            parseResponse(buf);
            if (_state == NOENOUGH || _state == PARSEERR) {
                _state = 0;
                break;
            }
        }
    }
    void parseResponse(Angel::Buffer& buf);
    void send();
    void start() { _client.start(); }
    int flag() const { return _flag; }
    Arglist& argv() { return _argv; }
private:
    Angel::EventLoop *_loop;
    Angel::TcpClient _client;
    Arglist _argv;
    std::string _message;
    int _state;
    int _flag;
};
}

#endif
