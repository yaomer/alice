#ifndef _ALICE_CLIENT_H
#define _ALICE_CLIENT_H

#include <angel/buffer.h>

#include "../src/util.h"

class AliceClient {
public:
    void connect(const std::string& ip, int port);
    void executor(const char *fmt, ...);
    const alice::argv_t& get_reply() const { return reply; }
    bool isok() const { return error.empty(); }
    const std::string& get_error() { return error; }
    void close();
private:
    void send_request();
    void parse_status_reply();
    void parse_integer_reply();
    void parse_bulk_reply();
    void parse_multi_bulk_reply();
    void recv_response();
    void read() { _buf.read_fd(_fd); }

    alice::argv_t argv;
    alice::argv_t reply;
    std::string error;
    angel::buffer _buf;
    int _fd = -1;
    bool protocolerr = false;
};

#endif // _ALICE_CLIENT_H
