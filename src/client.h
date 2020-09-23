#ifndef _ALICE_SRC_CLIENT_H
#define _ALICE_SRC_CLIENT_H

#include <angel/sockops.h>
#include <angel/inet_addr.h>

#include <vector>
#include <string>

namespace alice {

class client {
public:
    enum Flag {
        PROTOCOLERR = 1,
        PUBSUB = 2,
    };
    using argv_t = std::vector<std::string>;
    explicit client(angel::inet_addr addr)
        : flags(0),
        conn_addr(addr),
        conn_fd(-1)
    {
        connect();
    }
    const char *host() const { return conn_addr.to_host(); }
    void parse_response();
    void send_request();
    void connect()
    {
        conn_fd = angel::sockops::socket();
        int ret = angel::sockops::connect(conn_fd, &conn_addr.addr());
        if (ret < 0) {
            perror("connect");
            exit(1);
        }
    }
    int flags;
    argv_t argv;
private:
    void parse_status_reply();
    void parse_integer_reply();
    void parse_bulk_reply();
    void parse_multi_bulk_reply();
    void read() { buf.read_fd(conn_fd); }

    angel::inet_addr conn_addr;
    int conn_fd;
    angel::buffer buf;
};
}

#endif
