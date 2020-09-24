//
// 提供一个简易的C++ SYNC API
//

#include <angel/sockops.h>
#include <angel/inet_addr.h>
#include <angel/util.h>

#include <stdarg.h>
#include <unistd.h>
#include <fcntl.h>

#include <iostream>

#include "client.h"

void AliceClient::send_request()
{
    std::string message;
    message += "*";
    message += alice::i2s(argv.size());
    message += "\r\n";
    for (auto& it : argv) {
        message += "$";
        message += alice::i2s(it.size());
        message += "\r\n";
        message += it;
        message += "\r\n";
    }
    argv.clear();
    alice::fwrite(_fd, message.data(), message.size());
}

void AliceClient::executor(const char *fmt, ...)
{
    va_list ap, ap1;
    va_start(ap, fmt);
    va_copy(ap1, ap);
    // 获取可变参数表的长度
    // 当参数太长时，可能会有一定的性能损失
    int len = vsnprintf(nullptr, 0, fmt, ap1);
    va_end(ap1);
    // 获取格式化后的参数
    std::string buf(len + 1, 0);
    vsnprintf(&buf[0], buf.size(), fmt, ap);
    va_end(ap);
    buf[len] = '\n';
    alice::parse_line(argv, &buf[0], &buf[0] + len + 1);
    if (argv.empty()) return;
    send_request();
    recv_response();
    if (protocolerr)
        error = "protocol error";
}

void AliceClient::parse_status_reply()
{
next:
    char *s = _buf.peek();
    int crlf = _buf.find_crlf();
    if (crlf < 0) { read(); goto next; }
    if (s[0] == '-') {
        error.assign(s + 1, crlf);
    } else {
        reply.emplace_back(s + 1, crlf);
    }
    _buf.retrieve(crlf + 2);
}

void AliceClient::parse_integer_reply()
{
next:
    char *s = _buf.peek();
    int crlf = _buf.find_crlf();
    if (crlf < 0) { read(); goto next; }
    reply.emplace_back(s + 1, crlf);
    _buf.retrieve(crlf + 2);
}

void AliceClient::parse_bulk_reply()
{
next:
    char *s = _buf.peek();
    const char *es = s + _buf.readable();
    const char *ps = s;
    int crlf = _buf.find_crlf();
    if (crlf < 0) { read(); goto next; }
    int len = atoi(s + 1);
    if (len < 0 && len != -1) goto protocolerr;
    if (len == 0 || len == -1) {
        reply.emplace_back("(nil)");
        _buf.retrieve(crlf + 2);
        return;
    }
    s += crlf + 2;
    if (es - s < len + 2) { read(); goto next; }
    reply.emplace_back(s, len);
    _buf.retrieve(s + len + 2 - ps);
    return;
protocolerr:
    protocolerr = true;
}

void AliceClient::parse_multi_bulk_reply()
{
next:
    char *s = _buf.peek();
    int i = _buf.find_crlf();
    if (i < 0) { read(); goto next; }
    int len = atoi(s + 1);
    if (len < 0 && len != -1) goto protocolerr;
    if (len == 0 || len == -1) {
        if (len == 0) reply.emplace_back("(empty array)");
        else reply.emplace_back("(nil)");
        _buf.retrieve(i + 2);
        return;
    }
    s += i + 2;
    _buf.retrieve(i + 2);
    while (len-- > 0) {
        switch (s[0]) {
        case '+': case '-': parse_status_reply(); break;
        case ':': parse_integer_reply(); break;
        case '$': parse_bulk_reply(); break;
        case '*': parse_multi_bulk_reply(); break;
        default: goto protocolerr;
        }
        if (protocolerr) return;
        if (len > 0 && _buf.readable() == 0) read();
        s = _buf.peek();
    }
    return;
protocolerr:
    protocolerr = true;
}

void AliceClient::recv_response()
{
    reply.clear();
    protocolerr = false;
    error.clear();
    read();
next:
    char *s = _buf.peek();
    switch (s[0]) {
    case '+': case '-': parse_status_reply(); break;
    case ':': parse_integer_reply(); break;
    case '$': parse_bulk_reply(); break;
    case '*': parse_multi_bulk_reply(); break;
    default: protocolerr = true; break;
    }
    if (protocolerr) return;
    if (_buf.readable() > 0) goto next;
}

void AliceClient::connect(const std::string& ip, int port)
{
    _fd = angel::sockops::socket();
    int ret = angel::sockops::connect(_fd, &angel::inet_addr(ip, port).addr());
    if (ret < 0) {
        error = angel::util::strerrno();
    }
}

void AliceClient::close()
{
    ::close(_fd);
}
