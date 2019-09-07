//
// 提供一个简易的C++ SYNC API
//
#include <Angel/SockOps.h>
#include <Angel/InetAddr.h>
#include <Angel/util.h>
#include <Angel/Buffer.h>
#include <stdarg.h>
#include <unistd.h>
#include <fcntl.h>
#include <iostream>
#include <string>
#include "alice.h"
#include "util.h"

using namespace Alice;

void AliceContext::sendRequest()
{
    std::string message;
    message += "*";
    message += convert(_argv.size());
    message += "\r\n";
    for (auto& it : _argv) {
        message += "$";
        message += convert(it.size());
        message += "\r\n";
        message += it;
        message += "\r\n";
    }
    _argv.clear();
    size_t remainBytes = message.size();
    const char *data = message.data();
    while (remainBytes > 0) {
        ssize_t n = write(_fd, data, remainBytes);
        remainBytes -= n;
        data += n;
    }
}

void AliceContext::executor(const char *fmt, ...)
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
    parseLine(_argv, &buf[0], &buf[0] + len + 1);
    if (_argv.empty()) return;
    sendRequest();
    recvResponse();
}

void AliceContext::parseStatusReply()
{
next:
    char *s = _buf.peek();
    int crlf = _buf.findCrlf();
    if (crlf < 0) { read(); goto next; }
    if (s[0] == '-') {
        _err = OTHER_ERR;
        _errStr.assign(s + 1, crlf);
    } else {
        _reply.emplace_back(s + 1, crlf);
    }
    _buf.retrieve(crlf + 2);
}

void AliceContext::parseIntegerReply()
{
next:
    char *s = _buf.peek();
    int crlf = _buf.findCrlf();
    if (crlf < 0) { read(); goto next; }
    _reply.emplace_back(s + 1, crlf);
    _buf.retrieve(crlf + 2);
}

void AliceContext::parseBulkReply()
{
next:
    char *s = _buf.peek();
    const char *es = s + _buf.readable();
    const char *ps = s;
    int crlf = _buf.findCrlf();
    if (crlf < 0) { read(); goto next; }
    int len = atoi(s + 1);
    if (len < 0 && len != -1) goto protocolerr;
    if (len == 0 || len == -1) {
        _reply.emplace_back("(nil)");
        _buf.retrieve(crlf + 2);
        return;
    }
    s += crlf + 2;
    if (es - s < len + 2) { read(); goto next; }
    _reply.emplace_back(s, len);
    _buf.retrieve(s + len + 2 - ps);
    return;
protocolerr:
    _err = PROTOCOL_ERR;
}

void AliceContext::parseMultiBulkReply()
{
next:
    char *s = _buf.peek();
    int i = _buf.findCrlf();
    if (i < 0) { read(); goto next; }
    int len = atoi(s + 1);
    if (len < 0 && len != -1) goto protocolerr;
    if (len == 0 || len == -1) {
        if (len == 0) _reply.emplace_back("(empty array)");
        else _reply.emplace_back("(nil)");
        _buf.retrieve(i + 2);
        return;
    }
    s += i + 2;
    _buf.retrieve(i + 2);
    while (len-- > 0) {
        switch (s[0]) {
        case '+': case '-': parseStatusReply(); break;
        case ':': parseIntegerReply(); break;
        case '$': parseBulkReply(); break;
        case '*': parseMultiBulkReply(); break;
        default: goto protocolerr;
        }
        if (_err == PROTOCOL_ERR) return;
        if (len > 0 && _buf.readable() == 0) read();
        s = _buf.peek();
    }
    return;
protocolerr:
    _err = PROTOCOL_ERR;
}

void AliceContext::recvResponse()
{
    _reply.clear();
    read();
next:
    char *s = _buf.peek();
    switch (s[0]) {
    case '+': case '-': parseStatusReply(); break;
    case ':': parseIntegerReply(); break;
    case '$': parseBulkReply(); break;
    case '*': parseMultiBulkReply(); break;
    default: _err = PROTOCOL_ERR; break;
    }
    if (_err == PROTOCOL_ERR) return;
    if (_buf.readable() > 0) goto next;
}

void AliceContext::connect(const char *ip, int port)
{
    _fd = Angel::SockOps::socket();
    int ret = Angel::SockOps::connect(
            _fd, &Angel::InetAddr(port, ip).inetAddr());
    if (ret < 0) {
        _err = CONNECT_ERR;
        _errStr.assign(Angel::strerrno());
    }
}

void AliceContext::close()
{
    ::close(_fd);
}
