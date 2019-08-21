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
    size_t n = _argv.size();
    if (n == 0) return;
    std::string message;
    message += "*";
    message += convert(n);
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
    va_list ap;
    va_start(ap, fmt);
    char buf[1024];
    vsnprintf(buf, sizeof(buf) - 1, fmt, ap);
    size_t len = strlen(buf);
    buf[len] = '\n';
    parseLine(_argv, buf, buf + len + 1);
    sendRequest();
    recvResponse();
    va_end(ap);
}

void AliceContext::recvResponse()
{
    _reply.clear();
    Angel::Buffer buf;
wait:
    buf.readFd(_fd);
    char *s = buf.peek();
    switch (s[0]) {
    case '+':
    case '-': {
        int crlf = buf.findCrlf();
        if (crlf < 0) goto wait;
        if (s[0] == '-') {
            _err = OTHER_ERR;
            _errStr.assign(s + 1, crlf);
        } else {
            _reply.push_back(std::string(s + 1, crlf));
        }
        buf.retrieve(crlf + 2);
        break;
    }
    case '$': {
        const char *ps = s;
        int i = buf.findStr(s, "\r\n");
        if (i < 0) goto wait;
        size_t len = atoi(s + 1);
        if (len == 0) goto protocolerr;
        s += i + 2;
        i = buf.findStr(s, "\r\n");
        if (i < 0) goto wait;
        if (i != len) goto protocolerr;
        _reply.push_back(std::string(s, len));
        buf.retrieve(s + i + 2 - ps);
        break;
    }
    case '*': {
        char *ps = s;
        int i = buf.findStr(s, "\r\n");
        if (i < 0) goto wait;
        size_t len = atoi(s + 1);
        if (len == 0) goto protocolerr;
        s += i + 2;
        int j = 1;
        while (len > 0) {
            i = buf.findStr(s, "\r\n");
            if (i < 0) buf.readFd(_fd);
            if (s[0] != '$') goto protocolerr;
            size_t l = atoi(s + 1);
            if (l == 0) goto protocolerr;
            s += i + 2;
            i = buf.findStr(s, "\r\n");
            if (i < 0) buf.readFd(_fd);
            if (i != l) goto protocolerr;
            _reply.push_back(std::string(s, l));
            s += i + 2;
            len--;
            j++;
        }
        if (len == 0) {
            buf.retrieve(s - ps);
        }
        break;
    }
    case ':': {
        int i = buf.findStr(s, "\r\n");
        if (i < 0) goto wait;
        _reply.push_back(std::string(s + 1, i - 1));
        buf.retrieve(i + 2);
        break;
    }
    }
protocolerr:
    _err = PROTOCOL_ERR;
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
