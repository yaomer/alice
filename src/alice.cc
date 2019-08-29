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
        int len = atoi(s + 1);
        if (len != -1 && len <= 0) goto protocolerr;
        if (len == -1) {
            _reply.push_back(std::string("(nil)"));
            buf.retrieve(i + 2);
            break;
        }
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
        while (len > 0) {
            i = buf.findStr(s, "\r\n");
            if (i < 0) buf.readFd(_fd);
            if (s[0] != '$') goto protocolerr;
            int l = atoi(s + 1);
            if (l != -1 && l <= 0) goto protocolerr;
            if (l == -1) {
                _reply.push_back(std::string("(nil)"));
                s += i + 2;
                len--;
                continue;
            }
            s += i + 2;
            i = buf.findStr(s, "\r\n");
            if (i < 0) buf.readFd(_fd);
            if (i != l) goto protocolerr;
            _reply.push_back(std::string(s, l));
            s += i + 2;
            len--;
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
