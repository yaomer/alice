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
    const char *s = buf.peek();
    const char *es = buf.peek() + buf.readable();
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
        const char *next = std::find(s, es, '\r');
        if (next == es) goto wait;
        s += 1;
        size_t len = atoi(s);
        if (len == 0) {
            _err = PROTOCOL_ERR;
            return;
        }
        s = next + 2;
        next = std::find(s, es, '\r');
        if (next == es) goto wait;
        if (next - s != len) {
            _err = PROTOCOL_ERR;
            return;
        }
        _reply.push_back(std::string(s, len));
        buf.retrieve(next + 2 - ps);
        break;
    }
    case '*': {
        const char *ps = s;
        const char *next = std::find(s, es, '\r');
        if (next == es) goto wait;
        if (next[1] != '\n') {
            _err = PROTOCOL_ERR;
            return;
        }
        s += 1;
        size_t len = atoi(s);
        if (len == 0) {
            _reply.push_back(std::string("(nil)"));
            buf.retrieve(next + 2 - ps);
            return;
        }
        s = next + 2;
        int i = 1;
        while (len > 0) {
            if (s[0] != '$' && s[0] != '+') {
                _err = PROTOCOL_ERR;
                return;
            };
            next = std::find(s, es, '\r');
            if (next == es) {
                buf.readFd(_fd);
            }
            if (s[0] == '+') {
                s += 1;
                _reply.push_back(std::string(s, next - s));
                s = next + 2;
                len--;
                i++;
                continue;
            }
            s += 1;
            size_t l = atoi(s);
            if (l == 0) {
                _err = PROTOCOL_ERR;
                return;
            };
            s = next + 2;
            next = std::find(s, es, '\r');
            if (next == es) {
                buf.readFd(_fd);
            }
            if (next - s != l) {
                _err = PROTOCOL_ERR;
                return;
            }
            _reply.push_back(std::string(s, l));
            s = next + 2;
            len--;
            i++;
        }
        if (len == 0) {
            buf.retrieve(next + 2 - ps);
        }
        break;
    }
    case ':': {
        const char *next = std::find(s, es, '\r');
        s += 1;
        if (next == es) goto wait;
        _reply.push_back(std::string(s, next - s));
        buf.retrieve(next + 2 - (s - 1));
        break;
    }
    }
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
