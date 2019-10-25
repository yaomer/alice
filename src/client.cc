#include <Angel/Buffer.h>
#include <linenoise.h>
#include <unistd.h>
#include <iostream>
#include <map>

#include "client.h"
#include "util.h"

using namespace Alice;

void Client::send()
{
    size_t n = _argv.size();
    if (n == 0) return;
    _message += "*";
    _message += convert(n);
    _message += "\r\n";
    for (auto& it : _argv) {
        _message += "$";
        _message += convert(it.size());
        _message += "\r\n";
        _message += it;
        _message += "\r\n";
        if (strcasecmp(it.c_str(), "SUBSCRIBE") == 0)
            _flag = PUBSUB;
    }
    _argv.clear();
    writeToFile(_fd, _message.data(), _message.size());
    _message.clear();
}

// 解析状态回复和错误回复
// +OK\r\n
// -ERR syntax error\r\n
void Client::parseStatusReply()
{
next:
    char *s = _buf.peek();
    int crlf = _buf.findCrlf();
    if (crlf < 0) { read(); goto next; }
    if (s[0] == '-') std::cout << "(error) ";
    std::cout << std::string(s + 1, crlf) << "\n";
    _buf.retrieve(crlf + 2);
}

// 解析整数回复 :1\r\n
void Client::parseIntegerReply()
{
next:
    char *s = _buf.peek();
    int crlf = _buf.findCrlf();
    if (crlf < 0) { read(); goto next; }
    std::cout << "(integer) " << std::string(s + 1, crlf) << "\n";
    _buf.retrieve(crlf + 2);
}

// 解析批量回复
// hello -> $5\r\nhello\r\n
// $-1\r\n表示请求的值不存在，此时客户端会返回nil
void Client::parseBulkReply()
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
        std::cout << "(nil)\n";
        _buf.retrieve(crlf + 2);
        return;
    }
    s += crlf + 2;
    if (es - s < len + 2) { read(); goto next; }
    std::cout << "\"";
    for (int i = 0; i < len; i++) {
        if (s[i]) std::cout << s[i];
        else std::cout << "\\x00";
    }
    std::cout << "\"\n";
    _buf.retrieve(s + len + 2 - ps);
    return;
protocolerr:
    _flag = PROTOCOLERR;
}

// 解析多批量回复
// set key value -> *3\r\n$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
// *0\r\n表示一个空白多批量回复，此时客户端会返回一个空数组
// *-1\r\n则表示一个无内容多批量回复，此时客户端会返回nil
void Client::parseMultiBulkReply()
{
next:
    char *s = _buf.peek();
    int i = _buf.findCrlf();
    int j = 1;
    if (i < 0) { read(); goto next; }
    int len = atoi(s + 1);
    if (len < 0 && len != -1) goto protocolerr;
    if (len == 0 || len == -1) {
        if (len == 0) std::cout << "(empty array)\n";
        else std::cout << "(nil)\n";
        _buf.retrieve(i + 2);
        return;
    }
    s += i + 2;
    _buf.retrieve(i + 2);
    while (len-- > 0) {
        std::cout << j++ << ") ";
        switch (s[0]) {
        case '+': case '-': parseStatusReply(); break;
        case ':': parseIntegerReply(); break;
        case '$': parseBulkReply(); break;
        case '*': parseMultiBulkReply(); break;
        default: goto protocolerr;
        }
        if (_flag == PROTOCOLERR) return;
        if (len > 0 && _buf.readable() == 0) read();
        s = _buf.peek();
    }
    return;
protocolerr:
    _flag = PROTOCOLERR;
}

void Client::parseResponse()
{
    read();
next:
    char *s = _buf.peek();
    switch (s[0]) {
    case '+': case '-': parseStatusReply(); break;
    case ':': parseIntegerReply(); break;
    case '$': parseBulkReply(); break;
    case '*': parseMultiBulkReply(); break;
    default: _flag = PROTOCOLERR; return;
    }
    if (_flag == PROTOCOLERR) return;
    if (_buf.readable() > 0) goto next;
}

void completion(const char *buf, linenoiseCompletions *lc);
char *hints(const char *buf, int *color, int *bold);

int main(int argc, char *argv[])
{
    int port = 1296;
    if (argv[1]) port = atoi(argv[1]);
    Alice::Client client;
    client.connect("127.0.0.1", port);

    linenoiseSetCompletionCallback(completion);
    linenoiseSetHintsCallback(hints);

    char *line = nullptr;
    while ((line = linenoise("Alice>> "))) {
        size_t len = strlen(line);
        line[len] = '\n';
        int ret = parseLine(client.argv(), line, line + len + 1);
        if (ret < 0) {
            std::cout << "input error\n";
            continue;
        }
        client.send();
        client.parseResponse();
        if (client.flag() == Alice::Client::PUBSUB) {
            while (true)
                pause();
        }
        if (client.flag() == Client::PROTOCOLERR) {
            std::cout << "protocol error\n";
            abort();
        }
        line[len] = '\0';
        linenoiseHistoryAdd(line);
        linenoiseFree(line);
    }
}
