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
    size_t remainBytes = _message.size();
    const char *data = _message.data();
    while (remainBytes > 0) {
        ssize_t n = write(_fd, data, remainBytes);
        remainBytes -= n;
        data += n;
    }
    _message.clear();
}

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

void Client::parseIntegerReply()
{
next:
    char *s = _buf.peek();
    int crlf = _buf.findCrlf();
    if (crlf < 0) { read(); goto next; }
    std::cout << "(integer) " << std::string(s + 1, crlf) << "\n";
    _buf.retrieve(crlf + 2);
}

void Client::parseBulkReply()
{
next:
    char *s = _buf.peek();
    const char *ps = s;
    int crlf = _buf.findCrlf();
    if (crlf < 0) { read(); goto next; }
    int l = atoi(s + 1);
    if (l != -1 && l <= 0) goto protocolerr;
    if (l == -1) { std::cout << "(nil)\n"; return; }
    s += crlf + 2;
    crlf = _buf.findStr(s, "\r\n");
    if (crlf < 0) { read(); goto next; }
    if (crlf != l) goto protocolerr;
    std::cout << "\"" << std::string(s, l) << "\"\n";
    _buf.retrieve(s + l + 2 - ps);
    return;
protocolerr:
    _flag = PROTOCOLERR;
}

void Client::parseMultiBulkReply()
{
next:
    char *s = _buf.peek();
    int i = _buf.findCrlf();
    int j = 1;
    if (i < 0) { read(); goto next; }
    size_t len = atoi(s + 1);
    if (len == 0) goto protocolerr;
    s += i + 2;
    _buf.retrieve(i + 2);
    while (len > 0) {
then:
        const char *ps = s;
        i = _buf.findStr(s, "\r\n");
        if (i < 0) { read(); s = _buf.peek(); goto then; }
        if (s[0] != '$') goto protocolerr;
        int l = atoi(s + 1);
        if (l != -1 && l <= 0) goto protocolerr;
        s += i + 2;
        if (l == -1) {
            std::cout << j++ << ") (nil)\n";
            _buf.retrieve(i + 2);
            len--;
            continue;
        }
        i = _buf.findStr(s, "\r\n");
        if (i < 0) { read(); s = _buf.peek(); goto then; }
        if (i != l) goto protocolerr;
        std::cout << j++ << ") \"" << std::string(s, l) << "\"\n";
        s += i + 2;
        _buf.retrieve(s - ps);
        len--;
    }
    return;
protocolerr:
    _flag = PROTOCOLERR;
}

void Client::parseResponse()
{
    _buf.readFd(_fd);
    switch (_buf[0]) {
    case '+': case '-': parseStatusReply(); break;
    case ':': parseIntegerReply(); break;
    case '$': parseBulkReply(); break;
    case '*': parseMultiBulkReply(); break;
    default: _flag = PROTOCOLERR; break;
    }
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
        }
        client.send();
        if (client.flag() == Alice::Client::PUBSUB) {
            while (true)
                pause();
        }
        client.parseResponse();
        if (client.flag() == Client::PROTOCOLERR) {
            std::cout << "protocol error\n";
            abort();
        }
        line[len] = '\0';
        linenoiseHistoryAdd(line);
        linenoiseFree(line);
    }
}
