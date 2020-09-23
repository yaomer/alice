#include <angel/buffer.h>

#include <linenoise.h>
#include <unistd.h>

#include <iostream>
#include <map>

#include "client.h"
#include "util.h"

using namespace alice;

void client::send_request()
{
    std::string message;
    if (argv.size() == 0) return;
    message += "*";
    message += i2s(argv.size());
    message += "\r\n";
    for (auto& it : argv) {
        message += "$";
        message += i2s(it.size());
        message += "\r\n";
        message += it;
        message += "\r\n";
        if (strcasecmp(it.c_str(), "SUBSCRIBE") == 0)
            flags |= PUBSUB;
    }
    fwrite(conn_fd, message.data(), message.size());
    argv.clear();
}

// 解析状态回复和错误回复
// +OK\r\n
// -ERR syntax error\r\n
void client::parse_status_reply()
{
next:
    char *s = buf.peek();
    int crlf = buf.find_crlf();
    if (crlf < 0) { read(); goto next; }
    if (s[0] == '-') std::cout << "(error) ";
    std::cout << std::string(s + 1, crlf) << "\n";
    buf.retrieve(crlf + 2);
}

// 解析整数回复 :1\r\n
void client::parse_integer_reply()
{
next:
    char *s = buf.peek();
    int crlf = buf.find_crlf();
    if (crlf < 0) { read(); goto next; }
    std::cout << "(integer) " << std::string(s + 1, crlf) << "\n";
    buf.retrieve(crlf + 2);
}

// 解析批量回复
// hello -> $5\r\nhello\r\n
// $-1\r\n表示请求的值不存在，此时客户端会返回nil
void client::parse_bulk_reply()
{
next:
    char *s = buf.peek();
    const char *es = s + buf.readable();
    const char *ps = s;
    int crlf = buf.find_crlf();
    if (crlf < 0) { read(); goto next; }
    int len = atoi(s + 1);
    if (len < 0 && len != -1) goto protocolerr;
    if (len == 0 || len == -1) {
        std::cout << "(nil)\n";
        buf.retrieve(crlf + 2);
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
    buf.retrieve(s + len + 2 - ps);
    return;
protocolerr:
    flags = PROTOCOLERR;
}

// 解析多批量回复
// set key value -> *3\r\n$3\r\nset\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
// *0\r\n表示一个空白多批量回复，此时客户端会返回一个空数组
// *-1\r\n则表示一个无内容多批量回复，此时客户端会返回nil
void client::parse_multi_bulk_reply()
{
next:
    char *s = buf.peek();
    int i = buf.find_crlf();
    int j = 1;
    if (i < 0) { read(); goto next; }
    int len = atoi(s + 1);
    if (len < 0 && len != -1) goto protocolerr;
    if (len == 0 || len == -1) {
        if (len == 0) std::cout << "(empty array)\n";
        else std::cout << "(nil)\n";
        buf.retrieve(i + 2);
        return;
    }
    s += i + 2;
    buf.retrieve(i + 2);
    while (len-- > 0) {
        std::cout << j++ << ") ";
        switch (s[0]) {
        case '+': case '-': parse_status_reply(); break;
        case ':': parse_integer_reply(); break;
        case '$': parse_bulk_reply(); break;
        case '*': parse_multi_bulk_reply(); break;
        default: goto protocolerr;
        }
        if (flags == PROTOCOLERR) return;
        if (len > 0 && buf.readable() == 0) read();
        s = buf.peek();
    }
    return;
protocolerr:
    flags = PROTOCOLERR;
}

void client::parse_response()
{
    read();
next:
    char *s = buf.peek();
    switch (s[0]) {
    case '+': case '-': parse_status_reply(); break;
    case ':': parse_integer_reply(); break;
    case '$': parse_bulk_reply(); break;
    case '*': parse_multi_bulk_reply(); break;
    default: flags = PROTOCOLERR; return;
    }
    if (flags == PROTOCOLERR) return;
    if (buf.readable() > 0) goto next;
}

void completion(const char *buf, linenoiseCompletions *lc);
char *hints(const char *buf, int *color, int *bold);

int main(int argc, char *argv[])
{
    int port = 1296;
    if (argv[1]) port = atoi(argv[1]);
    alice::client client(angel::inet_addr("127.0.0.1", port));

    linenoiseSetCompletionCallback(completion);
    linenoiseSetHintsCallback(hints);

    char *line = nullptr;
    std::string prompt;
    prompt.append(client.host()).append("> ");
    while ((line = linenoise(prompt.c_str()))) {
        size_t len = strlen(line);
        line[len] = '\n';
        int ret = parse_line(client.argv, line, line + len + 1);
        if (ret < 0) {
            std::cout << "input error\n";
            continue;
        }
        client.send_request();
        client.parse_response();
        if (client.flags == client::PUBSUB) {
            while (true)
                client.parse_response();
        }
        if (client.flags == client::PROTOCOLERR) {
            std::cout << "protocol error\n";
            exit(1);
        }
        line[len] = '\0';
        linenoiseHistoryAdd(line);
        linenoiseFree(line);
    }
}
