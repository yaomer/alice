#include <Angel/EventLoopThread.h>
#include <Angel/TcpClient.h>
#include <linenoise.h>
#include <unistd.h>
#include <map>
#include "client.h"

using namespace Alice;

void Client::parseLine(const char *line, const char *linep)
{
    const char *next = nullptr;
    do {
        line = std::find_if(line, linep, [](char c){ return !isspace(c); });
        if (line == linep) break;
        if (*line == '\"') {
            line += 1;
            next = std::find(line, linep, '\"');
        } else {
            next = std::find_if(line, linep, [](char c){ return isspace(c); });
        }
        if (next == linep) break;
        _argv.push_back(std::string(line, next - line));
        line = next + 1;
    } while (1);
}

void Client::send()
{
    char buf[32];
    size_t n = _argv.size();
    if (n == 0) return;
    snprintf(buf, sizeof(buf), "%zu", n);
    _message += "*";
    _message += buf;
    _message += "\r\n";
    for (auto& it : _argv) {
        snprintf(buf, sizeof(buf), "%zu", it.size());
        _message += "$";
        _message += buf;
        _message += "\r\n";
        _message += it;
        _message += "\r\n";
    }
    _argv.clear();
    _client.conn()->send(_message);
    // std::cout << _message;
    _message.clear();
}

namespace Alice {

    thread_local char convert_buf[32];

    const char *convert(int64_t value)
    {
        snprintf(convert_buf, sizeof(convert_buf), "%lld", value);
        return convert_buf;
    }
}

void Client::parseResponse(Angel::Buffer& buf)
{
    const char *s = buf.peek();
    const char *es = buf.peek() + buf.readable();
    std::string answer;
    switch (s[0]) {
    case '+':
    case '-': {
        int crlf = buf.findCrlf();
        if (crlf < 0) return;
        answer.assign(s + 1, crlf);
        if (s[0] == '-') std::cout << "(error) ";
        buf.retrieve(crlf + 2);
        std::cout << answer << "\n";
        break;
    }
    case '$': {
        const char *ps = s;
        const char *next = std::find(s, es, '\r');
        if (next == es) return;
        s += 1;
        size_t len = atoi(s);
        if (len == 0) return;
        s = next + 2;
        next = std::find(s, es, '\r');
        if (next == es) return;
        if (next - s != len) return;
        answer.assign(s, len);
        buf.retrieve(next + 2 - ps);
        std::cout << "\"" << answer << "\"\n";
        break;
    }
    case '*': {
        const char *ps = s;
        const char *next = std::find(s, es, '\r');
        if (next == es) return;
        if (next[1] != '\n') return;
        s += 1;
        size_t len = atoi(s);
        if (len == 0) {
            answer.assign("(nil)");
            std::cout << answer << "\n";
            buf.retrieve(next + 2 - ps);
            return;
        }
        s = next + 2;
        int i = 1;
        while (len > 0) {
            if (s[0] != '$') break;
            next = std::find(s, es, '\r');
            if (next == es) return;
            s += 1;
            size_t l = atoi(s);
            if (l == 0) return;
            s = next + 2;
            next = std::find(s, es, '\r');
            if (next == es) return;
            if (next - s != l) return;
            answer.append(convert(i));
            answer.append(") \"");
            answer.append(std::string(s, l));
            answer.append("\"\n");
            s = next + 2;
            len--;
            i++;
        }
        if (len == 0) {
            std::cout << answer;
            buf.retrieve(next + 2 - ps);
        }
        break;
    }
    case ':': {
        const char *next = std::find(s, es, '\r');
        s += 1;
        if (next == es) return;
        answer.assign(s, next - s);
        std::cout << "(integer)" << answer << "\n";
        buf.retrieve(next + 2 - (s - 1));
        break;
    }
    }
}

void completion(const char *buf, linenoiseCompletions *lc);
char *hints(const char *buf, int *color, int *bold);

int main()
{
    Angel::EventLoopThread t_loop;
    Angel::InetAddr connAddr(8000, "127.0.0.1");
    Alice::Client client(t_loop.getLoop(), connAddr);
    client.start();

    linenoiseSetCompletionCallback(completion);
    linenoiseSetHintsCallback(hints);

    char *line = nullptr;
    while ((line = linenoise("Alice>> "))) {
        size_t len = strlen(line);
        line[len] = '\n';
        client.parseLine(line, line + len + 1);
        client.send();
        usleep(200000);
        linenoiseHistoryAdd(line);
        linenoiseFree(line);
    }
}
