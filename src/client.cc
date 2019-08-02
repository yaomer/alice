#include <Angel/EventLoopThread.h>
#include <Angel/TcpClient.h>
#include <linenoise.h>
#include <unistd.h>
#include <map>
#include "client.h"

using namespace Alice;

int Client::parseLine(const char *line, const char *linep)
{
    const char *start;
    do {
        line = std::find_if(line, linep, [](char c){ return !isspace(c); });
        if (line == linep) break;
        if (*line == '\"') {
            start = ++line;
search:
            line = std::find(line, linep, '\"');
            if (line == linep) goto err;
            if (line[-1] == '\\') {
                line++;
                goto search;
            }
            if (!isspace(line[1])) goto err;
            _argv.push_back(std::string(start, line - start));
            line++;
        } else {
            start = line;
            line = std::find_if(line, linep, [](char c){ return isspace(c); });
            if (line == linep) goto err;
            _argv.push_back(std::string(start, line - start));
        }
    } while (1);
    return 0;
err:
    _argv.clear();
    return -1;
}

namespace Alice {

    thread_local char convert_buf[32];

    const char *convert(int64_t value)
    {
        snprintf(convert_buf, sizeof(convert_buf), "%lld", value);
        return convert_buf;
    }
}

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
    }
    _argv.clear();
    _client.conn()->send(_message);
    _message.clear();
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
            if (s[0] != '$' && s[0] != '+') break;
            next = std::find(s, es, '\r');
            if (next == es) return;
            if (s[0] == '+') {
                s += 1;
                answer.append(convert(i));
                answer.append(") ");
                answer.append(std::string(s, next - s));
                answer.append("\n");
                s = next + 2;
                len--;
                i++;
                continue;
            }
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

int main(int argc, char *argv[])
{
    if (argc != 2) {
        fprintf(stderr, "./cli [port]\n");
        return 1;
    }
    Angel::EventLoopThread t_loop;
    Angel::InetAddr connAddr(atoi(argv[1]), "127.0.0.1");
    Alice::Client client(t_loop.getLoop(), connAddr);
    client.start();

    linenoiseSetCompletionCallback(completion);
    linenoiseSetHintsCallback(hints);

    char *line = nullptr;
    while ((line = linenoise("Alice>> "))) {
        size_t len = strlen(line);
        line[len] = '\n';
        int ret = client.parseLine(line, line + len + 1);
        if (ret < 0) {
            std::cout << "input error\n";
        }
        client.send();
        line[len] = '\0';
        usleep(200000);
        linenoiseHistoryAdd(line);
        linenoiseFree(line);
    }
}
