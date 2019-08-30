#include <Angel/EventLoopThread.h>
#include <Angel/TcpClient.h>
#include <linenoise.h>
#include <unistd.h>
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
            _flag |= PUBSUB;
    }
    _argv.clear();
    _client.conn()->send(_message);
    _message.clear();
}

void Client::parseResponse(Angel::Buffer& buf)
{
    char *s = buf.peek();
    std::string answer;
    switch (s[0]) {
    case '+':
    case '-': {
        int crlf = buf.findCrlf();
        if (crlf < 0) goto noenough;
        answer.assign(s + 1, crlf);
        if (s[0] == '-') std::cout << "(error) ";
        buf.retrieve(crlf + 2);
        std::cout << answer << "\n";
        break;
    }
    case '$': {
        const char *ps = s;
        int i = buf.findStr(s, "\r\n");
        if (i < 0) goto noenough;
        int len = atoi(s + 1);
        if (len != -1 && len <= 0) goto protocolerr;
        if (len == -1) {
            std::cout << "(nil)\n";
            buf.retrieve(i + 2);
            break;
        }
        s += i + 2;
        i = buf.findStr(s, "\r\n");
        if (i < 0) goto noenough;
        if (i != len) goto protocolerr;
        answer.assign(s, len);
        buf.retrieve(s + i + 2 - ps);
        std::cout << "\"" << answer << "\"\n";
        break;
    }
    case '*': {
        const char *ps = s;
        int i = buf.findStr(s, "\r\n");
        if (i < 0) goto noenough;
        size_t len = atoi(s + 1);
        if (len == 0) goto protocolerr;
        s += i + 2;
        int j = 1;
        while (len > 0) {
            i = buf.findStr(s, "\r\n");
            if (i < 0) goto noenough;
            if (s[0] != '$') goto protocolerr;
            int l = atoi(s + 1);
            if (l != -1 && l <= 0) goto protocolerr;
            if (l == -1) {
                answer.append(convert(j));
                answer.append(") (nil)\n");
                s += i + 2;
                len--;
                j++;
                continue;
            }
            s += i + 2;
            i = buf.findStr(s, "\r\n");
            if (i < 0) goto noenough;
            if (i != l) goto protocolerr;
            answer.append(convert(j));
            answer.append(") \"");
            answer.append(std::string(s, l));
            answer.append("\"\n");
            s += i + 2;
            len--;
            j++;
        }
        if (len == 0) {
            std::cout << answer;
            buf.retrieve(s - ps);
        }
        break;
    }
    case ':': {
        int i = buf.findStr(s, "\r\n");
        if (i < 0) goto noenough;
        answer.assign(s + 1, i - 1);
        std::cout << "(integer) " << answer << "\n";
        buf.retrieve(i + 2);
        break;
    }
    }
    return;
noenough:
    _state = NOENOUGH;
    return;
protocolerr:
    _state = PROTOCOLERR;
    return;
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
        int ret = parseLine(client.argv(), line, line + len + 1);
        if (ret < 0) {
            std::cout << "input error\n";
        }
        client.send();
        if (client.flag() & Alice::Client::PUBSUB) {
            while (true)
                pause();
        }
        line[len] = '\0';
        usleep(200000);
        linenoiseHistoryAdd(line);
        linenoiseFree(line);
    }
}
