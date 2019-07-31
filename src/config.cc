#include <stdio.h>
#include <vector>
#include <string>
#include "server.h"

using namespace Alice;

void Server::parseConf()
{
    // save:1:100
    FILE *fp = fopen("alice.conf", "r");
    char buf[1024];
    while (fgets(buf, sizeof(buf), fp)) {
        const char *s = buf;
        const char *es = buf + strlen(buf);
        std::vector<std::string> param;
        s = std::find_if(s, es, [](char c){ return !isspace(c); });
        if (s == es || s[0] == '#') continue;
next:
        const char *p = std::find(s, es, ':');
        if (p == es)  {
            p = std::find_if(s, es, isspace);
            if (p == es) continue;
            param.push_back(std::string(s, p - s));
            _confParamList.push_back(std::move(param));
            continue;
        }
        param.push_back(std::string(s, p - s));
        s = p + 1;
        goto next;
    }
    fclose(fp);
}

void Server::readConf()
{
    parseConf();
    for (auto& it : _confParamList) {
        if (strncasecmp(it[0].c_str(), "save", 4) == 0) {
            _dbServer.addSaveParam(atoi(it[1].c_str()), atoi(it[2].c_str()));
        } else if (strncasecmp(it[0].c_str(), "appendonly", 10) == 0) {
            if (strncasecmp(it[1].c_str(), "yes", 3) == 0)
                _dbServer.setFlag(DBServer::APPENDONLY);
        } else if (strncasecmp(it[0].c_str(), "appendfsync", 11) == 0) {
            if (strncasecmp(it[1].c_str(), "always", 6) == 0)
                _dbServer.aof()->setMode(Aof::ALWAYS);
            else if (strncasecmp(it[1].c_str(), "everysec", 8) == 0)
                _dbServer.aof()->setMode(Aof::EVERYSEC);
            else if (strncasecmp(it[1].c_str(), "no", 2) == 0)
                _dbServer.aof()->setMode(Aof::NO);
        }
    }
}

