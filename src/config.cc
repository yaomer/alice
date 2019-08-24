#include <stdio.h>
#include <vector>
#include <string>
#include "config.h"

using namespace Alice;

namespace Alice {

    using ConfParamList = std::vector<std::vector<std::string>>;
    struct ServerConf g_server_conf;
    struct SentinelConf g_sentinel_conf;
}

static void parseConf(ConfParamList& confParamList, const char *filename)
{
    FILE *fp = fopen(filename, "r");
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
            confParamList.push_back(std::move(param));
            continue;
        }
        param.push_back(std::string(s, p - s));
        s = p + 1;
        goto next;
    }
    fclose(fp);
}

static void error(const char *s)
{
    fprintf(stderr, "config error: %s\n", s);
    abort();
}

static ssize_t humanSizeToBytes(const char *s)
{
    const char *es = s + strlen(s);
    const char *p = std::find_if(s, es, [](char c){ return !isnumber(c); });
    if (p == s || p == es) return -1;
    ssize_t bytes = atoll(s);
    if (strcasecmp(p, "GB") == 0)
        bytes *= 1024 * 1024 * 1024;
    else if (strcasecmp(p, "MB") == 0)
        bytes *= 1024 * 1024;
    else if (strcasecmp(p, "KB") == 0)
        bytes *= 1024;
    return bytes;
}

void Alice::readServerConf()
{
    ConfParamList paramlist;
    parseConf(paramlist, "alice.conf");
    for (auto& it : paramlist) {
        if (strcasecmp(it[0].c_str(), "port") == 0) {
            g_server_conf.port = atoi(it[1].c_str());
        } else if (strcasecmp(it[0].c_str(), "ip") == 0) {
            g_server_conf.addr.assign(it[1]);
        } else if (strcasecmp(it[0].c_str(), "databases") == 0) {
            g_server_conf.databases = atoi(it[1].c_str());
        } else if (strcasecmp(it[0].c_str(), "save") == 0) {
            g_server_conf.save_params.push_back(
                    std::make_tuple(atol(it[1].c_str()), atol(it[2].c_str())));
        } else if (strcasecmp(it[0].c_str(), "appendonly") == 0) {
            if (strcasecmp(it[1].c_str(), "yes") == 0)
                g_server_conf.enable_appendonly = true;
            else if (strcasecmp(it[1].c_str(), "no"))
                error("appendonly");
        } else if (strcasecmp(it[0].c_str(), "appendfsync") == 0) {
            if (strcasecmp(it[1].c_str(), "always") == 0)
                g_server_conf.aof_mode = AOF_ALWAYS;
            else if (strcasecmp(it[1].c_str(), "everysec") == 0)
                g_server_conf.aof_mode = AOF_EVERYSEC;
            else if (strcasecmp(it[1].c_str(), "no") == 0)
                g_server_conf.aof_mode = AOF_NO;
            else
                error("appendfsync");
        } else if (strcasecmp(it[0].c_str(), "repl-timeout") == 0) {
            g_server_conf.repl_timeout = atoi(it[1].c_str()) * 1000;
        } else if (strcasecmp(it[0].c_str(), "repl-ping-period") == 0) {
            g_server_conf.repl_ping_preiod = atoi(it[1].c_str()) * 1000;
        } else if (strcasecmp(it[0].c_str(), "repl-backlog-size") == 0) {
            ssize_t size = humanSizeToBytes(it[1].c_str());
            if (size < 0) error("repl-backlog-size");
            g_server_conf.repl_backlog_size = size;
        }
    }
}

void Alice::readSentinelConf()
{
    ConfParamList paramlist;
    parseConf(paramlist, "sentinel.conf");
    for(auto& it : paramlist) {
        if (strcasecmp(it[1].c_str(), "ip") == 0) {
            g_sentinel_conf.addr = it[2];
        } else if (strcasecmp(it[1].c_str(), "port") == 0) {
            g_sentinel_conf.port = atoi(it[2].c_str());
        } else if (strcasecmp(it[1].c_str(), "monitor") == 0) {
            SentinelInstance master;
            master.setFlag(SentinelInstance::MASTER);
            master.setName(it[2]);
            master.setInetAddr(Angel::InetAddr(atoi(it[4].c_str()), it[3].c_str()));
            master.setQuorum(atoi(it[5].c_str()));
            g_sentinel_conf.masters[master.name()] = std::move(master);
        } else if (strcasecmp(it[1].c_str(), "down-after-milliseconds") == 0) {
            auto master = g_sentinel_conf.masters.find(it[2]);
            if (master == g_sentinel_conf.masters.end())
                error("down-after-milliseconds");
            master->second.setDownAfterPeriod(atoll(it[3].c_str()));
        }
    }
}
