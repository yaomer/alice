#include <stdio.h>
#include <vector>
#include <string>
#include "db.h"
#include "config.h"

using namespace Alice;

namespace Alice {

    using ConfParamList = std::vector<std::vector<std::string>>;
    struct ServerConf g_server_conf;
    struct SentinelConf g_sentinel_conf;
}

static void parseConf(ConfParamList& confParamList, const char *filename)
{
    char buf[1024];
    FILE *fp = fopen(filename, "r");
    if (!fp) {
        fprintf(stderr, "can't open %s\n", filename);
        abort();
    }
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
            param.emplace_back(s, p);
            confParamList.emplace_back(std::move(param));
            continue;
        }
        param.emplace_back(s, p);
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

#define ASSERT(expr, str) \
    do { if (!(expr)) error(str); } while (0)

static ssize_t humanSizeToBytes(const char *s)
{
    ssize_t bytes = atoll(s);
    const char *es = s + strlen(s);
    const char *p = std::find_if_not(s, es, ::isnumber);
    if (p == s || p == es) return bytes;
    if (strcasecmp(p, "GB") == 0)
        bytes *= 1024 * 1024 * 1024;
    else if (strcasecmp(p, "MB") == 0)
        bytes *= 1024 * 1024;
    else if (strcasecmp(p, "KB") == 0)
        bytes *= 1024;
    return bytes;
}

void Alice::readServerConf(const char *server_conf_file)
{
    ConfParamList paramlist;
    parseConf(paramlist, server_conf_file);
    for (auto& it : paramlist) {
        if (strcasecmp(it[0].c_str(), "port") == 0) {
            g_server_conf.port = atoi(it[1].c_str());
            ASSERT(g_server_conf.port > 0, "port");
        } else if (strcasecmp(it[0].c_str(), "ip") == 0) {
            g_server_conf.addr.assign(it[1]);
        } else if (strcasecmp(it[0].c_str(), "databases") == 0) {
            g_server_conf.databases = atoi(it[1].c_str());
            ASSERT(g_server_conf.databases > 0, "databases");
        } else if (strcasecmp(it[0].c_str(), "save") == 0) {
            g_server_conf.save_params.emplace_back(atol(it[1].c_str()), atol(it[2].c_str()));
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
            g_server_conf.repl_timeout = atoi(it[1].c_str());
            ASSERT(g_server_conf.repl_timeout > 0, "repl-timeout");
            g_server_conf.repl_timeout *= 1000;
        } else if (strcasecmp(it[0].c_str(), "repl-ping-period") == 0) {
            g_server_conf.repl_ping_period = atoi(it[1].c_str());
            ASSERT(g_server_conf.repl_ping_period > 0, "repl-ping-period");
            g_server_conf.repl_ping_period *= 1000;
        } else if (strcasecmp(it[0].c_str(), "repl-backlog-size") == 0) {
            g_server_conf.repl_backlog_size = humanSizeToBytes(it[1].c_str());
            ASSERT(g_server_conf.repl_backlog_size > 0, "repl-backlog-size");
        } else if (strcasecmp(it[0].c_str(), "expire-check-dbnums") == 0) {
            g_server_conf.expire_check_dbnums = atoi(it[1].c_str());
            ASSERT(g_server_conf.expire_check_dbnums > 0, "expire-check-dbnums");
        } else if (strcasecmp(it[0].c_str(), "expire-check-keys") == 0) {
            g_server_conf.expire_check_keys = atoi(it[1].c_str());
            ASSERT(g_server_conf.expire_check_keys > 0, "expire-check-keys");
        } else if (strcasecmp(it[0].c_str(), "maxmemory") == 0) {
            g_server_conf.maxmemory = humanSizeToBytes(it[1].c_str());
            ASSERT(g_server_conf.maxmemory >= 0, "maxmemory");
        } else if (strcasecmp(it[0].c_str(), "maxmemory-policy") == 0) {
            if (strcasecmp(it[1].c_str(), "allkeys-lru") == 0) {
                g_server_conf.maxmemory_policy = EVICT_ALLKEYS_LRU;
            } else if (strcasecmp(it[1].c_str(), "volatile-lru") == 0) {
                g_server_conf.maxmemory_policy = EVICT_VOLATILE_LRU;
            } else if (strcasecmp(it[1].c_str(), "allkeys-random") == 0) {
                g_server_conf.maxmemory_policy = EVICT_ALLKEYS_RANDOM;
            } else if (strcasecmp(it[1].c_str(), "volatile-random") == 0) {
                g_server_conf.maxmemory_policy = EVICT_VOLATILE_RANDOM;
            } else if (strcasecmp(it[1].c_str(), "volatile-ttl") == 0) {
                g_server_conf.maxmemory_policy = EVICT_VOLATILE_TTL;
            } else if (strcasecmp(it[1].c_str(), "noeviction"))
                error("maxmemory-policy");
        } else if (strcasecmp(it[0].c_str(), "maxmemory-samples") == 0) {
            g_server_conf.maxmemory_samples = atoi(it[1].c_str());
            ASSERT(g_server_conf.maxmemory_samples > 0, "maxmemory-samples");
        } else if (strcasecmp(it[0].c_str(), "slowlog-log-slower-than") == 0) {
            g_server_conf.slowlog_log_slower_than = atoi(it[1].c_str());
            ASSERT(g_server_conf.slowlog_log_slower_than >= 0, "slowlog-log-slower-than");
        } else if (strcasecmp(it[0].c_str(), "slowlog-max-len") == 0) {
            g_server_conf.slowlog_max_len = atoi(it[1].c_str());
            ASSERT(g_server_conf.slowlog_max_len >= 0, "slowlog-max-len");
        } else if (strcasecmp(it[0].c_str(), "rdb-file") == 0) {
            g_server_conf.rdb_file = it[1];
        } else if (strcasecmp(it[0].c_str(), "appendonly-file") == 0) {
            g_server_conf.appendonly_file = it[1];
        }
    }
}

void Alice::readSentinelConf(const char *sentinel_conf_file)
{
    ConfParamList paramlist;
    parseConf(paramlist, sentinel_conf_file);
    for(auto& it : paramlist) {
        if (strcasecmp(it[1].c_str(), "ip") == 0) {
            g_sentinel_conf.addr = it[2];
        } else if (strcasecmp(it[1].c_str(), "port") == 0) {
            g_sentinel_conf.port = atoi(it[2].c_str());
            ASSERT(g_sentinel_conf.port > 0, "port");
        } else if (strcasecmp(it[1].c_str(), "monitor") == 0) {
            auto master = new SentinelInstance;
            master->setFlag(SentinelInstance::MASTER);
            master->setName(it[2]);
            master->setInetAddr(Angel::InetAddr(atoi(it[4].c_str()), it[3].c_str()));
            master->setQuorum(atoi(it[5].c_str()));
            g_sentinel_conf.masters.emplace(master->name(), master);
        } else if (strcasecmp(it[1].c_str(), "down-after-milliseconds") == 0) {
            auto master = g_sentinel_conf.masters.find(it[2]);
            ASSERT(master != g_sentinel_conf.masters.end(), "down-after-milliseconds");
            master->second->setDownAfterPeriod(atoll(it[3].c_str()));
        }
    }
}

void DB::configGet(Context& con, const std::string& arg)
{
    appendReplyMulti(con, 2);
    appendReplyString(con, arg);
    if (strcasecmp(arg.c_str(), "databases") == 0) {
        appendReplyString(con, convert(g_server_conf.databases));
    } else if (strcasecmp(arg.c_str(), "expire-check-dbnums") == 0) {
        appendReplyString(con, convert(g_server_conf.expire_check_dbnums));
    } else if (strcasecmp(arg.c_str(), "expire-check-keys") == 0) {
        appendReplyString(con, convert(g_server_conf.expire_check_keys));
    } else if (strcasecmp(arg.c_str(), "maxmemory") == 0) {
        appendReplyString(con, convert(g_server_conf.maxmemory));
    } else if (strcasecmp(arg.c_str(), "maxmemory-policy") == 0) {
        appendReplyString(con, convert(g_server_conf.maxmemory_policy));
    } else if (strcasecmp(arg.c_str(), "maxmemory-samples") == 0) {
        appendReplyString(con, convert(g_server_conf.maxmemory_samples));
    } else if (strcasecmp(arg.c_str(), "save") == 0) {
        std::string s;
        for (auto& it : g_server_conf.save_params) {
            s.append(convert(std::get<0>(it)));
            s.append(" ");
            s.append(convert(std::get<1>(it)));
            s.append(" ");
        }
        s.pop_back();
        appendReplyString(con, s);
    } else if (strcasecmp(arg.c_str(), "appendonly") == 0) {
        appendReplyString(con, g_server_conf.enable_appendonly ? "yes" : "no");
    } else if (strcasecmp(arg.c_str(), "appendfsync") == 0) {
        if (g_server_conf.aof_mode == AOF_EVERYSEC)
            appendReplyString(con, "everysec");
        else if (g_server_conf.aof_mode == AOF_ALWAYS)
            appendReplyString(con, "always");
        else
            appendReplyString(con, "no");
    } else if (strcasecmp(arg.c_str(), "repl-timeout") == 0) {
        appendReplyString(con, convert(g_server_conf.repl_timeout));
    } else if (strcasecmp(arg.c_str(), "repl-ping-period") == 0) {
        appendReplyString(con, convert(g_server_conf.repl_ping_period));
    } else if (strcasecmp(arg.c_str(), "repl-backlog-size") == 0) {
        appendReplyString(con, convert(g_server_conf.repl_backlog_size));
    } else if (strcasecmp(arg.c_str(), "slowlog-log-slower-than") == 0) {
        appendReplyString(con, convert(g_server_conf.slowlog_log_slower_than));
    } else if (strcasecmp(arg.c_str(), "slowlog-max-len") == 0) {
        appendReplyString(con, convert(g_server_conf.slowlog_max_len));
    } else
        con.message().assign(db_return_multi_empty);
}

void DB::configSet(Context& con, const std::string& arg, const std::string& value)
{
    // TODO
}

void DB::configCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    if (strcasecmp(cmdlist[1].c_str(), "get") == 0) {
        configGet(con, cmdlist[2]);
    } else if (strcasecmp(cmdlist[1].c_str(), "set") == 0) {
        configSet(con, cmdlist[2], cmdlist[2]);
    } else
        con.append(db_return_subcommand_err);
}
