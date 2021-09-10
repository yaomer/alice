#include "server.h"

#include <stdio.h>

using namespace alice;

namespace alice {

    server_conf_t server_conf;
    sentinel_conf_t sentinel_conf;
}

static void error(const char *s)
{
    fprintf(stderr, "config error: %s\n", s);
    exit(1);
}

#define ASSERT(expr, str) \
    do { if (!(expr)) error(str); } while (0)

static ssize_t human_size_to_bytes(const char *s)
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

static bool parse_yes_or_no(const std::string& s, bool& option)
{
    if (strcasecmp(s.c_str(), "yes") == 0)
        option = true;
    else if (strcasecmp(s.c_str(), "no") == 0)
        option = false;
    else
        return false;
    return true;
}

void alice::read_server_conf(const std::string& filename)
{
    auto paramlist = parse_conf(filename.c_str());
    for (auto& it : paramlist) {
        if (strcasecmp(it[0].c_str(), "port") == 0) {
            server_conf.port = atoi(it[1].c_str());
            ASSERT(server_conf.port > 0, "port");
        } else if (strcasecmp(it[0].c_str(), "ip") == 0) {
            server_conf.ip = it[1];
        } else if (strcasecmp(it[0].c_str(), "engine") == 0) {
            if (strcasecmp(it[1].c_str(), "mmdb") == 0)
                server_conf.engine = ENGINE_MMDB;
            else if (strcasecmp(it[1].c_str(), "ssdb") == 0)
                server_conf.engine = ENGINE_SSDB;
            else
                ASSERT(0, "engine");
        } else if (strcasecmp(it[0].c_str(), "repl-timeout") == 0) {
            server_conf.repl_timeout = atoi(it[1].c_str());
            ASSERT(server_conf.repl_timeout > 0, "repl-timeout");
            server_conf.repl_timeout *= 1000;
        } else if (strcasecmp(it[0].c_str(), "repl-ping-period") == 0) {
            server_conf.repl_ping_period = atoi(it[1].c_str());
            ASSERT(server_conf.repl_ping_period > 0, "repl-ping-period");
            server_conf.repl_ping_period *= 1000;
        } else if (strcasecmp(it[0].c_str(), "repl-backlog-size") == 0) {
            server_conf.repl_backlog_size = human_size_to_bytes(it[1].c_str());
            ASSERT(server_conf.repl_backlog_size > 0, "repl-backlog-size");
        } else if (strcasecmp(it[0].c_str(), "slowlog-log-slower-than") == 0) {
            server_conf.slowlog_log_slower_than = atoi(it[1].c_str());
            ASSERT(server_conf.slowlog_log_slower_than >= 0, "slowlog-log-slower-than");
        } else if (strcasecmp(it[0].c_str(), "slowlog-max-len") == 0) {
            server_conf.slowlog_max_len = atoi(it[1].c_str());
            ASSERT(server_conf.slowlog_max_len >= 0, "slowlog-max-len");
        } else if (strcasecmp(it[0].c_str(), "slaveof") == 0) {
            server_conf.master_ip = it[1];
            server_conf.master_port = atoi(it[2].c_str());
            ASSERT(server_conf.master_port > 0, "slaveof");
        } else if (strcasecmp(it[0].c_str(), "mmdb-databases") == 0) {
            server_conf.mmdb_databases = atoi(it[1].c_str());
            ASSERT(server_conf.mmdb_databases > 0, "databases");
        } else if (strcasecmp(it[0].c_str(), "mmdb-expire-check-dbnums") == 0) {
            server_conf.mmdb_expire_check_dbnums = atoi(it[1].c_str());
            ASSERT(server_conf.mmdb_expire_check_dbnums > 0, "mmdb-expire-check-dbnums");
        } else if (strcasecmp(it[0].c_str(), "mmdb-expire-check-keys") == 0) {
            server_conf.mmdb_expire_check_keys = atoi(it[1].c_str());
            ASSERT(server_conf.mmdb_expire_check_keys > 0, "mmdb-expire-check-keys");
        } else if (strcasecmp(it[0].c_str(), "mmdb-maxmemory") == 0) {
            server_conf.mmdb_maxmemory = human_size_to_bytes(it[1].c_str());
            ASSERT(server_conf.mmdb_maxmemory >= 0, "mmdb-maxmemory");
        } else if (strcasecmp(it[0].c_str(), "mmdb-maxmemory-policy") == 0) {
            if (strcasecmp(it[1].c_str(), "allkeys-lru") == 0) {
                server_conf.mmdb_maxmemory_policy = EVICT_ALLKEYS_LRU;
            } else if (strcasecmp(it[1].c_str(), "volatile-lru") == 0) {
                server_conf.mmdb_maxmemory_policy = EVICT_VOLATILE_LRU;
            } else if (strcasecmp(it[1].c_str(), "allkeys-random") == 0) {
                server_conf.mmdb_maxmemory_policy = EVICT_ALLKEYS_RANDOM;
            } else if (strcasecmp(it[1].c_str(), "volatile-random") == 0) {
                server_conf.mmdb_maxmemory_policy = EVICT_VOLATILE_RANDOM;
            } else if (strcasecmp(it[1].c_str(), "volatile-ttl") == 0) {
                server_conf.mmdb_maxmemory_policy = EVICT_VOLATILE_TTL;
            } else if (strcasecmp(it[1].c_str(), "noeviction"))
                error("mmdb-maxmemory-policy");
        } else if (strcasecmp(it[0].c_str(), "mmdb-maxmemory-samples") == 0) {
            server_conf.mmdb_maxmemory_samples = atoi(it[1].c_str());
            ASSERT(server_conf.mmdb_maxmemory_samples > 0, "mmdb-maxmemory-samples");
        } else if (strcasecmp(it[0].c_str(), "mmdb-save") == 0) {
            auto seconds = atol(it[1].c_str());
            auto changes = atol(it[2].c_str());
            server_conf.mmdb_save_params.emplace_back(seconds, changes);
        } else if (strcasecmp(it[0].c_str(), "mmdb-rdb-compress") == 0) {
            if (!parse_yes_or_no(it[1], server_conf.mmdb_rdb_compress))
                error("mmdb-rdb-compress");
        } else if (strcasecmp(it[0].c_str(), "mmdb-rdb-compress-limit") == 0) {
            server_conf.mmdb_rdb_compress_limit = atoi(it[1].c_str());
            ASSERT(server_conf.mmdb_rdb_compress_limit >= 0, "mmdb-rdb-compress-limit");
        } else if (strcasecmp(it[0].c_str(), "mmdb-rdb-file") == 0) {
            server_conf.mmdb_rdb_file = it[1];
        } else if (strcasecmp(it[0].c_str(), "mmdb-appendonly") == 0) {
            if (!parse_yes_or_no(it[1], server_conf.mmdb_enable_appendonly))
                error("mmdb_appendonly");
        } else if (strcasecmp(it[0].c_str(), "mmdb-appendfsync") == 0) {
            if (strcasecmp(it[1].c_str(), "always") == 0)
                server_conf.mmdb_aof_mode = AOF_ALWAYS;
            else if (strcasecmp(it[1].c_str(), "everysec") == 0)
                server_conf.mmdb_aof_mode = AOF_EVERYSEC;
            else if (strcasecmp(it[1].c_str(), "no") == 0)
                server_conf.mmdb_aof_mode = AOF_NO;
            else
                error("mmdb-appendfsync");
        } else if (strcasecmp(it[0].c_str(), "mmdb-appendonly-file") == 0) {
            server_conf.mmdb_appendonly_file = it[1];
        } else if (strcasecmp(it[0].c_str(), "ssdb-leveldb-dbname") == 0) {
            server_conf.ssdb_leveldb_dbname = it[1];
        } else if (strcasecmp(it[0].c_str(), "ssdb-snapshot-name") == 0) {
            server_conf.ssdb_snapshot_name = it[1];
        } else if (strcasecmp(it[0].c_str(), "ssdb-expire-check-keys") == 0) {
            server_conf.ssdb_expire_check_keys = atoi(it[1].c_str());
            ASSERT(server_conf.ssdb_expire_check_keys > 0, "ssdb-expire-check-keys");
        } else if (strcasecmp(it[0].c_str(), "ssdb-leveldb-create-if-missing") == 0) {
            if (!parse_yes_or_no(it[1], server_conf.ssdb_leveldb_create_if_missing))
                error("ssdb-leveldb-create-if-missing");
        } else if (strcasecmp(it[0].c_str(), "ssdb-leveldb-write-buffer-size") == 0) {
            server_conf.ssdb_leveldb_write_buffer_size = human_size_to_bytes(it[1].c_str());
            ASSERT(server_conf.ssdb_leveldb_write_buffer_size > 0, "ssdb-leveldb-write-buffer-size");
        } else if (strcasecmp(it[0].c_str(), "ssdb-leveldb-max-open-files") == 0) {
            server_conf.ssdb_leveldb_max_open_files = atoi(it[1].c_str());
            ASSERT(server_conf.ssdb_leveldb_max_open_files > 0, "ssdb-leveldb-max-open-files");
        } else if (strcasecmp(it[0].c_str(), "ssdb-leveldb-max-file-size") == 0) {
            server_conf.ssdb_leveldb_max_file_size = human_size_to_bytes(it[1].c_str());
            ASSERT(server_conf.ssdb_leveldb_max_file_size > 0, "ssdb-leveldb-max-file-size");
        }
    }
}

void alice::read_sentinel_conf(const std::string& filename)
{
    auto paramlist = parse_conf(filename.c_str());
    for(auto& it : paramlist) {
        if (strcasecmp(it[1].c_str(), "ip") == 0) {
            sentinel_conf.ip = it[2];
        } else if (strcasecmp(it[1].c_str(), "port") == 0) {
            sentinel_conf.port = atoi(it[2].c_str());
            ASSERT(sentinel_conf.port > 0, "port");
        } else if (strcasecmp(it[1].c_str(), "monitor") == 0) {
            sentinel_instance_conf_t ins;
            ins.name = it[2];
            ins.ip = it[3];
            ins.port = atoi(it[4].c_str());
            ins.quorum = atoi(it[5].c_str());
            sentinel_conf.insmap.emplace(ins.name, ins);
        } else if (strcasecmp(it[1].c_str(), "down-after-milliseconds") == 0) {
            auto ins = sentinel_conf.insmap.find(it[2]);
            ASSERT(ins != sentinel_conf.insmap.end(), "down-after-milliseconds");
            ins->second.down_after_period = atol(it[3].c_str());
        }
    }
}

static void get_config(context_t& con, const std::string& arg)
{
    con.append_reply_multi(2);
    con.append_reply_string(arg);
    if (strcasecmp(arg.c_str(), "engine") == 0) {
        if (server_conf.engine == ENGINE_MMDB)
            con.append_reply_string("mmdb");
        else if (server_conf.engine == ENGINE_SSDB)
            con.append_reply_string("ssdb");
        else
            ASSERT(0, "engine");
    } else if (strcasecmp(arg.c_str(), "repl-timeout") == 0) {
        con.append_reply_string(i2s(server_conf.repl_timeout));
    } else if (strcasecmp(arg.c_str(), "repl-ping-period") == 0) {
        con.append_reply_string(i2s(server_conf.repl_ping_period));
    } else if (strcasecmp(arg.c_str(), "repl-backlog-size") == 0) {
        con.append_reply_string(i2s(server_conf.repl_backlog_size));
    } else if (strcasecmp(arg.c_str(), "slowlog-log-slower-than") == 0) {
        con.append_reply_string(i2s(server_conf.slowlog_log_slower_than));
    } else if (strcasecmp(arg.c_str(), "slowlog-max-len") == 0) {
        con.append_reply_string(i2s(server_conf.slowlog_max_len));
    } else if (strcasecmp(arg.c_str(), "mmdb-databases") == 0) {
        con.append_reply_string(i2s(server_conf.mmdb_databases));
    } else if (strcasecmp(arg.c_str(), "mmdb-expire-check-dbnums") == 0) {
        con.append_reply_string(i2s(server_conf.mmdb_expire_check_dbnums));
    } else if (strcasecmp(arg.c_str(), "mmdb-expire-check-keys") == 0) {
        con.append_reply_string(i2s(server_conf.mmdb_expire_check_keys));
    } else if (strcasecmp(arg.c_str(), "mmdb-maxmemory") == 0) {
        con.append_reply_string(i2s(server_conf.mmdb_maxmemory));
    } else if (strcasecmp(arg.c_str(), "mmdb-maxmemory-policy") == 0) {
        con.append_reply_string(i2s(server_conf.mmdb_maxmemory_policy));
    } else if (strcasecmp(arg.c_str(), "mmdb-maxmemory-samples") == 0) {
        con.append_reply_string(i2s(server_conf.mmdb_maxmemory_samples));
    } else if (strcasecmp(arg.c_str(), "mmdb-save") == 0) {
        std::string s;
        for (auto& it : server_conf.mmdb_save_params) {
            s.append(i2s(std::get<0>(it)));
            s.append(" ");
            s.append(i2s(std::get<1>(it)));
            s.append(" ");
        }
        s.pop_back();
        con.append_reply_string(s);
    } else if (strcasecmp(arg.c_str(), "mmdb-appendonly") == 0) {
        con.append_reply_string(server_conf.mmdb_enable_appendonly ? "yes" : "no");
    } else if (strcasecmp(arg.c_str(), "mmdb-appendfsync") == 0) {
        if (server_conf.mmdb_aof_mode == AOF_EVERYSEC)
            con.append_reply_string("everysec");
        else if (server_conf.mmdb_aof_mode == AOF_ALWAYS)
            con.append_reply_string("always");
        else
            con.append_reply_string("no");
    } else
        con.append(shared.nil);
}

static void set_config(context_t& con)
{
    // TODO
}

void dbserver::config(context_t& con)
{
    if (con.isequal(1, "get")) {
        get_config(con, con.argv[2]);
    } else if (con.isequal(1, "set")) {
        set_config(con);
    } else
        con.append(shared.subcommand_err);
}
