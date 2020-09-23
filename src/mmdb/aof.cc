#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>

#include "../config.h"
#include "../server.h"

#include "mmdb.h"
#include "aof.h"

using namespace alice;
using namespace alice::mmdb;

Aof::Aof(mmdb::engine *engine)
    : engine(engine),
    child_pid(-1),
    last_sync_time(0),
    cur_file_size(0),
    last_rewrite_file_size(0),
    fd(-1)
{
    bzero(tmpfile, sizeof(tmpfile));
}

// 保存服务器执行的所有写命令
void Aof::append(const argv_t& argv, const char *query, size_t len)
{
    dbserver::conv2resp_with_expire(buffer, argv, query, len);
}

// 保存aof重写过程中执行的所有写命令
void Aof::append_rewrite_buffer(const argv_t& argv, const char *query, size_t len)
{
    dbserver::conv2resp_with_expire(rewrite_buffer, argv, query, len);
}

// 将缓冲区中的命令flush到文件中
void Aof::fsync()
{
    auto now = lru_clock;
    if (buffer.empty()) return;
    auto sync_interval = now - last_sync_time;
    int fd = open(server_conf.mmdb_appendonly_file.c_str(), O_RDWR | O_APPEND | O_CREAT, 0660);
    fwrite(fd, buffer.data(), buffer.size());
    cur_file_size = get_filesize(fd);
    buffer.clear();
    if (server_conf.mmdb_aof_mode == AOF_ALWAYS) {
        __server->fsync(fd);
    } else if (server_conf.mmdb_aof_mode == AOF_EVERYSEC) {
        if (sync_interval >= 1000) {
            __server->fsync(fd);
            last_sync_time = now;
        }
    }
}

static void aof_set(argv_t& argv, int64_t now)
{
    for (size_t i = 1; i < argv.size(); i++) {
        if (strcasecmp(argv[i].c_str(), "EX") == 0) {
            int64_t timeval = atoll(argv[i+1].c_str()) - now;
            argv[i+1] = i2s(timeval / 1000);
            break;
        } else if (strcasecmp(argv[i].c_str(), "PX") == 0) {
            int64_t timeval = atoll(argv[i+1].c_str()) - now;
            argv[i+1] = i2s(timeval);
            break;
        }
    }
}

void Aof::load()
{
    context_t con(nullptr, engine);
    angel::buffer buf;
    FILE *fp = fopen(server_conf.mmdb_appendonly_file.c_str(), "r");
    if (!fp || get_filesize(fileno(fp)) == 0) {
        rewrite_select_db(0);
        if (fp) fclose(fp);
        return;
    }
    int64_t now = angel::util::get_cur_time_ms();
    char *line = nullptr;
    size_t len = 0;
    ssize_t n;
    while ((n = ::getline(&line, &len, fp)) > 0) {
        buf.append(line, n);
        free(line);
        line = nullptr;
        len = 0;
        ssize_t n = parse_request(con.argv, buf);
        if (n <= 0) continue;
        auto c = engine->find_command(con.argv[0]);
        assert(c);
        if (con.argv[0].compare("PEXPIRE") == 0) {
            int64_t timeval = atoll(con.argv[2].c_str()) - now;
            con.argv[2] = i2s(timeval);
        } else if (con.argv[0].compare("SET") == 0 && con.argv.size() >= 5) {
            aof_set(con.argv, now);
        }
        c->command_cb(con);
        con.argv.clear();
        buf.retrieve(n);
        con.buf.clear();
    }
    engine->switch_db(0);
    fclose(fp);
}

void Aof::rewrite_background()
{
    strcpy(tmpfile, "tmp.XXXXX");
    mktemp(tmpfile);
    child_pid = fork();
    // logInfo("Background AOF rewrite started by pid %ld", _childPid);
    if (child_pid == 0) {
        rewrite();
        done();
        abort();
    }
}

void Aof::rewrite()
{
    fd = open(tmpfile, O_RDWR | O_CREAT | O_APPEND, 0660);
    last_rewrite_file_size = get_filesize(fd);
    buffer.clear();
    int64_t now = angel::util::get_cur_time_ms();
    int index = 0;
    for (auto& db : engine->dbs) {
        auto& dict = db->get_dict();
        auto& expire_keys = db->get_expire_keys();
        if (dict.empty()) {
            index++;
            continue;
        }
        rewrite_select_db(index);
        for (auto it = dict.begin(); it != dict.end(); ++it) {
            auto expire = expire_keys.find(it->first);
            if (expire != expire_keys.end()) {
                if (expire->second <= now) {
                    db->del_key_with_expire(it->first);
                    continue;
                } else {
                    rewrite_expire(it->first, expire->second);
                }
            }
            if (is_type(it, DB::String))
                rewrite_string(it);
            else if (is_type(it, DB::List))
                rewrite_list(it);
            else if (is_type(it, DB::Set))
                rewrite_set(it);
            else if (is_type(it, DB::Hash))
                rewrite_hash(it);
            else if (is_type(it, Zset))
                rewrite_zset(it);
            index++;
        }
    }
    if (buffer.size() > 0) flush();
    __server->fsync(fd);
    rename(tmpfile, server_conf.mmdb_appendonly_file.c_str());
}

// 将aof重写缓冲区中的命令写到文件中
void Aof::fsync_rewrite_buffer()
{
    if (rewrite_buffer.empty()) return;
    int fd = open(server_conf.mmdb_appendonly_file.c_str(), O_RDWR | O_APPEND | O_CREAT, 0660);
    fwrite(fd, rewrite_buffer.data(), rewrite_buffer.size());
    rewrite_buffer.clear();
    __server->fsync(fd);
}

void Aof::rewrite_select_db(int dbnum)
{
    append("*2\r\n$6\r\nSELECT\r\n$");
    append(i2s(strlen(i2s(dbnum))));
    append("\r\n");
    append(i2s(dbnum));
    append("\r\n");
}

void Aof::rewrite_expire(const std::string& key, int64_t expire)
{
    append("*3\r\n$7\r\nPEXPIRE\r\n$");
    append(i2s(key.size()));
    append("\r\n");
    append(key + "\r\n$");
    append(i2s(strlen(i2s(expire))));
    append("\r\n");
    append(i2s(expire));
    append("\r\n");

}

void Aof::rewrite_string(const iterator& it)
{
    auto& value = get_string_value(it);
    append("*3\r\n$3\r\nSET\r\n$");
    append(i2s(it->first.size()));
    append("\r\n");
    append(it->first + "\r\n$");
    append(i2s(value.size()));
    append("\r\n");
    append(value + "\r\n");
}

void Aof::rewrite_list(const iterator& it)
{
    auto& list = get_list_value(it);
    if (list.empty()) return;
    append("*");
    append(i2s(list.size() + 2));
    append("\r\n$5\r\nRPUSH\r\n$");
    append(i2s(it->first.size()));
    append("\r\n");
    append(it->first + "\r\n");
    for (auto& it : list) {
        append("$");
        append(i2s(it.size()));
        append("\r\n");
        append(it + "\r\n");
    }
}

void Aof::rewrite_set(const iterator& it)
{
    auto& set = get_set_value(it);
    if (set.empty()) return;
    append("*");
    append(i2s(set.size() + 2));
    append("\r\n$4\r\nSADD\r\n$");
    append(i2s(it->first.size()));
    append("\r\n");
    append(it->first + "\r\n");
    for (auto& it : set) {
        append("$");
        append(i2s(it.size()));
        append("\r\n");
        append(it + "\r\n");
    }
}

void Aof::rewrite_hash(const iterator& it)
{
    auto& hash = get_hash_value(it);
    if (hash.empty()) return;
    append("*");
    append(i2s(hash.size() * 2 + 2));
    append("\r\n$5\r\nHMSET\r\n$");
    append(i2s(it->first.size()));
    append("\r\n");
    append(it->first + "\r\n");
    for (auto& it : hash) {
        append("$");
        append(i2s(it.first.size()));
        append("\r\n");
        append(it.first + "\r\n$");
        append(i2s(it.second.size()));
        append("\r\n");
        append(it.second + "\r\n");
    }
}

void Aof::rewrite_zset(const iterator& it)
{
    auto& zset = get_zset_value(it);
    if (zset.zmap.empty()) return;
    append("*");
    append(i2s(zset.size() * 2 + 2));
    append("\r\n$4\r\nZADD\r\n$");
    append(i2s(it->first.size()));
    append("\r\n");
    append(it->first + "\r\n");
    for (auto& it : zset.zmap) {
        append("$");
        append(i2s(strlen(d2s(it.second))));
        append("\r\n");
        append(d2s(it.second));
        append("\r\n$");
        append(i2s(it.first.size()));
        append("\r\n");
        append(it.first + "\r\n");
    }
}

// aof重写过程中使用，将重写的命令先追加到缓冲区中，
// 然后在合适的时候flush到文件中
void Aof::append(const std::string& s)
{
    buffer.append(s);
    if (buffer.size() >= buffer_flush_size) {
        flush();
    }
}

void Aof::flush()
{
    fwrite(fd, buffer.data(), buffer.size());
    buffer.clear();
}

bool Aof::can_rewrite()
{
    return cur_file_size >= rewrite_min_filesize &&
           cur_file_size >= last_rewrite_file_size * rewrite_rate;
}
