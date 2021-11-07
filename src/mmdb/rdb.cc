#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <assert.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <arpa/inet.h>

#include <tuple>

#include <snappy.h>

#include "../config.h"
#include "../server.h"

#include "internal.h"
#include "rdb.h"

namespace alice {

namespace mmdb {

static unsigned char magic[5] = { 0x41, 0x4c, 0x49, 0x43, 0x45 };
static unsigned char select_db = 0xfe;
static unsigned char eof = 0xff;
static unsigned char string_type = 0;
static unsigned char list_type = 1;
static unsigned char set_type = 2;
static unsigned char hash_type = 3;
static unsigned char zset_type = 4;
static unsigned char expire_key = 5;
static unsigned char compress_value = 0;
static unsigned char uncompress_value = 1;

// <key>: <key-len><key>
// <value>: <uncompress><origin-len><origin-value>
//        | <compress><origin-len><compressed-len><compressed-value>
// <pair>: <key><value>
//
// <string>: <string-type><pair>
// <list>: <list-type><key><list-len><value ...>
// <set>: <set-type><key><set-len><value ...>
// <hash>: <hash-type><key><hash-len><<pair> ...>
// <zset>: <zset-type><key><zset-len><<pair> ...>
//
// <any-value>: <<string>|<list>|<set>|<hash>|<zset>>
// <db>: <<key><any-value> ...>
//
// rdb-file:
// <magic><<db-num><db> ...><eof>

void Rdb::save()
{
    char tmpfile[16];
    strcpy(tmpfile, "tmp.XXXXX");
    mktemp(tmpfile);
    fd = open(tmpfile, O_RDWR | O_CREAT | O_APPEND, 0660);
    append(magic, 5);
    auto now = angel::util::get_cur_time_ms();
    int index = 0;
    for (auto& db : engine->dbs) {
        auto& dict = db->get_dict();
        auto& expire_keys = db->get_expire_keys();
        if (dict.empty()) {
            index++;
            continue;
        }
        save_len(select_db);
        save_len(index);
        for (auto it = dict.begin(); it != dict.end(); ++it) {
            auto expire = expire_keys.find(it->first);
            if (expire != expire_keys.end()) {
                if (expire->second <= now) {
                    db->del_key_with_expire(it->first);
                    continue;
                } else {
                    save_len(expire_key);
                    append(&expire->second, 8);
                }
            }
            if (is_type(it, DB::String)) {
                save_string(it);
            } else if (is_type(it, DB::List)) {
                save_list(it);
            } else if (is_type(it, DB::Set)) {
                save_set(it);
            } else if (is_type(it, DB::Hash)) {
                save_hash(it);
            } else if (is_type(it, Zset)) {
                save_zset(it);
            }
        }
        index++;
    }
    save_len(eof);
    if (buffer.size() > 0) flush();
    __server->fsync(fd);
    rename(tmpfile, server_conf.mmdb_rdb_file.c_str());
}

void Rdb::save_background()
{
    child_pid = fork();
    // logInfo("Background saving started by pid %ld", _childPid);
    if (child_pid == 0) {
        save();
        done();
        abort();
    }
}

int Rdb::save_len(uint64_t len)
{
    return alice::save_len(buffer, len);
}

void Rdb::save_key(const std::string& key)
{
    save_len(key.size());
    append(key);
}

void Rdb::save_value(const std::string& value)
{
    if (can_compress(value.size())) {
        save_len(compress_value);
        save_len(value.size());
        std::string output;
        compress(value.data(), value.size(), &output);
        save_len(output.size());
        append(output);
    } else {
        save_len(uncompress_value);
        save_len(value.size());
        append(value);
    }
}

void Rdb::save_string(const iterator& it)
{
    save_len(string_type);
    save_key(it->first);
    auto& value = get_string_value(it);
    save_value(value);
}

void Rdb::save_list(const iterator& it)
{
    save_len(list_type);
    save_key(it->first);
    auto& list = get_list_value(it);
    save_len(list.size());
    for (auto& value : list) {
        save_value(value);
    }
}

void Rdb::save_set(const iterator& it)
{
    save_len(set_type);
    save_key(it->first);
    auto& set = get_set_value(it);
    save_len(set.size());
    for (auto& value : set) {
        save_value(value);
    }
}

void Rdb::save_hash(const iterator& it)
{
    save_len(hash_type);
    save_key(it->first);
    auto& hash = get_hash_value(it);
    save_len(hash.size());
    for (auto& pair : hash) {
        save_key(pair.first);
        save_value(pair.second);
    }
}

void Rdb::save_zset(const iterator& it)
{
    save_len(zset_type);
    save_key(it->first);
    auto& zset = get_zset_value(it);
    save_len(zset.size());
    for (auto& it : zset.zmap) {
        save_len(strlen(d2s(it.second)));
        append(d2s(it.second));
        save_value(it.first);
    }
}

void Rdb::load()
{
    int fd = open(server_conf.mmdb_rdb_file.c_str(), O_RDONLY);
    if (fd < 0) return;
    off_t size = get_filesize(fd);
    void *start = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
    if (start == MAP_FAILED) return;
    char *buf = reinterpret_cast<char*>(start);
    if (size < 5 || strncmp(buf, "ALICE", 5))
        return;
    buf += 5;
    engine->clear();
    uint64_t type;
    int64_t timeval = 0;
    while (true) {
        int len = load_len(buf, &type);
        if (type == eof)
            break;
        if (type == select_db) {
            buf += len;
            buf += load_len(buf, &type);
            cur_db = engine->select_db(type);
            continue;
        }
        if (type == expire_key) {
            timeval = *reinterpret_cast<int64_t*>(buf+len);
            buf += len + 8;
        } else if (type == string_type) {
            buf = load_string(buf + len, &timeval);
        } else if (type == list_type) {
            buf = load_list(buf + len, &timeval);
        } else if (type == set_type) {
            buf = load_set(buf + len, &timeval);
        } else if (type == hash_type) {
            buf = load_hash(buf + len, &timeval);
        } else if (type == zset_type)
            buf = load_zset(buf + len, &timeval);
    }
    munmap(start, size);
    close(fd);
}

// load_xxx()不能使用stack-array，不然如果key/value太长的话
// 会导致stack-overflow

char *Rdb::load_key(char *ptr, std::string *key)
{
    uint64_t len;
    ptr += load_len(ptr, &len);
    key->assign(ptr, len);
    ptr += len;
    return ptr;
}

char *Rdb::load_value(char *ptr, std::string *value)
{
    uint64_t compress_if;
    ptr += load_len(ptr, &compress_if);
    assert(compress_if == compress_value || compress_if == uncompress_value);
    if (compress_if == compress_value) {
        uint64_t origin_len, compressed_len;
        ptr += load_len(ptr, &origin_len);
        ptr += load_len(ptr, &compressed_len);
        uncompress(ptr, compressed_len, value);
        assert(value->size() == origin_len);
        ptr += compressed_len;
    } else {
        uint64_t value_len;
        ptr += load_len(ptr, &value_len);
        value->assign(ptr, value_len);
        ptr += value_len;
    }
    return ptr;
}

char *Rdb::load_string(char *ptr, int64_t *tvptr)
{
    std::string key, value;
    ptr = load_key(ptr, &key);
    ptr = load_value(ptr, &value);
    cur_db->add_key(key, value);
    load_expire_key(key, tvptr);
    return ptr;
}

char *Rdb::load_list(char *ptr, int64_t *tvptr)
{
    std::string key;
    ptr = load_key(ptr, &key);
    uint64_t list_len;
    ptr += load_len(ptr, &list_len);
    DB::List list;
    while (list_len-- > 0) {
        std::string value;
        ptr = load_value(ptr, &value);
        list.emplace_back(std::move(value));
    }
    cur_db->add_key(key, list);
    load_expire_key(key, tvptr);
    return ptr;
}

char *Rdb::load_set(char *ptr, int64_t *tvptr)
{
    std::string key;
    ptr = load_key(ptr, &key);
    uint64_t set_len;
    ptr += load_len(ptr, &set_len);
    DB::Set set;
    while (set_len-- > 0) {
        std::string value;
        ptr = load_value(ptr, &value);
        set.emplace(std::move(value));
    }
    cur_db->add_key(key, set);
    load_expire_key(key, tvptr);
    return ptr;
}

char *Rdb::load_hash(char *ptr, int64_t *tvptr)
{
    std::string key;
    ptr = load_key(ptr, &key);
    uint64_t hash_len;
    ptr += load_len(ptr, &hash_len);
    DB::Hash hash;
    while (hash_len-- > 0) {
        std::string field, value;
        ptr = load_key(ptr, &field);
        ptr = load_value(ptr, &value);
        hash.emplace(std::move(field), std::move(value));
    }
    cur_db->add_key(key, hash);
    load_expire_key(key, tvptr);
    return ptr;
}

char *Rdb::load_zset(char *ptr, int64_t *tvptr)
{
    std::string key;
    ptr = load_key(ptr, &key);
    uint64_t zset_len;
    ptr += load_len(ptr, &zset_len);
    Zset zset;
    while (zset_len-- > 0) {
        std::string score_str, value;
        ptr = load_key(ptr, &score_str);
        double score = atof(score_str.c_str());
        ptr = load_value(ptr, &value);
        zset.insert(score, value);
    }
    cur_db->add_key(key, std::move(zset));
    load_expire_key(key, tvptr);
    return ptr;
}

void Rdb::load_expire_key(const std::string& key, int64_t *tvptr)
{
    if (*tvptr > 0) {
        *tvptr += angel::util::get_cur_time_ms();
        cur_db->add_expire_key(key, *tvptr);
        *tvptr = 0;
    }
}

void Rdb::append(const std::string& data)
{
    append(data.data(), data.size());
}

void Rdb::append(const void *data, size_t len)
{
    buffer.append(reinterpret_cast<const char*>(data), len);
    if (buffer.size() >= buffer_flush_size)
        flush();
}

void Rdb::flush()
{
    fwrite(fd, buffer.data(), buffer.size());
    buffer.clear();
}

bool Rdb::can_compress(size_t value_len)
{
    return server_conf.mmdb_rdb_compress && value_len > server_conf.mmdb_rdb_compress_limit;
}

void Rdb::compress(const char *input, size_t input_len,
                   std::string *output)
{
    snappy::Compress(input, input_len, output);
}

void Rdb::uncompress(const char *compressed, size_t compressed_len,
                     std::string *origin)
{
    snappy::Uncompress(compressed, compressed_len, origin);
}

}
}
