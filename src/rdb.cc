#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <assert.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <arpa/inet.h>
#include <tuple>

#include "rdb.h"
#include "server.h"
#include "configure.h"

#if defined (ALICE_HAVE_SNAPPY)
#include <snappy.h>
#endif

using namespace Alice;

namespace Alice {

    static unsigned char magic[5] = { 0x41, 0x4c, 0x49, 0x43, 0x45 };
    static unsigned char select_db = 0xfe;
    static unsigned char eof = 0xff;
    static unsigned char string_type = 0;
    static unsigned char list_type = 1;
    static unsigned char set_type = 2;
    static unsigned char hash_type = 3;
    static unsigned char zset_type = 4;
    static unsigned char expire_key = 5;
    static unsigned char rdb_6bit_len = 0;
    static unsigned char rdb_14bit_len = 1;
    static unsigned char rdb_32bit_len = 0x80;
    static unsigned char rdb_64bit_len = 0x81;
    static unsigned char compress_value = 0;
    static unsigned char uncompress_value = 1;
}

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
    _fd = open(tmpfile, O_RDWR | O_CREAT | O_APPEND, 0660);
    append(magic, 5);
    int64_t now = Angel::nowMs();
    int index = 0;
    for (auto& db : _dbServer->dbs()) {
        if (db->hashMap().empty()) {
            index++;
            continue;
        }
        saveLen(select_db);
        saveLen(index);
        for (auto it = db->hashMap().begin(); it != db->hashMap().end(); ++it) {
            auto expire = db->expireMap().find(it->first);
            if (expire != db->expireMap().end()) {
                if (expire->second <= now) {
                    db->delKeyWithExpire(it->first);
                    continue;
                } else {
                    saveLen(expire_key);
                    int64_t timeval = expire->second;
                    append(&timeval, 8);
                }
            }
            if (isType(it, DB::String)) {
                saveString(it);
            } else if (isType(it, DB::List)) {
                saveList(it);
            } else if (isType(it, DB::Set)) {
                saveSet(it);
            } else if (isType(it, DB::Hash)) {
                saveHash(it);
            } else if (isType(it, DB::Zset)) {
                saveZset(it);
            }
        }
        index++;
    }
    saveLen(eof);
    if (_buffer.size() > 0) flush();
    g_server->fsyncBackground(_fd);
    rename(tmpfile, g_server_conf.rdb_file.c_str());
}

void Rdb::saveBackground()
{
    _childPid = fork();
    logInfo("Background saving started by pid %ld", _childPid);
    if (_childPid == 0) {
        childPidReset();
        save();
        abort();
    }
}

// 借用redis中的两个编解码长度的函数
int Rdb::saveLen(uint64_t len)
{
    unsigned char buf[2];
    size_t writeBytes = 0;

    if (len < (1 << 6)) {
        buf[0] = (len & 0xff) | (rdb_6bit_len << 6);
        append(buf, 1);
        writeBytes = 1;
    } else if (len < (1 << 14)) {
        buf[0] = ((len >> 8) & 0xff) | (rdb_14bit_len << 6);
        buf[1] = len & 0xff;
        append(buf, 2);
        writeBytes = 2;
    } else if (len <= UINT32_MAX) {
        buf[0] = rdb_32bit_len;
        append(buf, 1);
        uint32_t len32 = htonl(len);
        append(&len32, 4);
        writeBytes = 1 + 4;
    } else {
        buf[0] = rdb_64bit_len;
        append(buf, 1);
        append(&len, 8);
        writeBytes = 1 + 8;
    }
    return writeBytes;
}

int Rdb::loadLen(char *ptr, uint64_t *lenptr)
{
    unsigned char buf[2] = {
        static_cast<unsigned char>(ptr[0]), 0 };
    int type = (buf[0] & 0xc0) >> 6;
    size_t readBytes = 0;

    if (type == rdb_6bit_len) {
        *lenptr = buf[0] & 0x3f;
        readBytes = 1;
    } else if (type == rdb_14bit_len) {
        buf[1] = static_cast<unsigned char>(ptr[1]);
        *lenptr = ((buf[0] & 0x3f) << 8) | buf[1];
        readBytes = 2;
    } else if (type == rdb_32bit_len) {
        uint32_t len32 = *reinterpret_cast<uint32_t*>(&ptr[1]);
        *lenptr = ntohl(len32);
        readBytes = 5;
    } else {
        *lenptr = *reinterpret_cast<uint64_t*>(&ptr[1]);
        readBytes = 8;
    }
    return readBytes;
}

void Rdb::saveKey(const std::string& key)
{
    saveLen(key.size());
    append(key);
}

void Rdb::saveValue(const std::string& value)
{
    if (canCompress(value.size())) {
        saveLen(compress_value);
        saveLen(value.size());
        std::string output;
        compress(value.data(), value.size(), &output);
        saveLen(output.size());
        append(output);
    } else {
        saveLen(uncompress_value);
        saveLen(value.size());
        append(value);
    }
}

void Rdb::saveString(const Iterator& it)
{
    saveLen(string_type);
    saveKey(it->first);
    DB::String& string = getValue(it, DB::String&);
    saveValue(string);
}

void Rdb::saveList(const Iterator& it)
{
    saveLen(list_type);
    saveKey(it->first);
    DB::List& list = getValue(it, DB::List&);
    saveLen(list.size());
    for (auto& value : list) {
        saveValue(value);
    }
}

void Rdb::saveSet(const Iterator& it)
{
    saveLen(set_type);
    saveKey(it->first);
    DB::Set& set = getValue(it, DB::Set&);
    saveLen(set.size());
    for (auto& value : set) {
        saveValue(value);
    }
}

void Rdb::saveHash(const Iterator& it)
{
    saveLen(hash_type);
    saveKey(it->first);
    DB::Hash& hash = getValue(it, DB::Hash&);
    saveLen(hash.size());
    for (auto& pair : hash) {
        saveKey(pair.first);
        saveValue(pair.second);
    }
}

void Rdb::saveZset(const Iterator& it)
{
    saveLen(zset_type);
    saveKey(it->first);
    auto& tuple = getValue(it, DB::Zset&);
    DB::_Zset& zset = std::get<0>(tuple);
    saveLen(zset.size());
    for (auto& it : zset) {
        saveLen(strlen(convert2f(std::get<0>(it))));
        append(convert2f(std::get<0>(it)));
        saveValue(std::get<1>(it));
    }
}

void Rdb::load()
{
    int fd = open(g_server_conf.rdb_file.c_str(), O_RDONLY);
    struct stat st;
    fstat(fd, &st);
    size_t size = st.st_size;
    void *start = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
    if (start == MAP_FAILED) return;
    char *buf = reinterpret_cast<char*>(start);
    if (size < 5 || strncmp(buf, "ALICE", 5))
        return;
    buf += 5;
    uint64_t type;
    int64_t timeval = 0;
    while (true) {
        int len = loadLen(buf, &type);
        if (type == eof)
            break;
        if (type == select_db) {
            buf += len;
            buf += loadLen(buf, &type);
            _curdb = _dbServer->selectDb(type);
            continue;
        }
        if (type == expire_key) {
            timeval = *reinterpret_cast<int64_t*>(buf+len);
            buf += len + 8;
        } else if (type == string_type) {
            buf = loadString(buf + len, &timeval);
        } else if (type == list_type) {
            buf = loadList(buf + len, &timeval);
        } else if (type == set_type) {
            buf = loadSet(buf + len, &timeval);
        } else if (type == hash_type) {
            buf = loadHash(buf + len, &timeval);
        } else
            buf = loadZset(buf + len, &timeval);
    }
    munmap(start, size);
    close(fd);
}

// loadXXX()不能使用stack-array，不然如果key/value太长的话
// 会导致stack-overflow

char *Rdb::loadKey(char *ptr, std::string *key)
{
    uint64_t len;
    ptr += loadLen(ptr, &len);
    key->assign(ptr, len);
    ptr += len;
    return ptr;
}

char *Rdb::loadValue(char *ptr, std::string *value)
{
    uint64_t compress_if;
    ptr += loadLen(ptr, &compress_if);
    assert(compress_if == compress_value || compress_if == uncompress_value);
    if (compress_if == compress_value) {
        uint64_t origin_len, compressed_len;
        ptr += loadLen(ptr, &origin_len);
        ptr += loadLen(ptr, &compressed_len);
        uncompress(ptr, compressed_len, value);
        assert(value->size() == origin_len);
        ptr += compressed_len;
    } else {
        uint64_t value_len;
        ptr += loadLen(ptr, &value_len);
        value->assign(ptr, value_len);
        ptr += value_len;
    }
    return ptr;
}

char *Rdb::loadString(char *ptr, int64_t *tvptr)
{
    std::string key, value;
    ptr = loadKey(ptr, &key);
    ptr = loadValue(ptr, &value);
    _curdb->hashMap().emplace(key, std::move(value));
    loadExpireKey(key, tvptr);
    return ptr;
}

char *Rdb::loadList(char *ptr, int64_t *tvptr)
{
    std::string key;
    ptr = loadKey(ptr, &key);
    uint64_t list_len;
    ptr += loadLen(ptr, &list_len);
    DB::List list;
    while (list_len-- > 0) {
        std::string value;
        ptr = loadValue(ptr, &value);
        list.emplace_back(std::move(value));
    }
    _curdb->hashMap().emplace(key, std::move(list));
    loadExpireKey(key, tvptr);
    return ptr;
}

char *Rdb::loadSet(char *ptr, int64_t *tvptr)
{
    std::string key;
    ptr = loadKey(ptr, &key);
    uint64_t set_len;
    ptr += loadLen(ptr, &set_len);
    DB::Set set;
    while (set_len-- > 0) {
        std::string value;
        ptr = loadValue(ptr, &value);
        set.emplace(std::move(value));
    }
    _curdb->hashMap().emplace(key, std::move(set));
    loadExpireKey(key, tvptr);
    return ptr;
}

char *Rdb::loadHash(char *ptr, int64_t *tvptr)
{
    std::string key;
    ptr = loadKey(ptr, &key);
    uint64_t hash_len;
    ptr += loadLen(ptr, &hash_len);
    DB::Hash hash;
    while (hash_len-- > 0) {
        std::string field, value;
        ptr = loadKey(ptr, &field);
        ptr = loadValue(ptr, &value);
        hash.emplace(std::move(field), std::move(value));
    }
    _curdb->hashMap().emplace(key, std::move(hash));
    loadExpireKey(key, tvptr);
    return ptr;
}

char *Rdb::loadZset(char *ptr, int64_t *tvptr)
{
    std::string key;
    ptr = loadKey(ptr, &key);
    uint64_t zset_len;
    ptr += loadLen(ptr, &zset_len);
    DB::_Zset zset;
    DB::_Zmap zmap;
    while (zset_len-- > 0) {
        std::string score_str, value;
        ptr = loadKey(ptr, &score_str);
        double score = atof(score_str.c_str());
        ptr = loadValue(ptr, &value);
        zmap.emplace(value, score);
        zset.emplace(score, std::move(value));
    }
    _curdb->hashMap().emplace(key, std::make_tuple(zset, zmap));
    loadExpireKey(key, tvptr);
    return ptr;
}

void Rdb::loadExpireKey(const std::string& key, int64_t *tvptr)
{
    if (*tvptr > 0) {
        _curdb->expireMap()[key] = *tvptr;
        *tvptr = 0;
    }
}

void Rdb::append(const std::string& data)
{
    append(data.data(), data.size());
}

void Rdb::append(const void *data, size_t len)
{
    _buffer.append(reinterpret_cast<const char*>(data), len);
    if (_buffer.size() >= buffer_flush_size)
        flush();
}

void Rdb::flush()
{
    writeToFile(_fd, _buffer.data(), _buffer.size());
    _buffer.clear();
}

void Rdb::appendSyncBuffer(const Context::CommandList& cmdlist,
                           const char *query, size_t len)
{
    if (query) _syncBuffer.append(query, len);
    else DBServer::appendCommand(_syncBuffer, cmdlist);
}

bool Rdb::canCompress(size_t value_len)
{
#if defined (ALICE_HAVE_SNAPPY)
    return value_len > compress_limit;
#else
    return false;
#endif
}

void Rdb::compress(const char *input, size_t input_len,
                   std::string *output)
{
#if defined (ALICE_HAVE_SNAPPY)
    snappy::Compress(input, input_len, output);
#else
    return;
#endif
}

void Rdb::uncompress(const char *compressed, size_t compressed_len,
                     std::string *origin)
{
#if defined (ALICE_HAVE_SNAPPY)
    snappy::Uncompress(compressed, compressed_len, origin);
#else
    return;
#endif
}
