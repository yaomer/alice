#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <stdint.h>
#include <arpa/inet.h>
#include <string.h>
#include <string>
#include "rdb.h"
#include "db.h"
#include "server.h"

using namespace Alice;

namespace Alice {

    static unsigned char magic[5] = { 0x41, 0x4c, 0x49, 0x43, 0x45 };
    static unsigned char eof = 0xff;
    static unsigned char string_type = 0;
    static unsigned char list_type = 1;
    static unsigned char set_type = 2;
    static unsigned char hash_type = 3;
    static unsigned char rdb_6bit_len = 0;
    static unsigned char rdb_14bit_len = 1;
    static unsigned char rdb_32bit_len = 0x80;
}

#define isXXType(it, _type) \
    ((it).second.value().type() == typeid(_type))
#define getXXType(it, _type) \
    (std::any_cast<_type>((it).second.value()))

void Rdb::rdbSave()
{
    if (rdb_modifies < rdb_save_modifies)
        return;
    rdb_modifies = 0;
    if (access("dump.rdb", F_OK) == 0) {
        strcpy(_tmpFilename, "tmp.XXXXX");
        mktemp(_tmpFilename);
        rename("dump.rdb", _tmpFilename);
    }
    _fd = open("dump.rdb", O_RDWR | O_CREAT | O_APPEND, 0660);
    append(magic, 5);
    auto& map = _dbServer->db().hashMap();
    for (auto& it : map) {
        if (isXXType(it, DB::String)) {
            rdbSaveString(it);
        } else if (isXXType(it, DB::List)) {
            rdbSaveList(it);
        } else if (isXXType(it, DB::Set)) {
            rdbSaveSet(it);
        } else
            rdbSaveHash(it);
    }
    rdbSaveLen(eof);
    if (_buffer.size() > 0) flush();
    fsync(_fd);
    close(_fd);
    remove(_tmpFilename);
}

void Rdb::rdbSaveString(Pair pair)
{
    rdbSaveLen(string_type);
    rdbSaveLen(pair.first.size());
    append(pair.first);
    DB::String& string = getXXType(pair, DB::String&);
    rdbSaveLen(string.size());
    append(string);
}

void Rdb::rdbSaveList(Pair pair)
{
    rdbSaveLen(list_type);
    rdbSaveLen(pair.first.size());
    append(pair.first);
    DB::List& list = getXXType(pair, DB::List&);
    rdbSaveLen(list.size());
    for (auto& it : list) {
        rdbSaveLen(it.size());
        append(it);
    }
}

void Rdb::rdbSaveSet(Pair pair)
{
    rdbSaveLen(set_type);
    rdbSaveLen(pair.first.size());
    append(pair.first);
    DB::Set& set = getXXType(pair, DB::Set&);
    rdbSaveLen(set.size());
    for (auto& it : set) {
        rdbSaveLen(it.size());
        append(it);
    }
}

void Rdb::rdbSaveHash(Pair pair)
{
    rdbSaveLen(hash_type);
    rdbSaveLen(pair.first.size());
    append(pair.first);
    DB::Hash& hash = getXXType(pair, DB::Hash&);
    rdbSaveLen(hash.size());
    for (auto& it : hash) {
        rdbSaveLen(it.first.size());
        append(it.first);
        rdbSaveLen(it.second.size());
        append(it.second);
    }
}

void Rdb::rdbRecover()
{
    int fd = open("dump.rdb", O_RDONLY);
    struct stat st;
    fstat(fd, &st);
    size_t size = st.st_size;
    void *start = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
    if (start == MAP_FAILED) return;
    char *buf = (char *)start;
    if (size < 5 || strncmp(buf, "ALICE", 5))
        return;
    uint64_t keylen, vallen;
    auto& map = _dbServer->db().hashMap();
    auto i = 5;
    while (true) {
        rdbLoadLen(&buf[i], &vallen);
        if (vallen == eof)
            break;
        if (buf[i] == string_type) {
            i++;
            i += rdbLoadLen(&buf[i], &keylen);
            char key[keylen];
            memcpy(&key[0], &buf[i], keylen);
            i += keylen;
            i += rdbLoadLen(&buf[i], &vallen);
            char val[vallen];
            memcpy(&val[0], &buf[i], vallen);
            i += vallen;
            map[std::string(key, keylen)] = std::string(val, vallen);
        } else if (buf[i] == list_type) {
            i++;
            i += rdbLoadLen(&buf[i], &keylen);
            char key[keylen];
            memcpy(&key[0], &buf[i], keylen);
            i += keylen;
            uint64_t listlen;
            i += rdbLoadLen(&buf[i], &listlen);
            DB::List list;
            while (listlen-- > 0) {
                i += rdbLoadLen(&buf[i], &vallen);
                char val[vallen];
                memcpy(&val[0], &buf[i], vallen);
                i += vallen;
                list.push_back(std::string(val, vallen));
            }
            map[std::string(key, keylen)] = std::move(list);
        } else if (buf[i] == hash_type) {
            i++;
            i += rdbLoadLen(&buf[i], &keylen);
            char key[keylen];
            memcpy(&key[0], &buf[i], keylen);
            i += keylen;
            uint64_t hashlen;
            i += rdbLoadLen(&buf[i], &hashlen);
            DB::Hash hash;
            uint64_t fieldlen;
            while (hashlen-- > 0) {
                i += rdbLoadLen(&buf[i], &fieldlen);
                char field[fieldlen];
                memcpy(&field[0], &buf[i], fieldlen);
                i += fieldlen;
                i += rdbLoadLen(&buf[i], &vallen);
                char val[vallen];
                memcpy(&val[0], &buf[i], vallen);
                i += vallen;
                hash[std::string(field, fieldlen)] = std::string(val, vallen);
            }
            map[std::string(key, keylen)] = std::move(hash);
        } else {
            i++;
            i += rdbLoadLen(&buf[i], &keylen);
            char key[keylen];
            memcpy(&key[0], &buf[i], keylen);
            i += keylen;
            uint64_t setlen;
            i += rdbLoadLen(&buf[i], &setlen);
            DB::Set set;
            while (setlen-- > 0) {
                i += rdbLoadLen(&buf[i], &vallen);
                char val[vallen];
                memcpy(&val[0], &buf[i], vallen);
                i += vallen;
                set.insert(std::string(val, vallen));
            }
            map[std::string(key, keylen)] = std::move(set);
        }
    }
    munmap(start, size);
    close(fd);
}

// 借用redis中的两个编解码长度的函数
int Rdb::rdbSaveLen(uint64_t len)
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
    }
    return writeBytes;
}


int Rdb::rdbLoadLen(char *ptr, uint64_t *lenptr)
{
    unsigned char buf[2] = { (unsigned char)ptr[0], 0 };
    int type = (buf[0] & 0xc0) >> 6;
    size_t readBytes = 0;

    if (type == rdb_6bit_len) {
        *lenptr = buf[0] & 0x3f;
        readBytes = 1;
    } else if (type == rdb_14bit_len) {
        buf[1] = (unsigned char)ptr[1];
        *lenptr = ((buf[0] & 0x3f) << 8) | buf[1];
        readBytes = 2;
    } else if (type == rdb_32bit_len) {
        uint32_t len32 = *(uint32_t*)&ptr[1];
        *lenptr = ntohl(len32);
        readBytes = 5;
    }
    return readBytes;
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
    write(_fd, _buffer.data(), _buffer.size());
    _buffer.clear();
}
