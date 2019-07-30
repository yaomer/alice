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

void Rdb::save()
{
    strcpy(_tmpFilename, "tmp.XXXXX");
    mktemp(_tmpFilename);
    _fd = open(_tmpFilename, O_RDWR | O_CREAT | O_APPEND, 0660);
    append(magic, 5);
    auto& map = _dbServer->db().hashMap();
    for (auto& it : map) {
        if (isXXType(it, DB::String)) {
            saveString(it);
        } else if (isXXType(it, DB::List)) {
            saveList(it);
        } else if (isXXType(it, DB::Set)) {
            saveSet(it);
        } else
            saveHash(it);
    }
    saveLen(eof);
    if (_buffer.size() > 0) flush();
    fsync(_fd);
    close(_fd);
    rename(_tmpFilename, "dump.rdb");
}

void Rdb::backgroundSave()
{
    _bgSavePid = fork();
    if (_bgSavePid == 0) {
        save();
        exit(0);
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
    }
    return readBytes;
}

void Rdb::saveString(Pair pair)
{
    saveLen(string_type);
    saveLen(pair.first.size());
    append(pair.first);
    DB::String& string = getXXType(pair, DB::String&);
    saveLen(string.size());
    append(string);
}

void Rdb::saveList(Pair pair)
{
    saveLen(list_type);
    saveLen(pair.first.size());
    append(pair.first);
    DB::List& list = getXXType(pair, DB::List&);
    saveLen(list.size());
    for (auto& it : list) {
        saveLen(it.size());
        append(it);
    }
}

void Rdb::saveSet(Pair pair)
{
    saveLen(set_type);
    saveLen(pair.first.size());
    append(pair.first);
    DB::Set& set = getXXType(pair, DB::Set&);
    saveLen(set.size());
    for (auto& it : set) {
        saveLen(it.size());
        append(it);
    }
}

void Rdb::saveHash(Pair pair)
{
    saveLen(hash_type);
    saveLen(pair.first.size());
    append(pair.first);
    DB::Hash& hash = getXXType(pair, DB::Hash&);
    saveLen(hash.size());
    for (auto& it : hash) {
        saveLen(it.first.size());
        append(it.first);
        saveLen(it.second.size());
        append(it.second);
    }
}

void Rdb::load()
{
    int fd = open("dump.rdb", O_RDONLY);
    struct stat st;
    fstat(fd, &st);
    size_t size = st.st_size;
    void *start = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
    if (start == MAP_FAILED) return;
    char *buf = reinterpret_cast<char*>(start);
    if (size < 5 || strncmp(buf, "ALICE", 5))
        return;
    buf += 5;
    uint64_t typelen;
    while (true) {
        loadLen(buf, &typelen);
        if (typelen == eof)
            break;
        if (buf[0] == string_type) {
            buf = loadString(buf + 1);
        } else if (buf[0] == list_type) {
            buf = loadList(buf + 1);
        } else if (buf[0] == set_type) {
            buf = loadSet(buf + 1);
        } else {
            buf = loadHash(buf + 1);
        }
    }
    munmap(start, size);
    close(fd);
}

char *Rdb::loadString(char *ptr)
{
    uint64_t keylen, vallen;
    ptr += loadLen(ptr, &keylen);
    char key[keylen];
    memcpy(key, ptr, keylen);
    ptr += keylen;
    ptr += loadLen(ptr, &vallen);
    char val[vallen];
    memcpy(val, ptr, vallen);
    ptr += vallen;
    auto& map = _dbServer->db().hashMap();
    map[std::string(key, keylen)] = std::string(val, vallen);
    return ptr;
}

char *Rdb::loadList(char *ptr)
{
    uint64_t keylen, vallen;
    ptr += loadLen(ptr, &keylen);
    char key[keylen];
    memcpy(key, ptr, keylen);
    ptr += keylen;
    uint64_t listlen;
    ptr += loadLen(ptr, &listlen);
    DB::List list;
    while (listlen-- > 0) {
        ptr += loadLen(ptr, &vallen);
        char val[vallen];
        memcpy(val, ptr, vallen);
        ptr += vallen;
        list.push_back(std::string(val, vallen));
    }
    auto& map = _dbServer->db().hashMap();
    map[std::string(key, keylen)] = std::move(list);
    return ptr;
}

char *Rdb::loadSet(char *ptr)
{
    uint64_t keylen, vallen;
    ptr += loadLen(ptr, &keylen);
    char key[keylen];
    memcpy(key, ptr, keylen);
    ptr += keylen;
    uint64_t setlen;
    ptr += loadLen(ptr, &setlen);
    DB::Set set;
    while (setlen-- > 0) {
        ptr += loadLen(ptr, &vallen);
        char val[vallen];
        memcpy(val, ptr, vallen);
        ptr += vallen;
        set.insert(std::string(val, vallen));
    }
    auto& map = _dbServer->db().hashMap();
    map[std::string(key, keylen)] = std::move(set);
    return ptr;
}

char *Rdb::loadHash(char *ptr)
{
    uint64_t keylen, vallen;
    ptr += loadLen(ptr, &keylen);
    char key[keylen];
    memcpy(key, ptr, keylen);
    ptr += keylen;
    uint64_t hashlen;
    ptr += loadLen(ptr, &hashlen);
    DB::Hash hash;
    uint64_t fieldlen;
    while (hashlen-- > 0) {
        ptr += loadLen(ptr, &fieldlen);
        char field[fieldlen];
        memcpy(field, ptr, fieldlen);
        ptr += fieldlen;
        ptr += loadLen(ptr, &vallen);
        char val[vallen];
        memcpy(val, ptr, vallen);
        ptr += vallen;
        hash[std::string(field, fieldlen)] = std::string(val, vallen);
    }
    auto& map = _dbServer->db().hashMap();
    map[std::string(key, keylen)] = std::move(hash);
    return ptr;
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
