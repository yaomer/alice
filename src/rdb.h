#ifndef _ALICE_SRC_RDB_H
#define _ALICE_SRC_RDB_H

#include <string.h>
#include <string>

#include "db.h"

namespace Alice {

class Rdb {
public:
    using iterator = DB::HashMap::iterator;

    static const size_t buffer_flush_size = 4096;
    static const size_t compress_limit = 30;

    explicit Rdb(DBServer *dbServer)
        : _dbServer(dbServer),
        _childPid(-1),
        _curdb(nullptr),
        _fd(-1)
    {

    }
    void save();
    void saveBackground();
    pid_t childPid() { return _childPid; }
    void childPidReset() { _childPid = -1; }
    void load();
    std::string& syncBuffer() { return _syncBuffer; }
    void appendSyncBuffer(const Context::CommandList& cmdlist, const char *query, size_t len);
private:
    int saveLen(uint64_t len);
    int loadLen(char *ptr, uint64_t *lenptr);
    void saveKey(const std::string& key);
    void saveValue(const std::string& value);
    void saveString(const iterator& it);
    void saveList(const iterator& it);
    void saveSet(const iterator& it);
    void saveHash(const iterator& it);
    void saveZset(const iterator& it);
    void loadExpireKey(const std::string& key, int64_t *tvptr);
    char *loadKey(char *ptr, std::string *key);
    char *loadValue(char *ptr, std::string *value);
    char *loadString(char *ptr, int64_t *tvptr);
    char *loadList(char *ptr, int64_t *tvptr);
    char *loadSet(char *ptr, int64_t *tvptr);
    char *loadHash(char *ptr, int64_t *tvptr);
    char *loadZset(char *ptr, int64_t *tvptr);
    void append(const std::string& data);
    void append(const void *data, size_t len);
    void flush();

    bool canCompress(size_t value_len);
    void compress(const char *input, size_t input_len, std::string *output);
    void uncompress(const char *compressed, size_t compressed_len, std::string *origin);

    DBServer *_dbServer;
    pid_t _childPid;
    std::string _buffer;
    std::string _syncBuffer;
    DB *_curdb;
    int _fd;
};
}

#endif // _ALICE_SRC_RDB_H
