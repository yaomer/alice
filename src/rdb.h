#ifndef _ALICE_SRC_RDB_H
#define _ALICE_SRC_RDB_H

#include <string.h>
#include <string>
#include "db.h"

namespace Alice {

class Rdb {
public:
    using Pair = std::pair<DB::Key, Value>;

    static const size_t buffer_flush_size = 4096;

    explicit Rdb(DBServer *dbServer)
        : _dbServer(dbServer),
        _childPid(-1),
        _curDb(nullptr),
        _fd(-1)
    {

    }
    void save();
    void saveBackground();
    pid_t childPid() { return _childPid; }
    void childPidReset() { _childPid = -1; }
    void load();
    std::string& syncBuffer() { return _syncBuffer; }
    void appendSyncBuffer(Context::CommandList& cmdlist);
private:
    int saveLen(uint64_t len);
    int loadLen(char *ptr, uint64_t *lenptr);
    void saveString(Pair pair);
    void saveList(Pair pair);
    void saveSet(Pair pair);
    void saveHash(Pair pair);
    void saveZset(Pair pair);
    char *loadString(char *ptr, int64_t *tvptr);
    char *loadList(char *ptr, int64_t *tvptr);
    char *loadSet(char *ptr, int64_t *tvptr);
    char *loadHash(char *ptr, int64_t *tvptr);
    char *loadZset(char *ptr, int64_t *tvptr);
    void append(const std::string& data);
    void append(const void *data, size_t len);
    void flush();

    DBServer *_dbServer;
    pid_t _childPid;
    std::string _buffer;
    std::string _syncBuffer;
    DB *_curDb;
    int _fd;
};
}

#endif // _ALICE_SRC_RDB_H
