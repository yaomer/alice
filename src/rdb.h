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
        _bgSavePid(0),
        _fd(-1) 
    { 
        bzero(_tmpFilename, sizeof(_tmpFilename));
    }
    void save();
    void backgroundSave();
    pid_t bgSavePid() { return _bgSavePid; }
    void saveString(Pair pair);
    void saveList(Pair pair);
    void saveSet(Pair pair);
    void saveHash(Pair pair);
    int saveLen(uint64_t len);
    int loadLen(char *ptr, uint64_t *lenptr);
    void load();
    char *loadString(char *ptr, int64_t *timevalptr);
    char *loadList(char *ptr, int64_t *timevalptr);
    char *loadSet(char *ptr, int64_t *timevalptr);
    char *loadHash(char *ptr, int64_t *timevalptr);
    void append(const std::string& data);
    void append(const void *data, size_t len);
    void flush();
private:
    DBServer *_dbServer;
    pid_t _bgSavePid;
    std::string _buffer;
    char _tmpFilename[16];
    int _fd;
};
}

#endif // _ALICE_SRC_RDB_H
