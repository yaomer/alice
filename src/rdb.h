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
        _fd(-1) 
    { 
        bzero(_tmpFilename, sizeof(_tmpFilename));
    }
    void rdbSave();
    void rdbSaveString(Pair pair);
    void rdbSaveList(Pair pair);
    void rdbSaveSet(Pair pair);
    void rdbSaveHash(Pair pair);
    int rdbSaveLen(uint64_t len);
    int rdbLoadLen(char *ptr, uint64_t *lenptr);
    void rdbRecover();
    void append(const std::string& data);
    void append(const void *data, size_t len);
    void flush();
private:
    DBServer *_dbServer;
    std::string _buffer;
    char _tmpFilename[16];
    int _fd;
};

}

#endif // _ALICE_SRC_RDB_H
