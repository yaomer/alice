#ifndef _ALICE_SRC_AOF_H
#define _ALICE_SRC_AOF_H

#include <Angel/TimeStamp.h>
#include <string>
#include "db.h"

namespace Alice {

class DBServer;

class Aof {
public:
    enum { ALWAYS, EVERYSEC, NO };
    using Pair = std::pair<DB::Key, Value>;

    static const size_t buffer_flush_size = 4096;

    explicit Aof(DBServer *dbServer)
        : _dbServer(dbServer),
        _mode(EVERYSEC),
        _childPid(-1),
        _lastSyncTime(0),
        _fd(-1)
    {

    }
    int mode() const { return _mode; }
    void setMode(int mode) { _mode = mode; }
    void append(Context::CommandList& cmdlist);
    void appendAof(int64_t now);
    void load();
    void rewriteBackground();
    pid_t childPid() const { return _childPid; }
    void childPidReset() { _childPid = -1; }
private:
    void append(const std::string& s);
    void flush();
    void rewrite();
    void rewriteExpire(const DB::Key& key, int64_t milliseconds);
    void rewriteString(Pair pair);
    void rewriteList(Pair pair);
    void rewriteSet(Pair pair);
    void rewriteHash(Pair pair);

    DBServer *_dbServer;
    std::string _buffer;
    int _mode;
    pid_t _childPid;
    int64_t _lastSyncTime;
    int _fd;
};
}

#endif // _ALICE_SRC_AOF_H
