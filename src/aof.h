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
    static const size_t rewrite_min_filesize = 16 * 1024 * 1024;
    static const size_t rewrite_rate = 2;

    explicit Aof(DBServer *dbServer);
    int mode() const { return _mode; }
    void setMode(int mode) { _mode = mode; }
    void append(Context::CommandList& cmdlist);
    void appendRewriteBuffer(Context::CommandList& cmdlist);
    void appendAof(int64_t now);
    void appendRewriteBufferToAof();
    void load();
    void rewriteBackground();
    pid_t childPid() const { return _childPid; }
    void childPidReset() { _childPid = -1; }
    bool rewriteIsOk();
private:
    void append(const std::string& s);
    void flush();
    void rewrite();
    void rewriteExpire(const DB::Key& key, int64_t milliseconds);
    void rewriteString(Pair pair);
    void rewriteList(Pair pair);
    void rewriteSet(Pair pair);
    void rewriteHash(Pair pair);
    size_t getFilesize(int fd);

    DBServer *_dbServer;
    std::string _buffer;
    std::string _rewriteBuffer;
    int _mode;
    pid_t _childPid;
    int64_t _lastSyncTime;
    size_t _currentFilesize;
    size_t _lastRewriteFilesize;
    int _fd;
    char tmpfile[16];
};
}

#endif // _ALICE_SRC_AOF_H
