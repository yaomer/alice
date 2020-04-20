#ifndef _ALICE_SRC_AOF_H
#define _ALICE_SRC_AOF_H

#include <string>

#include "db.h"

namespace Alice {

class DBServer;

class Aof {
public:
    using iterator = DB::HashMap::iterator;

    static const size_t buffer_flush_size = 4096;
    static const size_t rewrite_min_filesize = 16 * 1024 * 1024;
    static const size_t rewrite_rate = 2;

    explicit Aof(DBServer *dbServer);
    void append(const Context::CommandList& cmdlist, const char *query, size_t len);
    void appendRewriteBuffer(const Context::CommandList& cmdlist, const char *query, size_t len);
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
    void rewriteSelectDb(int dbnum);
    void rewriteExpire(const DB::Key& key, int64_t expire);
    void rewriteString(const iterator& it);
    void rewriteList(const iterator& it);
    void rewriteSet(const iterator& it);
    void rewriteHash(const iterator& it);
    void rewriteZset(const iterator& it);

    DBServer *_dbServer;
    std::string _buffer;
    std::string _rewriteBuffer;
    pid_t _childPid;
    int64_t _lastSyncTime;
    size_t _currentFilesize;
    size_t _lastRewriteFilesize;
    int _fd;
    char tmpfile[16];
};
}

#endif // _ALICE_SRC_AOF_H
