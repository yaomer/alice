#ifndef _ALICE_SRC_MMDB_AOF_H
#define _ALICE_SRC_MMDB_AOF_H

#include "mmdb.h"

namespace alice {

namespace mmdb {

class engine;

class Aof {
public:
    using iterator = DB::iterator;

    static const size_t buffer_flush_size = 4096;
    static const size_t rewrite_min_filesize = 16 * 1024 * 1024;
    static const size_t rewrite_rate = 2;

    explicit Aof(engine *engine);
    pid_t get_child_pid() const { return child_pid; }
    void append(const argv_t& argv, const char *query, size_t len);
    void append_rewrite_buffer(const argv_t& argv, const char *query, size_t len);
    // void appendAof(int64_t now);
    void fsync();
    void fsync_rewrite_buffer();
    void load();
    void rewrite_background();
    bool doing() const { return child_pid != -1; }
    void done() { child_pid = -1; }
    bool can_rewrite();
private:
    void append(const std::string& s);
    void flush();
    void rewrite();
    void rewrite_select_db(int dbnum);
    void rewrite_expire(const std::string& key, int64_t expire);
    void rewrite_string(const iterator& it);
    void rewrite_list(const iterator& it);
    void rewrite_set(const iterator& it);
    void rewrite_hash(const iterator& it);
    void rewrite_zset(const iterator& it);

    engine *engine;
    std::string buffer;
    std::string rewrite_buffer;
    pid_t child_pid;
    time_t last_sync_time;
    size_t cur_file_size;
    size_t last_rewrite_file_size;
    char tmpfile[16];
    int fd;
};
}
}

#endif
