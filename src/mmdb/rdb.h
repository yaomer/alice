#ifndef _ALICE_SRC_MMDB_RDB_H
#define _ALICE_SRC_MMDB_RDB_H

#include "mmdb.h"

namespace alice {

namespace mmdb {

class Rdb {
public:
    using iterator = DB::iterator;

    static const size_t buffer_flush_size = 4096;

    explicit Rdb(engine *engine)
        : engine(engine),
        child_pid(-1),
        cur_db(nullptr),
        fd(-1)
    {
    }
    void save();
    void save_background();
    pid_t get_child_pid() const { return child_pid; }
    bool doing() { return child_pid != -1; }
    void done() { child_pid = -1; }
    void load();
private:
    int save_len(uint64_t len);
    void save_key(const std::string& key);
    void save_value(const std::string& value);
    void save_string(const iterator& it);
    void save_list(const iterator& it);
    void save_set(const iterator& it);
    void save_hash(const iterator& it);
    void save_zset(const iterator& it);
    void load_expire_key(const std::string& key, int64_t *tvptr);
    char *load_key(char *ptr, std::string *key);
    char *load_value(char *ptr, std::string *value);
    char *load_string(char *ptr, int64_t *tvptr);
    char *load_list(char *ptr, int64_t *tvptr);
    char *load_set(char *ptr, int64_t *tvptr);
    char *load_hash(char *ptr, int64_t *tvptr);
    char *load_zset(char *ptr, int64_t *tvptr);
    void append(const std::string& data);
    void append(const void *data, size_t len);
    void flush();

    bool can_compress(size_t value_len);
    void compress(const char *input, size_t input_len, std::string *output);
    void uncompress(const char *compressed, size_t compressed_len, std::string *origin);

    engine *engine;
    pid_t child_pid;
    std::string buffer;
    DB *cur_db;
    int fd;
};
}
}

#endif
