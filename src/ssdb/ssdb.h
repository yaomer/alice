#ifndef _ALICE_SRC_SSDB_H
#define _ALICE_SRC_SSDB_H

#include <leveldb/db.h>
#include <leveldb/write_batch.h>
#include <leveldb/comparator.h>
#include <leveldb/cache.h>

#include <assert.h>

#include <vector>
#include <string>
#include <unordered_map>
#include <optional> // for c++2a

#include "../db_base.h"
#include "../config.h"

namespace alice {

namespace ssdb {

class engine;
class DB;

class engine : public db_base_t {
public:
    engine();
    void server_cron() override;
    void set_context(const angel::connection_ptr& conn)
    {
        conn->set_context(context_t(conn.get(), this));
    }
    void connection_handler(const angel::connection_ptr& conn) override
    {
        set_context(conn);
    }
    void close_handler(const angel::connection_ptr& conn) override
    {

    }
    void slave_connection_handler(const angel::connection_ptr& conn) override
    {
        set_context(conn);
    }
    void creat_snapshot() override;
    bool is_creating_snapshot() override;
    bool is_created_snapshot() override;
    std::string get_snapshot_name() override;
    void load_snapshot() override;
    command_t *find_command(const std::string& name) override
    {
        auto it = cmdtable.find(name);
        if (it != cmdtable.end())
            return &it->second;
        else
            return nullptr;
    }
    void do_after_exec_write_cmd(const argv_t& argv, const char *query, size_t len) override
    {

    }
    void check_expire_keys();
private:
    std::unordered_map<std::string, command_t> cmdtable;
    std::unique_ptr<DB> db;
};

struct keycomp : public leveldb::Comparator {
    virtual ~keycomp() {  }
    int Compare(const leveldb::Slice& l, const leveldb::Slice& r) const
    {
        if (l[0] == 'l' && r[0] == 'l') { // compare list key
            auto begin1 = l.data() + 1, begin2 = r.data() + 1;
            auto s1 = strrchr(begin1, ':');
            auto s2 = strrchr(begin2, ':');
            leveldb::Slice key1(begin1, s1-begin1), key2(begin2, s2-begin2);
            int r = key1.compare(key2);
            if (r) return r;
            auto i1 = atoi(s1+1), i2 = atoi(s2+1);
            return i1 - i2;
        } else if (l[0] == 'z' && r[0] == 'z') {
            uint64_t seq1, seq2;
            auto begin1 = const_cast<char*>(l.data()) + 1;
            auto begin2 = const_cast<char*>(r.data()) + 1;
            auto s1 = begin1 + load_len(begin1, &seq1);
            auto s2 = begin2 + load_len(begin2, &seq2);
            if (seq1 != seq2) return seq1 - seq2;
            auto score1 = atof(s1), score2 = atof(s2);
            if (score1 < score2) return -1;
            if (score1 > score2) return 1;
            s1 = strrchr(s1, ':') + 1;
            s2 = strrchr(s2, ':') + 1;
            leveldb::Slice member1(s1, begin1+l.size()-s1), member2(s2, begin2+r.size()-s2);
            return member1.compare(member2);
        }
        return l.compare(r);
    }
    const char* Name() const { return "ssdb-keycomp"; }
    void FindShortestSeparator(std::string* start,
                               const leveldb::Slice& limit) const {  }
    void FindShortSuccessor(std::string* key) const {  }
};

using errstr_t = std::optional<leveldb::Status>;

class DB {
public:
    using key_t = std::string;
    DB()
    {
        leveldb::Options ops = config_leveldb_options();
        auto s = leveldb::DB::Open(ops, server_conf.ssdb_leveldb_dbname, &db);
        if (!s.ok()) log_fatal("leveldb: %s", s.ToString().c_str());
        set_builtin_keys();
    }
    ~DB()
    {
        delete db;
    }
    void reload()
    {
        delete db;
        leveldb::Options ops = config_leveldb_options();
        auto s = leveldb::DB::Open(ops, server_conf.ssdb_leveldb_dbname, &db);
        if (!s.ok()) log_fatal("leveldb: %s", s.ToString().c_str());
        set_builtin_keys();
    }
    leveldb::Options config_leveldb_options()
    {
        leveldb::Options ops;
        ops.create_if_missing = server_conf.ssdb_leveldb_create_if_missing;
        ops.max_open_files = server_conf.ssdb_leveldb_max_open_files;
        ops.max_file_size = server_conf.ssdb_leveldb_max_file_size;
        ops.write_buffer_size = server_conf.ssdb_leveldb_write_buffer_size;
        ops.comparator = &comp;
        return ops;
    }

    void add_expire_key(const key_t& key, int64_t expire)
    {
        expire_keys[key] = expire;
    }
    errstr_t del_key(const key_t& key);
    errstr_t del_key_batch(leveldb::WriteBatch *batch, const key_t& key);
    void del_expire_key(const key_t& key)
    {
        expire_keys.erase(key);
    }
    errstr_t del_key_with_expire(const key_t& key)
    {
        del_expire_key(key);
        return del_key(key);
    }
    errstr_t del_key_with_expire_batch(leveldb::WriteBatch *batch, const key_t& key)
    {
        del_expire_key(key);
        return del_key_batch(batch, key);
    }

    void check_expire(const key_t& key);

    void keys(context_t& con);
    void del(context_t& con);

    void set(context_t& con);
    void setnx(context_t& con);
    void get(context_t& con);
    void getset(context_t& con);
    void strlen(context_t& con);
    void append(context_t& con);
    void mset(context_t& con);
    void mget(context_t& con);
    void incr(context_t& con);
    void incrby(context_t& con);
    void decr(context_t& con);
    void decrby(context_t& con);
    void setrange(context_t& con);
    void getrange(context_t& con);

    void lpush(context_t& con);
    void lpushx(context_t& con);
    void rpush(context_t& con);
    void rpushx(context_t& con);
    void lpop(context_t& con);
    void rpop(context_t& con);
    void rpoplpush(context_t& con);
    void lrem(context_t& con);
    void llen(context_t& con);
    void lindex(context_t& con);
    void lset(context_t& con);
    void lrange(context_t& con);
    void ltrim(context_t& con);
    // void blpop(context_t& con);
    // void brpop(context_t& con);
    // void brpoplpush(context_t& con);

    void hset(context_t& con);
    void hsetnx(context_t& con);
    void hget(context_t& con);
    void hexists(context_t& con);
    void hdel(context_t& con);
    void hlen(context_t& con);
    void hstrlen(context_t& con);
    void hincrby(context_t& con);
    void hmset(context_t& con);
    void hmget(context_t& con);
    void hkeys(context_t& con);
    void hvals(context_t& con);
    void hgetall(context_t& con);
private:
    void set_builtin_keys();
    bool is_not_type(const std::string& value, int type)
    {
        return atoi(value.c_str()) != type;
    }
    // return err-str if error else return null
    errstr_t del_list_key(const key_t& key);
    errstr_t del_list_key_batch(leveldb::WriteBatch *batch, const key_t& key);
    errstr_t del_string_key(const key_t& key);
    errstr_t del_string_key_batch(leveldb::WriteBatch *batch, const key_t& key);
    errstr_t del_hash_key(const key_t& key);
    errstr_t del_hash_key_batch(leveldb::WriteBatch *batch, const key_t& key);
    // errstr_t del_set_key(const key_t& key);
    // errstr_t del_set_key_batch(leveldb::WriteBatch *batch, const key_t& key);
    // errstr_t del_zset_key(const key_t& key);
    // errstr_t del_zset_key_batch(leveldb::WriteBatch *batch, const key_t& key);

    size_t get_next_seq();

    void _lpushx(context_t& con, bool is_lpushx);
    void _lpop(context_t& con, bool is_lpop);
    void _incr(context_t& con, int64_t incr);
    void _hget(context_t& con, int what);

    leveldb::DB *db;
    std::unordered_map<key_t, int64_t> expire_keys;
    keycomp comp;
    friend engine;
};

struct builtin_keys_t {
    const char *location = "@"; // 定位主键的起始位置
    const char *size = "$size$"; // 存储的总键数
    const char *seq = "$seq$";
};

struct ktype {
    static const char meta     = '@';
    static const char tstring  = 's';
    static const char tlist    = 'l';
    static const char thash    = 'h';
    static const char tset     = 'S';
    static const char tzset    = 'z';
};

static inline const char
get_type(const std::string& value)
{
    return value[0];
}

static inline std::string
encode_meta_key(const std::string& key)
{
    std::string buf;
    buf.append(1, ktype::meta);
    buf.append(key);
    return buf;
}

extern builtin_keys_t builtin_keys;

}
}

#endif // _ALICE_SRC_SSDB_H
