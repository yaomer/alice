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
#include <list>
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
    void watch(context_t& con) override;
    void unwatch(context_t& con) override;
    void check_expire_keys();
    void check_blocked_clients();

    void add_block_client(size_t id)
    {
        blocked_clients.push_back(id);
    }
    void del_block_client(size_t id)
    {
        for (auto it = blocked_clients.begin(); it != blocked_clients.end(); ++it)
            if (*it == id) {
                blocked_clients.erase(it);
                break;
            }
    }
private:
    std::unordered_map<std::string, command_t> cmdtable;
    std::unique_ptr<DB> db;
    std::list<size_t> blocked_clients;
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
        } else if (l[0] == 'Z' && r[0] == 'Z') {
            uint64_t seq1, seq2;
            auto begin1 = const_cast<char*>(l.data()) + 1;
            auto begin2 = const_cast<char*>(r.data()) + 1;
            auto s1 = begin1 + load_len(begin1, &seq1);
            auto s2 = begin2 + load_len(begin2, &seq2);
            if (seq1 != seq2) return seq1 - seq2;
            auto score1 = atof(s1), score2 = atof(s2);
            if (score1 < score2) return -1;
            if (score1 > score2) return 1;
            s1 = strchr(s1, ':') + 1;
            s2 = strchr(s2, ':') + 1;
            leveldb::Slice member1(s1, begin1-1+l.size()-s1);
            leveldb::Slice member2(s2, begin2-1+r.size()-s2);
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
    DB(engine *e) : engine(e)
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
    void clear();

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

    void rename_key(leveldb::WriteBatch *batch, const key_t& key,
                    const std::string& value, const key_t& newkey);

    void check_expire(const key_t& key);
    void touch_watch_key(const key_t& key);

    void watch(context_t& con);
    void unwatch(context_t& con);

    void keys(context_t& con);
    void del(context_t& con);
    void exists(context_t& con);
    void type(context_t& con);
    void ttl(context_t& con);
    void pttl(context_t& con);
    void expire(context_t& con);
    void pexpire(context_t& con);
    void flushdb(context_t& con);
    void flushall(context_t& con);
    void rename(context_t& con);
    void renamenx(context_t& con);

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
    void blpop(context_t& con);
    void brpop(context_t& con);
    void brpoplpush(context_t& con);

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

    void sadd(context_t& con);
    void sismember(context_t& con);
    void spop(context_t& con);
    void srandmember(context_t& con);
    void srem(context_t& con);
    void smove(context_t& con);
    void scard(context_t& con);
    void smembers(context_t& con);
    void sinter(context_t& con);
    void sinterstore(context_t& con);
    void sunion(context_t& con);
    void sunionstore(context_t& con);
    void sdiff(context_t& con);
    void sdiffstore(context_t& con);
private:
    void set_builtin_keys();
    // return err-str if error else return null
    errstr_t del_list_key(const key_t& key);
    errstr_t del_list_key_batch(leveldb::WriteBatch *batch, const key_t& key);
    errstr_t del_string_key(const key_t& key);
    errstr_t del_string_key_batch(leveldb::WriteBatch *batch, const key_t& key);
    errstr_t del_hash_key(const key_t& key);
    errstr_t del_hash_key_batch(leveldb::WriteBatch *batch, const key_t& key);
    errstr_t del_set_key(const key_t& key);
    errstr_t del_set_key_batch(leveldb::WriteBatch *batch, const key_t& key);
    // errstr_t del_zset_key(const key_t& key);
    // errstr_t del_zset_key_batch(leveldb::WriteBatch *batch, const key_t& key);

    void rename_string_key(leveldb::WriteBatch *batch, const key_t& key,
                           const std::string& meta_value, const key_t& newkey);
    void rename_list_key(leveldb::WriteBatch *batch, const key_t& key,
                         const std::string& meta_value, const key_t& newkey);
    void rename_hash_key(leveldb::WriteBatch *batch, const key_t& key,
                         const std::string& meta_value, const key_t& newkey);
    void rename_set_key(leveldb::WriteBatch *batch, const key_t& key,
                        const std::string& meta_value, const key_t& newkey);
    // void rename_zset_key(leveldb::WriteBatch *batch, const key_t& key,
                         // const std::string& meta_value, const key_t& newkey);

    uint64_t get_next_seq();

    void blocking_pop(const key_t& key);
    void clear_blocking_keys_for_context(context_t& con);
    void add_blocking_key(context_t& con, const key_t& key);
    void set_context_to_block(context_t& con, int timeout);

    void _ttl(context_t& con, bool is_ttl);
    void _expire(context_t& con, bool is_expire);
    void _rename(context_t& con, bool is_nx);
    void _lpushx(context_t& con, bool is_lpushx);
    void _lpop(context_t& con, bool is_lpop);
    void _blpop(context_t& con, bool is_blpop);
    void _rpoplpush(context_t& con, bool is_nonblock);
    void _incr(context_t& con, int64_t incr);
    void _hget(context_t& con, int what);
    void _sinter(context_t& con, std::unordered_set<std::string>& rset, int start);
    void _sunion(context_t& con, std::unordered_set<std::string>& rset, int start);
    void _sstore(context_t& con, std::unordered_set<std::string>& rset);

    leveldb::DB *db;
    std::unordered_map<key_t, int64_t> expire_keys;
    std::unordered_map<key_t, std::vector<size_t>> watch_keys;
    std::unordered_map<key_t, std::vector<size_t>> blocking_keys;
    engine *engine;
    keycomp comp;
    friend class engine;
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
    static const char tscore   = 'Z';
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
