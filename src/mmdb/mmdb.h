#ifndef _ALICE_SRC_MMDB_H
#define _ALICE_SRC_MMDB_H

#include <memory>
#include <unordered_map>
#include <string>
#include <functional>
#include <list>
#include <vector>
#include <unordered_set>
#include <algorithm>
#include <set>
#include <deque>
#include <tuple>
#include <any>

#include "../db_base.h"
#include "../skiplist.h"

namespace alice {

namespace mmdb {

class engine;
class Rdb;
class Aof;
class DB;

class engine : public db_base_t {
public:
    enum Flag {
        // 有rewriteaof请求被延迟
        REWRITEAOF_DELAY = 0x04,
    };
    engine();
    void start() override;
    void exit() override;
    void server_cron() override;
    void set_context(const angel::connection_ptr& conn)
    {
        conn->set_context(context_t(conn.get(), this));
    }
    void connection_handler(const angel::connection_ptr& conn) override
    {
        set_context(conn);
    }
    void close_handler(const angel::connection_ptr& conn) override;
    void slave_connection_handler(const angel::connection_ptr& conn) override
    {
        set_context(conn);
    }
    void free_memory_if_needed() override;
    void creat_snapshot() override;
    bool is_creating_snapshot() override;
    bool is_created_snapshot() override;
    std::string get_snapshot_name() override;
    void load_snapshot() override;
    command_t *find_command(const std::string& name) override;
    void do_after_exec_write_cmd(const argv_t& argv, const char *query, size_t len) override;
    void check_blocked_clients();
    void check_expire_keys();

    DB *db() { return dbs[cur_db_num].get(); }
    void switch_db(int dbnum) { cur_db_num = dbnum; }
    DB* select_db(int dbnum) { return dbs[dbnum].get(); }
    int get_cur_db_num() const { return cur_db_num; }
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
    void clear();
    int flags = 0;
    std::vector<std::unique_ptr<DB>> dbs;
    std::unique_ptr<Rdb> rdb;
    std::unique_ptr<Aof> aof;
    time_t last_save_time = angel::util::get_cur_time_ms(); // 上一次进行rdb持久化的时间
private:
    void evict_all_keys_with_lru();
    void evict_volatile_with_lru();
    void evict_all_keys_with_random();
    void evict_volatile_with_random();
    void evict_volatile_with_ttl();
    template <typename T>
    void evict_key(const T& hash, int (evict)[2])
    {
        for (auto it = hash.cbegin(evict[0]); it != hash.end(evict[0]); ++it) {
            if (evict[1]-- == 0) {
                evict_key(it->first);
                break;
            }
        }
    }
    void evict_key(const std::string& key);

    int cur_db_num = 0;
    int cur_check_db = 0;
    size_t dirty = 0; // 执行的写命令数
    std::list<size_t> blocked_clients;
};

// 表示一个键值对的值
struct Value {
public:
    Value() : value(0), lru(lru_clock) {  }
    Value(std::any&& value) : value(value), lru(lru_clock)
    {
    }
    Value& operator=(std::any&& value) noexcept
    {
        this->value = value;
        this->lru = lru_clock;
        return *this;
    }
    std::any value;
    // 最近一次访问该键的时间，用于进行lru内存淘汰
    int64_t lru;
};

// 排序时会为每个待排序的元素(e)创建一个sortobj对象(s)，s->value存储
// &e->value，s->u的值取决于按哪种方式进行排序
struct sortobj {
    sortobj(const std::string *value)
        : value(value)
    {
        u.cmpval = value;
    }
    const std::string *value;
    union {
        double score;
        const std::string *cmpval;
    } u;
};

struct zslkey {
    zslkey() {  }
    zslkey(double score, const std::string& key)
        : score(score), key(key) {  }
    double score;
    std::string key;
};

// 对于对象l和r，如果l.score < r.score，就认为l < r
// 否则如果l.score == r.score，就继续比较键值，如果l.key < r.key，
// 就认为l < r，否则就认为l > r
class zslkeycmp {
public:
    bool operator()(const zslkey& lhs, const zslkey& rhs) const
    {
        if (lhs.score < rhs.score) {
            return true;
        } else if (lhs.score == rhs.score) {
            if (lhs.key.size() > 0 && lhs.key.compare(rhs.key) < 0)
                return true;
        }
        return false;
    }
};

struct Zset {
    using iterator = skiplist<zslkey, bool, zslkeycmp>::iterator;
    bool empty() const { return zmap.empty(); }
    size_t size() const { return zmap.size(); }
    void insert(double score, const std::string& key)
    {
        zsl.insert(zslkey(score, key), false);
        auto it = zmap.emplace(key, score);
        if (!it.second) zmap[key] = score;
    }
    void erase(double score, const std::string& key)
    {
        zsl.erase(zslkey(score, key));
        zmap.erase(key);
    }
    size_t order_of_key(double score, const std::string& key)
    {
        return zsl.order_of_key(zslkey(score, key));
    }
    iterator lower_bound(double score, const std::string& key)
    {
        return zsl.lower_bound(zslkey(score, key));
    }
    iterator upper_bound(double score, const std::string& key)
    {
        return zsl.upper_bound(zslkey(score, key));
    }
    // value(bool)不作使用
    skiplist<zslkey, bool, zslkeycmp> zsl;
    // 根据一个member可以在常数时间找到其score
    std::unordered_map<std::string, double> zmap;
};

class DB {
public:
    using key_t = std::string;
    using dict_t = std::unordered_map<key_t, Value>;
    using expire_keys_t = std::unordered_map<key_t, int64_t>;
    using watch_keys_t = std::unordered_map<key_t, std::vector<size_t>>;
    using iterator = std::unordered_map<key_t, Value>::iterator;
    // value-type
    using String = std::string;
    using List = std::list<std::string>;
    using Set = std::unordered_set<std::string>;
    using Hash = std::unordered_map<std::string, std::string>;
    // 因为排序结果集需要剪切，所以deque优于vector
    using sobj_list = std::deque<sortobj>;
    explicit DB(engine *);
    ~DB() {  }
    dict_t& get_dict() { return dict; }
    expire_keys_t& get_expire_keys() { return expire_keys; }
    watch_keys_t& get_watch_keys() { return watch_keys; }
    void del_key(const key_t& key) { dict.erase(key); }
    void del_expire_key(const key_t& key) { expire_keys.erase(key); }
    void del_key_with_expire(const key_t& key)
    {
        dict.erase(key);
        expire_keys.erase(key);
    }

    template <typename T>
    void add_key(const key_t& key, const T& value)
    {
        dict.emplace(key, value);
    }

    void add_expire_key(const key_t& key, int64_t expire)
    {
        expire_keys[key] = expire;
    }

    void clear();

    void check_expire(const key_t& key);

    template <typename T>
    void del_key_if_empty(const T& container, const key_t& key)
    {
        if (container.empty())
            del_key_with_expire(key);
    }

    command_t *find_command(const std::string& name)
    {
        auto it = cmdtable.find(name);
        if (it != cmdtable.end())
            return &it->second;
        else
            return nullptr;
    }

    void touch_watch_key(const key_t& key);
    void clear_blocking_keys_for_context(context_t& con);

    void select(context_t& con);
    void exists(context_t& con);
    void type(context_t& con);
    void ttl(context_t& con);
    void pttl(context_t& con);
    void expire(context_t& con);
    void pexpire(context_t& con);
    void del(context_t& con);
    void keys(context_t& con);
    void save(context_t& con);
    void bgsave(context_t& con);
    void bgrewriteaof(context_t& con);
    void lastsave(context_t& con);
    void flushdb(context_t& con);
    void flushall(context_t& con);
    void dbsize(context_t& con);
    void rename(context_t& con);
    void renamenx(context_t& con);
    void lru(context_t& con);
    // transaction command
    void multi(context_t& con);
    void exec(context_t& con);
    void discard(context_t& con);
    void watch(context_t& con);
    void unwatch(context_t& con);
    void move(context_t& con);
    void sort(context_t& con);
    // string operations
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
    // list operations
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
    // hash operations
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
    // set operations
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
    // zset operations
    void zadd(context_t& con);
    void zscore(context_t& con);
    void zincrby(context_t& con);
    void zcard(context_t& con);
    void zcount(context_t& con);
    void zrange(context_t& con);
    void zrevrange(context_t& con);
    void zrank(context_t& con);
    void zrevrank(context_t& con);
    void zrangebyscore(context_t& con);
    void zrevrangebyscore(context_t& con);
    void zrem(context_t& con);
    void zremrangebyrank(context_t& con);
    void zremrangebyscore(context_t& con);

    iterator find(const key_t& key)
    {
        return dict.find(key);
    }
    bool not_found(const iterator& it)
    {
        return it == dict.end();
    }
    bool not_found(const key_t& key)
    {
        return not_found(find(key));
    }
    template <typename T>
    void insert(const key_t& key, const T& value)
    {
        auto it = dict.emplace(key, value);
        // emplace()和insert()都不会覆盖已存在的键
        if (!it.second)
            dict[key] = std::move(value);
        if (typeid(T) == typeid(List))
            blocking_pop(key);
    }
private:
    void _ttl(context_t& con, bool is_ttl);
    void _expire(context_t& con, bool is_expire);
    void _unwatch(context_t& con);
    void _incr(context_t& con, int64_t incr);
    void _lpush(context_t& con, bool is_lpush);
    void _lpushx(context_t& con, bool is_lpushx);
    void _lpop(context_t& con, bool is_lpop);
    void _rpoplpush(context_t& con, bool is_nonblock);
    void _blpop(context_t& con, bool is_blpop);
    void _hget(context_t& con, int what);
    void _sinter(context_t& con, Set& rset, int start);
    void _sunion(context_t& con, Set& rset, int start);
    void sreply(context_t& con, Set& rset);
    void sstore(context_t& con, Set& rset);
    void _zrange(context_t& con, bool reverse);
    void _zrank(context_t& con, bool reverse);
    void _zrangebyscore(context_t& con, bool reverse);

    void add_blocking_key(context_t& con, const key_t& key);
    void set_context_to_block(context_t& con, int timeout);
    void blocking_pop(const key_t& key);

    int sort_get_result(context_t& con, sobj_list& result, const key_t& key, unsigned& cmdops);
    void sort_by_pattern(sobj_list& result, const key_t& by, unsigned& cmdops);
    void sort_by_get_keys(sobj_list& result, const std::vector<std::string>& getset, unsigned cmdops);
    void sort_store(sobj_list& result, const key_t& des, unsigned cmdops);

    CommandTable cmdtable;
    std::unordered_map<key_t, Value> dict;
    // <键，键的到期时间>
    std::unordered_map<key_t, int64_t> expire_keys;
    // <键，监视该键的客户端列表>
    std::unordered_map<key_t, std::vector<size_t>> watch_keys;
    // 保存所有阻塞的键，每个键的值是阻塞于它的客户端列表
    std::unordered_map<key_t, std::list<size_t>> blocking_keys;
    engine *engine;
};

}
}

#endif
