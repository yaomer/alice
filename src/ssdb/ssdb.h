#ifndef _ALICE_SRC_SSDB_H
#define _ALICE_SRC_SSDB_H

#include <leveldb/db.h>
#include <leveldb/write_batch.h>
#include <leveldb/cache.h>

#include <assert.h>

#include <vector>
#include <string>
#include <unordered_map>

#include "../db_base.h"
#include "../config.h"

#include "coder.h"

namespace alice {

namespace ssdb {

class engine;
class DB;

class engine : public db_base_t {
public:
    engine();
    void server_cron() override
    {

    }
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
    void creat_snapshot() override {  }
    bool is_creating_snapshot() override { return 0; }
    bool is_created_snapshot() override { return 0; }
    std::string get_snapshot_name() override { return ""; }
    void load_snapshot() override {  }
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
private:
    std::unordered_map<std::string, command_t> cmdtable;
    std::unique_ptr<DB> db;
};

class DB {
public:
    using key_t = std::string;
    DB()
    {
        leveldb::Options ops;
        ops.create_if_missing = server_conf.ssdb_leveldb_create_if_missing;
        ops.max_open_files = server_conf.ssdb_leveldb_max_open_files;
        ops.max_file_size = server_conf.ssdb_leveldb_max_file_size;
        ops.write_buffer_size = server_conf.ssdb_leveldb_write_buffer_size;
        auto s = leveldb::DB::Open(ops, server_conf.ssdb_leveldb_dbname, &db);
        if (!s.ok()) log_fatal("leveldb: %s", s.ToString().c_str());
        set_builtin_keys();
    }
    ~DB()
    {
        delete db;
    }

    void del_key(const key_t& key);
    void del_expire_key(const key_t& key)
    {
        expire_keys.erase(key);
    }
    void del_key_with_expire(const key_t& key)
    {
        del_expire_key(key);
        del_key(key);
    }

    void check_expire(const key_t& key);

    void keys(context_t& con);
    void del(context_t& con);

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
private:
    void set_builtin_keys();
    bool is_not_type(const std::string& value, int type)
    {
        return atoi(value.c_str()) != type;
    }
    void del_list_key(const key_t& key);
    void del_string_key(const key_t& key){}
    void del_hash_key(const key_t& key){}
    void del_set_key(const key_t& key){}
    void del_zset_key(const key_t& key){}

    void _lpushx(context_t& con, bool is_lpushx);
    void _lpop(context_t& con, bool is_lpop);

    leveldb::DB *db;
    std::unordered_map<key_t, int64_t> expire_keys;
};

struct builtin_keys_t {
    const char *location = "@"; // 定位主键的起始位置
    const char *size = "$size$"; // 存储的总键数
};

extern builtin_keys_t builtin_keys;

}
}

#endif // _ALICE_SRC_SSDB_H
