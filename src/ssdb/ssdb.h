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

#define STRING  1
#define LIST    2
#define HASH    3
#define SET     4
#define ZSET    5

//
// string:
//     meta-key key@:[type:len]
//     real-key key$:value
// list:
//     meta-key key@:[type:left-number:right-number]
//     real-key [[key:number1$]:value1, [key:number2$]:value2 ...]
// hash:
//     meta-key key@:[type:size]
//     real-key [[key:field$]:value1, [key:field$]:value2 ...]
//

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
    DB()
    {
        leveldb::Options ops;
        ops.create_if_missing = server_conf.ssdb_leveldb_create_if_missing;
        ops.max_open_files = server_conf.ssdb_leveldb_max_open_files;
        ops.max_file_size = server_conf.ssdb_leveldb_max_file_size;
        ops.write_buffer_size = server_conf.ssdb_leveldb_write_buffer_size;
        auto status = leveldb::DB::Open(ops, server_conf.ssdb_leveldb_dbname, &db);
        assert(status.ok());
        set_builtin_keys();
    }
    ~DB()
    {
        delete db;
    }

    void keys(context_t& con);

    void lpush(context_t& con);
    void lpushx(context_t& con);
    void rpush(context_t& con);
    void rpushx(context_t& con);
    void lpop(context_t& con);
    void rpop(context_t& con);
    void rpoplpush(context_t& con);
    // void lrem(context_t& con);
    void llen(context_t& con);
    // void lindex(context_t& con);
    // void lset(context_t& con);
    void lrange(context_t& con);
    // void ltrim(context_t& con);
    // void blpop(context_t& con);
    // void brpop(context_t& con);
    // void brpoplpush(context_t& con);
private:
    void set_builtin_keys();
    bool is_not_type(const std::string& value, int type)
    {
        return atoi(value.c_str()) != type;
    }
    void del_list_key(const std::string& key);
    std::string get_meta_key(const std::string& key)
    {
        return "@" + key;
    }

    leveldb::DB *db;
};

struct builtin_keys_t {
    const char *location = "@"; // 定位主键的起始位置
};

extern builtin_keys_t builtin_keys;

}
}

#endif // _ALICE_SRC_SSDB_H
