#include "../config.h"
#include "../util.h"
#include "../server.h"

#include "internal.h"
#include "rdb.h"
#include "aof.h"

#include <fcntl.h>

using namespace alice;
using namespace alice::mmdb;

using std::placeholders::_1;

#define BIND(f) std::bind(&DB::f, this, _1)

engine::engine()
    : rdb(new Rdb(this)),
    aof(new Aof(this))
{
    for (int i = 0; i < server_conf.mmdb_databases; i++) {
        std::unique_ptr<DB> db(new DB(this));
        dbs.emplace_back(std::move(db));
    }
}

void engine::start()
{
    // 优先使用AOF文件来载入数据
    if (is_file_exists(server_conf.mmdb_appendonly_file))
        aof->load();
    else
        rdb->load();
}

void engine::exit()
{
    rdb->save_background();
    aof->rewrite_background();
}

void engine::close_handler(const angel::connection_ptr& conn)
{
    auto& con = std::any_cast<context_t&>(conn->get_context());
    if (con.flags & context_t::CON_BLOCK) {
        DB *db = select_db(con.block_db_num);
        db->clear_blocking_keys_for_context(con);
        del_block_client(conn->id());
    }
}

void engine::do_after_exec_write_cmd(const argv_t& argv, const char *query, size_t len)
{
    dirty++;
    if (server_conf.mmdb_enable_appendonly)
        aof->append(argv, query, len);
    if (aof->doing())
        aof->append_rewrite_buffer(argv, query, len);
}

void engine::creat_snapshot()
{
    rdb->save_background();
}

bool engine::is_creating_snapshot()
{
    return rdb->doing();
}

bool engine::is_created_snapshot()
{
    if (rdb->doing()) {
        pid_t pid = waitpid(rdb->get_child_pid(), nullptr, WNOHANG);
        if (pid > 0) {
            rdb->done();
            log_info("DB saved on disk");
            return true;
        }
    }
    return false;
}

std::string engine::get_snapshot_name()
{
    return server_conf.mmdb_rdb_file;
}

void engine::load_snapshot()
{
    rdb->load();
}

void engine::server_cron()
{
    auto now = lru_clock;

    if (aof->doing()) {
        pid_t pid = waitpid(aof->get_child_pid(), nullptr, WNOHANG);
        if (pid > 0) {
            aof->done();
            aof->fsync_rewrite_buffer();
            log_info("Background AOF rewrite finished successfully");
        }
    }

    if (!rdb->doing() && !aof->doing()) {
        if (flags & REWRITEAOF_DELAY) {
            flags &= ~REWRITEAOF_DELAY;
            aof->rewrite_background();
        }
    }

    auto save_interval = (now - last_save_time) / 1000;
    for (auto& it : server_conf.mmdb_save_params) {
        int seconds = std::get<0>(it);
        int changes = std::get<1>(it);
        if (save_interval >= seconds && dirty >= changes) {
            if (!rdb->doing() && !aof->doing()) {
                log_info("%d changes in %d seconds. Saving...", changes, seconds);
                rdb->save_background();
                last_save_time = now;
                dirty = 0;
            }
            break;
        }
    }

    if (aof->can_rewrite()) {
        if (!rdb->doing() && !aof->doing()) {
            aof->rewrite_background();
        }
    }

    check_blocked_clients();

    check_expire_keys();

    aof->fsync();
}

// 检查是否有阻塞的客户端超时
void engine::check_blocked_clients()
{
    auto now = lru_clock;
    std::string message;
    for (auto it = blocked_clients.begin(); it != blocked_clients.end(); ) {
        auto e = it++;
        auto conn = __server->get_server().get_connection(*e);
        if (!conn) continue;
        auto& context = std::any_cast<context_t&>(conn->get_context());
        auto db = select_db(context.block_db_num);
        if (context.block_start_time + context.blocked_time <= now) {
            message.append("*-1\r\n+(");
            double seconds = 1.0 * (now - context.block_start_time) / 1000;
            message.append(d2s(seconds));
            message.append("s)\r\n");
            conn->send(message);
            message.clear();
            blocked_clients.erase(e);
            db->clear_blocking_keys_for_context(context);
        }
    }
}

// 随机删除一定数量的过期键
void engine::check_expire_keys()
{
    auto now = lru_clock;
    int dbnums = server_conf.mmdb_expire_check_dbnums;
    int keys = server_conf.mmdb_expire_check_keys;
    if (dbs.size() < dbnums) dbnums = dbs.size();
    for (int i = 0; i < dbnums; i++) {
        if (cur_check_db == dbs.size())
            cur_check_db = 0;
        DB *db = select_db(cur_check_db++);
        if (keys > db->get_expire_keys().size())
            keys = db->get_expire_keys().size();
        for (int j = 0; j < keys; j++) {
            if (db->get_expire_keys().empty()) break;
            auto randkey = get_rand_hash_key(db->get_expire_keys());
            size_t bucket = std::get<0>(randkey);
            size_t where = std::get<1>(randkey);
            for (auto it = db->get_expire_keys().cbegin(bucket);
                    it != db->get_expire_keys().cend(bucket); ++it) {
                if (where-- == 0) {
                    if (it->second <= now) {
                        db->del_key_with_expire(it->first);
                    }
                    break;
                }
            }
        }
    }
}

command_t *engine::find_command(const std::string& name)
{
    return db()->find_command(name);
}

void engine::clear()
{
    for (auto& db : dbs)
        db->clear();
}

DB::DB(mmdb::engine *e) : engine(e)
{
    cmdtable = {
        { "SELECT",     { -2, IS_WRITE, BIND(select) } },
        { "EXISTS",     { -2, IS_READ,  BIND(exists) } },
        { "TYPE",       { -2, IS_READ,  BIND(type) } },
        { "TTL",        { -2, IS_READ,  BIND(ttl) } },
        { "PTTL",       { -2, IS_READ,  BIND(pttl) } },
        { "EXPIRE",     { -3, IS_WRITE, BIND(expire) } },
        { "PEXPIRE",    { -3, IS_WRITE, BIND(pexpire) } },
        { "DEL",        {  2, IS_WRITE, BIND(del) } },
        { "KEYS",       { -2, IS_READ,  BIND(keys) } },
        { "SAVE",       { -1, IS_READ,  BIND(save) } },
        { "BGSAVE",     { -1, IS_READ,  BIND(bgsave) } },
        { "BGREWRITEAOF",{-1, IS_READ,  BIND(bgrewriteaof) } },
        { "LASTSAVE",   { -1, IS_READ,  BIND(lastsave) } },
        { "FLUSHDB",    { -1, IS_WRITE, BIND(flushdb) } },
        { "FLUSHALL",   { -1, IS_WRITE, BIND(flushall) } },
        { "DBSIZE",     { -1, IS_READ,  BIND(dbsize) } },
        { "RENAME",     { -3, IS_WRITE, BIND(rename) } },
        { "RENAMENX",   { -3, IS_WRITE, BIND(renamenx) } },
        { "MOVE",       { -3, IS_WRITE, BIND(move) } },
        { "LRU",        { -2, IS_READ,  BIND(lru) } },
        { "MULTI",      { -1, IS_READ,  BIND(multi) } },
        { "EXEC",       { -1, IS_READ,  BIND(exec) } },
        { "DISCARD",    { -1, IS_READ,  BIND(discard) } },
        { "WATCH",      {  2, IS_READ,  BIND(watch) } },
        { "UNWATCH",    { -1, IS_READ,  BIND(unwatch) } },
        { "SORT",       {  2, IS_READ,  BIND(sort) } },
        { "SET",        {  3, IS_WRITE, BIND(set) } },
        { "SETNX",      { -3, IS_WRITE, BIND(setnx) } },
        { "GET",        { -2, IS_READ,  BIND(get) } },
        { "GETSET",     { -3, IS_WRITE, BIND(getset) } },
        { "APPEND",     { -3, IS_WRITE, BIND(append) } },
        { "STRLEN",     { -2, IS_READ,  BIND(strlen) } },
        { "MSET",       {  3, IS_WRITE, BIND(mset) } },
        { "MGET",       {  2, IS_READ,  BIND(mget) } },
        { "INCR",       { -2, IS_WRITE, BIND(incr) } },
        { "INCRBY",     { -3, IS_WRITE, BIND(incrby) } },
        { "DECR",       { -2, IS_WRITE, BIND(decr) } },
        { "DECRBY",     { -3, IS_WRITE, BIND(decrby) } },
        { "SETRANGE",   { -4, IS_WRITE, BIND(setrange) } },
        { "GETRANGE",   { -4, IS_READ,  BIND(getrange) } },
        { "LPUSH",      {  3, IS_WRITE, BIND(lpush) } },
        { "LPUSHX",     { -3, IS_WRITE, BIND(lpushx) } },
        { "RPUSH",      {  3, IS_WRITE, BIND(rpush) } },
        { "RPUSHX",     { -3, IS_WRITE, BIND(rpushx) } },
        { "LPOP",       { -2, IS_WRITE, BIND(lpop) } },
        { "RPOP",       { -2, IS_WRITE, BIND(rpop) } },
        { "RPOPLPUSH",  { -3, IS_WRITE, BIND(rpoplpush) } },
        { "LREM",       { -4, IS_WRITE, BIND(lrem) } },
        { "LLEN",       { -2, IS_READ,  BIND(llen) } },
        { "LINDEX",     { -3, IS_READ,  BIND(lindex) } },
        { "LSET",       { -4, IS_WRITE, BIND(lset) } },
        { "LRANGE",     { -4, IS_READ,  BIND(lrange) } },
        { "LTRIM",      { -4, IS_WRITE, BIND(ltrim) } },
        { "BLPOP",      {  3, IS_READ,  BIND(blpop) } },
        { "BRPOP",      {  3, IS_READ,  BIND(brpop) } },
        { "BRPOPLPUSH", { -4, IS_READ,  BIND(brpoplpush) } },
        { "HSET",       { -4, IS_WRITE, BIND(hset) } },
        { "HSETNX",     { -4, IS_WRITE, BIND(hsetnx) } },
        { "HGET",       { -3, IS_READ,  BIND(hget) } },
        { "HEXISTS",    { -3, IS_READ,  BIND(hexists) } },
        { "HDEL",       {  3, IS_WRITE, BIND(hdel) } },
        { "HLEN",       { -2, IS_READ,  BIND(hlen) } },
        { "HSTRLEN",    { -3, IS_READ,  BIND(hstrlen) } },
        { "HINCRBY",    { -4, IS_WRITE, BIND(hincrby) } },
        { "HMSET",      {  4, IS_WRITE, BIND(hmset) } },
        { "HMGET",      {  3, IS_READ,  BIND(hmget) } },
        { "HKEYS",      { -2, IS_READ,  BIND(hkeys) } },
        { "HVALS",      { -2, IS_READ,  BIND(hvals) } },
        { "HGETALL",    { -2, IS_READ,  BIND(hgetall) } },
        { "SADD",       {  3, IS_WRITE, BIND(sadd) } },
        { "SISMEMBER",  { -3, IS_READ,  BIND(sismember) } },
        { "SPOP",       { -2, IS_WRITE, BIND(spop) } },
        { "SRANDMEMBER",{  2, IS_READ,  BIND(srandmember) } },
        { "SREM",       {  3, IS_WRITE, BIND(srem)  } },
        { "SMOVE",      { -4, IS_WRITE, BIND(smove) } },
        { "SCARD",      { -2, IS_READ,  BIND(scard) } },
        { "SMEMBERS",   { -2, IS_READ,  BIND(smembers) } },
        { "SINTER",     {  2, IS_READ,  BIND(sinter) } },
        { "SINTERSTORE",{  3, IS_WRITE, BIND(sinterstore) } },
        { "SUNION",     {  2, IS_READ,  BIND(sunion) } },
        { "SUNIONSTORE",{  3, IS_WRITE, BIND(sunionstore) } },
        { "ZADD",       {  4, IS_WRITE, BIND(zadd) } },
        { "ZSCORE",     { -3, IS_READ,  BIND(zscore) } },
        { "ZINCRBY",    { -4, IS_WRITE, BIND(zincrby) } },
        { "ZCARD",      { -2, IS_READ,  BIND(zcard) } },
        { "ZCOUNT",     { -4, IS_READ,  BIND(zcount) } },
        { "ZRANGE",     {  4, IS_READ,  BIND(zrange) } },
        { "ZREVRANGE",  {  4, IS_READ,  BIND(zrevrange) } },
        { "ZRANK",      { -3, IS_READ,  BIND(zrank) } },
        { "ZREVRANK",   { -3, IS_READ,  BIND(zrevrank) } },
        { "ZREM",       {  3, IS_WRITE, BIND(zrem) } },
        { "ZRANGEBYSCORE",      {  4, IS_READ,  BIND(zrangebyscore) } },
        { "ZREVRANGEBYSCORE",   {  4, IS_READ,  BIND(zrevrangebyscore) } },
        { "ZREMRANGEBYRANK",    { -4, IS_WRITE, BIND(zremrangebyrank) } },
        { "ZREMRANGEBYSCORE",   { -4, IS_WRITE, BIND(zremrangebyscore) } },
    };
}

void DB::clear()
{
    dict.clear();
    expire_keys.clear();
}

void DB::flushdb(context_t& con)
{
    clear();
    con.append(shared.ok);
}

void DB::flushall(context_t& con)
{
    engine->clear();
    auto pos = con.buf.size();
    if (!server_conf.mmdb_save_params.empty())
        bgsave(con);
    if (server_conf.mmdb_enable_appendonly)
        bgrewriteaof(con);
    // clear reply of bgsave and bg_rewrite_aof
    con.buf.erase(pos);
    con.append(shared.ok);
}

void DB::dbsize(context_t& con)
{
    con.append_reply_number(dict.size());
}

// SELECT index
void DB::select(context_t& con)
{
    int dbnum = str2l(con.argv[1]);
    if (str2numerr()) ret(con, shared.invalid_db_index);
    if (dbnum < 0 || dbnum >= server_conf.mmdb_databases) {
        ret(con, shared.db_index_out_of_range);
    }
    engine->switch_db(dbnum);
    con.append(shared.ok);
}

// EXISTS key
void DB::exists(context_t& con)
{
    check_expire(con.argv[1]);
    con.append(not_found(con.argv[1]) ? shared.n0 : shared.n1);
}

// TYPE key
void DB::type(context_t& con)
{
    check_expire(con.argv[1]);
    auto it = find(con.argv[1]);
    if (not_found(it)) ret(con, shared.none_type);
    if (is_type(it, String))
        con.append(shared.string_type);
    else if (is_type(it, List))
        con.append(shared.list_type);
    else if (is_type(it, Set))
        con.append(shared.set_type);
    else if (is_type(it, Zset))
        con.append(shared.zset_type);
    else if (is_type(it, Hash))
        con.append(shared.hash_type);
}

// (P)TTL key
void DB::_ttl(context_t& con, bool is_ttl)
{
    auto& key = con.argv[1];
    if (not_found(key)) ret(con, shared.n_2);
    auto it = expire_keys.find(key);
    if (it == expire_keys.end()) ret(con, shared.n_1);
    int64_t ttl_ms = it->second - angel::util::get_cur_time_ms();
    if (is_ttl) ttl_ms /= 1000;
    con.append_reply_number(ttl_ms);
}

void DB::ttl(context_t& con)
{
    _ttl(con, true);
}

void DB::pttl(context_t& con)
{
    _ttl(con, false);
}


// EXPIRE key seconds
// PEXPIRE key milliseconds
void DB::_expire(context_t& con, bool is_expire)
{
    auto& key = con.argv[1];
    auto expire = str2ll(con.argv[2]);
    if (str2numerr()) ret(con, shared.integer_err);
    if (expire <= 0) ret(con, shared.timeout_err);
    if (not_found(key)) ret(con, shared.n0);
    if (is_expire) expire *= 1000;
    expire += angel::util::get_cur_time_ms();
    add_expire_key(key, expire);
    con.append(shared.n1);
}

void DB::expire(context_t& con)
{
    _expire(con, true);
}

void DB::pexpire(context_t& con)
{
    _expire(con, false);
}

// DEL key [key ...]
void DB::del(context_t& con)
{
    int dels = 0;
    for (size_t i = 1; i < con.argv.size(); i++) {
        if (!not_found(con.argv[i])) {
            del_key_with_expire(con.argv[i]);
            dels++;
        }
    }
    con.append_reply_number(dels);
}

// KEYS pattern[目前只支持*]
void DB::keys(context_t& con)
{
    if (con.argv[1].compare("*"))
        ret(con, shared.unknown_option);
    con.append_reply_multi(dict.size());
    for (auto& it : dict)
        con.append_reply_string(it.first);
}

void DB::save(context_t& con)
{
    if (engine->rdb->doing()) return;
    engine->rdb->save();
    con.append(shared.ok);
}

void DB::bgsave(context_t& con)
{
    if (engine->aof->doing())
        ret(con, "+Background append only file rewriting ...\r\n");
    if (engine->rdb->doing()) return;
    engine->rdb->save_background();
}

void DB::bgrewriteaof(context_t& con)
{
    if (engine->rdb->doing()) {
        engine->flags |= engine::REWRITEAOF_DELAY;
        return;
    }
    if (engine->aof->doing()) return;
    engine->aof->rewrite_background();
    con.append("+Background append only file rewriting started\r\n");
}

void DB::lastsave(context_t& con)
{
    con.append_reply_number(engine->last_save_time);
}

// RENAME key newkey
void DB::rename(context_t& con)
{
    auto& key = con.argv[1];
    auto& newkey = con.argv[2];
    check_expire(key);
    auto it = find(key);
    if (not_found(it)) ret(con, shared.no_such_key);
    if (key == newkey) ret(con, shared.ok);
    del_key_with_expire(newkey);
    insert(newkey, it->second);
    del_key_with_expire(key);
    con.append(shared.ok);
}

// RENAMENX key newkey
void DB::renamenx(context_t& con)
{
    auto& key = con.argv[1];
    auto& newkey = con.argv[2];
    check_expire(key);
    auto it = find(key);
    if (not_found(it)) ret(con, shared.no_such_key);
    if (key == newkey) ret(con, shared.n0);
    if (!not_found(newkey)) ret(con, shared.n0);
    insert(newkey, it->second);
    del_key_with_expire(key);
    con.append(shared.n1);
}

void DB::lru(context_t& con)
{
    check_expire(con.argv[1]);
    auto it = find(con.argv[1]);
    if (not_found(it)) ret(con, shared.n_1);
    auto seconds = (lru_clock - it->second.lru) / 1000;
    con.append_reply_number(seconds);
}

void DB::watch(context_t& con)
{
    for (size_t i = 1; i < con.argv.size(); i++) {
        auto& key = con.argv[i];
        check_expire(key);
        if (not_found(key)) continue;
        auto it = watch_keys.find(key);
        if (it == watch_keys.end()) {
            std::vector<size_t> clist = { con.conn->id() };
            watch_keys.emplace(key, std::move(clist));
        } else {
            it->second.push_back(con.conn->id());
        }
        con.watch_keys.emplace_back(con.argv[i]);
    }
    con.append(shared.ok);
}

void DB::multi(context_t& con)
{
    con.flags |= context_t::EXEC_MULTI;
    con.append(shared.ok);
}

void DB::exec(context_t& con)
{
    argv_t cl = { "MULTI" };
    bool is_write = (con.flags & context_t::EXEC_MULTI_WRITE);
    if (con.flags & context_t::EXEC_MULTI_ERR) {
        con.flags &= ~context_t::EXEC_MULTI_ERR;
        con.append(shared.nil);
        goto end;
    }
    if (is_write) {
        // _dbServer->freeMemoryIfNeeded();
        __server->do_write_command(cl, nullptr, 0);
    }
    for (auto& argv : con.transaction_list) {
        auto c = find_command(argv[0]);
        con.argv.swap(argv);
        c->command_cb(con);
        if (is_write)
            __server->do_write_command(cl, nullptr, 0);
    }
    if (is_write) {
        cl = { "EXEC" };
        __server->do_write_command(cl, nullptr, 0);
        con.flags &= ~context_t::EXEC_MULTI_WRITE;
    }
    _unwatch(con);
end:
    con.transaction_list.clear();
    con.flags &= ~context_t::EXEC_MULTI;
}

void DB::discard(context_t& con)
{
    con.transaction_list.clear();
    _unwatch(con);
    con.flags &= ~context_t::EXEC_MULTI;
    con.flags &= ~context_t::EXEC_MULTI_ERR;
    con.flags &= ~context_t::EXEC_MULTI_WRITE;
    con.append(shared.ok);
}

void DB::unwatch(context_t& con)
{
    _unwatch(con);
    con.append(shared.ok);
}

void DB::_unwatch(context_t& con)
{
    for (auto& key : con.watch_keys) {
        auto cl = watch_keys.find(key);
        if (cl != watch_keys.end()) {
            for (auto c = cl->second.begin(); c != cl->second.end(); ++c) {
                if (*c == con.conn->id()) {
                    cl->second.erase(c);
                    break;
                }
            }
        }
    }
    con.watch_keys.clear();
}

void DB::touch_watch_key(const key_t& key)
{
    auto cl = watch_keys.find(key);
    if (cl == watch_keys.end()) return;
    for (auto& id : cl->second) {
        auto conn = __server->get_server().get_connection(id);
        if (!conn) continue;
        auto& ctx = std::any_cast<context_t&>(conn->get_context());
        ctx.flags |= context_t::EXEC_MULTI_ERR;
    }
}

// 清空con.blocking_keys，并从DB::blocking_keys中移除所有con
void DB::clear_blocking_keys_for_context(context_t& con)
{
    for (auto& it : con.blocking_keys) {
        auto cl = blocking_keys.find(it);
        if (cl != blocking_keys.end()) {
            for (auto c = cl->second.begin(); c != cl->second.end(); ++c) {
                if (*c == con.conn->id()) {
                    cl->second.erase(c);
                    break;
                }
            }
            if (cl->second.empty())
                blocking_keys.erase(it);
        }
    }
    con.flags &= ~context_t::CON_BLOCK;
    con.blocking_keys.clear();
}

// MOVE key db
void DB::move(context_t& con)
{
    auto& key = con.argv[1];
    int dbnum = str2l(con.argv[2]);
    if (str2numerr()) ret(con, shared.integer_err);
    if (dbnum < 0 || dbnum >= server_conf.mmdb_databases) {
        ret(con, shared.db_index_out_of_range);
    }
    auto it = find(key);
    if (not_found(it)) ret(con, shared.n0);
    auto db = engine->select_db(dbnum);
    it = db->find(key);
    if (!not_found(it)) ret(con, shared.n0);
    insert(it->first, it->second);
    del_key_with_expire(key);
    con.append(shared.n1);
}

void DB::check_expire(const key_t& key)
{
    auto it = expire_keys.find(key);
    if (it == expire_keys.end())
        return;
    if (it->second > lru_clock)
        return;
    del_key_with_expire(key);
    argv_t argv = { "DEL", key };
    __server->append_write_command(argv, nullptr, 0);
}
