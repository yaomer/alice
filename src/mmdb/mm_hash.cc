#include "internal.h"

using namespace alice;
using namespace alice::mmdb;

// HSET key field value
void DB::hset(context_t& con)
{
    auto& key = con.argv[1];
    auto& field = con.argv[2];
    auto& value = con.argv[3];
    check_expire(key);
    touch_watch_key(key);
    auto it = find(key);
    if (not_found(it)) {
        Hash hash;
        hash.emplace(field, value);
        insert(key, std::move(hash));
        ret(con, shared.n1);
    }
    check_type(con, it, Hash);
    auto& hash = get_hash_value(it);
    if (hash.find(field) != hash.end()) {
        con.append(shared.n0);
    } else {
        con.append(shared.n1);
    }
    hash.emplace(field, value);
}

// HSETNX key field value
void DB::hsetnx(context_t& con)
{
    auto& key = con.argv[1];
    auto& field = con.argv[2];
    auto& value = con.argv[3];
    check_expire(key);
    auto it = find(key);
    if (not_found(it)) {
        Hash hash;
        hash.emplace(field, value);
        insert(key, std::move(hash));
        touch_watch_key(key);
        ret(con, shared.n1);
    }
    check_type(con, it, Hash);
    auto& hash = get_hash_value(it);
    if (hash.find(field) != hash.end()) {
        con.append(shared.n0);
    } else {
        hash.emplace(field, value);
        touch_watch_key(key);
        con.append(shared.n1);
    }
}

// HGET key field
void DB::hget(context_t& con)
{
    auto& key = con.argv[1];
    auto& field = con.argv[2];
    check_expire(key);
    auto it = find(key);
    if (not_found(it)) ret(con, shared.nil);
    check_type(con, it, Hash);
    auto& hash = get_hash_value(it);
    auto value = hash.find(field);
    if (value != hash.end()) {
        con.append_reply_string(value->second);
    } else
        con.append(shared.nil);
}

// HEXISTS key field
void DB::hexists(context_t& con)
{
    auto& key = con.argv[1];
    auto& field = con.argv[2];
    check_expire(key);
    auto it = find(key);
    if (not_found(it)) ret(con, shared.n0);
    check_type(con, it, Hash);
    auto& hash = get_hash_value(it);
    if (hash.find(field) != hash.end()) {
        con.append(shared.n1);
    } else {
        con.append(shared.n0);
    }
}

// HDEL key field [field ...]
void DB::hdel(context_t& con)
{
    auto& key = con.argv[1];
    check_expire(key);
    auto it = find(key);
    if (not_found(it)) ret(con, shared.n0);
    size_t size = con.argv.size();
    check_type(con, it, Hash);
    int dels = 0;
    auto& hash = get_hash_value(it);
    for (size_t i = 2; i < size; i++) {
        auto it = hash.find(con.argv[i]);
        if (it != hash.end()) {
            hash.erase(it);
            dels++;
        }
    }
    del_key_if_empty(hash, key);
    touch_watch_key(key);
    con.append_reply_number(dels);
}

// HLEN key
void DB::hlen(context_t& con)
{
    auto& key = con.argv[1];
    check_expire(key);
    auto it = find(key);
    if (not_found(it)) ret(con, shared.n0);
    check_type(con, it, Hash);
    auto& hash = get_hash_value(it);
    con.append_reply_number(hash.size());
}

// HSTRLEN key field
void DB::hstrlen(context_t& con)
{
    auto& key = con.argv[1];
    auto& field = con.argv[2];
    check_expire(key);
    auto it = find(key);
    if (not_found(it)) ret(con, shared.n0);
    check_type(con, it, Hash);
    auto& hash = get_hash_value(it);
    auto value = hash.find(field);
    if (value != hash.end()) {
        con.append_reply_number(value->second.size());
    } else {
        con.append(shared.n0);
    }
}

// HINCRBY key field increment
void DB::hincrby(context_t& con)
{
    auto& key = con.argv[1];
    auto& field = con.argv[2];
    auto& incr_str = con.argv[3];
    check_expire(key);
    auto incr = str2ll(incr_str);
    if (str2numerr()) ret(con, shared.integer_err);
    auto it = find(key);
    if (not_found(it)) {
        Hash hash;
        hash.emplace(field, incr_str);
        insert(key, std::move(hash));
        touch_watch_key(key);
        ret(con, shared.n0);
    }
    check_type(con, it, Hash);
    auto& hash = get_hash_value(it);
    auto value = hash.find(field);
    if (value != hash.end()) {
        int64_t number = str2ll(value->second);
        if (str2numerr()) ret(con, shared.integer_err);
        number += incr;
        value->second.assign(i2s(number));
        con.append_reply_number(number);
    } else {
        hash.emplace(field, String(i2s(incr)));
        con.append_reply_number(incr);
    }
    touch_watch_key(key);
}

// HMSET key field value [field value ...]
void DB::hmset(context_t& con)
{
    auto& key = con.argv[1];
    check_expire(key);
    size_t size = con.argv.size();
    if (size % 2 != 0) ret(con, shared.argnumber_err);
    touch_watch_key(key);
    auto it = find(key);
    if (not_found(it)) {
        Hash hash;
        for (size_t i = 2; i < size; i += 2)
            hash.emplace(con.argv[i], con.argv[i+1]);
        insert(key, std::move(hash));
        ret(con, shared.ok);
    }
    check_type(con, it, Hash);
    auto& hash = get_hash_value(it);
    for (size_t i = 2; i < size; i += 2)
        hash.emplace(con.argv[i], con.argv[i+1]);
    con.append(shared.ok);
}

// HMGET key field [field ...]
void DB::hmget(context_t& con)
{
    auto& key = con.argv[1];
    check_expire(key);
    size_t size = con.argv.size();
    auto it = find(key);
    con.append_reply_multi(size - 2);
    if (not_found(it)) {
        for (size_t i = 2; i < size; i++)
            con.append(shared.nil);
        return;
    }
    check_type(con, it, Hash);
    auto& hash = get_hash_value(it);
    for (size_t i = 2; i < size; i++) {
        auto it = hash.find(con.argv[i]);
        if (it != hash.end()) {
            con.append_reply_string(it->second);
        } else {
            con.append(shared.nil);
        }
    }
}

#define HGETKEYS     0
#define HGETVALUES   1
#define HGETALL      2

// HKEYS/HVALS/HGETALL key
void DB::_hget(context_t& con, int what)
{
    auto& key = con.argv[1];
    check_expire(key);
    auto it = find(key);
    if (not_found(it)) ret(con, shared.nil);
    check_type(con, it, Hash);
    auto& hash = get_hash_value(it);
    con.append_reply_multi(what == HGETALL ? hash.size() * 2 : hash.size());
    for (auto& it : hash) {
        if (what == HGETKEYS) {
            con.append_reply_string(it.first);
        } else if (what == HGETVALUES) {
            con.append_reply_string(it.second);
        } else {
            con.append_reply_string(it.first);
            con.append_reply_string(it.second);
        }
    }
}

void DB::hkeys(context_t& con)
{
    _hget(con, HGETKEYS);
}

void DB::hvals(context_t& con)
{
    _hget(con, HGETVALUES);
}

void DB::hgetall(context_t& con)
{
    _hget(con, HGETALL);
}
