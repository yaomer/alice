#include "internal.h"

using namespace alice;
using namespace alice::mmdb;

// SET key value [EX seconds|PX milliseconds] [NX|XX]
void DB::set(context_t& con)
{
    unsigned cmdops = 0;
    int64_t expire;
    auto& key = con.argv[1];
    auto& value = con.argv[2];
    if (parse_set_args(con, cmdops, expire) == C_ERR)
        return;
    // [NX] [XX] 不能同时存在
    if ((cmdops & (SET_NX | SET_XX)))
        ret(con, shared.syntax_err);
    if (cmdops & SET_NX) {
        if (!not_found(key)) ret(con, shared.nil);
    } else if (cmdops & SET_XX) {
        if (not_found(key)) ret(con, shared.nil);
    }
    insert(key, value);
    con.append(shared.ok);
    del_expire_key(key);
    touch_watch_key(key);
    if (cmdops & (SET_EX | SET_PX)) {
        expire += angel::util::get_cur_time_ms();
        add_expire_key(key, expire);
    }
}

// SETNX key value
void DB::setnx(context_t& con)
{
    auto& key = con.argv[1];
    auto& value = con.argv[2];
    if (!not_found(key)) ret(con, shared.n0);
    insert(key, value);
    touch_watch_key(key);
    con.append(shared.n1);
}

// GET key
void DB::get(context_t& con)
{
    auto& key = con.argv[1];
    check_expire(key);
    auto it = find(key);
    if (not_found(it)) ret(con, shared.nil);
    check_type(con, it, String);
    auto& value = get_string_value(it);
    con.append_reply_string(value);
}

// GETSET key value
void DB::getset(context_t& con)
{
    auto& key = con.argv[1];
    auto& new_value = con.argv[2];
    check_expire(key);
    touch_watch_key(key);
    auto it = find(key);
    if (not_found(it)) {
        insert(key, new_value);
        ret(con, shared.nil);
    }
    check_type(con, it, String);
    auto& old_value = get_string_value(it);
    con.append_reply_string(old_value);
    insert(key, new_value);
}

// STRLEN key
void DB::strlen(context_t& con)
{
    auto& key = con.argv[1];
    check_expire(key);
    auto it = find(key);
    if (not_found(it)) ret(con, shared.n0);
    check_type(con, it, String);
    auto& value = get_string_value(it);
    con.append_reply_number(value.size());
}

// APPEND key value
void DB::append(context_t& con)
{
    auto& key = con.argv[1];
    auto& value = con.argv[2];
    check_expire(key);
    touch_watch_key(key);
    auto it = find(key);
    if (not_found(it)) {
        insert(key, value);
        con.append_reply_number(value.size());
        return;
    }
    check_type(con, it, String);
    auto& old_value = get_string_value(it);
    old_value.append(value);
    con.append_reply_number(old_value.size());
}

// MSET key value [key value ...]
void DB::mset(context_t& con)
{
    size_t size = con.argv.size();
    if (size % 2 == 0) ret(con, shared.argnumber_err);
    for (size_t i = 1; i < size; i += 2) {
        check_expire(con.argv[i]);
        insert(con.argv[i], con.argv[i+1]);
        touch_watch_key(con.argv[i]);
    }
    con.append(shared.ok);
}

// MGET key [key ...]
void DB::mget(context_t& con)
{
    size_t size = con.argv.size();
    con.append_reply_multi(size - 1);
    for (size_t i = 1; i < size; i++) {
        check_expire(con.argv[i]);
        auto it = find(con.argv[i]);
        if (not_found(it)) {
            con.append(shared.nil);
        } else {
            if (is_type(it, String)) {
                con.append_reply_string(get_string_value(it));
            } else {
                con.append(shared.nil);
            }
        }
    }
}

void DB::_incr(context_t& con, int64_t incr)
{
    auto& key = con.argv[1];
    check_expire(key);
    auto it = find(key);
    if (!not_found(it)) {
        check_type(con, it, String);
        auto& value = get_string_value(it);
        auto number = str2ll(value);
        if (str2numerr()) ret(con, shared.integer_err);
        incr += number;
    }
    insert(key, String(i2s(incr)));
    con.append_reply_number(incr);
    touch_watch_key(key);
}

// INCR key
void DB::incr(context_t& con)
{
    _incr(con, 1);
}

// INCRBY key increment
void DB::incrby(context_t& con)
{
    auto increment = str2ll(con.argv[2]);
    if (str2numerr()) ret(con, shared.integer_err);
    _incr(con, increment);
}

// DECR key
void DB::decr(context_t& con)
{
    _incr(con, -1);
}

// DECRBY key decrement
void DB::decrby(context_t& con)
{
    auto decrement = str2ll(con.argv[2]);
    if (str2numerr()) ret(con, shared.integer_err);
    _incr(con, -decrement);
}

// SETRANGE key offset value
void DB::setrange(context_t& con)
{
    auto& key = con.argv[1];
    auto offset = str2l(con.argv[2]);
    auto& value = con.argv[3];
    if (str2numerr() || offset < 0)
        ret(con, shared.integer_err);
    String new_value;
    check_expire(key);
    touch_watch_key(key);
    auto it = find(key);
    if (not_found(it)) {
        new_value.reserve(offset + value.size());
        new_value.resize(offset, '\x00');
        new_value.append(value);
        con.append_reply_number(new_value.size());
        insert(key, std::move(new_value));
        return;
    }
    check_type(con, it, String);
    new_value.swap(get_string_value(it));
    size_t len = offset + value.size();
    if (len > new_value.capacity()) new_value.reserve(len);
    if (len > new_value.size()) new_value.resize(len);
    if (offset > new_value.size())
        new_value.resize(offset, '\x00');
    std::copy(value.begin(), value.end(), new_value.begin()+offset);
    con.append_reply_number(new_value.size());
    insert(key, std::move(new_value));
}

// GETRANGE key start end
void DB::getrange(context_t& con)
{
    auto& key = con.argv[1];
    int start = str2l(con.argv[2]);
    if (str2numerr()) ret(con, shared.integer_err);
    int stop = str2l(con.argv[3]);
    if (str2numerr()) ret(con, shared.integer_err);
    check_expire(key);
    auto it = find(key);
    if (not_found(it)) ret(con, shared.nil);
    check_type(con, it, String);
    auto& value = get_string_value(it);
    int upper = value.size() - 1;
    int lower = -value.size();
    if (check_range(con, start, stop, lower, upper) == C_ERR)
        return;
    auto result = value.substr(start, stop - start + 1);
    con.append_reply_string(result);
}
