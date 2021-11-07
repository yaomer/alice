#include "internal.h"

namespace alice {

namespace mmdb {

// SADD key member [member ...]
void DB::sadd(context_t& con)
{
    auto& key = con.argv[1];
    size_t size = con.argv.size();
    check_expire(key);
    size_t adds = 0;
    auto it = find(key);
    if (not_found(it)) {
        Set set;
        for (size_t i = 2; i < size; i++)
            set.emplace(con.argv[i]);
        insert(key, std::move(set));
        adds = size - 2;
    } else {
        check_type(con, it, Set);
        auto& set = get_set_value(it);
        for (size_t i = 2; i < size; i++) {
            auto it = set.emplace(con.argv[i]);
            if (it.second) adds++;
        }
    }
    con.append_reply_number(adds);
    touch_watch_key(key);
}

// SISMEMBER key member
void DB::sismember(context_t& con)
{
    auto& key = con.argv[1];
    auto& member = con.argv[2];
    check_expire(key);
    auto it = find(key);
    if (not_found(it)) ret(con, shared.n0);
    check_type(con, it, Set);
    auto& set = get_set_value(it);
    if (set.find(member) != set.end())
        con.append(shared.n1);
    else
        con.append(shared.n0);
}

// SPOP key
void DB::spop(context_t& con)
{
    auto& key = con.argv[1];
    check_expire(key);
    auto it = find(key);
    if (not_found(it)) ret(con, shared.nil);
    check_type(con, it, Set);
    auto& set = get_set_value(it);
    auto randkey = get_rand_hash_key(set);
    size_t bucket = std::get<0>(randkey);
    size_t where = std::get<1>(randkey);
    for (auto it = set.cbegin(bucket); it != set.cend(bucket); ++it) {
        if (where-- == 0) {
            con.append_reply_string(*it);
            set.erase(set.find(*it));
            break;
        }
    }
    del_key_if_empty(set, key);
    touch_watch_key(key);
}

// SRANDMEMBER key [count]
void DB::srandmember(context_t& con)
{
    auto& key = con.argv[1];
    check_expire(key);
    ssize_t count = 0;
    if (con.argv.size() > 2) {
        count = str2ll(con.argv[2]);
        if (str2numerr()) ret(con, shared.integer_err);
        if (count == 0) ret(con, shared.nil);
    }
    auto it = find(key);
    if (not_found(it)) ret(con, shared.nil);
    check_type(con, it, Set);
    auto& set = get_set_value(it);
    if (count >= static_cast<ssize_t>(set.size())) {
        con.append_reply_multi(set.size());
        for (auto& it : set) {
            con.append_reply_string(it);
        }
        return;
    }
    if (count == 0 || count < 0) {
        if (count == 0) count = -1;
        con.append_reply_multi(-count);
        while (count++ < 0) {
            auto randkey = get_rand_hash_key(set);
            size_t bucket = std::get<0>(randkey);
            size_t where = std::get<1>(randkey);
            for (auto it = set.cbegin(bucket); it != set.cend(bucket); ++it) {
                if (where-- == 0) {
                    con.append_reply_string(*it);
                    break;
                }
            }
        }
        return;
    }
    con.append_reply_multi(count);
    Set tset;
    while (count-- > 0) {
        auto randkey = get_rand_hash_key(set);
        size_t bucket = std::get<0>(randkey);
        size_t where = std::get<1>(randkey);
        for (auto it = set.cbegin(bucket); it != set.cend(bucket); ++it) {
            if (where-- == 0) {
                if (tset.find(*it) != tset.end()) {
                    count++;
                    break;
                }
                tset.insert(*it);
                break;
            }
        }
    }
    for (auto it : tset) {
        con.append_reply_string(it);
    }
}

// SREM key member [member ...]
void DB::srem(context_t& con)
{
    auto& key = con.argv[1];
    size_t size = con.argv.size();
    check_expire(key);
    auto it = find(key);
    if (not_found(it)) ret(con, shared.n0);
    check_type(con, it, Set);
    auto& set = get_set_value(it);
    size_t rems = 0;
    for (size_t i = 2; i < size; i++) {
        auto it = set.find(con.argv[i]);
        if (it != set.end()) {
            set.erase(it);
            rems++;
        }
    }
    del_key_if_empty(set, key);
    touch_watch_key(key);
    con.append_reply_number(rems);
}

// SMOVE source destination member
void DB::smove(context_t& con)
{
    auto& src = con.argv[1];
    auto& des = con.argv[2];
    auto& member = con.argv[3];
    check_expire(src);
    check_expire(des);
    auto it = find(src);
    if (not_found(it)) ret(con, shared.n0);
    check_type(con, it, Set);
    auto& src_set = get_set_value(it);
    auto src_it = src_set.find(member);
    if (src_it == src_set.end()) ret(con, shared.n0);
    src_set.erase(src_it);
    del_key_if_empty(src_set, src);
    auto des_it = find(des);
    if (not_found(des_it)) {
        Set set;
        set.emplace(member);
        insert(des, std::move(set));
    } else {
        check_type(con, des_it, Set);
        auto& des_set = get_set_value(des_it);
        des_set.emplace(member);
    }
    touch_watch_key(src);
    touch_watch_key(des);
    con.append(shared.n1);
}

// SCARD key
void DB::scard(context_t& con)
{
    auto& key = con.argv[1];
    check_expire(key);
    auto it = find(key);
    if (not_found(it)) ret(con, shared.n0);
    check_type(con, it, Set);
    auto& set = get_set_value(it);
    con.append_reply_number(set.size());
}

// SMEMBERS key
void DB::smembers(context_t& con)
{
    auto& key = con.argv[1];
    check_expire(key);
    auto it = find(key);
    if (not_found(it)) ret(con, shared.nil);
    check_type(con, it, Set);
    auto& set = get_set_value(it);
    con.append_reply_multi(set.size());
    for (auto& member : set) {
        con.append_reply_string(member);
    }
}

void DB::_sinter(context_t& con, Set& rset, int start)
{
    size_t size = con.argv.size();
    size_t min = 0, j = 0;
    // 挑选出元素最少的集合
    for (size_t i = start; i < size; i++) {
        check_expire(con.argv[i]);
        auto it = find(con.argv[i]);
        if (not_found(it)) ret(con, shared.nil);
        check_type(con, it, Set);
        auto& set = get_set_value(it);
        if (min == 0 || set.size() < min) {
            min = set.size();
            j = i;
        }
    }
    auto& set = get_set_value(find(con.argv[j]));
    for (auto& member : set) {
        size_t i;
        for (i = start; i < size; i++) {
            if (i == j) continue;
            auto& set = get_set_value(find(con.argv[i]));
            if (set.find(member) == set.end())
                break;
        }
        if (i == size)
            rset.emplace(member);
    }
}

void DB::_sunion(context_t& con, Set& rset, int start)
{
    size_t size = con.argv.size();
    for (size_t i = start; i < size; i++) {
        check_expire(con.argv[i]);
        auto it = find(con.argv[i]);
        if (not_found(it)) continue;
        check_type(con, it, Set);
        auto& set = get_set_value(it);
        for (auto& member : set) {
            rset.emplace(member);
        }
    }
}

void DB::sreply(context_t& con, Set& rset)
{
    if (rset.empty()) ret(con, shared.nil);
    con.append_reply_multi(rset.size());
    for (auto& member : rset)
        con.append_reply_string(member);
}

void DB::sstore(context_t& con, Set& rset)
{
    auto& key = con.argv[1];
    auto it = find(key);
    if (not_found(it)) {
        con.append_reply_number(rset.size());
        insert(key, std::move(rset));
    } else {
        check_type(con, it, Set);
        auto& set = get_set_value(it);
        set.swap(rset);
        con.append_reply_number(set.size());
    }
}

// SINTER key [key ...]
void DB::sinter(context_t& con)
{
    Set rset;
    auto pos = con.buf.size();
    _sinter(con, rset, 1);
    if (pos == con.buf.size())
        sreply(con, rset);
}

// SINTERSTORE destination key [key ...]
void DB::sinterstore(context_t& con)
{
    Set rset;
    auto pos = con.buf.size();
    _sinter(con, rset, 2);
    if (pos == con.buf.size())
        sstore(con, rset);
}

// SUNION key [key ...]
void DB::sunion(context_t& con)
{
    Set rset;
    auto pos = con.buf.size();
    _sunion(con, rset, 1);
    if (pos == con.buf.size())
        sreply(con, rset);
}

// SUNIONSTORE destination key [key ...]
void DB::sunionstore(context_t& con)
{
    Set rset;
    auto pos = con.buf.size();
    _sunion(con, rset, 2);
    if (pos == con.buf.size())
        sstore(con, rset);
}

void DB::sdiff(context_t& con)
{
    // TODO:
}

void DB::sdiffstore(context_t& con)
{
    // TODO:
}

}
}
