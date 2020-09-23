#include "mmdb.h"

#include "../server.h"

using namespace alice;
using namespace alice::mmdb;

// ZADD key score member [score member ...]
void DB::zadd(context_t& con)
{
    auto& key = con.argv[1];
    size_t size = con.argv.size();
    if (size % 2 != 0) ret(con, shared.argnumber_err);
    // 先检查参数中的所有score
    for (size_t i = 2; i < size; i += 2) {
        void(str2f(con.argv[i]));
        if (str2numerr()) ret(con, shared.float_err);
    }
    check_expire(key);
    touch_watch_key(key);
    auto it = find(key);
    if (not_found(it)) {
        Zset zset;
        for (size_t i = 2; i < size; i += 2) {
            double score = atof(con.argv[i].c_str());
            zset.insert(score, con.argv[i+1]);
        }
        insert(key, std::move(zset));
        con.append_reply_number((size - 2) / 2);
        return;
    }
    check_type(con, it, Zset);
    auto& zset = get_zset_value(it);
    int adds = 0;
    for (size_t i = 2; i < size; i += 2) {
        double score = atof(con.argv[i].c_str());
        auto it = zset.zmap.find(con.argv[i+1]);
        if (it != zset.zmap.end()) {
            // 如果成员已存在，则会更新它的分数
            zset.erase(it->second, con.argv[i+1]);
        } else {
            adds++;
        }
        zset.insert(score, con.argv[i+1]);
    }
    con.append_reply_number(adds);
}

// ZSCORE key member
void DB::zscore(context_t& con)
{
    auto& key = con.argv[1];
    auto& member = con.argv[2];
    check_expire(key);
    auto it = find(key);
    if (not_found(it)) ret(con, shared.nil);
    check_type(con, it, Zset);
    auto& zset = get_zset_value(it);
    auto e = zset.zmap.find(member);
    if (e != zset.zmap.end()) {
        con.append_reply_double(e->second);
    } else
        con.append(shared.nil);
}

// ZINCRBY key increment member
void DB::zincrby(context_t& con)
{
    auto& key = con.argv[1];
    auto& member = con.argv[3];
    double score = str2f(con.argv[2]);
    if (str2numerr()) ret(con, shared.float_err);
    check_expire(key);
    touch_watch_key(key);
    auto it = find(key);
    if (not_found(it)) {
        Zset zset;
        zset.insert(score, member);
        insert(key, std::move(zset));
        con.append_reply_double(score);
        return;
    }
    check_type(con, it, Zset);
    auto& zset = get_zset_value(it);
    auto e = zset.zmap.find(member);
    if (e != zset.zmap.end()) {
        zslkey zk;
        zk.score = e->second;
        zk.key = member;
        zset.zsl.erase(zk);
        score += e->second;
    }
    zset.insert(score, member);
    con.append_reply_double(score);
}

// ZCARD key
void DB::zcard(context_t& con)
{
    auto& key = con.argv[1];
    check_expire(key);
    auto it = find(key);
    if (not_found(it)) ret(con, shared.n0);
    check_type(con, it, Zset);
    auto& zset = get_zset_value(it);
    con.append_reply_number(zset.size());
}

// ZCOUNT key min max
void DB::zcount(context_t& con)
{
    auto& key = con.argv[1];
    double min = str2f(con.argv[2]);
    if (str2numerr()) ret(con, shared.float_err);
    double max = str2f(con.argv[3]);
    if (str2numerr()) ret(con, shared.float_err);
    if (min > max) ret(con, shared.n0);
    check_expire(key);
    auto it = find(key);
    if (not_found(it)) ret(con, shared.n0);
    check_type(con, it, Zset);
    auto& zset = get_zset_value(it);
    size_t lower = zset.order_of_key(min, "");
    if (lower == 0) ret(con, shared.n0);
    size_t upper = zset.order_of_key(max, "");
    if (upper == 0) ret(con, shared.n0);
    con.append_reply_number(upper-lower);
}

// Z(REV)RANGE key start stop [WITHSCORES]
void DB::_zrange(context_t& con, bool reverse)
{
    auto& key = con.argv[1];
    bool withscores = false;
    if (con.argv.size() > 4 ) {
        if (!con.isequal(4, "WITHSCORES")) ret(con, shared.syntax_err);
        withscores = true;
    }
    int start = str2l(con.argv[2]);
    if (str2numerr()) ret(con, shared.integer_err);
    int stop = str2l(con.argv[3]);
    if (str2numerr()) ret(con, shared.integer_err);
    check_expire(key);
    auto it = find(key);
    if (not_found(it)) ret(con, shared.nil);
    check_type(con, it, Zset);
    auto& zset = get_zset_value(it);
    int upper = zset.size() - 1;
    int lower = -zset.size();
    if (dbserver::check_range(con, start, stop, lower, upper) == C_ERR)
        return;
    if (withscores)
        con.append_reply_multi((stop - start + 1) * 2);
    else
        con.append_reply_multi(stop - start + 1);
    int i = 0;
    if (!reverse) {
        for (auto it = zset.zmap.cbegin(); it != zset.zmap.cend(); ++it, ++i) {
            if (i < start) continue;
            if (i > stop) break;
            con.append_reply_string(it->first);
            if (withscores)
                con.append_reply_double(it->second);
        }
    }
    if (reverse) {
        // auto& zsl = zset.zsl;
        // for (auto& it : zset.zsl)
            // std::cout << it.first.score << "," << it.first.key << "\n";
        // ret(con, shared.n0);
        for (auto it = --zset.zsl.end(); ; --it, ++i) {
            if (i < start) continue;
            if (i > stop) break;
            con.append_reply_string(it->first.key);
            if (withscores)
                con.append_reply_double(it->first.score);
            if (it == zset.zsl.begin())
                break;
        }
    }
}

void DB::zrange(context_t& con)
{
    _zrange(con, false);
}

void DB::zrevrange(context_t& con)
{
    _zrange(con, true);
}

// Z(REV)RANK key member
void DB::_zrank(context_t& con, bool reverse)
{
    auto& key = con.argv[1];
    auto& member = con.argv[2];
    check_expire(key);
    auto it = find(key);
    if (not_found(it)) ret(con, shared.nil);
    check_type(con, it, Zset);
    auto& zset = get_zset_value(it);
    auto e = zset.zmap.find(member);
    if (e == zset.zmap.end()) ret(con, shared.nil);
    size_t rank = zset.order_of_key(e->second, member);
    if (reverse) rank = zset.size() - rank;
    else rank -= 1; // base on 0
    con.append_reply_number(rank);
}

void DB::zrank(context_t& con)
{
    _zrank(con, false);
}

void DB::zrevrank(context_t& con)
{
    _zrank(con, true);
}

#define MIN_INF 1 // -inf
#define POS_INF 2 // +inf
#define WITHSCORES  0x01
#define LIMIT       0x02
#define LOI         0x04 // (min
#define ROI         0x08 // (max

namespace alice {

    thread_local std::unordered_map<std::string, int> zrbsops = {
        { "WITHSCORES", WITHSCORES },
        { "LIMIT",      LIMIT },
    };
}

static void check_limit(unsigned& cmdops, int& lower, int& upper,
                        const std::string& min, const std::string& max)
{
    if (min[0] == '(') cmdops |= LOI;
    if (max[0] == '(') cmdops |= ROI;
    if (cmdops & LOI) {
        if (strcasecmp(min.c_str(), "(-inf") == 0)
            lower = MIN_INF;
        else if (strcasecmp(min.c_str(), "(+inf") == 0)
            lower = POS_INF;
    } else {
        if (strcasecmp(min.c_str(), "-inf") == 0)
            lower = MIN_INF;
        else if (strcasecmp(min.c_str(), "+inf") == 0)
            lower = POS_INF;
    }
    if (cmdops & ROI) {
        if (strcasecmp(max.c_str(), "(-inf") == 0)
            upper = MIN_INF;
        else if (strcasecmp(max.c_str(), "(+inf") == 0)
            upper = POS_INF;
    } else {
        if (strcasecmp(max.c_str(), "-inf") == 0)
            upper = MIN_INF;
        else if (strcasecmp(max.c_str(), "+inf") == 0)
            upper = POS_INF;
    }
}

static void zrangefor(context_t& con, Zset::iterator first, Zset::iterator last,
                      int count, bool withscores, bool reverse)
{
    bool is_count = (count > 0);
    if (!reverse) {
        while (first != last) {
            con.append_reply_string(first->first.key);
            if (withscores)
                con.append_reply_double(first->first.score);
            ++first;
            if (is_count && --count == 0)
                break;
        }
    } else {
        for (--last; ; --last) {
            con.append_reply_string(last->first.key);
            if (withscores)
                con.append_reply_double(last->first.score);
            if (last == first || (is_count && --count == 0))
                break;
        }
    }
}

static int zrangebyscore_with_limit(context_t& con, Zset::iterator first,
                                    Zset::iterator last, int distance, int offset,
                                    int count, bool withscores, bool reverse)
{
    if (offset < 0 || count <= 0) {
        con.append(shared.nil);
        return C_ERR;
    }
    while (first != last && offset > 0) {
        reverse ? --last : ++first;
        --offset;
    }
    if (first == last) {
        con.append(shared.nil);
        return C_ERR;
    }
    if (count > distance) count = distance;
    if (withscores)
        con.append_reply_multi(count * 2);
    else
        con.append_reply_multi(count);
    zrangefor(con, first, last, count, withscores, reverse);
    return C_OK;
}

static int parse_zrangebyscore_args(context_t& con, unsigned& cmdops,
                                    int& offset, int& count)
{
    size_t len = con.argv.size();
    for (size_t i = 4; i < len; i++) {
        std::transform(con.argv[i].begin(), con.argv[i].end(), con.argv[i].begin(), ::toupper);
        auto op = zrbsops.find(con.argv[i]);
        if (op == zrbsops.end()) goto syntax_err;
        cmdops |= op->second;
        switch (op->second) {
        case WITHSCORES: break;
        case LIMIT: {
            if (i + 2 >= len) goto syntax_err;
            offset = str2l(con.argv[++i]);
            if (str2numerr()) goto integer_err;
            count = str2l(con.argv[++i]);
            if (str2numerr()) goto integer_err;
            break;
        }
        }
    }
    return C_OK;
syntax_err:
    con.append(shared.syntax_err);
    return C_ERR;
integer_err:
    con.append(shared.integer_err);
    return C_ERR;
}

// Z(REV)RANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
void DB::_zrangebyscore(context_t& con, bool reverse)
{
    unsigned cmdops = 0;
    int offset = 0, count = 0;
    if (parse_zrangebyscore_args(con, cmdops, offset, count) == C_ERR)
        return;
    auto& key = con.argv[1];
    auto& min_str = con.argv[2];
    auto& max_str = con.argv[3];
    int lower = 0, upper = 0;
    double min = 0, max = 0;
    check_limit(cmdops, lower, upper, min_str, max_str);
    if (!lower) {
        double fval = str2f((cmdops & LOI) ? min_str.c_str() + 1 : min_str.c_str());
        if (!reverse) min = fval;
        else max = fval;
        if (str2numerr()) ret(con, shared.float_err);
    }
    if (!upper) {
        double fval = str2f((cmdops & ROI) ? max_str.c_str() + 1 : max_str.c_str());
        if (!reverse) max = fval;
        else min = fval;
        if (str2numerr()) ret(con, shared.float_err);
    }
    // [+-]inf[other chars]中前缀可以被合法转换，但整个字符串是无效的
    if (isinf(min) || isinf(max)) ret(con, shared.syntax_err);
    if (!lower && !upper && min > max) ret(con, shared.nil);
    if ((reverse && (lower == MIN_INF || upper == POS_INF))
    || (!reverse && (lower == POS_INF || upper == MIN_INF))) {
        ret(con, shared.nil);
    }
    check_expire(key);
    auto it = find(key);
    if (not_found(it)) ret(con, shared.nil);
    check_type(con, it, Zset);
    auto& zset = get_zset_value(it);
    Zset::iterator first, last;
    if (lower) first = zset.zsl.begin();
    else first = zset.lower_bound(min, "");
    if (upper) last = zset.zsl.end();
    else last = zset.upper_bound(max, "");
    if (first == zset.zsl.end()) ret(con, shared.nil);
    if (!lower && (cmdops & LOI)) { // 左开区间
        if (!reverse) {
            while (first != last && first->first.score == min)
                ++first;
        } else {
            --last;
            while (first != last && last->first.score == max)
                --last;
            ++last;
        }
    }
    if (!upper && (cmdops & ROI)) { // 右开区间
        if (!reverse) {
            --last;
            while (first != last && last->first.score == max)
                --last;
            if (first == last && last->first.score == max) {
                ret(con, shared.nil);
            }
            ++last;
        } else {
            while (first != last && first->first.score == min)
                ++first;
        }
    }
    if (first == last) ret(con, shared.nil);
    int distance = 0;
    if (lower && upper)
        distance = zset.size();
    else {
        int lrank = zset.order_of_key(min, "");
        int rrank = zset.order_of_key(max, "");
        distance = rrank - lrank;
    }
    if ((cmdops & WITHSCORES) && (cmdops & LIMIT)) {
        zrangebyscore_with_limit(con, first, last, distance, offset, count, true, reverse);
    } else if (cmdops & WITHSCORES) {
        con.append_reply_multi(distance * 2);
        zrangefor(con, first, last, 0, true, reverse);
    } else if (cmdops & LIMIT) {
        zrangebyscore_with_limit(con, first, last, distance, offset, count, false, reverse);
    } else {
        con.append_reply_multi(distance);
        zrangefor(con, first, last, 0, false, reverse);
    }
}

void DB::zrangebyscore(context_t& con)
{
    _zrangebyscore(con, false);
}

void DB::zrevrangebyscore(context_t& con)
{
    _zrangebyscore(con, true);
}

// ZREM key member [member ...]
void DB::zrem(context_t& con)
{
    auto& key = con.argv[1];
    check_expire(key);
    auto it = find(key);
    if (not_found(it)) ret(con, shared.n0);
    check_type(con, it, Zset);
    auto& zset = get_zset_value(it);
    int rems = 0;
    size_t size = con.argv.size();
    for (size_t i = 2; i < size; i++) {
        auto e = zset.zmap.find(con.argv[i]);
        if (e != zset.zmap.end()) {
            zset.erase(e->second, e->first);
            rems++;
        }
    }
    del_key_if_empty(zset, key);
    touch_watch_key(key);
    con.append_reply_number(rems);
}

// ZREMRANGEBYRANK key start stop
void DB::zremrangebyrank(context_t& con)
{
    auto& key = con.argv[1];
    int start = str2l(con.argv[2]);
    if (str2numerr()) ret(con, shared.integer_err);
    int stop = str2l(con.argv[3]);
    if (str2numerr()) ret(con, shared.integer_err);
    check_expire(key);
    auto it = find(key);
    if (not_found(it)) ret(con, shared.n0);
    check_type(con, it, Zset);
    auto& zset = get_zset_value(it);
    int upper = zset.size() - 1;
    int lower = -zset.size();
    if (dbserver::check_range(con, start, stop, lower, upper) == C_ERR)
        return;
    int i = 0, rems = 0;
    for (auto it = zset.zmap.begin(); it != zset.zmap.end(); i++) {
        if (i < start) continue;
        if (i > stop) break;
        auto e = it++;
        zset.erase(e->second, e->first);
        rems++;
    }
    del_key_if_empty(zset, key);
    touch_watch_key(key);
    con.append_reply_number(rems);
}

// ZREMRANGEBYSCORE key min max
void DB::zremrangebyscore(context_t& con)
{
    unsigned cmdops = 0;
    int lower = 0, upper = 0;
    auto& key = con.argv[1];
    auto& min_str = con.argv[2];
    auto& max_str = con.argv[3];
    check_limit(cmdops, lower, upper, min_str, max_str);
    double min = 0, max = 0;
    if (!lower) {
        min = str2f((cmdops & LOI) ? min_str.c_str() + 1 : min_str.c_str());
        if (str2numerr()) ret(con, shared.float_err);
    }
    if (!upper) {
        max = str2f((cmdops & ROI) ? max_str.c_str() + 1 : max_str.c_str());
        if (str2numerr()) ret(con, shared.float_err);
    }
    if (isinf(min) || isinf(max)) ret(con, shared.syntax_err);
    if (!lower && !upper && min > max) ret(con, shared.n0);
    if (lower == POS_INF || upper == MIN_INF) ret(con, shared.n0);
    check_expire(key);
    auto it = find(key);
    if (not_found(it)) ret(con, shared.n0);
    check_type(con, it, Zset);
    auto& zset = get_zset_value(it);
    Zset::iterator first, last;
    if (lower) first = zset.zsl.begin();
    else first = zset.lower_bound(min, "");
    if (upper) last = zset.zsl.end();
    else last = zset.upper_bound(max, "");
    if (first == zset.zsl.end()) ret(con, shared.n0);
    if (cmdops & LOI) {
        while (first != last && first->first.score == min)
            ++first;
    }
    if (cmdops & ROI) {
        --last;
        while (first != last && last->first.score == max)
            --last;
        ++last;
    }
    if (first == last) ret(con, shared.n0);
    int rems = 0;
    while (first != last) {
        auto e = first++;
        zset.erase(e->first.score, e->first.key);
        rems++;
    }
    del_key_if_empty(zset, key);
    con.append_reply_number(rems);
}
