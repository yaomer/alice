#include "internal.h"

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
        zk.member = member;
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
    auto& min_str = con.argv[2];
    auto& max_str = con.argv[3];
    unsigned cmdops = 0;
    score_range r;
    if (parse_range_score(con, cmdops, r, min_str, max_str) == C_ERR)
        return;
    check_expire(key);
    auto it = find(key);
    if (not_found(it)) ret(con, shared.n0);
    check_type(con, it, Zset);
    auto& zset = get_zset_value(it);
    auto [first, last] = zset_range(zset, cmdops, r);
    if (first == last) con.append_reply_number(0);
    con.append_reply_number(zset.diff_range(first, last));
}

// Z(REV)RANGE key start stop [WITHSCORES]
void DB::_zrange(context_t& con, bool is_reverse)
{
    auto& key = con.argv[1];
    bool withscores = false;
    if (con.argv.size() > 4 ) {
        if (!con.isequal(4, "WITHSCORES")) ret(con, shared.syntax_err);
        withscores = true;
    }
    long long start = str2ll(con.argv[2]);
    if (str2numerr()) ret(con, shared.integer_err);
    long long stop = str2ll(con.argv[3]);
    if (str2numerr()) ret(con, shared.integer_err);
    check_expire(key);
    auto it = find(key);
    if (not_found(it)) ret(con, shared.nil);
    check_type(con, it, Zset);
    auto& zset = get_zset_value(it);
    long long upper = zset.size() - 1;
    long long lower = -zset.size();
    if (check_range_index(con, start, stop, lower, upper) == C_ERR)
        return;
    if (withscores)
        con.append_reply_multi((stop - start + 1) * 2);
    else
        con.append_reply_multi(stop - start + 1);
    long long i = 0;
    if (!is_reverse) {
        for (auto it = zset.zsl.cbegin(); it != zset.zsl.cend(); ++it, ++i) {
            if (i < start) continue;
            if (i > stop) break;
            con.append_reply_string(it->first.member);
            if (withscores)
                con.append_reply_double(it->first.score);
        }
    }
    if (is_reverse) {
        for (auto it = --zset.zsl.end(); ; --it, ++i) {
            if (i < start) continue;
            if (i > stop) break;
            con.append_reply_string(it->first.member);
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
void DB::_zrank(context_t& con, bool is_reverse)
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
    if (is_reverse) rank = zset.size() - rank;
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

static void zrangefor(context_t& con, unsigned cmdops, Zset::iterator it, Zset::iterator last,
                      long long offset, long long limit, long long dis, bool is_reverse)
{
    if (cmdops & LIMIT) {
        if (offset >= dis) ret(con, shared.multi_empty);
        limit = std::min(limit, dis - offset);
    } else {
        limit = dis;
    }
    while (it != last && offset > 0) {
        is_reverse ? --last : ++it;
        --offset;
    }
    if (it == last) ret(con, shared.nil);
    bool is_limit = (limit > 0);
    bool withscores = (cmdops & WITHSCORES);
    con.append_reply_multi(withscores ? limit * 2 : limit);
    if (!is_reverse) {
        while (it != last) {
            con.append_reply_string(it->first.member);
            if (withscores)
                con.append_reply_double(it->first.score);
            ++it;
            if (is_limit && --limit == 0)
                break;
        }
    } else {
        for (--last; ; --last) {
            con.append_reply_string(last->first.member);
            if (withscores)
                con.append_reply_double(last->first.score);
            if (last == it || (is_limit && --limit == 0))
                break;
        }
    }
}

// Z(REV)RANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
void DB::_zrangebyscore(context_t& con, bool is_reverse)
{
    unsigned cmdops = 0;
    long long offset = 0, limit = 0;
    if (parse_zrangebyscore_args(con, cmdops, offset, limit) == C_ERR)
        return;
    auto& key = con.argv[1];
    auto& min_str = con.argv[2];
    auto& max_str = con.argv[3];
    score_range r;
    if (parse_range_score(con, cmdops, r, min_str, max_str) == C_ERR)
        return;
    check_expire(key);
    auto it = find(key);
    if (not_found(it)) ret(con, shared.nil);
    check_type(con, it, Zset);
    auto& zset = get_zset_value(it);
    auto [first, last] = zset_range(zset, cmdops, r);
    if (first == last) ret(con, shared.nil);
    long long dis = zset.diff_range(first, last);
    zrangefor(con, cmdops, first, last, offset, limit, dis, is_reverse);
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
    long long start = str2l(con.argv[2]);
    if (str2numerr()) ret(con, shared.integer_err);
    long long stop = str2l(con.argv[3]);
    if (str2numerr()) ret(con, shared.integer_err);
    check_expire(key);
    auto it = find(key);
    if (not_found(it)) ret(con, shared.n0);
    check_type(con, it, Zset);
    auto& zset = get_zset_value(it);
    long long upper = zset.size() - 1;
    long long lower = -zset.size();
    if (check_range_index(con, start, stop, lower, upper) == C_ERR)
        return;
    long long i = 0, rems = 0;
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
    auto& key = con.argv[1];
    auto& min_str = con.argv[2];
    auto& max_str = con.argv[3];
    score_range r;
    if (parse_range_score(con, cmdops, r, min_str, max_str) == C_ERR)
        return;
    check_expire(key);
    auto it = find(key);
    if (not_found(it)) ret(con, shared.n0);
    check_type(con, it, Zset);
    auto& zset = get_zset_value(it);
    auto [first, last] = zset_range(zset, cmdops, r);
    if (first == last) ret(con, shared.n0);
    int rems = 0;
    while (first != last) {
        auto e = first++;
        zset.erase(e->first.score, e->first.member);
        rems++;
    }
    del_key_if_empty(zset, key);
    con.append_reply_number(rems);
}

zsk_range DB::zset_range(Zset& zset, unsigned cmdops, score_range& r)
{
    double min_score = zset.min_score();
    double max_score = zset.max_score();
    auto it = zset.zsl.begin();
    auto last = zset.zsl.end();
    if ((!r.lower && r.min > max_score) || (!r.upper && r.max < min_score)) {
        return { last, last };
    }
    if (!r.lower && r.min > min_score) it = zset.lower_bound(r.min);
    if (!r.upper && r.max < max_score) last = zset.upper_bound(r.max);
    double score = (--last)->first.score;
    for (++last; last != zset.zsl.end() && last->first.score == score; ++last)
        ;
    if (!r.lower && (cmdops & LOI)) {
        while (it != last && it->first.score == r.min)
            ++it;
    }
    if (it == last) return { it, last };
    if (!r.upper && (cmdops & ROI)) {
        for (--last; it != last && last->first.score == r.max; --last)
            ;
        if (last->first.score != r.max)
            ++last;
    }
    return { it, last };
}
