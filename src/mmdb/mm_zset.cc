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
    auto& min_str = con.argv[2];
    auto& max_str = con.argv[3];
    unsigned cmdops = 0;
    int lower = 0, upper = 0;
    double min = 0, max = 0;
    auto r = parse_interval(con, cmdops, lower, upper, min, max, min_str, max_str, false);
    if (r == C_ERR) return;
    check_expire(key);
    auto it = find(key);
    if (not_found(it)) ret(con, shared.n0);
    check_type(con, it, Zset);
    auto& zset = get_zset_value(it);
    auto count = zcount_range(zset, cmdops, lower, upper, min, max);
    con.append_reply_number(count);
}

size_t DB::zcount_range(Zset& zset, unsigned cmdops, int lower, int upper, double min, double max)
{
    if (zset.empty()) return 0;
    double min_score = zset.min_score();
    double max_score = zset.max_score();
    if (lower) min = std::min(min_score, max) - 1;
    if (upper) max = std::max(max_score, min) + 1;
    if (min > max_score || max < min_score) return 0;
    if (min < min_score) {
        if (max > max_score) return zset.size();
        auto order = zset.order_of_key(max, "");
        if (order > 0) {
            return (cmdops & ROI) ? order - 1 : order;
        }
        auto it = zset.lower_bound(max, "");
        order = zset.order_of_key(it->first.score, "");
        return order - 1;
    }
    // min is [min_score, max_score]
    auto lr = zset.order_of_key(min, "");
    auto rr = zset.order_of_key(max, "");
    if (max > max_score) {
        if (lr > 0) {
            auto order = zset.size() - lr;
            return (cmdops & LOI) ? order : order + 1;
        }
        auto it = zset.lower_bound(min, "");
        auto order = zset.order_of_key(it->first.score, "");
        order = zset.size() - order + 1;
        return order;
    }
    // min, max are both [min_score, max_score]
    if (lr > 0 && rr > 0) {
        auto count = rr - lr + 1;
        if (cmdops & LOI) count--;
        if (cmdops & ROI) count--;
        return count;
    } else if (lr > 0) {
        auto it = zset.lower_bound(max, "");
        rr = zset.order_of_key(it->first.score, "");
        auto count = rr - lr;
        if (cmdops & LOI) count--;
        return count;
    } else if (rr > 0) {
        auto it = zset.lower_bound(min, "");
        lr = zset.order_of_key(it->first.score, "");
        auto count = rr - lr + 1;
        if (cmdops & ROI) count--;
        return count;
    } else {
        auto it = zset.lower_bound(min, "");
        lr = zset.order_of_key(it->first.score, "");
        it = zset.lower_bound(max, "");
        rr = zset.order_of_key(it->first.score, "");
        return rr - lr;
    }
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
    if (check_range(con, start, stop, lower, upper) == C_ERR)
        return;
    if (withscores)
        con.append_reply_multi((stop - start + 1) * 2);
    else
        con.append_reply_multi(stop - start + 1);
    int i = 0;
    if (!is_reverse) {
        for (auto it = zset.zsl.cbegin(); it != zset.zsl.cend(); ++it, ++i) {
            if (i < start) continue;
            if (i > stop) break;
            con.append_reply_string(it->first.key);
            if (withscores)
                con.append_reply_double(it->first.score);
        }
    }
    if (is_reverse) {
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

static void zrangefor(context_t& con, Zset::iterator first, Zset::iterator last,
                      int count, bool withscores, bool is_reverse)
{
    bool is_count = (count > 0);
    if (!is_reverse) {
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
                                    int count, bool withscores, bool is_reverse)
{
    if (offset < 0 || count <= 0) {
        con.append(shared.nil);
        return C_ERR;
    }
    while (first != last && offset > 0) {
        is_reverse ? --last : ++first;
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
    zrangefor(con, first, last, count, withscores, is_reverse);
    return C_OK;
}

// Z(REV)RANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
void DB::_zrangebyscore(context_t& con, bool is_reverse)
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
    auto r = parse_interval(con, cmdops, lower, upper, min, max, min_str, max_str, is_reverse);
    if (r == C_ERR) return;
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
        if (!is_reverse) {
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
        if (!is_reverse) {
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
    auto distance = zcount_range(zset, cmdops, lower, upper, min, max);
    if ((cmdops & WITHSCORES) && (cmdops & LIMIT)) {
        zrangebyscore_with_limit(con, first, last, distance, offset, count, true, is_reverse);
    } else if (cmdops & WITHSCORES) {
        con.append_reply_multi(distance * 2);
        zrangefor(con, first, last, 0, true, is_reverse);
    } else if (cmdops & LIMIT) {
        zrangebyscore_with_limit(con, first, last, distance, offset, count, false, is_reverse);
    } else {
        con.append_reply_multi(distance);
        zrangefor(con, first, last, 0, false, is_reverse);
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
    if (check_range(con, start, stop, lower, upper) == C_ERR)
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
    auto& key = con.argv[1];
    auto& min_str = con.argv[2];
    auto& max_str = con.argv[3];
    int lower = 0, upper = 0;
    double min = 0, max = 0;
    auto r = parse_interval(con, cmdops, lower, upper, min, max, min_str, max_str, false);
    if (r == C_ERR) return;
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
