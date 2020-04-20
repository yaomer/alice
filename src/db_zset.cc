#include <tuple>
#include "db.h"

using namespace Alice;

// 由于set不支持rank操作，所以其相关操作都是O(n)的
// 包括zcount、zrange、zrank、zrangebyscore等

// ZADD key score member [score member ...]
void DB::zaddCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    size_t size = cmdlist.size();
    auto& key = cmdlist[1];
    if (cmdlist.size() % 2 != 0) db_return(con, reply.argnumber_err);
    // 先检查参数中的所有score
    for (size_t i = 2; i < size; i += 2) {
        void(str2f(cmdlist[i].c_str()));
        if (str2numberErr()) db_return(con, reply.float_err);
    }
    checkExpire(key);
    touchWatchKey(key);
    auto it = find(key);
    if (!isFound(it)) { // 添加一个新的zset-key
        _Zset zset;
        _Zmap zmap;
        for (size_t i = 2; i < size; i += 2) {
            double score = atof(cmdlist[i].c_str());
            zset.emplace(score, cmdlist[i+1]);
            zmap.emplace(cmdlist[i+1], score);
        }
        insert(key, std::make_tuple(zset, zmap));
        con.appendReplyNumber((size - 2) / 2);
        return;
    }
    checkType(con, it, Zset);
    auto& tuple = getZsetValue(it);
    _Zset& zset = std::get<0>(tuple);
    _Zmap& zmap = std::get<1>(tuple);
    int adds = 0;
    for (size_t i = 2; i < cmdlist.size(); i += 2) {
        double score = atof(cmdlist[i].c_str());
        auto tuple = std::make_tuple(score, cmdlist[i+1]);
        auto e = zmap.find(cmdlist[i+1]);
        if (e != zmap.end()) {
            // 如果成员已存在，则会更新它的分数
            zmap.erase(cmdlist[i+1]);
            zset.erase(std::make_tuple(e->second, cmdlist[i+1]));
        } else
            adds++;
        zmap.emplace(cmdlist[i+1], score);
        zset.emplace(std::move(tuple));
    }
    con.appendReplyNumber(adds);
}

// ZSCORE key member
void DB::zscoreCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    auto& key = cmdlist[1];
    auto& member = cmdlist[2];
    checkExpire(key);
    auto it = find(key);
    if (!isFound(it)) db_return(con, reply.nil);
    checkType(con, it, Zset);
    auto& tuple = getZsetValue(it);
    _Zmap zmap = std::get<1>(tuple);
    auto e = zmap.find(member);
    if (e != zmap.end()) {
        con.appendReplyDouble(e->second);
    } else
        con.append(reply.nil);
}

// ZINCRBY key increment member
void DB::zincrbyCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    auto& key = cmdlist[1];
    auto& member = cmdlist[3];
    double score = str2f(cmdlist[2].c_str());
    if (str2numberErr()) db_return(con, reply.float_err);
    checkExpire(key);
    touchWatchKey(key);
    auto it = find(key);
    if (!isFound(it)) {
        _Zset zset;
        _Zmap zmap;
        zmap.emplace(member, score);
        zset.emplace(score, member);
        insert(key, std::make_tuple(zset, zmap));
        con.appendReplyDouble(score);
        return;
    }
    checkType(con, it, Zset);
    auto& tuple = getZsetValue(it);
    _Zset& zset = std::get<0>(tuple);
    _Zmap& zmap = std::get<1>(tuple);
    auto e = zmap.find(member);
    if (e != zmap.end()) {
        zset.erase(std::make_tuple(e->second, member));
        score += e->second;
        zmap[member] = score;
    } else {
        zmap.emplace(member, score);
    }
    zset.emplace(score, member);
    con.appendReplyDouble(score);
}

// ZCARD key
void DB::zcardCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    auto& key = cmdlist[1];
    checkExpire(key);
    auto it = find(key);
    if (!isFound(it)) db_return(con, reply.n0);
    checkType(con, it, Zset);
    auto& tuple = getZsetValue(it);
    auto& zset = std::get<0>(tuple);
    con.appendReplyNumber(zset.size());
}

// ZCOUNT key min max
void DB::zcountCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    auto& key = cmdlist[1];
    double min = str2f(cmdlist[2].c_str());
    if (str2numberErr()) db_return(con, reply.float_err);
    double max = str2f(cmdlist[3].c_str());
    if (str2numberErr()) db_return(con, reply.float_err);
    if (min > max) db_return(con, reply.n0);
    checkExpire(key);
    auto it = find(key);
    if (!isFound(it)) db_return(con, reply.n0);
    checkType(con, it, Zset);
    auto& tuple = getZsetValue(it);
    _Zset& zset = std::get<0>(tuple);
    auto lower = zset.lower_bound(std::make_tuple(min, ""));
    auto upper = zset.upper_bound(std::make_tuple(max, ""));
    // if max >= set.max() upper == zset.end()
    // else upper = set.find(max) + 1
    if (lower == zset.end()) db_return(con, reply.n0);
    int distance = std::distance(lower, upper);
    con.appendReplyNumber(distance);
}

// Z(REV)RANGE key start stop [WITHSCORES]
void DB::zrange(Context& con, bool reverse)
{
    auto& cmdlist = con.commandList();
    auto& key = cmdlist[1];
    bool withscores = false;
    if (cmdlist.size() > 4 ) {
        if (strcasecmp(cmdlist[4].c_str(), "WITHSCORES")) {
            db_return(con, reply.syntax_err);
        }
        withscores = true;
    }
    int start = str2l(cmdlist[2].c_str());
    if (str2numberErr()) db_return(con, reply.integer_err);
    int stop = str2l(cmdlist[3].c_str());
    if (str2numberErr()) db_return(con, reply.integer_err);
    checkExpire(key);
    auto it = find(key);
    if (!isFound(it)) db_return(con, reply.nil);
    checkType(con, it, Zset);
    auto& tuple = getZsetValue(it);
    _Zset& zset = std::get<0>(tuple);
    int upper = zset.size() - 1;
    int lower = -zset.size();
    if (checkRange(con, start, stop, lower, upper) == C_ERR)
        return;
    if (withscores)
        con.appendReplyMulti((stop - start + 1) * 2);
    else
        con.appendReplyMulti(stop - start + 1);
    int i = 0;
    if (!reverse) {
        for (auto it = zset.cbegin(); it != zset.cend(); ++it, ++i) {
            if (i < start) continue;
            if (i > stop) break;
            con.appendReplyString(std::get<1>(*it));
            if (withscores)
                con.appendReplyDouble(std::get<0>(*it));
        }
    }
    if (reverse) {
        for (auto it = zset.crbegin(); it != zset.crend(); ++it, ++i) {
            if (i < start) continue;
            if (i > stop) break;
            con.appendReplyString(std::get<1>(*it));
            if (withscores)
                con.appendReplyDouble(std::get<0>(*it));
        }
    }
}

void DB::zrangeCommand(Context& con)
{
    zrange(con, false);
}

void DB::zrevRangeCommand(Context& con)
{
    zrange(con, true);
}

// Z(REV)RANK key member
void DB::zrank(Context& con, bool reverse)
{
    auto& cmdlist = con.commandList();
    auto& key = cmdlist[1];
    auto& member = cmdlist[2];
    checkExpire(key);
    auto it = find(key);
    if (!isFound(it)) db_return(con, reply.nil);
    checkType(con, it, Zset);
    auto& tuple = getZsetValue(it);
    _Zset& zset = std::get<0>(tuple);
    _Zmap& zmap = std::get<1>(tuple);
    auto e = zmap.find(member);
    if (e == zmap.end()) db_return(con, reply.nil);
    int distance = 0;
    auto last = zset.find(std::make_tuple(e->second, member));
    if (!reverse)
        distance = std::distance(zset.cbegin(), last);
    else
        distance = std::distance(last, zset.cend());
    con.appendReplyNumber(distance);
}

void DB::zrankCommand(Context& con)
{
    zrank(con, false);
}

void DB::zrevRankCommand(Context& con)
{
    zrank(con, true);
}

#define MIN_INF 1 // -inf
#define POS_INF 2 // +inf
#define WITHSCORES  0x01
#define LIMIT       0x02
#define LOI         0x04 // (min
#define ROI         0x08 // (max

namespace Alice {

    thread_local std::unordered_map<std::string, int> zrbsops = {
        { "WITHSCORES", WITHSCORES },
        { "LIMIT",      LIMIT },
    };
}

static void checkLimit(unsigned& cmdops, int& lower, int& upper,
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

static void zrangefor(Context& con, DB::_Zset::iterator first, DB::_Zset::iterator last,
                      int count, bool withscores, bool reverse)
{
    bool isCount = (count > 0);
    if (!reverse) {
        while (first != last) {
            con.appendReplyString(std::get<1>(*first));
            if (withscores)
                con.appendReplyDouble(std::get<0>(*first));
            ++first;
            if (isCount && --count == 0)
                break;
        }
    } else {
        for (--last; ; --last) {
            con.appendReplyString(std::get<1>(*last));
            if (withscores)
                con.appendReplyDouble(std::get<0>(*last));
            if (last == first || (isCount && --count == 0))
                break;
        }
    }
}

static int zrangeByScoreWithLimit(Context& con, DB::_Zset::iterator lowerbound,
                                  DB::_Zset::iterator upperbound, int offset, int count,
                                  bool withscores, bool reverse)
{
    if (offset < 0 || count <= 0) {
        con.append(reply.nil);
        return C_ERR;
    }
    while (lowerbound != upperbound && offset > 0) {
        reverse ? --upperbound : ++lowerbound;
        --offset;
    }
    if (lowerbound == upperbound) {
        con.append(reply.nil);
        return C_ERR;
    }
    int distance = std::distance(lowerbound, upperbound);
    if (count > distance) count = distance;
    if (withscores)
        con.appendReplyMulti(count * 2);
    else
        con.appendReplyMulti(count);
    zrangefor(con, lowerbound, upperbound, count, withscores, reverse);
    return C_OK;
}

static int parseZrangeByScoreArgs(Context& con, unsigned& cmdops,
                                  int& offset, int& count)
{
    auto& cmdlist = con.commandList();
    size_t len = cmdlist.size();
    for (size_t i = 4; i < len; i++) {
        std::transform(cmdlist[i].begin(), cmdlist[i].end(), cmdlist[i].begin(), ::toupper);
        auto op = zrbsops.find(cmdlist[i]);
        if (op == zrbsops.end()) goto syntax_err;
        cmdops |= op->second;
        switch (op->second) {
        case WITHSCORES: break;
        case LIMIT: {
            if (i + 2 >= len) goto syntax_err;
            offset = str2l(cmdlist[++i].c_str());
            if (str2numberErr()) goto integer_err;
            count = str2l(cmdlist[++i].c_str());
            if (str2numberErr()) goto integer_err;
            break;
        }
        }
    }
    return C_OK;
syntax_err:
    con.append(reply.syntax_err);
    return C_ERR;
integer_err:
    con.append(reply.integer_err);
    return C_ERR;
}
// Z(REV)RANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
void DB::zrangeByScore(Context& con, bool reverse)
{
    unsigned cmdops = 0;
    int offset = 0, count = 0;
    if (parseZrangeByScoreArgs(con, cmdops, offset, count) == C_ERR)
        return;
    auto& cmdlist = con.commandList();
    auto& key = cmdlist[1];
    auto& min_str = cmdlist[2];
    auto& max_str = cmdlist[3];
    int lower = 0, upper = 0;
    double min = 0, max = 0;
    checkLimit(cmdops, lower, upper, min_str, max_str);
    if (!lower) {
        double fval = str2f((cmdops & LOI) ? min_str.c_str() + 1 : min_str.c_str());
        if (!reverse) min = fval;
        else max = fval;
        if (str2numberErr()) db_return(con, reply.float_err);
    }
    if (!upper) {
        double fval = str2f((cmdops & ROI) ? max_str.c_str() + 1 : max_str.c_str());
        if (!reverse) max = fval;
        else min = fval;
        if (str2numberErr()) db_return(con, reply.float_err);
    }
    // [+-]inf[other chars]中前缀可以被合法转换，但整个字符串是无效的
    if (isinf(min) || isinf(max)) db_return(con, reply.syntax_err);
    if (!lower && !upper && min > max) db_return(con, reply.nil);
    if ((reverse && (lower == MIN_INF || upper == POS_INF))
    || (!reverse && (lower == POS_INF || upper == MIN_INF))) {
        db_return(con, reply.nil);
    }
    checkExpire(key);
    auto it = find(key);
    if (!isFound(it)) db_return(con, reply.nil);
    checkType(con, it, Zset);
    auto& tuple = getZsetValue(it);
    _Zset& zset = std::get<0>(tuple);
    _Zset::const_iterator lowerbound, upperbound;
    if (lower) lowerbound = zset.cbegin();
    else lowerbound = zset.lower_bound(std::make_tuple(min, ""));
    if (upper) upperbound = zset.cend();
    else upperbound = zset.upper_bound(std::make_tuple(max, ""));
    if (lowerbound == zset.end()) db_return(con, reply.nil);
    if (!lower && (cmdops & LOI)) {
        if (!reverse) {
            while (lowerbound != upperbound && std::get<0>(*lowerbound) == min)
                ++lowerbound;
        } else {
            --upperbound;
            while (lowerbound != upperbound && std::get<0>(*upperbound) == max)
                --upperbound;
            ++upperbound;
        }
    }
    if (!upper && (cmdops & ROI)) {
        if (!reverse) {
            --upperbound;
            while (lowerbound != upperbound && std::get<0>(*upperbound) == max)
                --upperbound;
            if (lowerbound == upperbound && std::get<0>(*upperbound) == max) {
                db_return(con, reply.nil);
            }
            ++upperbound;
        } else {
            while (lowerbound != upperbound && std::get<0>(*lowerbound) == min)
                ++lowerbound;
        }
    }
    if (lowerbound == upperbound) db_return(con, reply.nil);
    int distance = 0;
    if (lower && upper)
        distance = zset.size();
    else
        distance = std::distance(lowerbound, upperbound);
    if ((cmdops & WITHSCORES) && (cmdops & LIMIT)) {
        zrangeByScoreWithLimit(con, lowerbound, upperbound,
                offset, count, true, reverse);
    } else if (cmdops & WITHSCORES) {
        con.appendReplyMulti(distance * 2);
        zrangefor(con, lowerbound, upperbound, 0, true, reverse);
    } else if (cmdops & LIMIT) {
        zrangeByScoreWithLimit(con, lowerbound, upperbound,
                offset, count, false, reverse);
    } else {
        con.appendReplyMulti(distance);
        zrangefor(con, lowerbound, upperbound, 0, false, reverse);
    }
}

void DB::zrangeByScoreCommand(Context& con)
{
    zrangeByScore(con, false);
}

void DB::zrevRangeByScoreCommand(Context& con)
{
    zrangeByScore(con, true);
}

// ZREM key member [member ...]
void DB::zremCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    auto& key = cmdlist[1];
    checkExpire(key);
    auto it = find(key);
    if (!isFound(it)) db_return(con, reply.n0);
    checkType(con, it, Zset);
    auto& tuple = getZsetValue(it);
    _Zset& zset = std::get<0>(tuple);
    _Zmap& zmap = std::get<1>(tuple);
    int rems = 0;
    for (size_t i = 2; i < cmdlist.size(); i++) {
        auto e = zmap.find(cmdlist[i]);
        if (e != zmap.end()) {
            zset.erase(std::make_tuple(e->second, e->first));
            zmap.erase(e->first);
            rems++;
        }
    }
    checkEmpty(zset, key);
    touchWatchKey(key);
    con.appendReplyNumber(rems);
}

// ZREMRANGEBYRANK key start stop
void DB::zremRangeByRankCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    auto& key = cmdlist[1];
    int start = str2l(cmdlist[2].c_str());
    if (str2numberErr()) db_return(con, reply.integer_err);
    int stop = str2l(cmdlist[3].c_str());
    if (str2numberErr()) db_return(con, reply.integer_err);
    checkExpire(key);
    auto it = find(key);
    if (!isFound(it)) db_return(con, reply.n0);
    checkType(con, it, Zset);
    auto& tuple = getZsetValue(it);
    _Zset& zset = std::get<0>(tuple);
    _Zmap& zmap = std::get<1>(tuple);
    int upper = zmap.size() - 1;
    int lower = -zmap.size();
    if (checkRange(con, start, stop, lower, upper) == C_ERR)
        return;
    int i = 0, rems = 0;
    for (auto it = zmap.begin(); it != zmap.end(); i++) {
        if (i < start) continue;
        if (i > stop) break;
        auto e = it++;
        zset.erase(std::make_tuple(e->second, e->first));
        zmap.erase(e->first);
        rems++;
    }
    checkEmpty(zset, key);
    touchWatchKey(key);
    con.appendReplyNumber(rems);
}

// ZREMRANGEBYSCORE key min max
void DB::zremRangeByScoreCommand(Context& con)
{
    unsigned cmdops = 0;
    int lower = 0, upper = 0;
    auto& cmdlist = con.commandList();
    auto& key = cmdlist[1];
    auto& min_str = cmdlist[2];
    auto& max_str = cmdlist[3];
    checkLimit(cmdops, lower, upper, min_str, max_str);
    double min = 0, max = 0;
    if (!lower) {
        min = str2f((cmdops & LOI) ? min_str.c_str() + 1 : min_str.c_str());
        if (str2numberErr()) db_return(con, reply.float_err);
    }
    if (!upper) {
        max = str2f((cmdops & ROI) ? max_str.c_str() + 1 : max_str.c_str());
        if (str2numberErr()) db_return(con, reply.float_err);
    }
    if (isinf(min) || isinf(max)) db_return(con, reply.syntax_err);
    if (!lower && !upper && min > max) db_return(con, reply.n0);
    if (lower == POS_INF || upper == MIN_INF) db_return(con, reply.n0);
    checkExpire(key);
    auto it = find(key);
    if (!isFound(it)) db_return(con, reply.n0);
    checkType(con, it, Zset);
    auto& tuple = getZsetValue(it);
    _Zset& zset = std::get<0>(tuple);
    _Zmap& zmap = std::get<1>(tuple);
    auto lowerbound = zset.lower_bound(std::make_tuple(min, ""));
    auto upperbound = zset.upper_bound(std::make_tuple(max, ""));
    if (lowerbound == zset.end()) db_return(con, reply.n0);
    if (cmdops & LOI) {
        while (lowerbound != upperbound && std::get<0>(*lowerbound) == min)
            ++lowerbound;
    }
    if (cmdops & ROI) {
        --upperbound;
        while (lowerbound != upperbound && std::get<0>(*upperbound) == max)
            --upperbound;
        ++upperbound;
    }
    if (lowerbound == upperbound) db_return(con, reply.n0);
    int rems = 0;
    while (lowerbound != upperbound) {
        auto e = lowerbound++;
        zmap.erase(std::get<1>(*e));
        zset.erase(e);
        rems++;
    }
    checkEmpty(zset, key);
    con.appendReplyNumber(rems);
}
