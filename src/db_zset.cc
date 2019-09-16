#include <tuple>
#include "db.h"

using namespace Alice;

// 由于set不支持rank操作，所以其相关操作都是O(n)的
// 包括zcount、zrank、zrangebyscore等

void DB::zaddCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    if (cmdlist.size() % 2 != 0) db_return(con, db_return_argnumber_err);
    for (size_t i = 2; i < cmdlist.size(); i += 2) {
        void(str2f(cmdlist[i].c_str()));
        if (str2numberErr()) db_return(con, db_return_float_err);
    }
    expireIfNeeded(cmdlist[1]);
    touchWatchKey(cmdlist[1]);
    auto it = find(cmdlist[1]);
    if (!isFound(it)) {
        _Zset zset;
        _Zmap zmap;
        for (size_t i = 2; i < cmdlist.size(); i += 2) {
            double score = atof(cmdlist[i].c_str());
            zset.emplace(score, cmdlist[i+1]);
            zmap.emplace(cmdlist[i+1], score);
        }
        insert(cmdlist[1], std::make_tuple(zset, zmap));
        appendReplyNumber(con, (cmdlist.size() - 2) / 2);
        return;
    }
    checkType(con, it, Zset);
    auto& tuple = getZsetValue(it);
    _Zset& zset = std::get<0>(tuple);
    _Zmap& zmap = std::get<1>(tuple);
    int retval = 0;
    for (size_t i = 2; i < cmdlist.size(); i += 2) {
        double score = atof(cmdlist[i].c_str());
        auto tuple = std::make_tuple(score, cmdlist[i+1]);
        auto e = zmap.find(cmdlist[i+1]);
        if (e != zmap.end()) {
            zmap.erase(cmdlist[i+1]);
            zset.erase(std::make_tuple(e->second, cmdlist[i+1]));
        } else
            retval++;
        zmap.emplace(cmdlist[i+1], score);
        zset.emplace(std::move(tuple));
    }
    appendReplyNumber(con, retval);
}

void DB::zscoreCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    expireIfNeeded(cmdlist[1]);
    auto it = find(cmdlist[1]);
    if (!isFound(it)) db_return(con, db_return_nil);
    checkType(con, it, Zset);
    auto& tuple = getZsetValue(it);
    _Zmap zmap = std::get<1>(tuple);
    auto e = zmap.find(cmdlist[2]);
    if (e != zmap.end()) {
        appendReplyDouble(con, e->second);
    } else
        con.append(db_return_nil);
}

void DB::zincrbyCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    double score = str2f(cmdlist[2].c_str());
    if (str2numberErr()) db_return(con, db_return_float_err);
    expireIfNeeded(cmdlist[1]);
    touchWatchKey(cmdlist[1]);
    auto it = find(cmdlist[1]);
    if (!isFound(it)) {
        _Zset zset;
        _Zmap zmap;
        zmap.emplace(cmdlist[3], score);
        zset.emplace(score, cmdlist[3]);
        insert(cmdlist[1], std::make_tuple(zset, zmap));
        appendReplyDouble(con, score);
        return;
    }
    checkType(con, it, Zset);
    auto& tuple = getZsetValue(it);
    _Zset& zset = std::get<0>(tuple);
    _Zmap& zmap = std::get<1>(tuple);
    auto e = zmap.find(cmdlist[3]);
    if (e != zmap.end()) {
        zset.erase(std::make_tuple(e->second, cmdlist[3]));
        e->second += score;
        zset.emplace(e->second, cmdlist[3]);
        appendReplyDouble(con, e->second);
    } else {
        zmap.emplace(cmdlist[3], score);
        zset.emplace(score, cmdlist[3]);
        appendReplyDouble(con, score);
    }
}

void DB::zcardCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    expireIfNeeded(cmdlist[1]);
    auto it = find(cmdlist[1]);
    if (!isFound(it)) db_return(con, db_return_0);
    checkType(con, it, Zset);
    auto& tuple = getZsetValue(it);
    appendReplyNumber(con, std::get<0>(tuple).size());
}

void DB::zcountCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    double min = str2f(cmdlist[2].c_str());
    if (str2numberErr()) db_return(con, db_return_float_err);
    double max = str2f(cmdlist[3].c_str());
    if (str2numberErr()) db_return(con, db_return_float_err);
    if (min > max) db_return(con, db_return_0);
    expireIfNeeded(cmdlist[1]);
    auto it = find(cmdlist[1]);
    if (!isFound(it)) db_return(con, db_return_0);
    checkType(con, it, Zset);
    auto& tuple = getZsetValue(it);
    _Zset& zset = std::get<0>(tuple);
    auto lowerbound = zset.lower_bound(std::make_tuple(min, ""));
    auto upperbound = zset.upper_bound(std::make_tuple(max, ""));
    // if max >= set.max() upperbound == zset.end()
    // else upperbound = set.find(max) + 1
    if (lowerbound == zset.end()) db_return(con, db_return_0);
    int distance = std::distance(lowerbound, upperbound);
    appendReplyNumber(con, distance);
}

void DB::zrange(Context& con, bool reverse)
{
    auto& cmdlist = con.commandList();
    bool withscores = false;
    if (cmdlist.size() > 4 ) {
        if (strcasecmp(cmdlist[4].c_str(), "WITHSCORES")) {
            db_return(con, db_return_syntax_err);
        }
        withscores = true;
    }
    int start = str2l(cmdlist[2].c_str());
    if (str2numberErr()) db_return(con, db_return_integer_err);
    int stop = str2l(cmdlist[3].c_str());
    if (str2numberErr()) db_return(con, db_return_integer_err);
    expireIfNeeded(cmdlist[1]);
    auto it = find(cmdlist[1]);
    if (!isFound(it)) db_return(con, db_return_nil);
    checkType(con, it, Zset);
    auto& tuple = getZsetValue(it);
    _Zset& zset = std::get<0>(tuple);
    int upperbound = zset.size() - 1;
    int lowerbound = -zset.size();
    if (checkRange(con, &start, &stop, lowerbound, upperbound) == C_ERR)
        return;
    if (withscores)
        appendReplyMulti(con, (stop - start + 1) * 2);
    else
        appendReplyMulti(con, stop - start + 1);
    int i = 0;
    if (!reverse) {
        for (auto& it : zset) {
            if (i < start) {
                i++;
                continue;
            }
            if (i > stop)
                break;
            appendReplyString(con, std::get<1>(it));
            if (withscores)
                appendReplyDouble(con, std::get<0>(it));
            i++;
        }
    } else {
        for (auto it = zset.crbegin(); it != zset.crend(); it++) {
            if (i < start) {
                i++;
                continue;
            }
            if (i > stop)
                break;
            appendReplyString(con, std::get<1>(*it));
            if (withscores)
                appendReplyDouble(con, std::get<0>(*it));
            i++;
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

void DB::zrank(Context& con, bool reverse)
{
    auto& cmdlist = con.commandList();
    expireIfNeeded(cmdlist[1]);
    auto it = find(cmdlist[1]);
    if (!isFound(it)) db_return(con, db_return_nil);
    checkType(con, it, Zset);
    auto& tuple = getZsetValue(it);
    _Zset& zset = std::get<0>(tuple);
    _Zmap& zmap = std::get<1>(tuple);
    auto e = zmap.find(cmdlist[2]);
    if (e == zmap.end()) db_return(con, db_return_nil);
    int distance = 0;
    auto last = zset.find(std::make_tuple(e->second, cmdlist[2]));
    if (!reverse)
        distance = std::distance(zset.cbegin(), last);
    else
        distance = std::distance(last, zset.cend());
    appendReplyNumber(con, distance);
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

static void checkLimit(unsigned *cmdops, int *lower, int *upper,
        const std::string& min, const std::string& max)
{
    if (min[0] == '(') *cmdops |= LOI;
    if (max[0] == '(') *cmdops |= ROI;
    if (*cmdops & LOI) {
        if (strcasecmp(min.c_str(), "(-inf") == 0)
            *lower = MIN_INF;
        else if (strcasecmp(min.c_str(), "(+inf") == 0)
            *lower = POS_INF;
    } else {
        if (strcasecmp(min.c_str(), "-inf") == 0)
            *lower = MIN_INF;
        else if (strcasecmp(min.c_str(), "+inf") == 0)
            *lower = POS_INF;
    }
    if (*cmdops & ROI) {
        if (strcasecmp(max.c_str(), "(-inf") == 0)
            *upper = MIN_INF;
        else if (strcasecmp(max.c_str(), "(+inf") == 0)
            *upper = POS_INF;
    } else {
        if (strcasecmp(max.c_str(), "-inf") == 0)
            *upper = MIN_INF;
        else if (strcasecmp(max.c_str(), "+inf") == 0)
            *upper = POS_INF;
    }
}

static void zrangefor(Context& con, DB::_Zset::iterator first, DB::_Zset::iterator last,
        int count, bool withscores, bool reverse)
{
    bool isCount = (count > 0);
    if (!reverse) {
        while (first != last) {
            DB::appendReplyString(con, std::get<1>(*first));
            if (withscores)
                DB::appendReplyDouble(con, std::get<0>(*first));
            ++first;
            if (isCount && --count == 0)
                break;
        }
    } else {
        for (--last; ; --last) {
            DB::appendReplyString(con, std::get<1>(*last));
            if (withscores)
                DB::appendReplyDouble(con, std::get<0>(*last));
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
        con.append(db_return_nil);
        return C_ERR;
    }
    while (lowerbound != upperbound && offset > 0) {
        reverse ? --upperbound : ++lowerbound;
        --offset;
    }
    if (lowerbound == upperbound) {
        con.append(db_return_nil);
        return C_ERR;
    }
    int distance = std::distance(lowerbound, upperbound);
    if (count > distance) count = distance;
    if (withscores)
        DB::appendReplyMulti(con, count * 2);
    else
        DB::appendReplyMulti(con, count);
    zrangefor(con, lowerbound, upperbound, count, withscores, reverse);
    return C_OK;
}

void DB::zrangeByScore(Context& con, bool reverse)
{
    unsigned cmdops = 0;
    int offset = 0, count = 0;
    int lower = 0, upper = 0;
    double min = 0, max = 0;
    auto& cmdlist = con.commandList();
    size_t len = cmdlist.size();
    for (size_t i = 4; i < len; i++) {
        std::transform(cmdlist[i].begin(), cmdlist[i].end(), cmdlist[i].begin(), ::toupper);
        auto op = zrbsops.find(cmdlist[i]);
        if (op == zrbsops.end()) db_return(con, db_return_syntax_err);
        cmdops |= op->second;
        switch (op->second) {
        case WITHSCORES: break;
        case LIMIT: {
            if (i + 2 >= len) db_return(con, db_return_syntax_err);
            offset = str2l(cmdlist[++i].c_str());
            if (str2numberErr()) db_return(con, db_return_integer_err);
            count = str2l(cmdlist[++i].c_str());
            if (str2numberErr()) db_return(con, db_return_integer_err);
            break;
        }
        default: db_return(con, db_return_syntax_err);
        }
    }
    checkLimit(&cmdops, &lower, &upper, cmdlist[2], cmdlist[3]);
    if (!lower) {
        if (!reverse)
            min = str2f((cmdops & LOI) ? cmdlist[2].c_str() + 1 : cmdlist[2].c_str());
        else
            max = str2f((cmdops & LOI) ? cmdlist[2].c_str() + 1 : cmdlist[2].c_str());
        if (str2numberErr()) db_return(con, db_return_float_err);
    }
    if (!upper) {
        if (!reverse)
            max = str2f((cmdops & ROI) ? cmdlist[3].c_str() + 1 : cmdlist[3].c_str());
        else
            min = str2f((cmdops & ROI) ? cmdlist[3].c_str() + 1 : cmdlist[3].c_str());
        if (str2numberErr()) db_return(con, db_return_float_err);
    }
    // [+-]inf[other chars]中前缀可以被合法转换，但整个字符串是无效的
    if (isinf(min) || isinf(max)) db_return(con, db_return_syntax_err);
    if (!lower && !upper && min > max) db_return(con, db_return_nil);
    if ((reverse && (lower == MIN_INF || upper == POS_INF))
    || (!reverse && (lower == POS_INF || upper == MIN_INF))) {
        db_return(con, db_return_nil);
    }
    expireIfNeeded(cmdlist[1]);
    auto it = find(cmdlist[1]);
    if (!isFound(it)) db_return(con, db_return_nil);
    checkType(con, it, Zset);
    auto& tuple = getZsetValue(it);
    _Zset& zset = std::get<0>(tuple);
    _Zset::const_iterator lowerbound, upperbound;
    if (lower) lowerbound = zset.cbegin();
    else lowerbound = zset.lower_bound(std::make_tuple(min, ""));
    if (upper) upperbound = zset.cend();
    else upperbound = zset.upper_bound(std::make_tuple(max, ""));
    if (lowerbound == zset.end()) db_return(con, db_return_nil);
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
                db_return(con, db_return_nil);
            }
            ++upperbound;
        } else {
            while (lowerbound != upperbound && std::get<0>(*lowerbound) == min)
                ++lowerbound;
        }
    }
    if (lowerbound == upperbound) db_return(con, db_return_nil);
    int distance = 0;
    if (lower && upper)
        distance = zset.size();
    else
        distance = std::distance(lowerbound, upperbound);
    if ((cmdops & WITHSCORES) && (cmdops & LIMIT)) {
        zrangeByScoreWithLimit(con, lowerbound, upperbound,
                offset, count, true, reverse);
    } else if (cmdops & WITHSCORES) {
        appendReplyMulti(con, distance * 2);
        zrangefor(con, lowerbound, upperbound, 0, true, reverse);
    } else if (cmdops & LIMIT) {
        zrangeByScoreWithLimit(con, lowerbound, upperbound,
                offset, count, false, reverse);
    } else {
        appendReplyMulti(con, distance);
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

void DB::zremCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    expireIfNeeded(cmdlist[1]);
    auto it = find(cmdlist[1]);
    if (!isFound(it)) db_return(con, db_return_0);
    checkType(con, it, Zset);
    auto& tuple = getZsetValue(it);
    _Zset& zset = std::get<0>(tuple);
    _Zmap& zmap = std::get<1>(tuple);
    int rem = 0;
    for (size_t i = 2; i < cmdlist.size(); i++) {
        auto e = zmap.find(cmdlist[i]);
        if (e != zmap.end()) {
            zset.erase(std::make_tuple(e->second, e->first));
            zmap.erase(e->first);
            rem++;
        }
    }
    if (zset.empty()) delKeyWithExpire(cmdlist[1]);
    appendReplyNumber(con, rem);
    touchWatchKey(cmdlist[1]);
}

void DB::zremRangeByRankCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    int start = str2l(cmdlist[2].c_str());
    if (str2numberErr()) db_return(con, db_return_integer_err);
    int stop = str2l(cmdlist[3].c_str());
    if (str2numberErr()) db_return(con, db_return_integer_err);
    expireIfNeeded(cmdlist[1]);
    auto it = find(cmdlist[1]);
    if (!isFound(it)) db_return(con, db_return_0);
    checkType(con, it, Zset);
    auto& tuple = getZsetValue(it);
    _Zset& zset = std::get<0>(tuple);
    _Zmap& zmap = std::get<1>(tuple);
    int upperbound = zmap.size() - 1;
    int lowerbound = -zmap.size();
    if (checkRange(con, &start, &stop, lowerbound, upperbound) == C_ERR)
        return;
    int i = 0, rem = 0;
    for (auto it = zmap.begin(); it != zmap.end(); ) {
        if (i < start) {
            i++;
            continue;
        }
        if (i > stop)
            break;
        auto e = it++;
        zset.erase(std::make_tuple(e->second, e->first));
        zmap.erase(e->first);
        i++;
        rem++;
    }
    if (zset.empty()) delKeyWithExpire(cmdlist[1]);
    appendReplyNumber(con, rem);
    touchWatchKey(cmdlist[1]);
}

void DB::zremRangeByScoreCommand(Context& con)
{
    unsigned cmdops = 0;
    auto& cmdlist = con.commandList();
    int lower = 0, upper = 0;
    checkLimit(&cmdops, &lower, &upper, cmdlist[2], cmdlist[3]);
    double min = 0, max = 0;
    if (!lower) {
        min = str2f((cmdops & LOI)? cmdlist[2].c_str() + 1 : cmdlist[2].c_str());
        if (str2numberErr()) db_return(con, db_return_float_err);
    }
    if (!upper) {
        max = str2f((cmdops & ROI) ? cmdlist[3].c_str() + 1 : cmdlist[3].c_str());
        if (str2numberErr()) db_return(con, db_return_float_err);
    }
    if (isinf(min) || isinf(max)) db_return(con, db_return_syntax_err);
    if (!lower && !upper && min > max) db_return(con, db_return_0);
    if (lower == POS_INF || upper == MIN_INF) db_return(con, db_return_0);
    expireIfNeeded(cmdlist[1]);
    auto it = find(cmdlist[1]);
    if (!isFound(it)) db_return(con, db_return_0);
    checkType(con, it, Zset);
    auto& tuple = getZsetValue(it);
    _Zset& zset = std::get<0>(tuple);
    _Zmap& zmap = std::get<1>(tuple);
    auto lowerbound = zset.lower_bound(std::make_tuple(min, ""));
    auto upperbound = zset.upper_bound(std::make_tuple(max, ""));
    if (lowerbound == zset.end()) db_return(con, db_return_0);
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
    if (lowerbound == upperbound) db_return(con, db_return_0);
    int rem = 0;
    while (lowerbound != upperbound) {
        auto e = lowerbound++;
        zmap.erase(std::get<1>(*e));
        zset.erase(e);
        rem++;
    }
    if (zset.empty()) delKeyWithExpire(cmdlist[1]);
    appendReplyNumber(con, rem);
}
