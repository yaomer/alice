#include "parser.h"

namespace alice {

static std::unordered_map<std::string, int> setops = {
    { "NX", SET_NX },
    { "XX", SET_XX },
    { "EX", SET_EX },
    { "PX", SET_PX },
};

int parse_set_args(context_t& con, unsigned& cmdops, int64_t& expire)
{
    size_t len = con.argv.size();
    for (size_t i = 3; i < len; i++) {
        std::transform(con.argv[i].begin(), con.argv[i].end(), con.argv[i].begin(), ::toupper);
        auto op = setops.find(con.argv[i]);
        if (op != setops.end()) cmdops |= op->second;
        else goto syntax_err;
        switch (op->second) {
        case SET_NX: case SET_XX:
            break;
        case SET_EX: case SET_PX: {
            if (i + 1 >= len) goto syntax_err;
            expire = str2ll(con.argv[++i]);
            if (str2numerr()) goto integer_err;
            if (expire <= 0) goto timeout_err;
            if (op->second == SET_EX)
                expire *= 1000;
            break;
        }
        default:
            goto syntax_err;
        }
    }
    return C_OK;
syntax_err:
    con.append(shared.syntax_err);
    return C_ERR;
integer_err:
    con.append(shared.integer_err);
    return C_ERR;
timeout_err:
    con.append(shared.timeout_err);
    return C_ERR;
}

// 检查索引范围访问
// 类似于lrange key start stop
// 我们还会将负索引访问映射到正常的范围访问
int check_range_index(context_t& con, long& start, long& stop, long lower, long upper)
{
    if (start > upper || stop < lower) {
        con.append(shared.nil);
        return C_ERR;
    }
    if (start < 0 && start >= lower) {
        start += upper + 1;
    }
    if (stop < 0 && stop >= lower) {
        stop += upper + 1;
    }
    if (start < lower) {
        start = 0;
    }
    if (stop > upper) {
        stop = upper;
    }
    if (start > stop) {
        con.append(shared.nil);
        return C_ERR;
    }
    return C_OK;
}

#define MIN_INF 1 // -inf
#define POS_INF 2 // +inf

// 用于解析zset命令中出现的min和max参数
// `min_str`和`max_str`为传入的以上两个参数，我们会将解析后的结果写入到`min`和`max`中
// 特别地，如果`lower`为真，则表示`min`为inf；`upper`为真，则表示`max`为inf
int parse_range_score(context_t& con, unsigned& cmdops, score_range& r,
                      const std::string& min_str, const std::string& max_str)
{
    const char *mins = min_str.c_str();
    const char *maxs = max_str.c_str();
    if (min_str[0] == '(') cmdops |= LOI;
    if (max_str[0] == '(') cmdops |= ROI;
    if (cmdops & LOI) mins += 1;
    if (cmdops & ROI) maxs += 1;
    if (strcasecmp(mins, "-inf") == 0) r.lower = MIN_INF;
    else if (strcasecmp(mins, "+inf") == 0) r.lower = POS_INF;
    if (strcasecmp(maxs, "-inf") == 0) r.upper = MIN_INF;
    else if (strcasecmp(maxs, "+inf") == 0) r.upper = POS_INF;
    // get min and max
    if (!r.lower) {
        double fval = str2f(mins);
        if (str2numerr()) retval(con, shared.min_or_max_err, C_ERR);
        r.min = fval;
    }
    if (!r.upper) {
        double fval = str2f(maxs);
        if (str2numerr()) retval(con, shared.min_or_max_err, C_ERR);
        r.max = fval;
    }
    // [+-]inf[other chars]中前缀可以被合法转换，但整个字符串是无效的
    if (isinf(r.min) || isinf(r.max)) retval(con, shared.syntax_err, C_ERR);
    // min不能大于max
    if (!r.lower && !r.upper && r.min > r.max) retval(con, shared.nil, C_ERR);
    if (r.lower == POS_INF || r.upper == MIN_INF) {
        retval(con, shared.nil, C_ERR);
    }
    return C_OK;
}

thread_local std::unordered_map<std::string, int> zrbsops = {
    { "WITHSCORES", WITHSCORES },
    { "LIMIT",      LIMIT },
};

int parse_zrangebyscore_args(context_t& con, unsigned& cmdops,
                             long& offset, long& limit)
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
            limit = str2l(con.argv[++i]);
            if (str2numerr()) goto integer_err;
            if (offset < 0 || limit <= 0) {
                con.append(shared.multi_empty);
                return C_ERR;
            }
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

unsigned get_last_cmd(const std::string& lc)
{
    unsigned ops = 0;
    if (lc.compare("BLPOP") == 0) ops = BLOCK_LPOP;
    else if (lc.compare("BRPOP") == 0) ops = BLOCK_RPOP;
    else if (lc.compare("BRPOPLPUSH") == 0) ops = BLOCK_RPOPLPUSH;
    return ops;
}

}
