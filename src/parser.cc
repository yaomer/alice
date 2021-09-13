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

int check_range(context_t& con, int& start, int& stop, int lower, int upper)
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

// if lower is true, min is `-inf`
// if upper is true, max is `+inf`
int parse_interval(context_t& con, unsigned& cmdops, int& lower, int& upper,
                   double& min, double& max, const std::string& min_str,
                   const std::string& max_str, bool is_reverse)
{
    const char *mins = min_str.c_str();
    const char *maxs = max_str.c_str();
    if (min_str[0] == '(') cmdops |= LOI;
    if (max_str[0] == '(') cmdops |= ROI;
    if (cmdops & LOI) mins += 1;
    if (cmdops & ROI) maxs += 1;
    if (strcasecmp(mins, "-inf") == 0) lower = MIN_INF;
    else if (strcasecmp(mins, "+inf") == 0) lower = POS_INF;
    if (strcasecmp(maxs, "-inf") == 0) upper = MIN_INF;
    else if (strcasecmp(maxs, "+inf") == 0) upper = POS_INF;
    // get min and max
    if (!lower) {
        double fval = str2f(mins);
        if (str2numerr()) retval(con, shared.float_err, C_ERR);
        if (!is_reverse) min = fval;
        else max = fval;
    }
    if (!upper) {
        double fval = str2f(maxs);
        if (str2numerr()) retval(con, shared.float_err, C_ERR);
        if (!is_reverse) max = fval;
        else min = fval;
    }
    // [+-]inf[other chars]中前缀可以被合法转换，但整个字符串是无效的
    if (isinf(min) || isinf(max)) retval(con, shared.syntax_err, C_ERR);
    // min不能大于max
    if (!lower && !upper && min > max) retval(con, shared.nil, C_ERR);
    if ((is_reverse && (lower == MIN_INF || upper == POS_INF))
    || (!is_reverse && (lower == POS_INF || upper == MIN_INF))) {
        retval(con, shared.nil, C_ERR);
    }
    return C_OK;
}

thread_local std::unordered_map<std::string, int> zrbsops = {
    { "WITHSCORES", WITHSCORES },
    { "LIMIT",      LIMIT },
};

int parse_zrangebyscore_args(context_t& con, unsigned& cmdops,
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

unsigned get_last_cmd(const std::string& lc)
{
    unsigned ops = 0;
    if (lc.compare("BLPOP") == 0) ops = BLOCK_LPOP;
    else if (lc.compare("BRPOP") == 0) ops = BLOCK_RPOP;
    else if (lc.compare("BRPOPLPUSH") == 0) ops = BLOCK_RPOPLPUSH;
    return ops;
}

}
