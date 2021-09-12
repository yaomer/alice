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

unsigned get_last_cmd(const std::string& lc)
{
    unsigned ops = 0;
    if (lc.compare("BLPOP") == 0) ops = BLOCK_LPOP;
    else if (lc.compare("BRPOP") == 0) ops = BLOCK_RPOP;
    else if (lc.compare("BRPOPLPUSH") == 0) ops = BLOCK_RPOPLPUSH;
    return ops;
}

}
