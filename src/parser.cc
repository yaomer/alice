#include "db_base.h"
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

}
