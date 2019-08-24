#include <time.h>
#include <stdlib.h>
#include <errno.h>
#include <random>
#include <vector>
#include <string>

#include "util.h"

namespace Alice {

void setSelfRunId(char *buf)
{
    struct timespec tsp;
    clock_gettime(_CLOCK_REALTIME, &tsp);
    std::uniform_int_distribution<size_t> u;
    std::mt19937_64 e(tsp.tv_sec * 1000000000 + tsp.tv_nsec);
    snprintf(buf, 17, "%lx", u(e));
    snprintf(buf + 16, 17, "%lx", u(e));
}

thread_local char convert_buf[32];

const char *convert(int64_t value)
{
    snprintf(convert_buf, sizeof(convert_buf), "%lld", value);
    return convert_buf;
}

const char *convert2f(double value)
{
    snprintf(convert_buf, sizeof(convert_buf), "%g", value);
    return convert_buf;
}

thread_local bool str2numerr;

#define STR2LONG 1
#define STR2LLONG 2
#define STR2DOUBLE 3

#define str2number(ptr, val, opt) \
    do { \
        str2numerr = false; \
        errno = 0; \
        char *eptr = nullptr; \
        switch (opt) { \
        case STR2LONG: val = strtol(ptr, &eptr, 10); \
        case STR2LLONG: val = strtoll(ptr, &eptr, 10); \
        case STR2DOUBLE: val = strtod(ptr, &eptr); \
        } \
        if (errno == ERANGE || eptr == ptr) { \
            str2numerr = true; \
            val = 0; \
        } \
    } while (0)

bool str2numberErr()
{
    return str2numerr;
}

long str2l(const char *nptr)
{
    long lval;
    str2number(nptr, lval, STR2LONG);
    return lval;
}

long long str2ll(const char *nptr)
{
    long llval;
    str2number(nptr, llval, STR2LLONG);
    return llval;
}

double str2f(const char *nptr)
{
    double dval;
    str2number(nptr, dval, STR2DOUBLE);
    return dval;
}

// 分割一个以\n结尾的字符串，将结果保存在argv中
int parseLine(std::vector<std::string>& argv, const char *line, const char *linep)
{
    const char *start;
    do {
        line = std::find_if(line, linep, [](char c){ return !isspace(c); });
        if (line == linep) break;
        if (*line == '\"') {
            start = ++line;
search:
            line = std::find(line, linep, '\"');
            if (line == linep) goto err;
            if (line[-1] == '\\') {
                line++;
                goto search;
            }
            if (!isspace(line[1])) goto err;
            argv.push_back(std::string(start, line - start));
            line++;
        } else {
            start = line;
            line = std::find_if(line, linep, [](char c){ return isspace(c); });
            if (line == linep) goto err;
            argv.push_back(std::string(start, line - start));
        }
    } while (1);
    return 0;
err:
    argv.clear();
    return -1;
}

}
