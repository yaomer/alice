#include <time.h>
#include <stdlib.h>
#include <errno.h>
#include <random>
#include <vector>
#include <string>
#include "util.h"

namespace Alice {

// buflen >= 33
void setSelfRunId(char *buf)
{
    struct timespec tsp;
    clock_gettime(_CLOCK_REALTIME, &tsp);
    std::uniform_int_distribution<size_t> u;
    std::mt19937_64 e(tsp.tv_sec * 1000000000 + tsp.tv_nsec);
    snprintf(buf, 17, "%lx", u(e));
    snprintf(buf + 16, 17, "%lx", u(e));
}

thread_local char convert_buf[64];

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
        case STR2LONG: val = strtol(ptr, &eptr, 10); break; \
        case STR2LLONG: val = strtoll(ptr, &eptr, 10); break; \
        case STR2DOUBLE: val = strtod(ptr, &eptr); break; \
        } \
        if (errno == ERANGE || eptr == ptr || *eptr != '\0') { \
            str2numerr = true; \
            val = 0; \
        } \
    } while (0)

// if str2l/ll/f convert error, return true
// this func is thread-safe
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
// 带空白符的字符串必须包含在双引号内
// set key value -> [set] [key] [value]
// set key "hello, wrold" -> [set] [key] [hello, world]
// if this line is all whitespace or parse error, return -1; else return 0
int parseLine(std::vector<std::string>& argv, const char *line, const char *linep)
{
    bool iter = false;
    const char *start;
    do {
        line = std::find_if_not(line, linep, ::isspace);
        if (line == linep) {
            if (iter) break;
            else goto err;
        }
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
            argv.emplace_back(start, line - start);
            line++;
        } else {
            start = line;
            line = std::find_if(line, linep, ::isspace);
            if (line == linep) goto err;
            argv.emplace_back(start, line - start);
        }
        iter = true;
    } while (1);
    return 0;
err:
    argv.clear();
    return -1;
}

// if c == ','
// for [a,b,c,d] return [a][b][c][d]
void parseLineWithSeparator(std::vector<std::string>& argv,
                            const char *s,
                            const char *es,
                            char c)
{
    const char *p;
    while (true) {
        p = std::find(s, es, c);
        if (p == es) break;
        argv.emplace_back(s, p);
        s = p + 1;
    }
    argv.emplace_back(s, p);
}

}
