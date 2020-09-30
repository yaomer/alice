#include <angel/util.h>

#include <time.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/stat.h>
#include <errno.h>

#include <vector>
#include <string>

#include "db_base.h"
#include "util.h"

#if defined (__APPLE__)
#include <mach/task.h>
#include <mach/mach_init.h>
#endif

namespace alice {

// buflen >= 33
void set_run_id(char *buf)
{
    struct timespec tsp;
    clock_gettime(_CLOCK_REALTIME, &tsp);
    std::uniform_int_distribution<size_t> u;
    std::mt19937_64 e(tsp.tv_sec * 1000000000 + tsp.tv_nsec);
    snprintf(buf, 17, "%lx", u(e));
    snprintf(buf + 16, 17, "%lx", u(e));
}

// if ok, return parsed-bytes
// if not enough data, return 0
// if error, return -1
ssize_t parse_request(argv_t& argv, angel::buffer& buf)
{
    const char *s = buf.peek();
    const char *es = s + buf.readable();
    const char *ps = s;
    size_t argc, len;
    // 解析命令个数
    const char *next = std::find(s, es, '\n');
    if (next == es) goto clr;
    if (s[0] != '*' || next[-1] != '\r') goto err;
    argc = atol(s + 1);
    s = next + 1;
    // 解析各个命令
    while (argc > 0) {
        next = std::find(s, es, '\n');
        if (next == es) goto clr;
        if (s[0] != '$' || next[-1] != '\r') goto err;
        len = atol(s + 1);
        s = next + 1;
        if (es - s < len + 2) goto clr;
        if (s[len] != '\r' || s[len+1] != '\n') goto err;
        argv.emplace_back(s, len);
        s += len + 2;
        argc--;
    }
    return s - ps;
clr: // not enough data
    argv.clear();
    return 0;
err: // error
    argv.clear();
    return -1;
}

void conv2resp(std::string& buffer, const argv_t& argv)
{
    context_t con;
    con.append_reply_multi(argv.size());
    for (auto& it : argv)
        con.append_reply_string(it);
    con.buf.swap(buffer);
}

thread_local char convert_buf[64];

const char *d2s(double value)
{
    snprintf(convert_buf, sizeof(convert_buf), "%g", value);
    return convert_buf;
}

thread_local bool str2numerror;

#define STR2LONG 1
#define STR2LLONG 2
#define STR2DOUBLE 3

#define str2number(ptr, val, opt) \
    do { \
        str2numerror = false; \
        errno = 0; \
        char *eptr = nullptr; \
        switch (opt) { \
        case STR2LONG: val = strtol(ptr, &eptr, 10); break; \
        case STR2LLONG: val = strtoll(ptr, &eptr, 10); break; \
        case STR2DOUBLE: val = strtod(ptr, &eptr); break; \
        } \
        if (errno == ERANGE || eptr == ptr || *eptr != '\0') { \
            str2numerror = true; \
            val = 0; \
        } \
    } while (0)

// if str2l/ll/f convert error, return true
// this func is thread-safe
bool str2numerr()
{
    return str2numerror;
}

long str2l(const std::string& nstr)
{
    long lval;
    str2number(nstr.c_str(), lval, STR2LONG);
    return lval;
}

long long str2ll(const std::string& nstr)
{
    long long llval;
    str2number(nstr.c_str(), llval, STR2LLONG);
    return llval;
}

double str2f(const std::string& nstr)
{
    double dval;
    str2number(nstr.c_str(), dval, STR2DOUBLE);
    return dval;
}

// 分割一个以\n结尾的字符串，将结果保存在argv中
// 带空白符的字符串必须包含在双引号内
// set key value -> [set] [key] [value]
// set key "hello, wrold" -> [set] [key] [hello, world]
// if this line is all whitespace or parse error, return -1; else return 0
int parse_line(argv_t& argv, const char *line, const char *linep)
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

// [set] [key] [value] -> <set key value>
std::string argv2str(const argv_t& argv)
{
    std::string s = "<";
    for (auto& it : argv)
        s.append(it).append(" ");
    if (s.size() > 1)
        s.pop_back();
    s.append(">");
    return s;
}

// if c == ','
// for [a,b,c,d] return [a][b][c][d]
void split_line(std::vector<std::string>& argv,
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

int fwrite(int fd, const char *buf, size_t nbytes)
{
    while (nbytes > 0) {
        ssize_t n = ::write(fd, buf, nbytes);
        if (n < 0) return -1;
        nbytes -= n;
        buf += n;
    }
    return 0;
}

void parse_conf(conf_param_list& paramlist, const char *filename)
{
    char buf[1024];
    FILE *fp = fopen(filename, "r");
    if (!fp) {
        fprintf(stderr, "open %s error: %s", filename, angel::util::strerrno());
        abort();
    }
    while (fgets(buf, sizeof(buf), fp)) {
        const char *s = buf;
        const char *es = buf + strlen(buf);
        std::vector<std::string> param;
        s = std::find_if(s, es, [](char c){ return !isspace(c); });
        if (s == es || s[0] == '#') continue;
next:
        const char *p = std::find(s, es, ':');
        if (p == es)  {
            p = std::find_if(s, es, isspace);
            if (p == es) continue;
            param.emplace_back(s, p);
            paramlist.emplace_back(std::move(param));
            continue;
        }
        param.emplace_back(s, p);
        s = p + 1;
        goto next;
    }
    fclose(fp);
}

off_t get_filesize(int fd)
{
    struct stat st;
    fstat(fd, &st);
    return st.st_size;
}

bool is_file_exists(const std::string& filename)
{
    FILE *fp = fopen(filename.c_str(), "r");
    if (fp) {
        fclose(fp);
        return true;
    }
    return false;
}

ssize_t get_proc_memory()
{
#if defined (__APPLE__)
    task_t task;
    struct task_basic_info tbi;
    mach_msg_type_number_t count = TASK_BASIC_INFO_COUNT;
    if (task_for_pid(mach_task_self(), ::getpid(), &task) != KERN_SUCCESS)
        return -1;
    task_info(task, TASK_BASIC_INFO, (task_info_t)&tbi, &count);
    return tbi.resident_size;
#elif defined (__linux__)
    char filename[32] = { 0 };
    char buf[1024] = { 0 };
    int vmrss = -1;
    snprintf(filename, sizeof(filename), "/proc/%d/status", ::getpid());
    FILE *fp = fopen(filename, "r");
    if (fp == nullptr) return -1;
    while (fgets(buf, sizeof(buf), fp)) {
        if (strncasecmp(buf, "vmrss:", 6) == 0) {
            sscanf(buf, "%*s %d", &vmrss);
            vmrss *= 1024;
            break;
        }
    }
    fclose(fp);
    return vmrss;
#else
    assert(0);
    return -1;
#endif
}

}
