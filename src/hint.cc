#include <string.h>
#include <linenoise.h>
#include <ctype.h>
#include <algorithm>
#include <set>
#include <string>

typedef struct {
    const char name[32];
    int len;
    const char info[256];
} HintInfo;

#define HTSIZE 1024

HintInfo hiTable[HTSIZE] = {
    { "SETNX",      5,  " key value" },
    { "SET",        3,  " key value [EX seconds|PX milliseconds] [NX|XX]" },
    { "GETSET",     6,  " key value" },
    { "GET",        3,  " key" },
    { "APPEND",     6,  " key value" },
    { "STRLEN",     6,  " key" },
    { "MSET",       4,  " key value [key value ...]" },
    { "MGET",       4,  " key [key ...]" },
    { "INCRBY",     6,  " key increment" },
    { "INCR",       4,  " key" },
    { "DECRBY",     6,  " key decrement" },
    { "DECR",       4,  " key" },
    { "LPUSHX",     6,  " key value" },
    { "LPUSH",      5,  " key value [value ...]" },
    { "RPUSHX",     6,  " key value" },
    { "RPUSH",      5,  " key value [value ...]" },
    { "LPOP",       4,  " key" },
    { "RPOPLPUSH",  9,  " source destination" },
    { "RPOP",       4,  " key" },
    { "LREM",       4,  " key count value"},
    { "LLEN",       4,  " key" },
    { "LINDEX",     6,  " key index"},
    { "LSET",       4,  " key index value" },
    { "LRANGE",     6,  " key start stop" },
    { "LTRIM",      5,  " key start stop" },
    { "SADD",       4,  " key member [member ...]" },
    { "SISMEMBER",  9,  " key member" },
    { "SPOP",       4,  " key" },
    { "SRANDMEMBER",11, " key [count]" },
    { "SREM",       4,  " key member [member ...]" },
    { "SMOVE",      5,  " source destination member" },
    { "SCARD",      5,  " key" },
    { "SMEMBERS",   8,  " key" },
    { "SINTERSTORE",11, " destination key [key ...]" },
    { "SINTER",     6,  " key [key ...]" },
    { "SUNIONSTORE",11, " destination key [key ...]" },
    { "SUNION",     6,  " key [key ...]" },
    { "SDIFFSTORE", 10, " destination key [key ...]" },
    { "SDIFF",      5,  " key [key ...]" },
    { "HSETNX",     6,  " key field value" },
    { "HSET",       4,  " key field value" },
    { "HGETALL",    7,  " key" },
    { "HGET",       4,  " key field" },
    { "HEXISTS",    7,  " key field" },
    { "HDEL",       4,  " key field [field ...]" },
    { "HLEN",       4,  " key" },
    { "HSTRLEN",    7,  " key field" },
    { "HINCRBY",    7,  " key field increment" },
    { "HMSET",      5,  " key field value [field value ...]" },
    { "HMGET",      5,  " key field [field ...]" },
    { "HKEYS",      5,  " key" },
    { "HVALS",      5,  " key" },
    { "EXISTS",     6,  " key" },
    { "TYPE",       4,  " key" },
    { "TTL",        3,  " key" },
    { "PTTL",       4,  " key" },
    { "EXPIRE",     6,  " key seconds" },
    { "PEXPIRE",    7,  " key milliseconds" },
    { "DEL",        3,  " key [key ...]" },
    { "KEYS",       4,  " pattern" },
    { "SAVE",       4,  "" },
    { "BGSAVE",     6,  "" },
    { "BGREWRITEAOF", 12, "" },
    { "LASTSAVE",   8,  "" },
    { "FLUSHDB",    7,  "" },
    { "SLAVEOF",    7,  " host port" },
    { "", 0, "" },
};

std::set<std::string> cmdnames;
bool isInitCmdnames = false;

void completion(const char *buf, linenoiseCompletions *lc)
{
    if (!isInitCmdnames) {
        for (int i = 0; hiTable[i].name[0]; i++)
            cmdnames.insert(hiTable[i].name);
        isInitCmdnames = true;
    }
    size_t len = strlen(buf);
    for (auto& it : cmdnames) {
        if (strncasecmp(buf, it.c_str(), len) == 0)
            linenoiseAddCompletion(lc, it.c_str());
    }
}

char *hints(const char *buf, int *color, int *bold)
{
    for (int i = 0; hiTable[i].name[0]; i++) {
        const char *p = buf;
        size_t buflen = strlen(buf);
        const char *ep = buf + buflen;
        const char *ip = hiTable[i].info;
        const char *iep = ip + strlen(hiTable[i].info);
        const char *next = nullptr;
        while (*p == ' ' || *p == '\t') {
            p++;
            buflen--;
        }
        if (strncasecmp(p, hiTable[i].name, hiTable[i].len) == 0) {
            if (hiTable[i].len == buflen)
                goto ret;
            if (!ip[0]) return nullptr;
            if (!isspace(p[hiTable[i].len]))
                return nullptr;
            p += hiTable[i].len;
            while (true) {
                p = std::find_if(p, ep, [](char c){ return !isspace(c); });
                if (p == ep) break;
                if (ip[1] == '[') break;
                next = std::find_if(ip + 1, iep, isspace);
                if (next == iep) break;
                ip = next;
                p = std::find_if(p, ep, isspace);
                if (p == ep) break;
            }
ret:
            *color = 36;
            *bold = 0;
            return const_cast<char*>(ip);
        }
    }
    return nullptr;
}
