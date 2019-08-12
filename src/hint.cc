#include <string.h>
#include <linenoise.h>
#include <ctype.h>
#include <algorithm>
#include <set>
#include <string>

typedef struct {
    const char name[32];
    int len;
    int fields;
    const char info[256];
} HintInfo;

#define HTSIZE 1024

HintInfo hiTable[HTSIZE] = {
    { "SETNX",       5, 0, " key value" },
    { "SET",         3, 3, " key value [EX seconds|PX milliseconds] [NX|XX]" },
    { "GETSET",      6, 0, " key value" },
    { "GET",         3, 0, " key" },
    { "APPEND",      6, 0, " key value" },
    { "STRLEN",      6, 0, " key" },
    { "MSET",        4, 3, " key value [key value ...]" },
    { "MGET",        4, 2, " key [key ...]" },
    { "INCRBY",      6, 0, " key increment" },
    { "INCR",        4, 0, " key" },
    { "DECRBY",      6, 0, " key decrement" },
    { "DECR",        4, 0, " key" },
    { "LPUSHX",      6, 0, " key value" },
    { "LPUSH",       5, 3, " key value [value ...]" },
    { "RPUSHX",      6, 0, " key value" },
    { "RPUSH",       5, 3, " key value [value ...]" },
    { "LPOP",        4, 0, " key" },
    { "RPOPLPUSH",   9, 0, " source destination" },
    { "RPOP",        4, 0, " key" },
    { "LREM",        4, 0, " key count value"},
    { "LLEN",        4, 0, " key" },
    { "LINDEX",      6, 0, " key index"},
    { "LSET",        4, 0, " key index value" },
    { "LRANGE",      6, 0, " key start stop" },
    { "LTRIM",       5, 0, " key start stop" },
    { "SADD",        4, 3, " key member [member ...]" },
    { "SISMEMBER",   9, 0, " key member" },
    { "SPOP",        4, 0, " key" },
    { "SRANDMEMBER",11, 2, " key [count]" },
    { "SREM",        4, 3, " key member [member ...]" },
    { "SMOVE",       5, 0, " source destination member" },
    { "SCARD",       5, 0, " key" },
    { "SMEMBERS",    8, 0, " key" },
    { "SINTERSTORE",11, 3, " destination key [key ...]" },
    { "SINTER",      6, 2, " key [key ...]" },
    { "SUNIONSTORE",11, 3, " destination key [key ...]" },
    { "SUNION",      6, 2, " key [key ...]" },
    { "SDIFFSTORE", 10, 3, " destination key [key ...]" },
    { "SDIFF",       5, 2, " key [key ...]" },
    { "HSETNX",      6, 0, " key field value" },
    { "HSET",        4, 0, " key field value" },
    { "HGETALL",     7, 0, " key" },
    { "HGET",        4, 0, " key field" },
    { "HEXISTS",     7, 0, " key field" },
    { "HDEL",        4, 3, " key field [field ...]" },
    { "HLEN",        4, 0, " key" },
    { "HSTRLEN",     7, 0, " key field" },
    { "HINCRBY",     7, 0, " key field increment" },
    { "HMSET",       5, 4, " key field value [field value ...]" },
    { "HMGET",       5, 3, " key field [field ...]" },
    { "HKEYS",       5, 0, " key" },
    { "HVALS",       5, 0, " key" },
    { "EXISTS",      6, 0, " key" },
    { "TYPE",        4, 0, " key" },
    { "TTL",         3, 0, " key" },
    { "PTTL",        4, 0, " key" },
    { "EXPIRE",      6, 0, " key seconds" },
    { "PEXPIRE",     7, 0, " key milliseconds" },
    { "DEL",         3, 2, " key [key ...]" },
    { "KEYS",        4, 0, " pattern" },
    { "SAVE",        4, 0, "" },
    { "BGSAVE",      6, 0, "" },
    { "BGREWRITEAOF",12,0, "" },
    { "LASTSAVE",    8, 0, "" },
    { "FLUSHDB",     7, 0, "" },
    { "SLAVEOF",     7, 0, " host port" },
    { "MULTI",       5, 0, "" },
    { "EXEC",        4, 0, "" },
    { "DISCARD",     7, 0, "" },
    { "WATCH",       5, 2, " key [key ...]" },
    { "UNWATCH",     7, 0, "" },
    { "PUBLISH",     7, 0, " channel message" },
    { "SUBSCRIBE",   9, 2, " channel [channel ...]" },
    { "", 0, 0, "" },
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
        int fields = 1;
        while (*p == ' ' || *p == '\t') {
            p++;
            buflen--;
        }
        if (strncasecmp(p, hiTable[i].name, hiTable[i].len) == 0) {
            *color = 36;
            *bold = 0;
            if (hiTable[i].len == buflen) return const_cast<char*>(ip);
            if (!ip[0] || !isspace(p[hiTable[i].len])) return nullptr;
            p += hiTable[i].len;
            while (true) {
                p = std::find_if(p, ep, [](char c){ return !isspace(c); });
                if (p == ep) return const_cast<char*>(ip + 1);
                if (fields < hiTable[i].fields) {
                    if (ip[1] == '[') break;
                    next = std::find_if(ip + 1, iep, isspace);
                    if (next == iep) break;
                    ip = next;
                }
                p = std::find_if(p, ep, isspace);
                if (p == ep) break;
                fields++;
            }
            return const_cast<char*>(ip);
        }
    }
    return nullptr;
}
