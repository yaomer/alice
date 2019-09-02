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

// 在hiTable中，若cmdstr1是cmdstr2的前缀，则cmdstr2必须
// 放在cmdstr1前面（这是hints()实现导致的缺陷，但我暂时并不想去修复它)
// e.g: SETNX必须放在SET前面，否则将无法提示SETNX
HintInfo hiTable[HTSIZE] = {
    { "SET",         3, 3, " key value [EX seconds|PX milliseconds] [NX|XX]" },
    { "SETNX",       5, 2, " key value" },
    { "GETSET",      6, 2, " key value" },
    { "GET",         3, 1, " key" },
    { "APPEND",      6, 2, " key value" },
    { "STRLEN",      6, 1, " key" },
    { "MSET",        4, 3, " key value [key value ...]" },
    { "MGET",        4, 2, " key [key ...]" },
    { "INCRBY",      6, 2, " key increment" },
    { "INCR",        4, 1, " key" },
    { "DECRBY",      6, 2, " key decrement" },
    { "DECR",        4, 1, " key" },
    { "LPUSHX",      6, 2, " key value" },
    { "LPUSH",       5, 3, " key value [value ...]" },
    { "RPUSHX",      6, 2, " key value" },
    { "RPUSH",       5, 3, " key value [value ...]" },
    { "LPOP",        4, 1, " key" },
    { "RPOPLPUSH",   9, 2, " source destination" },
    { "RPOP",        4, 1, " key" },
    { "LREM",        4, 3, " key count value"},
    { "LLEN",        4, 1, " key" },
    { "LINDEX",      6, 2, " key index"},
    { "LSET",        4, 3, " key index value" },
    { "LRANGE",      6, 3, " key start stop" },
    { "LTRIM",       5, 3, " key start stop" },
    { "BLPOP",       5, 2, " key [key ...] timeout" },
    { "BRPOPLPUSH", 10, 3, " source destination timeout" },
    { "BRPOP",       5, 2, " key [key ...] timeout" },
    { "SADD",        4, 3, " key member [member ...]" },
    { "SISMEMBER",   9, 2, " key member" },
    { "SPOP",        4, 1, " key" },
    { "SRANDMEMBER",11, 2, " key [count]" },
    { "SREM",        4, 3, " key member [member ...]" },
    { "SMOVE",       5, 3, " source destination member" },
    { "SCARD",       5, 1, " key" },
    { "SMEMBERS",    8, 1, " key" },
    { "SINTERSTORE",11, 3, " destination key [key ...]" },
    { "SINTER",      6, 2, " key [key ...]" },
    { "SUNIONSTORE",11, 3, " destination key [key ...]" },
    { "SUNION",      6, 2, " key [key ...]" },
    { "SDIFFSTORE", 10, 3, " destination key [key ...]" },
    { "SDIFF",       5, 2, " key [key ...]" },
    { "HSETNX",      6, 3, " key field value" },
    { "HSET",        4, 3, " key field value" },
    { "HGETALL",     7, 1, " key" },
    { "HGET",        4, 2, " key field" },
    { "HEXISTS",     7, 2, " key field" },
    { "HDEL",        4, 3, " key field [field ...]" },
    { "HLEN",        4, 1, " key" },
    { "HSTRLEN",     7, 2, " key field" },
    { "HINCRBY",     7, 3, " key field increment" },
    { "HMSET",       5, 4, " key field value [field value ...]" },
    { "HMGET",       5, 3, " key field [field ...]" },
    { "HKEYS",       5, 1, " key" },
    { "HVALS",       5, 1, " key" },
    { "ZADD",        4, 4, " key score member [score member ...]" },
    { "ZSCORE",      6, 2, " key member" },
    { "ZINCRBY",     7, 3, " key increment member" },
    { "ZCARD",       5, 1, " key" },
    { "ZCOUNT",      6, 3, " key min max" },
    { "ZRANGEBYSCORE",      13, 4, " key min max [WITHSCORES] [LIMIT offset count]" },
    { "ZREVRANGEBYSCORE",   16, 4, " key min max [WITHSCORES] [LIMIT offset count]" },
    { "ZREMRANGEBYRANK",    15, 3, " key start stop" },
    { "ZREMRANGEBYSCORE",   16, 3, " key min max" },
    { "ZRANGE",      6, 4, " key start stop [WITHSCORES]" },
    { "ZREVRANGE",   9, 4, " key start stop [WITHSCORES]" },
    { "ZRANK",       5, 2, " key member" },
    { "ZREVRANK",    8, 2, " key member" },
    { "ZREM",        4, 2, " key member [member ...]" },
    { "EXISTS",      6, 1, " key" },
    { "TYPE",        4, 1, " key" },
    { "TTL",         3, 1, " key" },
    { "PTTL",        4, 1, " key" },
    { "EXPIRE",      6, 2, " key seconds" },
    { "PEXPIRE",     7, 2, " key milliseconds" },
    { "DEL",         3, 2, " key [key ...]" },
    { "KEYS",        4, 1, " pattern" },
    { "SAVE",        4, 0, "" },
    { "BGSAVE",      6, 0, "" },
    { "BGREWRITEAOF",12,0, "" },
    { "LASTSAVE",    8, 0, "" },
    { "FLUSHDB",     7, 0, "" },
    { "FLUSHALL",    8, 0, "" },
    { "SLAVEOF",     7, 2, " host port" },
    { "PING",        4, 0, "" },
    { "MULTI",       5, 0, "" },
    { "EXEC",        4, 0, "" },
    { "DISCARD",     7, 0, "" },
    { "WATCH",       5, 2, " key [key ...]" },
    { "UNWATCH",     7, 0, "" },
    { "PUBLISH",     7, 2, " channel message" },
    { "SUBSCRIBE",   9, 2, " channel [channel ...]" },
    { "INFO",        4, 0, "" },
    { "SELECT",      6, 1, " index" },
    { "DBSIZE",      6, 0, "" },
    { "SORT",        4, 2, " key [BY pattern] [LIMIT offset count] [GET pattern [GET pattern ...]]"
                           " [ASC|DESC] [ALPHA] [STORE destination]" },
    { "RENAMENX",    8, 2, " key newkey" },
    { "RENAME",      6, 2, " key newkey" },
    { "MOVE",        4, 2, " key db" },
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
