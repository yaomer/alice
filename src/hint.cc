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
    { "setnx",      5,  " key value" },
    { "set",        3,  " key value [EX seconds|PX milliseconds]" },
    { "getset",     6,  " key value" },
    { "get",        3,  " key" },
    { "append",     6,  " key value" },
    { "strlen",     6,  " key" },
    { "mset",       4,  " key value [key value ...]" },
    { "mget",       4,  " key [key ...]" },
    { "incrby",     6,  " key increment" },
    { "incr",       4,  " key" },
    { "decrby",     6,  " key decrement" },
    { "decr",       4,  " key" },
    { "lpushx",     6,  " key value" },
    { "lpush",      5,  " key value [value ...]" },
    { "rpushx",     6,  " key value" },
    { "rpush",      5,  " key value [value ...]" },
    { "lpop",       4,  " key" },
    { "rpoplpush",  9,  " source destination" },
    { "rpop",       4,  " key" },
    { "lrem",       4,  " key count value"},
    { "llen",       4,  " key" },
    { "lindex",     6,  " key index"},
    { "lset",       4,  " key index value" },
    { "lrange",     6,  " key start stop" },
    { "ltrim",      5,  " key start stop" },
    { "sadd",       4,  " key member [member ...]" },
    { "srandmember",11, " key [count]" },
    { "smembers",   8,  " key" },
    { "exists",     6,  " key" },
    { "type",       4,  " key" },
    { "ttl",        3,  " key" },
    { "pttl",       4,  " key" },
    { "expire",     6,  " key seconds" },
    { "pexpire",    7,  " key milliseconds" },
    { "del",        3,  " key [key ...]" },
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
