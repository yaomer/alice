#include <string.h>
#include <linenoise.h>

typedef struct {
    const char *name;
    int len;
    const char *info;
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
    { "lpush",      5,  " key value [value ...]" },
    { "lpushx",     6,  " key value" },
    { "rpush",      5,  " key value [value ...]" },
    { "rpushx",     6,  " key value" },
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
};

void completion(const char *buf, linenoiseCompletions *lc)
{
    switch (buf[0]) {
    case 'a':
        linenoiseAddCompletion(lc, "append");
        break;
    case 'd':
        linenoiseAddCompletion(lc, "decr");
        linenoiseAddCompletion(lc, "decrby");
        break;
    case 'g':
        linenoiseAddCompletion(lc, "get");
        linenoiseAddCompletion(lc, "getset");
        break;
    case 'i':
        linenoiseAddCompletion(lc, "incr");
        linenoiseAddCompletion(lc, "incrby");
        break;
    case 'm':
        linenoiseAddCompletion(lc, "mset");
        linenoiseAddCompletion(lc, "mget");
        break;
    case 's':
        linenoiseAddCompletion(lc, "set");
        linenoiseAddCompletion(lc, "setnx");
        linenoiseAddCompletion(lc, "strlen");
        break;
    }
}

char *hints(const char *buf, int *color, int *bold)
{
    for (int i = 0; i < HTSIZE; i++) {
        if (strncasecmp(buf, hiTable[i].name, hiTable[i].len) == 0) {
            *color = 36;
            *bold = 0;
            return const_cast<char*>(hiTable[i].info);
        }
    }
    return nullptr;
}

