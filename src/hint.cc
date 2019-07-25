#include <string.h>
#include <linenoise.h>

typedef struct {
    const char *name;
    int len;
    const char *info;
} HintInfo;

#define HTSIZE 12

HintInfo hiTable[HTSIZE] = {
    { "setnx",  5,  " [key] [value] [:if key not exists]" },
    { "set",    3,  " [key] [value] [EX seconds|PX milliseconds]" },
    { "getset", 6,  " [key] [value] [:get oldvalue, set newvalue]" },
    { "get",    3,  " [key]" },
    { "append", 6,  " [key] [value] [:oldvalue + value]" },
    { "strlen", 6,  " [key]" },
    { "mset",   4,  " [key1 value1] [key2 value2] ..." },
    { "mget",   4,  " [key1] [key2] ..." },
    { "incrby", 6,  " [key] [increment]" },
    { "incr",   4,  " [key]" },
    { "decrby", 6,  " [key] [decrement]" },
    { "decr",   4,  " [key]" },
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

