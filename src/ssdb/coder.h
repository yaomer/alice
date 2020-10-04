#ifndef _ALICE_SRC_SSDB_CODER_H
#define _ALICE_SRC_SSDB_CODER_H

#include <string>

#include "../util.h"

namespace alice {

namespace ssdb {

struct ktype {
    static const char meta     = '@';
    static const char tstring  = 's';
    static const char tlist    = 'l';
    static const char thash    = 'h';
    static const char tset     = 'S';
    static const char tzset    = 'z';
};

static inline const char
get_type(const std::string& value)
{
    return value[0];
}

static inline std::string
encode_meta_key(const std::string& key)
{
    std::string buf;
    buf.append(1, ktype::meta);
    buf.append(key);
    return buf;
}

static inline std::string
encode_list_meta_value(int li, int ri, int size)
{
    std::string buf;
    buf.append(1, ktype::tlist);
    buf.append(i2s(li));
    buf.append(1, ':');
    buf.append(i2s(ri));
    buf.append(1, ':');
    buf.append(i2s(size));
    return buf;
}

static inline void
decode_list_meta_value(const std::string& value, int& li, int& ri, int& size)
{
    const char *s = value.c_str() + 1;
    li = atoi(s);
    ri = atoi(strchr(s, ':') + 1);
    size = atoi(strrchr(s, ':') + 1);
}

static inline std::string
encode_list_key(const std::string& key, int number)
{
    std::string buf;
    buf.append(1, ktype::tlist);
    buf.append(key);
    buf.append(1, ':');
    buf.append(i2s(number));
    return buf;
}

static inline std::string
encode_string_meta_value()
{
    std::string buf;
    buf.append(1, ktype::tstring);
    return buf;
}

static inline std::string
encode_string_key(const std::string& key)
{
    std::string buf;
    buf.append(1, ktype::tstring);
    buf.append(key);
    return buf;
}

}
}

#endif // _ALICE_SRC_SSDB_CODER_H
