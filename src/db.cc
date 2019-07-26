#include <stdlib.h>
#include <map>
#include <vector>
#include <list>
#include <functional>
#include "db.h"
#include "server.h"

using namespace Alice;
using std::placeholders::_1;

DB::DB()
{
    _commandMap = {
        { "SET",        { "SET", 3, true, std::bind(&DB::strSet, this, _1) } },
        { "SETNX",      { "SETNX", -3, true, std::bind(&DB::strSetIfNotExist, this, _1) } },
        { "GET",        { "GET", -2, false, std::bind(&DB::strGet, this, _1) } },
        { "GETSET",     { "GETSET", -3, true, std::bind(&DB::strGetSet, this, _1) } },
        { "APPEND",     { "APPEND", -3, true, std::bind(&DB::strAppend, this, _1) } },
        { "STRLEN",     { "STRLEN", -2, false, std::bind(&DB::strLen, this, _1) } },
        { "MSET",       { "MSET", 3, true, std::bind(&DB::strMset, this, _1) } },
        { "MGET",       { "MGET", 2, false, std::bind(&DB::strMget, this, _1) } },
        { "INCR",       { "INCR", -2, true, std::bind(&DB::strIncr, this, _1) } },
        { "INCRBY",     { "INCRBY", -3, true, std::bind(&DB::strIncrBy, this, _1) } },
        { "DECR",       { "DECR", -2, true, std::bind(&DB::strDecr, this, _1) } },
        { "DECRBY",     { "DECRBY", -3, true, std::bind(&DB::strDecrBy, this, _1) } },
        { "LPUSH",      { "LPUSH", 3, true, std::bind(&DB::listLeftPush, this, _1) } },
        { "LPUSHX",     { "LPUSHX", -3, true, std::bind(&DB::listHeadPush, this, _1) } },
        { "RPUSH",      { "RPUSH", 3, true, std::bind(&DB::listRightPush, this, _1) } },
        { "RPUSHX",     { "RPUSHX", -3, true, std::bind(&DB::listTailPush, this, _1) } },
        { "LPOP",       { "LPOP", -2, true, std::bind(&DB::listLeftPop, this, _1) } },
        { "RPOP",       { "RPOP", -2, true, std::bind(&DB::listRightPop, this, _1) } },
        { "RPOPLPUSH",  { "RPOPLPUSH", -3, true, std::bind(&DB::listRightPopLeftPush, this, _1) } },
        { "LREM",       { "LREM", -4, true, std::bind(&DB::listRem, this, _1) } },
        { "LLEN",       { "LLEN", -2, false, std::bind(&DB::listLen, this, _1) } },
        { "LINDEX",     { "LINDEX", -3, false, std::bind(&DB::listIndex, this, _1) } },
        { "LSET",       { "LSET", -4, true, std::bind(&DB::listSet, this, _1) } },
        { "LRANGE",     { "LRANGE", -4, false, std::bind(&DB::listRange, this, _1) } },
        { "LTRIM",      { "LTRIM", -4, true, std::bind(&DB::listTrim, this, _1) } },
        { "SADD",       { "SADD", 3, true, std::bind(&DB::setAdd, this, _1) } },
        { "SISMEMBER",  { "SISMEMBER", -3, false, std::bind(&DB::setIsMember, this, _1) } },
        { "SPOP",       { "SPOP", -2, true, std::bind(&DB::setPop, this, _1) } },
        { "SRANDMEMBER",{ "SRANDMEMBER", 2, false, std::bind(&DB::setRandMember, this, _1) } },
        { "SREM",       { "SREM", 3, true, std::bind(&DB::setRem, this, _1) } },
        { "SMOVE",      { "SMOVE", -4, true, std::bind(&DB::setMove, this, _1) } },
        { "SCARD",      { "SCARD", -2, false, std::bind(&DB::setCard, this, _1) } },
        { "SMEMBERS",   { "SMEMBERS", -2, false, std::bind(&DB::setMembers, this, _1) } },
        { "SINTER",     { "SINTER", 2, false, std::bind(&DB::setInter, this, _1) } },
        { "SINTERSTORE",{ "SINTERSTORE", 3, true, std::bind(&DB::setInterStore, this, _1) } },
        { "SUNION",     { "SUNION", 2, false, std::bind(&DB::setUnion, this, _1) } },
        { "SUNIONSTORE",{ "SUNIONSTORE", 3, true, std::bind(&DB::setUnionStore, this, _1) } },
        { "SDIFF",      { "SDIFF", 2, false, std::bind(&DB::setDiff, this, _1) } },
        { "SDIFFSTORE", { "SDIFFSTORE", 3, true, std::bind(&DB::setDiffStore, this, _1) } },
        { "EXISTS",     { "EXISTS", -2, false, std::bind(&DB::isKeyExists, this, _1) } },
        { "TYPE",       { "TYPE", -2, false, std::bind(&DB::getKeyType, this, _1) } },
        { "TTL",        { "TTL", -2, false, std::bind(&DB::getTtlSecs, this, _1) } },
        { "PTTL",       { "PTTL", -2, false, std::bind(&DB::getTtlMils, this, _1) } },
        { "EXPIRE",     { "EXPIRE", -3, false, std::bind(&DB::setKeyExpireSecs, this, _1) } },
        { "PEXPIRE",    { "PEXPIRE", -3, false, std::bind(&DB::setKeyExpireMils, this, _1) } },
    };
}

namespace Alice {

    thread_local char convert_buf[32];

    const char *convert(int64_t value)
    {
        snprintf(convert_buf, sizeof(convert_buf), "%lld", value);
        return convert_buf;
    }

    static const char *db_return_ok = "+OK\r\n";
    static const char *db_return_err = "-ERR\r\n";
    static const char *db_return_nil = "+(nil)\r\n";
    static const char *db_return_integer_0 = ": 0\r\n";
    static const char *db_return_integer_1 = ": 1\r\n";
    static const char *db_return_integer__1 = ": -1\r\n";
    static const char *db_return_integer__2 = ": -2\r\n";
    static const char *db_return_type_err = "-WRONGTYPE Operation"
        " against a key holding the wrong kind of value\r\n";
    static const char *db_return_interger_err = "-ERR value is"
        " not an integer or out of range";
    static const char *db_return_no_such_key = "-ERR no such key\r\n";
    static const char *db_return_out_of_range = "-ERR index out of range\r\n";
    static const char *db_return_string_type = "+string\r\n";
    static const char *db_return_list_type = "+list\r\n";
    static const char *db_return_set_type = "+set\r\n";
    static const char *db_return_none_type = "+none\r\n";
}

//////////////////////////////////////////////////////////////////

#define isXXType(it, _type) \
    ((it)->second.value().type() == typeid(_type))
#define checkType(con, it, _type) \
    do { \
        if (!isXXType(it, _type)) { \
            (con).append(db_return_type_err); \
            return; \
        } \
    } while (0)
#define getXXType(it, _type) \
    (std::any_cast<_type>((it)->second.value()))

//////////////////////////////////////////////////////////////////

void DB::isKeyExists(Context& con)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
    auto it = _hashMap.find(cmdlist[1]);
    if (it != _hashMap.end()) {
        con.append(db_return_integer_1);
    } else {
        con.append(db_return_integer_0);
    }
}

void DB::getKeyType(Context& con)
{
    auto& cmdlist = con.commandList();
    auto it = _hashMap.find(cmdlist[1]);
    if (it == _hashMap.end()) {
        con.append(db_return_none_type);
        return;
    }
    if (isXXType(it, String))
        con.append(db_return_string_type);
    else if (isXXType(it, List))
        con.append(db_return_list_type);
    else if (isXXType(it, Set))
        con.append(db_return_set_type);
}

void DB::getTtlSecs(Context& con)
{
    _getTtl(con, true);
}

void DB::getTtlMils(Context& con)
{
    _getTtl(con, false);
}

void DB::_getTtl(Context& con, bool seconds)
{
    auto& cmdlist = con.commandList();
    auto it = _hashMap.find(cmdlist[1]);
    if (it == _hashMap.end()) {
        con.append(db_return_integer__2);
        return;
    }
    auto expire = con.db()->expireMap().find(cmdlist[1]);
    if (expire == con.db()->expireMap().end()) {
        con.append(db_return_integer__1);
        return;
    }
    int64_t milliseconds = expire->second - Angel::TimeStamp::now();
    if (seconds)
        milliseconds /= 1000;
    const char *s = convert(milliseconds);
    con.append(": ");
    con.append(s);
    con.append("\r\n");
}

void DB::setKeyExpireSecs(Context& con)
{
    _setKeyExpire(con, true);
}

void DB::setKeyExpireMils(Context& con)
{
    _setKeyExpire(con, false);
}

void DB::_setKeyExpire(Context& con, bool seconds)
{
    auto& cmdlist = con.commandList();
    auto it = _hashMap.find(cmdlist[1]);
    if (it != _hashMap.end()) {
        int64_t expire = atol(cmdlist[2].c_str());
        if (seconds)
            expire *= 1000;
        con.db()->addExpireKey(cmdlist[1], expire);
        con.append(db_return_integer_1);
    } else {
        con.append(db_return_integer_0);
    }
}

//////////////////////////////////////////////////////////////////
// String Keys Operation
//////////////////////////////////////////////////////////////////

#define getStringValue(it) getXXType(it, String&)

void DB::strSet(Context& con)
{
    auto& cmdlist = con.commandList();
    int64_t expire;
    con.db()->isExpiredKey(cmdlist[1]);
    if (cmdlist.size() > 3) {
        std::transform(cmdlist[3].begin(), cmdlist[3].end(), cmdlist[3].begin(), ::toupper);
        expire = atoll(cmdlist[4].c_str());
        if (cmdlist[3].compare("EX") == 0)
            expire *= 1000;
        else if (cmdlist[3].compare("PX")) {
            con.append(db_return_err);
            return;
        }
    }
    _hashMap[cmdlist[1]] = cmdlist[2];
    if (cmdlist.size() > 3) {
        auto it = con.db()->expireMap().find(cmdlist[1]);
        if (it != con.db()->expireMap().end())
            con.db()->delExpireKey(cmdlist[1]);
        con.db()->addExpireKey(cmdlist[1], expire);
    }
    con.append(db_return_ok);
}

void DB::strSetIfNotExist(Context& con)
{
    auto& cmdlist = con.commandList();
    auto it = _hashMap.find(cmdlist[1]);
    if (it != _hashMap.end()) {
        con.append(db_return_integer_0);
    } else {
        _hashMap[cmdlist[1]] = cmdlist[2];
        con.append(db_return_integer_1);
    }
}

void DB::strGet(Context& con)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
    auto it = _hashMap.find(cmdlist[1]);
    if (it == _hashMap.end()) {
        con.append(db_return_nil);
        return;
    }
    checkType(con, it, String);
    auto& value = getStringValue(it);
    con.append("$");
    con.append(convert(value.size()));
    con.append("\r\n");
    con.append(value);
    con.append("\r\n");
}

void DB::strGetSet(Context& con)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
    auto it = _hashMap.find(cmdlist[1]);
    if (it == _hashMap.end()) {
        _hashMap[cmdlist[1]] = cmdlist[2];
        con.append(db_return_nil);
        return;
    }
    checkType(con, it, String);
    String oldvalue = getStringValue(it);
    _hashMap[cmdlist[1]] = cmdlist[2];
    con.append("+");
    con.append(oldvalue);
    con.append("\r\n");
}

void DB::strLen(Context& con)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
    auto it = _hashMap.find(cmdlist[1]);
    if (it == _hashMap.end()) {
        con.append(db_return_integer_0);
        return;
    }
    checkType(con, it, String);
    auto& value = getStringValue(it);
    const char *len = convert(value.size());
    con.append(": ");
    con.append(len);
    con.append("\r\n");
}

void DB::strAppend(Context& con)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
    auto it = _hashMap.find(cmdlist[1]);
    if (it == _hashMap.end()) {
        _hashMap[cmdlist[1]] = cmdlist[2];
        con.append(": ");
        con.append(convert(cmdlist[2].size()));
        con.append("\r\n");
    }
    checkType(con, it, String);
    String& oldvalue = getStringValue(it);
    oldvalue += cmdlist[2];
    _hashMap[cmdlist[1]] = oldvalue;
    con.append(": ");
    con.append(convert(oldvalue.size()));
    con.append("\r\n");
}

void DB::strMset(Context& con)
{
    auto& cmdlist = con.commandList();
    size_t size = cmdlist.size();
    if (size % 2 == 0) {
        con.append("-ERR wrong number of arguments for '" + cmdlist[0] + "'\r\n");
        return;
    }
    for (int i = 1; i < size; i += 2) {
        con.db()->isExpiredKey(cmdlist[i]);
        _hashMap[cmdlist[i]] = cmdlist[i + 1];
    }
    con.append(db_return_ok);
}

void DB::strMget(Context& con)
{
    auto& cmdlist = con.commandList();
    size_t size = cmdlist.size();
    con.append("*");
    con.append(convert(size - 1));
    con.append("\r\n");
    for (int i = 1; i < size; i++) {
        con.db()->isExpiredKey(cmdlist[i]);
        auto it = _hashMap.find(cmdlist[i]);
        if (it != _hashMap.end()) {
            checkType(con, it, String);
            String& value = getStringValue(it);
            con.append("$");
            con.append(convert(value.size()));
            con.append("\r\n");
            con.append(value);
            con.append("\r\n");
        } else
            con.append(db_return_nil);
    }
}

void DB::_strIdCr(Context& con, int64_t incr)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
    auto it = _hashMap.find(cmdlist[1]);
    if (it != _hashMap.end()) {
        checkType(con, it, String);
        String& value = getStringValue(it);
        if (isnumber(value[0])) {
            int64_t number = atol(value.c_str());
            number += incr;
            const char *numstr = convert(number);
            _hashMap[cmdlist[1]] = String(numstr);
            con.append(": ");
            con.append(numstr);
            con.append("\r\n");
        } else {
            con.append(db_return_interger_err);
            return;
        }
    } else {
        int64_t number = 0;
        number += incr;
        const char *numstr = convert(number);
        _hashMap[cmdlist[1]] = String(numstr);
        con.append(": ");
        con.append(numstr);
        con.append("\r\n");
    }
}

void DB::strIncr(Context& con)
{
    _strIdCr(con, 1);
}

void DB::strIncrBy(Context& con)
{
    auto& cmdlist = con.commandList();
    int64_t incr = atol(cmdlist[2].c_str());
    _strIdCr(con, incr);
}

void DB::strDecr(Context& con)
{
    _strIdCr(con, -1);
}

void DB::strDecrBy(Context& con)
{
    auto& cmdlist = con.commandList();
    int64_t decr = -atol(cmdlist[2].c_str());
    _strIdCr(con, decr);
}

//////////////////////////////////////////////////////////////////
// List Keys Operation
//////////////////////////////////////////////////////////////////

#define getListValue(it) getXXType(it, List&)

void DB::listLeftPush(Context& con)
{
    auto& cmdlist = con.commandList();
    size_t size = cmdlist.size();
    con.db()->isExpiredKey(cmdlist[1]);
    auto it = _hashMap.find(cmdlist[1]);
    con.append(": ");
    if (it != _hashMap.end()) {
        checkType(con, it, List);
        List& list = getListValue(it);
        for (int i = 2; i < size; i++)
            list.push_front(cmdlist[i]);
        con.append(convert(list.size()));
    } else {
        List list;
        for (int i = 2; i < size; i++)
            list.push_front(cmdlist[i]);
        _hashMap[cmdlist[1]] = list;
        con.append(convert(list.size()));
    }
    con.append("\r\n");
}

void DB::listHeadPush(Context& con)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
    auto it = _hashMap.find(cmdlist[1]);
    if (it == _hashMap.end()) {
        con.append(db_return_integer_0);
        return;
    }
    checkType(con, it, List);
    List& list = getListValue(it);
    list.push_front(cmdlist[2]);
    con.append(": ");
    con.append(convert(list.size()));
    con.append("\r\n");
}

void DB::listRightPush(Context& con)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
    size_t size = cmdlist.size();
    auto it = _hashMap.find(cmdlist[1]);
    con.append(": ");
    if (it != _hashMap.end()) {
        checkType(con, it, List);
        List& list = getListValue(it);
        for (int i = 2; i < size; i++)
            list.push_back(cmdlist[i]);
        con.append(convert(list.size()));
    } else {
        List list;
        for (int i = 2; i < size; i++)
            list.push_back(cmdlist[i]);
        _hashMap[cmdlist[1]] = list;
        con.append(convert(list.size()));
    }
    con.append("\r\n");
}

void DB::listTailPush(Context& con)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
    auto it = _hashMap.find(cmdlist[1]);
    if (it == _hashMap.end()) {
        con.append(db_return_integer_0);
        return;
    }
    checkType(con, it, List);
    List& list = getListValue(it);
    list.push_back(cmdlist[2]);
    con.append(": ");
    con.append(convert(list.size()));
    con.append("\r\n");
}

void DB::listLeftPop(Context& con)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
    auto it = _hashMap.find(cmdlist[1]);
    if (it == _hashMap.end()) {
        con.append(db_return_nil);
        return;
    }
    checkType(con, it, List);
    List& list = getListValue(it);
    if (list.empty()) {
        con.append(db_return_nil);
        return;
    }
    String front = std::move(list.front());
    list.pop_front();
    con.append("$");
    con.append(convert(front.size()));
    con.append("\r\n");
    con.append(front);
    con.append("\r\n");
}

void DB::listRightPop(Context& con)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
    auto it = _hashMap.find(cmdlist[1]);
    if (it == _hashMap.end()) {
        con.append(db_return_nil);
        return;
    }
    checkType(con, it, List);
    List& list = getListValue(it);
    if (list.empty()) {
        con.append(db_return_nil);
        return;
    }
    String tail = std::move(list.back());
    list.pop_back();
    con.append("$");
    con.append(convert(tail.size()));
    con.append("\r\n");
    con.append(tail);
    con.append("\r\n");
}

void DB::listRightPopLeftPush(Context& con)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
    auto src = _hashMap.find(cmdlist[1]);
    if (src == _hashMap.end()) {
        con.append(db_return_nil);
        return;
    }
    checkType(con, src, List);
    List& srclist = getListValue(src);
    if (srclist.empty()) {
        con.append(db_return_nil);
        return;
    }
    con.append("$");
    con.append(convert(srclist.back().size()));
    con.append("\r\n");
    con.append(srclist.back());
    con.append("\r\n");
    auto des = _hashMap.find(cmdlist[2]);
    if (des != _hashMap.end()) {
        checkType(con, des, List);
        List& deslist = getListValue(des);
        deslist.push_front(std::move(srclist.back()));
    } else {
        srclist.push_front(std::move(srclist.back()));
    }
    srclist.pop_back();
}

void DB::listRem(Context& con)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
    int count = atoi(cmdlist[2].c_str());
    String& value = cmdlist[3];
    auto it = _hashMap.find(cmdlist[1]);
    if (it == _hashMap.end()) {
        con.append(db_return_integer_0);
        return;
    }
    checkType(con, it, List);
    List& list = getListValue(it);
    int retval = 0;
    if (count > 0) {
        for (auto it = list.cbegin(); it != list.cend(); ) {
            if ((*it).compare(value) == 0) {
                auto tmp = it++;
                list.erase(tmp);
                retval++;
                if (--count == 0)
                    break;
            } else
                it++;
        }
    } else if (count < 0) {
        for (auto it = list.crbegin(); it != list.crend(); it++) {
            if ((*it).compare(value) == 0) {
                // &*(reverse_iterator(i)) == &*(i - 1)
                list.erase((++it).base());
                retval++;
                if (++count == 0)
                    break;
            }
        }
    } else {
        for (auto it = list.cbegin(); it != list.cend(); ) {
            if ((*it).compare(value) == 0) {
                auto tmp = it++;
                list.erase(tmp);
                retval++;
            }
        }
    }
    con.append(": ");
    con.append(convert(retval));
    con.append("\r\n");
}

void DB::listLen(Context& con)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
    auto it = _hashMap.find(cmdlist[1]);
    if (it == _hashMap.end()) {
        con.append(db_return_integer_0);
        return;
    }
    checkType(con, it, List);
    List& list = getListValue(it);
    con.append(": ");
    con.append(convert(list.size()));
    con.append("\r\n");
}

void DB::listIndex(Context& con)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
    auto it = _hashMap.find(cmdlist[1]);
    if (it == _hashMap.end()) {
        con.append(db_return_nil);
        return;
    }
    checkType(con, it, List);
    List& list = getListValue(it);
    int index = atoi(cmdlist[2].c_str());
    size_t size = list.size();
    if (index < 0)
        index += size;
    if (index >= size) {
        con.append(db_return_nil);
        return;
    }
    for (auto& it : list)
        if (index-- == 0) {
            con.append("$");
            con.append(convert(it.size()));
            con.append("\r\n");
            con.append(it);
            con.append("\r\n");
            break;
        }
}

void DB::listSet(Context& con)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
    auto it = _hashMap.find(cmdlist[1]);
    if (it == _hashMap.end()) {
        con.append(db_return_no_such_key);
        return;
    }
    checkType(con, it, List);
    List& list = getListValue(it);
    int index = atoi(cmdlist[2].c_str());
    size_t size = list.size();
    if (index < 0)
        index += size;
    if (index >= size) {
        con.append(db_return_out_of_range);
        return;
    }
    for (auto& it : list)
        if (index-- == 0) {
            it.assign(cmdlist[3]);
            break;
        }
    con.append(db_return_ok);
}

void DB::listRange(Context& con)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
    auto it = _hashMap.find(cmdlist[1]);
    if (it == _hashMap.end()) {
        con.append(db_return_nil);
        return;
    }
    checkType(con, it, List);
    List& list = getListValue(it);
    size_t end = list.size() - 1;
    int start = atoi(cmdlist[2].c_str());
    int stop = atoi(cmdlist[3].c_str());
    if (start < 0)
        start += end + 1;
    if (stop < 0)
        stop += end + 1;
    if (stop > end)
        stop = end;
    if (start > end){
        con.append(db_return_nil);
        return;
    }
    con.append("*");
    con.append(convert(stop - start + 1));
    con.append("\r\n");
    int i = 0;
    for (auto& it : list) {
        if (i < start) {
            i++;
            continue;
        }
        if (i > stop)
            break;
        con.append("$");
        con.append(convert(it.size()));
        con.append("\r\n");
        con.append(it);
        con.append("\r\n");
        i++;
    }
}

void DB::listTrim(Context& con)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
    auto it = _hashMap.find(cmdlist[1]);
    if (it == _hashMap.end()) {
        con.append(db_return_ok);
        return;
    }
    checkType(con, it, List);
    List& list = getListValue(it);
    int start = atoi(cmdlist[2].c_str());
    int stop = atoi(cmdlist[3].c_str());
    size_t size = list.size();
    if (start < 0)
        start += size;
    if (stop < 0)
        stop += size;
    if (start > size - 1 || start > stop || stop > size - 1) {
        list.clear();
        con.append(db_return_ok);
        return;
    }
    int i = 0;
    for (auto it = list.cbegin(); it != list.cend(); ) {
        auto tmp = it++;
        if (i < start) {
            list.erase(tmp);
            i++;
        } else if (i > stop) {
            list.erase(tmp);
            i++;
        } else
            i++;
    }
    con.append(db_return_ok);
}

//////////////////////////////////////////////////////////////////
// Set Keys Operation
//////////////////////////////////////////////////////////////////

#define getSetValue(it) getXXType(it, Set&)

void DB::setAdd(Context& con)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
    size_t members = cmdlist.size();
    auto it = _hashMap.find(cmdlist[1]);
    if (it != _hashMap.end()) {
        checkType(con, it, Set);
        Set& set = getSetValue(it);
        int retval = 0;
        for (int i = 2; i < members; i++) {
            if (set.find(cmdlist[i]) == set.end()) {
                set.insert(cmdlist[i]);
                retval++;
            }
        }
        con.append(": ");
        con.append(convert(retval));
        con.append("\r\n");
    } else {
        Set set;
        for (int i = 2; i < members; i++)
            set.insert(cmdlist[i]);
        _hashMap[cmdlist[1]] = std::move(set);
        con.append(": ");
        con.append(convert(members - 2));
        con.append("\r\n");
    }
}

void DB::setIsMember(Context& con)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
    auto it = _hashMap.find(cmdlist[1]);
    if (it != _hashMap.end()) {
        checkType(con, it, Set);
        Set& set = getSetValue(it);
        if (set.find(cmdlist[2]) != set.end())
            con.append(db_return_integer_1);
        else
            con.append(db_return_integer_0);
    } else
        con.append(db_return_integer_0);
}

void DB::setPop(Context& con)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
    auto it = _hashMap.find(cmdlist[1]);
    if (it == _hashMap.end()) {
        con.append(db_return_nil);
        return;
    }
    checkType(con, it, Set);
    Set& set = getSetValue(it);
    if (set.empty()) {
        con.append(db_return_nil);
        return;
    }
    srand(clock());
    size_t index = rand() % set.size();
    for (auto it = set.cbegin(); it != set.cend(); it++)
        if (index-- == 0) {
            con.append("$");
            con.append(convert(it->size()));
            con.append("\r\n");
            con.append(*it);
            con.append("\r\n");
            set.erase(it);
            break;
        }
}

void DB::setRandMember(Context& con)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
    int count = 0;
    if (cmdlist.size() > 2) {
        count = atoi(cmdlist[2].c_str());
        if (count == 0) {
            con.append(db_return_nil);
            return;
        }
    }
    auto it = _hashMap.find(cmdlist[1]);
    if (it == _hashMap.end()) {
        con.append(db_return_nil);
        return;
    }
    checkType(con, it, Set);
    Set& set = getSetValue(it);
    if (set.empty()) {
        con.append(db_return_nil);
        return;
    }
    // 类型转换，int -> size_t
    if (count >= static_cast<ssize_t>(set.size())) {
        con.append("*");
        con.append(convert(set.size()));
        con.append("\r\n");
        for (auto& it : set) {
            con.append("$");
            con.append(convert(it.size()));
            con.append("\r\n");
            con.append(it);
            con.append("\r\n");
        }
        return;
    }
    if (count == 0 || count < 0) {
        if (count == 0)
            count = -1;
        con.append("*");
        con.append(convert(-count));
        con.append("\r\n");
        while (count++ < 0) {
            srand(clock());
            size_t next = rand() % set.size();
            for (auto& it : set) {
                if (next-- == 0) {
                    con.append("$");
                    con.append(convert(it.size()));
                    con.append("\r\n");
                    con.append(it);
                    con.append("\r\n");
                    break;
                }
            }
        }
        return;
    }
    con.append("*");
    con.append(convert(count));
    con.append("\r\n");
    String flag(count, 0);
    size_t next;
    while (count-- > 0) {
        do {
            srand(clock());
            next = rand() % set.size();
        } while (flag[next]);
        flag[next] = 1;
        for (auto& it : set) {
            if (next-- == 0) {
                con.append("$");
                con.append(convert(it.size()));
                con.append("\r\n");
                con.append(it);
                con.append("\r\n");
                break;
            }
        }
    }
}

void DB::setRem(Context& con)
{

}

void DB::setMove(Context& con)
{

}

void DB::setCard(Context& con)
{

}

void DB::setMembers(Context& con)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
    auto it = _hashMap.find(cmdlist[1]);
    if (it == _hashMap.end()) {
        con.append(db_return_nil);
        return;
    }
    checkType(con, it, Set);
    Set& set = getSetValue(it);
    con.append("*");
    con.append(convert(set.size()));
    con.append("\r\n");
    for (auto& it : set) {
        con.append("$");
        con.append(convert(it.size()));
        con.append("\r\n");
        con.append(it);
        con.append("\r\n");
    }
}

void DB::setInter(Context& con)
{

}

void DB::setInterStore(Context& con)
{

}

void DB::setUnion(Context& con)
{

}

void DB::setUnionStore(Context& con)
{

}

void DB::setDiff(Context& con)
{

}

void DB::setDiffStore(Context& con)
{

}
