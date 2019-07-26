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
        { "LLEN",       { "LLEN", -2, false, std::bind(&DB::listLen, this, _1)} },
        { "LINDEX",     { "LINDEX", -3, false, std::bind(&DB::listIndex, this, _1) } },
        { "LSET",       { "LSET", -4, true, std::bind(&DB::listSet, this, _1) } },
        { "LRANGE",     { "LRANGE", -4, false, std::bind(&DB::listRange, this, _1) } },
        { "LTRIM",      { "LTRIM", -4, true, std::bind(&DB::listTrim, this, _1)} },
        { "TTL",        { "TTL", -2, false, std::bind(&DB::ttl, this, _1) } },
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
    static const char *db_return_nil = "$5\r\n(nil)\r\n";
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
}

void DB::ttl(Context& con)
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
    const char *s = convert(expire->second - Angel::TimeStamp::now());
    con.append(": ");
    con.append(s);
    con.append("\r\n");
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
// String Keys Operators
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
        if (con.db()->isExpiredMap(cmdlist[1]))
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
    if (it != _hashMap.end()) {
        checkType(con, it, String);
        auto& value = getStringValue(it);
        con.append("$");
        con.append(convert(value.size()));
        con.append("\r\n");
        con.append(value);
        con.append("\r\n");
    } else
        con.append(db_return_nil);
}

void DB::strGetSet(Context& con)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
    auto it = _hashMap.find(cmdlist[1]);
    if (it != _hashMap.end()) {
        checkType(con, it, String);
        String oldvalue = getStringValue(it);
        _hashMap[cmdlist[1]] = cmdlist[2];
        con.append("+");
        con.append(oldvalue);
        con.append("\r\n");
    } else {
        _hashMap[cmdlist[1]] = cmdlist[2];
        con.append(db_return_nil);
    }
}

void DB::strLen(Context& con)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
    auto it = _hashMap.find(cmdlist[1]);
    if (it != _hashMap.end()) {
        checkType(con, it, String);
        auto& value = getStringValue(it);
        const char *len = convert(value.size());
        con.append(": ");
        con.append(len);
        con.append("\r\n");
    } else
        con.append(db_return_integer_0);
}

void DB::strAppend(Context& con)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
    auto it = _hashMap.find(cmdlist[1]);
    if (it != _hashMap.end()) {
        checkType(con, it, String);
        String& oldvalue = getStringValue(it);
        oldvalue += cmdlist[2];
        _hashMap[cmdlist[1]] = oldvalue;
        const char *len = convert(oldvalue.size());
        con.append(": ");
        con.append(len);
        con.append("\r\n");
    } else {
        _hashMap[cmdlist[1]] = cmdlist[2];
        const char *len = convert(cmdlist[2].size());
        con.append(": ");
        con.append(len);
        con.append("\r\n");
    }
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
    char buf[32];
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
            auto& value = getStringValue(it);
            snprintf(buf, sizeof(buf), "%zu", value.size());
            con.append("$");
            con.append(buf);
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
// List Keys Operators
//////////////////////////////////////////////////////////////////

#define getListValue(it) getXXType(it, List&)

void DB::listLeftPush(Context& con)
{
    auto& cmdlist = con.commandList();
    size_t size = cmdlist.size();
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
    auto it = _hashMap.find(cmdlist[1]);
    if (it != _hashMap.end()) {
        checkType(con, it, List);
        List& list = getListValue(it);
        list.push_front(cmdlist[2]);
        con.append(": ");
        con.append(convert(list.size()));
        con.append("\r\n");
    } else
        con.append(db_return_integer_0);
}

void DB::listRightPush(Context& con)
{
    auto& cmdlist = con.commandList();
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
    auto it = _hashMap.find(cmdlist[1]);
    if (it != _hashMap.end()) {
        checkType(con, it, List);
        List& list = getListValue(it);
        list.push_back(cmdlist[2]);
        con.append(": ");
        con.append(convert(list.size()));
        con.append("\r\n");
    } else
        con.append(db_return_integer_0);
}

void DB::listLeftPop(Context& con)
{
    auto& cmdlist = con.commandList();
    auto it = _hashMap.find(cmdlist[1]);
    if (it != _hashMap.end()) {
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
    } else
        con.append(db_return_nil);
}

void DB::listRightPop(Context& con)
{
    auto& cmdlist = con.commandList();
    auto it = _hashMap.find(cmdlist[1]);
    if (it != _hashMap.end()) {
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
    } else
        con.append(db_return_nil);
}

void DB::listRightPopLeftPush(Context& con)
{
    auto& cmdlist = con.commandList();
    auto src = _hashMap.find(cmdlist[1]);
    if (src != _hashMap.end()) {
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
    } else {
        con.append(db_return_nil);
    }
}

void DB::listRem(Context& con)
{
    auto& cmdlist = con.commandList();
    int count = atoi(cmdlist[2].c_str());
    String& value = cmdlist[3];
    auto it = _hashMap.find(cmdlist[1]);
    if (it != _hashMap.end()) {
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
    } else {
        con.append(db_return_integer_0);
    }
}

void DB::listLen(Context& con)
{
    auto& cmdlist = con.commandList();
    auto it = _hashMap.find(cmdlist[1]);
    if (it != _hashMap.end()) {
        checkType(con, it, List);
        List& list = getListValue(it);
        con.append(": ");
        con.append(convert(list.size()));
        con.append("\r\n");
    } else {
        con.append(db_return_integer_0);
    }
}

void DB::listIndex(Context& con)
{
    auto& cmdlist = con.commandList();
    auto it = _hashMap.find(cmdlist[1]);
    if (it != _hashMap.end()) {
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
    } else {
        con.append(db_return_nil);
    }
}

void DB::listSet(Context& con)
{
    auto& cmdlist = con.commandList();
    auto it = _hashMap.find(cmdlist[1]);
    if (it != _hashMap.end()) {
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
    } else {
        con.append(db_return_no_such_key);
    }
}

void DB::listRange(Context& con)
{
    auto& cmdlist = con.commandList();
    auto it = _hashMap.find(cmdlist[1]);
    if (it != _hashMap.end()) {
        checkType(con, it, List);
        List& list = getListValue(it);
        size_t end = list.size() - 1;
        int start = atoi(cmdlist[2].c_str());
        int stop = atoi(cmdlist[3].c_str());
        if (start < 0)
            start += end + 1;
        if (stop < 0)
            stop += end + 1;
        if (start > end) goto err;
        if (stop > end)
            stop = end;
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
err:
    con.append(db_return_nil);
}

void DB::listTrim(Context& con)
{
    auto& cmdlist = con.commandList();
    auto it = _hashMap.find(cmdlist[1]);
    if (it != _hashMap.end()) {
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
    }
    con.append(db_return_ok);
}
