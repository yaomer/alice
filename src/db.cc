#include <map>
#include <vector>
#include <functional>
#include "db.h"
#include "server.h"

using namespace Alice;
using std::placeholders::_1;

DB::DB()
{
    _commandMap = {
        { "SET",    { "SET", -3, true, std::bind(&DB::set, this, _1) } },
        { "SETNX",  { "SETNX", -3, true, std::bind(&DB::setnx, this, _1) } },
        { "GET",    { "GET", -2, false, std::bind(&DB::get, this, _1) } },
        { "GETSET", { "GETSET", -3, true, std::bind(&DB::getset, this, _1) } },
        { "APPEND", { "APPEND", -3, true, std::bind(&DB::append, this, _1) } },
        { "STRLEN", { "STRLEN", -2, false, std::bind(&DB::strlen, this, _1) } },
        { "MSET",   { "MSET", 0, true, std::bind(&DB::mset, this, _1) } },
        { "MGET",   { "MGET", 0, false, std::bind(&DB::mget, this, _1) } },
        { "INCR",   { "INCR", -2, true, std::bind(&DB::incr, this, _1) } },
        { "INCRBY", { "INCRBY", -3, true, std::bind(&DB::incrby, this, _1) } },
        { "DECR",   { "DECR", -2, true, std::bind(&DB::decr, this, _1) } },
        { "DECRBY", { "DECRBY", -3, true, std::bind(&DB::decrby, this, _1) } },
        { "TTL",    { "TTL", -2, false, std::bind(&DB::ttl, this, _1) } },
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
}

void DB::set(Connection& conn)
{
    auto& cmdlist = conn.commandList();
    int64_t expire;
    conn.db()->isExpiredKey(cmdlist[1]);
    if (cmdlist.size() > 3) {
        std::transform(cmdlist[3].begin(), cmdlist[3].end(), cmdlist[3].begin(), ::toupper);
        expire = atoll(cmdlist[4].c_str());
        if (cmdlist[3].compare("EX") == 0)
            expire *= 1000;
        else if (cmdlist[3].compare("PX")) {
            conn.append(db_return_err);
            return;
        }
    }
    _hashMap[cmdlist[1]] = cmdlist[2];
    if (cmdlist.size() > 3) {
        if (conn.db()->isExpiredMap(cmdlist[1]))
            conn.db()->delExpireKey(cmdlist[1]);
        conn.db()->addExpireKey(cmdlist[1], expire);
    }
    conn.append(db_return_ok);
}

void DB::setnx(Connection& conn)
{
    auto& cmdlist = conn.commandList();
    if (_hashMap.find(cmdlist[1]) != _hashMap.end()) {
        conn.append(db_return_integer_0);
    } else {
        _hashMap[cmdlist[1]] = cmdlist[2];
        conn.append(db_return_integer_1);
    }
}

void DB::get(Connection& conn)
{
    auto& cmdlist = conn.commandList();
    conn.db()->isExpiredKey(cmdlist[1]);
    auto it = _hashMap.find(cmdlist[1]);
    if (it != _hashMap.end()) {
        auto& value = std::any_cast<std::string&>(it->second.value());
        conn.append("$");
        conn.append(convert(value.size()));
        conn.append("\r\n");
        conn.append(value);
        conn.append("\r\n");
    } else
        conn.append(db_return_nil);
}

void DB::getset(Connection& conn)
{
    auto& cmdlist = conn.commandList();
    conn.db()->isExpiredKey(cmdlist[1]);
    auto it = _hashMap.find(cmdlist[1]);
    if (it != _hashMap.end()) {
        std::string& oldvalue =
            std::any_cast<std::string&>(_hashMap[cmdlist[1]].value());
        _hashMap[cmdlist[1]] = cmdlist[2];
        conn.append("+");
        conn.append(oldvalue);
        conn.append("\r\n");
    } else {
        _hashMap[cmdlist[1]] = cmdlist[2];
        conn.append(db_return_nil);
    }
}

void DB::strlen(Connection& conn)
{
    auto& cmdlist = conn.commandList();
    conn.db()->isExpiredKey(cmdlist[1]);
    auto it = _hashMap.find(cmdlist[1]);
    if (it != _hashMap.end()) {
        auto& value = std::any_cast<std::string&>(it->second.value());
        const char *len = convert(value.size());
        conn.append(": ");
        conn.append(len);
        conn.append("\r\n");
    } else
        conn.append(db_return_integer_0);
}

void DB::append(Connection& conn)
{
    auto& cmdlist = conn.commandList();
    conn.db()->isExpiredKey(cmdlist[1]);
    auto it = _hashMap.find(cmdlist[1]);
    if (it != _hashMap.end()) {
        std::string& oldvalue =
            std::any_cast<std::string&>(_hashMap[cmdlist[1]].value());
        oldvalue += cmdlist[2];
        _hashMap[cmdlist[1]] = oldvalue;
        const char *len = convert(oldvalue.size());
        conn.append(": ");
        conn.append(len);
        conn.append("\r\n");
    } else {
        _hashMap[cmdlist[1]] = cmdlist[2];
        const char *len = convert(cmdlist[2].size());
        conn.append(": ");
        conn.append(len);
        conn.append("\r\n");
    }
}

void DB::mset(Connection& conn)
{
    auto& cmdlist = conn.commandList();
    size_t size = cmdlist.size();
    if (size % 2 == 0) { conn.append(db_return_err); return; }
    for (int i = 1; i < size; i += 2) {
        conn.db()->isExpiredKey(cmdlist[1]);
        _hashMap[cmdlist[i]] = cmdlist[i + 1];
    }
    conn.append(db_return_ok);
}

void DB::mget(Connection& conn)
{
    char buf[32];
    auto& cmdlist = conn.commandList();
    size_t size = cmdlist.size();
    conn.append("*");
    conn.append(convert(size - 1));
    conn.append("\r\n");
    for (int i = 1; i < size; i++) {
        conn.db()->isExpiredKey(cmdlist[1]);
        auto it = _hashMap.find(cmdlist[i]);
        if (it != _hashMap.end()) {
            std::string& value = std::any_cast<std::string&>(it->second.value());
            snprintf(buf, sizeof(buf), "%zu", value.size());
            conn.append("$");
            conn.append(buf);
            conn.append("\r\n");
            conn.append(value);
            conn.append("\r\n");
        } else
            conn.append(db_return_nil);
    }
}

void DB::_idcr(Connection& conn, int64_t incr)
{
    auto& cmdlist = conn.commandList();
    conn.db()->isExpiredKey(cmdlist[1]);
    auto it = _hashMap.find(cmdlist[1]);
    if (it != _hashMap.end()) {
        auto& value = std::any_cast<std::string&>(it->second.value());
        if (isnumber(value[0])) {
            int64_t number = atol(value.c_str());
            number += incr;
            const char *numstr = convert(number);
            _hashMap[cmdlist[1]] = std::string(numstr);
            conn.append(": ");
            conn.append(numstr);
            conn.append("\r\n");
        } else
            conn.append(db_return_err);
    } else {
        int64_t number = 0;
        number += incr;
        const char *numstr = convert(number);
        _hashMap[cmdlist[1]] = std::string(numstr);
        conn.append(": ");
        conn.append(numstr);
        conn.append("\r\n");
    }
}

void DB::incr(Connection& conn)
{
    _idcr(conn, 1);
}

void DB::incrby(Connection& conn)
{
    auto& cmdlist = conn.commandList();
    int64_t incr = atol(cmdlist[2].c_str());
    _idcr(conn, incr);
}

void DB::decr(Connection& conn)
{
    _idcr(conn, -1);
}

void DB::decrby(Connection& conn)
{
    auto& cmdlist = conn.commandList();
    int64_t decr = -atol(cmdlist[2].c_str());
    _idcr(conn, decr);
}

void DB::ttl(Connection& conn)
{
    auto& cmdlist = conn.commandList();
    auto it = _hashMap.find(cmdlist[1]);
    if (it == _hashMap.end()) {
        conn.append(db_return_integer__2);
        return;
    }
    auto expire = conn.db()->expireMap().find(cmdlist[1]);
    if (expire == conn.db()->expireMap().end()) {
        conn.append(db_return_integer__1);
        return;
    }
    const char *s = convert(expire->second - Angel::TimeStamp::now());
    conn.append(": ");
    conn.append(s);
    conn.append("\r\n");
}
