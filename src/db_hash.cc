#include "db.h"

using namespace Alice;

// HSET key field value
void DB::hsetCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    auto& key = cmdlist[1];
    auto& field = cmdlist[2];
    auto& value = cmdlist[3];
    checkExpire(key);
    touchWatchKey(key);
    auto it = find(key);
    if (!isFound(it)) {
        Hash hash;
        hash.emplace(field, value);
        insert(key, std::move(hash));
        db_return(con, reply.n1);
    }
    checkType(con, it, Hash);
    auto& hash = getHashValue(it);
    if (hash.find(field) != hash.end()) {
        con.append(reply.n0);
    } else {
        con.append(reply.n1);
    }
    hash.emplace(field, value);
}

// HSETNX key field value
void DB::hsetnxCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    auto& key = cmdlist[1];
    auto& field = cmdlist[2];
    auto& value = cmdlist[3];
    checkExpire(key);
    auto it = find(key);
    if (!isFound(it)) {
        Hash hash;
        hash.emplace(field, value);
        insert(key, std::move(hash));
        touchWatchKey(key);
        db_return(con, reply.n1);
    }
    checkType(con, it, Hash);
    auto& hash = getHashValue(it);
    if (hash.find(field) != hash.end()) {
        con.append(reply.n0);
    } else {
        hash.emplace(field, value);
        touchWatchKey(key);
        con.append(reply.n1);
    }
}

// HGET key field
void DB::hgetCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    auto& key = cmdlist[1];
    auto& field = cmdlist[2];
    checkExpire(key);
    auto it = find(key);
    if (!isFound(it)) db_return(con, reply.nil);
    checkType(con, it, Hash);
    auto& hash = getHashValue(it);
    auto value = hash.find(field);
    if (value != hash.end()) {
        con.appendReplyString(value->second);
    } else
        con.append(reply.nil);
}

// HEXISTS key field
void DB::hexistsCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    auto& key = cmdlist[1];
    auto& field = cmdlist[2];
    checkExpire(key);
    auto it = find(key);
    if (!isFound(it)) db_return(con, reply.n0);
    checkType(con, it, Hash);
    auto& hash = getHashValue(it);
    if (hash.find(field) != hash.end()) {
        con.append(reply.n1);
    } else {
        con.append(reply.n0);
    }
}

// HDEL key field [field ...]
void DB::hdelCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    auto& key = cmdlist[1];
    checkExpire(key);
    auto it = find(key);
    if (!isFound(it)) db_return(con, reply.n0);
    size_t size = cmdlist.size();
    checkType(con, it, Hash);
    int dels = 0;
    auto& hash = getHashValue(it);
    for (size_t i = 2; i < size; i++) {
        auto it = hash.find(cmdlist[i]);
        if (it != hash.end()) {
            hash.erase(it);
            dels++;
        }
    }
    checkEmpty(hash, key);
    touchWatchKey(key);
    con.appendReplyNumber(dels);
}

// HLEN key
void DB::hlenCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    auto& key = cmdlist[1];
    checkExpire(key);
    auto it = find(key);
    if (isFound(it)) {
        checkType(con, it, Hash);
        auto& hash = getHashValue(it);
        con.appendReplyNumber(hash.size());
    } else {
        con.append(reply.n0);
    }
}

// HSTRLEN key field
void DB::hstrlenCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    auto& key = cmdlist[1];
    auto& field = cmdlist[2];
    checkExpire(key);
    auto it = find(key);
    if (!isFound(it)) db_return(con, reply.n0);
    checkType(con, it, Hash);
    auto& hash = getHashValue(it);
    auto value = hash.find(field);
    if (value != hash.end()) {
        con.appendReplyNumber(value->second.size());
    } else {
        con.append(reply.n0);
    }
}

// HINCRBY key field increment
void DB::hincrbyCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    auto& key = cmdlist[1];
    auto& field = cmdlist[2];
    auto& incr_str = cmdlist[3];
    checkExpire(key);
    auto incr = str2ll(incr_str.c_str());
    if (str2numberErr()) db_return(con, reply.integer_err);
    auto it = find(key);
    if (!isFound(it)) {
        Hash hash;
        hash.emplace(field, incr_str);
        insert(key, std::move(hash));
        touchWatchKey(key);
        db_return(con, reply.n0);
    }
    checkType(con, it, Hash);
    auto& hash = getHashValue(it);
    auto value = hash.find(field);
    if (value != hash.end()) {
        int64_t i64 = str2ll(value->second.c_str());
        if (str2numberErr()) db_return(con, reply.integer_err);
        i64 += incr;
        value->second.assign(convert(i64));
        con.appendReplyNumber(i64);
    } else {
        hash.emplace(field, String(convert(incr)));
        con.appendReplyNumber(incr);
    }
    touchWatchKey(key);
}

// HMSET key field value [field value ...]
void DB::hmsetCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    auto& key = cmdlist[1];
    checkExpire(key);
    size_t size = cmdlist.size();
    if (size % 2 != 0) db_return(con, reply.argnumber_err);
    touchWatchKey(key);
    auto it = find(key);
    if (!isFound(it)) {
        Hash hash;
        for (size_t i = 2; i < size; i += 2)
            hash.emplace(cmdlist[i], cmdlist[i+1]);
        insert(key, hash);
        db_return(con, reply.ok);
    }
    checkType(con, it, Hash);
    auto& hash = getHashValue(it);
    for (size_t i = 2; i < size; i += 2)
        hash.emplace(cmdlist[i], cmdlist[i+1]);
    con.append(reply.ok);
}

// HMGET key field [field ...]
void DB::hmgetCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    auto& key = cmdlist[1];
    checkExpire(key);
    size_t size = cmdlist.size();
    auto it = find(key);
    con.appendReplyMulti(size - 2);
    if (!isFound(it)) {
        for (size_t i = 2; i < size; i++)
            con.append(reply.nil);
        return;
    }
    checkType(con, it, Hash);
    auto& hash = getHashValue(it);
    for (size_t i = 2; i < size; i++) {
        auto it = hash.find(cmdlist[i]);
        if (it != hash.end()) {
            con.appendReplyString(it->second);
        } else {
            con.append(reply.nil);
        }
    }
}

#define HGETKEYS     0
#define HGETVALUES   1
#define HGETALL      2

// HKEYS/HVALS/HGETALL key
void DB::hget(Context& con, int what)
{
    auto& cmdlist = con.commandList();
    auto& key = cmdlist[1];
    checkExpire(key);
    auto it = find(key);
    if (!isFound(it)) db_return(con, reply.nil);
    checkType(con, it, Hash);
    auto& hash = getHashValue(it);
    con.appendReplyMulti(what == HGETALL ? hash.size() * 2 : hash.size());
    for (auto& it : hash) {
        if (what == HGETKEYS) {
            con.appendReplyString(it.first);
        } else if (what == HGETVALUES) {
            con.appendReplyString(it.second);
        } else {
            con.appendReplyString(it.first);
            con.appendReplyString(it.second);
        }
    }
}

void DB::hkeysCommand(Context& con)
{
    hget(con, HGETKEYS);
}

void DB::hvalsCommand(Context& con)
{
    hget(con, HGETVALUES);
}

void DB::hgetAllCommand(Context& con)
{
    hget(con, HGETALL);
}
