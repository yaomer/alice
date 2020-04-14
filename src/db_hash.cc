#include "db.h"

using namespace Alice;

void DB::hsetCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    expireIfNeeded(cmdlist[1]);
    touchWatchKey(cmdlist[1]);
    auto it = find(cmdlist[1]);
    if (!isFound(it)) {
        Hash hash;
        hash.emplace(cmdlist[2], cmdlist[3]);
        insert(cmdlist[1], hash);
        db_return(con, reply.n1);
    }
    checkType(con, it, Hash);
    Hash& hash = getHashValue(it);
    if (hash.find(cmdlist[2]) != hash.end()) {
        con.append(reply.n0);
    } else {
        con.append(reply.n1);
    }
    hash.emplace(cmdlist[2], cmdlist[3]);
}

void DB::hsetnxCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    expireIfNeeded(cmdlist[1]);
    auto it = find(cmdlist[1]);
    if (!isFound(it)) {
        Hash hash;
        hash.emplace(cmdlist[2], cmdlist[3]);
        insert(cmdlist[1], hash);
        touchWatchKey(cmdlist[1]);
        db_return(con, reply.n1);
    }
    checkType(con, it, Hash);
    Hash& hash = getHashValue(it);
    if (hash.find(cmdlist[2]) != hash.end()) {
        con.append(reply.n0);
    } else {
        hash.emplace(cmdlist[2], cmdlist[3]);
        touchWatchKey(cmdlist[1]);
        con.append(reply.n1);
    }
}

void DB::hgetCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    expireIfNeeded(cmdlist[1]);
    auto it = find(cmdlist[1]);
    if (!isFound(it)) db_return(con, reply.nil);
    checkType(con, it, Hash);
    Hash& hash = getHashValue(it);
    auto value = hash.find(cmdlist[2]);
    if (value != hash.end()) {
        appendReplyString(con, value->second);
    } else
        con.append(reply.nil);
}

void DB::hexistsCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    expireIfNeeded(cmdlist[1]);
    auto it = find(cmdlist[1]);
    if (!isFound(it)) db_return(con, reply.n0);
    checkType(con, it, Hash);
    Hash& hash = getHashValue(it);
    if (hash.find(cmdlist[2]) != hash.end()) {
        con.append(reply.n1);
    } else {
        con.append(reply.n0);
    }
}

void DB::hdelCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    expireIfNeeded(cmdlist[1]);
    auto it = find(cmdlist[1]);
    if (!isFound(it)) db_return(con, reply.n0);
    size_t size = cmdlist.size();
    checkType(con, it, Hash);
    int retval = 0;
    Hash& hash = getHashValue(it);
    for (size_t i = 2; i < size; i++) {
        auto it = hash.find(cmdlist[i]);
        if (it != hash.end()) {
            hash.erase(it);
            retval++;
        }
    }
    if (hash.empty()) delKeyWithExpire(cmdlist[1]);
    touchWatchKey(cmdlist[1]);
    appendReplyNumber(con, retval);
}

void DB::hlenCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    expireIfNeeded(cmdlist[1]);
    auto it = find(cmdlist[1]);
    if (isFound(it)) {
        checkType(con, it, Hash);
        Hash& hash = getHashValue(it);
        appendReplyNumber(con, hash.size());
    } else {
        con.append(reply.n0);
    }
}

void DB::hstrlenCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    expireIfNeeded(cmdlist[1]);
    auto it = find(cmdlist[1]);
    if (!isFound(it)) db_return(con, reply.n0);
    checkType(con, it, Hash);
    Hash& hash = getHashValue(it);
    auto value = hash.find(cmdlist[2]);
    if (value != hash.end()) {
        appendReplyNumber(con, value->second.size());
    } else {
        con.append(reply.n0);
    }
}

void DB::hincrbyCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    expireIfNeeded(cmdlist[1]);
    int64_t incr = str2ll(cmdlist[3].c_str());
    if (str2numberErr()) db_return(con, reply.integer_err);
    auto it = find(cmdlist[1]);
    if (!isFound(it)) {
        Hash hash;
        hash.emplace(cmdlist[2], cmdlist[3]);
        insert(cmdlist[1], hash);
        touchWatchKey(cmdlist[1]);
        db_return(con, reply.n0);
    }
    checkType(con, it, Hash);
    Hash& hash = getHashValue(it);
    auto value = hash.find(cmdlist[2]);
    if (value != hash.end()) {
        int64_t i64 = str2ll(value->second.c_str());
        if (str2numberErr()) db_return(con, reply.integer_err);
        i64 += incr;
        value->second.assign(convert(i64));
        appendReplyNumber(con, i64);
    } else {
        hash.emplace(cmdlist[2], String(convert(incr)));
        appendReplyNumber(con, incr);
    }
    touchWatchKey(cmdlist[1]);
}

void DB::hmsetCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    expireIfNeeded(cmdlist[1]);
    size_t size = cmdlist.size();
    if (size % 2 != 0) db_return(con, reply.argnumber_err);
    touchWatchKey(cmdlist[1]);
    auto it = find(cmdlist[1]);
    if (!isFound(it)) {
        Hash hash;
        for (size_t i = 2; i < size; i += 2)
            hash.emplace(cmdlist[i], cmdlist[i+1]);
        insert(cmdlist[1], hash);
        db_return(con, reply.ok);
    }
    checkType(con, it, Hash);
    Hash& hash = getHashValue(it);
    for (size_t i = 2; i < size; i += 2)
        hash.emplace(cmdlist[i], cmdlist[i+1]);
    con.append(reply.ok);
}

void DB::hmgetCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    expireIfNeeded(cmdlist[1]);
    size_t size = cmdlist.size();
    auto it = find(cmdlist[1]);
    appendReplyMulti(con, size - 2);
    if (!isFound(it)) {
        for (size_t i = 2; i < size; i++)
            con.append(reply.nil);
        return;
    }
    checkType(con, it, Hash);
    Hash& hash = getHashValue(it);
    for (size_t i = 2; i < size; i++) {
        auto it = hash.find(cmdlist[i]);
        if (it != hash.end()) {
            appendReplyString(con, it->second);
        } else {
            con.append(reply.nil);
        }
    }
}

#define HGETKEYS     0
#define HGETVALUES   1
#define HGETALL      2

void DB::hgetXX(Context& con, int getXX)
{
    auto& cmdlist = con.commandList();
    expireIfNeeded(cmdlist[1]);
    auto it = find(cmdlist[1]);
    if (!isFound(it)) db_return(con, reply.nil);
    checkType(con, it, Hash);
    Hash& hash = getHashValue(it);
    appendReplyMulti(con, getXX == HGETALL ? hash.size() * 2 : hash.size());
    for (auto& it : hash) {
        if (getXX == HGETKEYS) {
            appendReplyString(con, it.first);
        } else if (getXX == HGETVALUES) {
            appendReplyString(con, it.second);
        } else if (getXX == HGETALL) {
            appendReplyString(con, it.first);
            appendReplyString(con, it.second);
        }
    }
}

void DB::hkeysCommand(Context& con)
{
    hgetXX(con, HGETKEYS);
}

void DB::hvalsCommand(Context& con)
{
    hgetXX(con, HGETVALUES);
}

void DB::hgetAllCommand(Context& con)
{
    hgetXX(con, HGETALL);
}
