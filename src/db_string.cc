#include "db.h"

using namespace Alice;

#define SET_NX 0x01
#define SET_XX 0x02
#define SET_EX 0x04
#define SET_PX 0x08

namespace Alice {
    thread_local std::unordered_map<std::string, int> setops = {
        { "NX", SET_NX },
        { "XX", SET_XX },
        { "EX", SET_EX },
        { "PX", SET_PX },
    };
}

static int parseSetArgs(Context& con, unsigned& cmdops, int64_t& expire)
{
    auto& cmdlist = con.commandList();
    size_t len = cmdlist.size();
    for (size_t i = 3; i < len; i++) {
        std::transform(cmdlist[i].begin(), cmdlist[i].end(), cmdlist[i].begin(), ::toupper);
        auto op = setops.find(cmdlist[i]);
        if (op != setops.end()) cmdops |= op->second;
        else goto syntax_err;
        switch (op->second) {
        case SET_NX: case SET_XX:
            break;
        case SET_EX: case SET_PX: {
            if (i + 1 >= len) goto syntax_err;
            expire = str2ll(cmdlist[++i].c_str());
            if (str2numberErr()) goto integer_err;
            if (expire <= 0) goto timeout_err;
            if (op->second == SET_EX)
                expire *= 1000;
            break;
        }
        default:
            goto syntax_err;
        }
    }
    return C_OK;
syntax_err:
    con.append(reply.syntax_err);
    return C_ERR;
integer_err:
    con.append(reply.integer_err);
    return C_ERR;
timeout_err:
    con.append(reply.timeout_err);
    return C_ERR;
}

// SET key value [EX seconds|PX milliseconds] [NX|XX]
void DB::setCommand(Context& con)
{
    unsigned cmdops = 0;
    int64_t expire;
    auto& cmdlist = con.commandList();
    auto& key = cmdlist[1];
    auto& value = cmdlist[2];
    if (parseSetArgs(con, cmdops, expire) == C_ERR)
        return;
    // [NX] [XX] 不能同时存在
    if ((cmdops & (SET_NX | SET_XX)))
        db_return(con, reply.syntax_err);
    if (cmdops & SET_NX) {
        if (isFound(find(key))) db_return(con, reply.nil);
    } else if (cmdops & SET_XX) {
        if (!isFound(find(key))) db_return(con, reply.nil);
    }
    insert(key, value);
    con.append(reply.ok);
    delExpireKey(key);
    touchWatchKey(key);
    if (cmdops & (SET_EX | SET_PX)) {
        addExpireKey(key, expire);
    }
}

// SETNX key value
void DB::setnxCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    auto& key = cmdlist[1];
    auto& value = cmdlist[2];
    if (isFound(find(key))) db_return(con, reply.n0);
    insert(key, value);
    touchWatchKey(key);
    con.append(reply.n1);
}

// GET key
void DB::getCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    auto& key = cmdlist[1];
    checkExpire(key);
    auto it = find(key);
    if (!isFound(it)) db_return(con, reply.nil);
    checkType(con, it, String);
    auto& value = getStringValue(it);
    con.appendReplyString(value);
}

// GETSET key value
void DB::getSetCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    auto& key = cmdlist[1];
    auto& new_value = cmdlist[2];
    checkExpire(key);
    touchWatchKey(key);
    auto it = find(key);
    if (!isFound(it)) {
        insert(key, new_value);
        db_return(con, reply.nil);
    }
    checkType(con, it, String);
    auto& old_value = getStringValue(it);
    con.appendReplyString(old_value);
    insert(key, new_value);
}

// STRLEN key
void DB::strlenCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    auto& key = cmdlist[1];
    checkExpire(key);
    auto it = find(key);
    if (!isFound(it)) db_return(con, reply.n0);
    checkType(con, it, String);
    auto& value = getStringValue(it);
    con.appendReplyNumber(value.size());
}

// APPEND key value
void DB::appendCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    auto& key = cmdlist[1];
    auto& value = cmdlist[2];
    checkExpire(key);
    touchWatchKey(key);
    auto it = find(key);
    if (!isFound(it)) {
        insert(key, value);
        con.appendReplyNumber(value.size());
        return;
    }
    checkType(con, it, String);
    auto& old_value = getStringValue(it);
    old_value.append(value);
    con.appendReplyNumber(old_value.size());
}

// MSET key value [key value ...]
void DB::msetCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    size_t size = cmdlist.size();
    if (size % 2 == 0) db_return(con, reply.argnumber_err);
    for (size_t i = 1; i < size; i += 2) {
        checkExpire(cmdlist[i]);
        insert(cmdlist[i], cmdlist[i+1]);
        touchWatchKey(cmdlist[i]);
    }
    con.append(reply.ok);
}

// MGET key [key ...]
void DB::mgetCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    size_t size = cmdlist.size();
    con.appendReplyMulti(size - 1);
    for (size_t i = 1; i < size; i++) {
        checkExpire(cmdlist[i]);
        auto it = find(cmdlist[i]);
        if (isFound(it)) {
            if (isType(it, String)) {
                auto& value = getStringValue(it);
                con.appendReplyString(value);
            } else
                con.append(reply.nil);
        } else
            con.append(reply.nil);
    }
}

void DB::incr(Context& con, int64_t incr)
{
    auto& cmdlist = con.commandList();
    auto& key = cmdlist[1];
    checkExpire(key);
    auto it = find(key);
    if (isFound(it)) {
        checkType(con, it, String);
        auto& value = getStringValue(it);
        auto number = str2ll(value.c_str());
        if (str2numberErr()) db_return(con, reply.integer_err);
        number += incr;
        insert(key, String(convert(number)));
        con.appendReplyNumber(number);
    } else {
        insert(key, String(convert(incr)));
        con.appendReplyNumber(incr);
    }
    touchWatchKey(key);
}

// INCR key
void DB::incrCommand(Context& con)
{
    incr(con, 1);
}

// INCRBY key increment
void DB::incrbyCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    auto increment = str2ll(cmdlist[2].c_str());
    if (str2numberErr()) db_return(con, reply.integer_err);
    incr(con, increment);
}

// DECR key
void DB::decrCommand(Context& con)
{
    incr(con, -1);
}

// DECRBY key decrement
void DB::decrbyCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    auto decrement = str2ll(cmdlist[2].c_str());
    if (str2numberErr()) db_return(con, reply.integer_err);
    incr(con, -decrement);
}

// SETRANGE key offset value
void DB::setRangeCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    auto& key = cmdlist[1];
    auto offset = str2l(cmdlist[2].c_str());
    auto& value = cmdlist[3];
    if (str2numberErr() || offset < 0)
        db_return(con, reply.integer_err);
    String new_value;
    checkExpire(key);
    touchWatchKey(key);
    auto it = find(key);
    if (!isFound(it)) {
        new_value.reserve(offset + value.size());
        new_value.resize(offset, '\x00');
        new_value.append(value);
        con.appendReplyNumber(new_value.size());
        insert(key, std::move(new_value));
        return;
    }
    checkType(con, it, String);
    new_value.swap(getStringValue(it));
    size_t size = offset + value.size();
    if (new_value.capacity() < size) new_value.reserve(size);
    if (offset < new_value.size()) {
        for (size_t i = offset; i < size; i++) {
            new_value[i] = value[i-offset];
        }
    } else {
        for (size_t i = new_value.size(); i < offset; i++)
            new_value[i] = '\x00';
        new_value.append(value);
    }
    con.appendReplyNumber(new_value.size());
    insert(key, std::move(new_value));
}

// GETRANGE key start end
void DB::getRangeCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    auto& key = cmdlist[1];
    int start = str2l(cmdlist[2].c_str());
    if (str2numberErr()) db_return(con, reply.integer_err);
    int stop = str2l(cmdlist[3].c_str());
    if (str2numberErr()) db_return(con, reply.integer_err);
    checkExpire(key);
    auto it = find(key);
    if (!isFound(it)) db_return(con, reply.nil);
    checkType(con, it, String);
    auto& value = getStringValue(it);
    int upper = value.size() - 1;
    int lower = -value.size();
    if (checkRange(con, start, stop, lower, upper) == C_ERR)
        return;
    auto result = value.substr(start, stop - start + 1);
    con.appendReplyString(result);
}
