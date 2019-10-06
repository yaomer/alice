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

void DB::setCommand(Context& con)
{
    unsigned cmdops = 0;
    int64_t expire = 0;
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
            if (str2numberErr()) db_return(con, db_return_integer_err);
            if (expire <= 0) db_return(con, db_return_timeout_err);
            if (op->second == SET_EX)
                expire *= 1000;
            break;
        }
        default:
            goto syntax_err;
        }
    }
    if ((cmdops & SET_NX) && (cmdops & SET_XX))
        goto syntax_err;
    if (cmdops & SET_NX) {
        if (isFound(find(cmdlist[1]))) db_return(con, db_return_nil);
        insert(cmdlist[1], cmdlist[2]);
        con.append(db_return_ok);
    } else if (cmdops & SET_XX) {
        if (!isFound(find(cmdlist[1]))) db_return(con, db_return_nil);
        insert(cmdlist[1], cmdlist[2]);
        con.append(db_return_ok);
    } else {
        insert(cmdlist[1], cmdlist[2]);
        con.append(db_return_ok);
    }
    delExpireKey(cmdlist[1]);
    touchWatchKey(cmdlist[1]);
    if (cmdops & (SET_EX | SET_PX)) {
        addExpireKey(cmdlist[1], expire);
    }
    return;
syntax_err:
    con.append(db_return_syntax_err);
}

void DB::setnxCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    if (!isFound(find(cmdlist[1]))) {
        insert(cmdlist[1], cmdlist[2]);
        touchWatchKey(cmdlist[1]);
        con.append(db_return_1);
    } else {
        con.append(db_return_0);
    }
}

void DB::getCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    expireIfNeeded(cmdlist[1]);
    auto it = find(cmdlist[1]);
    if (!isFound(it)) db_return(con, db_return_nil);
    checkType(con, it, String);
    auto& value = getStringValue(it);
    appendReplyString(con, value);
}

void DB::getSetCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    expireIfNeeded(cmdlist[1]);
    touchWatchKey(cmdlist[1]);
    auto it = find(cmdlist[1]);
    if (!isFound(it)) {
        insert(cmdlist[1], cmdlist[2]);
        db_return(con, db_return_nil);
    }
    checkType(con, it, String);
    String oldvalue = getStringValue(it);
    insert(cmdlist[1], cmdlist[2]);
    appendReplyString(con, oldvalue);
}

void DB::strlenCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    expireIfNeeded(cmdlist[1]);
    auto it = find(cmdlist[1]);
    if (!isFound(it)) db_return(con, db_return_0);
    checkType(con, it, String);
    String& value = getStringValue(it);
    appendReplyNumber(con, value.size());
}

void DB::appendCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    expireIfNeeded(cmdlist[1]);
    touchWatchKey(cmdlist[1]);
    auto it = find(cmdlist[1]);
    if (!isFound(it)) {
        insert(cmdlist[1], cmdlist[2]);
        appendReplyNumber(con, cmdlist[2].size());
        return;
    }
    checkType(con, it, String);
    String& string = getStringValue(it);
    string.append(cmdlist[2]);
    appendReplyNumber(con, string.size());
}

void DB::msetCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    size_t size = cmdlist.size();
    if (size % 2 == 0) db_return(con, db_return_argnumber_err);
    for (size_t i = 1; i < size; i += 2) {
        expireIfNeeded(cmdlist[i]);
        insert(cmdlist[i], cmdlist[i+1]);
        touchWatchKey(cmdlist[i]);
    }
    con.append(db_return_ok);
}

void DB::mgetCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    size_t size = cmdlist.size();
    appendReplyMulti(con, size - 1);
    for (size_t i = 1; i < size; i++) {
        expireIfNeeded(cmdlist[i]);
        auto it = find(cmdlist[i]);
        if (isFound(it)) {
            if (!isXXType(it, String)) {
                con.append(db_return_nil);
                continue;
            }
            String& value = getStringValue(it);
            appendReplyString(con, value);
        } else
            con.append(db_return_nil);
    }
}

void DB::incr(Context& con, int64_t incr)
{
    auto& cmdlist = con.commandList();
    expireIfNeeded(cmdlist[1]);
    auto it = find(cmdlist[1]);
    if (isFound(it)) {
        checkType(con, it, String);
        String& value = getStringValue(it);
        int64_t number = str2ll(value.c_str());
        if (str2numberErr()) db_return(con, db_return_integer_err);
        number += incr;
        insert(cmdlist[1], String(convert(number)));
        appendReplyNumber(con, number);
    } else {
        insert(cmdlist[1], String(convert(incr)));
        appendReplyNumber(con, incr);
    }
    touchWatchKey(cmdlist[1]);
}

void DB::incrCommand(Context& con)
{
    incr(con, 1);
}

void DB::incrbyCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    int64_t increment = str2ll(cmdlist[2].c_str());
    if (str2numberErr()) db_return(con, db_return_integer_err);
    incr(con, increment);
}

void DB::decrCommand(Context& con)
{
    incr(con, -1);
}

void DB::decrbyCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    int64_t decrement = str2ll(cmdlist[2].c_str());
    if (str2numberErr()) db_return(con, db_return_integer_err);
    incr(con, -decrement);
}

void DB::setRangeCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    int offset = str2l(cmdlist[2].c_str());
    if (str2numberErr() || offset < 0)
        db_return(con, db_return_integer_err);
    String string;
    String& value = cmdlist[3];
    expireIfNeeded(cmdlist[1]);
    touchWatchKey(cmdlist[1]);
    auto it = find(cmdlist[1]);
    if (!isFound(it)) {
        string.reserve(offset + value.size());
        string.resize(offset, '\x00');
        string.append(value);
        appendReplyNumber(con, string.size());
        insert(cmdlist[1], std::move(string));
        return;
    }
    checkType(con, it, String);
    string.swap(getStringValue(it));
    size_t size = offset + value.size();
    if (string.capacity() < size) string.reserve(size);
    if (offset < string.size()) {
        for (size_t i = offset; i < size; i++) {
            string[i] = value[i-offset];
        }
    } else {
        for (size_t i = string.size(); i < offset; i++)
            string[i] = '\x00';
        string.append(value);
    }
    appendReplyNumber(con, string.size());
    insert(cmdlist[1], std::move(string));
}

void DB::getRangeCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    int start = str2l(cmdlist[2].c_str());
    if (str2numberErr()) db_return(con, db_return_integer_err);
    int stop = str2l(cmdlist[3].c_str());
    if (str2numberErr()) db_return(con, db_return_integer_err);
    expireIfNeeded(cmdlist[1]);
    auto it = find(cmdlist[1]);
    if (!isFound(it)) db_return(con, db_return_nil);
    checkType(con, it, String);
    String& string = getStringValue(it);
    int upperbound = string.size() - 1;
    int lowerbound = -string.size();
    if (checkRange(con, &start, &stop, lowerbound, upperbound) == C_ERR)
        return;
    appendReplyString(con, string.substr(start, stop - start + 1));
}
