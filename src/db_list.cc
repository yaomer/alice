#include "server.h"
#include "db.h"

using namespace Alice;

#define LPUSH 1
#define RPUSH 2

void DB::lpush(Context& con, int option)
{
    auto& cmdlist = con.commandList();
    size_t size = cmdlist.size();
    expireIfNeeded(cmdlist[1]);
    auto it = find(cmdlist[1]);
    if (isFound(it)) {
        checkType(con, it, List);
        List& list = getListValue(it);
        for (size_t i = 2; i < size; i++) {
            option == LPUSH ? list.emplace_front(cmdlist[i])
                            : list.emplace_back(cmdlist[i]);
        }
        appendReplyNumber(con, list.size());
    } else {
        List list;
        for (size_t i = 2; i < size; i++) {
            option == LPUSH ? list.emplace_front(cmdlist[i])
                            : list.emplace_back(cmdlist[i]);
        }
        insert(cmdlist[1], list);
        appendReplyNumber(con, list.size());
    }
    touchWatchKey(cmdlist[1]);
}

void DB::lpushCommand(Context& con)
{
    lpush(con, LPUSH);
}

void DB::rpushCommand(Context& con)
{
    lpush(con, RPUSH);
}

#define LPUSHX 1
#define RPUSHX 2

void DB::lpushx(Context& con, int option)
{
    auto& cmdlist = con.commandList();
    expireIfNeeded(cmdlist[1]);
    auto it = find(cmdlist[1]);
    if (!isFound(it)) {
        con.append(db_return_0);
        return;
    }
    checkType(con, it, List);
    List& list = getListValue(it);
    option == LPUSHX ? list.emplace_front(cmdlist[2])
                     : list.emplace_back(cmdlist[2]);
    touchWatchKey(cmdlist[1]);
    appendReplyNumber(con, list.size());
}

void DB::lpushxCommand(Context& con)
{
    lpushx(con, LPUSHX);
}

void DB::rpushxCommand(Context& con)
{
    lpushx(con, RPUSHX);
}

#define LPOP 1
#define RPOP 2

void DB::lpop(Context& con, int option)
{
    auto& cmdlist = con.commandList();
    expireIfNeeded(cmdlist[1]);
    auto it = find(cmdlist[1]);
    if (!isFound(it)) {
        con.append(db_return_nil);
        return;
    }
    checkType(con, it, List);
    List& list = getListValue(it);
    if (list.empty()) {
        con.append(db_return_nil);
        return;
    }
    if (option == LPOP) {
        appendReplySingleStr(con, list.front());
        list.pop_front();
    } else {
        appendReplySingleStr(con, list.back());
        list.pop_back();
    }
    if (list.empty()) delKey(cmdlist[1]);
    touchWatchKey(cmdlist[1]);
}

void DB::lpopCommand(Context& con)
{
    lpop(con, LPOP);
}

void DB::rpopCommand(Context& con)
{
    lpop(con, RPOP);
}

void DB::rpoplpushCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    expireIfNeeded(cmdlist[1]);
    expireIfNeeded(cmdlist[2]);
    auto src = find(cmdlist[1]);
    if (!isFound(src)) {
        con.append(db_return_nil);
        return;
    }
    checkType(con, src, List);
    List& srclist = getListValue(src);
    if (srclist.empty()) {
        con.append(db_return_nil);
        return;
    }
    appendReplySingleStr(con, srclist.back());
    auto des = find(cmdlist[2]);
    if (isFound(des)) {
        checkType(con, des, List);
        List& deslist = getListValue(des);
        deslist.emplace_front(srclist.back());
        touchWatchKey(cmdlist[1]);
        touchWatchKey(cmdlist[2]);
    } else {
        srclist.emplace_front(srclist.back());
        touchWatchKey(cmdlist[1]);
    }
    srclist.pop_back();
    if (srclist.empty()) delKey(cmdlist[1]);
}

void DB::lremCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    expireIfNeeded(cmdlist[1]);
    int count = str2l(cmdlist[2].c_str());
    if (str2numberErr()) {
        con.append(db_return_integer_err);
        return;
    }
    String& value = cmdlist[3];
    auto it = find(cmdlist[1]);
    if (!isFound(it)) {
        con.append(db_return_0);
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
    if (list.empty()) delKey(cmdlist[1]);
    touchWatchKey(cmdlist[1]);
    appendReplyNumber(con, retval);
}

void DB::llenCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    expireIfNeeded(cmdlist[1]);
    auto it = find(cmdlist[1]);
    if (!isFound(it)) {
        con.append(db_return_0);
        return;
    }
    checkType(con, it, List);
    List& list = getListValue(it);
    appendReplyNumber(con, list.size());
}

void DB::lindexCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    int index = str2l(cmdlist[2].c_str());
    if (str2numberErr()) {
        con.append(db_return_integer_err);
        return;
    }
    expireIfNeeded(cmdlist[1]);
    auto it = find(cmdlist[1]);
    if (!isFound(it)) {
        con.append(db_return_nil);
        return;
    }
    checkType(con, it, List);
    List& list = getListValue(it);
    size_t size = list.size();
    if (index < 0)
        index += size;
    if (index >= static_cast<ssize_t>(size)) {
        con.append(db_return_nil);
        return;
    }
    for (auto& it : list)
        if (index-- == 0) {
            appendReplySingleStr(con, it);
            break;
        }
}

void DB::lsetCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    int index = str2l(cmdlist[2].c_str());
    if (str2numberErr()) {
        con.append(db_return_integer_err);
        return;
    }
    expireIfNeeded(cmdlist[1]);
    auto it = find(cmdlist[1]);
    if (!isFound(it)) {
        con.append(db_return_no_such_key);
        return;
    }
    checkType(con, it, List);
    List& list = getListValue(it);
    size_t size = list.size();
    if (index < 0)
        index += size;
    if (index >= static_cast<ssize_t>(size)) {
        con.append("-ERR index out of range\r\n");
        return;
    }
    for (auto& it : list)
        if (index-- == 0) {
            it.assign(cmdlist[3]);
            break;
        }
    touchWatchKey(cmdlist[1]);
    con.append(db_return_ok);
}

void DB::lrangeCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    int start = str2l(cmdlist[2].c_str());
    if (str2numberErr()) {
        con.append(db_return_integer_err);
        return;
    }
    int stop = str2l(cmdlist[3].c_str());
    if (str2numberErr()) {
        con.append(db_return_integer_err);
        return;
    }
    expireIfNeeded(cmdlist[1]);
    auto it = find(cmdlist[1]);
    if (!isFound(it)) {
        con.append(db_return_nil);
        return;
    }
    checkType(con, it, List);
    List& list = getListValue(it);
    int upperbound = list.size() - 1;
    int lowerbound = -list.size();
    if (checkRange(con, &start, &stop, lowerbound, upperbound) == C_ERR)
        return;
    appendReplyMulti(con, stop - start + 1);
    int i = 0;
    for (auto& it : list) {
        if (i < start) {
            i++;
            continue;
        }
        if (i > stop)
            break;
        appendReplySingleStr(con, it);
        i++;
    }
}

int DB::checkRange(Context& con, int *start, int *stop,
        int lowerbound, int upperbound)
{
    if (*start > upperbound || *stop < lowerbound) {
        con.append(db_return_nil);
        return C_ERR;
    }
    if (*start < 0 && *start >= lowerbound) {
        *start += upperbound + 1;
    }
    if (*stop < 0 && *stop >= lowerbound) {
        *stop += upperbound + 1;
    }
    if (*start < lowerbound) {
        *start = 0;
    }
    if (*stop > upperbound) {
        *stop = upperbound;
    }
    if (*start > *stop) {
        con.append(db_return_nil);
        return C_ERR;
    }
    return C_OK;
}

void DB::ltrimCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    int start = str2l(cmdlist[2].c_str());
    if (str2numberErr()) {
        con.append(db_return_integer_err);
        return;
    }
    int stop = str2l(cmdlist[3].c_str());
    if (str2numberErr()) {
        con.append(db_return_integer_err);
        return;
    }
    expireIfNeeded(cmdlist[1]);
    auto it = find(cmdlist[1]);
    if (!isFound(it)) {
        con.append(db_return_ok);
        return;
    }
    checkType(con, it, List);
    List& list = getListValue(it);
    size_t size = list.size();
    if (start < 0)
        start += size;
    if (stop < 0)
        stop += size;
    if (start > static_cast<ssize_t>(size) - 1
     || start > stop
     || stop > static_cast<ssize_t>(size) - 1) {
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
    if (list.empty()) delKey(cmdlist[1]);
    touchWatchKey(cmdlist[1]);
    con.append(db_return_ok);
}
