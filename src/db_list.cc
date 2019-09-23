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
        insert(cmdlist[1], std::move(list));
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
    if (!isFound(it)) db_return(con, db_return_0);
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
    if (!isFound(it)) db_return(con, db_return_nil);
    checkType(con, it, List);
    List& list = getListValue(it);
    if (option == LPOP) {
        appendReplyString(con, list.front());
        list.pop_front();
    } else {
        appendReplyString(con, list.back());
        list.pop_back();
    }
    if (list.empty()) delKeyWithExpire(cmdlist[1]);
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

#define BLOCK 1
#define NONBLOCK 2

void DB::rpoplpush(Context& con, int option)
{
    auto& cmdlist = con.commandList();
    size_t size = cmdlist.size();
    expireIfNeeded(cmdlist[1]);
    expireIfNeeded(cmdlist[2]);
    int timeout = 0;
    if (option == BLOCK) {
        timeout = str2l(cmdlist[size - 1].c_str());
        if (str2numberErr()) db_return(con, db_return_integer_err);
        if (timeout < 0) db_return(con, "-ERR timeout out of range\r\n");
    }
    auto src = find(cmdlist[1]);
    if (!isFound(src)) {
        if (option == NONBLOCK) db_return(con, db_return_nil);
        auto e = find(cmdlist[2]);
        if (isFound(e)) checkType(con, e, List);
        addBlockingKey(con, cmdlist[1]);
        con.des().assign(cmdlist[2]);
        setContextToBlock(con, timeout);
        return;
    }
    checkType(con, src, List);
    List& srclist = getListValue(src);
    appendReplyString(con, srclist.back());
    auto des = find(cmdlist[2]);
    if (isFound(des)) {
        checkType(con, des, List);
        List& deslist = getListValue(des);
        deslist.emplace_front(srclist.back());
        touchWatchKey(cmdlist[1]);
        touchWatchKey(cmdlist[2]);
    } else {
        List deslist;
        deslist.emplace_front(srclist.back());
        insert(cmdlist[2], deslist);
        touchWatchKey(cmdlist[1]);
    }
    srclist.pop_back();
    if (srclist.empty()) delKeyWithExpire(cmdlist[1]);
}

void DB::rpoplpushCommand(Context& con)
{
    rpoplpush(con, NONBLOCK);
}

void DB::lremCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    expireIfNeeded(cmdlist[1]);
    int count = str2l(cmdlist[2].c_str());
    if (str2numberErr()) db_return(con, db_return_integer_err);
    String& value = cmdlist[3];
    auto it = find(cmdlist[1]);
    if (!isFound(it)) db_return(con, db_return_0);
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
    if (list.empty()) delKeyWithExpire(cmdlist[1]);
    touchWatchKey(cmdlist[1]);
    appendReplyNumber(con, retval);
}

void DB::llenCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    expireIfNeeded(cmdlist[1]);
    auto it = find(cmdlist[1]);
    if (!isFound(it)) db_return(con, db_return_0);
    checkType(con, it, List);
    List& list = getListValue(it);
    appendReplyNumber(con, list.size());
}

void DB::lindexCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    int index = str2l(cmdlist[2].c_str());
    if (str2numberErr()) db_return(con, db_return_integer_err);
    expireIfNeeded(cmdlist[1]);
    auto it = find(cmdlist[1]);
    if (!isFound(it)) db_return(con, db_return_nil);
    checkType(con, it, List);
    List& list = getListValue(it);
    size_t size = list.size();
    if (index < 0)
        index += size;
    if (index >= static_cast<ssize_t>(size)) {
        db_return(con, db_return_nil);
    }
    for (auto& it : list)
        if (index-- == 0) {
            appendReplyString(con, it);
            break;
        }
}

void DB::lsetCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    int index = str2l(cmdlist[2].c_str());
    if (str2numberErr()) db_return(con, db_return_integer_err);
    expireIfNeeded(cmdlist[1]);
    auto it = find(cmdlist[1]);
    if (!isFound(it)) db_return(con, db_return_no_such_key);
    checkType(con, it, List);
    List& list = getListValue(it);
    size_t size = list.size();
    if (index < 0)
        index += size;
    if (index >= static_cast<ssize_t>(size)) {
        db_return(con, "-ERR index out of range\r\n");
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
    if (str2numberErr()) db_return(con, db_return_integer_err);
    int stop = str2l(cmdlist[3].c_str());
    if (str2numberErr()) db_return(con, db_return_integer_err);
    expireIfNeeded(cmdlist[1]);
    auto it = find(cmdlist[1]);
    if (!isFound(it)) db_return(con, db_return_nil);
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
        appendReplyString(con, it);
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
    if (str2numberErr()) db_return(con, db_return_integer_err);
    int stop = str2l(cmdlist[3].c_str());
    if (str2numberErr()) db_return(con, db_return_integer_err);
    expireIfNeeded(cmdlist[1]);
    auto it = find(cmdlist[1]);
    if (!isFound(it)) db_return(con, db_return_ok);
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
        db_return(con, db_return_ok);
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
    if (list.empty()) delKeyWithExpire(cmdlist[1]);
    touchWatchKey(cmdlist[1]);
    con.append(db_return_ok);
}

#define BLPOP 1
#define BRPOP 2

// 将一个blocking Key添加到DB::blockingKeys和Context::blockingKeys中
void DB::addBlockingKey(Context& con, const Key& key)
{
    con.blockingKeys().emplace_back(key);
    auto it = _blockingKeys.find(key);
    if (it != _blockingKeys.end()) {
        it->second.push_back(con.conn()->id());
    } else {
        std::list<size_t> idlist = { con.conn()->id() };
        _blockingKeys.emplace(key, std::move(idlist));
    }
}

void DB::setContextToBlock(Context& con, int timeout)
{
    con.setFlag(Context::CON_BLOCK);
    con.setBlockDbnum(_dbServer->curDbnum());
    con.setBlockTimeout(timeout);
    _dbServer->blockedClients().push_back(con.conn()->id());
}

void DB::blpop(Context& con, int option)
{
    auto& cmdlist = con.commandList();
    size_t size = cmdlist.size();
    int timeout = str2l(cmdlist[size - 1].c_str());
    if (str2numberErr()) db_return(con, db_return_integer_err);
    if (timeout < 0) db_return(con, "-ERR timeout out of range\r\n");
    for (size_t i = 1 ; i < size - 1; i++) {
        auto it = find(cmdlist[i]);
        if (isFound(it)) {
            checkType(con, it, List);
            List& list = getListValue(it);
            appendReplyMulti(con, 2);
            appendReplyString(con, cmdlist[i]);
            appendReplyString(con, option == BLPOP ? list.front() : list.back());
            option == BLPOP ? list.pop_front() : list.pop_back();
            if (list.empty()) delKeyWithExpire(cmdlist[i]);
            return;
        }
    }
    for (size_t j = 1; j < size - 1; j++) {
        addBlockingKey(con, cmdlist[j]);
    }
    setContextToBlock(con, timeout);
}

void DB::blpopCommand(Context& con)
{
    (con.flag() & Context::EXEC_MULTI) ? lpopCommand(con) : blpop(con, BLPOP);
}

void DB::brpopCommand(Context& con)
{
    (con.flag() & Context::EXEC_MULTI) ? rpopCommand(con) : blpop(con, BRPOP);
}

void DB::brpoplpushCommand(Context& con)
{
    (con.flag() & Context::EXEC_MULTI) ? rpoplpushCommand(con) : rpoplpush(con, BLOCK);
}

#define BLOCK_LPOP 1
#define BLOCK_RPOP 2
#define BLOCK_RPOPLPUSH 3

static unsigned getLastcmd(const std::string& lc)
{
    unsigned ops = 0;
    if (lc.compare("BLPOP") == 0) ops = BLOCK_LPOP;
    else if (lc.compare("BRPOP") == 0) ops = BLOCK_RPOP;
    else if (lc.compare("BRPOPLPUSH") == 0) ops = BLOCK_RPOPLPUSH;
    return ops;
}

void DB::blockMoveSrcToDes(const String& src, const String& des)
{
    auto it = find(des);
    if (isFound(it)) {
        List& list = getListValue(it);
        list.emplace_front(src);
    } else {
        List list;
        list.emplace_front(src);
        insert(des, std::move(list));
    }
}

// 正常弹出解除阻塞的键
void DB::blockingPop(const std::string& key)
{
    auto cl = _blockingKeys.find(key);
    if (cl == _blockingKeys.end()) return;
    unsigned bops = 0;
    Context other(_dbServer, nullptr);
    int64_t now = Angel::TimeStamp::now();
    auto& maps = g_server->server().connectionMaps();
    auto& value = getListValue(find(key));

    auto conn = maps.find(*cl->second.begin());
    if (conn == maps.end()) return;
    auto& context = std::any_cast<Context&>(conn->second->getContext());
    bops = getLastcmd(context.lastcmd());
    DB::appendReplyMulti(other, 2);
    DB::appendReplyString(other, key);
    DB::appendReplyString(other, (bops == BLOCK_LPOP) ? value.front() : value.back());
    double seconds = 1.0 * (now - context.blockStartTime()) / 1000;
    other.append("+(");
    other.append(convert2f(seconds));
    other.append("s)\r\n");
    conn->second->send(other.message());
    other.message().clear();

    clearBlockingKeysForContext(context);
    _dbServer->removeBlockedClient(conn->second->id());
    if (bops == BLOCK_RPOPLPUSH) {
        blockMoveSrcToDes(value.back(), context.des());
    }
    (bops == BLOCK_LPOP) ? value.pop_front() : value.pop_back();
    if (value.empty()) delKeyWithExpire(key);
    touchWatchKey(key);
}
