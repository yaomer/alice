#include "server.h"
#include "db.h"

using namespace Alice;

// L(R)PUSH key value [value ...]
void DB::lpush(Context& con, bool is_lpush)
{
    auto& cmdlist = con.commandList();
    size_t size = cmdlist.size();
    auto& key = cmdlist[1];
    checkExpire(key);
    auto it = find(key);
    if (isFound(it)) {
        checkType(con, it, List);
        auto& list = getListValue(it);
        for (size_t i = 2; i < size; i++) {
            is_lpush ? list.emplace_front(cmdlist[i])
                     : list.emplace_back(cmdlist[i]);
        }
        con.appendReplyNumber(list.size());
    } else {
        List list;
        for (size_t i = 2; i < size; i++) {
            is_lpush ? list.emplace_front(cmdlist[i])
                     : list.emplace_back(cmdlist[i]);
        }
        insert(key, std::move(list));
        con.appendReplyNumber(list.size());
    }
    touchWatchKey(key);
}

void DB::lpushCommand(Context& con)
{
    lpush(con, true);
}

void DB::rpushCommand(Context& con)
{
    lpush(con, false);
}

// L(R)PUSHX key value
void DB::lpushx(Context& con, bool is_lpushx)
{
    auto& cmdlist = con.commandList();
    auto& key = cmdlist[1];
    auto& value = cmdlist[2];
    checkExpire(key);
    auto it = find(key);
    if (!isFound(it)) db_return(con, reply.n0);
    checkType(con, it, List);
    auto& list = getListValue(it);
    is_lpushx ? list.emplace_front(value)
              : list.emplace_back(value);
    touchWatchKey(key);
    con.appendReplyNumber(list.size());
}

void DB::lpushxCommand(Context& con)
{
    lpushx(con, true);
}

void DB::rpushxCommand(Context& con)
{
    lpushx(con, false);
}

// L(R)POP key
void DB::lpop(Context& con, bool is_lpop)
{
    auto& cmdlist = con.commandList();
    auto& key = cmdlist[1];
    checkExpire(key);
    auto it = find(key);
    if (!isFound(it)) db_return(con, reply.nil);
    checkType(con, it, List);
    auto& list = getListValue(it);
    if (is_lpop) {
        con.appendReplyString(list.front());
        list.pop_front();
    } else {
        con.appendReplyString(list.back());
        list.pop_back();
    }
    checkEmpty(list, key);
    touchWatchKey(key);
}

void DB::lpopCommand(Context& con)
{
    lpop(con, true);
}

void DB::rpopCommand(Context& con)
{
    lpop(con, false);
}

// RPOPLPUSH source destination
// BRPOPLPUSH source destination timeout
void DB::rpoplpush(Context& con, bool is_nonblock)
{
    auto& cmdlist = con.commandList();
    size_t size = cmdlist.size();
    auto& src_key = cmdlist[1];
    auto& des_key = cmdlist[2];
    checkExpire(src_key);
    checkExpire(des_key);
    int timeout = 0;
    if (!is_nonblock) {
        timeout = str2l(cmdlist[size - 1].c_str());
        if (str2numberErr()) db_return(con, reply.integer_err);
        if (timeout < 0) db_return(con, reply.timeout_out_of_range);
    }
    auto src_it = find(src_key);
    if (!isFound(src_it)) {
        if (is_nonblock) db_return(con, reply.nil);
        auto e = find(des_key);
        if (isFound(e)) checkType(con, e, List);
        addBlockingKey(con, src_key);
        con.des().assign(des_key);
        setContextToBlock(con, timeout);
        return;
    }
    checkType(con, src_it, List);
    auto& src_list = getListValue(src_it);
    con.appendReplyString(src_list.back());
    auto des_it = find(des_key);
    if (isFound(des_it)) {
        checkType(con, des_it, List);
        auto& des_list = getListValue(des_it);
        des_list.emplace_front(src_list.back());
        touchWatchKey(src_key);
        touchWatchKey(des_key);
    } else {
        List des_list;
        des_list.emplace_front(src_list.back());
        insert(des_key, std::move(des_list));
        touchWatchKey(src_key);
    }
    src_list.pop_back();
    checkEmpty(src_list, src_key);
}

void DB::rpoplpushCommand(Context& con)
{
    rpoplpush(con, true);
}

// LREM key count value
void DB::lremCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    auto& key = cmdlist[1];
    auto& value = cmdlist[3];
    checkExpire(key);
    int count = str2l(cmdlist[2].c_str());
    if (str2numberErr()) db_return(con, reply.integer_err);
    auto it = find(key);
    if (!isFound(it)) db_return(con, reply.n0);
    checkType(con, it, List);
    auto& list = getListValue(it);
    int rems = 0;
    if (count > 0) {
        for (auto it = list.cbegin(); it != list.cend(); ) {
            if ((*it).compare(value) == 0) {
                auto tmp = it++;
                list.erase(tmp);
                rems++;
                if (--count == 0)
                    break;
            } else
                ++it;
        }
    } else if (count < 0) {
        for (auto it = list.crbegin(); it != list.crend(); ++it) {
            if ((*it).compare(value) == 0) {
                // &*(reverse_iterator(i)) == &*(i - 1)
                list.erase((++it).base());
                rems++;
                if (++count == 0)
                    break;
            }
        }
    } else {
        for (auto it = list.cbegin(); it != list.cend(); ) {
            if ((*it).compare(value) == 0) {
                auto tmp = it++;
                list.erase(tmp);
                rems++;
            }
        }
    }
    checkEmpty(list, key);
    touchWatchKey(key);
    con.appendReplyNumber(rems);
}

// LLEN key
void DB::llenCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    auto& key = cmdlist[1];
    checkExpire(key);
    auto it = find(key);
    if (!isFound(it)) db_return(con, reply.n0);
    checkType(con, it, List);
    auto& list = getListValue(it);
    con.appendReplyNumber(list.size());
}

// LINDEX key index
void DB::lindexCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    auto& key = cmdlist[1];
    int index = str2l(cmdlist[2].c_str());
    if (str2numberErr()) db_return(con, reply.integer_err);
    checkExpire(key);
    auto it = find(key);
    if (!isFound(it)) db_return(con, reply.nil);
    checkType(con, it, List);
    auto& list = getListValue(it);
    size_t size = list.size();
    if (index < 0)
        index += size;
    if (index >= static_cast<ssize_t>(size)) {
        db_return(con, reply.nil);
    }
    for (auto& it : list)
        if (index-- == 0) {
            con.appendReplyString(it);
            break;
        }
}

// LSET key index value
void DB::lsetCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    auto& key = cmdlist[1];
    auto& value = cmdlist[3];
    int index = str2l(cmdlist[2].c_str());
    if (str2numberErr()) db_return(con, reply.integer_err);
    checkExpire(key);
    auto it = find(key);
    if (!isFound(it)) db_return(con, reply.no_such_key);
    checkType(con, it, List);
    auto& list = getListValue(it);
    size_t size = list.size();
    if (index < 0)
        index += size;
    if (index >= static_cast<ssize_t>(size)) {
        db_return(con, reply.index_out_of_range);
    }
    for (auto& it : list)
        if (index-- == 0) {
            it.assign(value);
            break;
        }
    touchWatchKey(key);
    con.append(reply.ok);
}

// LRANGE key start stop
void DB::lrangeCommand(Context& con)
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
    checkType(con, it, List);
    auto& list = getListValue(it);
    int upper = list.size() - 1;
    int lower = -list.size();
    if (checkRange(con, start, stop, lower, upper) == C_ERR)
        return;
    con.appendReplyMulti(stop - start + 1);
    int i = 0;
    for (auto& it : list) {
        if (i < start) {
            i++;
            continue;
        }
        if (i > stop)
            break;
        con.appendReplyString(it);
        i++;
    }
}

// LTRIM key start stop
void DB::ltrimCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    auto& key = cmdlist[1];
    int start = str2l(cmdlist[2].c_str());
    if (str2numberErr()) db_return(con, reply.integer_err);
    int stop = str2l(cmdlist[3].c_str());
    if (str2numberErr()) db_return(con, reply.integer_err);
    checkExpire(key);
    auto it = find(key);
    if (!isFound(it)) db_return(con, reply.ok);
    checkType(con, it, List);
    auto& list = getListValue(it);
    size_t size = list.size();
    if (start < 0)
        start += size;
    if (stop < 0)
        stop += size;
    if (start > static_cast<ssize_t>(size) - 1
     || start > stop
     || stop > static_cast<ssize_t>(size) - 1) {
        list.clear();
        db_return(con, reply.ok);
    }
    int i = 0;
    for (auto it = list.cbegin(); it != list.cend(); i++) {
        auto tmp = it++;
        if (i >= start && i <= stop) continue;
        list.erase(tmp);
    }
    checkEmpty(list, key);
    touchWatchKey(key);
    con.append(reply.ok);
}

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

// BLPOP key [key ...] timeout
void DB::blpop(Context& con, bool is_blpop)
{
    auto& cmdlist = con.commandList();
    size_t size = cmdlist.size();
    int timeout = str2l(cmdlist[size - 1].c_str());
    if (str2numberErr()) db_return(con, reply.integer_err);
    if (timeout < 0) db_return(con, reply.timeout_out_of_range);
    for (size_t i = 1 ; i < size - 1; i++) {
        auto& key = cmdlist[i];
        auto it = find(key);
        if (isFound(it)) {
            checkType(con, it, List);
            auto& list = getListValue(it);
            con.appendReplyMulti(2);
            con.appendReplyString(key);
            con.appendReplyString(is_blpop ? list.front() : list.back());
            is_blpop ? list.pop_front() : list.pop_back();
            checkEmpty(list, key);
            return;
        }
    }
    for (size_t i = 1; i < size - 1; i++) {
        addBlockingKey(con, cmdlist[i]);
    }
    setContextToBlock(con, timeout);
}

void DB::blpopCommand(Context& con)
{
    (con.flag() & Context::EXEC_MULTI) ? lpopCommand(con) : blpop(con, true);
}

void DB::brpopCommand(Context& con)
{
    (con.flag() & Context::EXEC_MULTI) ? rpopCommand(con) : blpop(con, false);
}

void DB::brpoplpushCommand(Context& con)
{
    (con.flag() & Context::EXEC_MULTI) ? rpoplpushCommand(con) : rpoplpush(con, false);
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
        auto& list = getListValue(it);
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
    int64_t now = Angel::nowMs();
    auto& list = getListValue(find(key));

    auto conn = g_server->server().getConnection(*cl->second.begin());
    if (!conn) return;
    auto& context = std::any_cast<Context&>(conn->getContext());
    bops = getLastcmd(context.lastcmd());
    other.appendReplyMulti(2);
    other.appendReplyString(key);
    other.appendReplyString((bops == BLOCK_LPOP) ? list.front() : list.back());
    double seconds = 1.0 * (now - context.blockStartTime()) / 1000;
    other.append("+(");
    other.append(convert2f(seconds));
    other.append("s)\r\n");
    conn->send(other.reply());
    other.reply().clear();

    clearBlockingKeysForContext(context);
    _dbServer->removeBlockedClient(conn->id());
    if (bops == BLOCK_RPOPLPUSH) {
        blockMoveSrcToDes(list.back(), context.des());
    }
    (bops == BLOCK_LPOP) ? list.pop_front() : list.pop_back();
    checkEmpty(list, key);
    touchWatchKey(key);
}
