#include <stdlib.h>
#include <time.h>
#include <ctype.h>
#include <unistd.h>
#include <map>
#include <vector>
#include <list>
#include <functional>
#include <random>
#include <tuple>
#include "db.h"
#include "server.h"
#include "config.h"

using namespace Alice;
using std::placeholders::_1;

#define BIND(f) std::bind(&DB::f, this, _1)

DB::DB(DBServer *dbServer)
    : _dbServer(dbServer)
{
    _commandMap = {
        { "SET",        {  3, IS_WRITE, BIND(setCommand) } },
        { "SETNX",      { -3, IS_WRITE, BIND(setnxCommand) } },
        { "GET",        { -2, IS_READ,  BIND(getCommand) } },
        { "GETSET",     { -3, IS_WRITE, BIND(getSetCommand) } },
        { "APPEND",     { -3, IS_WRITE, BIND(appendCommand) } },
        { "STRLEN",     { -2, IS_READ,  BIND(strlenCommand) } },
        { "MSET",       {  3, IS_WRITE, BIND(msetCommand) } },
        { "MGET",       {  2, IS_READ,  BIND(mgetCommand) } },
        { "INCR",       { -2, IS_WRITE, BIND(incrCommand) } },
        { "INCRBY",     { -3, IS_WRITE, BIND(incrbyCommand) } },
        { "DECR",       { -2, IS_WRITE, BIND(decrCommand) } },
        { "DECRBY",     { -3, IS_WRITE, BIND(decrbyCommand) } },
        { "LPUSH",      {  3, IS_WRITE, BIND(lpushCommand) } },
        { "LPUSHX",     { -3, IS_WRITE, BIND(lpushxCommand) } },
        { "RPUSH",      {  3, IS_WRITE, BIND(rpushCommand) } },
        { "RPUSHX",     { -3, IS_WRITE, BIND(rpushxCommand) } },
        { "LPOP",       { -2, IS_WRITE, BIND(lpopCommand) } },
        { "RPOP",       { -2, IS_WRITE, BIND(rpopCommand) } },
        { "RPOPLPUSH",  { -3, IS_WRITE, BIND(rpoplpushCommand) } },
        { "LREM",       { -4, IS_WRITE, BIND(lremCommand) } },
        { "LLEN",       { -2, IS_READ,  BIND(llenCommand) } },
        { "LINDEX",     { -3, IS_READ,  BIND(lindexCommand) } },
        { "LSET",       { -4, IS_WRITE, BIND(lsetCommand) } },
        { "LRANGE",     { -4, IS_READ,  BIND(lrangeCommand) } },
        { "LTRIM",      { -4, IS_WRITE, BIND(ltrimCommand) } },
        { "SADD",       {  3, IS_WRITE, BIND(saddCommand) } },
        { "SISMEMBER",  { -3, IS_READ,  BIND(sisMemberCommand) } },
        { "SPOP",       { -2, IS_WRITE, BIND(spopCommand) } },
        { "SRANDMEMBER",{  2, IS_READ,  BIND(srandMemberCommand) } },
        { "SREM",       {  3, IS_WRITE, BIND(sremCommand)  } },
        { "SMOVE",      { -4, IS_WRITE, BIND(smoveCommand) } },
        { "SCARD",      { -2, IS_READ,  BIND(scardCommand) } },
        { "SMEMBERS",   { -2, IS_READ,  BIND(smembersCommand) } },
        { "SINTER",     {  2, IS_READ,  BIND(sinterCommand) } },
        { "SINTERSTORE",{  3, IS_WRITE, BIND(sinterStoreCommand) } },
        { "SUNION",     {  2, IS_READ,  BIND(sunionCommand) } },
        { "SUNIONSTORE",{  3, IS_WRITE, BIND(sunionStoreCommand) } },
        { "SDIFF",      {  2, IS_READ,  BIND(sdiffCommand) } },
        { "SDIFFSTORE", {  3, IS_WRITE, BIND(sdiffStoreCommand) } },
        { "HSET",       { -4, IS_WRITE, BIND(hsetCommand) } },
        { "HSETNX",     { -4, IS_WRITE, BIND(hsetnxCommand) } },
        { "HGET",       { -3, IS_READ,  BIND(hgetCommand) } },
        { "HEXISTS",    { -3, IS_READ,  BIND(hexistsCommand) } },
        { "HDEL",       {  3, IS_WRITE, BIND(hdelCommand) } },
        { "HLEN",       { -2, IS_READ,  BIND(hlenCommand) } },
        { "HSTRLEN",    { -3, IS_READ,  BIND(hstrlenCommand) } },
        { "HINCRBY",    { -4, IS_WRITE, BIND(hincrbyCommand) } },
        { "HMSET",      {  4, IS_WRITE, BIND(hmsetCommand) } },
        { "HMGET",      {  3, IS_READ,  BIND(hmgetCommand) } },
        { "HKEYS",      { -2, IS_READ,  BIND(hkeysCommand) } },
        { "HVALS",      { -2, IS_READ,  BIND(hvalsCommand) } },
        { "HGETALL",    { -2, IS_READ,  BIND(hgetAllCommand) } },
        { "EXISTS",     { -2, IS_READ,  BIND(existsCommand) } },
        { "TYPE",       { -2, IS_READ,  BIND(typeCommand) } },
        { "TTL",        { -2, IS_READ,  BIND(ttlCommand) } },
        { "PTTL",       { -2, IS_READ,  BIND(pttlCommand) } },
        { "EXPIRE",     { -3, IS_WRITE, BIND(expireCommand) } },
        { "PEXPIRE",    { -3, IS_WRITE, BIND(pexpireCommand) } },
        { "DEL",        {  2, IS_WRITE, BIND(delCommand) } },
        { "KEYS",       { -2, IS_READ,  BIND(keysCommand) } },
        { "SAVE",       { -1, IS_READ,  BIND(saveCommand) } },
        { "BGSAVE",     { -1, IS_READ,  BIND(bgSaveCommand) } },
        { "BGREWRITEAOF",{-1, IS_READ,  BIND(bgRewriteAofCommand) } },
        { "LASTSAVE",   { -1, IS_READ,  BIND(lastSaveCommand) } },
        { "FLUSHDB",    { -1, IS_WRITE, BIND(flushdbCommand) } },
        { "FLUSHALL",   { -1, IS_WRITE, BIND(flushAllCommand) } },
        { "SLAVEOF",    { -3, IS_READ,  BIND(slaveofCommand) } },
        { "PSYNC",      { -3, IS_READ,  BIND(psyncCommand) } },
        { "REPLCONF",   {  3, IS_READ,  BIND(replconfCommand) } },
        { "PING",       { -1, IS_READ,  BIND(pingCommand) } },
        { "PONG",       { -1, IS_READ,  BIND(pongCommand) } },
        { "MULTI",      { -1, IS_READ,  BIND(multiCommand) } },
        { "EXEC",       { -1, IS_READ,  BIND(execCommand) } },
        { "DISCARD",    { -1, IS_READ,  BIND(discardCommand) } },
        { "WATCH",      {  2, IS_READ,  BIND(watchCommand) } },
        { "UNWATCH",    { -1, IS_READ,  BIND(unwatchCommand) } },
        { "PUBLISH",    { -3, IS_READ,  BIND(publishCommand) } },
        { "SUBSCRIBE",  {  2, IS_READ,  BIND(subscribeCommand) } },
        { "INFO",       { -1, IS_READ,  BIND(infoCommand) } },
        { "SELECT",     { -2, IS_WRITE, BIND(selectCommand) } },
        { "DBSIZE",     { -1, IS_READ,  BIND(dbsizeCommand) } },
    };
}

namespace Alice {

    static const char *db_return_ok = "+OK\r\n";
    static const char *db_return_syntax_err = "-ERR syntax error\r\n";
    static const char *db_return_nil = "+(nil)\r\n";
    static const char *db_return_integer_0 = ": 0\r\n";
    static const char *db_return_integer_1 = ": 1\r\n";
    static const char *db_return_integer__1 = ": -1\r\n";
    static const char *db_return_integer__2 = ": -2\r\n";
    static const char *db_return_type_err = "-WRONGTYPE Operation"
        " against a key holding the wrong kind of value\r\n";
    static const char *db_return_interger_err = "-ERR value is"
        " not an integer or out of range\r\n";
    static const char *db_return_no_such_key = "-ERR no such key\r\n";
    static const char *db_return_out_of_range = "-ERR index out of range\r\n";
    static const char *db_return_string_type = "+string\r\n";
    static const char *db_return_list_type = "+list\r\n";
    static const char *db_return_set_type = "+set\r\n";
    static const char *db_return_none_type = "+none\r\n";
    static const char *db_return_unknown_option = "-ERR unknown option\r\n";
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

void DB::appendReplyMulti(Context& con, size_t size)
{
    con.append("*");
    con.append(convert(size));
    con.append("\r\n");
}

void DB::appendReplySingleStr(Context& con, const std::string& s)
{
    con.append("$");
    con.append(convert(s.size()));
    con.append("\r\n" + s + "\r\n");
}

void DB::appendReplySingleLen(Context& con, size_t size)
{
    con.append("$");
    con.append(convert(strlen(convert(size))));
    con.append("\r\n");
    con.append(convert(size));
    con.append("\r\n");
}

void DB::appendReplyNumber(Context& con, int64_t number)
{
    con.append(": ");
    con.append(convert(number));
    con.append("\r\n");
}

//////////////////////////////////////////////////////////////////

void DB::selectCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    if (!_strIsNumber(cmdlist[1])) {
        con.append("-ERR invalid DB index\r\n");
        return;
    }
    int dbnum = atoi(cmdlist[1].c_str());
    if (dbnum < 0 || dbnum >= g_server_conf.databases) {
        con.append("-ERR DB index is out of range\r\n");
        return;
    }
    con.db()->switchDb(dbnum);
    con.append(db_return_ok);
}

void DB::existsCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    isExpiredKey(cmdlist[1]);
    auto it = _hashMap.find(cmdlist[1]);
    if (it != _hashMap.end()) {
        con.append(db_return_integer_1);
    } else {
        con.append(db_return_integer_0);
    }
}

void DB::typeCommand(Context& con)
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

void DB::_ttl(Context& con, bool seconds)
{
    auto& cmdlist = con.commandList();
    auto it = _hashMap.find(cmdlist[1]);
    if (it == _hashMap.end()) {
        con.append(db_return_integer__2);
        return;
    }
    auto expire = _expireMap.find(cmdlist[1]);
    if (expire == _expireMap.end()) {
        con.append(db_return_integer__1);
        return;
    }
    int64_t milliseconds = expire->second - Angel::TimeStamp::now();
    if (seconds)
        milliseconds /= 1000;
    appendReplyNumber(con, milliseconds);
}

void DB::ttlCommand(Context& con)
{
    _ttl(con, true);
}

void DB::pttlCommand(Context& con)
{
    _ttl(con, false);
}

void DB::_expire(Context& con, bool seconds)
{
    auto& cmdlist = con.commandList();
    auto it = _hashMap.find(cmdlist[1]);
    if (it != _hashMap.end()) {
        if (!_strIsNumber(cmdlist[2])) {
            con.append(db_return_interger_err);
            return;
        }
        int64_t expire = atoll(cmdlist[2].c_str());
        if (seconds) expire *= 1000;
        addExpireKey(cmdlist[1], expire);
        con.append(db_return_integer_1);
    } else {
        con.append(db_return_integer_0);
    }
}

void DB::expireCommand(Context& con)
{
    _expire(con, true);
}

void DB::pexpireCommand(Context& con)
{
    _expire(con, false);
}

void DB::delCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    size_t size = cmdlist.size();
    int retval = 0;
    for (auto i = 1; i < size; i++) {
        auto it = _hashMap.find(cmdlist[i]);
        if (it != _hashMap.end()) {
            delExpireKey(cmdlist[1]);
            delKey(cmdlist[1]);
            retval++;
        }
    }
    appendReplyNumber(con, retval);
}

void DB::keysCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    if (cmdlist[1].compare("*")) {
        con.append(db_return_unknown_option);
        return;
    }
    if (_hashMap.empty()) {
        con.append(db_return_nil);
        return;
    }
    appendReplyMulti(con, _hashMap.size());
    size_t size = con.message().size();
    for (auto& it : _hashMap) {
        isExpiredKey(it.first);
        appendReplySingleStr(con, it.first);
    }
    if (size == con.message().size())
        con.assign(db_return_nil);
}

void DB::saveCommand(Context& con)
{
    if (_dbServer->rdb()->childPid() != -1)
        return;
    _dbServer->rdb()->save();
    con.append(db_return_ok);
    _dbServer->setLastSaveTime(Angel::TimeStamp::now());
    _dbServer->dirtyReset();
}

void DB::bgSaveCommand(Context& con)
{
    // 有rdb或aof持久化在进行中，忽略此次请求
    if (_dbServer->aof()->childPid() != -1) {
        con.append("+Background append only file rewriting ...\r\n");
        return;
    }
    if (_dbServer->rdb()->childPid() != -1)
        return;
    _dbServer->rdb()->saveBackground();
    con.append("+Background saving started\r\n");
    _dbServer->setLastSaveTime(Angel::TimeStamp::now());
    _dbServer->dirtyReset();
}

void DB::bgRewriteAofCommand(Context& con)
{
    // 有rdb持久化在进行中，延迟此次bgrewriteaof请求，即在rdb持久化完成后，
    // 再进行一次aof持久化
    if (_dbServer->rdb()->childPid() != -1) {
        _dbServer->setFlag(DBServer::REWRITEAOF_DELAY);
        return;
    }
    // 有aof持久化在进行中，忽略此次请求
    if (_dbServer->aof()->childPid() != -1) {
        return;
    }
    _dbServer->aof()->rewriteBackground();
    con.append("+Background append only file rewriting started\r\n");
    _dbServer->setLastSaveTime(Angel::TimeStamp::now());
}

void DB::lastSaveCommand(Context& con)
{
    appendReplyNumber(con, _dbServer->lastSaveTime());
}

void DB::flushdbCommand(Context& con)
{
    _hashMap.clear();
    con.append(db_return_ok);
}

void DB::flushAllCommand(Context& con)
{
    for (auto& db : _dbServer->dbs())
        db->hashMap().clear();
    con.append(db_return_ok);
}

void DB::slaveofCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    if (_strIsNumber(cmdlist[2])) {
        _dbServer->setMasterAddr(
                Angel::InetAddr(atoi(cmdlist[2].c_str()), cmdlist[1].c_str()));
        _dbServer->connectMasterServer();
        con.append(db_return_ok);
    } else
        con.append(db_return_interger_err);
}

void DB::psyncCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    if (cmdlist[1].compare("?") == 0 && cmdlist[2].compare("-1") == 0) {
sync:
        // 执行完整重同步
        con.setFlag(Context::SYNC_RDB_FILE);
        con.append("+FULLRESYNC\r\n");
        con.append(_dbServer->selfRunId());
        con.append("\r\n");
        con.append(convert(_dbServer->masterOffset()));
        con.append("\r\n");
        if (_dbServer->rdb()->childPid() != -1) {
            // 虽然服务器后台正在生成rdb快照，但没有从服务器在等待，即服务器并没有
            // 记录此期间执行的写命令，所以之后仍然需要重新生成一次rdb快照
            if (!(_dbServer->flag() & DBServer::PSYNC))
                _dbServer->setFlag(DBServer::PSYNC_DELAY);
            return;
        }
        _dbServer->setFlag(DBServer::PSYNC);
        _dbServer->rdb()->saveBackground();
    } else {
        if (cmdlist[1].compare(_dbServer->selfRunId()))
            goto sync;
        size_t offset = atoll(cmdlist[2].c_str());
        ssize_t lastoffset = _dbServer->masterOffset() - offset;
        if (lastoffset < 0 || lastoffset > g_server_conf.repl_backlog_size)
            goto sync;
        // 执行部分重同步
        con.setFlag(Context::SYNC_COMMAND);
        con.append("+CONTINUE\r\n");
        if (lastoffset > 0) {
            size_t start = g_server_conf.repl_backlog_size - lastoffset;
            con.append(std::string(
                        &_dbServer->copyBacklogBuffer()[start], lastoffset));
        }
    }
}

void DB::replconfCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    if (strcasecmp(cmdlist[1].c_str(), "port") == 0) {
        auto it = _dbServer->slaveIds().find(con.conn()->id());
        if (it == _dbServer->slaveIds().end())
            _dbServer->addSlaveId(con.conn()->id());
        con.setSlaveAddr(
                Angel::InetAddr(atoi(cmdlist[2].c_str()), cmdlist[4].c_str()));
    } else if (strcasecmp(cmdlist[1].c_str(), "ack") == 0) {
        size_t offset = atoll(cmdlist[2].c_str());
        ssize_t lastoffset = _dbServer->masterOffset() - offset;
        if (lastoffset > 0) {
            if (lastoffset > g_server_conf.repl_backlog_size) {
                // TODO: 重新同步
            } else {
                // 重传丢失的命令
                size_t start = g_server_conf.repl_backlog_size - lastoffset;
                con.append(std::string(
                            &_dbServer->copyBacklogBuffer()[start], lastoffset));
            }
        }
    }
}

void DB::pingCommand(Context& con)
{
    con.append("*1\r\n$4\r\nPONG\r\n");
}

void DB::pongCommand(Context& con)
{
    int64_t now = Angel::TimeStamp::now();
    if (_dbServer->lastRecvHeartBeatTime() == 0) {
        _dbServer->setLastRecvHeartBeatTime(now);
        return;
    }
    if (now - _dbServer->lastRecvHeartBeatTime() > g_server_conf.repl_timeout) {
        _dbServer->connectMasterServer();
    } else
        _dbServer->setLastRecvHeartBeatTime(now);
}

void DB::multiCommand(Context& con)
{
    con.setFlag(Context::EXEC_MULTI);
    con.append(db_return_ok);
}

void DB::execCommand(Context& con)
{
    Context::CommandList tlist = { "MULTI" };
    if (con.flag() & Context::EXEC_MULTI_ERR) {
        con.clearFlag(Context::EXEC_MULTI_ERR);
        con.append(db_return_nil);
        goto end;
    }
    if (con.flag() & Context::EXEC_MULTI_WRITE)
        _dbServer->doWriteCommand(tlist);
    for (auto& cmdlist : con.transactionList()) {
        auto command = _commandMap.find(cmdlist[0]);
        con.commandList().swap(cmdlist);
        command->second._commandCb(con);
        if (con.flag() & Context::EXEC_MULTI_WRITE)
            _dbServer->doWriteCommand(con.commandList());
    }
    if (con.flag() & Context::EXEC_MULTI_WRITE) {
        tlist = { "EXEC" };
        _dbServer->doWriteCommand(tlist);
        con.clearFlag(Context::EXEC_MULTI_WRITE);
    }
    unwatchKeys();
end:
    con.transactionList().clear();
    con.clearFlag(Context::EXEC_MULTI);
}

void DB::discardCommand(Context& con)
{
    con.transactionList().clear();
    unwatchKeys();
    con.clearFlag(Context::EXEC_MULTI);
    if (con.flag() & Context::EXEC_MULTI_ERR)
        con.clearFlag(Context::EXEC_MULTI_ERR);
    if (con.flag() & Context::EXEC_MULTI_WRITE)
        con.clearFlag(Context::EXEC_MULTI_WRITE);
    con.append(db_return_ok);
}

void DB::watchCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    for (int i = 1; i < cmdlist.size(); i++) {
        isExpiredKey(cmdlist[i]);
        auto it = _hashMap.find(cmdlist[i]);
        if (it != _hashMap.end()) {
            watchKeyForClient(cmdlist[i], con.conn()->id());
        }
    }
    con.append(db_return_ok);
    return;
}

void DB::unwatchCommand(Context& con)
{
    unwatchKeys();
    con.append(db_return_ok);
}

void DB::publishCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    size_t subClients = _dbServer->pubMessage(cmdlist[2], cmdlist[1], con.conn()->id());
    appendReplyNumber(con, subClients);
}

void DB::subscribeCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    con.append("+Reading messages... (press Ctrl-C to quit)\r\n");
    for (int i = 1; i < cmdlist.size(); i++) {
        _dbServer->subChannel(cmdlist[i], con.conn()->id());
        appendReplyMulti(con, 3);
        con.append("$9\r\nsubscribe\r\n");
        appendReplySingleStr(con, cmdlist[i]);
        appendReplySingleLen(con, i);
    }
}

void DB::infoCommand(Context& con)
{
    int i = 0;
    con.append("+run_id:");
    con.append(_dbServer->selfRunId());
    con.append("\n");
    con.append("role:");
    if (_dbServer->flag() & DBServer::SLAVE) {
        con.append("slave\r\n");
        return;
    } else
        con.append("master\n");
    con.append("connected_slaves:");
    con.append(convert(_dbServer->slaveIds().size()));
    con.append("\n");
    auto& maps = g_server->server().connectionMaps();
    for (auto& id : _dbServer->slaveIds()) {
        auto conn = maps.find(id);
        if (conn != maps.end()) {
            auto& context = std::any_cast<Context&>(conn->second->getContext());
            con.append("slave");
            con.append(convert(i));
            con.append(":ip=");
            con.append(context.slaveAddr()->toIpAddr());
            con.append(",port=");
            con.append(convert(context.slaveAddr()->toIpPort()));
            con.append(",offset=");
            con.append(convert(_dbServer->slaveOffset()));
            con.append("\n");
            i++;
        }
    }
    con.append("xxx:yyy\r\n");
}

void DB::dbsizeCommand(Context& con)
{
    appendReplyNumber(con, _hashMap.size());
}

//////////////////////////////////////////////////////////////////
// String Keys Operation
//////////////////////////////////////////////////////////////////

#define getStringValue(it) getXXType(it, String&)

void DB::setCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    int64_t expire;
    if (cmdlist.size() == 3) {
        delExpireKey(cmdlist[1]);
        _hashMap[cmdlist[1]] = cmdlist[2];
        touchWatchKey(cmdlist[1]);
        con.append(db_return_ok);
        return;
    }
    switch (cmdlist.size()) {
    case 4: // SET key value [NX|XX]
        if (strcasecmp(cmdlist[3].c_str(), "XX") == 0) {
            auto it = _hashMap.find(cmdlist[1]);
            if (it != _hashMap.end()) {
                delExpireKey(cmdlist[1]);
                _hashMap[cmdlist[1]] = cmdlist[2];
                con.append(db_return_ok);
            } else
                con.append(db_return_nil);
        } else if (strcasecmp(cmdlist[3].c_str(), "NX") == 0) {
            auto it = _hashMap.find(cmdlist[1]);
            if (it == _hashMap.end()) {
                delExpireKey(cmdlist[1]);
                _hashMap[cmdlist[1]] = cmdlist[2];
                con.append(db_return_ok);
            } else
                con.append(db_return_nil);
        } else
            goto syntax_err;
        break;
    case 5: // SET key value [EX seconds|PX milliseconds]
        if (strcasecmp(cmdlist[3].c_str(), "EX") == 0
         || strcasecmp(cmdlist[3].c_str(), "PX") == 0) {
            if (!_strIsNumber(cmdlist[4])) {
                con.append(db_return_interger_err);
                return;
            }
            delExpireKey(cmdlist[1]);
            _hashMap[cmdlist[1]] = cmdlist[2];
            expire = atoll(cmdlist[4].c_str());
            if (strcasecmp(cmdlist[3].c_str(), "EX") == 0)
                expire *= 1000;
            addExpireKey(cmdlist[1], expire);
            con.append(db_return_ok);
        } else
            goto syntax_err;
        break;
    case 6: // SET key value [EX seconds|PX milliseconds] [NX|XX]
        if (strcasecmp(cmdlist[3].c_str(), "EX") == 0
         || strcasecmp(cmdlist[3].c_str(), "PX") == 0) {
            if (!_strIsNumber(cmdlist[4])) {
                con.append(db_return_interger_err);
                return;
            }
            if (strcasecmp(cmdlist[5].c_str(), "XX") == 0) {
                auto it = _hashMap.find(cmdlist[1]);
                if (it != _hashMap.end()) {
                    delExpireKey(cmdlist[1]);
                    _hashMap[cmdlist[1]] = cmdlist[2];
                } else {
                    con.append(db_return_nil);
                    return;
                }
            } else if (strcasecmp(cmdlist[5].c_str(), "NX") == 0) {
                auto it = _hashMap.find(cmdlist[1]);
                if (it == _hashMap.end()) {
                    delExpireKey(cmdlist[1]);
                    _hashMap[cmdlist[1]] = cmdlist[2];
                } else {
                    con.append(db_return_nil);
                    return;
                }
            } else
                goto syntax_err;
            expire = atoll(cmdlist[4].c_str());
            if (strcasecmp(cmdlist[3].c_str(), "EX") == 0)
                expire *= 1000;
            addExpireKey(cmdlist[1], expire);
            con.append(db_return_ok);
        } else
            goto syntax_err;
        break;
    default:
        goto syntax_err;
    }
    touchWatchKey(cmdlist[1]);
    return;
syntax_err:
    con.append(db_return_syntax_err);
    return;
}

void DB::setnxCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    auto it = _hashMap.find(cmdlist[1]);
    if (it != _hashMap.end()) {
        con.append(db_return_integer_0);
    } else {
        _hashMap[cmdlist[1]] = cmdlist[2];
        touchWatchKey(cmdlist[1]);
        con.append(db_return_integer_1);
    }
}

void DB::getCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    isExpiredKey(cmdlist[1]);
    auto it = _hashMap.find(cmdlist[1]);
    if (it == _hashMap.end()) {
        con.append(db_return_nil);
        return;
    }
    checkType(con, it, String);
    auto& value = getStringValue(it);
    appendReplySingleStr(con, value);
}

void DB::getSetCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    isExpiredKey(cmdlist[1]);
    touchWatchKey(cmdlist[1]);
    auto it = _hashMap.find(cmdlist[1]);
    if (it == _hashMap.end()) {
        _hashMap[cmdlist[1]] = cmdlist[2];
        con.append(db_return_nil);
        return;
    }
    checkType(con, it, String);
    String oldvalue = getStringValue(it);
    _hashMap[cmdlist[1]] = cmdlist[2];
    appendReplySingleStr(con, oldvalue);
}

void DB::strlenCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    isExpiredKey(cmdlist[1]);
    auto it = _hashMap.find(cmdlist[1]);
    if (it == _hashMap.end()) {
        con.append(db_return_integer_0);
        return;
    }
    checkType(con, it, String);
    String& value = getStringValue(it);
    appendReplyNumber(con, value.size());
}

void DB::appendCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    isExpiredKey(cmdlist[1]);
    touchWatchKey(cmdlist[1]);
    auto it = _hashMap.find(cmdlist[1]);
    if (it == _hashMap.end()) {
        _hashMap[cmdlist[1]] = cmdlist[2];
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
    if (size % 2 == 0) {
        con.append("-ERR wrong number of arguments for '" + cmdlist[0] + "'\r\n");
        return;
    }
    for (auto i = 1; i < size; i += 2) {
        isExpiredKey(cmdlist[i]);
        _hashMap[cmdlist[i]] = cmdlist[i + 1];
        touchWatchKey(cmdlist[i]);
    }
    con.append(db_return_ok);
}

void DB::mgetCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    size_t size = cmdlist.size();
    appendReplyMulti(con, size - 1);
    for (auto i = 1; i < size; i++) {
        isExpiredKey(cmdlist[i]);
        auto it = _hashMap.find(cmdlist[i]);
        if (it != _hashMap.end()) {
            if (!isXXType(it, String)) {
                con.append(db_return_nil);
                continue;
            }
            String& value = getStringValue(it);
            appendReplySingleStr(con, value);
        } else
            con.append(db_return_nil);
    }
}

bool DB::_strIsNumber(const String& s)
{
    bool isNumber = true;
    for (auto c : s) {
        if (!isnumber(c)) {
            isNumber = false;
            break;
        }
    }
    return isNumber;
}

void DB::_incr(Context& con, int64_t incr)
{
    auto& cmdlist = con.commandList();
    isExpiredKey(cmdlist[1]);
    auto it = _hashMap.find(cmdlist[1]);
    if (it != _hashMap.end()) {
        checkType(con, it, String);
        String& value = getStringValue(it);
        if (_strIsNumber(value)) {
            int64_t number = atoll(value.c_str());
            number += incr;
            _hashMap[cmdlist[1]] = String(convert(number));
            appendReplyNumber(con, number);
        } else {
            con.append(db_return_interger_err);
            return;
        }
    } else {
        _hashMap[cmdlist[1]] = String(convert(incr));
        appendReplyNumber(con, incr);
    }
    touchWatchKey(cmdlist[1]);
}

void DB::incrCommand(Context& con)
{
    _incr(con, 1);
}

void DB::incrbyCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    int64_t incr = atol(cmdlist[2].c_str());
    _incr(con, incr);
}

void DB::decrCommand(Context& con)
{
    _incr(con, -1);
}

void DB::decrbyCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    int64_t decr = -atol(cmdlist[2].c_str());
    _incr(con, decr);
}

//////////////////////////////////////////////////////////////////
// List Keys Operation
//////////////////////////////////////////////////////////////////

#define getListValue(it) getXXType(it, List&)

void DB::_lpush(Context& con, bool leftPush)
{
    auto& cmdlist = con.commandList();
    size_t size = cmdlist.size();
    isExpiredKey(cmdlist[1]);
    auto it = _hashMap.find(cmdlist[1]);
    if (it != _hashMap.end()) {
        checkType(con, it, List);
        List& list = getListValue(it);
        for (auto i = 2; i < size; i++) {
            if (leftPush)
                list.push_front(cmdlist[i]);
            else
                list.push_back(cmdlist[i]);
        }
        appendReplyNumber(con, list.size());
    } else {
        List list;
        for (auto i = 2; i < size; i++) {
            if (leftPush)
                list.push_front(cmdlist[i]);
            else
                list.push_back(cmdlist[i]);
        }
        _hashMap[cmdlist[1]] = list;
        appendReplyNumber(con, list.size());
    }
    touchWatchKey(cmdlist[1]);
}

void DB::lpushCommand(Context& con)
{
    _lpush(con, true);
}

void DB::rpushCommand(Context& con)
{
    _lpush(con, false);
}

void DB::_lpushx(Context& con, bool frontPush)
{
    auto& cmdlist = con.commandList();
    isExpiredKey(cmdlist[1]);
    auto it = _hashMap.find(cmdlist[1]);
    if (it == _hashMap.end()) {
        con.append(db_return_integer_0);
        return;
    }
    checkType(con, it, List);
    List& list = getListValue(it);
    if (frontPush)
        list.push_front(cmdlist[2]);
    else
        list.push_back(cmdlist[2]);
    touchWatchKey(cmdlist[1]);
    appendReplyNumber(con, list.size());
}

void DB::lpushxCommand(Context& con)
{
    _lpushx(con, true);
}

void DB::rpushxCommand(Context& con)
{
    _lpushx(con, false);
}

void DB::_lpop(Context& con, bool leftPop)
{
    auto& cmdlist = con.commandList();
    isExpiredKey(cmdlist[1]);
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
    if (leftPop) {
        appendReplySingleStr(con, list.front());
        list.pop_front();
    } else {
        appendReplySingleStr(con, list.back());
        list.pop_back();
    }
    touchWatchKey(cmdlist[1]);
}

void DB::lpopCommand(Context& con)
{
    _lpop(con, true);
}

void DB::rpopCommand(Context& con)
{
    _lpop(con, false);
}

void DB::rpoplpushCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    isExpiredKey(cmdlist[1]);
    isExpiredKey(cmdlist[2]);
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
    appendReplySingleStr(con, srclist.back());
    auto des = _hashMap.find(cmdlist[2]);
    if (des != _hashMap.end()) {
        checkType(con, des, List);
        List& deslist = getListValue(des);
        deslist.push_front(std::move(srclist.back()));
        touchWatchKey(cmdlist[1]);
        touchWatchKey(cmdlist[2]);
    } else {
        srclist.push_front(std::move(srclist.back()));
        touchWatchKey(cmdlist[1]);
    }
    srclist.pop_back();
}

void DB::lremCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    isExpiredKey(cmdlist[1]);
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
    touchWatchKey(cmdlist[1]);
    appendReplyNumber(con, retval);
}

void DB::llenCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    isExpiredKey(cmdlist[1]);
    auto it = _hashMap.find(cmdlist[1]);
    if (it == _hashMap.end()) {
        con.append(db_return_integer_0);
        return;
    }
    checkType(con, it, List);
    List& list = getListValue(it);
    appendReplyNumber(con, list.size());
}

void DB::lindexCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    isExpiredKey(cmdlist[1]);
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
    isExpiredKey(cmdlist[1]);
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
    if (index >= static_cast<ssize_t>(size)) {
        con.append(db_return_out_of_range);
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
    isExpiredKey(cmdlist[1]);
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
    if (stop > static_cast<ssize_t>(end))
        stop = end;
    if (start > static_cast<ssize_t>(end)){
        con.append(db_return_nil);
        return;
    }
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

void DB::ltrimCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    isExpiredKey(cmdlist[1]);
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
    touchWatchKey(cmdlist[1]);
    con.append(db_return_ok);
}

//////////////////////////////////////////////////////////////////
// Set Keys Operation
//////////////////////////////////////////////////////////////////

#define getSetValue(it) getXXType(it, Set&)

void DB::saddCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    isExpiredKey(cmdlist[1]);
    size_t members = cmdlist.size();
    auto it = _hashMap.find(cmdlist[1]);
    if (it != _hashMap.end()) {
        checkType(con, it, Set);
        Set& set = getSetValue(it);
        int retval = 0;
        for (auto i = 2; i < members; i++) {
            if (set.find(cmdlist[i]) == set.end()) {
                set.insert(cmdlist[i]);
                retval++;
            }
        }
        appendReplyNumber(con, retval);
    } else {
        Set set;
        for (auto i = 2; i < members; i++)
            set.insert(cmdlist[i]);
        _hashMap[cmdlist[1]] = std::move(set);
        appendReplyNumber(con, members - 2);
    }
    touchWatchKey(cmdlist[1]);
}

void DB::sisMemberCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    isExpiredKey(cmdlist[1]);
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

static std::tuple<size_t, size_t> getRandBucketNumber(DB::Set& set)
{
    size_t bucketNumber;
    std::uniform_int_distribution<size_t> u(0, set.bucket_count() - 1);
    std::default_random_engine e(clock());
    do {
        bucketNumber = u(e);
    } while (set.bucket_size(bucketNumber) == 0);
    std::uniform_int_distribution<size_t> _u(0, set.bucket_size(bucketNumber) - 1);
    return std::make_tuple(bucketNumber, _u(e));
}

void DB::spopCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    isExpiredKey(cmdlist[1]);
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
    auto bucket = getRandBucketNumber(set);
    size_t bucketNumber = std::get<0>(bucket);
    size_t where = std::get<1>(bucket);
    for (auto it = set.cbegin(bucketNumber);
            it != set.cend(bucketNumber); it++)
        if (where-- == 0) {
            appendReplySingleStr(con, *it);
            set.erase(set.find(*it));
            break;
        }
    touchWatchKey(cmdlist[1]);
}

void DB::srandMemberCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    isExpiredKey(cmdlist[1]);
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
        appendReplyMulti(con, set.size());
        for (auto& it : set) {
            appendReplySingleStr(con, it);
        }
        return;
    }
    if (count == 0 || count < 0) {
        if (count == 0)
            count = -1;
        appendReplyMulti(con, -count);
        while (count++ < 0) {
            auto bucket = getRandBucketNumber(set);
            size_t bucketNumber = std::get<0>(bucket);
            size_t where = std::get<1>(bucket);
            for (auto it = set.cbegin(bucketNumber);
                    it != set.cend(bucketNumber); it++) {
                if (where-- == 0) {
                    appendReplySingleStr(con, *it);
                    break;
                }
            }
        }
        return;
    }
    appendReplyMulti(con, count);
    Set tset;
    while (count-- > 0) {
        auto bucket = getRandBucketNumber(set);
        size_t bucketNumber = std::get<0>(bucket);
        size_t where = std::get<1>(bucket);
        for (auto it = set.cbegin(bucketNumber);
                it != set.cend(bucketNumber); it++) {
            if (where-- == 0) {
                if (tset.find(*it) != tset.end()) {
                    count++;
                    break;
                }
                tset.insert(*it);
                break;
            }
        }
    }
    for (auto it : tset) {
        appendReplySingleStr(con, it);
    }
}

void DB::sremCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    isExpiredKey(cmdlist[1]);
    size_t size = cmdlist.size();
    auto it = _hashMap.find(cmdlist[1]);
    if (it == _hashMap.end()) {
        con.append(db_return_integer_0);
        return;
    }
    checkType(con, it, Set);
    Set& set = getSetValue(it);
    int retval = 0;
    for (auto i = 2; i < size; i++) {
        auto it = set.find(cmdlist[i]);
        if (it != set.end()) {
            set.erase(it);
            retval++;
        }
    }
    touchWatchKey(cmdlist[1]);
    appendReplyNumber(con, retval);
}

void DB::smoveCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    isExpiredKey(cmdlist[1]);
    isExpiredKey(cmdlist[2]);
    auto src = _hashMap.find(cmdlist[1]);
    if (src == _hashMap.end()) {
        con.append(db_return_integer_0);
        return;
    }
    checkType(con, src, Set);
    Set& srcSet = getSetValue(src);
    auto srcIt = srcSet.find(cmdlist[3]);
    if (srcIt == srcSet.end()) {
        con.append(db_return_integer_0);
        return;
    }
    srcSet.erase(srcIt);
    auto des = _hashMap.find(cmdlist[2]);
    if (des != _hashMap.end()) {
        checkType(con, des, Set);
        Set& desSet = getSetValue(des);
        auto desIt = desSet.find(cmdlist[3]);
        if (desIt == desSet.end())
            desSet.insert(cmdlist[3]);
    } else {
        Set set;
        set.insert(cmdlist[3]);
        _hashMap[cmdlist[2]] = std::move(set);
    }
    touchWatchKey(cmdlist[1]);
    touchWatchKey(cmdlist[2]);
    con.append(db_return_integer_1);
}

void DB::scardCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    isExpiredKey(cmdlist[1]);
    auto it = _hashMap.find(cmdlist[1]);
    if (it == _hashMap.end()) {
        con.append(db_return_integer_0);
        return;
    }
    checkType(con, it, Set);
    Set& set = getSetValue(it);
    appendReplyNumber(con, set.size());
}

void DB::smembersCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    isExpiredKey(cmdlist[1]);
    auto it = _hashMap.find(cmdlist[1]);
    if (it == _hashMap.end()) {
        con.append(db_return_nil);
        return;
    }
    checkType(con, it, Set);
    Set& set = getSetValue(it);
    appendReplyMulti(con, set.size());
    for (auto& it : set) {
        appendReplySingleStr(con, it);
    }
}

void DB::sinterCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    size_t size = cmdlist.size();
    size_t minSet = 0, minSetIndex = 0;
    for (auto i = 1; i < size; i++) {
        isExpiredKey(cmdlist[i]);
        auto it = _hashMap.find(cmdlist[i]);
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
        if (minSet == 0)
            minSet = set.size();
        else if (minSet > set.size()) {
            minSet = set.size();
            minSetIndex = i;
        }
    }
    Set retSet;
    Set& set = getSetValue(_hashMap.find(cmdlist[minSetIndex]));
    for (auto& it : set) {
        size_t i;
        for (i = 1; i < size; i++) {
            if (i == minSetIndex)
                continue;
            Set& set = getSetValue(_hashMap.find(cmdlist[i]));
            if (set.find(it) == set.end())
                break;
        }
        if (i == size)
            retSet.insert(it);
    }
    appendReplyMulti(con, retSet.size());
    for (auto& it : retSet)
        appendReplySingleStr(con, it);
}

void DB::sinterStoreCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    isExpiredKey(cmdlist[1]);
    size_t size = cmdlist.size();
    size_t minSet = 0, minSetIndex = 0;
    for (auto i = 2; i < size; i++) {
        isExpiredKey(cmdlist[i]);
        auto it = _hashMap.find(cmdlist[i]);
        if (it == _hashMap.end()) {
            con.append(db_return_integer_0);
            return;
        }
        checkType(con, it, Set);
        Set& set = getSetValue(it);
        if (set.empty()) {
            con.append(db_return_integer_0);
            return;
        }
        if (minSet == 0)
            minSet = set.size();
        else if (minSet > set.size()) {
            minSet = set.size();
            minSetIndex = i;
        }
    }
    Set retSet;
    Set& set = getSetValue(_hashMap.find(cmdlist[minSetIndex]));
    for (auto& it : set) {
        size_t i;
        for (i = 2; i < size; i++) {
            if (i == minSetIndex)
                continue;
            Set& set = getSetValue(_hashMap.find(cmdlist[i]));
            if (set.find(it) == set.end())
                break;
        }
        if (i == size)
            retSet.insert(it);
    }
    auto it = _hashMap.find(cmdlist[1]);
    if (it != _hashMap.end()) {
        checkType(con, it, Set);
        Set& set = getSetValue(it);
        set.swap(retSet);
        appendReplyNumber(con, set.size());
    } else {
        appendReplyNumber(con, retSet.size());
        _hashMap[cmdlist[1]] = std::move(retSet);
    }
}

void DB::sunionCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    size_t size = cmdlist.size();
    Set retSet;
    for (auto i = 1; i < size; i++) {
        isExpiredKey(cmdlist[i]);
        auto it = _hashMap.find(cmdlist[i]);
        if (it != _hashMap.end()) {
            checkType(con, it, Set);
            Set& set = getSetValue(it);
            for (auto& it : set) {
                if (retSet.find(it) == retSet.end())
                    retSet.insert(it);
            }
        }
    }
    if (retSet.empty())
        con.append(db_return_nil);
    else {
        appendReplyMulti(con, retSet.size());
        for (auto& it : retSet)
            appendReplySingleStr(con, it);
    }
}

void DB::sunionStoreCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    size_t size = cmdlist.size();
    Set retSet;
    for (auto i = 2; i < size; i++) {
        isExpiredKey(cmdlist[i]);
        auto it = _hashMap.find(cmdlist[i]);
        if (it != _hashMap.end()) {
            checkType(con, it, Set);
            Set& set = getSetValue(it);
            for (auto& it : set) {
                if (retSet.find(it) == retSet.end())
                    retSet.insert(it);
            }
        }
    }
    auto it = _hashMap.find(cmdlist[1]);
    if (it != _hashMap.end()) {
        checkType(con, it, Set);
        Set& set = getSetValue(it);
        set.swap(retSet);
        appendReplyNumber(con, set.size());
    } else {
        appendReplyNumber(con, retSet.size());
        _hashMap[cmdlist[1]] = std::move(retSet);
    }
}

void DB::sdiffCommand(Context& con)
{
    // TODO:
}

void DB::sdiffStoreCommand(Context& con)
{
    // TODO:
}

//////////////////////////////////////////////////////////////////
// Hash Keys Operation
//////////////////////////////////////////////////////////////////

#define getHashValue(it) getXXType(it, Hash&)

void DB::hsetCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    isExpiredKey(cmdlist[1]);
    touchWatchKey(cmdlist[1]);
    auto it = _hashMap.find(cmdlist[1]);
    if (it == _hashMap.end()) {
        Hash hash;
        hash[cmdlist[2]] = cmdlist[3];
        con.append(db_return_integer_1);
        _hashMap[cmdlist[1]] = std::move(hash);
        return;
    }
    checkType(con, it, Hash);
    Hash& hash = getHashValue(it);
    if (hash.find(cmdlist[2]) != hash.end()) {
        con.append(db_return_integer_0);
    } else {
        con.append(db_return_integer_1);
    }
    hash[cmdlist[2]] = cmdlist[3];
}

void DB::hsetnxCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    isExpiredKey(cmdlist[1]);
    auto it = _hashMap.find(cmdlist[1]);
    if (it == _hashMap.end()) {
        Hash hash;
        hash[cmdlist[2]] = cmdlist[3];
        _hashMap[cmdlist[1]] = std::move(hash);
        touchWatchKey(cmdlist[1]);
        con.append(db_return_integer_1);
        return;
    }
    checkType(con, it, Hash);
    Hash& hash = getHashValue(it);
    if (hash.find(cmdlist[2]) != hash.end()) {
        con.append(db_return_integer_0);
    } else {
        hash[cmdlist[2]] = cmdlist[3];
        touchWatchKey(cmdlist[1]);
        con.append(db_return_integer_1);
    }
}

void DB::hgetCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    isExpiredKey(cmdlist[1]);
    auto it = _hashMap.find(cmdlist[1]);
    if (it == _hashMap.end()) {
        con.append(db_return_nil);
        return;
    }
    checkType(con, it, Hash);
    Hash& hash = getHashValue(it);
    auto value = hash.find(cmdlist[2]);
    if (value != hash.end()) {
        appendReplySingleStr(con, value->second);
    } else
        con.append(db_return_nil);
}

void DB::hexistsCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    isExpiredKey(cmdlist[1]);
    auto it = _hashMap.find(cmdlist[1]);
    if (it == _hashMap.end()) {
        con.append(db_return_integer_0);
        return;
    }
    checkType(con, it, Hash);
    Hash& hash = getHashValue(it);
    if (hash.find(cmdlist[2]) != hash.end()) {
        con.append(db_return_integer_1);
    } else {
        con.append(db_return_integer_0);
    }
}

void DB::hdelCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    isExpiredKey(cmdlist[1]);
    auto it = _hashMap.find(cmdlist[1]);
    if (it == _hashMap.end()) {
        con.append(db_return_integer_0);
        return;
    }
    size_t size = cmdlist.size();
    checkType(con, it, Hash);
    int retval = 0;
    Hash& hash = getHashValue(it);
    for (auto i = 2; i < size; i++) {
        auto it = hash.find(cmdlist[i]);
        if (it != hash.end()) {
            hash.erase(it);
            retval++;
        }
    }
    touchWatchKey(cmdlist[1]);
    appendReplyNumber(con, retval);
}

void DB::hlenCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    isExpiredKey(cmdlist[1]);
    auto it = _hashMap.find(cmdlist[1]);
    if (it != _hashMap.end()) {
        checkType(con, it, Hash);
        Hash& hash = getHashValue(it);
        appendReplyNumber(con, hash.size());
    } else {
        con.append(db_return_integer_0);
    }
}

void DB::hstrlenCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    isExpiredKey(cmdlist[1]);
    auto it = _hashMap.find(cmdlist[1]);
    if (it == _hashMap.end()) {
        con.append(db_return_integer_0);
        return;
    }
    checkType(con, it, Hash);
    Hash& hash = getHashValue(it);
    auto value = hash.find(cmdlist[2]);
    if (value != hash.end()) {
        appendReplyNumber(con, value->second.size());
    } else {
        con.append(db_return_integer_0);
    }
}

void DB::hincrbyCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    isExpiredKey(cmdlist[1]);
    if (!_strIsNumber(cmdlist[3])) {
        con.append(db_return_interger_err);
        return;
    }
    int64_t incr = atoll(cmdlist[3].c_str());
    auto it = _hashMap.find(cmdlist[1]);
    if (it == _hashMap.end()) {
        Hash hash;
        hash[cmdlist[2]] = cmdlist[3];
        _hashMap[cmdlist[1]] = std::move(hash);
        touchWatchKey(cmdlist[1]);
        con.append(db_return_integer_0);
        return;
    }
    checkType(con, it, Hash);
    Hash& hash = getHashValue(it);
    auto value = hash.find(cmdlist[2]);
    if (value != hash.end()) {
        if (!_strIsNumber(value->second)) {
            con.append(db_return_interger_err);
            return;
        }
        int64_t i64 = atoll(value->second.c_str());
        i64 += incr;
        value->second.assign(convert(i64));
        appendReplyNumber(con, i64);
    } else {
        hash[cmdlist[2]] = String(convert(incr));
        appendReplyNumber(con, incr);
    }
    touchWatchKey(cmdlist[1]);
}

void DB::hmsetCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    isExpiredKey(cmdlist[1]);
    size_t size = cmdlist.size();
    if (size % 2 != 0) {
        con.append("-ERR wrong number of arguments for '" + cmdlist[0] + "'\r\n");
        return;
    }
    touchWatchKey(cmdlist[1]);
    auto it = _hashMap.find(cmdlist[1]);
    if (it == _hashMap.end()) {
        Hash hash;
        for (auto i = 2; i < size; i += 2) {
            hash[cmdlist[i]] = cmdlist[i + 1];
        }
        _hashMap[cmdlist[1]] = std::move(hash);
        con.append(db_return_ok);
        return;
    }
    checkType(con, it, Hash);
    Hash& hash = getHashValue(it);
    for (auto i = 2; i < size; i += 2) {
        hash[cmdlist[i]] = cmdlist[i + 1];
    }
    con.append(db_return_ok);
}

void DB::hmgetCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    isExpiredKey(cmdlist[1]);
    size_t size = cmdlist.size();
    auto it = _hashMap.find(cmdlist[1]);
    appendReplyMulti(con, size - 2);
    if (it == _hashMap.end()) {
        for (auto i = 2; i < size; i++)
            con.append(db_return_nil);
        return;
    }
    checkType(con, it, Hash);
    Hash& hash = getHashValue(it);
    for (auto i = 2; i < size; i++) {
        auto it = hash.find(cmdlist[i]);
        if (it != hash.end()) {
            appendReplySingleStr(con, it->second);
        } else {
            con.append(db_return_nil);
        }
    }
}

#define GETKEYS     0
#define GETVALUES   1
#define GETALL      2

void DB::_hgetXX(Context& con, int getXX)
{
    auto& cmdlist = con.commandList();
    isExpiredKey(cmdlist[1]);
    auto it = _hashMap.find(cmdlist[1]);
    if (it == _hashMap.end()) {
        con.append(db_return_nil);
        return;
    }
    checkType(con, it, Hash);
    Hash& hash = getHashValue(it);
    if (hash.empty()) {
        con.append(db_return_nil);
        return;
    }
    if (getXX == GETALL)
        appendReplyMulti(con, hash.size() * 2);
    else
        appendReplyMulti(con, hash.size());
    for (auto& it : hash) {
        if (getXX == GETKEYS) {
            appendReplySingleStr(con, it.first);
        } else if (getXX == GETVALUES) {
            appendReplySingleStr(con, it.second);
        } else if (getXX == GETALL) {
            appendReplySingleStr(con, it.first);
            appendReplySingleStr(con, it.second);
        }
    }
}

void DB::hkeysCommand(Context& con)
{
    _hgetXX(con, GETKEYS);
}

void DB::hvalsCommand(Context& con)
{
    _hgetXX(con, GETVALUES);
}

void DB::hgetAllCommand(Context& con)
{
    _hgetXX(con, GETALL);
}

#undef GETKEYS
#undef GETVALUES
#undef GETALL

bool DB::isExpiredKey(const Key& key)
{
    auto it = _expireMap.find(key);
    if (it == _expireMap.end()) return false;
    int64_t now = Angel::TimeStamp::now();
    if (it->second > now) return false;
    delExpireKey(key);
    delKey(key);
    Context::CommandList cmdlist = { "DEL", key };
    _dbServer->appendWriteCommand(cmdlist);
    return true;
}

void DB::watchKeyForClient(const Key& key, size_t id)
{
    auto it = _watchMap.find(key);
    if (it == _watchMap.end()) {
        std::vector<size_t> clist = { id };
        _watchMap[key] = std::move(clist);
    } else {
        it->second.push_back(id);
    }
}

void DB::touchWatchKey(const Key& key)
{
    auto clist = _watchMap.find(key);
    if (clist == _watchMap.end())
        return;
    auto& maps = g_server->server().connectionMaps();
    for (auto& id : clist->second) {
        auto conn = maps.find(id);
        if (conn != maps.end()) {
            auto& context = std::any_cast<Context&>(conn->second->getContext());
            context.setFlag(Context::EXEC_MULTI_ERR);
        }
    }
}
