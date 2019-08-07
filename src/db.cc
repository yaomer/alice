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

using namespace Alice;
using std::placeholders::_1;

#define BIND(f) std::bind(&DB::f, this, _1)

DB::DB(DBServer *dbServer)
    : _dbServer(dbServer)
{
    _commandMap = {
        { "SET",        {  3,   IS_WRITE,   BIND(strSet) } },
        { "SETNX",      { -3,   IS_WRITE,   BIND(strSetIfNotExist) } },
        { "GET",        { -2,   IS_READ,  BIND(strGet) } },
        { "GETSET",     { -3,   IS_WRITE,   BIND(strGetSet) } },
        { "APPEND",     { -3,   IS_WRITE,   BIND(strAppend) } },
        { "STRLEN",     { -2,   IS_READ,  BIND(strLen) } },
        { "MSET",       {  3,   IS_WRITE,   BIND(strMset) } },
        { "MGET",       {  2,   IS_READ,  BIND(strMget) } },
        { "INCR",       { -2,   IS_WRITE,   BIND(strIncr) } },
        { "INCRBY",     { -3,   IS_WRITE,   BIND(strIncrBy) } },
        { "DECR",       { -2,   IS_WRITE,   BIND(strDecr) } },
        { "DECRBY",     { -3,   IS_WRITE,   BIND(strDecrBy) } },
        { "LPUSH",      {  3,   IS_WRITE,   BIND(listLeftPush) } },
        { "LPUSHX",     { -3,   IS_WRITE,   BIND(listHeadPush) } },
        { "RPUSH",      {  3,   IS_WRITE,   BIND(listRightPush) } },
        { "RPUSHX",     { -3,   IS_WRITE,   BIND(listTailPush) } },
        { "LPOP",       { -2,   IS_WRITE,   BIND(listLeftPop) } },
        { "RPOP",       { -2,   IS_WRITE,   BIND(listRightPop) } },
        { "RPOPLPUSH",  { -3,   IS_WRITE,   BIND(listRightPopToLeftPush) } },
        { "LREM",       { -4,   IS_WRITE,   BIND(listRem) } },
        { "LLEN",       { -2,   IS_READ,  BIND(listLen) } },
        { "LINDEX",     { -3,   IS_READ,  BIND(listIndex) } },
        { "LSET",       { -4,   IS_WRITE,   BIND(listSet) } },
        { "LRANGE",     { -4,   IS_READ,  BIND(listRange) } },
        { "LTRIM",      { -4,   IS_WRITE,   BIND(listTrim) } },
        { "SADD",       {  3,   IS_WRITE,   BIND(setAdd) } },
        { "SISMEMBER",  { -3,   IS_READ,  BIND(setIsMember) } },
        { "SPOP",       { -2,   IS_WRITE,   BIND(setPop) } },
        { "SRANDMEMBER",{  2,   IS_READ,  BIND(setRandMember) } },
        { "SREM",       {  3,   IS_WRITE,   BIND(setRem)  } },
        { "SMOVE",      { -4,   IS_WRITE,   BIND(setMove) } },
        { "SCARD",      { -2,   IS_READ,  BIND(setCard) } },
        { "SMEMBERS",   { -2,   IS_READ,  BIND(setMembers) } },
        { "SINTER",     {  2,   IS_READ,  BIND(setInter) } },
        { "SINTERSTORE",{  3,   IS_WRITE,   BIND(setInterStore) } },
        { "SUNION",     {  2,   IS_READ,  BIND(setUnion) } },
        { "SUNIONSTORE",{  3,   IS_WRITE,   BIND(setUnionStore) } },
        { "SDIFF",      {  2,   IS_READ,  BIND(setDiff) } },
        { "SDIFFSTORE", {  3,   IS_WRITE,   BIND(setDiffStore) } },
        { "HSET",       { -4,   IS_WRITE,   BIND(hashSet) } },
        { "HSETNX",     { -4,   IS_WRITE,   BIND(hashSetIfNotExists) } },
        { "HGET",       { -3,   IS_READ,  BIND(hashGet) } },
        { "HEXISTS",    { -3,   IS_READ,  BIND(hashFieldExists) } },
        { "HDEL",       {  3,   IS_WRITE,   BIND(hashDelete) } },
        { "HLEN",       { -2,   IS_READ,  BIND(hashFieldLen) } },
        { "HSTRLEN",    { -3,   IS_READ,  BIND(hashValueLen) } },
        { "HINCRBY",    { -4,   IS_WRITE,   BIND(hashIncrBy) } },
        { "HMSET",      {  4,   IS_WRITE,   BIND(hashMset) } },
        { "HMGET",      {  3,   IS_READ,  BIND(hashMget) } },
        { "HKEYS",      { -2,   IS_READ,  BIND(hashGetKeys) } },
        { "HVALS",      { -2,   IS_READ,  BIND(hashGetValues) } },
        { "HGETALL",    { -2,   IS_READ,  BIND(hashGetAll) } },
        { "EXISTS",     { -2,   IS_READ,  BIND(isKeyExists) } },
        { "TYPE",       { -2,   IS_READ,  BIND(getKeyType) } },
        { "TTL",        { -2,   IS_READ,  BIND(getTtlSecs) } },
        { "PTTL",       { -2,   IS_READ,  BIND(getTtlMils) } },
        { "EXPIRE",     { -3,   IS_WRITE,   BIND(setKeyExpireSecs) } },
        { "PEXPIRE",    { -3,   IS_WRITE,   BIND(setKeyExpireMils) } },
        { "DEL",        {  2,   IS_WRITE,   BIND(deleteKey) } },
        { "KEYS",       { -2,   IS_READ,  BIND(getAllKeys) } },
        { "SAVE",       { -1,   IS_READ,  BIND(save) } },
        { "BGSAVE",     { -1,   IS_READ,  BIND(saveBackground) } },
        { "BGREWRITEAOF",{ -1,  IS_READ,  BIND(rewriteAof) } },
        { "LASTSAVE",   { -1,   IS_READ,  BIND(lastSaveTime) } },
        { "FLUSHDB",    { -1,   IS_WRITE,   BIND(flushDb) } },
        { "SLAVEOF",    { -3,   IS_READ,  BIND(slaveOf) } },
        { "PSYNC",      { -3,   IS_READ,  BIND(psync) } },
        { "REPLCONF",   { -3,   IS_INTER,  BIND(replconf) } },
        { "PING",       { -1,   IS_READ,  BIND(ping) } },
        { "PONG",       { -1,   IS_INTER,  BIND(pong) } },
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

#define appendReplyMulti(con, size) \
    do { \
        (con).append("*"); \
        (con).append(convert(size)); \
        (con).append("\r\n"); \
    } while (0)

#define appendReplySingle(con, it) \
    do { \
        (con).append("$"); \
        (con).append(convert((it).size())); \
        (con).append("\r\n"); \
        (con).append(it); \
        (con).append("\r\n"); \
    } while (0)

#define appendReplyNumber(con, size) \
    do { \
        (con).append(": "); \
        (con).append(convert(size)); \
        (con).append("\r\n"); \
    } while (0)

//////////////////////////////////////////////////////////////////

void DB::isKeyExists(Context& con)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
    auto it = _hashMap.find(cmdlist[1]);
    if (it != _hashMap.end()) {
        con.append(db_return_integer_1);
    } else {
        con.append(db_return_integer_0);
    }
}

void DB::getKeyType(Context& con)
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

void DB::_getTtl(Context& con, bool seconds)
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
    int64_t milliseconds = expire->second - Angel::TimeStamp::now();
    if (seconds)
        milliseconds /= 1000;
    appendReplyNumber(con, milliseconds);
}

void DB::getTtlSecs(Context& con)
{
    _getTtl(con, true);
}

void DB::getTtlMils(Context& con)
{
    _getTtl(con, false);
}

void DB::_setKeyExpire(Context& con, bool seconds)
{
    auto& cmdlist = con.commandList();
    auto it = _hashMap.find(cmdlist[1]);
    if (it != _hashMap.end()) {
        if (!_strIsNumber(cmdlist[2])) {
            con.append(db_return_interger_err);
            return;
        }
        int64_t expire = atol(cmdlist[2].c_str());
        if (seconds)
            expire *= 1000;
        con.db()->addExpireKey(cmdlist[1], expire);
        con.append(db_return_integer_1);
    } else {
        con.append(db_return_integer_0);
    }
}

void DB::setKeyExpireSecs(Context& con)
{
    _setKeyExpire(con, true);
}

void DB::setKeyExpireMils(Context& con)
{
    _setKeyExpire(con, false);
}

void DB::deleteKey(Context& con)
{
    auto& cmdlist = con.commandList();
    size_t size = cmdlist.size();
    int retval = 0;
    for (auto i = 1; i < size; i++) {
        auto it = _hashMap.find(cmdlist[i]);
        if (it != _hashMap.end()) {
            con.db()->delExpireKey(cmdlist[1]);
            _hashMap.erase(it);
            retval++;
        }
    }
    appendReplyNumber(con, retval);
}

void DB::getAllKeys(Context& con)
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
        con.db()->isExpiredKey(it.first);
        appendReplySingle(con, it.first);
    }
    if (size == con.message().size())
        con.assign(db_return_nil);
}

void DB::save(Context& con)
{
    if (_dbServer->rdb()->childPid() != -1)
        return;
    _dbServer->rdb()->save();
    con.append(db_return_ok);
    _dbServer->setLastSaveTime(Angel::TimeStamp::now());
    _dbServer->dirtyReset();
}

void DB::saveBackground(Context& con)
{
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

void DB::rewriteAof(Context& con)
{
    if (_dbServer->rdb()->childPid() != -1) {
        _dbServer->setFlag(DBServer::REWRITEAOF_DELAY);
        return;
    }
    if (_dbServer->aof()->childPid() != -1) {
        return;
    }
    _dbServer->aof()->rewriteBackground();
    con.append("+Background append only file rewriting started\r\n");
    _dbServer->setLastSaveTime(Angel::TimeStamp::now());
}

void DB::lastSaveTime(Context& con)
{
    appendReplyNumber(con, _dbServer->lastSaveTime());
}

void DB::flushDb(Context& con)
{
    _hashMap.clear();
    con.append(db_return_ok);
}

void DB::slaveOf(Context& con)
{
    auto& cmdlist = con.commandList();
    if (_strIsNumber(cmdlist[2])) {
        _dbServer->setMasterAddr(
                Angel::InetAddr(atoi(cmdlist[2].c_str()), cmdlist[1].c_str()));
        g_server->loop()->queueInLoop(
                [this]{ this->_dbServer->connectMasterServer(); });
        con.append(db_return_ok);
    } else
        con.append(db_return_interger_err);
}

void DB::psync(Context& con)
{
    auto& cmdlist = con.commandList();
    auto it = _dbServer->slaveIds().find(con.conn()->id());
    if (it == _dbServer->slaveIds().end())
        _dbServer->addSlaveId(con.conn()->id());
    con.setPerm(IS_INTER);
    if (cmdlist[1].compare("?") == 0 && cmdlist[2].compare("-1") == 0) {
sync:
        // 执行完整重同步
        con.setFlag(Context::SYNC_RDB_FILE);
        _dbServer->setFlag(DBServer::PSYNC);
        con.append("+FULLRESYNC\r\n");
        con.append(_dbServer->runId());
        con.append("\r\n");
        con.append(convert(_dbServer->offset()));
        con.append("\r\n");
        if (_dbServer->rdb()->childPid() != -1)
            return;
        _dbServer->rdb()->saveBackground();
    } else {
        if (cmdlist[1].compare(_dbServer->runId()) == 0) {
            size_t offset = atoll(cmdlist[2].c_str());
            ssize_t lastoffset = _dbServer->offset() - offset;
            if (lastoffset <= DBServer::copy_backlog_buffer_size) {
                // 执行部分重同步
                con.append("+CONTINUE\r\n");
                size_t start = DBServer::copy_backlog_buffer_size - lastoffset;
                con.append(std::string(
                            &_dbServer->copyBacklogBuffer()[start], lastoffset));
            } else
                goto sync;
        } else
            goto sync;
    }
}

void DB::replconf(Context& con)
{
    auto& cmdlist = con.commandList();
    size_t offset = atoll(cmdlist[2].c_str());
    ssize_t lastoffset = _dbServer->offset() - offset;
    if (lastoffset > 0) {
        if (lastoffset > DBServer::copy_backlog_buffer_size) {
            // TODO: 重新同步
        } else {
            // 重传丢失的命令
            size_t start = DBServer::copy_backlog_buffer_size - lastoffset;
            con.append(std::string(
                        &_dbServer->copyBacklogBuffer()[start], lastoffset));
        }
    }
}

void DB::ping(Context& con)
{
    con.append("*1\r\n$4\r\nPONG\r\n");
}

void DB::pong(Context& con)
{
    int64_t now = Angel::TimeStamp::now();
    if (now - con._lastRecvPingTime > 8) {
        // TODO: 重连master
    } else
        con._lastRecvPingTime = now;
}

//////////////////////////////////////////////////////////////////
// String Keys Operation
//////////////////////////////////////////////////////////////////

#define getStringValue(it) getXXType(it, String&)

void DB::strSet(Context& con)
{
    auto& cmdlist = con.commandList();
    int64_t expire;
    con.db()->isExpiredKey(cmdlist[1]);
    if (cmdlist.size() == 3) {
        con.db()->delExpireKey(cmdlist[1]);
        _hashMap[cmdlist[1]] = cmdlist[2];
        con.append(db_return_ok);
        return;
    }
    switch (cmdlist.size()) {
    case 4: // SET key value [NX|XX]
        if (strcasecmp(cmdlist[3].c_str(), "XX") == 0) {
            auto it = _hashMap.find(cmdlist[1]);
            if (it != _hashMap.end()) {
                con.db()->delExpireKey(cmdlist[1]);
                _hashMap[cmdlist[1]] = cmdlist[2];
                con.append(db_return_ok);
            } else
                con.append(db_return_nil);
        } else if (strcasecmp(cmdlist[3].c_str(), "NX") == 0) {
            auto it = _hashMap.find(cmdlist[1]);
            if (it == _hashMap.end()) {
                con.db()->delExpireKey(cmdlist[1]);
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
            con.db()->delExpireKey(cmdlist[1]);
            _hashMap[cmdlist[1]] = cmdlist[2];
            expire = atoll(cmdlist[4].c_str());
            if (strcasecmp(cmdlist[3].c_str(), "EX") == 0)
                expire *= 1000;
            con.db()->addExpireKey(cmdlist[1], expire);
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
                    con.db()->delExpireKey(cmdlist[1]);
                    _hashMap[cmdlist[1]] = cmdlist[2];
                } else {
                    con.append(db_return_nil);
                    return;
                }
            } else if (strcasecmp(cmdlist[5].c_str(), "NX") == 0) {
                auto it = _hashMap.find(cmdlist[1]);
                if (it == _hashMap.end()) {
                    con.db()->delExpireKey(cmdlist[1]);
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
            con.db()->addExpireKey(cmdlist[1], expire);
            con.append(db_return_ok);
        } else
            goto syntax_err;
        break;
    default:
        goto syntax_err;
    }
    return;
syntax_err:
    con.append(db_return_syntax_err);
    return;
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
    if (it == _hashMap.end()) {
        con.append(db_return_nil);
        return;
    }
    checkType(con, it, String);
    auto& value = getStringValue(it);
    appendReplySingle(con, value);
}

void DB::strGetSet(Context& con)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
    auto it = _hashMap.find(cmdlist[1]);
    if (it == _hashMap.end()) {
        _hashMap[cmdlist[1]] = cmdlist[2];
        con.append(db_return_nil);
        return;
    }
    checkType(con, it, String);
    String oldvalue = getStringValue(it);
    _hashMap[cmdlist[1]] = cmdlist[2];
    appendReplySingle(con, oldvalue);
}

void DB::strLen(Context& con)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
    auto it = _hashMap.find(cmdlist[1]);
    if (it == _hashMap.end()) {
        con.append(db_return_integer_0);
        return;
    }
    checkType(con, it, String);
    String& value = getStringValue(it);
    appendReplyNumber(con, value.size());
}

void DB::strAppend(Context& con)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
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

void DB::strMset(Context& con)
{
    auto& cmdlist = con.commandList();
    size_t size = cmdlist.size();
    if (size % 2 == 0) {
        con.append("-ERR wrong number of arguments for '" + cmdlist[0] + "'\r\n");
        return;
    }
    for (auto i = 1; i < size; i += 2) {
        con.db()->isExpiredKey(cmdlist[i]);
        _hashMap[cmdlist[i]] = cmdlist[i + 1];
    }
    con.append(db_return_ok);
}

void DB::strMget(Context& con)
{
    auto& cmdlist = con.commandList();
    size_t size = cmdlist.size();
    appendReplyMulti(con, size - 1);
    for (auto i = 1; i < size; i++) {
        con.db()->isExpiredKey(cmdlist[i]);
        auto it = _hashMap.find(cmdlist[i]);
        if (it != _hashMap.end()) {
            if (!isXXType(it, String)) {
                con.append(db_return_nil);
                continue;
            }
            String& value = getStringValue(it);
            appendReplySingle(con, value);
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

void DB::_strIdCr(Context& con, int64_t incr)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
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
// List Keys Operation
//////////////////////////////////////////////////////////////////

#define getListValue(it) getXXType(it, List&)

void DB::_listPush(Context& con, bool leftPush)
{
    auto& cmdlist = con.commandList();
    size_t size = cmdlist.size();
    con.db()->isExpiredKey(cmdlist[1]);
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
}

void DB::listLeftPush(Context& con)
{
    _listPush(con, true);
}

void DB::listRightPush(Context& con)
{
    _listPush(con, false);
}

void DB::_listEndsPush(Context& con, bool frontPush)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
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
    appendReplyNumber(con, list.size());
}

void DB::listHeadPush(Context& con)
{
    _listEndsPush(con, true);
}

void DB::listTailPush(Context& con)
{
    _listEndsPush(con, false);
}

void DB::_listPop(Context& con, bool leftPop)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
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
        appendReplySingle(con, list.front());
        list.pop_front();
    } else {
        appendReplySingle(con, list.back());
        list.pop_back();
    }
}

void DB::listLeftPop(Context& con)
{
    _listPop(con, true);
}

void DB::listRightPop(Context& con)
{
    _listPop(con, false);
}

void DB::listRightPopToLeftPush(Context& con)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
    con.db()->isExpiredKey(cmdlist[2]);
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
    appendReplySingle(con, srclist.back());
    auto des = _hashMap.find(cmdlist[2]);
    if (des != _hashMap.end()) {
        checkType(con, des, List);
        List& deslist = getListValue(des);
        deslist.push_front(std::move(srclist.back()));
    } else {
        srclist.push_front(std::move(srclist.back()));
    }
    srclist.pop_back();
}

void DB::listRem(Context& con)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
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
    appendReplyNumber(con, retval);
}

void DB::listLen(Context& con)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
    auto it = _hashMap.find(cmdlist[1]);
    if (it == _hashMap.end()) {
        con.append(db_return_integer_0);
        return;
    }
    checkType(con, it, List);
    List& list = getListValue(it);
    appendReplyNumber(con, list.size());
}

void DB::listIndex(Context& con)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
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
            appendReplySingle(con, it);
            break;
        }
}

void DB::listSet(Context& con)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
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
    con.append(db_return_ok);
}

void DB::listRange(Context& con)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
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
        appendReplySingle(con, it);
        i++;
    }
}

void DB::listTrim(Context& con)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
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
    con.append(db_return_ok);
}

//////////////////////////////////////////////////////////////////
// Set Keys Operation
//////////////////////////////////////////////////////////////////

#define getSetValue(it) getXXType(it, Set&)

void DB::setAdd(Context& con)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
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
}

void DB::setIsMember(Context& con)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
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

void DB::setPop(Context& con)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
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
            appendReplySingle(con, *it);
            set.erase(set.find(*it));
            break;
        }
}

void DB::setRandMember(Context& con)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
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
            appendReplySingle(con, it);
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
                    appendReplySingle(con, *it);
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
        appendReplySingle(con, it);
    }
}

void DB::setRem(Context& con)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
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
    appendReplyNumber(con, retval);
}

void DB::setMove(Context& con)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
    con.db()->isExpiredKey(cmdlist[2]);
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
    con.append(db_return_integer_1);
}

void DB::setCard(Context& con)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
    auto it = _hashMap.find(cmdlist[1]);
    if (it == _hashMap.end()) {
        con.append(db_return_integer_0);
        return;
    }
    checkType(con, it, Set);
    Set& set = getSetValue(it);
    appendReplyNumber(con, set.size());
}

void DB::setMembers(Context& con)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
    auto it = _hashMap.find(cmdlist[1]);
    if (it == _hashMap.end()) {
        con.append(db_return_nil);
        return;
    }
    checkType(con, it, Set);
    Set& set = getSetValue(it);
    appendReplyMulti(con, set.size());
    for (auto& it : set) {
        appendReplySingle(con, it);
    }
}

void DB::setInter(Context& con)
{
    auto& cmdlist = con.commandList();
    size_t size = cmdlist.size();
    size_t minSet = 0, minSetIndex = 0;
    for (auto i = 1; i < size; i++) {
        con.db()->isExpiredKey(cmdlist[i]);
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
        appendReplySingle(con, it);
}

void DB::setInterStore(Context& con)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
    size_t size = cmdlist.size();
    size_t minSet = 0, minSetIndex = 0;
    for (auto i = 2; i < size; i++) {
        con.db()->isExpiredKey(cmdlist[i]);
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

void DB::setUnion(Context& con)
{
    auto& cmdlist = con.commandList();
    size_t size = cmdlist.size();
    Set retSet;
    for (auto i = 1; i < size; i++) {
        con.db()->isExpiredKey(cmdlist[i]);
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
            appendReplySingle(con, it);
    }
}

void DB::setUnionStore(Context& con)
{
    auto& cmdlist = con.commandList();
    size_t size = cmdlist.size();
    Set retSet;
    for (auto i = 2; i < size; i++) {
        con.db()->isExpiredKey(cmdlist[i]);
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

void DB::setDiff(Context& con)
{
    // TODO:
}

void DB::setDiffStore(Context& con)
{
    // TODO:
}

//////////////////////////////////////////////////////////////////
// Hash Keys Operation
//////////////////////////////////////////////////////////////////

#define getHashValue(it) getXXType(it, Hash&)

void DB::hashSet(Context& con)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
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

void DB::hashSetIfNotExists(Context& con)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
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
        hash[cmdlist[2]] = cmdlist[3];
        con.append(db_return_integer_1);
    }
}

void DB::hashGet(Context& con)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
    auto it = _hashMap.find(cmdlist[1]);
    if (it == _hashMap.end()) {
        con.append(db_return_nil);
        return;
    }
    checkType(con, it, Hash);
    Hash& hash = getHashValue(it);
    auto value = hash.find(cmdlist[2]);
    if (value != hash.end()) {
        appendReplySingle(con, value->second);
    } else
        con.append(db_return_nil);
}

void DB::hashFieldExists(Context& con)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
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

void DB::hashDelete(Context& con)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
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
    appendReplyNumber(con, retval);
}

void DB::hashFieldLen(Context& con)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
    auto it = _hashMap.find(cmdlist[1]);
    if (it != _hashMap.end()) {
        checkType(con, it, Hash);
        Hash& hash = getHashValue(it);
        appendReplyNumber(con, hash.size());
    } else {
        con.append(db_return_integer_0);
    }
}

void DB::hashValueLen(Context& con)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
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

void DB::hashIncrBy(Context& con)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
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
}

void DB::hashMset(Context& con)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
    size_t size = cmdlist.size();
    if (size % 2 != 0) {
        con.append("-ERR wrong number of arguments for '" + cmdlist[0] + "'\r\n");
        return;
    }
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

void DB::hashMget(Context& con)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
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
            appendReplySingle(con, it->second);
        } else {
            con.append(db_return_nil);
        }
    }
}

#define GETKEYS     0
#define GETVALUES   1
#define GETALL      2

void DB::_hashGetXX(Context& con, int getXX)
{
    auto& cmdlist = con.commandList();
    con.db()->isExpiredKey(cmdlist[1]);
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
            appendReplySingle(con, it.first);
        } else if (getXX == GETVALUES) {
            appendReplySingle(con, it.second);
        } else if (getXX == GETALL) {
            appendReplySingle(con, it.first);
            appendReplySingle(con, it.second);
        }
    }
}

void DB::hashGetKeys(Context& con)
{
    _hashGetXX(con, GETKEYS);
}

void DB::hashGetValues(Context& con)
{
    _hashGetXX(con, GETVALUES);
}

void DB::hashGetAll(Context& con)
{
    _hashGetXX(con, GETALL);
}

#undef GETKEYS
#undef GETVALUES
#undef GETALL
