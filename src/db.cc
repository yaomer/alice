#include <stdlib.h>
#include <time.h>
#include <ctype.h>
#include <unistd.h>

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
        { "SETRANGE",   { -4, IS_WRITE, BIND(setRangeCommand) } },
        { "GETRANGE",   { -4, IS_READ,  BIND(getRangeCommand) } },
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
        { "BLPOP",      {  3, IS_READ,  BIND(blpopCommand) } },
        { "BRPOP",      {  3, IS_READ,  BIND(brpopCommand) } },
        { "BRPOPLPUSH", { -4, IS_READ,  BIND(brpoplpushCommand) } },
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
        { "ZADD",       {  4, IS_WRITE, BIND(zaddCommand) } },
        { "ZSCORE",     { -3, IS_READ,  BIND(zscoreCommand) } },
        { "ZINCRBY",    { -4, IS_WRITE, BIND(zincrbyCommand) } },
        { "ZCARD",      { -2, IS_READ,  BIND(zcardCommand) } },
        { "ZCOUNT",     { -4, IS_READ,  BIND(zcountCommand) } },
        { "ZRANGE",     {  4, IS_READ,  BIND(zrangeCommand) } },
        { "ZREVRANGE",  {  4, IS_READ,  BIND(zrevRangeCommand) } },
        { "ZRANK",      { -3, IS_READ,  BIND(zrankCommand) } },
        { "ZREVRANK",   { -3, IS_READ,  BIND(zrevRankCommand) } },
        { "ZREM",       {  3, IS_WRITE, BIND(zremCommand) } },
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
        { "SORT",       {  2, IS_READ,  BIND(sortCommand) } },
        { "RENAME",     { -3, IS_WRITE, BIND(renameCommand) } },
        { "RENAMENX",   { -3, IS_WRITE, BIND(renamenxCommand) } },
        { "MOVE",       { -3, IS_WRITE, BIND(moveCommand) } },
        { "LRU",        { -2, IS_READ,  BIND(lruCommand) } },
        { "CONFIG",     {  3, IS_READ,  BIND(configCommand) } },
        { "SLOWLOG",    {  2, IS_READ,  BIND(slowlogCommand) } },
        { "ZRANGEBYSCORE",      {  4, IS_READ,  BIND(zrangeByScoreCommand) } },
        { "ZREVRANGEBYSCORE",   {  4, IS_READ,  BIND(zrevRangeByScoreCommand) } },
        { "ZREMRANGEBYRANK",    { -4, IS_WRITE, BIND(zremRangeByRankCommand) } },
        { "ZREMRANGEBYSCORE",   { -4, IS_WRITE, BIND(zremRangeByScoreCommand) } },
    };
}

Alice::ReplyString reply;

void DB::selectCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    int dbnum = str2l(cmdlist[1].c_str());
    if (str2numberErr()) db_return(con, reply.invalid_db_index);
    if (dbnum < 0 || dbnum >= g_server_conf.databases) {
        db_return(con, reply.db_index_out_of_range);
    }
    con.db()->switchDb(dbnum);
    con.append(reply.ok);
}

void DB::existsCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    expireIfNeeded(cmdlist[1]);
    con.append(isFound(find(cmdlist[1])) ? reply.n1 : reply.n0);
}

void DB::typeCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    expireIfNeeded(cmdlist[1]);
    auto it = find(cmdlist[1]);
    if (!isFound(it)) db_return(con, reply.none_type);
    if (isType(it, String))
        con.append(reply.string_type);
    else if (isType(it, List))
        con.append(reply.list_type);
    else if (isType(it, Set))
        con.append(reply.set_type);
    else if (isType(it, Zset))
        con.append(reply.zset_type);
    else if (isType(it, Hash))
        con.append(reply.hash_type);
}

#define TTL 1
#define PTTL 2

void DB::ttl(Context& con, int option)
{
    auto& cmdlist = con.commandList();
    if (!isFound(find(cmdlist[1]))) db_return(con, reply.n_2);
    auto expire = expireMap().find(cmdlist[1]);
    if (expire == expireMap().end()) db_return(con, reply.n_1);
    int64_t milliseconds = expire->second - Angel::nowMs();
    if (option == TTL) milliseconds /= 1000;
    con.appendReplyNumber(milliseconds);
}

void DB::ttlCommand(Context& con)
{
    ttl(con, TTL);
}

void DB::pttlCommand(Context& con)
{
    ttl(con, PTTL);
}

#define EXPIRE 1
#define PEXPIRE 2

void DB::expire(Context& con, int option)
{
    auto& cmdlist = con.commandList();
    int64_t expire = str2ll(cmdlist[2].c_str());
    if (str2numberErr()) db_return(con, reply.integer_err);
    if (expire <= 0) db_return(con, reply.timeout_err);
    if (!isFound(find(cmdlist[1]))) db_return(con, reply.n0);
    if (option == EXPIRE) expire *= 1000;
    addExpireKey(cmdlist[1], expire);
    con.append(reply.n1);
}

void DB::expireCommand(Context& con)
{
    expire(con, EXPIRE);
}

void DB::pexpireCommand(Context& con)
{
    expire(con, PEXPIRE);
}

void DB::delCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    size_t size = cmdlist.size();
    int retval = 0;
    for (size_t i = 1; i < size; i++) {
        if (isFound(find(cmdlist[i]))) {
            delKeyWithExpire(cmdlist[i]);
            retval++;
        }
    }
    con.appendReplyNumber(retval);
}

void DB::keysCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    if (cmdlist[1].compare("*")) db_return(con, reply.unknown_option);
    con.appendReplyMulti(_hashMap.size());
    for (auto& it : _hashMap)
        con.appendReplyString(it.first);
}

void DB::saveCommand(Context& con)
{
    if (_dbServer->rdb()->childPid() != -1) return;
    _dbServer->rdb()->save();
    con.append(reply.ok);
    _dbServer->setLastSaveTime(Angel::nowMs());
    _dbServer->dirtyReset();
}

void DB::bgSaveCommand(Context& con)
{
    // 有rdb或aof持久化在进行中，忽略此次请求
    if (_dbServer->aof()->childPid() != -1) {
        db_return(con, "+Background append only file rewriting ...\r\n");
    }
    if (_dbServer->rdb()->childPid() != -1) return;
    _dbServer->rdb()->saveBackground();
    con.append("+Background saving started\r\n");
    _dbServer->setLastSaveTime(Angel::nowMs());
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
    if (_dbServer->aof()->childPid() != -1) return;
    _dbServer->aof()->rewriteBackground();
    con.append("+Background append only file rewriting started\r\n");
    _dbServer->setLastSaveTime(Angel::nowMs());
}

void DB::lastSaveCommand(Context& con)
{
    con.appendReplyNumber(_dbServer->lastSaveTime());
}

void DB::flushdbCommand(Context& con)
{
    _hashMap.clear();
    con.append(reply.ok);
}

void DB::flushAllCommand(Context& con)
{
    for (auto& db : _dbServer->dbs())
        db->hashMap().clear();
    if (!g_server_conf.save_params.empty()) bgSaveCommand(con);
    if (g_server_conf.enable_appendonly) bgRewriteAofCommand(con);
    con.message().assign(reply.ok);
}

void DB::slaveofCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    if (strcasecmp(cmdlist[1].c_str(), "no") == 0
            && strcasecmp(cmdlist[2].c_str(), "one") == 0) {
        _dbServer->disconnectMasterServer();
        _dbServer->clearFlag(Context::SLAVE);
        db_return(con, reply.ok);
    }
    int port = str2l(cmdlist[2].c_str());
    if (str2numberErr()) db_return(con, reply.integer_err);
    con.append(reply.ok);
    if (port == g_server_conf.port && cmdlist[1].compare(g_server_conf.addr) == 0) {
        logWarn("try connect to self");
        return;
    }
    logInfo("connect to %s:%d", cmdlist[1].c_str(), port);
    _dbServer->setMasterAddr(Angel::InetAddr(port, cmdlist[1].c_str()));
    _dbServer->connectMasterServer();
}

void DB::psyncCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    if (cmdlist[1].compare("?") == 0 && cmdlist[2].compare("-1") == 0) {
sync:
        // 执行完整重同步
        con.setFlag(Context::SLAVE | Context::SYNC_RDB_FILE);
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
        size_t slaveoff = atoll(cmdlist[2].c_str());
        size_t off = _dbServer->masterOffset() - slaveoff;
        if (off > 0 && off <= _dbServer->copyBacklogBuffer().size())
            goto sync;
        // 执行部分重同步
        con.setFlag(Context::SYNC_COMMAND);
        con.append("+CONTINUE\r\n");
        _dbServer->appendPartialResyncData(con, off);
    }
}

void DB::replconfCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    if (strcasecmp(cmdlist[1].c_str(), "port") == 0) {
        auto it = _dbServer->slaves().find(con.conn()->id());
        if (it == _dbServer->slaves().end()) {
            logInfo("found a new slave %s:%s", cmdlist[4].c_str(), cmdlist[2].c_str());
            _dbServer->slaves().emplace(con.conn()->id(), _dbServer->masterOffset());
        }
        con.setSlaveAddr(
                Angel::InetAddr(atoi(cmdlist[2].c_str()), cmdlist[4].c_str()));
    } else if (strcasecmp(cmdlist[1].c_str(), "ack") == 0) {
        size_t slaveoff = atoll(cmdlist[2].c_str());
        auto it = _dbServer->slaves().find(con.conn()->id());
        it->second = slaveoff;
    }
}

void DB::pingCommand(Context& con)
{
    con.append("+PONG\r\n");
}

void DB::multiCommand(Context& con)
{
    con.setFlag(Context::EXEC_MULTI);
    con.append(reply.ok);
}

void DB::execCommand(Context& con)
{
    Context::CommandList tlist = { "MULTI" };
    bool multiIsWrite = (con.flag() & Context::EXEC_MULTI_WRITE);
    if (con.flag() & Context::EXEC_MULTI_ERR) {
        con.clearFlag(Context::EXEC_MULTI_ERR);
        con.append(reply.nil);
        goto end;
    }
    if (multiIsWrite) {
        _dbServer->freeMemoryIfNeeded();
        _dbServer->doWriteCommand(tlist, nullptr, 0);
    }
    for (auto& cmdlist : con.transactionList()) {
        auto command = _commandMap.find(cmdlist[0]);
        con.commandList().swap(cmdlist);
        command->second._commandCb(con);
        if (multiIsWrite)
            _dbServer->doWriteCommand(con.commandList(), nullptr, 0);
    }
    if (multiIsWrite) {
        tlist = { "EXEC" };
        _dbServer->doWriteCommand(tlist, nullptr, 0);
        con.clearFlag(Context::EXEC_MULTI_WRITE);
    }
    unwatchKeys(con);
end:
    con.transactionList().clear();
    con.clearFlag(Context::EXEC_MULTI);
}

void DB::discardCommand(Context& con)
{
    con.transactionList().clear();
    unwatchKeys(con);
    con.clearFlag(Context::EXEC_MULTI);
    con.clearFlag(Context::EXEC_MULTI_ERR);
    con.clearFlag(Context::EXEC_MULTI_WRITE);
    con.append(reply.ok);
}

void DB::watchCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    for (size_t i = 1; i < cmdlist.size(); i++) {
        expireIfNeeded(cmdlist[i]);
        if (isFound(find(cmdlist[i]))) {
            watchKeyForClient(cmdlist[i], con.conn()->id());
            con.watchKeys().emplace_back(cmdlist[i]);
        }
    }
    con.append(reply.ok);
}

void DB::unwatchCommand(Context& con)
{
    unwatchKeys(con);
    con.append(reply.ok);
}

void DB::unwatchKeys(Context& con)
{
    for (auto& key : con.watchKeys()) {
        auto clist = _watchMap.find(key);
        if (clist != _watchMap.end()) {
            for (auto c = clist->second.begin(); c != clist->second.end(); ++c) {
                if (*c == con.conn()->id()) {
                    clist->second.erase(c);
                    break;
                }
            }
        }
    }
    con.watchKeys().clear();
}

void DB::publishCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    size_t subClients = _dbServer->pubMessage(cmdlist[2], cmdlist[1], con.conn()->id());
    con.appendReplyNumber(subClients);
}

void DB::subscribeCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    con.append("+Reading messages... (press Ctrl-C to quit)\r\n");
    for (size_t i = 1; i < cmdlist.size(); i++) {
        _dbServer->subChannel(cmdlist[i], con.conn()->id());
        con.appendReplyMulti(3);
        con.appendReplyString("subscribe");
        con.appendReplyString(cmdlist[i]);
        con.appendReplyNumber(i);
    }
}

void DB::infoCommand(Context& con)
{
    int i = 0;
    con.append("+run_id:");
    con.append(_dbServer->selfRunId());
    con.append("\n");
    con.append("role:");
    if (_dbServer->flag() & DBServer::SLAVE) db_return(con, "slave\r\n");
    con.append("master\n");
    con.append("connected_slaves:");
    con.append(convert(_dbServer->slaves().size()));
    con.append("\n");
    for (auto& it : _dbServer->slaves()) {
        auto conn = g_server->server().getConnection(it.first);
        if (!conn) continue;
        auto& context = std::any_cast<Context&>(conn->getContext());
        con.append("slave");
        con.append(convert(i));
        con.append(":ip=");
        con.append(context.slaveAddr()->toIpAddr());
        con.append(",port=");
        con.append(convert(context.slaveAddr()->toIpPort()));
        con.append(",offset=");
        con.append(convert(it.second));
        con.append("\n");
        i++;
    }
    // 一个无意义的填充字段，方便以\r\n结尾
    con.append("xxx:yyy\r\n");
}

void DB::dbsizeCommand(Context& con)
{
    con.appendReplyNumber(_hashMap.size());
}

void DB::expireIfNeeded(const Key& key)
{
    auto it = expireMap().find(key);
    if (it == expireMap().end()) return;
    int64_t now = _lru_cache;
    if (it->second > now) return;
    delKeyWithExpire(key);
    Context::CommandList cmdlist = { "DEL", key };
    _dbServer->appendWriteCommand(cmdlist, nullptr, 0);
}

void DB::watchKeyForClient(const Key& key, size_t id)
{
    auto it = _watchMap.find(key);
    if (it == _watchMap.end()) {
        std::vector<size_t> clist = { id };
        _watchMap.emplace(key, std::move(clist));
    } else {
        it->second.push_back(id);
    }
}

void DB::touchWatchKey(const Key& key)
{
    auto clist = _watchMap.find(key);
    if (clist == _watchMap.end()) return;
    for (auto& id : clist->second) {
        auto conn = g_server->server().getConnection(id);
        if (!conn) continue;
        auto& context = std::any_cast<Context&>(conn->getContext());
        context.setFlag(Context::EXEC_MULTI_ERR);
    }
}

void DB::renameCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    expireIfNeeded(cmdlist[1]);
    auto it = find(cmdlist[1]);
    if (!isFound(it)) db_return(con, reply.no_such_key);
    if (strcasecmp(cmdlist[1].c_str(), cmdlist[2].c_str()) == 0) {
        db_return(con, reply.ok);
    }
    delKeyWithExpire(cmdlist[2]);
    insert(cmdlist[2], it->second);
    delKeyWithExpire(cmdlist[1]);
    con.append(reply.ok);
}

void DB::renamenxCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    expireIfNeeded(cmdlist[1]);
    auto it = find(cmdlist[1]);
    if (!isFound(it)) db_return(con, reply.no_such_key);
    if (strcasecmp(cmdlist[1].c_str(), cmdlist[2].c_str()) == 0) {
        db_return(con, reply.n0);
    }
    if (isFound(find(cmdlist[2]))) db_return(con, reply.n0);
    insert(cmdlist[2], it->second);
    delKeyWithExpire(cmdlist[1]);
    con.append(reply.n1);
}

void DB::moveCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    int dbnum = str2l(cmdlist[2].c_str());
    if (str2numberErr()) db_return(con, reply.integer_err);
    if (dbnum < 0 || dbnum >= g_server_conf.databases) {
        db_return(con, reply.db_index_out_of_range);
    }
    auto it = find(cmdlist[1]);
    if (!isFound(it)) db_return(con, reply.n0);
    auto& map = _dbServer->selectDb(dbnum)->hashMap();
    if (map.find(cmdlist[1]) != map.end()) db_return(con, reply.n0);
    map.emplace(it->first, it->second);
    delKeyWithExpire(cmdlist[1]);
    con.append(reply.n1);
}

void DB::lruCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    expireIfNeeded(cmdlist[1]);
    auto it = find(cmdlist[1]);
    if (!isFound(it)) db_return(con, reply.n_1);
    int64_t seconds = (_lru_cache - it->second.lru()) / 1000;
    con.appendReplyNumber(seconds);
}

#define SlOWLOG_GET_LOGS 10

void DB::slowlogGet(Context& con, Context::CommandList& cmdlist)
{
    int count = 0;
    if (cmdlist.size() > 2) {
        count = str2l(cmdlist[2].c_str());
        if (str2numberErr()) db_return(con, reply.integer_err);
        if (count <= 0) db_return(con, reply.multi_empty);
    } else
        count = SlOWLOG_GET_LOGS;
    if (count > _dbServer->slowlogQueue().size())
        count = _dbServer->slowlogQueue().size();
    con.appendReplyMulti(count);
    for (auto& it : _dbServer->slowlogQueue()) {
        con.appendReplyMulti(4);
        con.appendReplyString(convert(it._id));
        con.appendReplyString(convert(it._time));
        con.appendReplyString(convert(it._duration));
        con.appendReplyMulti(it._args.size());
        for (auto& arg : it._args)
            con.appendReplyString(arg);
    }
}

void DB::slowlogCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    if (strcasecmp(cmdlist[1].c_str(), "get") == 0) {
        slowlogGet(con, cmdlist);
    } else if (strcasecmp(cmdlist[1].c_str(), "len") == 0) {
        con.appendReplyNumber(_dbServer->slowlogQueue().size());
    } else if (strcasecmp(cmdlist[1].c_str(), "reset") == 0) {
        _dbServer->slowlogQueue().clear();
        con.append(reply.ok);
    } else
        con.append(reply.subcommand_err);
}

// 清空con.blockingKeys()，并从DB::blockingKeys()中移除所有con
void DB::clearBlockingKeysForContext(Context& con)
{
    for (auto& it : con.blockingKeys()) {
        auto cl = blockingKeys().find(it);
        if (cl != blockingKeys().end()) {
            for (auto c = cl->second.begin(); c != cl->second.end(); ++c) {
                if (*c == con.conn()->id()) {
                    cl->second.erase(c);
                    break;
                }
            }
            if (cl->second.empty())
                blockingKeys().erase(it);
        }
    }
    con.clearFlag(Context::CON_BLOCK);
    con.blockingKeys().clear();
}
