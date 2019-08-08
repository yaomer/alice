#ifndef _ALICE_SRC_DB_H
#define _ALICE_SRC_DB_H

#include <Angel/TcpServer.h>
#include <unordered_map>
#include <string>
#include <functional>
#include <list>
#include <vector>
#include <unordered_set>
#include <tuple>
#include <any>

namespace Alice {

class DBServer;
class Command;

extern thread_local int64_t _lru_cache;

enum CommandPerm {
    IS_READ = 0x01,
    IS_WRITE = 0x02,
    IS_INTER = 0x04,
};

class Context {
public:
    enum ParseState { 
        PARSING,        // 正在解析命令请求
        PROTOCOLERR,    // 协议错误
        SUCCEED,        // 解析完成
        REPLY,          // 发送响应
    };
    enum Flag{
        // 从服务器中设置该标志的连接表示与主服务器相连
        SLAVE = 0x01, // for slave
        // 主服务器向设置SYNC_RDB_FILE标志的连接发送rdb文件 
        // 从服务器设置该标志表示该连接处于接收同步文件的状态
        SYNC_RDB_FILE = 0x02, // for master and slave
        // 主服务器向设置SYNC_COMMAND标志的连接传播同步命令
        // 从服务器设置该标志表示该连接处于接收同步命令的状态
        SYNC_COMMAND = 0x04, // for master and slave
        // 从服务器处于等待接收主服务器的同步信息的状态
        SYNC_WAIT = 0x08, // for slave
        // 将要进行完全重同步
        SYNC_FULL = 0x10, // for slave
        SYNC_OK = 0x20, // for slave
    };
    explicit Context(DBServer *db, const Angel::TcpConnectionPtr& conn) 
        : _db(db),
        _conn(conn),
        _state(PARSING),
        _flag(0),
        _perm(IS_READ | IS_WRITE)
    {  
    }
    using CommandList = std::vector<std::string>;
    DBServer *db() { return _db; }
    const Angel::TcpConnectionPtr& conn() { return _conn; }
    void addArg(const char *s, const char *es)
    { _commandList.push_back(std::string(s, es)); }
    CommandList& commandList() { return _commandList; }
    void append(const std::string& s)
    { _buffer.append(s); }
    void assign(const std::string& s)
    { _buffer.assign(s); }
    std::string& message() { return _buffer; }
    int state() const { return _state; }
    void setState(int state) { _state = state; }
    int flag() const { return _flag; }
    void setFlag(int flag) { _flag |= flag; }
    void clearFlag(int flag) { _flag &= ~flag; }
    int perm() const { return _perm; }
    void setPerm(int perm) { _perm |= perm; }
    void clearPerm(int perm) { _perm &= ~perm; }
private:
    DBServer *_db;
    const Angel::TcpConnectionPtr _conn;
    // 请求命令表
    CommandList _commandList;
    // 发送缓冲区
    std::string _buffer;
    int _state;
    int _flag;
    // 能执行的命令的权限
    int _perm;
};

class Command {
public:
    using CommandCallback = std::function<void(Context&)>;

    Command(int arity, int perm, const CommandCallback _cb)
        : _commandCb(_cb),
        _arity(arity),
        _perm(perm)
    {
    }
    int arity() const { return _arity; }
    int perm() const { return _perm; }
    CommandCallback _commandCb;
private:
    int _arity;
    int _perm;
};

class Value {
public:
    Value() : _value(0), _lru(_lru_cache) {  }
    Value(std::any value) : _value(std::move(value)), _lru(_lru_cache)
    {
    }
    Value& operator=(std::any value)
    {
        this->_value = std::move(value);
        this->_lru = 0;
        return *this;
    }
    std::any& value() { return _value; }
    void setValue(std::any& value) { _value = std::move(value); }
    int64_t lru() { return _lru; }
    void setLru(int64_t lru) { _lru = lru; }
private:
    std::any _value;
    int64_t _lru;
};

class DB {
public:
    using Key = std::string;
    using Iterator = std::unordered_map<Key, Value>::iterator;
    using CommandMap = std::unordered_map<Key, Command>;
    using HashMap = std::unordered_map<Key, Value>;
    using String = std::string;
    using List = std::list<std::string>;
    using Set = std::unordered_set<std::string>;
    using Hash = std::unordered_map<std::string, std::string>;
    explicit DB(DBServer *);
    ~DB() {  }
    HashMap& hashMap() { return _hashMap; }
    void delKey(const Key& key)
    {
        auto it = _hashMap.find(key);
        if (it != _hashMap.end())
            _hashMap.erase(key);
    }
    CommandMap& commandMap() { return _commandMap; }

    void isKeyExists(Context& con);
    void getKeyType(Context& con);
    void getTtlSecs(Context& con);
    void getTtlMils(Context& con);
    void setKeyExpireSecs(Context& con);
    void setKeyExpireMils(Context& con);
    void deleteKey(Context& con);
    void getAllKeys(Context& con);
    void save(Context& con);
    void saveBackground(Context& con);
    void rewriteAof(Context& con);
    void lastSaveTime(Context& con);
    void flushDb(Context& con);
    void slaveOf(Context& con);
    void psync(Context& con);
    void replconf(Context& con);
    void ping(Context& con);
    void pong(Context& con);
    // String Keys Operation
    void strSet(Context& con);
    void strSetIfNotExist(Context& con);
    void strGet(Context& con);
    void strGetSet(Context& con);
    void strLen(Context& con);
    void strAppend(Context& con);
    void strMset(Context& con);
    void strMget(Context& con);
    void strIncr(Context& con);
    void strIncrBy(Context& con);
    void strDecr(Context& con);
    void strDecrBy(Context& con);
    // List Keys Operation
    void listLeftPush(Context& con);
    void listHeadPush(Context& con);
    void listRightPush(Context& con);
    void listTailPush(Context& con);
    void listLeftPop(Context& con);
    void listRightPop(Context& con);
    void listRightPopToLeftPush(Context& con);
    void listRem(Context& con);
    void listLen(Context& con);
    void listIndex(Context& con);
    void listSet(Context& con);
    void listRange(Context& con);
    void listTrim(Context& con);
    // Set Keys Operation
    void setAdd(Context& con);
    void setIsMember(Context& con);
    void setPop(Context& con);
    void setRandMember(Context& con);
    void setRem(Context& con);
    void setMove(Context& con);
    void setCard(Context& con);
    void setMembers(Context& con);
    void setInter(Context& con);
    void setInterStore(Context& con);
    void setUnion(Context& con);
    void setUnionStore(Context& con);
    void setDiff(Context& con);
    void setDiffStore(Context& con);
    // Hash Keys Operation
    void hashSet(Context& con);
    void hashSetIfNotExists(Context& con);
    void hashGet(Context& con);
    void hashFieldExists(Context& con);
    void hashDelete(Context& con);
    void hashFieldLen(Context& con);
    void hashValueLen(Context& con);
    void hashIncrBy(Context& con);
    void hashMset(Context& con);
    void hashMget(Context& con);
    void hashGetKeys(Context& con);
    void hashGetValues(Context& con);
    void hashGetAll(Context& con);
private:
    bool _strIsNumber(const String& s);
    void _getTtl(Context& con, bool seconds);
    void _setKeyExpire(Context& con, bool seconds);
    void _strIdCr(Context& con, int64_t incr);
    void _listPush(Context& con, bool leftPush);
    void _listEndsPush(Context& con, bool frontPush);
    void _listPop(Context& con, bool leftPop);
    void _hashGetXX(Context& con, int getXX);

    HashMap _hashMap;
    CommandMap _commandMap;
    DBServer *_dbServer;
};

const char *convert(int64_t value);

};

#endif
