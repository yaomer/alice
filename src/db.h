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
        SLAVE = 0x001, // for slave
        // 主服务器向设置SYNC_RDB_FILE标志的连接发送rdb文件 
        // 从服务器设置该标志表示该连接处于接收同步文件的状态
        SYNC_RDB_FILE = 0x002, // for master and slave
        // 主服务器向设置SYNC_COMMAND标志的连接传播同步命令
        // 从服务器设置该标志表示该连接处于接收同步命令的状态
        SYNC_COMMAND = 0x004, // for master and slave
        // 从服务器处于等待接收主服务器的同步信息的状态
        SYNC_WAIT = 0x008, // for slave
        // 将要进行完全重同步
        SYNC_FULL = 0x010, // for slave
        SYNC_OK = 0x020, // for slave
        // 客户端正在执行事务
        EXEC_MULTI = 0x040,
        // 事务的安全性被破坏
        EXEC_MULTI_ERR = 0x080,
        // 事务中有写操作
        EXEC_MULTI_WRITE = 0x100,
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
    using TransactionList = std::vector<CommandList>;

    DBServer *db() { return _db; }
    const Angel::TcpConnectionPtr& conn() { return _conn; }
    void addArg(const char *s, const char *es)
    { _commandList.push_back(std::string(s, es)); }
    CommandList& commandList() { return _commandList; }
    void addMultiArg(CommandList& cmdlist)
    { _transactionList.push_back(cmdlist); }
    TransactionList& transactionList() { return _transactionList; }
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
    // 事务队列
    TransactionList _transactionList;
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

    void existsCommand(Context& con);
    void typeCommand(Context& con);
    void ttlCommand(Context& con);
    void pttlCommand(Context& con);
    void expireCommand(Context& con);
    void pexpireCommand(Context& con);
    void delCommand(Context& con);
    void keysCommand(Context& con);
    void saveCommand(Context& con);
    void bgSaveCommand(Context& con);
    void bgRewriteAofCommand(Context& con);
    void lastSaveCommand(Context& con);
    void flushdbCommand(Context& con);
    void slaveofCommand(Context& con);
    void psyncCommand(Context& con);
    void replconfCommand(Context& con);
    void pingCommand(Context& con);
    void pongCommand(Context& con);
    void multiCommand(Context& con);
    void execCommand(Context& con);
    void discardCommand(Context& con);
    void watchCommand(Context& con);
    void unwatchCommand(Context& con);
    void publishCommand(Context& con);
    void subscribeCommand(Context& con);
    // String Keys Operation
    void setCommand(Context& con);
    void setnxCommand(Context& con);
    void getCommand(Context& con);
    void getSetCommand(Context& con);
    void strlenCommand(Context& con);
    void appendCommand(Context& con);
    void msetCommand(Context& con);
    void mgetCommand(Context& con);
    void incrCommand(Context& con);
    void incrbyCommand(Context& con);
    void decrCommand(Context& con);
    void decrbyCommand(Context& con);
    // List Keys Operation
    void lpushCommand(Context& con);
    void lpushxCommand(Context& con);
    void rpushCommand(Context& con);
    void rpushxCommand(Context& con);
    void lpopCommand(Context& con);
    void rpopCommand(Context& con);
    void rpoplpushCommand(Context& con);
    void lremCommand(Context& con);
    void llenCommand(Context& con);
    void lindexCommand(Context& con);
    void lsetCommand(Context& con);
    void lrangeCommand(Context& con);
    void ltrimCommand(Context& con);
    // Set Keys Operation
    void saddCommand(Context& con);
    void sisMemberCommand(Context& con);
    void spopCommand(Context& con);
    void srandMemberCommand(Context& con);
    void sremCommand(Context& con);
    void smoveCommand(Context& con);
    void scardCommand(Context& con);
    void smembersCommand(Context& con);
    void sinterCommand(Context& con);
    void sinterStoreCommand(Context& con);
    void sunionCommand(Context& con);
    void sunionStoreCommand(Context& con);
    void sdiffCommand(Context& con);
    void sdiffStoreCommand(Context& con);
    // Hash Keys Operation
    void hsetCommand(Context& con);
    void hsetnxCommand(Context& con);
    void hgetCommand(Context& con);
    void hexistsCommand(Context& con);
    void hdelCommand(Context& con);
    void hlenCommand(Context& con);
    void hstrlenCommand(Context& con);
    void hincrbyCommand(Context& con);
    void hmsetCommand(Context& con);
    void hmgetCommand(Context& con);
    void hkeysCommand(Context& con);
    void hvalsCommand(Context& con);
    void hgetAllCommand(Context& con);
private:
    bool _strIsNumber(const String& s);
    void _ttl(Context& con, bool seconds);
    void _expire(Context& con, bool seconds);
    void _incr(Context& con, int64_t incr);
    void _lpush(Context& con, bool leftPush);
    void _lpushx(Context& con, bool frontPush);
    void _lpop(Context& con, bool leftPop);
    void _hgetXX(Context& con, int getXX);

    HashMap _hashMap;
    CommandMap _commandMap;
    DBServer *_dbServer;
};

const char *convert(int64_t value);

};

#endif
