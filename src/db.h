#ifndef _ALICE_SRC_DB_H
#define _ALICE_SRC_DB_H

#include <Angel/TcpServer.h>
#include <Angel/util.h>

#include <unordered_map>
#include <string>
#include <functional>
#include <list>
#include <vector>
#include <unordered_set>
#include <set>
#include <tuple>
#include <any>

#include "util.h"

namespace Alice {

class DBServer;
class Command;

extern thread_local int64_t _lru_cache;

enum CommandPerm {
    IS_READ = 0x01,     // 可读命令
    IS_WRITE = 0x02,    // 可写命令
};

enum ReturnCode {
    C_OK,   // 函数执行成功
    C_ERR,  // 函数执行出错
};

// 一个Context表示一个客户端上下文
class Context {
public:
    enum Flag{
        // 主服务器中设置该标志的连接表示与从服务器相连
        SLAVE = 0x001, // for master
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
        // 客户端正在执行事务
        EXEC_MULTI = 0x040,
        // 事务的安全性被破坏
        EXEC_MULTI_ERR = 0x080,
        // 事务中有写操作
        EXEC_MULTI_WRITE = 0x100,
        SYNC_RECV_PING = 0x200,
        // 从服务器中设置该标志的连接表示与主服务器相连
        MASTER = 0x400, // for slave
        CON_BLOCK = 0x800,
    };
    explicit Context(DBServer *db, Angel::TcpConnection *conn)
        : _db(db),
        _conn(conn),
        _flag(0),
        _perm(IS_READ | IS_WRITE),
        _blockStartTime(-1),
        _blockTimeout(-1),
        _blockDbnum(-1)
    {
    }
    using CommandList = std::vector<std::string>;
    using TransactionList = std::vector<CommandList>;
    using WatchKeys = std::vector<std::string>;
    using BlockingKeys = std::vector<std::string>;

    DBServer *db() { return _db; }
    Angel::TcpConnection *conn() { return _conn; }
    Angel::InetAddr *slaveAddr() { return _slaveAddr.get(); }
    CommandList& commandList() { return _commandList; }
    TransactionList& transactionList() { return _transactionList; }
    WatchKeys& watchKeys() { return _watchKeys; }
    void append(const std::string& s)
    { _buffer.append(s); }
    void append(const char *s, size_t len)
    { _buffer.append(s, len); }
    void assign(const std::string& s)
    { _buffer.assign(s); }
    std::string& message() { return _buffer; }
    int flag() const { return _flag; }
    void setFlag(int flag) { _flag |= flag; }
    void clearFlag(int flag) { _flag &= ~flag; }
    int perm() const { return _perm; }
    void setPerm(int perm) { _perm |= perm; }
    void clearPerm(int perm) { _perm &= ~perm; }
    int64_t blockStartTime() const { return _blockStartTime; }
    int blockTimeout() const { return _blockTimeout; }
    int blockDbnum() const { return _blockDbnum; }
    void setBlockDbnum(int dbnum) { _blockDbnum = dbnum; }
    BlockingKeys& blockingKeys() { return _blockingKeys; }
    std::string& lastcmd() { return _lastcmd; }
    void setLastcmd(const std::string& cmd) { _lastcmd = cmd; }
    std::string& des() { return _des; }
    void setBlockTimeout(int timeout)
    {
        _blockStartTime = Angel::nowMs();
        _blockTimeout = timeout * 1000;
    }
    void setSlaveAddr(Angel::InetAddr slaveAddr)
    {
        _slaveAddr.reset(new Angel::InetAddr(slaveAddr.inetAddr()));
    }
private:
    DBServer *_db;
    Angel::TcpConnection *_conn;
    // 请求命令表
    CommandList _commandList;
    // 事务队列
    TransactionList _transactionList;
    WatchKeys _watchKeys;
    // 发送缓冲区
    std::string _buffer;
    int _flag;
    // 能执行的命令的权限
    int _perm;
    // 从服务器的inetAddr
    std::shared_ptr<Angel::InetAddr> _slaveAddr;
    // 执行阻塞命令开始阻塞的时间
    int64_t _blockStartTime;
    // 阻塞的时间
    int _blockTimeout;
    // 阻塞的所有keys
    BlockingKeys _blockingKeys;
    // 阻塞时操作的数据库
    int _blockDbnum;
    // brpoplpush中的des
    std::string _des;
    // 最后执行的命令
    std::string _lastcmd;
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
    // 命令回调，指向具体的实现
    CommandCallback _commandCb;
private:
    // 命令的合法参数个数
    int _arity;
    // 命令的执行权限
    int _perm;
};

// 表示一个键值对的值
class Value {
public:
    Value() : _value(0), _lru(_lru_cache) {  }
    Value(std::any&& value) : _value(value), _lru(_lru_cache)
    {
    }
    Value& operator=(std::any&& value) noexcept
    {
        this->_value = value;
        this->_lru = _lru_cache;
        return *this;
    }
    std::any& value() { return _value; }
    void setValue(std::any&& value) { _value = value; }
    int64_t lru() const { return _lru; }
    void updateLru() { _lru = _lru_cache; }
private:
    std::any _value;
    // 最近一次访问该键的时间，用于进行lru内存淘汰
    int64_t _lru;
};

// 有序集合的比较函数，对于对象l和r，如果l.score < r.score，就认为l < r
// 否则如果l.score == r.score，就继续比较键值，如果l.key < r.key，
// 就认为l < r，否则就认为l > r
class _ZsetCompare {
public:
    bool operator()(const std::tuple<double, std::string>& lhs,
                    const std::tuple<double, std::string>& rhs) const
    {
        double lf = std::get<0>(lhs);
        double rf = std::get<0>(rhs);
        if (lf < rf) {
            return true;
        } else if (lf == rf) {
            if (std::get<1>(lhs).size() > 0
             && std::get<1>(lhs).compare(std::get<1>(rhs)) < 0)
                return true;
        }
        return false;
    }
};

// 排序时会为每个待排序的元素(e)创建一个SortObject对象(s)，s->_value存储
// &e->value，s->_u的值取决于按哪种方式进行排序
struct SortObject {
    SortObject(const std::string *value)
        : _value(value)
    {
        _u.cmpVal = value;
    }
    const std::string *_value;
    union {
        double score;
        const std::string *cmpVal;
    } _u;
};

// 一个数据库实例
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
    // <分数，键值>
    using _Zset = std::multiset<std::tuple<double, std::string>, _ZsetCompare>;
    // 根据一个member可以在常数时间找到其score
    using _Zmap = std::unordered_map<std::string, double>;
    using Zset = std::tuple<_Zset, _Zmap>;
    // <键，键的到期时间>
    using ExpireMap = std::unordered_map<Key, int64_t>;
    // <键，监视该键的客户端列表>
    using WatchMap = std::unordered_map<Key, std::vector<size_t>>;
    // 因为排序结果集需要剪切，所以deque优于vector
    using SortObjectList = std::deque<SortObject>;
    // 保存所有阻塞的键，每个键的值是阻塞于它的客户端列表
    using BlockingKeys = std::unordered_map<Key, std::list<size_t>>;
    explicit DB(DBServer *);
    ~DB() {  }
    HashMap& hashMap() { return _hashMap; }
    void delKey(const Key& key) { _hashMap.erase(key); }
    void delKeyWithExpire(const Key& key)
    { _hashMap.erase(key); _expireMap.erase(key); }
    CommandMap& commandMap() { return _commandMap; }

    ExpireMap& expireMap() { return _expireMap; }
    void addExpireKey(const Key& key, int64_t expire)
    { _expireMap[key] = expire + Angel::nowMs(); }
    void delExpireKey(const Key& key) { _expireMap.erase(key); }
    void expireIfNeeded(const Key& key);

    BlockingKeys& blockingKeys() { return _blockingKeys; }
    void clearBlockingKeysForContext(Context& con);
    void blockMoveSrcToDes(const String& src, const String& des);
    void addBlockingKey(Context& con, const Key& key);
    void setContextToBlock(Context& con, int timeout);

    void watchKeyForClient(const Key& key, size_t id);
    void unwatchKeys(Context& con);
    void touchWatchKey(const Key& key);

    void selectCommand(Context& con);
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
    void flushAllCommand(Context& con);
    void slaveofCommand(Context& con);
    void psyncCommand(Context& con);
    void replconfCommand(Context& con);
    void pingCommand(Context& con);
    void multiCommand(Context& con);
    void execCommand(Context& con);
    void discardCommand(Context& con);
    void watchCommand(Context& con);
    void unwatchCommand(Context& con);
    void publishCommand(Context& con);
    void subscribeCommand(Context& con);
    void infoCommand(Context& con);
    void dbsizeCommand(Context& con);
    void sortCommand(Context& con);
    void renameCommand(Context& con);
    void renamenxCommand(Context& con);
    void moveCommand(Context& con);
    void lruCommand(Context& con);
    void configCommand(Context& con);
    void slowlogCommand(Context& con);
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
    void setRangeCommand(Context& con);
    void getRangeCommand(Context& con);
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
    void blpopCommand(Context& con);
    void brpopCommand(Context& con);
    void brpoplpushCommand(Context& con);
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
    // Zset Keys Operation
    void zaddCommand(Context& con);
    void zscoreCommand(Context& con);
    void zincrbyCommand(Context& con);
    void zcardCommand(Context& con);
    void zcountCommand(Context& con);
    void zrangeCommand(Context& con);
    void zrevRangeCommand(Context& con);
    void zrankCommand(Context& con);
    void zrevRankCommand(Context& con);
    void zrangeByScoreCommand(Context& con);
    void zrevRangeByScoreCommand(Context& con);
    void zremCommand(Context& con);
    void zremRangeByRankCommand(Context& con);
    void zremRangeByScoreCommand(Context& con);

    template <typename T>
    static void appendReplyMulti(Context& con, T size)
    {
        con.append("*");
        con.append(convert(size));
        con.append("\r\n");
    }
    static void appendReplyString(Context& con, const std::string& s)
    {
        con.append("$");
        con.append(convert(s.size()));
        con.append("\r\n" + s + "\r\n");
    }
    template <typename T>
    static void appendReplyNumber(Context& con, T number)
    {
        con.append(":");
        con.append(convert(number));
        con.append("\r\n");
    }
    static void appendReplyDouble(Context& con, double number)
    {
        con.append(":");
        con.append(convert2f(number));
        con.append("\r\n");
    }
private:
    Iterator find(const Key& key) { return _hashMap.find(key); }
    bool isFound(Iterator it) { return it != _hashMap.end(); }
    template <typename T>
    void insert(const Key& key, const T& value)
    {
        auto it = _hashMap.emplace(key, value);
        // emplace()和insert()都不会覆盖已存在的键
        if (!it.second) _hashMap[key] = std::move(value);
        if (typeid(T) == typeid(List))
            blockingPop(key);
    }

    void ttl(Context& con, int option);
    void expire(Context& con, int option);
    void incr(Context& con, int64_t incr);
    void lpush(Context& con, int option);
    void lpushx(Context& con, int option);
    void lpop(Context& con, int option);
    void blpop(Context& con, int option);
    void rpoplpush(Context& con, int option);
    void blockingPop(const std::string& key);
    void hgetXX(Context& con, int getXX);
    void zrange(Context& con, bool reverse);
    void zrank(Context& con, bool reverse);
    void zrangeByScore(Context& con, bool reverse);
    int checkRange(Context& con, int *start, int *stop, int lowerbound, int upperbound);
    int sortGetResult(Context& con, const String& key, SortObjectList& result, unsigned *cmdops);
    void sortByPattern(unsigned *cmdops, const String& by, SortObjectList& result);
    void sortByGetKeys(SortObjectList& result, unsigned cmdops, const std::vector<std::string>& get);
    void sortStore(SortObjectList& result, unsigned cmdops, const String& des);
    void configGet(Context& con, const std::string& arg);
    void configSet(Context& con, const std::string& arg, const std::string& value);
    void slowlogGet(Context& con, Context::CommandList& cmdlist);

    DBServer *_dbServer;
    HashMap _hashMap; // 存储所有数据
    CommandMap _commandMap; // 命令表
    ExpireMap _expireMap; // 存储所有过期键
    WatchMap _watchMap; // 存储所有客户watch的键
    BlockingKeys _blockingKeys; // 存储所有阻塞的键
};

struct ReplyString {
    const char *ok = "+OK\r\n";
    const char *nil = "$-1\r\n";
    const char *n0 = ":0\r\n";
    const char *n1 = ":1\r\n";
    const char *n_1 = ":-1\r\n";
    const char *n_2 = ":-2\r\n";
    const char *multi_empty = "*0\r\n";
    const char *type_err = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    const char *integer_err = "-ERR value is not an integer or out of range\r\n";
    const char *float_err = "-ERR value is not a valid float\r\n";
    const char *syntax_err = "-ERR syntax error\r\n";
    const char *no_such_key = "-ERR no such key\r\n";
    const char *subcommand_err = "-ERR Unknown subcommand or wrong argument\r\n";
    const char *argnumber_err = "-ERR wrong number of arguments\r\n";
    const char *timeout_err = "-ERR invalid expire timeout\r\n";
    const char *timeout_out_of_range = "-ERR timeout is out of range\r\n";
    const char *invalid_db_index = "-ERR invalid DB index\r\n";
    const char *db_index_out_of_range = "-ERR DB index is out of range\r\n";
    const char *index_out_of_range = "-ERR index is out of range\r\n";
    const char *unknown_option = "-ERR unknown option\r\n";
    const char *none_type = "+none\r\n";
    const char *string_type = "+string\r\n";
    const char *list_type = "+list\r\n";
    const char *hash_type = "+hash\r\n";
    const char *set_type = "+set\r\n";
    const char *zset_type = "+zset\r\n";
};

}

extern Alice::ReplyString reply;

// typeof(it) == HashMap::iterator
#define isType(it, _type) \
    ((it)->second.value().type() == typeid(_type))
#define checkType(con, it, _type) \
    do { \
        if (!isType(it, _type)) { \
            (con).append(reply.type_err); \
            return; \
        } \
        (it)->second.updateLru(); \
    } while (0)
#define getValue(it, _type) \
    (std::any_cast<_type>((it)->second.value()))

#define getStringValue(it)  getValue(it, DB::String&)
#define getListValue(it)    getValue(it, DB::List&)
#define getHashValue(it)    getValue(it, DB::Hash&)
#define getSetValue(it)     getValue(it, DB::Set&)
#define getZsetValue(it)    getValue(it, DB::Zset&)

#define db_return(con, str) \
    do { (con).append(str); return; } while(0)

#endif
