#ifndef _ALICE_SRC_DB_H
#define _ALICE_SRC_DB_H

#include <unordered_map>
#include <string>
#include <functional>
#include <any>

namespace Alice {

class DBServer;
class Command;

extern thread_local int64_t _lru_cache;

class Connection {
public:
    enum ParseStatus { 
        PARSING,
        PROTOCOLERR,
        SUCCEED,
        REPLY,
    };
    explicit Connection(DBServer *db) 
        : _db(db),
        _command(nullptr),
        _flag(PARSING) 
    {  
    }
    using CommandList = std::vector<std::string>;
    DBServer *db() { return _db; }
    Command *command() { return _command; }
    void addArg(const char *s, const char *es)
    { _commandList.push_back(std::string(s, es)); }
    CommandList& commandList() { return _commandList; }
    void append(const std::string& s)
    { _buffer.append(s); }
    std::string& message() { return _buffer; }
    int8_t flag() const { return _flag; }
    void setFlag(int flag) { _flag = flag; }
private:
    DBServer *_db;
    CommandList _commandList;
    Command *_command;
    std::string _buffer;
    int8_t _flag;
};

class Command {
public:
    using CommandCallback = std::function<void(Connection&)>;

    Command(const std::string& name, int8_t arity, bool isWrite, const CommandCallback _cb)
        : _commandCb(_cb),
        _name(name),
        _arity(arity),
        _isWrite(isWrite)
    {
    }
    const std::string& name() const { return _name; }
    int8_t arity() const { return _arity; }
    bool isWrite() const { return _isWrite; }
    CommandCallback _commandCb;
private:
    std::string _name;
    int8_t _arity;
    bool _isWrite;
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
    // enum { String, List, H }
    DB();
    ~DB() {  }
    HashMap& hashMap() { return _hashMap; }
    void delKey(const Key& key)
    {
        _hashMap.erase(key);
    }
    CommandMap& commandMap() { return _commandMap; }
    void set(Connection& conn);
    void setnx(Connection& conn);
    void get(Connection& conn);
    void getset(Connection& conn);
    void strlen(Connection& conn);
    void append(Connection& conn);
    void mset(Connection& conn);
    void mget(Connection& conn);
    void incr(Connection& conn);
    void incrby(Connection& conn);
    void decr(Connection& conn);
    void decrby(Connection& conn);
    void ttl(Connection& conn);
private:
    void _idcr(Connection& conn, int64_t incr);

    HashMap _hashMap;
    CommandMap _commandMap;
};
};

#endif
