#ifndef _ALICE_SRC_DB_H
#define _ALICE_SRC_DB_H

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

class Context {
public:
    enum ParseStatus { 
        PARSING,
        PROTOCOLERR,
        SUCCEED,
        REPLY,
    };
    explicit Context(DBServer *db) 
        : _db(db),
        _flag(PARSING) 
    {  
    }
    using CommandList = std::vector<std::string>;
    DBServer *db() { return _db; }
    void addArg(const char *s, const char *es)
    { _commandList.push_back(std::string(s, es)); }
    CommandList& commandList() { return _commandList; }
    void append(const std::string& s)
    { _buffer.append(s); }
    void assign(const std::string& s)
    { _buffer.assign(s); }
    std::string& message() { return _buffer; }
    int8_t flag() const { return _flag; }
    void setFlag(int flag) { _flag = flag; }
private:
    DBServer *_db;
    CommandList _commandList;
    std::string _buffer;
    int8_t _flag;
};

class Command {
public:
    using CommandCallback = std::function<void(Context&)>;

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
    using String = std::string;
    using List = std::list<std::string>;
    using Set = std::unordered_set<std::string>;
    using Hash = std::unordered_map<std::string, std::string>;
    explicit DB(DBServer *);
    ~DB() {  }
    HashMap& hashMap() { return _hashMap; }
    void delKey(const Key& key)
    {
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
    void backgroundSave(Context& con);
    void lastSaveTime(Context& con);
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
