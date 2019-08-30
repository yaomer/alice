#include <deque>
#include <algorithm>
#include "db.h"

using namespace Alice;

// 执行一个SORT命令，一般分为以下几步：
// 使用[BY] [ASC] [DESC] [ALPHA]对输入键进行排序，得到一个结果集
// 使用[LIMIT]过滤结果集
// 使用[GET]提供的外键来获取新的结果集
// 使用[STORE]保存结果集

#define SORT_BY         0x001
#define SORT_LIMIT      0x002
#define SORT_GET        0x004
#define SORT_ASC        0x008
#define SORT_DESC       0x010
#define SORT_ALPHA      0x020
#define SORT_STORE      0x040
#define SORT_LIST_TYPE  0x080
#define SORT_SET_TYPE   0x100
#define SORT_GET_VAL    0x200
#define SORT_NOT        0x400

namespace Alice {
    thread_local std::unordered_map<std::string, int> sortops = {
        { "BY",     SORT_BY },
        { "LIMIT",  SORT_LIMIT },
        { "GET",    SORT_GET },
        { "ASC",    SORT_ASC },
        { "DESC",   SORT_DESC },
        { "ALPHA",  SORT_ALPHA },
        { "STORE",  SORT_STORE },
    };

template <typename T>
class less {
public:
    bool operator()(const SortObject& lhs, const SortObject& rhs) const
    {
        if (typeid(T) == typeid(std::string))
            return lhs._u.cmpVal->compare(*rhs._u.cmpVal) < 0;
        else
            return lhs._u.score < rhs._u.score;
    }
};

template <typename T>
class greater {
public:
    bool operator()(const SortObject& lhs, const SortObject& rhs) const
    {
        if (typeid(T) == typeid(std::string))
            return lhs._u.cmpVal->compare(*rhs._u.cmpVal) > 0;
        else
            return lhs._u.score > rhs._u.score;
    }
};
}

static int parseSortArgs(Context& con, unsigned *cmdops, std::string& key, std::string& by,
        std::string& des, std::vector<std::string>& get, int *offset, int *count)
{
    auto& cmdlist = con.commandList();
    size_t len = cmdlist.size();
    key = cmdlist[1];
    for (size_t i = 2; i < len; i++) {
        std::transform(cmdlist[i].begin(), cmdlist[i].end(), cmdlist[i].begin(), ::toupper);
        auto op = sortops.find(cmdlist[i]);
        if (op != sortops.end()) *cmdops |= op->second;
        else goto syntax_err;
        switch (op->second) {
        case SORT_ASC: case SORT_DESC: case SORT_ALPHA:
            break;
        case SORT_BY: {
            if (i + 1 >= len) goto syntax_err;
            by.assign(cmdlist[++i]);
            break;
        }
        case SORT_LIMIT: {
            if (i + 2 >= len) goto syntax_err;
            *offset = str2l(cmdlist[++i].c_str());
            if (str2numberErr()) {
                con.append(db_return_integer_err);
                return C_ERR;
            }
            *count = str2l(cmdlist[++i].c_str());
            if (str2numberErr()) {
                con.append(db_return_integer_err);
                return C_ERR;
            }
            break;
        }
        case SORT_GET: {
            if (i + 1 >= len) goto syntax_err;
            if (cmdlist[i+1].compare("#") == 0)
                *cmdops |= SORT_GET_VAL;
            else
                get.push_back(cmdlist[i+1]);
            i++;
            break;
        }
        case SORT_STORE: {
            if (i + 1 >= len) goto syntax_err;
            des.assign(cmdlist[++i]);
            break;
        }
        }
    }
    return C_OK;
syntax_err:
    con.append(db_return_syntax_err);
    return C_ERR;
}

int DB::sortGetResult(Context& con, const std::string& key, DB::SortObjectList& result,
        unsigned *cmdops)
{
    expireIfNeeded(key);
    auto it = find(key);
    if (!isFound(it)) {
        con.append(db_return_nil);
        return C_ERR;
    }
    if (isXXType(it, List)) {
        *cmdops |= SORT_LIST_TYPE;
        List& list = getListValue(it);
        for (auto& it : list)
            result.emplace_back(&it);
    } else if (isXXType(it, Set)) {
        *cmdops |= SORT_SET_TYPE;
        Set& set = getSetValue(it);
        for (auto& it : set)
            result.emplace_back(&it);
    } else {
        con.append(db_return_type_err);
        return C_ERR;
    }
    return C_OK;
}

void DB::sortByPattern(unsigned *cmdops, const String& by, SortObjectList& result)
{
    std::string key;
    auto star = std::find(by.begin(), by.end(), '*');
    if (star != by.end()) {
        for (auto& it : result) {
            if (star == by.begin()) {
                key = *it._value + by.substr(1, by.size());
            } else if (star == --by.end()) {
                key = by.substr(0, by.size() - 1) + *it._value;
            } else {
                key = by.substr(0, star - by.begin()) + *it._value
                    + by.substr(star - by.begin(), by.size());
            }
            auto e = find(key);
            if (!isFound(e) || !isXXType(e, String)) {
                it._u.cmpVal = it._value;
            } else {
                String& val = getStringValue(e);
                it._u.cmpVal = &val;
            }
        }
    } else {
        *cmdops |= SORT_NOT;
    }
}

static int sort(Context& con, unsigned cmdops, DB::SortObjectList& result)
{
    if (!(cmdops & SORT_ALPHA)) {
        for (auto& it : result) {
            double v = str2f(it._u.cmpVal->c_str());
            if (str2numberErr()) {
                con.append("-ERR One or more scores can't be converted into double\r\n");
                return C_ERR;
            }
            it._u.score = v;
        }
        if (cmdops & SORT_DESC)
            std::sort(result.begin(), result.end(), Alice::greater<double>());
        else
            std::sort(result.begin(), result.end(), Alice::less<double>());
    } else {
        if (cmdops & SORT_DESC)
            std::sort(result.begin(), result.end(), Alice::greater<std::string>());
        else
            std::sort(result.begin(), result.end(), Alice::less<std::string>());
    }
    return C_OK;
}

static int sortLimit(Context& con, DB::SortObjectList& result, int offset, int count)
{
    if (offset < 0 || count <= 0) {
        con.append(db_return_nil);
        return C_ERR;
    }
    int size = result.size();
    if (offset >= size || offset + count > size) {
        con.append(db_return_nil);
        return C_ERR;
    }
    int i = 0;
    for (auto it = result.begin(); it != result.end(); ) {
        if (i < offset) {
            ++it;
            result.pop_front();
            i++;
            continue;
        }
        if (i++ >= count) {
            for (auto end = result.end(); end != it; ) {
                --end;
                result.pop_back();
            }
            break;
        }
        ++it;
    }
    return C_OK;
}

void DB::sortByGetKeys(SortObjectList& result, unsigned cmdops, const std::vector<std::string>& get)
{
    std::deque<SortObject> tres;
    std::string key;
    for (auto& it : result) {
        if (cmdops & SORT_GET_VAL)
            tres.emplace_back(it._value);
        for (auto& p : get) {
            auto star = std::find(p.begin(), p.end(), '*');
            if (star == p.end()) {
                tres.emplace_back(nullptr);
                continue;
            }
            if (star == p.begin()) {
                key = *it._value + p.substr(1, p.size());
            } else if (star == --p.end()) {
                key = p.substr(0, p.size() - 1) + *it._value;
            } else {
                key = p.substr(0, star - p.begin()) + *it._value
                    + p.substr(star - p.begin(), p.size());
            }
            auto e = find(key);
            if (isFound(e) && isXXType(e, String)) {
                String& val = getStringValue(e);
                tres.emplace_back(&val);
            } else
                tres.emplace_back(nullptr);
        }
    }
    result.swap(tres);
}

void DB::sortStore(SortObjectList& result, unsigned cmdops, const String& des)
{
    if (cmdops & SORT_LIST_TYPE) {
        List list;
        for (auto& it : result)
            list.emplace_back(it._value ? *it._value : "");
        insert(des, std::move(list));
    } else if (cmdops & SORT_SET_TYPE) {
        Set set;
        for (auto& it : result)
            set.emplace(it._value ? *it._value : "");
        insert(des, std::move(set));
    }
}

void DB::sortCommand(Context& con)
{
    unsigned cmdops = 0;
    String key, by, des;
    int offset = 0, count = 0;
    std::vector<String> get;
    if (parseSortArgs(con, &cmdops, key, by, des, get, &offset, &count) == C_ERR)
        return;
    SortObjectList result;
    if (sortGetResult(con, key, result, &cmdops) == C_ERR) return;
    if (cmdops & SORT_BY) sortByPattern(&cmdops, by, result);
    if (cmdops & SORT_NOT) goto end;
    if (sort(con, cmdops, result) == C_ERR) return;
    if (cmdops & SORT_LIMIT) {
        if (sortLimit(con, result, offset, count) == C_ERR)
            return;
    }
    if (cmdops & SORT_GET) sortByGetKeys(result, cmdops, get);
    if (cmdops & SORT_STORE) sortStore(result, cmdops, des);
end:
    if (result.empty()) {
        con.append(db_return_nil);
        return;
    }
    appendReplyMulti(con, result.size());
    for (auto& it : result) {
        if (it._value)
            appendReplySingleStr(con, *it._value);
        else
            con.append(db_return_nil);
    }
}
