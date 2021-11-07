#include "internal.h"

namespace alice {

namespace mmdb {

// sort目前只支持list和set
// SORT key [BY pattern] [LIMIT offset count] [GET pattern [GET pattern ...]]
//          [ASC|DESC] [ALPHA] [STORE destination]

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
struct less {
    bool operator()(const sortobj& lhs, const sortobj& rhs) const
    {
        if (typeid(T) == typeid(std::string))
            return lhs.u.cmpval->compare(*rhs.u.cmpval) < 0;
        else
            return lhs.u.score < rhs.u.score;
    }
};

template <typename T>
struct greater {
    bool operator()(const sortobj& lhs, const sortobj& rhs) const
    {
        if (typeid(T) == typeid(std::string))
            return lhs.u.cmpval->compare(*rhs.u.cmpval) > 0;
        else
            return lhs.u.score > rhs.u.score;
    }
};

static int parse_sort_args(context_t& con, unsigned& cmdops, std::string& key,
                           std::string& by, std::string& des, std::vector<std::string>& getset,
                           int& offset, int& count)
{
    key = con.argv[1];
    size_t len = con.argv.size();
    for (size_t i = 2; i < len; i++) {
        std::transform(con.argv[i].begin(), con.argv[i].end(), con.argv[i].begin(), ::toupper);
        auto op = sortops.find(con.argv[i]);
        if (op != sortops.end()) cmdops |= op->second;
        else goto syntax_err;
        switch (op->second) {
        case SORT_ASC: case SORT_DESC: case SORT_ALPHA:
            break;
        case SORT_BY: {
            if (i + 1 >= len) goto syntax_err;
            by = con.argv[++i];
            break;
        }
        case SORT_LIMIT: {
            if (i + 2 >= len) goto syntax_err;
            offset = str2l(con.argv[++i]);
            if (str2numerr()) goto integer_err;
            count = str2l(con.argv[++i]);
            if (str2numerr()) goto integer_err;
            break;
        }
        case SORT_GET: {
            if (i + 1 >= len) goto syntax_err;
            if (con.argv[i+1].compare("#") == 0)
                cmdops |= SORT_GET_VAL;
            else
                getset.emplace_back(con.argv[i + 1]);
            i++;
            break;
        }
        case SORT_STORE: {
            if (i + 1 >= len) goto syntax_err;
            des = con.argv[++i];
            break;
        }
        }
    }
    return C_OK;
syntax_err:
    con.append(shared.syntax_err);
    return C_ERR;
integer_err:
    con.append(shared.integer_err);
    return C_ERR;
}

int DB::sort_get_result(context_t& con, sobj_list& result, const key_t& key, unsigned& cmdops)
{
    check_expire(key);
    auto it = find(key);
    if (not_found(it)) {
        con.append(shared.multi_empty);
        return C_ERR;
    }
    if (is_type(it, List)) {
        cmdops |= SORT_LIST_TYPE;
        auto& list = get_list_value(it);
        for (auto& it : list)
            result.emplace_back(&it);
    } else if (is_type(it, Set)) {
        cmdops |= SORT_SET_TYPE;
        auto& set = get_set_value(it);
        for (auto& it : set)
            result.emplace_back(&it);
    } else {
        con.append(shared.type_err);
        return C_ERR;
    }
    return C_OK;

}

#define sub_key(key, star, by, it) \
    if (star == by.begin()) { \
        key = *it.value + by.substr(1, by.size()); \
    } else if (star == --by.end()) { \
        key = by.substr(0, by.size() - 1) + *it.value; \
    } else { \
        key = by.substr(0, star - by.begin()) + *it.value \
            + by.substr(star - by.begin(), by.size()); \
    }

void DB::sort_by_pattern(sobj_list& result, const key_t& by, unsigned& cmdops)
{
    std::string key;
    auto star = std::find(by.begin(), by.end(), '*');
    if (star == by.end()) {
        cmdops |= SORT_NOT;
        return;
    }
    for (auto& it : result) {
        sub_key(key, star, by, it);
        auto e = find(key);
        if (!not_found(e) && is_type(e, String)) {
            auto& val = get_string_value(e);
            it.u.cmpval = &val;
        } else {
            it.u.cmpval = it.value;
        }
    }
}

static int sort_result(context_t& con, DB::sobj_list& result, unsigned cmdops)
{
    if (cmdops & SORT_ALPHA) {
        if (cmdops & SORT_DESC)
            std::sort(result.begin(), result.end(), greater<std::string>());
        else
            std::sort(result.begin(), result.end(), less<std::string>());
    } else {
        for (auto& it : result) {
            double v = str2f(*it.u.cmpval);
            if (str2numerr()) {
                con.append("-ERR One or more scores can't be converted into double\r\n");
                return C_ERR;
            }
            it.u.score = v;
        }
        if (cmdops & SORT_DESC)
            std::sort(result.begin(), result.end(), greater<double>());
        else
            std::sort(result.begin(), result.end(), less<double>());
    }
    return C_OK;
}

static int sort_limit(context_t& con, DB::sobj_list& result, int offset, int count)
{
    int size = result.size();
    if (offset < 0 || count <= 0 || offset >= size || offset + count > size) {
        con.append(shared.multi_empty);
        return C_ERR;
    }
    int i = 0;
    for (auto it = result.begin(); it != result.end(); i++) {
        if (i < offset) {
            ++it;
            result.pop_front();
            continue;
        }
        if (i >= count) {
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

void DB::sort_by_get_keys(sobj_list& result, const std::vector<std::string>& getset, unsigned cmdops)
{
    sobj_list tres;
    std::string key;
    for (auto& it : result) {
        if (cmdops & SORT_GET_VAL)
            tres.emplace_back(it.value);
        for (auto& p : getset) {
            auto star = std::find(p.begin(), p.end(), '*');
            if (star == p.end()) {
                tres.emplace_back(nullptr);
                continue;
            }
            sub_key(key, star, p, it);
            auto e = find(key);
            if (!not_found(e) && is_type(e, String)) {
                auto& val = get_string_value(e);
                tres.emplace_back(&val);
            } else
                tres.emplace_back(nullptr);
        }
    }
    result.swap(tres);
}

void DB::sort_store(sobj_list& result, const key_t& des, unsigned cmdops)
{
    if (cmdops & SORT_LIST_TYPE) {
        List list;
        for (auto& it : result)
            list.emplace_back(it.value ? *it.value : "");
        insert(des, std::move(list));
    } else if (cmdops & SORT_SET_TYPE) {
        Set set;
        for (auto& it : result)
            set.emplace(it.value ? *it.value : "");
        insert(des, std::move(set));
    }
}

void DB::sort(context_t& con)
{
    unsigned cmdops = 0;
    std::string key, by, des;
    int offset = 0, count = 0;
    std::vector<std::string> getset;
    if (parse_sort_args(con, cmdops, key, by, des, getset, offset, count) == C_ERR)
        return;
    sobj_list result;
    if (sort_get_result(con, result, key, cmdops) == C_ERR) return;
    if (cmdops & SORT_BY) sort_by_pattern(result, by, cmdops);
    if (cmdops & SORT_NOT) goto end;
    if (sort_result(con, result, cmdops) == C_ERR) return;
    if ((cmdops & SORT_LIMIT) && sort_limit(con, result, offset, count) == C_ERR)
        return;
    if (cmdops & SORT_GET) sort_by_get_keys(result, getset, cmdops);
    if (cmdops & SORT_STORE) sort_store(result, des, cmdops);
end:
    con.append_reply_multi(result.size());
    for (auto& it : result) {
        if (it.value)
            con.append_reply_string(*it.value);
        else
            con.append(shared.nil);
    }
}

}
}
