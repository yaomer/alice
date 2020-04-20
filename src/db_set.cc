#include "db.h"

using namespace Alice;

// SADD key member [member ...]
void DB::saddCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    size_t size = cmdlist.size();
    auto& key = cmdlist[1];
    checkExpire(key);
    int adds = 0;
    auto it = find(key);
    if (isFound(it)) {
        checkType(con, it, Set);
        auto& set = getSetValue(it);
        for (size_t i = 2; i < size; i++) {
            auto it = set.emplace(cmdlist[i]);
            if (it.second) adds++;
        }
    } else {
        Set set;
        for (size_t i = 2; i < size; i++)
            set.emplace(cmdlist[i]);
        insert(key, std::move(set));
        adds = size - 2;
    }
    con.appendReplyNumber(adds);
    touchWatchKey(key);
}

// SISMEMBER key member
void DB::sisMemberCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    auto& key = cmdlist[1];
    auto& member = cmdlist[2];
    checkExpire(key);
    auto it = find(key);
    if (!isFound(it)) db_return(con, reply.n0);
    checkType(con, it, Set);
    auto& set = getSetValue(it);
    if (set.find(member) != set.end())
        con.append(reply.n1);
    else
        con.append(reply.n0);
}

// SPOP key
void DB::spopCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    auto& key = cmdlist[1];
    checkExpire(key);
    auto it = find(key);
    if (!isFound(it)) db_return(con, reply.nil);
    checkType(con, it, Set);
    auto& set = getSetValue(it);
    auto randkey = getRandHashKey(set);
    size_t bucketNumber = std::get<0>(randkey);
    size_t where = std::get<1>(randkey);
    for (auto it = set.cbegin(bucketNumber);
            it != set.cend(bucketNumber); ++it)
        if (where-- == 0) {
            con.appendReplyString(*it);
            set.erase(set.find(*it));
            break;
        }
    checkEmpty(set, key);
    touchWatchKey(key);
}

// SRANDMEMBER key [count]
void DB::srandMemberCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    auto& key = cmdlist[1];
    checkExpire(key);
    int count = 0;
    if (cmdlist.size() > 2) {
        count = str2l(cmdlist[2].c_str());
        if (str2numberErr()) db_return(con, reply.integer_err);
        if (count == 0) db_return(con, reply.nil);
    }
    auto it = find(key);
    if (!isFound(it)) db_return(con, reply.nil);
    checkType(con, it, Set);
    auto& set = getSetValue(it);
    // 类型转换，int -> size_t
    if (count >= static_cast<ssize_t>(set.size())) {
        con.appendReplyMulti(set.size());
        for (auto& it : set) {
            con.appendReplyString(it);
        }
        return;
    }
    if (count == 0 || count < 0) {
        if (count == 0) count = -1;
        con.appendReplyMulti(-count);
        while (count++ < 0) {
            auto randkey = getRandHashKey(set);
            size_t bucketNumber = std::get<0>(randkey);
            size_t where = std::get<1>(randkey);
            for (auto it = set.cbegin(bucketNumber);
                    it != set.cend(bucketNumber); ++it) {
                if (where-- == 0) {
                    con.appendReplyString(*it);
                    break;
                }
            }
        }
        return;
    }
    con.appendReplyMulti(count);
    Set tset;
    while (count-- > 0) {
        auto randkey = getRandHashKey(set);
        size_t bucketNumber = std::get<0>(randkey);
        size_t where = std::get<1>(randkey);
        for (auto it = set.cbegin(bucketNumber);
                it != set.cend(bucketNumber); ++it) {
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
        con.appendReplyString(it);
    }
}

// SREM key member [member ...]
void DB::sremCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    size_t size = cmdlist.size();
    auto& key = cmdlist[1];
    checkExpire(key);
    auto it = find(key);
    if (!isFound(it)) db_return(con, reply.n0);
    checkType(con, it, Set);
    auto& set = getSetValue(it);
    int rems = 0;
    for (size_t i = 2; i < size; i++) {
        auto it = set.find(cmdlist[i]);
        if (it != set.end()) {
            set.erase(it);
            rems++;
        }
    }
    checkEmpty(set, key);
    touchWatchKey(key);
    con.appendReplyNumber(rems);
}

// SMOVE source destination member
void DB::smoveCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    auto& src_key = cmdlist[1];
    auto& des_key = cmdlist[2];
    auto& member = cmdlist[3];
    checkExpire(src_key);
    checkExpire(des_key);
    auto it = find(src_key);
    if (!isFound(it)) db_return(con, reply.n0);
    checkType(con, it, Set);
    auto& src_set = getSetValue(it);
    auto src_it = src_set.find(member);
    if (src_it == src_set.end()) db_return(con, reply.n0);
    src_set.erase(src_it);
    checkEmpty(src_set, src_key);
    auto des_it = find(des_key);
    if (isFound(des_it)) {
        checkType(con, des_it, Set);
        auto& des_set = getSetValue(des_it);
        des_set.emplace(member);
    } else {
        Set set;
        set.emplace(member);
        insert(des_key, std::move(set));
    }
    touchWatchKey(src_key);
    touchWatchKey(des_key);
    con.append(reply.n1);
}

// SCARD key
void DB::scardCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    auto& key = cmdlist[1];
    checkExpire(key);
    auto it = find(key);
    if (!isFound(it)) db_return(con, reply.n0);
    checkType(con, it, Set);
    auto& set = getSetValue(it);
    con.appendReplyNumber(set.size());
}

// SMEMBERS key
void DB::smembersCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    auto& key = cmdlist[1];
    checkExpire(key);
    auto it = find(key);
    if (!isFound(it)) db_return(con, reply.nil);
    checkType(con, it, Set);
    auto& set = getSetValue(it);
    con.appendReplyMulti(set.size());
    for (auto& member : set) {
        con.appendReplyString(member);
    }
}

void DB::sinter(Context& con, Set& rset, int start)
{
    auto& cmdlist = con.commandList();
    size_t size = cmdlist.size();
    size_t min = 0, j = 0;
    // 挑选出元素最少的集合
    for (size_t i = start; i < size; i++) {
        checkExpire(cmdlist[i]);
        auto it = find(cmdlist[i]);
        if (!isFound(it)) db_return(con, reply.nil);
        checkType(con, it, Set);
        auto& set = getSetValue(it);
        if (min == 0 || set.size() < min) {
            min = set.size();
            j = i;
        }
    }
    auto& set = getSetValue(find(cmdlist[j]));
    for (auto& member : set) {
        size_t i;
        for (i = start; i < size; i++) {
            if (i == j) continue;
            auto& set = getSetValue(find(cmdlist[i]));
            if (set.find(member) == set.end())
                break;
        }
        if (i == size)
            rset.emplace(member);
    }
}

void DB::sunion(Context& con, Set& rset, int start)
{
    auto& cmdlist = con.commandList();
    size_t size = cmdlist.size();
    for (size_t i = start; i < size; i++) {
        checkExpire(cmdlist[i]);
        auto it = find(cmdlist[i]);
        if (isFound(it)) {
            checkType(con, it, Set);
            auto& set = getSetValue(it);
            for (auto& member : set) {
                rset.emplace(member);
            }
        }
    }
}

void DB::sreply(Context& con, Set& rset)
{
    if (!rset.empty()) {
        con.appendReplyMulti(rset.size());
        for (auto& member : rset)
            con.appendReplyString(member);
    } else {
        con.append(reply.nil);
    }
}

void DB::sstore(Context& con, Set& rset)
{
    auto& cmdlist = con.commandList();
    auto& key = cmdlist[1];
    auto it = find(key);
    if (isFound(it)) {
        checkType(con, it, Set);
        auto& set = getSetValue(it);
        set.swap(rset);
        con.appendReplyNumber(set.size());
    } else {
        con.appendReplyNumber(rset.size());
        insert(key, std::move(rset));
    }
}

// SINTER key [key ...]
void DB::sinterCommand(Context& con)
{
    Set rset;
    sinter(con, rset, 1);
    // sinter执行出错了
    if (con.haveNewReply()) return;
    sreply(con, rset);
}

// SINTERSTORE destination key [key ...]
void DB::sinterStoreCommand(Context& con)
{
    Set rset;
    sinter(con, rset, 2);
    if (con.haveNewReply()) return;
    sstore(con, rset);
}

// SUNION key [key ...]
void DB::sunionCommand(Context& con)
{
    Set rset;
    sunion(con, rset, 1);
    if (con.haveNewReply()) return;
    sreply(con, rset);
}

// SUNIONSTORE destination key [key ...]
void DB::sunionStoreCommand(Context& con)
{
    Set rset;
    sunion(con, rset, 2);
    if (con.haveNewReply()) return;
    sstore(con, rset);
}

void DB::sdiffCommand(Context& con)
{
    // TODO:
}

void DB::sdiffStoreCommand(Context& con)
{
    // TODO:
}
