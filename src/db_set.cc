#include "db.h"

using namespace Alice;

void DB::saddCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    expireIfNeeded(cmdlist[1]);
    size_t members = cmdlist.size();
    auto it = find(cmdlist[1]);
    if (isFound(it)) {
        checkType(con, it, Set);
        Set& set = getSetValue(it);
        int retval = 0;
        for (size_t i = 2; i < members; i++) {
            if (set.find(cmdlist[i]) == set.end()) {
                set.emplace(cmdlist[i]);
                retval++;
            }
        }
        appendReplyNumber(con, retval);
    } else {
        Set set;
        for (size_t i = 2; i < members; i++)
            set.emplace(cmdlist[i]);
        insert(cmdlist[1], set);
        appendReplyNumber(con, members - 2);
    }
    touchWatchKey(cmdlist[1]);
}

void DB::sisMemberCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    expireIfNeeded(cmdlist[1]);
    auto it = find(cmdlist[1]);
    if (!isFound(it)) db_return(con, db_return_0);
    checkType(con, it, Set);
    Set& set = getSetValue(it);
    if (set.find(cmdlist[2]) != set.end())
        con.append(db_return_1);
    else
        con.append(db_return_0);
}

void DB::spopCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    expireIfNeeded(cmdlist[1]);
    auto it = find(cmdlist[1]);
    if (!isFound(it)) db_return(con, db_return_nil);
    checkType(con, it, Set);
    Set& set = getSetValue(it);
    auto bucket = getRandBucketNumber(set);
    size_t bucketNumber = std::get<0>(bucket);
    size_t where = std::get<1>(bucket);
    for (auto it = set.cbegin(bucketNumber);
            it != set.cend(bucketNumber); it++)
        if (where-- == 0) {
            appendReplySingleStr(con, *it);
            set.erase(set.find(*it));
            break;
        }
    if (set.empty()) delKeyWithExpire(cmdlist[1]);
    touchWatchKey(cmdlist[1]);
}

void DB::srandMemberCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    expireIfNeeded(cmdlist[1]);
    int count = 0;
    if (cmdlist.size() > 2) {
        count = str2l(cmdlist[2].c_str());
        if (str2numberErr()) db_return(con, db_return_integer_err);
        if (count == 0) db_return(con, db_return_nil);
    }
    auto it = find(cmdlist[1]);
    if (!isFound(it)) db_return(con, db_return_nil);
    checkType(con, it, Set);
    Set& set = getSetValue(it);
    // 类型转换，int -> size_t
    if (count >= static_cast<ssize_t>(set.size())) {
        appendReplyMulti(con, set.size());
        for (auto& it : set) {
            appendReplySingleStr(con, it);
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
                    appendReplySingleStr(con, *it);
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
        appendReplySingleStr(con, it);
    }
}

void DB::sremCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    expireIfNeeded(cmdlist[1]);
    size_t size = cmdlist.size();
    auto it = find(cmdlist[1]);
    if (!isFound(it)) db_return(con, db_return_0);
    checkType(con, it, Set);
    Set& set = getSetValue(it);
    int retval = 0;
    for (size_t i = 2; i < size; i++) {
        auto it = set.find(cmdlist[i]);
        if (it != set.end()) {
            set.erase(it);
            retval++;
        }
    }
    if (set.empty()) delKeyWithExpire(cmdlist[1]);
    touchWatchKey(cmdlist[1]);
    appendReplyNumber(con, retval);
}

void DB::smoveCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    expireIfNeeded(cmdlist[1]);
    expireIfNeeded(cmdlist[2]);
    auto src = find(cmdlist[1]);
    if (!isFound(src)) db_return(con, db_return_0);
    checkType(con, src, Set);
    Set& srcSet = getSetValue(src);
    auto si = srcSet.find(cmdlist[3]);
    if (si == srcSet.end()) db_return(con, db_return_0);
    srcSet.erase(si);
    if (srcSet.empty()) delKeyWithExpire(cmdlist[1]);
    auto des = find(cmdlist[2]);
    if (isFound(des)) {
        checkType(con, des, Set);
        Set& desSet = getSetValue(des);
        desSet.emplace(cmdlist[3]);
    } else {
        Set set;
        set.emplace(cmdlist[3]);
        insert(cmdlist[2], set);
    }
    touchWatchKey(cmdlist[1]);
    touchWatchKey(cmdlist[2]);
    con.append(db_return_1);
}

void DB::scardCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    expireIfNeeded(cmdlist[1]);
    auto it = find(cmdlist[1]);
    if (!isFound(it)) db_return(con, db_return_0);
    checkType(con, it, Set);
    Set& set = getSetValue(it);
    appendReplyNumber(con, set.size());
}

void DB::smembersCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    expireIfNeeded(cmdlist[1]);
    auto it = find(cmdlist[1]);
    if (!isFound(it)) db_return(con, db_return_nil);
    checkType(con, it, Set);
    Set& set = getSetValue(it);
    appendReplyMulti(con, set.size());
    for (auto& it : set) {
        appendReplySingleStr(con, it);
    }
}

void DB::sinterCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    size_t size = cmdlist.size();
    size_t minSet = 0, minSetIndex = 0;
    for (size_t i = 1; i < size; i++) {
        expireIfNeeded(cmdlist[i]);
        auto it = find(cmdlist[i]);
        if (!isFound(it)) db_return(con, db_return_nil);
        checkType(con, it, Set);
        Set& set = getSetValue(it);
        if (minSet == 0)
            minSet = set.size();
        else if (minSet > set.size()) {
            minSet = set.size();
            minSetIndex = i;
        }
    }
    Set rset;
    Set& set = getSetValue(find(cmdlist[minSetIndex]));
    for (auto& it : set) {
        size_t i;
        for (i = 1; i < size; i++) {
            if (i == minSetIndex)
                continue;
            Set& set = getSetValue(find(cmdlist[i]));
            if (set.find(it) == set.end())
                break;
        }
        if (i == size)
            rset.insert(it);
    }
    appendReplyMulti(con, rset.size());
    for (auto& it : rset)
        appendReplySingleStr(con, it);
}

void DB::sinterStoreCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    expireIfNeeded(cmdlist[1]);
    size_t size = cmdlist.size();
    size_t minSet = 0, minSetIndex = 0;
    for (size_t i = 2; i < size; i++) {
        expireIfNeeded(cmdlist[i]);
        auto it = find(cmdlist[i]);
        if (!isFound(it)) db_return(con, db_return_0);
        checkType(con, it, Set);
        Set& set = getSetValue(it);
        if (minSet == 0)
            minSet = set.size();
        else if (minSet > set.size()) {
            minSet = set.size();
            minSetIndex = i;
        }
    }
    Set rset;
    Set& set = getSetValue(find(cmdlist[minSetIndex]));
    for (auto& it : set) {
        size_t i;
        for (i = 2; i < size; i++) {
            if (i == minSetIndex)
                continue;
            Set& set = getSetValue(find(cmdlist[i]));
            if (set.find(it) == set.end())
                break;
        }
        if (i == size)
            rset.insert(it);
    }
    auto it = find(cmdlist[1]);
    if (isFound(it)) {
        checkType(con, it, Set);
        Set& set = getSetValue(it);
        set.swap(rset);
        appendReplyNumber(con, set.size());
    } else {
        appendReplyNumber(con, rset.size());
        insert(cmdlist[1], rset);
    }
}

void DB::sunionCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    size_t size = cmdlist.size();
    Set rset;
    for (size_t i = 1; i < size; i++) {
        expireIfNeeded(cmdlist[i]);
        auto it = find(cmdlist[i]);
        if (isFound(it)) {
            checkType(con, it, Set);
            Set& set = getSetValue(it);
            for (auto& it : set) {
                rset.emplace(it);
            }
        }
    }
    if (rset.empty())
        con.append(db_return_nil);
    else {
        appendReplyMulti(con, rset.size());
        for (auto& it : rset)
            appendReplySingleStr(con, it);
    }
}

void DB::sunionStoreCommand(Context& con)
{
    auto& cmdlist = con.commandList();
    size_t size = cmdlist.size();
    Set rset;
    for (size_t i = 2; i < size; i++) {
        expireIfNeeded(cmdlist[i]);
        auto it = find(cmdlist[i]);
        if (isFound(it)) {
            checkType(con, it, Set);
            Set& set = getSetValue(it);
            for (auto& it : set) {
                rset.emplace(it);
            }
        }
    }
    auto it = find(cmdlist[1]);
    if (isFound(it)) {
        checkType(con, it, Set);
        Set& set = getSetValue(it);
        set.swap(rset);
        appendReplyNumber(con, set.size());
    } else {
        appendReplyNumber(con, rset.size());
        insert(cmdlist[1], rset);
    }
}

void DB::sdiffCommand(Context& con)
{
    // TODO:
}

void DB::sdiffStoreCommand(Context& con)
{
    // TODO:
}
