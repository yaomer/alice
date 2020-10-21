#include "internal.h"

#include "../server.h"

using namespace alice;
using namespace alice::mmdb;

// L(R)PUSH key value [value ...]
void DB::_lpush(context_t& con, bool is_lpush)
{
    size_t size = con.argv.size();
    auto& key = con.argv[1];
    check_expire(key);
    auto it = find(key);
    if (not_found(it)) {
        List list;
        for (size_t i = 2; i < size; i++) {
            is_lpush ? list.emplace_front(con.argv[i])
                     : list.emplace_back(con.argv[i]);
        }
        insert(key, std::move(list));
        con.append_reply_number(list.size());
    } else {
        check_type(con, it, List);
        auto& list = get_list_value(it);
        for (size_t i = 2; i < size; i++) {
            is_lpush ? list.emplace_front(con.argv[i])
                     : list.emplace_back(con.argv[i]);
        }
        con.append_reply_number(list.size());
    }
    touch_watch_key(key);
}

void DB::lpush(context_t& con)
{
    _lpush(con, true);
}

void DB::rpush(context_t& con)
{
    _lpush(con, false);
}

// L(R)PUSHX key value
void DB::_lpushx(context_t& con, bool is_lpushx)
{
    auto& key = con.argv[1];
    auto& value = con.argv[2];
    check_expire(key);
    auto it = find(key);
    if (not_found(it)) ret(con, shared.n0);
    check_type(con, it, List);
    auto& list = get_list_value(it);
    is_lpushx ? list.emplace_front(value)
              : list.emplace_back(value);
    touch_watch_key(key);
    con.append_reply_number(list.size());
}

void DB::lpushx(context_t& con)
{
    _lpushx(con, true);
}

void DB::rpushx(context_t& con)
{
    _lpushx(con, false);
}

// L(R)POP key
void DB::_lpop(context_t& con, bool is_lpop)
{
    auto& key = con.argv[1];
    check_expire(key);
    auto it = find(key);
    if (not_found(it)) ret(con, shared.nil);
    check_type(con, it, List);
    auto& list = get_list_value(it);
    if (is_lpop) {
        con.append_reply_string(list.front());
        list.pop_front();
    } else {
        con.append_reply_string(list.back());
        list.pop_back();
    }
    del_key_if_empty(list, key);
    touch_watch_key(key);
}

void DB::lpop(context_t& con)
{
    _lpop(con, true);
}

void DB::rpop(context_t& con)
{
    _lpop(con, false);
}

// RPOPLPUSH source destination
// BRPOPLPUSH source destination timeout
void DB::_rpoplpush(context_t& con, bool is_nonblock)
{
    size_t size = con.argv.size();
    auto& src_key = con.argv[1];
    auto& des_key = con.argv[2];
    check_expire(src_key);
    check_expire(des_key);
    int timeout = 0;
    if (!is_nonblock) {
        timeout = str2l(con.argv[size - 1]);
        if (str2numerr()) ret(con, shared.integer_err);
        if (timeout < 0) ret(con, shared.timeout_out_of_range);
    }
    auto src_it = find(src_key);
    if (not_found(src_it)) {
        if (is_nonblock) ret(con, shared.nil);
        auto e = find(des_key);
        if (!not_found(e)) check_type(con, e, List);
        add_blocking_key(con, src_key);
        con.des.assign(des_key);
        set_context_to_block(con, timeout);
        return;
    }
    check_type(con, src_it, List);
    auto& src_list = get_list_value(src_it);
    con.append_reply_string(src_list.back());
    auto des_it = find(des_key);
    if (not_found(des_it)) {
        List des_list;
        des_list.emplace_front(src_list.back());
        insert(des_key, std::move(des_list));
        touch_watch_key(src_key);
    } else {
        check_type(con, des_it, List);
        auto& des_list = get_list_value(des_it);
        des_list.emplace_front(src_list.back());
        touch_watch_key(src_key);
        touch_watch_key(des_key);
    }
    src_list.pop_back();
    del_key_if_empty(src_list, src_key);
}

void DB::rpoplpush(context_t& con)
{
    _rpoplpush(con, true);
}

// LREM key count value
void DB::lrem(context_t& con)
{
    auto& key = con.argv[1];
    auto& value = con.argv[3];
    check_expire(key);
    int count = str2l(con.argv[2].c_str());
    if (str2numerr()) ret(con, shared.integer_err);
    auto it = find(key);
    if (not_found(it)) ret(con, shared.n0);
    check_type(con, it, List);
    auto& list = get_list_value(it);
    int rems = 0;
    if (count > 0) {
        for (auto it = list.cbegin(); it != list.cend(); ) {
            if ((*it).compare(value) == 0) {
                auto tmp = it++;
                list.erase(tmp);
                rems++;
                if (--count == 0)
                    break;
            } else
                ++it;
        }
    } else if (count < 0) {
        for (auto it = list.crbegin(); it != list.crend(); ++it) {
            if ((*it).compare(value) == 0) {
                // &*(reverse_iterator(i)) == &*(i - 1)
                list.erase((++it).base());
                rems++;
                if (++count == 0)
                    break;
            }
        }
    } else {
        for (auto it = list.cbegin(); it != list.cend(); ) {
            if ((*it).compare(value) == 0) {
                auto tmp = it++;
                list.erase(tmp);
                rems++;
            }
        }
    }
    del_key_if_empty(list, key);
    touch_watch_key(key);
    con.append_reply_number(rems);
}

// LLEN key
void DB::llen(context_t& con)
{
    auto& key = con.argv[1];
    check_expire(key);
    auto it = find(key);
    if (not_found(it)) ret(con, shared.n0);
    check_type(con, it, List);
    auto& list = get_list_value(it);
    con.append_reply_number(list.size());
}

// LINDEX key index
void DB::lindex(context_t& con)
{
    auto& key = con.argv[1];
    int index = str2l(con.argv[2]);
    if (str2numerr()) ret(con, shared.integer_err);
    check_expire(key);
    auto it = find(key);
    if (not_found(it)) ret(con, shared.nil);
    check_type(con, it, List);
    auto& list = get_list_value(it);
    int size = list.size();
    if (index < 0)
        index += size;
    if (index < 0 || index >= size) {
        ret(con, shared.nil);
    }
    for (auto& it : list)
        if (index-- == 0) {
            con.append_reply_string(it);
            break;
        }
}

// LSET key index value
void DB::lset(context_t& con)
{
    auto& key = con.argv[1];
    auto& value = con.argv[3];
    int index = str2l(con.argv[2]);
    if (str2numerr()) ret(con, shared.integer_err);
    check_expire(key);
    auto it = find(key);
    if (not_found(it)) ret(con, shared.no_such_key);
    check_type(con, it, List);
    auto& list = get_list_value(it);
    int size = list.size();
    if (index < 0)
        index += size;
    if (index < 0 || index >= size) {
        ret(con, shared.index_out_of_range);
    }
    for (auto& it : list)
        if (index-- == 0) {
            it.assign(value);
            break;
        }
    touch_watch_key(key);
    con.append(shared.ok);
}

// LRANGE key start stop
void DB::lrange(context_t& con)
{
    auto& key = con.argv[1];
    int start = str2l(con.argv[2]);
    if (str2numerr()) ret(con, shared.integer_err);
    int stop = str2l(con.argv[3]);
    if (str2numerr()) ret(con, shared.integer_err);
    check_expire(key);
    auto it = find(key);
    if (not_found(it)) ret(con, shared.nil);
    check_type(con, it, List);
    auto& list = get_list_value(it);
    int upper = list.size() - 1;
    int lower = -list.size();
    if (check_range(con, start, stop, lower, upper) == C_ERR)
        return;
    con.append_reply_multi(stop - start + 1);
    int i = 0;
    for (auto& it : list) {
        if (i < start) {
            i++;
            continue;
        }
        if (i > stop)
            break;
        con.append_reply_string(it);
        i++;
    }
}

// LTRIM key start stop
void DB::ltrim(context_t& con)
{
    auto& key = con.argv[1];
    int start = str2l(con.argv[2]);
    if (str2numerr()) ret(con, shared.integer_err);
    int stop = str2l(con.argv[3]);
    if (str2numerr()) ret(con, shared.integer_err);
    check_expire(key);
    auto it = find(key);
    if (not_found(it)) ret(con, shared.ok);
    check_type(con, it, List);
    auto& list = get_list_value(it);
    int size = list.size();
    if (start < 0)
        start += size;
    if (stop < 0)
        stop += size;
    if (start > size - 1 || start > stop || stop > size - 1) {
        list.clear();
        ret(con, shared.ok);
    }
    int i = 0;
    for (auto it = list.cbegin(); it != list.cend(); i++) {
        auto tmp = it++;
        if (i >= start && i <= stop) continue;
        list.erase(tmp);
    }
    del_key_if_empty(list, key);
    touch_watch_key(key);
    con.append(shared.ok);
}

// BLPOP key [key ...] timeout
void DB::_blpop(context_t& con, bool is_blpop)
{
    size_t size = con.argv.size();
    int timeout = str2l(con.argv[size - 1]);
    if (str2numerr()) ret(con, shared.integer_err);
    if (timeout < 0) ret(con, shared.timeout_out_of_range);
    for (size_t i = 1 ; i < size - 1; i++) {
        auto& key = con.argv[i];
        auto it = find(key);
        if (!not_found(it)) {
            check_type(con, it, List);
            auto& list = get_list_value(it);
            con.append_reply_multi(2);
            con.append_reply_string(key);
            con.append_reply_string(is_blpop ? list.front() : list.back());
            is_blpop ? list.pop_front() : list.pop_back();
            del_key_if_empty(list, key);
            return;
        }
    }
    for (size_t i = 1; i < size - 1; i++) {
        add_blocking_key(con, con.argv[i]);
    }
    set_context_to_block(con, timeout);
}

void DB::blpop(context_t& con)
{
    if (con.flags & context_t::EXEC_MULTI)
        lpop(con);
    else
        _blpop(con, true);
}

void DB::brpop(context_t& con)
{
    if (con.flags & context_t::EXEC_MULTI)
        rpop(con);
    else
        _blpop(con, false);
}

void DB::brpoplpush(context_t& con)
{
    if (con.flags & context_t::EXEC_MULTI)
        rpoplpush(con);
    else
        _rpoplpush(con, false);
}

#define BLOCK_LPOP 1
#define BLOCK_RPOP 2
#define BLOCK_RPOPLPUSH 3

static unsigned getLastcmd(const std::string& lc)
{
    unsigned ops = 0;
    if (lc.compare("BLPOP") == 0) ops = BLOCK_LPOP;
    else if (lc.compare("BRPOP") == 0) ops = BLOCK_RPOP;
    else if (lc.compare("BRPOPLPUSH") == 0) ops = BLOCK_RPOPLPUSH;
    return ops;
}

// 正常弹出解除阻塞的键
void DB::blocking_pop(const key_t& key)
{
    auto cl = blocking_keys.find(key);
    if (cl == blocking_keys.end()) return;
    auto now = angel::util::get_cur_time_ms();
    auto it = find(key);
    assert(!not_found(it));
    auto& list = get_list_value(it);
    context_t other(nullptr, engine);

    auto conn = __server->get_server().get_connection(*cl->second.begin());
    if (!conn) return;
    auto& con = std::any_cast<context_t&>(conn->get_context());
    auto bops = getLastcmd(con.last_cmd);
    other.append_reply_multi(2);
    other.append_reply_string(key);
    other.append_reply_string((bops == BLOCK_LPOP) ? list.front() : list.back());
    double seconds = 1.0 * (now - con.block_start_time) / 1000;
    other.append("+(");
    other.append(d2s(seconds));
    other.append("s)\r\n");
    conn->send(other.buf);

    clear_blocking_keys_for_context(con);
    engine->del_block_client(conn->id());

    if (bops == BLOCK_RPOPLPUSH) {
        auto& src = list.back();
        auto it = find(con.des);
        if (not_found(it)) {
            List list;
            list.emplace_front(src);
            insert(con.des, std::move(list));
        } else {
            auto& list = get_list_value(it);
            list.emplace_front(src);
        }
    }
    (bops == BLOCK_LPOP) ? list.pop_front() : list.pop_back();
    del_key_if_empty(list, key);
    touch_watch_key(key);
}

// 将一个blocking key添加到DB::blocking_keys和context_t::blocking_keys中
void DB::add_blocking_key(context_t& con, const key_t& key)
{
    con.blocking_keys.emplace_back(key);
    auto it = blocking_keys.find(key);
    if (it == blocking_keys.end()) {
        std::list<size_t> idlist = { con.conn->id() };
        blocking_keys.emplace(key, std::move(idlist));
    } else {
        it->second.push_back(con.conn->id());
    }
}

void DB::set_context_to_block(context_t& con, int timeout)
{
    con.flags |= context_t::CON_BLOCK;
    con.block_db_num = engine->get_cur_db_num();
    con.block_start_time = angel::util::get_cur_time_ms();
    con.blocked_time = timeout * 1000;
    engine->add_block_client(con.conn->id());
}
