#include "internal.h"

#include "../server.h"

using namespace alice;
using namespace alice::ssdb;

// list-meta-value: [type][lindex][:][rindex][:][size]
// range: [lindex, rindex]
static inline std::string
encode_list_meta_value(long long li, long long ri, long long size)
{
    std::string buf;
    buf.append(1, ktype::tlist);
    buf.append(i2s(li));
    buf.append(1, ':');
    buf.append(i2s(ri));
    buf.append(1, ':');
    buf.append(i2s(size));
    return buf;
}

struct list_key_info {
    long long li = 0, ri = 0;
    long long size = 0;
    bool is_no_hole()
    {
        return li + size == ri + 1;
    }
};

static inline list_key_info
decode_list_meta_value(const std::string& value)
{
    list_key_info lk;
    const char *s = value.c_str() + 1;
    lk.li = atoll(s);
    lk.ri = atoll(strchr(s, ':') + 1);
    lk.size = atoll(strrchr(s, ':') + 1);
    return lk;
}

static inline std::string
encode_list_key(const std::string& key, long long number)
{
    std::string buf;
    buf.append(1, ktype::tlist);
    buf.append(key);
    buf.append(1, ':');
    buf.append(i2s(number));
    return buf;
}

static inline long long get_list_index(leveldb::Slice&& key)
{
    return atoll(strrchr(key.data(), ':') + 1);
}

static void pop_key(DB *db, leveldb::WriteBatch *batch,
                 const std::string& meta_key, const std::string& enc_key,
                 list_key_info& lk, bool is_lpop)
{
    if (lk.is_no_hole()) {
        is_lpop ? ++lk.li : --lk.ri;
    } else {
        auto it = db->newIterator();
        it->Seek(enc_key);
        assert(it->Valid());
        is_lpop ? it->Next() : it->Prev();
        assert(it->Valid());
        if (is_lpop) lk.li = get_list_index(it->key());
        else lk.ri = get_list_index(it->key());
    }
    batch->Put(meta_key, encode_list_meta_value(lk.li, lk.ri, --lk.size));
    if (lk.size == 0) batch->Delete(meta_key);
    batch->Delete(enc_key);
}

// LPUSH key value [value ...]
void DB::lpush(context_t& con)
{
    auto& key = con.argv[1];
    std::string value;
    leveldb::WriteBatch batch;
    list_key_info lk;
    auto meta_key = encode_meta_key(key);
    check_expire(meta_key);
    auto s = db->Get(leveldb::ReadOptions(), meta_key, &value);
    if (!s.IsNotFound() && !s.ok()) reterr(con, s);
    if (s.ok()) {
        check_type(con, value, ktype::tlist);
        lk = decode_list_meta_value(value);
        --lk.li;
    }

    for (int i = 2; i < con.argv.size(); i++) {
        batch.Put(encode_list_key(key, lk.li--), con.argv[i]);
    }
    lk.size += con.argv.size() - 2;
    batch.Put(meta_key, encode_list_meta_value(lk.li + 1, lk.ri, lk.size));

    s = db->Write(leveldb::WriteOptions(), &batch);
    check_status(con, s);
    touch_watch_key(key);
    con.append_reply_number(lk.size);
    blocking_pop(key);
}

// LPUSH key value [value ...]
void DB::rpush(context_t& con)
{
    auto& key = con.argv[1];
    std::string value;
    leveldb::WriteBatch batch;
    list_key_info lk;
    auto meta_key = encode_meta_key(key);
    check_expire(meta_key);
    auto s = db->Get(leveldb::ReadOptions(), meta_key, &value);
    if (!s.IsNotFound() && !s.ok()) reterr(con, s);
    if (s.ok()) {
        check_type(con, value, ktype::tlist);
        lk = decode_list_meta_value(value);
        ++lk.ri;
    }

    for (int i = 2; i < con.argv.size(); i++) {
        batch.Put(encode_list_key(key, lk.ri++), con.argv[i]);
    }
    lk.size += con.argv.size() - 2;
    batch.Put(meta_key, encode_list_meta_value(lk.li, lk.ri - 1, lk.size));

    s = db->Write(leveldb::WriteOptions(), &batch);
    check_status(con, s);
    touch_watch_key(key);
    con.append_reply_number(lk.size);
    blocking_pop(key);
}

void DB::_lpushx(context_t& con, bool is_lpushx)
{
    auto& key = con.argv[1];
    std::string value;
    auto meta_key = encode_meta_key(key);
    check_expire(meta_key);
    auto s = db->Get(leveldb::ReadOptions(), meta_key, &value);
    if (s.IsNotFound()) ret(con, shared.n0);
    check_status(con, s);
    check_type(con, value, ktype::tlist);
    leveldb::WriteBatch batch;
    auto lk = decode_list_meta_value(value);
    batch.Put(encode_list_key(key, is_lpushx ? --lk.li : ++lk.ri), con.argv[2]);
    batch.Put(meta_key, encode_list_meta_value(lk.li, lk.ri, ++lk.size));

    s = db->Write(leveldb::WriteOptions(), &batch);
    check_status(con, s);
    touch_watch_key(key);
    con.append_reply_number(lk.size);
    blocking_pop(key);
}

// LPUSHX key value
void DB::lpushx(context_t& con)
{
    _lpushx(con, true);
}

// LPUSHX key value
void DB::rpushx(context_t& con)
{
    _lpushx(con, false);
}

void DB::_lpop(context_t& con, bool is_lpop)
{
    auto& key = con.argv[1];
    std::string value;
    auto meta_key = encode_meta_key(key);
    check_expire(meta_key);
    auto s = db->Get(leveldb::ReadOptions(), meta_key, &value);
    if (s.IsNotFound()) ret(con, shared.nil);
    check_status(con, s);
    check_type(con, value, ktype::tlist);

    leveldb::WriteBatch batch;
    auto lk = decode_list_meta_value(value);

    auto enc_key = encode_list_key(key, is_lpop ? lk.li : lk.ri);
    s = db->Get(leveldb::ReadOptions(), enc_key, &value);
    check_status(con, s);
    pop_key(this, &batch, meta_key, enc_key, lk, is_lpop);

    s = db->Write(leveldb::WriteOptions(), &batch);
    check_status(con, s);
    touch_watch_key(key);
    con.append_reply_string(value);
}

// LPOP key
void DB::lpop(context_t& con)
{
    _lpop(con, true);
}

// RPOP key
void DB::rpop(context_t& con)
{
    _lpop(con, false);
}

// LLEN key
void DB::llen(context_t& con)
{
    auto& key = con.argv[1];
    std::string value;
    auto meta_key = encode_meta_key(key);
    check_expire(meta_key);
    auto s = db->Get(leveldb::ReadOptions(), meta_key, &value);
    if (s.IsNotFound()) ret(con, shared.n0);
    check_status(con, s);
    check_type(con, value, ktype::tlist);
    auto lk = decode_list_meta_value(value);
    con.append_reply_number(lk.size);
}

// LRANGE key start stop
void DB::lrange(context_t& con)
{
    std::string value;
    auto& key = con.argv[1];
    long long start = str2ll(con.argv[2]);
    if (str2numerr()) ret(con, shared.integer_err);
    long long stop = str2ll(con.argv[3]);
    if (str2numerr()) ret(con, shared.integer_err);
    auto meta_key = encode_meta_key(key);
    check_expire(meta_key);
    auto s = db->Get(leveldb::ReadOptions(), meta_key, &value);
    if (s.IsNotFound()) ret(con, shared.nil);
    check_status(con, s);
    check_type(con, value, ktype::tlist);
    auto lk = decode_list_meta_value(value);
    long long upper = lk.size - 1;
    long long lower = -lk.size;
    if (check_range_index(con, start, stop, lower, upper) == C_ERR)
        return;
    long long ranges = stop - start + 1;
    con.append_reply_multi(ranges);
    auto it = newIterator();
    for (it->Seek(encode_list_key(key, lk.li + start)); it->Valid(); it->Next()) {
        con.append_reply_string(it->value().ToString());
        if (--ranges == 0)
            break;
    }
}

// RPOPLPUSH source destination
// BRPOPLPUSH source destination timeout
void DB::_rpoplpush(context_t& con, bool is_nonblock)
{
    auto& src_key = con.argv[1];
    auto& des_key = con.argv[2];
    auto src_meta_key = encode_meta_key(src_key);
    auto des_meta_key = encode_meta_key(des_key);
    check_expire(src_meta_key);
    check_expire(des_meta_key);
    int timeout = 0;
    if (!is_nonblock) {
        timeout = str2l(con.argv[con.argv.size() - 1]);
        if (str2numerr()) ret(con, shared.integer_err);
        if (timeout < 0) ret(con, shared.timeout_out_of_range);
    }
    std::string src_value, des_value;
    auto s = db->Get(leveldb::ReadOptions(), src_meta_key, &src_value);
    if (s.IsNotFound()) {
        if (is_nonblock) ret(con, shared.nil);
        s = db->Get(leveldb::ReadOptions(), des_meta_key, &des_value);
        if (!s.ok() && !s.IsNotFound()) reterr(con, s);
        if (s.ok()) check_type(con, des_value, ktype::tlist);
        add_blocking_key(con, src_key);
        con.des = des_key;
        set_context_to_block(con, timeout);
        return;
    }
    check_status(con, s);
    check_type(con, src_value, ktype::tlist);
    leveldb::WriteBatch batch;
    // get src_value
    auto lk = decode_list_meta_value(src_value);
    auto enc_key = encode_list_key(src_key, lk.ri);
    s = db->Get(leveldb::ReadOptions(), enc_key, &src_value);
    check_status(con, s);
    pop_key(this, &batch, src_meta_key, enc_key, lk, false);
    // put src_value to des_key
    if (src_key == des_key) {
        --lk.li;
        touch_watch_key(src_key);
    } else {
        lk.li = lk.ri = lk.size = 0;
        s = db->Get(leveldb::ReadOptions(), des_meta_key, &des_value);
        if (s.ok()) {
            check_type(con, des_value, ktype::tlist);
            lk = decode_list_meta_value(des_value);
            --lk.li;
            touch_watch_key(src_key);
            touch_watch_key(des_key);
        } else if (s.IsNotFound()) {
            touch_watch_key(src_key);
        } else
            reterr(con, s);
    }
    batch.Put(encode_list_key(des_key, lk.li), src_value);
    batch.Put(des_meta_key, encode_list_meta_value(lk.li, lk.ri, ++lk.size));

    s = db->Write(leveldb::WriteOptions(), &batch);
    check_status(con, s);
    con.append_reply_string(src_value);
    blocking_pop(des_key);
}

void DB::rpoplpush(context_t& con)
{
    _rpoplpush(con, true);
}

// LREM key count value
void DB::lrem(context_t& con)
{
    auto& key = con.argv[1];
    long long count = str2ll(con.argv[2]);
    if (str2numerr()) ret(con, shared.integer_err);
    std::string enc_key, value;
    auto meta_key = encode_meta_key(key);
    check_expire(meta_key);
    auto s = db->Get(leveldb::ReadOptions(), meta_key, &value);
    if (s.IsNotFound()) ret(con, shared.n0);
    check_status(con, s);
    check_type(con, value, ktype::tlist);
    long long rems = 0;
    leveldb::WriteBatch batch;
    auto lk = decode_list_meta_value(value);
    // 因为可能会在中间删除元素，所以需要更新索引范围[li, ri]
    bool update = false;
    auto it = newIterator();
    if (count > 0) {
        for (it->Seek(encode_list_key(key, lk.li)); it->Valid(); it->Next()) {
            if (it->value() == con.argv[3]) {
                batch.Delete(it->key());
                ++rems;
                if (--count == 0) {
                    if (!update && rems != lk.size) { // 恰好删除了最左边的几个元素
                        it->Next();
                        assert(it->Valid());
                        lk.li = get_list_index(it->key());
                    }
                    break;
                }
            } else {
                if (!update) {
                    update = true;
                    lk.li = get_list_index(it->key());
                }
                lk.ri = get_list_index(it->key());
            }
        }
    } else if (count < 0) {
        for (it->Seek(encode_list_key(key, lk.ri)); it->Valid(); it->Prev()) {
            if (it->value() == con.argv[3]) {
                batch.Delete(it->key());
                ++rems;
                if (++count == 0) {
                    if (!update && rems != lk.size) { // 恰好删除了最右边的几个元素
                        it->Prev();
                        assert(it->Valid());
                        lk.ri = get_list_index(it->key());
                    }
                    break;
                }
            } else {
                if (!update) {
                    update = true;
                    lk.ri = get_list_index(it->key());
                }
                lk.li = get_list_index(it->key());
            }
        }
    } else {
        for (it->Seek(encode_list_key(key, lk.li)); it->Valid(); it->Next()) {
            if (it->value() == con.argv[3]) {
                batch.Delete(it->key());
                ++rems;
            } else {
                if (!update) {
                    update = true;
                    lk.li = get_list_index(it->key());
                }
                lk.ri = get_list_index(it->key());
            }
        }
    }
    if (rems == lk.size) {
        batch.Delete(meta_key);
    } else {
        assert(lk.size > rems);
        batch.Put(meta_key, encode_list_meta_value(lk.li, lk.ri, lk.size - rems));
    }
    s = db->Write(leveldb::WriteOptions(), &batch);
    check_status(con, s);
    touch_watch_key(key);
    con.append_reply_number(rems);
}

// LINDEX key index
void DB::lindex(context_t& con)
{
    auto& key = con.argv[1];
    long long index = str2ll(con.argv[2]);
    if (str2numerr()) ret(con, shared.integer_err);
    std::string value;
    auto meta_key = encode_meta_key(key);
    check_expire(meta_key);
    auto s = db->Get(leveldb::ReadOptions(), meta_key, &value);
    if (s.IsNotFound()) ret(con, shared.nil);
    check_status(con, s);
    check_type(con, value, ktype::tlist);

    auto lk = decode_list_meta_value(value);
    if (index < 0)
        index += lk.size;
    if (index < 0 || index >= lk.size)
        ret(con, shared.nil);
    if (lk.is_no_hole()) {
        auto enc_key = encode_list_key(key, lk.li + index);
        s = db->Get(leveldb::ReadOptions(), enc_key, &value);
        check_status(con, s);
        con.append_reply_string(value);
        return;
    }
    auto it = newIterator();
    for (it->Seek(encode_list_key(key, lk.li)); it->Valid(); it->Next()) {
        if (--index == -1) {
            con.append_reply_string(it->value().ToString());
            break;
        }
    }
    assert(index == -1);
}

// LSET key index value
void DB::lset(context_t& con)
{
    auto& key = con.argv[1];
    long long index = str2l(con.argv[2]);
    if (str2numerr()) ret(con, shared.integer_err);
    std::string value;
    auto meta_key = encode_meta_key(key);
    check_expire(meta_key);
    auto s = db->Get(leveldb::ReadOptions(), meta_key, &value);
    if (s.IsNotFound()) ret(con, shared.no_such_key);
    check_status(con, s);
    check_type(con, value, ktype::tlist);

    std::string enc_key;
    auto lk = decode_list_meta_value(value);
    if (index < 0)
        index += lk.size;
    if (index < 0 || index >= lk.size)
        ret(con, shared.index_out_of_range);
    if (lk.is_no_hole()) {
        enc_key = encode_list_key(key, index);
    } else {
        auto it = newIterator();
        for (it->Seek(encode_list_key(key, lk.li)); it->Valid(); it->Next()) {
            if (--index == -1) {
                enc_key = it->key().ToString();
                break;
            }
        }
        assert(index == -1);
    }
    s = db->Put(leveldb::WriteOptions(), enc_key, con.argv[3]);
    check_status(con, s);
    touch_watch_key(key);
    con.append_reply_string(shared.ok);
}

// LTRIM key start stop
void DB::ltrim(context_t& con)
{
    auto& key = con.argv[1];
    long long start = str2l(con.argv[2]);
    if (str2numerr()) ret(con, shared.integer_err);
    long long stop = str2l(con.argv[3]);
    if (str2numerr()) ret(con, shared.integer_err);
    std::string value;
    auto meta_key = encode_meta_key(key);
    check_expire(meta_key);
    auto s = db->Get(leveldb::ReadOptions(), meta_key, &value);
    if (s.IsNotFound()) ret(con, shared.ok);
    check_status(con, s);
    check_type(con, value, ktype::tlist);

    auto lk = decode_list_meta_value(value);
    if (start < 0)
        start += lk.size;
    if (stop < 0)
        stop += lk.size;
    if (start > lk.size - 1 || start > stop || stop > lk.size - 1 || start == stop) {
        auto err = del_list_key(key);
        if (err) reterr(con, err.value());
        ret(con, shared.ok);
    }
    long long i = 0;
    leveldb::WriteBatch batch;
    auto it = newIterator();
    for (it->Seek(encode_list_key(key, lk.li)); it->Valid(); it->Next()) {
        if (i == start) lk.li = get_list_index(it->key());
        if (i == stop) lk.ri = get_list_index(it->key());
        if (i >= start && i <= stop) continue;
        batch.Delete(it->key());
        i++;
    }
    lk.size -= stop - start;
    batch.Put(meta_key, encode_list_meta_value(lk.li, lk.ri, lk.size));
    s = db->Write(leveldb::WriteOptions(), &batch);
    check_status(con, s);
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
        std::string value;
        auto& key = con.argv[i];
        auto meta_key = encode_meta_key(key);
        auto s = db->Get(leveldb::ReadOptions(), meta_key, &value);
        if (s.ok()) {
            leveldb::WriteBatch batch;
            check_type(con, value, ktype::tlist);
            auto lk = decode_list_meta_value(value);
            auto enc_key = encode_list_key(key, is_blpop ? lk.li : lk.ri);
            s = db->Get(leveldb::ReadOptions(), enc_key, &value);
            check_status(con, s);
            pop_key(this, &batch, meta_key, enc_key, lk, is_blpop);
            s = db->Write(leveldb::WriteOptions(), &batch);
            check_status(con, s);
            con.append_reply_multi(2);
            con.append_reply_string(key);
            con.append_reply_string(value);
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

// 将一个blocking key添加到DB::blocking_keys和context_t::blocking_keys中
void DB::add_blocking_key(context_t& con, const key_t& key)
{
    con.blocking_keys.emplace_back(key);
    auto it = blocking_keys.find(key);
    if (it == blocking_keys.end()) {
        std::vector<size_t> idlist = { con.conn->id() };
        blocking_keys.emplace(key, std::move(idlist));
    } else {
        it->second.push_back(con.conn->id());
    }
}

void DB::set_context_to_block(context_t& con, int timeout)
{
    con.flags |= context_t::CON_BLOCK;
    con.block_start_time = angel::util::get_cur_time_ms();
    con.blocked_time = timeout * 1000;
    engine->add_block_client(con.conn->id());
}

void DB::blocking_pop(const key_t& key)
{
    std::string value;
    auto cl = blocking_keys.find(key);
    if (cl == blocking_keys.end()) return;
    auto now = angel::util::get_cur_time_ms();
    auto meta_key = encode_meta_key(key);
    auto s = db->Get(leveldb::ReadOptions(), meta_key, &value);
    assert(s.ok());
    auto lk = decode_list_meta_value(value);

    context_t context;
    auto conn = __server->get_server().get_connection(*cl->second.begin());
    if (!conn) return;
    auto& con = std::any_cast<context_t&>(conn->get_context());
    auto bops = get_last_cmd(con.last_cmd);
    auto enc_key = encode_list_key(key, bops == BLOCK_LPOP ? lk.li : lk.ri);
    s = db->Get(leveldb::ReadOptions(), enc_key, &value);
    assert(s.ok());
    context.append_reply_multi(3);
    context.append_reply_string(key);
    context.append_reply_string(value);
    double seconds = 1.0 * (now - con.block_start_time) / 1000;
    context.append("+(").append(d2s(seconds)).append("s)\r\n");
    conn->send(context.buf);

    clear_blocking_keys_for_context(con);
    engine->del_block_client(conn->id());

    leveldb::WriteBatch batch;
    pop_key(this, &batch, meta_key, enc_key, lk, bops == BLOCK_LPOP);
    if (bops == BLOCK_RPOPLPUSH) {
        auto src_value = value;
        auto meta_key = encode_meta_key(con.des);
        s = db->Get(leveldb::ReadOptions(), meta_key, &value);
        assert(s.ok() || s.IsNotFound());
        if (s.IsNotFound()) {
            batch.Put(encode_list_key(con.des, 0), src_value);
            batch.Put(meta_key, encode_list_meta_value(0, 0, 1));
        } else if (s.ok()) {
            auto lk = decode_list_meta_value(value);
            if (key != con.des) ++lk.size;
            batch.Put(encode_list_key(con.des, --lk.li), src_value);
            batch.Put(meta_key, encode_list_meta_value(lk.li, lk.ri, lk.size));
        }
    }
    s = db->Write(leveldb::WriteOptions(), &batch);
    assert(s.ok());
    touch_watch_key(key);
}

// 清空con.blocking_keys，并从DB::blocking_keys中移除所有con
void DB::clear_blocking_keys_for_context(context_t& con)
{
    for (auto& it : con.blocking_keys) {
        auto cl = blocking_keys.find(it);
        if (cl != blocking_keys.end()) {
            for (auto c = cl->second.begin(); c != cl->second.end(); ++c) {
                if (*c == con.conn->id()) {
                    cl->second.erase(c);
                    break;
                }
            }
            if (cl->second.empty())
                blocking_keys.erase(it);
        }
    }
    con.flags &= ~context_t::CON_BLOCK;
    con.blocking_keys.clear();
}

errstr_t DB::del_list_key(const key_t& key)
{
    leveldb::WriteBatch batch;
    auto err = del_list_key_batch(&batch, key);
    if (err) return err;
    auto s = db->Write(leveldb::WriteOptions(), &batch);
    if (s.ok()) return std::nullopt;
    return s;
}

errstr_t DB::del_list_key_batch(leveldb::WriteBatch *batch, const key_t& key)
{
    std::string value;
    auto meta_key = encode_meta_key(key);
    auto s = db->Get(leveldb::ReadOptions(), meta_key, &value);
    if (s.IsNotFound()) return std::nullopt;
    if (!s.ok()) return s;
    auto lk = decode_list_meta_value(value);
    auto it = newIterator();
    for (it->Seek(encode_list_key(key, lk.li)); it->Valid(); it->Next()) {
        batch->Delete(it->key());
        if (--lk.size == 0)
            break;
    }
    assert(lk.size == 0);
    batch->Delete(meta_key);
    return std::nullopt;
}

void DB::rename_list_key(leveldb::WriteBatch *batch, const key_t& key,
                         const std::string& meta_value, const key_t& newkey)
{
    long long i = 0;
    auto lk = decode_list_meta_value(meta_value);
    auto it = newIterator();
    for (it->Seek(encode_list_key(key, lk.li)); it->Valid(); it->Next()) {
        batch->Put(encode_list_key(newkey, i++), it->value());
        batch->Delete(it->key());
        if (--lk.size == 0)
            break;
    }
    assert(lk.size == 0);
    batch->Put(encode_meta_key(newkey), encode_list_meta_value(0, i - 1, i));
    batch->Delete(encode_meta_key(key));
}

int keycomp::list_compare(const leveldb::Slice& l, const leveldb::Slice& r) const
{
    auto begin1 = l.data() + 1, begin2 = r.data() + 1;
    auto s1 = strrchr(begin1, ':');
    auto s2 = strrchr(begin2, ':');
    leveldb::Slice key1(begin1, s1 - begin1), key2(begin2, s2 - begin2);
    int res = key1.compare(key2);
    if (res) return res;
    auto i1 = atoll(s1 + 1), i2 = atoll(s2 + 1);
    return i1 - i2;
}
