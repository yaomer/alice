#include "internal.h"

#include "../server.h"

using namespace alice;
using namespace alice::ssdb;

// list-meta-value: [type][lindex][:][rindex][:][size]
// range: [lindex, rindex]
static inline std::string
encode_list_meta_value(int li, int ri, int size)
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

static inline void
decode_list_meta_value(const std::string& value, int& li, int& ri, int& size)
{
    const char *s = value.c_str() + 1;
    li = atoi(s);
    ri = atoi(strchr(s, ':') + 1);
    size = atoi(strrchr(s, ':') + 1);
}

static inline std::string
encode_list_key(const std::string& key, int number)
{
    std::string buf;
    buf.append(1, ktype::tlist);
    buf.append(key);
    buf.append(1, ':');
    buf.append(i2s(number));
    return buf;
}

static inline int get_list_index(leveldb::Slice&& key)
{
    return atoi(strrchr(key.data(), ':')+1);
}

static inline bool is_no_hole(int li, int ri, int size)
{
    return ri - li + 1 == size;
}

void DB::pop_key(leveldb::WriteBatch *batch,
                 const std::string& meta_key, const std::string& enc_key,
                 int *li, int *ri, int *size, bool is_lpop)
{
    if (is_no_hole(*li, *ri, *size)) {
        is_lpop ? ++*li : --*ri;
    } else {
        auto it = newIterator();
        it->Seek(enc_key);
        assert(it->Valid());
        is_lpop ? it->Next() : it->Prev();
        assert(it->Valid());
        if (is_lpop) *li = get_list_index(it->key());
        else *ri = get_list_index(it->key());
    }
    batch->Put(meta_key, encode_list_meta_value(*li, *ri, --*size));
    if (*size == 0) batch->Delete(meta_key);
    batch->Delete(enc_key);
}

// LPUSH key value [value ...]
void DB::lpush(context_t& con)
{
    auto& key = con.argv[1];
    std::string value;
    leveldb::WriteBatch batch;
    int li = 0, ri = 0, size = 0;
    auto meta_key = encode_meta_key(key);
    check_expire(meta_key);
    auto s = db->Get(leveldb::ReadOptions(), meta_key, &value);
    if (!s.IsNotFound() && !s.ok()) reterr(con, s);
    if (s.ok()) {
        check_type(con, value, ktype::tlist);
        decode_list_meta_value(value, li, ri, size);
        --li;
    }

    for (int i = 2; i < con.argv.size(); i++) {
        batch.Put(encode_list_key(key, li--), con.argv[i]);
    }
    size += con.argv.size() - 2;
    batch.Put(meta_key, encode_list_meta_value(li+1, ri, size));

    s = db->Write(leveldb::WriteOptions(), &batch);
    check_status(con, s);
    touch_watch_key(key);
    con.append_reply_number(size);
    blocking_pop(key);
}

// LPUSH key value [value ...]
void DB::rpush(context_t& con)
{
    auto& key = con.argv[1];
    std::string value;
    leveldb::WriteBatch batch;
    int li = 0, ri = 0, size = 0;
    auto meta_key = encode_meta_key(key);
    check_expire(meta_key);
    auto s = db->Get(leveldb::ReadOptions(), meta_key, &value);
    if (!s.IsNotFound() && !s.ok()) reterr(con, s);
    if (s.ok()) {
        check_type(con, value, ktype::tlist);
        decode_list_meta_value(value, li, ri, size);
        ++ri;
    }

    for (int i = 2; i < con.argv.size(); i++) {
        batch.Put(encode_list_key(key, ri++), con.argv[i]);
    }
    size += con.argv.size() - 2;
    batch.Put(meta_key, encode_list_meta_value(li, ri-1, size));

    s = db->Write(leveldb::WriteOptions(), &batch);
    check_status(con, s);
    touch_watch_key(key);
    con.append_reply_number(size);
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
    int li, ri, size;
    leveldb::WriteBatch batch;
    decode_list_meta_value(value, li, ri, size);
    batch.Put(encode_list_key(key, is_lpushx ? --li : ++ri), con.argv[2]);
    batch.Put(meta_key, encode_list_meta_value(li, ri, ++size));

    s = db->Write(leveldb::WriteOptions(), &batch);
    check_status(con, s);
    touch_watch_key(key);
    con.append_reply_number(size);
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

    int li, ri, size;
    leveldb::WriteBatch batch;
    decode_list_meta_value(value, li, ri, size);

    value.clear();
    auto enc_key = encode_list_key(key, is_lpop ? li : ri);
    s = db->Get(leveldb::ReadOptions(), enc_key, &value);
    check_status(con, s);
    pop_key(&batch, meta_key, enc_key, &li, &ri, &size, is_lpop);

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
    int li, ri, size;
    decode_list_meta_value(value, li, ri, size);
    con.append_reply_number(size);
}

// LRANGE key start stop
void DB::lrange(context_t& con)
{
    std::string value;
    auto& key = con.argv[1];
    int start = str2l(con.argv[2]);
    if (str2numerr()) ret(con, shared.integer_err);
    int stop = str2l(con.argv[3]);
    if (str2numerr()) ret(con, shared.integer_err);
    auto meta_key = encode_meta_key(key);
    check_expire(meta_key);
    auto s = db->Get(leveldb::ReadOptions(), meta_key, &value);
    if (s.IsNotFound()) ret(con, shared.nil);
    check_status(con, s);
    check_type(con, value, ktype::tlist);
    int li, ri, size;
    decode_list_meta_value(value, li, ri, size);
    int upper = size - 1;
    int lower = -size;
    if (check_range(con, start, stop, lower, upper) == C_ERR)
        return;
    int ranges = stop - start + 1;
    con.append_reply_multi(ranges);
    auto it = newIterator();
    for (it->Seek(encode_list_key(key, li+start)); it->Valid(); it->Next()) {
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
        timeout = str2l(con.argv[con.argv.size()-1]);
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
    int li, ri, size;
    decode_list_meta_value(src_value, li, ri, size);
    src_value.clear();
    auto enc_key = encode_list_key(src_key, ri);
    s = db->Get(leveldb::ReadOptions(), enc_key, &src_value);
    check_status(con, s);
    pop_key(&batch, src_meta_key, enc_key, &li, &ri, &size, false);
    // put src_value to des_key
    if (src_key == des_key) {
        --li;
        touch_watch_key(src_key);
    } else {
        li = ri = size = 0;
        s = db->Get(leveldb::ReadOptions(), des_meta_key, &des_value);
        if (s.ok()) {
            check_type(con, des_value, ktype::tlist);
            decode_list_meta_value(des_value, li, ri, size);
            --li;
            touch_watch_key(src_key);
            touch_watch_key(des_key);
        } else if (s.IsNotFound()) {
            touch_watch_key(src_key);
        } else
            reterr(con, s);
    }
    batch.Put(encode_list_key(des_key, li), src_value);
    batch.Put(des_meta_key, encode_list_meta_value(li, ri, ++size));

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
    int count = str2l(con.argv[2].c_str());
    if (str2numerr()) ret(con, shared.integer_err);
    std::string enc_key, value;
    auto meta_key = encode_meta_key(key);
    check_expire(meta_key);
    auto s = db->Get(leveldb::ReadOptions(), meta_key, &value);
    if (s.IsNotFound()) ret(con, shared.n0);
    check_status(con, s);
    check_type(con, value, ktype::tlist);
    int rems = 0, li, ri, size;
    leveldb::WriteBatch batch;
    decode_list_meta_value(value, li, ri, size);
    // 因为可能会在中间删除元素，所以需要更新索引范围[li, ri]
    bool update = false;
    auto it = newIterator();
    if (count > 0) {
        for (it->Seek(encode_list_key(key, li)); it->Valid(); it->Next()) {
            if (it->value() == con.argv[3]) {
                batch.Delete(it->key());
                ++rems;
                if (--count == 0) {
                    if (!update && rems != size) { // 恰好删除了最左边的几个元素
                        it->Next();
                        assert(it->Valid());
                        li = get_list_index(it->key());
                    }
                    break;
                }
            } else {
                if (!update) {
                    update = true;
                    li = get_list_index(it->key());
                }
                ri = get_list_index(it->key());
            }
        }
    } else if (count < 0) {
        for (it->Seek(encode_list_key(key, ri)); it->Valid(); it->Prev()) {
            if (it->value() == con.argv[3]) {
                batch.Delete(it->key());
                ++rems;
                if (++count == 0) {
                    if (!update && rems != size) { // 恰好删除了最右边的几个元素
                        it->Prev();
                        assert(it->Valid());
                        ri = get_list_index(it->key());
                    }
                    break;
                }
            } else {
                if (!update) {
                    update = true;
                    ri = get_list_index(it->key());
                }
                li = get_list_index(it->key());
            }
        }
    } else {
        for (it->Seek(encode_list_key(key, li)); it->Valid(); it->Next()) {
            if (it->value() == con.argv[3]) {
                batch.Delete(it->key());
                ++rems;
            } else {
                if (!update) {
                    update = true;
                    li = get_list_index(it->key());
                }
                ri = get_list_index(it->key());
            }
        }
    }
    if (rems == size) {
        batch.Delete(meta_key);
    } else {
        batch.Put(meta_key, encode_list_meta_value(li, ri, size-rems));
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
    int index = str2l(con.argv[2]);
    if (str2numerr()) ret(con, shared.integer_err);
    std::string value;
    auto meta_key = encode_meta_key(key);
    check_expire(meta_key);
    auto s = db->Get(leveldb::ReadOptions(), meta_key, &value);
    if (s.IsNotFound()) ret(con, shared.nil);
    check_status(con, s);
    check_type(con, value, ktype::tlist);

    int li, ri, size;
    decode_list_meta_value(value, li, ri, size);
    if (index < 0)
        index += size;
    if (index < 0 || index >= size)
        ret(con, shared.nil);
    if (is_no_hole(li, ri, size)) {
        value.clear();
        auto enc_key = encode_list_key(key, li + index);
        s = db->Get(leveldb::ReadOptions(), enc_key, &value);
        check_status(con, s);
        con.append_reply_string(value);
        return;
    }
    auto it = newIterator();
    for (it->Seek(encode_list_key(key, li)); it->Valid(); it->Next()) {
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
    int index = str2l(con.argv[2]);
    if (str2numerr()) ret(con, shared.integer_err);
    std::string value;
    auto meta_key = encode_meta_key(key);
    check_expire(meta_key);
    auto s = db->Get(leveldb::ReadOptions(), meta_key, &value);
    if (s.IsNotFound()) ret(con, shared.no_such_key);
    check_status(con, s);
    check_type(con, value, ktype::tlist);

    int li, ri, size;
    std::string enc_key;
    decode_list_meta_value(value, li, ri, size);
    if (index < 0)
        index += size;
    if (index < 0 || index >= size)
        ret(con, shared.index_out_of_range);
    if (is_no_hole(li, ri, size)) {
        enc_key = encode_list_key(key, index);
    } else {
        auto it = newIterator();
        for (it->Seek(encode_list_key(key, li)); it->Valid(); it->Next()) {
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
    int start = str2l(con.argv[2]);
    if (str2numerr()) ret(con, shared.integer_err);
    int stop = str2l(con.argv[3]);
    if (str2numerr()) ret(con, shared.integer_err);
    std::string value;
    auto meta_key = encode_meta_key(key);
    check_expire(meta_key);
    auto s = db->Get(leveldb::ReadOptions(), meta_key, &value);
    if (s.IsNotFound()) ret(con, shared.ok);
    check_status(con, s);
    check_type(con, value, ktype::tlist);

    int li, ri, size;
    decode_list_meta_value(value, li, ri, size);
    if (start < 0)
        start += size;
    if (stop < 0)
        stop += size;
    if (start > size - 1 || start > stop || stop > size - 1 || start == stop) {
        auto err = del_list_key(key);
        if (err) reterr(con, err.value());
        ret(con, shared.ok);
    }
    int i = 0;
    leveldb::WriteBatch batch;
    auto it = newIterator();
    for (it->Seek(encode_list_key(key, li)); it->Valid(); it->Next()) {
        check_status(con, it->status());
        if (i == start) li = get_list_index(it->key());
        if (i == stop) ri = get_list_index(it->key());
        if (i >= start && i <= stop) continue;
        batch.Delete(it->key());
        i++;
    }
    size -= stop - start;
    batch.Put(meta_key, encode_list_meta_value(li, ri, size));
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
            int li, ri, size;
            leveldb::WriteBatch batch;
            check_type(con, value, ktype::tlist);
            decode_list_meta_value(value, li, ri, size);
            value.clear();
            auto enc_key = encode_list_key(key, is_blpop ? li : ri);
            s = db->Get(leveldb::ReadOptions(), enc_key, &value);
            check_status(con, s);
            pop_key(&batch, meta_key, enc_key, &li, &ri, &size, is_blpop);
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
    int li, ri, size;
    decode_list_meta_value(value, li, ri, size);

    context_t context;
    auto conn = __server->get_server().get_connection(*cl->second.begin());
    if (!conn) return;
    auto& con = std::any_cast<context_t&>(conn->get_context());
    auto bops = get_last_cmd(con.last_cmd);
    auto enc_key = encode_list_key(key, bops == BLOCK_LPOP ? li : ri);
    value.clear();
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
    pop_key(&batch, meta_key, enc_key, &li, &ri, &size, bops == BLOCK_LPOP);
    if (bops == BLOCK_RPOPLPUSH) {
        auto src_value = value;
        value.clear();
        auto meta_key = encode_meta_key(con.des);
        s = db->Get(leveldb::ReadOptions(), meta_key, &value);
        assert(s.ok() || s.IsNotFound());
        if (s.IsNotFound()) {
            batch.Put(encode_list_key(con.des, 0), src_value);
            batch.Put(meta_key, encode_list_meta_value(0, 0, 1));
        } else if (s.ok()) {
            int li, ri, size;
            decode_list_meta_value(value, li, ri, size);
            if (key != con.des) ++size;
            batch.Put(encode_list_key(con.des, --li), src_value);
            batch.Put(meta_key, encode_list_meta_value(li, ri, size));
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
    int li, ri, size;
    decode_list_meta_value(value, li, ri, size);
    auto it = newIterator();
    for (it->Seek(encode_list_key(key, li)); it->Valid(); it->Next()) {
        batch->Delete(it->key());
        if (--size == 0)
            break;
    }
    assert(size == 0);
    batch->Delete(meta_key);
    return std::nullopt;
}

void DB::rename_list_key(leveldb::WriteBatch *batch, const key_t& key,
                         const std::string& meta_value, const key_t& newkey)
{
    int li, ri, size, i = 0;
    decode_list_meta_value(meta_value, li, ri, size);
    auto it = newIterator();
    for (it->Seek(encode_list_key(key, li)); it->Valid(); it->Next()) {
        batch->Put(encode_list_key(newkey, i++), it->value());
        batch->Delete(it->key());
        if (--size == 0)
            break;
    }
    assert(size == 0);
    batch->Put(encode_meta_key(newkey), encode_list_meta_value(0, i-1, i));
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
    auto i1 = atoi(s1 + 1), i2 = atoi(s2 + 1);
    return i1 - i2;
}
