#include "internal.h"

using namespace alice;
using namespace alice::ssdb;

static inline std::string
encode_string_meta_value()
{
    std::string buf;
    buf.append(1, ktype::tstring);
    return buf;
}

static inline std::string
encode_string_key(const std::string& key)
{
    std::string buf;
    buf.append(1, ktype::tstring);
    buf.append(key);
    return buf;
}

// SET key value
void DB::set(context_t& con)
{
    unsigned cmdops = 0;
    int64_t expire;
    auto& key = con.argv[1];
    if (parse_set_args(con, cmdops, expire) == C_ERR)
        return;
    if ((cmdops & (SET_NX | SET_XX)))
        ret(con, shared.syntax_err);
    std::string value;
    auto meta_key = encode_meta_key(key);
    auto s = db->Get(leveldb::ReadOptions(), meta_key, &value);
    if (cmdops & SET_NX) {
        if (s.ok()) ret(con, shared.nil);
        if (!s.IsNotFound()) reterr(con, s);
    } else if (cmdops & SET_XX) {
        if (s.IsNotFound()) ret(con, shared.nil);
        check_status(con, s);
    }
    leveldb::WriteBatch batch;
    if (s.ok()) {
        auto err = del_key_with_expire_batch(&batch, key);
        if (err) reterr(con, err.value());
    }
    batch.Put(encode_meta_key(key), encode_string_meta_value());
    batch.Put(encode_string_key(key), con.argv[2]);
    s = db->Write(leveldb::WriteOptions(), &batch);
    check_status(con, s);
    touch_watch_key(key);
    con.append(shared.ok);
    if (cmdops & (SET_EX | SET_PX)) {
        expire += angel::util::get_cur_time_ms();
        add_expire_key(key, expire);
    }
}

// SETNX key value
void DB::setnx(context_t& con)
{
    std::string value;
    auto& key = con.argv[1];
    auto meta_key = encode_meta_key(key);
    auto s = db->Get(leveldb::ReadOptions(), meta_key, &value);
    if (s.ok()) ret(con, shared.n0);
    if (!s.IsNotFound()) reterr(con, s);
    check_type(con, value, ktype::tstring);
    leveldb::WriteBatch batch;
    batch.Put(meta_key, encode_string_meta_value());
    batch.Put(encode_string_key(key), con.argv[2]);
    s = db->Write(leveldb::WriteOptions(), &batch);
    check_status(con, s);
    touch_watch_key(key);
    con.append(shared.n1);
}

// GET key
void DB::get(context_t& con)
{
    std::string value;
    auto& key = con.argv[1];
    auto s = db->Get(leveldb::ReadOptions(), encode_meta_key(key), &value);
    if (s.IsNotFound()) ret(con, shared.nil);
    check_status(con, s);
    check_type(con, value, ktype::tstring);
    value.clear();
    s = db->Get(leveldb::ReadOptions(), encode_string_key(key), &value);
    check_status(con, s);
    con.append_reply_string(value);
}

// GETSET key value
void DB::getset(context_t& con)
{
    auto& key = con.argv[1];
    auto& new_value = con.argv[2];
    check_expire(key);
    touch_watch_key(key);
    std::string value;
    auto meta_key = encode_meta_key(key);
    auto s = db->Get(leveldb::ReadOptions(), meta_key, &value);
    leveldb::WriteBatch batch;
    if (s.IsNotFound()) {
        batch.Put(meta_key, encode_string_meta_value());
        batch.Put(encode_string_key(key), new_value);
        s = db->Write(leveldb::WriteOptions(), &batch);
        check_status(con, s);
        ret(con, shared.nil);
    }
    check_status(con, s);
    check_type(con, value, ktype::tstring);
    value.clear();
    auto enc_key = encode_string_key(key);
    s = db->Get(leveldb::ReadOptions(), enc_key, &value);
    check_status(con, s);
    s = db->Put(leveldb::WriteOptions(), enc_key, new_value);
    check_status(con, s);
    con.append_reply_string(value);
}

// STRLEN key
void DB::strlen(context_t& con)
{
    std::string value;
    auto& key = con.argv[1];
    auto s = db->Get(leveldb::ReadOptions(), encode_meta_key(key), &value);
    if (s.IsNotFound()) ret(con, shared.n0);
    check_status(con, s);
    check_type(con, value, ktype::tstring);
    value.clear();
    s = db->Get(leveldb::ReadOptions(), encode_string_key(key), &value);
    check_status(con, s);
    con.append_reply_number(value.size());
}

// APPEND key value
void DB::append(context_t& con)
{
    auto& key = con.argv[1];
    check_expire(key);
    touch_watch_key(key);
    std::string value;
    auto meta_key = encode_meta_key(key);
    auto s = db->Get(leveldb::ReadOptions(), meta_key, &value);
    if (s.IsNotFound()) {
        leveldb::WriteBatch batch;
        batch.Put(meta_key, encode_string_meta_value());
        batch.Put(encode_string_key(key), con.argv[2]);
        s = db->Write(leveldb::WriteOptions(), &batch);
        check_status(con, s);
        con.append_reply_number(con.argv[2].size());
        return;
    }
    check_type(con, value, ktype::tstring);
    check_status(con, s);
    value.clear();
    auto enc_key = encode_string_key(key);
    s = db->Get(leveldb::ReadOptions(), enc_key, &value);
    check_status(con, s);
    value.append(con.argv[2]);
    s = db->Put(leveldb::WriteOptions(), enc_key, value);
    check_status(con, s);
    con.append_reply_number(value.size());
}

// MSET key value [key value ...]
void DB::mset(context_t& con)
{
    std::string value;
    leveldb::WriteBatch batch;
    size_t size = con.argv.size();
    if (size % 2 == 0) ret(con, shared.argnumber_err);
    for (size_t i = 1; i < size; i += 2) {
        auto& key = con.argv[i];
        check_expire(key);
        auto meta_key = encode_meta_key(key);
        auto s = db->Get(leveldb::ReadOptions(), meta_key, &value);
        if (s.ok()) {
            auto err = del_key_with_expire_batch(&batch, key);
            if (err) reterr(con, err.value());
        } else if (!s.IsNotFound())
            reterr(con, s);
        batch.Put(meta_key, encode_string_meta_value());
        batch.Put(encode_string_key(key), con.argv[i+1]);
        touch_watch_key(key);
    }
    auto s = db->Write(leveldb::WriteOptions(), &batch);
    check_status(con, s);
    con.append(shared.ok);
}

// MGET key [key ...]
void DB::mget(context_t& con)
{
    int size = con.argv.size();
    con.append_reply_multi(size - 1);
    for (int i = 1; i < size; i++) {
        auto& key = con.argv[i];
        check_expire(key);
        std::string value;
        auto meta_key = encode_meta_key(key);
        auto s = db->Get(leveldb::ReadOptions(), meta_key, &value);
        if (s.ok()) {
            if (get_type(value) == ktype::tstring) {
                value.clear();
                s = db->Get(leveldb::ReadOptions(), encode_string_key(key), &value);
                if (s.ok())
                    con.append_reply_string(value);
                else
                    adderr(con, s);
            } else
                con.append(shared.nil);
        } else if (s.IsNotFound())
            con.append(shared.nil);
        else
            adderr(con, s);
    }
}

void DB::_incr(context_t& con, int64_t incr)
{
    auto& key = con.argv[1];
    check_expire(key);
    std::string value;
    auto meta_key = encode_meta_key(key);
    auto s = db->Get(leveldb::ReadOptions(), meta_key, &value);
    if (s.ok()) {
        check_type(con, value, ktype::tstring);
        value.clear();
        s = db->Get(leveldb::ReadOptions(), encode_string_key(key), &value);
        check_status(con, s);
        auto number = str2ll(value);
        if (str2numerr()) ret(con, shared.integer_err);
        incr += number;
    } else if (!s.IsNotFound())
        reterr(con, s);
    s = db->Put(leveldb::WriteOptions(), encode_string_key(key), i2s(incr));
    check_status(con, s);
    con.append_reply_number(incr);
    touch_watch_key(key);
}

// INCR key
void DB::incr(context_t& con)
{
    _incr(con, 1);
}

// INCRBY key increment
void DB::incrby(context_t& con)
{
    auto increment = str2ll(con.argv[2]);
    if (str2numerr()) ret(con, shared.integer_err);
    _incr(con, increment);
}

// DECR key
void DB::decr(context_t& con)
{
    _incr(con, -1);
}

// DECRBY key decrement
void DB::decrby(context_t& con)
{
    auto decrement = str2ll(con.argv[2]);
    if (str2numerr()) ret(con, shared.integer_err);
    _incr(con, -decrement);
}

// SETRANGE key offset value
void DB::setrange(context_t& con)
{
    auto& key = con.argv[1];
    auto offset = str2l(con.argv[2]);
    auto& arg_value = con.argv[3];
    if (str2numerr() || offset < 0)
        ret(con, shared.integer_err);
    std::string value, new_value;
    check_expire(key);
    touch_watch_key(key);
    auto meta_key = encode_meta_key(key);
    auto s = db->Get(leveldb::ReadOptions(), meta_key, &value);
    if (s.IsNotFound()) {
        new_value.reserve(offset + arg_value.size());
        new_value.resize(offset, '\x00');
        new_value.append(arg_value);
        leveldb::WriteBatch batch;
        batch.Put(meta_key, encode_string_meta_value());
        batch.Put(encode_string_key(key), new_value);
        s = db->Write(leveldb::WriteOptions(), &batch);
        check_status(con, s);
        con.append_reply_number(new_value.size());
        return;
    }
    check_status(con, s);
    check_type(con, value, ktype::tstring);
    value.clear();
    s = db->Get(leveldb::ReadOptions(), encode_string_key(key), &value);
    check_status(con, s);
    new_value.swap(value);
    size_t len = offset + arg_value.size();
    if (len > new_value.capacity()) new_value.reserve(len);
    if (len > new_value.size()) new_value.resize(len);
    if (offset > new_value.size())
        new_value.resize(offset, '\x00');
    std::copy(arg_value.begin(), arg_value.end(), new_value.begin()+offset);
    s = db->Put(leveldb::WriteOptions(), encode_string_key(key), new_value);
    check_status(con, s);
    con.append_reply_number(new_value.size());
}

// GETRANGE key start end
void DB::getrange(context_t& con)
{
    auto& key = con.argv[1];
    int start = str2l(con.argv[2]);
    if (str2numerr()) ret(con, shared.integer_err);
    int stop = str2l(con.argv[3]);
    if (str2numerr()) ret(con, shared.integer_err);
    check_expire(key);
    std::string value;
    auto meta_key = encode_meta_key(key);
    auto s = db->Get(leveldb::ReadOptions(), meta_key, &value);
    if (s.IsNotFound()) ret(con, shared.nil);
    check_type(con, value, ktype::tstring);
    value.clear();
    s = db->Get(leveldb::ReadOptions(), encode_string_key(key), &value);
    check_status(con, s);
    int upper = value.size() - 1;
    int lower = -value.size();
    if (check_range(con, start, stop, lower, upper) == C_ERR)
        return;
    con.append_reply_string(value.substr(start, stop-start+1));
}

errstr_t DB::del_string_key(const key_t& key)
{
    leveldb::WriteBatch batch;
    auto err = del_string_key_batch(&batch, key);
    if (err) return err;
    auto s = db->Write(leveldb::WriteOptions(), &batch);
    if (s.ok()) return std::nullopt;
    return s;
}

errstr_t DB::del_string_key_batch(leveldb::WriteBatch *batch, const key_t& key)
{
    std::string value;
    auto s = db->Get(leveldb::ReadOptions(), encode_meta_key(key), &value);
    if (s.IsNotFound()) return std::nullopt;
    if (!s.ok()) return s;
    batch->Delete(encode_meta_key(key));
    batch->Delete(encode_string_key(key));
    return std::nullopt;
}

void DB::rename_string_key(leveldb::WriteBatch *batch, const key_t& key,
                           const std::string& meta_value, const key_t& newkey)
{
    UNUSED(meta_value);
    std::string value;
    auto s = db->Get(leveldb::ReadOptions(), encode_string_key(key), &value);
    assert(s.ok());
    batch->Put(encode_meta_key(newkey), encode_string_meta_value());
    batch->Put(encode_string_key(newkey), value);
    batch->Delete(encode_meta_key(key));
    batch->Delete(encode_string_key(key));
}
