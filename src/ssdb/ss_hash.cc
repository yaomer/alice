#include "internal.h"

using namespace alice;
using namespace alice::ssdb;

#define HASH_ANCHOR_VAL ""

static inline std::string
encode_hash_meta_value(size_t seq, size_t size)
{
    std::string buf;
    buf.append(1, ktype::thash);
    buf.append(i2s(seq));
    buf.append(1, ':');
    buf.append(i2s(size));
    return buf;
}

static inline void
decode_hash_meta_value(const std::string& value, size_t *seq, size_t *size)
{
    const char *s = value.c_str() + 1;
    if (seq) *seq = atoll(s);
    if (size) *size = atoi(strchr(s, ':') + 1);
}

static inline std::string
encode_hash_key(size_t seq, const std::string& field)
{
    std::string buf;
    buf.append(1, ktype::thash);
    buf.append(i2s(seq));
    buf.append(1, ':');
    buf.append(field);
    return buf;
}

static inline std::string get_hash_anchor(size_t seq)
{
    std::string buf;
    buf.append(1, ktype::thash);
    buf.append(i2s(seq));
    buf.append(1, ':');
    return buf;
}

static leveldb::Slice remove_field_prefix(leveldb::Slice&& field)
{
    field.remove_prefix(strchr(field.data(), ':') + 1 - field.data());
    return field;
}

// HSET key field value
void DB::hset(context_t& con)
{
    auto& key = con.argv[1];
    auto& field = con.argv[2];
    check_expire(key);
    // touch_watch_key(key);
    std::string value;
    leveldb::WriteBatch batch;
    auto meta_key = encode_meta_key(key);
    auto s = db->Get(leveldb::ReadOptions(), meta_key, &value);
    if (s.IsNotFound()) {
        auto seq = get_next_seq();
        batch.Put(meta_key, encode_hash_meta_value(seq, 1));
        batch.Put(encode_hash_key(seq, field), con.argv[3]);
        batch.Put(get_hash_anchor(seq), HASH_ANCHOR_VAL);
        s = db->Write(leveldb::WriteOptions(), &batch);
        check_status(con, s);
        ret(con, shared.n1);
    }
    check_status(con, s);
    check_type(con, value, ktype::thash);
    size_t seq, size;
    bool found = false;
    decode_hash_meta_value(value, &seq, &size);
    auto enc_key = encode_hash_key(seq, field);
    value.clear();
    s = db->Get(leveldb::ReadOptions(), enc_key, &value);
    if (s.ok()) found = true;
    else if (!s.IsNotFound()) reterr(con, s);
    if (!found) {
        batch.Put(meta_key, encode_hash_meta_value(seq, ++size));
    }
    batch.Put(enc_key, con.argv[3]);
    s = db->Write(leveldb::WriteOptions(), &batch);
    check_status(con, s);
    con.append(found ? shared.n0 : shared.n1);
}

// HSETNX key field value
void DB::hsetnx(context_t& con)
{
    auto& key = con.argv[1];
    auto& field = con.argv[2];
    check_expire(key);
    std::string value;
    leveldb::WriteBatch batch;
    auto meta_key = encode_meta_key(key);
    auto s = db->Get(leveldb::ReadOptions(), meta_key, &value);
    if (s.IsNotFound()) {
        auto seq = get_next_seq();
        batch.Put(meta_key, encode_hash_meta_value(seq, 1));
        batch.Put(encode_hash_key(seq, field), con.argv[3]);
        batch.Put(get_hash_anchor(seq), HASH_ANCHOR_VAL);
        s = db->Write(leveldb::WriteOptions(), &batch);
        check_status(con, s);
        // touch_watch_key(key);
        ret(con, shared.n1);
    }
    check_status(con, s);
    check_type(con, value, ktype::thash);
    size_t seq, size;
    decode_hash_meta_value(value, &seq, &size);
    auto enc_key = encode_hash_key(seq, field);
    value.clear();
    s = db->Get(leveldb::ReadOptions(), enc_key, &value);
    if (s.ok()) ret(con, shared.n0);
    if (!s.IsNotFound()) reterr(con, s);
    batch.Put(meta_key, encode_hash_meta_value(seq, ++size));
    batch.Put(enc_key, con.argv[3]);
    s = db->Write(leveldb::WriteOptions(), &batch);
    check_status(con, s);
    // touch_watch_key(key);
    con.append(shared.n1);
}

// HGET key field
void DB::hget(context_t& con)
{
    auto& key = con.argv[1];
    auto& field = con.argv[2];
    check_expire(key);
    std::string value;
    auto s = db->Get(leveldb::ReadOptions(), encode_meta_key(key), &value);
    if (s.IsNotFound()) ret(con, shared.nil);
    check_status(con, s);
    check_type(con, value, ktype::thash);
    size_t seq;
    decode_hash_meta_value(value, &seq, nullptr);
    value.clear();
    s = db->Get(leveldb::ReadOptions(), encode_hash_key(seq, field), &value);
    if (s.IsNotFound()) ret(con, shared.nil);
    if (!s.ok()) reterr(con, s);
    con.append_reply_string(value);
}

// HEXISTS key field
void DB::hexists(context_t& con)
{
    auto& key = con.argv[1];
    auto& field = con.argv[2];
    check_expire(key);
    std::string value;
    auto s = db->Get(leveldb::ReadOptions(), encode_meta_key(key), &value);
    if (s.IsNotFound()) ret(con, shared.n0);
    check_status(con, s);
    check_type(con, value, ktype::thash);
    size_t seq;
    decode_hash_meta_value(value, &seq, nullptr);
    value.clear();
    s = db->Get(leveldb::ReadOptions(), encode_hash_key(seq, field), &value);
    if (s.ok()) ret(con, shared.n1);
    if (s.IsNotFound()) ret(con, shared.n0);
    reterr(con, s);
}

// HDEL key field [field ...]
void DB::hdel(context_t& con)
{
    auto& key = con.argv[1];
    check_expire(key);
    std::string value;
    auto meta_key = encode_meta_key(key);
    auto s = db->Get(leveldb::ReadOptions(), meta_key, &value);
    if (s.IsNotFound()) ret(con, shared.n0);
    check_status(con, s);
    check_type(con, value, ktype::thash);
    int dels = 0;
    size_t seq, size;
    leveldb::WriteBatch batch;
    decode_hash_meta_value(value, &seq, &size);
    for (size_t i = 2; i < con.argv.size(); i++) {
        auto enc_key = encode_hash_key(seq, con.argv[i]);
        s = db->Get(leveldb::ReadOptions(), enc_key, &value);
        if (s.ok()) {
            batch.Delete(enc_key);
            dels++;
        } else if (!s.IsNotFound())
            reterr(con, s);
    }
    if (dels == size) {
        batch.Delete(meta_key);
        batch.Delete(get_hash_anchor(seq));
    } else {
        batch.Put(meta_key, encode_hash_meta_value(seq, size-dels));
    }
    s = db->Write(leveldb::WriteOptions(), &batch);
    check_status(con, s);
    // touch_watch_key(key);
    con.append_reply_number(dels);
}

// HLEN key
void DB::hlen(context_t& con)
{
    auto& key = con.argv[1];
    check_expire(key);
    std::string value;
    auto s = db->Get(leveldb::ReadOptions(), encode_meta_key(key), &value);
    if (s.IsNotFound()) ret(con, shared.n0);
    check_status(con, s);
    check_type(con, value, ktype::thash);
    size_t size;
    decode_hash_meta_value(value, nullptr, &size);
    con.append_reply_number(size);
}

// HSTRLEN key field
void DB::hstrlen(context_t& con)
{
    auto& key = con.argv[1];
    auto& field = con.argv[2];
    check_expire(key);
    std::string value;
    auto s = db->Get(leveldb::ReadOptions(), encode_meta_key(key), &value);
    if (s.IsNotFound()) ret(con, shared.n0);
    check_status(con, s);
    check_type(con, value, ktype::thash);
    size_t seq;
    decode_hash_meta_value(value, &seq, nullptr);
    value.clear();
    s = db->Get(leveldb::ReadOptions(), encode_hash_key(seq, field), &value);
    if (s.IsNotFound()) ret(con, shared.n0);
    if (!s.ok()) reterr(con, s);
    con.append_reply_number(value.size());
}

// HINCRBY key field increment
void DB::hincrby(context_t& con)
{
    auto& key = con.argv[1];
    auto& field = con.argv[2];
    auto& incr_str = con.argv[3];
    check_expire(key);
    auto incr = str2ll(incr_str);
    if (str2numerr()) ret(con, shared.integer_err);
    std::string value;
    auto meta_key = encode_meta_key(key);
    auto s = db->Get(leveldb::ReadOptions(), meta_key, &value);
    if (s.IsNotFound()) {
        leveldb::WriteBatch batch;
        size_t seq = get_next_seq();
        batch.Put(meta_key, encode_hash_meta_value(seq, 1));
        batch.Put(encode_hash_key(seq, field), incr_str);
        batch.Put(get_hash_anchor(seq), HASH_ANCHOR_VAL);
        s = db->Write(leveldb::WriteOptions(), &batch);
        check_status(con, s);
        // touch_watch_key(key);
        ret(con, shared.n0);
    }
    check_status(con, s);
    check_type(con, value, ktype::thash);
    size_t seq, size;
    decode_hash_meta_value(value, &seq, &size);
    value.clear();
    auto enc_key = encode_hash_key(seq, field);
    s = db->Get(leveldb::ReadOptions(), enc_key, &value);
    if (s.ok()) {
        auto number = str2ll(value);
        if (str2numerr()) ret(con, shared.integer_err);
        incr += number;
        s = db->Put(leveldb::WriteOptions(), enc_key, i2s(incr));
    } else if (s.IsNotFound()) {
        leveldb::WriteBatch batch;
        batch.Put(meta_key, encode_hash_meta_value(seq, ++size));
        batch.Put(enc_key, incr_str);
        s = db->Write(leveldb::WriteOptions(), &batch);
    } else
        reterr(con, s);
    check_status(con, s);
    con.append_reply_number(incr);
    // touch_watch_key(key);
}

// HMSET key field value [field value ...]
void DB::hmset(context_t& con)
{
    auto& key = con.argv[1];
    check_expire(key);
    size_t size = con.argv.size();
    if (size % 2 != 0) ret(con, shared.argnumber_err);
    // touch_watch_key(key);
    std::string value;
    leveldb::WriteBatch batch;
    auto meta_key = encode_meta_key(key);
    auto s = db->Get(leveldb::ReadOptions(), meta_key, &value);
    if (s.IsNotFound()) {
        auto seq = get_next_seq();
        for (size_t i = 2; i < size; i += 2) {
            batch.Put(encode_hash_key(seq, con.argv[i]), con.argv[i+1]);
        }
        batch.Put(meta_key, encode_hash_meta_value(seq, (size-2)/2));
        batch.Put(get_hash_anchor(seq), HASH_ANCHOR_VAL);
        s = db->Write(leveldb::WriteOptions(), &batch);
        check_status(con, s);
        ret(con, shared.ok);
    }
    size_t seq, nums;
    check_status(con, s);
    check_type(con, value, ktype::thash);
    decode_hash_meta_value(value, &seq, &nums);
    for (size_t i = 2; i < size; i += 2) {
        auto enc_key = encode_hash_key(seq, con.argv[i]);
        s = db->Get(leveldb::ReadOptions(), enc_key, &value);
        if (s.IsNotFound()) nums++;
        else if (!s.ok()) reterr(con, s);
        batch.Put(enc_key, con.argv[i+1]);
    }
    batch.Put(meta_key, encode_hash_meta_value(seq, nums));
    s = db->Write(leveldb::WriteOptions(), &batch);
    check_status(con, s);
    con.append(shared.ok);
}

// HMGET key field [field ...]
void DB::hmget(context_t& con)
{
    auto& key = con.argv[1];
    size_t size = con.argv.size();
    check_expire(key);
    std::string value;
    auto s = db->Get(leveldb::ReadOptions(), encode_meta_key(key), &value);
    if (s.IsNotFound()) {
        con.append_reply_multi(size-2);
        for (size_t i = 2; i < size; i++)
            con.append(shared.nil);
        return;
    }
    check_status(con, s);
    check_type(con, value, ktype::thash);
    size_t seq;
    decode_hash_meta_value(value, &seq, nullptr);
    con.append_reply_multi(size-2);
    for (size_t i = 2; i < size; i++) {
        value.clear();
        s = db->Get(leveldb::ReadOptions(), encode_hash_key(seq, con.argv[i]), &value);
        if (s.ok()) con.append_reply_string(value);
        else if (s.IsNotFound()) con.append(shared.nil);
        else adderr(con, s);
    }
}

#define HGETKEYS     0
#define HGETVALUES   1
#define HGETALL      2

// HKEYS/HVALS/HGETALL key
void DB::_hget(context_t& con, int what)
{
    auto& key = con.argv[1];
    check_expire(key);
    std::string value;
    auto s = db->Get(leveldb::ReadOptions(), encode_meta_key(key), &value);
    if (s.IsNotFound()) ret(con, shared.nil);
    check_status(con, s);
    check_type(con, value, ktype::thash);
    size_t seq, size;
    decode_hash_meta_value(value, &seq, &size);
    auto anchor = get_hash_anchor(seq);
    con.append_reply_multi(what == HGETALL ? size * 2 : size);
    auto it = db->NewIterator(leveldb::ReadOptions());
    for (it->Seek(anchor), it->Next(); size-- > 0; it->Next()) {
        switch (what) {
        case HGETKEYS:
            con.append_reply_string(remove_field_prefix(it->key()).ToString());
            break;
        case HGETVALUES:
            con.append_reply_string(it->value().ToString());
            break;
        case HGETALL:
            con.append_reply_string(remove_field_prefix(it->key()).ToString());
            con.append_reply_string(it->value().ToString());
            break;
        default: assert(0);
        }
    }
}

void DB::hkeys(context_t& con)
{
    _hget(con, HGETKEYS);
}

void DB::hvals(context_t& con)
{
    _hget(con, HGETVALUES);
}

void DB::hgetall(context_t& con)
{
    _hget(con, HGETALL);
}

errstr_t DB::del_hash_key(const std::string& key)
{
    leveldb::WriteBatch batch;
    auto err = del_hash_key_batch(&batch, key);
    if (err) return err;
    auto s = db->Write(leveldb::WriteOptions(), &batch);
    if (!s.ok()) return s;
    return std::nullopt;
}

errstr_t DB::del_hash_key_batch(leveldb::WriteBatch *batch, const std::string& key)
{
    size_t seq, size;
    std::string value;
    auto meta_key = encode_meta_key(key);
    auto s = db->Get(leveldb::ReadOptions(), meta_key, &value);
    if (s.IsNotFound()) return std::nullopt;
    if (!s.ok()) return s;
    decode_hash_meta_value(value, &seq, &size);
    auto anchor = get_hash_anchor(seq);
    auto it = db->NewIterator(leveldb::ReadOptions());
    for (it->Seek(anchor), it->Next(); size-- > 0; it->Next()) {
        batch->Delete(it->key());
    }
    batch->Delete(meta_key);
    batch->Delete(anchor);
    return std::nullopt;
}
