#include "internal.h"

using namespace alice;
using namespace alice::ssdb;

#define SET_VAL ""
#define SET_ANCHOR_VAL ""

static inline std::string
encode_set_meta_value(size_t seq, size_t size)
{
    std::string buf;
    buf.append(1, ktype::tset);
    buf.append(i2s(seq));
    buf.append(1, ':');
    buf.append(i2s(size));
    return buf;
}

static inline void
decode_set_meta_value(const std::string& value, size_t *seq, size_t *size)
{
    const char *s = value.c_str() + 1;
    if (seq) *seq = atoll(s);
    if (size) *size = atoi(strchr(s, ':') + 1);
}

static inline std::string
encode_set_key(size_t seq, const std::string& member)
{
    std::string buf;
    buf.append(1, ktype::tset);
    buf.append(i2s(seq));
    buf.append(1, ':');
    buf.append(member);
    return buf;
}

static inline std::string get_set_member(leveldb::Slice&& key)
{
    key.remove_prefix(strchr(key.data(), ':') + 1 - key.data());
    return key.ToString();
}

static inline std::string get_set_anchor(size_t seq)
{
    std::string buf;
    buf.append(1, ktype::tset);
    buf.append(i2s(seq));
    buf.append(1, ':');
    return buf;
}

// SADD key member [member ...]
void DB::sadd(context_t& con)
{
    int adds = 0;
    std::string value;
    auto& key = con.argv[1];
    check_expire(key);
    auto meta_key = encode_meta_key(key);
    auto s = db->Get(leveldb::ReadOptions(), meta_key, &value);
    leveldb::WriteBatch batch;
    if (s.IsNotFound()) {
        size_t seq = get_next_seq();
        for (size_t i = 2; i < con.argv.size(); i++)
            batch.Put(encode_set_key(seq, con.argv[i]), SET_VAL);
        adds = con.argv.size() - 2;
        batch.Put(meta_key, encode_set_meta_value(seq, adds));
        batch.Put(get_set_anchor(seq), SET_ANCHOR_VAL);
    } else if (s.ok()) {
        size_t seq, size;
        check_type(con, value, ktype::tset);
        decode_set_meta_value(value, &seq, &size);
        for (size_t i = 2; i < con.argv.size(); i++) {
            value.clear();
            auto enc_key = encode_set_key(seq, con.argv[i]);
            s = db->Get(leveldb::ReadOptions(), enc_key, &value);
            if (s.ok()) continue;
            else if (!s.IsNotFound()) adderr(con, s);
            batch.Put(enc_key, SET_VAL);
            adds++;
        }
        batch.Put(meta_key, encode_set_meta_value(seq, size+adds));
    } else
        reterr(con, s);
    s = db->Write(leveldb::WriteOptions(), &batch);
    check_status(con, s);
    con.append_reply_number(adds);
    touch_watch_key(key);
}

// SISMEMBER key member
void DB::sismember(context_t& con)
{
    std::string value;
    auto& key = con.argv[1];
    auto& member = con.argv[2];
    auto s = db->Get(leveldb::ReadOptions(), encode_meta_key(key), &value);
    if (s.IsNotFound()) ret(con, shared.n0);
    check_status(con, s);
    check_type(con, value, ktype::tset);
    size_t seq;
    decode_set_meta_value(value, &seq, nullptr);
    s = db->Get(leveldb::ReadOptions(), encode_set_key(seq, member), &value);
    if (s.ok()) con.append(shared.n1);
    else if (s.IsNotFound()) con.append(shared.n0);
    else reterr(con, s);
}

// SPOP key
void DB::spop(context_t& con)
{
    std::string value;
    auto& key = con.argv[1];
    check_expire(key);
    auto meta_key = encode_meta_key(key);
    auto s = db->Get(leveldb::ReadOptions(), meta_key, &value);
    if (s.IsNotFound()) ret(con, shared.nil);
    check_status(con, s);
    check_type(con, value, ktype::tset);
    size_t seq, size;
    decode_set_meta_value(value, &seq, &size);
    std::default_random_engine e(time(nullptr));
    std::uniform_int_distribution<size_t> u(0, size - 1);
    size_t where = u(e);
    auto anchor = get_set_anchor(seq);
    auto it = db->NewIterator(leveldb::ReadOptions());
    leveldb::Slice pop_key;
    leveldb::WriteBatch batch;
    for (it->Seek(anchor), it->Next(); it->Valid(); it->Next()) {
        if (where-- == 0) {
            pop_key = it->key();
            batch.Delete(pop_key);
            break;
        }
    }
    if (--size == 0) {
        batch.Delete(meta_key);
        batch.Delete(anchor);
    } else {
        batch.Put(meta_key, encode_set_meta_value(seq, size));
    }
    s = db->Write(leveldb::WriteOptions(), &batch);
    check_status(con, s);
    con.append_reply_string(get_set_member(std::move(pop_key)));
    touch_watch_key(key);
}

// 产生count个[0, size)之间的随机数
static std::vector<size_t> get_rands(ssize_t count, size_t size)
{
    std::vector<size_t> rands;
    std::default_random_engine e(time(nullptr));
    std::uniform_int_distribution<size_t> u(0, size - 1);
    for (int i = 0; i < count; i++) {
        rands.emplace_back(u(e));
    }
    return rands;
}

static std::vector<size_t> get_rands_unrepeatable(ssize_t count, size_t size)
{
    size_t nr;
    std::vector<size_t> rands;
    std::unordered_set<size_t> randset;
    assert(count <= size);
    std::default_random_engine e(time(nullptr));
    std::uniform_int_distribution<size_t> u(0, size - 1);
    for (int i = 0; i < count; i++) {
        do {
            nr = u(e);
        } while (randset.find(nr) != randset.end());
        randset.emplace(nr);
        rands.emplace_back(nr);
    }
    return rands;
}

// SRANDMEMBER key [count]
void DB::srandmember(context_t& con)
{
    ssize_t count = 0;
    std::string value;
    auto& key = con.argv[1];
    check_expire(key);
    if (con.argv.size() > 2) {
        count = str2ll(con.argv[2]);
        if (str2numerr()) ret(con, shared.integer_err);
        if (count == 0) ret(con, shared.nil);
    }
    auto s = db->Get(leveldb::ReadOptions(), encode_meta_key(key), &value);
    if (s.IsNotFound()) ret(con, shared.nil);
    check_status(con, s);
    check_type(con, value, ktype::tset);
    size_t seq, size;
    decode_set_meta_value(value, &seq, &size);
    if (count >= static_cast<ssize_t>(size)) {
        con.append_reply_multi(size);
        auto anchor = get_set_anchor(seq);
        auto it = db->NewIterator(leveldb::ReadOptions());
        for (it->Seek(anchor), it->Next(); it->Valid(); it->Next()) {
            con.append_reply_string(get_set_member(it->key()));
            if (--size == 0)
                break;
        }
        assert(size == 0);
        return;
    }
    std::vector<size_t> rands;
    std::unordered_map<size_t, std::string> randmap;
    if (count == 0) count = -1;
    if (count < 0) {
        count = -count;
        rands = get_rands(count, size);
    } else {
        rands = get_rands_unrepeatable(count, size);
    }
    auto origin_rands = rands;
    std::sort(rands.begin(), rands.end(), std::less<size_t>());
    auto anchor = get_set_anchor(seq);
    auto it = db->NewIterator(leveldb::ReadOptions());
    size_t i = 0;
    auto where = rands[i];
    for (it->Seek(anchor), it->Next(); it->Valid(); it->Next()) {
        if (where-- == 0) {
            do {
                randmap[rands[i]] = get_set_member(it->key());
                if (++i == rands.size()) break;
                where = rands[i] - rands[i-1];
            } while (where == 0);
            if (i == rands.size())
                break;
            --where;
        }
        if (--size == 0)
            break;
    }
    con.append_reply_multi(count);
    for (auto& nr : origin_rands) {
        con.append_reply_string(randmap[nr]);
    }
}

// SREM key member [member ...]
void DB::srem(context_t& con)
{
    std::string value;
    auto& key = con.argv[1];
    check_expire(key);
    auto meta_key = encode_meta_key(key);
    auto s = db->Get(leveldb::ReadOptions(), meta_key, &value);
    if (s.IsNotFound()) ret(con, shared.n0);
    check_status(con, s);
    check_type(con, value, ktype::tset);
    int rems = 0;
    size_t seq, size;
    leveldb::WriteBatch batch;
    decode_set_meta_value(value, &seq, &size);
    for (size_t i = 2; i < con.argv.size(); i++) {
        auto enc_key = encode_set_key(seq, con.argv[i]);
        s = db->Get(leveldb::ReadOptions(), enc_key, &value);
        if (s.IsNotFound()) continue;
        else if (!s.ok()) adderr(con, s);
        batch.Delete(enc_key);
        rems++;
    }
    size -= rems;
    if (size == 0) {
        batch.Delete(meta_key);
        batch.Delete(get_set_anchor(seq));
    } else {
        batch.Put(meta_key, encode_set_meta_value(seq, size));
    }
    s = db->Write(leveldb::WriteOptions(), &batch);
    check_status(con, s);
    con.append_reply_number(rems);
    touch_watch_key(key);
}

// SMOVE source destination member
void DB::smove(context_t& con)
{
    std::string value;
    auto& src_key = con.argv[1];
    auto& des_key = con.argv[2];
    auto& member = con.argv[3];
    check_expire(src_key);
    check_expire(des_key);
    auto src_meta_key = encode_meta_key(src_key);
    auto s = db->Get(leveldb::ReadOptions(), src_meta_key, &value);
    if (s.IsNotFound()) ret(con, shared.n0);
    check_status(con, s);
    check_type(con, value, ktype::tset);
    size_t seq, size;
    decode_set_meta_value(value, &seq, &size);
    auto rem_key = encode_set_key(seq, member);
    s = db->Get(leveldb::ReadOptions(), rem_key, &value);
    if (s.IsNotFound()) ret(con, shared.n0);
    check_status(con, s);
    if (src_key == des_key) {
        touch_watch_key(src_key);
        ret(con, shared.n1);
    }
    leveldb::WriteBatch batch;
    if (--size == 0) {
        batch.Delete(src_meta_key);
        batch.Delete(get_set_anchor(seq));
    } else {
        batch.Put(src_meta_key, encode_set_meta_value(seq, size));
    }
    batch.Delete(rem_key);
    value.clear();
    auto des_meta_key = encode_meta_key(des_key);
    s = db->Get(leveldb::ReadOptions(), des_meta_key, &value);
    if (s.ok()) {
        check_type(con, value, ktype::tset);
        decode_set_meta_value(value, &seq, &size);
        rem_key = encode_set_key(seq, member);
        s = db->Get(leveldb::ReadOptions(), rem_key, &value);
        if (s.IsNotFound()) {
            batch.Put(rem_key, SET_VAL);
            batch.Put(des_meta_key, encode_set_meta_value(seq, size+1));
        } else if (!s.ok()) {
            reterr(con, s);
        }
    } else if (s.IsNotFound()) {
        size_t seq = get_next_seq();
        batch.Put(encode_set_key(seq, member), SET_VAL);
        batch.Put(des_meta_key, encode_set_meta_value(seq, 1));
        batch.Put(get_set_anchor(seq), SET_ANCHOR_VAL);
    } else {
        reterr(con, s);
    }
    s = db->Write(leveldb::WriteOptions(), &batch);
    check_status(con, s);
    touch_watch_key(src_key);
    touch_watch_key(des_key);
    con.append(shared.n1);
}

// SCARD key
void DB::scard(context_t& con)
{
    std::string value;
    auto& key = con.argv[1];
    check_expire(key);
    auto s = db->Get(leveldb::ReadOptions(), encode_meta_key(key), &value);
    if (s.IsNotFound()) ret(con, shared.n0);
    check_status(con, s);
    check_type(con, value, ktype::tset);
    size_t size;
    decode_set_meta_value(value, nullptr, &size);
    con.append_reply_number(size);
}

// SMEMBERS key
void DB::smembers(context_t& con)
{
    std::string value;
    auto& key = con.argv[1];
    check_expire(key);
    auto s = db->Get(leveldb::ReadOptions(), encode_meta_key(key), &value);
    if (s.IsNotFound()) ret(con, shared.nil);
    check_status(con, s);
    check_type(con, value, ktype::tset);
    size_t seq, size;
    decode_set_meta_value(value, &seq, &size);
    auto anchor = get_set_anchor(seq);
    auto it = db->NewIterator(leveldb::ReadOptions());
    con.append_reply_multi(size);
    for (it->Seek(anchor), it->Next(); it->Valid(); it->Next()) {
        con.append_reply_string(get_set_member(it->key()));
        if (--size == 0)
            break;
    }
    assert(size == 0);
}

void DB::_sinter(context_t& con, std::unordered_set<std::string>& rset, int start)
{
    std::string value;
    size_t min = 0, j = 0, seq, size, i;
    std::unordered_map<size_t, size_t> seqmap;
    // 挑选出元素最少的集合
    for (size_t i = start; i < con.argv.size(); i++) {
        value.clear();
        check_expire(con.argv[i]);
        auto s = db->Get(leveldb::ReadOptions(), encode_meta_key(con.argv[i]), &value);
        if (s.IsNotFound()) ret(con, shared.nil);
        check_status(con, s);
        check_type(con, value, ktype::tset);
        decode_set_meta_value(value, &seq, &size);
        seqmap[i] = seq;
        if (min == 0 || size < min) {
            min = size;
            j = i;
        }
    }
    auto anchor = get_set_anchor(seqmap[j]);
    auto it = db->NewIterator(leveldb::ReadOptions());
    for (it->Seek(anchor), it->Next(); it->Valid(); it->Next()) {
        auto member = get_set_member(it->key());
        for (i = start; i < con.argv.size(); i++) {
            if (i == j) continue;
            auto s = db->Get(leveldb::ReadOptions(), encode_set_key(seqmap[i], member), &value);
            if (s.IsNotFound()) break;
            check_status(con, s);
        }
        if (i == con.argv.size())
            rset.emplace(member);
        if (--min == 0)
            break;
    }
    assert(min == 0);
}

void DB::_sunion(context_t& con, std::unordered_set<std::string>& rset, int start)
{
    size_t seq, size;
    std::string value;
    for (size_t i = start; i < con.argv.size(); i++) {
        value.clear();
        check_expire(con.argv[i]);
        auto s = db->Get(leveldb::ReadOptions(), encode_meta_key(con.argv[i]), &value);
        if (s.IsNotFound()) continue;
        check_status(con, s);
        check_type(con, value, ktype::tset);
        decode_set_meta_value(value, &seq, &size);
        auto anchor = get_set_anchor(seq);
        auto it = db->NewIterator(leveldb::ReadOptions());
        for (it->Seek(anchor), it->Next(); it->Valid(); it->Next()) {
            rset.emplace(get_set_member(it->key()));
            if (--size == 0)
                break;
        }
        assert(size == 0);
    }
}

static void _sreply(context_t& con, std::unordered_set<std::string>& rset)
{
    if (rset.empty()) ret(con, shared.nil);
    con.append_reply_multi(rset.size());
    for (auto& member : rset)
        con.append_reply_string(member);
}

void DB::_sstore(context_t& con, std::unordered_set<std::string>& rset)
{
    std::string value;
    auto& key = con.argv[1];
    auto meta_key = encode_meta_key(key);
    auto s = db->Get(leveldb::ReadOptions(), meta_key, &value);
    leveldb::WriteBatch batch;
    if (s.ok()) {
        del_set_key_batch(&batch, key);
    } else if (!s.IsNotFound())
        reterr(con, s);
    size_t seq = get_next_seq();
    for (auto& member : rset) {
        batch.Put(encode_set_key(seq, member), SET_VAL);
    }
    batch.Put(meta_key, encode_set_meta_value(seq, rset.size()));
    batch.Put(get_set_anchor(seq), SET_ANCHOR_VAL);
    s = db->Write(leveldb::WriteOptions(), &batch);
    check_status(con, s);
    con.append_reply_number(rset.size());
}

// SINTER key [key ...]
void DB::sinter(context_t& con)
{
    std::unordered_set<std::string> rset;
    auto pos = con.buf.size();
    _sinter(con, rset, 1);
    if (pos == con.buf.size())
        _sreply(con, rset);
}

// SINTERSTORE destination key [key ...]
void DB::sinterstore(context_t& con)
{
    std::unordered_set<std::string> rset;
    auto pos = con.buf.size();
    _sinter(con, rset, 2);
    if (pos == con.buf.size())
        _sstore(con, rset);
}

// SUNION key [key ...]
void DB::sunion(context_t& con)
{
    std::unordered_set<std::string> rset;
    auto pos = con.buf.size();
    _sunion(con, rset, 1);
    if (pos == con.buf.size())
        _sreply(con, rset);
}

// SUNIONSTORE destination key [key ...]
void DB::sunionstore(context_t& con)
{
    std::unordered_set<std::string> rset;
    auto pos = con.buf.size();
    _sunion(con, rset, 2);
    if (pos == con.buf.size())
        _sstore(con, rset);
}

void DB::sdiff(context_t& con)
{
    // TODO:
}

void DB::sdiffstore(context_t& con)
{
    // TODO:
}

errstr_t DB::del_set_key(const key_t& key)
{
    leveldb::WriteBatch batch;
    auto err = del_set_key_batch(&batch, key);
    if (err) return err;
    auto s = db->Write(leveldb::WriteOptions(), &batch);
    if (!s.ok()) return s;
    return std::nullopt;
}

errstr_t DB::del_set_key_batch(leveldb::WriteBatch *batch, const key_t& key)
{
    std::string value;
    auto meta_key = encode_meta_key(key);
    auto s = db->Get(leveldb::ReadOptions(), meta_key, &value);
    if (s.IsNotFound()) return std::nullopt;
    if (!s.ok()) return s;
    size_t seq, size;
    decode_set_meta_value(value, &seq, &size);
    auto anchor = get_set_anchor(seq);
    auto it = db->NewIterator(leveldb::ReadOptions());
    for (it->Seek(anchor), it->Next(); it->Valid(); it->Next()) {
        batch->Delete(it->key());
        if (--size == 0)
            break;
    }
    assert(size == 0);
    batch->Delete(meta_key);
    batch->Delete(anchor);
    return std::nullopt;
}


void DB::rename_set_key(leveldb::WriteBatch *batch, const key_t& key,
                        const std::string& meta_value, const key_t& newkey)
{
    size_t seq, size;
    decode_set_meta_value(meta_value, &seq, &size);
    size_t newseq = get_next_seq();
    size_t newsize = size;
    auto anchor = get_set_anchor(seq);
    auto it = db->NewIterator(leveldb::ReadOptions());
    for (it->Seek(anchor), it->Next(); it->Valid(); it->Next()) {
        batch->Put(encode_set_key(newseq, get_set_member(it->key())), it->value());
        batch->Delete(it->key());
        if (--size == 0)
            break;
    }
    assert(size == 0);
    batch->Put(encode_meta_key(newkey), encode_set_meta_value(newseq, newsize));
    batch->Put(get_set_anchor(newseq), SET_ANCHOR_VAL);
    batch->Delete(encode_meta_key(key));
    batch->Delete(anchor);
}
