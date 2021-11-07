#include "internal.h"

namespace alice {

namespace ssdb {

// We need a map<key, score> and a skiplist<score, key>

#define ZSET_ANCHOR_VAL ""

static const char zset_score_key = '0';
static const char zset_anchor = '-'; // ascii('-') = 45, ascii('.') = 46
static const char zset_end_anchor = ':'; // ascii(':') = 58, ascii('9') = 57

static inline std::string
encode_zset_meta_value(uint64_t seq, long long size)
{
    std::string buf;
    buf.append(1, ktype::tzset);
    save_len(buf, seq);
    save_len(buf, (uint64_t)size);
    return buf;
}

static inline zsk_info
decode_zset_meta_value(const std::string& value)
{
    zsk_info zk;
    char *ptr = const_cast<char*>(value.c_str()) + 1;
    ptr += load_len(ptr, &zk.seq);
    load_len(ptr, (uint64_t*)&zk.size);
    return zk;
}

// 利用leveldb按score进行排序
// <anchor><score,member><score1,member1>...<end-anchor>
static inline std::string
encode_zset_score(uint64_t seq, const std::string& score, const std::string& member)
{
    std::string buf;
    buf.append(1, ktype::tscore);
    save_len(buf, seq);
    buf.append(score);
    buf.append(1, ':');
    buf.append(member);
    buf.append(1, zset_score_key);
    return buf;
}

static inline std::string
get_zset_score(leveldb::Slice&& slice)
{
    uint64_t seq;
    char *ptr = const_cast<char*>(slice.data() + 1);
    ptr += load_len(ptr, &seq);
    return slice.ToString().substr(ptr - slice.data(), strchr(ptr, ':') - ptr);
}

static inline std::string
get_zset_anchor(uint64_t seq)
{
    std::string buf;
    buf.append(1, ktype::tscore);
    save_len(buf, seq);
    buf.append(1, zset_anchor);
    return buf;
}

static inline std::string
get_zset_end_anchor(uint64_t seq)
{
    std::string buf;
    buf.append(1, ktype::tscore);
    save_len(buf, seq);
    buf.append(1, zset_end_anchor);
    return buf;
}

// 可以O(1)时间通过member找到score
static inline std::string
encode_zset_member(uint64_t seq, const std::string& member)
{
    std::string buf;
    buf.append(1, ktype::tzset);
    save_len(buf, seq);
    buf.append(member);
    return buf;
}

static void add_zset_meta_info(leveldb::WriteBatch *batch, const std::string& meta_key,
                               uint64_t seq, long long size)
{
    batch->Put(meta_key, encode_zset_meta_value(seq, size));
    batch->Put(get_zset_anchor(seq), ZSET_ANCHOR_VAL);
    batch->Put(get_zset_end_anchor(seq), ZSET_ANCHOR_VAL);
}

static void del_zset_meta_info(leveldb::WriteBatch *batch, const std::string& meta_key, uint64_t seq)
{
    batch->Delete(meta_key);
    batch->Delete(get_zset_anchor(seq));
    batch->Delete(get_zset_end_anchor(seq));
}

zsk_iterator DB::zset_get_min(zsk_info& zk)
{
    auto anchor = get_zset_anchor(zk.seq);
    auto it = newIterator();
    it->Seek(anchor);
    it->Next();
    return zsk_iterator(std::move(it), 0);
}

zsk_iterator DB::zset_get_max(zsk_info& zk)
{
    auto anchor = get_zset_end_anchor(zk.seq);
    auto it = newIterator();
    it->Seek(anchor);
    it->Prev();
    return zsk_iterator(std::move(it), zk.size - 1);
}

// returned `it` is invalid if not found
zsk_iterator DB::zset_lower_bound(zsk_info& zk, double score)
{
    auto it = zset_get_min(zk);
    for (size_t i = 0; i < zk.size && it.Valid(); it.Next(), ++i) {
        auto x = str2f(get_zset_score(it.key()));
        if (x >= score) break;
    }
    return it;
}

zsk_iterator DB::zset_upper_bound(zsk_info& zk, double score)
{
    auto it = zset_lower_bound(zk, score);
    while (it.Valid() && str2f(get_zset_score(it.key())) == score) {
        it.Next();
    }
    if (it.Valid()) {
        it.Prev();
        return it;
    } else {
        return zset_get_max(zk);
    }
}

// return { it, last }
// caller should just return if `it` is invalid
zsk_range DB::zset_range(context_t& con, zsk_info& zk, unsigned cmdops, score_range& r)
{
    auto it = zset_get_min(zk);
    auto last = zset_get_max(zk);
    double min_score = str2f(get_zset_score(it.key()));
    double max_score = str2f(get_zset_score(last.key()));
    if ((!r.lower && r.min > max_score) || (!r.upper && r.max < min_score)) {
        con.append(shared.n0);
        return { zsk_iterator(newErrorIterator(), 0), std::move(last) };
    }
    if (!r.lower && r.min > min_score) it = zset_lower_bound(zk, r.min);
    if (!r.upper && r.max < max_score) last = zset_upper_bound(zk, r.max);
    if (!r.lower && (cmdops & LOI)) {
        while (it.Valid() && str2f(get_zset_score(it.key())) == r.min)
            it.Next();
    }
    if (!r.upper && (cmdops & ROI)) {
        while (last.Valid() && str2f(get_zset_score(last.key())) == r.max)
            last.Prev();
    }
    return { std::move(it), std::move(last) };
}

// ZADD key score member [score member ...]
void DB::zadd(context_t& con)
{
    auto& key = con.argv[1];
    size_t size = con.argv.size();
    if (size % 2 != 0) ret(con, shared.argnumber_err);
    // 先检查参数中的所有score
    for (size_t i = 2; i < size; i += 2) {
        void(str2f(con.argv[i]));
        if (str2numerr()) ret(con, shared.float_err);
    }
    check_expire(key);
    touch_watch_key(key);
    std::string value;
    leveldb::WriteBatch batch;
    auto meta_key = encode_meta_key(key);
    auto s = db->Get(leveldb::ReadOptions(), meta_key, &value);
    if (s.IsNotFound()) { // create a new zset
        uint64_t seq = get_next_seq();
        add_zset_meta_info(&batch, meta_key, seq, (size - 2) / 2);
        for (size_t i = 2; i < size; i += 2) {
            auto& score = con.argv[i];
            auto& member = con.argv[i + 1];
            batch.Put(encode_zset_score(seq, score, member), member);
            batch.Put(encode_zset_member(seq, member), score);
        }
        s = db->Write(leveldb::WriteOptions(), &batch);
        check_status(con, s);
        con.append_reply_number((size - 2) / 2);
        return;
    }
    check_type(con, value, ktype::tzset);
    int adds = 0;
    auto zk = decode_zset_meta_value(value);
    for (size_t i = 2; i < size; i += 2) {
        auto& score = con.argv[i];
        auto& member = con.argv[i + 1];
        auto member_key = encode_zset_member(zk.seq, member);
        s = db->Get(leveldb::ReadOptions(), member_key, &value);
        if (!s.ok() && !s.IsNotFound()) reterr(con, s);
        if (s.ok()) { // 如果成员已存在，就更新它的分数
            batch.Delete(encode_zset_score(zk.seq, value, member));
            batch.Delete(member_key);
        } else {
            adds++;
        }
        batch.Put(encode_zset_score(zk.seq, score, member), member);
        batch.Put(member_key, score);
    }
    batch.Put(meta_key, encode_zset_meta_value(zk.seq, zk.size + adds));
    s = db->Write(leveldb::WriteOptions(), &batch);
    check_status(con, s);
    con.append_reply_number(adds);
}

 // ZSCORE key member
void DB::zscore(context_t& con)
{
    auto& key = con.argv[1];
    auto& member = con.argv[2];
    check_expire(key);
    std::string value;
    auto meta_key = encode_meta_key(key);
    auto s = db->Get(leveldb::ReadOptions(), meta_key, &value);
    if (s.IsNotFound()) ret(con, shared.nil);
    check_status(con, s);
    check_type(con, value, ktype::tzset);
    auto zk = decode_zset_meta_value(value);
    s = db->Get(leveldb::ReadOptions(), encode_zset_member(zk.seq, member), &value);
    if (s.IsNotFound()) ret(con, shared.nil);
    check_status(con, s);
    con.append_reply_string(value);
}

// ZINCRBY key increment member
void DB::zincrby(context_t& con)
{
    auto& key = con.argv[1];
    auto& score = con.argv[2];
    auto& member = con.argv[3];
    void(str2f(score));
    if (str2numerr()) ret(con, shared.float_err);
    check_expire(key);
    touch_watch_key(key);
    std::string value;
    leveldb::WriteBatch batch;
    auto meta_key = encode_meta_key(key);
    auto s = db->Get(leveldb::ReadOptions(), meta_key, &value);
    if (s.IsNotFound()) {
        uint64_t seq = get_next_seq();
        add_zset_meta_info(&batch, meta_key, seq, 1);
        batch.Put(encode_zset_score(seq, score, member), member);
        batch.Put(encode_zset_member(seq, member), score);
        s = db->Write(leveldb::WriteOptions(), &batch);
        check_status(con, s);
        con.append_reply_string(score);
        return;
    }
    check_status(con, s);
    check_type(con, value, ktype::tzset);
    auto zk = decode_zset_meta_value(value);
    s = db->Get(leveldb::ReadOptions(), encode_zset_member(zk.seq, member), &value);
    if (!s.ok() && !s.IsNotFound()) reterr(con, s);
    if (s.ok()) {
        batch.Delete(encode_zset_score(zk.seq, score, member));
        batch.Delete(encode_zset_member(zk.seq, member));
        score = d2s(str2f(value) + str2f(score));
    }
    batch.Put(encode_zset_score(zk.seq, score, member), member);
    batch.Put(encode_zset_member(zk.seq, member), score);
    con.append_reply_string(score);
}

// ZCARD key
void DB::zcard(context_t& con)
{
    auto& key = con.argv[1];
    check_expire(key);
    std::string value;
    auto s = db->Get(leveldb::ReadOptions(), encode_meta_key(key), &value);
    if (s.IsNotFound()) ret(con, shared.n0);
    check_status(con, s);
    check_type(con, value, ktype::tzset);
    auto zk = decode_zset_meta_value(value);
    con.append_reply_number(zk.size);
}

// ZCOUNT key min max
void DB::zcount(context_t& con)
{
    auto& key = con.argv[1];
    auto& min_str = con.argv[2];
    auto& max_str = con.argv[3];
    unsigned cmdops = 0;
    score_range r;
    if (parse_range_score(con, cmdops, r, min_str, max_str) == C_ERR)
        return;
    check_expire(key);
    std::string value;
    auto s = db->Get(leveldb::ReadOptions(), encode_meta_key(key), &value);
    if (s.IsNotFound()) ret(con, shared.n0);
    check_status(con, s);
    check_type(con, value, ktype::tzset);
    auto zk = decode_zset_meta_value(value);
    auto [it, last] = zset_range(con, zk, cmdops, r);
    if (!it.Valid()) return;
    con.append_reply_number(last.order - it.order + 1);
}

// Z(REV)RANGE key start stop [WITHSCORES]
void DB::_zrange(context_t& con, bool is_reverse)
{
    auto& key = con.argv[1];
    bool withscores = false;
    if (con.argv.size() > 4 ) {
        if (!con.isequal(4, "WITHSCORES")) ret(con, shared.syntax_err);
        withscores = true;
    }
    long long start = str2ll(con.argv[2]);
    if (str2numerr()) ret(con, shared.integer_err);
    long long stop = str2ll(con.argv[3]);
    if (str2numerr()) ret(con, shared.integer_err);
    check_expire(key);
    std::string value;
    auto meta_key = encode_meta_key(key);
    auto s = db->Get(leveldb::ReadOptions(), meta_key, &value);
    if (s.IsNotFound()) ret(con, shared.nil);
    check_status(con, s);
    check_type(con, value, ktype::tzset);
    auto zk = decode_zset_meta_value(value);
    long long upper = zk.size - 1;
    long long lower = -zk.size;
    if (check_range_index(con, start, stop, lower, upper) == C_ERR)
        return;
    if (withscores)
        con.append_reply_multi((stop - start + 1) * 2);
    else
        con.append_reply_multi(stop - start + 1);

    auto anchor = get_zset_anchor(zk.seq);
    auto it = newIterator();
    if (!is_reverse) {
        int i = 0;
        for (it->Seek(anchor), it->Next(); i < zk.size && it->Valid(); it->Next(), ++i) {
            if (i < start) continue;
            if (i > stop) break;
            con.append_reply_string(it->value().ToString());
            if (withscores)
                con.append_reply_string(get_zset_score(it->key()));
        }
    } else {
        auto anchor = get_zset_end_anchor(zk.seq);
        if (is_reverse) {
            int i = 0;
            for (it->Seek(anchor), it->Prev(); i < zk.size && it->Valid(); it->Prev(), ++i) {
                if (i < start) continue;
                if (i > stop) break;
                con.append_reply_string(it->value().ToString());
                if (withscores)
                    con.append_reply_string(get_zset_score(it->key()));
            }
        }
    }
}

void DB::zrange(context_t& con)
{
    _zrange(con, false);
}

void DB::zrevrange(context_t& con)
{
    _zrange(con, true);
}

// Z(REV)RANK key member
void DB::_zrank(context_t& con, bool is_reverse)
{
    auto& key = con.argv[1];
    auto& member = con.argv[2];
    check_expire(key);
    std::string value;
    auto meta_key = encode_meta_key(key);
    auto s = db->Get(leveldb::ReadOptions(), meta_key, &value);
    if (s.IsNotFound()) ret(con, shared.nil);
    check_status(con, s);
    check_type(con, value, ktype::tzset);
    auto zk = decode_zset_meta_value(value);
    s = db->Get(leveldb::ReadOptions(), encode_zset_member(zk.seq, member), &value);
    if (s.IsNotFound()) ret(con, shared.nil);
    // find score(value)'s rank(O(n))
    auto it = newIterator();
    int rank = 0;
    if (!is_reverse) {
        auto anchor = get_zset_anchor(zk.seq);
        for (it->Seek(anchor), it->Next(); rank < zk.size && it->Valid(); it->Next(), ++rank) {
            if (get_zset_score(it->key()) == value) {
                break;
            }
        }
    } else {
        auto anchor = get_zset_end_anchor(zk.seq);
        for (it->Seek(anchor), it->Prev(); rank < zk.size && it->Valid(); it->Prev(), ++rank) {
            if (get_zset_score(it->key()) == value) {
                break;
            }
        }
    }
    con.append_reply_number(rank);
}

void DB::zrank(context_t& con)
{
    _zrank(con, false);
}

void DB::zrevrank(context_t& con)
{
    _zrank(con, true);
}

// Z(REV)RANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
void DB::_zrangebyscore(context_t& con, bool is_reverse)
{
    unsigned cmdops = 0;
    long long offset = 0, limit = 0;
    if (parse_zrangebyscore_args(con, cmdops, offset, limit) == C_ERR)
        return;
    auto& key = con.argv[1];
    auto& min_str = con.argv[2];
    auto& max_str = con.argv[3];
    score_range r;
    if (parse_range_score(con, cmdops, r, min_str, max_str) == C_ERR)
        return;
    check_expire(key);
    auto meta_key = encode_meta_key(key);
    std::string value;
    auto s = db->Get(leveldb::ReadOptions(), meta_key, &value);
    if (s.IsNotFound()) ret(con, shared.nil);
    check_status(con, s);
    check_type(con, value, ktype::tzset);

    auto zk = decode_zset_meta_value(value);
    auto [it, last] = zset_range(con, zk, cmdops, r);
    if (!it.Valid()) return;
    long long dis = last.order - it.order + 1;
    if (!is_reverse) {
        _zrangefor(con, cmdops, it, dis, offset, limit, is_reverse);
    } else {
        _zrangefor(con, cmdops, last, dis, offset, limit, is_reverse);
    }
}

void DB::_zrangefor(context_t& con, unsigned cmdops, zsk_iterator& it,
                    long long dis, long long offset, long long limit, bool is_reverse)
{
    if (cmdops & LIMIT) {
        if (offset >= dis) ret(con, shared.multi_empty);
        limit = std::min(limit, dis - offset);
    } else {
        limit = dis;
    }
    while (offset-- > 0 && it.Valid()) {
        is_reverse ? it.Prev() : it.Next();
    }
    bool is_limit = limit > 0;
    bool withscores = (cmdops & WITHSCORES);
    con.append_reply_multi(withscores ? limit * 2 : limit);
    if (!is_reverse) {
        for ( ; it.Valid(); it.Next()) {
            con.append_reply_string(it.value().ToString());
            if (withscores)
                con.append_reply_string(get_zset_score(it.key()));
            if (is_limit && --limit == 0)
                break;
        }
    } else {
        for ( ; it.Valid(); it.Prev()) {
            con.append_reply_string(it.value().ToString());
            if (withscores)
                con.append_reply_string(get_zset_score(it.key()));
            if (is_limit && --limit == 0)
                break;
        }
    }
}

void DB::zrangebyscore(context_t& con)
{
    _zrangebyscore(con, false);
}

void DB::zrevrangebyscore(context_t& con)
{
    _zrangebyscore(con, true);
}

// ZREM key member [member ...]
void DB::zrem(context_t& con)
{
    auto& key = con.argv[1];
    check_expire(key);
    auto meta_key = encode_meta_key(key);
    std::string value;
    auto s = db->Get(leveldb::ReadOptions(), meta_key, &value);
    if (s.IsNotFound()) ret(con, shared.n0);
    check_status(con, s);
    check_type(con, value, ktype::tzset);
    auto zk = decode_zset_meta_value(value);
    size_t rems = 0;
    size_t size = con.argv.size();
    leveldb::WriteBatch batch;
    for (size_t i = 2; i < size; i++) {
        auto& member = con.argv[i];
        auto member_key = encode_zset_member(zk.seq, member);
        s = db->Get(leveldb::ReadOptions(), member_key, &value);
        if (s.ok()) {
            batch.Delete(member_key);
            batch.Delete(encode_zset_score(zk.seq, value, member));
            rems++;
        }
    }
    if (rems == zk.size) {
        del_zset_meta_info(&batch, meta_key, zk.seq);
    } else {
        assert(zk.size > rems);
        batch.Put(meta_key, encode_zset_meta_value(zk.seq, zk.size - rems));
    }
    s = db->Write(leveldb::WriteOptions(), &batch);
    check_status(con, s);
    touch_watch_key(key);
    con.append_reply_number(rems);
}

// ZREMRANGEBYRANK key start stop
void DB::zremrangebyrank(context_t& con)
{
    auto& key = con.argv[1];
    long long start = str2ll(con.argv[2]);
    if (str2numerr()) ret(con, shared.integer_err);
    long long stop = str2ll(con.argv[3]);
    if (str2numerr()) ret(con, shared.integer_err);
    check_expire(key);
    auto meta_key = encode_meta_key(key);
    std::string value;
    auto s = db->Get(leveldb::ReadOptions(), meta_key, &value);
    if (s.IsNotFound()) ret(con, shared.n0);
    check_status(con, s);
    check_type(con, value, ktype::tzset);
    auto zk = decode_zset_meta_value(value);
    long long upper = zk.size - 1;
    long long lower = -zk.size;
    if (check_range_index(con, start, stop, lower, upper) == C_ERR)
        return;
    long long i = 0, rems = 0;
    leveldb::WriteBatch batch;
    auto anchor = get_zset_anchor(zk.seq);
    auto it = newIterator();
    for (it->Seek(anchor), it->Next(); i < zk.size && it->Valid(); it->Next(), ++i) {
        if (i < start) continue;
        if (i > stop) break;
        batch.Delete(it->key());
        batch.Delete(encode_zset_member(zk.seq, it->value().ToString()));
        rems++;
    }
    if (rems == zk.size) {
        del_zset_meta_info(&batch, meta_key, zk.seq);
    } else {
        assert(zk.size > rems);
        batch.Put(meta_key, encode_zset_meta_value(zk.seq, zk.size - rems));
    }
    s = db->Write(leveldb::WriteOptions(), &batch);
    check_status(con, s);
    touch_watch_key(key);
    con.append_reply_number(rems);
}

// ZREMRANGEBYSCORE key min max
void DB::zremrangebyscore(context_t& con)
{
    unsigned cmdops = 0;
    auto& key = con.argv[1];
    auto& min_str = con.argv[2];
    auto& max_str = con.argv[3];
    score_range r;
    if (parse_range_score(con, cmdops, r, min_str, max_str) == C_ERR)
        return;
    check_expire(key);
    auto meta_key = encode_meta_key(key);
    std::string value;
    auto s = db->Get(leveldb::ReadOptions(), meta_key, &value);
    if (s.IsNotFound()) ret(con, shared.n0);
    check_status(con, s);
    check_type(con, value, ktype::tzset);
    auto zk = decode_zset_meta_value(value);
    auto [it, last] = zset_range(con, zk, cmdops, r);
    if (!it.Valid()) return;
    long long rems = 0;
    leveldb::WriteBatch batch;
    while (it.Valid() && it.order <= last.order) {
        batch.Delete(it.key());
        batch.Delete(encode_zset_member(zk.seq, it.value().ToString()));
        it.Next();
        rems++;
    }
    if (rems == zk.size) {
        del_zset_meta_info(&batch, meta_key, zk.seq);
    } else {
        assert(zk.size > rems);
        batch.Put(meta_key, encode_zset_meta_value(zk.seq, zk.size - rems));
    }
    s = db->Write(leveldb::WriteOptions(), &batch);
    check_status(con, s);
    con.append_reply_number(rems);
}

errstr_t DB::del_zset_key(const key_t& key)
{
    leveldb::WriteBatch batch;
    auto err = del_zset_key_batch(&batch, key);
    if (err) return err;
    auto s = db->Write(leveldb::WriteOptions(), &batch);
    if (!s.ok()) return s;
    return std::nullopt;
}

errstr_t DB::del_zset_key_batch(leveldb::WriteBatch *batch, const key_t& key)
{
    std::string value;
    auto meta_key = encode_meta_key(key);
    auto s = db->Get(leveldb::ReadOptions(), meta_key, &value);
    if (s.IsNotFound()) return std::nullopt;
    if (!s.ok()) return s;
    auto zk = decode_zset_meta_value(value);
    auto anchor = get_zset_anchor(zk.seq);
    auto it = newIterator();
    for (it->Seek(anchor), it->Next(); it->Valid(); it->Next()) {
        batch->Delete(it->key());
        batch->Delete(encode_zset_member(zk.seq, it->value().ToString()));
        if (--zk.size == 0)
            break;
    }
    assert(zk.size == 0);
    del_zset_meta_info(batch, meta_key, zk.seq);
    return std::nullopt;
}

void DB::rename_zset_key(leveldb::WriteBatch *batch, const key_t& key,
                         const std::string& meta_value, const key_t& newkey)
{
    auto zk = decode_zset_meta_value(meta_value);
    uint64_t newseq = get_next_seq();
    long long newsize = zk.size;
    auto anchor = get_zset_anchor(zk.seq);
    auto it = newIterator();
    for (it->Seek(anchor), it->Next(); it->Valid(); it->Next()) {
        auto score = get_zset_score(it->key());
        auto member = it->value().ToString();
        batch->Put(encode_zset_score(newseq, score, member), it->value());
        batch->Put(encode_zset_member(newseq, member), score);
        batch->Delete(it->key());
        batch->Delete(encode_zset_member(zk.seq, member));
        if (--zk.size == 0)
            break;
    }
    assert(zk.size == 0);
    add_zset_meta_info(batch, encode_meta_key(newkey), newseq, newsize);
    del_zset_meta_info(batch, encode_meta_key(key), zk.seq);
}

int keycomp::zset_compare(const leveldb::Slice& l, const leveldb::Slice& r) const
{
    uint64_t seq1, seq2;
    if (l[l.size() - 1] != zset_score_key || r[r.size() - 1] != zset_score_key)
        return l.compare(r);
    auto begin1 = const_cast<char*>(l.data()) + 1;
    auto begin2 = const_cast<char*>(r.data()) + 1;
    auto s1 = begin1 + load_len(begin1, &seq1);
    auto s2 = begin2 + load_len(begin2, &seq2);
    if (seq1 != seq2) return seq1 - seq2;
    auto score1 = atof(s1), score2 = atof(s2);
    if (score1 < score2) return -1;
    if (score1 > score2) return 1;
    s1 = strchr(s1, ':') + 1;
    s2 = strchr(s2, ':') + 1;
    leveldb::Slice member1(s1, begin1 - 1 + l.size() - 1 - s1);
    leveldb::Slice member2(s2, begin2 - 1 + r.size() - 1 - s2);
    return member1.compare(member2);
}

}
}
