#include "internal.h"

#include "../server.h"

using namespace alice;
using namespace alice::ssdb;

static inline int get_list_index(leveldb::Slice&& key)
{
    return atoi(strrchr(key.data(), ':')+1);
}

static inline bool is_no_hole(int li, int ri, int size)
{
    return ri - li + 1 == size;
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
    con.append_reply_number(size);
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
    con.append_reply_number(size);
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
    con.append_reply_number(size);
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
    if (size == 1) { // last one
        batch.Delete(meta_key);
    } else {
        if (is_no_hole(li, ri, size)) {
            is_lpop ? ++li : --ri;
        } else {
            auto it = db->NewIterator(leveldb::ReadOptions());
            it->Seek(enc_key);
            assert(it->Valid());
            is_lpop ? it->Next() : it->Prev();
            assert(it->Valid());
            if (is_lpop) li = get_list_index(it->key());
            else ri = get_list_index(it->key());
        }
        batch.Put(meta_key, encode_list_meta_value(li, ri, --size));
    }
    batch.Delete(enc_key);

    s = db->Write(leveldb::WriteOptions(), &batch);
    check_status(con, s);
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
    if (dbserver::check_range(con, start, stop, lower, upper) == C_ERR)
        return;
    int ranges = stop - start + 1;
    con.append_reply_multi(ranges);
    auto it = db->NewIterator(leveldb::ReadOptions());
    for (it->Seek(encode_list_key(key, li+start)); it->Valid(); it->Next()) {
        con.append_reply_string(it->value().ToString());
        if (--ranges == 0)
            break;
    }
}

// RPOPLPUSH source destination
void DB::rpoplpush(context_t& con)
{
    auto& src_key = con.argv[1];
    auto& des_key = con.argv[2];
    auto src_meta_key = encode_meta_key(src_key);
    auto des_meta_key = encode_meta_key(des_key);
    check_expire(src_meta_key);
    check_expire(des_meta_key);
    std::string src_value, des_value;
    auto s = db->Get(leveldb::ReadOptions(), src_meta_key, &src_value);
    if (s.IsNotFound()) ret(con, shared.nil);
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
    batch.Delete(enc_key);
    if (size == 1) { // last one
        batch.Delete(src_meta_key);
    } else {
        if (is_no_hole(li, ri, size)) {
            --ri;
        } else {
            auto it = db->NewIterator(leveldb::ReadOptions());
            it->Seek(enc_key);
            assert(it->Valid());
            it->Prev();
            assert(it->Valid());
            ri = get_list_index(it->key());
        }
        batch.Put(src_meta_key, encode_list_meta_value(li, ri, --size));
    }
    // put src_value to des_key
    if (src_key == des_key) {
        --li;
    } else {
        li = ri = size = 0;
        s = db->Get(leveldb::ReadOptions(), des_meta_key, &des_value);
        if (!s.IsNotFound() && !s.ok()) reterr(con, s);
        if (s.ok()) {
            check_type(con, des_value, ktype::tlist);
            decode_list_meta_value(des_value, li, ri, size);
            --li;
        }
    }
    batch.Put(encode_list_key(des_key, li), src_value);
    batch.Put(des_meta_key, encode_list_meta_value(li, ri, ++size));

    s = db->Write(leveldb::WriteOptions(), &batch);
    check_status(con, s);
    con.append_reply_string(src_value);
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
    auto it = db->NewIterator(leveldb::ReadOptions());
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
    // touch_watch_key(key);
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
        auto enc_key = encode_list_key(key, index);
        s = db->Get(leveldb::ReadOptions(), enc_key, &value);
        check_status(con, s);
        con.append_reply_string(value);
        return;
    }
    auto it = db->NewIterator(leveldb::ReadOptions());
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
        auto it = db->NewIterator(leveldb::ReadOptions());
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
    auto it = db->NewIterator(leveldb::ReadOptions());
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
    // touch_watch_key(key);
    con.append(shared.ok);
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
    auto it = db->NewIterator(leveldb::ReadOptions());
    for (it->Seek(encode_list_key(key, li)); it->Valid(); it->Next()) {
        batch->Delete(it->key());
        if (--size == 0)
            break;
    }
    assert(size == 0);
    batch->Delete(meta_key);
    return std::nullopt;
}
