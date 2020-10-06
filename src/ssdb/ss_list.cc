#include "internal.h"

#include "../server.h"

using namespace alice;
using namespace alice::ssdb;

// LPUSH key value [value ...]
void DB::lpush(context_t& con)
{
    auto& key = con.argv[1];
    std::string value;
    leveldb::WriteBatch batch;
    int li = 0, ri = 1, size = 0;
    auto meta_key = encode_meta_key(key);
    check_expire(meta_key);
    auto s = db->Get(leveldb::ReadOptions(), meta_key, &value);
    if (!s.IsNotFound() && !s.ok()) reterr(con, s);
    if (s.ok()) {
        check_type(con, value, ktype::tlist);
        decode_list_meta_value(value, li, ri, size);
        li--;
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
    }

    for (int i = 2; i < con.argv.size(); i++) {
        batch.Put(encode_list_key(key, ri++), con.argv[i]);
    }
    size += con.argv.size() - 2;
    batch.Put(meta_key, encode_list_meta_value(li, ri, size));

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
    batch.Put(encode_list_key(key, is_lpushx ? --li : ri++), con.argv[2]);
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
    // find poped-key
    while (true) {
        value.clear();
        auto enc_key = encode_list_key(key, is_lpop ? li++ : --ri);
        s = db->Get(leveldb::ReadOptions(), enc_key, &value);
        if (s.IsNotFound()) {
            if (li == ri) {
                s = db->Delete(leveldb::WriteOptions(), meta_key);
                check_status(con, s);
                ret(con, shared.nil);
            }
        } else {
            check_status(con, s);
            batch.Delete(enc_key);
            break;
        }
    }
    if (li == ri) { // the last one
        batch.Delete(meta_key);
    } else {
        batch.Put(meta_key, encode_list_meta_value(li, ri, --size));
    }
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
    int nums = 0;
    int ranges = stop - start + 1;
    con.reserve_multi_head();
    for (li += start; ; li++) {
        value.clear();
        s = db->Get(leveldb::ReadOptions(), encode_list_key(key, li), &value);
        if (s.IsNotFound()) continue;
        if (s.ok()) {
            con.append_reply_string(value);
            nums++;
        } else
            adderr(con, s);
        if (--ranges == 0)
            break;
    }
    con.set_multi_head(nums);
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
    auto enc_key = encode_list_key(src_key, --ri);
    s = db->Get(leveldb::ReadOptions(), enc_key, &src_value);
    check_status(con, s);
    if (li == ri) {
        batch.Delete(src_key);
    } else {
        batch.Put(src_meta_key, encode_list_meta_value(li, ri, --size));
    }
    batch.Delete(enc_key);
    // put src_value to des_key
    li = 0;
    ri = 1;
    size = 0;
    s = db->Get(leveldb::ReadOptions(), des_meta_key, &des_value);
    if (!s.IsNotFound() && !s.ok()) reterr(con, s);
    if (s.ok()) {
        check_type(con, des_value, ktype::tlist);
        decode_list_meta_value(des_value, li, ri, size);
        li--;
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
    // 因为可能会在中间删除元素，所以需要记录边界用以更新索引范围
    int left; // 最左边的未被删除的元素索引
    int right; // 最右边的未被删除的元素索引
    bool update = false;
    if (count > 0) {
        for ( ; li < ri; li++) {
            value.clear();
            auto enc_key = encode_list_key(key, li);
            s = db->Get(leveldb::ReadOptions(), enc_key, &value);
            if (s.IsNotFound()) continue;
            if (!s.ok()) adderr(con, s);
            if (value == con.argv[3]) {
                batch.Delete(enc_key);
                rems++;
                if (--count == 0) {
                    if (!update) { // 恰好删除了最左边的几个元素
                        left = li + 1;
                        right = ri;
                    }
                    break;
                }
            } else {
                if (!update) {
                    update = true;
                    left = li;
                }
                right = li + 1;
            }
        }
    } else if (count < 0) {
        for (ri--; ri >= li; ri--) {
            value.clear();
            auto enc_key = encode_list_key(key, ri);
            s = db->Get(leveldb::ReadOptions(), enc_key, &value);
            if (s.IsNotFound()) continue;
            if (!s.ok()) adderr(con, s);
            if (value == con.argv[3]) {
                batch.Delete(enc_key);
                rems++;
                if (++count == 0) {
                    if (!update) { // 恰好删除了最右边的几个元素
                        right = ri + 1;
                        left = li;
                    }
                    break;
                }
            } else {
                if (!update) {
                    update = true;
                    right = ri + 1;
                }
                left = ri;
            }
        }
    } else {
        for ( ; li < ri; li++) {
            value.clear();
            auto enc_key = encode_list_key(key, li);
            s = db->Get(leveldb::ReadOptions(), enc_key, &value);
            if (s.IsNotFound()) continue;
            if (!s.ok()) adderr(con, s);
            if (value == con.argv[3]) {
                batch.Delete(enc_key);
                rems++;
            } else {
                if (!update) {
                    update = true;
                    left = li;
                }
                right = li + 1;
            }
        }
    }
    if (rems == size) {
        batch.Delete(meta_key);
    } else {
        batch.Put(meta_key, encode_list_meta_value(left, right, size-rems));
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
    if (index >= size)
        ret(con, shared.nil);
    if (size == li - ri) {
        value.clear();
        auto enc_key = encode_list_key(key, index);
        s = db->Get(leveldb::ReadOptions(), enc_key, &value);
        check_status(con, s);
        con.append_reply_string(value);
        return;
    }
    for ( ; ; ) {
        value.clear();
        auto enc_key = encode_list_key(key, li++);
        s = db->Get(leveldb::ReadOptions(), enc_key, &value);
        if (s.IsNotFound()) continue;
        check_status(con, s);
        if (--index == 0) { // found
            con.append_reply_string(value);
            break;
        }
    }
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
    decode_list_meta_value(value, li, ri, size);
    if (index < 0)
        index += size;
    if (index >= size)
        ret(con, shared.index_out_of_range);
    std::string enc_key;
    if (size == li - ri) {
        enc_key = encode_list_key(key, index);
    } else {
        for ( ; ; ) {
            value.clear();
            enc_key = encode_list_key(key, li++);
            s = db->Get(leveldb::ReadOptions(), enc_key, &value);
            if (s.IsNotFound()) continue;
            check_status(con, s);
            if (--index == 0) { // found
                break;
            }
        }
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
    int left, right;
    leveldb::WriteBatch batch;
    for ( ; li < ri; li++) {
        value.clear();
        auto enc_key = encode_list_key(key, li);
        s = db->Get(leveldb::ReadOptions(), enc_key, &value);
        if (s.IsNotFound()) continue;
        check_status(con, s);
        if (i == start) left = li;
        if (i == stop) right = li + 1;
        if (i >= start && i <= stop) continue;
        batch.Delete(enc_key);
        i++;
    }
    size -= stop - start;
    batch.Put(meta_key, encode_list_meta_value(left, right, size));
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
    for ( ; li < ri; li++) {
        batch->Delete(encode_list_key(key, li));
    }
    batch->Delete(meta_key);
    return std::nullopt;
}
