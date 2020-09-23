#include "ssdb.h"

#include "../server.h"

using namespace alice;
using namespace alice::ssdb;

#define enc_list_meta_value(value, li, ri) \
    (value).assign(i2s(LIST)).append(":").append(i2s(li)).append(":").append(i2s(ri))

#define enc_list_key(enc_key, key, number) \
    (enc_key).assign(key).append(":").append(i2s(number)).append("$")

static int get_li_by_value(const std::string& value)
{
    return atoi(strchr(value.c_str(), ':') + 1);
}

static int get_ri_by_value(const std::string& value)
{
    return atoi(strrchr(value.c_str(), ':') + 1);
}

// LPUSH key value [value ...]
void DB::lpush(context_t& con)
{
    auto& key = con.argv[1];
    std::string enc_key, value;
    leveldb::WriteBatch batch;
    int li = 0, ri = 1;
    auto meta_key = get_meta_key(key);
    auto status = db->Get(leveldb::ReadOptions(), meta_key, &value);
    if (status.ok()) {
        li = get_li_by_value(value) - 1;
        ri = get_ri_by_value(value);
    }
    for (int i = 2; i < con.argv.size(); i++) {
        enc_list_key(enc_key, key, li--);
        batch.Put(enc_key, con.argv[i]);
    }
    enc_list_meta_value(value, li+1, ri);
    batch.Put(meta_key, value);

    status = db->Write(leveldb::WriteOptions(), &batch);
    assert(status.ok());
    con.append_reply_number(ri-li-1);
}

// LPUSH key value [value ...]
void DB::rpush(context_t& con)
{
    auto& key = con.argv[1];
    std::string enc_key, value;
    leveldb::WriteBatch batch;
    int li = 0, ri = 0;
    auto meta_key = get_meta_key(key);
    auto status = db->Get(leveldb::ReadOptions(), meta_key, &value);
    if (status.ok()) {
        li = get_li_by_value(value);
        ri = get_ri_by_value(value);
    }
    for (int i = 2; i < con.argv.size(); i++) {
        enc_list_key(enc_key, key, ri++);
        batch.Put(enc_key, con.argv[i]);
    }
    enc_list_meta_value(value, li, ri);
    batch.Put(meta_key, value);

    status = db->Write(leveldb::WriteOptions(), &batch);
    assert(status.ok());
    con.append_reply_number(ri-li);
}

// LPUSHX key value
void DB::lpushx(context_t& con)
{
    auto& key = con.argv[1];
    std::string enc_key, value;
    auto meta_key = get_meta_key(key);
    auto status = db->Get(leveldb::ReadOptions(), meta_key, &value);
    if (status.IsNotFound()) ret(con, shared.n0);
    leveldb::WriteBatch batch;
    int li = get_li_by_value(value) - 1;
    int ri = get_ri_by_value(value);
    enc_list_key(enc_key, key, li);
    batch.Put(enc_key, con.argv[2]);
    enc_list_meta_value(value, li, ri);
    batch.Put(meta_key, value);

    status = db->Write(leveldb::WriteOptions(), &batch);
    assert(status.ok());
    con.append_reply_number(ri-li);
}

// LPUSHX key value
void DB::rpushx(context_t& con)
{
    auto& key = con.argv[1];
    std::string enc_key, value;
    auto meta_key = get_meta_key(key);
    auto status = db->Get(leveldb::ReadOptions(), meta_key, &value);
    if (status.IsNotFound()) ret(con, shared.n0);
    leveldb::WriteBatch batch;
    int li = get_li_by_value(value);
    int ri = get_ri_by_value(value);
    enc_list_key(enc_key, key, ri++);
    batch.Put(enc_key, con.argv[2]);
    enc_list_meta_value(value, li, ri);
    batch.Put(meta_key, value);

    status = db->Write(leveldb::WriteOptions(), &batch);
    assert(status.ok());
    con.append_reply_number(ri-li);
}

// LPOP key
void DB::lpop(context_t& con)
{
    auto& key = con.argv[1];
    std::string value;
    auto meta_key = get_meta_key(key);
    auto status = db->Get(leveldb::ReadOptions(), meta_key, &value);
    if (status.IsNotFound()) ret(con, shared.nil);
    int li = get_li_by_value(value);
    int ri = get_ri_by_value(value);
    std::string enc_key;
    enc_list_key(enc_key, key, li++);
    value.clear();
    status = db->Get(leveldb::ReadOptions(), enc_key, &value);
    assert(status.ok());
    con.append_reply_string(value);
    leveldb::WriteBatch batch;
    if (li == ri) { // the last one
        batch.Delete(meta_key);
    } else {
        enc_list_meta_value(value, li, ri);
        batch.Put(meta_key, value);
    }
    batch.Delete(enc_key);
    status = db->Write(leveldb::WriteOptions(), &batch);
    assert(status.ok());
}

// RPOP key
void DB::rpop(context_t& con)
{
    auto& key = con.argv[1];
    std::string value;
    auto meta_key = get_meta_key(key);
    auto status = db->Get(leveldb::ReadOptions(), meta_key, &value);
    if (status.IsNotFound()) ret(con, shared.nil);
    int li = get_li_by_value(value);
    int ri = get_ri_by_value(value);
    std::string enc_key;
    enc_list_key(enc_key, key, --ri);
    value.clear();
    status = db->Get(leveldb::ReadOptions(), enc_key, &value);
    assert(status.ok());
    con.append_reply_string(value);
    leveldb::WriteBatch batch;
    if (li == ri) {
        batch.Delete(meta_key);
    } else {
        enc_list_meta_value(value, li, ri);
        batch.Put(meta_key, value);
    }
    batch.Delete(enc_key);
    status = db->Write(leveldb::WriteOptions(), &batch);
    assert(status.ok());
}

// LLEN key
void DB::llen(context_t& con)
{
    auto& key = con.argv[1];
    std::string value;
    auto meta_key = get_meta_key(key);
    auto status = db->Get(leveldb::ReadOptions(), meta_key, &value);
    if (status.IsNotFound()) ret(con, shared.n0);
    int li = get_li_by_value(value);
    int ri = get_ri_by_value(value);
    con.append_reply_number(ri-li);
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
    auto meta_key = get_meta_key(key);
    auto status = db->Get(leveldb::ReadOptions(), meta_key, &value);
    if (status.IsNotFound()) ret(con, shared.nil);
    int li = get_li_by_value(value);
    int ri = get_ri_by_value(value);
    int len = ri - li;
    int upper = len - 1;
    int lower = -len;
    if (dbserver::check_range(con, start, stop, lower, upper) == C_ERR)
        return;
    int i = 0;
    std::string enc_key;
    con.append_reply_multi(stop-start+1);
    for ( ; li < ri; li++) {
        if (i < start) {
            i++;
            continue;
        }
        if (i > stop)
            break;
        enc_list_key(enc_key, key, li);
        value.clear();
        status = db->Get(leveldb::ReadOptions(), enc_key, &value);
        assert(status.ok());
        con.append_reply_string(value);
        i++;
    }
}

// RPOPLPUSH source destination
void DB::rpoplpush(context_t& con)
{
    auto& src_key = con.argv[1];
    auto& des_key = con.argv[2];
    auto src_meta_key = get_meta_key(src_key);
    std::string enc_key, value, src_value, des_value;
    auto status = db->Get(leveldb::ReadOptions(), src_meta_key, &src_value);
    if (status.IsNotFound()) ret(con, shared.nil);
    assert(status.ok());
    leveldb::WriteBatch batch;
    // get src_value
    int li = get_li_by_value(src_value);
    int ri = get_ri_by_value(src_value);
    enc_list_key(enc_key, src_key, --ri);
    src_value.clear();
    status = db->Get(leveldb::ReadOptions(), enc_key, &src_value);
    assert(status.ok());
    if (li == ri) {
        batch.Delete(src_key);
    } else {
        enc_list_meta_value(value, li, ri);
        batch.Put(src_meta_key, value);
    }
    batch.Delete(enc_key);
    // put src_value to des_key
    li = 0;
    ri = 1;
    auto des_meta_key = get_meta_key(des_key);
    status = db->Get(leveldb::ReadOptions(), des_meta_key, &des_value);
    if (status.ok()) {
        li = get_li_by_value(des_value) - 1;
        ri = get_ri_by_value(des_value);
    }
    enc_list_key(enc_key, des_key, li);
    batch.Put(enc_key, src_value);
    enc_list_meta_value(value, li, ri);
    batch.Put(des_meta_key, value);

    status = db->Write(leveldb::WriteOptions(), &batch);
    assert(status.ok());
    con.append_reply_string(src_value);
}

void DB::del_list_key(const std::string& key)
{
    std::string value;
    auto meta_key = get_meta_key(key);
    auto status = db->Get(leveldb::ReadOptions(), meta_key, &value);
    if (status.IsNotFound()) return;
    int li = get_li_by_value(value);
    int ri = get_ri_by_value(value);
    std::string enc_key;
    leveldb::WriteBatch batch;
    for ( ; li < ri; li++) {
        enc_list_key(enc_key, key, li);
        batch.Delete(enc_key);
    }
    batch.Delete(meta_key);
    status = db->Write(leveldb::WriteOptions(), &batch);
    assert(status.ok());
}
