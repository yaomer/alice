#include "ssdb.h"

#include "../server.h"

using namespace alice::ssdb;

#define BIND(f) std::bind(&DB::f, db.get(), std::placeholders::_1)

namespace alice {
    namespace ssdb {
        builtin_keys_t builtin_keys;
    }
}

engine::engine()
    : db(new DB())
{
    cmdtable = {
        { "KEYS",       { -2, IS_READ,  BIND(keys) } },
        { "DEL",        {  2, IS_WRITE, BIND(del) } },
        { "LPUSH",      {  3, IS_WRITE, BIND(lpush) } },
        { "LPUSHX",     { -3, IS_WRITE, BIND(lpushx) } },
        { "RPUSH",      {  3, IS_WRITE, BIND(rpush) } },
        { "RPUSHX",     { -3, IS_WRITE, BIND(rpushx) } },
        { "LPOP",       { -2, IS_WRITE, BIND(lpop) } },
        { "RPOP",       { -2, IS_WRITE, BIND(rpop) } },
        { "RPOPLPUSH",  { -3, IS_WRITE, BIND(rpoplpush) } },
        { "LREM",       { -4, IS_WRITE, BIND(lrem) } },
        { "LLEN",       { -2, IS_READ,  BIND(llen) } },
        { "LINDEX",     { -3, IS_READ,  BIND(lindex) } },
        { "LSET",       { -4, IS_WRITE, BIND(lset) } },
        { "LRANGE",     { -4, IS_READ,  BIND(lrange) } },
        { "LTRIM",      { -4, IS_WRITE, BIND(ltrim) } },
        // { "BLPOP",      {  3, IS_READ,  BIND(blpop) } },
        // { "BRPOP",      {  3, IS_READ,  BIND(brpop) } },
        // { "BRPOPLPUSH", { -4, IS_READ,  BIND(brpoplpush) } },
    };
}

void DB::set_builtin_keys()
{
    std::string value;
    auto s = db->Get(leveldb::ReadOptions(), builtin_keys.location, &value);
    if (s.IsNotFound()) {
        s = db->Put(leveldb::WriteOptions(), builtin_keys.location, builtin_keys.location);
        assert(s.ok());
    }
    s = db->Get(leveldb::ReadOptions(), builtin_keys.size, &value);
    if (s.IsNotFound()) {
        s = db->Put(leveldb::WriteOptions(), builtin_keys.size, "0");
        assert(s.ok());
    }
}

void DB::keys(context_t& con)
{
    if (con.argv[1].compare("*"))
        ret(con, shared.unknown_option);
    int nums = 0;
    con.reserve_multi_head();
    auto *it = db->NewIterator(leveldb::ReadOptions());
    it->Seek(builtin_keys.location);
    assert(it->Valid());
    for (it->Next(); it->Valid(); it->Next()) {
        auto key = it->key();
        if (key[0] != '@') break;
        key.remove_prefix(1); // remove prefix '@'
        con.append_reply_string(key.ToString());
        nums++;
    }
    con.set_multi_head(nums);
}

// DEL key [key ...]
void DB::del(context_t& con)
{
    int dels = 0;
    for (size_t i = 1; i < con.argv.size(); i++) {
        del_key_with_expire(con.argv[i]);
        dels++;
    }
    con.append_reply_number(dels);
}

void DB::check_expire(const key_t& key)
{
    auto it = expire_keys.find(key);
    if (it == expire_keys.end()) return;
    if (it->second > lru_clock) return;
    del_key_with_expire(key);
    argv_t argv = { "DEL", key };
    __server->append_write_command(argv, nullptr, 0);
}

void DB::del_key(const key_t& key)
{
    std::string value;
    auto meta_key = encode_meta_key(key);
    auto s = db->Get(leveldb::ReadOptions(), meta_key, &value);
    if (s.IsNotFound()) return;
    assert(s.ok());
    auto type = get_type(value);
    switch (type) {
    case ktype::tstring: del_string_key(key); break;
    case ktype::tlist: del_list_key(key); break;
    case ktype::thash: del_hash_key(key); break;
    case ktype::tset: del_set_key(key); break;
    case ktype::tzset: del_zset_key(key); break;
    }
}
