#include "ssdb.h"

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
        { "LPUSH",      {  3, IS_WRITE, BIND(lpush) } },
        { "LPUSHX",     { -3, IS_WRITE, BIND(lpushx) } },
        { "RPUSH",      {  3, IS_WRITE, BIND(rpush) } },
        { "RPUSHX",     { -3, IS_WRITE, BIND(rpushx) } },
        { "LPOP",       { -2, IS_WRITE, BIND(lpop) } },
        { "RPOP",       { -2, IS_WRITE, BIND(rpop) } },
        { "RPOPLPUSH",  { -3, IS_WRITE, BIND(rpoplpush) } },
        // { "LREM",       { -4, IS_WRITE, BIND(lrem) } },
        { "LLEN",       { -2, IS_READ,  BIND(llen) } },
        // { "LINDEX",     { -3, IS_READ,  BIND(lindex) } },
        // { "LSET",       { -4, IS_WRITE, BIND(lset) } },
        { "LRANGE",     { -4, IS_READ,  BIND(lrange) } },
        // { "LTRIM",      { -4, IS_WRITE, BIND(ltrim) } },
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
}

void DB::keys(context_t& con)
{
    if (con.argv[1].compare("*"))
        ret(con, shared.unknown_option);
    int size = 0;
    std::string buffer;
    buffer.swap(con.buf);
    auto *it = db->NewIterator(leveldb::ReadOptions());
    it->Seek(builtin_keys.location);
    assert(it->Valid());
    for (it->Next(); it->Valid(); it->Next()) {
        auto key = it->key();
        if (key[0] != '@') break;
        key.remove_prefix(1); // remove prefix '@'
        con.append_reply_string(key.ToString());
        size++;
    }
    buffer.swap(con.buf);
    con.append_reply_multi(size);
    con.append(buffer);
}
