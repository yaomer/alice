#ifndef _ALICE_SRC_MMDB_INTERNAL_H
#define _ALICE_SRC_MMDB_INTERNAL_H

#include "mmdb.h"

// type(it) is DB::iterator
#define is_type(it, _type) \
    ((it)->second.value.type() == typeid(_type))

// type(con) is context_t
// type(it) is DB::iterator
// check type and update key-lru
#define check_type(con, it, _type) \
    do { \
        if (!is_type(it, _type)) { \
            (con).append(shared.type_err); \
            return; \
        } \
        (it)->second.lru = lru_clock; \
    } while (0)

// type(it) is DB::iterator
#define get_value(it, _type) \
    (std::any_cast<_type>((it)->second.value))

#define get_string_value(it) get_value(it, DB::String&)
#define get_list_value(it)   get_value(it, DB::List&)
#define get_hash_value(it)   get_value(it, DB::Hash&)
#define get_set_value(it)    get_value(it, DB::Set&)
#define get_zset_value(it)   get_value(it, Zset&)

#endif // _ALICE_SRC_MMDB_INTERNAL_H
