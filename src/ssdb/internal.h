#ifndef _ALICE_SRC_SSDB_INTERNAL_H
#define _ALICE_SRC_SSDB_INTERNAL_H

#include "ssdb.h"

#define adderr(con, s) \
    do { \
        std::string __str = "-ERR " + s.ToString(); \
        con.append(__str); \
        __str = argv2str(con.argv); \
        log_error("leveldb: %s error: %s", __str.c_str(), s.ToString().c_str()); \
    } while (0)

#define reterr(con, s) \
    do { \
        adderr(con, s); \
        return; \
    } while (0)

#define check_status(con, s) \
    do { \
        if (!s.ok()) reterr(con, s); \
    } while (0)

#define check_type(con, value, type) \
    do { \
        if (get_type(value) != type) { \
            (con).append(shared.type_err); \
            return; \
        } \
    } while (0)

#endif // _ALICE_SRC_SSDB_INTERNAL_H
