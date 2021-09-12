#ifndef _ALICE_SRC_PARSER_H
#define _ALICE_SRC_PARSER_H

#include "db_base.h"

#define SET_NX 0x01
#define SET_XX 0x02
#define SET_EX 0x04
#define SET_PX 0x08

#define BLOCK_LPOP      1
#define BLOCK_RPOP      2
#define BLOCK_RPOPLPUSH 3

namespace alice {

int parse_set_args(context_t& con, unsigned& cmdops, int64_t& expire);

int check_range(context_t& con, int& start, int& stop, int lower, int upper);

unsigned get_last_cmd(const std::string& lc);

}

#endif // _ALICE_SRC_PARSER_H
