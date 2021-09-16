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

#define WITHSCORES  0x01
#define LIMIT       0x02
#define LOI         0x04 // (min，左开区间
#define ROI         0x08 // (max，右开区间

namespace alice {

int parse_set_args(context_t& con, unsigned& cmdops, int64_t& expire);

int check_range_index(context_t& con, long& start, long& stop, long lower, long upper);

struct score_range {
    int lower = 0, upper = 0;
    double min = 0, max = 0;
};

int parse_range_score(context_t& con, unsigned& cmdops, score_range& r,
                      const std::string& min_str, const std::string& max_str);

int parse_zrangebyscore_args(context_t& con, unsigned& cmdops,
                             long& offset, long& limit);

unsigned get_last_cmd(const std::string& lc);

}

#endif // _ALICE_SRC_PARSER_H
