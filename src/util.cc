#include <time.h>
#include <random>

#include "util.h"

namespace Alice {

void setSelfRunId(char *buf)
{
    struct timespec tsp;
    clock_gettime(_CLOCK_REALTIME, &tsp);
    std::uniform_int_distribution<size_t> u;
    std::mt19937_64 e(tsp.tv_sec * 1000000000 + tsp.tv_nsec);
    snprintf(buf, 17, "%lx", u(e));
    snprintf(buf + 16, 17, "%lx", u(e));
}

thread_local char convert_buf[32];

const char *convert(int64_t value)
{
    snprintf(convert_buf, sizeof(convert_buf), "%lld", value);
    return convert_buf;
}
}
