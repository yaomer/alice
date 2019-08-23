#ifndef _ALICE_SRC_UTIL_H
#define _ALICE_SRC_UTIL_H

#include <unordered_map>
#include <unordered_set>
#include <random>
#include <tuple>

namespace Alice {

void setSelfRunId(char *buf);
const char *convert(int64_t value);
int parseLine(std::vector<std::string>& argv, const char *line, const char *linep);
// Hash可以是unordered_map或unorderd_set
// getRandBucketNumber()从Hash中挑选出一个随机元素，并返回这个元素
// 所在的bucket以及bucket中的位置
template <typename Hash>
std::tuple<size_t, size_t> getRandBucketNumber(Hash h)
{
    size_t bucketNumber;
    std::uniform_int_distribution<size_t> u(0, h.bucket_count() - 1);
    std::default_random_engine e(clock());
    do {
        bucketNumber = u(e);
    } while (h.bucket_size(bucketNumber) == 0);
    std::uniform_int_distribution<size_t> _u(0, h.bucket_size(bucketNumber) - 1);
    return std::make_tuple(bucketNumber, _u(e));
}

}

#endif // _ALICE_SRC_UTIL_H
