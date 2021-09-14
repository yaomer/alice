#ifndef _ALICE_SRC_UTIL_H
#define _ALICE_SRC_UTIL_H

#include <angel/buffer.h>

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <random>
#include <tuple>

namespace alice {

using argv_t = std::vector<std::string>;

std::string generate_run_id();

ssize_t parse_request(argv_t& argv, angel::buffer& buf);

void conv2resp(std::string& buffer, const argv_t& argv);

extern thread_local char convert_buf[64];

template <typename T>
const char *i2s(T value)
{
    T v = value;
    // 如果使用(v | bits)而不是(v |= bits)
    // 那么对于较短的类型(char、short)就会发生整型提升
    bool isunsigned = ((v |= ((T)1 << (sizeof(T)*8-1))) > 0 ? true : false);
    if (isunsigned) {
        snprintf(convert_buf, sizeof(convert_buf), "%llu", (unsigned long long)value);
    } else {
        snprintf(convert_buf, sizeof(convert_buf), "%lld", (long long)value);
    }
    return convert_buf;
}
const char *d2s(double value);
long str2l(const std::string& nstr);
long long str2ll(const std::string& nstr);
double str2f(const std::string& nstr);
bool str2numerr();

int parse_line(argv_t& argv, const char *line, const char *linep);
std::string argv2str(const argv_t& argv);

// Hash可以是unordered_map或unorderd_set
// get_rand_hash_key()从Hash中挑选出一个随机元素，并返回这个元素
// 所在的bucket以及bucket中的位置
// 切记Hash不可为空，否则函数会陷入死循环!!!
template <typename Hash>
std::tuple<size_t, size_t> get_rand_hash_key(const Hash& h)
{
    size_t bucket;
    std::uniform_int_distribution<size_t> u(0, h.bucket_count() - 1);
    std::default_random_engine e(clock());
    do {
        bucket = u(e);
    } while (h.bucket_size(bucket) == 0);
    std::uniform_int_distribution<size_t> _u(0, h.bucket_size(bucket) - 1);
    return std::make_tuple(bucket, _u(e));
}

void split_line(std::vector<std::string>& argv,
                const char *s,
                const char *es,
                char c);
int fwrite(int fd, const char *buf, size_t nbytes);

using conf_param_list = std::vector<argv_t>;
conf_param_list parse_conf(const char *filename);

off_t get_filesize(int fd);
bool is_file_exists(const std::string& filename);

ssize_t get_proc_memory();

// 判断n是否是2的整数次幂
bool inline is_power_of_2(unsigned n)
{
    return n > 0 && (n & (n - 1)) == 0;
}

#define MAX_POWER_OF_2 (1u << 31)

// 返回不小于n的最小二次幂
unsigned inline round_up_power_of_2(unsigned n)
{
    if (n == 0) return 2;
    if (n >= MAX_POWER_OF_2) return MAX_POWER_OF_2;
    n--;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
    return n + 1;
}

int save_len(std::string& s, uint64_t len);
int load_len(char *ptr, uint64_t *lenptr);

#define UNUSED(x) ((void)(x))

}

#endif // _ALICE_SRC_UTIL_H
