#include <unistd.h>

#include "mmdb.h"

#include "../server.h"

using namespace alice;
using namespace alice::mmdb;

void engine::free_memory_if_needed()
{
    if (server_conf.mmdb_maxmemory == 0) return;
    auto memory_size = get_proc_memory();
    if (memory_size < 0) {
        log_error("get_proc_memory error: %s", angel::util::strerrno());
        return;
    }
    if (memory_size < server_conf.mmdb_maxmemory) return;
    switch (server_conf.mmdb_maxmemory_policy) {
    case EVICT_ALLKEYS_LRU: evict_all_keys_with_lru(); break;
    case EVICT_VOLATILE_LRU: evict_volatile_with_lru(); break;
    case EVICT_ALLKEYS_RANDOM: evict_all_keys_with_random(); break;
    case EVICT_VOLATILE_RANDOM: evict_volatile_with_random(); break;
    case EVICT_VOLATILE_TTL: evict_volatile_with_ttl(); break;
    case EVICT_NO: break;
    }
}

// 以下的内存淘汰都只是从当前操作的数据库中挑选出一个合适的淘汰对象
// 更合理的方法应该是兼顾所有的数据库

// 真正的lru算法需要一个双端链表来保存维护所有键的lru关系，这需要额外的内存，
// 所以我们这里只是近似模拟一下lru算法
// 我们从当前操作的数据库中随机选出server_conf.mmdb_maxmemory_samples个键，
// 剔除掉其中lru值最大的，即相对来说最近没有被使用的
// 显然mmdb_maxmemory_samples越大，就越接近真正的lru算法，但相对的会有一定的性能
// 损失，所以需要在这两者之间达到一定的平衡
void engine::evict_all_keys_with_lru()
{
    int64_t lastlru = 0;
    int evict[2] = { -1 };
    auto& dict = db()->get_dict();
    for (int i = 0; i < server_conf.mmdb_maxmemory_samples; i++) {
        auto randkey = get_rand_hash_key(dict);
        auto bucket = std::get<0>(randkey);
        auto where = std::get<1>(randkey);
        auto j = where;
        for (auto it = dict.cbegin(bucket); it != dict.end(bucket); ++it) {
            if (j-- == 0) {
                if (it->second.lru > lastlru) {
                    lastlru = it->second.lru;
                    evict[0] = bucket;
                    evict[1] = where;
                    break;
                }
            }
        }
    }
    if (evict[0] == -1) return;
    evict_key(dict, evict);
}

void engine::evict_volatile_with_lru()
{
    int64_t lastlru = 0;
    int evict[2] = { -1 };
    auto& dict = db()->get_expire_keys();
    for (int i = 0; i < server_conf.mmdb_maxmemory_samples; i++) {
        auto randkey = get_rand_hash_key(dict);
        auto bucket = std::get<0>(randkey);
        auto where = std::get<1>(randkey);
        auto j = where;
        for (auto it = dict.cbegin(bucket); it != dict.end(bucket); ++it) {
            if (j-- == 0) {
                auto e = db()->get_dict().find(it->first);
                if (e->second.lru > lastlru) {
                    lastlru = e->second.lru;
                    evict[0] = bucket;
                    evict[1] = where;
                    break;
                }
            }
        }
    }
    if (evict[0] == -1) return;
    evict_key(dict, evict);
}

void engine::evict_all_keys_with_random()
{
    int evict[2];
    auto& dict = db()->get_dict();
    auto randkey = get_rand_hash_key(dict);
    evict[0] = std::get<0>(randkey);
    evict[1] = std::get<1>(randkey);
    evict_key(dict, evict);
}

void engine::evict_volatile_with_random()
{
    auto& dict = db()->get_expire_keys();
    auto randkey = get_rand_hash_key(dict);
    auto bucket = std::get<0>(randkey);
    auto where = std::get<1>(randkey);
    for (auto it = dict.cbegin(bucket); it != dict.end(bucket); ++it) {
        if (where-- == 0) {
            auto e = db()->get_dict().find(it->first);
            evict_key(e->first);
            break;
        }
    }
}

// 从当前操作的数据库的expireMap中随机选出server_conf.mmdb_maxmemory_samples个键,
// 剔除掉其中ttl值最小的，即存活时间最短的
void engine::evict_volatile_with_ttl()
{
    int64_t ttl = 0;
    int evict[2] = { -1 };
    auto& dict = db()->get_expire_keys();
    for (int i = 0; i < server_conf.mmdb_maxmemory_samples; i++) {
        auto randkey = get_rand_hash_key(dict);
        auto bucket = std::get<0>(randkey);
        auto where = std::get<1>(randkey);
        auto j = where;
        for (auto it = dict.cbegin(bucket); it != dict.end(bucket); ++it) {
            if (j-- == 0) {
                if (it->second < ttl) {
                    ttl = it->second;
                    evict[0] = bucket;
                    evict[1] = where;
                    break;
                }
            }
        }
    }
    if (evict[0] == -1) return;
    evict_key(dict, evict);
}

void engine::evict_key(const std::string& key)
{
    argv_t cl = { "DEL", key };
    db()->del_key_with_expire(key);
    __server->append_write_command(cl, nullptr, 0);
}
