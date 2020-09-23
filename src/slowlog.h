#ifndef _ALICE_SRC_SLOWLOG_H
#define _ALICE_SRC_SLOWLOG_H

#include <list>

#include "db_base.h"
#include "config.h"

namespace alice {

class slowlog_t {
public:
    slowlog_t() : logid(1) {  }
    void add_slowlog_if_needed(argv_t& argv, int64_t start, int64_t end)
    {
        auto duration = end - start;
        if (server_conf.slowlog_max_len == 0) return;
        if (duration < server_conf.slowlog_log_slower_than) return;
        slowlog log;
        log.id = logid++;
        log.time = start;
        log.duration = duration;
        log.args = argv;
        if (loglist.size() >= server_conf.slowlog_max_len)
            loglist.pop_front();
        loglist.emplace_back(std::move(log));
    }
private:
    struct slowlog {
        size_t id;
        int64_t time;
        int64_t duration;
        std::vector<std::string> args;
    };
    std::list<slowlog> loglist;
    size_t logid;
};
}

#endif
