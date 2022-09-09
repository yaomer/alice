#include <assert.h>
#include <stdlib.h>
#include <string.h>

#include "proxy.h"

//
// 分布式集群解决方案，使用一致性哈希
//

Node::Node(Proxy *proxy, angel::inet_addr conn_addr)
    : proxy(proxy), rnode(nullptr)
{
    if (!proxy) {
        is_vnode = true;
        return;
    }
    angel::client_options ops;
    ops.is_quit_loop = false;
    client.reset(new angel::client(proxy->loop, conn_addr, ops));
    client->set_message_handler([this](const angel::connection_ptr& conn, angel::buffer& buf){
            this->forward_response_to_client(conn, buf);
            });
    client->set_close_handler([this](const angel::connection_ptr& conn){
            this->proxy->del_node(this->name);
            });
    client->start();
}

Proxy::Proxy(angel::evloop *loop, angel::inet_addr& listen_addr)
    : loop(loop),
    server(loop, listen_addr)
{
    server.set_message_handler([this](const angel::connection_ptr& conn, angel::buffer& buf){
            this->message_handler(conn, buf);
            });
    for (auto& node : proxy_conf.nodes) {
        add_node(std::get<0>(node.second), std::get<1>(node.second));
    }
}

void Proxy::add_node(std::string s1, const std::string& s,
                    const std::string& sep, Node *node, int& index)
{
    // 实际上发生冲突的概率是很小的
    while (true) {
        auto it = nodes.emplace(hash(s1), node);
        if (it.second) {
            node->name = s1;
            break;
        }
        s1 = s;
        s1 += sep;
        s1 += alice::i2s(index++);
    }
}

void Proxy::add_node(const std::string& ip, int port)
{
    std::string name;
    name += ip;
    name += ":";
    name += alice::i2s(port);
    angel::inet_addr addr(ip, port);
    size_t vnodes = proxy_conf.vnodes;
    auto node = new Node(this, addr);
    node->vnodes = vnodes;
    int index = 1;
    add_node(name, name, ":", node, index);
    std::string vname;
    int vindex = 1;
    while (vnodes-- > 0) {
        vname = name;
        vname += "#";
        vname += alice::i2s(vindex++);
        auto vnode = new Node(nullptr, addr);
        vnode->rnode = node;
        add_node(vname, name, "#", vnode, vindex);
    }
    auto it = proxy_conf.nodes.find(name);
    if (it == proxy_conf.nodes.end())
        proxy_conf.nodes.emplace(name, std::make_tuple(ip, port));
}

size_t Proxy::del_node(std::string s1, const std::string& s,
                      const std::string& sep, int& index)
{
    while (true) {
        auto it = nodes.find(hash(s1));
        assert(it != nodes.end());
        if (it->second->name == s1) {
            size_t vnodes = it->second->vnodes;
            nodes.erase(it);
            return vnodes;
        }
        s1 = s;
        s1 += sep;
        s1 += alice::i2s(index++);
    }
}

void Proxy::del_node(const std::string& ip, int port)
{
    std::string name;
    name += ip;
    name += ":";
    name += alice::i2s(port);
    del_node(name);
}

void Proxy::del_node(const std::string& name)
{
    auto it = proxy_conf.nodes.find(name);
    if (it == proxy_conf.nodes.end()) return;
    proxy_conf.nodes.erase(it);
    int index = 1;
    size_t vnodes = del_node(name, name, ":", index);
    std::string vname;
    int vindex = 1;
    while (vnodes-- > 0) {
        vname = name;
        vname += "#";
        vname += alice::i2s(vindex++);
        del_node(vname, name, "#", vindex);
    }
}

// 由于server是单线程的，所以先发送的请求总是先接收到响应，这样可以保证
// id和response总是匹配的
// 并且由于每个连接的id是唯一的，所以就算新到来的连接恰好复用了刚才意外断开
// (发送了请求，但还未接收响应)的连接也不会导致串话
void Node::forward_request_to_server(size_t id, angel::buffer& buf, size_t n)
{
    id_queue.push(id);
    client->conn()->send(buf.peek(), n);
    requests++;
}

void Node::forward_response_to_client(const angel::connection_ptr& conn,
                                      angel::buffer& buf)
{
    ssize_t n = Proxy::parse_response(buf);
    if (n < 0) {
        buf.retrieve_all();
        return;
    }
    if (n == 0) return;
    size_t id = id_queue.front();
    id_queue.pop();
    auto node = proxy->server.get_connection(id);
    if (!node) return;
    node->send(buf.peek(), n);
    buf.retrieve(n);
}

static ssize_t parse_single(angel::buffer& buf, size_t pos)
{
    int crlf = buf.find(buf.peek() + pos, "\r\n");
    return crlf >= 0 ? crlf + 2 : 0;
}

static ssize_t parse_bulk(angel::buffer& buf, size_t pos)
{
    char *s = buf.peek() + pos;
    const char *es = buf.peek() + buf.readable();
    int crlf = buf.find(s, "\r\n");
    if (crlf < 0) return 0;
    int len = atoi(s + 1);
    if (len < 0 && len != -1) return -1;
    if (len == 0 || len == -1) return crlf + 2;
    s += crlf + 2;
    if (es - s < len + 2) return 0;
    return crlf + 2 + len + 2;
}

static ssize_t parse_multi_bulk(angel::buffer& buf, size_t curpos)
{
    char *s = buf.peek() + curpos;
    int i = buf.find(s, "\r\n");
    int pos = 0;
    if (i < 0) return 0;
    int len = atoi(s + 1);
    if (len < 0 && len != -1) return -1;
    if (len == 0 || len == -1) return i + 2;
    s += i + 2;
    pos += i + 2;
    while (len-- > 0) {
        switch (s[0]) {
        case '+': case '-': case ':': i = parse_single(buf, pos); break;
        case '$': i = parse_bulk(buf, pos); break;
        case '*': i = parse_multi_bulk(buf, pos); break;
        default: return -1;
        }
        if (i < 0) return -1;
        if (i == 0) return 0;
        pos += i;
    }
    return pos;
}

// if parse error, return -1
// if not enough data, return 0
// else return length of response
ssize_t Proxy::parse_response(angel::buffer& buf)
{
    switch (buf[0]) {
    case '+': case '-': case ':': return parse_single(buf, 0);
    case '$': return parse_bulk(buf, 0);
    case '*': return parse_multi_bulk(buf, 0);
    default: return -1;
    }
}

static void read_conf(const std::string& filename)
{
    auto paramlist = alice::parse_conf(filename.c_str());
    for (auto& it : paramlist) {
        if (strcasecmp(it[0].c_str(), "ip") == 0) {
            proxy_conf.ip = it[1];
        } else if (strcasecmp(it[0].c_str(), "port") == 0) {
            proxy_conf.port = atoi(it[1].c_str());
        } else if (strcasecmp(it[0].c_str(), "node") == 0) {
            std::string name = it[1] + ":" + it[2];
            auto tuple = std::make_tuple(it[1], atoi(it[2].c_str()));
            proxy_conf.nodes.emplace(name, tuple);
        } else if (strcasecmp(it[0].c_str(), "vnodes") == 0) {
            proxy_conf.vnodes = atoi(it[1].c_str());
        }
    }
}

// PROXY [add | del] <node-ip> <node-port>
void Proxy::proxyCommand(const angel::connection_ptr& conn,
                         const alice::argv_t& argv)
{
    if (argv.size() != 4) {
        conn->send("-ERR wrong number of arguments\r\n");
        return;
    }
    auto& ip = argv[2];
    int port = alice::str2l(argv[3].c_str());
    if (alice::str2numerr()) {
        conn->send("-ERR value is not an integer or out of range\r\n");
        return;
    }
    if (strcasecmp(argv[1].c_str(), "add") == 0) {
        add_node(ip, port);
        conn->send("+OK\r\n");
    } else if (strcasecmp(argv[1].c_str(), "del") == 0) {
        del_node(ip, port);
        conn->send("+OK\r\n");
    } else
        conn->send("-ERR subcommand error\r\n");
}

uint32_t Proxy::murmurHash2(const void *key, size_t len)
{
    // 'm' and 'r' are mixing constants generated offline.
    // They're not really 'magic', they just happen to work well.
    const uint32_t m = 0x5bd1e995;
    const int r = 24;
    // Initialize the hash to a 'random' value
    uint32_t h = 0 ^ len;
    // Mix 4 bytes at a time into the hash
    const unsigned char *data = (const unsigned char*)key;

    while(len >= 4) {
        uint32_t k = *(uint32_t*)data;
        k *= m;
        k ^= k >> r;
        k *= m;
        h *= m;
        h ^= k;
        data += 4;
        len -= 4;
    }
    // Handle the last few bytes of the input array
    switch(len) {
    case 3: h ^= data[2] << 16;
    case 2: h ^= data[1] << 8;
    case 1: h ^= data[0];
    h *= m;
    };
    // Do a few final mixes of the hash to ensure the last few
    // bytes are well-incorporated.
    h ^= h >> 13;
    h *= m;
    h ^= h >> 15;

    return h;
}

// now, only exec single-key command
Proxy::CommandTable Proxy::commandTable = {
    "SET", "SETNX", "GET", "GETSET", "APPEND", "STRLEN", "INCR",
    "INCRBY", "DECR", "DECRBY", "SETRANGE", "GETRANGE", "LPUSH",
    "LPUSHX", "RPUSH", "RPUSHX", "LPOP", "RPOP", "LREM", "LLEN",
    "LINDEX", "LSET", "LRANGE", "LTRIM", "BLPOP", "BRPOP", "SADD",
    "SISMEMBER", "SPOP", "SRANDMEMBER", "SREM", "SCARD", "SMEMBERS",
    "HSET", "HSETNX", "HGET", "HEXISTS", "HDEL", "HLEN", "HSTRLEN",
    "HINCRBY", "HMSET", "HMGET", "HKEYS", "HVALS", "HGETALL", "ZADD",
    "ZSCORE", "ZINCRBY", "ZCARD", "ZCOUNT", "ZRANGE", "ZREVRANGE",
    "ZRANK", "ZREVRANK", "ZREM", "EXISTS", "TYPE", "TTL", "PTTL",
    "EXPIRE", "PEXPIRE", "MOVE", "LRU", "ZRANGEBYSCORE",
    "ZREVRANGEBYSCORE", "ZREMRANGEBYRANK", "ZREMRANGEBYSCORE",
};

int main(int argc, char *argv[])
{
    const char *proxy_conf_file = "proxy.conf";
    if (argc > 1) proxy_conf_file = argv[1];
    read_conf(proxy_conf_file);
    angel::evloop loop;
    angel::inet_addr listen_addr(proxy_conf.ip, proxy_conf.port);
    Proxy proxy(&loop, listen_addr);
    proxy.start();
    loop.run();
}
