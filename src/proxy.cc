#include <assert.h>
#include <stdlib.h>
#include "proxy.h"

//
// 分布式集群解决方案，使用一致性哈希
//

Proxy::Proxy(Angel::EventLoop *loop, Angel::InetAddr& inetAddr)
    : _loop(loop),
    _server(loop, inetAddr)
{
    _server.setMessageCb(
            std::bind(&Proxy::onMessage, this, _1, _2));
    for (auto& node : g_proxy_conf.nodes) {
        addNode(std::get<0>(node.second), std::get<1>(node.second));
    }
}

void Proxy::addNode(std::string s1, const std::string& s,
                    const std::string& sep, Node *node, int& index)
{
    // 实际上发生冲突的概率是很小的
    while (true) {
        auto it = _nodes.emplace(hash(s1), node);
        if (it.second) break;
        s1 = s;
        s1 += sep;
        s1 += Alice::convert(index++);
    }
}

void Proxy::addNode(const std::string& ip, int port)
{
    std::string name;
    name += ip;
    name += ":";
    name += Alice::convert(port);
    Angel::InetAddr inetAddr(port, ip.c_str());
    size_t vnodes = getVNodesPerNode();
    auto node = new Node(_loop, inetAddr, name);
    node->setVNodes(vnodes);
    int index = 1;
    addNode(name, name, ":", node, index);
    std::string vname;
    int vindex = 1;
    while (vnodes-- > 0) {
        vname = name;
        vname += "#";
        vname += Alice::convert(vindex++);
        auto vnode = new Node(nullptr, inetAddr, vname);
        vnode->setRNodeForVNode(node);
        addNode(vname, name, "#", vnode, vindex);
    }
    auto it = g_proxy_conf.nodes.find(name);
    if (it == g_proxy_conf.nodes.end())
        g_proxy_conf.nodes.emplace(name, std::make_tuple(ip, port));
}

size_t Proxy::delNode(std::string s1, const std::string& s,
                      const std::string& sep, int& index)
{
    while (true) {
        auto it = _nodes.find(hash(s1));
        if (it != _nodes.end()) {
            size_t vnodes = it->second->vnodes();
            _nodes.erase(it);
            return vnodes;
        }
        s1 = s;
        s1 += sep;
        s1 += Alice::convert(index++);
    }
}

void Proxy::delNode(const std::string& name)
{
    auto it = g_proxy_conf.nodes.find(name);
    if (it == g_proxy_conf.nodes.end()) return;
    int index = 1;
    size_t vnodes = delNode(name, name, ":", index);
    std::string vname;
    int vindex = 1;
    while (vnodes-- > 0) {
        vname = name;
        vname += "#";
        vname += Alice::convert(vindex++);
        delNode(vname, name, "#", vindex);
    }
    auto iter = g_proxy_conf.nodes.find(name);
    if (iter != g_proxy_conf.nodes.end())
        g_proxy_conf.nodes.erase(iter);
}

void Node::removeNode(const Angel::TcpConnectionPtr& conn)
{
    g_proxy->delNode(this->name());
}

// 由于server是单线程的，所以先发送的请求总是先接收到响应，这样可以保证
// id和response总是匹配的
// 并且由于每个连接的id是唯一的，所以就算新到来的连接恰好复用了刚才意外断开
// (发送了请求，但还未接收响应)的连接也不会导致串话
void Node::forwardRequestToServer(size_t id, Angel::Buffer& buf, size_t n)
{
    _idQueue.push(id);
    _client->conn()->send(buf.peek(), n);
    _requests++;
}

void Node::forwardResponseToClient(const Angel::TcpConnectionPtr& conn,
                                   Angel::Buffer& buf)
{
    ssize_t n = Proxy::parseResponse(buf);
    if (n < 0) {
        buf.retrieveAll();
        return;
    }
    if (n == 0) return;
    size_t id = _idQueue.front();
    _idQueue.pop();
    auto& maps = g_proxy->server().connectionMaps();
    auto it = maps.find(id);
    if (it == maps.end()) return;
    it->second->send(buf.peek(), n);
    buf.retrieve(n);
}

// if parse error, return -1
// if not enough data, return 0
// else return length of response
ssize_t Proxy::parseRequest(Angel::Buffer& buf, CommandList& cmdlist)
{
    const char *s = buf.peek();
    const char *es = s + buf.readable();
    const char *ps = s;
    size_t l, argc;
    // 解析命令个数
    const char *next = std::find(s, es, '\n');
    if (next == es) return 0;
    if (s[0] != '*' || next[-1] != '\r') return -1;
    s += 1;
    l = argc = atol(s);
    s = std::find_if_not(s, es, ::isnumber);
    if (s[0] != '\r' || s[1] != '\n') return -1;
    s += 2;
    // 解析各个命令
    while (argc > 0) {
        next = std::find(s, es, '\n');
        if (next == es) return 0;
        if (s[0] != '$' || next[-1] != '\r') return -1;
        int len = atol(s + 1);
        s = next + 1;
        if (es - s < len + 2) return 0;
        if (s[len] != '\r' || s[len+1] != '\n') return -1;
        cmdlist.emplace_back(s, len);
        s += len + 2;
        argc--;
    }
    return s - ps;
}

static ssize_t parseSingle(Angel::Buffer& buf, size_t pos)
{
    int crlf = buf.findStr(buf.peek() + pos, "\r\n");
    return crlf >= 0 ? crlf + 2 : 0;
}

static ssize_t parseBulk(Angel::Buffer& buf, size_t pos)
{
    char *s = buf.peek() + pos;
    const char *es = buf.peek() + buf.readable();
    int crlf = buf.findStr(s, "\r\n");
    if (crlf < 0) return 0;
    int len = atoi(s + 1);
    if (len < 0 && len != -1) return -1;
    if (len == 0 || len == -1) return crlf + 2;
    s += crlf + 2;
    if (es - s < len + 2) return 0;
    return crlf + 2 + len + 2;
}

static ssize_t parseMultiBulk(Angel::Buffer& buf, size_t curpos)
{
    char *s = buf.peek() + curpos;
    int i = buf.findStr(s, "\r\n");
    int pos = 0;
    if (i < 0) return 0;
    int len = atoi(s + 1);
    if (len < 0 && len != -1) return -1;
    if (len == 0 || len == -1) return i + 2;
    s += i + 2;
    pos += i + 2;
    while (len-- > 0) {
        switch (s[0]) {
        case '+': case '-': case ':': i = parseSingle(buf, pos); break;
        case '$': i = parseBulk(buf, pos); break;
        case '*': i = parseMultiBulk(buf, pos); break;
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
ssize_t Proxy::parseResponse(Angel::Buffer& buf)
{
    switch (buf[0]) {
    case '+': case '-': case ':': return parseSingle(buf, 0);
    case '$': return parseBulk(buf, 0);
    case '*': return parseMultiBulk(buf, 0);
    default: return -1;
    }
}

void Proxy::readConf(const char *proxy_conf_file)
{
    Alice::ConfParamList paramlist;
    Alice::parseConf(paramlist, proxy_conf_file);
    for (auto& it : paramlist) {
        if (strcasecmp(it[0].c_str(), "ip") == 0) {
            g_proxy_conf.ip = it[1];
        } else if (strcasecmp(it[0].c_str(), "port") == 0) {
            g_proxy_conf.port = atoi(it[1].c_str());
        } else if (strcasecmp(it[0].c_str(), "node") == 0) {
            std::string name = it[1] + ":" + it[2];
            auto tuple = std::make_tuple(it[1], atoi(it[2].c_str()));
            g_proxy_conf.nodes.emplace(name, tuple);
        }
    }
}

// PROXY [add | del] <node-ip> <node-port>
void Proxy::proxyCommand(const Angel::TcpConnectionPtr& conn,
                         const CommandList& cmdlist)
{
    if (cmdlist.size() != 4) {
        conn->send("-ERR wrong number of arguments\r\n");
        return;
    }
    auto& ip = cmdlist[2];
    const char *port = cmdlist[3].c_str();
    if (strcasecmp(cmdlist[1].c_str(), "add") == 0) {
        addNode(ip, atoi(port));
        conn->send("+OK\r\n");
    } else if (strcasecmp(cmdlist[1].c_str(), "del") == 0) {
        delNode(ip + std::string(":") + port);
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
    "SET", "SETNX", "GET", "GETSET", "APPEND", "STRLEN",
    "INCR", "INCRBY", "DECR", "DECRBY", "SETRANGE", "GETRANGE", "LPUSH",
    "LPUSHX", "RPUSH", "RPUSHX", "LPOP", "RPOP", "LREM", "LLEN",
    "LINDEX", "LSET", "LRANGE", "LTRIM", "BLPOP", "BRPOP", "SADD",
    "SISMEMBER", "SPOP", "SRANDMEMBER", "SREM", "SCARD", "SMEMBERS",
    "HSET", "HSETNX", "HGET", "HEXISTS", "HDEL", "HLEN", "HSTRLEN", "HINCRBY",
    "HMSET", "HMGET", "HKEYS", "HVALS", "HGETALL", "ZADD", "ZSCORE", "ZINCRBY",
    "ZCARD", "ZCOUNT", "ZRANGE", "ZREVRANGE", "ZRANK", "ZREVRANK", "ZREM", "EXISTS",
    "TYPE", "TTL", "PTTL", "EXPIRE", "PEXPIRE", "MOVE", "LRU", "ZRANGEBYSCORE",
    "ZREVRANGEBYSCORE", "ZREMRANGEBYRANK", "ZREMRANGEBYSCORE",
};

Proxy::RVMap Proxy::rvMap = {
    { 5,    500 },
    { 10,   200 },
    { 20,   100 },
    { 50,   50  },
    { 100,  20  },
    { 200,  10  },
    { 500,  5   },
    { 1000, 2   },
};

Proxy *g_proxy;

int main(int argc, char *argv[])
{
    const char *proxy_conf_file = "proxy.conf";
    if (argc > 1) proxy_conf_file = argv[1];
    Proxy::readConf(proxy_conf_file);
    Angel::EventLoop loop;
    Angel::InetAddr inetAddr(g_proxy_conf.port, g_proxy_conf.ip.c_str());
    Proxy proxy(&loop, inetAddr);
    g_proxy = &proxy;
    proxy.start();
    loop.run();
}
