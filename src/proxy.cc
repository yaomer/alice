#include <assert.h>
#include "proxy.h"

//
// 分布式集群解决方案
//

Proxy::Proxy(Angel::EventLoop *loop, Angel::InetAddr& inetAddr)
    : _loop(loop),
    _server(loop, inetAddr)
{
    _server.setConnectionCb(
            std::bind(&Proxy::onConnection, this, _1));
    _server.setMessageCb(
            std::bind(&Proxy::onMessage, this, _1, _2));
    _nodeNums = g_proxy_conf.nodes.size();
    for (auto& node : g_proxy_conf.nodes) {
        addNode(std::get<0>(node), std::get<1>(node));
    }
}

void Proxy::addNode(const std::string& ip, int port)
{
    std::string name;
    name += ip;
    name += ":";
    name += Alice::convert(port);
    Angel::InetAddr inetAddr(port, ip.c_str());
    auto node = new Node(_loop, inetAddr, name);
    _nodes.emplace(hash(name), node);
    size_t vnodes = getVNodeNums();
    int vindex = 1;
    while (vnodes-- > 0) {
        std::string vname;
        vname += name;
        vname += "#";
        vname += Alice::convert(vindex++);
        auto vnode = new Node(nullptr, inetAddr, vname);
        vnode->setRNodeForVNode(node);
        _nodes.emplace(hash(vname), vnode);
    }
}

void Proxy::delNode(const std::string& name)
{
    auto it = _nodes.find(hash(name));
    if (it == _nodes.end()) return;
    size_t vnodes = getVNodeNums();
    int vindex = 1;
    while (vnodes-- > 0) {
        std::string vname;
        vname += name;
        vname += "#";
        vname += Alice::convert(vindex++);
        auto it = _nodes.find(hash(vname));
        if (it == _nodes.end()) return;
        _nodes.erase(it);
    }
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
    // if response is ok
    size_t id = _idQueue.front();
    _idQueue.pop();
    auto& maps = g_proxy->server().connectionMaps();
    auto it = maps.find(id);
    if (it == maps.end()) return;
    it->second->send(buf.peek(), buf.readable());
    buf.retrieveAll();
}

ssize_t Proxy::parseRequest(int& state, Angel::Buffer& buf, std::string& key)
{
    bool cmderr = false;
    const char *s = buf.peek();
    const char *es = s + buf.readable();
    const char *ps = s;
    size_t l, argc, len;
    // 解析命令个数
    const char *next = std::find(s, es, '\n');
    if (next == es) return 0;
    if (s[0] != '*' || next[-1] != '\r') goto err;
    s += 1;
    l = argc = atol(s);
    s = std::find_if_not(s, es, ::isnumber);
    if (s[0] != '\r' || s[1] != '\n') goto err;
    s += 2;
    // 解析各个命令
    while (argc > 0) {
        next = std::find(s, es, '\n');
        if (next == es) return 0;
        if (s[0] != '$' || next[-1] != '\r') goto err;
        len = atol(s + 1);
        s = next + 1;
        if (es - s < len + 2) return 0;
        if (s[len] != '\r' || s[len+1] != '\n') goto err;
        if (argc == l) {
            std::string cmd(len, 0);
            std::transform(s, s + len, cmd.begin(), ::toupper);
            if (commandTable.find(cmd) == commandTable.end()) {
                cmderr = true;
            }
        }
        if (argc == l - 1 && !cmderr) key.assign(s, len);
        s += len + 2;
        argc--;
    }
    state = SUCCEED;
    return s - ps;
err:
    state = PROTOCOLERR;
    return -1;
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
            g_proxy_conf.nodes.emplace_back(it[1], atoi(it[2].c_str()));
        }
    }
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
    { 20,   50 },
    { 50,   20 },
    { 100,  10 },
    { 200,  5 },
    { 500,  2 },
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
