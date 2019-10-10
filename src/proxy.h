#ifndef _ALICE_PROXY_H
#define _ALICE_PROXY_H

#include <Angel/TcpServer.h>
#include <Angel/TcpClient.h>

#include <vector>
#include <queue>
#include <string>
#include <map>
#include <unordered_set>
#include <memory>

#include "util.h"

using std::placeholders::_1;
using std::placeholders::_2;

class Node {
public:
    using IdQueue = std::queue<size_t>;

    Node(Angel::EventLoop *loop, Angel::InetAddr& inetAddr, const std::string& name)
        : _isVNode(false),
        _rnode(nullptr),
        _name(name)
    {
        if (!loop) { _isVNode = true; return; }
        _client.reset(new Angel::TcpClient(loop, inetAddr));
        _client->setMessageCb(
                std::bind(&Node::forwardResponseToClient, this, _1, _2));
        _client->setCloseCb(
                std::bind(&Node::removeNode, this, _1));
        _client->notExitLoop();
        _client->start();
    }
    bool isVNode() const { return _isVNode; }
    Node *rnode() { return _rnode; }
    const std::string& name() const { return _name; }
    void setRNodeForVNode(Node *node) { _rnode = node; }
    void forwardRequestToServer(size_t id, Angel::Buffer& buf, size_t n);
    void forwardResponseToClient(const Angel::TcpConnectionPtr& conn, Angel::Buffer& buf);
    void removeNode(const Angel::TcpConnectionPtr& conn);
    size_t requests() const { return _requests; }
private:
    bool _isVNode; // 是否是虚拟节点
    Node *_rnode; // 指向真实节点的指针
    std::string _name;
    std::unique_ptr<Angel::TcpClient> _client;
    IdQueue _idQueue;
    size_t _requests; // 该节点执行的请求数
};

class Proxy {
public:
    using NodeMaps = std::map<uint32_t, std::unique_ptr<Node>>;
    using CommandTable = std::unordered_set<std::string>;
    // real node nums -> virtual node nums
    using RVMap = std::map<size_t, size_t>;

    enum State { PARSING, PROTOCOLERR, SUCCEED };

    Proxy(Angel::EventLoop *loop, Angel::InetAddr& inetAddr);
    void onConnection(const Angel::TcpConnectionPtr& conn)
    {
        int state = PARSING;
        conn->setContext(state);
    }
    void onMessage(const Angel::TcpConnectionPtr& conn, Angel::Buffer& buf)
    {
        std::string key;
        int& state = std::any_cast<int&>(conn->getContext());
        while (state == PARSING) {
            ssize_t n = parseRequest(state, buf, key);
            if (state == PARSING) return;
            if (state == PROTOCOLERR) {
                logInfo("conn %d protocol error", conn->id());
                conn->close();
                return;
            }
            state = PARSING;
            if (key.empty()) {
                conn->send("-ERR unknown command\r\n");
                buf.retrieve(n);
                continue;
            }
            Node *node = findNode(key);
            std::cout << key << "\t->\t" << node->name() << "\n";
            if (node) node->forwardRequestToServer(conn->id(), buf, n);
            buf.retrieve(n);
        }
    }
    uint32_t hash(const std::string& key)
    {
        // FNV-1a 32-bits hash
        uint32_t hashval = 2166136261;
        uint32_t fnv_prime = 16777619;
        for (unsigned char c : key) {
            hashval ^= c;
            hashval *= fnv_prime;
        }
        return hashval;
    }
    ssize_t parseRequest(int& state, Angel::Buffer& buf, std::string& key);
    Node *findNode(const std::string& key)
    {
        auto it = _nodes.lower_bound(hash(key));
        if (it == _nodes.end()) {
            it = _nodes.lower_bound(0);
            if (it == _nodes.end()) return nullptr;
        }
        if (it->second->isVNode())
            return it->second->rnode();
        else
            return it->second.get();
    }
    void addNode(const std::string& ip, int port);
    void delNode(const std::string& name);
    size_t nodeNums() const { return _nodeNums; }
    size_t getVNodeNums()
    {
        auto it = rvMap.lower_bound(_nodeNums);
        if (it != rvMap.end()) return it->second;
        else return 0;
    }
    Angel::EventLoop *loop() { return _loop; }
    Angel::TcpServer& server() { return _server; }
    void start() { _server.start(); }
    static void readConf(const char *proxy_conf_file);

    static CommandTable commandTable;
    static RVMap rvMap;
private:
    Angel::EventLoop *_loop;
    Angel::TcpServer _server;
    NodeMaps _nodes;
    size_t _nodeNums;
};

struct ProxyConf {
    int port;
    std::string ip;
    // all nodes <ip, port>
    std::vector<std::tuple<std::string, int>> nodes;
} g_proxy_conf;

extern Proxy *g_proxy;

#endif // _ALICE_PROXY_H
