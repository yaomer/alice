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

struct ProxyConf {
    int port;
    std::string ip;
    // all nodes <ip:name, <ip, port>>
    std::map<std::string, std::tuple<std::string, int>> nodes;
    size_t vnodes;
} g_proxy_conf;

class Node {
public:
    using IdQueue = std::queue<size_t>;

    Node(Angel::EventLoop *loop, Angel::InetAddr& inetAddr)
        : _isVNode(false),
        _rnode(nullptr)
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
    void setName(const std::string& name) { _name = name; }
    void setRNodeForVNode(Node *node) { _rnode = node; }
    size_t vnodes() const { return _vnodes; }
    void setVNodes(size_t vnodes) { _vnodes = vnodes; }
    void forwardRequestToServer(size_t id, Angel::Buffer& buf, size_t n);
    void forwardResponseToClient(const Angel::TcpConnectionPtr& conn, Angel::Buffer& buf);
    void removeNode(const Angel::TcpConnectionPtr& conn);
    size_t requests() const { return _requests; }
private:
    bool _isVNode; // 是否是虚拟节点
    Node *_rnode; // 指向真实节点的指针
    size_t _vnodes; // 虚节点数目
    std::string _name;
    std::unique_ptr<Angel::TcpClient> _client;
    IdQueue _idQueue;
    size_t _requests; // 该节点执行的请求数
};

class Proxy {
public:
    using NodeMaps = std::map<uint32_t, std::unique_ptr<Node>>;
    using CommandTable = std::unordered_set<std::string>;
    using CommandList = std::vector<std::string>;

    Proxy(Angel::EventLoop *loop, Angel::InetAddr& inetAddr);
    void onMessage(const Angel::TcpConnectionPtr& conn, Angel::Buffer& buf)
    {
        CommandList cmdlist;
        while (true) {
            cmdlist.clear();
            // FIXME: avoid saving parsed commands
            ssize_t n = parseRequest(buf, cmdlist);
            if (n == 0) return;
            if (n < 0) {
                logInfo("conn %d protocol error", conn->id());
                conn->close();
                return;
            }
            std::transform(cmdlist[0].begin(), cmdlist[0].end(), cmdlist[0].begin(), ::toupper);
            if (cmdlist[0].compare("PROXY") == 0) {
                proxyCommand(conn, cmdlist);
                buf.retrieve(n);
                continue;
            }
            if (commandTable.find(cmdlist[0]) == commandTable.end()) {
                conn->send("-ERR unknown command\r\n");
                buf.retrieve(n);
                continue;
            }
            Node *node = findNode(cmdlist[1]);
            if (node) {
                std::cout << cmdlist[1] << "\t->\t" << node->name() << "\n";
                node->forwardRequestToServer(conn->id(), buf, n);
            }
            buf.retrieve(n);
        }
    }
    uint32_t hash(const std::string& key)
    {
        return murmurHash2(key.data(), key.size());
    }
    static ssize_t parseRequest(Angel::Buffer& buf, CommandList& cmdlist);
    static ssize_t parseResponse(Angel::Buffer& buf);
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
    void delNode(const std::string& ip, int port);
    void delNode(const std::string& name);
    size_t nodeNums() const { return g_proxy_conf.nodes.size(); }
    size_t getVNodesPerNode() const { return g_proxy_conf.vnodes; }
    Angel::EventLoop *loop() { return _loop; }
    Angel::TcpServer& server() { return _server; }
    void start() { _server.start(); }
    static void readConf(const char *proxy_conf_file);
    void proxyCommand(const Angel::TcpConnectionPtr& conn,
                      const CommandList& cmdlist);

    static CommandTable commandTable;
private:
    static uint32_t murmurHash2(const void *key, size_t len);
    void addNode(std::string s1, const std::string& s,
                 const std::string& sep, Node *node, int& index);
    size_t delNode(std::string s1, const std::string& s,
                   const std::string& sep, int& index);

    Angel::EventLoop *_loop;
    Angel::TcpServer _server;
    NodeMaps _nodes;
};

extern Proxy *g_proxy;

#endif // _ALICE_PROXY_H
