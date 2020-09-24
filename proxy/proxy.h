#ifndef _ALICE_PROXY_H
#define _ALICE_PROXY_H

#include <angel/server.h>
#include <angel/client.h>

#include <vector>
#include <queue>
#include <string>
#include <map>
#include <unordered_set>
#include <memory>

#include "../src/util.h"

struct proxy_conf_t {
    int port;
    std::string ip;
    // all nodes <ip:name, <ip, port>>
    std::map<std::string, std::tuple<std::string, int>> nodes;
    int vnodes;
} proxy_conf;

class Proxy;

struct Node {
    Node(Proxy *proxy, angel::inet_addr conn_addr);
    void forward_request_to_server(size_t id, angel::buffer& buf, size_t n);
    void forward_response_to_client(const angel::connection_ptr& conn, angel::buffer& buf);

    Proxy *proxy;
    bool is_vnode; // 是否是虚拟节点
    Node *rnode; // 指向真实节点的指针
    int vnodes; // 虚节点数目
    std::string name;
    std::unique_ptr<angel::client> client;
    std::queue<size_t> id_queue;
    size_t requests; // 该节点执行的请求数
};

class Proxy {
public:
    using NodeMaps = std::map<uint32_t, std::unique_ptr<Node>>;
    using CommandTable = std::unordered_set<std::string>;

    Proxy(angel::evloop *loop, angel::inet_addr& listen_addr);
    void message_handler(const angel::connection_ptr& conn, angel::buffer& buf)
    {
        alice::argv_t argv;
        while (true) {
            argv.clear();
            // FIXME: avoid saving parsed commands
            ssize_t n = alice::parse_request(argv, buf);
            if (n == 0) return;
            if (n < 0) {
                log_error("conn %d protocol error", conn->id());
                conn->close();
                return;
            }
            std::transform(argv[0].begin(), argv[0].end(), argv[0].begin(), ::toupper);
            if (argv[0].compare("PROXY") == 0) {
                proxyCommand(conn, argv);
                buf.retrieve(n);
                continue;
            }
            if (commandTable.find(argv[0]) == commandTable.end()) {
                conn->send("-ERR unknown command\r\n");
                buf.retrieve(n);
                continue;
            }
            Node *node = find_node(argv[1]);
            if (node) {
                log_info("%s -> %s", argv[1].c_str(), node->name.c_str());
                node->forward_request_to_server(conn->id(), buf, n);
            }
            buf.retrieve(n);
        }
    }
    uint32_t hash(const std::string& key)
    {
        return murmurHash2(key.data(), key.size());
    }
    static ssize_t parse_response(angel::buffer& buf);
    Node *find_node(const std::string& key)
    {
        auto it = nodes.lower_bound(hash(key));
        if (it == nodes.end()) {
            it = nodes.lower_bound(0);
            if (it == nodes.end()) return nullptr;
        }
        if (it->second->is_vnode)
            return it->second->rnode;
        else
            return it->second.get();
    }
    void add_node(const std::string& ip, int port);
    void del_node(const std::string& ip, int port);
    void del_node(const std::string& name);
    void start() { server.start(); }
    void proxyCommand(const angel::connection_ptr& conn,
                      const alice::argv_t& argv);

    static CommandTable commandTable;
private:
    static uint32_t murmurHash2(const void *key, size_t len);
    void add_node(std::string s1, const std::string& s,
                  const std::string& sep, Node *node, int& index);
    size_t del_node(std::string s1, const std::string& s,
                    const std::string& sep, int& index);

    angel::evloop *loop;
    angel::server server;
    NodeMaps nodes;
    friend Node;
};

#endif // _ALICE_PROXY_H
