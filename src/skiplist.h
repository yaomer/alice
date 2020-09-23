#ifndef _ALICE_SRC_SKIPLIST_H
#define _ALICE_SRC_SKIPLIST_H

#include <stdlib.h>

#include <utility>

#define SKIPLIST_MAX_LEVEL 32 /* 1 ~ 32 */

#define SKIPLIST_P 0.25

namespace alice {

template <typename Key,
          typename T,
          typename Compare = std::less<Key>>
class skiplist {
public:
    using key_type = Key;
    using value_type = std::pair<const key_type, T>;
    using reference = value_type&;
    using const_reference = const value_type&;
    using pointer = value_type*;
    using const_pointer = const value_type*;
    struct skiplist_node {
        value_type *value;
        skiplist_node *prev;
        struct level {
            skiplist_node *next;
            int span; /* 从当前索引节点到下一索引节点的跨度 */
        } level[];
    };
    class iterator {
    public:
        iterator() : node(nullptr), prev(nullptr) {  }
        iterator(skiplist_node *node) : node(node), prev(nullptr)
        {
        }
        iterator(skiplist_node *node, skiplist_node *prev) : node(node), prev(prev)
        {
        }
        bool operator==(const iterator& iter)
        {
            return node == iter.node;
        }
        bool operator!=(const iterator& iter)
        {
            return node != iter.node;
        }
        iterator& operator++() // ++iter
        {
            if (node) node = node->level[0].next;
            return *this;
        }
        iterator operator++(int) // iter++
        {
            iterator iter(*this);
            ++(*this);
            return iter;
        }
        iterator& operator--()
        {
            if (node) node = node->prev;
            else node = prev;
            return *this;
        }
        iterator operator--(int)
        {
            iterator iter(*this);
            --(*this);
            return iter;
        }
        const_pointer operator->()
        {
            return node->value;
        }
        const_reference operator*()
        {
            return *node->value;
        }
    private:
        skiplist_node *node;
        skiplist_node *prev;
        friend skiplist;
    };
    using node_type = skiplist_node;
    using const_iterator = const iterator;
    using key_compare = Compare;
    skiplist()
    {
        __level = 1;
        __size = 0;
        __head = alloc_node(SKIPLIST_MAX_LEVEL);
        __tail = nullptr;
        __srandom();
    }
    ~skiplist() { __clear(); free(__head); }
    skiplist(const skiplist& sl)
    {
        __level = 1;
        __size = 0;
        __head = alloc_node(SKIPLIST_MAX_LEVEL);
        __tail = nullptr;
        __comp = sl.__comp;
        __srandom();
        for (auto it = sl.cbegin(); it != sl.cend(); ++it)
            __insert(it->first, it->second);
    }
    skiplist(skiplist&& sl) : __level(sl.__level), __size(sl.__size),
        __head(sl.__head), __tail(sl.__tail), __comp(sl.__comp)
    {
    }
    skiplist& operator=(skiplist&& sl)
    {
        __level = sl.__level;
        __size = sl.__size;
        __head = sl.__head;
        __tail = sl.__tail;
        __comp = sl.__comp;
        return *this;
    }
    iterator begin() const { return __head->level[0].next; }
    iterator end() const { return iterator(nullptr, __tail); }
    const_iterator cbegin() const { return begin(); }
    const_iterator cend() const { return end(); }
    iterator lower_bound(const key_type& key)
    {
        node_type *p = __find(key);
        return p ? p : end();
    }
    // if not found or result is maximal, reutrn end()
    iterator upper_bound(const key_type& key)
    {
        node_type *p = __find(key);
        if (!p) return end();
        if (equal(key, p->value->first)) {
            return p == __tail ? end() : p;
        }
        if (p->prev) return p->prev;
        else return end();
    }
    iterator find(const key_type& key)
    {
        node_type *p = __find(key);
        return (p && equal(key, p->value->first)) ? p : end();
    }
    size_t order_of_key(const key_type& key)
    {
        return __order_of_key(key);
    }
    iterator find_by_order(size_t order)
    {
        if (order <= 0 || order > size()) return end();
        node_type *p = __find_by_order(order);
        return p ? p : end();
    }
    bool empty() { return __size == 0; }
    size_t size() { return __size; }
    void insert(const key_type& key, const T& value)
    {
        __insert(key, value);
    }
    void erase(const key_type& key) { __erase(key); }
    void clear() { __clear(); }
private:
    skiplist& operator=(const skiplist&);
    node_type *alloc_node(int level)
    {
        size_t varlen = level * sizeof(struct skiplist_node::level);
        return static_cast<node_type*>(calloc(1, sizeof(node_type) + varlen));
    }
    node_type *alloc_node(const key_type& key, const T& value, int level)
    {
        node_type *node = alloc_node(level);
        node->value = new value_type(key, value);
        return node;
    }
    void free_node(node_type *node)
    {
        delete node->value;
        free(node);
    }
    int rand_level()
    {
        int level = 1;
        while ((random() & 0xffff) < (SKIPLIST_P * 0xffff))
            level++;
        return (level < SKIPLIST_MAX_LEVEL) ? level : SKIPLIST_MAX_LEVEL;
    }
    // 返回键大于等于key的最小节点
    node_type *__find(const key_type& key)
    {
        node_type *p = __head;
        node_type *pre = p;

        for (int i = __level - 1; i >= 0; i--) {
            while ((p = pre->level[i].next) && greater(key, p->value->first))
                pre = p;
            if (p && equal(key, p->value->first))
                break;
        }
        return p;
    }
    size_t __order_of_key(const key_type& key)
    {
        size_t order = 0;
        node_type *pre = __head;
        node_type *p = nullptr;

        for (int i = __level - 1; i >= 0; i--) {
            while ((p = pre->level[i].next) && greater(key, p->value->first)) {
                order += pre->level[i].span;
                pre = p;
            }
            if (p && equal(key, p->value->first)) {
                order += pre->level[i].span;
                break;
            }
        }
        return order;
    }
    node_type *__find_by_order(size_t order)
    {
        node_type *pre = __head;
        node_type *p = nullptr;

        for (int i = __level - 1; i >= 0; i--) {
            while ((p = pre->level[i].next) && order > pre->level[i].span) {
                order -= pre->level[i].span;
                pre = p;
            }
            if (p && order == pre->level[i].span)
                break;
        }
        return p;
    }
    void __insert(const key_type& key, const T& value)
    {
        int level = rand_level();
        if (level > __level) __level = level;
        // 寻找插入位置
        int span[SKIPLIST_MAX_LEVEL] = { 0 }; // level(i)层走过的步数
        node_type *update[__level]; // 指向需要更新的节点
        node_type *pre = __head;
        node_type *p = nullptr;
        for (int i = __level - 1; i >= 0; i--) {
            while ((p = pre->level[i].next) && greater(key, p->value->first)) {
                span[i] += p->level[i].span;
                pre = p;
            }
            update[i] = pre;
        }
        for (int i = __level - 1; i >= 0; i--) {
            for (int j = i - 1; j >= 0; j--)
                span[i] += span[j];
        }
        // 已存在键为key的节点，更新它的值
        if (p && equal(key, p->value->first)) {
            p->value->second = value;
            return;
        }

        p = alloc_node(key, value, level);

        if (!__tail || greater(p->value->first, __tail->value->first))
            __tail = p;

        // 逐层插入
        for (int i = __level - 1; i >= level; i--)
            update[i]->level[i].span++;
        for (int i = level - 1; i > 0; i--) {
            p->level[i].span = update[i]->level[i].span - span[i];
            update[i]->level[i].span = span[i] + 1;
            p->level[i].next = update[i]->level[i].next;
            update[i]->level[i].next = p;
        }
        p->level[0].span = update[0]->level[0].span = 1;
        p->level[0].next = update[0]->level[0].next;
        update[0]->level[0].next = p;
        // update prev
        if (p->level[0].next) p->level[0].next->prev = p;
        p->prev = (update[0] == __head) ? NULL : update[0];
        __size++;
    }
    void __erase(const key_type& key)
    {
        node_type *update[__level];
        node_type *pre = __head;
        node_type *p = nullptr;

        for (int i = __level - 1; i >= 0; i--) {
            while ((p = pre->level[i].next) && greater(key, p->value->first))
                pre = p;
            update[i] = pre;
        }
        if (!p || !equal(key, p->value->first)) return;
        for (int i = __level - 1; i >= 0; i--) {
            if (update[i]->level[i].next == p) {
                update[i]->level[i].span = p->level[i].span;
                update[i]->level[i].next = p->level[i].next;
                if (!__head->level[i].next) __level--;
            } else if (update[i]->level[i].next) {
                update[i]->level[i].span--;
            }
        }
        if (p == __tail) __tail = p->prev;
        if (p->level[0].next) p->level[0].next->prev = p->prev;
        free_node(p);
        __size--;
        return;
    }
    void __clear()
    {
        node_type *p = __head->level[0].next;
        while (p) {
            node_type *np = p->level[0].next;
            free_node(p);
            p = np;
        }
        size_t len = SKIPLIST_MAX_LEVEL * sizeof(struct skiplist_node::level);
        memset(__head->level, 0, len);
        __level = 1;
        __size = 0;
        __tail = nullptr;
    }
    bool less(const key_type &lhs, const key_type &rhs)
    {
        return __comp(lhs, rhs);
    }
    bool greater(const key_type &lhs, const key_type &rhs)
    {
        return __comp(rhs, lhs);
    }
    bool equal(const key_type &lhs, const key_type &rhs)
    { // l >= r && r >= l ==> l == r
        return !__comp(lhs, rhs) && !__comp(rhs, lhs);
    }
    void __srandom() { srandom(time(nullptr)); }

    int __level;
    size_t __size;
    node_type *__head, *__tail;
    key_compare __comp;
};
}

#endif
