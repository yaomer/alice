#ifndef _ALICE_SRC_SKIPLIST_H
#define _ALICE_SRC_SKIPLIST_H

#include <stdlib.h>
#include <iostream>

#include <utility>
#include <vector>

#define SKIPLIST_MAX_LEVEL 32 /* 1 ~ 32 */

#define SKIPLIST_P 0.25

namespace alice {

template <typename T>
struct skiplist_node {
    T *value;
    skiplist_node<T> *prev;
    struct level {
        skiplist_node<T> *next;
        unsigned span; /* 从当前索引节点到下一索引节点的跨度 */
    } level[];
};

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
    using node_type = skiplist_node<value_type>;
    using key_compare = Compare;
    class iterator {
    public:
        iterator() : node(nullptr), prev(nullptr) {  }
        iterator(node_type *node) : node(node), prev(nullptr)
        {
        }
        iterator(node_type *node, node_type *prev) : node(node), prev(prev)
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
        node_type *node;
        node_type *prev;
        friend skiplist;
    };
    using const_iterator = const iterator;
    skiplist()
    {
        max_level = 1;
        length = 0;
        head = alloc_head();
        tail = nullptr;
        __srandom();
    }
    ~skiplist() { __clear(); free_node(head); }
    skiplist(const skiplist& sl)
    {
        max_level = 1;
        length = 0;
        head = alloc_head();
        tail = nullptr;
        comp = sl.comp;
        __srandom();
        for (auto it = sl.cbegin(); it != sl.cend(); ++it)
            __insert(it->first, it->second);
    }
    skiplist(skiplist&& sl) : max_level(sl.max_level), length(sl.length),
        head(sl.head), tail(sl.tail), comp(sl.comp)
    {
    }
    skiplist& operator=(skiplist&& sl)
    {
        max_level = sl.max_level;
        length = sl.length;
        head = sl.head;
        tail = sl.tail;
        comp = sl.comp;
        return *this;
    }
    iterator begin() const { return head->level[0].next; }
    iterator end() const { return iterator(nullptr, tail); }
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
            return p == tail ? end() : p;
        }
        if (p->prev) return p->prev;
        else return end();
    }
    iterator find(const key_type& key)
    {
        node_type *p = __find(key);
        return (p && equal(key, p->value->first)) ? p : end();
    }
    // return 0 if not found
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
    bool empty() { return length == 0; }
    size_t size() { return length; }
    void insert(const key_type& key, const T& value)
    {
        __insert(key, value);
    }
    void erase(const key_type& key) { __erase(key); }
    void clear() { __clear(); }
private:
    skiplist& operator=(const skiplist&);
    node_type *alloc_node(int level, value_type *value)
    {
        node_type *node = reinterpret_cast<node_type*>(
                malloc(sizeof(node_type) + level * sizeof(struct skiplist_node<value_type>::level)));
        node->value = value;
        return node;
    }
    void free_node(node_type *node)
    {
        delete node->value;
        free(node);
    }
    node_type *alloc_head()
    {
        node_type *x = alloc_node(SKIPLIST_MAX_LEVEL, nullptr);
        for (int i = 0; i < SKIPLIST_MAX_LEVEL; i++) {
            x->level[i].next = nullptr;
            x->level[i].span = 0;
        }
        x->prev = nullptr;
        return x;
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
        node_type *p = head;
        node_type *pre = p;

        for (int i = max_level - 1; i >= 0; i--) {
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
        node_type *pre = head;
        node_type *p = nullptr;

        for (int i = max_level - 1; i >= 0; i--) {
            while ((p = pre->level[i].next) && greater(key, p->value->first)) {
                order += pre->level[i].span;
                pre = p;
            }
            if (p && equal(key, p->value->first)) {
                order += pre->level[i].span;
                return order;
            }
        }
        return 0;
    }
    node_type *__find_by_order(size_t order)
    {
        node_type *pre = head;
        node_type *p = nullptr;

        for (int i = max_level - 1; i >= 0; i--) {
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
        // 每一层插入位置的前驱
        node_type *update[SKIPLIST_MAX_LEVEL];
        unsigned rank[SKIPLIST_MAX_LEVEL];
        node_type *x = head;

        // 寻找每一层的插入位置
        for (int i = max_level - 1; i >= 0; i--) {
            rank[i] = i == max_level - 1 ? 0 : rank[i + 1];
            while (x->level[i].next && greater(key, x->level[i].next->value->first)) {
                rank[i] += x->level[i].span;
                x = x->level[i].next;
            }
            update[i] = x;
        }

        int level = rand_level();
        if (level > max_level) {
            for (int i = max_level; i < level; i++) {
                rank[i] = 0;
                update[i] = head;
                update[i]->level[i].span = length;
            }
            max_level = level;
        }
        x = alloc_node(level, new value_type(key, value));
        // 逐层插入x，并更新对应的span
        for (int i = 0; i < level; i++) {
            x->level[i].next = update[i]->level[i].next;
            update[i]->level[i].next = x;

            x->level[i].span = update[i]->level[i].span - (rank[0] - rank[i]);
            update[i]->level[i].span = (rank[0] - rank[i]) + 1;
        }

        for (int i = level; i < max_level; i++) {
            update[i]->level[i].span++;
        }

        x->prev = (update[0] == head) ? nullptr : update[0];
        if (x->level[0].next) x->level[0].next->prev = x;
        else tail = x;
        length++;
    }
    void __erase(const key_type& key)
    {
        node_type *update[SKIPLIST_MAX_LEVEL];
        node_type *x = head;
        // 寻找待删除节点
        for (int i = max_level - 1; i >= 0; i--) {
            while (x->level[i].next && greater(key, x->level[i].next->value->first)) {
                x = x->level[i].next;
            }
            update[i] = x;
        }
        x = x->level[0].next;
        // 逐层删除x
        for (int i = 0; i < max_level; i++) {
            if (update[i]->level[i].next == x) {
                update[i]->level[i].span += x->level[i].span - 1;
                update[i]->level[i].next = x->level[i].next;
            } else {
                update[i]->level[i].span--;
            }
        }
        if (x->level[0].next) {
            x->level[0].next->prev = x->prev;
        } else {
            tail = x->prev;
        }
        free_node(x);
        while (max_level > 1 && head->level[max_level - 1].next == nullptr)
            max_level--;
        length--;
    }
    void __clear()
    {
        for (auto *p = head->level[0].next; p; ) {
            node_type *np = p->level[0].next;
            free_node(p);
            p = np;
        }
        free_node(head);
        head = alloc_head();
        tail = nullptr;
        max_level = 1;
        length = 0;
    }
    bool greater(const key_type &lhs, const key_type &rhs)
    {
        return comp(rhs, lhs);
    }
    bool equal(const key_type &lhs, const key_type &rhs)
    {
        // (l >= r && r >= l) ==> l == r
        return !comp(lhs, rhs) && !comp(rhs, lhs);
    }
    void __srandom() { srandom(time(nullptr)); }

    int max_level;
    size_t length;
    node_type *head, *tail;
    key_compare comp;
};
}

#endif
