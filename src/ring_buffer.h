#ifndef _ALICE_SRC_RING_BUFFER_H
#define _ALICE_SRC_RING_BUFFER_H

#include <string>
#include <string.h>

#include "util.h"

namespace Alice {
// 一个固定长度的fifo队列，如果到达末尾，就会重新绕回到开头，并覆盖掉旧的数据
// 从而使队列中总是保存着最新的数据
// e.g: r是一个大小为3的RingBuffer，将'hello'加入r中的过程如下：
// [h e l] -> [l e l] -> [l o l]
// index=3    index=4    index=5
// 要取出r中的数据，则必须以index为起始索引，因此上述过程可以更直观地表示为：
// [h e l] -> [e l l] -> [l l o]
//            pop 'h'    pop 'e'
class RingBuffer {
public:
    explicit RingBuffer(unsigned size)
    {
        if (!is_power_of_2(size))
            size = round_up_power_of_2(size);
        _buffer.reserve(size);
        _size = size;
        _index = 0;
    }
    // 目前RingBuffer中已经存放有多少数据
    unsigned size() const
    {
        if (_index < _size) return _index;
        else return _size;
    }
    // get只dup一份需要的数据，并不会对RingBuffer作任何修改
    void get(char *to, unsigned len)
    {
        len = std::min(len, size(), std::less<unsigned>());
        if (_index < _size) {
            memcpy(to, begin(), len);
        } else {
            unsigned off = __off();
            unsigned l = std::min(len, _size - off, std::less<unsigned>());
            memcpy(to, begin() + off, l);
            memcpy(to + l, begin(), len - l);
        }
    }
    void put(const char *from, unsigned len)
    {
        if (len > _size) {
            from += len - _size;
            len = _size;
        }
        unsigned off = __off();
        unsigned l = std::min(len, _size - off, std::less<unsigned>());
        memcpy(begin() + off, from, l);
        memcpy(begin(), from + l, len - l);
        _index += len;
    }
private:
    // 因为_size是2的整数次幂，所以off % _size <==> off & (_size - 1)
    unsigned __off() const { return _index & (_size - 1); }

    char *begin() { return &_buffer[0]; }

    std::string _buffer;
    unsigned _size;
    unsigned _index;
};
}

#endif // _ALICE_SRC_RING_BUFFER_H
