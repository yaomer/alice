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
    const char *front() const { return &*_buffer.begin() + index(); }
    unsigned index() const { return __off(_index); }
    unsigned size() const { return _size; }
    void push(const char *from, unsigned len)
    {
        if (len > _size) {
            from += len - _size;
            len = _size;
        }
        unsigned off = __off(_index);
        unsigned l = std::min(len, _size - off, std::less<unsigned>());
        memcpy(&_buffer[0] + off, from, l);
        memcpy(&_buffer[0], from + l, len - l);
        _index += len;
    }
    static unsigned off(const RingBuffer& r, unsigned offset)
    {
        return r.__off(offset);
    }
private:
    // 因为_size是2的整数次幂，所以off % _size <==> off & (_size - 1)
    unsigned __off(unsigned off) const { return off & (_size - 1); }

    std::string _buffer;
    unsigned _size;
    unsigned _index;
};
}
// 按顺序遍历RingBuffer
// for (int i = r.index(); i < r.size(); i++)
//     ;
// for (int i = 0; i < r.index(); i++)
//     ;

#endif // _ALICE_SRC_RING_BUFFER_H
