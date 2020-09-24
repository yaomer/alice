#include "../client.h"

#include <iostream>

int main()
{
    AliceClient c;
    c.connect("127.0.0.1", 1296);
    if (!c.isok()) {
        std::cout << c.get_error() << "\n";
        return 1;
    }
    c.executor("set key hello");
    std::cout << c.get_reply()[0] << "\n";
    c.executor("get key");
    std::cout << c.get_reply()[0] << "\n";
    c.executor("del name");
    for (int i = 0; i < 10; i++)
        c.executor("lpush name #%d#", i);
    c.executor("lrange name 0 -1");
    std::cout << "name(list):\n";
    for (auto& it : c.get_reply())
        std::cout << it << "\n";
    c.close();
}
