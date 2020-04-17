#include <iostream>
#include "../alice.h"

int main()
{
    Alice::AliceContext alice;
    alice.connect("127.0.0.1", 6379);
    if (alice.err()) {
        std::cout << alice.errStr() << "\n";
        return 1;
    }
    alice.executor("set key hello");
    std::cout << alice.reply()[0] << "\n";
    alice.executor("get key");
    std::cout << alice.reply()[0] << "\n";
    alice.executor("del name");
    for (int i = 0; i < 10; i++)
        alice.executor("lpush name #%d#", i);
    alice.executor("lrange name 0 -1");
    std::cout << "name(list):\n";
    for (auto& it : alice.reply())
        std::cout << it << "\n";
    std::cout << alice.lock("hello") << "\n";
    alice.release("hello");
    alice.close();
}
