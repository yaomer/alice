#ifndef _ALICE_SRC_UTIL_H
#define _ALICE_SRC_UTIL_H

namespace Alice {

void setSelfRunId(char *buf);
const char *convert(int64_t value);
int parseLine(std::vector<std::string>& argv, const char *line, const char *linep);

}

#endif // _ALICE_SRC_UTIL_H
