#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include "aof.h"
#include "db.h"
#include "server.h"

using namespace Alice;

Aof::Aof(DBServer *dbServer)
    : _dbServer(dbServer),
    _mode(EVERYSEC),
    _childPid(-1),
    _lastSyncTime(0),
    _currentFilesize(0),
    _lastRewriteFilesize(0),
    _fd(-1)
{

}

void Aof::append(Context::CommandList& cmdlist)
{
    _buffer += "*";
    _buffer += convert(cmdlist.size());
    _buffer += "\r\n";
    for (auto& it : cmdlist) {
        _buffer += "$";
        _buffer += convert(it.size());
        _buffer += "\r\n";
        _buffer += it;
        _buffer += "\r\n";
    }
}

void Aof::appendAof(int64_t now)
{
    if (_buffer.empty()) return;
    int64_t syncInterval = now - _lastSyncTime;
    int fd = open("appendonly.aof", O_RDWR | O_APPEND | O_CREAT, 0660);
    write(fd, _buffer.data(), _buffer.size());
    _buffer.clear();
    if (mode() == ALWAYS) {
        fsync(fd);
    } else if (mode() == EVERYSEC) {
        if (syncInterval >= 1000) {
            fsync(fd);
            _lastSyncTime = now;
        }
    }
    _currentFilesize = getFilesize(fd);
    close(fd);
}

void Aof::load()
{
    Context pseudoClient(_dbServer);
    Angel::Buffer buf;
    FILE *fp = fopen("appendonly.aof", "r");
    if (fp == nullptr) return;
    char *line = nullptr;
    size_t len = 0;
    ssize_t n;
    while ((n = ::getline(&line, &len, fp)) > 0) {
        buf.append(line, n);
        free(line);
        line = nullptr;
        len = 0;
        Server::parseRequest(pseudoClient, buf);
        if (pseudoClient.flag() == Context::PARSING)
            continue;
        if (pseudoClient.flag() == Context::SUCCEED) {
            auto& cmdlist = pseudoClient.commandList();
            auto it = _dbServer->db().commandMap().find(cmdlist[0]);
            if (strncasecmp(cmdlist[0].c_str(), "PEXPIRE", 7) == 0) {
                _dbServer->expireMap()[cmdlist[1]] = atoll(cmdlist[2].c_str());
            } else
                it->second._commandCb(pseudoClient);
            cmdlist.clear();
            pseudoClient.setFlag(Context::PARSING);
        }
        pseudoClient.message().clear();
    }
    fclose(fp);
}

#define isXXType(it, _type) \
    ((it).second.value().type() == typeid(_type))
#define getXXType(it, _type) \
    (std::any_cast<_type>((it).second.value()))

void Aof::rewriteBackground()
{
    _childPid = fork();
    if (_childPid == 0) {
        childPidReset();
        rewrite();
        abort();
    }
}

void Aof::rewrite()
{
    char tmpfile[16];
    strcpy(tmpfile, "tmp.XXXXX");
    mktemp(tmpfile);
    _fd = open(tmpfile, O_RDWR | O_CREAT | O_APPEND, 0660);
    _lastRewriteFilesize = getFilesize(_fd);
    _buffer.clear();
    auto& db = _dbServer->db().hashMap();
    int64_t now = Angel::TimeStamp::now();
    for (auto& it : db) {
        auto expire = _dbServer->expireMap().find(it.first);
        if (expire != _dbServer->expireMap().end()) {
            if (expire->second <= now) {
                _dbServer->delExpireKey(it.first);
                _dbServer->db().delKey(it.first);
                continue;
            } else {
                rewriteExpire(it.first, expire->second);
            }
        }
        if (isXXType(it, DB::String))
            rewriteString(it);
        else if (isXXType(it, DB::List))
            rewriteList(it);
        else if (isXXType(it, DB::Set))
            rewriteSet(it);
        else
            rewriteHash(it);
    }
    if (_buffer.size() > 0) flush();
    fsync(_fd);
    close(_fd);
    rename(tmpfile, "appendonly.aof");
}

void Aof::rewriteExpire(const DB::Key& key, int64_t milliseconds)
{
    append("*3\r\n$7\r\nPEXPIRE\r\n$");
    append(convert(key.size()));
    append("\r\n");
    append(key + "\r\n$");
    append(convert(strlen(convert(milliseconds))));
    append("\r\n");
    append(convert(milliseconds));
    append("\r\n");

}

void Aof::rewriteString(Pair pair)
{
    DB::String& string = getXXType(pair, DB::String&);
    append("*3\r\n$3\r\nSET\r\n$");
    append(convert(pair.first.size()));
    append("\r\n");
    append(pair.first + "\r\n$");
    append(convert(string.size()));
    append("\r\n");
    append(string + "\r\n");
}

void Aof::rewriteList(Pair pair)
{
    DB::List& list = getXXType(pair, DB::List&);
    if (list.empty()) return;
    append("*");
    append(convert(list.size() + 2));
    append("\r\n$5\r\nRPUSH\r\n$");
    append(convert(pair.first.size()));
    append("\r\n");
    append(pair.first + "\r\n");
    for (auto& it : list) {
        append("$");
        append(convert(it.size()));
        append("\r\n");
        append(it + "\r\n");
    }
}

void Aof::rewriteSet(Pair pair)
{
    DB::Set& set = getXXType(pair, DB::Set&);
    if (set.empty()) return;
    append("*");
    append(convert(set.size() + 2));
    append("\r\n$4\r\nSADD\r\n$");
    append(convert(pair.first.size()));
    append("\r\n");
    append(pair.first + "\r\n");
    for (auto& it : set) {
        append("$");
        append(convert(it.size()));
        append("\r\n");
        append(it + "\r\n");
    }
}

void Aof::rewriteHash(Pair pair)
{
    DB::Hash& hash = getXXType(pair, DB::Hash&);
    if (hash.empty()) return;
    append("*");
    append(convert(hash.size() * 2 + 2));
    append("\r\n$5\r\nHMSET\r\n$");
    append(convert(pair.first.size()));
    append("\r\n");
    append(pair.first + "\r\n");
    for (auto& it : hash) {
        append("$");
        append(convert(it.first.size()));
        append("\r\n");
        append(it.first + "\r\n$");
        append(convert(it.second.size()));
        append("\r\n");
        append(it.second + "\r\n");
    }
}

void Aof::append(const std::string& s)
{
    _buffer.append(s);
    if (_buffer.size() >= buffer_flush_size) {
        flush();
    }
}

void Aof::flush()
{
    write(_fd, _buffer.data(), _buffer.size());
    _buffer.clear();
}

size_t Aof::getFilesize(int fd)
{
    struct stat st;
    fstat(fd, &st);
    return st.st_size;
}

bool Aof::rewriteIsOk()
{
    return _currentFilesize >= rewrite_min_filesize
        && _currentFilesize >= _lastRewriteFilesize * rewrite_rate;
}
