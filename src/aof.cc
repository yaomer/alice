#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <string.h>
#include "aof.h"
#include "db.h"
#include "server.h"
#include "config.h"

using namespace Alice;

Aof::Aof(DBServer *dbServer)
    : _dbServer(dbServer),
    _childPid(-1),
    _lastSyncTime(0),
    _currentFilesize(0),
    _lastRewriteFilesize(0),
    _fd(-1)
{
    bzero(tmpfile, sizeof(tmpfile));
}

// 保存服务器执行的所有写命令
void Aof::append(Context::CommandList& cmdlist)
{
    DBServer::appendCommand(_buffer, cmdlist, true);
}

// 保存aof重写过程中执行的所有写命令
void Aof::appendRewriteBuffer(Context::CommandList& cmdlist)
{
    DBServer::appendCommand(_rewriteBuffer, cmdlist, true);
}

// 将缓冲区中的命令flush到文件中
void Aof::appendAof(int64_t now)
{
    if (_buffer.empty()) return;
    int64_t syncInterval = now - _lastSyncTime;
    int fd = open(g_server_conf.appendonly_file.c_str(), O_RDWR | O_APPEND | O_CREAT, 0660);
    writeToFile(fd, _buffer.data(), _buffer.size());
    _buffer.clear();
    if (g_server_conf.aof_mode == AOF_ALWAYS) {
        fsync(fd);
    } else if (g_server_conf.aof_mode == AOF_EVERYSEC) {
        if (syncInterval >= 1000) {
            fsync(fd);
            _lastSyncTime = now;
        }
    }
    _currentFilesize = getFilesize(fd);
    close(fd);
}

static void aofSetCommand(Context::CommandList& cmdlist, int64_t now)
{
    for (size_t i = 1; i < cmdlist.size(); i++) {
        if (strcasecmp(cmdlist[i].c_str(), "EX") == 0) {
            int64_t timeval = atoll(cmdlist[i+1].c_str()) - now;
            cmdlist[i+1] = convert(timeval / 1000);
            break;
        } else if (strcasecmp(cmdlist[i].c_str(), "PX") == 0) {
            int64_t timeval = atoll(cmdlist[i+1].c_str()) - now;
            cmdlist[i+1] = convert(timeval);
            break;
        }
    }
}

void Aof::load()
{
    Context pseudoClient(_dbServer, nullptr);
    Angel::Buffer buf;
    FILE *fp = fopen(g_server_conf.appendonly_file.c_str(), "r");
    if (!fp || getFilesize(fileno(fp)) == 0) {
        rewriteSelectDb(0);
        if (fp) fclose(fp);
        return;
    }
    int64_t now = Angel::TimeStamp::now();
    char *line = nullptr;
    size_t len = 0;
    ssize_t n;
    while ((n = ::getline(&line, &len, fp)) > 0) {
        buf.append(line, n);
        free(line);
        line = nullptr;
        len = 0;
        Server::parseRequest(pseudoClient, buf);
        if (pseudoClient.state() == Context::PARSING)
            continue;
        if (pseudoClient.state() == Context::SUCCEED) {
            auto& cmdlist = pseudoClient.commandList();
            auto it = _dbServer->db()->commandMap().find(cmdlist[0]);
            if (cmdlist[0].compare("PEXPIRE") == 0) {
                int64_t timeval = atoll(cmdlist[2].c_str()) - now;
                cmdlist[2] = convert(timeval);
            } else if (cmdlist[0].compare("SET") == 0 && cmdlist.size() >= 5) {
                aofSetCommand(cmdlist, now);
            }
            it->second._commandCb(pseudoClient);
            cmdlist.clear();
            pseudoClient.setState(Context::PARSING);
        }
        pseudoClient.message().clear();
    }
    _dbServer->switchDb(0);
    fclose(fp);
}

void Aof::rewriteBackground()
{
    strcpy(tmpfile, "tmp.XXXXX");
    mktemp(tmpfile);
    _childPid = fork();
    logInfo("Background AOF rewrite started by pid %ld", _childPid);
    if (_childPid == 0) {
        childPidReset();
        rewrite();
        abort();
    }
}

void Aof::rewrite()
{
    _fd = open(tmpfile, O_RDWR | O_CREAT | O_APPEND, 0660);
    _lastRewriteFilesize = getFilesize(_fd);
    _buffer.clear();
    int64_t now = Angel::TimeStamp::now();
    int index = 0;
    for (auto& db : _dbServer->dbs()) {
        if (db->hashMap().empty()) {
            index++;
            continue;
        }
        rewriteSelectDb(index);
        for (auto it = db->hashMap().begin(); it != db->hashMap().end(); ++it) {
            auto expire = db->expireMap().find(it->first);
            if (expire != db->expireMap().end()) {
                if (expire->second <= now) {
                    db->delKeyWithExpire(it->first);
                    continue;
                } else {
                    rewriteExpire(it->first, expire->second);
                }
            }
            if (isXXType(it, DB::String))
                rewriteString(it);
            else if (isXXType(it, DB::List))
                rewriteList(it);
            else if (isXXType(it, DB::Set))
                rewriteSet(it);
            else if (isXXType(it, DB::Hash))
                rewriteHash(it);
            else if (isXXType(it, DB::Zset))
                rewriteZset(it);
            index++;
        }
    }
    if (_buffer.size() > 0) flush();
    fsync(_fd);
    close(_fd);
    rename(tmpfile, g_server_conf.appendonly_file.c_str());
}

// 将aof重写缓冲区中的命令写到文件中
void Aof::appendRewriteBufferToAof()
{
    if (_rewriteBuffer.empty()) return;
    int fd = open(g_server_conf.appendonly_file.c_str(), O_RDWR | O_APPEND | O_CREAT, 0660);
    writeToFile(fd, _rewriteBuffer.data(), _rewriteBuffer.size());
    _rewriteBuffer.clear();
    fsync(fd);
    close(fd);
}

void Aof::rewriteSelectDb(int dbnum)
{
    append("*2\r\n$6\r\nSELECT\r\n$");
    append(convert(strlen(convert(dbnum))));
    append("\r\n");
    append(convert(dbnum));
    append("\r\n");
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

void Aof::rewriteString(Iterator it)
{
    DB::String& string = getXXType(it, DB::String&);
    append("*3\r\n$3\r\nSET\r\n$");
    append(convert(it->first.size()));
    append("\r\n");
    append(it->first + "\r\n$");
    append(convert(string.size()));
    append("\r\n");
    append(string + "\r\n");
}

void Aof::rewriteList(Iterator it)
{
    DB::List& list = getXXType(it, DB::List&);
    if (list.empty()) return;
    append("*");
    append(convert(list.size() + 2));
    append("\r\n$5\r\nRPUSH\r\n$");
    append(convert(it->first.size()));
    append("\r\n");
    append(it->first + "\r\n");
    for (auto& it : list) {
        append("$");
        append(convert(it.size()));
        append("\r\n");
        append(it + "\r\n");
    }
}

void Aof::rewriteSet(Iterator it)
{
    DB::Set& set = getXXType(it, DB::Set&);
    if (set.empty()) return;
    append("*");
    append(convert(set.size() + 2));
    append("\r\n$4\r\nSADD\r\n$");
    append(convert(it->first.size()));
    append("\r\n");
    append(it->first + "\r\n");
    for (auto& it : set) {
        append("$");
        append(convert(it.size()));
        append("\r\n");
        append(it + "\r\n");
    }
}

void Aof::rewriteHash(Iterator it)
{
    DB::Hash& hash = getXXType(it, DB::Hash&);
    if (hash.empty()) return;
    append("*");
    append(convert(hash.size() * 2 + 2));
    append("\r\n$5\r\nHMSET\r\n$");
    append(convert(it->first.size()));
    append("\r\n");
    append(it->first + "\r\n");
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

void Aof::rewriteZset(Iterator it)
{
    auto& tuple = getXXType(it, DB::Zset&);
    DB::_Zset& zset = std::get<0>(tuple);
    if (zset.empty()) return;
    append("*");
    append(convert(zset.size() * 2 + 2));
    append("\r\n$4\r\nZADD\r\n$");
    append(convert(it->first.size()));
    append("\r\n");
    append(it->first + "\r\n");
    for (auto& it : zset) {
        append("$");
        append(convert(strlen(convert2f(std::get<0>(it)))));
        append("\r\n");
        append(convert2f(std::get<0>(it)));
        append("\r\n$");
        append(convert(std::get<1>(it).size()));
        append("\r\n");
        append(std::get<1>(it) + "\r\n");
    }
}

// aof重写过程中使用，将重写的命令先追加到缓冲区中，
// 然后在合适的时候flush到文件中
void Aof::append(const std::string& s)
{
    _buffer.append(s);
    if (_buffer.size() >= buffer_flush_size) {
        flush();
    }
}

void Aof::flush()
{
    writeToFile(_fd, _buffer.data(), _buffer.size());
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
