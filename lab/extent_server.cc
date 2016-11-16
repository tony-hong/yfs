// the extent server implementation

#include "extent_server.h"
#include "gettime.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

extent_server::extent_server() {
    _extent_content_map[1] = "";

    struct timespec now;
    clock_gettime(CLOCK_REALTIME, &now);
    
    extent_protocol::attr at;
    at.size = buf.size();
    at.atime = at.mtime = at.ctime = now.tv_sec;
    _extent_attr_map[1] = at;
}

extent_server::~extent_server() {
    std::map<extent_protocol::extentid_t, std::string>::iterator it;
    for (it = _extent_content_map.begin(); it != _extent_content_map.end(); ++it)
        it->second = "";
    _extent_content_map.clear();
    _extent_attr_map.clear();
}

int extent_server::put(extent_protocol::extentid_t id, std::string buf, int &)
{
    extent_protocol::xxstatus status = extent_protocol::OK;

    if (buf.size() > extent_protocol::maxextent) {
        status = extent_protocol::FBIG;
    } else {    
        if (_extent_content_map.find(id) == _extent_content_map.end()){
            // create content pair
            assert(_extent_content_map[id].size() == 0);
            _extent_content_map[id] = buf;

            // create attribute pair
            assert(_extent_attr_map[id].size == 0);
        } else {
            assert(_extent_attr_map.find(id) != _extent_attr_map.end());

            // change to append?
            _extent_content_map[id] = buf;
        }
        struct timespec now;
        clock_gettime(CLOCK_REALTIME, &now);
        
        extent_protocol::attr at = _extent_attr_map[id];
        at.size = buf.size();
        at.atime = at.mtime = at.ctime = now.tv_sec;
    }
    return status;
}

int extent_server::get(extent_protocol::extentid_t id, std::string &buf)
{
    extent_protocol::xxstatus status = extent_protocol::OK;

    if (_extent_content_map.find(id) == _extent_content_map.end()){
        status = extent_protocol::NOENT;
    } else {
        buf = _extent_content_map[id];

        assert(_extent_attr_map.find(id) != _extent_attr_map.end());
        struct timespec now;
        clock_gettime(CLOCK_REALTIME, &now);

        _extent_attr_map[id].atime = now.tv_sec;
    }
    return status;
}

int extent_server::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a)
{
  // You replace this with a real implementation. We send a phony response
  // for now because it's difficult to get FUSE to do anything (including
  // unmount) if getattr fails.
    extent_protocol::xxstatus status = extent_protocol::OK;

    if (_extent_attr_map.find(id) == _extent_attr_map.end()) {
        status = extent_protocol::NOENT;
    } else {
        extent_protocol::attr at = _extent_attr_map[id];
        a.size = at.size;
        a.atime = at.atime;
        a.mtime = at.mtime;
        a.ctime = at.ctime;
    }
    return status;
}

int extent_server::remove(extent_protocol::extentid_t id, int &)
{
    extent_protocol::xxstatus status = extent_protocol::OK;
    if (_extent_content_map.find(id) == _extent_content_map.end()) {
        status = extent_protocol::NOENT;
    } else {
        _extent_content_map.erase(id);
        assert(_extent_attr_map.find(id) != _extent_attr_map.end());
        _extent_attr_map.erase(id);
    }
    return status;
}

