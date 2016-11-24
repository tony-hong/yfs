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
    _content_map[1];
    _attr_map[1];
    assert(_content_map.size() == 1);
    assert(_attr_map.size() == 1);
}

extent_server::~extent_server() {
    std::map<extent_protocol::extentid_t, std::string>::iterator it;
    for (it = _content_map.begin(); it != _content_map.end(); ++it)
        it->second = "";
    _content_map.clear();
    _attr_map.clear();
}

int extent_server::put(extent_protocol::extentid_t id, std::string buf, int &)
{
    extent_protocol::xxstatus status = extent_protocol::OK;

    if (buf.size() > extent_protocol::maxextent) {
        status = extent_protocol::FBIG;
    } else {    
        // should be overwrite

        _content_map[id] = buf;

        struct timespec now;
        clock_gettime(CLOCK_REALTIME, &now);
        
        extent_protocol::attr at;
        if (_attr_map.count(id) != 0)
        {
            at = _attr_map[id];
        }
        at.size = buf.size();
        at.atime = at.mtime = at.ctime = now.tv_sec;            
        _attr_map[id] = at;
        // status = extent_protocol::IOERR;
    }
    return status;
}

int extent_server::get(extent_protocol::extentid_t id, std::string &buf)
{
    extent_protocol::xxstatus status = extent_protocol::OK;

    if (_content_map.find(id) == _content_map.end()){
        status = extent_protocol::NOENT;
    } else {
        buf = _content_map[id];

        assert(_attr_map.find(id) != _attr_map.end());
        struct timespec now;
        clock_gettime(CLOCK_REALTIME, &now);

        _attr_map[id].atime = now.tv_sec;
    }
    return status;
}

int extent_server::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a)
{
  // You replace this with a real implementation. We send a phony response
  // for now because it's difficult to get FUSE to do anything (including
  // unmount) if getattr fails.
    extent_protocol::xxstatus status = extent_protocol::OK;

    if (_attr_map.find(id) == _attr_map.end()) {
        status = extent_protocol::NOENT;
    } else {
        extent_protocol::attr at = _attr_map[id];
        a.size = at.size;
        a.atime = at.atime;
        a.mtime = at.mtime;
        a.ctime = at.ctime;
    }
    return status;
}


int extent_server::setattr(extent_protocol::extentid_t id, extent_protocol::attr a, int &r){

    extent_protocol::xxstatus status = extent_protocol::OK;

    if (_attr_map.find(id) == _attr_map.end()) {
        status = extent_protocol::NOENT;
    }

    extent_protocol::attr old_a = _attr_map[id];

    assert(_content_map.find(id) != _content_map.end());

    std::string buf = _content_map[id];
    assert(buf.size() == old_a.size);
    
    if (a.size < old_a.size){  
        buf = buf.substr(0, a.size);
        _content_map[id] = buf;
        assert(buf.size() == a.size);
    } else if (old_a.size < a.size){
        buf.resize(a.size);
        _content_map[id] = buf;
    }
    
    _attr_map[id] = a ;

    return status;

}

int extent_server::remove(extent_protocol::extentid_t id, int &)
{
    extent_protocol::xxstatus status = extent_protocol::OK;
    if (_content_map.find(id) == _content_map.end()) {
        status = extent_protocol::NOENT;
    } else {
        _content_map.erase(id);
        assert(_attr_map.find(id) != _attr_map.end());
        _attr_map.erase(id);
    }
    return status;
}

