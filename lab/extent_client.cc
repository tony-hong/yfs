// RPC stubs for clients to talk to extent_server

#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <time.h>

// The calls assume that the caller holds a lock on the extent

extent_client::extent_client(std::string dst)
{
  sockaddr_in dstsock;
	make_sockaddr(dst.c_str(), &dstsock);
  cl = new rpcc(dstsock);
  if (cl->bind() != 0) {
    printf("extent_client: bind failed\n");
  }
}

extent_client::~extent_client(){
  delete cl;
}

extent_protocol::status
extent_client::get(extent_protocol::extentid_t eid, std::string &buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  std::map<extent_protocol::extentid_t, extent_cache>::iterator it;
  it = _extent_cache_map.find(eid);

  if (it != _extent_cache_map.end()){
    if(it->second.deleted){
      return extent_protocol::NOENT;
    }
    buf = it->second.data;
  } else {
    ret = cl->call(extent_protocol::get, eid, buf);
    _extent_cache_map[eid] = extent_cache();
    _extent_cache_map[eid].data = buf;
  }
  return ret;
}

extent_protocol::status
extent_client::put(extent_protocol::extentid_t eid, std::string buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  //int r;
  //ret = cl->call(extent_protocol::put, eid, buf, r);
  std::map<extent_protocol::extentid_t, extent_cache>::iterator it;
  it = _extent_cache_map.find(eid);

  if (it == _extent_cache_map.end()){
    _extent_cache_map[eid] = extent_cache();
  }

  //set content
  _extent_cache_map[eid].data = buf;
  _extent_cache_map[eid].dirty = true;

  //set attributes
  extent_protocol::attr a;
  a.size = buf.size();
  time_t TIME_CUR = time(NULL);
  a.atime = TIME_CUR;
  a.ctime = TIME_CUR;
  a.mtime = TIME_CUR;
  _attr_cache_map[eid] = a;

  return ret;
}

extent_protocol::status
extent_client::remove(extent_protocol::extentid_t eid)
{
  extent_protocol::status ret = extent_protocol::OK;
  std::map<extent_protocol::extentid_t, extent_cache>::iterator it;
  it = _extent_cache_map.find(eid);

  assert(it != _extent_cache_map.end());

  if(it->second.deleted){
    return extent_protocol::NOENT;
  }

  _extent_cache_map[eid].dirty = true;
  _extent_cache_map[eid].deleted = true;

  return ret;
}



extent_protocol::status
extent_client::getattr(extent_protocol::extentid_t eid, extent_protocol::attr &attr)
{
  extent_protocol::status ret = extent_protocol::OK;
  std::map<extent_protocol::extentid_t, extent_protocol::attr>::iterator it;
  it = _attr_cache_map.find(eid);

  if (it != _attr_cache_map.end()){
    if(_extent_cache_map[eid].deleted){
      return extent_protocol::NOENT; 
    }
    attr = it->second;
  } else {
    ret = cl->call(extent_protocol::getattr, eid, attr);
    _attr_cache_map[eid] = attr;
  }

  return ret;
}

extent_protocol::status
extent_client::setattr(extent_protocol::extentid_t eid, extent_protocol::attr a)
{
  extent_protocol::status ret = extent_protocol::OK;
  std::string buf;
  //int r;
  //ret = cl->call(extent_protocol::setattr, eid, a, r);
  assert(_attr_cache_map.count(eid) > 0);

  extent_protocol::attr old_a = _attr_cache_map[eid];

  if(a.size != old_a.size){
    get(eid, buf);
  }

  if (a.size < old_a.size){  
        buf = buf.substr(0, a.size);
        _extent_cache_map[eid].data = buf;
        assert(buf.size() == a.size);   
    } else if (old_a.size < a.size){
        buf.resize(a.size);
        _extent_cache_map[eid].data = buf;
  }

  _attr_cache_map[eid].size = a.size;
  time_t TIME_NULL = time(NULL);
  _attr_cache_map[eid].atime = TIME_NULL;
  _attr_cache_map[eid].mtime = TIME_NULL;
  _attr_cache_map[eid].ctime = TIME_NULL;

  _extent_cache_map[eid].dirty = true;

  return ret;
}


extent_protocol::status
extent_client::flush(extent_protocol::extentid_t eid)
{
  extent_protocol::status ret = extent_protocol::OK;
  int r;


  assert(_extent_cache_map.count(eid) > 0);

  extent_cache e_cache = _extent_cache_map[eid];

  //potential optimization: in current implemenation, if a client create a file local and then delete it (suppose that the created file name does not exist at the server), then we will send this file to the server via put RPC, and then delete it via remove RPC, which wastes two RPCs.

  if(e_cache.dirty){
    assert(_attr_cache_map.count(eid) > 0);
    //send the data to the server, we don't need to send the attr to the server, since the server will update it when the server receives the put RPC
    printf("RPC call put\n");
    ret = cl->call(extent_protocol::put, eid, e_cache.data, r);   
  }

  if(e_cache.deleted){
    assert(_attr_cache_map.count(eid) > 0);
    ret = cl->call(extent_protocol::remove, eid, r);
  }


  printf("flush is called\n");
  if(e_cache.dirty ){
    printf("data is dirty\n");
  }

  if(e_cache.deleted ){
    printf("data is deleted\n");
  }

  if(e_cache.dirty || e_cache.deleted){
    //delete the cache
    _extent_cache_map.erase(eid);
    _attr_cache_map.erase(eid);
  }

  return ret;
} 
