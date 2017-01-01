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
  if(extent_cache_map.count(eid) > 0){
    buf = extent_cache_map[eid];
  }else{
    ret = cl->call(extent_protocol::get, eid, buf);
    extent_cache_map[eid] = buf;
  }
  return ret;
}


extent_protocol::status
extent_client::put(extent_protocol::extentid_t eid, std::string buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  //int r;
  //ret = cl->call(extent_protocol::put, eid, buf, r);
  extent_cache_map[eid] = buf;
  extent_protocol::attr a;
  a.size = buf.size();
  a.atime = a.ctime = a.mtime = time(NULL);
  attr_cache_map[eid] = a;
  return ret;
}

extent_protocol::status
extent_client::remove(extent_protocol::extentid_t eid)
{
  extent_protocol::status ret = extent_protocol::OK;
  //int r;
  //ret = cl->call(extent_protocol::remove, eid, r);
  extent_cache_map.erase(eid);
  return ret;
}



extent_protocol::status
extent_client::getattr(extent_protocol::extentid_t eid, 
		       extent_protocol::attr &attr)
{
  extent_protocol::status ret = extent_protocol::OK;

  if(attr_cache_map.count(eid) > 0){
    attr = attr_cache_map[eid];
  }else{
    ret = cl->call(extent_protocol::getattr, eid, attr);
    attr_cache_map[eid] = attr;
  }

  return ret;
}

extent_protocol::status
extent_client::setattr(extent_protocol::extentid_t eid, 
          extent_protocol::attr a)
{
  extent_protocol::status ret = extent_protocol::OK;
  std::string buf;
  //int r;
  //ret = cl->call(extent_protocol::setattr, eid, a, r);
  assert(attr_cache_map.count(eid) > 0);

  extent_protocol::attr old_a = attr_cache_map[eid];

  if(a.size != old_a.size){
    get(eid, buf);
  }

  if (a.size < old_a.size){  
        buf = buf.substr(0, a.size);
        extent_cache_map[eid] = buf;
        assert(buf.size() == a.size);   
    } else if (old_a.size < a.size){
        buf.resize(a.size);
        extent_cache_map[eid] = buf;
  }

  attr_cache_map[eid].size = a.size;
  attr_cache_map[eid].atime = attr_cache_map[eid].mtime = attr_cache_map[eid].ctime = time(NULL);


  return ret;
} 





