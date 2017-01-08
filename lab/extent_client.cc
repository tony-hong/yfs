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
    assert(!it->second.deleted);
    // if(it->second.deleted){
    //   return extent_protocol::NOENT;
    // }
    buf = it->second.data;
    printf("extent_client content id = %016llx is cached , content is (%s)\n",eid,buf.c_str());
  } else {
    printf("extent_client content id = %016llx is not cached \n",eid);
    ret = cl->call(extent_protocol::get, eid, buf);
    _extent_cache_map[eid] = extent_cache();
    _extent_cache_map[eid].data = buf;
  }
  assert(extent_protocol::OK == ret);
  return ret;
}

extent_protocol::status
extent_client::put(extent_protocol::extentid_t eid, std::string buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  //int r;
  //ret = cl->call(extent_protocol::put, eid, buf, r);
  printf("extent_client put id = %016llx with content: %s \n",eid,buf.c_str());
  std::map<extent_protocol::extentid_t, extent_cache>::iterator it;
  it = _extent_cache_map.find(eid);

  if (it == _extent_cache_map.end()){
    _extent_cache_map[eid] = extent_cache();
  }

  //set content
  _extent_cache_map[eid].data = buf;
  _extent_cache_map[eid].dirty = true;
  _extent_cache_map[eid].deleted = false;

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

  if(it != _extent_cache_map.end()){
    extent_cache e_cache = _extent_cache_map[eid];
    
    if(e_cache.deleted){
      assert(true == _extent_cache_map[eid].dirty);
      return extent_protocol::NOENT;
    }else{
      _extent_cache_map[eid].dirty = true;
      _extent_cache_map[eid].deleted = true;
    }
  }else{
    _extent_cache_map[eid] = extent_cache();
    _extent_cache_map[eid].dirty = true;
    _extent_cache_map[eid].deleted = true;
  }

  return ret;
}

extent_protocol::status
extent_client::getattr(extent_protocol::extentid_t eid, extent_protocol::attr &attr)
{
  extent_protocol::status ret = extent_protocol::OK;  
  std::map<extent_protocol::extentid_t, extent_protocol::attr>::iterator it;
  it = _attr_cache_map.find(eid);

  printf("extent_client getattr  id =%016llx\n",eid);

  if (it != _attr_cache_map.end()){
    // if(_extent_cache_map[eid].deleted){
    //   return extent_protocol::NOENT; 
    // }
    attr = it->second;
    printf("extent_client attr id = %016llx is cached \n",eid);
  } else {
    printf("extent_client getattr id = %016llx is not cached, try to get attr from the server\n",eid);
    ret = cl->call(extent_protocol::getattr, eid, attr);
    assert(extent_protocol::OK == ret);
    _attr_cache_map[eid] = attr;
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
  assert(_attr_cache_map.count(eid) > 0);

  extent_protocol::attr old_a = _attr_cache_map[eid];

  if(a.size != old_a.size){
    assert(extent_protocol::OK == get(eid, buf));
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

  printf("flush is called with eid = %016llx\n",eid);

  //assert(_extent_cache_map.count(eid) > 0);
  if(_extent_cache_map.count(eid) == 0){
    assert(_attr_cache_map.count(eid) > 0);
    _attr_cache_map.erase(eid);
    assert(_attr_cache_map.count(eid) == 0);
    return ret;
  } 

  extent_cache e_cache = _extent_cache_map[eid];

  /** potential optimization: in current implemenation, if a client create a 
  file local and then delete it (suppose that the created file name does not 
  exist at the server), then we will send this file to the server via put RPC, 
  and then delete it via remove RPC, which wastes two RPCs.
  */

  if(e_cache.dirty){
    assert(_attr_cache_map.count(eid) > 0);
    /** send the data to the server, we don't need to send the attr to the server, 
    since the server will update it when the server receives the put RPC
    */
    printf("data is dirty, e_cache.data is:%s\n",e_cache.data.c_str());
    ret = cl->call(extent_protocol::put, eid, e_cache.data, r);
    assert(extent_protocol::OK == ret);   
  }

  if(e_cache.deleted){
    assert(_attr_cache_map.count(eid) > 0);
    printf("data is deleted\n");
    ret = cl->call(extent_protocol::remove, eid, r);
    assert(extent_protocol::OK == ret); 
  }

  _extent_cache_map.erase(eid);
  assert(_extent_cache_map.count(eid) == 0);
  _attr_cache_map.erase(eid);
  assert(_attr_cache_map.count(eid) == 0);

  return ret;
} 
