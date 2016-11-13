// this is the extent server

#ifndef extent_server_h
#define extent_server_h

#include <string>
#include <map>
#include "extent_protocol.h"

class extent_server {
private:
    std::map<extent_protocol::extentid_t, std::string> _extent_content_map;
    std::map<extent_protocol::extentid_t, extent_protocol::attr> _extent_attr_map;

public:
  extent_server();
    ~extent_server();

  // The put and get RPCs are used to update and retrieve an extent's contents.
  int put(extent_protocol::extentid_t id, std::string, int &);
  int get(extent_protocol::extentid_t id, std::string &);

  // The getattr RPC retrieves an extent's attributes.
  int getattr(extent_protocol::extentid_t id, extent_protocol::attr &);

  int remove(extent_protocol::extentid_t id, int &);
};

#endif 







