// this is the lock server
// the lock client has a similar interface

#ifndef lock_server_h
#define lock_server_h

#include <string>
#include <map>
#include <pthread.h>
#include "lock_protocol.h"
#include "lock_client.h"
#include "rpc.h"


class lock_server {

 protected:
  int nacquire;
  std::map<lock_protocol::lockid_t, pthread_mutex_t> lock_map; // each id can be associated with a mutex
  pthread_mutex_t map_mutex; //a mutex for the mutex_map so that only one thread can read/write the data in the map in a

 public:
  lock_server();
  ~lock_server() {};
  lock_protocol::status stat(int clt, lock_protocol::lockid_t lid, int &);
  lock_protocol::status acquire(int clt, lock_protocol::lockid_t lid, int &);
  lock_protocol::status release(int clt, lock_protocol::lockid_t lid, int &);

};

#endif 







