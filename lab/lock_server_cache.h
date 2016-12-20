#ifndef lock_server_cache_h
#define lock_server_cache_h

#include <string>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_server.h"



class lock_server_cache {

 private:

  std::map<std::string, rpcc*> rpcc_pool; //<client_nonce, rpc_client>, rpcc_pool is used for manage all lock_client

  //mutexes
  pthread_mutex_t rpcc_pool_mutex;

  //private functions
  rpcc* get_rpcc(std::string id);

 public:
  lock_server_cache();
  void revoker();
  void retryer();

  lock_protocol::status stat(lock_protocol::lockid_t, int &);
  lock_protocol::status acquire(std::string id, lock_protocol::lockid_t lid, int &);
  lock_protocol::status release(std::string id, lock_protocol::lockid_t lid, int &);
  
  
};

#endif
