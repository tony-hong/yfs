#ifndef lock_server_cache_h
#define lock_server_cache_h

#include <string>
#include <map>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_server.h"

#include "rsm.h"


class lock_server_cache {
  // TODO: new for lab8 ?
 private:
  class rsm *rsm;

 protected:
  //enum and structs
  enum lock_obj_state {FREE, LOCKED, REVOKING, RETRYING};
  struct lock_obj{
  	lock_obj_state lock_state;
  	std::string owner_clientid; // used to send revoke
  	//std::string retrying_clientid; // used to match with incoming acquire request
  	std::list<std::string> waiting_clientids; // need to send retry

  	 lock_obj(){
      lock_state = FREE;
    }
  };

  struct lock_info { // relationship between client and lock. refer to revke_list and retry_list
    std::string client_id;
    lock_protocol::lockid_t lid;

    lock_info(std::string id_, lock_protocol::lockid_t lid_){
    	client_id = id_;
    	lid = lid_;
    }
  };

  //maps and lists
  std::map<lock_protocol::lockid_t, lock_obj> lock_obj_map;
  std::map<std::string, rpcc*> rpcc_pool; //<client_nonce, rpc_client>, rpcc_pool is used for manage all lock_client
  std::list<lock_info> revoke_list;
  std::list<lock_info> retry_list;

  //mutexes
  pthread_mutex_t lock_obj_map_mutex;
  pthread_mutex_t rpcc_pool_mutex;
  pthread_mutex_t revoke_list_mutex;
  pthread_mutex_t retry_list_mutex;

  //conditions
  pthread_cond_t retryer_condition;
  pthread_cond_t revoker_condition;

  //private functions
  rpcc* get_rpcc(std::string id);

 public:
  lock_server_cache(class rsm *_rsm);
  ~lock_server_cache();
  void revoker();
  void retryer();

  lock_protocol::status stat(lock_protocol::lockid_t, int &);
  lock_protocol::status acquire(std::string id, lock_protocol::lockid_t lid, int &);
  lock_protocol::status release(std::string id, lock_protocol::lockid_t lid, int &);
  
  
};

#endif
