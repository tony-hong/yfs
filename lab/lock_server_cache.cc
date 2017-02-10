// the caching lock server implementation

#include "lock_server_cache.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "handle.h"

static void *
revokethread(void *x)
{
  lock_server_cache *sc = (lock_server_cache *) x;
  sc->revoker();
  return 0;
}

static void *
retrythread(void *x)
{
  lock_server_cache *sc = (lock_server_cache *) x;
  sc->retryer();
  return 0;
}

lock_server_cache::lock_server_cache(class rsm *_rsm) 
  : rsm (_rsm)
{
  assert(pthread_mutex_init(&lock_obj_map_mutex, NULL) == 0);
  assert(pthread_mutex_init(&rpcc_pool_mutex, NULL) == 0);

  assert(pthread_mutex_init(&revoke_list_mutex, NULL) == 0);
  assert(pthread_mutex_init(&retry_list_mutex, NULL) == 0);

  assert(pthread_cond_init(&revoker_condition, NULL) == 0);
  assert(pthread_cond_init(&retryer_condition, NULL) == 0);


  pthread_t th;
  int r = pthread_create(&th, NULL, &revokethread, (void *) this);
  assert (r == 0);
  r = pthread_create(&th, NULL, &retrythread, (void *) this);
  assert (r == 0);
}

lock_server_cache::~lock_server_cache()
{
    //int r;
    //terminated = true;
    //pthread_join(_thread_retry, (void **)&r);
    //pthread_join(_thread_revoke, (void **)&r);
    pthread_mutex_destroy(&lock_obj_map_mutex);
    pthread_mutex_destroy(&rpcc_pool_mutex);

    pthread_mutex_destroy(&revoke_list_mutex);
    pthread_mutex_destroy(&retry_list_mutex);
    
    pthread_cond_destroy(&revoker_condition);
    pthread_cond_destroy(&retryer_condition);

    for (std::map<std::string, rpcc*>::iterator it = rpcc_pool.begin(); it != rpcc_pool.end(); ++it)
    {
        delete it->second;
    }
}

void
lock_server_cache::revoker()
{

  // This method should be a continuous loop, that sends revoke
  // messages to lock holders whenever another client wants the
  // same lock

  int r;
  pthread_mutex_lock(&revoke_list_mutex);

  while(true){
    if(!revoke_list.empty()){
      lock_info l_info = revoke_list.front();
      revoke_list.pop_front();

      if (rsm->amiprimary()) {
        rpcc *cl = handle(l_info.client_id).get_rpcc();
        assert(cl != NULL);

        printf("send revoke to client_id = %s for lockid =%016llx \n", l_info.client_id.c_str() ,l_info.lid );
        //send revoke RPC, do not hold mutex while calling RPC
        pthread_mutex_unlock(&revoke_list_mutex);
        assert(rlock_protocol::OK == cl->call(rlock_protocol::revoke, l_info.lid, r));
        //get the list mutex again
        pthread_mutex_lock(&revoke_list_mutex);
      }
    }else{
      pthread_cond_wait(&revoker_condition, &revoke_list_mutex); 
    }
  }

  pthread_mutex_unlock(&revoke_list_mutex);

}


void
lock_server_cache::retryer()
{

  // This method should be a continuous loop, waiting for locks
  // to be released and then sending retry messages to those who
  // are waiting for it.

  int r;
  pthread_mutex_lock(&retry_list_mutex);
  while(true){
    if(!retry_list.empty()){
      lock_info l_info = retry_list.front();
      retry_list.pop_front();

      if (rsm->amiprimary()) {
        rpcc *cl = handle(l_info.client_id).get_rpcc();
        assert(cl != NULL);

        //send retry RPC, do not hold mutex while calling RPC
        pthread_mutex_unlock(&retry_list_mutex);
        assert(rlock_protocol::OK == cl->call(rlock_protocol::retry, l_info.lid, r));

        //get the list mutex again
        pthread_mutex_lock(&retry_list_mutex);
      }
    }else{
      pthread_cond_wait(&retryer_condition, &retry_list_mutex); 
    }
  }

  pthread_mutex_unlock(&retry_list_mutex);
  return;

}

lock_protocol::status
lock_server_cache::stat(lock_protocol::lockid_t, int &){
  return lock_protocol::OK;
}

lock_protocol::status
lock_server_cache::acquire(std::string id, lock_protocol::lockid_t lid, lock_protocol::xid_t xid, int &r){
  pthread_mutex_lock(&lock_obj_map_mutex);

  printf("client id: %s acquires lock lid = %016llx, xid = %016llx \n", id.c_str(), lid, xid);

  //if never seen this lock, we create one and add it to the map
  if(lock_obj_map.find(lid) == lock_obj_map.end()){ 
    lock_obj_map[lid] = lock_obj();
  }
  //get the lock object and check the info in it
  lock_obj &l_obj = lock_obj_map[lid];

  //check the sequential number xid
  if(l_obj.highest_xid_from_client_map.count(id) == 0){ //never know this client?
    l_obj.highest_xid_from_client_map[id] = 0;
  }

  printf("I have seen acquire request with xid = %016llx from client id: %s with lid = %016llx. \n", l_obj.highest_xid_from_client_map[id], id.c_str(), lid);

  if(l_obj.owner_clientid == id){
      pthread_mutex_unlock(&lock_obj_map_mutex);
      return lock_protocol::OK;
  }


  if(l_obj.highest_xid_from_client_map[id] == xid && l_obj.owner_clientid == id){ //this should be a duplicated request
  
      printf("duplicated accquire request, reuqire id = %s  current owner  id = %s \n", id.c_str(), l_obj.owner_clientid.c_str());
      pthread_mutex_unlock(&lock_obj_map_mutex);
      return lock_protocol::OK;
  }

  if(l_obj.highest_xid_from_client_map[id] == xid && l_obj.owner_clientid != id){
    printf("this should be a duplicated accquire request but we will send revoke agian tho the current owner = %s \n", l_obj.owner_clientid.c_str());
  }else{
    assert(xid == (l_obj.highest_xid_from_client_map[id] + 1) );
    l_obj.highest_xid_from_client_map[id] = xid;
  }


  // if the lock is FREE
  if(FREE == l_obj.lock_state){
    printf("acquire: id = %s  lid = %016llx, lock is free\n", id.c_str(), lid);
    l_obj.lock_state = LOCKED;
    l_obj.owner_clientid = id;

    //if there are also other clients waiting for the locks
    if(l_obj.waiting_clientids.size() > 0){
      l_obj.lock_state = REVOKING;

      //use revoker notify the current owner to give lock back
      pthread_mutex_lock(&revoke_list_mutex);
      printf("lock is free but we need to revoke lid = %016llx from owner = %s because others also wait for this lock\n",  lid, l_obj.owner_clientid.c_str());
      revoke_list.push_back(lock_info(l_obj.owner_clientid, lid));
      pthread_cond_signal(&revoker_condition);
      pthread_mutex_unlock(&revoke_list_mutex);

    }

  pthread_mutex_unlock(&lock_obj_map_mutex);
  return lock_protocol::OK;
  }

  // if the lock is LOCKED or REVOKING (REVOKING implies that the lock is still locked)
  if(LOCKED == l_obj.lock_state || REVOKING == l_obj.lock_state){
    assert(!l_obj.owner_clientid.empty());

    printf("acquire: id = %s lid = %016llx, lock is LOCKED OR REVOKING\n", id.c_str(), lid);

    //in both cases the client should wait for the lock, push it into the list
    l_obj.waiting_clientids.push_back(id);

    //if the lock state is REVOKING, we are already done
    //if the lock is currently LOCKED, we need to revoke the lock from the current owner
    if(LOCKED == l_obj.lock_state){
      l_obj.lock_state = REVOKING;

      //use revoker notify the current owner to give lock back
      pthread_mutex_lock(&revoke_list_mutex);
      printf("need to revoke lid = %016llx from owner = %s\n",  lid, l_obj.owner_clientid.c_str());
      revoke_list.push_back(lock_info(l_obj.owner_clientid, lid));
      pthread_cond_signal(&revoker_condition);
      pthread_mutex_unlock(&revoke_list_mutex);
    }
    
    //unlock and return
    pthread_mutex_unlock(&lock_obj_map_mutex);
    return lock_protocol::RETRY;
  }

  assert(false);
  pthread_mutex_unlock(&lock_obj_map_mutex);
  return lock_protocol::IOERR;
}




lock_protocol::status
lock_server_cache::release(std::string id, lock_protocol::lockid_t lid, lock_protocol::xid_t xid, int &r){
  pthread_mutex_lock(&lock_obj_map_mutex);
  assert(lock_obj_map.find(lid) != lock_obj_map.end());
  lock_obj &l_obj = lock_obj_map[lid];

  if(xid == l_obj.highest_xid_from_client_map[id]){
    if( l_obj.lock_state == FREE || l_obj.owner_clientid.empty() ){
      printf("[debug] This should be a duplicated request\n");
      pthread_mutex_unlock(&lock_obj_map_mutex);
      return lock_protocol::OK;
    }

  }else{
    printf("[error] there is something wrong\n");
    assert(false);
  }

  printf(" owner = %s tries to release the lock_lid = %016llx  xid = %016llx\n", l_obj.owner_clientid.c_str(), lid, xid);
  
  assert(LOCKED == l_obj.lock_state || REVOKING == l_obj.lock_state);

  //printf(" owner = %s releases the lock_lid = %016llx \n", l_obj.owner_clientid.c_str(), lid);

  //unlock and empty the owner
  l_obj.lock_state = FREE;
  l_obj.owner_clientid = "";
  
  //if there is a client waiting for the lock, we should use retryer to send retry() to the next client in the waiting list (if there is at least one client in the waiting list)
  if(l_obj.waiting_clientids.size() > 0){
    std::string nxt_client_id = l_obj.waiting_clientids.front();
    l_obj.waiting_clientids.pop_front();

    //add the lock_info to the retryer list
    pthread_mutex_lock(&retry_list_mutex);
    retry_list.push_back(lock_info(nxt_client_id, lid));
    //wake up the retryer to send rerty() to the next client in the waiting list
    pthread_cond_signal(&retryer_condition);
    pthread_mutex_unlock(&retry_list_mutex);
  }

  

  pthread_mutex_unlock(&lock_obj_map_mutex);
  return lock_protocol::OK;
}




rpcc* lock_server_cache::get_rpcc(std::string id)
{
    rpcc *cl = NULL;
    pthread_mutex_lock(&rpcc_pool_mutex);
    if (rpcc_pool.count(id) == 0)
    {
        sockaddr_in dstsock;
        make_sockaddr(id.c_str(), &dstsock);
        cl = new rpcc(dstsock);
        if (cl->bind() == 0)
            rpcc_pool[id] = cl;
    }
    else
        cl = rpcc_pool[id];
    pthread_mutex_unlock(&rpcc_pool_mutex);
    return cl;
}


std::string
lock_server_cache::marshal_state()
{

  pthread_mutex_lock(&lock_obj_map_mutex);

  marshall rep;
  lock_obj lock_cache_obj; 
  unsigned int size; 

  //iterators
  std::map<lock_protocol::lockid_t, lock_obj>::iterator iter_lock;
  std::list<std::string>::iterator iter_waiting;


  //marshall lock_obj_map
  size = lock_obj_map.size();
  rep << size;

  for (iter_lock = lock_obj_map.begin(); iter_lock != lock_obj_map.end(); iter_lock++) {
    
    lock_protocol::lockid_t lock_id = iter_lock->first;
    lock_cache_obj = lock_obj_map[lock_id];

    //marshal lock_id 
    rep << lock_id;
    //marshal lock_obj
    rep << lock_cache_obj.lock_state;
    rep << lock_cache_obj.owner_clientid;

    //marshal maps and lists in lock_obj
    //1. marshal the waiting list
    unsigned int wait_size = lock_cache_obj.waiting_clientids.size();
    rep << wait_size;

    for (iter_waiting = lock_cache_obj.waiting_clientids.begin(); iter_waiting != lock_cache_obj.waiting_clientids.end(); iter_waiting++) {
      rep << *iter_waiting;
    }

    //2. marshal the highest_xid_from_client_map;
    std::map<std::string, lock_protocol::xid_t>::iterator iter_highest_xid;
    unsigned int xid_size = lock_cache_obj.highest_xid_from_client_map.size();
    rep << xid_size;

    for (iter_highest_xid = lock_cache_obj.highest_xid_from_client_map.begin(); 
         iter_highest_xid != lock_cache_obj.highest_xid_from_client_map.end(); 
         iter_highest_xid++) {
        rep << iter_highest_xid->first;
        rep << iter_highest_xid->second;
    }

    //3. marshal the acquire_reply_map;
    std::map<std::string, int>::iterator iter_ac_reply_map;
    unsigned int ac_reply_size = lock_cache_obj.acquire_reply_map.size();
    rep << ac_reply_size;
    for (iter_ac_reply_map = lock_cache_obj.acquire_reply_map.begin(); 
         iter_ac_reply_map != lock_cache_obj.acquire_reply_map.end(); 
         iter_ac_reply_map++) {
        rep << iter_ac_reply_map->first;
        rep << iter_ac_reply_map->second;
    }

    //4. marshal the release_reply_map;
    std::map<std::string, int>::iterator iter_release_reply_map;
    unsigned int release_reply_size = lock_cache_obj.release_reply_map.size();
    rep << release_reply_size;
    for (iter_release_reply_map = lock_cache_obj.release_reply_map.begin(); 
         iter_release_reply_map != lock_cache_obj.release_reply_map.end(); 
         iter_release_reply_map++) {
        rep << iter_release_reply_map->first;
        rep << iter_release_reply_map->second;
    }


  }

  pthread_mutex_unlock(&lock_obj_map_mutex);
  return rep.str();
}


void
lock_server_cache::unmarshal_state(std::string state)
{
  pthread_mutex_lock(&lock_obj_map_mutex);


  
  unsigned int locks_size;
  unsigned int waiting_size;
  unsigned int xid_size;
  std::string waitinglockid; 

  unmarshall rep(state);
  rep >> locks_size;

  for(unsigned int i = 0; i < locks_size; i++){
    lock_protocol::lockid_t lock_id;
    rep >> lock_id;

    lock_obj *lock_cache_obj = new lock_obj();
    int temp_state;
    rep >> temp_state;
    lock_cache_obj->lock_state = (lock_obj_state)temp_state;
    rep >> lock_cache_obj->owner_clientid;

    //1. unmashall waiting list
    rep >> waiting_size;
    for(unsigned int i = 0; i < waiting_size; i++){
      rep >> waitinglockid;
      lock_cache_obj->waiting_clientids.push_back(waitinglockid);
    }


    //2. unmashall highest_xid_from_client_map
    rep >> xid_size;
    std::string client_id;
    lock_protocol::xid_t xid;
    
    for (unsigned int m = 0; m < xid_size; m++) {
      rep >> client_id;
      rep >> xid;
      lock_cache_obj->highest_xid_from_client_map[client_id] = xid;
    }

    //3. unmashall acquire_reply_map
    unsigned int ac_reply_size;
    rep >> ac_reply_size;
    int ret;
    for (unsigned int n = 0; n < ac_reply_size; n++) {
      rep >> client_id;
      rep >> ret;
      lock_cache_obj->acquire_reply_map[client_id] = ret;
    }

    //4. unmashall release_reply_map
    unsigned int release_reply_size;
    rep >> release_reply_size;
    for (unsigned int j = 0; j < release_reply_size; j++) {
      rep >> client_id;
      rep >> ret;
      lock_cache_obj->release_reply_map[client_id] = ret;
    }

    lock_obj_map[lock_id] = *lock_cache_obj;
  }

  pthread_mutex_unlock(&lock_obj_map_mutex);
}

