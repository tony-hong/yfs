// the caching lock server implementation

#include "lock_server_cache.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

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

lock_server_cache::lock_server_cache()
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

      rpcc* cl = get_rpcc(l_info.client_id);
      assert(cl != NULL);

      //send revoke RPC, do not hold mutex while calling RPC
      pthread_mutex_unlock(&revoke_list_mutex);
      assert(rlock_protocol::OK == cl->call(rlock_protocol::revoke, l_info.lid, r));

      //get the list mutex again
      pthread_mutex_lock(&revoke_list_mutex);
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

      rpcc* cl = get_rpcc(l_info.client_id);
      assert(cl != NULL);

      //send retry RPC, do not hold mutex while calling RPC
      pthread_mutex_unlock(&retry_list_mutex);
      assert(rlock_protocol::OK == cl->call(rlock_protocol::retry, l_info.lid, r));

      //get the list mutex again
      pthread_mutex_lock(&retry_list_mutex);
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
lock_server_cache::acquire(std::string id, lock_protocol::lockid_t lid, int &r){
  pthread_mutex_lock(&lock_obj_map_mutex);

  //if never seen this lock, we create one and add it to the map
  if(lock_obj_map.find(lid) == lock_obj_map.end()){ 
    lock_obj_map[lid] = lock_obj();
  }
  //get the lock object and check the info in it
  lock_obj &l_obj = lock_obj_map[lid];

  // if the lock is FREE
  if(FREE == l_obj.lock_state){
    l_obj.lock_state = LOCKED;
    l_obj.owner_clientid = id;
  pthread_mutex_unlock(&lock_obj_map_mutex);
  return lock_protocol::OK;

  }

  // if the lock is LOCKED or REVOKING (REVOKING implies that the lock is still locked)
  if(LOCKED == l_obj.lock_state || REVOKING == l_obj.lock_state){
    assert(!l_obj.owner_clientid.empty());

    //in both cases the client should wait for the lock, push it into the list
    l_obj.waiting_clientids.push_back(id);

    //if the lock state is REVOKING, we are already done
    //if the lock is currently LOCKED, we need to revoke the lock from the current owner
    if(LOCKED == l_obj.lock_state){
      l_obj.lock_state = REVOKING;

      //use revoker notify the current owner to give lock back
      pthread_mutex_lock(&revoke_list_mutex);
      revoke_list.push_back(lock_info(l_obj.owner_clientid, lid));
      pthread_cond_signal(&revoker_condition);
      pthread_mutex_unlock(&revoke_list_mutex);
    }

    //unlock and return
    pthread_mutex_unlock(&lock_obj_map_mutex);
    return lock_protocol::RETRY;
  }

  pthread_mutex_unlock(&lock_obj_map_mutex);
  return lock_protocol::IOERR;
}




lock_protocol::status
lock_server_cache::release(std::string id, lock_protocol::lockid_t lid, int &r){
  pthread_mutex_lock(&lock_obj_map_mutex);
  assert(lock_obj_map.find(lid) != lock_obj_map.end());

  lock_obj &l_obj = lock_obj_map[lid];
  assert(LOCKED == l_obj.lock_state);

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

