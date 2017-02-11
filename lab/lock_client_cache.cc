// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <random>


static void *
releasethread(void *x)
{
  lock_client_cache *cc = (lock_client_cache *) x;
  cc->releaser();
  return 0;
}

int lock_client_cache::last_port = 0;

lock_client_cache::lock_client_cache(std::string xdst, 
             class lock_release_user *_lu)
  : lock_client(xdst), lu(_lu)
{
    
  std::random_device rd;
  std::mt19937 rndgen(rd());
  std::uniform_int_distribution<int> uniformIntDistribution(1500, 65535); 
    
  srand(time(NULL)^last_port);
  //rlock_port = ((rand()%32000) | (0x1 << 10));
  rlock_port = uniformIntDistribution(rndgen);

  
  rpcs* rlsrpc;
  
  while (true) {
        try {
            std::cout << "Trying port " << rlock_port << "..." << std::endl;
            rlsrpc = new rpcs(rlock_port);
            break;
        } catch (PortBusyException e) {
            std::cout << "Port " << rlock_port << " busy" << std::endl;
            rlock_port = uniformIntDistribution(rndgen);
        }
    }
    
    
  const char *hname;
  // assert(gethostname(hname, 100) == 0);
  hname = "127.0.0.1";
  std::ostringstream host;
  host << hname << ":" << rlock_port;
  id = host.str();
  last_port = rlock_port;
  //rpcs *rlsrpc = new rpcs(rlock_port);    
    
  
  
  /* register RPC handlers with rlsrpc */
  rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache::revoke);
  rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry);

  //initialize the cl_map mutex
  assert(pthread_mutex_init(&c_lock_map_mutex, NULL) == 0);
  assert(pthread_mutex_init(&revoke_list_mutex, NULL) == 0);
  assert(pthread_cond_init(&releaser_cv, NULL) == 0);

  //for lab8, create rsm_client object
  rsmc = new rsm_client(xdst);
  //for lab8, add sequential number, initial value is 0
  xid = 0;

  pthread_t th;
  int r = pthread_create(&th, NULL, &releasethread, (void *) this);
  assert (r == 0);
}


void
lock_client_cache::releaser()
{

  // This method should be a continuous loop, waiting to be notified of
  // freed locks that have been revoked by the server, so that it can
  // send a release RPC.
  int ret, r;

  pthread_mutex_lock(&revoke_list_mutex);

  while(true){
    while(revoke_list.size() > 0){
      lock_protocol::lockid_t lid = revoke_list.front();
      revoke_list.pop_front();
      pthread_mutex_unlock(&revoke_list_mutex);

      //get the c_lock
      pthread_mutex_lock(&c_lock_map_mutex);
      assert(c_lock_map.find(lid) != c_lock_map.end());
      cached_lock &c_lock = c_lock_map[lid];
      pthread_mutex_unlock(&c_lock_map_mutex);

      //operations on c_lock
      pthread_mutex_lock(&c_lock.cached_lock_mutex);
      
      printf("id = %s now tries to release lock lid = %016llx from server\n", id.c_str(), lid);  

      assert(RELEASING == c_lock.lock_state); //all entires in this list should have state == RELEASING
      lock_protocol::xid_t cur_xid = c_lock.xid;
      pthread_mutex_unlock(&c_lock.cached_lock_mutex);
      

      if(lu != NULL){
          lu->dorelease(lid);
      }

      //do NOT hold mutex across RPC, thus we release the revoke_list_mutex
      ret = rsmc->call(lock_protocol::release, id, lid, cur_xid, r);

      

      if(lock_protocol::OK == ret){
          pthread_mutex_lock(&c_lock.cached_lock_mutex);
          assert(c_lock.lock_state == RELEASING);
          c_lock.lock_state = NONE;
          assert(true == c_lock.revoke_flag);
          c_lock.revoke_flag = false;
          pthread_cond_signal(&c_lock.ac_cv);
          pthread_mutex_unlock(&c_lock.cached_lock_mutex);
        }else{
          printf("ERROR from releaser in lock_client_cache\n");
          assert(false);
          return;
        }

      pthread_mutex_lock(&revoke_list_mutex); //get the mutex again
      
    }
    pthread_cond_wait(&releaser_cv, &revoke_list_mutex); 
  }

  pthread_mutex_unlock(&revoke_list_mutex);

}


lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
{

  int ret, r = lock_protocol::OK;

  //get the cached lock with lid from the map, if it does not exist, create one
  pthread_mutex_lock(&c_lock_map_mutex);
  if(c_lock_map.find(lid) == c_lock_map.end()){
    c_lock_map[lid] = cached_lock();
  }
  cached_lock &c_lock = c_lock_map[lid];
  pthread_mutex_unlock(&c_lock_map_mutex);

  //opertaion on the cachedlock
  new_acquire:
    pthread_mutex_lock(&c_lock.cached_lock_mutex);

    while((c_lock.lock_state != NONE && c_lock.lock_state != FREE ) || c_lock.outstanding ){
      pthread_cond_wait(&c_lock.ac_cv, &c_lock.cached_lock_mutex);
    }

    if(NONE == c_lock.lock_state){
      c_lock.lock_state = ACQUIRING;
      c_lock.xid = c_lock.xid + 1;
      lock_protocol::xid_t nxt_xid = c_lock.xid;
      c_lock.outstanding = true;
      
      pthread_mutex_unlock(&c_lock.cached_lock_mutex); 

      // do not hold mutex while calling RPC
      ret = rsmc->call(lock_protocol::acquire, id, lid, nxt_xid, r);

      pthread_mutex_lock(&c_lock.cached_lock_mutex);
      //assert(ACQUIRING == c_lock.lock_state); //since this thread calls RPC, all other acquire thread will not change the state
      c_lock.outstanding = false;

      if(lock_protocol::OK == ret){
        //Notice that the revoke_flag may be = true. But we ignore this revoke_flag this time
        //and STILL get the lock, in order to prevent a special case, that two clients acquire the same lock cucurrently and acquire rpc delays.
        c_lock.lock_state = LOCKED;
        pthread_mutex_unlock(&c_lock.cached_lock_mutex);
        return lock_protocol::OK;
      }else if (lock_protocol::RETRY == ret){
        pthread_mutex_unlock(&c_lock.cached_lock_mutex);
        goto new_acquire; //we need start over, and we STILL hold the lock.
      }
    }

    if(FREE == c_lock.lock_state){
      c_lock.lock_state = LOCKED;
      pthread_mutex_unlock(&c_lock.cached_lock_mutex);
      return lock_protocol::OK;
    }

    return lock_protocol::IOERR;
}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{
  //get the cached lock from the map
  pthread_mutex_lock(&c_lock_map_mutex);
  assert(c_lock_map.find(lid) != c_lock_map.end());
  cached_lock &c_lock = c_lock_map[lid];
  pthread_mutex_unlock(&c_lock_map_mutex);

  //operation on the ached lock 
  pthread_mutex_lock(&c_lock.cached_lock_mutex);
  assert(LOCKED == c_lock.lock_state);

  if(false == c_lock.revoke_flag){
    c_lock.lock_state = FREE;  // the lock is not revoked yet, other threads can still try to get the cached lock
    pthread_cond_signal(&c_lock.ac_cv);
    pthread_mutex_unlock(&c_lock.cached_lock_mutex);
    return lock_protocol::OK;
  }else{
    c_lock.lock_state = RELEASING;

    pthread_mutex_unlock(&c_lock.cached_lock_mutex);

    pthread_mutex_lock(&revoke_list_mutex);
    revoke_list.push_back(lid);
    pthread_cond_signal(&releaser_cv); // only the releaser thread waits for this condition variable
    pthread_mutex_unlock(&revoke_list_mutex);

    return lock_protocol::OK;
  }
  
}


rlock_protocol::status
lock_client_cache::revoke(lock_protocol::lockid_t lid, int &){

  //get the cached lock from the map
  pthread_mutex_lock(&c_lock_map_mutex);
  assert(c_lock_map.find(lid) != c_lock_map.end());
  cached_lock &c_lock = c_lock_map[lid];  
  pthread_mutex_unlock(&c_lock_map_mutex);


  //operation on the ached lock 
  pthread_mutex_lock(&c_lock.cached_lock_mutex);
  if(FREE == c_lock.lock_state){ //good, we can release it now
    c_lock.lock_state = RELEASING;
    c_lock.revoke_flag = true;

    pthread_mutex_unlock(&c_lock.cached_lock_mutex);

    // push it into the revoke list
    pthread_mutex_lock(&revoke_list_mutex);
    revoke_list.push_back(lid);
    pthread_cond_signal(&releaser_cv);
    pthread_mutex_unlock(&revoke_list_mutex); 

    
    
    return rlock_protocol::OK;
  }

  //assert(LOCKED == c_lock.lock_state || ACQUIRING == c_lock.lock_state); //Otherwise it should be locked now
  c_lock.revoke_flag = true;
  pthread_mutex_unlock(&c_lock.cached_lock_mutex);

  return rlock_protocol::OK;
}


rlock_protocol::status
lock_client_cache::retry(lock_protocol::lockid_t lid, int &){
  //get the cached lock from the map
  pthread_mutex_lock(&c_lock_map_mutex);
  assert(c_lock_map.find(lid) != c_lock_map.end());
  cached_lock &c_lock = c_lock_map[lid];  
  pthread_mutex_unlock(&c_lock_map_mutex);
  
  //operation on the ached lock 
  pthread_mutex_lock(&c_lock.cached_lock_mutex);

  if(ACQUIRING == c_lock.lock_state){
    c_lock.lock_state = NONE;
    pthread_cond_signal(&c_lock.ac_cv);
    pthread_mutex_unlock(&c_lock.cached_lock_mutex);
    return rlock_protocol::OK;
  }
  
  pthread_mutex_unlock(&c_lock.cached_lock_mutex);

  return rlock_protocol::OK;
}
