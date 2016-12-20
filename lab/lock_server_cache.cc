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

  assert(pthread_mutex_init(&rpcc_pool_mutex, NULL) == 0);


  pthread_t th;
  int r = pthread_create(&th, NULL, &revokethread, (void *) this);
  assert (r == 0);
  r = pthread_create(&th, NULL, &retrythread, (void *) this);
  assert (r == 0);
}

void
lock_server_cache::revoker()
{

  // This method should be a continuous loop, that sends revoke
  // messages to lock holders whenever another client wants the
  // same lock

}


void
lock_server_cache::retryer()
{

  // This method should be a continuous loop, waiting for locks
  // to be released and then sending retry messages to those who
  // are waiting for it.

}

lock_protocol::status
lock_server_cache::stat(lock_protocol::lockid_t, int &){
  return lock_protocol::OK;
}

lock_protocol::status
lock_server_cache::acquire(std::string id, lock_protocol::lockid_t lid, int &r){
  return lock_protocol::OK;
}




lock_protocol::status
lock_server_cache::release(std::string id, lock_protocol::lockid_t lid, int &r){
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

