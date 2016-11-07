// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

lock_server::lock_server():
  nacquire (0)
{
	assert(pthread_mutex_init(&map_mutex, NULL) == 0);
  assert(pthread_cond_init (&lock_condition, NULL) == 0);
}

lock_server::~lock_server()
{
  assert(pthread_mutex_lock (&map_mutex) == 0);
  lock_map.clear();
  assert(pthread_mutex_unlock (&map_mutex) == 0);
  assert(pthread_mutex_destroy(&map_mutex) == 0);
}


lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("stat request from clt %d\n", clt);
  r = nacquire;
  return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &)
{
	lock_protocol::status ret = lock_protocol::OK;

  assert(pthread_mutex_lock (&map_mutex) == 0);
  while (lock_map.find(lid) != lock_map.end()) {
    pthread_cond_wait(&lock_condition, &map_mutex);
  }
  lock_map[lid] = clt;
  assert(pthread_mutex_unlock (&map_mutex) == 0);
  return ret;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &)
{
  
  lock_protocol::status ret = lock_protocol::OK;
  assert(pthread_mutex_lock (&map_mutex) == 0);
  assert (lock_map.find(lid) != lock_map.end()); 
  assert (lock_map[lid] == clt);
  lock_map.erase(lid); 
  pthread_cond_broadcast(&lock_condition);
  assert(pthread_mutex_unlock (&map_mutex) == 0);
 
  return ret;
}


