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
}

lock_server::~lock_server()
{
  for (std::map<lock_protocol::lockid_t, pthread_mutex_t>::iterator it = lock_map.begin(); it != lock_map.end(); it++)
  {
    assert(pthread_mutex_destroy(&it->second) == 0);
  }
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
	//first critical section
	assert(pthread_mutex_lock (&map_mutex) == 0);// only one thread can look into the mutex_map at a time
	if(lock_map.find(lid) == lock_map.end()){ // server has never seen the lock id before
		pthread_mutex_t new_mutex;				// add the new mutex in the map
		assert(pthread_mutex_init(&new_mutex, NULL) == 0);
		lock_map[lid] = new_mutex;
		//assert(pthread_mutex_init(&lock_map[lid], 0) == 0);
	}
    assert(pthread_mutex_unlock (&map_mutex) == 0);
  
  	
  	assert(pthread_mutex_lock (&lock_map[lid]) == 0); //thread tries to get the lock
  	return ret;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &)
{
  
  lock_protocol::status ret = lock_protocol::OK;
  assert(pthread_mutex_unlock (&lock_map[lid]) == 0);
  return ret;
}


