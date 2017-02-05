// lock client interface.

#ifndef lock_client_cache_h

#define lock_client_cache_h

#include <string>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_client.h"


// Classes that inherit lock_release_user can override dorelease so that 
// that they will be called when lock_client releases a lock.
// You will not need to do anything with this class until Lab 6.
class lock_release_user {
 public:
  virtual void dorelease(lock_protocol::lockid_t) = 0;
  virtual ~lock_release_user() {};
};


// SUGGESTED LOCK CACHING IMPLEMENTATION PLAN:
//
// to work correctly for lab 7,  all the requests on the server run till 
// completion and threads wait on condition variables on the client to
// wait for a lock.  this allows the server to be replicated using the
// replicated state machine approach.
//
// On the client a lock can be in several states:
//  - free: client owns the lock and no thread has it
//  - locked: client owns the lock and a thread has it
//  - acquiring: the client is acquiring ownership
//  - releasing: the client is releasing ownership
//
// in the state acquiring and locked there may be several threads
// waiting for the lock, but the first thread in the list interacts
// with the server and wakes up the threads when its done (released
// the lock).  a thread in the list is identified by its thread id
// (tid).
//
// a thread is in charge of getting a lock: if the server cannot grant
// it the lock, the thread will receive a retry reply.  at some point
// later, the server sends the thread a retry RPC, encouraging the client
// thread to ask for the lock again.
//
// once a thread has acquired a lock, its client obtains ownership of
// the lock. the client can grant the lock to other threads on the client 
// without interacting with the server. 
//
// the server must send the client a revoke request to get the lock back. this
// request tells the client to send the lock back to the
// server when the lock is released or right now if no thread on the
// client is holding the lock.  when receiving a revoke request, the
// client adds it to a list and wakes up a releaser thread, which returns
// the lock the server as soon it is free.
//
// the releasing is done in a separate a thread to avoid
// deadlocks and to ensure that revoke and retry RPCs from the server
// run to completion (i.e., the revoke RPC cannot do the release when
// the lock is free.
//
// a challenge in the implementation is that retry and revoke requests
// can be out of order with the acquire and release requests.  that
// is, a client may receive a revoke request before it has received
// the positive acknowledgement on its acquire request.  similarly, a
// client may receive a retry before it has received a response on its
// initial acquire request.  a flag field is used to record if a retry
// has been received.
//


class lock_client_cache : public lock_client {
 private:
  //rsm_client *rsmc;
  class lock_release_user *lu;
  int rlock_port;
  std::string hostname;
  std::string id;
  lock_protocol::xid_t xid; //sequential number. Send it along with acquire and realease requests to lock_server, see lab5 and lab8 description for more detail

  //cached lock structure
  enum cached_lock_state{NONE, FREE, LOCKED, ACQUIRING, RELEASING};
  struct cached_lock
  {
    cached_lock_state lock_state;
    bool revoke_flag; 
    pthread_mutex_t cached_lock_mutex;
    pthread_cond_t ac_cv;
    pthread_cond_t revoke_cv;
    lock_protocol::xid_t xid;
    bool outstanding; // we dont want sedn additional acquires for the same lock to the server until the outstaning one has been completed

    cached_lock(){
      lock_state = NONE;
      revoke_flag = false; //if server calls revoke() RPC, set revoke_flag = true
      assert(pthread_mutex_init(&cached_lock_mutex, NULL) == 0);
      assert(pthread_cond_init(&ac_cv, NULL) == 0);
      assert(pthread_cond_init(&revoke_cv, NULL) == 0);
      xid = 0;  
      outstanding = false;
    }
  };

  //maps and lists
  std::map<lock_protocol::lockid_t, cached_lock> c_lock_map; //cached lock map
  std::list<lock_protocol::lockid_t> revoke_list;  //list that saved revoke request from server

  //mutexes
  pthread_mutex_t c_lock_map_mutex;
  pthread_mutex_t revoke_list_mutex;

  //condition variables
  pthread_cond_t releaser_cv; //only the releaser thread should wait for this cv


 public:
  static int last_port;
  lock_client_cache(std::string xdst, class lock_release_user *l = 0);
  virtual ~lock_client_cache() {};
  
  //local function
  lock_protocol::status acquire(lock_protocol::lockid_t);
  lock_protocol::status release(lock_protocol::lockid_t);
  void releaser();

  //RPC function which will called by the LOCK_SERVER_CACHE
  rlock_protocol::status revoke(lock_protocol::lockid_t, int &);
  rlock_protocol::status retry(lock_protocol::lockid_t, int &);
};
#endif


