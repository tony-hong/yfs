#include "paxos.h"
#include "handle.h"
// #include <signal.h>
#include <stdio.h>

// This module implements the proposer and acceptor of the Paxos
// distributed algorithm as described by Lamport's "Paxos Made
// Simple".  To kick off an instance of Paxos, the caller supplies a
// list of nodes, a proposed value, and invokes the proposer.  If the
// majority of the nodes agree on the proposed value after running
// this instance of Paxos, the acceptor invokes the upcall
// paxos_commit to inform higher layers of the agreed value for this
// instance.


bool
operator> (const prop_t &a, const prop_t &b)
{
  return (a.n > b.n || (a.n == b.n && a.m > b.m));
}

bool
operator>= (const prop_t &a, const prop_t &b)
{
  return (a.n > b.n || (a.n == b.n && a.m >= b.m));
}

std::string
print_members(const std::vector<std::string> &nodes)
{
  std::string s;
  s.clear();
  for (unsigned i = 0; i < nodes.size(); i++) {
    s += nodes[i];
    if (i < (nodes.size()-1))
      s += ",";
  }
  return s;
}

bool isamember(std::string m, const std::vector<std::string> &nodes)
{
  for (unsigned i = 0; i < nodes.size(); i++) {
    if (nodes[i] == m) return 1;
  }
  return 0;
}

bool
proposer::isrunning()
{
  bool r;
  assert(pthread_mutex_lock(&pxs_mutex)==0);
  r = !stable;
  assert(pthread_mutex_unlock(&pxs_mutex)==0);
  return r;
}

// check if the servers in l2 contains a majority of servers in l1
bool
proposer::majority(const std::vector<std::string> &l1, 
		const std::vector<std::string> &l2)
{
  unsigned n = 0;

  for (unsigned i = 0; i < l1.size(); i++) {
    if (isamember(l1[i], l2))
      n++;
  }
  return n >= (l1.size() >> 1) + 1;
}

proposer::proposer(class paxos_change *_cfg, class acceptor *_acceptor, 
		   std::string _me)
  : cfg(_cfg), acc (_acceptor), me (_me), break1 (false), break2 (false), 
    stable (true)
{
  assert (pthread_mutex_init(&pxs_mutex, NULL) == 0);
  //initialize the initial proposer number
  my_n.n = 0;
  my_n.m = me;

}

void
proposer::setn()
{
  my_n.n = acc->get_n_h().n + 1 > my_n.n + 1 ? acc->get_n_h().n + 1 : my_n.n + 1;
}

bool
proposer::run(int instance, std::vector<std::string> newnodes, std::string newv)
{
  std::vector<std::string> accepts;
  std::vector<std::string> nodes;
  std::vector<std::string> nodes1;
  std::string v;
  bool r = false;

  pthread_mutex_lock(&pxs_mutex);
  printf("start: initiate paxos for %s w. i=%d v=%s stable=%d\n",
	 print_members(newnodes).c_str(), instance, newv.c_str(), stable);
  if (!stable) {  // already running proposer?
    printf("proposer::run: already running\n");
    pthread_mutex_unlock(&pxs_mutex);
    return false;
  }
  setn();
  accepts.clear();
  nodes.clear();
  v.clear();

  //stable: "false" when running Paxos
  //own value is newv
  stable = false;
  c_v = newv;

  //the proposer will send its proposal to all nodes in c_nodes. 
  c_nodes = newnodes;
  nodes = c_nodes;

  if (prepare(instance, accepts, nodes, v)) {

    if (majority(c_nodes, accepts)) {
      printf("paxos::manager: received a majority of prepare responses\n");

      if (v.size() == 0) {
	v = c_v;
      }

      breakpoint1();

      nodes1 = accepts;
      accepts.clear();
      accept(instance, accepts, nodes1, v);

      if (majority(c_nodes, accepts)) {
	printf("paxos::manager: received a majority of accept responses\n");

	breakpoint2();

	decide(instance, accepts, v);
	r = true;
      } else {
	printf("paxos::manager: no majority of accept responses\n");
      }
    } else {
      printf("paxos::manager: no majority of prepare responses\n");
    }
  } else {
    printf("paxos::manager: prepare is rejected %d\n", stable);
  }
  stable = true;
  pthread_mutex_unlock(&pxs_mutex);
  return r;
}

bool
proposer::prepare(unsigned instance, std::vector<std::string> &accepts, 
         std::vector<std::string> nodes,
         std::string &v)
{
  

  //initialize prepare msg. prepare msg consists of the instance number (instance), the proposal sequential number(n)
  paxos_protocol::preparearg a;
  a.instance = instance;
  a.n = my_n;

  //according to paxos, the prepare handel at an acceptor will return (n_a,v_a) to our proposal, and we should choose the v_a with highest n_a
  //Thus, we initialize a number max_n with the lowest vlaue(i.e. max_n.n = 0), in order to ensure that once an acceptor returns (n_a,v_a), we can update max_n to n_a
  //At the first time an acceptor replies (n_a,v_a), max_n will be replaced by n_a (since max_n.n has the lowest value 0)
  //Other acceptors will update the max_n later if its reply's n_a is greater than max_n
  prop_t max_n; 
  max_n.n = 0;
  max_n.m = std::string(); 
  
  
  //the prepare handle will return a prepareres
  paxos_protocol::prepareres r;

  for(unsigned i = 0; i < nodes.size(); i++){
    handle h(nodes[i]);
    rpcc* cl = h.get_rpcc();
    if (cl != NULL) 
    {
        printf("[debug] proposer::prepare node(%s) preparereq v = %s\n", nodes[i].c_str(), a.v.c_str());
        if (cl->call(paxos_protocol::preparereq, me, a, r, rpcc::to(1000)) == paxos_protocol::OK)
        {
          // node instance is behind, commit the old value(recovery from replica)
          if (r.oldinstance)
          {
            assert(r.v_a.size() > 0); 
            printf("[debug]proposer::prepare node(%s) oldinstance r.v_a = %s\n", nodes[i].c_str(), r.v_a.c_str());
            acc->commit(instance, r.v_a);
            return false;
          }
          // node accept, get the highest prop
          if (r.accept)
          {
            accepts.push_back(nodes[i]);
            printf("[debug] proposer::prepare node(%s) accept r.n_a.n = %d r.n_a.v = %s\n", nodes[i].c_str(), r.n_a.n, r.v_a.c_str());
            if (r.n_a > max_n) //if the n_a from acceptor nodes[i] is bigger than max_n, we update it (and we update the value too). We use this to choose the v_a from the highest n_a
            {
              max_n = r.n_a;
              v = r.v_a;  // highest v from nodes
            }
          }
          else{ // the proposal is rejected by the acceptor nodes[i]
            printf("[debug] proposer::prepare node(%s) r.accept = 0, i.e. proposer is rejected\n", nodes[i].c_str());
          }
        }else{  //no OK reply from an acceptor
            printf("[error] proposer::prepare node(%s) rpcc error\n", nodes[i].c_str());
        }
    }else{ //bind fails
        printf("[debug] proposer::prepare node(%s) h.get_rpcc() == NULL\n", nodes[i].c_str());
    }
  }

  return true;
}


void
proposer::accept(unsigned instance, std::vector<std::string> &accepts,
        std::vector<std::string> nodes, std::string v)
{
  
  paxos_protocol::acceptarg a;
  a.instance = instance;
  a.n = my_n;
  a.v = v;
  unsigned r;

  for(unsigned i = 0; i < nodes.size(); i++){
    handle h(nodes[i]);
    rpcc* cl = h.get_rpcc();
    if (cl != NULL){
      printf("[debug] proposer::accept node(%s) acceptreq v = %s\n", nodes[i].c_str(), a.v.c_str());
      if (cl->call(paxos_protocol::acceptreq, me, a, r) == paxos_protocol::OK){
        
        if(r){ //accept
          accepts.push_back(nodes[i]);
        }else{
          printf("[debug] proposer::accept node(%s) ret = 0, proposal not accepted\n", nodes[i].c_str());
        }

      }else{
        printf("[error] proposer::accept node(%s) rpcc error\n", nodes[i].c_str());
      }

    }else{
      printf("[debug] proposer::accept node(%s) h.get_rpcc() == NULL\n", nodes[i].c_str());
    }
  }
}

void
proposer::decide(unsigned instance, std::vector<std::string> accepts, 
	      std::string v)
{
  paxos_protocol::decidearg a;
  a.instance = instance;
  a.v =v;

  int r;

  for(unsigned i = 0; i < accepts.size(); i++){
    handle h(accepts[i]);
    rpcc* cl = h.get_rpcc();

    if(cl != NULL ){
      printf("[debug] proposer::decide node(%s) decidereq v = %s\n", accepts[i].c_str(), a.v.c_str());
      if (cl->call(paxos_protocol::decidereq, me, a, r, rpcc::to(1000)) == paxos_protocol::OK){
        printf("[debug] proposer::decide node(%s) ret = %d\n", accepts[i].c_str(), r);
      }else{
        printf("[error] proposer::decide node(%s) rpcc error\n", accepts[i].c_str());
      }
    }else{
      printf("[debug] proposer::decide node(%s) h.get_rpcc() == NULL\n", accepts[i].c_str());
    }
  }
}

acceptor::acceptor(class paxos_change *_cfg, bool _first, std::string _me, 
	     std::string _value)
  : cfg(_cfg), me (_me), instance_h(0)
{
  assert (pthread_mutex_init(&pxs_mutex, NULL) == 0);

  n_h.n = 0;
  n_h.m = me;
  n_a.n = 0;
  n_a.m = me;
  v_a.clear();

  l = new log (this, me);

  if (instance_h == 0 && _first) {
    values[1] = _value;
    l->loginstance(1, _value);
    instance_h = 1;
  }

  pxs = new rpcs(atoi(_me.c_str()));
  pxs->reg(paxos_protocol::preparereq, this, &acceptor::preparereq);
  pxs->reg(paxos_protocol::acceptreq, this, &acceptor::acceptreq);
  pxs->reg(paxos_protocol::decidereq, this, &acceptor::decidereq);
}

paxos_protocol::status
acceptor::preparereq(std::string src, paxos_protocol::preparearg a,
    paxos_protocol::prepareres &r)
{
  // handle a preparereq message from proposer

  ScopedLock ml(&pxs_mutex);
  //check instance number
  if(a.instance <= instance_h){
    r.oldinstance = 1;
    r.accept = 0;
    assert(values.count(instance_h) > 0);
    r.v_a = values[instance_h];
    return paxos_protocol::OK;
  }

  //assert(a.instance == instance_h);

  if(a.n > n_h){ //accept
    n_h = a.n;
    l->loghigh(n_h);

    r.oldinstance = 0;
    r.accept = 1;
    r.n_a = n_a;
    r.v_a = v_a;
  }else{ //reject
    r.oldinstance = 0;
    r.accept = 0;
  }
  return paxos_protocol::OK;

}

paxos_protocol::status
acceptor::acceptreq(std::string src, paxos_protocol::acceptarg a, int &r)
{

  // handle an acceptreq message from proposer

  ScopedLock ml(&pxs_mutex);

  if(a.n >= n_h){
    n_a = a.n;
    v_a = a.v;
    r = 1;
    l->logprop(n_a, v_a);
  }else{
    r = 0;
  }
  return paxos_protocol::OK;
}

paxos_protocol::status
acceptor::decidereq(std::string src, paxos_protocol::decidearg a, int &r)
{

  // handle an decide message from proposer

  ScopedLock ml(&pxs_mutex);
  
  if(a.instance <= instance_h){//ignore the request
    return paxos_protocol::OK;
  }

  if(a.instance == instance_h + 1){ //commit
    commit_wo(a.instance, a.v);
  }else{ //a.instance > (instance_h + 1) TODO: is it even possible?
    assert(false);
  }

  return paxos_protocol::OK;
}

void
acceptor::commit_wo(unsigned instance, std::string value)
{
  //assume pxs_mutex is held
  printf("acceptor::commit: instance=%d has v= %s\n", instance, value.c_str());
  if (instance > instance_h) {
    printf("commit: highestaccepteinstance = %d\n", instance);
    values[instance] = value;
    l->loginstance(instance, value);
    instance_h = instance;
    n_h.n = 0;
    n_h.m = me;
    n_a.n = 0;
    n_a.m = me;
    v_a.clear();
    if (cfg) {
      pthread_mutex_unlock(&pxs_mutex);
      cfg->paxos_commit(instance, value);
      pthread_mutex_lock(&pxs_mutex);
    }
  }
}

void
acceptor::commit(unsigned instance, std::string value)
{
  pthread_mutex_lock(&pxs_mutex);
  commit_wo(instance, value);
  pthread_mutex_unlock(&pxs_mutex);
}

std::string
acceptor::dump()
{
  return l->dump();
}

void
acceptor::restore(std::string s)
{
  l->restore(s);
  l->logread();
}



// For testing purposes

// Call this from your code between phases prepare and accept of proposer
void
proposer::breakpoint1()
{
  if (break1) {
    printf("Dying at breakpoint 1!\n");
    exit(1);
  }
}

// Call this from your code between phases accept and decide of proposer
void
proposer::breakpoint2()
{
  if (break2) {
    printf("Dying at breakpoint 2!\n");
    exit(1);
  }
}

void
proposer::breakpoint(int b)
{
  if (b == 3) {
    printf("Proposer: breakpoint 1\n");
    break1 = true;
  } else if (b == 4) {
    printf("Proposer: breakpoint 2\n");
    break2 = true;
  }
}
