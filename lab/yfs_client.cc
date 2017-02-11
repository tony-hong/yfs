// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include "lock_client.h"
#include "lock_client_cache.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <random>



yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
  int randseed = uniformIntDistribution(rndgen);
  srand(time(NULL) ^ getpid() ^ randseed);
  ec = new extent_client(extent_dst);
  lu = new yfs_lock_release_user(ec);
  //lc = new lock_client(lock_dst);
  lc = new lock_client_cache(lock_dst, lu);

  // yfs_lock(1);
  // dirmap m;
  // m[""] = 1;
  // putdirmap(1, m);
  // yfs_unlock(1);
}

yfs_client::~yfs_client(){
  delete ec;
}

yfs_client::inum
yfs_client::n2i(std::string n)
{
  std::istringstream ist(n);
  unsigned long long finum;
  ist >> finum;
  return finum;
}

std::string
yfs_client::filename(inum inum)
{
  std::ostringstream ost;
  ost << inum;
  return ost.str();
}

bool
yfs_client::isfile(inum inum)
{
  if(inum & 0x80000000)
    return true;
  return false;
}

bool
yfs_client::isdir(inum inum)
{
  return ! isfile(inum);
}

int
yfs_client::getfile(inum inum, fileinfo &fin)
{
  int r = OK;


  printf("getfile %016llx\n", inum);
  extent_protocol::attr a;
  if (ec->getattr(inum, a) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }

  fin.atime = a.atime;
  fin.mtime = a.mtime;
  fin.ctime = a.ctime;
  fin.size = a.size;
  printf("getfile %016llx -> sz %llu\n", inum, fin.size);

 release:

  return r;
}

int
yfs_client::setfile(inum inum, const fileinfo &fin)
{

  printf("setfile %016llx\n", inum);
  assert(isfile(inum));
  extent_protocol::attr a;
  // TODO: may occur cast problem
  a.atime = fin.atime;
  a.ctime = fin.ctime;
  a.mtime = fin.mtime;
  a.size = fin.size;

  if (ec->setattr(inum, a) != extent_protocol::OK) {
    return IOERR;
  }

  return OK;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
  int r = OK;

  printf("getdir %016llx\n", inum);
  extent_protocol::attr a;
  if (ec->getattr(inum, a) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }
  din.atime = a.atime;
  din.mtime = a.mtime;
  din.ctime = a.ctime;

 release:
  return r;
}



int
yfs_client::getcontent(inum ino, std::string &buf){
  int r = OK;

  printf("getcontent %016llx\n", ino);

  if (ec->get(ino, buf) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }

release:
  return r;
}

int
yfs_client::getdirmap(inum dir_ino, dirmap &m){
  int r = OK;
  std::string buf;
  
  printf("getdirmap %016llx\n", dir_ino);

  if (getcontent(dir_ino, buf) != extent_protocol::OK) {
    printf("getdirmap: content not found!!!: %016llx\n", dir_ino);
    r = NOENT;
    goto release;
  }  

  if (deserialize(buf, m) != OK){
    printf("getdirmap: deserialize failed: %016llx\n", dir_ino);
    r = IOERR;
    goto release;
  }

 release:
  return r;
}

int
yfs_client::lookup(inum dir_ino, std::string file_name, inum & file_ino)
{
  int r = OK;

  dirmap dir_map;
  if (getdirmap(dir_ino, dir_map) != OK){
    printf("\t lookup: map not found!!!: parent(%08llx), name(%s)\n", dir_ino, file_name.c_str());
    r = IOERR;
    goto release;
  }

  if (dir_map.find(file_name) == dir_map.end()){
    printf("\t lookup: file not found!!!: parent(%08llx), name(%s)\n", dir_ino, file_name.c_str());
    r = NOENT;
    goto release;
  }
  file_ino = dir_map[file_name];

 release:
  return r;
}

int
yfs_client::putcontent(inum ino, const std::string &buf){
  int r = OK;
  printf("putcontent %016llx\n", ino);

  if (ec->put(ino, buf) != extent_protocol::OK) {
    r = FBIG;
    goto release;
  }

release:
  return r;
}

int
yfs_client::putdirmap(inum dir_ino, const dirmap &m){
  int r = OK;
  std::string buf;
  
  printf("putdirmap %016llx\n", dir_ino);

  dirinfo dir_info;
  if (getdir(dir_ino, dir_info) != OK) {
    printf("putdirmap: dir not found: %016llx\n", dir_ino);
    r = NOENT;
    goto release;
  }  

  if (serialize(m, buf) != OK){
    printf("putdirmap: serialize failed: %016llx\n", dir_ino);
    r = IOERR;
    goto release;
  }

  if (putcontent(dir_ino, buf) != OK){
    printf("putdirmap: putcontent failed: %016llx\n", dir_ino);
    r = IOERR;
    goto release;
  }

 release:
  return r;
}

int
yfs_client::create(inum parent, const char *name, inum & file_ino, int isfile){
  //WARNING: This implementation could cause problems, see warning below
  int r = OK;

  dirmap m;
  std::string buf;
  dirmap mp;
  std::string file_name(name);

  file_ino = (inum)llrand(isfile);

  yfs_lock(file_ino);

  printf("create  name = %s with id = %08llx in parent = %016llx,\n", file_name.c_str(), file_ino, parent);

  if (getdirmap(parent, m) != OK){
    printf("\t create: map not found!!!: parent(%016llx), name(%s)\n", parent, file_name.c_str());
    r = NOENT;
    goto release;
  }

  //check duplicate
  // if (m.find(file_name) != m.end()){
  //   printf("\t create: name duplicate: parent(%08llx), name(%s)\n", parent, file_name.c_str());
  //   r = NOENT;
  //   goto release;    
  // }

  m[file_name] = file_ino;

  if (putdirmap(parent, m) != OK){
    printf("\t create: putdirmap failed!!!: parent(%08llx), name(%s)\n", parent, file_name.c_str());
    r = IOERR;
    goto release;
  }

  if (isfile == 0){
    if (serialize(mp, buf) != OK){
      printf("create: serialize failed: %016llx\n", parent);
      r = IOERR;
      goto release;
    }
  }

  if (putcontent(file_ino, buf) != OK){
    printf("\t create: init file content failed!!!: parent(%08llx), name(%s)\n", parent, file_name.c_str());
    r = IOERR;
    goto release;
  }

  extent_protocol::attr a;
  if (ec->getattr(file_ino, a) != extent_protocol::OK) {
    printf("create: file not found: %016llx\n", file_ino);
    r = IOERR;
    goto release;
  }

  extent_protocol::attr a_par_old;
  if (ec->getattr(parent, a_par_old) != extent_protocol::OK) {
    printf("create: dir not found: %016llx\n", parent);
    r = NOENT;
    goto release;
  }

  extent_protocol::attr a_par;
  a_par.size = a_par_old.size;
  a_par.atime = a_par_old.atime;
  a_par.ctime = a.ctime;
  a_par.mtime = a.mtime;

  if (ec->setattr(parent, a_par) != extent_protocol::OK) {
    printf("\t create: failed!!!: parent(%08llx)", parent);
    r = IOERR;
    goto release;
  }

//WARNING: this implementation of create function may cause problems
  //by calling putcontent we give the new created file the newest timestamp
  //after putcontent, we call setattr for the dir the file is in, that gives the dir a new timestamp, which could be newer than the timestamp of the created file 
  //That is, after create, the new file and the dir could have different ctime, which may ok if we do not require it.
  //However, in the linux system, after we created a new file, the new file and the dir should have the same ctime
  //Thus, following code may cause assertion error: dirinfo din; assert(getdir(parent, din) == OK); assert(din.ctime == a.ctime);assert(din.mtime == a.mtime);

release:
  yfs_unlock(file_ino);
  return r;
}

int
yfs_client::remove(inum dir_ino, const char *name){
  int r = OK;

  dirmap m;
  inum ino;
  std::string file_name(name);

  if (getdirmap(dir_ino, m) != OK){
    printf("\t remove: map not found!!!: parent(%08llx), name(%s)\n", dir_ino, file_name.c_str());
    r = NOENT;
    goto release;
  }

  if (m.find(file_name) == m.end()){
    printf("\t remove: name not found!!!: parent(%08llx), name(%s)\n", dir_ino, file_name.c_str());
    r = NOENT;
    goto release;    
  }

  ino = m[file_name];
  if (remove_recur(ino) != OK){
    printf("\t remove: remove failed!!!: parent(%08llx), name(%s)\n", dir_ino, file_name.c_str());
    r = IOERR;
    goto release;
  }

  m.erase(file_name);
  if (putdirmap(dir_ino, m) != OK){
    printf("\t create: putdirmap failed!!!: parent(%08llx), name(%s)\n", dir_ino, file_name.c_str());
    r = IOERR;
    goto release;
  }  

release:
  return r;
}


int
yfs_client::remove_recur(inum ino){
  yfs_lock(ino);
  int r = OK;

  if(isdir(ino)){
    dirmap m;
    if (getdirmap(ino, m) != OK){
      printf("\t remove: map not found!!!: parent(%08llx)\n", ino);
      r = NOENT;
      goto release;
    }
    
    foreach(m, it){
      if (remove_recur(it->second) != OK){
        printf("\t remove: remove failed!!!: parent(%08llx)\n", ino);
        r = IOERR;
        goto release;
      }
      m.erase(it);
    }
    m.clear();
  }

  if (ec->remove(ino) != OK){
    printf("\t remove: remove failed!!!: parent(%08llx)\n", ino);
    r = IOERR;
    goto release;
  } 
release:
  yfs_unlock(ino);
  return r;  
}

int
yfs_client::yfs_lock(inum id){
  lc->acquire(id);
  return OK;
}

int
yfs_client::yfs_unlock(inum id){
  lc->release(id);
  return OK;
}



void split(const std::string &s, char delim, std::vector<std::string> &elems) {
    std::stringstream ss;
    ss.str(s);
    std::string item;
    while (std::getline(ss, item, delim)) {
        elems.push_back(item);
    }
}


int
yfs_client::serialize(const dirmap &dirmap, std::string &buf)
{
  int r = OK;
  
  std::map<std::string, inum>::const_iterator  it;


    for(it = dirmap.begin(); it != dirmap.end(); it++) {
        std::string f = it->first;

        std::stringstream ss;
        ss << it->second;
        std::string s = ss.str();

        std::string one_element = (f.append(";")).append(s);
        buf.append(one_element);
        buf.append(",");
        //printf( "the name is %s  and the value is  %s\n", it->first.c_str(), it->second.c_str() );
    }


    buf = buf.substr(0, buf.size()-1);



  return r;
}

int
yfs_client::deserialize(const std::string &buf, dirmap &dir_map)
{
    int r = OK;
    
    printf("deserialize %s\n", buf.c_str());

    char delim = ',';
    std::vector<std::string> elems;
    split(buf, delim, elems);

    for(std::vector<std::string>::size_type i = 0; i != elems.size(); i++) {
        std::string el_str = elems[i];
        //printf("one element %s \n", elems[i].c_str());
        char delim2 = ';';
        std::vector<std::string> pair_vector;
        split(el_str, delim2, pair_vector);
        assert(pair_vector.size() == 2);
        dir_map[pair_vector[0]] =  std::strtoll(pair_vector[1].c_str(),NULL,10);
    }

    return r;
}

//generate a 64bit (long long) number
unsigned long long 
yfs_client::llrand(unsigned int isfile) {
    int randnum = uniformIntDistribution(rndgen);
    return ((randnum % 0x7FFFFF) | (isfile << 31));
}
