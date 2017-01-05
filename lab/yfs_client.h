#ifndef yfs_client_h
#define yfs_client_h

#include <string>
//#include "yfs_protocol.h"
#include "extent_client.h"
#include <map>

#include "lock_protocol.h"
#include "lock_client.h"
#include "lock_client_cache.h"

#define foreach(container,it) \
    for(typeof((container).begin()) it = (container).begin();it!=(container).end();++it)




class yfs_lock_release_user : public lock_release_user{
private:
    extent_client *ec;
public:
    yfs_lock_release_user(extent_client *ec_) : ec(ec_) {};
    void dorelease(lock_protocol::lockid_t lid)
    {
        ec->flush(lid);
    }
};


class yfs_client {
  extent_client *ec;
  lock_client *lc;
  lock_release_user *lu;
 public:

  // unique identifier, 64-bit identifier
  typedef unsigned long long inum;
  enum xxstatus { OK, RPCERR, NOENT, IOERR, FBIG };
  typedef int status;
  typedef std::map<std::string, inum> dirmap;

  struct fileinfo {
    unsigned long long size;
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirinfo {
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirent {
    std::string name;
    unsigned long long inum;
  };

 private:
  static std::string filename(inum);
  static inum n2i(std::string);
  static int serialize(const dirmap &, std::string &);
  static int deserialize(const std::string &, dirmap &);
  static unsigned long long llrand(unsigned int );

 public:
  yfs_client(std::string, std::string);
  ~yfs_client();
  
  bool isfile(inum);
  bool isdir(inum);
  // inum ilookup(inum, std::string);

  int getfile(inum, fileinfo &);
  int setfile(inum, const fileinfo &);
  int getdir(inum, dirinfo &);
  
  // obtain content from content map
  int getcontent(inum, std::string &);
  int getdirmap(inum, dirmap &);
  int lookup(inum , std::string, inum &);
  int putcontent(inum, const std::string &);
  int putdirmap(inum, const dirmap &);
  int create(inum, const char *, inum &, int);
  int remove(inum, const char *);
  int remove_recur(inum);

  //lock and unlock
  int yfs_lock(inum);
  int yfs_unlock(inum);


};

#endif 
