// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>


yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
  ec = new extent_client(extent_dst);
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
yfs_client::getdirent(inum dir_ino, std::string file_name, dirent &e)
{
  int r = OK;

  printf("getdirent %016llx\n", dir_ino);
  std::string buf;
  if (ec->get(dir_ino, buf) != extent_protocol::OK) {
    r = NOENT;
    goto release;
  }

  std::map<inum, std::string> dirmap;
  if (deserialize(buf, &dirmap) != OK){
    r = IOERR;
    goto release;
  }

  if (dirmap.find(file_name) == dirmap.end()){
    r = NOENT;
    goto release;
  }
  e.name = file_name;
  e.inum = dirmap[file_name];

 release:
  return r;
}



int
yfs_client::serialize(const std::map<std::string, inum> &dirmap, std::string &buf)
{
  int r = OK;
  unsigned int size = dirmap.size();
  buf.append((char *)&size, sizeof(unsigned int));
  foreach(dirmap, it)
  {
      size = it->first.size();
      buf.append(it->first.c_str(), it->first.size());
      buf.append((char *)&size, sizeof(unsigned int));
      buf.append((char *)&it->second, sizeof(inum));
  }
  return r;
}

int
yfs_client::deserialize(const std::string &buf, std::map<std::string, inum> &dirmap)
{
    int r = OK;
    const char* cbuf = buf.c_str();
    unsigned int size_buf = buf.size();
    unsigned int size_fl = *(unsigned int*)cbuf;
    unsigned int p = 0;
    unsigned int size_name;
    inum id;
    std::string name;

    if (size_fl * (sizeof(unsigned int) + sizeof(inum)) + 4 > size_buf)
    {
        r = IOERR;
        return r;
    }

    printf("deserialize size = %d\n", size);
    p += sizeof(unsigned int);
    for (unsigned int i = 0; i < size_fl; i++)
    {
        // make file name string
        name = std::string((cbuf + p), size_name);
        p += size_name;
        if (p >= size_buf + (i == size_fl - 1))
            return IOERR;

        // make size string
        size_name = *(unsigned int*)(cbuf + p);
        p += sizeof(unsigned int);
        if (p >= size_buf)
            return IOERR;

        // make inum string
        id = *(inum*)(cbuf + p);
        p += sizeof(inum);
        if (p >= size_buf)
            return IOERR;

        dirmap.push_back(std::make_pair<inum, std::string>(id, name));
        printf("id = %016llx name = %s\n", id, name.c_str());
    }
    return r;
}

