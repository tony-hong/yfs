/*
 * receive request from fuse and call methods of yfs_client
 *
 * started life as low-level example in the fuse distribution.  we
 * have to use low-level interface in order to get i-numbers.  the
 * high-level interface only gives us complete paths.
 */

#include <fuse_lowlevel.h>
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <string.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <assert.h>
#include <arpa/inet.h>
#include "yfs_client.h"

int myid;
yfs_client *yfs;

int id() { 
  return myid;
}

#define min(x, y) ((x) < (y) ? (x) : (y))

struct dirbuf {
    char *p;
    size_t size;
};

void dirbuf_add(struct dirbuf *b, const char *name, fuse_ino_t ino)
{
    struct stat stbuf;
    size_t oldsize = b->size;
    b->size += fuse_dirent_size(strlen(name));
    b->p = (char *) realloc(b->p, b->size);
    memset(&stbuf, 0, sizeof(stbuf));
    stbuf.st_ino = ino;
    fuse_add_dirent(b->p + oldsize, name, &stbuf, b->size);
}

int reply_buf_limited(fuse_req_t req, const char *buf, size_t bufsize,
          off_t off, size_t maxsize)
{
  if ((size_t)off < bufsize)
    return fuse_reply_buf(req, buf + off, min(bufsize - off, maxsize));
  else
    return fuse_reply_buf(req, NULL, 0);
}

yfs_client::status
getattr(yfs_client::inum inum, struct stat &st)
{
  yfs_client::status ret;

  bzero(&st, sizeof(st));

  st.st_ino = inum;
  printf("getattr %016llx %d\n", inum, yfs->isfile(inum));
  if(yfs->isfile(inum)){
     yfs_client::fileinfo info;
     ret = yfs->getfile(inum, info);
     if(ret != yfs_client::OK)
       return ret;
     st.st_mode = S_IFREG | 0666;
     st.st_nlink = 1;
     st.st_atime = info.atime;
     st.st_mtime = info.mtime;
     st.st_ctime = info.ctime;
     st.st_size = info.size;
     printf("   getattr -> %llu\n", info.size);
   } else {
     yfs_client::dirinfo info;
     ret = yfs->getdir(inum, info);
     if(ret != yfs_client::OK)
       return ret;
     st.st_mode = S_IFDIR | 0777;
     st.st_nlink = 2;
     st.st_atime = info.atime;
     st.st_mtime = info.mtime;
     st.st_ctime = info.ctime;
     printf("   getattr -> %lu %lu %lu\n", info.atime, info.mtime, info.ctime);
   }
   return yfs_client::OK;
}


yfs_client::status
setfilesize(yfs_client::inum inum, struct stat *attr, struct stat &st)
{
  yfs_client::status ret = yfs_client::OK;

  ret = getattr(inum, st);
  if(ret != yfs_client::OK){
    return ret;
  }

  st.st_size = attr->st_size;

  yfs_client::fileinfo info;
  //TODO: cast problem?
  info.atime = st.st_atime;
  info.ctime = st.st_ctime;
  info.mtime = st.st_mtime;
  info.size = st.st_size;

  ret = yfs->setfile(inum, info);
  return ret;
}

// example
void
fuseserver_getattr(fuse_req_t req, fuse_ino_t ino,
          struct fuse_file_info *fi)
{
    yfs->yfs_lock(ino);

    struct stat st;
    yfs_client::inum inum = ino; // req->in.h.nodeid;
    yfs_client::status ret;

    ret = getattr(inum, st);
    if(ret != yfs_client::OK){
      fuse_reply_err(req, ENOENT);
    } else {
      fuse_reply_attr(req, &st, 0);
    }
    yfs->yfs_unlock(ino);
    return;
}

void
fuseserver_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr, int to_set, struct fuse_file_info *fi)
{

  yfs->yfs_lock(ino);

  printf("fuseserver_setattr 0x%x\n", to_set);
  if (FUSE_SET_ATTR_SIZE & to_set) {
    //We only support changing the size attr
    printf("   fuseserver_setattr set size to %zu\n", attr->st_size);

    struct stat st;

    //Only file has size attr
    assert(yfs->isfile(ino));
    
    // You fill this in
    if (setfilesize(ino, attr, st) == yfs_client::OK){
      fuse_reply_attr(req, &st, 0);
    } else {
      fuse_reply_err(req, ENOENT);
    }
  } else {
    fuse_reply_err(req, ENOSYS);
  }
  yfs->yfs_unlock(ino);
  return;
}

void
fuseserver_read(fuse_req_t req, fuse_ino_t ino, size_t size,
      off_t off, struct fuse_file_info *fi)
{
  // You fill this in

  //We can only read conetent in a file (not in a dir)

  yfs->yfs_lock(ino);

    assert(yfs->isfile(ino));

    std::string buf;

  if (yfs->getcontent(ino,buf) == yfs_client::OK){

    assert(0 <= off);
    assert((unsigned)off <= buf.size());


    reply_buf_limited(req, buf.c_str(), buf.size(), off, size);

    //fuse_reply_buf(req, buf.c_str(), size);
  }else{
    fuse_reply_err(req, ENOSYS);
  }
  yfs->yfs_unlock(ino);
  return;
}

void
fuseserver_write(fuse_req_t req, fuse_ino_t ino,
  const char *buf, size_t size, off_t off,
  struct fuse_file_info *fi)
{
  // You fill this in
  yfs->yfs_lock(ino);

  size_t bytes_written = size;

  //First, get the strbuf from extent server
  std::string strbuf;
  if(yfs->getcontent(ino,strbuf) != yfs_client::OK){
    fuse_reply_err(req, ENOENT);
    yfs->yfs_unlock(ino);
    return;
  }

  //Second, get the buf we need to write into strbu
  std::string wbuf(buf,size);
  //resize strbuf if needed
  if((off+size) > strbuf.size()){
    strbuf.resize(off+size);
    assert(strbuf.size() == (off+size));
  }

  //Third, merge the wbuf with strbuf
  strbuf.replace(strbuf.begin() + off, strbuf.begin() + off + size, wbuf.begin(), wbuf.end());

  //Fourth, write the new strbuf back into extentserver
   if(yfs->putcontent(ino,strbuf) != yfs_client::OK){
    fuse_reply_err(req, ENOSYS);
  } else {
    fuse_reply_write(req, bytes_written);
  }

  yfs->yfs_unlock(ino);
  return;
}

yfs_client::status
fuseserver_createhelper(fuse_ino_t parent, const char *name,
     mode_t mode, struct fuse_entry_param *e)
// fuse_ino_t : unsigned long  (MacOS, Ubuntu)
// mode_t     : unsigned short (MacOS)
// fuse_entry_param : struct {fuse_ino_t, stat(stat defined in system fs ), ...} (MacOS)
{
// TODO: TEST
  int r = yfs_client::OK;
  // fuse_ino_t fuse_ino;

  struct stat st;
  
  

  yfs_client::inum file_ino;
  if (yfs->create(parent, name, file_ino, 1) != yfs_client::OK){
    r = yfs_client::IOERR;
    goto release;
  }

  // fuse_ino = (fuse_ino_t)(file_ino & 0xFFFFFFFFUL);



  if (getattr(file_ino, st) != yfs_client::OK){
    r = yfs_client::IOERR;
    goto release;
  }

  e->attr = st;
  e->ino = (fuse_ino_t)file_ino;
  e->attr_timeout = 0.0;
  e->entry_timeout = 0.0;
  e->generation = 0;

release:
  
  return r;
}

void
fuseserver_create(fuse_req_t req, fuse_ino_t parent, const char *name,
   mode_t mode, struct fuse_file_info *fi)
{

  yfs->yfs_lock(parent);

  struct fuse_entry_param e;

  printf("\tcreate: parent(%08lx), name(%s), mode(%o)\n", parent, name, mode);

  if( fuseserver_createhelper( parent, name, mode, &e ) == yfs_client::OK ) {
    fuse_reply_create(req, &e, fi);
  } else {
    fuse_reply_err(req, ENOENT);
  }
  yfs->yfs_unlock(parent);
  return;

}

void fuseserver_mknod( fuse_req_t req, fuse_ino_t parent, 
    const char *name, mode_t mode, dev_t rdev ) {

  yfs->yfs_lock(parent);
  struct fuse_entry_param e;
  
  printf("\tcreate: parent(%08lx), name(%s), mode(%o)\n", parent, name, mode);
  
  if( fuseserver_createhelper( parent, name, mode, &e ) == yfs_client::OK ) {
    fuse_reply_entry(req, &e);
  } else {
    fuse_reply_err(req, ENOENT);
  }
  yfs->yfs_unlock(parent);
  return;

}

void
fuseserver_lookup(fuse_req_t req, fuse_ino_t parent, const char *name)
{
  struct fuse_entry_param e;
  bool found = false;

  e.attr_timeout = 0.0;
  e.entry_timeout = 0.0;

  yfs->yfs_lock(parent);
  // Look up the file named `name' in the directory referred to by
  // `parent' in YFS. If the file was found, initialize e.ino and
  // e.attr appropriately.
// TODO: TEST
  yfs_client::inum ino;
  //yfs_client::fileinfo file_info;

  std::string file_name(name);
  std::string file_buf;

  //parent must be a dir.
  assert(yfs->isdir(parent));
  
  printf("\t lookup: parent(%08lx), name(%s)\n", parent, name);
  if (yfs->lookup(parent, file_name, ino) == yfs_client::OK)
  {
      printf("\tfuse.cc parent(%08lx), name(%s),ino(%08llx) found\n", parent, name, ino);
      e.ino = ino;

      yfs->yfs_lock(ino);
      assert(getattr(ino, e.attr) == yfs_client::OK);

      found = true;
      yfs->yfs_unlock(ino);
  }

  if (found){
    fuse_reply_entry(req, &e);
  }
  else{
    fuse_reply_err(req, ENOENT);
  }
  yfs->yfs_unlock(parent);
  return;
}

void
fuseserver_readdir(fuse_req_t req, fuse_ino_t ino, size_t size,
          off_t off, struct fuse_file_info *fi)
{
  yfs->yfs_lock(ino);

  yfs_client::inum inum = ino; // req->in.h.nodeid;
  struct dirbuf b;
  yfs_client::dirent e;

  printf("fuseserver_readdir\n");

  if(!yfs->isdir(inum)){
    fuse_reply_err(req, ENOTDIR);
    yfs->yfs_unlock(ino);
    return;
  }

  memset(&b, 0, sizeof(b));

// TODO: TEST
   // fill in the b data structure using dirbuf_add
  yfs_client::dirmap m;
  if (yfs->getdirmap(inum, m) == yfs_client::OK){
    yfs_client::dirmap::iterator it;
    foreach(m, it){
      std::string name (it->first);
      yfs_client::inum i = it->second;
      dirbuf_add(&b, name.c_str(), i);

    }
  }

   reply_buf_limited(req, b.p, b.size, off, size);
   free(b.p);
   yfs->yfs_unlock(ino);
 }


void
fuseserver_open(fuse_req_t req, fuse_ino_t ino,
     struct fuse_file_info *fi)
{
// TODO: TEST
  yfs_client::fileinfo fin;

  yfs->yfs_lock(ino);

  if(yfs->getfile(ino, fin) != yfs_client::OK){
    fuse_reply_err(req, ENOSYS);
  } else {
    fuse_reply_open(req, fi);
  }
  
  yfs->yfs_unlock(ino);
  return;
}

void
fuseserver_mkdir(fuse_req_t req, fuse_ino_t parent, const char *name,
     mode_t mode)
{

// TODO: TEST

  yfs->yfs_lock(parent);

  struct fuse_entry_param e;
  struct stat st;
  
  yfs_client::inum dir_ino;

  if (yfs->create(parent, name, dir_ino, 0) != yfs_client::OK){
    fuse_reply_err(req, ENOSYS);
    yfs->yfs_unlock(parent);
    return;
  }

  // fuse_ino = (fuse_ino_t)(file_ino & 0xFFFFFFFFUL);

  if (getattr(dir_ino, st) != yfs_client::OK){
    fuse_reply_err(req, ENOENT);
    yfs->yfs_unlock(parent);
    return;
  }
  
  e.attr = st;
  e.ino = (fuse_ino_t)dir_ino;
  e.attr_timeout = 0.0;
  e.entry_timeout = 0.0;
  e.generation = 0;

  fuse_reply_entry(req, &e);
  yfs->yfs_unlock(parent);
    return;
}

void
fuseserver_unlink(fuse_req_t req, fuse_ino_t parent, const char *name)
{
// TODO: TO TEST
  // Success: fuse_reply_err(req, 0);
  // Not found: fuse_reply_err(req, ENOENT);

  yfs->yfs_lock(parent);
  yfs_client::status r;
  r = yfs->remove(parent, name);
  if (r == yfs_client::NOENT){
    fuse_reply_err(req, ENOENT);
  } else if (r == yfs_client::OK){
    fuse_reply_err(req, 0);
  } else {
    fuse_reply_err(req, ENOSYS);
  }
  yfs->yfs_unlock(parent);
  return;
}

// example
void
fuseserver_statfs(fuse_req_t req)
{
  struct statvfs buf;

  printf("statfs\n");

  memset(&buf, 0, sizeof(buf));

  buf.f_namemax = 255;
  buf.f_bsize = 512;

  fuse_reply_statfs(req, &buf);
}

struct fuse_lowlevel_ops fuseserver_oper;

int
main(int argc, char *argv[])
{
  char *mountpoint = 0;
  int err = -1;
  int fd;

  setvbuf(stdout, NULL, _IONBF, 0);

  if(argc != 4){
    fprintf(stderr, "Usage: yfs_client <mountpoint> <port-extent-server> <port-lock-server>\n");
    exit(1);
  }
  mountpoint = argv[1];

  srandom(getpid());

  myid = random();

  yfs = new yfs_client(argv[2], argv[3]);

  fuseserver_oper.getattr    = fuseserver_getattr;
  fuseserver_oper.statfs     = fuseserver_statfs;
  fuseserver_oper.readdir    = fuseserver_readdir;
  fuseserver_oper.lookup     = fuseserver_lookup;
  fuseserver_oper.create     = fuseserver_create;
  fuseserver_oper.mknod      = fuseserver_mknod;
  fuseserver_oper.open       = fuseserver_open;
  fuseserver_oper.read       = fuseserver_read;
  fuseserver_oper.write      = fuseserver_write;
  fuseserver_oper.setattr    = fuseserver_setattr;
  fuseserver_oper.unlink     = fuseserver_unlink;
  fuseserver_oper.mkdir      = fuseserver_mkdir;

  const char *fuse_argv[20];
  int fuse_argc = 0;
  fuse_argv[fuse_argc++] = argv[0];
#ifdef __APPLE__
  fuse_argv[fuse_argc++] = "-o";
  fuse_argv[fuse_argc++] = "nolocalcaches"; // no dir entry caching
  fuse_argv[fuse_argc++] = "-o";
  fuse_argv[fuse_argc++] = "daemon_timeout=86400";
#endif

  // everyone can play, why not?
  //fuse_argv[fuse_argc++] = "-o";
  //fuse_argv[fuse_argc++] = "allow_other";

  fuse_argv[fuse_argc++] = mountpoint;
  fuse_argv[fuse_argc++] = "-d";

  fuse_args args = FUSE_ARGS_INIT( fuse_argc, (char **) fuse_argv );
  int foreground;
  int res = fuse_parse_cmdline( &args, &mountpoint, 0 /*multithreaded*/, 
        &foreground );
  if( res == -1 ) {
    fprintf(stderr, "fuse_parse_cmdline failed\n");
    return 0;
  }
  
  args.allocated = 0;

  fd = fuse_mount(mountpoint, &args);
  if(fd == -1){
    fprintf(stderr, "fuse_mount failed\n");
    exit(1);
  }

  struct fuse_session *se;

  se = fuse_lowlevel_new(&args, &fuseserver_oper, sizeof(fuseserver_oper),
       NULL);
  if(se == 0){
    fprintf(stderr, "fuse_lowlevel_new failed\n");
    exit(1);
  }

  struct fuse_chan *ch = fuse_kern_chan_new(fd);
  if (ch == NULL) {
    fprintf(stderr, "fuse_kern_chan_new failed\n");
    exit(1);
  }

  fuse_session_add_chan(se, ch);
  // err = fuse_session_loop_mt(se);   // FK: wheelfs does this; why?
  err = fuse_session_loop(se);
    
  fuse_session_destroy(se);
  close(fd);
  fuse_unmount(mountpoint);

  return err ? 1 : 0;
}
