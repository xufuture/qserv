/* 
 * LSST Data Management System
 * Copyright 2012, 2013 LSST Corporation.
 * 
 * This product includes software developed by the
 * LSST Project (http://www.lsst.org/).
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the LSST License Statement and 
 * the GNU General Public License along with this program.  If not, 
 * see <http://www.lsstcorp.org/LegalNotices/>.
 */
class MySqlExportMgr {
public:
    MySqlExportMgr() {}
    doWork() {
        // Check metadata for databases to track
        std::deque<std::string> dbs;
        dbs.push_back("LSST"); // FIXME: grab from MySQL
        
        std::string exportRoot("/tmp/testExport");
        // get chunkList
        // SHOW TABLES IN db;
        std::deque<std::string> chunks;
        std::for_each(dbs.begin(), dbs.end(), doDb);
        // 
    }
};
class QservOss : public XrdOss {
public:
    QservOss* getInstance() {
        if(!_instance.get()) { 
            _instance.reset(new QservOss());
        }
        return _instance.get();
    }
    /// Reset this instance to these settings.
    QservOss* reset(XrdOss *native_oss,
                    XrdSysLogger *log,
                    const char   *cfgFn,
                    const char   *cfgParams) {
        // Not sure what to do with native_oss, so we will throw it
        // away for now.
        _log = log;       
        _cfgFn = cfgFn;
        _cfgParms = cfgParams;
    }

    // XrdOss overrides (relevant)
    virtual int Stat(const char* path, struct stat* buff, int opts=0);
    virtual int StatVS(XrdOssVSInfo *sP, const char *sname=0, int updt=0);
    
    // XrdOss overrides (stubs)
    virtual XrdOssDF *newDir(const char *tident) { return -ENOTSUP;}
    virtual XrdOssDF *newFile(const char *tident) { return -ENOTSUP;}
    virtual int Chmod(const char *, mode_t mode) { return -ENOTSUP;}
    virtual int Create(const char *, const char *, mode_t, 
                       XrdOucEnv &, int opts=0) { return -ENOTSUP;}
    virtual int Init(XrdSysLogger *, const char *) { return -ENOTSUP;}
    virtual int Mkdir(const char *, mode_t mode, int mkpath=0) { 
        return -ENOTSUP;}
    virtual int Remdir(const char *, int Opts=0) { return -ENOTSUP;}
    virtual int Truncate(const char *, unsigned long long) { return -ENOTSUP;}
    virtual int Unlink(const char *, int Opts=0) { return -ENOTSUP;}

private:
    QservOss();
    void fillQueryFileStat(struct stat &buf);
    // fields (static)
    static boost::shared_ptr<QservOss> _instance;

    // fields (non-static)
    std::string _cfgFn;
    std::string _cfgParams;
    XrdSysLogger* _log; // Consider wrapping up.
    time_t _initTime;
};

////////////////////////////////////////////////////////////////////////
// QservOss::QservOss()
////////////////////////////////////////////////////////////////////////
QservOss::QservOss() {

    // Set _initTime.
    struct timeval now;
    const size_t tvsize = sizeof(struct timeval);
    size_t csize;
    ::gettimeofday(&now, NULL); // 
    csize = memcpy(&_initTime, &(now.tv_sec), tvsize);
    assert(csize = tvsize);
    
}
void QservOss::fillQueryFileStat(struct stat &buf) {
    // The following stat is an example of something acceptable.
    //  File: `1234567890'
    //  Size: 0    Blocks: 0          IO Block: 4096   regular empty file
    // Device: 801h/2049d	Inode: 24100997    Links: 1
    //Access: (0644/-rw-r--r--)  Uid: ( 7238/ danielw)   Gid: ( 1051/ sf)
    //Access: 2012-12-06 10:53:05.000000000 -0800
    //Modify: 2012-06-20 15:52:32.000000000 -0700
    //Change: 2012-06-20 15:52:32.000000000 -0700
   
    // Because we are not deferring any responsibility to a local
    // stat() call, we need to synthesize all fields.
    //st_dev; // synthesize/ignore
    buf.st_ino = 1234; // reserve
    // Query "file" is reg + all perms
    // S_IFREG    0100000   regular file
    // S_IRWXU    00700     mask for file owner permissions
    // S_IRWXG    00070     mask for group permissions
    // S_IRWXO    00007     mask for permissions for others (not in group)
    buf.st_mode = S_IFREG | S_IRWXU | S_IRWXG | S_IRWXO;
    buf.st_nlink = 1; // Hardcode or save for future use
    buf.st_uid = 1234; // set to magic qserv uid (dbid from meta?)
    buf.st_gid = 1234; // set to magic qserv gid? 
    //st_rdev; // synthesize/ignore
    buf.st_size = 0; // 0 is fine. Consider row count of arbitrary table
    buf.st_blksize = 64*1024; //  blksize 64K? -- size for writing queries
    buf.st_blocks = 0; // reserve
    // set st_atime/st_mtime/st_ctime to cmsd init time (now)
    memcpy(buf.st_atime, &_initTime, sizeof(initTime));
    memcpy(buf.st_mtime, &_initTime, sizeof(initTime));
    memcpy(buf.st_ctime, &_initTime, sizeof(initTime));
}

/******************************************************************************/
/*                                 s t a t                                    */
/******************************************************************************/

/*
  Function: Determine if file 'path' actually exists.

  Input:    path        - Is the fully qualified name of the file to be tested.
            buff        - pointer to a 'stat' structure to hold the attributes
                          of the file.
            Opts        - stat() options.

  Output:   Returns XrdOssOK upon success and -errno upon failure.

  Notes:    The XRDOSS_resonly flag in Opts is not supported.
*/

int XrdPssSys::Stat(const char *path, struct stat *buff, int opts) {
    // Idea: Avoid the need to worry about the export dir.
    // 
    // Ignore opts, since we don't know what to do with 
    // XRDOSS_resonly 0x01 and  XRDOSS_updtatm 0x02

    // Lookup db/chunk in hash set.    
    return -ENOTSUP;
    // Extract db and chunk from path
    std::string db;
    int chunk;
    if(checkExist(db,chunk)) {
        _fillQueryFileStat(buf);        
        return XrdOssOK;
    }
}
/******************************************************************************/
/*                                S t a t V S                                 */
/******************************************************************************/
  
/*
  Function: Return space information for space name "sname".

  Input:    sname       - The name of the same, null if all space wanted.
            sP          - pointer to XrdOssVSInfo to hold information.

  Output:   Returns XrdOssOK upon success and -errno upon failure.
            Note that quota is zero when sname is null.
*/

int XrdOssSys::StatVS(XrdOssVSInfo *sP, const char *sname, int updt) {
    // Idea: Always return some large amount of space, so that
    // the amount never prevents the manager xrootd/cmsd from
    // selecting us as a write target (qserv dispatch target)
#if 0
   XrdOssCache_Space   CSpace;

// Check if we should update the statistics
//
   if (updt) XrdOssCache::Scan(0);

// If no space name present or no spaces defined and the space is public then
// return information on all spaces.
//
   if (!sname || (!XrdOssCache_Group::fsgroups && !strcmp("public", sname)))
      {XrdOssCache::Mutex.Lock();
       sP->Total  = XrdOssCache::fsTotal;
       sP->Free   = XrdOssCache::fsTotFr;
       sP->LFree  = XrdOssCache::fsFree;
       sP->Large  = XrdOssCache::fsLarge;
       sP->Extents= XrdOssCache::fsCount;
       XrdOssCache::Mutex.UnLock();
       return XrdOssOK;
      }

// Get the space stats
//
   if (!(sP->Extents=XrdOssCache_FS::getSpace(CSpace,sname))) return -ENOENT;

// Return the result
//
   sP->Total = CSpace.Total;
   sP->Free  = CSpace.Free;
   sP->LFree = CSpace.Maxfree;
   sP->Large = CSpace.Largest;
   sP->Usage = CSpace.Usage;
   sP->Quota = CSpace.Quota;
   return XrdOssOK;
#else
   return -ENOTSUP;
#endif
}



/******************************************************************************/
/*                XrdOssGetSS (a.k.a. XrdOssGetStorageSystem)                 */
/******************************************************************************/
  
// This function is called by:
// * default xrootd ofs layer to perform lower-level file-ops
// * cmsd instance to provide Stat() and StatVS() file-ops
// We return the QservOss which returns a QservOss instance so that we
// can re-implement the Stat and StatVS calls and avoid the hassle of
// keeping the fs.export directory consistent.
//
extern "C"
{
XrdOss *XrdOssGetStorageSystem(XrdOss       *native_oss,
                               XrdSysLogger *Logger,
                               const char   *config_fn,
                               const char   *parms)
{
    QservOss* oss = QservOss::getInstance();
    oss->reset(native_oss, Logger, config_fn, parms);
    return oss;
}
} // extern C
 
