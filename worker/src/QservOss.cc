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
#include "lsst/qserv/worker/QservOss.h"
#include <deque>
#include <string>
#include <algorithm>
#include <sstream>
#include <sys/time.h>

using lsst::qserv::worker::QservOss;

namespace {

inline void fillVSInfo(XrdOssVSInfo *sP) {
    assert(sP);
    // Fill with bogus large known values
    long long giga = 1000*1000*1000LL;
    sP->Total = giga*100; // 100G total
    sP->Free  = giga*99; // 99G free
    sP->LFree = giga*99; // 99G free in contiguous
    sP->Large = giga*99; // 99G in largest partition
    sP->Usage = giga*1; // 1G in use
    sP->Quota = giga*100; // 100G quota bytes
    
}
inline std::string makeKey(std::string const& db, int chunk) {
    std::stringstream ss;
    ss << db << chunk << "**key";
    return std::string(ss.str());
}
}

class MySqlExportMgr {
public:
    MySqlExportMgr() {}
    void doWork() {
        // Check metadata for databases to track
        std::deque<std::string> dbs;
        dbs.push_back("LSST"); // FIXME: grab from MySQL
        
        std::string exportRoot("/tmp/testExport");
        // get chunkList
        // SHOW TABLES IN db;
        std::deque<std::string> chunks;
        //std::for_each(dbs.begin(), dbs.end(), doDb(dbs));
        // 
    }
};

class TableListing {
    // should reuse an idle sql connection
public:
    void reset() {
    }
    std::string getDb() const {
    }
    std::string getTable() const {
    }
};
class MetaClient {
public:
    MetaClient() {}

    void reset() {
        // 
        // for db in should_track:
        // get table listing for db
        // 
    }
};
#if 0
/// generates export directory paths for every chunk in every database served
bool 
qWorker::Metadata::generateExportPaths(std::string const& baseDir,
                                       SqlConnection& sqlConn,
                                       SqlErrorObject& errObj,
                                       std::vector<std::string>& exportPaths) {
    if (!sqlConn.selectDb(_metadataDbName, errObj)) {
        return false;
    }
    std::string sql = "SELECT dbName, partitionedTables FROM Dbs";
    SqlResults results;
    if (!sqlConn.runQuery(sql, results, errObj)) {
        return errObj.addErrMsg("Failed to execute: " + sql);
    }
    std::vector<std::string> dbs;
    std::vector<std::string> pts; // each string = comma separated list
    if (!results.extractFirst2Columns(dbs, pts, errObj)) {
        return errObj.addErrMsg("Failed to receive results from: " + sql);
    }
    int i, s = dbs.size();
    for (i=0; i<s ; i++) {
        std::string dbName = dbs[i];
        std::string tableList = pts[i];
        if (!generateExportPathsForDb(baseDir, dbName, tableList, 
                                      sqlConn, errObj, exportPaths)) {
            std::stringstream ss;
            ss << "Failed to create export dir for baseDir="
               << baseDir << ", dbName=" << dbName << ", tableList=" 
               << tableList << std::endl;
            return errObj.addErrMsg(ss.str());
        }
    }
    return true;
}
#endif
////////////////////////////////////////////////////////////////////////
// QservOss static
////////////////////////////////////////////////////////////////////////
boost::shared_ptr<QservOss> QservOss::_instance;
////////////////////////////////////////////////////////////////////////
// QservOss::QservOss()
////////////////////////////////////////////////////////////////////////
QservOss::QservOss() {
    // Set _initTime.
    struct timeval now;
    const size_t tvsize = sizeof(struct timeval);
    void* res;
    ::gettimeofday(&now, NULL); // 
    res = memcpy(&_initTime, &(now.tv_sec), tvsize);
    assert(res == &_initTime);
    
}

void QservOss::_fillQueryFileStat(struct stat &buf) {
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
    memcpy(&(buf.st_atime), &_initTime, sizeof(_initTime));
    memcpy(&(buf.st_mtime), &_initTime, sizeof(_initTime));
    memcpy(&(buf.st_ctime), &_initTime, sizeof(_initTime));
}

bool QservOss::_checkExist(std::string const& db, int chunk) {
    std::string key = makeKey(db, chunk);
    assert(_hashSet.get());
    HashSet::const_iterator i = _hashSet->find(key);
    return (i != _hashSet->end());
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
int QservOss::Stat(const char *path, struct stat *buff, int opts) {
    // Idea: Avoid the need to worry about the export dir.
    // 
    // Ignore opts, since we don't know what to do with 
    // XRDOSS_resonly 0x01 and  XRDOSS_updtatm 0x02

    // Lookup db/chunk in hash set.    
    return -ENOTSUP;
    // Extract db and chunk from path
    std::string db;
    int chunk;
    if(_checkExist(db,chunk)) {
        _fillQueryFileStat(*buff);
        return XrdOssOK;
    } else {
        return -ENOENT;
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

int QservOss::StatVS(XrdOssVSInfo *sP, const char *sname, int updt) {
    // Idea: Always return some large amount of space, so that
    // the amount never prevents the manager xrootd/cmsd from
    // selecting us as a write target (qserv dispatch target)
    fillVSInfo(sP);
    return XrdOssOK;
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
 
