// -*- LSST-C++ -*-
/* 
 * LSST Data Management System
 * Copyright 2012 LSST Corporation.
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
// X is a ...

#ifndef LSST_QSERV_WORKER_QSERVOSS_H
#define LSST_QSERV_WORKER_QSERVOSS_H
#include "XrdOss/XrdOss.hh"
#include <deque>
#include <set>
#include <string>
#include <boost/shared_ptr.hpp>
// Forward
class XrdOss;
class XrdSysLogger;
class XrdOucEnv;


namespace lsst { namespace qserv { namespace worker {

class FakeOssDf : public XrdOssDF {
public:
    int Close(long long *retsz=0) { return XrdOssOK; }
    int Opendir(const char *) { return XrdOssOK; }
    int Readdir(char *buff, int blen) { return XrdOssOK; }

    // Constructor and destructor
    FakeOssDf() {}
    ~FakeOssDf() {}
};

class QservOss : public XrdOss {
public:
    typedef std::set<std::string> HashSet; // Convert to unordered_set (C++0x)
    static QservOss* getInstance() {
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
        _cfgParams = cfgParams;
    }

    // XrdOss overrides (relevant)
    virtual int Stat(const char* path, struct stat* buff, int opts=0);
    virtual int StatVS(XrdOssVSInfo *sP, const char *sname=0, int updt=0);
    
    // XrdOss overrides (stubs)
    virtual XrdOssDF *newDir(const char *tident) { return new FakeOssDf(); }
    virtual XrdOssDF *newFile(const char *tident) { return new FakeOssDf(); }
    virtual int Chmod(const char *, mode_t mode) { return -ENOTSUP;}
    virtual int Create(const char *, const char *, mode_t, 
                       XrdOucEnv &, int opts=0) { return -ENOTSUP;}
    virtual int Init(XrdSysLogger *, const char *) { return -ENOTSUP;}
    virtual int Mkdir(const char *, mode_t mode, int mkpath=0) { 
        return -ENOTSUP;}
    virtual int Remdir(const char *, int Opts=0) { return -ENOTSUP;}
    virtual int Truncate(const char *, unsigned long long) { return -ENOTSUP;}
    virtual int Unlink(const char *, int Opts=0) { return -ENOTSUP;}
    virtual int Rename(const char*, const char*) { return -ENOTSUP;}

    void refresh();
private:
    QservOss();
    void _fillQueryFileStat(struct stat &buf);
    bool _checkExist(std::string const& db, int chunk);

    // fields (static)
    static boost::shared_ptr<QservOss> _instance;
    boost::shared_ptr<HashSet> _hashSet;

    // fields (non-static)
    std::string _cfgFn;
    std::string _cfgParams;
    XrdSysLogger* _log; // Consider wrapping up.
    time_t _initTime;
};
}}} // namespace lsst::qserv::master


#endif //  LSST_QSERV_WORKER_QSERVOSS_H

