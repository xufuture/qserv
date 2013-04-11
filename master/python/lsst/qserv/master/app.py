# 
# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
# 
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the LSST License Statement and 
# the GNU General Public License along with this program.  If not, 
# see <http://www.lsstcorp.org/LegalNotices/>.
#

# app module for lsst.qserv.master
#
# The app module can be thought-of as the top-level code module for
# the qserv frontend's functionality.  It implements the "interesting"
# code that accepts an incoming query, parses it, generates
# subqueries, dispatches to workers, collects results, and returns to
# the caller, marshalling code from other modules as necessary.
# 
# This is the  "high-level application logic" and glue of the qserv
# master/frontend.  
#
# Warning: Older code for older/alternate parsing models and
# older/alternate dispatch/collection models remains here, in
# anticipation of supporting performance investigation.  The most
# current functionality can be understood by examining the
# HintedQueryAction class, QueryBabysitter class, and other related
# classes. 
# 

# Standard Python imports
import errno
import hashlib
from itertools import chain, imap
import os
import cProfile as profile
import pstats
import random
import re
from subprocess import Popen, PIPE
import sys
import threading
import time
from string import Template

# Package imports
import metadata
import lsst.qserv.master.config
from lsst.qserv.master import geometry
from lsst.qserv.master.geometry import SphericalBox
from lsst.qserv.master.geometry import SphericalConvexPolygon, convexHull
from lsst.qserv.master.db import TaskDb as Persistence
from lsst.qserv.master.db import Db
from lsst.qserv.master import protocol
from lsst.qserv.master.spatial import getSpatialConfig, getRegionFactory

# SWIG'd functions

# xrdfile - raw xrootd access
from lsst.qserv.master import xrdOpen, xrdClose, xrdRead, xrdWrite
from lsst.qserv.master import xrdLseekSet, xrdReadStr
from lsst.qserv.master import xrdReadToLocalFile, xrdOpenWriteReadSaveClose

from lsst.qserv.master import charArray_frompointer, charArray

# transaction
from lsst.qserv.master import TransactionSpec

# Dispatcher 
from lsst.qserv.master import newSession, discardSession
from lsst.qserv.master import setupQuery, getSessionError
from lsst.qserv.master import getConstraints, addChunk, ChunkSpec
from lsst.qserv.master import configureSessionMerger3, submitQuery3


from lsst.qserv.master import submitQuery, submitQueryMsg
from lsst.qserv.master import initDispatcher
from lsst.qserv.master import tryJoinQuery, joinSession
from lsst.qserv.master import getQueryStateString, getErrorDesc
from lsst.qserv.master import SUCCESS as QueryState_SUCCESS
from lsst.qserv.master import pauseReadTrans, resumeReadTrans
# Parser
from lsst.qserv.master import ChunkMeta
from lsst.qserv.master import ChunkMapping, SqlSubstitution
# Merger
from lsst.qserv.master import TableMerger, TableMergerError, TableMergerConfig
from lsst.qserv.master import configureSessionMerger, getSessionResultName

# Experimental interactive prompt (not currently working)
import code, traceback, signal


# Constant, long-term, this should be defined differently
dummyEmptyChunk = 1234567890


def debug(sig, frame):
    """Interrupt running process, and provide a python prompt for
    interactive debugging."""
    d={'_frame':frame}         # Allow access to frame object.
    d.update(frame.f_globals)  # Unless shadowed by global
    d.update(frame.f_locals)

    i = code.InteractiveConsole(d)
    message  = "Signal recieved : entering python shell.\nTraceback:\n"
    message += ''.join(traceback.format_stack(frame))
    i.interact(message)

def listen():
    signal.signal(signal.SIGUSR1, debug)  # Register handler
listen()


######################################################################
## Methods
######################################################################
## MySQL interface helpers
def computeShortCircuitQuery(query, conditions):
    if query == "select current_user()":
        return ("qserv@%","","")
    return # not a short circuit query.

## Helpers
def getResultTable(tableName):
    """A convenience function that uses the mysql client to get quick 
    results."""
    # Minimal sanitizing
    tableName = tableName.strip()
    assert not filter(lambda x: x in tableName, ["(",")",";"])
    sqlCmd = "SELECT * FROM %s;" % tableName

    # Get config
    config = lsst.qserv.master.config.config
    socket = config.get("resultdb", "unix_socket")
    db = config.get("resultdb", "db")
    mysql = config.get("mysql", "mysqlclient")
    if not mysql:
        mysql = "mysql"

    # Execute
    cmdList = [mysql, "--socket", socket, db]
    p = Popen(cmdList, bufsize=0, 
              stdin=PIPE, stdout=PIPE, close_fds=True)
    (outdata,errdummy) = p.communicate(sqlCmd)
    p.stdin.close()
    if p.wait() != 0:
        return "Error getting table %s." % tableName, outdata
    return outdata

def loadEmptyChunks(filename):
    def tolerantInt(i):
        try:
            return int(i)
        except:
            return None
    empty = set()
    try:
        if filename:
            s = open(filename).read()
            empty = set(map(tolerantInt, s.split("\n")))
    except:
        print filename, "not found while loading empty chunks file."
        return None
    return empty
    
    
######################################################################
## Classes
######################################################################

class TaskTracker:
    def __init__(self):
        self.tasks = {}
        self.pers = Persistence()
        pass

    def track(self, name, task, querytext):
        # create task in db with name
        taskId = self.pers.addTask((None, querytext))
        self.tasks[taskId] = {"task" : task}
        return taskId
    
    def task(self, taskId):
        return self.tasks[taskId]["task"]

########################################################################

class SleepingThread(threading.Thread):
    def __init__(self, howlong=1.0):
        self.howlong=howlong
        threading.Thread.__init__(self)
    def run(self):
        time.sleep(self.howlong)

########################################################################
class QueryHintError(Exception):
    """An error in handling query hints"""
    def __init__(self, reason):
        self.reason = reason
    def __str__(self):
        return repr(self.reason)

def setupResultScratch():
    # Make sure scratch directory exists.
    cm = lsst.qserv.master.config
    c = lsst.qserv.master.config.config
    
    scratchPath = c.get("frontend", "scratch_path")
    try: # Make sure the path is there
        os.makedirs(scratchPath)
    except OSError, exc: 
        if exc.errno == errno.EEXIST:
            pass
        else: 
            raise cm.ConfigError("Bad scratch_dir")
    # Make sure we can read/write the dir.
    if not os.access(scratchPath, os.R_OK | os.W_OK):
        raise cm.ConfigError("No access for scratch_path(%s)" % scratchPath)
    return scratchPath

########################################################################    

class QueryBabysitter:
    """Watches over queries in-flight.  An instrument of query care that 
    can be used by a client.  Unlike the collater, it doesn't do merging.
    """
    def __init__(self, sessionId, queryHash, fixup, 
                 reportError=lambda e:None, resultName=""):
        self._sessionId = sessionId
        self._inFlight = {}
        # Scratch mgmt (Consider putting somewhere else)
        self._scratchPath = setupResultScratch()

        self._setupMerger(fixup, resultName) 
        self._reportError = reportError
        pass

    def _setupMerger(self, fixup, resultName):
        c = lsst.qserv.master.config.config
        dbSock = c.get("resultdb", "unix_socket")
        dbUser = c.get("resultdb", "user")
        dbName = c.get("resultdb", "db")        
        dropMem = c.get("resultdb","dropMem")

        mysqlBin = c.get("mysql", "mysqlclient")
        if not mysqlBin:
            mysqlBin = "mysql"

        mergeConfig = TableMergerConfig(dbName, resultName, 
                                        fixup,
                                        dbUser, dbSock, 
                                        mysqlBin, dropMem)
        configureSessionMerger(self._sessionId, mergeConfig)

    def pauseReadback(self):
        pauseReadTrans(self._sessionId)
        pass

    def resumeReadback(self):
        resumeReadTrans(self._sessionId)
        pass

    def submit(self, chunk, table, q):
        saveName = self._saveName(chunk)
        handle = submitQuery(self._sessionId, chunk, q, saveName, table)
        #print "Chunk %d to %s    (%s)" % (chunk, saveName, table)

    def submitMsg(self, db, chunk, msg, table):
        saveName = self._saveName(chunk)
        handle = submitQueryMsg(self._sessionId, db, chunk, msg, 
                                saveName, table)
        self._inFlight[chunk] = (handle, table)

    def finish(self):
        for (k,v) in self._inFlight.items():
            s = tryJoinQuery(self._sessionId, v[0])
            #print "State of", k, "is", getQueryStateString(s)

        s = joinSession(self._sessionId)
        if s != QueryState_SUCCESS:
            self._reportError(getErrorDesc(self._sessionId))
        print "Final state of all queries", getQueryStateString(s)
        
    def getResultTableName(self):
        ## Should do sanity checking to make sure that the name has been
        ## computed.
        return getSessionResultName(self._sessionId)

    def _getTimeStampId(self):
        unixtime = time.time()
        # Use the lower digits as pseudo-unique (usec, sec % 10000)
        # FIXME: is there a better idea?
        return str(int(1000000*(unixtime % 10000)))

    def _saveName(self, chunk):
        ## Want to delegate this to the merger.
        dumpName = "%s_%s.dump" % (str(self._sessionId), str(chunk))
        return os.path.join(self._scratchPath, dumpName)

class HintedQueryAction:
    """A HintedQueryAction encapsulates logic to prepare, execute, and 
    retrieve results of a query that has a hint string."""
    def __init__(self, query, hints, pmap, reportError, resultName=""):
        self.queryStr = query.strip()# Pull trailing whitespace
        # Force semicolon to facilitate worker-side splitting
        if self.queryStr[-1] != ";":  # Add terminal semicolon
            self.queryStr += ";" 

        # queryHash identifies the top-level query.
        self.queryHash = self._computeHash(self.queryStr)[:18]
        self.chunkLimit = 2**32 # something big
        self._isValid = False
        if not self._importQconfig(pmap, hints):
            return

        if not self._parseAndPrep(query, hints):
            return

        if not self._isValid:
            discardSession(self._sessionId)
            return
        self._prepForExec(self._useMemory, reportError, resultName)

    def _importQconfig(self, pmap, hints):
        self._dbContext = "LSST" # Later adjusted by hints.        
        # Hint evaluation
        self._pmap = pmap            
        self._isFullSky = False # Does query involves whole sky
        try:
            self._evaluateHints(hints, pmap) # Also gets new dbContext
        except QueryHintError, e:
            self._isValid = False
            self._error = e.reason
            return 

        # Config preparation
        qConfig = self._prepareCppConfig(self._dbContext, hints)
        assert not getSessionError(self._sessionId)
        cModule = lsst.qserv.master.config
        cf = cModule.config.get("partitioner", "emptyChunkListFile")
        self._emptyChunks = loadEmptyChunks(cf)
        if self._emptyChunks == None:
            print "WARNING: error with partitioner.emptyChunkListFile"
            print "Continuing with no empty chunks."
            self._emptyChunks = set()

        cfgLimit = int(cModule.config.get("debug", "chunkLimit"))
        if cfgLimit > 0:
            self.chunkLimit = cfgLimit
            print "Using debugging chunklimit:",cfgLimit
        self._useMemory = cModule.config.get("tuning", "memoryEngine")
        return True

    def _parseAndPrep(self, query, hints):
        # Table mapping 
        try:
            self._pConfig = getSpatialConfig()
            cfg = self._prepareCppConfig(self._dbContext, hints)
            self._substitution = SqlSubstitution(query, 
                                                 self._pConfig.chunkMeta,
                                                 cfg)

            if self._substitution.getError():
                self._error = self._substitution.getError()
                self._isValid = False
            else:
                self._isValid = True
        except Exception, e:
            print "Exception!",e
            self._isValid = False
        return True

    def _prepForExec(self, useMemory, reportError, resultName):
        self._postFixChunkScope(self._substitution.getChunkLevel())

        # Query babysitter.
        self._babysitter = QueryBabysitter(self._sessionId, self.queryHash,
                                           self._substitution.getMergeFixup(),
                                           reportError, resultName)
        self._reportError = reportError
        ## For generating subqueries
        if useMemory == "yes":
            print "Memory spec:", useMemory
            engineSpec = "ENGINE=MEMORY "
        else: engineSpec = ""
        self._createTableTmpl = "CREATE TABLE IF NOT EXISTS %s " + engineSpec
        self._insertTableTmpl = "INSERT INTO %s " ;
        self._resultTableTmpl = "r_%s_%s" % (self._sessionId,
                                             self.queryHash) + "_%s"
        # Should use db from query, not necessarily context.
        self._factory = protocol.TaskMsgFactory(self._sessionId, 
                                                self._dbContext)

        # We want result table names longer than result-merging table names.
        self._isValid = True
        self._invokeLock = threading.Semaphore()
        self._invokeLock.acquire() # Prevent result retrieval before invoke
        pass


    # In transition to new protocol: only 1 result table allowed.
    def _headerFunc(self, tableNames, subc=[]):
        return ['-- SUBCHUNKS:' + ", ".join(imap(str,subc)),
                '-- RESULTTABLES:' + ",".join(tableNames)]

    def _prepareCppConfig(self, dbContext, hints):
        hintCopy = hints.copy()        
        hintCopy.pop("db") # Remove db hint--only pass spatial hints now.
        cfg = lsst.qserv.master.config.getStringMap()
        cfg["table.defaultdb"] = dbContext
        cfg["query.hints"] = ";".join(
            map(lambda (k,v): k + "," + v, hintCopy.items()))
        return cfg

    def _parseRegions(self, hints):
        r = getRegionFactory()
        regs = r.getRegionFromHint(hints)
        if regs != None:
            return regs
        else:
            if r.errorDesc:
                # How can we give a good error msg to the client?
                s = "Error parsing hint string %s"
                raise QueryHintError(s % r.errorDesc)
            return []
        pass


    def _postFixChunkScope(self, chunkLevel):
        if chunkLevel == 0:
            # In this case, non-chunked, so dummy chunk is good enough
            # for dispatch.
            self._intersectIter = [(dummyEmptyChunk, [])]
            return
        return

    def _evaluateHints(self, hints, pmap):
        """Modify self.fullSky and self.partitionCoverage according to 
        spatial hints"""
        self._isFullSky = True
        self._intersectIter = pmap

        if hints:
            regions = self._parseRegions(hints)
            self._dbContext = hints.get("db", "")
            ids = hints.get("objectId", "")
            if regions != []:
                self._intersectIter = pmap.intersect(regions)
                self._isFullSky = False
            if ids:
                chunkIds = self._getChunkIdsFromObjs(ids)
                if regions != []:
                    self._intersectIter = chain(self._intersectIter, chunkIds)
                else:
                    self._intersectIter = map(lambda i: (i,[]), chunkIds)
                self._isFullSky = False
                if not self._intersectIter:
                    self._intersectIter = [(dummyEmptyChunk, [])]
        # _isFullSky indicates that no spatial hints were found.
        # However, if spatial tables are not found in the query, then
        # we should use the dummy chunk so the query can be dispatched
        # once to a node of the balancer's choosing.
                    
        # If hints only apply when partitioned tables are in play.
        # FIXME: we should check if partitionined tables are being accessed,
        # and then act to support the heaviest need (e.g., if a chunked table
        # is being used, then issue chunked queries).
        #print "Affected chunks: ", [x[0] for x in self._intersectIter]
        pass

    def _getChunkIdsFromObjs(self, ids):
        table = metadata.getIndexNameForTable("LSST.Object")
        objCol = "objectId"
        chunkCol = "x_chunkId"
        try:
            test = ",".join(map(str, map(int, ids.split(","))))
            chopped = filter(lambda c: not c.isspace(), ids)
            assert test == chopped
        except Exception, e:
            print "Error converting objectId spec. ", ids, "Ignoring.",e
            #print test,"---",chopped
            return []
        sql = "SELECT %s FROM %s WHERE %s IN (%s);" % (chunkCol, table,
                                                       objCol, ids)
        db = Db()
        db.activate()
        cids = db.applySql(sql)
        cids = map(lambda t: t[0], cids)
        del db
        return cids

    def _prepareMsg(self, chunkId, subIter):
        table = self._resultTableTmpl % str(chunkId)
        self._factory.newChunk(table, chunkId);
        x =  self._substitution.getChunkLevel()
        if x > 1:
            sclist =  self._getSubChunkList(subIter)
            for subChunkId in sclist:
                q = self._substitution.transform(chunkId, subChunkId)
                self._factory.fillFragment(q, [subChunkId])
        else:
            query = self._substitution.transform(chunkId, 0)
            self._factory.fillFragment(query, None)
        return self._factory.getBytes()

    def dispatchChunk(self, chunkId, subIter, lastTime):
        print "Dispatch iter: ", time.time() - lastTime
        msg = self._prepareMsg(chunkId, subIter)
        prepTime = time.time()
        print "DISPATCH: ", chunkId, self.queryStr # Limit printout spew
        self._babysitter.submitMsg(self._factory.msg.db,
                                   chunkId, msg, 
                                   self._factory.resulttable)
        print "Chunk %d dispatch took %f seconds (prep: %f )" % (
            chunkId, time.time() - lastTime, prepTime - lastTime)

    def invokeProtocol2(self):
        count = 0
        self._babysitter.pauseReadback();
        lastTime = time.time()
        chunkLimit = self.chunkLimit
        for chunkId, subIter in self._intersectIter:
            if chunkId in self._emptyChunks:
                continue
            self.dispatchChunk(chunkId, subIter, lastTime)
            lastTime = time.time()
            count += 1
            if count >= chunkLimit: break
            ##print >>sys.stderr, q, "submitted"
        if count == 0:
            self.dispatchChunk(dummyEmptyChunk, [], lastTime)

        self._babysitter.resumeReadback()
        self._invokeLock.release()
        return

    def invoke(self):
        self.invokeProtocol2()

    def invokeOldProtocol(self):
        count=0
        self._babysitter.pauseReadback();
        lastTime = time.time()
        chunkLimit = self.chunkLimit
        for chunkId, subIter in self._intersectIter:
            if chunkId in self._emptyChunks:
                continue # FIXME: What if all chunks are empty?
            print "Dispatch iter: ", time.time() - lastTime
            table = self._resultTableTmpl % str(chunkId)
            q = None
            x =  self._substitution.getChunkLevel()
            if x > 1:
                q = self._makeSubChunkQuery(chunkId, subIter, table)
            else:
                q = self._makeChunkQuery(chunkId, table)
            prepTime = time.time()
            print "DISPATCH: ", q[:500] # Limit printout spew
            self._babysitter.submit(chunkId, table, q)
            print "Chunk %d dispatch took %f seconds (prep: %f )" % (
                chunkId, time.time() - lastTime, prepTime - lastTime)
            lastTime = time.time()
            count += 1
            if count >= chunkLimit: break
            ##print >>sys.stderr, q, "submitted"
        self._babysitter.resumeReadback()
        self._invokeLock.release()
        return

    def getError(self):
        try:
            return self._error
        except:
            return ""

    def getResult(self):
        """Wait for query to complete (as necessary) and then return 
        a handle to the result."""
        self._invokeLock.acquire()
        self._babysitter.finish()
        self._invokeLock.release()
        table = self._babysitter.getResultTableName()
        #self._collater.finish()
        #table = self._collater.getResultTableName()
        return table

    def getIsValid(self):
        return self._isValid

    def _makeNonPartQuery(self, table):
        # Should be able to do less work than chunk query.
        query = "\n".join(self._headerFunc([table])) +"\n"
        query += self._createTableTmpl % table
        query += self._substitution.transform(0,0)
        return query

    def _makeChunkQuery(self, chunkId, table):
        # Prefix with empty subchunk spec.
        query = "\n".join(self._headerFunc([table])) +"\n"
        query += self._createTableTmpl % table
        query += self._substitution.transform(chunkId, 0)
        return query

    def _getSubChunkList(self, subIter):
        # Extract list first.
        if self._isFullSky:
            scList = [x for x in subIter]
        else:
            scList = [sub for (sub, regions) in subIter]
        return scList

    def _makeSubChunkQuery(self, chunkId, subIter, table):
        qList = [None] # Include placeholder for header
        scList = self._getSubChunkList(subIter)

        pfx = None
        qList = self._headerFunc([table], scList)
        for subChunkId in scList:
            q = self._substitution.transform(chunkId, subChunkId)
            if pfx:
                qList.append(pfx + q)
            else:
                qList.append((self._createTableTmpl % table) + q)
                pfx = self._insertTableTmpl % table
        return "\n".join(qList)

    def _computeHash(self, bytes):
        return hashlib.md5(bytes).hexdigest()

class InbandQueryAction:
    """InbandQueryAction is an action which represents a user-query
    that is executed using qserv in many pieces. It borrows a little
    from HintedQueryAction, but uses different abstractions
    underneath.
    """
    def __init__(self, query, hints, pmap, reportError, resultName=""):
        self.queryStr = query.strip()# Pull trailing whitespace
        # Force semicolon to facilitate worker-side splitting
        if self.queryStr[-1] != ";":  # Add terminal semicolon
            self.queryStr += ";" 

        # queryHash identifies the top-level query.
        self.queryHash = self._computeHash(self.queryStr)[:18]
        self.chunkLimit = 2**32 # something big
        self.isValid = False

        self.hints = hints
        self.pmap = pmap

        self._importQconfig()
        self._invokeLock = threading.Semaphore()
        self._invokeLock.acquire() # Prevent res-retrieval before invoke
        self._resultName = resultName
        self._reportError = reportError
        self.isValid = True
        pass

    def invoke(self):
        self._prepareForExec()
        self._execAndJoin()
        self._invokeLock.release()

    def getError(self):
        try:
            return self._error
        except:
            return "Unknown error InbandQueryAction"

    def getResult(self):
        """Wait for query to complete (as necessary) and then return 
        a handle to the result."""
        self._invokeLock.acquire()
        # Make sure it's joined.
        self._invokeLock.release()
        return self._resultName

    def getIsValid(self):
        return self.isValid

    def _computeHash(self, bytes):
        return hashlib.md5(bytes).hexdigest()

    def _prepareForExec(self):
        self._applyHints(self.hints)
        cfg = self._prepareCppConfig()
        self.sessionId = newSession(cfg)
        setupQuery(self.sessionId, self.queryStr, self._resultName)
        errorMsg = getSessionError(self.sessionId)
        # TODO: Handle error more gracefully.
        assert not getSessionError(self.sessionId)

        self._applyConstraints()
        self._prepareMerger()
        pass

    def _evaluateHints(self, hints, pmap):
        """Modify self.fullSky and self.partitionCoverage according to 
        spatial hints. This is copied from older parser model"""
        self._isFullSky = True
        self._intersectIter = pmap

        if hints:
            regions = self._parseRegions(hints)
            self._dbContext = hints.get("db", "")
            ids = hints.get("objectId", "")
            if regions != []:
                self._intersectIter = pmap.intersect(regions)
                self._isFullSky = False
            if ids:
                chunkIds = self._getChunkIdsFromObjs(ids)
                if regions != []:
                    self._intersectIter = chain(self._intersectIter, chunkIds)
                else:
                    self._intersectIter = map(lambda i: (i,[]), chunkIds)
                self._isFullSky = False
                if not self._intersectIter:
                    self._intersectIter = [(dummyEmptyChunk, [])]
        # _isFullSky indicates that no spatial hints were found.
        # However, if spatial tables are not found in the query, then
        # we should use the dummy chunk so the query can be dispatched
        # once to a node of the balancer's choosing.
                    
        # If hints only apply when partitioned tables are in play.
        # FIXME: we should check if partitionined tables are being accessed,
        # and then act to support the heaviest need (e.g., if a chunked table
        # is being used, then issue chunked queries).
        #print "Affected chunks: ", [x[0] for x in self._intersectIter]
        pass

    def _applyConstraints(self):
        # Retrieve constraints as (name, [param1,param2,param3,...])
        self.constraints = getConstraints(self.sessionId)
        # Apply constraints
        def iterateConstraints(constraintVec):
            for i in range(constraintVec.size()):
                yield constraintVec.get(i)
        for constraint in iterateConstraints(self.constraints):
            pass # TODO: Prepare hints for region factory
        # TODO: hand off to spatial module and indexing
        self._evaluateHints(self.hints, self.pmap)
        
        count = 0
        chunkLimit = self.chunkLimit
        for chunkId, subIter in self._intersectIter:
            if chunkId in self._emptyChunks:
                continue
            #prepare chunkspec
            c = ChunkSpec()
            c.chunkId = chunkId
            map(c.addSubChunk, subIter)
            addChunk(self.sessionId, c)
            count += 1
            if count >= chunkLimit: break
        if count == 0:
            addChunk(dummyEmpty)
        pass

    def _execAndJoin(self):
        submitQuery3(self.sessionId)
        s = joinSession(self.sessionId)
        if s != QueryState_SUCCESS:
            self._reportError(getErrorDesc(self.sessionId))

        if not self.isValid:
            discardSession(self.sessionId)
            return

    def _importQconfig(self):
        """Import config file settings into self"""
        # Config preparation
        cModule = lsst.qserv.master.config
        # Empty chunks. TODO: use parsed dominant db to select emptychunks
        cf = cModule.config.get("partitioner", "emptyChunkListFile")
        self._emptyChunks = loadEmptyChunks(cf)
        if self._emptyChunks == None:
            print "WARNING: error with partitioner.emptyChunkListFile"
            print "Continuing with no empty chunks."
            self._emptyChunks = set()

        # chunk limit: For debugging
        cfgLimit = int(cModule.config.get("debug", "chunkLimit"))
        if cfgLimit > 0:
            self.chunkLimit = cfgLimit
            print "Using debugging chunklimit:",cfgLimit

        # Memory engine(unimplemented): Buffer results/temporaries in
        # memory on the master. (no control over worker) 
        self._useMemory = cModule.config.get("tuning", "memoryEngine")
        return True
    
    def _applyHints(self, hints):
        """Apply user/proxy supplied context"""
        # Use hints for query context (i.e. default db, etc)
        self._dbContext = hints.get("db", "")
        self._hints = hints.copy()
        pass

    def _prepareCppConfig(self):
        """Construct a C++ stringmap for passing settings and context
        to the C++ layer."""
        cfg = lsst.qserv.master.config.getStringMap()
        cfg["frontend.scratchPath"] = setupResultScratch()
        cfg["table.defaultdb"] = self._dbContext
        cfg["query.hints"] = ";".join(
            map(lambda (k,v): k + "," + v, self._hints.items()))
        cfg["table.result"] = self._resultName
        return cfg

    def _parseRegions(self, hints):
        r = getRegionFactory()
        regs = r.getRegionFromHint(hints)
        if regs != None:
            return regs
        else:
            if r.errorDesc:
                # How can we give a good error msg to the client?
                s = "Error parsing hint string %s"
                raise QueryHintError(s % r.errorDesc)
            return []
        pass

    def _prepareMerger(self):
        c = lsst.qserv.master.config.config
        dbSock = c.get("resultdb", "unix_socket")
        dbUser = c.get("resultdb", "user")
        dbName = c.get("resultdb", "db")        
        dropMem = c.get("resultdb","dropMem")

        mysqlBin = c.get("mysql", "mysqlclient")
        if not mysqlBin:
            mysqlBin = "mysql"
        configureSessionMerger3(self.sessionId)
        pass
        
    pass # class InbandQueryAction

class CheckAction:
    def __init__(self, tracker, handle):
        self.tracker = tracker
        self.texthandle = handle
        pass
    def invoke(self):
        self.results = None
        id = int(self.texthandle)
        t = self.tracker.task(id)
        if t: 
            self.results = 50 # placeholder. 50%

#see if it's better to not bother with an action object
def results(tracker, handle):
        id = int(handle)
        t = tracker.task(id)
        if t:
            return "Some host with some port with some db"
        return None

def clauses(col, cmin, cmax):
    return ["%s between %smin and %smax" % (cmin, col, col),
            "%s between %smin and %smax" % (cmax, col, col),
            "%smin between %s and %s" % (col, cmin, cmax)]

# Watch out for memory errors:
# Exception in thread Thread-2897:
# Traceback (most recent call last):
#   File "/home/wang55/scratch/s/Linux/external/python/2.5.2/lib/python2.5/threading.py", line 486, in __bootstrap_inner
#     self.run()
#   File "/home/wang55/5node/m121/lsst/qserv/master/app.py", line 192, in run
#     buf = "".center(bufSize) # Fill buffer
# MemoryError
