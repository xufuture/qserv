#
# LSST Data Management System
# Copyright 2009-2013 LSST Corporation.
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

# metadata module for lsst.qserv.master
#
# The metadata module contains functions, constants, and data related
# strictly to qserv's runtime metadata.  This includes the objectId
# index (secondary indexing) and the empty chunks file(s). This needs
# to be re-thought, now that the Qms is available.

# Pkg imports
import config
import string
from collections import defaultdict
import lsst.qserv.master

class Runtime:
    """The in-memory class for containing general Qserv frontend metadata.
    """
    def __init__(self):
        self.metaDbName = config.config.get("mgmtdb", "db")
        self.emptyChunkInfo = {}
        # occupied chunks and subchunks, keyed by database.
        self.occupied = {}
        self.defaultEmptyChunks = config.config.get("partitioner",
                                        "emptyChunkListFile")
        print "Using %s as default empty chunks file." % (self.defaultEmptyChunks)
        self.emptyChunkInfo[""] = self.loadIntsFromFile(self.defaultEmptyChunks)
        pass

    def populateEmptyChunkInfo(self, dbName):
        """Populate self.emptyChunkInfo[dbName] with a set() containing
        chunkIds for the empty chunks of the particular db. If no empty
        chunk information can be found and loaded for the db, a default
        empty chunks file is used."""

        sanitizedDbName = self.sanitizeName(dbName)
        name = "empty_%s.txt" % sanitizedDbName
        info = self.loadIntsFromFile(name)
        if info == None:
            print "Couldn't find %s, using %s." % (name,
                                                   self.defaultEmptyChunks)
            self.emptyChunkInfo[dbName] = self.emptyChunkInfo[""]
        else:
            self.emptyChunkInfo[dbName] = info
        pass

    def sanitizeName(self, name):
        sanitized = filter(lambda c: (c in string.letters) or
                           (c in string.digits) or (c in ["_"]),
                           name)
        if sanitized != name:
            print "WARNING, name=", name,
            print "contains questionable characters. sanitized=",
            print sanitized
        return sanitized

    def populateOccupied(self, dbName):
        """Populate self.occupied[dbName] with a dict containing lists
        of occupied subChunkIds, keyed by chunkId.
        Reads from secondary index (aka "objectId index")
        """
        dbOccupied = self.loadOccupied(dbName)
        self.occupied[dbName] = dbOccupied
        return
        import cPickle as pickle
        pickled = pickle.dumps(dbOccupied)
        print "Pickled size for %s is %d, %d chunks" % (
            dbName, len(pickled), len(dbOccupied))
        print "subChunk total", sum(map(len, dbOccupied.values()))

        pass

    def makeSqlSource(self, dbName):
        # Replace with metadata lookup if metadata has
        # key-table/boss-table/head-table defined.
        headTable = {"LSST" : "Object",
                     "pt11_final" : "Object",
                     "pt12_final" : "Object",
                     "winter13" : "GoodSeeingSource"
                     }
        qualifiedTable = "qservMeta.%s__%s" % (dbName,
                                               headTable[dbName])
        db = lsst.qserv.master.db.Db()
        db.activate()
        sql = "SELECT DISTINCT %s, %s FROM %s" % (
            lsst.qserv.master.CHUNK_COLUMN,
            lsst.qserv.master.SUBCHUNK_COLUMN,
            qualifiedTable)
        for row in db.applySql(sql):
            yield (int(row[0]), int(row[1]))
        pass

    def loadOccupied(self, dbName):
        src = self.makeSqlSource(dbName)
        # src = makeIndexSource(dbName)
        chunks = defaultdict(list)
        for r in src:
            chunks[r[0]].append(r[1])
        return chunks

    def loadIntsFromFile(self, filename):
        """Return a set() of ints from a file that is assumed to contain
        ASCII-represented integers delimited with newline characters.
        @return set of ints, or None on any error.
        """
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


# Module data
_myRuntime = None
def _getRuntime():
    global _myRuntime
    if not _myRuntime: _myRuntime = Runtime()
    return _myRuntime

# External interface
def getIndexNameForTable(tableName):
    """@return name of index table for @param tableName"""
    runtime =_getRuntime()
    return runtime.metaDbName + "." + tableName.replace(".","__")

def getMetaDbName():
    """@return the name of database containing the index tables
    (for objectId index tables)"""
    runtime = _getRuntime()
    return runtime.metaDbName

def getEmptyChunks(dbName):
    """@return a set containing chunkIds (int) that are empty for a
    given qserv dbName.
    """
    runtime = _getRuntime()
    if dbName not in runtime.emptyChunkInfo:
        runtime.populateEmptyChunkInfo(dbName)
    return runtime.emptyChunkInfo[dbName]

def getOccupied(dbName):
    runtime = _getRuntime()
    if dbName not in runtime.occupied:
        runtime.populateOccupied(dbName)
    return runtime.occupied[dbName]

def makeEmpty():
    runtime = _getRuntime()
    runtime.populateOccupied("LSST")

