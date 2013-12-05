#!/usr/bin/env python

# LSST Data Management System
# Copyright 2013 LSST Corporation.
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

"""
Internals that do the actual work for the qserv client program.
Note that this depends on kazoo, so it'd be best to avoid distributing
this to every user. For that reason in the future we might run this
separately from the client, so this may not have access to local
config files provided by user.

Known todos:
 - need to deal with error handling properly, e.g., define error status
   instead of using dummy SUCCESS, ERROR, 
 - need to find out how to abort transaction.
"""

from cssIFace import CssIFace, CssException

SUCCESS = 0
ERROR = -1

class QservAdminImpl(object):
    """
    Implements functions needed by qserv_admin client program.
    """
    def __init__(self):
        self._iFace = CssIFace()

    ################################################################################
    #### createDb
    ################################################################################
    def createDb(self, dbName, options):
        """Creates database (options specified explicitly)."""

        if self._dbExists(dbName):
            print "ERROR: db '%s' already exists" % dbName
            return ERROR
        dbP = "/DATABASES/%s" % dbName
        t = self._iFace.startTransaction()
        try:
            self._iFace.create(dbP, "PENDING")
            ptP = self._iFace.create("/DATABASE_PARTITIONING/_", sequence=True)
            self._iFace.create("%s/nStripes"    % ptP, options["nStripes"   ])
            self._iFace.create("%s/nSubStripes" % ptP, options["nSubStripes"])
            self._iFace.create("%s/overlap"     % ptP, options["overlap"    ])
            self._iFace.create("%s/dbGroup" % dbP, options["level"])
            pId = ptP[-10:] # the partitioning id is always 10 digit, 0 padded
            self._iFace.create("%s/partitioningId" % dbP, str(pId))
            self._iFace.create("%s/releaseStatus" % dbP,"UNRELEASED")
            self._iFace.create("%s/objIdIndex" % dbP, options["objectIdIndex"])
            self._createDbLockSection(dbP)
            t.commit()
            self._iFace.set(dbP, "READY")
        except CssException as e:
            print "Failed to create database, error was: ", e.getErrMsg()
            self._iFace.delete(dbP, recursive=True)
            self._iFace.delete(ptP, recursive=True)
            return ERROR
        return SUCCESS

    ################################################################################
    #### createDbLike
    ################################################################################
    def createDbLike(self, dbName, dbName2):
        """Creates database using an existing database as a template."""
        print "Creating db '%s' like '%s'" % (dbName, dbName2)
        if self._dbExists(dbName):
            print "ERROR: db '%s' already exists" % dbName
            return ERROR
        if not self._dbExists(dbName2):
            print "ERROR: db '%s' does not exist." % dbName2
            return ERROR
        dbP = "/DATABASES/%s" % dbName
        t = self._iFace.startTransaction()
        try:
            self._iFace.create(dbP, "PENDING")
            self._copyKeyValue(dbName, dbName2, 
                               ("dbGroup", "partitioningId", 
                                "releaseStatus", "objIdIndex"))
            t.commit()
            self._iFace.set(dbP, "READY")
        except CssException as e:
            print "Failed to create database, error was: ", e.getErrMsg()
            self._iFace.delete(dbP, recursive=True)
            return ERROR
        self._createDbLockSection(dbP)
        return SUCCESS

    ################################################################################
    #### dropDb
    ################################################################################
    def dropDb(self, dbName):
        """Drops database."""
        if not self._dbExists(dbName):
            print "ERROR: db does not exist"
            return ERROR
        self._iFace.delete("/DATABASES/%s" % dbName, recursive=True)
        return SUCCESS

    ################################################################################
    #### showDatabases
    ################################################################################
    def showDatabases(self):
        """Shows list of databases registered for Qserv use."""
        if not self._iFace.exists("/DATABASES"):
            print "No databases found."
        else:
            print self._iFace.getChildren("/DATABASES")

    ################################################################################
    #### showEverything
    ################################################################################
    def showEverything(self):
        """Dumps entire metadata in CSS to stdout. Very useful for debugging."""
        self._iFace.printAll()

    ################################################################################
    #### dropEverything
    ################################################################################
    def dropEverything(self):
        """Deletes everything from the CSS (very dangerous, very useful for
        debugging.)"""
        self._iFace.deleteAll("/")

    ################################################################################
    #### _dbExists
    ################################################################################
    def _dbExists(self, dbName):
        """Checks if the database exists."""
        p = "/DATABASES/%s" % dbName
        return self._iFace.exists(p)

    ################################################################################
    #### _copyKeyValue
    ################################################################################
    def _copyKeyValue(self, dbDest, dbSrc, theList):
        """It copies items specified in theList from dbSrc to dbDest."""
        dbS  = "/DATABASES/%s" % dbSrc
        dbD = "/DATABASES/%s" % dbDest
        for x in theList:
            v = self._iFace.get("%s/%s" % (dbS, x))
            self._iFace.create("%s/%s" % (dbD, x), v)

    ################################################################################
    #### _createDbLockSection
    ################################################################################
    def _createDbLockSection(self, dbP):
        """It creates key/values related to "LOCK" for a given db path."""
        self._iFace.create("%s/LOCK/comments" % dbP)
        self._iFace.create("%s/LOCK/estimatedDuration" % dbP)
        self._iFace.create("%s/LOCK/lockedBy" % dbP)
        self._iFace.create("%s/LOCK/lockedTime" % dbP)
        self._iFace.create("%s/LOCK/mode" % dbP)
        self._iFace.create("%s/LOCK/reason" % dbP)
