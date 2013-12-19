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

@author  Jacek Becla, SLAC


Known issues and todos:
 - Note that this depends on kazoo, so it'd be best to avoid distributing this to
   every user. For that reason in the future we might run this separately from the
   client, so this may not have access to local config files provided by user
   - and that will complicate error handling, e.g., if we raise exception here, the
     qserv_admin which will run on a separate server will not be able to catch it.
"""

import logging

from cssInterface import CssInterface, CssException

class QservAdminImpl(object):
    """
    QservAdminImpl implements functions needed by qserv_admin client program.
    """

    def __init__(self, loggerName, verbosityT=40):
        """
        Initialize: create CssInterface object.

        @param verbosityT   Verbosity threshold. Logging messages which are less
                            severe than verbosityT will be ignored. Expected values
                            values match python logging numeric values (CRITICAL=50,
                            ERROR=40, WARNING=30, INFO=20, DEBUG=10, NOTSET=0). 
                            The default is ERROR.
        @param loggerName   Name of the logger. The logger should be already
                            initialized.
        """
        self._cssI = CssInterface(verbosityT)
        self._logger = logging.getLogger(loggerName)

    def createDb(self, dbName, options):
        """
        Create database (options specified explicitly).

        @param dbName    Database name
        @param options   Array with options (key/value)
        """

        if self._dbExists(dbName):
            self._logger.error("Database '%s' already exists." % dbName)
            return CssException.ERR_DB_EXISTS
        dbP = "/DATABASES/%s" % dbName
        ptP = None
        try:
            self._cssI.create(dbP, "PENDING")
            ptP = self._cssI.create("/DATABASE_PARTITIONING/_", sequence=True)
            self._cssI.create("%s/nStripes"    % ptP, options["nStripes"   ])
            self._cssI.create("%s/nSubStripes" % ptP, options["nSubStripes"])
            self._cssI.create("%s/overlap"     % ptP, options["overlap"    ])
            self._cssI.create("%s/dbGroup" % dbP, options["level"])
            pId = ptP[-10:] # the partitioning id is always 10 digit, 0 padded
            self._cssI.create("%s/partitioningId" % dbP, str(pId))
            self._cssI.create("%s/releaseStatus" % dbP,"UNRELEASED")
            self._cssI.create("%s/objIdIndex" % dbP, options["objectIdIndex"])
            self._createDbLockSection(dbP)
            self._cssI.set(dbP, "READY")
        except CssException as e:
            self._logger.error("Failed to create database, error was: ", e)
            self._cssI.delete(dbP, recursive=True)
            if ptP is not None: self._cssI.delete(ptP, recursive=True)
            return e.getErrNo()
        return CssException.SUCCESS

    def createDbLike(self, dbName, dbName2):
        """
        Create database using an existing database as a template.

        @param dbName    Database name (of the database to create)
        @param dbName2   Database name (of the template database)
        """
        self._logger.info("Creating db '%s' like '%s'" % (dbName, dbName2))
        if self._dbExists(dbName):
            self._logger.error("Database '%s' already exists." % dbName)
            return CssException.DB_EXISTS
        if not self._dbExists(dbName2):
            self._logger.error("Database '%s' does not exist." % dbName2)
            return CssException.ERR_DB_DOES_NOT_EXIST
        dbP = "/DATABASES/%s" % dbName
        try:
            self._cssI.create(dbP, "PENDING")
            self._copyKeyValue(dbName, dbName2, 
                               ("dbGroup", "partitioningId", 
                                "releaseStatus", "objIdIndex"))
            self._createDbLockSection(dbP)
            self._cssI.set(dbP, "READY")
        except CssException as e:
            self._logger.error( "Failed to create database, error was: ", e)
            self._cssI.delete(dbP, recursive=True)
            return e.getErrNo()
        return CssException.SUCCESS

    def dropDb(self, dbName):
        """
        Drop database.

        @param dbName    Database name.
        """
        if not self._dbExists(dbName):
            self._logger.error("Database '%s' does not exist." % dbName)
            return CssException.ERR_DB_DOES_NOT_EXIST
        self._cssI.delete("/DATABASES/%s" % dbName, recursive=True)
        return CssException.SUCCESS

    def showDatabases(self):
        """
        Print to stdout the list of databases registered for Qserv use.
        """
        if not self._cssI.exists("/DATABASES"):
            print "No databases found."
        else:
            print self._cssI.getChildren("/DATABASES")

    def showEverything(self):
        """
        Dumps entire metadata in CSS to stdout. Very useful for debugging.
        """
        self._cssI.printAll()

    def dropEverything(self):
        """
        Delete everything from the CSS (very dangerous, very useful for debugging.)
        """
        self._cssI.deleteAll("/")

    def _dbExists(self, dbName):
        """
        Check if the database exists.

        @param dbName    Database name.
        """
        p = "/DATABASES/%s" % dbName
        return self._cssI.exists(p)

    def _copyKeyValue(self, dbDest, dbSrc, theList):
        """
        Copy items specified in theList from dbSrc to dbDest.

        @param dbDest    Destination database name.
        @param dbSrc     Source database name
        @param theList   The list of elements to copy.
        """
        dbS  = "/DATABASES/%s" % dbSrc
        dbD = "/DATABASES/%s" % dbDest
        for x in theList:
            v = self._cssI.get("%s/%s" % (dbS, x))
            self._cssI.create("%s/%s" % (dbD, x), v)

    def _createDbLockSection(self, dbP):
        """
        Create key/values related to "LOCK" for a given db path.

        @param dbP    Path to the database.
        """
        self._cssI.create("%s/LOCK/comments" % dbP)
        self._cssI.create("%s/LOCK/estimatedDuration" % dbP)
        self._cssI.create("%s/LOCK/lockedBy" % dbP)
        self._cssI.create("%s/LOCK/lockedTime" % dbP)
        self._cssI.create("%s/LOCK/mode" % dbP)
        self._cssI.create("%s/LOCK/reason" % dbP)
