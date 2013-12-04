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
"""

from cssIFace import CssIFace
from cssStatus import CssException

SUCCESS = 0
ERROR = -1

class QservAdminImpl(object):
    """
    Implements functions needed by qserv_admin client program.
    """
    def __init__(self):
        self._iFace = CssIFace()

    # ---------------------------------------------------------------------------
    def createDb(self, dbName, options):
        """Creates database (options specified explicitly)."""

        if self._dbExists(dbName):
            print "ERROR: db already exists"
            return ERROR
        try:
            dbP = "/DATABASES/%s" % dbName
            self._iFace.create(dbP)
            p = self._iFace.create("/DATABASES/partitioning_", sequence=True)
            self._iFace.create("%s/nStripes"    % p, options["nStripes"   ])
            self._iFace.create("%s/nSubStripes" % p, options["nSubStripes"])
            self._iFace.create("%s/overlap"     % p, options["overlap"    ])
            self._iFace.create("%s/dbGroup" % dbP, options["level"])
            pId = p[-10:] # the partitioning id is always 10 digit, 0 padded
            self._iFace.create("%s/partitioningId" % dbP, str(pId))
            self._iFace.create("%s/releaseStatus" % dbP,"UNRELEASED")
            self._iFace.create("%s/objIdIndex" % dbP, options["objectIdIndex"])
            self._iFace.create("%s/LOCK/comments" % dbP)
            self._iFace.create("%s/LOCK/estimatedDuration" % dbP)
            self._iFace.create("%s/LOCK/lockedBy" % dbP)
            self._iFace.create("%s/LOCK/lockedTime" % dbP)
            self._iFace.create("%s/LOCK/mode" % dbP)
            self._iFace.create("%s/LOCK/reason" % dbP)
        except CssException as e:
            print e.getErrMsg()
            return ERROR
        return SUCCESS

    # ---------------------------------------------------------------------------
    def createDbLike(self, dbName, dbName2):
        """Creates database using an existing database as a template."""
        print "Creating db '%s' like '%s'" % (dbName, dbName2)
        print "createDbLike impl not finished"
        # need to create all other things in CSS related to that db
        return SUCCESS

    # ---------------------------------------------------------------------------
    def dropDb(self, dbName):
        """Drops database."""
        if not self._dbExists(dbName):
            print "ERROR: db does not exist"
            return ERROR
        self._iFace.delete("/DATABASES/%s" % dbName)
        print "createDb impl not finished"
        # need to delete all other things in CSS related to that db
        return SUCCESS

    # ---------------------------------------------------------------------------
    def showDatabases(self):
        """Shows list of databases registered for Qserv use."""
        if not self._iFace.exists("/DATABASES"):
            print "No databases found."
        else:
            print self._iFace.getChildren("/DATABASES")

    # ---------------------------------------------------------------------------
    def showEverything(self):
        """Dumps entire metadata in CSS to stdout. Very useful for debugging."""
        self._iFace.printAll()

    # ---------------------------------------------------------------------------
    def dropEverything(self):
        """Deletes everything from the CSS (very dangerous, very useful for
        debugging."""
        self._iFace.deleteAll("/")

    # ---------------------------------------------------------------------------
    def _dbExists(self, dbName):
        """Checks if the database exists."""
        p = "/DATABASES/%s" % dbName
        return self._iFace.exists(p)
