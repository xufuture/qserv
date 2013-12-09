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
Database Watcher - runs on each Qserv node and maintains Qserv databases (creates databases, deletes databases, creates tables, drops tables etc).

@author  Jacek Becla, SLAC


Known issues and todos:
 - mysql host/port user/passwd/socket
 - need to go through cssIFace interface, now bypassing it in two places:
    - @self._iFace._zk.DataWatch
    - @self._iFace._zk.ChildrenWatch
 - considering implementing garbage collection for threads corresponding to
   deleted databases.
 - If all metadata is deleted when the watcher is running, the watcher will die
   with: ERROR:kazoo.handlers.threading:Exception in worker queue thread.
   To reproduce, just run: 
       echo "drop everything;" | ./qserv_admin.py
"""

import time
import threading

from cssIFace import CssIFace
from db import Db, DbException, DbStatus

# IThis helps uncomment logging if you see errors:
# No handlers could be found for logger "kazoo.recipe.watchers"
import logging
logging.basicConfig()

####################################################################################
####################################################################################
####################################################################################
class OneDbWatcher(threading.Thread):
    """
    This class implements a database watcher. Each instance is responsible for
    creating / dropping one database. It is relying on Zookeeper's DataWatch.
    It runs in a dedicated thread.
    """

    ### __init__ ###################################################################
    def __init__(self, iFace, db, pathToWatch, verbose=True):
        """
        Initialize shared state.
        """
        self._iFace = iFace
        self._db = db
        self._path = pathToWatch
        self._dbName = pathToWatch[11:]
        self._data = None
        self._verbose = verbose
        threading.Thread.__init__(self)

    ### run ########################################################################
    def run(self):
        """
        Watch for changes, and act upon them: create/drop databases.
        """
        @self._iFace._zk.DataWatch(self._path, allow_missing_node=True)
        def my_watcher_func(newData, stat):
            if newData == self._data: return
            if newData is None and stat is None:
                if self._verbose:
                    print "Path %s deleted. (was %s)" % (self._path, self._data)
                if self._db.checkDbExists(self._dbName):
                    self._db.dropDb(self._dbName)
            elif newData == 'PENDING':
                if self._verbose:
                    print "Meta not initialized yet for '%s'" % self._dbName
            elif newData == 'READY':
                if self._db.checkDbExists(self._dbName):
                    if self._verbose: print "Database '%s' exists." % self._dbName
                else:
                    if self._verbose: print "Creating database '%s'" % self._dbName
                    self._db.createDb(self._dbName)
            else:
                print "Unsupported status '%s' for db '%s'" % \
                    (newData, self._dbName)
            self._data = newData

####################################################################################
####################################################################################
####################################################################################
class AllDbsWatcher(threading.Thread):
    """
    This class implements watcher that watches for new znodes that represent
    databases. A new OnedbWatcher is setup for each new znode that is creeated. 
    If ensures only one watcher runs, even if a database is created/deleted/created
    again. It currently does NOT do any garbage collection of threads for deleted
    databases. It is relying on Zookeeper's ChildrenWatch. It runs in a dedicated
    thread.
    """

    ### __init__ ###################################################################
    def __init__(self, iFace, db):
        """
        Initialize shared data.
        """
        self._iFace = iFace
        self._db = db
        self._path =  "/DATABASES"
        self._children = []
        self._watchedDbs = [] # registry of all watched databases
        # make sure the path exists
        if not iFace.exists(self._path): iFace.create(self._path)
        threading.Thread.__init__(self)

    ### run ########################################################################
    def run(self):
        """
        Watch for new/deleted nodes, and act upon them: setup new watcher for each
        new database. Note, if the path "/DATABASES" is deleted, this will fail
        with an error: No handlers could be found for logger
        "kazoo.handlers.threading".
        """
        @self._iFace._zk.ChildrenWatch(self._path)
        def my_watcher_func(children):
            # look for new entries
            for val in children:
                if not val in self._children:
                    print "node '%s' was added" % val
                    # set data watcher for this node (unless it is already up)
                    p2 = "%s/%s" % (self._path, val)
                    if p2 not in self._watchedDbs:
                        print "Setting a new watcher for '%s'" % p2
                        w = OneDbWatcher(self._iFace, self._db, p2)
                        w.start()
                        self._watchedDbs.append(p2)
                    else:
                        print "Already have a watcher for '%s'" % p2
                    self._children.append(val)
            # look for entries that were removed
            for val in self._children:
                if not val in children:
                    print "node '%s' was removed" % val
                    self._children.remove(val)

####################################################################################
####################################################################################
####################################################################################
def main():
    iFace = CssIFace(verbose=True)
    db = Db(host='localhost', port=3306, user='becla', passwd='') # FIXME
    try:
        w1 = AllDbsWatcher(iFace, db)
        w1.start()
        while True: time.sleep(60)
    except(KeyboardInterrupt, SystemExit):
        print ""

if __name__ == "__main__":
    main()
