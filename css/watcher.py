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
Database Watcher - runs on each Qserv node and maintains Qserv databases
(creates databases, deletes databases, creates tables, drops tables etc).
"""

import time
import threading

from cssIFace import CssIFace
from cssStatus import CssException

#################################################################################
#### OneDbWatcher
#################################################################################
class OneDbWatcher(threading.Thread):
    """This class implements watcher that watches for changes to one znode
       that represents a database. It is based on Zookeeper's DataWatch."""
    def __init__(self, iFace, pathToWatch):
        self._iFace = iFace
        self._path = pathToWatch
        self._data = None
        threading.Thread.__init__(self)

    def run(self):
        @self._iFace._zk.DataWatch(self._path, allow_missing_node=True)
        def my_watcher_func(data, stat):
            if data != self._data:
                if data is None:
                    print "Path %s deleted. (was %s)" % (self._path, self._data)
                else:
                    print "Path %s changed. (%s --> %s, version %s)" % \
                    (self._path, self._data, data, stat.version)
            else:
                print "Path %s updated. Same value (%s)" % (self._path, data)
            self._data = data
        while True:
            time.sleep(60)


#################################################################################
#### AllDbsWatcher
#################################################################################
class AllDbsWatcher(threading.Thread):
    """This class implements watcher that watches for new znodes that
       represent databases. A new dbWatcher is setup for each new znode that 
       is created. It is based on Zookeeper's ChildrenWatch."""
    def __init__(self, iFace):
        self._iFace = iFace
        self._path =  "/DATABASES"
        self._children = []
        self._watchedDbs = [] # registry of all watched databases
        # make sure the path exists
        if not iFace.exists(self._path): iFace.create(self._path)
        threading.Thread.__init__(self)

    def run(self):
        # known issue: if the path is deleted, this will fail with:
        # No handlers could be found for logger "kazoo.handlers.threading"
        @self._iFace._zk.ChildrenWatch(self._path)
        def my_watcher_func(children):
            # look for new entries
            print "children:", children
            for val in children:
                if not val in self._children:
                    print "node '%s' was added" % val
                    # set data watcher for this node (unless it is already up)
                    p2 = "%s/%s" % (self._path, val)
                    if p2 not in self._watchedDbs:
                        print "setting new watcher for '%s'" % p2
                        w = OneDbWatcher(self._iFace, p2)
                        w.start()
                        self._watchedDbs.append(p2)
                    else:
                        print "already have watcher for '%s'" % p2
                    self._children.append(val)
            # look for entries that were removed
            for val in self._children:
                if not val in children:
                    print "node '%s' was removed" % val
                    self._children.remove(val)
        while True:
            time.sleep(60)

def main():
    iFace = CssIFace()

    # watch for database changes (new db, deleted db)
    w1 = AllDbsWatcher(iFace)
    w1.start()

    #w2 = DataWatcher(iFace, "/watchTest/a")
    #w2.start()
    #w3 = DataWatcher(iFace, "/watchTest/b")
    #w3.start()

if __name__ == "__main__":
    main()
