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

class DataWatcher(threading.Thread):
    """This class implementes watcher that watches for changes to one znode."""
    def __init__(self, iFace, pathToWatch):
        self._iFace = iFace
        self._path = pathToWatch
        threading.Thread.__init__(self)
            # temporarily here, for testing
        self._data = "a"
        if self._iFace.exists(self._path): self._iFace.delete(self._path)
        self._iFace.create(self._path, self._data)

    def run(self):
        @self._iFace._zk.DataWatch(self._path)
        def my_watcher_func(data, stat):
            if data != self._data:
                print "Path %s changed: %s --> %s (version is: %s)" % \
                (self._path, self._data, data, stat.version)
            else:
                print "Path %s updated, same value: %s (version is: %s)" % \
                (self._path, data, stat.version)
            self._data = data
        while True:
            time.sleep(60)

def main():
    iFace = CssIFace()

    w1 = DataWatcher(iFace, "/watchTest/a")
    w1.start()

    w2 = DataWatcher(iFace, "/watchTest/b")
    w2.start()

if __name__ == "__main__":
    main()
