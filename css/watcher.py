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

from cssIFace import CssIFace
from cssStatus import CssException

class Watcher(object):
    def __init__(self, pathToWatch):
        self._iFace = CssIFace()
        self._path = pathToWatch
        self._data = "a"                            # temporarily here, for testing
        self._iFace.deleteAll(self._path)           # temporarily here, for testing
        self._iFace.create(self._path, self._data)  # temporarily here, for testing

    def watch(self):
        @self._iFace._zk.DataWatch(self._path)
        def my_watcher_func(data, stat):
            if data != self._data:
                print "Path %s changed: %s --> %s (version is: %s)" % \
                (self._path, self._data, data, stat.version)
            else:
                print "Path %s updated, same value: %s --> %s (version is: %s)" % \
                (self._path, self._data, data, stat.version)
        while True:
            time.sleep(60)

def main():
    w = Watcher("/watchTest/a")
    w.watch()

if __name__ == "__main__":
    main()


