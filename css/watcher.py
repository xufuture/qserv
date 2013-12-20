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
 - need to go through cssInterface interface, now bypassing it in two places:
    - @self._interface._zk.DataWatch
    - @self._interface._zk.ChildrenWatch
 - considering implementing garbage collection for threads corresponding to
   deleted databases.
 - If all metadata is deleted when the watcher is running, the watcher will die
   with: ERROR:kazoo.handlers.threading:Exception in worker queue thread.
   To reproduce, just run (when watcher is running): 
       echo "drop everything;" | ./qserv_admin.py
"""

import logging
from optparse import OptionParser
import os
import time
import threading

from cssInterface import CssInterface
from db import Db, DbException


class OneDbWatcher(threading.Thread):
    """
    This class implements a database watcher. Each instance is responsible for
    creating / dropping one database. It is relying on Zookeeper's DataWatch.
    It runs in a dedicated thread.
    """

    def __init__(self, cssI, db, pathToWatch):
        """
        Initialize shared state.
        """
        self._cssI = cssI
        self._db = db
        self._path = pathToWatch
        self._dbName = pathToWatch[11:]
        self._data = None
        self._logger = logging.getLogger("WATCHER.DB_%s" % self._dbName)
        threading.Thread.__init__(self)

    def run(self):
        """
        Watch for changes, and act upon them: create/drop databases.
        """
        @self._cssI._zk.DataWatch(self._path, allow_missing_node=True)
        def my_watcher_func(newData, stat):
            if newData == self._data: return
            if newData is None and stat is None:
                self._logger.info(
                    "Path %s deleted. (was %s)" % (self._path, self._data))
                if self._db.checkDbExists(self._dbName):
                    self._logger.info("Dropping my database")
                    self._db.dropDb(self._dbName)
            elif newData == 'PENDING':
                    self._logger.info("Meta not initialized yet for my database.")
            elif newData == 'READY':
                if self._db.checkDbExists(self._dbName):
                    self._logger.info("My database already exists.")
                else:
                    self._logger.info("Creating my database")
                    self._db.createDb(self._dbName)
            else:
                self._logger.error("Unsupported status '%s' for my db." % newData)
            self._data = newData

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

    def __init__(self, cssI, db):
        """
        Initialize shared data.
        """
        self._cssI = cssI
        self._db = db
        self._path =  "/DATABASES"
        self._children = []
        self._watchedDbs = [] # registry of all watched databases
        # make sure the path exists
        if not cssI.exists(self._path): cssI.create(self._path)
        self._logger = logging.getLogger("WATCHER.ALLDBS")
        threading.Thread.__init__(self)

    def run(self):
        """
        Watch for new/deleted nodes, and act upon them: setup new watcher for each
        new database.
        """
        @self._cssI._zk.ChildrenWatch(self._path)
        def my_watcher_func(children):
            # look for new entries
            for val in children:
                if not val in self._children:
                    self._logger.info("node '%s' was added" % val)
                    # set data watcher for this node (unless it is already up)
                    p2 = "%s/%s" % (self._path, val)
                    if p2 not in self._watchedDbs:
                        self._logger.info("Setting a new watcher for '%s'" % p2)
                        w = OneDbWatcher(self._cssI, self._db, p2)
                        w.start()
                        self._watchedDbs.append(p2)
                    else:
                        self._logger.debug("Already have a watcher for '%s'" % p2)
                    self._children.append(val)
            # look for entries that were removed
            for val in self._children:
                if not val in children:
                    self._logger.info("node '%s' was removed" % val)
                    self._children.remove(val)

####################################################################################
class SimpleOptionParser:
    """
    Parse command line options.
    """

    def __init__(self):
        self._verbosityT = 40 # default is ERROR
        self._logFileName = None
        self._usage = \
"""

NAME
        watcher - Watches CSS and acts upon changes to keep node up to date.

SYNOPSIS
        watcher [OPTIONS]

OPTIONS
   -v
        Verbosity threshold. Logging messages which are less severe than
        provided will be ignored. Expected value range: 0=50: (CRITICAL=50,
        ERROR=40, WARNING=30, INFO=20, DEBUG=10). Default value is ERROR.
   -f
        Name of the output log file. If not specified, the output goes to stderr.
"""

    def getVerbosityT(self):
        """
        Return verbosity threshold.
        """
        return self._verbosityT

    def getLogFileName(self):
        """
        Return the name of the output log file.
        """
        return self._logFileName

    def parse(self):
        """
        Parse options.
        """
        parser = OptionParser(usage=self._usage)
        parser.add_option("-v", dest="verbT")
        parser.add_option("-f", dest="logF")
        (options, args) = parser.parse_args()
        if options.verbT: 
            self._verbosityT = int(options.verbT)
            if   self._verbosityT > 50: self._verbosityT = 50
            elif self._verbosityT <  0: self._verbosityT = 0
        if options.logF:
            self._logFileName = options.logF

####################################################################################
def main():
    # parse arguments
    p = SimpleOptionParser()
    p.parse()

    # configure logging
    if p.getLogFileName():
        logging.basicConfig(
            filename=p.getLogFileName(),
            format='%(asctime)s %(name)s %(levelname)s: %(message)s', 
            datefmt='%m/%d/%Y %I:%M:%S', 
            level=p.getVerbosityT())
    else:
        logging.basicConfig(
            format='%(asctime)s %(name)s %(levelname)s: %(message)s', 
            datefmt='%m/%d/%Y %I:%M:%S', 
            level=p.getVerbosityT())

    # disable kazoo logging if requested
    kL = os.getenv('KAZOO_LOGGING')
    if kL: logging.getLogger("kazoo.client").setLevel(int(kL))

    # initialize CSS
    cssI = CssInterface(p.getVerbosityT())
    db = Db(host='localhost', port=3306, user='becla', passwd='') # FIXME

    # setup the thread watching
    try:
        w1 = AllDbsWatcher(cssI, db)
        w1.start()
        while True: time.sleep(60)
    except(KeyboardInterrupt, SystemExit):
        print ""

if __name__ == "__main__":
    main()
