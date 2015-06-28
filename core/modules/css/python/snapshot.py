#!/usr/bin/env python

# LSST Data Management System
# Copyright 2014-2015 AURA/LSST.
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


""" lsst.qserv.css.snapshot: a module for taking css snapshots for use
in lower levels (c++ execution).
"""

# standard library imports
import logging
import json
import os
import socket
import sys
import time

# third-party software imports
from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError, NoNodeError

# local imports
from lsst.db.exception import produceExceptionClass
from lsst.qserv.css import KvInterfaceImplMem
from kvInterface import KvInterface

class Unpacker:

    # this produces a string in a form: 198.129.220.176_2899, used for unique id
    _addrPort = str(socket.getaddrinfo(socket.gethostname(), None)[0][4][0]) + '_' + str(os.getpid())

    def __init__(self, target):
        """accepts css.python KvInterface instance"""
        self.target = target
        self._uniqueLockId = 0
        pass

    def readTree(self, inputKvi):
        def acceptFunc(self, path):
            return path != "/zookeeper"

        def nodeFunc(path):
            packRoot = inputKvi.isPacked(path)
            if packRoot:
                obj = inputKvi.getUnpacked(path)
                self.insertObj(packRoot, obj)
            else:
                data = inputKvi.get(path)
                #print "Creating kvi key: %s = %s" % (path, data)
                self.target.set(str(path), str(data))
            pass

        # Create "/" in the snapshot
        self.target.set("/", str(inputKvi.get("/")))

        # Take a snapshot of version
        inputKvi.visitPrefix("/css_meta", nodeFunc, acceptFunc)

        # Take a snapshot of /DBS tree, pay attention to locks
        dbs = inputKvi.getChildren("/DBS")
        for db in dbs:
            dbKey = "/DBS/%s" % db
            with inputKvi.getLockObject(dbKey, self._uniqueId()):
                inputKvi.visitPrefix(dbKey, nodeFunc, acceptFunc)

        # Take a snapshot of /PARTITIONING tree. Locking is not
        # necessary (partitioning information is guaranteed to be
        # available for each db that was successfully added to the
        # /DBS snapshot a moment ago. Why? We are creating it when
        # db info is locked, and we never delete partitioning info).
        pts = inputKvi.getChildren("/PARTITIONING")
        for pt in pts:
            ptKey = "/PARTITIONING/%s" % pt
            inputKvi.visitPrefix(ptKey, nodeFunc, acceptFunc)


    def insertObj(self, path, obj):
        """Save an object into the kvi. This is equivalent to
        self.kvi.create if the object is primitive (e.g., a
        string). If the object is non-primitive, (e.g., it is
        a list or a dict), then expand it and insert its
        elements."""
        if isinstance(obj, list):
            # For now, apply comma separation for list elements.
            if not obj: # Empty list
                self.target.create(path, "")
            else:
                self.target.create(path, ",".join(map(str, obj)))
        elif isinstance(obj, dict):
            p = path
            if p.endswith("/"): p = path[:-1] # clip the /
            self.ensureKey(path)
            for k in obj:
                self.insertObj(path + "/" + k, str(obj[k]))
        else: self.target.set(str(path), str(obj))

    def ensureKey(self, path):
        """Ensure that a self.kvi.exists(path) evaluates to
        true. The entry is inserted with an empty string if it
        does not already exists."""
        if not self.target.exists(str(path)):
            self.target.create(str(path), "")
        pass

    def _uniqueId(self):
        self._uniqueLockId += 1
        return Unpacker._addrPort + '_' + str(self._uniqueLockId)


class Snapshot(object):
    """
    @brief Constructs in-memory snapshot of data realated to databases from Central State Service (CSS).

    It takes snapshot of /DBS and /PARTITIONING branches. It plays nicely with our internal locking
    mechanism, thus it is save even if new databases/tables are created or deleted.
    """
    def __init__(self, kvi):
        """
        Create a snapshot of the specified kvi.
        """
        self.snapshot = KvInterfaceImplMem()
        u = Unpacker(self.snapshot)
        u.readTree(kvi)

    def dump(self):
        """Read the current central CSS state and dump it.
        @return line-delimited CSS state."""
        class NodePrinter:
            def __init__(self):
                self.entries = []
                pass
            def dataFunc(self, path, data):
                self.entries.append(
                    path + '\t'
                    + (data if data else '\N'))
            pass
        np = NodePrinter()
        self.visitPrefix("/", np.dataFunc,
                    lambda p: p != "/zookeeper")
        return "\n".join(np.entries)
