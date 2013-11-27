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
This module defines the interface to the Central State Service (CSS)
.
"""

import time

from kazoo.client import KazooClient

from cssStatus import Status, CssException


# todo:
#  - recover from lost connection by reconnecting

class CssIFace(object):
    def __init__(self):
        self._zk = KazooClient(hosts='127.0.0.1:2181')
        self._zk.start()

    # -------------------------------------------------------------------------
    # Adds a new key/value entry. Creates entire path as necessary.
    def create(self, k, v):
        # check if the key exists
        if self._zk.exists(k):
            raise CssException(Status.ERR_KEY_ALREADY_EXISTS, k)
        p = self._extractPath(k)
        if p is None:
            raise CssException(Status.ERR_KEY_INVALID, k)
        self._zk.ensure_path(p)
        self._zk.create(k, v)
        
    # -------------------------------------------------------------------------
    # Returns value for a given key. Raises exception if the key doesn't exist.
    def get(self, k):
        if not self._zk.exists(k):
            raise CssException(Status.ERR_KEY_DOES_NOT_EXIST, k)
        data, stat = self._zk.get(k)
        return data

    # -------------------------------------------------------------------------
    # Sets value for a given key. Raises exception if the key doesn't exist.
    def set(self, k, v):
        # check if the key exists
        if not self._zk.exists(k):
            raise CssException(Status.ERR_KEY_DOES_NOT_EXIST, k)
        v1, stat = self._zk.get(k)
        self._zk.set(k, v)
        v2, stat = self._zk.get(k)

    # -------------------------------------------------------------------------
    # Deletes the key. Raises exception if the key doesn't exist.
    def delete(self, k):
        if not self._zk.exists(k):
            raise CssException(Status.ERR_KEY_DOES_NOT_EXIST, k)
        self._zk.delete(k, recursive=False)

    # -------------------------------------------------------------------------
    def watch(self, k):
        raise CssException(Status.ERR_NOT_IMPLEMENTED)

    # -------------------------------------------------------------------------
    # Deletes everything recursively starting from a given point in the tree.
    # Prints to stdout deleted entries if verbose is set to True.
    def deleteAll(self, p, verbose):
        if self._zk.exists(p):
            self._deleteOne(p, verbose)

    # -------------------------------------------------------------------------
    # Prints entire contents to stdout
    def printAll(self):
        self._printOne("/")

    # -------------------------------------------------------------------------
    # ---- P R I V A T E    M E T H O DS     B E L O W
    # ------------------------------------------------------------------------ 

    # -------------------------------------------------------------------------
    # Returns path for given key, e.g, for key /a/b/c/key, it'll return /a/b/c
    def _extractPath(self, k):
        x = k.rfind('/')
        if x == -1: return None
        return k[0:x]

    # -------------------------------------------------------------------------
    # Recursive print of the contents
    def _printOne(self, p):
        children = self._zk.get_children(p)
        if len(children) == 0:
            data, stat = self._zk.get(p)
            print p, "=", data
        else:
            print p
        for child in children:
            if p == "/":
                if child != "zookeeper":
                    self._printOne("%s%s" % (p, child))
            else:
                self._printOne("%s/%s" % (p, child))


    # -------------------------------------------------------------------------
    # Recursive delete
    def _deleteOne(self, p, verbose=True):
        children = self._zk.get_children(p)
        for child in children:
            if p == "/":
                if child != "zookeeper": # skip "/zookeeper"
                    self._deleteOne("%s%s" % (p, child), verbose)
            else:
                self._deleteOne("%s/%s" % (p, child), verbose)
        if p != "/": 
            if verbose: print "deleting", p
            self._zk.delete(p)
