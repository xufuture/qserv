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

import time

from kazoo.client import KazooClient

from cssStatus import Status, CssException

# todo:
#  - recover from lost connection by reconnecting
#  - issue: watcher is currently using the "_zk",
#    and bypasses the official API!


class CssIFace(object):
    """The CssIFace class defines the interface to the Central State Service CSS).
    """

    def __init__(self):
        self._zk = KazooClient(hosts='127.0.0.1:2181')
        self._zk.start()

    ################################################################################
    #### create
    ################################################################################
    def create(self, k, v='', sequence=False):
        """Adds a new key/value entry. Creates entire path as necessary.
        If 'sequence' is true, it will append a suffix (10 digits, 0 padded,
        unique sequential number). Returns real path to the just created node."""
        # check if the key exists
        if self._zk.exists(k):
            raise CssException(Status.ERR_KEY_ALREADY_EXISTS, k)
        p = self._chopLastSection(k)
        if p is None:
            raise CssException(Status.ERR_KEY_INVALID, k)
        return self._zk.create(k, v, sequence=sequence, makepath=True)

    ################################################################################
    #### exists
    ################################################################################
    def exists(self, k):
        return self._zk.exists(k)

    ################################################################################
    #### get
    ################################################################################
    def get(self, k):
        """Returns value for a given key. Raises exception if the key doesn't
           exist."""
        if not self._zk.exists(k):
            raise CssException(Status.ERR_KEY_DOES_NOT_EXIST, k)
        data, stat = self._zk.get(k)
        return data

    ################################################################################
    #### getChildren
    ################################################################################
    def getChildren(self, k):
        """Returns a list of children for a given key. Raises exception if the
           key doesn't exist."""
        if not self._zk.exists(k):
            raise CssException(Status.ERR_KEY_DOES_NOT_EXIST, k)
        return self._zk.get_children(k)

    ################################################################################
    #### set
    ################################################################################
    def set(self, k, v):
        """Sets value for a given key. Raises exception if the key doesn't
           exist."""
        # check if the key exists
        if not self._zk.exists(k):
            raise CssException(Status.ERR_KEY_DOES_NOT_EXIST, k)
        v1, stat = self._zk.get(k)
        self._zk.set(k, v)
        v2, stat = self._zk.get(k)

    ################################################################################
    #### delete
    ################################################################################
    def delete(self, k, ignoreNonExist=False, recursive=False):
        """Deletes a key. Raises exception if the key doesn't exist."""
        if not self._zk.exists(k):
            if ignoreNonExist:
                return
            else:
                raise CssException(Status.ERR_KEY_DOES_NOT_EXIST, k)
        print "deleting:'%s'" % k
        self._zk.delete(k, recursive=recursive)
        # remove orphan znodes
        k = self._chopLastSection(k)
        if k != -1:
            children = self._zk.get_children(k)
            if len(children) == 0:
                print "requesting deleting of orphan znode:", k
                self.delete(k, recursive)

    ################################################################################
    #### deleteAll
    ################################################################################
    def deleteAll(self, p, verbose=True):
        """Deletes everything recursively starting from a given point in the
           tree. Prints to stdout deleted entries if verbose is set to True."""
        if self._zk.exists(p):
            self._deleteOne(p, verbose)

    ################################################################################
    #### printAll
    ################################################################################
    def printAll(self):
        """Prints entire contents to stdout."""
        self._printOne("/")

    ################################################################################
    #### P R I V A T E     M E T H O D S     B E L O W
    ################################################################################

    ################################################################################
    #### _chopLastSection
    ################################################################################
    def _chopLastSection(self, k):
        """Removes substring after last '/', e.g. for /xx/y/abc it'll return 
        /xx/y."""
        x = k.rfind('/')
        if x == -1: return None
        return k[0:x]

    ################################################################################
    #### _printOne
    ################################################################################
    def _printOne(self, p):
        """Recursive print of the contents."""
        children = self._zk.get_children(p)
        data, stat = self._zk.get(p)
        print p, "=", data
        for child in children:
            if p == "/":
                if child != "zookeeper":
                    self._printOne("%s%s" % (p, child))
            else:
                self._printOne("%s/%s" % (p, child))

    ################################################################################
    #### _deleteOne
    ################################################################################
    def _deleteOne(self, p, verbose=True):
        """Recursive delete."""
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
