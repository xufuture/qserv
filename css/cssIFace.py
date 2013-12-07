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

# todo:
#  - recover from lost connection by reconnecting
#  - issue: watcher is currently using the "_zk",
#    and bypasses the official API!


####################################################################################
#### CssStatus class. Defines erorr codes and messages used by the CssIFace
####################################################################################
class CssStatus:
    SUCCESS                     =    0
    ERR_KEY_ALREADY_EXISTS      = 2001
    ERR_KEY_DOES_NOT_EXIST      = 2002
    ERR_KEY_INVALID             = 2003
    ERR_NOT_IMPLEMENTED         = 9998
    ERR_INTERNAL                = 9999

    errors = { 
        ERR_KEY_ALREADY_EXISTS: ("Key already exists."),
        ERR_KEY_INVALID: ("Invalid key."),
        ERR_KEY_DOES_NOT_EXIST: ("Key does not exist."),
        ERR_NOT_IMPLEMENTED: ("This feature is not implemented yet."),
        ERR_INTERNAL: "Internal error."
    }

####################################################################################
#### CssException class. Defines Css-specific exception
####################################################################################
class CssException(Exception):
    def __init__(self, errNo, extraMsg1=None, extraMsg2=None):
        self._errNo = errNo
        self._extraMsg1 = extraMsg1
        self._extraMsg2 = extraMsg2

    def getErrMsg(self):
        msg = ''
        s = CssStatus()
        if self._errNo in s.errors: msg = s.errors[self._errNo]
        else: msg = "Undefined css error"
        if self._extraMsg1 is not None: msg += " (%s)" % self._extraMsg1
        if self._extraMsg2 is not None: msg += " (%s)" % self._extraMsg2
        return msg

    def getErrNo(self):
        return self._errNo

####################################################################################
#### CssIFace class.
####################################################################################
class CssIFace(object):
    """The CssIFace class defines the interface to the Central State Service CSS)."""

    def __init__(self, verbose=True):
        self._zk = KazooClient(hosts='127.0.0.1:2181')
        self._zk.start()
        self._verbose = verbose

    ################################################################################
    #### create
    ################################################################################
    def create(self, k, v='', sequence=False):
        """Adds a new key/value entry. Creates entire path as necessary. If 'sequence' is true, it will append a suffix (10 digits, 0 padded, unique sequential number). Returns real path to the just created node."""
        # check if the key exists
        if self._zk.exists(k):
            raise CssException(CssStatus.ERR_KEY_ALREADY_EXISTS, k)
        p = self._chopLastSection(k)
        if p is None:
            raise CssException(CssStatus.ERR_KEY_INVALID, k)
        if self._verbose: print "cssIface: CREATE '%s' --> '%s'" % (k, v) 
        return self._zk.create(k, v, sequence=sequence, makepath=True)

    ################################################################################
    #### exists
    ################################################################################
    def exists(self, k):
        """Checks if a given key exists. Returns True if it does, otherwise returns False."""
        return self._zk.exists(k)

    ################################################################################
    #### get
    ################################################################################
    def get(self, k):
        """Returns value for a given key. Raises exception if the key doesn't exist."""
        if not self._zk.exists(k):
            raise CssException(CssStatus.ERR_KEY_DOES_NOT_EXIST, k)
        v, stat = self._zk.get(k)
        if self._verbose: print "cssIface: GET '%s' --> '%s'" % (k, v)
        return v

    ################################################################################
    #### getChildren
    ################################################################################
    def getChildren(self, k):
        """Returns a list of children for a given key. Raises exception if the key doesn't exist."""
        if not self._zk.exists(k):
            raise CssException(CssStatus.ERR_KEY_DOES_NOT_EXIST, k)
        if self._verbose: print "cssIface: GETCHILDREN '%s'" % (k)
        return self._zk.get_children(k)

    ################################################################################
    #### set
    ################################################################################
    def set(self, k, v):
        """Sets value for a given key. Raises exception if the key doesn't exist."""
        # check if the key exists
        if not self._zk.exists(k):
            raise CssException(CssStatus.ERR_KEY_DOES_NOT_EXIST, k)
        v1, stat = self._zk.get(k)
        if self._verbose: print "cssIface: SET '%s' --> '%s'" % (k, v)
        self._zk.set(k, v)
        v2, stat = self._zk.get(k)

    ################################################################################
    #### delete
    ################################################################################
    def delete(self, k, recursive=False):
        """Deletes a key. If 'recursive' flag is set, it will delete all existing children nodes. Raises exception if the key doesn't exist."""
        if not self._zk.exists(k):
            raise CssException(CssStatus.ERR_KEY_DOES_NOT_EXIST, k)
        if self._verbose: print "cssIface: DELETE '%s'" % (k)
        self._zk.delete(k, recursive=recursive)

    ################################################################################
    #### deleteAll
    ################################################################################
    def deleteAll(self, p):
        """Deletes everything recursively starting from a given point in the tree."""
        if self._zk.exists(p):
            self._deleteOne(p)

    ################################################################################
    #### printAll
    ################################################################################
    def printAll(self):
        """Prints entire contents to stdout."""
        self._printOne("/")

    ################################################################################
    #### startTransaction
    ################################################################################
    def startTransaction(self):
        """Starts transaction and returns transactionRequest instance."""
        return self._zk.transaction()

    ################################################################################
    #### P R I V A T E     M E T H O D S     B E L O W
    ################################################################################

    ################################################################################
    #### _chopLastSection
    ################################################################################
    def _chopLastSection(self, k):
        """Removes substring after last '/', e.g. for /xx/y/abc it'll return /xx/y."""
        x = k.rfind('/')
        if x == -1: return None
        return k[0:x]

    ################################################################################
    #### _printOne
    ################################################################################
    def _printOne(self, p):
        """Recursive print of the contents."""
        t = self.startTransaction()
        children = None
        data = None
        stat = None
        if self.exists(p):
            children = self._zk.get_children(p)
            data, stat = self._zk.get(p)
        t.commit()

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
    def _deleteOne(self, p):
        """Recursive delete."""
        children = self._zk.get_children(p)
        for child in children:
            if p == "/":
                if child != "zookeeper": # skip "/zookeeper"
                    self._deleteOne("%s%s" % (p, child))
            else:
                self._deleteOne("%s/%s" % (p, child))
        if p != "/": 
            self._zk.delete(p)
