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
This module implements interface to the Central State System (CSS).

@author  Jacek Becla, SLAC


Known issues and todos:
 - connection information for the Kazoo Client needs to be configurable.
 - recover from lost connection by reconnecting
 - issue: watcher is currently using the "_zk", and bypasses the official API!
 - implement proper logging instead of print

"""

import time

from kazoo.client import KazooClient


####################################################################################
####################################################################################
####################################################################################
class CssException(Exception):
    """
    Exception raised by CSSInterface.
    """

    SUCCESS                     =    0
    ERR_DB_DOES_NOT_EXIST       = 2001
    ERR_KEY_ALREADY_EXISTS      = 2002
    ERR_KEY_DOES_NOT_EXIST      = 2003
    ERR_KEY_INVALID             = 2004
    ERR_NOT_IMPLEMENTED         = 9998
    ERR_INTERNAL                = 9999

    ################################################################################
    def __init__(self, errNo, extraMsgList=None):
        """
        Initialize the shared data.

        @param errNo      Error number.
        @param extraMsgList  Optional list of extra messages.
        """
        self._errNo = errNo
        self._extraMsgList = extraMsgList

        self._errors = { 
            CssException.ERR_DB_DOES_NOT_EXIST: ("Database does not exist."),
            CssException.ERR_KEY_ALREADY_EXISTS: ("Key already exists."),
            CssException.ERR_KEY_INVALID: ("Invalid key."),
            CssException.ERR_KEY_DOES_NOT_EXIST: ("Key does not exist."),
            CssException.ERR_NOT_IMPLEMENTED: ("Fature not implemented yet."),
            CssException.ERR_INTERNAL: "Internal error."
        }

    ################################################################################
    def __str__(self):
        """
        Return string representation of the error.

        @return string  Error message string, including all optional messages.
        """
        msg = self._errors.get(self._errNo, "Undefined css error.")
        if self._extraMsgList is not None: 
            for s in self._extraMsgList: msg += " (%s)" % s
        return msg

    ################################################################################
    def getErrNo(self):
        """
        Return error number.
        """
        return self._errNo

####################################################################################
####################################################################################
####################################################################################
class CssInterface(object):
    """
    @brief CssInterface class defines interface to the Central State Service CSS).
    """

    ################################################################################
    def __init__(self, verbose=True):
        """
        Initialize the interface.

        @param verbose Verbose flag, default is True.
        """
        self._zk = KazooClient(hosts='127.0.0.1:2181') # FIXME
        self._zk.start()
        self._verbose = verbose

    ################################################################################
    def create(self, k, v='', sequence=False):
        """
        Add a new key/value entry. Create entire path as necessary. 

        @param sequence  Sequence flag -- if set to True, a 10-digid, 0-padded
                         suffix (unique sequential number) will be added to the key.
        @return string   Real path to the just created node.
        """
        # check if the key exists
        if self._zk.exists(k):
            raise CssException(CssException.ERR_KEY_ALREADY_EXISTS, [k])
        p = self._chopLastSection(k)
        if p is None:
            raise CssException(CssException.ERR_KEY_INVALID, [k])
        if self._verbose: print "cssInterface: CREATE '%s' --> '%s'" % (k, v) 
        return self._zk.create(k, v, sequence=sequence, makepath=True)

    ################################################################################
    def exists(self, k):
        """
        Check if a given key exists.

        @param k Key.

        @return boolean  True if the key exists, False otherwise.
        """
        return self._zk.exists(k)

    ################################################################################
    def get(self, k):
        """
        Get value for a key. Raise exception if the key doesn't exist.

        @param k   Key.

        @return string  Value for a given key. 
        """
        if not self._zk.exists(k):
            raise CssException(CssException.ERR_KEY_DOES_NOT_EXIST, [k])
        v, stat = self._zk.get(k)
        if self._verbose: print "cssInterface: GET '%s' --> '%s'" % (k, v)
        return v

    ################################################################################
    def getChildren(self, k):
        """
        Get children for a given key. Raise exception if the key doesn't exist.

        @param k   Key.

        @return    List_of_strings  A list of children for a given key. 
        """
        if not self._zk.exists(k):
            raise CssException(CssException.ERR_KEY_DOES_NOT_EXIST, [k])
        if self._verbose: print "cssInterface: GETCHILDREN '%s'" % (k)
        return self._zk.get_children(k)

    ################################################################################
    def set(self, k, v):
        """
        Set value for a given key. Raise exception if the key doesn't exist.

        @param k  Key.
        @param v  Value.
        """
        # check if the key exists
        if not self._zk.exists(k):
            raise CssException(CssException.ERR_KEY_DOES_NOT_EXIST, [k])
        v1, stat = self._zk.get(k)
        if self._verbose: print "cssInterface: SET '%s' --> '%s'" % (k, v)
        self._zk.set(k, v)
        v2, stat = self._zk.get(k)

    ################################################################################
    def delete(self, k, recursive=False):
        """
        Delete a key, including all children if recursive flag is set.

        @param k         Key.
        @param recursive Flag. If set, all existing children nodes will be
                         deleted.

        Delete a key, including all children if recursive flag is set. Raise
        exception if the key doesn't exist.
        """
        if not self._zk.exists(k):
            raise CssException(CssException.ERR_KEY_DOES_NOT_EXIST, [k])
        if self._verbose: print "cssInterface: DELETE '%s'" % (k)
        self._zk.delete(k, recursive=recursive)

    ################################################################################
    def deleteAll(self, p):
        """
        Delete everything recursively starting from a given point in the tree.

        @param p  Path.
        """
        if self._zk.exists(p):
            self._deleteOne(p)

    ################################################################################
    def printAll(self):
        """
        Print entire contents to stdout.
        """
        self._printOne("/")

    ################################################################################
    def startTransaction(self):
        """
        Start transaction and return transactionRequest instance.
        """
        return self._zk.transaction()

    ################################################################################
    def _chopLastSection(self, k):
        """
        Remove substring after last '/', e.g. for /xx/y/abc it'll return /xx/y.

        @param k  Key.

        @return string
        """
        x = k.rfind('/')
        if x == -1: return None
        return k[0:x]

    ################################################################################
    def _printOne(self, p):
        """
        Print content of one znode. Note, this function is recursive.

        @param p  Path.

        @return   string
        """
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
    def _deleteOne(self, p):
        """
        Delete one znode. Note, this function is recursive.

        @param p  Path.
        """
        children = self._zk.get_children(p)
        for child in children:
            if p == "/":
                if child != "zookeeper": # skip "/zookeeper"
                    self._deleteOne("%s%s" % (p, child))
            else:
                self._deleteOne("%s/%s" % (p, child))
        if p != "/": 
            self._zk.delete(p)
