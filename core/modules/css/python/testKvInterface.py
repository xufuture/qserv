#!/usr/bin/env python

# LSST Data Management System
# Copyright 2013-2014 AURA/LSST.
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
This is a unittest for the Central State System Interface class.

@author  Jacek Becla, SLAC

"""

import ConfigParser
import logging
import os
import time
import unittest
#from kvInterface import KvInterface, KvException

from lsst.qserv.css import cssLib
from lsst.qserv.css.cssLib import KvInterfaceImplMySql

sqlConn = None
cfg = None

def initConfig():
    """
    @brief loads the configuration information needed to test KvInterface with a running mysql server
    @return a formatted MySqlConfig object
    """
    cfgFile = os.path.join(os.path.expanduser("~"), ".lsst", "KvInterfaceImplMySql-testRemote.txt")
    cfgParser = ConfigParser.ConfigParser()
    filesRead = cfgParser.read(cfgFile)
    if not cfgFile in filesRead:
        return None
    cfg = cssLib.MySqlConfig()
    cfg.username = cfgParser.get('mysql', 'user')
    cfg.password = cfgParser.get('mysql', 'passwd')
    cfg.hostname = cfgParser.get('mysql', 'host')
    cfg.port = cfgParser.getint('mysql', 'port')

    
def initConnection():
    """
    @brief connects to the database and sets up a table with the schema to run the tests
    @return True if conected & schema loaded, else false
    """
    # need config without database name
    sqlConfigLocal = sqlConfig
    sqlConfigLocal.dbName = ""
    print "config:%s" % sqlConfigLocal.asString();
    sqlConn = cssLib.SqlConnection(sqlConfigLocal)

    
def initDatabase():
    """
    todo
    """
    sqlConfig.dbName = "testCSSZ012sdrt";
    # todo: how to find the schema file at this location? or where should it go?
    # this mechanism of opening it seems way wrong.
    schemaFile = open('../../admin/templates/configuration/tmp/configure/sql/CssData.sql', 'r')
    # read whole file into buffer
    schema = schemaFile.read()
    # replace production schema name with test schema
    schema.replace("qservCssData", sqlConfig.dbName);
    errObj = cssLib.SqlErrorObject()
    sqlConn.runQuery(schema, errObj);
    if errObj.isSet():
        print "setupDatabase error:%s" %(errObj.printErrMsg())
        return False
    return True 


class TestKvInterface(unittest.TestCase):
    def setUp(self):
        print "running setUp"
        #self._kvI = KvInterface.newImpl(connInfo='127.0.0.1:12181')
        
        #cfg = getConfig()
        #if (cfg is None):
        #    self
        self._kvI = KvInterfaceImplMySql(cfg)

    def testCreateGetSetDelete(self):
        print "running testCreateGetSetDelete"
        
        # first delete everything
        #self._kvI.deleteKey("/unittest")
        # try second time, just for fun, that should work too
        #self._kvI.deleteKey("/unittest")
        # define key/value for testing
        k1 = "/unittest/my/first/testX"
        k2 = "/unittest/my/testY"
        v1 = "aaa"
        v2 = "AAA"
        # create the key
        print "calling create(%s, %s)" %(k1, v1)
        self._kvI.create(k1, v1)
        # and another one
        self._kvI.create(k2, v2)
        # get the first one
        v1a = self._kvI.get(k1)
        assert(v1a == v1)
        # try to create it again, this should fail
        self.assertRaises(KvException, self._kvI.create, k1, v1)
        # set the value to something else
        self._kvI.set(k1, v2)
        # get it
        v2a = self._kvI.get(k1)
        assert(v2a == v2)
        # delete it
        self._kvI.delete(k1)
        # try deleting it again, this should fail
        self.assertRaises(KvException, self._kvI.delete, k1)
        # try to get it, it should fail
        self.assertRaises(KvException, self._kvI.get, k1)
        # try to set it, it should fail
        self.assertRaises(KvException, self._kvI.set, k1, v1)
        # get the second key
        v2a = self._kvI.get(k2)
        assert(v2a == v2)
        # test getChildren
        self._kvI.getChildren("/unittest/")
        self.assertRaises(KvException, self._kvI.getChildren, "/whatever")
        # try to set for invalid key
        self.assertRaises(KvException, self._kvI.set, "/whatever", "value")
        # try to delete invalid key
        self.assertRaises(KvException, self._kvI.delete, "/whatever")
        # print everything
        self._kvI.dumpAll()

    #def testPerformance(self):
    #    n = 10 # set it to something larger for real test...
    #    start = time.clock()
    #    for i in range(1,n+1):
    #        k = "unittest/node_%02i" % i
    #        # print ("creating %s --> %i" % (k, i))
    #        self._kvI.create(k, str(i))
    #    elapsed = time.clock()-start
    #    print "It took", elapsed, "to create", n, "entries"


####################################################################################
#def setLogging():
#    logging.basicConfig(
#        #filename="/tmp/testKvInterface.log",
#        format='%(asctime)s %(name)s %(levelname)s: %(message)s',
#        datefmt='%m/%d/%Y %I:%M:%S',
#        level=logging.DEBUG)
#    logging.getLogger("kazoo.client").setLevel(logging.ERROR)

def main():
#    setLogging()
    print "running main"
    unittest.main()

if __name__ == "__main__":
    initConfig()
    initConnection()
    initDatabase()

    main()

#    cfg = getConfig()
#    if cfg is not None and cfg.isValid():
#        #todo check password - if not there, query. 
#        if setupDatabase(cfg):
#            main()
#        else:
#            print "error setting up database"
#    else:
#        print "error with config"
    #todo emit a warning if cfg is not valid or db not set up
 
