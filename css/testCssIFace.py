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
This is a unittest for the Central State System Interface class.
"""

import time
import unittest
from cssIFace import CssIFace
from cssStatus import CssException

class TestCssIFace(unittest.TestCase):
    def setUp(self):
        self._iFace = CssIFace()

    def testCreateGetSetDelete(self):
        # first delete everything
        self._iFace.deleteAll("/unittest", False)
        # try second time, just for fun, that should work too
        self._iFace.deleteAll("/unittest", False)
        # define key/value for testing
        k1 = "/unittest/my/first/testX"
        k2 = "/unittest/my/testY"
        v1 = "aaa"
        v2 = "AAA"
        # create the key
        self._iFace.create(k1, v1)
        # and another one
        self._iFace.create(k2, v2)
        # get the first one
        v1a = self._iFace.get(k1)
        assert(v1a == v1)
        # try to create it again, this should fail
        self.assertRaises(CssException, self._iFace.create, k1, v1)
        # set the value to something else
        self._iFace.set(k1, v2)
        # get it
        v2a = self._iFace.get(k1)
        assert(v2a == v2)
        # delete it
        self._iFace.delete(k1)
        # try deleting it again, this should fail
        self.assertRaises(CssException, self._iFace.delete, k1)
        # try to get it, it should fail
        self.assertRaises(CssException, self._iFace.get, k1)
        # try to set it, it should fail
        self.assertRaises(CssException, self._iFace.set, k1, v1)
        # get the second key
        v2a = self._iFace.get(k2)
        assert(v2a == v2)
        # print everything
        self._iFace.printAll()

    def testBadKeys(self):
        # try to create invalid key
        self.assertRaises(CssException, self._iFace.create, "badKey", "v")

    #def testPerformance(self):
    #    n = 10 # set it to something larger for real test...
    #    start = time.clock()
    #    for i in range(1,n+1):
    #        k = "unittest/node_%02i" % i
    #        # print ("creating %s --> %i" % (k, i))
    #        self._iFace.create(k, str(i))
    #    elapsed = time.clock()-start
    #    print "It took", elapsed, "to create", n, "entries"

def main():
    unittest.main()

if __name__ == "__main__":
    main()


