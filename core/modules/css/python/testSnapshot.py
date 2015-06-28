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


"""Unit test for lsst.qserv.css.snapshot"""

import json
import time

from snapshot import Snapshot
from kvInterface import KvInterface

class FakeStat:
    def __init__(self, lastModified):
        self.lastModified = lastModified
        pass
    pass

class FakeZk:
    def __init__(self):
        dummyStat = FakeStat(lastModified=time.time())
        dummyData = ("(dummy)", dummyStat)
        def makeFakeData(txt):
            return (txt, dummyStat)
        recipe = json.dumps({ "name":"Apple Pie",
                              "cost":"$200",
                              "generation":3,
                              })
        lockData = json.dumps({"password" : "123password",
                               "lastSet" : time.time() })

        self.getDict = {
            "/" : dummyData,
            "/css_meta" : dummyData,
            "/DBS/alice" : dummyData,
            "/DBS/bob" : dummyData,
            "/DBS/eve" : dummyData,
            "/DBS/alice/secret" : makeFakeData("My dog has fleas"),
            "/DBS/alice/secret.json" : makeFakeData(recipe),
            "/DBS/eve/LOCK" : dummyData,
            "/DBS/eve/LOCK.json" : makeFakeData(lockData),
            "/PARTITIONING/1" : dummyData,
            "/PARTITIONING/2" : dummyData
            }
        self.getChildDict = {
            "/" : "css_meta DBS".split(),
            "/DBS" : "alice bob eve".split(),
            "/DBS/alice" : "secret secret.json".split(),
            "/DBS/eve" : "LOCK LOCK.json".split(),
            "/PARTITIONING" : "1 2".split()
            }
        pass
    def get(self, key):
        return self.getDict[key]
    def get_children(self, key):
        ret = self.getChildDict.get(key, [])
        return ret

    def visitPrefix(self, p, nodeFunc, acceptFunc=lambda n: True):
        children = self.get_children(p)
        nodeFunc(p)
        if p == "/": p = ""
        for child in children:
            self.visitPrefix(p + "/" + child, nodeFunc, acceptFunc)

class FakeLock:
    def __enter__(self):
        pass
    def __exit__(self, type, value, traceback):
        pass

class TestKvi(KvInterface):
    def __init__(self):
        self.zk = FakeZk()

    def get(self, key):
        return self.zk.get(key)[0]

    def getChildren(self, key):
        return self.zk.get_children(key)

    def getLockObject(self, key, id):
        return FakeLock()

class Test:
    """Test basic css module behavior"""

    def testFake(self):
        kvi = TestKvi()
        s = Snapshot(kvi)
        print "AAA"
        mykvi = KvInterface.newImpl(config={
                "technology" : "mem",
                "connection" : s.snapshot})
        print "BBB"
        mykvi.dumpAll()
        print "CCC"
        getFunc = mykvi.get
        assert mykvi.get("/DBS/alice/secret") == "My dog has fleas"
        assert mykvi.get("/DBS/eve/LOCK/password") == "123password"

    def go(self):
        self.testFake()


t = Test()
t.go()
