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
This is a unittest for the Db class.
"""

import time
import unittest
from db import Db, DbException, DbStatus

class TestDb(unittest.TestCase):
    def setUp(self):
        None

    def test1(self):
        self._db = Db(host='localhost',
                      port=3306,
                      user='becla',
                      passwd='',
                      socket=None)
        self._db.createDb("myDb1")
        self._db.connectToDb("myDb1")
        self._db.createTable("t1", "(i int)")
        self._db.createDb("myDb2")
        self._db.createTable("t2a", "(i int)")
        self._db.createTableInDb("myDb2", "t2a", "(i int)")
        self._db.dropDb("myDb2")
        self._db.createTable("t2b", "(i int)")
        self._db.createDb("myDb2")
        self._db.createTableInDb("myDb2", "t2b", "(i int)")
        # this db already exists, so that should fail
        self.assertRaises(DbException, self._db.createDb, "myDb1")
        # this table already exists, so that should fail
        self.assertRaises(DbException, self._db.createTable, "t2a", "(i int)")
        # simply cleanup after yourself
        self._db.dropDb("myDb1")
        self._db.dropDb("myDb2")

def main():
    # try:
    unittest.main()
    # except DbException as e:
    #     print "Oops", e.getErrMsg()


if __name__ == "__main__":
    main()
