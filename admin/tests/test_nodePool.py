#!/usr/bin/env python

# LSST Data Management System
# Copyright 2015 AURA/LSST.
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
This is a unit test for workerAdmin module.

@author  Andy Salnikov, SLAC

"""

import logging
import os
import socket
import tempfile
import unittest
from threading import Thread

import lsst.qserv.admin.workerAdmin as workerAdmin
import lsst.qserv.admin.qservAdmin as qservAdmin
import lsst.qserv.admin.nodePool as nodePool


logging.basicConfig(level=logging.INFO)
_LOG = logging.getLogger('TEST')

def _makeAdmin(data=None):
    """
    Create QservAdmin instance with some pre-defined data.
    """
    if data is None:
        # read from /dev/null
        connection = '/dev/null'
    else:
        # make temp file and save data in it
        file = tempfile.NamedTemporaryFile(delete=False)
        connection = file.name
        file.write(data)
        file.close()

    # make an instance
    config = dict(technology='mem', connection=connection)
    admin = qservAdmin.QservAdmin(config=config)

    # remove tmp file
    if connection != '/dev/null':
        os.unlink(connection)

    return admin


class TestNodePool(unittest.TestCase):

    def test_NodePoolExecParallel(self):
        """ Check execution of simple command in parallel, TODO change CWD
        """

        nb_node=3

        # setup for direct connection
        wAdmins = [workerAdmin.WorkerAdmin(host="localhost", runDir="/bin") for x in range(nb_node)]
        nPool = nodePool.NodePool(wAdmins)
        output = nPool.execParallel('./ls')
        self.assertIs(output, None)

#
#     def test_NodePoolExecParallel_Fail(self):
#         """ Check execution of simple command, failure results in exception """
#
#         # setup for direct connection
#         wAdmin = workerAdmin.WorkerAdmin(host="localhost", runDir='/tmp')
#
#         self.assertRaises(Exception, wAdmin.execCommand, '/bin/false')


####################################################################################

if __name__ == "__main__":
    suite = unittest.TestLoader().loadTestsFromTestCase(TestNodePool)
    unittest.TextTestRunner(verbosity=3).run(suite)
