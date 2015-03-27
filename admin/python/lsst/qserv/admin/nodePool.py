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
Module defining WorkerAdmin class and related methods.

WorkerAdmin is an interface which controls or communicates with qserv
worker instance. Exact form of control is still to be defined, for now
we are going to support communication with mysql instance or ability to
start/stop worker processes. Current implementation is based on SSH
tunneling (or direct mysql connection if possible), in the future there
is supposed to be a special service for worker administration.

WorkerAdmin uses information about worker nodes defined in Qserv CSS, for
details see https://dev.lsstcorp.org/trac/wiki/db/Qserv/CSS#Node-related.
For testing purposes it it also possible to provide worker information as
a set of parameters to constructor.

@author  Fabrice Jammes, IN2P3/SLAC
"""

#--------------------------------
#  Imports of standard modules --
#--------------------------------
import logging
import os
from subprocess import Popen, list2cmdline
import sys
import time

#-----------------------------
# Imports for other modules --
#-----------------------------

#----------------------------------
# Local non-exported definitions --
#----------------------------------
_LOG = logging.getLogger(__name__)

def cpu_count():
    ''' Returns the number of CPUs in the system
    '''
    num = 1
    try:
        num = os.sysconf('SC_NPROCESSORS_ONLN')
    except (ValueError, OSError, AttributeError):
        pass

    return num

class NodePool(object):
    """
    Class representing administration/communication endpoint for qserv worker.
    """

    def __init__(self, workerAdmins=None):
        """
        Make new endpoint for remote worker. Worker can be specified
        either by its name or by the complete set of parameters. If name
        is given then all other parameters are taken from CSS and qservAdmin
        has to be specified as well. If name is not given then all other
        parameters (except qservAdmin) need to be specified.

        This will throw if node with given name is not defined in CSS.

        @param name:        Name of the worker as defined in CSS.
        @param qservAdmin:  QservAdmin instance, required if name is provided.
        @param host:        worker host name, required if name is not provided.
        @param runDir:      qserv run directory on remote host, optional. Only
                            needed if name is not provided and methods that need
                            this parameter are called.
        @param mysqlConn:   comma-separated set of mysql connection options.
                            Optional, only needed if name is not provided and methods
                            that need this parameter are called.
        """


        self.nodes = workerAdmins

    def execParallel(self, command):
        """
        Exec commands in parallel in multiple process
        (as much as we have CPU)
        """
        nodes_to_run = self.nodes

        if not command: return # empty list

        def done(p):
            return p.poll() is not None
        def success(p):
            return p.returncode == 0
        def fail():
            sys.exit(1)

        max_task = cpu_count()
        _LOG.info("max_task: %s", max_task)
        processes = []
        while True:
            while nodes_to_run and len(processes) < max_task:
                node = nodes_to_run.pop()

                cmd = ""
                if node.runDir:
                    cmd = "cd '{0}'; ".format(node.runDir)
                    cmd += command

                task = node.getSshCmd() + [cmd];
                _LOG.info("Running: %s", list2cmdline(task))
                with open(str(node)+"-stdout.txt","wb") as out, open(str(node)+"-stderr.txt","wb") as err:
                    processes.append(Popen(task, stdout=out,stderr=err))

            for p in processes:
                if done(p):
                    if success(p):
                        processes.remove(p)
                    else:
                        fail()

            if not processes and not nodes_to_run:
                break
            else:
                time.sleep(0.05)
