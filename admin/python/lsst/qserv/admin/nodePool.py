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

    def __init__(self, workerAdmins=None, max_task=None):
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

        if max_task:
            self._max_task = max_task
        else:
            self._max_task = cpu_count()

    def execParallel(self, command):
        """
        Exec commands in parallel in multiple process
        (as much as we have CPU)
        """
        nodes_remaining = self.nodes

        if not command: return # empty list

        def done(p):
            return p.poll() is not None
        def success(p):
            return p.returncode == 0

        max_task = cpu_count()
        _LOG.info("max_task: %s", max_task)
        processes = []
        process_id=0
        nodes_failed = []
        while True:

            # batch processes
            while nodes_remaining and len(processes) < max_task:
                process_id += 1
                node = nodes_remaining.pop()

                task = node.getSshCmd(command);
                _LOG.info("Running: %s", list2cmdline(task))
                out = "{0}-{1}-stdout.txt".format(node.host, process_id)
                err = "{0}-{1}-stderr.txt".format(node.host, process_id)
                with open(out,"wb") as out, open(err,"wb") as err:
                    processes.append([Popen(task, stdout=out,stderr=err), node.host, process_id])

            # check for ended processes
            for p in processes:
                [process, host, p_id] = p
                if done(process):
                    processes.remove(p)
                    if not success(process):
                        _LOG.error("Failed to run command #%s on host %s", p_id, host)
                        nodes_failed.append((process_id, host))

            if not processes and not nodes_remaining:
                break
            else:
                time.sleep(0.05)

        if nodes_failed:
            _LOG.error("Command failed on nodes: %s" nodes_failed)
        else:
            _LOG.info("Command succeed on all nodes")
