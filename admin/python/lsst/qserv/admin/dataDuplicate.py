# LSST Data Management System
# Copyright 2014 AURA/LSST.
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
Script that runs data duplicator for integration tests.

Simply running the duplicator as listed in the documentation example here:
https://github.com/LSST/partition/blob/master/docs/duplication.md

@author  Vaikunth Thukral, TAMU/SLAC
"""

#--------------------------------
#  Imports of standard modules --
#--------------------------------
import logging
import os
import subprocess

#-----------------------------
# Imports for other modules --
#-----------------------------
from lsst.qserv.admin import commons

#----------------------------------
# Local non-exported definitions --
#----------------------------------

#------------------------
# Exported definitions --
#------------------------

class DataDuplicator(object):

    def __init__(self, data_reader, cfg_dir, out_dir):

        self.dataConfig = data_reader
        self.logger = logging.getLogger(__name__)

        self.cfg_dirname = cfg_dir
        self.out_dirname = out_dir

    def runIndex(self):
    """
    Run sph-htm-index as step 1 of the duplication process
    """
        
    for table in tables:        
        if os.path.isfile(cfg_dir+table+".cfg")==False:
            self.logger.error("Path to indexing config file not found")
            
        run_index = commons.run_command(["sph-htm-index",
                                         "--config-file=" + cfg_dir + table + ".cfg",
                                         "--config-file=" + cfg_dir + "common.cfg",
                                         "--in=" + cfg_dir + table + ".txt",
                                         "--verbose",
                                         "--out.dir=" + out_dir +"index/" + table])
                    
                    
    def runDuplicate(self):
    """
    Run sph-duplicate to set up partitioned data that the data loader needs
    """

    # Run Indexer first
    runIndex()
    
    for table in tables:
        if os.path.isfile(cfg_dir + "common.cfg")==False:
            self.logger.error("Path to duplicator config file not found")
                            
        run_dupl = commons.run_command(["sph-duplicate",
                                        "--config-file=" + cfg_dir + table + ".cfg",
                                        "--config-file=" + cfg_dir + "common.cfg",
                                        "--index=" + out_dir + "index/" + table + "/htm_index.bin",
                                        "--part.index=" + out_dir + "index/Object/htm_index.bin",                                    
                                        "--verbose",
                                        "--lon-min=60",
                                        "--lon-max=72",
                                        "--lat-min=-30",
                                        "--lat-max=-18",
                                        "--out.dir=" + out_dir + "chunks/" + table])
