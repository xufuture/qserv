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

Currently using a small .tsv  file from a "Object-only" table.

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

#----------------------------------
# Local non-exported definitions --
#----------------------------------

#------------------------
# Exported definitions --
#------------------------

# Directory holding the config files 
cfg_dir = '/home/vaikunth/src/partition/'


def RunIndex():
    """
    Run sph-htm-index as step 1 of the duplication process
    """

    # Specify the tables to run over
    tables = ['Object']

    for table in tables:        
        if os.path.isfile(cfg_dir+table+".cfg")==False:
            logging.error("Path to indexing config file not found")

        run_index = subprocess.Popen(["sph-htm-index",
                                      "--config-file=" + cfg_dir + table + ".cfg",
                                      "--htm.level=8",
                                      "--in.csv.null=NULL",
                                      "--in.csv.delimiter=\t",
                                      "--in.csv.escape=\\",
                                      "--in.csv.quote=\"",
                                      "--in=" + cfg_dir + table + ".tsv",
                                      "--verbose",
                                      "--mr.num-worker=6",
                                      "--mr.pool-size=32768",
                                      "--mr.block-size=16",
                                      "--out.dir=index/" + table])

        run_index.wait()


def RunDuplicate():
    """
    Run sph-duplicate to set up partitioned data that the data loader needs
    """
     
    # Specify the tables to run over
    tables = ['Object']

    # Run Indexer first
    RunIndex()
    
    for table in tables:
        if os.path.isfile(cfg_dir + "common.cfg")==False:
            logging.error("Path to duplicator config file not found")
            
        run_dupl = subprocess.Popen(["sph-duplicate",
                                     "--config-file=" + cfg_dir + table + ".cfg",
                                     "--config-file=" + cfg_dir + "common.cfg",
                                     "--in.csv.null=\\N",
                                     "--in.csv.delimiter=\t",
                                     "--in.csv.escape=\\",
                                     "--in.csv.no-quote=true",
                                     "--index=index/" + table + "/htm_index.bin",
                                     "--part.index=index/Object/htm_index.bin",                                    
                                     "--verbose",
                                     "--lon-min=60",
                                     "--lon-max=72",
                                     "--lat-min=-30",
                                     "--lat-max=-18",
                                     "--mr.num-worker=6",
                                     "--mr.pool-size=32768",
                                     "--mr.block-size=16",
                                     "--out.dir=chunks/" + table])

        run_dupl.wait()
