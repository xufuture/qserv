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
Compute basic queries results without using Qserv.
Used at IN2P3 cluster for the Summer 2015 test dataset.
Allow to check Qserv queries correctness.

@author  Fabrice JAMMES, IN2P3
"""
from __future__ import print_function

# -------------------------------
#  Imports of standard modules --
# -------------------------------
import argparse
import logging
import os
import pprint
import random
from sets import Set
import subprocess 
import time
import yaml 

# ----------------------------
# Imports for other modules --
# ----------------------------
import MySQLdb

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------

def main():

    default_output_dir = os.path.join(
        os.path.expanduser("~"), "regression_out")

    parser = argparse.ArgumentParser(
        description="Qserv non-regression tool"
        )

    parser.add_argument('-v', '--verbose', dest='verbose', default=['v', 'v', 'v'],
                        action='append_const',
                        const=None,
                        help='More verbose output, can use several times.')

    parser.add_argument("-O", "--output-dir", dest="output_dir",
                        default=default_output_dir,
                        help="Absolute path to output directory. "
                        "Default: %(default)s"
                        )

    args = parser.parse_args()

    try:
        os.makedirs(args.output_dir)
    except OSError:
        print("XXXXXXX ", os.path.isdir(args.output_dir))
        if not os.path.isdir(args.output_dir):
            raise

    verbosity = len(args.verbose)
    levels = {0: logging.ERROR, 1: logging.WARNING, 2: logging.INFO,
              3: logging.DEBUG}
    logformat = "[%(levelname)s] %(name)s: %(message)s"
    level = levels.get(verbosity)
    logfile = os.path.join(args.output_dir, "regression.log")
    #logging.basicConfig(format=logformat, level=level, filename=logfile)
    logging.basicConfig(format=logformat, level=level)

    cmd='''cp count.sh /tmp/count.sh && \
           echo /tmp/count.sh | \
           parallel --files --onall \
           --slf qserv.slf \"sh -c '{}'\"'''
    outfiles = subprocess.check_output(cmd, shell=True)

    logging.debug("Chunk queries results files (per node): %s", outfiles)

    chunk_results = []
    chunk_tables = Set()
    for f in outfiles.split():
        with open(f, 'r') as stream:
            try:
                r = yaml.safe_load(stream)
                chunk_results.append(r)
                for t in r['tables'].split():
                    if t in chunk_tables:
                        logging.fatal("Duplicate chunk table %s in node %s",
                                      t, r['node'])
                        raise ValueError("Duplicate chunk table")
                    elif t != "Object_1234567890":
                        logging.debug("Adding chunk table %s from node %s",
                                      t, r['node'])
                        chunk_tables.add(t)
            except yaml.YAMLError as exc:
                print(exc)

    logging.debug("Chunk results: %s", chunk_results)


if __name__ == "__main__":
    main()
