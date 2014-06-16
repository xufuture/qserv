#!/usr/bin/env python

from lsst.qserv.admin import commons, logger
from subprocess import check_output
import fileinput
import logging
import os
import argparse
import shutil


def parseArgs():
    parser = argparse.ArgumentParser(
            formatter_class=argparse.ArgumentDefaultsHelpFormatter
            )

    # 
    step_list = ['prepare','position']
    step_option_values = step_list + ['all']
    parser.add_argument("-s", "--step", dest="step", choices=step_option_values,
        default='all',
        help="Qserv configurator install step : \n'" +
        "     " + step_option_values[0] + " : create a qserv_run_dir " +
        "directory which will contains configuration and execution data " +
        "for a given Qserv instance\n" +
        "     " + step_option_values[1] + " : fill qserv_run_dir " +
        "configuration files with values from meta-config file qserv.conf"
        )

    # run dir, all mutable data related to a qserv running instance are
    # located here
    qserv_version=check_output(["qserv-version.sh"])
    qserv_version = qserv_version.strip(' \t\n\r')
    default_qserv_run_dir=os.path.join(os.path.expanduser("~"),"qserv-run",qserv_version)
    parser.add_argument("-r", "--qserv-run-dir", dest="qserv_run_dir",
            default=default_qserv_run_dir,
            help="Full path to qserv_run_dir"
            )

    # meta-configuration file whose parameters will be dispatched in Qserv
    # services configuration files
    args = parser.parse_args()
    default_meta_config_file = os.path.join(args.qserv_run_dir, "qserv.conf")
    parser.add_argument("-c", "--metaconfig",  dest="meta_config_file",
            default=default_meta_config_file,
            help="Full path to Qserv meta-configuration file"
            )

    args = parser.parse_args()

    if args.step=='all':
        args.step_list = step_list 
    else:
        args.step_list = [args.step]

    return args

def copy_and_overwrite(from_path, to_path):
    if os.path.exists(to_path):
        shutil.rmtree(to_path)
    shutil.copytree(from_path, to_path)

def main():

    args = parseArgs()

    logging.basicConfig(level=logging.INFO)

    qserv_dir = os.path.abspath(
                    os.path.join(
                        os.path.dirname(os.path.realpath(__file__)),
                        "..")
                )

    if 'prepare' in args.step_list:
        template_config_dir = os.path.join( qserv_dir, "admin")

        logging.info("Initializing configuration from {0} to {1}".format(template_config_dir, args.qserv_run_dir))
        copy_and_overwrite(template_config_dir, args.qserv_run_dir)

        for line in fileinput.input(args.meta_config_file, inplace = 1): 
            print line.replace("run_base_dir =", "run_base_dir = " + args.qserv_run_dir),

    if 'position' in args.step_list: 
        sconstruct_file = os.path.join(template_config_dir, "SConstruct")
        cmd = ["scons", 
                "-f {0}".format(sconstruct_file),
                "METACONFIG={0}".format(args.meta_config_file)
                ] 
        logging.info("Lauching configuration command {0}".format(cmd))
        commons.run_command(cmd)
 
if __name__ == '__main__':
    main()
