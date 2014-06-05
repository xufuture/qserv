#!/usr/bin/env python

from lsst.qserv.admin import commons, logger
from subprocess import check_output
import fileinput
import logging
import os
import optparse
import shutil


def parseOptions():
    op = optparse.OptionParser()

    step_list = ['prepare','position']
    step_option_values = step_list + ['all']
    op.add_option("-s", "--step", type='choice', dest="step", choices=step_option_values,
        default='all',
        help="Qserv configurator install step : \n'" +
        "     " + step_option_values[0] + " : create a qserv_run_dir " +
        "directory which will contains configuration and execution data " +
        "for a given Qserv instance\n" +
        "     " + step_option_values[1] + " : fill qserv_run_dir " +
        "configuration files with values from meta-config file qserv.conf" +
        "' [default: %default]")

    qserv_version=check_output(["qserv-version.sh"])
    qserv_version = qserv_version.strip(' \t\n\r')
    default_qserv_run_dir=os.path.join(os.path.expanduser("~"),"qserv-run",qserv_version)
    op.add_option("-r", "--qserv-run-dir", dest="qserv_run_dir",
              default=default_qserv_run_dir,
              help="Full path to qserv_run_dir" +
              "' [default: %default]")
    options = op.parse_args()[0]

    if options.step=='all':
        options.step_list = step_list 
    else:
        options.step_list = [options.step]

    return options

def copy_and_overwrite(from_path, to_path):
    if os.path.exists(to_path):
        shutil.rmtree(to_path)
    shutil.copytree(from_path, to_path)

def main():

    options = parseOptions()

    logging.basicConfig(level=logging.INFO)
    meta_config_file = os.path.join(options.qserv_run_dir, "qserv.conf")

    if 'prepare' in options.step_list:
        template_config_dir = os.path.abspath(
                                os.path.join(
                                    os.path.dirname(os.path.realpath(__file__)),
                                    "..","admin"
                                )
                            )

        logging.info("Initializing configuration from {0} to {1}".format(template_config_dir, options.qserv_run_dir))
        copy_and_overwrite(template_config_dir, options.qserv_run_dir)

        for line in fileinput.input(meta_config_file, inplace = 1): 
            print line.replace("run_base_dir =", "run_base_dir = " + options.qserv_run_dir),

    if 'position' in options.step_list: 
        config = commons.read_config(meta_config_file)
    
        logger.init_default_logger(
            "qserv-configurator",
            logging.DEBUG,
            log_path=config['qserv']['log_dir']
            )
        
if __name__ == '__main__':
    main()
