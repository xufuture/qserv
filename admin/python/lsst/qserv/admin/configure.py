import commons
from datetime import datetime
import logging
import os
import sys

from lsst.qserv.admin import path 

def exists_and_is_writable(dir) :
    """
    Test if a dir exists. If no creates it, if yes checks if it is writeable.
    Return True if a writeable directory exists at the end of function execution, else False
    """
    logger = logging.getLogger()
    logger.debug("Checking existence and write access for : %s", dir)
    if not os.path.exists(dir):
	try: 
    		os.makedirs(dir)
	except OSerror:
            logger.error("Unable to create dir : %s " % dir)
            return False 
    elif not path.is_writable(dir):
        return False
    return True	 


# TODO : put in a shell script
def check_root_dirs():

    logger = logging.getLogger()

    check_success=True

    config = commons.getConfig()

    for (section,option) in (('qserv','base_dir'),('qserv','log_dir'),('qserv','tmp_dir'),('mysqld','data_dir')):
        dir = config[section][option]
        if not exists_and_is_writable(dir):
	    logging.fatal(  ("%s is not writable check/update permissions or"
                            " change config['%s']['%s']") %
                            (dir,section,option)
                         )
            sys.exit(1)

    for suffix in ('etc', 'var', 'var/lib', 'var/run', 'var/run/mysqld', 'var/lock/subsys'):
        dir = os.path.join(config['qserv']['run_base_dir'],suffix)
        if not exists_and_is_writable(dir):
	    logging.fatal("%s is not writable check/update permissions" % dir)
            sys.exit(1)

    # user config
    user_config_dir=os.path.join(os.getenv("HOME"),".lsst")
    if not exists_and_is_writable(user_config_dir):
	    logging.fatal("%s is not writable check/update permissions" % dir)
            sys.exit(1)

    logger.info("Qserv directory structure creation succeeded")

def check_root_symlinks():
    """ symlinks creation for directories externalised from qserv run dir
	i.e. QSERV_RUN_DIR/var/log will be symlinked to  config['qserv']['log_dir'] if needed
    """
    log = logging.getLogger()
    config=commons.getConfig()

    for (section,option,symlink_suffix) in (
        ('qserv','log_dir','var/log'),
        ('qserv','tmp_dir','tmp'),
        ('mysqld','data_dir', 'var/lib/mysql')
        ):
        symlink_target = config[section][option]
        default_dir = os.path.join(config['qserv']['run_base_dir'],symlink_suffix)

        # A symlink is needed if the target directory is not set to its default value 
        if  not os.path.samefile(symlink_target, os.path.realpath(default_dir)):
            if os.path.exists(default_dir) :
                if os.path.islink(default_dir):
                    os.unlink(default_dir)
                else:
		    log.critical("Please remove {0} and restart the configuration procedure".format(default_dir))
		    sys.exit(1)
            _symlink(symlink_target, default_dir)

    log.info("Qserv symlinks creation for externalized directories succeeded")

def _symlink(target, link_name):
    logger = logging.getLogger()
    logger.debug("Creating symlink, target : %s, link name : %s " % (target,link_name))
    os.symlink(target, link_name)

def uninstall(target, source, env):
    logger = logging.getLogger()
    uninstall_paths = [
            os.path.join(config['qserv']['log_dir']),
            os.path.join(config['mysqld']['data_dir']),
            os.path.join(config['qserv']['scratch_dir']),
            client_config_dir
            ]
    for path in uninstall_paths:
        if not os.path.exists(path):
            logger.info("Not uninstalling %s because it doesn't exists." % path)
        else:
            shutil.rmtree(path)
