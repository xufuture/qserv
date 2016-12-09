#!/bin/bash

# Optimize MySQL data 

# @author  Fabrice Jammes, IN2P3

set -x

DEST_DIR="/qserv/data"

DIR=$(cd "$(dirname "$0")"; pwd -P)
cd "${DIR}/.."
. "env.sh"


usage() {
  cat << EOD

Usage: `basename $0` [options]

  Available options:
    -h          this message


  Copy initial MySQL/qserv data from container to host disk.
  Pre-requisites:
      - run 'run.sh' with HOST_DATA_DIR commented in 'env.sh'
      - run 'stop.sh -K'

EOD
}

# get the options
while getopts h ; do
    case $c in
            h) usage ; exit 0 ;;
            \?) usage ; exit 2 ;;
    esac
done
shift $(($OPTIND - 1))

if [ $# -ne 0 ] ; then
    usage
    exit 2
fi

read -p "WARNING: Do not stop this script or it might corrupt huge data set. Confirm? " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]
then
    exit 1
fi

shmux -S all -c "docker exec qserv bash -c '. /qserv/stack/loadLSST.bash && \
    setup mariadb && \
    mysqlcheck -a -u qsmaster -S /qserv/run/var/lib/mysql/mysql.sock LSST'" $SSH_MASTER $SSH_WORKERS

