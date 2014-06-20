#!/bin/sh

QSERV_RUN_DIR={{QSERV_RUN_DIR}}
. ${QSERV_RUN_DIR}/bin/env.sh

usage(){
	echo "Usage: $0 [-r qserv_run_dir] [-h]"
	exit 1
}

while getopts "hr:" option
do
    case $option in
    h)
	usage
        ;;
    r)
        echo "Exécution des commandes de l'option e"
	QSERV_RUN_DIR=${OPTARG}
	;;
    esac
done

[ -z ${QSERV_RUN_DIR} ] && 
{
    echo "Please specify an execution (i.e. run) directory using -h option, or by running : \n "
    echo "    export QSERV_RUN_DIR=/qserv/run/dir/\n "
    exit 1
}

[ -d ${QSERV_RUN_DIR} ] && 
{
    echo "QSERV_RUN_DIR (${QSERV_RUN_DIR}) has to point on an existing directory"
    exit 1
}

[ -w ${QSERV_RUN_DIR} ] && 
{
    echo "Write access required to QSERV_RUN_DIR (${QSERV_RUN_DIR})"
    exit 1
}

for service in ${SERVICES}; do
    ${QSERV_RUN_DIR}/etc/init.d/$service start
done
 
