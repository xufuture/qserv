SERVICES="mysqld xrootd zookeeper mysql-proxy qserv-czar"

usage() {
	echo "Usage: $0 [-r qserv_run_dir] [-h]"
	exit 1
}

check_qserv_run_dir() {

    [ -z ${QSERV_RUN_DIR} ] &&
    {
        DEFAULT_QSERV_RUN_DIR=${HOME}/qserv-run/$(qserv-version.sh)
        echo "INFO: Qserv execution directory unspecified, using default one : ${DEFAULT_QSERV_RUN_DIR}"
        echo "INFO: Qserv execution directory can be specified using -r option, or by running : "
        echo "INFO:    export QSERV_RUN_DIR=/qserv/run/dir/ "
        QSERV_RUN_DIR=${DEFAULT_QSERV_RUN_DIR}
    }

    [ ! -d ${QSERV_RUN_DIR} ] &&
    {
        echo "ERROR: Unable to start Qserv"
        echo "ERROR: QSERV_RUN_DIR (${QSERV_RUN_DIR}) has to point on an existing directory"
        exit 1
    }

    [ ! -w ${QSERV_RUN_DIR} ] &&
    {
        echo "ERROR: Unable to start Qserv"
        echo "ERROR: Write access required to QSERV_RUN_DIR (${QSERV_RUN_DIR})"
        exit 1
    }

    echo "INFO: Qserv execution directory : ${QSERV_RUN_DIR}"
}
