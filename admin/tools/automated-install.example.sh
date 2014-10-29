#!/bin/sh

set -e
set -x

#############################
# CUSTOMIZE NEXT PARAMETERS :
#############################

INSTALL_DIR=root/directory/where/qserv/stack/will/be/installed

# If you are using internet-free mode, please specify the location
# of the local distribution server, if no comment line below.
INTERNET_FREE_DISTSERVER_DIR=shared/dir/available/to/all/nodes/distserver

# WARN: previous configuration data contained in QSERV_RUN_DIR
# will be removed
QSERV_RUN_DIR=${HOME}/qserv-run/${QSERV_RELEASE}

#############################

if [[ ! -z  ${QSERV_RUN_DIR} ]]; then
    rm -rf ${QSERV_RUN_DIR}
fi

if [[ ! -z  ${INTERNET_FREE_DISTSERVER_DIR} ]]; then
    INTERNET_FREE_OPT="-r ${INTERNET_FREE_DISTSERVER_DIR}"
fi
QSERV_RUN_DIR_OPT="-R ${QSERV_RUN_DIR}"

# automated install script is available ether in internet-free distserver, or in Qserv sources :
if [[ ! -z  ${INTERNET_FREE_DISTSERVER_DIR} ]]; then
    INTERNET_FREE_OPT="-r ${INTERNET_FREE_DISTSERVER_DIR}"
    QSERV_INSTALL_SCRIPT=${INTERNET_FREE_DISTSERVER_DIR}/qserv-install.sh
else
    QSERV_INSTALL_SCRIPT=${QSERV_SRC_DIR}/admin/tools/qserv-install.sh
fi
bash ${QSERV_INSTALL_SCRIPT} ${INTERNET_FREE_OPT} ${QSERV_RUN_DIR_OPT} -i ${INSTALL_DIR}
