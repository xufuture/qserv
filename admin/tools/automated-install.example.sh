#!/bin/sh

set -e
set -x

#############################
# CUSTOMIZE NEXT PARAMETERS :
#############################

INSTALL_DIR=root/directory/where/qserv/stack/will/be/installed

# If you are using internet-free mode, please specify the location of the local distribution
# server, if no comment two lines below. 
INTERNET_FREE_DISTSERVER_PARENT_DIR=shared/dir/available/to/all/nodes
INTERNET_FREE_DISTSERVER_DIR=${INTERNET_FREE_DISTSERVER_PARENT_DIR}/distserver

# If you want to remove previous configuration data, please set QSERV_RELEASE,
# if no comment line below
QSERV_RUN_DIR=${HOME}/qserv-run/${QSERV_RELEASE}

#############################

# CAUTION: remove previous configuration data
if [[ ! -z  ${QSERV_RUN_DIR} ]]; then
    rm -rf ${QSERV_RUN_DIR}
fi

if [[ ! -z  ${INTERNET_FREE_DISTSERVER_DIR} ]]; then
    INTERNET_FREE_OPT="-r ${INTERNET_FREE_DISTSERVER_DIR}"
fi
bash ${INTERNET_FREE_DISTSERVER_DIR}/qserv-install.sh ${INTERNET_FREE_OPT} -i ${INSTALL_DIR}

