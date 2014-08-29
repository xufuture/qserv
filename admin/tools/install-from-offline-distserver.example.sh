#!/bin/sh

set -e
set -x

OFFLINE_DISTSERVER_DIR=shared/dir/available/to/all/nodes
INSTALL_DIR=root/directory/where/qserv/stack/will/be/installed

# CAUTION : remove previous configuration data
rm -rf ~/qserv-run
# CAUTION : previous install will be removed
bash ${OFFLINE_DISTSERVER_DIR}/qserv-install.sh -r ${OFFLINE_DISTSERVER_DIR} -i ${INSTALL_DIR}

