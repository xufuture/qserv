#!/bin/sh

set -e
set -x

QSERV_SRC_DIR=~fjammes/src/qserv
OFFLINE_DISTSERVER=~fjammes/distserver
INSTALL_DIR=/data/qserv/stack

rm -rf ~/qserv-run
chmod -R 777 ${INSTALL_DIR}
bash ${QSERV_SRC_DIR}/admin/tools/qserv-install.sh -r ${OFFLINE_DISTSERVER} -i
${INSTALL_DIR}

