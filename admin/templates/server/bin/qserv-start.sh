#!/bin/sh

QSERV_RUN_DIR={{QSERV_RUN_DIR}}
. ${QSERV_RUN_DIR}/bin/env.sh

for service in ${SERVICES}; do
    ${QSERV_RUN_DIR}/etc/init.d/$service start
done
 
