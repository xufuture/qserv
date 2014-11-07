#!/usr/bin/env bash

set -e

QSERV_RUN_DIR={{QSERV_RUN_DIR}}
SCISQL_DIR={{SCISQL_DIR}}
MYSQL_DIR={{MYSQL_DIR}}
MYSQLD_PASS={{MYSQLD_PASS}}
MYSQLD_SOCK_WORKER=${QSERV_RUN_DIR}/var/lib/mysql-worker/mysql.sock
PYTHON_BIN={{PYTHON_BIN}}
export PYTHONPATH={{PYTHONPATH}}

PYTHON_DIR=`dirname ${PYTHON_BIN}`
export PATH=${PYTHON_DIR}:${PATH}
export LD_LIBRARY_PATH={{LD_LIBRARY_PATH}}

mysqld_safe --defaults-file=${QSERV_RUN_DIR}/etc/my.worker.cnf >/dev/null 2>&1 &
cd ${SCISQL_DIR}
echo "-- Deploying sciSQL plugin in MySQL database"
# password is given on stdin, so that is can't be catched by ps command
PASSFILE=${QSERV_RUN_DIR}/tmp/pass.txt
cat <<EOF > ${PASSFILE}
${MYSQLD_PASS}
EOF
cat  ${PASSFILE} | scisql-deploy.py --mysql-socket=${MYSQLD_SOCK_WORKER}
rm ${PASSFILE}
kill `cat ${QSERV_RUN_DIR}/var/run/mysqld-worker/mysqld.pid`

echo "INFO: sciSQL installation and configuration SUCCESSFUL"
