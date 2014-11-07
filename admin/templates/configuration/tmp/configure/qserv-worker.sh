#!/bin/bash

set -e

QSERV_RUN_DIR={{QSERV_RUN_DIR}}
MYSQLD_SOCK_WORKER={{QSERV_RUN_DIR}}/var/lib/mysql-worker/mysql.sock
MYSQLD_PASS={{MYSQLD_PASS}}
MYSQL_DIR={{MYSQL_DIR}}
MYSQLD_USER={{MYSQLD_USER}}

SQL_FILE=${QSERV_RUN_DIR}/tmp/configure/sql/qserv-worker.sql

MYSQL_CMD="${MYSQL_DIR}/bin/mysql --no-defaults -vvv --user=${MYSQLD_USER}
--password=${MYSQLD_PASS} --sock=${MYSQLD_SOCK_WORKER}"

echo 
echo "-- Initializing Qserv worker database "
mkdir -p {{QSERV_RUN_DIR}}/var/run/mysqld-worker
mysqld_safe --defaults-file=${QSERV_RUN_DIR}/etc/my.worker.cnf >/dev/null 2>&1 & 
sleep 1
echo "-- Loading ${SQL_FILE} in MySQL"
${MYSQL_CMD} < ${SQL_FILE}
kill `cat ${QSERV_RUN_DIR}/var/run/mysqld-worker/mysqld.pid`

echo "INFO: Qserv worker initialization SUCCESSFUL"
