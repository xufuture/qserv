#!/bin/bash

set -e

QSERV_RUN_DIR={{QSERV_RUN_DIR}}
MYSQLD_SOCK={{MYSQLD_SOCK}}
MYSQLD_PASS={{MYSQLD_PASS}}
MYSQL_DIR={{MYSQL_DIR}}
MYSQLD_USER={{MYSQLD_USER}}

SQL_FILE=${QSERV_RUN_DIR}/tmp/configure/sql/qserv-czar.sql

MYSQL_CMD="${MYSQL_DIR}/bin/mysql --no-defaults -vvv --user=${MYSQLD_USER} --password=${MYSQLD_PASS} --sock=${MYSQLD_SOCK}"

echo 
echo "-- Initializing Qserv czar database "
${QSERV_RUN_DIR}/etc/init.d/mysqld start
echo "-- Loading ${SQL_FILE} in MySQL"
${MYSQL_CMD} < ${SQL_FILE}
${QSERV_RUN_DIR}/etc/init.d/mysqld stop

echo "INFO: Qserv Czar initialization SUCCESSFUL"
